//! Producer dispatcher that routes ready batches through the wire client.

use std::{
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

use ahash::{AHashMap, AHashSet};
use kacrab_protocol::{
    KafkaString,
    generated::{
        AddPartitionsToTxnRequestData, AddPartitionsToTxnResponseData, AddPartitionsToTxnTopic,
        AddPartitionsToTxnTransaction, ApiKey, EndTxnRequestData, EndTxnResponseData,
        FindCoordinatorRequestData, FindCoordinatorResponseData, InitProducerIdRequestData,
        InitProducerIdResponseData, PartitionProduceData, ProduceRequestData, TopicProduceData,
    },
    version::client_api_info,
};
use tokio::{sync::Mutex, task::JoinSet};

use super::{
    accumulator::{
        RECORD_BATCH_OVERHEAD_BYTES, ReadyBatch, RecordAccumulator, estimate_record_batch_bytes,
    },
    batch::encode_record_batch_with_producer_state,
    config::{
        DEFAULT_DELIVERY_TIMEOUT, DEFAULT_TIMEOUT_MS, DEFAULT_TRANSACTION_TIMEOUT_MS,
        ProducerCompression, ProducerIdempotenceConfig, ProducerRuntimeConfig,
    },
    error::{ProducerError, Result},
    record::ProduceReceipt,
    response::produce_receipts,
    routing::{ProduceRoute, murmur2_java, route},
    transaction::{ProducerBatchState, ProducerIdentity},
};
use crate::wire::{BrokerEndpoint, TopicMetadata, WireClient};

/// Dispatcher-only fallback for tests/manual construction. Public producer
/// configs still default to `acks=all`; this keeps `ProducerDispatcher::new`
/// compatible with earlier unit tests that modeled leader-only acknowledgements.
const DEFAULT_DISPATCHER_ACKS: i16 = 1;

/// Dispatches ready accumulator batches to broker leaders through [`WireClient`].
#[derive(Debug, Clone)]
pub struct ProducerDispatcher {
    wire: WireClient,
    acks: i16,
    timeout_ms: i32,
    retry_attempts: usize,
    delivery_timeout: Duration,
    compression: ProducerCompression,
    max_in_flight_requests_per_connection: usize,
    partitioner_ignore_keys: bool,
    partition_sticky_batch_size: usize,
    idempotence: ProducerIdempotenceConfig,
    producer_state: Arc<Mutex<ProducerIdempotenceState>>,
    partitioner_state: Arc<Mutex<ProducerPartitionerState>>,
}

impl ProducerDispatcher {
    /// Create a dispatcher with Kafka producer defaults for acks and timeout.
    #[must_use]
    pub fn new(wire: WireClient) -> Self {
        Self {
            wire,
            acks: DEFAULT_DISPATCHER_ACKS,
            timeout_ms: DEFAULT_TIMEOUT_MS,
            retry_attempts: 0,
            delivery_timeout: DEFAULT_DELIVERY_TIMEOUT,
            compression: ProducerCompression {
                codec: kacrab_protocol::compression::Compression::None,
                level: None,
            },
            max_in_flight_requests_per_connection:
                crate::wire::DEFAULT_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
            partitioner_ignore_keys: false,
            partition_sticky_batch_size: super::AccumulatorConfig::default().batch_size,
            idempotence: ProducerIdempotenceConfig {
                enabled: false,
                transactional_id: None,
                transaction_timeout_ms: DEFAULT_TRANSACTION_TIMEOUT_MS,
                transaction_two_phase_commit: false,
            },
            producer_state: Arc::new(Mutex::new(ProducerIdempotenceState::default())),
            partitioner_state: Arc::new(Mutex::new(ProducerPartitionerState::default())),
        }
    }

    /// Create a dispatcher from explicit runtime producer config.
    #[must_use]
    pub fn with_config(wire: WireClient, config: ProducerRuntimeConfig) -> Self {
        Self {
            wire,
            acks: config.acks,
            timeout_ms: config.timeout_ms,
            retry_attempts: config.retry_attempts,
            delivery_timeout: config.delivery_timeout,
            compression: config.compression,
            max_in_flight_requests_per_connection: config.max_in_flight_requests_per_connection,
            partitioner_ignore_keys: config.partitioner_ignore_keys,
            partition_sticky_batch_size: config.accumulator.batch_size,
            idempotence: config.idempotence,
            producer_state: Arc::new(Mutex::new(ProducerIdempotenceState::default())),
            partitioner_state: Arc::new(Mutex::new(ProducerPartitionerState::default())),
        }
    }

    /// Set the number of retry attempts for retryable leadership errors.
    #[must_use]
    pub const fn retry_attempts(mut self, attempts: usize) -> Self {
        self.retry_attempts = attempts;
        self
    }

    /// Set the upper bound for delivering a drained batch.
    #[must_use]
    pub const fn delivery_timeout(mut self, timeout: Duration) -> Self {
        self.delivery_timeout = timeout;
        self
    }

    /// Assign a concrete partition to an unassigned record using current metadata.
    pub async fn assign_partition(&self, record: &mut super::ProducerRecord) -> Result<()> {
        if record.has_assigned_partition() {
            return Ok(());
        }
        let metadata = self
            .wire
            .metadata_for_topics([record.topic.as_ref()])
            .await?;
        let partition = {
            let mut state = self.partitioner_state.lock().await;
            state.partition_for_record(
                &metadata,
                record,
                self.partitioner_ignore_keys,
                self.partition_sticky_batch_size,
            )?
        };
        record.partition = partition;
        Ok(())
    }

    /// Assign concrete partitions to a batch using one metadata snapshot.
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Batch partition selection mutates sticky state while holding the partitioner \
                  lock."
    )]
    pub async fn assign_partitions(&self, records: &mut [super::ProducerRecord]) -> Result<()> {
        let topics = unique_unassigned_record_topics(records);
        if topics.is_empty() {
            return Ok(());
        }
        let metadata = self
            .wire
            .metadata_for_topics(topics.iter().map(String::as_str))
            .await?;
        let mut state = self.partitioner_state.lock().await;
        for topic in topics {
            let topic_metadata = metadata
                .topic(&topic)
                .ok_or_else(|| ProducerError::UnknownTopic(topic.clone()))?;
            state.assign_topic_partitions(
                TopicPartitionAssignment {
                    topic: &topic,
                    topic_metadata,
                    ignore_keys: self.partitioner_ignore_keys,
                    sticky_batch_size: self.partition_sticky_batch_size,
                },
                records,
            )?;
        }
        Ok(())
    }

    /// Initialize a transactional producer id through the transaction coordinator.
    pub async fn init_transactions(&self) -> Result<()> {
        let coordinator_id = self.coordinator_id().await?;
        let _identity = self.producer_identity(coordinator_id).await?;
        Ok(())
    }

    /// Begin a transaction after [`Self::init_transactions`] has succeeded.
    pub fn begin_transaction(&self) -> Result<()> {
        if self.idempotence.transactional_id.is_none() {
            return Err(ProducerError::TransactionalIdRequired);
        }
        let mut state = self
            .producer_state
            .try_lock()
            .map_err(|_error| ProducerError::TransactionStateBusy)?;
        if state.identity.is_none() {
            return Err(ProducerError::InvalidTransactionState(
                "init_transactions must run before begin_transaction",
            ));
        }
        if state.in_transaction {
            return Err(ProducerError::InvalidTransactionState(
                "transaction is already open",
            ));
        }
        state.in_transaction = true;
        state.transaction_partitions.clear();
        drop(state);
        Ok(())
    }

    /// End the currently open transaction.
    pub async fn end_transaction(&self, committed: bool) -> Result<()> {
        let transactional_id = self
            .idempotence
            .transactional_id
            .as_ref()
            .ok_or(ProducerError::TransactionalIdRequired)?;
        let coordinator_id = self.coordinator_id().await?;
        let identity = self.producer_identity(coordinator_id).await?;
        {
            let state = self.producer_state.lock().await;
            if !state.in_transaction {
                return Err(ProducerError::InvalidTransactionState(
                    "no transaction is open",
                ));
            }
        }
        let request = EndTxnRequestData {
            transactional_id: KafkaString::from(transactional_id.clone()),
            producer_id: identity.producer_id,
            producer_epoch: identity.producer_epoch,
            committed,
            _unknown_tagged_fields: Vec::new(),
        };
        let version = client_api_info(ApiKey::EndTxn).max_version;
        let response: EndTxnResponseData = self
            .wire
            .send_to_broker(coordinator_id, ApiKey::EndTxn, version, &request)
            .await?;
        let error = kacrab_protocol::generated::ErrorCode::from(response.error_code);
        if error.is_error() {
            return Err(ProducerError::Transaction {
                operation: "end_txn",
                error,
            });
        }
        let mut state = self.producer_state.lock().await;
        state.in_transaction = false;
        state.transaction_partitions.clear();
        drop(state);
        Ok(())
    }

    /// Drain ready accumulator batches, route them by leader, and send produce requests.
    pub async fn dispatch_ready(
        &self,
        accumulator: &mut RecordAccumulator,
        now: Instant,
    ) -> Result<Vec<ProduceReceipt>> {
        let mut batches = accumulator.drain_ready(now);
        if batches.is_empty() {
            return Ok(Vec::new());
        }
        if let Some(batch) = self.expired_batch(&batches, now) {
            return Err(ProducerError::DeliveryTimeout {
                topic: batch.topic.clone(),
                partition: batch.partition,
            });
        }

        let mut attempts_remaining = self.retry_attempts;
        loop {
            match self.dispatch_batches(&mut batches).await {
                Ok(receipts) => return Ok(receipts),
                Err(DispatchError::Requeue) => {
                    accumulator.requeue_front(batches);
                    return Ok(Vec::new());
                },
                Err(DispatchError::RetryableLeadership {
                    topic,
                    partition,
                    error,
                }) => {
                    self.wire.invalidate_topic_partition(&topic, partition);
                    if attempts_remaining == 0 {
                        return Err(ProducerError::Broker {
                            topic,
                            partition,
                            error,
                        });
                    }
                    attempts_remaining = attempts_remaining.saturating_sub(1);
                    if let Some(error) = self.wait_before_retry(&batches).await {
                        return Err(error);
                    }
                },
                Err(DispatchError::Producer(error)) => return Err(error),
            }
        }
    }

    /// Dispatch already-drained ready batches.
    ///
    /// This owned-batch path lets callers keep multiple produce requests in
    /// flight while reusing an accumulator after `drain_ready`.
    pub async fn dispatch_ready_batches(
        &self,
        batches: Vec<ReadyBatch>,
        now: Instant,
    ) -> Result<Vec<ProduceReceipt>> {
        if batches.is_empty() {
            return Ok(Vec::new());
        }
        match self.dispatch_drained(batches, now).await {
            DispatchOutcome::Delivered(result) => result,
            DispatchOutcome::Requeue(_batches) => Err(ProducerError::FlushIncomplete),
        }
    }

    /// Drain all accumulator batches and send them regardless of linger or size.
    pub async fn dispatch_all(
        &self,
        accumulator: &mut RecordAccumulator,
    ) -> Result<Vec<ProduceReceipt>> {
        let batches = accumulator.drain_all();
        if batches.is_empty() {
            return Ok(Vec::new());
        }
        match self.dispatch_drained(batches, Instant::now()).await {
            DispatchOutcome::Delivered(result) => result,
            DispatchOutcome::Requeue(batches) => {
                accumulator.requeue_front(batches);
                Err(ProducerError::FlushIncomplete)
            },
        }
    }

    pub(crate) async fn dispatch_drained(
        &self,
        mut batches: Vec<ReadyBatch>,
        now: Instant,
    ) -> DispatchOutcome {
        if let Some(batch) = self.expired_batch(&batches, now) {
            return DispatchOutcome::Delivered(Err(ProducerError::DeliveryTimeout {
                topic: batch.topic.clone(),
                partition: batch.partition,
            }));
        }

        let mut attempts_remaining = self.retry_attempts;
        loop {
            match self.dispatch_batches(&mut batches).await {
                Ok(receipts) => {
                    complete_deliveries(&mut batches, &receipts);
                    return DispatchOutcome::Delivered(Ok(receipts));
                },
                Err(DispatchError::Requeue) => return DispatchOutcome::Requeue(batches),
                Err(DispatchError::RetryableLeadership {
                    topic,
                    partition,
                    error,
                }) => {
                    self.wire.invalidate_topic_partition(&topic, partition);
                    if attempts_remaining == 0 {
                        return DispatchOutcome::Delivered(Err(ProducerError::Broker {
                            topic,
                            partition,
                            error,
                        }));
                    }
                    attempts_remaining = attempts_remaining.saturating_sub(1);
                    if let Some(error) = self.wait_before_retry(&batches).await {
                        return DispatchOutcome::Delivered(Err(error));
                    }
                },
                Err(DispatchError::Producer(error)) => {
                    return DispatchOutcome::Delivered(Err(error));
                },
            }
        }
    }

    async fn dispatch_batches(
        &self,
        batches: &mut [ReadyBatch],
    ) -> std::result::Result<Vec<ProduceReceipt>, DispatchError> {
        let topics = unique_topics(batches);
        let metadata = self
            .wire
            .metadata_for_topics(topics.iter().map(String::as_str))
            .await
            .map_err(DispatchError::from)?;
        let mut by_broker: AHashMap<i32, Vec<BrokerProduceRequest>> = AHashMap::new();
        for batch in batches {
            let Some(first_record) = batch.records.first().cloned() else {
                continue;
            };
            let route = self
                .route_for_batch(&metadata, batch, &first_record)
                .await
                .map_err(DispatchError::from_route)?;
            self.add_partition_to_transaction(&route)
                .await
                .map_err(DispatchError::from)?;
            if batch.producer_state.is_none() {
                batch.producer_state = self
                    .producer_batch_state(&route, batch.records.len())
                    .await
                    .map_err(DispatchError::from)?;
            }
            let records = encode_record_batch_with_producer_state(
                &batch.records,
                self.compression,
                batch.producer_state,
            )
            .map_err(DispatchError::from)?;
            let requests = by_broker.entry(route.leader_id).or_default();
            let request_index = requests
                .iter()
                .position(|request| !request.contains_route(&route));
            let request_index = request_index.unwrap_or_else(|| {
                requests.push(BrokerProduceRequest::default());
                requests.len().saturating_sub(1)
            });
            let Some(request) = requests.get_mut(request_index) else {
                return Err(DispatchError::Producer(ProducerError::FlushIncomplete));
            };
            request.push(
                route,
                records,
                BrokerProduceOptions {
                    acks: self.acks,
                    timeout_ms: self.timeout_ms,
                    transactional_id: self.idempotence.transactional_id.as_deref(),
                },
            );
        }

        let mut receipts = Vec::new();
        let version = client_api_info(ApiKey::Produce).max_version;
        for (broker_id, requests) in by_broker {
            let mut broker_receipts = self
                .dispatch_broker_requests(broker_id, requests, version)
                .await?;
            receipts.append(&mut broker_receipts);
        }
        Ok(receipts)
    }

    async fn dispatch_broker_requests(
        &self,
        broker_id: i32,
        requests: Vec<BrokerProduceRequest>,
        version: i16,
    ) -> std::result::Result<Vec<ProduceReceipt>, DispatchError> {
        let mut pending = requests.into_iter().enumerate();
        let mut in_flight = JoinSet::new();
        let mut completed = Vec::new();
        let max_in_flight = self.max_in_flight_requests_per_connection.max(1);

        loop {
            while in_flight.len() < max_in_flight {
                let Some((index, request)) = pending.next() else {
                    break;
                };
                let wire = self.wire.clone();
                let abort_handle = in_flight.spawn(async move {
                    let response =
                        send_produce_with_backpressure_retry(&wire, broker_id, version, &request)
                            .await;
                    (index, request, response)
                });
                drop(abort_handle);
            }

            let Some(result) = in_flight.join_next().await else {
                break;
            };
            let (index, request, response) =
                result.map_err(|error| ProducerError::DispatchTask(error.to_string()))?;
            completed.push((index, request, response.map_err(ProducerError::from)?));
        }
        completed.sort_by_key(|(index, _request, _response)| *index);

        let mut receipts = Vec::new();
        for (_index, request, response) in completed {
            match produce_receipts(&response, &request.routes) {
                Ok(mut request_receipts) => receipts.append(&mut request_receipts),
                Err(ProducerError::Broker {
                    topic,
                    partition,
                    error,
                }) if is_leadership_error(error) => {
                    return Err(DispatchError::RetryableLeadership {
                        topic,
                        partition,
                        error,
                    });
                },
                Err(error) => return Err(DispatchError::from(error)),
            }
        }
        Ok(receipts)
    }

    async fn route_for_batch(
        &self,
        metadata: &crate::wire::ClusterMetadata,
        batch: &mut ReadyBatch,
        first_record: &super::ProducerRecord,
    ) -> Result<ProduceRoute> {
        if first_record.has_assigned_partition() {
            return route(metadata, first_record);
        }
        let mut record = first_record.clone();
        let partition = {
            let mut state = self.partitioner_state.lock().await;
            state.partition_for_record(
                metadata,
                &record,
                self.partitioner_ignore_keys,
                self.partition_sticky_batch_size,
            )?
        };
        record.partition = partition;
        batch.partition = record.partition;
        for batch_record in &mut batch.records {
            batch_record.partition = record.partition;
        }
        route(metadata, &record)
    }

    fn expired_batch<'a>(&self, batches: &'a [ReadyBatch], now: Instant) -> Option<&'a ReadyBatch> {
        batches
            .iter()
            .find(|batch| now.duration_since(batch.first_append_at) >= self.delivery_timeout)
    }

    async fn wait_before_retry(&self, batches: &[ReadyBatch]) -> Option<ProducerError> {
        let now = Instant::now();
        let earliest = batches
            .iter()
            .map(|batch| batch.first_append_at)
            .min()
            .unwrap_or(now);
        let elapsed = now.duration_since(earliest);
        if elapsed >= self.delivery_timeout {
            return batches.first().map(|batch| ProducerError::DeliveryTimeout {
                topic: batch.topic.clone(),
                partition: batch.partition,
            });
        }
        let remaining = self.delivery_timeout.saturating_sub(elapsed);
        tokio::time::sleep(PRODUCE_RETRY_BACKOFF.min(remaining)).await;
        self.expired_batch(batches, Instant::now())
            .map(|batch| ProducerError::DeliveryTimeout {
                topic: batch.topic.clone(),
                partition: batch.partition,
            })
    }

    async fn producer_batch_state(
        &self,
        route: &ProduceRoute,
        record_count: usize,
    ) -> Result<Option<ProducerBatchState>> {
        if !self.idempotence.enabled {
            return Ok(None);
        }
        let identity = self.producer_identity(route.leader_id).await?;
        let record_count =
            i32::try_from(record_count).map_err(|_error| ProducerError::SequenceOverflow {
                topic: route.topic.clone(),
                partition: route.partition,
            })?;
        let mut state = self.producer_state.lock().await;
        let base_sequence = state.next_sequence(&route.topic, route.partition, record_count)?;
        drop(state);
        Ok(Some(ProducerBatchState {
            identity,
            base_sequence,
        }))
    }

    async fn add_partition_to_transaction(&self, route: &ProduceRoute) -> Result<()> {
        let Some(transactional_id) = self.idempotence.transactional_id.as_ref() else {
            return Ok(());
        };
        let key = TopicPartitionKey {
            topic: route.topic.clone(),
            partition: route.partition,
        };
        {
            let state = self.producer_state.lock().await;
            if !state.in_transaction {
                return Err(ProducerError::InvalidTransactionState(
                    "produce called outside an open transaction",
                ));
            }
            if state.transaction_partitions.contains(&key) {
                return Ok(());
            }
        }

        let coordinator_id = self.coordinator_id().await?;
        let identity = self.producer_identity(coordinator_id).await?;
        let request = AddPartitionsToTxnRequestData {
            transactions: vec![AddPartitionsToTxnTransaction {
                transactional_id: KafkaString::from(transactional_id.clone()),
                producer_id: identity.producer_id,
                producer_epoch: identity.producer_epoch,
                verify_only: false,
                topics: vec![AddPartitionsToTxnTopic {
                    name: KafkaString::from(route.topic.clone()),
                    partitions: vec![route.partition],
                    _unknown_tagged_fields: Vec::new(),
                }],
                _unknown_tagged_fields: Vec::new(),
            }],
            ..AddPartitionsToTxnRequestData::default()
        };
        let version = client_api_info(ApiKey::AddPartitionsToTxn).max_version;
        let response: AddPartitionsToTxnResponseData = self
            .wire
            .send_to_broker(
                coordinator_id,
                ApiKey::AddPartitionsToTxn,
                version,
                &request,
            )
            .await?;
        Self::check_add_partitions_response(response, route)?;
        {
            let mut state = self.producer_state.lock().await;
            let _inserted = state.transaction_partitions.insert(key);
            drop(state);
        }
        Ok(())
    }

    fn check_add_partitions_response(
        response: AddPartitionsToTxnResponseData,
        route: &ProduceRoute,
    ) -> Result<()> {
        let top_level_error = kacrab_protocol::generated::ErrorCode::from(response.error_code);
        if top_level_error.is_error() {
            return Err(ProducerError::Transaction {
                operation: "add_partitions_to_txn",
                error: top_level_error,
            });
        }
        for transaction in response.results_by_transaction {
            for topic in transaction.topic_results {
                if topic.name.to_string() != route.topic {
                    continue;
                }
                for partition in topic.results_by_partition {
                    if partition.partition_index != route.partition {
                        continue;
                    }
                    let error =
                        kacrab_protocol::generated::ErrorCode::from(partition.partition_error_code);
                    if error.is_error() {
                        return Err(ProducerError::Transaction {
                            operation: "add_partitions_to_txn",
                            error,
                        });
                    }
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    async fn producer_identity(&self, broker_id: i32) -> Result<ProducerIdentity> {
        {
            let state = self.producer_state.lock().await;
            if let Some(identity) = state.identity {
                return Ok(identity);
            }
        }

        let request = InitProducerIdRequestData {
            transactional_id: self
                .idempotence
                .transactional_id
                .as_ref()
                .map(|id| KafkaString::from(id.clone())),
            transaction_timeout_ms: self.idempotence.transaction_timeout_ms,
            enable2_pc: self.idempotence.transaction_two_phase_commit,
            ..InitProducerIdRequestData::default()
        };
        let version = init_producer_id_version(self.idempotence.transaction_two_phase_commit);
        let mut attempts_remaining = self.retry_attempts;
        let response = loop {
            let response: InitProducerIdResponseData = self
                .wire
                .send_to_broker(broker_id, ApiKey::InitProducerId, version, &request)
                .await?;
            let error = kacrab_protocol::generated::ErrorCode::from(response.error_code);
            if !error.is_error() {
                break response;
            }
            if attempts_remaining == 0 || !error.is_retriable() {
                return Err(ProducerError::Transaction {
                    operation: "init_producer_id",
                    error,
                });
            }
            attempts_remaining = attempts_remaining.saturating_sub(1);
            tokio::time::sleep(TRANSACTION_COORDINATOR_RETRY_BACKOFF).await;
        };
        let identity = ProducerIdentity {
            producer_id: response.producer_id,
            producer_epoch: response.producer_epoch,
        };
        let mut state = self.producer_state.lock().await;
        let identity = *state.identity.get_or_insert(identity);
        drop(state);
        Ok(identity)
    }

    async fn coordinator_id(&self) -> Result<i32> {
        let transactional_id = self
            .idempotence
            .transactional_id
            .as_ref()
            .ok_or(ProducerError::TransactionalIdRequired)?;
        {
            let state = self.producer_state.lock().await;
            if let Some(coordinator_id) = state.coordinator_id {
                return Ok(coordinator_id);
            }
        }

        let request = FindCoordinatorRequestData {
            key_type: 1,
            coordinator_keys: vec![KafkaString::from(transactional_id.clone())],
            ..FindCoordinatorRequestData::default()
        };
        let broker_id = self.wire.any_broker_id()?;
        let version = client_api_info(ApiKey::FindCoordinator).max_version;
        let mut attempts_remaining = self.retry_attempts;
        let coordinator = loop {
            let response: FindCoordinatorResponseData = self
                .wire
                .send_to_broker(broker_id, ApiKey::FindCoordinator, version, &request)
                .await?;
            let coordinator = response
                .coordinators
                .into_iter()
                .find(|coordinator| coordinator.key.to_string() == *transactional_id)
                .ok_or(ProducerError::InvalidTransactionState(
                    "transaction coordinator response was missing transactional.id",
                ))?;
            let error = kacrab_protocol::generated::ErrorCode::from(coordinator.error_code);
            if !error.is_error() {
                break coordinator;
            }
            if attempts_remaining == 0 || !error.is_retriable() {
                return Err(ProducerError::Transaction {
                    operation: "find_coordinator",
                    error,
                });
            }
            attempts_remaining = attempts_remaining.saturating_sub(1);
            tokio::time::sleep(TRANSACTION_COORDINATOR_RETRY_BACKOFF).await;
        };
        let port = u16::try_from(coordinator.port).map_err(|_error| {
            ProducerError::InvalidTransactionState("transaction coordinator returned invalid port")
        })?;
        let addresses = tokio::net::lookup_host((coordinator.host.to_string(), port))
            .await
            .map_err(crate::wire::WireError::from)?;
        let addr = choose_coordinator_addr(addresses).ok_or(
            ProducerError::InvalidTransactionState("transaction coordinator host did not resolve"),
        )?;
        self.wire.upsert_broker(BrokerEndpoint::from_resolved(
            coordinator.node_id,
            coordinator.host.to_string(),
            port,
            addr,
        ));
        let mut state = self.producer_state.lock().await;
        let coordinator_id = *state.coordinator_id.get_or_insert(coordinator.node_id);
        drop(state);
        Ok(coordinator_id)
    }
}

#[derive(Debug, Default)]
struct ProducerIdempotenceState {
    identity: Option<ProducerIdentity>,
    sequences: AHashMap<TopicPartitionKey, i32>,
    coordinator_id: Option<i32>,
    in_transaction: bool,
    transaction_partitions: AHashSet<TopicPartitionKey>,
}

#[derive(Debug, Default)]
struct ProducerPartitionerState {
    next_by_topic: AHashMap<String, i32>,
    sticky_by_topic: AHashMap<String, StickyPartitionState>,
}

#[derive(Debug, Clone, Copy)]
struct StickyPartitionState {
    partition: i32,
    bytes: usize,
}

#[derive(Debug, Clone, Copy)]
struct TopicPartitionAssignment<'a> {
    topic: &'a str,
    topic_metadata: &'a TopicMetadata,
    ignore_keys: bool,
    sticky_batch_size: usize,
}

impl ProducerPartitionerState {
    fn next_for_topic(&mut self, topic: &str) -> &mut i32 {
        self.next_by_topic.entry(topic.to_owned()).or_insert(0)
    }

    fn partition_for_record(
        &mut self,
        metadata: &crate::wire::ClusterMetadata,
        record: &super::ProducerRecord,
        ignore_keys: bool,
        sticky_batch_size: usize,
    ) -> Result<i32> {
        if record.has_assigned_partition() {
            return Ok(record.partition);
        }
        if !ignore_keys && record.key.is_some() {
            let topic_metadata = Self::topic_metadata(metadata, &record.topic, record.partition)?;
            return key_partition(&record.topic, record.partition, topic_metadata, record);
        }
        let topic_metadata = Self::topic_metadata(metadata, &record.topic, record.partition)?;
        self.sticky_partition(&record.topic, topic_metadata, record, sticky_batch_size)
    }

    fn assign_topic_partitions(
        &mut self,
        assignment: TopicPartitionAssignment<'_>,
        records: &mut [super::ProducerRecord],
    ) -> Result<()> {
        let TopicPartitionAssignment {
            topic,
            topic_metadata,
            ignore_keys,
            sticky_batch_size,
        } = assignment;
        ensure_partitions(topic, super::record::UNASSIGNED_PARTITION, topic_metadata)?;
        let existing_sticky = self.valid_sticky(topic, topic_metadata);
        let mut sticky = match existing_sticky {
            Some(sticky) => sticky,
            None => StickyPartitionState {
                partition: self.next_partition(topic, topic_metadata)?,
                bytes: RECORD_BATCH_OVERHEAD_BYTES,
            },
        };
        let mut sticky_used = false;

        for record in records {
            if record.has_assigned_partition() || record.topic.as_ref() != topic {
                continue;
            }
            if !ignore_keys && record.key.is_some() {
                record.partition = key_partition(topic, record.partition, topic_metadata, record)?;
                continue;
            }

            sticky_used = true;
            record.partition = sticky.partition;
            sticky.bytes = sticky
                .bytes
                .saturating_add(estimate_record_batch_bytes(record));
            if sticky.bytes >= sticky_batch_size.max(1) {
                sticky = StickyPartitionState {
                    partition: self.next_partition(topic, topic_metadata)?,
                    bytes: RECORD_BATCH_OVERHEAD_BYTES,
                };
            }
        }

        if sticky_used {
            let _previous = self.sticky_by_topic.insert(topic.to_owned(), sticky);
        }
        Ok(())
    }

    fn sticky_partition(
        &mut self,
        topic: &str,
        topic_metadata: &TopicMetadata,
        record: &super::ProducerRecord,
        sticky_batch_size: usize,
    ) -> Result<i32> {
        ensure_partitions(topic, record.partition, topic_metadata)?;

        let existing_sticky = self.valid_sticky(topic, topic_metadata);
        let mut sticky = match existing_sticky {
            Some(sticky) => sticky,
            None => StickyPartitionState {
                partition: self.next_partition(topic, topic_metadata)?,
                bytes: RECORD_BATCH_OVERHEAD_BYTES,
            },
        };

        let partition = sticky.partition;
        sticky.bytes = sticky
            .bytes
            .saturating_add(estimate_record_batch_bytes(record));
        if sticky.bytes >= sticky_batch_size.max(1) {
            sticky = StickyPartitionState {
                partition: self.next_partition(topic, topic_metadata)?,
                bytes: RECORD_BATCH_OVERHEAD_BYTES,
            };
        }
        let _previous = self.sticky_by_topic.insert(topic.to_owned(), sticky);
        Ok(partition)
    }

    fn next_partition(&mut self, topic: &str, topic_metadata: &TopicMetadata) -> Result<i32> {
        ensure_partitions(topic, super::record::UNASSIGNED_PARTITION, topic_metadata)?;
        let partition_count = topic_metadata.partitions.len();
        let next_round_robin = self.next_for_topic(topic);
        let next = usize::try_from(*next_round_robin).unwrap_or(0);
        *next_round_robin = next_round_robin
            .checked_add(1)
            .filter(|value| *value >= 0)
            .unwrap_or(0);
        let offset = next.checked_rem(partition_count).unwrap_or(0);
        topic_metadata
            .partitions
            .get(offset)
            .map(|partition| partition.partition_index)
            .ok_or_else(|| ProducerError::UnknownPartition {
                topic: topic.to_owned(),
                partition: super::record::UNASSIGNED_PARTITION,
            })
    }

    fn topic_metadata<'a>(
        metadata: &'a crate::wire::ClusterMetadata,
        topic: &str,
        partition: i32,
    ) -> Result<&'a TopicMetadata> {
        metadata
            .topic(topic)
            .ok_or_else(|| ProducerError::UnknownTopic(topic.to_owned()))
            .and_then(|topic_metadata| {
                ensure_partitions(topic, partition, topic_metadata)?;
                Ok(topic_metadata)
            })
    }

    fn valid_sticky(
        &self,
        topic: &str,
        topic_metadata: &TopicMetadata,
    ) -> Option<StickyPartitionState> {
        self.sticky_by_topic.get(topic).copied().filter(|sticky| {
            topic_metadata
                .partitions
                .iter()
                .any(|partition| partition.partition_index == sticky.partition)
        })
    }
}

fn ensure_partitions(topic: &str, partition: i32, topic_metadata: &TopicMetadata) -> Result<()> {
    if topic_metadata.partitions.is_empty() {
        return Err(ProducerError::UnknownPartition {
            topic: topic.to_owned(),
            partition,
        });
    }
    Ok(())
}

fn key_partition(
    topic: &str,
    partition: i32,
    topic_metadata: &TopicMetadata,
    record: &super::ProducerRecord,
) -> Result<i32> {
    ensure_partitions(topic, partition, topic_metadata)?;
    let Some(key) = record.key.as_ref() else {
        return Err(ProducerError::UnknownPartition {
            topic: topic.to_owned(),
            partition,
        });
    };
    let partition_count = topic_metadata.partitions.len();
    let hash = usize::try_from(murmur2_java(key.as_ref()) & 0x7fff_ffff).unwrap_or(0);
    let offset = hash.checked_rem(partition_count).unwrap_or(0);
    topic_metadata
        .partitions
        .get(offset)
        .map(|partition_metadata| partition_metadata.partition_index)
        .ok_or_else(|| ProducerError::UnknownPartition {
            topic: topic.to_owned(),
            partition,
        })
}

impl ProducerIdempotenceState {
    fn next_sequence(&mut self, topic: &str, partition: i32, record_count: i32) -> Result<i32> {
        let key = TopicPartitionKey {
            topic: topic.to_owned(),
            partition,
        };
        let base_sequence = self.sequences.get(&key).copied().unwrap_or(0);
        let next_sequence = base_sequence.checked_add(record_count).ok_or_else(|| {
            ProducerError::SequenceOverflow {
                topic: topic.to_owned(),
                partition,
            }
        })?;
        let _previous = self.sequences.insert(key, next_sequence);
        Ok(base_sequence)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct TopicPartitionKey {
    topic: String,
    partition: i32,
}

#[derive(Debug)]
pub(crate) enum DispatchOutcome {
    Delivered(Result<Vec<ProduceReceipt>>),
    Requeue(Vec<ReadyBatch>),
}

enum DispatchError {
    Producer(ProducerError),
    Requeue,
    RetryableLeadership {
        topic: String,
        partition: i32,
        error: kacrab_protocol::generated::ErrorCode,
    },
}

impl From<ProducerError> for DispatchError {
    fn from(error: ProducerError) -> Self {
        Self::Producer(error)
    }
}

impl DispatchError {
    fn from_route(error: ProducerError) -> Self {
        match error {
            ProducerError::UnknownTopic(_)
            | ProducerError::UnknownPartition { .. }
            | ProducerError::LeaderNotFound { .. } => Self::Requeue,
            other => Self::Producer(other),
        }
    }
}

impl From<crate::wire::WireError> for DispatchError {
    fn from(error: crate::wire::WireError) -> Self {
        Self::Producer(ProducerError::from(error))
    }
}

#[derive(Debug, Default)]
struct BrokerProduceRequest {
    data: ProduceRequestData,
    routes: Vec<ProduceRoute>,
}

impl BrokerProduceRequest {
    fn contains_route(&self, route: &ProduceRoute) -> bool {
        self.routes.iter().any(|existing| {
            existing.topic_id == route.topic_id && existing.partition == route.partition
        })
    }

    fn push(
        &mut self,
        route: ProduceRoute,
        records: bytes::Bytes,
        options: BrokerProduceOptions<'_>,
    ) {
        self.data.acks = options.acks;
        self.data.timeout_ms = options.timeout_ms;
        self.data.transactional_id = options
            .transactional_id
            .map(|id| KafkaString::from(id.to_owned()));
        push_partition(&mut self.data.topic_data, &route, records);
        self.routes.push(route);
    }
}

async fn send_produce_with_backpressure_retry(
    wire: &WireClient,
    broker_id: i32,
    version: i16,
    request: &BrokerProduceRequest,
) -> crate::wire::Result<kacrab_protocol::generated::ProduceResponseData> {
    loop {
        match wire
            .send_to_broker::<_, kacrab_protocol::generated::ProduceResponseData>(
                broker_id,
                ApiKey::Produce,
                version,
                &request.data,
            )
            .await
        {
            Err(crate::wire::WireError::Backpressure) => tokio::task::yield_now().await,
            result => return result,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct BrokerProduceOptions<'a> {
    acks: i16,
    timeout_ms: i32,
    transactional_id: Option<&'a str>,
}

fn push_partition(topics: &mut Vec<TopicProduceData>, route: &ProduceRoute, records: bytes::Bytes) {
    let partition = PartitionProduceData {
        index: route.partition,
        records: Some(records),
        _unknown_tagged_fields: Vec::new(),
    };
    if let Some(topic) = topics
        .iter_mut()
        .find(|topic| topic.topic_id == route.topic_id)
    {
        topic.partition_data.push(partition);
        return;
    }
    topics.push(TopicProduceData {
        name: KafkaString::from(route.topic.clone()),
        topic_id: route.topic_id,
        partition_data: vec![partition],
        _unknown_tagged_fields: Vec::new(),
    });
}

fn unique_topics(batches: &[ReadyBatch]) -> Vec<String> {
    let mut topics = Vec::new();
    for batch in batches {
        if !topics.contains(&batch.topic) {
            topics.push(batch.topic.clone());
        }
    }
    topics
}

fn unique_unassigned_record_topics(records: &[super::ProducerRecord]) -> Vec<String> {
    let mut topics = Vec::new();
    for record in records {
        if record.has_assigned_partition()
            || topics.iter().any(|topic| topic == record.topic.as_ref())
        {
            continue;
        }
        topics.push(record.topic.to_string());
    }
    topics
}

fn complete_deliveries(batches: &mut [ReadyBatch], receipts: &[ProduceReceipt]) {
    let mut used_receipts = vec![false; receipts.len()];
    for batch in batches {
        let Some(sender) = batch.delivery.take() else {
            continue;
        };
        if !sender.has_receivers() {
            continue;
        }
        let Some((receipt_index, receipt)) =
            receipts.iter().enumerate().find(|(index, receipt)| {
                !used_receipts.get(*index).copied().unwrap_or(false)
                    && receipt.topic == batch.topic
                    && receipt.partition == batch.partition
            })
        else {
            continue;
        };
        if let Some(used) = used_receipts.get_mut(receipt_index) {
            *used = true;
        }
        sender.send(receipt.clone());
    }
}

const fn is_leadership_error(error: kacrab_protocol::generated::ErrorCode) -> bool {
    matches!(
        error,
        kacrab_protocol::generated::ErrorCode::LeaderNotAvailable
            | kacrab_protocol::generated::ErrorCode::NotLeaderOrFollower
            | kacrab_protocol::generated::ErrorCode::UnknownLeaderEpoch
            | kacrab_protocol::generated::ErrorCode::FencedLeaderEpoch
    )
}

/// Retry delay for coordinator discovery and producer-id initialization. Kafka
/// uses retry.backoff.ms defaults in public config; this private dispatcher
/// fallback is short so transactional smoke tests do not stall when the public
/// config path is not used.
const TRANSACTION_COORDINATOR_RETRY_BACKOFF: Duration = Duration::from_millis(50);
const PRODUCE_RETRY_BACKOFF: Duration = Duration::from_millis(1);
/// Kafka 4.3 closes `InitProducerId` v6 when two-phase commit is disabled, so
/// non-2PC producers cap negotiation at v5 until the broker-side behavior changes.
const NON_2PC_INIT_PRODUCER_ID_MAX_VERSION: i16 = 5;

fn init_producer_id_version(transaction_two_phase_commit: bool) -> i16 {
    if transaction_two_phase_commit {
        client_api_info(ApiKey::InitProducerId).max_version
    } else {
        client_api_info(ApiKey::InitProducerId)
            .max_version
            .min(NON_2PC_INIT_PRODUCER_ID_MAX_VERSION)
    }
}

fn choose_coordinator_addr(addresses: impl IntoIterator<Item = SocketAddr>) -> Option<SocketAddr> {
    let mut first = None;
    for address in addresses {
        if first.is_none() {
            first = Some(address);
        }
        if address.is_ipv4() {
            return Some(address);
        }
    }
    first
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::expect_used,
        clippy::missing_assert_message,
        clippy::unwrap_used,
        reason = "Unit test fixtures fail fastest with contextual unwrap/expect calls."
    )]

    use std::{
        net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
        time::{Duration, Instant},
    };

    use bytes::Bytes;
    use kacrab_protocol::{
        KafkaString, KafkaUuid,
        generated::{
            AddPartitionsToTxnPartitionResult, AddPartitionsToTxnResponseData,
            AddPartitionsToTxnResult, AddPartitionsToTxnTopicResult, ErrorCode,
        },
    };

    use super::{
        BrokerProduceOptions, BrokerProduceRequest, DispatchError, ProducerDispatcher,
        ProducerIdempotenceState, ProducerPartitionerState, TopicPartitionKey,
        choose_coordinator_addr, complete_deliveries, init_producer_id_version,
        is_leadership_error, unique_topics, unique_unassigned_record_topics,
    };
    use crate::{
        producer::{
            AccumulatorConfig, ProduceReceipt, ProducerError, ProducerIdempotenceConfig,
            ProducerIdentity, ProducerRecord, RecordAccumulator,
        },
        wire::{
            BrokerEndpoint, BrokerMetadata, ClusterMetadata, ConnectionConfig, PartitionMetadata,
            TopicMetadata, WireClient,
        },
    };

    fn test_wire() -> WireClient {
        WireClient::connect_with_brokers(
            ConnectionConfig::default(),
            "dispatcher-test",
            [BrokerEndpoint::new(
                7,
                "127.0.0.1:9092".parse().expect("valid socket address"),
            )],
        )
    }

    fn transactional_dispatcher() -> ProducerDispatcher {
        ProducerDispatcher::with_config(
            test_wire(),
            crate::producer::ProducerRuntimeConfig {
                idempotence: ProducerIdempotenceConfig {
                    enabled: true,
                    transactional_id: Some("txn-a".to_owned()),
                    transaction_timeout_ms: 30_000,
                    transaction_two_phase_commit: false,
                },
                ..crate::producer::ProducerRuntimeConfig::default()
            },
        )
    }

    fn route(topic: &str, partition: i32) -> super::ProduceRoute {
        super::ProduceRoute {
            topic: topic.to_owned(),
            partition,
            topic_id: KafkaUuid::ZERO,
            leader_id: 7,
        }
    }

    fn add_partitions_response(
        topic: &str,
        partition: i32,
        error: ErrorCode,
    ) -> AddPartitionsToTxnResponseData {
        AddPartitionsToTxnResponseData {
            results_by_transaction: vec![AddPartitionsToTxnResult {
                transactional_id: KafkaString::from("txn-a".to_owned()),
                topic_results: vec![AddPartitionsToTxnTopicResult {
                    name: KafkaString::from(topic.to_owned()),
                    results_by_partition: vec![AddPartitionsToTxnPartitionResult {
                        partition_index: partition,
                        partition_error_code: i16::from(error),
                        _unknown_tagged_fields: Vec::new(),
                    }],
                    _unknown_tagged_fields: Vec::new(),
                }],
                _unknown_tagged_fields: Vec::new(),
            }],
            ..AddPartitionsToTxnResponseData::default()
        }
    }

    fn metadata_with_partitions(topic: &str, partitions: usize) -> ClusterMetadata {
        let mut partition_metadata = Vec::with_capacity(partitions);
        for partition in 0..partitions {
            let partition_index = i32::try_from(partition).expect("test partition fits i32");
            partition_metadata.push(PartitionMetadata {
                partition_index,
                leader_id: 7,
                leader_epoch: 1,
                replica_nodes: vec![7],
                isr_nodes: vec![7],
                offline_replicas: Vec::new(),
            });
        }
        ClusterMetadata {
            cluster_id: Some("cluster-a".to_owned()),
            controller_id: 7,
            brokers: vec![BrokerMetadata {
                node_id: 7,
                host: "localhost".to_owned(),
                port: 9092,
                rack: None,
            }],
            topics: vec![TopicMetadata {
                name: topic.to_owned(),
                topic_id: KafkaUuid::ZERO,
                partitions: partition_metadata,
            }],
        }
    }

    #[test]
    fn default_partitioner_sticks_unkeyed_records_until_batch_budget() {
        let metadata = metadata_with_partitions("orders", 3);
        let mut state = ProducerPartitionerState::default();
        let record = ProducerRecord::unassigned("orders").value(Bytes::from_static(b"1234567890"));

        let partitions: Vec<_> = (0..5)
            .map(|_| {
                state
                    .partition_for_record(&metadata, &record, false, 128)
                    .expect("partition")
            })
            .collect();

        assert_eq!(partitions, vec![0, 0, 0, 0, 1]);
    }

    #[test]
    fn default_partitioner_hashes_keyed_records_with_java_murmur2() {
        let metadata = metadata_with_partitions("orders", 3);
        let mut state = ProducerPartitionerState::default();
        let record = ProducerRecord::unassigned("orders")
            .key(Bytes::from_static(b"customer-42"))
            .value(Bytes::from_static(b"1234567890"));
        let mut next_round_robin = 0;
        let expected = super::super::routing::partition_for_record(
            &metadata,
            &record,
            false,
            &mut next_round_robin,
        )
        .expect("keyed partition");

        for _ in 0..5 {
            assert_eq!(
                state
                    .partition_for_record(&metadata, &record, false, 256)
                    .expect("partition"),
                expected
            );
        }
    }

    #[test]
    fn ignore_keys_uses_sticky_default_partitioner() {
        let metadata = metadata_with_partitions("orders", 3);
        let mut state = ProducerPartitionerState::default();
        let record = ProducerRecord::unassigned("orders")
            .key(Bytes::from_static(b"customer-42"))
            .value(Bytes::from_static(b"1234567890"));

        let partitions: Vec<_> = (0..5)
            .map(|_| {
                state
                    .partition_for_record(&metadata, &record, true, 128)
                    .expect("partition")
            })
            .collect();

        assert_eq!(partitions, vec![0, 0, 0, 1, 1]);
    }

    #[test]
    fn unique_unassigned_record_topics_skips_assigned_records_and_duplicates() {
        let records = [
            ProducerRecord::new("orders", 0),
            ProducerRecord::unassigned("orders"),
            ProducerRecord::unassigned("payments"),
            ProducerRecord::unassigned("orders"),
            ProducerRecord::new("payments", 1),
        ];

        assert_eq!(
            unique_unassigned_record_topics(&records),
            vec!["orders".to_owned(), "payments".to_owned()]
        );
    }

    #[tokio::test]
    async fn retry_wait_returns_delivery_timeout_when_outer_deadline_expires() {
        let dispatcher =
            ProducerDispatcher::new(test_wire()).delivery_timeout(Duration::from_millis(1));
        let mut accumulator = RecordAccumulator::new(
            AccumulatorConfig::default()
                .batch_size(1)
                .buffer_memory(16 * 1024),
        );
        let now = Instant::now();
        accumulator
            .append_at(
                ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")),
                now,
            )
            .expect("append record");
        let batches = accumulator.drain_ready(now);

        let error = dispatcher
            .wait_before_retry(&batches)
            .await
            .expect("retry wait should expire delivery timeout");

        assert!(matches!(
            error,
            ProducerError::DeliveryTimeout { topic, partition }
                if topic == "orders" && partition == 0
        ));
    }

    fn ready_batch(topic: &str, partition: i32) -> super::ReadyBatch {
        let mut accumulator = RecordAccumulator::new(
            AccumulatorConfig::default()
                .batch_size(1)
                .linger(Duration::from_secs(1)),
        );
        accumulator
            .append_at(
                ProducerRecord::new(topic, partition).value(Bytes::from_static(b"value")),
                Instant::now(),
            )
            .expect("append test record");
        accumulator
            .drain_ready(Instant::now())
            .pop()
            .expect("ready batch")
    }

    #[tokio::test]
    async fn begin_transaction_rejects_invalid_state_transitions() {
        let non_transactional = ProducerDispatcher::new(test_wire());
        assert!(matches!(
            non_transactional.begin_transaction(),
            Err(ProducerError::TransactionalIdRequired)
        ));

        let dispatcher = transactional_dispatcher();
        assert!(matches!(
            dispatcher.begin_transaction(),
            Err(ProducerError::InvalidTransactionState(message))
                if message == "init_transactions must run before begin_transaction"
        ));

        {
            let mut state = dispatcher.producer_state.lock().await;
            state.identity = Some(ProducerIdentity {
                producer_id: 11,
                producer_epoch: 2,
            });
        }
        dispatcher
            .begin_transaction()
            .expect("identity allows opening transaction");
        assert!(matches!(
            dispatcher.begin_transaction(),
            Err(ProducerError::InvalidTransactionState(message))
                if message == "transaction is already open"
        ));
    }

    #[tokio::test]
    async fn end_transaction_requires_transactional_id_before_network_io() {
        let dispatcher = ProducerDispatcher::new(test_wire());

        let error = dispatcher
            .end_transaction(true)
            .await
            .expect_err("non-transactional dispatcher should fail locally");

        assert!(matches!(error, ProducerError::TransactionalIdRequired));
    }

    #[tokio::test]
    async fn end_transaction_rejects_missing_open_transaction_after_cached_identity() {
        let dispatcher = transactional_dispatcher();
        {
            let mut state = dispatcher.producer_state.lock().await;
            state.coordinator_id = Some(7);
            state.identity = Some(ProducerIdentity {
                producer_id: 11,
                producer_epoch: 2,
            });
        }

        assert!(matches!(
            dispatcher.end_transaction(false).await,
            Err(ProducerError::InvalidTransactionState(message))
                if message == "no transaction is open"
        ));
    }

    #[tokio::test]
    async fn dispatch_entrypoints_return_immediately_for_empty_inputs() {
        let dispatcher = ProducerDispatcher::new(test_wire());
        let mut accumulator = RecordAccumulator::new(AccumulatorConfig::default());

        assert!(
            dispatcher
                .dispatch_ready(&mut accumulator, Instant::now())
                .await
                .expect("empty ready")
                .is_empty()
        );
        assert!(
            dispatcher
                .dispatch_ready_batches(Vec::new(), Instant::now())
                .await
                .expect("empty batches")
                .is_empty()
        );
        assert!(
            dispatcher
                .dispatch_all(&mut accumulator)
                .await
                .expect("empty all")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn dispatch_ready_returns_producer_error_when_no_broker_is_available() {
        let wire = WireClient::connect_with_brokers(ConnectionConfig::default(), "client-a", []);
        let dispatcher = ProducerDispatcher::new(wire);
        let mut accumulator = RecordAccumulator::new(
            AccumulatorConfig::default()
                .batch_size(1)
                .buffer_memory(16 * 1024),
        );
        accumulator
            .append_at(
                ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")),
                Instant::now(),
            )
            .expect("append record");

        assert!(matches!(
            dispatcher
                .dispatch_ready(&mut accumulator, Instant::now())
                .await,
            Err(ProducerError::Wire(
                crate::wire::WireError::NoBrokerAvailable
            ))
        ));
    }

    #[tokio::test]
    async fn dispatch_drained_reports_local_delivery_timeout() {
        let dispatcher = ProducerDispatcher::new(test_wire()).delivery_timeout(Duration::ZERO);

        let outcome = dispatcher
            .dispatch_drained(vec![ready_batch("orders", 0)], Instant::now())
            .await;

        assert!(matches!(
            outcome,
            super::DispatchOutcome::Delivered(Err(ProducerError::DeliveryTimeout {
                topic,
                partition
            })) if topic == "orders" && partition == 0
        ));
    }

    #[tokio::test]
    async fn add_partition_to_transaction_handles_local_state_branches() {
        let route = route("orders", 0);
        ProducerDispatcher::new(test_wire())
            .add_partition_to_transaction(&route)
            .await
            .expect("non-transactional producer skips transaction partition registration");

        let dispatcher = transactional_dispatcher();
        assert!(matches!(
            dispatcher.add_partition_to_transaction(&route).await,
            Err(ProducerError::InvalidTransactionState(message))
                if message == "produce called outside an open transaction"
        ));

        {
            let mut state = dispatcher.producer_state.lock().await;
            state.in_transaction = true;
            let _inserted = state.transaction_partitions.insert(TopicPartitionKey {
                topic: "orders".to_owned(),
                partition: 0,
            });
        }
        dispatcher
            .add_partition_to_transaction(&route)
            .await
            .expect("already registered partition is a no-op");
    }

    #[tokio::test]
    async fn producer_batch_state_rejects_record_counts_above_i32() {
        let dispatcher = ProducerDispatcher::with_config(
            test_wire(),
            crate::producer::ProducerRuntimeConfig {
                idempotence: ProducerIdempotenceConfig {
                    enabled: true,
                    transactional_id: None,
                    transaction_timeout_ms: 30_000,
                    transaction_two_phase_commit: false,
                },
                ..crate::producer::ProducerRuntimeConfig::default()
            },
        );
        {
            let mut state = dispatcher.producer_state.lock().await;
            state.identity = Some(ProducerIdentity {
                producer_id: 11,
                producer_epoch: 2,
            });
        }
        let too_many_records = usize::try_from(i32::MAX)
            .expect("i32 max fits usize")
            .saturating_add(1);

        assert!(matches!(
            dispatcher
                .producer_batch_state(&route("orders", 0), too_many_records)
                .await,
            Err(ProducerError::SequenceOverflow { topic, partition })
                if topic == "orders" && partition == 0
        ));
    }

    #[test]
    fn add_partitions_response_reports_top_level_and_partition_errors() {
        let route = route("orders", 0);
        let response = AddPartitionsToTxnResponseData {
            error_code: i16::from(ErrorCode::CoordinatorNotAvailable),
            ..AddPartitionsToTxnResponseData::default()
        };
        assert!(matches!(
            ProducerDispatcher::check_add_partitions_response(response, &route),
            Err(ProducerError::Transaction {
                operation: "add_partitions_to_txn",
                error: ErrorCode::CoordinatorNotAvailable
            })
        ));

        let response = add_partitions_response("orders", 0, ErrorCode::InvalidTxnState);
        assert!(matches!(
            ProducerDispatcher::check_add_partitions_response(response, &route),
            Err(ProducerError::Transaction {
                operation: "add_partitions_to_txn",
                error: ErrorCode::InvalidTxnState
            })
        ));
    }

    #[test]
    fn add_partitions_response_ignores_unmatched_topics_and_partitions() {
        let route = route("orders", 0);

        ProducerDispatcher::check_add_partitions_response(
            add_partitions_response("payments", 0, ErrorCode::InvalidTxnState),
            &route,
        )
        .expect("unmatched topic is not this route's failure");
        ProducerDispatcher::check_add_partitions_response(
            add_partitions_response("orders", 1, ErrorCode::InvalidTxnState),
            &route,
        )
        .expect("unmatched partition is not this route's failure");
    }

    #[test]
    fn idempotence_state_tracks_sequences_and_reports_overflow() {
        let mut state = ProducerIdempotenceState::default();

        assert_eq!(
            state.next_sequence("orders", 0, 3).expect("first sequence"),
            0
        );
        assert_eq!(
            state
                .next_sequence("orders", 0, 2)
                .expect("second sequence"),
            3
        );

        let key = TopicPartitionKey {
            topic: "orders".to_owned(),
            partition: 0,
        };
        let _previous = state.sequences.insert(key, i32::MAX);
        assert!(matches!(
            state.next_sequence("orders", 0, 1),
            Err(ProducerError::SequenceOverflow { topic, partition })
                if topic == "orders" && partition == 0
        ));
    }

    #[test]
    fn dispatch_error_classifies_route_and_wire_failures() {
        assert!(matches!(
            DispatchError::from_route(ProducerError::UnknownTopic("orders".to_owned())),
            DispatchError::Requeue
        ));
        assert!(matches!(
            DispatchError::from_route(ProducerError::Backpressure),
            DispatchError::Producer(ProducerError::Backpressure)
        ));
        assert!(matches!(
            DispatchError::from(crate::wire::WireError::UnknownBroker(42)),
            DispatchError::Producer(ProducerError::Wire(crate::wire::WireError::UnknownBroker(
                42
            )))
        ));
        assert!(matches!(
            DispatchError::from(ProducerError::Backpressure),
            DispatchError::Producer(ProducerError::Backpressure)
        ));
    }

    #[test]
    fn broker_produce_request_groups_partitions_by_topic_id() {
        let mut request = BrokerProduceRequest::default();
        request.push(
            route("orders", 0),
            Bytes::from_static(b"records-0"),
            BrokerProduceOptions {
                acks: -1,
                timeout_ms: 1_500,
                transactional_id: Some("txn-a"),
            },
        );
        request.push(
            route("orders", 1),
            Bytes::from_static(b"records-1"),
            BrokerProduceOptions {
                acks: -1,
                timeout_ms: 1_500,
                transactional_id: Some("txn-a"),
            },
        );

        assert_eq!(request.data.acks, -1);
        assert_eq!(request.data.timeout_ms, 1_500);
        assert_eq!(
            request.data.transactional_id,
            Some(KafkaString::from("txn-a".to_owned()))
        );
        assert_eq!(request.data.topic_data.len(), 1);
        let topic = request.data.topic_data.first().expect("topic group");
        assert_eq!(topic.partition_data.len(), 2);
        assert_eq!(request.routes.len(), 2);
    }

    #[test]
    fn broker_produce_request_detects_duplicate_partition_route() {
        let mut request = BrokerProduceRequest::default();
        let options = BrokerProduceOptions {
            acks: 1,
            timeout_ms: 1_500,
            transactional_id: None,
        };
        let route = route("orders", 0);

        assert!(!request.contains_route(&route));
        request.push(route.clone(), Bytes::from_static(b"a"), options);

        let topic = request.data.topic_data.first().expect("topic group");
        assert_eq!(topic.partition_data.len(), 1);
        assert_eq!(request.routes.len(), 1);
        assert!(request.contains_route(&route));
    }

    #[test]
    fn unique_topics_keeps_first_seen_order() {
        let batches = vec![
            ready_batch("orders", 0),
            ready_batch("payments", 0),
            ready_batch("orders", 1),
        ];

        assert_eq!(unique_topics(&batches), ["orders", "payments"]);
    }

    #[tokio::test]
    async fn complete_deliveries_skips_missing_receivers_and_missing_receipts() {
        let mut accumulator = RecordAccumulator::new(
            AccumulatorConfig::default()
                .batch_size(1)
                .linger(Duration::from_secs(1)),
        );
        let dropped_delivery = accumulator
            .append_for_delivery(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")))
            .expect("append for delivery");
        accumulator
            .append_at(
                ProducerRecord::new("payments", 0).value(Bytes::from_static(b"b")),
                Instant::now(),
            )
            .expect("append without delivery");
        let mut batches = accumulator.drain_ready(Instant::now());

        complete_deliveries(&mut batches, &[]);
        assert!(matches!(
            dropped_delivery.await,
            Err(ProducerError::DeliveryDropped)
        ));

        let mut accumulator = RecordAccumulator::new(
            AccumulatorConfig::default()
                .batch_size(1)
                .linger(Duration::from_secs(1)),
        );
        let delivery = accumulator
            .append_for_delivery(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")))
            .expect("append for delivery");
        let mut batches = accumulator.drain_ready(Instant::now());
        let receipt = ProduceReceipt {
            topic: "orders".to_owned(),
            partition: 0,
            leader_id: 7,
            base_offset: 9,
            log_append_time_ms: -1,
        };
        complete_deliveries(&mut batches, std::slice::from_ref(&receipt));

        assert_eq!(delivery.await.expect("delivered receipt"), receipt);
    }

    #[test]
    fn leadership_errors_match_retryable_metadata_failures() {
        assert!(is_leadership_error(ErrorCode::LeaderNotAvailable));
        assert!(is_leadership_error(ErrorCode::NotLeaderOrFollower));
        assert!(is_leadership_error(ErrorCode::UnknownLeaderEpoch));
        assert!(is_leadership_error(ErrorCode::FencedLeaderEpoch));
        assert!(!is_leadership_error(ErrorCode::InvalidTxnState));
    }

    #[test]
    fn init_producer_id_version_caps_non_2pc_requests() {
        assert_eq!(init_producer_id_version(false), 5);
        assert!(
            init_producer_id_version(true) >= init_producer_id_version(false),
            "2PC path should use negotiated maximum"
        );
    }

    #[test]
    fn coordinator_lookup_prefers_ipv4_when_localhost_resolves_to_both() {
        let ipv6 = SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 9092);
        let ipv4 = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9092);

        assert_eq!(choose_coordinator_addr([ipv6, ipv4]), Some(ipv4));
    }

    #[test]
    fn coordinator_lookup_uses_first_address_when_ipv4_is_absent() {
        let ipv6 = SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 9092);

        assert_eq!(choose_coordinator_addr([ipv6]), Some(ipv6));
        assert_eq!(choose_coordinator_addr([]), None);
    }
}
