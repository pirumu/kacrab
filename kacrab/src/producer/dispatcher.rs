//! Producer dispatcher that routes ready batches through the wire client.

use std::{
    collections::VecDeque,
    net::SocketAddr,
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use ahash::{AHashMap, AHashSet};
use bytes::BytesMut;
use kacrab_protocol::{
    KafkaString,
    generated::{
        AddOffsetsToTxnRequestData, AddOffsetsToTxnResponseData, AddPartitionsToTxnRequestData,
        AddPartitionsToTxnResponseData, AddPartitionsToTxnTopic, AddPartitionsToTxnTransaction,
        ApiKey, EndTxnRequestData, EndTxnResponseData, ErrorCode, FindCoordinatorRequestData,
        FindCoordinatorResponseData, InitProducerIdRequestData, InitProducerIdResponseData,
        PartitionProduceData, ProduceRequestData, TopicProduceData, TxnOffsetCommitRequestData,
        TxnOffsetCommitRequestPartition, TxnOffsetCommitRequestTopic, TxnOffsetCommitResponseData,
    },
    version::client_api_info,
};
use tokio::{
    sync::{Mutex, Notify},
    task::JoinSet,
};

use super::{
    accumulator::{
        RECORD_BATCH_OVERHEAD_BYTES, ReadyBatch, RecordAccumulator, estimate_record_batch_bytes,
    },
    api::{ConsumerGroupMetadata, OffsetAndMetadata, TopicPartition},
    batch::encode_record_batch_with_producer_state_at_offset,
    config::{
        ACKS_NONE, DEFAULT_DELIVERY_TIMEOUT, DEFAULT_TIMEOUT_MS, DEFAULT_TRANSACTION_TIMEOUT_MS,
        ProducerCompression, ProducerIdempotenceConfig, ProducerRuntimeConfig,
    },
    error::{ProducerError, Result},
    metrics::{ProducerMetrics, ProducerMetricsSnapshot},
    record::RecordMetadata,
    response::{ProduceBrokerError, ProduceReceiptError, produce_receipts_with_error_details},
    routing::{ProduceRoute, murmur2_java, route},
    transaction::{ProducerBatchState, ProducerIdentity, TransactionState},
};
use crate::wire::{BrokerEndpoint, RequestMessage, TopicMetadata, WireClient};

/// Dispatcher-only fallback for tests/manual construction. Public producer
/// configs still default to `acks=all`; this keeps `ProducerDispatcher::new`
/// compatible with earlier unit tests that modeled leader-only acknowledgements.
const DEFAULT_DISPATCHER_ACKS: i16 = 1;
const PENDING_TRANSACTION_OPERATION_MESSAGE: &str =
    "previous transaction operation is pending and must be retried";

/// Dispatches ready accumulator batches to broker leaders through [`WireClient`].
#[derive(Debug, Clone)]
pub struct ProducerDispatcher {
    wire: WireClient,
    acks: i16,
    timeout_ms: i32,
    retry_attempts: usize,
    delivery_timeout: Duration,
    compression: ProducerCompression,
    max_request_size: usize,
    max_in_flight_requests_per_connection: usize,
    partitioner_ignore_keys: bool,
    partitioner_adaptive_partitioning_enable: bool,
    partitioner_availability_timeout: Duration,
    partition_sticky_batch_size: usize,
    idempotence: ProducerIdempotenceConfig,
    producer_state: Arc<Mutex<ProducerIdempotenceState>>,
    producer_identity_init: Arc<Mutex<()>>,
    produce_enqueue_order: Arc<Mutex<()>>,
    partitioner_state: Arc<Mutex<ProducerPartitionerState>>,
    metrics: ProducerMetrics,
    metrics_enabled: Arc<AtomicBool>,
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
            max_request_size: super::config::DEFAULT_MAX_REQUEST_SIZE,
            max_in_flight_requests_per_connection:
                crate::wire::DEFAULT_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
            partitioner_ignore_keys: false,
            partitioner_adaptive_partitioning_enable: true,
            partitioner_availability_timeout: Duration::ZERO,
            partition_sticky_batch_size: super::AccumulatorConfig::default().batch_size,
            idempotence: ProducerIdempotenceConfig {
                enabled: false,
                transactional_id: None,
                transaction_timeout_ms: DEFAULT_TRANSACTION_TIMEOUT_MS,
                transaction_two_phase_commit: false,
            },
            producer_state: Arc::new(Mutex::new(ProducerIdempotenceState::default())),
            producer_identity_init: Arc::new(Mutex::new(())),
            produce_enqueue_order: Arc::new(Mutex::new(())),
            partitioner_state: Arc::new(Mutex::new(ProducerPartitionerState::default())),
            metrics: ProducerMetrics::default(),
            metrics_enabled: Arc::new(AtomicBool::new(false)),
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
            max_request_size: config.max_request_size,
            max_in_flight_requests_per_connection: config.max_in_flight_requests_per_connection,
            partitioner_ignore_keys: config.partitioner_ignore_keys,
            partitioner_adaptive_partitioning_enable: config
                .partitioner_adaptive_partitioning_enable,
            partitioner_availability_timeout: config.partitioner_availability_timeout,
            partition_sticky_batch_size: config.accumulator.batch_size,
            idempotence: config.idempotence,
            producer_state: Arc::new(Mutex::new(ProducerIdempotenceState::default())),
            producer_identity_init: Arc::new(Mutex::new(())),
            produce_enqueue_order: Arc::new(Mutex::new(())),
            partitioner_state: Arc::new(Mutex::new(ProducerPartitionerState::default())),
            metrics: ProducerMetrics::default(),
            metrics_enabled: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Point-in-time producer dispatch metrics snapshot.
    #[must_use]
    pub fn metrics(&self) -> ProducerMetricsSnapshot {
        self.metrics.snapshot(0, 0, 0)
    }

    pub(crate) fn metrics_handle(&self) -> ProducerMetrics {
        self.metrics.clone()
    }

    pub(crate) async fn fail_if_transaction_error(&self) -> Result<()> {
        if self.idempotence.transactional_id.is_none() {
            return Ok(());
        }
        let state = self.producer_state.lock().await;
        fail_transaction_state_if_needed(&state, false)
    }

    pub(crate) async fn fail_if_fatal_transaction_error_for_abort(&self) -> Result<()> {
        if self.idempotence.transactional_id.is_none() {
            return Ok(());
        }
        let state = self.producer_state.lock().await;
        fail_transaction_state_if_needed(&state, true)
    }

    pub(crate) fn validate_commit_transaction_start(&self) -> Result<()> {
        if self.idempotence.transactional_id.is_none() {
            return Err(ProducerError::TransactionalIdRequired);
        }
        let mut state = self
            .producer_state
            .try_lock()
            .map_err(|_error| ProducerError::TransactionStateBusy)?;
        fail_transaction_state_if_needed(&state, false)?;
        fail_pending_transaction_operation(&mut state)?;
        if !state.in_transaction {
            return Err(ProducerError::InvalidTransactionState(
                "no transaction is open",
            ));
        }
        if state.identity.is_none() {
            return Err(ProducerError::InvalidTransactionState(
                "init_transactions must run before end_transaction",
            ));
        }
        drop(state);
        Ok(())
    }

    pub(crate) async fn pending_end_transaction_matches(&self, committed: bool) -> Result<bool> {
        if self.idempotence.transactional_id.is_none() {
            return Err(ProducerError::TransactionalIdRequired);
        }
        let mut state = self.producer_state.lock().await;
        clear_acked_pending_transaction_operation(&mut state);
        match state.pending_operation {
            Some(TransactionOperation::EndTransaction {
                committed: pending_committed,
            }) if pending_committed == committed => Ok(true),
            Some(_operation) => Err(ProducerError::InvalidTransactionState(
                PENDING_TRANSACTION_OPERATION_MESSAGE,
            )),
            None => Ok(false),
        }
    }

    pub(crate) async fn mark_init_transactions_timed_out(&self) {
        self.mark_pending_transaction_operation_timed_out(TransactionOperation::InitTransactions)
            .await;
    }

    pub(crate) async fn mark_send_offsets_to_transaction_timed_out(&self) {
        self.mark_pending_transaction_operation_timed_out(
            TransactionOperation::SendOffsetsToTransaction,
        )
        .await;
    }

    pub(crate) async fn mark_end_transaction_timed_out(&self, committed: bool) {
        self.mark_pending_transaction_operation_timed_out(TransactionOperation::EndTransaction {
            committed,
        })
        .await;
    }

    async fn mark_pending_transaction_operation_timed_out(&self, operation: TransactionOperation) {
        if self.idempotence.transactional_id.is_none() {
            return;
        }
        let mut state = self.producer_state.lock().await;
        if state.pending_operation == Some(operation) {
            state.pending_operation_status = PendingTransactionOperationStatus::TimedOut;
        }
    }

    #[cfg(test)]
    pub(crate) async fn set_abortable_transaction_error_for_test(&self, error: ErrorCode) {
        let mut state = self.producer_state.lock().await;
        state.abortable_error = Some(error);
    }

    #[cfg(test)]
    pub(crate) async fn set_open_transaction_for_test(
        &self,
        identity: ProducerIdentity,
        transaction_started: bool,
    ) {
        let mut state = self.producer_state.lock().await;
        state.identity = Some(identity);
        state.in_transaction = true;
        state.transaction_state = TransactionState::InTransaction;
        state.transaction_started = transaction_started;
    }

    pub(crate) fn any_broker_id(&self) -> Result<i32> {
        self.wire.any_broker_id().map_err(ProducerError::from)
    }

    pub(crate) async fn send_control_request<Req, Resp>(
        &self,
        broker_id: i32,
        api_key: ApiKey,
        api_version: i16,
        request: &Req,
    ) -> Result<Resp>
    where
        Req: RequestMessage + Clone + Send + Sync + 'static,
        Resp: crate::wire::ResponseMessage,
    {
        self.wire
            .send_to_broker(broker_id, api_key, api_version, request)
            .await
            .map_err(ProducerError::from)
    }

    /// Enable dispatch metrics that require per-request accounting.
    pub fn enable_metrics(&self) {
        self.metrics_enabled.store(true, Ordering::Relaxed);
    }

    fn metrics_are_enabled(&self) -> bool {
        self.metrics_enabled.load(Ordering::Relaxed)
    }

    pub(crate) async fn prepare_drained_batches(&self, batches: &mut [ReadyBatch]) -> Result<()> {
        if !self.idempotence.enabled && self.idempotence.transactional_id.is_none() {
            return Ok(());
        }
        let topics = unique_topics(batches);
        let metadata = self
            .wire
            .metadata_for_topics(topics.iter().map(String::as_str))
            .await?;
        for batch in batches {
            let Some(first_record) = batch.records.first().cloned() else {
                continue;
            };
            let route = self
                .route_for_batch(&metadata, batch, &first_record)
                .await?;
            self.add_partition_to_transaction(&route).await?;
            if batch.producer_state.is_none() {
                batch.producer_state = self
                    .producer_batch_state(&route, batch.records.len())
                    .await?;
            }
        }
        Ok(())
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
                self.partitioner_adaptive_partitioning_enable,
                self.partition_sticky_batch_size,
            )?
        };
        record.partition = partition;
        Ok(())
    }

    /// Assign a concrete partition while allowing the default sticky partitioner
    /// to reuse its current sticky partition without a metadata lookup.
    pub(crate) async fn assign_partition_with_accumulator(
        &self,
        accumulator: &RecordAccumulator,
        record: &mut super::ProducerRecord,
    ) -> Result<()> {
        if record.has_assigned_partition() {
            return Ok(());
        }
        if self.try_assign_cached_sticky_partition(record).await {
            return Ok(());
        }
        let metadata = self
            .wire
            .metadata_for_topics([record.topic.as_ref()])
            .await?;
        let partition = {
            let mut state = self.partitioner_state.lock().await;
            if self.uses_sticky_partitioner(record) {
                let topic_metadata = metadata
                    .topic(record.topic.as_ref())
                    .ok_or_else(|| ProducerError::UnknownTopic(record.topic.to_string()))?;
                state.update_partition_load_stats_from_accumulator_at(PartitionLoadRefresh {
                    topic: record.topic.as_ref(),
                    topic_metadata,
                    accumulator,
                    now: Instant::now(),
                    availability_timeout: self.partitioner_availability_timeout,
                });
            }
            state.partition_for_record(
                &metadata,
                record,
                self.partitioner_ignore_keys,
                self.partitioner_adaptive_partitioning_enable,
                self.partition_sticky_batch_size,
            )?
        };
        record.partition = partition;
        Ok(())
    }

    /// Assign a concrete partition using an already-fetched metadata snapshot.
    pub(crate) async fn assign_partition_with_metadata(
        &self,
        metadata: &crate::wire::ClusterMetadata,
        record: &mut super::ProducerRecord,
    ) -> Result<()> {
        if record.has_assigned_partition() {
            return Ok(());
        }
        let partition = {
            let mut state = self.partitioner_state.lock().await;
            state.partition_for_record(
                metadata,
                record,
                self.partitioner_ignore_keys,
                self.partitioner_adaptive_partitioning_enable,
                self.partition_sticky_batch_size,
            )?
        };
        record.partition = partition;
        Ok(())
    }

    pub(crate) const fn uses_sticky_partitioner(&self, record: &super::ProducerRecord) -> bool {
        !record.has_assigned_partition() && (self.partitioner_ignore_keys || record.key.is_none())
    }

    async fn try_assign_cached_sticky_partition(&self, record: &mut super::ProducerRecord) -> bool {
        if !self.uses_sticky_partitioner(record) {
            return false;
        }
        let mut state = self.partitioner_state.lock().await;
        state.try_assign_cached_sticky_partition(record, self.partition_sticky_batch_size)
    }

    pub(crate) async fn mark_sticky_batch_ready(&self, topic: &str) {
        let mut state = self.partitioner_state.lock().await;
        state.mark_sticky_batch_ready(topic, self.partition_sticky_batch_size);
    }

    /// Fetch cached or refreshed metadata for the supplied topic set.
    pub async fn metadata_for_topics<I, S>(
        &self,
        topics: I,
    ) -> Result<Arc<crate::wire::ClusterMetadata>>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.wire
            .metadata_for_topics(topics)
            .await
            .map_err(Into::into)
    }

    /// Assign concrete partitions to a batch using one metadata snapshot.
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
                    adaptive: self.partitioner_adaptive_partitioning_enable,
                    sticky_batch_size: self.partition_sticky_batch_size,
                },
                records,
            )?;
        }
        drop(state);
        Ok(())
    }

    /// Assign concrete partitions to a batch using a caller-provided metadata snapshot.
    #[cfg(test)]
    pub(crate) async fn assign_partitions_with_metadata(
        &self,
        metadata: &crate::wire::ClusterMetadata,
        records: &mut [super::ProducerRecord],
    ) -> Result<()> {
        let topics = unique_unassigned_record_topics(records);
        if topics.is_empty() {
            return Ok(());
        }
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
                    adaptive: self.partitioner_adaptive_partitioning_enable,
                    sticky_batch_size: self.partition_sticky_batch_size,
                },
                records,
            )?;
        }
        drop(state);
        Ok(())
    }

    #[cfg(test)]
    pub(crate) async fn assign_topic_partitions_with_metadata(
        &self,
        metadata: &crate::wire::ClusterMetadata,
        topic: &str,
        records: &mut [super::ProducerRecord],
    ) -> Result<()> {
        let topic_metadata = metadata
            .topic(topic)
            .ok_or_else(|| ProducerError::UnknownTopic(topic.to_owned()))?;
        let mut state = self.partitioner_state.lock().await;
        state.assign_topic_partitions(
            TopicPartitionAssignment {
                topic,
                topic_metadata,
                ignore_keys: self.partitioner_ignore_keys,
                adaptive: self.partitioner_adaptive_partitioning_enable,
                sticky_batch_size: self.partition_sticky_batch_size,
            },
            records,
        )?;
        drop(state);
        Ok(())
    }

    #[cfg(test)]
    pub(crate) async fn assign_sticky_topic_partitions_with_metadata(
        &self,
        metadata: &crate::wire::ClusterMetadata,
        topic: &str,
        records: &mut [super::ProducerRecord],
    ) -> Result<()> {
        let topic_metadata = metadata
            .topic(topic)
            .ok_or_else(|| ProducerError::UnknownTopic(topic.to_owned()))?;
        let mut state = self.partitioner_state.lock().await;
        state.assign_sticky_topic_partitions(
            TopicPartitionAssignment {
                topic,
                topic_metadata,
                ignore_keys: self.partitioner_ignore_keys,
                adaptive: self.partitioner_adaptive_partitioning_enable,
                sticky_batch_size: self.partition_sticky_batch_size,
            },
            records,
        )?;
        drop(state);
        Ok(())
    }

    /// Refresh adaptive sticky partition load stats from the current accumulator queues.
    pub async fn refresh_partition_load_stats<I, S>(
        &self,
        accumulator: &RecordAccumulator,
        topics: I,
    ) -> Result<()>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let topics: Vec<String> = topics
            .into_iter()
            .map(|topic| topic.as_ref().to_owned())
            .collect();
        if topics.is_empty() {
            return Ok(());
        }
        let metadata = self
            .wire
            .metadata_for_topics(topics.iter().map(String::as_str))
            .await?;
        self.refresh_partition_load_stats_with_metadata(
            accumulator,
            &metadata,
            topics.iter().map(String::as_str),
        )
        .await
    }

    /// Refresh adaptive sticky partition load stats using a caller-provided metadata snapshot.
    pub(crate) async fn refresh_partition_load_stats_with_metadata<I, S>(
        &self,
        accumulator: &RecordAccumulator,
        metadata: &crate::wire::ClusterMetadata,
        topics: I,
    ) -> Result<()>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let topics: Vec<String> = topics
            .into_iter()
            .map(|topic| topic.as_ref().to_owned())
            .collect();
        if topics.is_empty() {
            return Ok(());
        }
        let mut state = self.partitioner_state.lock().await;
        for topic in topics {
            let topic_metadata = metadata
                .topic(&topic)
                .ok_or_else(|| ProducerError::UnknownTopic(topic.clone()))?;
            state.update_partition_load_stats_from_accumulator_at(PartitionLoadRefresh {
                topic: &topic,
                topic_metadata,
                accumulator,
                now: Instant::now(),
                availability_timeout: self.partitioner_availability_timeout,
            });
        }
        drop(state);
        Ok(())
    }

    #[cfg(test)]
    pub(crate) async fn refresh_topic_load_stats_with_metadata(
        &self,
        accumulator: &RecordAccumulator,
        metadata: &crate::wire::ClusterMetadata,
        topic: &str,
    ) -> Result<()> {
        let topic_metadata = metadata
            .topic(topic)
            .ok_or_else(|| ProducerError::UnknownTopic(topic.to_owned()))?;
        let mut state = self.partitioner_state.lock().await;
        state.update_partition_load_stats_from_accumulator_at(PartitionLoadRefresh {
            topic,
            topic_metadata,
            accumulator,
            now: Instant::now(),
            availability_timeout: self.partitioner_availability_timeout,
        });
        drop(state);
        Ok(())
    }

    async fn record_broker_drain_started(&self, broker_id: i32, now: Instant) {
        if self.partitioner_availability_timeout.is_zero() {
            return;
        }
        let mut state = self.partitioner_state.lock().await;
        state.record_broker_drain_started(broker_id, now);
    }

    async fn record_broker_drain_finished(&self, broker_id: i32, now: Instant) {
        if self.partitioner_availability_timeout.is_zero() {
            return;
        }
        let mut state = self.partitioner_state.lock().await;
        state.record_broker_drain_finished(broker_id, now);
    }

    /// Initialize a transactional producer id through the transaction coordinator.
    pub async fn init_transactions(&self) -> Result<()> {
        let operation = TransactionOperation::InitTransactions;
        let pending_result = {
            let mut state = self.producer_state.lock().await;
            fail_transaction_state_if_needed(&state, false)?;
            clear_acked_pending_transaction_operation(&mut state);
            if state.pending_operation.is_some() {
                match state.begin_pending_transaction_operation(operation)? {
                    TransactionPendingOperationStart::Started(_result) => {
                        return Err(ProducerError::InvalidTransactionState(
                            PENDING_TRANSACTION_OPERATION_MESSAGE,
                        ));
                    },
                    TransactionPendingOperationStart::Cached(result) => {
                        drop(state);
                        return result.wait().await;
                    },
                }
            }
            if state.identity.is_some() {
                return Err(ProducerError::InvalidTransactionState(
                    "init_transactions must only run once",
                ));
            }
            state.transition_to(TransactionState::Initializing)?;
            match state.begin_pending_transaction_operation(operation)? {
                TransactionPendingOperationStart::Started(result) => result,
                TransactionPendingOperationStart::Cached(result) => {
                    drop(state);
                    return result.wait().await;
                },
            }
        };
        let mut pending_operation = PendingTransactionOperationGuard::new(
            Arc::clone(&self.producer_state),
            operation,
            pending_result,
        );
        let result = async {
            let coordinator_id = self.coordinator_id().await?;
            let _identity = self.producer_identity(coordinator_id).await?;
            Ok(())
        }
        .await;
        pending_operation.complete(&result).await;
        result
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
        fail_pending_transaction_operation(&mut state)?;
        fail_transaction_state_if_needed(&state, false)?;
        if state.identity.is_none() {
            return Err(ProducerError::InvalidTransactionState(
                "init_transactions must run before begin_transaction",
            ));
        }
        if state.transaction_state == TransactionState::Uninitialized {
            state.transaction_state = TransactionState::Ready;
        }
        if state.transaction_state == TransactionState::InTransaction || state.in_transaction {
            return Err(ProducerError::InvalidTransactionState(
                "transaction is already open",
            ));
        }
        state.transition_to(TransactionState::InTransaction)?;
        state.in_transaction = true;
        state.transaction_started = false;
        state.new_partitions_in_transaction.clear();
        state.pending_partitions_in_transaction.clear();
        state.partitions_in_transaction.clear();
        state.transaction_partitions.clear();
        drop(state);
        Ok(())
    }

    /// Add consumer offsets to the currently open transaction.
    pub async fn send_offsets_to_transaction<I>(
        &self,
        offsets: I,
        group_metadata: ConsumerGroupMetadata,
    ) -> Result<()>
    where
        I: IntoIterator<Item = (TopicPartition, OffsetAndMetadata)>,
    {
        let offsets: Vec<_> = offsets.into_iter().collect();
        validate_consumer_group_metadata(&group_metadata)?;
        let transactional_id = self
            .idempotence
            .transactional_id
            .as_ref()
            .ok_or(ProducerError::TransactionalIdRequired)?;
        if offsets.is_empty() {
            return Ok(());
        }
        let pending_result = {
            let mut state = self
                .producer_state
                .try_lock()
                .map_err(|_error| ProducerError::TransactionStateBusy)?;
            fail_transaction_state_if_needed(&state, false)?;
            if state.identity.is_none() {
                return Err(ProducerError::InvalidTransactionState(
                    "init_transactions must run before send_offsets_to_transaction",
                ));
            }
            if state.transaction_state != TransactionState::InTransaction && !state.in_transaction {
                return Err(ProducerError::InvalidTransactionState(
                    "cannot send offsets outside an open transaction",
                ));
            }
            match state.begin_pending_transaction_operation(
                TransactionOperation::SendOffsetsToTransaction,
            )? {
                TransactionPendingOperationStart::Started(result) => result,
                TransactionPendingOperationStart::Cached(result) => {
                    drop(state);
                    return result.wait().await;
                },
            }
        };
        let mut pending_operation = PendingTransactionOperationGuard::new(
            Arc::clone(&self.producer_state),
            TransactionOperation::SendOffsetsToTransaction,
            pending_result,
        );
        let result = async {
            let transaction_coordinator_id = self.coordinator_id().await?;
            let identity = self.producer_identity(transaction_coordinator_id).await?;

            if self.idempotence.transaction_two_phase_commit {
                let mut state = self.producer_state.lock().await;
                state.transaction_started = true;
                drop(state);
            } else {
                self.add_offsets_to_transaction(
                    transaction_coordinator_id,
                    transactional_id,
                    identity,
                    &group_metadata,
                )
                .await?;
            }
            let group_coordinator_id =
                match self.group_coordinator_id(&group_metadata.group_id).await {
                    Ok(group_coordinator_id) => group_coordinator_id,
                    Err(ProducerError::Transaction { operation, error }) => {
                        self.record_transaction_error(error).await;
                        return Err(ProducerError::Transaction { operation, error });
                    },
                    Err(error) => return Err(error),
                };
            self.txn_offset_commit(
                group_coordinator_id,
                transactional_id,
                identity,
                offsets,
                group_metadata,
            )
            .await
        }
        .await;
        pending_operation.complete(&result).await;
        result
    }

    /// End the currently open transaction.
    #[expect(
        clippy::too_many_lines,
        reason = "EndTxn retry, coordinator refresh, epoch bump, and transaction error \
                  classification stay together to mirror Java TransactionManager ordering."
    )]
    pub async fn end_transaction(&self, committed: bool) -> Result<()> {
        let transactional_id = self
            .idempotence
            .transactional_id
            .as_ref()
            .ok_or(ProducerError::TransactionalIdRequired)?;
        let operation = TransactionOperation::EndTransaction { committed };
        let (identity, pending_result) = {
            let mut state = self
                .producer_state
                .try_lock()
                .map_err(|_error| ProducerError::TransactionStateBusy)?;
            fail_transaction_state_if_needed(&state, !committed)?;
            if !state.in_transaction {
                return Err(ProducerError::InvalidTransactionState(
                    "no transaction is open",
                ));
            }
            let identity = state
                .identity
                .ok_or(ProducerError::InvalidTransactionState(
                    "init_transactions must run before end_transaction",
                ))?;
            if !state.transaction_started {
                drop(state);
                self.complete_empty_transaction_after_end(committed).await?;
                return Ok(());
            }
            let pending_result = match state.begin_pending_transaction_operation(operation)? {
                TransactionPendingOperationStart::Started(result) => result,
                TransactionPendingOperationStart::Cached(result) => {
                    drop(state);
                    return result.wait().await;
                },
            };
            state.transition_to(if committed {
                TransactionState::CommittingTransaction
            } else {
                TransactionState::AbortingTransaction
            })?;
            drop(state);
            (identity, pending_result)
        };
        let mut pending_operation = PendingTransactionOperationGuard::new(
            Arc::clone(&self.producer_state),
            operation,
            pending_result,
        );
        let result = async {
            let mut coordinator_id = self.coordinator_id().await?;
            let request = EndTxnRequestData {
                transactional_id: KafkaString::from(transactional_id.clone()),
                producer_id: identity.producer_id,
                producer_epoch: identity.producer_epoch,
                committed,
                _unknown_tagged_fields: Vec::new(),
            };
            let version = end_txn_version(self.idempotence.transaction_two_phase_commit);
            let mut attempts_remaining = self.retry_attempts;
            loop {
                let response: EndTxnResponseData = self
                    .wire
                    .send_to_broker(coordinator_id, ApiKey::EndTxn, version, &request)
                    .await?;
                let error = ErrorCode::from(response.error_code);
                if !error.is_error() {
                    let needs_epoch_bump = {
                        let mut state = self.producer_state.lock().await;
                        if response.producer_id != -1 {
                            state.identity = Some(ProducerIdentity {
                                producer_id: response.producer_id,
                                producer_epoch: response.producer_epoch,
                            });
                            state.reset_sequences_after_epoch_bump();
                        }
                        let needs_epoch_bump = !committed && state.epoch_bump_required;
                        state.reset_transaction_after_end(!committed);
                        needs_epoch_bump
                    };
                    if needs_epoch_bump {
                        let _identity = self.bump_producer_identity(coordinator_id).await?;
                        let mut state = self.producer_state.lock().await;
                        state.reset_sequences_after_epoch_bump();
                    }
                    return Ok(());
                }

                if attempts_remaining > 0 && is_transaction_coordinator_error(error) {
                    attempts_remaining = attempts_remaining.saturating_sub(1);
                    coordinator_id = self.refresh_coordinator_id(transactional_id).await?;
                    tokio::time::sleep(TRANSACTION_COORDINATOR_RETRY_BACKOFF).await;
                    continue;
                }
                if attempts_remaining > 0 && error.is_retriable() {
                    attempts_remaining = attempts_remaining.saturating_sub(1);
                    tokio::time::sleep(TRANSACTION_COORDINATOR_RETRY_BACKOFF).await;
                    continue;
                }

                let transaction_error = transaction_control_error_for_client(error);
                if !committed && is_fatal_abort_transaction_error(error) {
                    self.record_fatal_transaction_error(transaction_error).await;
                } else {
                    self.record_transaction_error(transaction_error).await;
                }
                return Err(ProducerError::Transaction {
                    operation: "end_txn",
                    error: transaction_error,
                });
            }
        }
        .await;
        pending_operation.complete(&result).await;
        result
    }

    async fn complete_empty_transaction_after_end(&self, committed: bool) -> Result<()> {
        let needs_epoch_bump = {
            let mut state = self.producer_state.lock().await;
            let needs_epoch_bump = !committed && state.epoch_bump_required;
            state.reset_transaction_after_end(!committed);
            needs_epoch_bump
        };
        if needs_epoch_bump {
            let coordinator_id = self.coordinator_id().await?;
            let _identity = self.bump_producer_identity(coordinator_id).await?;
            let mut state = self.producer_state.lock().await;
            state.reset_sequences_after_epoch_bump();
        }
        Ok(())
    }

    /// Drain ready accumulator batches, route them by leader, and send produce requests.
    #[expect(
        clippy::too_many_lines,
        reason = "Dispatch retry loop keeps leadership and idempotent retry branches together."
    )]
    pub async fn dispatch_ready(
        &self,
        accumulator: &mut RecordAccumulator,
        now: Instant,
    ) -> Result<Vec<RecordMetadata>> {
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
                    if self.metrics_are_enabled() {
                        self.metrics.record_requeue();
                    }
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
                        if self.metrics_are_enabled() {
                            self.metrics.record_error();
                        }
                        self.release_idempotent_partition_after_definite_error(
                            &batches, &topic, partition,
                        )
                        .await;
                        return Err(ProducerError::Broker {
                            topic,
                            partition,
                            error,
                        });
                    }
                    attempts_remaining = attempts_remaining.saturating_sub(1);
                    if self.metrics_are_enabled() {
                        self.metrics.record_retry();
                    }
                    if let Some(error) = self.wait_before_retry(&batches).await {
                        if self.metrics_are_enabled() {
                            self.metrics.record_error();
                        }
                        self.release_idempotent_partition_after_definite_error(
                            &batches, &topic, partition,
                        )
                        .await;
                        return Err(error);
                    }
                },
                Err(DispatchError::RetryableIdempotent {
                    topic,
                    partition,
                    leader_id,
                    error,
                    reset_sequence,
                }) => {
                    let retry = IdempotentRetry {
                        topic,
                        partition,
                        leader_id,
                        error,
                        reset_sequence,
                    };
                    if attempts_remaining == 0 {
                        if self.metrics_are_enabled() {
                            self.metrics.record_error();
                        }
                        return Err(retry.broker_error());
                    }
                    attempts_remaining = attempts_remaining.saturating_sub(1);
                    if self.metrics_are_enabled() {
                        self.metrics.record_retry();
                    }
                    if retry.reset_sequence {
                        self.recover_idempotent_partition(
                            &mut batches,
                            &retry.topic,
                            retry.partition,
                            retry.leader_id,
                        )
                        .await?;
                    }
                    if let Some(error) = self.wait_before_retry(&batches).await {
                        if self.metrics_are_enabled() {
                            self.metrics.record_error();
                        }
                        self.recover_idempotent_partition_after_retry_timeout(&mut batches, &retry)
                            .await?;
                        return Err(error);
                    }
                },
                Err(DispatchError::Producer(error)) => {
                    if self.metrics_are_enabled() {
                        self.metrics.record_error();
                    }
                    return Err(error);
                },
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
    ) -> Result<Vec<RecordMetadata>> {
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
    ) -> Result<Vec<RecordMetadata>> {
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
                    return self
                        .deliver_successful_batches(&mut batches, receipts)
                        .await;
                },
                Err(DispatchError::Requeue) => return DispatchOutcome::Requeue(batches),
                Err(DispatchError::RetryableLeadership {
                    topic,
                    partition,
                    error,
                }) => {
                    self.wire.invalidate_topic_partition(&topic, partition);
                    if attempts_remaining == 0 {
                        if self.metrics_are_enabled() {
                            self.metrics.record_error();
                        }
                        self.release_idempotent_partition_after_definite_error(
                            &batches, &topic, partition,
                        )
                        .await;
                        return DispatchOutcome::Delivered(Err(ProducerError::Broker {
                            topic,
                            partition,
                            error,
                        }));
                    }
                    attempts_remaining = attempts_remaining.saturating_sub(1);
                    if self.metrics_are_enabled() {
                        self.metrics.record_retry();
                    }
                    if let Some(error) = self.wait_before_retry(&batches).await {
                        if self.metrics_are_enabled() {
                            self.metrics.record_error();
                        }
                        self.release_idempotent_partition_after_definite_error(
                            &batches, &topic, partition,
                        )
                        .await;
                        return DispatchOutcome::Delivered(Err(error));
                    }
                },
                Err(DispatchError::RetryableIdempotent {
                    topic,
                    partition,
                    leader_id,
                    error,
                    reset_sequence,
                }) => {
                    let retry = IdempotentRetry {
                        topic,
                        partition,
                        leader_id,
                        error,
                        reset_sequence,
                    };
                    if let Some(outcome) = self
                        .handle_drained_idempotent_retry(
                            &mut batches,
                            &mut attempts_remaining,
                            &retry,
                        )
                        .await
                    {
                        return outcome;
                    }
                },
                Err(DispatchError::Producer(error)) => {
                    if self.metrics_are_enabled() {
                        self.metrics.record_error();
                    }
                    return DispatchOutcome::Delivered(Err(error));
                },
            }
        }
    }

    async fn deliver_successful_batches(
        &self,
        batches: &mut [ReadyBatch],
        receipts: Vec<RecordMetadata>,
    ) -> DispatchOutcome {
        self.release_successful_idempotent_sequences(batches, &receipts)
            .await;
        complete_deliveries(batches, &receipts);
        DispatchOutcome::Delivered(Ok(receipts))
    }

    async fn handle_drained_idempotent_retry(
        &self,
        batches: &mut [ReadyBatch],
        attempts_remaining: &mut usize,
        retry: &IdempotentRetry,
    ) -> Option<DispatchOutcome> {
        if *attempts_remaining == 0 {
            if self.metrics_are_enabled() {
                self.metrics.record_error();
            }
            return Some(DispatchOutcome::Delivered(Err(retry.broker_error())));
        }
        *attempts_remaining = attempts_remaining.saturating_sub(1);
        if self.metrics_are_enabled() {
            self.metrics.record_retry();
        }
        if retry.reset_sequence
            && let Err(error) = self
                .recover_idempotent_partition(
                    batches,
                    &retry.topic,
                    retry.partition,
                    retry.leader_id,
                )
                .await
        {
            return Some(DispatchOutcome::Delivered(Err(error)));
        }
        if let Some(error) = self.wait_before_retry(batches).await {
            if self.metrics_are_enabled() {
                self.metrics.record_error();
            }
            if let Err(recovery_error) = self
                .recover_idempotent_partition_after_retry_timeout(batches, retry)
                .await
            {
                return Some(DispatchOutcome::Delivered(Err(recovery_error)));
            }
            return Some(DispatchOutcome::Delivered(Err(error)));
        }
        None
    }

    async fn dispatch_batches(
        &self,
        batches: &mut [ReadyBatch],
    ) -> std::result::Result<Vec<RecordMetadata>, DispatchError> {
        let topics = unique_topics(batches);
        let metadata = self
            .wire
            .metadata_for_topics(topics.iter().map(String::as_str))
            .await
            .map_err(DispatchError::from)?;
        let version = if self.idempotence.transactional_id.is_some() {
            produce_version(self.idempotence.transaction_two_phase_commit)
        } else {
            client_api_info(ApiKey::Produce).max_version
        };
        let mut by_broker: AHashMap<i32, Vec<BrokerProduceRequest>> = AHashMap::new();
        for batch in batches {
            let Some(first_record) = batch.records.first().cloned() else {
                continue;
            };
            let mut route = self
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
            route.base_sequence = batch.producer_state.map(|state| state.base_sequence);
            let records = encode_record_batch_with_producer_state_at_offset(
                &batch.records,
                self.compression,
                batch.producer_state,
                0,
            )
            .map_err(DispatchError::from)?;
            let options = BrokerProduceOptions {
                acks: self.acks,
                timeout_ms: self.timeout_ms,
                transactional_id: self.idempotence.transactional_id.as_deref(),
            };
            let requests = by_broker.entry(route.leader_id).or_default();
            let placement = broker_request_placement_for_batch(
                requests,
                &route,
                &records,
                options,
                ProduceRequestSizing {
                    version,
                    max_request_size: self.max_request_size,
                },
            )?;
            if placement.split && self.metrics_are_enabled() {
                self.metrics.record_request_split();
            }
            let request_base_offset = request_batch_base_offset(requests, placement.index, &route)?;
            let records = if request_base_offset == 0 {
                records
            } else {
                encode_record_batch_with_producer_state_at_offset(
                    &batch.records,
                    self.compression,
                    batch.producer_state,
                    request_base_offset,
                )
                .map_err(DispatchError::from)?
            };
            if placement.index == requests.len() {
                requests.push(BrokerProduceRequest::default());
            }
            let Some(request) = requests.get_mut(placement.index) else {
                return Err(DispatchError::Producer(ProducerError::FlushIncomplete));
            };
            request.push(route, records, options, batch.records.len());
        }

        let mut receipts = Vec::new();
        for (broker_id, requests) in by_broker {
            let mut broker_receipts = self
                .dispatch_broker_requests(broker_id, requests, version)
                .await?;
            receipts.append(&mut broker_receipts);
        }
        Ok(receipts)
    }

    #[expect(
        clippy::too_many_lines,
        reason = "Broker dispatch owns ordered enqueue, in-flight gating, and idempotent retry \
                  recovery together."
    )]
    async fn dispatch_broker_requests(
        &self,
        broker_id: i32,
        requests: Vec<BrokerProduceRequest>,
        version: i16,
    ) -> std::result::Result<Vec<RecordMetadata>, DispatchError> {
        let mut pending: VecDeque<_> = requests.into_iter().enumerate().collect();
        let mut in_flight = JoinSet::new();
        let mut completed = Vec::new();
        let mut in_flight_routes = AHashSet::new();
        let max_in_flight = self.broker_dispatch_in_flight_limit();
        let mut enqueue_guard = Some(self.produce_enqueue_order.lock().await);

        loop {
            while in_flight.len() < max_in_flight {
                let Some((index, request)) =
                    pop_dispatchable_broker_request(&mut pending, &in_flight_routes)
                else {
                    break;
                };
                for route in &request.routes {
                    let _inserted = in_flight_routes.insert(TopicPartitionKey::from(route));
                }
                let metrics = self.metrics.clone();
                let metrics_enabled = self.metrics_are_enabled();
                if metrics_enabled {
                    let request_bytes = RequestMessage::encoded_len(&request.data, version)
                        .map_err(crate::wire::WireError::from)
                        .map_err(DispatchError::from)?;
                    metrics.record_produce_request(request_bytes, request.payload_bytes);
                }
                self.record_broker_drain_started(broker_id, Instant::now())
                    .await;
                let wire = self.wire.clone();
                let partitioner_state = if self.partitioner_availability_timeout.is_zero() {
                    None
                } else {
                    Some(Arc::clone(&self.partitioner_state))
                };
                let abort_handle = if request.data.acks == ACKS_NONE {
                    in_flight.spawn(async move {
                        let target = BrokerProduceTarget { broker_id, version };
                        let backpressure = ProduceBackpressureRecorder::new(
                            partitioner_state,
                            metrics,
                            metrics_enabled,
                        );
                        let response = send_produce_with_backpressure_retry(
                            &wire,
                            target,
                            &request,
                            backpressure,
                        )
                        .await;
                        (index, broker_id, request, response)
                    })
                } else {
                    let target = BrokerProduceTarget { broker_id, version };
                    let backpressure = ProduceBackpressureRecorder::new(
                        partitioner_state,
                        metrics,
                        metrics_enabled,
                    );
                    let pending_response = enqueue_produce_with_backpressure_retry(
                        &wire,
                        target,
                        &request,
                        backpressure,
                    )
                    .await;
                    in_flight.spawn(async move {
                        let response = match pending_response {
                            Ok(response) => response
                                .wait()
                                .await
                                .map(ProduceDispatchResponse::Acknowledged),
                            Err(error) => Err(error),
                        };
                        (index, broker_id, request, response)
                    })
                };
                drop(abort_handle);
            }
            if pending.is_empty() {
                drop(enqueue_guard.take());
            }

            let Some(result) = in_flight.join_next().await else {
                break;
            };
            let (index, completed_broker_id, request, response) =
                result.map_err(|error| ProducerError::DispatchTask(error.to_string()))?;
            self.record_broker_drain_finished(completed_broker_id, Instant::now())
                .await;
            for route in &request.routes {
                let _removed = in_flight_routes.remove(&TopicPartitionKey::from(route));
            }
            completed.push((index, request, response.map_err(ProducerError::from)?));
        }
        completed.sort_by_key(|(index, _request, _response)| *index);

        let mut receipts = Vec::new();
        for (_index, request, response) in completed {
            let request_receipts = match response {
                ProduceDispatchResponse::Acknowledged(response) => {
                    produce_receipts_with_error_details(&response, &request.routes)
                },
                ProduceDispatchResponse::NoAcknowledgement => Ok(no_ack_receipts(&request.routes)),
            };
            match request_receipts {
                Ok(mut request_receipts) => receipts.append(&mut request_receipts),
                Err(ProduceReceiptError::Broker(error)) if is_leadership_error(error.error) => {
                    if self.metrics_are_enabled() {
                        self.metrics.record_error();
                    }
                    return Err(DispatchError::RetryableLeadership {
                        topic: error.topic,
                        partition: error.partition,
                        error: error.error,
                    });
                },
                Err(ProduceReceiptError::Broker(error))
                    if self.idempotence.enabled && is_idempotent_retry_error(error.error) =>
                {
                    if self.metrics_are_enabled() {
                        self.metrics.record_error();
                    }
                    if self.idempotence.transactional_id.is_some() {
                        self.record_transactional_produce_error(
                            error.error,
                            &error.topic,
                            error.partition,
                        )
                        .await;
                        return Err(DispatchError::Producer(ProducerError::Broker {
                            topic: error.topic,
                            partition: error.partition,
                            error: error.error,
                        }));
                    }
                    let reset_sequence = self
                        .should_reset_sequence_for_idempotent_retry(&request, &error)
                        .await;
                    return Err(DispatchError::RetryableIdempotent {
                        leader_id: broker_id,
                        topic: error.topic,
                        partition: error.partition,
                        error: error.error,
                        reset_sequence,
                    });
                },
                Err(ProduceReceiptError::Broker(error))
                    if self.idempotence.transactional_id.is_some() =>
                {
                    if self.metrics_are_enabled() {
                        self.metrics.record_error();
                    }
                    self.record_transactional_produce_error(
                        error.error,
                        &error.topic,
                        error.partition,
                    )
                    .await;
                    return Err(DispatchError::Producer(ProducerError::Broker {
                        topic: error.topic,
                        partition: error.partition,
                        error: error.error,
                    }));
                },
                Err(ProduceReceiptError::Broker(error)) => {
                    if self.metrics_are_enabled() {
                        self.metrics.record_error();
                    }
                    return Err(DispatchError::Producer(ProducerError::Broker {
                        topic: error.topic,
                        partition: error.partition,
                        error: error.error,
                    }));
                },
                Err(ProduceReceiptError::Producer(error)) => {
                    if self.metrics_are_enabled() {
                        self.metrics.record_error();
                    }
                    return Err(DispatchError::from(error));
                },
            }
        }
        Ok(receipts)
    }

    fn broker_dispatch_in_flight_limit(&self) -> usize {
        self.max_in_flight_requests_per_connection.max(1)
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
                true,
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
            self.mark_expired_idempotent_batches_unresolved(batches, now)
                .await;
            return batches.first().map(|batch| ProducerError::DeliveryTimeout {
                topic: batch.topic.clone(),
                partition: batch.partition,
            });
        }
        let remaining = self.delivery_timeout.saturating_sub(elapsed);
        tokio::time::sleep(PRODUCE_RETRY_BACKOFF.min(remaining)).await;
        let now = Instant::now();
        if let Some(batch) = self.expired_batch(batches, now) {
            self.mark_expired_idempotent_batches_unresolved(batches, now)
                .await;
            return Some(ProducerError::DeliveryTimeout {
                topic: batch.topic.clone(),
                partition: batch.partition,
            });
        }
        None
    }

    async fn mark_expired_idempotent_batches_unresolved(
        &self,
        batches: &[ReadyBatch],
        now: Instant,
    ) {
        if !self.idempotence.enabled {
            return;
        }
        let mut state = self.producer_state.lock().await;
        for batch in batches {
            if now.duration_since(batch.first_append_at) < self.delivery_timeout {
                continue;
            }
            let Some(producer_state) = batch.producer_state else {
                continue;
            };
            let Ok(record_count) = i32::try_from(batch.records.len()) else {
                continue;
            };
            state.mark_sequence_unresolved(
                &batch.topic,
                batch.partition,
                producer_state.base_sequence,
                record_count,
            );
        }
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

    async fn recover_idempotent_partition(
        &self,
        batches: &mut [ReadyBatch],
        topic: &str,
        partition: i32,
        leader_id: i32,
    ) -> Result<()> {
        let _identity = self.bump_producer_identity(leader_id).await?;
        {
            let mut state = self.producer_state.lock().await;
            state.reset_sequence(topic, partition);
        }
        for batch in batches {
            if batch.topic == topic && batch.partition == partition {
                batch.producer_state = None;
            }
        }
        Ok(())
    }

    async fn recover_idempotent_partition_after_retry_timeout(
        &self,
        batches: &mut [ReadyBatch],
        retry: &IdempotentRetry,
    ) -> Result<()> {
        if retry.reset_sequence {
            return Ok(());
        }
        self.recover_idempotent_partition(batches, &retry.topic, retry.partition, retry.leader_id)
            .await
    }

    async fn release_idempotent_partition_after_definite_error(
        &self,
        batches: &[ReadyBatch],
        topic: &str,
        partition: i32,
    ) {
        if !self.idempotence.enabled || self.idempotence.transactional_id.is_some() {
            return;
        }
        let release_to = batches
            .iter()
            .filter(|batch| batch.topic == topic && batch.partition == partition)
            .filter_map(|batch| batch.producer_state.map(|state| state.base_sequence))
            .min();
        let Some(base_sequence) = release_to else {
            return;
        };
        let mut state = self.producer_state.lock().await;
        state.rewind_sequence_to(topic, partition, base_sequence);
    }

    async fn should_reset_sequence_for_idempotent_retry(
        &self,
        request: &BrokerProduceRequest,
        error: &ProduceBrokerError,
    ) -> bool {
        let base_sequence = request
            .routes
            .iter()
            .find(|route| route.topic == error.topic && route.partition == error.partition)
            .and_then(|route| route.base_sequence);
        let state = self.producer_state.lock().await;
        state.should_reset_sequence_for_idempotent_retry(IdempotentRetryDecision {
            topic: &error.topic,
            partition: error.partition,
            error: error.error,
            log_start_offset: error.log_start_offset,
            base_sequence,
        })
    }

    async fn release_successful_idempotent_sequences(
        &self,
        batches: &[ReadyBatch],
        receipts: &[RecordMetadata],
    ) {
        if !self.idempotence.enabled {
            return;
        }
        let mut state = self.producer_state.lock().await;
        for batch in batches {
            let Some(producer_state) = batch.producer_state else {
                continue;
            };
            if !receipts.iter().any(|receipt| {
                receipt.topic.as_ref() == batch.topic && receipt.partition == batch.partition
            }) {
                continue;
            }
            let Ok(record_count) = i32::try_from(batch.records.len()) else {
                continue;
            };
            let Some(next_sequence) = producer_state.base_sequence.checked_add(record_count) else {
                continue;
            };
            state.release_sequence(&batch.topic, batch.partition, next_sequence);
        }
    }

    #[expect(
        clippy::too_many_lines,
        reason = "AddPartitions state tracking, coordinator retry, and partition-set promotion \
                  mirror Java ordering."
    )]
    async fn add_partition_to_transaction(&self, route: &ProduceRoute) -> Result<()> {
        let Some(transactional_id) = self.idempotence.transactional_id.as_ref() else {
            return Ok(());
        };
        let key = TopicPartitionKey {
            topic: route.topic.clone(),
            partition: route.partition,
        };
        {
            let mut state = self.producer_state.lock().await;
            fail_pending_transaction_operation(&mut state)?;
            if !state.in_transaction {
                return Err(ProducerError::InvalidTransactionState(
                    "produce called outside an open transaction",
                ));
            }
            if state.identity.is_none() {
                return Err(ProducerError::InvalidTransactionState(
                    "init_transactions must run before produce",
                ));
            }
            if state.transaction_contains_partition(&key) {
                return Ok(());
            }
            if self.idempotence.transaction_two_phase_commit {
                let _inserted = state.partitions_in_transaction.insert(key.clone());
                let _inserted = state.transaction_partitions.insert(key);
                state.transaction_started = true;
                drop(state);
                return Ok(());
            }
            let _inserted = state.mark_new_transaction_partition(key.clone());
        }

        let mut coordinator_id = self.coordinator_id().await?;
        let identity = self.producer_identity(coordinator_id).await?;
        let pending_partitions = {
            let mut state = self.producer_state.lock().await;
            state.begin_pending_transaction_partitions()
        };
        let request = add_partitions_to_txn_request(transactional_id, identity, route);
        let version = client_api_info(ApiKey::AddPartitionsToTxn).max_version;
        let mut attempts_remaining = self.retry_attempts;
        let mut request_guard = self
            .track_transaction_request(TransactionRequestKind::AddPartitionsOrOffsets)
            .await;
        loop {
            let response: AddPartitionsToTxnResponseData = self
                .wire
                .send_to_broker(
                    coordinator_id,
                    ApiKey::AddPartitionsToTxn,
                    version,
                    &request,
                )
                .await?;
            match Self::check_add_partitions_response(response, route) {
                Ok(()) => break,
                Err(ProducerError::Transaction {
                    error: transaction_error,
                    ..
                }) if attempts_remaining > 0
                    && is_transaction_coordinator_error(transaction_error) =>
                {
                    attempts_remaining = attempts_remaining.saturating_sub(1);
                    coordinator_id = self.refresh_coordinator_id(transactional_id).await?;
                    tokio::time::sleep(TRANSACTION_COORDINATOR_RETRY_BACKOFF).await;
                },
                Err(ProducerError::Transaction {
                    error: transaction_error,
                    ..
                }) if attempts_remaining > 0
                    && is_add_partitions_retry_error(transaction_error) =>
                {
                    attempts_remaining = attempts_remaining.saturating_sub(1);
                    tokio::time::sleep(TRANSACTION_COORDINATOR_RETRY_BACKOFF).await;
                },
                Err(ProducerError::Transaction {
                    operation,
                    error: transaction_error,
                }) => {
                    let transaction_error = transaction_control_error_for_client(transaction_error);
                    let mut state = self.producer_state.lock().await;
                    state.fail_pending_transaction_partitions(&pending_partitions);
                    drop(state);
                    self.record_transaction_error(transaction_error).await;
                    return Err(ProducerError::Transaction {
                        operation,
                        error: transaction_error,
                    });
                },
                Err(error) => return Err(error),
            }
        }
        request_guard.clear().await;
        {
            let mut state = self.producer_state.lock().await;
            state.complete_pending_transaction_partitions(&pending_partitions);
            state.transaction_started = true;
            drop(state);
        }
        Ok(())
    }

    fn check_add_partitions_response(
        response: AddPartitionsToTxnResponseData,
        route: &ProduceRoute,
    ) -> Result<()> {
        let top_level_error = ErrorCode::from(response.error_code);
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
                    let error = ErrorCode::from(partition.partition_error_code);
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

    async fn add_offsets_to_transaction(
        &self,
        mut coordinator_id: i32,
        transactional_id: &str,
        identity: ProducerIdentity,
        group_metadata: &ConsumerGroupMetadata,
    ) -> Result<()> {
        let request = AddOffsetsToTxnRequestData {
            transactional_id: KafkaString::from(transactional_id.to_owned()),
            producer_id: identity.producer_id,
            producer_epoch: identity.producer_epoch,
            group_id: KafkaString::from(group_metadata.group_id.clone()),
            _unknown_tagged_fields: Vec::new(),
        };
        let version = client_api_info(ApiKey::AddOffsetsToTxn).max_version;
        let mut attempts_remaining = self.retry_attempts;
        loop {
            let response: AddOffsetsToTxnResponseData = self
                .wire
                .send_to_broker(coordinator_id, ApiKey::AddOffsetsToTxn, version, &request)
                .await?;
            let error = ErrorCode::from(response.error_code);
            if !error.is_error() {
                let mut state = self.producer_state.lock().await;
                state.transaction_started = true;
                drop(state);
                return Ok(());
            }
            if attempts_remaining > 0 && is_transaction_coordinator_error(error) {
                attempts_remaining = attempts_remaining.saturating_sub(1);
                coordinator_id = self.refresh_coordinator_id(transactional_id).await?;
                tokio::time::sleep(TRANSACTION_COORDINATOR_RETRY_BACKOFF).await;
                continue;
            }
            if attempts_remaining == 0 || !error.is_retriable() {
                let transaction_error = transaction_control_error_for_client(error);
                self.record_transaction_error(transaction_error).await;
                return Err(ProducerError::Transaction {
                    operation: "add_offsets_to_txn",
                    error: transaction_error,
                });
            }
            attempts_remaining = attempts_remaining.saturating_sub(1);
            tokio::time::sleep(TRANSACTION_COORDINATOR_RETRY_BACKOFF).await;
        }
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "TxnOffsetCommit mirrors the generated request fields and transaction context."
    )]
    async fn txn_offset_commit(
        &self,
        mut coordinator_id: i32,
        transactional_id: &str,
        identity: ProducerIdentity,
        offsets: Vec<(TopicPartition, OffsetAndMetadata)>,
        group_metadata: ConsumerGroupMetadata,
    ) -> Result<()> {
        let group_id = group_metadata.group_id;
        let request = TxnOffsetCommitRequestData {
            transactional_id: KafkaString::from(transactional_id.to_owned()),
            group_id: KafkaString::from(group_id.clone()),
            generation_id: group_metadata.generation_id,
            member_id: KafkaString::from(group_metadata.member_id),
            group_instance_id: group_metadata.group_instance_id.map(KafkaString::from),
            producer_id: identity.producer_id,
            producer_epoch: identity.producer_epoch,
            topics: txn_offset_commit_topics(offsets),
            ..TxnOffsetCommitRequestData::default()
        };
        let version = txn_offset_commit_version(self.idempotence.transaction_two_phase_commit);
        let mut attempts_remaining = self.retry_attempts;
        loop {
            let response: TxnOffsetCommitResponseData = self
                .wire
                .send_to_broker(coordinator_id, ApiKey::TxnOffsetCommit, version, &request)
                .await?;
            match txn_offset_commit_error(&response) {
                None => return Ok(()),
                Some(error)
                    if attempts_remaining > 0 && is_txn_offset_commit_coordinator_error(error) =>
                {
                    attempts_remaining = attempts_remaining.saturating_sub(1);
                    coordinator_id = match self.group_coordinator_id(&group_id).await {
                        Ok(coordinator_id) => coordinator_id,
                        Err(ProducerError::Transaction { operation, error }) => {
                            self.record_transaction_error(error).await;
                            return Err(ProducerError::Transaction { operation, error });
                        },
                        Err(error) => return Err(error),
                    };
                    tokio::time::sleep(TRANSACTION_COORDINATOR_RETRY_BACKOFF).await;
                },
                Some(error) if attempts_remaining > 0 && error.is_retriable() => {
                    attempts_remaining = attempts_remaining.saturating_sub(1);
                    tokio::time::sleep(TRANSACTION_COORDINATOR_RETRY_BACKOFF).await;
                },
                Some(error) => {
                    let transaction_error = transaction_control_error_for_client(error);
                    if is_txn_offset_commit_abortable_error(error) {
                        self.record_abortable_transaction_error(transaction_error)
                            .await;
                    } else {
                        self.record_transaction_error(transaction_error).await;
                    }
                    return Err(ProducerError::Transaction {
                        operation: "txn_offset_commit",
                        error: transaction_error,
                    });
                },
            }
        }
    }

    async fn record_transaction_error(&self, error: ErrorCode) {
        let error = transaction_control_error_for_client(error);
        let mut state = self.producer_state.lock().await;
        if is_fatal_transaction_error(error) {
            state.fatal_error = Some(error);
            state.transaction_state = TransactionState::FatalError;
        } else if state.in_transaction || state.transaction_state.is_completing() {
            state.abortable_error = Some(error);
            state.transaction_state = TransactionState::AbortableError;
            if matches!(error, ErrorCode::UnknownProducerId) {
                state.epoch_bump_required = true;
            }
        }
    }

    async fn record_abortable_transaction_error(&self, error: ErrorCode) {
        let mut state = self.producer_state.lock().await;
        if state.in_transaction {
            state.abortable_error = Some(error);
            state.transaction_state = TransactionState::AbortableError;
        }
    }

    async fn record_transactional_produce_error(
        &self,
        error: ErrorCode,
        topic: &str,
        partition: i32,
    ) {
        let mut state = self.producer_state.lock().await;
        if state.in_transaction {
            if is_fatal_transactional_produce_error(error) {
                state.abortable_error = None;
                state.fatal_error = Some(error);
                state.transaction_state = TransactionState::FatalError;
            } else {
                state.abortable_error = Some(error);
                state.transaction_state = TransactionState::AbortableError;
                state.epoch_bump_required = true;
            }
            if matches!(error, ErrorCode::UnknownProducerId) {
                state.reset_sequence(topic, partition);
            }
        }
    }

    async fn record_fatal_transaction_error(&self, error: ErrorCode) {
        let error = transaction_control_error_for_client(error);
        let mut state = self.producer_state.lock().await;
        state.abortable_error = None;
        state.fatal_error = Some(error);
        state.transaction_state = TransactionState::FatalError;
    }

    async fn producer_identity(&self, broker_id: i32) -> Result<ProducerIdentity> {
        {
            let state = self.producer_state.lock().await;
            if let Some(identity) = state.identity {
                return Ok(identity);
            }
        }

        let _init_guard = self.producer_identity_init.lock().await;
        {
            let state = self.producer_state.lock().await;
            if let Some(identity) = state.identity {
                return Ok(identity);
            }
        }

        let response = self.init_producer_identity(broker_id, None).await?;
        let identity = ProducerIdentity {
            producer_id: response.producer_id,
            producer_epoch: response.producer_epoch,
        };
        let mut state = self.producer_state.lock().await;
        let identity = *state.identity.get_or_insert(identity);
        if self.idempotence.transactional_id.is_some()
            && matches!(
                state.transaction_state,
                TransactionState::Uninitialized | TransactionState::Initializing
            )
        {
            state.transition_to(TransactionState::Ready)?;
        }
        drop(state);
        Ok(identity)
    }

    async fn bump_producer_identity(&self, broker_id: i32) -> Result<ProducerIdentity> {
        let current = {
            let state = self.producer_state.lock().await;
            state.identity
        };
        let response = self.init_producer_identity(broker_id, current).await?;
        let identity = ProducerIdentity {
            producer_id: response.producer_id,
            producer_epoch: response.producer_epoch,
        };
        let mut state = self.producer_state.lock().await;
        state.identity = Some(identity);
        if self.idempotence.transactional_id.is_some()
            && state.transaction_state == TransactionState::Initializing
        {
            state.transition_to(TransactionState::Ready)?;
        }
        drop(state);
        Ok(identity)
    }

    async fn init_producer_identity(
        &self,
        mut broker_id: i32,
        current: Option<ProducerIdentity>,
    ) -> Result<InitProducerIdResponseData> {
        let transactional_id = self.idempotence.transactional_id.clone();
        let request = InitProducerIdRequestData {
            transactional_id: transactional_id
                .as_ref()
                .map(|id| KafkaString::from(id.clone())),
            transaction_timeout_ms: self.idempotence.transaction_timeout_ms,
            producer_id: current.map_or(-1, |identity| identity.producer_id),
            producer_epoch: current.map_or(-1, |identity| identity.producer_epoch),
            enable2_pc: self.idempotence.transaction_two_phase_commit,
            ..InitProducerIdRequestData::default()
        };
        let version = init_producer_id_version(self.idempotence.transaction_two_phase_commit);
        let mut attempts_remaining = self.retry_attempts;
        let request_kind = if current.is_some() {
            TransactionRequestKind::EpochBump
        } else {
            TransactionRequestKind::InitProducerId
        };
        let mut request_guard = self.track_transaction_request(request_kind).await;
        loop {
            let response: InitProducerIdResponseData = self
                .wire
                .send_to_broker(broker_id, ApiKey::InitProducerId, version, &request)
                .await?;
            let error = ErrorCode::from(response.error_code);
            if !error.is_error() {
                request_guard.clear().await;
                return Ok(response);
            }
            if attempts_remaining > 0
                && is_transaction_coordinator_error(error)
                && let Some(transactional_id) = transactional_id.as_deref()
            {
                attempts_remaining = attempts_remaining.saturating_sub(1);
                broker_id = self.refresh_coordinator_id(transactional_id).await?;
                tokio::time::sleep(TRANSACTION_COORDINATOR_RETRY_BACKOFF).await;
                continue;
            }
            if attempts_remaining == 0 || !error.is_retriable() {
                let transaction_error = transaction_control_error_for_client(error);
                self.record_init_producer_error(transaction_error).await;
                return Err(ProducerError::Transaction {
                    operation: "init_producer_id",
                    error: transaction_error,
                });
            }
            attempts_remaining = attempts_remaining.saturating_sub(1);
            tokio::time::sleep(TRANSACTION_COORDINATOR_RETRY_BACKOFF).await;
        }
    }

    async fn record_init_producer_error(&self, error: ErrorCode) {
        if self.idempotence.transactional_id.is_none() {
            return;
        }
        if is_init_producer_abortable_error(error) {
            let mut state = self.producer_state.lock().await;
            state.abortable_error = Some(error);
            state.transaction_state = TransactionState::AbortableError;
        } else {
            self.record_fatal_transaction_error(error).await;
        }
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

        let coordinator_id = match self
            .find_coordinator_id(transactional_id, 1, "find_coordinator")
            .await
        {
            Ok(coordinator_id) => coordinator_id,
            Err(ProducerError::Transaction { operation, error }) => {
                self.record_transaction_error(error).await;
                return Err(ProducerError::Transaction { operation, error });
            },
            Err(error) => return Err(error),
        };
        let mut state = self.producer_state.lock().await;
        let coordinator_id = *state.coordinator_id.get_or_insert(coordinator_id);
        drop(state);
        Ok(coordinator_id)
    }

    async fn refresh_coordinator_id(&self, transactional_id: &str) -> Result<i32> {
        let coordinator_id = match self
            .find_coordinator_id(transactional_id, 1, "find_coordinator")
            .await
        {
            Ok(coordinator_id) => coordinator_id,
            Err(ProducerError::Transaction { operation, error }) => {
                self.record_transaction_error(error).await;
                return Err(ProducerError::Transaction { operation, error });
            },
            Err(error) => return Err(error),
        };
        let mut state = self.producer_state.lock().await;
        state.coordinator_id = Some(coordinator_id);
        drop(state);
        Ok(coordinator_id)
    }

    async fn group_coordinator_id(&self, group_id: &str) -> Result<i32> {
        self.find_coordinator_id(group_id, 0, "find_coordinator")
            .await
    }

    async fn find_coordinator_id(
        &self,
        key: &str,
        key_type: i8,
        operation: &'static str,
    ) -> Result<i32> {
        let request = FindCoordinatorRequestData {
            key_type,
            coordinator_keys: vec![KafkaString::from(key.to_owned())],
            ..FindCoordinatorRequestData::default()
        };
        let broker_id = self.wire.any_broker_id()?;
        let version = client_api_info(ApiKey::FindCoordinator).max_version;
        let mut attempts_remaining = self.retry_attempts;
        let mut request_guard = self
            .track_transaction_request(TransactionRequestKind::FindCoordinator)
            .await;
        let coordinator = loop {
            let response: FindCoordinatorResponseData = self
                .wire
                .send_to_broker(broker_id, ApiKey::FindCoordinator, version, &request)
                .await?;
            let coordinator = response
                .coordinators
                .into_iter()
                .find(|coordinator| coordinator.key.to_string() == key)
                .ok_or(ProducerError::InvalidTransactionState(
                    "coordinator response was missing requested key",
                ))?;
            let error = ErrorCode::from(coordinator.error_code);
            if !error.is_error() {
                break coordinator;
            }
            if attempts_remaining == 0 || !error.is_retriable() {
                return Err(ProducerError::Transaction { operation, error });
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
        request_guard.clear().await;
        Ok(coordinator.node_id)
    }

    async fn track_transaction_request(
        &self,
        kind: TransactionRequestKind,
    ) -> TransactionRequestGuard {
        if self.idempotence.transactional_id.is_none() {
            return TransactionRequestGuard::empty();
        }
        let mut state = self.producer_state.lock().await;
        state.pending_requests.push(kind);
        drop(state);
        TransactionRequestGuard::new(Arc::clone(&self.producer_state), kind)
    }
}

#[derive(Debug, Default)]
struct ProducerIdempotenceState {
    identity: Option<ProducerIdentity>,
    sequences: AHashMap<TopicPartitionKey, i32>,
    unresolved_sequences: AHashMap<TopicPartitionKey, i32>,
    coordinator_id: Option<i32>,
    transaction_state: TransactionState,
    in_transaction: bool,
    transaction_started: bool,
    new_partitions_in_transaction: AHashSet<TopicPartitionKey>,
    pending_partitions_in_transaction: AHashSet<TopicPartitionKey>,
    partitions_in_transaction: AHashSet<TopicPartitionKey>,
    transaction_partitions: AHashSet<TopicPartitionKey>,
    abortable_error: Option<ErrorCode>,
    fatal_error: Option<ErrorCode>,
    epoch_bump_required: bool,
    pending_operation: Option<TransactionOperation>,
    pending_result: Option<TransactionalRequestResult>,
    pending_operation_status: PendingTransactionOperationStatus,
    pending_requests: TransactionRequestQueue,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TransactionOperation {
    InitTransactions,
    SendOffsetsToTransaction,
    EndTransaction { committed: bool },
}

impl TransactionOperation {
    const fn request_kind(self) -> TransactionRequestKind {
        match self {
            Self::InitTransactions => TransactionRequestKind::InitProducerId,
            Self::SendOffsetsToTransaction => TransactionRequestKind::AddPartitionsOrOffsets,
            Self::EndTransaction { .. } => TransactionRequestKind::EndTxn,
        }
    }
}

#[derive(Debug)]
enum TransactionPendingOperationStart {
    Started(TransactionalRequestResult),
    Cached(TransactionalRequestResult),
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
enum PendingTransactionOperationStatus {
    #[default]
    Active,
    TimedOut,
}

#[derive(Debug, Clone)]
struct TransactionalRequestResult {
    inner: Arc<TransactionalRequestResultInner>,
}

#[derive(Debug)]
struct TransactionalRequestResultInner {
    completion: StdMutex<Option<TransactionRequestCompletion>>,
    acked: AtomicBool,
    notify: Notify,
}

impl TransactionalRequestResult {
    fn new() -> Self {
        Self {
            inner: Arc::new(TransactionalRequestResultInner {
                completion: StdMutex::new(None),
                acked: AtomicBool::new(false),
                notify: Notify::new(),
            }),
        }
    }

    fn done(&self) {
        self.complete(TransactionRequestCompletion::Success);
    }

    fn fail(&self, error: &ProducerError) {
        self.complete(TransactionRequestCompletion::Failure(
            CachedProducerError::from(error),
        ));
    }

    async fn wait(&self) -> Result<()> {
        loop {
            if let Some(completion) = self.completion()? {
                self.inner.acked.store(true, Ordering::Release);
                return completion.into_result();
            }
            self.inner.notify.notified().await;
        }
    }

    fn completion(&self) -> Result<Option<TransactionRequestCompletion>> {
        let completion = self.inner.completion.lock().map_err(|_error| {
            ProducerError::InvalidTransactionState("cached transaction result lock poisoned")
        })?;
        Ok(completion.clone())
    }

    fn complete(&self, completion: TransactionRequestCompletion) {
        if let Ok(mut pending_completion) = self.inner.completion.lock()
            && pending_completion.is_none()
        {
            *pending_completion = Some(completion);
            self.inner.notify.notify_waiters();
        }
    }

    #[cfg(test)]
    fn is_same_handle(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }

    #[cfg(test)]
    fn is_completed(&self) -> bool {
        self.completion()
            .is_ok_and(|completion| completion.is_some())
    }

    fn is_acked(&self) -> bool {
        self.inner.acked.load(Ordering::Acquire)
    }

    #[cfg(test)]
    fn is_successful(&self) -> bool {
        self.completion().is_ok_and(|completion| {
            matches!(completion, Some(TransactionRequestCompletion::Success))
        })
    }
}

#[derive(Debug, Clone)]
enum TransactionRequestCompletion {
    Success,
    Failure(CachedProducerError),
}

impl TransactionRequestCompletion {
    fn into_result(self) -> Result<()> {
        match self {
            Self::Success => Ok(()),
            Self::Failure(error) => Err(error.into_producer_error()),
        }
    }
}

#[derive(Debug, Clone)]
enum CachedProducerError {
    Transaction {
        operation: &'static str,
        error: ErrorCode,
    },
    InvalidTransactionState(&'static str),
    TransactionalIdRequired,
    TransactionStateBusy,
    InvalidConsumerGroupMetadata(&'static str),
    TelemetryDisabled,
    Telemetry {
        operation: &'static str,
        error: ErrorCode,
    },
    InvalidTelemetrySubscription(&'static str),
    InvalidTelemetryTimeout {
        timeout_ms: i64,
    },
    InvalidCloseTimeout {
        timeout_ms: i64,
    },
    UnsupportedOperation(&'static str),
    DispatchTask(String),
}

impl CachedProducerError {
    fn into_producer_error(self) -> ProducerError {
        match self {
            Self::Transaction { operation, error } => {
                ProducerError::Transaction { operation, error }
            },
            Self::InvalidTransactionState(message) => {
                ProducerError::InvalidTransactionState(message)
            },
            Self::TransactionalIdRequired => ProducerError::TransactionalIdRequired,
            Self::TransactionStateBusy => ProducerError::TransactionStateBusy,
            Self::InvalidConsumerGroupMetadata(message) => {
                ProducerError::InvalidConsumerGroupMetadata(message)
            },
            Self::TelemetryDisabled => ProducerError::TelemetryDisabled,
            Self::Telemetry { operation, error } => ProducerError::Telemetry { operation, error },
            Self::InvalidTelemetrySubscription(message) => {
                ProducerError::InvalidTelemetrySubscription(message)
            },
            Self::InvalidTelemetryTimeout { timeout_ms } => {
                ProducerError::InvalidTelemetryTimeout { timeout_ms }
            },
            Self::InvalidCloseTimeout { timeout_ms } => {
                ProducerError::InvalidCloseTimeout { timeout_ms }
            },
            Self::UnsupportedOperation(operation) => ProducerError::UnsupportedOperation(operation),
            Self::DispatchTask(message) => ProducerError::DispatchTask(message),
        }
    }
}

impl From<&ProducerError> for CachedProducerError {
    fn from(error: &ProducerError) -> Self {
        match error {
            ProducerError::Transaction { operation, error } => Self::Transaction {
                operation,
                error: *error,
            },
            ProducerError::InvalidTransactionState(message) => {
                Self::InvalidTransactionState(message)
            },
            ProducerError::TransactionalIdRequired => Self::TransactionalIdRequired,
            ProducerError::TransactionStateBusy => Self::TransactionStateBusy,
            ProducerError::InvalidConsumerGroupMetadata(message) => {
                Self::InvalidConsumerGroupMetadata(message)
            },
            ProducerError::TelemetryDisabled => Self::TelemetryDisabled,
            ProducerError::Telemetry { operation, error } => Self::Telemetry {
                operation,
                error: *error,
            },
            ProducerError::InvalidTelemetrySubscription(message) => {
                Self::InvalidTelemetrySubscription(message)
            },
            ProducerError::InvalidTelemetryTimeout { timeout_ms } => {
                Self::InvalidTelemetryTimeout {
                    timeout_ms: *timeout_ms,
                }
            },
            ProducerError::InvalidCloseTimeout { timeout_ms } => Self::InvalidCloseTimeout {
                timeout_ms: *timeout_ms,
            },
            ProducerError::UnsupportedOperation(operation) => Self::UnsupportedOperation(operation),
            _ => Self::DispatchTask(format!("cached transaction operation failed: {error}")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TransactionRequestKind {
    FindCoordinator,
    InitProducerId,
    AddPartitionsOrOffsets,
    EndTxn,
    EpochBump,
}

impl TransactionRequestKind {
    const fn priority(self) -> u8 {
        match self {
            Self::FindCoordinator => 0,
            Self::InitProducerId => 1,
            Self::AddPartitionsOrOffsets => 2,
            Self::EndTxn => 3,
            Self::EpochBump => 4,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TransactionRequestQueueEntry {
    kind: TransactionRequestKind,
    sequence: u64,
}

#[derive(Debug, Default)]
pub(crate) struct TransactionRequestQueue {
    entries: Vec<TransactionRequestQueueEntry>,
    next_sequence: u64,
}

impl TransactionRequestQueue {
    pub(crate) fn push(&mut self, kind: TransactionRequestKind) {
        self.entries.push(TransactionRequestQueueEntry {
            kind,
            sequence: self.next_sequence,
        });
        self.next_sequence = self.next_sequence.saturating_add(1);
    }

    pub(crate) fn pop_next(&mut self) -> Option<TransactionRequestKind> {
        let index = self.next_index()?;
        Some(self.entries.remove(index).kind)
    }

    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Sender-loop transaction request selection is covered before the async \
                      dispatcher consumes it directly."
        )
    )]
    pub(crate) fn next_request(
        &mut self,
        has_incomplete_batches: bool,
    ) -> Option<TransactionRequestKind> {
        let index = self.next_index()?;
        let kind = self.entries.get(index)?.kind;
        if kind == TransactionRequestKind::EndTxn && has_incomplete_batches {
            return None;
        }
        Some(self.entries.remove(index).kind)
    }

    fn remove_first(&mut self, kind: TransactionRequestKind) -> bool {
        if self
            .next_index()
            .and_then(|index| self.entries.get(index))
            .is_some_and(|entry| entry.kind == kind)
        {
            return self.pop_next().is_some();
        }
        let Some(index) = self.entries.iter().position(|entry| entry.kind == kind) else {
            return false;
        };
        let _entry = self.entries.remove(index);
        true
    }

    fn next_index(&self) -> Option<usize> {
        self.entries
            .iter()
            .enumerate()
            .min_by_key(|(_index, entry)| (entry.kind.priority(), entry.sequence))
            .map(|(index, _entry)| index)
    }
}

#[derive(Debug)]
struct TransactionRequestGuard {
    producer_state: Option<Arc<Mutex<ProducerIdempotenceState>>>,
    kind: TransactionRequestKind,
    armed: bool,
}

impl TransactionRequestGuard {
    const fn new(
        producer_state: Arc<Mutex<ProducerIdempotenceState>>,
        kind: TransactionRequestKind,
    ) -> Self {
        Self {
            producer_state: Some(producer_state),
            kind,
            armed: true,
        }
    }

    const fn empty() -> Self {
        Self {
            producer_state: None,
            kind: TransactionRequestKind::FindCoordinator,
            armed: false,
        }
    }

    async fn clear(&mut self) {
        if !self.armed {
            return;
        }
        if let Some(producer_state) = &self.producer_state {
            clear_transaction_request(producer_state, self.kind).await;
        }
        self.armed = false;
    }
}

impl Drop for TransactionRequestGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let Some(producer_state) = self.producer_state.clone() else {
            return;
        };
        let kind = self.kind;
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let _clear_task = handle.spawn(async move {
                clear_transaction_request(&producer_state, kind).await;
            });
        }
    }
}

#[derive(Debug)]
struct PendingTransactionOperationGuard {
    producer_state: Arc<Mutex<ProducerIdempotenceState>>,
    operation: TransactionOperation,
    result: TransactionalRequestResult,
    armed: bool,
}

impl PendingTransactionOperationGuard {
    const fn new(
        producer_state: Arc<Mutex<ProducerIdempotenceState>>,
        operation: TransactionOperation,
        result: TransactionalRequestResult,
    ) -> Self {
        Self {
            producer_state,
            operation,
            result,
            armed: true,
        }
    }

    async fn complete(&mut self, result: &Result<()>) {
        if self.armed {
            match result {
                Ok(()) => self.result.done(),
                Err(error) => self.result.fail(error),
            }
            complete_pending_transaction_operation(&self.producer_state, self.operation).await;
            self.armed = false;
        }
    }
}

impl Drop for PendingTransactionOperationGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let producer_state = Arc::clone(&self.producer_state);
        let operation = self.operation;
        self.result.fail(&ProducerError::InvalidTransactionState(
            "pending transaction operation dropped before completion",
        ));
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let _clear_task = handle.spawn(async move {
                clear_pending_transaction_operation(&producer_state, operation).await;
            });
        }
    }
}

#[derive(Debug, Default)]
struct ProducerPartitionerState {
    next_by_topic: AHashMap<String, i32>,
    sticky_by_topic: AHashMap<String, StickyPartitionState>,
    load_stats_by_topic: AHashMap<String, PartitionLoadStats>,
    broker_drain_stats_by_id: AHashMap<i32, BrokerDrainStats>,
}

#[derive(Debug, Clone, Copy)]
struct StickyPartitionState {
    partition: i32,
    bytes: usize,
    switch_on_next: bool,
}

#[derive(Debug, Clone, Copy)]
struct TopicPartitionAssignment<'a> {
    topic: &'a str,
    topic_metadata: &'a TopicMetadata,
    ignore_keys: bool,
    adaptive: bool,
    sticky_batch_size: usize,
}

#[derive(Debug, Clone, Copy)]
struct PartitionLoadRefresh<'a> {
    topic: &'a str,
    topic_metadata: &'a TopicMetadata,
    accumulator: &'a RecordAccumulator,
    now: Instant,
    availability_timeout: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PartitionLoadStats {
    cumulative_frequency_table: Vec<i32>,
    partition_ids: Vec<i32>,
}

#[derive(Debug, Clone, Copy)]
struct BrokerDrainStats {
    ready_at: Instant,
    drain_at: Instant,
    in_flight: usize,
}

impl ProducerPartitionerState {
    fn next_for_topic(&mut self, topic: &str) -> &mut i32 {
        self.next_by_topic.entry(topic.to_owned()).or_insert(0)
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "Partition choice needs record data, metadata, and current partitioner policies."
    )]
    fn partition_for_record(
        &mut self,
        metadata: &crate::wire::ClusterMetadata,
        record: &super::ProducerRecord,
        ignore_keys: bool,
        adaptive: bool,
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
        self.sticky_partition(
            &record.topic,
            topic_metadata,
            record,
            adaptive,
            sticky_batch_size,
        )
    }

    fn try_assign_cached_sticky_partition(
        &mut self,
        record: &mut super::ProducerRecord,
        sticky_batch_size: usize,
    ) -> bool {
        if record.has_assigned_partition() {
            return true;
        }
        let Some(sticky) = self.sticky_by_topic.get_mut(record.topic.as_ref()) else {
            return false;
        };
        if sticky.switch_on_next {
            return false;
        }
        let record_bytes = estimate_record_batch_bytes(record);
        let next_bytes = sticky.bytes.saturating_add(record_bytes);
        if next_bytes >= sticky_batch_size.max(1).saturating_mul(2) {
            return false;
        }
        record.partition = sticky.partition;
        sticky.bytes = next_bytes;
        true
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
            adaptive,
            sticky_batch_size,
        } = assignment;
        ensure_partitions(topic, super::record::UNASSIGNED_PARTITION, topic_metadata)?;
        let existing_sticky = self.valid_sticky(topic, topic_metadata);
        let mut sticky = match existing_sticky {
            Some(sticky) => sticky,
            None => StickyPartitionState {
                partition: self.next_partition(topic, topic_metadata, adaptive)?,
                bytes: RECORD_BATCH_OVERHEAD_BYTES,
                switch_on_next: false,
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

            if sticky.switch_on_next {
                sticky = StickyPartitionState {
                    partition: self.next_partition(topic, topic_metadata, adaptive)?,
                    bytes: RECORD_BATCH_OVERHEAD_BYTES,
                    switch_on_next: false,
                };
            }
            sticky_used = true;
            record.partition = sticky.partition;
            sticky.bytes = sticky
                .bytes
                .saturating_add(estimate_record_batch_bytes(record));
            if sticky.bytes >= sticky_batch_size.max(1).saturating_mul(2) {
                sticky = StickyPartitionState {
                    partition: self.next_partition(topic, topic_metadata, adaptive)?,
                    bytes: RECORD_BATCH_OVERHEAD_BYTES,
                    switch_on_next: false,
                };
            }
        }

        if sticky_used {
            let _previous = self.sticky_by_topic.insert(topic.to_owned(), sticky);
        }
        Ok(())
    }

    #[cfg(test)]
    fn assign_sticky_topic_partitions(
        &mut self,
        assignment: TopicPartitionAssignment<'_>,
        records: &mut [super::ProducerRecord],
    ) -> Result<()> {
        let TopicPartitionAssignment {
            topic,
            topic_metadata,
            adaptive,
            sticky_batch_size,
            ..
        } = assignment;
        ensure_partitions(topic, super::record::UNASSIGNED_PARTITION, topic_metadata)?;
        let existing_sticky = self.valid_sticky(topic, topic_metadata);
        let mut sticky = match existing_sticky {
            Some(sticky) => sticky,
            None => StickyPartitionState {
                partition: self.next_partition(topic, topic_metadata, adaptive)?,
                bytes: RECORD_BATCH_OVERHEAD_BYTES,
                switch_on_next: false,
            },
        };

        for record in records {
            if sticky.switch_on_next {
                sticky = StickyPartitionState {
                    partition: self.next_partition(topic, topic_metadata, adaptive)?,
                    bytes: RECORD_BATCH_OVERHEAD_BYTES,
                    switch_on_next: false,
                };
            }
            record.partition = sticky.partition;
            sticky.bytes = sticky
                .bytes
                .saturating_add(estimate_record_batch_bytes(record));
            if sticky.bytes >= sticky_batch_size.max(1).saturating_mul(2) {
                sticky = StickyPartitionState {
                    partition: self.next_partition(topic, topic_metadata, adaptive)?,
                    bytes: RECORD_BATCH_OVERHEAD_BYTES,
                    switch_on_next: false,
                };
            }
        }

        let _previous = self.sticky_by_topic.insert(topic.to_owned(), sticky);
        Ok(())
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "Sticky partitioning combines metadata, record size, and active policy knobs."
    )]
    fn sticky_partition(
        &mut self,
        topic: &str,
        topic_metadata: &TopicMetadata,
        record: &super::ProducerRecord,
        adaptive: bool,
        sticky_batch_size: usize,
    ) -> Result<i32> {
        ensure_partitions(topic, record.partition, topic_metadata)?;

        let existing_sticky = self.valid_sticky(topic, topic_metadata);
        let mut sticky = match existing_sticky {
            Some(sticky) => sticky,
            None => StickyPartitionState {
                partition: self.next_partition(topic, topic_metadata, adaptive)?,
                bytes: RECORD_BATCH_OVERHEAD_BYTES,
                switch_on_next: false,
            },
        };

        if sticky.switch_on_next {
            sticky = StickyPartitionState {
                partition: self.next_partition(topic, topic_metadata, adaptive)?,
                bytes: RECORD_BATCH_OVERHEAD_BYTES,
                switch_on_next: false,
            };
        }
        let partition = sticky.partition;
        sticky.bytes = sticky
            .bytes
            .saturating_add(estimate_record_batch_bytes(record));
        if sticky.bytes >= sticky_batch_size.max(1).saturating_mul(2) {
            sticky = StickyPartitionState {
                partition: self.next_partition(topic, topic_metadata, adaptive)?,
                bytes: RECORD_BATCH_OVERHEAD_BYTES,
                switch_on_next: false,
            };
        }
        let _previous = self.sticky_by_topic.insert(topic.to_owned(), sticky);
        Ok(partition)
    }

    fn mark_sticky_batch_ready(&mut self, topic: &str, sticky_batch_size: usize) {
        let Some(sticky) = self.sticky_by_topic.get_mut(topic) else {
            return;
        };
        if sticky.bytes >= sticky_batch_size.max(1) {
            sticky.switch_on_next = true;
        }
    }

    fn next_partition(
        &mut self,
        topic: &str,
        topic_metadata: &TopicMetadata,
        adaptive: bool,
    ) -> Result<i32> {
        ensure_partitions(topic, super::record::UNASSIGNED_PARTITION, topic_metadata)?;
        if adaptive && let Some(partition) = self.next_adaptive_partition(topic, topic_metadata) {
            return Ok(partition);
        }
        let random = self.next_partition_counter(topic);
        uniform_partition_for_random(topic, topic_metadata, random)
    }

    fn next_adaptive_partition(
        &mut self,
        topic: &str,
        topic_metadata: &TopicMetadata,
    ) -> Option<i32> {
        let random = self.next_partition_counter(topic);
        self.adaptive_partition_for_random(topic, topic_metadata, random)
    }

    fn adaptive_partition_for_random(
        &self,
        topic: &str,
        topic_metadata: &TopicMetadata,
        random: usize,
    ) -> Option<i32> {
        let range_end = self
            .load_stats_by_topic
            .get(topic)?
            .cumulative_frequency_table
            .last()
            .copied()?;
        let range_end = usize::try_from(range_end).ok()?.max(1);
        let weighted = i32::try_from(random.checked_rem(range_end).unwrap_or(0)).ok()?;
        let stats = self.load_stats_by_topic.get(topic)?;
        let partition = stats
            .cumulative_frequency_table
            .iter()
            .position(|limit| weighted < *limit)
            .and_then(|index| stats.partition_ids.get(index).copied())?;
        topic_metadata
            .partitions
            .iter()
            .any(|metadata| metadata.partition_index == partition)
            .then_some(partition)
    }

    fn update_partition_load_stats(
        &mut self,
        topic: &str,
        queue_sizes: &[i32],
        partition_ids: &[i32],
        length: usize,
    ) {
        let Some(stats) = build_partition_load_stats(queue_sizes, partition_ids, length) else {
            let _removed = self.load_stats_by_topic.remove(topic);
            return;
        };
        let _previous = self.load_stats_by_topic.insert(topic.to_owned(), stats);
    }

    #[cfg(test)]
    fn update_partition_load_stats_from_accumulator(
        &mut self,
        topic: &str,
        topic_metadata: &TopicMetadata,
        accumulator: &RecordAccumulator,
    ) {
        self.update_partition_load_stats_from_accumulator_at(PartitionLoadRefresh {
            topic,
            topic_metadata,
            accumulator,
            now: Instant::now(),
            availability_timeout: Duration::ZERO,
        });
    }

    fn update_partition_load_stats_from_accumulator_at(
        &mut self,
        refresh: PartitionLoadRefresh<'_>,
    ) {
        let unavailable_brokers = self.unavailable_topic_leaders(
            refresh.topic_metadata,
            refresh.now,
            refresh.availability_timeout,
        );
        let Some(load) = refresh
            .accumulator
            .partition_queue_load_with_availability(refresh.topic_metadata, |partition| {
                !unavailable_brokers.contains(&partition.leader_id)
            })
        else {
            let _removed = self.load_stats_by_topic.remove(refresh.topic);
            return;
        };
        self.update_partition_load_stats(
            refresh.topic,
            &load.queue_sizes,
            &load.partition_ids,
            load.length,
        );
    }

    #[cfg(test)]
    fn update_broker_drain_stats(&mut self, broker_id: i32, now: Instant, can_drain: bool) {
        self.update_broker_latency_stats(broker_id, now, can_drain);
    }

    fn update_broker_latency_stats(&mut self, broker_id: i32, now: Instant, can_drain: bool) {
        let stats = self
            .broker_drain_stats_by_id
            .entry(broker_id)
            .or_insert(BrokerDrainStats {
                ready_at: now,
                drain_at: now,
                in_flight: 0,
            });
        if can_drain {
            stats.drain_at = now;
        }
        stats.ready_at = now;
    }

    fn record_broker_drain_started(&mut self, broker_id: i32, now: Instant) {
        self.update_broker_latency_stats(broker_id, now, true);
        let stats = self
            .broker_drain_stats_by_id
            .entry(broker_id)
            .or_insert(BrokerDrainStats {
                ready_at: now,
                drain_at: now,
                in_flight: 0,
            });
        stats.in_flight = stats.in_flight.saturating_add(1);
    }

    fn record_broker_drain_finished(&mut self, broker_id: i32, now: Instant) {
        let stats = self
            .broker_drain_stats_by_id
            .entry(broker_id)
            .or_insert(BrokerDrainStats {
                ready_at: now,
                drain_at: now,
                in_flight: 0,
            });
        stats.in_flight = stats.in_flight.saturating_sub(1);
        if stats.in_flight == 0 {
            stats.drain_at = now;
            stats.ready_at = now;
        }
    }

    fn unavailable_topic_leaders(
        &mut self,
        topic_metadata: &TopicMetadata,
        now: Instant,
        availability_timeout: Duration,
    ) -> AHashSet<i32> {
        if availability_timeout.is_zero() {
            return AHashSet::new();
        }
        let mut unavailable = AHashSet::new();
        for partition in &topic_metadata.partitions {
            if partition.leader_id < 0 {
                continue;
            }
            if let Some(stats) = self.broker_drain_stats_by_id.get_mut(&partition.leader_id) {
                if stats.in_flight > 0 {
                    stats.ready_at = now;
                }
                let waiting = stats
                    .ready_at
                    .checked_duration_since(stats.drain_at)
                    .unwrap_or(Duration::ZERO);
                if waiting > availability_timeout {
                    let _inserted = unavailable.insert(partition.leader_id);
                }
            }
        }
        unavailable
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

    fn next_partition_counter(&mut self, topic: &str) -> usize {
        let next_round_robin = self.next_for_topic(topic);
        let next = usize::try_from(*next_round_robin).unwrap_or(0);
        *next_round_robin = next_round_robin
            .checked_add(1)
            .filter(|value| *value >= 0)
            .unwrap_or(0);
        next
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

fn uniform_partition_for_random(
    topic: &str,
    topic_metadata: &TopicMetadata,
    random: usize,
) -> Result<i32> {
    ensure_partitions(topic, super::record::UNASSIGNED_PARTITION, topic_metadata)?;
    let available_count = topic_metadata
        .partitions
        .iter()
        .filter(|partition| partition.leader_id >= 0)
        .count();
    let partition_count = if available_count > 0 {
        available_count
    } else {
        topic_metadata.partitions.len()
    };
    let offset = random.checked_rem(partition_count).unwrap_or(0);
    let selected = if available_count > 0 {
        topic_metadata
            .partitions
            .iter()
            .filter(|partition| partition.leader_id >= 0)
            .nth(offset)
    } else {
        topic_metadata.partitions.get(offset)
    };
    selected
        .map(|partition| partition.partition_index)
        .ok_or_else(|| ProducerError::UnknownPartition {
            topic: topic.to_owned(),
            partition: super::record::UNASSIGNED_PARTITION,
        })
}

fn build_partition_load_stats(
    queue_sizes: &[i32],
    partition_ids: &[i32],
    length: usize,
) -> Option<PartitionLoadStats> {
    if queue_sizes.len() != partition_ids.len() || length == 0 || length > queue_sizes.len() {
        return None;
    }
    if queue_sizes.len() < 2 {
        return None;
    }
    let logical_sizes = queue_sizes.get(..length)?;
    let logical_partitions = partition_ids.get(..length)?;
    let first = *logical_sizes.first()?;
    let mut max_size = first;
    let mut all_equal = true;
    for size in logical_sizes.iter().copied().skip(1) {
        if size != first {
            all_equal = false;
        }
        if size > max_size {
            max_size = size;
        }
    }
    if all_equal && length == queue_sizes.len() {
        return None;
    }
    let max_size_plus_one = max_size.checked_add(1)?;
    let mut cumulative_frequency_table = Vec::with_capacity(length);
    let mut running = 0i32;
    for size in logical_sizes.iter().copied() {
        let frequency = max_size_plus_one.checked_sub(size)?;
        running = running.checked_add(frequency)?;
        cumulative_frequency_table.push(running);
    }
    if running <= 0 {
        return None;
    }
    Some(PartitionLoadStats {
        cumulative_frequency_table,
        partition_ids: logical_partitions.to_vec(),
    })
}

fn add_partitions_to_txn_request(
    transactional_id: &str,
    identity: ProducerIdentity,
    route: &ProduceRoute,
) -> AddPartitionsToTxnRequestData {
    AddPartitionsToTxnRequestData {
        transactions: vec![AddPartitionsToTxnTransaction {
            transactional_id: KafkaString::from(transactional_id.to_owned()),
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
    }
}

fn txn_offset_commit_topics(
    offsets: Vec<(TopicPartition, OffsetAndMetadata)>,
) -> Vec<TxnOffsetCommitRequestTopic> {
    let mut topics: Vec<TxnOffsetCommitRequestTopic> = Vec::new();
    for (topic_partition, offset) in offsets {
        let partition = TxnOffsetCommitRequestPartition {
            partition_index: topic_partition.partition,
            committed_offset: offset.offset,
            committed_leader_epoch: offset.leader_epoch.unwrap_or(-1),
            committed_metadata: Some(KafkaString::from(offset.metadata.unwrap_or_default())),
            ..TxnOffsetCommitRequestPartition::default()
        };
        if let Some(topic) = topics
            .iter_mut()
            .find(|topic| topic.name.to_string() == topic_partition.topic)
        {
            topic.partitions.push(partition);
        } else {
            topics.push(TxnOffsetCommitRequestTopic {
                name: KafkaString::from(topic_partition.topic),
                partitions: vec![partition],
                _unknown_tagged_fields: Vec::new(),
            });
        }
    }
    topics
}

const fn validate_consumer_group_metadata(group_metadata: &ConsumerGroupMetadata) -> Result<()> {
    if group_metadata.generation_id > 0 && group_metadata.member_id.is_empty() {
        return Err(ProducerError::InvalidConsumerGroupMetadata(
            "generation_id > 0 requires a known member_id",
        ));
    }
    Ok(())
}

fn fail_transaction_state_if_needed(
    state: &ProducerIdempotenceState,
    allow_abortable_abort: bool,
) -> Result<()> {
    if let Some(error) = state.fatal_error {
        return Err(ProducerError::Transaction {
            operation: "transaction_state",
            error,
        });
    }
    if state.transaction_state == TransactionState::FatalError {
        return Err(ProducerError::Transaction {
            operation: "transaction_state",
            error: ErrorCode::InvalidTxnState,
        });
    }
    if !allow_abortable_abort && let Some(error) = state.abortable_error {
        return Err(ProducerError::Transaction {
            operation: "transaction_state",
            error,
        });
    }
    if !allow_abortable_abort && state.transaction_state == TransactionState::AbortableError {
        return Err(ProducerError::Transaction {
            operation: "transaction_state",
            error: ErrorCode::InvalidTxnState,
        });
    }
    Ok(())
}

fn fail_pending_transaction_operation(state: &mut ProducerIdempotenceState) -> Result<()> {
    clear_acked_pending_transaction_operation(state);
    if state.pending_operation.is_some() {
        return Err(ProducerError::InvalidTransactionState(
            PENDING_TRANSACTION_OPERATION_MESSAGE,
        ));
    }
    Ok(())
}

fn clear_acked_pending_transaction_operation(state: &mut ProducerIdempotenceState) {
    let Some(pending_operation) = state.pending_operation else {
        return;
    };
    if state
        .pending_result
        .as_ref()
        .is_some_and(TransactionalRequestResult::is_acked)
    {
        state.clear_pending_transaction_operation(pending_operation);
    }
}

async fn complete_pending_transaction_operation(
    producer_state: &Mutex<ProducerIdempotenceState>,
    operation: TransactionOperation,
) {
    let mut state = producer_state.lock().await;
    let _removed = state
        .pending_requests
        .remove_first(operation.request_kind());
    if state.pending_operation == Some(operation) {
        let keep_for_retry = state.pending_operation_status
            == PendingTransactionOperationStatus::TimedOut
            && state
                .pending_result
                .as_ref()
                .is_some_and(|result| !result.is_acked());
        if !keep_for_retry {
            state.clear_pending_transaction_operation(operation);
        }
    }
}

async fn clear_pending_transaction_operation(
    producer_state: &Mutex<ProducerIdempotenceState>,
    operation: TransactionOperation,
) {
    let mut state = producer_state.lock().await;
    state.clear_pending_transaction_operation(operation);
}

async fn clear_transaction_request(
    producer_state: &Mutex<ProducerIdempotenceState>,
    kind: TransactionRequestKind,
) {
    let mut state = producer_state.lock().await;
    let _removed = state.pending_requests.remove_first(kind);
}

const fn transaction_control_error_for_client(error: ErrorCode) -> ErrorCode {
    if matches!(error, ErrorCode::InvalidProducerEpoch) {
        ErrorCode::ProducerFenced
    } else {
        error
    }
}

const fn is_fatal_transaction_error(error: ErrorCode) -> bool {
    matches!(
        error,
        ErrorCode::ProducerFenced
            | ErrorCode::InvalidProducerEpoch
            | ErrorCode::FencedInstanceId
            | ErrorCode::InvalidTxnState
            | ErrorCode::InvalidProducerIdMapping
            | ErrorCode::TransactionalIdAuthorizationFailed
            | ErrorCode::UnsupportedForMessageFormat
            | ErrorCode::UnsupportedVersion
    )
}

const fn is_fatal_transactional_produce_error(error: ErrorCode) -> bool {
    matches!(
        error,
        ErrorCode::ProducerFenced
            | ErrorCode::InvalidProducerEpoch
            | ErrorCode::InvalidProducerIdMapping
            | ErrorCode::TransactionalIdAuthorizationFailed
            | ErrorCode::ClusterAuthorizationFailed
            | ErrorCode::UnsupportedVersion
    )
}

const fn is_init_producer_abortable_error(error: ErrorCode) -> bool {
    matches!(
        error,
        ErrorCode::TransactionalIdAuthorizationFailed
            | ErrorCode::ClusterAuthorizationFailed
            | ErrorCode::TransactionAbortable
    )
}

const fn is_fatal_abort_transaction_error(error: ErrorCode) -> bool {
    matches!(error, ErrorCode::TransactionAbortable)
}

const fn is_transaction_coordinator_error(error: ErrorCode) -> bool {
    matches!(
        error,
        ErrorCode::CoordinatorNotAvailable | ErrorCode::NotCoordinator
    )
}

fn is_add_partitions_retry_error(error: ErrorCode) -> bool {
    error.is_retriable() || matches!(error, ErrorCode::ConcurrentTransactions)
}

const fn is_txn_offset_commit_coordinator_error(error: ErrorCode) -> bool {
    matches!(
        error,
        ErrorCode::CoordinatorNotAvailable | ErrorCode::NotCoordinator | ErrorCode::RequestTimedOut
    )
}

const fn is_txn_offset_commit_abortable_error(error: ErrorCode) -> bool {
    matches!(
        error,
        ErrorCode::GroupAuthorizationFailed
            | ErrorCode::FencedInstanceId
            | ErrorCode::TransactionAbortable
            | ErrorCode::UnknownMemberId
            | ErrorCode::IllegalGeneration
    )
}

fn txn_offset_commit_error(response: &TxnOffsetCommitResponseData) -> Option<ErrorCode> {
    let mut coordinator_error = None;
    let mut retriable_error = None;
    for error in response
        .topics
        .iter()
        .flat_map(|topic| topic.partitions.iter())
        .map(|partition| ErrorCode::from(partition.error_code))
        .filter(ErrorCode::is_error)
    {
        if is_txn_offset_commit_coordinator_error(error) {
            let _ = coordinator_error.get_or_insert(error);
            continue;
        }
        if error.is_retriable() {
            let _ = retriable_error.get_or_insert(error);
            continue;
        }
        return Some(error);
    }
    coordinator_error.or(retriable_error)
}

impl ProducerIdempotenceState {
    const fn transition_to(&mut self, target: TransactionState) -> Result<()> {
        if !self.transaction_state.is_transition_valid(target) {
            self.transaction_state = TransactionState::FatalError;
            self.fatal_error = Some(ErrorCode::InvalidTxnState);
            return Err(ProducerError::InvalidTransactionState(
                "invalid transaction state transition",
            ));
        }
        self.transaction_state = target;
        match target {
            TransactionState::InTransaction
            | TransactionState::PreparedTransaction
            | TransactionState::CommittingTransaction
            | TransactionState::AbortingTransaction
            | TransactionState::AbortableError => {
                self.in_transaction = true;
            },
            TransactionState::Uninitialized
            | TransactionState::Initializing
            | TransactionState::Ready
            | TransactionState::FatalError => {
                self.in_transaction = false;
            },
        }
        Ok(())
    }

    fn mark_new_transaction_partition(&mut self, key: TopicPartitionKey) -> bool {
        if self.transaction_contains_partition(&key) || self.is_partition_pending_add(&key) {
            return false;
        }
        self.new_partitions_in_transaction.insert(key)
    }

    fn begin_pending_transaction_partitions(&mut self) -> Vec<TopicPartitionKey> {
        let pending: Vec<_> = self.new_partitions_in_transaction.drain().collect();
        self.pending_partitions_in_transaction
            .extend(pending.iter().cloned());
        pending
    }

    fn complete_pending_transaction_partitions(&mut self, partitions: &[TopicPartitionKey]) {
        for partition in partitions {
            let _removed = self.pending_partitions_in_transaction.remove(partition);
            let _inserted = self.partitions_in_transaction.insert(partition.clone());
            let _inserted = self.transaction_partitions.insert(partition.clone());
        }
    }

    fn fail_pending_transaction_partitions(&mut self, partitions: &[TopicPartitionKey]) {
        for partition in partitions {
            let _removed = self.pending_partitions_in_transaction.remove(partition);
        }
    }

    fn is_partition_pending_add(&self, key: &TopicPartitionKey) -> bool {
        self.new_partitions_in_transaction.contains(key)
            || self.pending_partitions_in_transaction.contains(key)
    }

    fn transaction_contains_partition(&self, key: &TopicPartitionKey) -> bool {
        self.partitions_in_transaction.contains(key) || self.transaction_partitions.contains(key)
    }

    fn clear_pending_transaction_operation(&mut self, operation: TransactionOperation) {
        let _removed = self.pending_requests.remove_first(operation.request_kind());
        if self.pending_operation == Some(operation) {
            self.pending_operation = None;
            self.pending_result = None;
            self.pending_operation_status = PendingTransactionOperationStatus::Active;
        }
    }

    fn begin_pending_transaction_operation(
        &mut self,
        operation: TransactionOperation,
    ) -> Result<TransactionPendingOperationStart> {
        if let Some(pending_operation) = self.pending_operation {
            if let Some(result) = self.pending_result.clone() {
                if result.is_acked() {
                    self.clear_pending_transaction_operation(pending_operation);
                } else if pending_operation == operation {
                    return Ok(TransactionPendingOperationStart::Cached(result));
                } else {
                    return Err(ProducerError::InvalidTransactionState(
                        PENDING_TRANSACTION_OPERATION_MESSAGE,
                    ));
                }
            } else {
                return Err(ProducerError::InvalidTransactionState(
                    PENDING_TRANSACTION_OPERATION_MESSAGE,
                ));
            }
        }

        let result = TransactionalRequestResult::new();
        self.pending_operation = Some(operation);
        self.pending_result = Some(result.clone());
        self.pending_operation_status = PendingTransactionOperationStatus::Active;
        self.pending_requests.push(operation.request_kind());
        Ok(TransactionPendingOperationStart::Started(result))
    }

    fn reset_transaction_after_end(&mut self, clear_abortable_error: bool) {
        self.in_transaction = false;
        self.transaction_state = TransactionState::Ready;
        self.transaction_started = false;
        self.new_partitions_in_transaction.clear();
        self.pending_partitions_in_transaction.clear();
        self.partitions_in_transaction.clear();
        self.transaction_partitions.clear();
        if clear_abortable_error {
            self.abortable_error = None;
        }
    }

    fn reset_sequences_after_epoch_bump(&mut self) {
        self.sequences.clear();
        self.unresolved_sequences.clear();
        self.epoch_bump_required = false;
    }

    fn next_sequence(&mut self, topic: &str, partition: i32, record_count: i32) -> Result<i32> {
        let key = TopicPartitionKey {
            topic: topic.to_owned(),
            partition,
        };
        if self.unresolved_sequences.contains_key(&key) {
            return Err(ProducerError::UnresolvedSequence {
                topic: topic.to_owned(),
                partition,
            });
        }
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

    fn mark_sequence_unresolved(
        &mut self,
        topic: &str,
        partition: i32,
        base_sequence: i32,
        record_count: i32,
    ) {
        let key = TopicPartitionKey {
            topic: topic.to_owned(),
            partition,
        };
        let next_sequence = base_sequence.saturating_add(record_count);
        let _sequence = self
            .unresolved_sequences
            .entry(key)
            .and_modify(|sequence| *sequence = (*sequence).max(next_sequence))
            .or_insert(next_sequence);
    }

    fn reset_sequence(&mut self, topic: &str, partition: i32) {
        let key = TopicPartitionKey {
            topic: topic.to_owned(),
            partition,
        };
        let _removed = self.unresolved_sequences.remove(&key);
        let _previous = self.sequences.insert(key, 0);
    }

    fn release_sequence(&mut self, topic: &str, partition: i32, base_sequence: i32) {
        let key = TopicPartitionKey {
            topic: topic.to_owned(),
            partition,
        };
        if self
            .unresolved_sequences
            .get(&key)
            .is_some_and(|sequence| *sequence <= base_sequence)
        {
            let _removed = self.unresolved_sequences.remove(&key);
        }
    }

    fn should_reset_sequence_for_idempotent_retry(
        &self,
        decision: IdempotentRetryDecision<'_>,
    ) -> bool {
        if matches!(decision.error, ErrorCode::UnknownProducerId) {
            return decision.log_start_offset != -1;
        }
        if !matches!(decision.error, ErrorCode::OutOfOrderSequenceNumber) {
            return true;
        }
        let Some(base_sequence) = decision.base_sequence else {
            return true;
        };
        let key = TopicPartitionKey {
            topic: decision.topic.to_owned(),
            partition: decision.partition,
        };
        self.unresolved_sequences
            .get(&key)
            .is_none_or(|unresolved_next_sequence| base_sequence == *unresolved_next_sequence)
    }

    fn rewind_sequence_to(&mut self, topic: &str, partition: i32, base_sequence: i32) {
        let key = TopicPartitionKey {
            topic: topic.to_owned(),
            partition,
        };
        let _removed = self.unresolved_sequences.remove(&key);
        let current = self.sequences.entry(key).or_insert(base_sequence);
        if *current >= base_sequence {
            *current = base_sequence;
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct TopicPartitionKey {
    topic: String,
    partition: i32,
}

impl From<&ProduceRoute> for TopicPartitionKey {
    fn from(route: &ProduceRoute) -> Self {
        Self {
            topic: route.topic.clone(),
            partition: route.partition,
        }
    }
}

#[derive(Debug)]
pub(crate) enum DispatchOutcome {
    Delivered(Result<Vec<RecordMetadata>>),
    Requeue(Vec<ReadyBatch>),
}

#[derive(Debug)]
enum DispatchError {
    Producer(ProducerError),
    Requeue,
    RetryableLeadership {
        topic: String,
        partition: i32,
        error: ErrorCode,
    },
    RetryableIdempotent {
        topic: String,
        partition: i32,
        leader_id: i32,
        error: ErrorCode,
        reset_sequence: bool,
    },
}

struct IdempotentRetry {
    topic: String,
    partition: i32,
    leader_id: i32,
    error: ErrorCode,
    reset_sequence: bool,
}

#[derive(Clone, Copy)]
struct IdempotentRetryDecision<'a> {
    topic: &'a str,
    partition: i32,
    error: ErrorCode,
    log_start_offset: i64,
    base_sequence: Option<i32>,
}

impl IdempotentRetry {
    fn broker_error(&self) -> ProducerError {
        ProducerError::Broker {
            topic: self.topic.clone(),
            partition: self.partition,
            error: self.error,
        }
    }
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
    record_count: usize,
    batch_count: usize,
    payload_bytes: usize,
}

impl BrokerProduceRequest {
    fn contains_route(&self, route: &ProduceRoute) -> bool {
        self.routes.iter().any(|existing| {
            existing.topic_id == route.topic_id && existing.partition == route.partition
        })
    }

    fn encoded_len_after_push(
        &self,
        route: &ProduceRoute,
        records: &bytes::Bytes,
        options: BrokerProduceOptions<'_>,
        version: i16,
    ) -> std::result::Result<usize, DispatchError> {
        let mut request_data = self.data.clone();
        apply_produce_options(&mut request_data, options);
        push_partition(&mut request_data.topic_data, route, records.clone());
        RequestMessage::encoded_len(&request_data, version)
            .map_err(crate::wire::WireError::from)
            .map_err(DispatchError::from)
    }

    fn encoded_len_upper_bound_after_push(
        &self,
        route: &ProduceRoute,
        records: &bytes::Bytes,
        options: BrokerProduceOptions<'_>,
    ) -> usize {
        const PRODUCE_REQUEST_BASE_OVERHEAD: usize = 256;
        const PRODUCE_ROUTE_OVERHEAD: usize = 256;

        let transactional_id_bytes = options
            .transactional_id
            .map_or(1usize, |id| id.len().saturating_add(8));
        let existing_route_overhead = self.routes.iter().fold(0usize, |bytes, route| {
            bytes
                .saturating_add(PRODUCE_ROUTE_OVERHEAD)
                .saturating_add(route.topic.len())
        });
        self.payload_bytes
            .saturating_add(records.len())
            .saturating_add(PRODUCE_REQUEST_BASE_OVERHEAD)
            .saturating_add(transactional_id_bytes)
            .saturating_add(existing_route_overhead)
            .saturating_add(PRODUCE_ROUTE_OVERHEAD)
            .saturating_add(route.topic.len())
    }

    fn push(
        &mut self,
        mut route: ProduceRoute,
        records: bytes::Bytes,
        options: BrokerProduceOptions<'_>,
        record_count: usize,
    ) {
        apply_produce_options(&mut self.data, options);
        route.request_offset_delta = self
            .records_before_route(&route)
            .and_then(|count| i64::try_from(count).ok())
            .unwrap_or(i64::MAX);
        route.record_count = record_count;
        self.record_count = self.record_count.saturating_add(record_count);
        self.batch_count = self.batch_count.saturating_add(1);
        self.payload_bytes = self.payload_bytes.saturating_add(records.len());
        push_partition(&mut self.data.topic_data, &route, records);
        self.routes.push(route);
    }

    fn records_before_route(&self, route: &ProduceRoute) -> Option<usize> {
        self.routes
            .iter()
            .filter(|existing| {
                existing.topic_id == route.topic_id && existing.partition == route.partition
            })
            .try_fold(0usize, |count, existing| {
                count.checked_add(existing.record_count)
            })
    }
}

#[derive(Debug, Clone, Copy)]
struct ProduceRequestSizing {
    version: i16,
    max_request_size: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BrokerRequestPlacement {
    index: usize,
    split: bool,
}

fn broker_request_placement_for_batch(
    requests: &[BrokerProduceRequest],
    route: &ProduceRoute,
    records: &bytes::Bytes,
    options: BrokerProduceOptions<'_>,
    sizing: ProduceRequestSizing,
) -> std::result::Result<BrokerRequestPlacement, DispatchError> {
    let max_request_size = sizing.max_request_size.max(1);
    let mut size_split_candidate = false;
    for (index, request) in requests.iter().enumerate() {
        if request.contains_route(route) {
            continue;
        }
        size_split_candidate = true;
        if request.encoded_len_upper_bound_after_push(route, records, options) <= max_request_size {
            return Ok(BrokerRequestPlacement {
                index,
                split: false,
            });
        }
        if request.encoded_len_after_push(route, records, options, sizing.version)?
            <= max_request_size
        {
            return Ok(BrokerRequestPlacement {
                index,
                split: false,
            });
        }
    }
    let empty_request = BrokerProduceRequest::default();
    let encoded_len =
        empty_request.encoded_len_after_push(route, records, options, sizing.version)?;
    if encoded_len > max_request_size {
        return Err(DispatchError::Producer(ProducerError::RecordTooLarge {
            size: encoded_len,
            max_request_size,
        }));
    }
    Ok(BrokerRequestPlacement {
        index: requests.len(),
        split: size_split_candidate,
    })
}

fn request_batch_base_offset(
    requests: &[BrokerProduceRequest],
    index: usize,
    route: &ProduceRoute,
) -> std::result::Result<i64, DispatchError> {
    let Some(request) = requests.get(index) else {
        return Ok(0);
    };
    let count = request
        .records_before_route(route)
        .ok_or_else(|| sequence_overflow(route))?;
    i64::try_from(count).map_err(|_error| sequence_overflow(route))
}

fn sequence_overflow(route: &ProduceRoute) -> DispatchError {
    DispatchError::Producer(ProducerError::SequenceOverflow {
        topic: route.topic.clone(),
        partition: route.partition,
    })
}

fn apply_produce_options(request_data: &mut ProduceRequestData, options: BrokerProduceOptions<'_>) {
    request_data.acks = options.acks;
    request_data.timeout_ms = options.timeout_ms;
    request_data.transactional_id = options
        .transactional_id
        .map(|id| KafkaString::from(id.to_owned()));
}

fn pop_dispatchable_broker_request(
    pending: &mut VecDeque<(usize, BrokerProduceRequest)>,
    in_flight_routes: &AHashSet<TopicPartitionKey>,
) -> Option<(usize, BrokerProduceRequest)> {
    let dispatch_index = pending.iter().position(|(_index, request)| {
        !request_conflicts_with_in_flight(request, in_flight_routes)
    })?;
    pending.remove(dispatch_index)
}

fn request_conflicts_with_in_flight(
    request: &BrokerProduceRequest,
    in_flight_routes: &AHashSet<TopicPartitionKey>,
) -> bool {
    request
        .routes
        .iter()
        .map(TopicPartitionKey::from)
        .any(|route| in_flight_routes.contains(&route))
}

#[derive(Debug, Clone, Copy)]
struct BrokerProduceTarget {
    broker_id: i32,
    version: i16,
}

#[derive(Debug)]
struct ProduceBackpressureRecorder {
    partitioner_state: Option<Arc<Mutex<ProducerPartitionerState>>>,
    metrics: ProducerMetrics,
    metrics_enabled: bool,
}

impl ProduceBackpressureRecorder {
    const fn new(
        partitioner_state: Option<Arc<Mutex<ProducerPartitionerState>>>,
        metrics: ProducerMetrics,
        metrics_enabled: bool,
    ) -> Self {
        Self {
            partitioner_state,
            metrics,
            metrics_enabled,
        }
    }

    async fn record(&self, broker_id: i32) {
        if self.metrics_enabled {
            self.metrics.record_in_flight_stall();
        }
        record_broker_backpressure(self.partitioner_state.as_ref(), broker_id).await;
    }
}

async fn send_produce_with_backpressure_retry(
    wire: &WireClient,
    target: BrokerProduceTarget,
    request: &BrokerProduceRequest,
    backpressure: ProduceBackpressureRecorder,
) -> crate::wire::Result<ProduceDispatchResponse> {
    loop {
        let result = if request.data.acks == ACKS_NONE {
            wire.send_to_broker_without_response(
                target.broker_id,
                ApiKey::Produce,
                target.version,
                &request.data,
            )
            .await
            .map(|()| ProduceDispatchResponse::NoAcknowledgement)
        } else {
            wire.send_to_broker::<_, kacrab_protocol::generated::ProduceResponseData>(
                target.broker_id,
                ApiKey::Produce,
                target.version,
                &request.data,
            )
            .await
            .map(ProduceDispatchResponse::Acknowledged)
        };
        match result {
            Err(crate::wire::WireError::Backpressure) => {
                backpressure.record(target.broker_id).await;
                tokio::task::yield_now().await;
            },
            result => return result,
        }
    }
}

async fn enqueue_produce_with_backpressure_retry(
    wire: &WireClient,
    target: BrokerProduceTarget,
    request: &BrokerProduceRequest,
    backpressure: ProduceBackpressureRecorder,
) -> crate::wire::Result<
    crate::wire::PendingBrokerResponse<kacrab_protocol::generated::ProduceResponseData>,
> {
    loop {
        let result = wire.enqueue_to_broker::<_, kacrab_protocol::generated::ProduceResponseData>(
            target.broker_id,
            ApiKey::Produce,
            target.version,
            &request.data,
        );
        match result {
            Err(crate::wire::WireError::Backpressure) => {
                backpressure.record(target.broker_id).await;
                tokio::task::yield_now().await;
            },
            result => return result,
        }
    }
}

async fn record_broker_backpressure(
    partitioner_state: Option<&Arc<Mutex<ProducerPartitionerState>>>,
    broker_id: i32,
) {
    let Some(partitioner_state) = partitioner_state else {
        return;
    };
    let mut state = partitioner_state.lock().await;
    state.update_broker_latency_stats(broker_id, Instant::now(), false);
}

#[derive(Debug)]
enum ProduceDispatchResponse {
    Acknowledged(kacrab_protocol::generated::ProduceResponseData),
    NoAcknowledgement,
}

fn no_ack_receipts(routes: &[ProduceRoute]) -> Vec<RecordMetadata> {
    routes
        .iter()
        .map(|route| RecordMetadata {
            topic: Arc::from(route.topic.as_str()),
            partition: route.partition,
            leader_id: route.leader_id,
            offset: -1,
            timestamp_ms: -1,
            serialized_key_size: -1,
            serialized_value_size: -1,
        })
        .collect()
}

#[derive(Debug, Clone, Copy)]
struct BrokerProduceOptions<'a> {
    acks: i16,
    timeout_ms: i32,
    transactional_id: Option<&'a str>,
}

fn push_partition(topics: &mut Vec<TopicProduceData>, route: &ProduceRoute, records: bytes::Bytes) {
    if let Some(topic) = topics
        .iter_mut()
        .find(|topic| topic.topic_id == route.topic_id)
    {
        if let Some(partition) = topic
            .partition_data
            .iter_mut()
            .find(|partition| partition.index == route.partition)
        {
            append_partition_records(partition, records);
            return;
        }
        topic.partition_data.push(partition_data(route, records));
        return;
    }
    topics.push(TopicProduceData {
        name: KafkaString::from(route.topic.clone()),
        topic_id: route.topic_id,
        partition_data: vec![partition_data(route, records)],
        _unknown_tagged_fields: Vec::new(),
    });
}

fn append_partition_records(partition: &mut PartitionProduceData, records: bytes::Bytes) {
    match partition.records.take() {
        Some(existing) => {
            let mut combined =
                BytesMut::with_capacity(existing.len().saturating_add(records.len()));
            combined.extend_from_slice(&existing);
            combined.extend_from_slice(&records);
            partition.records = Some(combined.freeze());
        },
        None => {
            partition.records = Some(records);
        },
    }
}

const fn partition_data(route: &ProduceRoute, records: bytes::Bytes) -> PartitionProduceData {
    PartitionProduceData {
        index: route.partition,
        records: Some(records),
        _unknown_tagged_fields: Vec::new(),
    }
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

fn complete_deliveries(batches: &mut [ReadyBatch], receipts: &[RecordMetadata]) {
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
                    && receipt.topic.as_ref() == batch.topic
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

const fn is_leadership_error(error: ErrorCode) -> bool {
    matches!(
        error,
        ErrorCode::LeaderNotAvailable
            | ErrorCode::NotLeaderOrFollower
            | ErrorCode::UnknownLeaderEpoch
            | ErrorCode::FencedLeaderEpoch
    )
}

const fn is_idempotent_retry_error(error: ErrorCode) -> bool {
    matches!(
        error,
        ErrorCode::UnknownProducerId | ErrorCode::OutOfOrderSequenceNumber
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
/// Kafka 4.3 marks Produce v12+ as transaction V2.
const TRANSACTION_V1_PRODUCE_MAX_VERSION: i16 = 11;
/// Kafka 4.3 marks `TxnOffsetCommit` v5+ as transaction V2.
const TRANSACTION_V1_TXN_OFFSET_COMMIT_MAX_VERSION: i16 = 4;
/// Kafka 4.3 marks `EndTxn` v5+ as transaction V2.
const TRANSACTION_V1_END_TXN_MAX_VERSION: i16 = 4;

fn init_producer_id_version(transaction_two_phase_commit: bool) -> i16 {
    if transaction_two_phase_commit {
        client_api_info(ApiKey::InitProducerId).max_version
    } else {
        client_api_info(ApiKey::InitProducerId)
            .max_version
            .min(NON_2PC_INIT_PRODUCER_ID_MAX_VERSION)
    }
}

fn produce_version(transaction_two_phase_commit: bool) -> i16 {
    if transaction_two_phase_commit {
        client_api_info(ApiKey::Produce).max_version
    } else {
        client_api_info(ApiKey::Produce)
            .max_version
            .min(TRANSACTION_V1_PRODUCE_MAX_VERSION)
    }
}

fn txn_offset_commit_version(transaction_two_phase_commit: bool) -> i16 {
    if transaction_two_phase_commit {
        client_api_info(ApiKey::TxnOffsetCommit).max_version
    } else {
        client_api_info(ApiKey::TxnOffsetCommit)
            .max_version
            .min(TRANSACTION_V1_TXN_OFFSET_COMMIT_MAX_VERSION)
    }
}

fn end_txn_version(transaction_two_phase_commit: bool) -> i16 {
    if transaction_two_phase_commit {
        client_api_info(ApiKey::EndTxn).max_version
    } else {
        client_api_info(ApiKey::EndTxn)
            .max_version
            .min(TRANSACTION_V1_END_TXN_MAX_VERSION)
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
        collections::VecDeque,
        net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
        sync::Arc,
        time::{Duration, Instant},
    };

    use ahash::AHashSet;
    use bytes::Bytes;
    use kacrab_protocol::{
        KafkaString, KafkaUuid,
        generated::{
            AddPartitionsToTxnPartitionResult, AddPartitionsToTxnResponseData,
            AddPartitionsToTxnResult, AddPartitionsToTxnTopicResult, ApiKey, ErrorCode,
        },
        version::client_api_info,
    };

    use super::{
        BrokerProduceOptions, BrokerProduceRequest, BrokerRequestPlacement, DispatchError,
        IdempotentRetryDecision, PartitionLoadRefresh, PendingTransactionOperationGuard,
        ProduceRequestSizing, ProducerDispatcher, ProducerIdempotenceState,
        ProducerPartitionerState, RECORD_BATCH_OVERHEAD_BYTES, TopicPartitionKey,
        TransactionOperation, TransactionPendingOperationStart, broker_request_placement_for_batch,
        build_partition_load_stats, choose_coordinator_addr, complete_deliveries, end_txn_version,
        estimate_record_batch_bytes, fail_pending_transaction_operation, init_producer_id_version,
        is_fatal_transaction_error, is_leadership_error, no_ack_receipts,
        pop_dispatchable_broker_request, produce_version, txn_offset_commit_version,
        uniform_partition_for_random, unique_topics, unique_unassigned_record_topics,
        validate_consumer_group_metadata,
    };
    use crate::{
        producer::{
            AccumulatorConfig, ConsumerGroupMetadata, ProducerError, ProducerIdempotenceConfig,
            ProducerIdentity, ProducerRecord, ProducerRuntimeConfig, RecordAccumulator,
            RecordMetadata,
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
            ProducerRuntimeConfig {
                idempotence: ProducerIdempotenceConfig {
                    enabled: true,
                    transactional_id: Some("txn-a".to_owned()),
                    transaction_timeout_ms: 30_000,
                    transaction_two_phase_commit: false,
                },
                ..ProducerRuntimeConfig::default()
            },
        )
    }

    #[test]
    fn dispatcher_metrics_enablement_is_shared_across_clones() {
        let dispatcher = ProducerDispatcher::new(test_wire());
        let cloned = dispatcher.clone();

        dispatcher.enable_metrics();

        assert!(cloned.metrics_are_enabled());
    }

    fn route(topic: &str, partition: i32) -> super::ProduceRoute {
        super::ProduceRoute {
            topic: topic.to_owned(),
            partition,
            topic_id: KafkaUuid::ZERO,
            leader_id: 7,
            base_sequence: None,
            request_offset_delta: 0,
            record_count: 0,
        }
    }

    #[test]
    fn idempotent_dispatch_keeps_configured_broker_in_flight_limit() {
        let idempotent = ProducerDispatcher::with_config(
            test_wire(),
            ProducerRuntimeConfig {
                max_in_flight_requests_per_connection: 5,
                idempotence: ProducerIdempotenceConfig {
                    enabled: true,
                    ..ProducerIdempotenceConfig::default()
                },
                ..ProducerRuntimeConfig::default()
            },
        );
        let non_idempotent = ProducerDispatcher::with_config(
            test_wire(),
            ProducerRuntimeConfig {
                max_in_flight_requests_per_connection: 5,
                idempotence: ProducerIdempotenceConfig {
                    enabled: false,
                    ..ProducerIdempotenceConfig::default()
                },
                ..ProducerRuntimeConfig::default()
            },
        );

        assert_eq!(idempotent.broker_dispatch_in_flight_limit(), 5);
        assert_eq!(non_idempotent.broker_dispatch_in_flight_limit(), 5);
    }

    #[test]
    fn transaction_control_unsupported_version_is_fatal_like_java() {
        assert!(is_fatal_transaction_error(ErrorCode::UnsupportedVersion));
    }

    #[test]
    fn transaction_request_queue_orders_requests_like_java_transaction_manager() {
        let mut queue = super::TransactionRequestQueue::default();
        queue.push(super::TransactionRequestKind::EndTxn);
        queue.push(super::TransactionRequestKind::AddPartitionsOrOffsets);
        queue.push(super::TransactionRequestKind::EpochBump);
        queue.push(super::TransactionRequestKind::FindCoordinator);
        queue.push(super::TransactionRequestKind::InitProducerId);
        queue.push(super::TransactionRequestKind::AddPartitionsOrOffsets);

        let mut ordered = Vec::new();
        while let Some(kind) = queue.pop_next() {
            ordered.push(kind);
        }

        assert_eq!(
            ordered,
            vec![
                super::TransactionRequestKind::FindCoordinator,
                super::TransactionRequestKind::InitProducerId,
                super::TransactionRequestKind::AddPartitionsOrOffsets,
                super::TransactionRequestKind::AddPartitionsOrOffsets,
                super::TransactionRequestKind::EndTxn,
                super::TransactionRequestKind::EpochBump,
            ]
        );
    }

    #[test]
    fn transaction_state_transition_matrix_matches_java_transaction_manager() {
        use super::super::transaction::TransactionState;

        let allowed = [
            (TransactionState::Ready, TransactionState::Uninitialized),
            (
                TransactionState::AbortableError,
                TransactionState::Uninitialized,
            ),
            (
                TransactionState::Uninitialized,
                TransactionState::Initializing,
            ),
            (
                TransactionState::CommittingTransaction,
                TransactionState::Initializing,
            ),
            (
                TransactionState::AbortingTransaction,
                TransactionState::Initializing,
            ),
            (TransactionState::Initializing, TransactionState::Ready),
            (
                TransactionState::CommittingTransaction,
                TransactionState::Ready,
            ),
            (
                TransactionState::AbortingTransaction,
                TransactionState::Ready,
            ),
            (TransactionState::Ready, TransactionState::InTransaction),
            (
                TransactionState::InTransaction,
                TransactionState::PreparedTransaction,
            ),
            (
                TransactionState::Initializing,
                TransactionState::PreparedTransaction,
            ),
            (
                TransactionState::InTransaction,
                TransactionState::CommittingTransaction,
            ),
            (
                TransactionState::PreparedTransaction,
                TransactionState::CommittingTransaction,
            ),
            (
                TransactionState::InTransaction,
                TransactionState::AbortingTransaction,
            ),
            (
                TransactionState::PreparedTransaction,
                TransactionState::AbortingTransaction,
            ),
            (
                TransactionState::AbortableError,
                TransactionState::AbortingTransaction,
            ),
            (
                TransactionState::InTransaction,
                TransactionState::AbortableError,
            ),
            (
                TransactionState::CommittingTransaction,
                TransactionState::AbortableError,
            ),
            (
                TransactionState::AbortableError,
                TransactionState::AbortableError,
            ),
            (
                TransactionState::Initializing,
                TransactionState::AbortableError,
            ),
        ];

        for source in TransactionState::ALL {
            for target in TransactionState::ALL {
                let expected =
                    target == TransactionState::FatalError || allowed.contains(&(source, target));
                assert_eq!(
                    source.is_transition_valid(target),
                    expected,
                    "transition {source:?} -> {target:?}"
                );
            }
        }
    }

    #[test]
    fn transaction_request_queue_holds_end_txn_until_batches_drain_like_java() {
        let mut queue = super::TransactionRequestQueue::default();
        queue.push(super::TransactionRequestKind::EndTxn);
        queue.push(super::TransactionRequestKind::EpochBump);

        assert_eq!(queue.next_request(true), None);
        assert_eq!(
            queue.next_request(false),
            Some(super::TransactionRequestKind::EndTxn)
        );
        assert_eq!(
            queue.next_request(false),
            Some(super::TransactionRequestKind::EpochBump)
        );
    }

    #[test]
    fn transaction_partition_sets_move_new_to_pending_to_in_transaction_like_java() {
        let mut state = ProducerIdempotenceState::default();
        let key = TopicPartitionKey {
            topic: "orders".to_owned(),
            partition: 0,
        };

        assert!(state.mark_new_transaction_partition(key.clone()));
        assert!(!state.mark_new_transaction_partition(key.clone()));
        assert!(state.is_partition_pending_add(&key));
        assert!(!state.transaction_contains_partition(&key));

        let pending = state.begin_pending_transaction_partitions();
        assert_eq!(pending, vec![key.clone()]);
        assert!(state.new_partitions_in_transaction.is_empty());
        assert!(state.pending_partitions_in_transaction.contains(&key));
        assert!(state.is_partition_pending_add(&key));

        state.complete_pending_transaction_partitions(&pending);
        assert!(!state.is_partition_pending_add(&key));
        assert!(state.transaction_contains_partition(&key));
    }

    #[test]
    fn consumer_group_metadata_requires_member_for_known_generation() {
        assert!(matches!(
            validate_consumer_group_metadata(
                &ConsumerGroupMetadata::new("group-a").generation_id(42)
            ),
            Err(ProducerError::InvalidConsumerGroupMetadata(message))
                if message == "generation_id > 0 requires a known member_id"
        ));

        validate_consumer_group_metadata(
            &ConsumerGroupMetadata::new("group-a")
                .generation_id(42)
                .member_id("member-a"),
        )
        .expect("known generation with member id is valid");
    }

    #[test]
    fn broker_request_scheduler_skips_in_flight_partition_without_reordering_it() {
        let options = BrokerProduceOptions {
            acks: 1,
            timeout_ms: 1_500,
            transactional_id: None,
        };
        let mut first_partition = BrokerProduceRequest::default();
        first_partition.push(
            route("orders", 0),
            Bytes::from_static(b"records-0a"),
            options,
            1,
        );
        let mut other_partition = BrokerProduceRequest::default();
        other_partition.push(
            route("orders", 1),
            Bytes::from_static(b"records-1"),
            options,
            1,
        );
        let mut next_first_partition = BrokerProduceRequest::default();
        next_first_partition.push(
            route("orders", 0),
            Bytes::from_static(b"records-0b"),
            options,
            1,
        );
        let mut pending = VecDeque::from([
            (0, first_partition),
            (1, other_partition),
            (2, next_first_partition),
        ]);
        let mut in_flight_routes = AHashSet::new();
        let _inserted = in_flight_routes.insert(TopicPartitionKey {
            topic: "orders".to_owned(),
            partition: 0,
        });

        let Some((index, request)) =
            pop_dispatchable_broker_request(&mut pending, &in_flight_routes)
        else {
            panic!("other partition should remain dispatchable");
        };
        assert_eq!(index, 1);
        assert_eq!(
            request
                .routes
                .first()
                .expect("route should exist")
                .partition,
            1
        );

        in_flight_routes.clear();
        let Some((index, request)) =
            pop_dispatchable_broker_request(&mut pending, &in_flight_routes)
        else {
            panic!("first partition should dispatch after in-flight completion");
        };
        assert_eq!(index, 0);
        assert_eq!(
            request
                .routes
                .first()
                .expect("route should exist")
                .partition,
            0
        );
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

        let partitions: Vec<_> = (0..4)
            .map(|_| {
                state
                    .partition_for_record(&metadata, &record, false, true, 128)
                    .expect("partition")
            })
            .collect();
        assert!(
            partitions
                .iter()
                .all(|partition| *partition == partitions[0])
        );

        state.mark_sticky_batch_ready("orders", 128);
        let _next_partition = state
            .partition_for_record(&metadata, &record, false, true, 128)
            .expect("partition");
        let sticky = state
            .sticky_by_topic
            .get("orders")
            .expect("sticky state should exist");

        assert!(!sticky.switch_on_next);
    }

    #[test]
    fn sticky_partitioner_waits_for_batch_ready_before_switching_like_java() {
        let metadata = metadata_with_partitions("orders", 3);
        let mut state = ProducerPartitionerState::default();
        let record = ProducerRecord::unassigned("orders").value(Bytes::from_static(b"1234567890"));
        let sticky_batch_size =
            RECORD_BATCH_OVERHEAD_BYTES + estimate_record_batch_bytes(&record) + 1;

        let partitions: Vec<_> = (0..3)
            .map(|_| {
                state
                    .partition_for_record(&metadata, &record, false, true, sticky_batch_size)
                    .expect("partition")
            })
            .collect();

        assert!(
            partitions
                .iter()
                .all(|partition| *partition == partitions[0])
        );
    }

    #[test]
    fn cached_sticky_partition_assigns_without_metadata_until_batch_ready() {
        let metadata = metadata_with_partitions("orders", 3);
        let mut state = ProducerPartitionerState::default();
        let record = ProducerRecord::unassigned("orders").value(Bytes::from_static(b"1234567890"));
        let sticky_batch_size = 1024;
        let partition = state
            .partition_for_record(&metadata, &record, false, true, sticky_batch_size)
            .expect("initial sticky partition");

        let mut cached_record =
            ProducerRecord::unassigned("orders").value(Bytes::from_static(b"cached"));
        assert!(state.try_assign_cached_sticky_partition(&mut cached_record, sticky_batch_size));
        assert_eq!(cached_record.partition, partition);

        state.mark_sticky_batch_ready("orders", 1);
        let mut next_record =
            ProducerRecord::unassigned("orders").value(Bytes::from_static(b"next"));
        assert!(!state.try_assign_cached_sticky_partition(&mut next_record, sticky_batch_size));
        assert_eq!(
            next_record.partition,
            crate::producer::record::UNASSIGNED_PARTITION
        );
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
                    .partition_for_record(&metadata, &record, false, true, 256)
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

        let partitions: Vec<_> = (0..3)
            .map(|_| {
                state
                    .partition_for_record(&metadata, &record, true, true, 128)
                    .expect("partition")
            })
            .collect();
        assert!(
            partitions
                .iter()
                .all(|partition| *partition == partitions[0])
        );

        state.mark_sticky_batch_ready("orders", 128);
        let after_switch: Vec<_> = (0..2)
            .map(|_| {
                state
                    .partition_for_record(&metadata, &record, true, true, 128)
                    .expect("partition")
            })
            .collect();

        assert!(
            after_switch
                .iter()
                .all(|partition| *partition == after_switch[0])
        );
    }

    #[test]
    fn sticky_partitioner_fallback_skips_unavailable_partitions_like_java() {
        let mut metadata = metadata_with_partitions("orders", 3);
        metadata.topics[0].partitions[0].leader_id = -1;
        metadata.topics[0].partitions[2].leader_id = -1;
        let mut state = ProducerPartitionerState::default();
        let record = ProducerRecord::unassigned("orders");

        let partitions: Vec<_> = (0..3)
            .map(|_| {
                state
                    .partition_for_record(&metadata, &record, true, true, 1)
                    .expect("partition")
            })
            .collect();

        assert_eq!(partitions, vec![1, 1, 1]);
    }

    #[test]
    fn adaptive_sticky_builds_weighted_load_stats_like_java() {
        let stats = build_partition_load_stats(&[0, 3, 1], &[0, 1, 2], 3).expect("adaptive stats");

        assert_eq!(stats.cumulative_frequency_table, vec![4, 5, 8]);
        assert_eq!(stats.partition_ids, vec![0, 1, 2]);
        assert!(build_partition_load_stats(&[1, 1, 1], &[0, 1, 2], 3).is_none());
    }

    #[test]
    fn adaptive_sticky_selects_partition_from_weighted_frequency_table() {
        let metadata = metadata_with_partitions("orders", 3);
        let topic_metadata = metadata.topic("orders").expect("topic metadata");
        let mut state = ProducerPartitionerState::default();
        state.update_partition_load_stats("orders", &[0, 3, 1], &[0, 1, 2], 3);

        let partition = state
            .adaptive_partition_for_random("orders", topic_metadata, 4)
            .expect("adaptive partition");

        assert_eq!(partition, 1);
    }

    #[test]
    fn adaptive_sticky_distribution_matches_java_builtin_partitioner_oracle() {
        let metadata = metadata_with_partitions("orders", 5);
        let topic_metadata = metadata.topic("orders").expect("topic metadata");
        let mut state = ProducerPartitionerState::default();
        state.update_partition_load_stats("orders", &[5, 0, 3, 0, 1], &[0, 1, 2, 3, 4], 5);
        let expected_frequencies = [1_usize, 6, 3, 6, 5];
        let range_end = expected_frequencies.iter().copied().sum::<usize>();
        let mut actual_frequencies = [0_usize; 5];

        for random in 0..range_end {
            let partition = state
                .adaptive_partition_for_random("orders", topic_metadata, random)
                .expect("adaptive partition");
            let index = usize::try_from(partition).expect("non-negative partition");
            actual_frequencies[index] += 1;
        }

        assert_eq!(actual_frequencies, expected_frequencies);
    }

    #[test]
    fn sticky_uniform_fallback_uses_random_available_partition_like_java() {
        let mut metadata = metadata_with_partitions("orders", 4);
        metadata.topics[0].partitions[0].leader_id = -1;
        metadata.topics[0].partitions[1].leader_id = 7;
        metadata.topics[0].partitions[2].leader_id = -1;
        metadata.topics[0].partitions[3].leader_id = 9;
        let topic_metadata = metadata.topic("orders").expect("topic metadata");

        assert_eq!(
            uniform_partition_for_random("orders", topic_metadata, 0).expect("partition"),
            1
        );
        assert_eq!(
            uniform_partition_for_random("orders", topic_metadata, 1).expect("partition"),
            3
        );

        metadata.topics[0]
            .partitions
            .iter_mut()
            .for_each(|partition| partition.leader_id = -1);
        let topic_metadata = metadata.topic("orders").expect("topic metadata");
        assert_eq!(
            uniform_partition_for_random("orders", topic_metadata, 2).expect("partition"),
            2
        );
    }

    #[test]
    fn adaptive_sticky_updates_load_stats_from_accumulator_queues() {
        let metadata = metadata_with_partitions("orders", 3);
        let topic_metadata = metadata.topic("orders").expect("topic metadata");
        let mut accumulator = RecordAccumulator::new(AccumulatorConfig::default().batch_size(1));
        let now = Instant::now();
        accumulator
            .append_at(ProducerRecord::new("orders", 0), now)
            .expect("append partition 0");
        for _ in 0..3 {
            accumulator
                .append_at(ProducerRecord::new("orders", 1), now)
                .expect("append partition 1");
        }
        accumulator
            .append_at(ProducerRecord::new("orders", 2), now)
            .expect("append partition 2");

        let mut state = ProducerPartitionerState::default();
        state.update_partition_load_stats_from_accumulator("orders", topic_metadata, &accumulator);

        let partition = state
            .adaptive_partition_for_random("orders", topic_metadata, 4)
            .expect("adaptive partition");

        assert_eq!(partition, 2);
    }

    #[test]
    fn adaptive_sticky_keeps_drained_empty_partition_queues_like_java() {
        let metadata = metadata_with_partitions("orders", 3);
        let topic_metadata = metadata.topic("orders").expect("topic metadata");
        let mut accumulator = RecordAccumulator::new(
            AccumulatorConfig::default()
                .batch_size(128)
                .linger(Duration::from_mins(1)),
        );
        let now = Instant::now();
        accumulator
            .append_at(
                ProducerRecord::partition_value("orders", 0, vec![0_u8; 256]),
                now,
            )
            .expect("append ready partition 0");
        accumulator
            .append_at(ProducerRecord::new("orders", 1), now)
            .expect("append partition 1");
        accumulator
            .append_at(ProducerRecord::new("orders", 2), now)
            .expect("append partition 2");

        let drained = accumulator.drain_ready(now);
        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0].partition, 0);

        let mut state = ProducerPartitionerState::default();
        state.update_partition_load_stats_from_accumulator("orders", topic_metadata, &accumulator);

        let partition = state
            .adaptive_partition_for_random("orders", topic_metadata, 0)
            .expect("adaptive partition should include the drained empty queue");

        assert_eq!(partition, 0);
    }

    #[test]
    fn adaptive_sticky_excludes_partitions_whose_leader_exceeds_availability_timeout() {
        let mut metadata = metadata_with_partitions("orders", 3);
        metadata.topics[0].partitions[0].leader_id = 7;
        metadata.topics[0].partitions[1].leader_id = 8;
        metadata.topics[0].partitions[2].leader_id = 9;
        let topic_metadata = metadata.topic("orders").expect("topic metadata");
        let mut accumulator = RecordAccumulator::new(AccumulatorConfig::default().batch_size(1024));
        let now = Instant::now();
        for partition in 0..3 {
            accumulator
                .append_at(ProducerRecord::new("orders", partition), now)
                .expect("append partition");
        }

        let mut state = ProducerPartitionerState::default();
        state.update_broker_drain_stats(
            8,
            now.checked_sub(Duration::from_secs(2))
                .expect("test time before now"),
            true,
        );
        state.update_broker_drain_stats(8, now, false);
        state.update_partition_load_stats_from_accumulator_at(PartitionLoadRefresh {
            topic: "orders",
            topic_metadata,
            accumulator: &accumulator,
            now,
            availability_timeout: Duration::from_secs(1),
        });

        let partition = state
            .adaptive_partition_for_random("orders", topic_metadata, 1)
            .expect("adaptive partition");

        assert_eq!(partition, 2);
    }

    #[test]
    fn adaptive_sticky_updates_node_latency_stats_like_java_record_accumulator() {
        let mut metadata = metadata_with_partitions("orders", 3);
        metadata.topics[0].partitions[0].leader_id = 7;
        metadata.topics[0].partitions[1].leader_id = 8;
        metadata.topics[0].partitions[2].leader_id = 9;
        let topic_metadata = metadata.topic("orders").expect("topic metadata");
        let mut accumulator = RecordAccumulator::new(AccumulatorConfig::default().batch_size(1024));
        let now = Instant::now();
        for partition in 0..3 {
            accumulator
                .append_at(ProducerRecord::new("orders", partition), now)
                .expect("append partition");
        }

        let mut state = ProducerPartitionerState::default();
        state.update_broker_latency_stats(8, now, true);
        state.update_broker_latency_stats(
            8,
            now.checked_add(Duration::from_secs(2))
                .expect("test time after now"),
            false,
        );

        let latency = state
            .broker_drain_stats_by_id
            .get(&8)
            .expect("broker stats should exist");
        assert_eq!(latency.drain_at, now);
        assert_eq!(
            latency.ready_at,
            now.checked_add(Duration::from_secs(2))
                .expect("test time after now")
        );

        state.update_partition_load_stats_from_accumulator_at(PartitionLoadRefresh {
            topic: "orders",
            topic_metadata,
            accumulator: &accumulator,
            now: now
                .checked_add(Duration::from_secs(2))
                .expect("test time after now"),
            availability_timeout: Duration::from_secs(1),
        });

        let partition = state
            .adaptive_partition_for_random("orders", topic_metadata, 1)
            .expect("adaptive partition");

        assert_eq!(partition, 2);
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
    async fn begin_transaction_reports_transaction_error_before_missing_identity_like_java() {
        let dispatcher = transactional_dispatcher();
        {
            let mut state = dispatcher.producer_state.lock().await;
            state.fatal_error = Some(ErrorCode::ProducerFenced);
        }

        assert!(matches!(
            dispatcher.begin_transaction(),
            Err(ProducerError::Transaction {
                operation: "transaction_state",
                error: ErrorCode::ProducerFenced
            })
        ));
    }

    #[tokio::test]
    async fn begin_transaction_reports_pending_operation_before_transaction_error_like_java() {
        let dispatcher = transactional_dispatcher();
        {
            let mut state = dispatcher.producer_state.lock().await;
            state.fatal_error = Some(ErrorCode::ProducerFenced);
            state.pending_operation = Some(TransactionOperation::SendOffsetsToTransaction);
        }

        assert!(matches!(
            dispatcher.begin_transaction(),
            Err(ProducerError::InvalidTransactionState(
                "previous transaction operation is pending and must be retried"
            ))
        ));
    }

    #[tokio::test]
    async fn init_transactions_rejects_repeated_initialization_like_java() {
        let dispatcher = transactional_dispatcher();
        {
            let mut state = dispatcher.producer_state.lock().await;
            state.identity = Some(ProducerIdentity {
                producer_id: 11,
                producer_epoch: 2,
            });
            state.coordinator_id = Some(7);
        }

        assert!(matches!(
            dispatcher.init_transactions().await,
            Err(ProducerError::InvalidTransactionState(
                "init_transactions must only run once"
            ))
        ));
    }

    #[tokio::test]
    async fn send_offsets_to_transaction_reports_busy_when_transaction_state_is_locked_like_java() {
        let dispatcher = transactional_dispatcher();
        {
            let mut state = dispatcher.producer_state.lock().await;
            state.identity = Some(ProducerIdentity {
                producer_id: 11,
                producer_epoch: 2,
            });
            state.in_transaction = true;
        }
        let locked_dispatcher = dispatcher.clone();
        let _state_guard = locked_dispatcher.producer_state.lock().await;

        let result = tokio::time::timeout(
            Duration::from_millis(10),
            dispatcher.send_offsets_to_transaction(
                [(
                    crate::producer::TopicPartition::new("orders", 0),
                    crate::producer::OffsetAndMetadata::new(7),
                )],
                ConsumerGroupMetadata::new("group-a"),
            ),
        )
        .await;

        assert!(matches!(
            result,
            Ok(Err(ProducerError::TransactionStateBusy))
        ));
    }

    #[tokio::test]
    async fn transaction_operations_reject_previous_pending_operation_like_java() {
        let dispatcher = transactional_dispatcher();
        {
            let mut state = dispatcher.producer_state.lock().await;
            state.identity = Some(ProducerIdentity {
                producer_id: 11,
                producer_epoch: 2,
            });
            state.in_transaction = true;
            state.transaction_started = true;
            state.pending_operation = Some(TransactionOperation::SendOffsetsToTransaction);
        }

        assert!(matches!(
            dispatcher.begin_transaction(),
            Err(ProducerError::InvalidTransactionState(
                "previous transaction operation is pending and must be retried"
            ))
        ));
        assert!(matches!(
            dispatcher
                .send_offsets_to_transaction(
                    [(
                        crate::producer::TopicPartition::new("orders", 0),
                        crate::producer::OffsetAndMetadata::new(7),
                    )],
                    ConsumerGroupMetadata::new("group-a"),
                )
                .await,
            Err(ProducerError::InvalidTransactionState(
                "previous transaction operation is pending and must be retried"
            ))
        ));
        assert!(matches!(
            dispatcher.end_transaction(false).await,
            Err(ProducerError::InvalidTransactionState(
                "previous transaction operation is pending and must be retried"
            ))
        ));
    }

    #[tokio::test]
    async fn dropped_pending_transaction_operation_clears_state_for_retry_like_java() {
        let dispatcher = transactional_dispatcher();
        let mut state = dispatcher.producer_state.lock().await;
        let TransactionPendingOperationStart::Started(result) = state
            .begin_pending_transaction_operation(TransactionOperation::SendOffsetsToTransaction)
            .expect("operation should start")
        else {
            panic!("operation should not be cached");
        };
        drop(state);
        {
            let state = dispatcher.producer_state.lock().await;
            assert!(state.pending_operation.is_some());
            drop(state);
        }

        drop(PendingTransactionOperationGuard::new(
            Arc::clone(&dispatcher.producer_state),
            TransactionOperation::SendOffsetsToTransaction,
            result,
        ));

        tokio::time::timeout(Duration::from_millis(100), async {
            loop {
                let pending_operation = {
                    let state = dispatcher.producer_state.lock().await;
                    state.pending_operation
                };
                if pending_operation.is_none() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("dropped pending operation should clear promptly");
        let mut state = dispatcher.producer_state.lock().await;
        fail_pending_transaction_operation(&mut state).expect("retry should not be blocked");
    }

    #[tokio::test]
    async fn same_pending_transaction_operation_reuses_cached_result_like_java() {
        let mut state = ProducerIdempotenceState::default();
        let TransactionPendingOperationStart::Started(first_result) = state
            .begin_pending_transaction_operation(TransactionOperation::EndTransaction {
                committed: true,
            })
            .expect("first commit operation should start")
        else {
            panic!("first operation should start");
        };

        let TransactionPendingOperationStart::Cached(second_result) = state
            .begin_pending_transaction_operation(TransactionOperation::EndTransaction {
                committed: true,
            })
            .expect("same commit operation should reuse cached result")
        else {
            panic!("same operation should return cached result");
        };

        assert!(first_result.is_same_handle(&second_result));
        assert!(!second_result.is_completed());
        first_result.done();
        second_result
            .wait()
            .await
            .expect("cached waiter should observe successful completion");
        assert!(second_result.is_successful());
    }

    #[test]
    fn completed_unacked_pending_transaction_result_blocks_next_operation_like_java() {
        let mut state = ProducerIdempotenceState::default();
        let TransactionPendingOperationStart::Started(first_result) = state
            .begin_pending_transaction_operation(TransactionOperation::InitTransactions)
            .expect("first init operation should start")
        else {
            panic!("first operation should start");
        };
        first_result.done();

        assert!(matches!(
            state.begin_pending_transaction_operation(
                TransactionOperation::SendOffsetsToTransaction
            ),
            Err(ProducerError::InvalidTransactionState(
                "previous transaction operation is pending and must be retried"
            ))
        ));
    }

    #[tokio::test]
    async fn acked_pending_transaction_result_allows_next_operation_like_java() {
        let mut state = ProducerIdempotenceState::default();
        let TransactionPendingOperationStart::Started(first_result) = state
            .begin_pending_transaction_operation(TransactionOperation::InitTransactions)
            .expect("first init operation should start")
        else {
            panic!("first operation should start");
        };
        first_result.done();
        first_result
            .wait()
            .await
            .expect("awaiting the cached result should ack it like Java");

        let TransactionPendingOperationStart::Started(second_result) = state
            .begin_pending_transaction_operation(TransactionOperation::SendOffsetsToTransaction)
            .expect("acked pending operation should be cleared for the next operation")
        else {
            panic!("next operation should start instead of using a cached result");
        };

        assert!(!first_result.is_same_handle(&second_result));
        assert_eq!(
            state.pending_operation,
            Some(TransactionOperation::SendOffsetsToTransaction)
        );
        assert_eq!(
            state.pending_requests.pop_next(),
            Some(super::TransactionRequestKind::AddPartitionsOrOffsets)
        );
    }

    #[test]
    fn completed_unacked_pending_transaction_result_blocks_non_cached_operation_like_java() {
        let mut state = ProducerIdempotenceState::default();
        let TransactionPendingOperationStart::Started(first_result) = state
            .begin_pending_transaction_operation(TransactionOperation::InitTransactions)
            .expect("first init operation should start")
        else {
            panic!("first operation should start");
        };
        first_result.done();

        assert!(matches!(
            fail_pending_transaction_operation(&mut state),
            Err(ProducerError::InvalidTransactionState(
                "previous transaction operation is pending and must be retried"
            ))
        ));
    }

    #[tokio::test]
    async fn acked_pending_transaction_result_allows_non_cached_operation_like_java() {
        let mut state = ProducerIdempotenceState::default();
        let TransactionPendingOperationStart::Started(first_result) = state
            .begin_pending_transaction_operation(TransactionOperation::InitTransactions)
            .expect("first init operation should start")
        else {
            panic!("first operation should start");
        };
        first_result.done();
        first_result
            .wait()
            .await
            .expect("awaiting the cached result should ack it like Java");

        fail_pending_transaction_operation(&mut state)
            .expect("acked pending operation should be cleared");

        assert_eq!(state.pending_operation, None);
        assert!(state.pending_result.is_none());
        assert!(state.pending_requests.pop_next().is_none());
    }

    #[test]
    fn different_pending_transaction_operation_still_reports_busy_like_java() {
        let mut state = ProducerIdempotenceState::default();
        let _started = state
            .begin_pending_transaction_operation(TransactionOperation::EndTransaction {
                committed: true,
            })
            .expect("first commit operation should start");

        assert!(matches!(
            state.begin_pending_transaction_operation(TransactionOperation::EndTransaction {
                committed: false,
            }),
            Err(ProducerError::InvalidTransactionState(
                "previous transaction operation is pending and must be retried"
            ))
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
    async fn end_transaction_reports_busy_when_transaction_state_is_locked_like_java() {
        let dispatcher = transactional_dispatcher();
        {
            let mut state = dispatcher.producer_state.lock().await;
            state.identity = Some(ProducerIdentity {
                producer_id: 11,
                producer_epoch: 2,
            });
            state.in_transaction = true;
        }
        let locked_dispatcher = dispatcher.clone();
        let _state_guard = locked_dispatcher.producer_state.lock().await;

        let result =
            tokio::time::timeout(Duration::from_millis(10), dispatcher.end_transaction(false))
                .await;

        assert!(matches!(
            result,
            Ok(Err(ProducerError::TransactionStateBusy))
        ));
    }

    #[tokio::test]
    async fn end_transaction_reports_transaction_error_before_network_io_like_java() {
        let dispatcher = ProducerDispatcher::with_config(
            WireClient::connect_with_brokers(ConnectionConfig::default(), "producer-test", []),
            ProducerRuntimeConfig {
                idempotence: ProducerIdempotenceConfig {
                    enabled: true,
                    transactional_id: Some("txn-a".to_owned()),
                    transaction_timeout_ms: 30_000,
                    transaction_two_phase_commit: false,
                },
                ..ProducerRuntimeConfig::default()
            },
        );
        {
            let mut state = dispatcher.producer_state.lock().await;
            state.fatal_error = Some(ErrorCode::ProducerFenced);
        }

        assert!(matches!(
            dispatcher.end_transaction(false).await,
            Err(ProducerError::Transaction {
                operation: "transaction_state",
                error: ErrorCode::ProducerFenced
            })
        ));
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
    async fn end_transaction_skips_end_txn_for_empty_transaction_like_java() {
        for committed in [true, false] {
            let dispatcher = ProducerDispatcher::with_config(
                WireClient::connect_with_brokers(ConnectionConfig::default(), "producer-test", []),
                ProducerRuntimeConfig {
                    idempotence: ProducerIdempotenceConfig {
                        enabled: true,
                        transactional_id: Some("txn-a".to_owned()),
                        transaction_timeout_ms: 30_000,
                        transaction_two_phase_commit: false,
                    },
                    ..ProducerRuntimeConfig::default()
                },
            );
            {
                let mut state = dispatcher.producer_state.lock().await;
                state.identity = Some(ProducerIdentity {
                    producer_id: 11,
                    producer_epoch: 2,
                });
                state.in_transaction = true;
            }

            dispatcher
                .end_transaction(committed)
                .await
                .expect("empty transaction should complete without EndTxn");

            let state = dispatcher.producer_state.lock().await;
            assert!(!state.in_transaction);
            drop(state);
        }
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
            state.identity = Some(ProducerIdentity {
                producer_id: 11,
                producer_epoch: 2,
            });
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
    async fn add_partition_to_transaction_requires_initialized_identity_like_java() {
        for transaction_two_phase_commit in [false, true] {
            let dispatcher = ProducerDispatcher::with_config(
                test_wire(),
                ProducerRuntimeConfig {
                    idempotence: ProducerIdempotenceConfig {
                        enabled: true,
                        transactional_id: Some("txn-a".to_owned()),
                        transaction_timeout_ms: 30_000,
                        transaction_two_phase_commit,
                    },
                    ..ProducerRuntimeConfig::default()
                },
            );
            let route = route("orders", 0);
            {
                let mut state = dispatcher.producer_state.lock().await;
                state.in_transaction = true;
                if !transaction_two_phase_commit {
                    let _inserted = state.transaction_partitions.insert(TopicPartitionKey {
                        topic: "orders".to_owned(),
                        partition: 0,
                    });
                }
            }

            assert!(matches!(
                dispatcher.add_partition_to_transaction(&route).await,
                Err(ProducerError::InvalidTransactionState(message))
                    if message == "init_transactions must run before produce"
            ));
        }
    }

    #[tokio::test]
    async fn transactional_send_rejects_previous_pending_operation_like_java() {
        let dispatcher = transactional_dispatcher();
        {
            let mut state = dispatcher.producer_state.lock().await;
            state.identity = Some(ProducerIdentity {
                producer_id: 11,
                producer_epoch: 2,
            });
            state.in_transaction = true;
            state.pending_operation = Some(TransactionOperation::SendOffsetsToTransaction);
        }

        let route = route("orders", 0);
        let result = tokio::time::timeout(
            Duration::from_millis(10),
            dispatcher.add_partition_to_transaction(&route),
        )
        .await;

        assert!(matches!(
            result,
            Ok(Err(ProducerError::InvalidTransactionState(
                "previous transaction operation is pending and must be retried"
            )))
        ));
    }

    #[tokio::test]
    async fn producer_batch_state_rejects_record_counts_above_i32() {
        let dispatcher = ProducerDispatcher::with_config(
            test_wire(),
            ProducerRuntimeConfig {
                idempotence: ProducerIdempotenceConfig {
                    enabled: true,
                    transactional_id: None,
                    transaction_timeout_ms: 30_000,
                    transaction_two_phase_commit: false,
                },
                ..ProducerRuntimeConfig::default()
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
    fn idempotence_state_blocks_new_sequences_for_unresolved_partition_like_java() {
        let mut state = ProducerIdempotenceState::default();
        assert_eq!(
            state
                .next_sequence("orders", 0, 3)
                .expect("initial sequence"),
            0
        );

        state.mark_sequence_unresolved("orders", 0, 0, 3);

        assert!(matches!(
            state.next_sequence("orders", 0, 1),
            Err(ProducerError::UnresolvedSequence { topic, partition })
                if topic == "orders" && partition == 0
        ));
        assert_eq!(
            state
                .next_sequence("orders", 1, 1)
                .expect("other partition remains usable"),
            0
        );

        state.reset_sequence("orders", 0);
        assert_eq!(
            state
                .next_sequence("orders", 0, 1)
                .expect("reset clears unresolved sequence"),
            0
        );
    }

    #[test]
    fn idempotence_state_release_sequence_does_not_reuse_acked_sequence_like_java() {
        let mut state = ProducerIdempotenceState::default();
        assert_eq!(
            state
                .next_sequence("orders", 0, 3)
                .expect("initial sequence"),
            0
        );

        state.release_sequence("orders", 0, 0);

        assert_eq!(
            state
                .next_sequence("orders", 0, 1)
                .expect("next sequence after ack"),
            3
        );
    }

    #[test]
    fn idempotence_state_out_of_order_retry_waits_when_unresolved_gap_is_not_next_like_java() {
        let mut state = ProducerIdempotenceState::default();
        state.mark_sequence_unresolved("orders", 0, 0, 1);

        assert!(
            !state.should_reset_sequence_for_idempotent_retry(IdempotentRetryDecision {
                topic: "orders",
                partition: 0,
                error: ErrorCode::OutOfOrderSequenceNumber,
                log_start_offset: -1,
                base_sequence: Some(2),
            },)
        );
        assert!(
            state.should_reset_sequence_for_idempotent_retry(IdempotentRetryDecision {
                topic: "orders",
                partition: 0,
                error: ErrorCode::OutOfOrderSequenceNumber,
                log_start_offset: -1,
                base_sequence: Some(1),
            },)
        );
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
            3,
        );
        request.push(
            route("orders", 1),
            Bytes::from_static(b"records-1"),
            BrokerProduceOptions {
                acks: -1,
                timeout_ms: 1_500,
                transactional_id: Some("txn-a"),
            },
            5,
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
        request.push(route.clone(), Bytes::from_static(b"a"), options, 2);
        request.push(route.clone(), Bytes::from_static(b"b"), options, 1);

        let topic = request.data.topic_data.first().expect("topic group");
        assert_eq!(topic.partition_data.len(), 1);
        let partition = topic.partition_data.first().expect("partition data");
        assert_eq!(partition.records.as_ref().expect("records").as_ref(), b"ab");
        assert_eq!(request.routes.len(), 2);
        assert_eq!(request.routes[0].request_offset_delta, 0);
        assert_eq!(request.routes[0].record_count, 2);
        assert_eq!(request.routes[1].request_offset_delta, 2);
        assert_eq!(request.routes[1].record_count, 1);
        assert!(request.contains_route(&route));
    }

    #[test]
    fn broker_request_index_starts_fresh_request_for_same_partition_batch() {
        let options = BrokerProduceOptions {
            acks: -1,
            timeout_ms: 1_500,
            transactional_id: None,
        };
        let version = client_api_info(ApiKey::Produce).max_version;
        let route = route("orders", 0);
        let first_records = Bytes::from(vec![b'a'; 128]);
        let second_records = Bytes::from(vec![b'b'; 128]);
        let mut request = BrokerProduceRequest::default();
        request.push(route.clone(), first_records, options, 1);
        let combined_len = request
            .encoded_len_after_push(&route, &second_records, options, version)
            .expect("combined request encoded len");

        let placement = broker_request_placement_for_batch(
            &[request],
            &route,
            &second_records,
            options,
            ProduceRequestSizing {
                version,
                max_request_size: combined_len,
            },
        )
        .expect("second same-partition batch fits in fresh request");

        assert_eq!(
            placement,
            BrokerRequestPlacement {
                index: 1,
                split: false
            }
        );
    }

    #[test]
    fn broker_request_index_splits_when_next_partition_would_exceed_max_request_size() {
        let options = BrokerProduceOptions {
            acks: -1,
            timeout_ms: 1_500,
            transactional_id: None,
        };
        let version = client_api_info(ApiKey::Produce).max_version;
        let first_route = route("orders", 0);
        let second_route = route("orders", 1);
        let first_records = Bytes::from(vec![b'a'; 128]);
        let second_records = Bytes::from(vec![b'b'; 128]);
        let mut packed_request = BrokerProduceRequest::default();
        packed_request.push(first_route.clone(), first_records.clone(), options, 1);
        let packed_len = packed_request
            .encoded_len_after_push(&second_route, &second_records, options, version)
            .expect("packed request encoded len");
        let sizing = ProduceRequestSizing {
            version,
            max_request_size: packed_len.saturating_sub(1),
        };

        let mut requests = Vec::new();
        let first_placement = broker_request_placement_for_batch(
            &requests,
            &first_route,
            &first_records,
            options,
            sizing,
        )
        .expect("first request fits");
        assert_eq!(
            first_placement,
            BrokerRequestPlacement {
                index: 0,
                split: false
            }
        );
        requests.push(BrokerProduceRequest::default());
        requests[first_placement.index].push(first_route, first_records, options, 1);

        let second_placement = broker_request_placement_for_batch(
            &requests,
            &second_route,
            &second_records,
            options,
            sizing,
        )
        .expect("second request fits in a fresh request");

        assert_eq!(
            second_placement,
            BrokerRequestPlacement {
                index: 1,
                split: true
            }
        );
    }

    #[test]
    fn broker_request_index_reports_record_too_large_for_single_oversize_request() {
        let options = BrokerProduceOptions {
            acks: -1,
            timeout_ms: 1_500,
            transactional_id: None,
        };
        let version = client_api_info(ApiKey::Produce).max_version;
        let route = route("orders", 0);
        let records = Bytes::from(vec![b'a'; 128]);
        let empty_request = BrokerProduceRequest::default();
        let encoded_len = empty_request
            .encoded_len_after_push(&route, &records, options, version)
            .expect("single request encoded len");

        let error = broker_request_placement_for_batch(
            &[],
            &route,
            &records,
            options,
            ProduceRequestSizing {
                version,
                max_request_size: encoded_len.saturating_sub(1),
            },
        )
        .expect_err("single oversize request should fail");

        assert!(matches!(
            error,
            DispatchError::Producer(ProducerError::RecordTooLarge { .. })
        ));
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

    #[test]
    fn no_ack_receipts_use_synthetic_offsets() {
        let receipts = no_ack_receipts(&[route("orders", 2)]);

        assert_eq!(
            receipts,
            [RecordMetadata {
                topic: Arc::from("orders"),
                partition: 2,
                leader_id: 7,
                offset: -1,
                timestamp_ms: -1,
                serialized_key_size: -1,
                serialized_value_size: -1,
            }]
        );
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
        let receipt = RecordMetadata {
            topic: Arc::from("orders"),
            partition: 0,
            leader_id: 7,
            offset: 9,
            timestamp_ms: -1,
            serialized_key_size: -1,
            serialized_value_size: 1,
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
    fn transaction_v1_request_versions_match_java_caps() {
        assert_eq!(produce_version(false), 11);
        assert_eq!(txn_offset_commit_version(false), 4);
        assert_eq!(end_txn_version(false), 4);

        assert_eq!(
            produce_version(true),
            client_api_info(ApiKey::Produce).max_version
        );
        assert_eq!(
            txn_offset_commit_version(true),
            client_api_info(ApiKey::TxnOffsetCommit).max_version
        );
        assert_eq!(
            end_txn_version(true),
            client_api_info(ApiKey::EndTxn).max_version
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
