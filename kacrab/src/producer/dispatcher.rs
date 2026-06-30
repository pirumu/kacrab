//! Producer dispatcher that routes ready batches through the wire client.

use std::{
    collections::{BTreeSet, VecDeque},
    net::SocketAddr,
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use ahash::{AHashMap, AHashSet};
use bytes::BytesMut;
use kacrab_protocol::{
    KafkaString, KafkaUuid,
    compression::Compression,
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

#[cfg(test)]
use super::batch::encode_record_batch_with_producer_state_at_offset_into;
use super::{
    accumulator::{
        RECORD_BATCH_OVERHEAD_BYTES, ReadyBatch, SharedAccumulator, estimate_record_batch_bytes,
    },
    api::{ConsumerGroupMetadata, OffsetAndMetadata, TopicPartition},
    batch::{
        encode_record_batch_with_producer_state_at_offset,
        encode_record_batch_with_producer_state_at_offset_into_buffer,
    },
    compression_ratio::CompressionRatioEstimator,
    config::{
        ACKS_NONE, DEFAULT_DELIVERY_TIMEOUT, DEFAULT_RETRY_BACKOFF, DEFAULT_RETRY_BACKOFF_MAX,
        DEFAULT_TIMEOUT_MS, DEFAULT_TRANSACTION_TIMEOUT_MS, ProducerCompression,
        ProducerIdempotenceConfig, ProducerRuntimeConfig,
    },
    error::{ProducerError, Result},
    metrics::{ProducerMetrics, ProducerMetricsSnapshot, ProducerQueueMetrics},
    record::RecordMetadata,
    response::{
        PartitionLeaderUpdate, ProduceBrokerError, ProduceReceiptError, current_leader_updates,
        node_endpoint_updates, produce_receipts_with_error_details,
    },
    routing::{ProduceRoute, murmur2_java, route},
    transaction::{ProducerBatchState, ProducerIdentity, TransactionState},
};
use crate::wire::{
    BackoffPolicy, BackoffState, BrokerEndpoint, PartitionLeaderChange, RequestMessage,
    TopicMetadata, WireClient,
};

mod idempotence;
mod partitioner;
mod sequencer;
mod transactions;

pub(crate) use idempotence::*;
pub(crate) use partitioner::*;
pub(crate) use sequencer::*;
pub(crate) use transactions::*;

use super::{ProducerRecord, record};

/// Dispatcher-only fallback for tests/manual construction. Public producer
/// configs still default to `acks=all`; this keeps `ProducerDispatcher::new`
/// compatible with earlier unit tests that modeled leader-only acknowledgements.
const DEFAULT_DISPATCHER_ACKS: i16 = 1;
const PENDING_TRANSACTION_OPERATION_MESSAGE: &str =
    "previous transaction operation is pending and must be retried";
const COMPRESSION_RATE_ESTIMATION_FACTOR: f32 = 1.05;

/// Dispatches ready accumulator batches to broker leaders through [`WireClient`].
#[derive(Debug, Clone)]
pub struct ProducerDispatcher {
    wire: WireClient,
    acks: i16,
    timeout_ms: i32,
    retry_attempts: usize,
    retry_backoff: Duration,
    retry_backoff_max: Duration,
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
    enqueue_sequencer: Arc<EnqueueSequencer>,
    partitioner_state: Arc<Mutex<ProducerPartitionerState>>,
    compression_ratios: Arc<StdMutex<CompressionRatioEstimator>>,
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
            retry_backoff: DEFAULT_RETRY_BACKOFF,
            retry_backoff_max: DEFAULT_RETRY_BACKOFF_MAX,
            delivery_timeout: DEFAULT_DELIVERY_TIMEOUT,
            compression: ProducerCompression {
                codec: Compression::None,
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
            enqueue_sequencer: Arc::new(EnqueueSequencer::new()),
            partitioner_state: Arc::new(Mutex::new(ProducerPartitionerState::default())),
            compression_ratios: Arc::new(StdMutex::new(CompressionRatioEstimator::default())),
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
            retry_backoff: config.retry_backoff,
            retry_backoff_max: config.retry_backoff_max,
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
            enqueue_sequencer: Arc::new(EnqueueSequencer::new()),
            partitioner_state: Arc::new(Mutex::new(ProducerPartitionerState::default())),
            compression_ratios: Arc::new(StdMutex::new(CompressionRatioEstimator::default())),
            metrics: ProducerMetrics::default(),
            metrics_enabled: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Point-in-time producer dispatch metrics snapshot.
    #[must_use]
    pub fn metrics(&self) -> ProducerMetricsSnapshot {
        self.metrics.snapshot(ProducerQueueMetrics::default())
    }

    pub(crate) fn metrics_handle(&self) -> ProducerMetrics {
        self.metrics.clone()
    }

    #[cfg(test)]
    fn set_compression_ratio_estimation_for_test(
        &self,
        topic: &str,
        codec: Compression,
        ratio: f32,
    ) {
        self.compression_ratios
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .set_estimation(topic, codec, ratio);
    }

    #[cfg(test)]
    fn compression_ratio_estimation_for_test(&self, topic: &str, codec: Compression) -> f32 {
        self.compression_ratios
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .estimation(topic, codec)
    }

    /// Whether this producer was configured with a `transactional.id`. Cheap,
    /// lock-free check used to skip the async transaction-error guard entirely
    /// on the per-record send hot path for non-transactional producers.
    pub(crate) const fn is_transactional(&self) -> bool {
        self.idempotence.transactional_id.is_some()
    }

    pub(crate) async fn fail_if_transaction_error(&self) -> Result<()> {
        if self.idempotence.transactional_id.is_none() {
            return Ok(());
        }
        let state = self.producer_state.lock().await;
        fail_transaction_state_if_needed(&state, false)
    }

    /// Non-blocking transaction-error guard for the synchronous send path. Reads
    /// the producer state via `try_lock` so a transactional send can stay on the
    /// fast synchronous append path (Kafka throws fatal transaction errors
    /// synchronously). Returns `None` on momentary lock contention, in which case
    /// the caller takes the slow drain (which performs the awaiting guard).
    pub(crate) fn try_fail_if_transaction_error_now(&self) -> Option<Result<()>> {
        if self.idempotence.transactional_id.is_none() {
            return Some(Ok(()));
        }
        let state = self.producer_state.try_lock().ok()?;
        Some(fail_transaction_state_if_needed(&state, false))
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

    /// Age of the currently cached cluster metadata (Kafka `metadata-age`).
    pub(crate) fn metadata_age(&self) -> Option<Duration> {
        self.wire.metadata_age()
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

    /// Assign idempotent sequences to freshly drained batches. Returns the indices
    /// of batches that must be DEFERRED (re-enqueued, not dispatched) this cycle —
    /// Kafka `shouldStopDrainBatchesForPartition`'s unresolved-sequence clause: a
    /// partition with an unresolved sequence and still-in-flight batches stops
    /// draining new batches until it resolves. The returned indices are positions
    /// into `batches`; the caller removes them (and their parallel partition keys).
    /// On error `batches` is left untouched so the caller can re-enqueue them all.
    pub(crate) async fn prepare_drained_batches(
        &self,
        batches: &mut [ReadyBatch],
    ) -> Result<Vec<usize>> {
        if !self.idempotence.enabled && self.idempotence.transactional_id.is_none() {
            return Ok(Vec::new());
        }
        let topics = unique_topics(batches);
        let metadata = self
            .wire
            .metadata_for_topics(topics.iter().map(String::as_str))
            .await?;
        let mut deferred = Vec::new();
        for (index, batch) in batches.iter_mut().enumerate() {
            let Some(first_record) = batch.records.first().cloned() else {
                continue;
            };
            let route = self
                .route_for_batch(&metadata, batch, &first_record)
                .await?;
            self.add_partition_to_transaction(&route).await?;
            // A batch whose stamped epoch was bumped since (stale) must be re-stamped
            // under the new epoch with a fresh sequence (Kafka startSequencesAtBeginning):
            // clear it so it preps as fresh below.
            if let Some(producer_state) = batch.producer_state
                && self
                    .producer_state
                    .lock()
                    .await
                    .is_stale_identity(producer_state.identity)
            {
                batch.producer_state = None;
            }
            match batch.producer_state {
                None => match self
                    .producer_batch_state(&route, batch.records.len())
                    .await?
                {
                    ProducerBatchPrep::Ready(state) => batch.producer_state = state,
                    ProducerBatchPrep::DeferUnresolved => deferred.push(index),
                },
                // Retried batch (kept its assigned sequence): Kafka
                // `shouldStopDrainBatchesForPartition` retry clause — only dispatch when
                // its base sequence is the partition's first in-flight sequence; else an
                // earlier-sequence batch is still unacked, so defer to keep retries in
                // strict sequence order (reducing to one in-flight while retrying).
                Some(producer_state) => {
                    let first_inflight = {
                        let state = self.producer_state.lock().await;
                        state.first_inflight_sequence(&route.topic, route.partition)
                    };
                    if first_inflight.is_some_and(|first| producer_state.base_sequence != first) {
                        deferred.push(index);
                    }
                },
            }
        }
        Ok(deferred)
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
    pub async fn assign_partition(&self, record: &mut ProducerRecord) -> Result<()> {
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
                self.compression_ratio_estimation(record.topic.as_ref()),
            )?
        };
        record.partition = partition;
        Ok(())
    }

    /// Assign a concrete partition while allowing the default sticky partitioner
    /// to reuse its current sticky partition without a metadata lookup.
    pub(crate) async fn assign_partition_with_accumulator(
        &self,
        accumulator: &SharedAccumulator,
        record: &mut ProducerRecord,
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
                self.compression_ratio_estimation(record.topic.as_ref()),
            )?
        };
        record.partition = partition;
        Ok(())
    }

    /// Assign a concrete partition using an already-fetched metadata snapshot.
    pub(crate) async fn assign_partition_with_metadata(
        &self,
        metadata: &crate::wire::ClusterMetadata,
        record: &mut ProducerRecord,
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
                self.compression_ratio_estimation(record.topic.as_ref()),
            )?
        };
        record.partition = partition;
        Ok(())
    }

    pub(crate) const fn uses_sticky_partitioner(&self, record: &ProducerRecord) -> bool {
        !record.has_assigned_partition() && (self.partitioner_ignore_keys || record.key.is_none())
    }

    async fn try_assign_cached_sticky_partition(&self, record: &mut ProducerRecord) -> bool {
        if !self.uses_sticky_partitioner(record) {
            return false;
        }
        let mut state = self.partitioner_state.lock().await;
        state.try_assign_cached_sticky_partition(
            record,
            self.partition_sticky_batch_size,
            self.compression_ratio_estimation(record.topic.as_ref()),
        )
    }

    /// Synchronous (non-blocking) sticky-partition assignment for `send_now`: reads
    /// the cached sticky partition via `try_lock` (no `.await`, no OS-thread block).
    /// Returns `false` when the record isn't sticky-eligible, the partitioner lock is
    /// momentarily contended, or the sticky batch budget is exhausted (rotation) — the
    /// caller then takes the async assignment path for that one record.
    pub(crate) fn try_assign_cached_sticky_partition_now(
        &self,
        record: &mut ProducerRecord,
    ) -> bool {
        if record.has_assigned_partition() {
            return true;
        }
        let Ok(mut state) = self.partitioner_state.try_lock() else {
            return false;
        };
        let compression_ratio = self.compression_ratio_estimation(record.topic.as_ref());
        // Fast sticky reuse: no metadata lookup needed while the current sticky
        // batch still has budget (the common steady-state case).
        if self.uses_sticky_partitioner(record)
            && state.try_assign_cached_sticky_partition(
                record,
                self.partition_sticky_batch_size,
                compression_ratio,
            )
        {
            return true;
        }
        // Full assignment (sticky rotation OR keyed) via synchronously cached
        // cluster metadata — `cached_metadata_for` reads an `RwLock` with no
        // `.await`, so rotation stays on the lock-free sync send path instead of
        // falling back to the async assignment path every sticky-batch boundary.
        let topic = record.topic.to_string();
        let Some(metadata) = self.wire.cached_metadata_for(std::slice::from_ref(&topic)) else {
            return false;
        };
        match state.partition_for_record(
            &metadata,
            record,
            self.partitioner_ignore_keys,
            self.partitioner_adaptive_partitioning_enable,
            self.partition_sticky_batch_size,
            compression_ratio,
        ) {
            Ok(partition) => {
                record.partition = partition;
                true
            },
            Err(_) => false,
        }
    }

    pub(crate) fn compression_ratio_estimation(&self, topic: &str) -> f32 {
        // No compression -> ratio is always 1.0; skip the shared estimator lock so
        // the per-record partition-assign path doesn't serialize on it.
        if matches!(self.compression.codec, Compression::None) {
            return 1.0;
        }
        self.compression_ratios.lock().map_or(1.0, |ratios| {
            ratios.estimation(topic, self.compression.codec)
        })
    }

    pub(crate) async fn mark_sticky_batch_ready(&self, topic: &str) {
        let mut state = self.partitioner_state.lock().await;
        state.mark_sticky_batch_ready(topic, self.partition_sticky_batch_size);
    }

    /// Non-blocking sticky-batch-ready mark for the synchronous send path: flips
    /// `switch_on_next` via `try_lock` so the next record rotates the sticky
    /// partition. Returns `false` on momentary lock contention, in which case the
    /// sticky budget check (`>= 2x batch size`) rotates a little later instead —
    /// the record that triggered this has already been appended either way.
    pub(crate) fn try_mark_sticky_batch_ready_now(&self, topic: &str) -> bool {
        let Ok(mut state) = self.partitioner_state.try_lock() else {
            return false;
        };
        state.mark_sticky_batch_ready(topic, self.partition_sticky_batch_size);
        true
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
    pub async fn assign_partitions(&self, records: &mut [ProducerRecord]) -> Result<()> {
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
                    compression_ratio: self.compression_ratio_estimation(&topic),
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
        records: &mut [ProducerRecord],
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
                    compression_ratio: self.compression_ratio_estimation(&topic),
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
        records: &mut [ProducerRecord],
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
                compression_ratio: self.compression_ratio_estimation(topic),
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
        records: &mut [ProducerRecord],
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
                compression_ratio: self.compression_ratio_estimation(topic),
            },
            records,
        )?;
        drop(state);
        Ok(())
    }

    /// Refresh adaptive sticky partition load stats from the current accumulator queues.
    pub async fn refresh_partition_load_stats<I, S>(
        &self,
        accumulator: &SharedAccumulator,
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
        accumulator: &SharedAccumulator,
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
        accumulator: &SharedAccumulator,
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
                  classification stay together to mirror Kafka's TransactionManager ordering."
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
            let mut retry_backoff = self.retry_backoff_state();
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
                    self.sleep_retry_backoff(&mut retry_backoff).await?;
                    continue;
                }
                if attempts_remaining > 0 && error.is_retriable() {
                    attempts_remaining = attempts_remaining.saturating_sub(1);
                    self.sleep_retry_backoff(&mut retry_backoff).await?;
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
        accumulator: &SharedAccumulator,
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
        let mut retry_backoff = self.retry_backoff_state();
        let enqueue_ticket = self.enqueue_sequencer.reserve_ticket();
        loop {
            match self.dispatch_batches(&mut batches, enqueue_ticket).await {
                Ok(receipts) => return Ok(receipts),
                Err(DispatchError::Requeue) => {
                    if self.metrics_are_enabled() {
                        self.metrics.record_requeue();
                    }
                    accumulator.requeue_front(batches)?;
                    return Ok(Vec::new());
                },
                Err(DispatchError::SplitAndRequeue { topic, partition }) => {
                    let compression_ratio = self.reset_compression_ratio_after_message_too_large(
                        &batches, &topic, partition,
                    );
                    let Some(split) = split_message_too_large_batches(
                        batches,
                        &topic,
                        partition,
                        self.partition_sticky_batch_size,
                        compression_ratio,
                    ) else {
                        if self.metrics_are_enabled() {
                            self.metrics.record_error();
                        }
                        return Err(ProducerError::Broker {
                            topic,
                            partition,
                            error: ErrorCode::MessageTooLarge,
                        });
                    };
                    if self.metrics_are_enabled() {
                        self.metrics.record_requeue();
                    }
                    accumulator.requeue_front(split)?;
                    return Ok(Vec::new());
                },
                Err(DispatchError::RetryableLeadership {
                    topic,
                    partition,
                    error,
                    metadata_updated,
                }) => {
                    if !metadata_updated {
                        self.wire.invalidate_topic_partition(&topic, partition);
                    }
                    if attempts_remaining == 0 {
                        if self.metrics_are_enabled() {
                            self.metrics.record_error_for_topic(Some(&topic));
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
                        self.metrics.record_retry_for_topic(Some(&topic));
                    }
                    // Leader changed for the ongoing retry -> retry immediately
                    // (Kafka skips backoff); otherwise back off normally.
                    let retry_wait = if metadata_updated {
                        self.check_delivery_timeout_before_retry(&batches, false)
                            .await
                    } else {
                        self.wait_before_retry(&batches, &mut retry_backoff, false)
                            .await
                    };
                    if let Some(error) = retry_wait {
                        if self.metrics_are_enabled() {
                            self.metrics.record_error_for_topic(Some(&topic));
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
                            self.metrics.record_error_for_topic(Some(&retry.topic));
                        }
                        return Err(retry.broker_error());
                    }
                    attempts_remaining = attempts_remaining.saturating_sub(1);
                    if self.metrics_are_enabled() {
                        self.metrics.record_retry_for_topic(Some(&retry.topic));
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
                    if let Some(error) = self
                        .wait_before_retry(&batches, &mut retry_backoff, false)
                        .await
                    {
                        if self.metrics_are_enabled() {
                            self.metrics.record_error_for_topic(Some(&retry.topic));
                        }
                        self.recover_idempotent_partition_after_retry_timeout(&mut batches, &retry)
                            .await?;
                        return Err(error);
                    }
                },
                Err(DispatchError::RetryableBroker {
                    topic,
                    partition,
                    error,
                }) => {
                    if let Some(error) = self
                        .handle_retryable_broker_retry(
                            &batches,
                            &mut attempts_remaining,
                            &mut retry_backoff,
                            &topic,
                            partition,
                            error,
                        )
                        .await
                    {
                        return Err(error);
                    }
                },
                Err(DispatchError::RetryableWire(error)) => {
                    if let Some(error) = self
                        .handle_retryable_wire_retry(
                            &batches,
                            &mut attempts_remaining,
                            &mut retry_backoff,
                            error,
                        )
                        .await
                    {
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
        let enqueue_ticket = self.enqueue_sequencer.reserve_ticket();
        match self.dispatch_drained(batches, now, enqueue_ticket).await {
            DispatchOutcome::Delivered(result) => result,
            DispatchOutcome::Requeue(_batches) => Err(ProducerError::FlushIncomplete),
        }
    }

    /// Drain all accumulator batches and send them regardless of linger or size.
    pub async fn dispatch_all(
        &self,
        accumulator: &SharedAccumulator,
    ) -> Result<Vec<RecordMetadata>> {
        let batches = accumulator.drain_all();
        if batches.is_empty() {
            return Ok(Vec::new());
        }
        let enqueue_ticket = self.enqueue_sequencer.reserve_ticket();
        match self
            .dispatch_drained(batches, Instant::now(), enqueue_ticket)
            .await
        {
            DispatchOutcome::Delivered(result) => result,
            DispatchOutcome::Requeue(batches) => {
                accumulator.requeue_front(batches)?;
                Err(ProducerError::FlushIncomplete)
            },
        }
    }

    /// Reserve a spawn-order enqueue ticket. The sender calls this in its single-threaded
    /// loop before spawning a dispatch task so concurrent same-partition requests enqueue in
    /// ascending base-sequence order (see [`EnqueueSequencer`]).
    pub(crate) fn next_enqueue_ticket(&self) -> u64 {
        self.enqueue_sequencer.reserve_ticket()
    }

    /// Register a dispatch's idempotent batches as in flight (Kafka
    /// `addInFlightBatch`, done at drain). Each batch already carries its assigned
    /// `producer_state` from `prepare_drained_batches`; retried batches re-register
    /// the sequence they kept (a no-op on the set).
    async fn register_idempotent_inflight(&self, inflight: &[(String, i32, i32)]) {
        if !self.idempotence.enabled || inflight.is_empty() {
            return;
        }
        let mut state = self.producer_state.lock().await;
        for (topic, partition, base_sequence) in inflight {
            state.register_inflight_sequence(topic, *partition, *base_sequence);
        }
    }

    /// Terminal completion of a dispatch (anything but a requeue): drop its batches
    /// from the in-flight set (Kafka `removeInFlightBatch`) and re-attempt
    /// `maybeResolveSequences` for each touched partition, which now succeeds for
    /// any partition that has fully drained.
    async fn release_idempotent_inflight_after_terminal(&self, inflight: &[(String, i32, i32)]) {
        if !self.idempotence.enabled || inflight.is_empty() {
            return;
        }
        let transactional = self.idempotence.transactional_id.is_some();
        let mut state = self.producer_state.lock().await;
        for (topic, partition, base_sequence) in inflight {
            state.remove_inflight_sequence(topic, *partition, *base_sequence);
        }
        let mut resolved = AHashSet::new();
        for (topic, partition, _) in inflight {
            if !resolved.insert((topic.as_str(), *partition)) {
                continue;
            }
            let ambiguous = state.unresolved_loss_ambiguous(topic, *partition);
            state.resolve_unresolved_sequence_after_drain(
                topic,
                *partition,
                transactional,
                ambiguous,
            );
        }
    }

    pub(crate) async fn dispatch_drained(
        &self,
        batches: Vec<ReadyBatch>,
        now: Instant,
        enqueue_ticket: u64,
    ) -> DispatchOutcome {
        // Capture each idempotent batch's (topic, partition, base_sequence) before the
        // batches move into the dispatch, then register them as in flight (Kafka
        // addInFlightBatch). On a terminal outcome they are removed and the partition's
        // unresolved sequence is re-resolved; on a requeue they stay tracked so a
        // retried batch keeps gating `first_inflight_sequence`/`maybeResolveSequences`.
        let inflight = idempotent_inflight_of(&batches);
        self.register_idempotent_inflight(&inflight).await;
        let outcome = self
            .dispatch_drained_inner(batches, now, enqueue_ticket)
            .await;
        // Guarantee the enqueue turn is always advanced, even when the dispatch returned
        // before reaching `dispatch_broker_requests` (e.g. a local RecordTooLarge or an
        // expired batch never enqueued). Wait for this ticket's turn first so earlier tickets
        // are not skipped, then advance — idempotent if the enqueue path already advanced.
        self.enqueue_sequencer.wait_turn(enqueue_ticket).await;
        self.enqueue_sequencer.advance_past(enqueue_ticket);
        if !matches!(outcome, DispatchOutcome::Requeue(_)) {
            self.release_idempotent_inflight_after_terminal(&inflight)
                .await;
        }
        outcome
    }

    #[expect(
        clippy::too_many_lines,
        reason = "Single drain loop handles every dispatch outcome (split, leadership, \
                  idempotent, retriable broker, wire) in one place to mirror Kafka's \
                  Sender.runOnce."
    )]
    async fn dispatch_drained_inner(
        &self,
        mut batches: Vec<ReadyBatch>,
        now: Instant,
        enqueue_ticket: u64,
    ) -> DispatchOutcome {
        if let Some(batch) = self.expired_batch(&batches, now) {
            let error = ProducerError::DeliveryTimeout {
                topic: batch.topic.clone(),
                partition: batch.partition,
            };
            fail_deliveries(&mut batches, &error);
            return DispatchOutcome::Delivered(Err(error));
        }

        let mut attempts_remaining = self.retry_attempts;
        let mut retry_backoff = self.retry_backoff_state();
        loop {
            match self.dispatch_batches(&mut batches, enqueue_ticket).await {
                Ok(receipts) => {
                    return self
                        .deliver_successful_batches(&mut batches, receipts)
                        .await;
                },
                Err(DispatchError::Requeue) => return DispatchOutcome::Requeue(batches),
                Err(DispatchError::SplitAndRequeue { topic, partition }) => {
                    return self
                        .message_too_large_split_outcome(batches, topic, partition)
                        .await;
                },
                Err(DispatchError::RetryableLeadership {
                    topic,
                    partition,
                    error,
                    metadata_updated,
                }) => {
                    let retry = LeadershipRetry {
                        topic,
                        partition,
                        error,
                        metadata_updated,
                    };
                    if let Some(outcome) = self
                        .handle_drained_leadership_retry(
                            &batches,
                            &mut attempts_remaining,
                            &mut retry_backoff,
                            &retry,
                        )
                        .await
                    {
                        return terminal_error_delivered_to_futures(&mut batches, outcome);
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
                            &mut retry_backoff,
                            &retry,
                        )
                        .await
                    {
                        return terminal_error_delivered_to_futures(&mut batches, outcome);
                    }
                },
                Err(DispatchError::RetryableBroker {
                    topic,
                    partition,
                    error,
                }) => {
                    if let Some(error) = self
                        .handle_retryable_broker_retry(
                            &batches,
                            &mut attempts_remaining,
                            &mut retry_backoff,
                            &topic,
                            partition,
                            error,
                        )
                        .await
                    {
                        fail_deliveries(&mut batches, &error);
                        return DispatchOutcome::Delivered(Err(error));
                    }
                },
                Err(DispatchError::RetryableWire(error)) => {
                    if let Some(error) = self
                        .handle_retryable_wire_retry(
                            &batches,
                            &mut attempts_remaining,
                            &mut retry_backoff,
                            error,
                        )
                        .await
                    {
                        fail_deliveries(&mut batches, &error);
                        return DispatchOutcome::Delivered(Err(error));
                    }
                },
                Err(DispatchError::Producer(error)) => {
                    if self.metrics_are_enabled() {
                        self.metrics.record_error();
                    }
                    fail_deliveries(&mut batches, &error);
                    return DispatchOutcome::Delivered(Err(error));
                },
            }
        }
    }

    async fn message_too_large_split_outcome(
        &self,
        batches: Vec<ReadyBatch>,
        topic: String,
        partition: i32,
    ) -> DispatchOutcome {
        let compression_ratio =
            self.reset_compression_ratio_after_message_too_large(&batches, &topic, partition);
        let Some(split) = split_message_too_large_batches(
            batches,
            &topic,
            partition,
            self.partition_sticky_batch_size,
            compression_ratio,
        ) else {
            // A single-record batch that still exceeds max.request.size cannot be split
            // and fails terminally, leaving a hole in the partition's sequence. Kafka
            // failBatch(adjustSequenceNumbers=true) -> requestIdempotentEpochBumpForPartition
            // requests an epoch bump so the next produce restarts the sequence and heals
            // the gap rather than wedging later batches on OUT_OF_ORDER.
            if self.idempotence.enabled && self.idempotence.transactional_id.is_none() {
                self.producer_state.lock().await.request_epoch_bump();
            }
            return DispatchOutcome::Delivered(Err(ProducerError::Broker {
                topic,
                partition,
                error: ErrorCode::MessageTooLarge,
            }));
        };
        if self.metrics_are_enabled() {
            self.metrics.record_requeue();
        }
        DispatchOutcome::Requeue(split)
    }

    fn reset_compression_ratio_after_message_too_large(
        &self,
        batches: &[ReadyBatch],
        topic: &str,
        partition: i32,
    ) -> f32 {
        let observed_ratio = batches
            .iter()
            .find(|batch| batch.topic == topic && batch.partition == partition)
            .map_or(1.0, |batch| self.observed_compression_ratio(batch));
        let mut compression_ratios = self
            .compression_ratios
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        compression_ratios.reset_after_split(topic, self.compression.codec, observed_ratio);
        compression_ratios.estimation(topic, self.compression.codec)
    }

    #[expect(
        clippy::cast_precision_loss,
        reason = "Kafka compression ratio estimator stores f32 ratios; lengths are only used as a \
                  coarse feedback signal."
    )]
    fn observed_compression_ratio(&self, batch: &ReadyBatch) -> f32 {
        if self.compression.codec == Compression::None {
            return 1.0;
        }
        let Ok(compressed) = encode_record_batch_with_producer_state_at_offset(
            &batch.records,
            self.compression,
            batch.producer_state,
            0,
        ) else {
            return 1.0;
        };
        let Ok(uncompressed) = encode_record_batch_with_producer_state_at_offset(
            &batch.records,
            ProducerCompression::default(),
            batch.producer_state,
            0,
        ) else {
            return 1.0;
        };
        if uncompressed.is_empty() {
            return 1.0;
        }
        compressed.len() as f32 / uncompressed.len() as f32
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
        retry_backoff: &mut BackoffState,
        retry: &IdempotentRetry,
    ) -> Option<DispatchOutcome> {
        if *attempts_remaining == 0 {
            if self.metrics_are_enabled() {
                self.metrics.record_error_for_topic(Some(&retry.topic));
            }
            return Some(DispatchOutcome::Delivered(Err(retry.broker_error())));
        }
        *attempts_remaining = attempts_remaining.saturating_sub(1);
        if self.metrics_are_enabled() {
            self.metrics.record_retry_for_topic(Some(&retry.topic));
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
        if let Some(error) = self.wait_before_retry(batches, retry_backoff, false).await {
            if self.metrics_are_enabled() {
                self.metrics.record_error_for_topic(Some(&retry.topic));
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

    async fn handle_drained_leadership_retry(
        &self,
        batches: &[ReadyBatch],
        attempts_remaining: &mut usize,
        retry_backoff: &mut BackoffState,
        retry: &LeadershipRetry,
    ) -> Option<DispatchOutcome> {
        if !retry.metadata_updated {
            self.wire
                .invalidate_topic_partition(&retry.topic, retry.partition);
        }
        if *attempts_remaining == 0 {
            if self.metrics_are_enabled() {
                self.metrics.record_error_for_topic(Some(&retry.topic));
            }
            self.release_idempotent_partition_after_definite_error(
                batches,
                &retry.topic,
                retry.partition,
            )
            .await;
            return Some(DispatchOutcome::Delivered(Err(retry.broker_error())));
        }
        *attempts_remaining = attempts_remaining.saturating_sub(1);
        if self.metrics_are_enabled() {
            self.metrics.record_retry_for_topic(Some(&retry.topic));
        }
        // Leader changed for the ongoing retry -> retry immediately (Kafka skips
        // backoff); otherwise back off normally.
        let retry_wait = if retry.metadata_updated {
            self.check_delivery_timeout_before_retry(batches, false)
                .await
        } else {
            self.wait_before_retry(batches, retry_backoff, false).await
        };
        if let Some(error) = retry_wait {
            if self.metrics_are_enabled() {
                self.metrics.record_error_for_topic(Some(&retry.topic));
            }
            self.release_idempotent_partition_after_definite_error(
                batches,
                &retry.topic,
                retry.partition,
            )
            .await;
            return Some(DispatchOutcome::Delivered(Err(error)));
        }
        None
    }

    async fn handle_retryable_wire_retry(
        &self,
        batches: &[ReadyBatch],
        attempts_remaining: &mut usize,
        retry_backoff: &mut BackoffState,
        error: ProducerError,
    ) -> Option<ProducerError> {
        if *attempts_remaining == 0 {
            if self.metrics_are_enabled() {
                self.metrics.record_error();
            }
            return Some(error);
        }
        *attempts_remaining = attempts_remaining.saturating_sub(1);
        if self.metrics_are_enabled() {
            self.metrics.record_retry();
        }
        // A wire/connection failure (e.g. a broker that went down) leaves this
        // dispatch's leader metadata stale -- it still points at the broker that
        // just dropped. Unlike a NotLeader broker response, a connection error
        // carries no fresh leader, so invalidate the affected partitions to force
        // the next attempt to re-fetch metadata and re-route to the new leaders.
        // Without this the retry loops back to the same dead broker until the
        // delivery timeout, wedging every partition batched into this dispatch
        // (including ones whose own leader is still healthy). Mirrors Java's
        // `NetworkClient` requesting a metadata update on server disconnect.
        for batch in batches {
            self.wire
                .invalidate_topic_partition(&batch.topic, batch.partition);
        }
        // A wire/connection failure left no broker response, so a final delivery
        // timeout here is an AMBIGUOUS loss (the records may have been written) and
        // must bump the idempotent epoch before the next produce.
        if let Some(error) = self.wait_before_retry(batches, retry_backoff, true).await {
            if self.metrics_are_enabled() {
                self.metrics.record_error();
            }
            return Some(error);
        }
        None
    }

    /// Retry a generic retriable broker produce error (Kafka `Sender.canRetry`):
    /// retry while attempts remain and the delivery timeout has not elapsed,
    /// otherwise fail the batch definitively.
    #[expect(
        clippy::too_many_arguments,
        reason = "Mirrors the existing leadership/wire retry handlers' parameter set."
    )]
    async fn handle_retryable_broker_retry(
        &self,
        batches: &[ReadyBatch],
        attempts_remaining: &mut usize,
        retry_backoff: &mut BackoffState,
        topic: &str,
        partition: i32,
        error: ErrorCode,
    ) -> Option<ProducerError> {
        if *attempts_remaining == 0 {
            if self.metrics_are_enabled() {
                self.metrics.record_error_for_topic(Some(topic));
            }
            self.release_idempotent_partition_after_definite_error(batches, topic, partition)
                .await;
            return Some(ProducerError::Broker {
                topic: topic.to_owned(),
                partition,
                error,
            });
        }
        *attempts_remaining = attempts_remaining.saturating_sub(1);
        if self.metrics_are_enabled() {
            self.metrics.record_retry_for_topic(Some(topic));
        }
        if let Some(timeout_error) = self.wait_before_retry(batches, retry_backoff, false).await {
            if self.metrics_are_enabled() {
                self.metrics.record_error_for_topic(Some(topic));
            }
            self.release_idempotent_partition_after_definite_error(batches, topic, partition)
                .await;
            return Some(timeout_error);
        }
        None
    }

    #[expect(
        clippy::too_many_lines,
        reason = "Produce request preparation keeps idempotent sequence rewind next to each local \
                  pre-send error path."
    )]
    async fn dispatch_batches(
        &self,
        batches: &mut [ReadyBatch],
        enqueue_ticket: u64,
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
        let mut batch_metric_samples = Vec::new();
        for batch in &mut *batches {
            let Some(first_record) = batch.records.first().cloned() else {
                continue;
            };
            let mut route = match self.route_for_batch(&metadata, batch, &first_record).await {
                Ok(route) => route,
                Err(error) => {
                    self.rewind_unsent_idempotent_sequences(batches).await;
                    return Err(DispatchError::from_route(error));
                },
            };
            if let Err(error) = self.add_partition_to_transaction(&route).await {
                self.rewind_unsent_idempotent_sequences(batches).await;
                return Err(DispatchError::from(error));
            }
            // A batch whose epoch was bumped since it was stamped (stale) must not be sent
            // under the fenced old epoch. Re-enqueue the whole dispatch so it is re-stamped
            // (fresh sequence under the new epoch) on the next drain via prepare_drained_
            // batches, where the in-flight registration is rebuilt consistently.
            if let Some(producer_state) = batch.producer_state
                && self
                    .producer_state
                    .lock()
                    .await
                    .is_stale_identity(producer_state.identity)
            {
                self.rewind_unsent_idempotent_sequences(batches).await;
                return Err(DispatchError::Requeue);
            }
            if batch.producer_state.is_none() {
                match self.producer_batch_state(&route, batch.records.len()).await {
                    Ok(ProducerBatchPrep::Ready(producer_state)) => {
                        batch.producer_state = producer_state;
                    },
                    // The partition entered unresolved-sequence recovery with in-flight
                    // batches between selection and encoding (Kafka stop-drain): re-enqueue
                    // the whole dispatch so these batches are re-evaluated (and deferred)
                    // on the next drain instead of being sent out of order.
                    Ok(ProducerBatchPrep::DeferUnresolved) => {
                        self.rewind_unsent_idempotent_sequences(batches).await;
                        return Err(DispatchError::Requeue);
                    },
                    Err(error) => {
                        self.rewind_unsent_idempotent_sequences(batches).await;
                        return Err(DispatchError::from(error));
                    },
                }
            }
            route.base_sequence = batch.producer_state.map(|state| state.base_sequence);
            let records = match self.encode_ready_batch_records_owned(batch, 0) {
                Ok(records) => records,
                Err(error) => {
                    self.rewind_unsent_idempotent_sequences(batches).await;
                    return Err(error);
                },
            };
            let options = BrokerProduceOptions {
                acks: self.acks,
                timeout_ms: self.timeout_ms,
                transactional_id: self.idempotence.transactional_id.as_deref(),
            };
            let requests = by_broker.entry(route.leader_id).or_default();
            let placement = match broker_request_placement_for_batch(
                requests,
                &route,
                &records,
                options,
                ProduceRequestSizing {
                    version,
                    max_request_size: self.max_request_size,
                },
            ) {
                Ok(placement) => placement,
                Err(error) => {
                    self.rewind_unsent_idempotent_sequences(batches).await;
                    return Err(error);
                },
            };
            if placement.split && self.metrics_are_enabled() {
                self.metrics.record_request_split();
            }
            let request_base_offset =
                match request_batch_base_offset(requests, placement.index, &route) {
                    Ok(offset) => offset,
                    Err(error) => {
                        self.rewind_unsent_idempotent_sequences(batches).await;
                        return Err(error);
                    },
                };
            let records = if request_base_offset == 0 {
                records
            } else {
                match self.encode_ready_batch_records_owned(batch, request_base_offset) {
                    Ok(records) => records,
                    Err(error) => {
                        self.rewind_unsent_idempotent_sequences(batches).await;
                        return Err(error);
                    },
                }
            };
            let batch_metric_sample =
                self.metrics_are_enabled()
                    .then(|| ProduceBatchMetricSample {
                        topic: batch.topic.clone(),
                        bytes: records.len(),
                        records: batch.records.len(),
                        queued: Instant::now().saturating_duration_since(batch.first_append_at),
                        // Per-record serialized (uncompressed) sizes for Kafka's
                        // record-size avg/max, mirroring Kafka's per-record metric.
                        record_sizes: batch
                            .records
                            .iter()
                            .map(|record| {
                                record
                                    .key
                                    .as_ref()
                                    .map_or(0, bytes::Bytes::len)
                                    .saturating_add(
                                        record.value.as_ref().map_or(0, bytes::Bytes::len),
                                    )
                            })
                            .collect(),
                        compression_ratio: self.actual_compression_ratio_for_encoded_batch(
                            batch,
                            request_base_offset,
                            records.len(),
                        ),
                    });
            if placement.index == requests.len() {
                requests.push(BrokerProduceRequest::with_record_buffer_owner(&self.wire));
            }
            let Some(request) = requests.get_mut(placement.index) else {
                self.rewind_unsent_idempotent_sequences(batches).await;
                return Err(DispatchError::Producer(ProducerError::FlushIncomplete));
            };
            request.push(route, records, options, batch.records.len());
            if let Some(sample) = batch_metric_sample {
                batch_metric_samples.push(sample);
            }
        }

        if self.metrics_are_enabled() {
            for sample in batch_metric_samples {
                self.metrics.record_produce_batch_with_compression_ratio(
                    &sample.topic,
                    sample.bytes,
                    self.partition_sticky_batch_size,
                    sample.records,
                    sample.compression_ratio,
                );
                self.metrics.record_queue_time(sample.queued);
                self.metrics.record_record_sizes(&sample.record_sizes);
            }
        }

        let mut receipts = Vec::new();
        for (broker_id, requests) in by_broker {
            let request_started = self.metrics_are_enabled().then(Instant::now);
            let mut broker_receipts = self
                .dispatch_broker_requests(broker_id, requests, version, enqueue_ticket)
                .await?;
            if let Some(started) = request_started {
                self.metrics.record_request_latency(started.elapsed());
            }
            receipts.append(&mut broker_receipts);
        }
        Ok(receipts)
    }

    #[expect(
        clippy::cast_precision_loss,
        reason = "Producer metrics expose compression ratios as f64 like Kafka metrics."
    )]
    fn actual_compression_ratio_for_encoded_batch(
        &self,
        batch: &ReadyBatch,
        base_offset: i64,
        encoded_bytes: usize,
    ) -> f64 {
        if self.compression.codec == Compression::None {
            return 1.0;
        }
        let Ok(uncompressed) = encode_record_batch_with_producer_state_at_offset(
            &batch.records,
            ProducerCompression::default(),
            batch.producer_state,
            base_offset,
        ) else {
            return 1.0;
        };
        if uncompressed.is_empty() {
            return 1.0;
        }
        encoded_bytes as f64 / uncompressed.len() as f64
    }

    #[cfg(test)]
    fn encode_ready_batch_records(
        &self,
        batch: &ReadyBatch,
        base_offset: i64,
    ) -> std::result::Result<bytes::Bytes, DispatchError> {
        let mut buffer = self
            .wire
            .acquire_write_buffer(batch.bytes.max(RECORD_BATCH_OVERHEAD_BYTES));
        let result = encode_record_batch_with_producer_state_at_offset_into(
            &batch.records,
            self.compression,
            batch.producer_state,
            base_offset,
            &mut buffer,
        )
        .map_err(DispatchError::from);
        self.wire.release_write_buffer(buffer);
        result
    }

    fn encode_ready_batch_records_owned(
        &self,
        batch: &ReadyBatch,
        base_offset: i64,
    ) -> std::result::Result<bytes::Bytes, DispatchError> {
        let mut buffer = self
            .wire
            .acquire_write_buffer(batch.bytes.max(RECORD_BATCH_OVERHEAD_BYTES));
        match encode_record_batch_with_producer_state_at_offset_into_buffer(
            &batch.records,
            self.compression,
            batch.producer_state,
            base_offset,
            &mut buffer,
        ) {
            Ok(()) => Ok(bytes::Bytes::from_owner(PooledRecordBuffer::new(
                buffer,
                self.wire.clone(),
            ))),
            Err(error) => {
                self.wire.release_write_buffer(buffer);
                Err(DispatchError::from(error))
            },
        }
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
        enqueue_ticket: u64,
    ) -> std::result::Result<Vec<RecordMetadata>, DispatchError> {
        let mut pending: VecDeque<_> = requests.into_iter().enumerate().collect();
        let mut in_flight = JoinSet::new();
        let mut completed = Vec::new();
        let mut in_flight_routes = AHashSet::new();
        let max_in_flight = self.broker_dispatch_in_flight_limit();
        // Only idempotent/transactional producers must serialize same-partition
        // requests; non-idempotent producers pipeline them up to max.in.flight.
        let enforce_partition_ordering = self.idempotence.enabled;
        // Wait for this dispatch's spawn-order turn before enqueuing, so concurrent
        // same-partition requests reach the broker in ascending base-sequence order. The
        // turn is released (advanced) as soon as this dispatch's requests are enqueued
        // (pending drained), letting the concurrent response waits overlap the next turn.
        self.enqueue_sequencer.wait_turn(enqueue_ticket).await;
        let mut enqueue_turn = EnqueueTurn {
            sequencer: &self.enqueue_sequencer,
            ticket: enqueue_ticket,
            advanced: false,
        };

        loop {
            while in_flight.len() < max_in_flight {
                let Some((index, request)) = pop_dispatchable_broker_request(
                    &mut pending,
                    &in_flight_routes,
                    enforce_partition_ordering,
                ) else {
                    break;
                };
                for route in &request.routes {
                    let _inserted = in_flight_routes.insert(TopicPartitionKey::from(route));
                }
                let metrics = self.metrics.clone();
                let metrics_enabled = self.metrics_are_enabled();
                let backpressure_deadline =
                    backpressure_deadline_after(self.delivery_timeout, Instant::now());
                if metrics_enabled {
                    let request_bytes = match RequestMessage::encoded_len(&request.data, version) {
                        Ok(request_bytes) => request_bytes,
                        Err(error) => {
                            let mut request = request;
                            let _released_record_buffers =
                                request.release_record_buffers(&self.wire);
                            return Err(DispatchError::from(crate::wire::WireError::from(error)));
                        },
                    };
                    metrics.record_produce_request(
                        request_bytes,
                        request.payload_bytes,
                        request.record_count,
                    );
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
                            backpressure_deadline,
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
                        backpressure_deadline,
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
                enqueue_turn.advance();
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
            completed.push(broker_dispatch_completed_result(
                &self.wire, index, request, response,
            )?);
        }
        completed.sort_by_key(|(index, _request, _response)| *index);

        let mut receipts = Vec::new();
        for (_index, request, response) in completed {
            let mut updated_leaders = AHashSet::new();
            let request_receipts = match response {
                ProduceDispatchResponse::Acknowledged(response) => {
                    // Kafka honors the broker-imposed quota throttle window by
                    // muting the channel for throttle_time_ms. Honor it here by
                    // delaying this dispatch before the next request is issued.
                    // No-op in the common (unthrottled) case where it is 0.
                    if response.throttle_time_ms > 0
                        && let Ok(throttle_ms) = u64::try_from(response.throttle_time_ms)
                    {
                        if self.metrics_are_enabled() {
                            self.metrics
                                .record_throttle_time(Duration::from_millis(throttle_ms));
                        }
                        tokio::time::sleep(Duration::from_millis(throttle_ms)).await;
                    }
                    let endpoints = node_endpoint_updates(&response);
                    if !endpoints.is_empty()
                        && let Err(error) = self.wire.upsert_broker_metadata(&endpoints).await
                    {
                        Err(ProduceReceiptError::Producer(ProducerError::Wire(error)))
                    } else {
                        let leader_changes = current_leader_updates(&response);
                        for update in leader_changes {
                            let topic = resolved_leader_update_topic(&update, &request.routes);
                            let leader_broker = endpoints
                                .iter()
                                .find(|endpoint| endpoint.node_id == update.leader_id);
                            let applied_leader_change =
                                self.wire
                                    .apply_partition_leader_update(PartitionLeaderChange {
                                        topic,
                                        partition_index: update.partition,
                                        leader_id: update.leader_id,
                                        leader_epoch: update.leader_epoch,
                                        leader_broker,
                                    });
                            if applied_leader_change {
                                let _inserted = updated_leaders.insert(TopicPartitionKey {
                                    topic: topic.to_owned(),
                                    partition: update.partition,
                                });
                            }
                        }
                        produce_receipts_with_error_details(&response, &request.routes)
                    }
                },
                ProduceDispatchResponse::NoAcknowledgement => Ok(no_ack_receipts(&request.routes)),
            };
            match request_receipts {
                Ok(mut request_receipts) => receipts.append(&mut request_receipts),
                Err(ProduceReceiptError::Broker(error)) if is_leadership_error(error.error) => {
                    if self.metrics_are_enabled() {
                        self.metrics.record_error();
                    }
                    let metadata_updated = updated_leaders.contains(&TopicPartitionKey {
                        topic: error.topic.clone(),
                        partition: error.partition,
                    });
                    return Err(DispatchError::RetryableLeadership {
                        topic: error.topic,
                        partition: error.partition,
                        error: error.error,
                        metadata_updated,
                    });
                },
                Err(ProduceReceiptError::Broker(error))
                    if error.error == ErrorCode::MessageTooLarge =>
                {
                    return Err(DispatchError::SplitAndRequeue {
                        topic: error.topic,
                        partition: error.partition,
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
                Err(ProduceReceiptError::Broker(error)) if error.error.is_retriable() => {
                    // Kafka Sender.canRetry: any RetriableException is retried
                    // (non-transactional path; transactional errors are handled
                    // above) subject to attempts < retries and the delivery
                    // timeout.
                    return Err(DispatchError::RetryableBroker {
                        topic: error.topic,
                        partition: error.partition,
                        error: error.error,
                    });
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

    async fn rewind_unsent_idempotent_sequences(&self, batches: &[ReadyBatch]) {
        if !self.idempotence.enabled {
            return;
        }
        let mut rewind_to: AHashMap<TopicPartitionKey, i32> = AHashMap::new();
        for batch in batches {
            let Some(producer_state) = batch.producer_state else {
                continue;
            };
            let key = TopicPartitionKey {
                topic: batch.topic.clone(),
                partition: batch.partition,
            };
            let _previous = rewind_to
                .entry(key)
                .and_modify(|base_sequence| {
                    *base_sequence = (*base_sequence).min(producer_state.base_sequence);
                })
                .or_insert(producer_state.base_sequence);
        }
        if rewind_to.is_empty() {
            return;
        }
        let mut state = self.producer_state.lock().await;
        for (key, base_sequence) in rewind_to {
            state.rewind_sequence_to(&key.topic, key.partition, base_sequence);
        }
    }

    fn broker_dispatch_in_flight_limit(&self) -> usize {
        self.max_in_flight_requests_per_connection.max(1)
    }

    async fn route_for_batch(
        &self,
        metadata: &crate::wire::ClusterMetadata,
        batch: &mut ReadyBatch,
        first_record: &ProducerRecord,
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
                self.compression_ratio_estimation(record.topic.as_ref()),
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

    async fn wait_before_retry(
        &self,
        batches: &[ReadyBatch],
        retry_backoff: &mut BackoffState,
        loss_is_ambiguous: bool,
    ) -> Option<ProducerError> {
        let now = Instant::now();
        // Label any timeout with the batch that actually expired first (earliest
        // append), not an arbitrary `batches.first()` -- a dispatch can carry
        // batches for several partitions, so `.first()` could name a partition
        // that had not yet timed out.
        let earliest_batch = batches.iter().min_by_key(|batch| batch.first_append_at);
        let earliest = earliest_batch.map_or(now, |batch| batch.first_append_at);
        let elapsed = now.duration_since(earliest);
        if elapsed >= self.delivery_timeout {
            self.mark_expired_idempotent_batches_unresolved(batches, now, loss_is_ambiguous)
                .await;
            return earliest_batch.map(|batch| ProducerError::DeliveryTimeout {
                topic: batch.topic.clone(),
                partition: batch.partition,
            });
        }
        let remaining = self.delivery_timeout.saturating_sub(elapsed);
        let delay = match retry_backoff.next_delay() {
            Ok(delay) => delay,
            Err(error) => return Some(error.into()),
        };
        tokio::time::sleep(delay.min(remaining)).await;
        let now = Instant::now();
        if let Some(batch) = self.expired_batch(batches, now) {
            self.mark_expired_idempotent_batches_unresolved(batches, now, loss_is_ambiguous)
                .await;
            return Some(ProducerError::DeliveryTimeout {
                topic: batch.topic.clone(),
                partition: batch.partition,
            });
        }
        None
    }

    /// Kafka `hasLeaderChangedForTheOngoingRetry`: when the partition leader has
    /// changed for the ongoing retry, retry immediately without the backoff
    /// sleep, while still honoring the delivery timeout. Mirrors the
    /// delivery-timeout guard of [`Self::wait_before_retry`] without the wait.
    async fn check_delivery_timeout_before_retry(
        &self,
        batches: &[ReadyBatch],
        loss_is_ambiguous: bool,
    ) -> Option<ProducerError> {
        let now = Instant::now();
        let earliest_batch = batches.iter().min_by_key(|batch| batch.first_append_at);
        let earliest = earliest_batch.map_or(now, |batch| batch.first_append_at);
        if now.duration_since(earliest) >= self.delivery_timeout {
            self.mark_expired_idempotent_batches_unresolved(batches, now, loss_is_ambiguous)
                .await;
            return earliest_batch.map(|batch| ProducerError::DeliveryTimeout {
                topic: batch.topic.clone(),
                partition: batch.partition,
            });
        }
        None
    }

    fn retry_backoff_state(&self) -> BackoffState {
        BackoffState::new(BackoffPolicy::new(
            self.retry_backoff,
            self.retry_backoff_max,
        ))
    }

    async fn sleep_retry_backoff(&self, retry_backoff: &mut BackoffState) -> Result<()> {
        let delay = retry_backoff.next_delay()?;
        tokio::time::sleep(delay).await;
        Ok(())
    }

    async fn mark_expired_idempotent_batches_unresolved(
        &self,
        batches: &[ReadyBatch],
        now: Instant,
        loss_is_ambiguous: bool,
    ) {
        if !self.idempotence.enabled {
            return;
        }
        let transactional = self.idempotence.transactional_id.is_some();
        let mut state = self.producer_state.lock().await;
        let mut expired_partitions: Vec<(String, i32)> = Vec::new();
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
            // Remember the loss ambiguity with the marker so the resolve that runs
            // once the partition drains bumps the epoch only for ambiguous losses,
            // even when it is deferred past this call.
            state.record_unresolved_loss_ambiguity(
                &batch.topic,
                batch.partition,
                loss_is_ambiguous,
            );
            if !expired_partitions
                .iter()
                .any(|(topic, partition)| topic == &batch.topic && *partition == batch.partition)
            {
                expired_partitions.push((batch.topic.clone(), batch.partition));
            }
        }
        // Kafka maybeResolveSequences: attempt to resolve each partition now. While
        // this expiring request (or any sibling request) is still tracked in flight,
        // `resolve_unresolved_sequence_after_drain` defers; the deferred resolve is
        // retriggered from `release_idempotent_inflight_after_terminal` as each
        // request terminally completes, so the epoch is bumped only after the
        // partition has fully drained.
        for (topic, partition) in expired_partitions {
            let ambiguous = state.unresolved_loss_ambiguous(&topic, partition);
            state.resolve_unresolved_sequence_after_drain(
                &topic,
                partition,
                transactional,
                ambiguous,
            );
        }
    }

    async fn producer_batch_state(
        &self,
        route: &ProduceRoute,
        record_count: usize,
    ) -> Result<ProducerBatchPrep> {
        if !self.idempotence.enabled {
            return Ok(ProducerBatchPrep::Ready(None));
        }
        let record_count =
            i32::try_from(record_count).map_err(|_error| ProducerError::SequenceOverflow {
                topic: route.topic.clone(),
                partition: route.partition,
            })?;
        // Idempotent (non-transactional) lost-sequence recovery: an ambiguous loss
        // (a no-response/connection delivery timeout) sets `epoch_bump_required` while
        // clearing the unresolved marker, so bump the producer epoch via InitProducerId
        // and reset the per-partition sequences before the next produce — Kafka
        // bumpIdempotentEpochAndResetIdIfNeeded. Definitive rejections never set the
        // flag (see resolve_unresolved_sequence_after_drain), so leadership / unknown-
        // producer-id recovery is left to their own retry paths.
        if self.idempotence.transactional_id.is_none()
            && self.producer_state.lock().await.epoch_bump_required
        {
            let identity = self.bump_producer_identity(route.leader_id).await?;
            let mut state = self.producer_state.lock().await;
            state.reset_sequences_after_epoch_bump();
            let base_sequence = state.next_sequence(&route.topic, route.partition, record_count)?;
            drop(state);
            return Ok(ProducerBatchPrep::Ready(Some(ProducerBatchState {
                identity,
                base_sequence,
            })));
        }
        let identity = self.producer_identity(route.leader_id).await?;
        let mut state = self.producer_state.lock().await;
        let base_sequence = match state.next_sequence(&route.topic, route.partition, record_count) {
            Ok(base_sequence) => base_sequence,
            Err(ProducerError::UnresolvedSequence { .. })
                if self.idempotence.transactional_id.is_none() =>
            {
                // Kafka `shouldStopDrainBatchesForPartition`: while the partition has
                // an unresolved sequence AND still has in-flight batches, do NOT drain
                // this fresh batch — defer it until the in-flight requests drain and
                // `maybeResolveSequences` resolves the marker (which may clear it with
                // no epoch bump if the gap was filled). Only bump+reset when the
                // partition has fully drained yet is still unresolved (a genuine gap),
                // matching `bumpIdempotentEpochAndResetIdIfNeeded`.
                if state.has_inflight_batches(&route.topic, route.partition) {
                    drop(state);
                    return Ok(ProducerBatchPrep::DeferUnresolved);
                }
                drop(state);
                let identity = self.bump_producer_identity(route.leader_id).await?;
                let mut state = self.producer_state.lock().await;
                // A producer-epoch bump invalidates EVERY partition's old sequences, so
                // restart them all at 0 under the new epoch (Kafka startSequencesAtBeginning
                // applies per-partition as each drains; kacrab's global reset is equivalent
                // because stale in-flight batches are re-stamped, not renumbered in place).
                state.reset_sequences_after_epoch_bump();
                let base_sequence =
                    state.next_sequence(&route.topic, route.partition, record_count)?;
                drop(state);
                return Ok(ProducerBatchPrep::Ready(Some(ProducerBatchState {
                    identity,
                    base_sequence,
                })));
            },
            Err(error) => return Err(error),
        };
        drop(state);
        Ok(ProducerBatchPrep::Ready(Some(ProducerBatchState {
            identity,
            base_sequence,
        })))
    }

    async fn recover_idempotent_partition(
        &self,
        batches: &mut [ReadyBatch],
        topic: &str,
        partition: i32,
        leader_id: i32,
    ) -> Result<()> {
        // Kafka `sequenceHasBeenReset`/`reopened` short-circuit: if the producer epoch was
        // already bumped (e.g. by a sibling in-flight request's recovery), these batches
        // are already stale — re-stamping them under the current epoch is enough; do NOT
        // bump the epoch a second time (which would churn through InitProducerId and reset
        // every partition again).
        let already_bumped = {
            let state = self.producer_state.lock().await;
            batches
                .iter()
                .filter(|batch| batch.topic == topic && batch.partition == partition)
                .filter_map(|batch| batch.producer_state)
                .any(|producer_state| state.is_stale_identity(producer_state.identity))
        };
        if !already_bumped {
            let _identity = self.bump_producer_identity(leader_id).await?;
            // A producer-epoch bump invalidates every partition's sequences, so restart
            // them all at 0 under the new epoch (not just this one). Stale in-flight
            // batches on other partitions are re-stamped on their next drain.
            self.producer_state
                .lock()
                .await
                .reset_sequences_after_epoch_bump();
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
            if record_count < 1 {
                continue;
            }
            let next_sequence = increment_sequence(producer_state.base_sequence, record_count);
            state.release_sequence(&batch.topic, batch.partition, next_sequence);
            // Kafka handleCompletedBatch: record the last acked sequence/offset for
            // this partition (used by maybeResolveSequences and UnknownProducerId
            // disambiguation). lastSequence = base + recordCount - 1 (wrapping).
            let last_sequence =
                increment_sequence(producer_state.base_sequence, record_count.saturating_sub(1));
            state.maybe_update_last_acked_sequence(&batch.topic, batch.partition, last_sequence);
            if let Some(receipt) = receipts.iter().find(|receipt| {
                receipt.topic.as_ref() == batch.topic && receipt.partition == batch.partition
            }) {
                state.update_last_acked_offset(&batch.topic, batch.partition, receipt.offset);
            }
        }
    }

    #[expect(
        clippy::too_many_lines,
        reason = "AddPartitions state tracking, coordinator retry, and partition-set promotion \
                  mirror Kafka ordering."
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
        let mut retry_backoff = self.retry_backoff_state();
        // Kafka maybeOverrideRetryBackoffMs latches a 20ms backoff once the first
        // AddPartitions of a transaction hits CONCURRENT_TRANSACTIONS.
        let mut reduced_concurrent_backoff = false;
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
                    self.sleep_retry_backoff(&mut retry_backoff).await?;
                },
                Err(ProducerError::Transaction {
                    error: transaction_error,
                    ..
                }) if attempts_remaining > 0
                    && is_add_partitions_retry_error(transaction_error) =>
                {
                    attempts_remaining = attempts_remaining.saturating_sub(1);
                    if transaction_error == ErrorCode::ConcurrentTransactions
                        && !reduced_concurrent_backoff
                    {
                        let partitions_empty = {
                            let state = self.producer_state.lock().await;
                            state.partitions_in_transaction.is_empty()
                        };
                        reduced_concurrent_backoff = partitions_empty;
                    }
                    if reduced_concurrent_backoff {
                        tokio::time::sleep(ADD_PARTITIONS_RETRY_BACKOFF).await;
                    } else {
                        self.sleep_retry_backoff(&mut retry_backoff).await?;
                    }
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
        let mut retry_backoff = self.retry_backoff_state();
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
                self.sleep_retry_backoff(&mut retry_backoff).await?;
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
            self.sleep_retry_backoff(&mut retry_backoff).await?;
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
        let mut retry_backoff = self.retry_backoff_state();
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
                    self.sleep_retry_backoff(&mut retry_backoff).await?;
                },
                Some(error) if attempts_remaining > 0 && error.is_retriable() => {
                    attempts_remaining = attempts_remaining.saturating_sub(1);
                    self.sleep_retry_backoff(&mut retry_backoff).await?;
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

    /// Record whether the transaction coordinator can bump the producer epoch,
    /// derived from the `InitProducerId` version it advertised (Kafka's
    /// `handleCoordinatorReady` / `coordinatorSupportsBumpingEpoch`). Only flips
    /// the flag when a concrete coordinator version is known, so a transient
    /// missing-capability read never falsely escalates errors to fatal.
    async fn record_coordinator_epoch_bump_support(&self, coordinator_id: i32) {
        if let Some(max_version) = self
            .wire
            .negotiated_version(coordinator_id, ApiKey::InitProducerId)
        {
            let mut state = self.producer_state.lock().await;
            state.coordinator_lacks_epoch_bump_support =
                max_version < COORDINATOR_EPOCH_BUMP_MIN_INIT_PRODUCER_ID_VERSION;
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
                if state.coordinator_lacks_epoch_bump_support {
                    // Coordinator cannot bump the epoch, so this abortable error
                    // is unrecoverable and must escalate to fatal (Kafka
                    // canHandleAbortableError() == false).
                    state.abortable_error = None;
                    state.fatal_error = Some(error);
                    state.transaction_state = TransactionState::FatalError;
                } else {
                    state.epoch_bump_required = true;
                }
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
            } else if state.coordinator_lacks_epoch_bump_support {
                // Recovering this abortable error needs a client-side epoch
                // bump the coordinator cannot perform, so escalate to fatal
                // (Kafka canHandleAbortableError() == false).
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
        let expected = {
            let state = self.producer_state.lock().await;
            state.identity
        };
        // Serialize bumps so concurrent recoveries (e.g. two sibling in-flight requests
        // both failing with UNKNOWN_PRODUCER_ID) do not each send an InitProducerId.
        let _init_guard = self.producer_identity_init.lock().await;
        {
            let state = self.producer_state.lock().await;
            // A concurrent recovery already advanced the identity past what we read: the
            // bump already happened, so reuse it instead of bumping again (Kafka's single
            // epoch bump per recovery generation / reopened short-circuit).
            if let Some(current) = state.identity
                && Some(current) != expected
            {
                return Ok(current);
            }
        }
        let response = self.init_producer_identity(broker_id, expected).await?;
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
        let mut retry_backoff = self.retry_backoff_state();
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
                self.record_coordinator_epoch_bump_support(broker_id).await;
                request_guard.clear().await;
                return Ok(response);
            }
            if attempts_remaining > 0
                && is_transaction_coordinator_error(error)
                && let Some(transactional_id) = transactional_id.as_deref()
            {
                attempts_remaining = attempts_remaining.saturating_sub(1);
                broker_id = self.refresh_coordinator_id(transactional_id).await?;
                self.sleep_retry_backoff(&mut retry_backoff).await?;
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
            self.sleep_retry_backoff(&mut retry_backoff).await?;
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
        let mut retry_backoff = self.retry_backoff_state();
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
            self.sleep_retry_backoff(&mut retry_backoff).await?;
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

const fn validate_consumer_group_metadata(group_metadata: &ConsumerGroupMetadata) -> Result<()> {
    if group_metadata.generation_id > 0 && group_metadata.member_id.is_empty() {
        return Err(ProducerError::InvalidConsumerGroupMetadata(
            "generation_id > 0 requires a known member_id",
        ));
    }
    Ok(())
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

/// Kafka `TxnPartitionEntry::NO_LAST_ACKED_SEQUENCE_NUMBER`.
const NO_LAST_ACKED_SEQUENCE: i32 = -1;
/// Kafka `ProduceResponse::INVALID_OFFSET`.
const INVALID_LAST_ACKED_OFFSET: i64 = -1;

/// Kafka `DefaultRecordBatch.incrementSequence`: advance a non-negative idempotent
/// base sequence by `increment`, wrapping at `i32::MAX` back to 0 (Kafka sequences
/// are 31-bit and wrap rather than overflow). `increment` is a record count (>= 0).
const fn increment_sequence(sequence: i32, increment: i32) -> i32 {
    // All branches are provably non-overflowing for non-negative `sequence`/`increment`;
    // wrapping ops also mirror Kafka's wrapping int arithmetic exactly.
    if sequence > i32::MAX.wrapping_sub(increment) {
        increment
            .wrapping_sub(i32::MAX.wrapping_sub(sequence))
            .wrapping_sub(1)
    } else {
        sequence.wrapping_add(increment)
    }
}

#[derive(Debug)]
pub(crate) enum DispatchOutcome {
    Delivered(Result<Vec<RecordMetadata>>),
    Requeue(Vec<ReadyBatch>),
}

/// Outcome of assigning idempotent state to one freshly drained batch.
#[derive(Debug)]
enum ProducerBatchPrep {
    /// Sequence assigned (`Some` for idempotent, `None` for non-idempotent).
    Ready(Option<ProducerBatchState>),
    /// The partition has an unresolved sequence with in-flight batches: stop
    /// draining this batch until the partition resolves (Kafka
    /// `shouldStopDrainBatchesForPartition`).
    DeferUnresolved,
}

#[derive(Debug)]
enum DispatchError {
    Producer(ProducerError),
    Requeue,
    SplitAndRequeue {
        topic: String,
        partition: i32,
    },
    RetryableLeadership {
        topic: String,
        partition: i32,
        error: ErrorCode,
        metadata_updated: bool,
    },
    RetryableIdempotent {
        topic: String,
        partition: i32,
        leader_id: i32,
        error: ErrorCode,
        reset_sequence: bool,
    },
    /// A generic retriable broker produce error (Kafka `RetriableException`) that
    /// is neither leadership- nor idempotence-specific.
    RetryableBroker {
        topic: String,
        partition: i32,
        error: ErrorCode,
    },
    RetryableWire(ProducerError),
}

struct IdempotentRetry {
    topic: String,
    partition: i32,
    leader_id: i32,
    error: ErrorCode,
    reset_sequence: bool,
}

struct LeadershipRetry {
    topic: String,
    partition: i32,
    error: ErrorCode,
    metadata_updated: bool,
}

#[derive(Clone, Copy)]
pub(crate) struct IdempotentRetryDecision<'a> {
    pub(crate) topic: &'a str,
    pub(crate) partition: i32,
    pub(crate) error: ErrorCode,
    pub(crate) log_start_offset: i64,
    pub(crate) base_sequence: Option<i32>,
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

impl LeadershipRetry {
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
        if is_retryable_wire_error(&error) {
            Self::RetryableWire(ProducerError::from(error))
        } else {
            Self::Producer(ProducerError::from(error))
        }
    }
}

const fn is_retryable_wire_error(error: &crate::wire::WireError) -> bool {
    matches!(
        error,
        crate::wire::WireError::ConnectionClosed
            | crate::wire::WireError::Timeout
            | crate::wire::WireError::Io(_)
    )
}

#[derive(Debug, Default)]
struct BrokerProduceRequest {
    data: ProduceRequestData,
    routes: Vec<ProduceRoute>,
    record_count: usize,
    batch_count: usize,
    record_buffer_leases: Vec<RecordBufferLease>,
    record_buffer_owner: Option<WireClient>,
    payload_bytes: usize,
}

impl BrokerProduceRequest {
    fn with_record_buffer_owner(wire: &WireClient) -> Self {
        Self {
            data: ProduceRequestData::default(),
            routes: Vec::new(),
            record_count: 0,
            batch_count: 0,
            record_buffer_leases: Vec::new(),
            record_buffer_owner: Some(wire.clone()),
            payload_bytes: 0,
        }
    }

    fn contains_route(&self, route: &ProduceRoute) -> bool {
        self.routes
            .iter()
            .any(|existing| produce_route_matches(existing, route))
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
        route: ProduceRoute,
        records: bytes::Bytes,
        options: BrokerProduceOptions<'_>,
        record_count: usize,
    ) {
        let mut route = route;
        apply_produce_options(&mut self.data, options);
        route.request_offset_delta = self
            .records_before_route(&route)
            .and_then(|count| i64::try_from(count).ok())
            .unwrap_or(i64::MAX);
        route.record_count = record_count;
        self.record_count = self.record_count.saturating_add(record_count);
        self.batch_count = self.batch_count.saturating_add(1);
        self.record_buffer_leases.push(RecordBufferLease::new(
            &route,
            records.len(),
            self.record_buffer_owner.clone(),
        ));
        self.payload_bytes = self.payload_bytes.saturating_add(records.len());
        push_partition_with_owner(
            &mut self.data.topic_data,
            &route,
            records,
            self.record_buffer_owner.as_ref(),
        );
        self.routes.push(route);
    }

    fn records_before_route(&self, route: &ProduceRoute) -> Option<usize> {
        self.routes
            .iter()
            .filter(|existing| produce_route_matches(existing, route))
            .try_fold(0usize, |count, existing| {
                count.checked_add(existing.record_count)
            })
    }

    fn release_record_buffers(&mut self, wire: &WireClient) -> RecordBufferRelease {
        self.release_record_buffers_with(|leases, topic_id, topic, partition| {
            if record_buffer_has_owner_for_partition(leases, topic_id, topic, partition) {
                Some(RecordBufferReleaseTarget::OwnedBytes)
            } else {
                Some(RecordBufferReleaseTarget::RecoveredBytes(wire.clone()))
            }
        })
    }

    fn release_owned_record_buffers(&mut self) -> RecordBufferRelease {
        self.release_record_buffers_with(|leases, topic_id, topic, partition| {
            record_buffer_has_owner_for_partition(leases, topic_id, topic, partition)
                .then_some(RecordBufferReleaseTarget::OwnedBytes)
        })
    }

    fn release_record_buffers_with(
        &mut self,
        mut release_target_for_partition: impl FnMut(
            &[RecordBufferLease],
            KafkaUuid,
            &str,
            i32,
        ) -> Option<RecordBufferReleaseTarget>,
    ) -> RecordBufferRelease {
        let mut release = RecordBufferRelease::from_leases(&self.record_buffer_leases);
        for topic in &mut self.data.topic_data {
            let topic_id = topic.topic_id;
            let topic_name = topic.name.as_str();
            for partition in &mut topic.partition_data {
                let Some(records) = partition.records.take() else {
                    continue;
                };
                let Some(target) = release_target_for_partition(
                    &self.record_buffer_leases,
                    topic_id,
                    topic_name,
                    partition.index,
                ) else {
                    partition.records = Some(records);
                    continue;
                };
                match target {
                    RecordBufferReleaseTarget::OwnedBytes => {
                        let released = release_record_buffer_leases_for_partition(
                            &mut self.record_buffer_leases,
                            topic_id,
                            topic_name,
                            partition.index,
                        );
                        drop(records);
                        release.released = release.released.saturating_add(released.expected);
                        release.released_bytes = release
                            .released_bytes
                            .saturating_add(released.expected_bytes);
                    },
                    RecordBufferReleaseTarget::RecoveredBytes(wire) => {
                        match records.try_into_mut() {
                            Ok(buffer) => {
                                let released = release_record_buffer_leases_for_partition(
                                    &mut self.record_buffer_leases,
                                    topic_id,
                                    topic_name,
                                    partition.index,
                                );
                                wire.release_write_buffer(buffer);
                                release.released =
                                    release.released.saturating_add(released.expected);
                                release.released_bytes = release
                                    .released_bytes
                                    .saturating_add(released.expected_bytes);
                            },
                            Err(records) => {
                                partition.records = Some(records);
                            },
                        }
                    },
                }
            }
        }
        let unreleased = RecordBufferRelease::from_leases(&self.record_buffer_leases);
        release.unrecovered = unreleased.expected;
        release.unrecovered_bytes = unreleased.expected_bytes;
        release
    }
}

impl Drop for BrokerProduceRequest {
    fn drop(&mut self) {
        let _released_record_buffers = self.release_owned_record_buffers();
    }
}

#[derive(Debug)]
struct PooledRecordBuffer {
    buffer: Option<BytesMut>,
    wire: WireClient,
}

impl PooledRecordBuffer {
    const fn new(buffer: BytesMut, wire: WireClient) -> Self {
        Self {
            buffer: Some(buffer),
            wire,
        }
    }
}

impl AsRef<[u8]> for PooledRecordBuffer {
    fn as_ref(&self) -> &[u8] {
        self.buffer.as_ref().map_or(&[], BytesMut::as_ref)
    }
}

impl Drop for PooledRecordBuffer {
    fn drop(&mut self) {
        if let Some(buffer) = self.buffer.take() {
            self.wire.release_write_buffer(buffer);
        }
    }
}

#[derive(Debug, Clone)]
enum RecordBufferReleaseTarget {
    OwnedBytes,
    RecoveredBytes(WireClient),
}

#[derive(Debug, Clone)]
struct RecordBufferLease {
    topic_id: KafkaUuid,
    topic: String,
    partition: i32,
    bytes: usize,
    released: bool,
    owner: Option<WireClient>,
}

impl RecordBufferLease {
    fn new(route: &ProduceRoute, bytes: usize, owner: Option<WireClient>) -> Self {
        Self {
            topic_id: route.topic_id,
            topic: route.topic.clone(),
            partition: route.partition,
            bytes,
            released: false,
            owner,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct RecordBufferRelease {
    expected: usize,
    released: usize,
    unrecovered: usize,
    expected_bytes: usize,
    released_bytes: usize,
    unrecovered_bytes: usize,
}

impl RecordBufferRelease {
    fn from_leases(leases: &[RecordBufferLease]) -> Self {
        leases
            .iter()
            .filter(|lease| !lease.released)
            .fold(Self::default(), |mut release, lease| {
                release.expected = release.expected.saturating_add(1);
                release.expected_bytes = release.expected_bytes.saturating_add(lease.bytes);
                release
            })
    }
}

fn release_record_buffer_leases_for_partition(
    leases: &mut [RecordBufferLease],
    topic_id: KafkaUuid,
    topic: &str,
    partition: i32,
) -> RecordBufferRelease {
    leases
        .iter_mut()
        .filter(|lease| {
            !lease.released
                && lease.partition == partition
                && produce_topic_key_matches(lease.topic_id, &lease.topic, topic_id, topic)
        })
        .fold(RecordBufferRelease::default(), |mut release, lease| {
            lease.released = true;
            release.expected = release.expected.saturating_add(1);
            release.expected_bytes = release.expected_bytes.saturating_add(lease.bytes);
            release
        })
}

fn record_buffer_has_owner_for_partition(
    leases: &[RecordBufferLease],
    topic_id: KafkaUuid,
    topic: &str,
    partition: i32,
) -> bool {
    leases.iter().any(|lease| {
        !lease.released
            && lease.owner.is_some()
            && lease.partition == partition
            && produce_topic_key_matches(lease.topic_id, &lease.topic, topic_id, topic)
    })
}

fn produce_route_matches(existing: &ProduceRoute, route: &ProduceRoute) -> bool {
    existing.partition == route.partition
        && produce_topic_key_matches(
            existing.topic_id,
            &existing.topic,
            route.topic_id,
            &route.topic,
        )
}

fn produce_topic_key_matches(
    existing_topic_id: KafkaUuid,
    existing_topic: &str,
    route_topic_id: KafkaUuid,
    route_topic: &str,
) -> bool {
    if existing_topic_id != KafkaUuid::ZERO && route_topic_id != KafkaUuid::ZERO {
        existing_topic_id == route_topic_id
    } else {
        existing_topic == route_topic
    }
}

fn broker_dispatch_completed_result(
    wire: &WireClient,
    index: usize,
    mut request: BrokerProduceRequest,
    response: crate::wire::Result<ProduceDispatchResponse>,
) -> std::result::Result<(usize, BrokerProduceRequest, ProduceDispatchResponse), DispatchError> {
    let _released_record_buffers = request.release_record_buffers(wire);
    let response = response.map_err(DispatchError::from)?;
    Ok((index, request, response))
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

fn resolved_leader_update_topic<'a>(
    update: &'a PartitionLeaderUpdate,
    routes: &'a [ProduceRoute],
) -> &'a str {
    if !update.topic.is_empty() {
        return update.topic.as_str();
    }
    routes
        .iter()
        .find(|route| route.topic_id == update.topic_id && route.partition == update.partition)
        .map_or(update.topic.as_str(), |route| route.topic.as_str())
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
    enforce_partition_ordering: bool,
) -> Option<(usize, BrokerProduceRequest)> {
    // Idempotent/transactional producers keep at most one in-flight request per
    // partition so broker-side sequence numbers can never reorder. Non-idempotent
    // producers may pipeline several requests to the same partition (up to
    // max.in.flight.requests.per.connection), matching Kafka, so they take requests
    // in FIFO order regardless of which partitions are already in flight.
    let dispatch_index = if enforce_partition_ordering {
        pending.iter().position(|(_index, request)| {
            !request_conflicts_with_in_flight(request, in_flight_routes)
        })?
    } else {
        if pending.is_empty() {
            return None;
        }
        0
    };
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
    backpressure_deadline: Option<Instant>,
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
                if backpressure_deadline_expired(backpressure_deadline) {
                    return Err(crate::wire::WireError::Backpressure);
                }
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
    backpressure_deadline: Option<Instant>,
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
                if backpressure_deadline_expired(backpressure_deadline) {
                    return Err(crate::wire::WireError::Backpressure);
                }
                backpressure.record(target.broker_id).await;
                tokio::task::yield_now().await;
            },
            result => return result,
        }
    }
}

fn backpressure_deadline_after(timeout: Duration, now: Instant) -> Option<Instant> {
    now.checked_add(timeout)
}

fn backpressure_deadline_expired(deadline: Option<Instant>) -> bool {
    deadline.is_some_and(|deadline| Instant::now() >= deadline)
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
        .flat_map(|route| {
            (0..route.record_count.max(1)).map(|_record_index| RecordMetadata {
                topic: Arc::from(route.topic.as_str()),
                partition: route.partition,
                leader_id: route.leader_id,
                offset: -1,
                timestamp_ms: -1,
                serialized_key_size: -1,
                serialized_value_size: -1,
            })
        })
        .collect()
}

#[derive(Debug, Clone, Copy)]
struct BrokerProduceOptions<'a> {
    acks: i16,
    timeout_ms: i32,
    transactional_id: Option<&'a str>,
}

#[derive(Debug, Clone)]
struct ProduceBatchMetricSample {
    topic: String,
    bytes: usize,
    records: usize,
    queued: Duration,
    record_sizes: Vec<usize>,
    compression_ratio: f64,
}

fn push_partition(topics: &mut Vec<TopicProduceData>, route: &ProduceRoute, records: bytes::Bytes) {
    push_partition_with_owner(topics, route, records, None);
}

fn push_partition_with_owner(
    topics: &mut Vec<TopicProduceData>,
    route: &ProduceRoute,
    records: bytes::Bytes,
    owner: Option<&WireClient>,
) {
    if let Some(topic) = topics.iter_mut().find(|topic| {
        produce_topic_key_matches(
            topic.topic_id,
            topic.name.as_str(),
            route.topic_id,
            &route.topic,
        )
    }) {
        if let Some(partition) = topic
            .partition_data
            .iter_mut()
            .find(|partition| partition.index == route.partition)
        {
            append_partition_records(partition, records, owner);
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

fn append_partition_records(
    partition: &mut PartitionProduceData,
    records: bytes::Bytes,
    owner: Option<&WireClient>,
) {
    match partition.records.take() {
        Some(existing) => {
            let capacity = existing.len().saturating_add(records.len());
            let mut combined = owner.map_or_else(
                || BytesMut::with_capacity(capacity),
                |wire| wire.acquire_write_buffer(capacity),
            );
            combined.extend_from_slice(&existing);
            combined.extend_from_slice(&records);
            partition.records = Some(match owner {
                Some(wire) => {
                    bytes::Bytes::from_owner(PooledRecordBuffer::new(combined, wire.clone()))
                },
                None => combined.freeze(),
            });
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

fn split_message_too_large_batches(
    batches: Vec<ReadyBatch>,
    topic: &str,
    partition: i32,
    target_batch_bytes: usize,
    compression_ratio: f32,
) -> Option<Vec<ReadyBatch>> {
    let mut split_batches = Vec::with_capacity(batches.len());
    let mut split_found = false;
    for batch in batches {
        if !split_found && batch.topic == topic && batch.partition == partition {
            let split = batch
                .split_for_retry_with_compression_ratio(target_batch_bytes, compression_ratio)?;
            split_batches.extend(split);
            split_found = true;
        } else {
            split_batches.push(batch);
        }
    }
    split_found.then_some(split_batches)
}

fn unique_unassigned_record_topics(records: &[ProducerRecord]) -> Vec<String> {
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
        let expected_receipts = sender.record_count();
        let receipt_indexes: Vec<_> = receipts
            .iter()
            .enumerate()
            .filter_map(|(index, receipt)| {
                (!used_receipts.get(index).copied().unwrap_or(false)
                    && receipt.topic.as_ref() == batch.topic
                    && receipt.partition == batch.partition)
                    .then_some(index)
            })
            .take(expected_receipts)
            .collect();
        if receipt_indexes.len() != expected_receipts {
            continue;
        }
        let record_receipts: Vec<RecordMetadata> = receipt_indexes
            .iter()
            .filter_map(|index| receipts.get(*index))
            .cloned()
            .collect();
        for receipt_index in receipt_indexes {
            if let Some(used) = used_receipts.get_mut(receipt_index) {
                *used = true;
            }
        }
        if expected_receipts == 1 {
            if let Some(receipt) = record_receipts.into_iter().next() {
                sender.send(&receipt);
            }
        } else {
            sender.send_records(record_receipts);
        }
    }
}

fn fail_deliveries(batches: &mut [ReadyBatch], error: &ProducerError) {
    for batch in batches {
        let Some(sender) = batch.delivery.take() else {
            continue;
        };
        sender.send_error(error);
    }
}

/// The `(topic, partition, base_sequence)` of each idempotent batch in a dispatch,
/// used to register/remove them from the per-partition in-flight set. Batches
/// without an assigned `producer_state` (non-idempotent) contribute nothing.
fn idempotent_inflight_of(batches: &[ReadyBatch]) -> Vec<(String, i32, i32)> {
    batches
        .iter()
        .filter_map(|batch| {
            batch
                .producer_state
                .map(|state| (batch.topic.clone(), batch.partition, state.base_sequence))
        })
        .collect()
}

fn terminal_error_delivered_to_futures(
    batches: &mut [ReadyBatch],
    outcome: DispatchOutcome,
) -> DispatchOutcome {
    if let DispatchOutcome::Delivered(Err(error)) = &outcome {
        fail_deliveries(batches, error);
    }
    outcome
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

/// Kafka 4.3 closes `InitProducerId` v6 when two-phase commit is disabled, so
/// non-2PC producers cap negotiation at v5 until the broker-side behavior changes.
const NON_2PC_INIT_PRODUCER_ID_MAX_VERSION: i16 = 5;
/// Lowest coordinator `InitProducerId` version that supports bumping the
/// producer epoch from the client (Kafka `coordinatorSupportsBumpingEpoch`).
const COORDINATOR_EPOCH_BUMP_MIN_INIT_PRODUCER_ID_VERSION: i16 = 3;
/// Kafka `ADD_PARTITIONS_RETRY_BACKOFF_MS`: shortened backoff for the first
/// `AddPartitionsToTxn` retrying on `CONCURRENT_TRANSACTIONS`, so a new
/// transaction does not wait long for the previous one to finish completing.
const ADD_PARTITIONS_RETRY_BACKOFF: Duration = Duration::from_millis(20);
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
mod tests;
