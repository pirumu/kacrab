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

/// Dispatcher-only fallback for tests/manual construction. Public producer
/// configs still default to `acks=all`; this keeps `ProducerDispatcher::new`
/// compatible with earlier unit tests that modeled leader-only acknowledgements.
const DEFAULT_DISPATCHER_ACKS: i16 = 1;
const PENDING_TRANSACTION_OPERATION_MESSAGE: &str =
    "previous transaction operation is pending and must be retried";
const COMPRESSION_RATE_ESTIMATION_FACTOR: f32 = 1.05;

/// Serializes `ProduceRequest` enqueues in spawn order so idempotent producers can keep
/// multiple in-flight requests per partition without reordering record-batch sequence numbers
/// on the wire. The sender assigns each dispatch a monotonically increasing ticket (in
/// single-threaded drain/sequence order). A dispatch waits for its ticket's turn before
/// enqueuing, then advances the turn once its requests are enqueued — so the broker observes
/// ascending base sequences per partition even though the response waits run concurrently.
/// This replaces Kafka's single Sender thread, which is the in-order enqueuer by construction.
struct EnqueueSequencer {
    next_ticket: AtomicU64,
    serving: AtomicU64,
    notify: Notify,
}

impl EnqueueSequencer {
    fn new() -> Self {
        Self {
            next_ticket: AtomicU64::new(0),
            serving: AtomicU64::new(0),
            notify: Notify::new(),
        }
    }

    /// Reserve the next enqueue ticket. MUST be called from the single-threaded sender loop
    /// (or any sequential flush path) so tickets are handed out in drain/sequence order.
    fn reserve_ticket(&self) -> u64 {
        self.next_ticket.fetch_add(1, Ordering::Relaxed)
    }

    /// Wait until it is `ticket`'s turn to enqueue. Returns immediately for a ticket whose
    /// turn has already passed (an in-task retry reusing its ticket), so retries never block.
    async fn wait_turn(&self, ticket: u64) {
        loop {
            let notified = self.notify.notified();
            if self.serving.load(Ordering::Acquire) >= ticket {
                return;
            }
            notified.await;
        }
    }

    /// Advance the turn past `ticket`. Idempotent (monotonic) so an in-task retry reusing its
    /// ticket cannot rewind the turn.
    fn advance_past(&self, ticket: u64) {
        let _previous = self
            .serving
            .fetch_max(ticket.saturating_add(1), Ordering::AcqRel);
        self.notify.notify_waiters();
    }
}

impl std::fmt::Debug for EnqueueSequencer {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EnqueueSequencer")
            .field("next_ticket", &self.next_ticket.load(Ordering::Relaxed))
            .field("serving", &self.serving.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

/// RAII guard that advances the enqueue turn exactly once — explicitly after a dispatch's
/// requests are enqueued (to release the turn before the concurrent response waits) and, as a
/// safety net, on drop if a dispatch returns early before reaching the explicit advance.
struct EnqueueTurn<'a> {
    sequencer: &'a EnqueueSequencer,
    ticket: u64,
    advanced: bool,
}

impl EnqueueTurn<'_> {
    fn advance(&mut self) {
        if !self.advanced {
            self.sequencer.advance_past(self.ticket);
            self.advanced = true;
        }
    }
}

impl Drop for EnqueueTurn<'_> {
    fn drop(&mut self) {
        self.advance();
    }
}

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
                self.compression_ratio_estimation(record.topic.as_ref()),
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
        record: &mut super::ProducerRecord,
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

#[derive(Debug, Default)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "Mirrors Kafka's TransactionManager flat transaction/idempotence state flags."
)]
struct ProducerIdempotenceState {
    identity: Option<ProducerIdentity>,
    /// Per-partition idempotent bookkeeping (Kafka `TxnPartitionMap` /
    /// `TxnPartitionEntry`): next sequence, last-acked sequence and offset, and
    /// any unresolved-sequence marker.
    partitions: AHashMap<TopicPartitionKey, IdempotentPartitionEntry>,
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
    /// Set once the transaction coordinator is observed to advertise an
    /// `InitProducerId` version below v3, meaning it cannot bump the producer
    /// epoch. Kafka's `coordinatorSupportsBumpingEpoch` (the inverse) gates
    /// whether an abortable error that needs an epoch bump can be recovered or
    /// must escalate to a fatal error. Defaults to `false` (assume supported),
    /// matching modern brokers; flipped only when an old coordinator is seen.
    coordinator_lacks_epoch_bump_support: bool,
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
    compression_ratio: f32,
}

#[derive(Debug, Clone, Copy)]
struct PartitionLoadRefresh<'a> {
    topic: &'a str,
    topic_metadata: &'a TopicMetadata,
    accumulator: &'a SharedAccumulator,
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
        compression_ratio: f32,
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
            compression_ratio,
        )
    }

    #[cfg(test)]
    #[expect(
        clippy::too_many_arguments,
        reason = "Test-only entrypoint pins ratio-aware sticky sizing without constructing a \
                  dispatcher."
    )]
    fn partition_for_record_with_compression_ratio(
        &mut self,
        metadata: &crate::wire::ClusterMetadata,
        record: &super::ProducerRecord,
        ignore_keys: bool,
        adaptive: bool,
        sticky_batch_size: usize,
        compression_ratio: f32,
    ) -> Result<i32> {
        self.partition_for_record(
            metadata,
            record,
            ignore_keys,
            adaptive,
            sticky_batch_size,
            compression_ratio,
        )
    }

    fn try_assign_cached_sticky_partition(
        &mut self,
        record: &mut super::ProducerRecord,
        sticky_batch_size: usize,
        compression_ratio: f32,
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
        let record_bytes = estimate_sticky_record_bytes(record, compression_ratio);
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
            compression_ratio,
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
                .saturating_add(estimate_sticky_record_bytes(record, compression_ratio));
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
            compression_ratio,
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
                .saturating_add(estimate_sticky_record_bytes(record, compression_ratio));
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
        compression_ratio: f32,
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
            .saturating_add(estimate_sticky_record_bytes(record, compression_ratio));
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
        accumulator: &SharedAccumulator,
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

#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    reason = "Kafka compression ratio estimates are f32 and only influence sticky byte budgets."
)]
fn estimate_sticky_record_bytes(record: &super::ProducerRecord, compression_ratio: f32) -> usize {
    let ratio = if compression_ratio.is_finite() && compression_ratio > 0.0 {
        compression_ratio
    } else {
        1.0
    };
    ((estimate_record_batch_bytes(record) as f32) * ratio * COMPRESSION_RATE_ESTIMATION_FACTOR)
        .ceil() as usize
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
        self.partitions.clear();
        self.epoch_bump_required = false;
    }

    /// Kafka `requestIdempotentEpochBumpForPartition`: flag that the producer epoch
    /// must be bumped before the next produce (applied in `producer_batch_state`),
    /// healing a sequence gap left by a terminally failed batch.
    const fn request_epoch_bump(&mut self) {
        self.epoch_bump_required = true;
    }

    fn next_sequence(&mut self, topic: &str, partition: i32, record_count: i32) -> Result<i32> {
        let key = TopicPartitionKey {
            topic: topic.to_owned(),
            partition,
        };
        let entry = self.partitions.entry(key).or_default();
        if entry.unresolved_next_sequence.is_some() {
            return Err(ProducerError::UnresolvedSequence {
                topic: topic.to_owned(),
                partition,
            });
        }
        let base_sequence = entry.next_sequence;
        entry.next_sequence = increment_sequence(base_sequence, record_count);
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
        let next_sequence = increment_sequence(base_sequence, record_count);
        let entry = self.partitions.entry(key).or_default();
        entry.unresolved_next_sequence = Some(
            entry
                .unresolved_next_sequence
                .map_or(next_sequence, |existing| existing.max(next_sequence)),
        );
    }

    fn reset_sequence(&mut self, topic: &str, partition: i32) {
        let key = TopicPartitionKey {
            topic: topic.to_owned(),
            partition,
        };
        // Kafka startSequencesAtBeginning: sequence restarts at 0 with no acks.
        let entry = self.partitions.entry(key).or_default();
        entry.unresolved_next_sequence = None;
        entry.unresolved_loss_ambiguous = false;
        entry.next_sequence = 0;
        entry.last_acked_sequence = NO_LAST_ACKED_SEQUENCE;
        entry.last_acked_offset = INVALID_LAST_ACKED_OFFSET;
    }

    fn release_sequence(&mut self, topic: &str, partition: i32, base_sequence: i32) {
        let key = TopicPartitionKey {
            topic: topic.to_owned(),
            partition,
        };
        if let Some(entry) = self.partitions.get_mut(&key)
            && entry
                .unresolved_next_sequence
                .is_some_and(|sequence| sequence <= base_sequence)
        {
            entry.unresolved_next_sequence = None;
        }
    }

    /// Kafka `TxnPartitionEntry::addInflightBatch`: track a dispatched batch's base
    /// sequence as in flight for its partition. Re-adding a retried batch's
    /// sequence is a no-op (the set already holds it).
    fn register_inflight_sequence(&mut self, topic: &str, partition: i32, base_sequence: i32) {
        let key = TopicPartitionKey {
            topic: topic.to_owned(),
            partition,
        };
        let _inserted = self
            .partitions
            .entry(key)
            .or_default()
            .inflight_by_sequence
            .insert(base_sequence);
    }

    /// Kafka `TxnPartitionEntry::removeInFlightBatch`: drop a base sequence once its
    /// batch terminally completes (NOT on requeue).
    fn remove_inflight_sequence(&mut self, topic: &str, partition: i32, base_sequence: i32) {
        let key = TopicPartitionKey {
            topic: topic.to_owned(),
            partition,
        };
        if let Some(entry) = self.partitions.get_mut(&key) {
            let _removed = entry.inflight_by_sequence.remove(&base_sequence);
        }
    }

    /// Kafka `TransactionManager::hasInflightBatches`: whether the partition still
    /// has any batch dispatched-but-not-terminally-completed. Gates
    /// `resolve_unresolved_sequence_after_drain`.
    fn has_inflight_batches(&self, topic: &str, partition: i32) -> bool {
        let key = TopicPartitionKey {
            topic: topic.to_owned(),
            partition,
        };
        self.partitions
            .get(&key)
            .is_some_and(|entry| !entry.inflight_by_sequence.is_empty())
    }

    /// Whether a batch's stamped producer id/epoch is stale relative to the current
    /// producer identity (Kafka `hasStaleProducerIdAndEpoch`). True after an epoch
    /// bump for any batch still carrying the old epoch — such a batch must be
    /// re-stamped (fresh sequence under the new epoch) before it is sent, which is
    /// kacrab's equivalent of Kafka `startSequencesAtBeginning` renumbering an
    /// in-flight batch in place.
    fn is_stale_identity(&self, identity: ProducerIdentity) -> bool {
        self.identity.is_some_and(|current| current != identity)
    }

    /// Kafka `firstInFlightSequence`: the lowest in-flight base sequence for a
    /// partition, or `None` when nothing is in flight. The drain gate defers a
    /// retried batch whose base sequence is not this value, so retries re-send
    /// strictly in sequence order.
    fn first_inflight_sequence(&self, topic: &str, partition: i32) -> Option<i32> {
        let key = TopicPartitionKey {
            topic: topic.to_owned(),
            partition,
        };
        self.partitions
            .get(&key)
            .and_then(|entry| entry.inflight_by_sequence.iter().next().copied())
    }

    /// Record whether the loss that left a partition unresolved was ambiguous, so a
    /// deferred resolve (run later, once the partition has drained) bumps the epoch
    /// only for ambiguous losses. Sticky across multiple contributing losses.
    fn record_unresolved_loss_ambiguity(&mut self, topic: &str, partition: i32, ambiguous: bool) {
        let key = TopicPartitionKey {
            topic: topic.to_owned(),
            partition,
        };
        let entry = self.partitions.entry(key).or_default();
        entry.unresolved_loss_ambiguous = entry.unresolved_loss_ambiguous || ambiguous;
    }

    fn unresolved_loss_ambiguous(&self, topic: &str, partition: i32) -> bool {
        let key = TopicPartitionKey {
            topic: topic.to_owned(),
            partition,
        };
        self.partitions
            .get(&key)
            .is_some_and(|entry| entry.unresolved_loss_ambiguous)
    }

    /// Kafka `maybeResolveSequences`: once a partition's in-flight batches have
    /// drained with its sequence still unresolved, either confirm it was fully
    /// acknowledged (drop the marker) or recover. When unacked messages remain,
    /// a transactional producer transitions to an abortable error (or fatal when
    /// the coordinator cannot bump the epoch) and an idempotent producer requests
    /// an epoch bump.
    fn resolve_unresolved_sequence_after_drain(
        &mut self,
        topic: &str,
        partition: i32,
        transactional: bool,
        loss_is_ambiguous: bool,
    ) {
        // Kafka `maybeResolveSequences` only resolves a partition once it has NO
        // in-flight batches. With multiple in-flight requests per partition, an
        // ambiguous timeout on one batch must NOT bump the epoch while later
        // batches are still in flight under the current epoch — defer until the
        // partition drains. The deferred resolve is retriggered from
        // `release_idempotent_inflight_after_terminal` as each request completes.
        if self.has_inflight_batches(topic, partition) {
            return;
        }
        let key = TopicPartitionKey {
            topic: topic.to_owned(),
            partition,
        };
        let (has_unresolved, resolved) = match self.partitions.get(&key) {
            Some(entry) => (
                entry.unresolved_next_sequence.is_some(),
                // isNextSequence: nextSequence - lastAcked == 1 => fully acked.
                // Kafka uses wrapping int subtraction so it stays correct across the
                // i32::MAX sequence wraparound.
                entry.last_acked_sequence != NO_LAST_ACKED_SEQUENCE
                    && entry.next_sequence.wrapping_sub(entry.last_acked_sequence) == 1,
            ),
            None => return,
        };
        if !has_unresolved {
            return;
        }
        if !resolved {
            if transactional {
                if self.in_transaction || self.transaction_state.is_completing() {
                    if self.coordinator_lacks_epoch_bump_support {
                        self.transaction_state = TransactionState::FatalError;
                    } else {
                        self.transaction_state = TransactionState::AbortableError;
                        self.epoch_bump_required = true;
                    }
                }
            } else if loss_is_ambiguous {
                // An idempotent (non-transactional) batch whose final failure was a
                // no-response/connection loss MIGHT have been written by the broker,
                // so the per-producer sequence state is now ambiguous and the epoch
                // must be bumped before the next produce. A definitive rejection
                // (e.g. NotLeaderOrFollower) means the records were never written, so
                // the sequence can simply be released/rewound without an epoch bump.
                self.epoch_bump_required = true;
            }
        }
        if let Some(entry) = self.partitions.get_mut(&key) {
            entry.unresolved_next_sequence = None;
            entry.unresolved_loss_ambiguous = false;
        }
    }

    /// Kafka `TxnPartitionEntry::maybeUpdateLastAckedSequence`: record the highest
    /// acknowledged sequence for a partition.
    fn maybe_update_last_acked_sequence(&mut self, topic: &str, partition: i32, sequence: i32) {
        let key = TopicPartitionKey {
            topic: topic.to_owned(),
            partition,
        };
        let entry = self.partitions.entry(key).or_default();
        if sequence > entry.last_acked_sequence {
            entry.last_acked_sequence = sequence;
        }
    }

    /// Kafka `TxnPartitionMap::updateLastAckedOffset`: record the highest
    /// acknowledged base offset, used to disambiguate `UnknownProducerId`.
    fn update_last_acked_offset(&mut self, topic: &str, partition: i32, last_offset: i64) {
        if last_offset == INVALID_LAST_ACKED_OFFSET {
            return;
        }
        let key = TopicPartitionKey {
            topic: topic.to_owned(),
            partition,
        };
        let entry = self.partitions.entry(key).or_default();
        if last_offset > entry.last_acked_offset {
            entry.last_acked_offset = last_offset;
        }
    }

    fn last_acked_offset(&self, topic: &str, partition: i32) -> Option<i64> {
        let key = TopicPartitionKey {
            topic: topic.to_owned(),
            partition,
        };
        self.partitions
            .get(&key)
            .map(|entry| entry.last_acked_offset)
            .filter(|offset| *offset != INVALID_LAST_ACKED_OFFSET)
    }

    fn should_reset_sequence_for_idempotent_retry(
        &self,
        decision: IdempotentRetryDecision<'_>,
    ) -> bool {
        if matches!(decision.error, ErrorCode::UnknownProducerId) {
            // Retention elapsed (lastAckedOffset < logStartOffset) is recoverable
            // by resetting; genuine data loss is not. Without a last-acked offset
            // we fall back to Kafka's logStartOffset != -1 heuristic.
            if let Some(last_acked_offset) =
                self.last_acked_offset(decision.topic, decision.partition)
                && decision.log_start_offset != -1
            {
                return last_acked_offset < decision.log_start_offset;
            }
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
        self.partitions
            .get(&key)
            .and_then(|entry| entry.unresolved_next_sequence)
            .is_none_or(|unresolved_next_sequence| base_sequence == unresolved_next_sequence)
    }

    fn rewind_sequence_to(&mut self, topic: &str, partition: i32, base_sequence: i32) {
        let key = TopicPartitionKey {
            topic: topic.to_owned(),
            partition,
        };
        let entry = self
            .partitions
            .entry(key)
            .or_insert_with(|| IdempotentPartitionEntry {
                next_sequence: base_sequence,
                ..IdempotentPartitionEntry::default()
            });
        entry.unresolved_next_sequence = None;
        if entry.next_sequence >= base_sequence {
            entry.next_sequence = base_sequence;
        }
    }
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

/// Per-partition idempotent bookkeeping, mirroring Kafka `TxnPartitionEntry`.
#[derive(Debug, Clone)]
struct IdempotentPartitionEntry {
    /// Base sequence of the next batch bound for this partition.
    next_sequence: i32,
    /// Sequence of the last record of the last acknowledged batch, or
    /// [`NO_LAST_ACKED_SEQUENCE`] when nothing has been acknowledged.
    last_acked_sequence: i32,
    /// Last acknowledged base offset, or [`INVALID_LAST_ACKED_OFFSET`].
    last_acked_offset: i64,
    /// When set, new sends to this partition block until the marked sequence is
    /// resolved (Kafka `partitionsWithUnresolvedSequences`).
    unresolved_next_sequence: Option<i32>,
    /// Whether the loss that marked this partition unresolved was ambiguous (a
    /// no-response timeout that MIGHT have been written), recorded so the deferred
    /// resolve (run once the partition has no in-flight batches) bumps the epoch
    /// only for ambiguous losses. Kafka carries this via the batch's last error.
    unresolved_loss_ambiguous: bool,
    /// Base sequences of this partition's batches that have been dispatched at
    /// least once and not yet terminally completed (Kafka
    /// `TxnPartitionEntry::inflightBatchesBySequence`). A sequence is added when
    /// its batch is dispatched and removed only on terminal completion (NOT on
    /// requeue), so `has_inflight_batches` gates `maybeResolveSequences`: an
    /// unresolved sequence is only resolved (and the epoch bumped) once every
    /// in-flight batch for the partition has drained — otherwise an ambiguous
    /// timeout on one batch could bump the epoch while later batches are still in
    /// flight under the old epoch.
    inflight_by_sequence: BTreeSet<i32>,
}

impl Default for IdempotentPartitionEntry {
    fn default() -> Self {
        Self {
            next_sequence: 0,
            last_acked_sequence: NO_LAST_ACKED_SEQUENCE,
            last_acked_offset: INVALID_LAST_ACKED_OFFSET,
            unresolved_next_sequence: None,
            unresolved_loss_ambiguous: false,
            inflight_by_sequence: BTreeSet::new(),
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
mod tests {
    #![allow(
        clippy::expect_used,
        clippy::field_reassign_with_default,
        clippy::missing_assert_message,
        clippy::significant_drop_tightening,
        clippy::unwrap_used,
        reason = "Unit test fixtures fail fastest with contextual unwrap/expect and direct state \
                  setup."
    )]

    use std::{
        collections::VecDeque,
        net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
        sync::{
            Arc,
            atomic::{AtomicI64, Ordering},
        },
        time::{Duration, Instant},
    };

    use ahash::AHashSet;
    use bytes::Bytes;
    use kacrab_protocol::{
        KafkaString, KafkaUuid,
        compression::Compression,
        generated::{
            AddPartitionsToTxnPartitionResult, AddPartitionsToTxnResponseData,
            AddPartitionsToTxnResult, AddPartitionsToTxnTopicResult, ApiKey, ErrorCode,
            ProduceRequestData,
        },
        version::client_api_info,
    };

    use super::{
        BrokerProduceOptions, BrokerProduceRequest, BrokerRequestPlacement, DispatchError,
        IdempotentRetryDecision, PartitionLoadRefresh, PendingTransactionOperationGuard,
        ProduceRequestSizing, ProducerDispatcher, ProducerIdempotenceState,
        ProducerPartitionerState, RECORD_BATCH_OVERHEAD_BYTES, RecordBufferRelease,
        SharedAccumulator, TopicPartitionKey, TransactionOperation,
        TransactionPendingOperationStart, broker_dispatch_completed_result,
        broker_request_placement_for_batch, build_partition_load_stats, choose_coordinator_addr,
        complete_deliveries, end_txn_version, estimate_record_batch_bytes,
        estimate_sticky_record_bytes, fail_pending_transaction_operation, init_producer_id_version,
        is_fatal_transaction_error, is_leadership_error, no_ack_receipts,
        pop_dispatchable_broker_request, produce_version, txn_offset_commit_version,
        uniform_partition_for_random, unique_topics, unique_unassigned_record_topics,
        validate_consumer_group_metadata,
    };
    use crate::{
        producer::{
            AccumulatorConfig, ConsumerGroupMetadata, ProducerCompression, ProducerError,
            ProducerIdempotenceConfig, ProducerIdentity, ProducerRecord, ProducerRuntimeConfig,
            RecordMetadata, compression_ratio::CompressionRatioEstimator,
        },
        wire::{
            BrokerEndpoint, BrokerMetadata, ClusterMetadata, ConnectionConfig, PartitionMetadata,
            TopicMetadata, WireClient,
        },
    };

    const TEST_LARGE_BATCH_SIZE: usize = 16 * 1024;

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
    fn dispatcher_retry_delay_doubles_to_runtime_max() {
        let dispatcher = ProducerDispatcher::with_config(
            test_wire(),
            ProducerRuntimeConfig {
                retry_backoff: Duration::from_millis(25),
                retry_backoff_max: Duration::from_millis(80),
                ..ProducerRuntimeConfig::default()
            },
        );
        let mut retry = dispatcher.retry_backoff_state();

        assert_eq!(retry.next_delay_with_sample(0.5), Duration::from_millis(25));
        assert_eq!(retry.next_delay_with_sample(0.5), Duration::from_millis(50));
        assert_eq!(retry.next_delay_with_sample(0.5), Duration::from_millis(80));
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
            pop_dispatchable_broker_request(&mut pending, &in_flight_routes, true)
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
            pop_dispatchable_broker_request(&mut pending, &in_flight_routes, true)
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
                    .partition_for_record(&metadata, &record, false, true, 128, 1.0)
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
            .partition_for_record(&metadata, &record, false, true, 128, 1.0)
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
                    .partition_for_record(&metadata, &record, false, true, sticky_batch_size, 1.0)
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
    fn sticky_partitioner_uses_compression_ratio_estimate_for_batch_budget() {
        let metadata = metadata_with_partitions("orders", 3);
        let record = ProducerRecord::unassigned("orders").value(Bytes::from(vec![b'x'; 128]));
        let sticky_batch_size =
            RECORD_BATCH_OVERHEAD_BYTES + estimate_record_batch_bytes(&record) / 2;

        let mut uncompressed = ProducerPartitionerState::default();
        let uncompressed_partitions: Vec<_> = (0..4)
            .map(|_| {
                uncompressed
                    .partition_for_record_with_compression_ratio(
                        &metadata,
                        &record,
                        false,
                        false,
                        sticky_batch_size,
                        1.0,
                    )
                    .expect("partition")
            })
            .collect();

        let mut compressed = ProducerPartitionerState::default();
        let compressed_partitions: Vec<_> = (0..4)
            .map(|_| {
                compressed
                    .partition_for_record_with_compression_ratio(
                        &metadata,
                        &record,
                        false,
                        false,
                        sticky_batch_size,
                        0.25,
                    )
                    .expect("partition")
            })
            .collect();

        assert_ne!(
            uncompressed_partitions[2], uncompressed_partitions[0],
            "raw sizing should exhaust the sticky batch before the third record"
        );
        assert!(
            compressed_partitions
                .iter()
                .all(|partition| *partition == compressed_partitions[0]),
            "compressed sizing should keep the sticky partition longer"
        );
    }

    #[test]
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_precision_loss,
        clippy::cast_sign_loss,
        reason = "Test mirrors Kafka's f32 compression estimate formula exactly."
    )]
    fn sticky_record_estimate_uses_java_compression_safety_factor() {
        let record = ProducerRecord::unassigned("orders").value(Bytes::from(vec![b'x'; 128]));
        let raw = estimate_record_batch_bytes(&record);
        let estimated = estimate_sticky_record_bytes(&record, 0.50);

        assert_eq!(estimated, ((raw as f32) * 0.50 * 1.05).ceil() as usize);
    }

    #[test]
    fn cached_sticky_partition_assigns_without_metadata_until_batch_ready() {
        let metadata = metadata_with_partitions("orders", 3);
        let mut state = ProducerPartitionerState::default();
        let record = ProducerRecord::unassigned("orders").value(Bytes::from_static(b"1234567890"));
        let sticky_batch_size = 1024;
        let partition = state
            .partition_for_record(&metadata, &record, false, true, sticky_batch_size, 1.0)
            .expect("initial sticky partition");

        let mut cached_record =
            ProducerRecord::unassigned("orders").value(Bytes::from_static(b"cached"));
        assert!(state.try_assign_cached_sticky_partition(
            &mut cached_record,
            sticky_batch_size,
            1.0
        ));
        assert_eq!(cached_record.partition, partition);

        state.mark_sticky_batch_ready("orders", 1);
        let mut next_record =
            ProducerRecord::unassigned("orders").value(Bytes::from_static(b"next"));
        assert!(!state.try_assign_cached_sticky_partition(
            &mut next_record,
            sticky_batch_size,
            1.0
        ));
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
                    .partition_for_record(&metadata, &record, false, true, 256, 1.0)
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
                    .partition_for_record(&metadata, &record, true, true, 128, 1.0)
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
                    .partition_for_record(&metadata, &record, true, true, 128, 1.0)
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
                    .partition_for_record(&metadata, &record, true, true, 1, 1.0)
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
        let accumulator =
            SharedAccumulator::with_config(AccumulatorConfig::default().batch_size(1));
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
        let accumulator = SharedAccumulator::with_config(
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
        let accumulator =
            SharedAccumulator::with_config(AccumulatorConfig::default().batch_size(1024));
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
        let accumulator =
            SharedAccumulator::with_config(AccumulatorConfig::default().batch_size(1024));
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

    #[test]
    fn compression_ratio_estimator_updates_and_resets_like_java() {
        let mut estimator = CompressionRatioEstimator::default();

        assert_ratio_close(estimator.estimation("orders", Compression::Lz4), 1.0);
        assert_ratio_close(
            estimator.update_estimation("orders", Compression::Lz4, 0.60),
            0.995,
        );
        assert_ratio_close(
            estimator.update_estimation("orders", Compression::Lz4, 1.30),
            1.30,
        );
        assert_ratio_close(
            estimator.update_estimation("orders", Compression::Lz4, 1.20),
            1.295,
        );

        estimator.reset_after_split("orders", Compression::Lz4, 0.42);
        assert_ratio_close(estimator.estimation("orders", Compression::Lz4), 1.0);

        estimator.reset_after_split("orders", Compression::Lz4, 1.25);
        assert_ratio_close(estimator.estimation("orders", Compression::Lz4), 1.25);
    }

    #[tokio::test]
    async fn message_too_large_split_resets_compression_ratio_estimation() {
        let dispatcher = ProducerDispatcher::with_config(
            test_wire(),
            ProducerRuntimeConfig {
                compression: ProducerCompression {
                    codec: Compression::None,
                    level: None,
                },
                ..ProducerRuntimeConfig::default()
            },
        );
        dispatcher.set_compression_ratio_estimation_for_test("orders", Compression::None, 0.40);
        let batches = vec![ready_batch("orders", 0)];

        let outcome = dispatcher
            .message_too_large_split_outcome(batches, "orders".to_owned(), 0)
            .await;

        assert!(matches!(
            outcome,
            super::DispatchOutcome::Delivered(Err(ProducerError::Broker {
                topic,
                partition: 0,
                error: ErrorCode::MessageTooLarge,
            })) if topic == "orders"
        ));
        assert_ratio_close(
            dispatcher.compression_ratio_estimation_for_test("orders", Compression::None),
            1.0,
        );
    }

    #[test]
    fn message_too_large_split_uses_compression_ratio_estimation_for_split_groups() {
        let now = Instant::now();
        let accumulator = SharedAccumulator::with_config(
            AccumulatorConfig::default()
                .batch_size(TEST_LARGE_BATCH_SIZE)
                .buffer_memory(TEST_LARGE_BATCH_SIZE * 4)
                .linger(Duration::from_secs(1)),
        );
        for value in [b"a".as_slice(), b"b".as_slice(), b"c".as_slice()] {
            accumulator
                .append_at(
                    ProducerRecord::new("orders", 0).value(Bytes::copy_from_slice(value)),
                    now,
                )
                .expect("append record");
        }
        let batches = accumulator.drain_all();

        let split = super::split_message_too_large_batches(batches, "orders", 0, 78, 2.0)
            .expect("multi-record batch should split");

        assert_eq!(split.len(), 3);
        assert!(split.iter().all(|batch| batch.records.len() == 1));
    }

    fn assert_ratio_close(actual: f32, expected: f32) {
        assert!(
            (actual - expected).abs() <= 0.000_001,
            "expected {expected}, got {actual}"
        );
    }

    #[test]
    fn dispatcher_record_batch_encoding_uses_wire_write_buffer_pool() {
        let wire = WireClient::connect_with_brokers(
            ConnectionConfig::default().buffer_pool_capacity(1),
            "dispatcher-test",
            [BrokerEndpoint::new(
                7,
                "127.0.0.1:9092".parse().expect("valid socket address"),
            )],
        );
        let dispatcher = ProducerDispatcher::new(wire.clone());
        let batch = ready_batch("orders", 0);

        let encoded = dispatcher
            .encode_ready_batch_records(&batch, 0)
            .expect("ready batch should encode");

        assert!(!encoded.is_empty());
        let stats = wire.buffer_pool_stats();
        assert_eq!(stats.write_acquired, 1);
        assert_eq!(stats.write_released, 1);
    }

    #[test]
    fn broker_produce_request_releases_unique_record_bytes_to_wire_pool() {
        let wire = WireClient::connect_with_brokers(
            ConnectionConfig::default().buffer_pool_capacity(2),
            "dispatcher-test",
            [BrokerEndpoint::new(
                7,
                "127.0.0.1:9092".parse().expect("valid socket address"),
            )],
        );
        let dispatcher = ProducerDispatcher::new(wire.clone());
        let records = dispatcher
            .encode_ready_batch_records_owned(&ready_batch("orders", 0), 0)
            .expect("ready batch should encode");
        let mut request = BrokerProduceRequest::with_record_buffer_owner(&wire);
        request.push(
            route("orders", 0),
            records,
            BrokerProduceOptions {
                acks: 1,
                timeout_ms: 1_500,
                transactional_id: None,
            },
            1,
        );
        let before_release = wire.buffer_pool_stats();

        assert_eq!(request.release_record_buffers(&wire).released, 1);

        let after_release = wire.buffer_pool_stats();
        assert_eq!(
            after_release.write_released,
            before_release.write_released + 1
        );
        assert!(
            request.data.topic_data[0].partition_data[0]
                .records
                .is_none()
        );
    }

    #[test]
    fn owned_broker_produce_request_drop_releases_unique_record_bytes_to_wire_pool() {
        let wire = WireClient::connect_with_brokers(
            ConnectionConfig::default().buffer_pool_capacity(2),
            "dispatcher-test",
            [BrokerEndpoint::new(
                7,
                "127.0.0.1:9092".parse().expect("valid socket address"),
            )],
        );
        let dispatcher = ProducerDispatcher::new(wire.clone());
        let records = dispatcher
            .encode_ready_batch_records_owned(&ready_batch("orders", 0), 0)
            .expect("ready batch should encode");
        let before_drop = wire.buffer_pool_stats();

        {
            let mut request = BrokerProduceRequest::with_record_buffer_owner(&wire);
            request.push(
                route("orders", 0),
                records,
                BrokerProduceOptions {
                    acks: 1,
                    timeout_ms: 1_500,
                    transactional_id: None,
                },
                1,
            );
        }

        let after_drop = wire.buffer_pool_stats();
        assert_eq!(after_drop.write_released, before_drop.write_released + 1);
    }

    #[test]
    fn owned_record_buffer_releases_when_last_request_clone_drops() {
        let wire = WireClient::connect_with_brokers(
            ConnectionConfig::default().buffer_pool_capacity(2),
            "dispatcher-test",
            [BrokerEndpoint::new(
                7,
                "127.0.0.1:9092".parse().expect("valid socket address"),
            )],
        );
        let dispatcher = ProducerDispatcher::new(wire.clone());
        let records = dispatcher
            .encode_ready_batch_records_owned(&ready_batch("orders", 0), 0)
            .expect("ready batch should encode");
        let cloned_records = {
            let mut request = BrokerProduceRequest::with_record_buffer_owner(&wire);
            request.push(
                route("orders", 0),
                records,
                BrokerProduceOptions {
                    acks: 1,
                    timeout_ms: 1_500,
                    transactional_id: None,
                },
                1,
            );
            let records = request.data.topic_data[0].partition_data[0]
                .records
                .as_ref()
                .expect("records should be present")
                .clone();
            let before_drop = wire.buffer_pool_stats();

            drop(request);

            let after_request_drop = wire.buffer_pool_stats();
            assert_eq!(
                after_request_drop.write_released,
                before_drop.write_released
            );
            records
        };

        let before_clone_drop = wire.buffer_pool_stats();
        drop(cloned_records);

        let after_clone_drop = wire.buffer_pool_stats();
        assert_eq!(
            after_clone_drop.write_released,
            before_clone_drop.write_released + 1
        );
    }

    #[test]
    fn broker_produce_request_reports_unrecovered_record_buffer_when_clone_is_alive() {
        let wire = WireClient::connect_with_brokers(
            ConnectionConfig::default().buffer_pool_capacity(2),
            "dispatcher-test",
            [BrokerEndpoint::new(
                7,
                "127.0.0.1:9092".parse().expect("valid socket address"),
            )],
        );
        let dispatcher = ProducerDispatcher::new(wire.clone());
        let records = dispatcher
            .encode_ready_batch_records(&ready_batch("orders", 0), 0)
            .expect("ready batch should encode");
        let encoded_bytes = records.len();
        let mut request = BrokerProduceRequest::default();
        request.push(
            route("orders", 0),
            records,
            BrokerProduceOptions {
                acks: 1,
                timeout_ms: 1_500,
                transactional_id: None,
            },
            1,
        );
        let cloned_request_data = request.data.clone();
        let before_release = wire.buffer_pool_stats();

        let release = request.release_record_buffers(&wire);

        assert_eq!(
            release,
            RecordBufferRelease {
                expected: 1,
                released: 0,
                unrecovered: 1,
                expected_bytes: encoded_bytes,
                released_bytes: 0,
                unrecovered_bytes: encoded_bytes,
            }
        );
        let after_first_release = wire.buffer_pool_stats();
        assert_eq!(
            after_first_release.write_released,
            before_release.write_released
        );
        assert!(
            request.data.topic_data[0].partition_data[0]
                .records
                .is_some()
        );

        drop(cloned_request_data);
        let release = request.release_record_buffers(&wire);

        assert_eq!(
            release,
            RecordBufferRelease {
                expected: 1,
                released: 1,
                unrecovered: 0,
                expected_bytes: encoded_bytes,
                released_bytes: encoded_bytes,
                unrecovered_bytes: 0,
            }
        );
        let after_second_release = wire.buffer_pool_stats();
        assert_eq!(
            after_second_release.write_released,
            after_first_release.write_released + 1
        );
        assert!(
            request.data.topic_data[0].partition_data[0]
                .records
                .is_none()
        );
    }

    #[test]
    fn broker_produce_request_reports_unrecovered_lease_when_records_are_missing() {
        let wire = WireClient::connect_with_brokers(
            ConnectionConfig::default().buffer_pool_capacity(2),
            "dispatcher-test",
            [BrokerEndpoint::new(
                7,
                "127.0.0.1:9092".parse().expect("valid socket address"),
            )],
        );
        let dispatcher = ProducerDispatcher::new(wire.clone());
        let records = dispatcher
            .encode_ready_batch_records(&ready_batch("orders", 0), 0)
            .expect("ready batch should encode");
        let encoded_bytes = records.len();
        let mut request = BrokerProduceRequest::default();
        request.push(
            route("orders", 0),
            records,
            BrokerProduceOptions {
                acks: 1,
                timeout_ms: 1_500,
                transactional_id: None,
            },
            1,
        );
        let _lost_records = request.data.topic_data[0].partition_data[0]
            .records
            .take()
            .expect("test should remove encoded records");
        let before_release = wire.buffer_pool_stats();

        let release = request.release_record_buffers(&wire);

        assert_eq!(
            release,
            RecordBufferRelease {
                expected: 1,
                released: 0,
                unrecovered: 1,
                expected_bytes: encoded_bytes,
                released_bytes: 0,
                unrecovered_bytes: encoded_bytes,
            }
        );
        let after_release = wire.buffer_pool_stats();
        assert_eq!(after_release.write_released, before_release.write_released);
    }

    #[test]
    fn broker_produce_request_reports_unrecovered_lease_bytes_when_records_are_missing() {
        let wire = WireClient::connect_with_brokers(
            ConnectionConfig::default().buffer_pool_capacity(2),
            "dispatcher-test",
            [BrokerEndpoint::new(
                7,
                "127.0.0.1:9092".parse().expect("valid socket address"),
            )],
        );
        let dispatcher = ProducerDispatcher::new(wire.clone());
        let records = dispatcher
            .encode_ready_batch_records(&ready_batch("orders", 0), 0)
            .expect("ready batch should encode");
        let encoded_bytes = records.len();
        let mut request = BrokerProduceRequest::default();
        request.push(
            route("orders", 0),
            records,
            BrokerProduceOptions {
                acks: 1,
                timeout_ms: 1_500,
                transactional_id: None,
            },
            1,
        );
        let _lost_records = request.data.topic_data[0].partition_data[0]
            .records
            .take()
            .expect("test should remove encoded records");

        let release = request.release_record_buffers(&wire);

        assert_eq!(
            release,
            RecordBufferRelease {
                expected: 1,
                released: 0,
                unrecovered: 1,
                expected_bytes: encoded_bytes,
                released_bytes: 0,
                unrecovered_bytes: encoded_bytes,
            }
        );
    }

    #[test]
    fn broker_produce_request_matches_partial_release_bytes_to_partition_lease() {
        let wire = WireClient::connect_with_brokers(
            ConnectionConfig::default().buffer_pool_capacity(2),
            "dispatcher-test",
            [BrokerEndpoint::new(
                7,
                "127.0.0.1:9092".parse().expect("valid socket address"),
            )],
        );
        let dispatcher = ProducerDispatcher::new(wire.clone());
        let first_records = dispatcher
            .encode_ready_batch_records(&ready_batch_with_value("orders", 0, b"a"), 0)
            .expect("first ready batch should encode");
        let first_bytes = first_records.len();
        let second_records = dispatcher
            .encode_ready_batch_records(
                &ready_batch_with_value("orders", 1, b"second-value-is-longer"),
                0,
            )
            .expect("second ready batch should encode");
        let second_bytes = second_records.len();
        assert_ne!(first_bytes, second_bytes);
        let mut request = BrokerProduceRequest::default();
        request.push(
            route("orders", 0),
            first_records,
            BrokerProduceOptions {
                acks: 1,
                timeout_ms: 1_500,
                transactional_id: None,
            },
            1,
        );
        request.push(
            route("orders", 1),
            second_records,
            BrokerProduceOptions {
                acks: 1,
                timeout_ms: 1_500,
                transactional_id: None,
            },
            1,
        );
        let _first_records_clone = request.data.topic_data[0].partition_data[0]
            .records
            .as_ref()
            .expect("first records")
            .clone();

        let release = request.release_record_buffers(&wire);

        assert_eq!(
            release,
            RecordBufferRelease {
                expected: 2,
                released: 1,
                unrecovered: 1,
                expected_bytes: first_bytes + second_bytes,
                released_bytes: second_bytes,
                unrecovered_bytes: first_bytes,
            }
        );
        assert!(
            request.data.topic_data[0].partition_data[0]
                .records
                .is_some()
        );
        assert!(
            request.data.topic_data[0].partition_data[1]
                .records
                .is_none()
        );
    }

    #[test]
    fn broker_produce_request_reports_all_leases_when_same_partition_records_are_merged() {
        let wire = WireClient::connect_with_brokers(
            ConnectionConfig::default().buffer_pool_capacity(2),
            "dispatcher-test",
            [BrokerEndpoint::new(
                7,
                "127.0.0.1:9092".parse().expect("valid socket address"),
            )],
        );
        let dispatcher = ProducerDispatcher::new(wire.clone());
        let first_records = dispatcher
            .encode_ready_batch_records(&ready_batch_with_value("orders", 0, b"a"), 0)
            .expect("first ready batch should encode");
        let first_bytes = first_records.len();
        let second_records = dispatcher
            .encode_ready_batch_records(
                &ready_batch_with_value("orders", 0, b"second-value-is-longer"),
                0,
            )
            .expect("second ready batch should encode");
        let second_bytes = second_records.len();
        assert_ne!(first_bytes, second_bytes);
        let mut request = BrokerProduceRequest::default();
        request.push(
            route("orders", 0),
            first_records,
            BrokerProduceOptions {
                acks: 1,
                timeout_ms: 1_500,
                transactional_id: None,
            },
            1,
        );
        request.push(
            route("orders", 0),
            second_records,
            BrokerProduceOptions {
                acks: 1,
                timeout_ms: 1_500,
                transactional_id: None,
            },
            1,
        );
        assert_eq!(request.data.topic_data[0].partition_data.len(), 1);
        let before_release = wire.buffer_pool_stats();

        let release = request.release_record_buffers(&wire);

        assert_eq!(
            release,
            RecordBufferRelease {
                expected: 2,
                released: 2,
                unrecovered: 0,
                expected_bytes: first_bytes + second_bytes,
                released_bytes: first_bytes + second_bytes,
                unrecovered_bytes: 0,
            }
        );
        let after_release = wire.buffer_pool_stats();
        assert_eq!(
            after_release.write_released,
            before_release.write_released + 1
        );
        assert!(
            request.data.topic_data[0].partition_data[0]
                .records
                .is_none()
        );
    }

    #[test]
    fn owned_same_partition_merge_keeps_combined_records_in_write_pool() {
        let wire = WireClient::connect_with_brokers(
            ConnectionConfig::default().buffer_pool_capacity(4),
            "dispatcher-test",
            [BrokerEndpoint::new(
                7,
                "127.0.0.1:9092".parse().expect("valid socket address"),
            )],
        );
        let dispatcher = ProducerDispatcher::new(wire.clone());
        let first_records = dispatcher
            .encode_ready_batch_records_owned(&ready_batch_with_value("orders", 0, b"a"), 0)
            .expect("first ready batch should encode");
        let second_records = dispatcher
            .encode_ready_batch_records_owned(
                &ready_batch_with_value("orders", 0, b"second-value-is-longer"),
                0,
            )
            .expect("second ready batch should encode");
        let before_merge = wire.buffer_pool_stats();

        {
            let mut request = BrokerProduceRequest::with_record_buffer_owner(&wire);
            request.push(
                route("orders", 0),
                first_records,
                BrokerProduceOptions {
                    acks: 1,
                    timeout_ms: 1_500,
                    transactional_id: None,
                },
                1,
            );
            request.push(
                route("orders", 0),
                second_records,
                BrokerProduceOptions {
                    acks: 1,
                    timeout_ms: 1_500,
                    transactional_id: None,
                },
                1,
            );
            assert_eq!(request.data.topic_data[0].partition_data.len(), 1);
            let after_merge = wire.buffer_pool_stats();
            assert_eq!(after_merge.write_acquired, before_merge.write_acquired + 1);
            assert_eq!(after_merge.write_released, before_merge.write_released + 2);
        }

        let after_drop = wire.buffer_pool_stats();
        assert_eq!(after_drop.write_acquired, before_merge.write_acquired + 1);
        assert_eq!(after_drop.write_released, before_merge.write_released + 3);
    }

    #[test]
    fn broker_dispatch_result_releases_record_bytes_before_wire_error() {
        let wire = WireClient::connect_with_brokers(
            ConnectionConfig::default().buffer_pool_capacity(2),
            "dispatcher-test",
            [BrokerEndpoint::new(
                7,
                "127.0.0.1:9092".parse().expect("valid socket address"),
            )],
        );
        let dispatcher = ProducerDispatcher::new(wire.clone());
        let records = dispatcher
            .encode_ready_batch_records(&ready_batch("orders", 0), 0)
            .expect("ready batch should encode");
        let mut request = BrokerProduceRequest::default();
        request.push(
            route("orders", 0),
            records,
            BrokerProduceOptions {
                acks: 1,
                timeout_ms: 1_500,
                transactional_id: None,
            },
            1,
        );
        let before_error = wire.buffer_pool_stats();

        let result = broker_dispatch_completed_result(
            &wire,
            0,
            request,
            Err(crate::wire::WireError::ConnectionClosed),
        );

        assert!(matches!(
            result,
            Err(DispatchError::RetryableWire(ProducerError::Wire(
                crate::wire::WireError::ConnectionClosed
            )))
        ));
        let after_error = wire.buffer_pool_stats();
        assert_eq!(after_error.write_released, before_error.write_released + 1);
    }

    #[tokio::test]
    async fn broker_dispatch_releases_record_bytes_when_metrics_sizing_fails() {
        let wire = WireClient::connect_with_brokers(
            ConnectionConfig::default().buffer_pool_capacity(2),
            "dispatcher-test",
            [BrokerEndpoint::new(
                7,
                "127.0.0.1:9092".parse().expect("valid socket address"),
            )],
        );
        let dispatcher = ProducerDispatcher::new(wire.clone());
        dispatcher.enable_metrics();
        let records = dispatcher
            .encode_ready_batch_records(&ready_batch("orders", 0), 0)
            .expect("ready batch should encode");
        let mut request = BrokerProduceRequest::default();
        request.push(
            route("orders", 0),
            records,
            BrokerProduceOptions {
                acks: 1,
                timeout_ms: 1_500,
                transactional_id: None,
            },
            1,
        );
        let before_error = wire.buffer_pool_stats();

        let result = dispatcher
            .dispatch_broker_requests(7, vec![request], i16::MAX, 0)
            .await;

        assert!(matches!(
            result,
            Err(DispatchError::Producer(ProducerError::Wire(_)))
        ));
        let after_error = wire.buffer_pool_stats();
        assert_eq!(after_error.write_released, before_error.write_released + 1);
    }

    #[tokio::test]
    async fn broker_dispatch_backpressure_retry_is_bounded_and_releases_record_bytes() {
        let version = client_api_info(ApiKey::Produce).max_version;
        let wire = WireClient::connect_with_brokers(
            ConnectionConfig::default()
                .broker_queue_capacity(1)
                .request_timeout(Duration::from_secs(30))
                .reconnect_backoff_initial(Duration::from_secs(30))
                .reconnect_backoff_max(Duration::from_secs(30))
                .buffer_pool_capacity(2),
            "dispatcher-test",
            [BrokerEndpoint::new(
                7,
                "127.0.0.1:9".parse().expect("valid socket address"),
            )],
        );
        let filler = ProduceRequestData::default();
        let _first = wire
            .enqueue_to_broker::<_, kacrab_protocol::generated::ProduceResponseData>(
                7,
                ApiKey::Produce,
                version,
                &filler,
            )
            .expect("first command should create broker handle");
        tokio::task::yield_now().await;
        let _second = wire.enqueue_to_broker::<_, kacrab_protocol::generated::ProduceResponseData>(
            7,
            ApiKey::Produce,
            version,
            &filler,
        );
        let dispatcher =
            ProducerDispatcher::new(wire.clone()).delivery_timeout(Duration::from_millis(5));
        let records = dispatcher
            .encode_ready_batch_records(&ready_batch("orders", 0), 0)
            .expect("ready batch should encode");
        let mut request = BrokerProduceRequest::default();
        request.push(
            route("orders", 0),
            records,
            BrokerProduceOptions {
                acks: 1,
                timeout_ms: 1_500,
                transactional_id: None,
            },
            1,
        );
        let before_dispatch = wire.buffer_pool_stats();

        let result = tokio::time::timeout(
            Duration::from_millis(100),
            dispatcher.dispatch_broker_requests(7, vec![request], version, 0),
        )
        .await
        .expect("backpressure retry should be bounded by delivery timeout");

        assert!(matches!(
            result,
            Err(DispatchError::Producer(ProducerError::Wire(
                crate::wire::WireError::Backpressure
            )))
        ));
        let after_dispatch = wire.buffer_pool_stats();
        assert_eq!(
            after_dispatch.write_released,
            before_dispatch.write_released + 1
        );
    }

    #[tokio::test]
    async fn retry_wait_returns_delivery_timeout_when_outer_deadline_expires() {
        let dispatcher =
            ProducerDispatcher::new(test_wire()).delivery_timeout(Duration::from_millis(1));
        let accumulator = SharedAccumulator::with_config(
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
        let mut retry_backoff = dispatcher.retry_backoff_state();

        let error = dispatcher
            .wait_before_retry(&batches, &mut retry_backoff, false)
            .await
            .expect("retry wait should expire delivery timeout");

        assert!(matches!(
            error,
            ProducerError::DeliveryTimeout { topic, partition }
                if topic == "orders" && partition == 0
        ));
    }

    fn ready_batch(topic: &str, partition: i32) -> super::ReadyBatch {
        ready_batch_with_value(topic, partition, b"value")
    }

    fn ready_batch_with_value(
        topic: &str,
        partition: i32,
        value: &'static [u8],
    ) -> super::ReadyBatch {
        let accumulator = SharedAccumulator::with_config(
            AccumulatorConfig::default()
                .batch_size(1)
                .linger(Duration::from_secs(1)),
        );
        accumulator
            .append_at(
                ProducerRecord::new(topic, partition).value(Bytes::from_static(value)),
                Instant::now(),
            )
            .expect("append test record");
        accumulator
            .drain_ready(Instant::now())
            .pop()
            .expect("ready batch")
    }

    #[tokio::test]
    async fn retryable_broker_error_fails_definitively_when_attempts_exhausted_like_java() {
        // Kafka Sender.canRetry stops retrying once attempts are exhausted: the
        // generic retriable broker error becomes a definite Broker failure.
        let dispatcher = ProducerDispatcher::new(test_wire());
        let batches = vec![ready_batch("orders", 0)];
        let mut retry_backoff = dispatcher.retry_backoff_state();
        let mut attempts_remaining = 0;

        let error = dispatcher
            .handle_retryable_broker_retry(
                &batches,
                &mut attempts_remaining,
                &mut retry_backoff,
                "orders",
                0,
                ErrorCode::NotEnoughReplicas,
            )
            .await
            .expect("exhausted attempts fail definitively");

        assert!(matches!(
            error,
            ProducerError::Broker {
                error: ErrorCode::NotEnoughReplicas,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn retryable_broker_error_retries_while_attempts_remain_like_java() {
        // With attempts left and the delivery timeout not reached, a retriable
        // broker error is retried (no error returned) and the attempt budget
        // is decremented.
        let dispatcher = ProducerDispatcher::new(test_wire());
        let batches = vec![ready_batch("orders", 0)];
        let mut retry_backoff = dispatcher.retry_backoff_state();
        let mut attempts_remaining = 3;

        let outcome = dispatcher
            .handle_retryable_broker_retry(
                &batches,
                &mut attempts_remaining,
                &mut retry_backoff,
                "orders",
                0,
                ErrorCode::NotEnoughReplicas,
            )
            .await;

        assert!(outcome.is_none());
        assert_eq!(attempts_remaining, 2);
    }

    #[tokio::test]
    async fn leader_change_retry_skips_backoff_but_honors_delivery_timeout() {
        // Leader changed and delivery timeout not reached -> retry immediately
        // (no backoff sleep, returns None).
        let dispatcher = ProducerDispatcher::new(test_wire());
        let batches = vec![ready_batch("orders", 0)];
        assert!(
            dispatcher
                .check_delivery_timeout_before_retry(&batches, false)
                .await
                .is_none()
        );

        // Delivery timeout already elapsed -> fail even on a leader change.
        let expired = ProducerDispatcher::new(test_wire()).delivery_timeout(Duration::ZERO);
        let batches = vec![ready_batch("orders", 0)];
        assert!(matches!(
            expired.check_delivery_timeout_before_retry(&batches, false).await,
            Some(ProducerError::DeliveryTimeout { topic, partition })
                if topic == "orders" && partition == 0
        ));
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
    async fn transactional_produce_error_escalates_to_fatal_when_coordinator_cannot_bump_epoch() {
        use super::super::transaction::TransactionState;
        // A bump-capable coordinator keeps UnknownProducerId abortable and
        // requests a client-side epoch bump (Kafka needToTriggerEpochBumpFromClient).
        let can_bump = transactional_dispatcher();
        {
            let mut state = can_bump.producer_state.lock().await;
            state.in_transaction = true;
            // Default coordinator_lacks_epoch_bump_support == false (assume supported).
        }
        can_bump
            .record_transactional_produce_error(ErrorCode::UnknownProducerId, "orders", 0)
            .await;
        {
            let state = can_bump.producer_state.lock().await;
            assert_eq!(state.transaction_state, TransactionState::AbortableError);
            assert!(state.epoch_bump_required);
        }

        // A coordinator that cannot bump the epoch (InitProducerId < v3) turns the
        // same error fatal (Kafka canHandleAbortableError() == false).
        let cannot_bump = transactional_dispatcher();
        {
            let mut state = cannot_bump.producer_state.lock().await;
            state.in_transaction = true;
            state.coordinator_lacks_epoch_bump_support = true;
        }
        cannot_bump
            .record_transactional_produce_error(ErrorCode::UnknownProducerId, "orders", 0)
            .await;
        {
            let state = cannot_bump.producer_state.lock().await;
            assert_eq!(state.transaction_state, TransactionState::FatalError);
            assert_eq!(state.fatal_error, Some(ErrorCode::UnknownProducerId));
            assert!(!state.epoch_bump_required);
            assert!(state.abortable_error.is_none());
        }
    }

    #[tokio::test]
    async fn transaction_control_error_escalates_to_fatal_when_coordinator_cannot_bump_epoch() {
        use super::super::transaction::TransactionState;
        // Control-plane UnknownProducerId stays abortable + epoch bump when the
        // coordinator supports bumping, and goes fatal when it does not.
        let can_bump = transactional_dispatcher();
        {
            let mut state = can_bump.producer_state.lock().await;
            state.in_transaction = true;
        }
        can_bump
            .record_transaction_error(ErrorCode::UnknownProducerId)
            .await;
        {
            let state = can_bump.producer_state.lock().await;
            assert_eq!(state.transaction_state, TransactionState::AbortableError);
            assert!(state.epoch_bump_required);
        }

        let cannot_bump = transactional_dispatcher();
        {
            let mut state = cannot_bump.producer_state.lock().await;
            state.in_transaction = true;
            state.coordinator_lacks_epoch_bump_support = true;
        }
        cannot_bump
            .record_transaction_error(ErrorCode::UnknownProducerId)
            .await;
        {
            let state = cannot_bump.producer_state.lock().await;
            assert_eq!(state.transaction_state, TransactionState::FatalError);
            assert_eq!(state.fatal_error, Some(ErrorCode::UnknownProducerId));
            assert!(!state.epoch_bump_required);
            assert!(state.abortable_error.is_none());
        }
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
            .expect("awaiting the cached result should ack it like Kafka");

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
            .expect("awaiting the cached result should ack it like Kafka");

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
        let accumulator = SharedAccumulator::with_config(AccumulatorConfig::default());

        assert!(
            dispatcher
                .dispatch_ready(&accumulator, Instant::now())
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
                .dispatch_all(&accumulator)
                .await
                .expect("empty all")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn dispatch_ready_returns_producer_error_when_no_broker_is_available() {
        let wire = WireClient::connect_with_brokers(ConnectionConfig::default(), "client-a", []);
        let dispatcher = ProducerDispatcher::new(wire);
        let accumulator = SharedAccumulator::with_config(
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
                .dispatch_ready(&accumulator, Instant::now())
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
            .dispatch_drained(vec![ready_batch("orders", 0)], Instant::now(), 0)
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
    fn idempotence_state_tracks_sequences_and_wraps_at_max_like_java() {
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

        // Kafka DefaultRecordBatch.incrementSequence wraps at i32::MAX rather than
        // overflowing: base i32::MAX with a 1-record batch returns i32::MAX and the
        // next base wraps to 0.
        let key = TopicPartitionKey {
            topic: "orders".to_owned(),
            partition: 0,
        };
        state
            .partitions
            .entry(key.clone())
            .or_default()
            .next_sequence = i32::MAX;
        assert_eq!(
            state
                .next_sequence("orders", 0, 1)
                .expect("wrapping base sequence"),
            i32::MAX
        );
        assert_eq!(state.partitions[&key].next_sequence, 0);
        // A 3-record batch straddling the boundary: base 2147483646 -> next wraps to 2.
        state
            .partitions
            .entry(key.clone())
            .or_default()
            .next_sequence = i32::MAX - 1;
        assert_eq!(
            state
                .next_sequence("orders", 0, 3)
                .expect("straddling base sequence"),
            i32::MAX - 1
        );
        assert_eq!(state.partitions[&key].next_sequence, 1);
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
    fn resolve_unresolved_after_drain_bumps_epoch_for_idempotent_like_java() {
        // Idempotent producer: unacked messages after drain request an epoch bump.
        let mut state = ProducerIdempotenceState::default();
        let _ = state.next_sequence("orders", 0, 3).expect("sequence");
        state.mark_sequence_unresolved("orders", 0, 0, 3);

        state.resolve_unresolved_sequence_after_drain("orders", 0, false, true);

        assert!(state.epoch_bump_required);
        // Marker cleared, so the partition no longer blocks (epoch bump resets it).
        assert_eq!(state.next_sequence("orders", 0, 1).expect("unblocked"), 3);
    }

    #[test]
    fn resolve_defers_until_partition_has_no_inflight_batches_like_java() {
        // Kafka maybeResolveSequences only resolves a partition once it has NO
        // in-flight batches. Two batches are in flight (base sequence 0 and 3).
        let mut state = ProducerIdempotenceState::default();
        let seq0 = state.next_sequence("orders", 0, 3).expect("seq0");
        let seq3 = state.next_sequence("orders", 0, 3).expect("seq3");
        assert_eq!(seq0, 0);
        assert_eq!(seq3, 3);
        state.register_inflight_sequence("orders", 0, seq0);
        state.register_inflight_sequence("orders", 0, seq3);

        // Batch seq0 times out ambiguously and is marked unresolved. Resolving now
        // must NOT bump the epoch: seq3 is still in flight under the current epoch,
        // and bumping would re-send seq0 under a new epoch while seq3 could still
        // be written, risking a duplicate.
        state.mark_sequence_unresolved("orders", 0, seq0, 3);
        state.record_unresolved_loss_ambiguity("orders", 0, true);
        state.remove_inflight_sequence("orders", 0, seq0);
        let ambiguous = state.unresolved_loss_ambiguous("orders", 0);
        state.resolve_unresolved_sequence_after_drain("orders", 0, false, ambiguous);
        assert!(
            !state.epoch_bump_required,
            "must not bump the epoch while seq3 is still in flight"
        );
        assert!(state.has_inflight_batches("orders", 0));

        // seq3 drains → the partition has no in-flight batches, so the deferred
        // resolve now bumps the epoch (the loss was ambiguous).
        state.remove_inflight_sequence("orders", 0, seq3);
        assert!(!state.has_inflight_batches("orders", 0));
        let ambiguous = state.unresolved_loss_ambiguous("orders", 0);
        state.resolve_unresolved_sequence_after_drain("orders", 0, false, ambiguous);
        assert!(
            state.epoch_bump_required,
            "bump the epoch once the partition has fully drained"
        );
    }

    #[test]
    fn detects_stale_producer_identity_after_epoch_bump_like_java() {
        use super::super::transaction::ProducerIdentity;
        // Kafka hasStaleProducerIdAndEpoch: a batch stamped under an older epoch is stale
        // once the producer identity advances, so it is re-stamped before being sent.
        let mut state = ProducerIdempotenceState::default();
        // No identity yet -> nothing is stale (cannot compare).
        assert!(!state.is_stale_identity(ProducerIdentity {
            producer_id: 42,
            producer_epoch: 3,
        }));

        state.identity = Some(ProducerIdentity {
            producer_id: 42,
            producer_epoch: 4,
        });
        // Same id, older epoch -> stale (epoch was bumped since).
        assert!(state.is_stale_identity(ProducerIdentity {
            producer_id: 42,
            producer_epoch: 3,
        }));
        // Current identity -> not stale.
        assert!(!state.is_stale_identity(ProducerIdentity {
            producer_id: 42,
            producer_epoch: 4,
        }));
        // Different producer id -> stale.
        assert!(state.is_stale_identity(ProducerIdentity {
            producer_id: 7,
            producer_epoch: 4,
        }));

        // request_epoch_bump flags the deferred bump applied in producer_batch_state.
        assert!(!state.epoch_bump_required);
        state.request_epoch_bump();
        assert!(state.epoch_bump_required);
        // reset_sequences_after_epoch_bump clears every partition's sequence state so a
        // global epoch bump restarts all partitions at 0 under the new epoch.
        let _ = state.next_sequence("orders", 0, 3).expect("sequence");
        state.register_inflight_sequence("orders", 0, 0);
        state.reset_sequences_after_epoch_bump();
        assert!(!state.epoch_bump_required);
        assert!(!state.has_inflight_batches("orders", 0));
        assert_eq!(
            state.next_sequence("orders", 0, 1).expect("reset sequence"),
            0
        );
    }

    #[test]
    fn resolve_unresolved_after_drain_aborts_transaction_like_java() {
        use super::super::transaction::TransactionState;
        // Transactional producer with a bump-capable coordinator: abortable error.
        let mut state = ProducerIdempotenceState::default();
        state.in_transaction = true;
        let _ = state.next_sequence("orders", 0, 3).expect("sequence");
        state.mark_sequence_unresolved("orders", 0, 0, 3);

        state.resolve_unresolved_sequence_after_drain("orders", 0, true, true);

        assert_eq!(state.transaction_state, TransactionState::AbortableError);
        assert!(state.epoch_bump_required);

        // A coordinator that cannot bump escalates to fatal.
        let mut fatal = ProducerIdempotenceState::default();
        fatal.in_transaction = true;
        fatal.coordinator_lacks_epoch_bump_support = true;
        let _ = fatal.next_sequence("orders", 0, 3).expect("sequence");
        fatal.mark_sequence_unresolved("orders", 0, 0, 3);

        fatal.resolve_unresolved_sequence_after_drain("orders", 0, true, true);

        assert_eq!(fatal.transaction_state, TransactionState::FatalError);
    }

    #[test]
    fn resolve_unresolved_after_drain_clears_marker_when_fully_acked_like_java() {
        // If subsequent batches were acked (lastAcked + 1 == next), the partition
        // is resolved without an epoch bump.
        let mut state = ProducerIdempotenceState::default();
        let _ = state.next_sequence("orders", 0, 3).expect("sequence");
        state.mark_sequence_unresolved("orders", 0, 0, 3);
        state.maybe_update_last_acked_sequence("orders", 0, 2); // next=3, lastAcked=2 => resolved

        state.resolve_unresolved_sequence_after_drain("orders", 0, false, false);

        assert!(!state.epoch_bump_required);
        assert_eq!(state.next_sequence("orders", 0, 1).expect("unblocked"), 3);
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
    fn broker_produce_request_keeps_zero_topic_id_topics_separate() {
        let mut request = BrokerProduceRequest::default();
        let orders = route("orders", 0);
        let payments = route("payments", 0);
        request.push(
            orders,
            Bytes::from_static(b"orders-records"),
            BrokerProduceOptions {
                acks: -1,
                timeout_ms: 1_500,
                transactional_id: None,
            },
            1,
        );

        assert!(!request.contains_route(&payments));

        request.push(
            payments,
            Bytes::from_static(b"payments-records"),
            BrokerProduceOptions {
                acks: -1,
                timeout_ms: 1_500,
                transactional_id: None,
            },
            1,
        );

        assert_eq!(request.data.topic_data.len(), 2);
        assert_eq!(request.data.topic_data[0].name.as_str(), "orders");
        assert_eq!(request.data.topic_data[1].name.as_str(), "payments");
        assert_eq!(
            request.data.topic_data[0].partition_data[0]
                .records
                .as_ref()
                .expect("orders records")
                .as_ref(),
            b"orders-records"
        );
        assert_eq!(
            request.data.topic_data[1].partition_data[0]
                .records
                .as_ref()
                .expect("payments records")
                .as_ref(),
            b"payments-records"
        );
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
        let accumulator = SharedAccumulator::with_config(
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

        let accumulator = SharedAccumulator::with_config(
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

    #[tokio::test]
    async fn complete_deliveries_uses_later_split_receipts_for_original_delivery_handles() {
        let accumulator = SharedAccumulator::with_config(
            AccumulatorConfig::default()
                .batch_size(TEST_LARGE_BATCH_SIZE)
                .buffer_memory(TEST_LARGE_BATCH_SIZE * 4)
                .linger(Duration::from_secs(1)),
        );
        let first = accumulator
            .append_for_delivery(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")))
            .expect("append first delivery");
        let second = accumulator
            .append_for_delivery(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"b")))
            .expect("append second delivery");
        let third = accumulator
            .append_for_delivery(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"c")))
            .expect("append third delivery");
        let callback_offset = Arc::new(AtomicI64::new(-1));
        let callback_sink = Arc::clone(&callback_offset);
        third.register_callback(Box::new(move |result| {
            callback_sink.store(
                result.expect("third callback receipt").offset,
                Ordering::Relaxed,
            );
        }));
        let batch = accumulator
            .drain_all()
            .pop()
            .expect("drained delivery batch");
        let mut batches = batch
            .split_for_retry_with_compression_ratio(78, 1.0)
            .expect("split into retry batches");
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].records.len(), 2);
        assert_eq!(batches[1].records.len(), 1);
        let receipts = vec![
            RecordMetadata {
                topic: Arc::from("orders"),
                partition: 0,
                leader_id: 7,
                offset: 40,
                timestamp_ms: -1,
                serialized_key_size: -1,
                serialized_value_size: 1,
            },
            RecordMetadata {
                topic: Arc::from("orders"),
                partition: 0,
                leader_id: 7,
                offset: 41,
                timestamp_ms: -1,
                serialized_key_size: -1,
                serialized_value_size: 1,
            },
            RecordMetadata {
                topic: Arc::from("orders"),
                partition: 0,
                leader_id: 7,
                offset: 99,
                timestamp_ms: -1,
                serialized_key_size: -1,
                serialized_value_size: 1,
            },
        ];

        complete_deliveries(&mut batches, &receipts);

        assert_eq!(first.await.expect("first receipt").offset, 40);
        assert_eq!(second.await.expect("second receipt").offset, 41);
        assert_eq!(callback_offset.load(Ordering::Relaxed), 99);
        assert_eq!(third.await.expect("third receipt").offset, 99);
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
