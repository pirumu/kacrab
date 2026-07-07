//! Sender-side dispatch scheduling state for the producer.

use std::{
    collections::VecDeque,
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use ahash::{AHashMap, AHashSet};
use tokio::{
    runtime::Handle,
    sync::{Mutex as AsyncMutex, Notify},
    task::{AbortHandle, JoinError, JoinSet},
};

use super::{
    ProducerRecord, ReadyBatch,
    accumulator::{AppendStatus, ReadyBatchIdentity, SharedAccumulator},
    dispatcher::{DispatchOutcome, ProducerDispatcher},
    error::ProducerError,
    metrics::ProducerMetrics,
    record::SendFuture,
};
#[cfg(test)]
use crate::wire::{ConnectionConfig, WireClient};

const COMPLETED_BATCH_IDENTITY_TOMBSTONE_LIMIT: usize = 4096;

#[derive(Debug)]
pub(crate) struct ProducerSenderState {
    in_flight: JoinSet<TimedDispatchOutcome>,
    // Per-partition in-flight DEPTH (number of outstanding ProduceRequests for each
    // partition). Idempotent producers pipeline up to max.in.flight.requests.per.connection
    // requests per partition (Kafka parity); `select_dispatchable_batches` emits at most one
    // new request per partition per cycle so each becomes its own concurrent dispatch task
    // (pipelining across the outer in-flight JoinSet) and the depth here bounds the pipeline.
    in_flight_partitions: AHashMap<InFlightPartitionKey, usize>,
    in_flight_batch_identities: AHashSet<ReadyBatchIdentity>,
    completed_batch_identities: AHashSet<ReadyBatchIdentity>,
    completed_batch_identity_order: VecDeque<ReadyBatchIdentity>,
    pending_accumulator_completions: Vec<ReadyBatchIdentity>,
    in_flight_reservations: AHashMap<tokio::task::Id, InFlightDispatchReservation>,
    in_flight_buffered_bytes: usize,
    in_flight_incomplete_batches: usize,
    callback_append_poll_budget: AppendPollBudget,
    max_in_flight_requests: usize,
    idempotent_ordering: bool,
    completion_notify: Option<Arc<Notify>>,
}

#[derive(Debug)]
pub(crate) struct ProducerSender {
    pub(crate) accumulator: Arc<SharedAccumulator>,
    pub(crate) state: ProducerSenderState,
    dispatcher: ProducerDispatcher,
    sender_loop_notify: Arc<Notify>,
    background_dispatch_paused: Arc<AtomicBool>,
}

/// SPIKE diagnostic: total spins waiting for buffer.memory in the sync `send_now`
/// path (background loop draining on another worker). Tells the bench whether a
/// slow sync run is loop-drain-bound.
pub static SYNC_NOW_BUFFER_SPINS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

#[derive(Debug)]
pub(crate) struct ProducerSenderRuntime {
    sender: Arc<AsyncMutex<ProducerSender>>,
    loop_handle: ProducerSenderLoop,
    loop_metrics_enabled: Arc<AtomicBool>,
    accumulator_batch_size: usize,
    metrics: ProducerMetrics,
    // Shared handles for the lock-free append fast path: appending touches only
    // these (no `sender` async-mutex), so concurrent send(&self) calls don't
    // serialize on the sender mutex with each other or the background loop.
    bypass_accumulator: Arc<SharedAccumulator>,
    bypass_dispatcher: ProducerDispatcher,
    bypass_paused: Arc<AtomicBool>,
    bypass_notify: Arc<Notify>,
}

#[derive(Debug, Default)]
pub(crate) struct ProducerSenderLoop {
    // Interior mutability so the send hot path can ensure the loop is running
    // through `&self`, which lets `Producer::send` take `&self` and be called
    // concurrently from multiple tasks (thread-safe producer).
    started: AtomicBool,
    handle: std::sync::Mutex<Option<AbortHandle>>,
}

impl ProducerSenderLoop {
    const fn store_handle(handle: AbortHandle) -> Self {
        Self {
            started: AtomicBool::new(true),
            handle: std::sync::Mutex::new(Some(handle)),
        }
    }

    pub(crate) fn is_running(&self) -> bool {
        self.started.load(Ordering::Acquire)
    }

    pub(crate) fn spawn(
        sender: Arc<AsyncMutex<ProducerSender>>,
        metrics_enabled: Arc<AtomicBool>,
        metrics: ProducerMetrics,
        accumulator_batch_size: usize,
    ) -> Self {
        let Ok(handle) = Handle::try_current() else {
            return Self::default();
        };
        let task = handle.spawn(ProducerSender::background_loop(
            sender,
            metrics_enabled,
            metrics,
            accumulator_batch_size,
        ));
        Self::store_handle(task.abort_handle())
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "the started flag is published while the handle lock is held so a concurrent \
                  ensure_running cannot observe started=false and double-spawn the loop"
    )]
    pub(crate) fn ensure_running(
        &self,
        sender: Arc<AsyncMutex<ProducerSender>>,
        metrics_enabled: Arc<AtomicBool>,
        metrics: ProducerMetrics,
        accumulator_batch_size: usize,
    ) {
        if self.started.load(Ordering::Acquire) {
            return;
        }
        let mut guard = self
            .handle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Double-check under the lock so only one task spawns the loop.
        if self.started.load(Ordering::Relaxed) {
            return;
        }
        let Ok(handle) = Handle::try_current() else {
            return;
        };
        let task = handle.spawn(ProducerSender::background_loop(
            sender,
            metrics_enabled,
            metrics,
            accumulator_batch_size,
        ));
        *guard = Some(task.abort_handle());
        self.started.store(true, Ordering::Release);
    }

    fn abort_inner(&self) {
        let handle = self
            .handle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(handle) = handle {
            handle.abort();
        }
        self.started.store(false, Ordering::Release);
    }
}

impl Drop for ProducerSenderLoop {
    fn drop(&mut self) {
        self.abort_inner();
    }
}

impl ProducerSenderRuntime {
    pub(crate) fn with_dispatcher(
        accumulator_config: super::AccumulatorConfig,
        max_in_flight_requests: usize,
        idempotent_ordering: bool,
        dispatcher: ProducerDispatcher,
    ) -> (Self, ProducerMetrics) {
        let accumulator_batch_size = accumulator_config.batch_size;
        Self::new(
            ProducerSender::with_dispatcher(
                accumulator_config,
                max_in_flight_requests,
                idempotent_ordering,
                dispatcher,
            ),
            accumulator_batch_size,
        )
    }

    pub(crate) fn new(
        sender: ProducerSender,
        accumulator_batch_size: usize,
    ) -> (Self, ProducerMetrics) {
        let metrics = sender.metrics_handle();
        let bypass_accumulator = Arc::clone(&sender.accumulator);
        let bypass_dispatcher = sender.dispatcher.clone();
        let bypass_paused = Arc::clone(&sender.background_dispatch_paused);
        let bypass_notify = Arc::clone(&sender.sender_loop_notify);
        let sender = Arc::new(AsyncMutex::new(sender));
        let loop_metrics_enabled = Arc::new(AtomicBool::new(false));
        let loop_handle = ProducerSenderLoop::spawn(
            Arc::clone(&sender),
            Arc::clone(&loop_metrics_enabled),
            metrics.clone(),
            accumulator_batch_size,
        );
        (
            Self {
                sender,
                loop_handle,
                loop_metrics_enabled,
                accumulator_batch_size,
                metrics: metrics.clone(),
                bypass_accumulator,
                bypass_dispatcher,
                bypass_paused,
                bypass_notify,
            },
            metrics,
        )
    }

    pub(crate) async fn lock(&self) -> tokio::sync::MutexGuard<'_, ProducerSender> {
        self.sender.lock().await
    }

    /// Lock-free callback append using the bypass handles (no sender mutex).
    fn try_bypass_append_callback<'a, BeforeDispatch>(
        &self,
        append: AppendCallbackDeliveryRecord<'a>,
        before_dispatch: BeforeDispatch,
    ) -> SyncCallbackAppend<'a, BeforeDispatch>
    where
        BeforeDispatch: FnOnce(&SendFuture),
    {
        let compression_ratio = self
            .bypass_dispatcher
            .compression_ratio_estimation(append.record.topic.as_ref());
        // Fast-path capacity check ignores in-flight bytes (a small, bounded
        // buffer.memory over-commit); the awaiting slow path counts them exactly.
        if !self
            .bypass_accumulator
            .has_available_memory_for_reserved_with_compression_ratio(
                &append.record,
                0,
                compression_ratio,
            )
        {
            return SyncCallbackAppend::WouldBlock(append, before_dispatch);
        }
        let AppendCallbackDeliveryRecord {
            record,
            now,
            sticky_topic,
            ..
        } = append;
        let (delivery, status) = match self
            .bypass_accumulator
            .append_for_delivery_with_status_at_compression_ratio(record, now, compression_ratio)
        {
            Ok(appended) => appended,
            Err(error) => return SyncCallbackAppend::Failed(error),
        };
        before_dispatch(&delivery);
        let decision = ProducerSenderState::single_append_dispatch_decision(status);
        self.note_bypass_append(status);
        let sticky_ready_topic = if decision.should_mark_sticky_batch_ready() {
            sticky_topic.map(str::to_owned)
        } else {
            None
        };
        SyncCallbackAppend::Appended {
            delivery,
            sticky_ready_topic,
        }
    }

    /// Wake the background sender loop after a bypass append, gated like
    /// `note_append_status_for_sender_loop` (only on new batch or batch ready).
    fn note_bypass_append(&self, status: AppendStatus) {
        if status.starts_new_batch || status.batch_ready {
            self.bypass_paused.store(false, Ordering::Relaxed);
            self.bypass_notify.notify_one();
        }
    }

    /// SYNC `send_now`: append one callback-delivery record with ZERO `.await` via
    /// the bypass (shared accumulator, NO sender async mutex, NO spin — only the
    /// brief `SharedAccumulator` lock). Returns `None` when it would need to suspend
    /// (sticky rotation) or block (buffer full); the caller handles those.
    pub(crate) fn append_callback_now<BeforeDispatch>(
        &self,
        append: AppendCallbackDeliveryRecord<'_>,
        before_dispatch: BeforeDispatch,
    ) -> Option<Result<SendFuture, ProducerError>>
    where
        BeforeDispatch: FnOnce(&SendFuture),
    {
        let mut append = append;
        let mut before_dispatch = before_dispatch;
        loop {
            match self.try_bypass_append_callback(append, before_dispatch) {
                SyncCallbackAppend::Appended {
                    delivery,
                    sticky_ready_topic,
                } => {
                    if let Some(topic) = sticky_ready_topic {
                        // The record is already appended; mark the now-full sticky
                        // batch ready synchronously (best effort) so the next record
                        // rotates. On lock contention the sticky budget check rotates
                        // a little later — never drop the delivery handle.
                        let _marked = self
                            .bypass_dispatcher
                            .try_mark_sticky_batch_ready_now(&topic);
                    }
                    return Some(Ok(delivery));
                },
                SyncCallbackAppend::Failed(error) => return Some(Err(error)),
                SyncCallbackAppend::WouldBlock(retry_append, retry_before_dispatch) => {
                    // buffer.memory full. On a multi-threaded runtime the background
                    // loop drains on another worker, so spin until it frees space.
                    // Bound the wait by the record's max.block deadline (like Kafka's
                    // BufferPool wait) so a single-threaded runtime — where no other
                    // worker can drain while we spin — reports backpressure instead
                    // of spinning forever.
                    if std::time::Instant::now() >= retry_append.deadline {
                        return Some(Err(ProducerError::Backpressure));
                    }
                    let _spins = SYNC_NOW_BUFFER_SPINS.fetch_add(1, Ordering::Relaxed);
                    append = retry_append;
                    before_dispatch = retry_before_dispatch;
                    std::thread::yield_now();
                    std::hint::spin_loop();
                },
            }
        }
    }

    pub(crate) fn try_lock(
        &self,
    ) -> Result<tokio::sync::MutexGuard<'_, ProducerSender>, tokio::sync::TryLockError> {
        self.sender.try_lock()
    }

    /// Non-blocking sticky/keyed partition assignment for the sync send path,
    /// routed through the bypass handles so adaptive rotation can re-sample the
    /// accumulator's live partition queue depths.
    pub(crate) fn try_assign_cached_sticky_partition_now(
        &self,
        record: &mut ProducerRecord,
    ) -> bool {
        self.bypass_dispatcher
            .try_assign_cached_sticky_partition_now(record, &self.bypass_accumulator)
    }

    /// Clone the shared `ProducerSender` handle so the rare synchronous-send slow
    /// path (cold metadata / buffer-full / transactional / custom partitioner) can
    /// drive the awaiting append from a dedicated drain task without owning the
    /// (non-`Clone`) runtime.
    pub(crate) fn shared_sender(&self) -> Arc<AsyncMutex<ProducerSender>> {
        Arc::clone(&self.sender)
    }

    pub(crate) fn enable_loop_metrics(&self) {
        self.loop_metrics_enabled.store(true, Ordering::Relaxed);
    }

    pub(crate) fn ensure_loop_running(&self) {
        // Fast path: avoid cloning the sender Arc, metrics-enabled Arc, and the
        // metrics handle on every send once the background loop is already up.
        if self.loop_handle.is_running() {
            return;
        }
        self.loop_handle.ensure_running(
            Arc::clone(&self.sender),
            Arc::clone(&self.loop_metrics_enabled),
            self.metrics.clone(),
            self.accumulator_batch_size,
        );
    }

    #[cfg(test)]
    pub(crate) fn loop_is_running(&self) -> bool {
        self.loop_handle.is_running()
    }
}

impl ProducerSender {
    pub(crate) fn with_dispatcher(
        accumulator_config: super::AccumulatorConfig,
        max_in_flight_requests: usize,
        idempotent_ordering: bool,
        dispatcher: ProducerDispatcher,
    ) -> Self {
        let sender_loop_notify = Arc::new(Notify::new());
        let mut state = ProducerSenderState::new_with_idempotent_ordering(
            max_in_flight_requests,
            idempotent_ordering,
        );
        state.notify_on_dispatch_completion(Arc::clone(&sender_loop_notify));
        Self {
            accumulator: Arc::new(SharedAccumulator::with_config(accumulator_config)),
            state,
            dispatcher,
            sender_loop_notify,
            background_dispatch_paused: Arc::new(AtomicBool::new(false)),
        }
    }

    async fn background_loop(
        sender: Arc<AsyncMutex<Self>>,
        metrics_enabled: Arc<AtomicBool>,
        metrics: ProducerMetrics,
        _accumulator_batch_size: usize,
    ) {
        loop {
            let (wait, notify) = {
                let mut sender = sender.lock().await;
                let notify = sender.sender_loop_notifier();
                let wait = sender
                    .drive_wake_until_waiting(
                        std::time::Instant::now(),
                        ReadyDispatchObservers::new(
                            |_| {},
                            || {
                                if metrics_enabled.load(Ordering::Relaxed) {
                                    metrics.record_requeue();
                                }
                            },
                            |_: &[ReadyBatch]| {},
                        ),
                    )
                    .await;
                drop(sender);
                match wait {
                    Ok(wait) => (wait, notify),
                    Err(_error) => {
                        if metrics_enabled.load(Ordering::Relaxed) {
                            metrics.record_error();
                        }
                        (SenderLoopWait::Parked, notify)
                    },
                }
            };
            match wait {
                SenderLoopWait::SleepUntil(ready_at) => {
                    tokio::select! {
                        () = notify.notified() => {},
                        () = tokio::time::sleep(ready_at.saturating_duration_since(std::time::Instant::now())) => {},
                    }
                },
                SenderLoopWait::DispatchCompletion | SenderLoopWait::Parked => {
                    notify.notified().await;
                },
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn new(
        accumulator_config: super::AccumulatorConfig,
        max_in_flight_requests: usize,
    ) -> Self {
        Self::with_dispatcher(
            accumulator_config,
            max_in_flight_requests,
            false,
            ProducerDispatcher::new(WireClient::connect_with_brokers(
                ConnectionConfig::default(),
                "producer-sender-test",
                [],
            )),
        )
    }

    pub(crate) fn buffered_bytes(&self) -> usize {
        self.state.buffered_bytes(&self.accumulator)
    }

    pub(crate) fn queue_snapshot(&self) -> SenderQueueSnapshot {
        self.state.queue_snapshot(&self.accumulator)
    }

    pub(crate) fn next_ready_at(&self, now: std::time::Instant) -> Option<std::time::Instant> {
        self.accumulator.next_ready_at(now)
    }

    pub(crate) fn sender_loop_notifier(&self) -> Arc<Notify> {
        Arc::clone(&self.sender_loop_notify)
    }

    fn notify_sender_loop(&self) {
        self.sender_loop_notify.notify_one();
    }

    fn note_append_status_for_sender_loop(&self, status: AppendStatus) {
        if Self::should_notify_sender_loop_after_append(status) {
            self.background_dispatch_paused
                .store(false, Ordering::Relaxed);
            self.notify_sender_loop();
        }
    }

    fn pause_background_dispatch_after_requeue(&self) {
        self.background_dispatch_paused
            .store(true, Ordering::Relaxed);
    }

    const fn should_notify_sender_loop_after_append(status: AppendStatus) -> bool {
        status.starts_new_batch || status.batch_ready
    }

    pub(crate) async fn wait_for_sender_loop_delay(
        &self,
        delay: std::time::Duration,
    ) -> SenderWaitSignal {
        tokio::select! {
            () = self.sender_loop_notify.notified() => SenderWaitSignal::Notified,
            () = tokio::time::sleep(delay) => SenderWaitSignal::TimedOut,
        }
    }

    pub(crate) async fn wait_for_sender_loop_wait<LatencyObserver, RequeueObserver>(
        &mut self,
        wait: SenderLoopWait,
        now: std::time::Instant,
        mut observe_latency: LatencyObserver,
        mut observe_requeue: RequeueObserver,
    ) -> Result<SenderWaitSignal, ProducerError>
    where
        LatencyObserver: FnMut(std::time::Duration),
        RequeueObserver: FnMut(),
    {
        match wait {
            SenderLoopWait::DispatchCompletion => {
                self.state
                    .wait_for_handled_dispatch(
                        &self.accumulator,
                        false,
                        &mut observe_latency,
                        &mut observe_requeue,
                    )
                    .await?;
                Ok(SenderWaitSignal::DispatchCompleted)
            },
            SenderLoopWait::SleepUntil(ready_at) => Ok(self
                .wait_for_sender_loop_delay(ready_at.saturating_duration_since(now))
                .await),
            SenderLoopWait::Parked => {
                self.sender_loop_notify.notified().await;
                Ok(SenderWaitSignal::Notified)
            },
        }
    }

    pub(crate) fn next_wake_action(&self, now: std::time::Instant) -> SenderWakeAction {
        // Only stop dispatching when AT the in-flight capacity (Kafka pipelines up to
        // max.in.flight); previously this stopped at the first in-flight request,
        // serializing the sticky single-partition path to in-flight=1.
        if self.state.at_in_flight_capacity() {
            return SenderWakeAction::WaitForDispatch;
        }
        match self.next_ready_at(now) {
            Some(ready_at) if ready_at <= now => SenderWakeAction::DispatchReady,
            Some(ready_at) => SenderWakeAction::SleepUntil(ready_at),
            None => SenderWakeAction::Park,
        }
    }

    pub(crate) fn metrics_handle(&self) -> ProducerMetrics {
        self.dispatcher.metrics_handle()
    }

    pub(crate) fn control_dispatcher(&self) -> ProducerDispatcher {
        self.dispatcher.clone()
    }

    #[cfg(test)]
    pub(crate) const fn uses_sticky_partitioner(&self, record: &ProducerRecord) -> bool {
        self.dispatcher.uses_sticky_partitioner(record)
    }

    #[cfg(test)]
    pub(crate) async fn fail_if_transaction_error(&self) -> Result<(), ProducerError> {
        self.dispatcher.fail_if_transaction_error().await
    }

    pub(crate) async fn metadata_for_topics<I, S>(
        &self,
        topics: I,
    ) -> Result<Arc<crate::wire::ClusterMetadata>, ProducerError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.dispatcher.metadata_for_topics(topics).await
    }

    pub(crate) async fn assign_partition_with_accumulator(
        &self,
        record: &mut ProducerRecord,
    ) -> Result<(), ProducerError> {
        self.dispatcher
            .assign_partition_with_accumulator(&self.accumulator, record)
            .await
    }

    pub(crate) async fn assign_partition_with_metadata(
        &self,
        metadata: &crate::wire::ClusterMetadata,
        record: &mut ProducerRecord,
    ) -> Result<(), ProducerError> {
        self.dispatcher
            .assign_partition_with_metadata(metadata, record)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn refresh_partition_load_stats_with_metadata<I, S>(
        &self,
        metadata: &crate::wire::ClusterMetadata,
        topics: I,
    ) -> Result<(), ProducerError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.dispatcher
            .refresh_partition_load_stats_with_metadata(&self.accumulator, metadata, topics)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn refresh_topic_load_stats_with_metadata(
        &self,
        metadata: &crate::wire::ClusterMetadata,
        topic: &str,
    ) -> Result<(), ProducerError> {
        self.dispatcher
            .refresh_topic_load_stats_with_metadata(&self.accumulator, metadata, topic)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn refresh_and_assign_topic_partitions_with_metadata(
        &self,
        metadata: &crate::wire::ClusterMetadata,
        topic: &str,
        records: &mut [ProducerRecord],
        sticky: bool,
    ) -> Result<(), ProducerError> {
        self.refresh_topic_load_stats_with_metadata(metadata, topic)
            .await?;
        if sticky {
            self.dispatcher
                .assign_sticky_topic_partitions_with_metadata(metadata, topic, records)
                .await
        } else {
            self.dispatcher
                .assign_topic_partitions_with_metadata(metadata, topic, records)
                .await
        }
    }

    #[cfg(test)]
    pub(crate) async fn refresh_and_assign_partitions_with_metadata<I, S>(
        &self,
        metadata: &crate::wire::ClusterMetadata,
        topics: I,
        records: &mut [ProducerRecord],
    ) -> Result<(), ProducerError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.refresh_partition_load_stats_with_metadata(metadata, topics)
            .await?;
        self.dispatcher
            .assign_partitions_with_metadata(metadata, records)
            .await
    }

    #[cfg(test)]
    pub(crate) const fn append_poll_budget(&self) -> AppendPollBudget {
        self.state.append_poll_budget()
    }

    pub(crate) const fn callback_append_dispatch_decision(
        &mut self,
        status: AppendStatus,
    ) -> AppendDispatchDecision {
        self.state.callback_append_dispatch_decision(status)
    }

    pub(crate) fn discard_buffered_batches(&self) -> usize {
        ProducerSenderState::discard_buffered_batches(&self.accumulator)
    }

    /// Fail every buffered record's delivery with `error` and drop the batches,
    /// mirroring Kafka `RecordAccumulator.abortIncompleteBatches` on a forced
    /// close. Returns the number of batches aborted.
    pub(crate) fn fail_buffered_batches(&self, error: &ProducerError) -> usize {
        let mut batches = self.accumulator.discard_all();
        let count = batches.len();
        for batch in &mut batches {
            if let Some(delivery) = batch.delivery.take() {
                delivery.send_error(error);
            }
        }
        count
    }

    pub(crate) fn buffer_wait_action(
        &self,
        now: std::time::Instant,
        deadline: std::time::Instant,
    ) -> BufferWaitAction {
        match self.next_wake_action(now) {
            SenderWakeAction::WaitForDispatch => BufferWaitAction::WaitForDispatch,
            SenderWakeAction::DispatchReady => BufferWaitAction::PollReady,
            SenderWakeAction::SleepUntil(ready_at) => {
                if now >= deadline {
                    return BufferWaitAction::DeadlineElapsed;
                }
                let wait = ready_at
                    .saturating_duration_since(now)
                    .min(deadline.duration_since(now))
                    .min(std::time::Duration::from_millis(1));
                if wait.is_zero() {
                    return BufferWaitAction::PollReady;
                }
                BufferWaitAction::Sleep(wait)
            },
            SenderWakeAction::Park => {
                if now >= deadline {
                    return BufferWaitAction::DeadlineElapsed;
                }
                BufferWaitAction::Sleep(
                    deadline
                        .duration_since(now)
                        .min(std::time::Duration::from_millis(1)),
                )
            },
        }
    }

    #[cfg(test)]
    pub(crate) async fn drive_wake_step<LatencyObserver, RequeueObserver, BatchObserver>(
        &mut self,
        now: std::time::Instant,
        observers: ReadyDispatchObservers<LatencyObserver, RequeueObserver, BatchObserver>,
    ) -> Result<SenderWakeStep, ProducerError>
    where
        LatencyObserver: FnMut(std::time::Duration),
        RequeueObserver: FnMut(),
        BatchObserver: FnMut(&[ReadyBatch]),
    {
        let ReadyDispatchObservers {
            latency: mut observe_latency,
            requeue: mut observe_requeue,
            batches: mut observe_batches,
        } = observers;
        match self.next_wake_action(now) {
            SenderWakeAction::WaitForDispatch => {
                let _signal = self
                    .wait_for_sender_loop_wait(
                        SenderLoopWait::DispatchCompletion,
                        now,
                        &mut observe_latency,
                        &mut observe_requeue,
                    )
                    .await?;
                Ok(SenderWakeStep::WaitedForDispatch)
            },
            SenderWakeAction::DispatchReady => {
                let _dispatched = self
                    .state
                    .drive_ready_dispatch_until_blocked_with_policy(
                        &self.dispatcher,
                        &self.accumulator,
                        false,
                        ReadyDispatchObservers::new(
                            &mut observe_latency,
                            &mut observe_requeue,
                            &mut observe_batches,
                        ),
                    )
                    .await?;
                Ok(SenderWakeStep::DispatchedReady)
            },
            SenderWakeAction::SleepUntil(ready_at) => Ok(SenderWakeStep::SleepUntil(ready_at)),
            SenderWakeAction::Park => Ok(SenderWakeStep::Parked),
        }
    }

    pub(crate) async fn drive_wake_until_waiting<LatencyObserver, RequeueObserver, BatchObserver>(
        &mut self,
        now: std::time::Instant,
        observers: ReadyDispatchObservers<LatencyObserver, RequeueObserver, BatchObserver>,
    ) -> Result<SenderLoopWait, ProducerError>
    where
        LatencyObserver: FnMut(std::time::Duration),
        RequeueObserver: FnMut(),
        BatchObserver: FnMut(&[ReadyBatch]),
    {
        let ReadyDispatchObservers {
            latency: mut observe_latency,
            requeue: mut observe_requeue,
            batches: mut observe_batches,
        } = observers;
        loop {
            let mut requeued = false;
            self.state.handle_finished_dispatches(
                &self.accumulator,
                false,
                &mut observe_latency,
                || {
                    requeued = true;
                    observe_requeue();
                },
            )?;
            if requeued {
                self.pause_background_dispatch_after_requeue();
            }
            if self.background_dispatch_paused.load(Ordering::Relaxed) {
                // A requeue means the batches' leaders were unroutable
                // (metadata refresh failed — e.g. every broker down). Parking
                // until the next append would deadlock any caller applying
                // backpressure (flush, close, a bounded in-flight window): no
                // append ever comes, the requeued batches are never
                // re-drained, and the drain-time delivery-timeout check never
                // runs — their delivery futures would hang forever. Retry on
                // the retry backoff instead: each pass re-attempts metadata +
                // dispatch, and the drain expires batches past
                // `delivery.timeout.ms` so a sustained outage fails loudly
                // and a restored cluster resumes without waiting for traffic.
                self.background_dispatch_paused
                    .store(false, Ordering::Relaxed);
                return Ok(SenderLoopWait::SleepUntil(
                    now + self.dispatcher.retry_backoff_initial(),
                ));
            }
            match self.next_wake_action(now) {
                SenderWakeAction::WaitForDispatch => {
                    return Ok(SenderLoopWait::DispatchCompletion);
                },
                SenderWakeAction::DispatchReady => {
                    let dispatched_any = self
                        .state
                        .drive_ready_dispatch_until_blocked_with_policy(
                            &self.dispatcher,
                            &self.accumulator,
                            false,
                            ReadyDispatchObservers::new(
                                &mut observe_latency,
                                &mut observe_requeue,
                                &mut observe_batches,
                            ),
                        )
                        .await?;
                    // Ready batches existed but none were dispatchable (each is for a
                    // partition that already has an in-flight request). Wait for a
                    // completion to free a partition instead of re-polling forever.
                    if !dispatched_any {
                        return Ok(if self.state.has_in_flight_dispatches() {
                            SenderLoopWait::DispatchCompletion
                        } else {
                            SenderLoopWait::Parked
                        });
                    }
                },
                SenderWakeAction::SleepUntil(ready_at) => {
                    return Ok(SenderLoopWait::SleepUntil(ready_at));
                },
                SenderWakeAction::Park => return Ok(SenderLoopWait::Parked),
            }
        }
    }

    pub(crate) async fn drive_sender_loop_once<LatencyObserver, RequeueObserver, BatchObserver>(
        &mut self,
        now: std::time::Instant,
        observers: ReadyDispatchObservers<LatencyObserver, RequeueObserver, BatchObserver>,
    ) -> Result<SenderWaitSignal, ProducerError>
    where
        LatencyObserver: FnMut(std::time::Duration),
        RequeueObserver: FnMut(),
        BatchObserver: FnMut(&[ReadyBatch]),
    {
        let ReadyDispatchObservers {
            latency: mut observe_latency,
            requeue: mut observe_requeue,
            batches: mut observe_batches,
        } = observers;
        let wait = self
            .drive_wake_until_waiting(
                now,
                ReadyDispatchObservers::new(
                    &mut observe_latency,
                    &mut observe_requeue,
                    &mut observe_batches,
                ),
            )
            .await?;
        self.wait_for_sender_loop_wait(wait, now, observe_latency, observe_requeue)
            .await
    }

    pub(crate) async fn wait_for_buffer_progress<LatencyObserver, RequeueObserver, BatchObserver>(
        &mut self,
        deadline: std::time::Instant,
        observers: ReadyDispatchObservers<LatencyObserver, RequeueObserver, BatchObserver>,
    ) -> Result<(), ProducerError>
    where
        LatencyObserver: FnMut(std::time::Duration),
        RequeueObserver: FnMut(),
        BatchObserver: FnMut(&[ReadyBatch]),
    {
        let ReadyDispatchObservers {
            latency: mut observe_latency,
            requeue: mut observe_requeue,
            batches: mut observe_batches,
        } = observers;
        loop {
            let now = std::time::Instant::now();
            match self.buffer_wait_action(now, deadline) {
                BufferWaitAction::WaitForDispatch => {
                    let _signal = self
                        .drive_sender_loop_once(
                            now,
                            ReadyDispatchObservers::new(
                                &mut observe_latency,
                                &mut observe_requeue,
                                &mut observe_batches,
                            ),
                        )
                        .await?;
                    return Ok(());
                },
                BufferWaitAction::PollReady => {
                    let _wait = self
                        .drive_wake_until_waiting(
                            now,
                            ReadyDispatchObservers::new(
                                &mut observe_latency,
                                &mut observe_requeue,
                                &mut observe_batches,
                            ),
                        )
                        .await?;
                    return Ok(());
                },
                BufferWaitAction::Sleep(wait) => {
                    let _signal = self.wait_for_sender_loop_delay(wait).await;
                },
                BufferWaitAction::DeadlineElapsed => return Ok(()),
            }
        }
    }

    pub(crate) async fn wait_for_append_capacity<LatencyObserver, RequeueObserver, BatchObserver>(
        &mut self,
        record: &ProducerRecord,
        deadline: std::time::Instant,
        observers: ReadyDispatchObservers<LatencyObserver, RequeueObserver, BatchObserver>,
    ) -> Result<(), ProducerError>
    where
        LatencyObserver: FnMut(std::time::Duration),
        RequeueObserver: FnMut(),
        BatchObserver: FnMut(&[ReadyBatch]),
    {
        let ReadyDispatchObservers {
            latency: mut observe_latency,
            requeue: mut observe_requeue,
            batches: mut observe_batches,
        } = observers;
        let metrics = self.metrics_handle();
        let mut buffer_wait = None;
        let compression_ratio = self
            .dispatcher
            .compression_ratio_estimation(record.topic.as_ref());
        loop {
            match self
                .state
                .append_backpressure_action_with_compression_ratio(
                    &self.accumulator,
                    record,
                    std::time::Instant::now(),
                    deadline,
                    compression_ratio,
                ) {
                AppendBackpressureAction::Append => return Ok(()),
                AppendBackpressureAction::WaitForBuffer => {
                    if buffer_wait.is_none() {
                        buffer_wait = Some(metrics.start_buffer_wait());
                    }
                    self.wait_for_buffer_progress(
                        deadline,
                        ReadyDispatchObservers::new(
                            &mut observe_latency,
                            &mut observe_requeue,
                            &mut observe_batches,
                        ),
                    )
                    .await?;
                },
                AppendBackpressureAction::Backpressure => return Err(ProducerError::Backpressure),
            }
        }
    }

    pub(crate) async fn wait_for_abort_completion<LatencyObserver, RequeueObserver>(
        &mut self,
        observe_latency: LatencyObserver,
        observe_requeue: RequeueObserver,
    ) -> Result<(), ProducerError>
    where
        LatencyObserver: FnMut(std::time::Duration),
        RequeueObserver: FnMut(),
    {
        self.state
            .wait_for_abort_completion(&self.accumulator, observe_latency, observe_requeue)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn append_untracked_record_with_capacity_wait<
        LatencyObserver,
        RequeueObserver,
        BatchObserver,
    >(
        &mut self,
        record: ProducerRecord,
        now: std::time::Instant,
        deadline: std::time::Instant,
        observers: ReadyDispatchObservers<LatencyObserver, RequeueObserver, BatchObserver>,
    ) -> Result<AppendStatus, ProducerError>
    where
        LatencyObserver: FnMut(std::time::Duration),
        RequeueObserver: FnMut(),
        BatchObserver: FnMut(&[ReadyBatch]),
    {
        self.wait_for_append_capacity(&record, deadline, observers)
            .await?;
        let compression_ratio = self
            .dispatcher
            .compression_ratio_estimation(record.topic.as_ref());
        let status = self.accumulator.append_with_status_at_compression_ratio(
            record,
            now,
            compression_ratio,
        )?;
        self.note_append_status_for_sender_loop(status);
        Ok(status)
    }

    #[cfg(test)]
    pub(crate) async fn append_untracked_record_then_apply_batch_status<
        LatencyObserver,
        RequeueObserver,
        BatchObserver,
    >(
        &mut self,
        append: AppendUntrackedBatchApply<'_>,
        observers: ReadyDispatchObservers<LatencyObserver, RequeueObserver, BatchObserver>,
    ) -> Result<(), ProducerError>
    where
        LatencyObserver: FnMut(std::time::Duration),
        RequeueObserver: FnMut(),
        BatchObserver: FnMut(&[ReadyBatch]),
    {
        let ReadyDispatchObservers {
            latency: mut observe_latency,
            requeue: mut observe_requeue,
            batches: mut observe_batches,
        } = observers;
        let AppendUntrackedBatchApply {
            budget,
            append,
            sticky_topic,
        } = append;
        let AppendUntrackedRecord {
            record,
            now,
            deadline,
        } = append;
        let status = self
            .append_untracked_record_with_capacity_wait(
                record,
                now,
                deadline,
                ReadyDispatchObservers::new(
                    &mut observe_latency,
                    &mut observe_requeue,
                    &mut observe_batches,
                ),
            )
            .await?;
        self.apply_batch_append_status(
            budget,
            status,
            sticky_topic,
            ReadyDispatchObservers::new(observe_latency, observe_requeue, observe_batches),
        )
        .await
    }

    pub(crate) async fn append_delivery_record_with_capacity_wait<
        LatencyObserver,
        RequeueObserver,
        BatchObserver,
    >(
        &mut self,
        record: ProducerRecord,
        now: std::time::Instant,
        deadline: std::time::Instant,
        observers: ReadyDispatchObservers<LatencyObserver, RequeueObserver, BatchObserver>,
    ) -> Result<(SendFuture, AppendStatus), ProducerError>
    where
        LatencyObserver: FnMut(std::time::Duration),
        RequeueObserver: FnMut(),
        BatchObserver: FnMut(&[ReadyBatch]),
    {
        self.wait_for_append_capacity(&record, deadline, observers)
            .await?;
        let compression_ratio = self
            .dispatcher
            .compression_ratio_estimation(record.topic.as_ref());
        let appended = self
            .accumulator
            .append_for_delivery_with_status_at_compression_ratio(record, now, compression_ratio)?;
        self.note_append_status_for_sender_loop(appended.1);
        Ok(appended)
    }

    pub(crate) async fn append_callback_delivery_record_with_capacity_wait<
        LatencyObserver,
        RequeueObserver,
        BatchObserver,
    >(
        &mut self,
        record: ProducerRecord,
        now: std::time::Instant,
        deadline: std::time::Instant,
        observers: ReadyDispatchObservers<LatencyObserver, RequeueObserver, BatchObserver>,
    ) -> Result<(SendFuture, AppendDispatchDecision), ProducerError>
    where
        LatencyObserver: FnMut(std::time::Duration),
        RequeueObserver: FnMut(),
        BatchObserver: FnMut(&[ReadyBatch]),
    {
        let (delivery, status) = self
            .append_delivery_record_with_capacity_wait(record, now, deadline, observers)
            .await?;
        let decision = self.callback_append_dispatch_decision(status);
        Ok((delivery, decision))
    }

    pub(crate) async fn append_callback_delivery_record_then_apply_dispatch<
        LatencyObserver,
        RequeueObserver,
        BatchObserver,
        BeforeDispatch,
    >(
        &mut self,
        append: AppendCallbackDeliveryRecord<'_>,
        before_dispatch: BeforeDispatch,
        observers: ReadyDispatchObservers<LatencyObserver, RequeueObserver, BatchObserver>,
    ) -> Result<SendFuture, ProducerError>
    where
        LatencyObserver: FnMut(std::time::Duration),
        RequeueObserver: FnMut(),
        BatchObserver: FnMut(&[ReadyBatch]),
        BeforeDispatch: FnOnce(&SendFuture),
    {
        let ReadyDispatchObservers {
            latency: mut observe_latency,
            requeue: mut observe_requeue,
            batches: mut observe_batches,
        } = observers;
        let AppendCallbackDeliveryRecord {
            record,
            now,
            deadline,
            sticky_topic,
        } = append;
        let (delivery, decision) = self
            .append_callback_delivery_record_with_capacity_wait(
                record,
                now,
                deadline,
                ReadyDispatchObservers::new(
                    &mut observe_latency,
                    &mut observe_requeue,
                    &mut observe_batches,
                ),
            )
            .await?;
        before_dispatch(&delivery);
        self.apply_append_dispatch_decision_then_collect_finished(
            decision,
            sticky_topic,
            false,
            ReadyDispatchObservers::new(observe_latency, observe_requeue, observe_batches),
        )
        .await?;
        Ok(delivery)
    }

    #[cfg(test)]
    pub(crate) fn try_append_callback_delivery_record(
        &mut self,
        record: ProducerRecord,
        now: std::time::Instant,
    ) -> CallbackAppendFastPath {
        let compression_ratio = self
            .dispatcher
            .compression_ratio_estimation(record.topic.as_ref());
        if !self.state.has_available_memory_for_with_compression_ratio(
            &self.accumulator,
            &record,
            compression_ratio,
        ) {
            return CallbackAppendFastPath::WouldBlock(record);
        }
        let appended = self
            .accumulator
            .append_for_delivery_with_status_at_compression_ratio(record, now, compression_ratio);
        CallbackAppendFastPath::Appended(appended.map(|(delivery, status)| {
            self.note_append_status_for_sender_loop(status);
            let decision = self.callback_append_dispatch_decision(status);
            (delivery, decision)
        }))
    }

    pub(crate) async fn apply_append_dispatch_decision_then_collect_finished<
        LatencyObserver,
        RequeueObserver,
        BatchObserver,
    >(
        &mut self,
        decision: AppendDispatchDecision,
        sticky_topic: Option<&str>,
        requeue_is_error: bool,
        observers: ReadyDispatchObservers<LatencyObserver, RequeueObserver, BatchObserver>,
    ) -> Result<(), ProducerError>
    where
        LatencyObserver: FnMut(std::time::Duration),
        RequeueObserver: FnMut(),
        BatchObserver: FnMut(&[ReadyBatch]),
    {
        self.state
            .apply_append_dispatch_decision_then_collect_finished(
                &self.accumulator,
                AppendDispatchApplication::new(&self.dispatcher, decision, sticky_topic),
                requeue_is_error,
                observers,
            )
            .await
    }

    #[cfg(test)]
    pub(crate) async fn finish_batch_append_dispatch<
        LatencyObserver,
        RequeueObserver,
        BatchObserver,
    >(
        &mut self,
        observers: ReadyDispatchObservers<LatencyObserver, RequeueObserver, BatchObserver>,
    ) -> Result<(), ProducerError>
    where
        LatencyObserver: FnMut(std::time::Duration),
        RequeueObserver: FnMut(),
        BatchObserver: FnMut(&[ReadyBatch]),
    {
        self.state
            .finish_batch_append_dispatch(&self.dispatcher, &self.accumulator, observers)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn apply_batch_append_status<LatencyObserver, RequeueObserver, BatchObserver>(
        &mut self,
        budget: &mut AppendPollBudget,
        status: AppendStatus,
        sticky_topic: Option<&str>,
        observers: ReadyDispatchObservers<LatencyObserver, RequeueObserver, BatchObserver>,
    ) -> Result<(), ProducerError>
    where
        LatencyObserver: FnMut(std::time::Duration),
        RequeueObserver: FnMut(),
        BatchObserver: FnMut(&[ReadyBatch]),
    {
        self.state
            .apply_batch_append_status(
                &self.accumulator,
                budget,
                BatchAppendStatusApplication::new(&self.dispatcher, status, sticky_topic),
                observers,
            )
            .await
    }

    #[cfg(test)]
    pub(crate) async fn drive_ready_dispatch_until_blocked<
        LatencyObserver,
        RequeueObserver,
        BatchObserver,
    >(
        &mut self,
        observers: ReadyDispatchObservers<LatencyObserver, RequeueObserver, BatchObserver>,
    ) -> Result<bool, ProducerError>
    where
        LatencyObserver: FnMut(std::time::Duration),
        RequeueObserver: FnMut(),
        BatchObserver: FnMut(&[ReadyBatch]),
    {
        self.state
            .drive_ready_dispatch_until_blocked(&self.dispatcher, &self.accumulator, observers)
            .await
    }

    pub(crate) async fn drive_flush_until_complete<
        LatencyObserver,
        RequeueObserver,
        BatchObserver,
    >(
        &mut self,
        observers: ReadyDispatchObservers<LatencyObserver, RequeueObserver, BatchObserver>,
    ) -> Result<(), ProducerError>
    where
        LatencyObserver: FnMut(std::time::Duration),
        RequeueObserver: FnMut(),
        BatchObserver: FnMut(&[ReadyBatch]),
    {
        let ReadyDispatchObservers {
            latency,
            mut requeue,
            batches,
        } = observers;
        let mut background_dispatch_paused = false;
        let result = self
            .state
            .drive_flush_until_complete(
                &self.dispatcher,
                &self.accumulator,
                ReadyDispatchObservers::new(
                    latency,
                    || {
                        background_dispatch_paused = true;
                        requeue();
                    },
                    batches,
                ),
            )
            .await;
        if background_dispatch_paused
            || (result.is_err() && self.accumulator.buffered_records() > 0)
        {
            self.pause_background_dispatch_after_requeue();
        }
        result
    }

    #[cfg(test)]
    pub(crate) fn handle_completed_dispatch<LatencyObserver, RequeueObserver>(
        &self,
        completed: CompletedDispatch,
        observe_latency: LatencyObserver,
        observe_requeue: RequeueObserver,
    ) -> Result<(), ProducerError>
    where
        LatencyObserver: FnMut(std::time::Duration),
        RequeueObserver: FnMut(),
    {
        ProducerSenderState::handle_completed_dispatch(
            &self.accumulator,
            completed,
            observe_latency,
            observe_requeue,
        )
    }

    #[cfg(test)]
    pub(crate) fn handle_finished_dispatches<LatencyObserver, RequeueObserver>(
        &mut self,
        requeue_is_error: bool,
        observe_latency: LatencyObserver,
        observe_requeue: RequeueObserver,
    ) -> Result<(), ProducerError>
    where
        LatencyObserver: FnMut(std::time::Duration),
        RequeueObserver: FnMut(),
    {
        self.state.handle_finished_dispatches(
            &self.accumulator,
            requeue_is_error,
            observe_latency,
            observe_requeue,
        )
    }

    #[cfg(test)]
    pub(crate) async fn wait_for_handled_dispatch<LatencyObserver, RequeueObserver>(
        &mut self,
        requeue_is_error: bool,
        observe_latency: LatencyObserver,
        observe_requeue: RequeueObserver,
    ) -> Result<(), ProducerError>
    where
        LatencyObserver: FnMut(std::time::Duration),
        RequeueObserver: FnMut(),
    {
        self.state
            .wait_for_handled_dispatch(
                &self.accumulator,
                requeue_is_error,
                observe_latency,
                observe_requeue,
            )
            .await
    }
}

#[derive(Debug)]
pub(crate) struct TimedDispatchOutcome {
    pub(crate) outcome: DispatchOutcome,
    pub(crate) latency: std::time::Duration,
    pub(crate) partitions: Vec<InFlightPartitionKey>,
}

#[derive(Debug)]
struct InFlightDispatchReservation {
    partitions: Vec<InFlightPartitionKey>,
    identities: Vec<ReadyBatchIdentity>,
    bytes: usize,
    incomplete_batches: usize,
}

#[derive(Debug, Clone, Copy, Default)]
struct InFlightDispatchAccounting {
    bytes: usize,
    incomplete_batches: usize,
}

/// Per-dispatch reservation payload registered when a dispatch task is spawned: the
/// idempotent base sequences it puts in flight, the batch identities it owns, and its
/// buffer accounting. Bundled so spawning stays within the argument-count budget.
#[derive(Debug, Default)]
struct DispatchReservationInputs {
    identities: Vec<ReadyBatchIdentity>,
    accounting: InFlightDispatchAccounting,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct InFlightPartitionKey {
    topic: String,
    partition: i32,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct SenderQueueSnapshot {
    pub(crate) buffered_bytes: usize,
    pub(crate) buffered_records: usize,
    pub(crate) buffer_available_bytes: usize,
    pub(crate) incomplete_batches: usize,
    pub(crate) in_flight_dispatches: usize,
}

#[derive(Debug)]
pub(crate) struct DispatchSelection {
    pub(crate) dispatchable: Vec<ReadyBatch>,
    pub(crate) deferred: Vec<ReadyBatch>,
    pub(crate) partitions: Vec<InFlightPartitionKey>,
}

#[derive(Debug)]
pub(crate) struct DispatchPrepareError {
    pub(crate) error: ProducerError,
    pub(crate) batches: Vec<ReadyBatch>,
}

#[derive(Debug)]
pub(crate) struct CompletedDispatch {
    result: Result<TimedDispatchOutcome, ProducerError>,
    requeue_is_error: bool,
}

impl CompletedDispatch {
    pub(crate) const fn new(
        result: Result<TimedDispatchOutcome, ProducerError>,
        requeue_is_error: bool,
    ) -> Self {
        Self {
            result,
            requeue_is_error,
        }
    }
}

#[derive(Debug)]
pub(crate) struct DispatchSelectionStart {
    dispatcher: ProducerDispatcher,
    selection: DispatchSelection,
    now: std::time::Instant,
}

impl DispatchSelectionStart {
    pub(crate) const fn new(
        dispatcher: ProducerDispatcher,
        selection: DispatchSelection,
        now: std::time::Instant,
    ) -> Self {
        Self {
            dispatcher,
            selection,
            now,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DispatchStart {
    Empty,
    Spawned,
}

#[derive(Debug)]
pub(crate) struct DrainedDispatch {
    dispatcher: ProducerDispatcher,
    batches: Vec<ReadyBatch>,
    now: std::time::Instant,
    partitions: Vec<InFlightPartitionKey>,
}

impl DrainedDispatch {
    pub(crate) const fn new(
        dispatcher: ProducerDispatcher,
        batches: Vec<ReadyBatch>,
        now: std::time::Instant,
        partitions: Vec<InFlightPartitionKey>,
    ) -> Self {
        Self {
            dispatcher,
            batches,
            now,
            partitions,
        }
    }
}

#[cfg(test)]
#[derive(Debug, Clone, Copy)]
pub(crate) struct AppendCapacityWait<'a> {
    dispatcher: &'a ProducerDispatcher,
    record: &'a ProducerRecord,
    deadline: std::time::Instant,
}

#[cfg(test)]
impl<'a> AppendCapacityWait<'a> {
    pub(crate) const fn new(
        dispatcher: &'a ProducerDispatcher,
        record: &'a ProducerRecord,
        deadline: std::time::Instant,
    ) -> Self {
        Self {
            dispatcher,
            record,
            deadline,
        }
    }
}

#[cfg(test)]
#[derive(Debug)]
pub(crate) struct AppendUntracked<'a> {
    dispatcher: &'a ProducerDispatcher,
    record: ProducerRecord,
    now: std::time::Instant,
    deadline: std::time::Instant,
}

#[cfg(test)]
impl<'a> AppendUntracked<'a> {
    pub(crate) const fn new(
        dispatcher: &'a ProducerDispatcher,
        record: ProducerRecord,
        now: std::time::Instant,
        deadline: std::time::Instant,
    ) -> Self {
        Self {
            dispatcher,
            record,
            now,
            deadline,
        }
    }
}

#[cfg(test)]
#[derive(Debug)]
pub(crate) struct AppendDelivery<'a> {
    dispatcher: &'a ProducerDispatcher,
    record: ProducerRecord,
    now: std::time::Instant,
    deadline: std::time::Instant,
}

#[cfg(test)]
impl<'a> AppendDelivery<'a> {
    pub(crate) const fn new(
        dispatcher: &'a ProducerDispatcher,
        record: ProducerRecord,
        now: std::time::Instant,
        deadline: std::time::Instant,
    ) -> Self {
        Self {
            dispatcher,
            record,
            now,
            deadline,
        }
    }
}

#[cfg(test)]
#[derive(Debug)]
pub(crate) struct AppendUntrackedRecord {
    record: ProducerRecord,
    now: std::time::Instant,
    deadline: std::time::Instant,
}

#[cfg(test)]
impl AppendUntrackedRecord {
    pub(crate) const fn new(
        record: ProducerRecord,
        now: std::time::Instant,
        deadline: std::time::Instant,
    ) -> Self {
        Self {
            record,
            now,
            deadline,
        }
    }
}

#[cfg(test)]
#[derive(Debug)]
pub(crate) struct AppendUntrackedBatchApply<'a> {
    budget: &'a mut AppendPollBudget,
    append: AppendUntrackedRecord,
    sticky_topic: Option<&'a str>,
}

#[cfg(test)]
impl<'a> AppendUntrackedBatchApply<'a> {
    pub(crate) const fn new(
        budget: &'a mut AppendPollBudget,
        append: AppendUntrackedRecord,
        sticky_topic: Option<&'a str>,
    ) -> Self {
        Self {
            budget,
            append,
            sticky_topic,
        }
    }
}

#[derive(Debug)]
pub(crate) struct AppendCallbackDeliveryRecord<'a> {
    record: ProducerRecord,
    now: std::time::Instant,
    deadline: std::time::Instant,
    sticky_topic: Option<&'a str>,
}

impl<'a> AppendCallbackDeliveryRecord<'a> {
    pub(crate) const fn new(
        record: ProducerRecord,
        now: std::time::Instant,
        deadline: std::time::Instant,
        sticky_topic: Option<&'a str>,
    ) -> Self {
        Self {
            record,
            now,
            deadline,
            sticky_topic,
        }
    }
}

#[derive(Debug)]
pub(crate) enum ReadyDispatchSlot {
    Idle,
    Ready {
        completed: Option<Result<TimedDispatchOutcome, ProducerError>>,
    },
}

#[derive(Debug)]
pub(crate) enum PreparedReadyDispatch {
    Idle,
    PendingCompletion(Result<TimedDispatchOutcome, ProducerError>),
    Prepared(DispatchSelection),
}

#[derive(Debug)]
pub(crate) struct ReadyDispatchApplication {
    dispatcher: ProducerDispatcher,
    prepared: PreparedReadyDispatch,
    now: std::time::Instant,
}

impl ReadyDispatchApplication {
    pub(crate) const fn new(
        dispatcher: ProducerDispatcher,
        prepared: PreparedReadyDispatch,
        now: std::time::Instant,
    ) -> Self {
        Self {
            dispatcher,
            prepared,
            now,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReadyDispatchProgress {
    Idle,
    Continue,
    Started(DispatchStart),
}

#[derive(Debug)]
pub(crate) struct ReadyDispatchObservers<LatencyObserver, RequeueObserver, BatchObserver> {
    latency: LatencyObserver,
    requeue: RequeueObserver,
    batches: BatchObserver,
}

impl<LatencyObserver, RequeueObserver, BatchObserver>
    ReadyDispatchObservers<LatencyObserver, RequeueObserver, BatchObserver>
{
    pub(crate) const fn new(
        observe_latency: LatencyObserver,
        observe_requeue: RequeueObserver,
        observe_batches: BatchObserver,
    ) -> Self {
        Self {
            latency: observe_latency,
            requeue: observe_requeue,
            batches: observe_batches,
        }
    }
}

#[derive(Debug)]
pub(crate) enum PreparedAllDispatch {
    Empty,
    PendingCompletion(Result<TimedDispatchOutcome, ProducerError>),
    Prepared(DispatchSelection),
}

#[derive(Debug)]
pub(crate) struct AllDispatchApplication {
    dispatcher: ProducerDispatcher,
    prepared: PreparedAllDispatch,
    now: std::time::Instant,
}

impl AllDispatchApplication {
    pub(crate) const fn new(
        dispatcher: ProducerDispatcher,
        prepared: PreparedAllDispatch,
        now: std::time::Instant,
    ) -> Self {
        Self {
            dispatcher,
            prepared,
            now,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AllDispatchProgress {
    Empty,
    Continue,
    Started(DispatchStart),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FlushDispatchProgress {
    Complete,
    Continue,
    WaitForCompletion,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SenderWakeAction {
    WaitForDispatch,
    DispatchReady,
    SleepUntil(std::time::Instant),
    Park,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SenderWakeStep {
    WaitedForDispatch,
    DispatchedReady,
    SleepUntil(std::time::Instant),
    Parked,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SenderLoopWait {
    DispatchCompletion,
    SleepUntil(std::time::Instant),
    Parked,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SenderWaitSignal {
    DispatchCompleted,
    Notified,
    TimedOut,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BufferWaitAction {
    WaitForDispatch,
    PollReady,
    Sleep(std::time::Duration),
    DeadlineElapsed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AppendBackpressureAction {
    Append,
    WaitForBuffer,
    Backpressure,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AppendDispatchDecision {
    Idle,
    MarkBatchReady,
    DriveReady,
}

impl AppendDispatchDecision {
    const fn from_append_status(status: AppendStatus, should_drive: bool) -> Self {
        if !status.batch_ready {
            return Self::Idle;
        }
        if should_drive {
            return Self::DriveReady;
        }
        Self::MarkBatchReady
    }

    pub(crate) const fn should_mark_sticky_batch_ready(self) -> bool {
        matches!(self, Self::MarkBatchReady | Self::DriveReady)
    }

    pub(crate) const fn should_drive_ready_dispatch(self) -> bool {
        matches!(self, Self::DriveReady)
    }
}

#[cfg(test)]
pub(crate) enum CallbackAppendFastPath {
    Appended(Result<(SendFuture, AppendDispatchDecision), ProducerError>),
    WouldBlock(ProducerRecord),
}

/// Outcome of the synchronous callback-append fast path.
pub(crate) enum SyncCallbackAppend<'a, BeforeDispatch> {
    /// Appended without awaiting; `sticky_ready_topic` is set when the caller
    /// must async-mark a sealed sticky batch ready.
    Appended {
        delivery: SendFuture,
        sticky_ready_topic: Option<String>,
    },
    /// No buffer capacity — caller should take the awaiting slow path with the
    /// returned record and callback.
    WouldBlock(AppendCallbackDeliveryRecord<'a>, BeforeDispatch),
    /// The append itself failed.
    Failed(ProducerError),
}

#[derive(Debug)]
pub(crate) struct AppendDispatchApplication<'a> {
    dispatcher: &'a ProducerDispatcher,
    decision: AppendDispatchDecision,
    sticky_topic: Option<&'a str>,
}

impl<'a> AppendDispatchApplication<'a> {
    pub(crate) const fn new(
        dispatcher: &'a ProducerDispatcher,
        decision: AppendDispatchDecision,
        sticky_topic: Option<&'a str>,
    ) -> Self {
        Self {
            dispatcher,
            decision,
            sticky_topic,
        }
    }
}

#[cfg(test)]
#[derive(Debug)]
pub(crate) struct BatchAppendStatusApplication<'a> {
    dispatcher: &'a ProducerDispatcher,
    status: AppendStatus,
    sticky_topic: Option<&'a str>,
}

#[cfg(test)]
impl<'a> BatchAppendStatusApplication<'a> {
    pub(crate) const fn new(
        dispatcher: &'a ProducerDispatcher,
        status: AppendStatus,
        sticky_topic: Option<&'a str>,
    ) -> Self {
        Self {
            dispatcher,
            status,
            sticky_topic,
        }
    }
}

const DENSE_READY_BATCH_RECORDS: usize = 32;
pub(crate) const CALLBACK_READY_BATCH_POLL_THRESHOLD: usize = 64;

#[derive(Debug)]
pub(crate) struct AppendPollBudget {
    ready_batches: usize,
    threshold: usize,
}

impl AppendPollBudget {
    const fn new(max_in_flight_requests: usize) -> Self {
        let threshold = if max_in_flight_requests == 0 {
            1
        } else {
            max_in_flight_requests
        };
        Self {
            ready_batches: 0,
            threshold,
        }
    }

    const fn for_callback_sender(max_in_flight_requests: usize) -> Self {
        let base = Self::new(max_in_flight_requests);
        let threshold = if base.threshold < CALLBACK_READY_BATCH_POLL_THRESHOLD {
            CALLBACK_READY_BATCH_POLL_THRESHOLD
        } else {
            base.threshold
        };
        Self {
            ready_batches: 0,
            threshold,
        }
    }

    const fn observe(&mut self, status: AppendStatus) -> bool {
        if !status.batch_ready {
            return false;
        }
        if status.ready_batch_records >= DENSE_READY_BATCH_RECORDS {
            self.ready_batches = 0;
            return true;
        }
        self.ready_batches = self.ready_batches.saturating_add(1);
        if self.ready_batches < self.threshold {
            return false;
        }
        self.ready_batches = 0;
        true
    }
}

impl ProducerSenderState {
    #[cfg(test)]
    pub(crate) fn new(max_in_flight_requests: usize) -> Self {
        Self::new_with_idempotent_ordering(max_in_flight_requests, false)
    }

    pub(crate) fn new_with_idempotent_ordering(
        max_in_flight_requests: usize,
        idempotent_ordering: bool,
    ) -> Self {
        let max_in_flight_requests = max_in_flight_requests.max(1);
        let callback_append_poll_budget =
            AppendPollBudget::for_callback_sender(max_in_flight_requests);
        Self {
            in_flight: JoinSet::new(),
            in_flight_partitions: AHashMap::new(),
            in_flight_batch_identities: AHashSet::new(),
            completed_batch_identities: AHashSet::new(),
            completed_batch_identity_order: VecDeque::new(),
            pending_accumulator_completions: Vec::new(),
            in_flight_reservations: AHashMap::new(),
            in_flight_buffered_bytes: 0,
            in_flight_incomplete_batches: 0,
            callback_append_poll_budget,
            max_in_flight_requests,
            idempotent_ordering: idempotent_ordering || max_in_flight_requests == 1,
            completion_notify: None,
        }
    }

    pub(crate) fn notify_on_dispatch_completion(&mut self, notify: Arc<Notify>) {
        self.completion_notify = Some(notify);
    }

    #[cfg(test)]
    pub(crate) const fn max_in_flight_requests(&self) -> usize {
        self.max_in_flight_requests
    }

    #[cfg(test)]
    pub(crate) const fn uses_idempotent_ordering(&self) -> bool {
        self.idempotent_ordering
    }

    #[cfg(test)]
    pub(crate) const fn append_poll_budget_for_max_in_flight(
        max_in_flight_requests: usize,
    ) -> AppendPollBudget {
        let max_in_flight_requests = if max_in_flight_requests == 0 {
            1
        } else {
            max_in_flight_requests
        };
        AppendPollBudget::new(max_in_flight_requests)
    }

    #[cfg(test)]
    pub(crate) const fn append_poll_budget(&self) -> AppendPollBudget {
        Self::append_poll_budget_for_max_in_flight(self.max_in_flight_requests)
    }

    #[cfg(test)]
    pub(crate) const fn observe_batch_append_status(
        budget: &mut AppendPollBudget,
        status: AppendStatus,
    ) -> bool {
        budget.observe(status)
    }

    pub(crate) const fn single_append_dispatch_decision(
        status: AppendStatus,
    ) -> AppendDispatchDecision {
        AppendDispatchDecision::from_append_status(status, status.batch_ready)
    }

    #[cfg(test)]
    pub(crate) const fn batch_append_dispatch_decision(
        budget: &mut AppendPollBudget,
        status: AppendStatus,
    ) -> AppendDispatchDecision {
        let should_drive = Self::observe_batch_append_status(budget, status);
        AppendDispatchDecision::from_append_status(status, should_drive)
    }

    pub(crate) const fn observe_callback_append_status(&mut self, status: AppendStatus) -> bool {
        self.callback_append_poll_budget.observe(status)
    }

    pub(crate) const fn callback_append_dispatch_decision(
        &mut self,
        status: AppendStatus,
    ) -> AppendDispatchDecision {
        let should_drive = self.observe_callback_append_status(status);
        AppendDispatchDecision::from_append_status(status, should_drive)
    }

    #[cfg(test)]
    pub(crate) async fn apply_append_dispatch_decision<
        LatencyObserver,
        RequeueObserver,
        BatchObserver,
    >(
        &mut self,
        accumulator: &SharedAccumulator,
        application: AppendDispatchApplication<'_>,
        observers: ReadyDispatchObservers<LatencyObserver, RequeueObserver, BatchObserver>,
    ) -> Result<(), ProducerError>
    where
        LatencyObserver: FnMut(std::time::Duration),
        RequeueObserver: FnMut(),
        BatchObserver: FnMut(&[ReadyBatch]),
    {
        let AppendDispatchApplication {
            dispatcher,
            decision,
            sticky_topic,
        } = application;
        if decision.should_mark_sticky_batch_ready()
            && let Some(topic) = sticky_topic
        {
            dispatcher.mark_sticky_batch_ready(topic).await;
        }
        if decision.should_drive_ready_dispatch() {
            let _dispatched = self
                .drive_ready_dispatch_until_blocked(dispatcher, accumulator, observers)
                .await?;
        }
        Ok(())
    }

    pub(crate) async fn apply_append_dispatch_decision_then_collect_finished<
        LatencyObserver,
        RequeueObserver,
        BatchObserver,
    >(
        &mut self,
        accumulator: &SharedAccumulator,
        application: AppendDispatchApplication<'_>,
        requeue_is_error: bool,
        observers: ReadyDispatchObservers<LatencyObserver, RequeueObserver, BatchObserver>,
    ) -> Result<(), ProducerError>
    where
        LatencyObserver: FnMut(std::time::Duration),
        RequeueObserver: FnMut(),
        BatchObserver: FnMut(&[ReadyBatch]),
    {
        let AppendDispatchApplication {
            dispatcher,
            decision,
            sticky_topic,
        } = application;
        let ReadyDispatchObservers {
            mut latency,
            mut requeue,
            batches,
        } = observers;
        if decision.should_mark_sticky_batch_ready()
            && let Some(topic) = sticky_topic
        {
            dispatcher.mark_sticky_batch_ready(topic).await;
        }
        if decision.should_drive_ready_dispatch() {
            let _dispatched = self
                .drive_ready_dispatch_until_blocked(
                    dispatcher,
                    accumulator,
                    ReadyDispatchObservers::new(&mut latency, &mut requeue, batches),
                )
                .await?;
        }
        self.handle_finished_dispatches(accumulator, requeue_is_error, latency, requeue)
    }

    #[cfg(test)]
    pub(crate) async fn finish_batch_append_dispatch<
        LatencyObserver,
        RequeueObserver,
        BatchObserver,
    >(
        &mut self,
        dispatcher: &ProducerDispatcher,
        accumulator: &SharedAccumulator,
        observers: ReadyDispatchObservers<LatencyObserver, RequeueObserver, BatchObserver>,
    ) -> Result<(), ProducerError>
    where
        LatencyObserver: FnMut(std::time::Duration),
        RequeueObserver: FnMut(),
        BatchObserver: FnMut(&[ReadyBatch]),
    {
        self.drive_ready_dispatch_until_blocked(dispatcher, accumulator, observers)
            .await
            .map(|_dispatched| ())
    }

    #[cfg(test)]
    pub(crate) async fn apply_batch_append_status<LatencyObserver, RequeueObserver, BatchObserver>(
        &mut self,
        accumulator: &SharedAccumulator,
        budget: &mut AppendPollBudget,
        application: BatchAppendStatusApplication<'_>,
        observers: ReadyDispatchObservers<LatencyObserver, RequeueObserver, BatchObserver>,
    ) -> Result<(), ProducerError>
    where
        LatencyObserver: FnMut(std::time::Duration),
        RequeueObserver: FnMut(),
        BatchObserver: FnMut(&[ReadyBatch]),
    {
        let BatchAppendStatusApplication {
            dispatcher,
            status,
            sticky_topic,
        } = application;
        let decision = Self::batch_append_dispatch_decision(budget, status);
        self.apply_append_dispatch_decision(
            accumulator,
            AppendDispatchApplication::new(dispatcher, decision, sticky_topic),
            observers,
        )
        .await
    }

    pub(crate) fn in_flight_dispatch_count(&self) -> usize {
        self.in_flight.len()
    }

    pub(crate) fn buffered_bytes(&self, accumulator: &SharedAccumulator) -> usize {
        accumulator
            .buffered_bytes()
            .saturating_add(self.in_flight_buffered_bytes)
    }

    pub(crate) fn buffer_available_bytes(&self, accumulator: &SharedAccumulator) -> usize {
        accumulator
            .buffer_memory()
            .saturating_sub(self.buffered_bytes(accumulator))
    }

    pub(crate) fn queue_snapshot(&self, accumulator: &SharedAccumulator) -> SenderQueueSnapshot {
        SenderQueueSnapshot {
            buffered_bytes: self.buffered_bytes(accumulator),
            buffered_records: accumulator.buffered_records(),
            buffer_available_bytes: self.buffer_available_bytes(accumulator),
            incomplete_batches: self.incomplete_batch_count(accumulator),
            in_flight_dispatches: self.in_flight_dispatch_count(),
        }
    }

    #[cfg(test)]
    pub(crate) fn in_flight_len(&self) -> usize {
        self.in_flight_dispatch_count()
    }

    pub(crate) fn has_pending_work(&self, accumulator: &SharedAccumulator) -> bool {
        self.incomplete_batch_count(accumulator) > 0 || !self.in_flight.is_empty()
    }

    pub(crate) fn incomplete_batch_count(&self, accumulator: &SharedAccumulator) -> usize {
        accumulator
            .buffered_batches()
            .saturating_add(self.in_flight_incomplete_batches)
    }

    #[cfg(test)]
    pub(crate) fn has_available_memory_for(
        &self,
        accumulator: &SharedAccumulator,
        record: &ProducerRecord,
    ) -> bool {
        self.has_available_memory_for_with_compression_ratio(accumulator, record, 1.0)
    }

    pub(crate) fn has_available_memory_for_with_compression_ratio(
        &self,
        accumulator: &SharedAccumulator,
        record: &ProducerRecord,
        compression_ratio: f32,
    ) -> bool {
        accumulator.has_available_memory_for_reserved_with_compression_ratio(
            record,
            self.in_flight_buffered_bytes,
            compression_ratio,
        )
    }

    pub(crate) fn discard_buffered_batches(accumulator: &SharedAccumulator) -> usize {
        accumulator.discard_all().len()
    }

    #[cfg(test)]
    pub(crate) fn append_backpressure_action(
        &self,
        accumulator: &SharedAccumulator,
        record: &ProducerRecord,
        now: std::time::Instant,
        deadline: std::time::Instant,
    ) -> AppendBackpressureAction {
        self.append_backpressure_action_with_compression_ratio(
            accumulator,
            record,
            now,
            deadline,
            1.0,
        )
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "Append capacity decisions keep record timing and compression estimate explicit \
                  on the hot path."
    )]
    pub(crate) fn append_backpressure_action_with_compression_ratio(
        &self,
        accumulator: &SharedAccumulator,
        record: &ProducerRecord,
        now: std::time::Instant,
        deadline: std::time::Instant,
        compression_ratio: f32,
    ) -> AppendBackpressureAction {
        if self.has_available_memory_for_with_compression_ratio(
            accumulator,
            record,
            compression_ratio,
        ) {
            return AppendBackpressureAction::Append;
        }
        if self.has_pending_work(accumulator) && now < deadline {
            return AppendBackpressureAction::WaitForBuffer;
        }
        AppendBackpressureAction::Backpressure
    }

    pub(crate) fn has_in_flight_dispatches(&self) -> bool {
        !self.in_flight.is_empty()
    }

    /// True when the connection is at its in-flight `ProduceRequest` capacity
    /// (`max.in.flight.requests.per.connection`). The loop should keep dispatching
    /// newly-ready batches until this, instead of stopping at the first in-flight.
    pub(crate) fn at_in_flight_capacity(&self) -> bool {
        self.in_flight_dispatch_count() >= self.max_in_flight_requests
    }

    pub(crate) fn flush_completion_progress(&self) -> FlushDispatchProgress {
        if self.has_in_flight_dispatches() {
            FlushDispatchProgress::WaitForCompletion
        } else {
            FlushDispatchProgress::Complete
        }
    }

    #[cfg(test)]
    pub(crate) fn buffer_wait_action(
        &self,
        accumulator: &SharedAccumulator,
        now: std::time::Instant,
        deadline: std::time::Instant,
    ) -> BufferWaitAction {
        if !self.in_flight.is_empty() {
            return BufferWaitAction::WaitForDispatch;
        }
        if now >= deadline {
            return BufferWaitAction::DeadlineElapsed;
        }
        let remaining = deadline.duration_since(now);
        let wait = accumulator
            .next_ready_at(now)
            .map_or(remaining, |ready_at| {
                ready_at.saturating_duration_since(now)
            })
            .min(remaining)
            .min(std::time::Duration::from_millis(1));
        if wait.is_zero() {
            return BufferWaitAction::PollReady;
        }
        BufferWaitAction::Sleep(wait)
    }

    #[cfg(test)]
    pub(crate) async fn wait_for_buffer_progress<LatencyObserver, RequeueObserver, BatchObserver>(
        &mut self,
        dispatcher: &ProducerDispatcher,
        accumulator: &SharedAccumulator,
        deadline: std::time::Instant,
        observers: ReadyDispatchObservers<LatencyObserver, RequeueObserver, BatchObserver>,
    ) -> Result<(), ProducerError>
    where
        LatencyObserver: FnMut(std::time::Duration),
        RequeueObserver: FnMut(),
        BatchObserver: FnMut(&[ReadyBatch]),
    {
        let ReadyDispatchObservers {
            latency: mut observe_latency,
            requeue: mut observe_requeue,
            batches: mut observe_batches,
        } = observers;
        match self.buffer_wait_action(accumulator, std::time::Instant::now(), deadline) {
            BufferWaitAction::WaitForDispatch => {
                self.wait_for_handled_dispatch(
                    accumulator,
                    false,
                    &mut observe_latency,
                    &mut observe_requeue,
                )
                .await?;
            },
            BufferWaitAction::PollReady => {
                let _dispatched = self
                    .drive_ready_dispatch_until_blocked(
                        dispatcher,
                        accumulator,
                        ReadyDispatchObservers::new(
                            &mut observe_latency,
                            &mut observe_requeue,
                            &mut observe_batches,
                        ),
                    )
                    .await?;
            },
            BufferWaitAction::Sleep(wait) => tokio::time::sleep(wait).await,
            BufferWaitAction::DeadlineElapsed => {},
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) async fn wait_for_append_capacity<LatencyObserver, RequeueObserver, BatchObserver>(
        &mut self,
        accumulator: &SharedAccumulator,
        wait: AppendCapacityWait<'_>,
        observers: ReadyDispatchObservers<LatencyObserver, RequeueObserver, BatchObserver>,
    ) -> Result<(), ProducerError>
    where
        LatencyObserver: FnMut(std::time::Duration),
        RequeueObserver: FnMut(),
        BatchObserver: FnMut(&[ReadyBatch]),
    {
        let ReadyDispatchObservers {
            latency: mut observe_latency,
            requeue: mut observe_requeue,
            batches: mut observe_batches,
        } = observers;
        let compression_ratio = wait
            .dispatcher
            .compression_ratio_estimation(wait.record.topic.as_ref());
        loop {
            match self.append_backpressure_action_with_compression_ratio(
                accumulator,
                wait.record,
                std::time::Instant::now(),
                wait.deadline,
                compression_ratio,
            ) {
                AppendBackpressureAction::Append => return Ok(()),
                AppendBackpressureAction::WaitForBuffer => {
                    self.wait_for_buffer_progress(
                        wait.dispatcher,
                        accumulator,
                        wait.deadline,
                        ReadyDispatchObservers::new(
                            &mut observe_latency,
                            &mut observe_requeue,
                            &mut observe_batches,
                        ),
                    )
                    .await?;
                },
                AppendBackpressureAction::Backpressure => return Err(ProducerError::Backpressure),
            }
        }
    }

    #[cfg(test)]
    pub(crate) async fn append_untracked_with_capacity_wait<
        LatencyObserver,
        RequeueObserver,
        BatchObserver,
    >(
        &mut self,
        accumulator: &SharedAccumulator,
        append: AppendUntracked<'_>,
        observers: ReadyDispatchObservers<LatencyObserver, RequeueObserver, BatchObserver>,
    ) -> Result<AppendStatus, ProducerError>
    where
        LatencyObserver: FnMut(std::time::Duration),
        RequeueObserver: FnMut(),
        BatchObserver: FnMut(&[ReadyBatch]),
    {
        let AppendUntracked {
            dispatcher,
            record,
            now,
            deadline,
        } = append;
        self.wait_for_append_capacity(
            accumulator,
            AppendCapacityWait::new(dispatcher, &record, deadline),
            observers,
        )
        .await?;
        let compression_ratio = dispatcher.compression_ratio_estimation(record.topic.as_ref());
        accumulator.append_with_status_at_compression_ratio(record, now, compression_ratio)
    }

    #[cfg(test)]
    pub(crate) async fn append_for_delivery_with_capacity_wait<
        LatencyObserver,
        RequeueObserver,
        BatchObserver,
    >(
        &mut self,
        accumulator: &SharedAccumulator,
        append: AppendDelivery<'_>,
        observers: ReadyDispatchObservers<LatencyObserver, RequeueObserver, BatchObserver>,
    ) -> Result<(SendFuture, AppendStatus), ProducerError>
    where
        LatencyObserver: FnMut(std::time::Duration),
        RequeueObserver: FnMut(),
        BatchObserver: FnMut(&[ReadyBatch]),
    {
        let AppendDelivery {
            dispatcher,
            record,
            now,
            deadline,
        } = append;
        self.wait_for_append_capacity(
            accumulator,
            AppendCapacityWait::new(dispatcher, &record, deadline),
            observers,
        )
        .await?;
        let compression_ratio = dispatcher.compression_ratio_estimation(record.topic.as_ref());
        accumulator.append_for_delivery_with_status_at_compression_ratio(
            record,
            now,
            compression_ratio,
        )
    }

    pub(crate) fn spawn_in_flight<F>(&mut self, task: F) -> AbortHandle
    where
        F: Future<Output = TimedDispatchOutcome> + Send + 'static,
    {
        let completion_notify = self.completion_notify.clone();
        self.in_flight.spawn(async move {
            let outcome = task.await;
            if let Some(notify) = completion_notify {
                notify.notify_one();
            }
            outcome
        })
    }

    #[cfg(test)]
    pub(crate) fn spawn_dispatch_task<F>(
        &mut self,
        partitions: &[InFlightPartitionKey],
        task: F,
    ) -> Result<AbortHandle, ProducerError>
    where
        F: Future<Output = TimedDispatchOutcome> + Send + 'static,
    {
        self.spawn_dispatch_task_with_buffered_bytes(
            partitions,
            DispatchReservationInputs::default(),
            task,
        )
    }

    fn spawn_dispatch_task_with_buffered_bytes<F>(
        &mut self,
        partitions: &[InFlightPartitionKey],
        inputs: DispatchReservationInputs,
        task: F,
    ) -> Result<AbortHandle, ProducerError>
    where
        F: Future<Output = TimedDispatchOutcome> + Send + 'static,
    {
        let DispatchReservationInputs {
            identities,
            accounting,
        } = inputs;
        self.reserve_in_flight_batch_identities(&identities)?;
        self.reserve_dispatch_partitions(partitions);
        let handle = self.spawn_in_flight(task);
        if !partitions.is_empty() || accounting.bytes > 0 || !identities.is_empty() {
            let previous = self.in_flight_reservations.insert(
                handle.id(),
                InFlightDispatchReservation {
                    partitions: partitions.to_vec(),
                    identities,
                    bytes: accounting.bytes,
                    incomplete_batches: accounting.incomplete_batches,
                },
            );
            if let Some(previous) = previous {
                self.release_in_flight_batch_identities(previous.identities);
                self.in_flight_buffered_bytes =
                    self.in_flight_buffered_bytes.saturating_sub(previous.bytes);
                self.in_flight_incomplete_batches = self
                    .in_flight_incomplete_batches
                    .saturating_sub(previous.incomplete_batches);
            }
            self.in_flight_buffered_bytes = self
                .in_flight_buffered_bytes
                .saturating_add(accounting.bytes);
            self.in_flight_incomplete_batches = self
                .in_flight_incomplete_batches
                .saturating_add(accounting.incomplete_batches);
        }
        Ok(handle)
    }

    #[cfg(test)]
    pub(crate) fn spawn_drained_dispatch(
        &mut self,
        dispatch: DrainedDispatch,
    ) -> Result<AbortHandle, ProducerError> {
        self.spawn_observed_drained_dispatch(dispatch, |_| {})
    }

    pub(crate) fn spawn_observed_drained_dispatch<F>(
        &mut self,
        dispatch: DrainedDispatch,
        observe_batches: F,
    ) -> Result<AbortHandle, ProducerError>
    where
        F: FnOnce(&[ReadyBatch]),
    {
        let DrainedDispatch {
            dispatcher,
            batches,
            now,
            partitions,
        } = dispatch;
        observe_batches(&batches);
        let reserved_partitions = partitions.clone();
        let buffered_bytes = batches.iter().fold(0usize, |bytes, batch| {
            bytes.saturating_add(batch.pooled_buffer_bytes())
        });
        let incomplete_batches = batches.len();
        let identities = batches.iter().map(|batch| batch.identity).collect();
        let started_at = batches
            .iter()
            .map(|batch| batch.first_append_at)
            .min()
            .unwrap_or(now);
        // Reserve the enqueue ticket in the single-threaded sender loop (before spawning the
        // concurrent task) so same-partition requests enqueue in ascending base-sequence
        // order even when several are in flight per partition (idempotent pipelining).
        let enqueue_ticket = dispatcher.next_enqueue_ticket();
        self.spawn_dispatch_task_with_buffered_bytes(
            &reserved_partitions,
            DispatchReservationInputs {
                identities,
                accounting: InFlightDispatchAccounting {
                    bytes: buffered_bytes,
                    incomplete_batches,
                },
            },
            async move {
                let outcome = dispatcher
                    .dispatch_drained(batches, now, enqueue_ticket)
                    .await;
                TimedDispatchOutcome {
                    outcome,
                    latency: started_at.elapsed(),
                    partitions,
                }
            },
        )
    }

    pub(crate) fn start_dispatch_selection<F>(
        &mut self,
        accumulator: &SharedAccumulator,
        start: DispatchSelectionStart,
        observe_batches: F,
    ) -> Result<DispatchStart, ProducerError>
    where
        F: FnOnce(&[ReadyBatch]),
    {
        let DispatchSelectionStart {
            dispatcher,
            selection,
            now,
        } = start;
        if !selection.deferred.is_empty() {
            accumulator.requeue_front(selection.deferred)?;
        }
        if selection.dispatchable.is_empty() {
            return Ok(DispatchStart::Empty);
        }
        let dispatch = DrainedDispatch::new(
            dispatcher,
            selection.dispatchable,
            now,
            selection.partitions,
        );
        let abort_handle = self.spawn_observed_drained_dispatch(dispatch, observe_batches)?;
        drop(abort_handle);
        Ok(DispatchStart::Spawned)
    }

    fn try_join_next(
        &mut self,
    ) -> Option<Result<(tokio::task::Id, TimedDispatchOutcome), JoinError>> {
        self.in_flight.try_join_next_with_id()
    }

    fn collect_finished_dispatches(&mut self) -> Vec<Result<TimedDispatchOutcome, JoinError>> {
        let mut completed = Vec::new();
        while let Some(result) = self.try_join_next() {
            completed.push(self.complete_joined_dispatch_with_id(result));
        }
        completed
    }

    pub(crate) fn collect_completed_dispatches(
        &mut self,
    ) -> Vec<Result<TimedDispatchOutcome, ProducerError>> {
        let completed = self.collect_finished_dispatches();
        completed
            .into_iter()
            .map(|result| result.map_err(|error| ProducerError::DispatchTask(error.to_string())))
            .collect()
    }

    async fn join_next(
        &mut self,
    ) -> Option<Result<(tokio::task::Id, TimedDispatchOutcome), JoinError>> {
        self.in_flight.join_next_with_id().await
    }

    #[cfg(test)]
    async fn join_next_without_id(&mut self) -> Option<Result<TimedDispatchOutcome, JoinError>> {
        self.in_flight.join_next().await
    }

    async fn wait_for_joined_dispatch_completion(
        &mut self,
    ) -> Option<Result<(tokio::task::Id, TimedDispatchOutcome), JoinError>> {
        self.join_next().await
    }

    #[cfg(test)]
    async fn wait_for_dispatch_completion(
        &mut self,
    ) -> Option<Result<TimedDispatchOutcome, JoinError>> {
        self.join_next_without_id().await
    }

    pub(crate) async fn wait_for_completed_dispatch(
        &mut self,
    ) -> Option<Result<TimedDispatchOutcome, ProducerError>> {
        let result = self.wait_for_joined_dispatch_completion().await?;
        Some(self.complete_dispatch_result_with_id(result))
    }

    #[cfg(test)]
    pub(crate) async fn wait_for_next_dispatch(
        &mut self,
    ) -> Option<Result<TimedDispatchOutcome, JoinError>> {
        self.join_next_without_id().await
    }

    #[cfg(test)]
    async fn wait_for_dispatch_slot(&mut self) -> Option<Result<TimedDispatchOutcome, JoinError>> {
        if self.in_flight.len() < self.max_in_flight_requests {
            return None;
        }
        self.join_next_without_id().await
    }

    pub(crate) async fn wait_for_completed_dispatch_slot(
        &mut self,
    ) -> Option<Result<TimedDispatchOutcome, ProducerError>> {
        if self.in_flight.len() < self.max_in_flight_requests {
            return None;
        }
        self.wait_for_completed_dispatch().await
    }

    pub(crate) async fn wait_for_ready_dispatch_slot(
        &mut self,
        accumulator: &SharedAccumulator,
        now: std::time::Instant,
    ) -> ReadyDispatchSlot {
        if !Self::has_ready_dispatch_batches(accumulator, now) {
            return ReadyDispatchSlot::Idle;
        }
        ReadyDispatchSlot::Ready {
            completed: self.wait_for_completed_dispatch_slot().await,
        }
    }

    #[cfg(test)]
    fn complete_joined_dispatch(
        &mut self,
        result: Result<TimedDispatchOutcome, JoinError>,
    ) -> Result<TimedDispatchOutcome, JoinError> {
        match &result {
            Ok(outcome) => {
                // Release the per-partition depth exactly once: via the matched reservations
                // when present, otherwise (tests that reserved depth directly) release here.
                let reservations =
                    self.release_in_flight_reservations_for_partitions(&outcome.partitions);
                if reservations.is_empty() {
                    self.release_dispatch_partitions(outcome.partitions.clone());
                } else if matches!(&outcome.outcome, DispatchOutcome::Delivered(_)) {
                    for reservation in reservations {
                        self.mark_completed_batch_identities(reservation.identities);
                    }
                }
            },
            Err(error) => {
                if let Some(reservation) = self.release_in_flight_reservation(error.id()) {
                    self.mark_completed_batch_identities(reservation.identities);
                }
            },
        }
        result
    }

    fn complete_joined_dispatch_with_id(
        &mut self,
        result: Result<(tokio::task::Id, TimedDispatchOutcome), JoinError>,
    ) -> Result<TimedDispatchOutcome, JoinError> {
        match result {
            Ok((task_id, outcome)) => {
                // `release_in_flight_reservation` already decrements the per-partition
                // in-flight depth via `release_dispatch_partitions`; releasing again here
                // would double-decrement and under-count when a partition has more than one
                // in-flight request (idempotent pipelining). Only fall back to a direct
                // release when no reservation was registered (defensive).
                match self.release_in_flight_reservation(task_id) {
                    Some(reservation) => {
                        if matches!(&outcome.outcome, DispatchOutcome::Delivered(_)) {
                            // In-flight base-sequence tracking lives in producer_state and is
                            // released by `dispatch_drained` on a terminal outcome; here we
                            // only mark the batch identities complete.
                            self.mark_completed_batch_identities(reservation.identities);
                        }
                    },
                    None => self.release_dispatch_partitions(outcome.partitions.clone()),
                }
                Ok(outcome)
            },
            Err(error) => {
                if let Some(reservation) = self.release_in_flight_reservation(error.id()) {
                    self.mark_completed_batch_identities(reservation.identities);
                }
                Err(error)
            },
        }
    }

    #[cfg(test)]
    pub(crate) fn complete_dispatch_result(
        &mut self,
        result: Result<TimedDispatchOutcome, JoinError>,
    ) -> Result<TimedDispatchOutcome, ProducerError> {
        self.complete_joined_dispatch(result)
            .map_err(|error| ProducerError::DispatchTask(error.to_string()))
    }

    fn complete_dispatch_result_with_id(
        &mut self,
        result: Result<(tokio::task::Id, TimedDispatchOutcome), JoinError>,
    ) -> Result<TimedDispatchOutcome, ProducerError> {
        self.complete_joined_dispatch_with_id(result)
            .map_err(|error| ProducerError::DispatchTask(error.to_string()))
    }

    pub(crate) fn handle_completed_dispatch<LatencyObserver, RequeueObserver>(
        accumulator: &SharedAccumulator,
        completed: CompletedDispatch,
        mut observe_latency: LatencyObserver,
        mut observe_requeue: RequeueObserver,
    ) -> Result<(), ProducerError>
    where
        LatencyObserver: FnMut(std::time::Duration),
        RequeueObserver: FnMut(),
    {
        let CompletedDispatch {
            result,
            requeue_is_error,
        } = completed;
        match result {
            Ok(TimedDispatchOutcome {
                outcome: DispatchOutcome::Delivered(result),
                latency,
                ..
            }) => {
                observe_latency(latency);
                result.map(|_receipts| ())
            },
            Ok(TimedDispatchOutcome {
                outcome: DispatchOutcome::Requeue(batches),
                ..
            }) => {
                observe_requeue();
                accumulator.requeue_front(batches)?;
                if requeue_is_error {
                    Err(ProducerError::FlushIncomplete)
                } else {
                    Ok(())
                }
            },
            Err(error) => Err(error),
        }
    }

    fn handle_completed_dispatch_with_lifecycle<LatencyObserver, RequeueObserver>(
        &mut self,
        accumulator: &SharedAccumulator,
        completed: CompletedDispatch,
        observe_latency: LatencyObserver,
        observe_requeue: RequeueObserver,
    ) -> Result<(), ProducerError>
    where
        LatencyObserver: FnMut(std::time::Duration),
        RequeueObserver: FnMut(),
    {
        self.complete_pending_accumulator_batches(accumulator);
        Self::handle_completed_dispatch(accumulator, completed, observe_latency, observe_requeue)
    }

    pub(crate) fn handle_finished_dispatches<LatencyObserver, RequeueObserver>(
        &mut self,
        accumulator: &SharedAccumulator,
        requeue_is_error: bool,
        mut observe_latency: LatencyObserver,
        mut observe_requeue: RequeueObserver,
    ) -> Result<(), ProducerError>
    where
        LatencyObserver: FnMut(std::time::Duration),
        RequeueObserver: FnMut(),
    {
        for result in self.collect_completed_dispatches() {
            self.handle_completed_dispatch_with_lifecycle(
                accumulator,
                CompletedDispatch::new(result, requeue_is_error),
                &mut observe_latency,
                &mut observe_requeue,
            )?;
        }
        Ok(())
    }

    /// Reap finished dispatches for the background sender pump. Unlike the
    /// flush / append paths, a per-batch `Delivered(Err)` is swallowed: the
    /// dispatch task already resolved every record's delivery future (success,
    /// or failure via `fail_deliveries`), so surfacing it here would abort the
    /// pump before it drains the other partitions — the permanent wedge one
    /// expired batch caused after an outage, where every buffered batch had
    /// aged past `delivery.timeout.ms` and each drained dispatch tripped the
    /// error again. Slot accounting already happened in
    /// `collect_completed_dispatches`; structural failures (requeue accounting,
    /// join panic) still propagate.
    fn reap_finished_dispatches_for_pump<LatencyObserver, RequeueObserver>(
        &mut self,
        accumulator: &SharedAccumulator,
        mut observe_latency: LatencyObserver,
        mut observe_requeue: RequeueObserver,
    ) -> Result<(), ProducerError>
    where
        LatencyObserver: FnMut(std::time::Duration),
        RequeueObserver: FnMut(),
    {
        for result in self.collect_completed_dispatches() {
            let delivered_error = matches!(
                &result,
                Ok(TimedDispatchOutcome {
                    outcome: DispatchOutcome::Delivered(Err(_)),
                    ..
                })
            );
            if delivered_error {
                if let Ok(TimedDispatchOutcome { latency, .. }) = result {
                    observe_latency(latency);
                }
                self.complete_pending_accumulator_batches(accumulator);
            } else {
                self.handle_completed_dispatch_with_lifecycle(
                    accumulator,
                    CompletedDispatch::new(result, false),
                    &mut observe_latency,
                    &mut observe_requeue,
                )?;
            }
        }
        Ok(())
    }

    pub(crate) async fn wait_for_handled_dispatch<LatencyObserver, RequeueObserver>(
        &mut self,
        accumulator: &SharedAccumulator,
        requeue_is_error: bool,
        observe_latency: LatencyObserver,
        observe_requeue: RequeueObserver,
    ) -> Result<(), ProducerError>
    where
        LatencyObserver: FnMut(std::time::Duration),
        RequeueObserver: FnMut(),
    {
        let Some(result) = self.wait_for_completed_dispatch().await else {
            return Ok(());
        };
        self.handle_completed_dispatch_with_lifecycle(
            accumulator,
            CompletedDispatch::new(result, requeue_is_error),
            observe_latency,
            observe_requeue,
        )
    }

    pub(crate) async fn wait_for_abort_completion<LatencyObserver, RequeueObserver>(
        &mut self,
        accumulator: &SharedAccumulator,
        mut observe_latency: LatencyObserver,
        mut observe_requeue: RequeueObserver,
    ) -> Result<(), ProducerError>
    where
        LatencyObserver: FnMut(std::time::Duration),
        RequeueObserver: FnMut(),
    {
        while self.flush_completion_progress() == FlushDispatchProgress::WaitForCompletion {
            let Some(result) = self.wait_for_completed_dispatch().await else {
                return Ok(());
            };
            match result {
                Ok(TimedDispatchOutcome {
                    outcome: DispatchOutcome::Delivered(result),
                    latency,
                    ..
                }) => {
                    self.complete_pending_accumulator_batches(accumulator);
                    observe_latency(latency);
                    result.map(|_receipts| ())?;
                },
                Ok(TimedDispatchOutcome {
                    outcome: DispatchOutcome::Requeue(batches),
                    ..
                }) => {
                    let identities = batches.iter().map(|batch| batch.identity);
                    let _completed = accumulator.complete_batch_identities(identities);
                    observe_requeue();
                    drop(batches);
                },
                Err(error) => {
                    self.complete_pending_accumulator_batches(accumulator);
                    return Err(error);
                },
            }
        }
        Ok(())
    }

    pub(crate) fn apply_ready_dispatch_progress<LatencyObserver, RequeueObserver, BatchObserver>(
        &mut self,
        accumulator: &SharedAccumulator,
        application: ReadyDispatchApplication,
        observers: ReadyDispatchObservers<LatencyObserver, RequeueObserver, BatchObserver>,
    ) -> Result<ReadyDispatchProgress, ProducerError>
    where
        LatencyObserver: FnMut(std::time::Duration),
        RequeueObserver: FnMut(),
        BatchObserver: FnOnce(&[ReadyBatch]),
    {
        let ReadyDispatchApplication {
            dispatcher,
            prepared,
            now,
        } = application;
        let ReadyDispatchObservers {
            latency: observe_latency,
            requeue: observe_requeue,
            batches: observe_batches,
        } = observers;
        match prepared {
            PreparedReadyDispatch::Idle => Ok(ReadyDispatchProgress::Idle),
            PreparedReadyDispatch::PendingCompletion(result) => {
                self.handle_completed_dispatch_with_lifecycle(
                    accumulator,
                    CompletedDispatch::new(result, false),
                    observe_latency,
                    observe_requeue,
                )?;
                Ok(ReadyDispatchProgress::Continue)
            },
            PreparedReadyDispatch::Prepared(selection) => {
                let start = DispatchSelectionStart::new(dispatcher, selection, now);
                let started = self.start_dispatch_selection(accumulator, start, observe_batches)?;
                Ok(ReadyDispatchProgress::Started(started))
            },
        }
    }

    pub(crate) async fn drive_ready_dispatch_progress<
        LatencyObserver,
        RequeueObserver,
        BatchObserver,
    >(
        &mut self,
        dispatcher: &ProducerDispatcher,
        accumulator: &SharedAccumulator,
        now: std::time::Instant,
        observers: ReadyDispatchObservers<LatencyObserver, RequeueObserver, BatchObserver>,
    ) -> Result<ReadyDispatchProgress, ProducerError>
    where
        LatencyObserver: FnMut(std::time::Duration),
        RequeueObserver: FnMut(),
        BatchObserver: FnOnce(&[ReadyBatch]),
    {
        let prepared = self
            .prepare_ready_dispatch_batches_or_requeue(dispatcher, accumulator, now)
            .await?;
        self.apply_ready_dispatch_progress(
            accumulator,
            ReadyDispatchApplication::new(dispatcher.clone(), prepared, now),
            observers,
        )
    }

    /// Returns `true` if at least one batch was dispatched. `false` means nothing
    /// was dispatchable right now (nothing ready, or every ready batch is for a
    /// partition that already has an in-flight request) — the caller must WAIT for
    /// an in-flight completion rather than re-poll, or it livelocks.
    pub(crate) async fn drive_ready_dispatch_until_blocked<
        LatencyObserver,
        RequeueObserver,
        BatchObserver,
    >(
        &mut self,
        dispatcher: &ProducerDispatcher,
        accumulator: &SharedAccumulator,
        observers: ReadyDispatchObservers<LatencyObserver, RequeueObserver, BatchObserver>,
    ) -> Result<bool, ProducerError>
    where
        LatencyObserver: FnMut(std::time::Duration),
        RequeueObserver: FnMut(),
        BatchObserver: FnMut(&[ReadyBatch]),
    {
        self.drive_ready_dispatch_until_blocked_with_policy(
            dispatcher,
            accumulator,
            true,
            observers,
        )
        .await
    }

    /// [`drive_ready_dispatch_until_blocked`](Self::drive_ready_dispatch_until_blocked)
    /// with control over whether a per-batch `Delivered(Err)` aborts the drive.
    /// The background sender pump passes `surface_delivered_errors = false` so a
    /// single expired batch cannot starve every other partition; the flush and
    /// append-capacity callers pass `true` so the awaiting caller still sees the
    /// failure.
    pub(crate) async fn drive_ready_dispatch_until_blocked_with_policy<
        LatencyObserver,
        RequeueObserver,
        BatchObserver,
    >(
        &mut self,
        dispatcher: &ProducerDispatcher,
        accumulator: &SharedAccumulator,
        surface_delivered_errors: bool,
        observers: ReadyDispatchObservers<LatencyObserver, RequeueObserver, BatchObserver>,
    ) -> Result<bool, ProducerError>
    where
        LatencyObserver: FnMut(std::time::Duration),
        RequeueObserver: FnMut(),
        BatchObserver: FnMut(&[ReadyBatch]),
    {
        let ReadyDispatchObservers {
            latency: mut observe_latency,
            requeue: mut observe_requeue,
            batches: mut observe_batches,
        } = observers;
        let mut dispatched_any = false;
        loop {
            if surface_delivered_errors {
                self.handle_finished_dispatches(
                    accumulator,
                    false,
                    &mut observe_latency,
                    &mut observe_requeue,
                )?;
            } else {
                self.reap_finished_dispatches_for_pump(
                    accumulator,
                    &mut observe_latency,
                    &mut observe_requeue,
                )?;
            }
            let progress = self
                .drive_ready_dispatch_progress(
                    dispatcher,
                    accumulator,
                    std::time::Instant::now(),
                    ReadyDispatchObservers::new(
                        &mut observe_latency,
                        &mut observe_requeue,
                        &mut observe_batches,
                    ),
                )
                .await?;
            match progress {
                ReadyDispatchProgress::Idle => return Ok(dispatched_any),
                ReadyDispatchProgress::Started(DispatchStart::Spawned) => return Ok(true),
                // Ready batches existed but every one was deferred back (its
                // partition is already at the in-flight depth cap). Reporting this
                // as "dispatched" would make the caller re-poll immediately and
                // livelock in a hot drain/defer/requeue spin for the whole
                // in-flight round trip; report it like Idle so the caller waits
                // for a completion.
                ReadyDispatchProgress::Started(DispatchStart::Empty) => {
                    return Ok(dispatched_any);
                },
                ReadyDispatchProgress::Continue => dispatched_any = true,
            }
        }
    }

    pub(crate) fn apply_all_dispatch_progress<LatencyObserver, RequeueObserver, BatchObserver>(
        &mut self,
        accumulator: &SharedAccumulator,
        application: AllDispatchApplication,
        observers: ReadyDispatchObservers<LatencyObserver, RequeueObserver, BatchObserver>,
    ) -> Result<AllDispatchProgress, ProducerError>
    where
        LatencyObserver: FnMut(std::time::Duration),
        RequeueObserver: FnMut(),
        BatchObserver: FnOnce(&[ReadyBatch]),
    {
        let AllDispatchApplication {
            dispatcher,
            prepared,
            now,
        } = application;
        let ReadyDispatchObservers {
            latency: observe_latency,
            requeue: observe_requeue,
            batches: observe_batches,
        } = observers;
        match prepared {
            PreparedAllDispatch::Empty => Ok(AllDispatchProgress::Empty),
            PreparedAllDispatch::PendingCompletion(result) => {
                // A retriable requeue is not a flush failure: the batch is put back on
                // the accumulator and re-dispatched by the surrounding flush loop (Kafka
                // flush() retries until delivery or delivery.timeout). `false` keeps this
                // consistent with the non-flush `apply_ready_dispatch_progress` path.
                self.handle_completed_dispatch_with_lifecycle(
                    accumulator,
                    CompletedDispatch::new(result, false),
                    observe_latency,
                    observe_requeue,
                )?;
                Ok(AllDispatchProgress::Continue)
            },
            PreparedAllDispatch::Prepared(selection) => {
                let start = DispatchSelectionStart::new(dispatcher, selection, now);
                let started = self.start_dispatch_selection(accumulator, start, observe_batches)?;
                Ok(AllDispatchProgress::Started(started))
            },
        }
    }

    pub(crate) async fn drive_all_dispatch_progress<
        LatencyObserver,
        RequeueObserver,
        BatchObserver,
    >(
        &mut self,
        dispatcher: &ProducerDispatcher,
        accumulator: &SharedAccumulator,
        now: std::time::Instant,
        observers: ReadyDispatchObservers<LatencyObserver, RequeueObserver, BatchObserver>,
    ) -> Result<AllDispatchProgress, ProducerError>
    where
        LatencyObserver: FnMut(std::time::Duration),
        RequeueObserver: FnMut(),
        BatchObserver: FnOnce(&[ReadyBatch]),
    {
        let prepared = self
            .prepare_all_dispatch_batches_or_requeue(dispatcher, accumulator)
            .await?;
        self.apply_all_dispatch_progress(
            accumulator,
            AllDispatchApplication::new(dispatcher.clone(), prepared, now),
            observers,
        )
    }

    pub(crate) fn apply_flush_dispatch_progress(
        &self,
        progress: AllDispatchProgress,
    ) -> Result<FlushDispatchProgress, ProducerError> {
        match progress {
            AllDispatchProgress::Empty => Ok(FlushDispatchProgress::Complete),
            AllDispatchProgress::Continue
            | AllDispatchProgress::Started(DispatchStart::Spawned) => {
                Ok(FlushDispatchProgress::Continue)
            },
            AllDispatchProgress::Started(DispatchStart::Empty) => {
                if self.has_in_flight_dispatches() {
                    Ok(FlushDispatchProgress::WaitForCompletion)
                } else {
                    Err(ProducerError::FlushIncomplete)
                }
            },
        }
    }

    #[cfg(test)]
    pub(crate) async fn drive_flush_dispatch_progress<
        LatencyObserver,
        RequeueObserver,
        BatchObserver,
    >(
        &mut self,
        dispatcher: &ProducerDispatcher,
        accumulator: &SharedAccumulator,
        now: std::time::Instant,
        observers: ReadyDispatchObservers<LatencyObserver, RequeueObserver, BatchObserver>,
    ) -> Result<FlushDispatchProgress, ProducerError>
    where
        LatencyObserver: FnMut(std::time::Duration),
        RequeueObserver: FnMut(),
        BatchObserver: FnOnce(&[ReadyBatch]),
    {
        let progress = self
            .drive_all_dispatch_progress(dispatcher, accumulator, now, observers)
            .await?;
        self.apply_flush_dispatch_progress(progress)
    }

    pub(crate) async fn drive_flush_dispatch_step<LatencyObserver, RequeueObserver, BatchObserver>(
        &mut self,
        dispatcher: &ProducerDispatcher,
        accumulator: &SharedAccumulator,
        now: std::time::Instant,
        observers: ReadyDispatchObservers<LatencyObserver, RequeueObserver, BatchObserver>,
    ) -> Result<FlushDispatchProgress, ProducerError>
    where
        LatencyObserver: FnMut(std::time::Duration),
        RequeueObserver: FnMut(),
        BatchObserver: FnOnce(&[ReadyBatch]),
    {
        let ReadyDispatchObservers {
            latency: mut observe_latency,
            requeue: mut observe_requeue,
            batches: observe_batches,
        } = observers;
        let progress = self
            .drive_all_dispatch_progress(
                dispatcher,
                accumulator,
                now,
                ReadyDispatchObservers::new(
                    &mut observe_latency,
                    &mut observe_requeue,
                    observe_batches,
                ),
            )
            .await?;
        let flush_progress = self.apply_flush_dispatch_progress(progress)?;
        if flush_progress == FlushDispatchProgress::WaitForCompletion {
            // Tolerate a retriable requeue (re-dispatched by the outer flush loop)
            // rather than treating it as a terminal FlushIncomplete.
            self.wait_for_handled_dispatch(
                accumulator,
                false,
                &mut observe_latency,
                &mut observe_requeue,
            )
            .await?;
            return Ok(FlushDispatchProgress::Continue);
        }
        Ok(flush_progress)
    }

    pub(crate) async fn drive_flush_until_complete<
        LatencyObserver,
        RequeueObserver,
        BatchObserver,
    >(
        &mut self,
        dispatcher: &ProducerDispatcher,
        accumulator: &SharedAccumulator,
        observers: ReadyDispatchObservers<LatencyObserver, RequeueObserver, BatchObserver>,
    ) -> Result<(), ProducerError>
    where
        LatencyObserver: FnMut(std::time::Duration),
        RequeueObserver: FnMut(),
        BatchObserver: FnMut(&[ReadyBatch]),
    {
        let ReadyDispatchObservers {
            latency: mut observe_latency,
            requeue: mut observe_requeue,
            batches: mut observe_batches,
        } = observers;
        loop {
            // A finished dispatch that came back as a retriable Requeue is NOT a flush
            // failure: the batch is put back on the accumulator and must be re-dispatched.
            // Kafka flush() blocks until every record reaches a terminal outcome
            // (delivered, or failed after retries / delivery.timeout) — a transient
            // requeue (e.g. a cold connection to a non-bootstrap leader) is just a retry.
            // Tolerate it here and let the next dispatch step pick the batch back up.
            self.handle_finished_dispatches(
                accumulator,
                false,
                &mut observe_latency,
                &mut observe_requeue,
            )?;
            match self
                .drive_flush_dispatch_step(
                    dispatcher,
                    accumulator,
                    std::time::Instant::now(),
                    ReadyDispatchObservers::new(
                        &mut observe_latency,
                        &mut observe_requeue,
                        &mut observe_batches,
                    ),
                )
                .await?
            {
                FlushDispatchProgress::Complete => {
                    if !self.has_in_flight_dispatches() {
                        break;
                    }
                    // Nothing left ready to dispatch, but produce requests are still
                    // in flight (e.g. cold connections to non-bootstrap leaders still
                    // negotiating). Wait for one to complete — tolerating a retriable
                    // requeue so the loop re-dispatches it on the next iteration —
                    // instead of giving up early with FlushIncomplete.
                    self.wait_for_handled_dispatch(
                        accumulator,
                        false,
                        &mut observe_latency,
                        &mut observe_requeue,
                    )
                    .await?;
                },
                FlushDispatchProgress::Continue | FlushDispatchProgress::WaitForCompletion => {},
            }
        }
        Ok(())
    }

    pub(crate) fn reserve_dispatch_partitions(&mut self, partitions: &[InFlightPartitionKey]) {
        for partition in partitions {
            let depth = self
                .in_flight_partitions
                .entry(partition.clone())
                .or_insert(0);
            *depth = depth.saturating_add(1);
        }
    }

    #[cfg(test)]
    pub(crate) fn reserve_partitions_for_dispatch(&mut self, batches: &[ReadyBatch]) {
        for batch in batches {
            let depth = self
                .in_flight_partitions
                .entry(InFlightPartitionKey::from(batch))
                .or_insert(0);
            *depth = depth.saturating_add(1);
        }
    }

    pub(crate) fn release_dispatch_partitions(&mut self, partitions: Vec<InFlightPartitionKey>) {
        for partition in partitions {
            if let Some(depth) = self.in_flight_partitions.get_mut(&partition) {
                *depth = depth.saturating_sub(1);
                if *depth == 0 {
                    let _removed = self.in_flight_partitions.remove(&partition);
                }
            }
        }
    }

    fn reserve_in_flight_batch_identities(
        &mut self,
        identities: &[ReadyBatchIdentity],
    ) -> Result<(), ProducerError> {
        let mut seen = AHashSet::with_capacity(identities.len());
        for identity in identities {
            if self.in_flight_batch_identities.contains(identity)
                || self.completed_batch_identities.contains(identity)
                || !seen.insert(*identity)
            {
                return Err(ProducerError::BatchLifecycle(
                    "duplicate ready batch identity dispatched",
                ));
            }
        }
        for identity in identities {
            let _inserted = self.in_flight_batch_identities.insert(*identity);
        }
        Ok(())
    }

    fn release_in_flight_batch_identities<I>(&mut self, identities: I)
    where
        I: IntoIterator<Item = ReadyBatchIdentity>,
    {
        for identity in identities {
            let _removed = self.in_flight_batch_identities.remove(&identity);
        }
    }

    fn mark_completed_batch_identities<I>(&mut self, identities: I)
    where
        I: IntoIterator<Item = ReadyBatchIdentity>,
    {
        for identity in identities {
            if self.completed_batch_identities.insert(identity) {
                self.completed_batch_identity_order.push_back(identity);
            }
            while self.completed_batch_identity_order.len()
                > COMPLETED_BATCH_IDENTITY_TOMBSTONE_LIMIT
            {
                if let Some(expired) = self.completed_batch_identity_order.pop_front() {
                    let _removed = self.completed_batch_identities.remove(&expired);
                }
            }
            self.pending_accumulator_completions.push(identity);
        }
    }

    fn complete_pending_accumulator_batches(&mut self, accumulator: &SharedAccumulator) {
        let identities = std::mem::take(&mut self.pending_accumulator_completions);
        let _completed = accumulator.complete_batch_identities(identities);
    }

    fn release_in_flight_reservation(
        &mut self,
        task_id: tokio::task::Id,
    ) -> Option<InFlightDispatchReservation> {
        let reservation = self.in_flight_reservations.remove(&task_id)?;
        self.in_flight_buffered_bytes = self
            .in_flight_buffered_bytes
            .saturating_sub(reservation.bytes);
        self.in_flight_incomplete_batches = self
            .in_flight_incomplete_batches
            .saturating_sub(reservation.incomplete_batches);
        self.release_dispatch_partitions(reservation.partitions.clone());
        self.release_in_flight_batch_identities(reservation.identities.iter().copied());
        Some(reservation)
    }

    #[cfg(test)]
    fn release_in_flight_reservations_for_partitions(
        &mut self,
        partitions: &[InFlightPartitionKey],
    ) -> Vec<InFlightDispatchReservation> {
        if partitions.is_empty() {
            return Vec::new();
        }
        let mut reservations = Vec::new();
        while let Some(task_id) =
            self.in_flight_reservations
                .iter()
                .find_map(|(task_id, reservation)| {
                    (reservation.partitions == partitions).then_some(*task_id)
                })
        {
            if let Some(reservation) = self.release_in_flight_reservation(task_id) {
                reservations.push(reservation);
            }
        }
        reservations
    }

    pub(crate) fn select_dispatchable_batches(
        &self,
        batches: Vec<ReadyBatch>,
    ) -> DispatchSelection {
        if !self.idempotent_ordering {
            return DispatchSelection {
                dispatchable: batches,
                deferred: Vec::new(),
                partitions: Vec::new(),
            };
        }

        let mut dispatchable = Vec::with_capacity(batches.len());
        let mut deferred = Vec::new();
        let mut partitions = Vec::new();
        let mut reserved = AHashSet::new();
        for batch in batches {
            let key = InFlightPartitionKey::from(&batch);
            // The Kafka `shouldStopDrainBatchesForPartition` retry / unresolved gates run in
            // `prepare_drained_batches` (under the producer_state lock, the single source of
            // truth for per-partition in-flight sequences). Here we only apply the
            // per-partition request-depth cap.
            // Up to max.in.flight.requests.per.connection in-flight requests per partition
            // (Kafka parity), but at most ONE new request per partition per selection so each
            // becomes its own concurrent dispatch task (pipelining across the outer
            // in-flight JoinSet). Additional same-partition batches defer to the next cycle.
            let in_flight_depth = self.in_flight_partitions.get(&key).copied().unwrap_or(0);
            if reserved.contains(&key) || in_flight_depth >= self.max_in_flight_requests {
                deferred.push(batch);
                continue;
            }
            let _inserted = reserved.insert(key.clone());
            partitions.push(key);
            dispatchable.push(batch);
        }
        DispatchSelection {
            dispatchable,
            deferred,
            partitions,
        }
    }

    pub(crate) async fn prepare_dispatch_batches(
        &self,
        dispatcher: &ProducerDispatcher,
        batches: Vec<ReadyBatch>,
    ) -> Result<DispatchSelection, DispatchPrepareError> {
        let mut selection = self.select_dispatchable_batches(batches);
        match dispatcher
            .prepare_drained_batches(&mut selection.dispatchable)
            .await
        {
            // Batches whose partition is mid unresolved-sequence recovery are deferred
            // (Kafka stop-drain). `prepare_drained_batches` returns their indices into
            // `dispatchable`, which is built parallel to `partitions`, so removing
            // high-to-low keeps both lists aligned. The deferred batches are
            // re-enqueued by `start_dispatch_selection`.
            Ok(deferred_indices) => {
                for index in deferred_indices.into_iter().rev() {
                    if index < selection.partitions.len() {
                        let _dropped = selection.partitions.remove(index);
                    }
                    selection
                        .deferred
                        .push(selection.dispatchable.remove(index));
                }
                Ok(selection)
            },
            Err(error) => {
                selection.dispatchable.extend(selection.deferred);
                Err(DispatchPrepareError {
                    error,
                    batches: selection.dispatchable,
                })
            },
        }
    }

    pub(crate) async fn drain_ready_dispatch_batches(
        &self,
        dispatcher: &ProducerDispatcher,
        accumulator: &SharedAccumulator,
        now: std::time::Instant,
    ) -> Result<Option<DispatchSelection>, DispatchPrepareError> {
        // Idempotent ordering dispatches at most one new request per partition per
        // selection, so drain only each partition's front batch; draining the whole
        // ready backlog would requeue all but one per partition every cycle — O(N)
        // churn under the accumulator lock per dispatch. Non-idempotent dispatch
        // coalesces every ready batch into one request, so it drains them all.
        let batches = if self.idempotent_ordering {
            accumulator.drain_front_ready(now)
        } else {
            accumulator.drain_ready(now)
        };
        if batches.is_empty() {
            return Ok(None);
        }
        self.prepare_dispatch_batches(dispatcher, batches)
            .await
            .map(Some)
    }

    pub(crate) async fn prepare_ready_dispatch_batches(
        &mut self,
        dispatcher: &ProducerDispatcher,
        accumulator: &SharedAccumulator,
        now: std::time::Instant,
    ) -> Result<PreparedReadyDispatch, DispatchPrepareError> {
        match self.wait_for_ready_dispatch_slot(accumulator, now).await {
            ReadyDispatchSlot::Idle => Ok(PreparedReadyDispatch::Idle),
            ReadyDispatchSlot::Ready {
                completed: Some(result),
            } => Ok(PreparedReadyDispatch::PendingCompletion(result)),
            ReadyDispatchSlot::Ready { completed: None } => self
                .drain_ready_dispatch_batches(dispatcher, accumulator, now)
                .await
                .map(|selection| {
                    selection.map_or(PreparedReadyDispatch::Idle, PreparedReadyDispatch::Prepared)
                }),
        }
    }

    pub(crate) async fn prepare_ready_dispatch_batches_or_requeue(
        &mut self,
        dispatcher: &ProducerDispatcher,
        accumulator: &SharedAccumulator,
        now: std::time::Instant,
    ) -> Result<PreparedReadyDispatch, ProducerError> {
        match self
            .prepare_ready_dispatch_batches(dispatcher, accumulator, now)
            .await
        {
            Ok(prepared) => Ok(prepared),
            Err(error) => Err(Self::requeue_prepare_error(accumulator, error)),
        }
    }

    pub(crate) fn has_ready_dispatch_batches(
        accumulator: &SharedAccumulator,
        now: std::time::Instant,
    ) -> bool {
        accumulator
            .next_ready_at(now)
            .is_some_and(|ready_at| ready_at <= now)
    }

    pub(crate) async fn drain_all_dispatch_batches(
        &self,
        dispatcher: &ProducerDispatcher,
        accumulator: &SharedAccumulator,
    ) -> Result<DispatchSelection, DispatchPrepareError> {
        let batches = accumulator.drain_all();
        self.prepare_dispatch_batches(dispatcher, batches).await
    }

    pub(crate) async fn prepare_all_dispatch_batches(
        &mut self,
        dispatcher: &ProducerDispatcher,
        accumulator: &SharedAccumulator,
    ) -> Result<PreparedAllDispatch, DispatchPrepareError> {
        if accumulator.buffered_bytes() == 0 {
            return Ok(PreparedAllDispatch::Empty);
        }
        if let Some(result) = self.wait_for_completed_dispatch_slot().await {
            return Ok(PreparedAllDispatch::PendingCompletion(result));
        }
        self.drain_all_dispatch_batches(dispatcher, accumulator)
            .await
            .map(PreparedAllDispatch::Prepared)
    }

    pub(crate) async fn prepare_all_dispatch_batches_or_requeue(
        &mut self,
        dispatcher: &ProducerDispatcher,
        accumulator: &SharedAccumulator,
    ) -> Result<PreparedAllDispatch, ProducerError> {
        match self
            .prepare_all_dispatch_batches(dispatcher, accumulator)
            .await
        {
            Ok(prepared) => Ok(prepared),
            Err(error) => Err(Self::requeue_prepare_error(accumulator, error)),
        }
    }

    fn requeue_prepare_error(
        accumulator: &SharedAccumulator,
        error: DispatchPrepareError,
    ) -> ProducerError {
        let DispatchPrepareError { error, mut batches } = error;
        if matches!(
            error,
            ProducerError::Transaction { .. } | ProducerError::InvalidTransactionState(_)
        ) {
            // A terminal transaction error (e.g. a fatal AddPartitionsToTxn response;
            // its internal retries are already exhausted) can never succeed on retry,
            // so requeuing the batches would spin in an infinite drain/prepare/fail
            // loop and the delivery futures would never resolve. Fail the batches'
            // deliveries instead. The old awaited inline send surfaced this error to
            // the caller directly; the background dispatch loop must fail it here.
            for batch in &mut batches {
                if let Some(sender) = batch.delivery.take() {
                    sender.send_error(&error);
                }
            }
            return error;
        }
        if let Err(requeue_error) = accumulator.requeue_front(batches) {
            return requeue_error;
        }
        error
    }
}

impl From<&ReadyBatch> for InFlightPartitionKey {
    fn from(batch: &ReadyBatch) -> Self {
        Self {
            topic: batch.topic.clone(),
            partition: batch.partition,
        }
    }
}

#[cfg(test)]
mod tests;
