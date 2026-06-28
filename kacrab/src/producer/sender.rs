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
    // requests per partition (Java parity); `select_dispatchable_batches` emits at most one
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
    // Lock-free handle to the dispatcher's idempotent-recovery flag. While any
    // partition is mid-recovery (an unresolved sequence or a pending epoch bump),
    // `select_dispatchable_batches` caps the effective in-flight depth at 1 so a
    // recovering partition falls back to the verified single-in-flight path
    // (option Y) instead of pipelining new requests on top of an ambiguous
    // in-flight loss. `None` for unit-test senders that never recover.
    recovering: Option<Arc<AtomicBool>>,
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
    // concurrently from multiple tasks (Java-style thread-safe producer).
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
                    // Bound the wait by the record's max.block deadline (like Java's
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
        state.track_recovering(dispatcher.recovering_handle());
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
        // Only stop dispatching when AT the in-flight capacity (Java pipelines up to
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
    /// mirroring Java `RecordAccumulator.abortIncompleteBatches` on a forced
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
                    .drive_ready_dispatch_until_blocked(
                        &self.dispatcher,
                        &self.accumulator,
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
                return Ok(SenderLoopWait::Parked);
            }
            match self.next_wake_action(now) {
                SenderWakeAction::WaitForDispatch => {
                    return Ok(SenderLoopWait::DispatchCompletion);
                },
                SenderWakeAction::DispatchReady => {
                    let dispatched_any = self
                        .state
                        .drive_ready_dispatch_until_blocked(
                            &self.dispatcher,
                            &self.accumulator,
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
            recovering: None,
        }
    }

    pub(crate) fn notify_on_dispatch_completion(&mut self, notify: Arc<Notify>) {
        self.completion_notify = Some(notify);
    }

    /// Wire the lock-free idempotent-recovery flag published by the dispatcher so
    /// dispatch selection can serialize a recovering partition (option Y).
    pub(crate) fn track_recovering(&mut self, recovering: Arc<AtomicBool>) {
        self.recovering = Some(recovering);
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
            Vec::new(),
            InFlightDispatchAccounting::default(),
            task,
        )
    }

    fn spawn_dispatch_task_with_buffered_bytes<F>(
        &mut self,
        partitions: &[InFlightPartitionKey],
        identities: Vec<ReadyBatchIdentity>,
        accounting: InFlightDispatchAccounting,
        task: F,
    ) -> Result<AbortHandle, ProducerError>
    where
        F: Future<Output = TimedDispatchOutcome> + Send + 'static,
    {
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
            identities,
            InFlightDispatchAccounting {
                bytes: buffered_bytes,
                incomplete_batches,
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

    pub(crate) async fn wait_for_flush_completion<LatencyObserver, RequeueObserver>(
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
            self.wait_for_handled_dispatch(
                accumulator,
                true,
                &mut observe_latency,
                &mut observe_requeue,
            )
            .await?;
        }
        Ok(())
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
        let ReadyDispatchObservers {
            latency: mut observe_latency,
            requeue: mut observe_requeue,
            batches: mut observe_batches,
        } = observers;
        let mut dispatched_any = false;
        loop {
            self.handle_finished_dispatches(
                accumulator,
                false,
                &mut observe_latency,
                &mut observe_requeue,
            )?;
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
                ReadyDispatchProgress::Started(_) => return Ok(true),
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
                self.handle_completed_dispatch_with_lifecycle(
                    accumulator,
                    CompletedDispatch::new(result, true),
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
            self.wait_for_handled_dispatch(
                accumulator,
                true,
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
            self.handle_finished_dispatches(
                accumulator,
                true,
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
                FlushDispatchProgress::Complete => break,
                FlushDispatchProgress::Continue | FlushDispatchProgress::WaitForCompletion => {},
            }
        }
        self.wait_for_flush_completion(accumulator, observe_latency, observe_requeue)
            .await
    }

    pub(crate) fn reserve_dispatch_partitions(&mut self, partitions: &[InFlightPartitionKey]) {
        for partition in partitions {
            let depth = self.in_flight_partitions.entry(partition.clone()).or_insert(0);
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
        let identities = core::mem::take(&mut self.pending_accumulator_completions);
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

        // While the idempotent producer is mid-recovery (an unresolved sequence or a
        // pending epoch bump on any partition), cap the effective per-partition
        // pipeline at one in-flight request so dispatch falls back to the verified
        // single-in-flight path (option Y). Pipelining new requests on top of an
        // ambiguous in-flight loss is the unverified multi-in-flight failure path;
        // serializing here keeps recovery on the depth-1 behaviour already shipped.
        // Recovery is rare, so the common case loads the published flag once and
        // keeps the full max.in.flight pipeline.
        let recovering = self
            .recovering
            .as_ref()
            .is_some_and(|recovering| recovering.load(Ordering::Relaxed));
        let max_in_flight = if recovering {
            1
        } else {
            self.max_in_flight_requests
        };

        let mut dispatchable = Vec::with_capacity(batches.len());
        let mut deferred = Vec::new();
        let mut partitions = Vec::new();
        let mut reserved = AHashSet::new();
        for batch in batches {
            let key = InFlightPartitionKey::from(&batch);
            // Up to max.in.flight.requests.per.connection in-flight requests per partition
            // (Java parity), but at most ONE new request per partition per selection so each
            // becomes its own concurrent dispatch task (pipelining across the outer
            // in-flight JoinSet). Additional same-partition batches defer to the next cycle.
            let in_flight_depth = self.in_flight_partitions.get(&key).copied().unwrap_or(0);
            if reserved.contains(&key) || in_flight_depth >= max_in_flight {
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
        if let Err(error) = dispatcher
            .prepare_drained_batches(&mut selection.dispatchable)
            .await
        {
            selection.dispatchable.extend(selection.deferred);
            return Err(DispatchPrepareError {
                error,
                batches: selection.dispatchable,
            });
        }
        Ok(selection)
    }

    pub(crate) async fn drain_ready_dispatch_batches(
        &self,
        dispatcher: &ProducerDispatcher,
        accumulator: &SharedAccumulator,
        now: std::time::Instant,
    ) -> Result<Option<DispatchSelection>, DispatchPrepareError> {
        let batches = accumulator.drain_ready(now);
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
mod tests {
    #![allow(
        clippy::expect_used,
        clippy::missing_assert_message,
        reason = "Unit test fixtures fail fastest with contextual expect calls."
    )]

    use std::time::Duration;

    use bytes::Bytes;
    use kacrab_protocol::KafkaUuid;

    use super::{
        AllDispatchApplication, AllDispatchProgress, AppendBackpressureAction,
        AppendCallbackDeliveryRecord, AppendCapacityWait, AppendDelivery,
        AppendDispatchApplication, AppendDispatchDecision, AppendUntracked,
        AppendUntrackedBatchApply, AppendUntrackedRecord, BatchAppendStatusApplication,
        BufferWaitAction, CALLBACK_READY_BATCH_POLL_THRESHOLD, CallbackAppendFastPath,
        CompletedDispatch, DENSE_READY_BATCH_RECORDS, DispatchSelection, DispatchSelectionStart,
        DispatchStart, DrainedDispatch, FlushDispatchProgress, PreparedAllDispatch,
        PreparedReadyDispatch, ProducerSender, ProducerSenderState, ReadyDispatchApplication,
        ReadyDispatchObservers, ReadyDispatchProgress, ReadyDispatchSlot, SenderLoopWait,
        SenderWaitSignal, SenderWakeAction, SenderWakeStep, TimedDispatchOutcome,
    };
    use crate::{
        producer::{
            ProducerError, ProducerRecord, ProducerRuntimeConfig,
            accumulator::{AccumulatorConfig, AppendStatus, ReadyBatchIdentity, SharedAccumulator},
            dispatcher::DispatchOutcome,
            metrics::{ProducerMetricValue, ProducerQueueMetrics},
        },
        wire::{
            BrokerMetadata, ClusterMetadata, ConnectionConfig, PartitionMetadata, TopicMetadata,
            WireClient,
        },
    };

    const TEST_LARGE_BATCH_SIZE: usize = 16 * 1024;

    fn ready_batch(topic: &str, partition: i32) -> crate::producer::ReadyBatch {
        crate::producer::ReadyBatch {
            identity: ReadyBatchIdentity::test(0),
            topic: topic.to_owned(),
            partition,
            records: vec![ProducerRecord::new(topic, partition).value(Bytes::from_static(b"v"))],
            delivery: None,
            bytes: 1,
            pooled_buffer_bytes: 1,
            first_append_at: std::time::Instant::now(),
            producer_state: None,
        }
    }

    fn ready_accumulator() -> SharedAccumulator {
        let accumulator = SharedAccumulator::with_config(
            AccumulatorConfig::default()
                .batch_size(1)
                .linger(Duration::ZERO)
                .buffer_memory(16 * 1024),
        );
        accumulator
            .append_at(
                ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")),
                std::time::Instant::now(),
            )
            .expect("append record");
        accumulator
    }

    fn lingering_accumulator(now: std::time::Instant) -> SharedAccumulator {
        let accumulator = SharedAccumulator::with_config(
            AccumulatorConfig::default()
                .batch_size(16 * 1024)
                .linger(Duration::from_millis(10))
                .buffer_memory(16 * 1024),
        );
        accumulator
            .append_at(
                ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")),
                now,
            )
            .expect("append record");
        accumulator
    }

    fn test_dispatcher() -> crate::producer::dispatcher::ProducerDispatcher {
        crate::producer::dispatcher::ProducerDispatcher::new(WireClient::connect_with_brokers(
            ConnectionConfig::default(),
            "producer-test",
            [],
        ))
        .delivery_timeout(Duration::ZERO)
    }

    fn empty_cluster_metadata() -> ClusterMetadata {
        ClusterMetadata {
            cluster_id: None,
            controller_id: -1,
            brokers: Vec::new(),
            topics: Vec::new(),
        }
    }

    fn metadata_with_topics(topics: &[(&str, usize)]) -> ClusterMetadata {
        let topics = topics
            .iter()
            .map(|(topic, partitions)| {
                let partitions = (0..*partitions)
                    .map(|partition| {
                        let partition_index =
                            i32::try_from(partition).expect("test partition fits i32");
                        PartitionMetadata {
                            partition_index,
                            leader_id: 7,
                            leader_epoch: 1,
                            replica_nodes: vec![7],
                            isr_nodes: vec![7],
                            offline_replicas: Vec::new(),
                        }
                    })
                    .collect();
                TopicMetadata {
                    name: (*topic).to_owned(),
                    topic_id: KafkaUuid::ZERO,
                    partitions,
                }
            })
            .collect();
        ClusterMetadata {
            cluster_id: Some("cluster-a".to_owned()),
            controller_id: 7,
            brokers: vec![BrokerMetadata {
                node_id: 7,
                host: "localhost".to_owned(),
                port: 9092,
                rack: None,
            }],
            topics,
        }
    }

    #[tokio::test]
    async fn sender_state_reports_pending_work_from_buffer_or_in_flight_dispatch() {
        let mut state = ProducerSenderState::new(1);
        let empty = SharedAccumulator::with_config(AccumulatorConfig::default());
        assert!(!state.has_pending_work(&empty));

        let buffered = ready_accumulator();
        assert!(state.has_pending_work(&buffered));

        let _abort = state.spawn_in_flight(async {
            TimedDispatchOutcome {
                outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
                latency: Duration::ZERO,
                partitions: Vec::new(),
            }
        });
        assert!(state.has_pending_work(&empty));
    }

    #[tokio::test]
    async fn sender_state_reports_in_flight_dispatches_for_flush_waits() {
        let mut state = ProducerSenderState::new(1);
        assert!(!state.has_in_flight_dispatches());

        let _abort = state.spawn_in_flight(async {
            TimedDispatchOutcome {
                outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
                latency: Duration::ZERO,
                partitions: Vec::new(),
            }
        });
        assert!(state.has_in_flight_dispatches());

        let joined = state
            .wait_for_next_dispatch()
            .await
            .expect("in-flight task should be present");
        let _completed = state
            .complete_joined_dispatch(joined)
            .expect("in-flight task should not panic");
        assert!(!state.has_in_flight_dispatches());
    }

    #[tokio::test]
    async fn flush_completion_progress_reports_complete_or_waiting_for_in_flight_dispatch() {
        let mut state = ProducerSenderState::new(1);
        assert_eq!(
            state.flush_completion_progress(),
            FlushDispatchProgress::Complete
        );

        let _abort = state.spawn_in_flight(std::future::pending::<TimedDispatchOutcome>());

        assert_eq!(
            state.flush_completion_progress(),
            FlushDispatchProgress::WaitForCompletion
        );
    }

    #[tokio::test]
    async fn wait_for_flush_completion_handles_in_flight_dispatches_until_empty() {
        let mut state = ProducerSenderState::new(2);
        let accumulator = SharedAccumulator::with_config(AccumulatorConfig::default());
        let first_latency = Duration::from_millis(3);
        let second_latency = Duration::from_millis(7);
        let mut observed_latencies = Vec::new();
        let mut observed_requeues = 0;

        let _first = state.spawn_in_flight(async move {
            TimedDispatchOutcome {
                outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
                latency: first_latency,
                partitions: Vec::new(),
            }
        });
        let _second = state.spawn_in_flight(async move {
            TimedDispatchOutcome {
                outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
                latency: second_latency,
                partitions: Vec::new(),
            }
        });

        state
            .wait_for_flush_completion(
                &accumulator,
                |latency| observed_latencies.push(latency),
                || observed_requeues += 1,
            )
            .await
            .expect("flush completion should drain delivered dispatches");

        observed_latencies.sort_unstable();
        assert_eq!(observed_latencies, vec![first_latency, second_latency]);
        assert_eq!(observed_requeues, 0);
        assert_eq!(
            state.flush_completion_progress(),
            FlushDispatchProgress::Complete
        );
    }

    #[tokio::test]
    async fn wait_for_abort_completion_handles_in_flight_dispatches_until_empty() {
        let mut state = ProducerSenderState::new(2);
        let accumulator = SharedAccumulator::with_config(AccumulatorConfig::default());
        let first_latency = Duration::from_millis(5);
        let second_latency = Duration::from_millis(11);
        let mut observed_latencies = Vec::new();
        let mut observed_requeues = 0;

        let _first = state.spawn_in_flight(async move {
            TimedDispatchOutcome {
                outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
                latency: first_latency,
                partitions: Vec::new(),
            }
        });
        let _second = state.spawn_in_flight(async move {
            TimedDispatchOutcome {
                outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
                latency: second_latency,
                partitions: Vec::new(),
            }
        });

        state
            .wait_for_abort_completion(
                &accumulator,
                |latency| observed_latencies.push(latency),
                || observed_requeues += 1,
            )
            .await
            .expect("abort completion should drain delivered dispatches");

        observed_latencies.sort_unstable();
        assert_eq!(observed_latencies, vec![first_latency, second_latency]);
        assert_eq!(observed_requeues, 0);
        assert_eq!(
            state.flush_completion_progress(),
            FlushDispatchProgress::Complete
        );
    }

    #[tokio::test]
    async fn wait_for_abort_completion_drops_requeued_in_flight_batches() {
        let mut state = ProducerSenderState::new(1);
        let accumulator = SharedAccumulator::with_config(AccumulatorConfig::default());
        let batch = ready_batch("orders", 0);
        let mut observed_latencies = Vec::new();
        let mut observed_requeues = 0;

        let _in_flight = state.spawn_in_flight(async move {
            TimedDispatchOutcome {
                outcome: DispatchOutcome::Requeue(vec![batch]),
                latency: Duration::from_millis(7),
                partitions: Vec::new(),
            }
        });

        state
            .wait_for_abort_completion(
                &accumulator,
                |latency| observed_latencies.push(latency),
                || observed_requeues += 1,
            )
            .await
            .expect("abort completion should drop requeued in-flight batches");

        assert!(observed_latencies.is_empty());
        assert_eq!(observed_requeues, 1);
        assert_eq!(accumulator.buffered_records(), 0);
        assert_eq!(
            state.flush_completion_progress(),
            FlushDispatchProgress::Complete
        );
    }

    #[tokio::test]
    async fn producer_sender_waits_for_abort_completion_until_empty() {
        let mut sender = ProducerSender::new(AccumulatorConfig::default(), 2);
        let first_latency = Duration::from_millis(5);
        let second_latency = Duration::from_millis(11);
        let mut observed_latencies = Vec::new();
        let mut observed_requeues = 0;

        let _first = sender.state.spawn_in_flight(async move {
            TimedDispatchOutcome {
                outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
                latency: first_latency,
                partitions: Vec::new(),
            }
        });
        let _second = sender.state.spawn_in_flight(async move {
            TimedDispatchOutcome {
                outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
                latency: second_latency,
                partitions: Vec::new(),
            }
        });

        sender
            .wait_for_abort_completion(
                |latency| observed_latencies.push(latency),
                || observed_requeues += 1,
            )
            .await
            .expect("abort completion should drain delivered dispatches");

        observed_latencies.sort_unstable();
        assert_eq!(observed_latencies, vec![first_latency, second_latency]);
        assert_eq!(observed_requeues, 0);
        assert_eq!(
            sender.state.flush_completion_progress(),
            FlushDispatchProgress::Complete
        );
    }

    #[tokio::test]
    async fn drive_flush_until_complete_drains_in_flight_completion_after_empty_step() {
        let mut state = ProducerSenderState::new(1);
        let accumulator = SharedAccumulator::with_config(AccumulatorConfig::default());
        let latency = Duration::from_millis(23);
        let mut observed_latencies = Vec::new();
        let mut observed_requeues = 0;
        let mut observed_batches = Vec::new();
        let _in_flight = state.spawn_in_flight(async move {
            TimedDispatchOutcome {
                outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
                latency,
                partitions: Vec::new(),
            }
        });

        state
            .drive_flush_until_complete(
                &test_dispatcher(),
                &accumulator,
                ReadyDispatchObservers::new(
                    |observed_latency| observed_latencies.push(observed_latency),
                    || observed_requeues += 1,
                    |batches: &[crate::producer::ReadyBatch]| {
                        observed_batches.push(batches.len());
                    },
                ),
            )
            .await
            .expect("flush loop should drain in-flight completion");

        assert_eq!(observed_latencies, vec![latency]);
        assert_eq!(observed_requeues, 0);
        assert!(observed_batches.is_empty());
        assert_eq!(state.in_flight_len(), 0);
    }

    #[tokio::test]
    async fn sender_state_reports_in_flight_dispatch_count_for_metrics() {
        let mut state = ProducerSenderState::new(2);
        assert_eq!(state.in_flight_dispatch_count(), 0);

        let _first = state.spawn_in_flight(async {
            TimedDispatchOutcome {
                outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
                latency: Duration::ZERO,
                partitions: Vec::new(),
            }
        });
        let _second = state.spawn_in_flight(async {
            TimedDispatchOutcome {
                outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
                latency: Duration::ZERO,
                partitions: Vec::new(),
            }
        });

        assert_eq!(state.in_flight_dispatch_count(), 2);
        tokio::task::yield_now().await;
        let _completed = state.collect_finished_dispatches();
        assert_eq!(state.in_flight_dispatch_count(), 0);
    }

    #[tokio::test]
    async fn sender_state_reports_queue_snapshot_for_metrics() {
        let mut state = ProducerSenderState::new(2);
        let accumulator = ready_accumulator();
        let _dispatch = state.spawn_in_flight(async {
            TimedDispatchOutcome {
                outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
                latency: Duration::ZERO,
                partitions: Vec::new(),
            }
        });

        let snapshot = state.queue_snapshot(&accumulator);

        assert_eq!(snapshot.buffered_bytes, accumulator.buffered_bytes());
        assert_eq!(snapshot.buffered_records, accumulator.buffered_records());
        assert_eq!(snapshot.in_flight_dispatches, 1);
    }

    #[test]
    fn producer_sender_owns_accumulator_and_state_queue_snapshot() {
        let sender = ProducerSender::new(AccumulatorConfig::default(), 2);
        sender
            .accumulator
            .append_at(
                ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")),
                std::time::Instant::now(),
            )
            .expect("append record");

        let snapshot = sender.queue_snapshot();

        assert!(sender.buffered_bytes() > 0);
        assert!(snapshot.buffered_bytes > 0);
        assert_eq!(snapshot.buffered_records, 1);
        assert_eq!(snapshot.in_flight_dispatches, 0);
        assert_eq!(sender.state.max_in_flight_requests(), 2);
    }

    #[test]
    fn producer_sender_reports_next_ready_at_for_linger_scheduling() {
        let base = std::time::Instant::now();
        let linger = Duration::from_millis(10);
        let expected_ready_at = base
            .checked_add(linger)
            .expect("test ready deadline should fit");
        let before_ready = base
            .checked_add(Duration::from_millis(3))
            .expect("test before-ready instant should fit");
        let sender = ProducerSender::new(
            AccumulatorConfig::default()
                .batch_size(TEST_LARGE_BATCH_SIZE)
                .linger(linger),
            1,
        );
        sender
            .accumulator
            .append_at(
                ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")),
                base,
            )
            .expect("append lingering record");

        assert_eq!(sender.next_ready_at(before_ready), Some(expected_ready_at));
    }

    #[tokio::test]
    async fn producer_sender_reports_next_wake_action_for_sender_loop() {
        let base = std::time::Instant::now();
        let linger = Duration::from_millis(10);
        let expected_ready_at = base
            .checked_add(linger)
            .expect("test ready deadline should fit");
        let before_ready = base
            .checked_add(Duration::from_millis(3))
            .expect("test before-ready instant should fit");
        let mut sender = ProducerSender::new(
            AccumulatorConfig::default()
                .batch_size(TEST_LARGE_BATCH_SIZE)
                .linger(linger),
            2,
        );

        assert_eq!(
            sender.next_wake_action(before_ready),
            SenderWakeAction::Park
        );

        sender
            .accumulator
            .append_at(
                ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")),
                base,
            )
            .expect("append lingering record");

        assert_eq!(
            sender.next_wake_action(before_ready),
            SenderWakeAction::SleepUntil(expected_ready_at)
        );
        assert_eq!(
            sender.next_wake_action(expected_ready_at),
            SenderWakeAction::DispatchReady
        );

        let _in_flight = sender.state.spawn_in_flight(async {
            TimedDispatchOutcome {
                outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
                latency: Duration::ZERO,
                partitions: Vec::new(),
            }
        });

        // One in-flight request but max.in.flight=2 -> still below the connection
        // capacity, so the loop keeps dispatching newly-ready batches (cross-partition
        // pipelining) instead of stopping at the first in-flight.
        assert_eq!(
            sender.next_wake_action(expected_ready_at),
            SenderWakeAction::DispatchReady
        );

        let _in_flight_2 = sender.state.spawn_in_flight(async {
            TimedDispatchOutcome {
                outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
                latency: Duration::ZERO,
                partitions: Vec::new(),
            }
        });

        // Now at the in-flight capacity (2) -> wait for a completion.
        assert_eq!(
            sender.next_wake_action(expected_ready_at),
            SenderWakeAction::WaitForDispatch
        );
    }

    #[tokio::test]
    async fn producer_sender_drives_one_wake_step_for_sender_loop() {
        let now = std::time::Instant::now();
        let mut completion_sender = ProducerSender::new(AccumulatorConfig::default(), 1);
        let latency = Duration::from_millis(7);
        let mut observed_latencies = Vec::new();
        let mut observed_requeues = 0;
        let mut observed_batches = Vec::new();
        let _in_flight = completion_sender.state.spawn_in_flight(async move {
            TimedDispatchOutcome {
                outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
                latency,
                partitions: Vec::new(),
            }
        });

        let step = completion_sender
            .drive_wake_step(
                now,
                ReadyDispatchObservers::new(
                    |observed_latency| observed_latencies.push(observed_latency),
                    || observed_requeues += 1,
                    |batches: &[crate::producer::ReadyBatch]| {
                        observed_batches.push(batches.len());
                    },
                ),
            )
            .await
            .expect("sender step should handle completed dispatch");

        assert_eq!(step, SenderWakeStep::WaitedForDispatch);
        assert_eq!(observed_latencies, vec![latency]);
        assert_eq!(observed_requeues, 0);
        assert!(observed_batches.is_empty());
        assert_eq!(completion_sender.state.in_flight_len(), 0);

        let mut ready_sender = ProducerSender::new(
            AccumulatorConfig::default()
                .batch_size(1)
                .linger(Duration::ZERO),
            1,
        );
        ready_sender
            .accumulator
            .append_at(
                ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")),
                now,
            )
            .expect("append ready record");
        let mut ready_batches = Vec::new();

        let step = ready_sender
            .drive_wake_step(
                now,
                ReadyDispatchObservers::new(
                    |_| {},
                    || {},
                    |batches: &[crate::producer::ReadyBatch]| {
                        ready_batches.push(batches.len());
                    },
                ),
            )
            .await
            .expect("sender step should dispatch ready batch");

        assert_eq!(step, SenderWakeStep::DispatchedReady);
        assert_eq!(ready_batches, vec![1]);
        assert_eq!(ready_sender.accumulator.buffered_records(), 0);
        assert_eq!(ready_sender.state.in_flight_len(), 1);

        let linger = Duration::from_millis(10);
        let expected_ready_at = now
            .checked_add(linger)
            .expect("test ready deadline should fit");
        let before_ready = now
            .checked_add(Duration::from_millis(3))
            .expect("test before-ready instant should fit");
        let mut sleepy_sender = ProducerSender::new(
            AccumulatorConfig::default()
                .batch_size(TEST_LARGE_BATCH_SIZE)
                .linger(linger),
            1,
        );
        sleepy_sender
            .accumulator
            .append_at(
                ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")),
                now,
            )
            .expect("append lingering record");

        let step = sleepy_sender
            .drive_wake_step(
                before_ready,
                ReadyDispatchObservers::new(|_| {}, || {}, |_: &[crate::producer::ReadyBatch]| {}),
            )
            .await
            .expect("sender step should report linger wake");

        assert_eq!(step, SenderWakeStep::SleepUntil(expected_ready_at));
        assert_eq!(sleepy_sender.accumulator.buffered_records(), 1);

        let mut parked_sender = ProducerSender::new(AccumulatorConfig::default(), 1);
        let step = parked_sender
            .drive_wake_step(
                now,
                ReadyDispatchObservers::new(|_| {}, || {}, |_: &[crate::producer::ReadyBatch]| {}),
            )
            .await
            .expect("empty sender step should park");

        assert_eq!(step, SenderWakeStep::Parked);
    }

    #[tokio::test]
    async fn producer_sender_drives_available_wake_work_until_waiting() {
        let now = std::time::Instant::now();
        let mut sender = ProducerSender::new(
            AccumulatorConfig::default()
                .batch_size(1)
                .linger(Duration::ZERO),
            1,
        );
        sender
            .accumulator
            .append_at(
                ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")),
                now,
            )
            .expect("append ready record");
        let latency = Duration::from_millis(13);
        let mut observed_latencies = Vec::new();
        let mut observed_requeues = 0;
        let mut observed_batches = Vec::new();
        let _completed = sender.state.spawn_in_flight(async move {
            TimedDispatchOutcome {
                outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
                latency,
                partitions: Vec::new(),
            }
        });
        tokio::task::yield_now().await;

        let wait = sender
            .drive_wake_until_waiting(
                now,
                ReadyDispatchObservers::new(
                    |observed_latency| observed_latencies.push(observed_latency),
                    || observed_requeues += 1,
                    |batches: &[crate::producer::ReadyBatch]| {
                        observed_batches.push(batches.len());
                    },
                ),
            )
            .await
            .expect("sender loop work should reach next wait state");

        assert_eq!(wait, SenderLoopWait::DispatchCompletion);
        assert_eq!(observed_latencies, vec![latency]);
        assert_eq!(observed_requeues, 0);
        assert_eq!(observed_batches, vec![1]);
        assert_eq!(sender.accumulator.buffered_records(), 0);
        assert_eq!(sender.state.in_flight_len(), 1);
    }

    #[test]
    fn sender_state_discards_buffered_batches_for_abort_lifecycle() {
        let accumulator = ready_accumulator();

        let dropped = ProducerSenderState::discard_buffered_batches(&accumulator);

        assert_eq!(dropped, 1);
        assert_eq!(accumulator.buffered_records(), 0);
        assert_eq!(accumulator.buffered_bytes(), 0);
    }

    #[test]
    fn producer_sender_discards_buffered_batches_for_abort_lifecycle() {
        let sender = ProducerSender::new(
            AccumulatorConfig::default()
                .batch_size(1)
                .linger(Duration::ZERO)
                .buffer_memory(16 * 1024),
            1,
        );
        sender
            .accumulator
            .append_at(
                ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")),
                std::time::Instant::now(),
            )
            .expect("append ready batch");

        let dropped = sender.discard_buffered_batches();

        assert_eq!(dropped, 1);
        assert_eq!(sender.accumulator.buffered_records(), 0);
        assert_eq!(sender.accumulator.buffered_bytes(), 0);
    }

    #[tokio::test]
    async fn producer_sender_assigns_partition_with_accumulator_without_exposing_accumulator() {
        let sender = ProducerSender::new(AccumulatorConfig::default(), 1);
        let mut record = ProducerRecord::new("orders", 2);

        sender
            .assign_partition_with_accumulator(&mut record)
            .await
            .expect("assigned record should not need metadata");

        assert_eq!(record.partition, 2);
    }

    #[tokio::test]
    async fn producer_sender_refreshes_empty_partition_load_stats_without_exposing_accumulator() {
        let sender = ProducerSender::new(AccumulatorConfig::default(), 1);
        let metadata = empty_cluster_metadata();

        sender
            .refresh_partition_load_stats_with_metadata(&metadata, std::iter::empty::<&str>())
            .await
            .expect("empty topic refresh should be a no-op");
    }

    #[tokio::test]
    async fn producer_sender_refreshes_topic_load_stats_without_exposing_accumulator() {
        let sender = ProducerSender::new(AccumulatorConfig::default(), 1);
        let metadata = empty_cluster_metadata();

        let result = sender
            .refresh_topic_load_stats_with_metadata(&metadata, "orders")
            .await;

        assert!(matches!(
            result,
            Err(ProducerError::UnknownTopic(topic)) if topic == "orders"
        ));
    }

    #[test]
    fn producer_sender_reports_default_sticky_partitioner_policy() {
        let sender = ProducerSender::new(AccumulatorConfig::default(), 1);

        assert!(sender.uses_sticky_partitioner(&ProducerRecord::unassigned("orders")));
        assert!(!sender.uses_sticky_partitioner(
            &ProducerRecord::unassigned("orders").key(Bytes::from_static(b"key"))
        ));
        assert!(!sender.uses_sticky_partitioner(&ProducerRecord::new("orders", 2)));
    }

    #[tokio::test]
    async fn producer_sender_fetches_metadata_through_owned_dispatcher() {
        let sender = ProducerSender::new(AccumulatorConfig::default(), 1);

        let result = sender.metadata_for_topics(["orders"]).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn producer_sender_checks_transaction_error_through_owned_dispatcher() {
        let sender = ProducerSender::new(AccumulatorConfig::default(), 1);

        sender.fail_if_transaction_error().await.unwrap();
    }

    #[tokio::test]
    async fn producer_sender_assigns_partition_with_metadata_through_owned_dispatcher() {
        let sender = ProducerSender::new(AccumulatorConfig::default(), 1);
        let metadata = metadata_with_topics(&[("orders", 3)]);
        let mut record = ProducerRecord::unassigned("orders").value(Bytes::from_static(b"a"));

        sender
            .assign_partition_with_metadata(&metadata, &mut record)
            .await
            .expect("sender should assign partition through owned dispatcher");

        assert!(record.has_assigned_partition());
    }

    #[test]
    fn producer_sender_exposes_metrics_handle_from_owned_dispatcher() {
        let sender = ProducerSender::new(AccumulatorConfig::default(), 1);

        let metrics = sender.metrics_handle();

        assert_eq!(
            metrics
                .snapshot(ProducerQueueMetrics::default())
                .produce_request_count,
            0
        );
    }

    #[tokio::test]
    async fn producer_sender_refreshes_and_assigns_topic_partitions_with_metadata() {
        let sender = ProducerSender::new(AccumulatorConfig::default(), 1);
        let metadata = metadata_with_topics(&[("orders", 3)]);
        let mut records = vec![
            ProducerRecord::unassigned("orders").value(Bytes::from_static(b"a")),
            ProducerRecord::unassigned("orders").value(Bytes::from_static(b"b")),
        ];

        sender
            .refresh_and_assign_topic_partitions_with_metadata(
                &metadata,
                "orders",
                &mut records,
                true,
            )
            .await
            .expect("sender should refresh load stats and assign topic partitions");

        assert!(records.iter().all(ProducerRecord::has_assigned_partition));
    }

    #[tokio::test]
    async fn producer_sender_refreshes_and_assigns_multiple_topics_with_metadata() {
        let sender = ProducerSender::new(AccumulatorConfig::default(), 1);
        let metadata = metadata_with_topics(&[("orders", 3), ("payments", 2)]);
        let mut records = vec![
            ProducerRecord::unassigned("orders").value(Bytes::from_static(b"a")),
            ProducerRecord::unassigned("payments").value(Bytes::from_static(b"b")),
        ];

        sender
            .refresh_and_assign_partitions_with_metadata(
                &metadata,
                ["orders", "payments"],
                &mut records,
            )
            .await
            .expect("sender should refresh load stats and assign multi-topic partitions");

        assert!(records.iter().all(ProducerRecord::has_assigned_partition));
    }

    #[test]
    fn sender_state_creates_append_poll_budget_from_in_flight_limit() {
        let state = ProducerSenderState::new(3);
        let mut budget = state.append_poll_budget();
        let ready = AppendStatus {
            batch_ready: true,
            ready_batch_records: 1,
            starts_new_batch: false,
        };

        assert!(!ProducerSenderState::observe_batch_append_status(
            &mut budget,
            ready
        ));
        assert!(!ProducerSenderState::observe_batch_append_status(
            &mut budget,
            ready
        ));
        assert!(ProducerSenderState::observe_batch_append_status(
            &mut budget,
            ready
        ));
    }

    #[test]
    fn producer_sender_creates_append_poll_budget_from_in_flight_limit() {
        let sender = ProducerSender::new(AccumulatorConfig::default(), 3);
        let mut budget = sender.append_poll_budget();
        let ready = AppendStatus {
            batch_ready: true,
            ready_batch_records: 1,
            starts_new_batch: false,
        };

        assert!(!ProducerSenderState::observe_batch_append_status(
            &mut budget,
            ready
        ));
        assert!(!ProducerSenderState::observe_batch_append_status(
            &mut budget,
            ready
        ));
        assert!(ProducerSenderState::observe_batch_append_status(
            &mut budget,
            ready
        ));
    }

    #[test]
    fn sender_state_observes_batch_append_poll_budget() {
        let state = ProducerSenderState::new(3);
        let mut budget = state.append_poll_budget();
        let pending = AppendStatus {
            batch_ready: false,
            ready_batch_records: 0,
            starts_new_batch: false,
        };
        let ready = AppendStatus {
            batch_ready: true,
            ready_batch_records: 1,
            starts_new_batch: false,
        };
        let dense_ready = AppendStatus {
            batch_ready: true,
            ready_batch_records: DENSE_READY_BATCH_RECORDS,
            starts_new_batch: false,
        };

        assert!(!ProducerSenderState::observe_batch_append_status(
            &mut budget,
            pending
        ));
        assert!(!ProducerSenderState::observe_batch_append_status(
            &mut budget,
            ready
        ));
        assert!(!ProducerSenderState::observe_batch_append_status(
            &mut budget,
            ready
        ));
        assert!(ProducerSenderState::observe_batch_append_status(
            &mut budget,
            ready
        ));
        assert!(ProducerSenderState::observe_batch_append_status(
            &mut budget,
            dense_ready
        ));
    }

    #[test]
    fn sender_state_reports_append_dispatch_decisions() {
        let mut state = ProducerSenderState::new(3);
        let mut budget = state.append_poll_budget();
        let pending = AppendStatus {
            batch_ready: false,
            ready_batch_records: 0,
            starts_new_batch: false,
        };
        let ready = AppendStatus {
            batch_ready: true,
            ready_batch_records: 1,
            starts_new_batch: false,
        };
        let dense_ready = AppendStatus {
            batch_ready: true,
            ready_batch_records: DENSE_READY_BATCH_RECORDS,
            starts_new_batch: false,
        };

        assert_eq!(
            ProducerSenderState::single_append_dispatch_decision(pending),
            AppendDispatchDecision::Idle
        );
        assert_eq!(
            ProducerSenderState::single_append_dispatch_decision(ready),
            AppendDispatchDecision::DriveReady
        );
        assert_eq!(
            ProducerSenderState::batch_append_dispatch_decision(&mut budget, pending),
            AppendDispatchDecision::Idle
        );
        assert_eq!(
            ProducerSenderState::batch_append_dispatch_decision(&mut budget, ready),
            AppendDispatchDecision::MarkBatchReady
        );
        assert_eq!(
            ProducerSenderState::batch_append_dispatch_decision(&mut budget, ready),
            AppendDispatchDecision::MarkBatchReady
        );
        assert_eq!(
            ProducerSenderState::batch_append_dispatch_decision(&mut budget, ready),
            AppendDispatchDecision::DriveReady
        );
        assert_eq!(
            ProducerSenderState::batch_append_dispatch_decision(&mut budget, dense_ready),
            AppendDispatchDecision::DriveReady
        );
        assert_eq!(
            state.callback_append_dispatch_decision(ready),
            AppendDispatchDecision::MarkBatchReady
        );
    }

    #[test]
    fn producer_sender_reports_callback_append_dispatch_decision() {
        let mut sender = ProducerSender::new(AccumulatorConfig::default(), 3);
        let pending = AppendStatus {
            batch_ready: false,
            ready_batch_records: 0,
            starts_new_batch: false,
        };
        let ready = AppendStatus {
            batch_ready: true,
            ready_batch_records: 1,
            starts_new_batch: false,
        };

        assert_eq!(
            ProducerSenderState::single_append_dispatch_decision(pending),
            AppendDispatchDecision::Idle
        );
        assert_eq!(
            sender.callback_append_dispatch_decision(ready),
            AppendDispatchDecision::MarkBatchReady
        );
    }

    #[tokio::test]
    async fn sender_state_applies_append_dispatch_decision() {
        let mut state = ProducerSenderState::new(1);
        let dispatcher = test_dispatcher();
        let accumulator = ready_accumulator();
        let mut observed_batches = Vec::new();

        state
            .apply_append_dispatch_decision(
                &accumulator,
                AppendDispatchApplication::new(
                    &dispatcher,
                    AppendDispatchDecision::MarkBatchReady,
                    Some("orders"),
                ),
                ReadyDispatchObservers::new(
                    |_| {},
                    || {},
                    |batches: &[crate::producer::ReadyBatch]| {
                        observed_batches.push(batches.len());
                    },
                ),
            )
            .await
            .expect("mark-only decision should not dispatch");

        assert!(observed_batches.is_empty());
        assert_eq!(accumulator.buffered_records(), 1);
        assert_eq!(state.in_flight_len(), 0);

        state
            .apply_append_dispatch_decision(
                &accumulator,
                AppendDispatchApplication::new(
                    &dispatcher,
                    AppendDispatchDecision::DriveReady,
                    Some("orders"),
                ),
                ReadyDispatchObservers::new(
                    |_| {},
                    || {},
                    |batches: &[crate::producer::ReadyBatch]| {
                        observed_batches.push(batches.len());
                    },
                ),
            )
            .await
            .expect("drive decision should dispatch ready batches");

        assert_eq!(observed_batches, vec![1]);
        assert_eq!(accumulator.buffered_records(), 0);
        assert_eq!(state.in_flight_len(), 1);
    }

    #[tokio::test]
    async fn sender_state_applies_append_dispatch_decision_then_collects_finished_dispatches() {
        let mut state = ProducerSenderState::new(2);
        let dispatcher = test_dispatcher();
        let accumulator = SharedAccumulator::with_config(AccumulatorConfig::default());
        let latency = Duration::from_millis(7);
        let mut observed_latencies = Vec::new();
        let mut observed_requeues = 0;
        let mut observed_batches = Vec::new();

        let _completed = state.spawn_in_flight(async move {
            TimedDispatchOutcome {
                outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
                latency,
                partitions: Vec::new(),
            }
        });
        tokio::task::yield_now().await;

        state
            .apply_append_dispatch_decision_then_collect_finished(
                &accumulator,
                AppendDispatchApplication::new(&dispatcher, AppendDispatchDecision::Idle, None),
                false,
                ReadyDispatchObservers::new(
                    |duration| observed_latencies.push(duration),
                    || observed_requeues += 1,
                    |batches: &[crate::producer::ReadyBatch]| {
                        observed_batches.push(batches.len());
                    },
                ),
            )
            .await
            .expect("idle decision should still collect completed dispatches");

        assert_eq!(observed_latencies, vec![latency]);
        assert_eq!(observed_requeues, 0);
        assert!(observed_batches.is_empty());
        assert_eq!(state.in_flight_len(), 0);
    }

    #[tokio::test]
    async fn producer_sender_applies_append_dispatch_decision_then_collects_finished_dispatches() {
        let mut sender = ProducerSender::new(AccumulatorConfig::default(), 2);
        let latency = Duration::from_millis(7);
        let mut observed_latencies = Vec::new();
        let mut observed_requeues = 0;
        let mut observed_batches = Vec::new();

        let _completed = sender.state.spawn_in_flight(async move {
            TimedDispatchOutcome {
                outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
                latency,
                partitions: Vec::new(),
            }
        });
        tokio::task::yield_now().await;

        sender
            .apply_append_dispatch_decision_then_collect_finished(
                AppendDispatchDecision::Idle,
                None,
                false,
                ReadyDispatchObservers::new(
                    |duration| observed_latencies.push(duration),
                    || observed_requeues += 1,
                    |batches: &[crate::producer::ReadyBatch]| {
                        observed_batches.push(batches.len());
                    },
                ),
            )
            .await
            .expect("idle decision should still collect completed dispatches");

        assert_eq!(observed_latencies, vec![latency]);
        assert_eq!(observed_requeues, 0);
        assert!(observed_batches.is_empty());
        assert_eq!(sender.state.in_flight_len(), 0);
    }

    #[tokio::test]
    async fn sender_state_finishes_batch_append_by_driving_ready_dispatch() {
        let mut state = ProducerSenderState::new(1);
        let dispatcher = test_dispatcher();
        let accumulator = ready_accumulator();
        let mut observed_batches = Vec::new();

        state
            .finish_batch_append_dispatch(
                &dispatcher,
                &accumulator,
                ReadyDispatchObservers::new(
                    |_| {},
                    || {},
                    |batches: &[crate::producer::ReadyBatch]| {
                        observed_batches.push(batches.len());
                    },
                ),
            )
            .await
            .expect("batch append finish should drive ready batches");

        assert_eq!(observed_batches, vec![1]);
        assert_eq!(accumulator.buffered_records(), 0);
        assert_eq!(state.in_flight_len(), 1);
    }

    #[tokio::test]
    async fn producer_sender_finishes_batch_append_by_driving_ready_dispatch() {
        let mut sender = ProducerSender::new(
            AccumulatorConfig::default()
                .batch_size(1)
                .linger(Duration::ZERO),
            1,
        );
        sender
            .accumulator
            .append_at(
                ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")),
                std::time::Instant::now(),
            )
            .expect("append ready batch");
        let mut observed_batches = Vec::new();

        sender
            .finish_batch_append_dispatch(ReadyDispatchObservers::new(
                |_| {},
                || {},
                |batches: &[crate::producer::ReadyBatch]| {
                    observed_batches.push(batches.len());
                },
            ))
            .await
            .expect("batch append finish should drive ready batches");

        assert_eq!(observed_batches, vec![1]);
        assert_eq!(sender.accumulator.buffered_records(), 0);
        assert_eq!(sender.state.in_flight_len(), 1);
    }

    #[tokio::test]
    async fn sender_state_applies_batch_append_status_with_poll_budget() {
        let mut state = ProducerSenderState::new(2);
        let dispatcher = test_dispatcher();
        let accumulator = ready_accumulator();
        let mut budget = state.append_poll_budget();
        let status = AppendStatus {
            batch_ready: true,
            ready_batch_records: 1,
            starts_new_batch: false,
        };
        let mut observed_batches = Vec::new();

        state
            .apply_batch_append_status(
                &accumulator,
                &mut budget,
                BatchAppendStatusApplication::new(&dispatcher, status, Some("orders")),
                ReadyDispatchObservers::new(
                    |_| {},
                    || {},
                    |batches: &[crate::producer::ReadyBatch]| {
                        observed_batches.push(batches.len());
                    },
                ),
            )
            .await
            .expect("first ready batch should only mark sticky readiness");

        assert!(observed_batches.is_empty());
        assert_eq!(accumulator.buffered_records(), 1);
        assert_eq!(state.in_flight_len(), 0);

        state
            .apply_batch_append_status(
                &accumulator,
                &mut budget,
                BatchAppendStatusApplication::new(&dispatcher, status, Some("orders")),
                ReadyDispatchObservers::new(
                    |_| {},
                    || {},
                    |batches: &[crate::producer::ReadyBatch]| {
                        observed_batches.push(batches.len());
                    },
                ),
            )
            .await
            .expect("second ready batch should drive dispatch at budget threshold");

        assert_eq!(observed_batches, vec![1]);
        assert_eq!(accumulator.buffered_records(), 0);
        assert_eq!(state.in_flight_len(), 1);
    }

    #[tokio::test]
    async fn producer_sender_applies_batch_append_status_with_poll_budget() {
        let mut sender = ProducerSender::new(
            AccumulatorConfig::default()
                .batch_size(1)
                .linger(Duration::ZERO),
            2,
        );
        sender
            .accumulator
            .append_at(
                ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")),
                std::time::Instant::now(),
            )
            .expect("append ready batch");
        let mut budget = sender.state.append_poll_budget();
        let status = AppendStatus {
            batch_ready: true,
            ready_batch_records: 1,
            starts_new_batch: false,
        };
        let mut observed_batches = Vec::new();

        sender
            .apply_batch_append_status(
                &mut budget,
                status,
                Some("orders"),
                ReadyDispatchObservers::new(
                    |_| {},
                    || {},
                    |batches: &[crate::producer::ReadyBatch]| {
                        observed_batches.push(batches.len());
                    },
                ),
            )
            .await
            .expect("first ready batch should only mark sticky readiness");

        assert!(observed_batches.is_empty());
        assert_eq!(sender.accumulator.buffered_records(), 1);
        assert_eq!(sender.state.in_flight_len(), 0);

        sender
            .apply_batch_append_status(
                &mut budget,
                status,
                Some("orders"),
                ReadyDispatchObservers::new(
                    |_| {},
                    || {},
                    |batches: &[crate::producer::ReadyBatch]| {
                        observed_batches.push(batches.len());
                    },
                ),
            )
            .await
            .expect("second ready batch should drive dispatch at budget threshold");

        assert_eq!(observed_batches, vec![1]);
        assert_eq!(sender.accumulator.buffered_records(), 0);
        assert_eq!(sender.state.in_flight_len(), 1);
    }

    #[tokio::test]
    async fn producer_sender_appends_untracked_batch_record_and_applies_batch_status() {
        let now = std::time::Instant::now();
        let deadline = now + Duration::from_millis(5);
        let mut sender = ProducerSender::new(
            AccumulatorConfig::default()
                .batch_size(1)
                .linger(Duration::ZERO)
                .buffer_memory(16 * 1024),
            1,
        );
        let mut budget = sender.append_poll_budget();
        let mut observed_batches = Vec::new();

        sender
            .append_untracked_record_then_apply_batch_status(
                AppendUntrackedBatchApply::new(
                    &mut budget,
                    AppendUntrackedRecord::new(
                        ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")),
                        now,
                        deadline,
                    ),
                    None,
                ),
                ReadyDispatchObservers::new(
                    |_| {},
                    || {},
                    |batches: &[crate::producer::ReadyBatch]| {
                        observed_batches.push(batches.len());
                    },
                ),
            )
            .await
            .expect("untracked batch append should dispatch ready batch");

        assert_eq!(observed_batches, vec![1]);
        assert_eq!(sender.accumulator.buffered_records(), 0);
        assert_eq!(sender.state.in_flight_len(), 1);
    }

    #[test]
    fn sender_state_owns_callback_append_poll_budget_across_records() {
        let mut state = ProducerSenderState::new(5);
        let ready = AppendStatus {
            batch_ready: true,
            ready_batch_records: 1,
            starts_new_batch: false,
        };

        for _ in 0..CALLBACK_READY_BATCH_POLL_THRESHOLD.saturating_sub(1) {
            assert!(!state.observe_callback_append_status(ready));
        }
        assert!(state.observe_callback_append_status(ready));
        assert!(!state.observe_callback_append_status(ready));
    }

    #[tokio::test]
    async fn sender_state_prioritizes_dispatch_completion_for_buffer_wait() {
        let mut state = ProducerSenderState::new(1);
        let now = std::time::Instant::now();
        let accumulator = lingering_accumulator(now);
        let _abort = state.spawn_in_flight(async {
            TimedDispatchOutcome {
                outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
                latency: Duration::ZERO,
                partitions: Vec::new(),
            }
        });

        let action = state.buffer_wait_action(&accumulator, now, now + Duration::from_millis(5));

        assert_eq!(action, BufferWaitAction::WaitForDispatch);
    }

    #[tokio::test]
    async fn wait_for_buffer_progress_handles_dispatch_completion() {
        let mut state = ProducerSenderState::new(1);
        let now = std::time::Instant::now();
        let accumulator = lingering_accumulator(now);
        let latency = Duration::from_millis(19);
        let mut observed_latencies = Vec::new();
        let mut observed_requeues = 0;
        let mut observed_batches = Vec::new();
        let _abort = state.spawn_in_flight(async move {
            TimedDispatchOutcome {
                outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
                latency,
                partitions: Vec::new(),
            }
        });

        state
            .wait_for_buffer_progress(
                &test_dispatcher(),
                &accumulator,
                now + Duration::from_millis(5),
                ReadyDispatchObservers::new(
                    |observed_latency| observed_latencies.push(observed_latency),
                    || observed_requeues += 1,
                    |batches: &[crate::producer::ReadyBatch]| {
                        observed_batches.push(batches.len());
                    },
                ),
            )
            .await
            .expect("buffer wait should handle in-flight completion");

        assert_eq!(observed_latencies, vec![latency]);
        assert_eq!(observed_requeues, 0);
        assert!(observed_batches.is_empty());
        assert_eq!(accumulator.buffered_records(), 1);
        assert_eq!(state.in_flight_len(), 0);
    }

    #[test]
    fn sender_state_reports_buffer_wait_deadline_elapsed() {
        let state = ProducerSenderState::new(1);
        let now = std::time::Instant::now();
        let accumulator = lingering_accumulator(now);

        let action = state.buffer_wait_action(&accumulator, now, now);

        assert_eq!(action, BufferWaitAction::DeadlineElapsed);
    }

    #[test]
    fn sender_state_polls_when_buffer_is_ready_now() {
        let state = ProducerSenderState::new(1);
        let now = std::time::Instant::now();
        let accumulator = ready_accumulator();

        let action = state.buffer_wait_action(&accumulator, now, now + Duration::from_millis(5));

        assert_eq!(action, BufferWaitAction::PollReady);
    }

    #[test]
    fn sender_state_caps_buffer_sleep_to_one_millisecond() {
        let state = ProducerSenderState::new(1);
        let now = std::time::Instant::now();
        let accumulator = lingering_accumulator(now);

        let action = state.buffer_wait_action(&accumulator, now, now + Duration::from_millis(5));

        assert_eq!(action, BufferWaitAction::Sleep(Duration::from_millis(1)));
    }

    #[test]
    fn sender_state_owns_append_backpressure_action() {
        let state = ProducerSenderState::new(1);
        let now = std::time::Instant::now();
        let deadline = now + Duration::from_millis(5);
        let record = ProducerRecord::new("orders", 0).value(Bytes::from_static(
            b"this record is intentionally too large for the remaining buffer",
        ));
        let available = SharedAccumulator::with_config(AccumulatorConfig::default());

        assert_eq!(
            state.append_backpressure_action(&available, &record, now, deadline),
            AppendBackpressureAction::Append
        );

        let full_without_work =
            SharedAccumulator::with_config(AccumulatorConfig::default().buffer_memory(1));
        assert_eq!(
            state.append_backpressure_action(&full_without_work, &record, now, deadline),
            AppendBackpressureAction::Backpressure
        );

        let full_with_work = SharedAccumulator::with_config(
            AccumulatorConfig::default()
                .batch_size(128)
                .buffer_memory(128),
        );
        full_with_work
            .append_at(
                ProducerRecord::new("orders", 0).value(Bytes::from_static(b"x")),
                now,
            )
            .expect("append pending work");
        assert_eq!(
            state.append_backpressure_action(&full_with_work, &record, now, deadline),
            AppendBackpressureAction::WaitForBuffer
        );
        assert_eq!(
            state.append_backpressure_action(&full_with_work, &record, deadline, deadline),
            AppendBackpressureAction::Backpressure
        );
    }

    #[tokio::test]
    async fn wait_for_append_capacity_drains_ready_buffered_batch_before_append() {
        let mut state = ProducerSenderState::new(1);
        let now = std::time::Instant::now();
        let deadline = now + Duration::from_millis(5);
        let accumulator = SharedAccumulator::with_config(
            AccumulatorConfig::default()
                .batch_size(128)
                .linger(Duration::ZERO)
                .buffer_memory(160),
        );
        accumulator
            .append_at(
                ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")),
                now,
            )
            .expect("append pending ready batch");
        let record = ProducerRecord::new("orders", 0).value(Bytes::from_static(
            b"this record fits only after the ready batch is drained",
        ));
        let dispatcher = test_dispatcher();
        let mut observed_batches = Vec::new();

        let result = state
            .wait_for_append_capacity(
                &accumulator,
                AppendCapacityWait::new(&dispatcher, &record, deadline),
                ReadyDispatchObservers::new(
                    |_| {},
                    || {},
                    |batches: &[crate::producer::ReadyBatch]| {
                        observed_batches.push(batches.len());
                    },
                ),
            )
            .await;

        assert_eq!(observed_batches, vec![1]);
        assert_eq!(accumulator.buffered_records(), 0);
        assert_eq!(state.in_flight_len(), 0);
        assert!(matches!(
            result,
            Err(ProducerError::DeliveryTimeout {
                topic,
                partition: 0,
            }) if topic == "orders"
        ));
        assert!(state.has_available_memory_for(&accumulator, &record));
    }

    #[tokio::test]
    async fn append_untracked_waits_for_capacity_then_appends_record() {
        let mut state = ProducerSenderState::new(1);
        let now = std::time::Instant::now();
        let deadline = now + Duration::from_millis(5);
        let accumulator = SharedAccumulator::with_config(
            AccumulatorConfig::default()
                .batch_size(128)
                .linger(Duration::ZERO)
                .buffer_memory(160),
        );
        accumulator
            .append_at(
                ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")),
                now,
            )
            .expect("append pending ready batch");
        let record = ProducerRecord::new("orders", 0).value(Bytes::from_static(
            b"this record fits only after the ready batch is drained",
        ));
        let dispatcher = test_dispatcher();
        let mut observed_batches = Vec::new();

        let result = state
            .append_untracked_with_capacity_wait(
                &accumulator,
                AppendUntracked::new(&dispatcher, record, now, deadline),
                ReadyDispatchObservers::new(
                    |_| {},
                    || {},
                    |batches: &[crate::producer::ReadyBatch]| {
                        observed_batches.push(batches.len());
                    },
                ),
            )
            .await;

        assert_eq!(observed_batches, vec![1]);
        match result {
            Ok(status) => assert!(status.starts_new_batch),
            Err(ProducerError::Backpressure | ProducerError::DeliveryTimeout { .. }) => {},
            Err(error) => panic!("unexpected append result: {error:?}"),
        }
        assert!(accumulator.buffered_records() <= 1);
        assert_eq!(state.in_flight_len(), 0);
    }

    #[tokio::test]
    async fn producer_sender_append_untracked_owns_capacity_wait_and_append() {
        let now = std::time::Instant::now();
        let deadline = now + Duration::from_millis(5);
        let mut sender = ProducerSender::new(
            AccumulatorConfig::default()
                .batch_size(128)
                .linger(Duration::ZERO)
                .buffer_memory(160),
            1,
        );
        sender
            .accumulator
            .append_at(
                ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")),
                now,
            )
            .expect("append pending ready batch");
        let record = ProducerRecord::new("orders", 0).value(Bytes::from_static(
            b"this record fits only after the ready batch is drained",
        ));
        let mut observed_batches = Vec::new();

        let result = sender
            .append_untracked_record_with_capacity_wait(
                record,
                now,
                deadline,
                ReadyDispatchObservers::new(
                    |_| {},
                    || {},
                    |batches: &[crate::producer::ReadyBatch]| {
                        observed_batches.push(batches.len());
                    },
                ),
            )
            .await;

        assert_eq!(observed_batches, vec![1]);
        assert!(matches!(result, Err(ProducerError::Wire(_))));
        assert_eq!(sender.accumulator.buffered_records(), 0);
        assert_eq!(sender.state.in_flight_len(), 0);
    }

    #[tokio::test]
    async fn producer_sender_notifies_sender_loop_after_untracked_append() {
        let now = std::time::Instant::now();
        let deadline = now + Duration::from_millis(5);
        let mut sender = ProducerSender::new(AccumulatorConfig::default(), 1);
        let notify = sender.sender_loop_notifier();

        let status = sender
            .append_untracked_record_with_capacity_wait(
                ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")),
                now,
                deadline,
                ReadyDispatchObservers::new(|_| {}, || {}, |_: &[crate::producer::ReadyBatch]| {}),
            )
            .await
            .expect("append should succeed");

        assert!(!status.batch_ready);
        tokio::time::timeout(Duration::from_millis(20), notify.notified())
            .await
            .expect("append should notify sender loop");
    }

    #[tokio::test]
    async fn producer_sender_skips_redundant_pending_append_notify() {
        let now = std::time::Instant::now();
        let deadline = now + Duration::from_millis(5);
        let mut sender = ProducerSender::new(
            AccumulatorConfig::default()
                .batch_size(16 * 1024)
                .linger(Duration::from_millis(5)),
            1,
        );
        let notify = sender.sender_loop_notifier();

        let first = sender
            .append_untracked_record_with_capacity_wait(
                ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")),
                now,
                deadline,
                ReadyDispatchObservers::new(|_| {}, || {}, |_: &[crate::producer::ReadyBatch]| {}),
            )
            .await
            .expect("first append should succeed");
        tokio::time::timeout(Duration::from_millis(20), notify.notified())
            .await
            .expect("first append should notify sender loop");
        let second = sender
            .append_untracked_record_with_capacity_wait(
                ProducerRecord::new("orders", 0).value(Bytes::from_static(b"b")),
                now,
                deadline,
                ReadyDispatchObservers::new(|_| {}, || {}, |_: &[crate::producer::ReadyBatch]| {}),
            )
            .await
            .expect("second append should succeed");

        assert!(first.starts_new_batch);
        assert!(!first.batch_ready);
        assert!(!second.starts_new_batch);
        assert!(!second.batch_ready);
        assert!(
            tokio::time::timeout(Duration::from_millis(5), notify.notified())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn producer_sender_delay_wait_wakes_on_sender_loop_notification() {
        let sender = ProducerSender::new(AccumulatorConfig::default(), 1);
        let notify = sender.sender_loop_notifier();
        notify.notify_one();

        let signal = tokio::time::timeout(
            Duration::from_millis(20),
            sender.wait_for_sender_loop_delay(Duration::from_secs(5)),
        )
        .await
        .expect("notify should wake sender loop delay");

        assert_eq!(signal, SenderWaitSignal::Notified);
    }

    #[tokio::test]
    async fn producer_sender_notifies_sender_loop_when_dispatch_completes() {
        let mut sender = ProducerSender::new(AccumulatorConfig::default(), 1);
        let notify = sender.sender_loop_notifier();
        let _in_flight = sender.state.spawn_in_flight(async {
            TimedDispatchOutcome {
                outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
                latency: Duration::ZERO,
                partitions: Vec::new(),
            }
        });

        tokio::time::timeout(Duration::from_millis(50), notify.notified())
            .await
            .expect("dispatch completion should wake sender loop");
    }

    #[tokio::test]
    async fn producer_sender_loop_wait_handles_dispatch_completion() {
        let now = std::time::Instant::now();
        let mut sender = ProducerSender::new(AccumulatorConfig::default(), 1);
        let latency = Duration::from_millis(23);
        let mut observed_latencies = Vec::new();
        let mut observed_requeues = 0;
        let _in_flight = sender.state.spawn_in_flight(async move {
            TimedDispatchOutcome {
                outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
                latency,
                partitions: Vec::new(),
            }
        });

        let signal = sender
            .wait_for_sender_loop_wait(
                SenderLoopWait::DispatchCompletion,
                now,
                |observed_latency| observed_latencies.push(observed_latency),
                || observed_requeues += 1,
            )
            .await
            .expect("sender loop wait should handle completed dispatch");

        assert_eq!(signal, SenderWaitSignal::DispatchCompleted);
        assert_eq!(observed_latencies, vec![latency]);
        assert_eq!(observed_requeues, 0);
        assert_eq!(sender.state.in_flight_len(), 0);
    }

    #[tokio::test]
    async fn producer_sender_loop_tick_waits_after_reaching_dispatch_completion() {
        let now = std::time::Instant::now();
        let mut sender = ProducerSender::new(AccumulatorConfig::default(), 1);
        let latency = Duration::from_millis(29);
        let mut observed_latencies = Vec::new();
        let mut observed_requeues = 0;
        let mut observed_batches = Vec::new();
        let _in_flight = sender.state.spawn_in_flight(async move {
            tokio::task::yield_now().await;
            TimedDispatchOutcome {
                outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
                latency,
                partitions: Vec::new(),
            }
        });

        let signal = tokio::time::timeout(
            Duration::from_millis(50),
            sender.drive_sender_loop_once(
                now,
                ReadyDispatchObservers::new(
                    |observed_latency| observed_latencies.push(observed_latency),
                    || observed_requeues += 1,
                    |batches: &[crate::producer::ReadyBatch]| {
                        observed_batches.push(batches.len());
                    },
                ),
            ),
        )
        .await
        .expect("sender loop tick should wait for dispatch completion")
        .expect("sender loop tick should handle dispatch completion");

        assert_eq!(signal, SenderWaitSignal::DispatchCompleted);
        assert_eq!(observed_latencies, vec![latency]);
        assert_eq!(observed_requeues, 0);
        assert!(observed_batches.is_empty());
        assert_eq!(sender.state.in_flight_len(), 0);
    }

    #[tokio::test]
    async fn producer_sender_buffer_progress_dispatches_after_linger_sleep() {
        let now = std::time::Instant::now();
        let mut sender = ProducerSender::new(
            AccumulatorConfig::default()
                .batch_size(TEST_LARGE_BATCH_SIZE)
                .linger(Duration::from_millis(10)),
            1,
        );
        sender
            .accumulator
            .append_at(
                ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")),
                now,
            )
            .expect("append lingering record");
        let mut observed_batches = Vec::new();

        sender
            .wait_for_buffer_progress(
                now.checked_add(Duration::from_millis(50))
                    .expect("deadline should fit"),
                ReadyDispatchObservers::new(
                    |_| {},
                    || {},
                    |batches: &[crate::producer::ReadyBatch]| {
                        observed_batches.push(batches.len());
                    },
                ),
            )
            .await
            .expect("buffer progress should dispatch after linger sleep");

        assert_eq!(observed_batches, vec![1]);
        assert_eq!(sender.accumulator.buffered_records(), 0);
        assert_eq!(sender.state.in_flight_len(), 1);
    }

    #[tokio::test]
    async fn append_for_delivery_waits_for_capacity_then_returns_delivery() {
        let mut state = ProducerSenderState::new(1);
        let now = std::time::Instant::now();
        let deadline = now + Duration::from_millis(5);
        let accumulator = SharedAccumulator::with_config(
            AccumulatorConfig::default()
                .batch_size(128)
                .linger(Duration::ZERO)
                .buffer_memory(160),
        );
        accumulator
            .append_at(
                ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")),
                now,
            )
            .expect("append pending ready batch");
        let record = ProducerRecord::new("orders", 0).value(Bytes::from_static(
            b"this record fits only after the ready batch is drained",
        ));
        let dispatcher = test_dispatcher();
        let mut observed_batches = Vec::new();

        let result = state
            .append_for_delivery_with_capacity_wait(
                &accumulator,
                AppendDelivery::new(&dispatcher, record, now, deadline),
                ReadyDispatchObservers::new(
                    |_| {},
                    || {},
                    |batches: &[crate::producer::ReadyBatch]| {
                        observed_batches.push(batches.len());
                    },
                ),
            )
            .await;

        assert_eq!(observed_batches, vec![1]);
        match result {
            Ok((_delivery, status)) => assert!(status.starts_new_batch),
            Err(ProducerError::Backpressure | ProducerError::DeliveryTimeout { .. }) => {},
            Err(error) => panic!("unexpected append result: {error:?}"),
        }
        assert!(accumulator.buffered_records() <= 1);
        assert_eq!(state.in_flight_len(), 0);
    }

    #[tokio::test]
    async fn producer_sender_append_for_delivery_owns_capacity_wait_and_append() {
        let now = std::time::Instant::now();
        let deadline = now + Duration::from_millis(5);
        let mut sender = ProducerSender::new(
            AccumulatorConfig::default()
                .batch_size(128)
                .linger(Duration::ZERO)
                .buffer_memory(160),
            1,
        );
        sender
            .accumulator
            .append_at(
                ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")),
                now,
            )
            .expect("append pending ready batch");
        let record = ProducerRecord::new("orders", 0).value(Bytes::from_static(
            b"this record fits only after the ready batch is drained",
        ));
        let mut observed_batches = Vec::new();

        let result = sender
            .append_delivery_record_with_capacity_wait(
                record,
                now,
                deadline,
                ReadyDispatchObservers::new(
                    |_| {},
                    || {},
                    |batches: &[crate::producer::ReadyBatch]| {
                        observed_batches.push(batches.len());
                    },
                ),
            )
            .await;

        assert_eq!(observed_batches, vec![1]);
        assert!(matches!(result, Err(ProducerError::Wire(_))));
        assert_eq!(sender.accumulator.buffered_records(), 0);
        assert_eq!(sender.state.in_flight_len(), 0);
    }

    #[tokio::test]
    async fn producer_sender_appends_callback_delivery_and_returns_dispatch_decision() {
        let now = std::time::Instant::now();
        let deadline = now + Duration::from_millis(5);
        let mut sender = ProducerSender::new(
            AccumulatorConfig::default()
                .batch_size(1)
                .linger(Duration::ZERO)
                .buffer_memory(16 * 1024),
            5,
        );

        for _ in 0..CALLBACK_READY_BATCH_POLL_THRESHOLD.saturating_sub(1) {
            let (_delivery, decision) = sender
                .append_callback_delivery_record_with_capacity_wait(
                    ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")),
                    now,
                    deadline,
                    ReadyDispatchObservers::new(
                        |_| {},
                        || {},
                        |_: &[crate::producer::ReadyBatch]| {},
                    ),
                )
                .await
                .expect("callback append should succeed");

            assert_eq!(decision, AppendDispatchDecision::MarkBatchReady);
        }

        let (_delivery, decision) = sender
            .append_callback_delivery_record_with_capacity_wait(
                ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")),
                now,
                deadline,
                ReadyDispatchObservers::new(|_| {}, || {}, |_: &[crate::producer::ReadyBatch]| {}),
            )
            .await
            .expect("callback append should reach poll threshold");

        assert_eq!(decision, AppendDispatchDecision::DriveReady);
    }

    #[tokio::test]
    async fn producer_sender_appends_callback_delivery_and_applies_dispatch_decision() {
        let now = std::time::Instant::now();
        let deadline = now + Duration::from_millis(5);
        let mut sender = ProducerSender::new(
            AccumulatorConfig::default()
                .batch_size(1)
                .linger(Duration::ZERO)
                .buffer_memory(16 * 1024),
            5,
        );
        let mut observed_batches = Vec::new();
        let registered_before_dispatch = std::cell::Cell::new(false);

        for _ in 0..CALLBACK_READY_BATCH_POLL_THRESHOLD.saturating_sub(1) {
            let _delivery = sender
                .append_callback_delivery_record_then_apply_dispatch(
                    AppendCallbackDeliveryRecord::new(
                        ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")),
                        now,
                        deadline,
                        None,
                    ),
                    |_| {
                        registered_before_dispatch.set(true);
                    },
                    ReadyDispatchObservers::new(
                        |_| {},
                        || {},
                        |batches: &[crate::producer::ReadyBatch]| {
                            assert!(registered_before_dispatch.get());
                            observed_batches.push(batches.len());
                        },
                    ),
                )
                .await
                .expect("callback append should succeed before poll threshold");
        }

        assert!(observed_batches.is_empty());
        assert_eq!(
            sender.accumulator.buffered_records(),
            CALLBACK_READY_BATCH_POLL_THRESHOLD.saturating_sub(1)
        );

        let _delivery = sender
            .append_callback_delivery_record_then_apply_dispatch(
                AppendCallbackDeliveryRecord::new(
                    ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")),
                    now,
                    deadline,
                    None,
                ),
                |_| {
                    registered_before_dispatch.set(true);
                },
                ReadyDispatchObservers::new(
                    |_| {},
                    || {},
                    |batches: &[crate::producer::ReadyBatch]| {
                        assert!(registered_before_dispatch.get());
                        observed_batches.push(batches.len());
                    },
                ),
            )
            .await
            .expect("callback append should dispatch at poll threshold");

        assert_eq!(observed_batches, vec![CALLBACK_READY_BATCH_POLL_THRESHOLD]);
        assert_eq!(sender.accumulator.buffered_records(), 0);
        assert_eq!(sender.state.in_flight_len(), 1);
    }

    #[test]
    fn producer_sender_fast_appends_callback_delivery_when_capacity_available() {
        let now = std::time::Instant::now();
        let mut sender = ProducerSender::new(
            AccumulatorConfig::default()
                .batch_size(1)
                .linger(Duration::ZERO)
                .buffer_memory(16 * 1024),
            5,
        );

        let result = sender.try_append_callback_delivery_record(
            ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")),
            now,
        );

        match result {
            CallbackAppendFastPath::Appended(Ok((_delivery, decision))) => {
                assert_eq!(decision, AppendDispatchDecision::MarkBatchReady);
            },
            CallbackAppendFastPath::Appended(Err(err)) => {
                panic!("fast callback append should succeed: {err}");
            },
            CallbackAppendFastPath::WouldBlock(_) => {
                panic!("fast callback append should not block with available capacity");
            },
        }
        assert_eq!(sender.accumulator.buffered_records(), 1);
    }

    #[test]
    fn producer_sender_fast_callback_append_returns_record_when_capacity_missing() {
        let now = std::time::Instant::now();
        let mut sender = ProducerSender::new(
            AccumulatorConfig::default()
                .batch_size(1024)
                .linger(Duration::from_millis(100))
                .buffer_memory(1),
            5,
        );

        let result = sender.try_append_callback_delivery_record(
            ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")),
            now,
        );

        match result {
            CallbackAppendFastPath::WouldBlock(record) => {
                assert_eq!(record.topic.as_ref(), "orders");
                assert_eq!(record.partition, 0);
                assert_eq!(
                    record.value.as_ref().map(Bytes::as_ref),
                    Some(&b"value"[..])
                );
            },
            CallbackAppendFastPath::Appended(Ok(_)) => {
                panic!("fast callback append should not append without capacity");
            },
            CallbackAppendFastPath::Appended(Err(err)) => {
                panic!("fast callback append should return record for fallback, got {err}");
            },
        }
        assert_eq!(sender.accumulator.buffered_records(), 0);
    }

    #[test]
    fn idempotent_sender_state_defers_partitions_already_in_flight() {
        let mut state = ProducerSenderState::new(1);
        state.reserve_partitions_for_dispatch(&[ready_batch("orders", 0)]);

        let selection = state
            .select_dispatchable_batches(vec![ready_batch("orders", 0), ready_batch("orders", 1)]);

        assert_eq!(selection.dispatchable.len(), 1);
        assert_eq!(selection.dispatchable[0].partition, 1);
        assert_eq!(selection.deferred.len(), 1);
        assert_eq!(selection.deferred[0].partition, 0);
        assert_eq!(selection.partitions.len(), 1);
    }

    #[test]
    fn idempotent_selection_serializes_partition_while_recovering() {
        use std::sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        };

        // max.in.flight=5 idempotent producer with one request already in flight
        // for partition 0 (depth 1).
        let mut state = ProducerSenderState::new_with_idempotent_ordering(5, true);
        let recovering = Arc::new(AtomicBool::new(false));
        state.track_recovering(Arc::clone(&recovering));
        state.reserve_partitions_for_dispatch(&[ready_batch("orders", 0)]);

        // Not recovering: the hot partition pipelines a second in-flight request
        // (depth 1 < max.in.flight), the multi-in-flight happy path.
        let pipelined = state.select_dispatchable_batches(vec![ready_batch("orders", 0)]);
        assert_eq!(pipelined.dispatchable.len(), 1);
        assert!(pipelined.deferred.is_empty());

        // Recovering (option Y): the effective depth caps at 1, so the same
        // partition-0 batch defers to the verified single-in-flight path instead of
        // stacking a request on top of the ambiguous in-flight loss.
        recovering.store(true, Ordering::Relaxed);
        let serialized = state.select_dispatchable_batches(vec![ready_batch("orders", 0)]);
        assert!(serialized.dispatchable.is_empty());
        assert_eq!(serialized.deferred.len(), 1);
        assert_eq!(serialized.deferred[0].partition, 0);

        // An idle partition still dispatches its first request (depth 0 < 1).
        let idle = state.select_dispatchable_batches(vec![ready_batch("orders", 1)]);
        assert_eq!(idle.dispatchable.len(), 1);
        assert_eq!(idle.dispatchable[0].partition, 1);

        // Recovery cleared: full pipelining resumes.
        recovering.store(false, Ordering::Relaxed);
        let resumed = state.select_dispatchable_batches(vec![ready_batch("orders", 0)]);
        assert_eq!(resumed.dispatchable.len(), 1);
        assert!(resumed.deferred.is_empty());
    }

    #[tokio::test]
    async fn sender_state_owns_in_flight_task_slots() {
        let mut state = ProducerSenderState::new(1);

        let _abort = state.spawn_in_flight(async {
            TimedDispatchOutcome {
                outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
                latency: Duration::ZERO,
                partitions: Vec::new(),
            }
        });
        assert_eq!(state.in_flight_len(), 1);

        let joined = state
            .wait_for_next_dispatch()
            .await
            .expect("in-flight task should be present")
            .expect("in-flight task should not panic");

        assert!(matches!(joined.outcome, DispatchOutcome::Delivered(Ok(_))));
        assert_eq!(state.in_flight_len(), 0);
    }

    #[tokio::test]
    async fn sender_state_collects_finished_dispatch_tasks_without_blocking() {
        let mut state = ProducerSenderState::new(2);

        let _first = state.spawn_in_flight(async {
            TimedDispatchOutcome {
                outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
                latency: Duration::ZERO,
                partitions: Vec::new(),
            }
        });
        let _second = state.spawn_in_flight(async {
            TimedDispatchOutcome {
                outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
                latency: Duration::ZERO,
                partitions: Vec::new(),
            }
        });
        tokio::task::yield_now().await;

        let completed = state.collect_finished_dispatches();

        assert_eq!(completed.len(), 2);
        assert_eq!(state.in_flight_len(), 0);
        for result in completed {
            let joined = result.expect("finished dispatch task should not panic");
            assert!(matches!(joined.outcome, DispatchOutcome::Delivered(Ok(_))));
        }
    }

    #[tokio::test]
    async fn sender_state_waits_for_next_dispatch_task() {
        let mut state = ProducerSenderState::new(1);
        assert!(state.wait_for_next_dispatch().await.is_none());

        let _abort = state.spawn_in_flight(async {
            TimedDispatchOutcome {
                outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
                latency: Duration::ZERO,
                partitions: Vec::new(),
            }
        });

        let joined = state
            .wait_for_next_dispatch()
            .await
            .expect("in-flight task should be present")
            .expect("in-flight task should not panic");
        assert!(matches!(joined.outcome, DispatchOutcome::Delivered(Ok(_))));
        assert_eq!(state.in_flight_len(), 0);
    }

    #[tokio::test]
    async fn sender_state_waits_for_dispatch_completion() {
        let mut state = ProducerSenderState::new(1);
        assert!(state.wait_for_dispatch_completion().await.is_none());

        let _abort = state.spawn_in_flight(async {
            TimedDispatchOutcome {
                outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
                latency: Duration::ZERO,
                partitions: Vec::new(),
            }
        });

        let joined = state
            .wait_for_dispatch_completion()
            .await
            .expect("in-flight task should be present")
            .expect("in-flight task should not panic");
        assert!(matches!(joined.outcome, DispatchOutcome::Delivered(Ok(_))));
        assert_eq!(state.in_flight_len(), 0);
    }

    #[tokio::test]
    async fn spawn_dispatch_task_reserves_partitions_until_completion() {
        let mut state = ProducerSenderState::new(1);
        let reserved = ready_batch("orders", 0);
        let partition = super::InFlightPartitionKey::from(&reserved);
        let partition_for_task = partition.clone();

        let _abort = state
            .spawn_dispatch_task(std::slice::from_ref(&partition), async move {
                TimedDispatchOutcome {
                    outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
                    latency: Duration::ZERO,
                    partitions: vec![partition_for_task],
                }
            })
            .expect("dispatch task should spawn");

        let blocked = state.select_dispatchable_batches(vec![ready_batch("orders", 0)]);
        assert!(blocked.dispatchable.is_empty());
        assert_eq!(blocked.deferred.len(), 1);

        let joined = state
            .wait_for_next_dispatch()
            .await
            .expect("in-flight task should be present");
        let completed = state
            .complete_joined_dispatch(joined)
            .expect("completed dispatch should not panic");
        assert!(matches!(
            completed.outcome,
            DispatchOutcome::Delivered(Ok(_))
        ));

        let unblocked = state.select_dispatchable_batches(vec![ready_batch("orders", 0)]);
        assert_eq!(unblocked.dispatchable.len(), 1);
        assert!(unblocked.deferred.is_empty());
    }

    #[tokio::test]
    async fn spawn_drained_dispatch_owns_dispatch_task_body_and_partition_reservation() {
        let mut state = ProducerSenderState::new(1);
        let batch = ready_batch("orders", 0);
        let partition = super::InFlightPartitionKey::from(&batch);
        let dispatcher = crate::producer::dispatcher::ProducerDispatcher::new(
            WireClient::connect_with_brokers(ConnectionConfig::default(), "producer-test", []),
        )
        .delivery_timeout(Duration::ZERO);

        let dispatch = DrainedDispatch::new(
            dispatcher,
            vec![batch],
            std::time::Instant::now(),
            vec![partition.clone()],
        );
        let _abort = state
            .spawn_drained_dispatch(dispatch)
            .expect("drained dispatch should spawn");

        let blocked = state.select_dispatchable_batches(vec![ready_batch("orders", 0)]);
        assert!(blocked.dispatchable.is_empty());

        let joined = state
            .wait_for_next_dispatch()
            .await
            .expect("in-flight task should be present");
        let completed = state
            .complete_joined_dispatch(joined)
            .expect("completed dispatch should not panic");
        assert!(matches!(
            completed.outcome,
            DispatchOutcome::Delivered(Err(ProducerError::DeliveryTimeout {
                topic,
                partition: 0,
            })) if topic == "orders"
        ));

        let unblocked = state.select_dispatchable_batches(vec![ready_batch("orders", 0)]);
        assert_eq!(unblocked.dispatchable.len(), 1);
        assert!(completed.latency >= Duration::ZERO);
    }

    #[tokio::test]
    async fn spawn_observed_drained_dispatch_records_batches_before_task_owns_them() {
        let mut state = ProducerSenderState::new(1);
        let batch = ready_batch("orders", 0);
        let partition = super::InFlightPartitionKey::from(&batch);
        let dispatcher = crate::producer::dispatcher::ProducerDispatcher::new(
            WireClient::connect_with_brokers(ConnectionConfig::default(), "producer-test", []),
        )
        .delivery_timeout(Duration::ZERO);
        let mut observed = Vec::new();

        let dispatch = DrainedDispatch::new(
            dispatcher,
            vec![batch],
            std::time::Instant::now(),
            vec![partition.clone()],
        );
        let _abort = state
            .spawn_observed_drained_dispatch(dispatch, |batches| {
                observed.push((batches.len(), batches[0].bytes, batches[0].records.len()));
            })
            .expect("observed drained dispatch should spawn");

        assert_eq!(observed, vec![(1, 1, 1)]);
        let blocked = state.select_dispatchable_batches(vec![ready_batch("orders", 0)]);
        assert!(blocked.dispatchable.is_empty());

        let completed = state
            .wait_for_completed_dispatch()
            .await
            .expect("in-flight task should be present")
            .expect("completed dispatch should not panic");
        assert!(matches!(
            completed.outcome,
            DispatchOutcome::Delivered(Err(ProducerError::DeliveryTimeout {
                topic,
                partition: 0,
            })) if topic == "orders"
        ));

        let unblocked = state.select_dispatchable_batches(vec![ready_batch("orders", 0)]);
        assert_eq!(unblocked.dispatchable.len(), 1);
    }

    #[tokio::test]
    async fn in_flight_batch_bytes_hold_append_backpressure_until_dispatch_completes() {
        let mut state = ProducerSenderState::new(1);
        let mut batch = ready_batch("orders", 0);
        batch.bytes = 128;
        batch.pooled_buffer_bytes = 128;
        let partition = super::InFlightPartitionKey::from(&batch);
        let dispatcher = crate::producer::dispatcher::ProducerDispatcher::new(
            WireClient::connect_with_brokers(ConnectionConfig::default(), "producer-test", []),
        )
        .delivery_timeout(Duration::ZERO);
        let now = std::time::Instant::now();
        let deadline = now + Duration::from_millis(5);
        let accumulator = SharedAccumulator::with_config(
            AccumulatorConfig::default()
                .batch_size(128)
                .buffer_memory(160),
        );
        let record = ProducerRecord::new("orders", 0).value(Bytes::from_static(
            b"this record only fits after in-flight batch bytes are released",
        ));

        let dispatch = DrainedDispatch::new(dispatcher, vec![batch], now, vec![partition]);
        let _abort = state
            .spawn_observed_drained_dispatch(dispatch, |_| {})
            .expect("observed drained dispatch should spawn");

        assert_eq!(
            state.append_backpressure_action(&accumulator, &record, now, deadline),
            AppendBackpressureAction::WaitForBuffer
        );

        let result = state
            .wait_for_handled_dispatch(
                &SharedAccumulator::with_config(AccumulatorConfig::default()),
                false,
                |_| {},
                || {},
            )
            .await;
        assert!(matches!(
            result,
            Err(ProducerError::DeliveryTimeout {
                topic,
                partition: 0,
            }) if topic == "orders"
        ));

        assert_eq!(
            state.append_backpressure_action(&accumulator, &record, now, deadline),
            AppendBackpressureAction::Append
        );
    }

    #[tokio::test]
    async fn buffer_wait_metric_tracks_append_blocked_on_buffer_memory_like_java() {
        fn ignore_latency(_: Duration) {}
        fn ignore_requeue() {}
        fn ignore_batches(_: &[super::ReadyBatch]) {}

        let now = std::time::Instant::now();
        let deadline = now + Duration::from_millis(50);
        let mut sender = ProducerSender::new(
            AccumulatorConfig::default()
                .batch_size(80)
                .linger(Duration::from_mins(1))
                .buffer_memory(80),
            1,
        );
        let _status = sender
            .accumulator
            .append_with_status_at(
                ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")),
                now,
            )
            .expect("append first buffered batch");
        let metrics = sender.metrics_handle();

        let task = tokio::spawn(async move {
            let record = ProducerRecord::new("orders", 1).value(Bytes::from_static(b"b"));
            sender
                .append_delivery_record_with_capacity_wait(
                    record,
                    now,
                    deadline,
                    ReadyDispatchObservers::new(ignore_latency, ignore_requeue, ignore_batches),
                )
                .await
        });

        let mut observed_waiter = false;
        let poll_deadline = std::time::Instant::now() + Duration::from_millis(20);
        while std::time::Instant::now() < poll_deadline {
            if metrics
                .snapshot(ProducerQueueMetrics::default())
                .metric("waiting_threads")
                == Some(ProducerMetricValue::Gauge(1))
            {
                observed_waiter = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        assert!(observed_waiter);
        let result = task.await.expect("buffer wait task should not panic");
        assert!(matches!(result, Err(ProducerError::Backpressure)));
        assert_eq!(
            metrics
                .snapshot(ProducerQueueMetrics::default())
                .metric("waiting_threads"),
            Some(ProducerMetricValue::Gauge(0))
        );
    }

    #[tokio::test]
    async fn in_flight_dispatch_tracks_incomplete_batches_until_completion() {
        let mut state = ProducerSenderState::new(1);
        let mut batch = ready_batch("orders", 0);
        batch.bytes = 128;
        batch.pooled_buffer_bytes = 128;
        let partition = super::InFlightPartitionKey::from(&batch);
        let dispatcher = crate::producer::dispatcher::ProducerDispatcher::new(
            WireClient::connect_with_brokers(ConnectionConfig::default(), "producer-test", []),
        )
        .delivery_timeout(Duration::ZERO);
        let accumulator = SharedAccumulator::with_config(
            AccumulatorConfig::default()
                .batch_size(128)
                .buffer_memory(160),
        );

        let dispatch = DrainedDispatch::new(
            dispatcher,
            vec![batch],
            std::time::Instant::now(),
            vec![partition],
        );
        let _abort = state
            .spawn_observed_drained_dispatch(dispatch, |_| {})
            .expect("observed drained dispatch should spawn");

        let snapshot = state.queue_snapshot(&accumulator);
        assert_eq!(snapshot.incomplete_batches, 1);
        assert_eq!(snapshot.in_flight_dispatches, 1);
        assert_eq!(snapshot.buffered_bytes, 128);

        let result = state
            .wait_for_handled_dispatch(
                &SharedAccumulator::with_config(AccumulatorConfig::default()),
                false,
                |_| {},
                || {},
            )
            .await;
        assert!(matches!(
            result,
            Err(ProducerError::DeliveryTimeout {
                topic,
                partition: 0,
            }) if topic == "orders"
        ));

        let snapshot = state.queue_snapshot(&accumulator);
        assert_eq!(snapshot.incomplete_batches, 0);
        assert_eq!(snapshot.in_flight_dispatches, 0);
        assert_eq!(snapshot.buffered_bytes, 0);
    }

    #[tokio::test]
    async fn in_flight_dispatch_rejects_duplicate_batch_identity_without_double_counting() {
        let mut state = ProducerSenderState::new(2);
        let mut first = ready_batch("orders", 0);
        first.bytes = 128;
        first.pooled_buffer_bytes = 128;
        let mut duplicate = ready_batch("orders", 1);
        duplicate.bytes = 128;
        duplicate.pooled_buffer_bytes = 128;
        let first_partition = super::InFlightPartitionKey::from(&first);
        let duplicate_partition = super::InFlightPartitionKey::from(&duplicate);
        let dispatcher = crate::producer::dispatcher::ProducerDispatcher::new(
            WireClient::connect_with_brokers(ConnectionConfig::default(), "producer-test", []),
        )
        .delivery_timeout(Duration::from_millis(50));
        let accumulator = SharedAccumulator::with_config(
            AccumulatorConfig::default()
                .batch_size(128)
                .buffer_memory(256),
        );

        let first_dispatch = DrainedDispatch::new(
            dispatcher.clone(),
            vec![first],
            std::time::Instant::now(),
            vec![first_partition],
        );
        let _first_abort = state
            .spawn_observed_drained_dispatch(first_dispatch, |_| {})
            .expect("first dispatch should reserve identity");
        let duplicate_dispatch = DrainedDispatch::new(
            dispatcher,
            vec![duplicate],
            std::time::Instant::now(),
            vec![duplicate_partition],
        );
        let error = state
            .spawn_observed_drained_dispatch(duplicate_dispatch, |_| {})
            .expect_err("duplicate in-flight identity should fail");

        assert!(matches!(error, ProducerError::BatchLifecycle(_)));
        let snapshot = state.queue_snapshot(&accumulator);
        assert_eq!(snapshot.incomplete_batches, 1);
        assert_eq!(snapshot.buffered_bytes, 128);
    }

    #[tokio::test]
    async fn terminal_dispatch_rejects_stale_batch_identity_after_completion() {
        let mut state = ProducerSenderState::new(2);
        let mut first = ready_batch("orders", 0);
        first.bytes = 128;
        first.pooled_buffer_bytes = 128;
        let mut stale_batch = ready_batch("orders", 1);
        stale_batch.bytes = 128;
        stale_batch.pooled_buffer_bytes = 128;
        let first_partition = super::InFlightPartitionKey::from(&first);
        let stale_partition = super::InFlightPartitionKey::from(&stale_batch);
        let dispatcher = crate::producer::dispatcher::ProducerDispatcher::new(
            WireClient::connect_with_brokers(ConnectionConfig::default(), "producer-test", []),
        )
        .delivery_timeout(Duration::ZERO);
        let accumulator = SharedAccumulator::with_config(
            AccumulatorConfig::default()
                .batch_size(128)
                .buffer_memory(256),
        );

        let first_dispatch = DrainedDispatch::new(
            dispatcher.clone(),
            vec![first],
            std::time::Instant::now(),
            vec![first_partition],
        );
        let _first_abort = state
            .spawn_observed_drained_dispatch(first_dispatch, |_| {})
            .expect("first dispatch should reserve identity");
        let result = state
            .wait_for_handled_dispatch(
                &SharedAccumulator::with_config(AccumulatorConfig::default()),
                false,
                |_| {},
                || {},
            )
            .await;
        assert!(matches!(
            result,
            Err(ProducerError::DeliveryTimeout {
                topic,
                partition: 0,
            }) if topic == "orders"
        ));

        let stale_dispatch = DrainedDispatch::new(
            dispatcher,
            vec![stale_batch],
            std::time::Instant::now(),
            vec![stale_partition],
        );
        let error = state
            .spawn_observed_drained_dispatch(stale_dispatch, |_| {})
            .expect_err("terminal identity should not be dispatched again");

        assert!(matches!(error, ProducerError::BatchLifecycle(_)));
        let snapshot = state.queue_snapshot(&accumulator);
        assert_eq!(snapshot.incomplete_batches, 0);
        assert_eq!(snapshot.buffered_bytes, 0);
    }

    #[tokio::test]
    async fn terminal_dispatch_completes_accumulator_batch_identity() {
        let mut state = ProducerSenderState::new(2);
        let now = std::time::Instant::now();
        let accumulator = SharedAccumulator::with_config(
            AccumulatorConfig::default()
                .batch_size(128)
                .buffer_memory(256),
        );
        let _status = accumulator
            .append_with_status_at(
                ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")),
                now,
            )
            .expect("append buffered batch");
        let mut batches = accumulator.drain_all();
        assert_eq!(batches.len(), 1);
        let mut stale_batch = crate::producer::ReadyBatch {
            identity: batches[0].identity,
            topic: batches[0].topic.clone(),
            partition: batches[0].partition,
            records: vec![ProducerRecord::new("orders", 0).value(Bytes::from_static(b"stale"))],
            delivery: None,
            bytes: batches[0].bytes,
            pooled_buffer_bytes: batches[0].pooled_buffer_bytes(),
            first_append_at: batches[0].first_append_at,
            producer_state: None,
        };
        batches[0].bytes = 128;
        batches[0].pooled_buffer_bytes = 128;
        stale_batch.bytes = 128;
        stale_batch.pooled_buffer_bytes = 128;
        let partition = super::InFlightPartitionKey::from(&batches[0]);
        let dispatcher = crate::producer::dispatcher::ProducerDispatcher::new(
            WireClient::connect_with_brokers(ConnectionConfig::default(), "producer-test", []),
        )
        .delivery_timeout(Duration::ZERO);

        let dispatch = DrainedDispatch::new(dispatcher, batches, now, vec![partition]);
        let _abort = state
            .spawn_observed_drained_dispatch(dispatch, |_| {})
            .expect("observed drained dispatch should spawn");
        let result = state
            .wait_for_handled_dispatch(&accumulator, false, |_| {}, || {})
            .await;
        assert!(matches!(
            result,
            Err(ProducerError::DeliveryTimeout {
                topic,
                partition: 0,
            }) if topic == "orders"
        ));

        let error = accumulator
            .requeue_front(vec![stale_batch])
            .expect_err("terminal dispatch should complete accumulator identity");
        assert!(matches!(error, ProducerError::BatchLifecycle(_)));
        assert_eq!(accumulator.buffered_batches(), 0);
        assert_eq!(accumulator.buffered_bytes(), 0);
    }

    #[test]
    fn completed_batch_identity_tombstones_are_bounded_for_long_running_sender() {
        let mut state = ProducerSenderState::new(1);
        let tombstone_limit = 4096usize;
        let completed = tombstone_limit + 16;

        for index in 0..completed {
            state.mark_completed_batch_identities([ReadyBatchIdentity::test(
                u64::try_from(index).expect("test index should fit"),
            )]);
        }

        assert_eq!(state.completed_batch_identities.len(), tombstone_limit);
        assert!(
            !state
                .completed_batch_identities
                .contains(&ReadyBatchIdentity::test(0))
        );
        assert!(
            state
                .completed_batch_identities
                .contains(&ReadyBatchIdentity::test(
                    u64::try_from(completed - 1).expect("test index should fit"),
                ))
        );
        assert_eq!(state.pending_accumulator_completions.len(), completed);
    }

    #[tokio::test]
    async fn queue_snapshot_reports_buffer_available_across_in_flight_release() {
        let mut state = ProducerSenderState::new(1);
        let now = std::time::Instant::now();
        let accumulator = SharedAccumulator::with_config(
            AccumulatorConfig::default()
                .batch_size(128)
                .buffer_memory(160),
        );
        let _status = accumulator
            .append_with_status_at(
                ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")),
                now,
            )
            .expect("append buffered batch");

        let buffered = state.queue_snapshot(&accumulator);
        assert_eq!(buffered.buffered_bytes, 128);
        assert_eq!(buffered.buffer_available_bytes, 32);

        let mut batches = accumulator.drain_ready(now);
        assert_eq!(batches.len(), 1);
        batches[0].bytes = 128;
        batches[0].pooled_buffer_bytes = 128;
        let partition = super::InFlightPartitionKey::from(&batches[0]);
        let dispatcher = crate::producer::dispatcher::ProducerDispatcher::new(
            WireClient::connect_with_brokers(ConnectionConfig::default(), "producer-test", []),
        )
        .delivery_timeout(Duration::ZERO);
        let dispatch = DrainedDispatch::new(dispatcher, batches, now, vec![partition]);
        let _abort = state
            .spawn_observed_drained_dispatch(dispatch, |_| {})
            .expect("observed drained dispatch should spawn");

        let in_flight = state.queue_snapshot(&accumulator);
        assert_eq!(in_flight.buffered_bytes, 128);
        assert_eq!(in_flight.buffer_available_bytes, 32);

        let result = state
            .wait_for_handled_dispatch(
                &SharedAccumulator::with_config(AccumulatorConfig::default()),
                false,
                |_| {},
                || {},
            )
            .await;
        assert!(matches!(
            result,
            Err(ProducerError::DeliveryTimeout {
                topic,
                partition: 0,
            }) if topic == "orders"
        ));

        let released = state.queue_snapshot(&accumulator);
        assert_eq!(released.buffered_bytes, 0);
        assert_eq!(released.buffer_available_bytes, 160);
    }

    #[tokio::test]
    async fn start_dispatch_selection_requeues_deferred_and_spawns_dispatchable_batches() {
        let mut state = ProducerSenderState::new(1);
        let dispatchable = ready_batch("orders", 0);
        let deferred = ready_batch("orders", 1);
        let partition = super::InFlightPartitionKey::from(&dispatchable);
        let dispatcher = crate::producer::dispatcher::ProducerDispatcher::new(
            WireClient::connect_with_brokers(ConnectionConfig::default(), "producer-test", []),
        )
        .delivery_timeout(Duration::ZERO);
        let accumulator = SharedAccumulator::with_config(AccumulatorConfig::default());
        let selection = DispatchSelection {
            dispatchable: vec![dispatchable],
            deferred: vec![deferred],
            partitions: vec![partition],
        };
        let mut observed_records = 0;

        let started = state
            .start_dispatch_selection(
                &accumulator,
                DispatchSelectionStart::new(dispatcher, selection, std::time::Instant::now()),
                |batches| {
                    observed_records = batches.iter().map(|batch| batch.records.len()).sum();
                },
            )
            .expect("dispatch selection should start");

        assert!(matches!(started, DispatchStart::Spawned));
        assert_eq!(observed_records, 1);
        assert_eq!(accumulator.buffered_records(), 1);
        let blocked = state.select_dispatchable_batches(vec![ready_batch("orders", 0)]);
        assert!(blocked.dispatchable.is_empty());

        let completed = state
            .wait_for_completed_dispatch()
            .await
            .expect("in-flight task should be present")
            .expect("completed dispatch should not panic");
        assert!(matches!(
            completed.outcome,
            DispatchOutcome::Delivered(Err(ProducerError::DeliveryTimeout {
                topic,
                partition: 0,
            })) if topic == "orders"
        ));
    }

    #[tokio::test]
    async fn prepare_dispatch_batches_selects_and_prepares_dispatchable_batches() {
        let mut state = ProducerSenderState::new(1);
        state.reserve_partitions_for_dispatch(&[ready_batch("orders", 0)]);
        let dispatcher = crate::producer::dispatcher::ProducerDispatcher::new(
            WireClient::connect_with_brokers(ConnectionConfig::default(), "producer-test", []),
        );

        let selection = state
            .prepare_dispatch_batches(
                &dispatcher,
                vec![ready_batch("orders", 0), ready_batch("orders", 1)],
            )
            .await
            .expect("non-idempotent dispatcher preparation should not need metadata");

        assert_eq!(selection.dispatchable.len(), 1);
        assert_eq!(selection.dispatchable[0].partition, 1);
        assert_eq!(selection.deferred.len(), 1);
        assert_eq!(selection.deferred[0].partition, 0);
        assert_eq!(selection.partitions.len(), 1);
    }

    #[tokio::test]
    async fn drain_ready_dispatch_batches_drains_accumulator_before_preparing() {
        let state = ProducerSenderState::new(1);
        let dispatcher = crate::producer::dispatcher::ProducerDispatcher::new(
            WireClient::connect_with_brokers(ConnectionConfig::default(), "producer-test", []),
        );
        let accumulator = SharedAccumulator::with_config(
            AccumulatorConfig::default()
                .batch_size(1)
                .buffer_memory(16 * 1024),
        );
        accumulator
            .append_at(
                ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")),
                std::time::Instant::now(),
            )
            .expect("append record");

        let selection = state
            .drain_ready_dispatch_batches(&dispatcher, &accumulator, std::time::Instant::now())
            .await
            .expect("ready non-idempotent batches should prepare")
            .expect("ready batch should produce a selection");

        assert_eq!(selection.dispatchable.len(), 1);
        assert_eq!(selection.dispatchable[0].partition, 0);
        assert!(selection.deferred.is_empty());
        assert_eq!(accumulator.buffered_records(), 0);
    }

    #[tokio::test]
    async fn sender_state_waits_for_dispatch_slot_only_when_limit_is_reached() {
        let mut state = ProducerSenderState::new(2);
        let _abort = state.spawn_in_flight(async {
            TimedDispatchOutcome {
                outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
                latency: Duration::ZERO,
                partitions: Vec::new(),
            }
        });

        assert!(state.wait_for_dispatch_slot().await.is_none());

        let _abort = state.spawn_in_flight(async {
            TimedDispatchOutcome {
                outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
                latency: Duration::ZERO,
                partitions: Vec::new(),
            }
        });

        let joined = state
            .wait_for_dispatch_slot()
            .await
            .expect("full sender state should wait for one task")
            .expect("in-flight task should not panic");
        assert!(matches!(joined.outcome, DispatchOutcome::Delivered(Ok(_))));
        assert_eq!(state.in_flight_len(), 1);
    }

    #[tokio::test]
    async fn sender_state_waits_for_ready_dispatch_slot_after_readiness() {
        let mut state = ProducerSenderState::new(1);
        let empty = SharedAccumulator::with_config(AccumulatorConfig::default());
        let idle = state
            .wait_for_ready_dispatch_slot(&empty, std::time::Instant::now())
            .await;
        assert!(matches!(idle, ReadyDispatchSlot::Idle));

        let ready = ready_accumulator();
        let slot = state
            .wait_for_ready_dispatch_slot(&ready, std::time::Instant::now())
            .await;
        assert!(matches!(slot, ReadyDispatchSlot::Ready { completed: None }));

        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let _abort = state.spawn_in_flight(async {
            let _released = release_rx.await;
            TimedDispatchOutcome {
                outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
                latency: Duration::ZERO,
                partitions: Vec::new(),
            }
        });
        let blocked = tokio::time::timeout(
            Duration::from_millis(10),
            state.wait_for_ready_dispatch_slot(&ready, std::time::Instant::now()),
        )
        .await;
        assert!(blocked.is_err());

        release_tx.send(()).expect("release in-flight task");
        let unblocked = state
            .wait_for_ready_dispatch_slot(&ready, std::time::Instant::now())
            .await;
        assert!(matches!(
            unblocked,
            ReadyDispatchSlot::Ready {
                completed: Some(Ok(_))
            }
        ));
    }

    #[tokio::test]
    async fn sender_state_prepares_ready_dispatch_without_draining_before_completion_is_processed()
    {
        let dispatcher = crate::producer::dispatcher::ProducerDispatcher::new(
            WireClient::connect_with_brokers(ConnectionConfig::default(), "producer-test", []),
        );

        let mut empty_state = ProducerSenderState::new(1);
        let empty = SharedAccumulator::with_config(AccumulatorConfig::default());
        let idle = empty_state
            .prepare_ready_dispatch_batches(&dispatcher, &empty, std::time::Instant::now())
            .await
            .expect("empty accumulator should not need prepare");
        assert!(matches!(idle, PreparedReadyDispatch::Idle));

        let mut ready_state = ProducerSenderState::new(1);
        let ready = ready_accumulator();
        let prepared = ready_state
            .prepare_ready_dispatch_batches(&dispatcher, &ready, std::time::Instant::now())
            .await
            .expect("ready accumulator should prepare");
        assert!(matches!(prepared, PreparedReadyDispatch::Prepared(_)));
        assert_eq!(ready.buffered_records(), 0);

        let mut blocked_state = ProducerSenderState::new(1);
        let _abort = blocked_state.spawn_in_flight(async {
            TimedDispatchOutcome {
                outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
                latency: Duration::ZERO,
                partitions: Vec::new(),
            }
        });
        let blocked = ready_accumulator();
        let pending = blocked_state
            .prepare_ready_dispatch_batches(&dispatcher, &blocked, std::time::Instant::now())
            .await
            .expect("completed dispatch should be returned before draining");
        assert!(matches!(
            pending,
            PreparedReadyDispatch::PendingCompletion(Ok(_))
        ));
        assert_eq!(blocked.buffered_records(), 1);
    }

    #[tokio::test]
    async fn prepare_ready_dispatch_or_requeue_restores_batches_on_prepare_error() {
        let dispatcher = crate::producer::dispatcher::ProducerDispatcher::with_config(
            WireClient::connect_with_brokers(ConnectionConfig::default(), "producer-test", []),
            ProducerRuntimeConfig::default(),
        );
        let mut state = ProducerSenderState::new(1);
        let accumulator = ready_accumulator();

        let result = state
            .prepare_ready_dispatch_batches_or_requeue(
                &dispatcher,
                &accumulator,
                std::time::Instant::now(),
            )
            .await;

        assert!(result.is_err());
        assert_eq!(accumulator.buffered_records(), 1);
    }

    #[tokio::test]
    async fn sender_state_prepares_all_dispatch_without_draining_before_completion_is_processed() {
        let dispatcher = crate::producer::dispatcher::ProducerDispatcher::new(
            WireClient::connect_with_brokers(ConnectionConfig::default(), "producer-test", []),
        );

        let mut empty_state = ProducerSenderState::new(1);
        let empty = SharedAccumulator::with_config(AccumulatorConfig::default());
        let idle = empty_state
            .prepare_all_dispatch_batches(&dispatcher, &empty)
            .await
            .expect("empty accumulator should not need prepare");
        assert!(matches!(idle, PreparedAllDispatch::Empty));

        let mut ready_state = ProducerSenderState::new(1);
        let ready = ready_accumulator();
        let prepared = ready_state
            .prepare_all_dispatch_batches(&dispatcher, &ready)
            .await
            .expect("buffered accumulator should prepare");
        assert!(matches!(prepared, PreparedAllDispatch::Prepared(_)));
        assert_eq!(ready.buffered_records(), 0);

        let mut blocked_state = ProducerSenderState::new(1);
        let _abort = blocked_state.spawn_in_flight(async {
            TimedDispatchOutcome {
                outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
                latency: Duration::ZERO,
                partitions: Vec::new(),
            }
        });
        let blocked = ready_accumulator();
        let pending = blocked_state
            .prepare_all_dispatch_batches(&dispatcher, &blocked)
            .await
            .expect("completed dispatch should be returned before drain-all");
        assert!(matches!(
            pending,
            PreparedAllDispatch::PendingCompletion(Ok(_))
        ));
        assert_eq!(blocked.buffered_records(), 1);
    }

    #[tokio::test]
    async fn prepare_all_dispatch_or_requeue_restores_batches_on_prepare_error() {
        let dispatcher = crate::producer::dispatcher::ProducerDispatcher::with_config(
            WireClient::connect_with_brokers(ConnectionConfig::default(), "producer-test", []),
            ProducerRuntimeConfig::default(),
        );
        let mut state = ProducerSenderState::new(1);
        let accumulator = ready_accumulator();

        let result = state
            .prepare_all_dispatch_batches_or_requeue(&dispatcher, &accumulator)
            .await;

        assert!(result.is_err());
        assert_eq!(accumulator.buffered_records(), 1);
    }

    #[test]
    fn completing_joined_dispatch_releases_reserved_partitions() {
        let mut state = ProducerSenderState::new(1);
        let reserved = ready_batch("orders", 0);
        let partition = super::InFlightPartitionKey::from(&reserved);
        state.reserve_dispatch_partitions(std::slice::from_ref(&partition));

        let blocked = state.select_dispatchable_batches(vec![ready_batch("orders", 0)]);
        assert!(blocked.dispatchable.is_empty());
        assert_eq!(blocked.deferred.len(), 1);

        let completed = state
            .complete_joined_dispatch(Ok(TimedDispatchOutcome {
                outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
                latency: Duration::ZERO,
                partitions: vec![partition],
            }))
            .expect("completed dispatch should not panic");
        assert!(matches!(
            completed.outcome,
            DispatchOutcome::Delivered(Ok(_))
        ));

        let unblocked = state.select_dispatchable_batches(vec![ready_batch("orders", 0)]);
        assert_eq!(unblocked.dispatchable.len(), 1);
        assert!(unblocked.deferred.is_empty());
    }

    #[tokio::test]
    async fn complete_dispatch_result_normalizes_join_errors_and_releases_partitions() {
        let mut state = ProducerSenderState::new(1);
        let reserved = ready_batch("orders", 0);
        let partition = super::InFlightPartitionKey::from(&reserved);
        let partition_for_task = partition.clone();
        let _abort = state
            .spawn_dispatch_task(std::slice::from_ref(&partition), async move {
                TimedDispatchOutcome {
                    outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
                    latency: Duration::ZERO,
                    partitions: vec![partition_for_task],
                }
            })
            .expect("dispatch task should spawn");

        let joined = state
            .wait_for_next_dispatch()
            .await
            .expect("in-flight task should be present");
        let completed = state
            .complete_dispatch_result(joined)
            .expect("completed dispatch should not panic");
        assert!(matches!(
            completed.outcome,
            DispatchOutcome::Delivered(Ok(_))
        ));
        let unblocked = state.select_dispatchable_batches(vec![ready_batch("orders", 0)]);
        assert_eq!(unblocked.dispatchable.len(), 1);

        let _abort = state.spawn_in_flight(async {
            panic!("dispatch task panic");
        });
        let joined = state
            .wait_for_next_dispatch()
            .await
            .expect("panicked task should be present");
        assert!(matches!(
            state.complete_dispatch_result(joined),
            Err(ProducerError::DispatchTask(_))
        ));
    }

    #[tokio::test]
    async fn complete_dispatch_result_releases_reserved_partitions_after_dispatch_panic() {
        let mut state = ProducerSenderState::new_with_idempotent_ordering(1, true);
        let reserved = ready_batch("orders", 0);
        let partition = super::InFlightPartitionKey::from(&reserved);
        let _abort = state
            .spawn_dispatch_task(std::slice::from_ref(&partition), async {
                panic!("dispatch task panic");
            })
            .expect("dispatch task should spawn");

        let blocked = state.select_dispatchable_batches(vec![ready_batch("orders", 0)]);
        assert!(blocked.dispatchable.is_empty());
        assert_eq!(blocked.deferred.len(), 1);

        let joined = state
            .wait_for_next_dispatch()
            .await
            .expect("panicked task should be present");
        assert!(matches!(
            state.complete_dispatch_result(joined),
            Err(ProducerError::DispatchTask(_))
        ));

        let unblocked = state.select_dispatchable_batches(vec![ready_batch("orders", 0)]);
        assert_eq!(unblocked.dispatchable.len(), 1);
        assert!(unblocked.deferred.is_empty());
    }

    #[tokio::test]
    async fn wait_for_completed_dispatch_returns_normalized_completion() {
        let mut state = ProducerSenderState::new(1);
        assert!(state.wait_for_completed_dispatch().await.is_none());

        let reserved = ready_batch("orders", 0);
        let partition = super::InFlightPartitionKey::from(&reserved);
        let partition_for_task = partition.clone();
        let _abort = state
            .spawn_dispatch_task(std::slice::from_ref(&partition), async move {
                TimedDispatchOutcome {
                    outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
                    latency: Duration::ZERO,
                    partitions: vec![partition_for_task],
                }
            })
            .expect("dispatch task should spawn");

        let completed = state
            .wait_for_completed_dispatch()
            .await
            .expect("in-flight task should be present")
            .expect("in-flight task should not panic");
        assert!(matches!(
            completed.outcome,
            DispatchOutcome::Delivered(Ok(_))
        ));
        let unblocked = state.select_dispatchable_batches(vec![ready_batch("orders", 0)]);
        assert_eq!(unblocked.dispatchable.len(), 1);

        let _abort = state.spawn_in_flight(async {
            panic!("dispatch task panic");
        });
        let completed = state
            .wait_for_completed_dispatch()
            .await
            .expect("panicked task should be present");
        assert!(matches!(completed, Err(ProducerError::DispatchTask(_))));
    }

    #[tokio::test]
    async fn collect_completed_dispatches_returns_normalized_completions() {
        let mut state = ProducerSenderState::new(2);
        let _delivered = state.spawn_in_flight(async {
            TimedDispatchOutcome {
                outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
                latency: Duration::ZERO,
                partitions: Vec::new(),
            }
        });
        let _panicked = state.spawn_in_flight(async {
            panic!("dispatch task panic");
        });
        tokio::task::yield_now().await;

        let completed = state.collect_completed_dispatches();

        assert_eq!(completed.len(), 2);
        assert_eq!(
            completed
                .iter()
                .filter(|result| matches!(result, Ok(TimedDispatchOutcome { .. })))
                .count(),
            1
        );
        assert_eq!(
            completed
                .iter()
                .filter(|result| matches!(result, Err(ProducerError::DispatchTask(_))))
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn handle_finished_dispatches_collects_and_handles_completed_tasks() {
        let mut state = ProducerSenderState::new(2);
        let accumulator = SharedAccumulator::with_config(AccumulatorConfig::default());
        let latency = Duration::from_millis(9);
        let batch = ready_batch("orders", 0);
        let mut observed_latencies = Vec::new();
        let mut observed_requeues = 0;

        let _delivered = state.spawn_in_flight(async move {
            TimedDispatchOutcome {
                outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
                latency,
                partitions: Vec::new(),
            }
        });
        let _requeued = state.spawn_in_flight(async move {
            TimedDispatchOutcome {
                outcome: DispatchOutcome::Requeue(vec![batch]),
                latency: Duration::ZERO,
                partitions: Vec::new(),
            }
        });
        tokio::task::yield_now().await;

        let result = state.handle_finished_dispatches(
            &accumulator,
            false,
            |duration| observed_latencies.push(duration),
            || observed_requeues += 1,
        );

        result.expect("non-flush requeue should not be an error");
        assert_eq!(observed_latencies, vec![latency]);
        assert_eq!(observed_requeues, 1);
        assert_eq!(accumulator.buffered_records(), 1);
        assert_eq!(state.in_flight_len(), 0);
    }

    #[tokio::test]
    async fn producer_sender_handles_finished_dispatches_without_exposing_accumulator() {
        let mut sender = ProducerSender::new(AccumulatorConfig::default(), 2);
        let latency = Duration::from_millis(9);
        let batch = ready_batch("orders", 0);
        let mut observed_latencies = Vec::new();
        let mut observed_requeues = 0;

        let _delivered = sender.state.spawn_in_flight(async move {
            TimedDispatchOutcome {
                outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
                latency,
                partitions: Vec::new(),
            }
        });
        let _requeued = sender.state.spawn_in_flight(async move {
            TimedDispatchOutcome {
                outcome: DispatchOutcome::Requeue(vec![batch]),
                latency: Duration::ZERO,
                partitions: Vec::new(),
            }
        });
        tokio::task::yield_now().await;

        sender
            .handle_finished_dispatches(
                false,
                |duration| observed_latencies.push(duration),
                || observed_requeues += 1,
            )
            .expect("non-flush requeue should not be an error");

        assert_eq!(observed_latencies, vec![latency]);
        assert_eq!(observed_requeues, 1);
        assert_eq!(sender.accumulator.buffered_records(), 1);
        assert_eq!(sender.state.in_flight_len(), 0);
    }

    #[tokio::test]
    async fn wait_for_handled_dispatch_handles_next_completed_task() {
        let mut state = ProducerSenderState::new(1);
        let accumulator = SharedAccumulator::with_config(AccumulatorConfig::default());
        let latency = Duration::from_millis(11);
        let mut observed_latencies = Vec::new();
        let mut observed_requeues = 0;

        let _delivered = state.spawn_in_flight(async move {
            TimedDispatchOutcome {
                outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
                latency,
                partitions: Vec::new(),
            }
        });

        state
            .wait_for_handled_dispatch(
                &accumulator,
                false,
                |duration| observed_latencies.push(duration),
                || observed_requeues += 1,
            )
            .await
            .expect("delivered completion should be ok");

        assert_eq!(observed_latencies, vec![latency]);
        assert_eq!(observed_requeues, 0);
        assert_eq!(accumulator.buffered_records(), 0);
        assert_eq!(state.in_flight_len(), 0);
    }

    #[tokio::test]
    async fn producer_sender_waits_for_handled_dispatch_without_exposing_accumulator() {
        let mut sender = ProducerSender::new(AccumulatorConfig::default(), 1);
        let latency = Duration::from_millis(11);
        let mut observed_latencies = Vec::new();
        let mut observed_requeues = 0;

        let _delivered = sender.state.spawn_in_flight(async move {
            TimedDispatchOutcome {
                outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
                latency,
                partitions: Vec::new(),
            }
        });

        sender
            .wait_for_handled_dispatch(
                false,
                |duration| observed_latencies.push(duration),
                || observed_requeues += 1,
            )
            .await
            .expect("delivered completion should be ok");

        assert_eq!(observed_latencies, vec![latency]);
        assert_eq!(observed_requeues, 0);
        assert_eq!(sender.accumulator.buffered_records(), 0);
        assert_eq!(sender.state.in_flight_len(), 0);
    }

    #[tokio::test]
    async fn apply_ready_dispatch_progress_handles_completion_and_prepared_selection() {
        let mut state = ProducerSenderState::new(1);
        let accumulator = SharedAccumulator::with_config(AccumulatorConfig::default());
        let latency = Duration::from_millis(13);
        let mut observed_latencies = Vec::new();
        let mut observed_requeues = 0;
        let mut observed_batches = Vec::new();

        let progress = state.apply_ready_dispatch_progress(
            &accumulator,
            ReadyDispatchApplication::new(
                test_dispatcher(),
                PreparedReadyDispatch::PendingCompletion(Ok(TimedDispatchOutcome {
                    outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
                    latency,
                    partitions: Vec::new(),
                })),
                std::time::Instant::now(),
            ),
            ReadyDispatchObservers::new(
                |duration| observed_latencies.push(duration),
                || observed_requeues += 1,
                |batches: &[crate::producer::ReadyBatch]| observed_batches.push(batches.len()),
            ),
        );

        assert!(matches!(progress, Ok(ReadyDispatchProgress::Continue)));
        assert_eq!(observed_latencies, vec![latency]);
        assert_eq!(observed_requeues, 0);
        assert!(observed_batches.is_empty());

        let batch = ready_batch("orders", 0);
        let partition = super::InFlightPartitionKey::from(&batch);
        let selection = DispatchSelection {
            dispatchable: vec![batch],
            deferred: Vec::new(),
            partitions: vec![partition],
        };
        let progress = state.apply_ready_dispatch_progress(
            &accumulator,
            ReadyDispatchApplication::new(
                test_dispatcher(),
                PreparedReadyDispatch::Prepared(selection),
                std::time::Instant::now(),
            ),
            ReadyDispatchObservers::new(
                |duration| observed_latencies.push(duration),
                || observed_requeues += 1,
                |batches: &[crate::producer::ReadyBatch]| observed_batches.push(batches.len()),
            ),
        );

        assert!(matches!(
            progress,
            Ok(ReadyDispatchProgress::Started(DispatchStart::Spawned))
        ));
        assert_eq!(observed_batches, vec![1]);
        assert_eq!(state.in_flight_len(), 1);
    }

    #[tokio::test]
    async fn drive_ready_dispatch_progress_prepares_and_applies_ready_batches() {
        let mut state = ProducerSenderState::new(1);
        let accumulator = ready_accumulator();
        let mut observed_batches = Vec::new();

        let progress = state
            .drive_ready_dispatch_progress(
                &test_dispatcher(),
                &accumulator,
                std::time::Instant::now(),
                ReadyDispatchObservers::new(
                    |_| {},
                    || {},
                    |batches: &[crate::producer::ReadyBatch]| {
                        observed_batches.push(batches.len());
                    },
                ),
            )
            .await
            .expect("ready dispatch should be applied");

        assert!(matches!(
            progress,
            ReadyDispatchProgress::Started(DispatchStart::Spawned)
        ));
        assert_eq!(observed_batches, vec![1]);
        assert_eq!(accumulator.buffered_records(), 0);
        assert_eq!(state.in_flight_len(), 1);
    }

    #[tokio::test]
    async fn drive_ready_dispatch_until_blocked_handles_completion_then_starts_ready_batch() {
        let mut state = ProducerSenderState::new(1);
        let accumulator = ready_accumulator();
        let latency = Duration::from_millis(17);
        let mut observed_latencies = Vec::new();
        let mut observed_requeues = 0;
        let mut observed_batches = Vec::new();
        let _in_flight = state.spawn_in_flight(async move {
            TimedDispatchOutcome {
                outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
                latency,
                partitions: Vec::new(),
            }
        });

        let _dispatched = state
            .drive_ready_dispatch_until_blocked(
                &test_dispatcher(),
                &accumulator,
                ReadyDispatchObservers::new(
                    |observed_latency| observed_latencies.push(observed_latency),
                    || observed_requeues += 1,
                    |batches: &[crate::producer::ReadyBatch]| {
                        observed_batches.push(batches.len());
                    },
                ),
            )
            .await
            .expect("ready dispatch loop should consume completion and start batch");

        assert_eq!(observed_latencies, vec![latency]);
        assert_eq!(observed_requeues, 0);
        assert_eq!(observed_batches, vec![1]);
        assert_eq!(accumulator.buffered_records(), 0);
        assert_eq!(state.in_flight_len(), 1);
    }

    #[tokio::test]
    async fn producer_sender_drives_ready_dispatch_until_blocked() {
        let mut sender = ProducerSender::new(
            AccumulatorConfig::default()
                .batch_size(1)
                .linger(Duration::ZERO),
            1,
        );
        sender
            .accumulator
            .append_at(
                ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")),
                std::time::Instant::now(),
            )
            .expect("append ready batch");
        let latency = Duration::from_millis(17);
        let mut observed_latencies = Vec::new();
        let mut observed_requeues = 0;
        let mut observed_batches = Vec::new();
        let _in_flight = sender.state.spawn_in_flight(async move {
            TimedDispatchOutcome {
                outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
                latency,
                partitions: Vec::new(),
            }
        });

        let _dispatched = sender
            .drive_ready_dispatch_until_blocked(ReadyDispatchObservers::new(
                |observed_latency| observed_latencies.push(observed_latency),
                || observed_requeues += 1,
                |batches: &[crate::producer::ReadyBatch]| {
                    observed_batches.push(batches.len());
                },
            ))
            .await
            .expect("ready dispatch loop should consume completion and start batch");

        assert_eq!(observed_latencies, vec![latency]);
        assert_eq!(observed_requeues, 0);
        assert_eq!(observed_batches, vec![1]);
        assert_eq!(sender.accumulator.buffered_records(), 0);
        assert_eq!(sender.state.in_flight_len(), 1);
    }

    #[tokio::test]
    async fn apply_all_dispatch_progress_handles_empty_completion_and_prepared_selection() {
        let mut state = ProducerSenderState::new(1);
        let accumulator = SharedAccumulator::with_config(AccumulatorConfig::default());
        let latency = Duration::from_millis(17);
        let mut observed_latencies = Vec::new();
        let mut observed_requeues = 0;
        let mut observed_batches = Vec::new();

        let progress = state.apply_all_dispatch_progress(
            &accumulator,
            AllDispatchApplication::new(
                test_dispatcher(),
                PreparedAllDispatch::Empty,
                std::time::Instant::now(),
            ),
            ReadyDispatchObservers::new(
                |duration| observed_latencies.push(duration),
                || observed_requeues += 1,
                |batches: &[crate::producer::ReadyBatch]| observed_batches.push(batches.len()),
            ),
        );

        assert!(matches!(progress, Ok(AllDispatchProgress::Empty)));
        assert!(observed_latencies.is_empty());
        assert_eq!(observed_requeues, 0);
        assert!(observed_batches.is_empty());

        let progress = state.apply_all_dispatch_progress(
            &accumulator,
            AllDispatchApplication::new(
                test_dispatcher(),
                PreparedAllDispatch::PendingCompletion(Ok(TimedDispatchOutcome {
                    outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
                    latency,
                    partitions: Vec::new(),
                })),
                std::time::Instant::now(),
            ),
            ReadyDispatchObservers::new(
                |duration| observed_latencies.push(duration),
                || observed_requeues += 1,
                |batches: &[crate::producer::ReadyBatch]| observed_batches.push(batches.len()),
            ),
        );

        assert!(matches!(progress, Ok(AllDispatchProgress::Continue)));
        assert_eq!(observed_latencies, vec![latency]);

        let batch = ready_batch("orders", 0);
        let partition = super::InFlightPartitionKey::from(&batch);
        let selection = DispatchSelection {
            dispatchable: vec![batch],
            deferred: Vec::new(),
            partitions: vec![partition],
        };
        let progress = state.apply_all_dispatch_progress(
            &accumulator,
            AllDispatchApplication::new(
                test_dispatcher(),
                PreparedAllDispatch::Prepared(selection),
                std::time::Instant::now(),
            ),
            ReadyDispatchObservers::new(
                |duration| observed_latencies.push(duration),
                || observed_requeues += 1,
                |batches: &[crate::producer::ReadyBatch]| observed_batches.push(batches.len()),
            ),
        );

        assert!(matches!(
            progress,
            Ok(AllDispatchProgress::Started(DispatchStart::Spawned))
        ));
        assert_eq!(observed_batches, vec![1]);
        assert_eq!(state.in_flight_len(), 1);
    }

    #[tokio::test]
    async fn drive_all_dispatch_progress_prepares_and_applies_buffered_batches() {
        let mut state = ProducerSenderState::new(1);
        let accumulator = ready_accumulator();
        let mut observed_batches = Vec::new();

        let progress = state
            .drive_all_dispatch_progress(
                &test_dispatcher(),
                &accumulator,
                std::time::Instant::now(),
                ReadyDispatchObservers::new(
                    |_| {},
                    || {},
                    |batches: &[crate::producer::ReadyBatch]| {
                        observed_batches.push(batches.len());
                    },
                ),
            )
            .await
            .expect("all dispatch should be applied");

        assert!(matches!(
            progress,
            AllDispatchProgress::Started(DispatchStart::Spawned)
        ));
        assert_eq!(observed_batches, vec![1]);
        assert_eq!(accumulator.buffered_records(), 0);
        assert_eq!(state.in_flight_len(), 1);
    }

    #[tokio::test]
    async fn producer_sender_drives_flush_until_complete() {
        let mut sender = ProducerSender::new(AccumulatorConfig::default(), 1);
        let latency = Duration::from_millis(19);
        let mut observed_latencies = Vec::new();
        let mut observed_requeues = 0;
        let mut observed_batches = Vec::new();
        let _in_flight = sender.state.spawn_in_flight(async move {
            TimedDispatchOutcome {
                outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
                latency,
                partitions: Vec::new(),
            }
        });
        tokio::task::yield_now().await;

        sender
            .drive_flush_until_complete(ReadyDispatchObservers::new(
                |observed_latency| observed_latencies.push(observed_latency),
                || observed_requeues += 1,
                |batches: &[crate::producer::ReadyBatch]| {
                    observed_batches.push(batches.len());
                },
            ))
            .await
            .expect("flush loop should dispatch and wait for completion");

        assert_eq!(observed_latencies, vec![latency]);
        assert_eq!(observed_requeues, 0);
        assert!(observed_batches.is_empty());
        assert_eq!(sender.accumulator.buffered_records(), 0);
        assert_eq!(sender.state.in_flight_len(), 0);
    }

    #[tokio::test]
    async fn drive_flush_dispatch_progress_maps_empty_and_spawned_steps() {
        let mut state = ProducerSenderState::new(1);
        let accumulator = SharedAccumulator::with_config(AccumulatorConfig::default());
        let mut observed_batches = Vec::new();

        let progress = state
            .drive_flush_dispatch_progress(
                &test_dispatcher(),
                &accumulator,
                std::time::Instant::now(),
                ReadyDispatchObservers::new(
                    |_| {},
                    || {},
                    |batches: &[crate::producer::ReadyBatch]| {
                        observed_batches.push(batches.len());
                    },
                ),
            )
            .await
            .expect("empty flush step should complete");

        assert_eq!(progress, FlushDispatchProgress::Complete);
        assert!(observed_batches.is_empty());

        let accumulator = ready_accumulator();
        let progress = state
            .drive_flush_dispatch_progress(
                &test_dispatcher(),
                &accumulator,
                std::time::Instant::now(),
                ReadyDispatchObservers::new(
                    |_| {},
                    || {},
                    |batches: &[crate::producer::ReadyBatch]| {
                        observed_batches.push(batches.len());
                    },
                ),
            )
            .await
            .expect("spawned flush step should continue");

        assert_eq!(progress, FlushDispatchProgress::Continue);
        assert_eq!(observed_batches, vec![1]);
        assert_eq!(accumulator.buffered_records(), 0);
        assert_eq!(state.in_flight_len(), 1);
    }

    #[tokio::test]
    async fn drive_flush_dispatch_step_waits_for_deferred_in_flight_partition() {
        // max.in.flight=1 so the single in-flight request fills the partition's pipeline
        // depth and the next same-partition batch is deferred (the flush must wait).
        let mut state = ProducerSenderState::new(1);
        state.idempotent_ordering = true;
        let accumulator = ready_accumulator();
        let blocked = ready_batch("orders", 0);
        let partition = super::InFlightPartitionKey::from(&blocked);
        let partitions = vec![partition.clone()];
        let latency = Duration::from_millis(13);
        let mut observed_latencies = Vec::new();
        let mut observed_requeues = 0;
        let mut observed_batches = Vec::new();
        state.reserve_dispatch_partitions(std::slice::from_ref(&partition));
        let (complete_tx, complete_rx) = tokio::sync::oneshot::channel();
        let _abort = state.spawn_in_flight(async move {
            let _received = complete_rx.await;
            TimedDispatchOutcome {
                outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
                latency,
                partitions,
            }
        });
        let _complete = tokio::spawn(async move {
            tokio::task::yield_now().await;
            let _sent = complete_tx.send(());
        });

        let progress = state
            .drive_flush_dispatch_step(
                &test_dispatcher(),
                &accumulator,
                std::time::Instant::now(),
                ReadyDispatchObservers::new(
                    |observed_latency| observed_latencies.push(observed_latency),
                    || observed_requeues += 1,
                    |batches: &[crate::producer::ReadyBatch]| {
                        observed_batches.push(batches.len());
                    },
                ),
            )
            .await
            .expect("flush step should wait for blocked in-flight partition");

        assert_eq!(progress, FlushDispatchProgress::Continue);
        assert_eq!(observed_latencies, vec![latency]);
        assert_eq!(observed_requeues, 0);
        assert!(observed_batches.is_empty());
        assert_eq!(accumulator.buffered_records(), 1);
        assert!(!state.has_in_flight_dispatches());
        let unblocked = state.select_dispatchable_batches(vec![ready_batch("orders", 0)]);
        assert_eq!(unblocked.dispatchable.len(), 1);
    }

    #[test]
    fn apply_flush_dispatch_progress_reports_incomplete_without_in_flight_dispatch() {
        let state = ProducerSenderState::new(1);

        let result =
            state.apply_flush_dispatch_progress(AllDispatchProgress::Started(DispatchStart::Empty));

        assert!(matches!(result, Err(ProducerError::FlushIncomplete)));
    }

    #[test]
    fn handle_completed_dispatch_records_latency_and_propagates_delivered_error() {
        let accumulator = SharedAccumulator::with_config(AccumulatorConfig::default());
        let latency = Duration::from_millis(7);
        let mut observed_latency = None;
        let mut observed_requeues = 0;

        let result = ProducerSenderState::handle_completed_dispatch(
            &accumulator,
            CompletedDispatch::new(
                Ok(TimedDispatchOutcome {
                    outcome: DispatchOutcome::Delivered(Err(ProducerError::Backpressure)),
                    latency,
                    partitions: Vec::new(),
                }),
                false,
            ),
            |duration| observed_latency = Some(duration),
            || observed_requeues += 1,
        );

        assert!(matches!(result, Err(ProducerError::Backpressure)));
        assert_eq!(observed_latency, Some(latency));
        assert_eq!(observed_requeues, 0);
        assert_eq!(accumulator.buffered_records(), 0);
    }

    #[test]
    fn producer_sender_handles_completed_dispatch_without_exposing_accumulator() {
        let sender = ProducerSender::new(AccumulatorConfig::default(), 1);
        let batch = ready_batch("orders", 0);
        let mut observed_latency = None;
        let mut observed_requeues = 0;

        sender
            .handle_completed_dispatch(
                CompletedDispatch::new(
                    Ok(TimedDispatchOutcome {
                        outcome: DispatchOutcome::Requeue(vec![batch]),
                        latency: Duration::from_millis(3),
                        partitions: Vec::new(),
                    }),
                    false,
                ),
                |duration| observed_latency = Some(duration),
                || observed_requeues += 1,
            )
            .expect("non-flush requeue should not be an error");

        assert_eq!(sender.accumulator.buffered_records(), 1);
        assert_eq!(observed_latency, None);
        assert_eq!(observed_requeues, 1);
    }

    #[test]
    fn handle_completed_dispatch_requeues_batches_and_reports_flush_incomplete() {
        let accumulator = SharedAccumulator::with_config(AccumulatorConfig::default());
        let batch = ready_batch("orders", 0);
        let mut observed_latency = None;
        let mut observed_requeues = 0;

        ProducerSenderState::handle_completed_dispatch(
            &accumulator,
            CompletedDispatch::new(
                Ok(TimedDispatchOutcome {
                    outcome: DispatchOutcome::Requeue(vec![batch]),
                    latency: Duration::from_millis(3),
                    partitions: Vec::new(),
                }),
                false,
            ),
            |duration| observed_latency = Some(duration),
            || observed_requeues += 1,
        )
        .expect("non-flush requeue should not be an error");
        assert_eq!(accumulator.buffered_records(), 1);
        assert_eq!(observed_latency, None);
        assert_eq!(observed_requeues, 1);

        let batch = accumulator.drain_all().pop().expect("requeued batch");
        let result = ProducerSenderState::handle_completed_dispatch(
            &accumulator,
            CompletedDispatch::new(
                Ok(TimedDispatchOutcome {
                    outcome: DispatchOutcome::Requeue(vec![batch]),
                    latency: Duration::ZERO,
                    partitions: Vec::new(),
                }),
                true,
            ),
            |_| {},
            || observed_requeues += 1,
        );

        assert!(matches!(result, Err(ProducerError::FlushIncomplete)));
        assert_eq!(accumulator.buffered_records(), 1);
        assert_eq!(observed_requeues, 2);
    }
}
