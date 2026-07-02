use super::{AtomicU64, Notify, Ordering};

/// Serializes `ProduceRequest` enqueues in spawn order so idempotent producers can keep
/// multiple in-flight requests per partition without reordering record-batch sequence numbers
/// on the wire. The sender assigns each dispatch a monotonically increasing ticket (in
/// single-threaded drain/sequence order). A dispatch waits for its ticket's turn before
/// enqueuing, then advances the turn once its requests are enqueued — so the broker observes
/// ascending base sequences per partition even though the response waits run concurrently.
/// This replaces Kafka's single Sender thread, which is the in-order enqueuer by construction.
pub(crate) struct EnqueueSequencer {
    next_ticket: AtomicU64,
    serving: AtomicU64,
    notify: Notify,
}

impl EnqueueSequencer {
    pub(crate) fn new() -> Self {
        Self {
            next_ticket: AtomicU64::new(0),
            serving: AtomicU64::new(0),
            notify: Notify::new(),
        }
    }

    /// Reserve the next enqueue ticket. MUST be called from the single-threaded sender loop
    /// (or any sequential flush path) so tickets are handed out in drain/sequence order.
    pub(crate) fn reserve_ticket(&self) -> u64 {
        self.next_ticket.fetch_add(1, Ordering::Relaxed)
    }

    /// Wait until it is `ticket`'s turn to enqueue. Returns immediately for a ticket whose
    /// turn has already passed (an in-task retry reusing its ticket), so retries never block.
    pub(crate) async fn wait_turn(&self, ticket: u64) {
        let notified = self.notify.notified();
        tokio::pin!(notified);
        loop {
            // Register this waiter with `notify` BEFORE reading `serving`. A
            // `Notified` future only registers when polled, and `advance_past`
            // wakes via `notify_waiters()` (which stores no permit); without
            // `enable()` an `advance_past` landing between the `serving` read and
            // the `.await` is lost, stalling this ticket until the *next* advance.
            // With one partition dispatching serially `serving` is always caught
            // up so this never awaits; but with several partitions dispatching
            // concurrently the lost wakeup serialized them and collapsed
            // throughput (~26x on 10 KiB records across 3 partitions).
            let _newly_registered = notified.as_mut().enable();
            if self.serving.load(Ordering::Acquire) >= ticket {
                return;
            }
            notified.as_mut().await;
            notified.set(self.notify.notified());
        }
    }

    /// Advance the turn past `ticket`. Idempotent (monotonic) so an in-task retry reusing its
    /// ticket cannot rewind the turn.
    pub(crate) fn advance_past(&self, ticket: u64) {
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
pub(crate) struct EnqueueTurn<'a> {
    pub(crate) sequencer: &'a EnqueueSequencer,
    pub(crate) ticket: u64,
    pub(crate) advanced: bool,
}

impl EnqueueTurn<'_> {
    pub(crate) fn advance(&mut self) {
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
