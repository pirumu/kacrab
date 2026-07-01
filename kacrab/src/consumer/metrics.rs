//! Consumer metrics.
//!
//! kacrab tracks native counters for the consumer and exposes them via
//! [`Consumer::metrics`](super::Consumer::metrics), the analogue of Java's
//! `Consumer.metrics()` — a typed snapshot rather than a `Map<MetricName,
//! KafkaMetric>`. The counters cover the consumer-specific metric families Java
//! also reports (poll, records-consumed, fetch, commit, heartbeat, and rebalance
//! totals) and fold in the wire buffer-pool counters. They are shared behind an
//! `Arc` so the background heartbeat task records into the same aggregate the
//! facade reads.

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use crate::wire::BufferPoolStats;

/// Shared, cheap-to-clone consumer counters.
#[derive(Debug, Clone, Default)]
pub(super) struct ConsumerMetrics {
    inner: Arc<ConsumerMetricsInner>,
}

#[derive(Debug, Default)]
#[expect(
    clippy::struct_field_names,
    reason = "Every counter is a running total."
)]
struct ConsumerMetricsInner {
    poll_total: AtomicU64,
    records_consumed_total: AtomicU64,
    fetch_total: AtomicU64,
    commit_total: AtomicU64,
    heartbeat_total: AtomicU64,
    rebalance_total: AtomicU64,
}

impl ConsumerMetrics {
    /// Record one `poll` call.
    pub(super) fn record_poll(&self) {
        let _previous = self.inner.poll_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record `count` records handed back to the caller.
    pub(super) fn record_records(&self, count: usize) {
        let count = u64::try_from(count).unwrap_or(u64::MAX);
        let _previous = self
            .inner
            .records_consumed_total
            .fetch_add(count, Ordering::Relaxed);
    }

    /// Record one `Fetch` request.
    pub(super) fn record_fetch(&self) {
        let _previous = self.inner.fetch_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record one successful offset commit (sync, async, or auto).
    pub(super) fn record_commit(&self) {
        let _previous = self.inner.commit_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record one `Heartbeat` sent by the background task.
    pub(super) fn record_heartbeat(&self) {
        let _previous = self.inner.heartbeat_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record one completed group (re)join.
    pub(super) fn record_rebalance(&self) {
        let _previous = self.inner.rebalance_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Take a point-in-time snapshot, folding in the wire buffer-pool counters.
    pub(super) fn snapshot(&self, buffer_pool: BufferPoolStats) -> ConsumerMetricsSnapshot {
        ConsumerMetricsSnapshot {
            poll_total: self.inner.poll_total.load(Ordering::Relaxed),
            records_consumed_total: self.inner.records_consumed_total.load(Ordering::Relaxed),
            fetch_total: self.inner.fetch_total.load(Ordering::Relaxed),
            commit_total: self.inner.commit_total.load(Ordering::Relaxed),
            heartbeat_total: self.inner.heartbeat_total.load(Ordering::Relaxed),
            rebalance_total: self.inner.rebalance_total.load(Ordering::Relaxed),
            buffer_pool,
        }
    }
}

/// A snapshot of the consumer's metrics.
///
/// Returned by [`Consumer::metrics`](super::Consumer::metrics); kacrab's native
/// analogue of Java's `Consumer.metrics()` (a typed snapshot rather than a
/// `Map<MetricName, KafkaMetric>`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerMetricsSnapshot {
    /// Total `poll` calls made on this consumer.
    pub poll_total: u64,
    /// Total records handed back to the caller across all polls.
    pub records_consumed_total: u64,
    /// Total `Fetch` requests sent.
    pub fetch_total: u64,
    /// Total successful offset commits (sync, async, and auto).
    pub commit_total: u64,
    /// Total `Heartbeat` requests sent by the background task.
    pub heartbeat_total: u64,
    /// Total completed group (re)joins.
    pub rebalance_total: u64,
    /// The wire buffer-pool counters at snapshot time.
    pub buffer_pool: BufferPoolStats,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_reflects_recorded_counters() {
        let metrics = ConsumerMetrics::default();
        metrics.record_poll();
        metrics.record_poll();
        metrics.record_records(7);
        metrics.record_fetch();
        metrics.record_commit();
        metrics.record_heartbeat();
        metrics.record_rebalance();
        let snapshot = metrics.snapshot(BufferPoolStats::default());
        assert_eq!(snapshot.poll_total, 2);
        assert_eq!(snapshot.records_consumed_total, 7);
        assert_eq!(snapshot.fetch_total, 1);
        assert_eq!(snapshot.commit_total, 1);
        assert_eq!(snapshot.heartbeat_total, 1);
        assert_eq!(snapshot.rebalance_total, 1);
    }

    #[test]
    fn clones_share_the_same_aggregate() {
        let metrics = ConsumerMetrics::default();
        let clone = metrics.clone();
        clone.record_poll();
        assert_eq!(metrics.snapshot(BufferPoolStats::default()).poll_total, 1);
    }
}
