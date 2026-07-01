//! Admin client metrics.
//!
//! kacrab tracks native request-level counters for the admin client and exposes
//! them via [`AdminClient::metrics`](super::AdminClient::metrics), the analogue
//! of Java's `Admin.metrics()`. The counters are shared behind an `Arc` so admin
//! client clones report the same aggregate.

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use crate::wire::BufferPoolStats;

/// Shared, cheap-to-clone request metrics for the admin client.
#[derive(Debug, Clone, Default)]
pub(super) struct AdminMetrics {
    inner: Arc<AdminMetricsInner>,
}

#[derive(Debug, Default)]
#[expect(
    clippy::struct_field_names,
    reason = "All counters are request-scoped."
)]
struct AdminMetricsInner {
    request_total: AtomicU64,
    request_error_total: AtomicU64,
    request_latency_nanos_total: AtomicU64,
    request_latency_nanos_max: AtomicU64,
}

impl AdminMetrics {
    /// Record one completed broker request and its latency.
    pub(super) fn record(&self, latency: Duration, is_error: bool) {
        let nanos = u64::try_from(latency.as_nanos()).unwrap_or(u64::MAX);
        let _previous = self.inner.request_total.fetch_add(1, Ordering::Relaxed);
        if is_error {
            let _previous = self
                .inner
                .request_error_total
                .fetch_add(1, Ordering::Relaxed);
        }
        let _previous = self
            .inner
            .request_latency_nanos_total
            .fetch_add(nanos, Ordering::Relaxed);
        let _previous = self
            .inner
            .request_latency_nanos_max
            .fetch_max(nanos, Ordering::Relaxed);
    }

    /// Take a point-in-time snapshot, folding in the current wire buffer-pool
    /// counters.
    pub(super) fn snapshot(&self, buffer_pool: BufferPoolStats) -> AdminMetricsSnapshot {
        let request_total = self.inner.request_total.load(Ordering::Relaxed);
        let request_latency_nanos_total = self
            .inner
            .request_latency_nanos_total
            .load(Ordering::Relaxed);
        let request_latency_avg_nanos = request_latency_nanos_total
            .checked_div(request_total)
            .unwrap_or(0);
        AdminMetricsSnapshot {
            request_total,
            request_error_total: self.inner.request_error_total.load(Ordering::Relaxed),
            request_latency_avg_nanos,
            request_latency_max_nanos: self.inner.request_latency_nanos_max.load(Ordering::Relaxed),
            buffer_pool,
        }
    }
}

/// A snapshot of the admin client's metrics.
///
/// Returned by [`AdminClient::metrics`](super::AdminClient::metrics); kacrab's
/// native analogue of Java's `Admin.metrics()` (a typed snapshot rather than a
/// `Map<MetricName, KafkaMetric>`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdminMetricsSnapshot {
    /// Total broker requests the admin client has sent.
    pub request_total: u64,
    /// Total broker requests that failed at the wire/transport layer.
    pub request_error_total: u64,
    /// Mean request latency, in nanoseconds (0 when no requests have been sent).
    pub request_latency_avg_nanos: u64,
    /// Maximum observed request latency, in nanoseconds.
    pub request_latency_max_nanos: u64,
    /// The wire buffer-pool counters at snapshot time.
    pub buffer_pool: BufferPoolStats,
}
