//! Low-overhead producer metrics snapshots.

use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

/// Typed value for a named producer metric.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ProducerMetricValue {
    /// Monotonic count metric.
    Count(u64),
    /// Point-in-time unsigned gauge.
    Gauge(usize),
    /// Wall-clock duration metric.
    Duration(Duration),
    /// Floating-point ratio metric.
    Ratio(f64),
}

/// Point-in-time producer metrics for operational diagnostics.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ProducerMetricsSnapshot {
    /// Records accepted into the producer accumulator.
    pub records_appended: u64,
    /// Produce requests sent to brokers.
    pub produce_request_count: u64,
    /// Records included in produce requests sent to brokers.
    pub produce_record_count: u64,
    /// Retry attempts after retryable produce failures.
    pub produce_retry_count: u64,
    /// Produce responses or dispatches that reported an error.
    pub produce_error_count: u64,
    /// Batches requeued because metadata/routing was not yet complete.
    pub requeue_count: u64,
    /// Bytes currently buffered in the accumulator.
    pub queue_depth_bytes: usize,
    /// Records currently buffered in the accumulator.
    pub queue_depth_records: usize,
    /// Producer dispatch tasks currently in flight.
    pub in_flight_dispatches: usize,
    /// Average drained batch fill ratio, capped at `1.0`.
    pub average_batch_fill_ratio: f64,
    /// Number of explicit flush calls.
    pub flush_count: u64,
    /// Total wall-clock latency spent in flush calls.
    pub flush_total_latency: Duration,
    /// Number of successful API-thread metadata wait operations.
    pub metadata_wait_count: u64,
    /// Total wall-clock latency spent waiting for metadata in API calls.
    pub metadata_wait_total_latency: Duration,
    /// Number of `init_transactions` calls.
    pub transaction_init_count: u64,
    /// Total wall-clock latency spent in `init_transactions`.
    pub transaction_init_total_latency: Duration,
    /// Number of `begin_transaction` calls.
    pub transaction_begin_count: u64,
    /// Total wall-clock latency spent in `begin_transaction`.
    pub transaction_begin_total_latency: Duration,
    /// Number of `send_offsets_to_transaction` calls with non-empty offsets.
    pub send_offsets_to_transaction_count: u64,
    /// Total wall-clock latency spent in `send_offsets_to_transaction`.
    pub send_offsets_to_transaction_total_latency: Duration,
    /// Number of `commit_transaction` calls.
    pub transaction_commit_count: u64,
    /// Total wall-clock latency spent in `commit_transaction`.
    pub transaction_commit_total_latency: Duration,
    /// Number of `abort_transaction` calls.
    pub transaction_abort_count: u64,
    /// Total wall-clock latency spent in `abort_transaction`.
    pub transaction_abort_total_latency: Duration,
}

impl ProducerMetricsSnapshot {
    /// Return one named metric value from this snapshot.
    #[must_use]
    pub fn metric(&self, name: &str) -> Option<ProducerMetricValue> {
        match name {
            "records_appended" => Some(ProducerMetricValue::Count(self.records_appended)),
            "produce_request_count" => Some(ProducerMetricValue::Count(self.produce_request_count)),
            "produce_record_count" => Some(ProducerMetricValue::Count(self.produce_record_count)),
            "produce_retry_count" => Some(ProducerMetricValue::Count(self.produce_retry_count)),
            "produce_error_count" => Some(ProducerMetricValue::Count(self.produce_error_count)),
            "requeue_count" => Some(ProducerMetricValue::Count(self.requeue_count)),
            "queue_depth_bytes" => Some(ProducerMetricValue::Gauge(self.queue_depth_bytes)),
            "queue_depth_records" => Some(ProducerMetricValue::Gauge(self.queue_depth_records)),
            "in_flight_dispatches" => Some(ProducerMetricValue::Gauge(self.in_flight_dispatches)),
            "average_batch_fill_ratio" => {
                Some(ProducerMetricValue::Ratio(self.average_batch_fill_ratio))
            },
            "flush_count" => Some(ProducerMetricValue::Count(self.flush_count)),
            "flush_total_latency" => Some(ProducerMetricValue::Duration(self.flush_total_latency)),
            "metadata_wait_count" => Some(ProducerMetricValue::Count(self.metadata_wait_count)),
            "metadata_wait_total_latency" => Some(ProducerMetricValue::Duration(
                self.metadata_wait_total_latency,
            )),
            "transaction_init_count" => {
                Some(ProducerMetricValue::Count(self.transaction_init_count))
            },
            "transaction_init_total_latency" => Some(ProducerMetricValue::Duration(
                self.transaction_init_total_latency,
            )),
            "transaction_begin_count" => {
                Some(ProducerMetricValue::Count(self.transaction_begin_count))
            },
            "transaction_begin_total_latency" => Some(ProducerMetricValue::Duration(
                self.transaction_begin_total_latency,
            )),
            "send_offsets_to_transaction_count" => Some(ProducerMetricValue::Count(
                self.send_offsets_to_transaction_count,
            )),
            "send_offsets_to_transaction_total_latency" => Some(ProducerMetricValue::Duration(
                self.send_offsets_to_transaction_total_latency,
            )),
            "transaction_commit_count" => {
                Some(ProducerMetricValue::Count(self.transaction_commit_count))
            },
            "transaction_commit_total_latency" => Some(ProducerMetricValue::Duration(
                self.transaction_commit_total_latency,
            )),
            "transaction_abort_count" => {
                Some(ProducerMetricValue::Count(self.transaction_abort_count))
            },
            "transaction_abort_total_latency" => Some(ProducerMetricValue::Duration(
                self.transaction_abort_total_latency,
            )),
            _ => None,
        }
    }

    /// Return a read-only-by-value registry of stable producer metrics.
    #[must_use]
    pub fn as_metric_map(&self) -> BTreeMap<&'static str, ProducerMetricValue> {
        [
            "records_appended",
            "produce_request_count",
            "produce_record_count",
            "produce_retry_count",
            "produce_error_count",
            "requeue_count",
            "queue_depth_bytes",
            "queue_depth_records",
            "in_flight_dispatches",
            "average_batch_fill_ratio",
            "flush_count",
            "flush_total_latency",
            "metadata_wait_count",
            "metadata_wait_total_latency",
            "transaction_init_count",
            "transaction_init_total_latency",
            "transaction_begin_count",
            "transaction_begin_total_latency",
            "send_offsets_to_transaction_count",
            "send_offsets_to_transaction_total_latency",
            "transaction_commit_count",
            "transaction_commit_total_latency",
            "transaction_abort_count",
            "transaction_abort_total_latency",
        ]
        .into_iter()
        .filter_map(|name| self.metric(name).map(|value| (name, value)))
        .collect()
    }

    pub(crate) fn is_internal_metric_name(name: &str) -> bool {
        matches!(
            name,
            "records_appended"
                | "produce_request_count"
                | "produce_record_count"
                | "produce_retry_count"
                | "produce_error_count"
                | "requeue_count"
                | "queue_depth_bytes"
                | "queue_depth_records"
                | "in_flight_dispatches"
                | "average_batch_fill_ratio"
                | "flush_count"
                | "flush_total_latency"
                | "metadata_wait_count"
                | "metadata_wait_total_latency"
                | "transaction_init_count"
                | "transaction_init_total_latency"
                | "transaction_begin_count"
                | "transaction_begin_total_latency"
                | "send_offsets_to_transaction_count"
                | "send_offsets_to_transaction_total_latency"
                | "transaction_commit_count"
                | "transaction_commit_total_latency"
                | "transaction_abort_count"
                | "transaction_abort_total_latency"
        )
    }
}

/// Shared producer metrics handle.
#[derive(Debug, Clone, Default)]
pub(crate) struct ProducerMetrics {
    inner: Arc<ProducerMetricsInner>,
}

#[derive(Debug, Default)]
struct ProducerMetricsInner {
    produce_request_count: AtomicU64,
    produce_record_count: AtomicU64,
    produce_retry_count: AtomicU64,
    produce_error_count: AtomicU64,
    requeue_count: AtomicU64,
    batch_fill_per_mille_sum: AtomicU64,
    batch_fill_samples: AtomicU64,
    flush_count: AtomicU64,
    flush_total_latency_ns: AtomicU64,
    metadata_wait_count: AtomicU64,
    metadata_wait_total_latency_ns: AtomicU64,
    transaction_init_count: AtomicU64,
    transaction_init_total_latency_ns: AtomicU64,
    transaction_begin_count: AtomicU64,
    transaction_begin_total_latency_ns: AtomicU64,
    send_offsets_to_transaction_count: AtomicU64,
    send_offsets_to_transaction_total_latency_ns: AtomicU64,
    transaction_commit_count: AtomicU64,
    transaction_commit_total_latency_ns: AtomicU64,
    transaction_abort_count: AtomicU64,
    transaction_abort_total_latency_ns: AtomicU64,
}

impl ProducerMetrics {
    pub(crate) fn record_produce_request(&self) {
        let _previous = self
            .inner
            .produce_request_count
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_produce_batch(
        &self,
        batch_bytes: usize,
        batch_size: usize,
        records: usize,
    ) {
        let records = u64::try_from(records).unwrap_or(u64::MAX);
        let _previous = self
            .inner
            .produce_record_count
            .fetch_add(records, Ordering::Relaxed);

        let batch_size = batch_size.max(1);
        let scaled = batch_bytes
            .saturating_mul(1_000)
            .checked_div(batch_size)
            .unwrap_or(0)
            .min(1_000);
        let scaled = u64::try_from(scaled).unwrap_or(1_000);
        let _previous = self
            .inner
            .batch_fill_per_mille_sum
            .fetch_add(scaled, Ordering::Relaxed);
        let _previous = self
            .inner
            .batch_fill_samples
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_retry(&self) {
        let _previous = self
            .inner
            .produce_retry_count
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_error(&self) {
        let _previous = self
            .inner
            .produce_error_count
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_requeue(&self) {
        let _previous = self.inner.requeue_count.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_flush(&self, latency: Duration) {
        let _previous = self.inner.flush_count.fetch_add(1, Ordering::Relaxed);
        let _previous = self
            .inner
            .flush_total_latency_ns
            .fetch_add(duration_nanos(latency), Ordering::Relaxed);
    }

    pub(crate) fn record_metadata_wait(&self, latency: Duration) {
        let _previous = self
            .inner
            .metadata_wait_count
            .fetch_add(1, Ordering::Relaxed);
        let _previous = self
            .inner
            .metadata_wait_total_latency_ns
            .fetch_add(duration_nanos(latency), Ordering::Relaxed);
    }

    pub(crate) fn record_transaction_init(&self, latency: Duration) {
        let _previous = self
            .inner
            .transaction_init_count
            .fetch_add(1, Ordering::Relaxed);
        let _previous = self
            .inner
            .transaction_init_total_latency_ns
            .fetch_add(duration_nanos(latency), Ordering::Relaxed);
    }

    pub(crate) fn record_transaction_begin(&self, latency: Duration) {
        let _previous = self
            .inner
            .transaction_begin_count
            .fetch_add(1, Ordering::Relaxed);
        let _previous = self
            .inner
            .transaction_begin_total_latency_ns
            .fetch_add(duration_nanos(latency), Ordering::Relaxed);
    }

    pub(crate) fn record_send_offsets_to_transaction(&self, latency: Duration) {
        let _previous = self
            .inner
            .send_offsets_to_transaction_count
            .fetch_add(1, Ordering::Relaxed);
        let _previous = self
            .inner
            .send_offsets_to_transaction_total_latency_ns
            .fetch_add(duration_nanos(latency), Ordering::Relaxed);
    }

    pub(crate) fn record_transaction_commit(&self, latency: Duration) {
        let _previous = self
            .inner
            .transaction_commit_count
            .fetch_add(1, Ordering::Relaxed);
        let _previous = self
            .inner
            .transaction_commit_total_latency_ns
            .fetch_add(duration_nanos(latency), Ordering::Relaxed);
    }

    pub(crate) fn record_transaction_abort(&self, latency: Duration) {
        let _previous = self
            .inner
            .transaction_abort_count
            .fetch_add(1, Ordering::Relaxed);
        let _previous = self
            .inner
            .transaction_abort_total_latency_ns
            .fetch_add(duration_nanos(latency), Ordering::Relaxed);
    }

    pub(crate) fn snapshot(
        &self,
        queue_depth_bytes: usize,
        queue_depth_records: usize,
        in_flight_dispatches: usize,
    ) -> ProducerMetricsSnapshot {
        let batch_fill_samples = self.inner.batch_fill_samples.load(Ordering::Relaxed);
        let batch_fill_sum = self.inner.batch_fill_per_mille_sum.load(Ordering::Relaxed);
        let average_batch_fill_ratio = if batch_fill_samples == 0 {
            0.0
        } else {
            let average_per_mille = batch_fill_sum.checked_div(batch_fill_samples).unwrap_or(0);
            let average_per_mille = u32::try_from(average_per_mille).unwrap_or(1_000);
            f64::from(average_per_mille) / 1_000.0
        };
        let produce_record_count = self.inner.produce_record_count.load(Ordering::Relaxed);
        let queued_records = u64::try_from(queue_depth_records).unwrap_or(u64::MAX);
        let flush_total_latency_ns = self.inner.flush_total_latency_ns.load(Ordering::Relaxed);
        let metadata_wait_total_latency_ns = self
            .inner
            .metadata_wait_total_latency_ns
            .load(Ordering::Relaxed);
        let transaction_init_total_latency_ns = self
            .inner
            .transaction_init_total_latency_ns
            .load(Ordering::Relaxed);
        let transaction_begin_total_latency_ns = self
            .inner
            .transaction_begin_total_latency_ns
            .load(Ordering::Relaxed);
        let send_offsets_to_transaction_total_latency_ns = self
            .inner
            .send_offsets_to_transaction_total_latency_ns
            .load(Ordering::Relaxed);
        let transaction_commit_total_latency_ns = self
            .inner
            .transaction_commit_total_latency_ns
            .load(Ordering::Relaxed);
        let transaction_abort_total_latency_ns = self
            .inner
            .transaction_abort_total_latency_ns
            .load(Ordering::Relaxed);
        ProducerMetricsSnapshot {
            records_appended: produce_record_count.saturating_add(queued_records),
            produce_request_count: self.inner.produce_request_count.load(Ordering::Relaxed),
            produce_record_count,
            produce_retry_count: self.inner.produce_retry_count.load(Ordering::Relaxed),
            produce_error_count: self.inner.produce_error_count.load(Ordering::Relaxed),
            requeue_count: self.inner.requeue_count.load(Ordering::Relaxed),
            queue_depth_bytes,
            queue_depth_records,
            in_flight_dispatches,
            average_batch_fill_ratio,
            flush_count: self.inner.flush_count.load(Ordering::Relaxed),
            flush_total_latency: Duration::from_nanos(flush_total_latency_ns),
            metadata_wait_count: self.inner.metadata_wait_count.load(Ordering::Relaxed),
            metadata_wait_total_latency: Duration::from_nanos(metadata_wait_total_latency_ns),
            transaction_init_count: self.inner.transaction_init_count.load(Ordering::Relaxed),
            transaction_init_total_latency: Duration::from_nanos(transaction_init_total_latency_ns),
            transaction_begin_count: self.inner.transaction_begin_count.load(Ordering::Relaxed),
            transaction_begin_total_latency: Duration::from_nanos(
                transaction_begin_total_latency_ns,
            ),
            send_offsets_to_transaction_count: self
                .inner
                .send_offsets_to_transaction_count
                .load(Ordering::Relaxed),
            send_offsets_to_transaction_total_latency: Duration::from_nanos(
                send_offsets_to_transaction_total_latency_ns,
            ),
            transaction_commit_count: self.inner.transaction_commit_count.load(Ordering::Relaxed),
            transaction_commit_total_latency: Duration::from_nanos(
                transaction_commit_total_latency_ns,
            ),
            transaction_abort_count: self.inner.transaction_abort_count.load(Ordering::Relaxed),
            transaction_abort_total_latency: Duration::from_nanos(
                transaction_abort_total_latency_ns,
            ),
        }
    }
}

fn duration_nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}
