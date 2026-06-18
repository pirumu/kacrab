//! Low-overhead producer metrics snapshots.

mod registry;

use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use bytes::{Bytes, BytesMut};
pub use registry::{
    KafkaMetric, MetricConfig, MetricName, MetricNameTemplate, MetricQuota, MetricReporter,
    MetricValue, Metrics, MetricsError, SensorId, SensorRecordingLevel,
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

    /// Serialize this snapshot as an uncompressed OTLP `MetricsData` protobuf payload.
    ///
    /// Count metrics are exported as cumulative monotonic `Sum` metrics. Gauge,
    /// duration, and ratio metrics are exported as `Gauge` metrics with one
    /// `NumberDataPoint` each.
    #[must_use]
    pub fn to_otlp_metrics_data(self, time_unix_nanos: u64) -> Bytes {
        self.to_otlp_metrics_data_with_kafka_metrics(time_unix_nanos, [])
    }

    /// Serialize this snapshot plus application Kafka metrics as OTLP metrics.
    ///
    /// Application metrics use their [`MetricName`] description and tags, and
    /// are exported as gauge number data points because this Rust-native
    /// `KafkaMetric` facade stores a value provider but not a Java metric type.
    #[must_use]
    pub fn to_otlp_metrics_data_with_kafka_metrics<'a, I>(
        self,
        time_unix_nanos: u64,
        application_metrics: I,
    ) -> Bytes
    where
        I: IntoIterator<Item = &'a KafkaMetric>,
    {
        let mut scope_metrics = BytesMut::new();
        for (name, value) in self.as_metric_map() {
            encode_message_field(&mut scope_metrics, 2, |metric| {
                encode_string_field(metric, 1, name);
                encode_string_field(metric, 2, producer_metric_description(name));
                encode_string_field(metric, 3, producer_metric_unit(value));
                match value {
                    ProducerMetricValue::Count(count) => {
                        encode_message_field(metric, 7, |sum| {
                            encode_int_data_point(sum, 1, time_unix_nanos, u64_to_i64(count));
                            encode_varint_field(sum, 2, 2);
                            encode_bool_field(sum, 3, true);
                        });
                    },
                    ProducerMetricValue::Gauge(gauge) => {
                        encode_message_field(metric, 5, |gauge_metric| {
                            encode_int_data_point(
                                gauge_metric,
                                1,
                                time_unix_nanos,
                                usize_to_i64(gauge),
                            );
                        });
                    },
                    ProducerMetricValue::Duration(duration) => {
                        encode_message_field(metric, 5, |gauge_metric| {
                            encode_number_data_point(
                                gauge_metric,
                                1,
                                time_unix_nanos,
                                duration.as_secs_f64(),
                            );
                        });
                    },
                    ProducerMetricValue::Ratio(ratio) => {
                        encode_message_field(metric, 5, |gauge_metric| {
                            encode_number_data_point(gauge_metric, 1, time_unix_nanos, ratio);
                        });
                    },
                }
            });
        }
        for metric in application_metrics {
            encode_kafka_metric(&mut scope_metrics, time_unix_nanos, metric);
        }

        scope_metrics_to_metrics_data(scope_metrics)
    }
}

fn scope_metrics_to_metrics_data(scope_metrics: BytesMut) -> Bytes {
    let mut resource_metrics = BytesMut::new();
    encode_message_bytes_field(&mut resource_metrics, 2, &scope_metrics.freeze());
    let mut metrics_data = BytesMut::new();
    encode_message_bytes_field(&mut metrics_data, 1, &resource_metrics.freeze());
    metrics_data.freeze()
}

fn encode_kafka_metric(
    scope_metrics: &mut BytesMut,
    time_unix_nanos: u64,
    kafka_metric: &KafkaMetric,
) {
    let metric_name = kafka_metric.metric_name();
    encode_message_field(scope_metrics, 2, |metric| {
        encode_string_field(metric, 1, metric_name.name());
        encode_string_field(metric, 2, metric_name.description());
        encode_string_field(metric, 3, "1");
        encode_message_field(metric, 5, |gauge_metric| {
            encode_number_data_point_with_tags(
                gauge_metric,
                1,
                time_unix_nanos,
                kafka_metric.metric_value(),
                metric_name.tags(),
            );
        });
    });
}

fn producer_metric_description(name: &str) -> &'static str {
    match name {
        "records_appended" => "records accepted into the producer accumulator",
        "produce_request_count" => "produce requests sent to brokers",
        "produce_record_count" => "records included in produce requests",
        "produce_retry_count" => "retry attempts after retryable produce failures",
        "produce_error_count" => "produce responses or dispatches that reported an error",
        "requeue_count" => "batches requeued because routing was incomplete",
        "queue_depth_bytes" => "bytes currently buffered in the accumulator",
        "queue_depth_records" => "records currently buffered in the accumulator",
        "in_flight_dispatches" => "producer dispatch tasks currently in flight",
        "average_batch_fill_ratio" => "average drained batch fill ratio",
        "flush_count" => "explicit flush calls",
        "flush_total_latency" => "total wall-clock latency spent in flush calls",
        "metadata_wait_count" => "metadata wait operations",
        "metadata_wait_total_latency" => "total latency spent waiting for metadata",
        "transaction_init_count" => "init_transactions calls",
        "transaction_init_total_latency" => "total latency spent in init_transactions",
        "transaction_begin_count" => "begin_transaction calls",
        "transaction_begin_total_latency" => "total latency spent in begin_transaction",
        "send_offsets_to_transaction_count" => "send_offsets_to_transaction calls",
        "send_offsets_to_transaction_total_latency" => {
            "total latency spent in send_offsets_to_transaction"
        },
        "transaction_commit_count" => "commit_transaction calls",
        "transaction_commit_total_latency" => "total latency spent in commit_transaction",
        "transaction_abort_count" => "abort_transaction calls",
        "transaction_abort_total_latency" => "total latency spent in abort_transaction",
        _ => "",
    }
}

const fn producer_metric_unit(value: ProducerMetricValue) -> &'static str {
    match value {
        ProducerMetricValue::Duration(_) => "s",
        ProducerMetricValue::Count(_)
        | ProducerMetricValue::Gauge(_)
        | ProducerMetricValue::Ratio(_) => "1",
    }
}

fn encode_number_data_point(
    parent: &mut BytesMut,
    field_number: u32,
    time_unix_nanos: u64,
    value: f64,
) {
    encode_message_field(parent, field_number, |point| {
        encode_fixed64_field(point, 3, time_unix_nanos);
        encode_double_field(point, 4, value);
    });
}

fn encode_number_data_point_with_tags(
    parent: &mut BytesMut,
    field_number: u32,
    time_unix_nanos: u64,
    value: f64,
    tags: &BTreeMap<String, String>,
) {
    encode_message_field(parent, field_number, |point| {
        encode_fixed64_field(point, 3, time_unix_nanos);
        encode_double_field(point, 4, value);
        for (key, value) in tags {
            encode_string_attribute(point, key, value);
        }
    });
}

fn encode_string_attribute(parent: &mut BytesMut, key: &str, value: &str) {
    encode_message_field(parent, 7, |attribute| {
        encode_string_field(attribute, 1, key);
        encode_message_field(attribute, 2, |any_value| {
            encode_string_field(any_value, 1, value);
        });
    });
}

fn encode_int_data_point(
    parent: &mut BytesMut,
    field_number: u32,
    time_unix_nanos: u64,
    value: i64,
) {
    encode_message_field(parent, field_number, |point| {
        encode_fixed64_field(point, 3, time_unix_nanos);
        encode_sfixed64_field(point, 6, value);
    });
}

fn encode_message_field<F>(parent: &mut BytesMut, field_number: u32, encode: F)
where
    F: FnOnce(&mut BytesMut),
{
    let mut nested = BytesMut::new();
    encode(&mut nested);
    encode_message_bytes_field(parent, field_number, &nested.freeze());
}

fn encode_message_bytes_field(parent: &mut BytesMut, field_number: u32, value: &Bytes) {
    encode_key(parent, field_number, 2);
    encode_varint(parent, u64::try_from(value.len()).unwrap_or(u64::MAX));
    parent.extend_from_slice(value.as_ref());
}

fn encode_string_field(buf: &mut BytesMut, field_number: u32, value: &str) {
    if value.is_empty() {
        return;
    }
    encode_key(buf, field_number, 2);
    encode_varint(buf, u64::try_from(value.len()).unwrap_or(u64::MAX));
    buf.extend_from_slice(value.as_bytes());
}

fn encode_varint_field(buf: &mut BytesMut, field_number: u32, value: u64) {
    encode_key(buf, field_number, 0);
    encode_varint(buf, value);
}

fn encode_bool_field(buf: &mut BytesMut, field_number: u32, value: bool) {
    if value {
        encode_varint_field(buf, field_number, 1);
    }
}

fn encode_fixed64_field(buf: &mut BytesMut, field_number: u32, value: u64) {
    encode_key(buf, field_number, 1);
    buf.extend_from_slice(&value.to_le_bytes());
}

fn encode_sfixed64_field(buf: &mut BytesMut, field_number: u32, value: i64) {
    encode_key(buf, field_number, 1);
    buf.extend_from_slice(&value.to_le_bytes());
}

fn encode_double_field(buf: &mut BytesMut, field_number: u32, value: f64) {
    encode_fixed64_field(buf, field_number, value.to_bits());
}

fn encode_key(buf: &mut BytesMut, field_number: u32, wire_type: u8) {
    let key = (u64::from(field_number) << 3) | u64::from(wire_type);
    encode_varint(buf, key);
}

fn encode_varint(buf: &mut BytesMut, mut value: u64) {
    while value >= 0x80 {
        let byte = u8::try_from(value & 0x7f).unwrap_or(0) | 0x80;
        buf.extend_from_slice(&[byte]);
        value >>= 7;
    }
    let byte = u8::try_from(value).unwrap_or(0);
    buf.extend_from_slice(&[byte]);
}

fn u64_to_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn usize_to_i64(value: usize) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
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

#[cfg(test)]
mod tests {
    #![allow(
        clippy::expect_used,
        clippy::missing_assert_message,
        clippy::unwrap_used,
        reason = "Unit test fixtures fail fastest with contextual unwrap/expect calls."
    )]

    use std::{
        sync::{Arc, Mutex},
        time::Duration,
    };

    use super::{
        KafkaMetric, MetricConfig, MetricName, MetricNameTemplate, MetricQuota, MetricReporter,
        MetricValue, Metrics, MetricsError, ProducerMetricsSnapshot, SensorRecordingLevel,
    };

    #[test]
    fn metric_name_identity_matches_java_name_group_and_tags_only() {
        let first = MetricName::new("request-rate", "producer-metrics")
            .with_description("first description")
            .tag("client-id", "a");
        let second = MetricName::new("request-rate", "producer-metrics")
            .with_description("different description")
            .tag("client-id", "a");

        assert_eq!(first, second);
        assert_eq!(first.name(), "request-rate");
        assert_eq!(first.group(), "producer-metrics");
        assert_eq!(first.description(), "first description");
        assert_eq!(first.tags().get("client-id").map(String::as_str), Some("a"));
    }

    #[test]
    fn metric_name_merges_default_tags_and_explicit_tags_like_java() {
        let metrics = Metrics::new()
            .with_default_tag("client-id", "producer-a")
            .with_default_tag("thread-id", "sender-1");

        let metric = metrics.metric_name_with_tags(
            "request-rate",
            "producer-metrics",
            "request rate",
            [("client-id", "producer-b"), ("topic", "orders")],
        );

        assert_eq!(metric.name(), "request-rate");
        assert_eq!(metric.group(), "producer-metrics");
        assert_eq!(metric.description(), "request rate");
        assert_eq!(
            metric.tags().get("client-id").map(String::as_str),
            Some("producer-b")
        );
        assert_eq!(
            metric.tags().get("thread-id").map(String::as_str),
            Some("sender-1")
        );
        assert_eq!(
            metric.tags().get("topic").map(String::as_str),
            Some("orders")
        );
    }

    #[test]
    fn metric_name_template_instance_validates_runtime_tags_like_java() {
        let metrics = Metrics::new().with_default_tag("client-id", "producer-a");
        let template = MetricNameTemplate::new(
            "record-send-rate",
            "producer-topic-metrics",
            "record send rate",
            ["client-id", "topic"],
        );

        let metric_name = metrics
            .metric_instance(&template, [("topic", "orders")])
            .expect("template tags match defaults plus runtime tags");

        assert_eq!(metric_name.name(), "record-send-rate");
        assert_eq!(metric_name.group(), "producer-topic-metrics");
        assert_eq!(metric_name.description(), "record send rate");
        assert_eq!(
            metric_name.tags().get("client-id").map(String::as_str),
            Some("producer-a")
        );
        assert_eq!(
            metric_name.tags().get("topic").map(String::as_str),
            Some("orders")
        );

        let same_identity = MetricNameTemplate::new(
            "record-send-rate",
            "producer-topic-metrics",
            "different description",
            ["topic", "client-id"],
        );
        assert_eq!(template, same_identity);

        let mismatch = metrics
            .metric_instance(&template, [("partition", "0")])
            .expect_err("runtime tags must match template tags");
        assert!(matches!(mismatch, MetricsError::InvalidMetricConfig { .. }));
    }

    #[test]
    fn metrics_reporter_lifecycle_matches_java_add_remove_and_close() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let mut metrics = Metrics::new();
        metrics.add_reporter(RecordingReporter {
            events: Arc::clone(&events),
        });
        let metric_name = metrics.metric_name("count", "producer-metrics", "request count");

        metrics
            .add_metric(metric_name.clone(), || MetricValue::Number(3.0))
            .expect("metric should register");
        assert_metric_value(&metrics, &metric_name, 3.0);
        let removed = metrics.remove_metric(&metric_name).expect("removed metric");
        assert_eq!(removed.metric_name(), &metric_name);
        metrics.close();

        let events = events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        assert_eq!(
            events,
            vec![
                "init:0".to_owned(),
                "change:count".to_owned(),
                "remove:count".to_owned(),
                "close".to_owned(),
            ]
        );
    }

    #[test]
    fn add_metric_if_absent_returns_existing_metric_without_reporter_change_like_java() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let mut metrics = Metrics::new();
        metrics.add_reporter(RecordingReporter {
            events: Arc::clone(&events),
        });
        let metric_name = metrics.metric_name("count", "producer-metrics", "request count");

        let first = metrics.add_metric_if_absent(metric_name.clone(), || MetricValue::Number(1.0));
        let second = metrics.add_metric_if_absent(metric_name.clone(), || MetricValue::Number(2.0));

        assert!((first.metric_value() - 1.0).abs() < f64::EPSILON);
        assert!((second.metric_value() - 1.0).abs() < f64::EPSILON);
        assert_metric_value(&metrics, &metric_name, 1.0);
        let events = events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        assert_eq!(events, vec!["init:0".to_owned(), "change:count".to_owned()]);
    }

    #[test]
    fn remove_metric_if_present_is_noop_for_missing_metric_like_java() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let mut metrics = Metrics::new();
        metrics.add_reporter(RecordingReporter {
            events: Arc::clone(&events),
        });
        let missing = metrics.metric_name("missing", "producer-metrics", "");

        let removed = metrics.remove_metric_if_present(&missing);

        assert!(removed.is_none());
        let events = events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        assert_eq!(events, vec!["init:0".to_owned()]);
    }

    #[test]
    fn sensor_records_to_stats_and_parent_sensors_like_java() {
        let mut metrics = Metrics::new();
        let parent = metrics.sensor("parent");
        let child = metrics.sensor_with_parents("child", SensorRecordingLevel::Info, [parent]);
        let child_metric = metrics.metric_name("child-total", "producer-metrics", "");
        let parent_metric = metrics.metric_name("parent-total", "producer-metrics", "");

        metrics
            .sensor_add_total(child, child_metric.clone())
            .expect("child metric");
        metrics
            .sensor_add_total(parent, parent_metric.clone())
            .expect("parent metric");
        metrics.record(child, 2.5).expect("record child");
        metrics.record(child, 1.5).expect("record child");

        assert_eq!(metrics.sensor_name(child).expect("sensor name"), "child");
        assert_metric_value(&metrics, &child_metric, 4.0);
        assert_metric_value(&metrics, &parent_metric, 4.0);
    }

    #[test]
    fn sensor_record_once_uses_java_record_default_value() {
        let mut metrics = Metrics::new();
        let sensor = metrics.sensor("request-count");
        let metric_name = metrics.metric_name("request-count-total", "producer-metrics", "");

        metrics
            .sensor_add_total(sensor, metric_name.clone())
            .expect("total metric");
        metrics.record_once(sensor).expect("default record");
        metrics.record_once(sensor).expect("default record");

        assert_metric_value(&metrics, &metric_name, 2.0);
    }

    #[test]
    fn sensor_has_metrics_reports_registered_stats_like_java() {
        let mut metrics = Metrics::new();
        let sensor = metrics.sensor("sensor");
        let first = metrics.metric_name("name1", "group1", "description1");
        let second = metrics.metric_name("name2", "group2", "description2");

        assert!(!metrics.sensor_has_metrics(sensor).expect("sensor exists"));

        metrics
            .sensor_add_total(sensor, first)
            .expect("first metric");
        assert!(metrics.sensor_has_metrics(sensor).expect("sensor exists"));

        metrics
            .sensor_add_count(sensor, second)
            .expect("second metric");
        assert!(metrics.sensor_has_metrics(sensor).expect("sensor exists"));
    }

    #[test]
    fn sensor_metrics_returns_sensor_metric_list_copy_like_java() {
        let mut metrics = Metrics::new();
        let sensor = metrics.sensor("sensor");
        let first = metrics.metric_name("name1", "group1", "description1");
        let second = metrics.metric_name("name2", "group2", "description2");

        assert!(
            metrics
                .sensor_metrics(sensor)
                .expect("sensor exists")
                .is_empty()
        );

        metrics
            .sensor_add_total(sensor, first.clone())
            .expect("first metric");
        metrics
            .sensor_add_count(sensor, second.clone())
            .expect("second metric");

        let sensor_metrics = metrics.sensor_metrics(sensor).expect("sensor exists");
        let names = sensor_metrics
            .iter()
            .map(|metric| metric.metric_name().clone())
            .collect::<Vec<_>>();
        assert_eq!(names, vec![first, second]);
        assert!((sensor_metrics[0].metric_value() - 0.0).abs() < f64::EPSILON);
        metrics.record_once(sensor).expect("default record");
        assert!((sensor_metrics[0].metric_value() - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn sensor_expiration_tracks_last_record_time_like_java() {
        let mut metrics = Metrics::new();
        let default_sensor = metrics.sensor("default-sensor");
        let expiring_sensor = metrics.sensor_with_expiration(
            "expiring-sensor",
            SensorRecordingLevel::Info,
            Duration::from_mins(1),
            [],
        );
        let total = metrics.metric_name("total", "producer-metrics", "");

        metrics
            .sensor_add_total(expiring_sensor, total.clone())
            .expect("total metric");

        assert!(
            !metrics
                .sensor_has_expired_at_ms(default_sensor, u64::MAX)
                .expect("default sensor exists")
        );
        assert!(
            !metrics
                .sensor_has_expired_at_ms(expiring_sensor, 60_000)
                .expect("expiring sensor exists")
        );
        assert!(
            metrics
                .sensor_has_expired_at_ms(expiring_sensor, 60_001)
                .expect("expiring sensor exists")
        );

        metrics
            .record_at_ms(expiring_sensor, 2.0, 30_000)
            .expect("record with timestamp");
        assert_metric_value(&metrics, &total, 2.0);
        assert!(
            !metrics
                .sensor_has_expired_at_ms(expiring_sensor, 90_000)
                .expect("expiring sensor exists")
        );
        assert!(
            metrics
                .sensor_has_expired_at_ms(expiring_sensor, 90_001)
                .expect("expiring sensor exists")
        );
    }

    #[test]
    fn expire_sensors_removes_expired_sensors_metrics_and_children_like_java() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let mut metrics = Metrics::new();
        metrics.add_reporter(RecordingReporter {
            events: Arc::clone(&events),
        });
        let parent = metrics.sensor_with_expiration(
            "parent",
            SensorRecordingLevel::Info,
            Duration::from_mins(1),
            [],
        );
        let child = metrics.sensor_with_parents("child", SensorRecordingLevel::Info, [parent]);
        let survivor = metrics.sensor_with_expiration(
            "survivor",
            SensorRecordingLevel::Info,
            Duration::from_mins(2),
            [],
        );
        let parent_metric = metrics.metric_name("parent-total", "producer-metrics", "");
        let child_metric = metrics.metric_name("child-total", "producer-metrics", "");
        let survivor_metric = metrics.metric_name("survivor-total", "producer-metrics", "");

        metrics
            .sensor_add_total(parent, parent_metric.clone())
            .expect("parent metric");
        metrics
            .sensor_add_total(child, child_metric.clone())
            .expect("child metric");
        metrics
            .sensor_add_total(survivor, survivor_metric.clone())
            .expect("survivor metric");

        let removed = metrics.expire_sensors_at_ms(60_001);

        assert_eq!(removed, 2);
        assert!(metrics.metric(&parent_metric).is_none());
        assert!(metrics.metric(&child_metric).is_none());
        assert!(metrics.metric(&survivor_metric).is_some());
        assert!(matches!(
            metrics.sensor_has_metrics(parent),
            Err(MetricsError::UnknownSensor { .. })
        ));
        assert!(matches!(
            metrics.sensor_has_metrics(child),
            Err(MetricsError::UnknownSensor { .. })
        ));
        assert!(metrics.sensor_has_metrics(survivor).expect("survivor"));

        let events = events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        assert_eq!(
            events,
            vec![
                "init:0".to_owned(),
                "change:parent-total".to_owned(),
                "change:child-total".to_owned(),
                "change:survivor-total".to_owned(),
                "remove:parent-total".to_owned(),
                "remove:child-total".to_owned(),
            ]
        );
    }

    #[test]
    fn sensor_value_stat_keeps_latest_recorded_value_like_java() {
        let mut metrics = Metrics::new();
        let sensor = metrics.sensor("request-size");
        let metric_name = metrics.metric_name("request-size-value", "producer-metrics", "");

        metrics
            .sensor_add_value(sensor, metric_name.clone())
            .expect("value metric");
        metrics.record(sensor, 2.0).expect("record value");
        metrics.record(sensor, 7.0).expect("record value");

        assert_metric_value(&metrics, &metric_name, 7.0);
    }

    #[test]
    fn sensor_avg_stat_reports_nan_until_records_then_average_like_java() {
        let mut metrics = Metrics::new();
        let sensor = metrics.sensor("request-size");
        let metric_name = metrics.metric_name("request-size-avg", "producer-metrics", "");

        metrics
            .sensor_add_avg(sensor, metric_name.clone())
            .expect("avg metric");
        assert!(
            metrics
                .metric(&metric_name)
                .unwrap()
                .metric_value()
                .is_nan()
        );

        metrics.record(sensor, 2.0).expect("record avg");
        metrics.record(sensor, 4.0).expect("record avg");
        metrics.record(sensor, 9.0).expect("record avg");

        assert_metric_value(&metrics, &metric_name, 5.0);
    }

    #[test]
    fn sensor_min_max_stats_report_nan_until_records_like_java() {
        let mut metrics = Metrics::new();
        let sensor = metrics.sensor("request-size");
        let min_metric = metrics.metric_name("request-size-min", "producer-metrics", "");
        let max_metric = metrics.metric_name("request-size-max", "producer-metrics", "");

        metrics
            .sensor_add_min(sensor, min_metric.clone())
            .expect("min metric");
        metrics
            .sensor_add_max(sensor, max_metric.clone())
            .expect("max metric");
        assert!(metrics.metric(&min_metric).unwrap().metric_value().is_nan());
        assert!(metrics.metric(&max_metric).unwrap().metric_value().is_nan());

        metrics.record(sensor, 7.0).expect("record extrema");
        metrics.record(sensor, 2.0).expect("record extrema");
        metrics.record(sensor, 9.0).expect("record extrema");
        metrics.record(sensor, 4.0).expect("record extrema");

        assert_metric_value(&metrics, &min_metric, 2.0);
        assert_metric_value(&metrics, &max_metric, 9.0);
    }

    #[test]
    fn sensor_count_stat_counts_record_calls_like_java_cumulative_count() {
        let mut metrics = Metrics::new();
        let sensor = metrics.sensor("request-count");
        let metric_name = metrics.metric_name("request-count-total", "producer-metrics", "");

        metrics
            .sensor_add_count(sensor, metric_name.clone())
            .expect("count metric");
        metrics.record(sensor, 7.0).expect("record count");
        metrics.record(sensor, 2.0).expect("record count");
        metrics.record(sensor, 9.0).expect("record count");

        assert_metric_value(&metrics, &metric_name, 3.0);
    }

    #[test]
    fn sensor_rate_stat_uses_java_window_size_rule() {
        let mut metrics = Metrics::new();
        let sensor = metrics.sensor("request-size");
        let metric_name = metrics.metric_name("request-size-rate", "producer-metrics", "");

        metrics
            .sensor_add_rate(sensor, metric_name.clone())
            .expect("rate metric");
        metrics
            .record_at_ms(sensor, 30.0, 1_000)
            .expect("record rate value");

        assert_metric_value_at_ms(&metrics, &metric_name, 1_000, 1.0);
    }

    #[test]
    fn rate_quota_check_uses_supplied_time_like_java() {
        let mut metrics = Metrics::new();
        let sensor = metrics.sensor("request-size");
        let metric_name = metrics.metric_name("request-size-rate", "producer-metrics", "");

        metrics
            .sensor_add_rate_with_config(
                sensor,
                metric_name.clone(),
                MetricConfig::new().with_quota(MetricQuota::upper_bound(0.5)),
            )
            .expect("rate metric");
        metrics
            .record_with_quota_check_at_ms(sensor, 30.0, 1_000, false)
            .expect("record rate value without quota check");

        assert!(matches!(
            metrics.check_sensor_quotas_at_ms(sensor, 1_000),
            Err(MetricsError::QuotaViolation {
                metric_name: violated,
                value,
                bound,
            }) if violated == metric_name
                && (value - 1.0).abs() < f64::EPSILON
                && (bound - 0.5).abs() < f64::EPSILON
        ));
        metrics
            .check_sensor_quotas_at_ms(sensor, 61_000)
            .expect("expired rate sample should pass quota at supplied time");
    }

    #[test]
    fn token_bucket_quota_refills_continuously_like_java() {
        let mut metrics = Metrics::new();
        let sensor = metrics.sensor("client-quota");
        let metric_name = metrics.metric_name("tokens", "producer-metrics", "");
        let config = MetricConfig::new()
            .with_quota(MetricQuota::upper_bound(5.0))
            .with_time_window_ms(1_000)
            .with_samples(2)
            .expect("valid samples");

        metrics
            .sensor_add_token_bucket_with_config(sensor, metric_name.clone(), config)
            .expect("token bucket metric");

        metrics
            .record_with_quota_check_at_ms(sensor, 7.0, 1_000, false)
            .expect("record over burst without immediate quota check");
        let violation = metrics
            .check_sensor_quotas_at_ms(sensor, 1_000)
            .expect_err("bucket should be exhausted");
        assert!(matches!(
            violation,
            MetricsError::QuotaViolation {
                metric_name: violated,
                value,
                bound,
            } if violated == metric_name
                && (value + 2.0).abs() < f64::EPSILON
                && (bound - 5.0).abs() < f64::EPSILON
        ));

        metrics
            .check_sensor_quotas_at_ms(sensor, 1_400)
            .expect("bucket should refill back to zero after 400ms");
        assert_metric_value_at_ms(&metrics, &metric_name, 1_400, 0.0);
    }

    #[test]
    fn boolean_frequencies_report_normalized_distribution_like_java() {
        let mut metrics = Metrics::new();
        let sensor = metrics.sensor("request-success");
        let false_metric = metrics.metric_name("request-failure-frequency", "producer-metrics", "");
        let true_metric = metrics.metric_name("request-success-frequency", "producer-metrics", "");

        metrics
            .sensor_add_boolean_frequencies(
                sensor,
                Some(false_metric.clone()),
                Some(true_metric.clone()),
            )
            .expect("boolean frequencies");
        metrics
            .record_at_ms(sensor, 1.0, 1_000)
            .expect("record true value");
        metrics
            .record_at_ms(sensor, 0.0, 2_000)
            .expect("record false value");
        metrics
            .record_at_ms(sensor, 1.0, 3_000)
            .expect("record true value");

        assert_metric_value_at_ms(&metrics, &false_metric, 3_000, 1.0 / 3.0);
        assert_metric_value_at_ms(&metrics, &true_metric, 3_000, 2.0 / 3.0);
    }

    #[test]
    fn sensor_meter_registers_rate_and_total_like_java_meter() {
        let mut metrics = Metrics::new();
        let sensor = metrics.sensor("request-size");
        let rate_metric = metrics.metric_name("request-size-rate", "producer-metrics", "");
        let total_metric = metrics.metric_name("request-size-total", "producer-metrics", "");

        metrics
            .sensor_add_meter(sensor, rate_metric.clone(), total_metric.clone())
            .expect("meter metrics");
        metrics
            .record_at_ms(sensor, 30.0, 1_000)
            .expect("record meter value");
        metrics
            .record_at_ms(sensor, 15.0, 31_000)
            .expect("record meter value");

        assert_metric_value(&metrics, &total_metric, 45.0);
        assert_metric_value_at_ms(&metrics, &rate_metric, 31_000, 1.5);
    }

    #[test]
    fn sensor_add_duplicate_metric_on_same_sensor_is_noop_like_java() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let mut metrics = Metrics::new();
        metrics.add_reporter(RecordingReporter {
            events: Arc::clone(&events),
        });
        let sensor = metrics.sensor("request-size");
        let metric_name = metrics.metric_name("request-size-total", "producer-metrics", "");

        metrics
            .sensor_add_total(sensor, metric_name.clone())
            .expect("first metric");
        metrics.record(sensor, 2.0).expect("record total");
        metrics
            .sensor_add_total(sensor, metric_name.clone())
            .expect("duplicate metric should be a no-op");
        metrics.record(sensor, 3.0).expect("record total");

        assert_metric_value(&metrics, &metric_name, 5.0);
        let events = events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        assert_eq!(
            events,
            vec!["init:0".to_owned(), "change:request-size-total".to_owned()]
        );
    }

    #[test]
    fn sensor_check_quotas_reports_upper_and_lower_violations_like_java() {
        let mut metrics = Metrics::new();
        let sensor = metrics.sensor("quota-sensor");
        let total_metric = metrics.metric_name("credits-total", "producer-metrics", "");
        let value_metric = metrics.metric_name("credits-value", "producer-metrics", "");

        metrics
            .sensor_add_total_with_quota(
                sensor,
                total_metric.clone(),
                MetricQuota::upper_bound(2.0),
            )
            .expect("upper quota metric");
        metrics
            .sensor_add_value_with_quota(sensor, value_metric, MetricQuota::lower_bound(0.0))
            .expect("lower quota metric");

        metrics
            .record_at_ms(sensor, 1.0, 1)
            .expect("record in bounds");
        metrics
            .check_sensor_quotas_at_ms(sensor, 1)
            .expect("quota should pass");

        metrics
            .record_with_quota_check_at_ms(sensor, 2.0, 2, false)
            .expect("record above upper bound");
        let upper_violation = metrics
            .check_sensor_quotas_at_ms(sensor, 2)
            .expect_err("quota should fail");
        assert!(matches!(
            upper_violation,
            MetricsError::QuotaViolation {
                metric_name,
                value,
                bound,
            } if metric_name == total_metric
                && (value - 3.0).abs() < f64::EPSILON
                && (bound - 2.0).abs() < f64::EPSILON
        ));

        let value_only = metrics.sensor("value-only-quota-sensor");
        let value_only_metric = metrics.metric_name("credits-value-only", "producer-metrics", "");
        metrics
            .sensor_add_value_with_quota(
                value_only,
                value_only_metric.clone(),
                MetricQuota::lower_bound(0.0),
            )
            .expect("lower quota metric");
        metrics
            .record_with_quota_check_at_ms(value_only, -1.0, 3, false)
            .expect("record below lower bound");
        let lower_violation = metrics
            .check_sensor_quotas_at_ms(value_only, 3)
            .expect_err("quota should fail");
        assert!(matches!(
            lower_violation,
            MetricsError::QuotaViolation {
                metric_name,
                value,
                bound,
            } if metric_name == value_only_metric
                && (value + 1.0).abs() < f64::EPSILON
                && bound.abs() < f64::EPSILON
        ));
    }

    #[test]
    fn metric_config_defaults_and_chaining_match_java_shape() {
        let default_config = MetricConfig::new();

        assert_eq!(default_config.quota(), None);
        assert_eq!(default_config.samples(), 2);
        assert_eq!(default_config.event_window(), u64::MAX);
        assert_eq!(default_config.time_window_ms(), 30_000);
        assert!(default_config.tags().is_empty());
        assert_eq!(default_config.record_level(), SensorRecordingLevel::Info);

        let config = MetricConfig::new()
            .with_quota(MetricQuota::lower_bound(1.5))
            .with_event_window(42)
            .with_time_window_ms(750)
            .with_tag("client-id", "producer-a")
            .with_record_level(SensorRecordingLevel::Debug)
            .with_samples(3)
            .expect("valid samples");

        assert_eq!(config.quota(), Some(MetricQuota::lower_bound(1.5)));
        assert_eq!(config.samples(), 3);
        assert_eq!(config.event_window(), 42);
        assert_eq!(config.time_window_ms(), 750);
        assert_eq!(
            config.tags().get("client-id").map(String::as_str),
            Some("producer-a")
        );
        assert_eq!(config.record_level(), SensorRecordingLevel::Debug);
        assert!(MetricConfig::new().with_samples(0).is_err());
    }

    #[test]
    fn updating_metric_config_is_reflected_in_sensor_quota_checks_like_java() {
        let mut metrics = Metrics::new();
        let sensor = metrics.sensor("quota-config-sensor");
        let metric_name = metrics.metric_name("credits", "producer-metrics", "");

        metrics
            .sensor_add_total_with_config(
                sensor,
                metric_name.clone(),
                MetricConfig::new().with_quota(MetricQuota::upper_bound(5.0)),
            )
            .expect("quota metric");
        metrics
            .record_with_quota_check_at_ms(sensor, 10.0, 1, false)
            .expect("record above original bound");
        assert!(matches!(
            metrics.check_sensor_quotas_at_ms(sensor, 2),
            Err(MetricsError::QuotaViolation {
                metric_name: violated,
                value,
                bound,
            }) if violated == metric_name
                && (value - 10.0).abs() < f64::EPSILON
                && (bound - 5.0).abs() < f64::EPSILON
        ));

        metrics
            .metric(&metric_name)
            .expect("registered metric")
            .set_metric_config(MetricConfig::new().with_quota(MetricQuota::upper_bound(10.0)));

        metrics
            .check_sensor_quotas_at_ms(sensor, 3)
            .expect("updated quota should pass");
    }

    #[test]
    fn record_with_quota_check_enforces_after_recording_like_java() {
        let mut metrics = Metrics::new();
        let sensor = metrics.sensor("checked-record-sensor");
        let metric_name = metrics.metric_name("credits-total", "producer-metrics", "");

        metrics
            .sensor_add_total_with_quota(sensor, metric_name.clone(), MetricQuota::upper_bound(5.0))
            .expect("quota metric");
        metrics
            .record_with_quota_check_at_ms(sensor, 3.0, 1, true)
            .expect("record in bounds");
        let violation = metrics
            .record_with_quota_check_at_ms(sensor, 4.0, 2, true)
            .expect_err("record should violate quota");

        assert!(matches!(
            violation,
            MetricsError::QuotaViolation {
                metric_name: violated,
                value,
                bound,
            } if violated == metric_name
                && (value - 7.0).abs() < f64::EPSILON
                && (bound - 5.0).abs() < f64::EPSILON
        ));
        assert_metric_value(&metrics, &metric_name, 7.0);

        metrics
            .record_with_quota_check_at_ms(sensor, 10.0, 3, false)
            .expect("quota check disabled");
        assert_metric_value(&metrics, &metric_name, 17.0);
    }

    #[test]
    fn sensor_recording_level_filters_lower_priority_records_like_java() {
        let mut metrics = Metrics::new().with_recording_level(SensorRecordingLevel::Info);
        let debug_sensor = metrics.sensor("debug");
        let metric_name = metrics.metric_name("debug-total", "producer-metrics", "");
        metrics
            .sensor_set_recording_level(debug_sensor, SensorRecordingLevel::Debug)
            .expect("debug level");
        metrics
            .sensor_add_total(debug_sensor, metric_name.clone())
            .expect("debug metric");
        metrics.record(debug_sensor, 1.0).expect("record debug");

        assert_metric_value(&metrics, &metric_name, 0.0);
    }

    #[test]
    fn remove_sensor_removes_child_sensors_and_metrics_like_java() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let mut metrics = Metrics::new();
        metrics.add_reporter(RecordingReporter {
            events: Arc::clone(&events),
        });
        let parent = metrics.sensor("parent");
        let child = metrics.sensor_with_parents("child", SensorRecordingLevel::Info, [parent]);
        let parent_metric = metrics.metric_name("parent-total", "producer-metrics", "");
        let child_metric = metrics.metric_name("child-total", "producer-metrics", "");
        metrics
            .sensor_add_total(parent, parent_metric.clone())
            .expect("parent metric");
        metrics
            .sensor_add_total(child, child_metric.clone())
            .expect("child metric");

        assert!(metrics.remove_sensor("parent"));

        assert!(metrics.metric(&parent_metric).is_none());
        assert!(metrics.metric(&child_metric).is_none());
        assert!(metrics.sensor("parent") != parent);
        assert!(matches!(
            metrics.record(child, 1.0),
            Err(MetricsError::UnknownSensor { sensor }) if sensor == child
        ));
        let events = events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        assert!(events.contains(&"remove:parent-total".to_owned()));
        assert!(events.contains(&"remove:child-total".to_owned()));
    }

    #[test]
    fn producer_snapshot_serializes_otlp_metrics_data_for_telemetry_push() {
        let snapshot = ProducerMetricsSnapshot {
            records_appended: 3,
            produce_request_count: 2,
            produce_record_count: 3,
            produce_retry_count: 1,
            produce_error_count: 0,
            requeue_count: 0,
            queue_depth_bytes: 128,
            queue_depth_records: 4,
            in_flight_dispatches: 1,
            average_batch_fill_ratio: 0.5,
            flush_count: 1,
            flush_total_latency: Duration::from_millis(2),
            metadata_wait_count: 0,
            metadata_wait_total_latency: Duration::ZERO,
            transaction_init_count: 0,
            transaction_init_total_latency: Duration::ZERO,
            transaction_begin_count: 0,
            transaction_begin_total_latency: Duration::ZERO,
            send_offsets_to_transaction_count: 0,
            send_offsets_to_transaction_total_latency: Duration::ZERO,
            transaction_commit_count: 0,
            transaction_commit_total_latency: Duration::ZERO,
            transaction_abort_count: 0,
            transaction_abort_total_latency: Duration::ZERO,
        };

        let payload = snapshot.to_otlp_metrics_data(42);

        assert!(
            payload
                .windows(b"records_appended".len())
                .any(|window| window == b"records_appended")
        );
        assert!(
            payload
                .windows(b"queue_depth_bytes".len())
                .any(|window| window == b"queue_depth_bytes")
        );
        assert!(
            payload
                .windows(b"flush_total_latency".len())
                .any(|window| window == b"flush_total_latency")
        );
        assert!(
            payload
                .windows([0x10, 0x02, 0x18, 0x01].len())
                .any(|window| window == [0x10, 0x02, 0x18, 0x01])
        );
    }

    #[derive(Debug)]
    struct RecordingReporter {
        events: Arc<Mutex<Vec<String>>>,
    }

    impl MetricReporter for RecordingReporter {
        fn init(&self, metrics: &[KafkaMetric]) {
            self.push(format!("init:{}", metrics.len()));
        }

        fn metric_change(&self, metric: &KafkaMetric) {
            self.push(format!("change:{}", metric.metric_name().name()));
        }

        fn metric_removal(&self, metric: &KafkaMetric) {
            self.push(format!("remove:{}", metric.metric_name().name()));
        }

        fn close(&self) {
            self.push("close".to_owned());
        }
    }

    impl RecordingReporter {
        fn push(&self, event: String) {
            self.events
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(event);
        }
    }

    fn assert_metric_value(metrics: &Metrics, metric_name: &MetricName, expected: f64) {
        let value = metrics.metric(metric_name).unwrap().metric_value();
        assert!((value - expected).abs() < f64::EPSILON);
    }

    fn assert_metric_value_at_ms(
        metrics: &Metrics,
        metric_name: &MetricName,
        time_ms: u64,
        expected: f64,
    ) {
        let value = metrics
            .metric(metric_name)
            .unwrap()
            .metric_value_at_ms(time_ms);
        assert!((value - expected).abs() < f64::EPSILON);
    }
}
