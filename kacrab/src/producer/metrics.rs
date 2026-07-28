//! Low-overhead producer metrics snapshots.

mod registry;
mod sender_registry;

use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use bytes::{Bytes, BytesMut};
pub use registry::{KafkaMetric, MetricName, MetricReporter, MetricValue};
pub(crate) use sender_registry::SenderMetricsRegistry;

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

/// Field type for one metric kind.
macro_rules! producer_metric_ty {
    (Count) => {
        u64
    };
    (Gauge) => {
        usize
    };
    (Duration) => {
        Duration
    };
    (Ratio) => {
        f64
    };
}

/// `ProducerMetricsSnapshot::ZERO` value for one metric kind.
macro_rules! producer_metric_zero {
    (Count) => {
        0
    };
    (Gauge) => {
        0
    };
    (Duration) => {
        Duration::ZERO
    };
    (Ratio) => {
        0.0
    };
}

/// `delta_since` rule for one metric kind.
///
/// `Count` and `Duration` are monotonic accumulators, so they subtract the
/// baseline (saturating, because a baseline above the current value means the
/// caller mixed up two producers rather than that time ran backwards). `Gauge`
/// and `Ratio` are point-in-time readings with no meaningful difference, so the
/// baseline is ignored and the current value is reported as-is.
macro_rules! producer_metric_delta {
    (Count, $current:expr, $baseline:expr) => {
        $current.saturating_sub($baseline)
    };
    (Duration, $current:expr, $baseline:expr) => {
        $current.saturating_sub($baseline)
    };
    (Gauge, $current:expr, $_baseline:expr) => {
        $current
    };
    (Ratio, $current:expr, $_baseline:expr) => {
        $current
    };
}

/// Declare the producer's stable metric surface exactly once.
///
/// Each row is `#[doc] name: Kind = "OTLP description";`, and the kind decides
/// four things at once: the snapshot field type, its [`ZERO`] value, the
/// [`ProducerMetricValue`] variant it reads back as, and the [`delta_since`]
/// rule. From that one table this generates [`ProducerMetricsSnapshot`] itself
/// plus every list that used to be kept in sync by hand: `ZERO`, `delta_since`,
/// `metric`, `as_metric_map`, `is_internal_metric_name`, and
/// `producer_metric_description`.
///
/// Adding a metric is therefore a one-line edit, and a metric that is *not* in
/// the table does not exist: there is no way to grow the snapshot struct without
/// also growing every reader of it, which is exactly what the six hand-kept
/// lists this replaces could not guarantee.
///
/// [`ZERO`]: ProducerMetricsSnapshot::ZERO
/// [`delta_since`]: ProducerMetricsSnapshot::delta_since
macro_rules! producer_metric_table {
    ($($(#[$field_meta:meta])* $field:ident: $kind:ident = $description:literal;)+) => {
        /// Point-in-time producer metrics for operational diagnostics.
        #[derive(Debug, Clone, Copy, PartialEq)]
        #[non_exhaustive]
        pub struct ProducerMetricsSnapshot {
            $($(#[$field_meta])* pub $field: producer_metric_ty!($kind),)+
        }

        impl ProducerMetricsSnapshot {
            /// An all-zero snapshot.
            ///
            /// This type is `#[non_exhaustive]`, so downstream crates cannot build one with a
            /// struct expression. `ZERO` is usable in const context as their baseline value.
            pub const ZERO: Self = Self {
                $($field: producer_metric_zero!($kind),)+
            };

            /// Return the difference between this snapshot and an earlier `baseline`.
            ///
            /// The two field kinds are treated differently, which is the part that is easy to
            /// get wrong:
            ///
            /// - **Monotonic counters** (every `Count` and `Duration` metric: the `*_count`,
            ///   `*_bytes`, `records_appended`, and `*_total_latency` fields) are
            ///   `saturating_sub(baseline)`, so a baseline above the current value clamps to
            ///   zero instead of wrapping.
            /// - **Gauges and instantaneous values** (every `Gauge` and `Ratio` metric) are
            ///   point-in-time readings, not accumulators, so the baseline is ignored and the
            ///   current value is reported as-is.
            ///
            /// The rule is a property of each metric's kind in the table, so a new metric
            /// cannot be added without choosing one.
            #[must_use]
            pub const fn delta_since(&self, baseline: &Self) -> Self {
                Self {
                    $($field: producer_metric_delta!($kind, self.$field, baseline.$field),)+
                }
            }

            /// Return one named metric value from this snapshot.
            #[must_use]
            pub fn metric(&self, name: &str) -> Option<ProducerMetricValue> {
                $(if name == stringify!($field) { return Some(ProducerMetricValue::$kind(self.$field)); })+
                None
            }

            /// Return a read-only-by-value registry of stable producer metrics.
            #[must_use]
            pub fn as_metric_map(&self) -> BTreeMap<&'static str, ProducerMetricValue> {
                BTreeMap::from([
                    $((stringify!($field), ProducerMetricValue::$kind(self.$field)),)+
                ])
            }

            /// Return whether `name` is one of the metrics this snapshot owns.
            ///
            /// Derived from [`Self::metric`] rather than from a second name list, so the two
            /// answers cannot disagree.
            pub(crate) fn is_internal_metric_name(name: &str) -> bool {
                Self::ZERO.metric(name).is_some()
            }
        }

        /// OTLP description for one stable producer metric name.
        fn producer_metric_description(name: &str) -> &'static str {
            $(if name == stringify!($field) { return $description; })+
            ""
        }
    };
}

producer_metric_table! {
    /// Records accepted into the producer accumulator.
    records_appended: Count = "records accepted into the producer accumulator";
    /// Produce requests sent to brokers.
    produce_request_count: Count = "produce requests sent to brokers";
    /// Encoded produce request bytes sent to brokers.
    produce_request_bytes: Count = "encoded produce request bytes sent to brokers";
    /// Serialized record batches sent in produce requests.
    produce_batch_count: Count = "record batches sent in produce requests";
    /// Encoded record batch bytes sent in produce requests.
    produce_batch_bytes: Count = "encoded record batch bytes sent in produce requests";
    /// Encoded record batch payload bytes grouped into produce requests.
    produce_request_payload_bytes: Count =
        "encoded record batch payload bytes grouped into produce requests";
    /// Produce request grouping splits forced by the max request size limit.
    produce_request_split_count: Count =
        "produce request grouping splits forced by max request size";
    /// Record batch splits forced by a broker `MESSAGE_TOO_LARGE` response.
    record_batch_split_count: Count =
        "record batch splits forced by a broker MESSAGE_TOO_LARGE response";
    /// Records included in produce requests sent to brokers.
    produce_record_count: Count = "records included in produce requests";
    /// Retry attempts after retryable produce failures.
    produce_retry_count: Count = "retry attempts after retryable produce failures";
    /// Produce responses or dispatches that reported an error.
    produce_error_count: Count = "produce responses or dispatches that reported an error";
    /// Batches requeued because metadata/routing was not yet complete.
    requeue_count: Count = "batches requeued because routing was incomplete";
    /// Backpressure stalls while enqueueing produce requests to broker sessions.
    in_flight_stall_count: Count = "backpressure stalls while enqueueing produce requests";
    /// Bytes currently buffered in the accumulator.
    queue_depth_bytes: Gauge = "bytes currently buffered in the accumulator";
    /// Records currently buffered in the accumulator.
    queue_depth_records: Gauge = "records currently buffered in the accumulator";
    /// Producer buffer memory currently available for new batch reservations.
    buffer_available_bytes: Gauge = "producer buffer memory available for new batch reservations";
    /// API tasks currently blocked waiting for producer buffer memory.
    waiting_threads: Gauge = "API tasks blocked waiting for producer buffer memory";
    /// Batches currently buffered or in flight.
    incomplete_batches: Gauge = "batches currently buffered or in flight";
    /// Producer dispatch tasks currently in flight.
    in_flight_dispatches: Gauge = "producer dispatch tasks currently in flight";
    /// Average drained batch fill ratio, capped at `1.0`.
    average_batch_fill_ratio: Ratio = "average drained batch fill ratio";
    /// Average encoded/uncompressed batch compression ratio.
    average_compression_ratio: Ratio = "average encoded/uncompressed batch compression ratio";
    /// Number of explicit flush calls.
    flush_count: Count = "explicit flush calls";
    /// Total wall-clock latency spent in flush calls.
    flush_total_latency: Duration = "total wall-clock latency spent in flush calls";
    /// Number of successful API-thread metadata wait operations.
    metadata_wait_count: Count = "metadata wait operations";
    /// Total wall-clock latency spent waiting for metadata in API calls.
    metadata_wait_total_latency: Duration = "total latency spent waiting for metadata";
    /// Number of `init_transactions` calls.
    transaction_init_count: Count = "init_transactions calls";
    /// Total wall-clock latency spent in `init_transactions`.
    transaction_init_total_latency: Duration = "total latency spent in init_transactions";
    /// Number of `begin_transaction` calls.
    transaction_begin_count: Count = "begin_transaction calls";
    /// Total wall-clock latency spent in `begin_transaction`.
    transaction_begin_total_latency: Duration = "total latency spent in begin_transaction";
    /// Number of `send_offsets_to_transaction` calls with non-empty offsets.
    send_offsets_to_transaction_count: Count = "send_offsets_to_transaction calls";
    /// Total wall-clock latency spent in `send_offsets_to_transaction`.
    send_offsets_to_transaction_total_latency: Duration =
        "total latency spent in send_offsets_to_transaction";
    /// Number of `commit_transaction` calls.
    transaction_commit_count: Count = "commit_transaction calls";
    /// Total wall-clock latency spent in `commit_transaction`.
    transaction_commit_total_latency: Duration = "total latency spent in commit_transaction";
    /// Number of `abort_transaction` calls.
    transaction_abort_count: Count = "abort_transaction calls";
    /// Total wall-clock latency spent in `abort_transaction`.
    transaction_abort_total_latency: Duration = "total latency spent in abort_transaction";
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ProducerQueueMetrics {
    pub(crate) queue_depth_bytes: usize,
    pub(crate) queue_depth_records: usize,
    pub(crate) buffer_available_bytes: usize,
    pub(crate) incomplete_batches: usize,
    pub(crate) in_flight_dispatches: usize,
}

impl ProducerMetricsSnapshot {
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
    /// `KafkaMetric` facade stores a value provider but not a Kafka metric type.
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
    produce_request_bytes: AtomicU64,
    produce_batch_count: AtomicU64,
    produce_batch_bytes: AtomicU64,
    produce_request_payload_bytes: AtomicU64,
    produce_request_split_count: AtomicU64,
    record_batch_split_count: AtomicU64,
    produce_record_count: AtomicU64,
    produce_retry_count: AtomicU64,
    produce_error_count: AtomicU64,
    requeue_count: AtomicU64,
    in_flight_stall_count: AtomicU64,
    waiting_threads: AtomicUsize,
    batch_fill_per_mille_sum: AtomicU64,
    batch_fill_samples: AtomicU64,
    compression_ratio_per_mille_sum: AtomicU64,
    compression_ratio_samples: AtomicU64,
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
    /// Kafka-named client + per-topic metrics (Kafka `SenderMetricsRegistry`).
    sender_registry: SenderMetricsRegistry,
}

impl ProducerMetrics {
    pub(crate) fn record_produce_request(
        &self,
        request_bytes: usize,
        payload_bytes: usize,
        records: usize,
    ) {
        self.inner
            .sender_registry
            .record_records_per_request(u64::try_from(records).unwrap_or(u64::MAX));
        let _previous = self
            .inner
            .produce_request_count
            .fetch_add(1, Ordering::Relaxed);
        let request_bytes = u64::try_from(request_bytes).unwrap_or(u64::MAX);
        let _previous = self
            .inner
            .produce_request_bytes
            .fetch_add(request_bytes, Ordering::Relaxed);
        let payload_bytes = u64::try_from(payload_bytes).unwrap_or(u64::MAX);
        let _previous = self
            .inner
            .produce_request_payload_bytes
            .fetch_add(payload_bytes, Ordering::Relaxed);
    }

    #[cfg(test)]
    pub(crate) fn record_produce_batch(
        &self,
        batch_bytes: usize,
        batch_size: usize,
        records: usize,
    ) {
        self.record_produce_batch_with_compression_ratio(
            "test",
            batch_bytes,
            batch_size,
            records,
            1.0,
        );
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "Mirrors Kafka's handleProduceResponse batch metrics (topic, bytes, size, count, \
                  ratio)."
    )]
    pub(crate) fn record_produce_batch_with_compression_ratio(
        &self,
        topic: &str,
        batch_bytes: usize,
        batch_size: usize,
        records: usize,
        compression_ratio: f64,
    ) {
        let records = u64::try_from(records).unwrap_or(u64::MAX);
        self.inner.sender_registry.record_batch(
            topic,
            records,
            u64::try_from(batch_bytes).unwrap_or(u64::MAX),
            compression_ratio,
        );
        let _previous = self
            .inner
            .produce_record_count
            .fetch_add(records, Ordering::Relaxed);
        let _previous = self
            .inner
            .produce_batch_count
            .fetch_add(1, Ordering::Relaxed);
        let batch_bytes = u64::try_from(batch_bytes).unwrap_or(u64::MAX);
        let _previous = self
            .inner
            .produce_batch_bytes
            .fetch_add(batch_bytes, Ordering::Relaxed);

        let batch_size = u64::try_from(batch_size.max(1)).unwrap_or(u64::MAX);
        let scaled = batch_bytes
            .saturating_mul(1_000)
            .checked_div(batch_size)
            .unwrap_or(0)
            .min(1_000);
        let _previous = self
            .inner
            .batch_fill_per_mille_sum
            .fetch_add(scaled, Ordering::Relaxed);
        let _previous = self
            .inner
            .batch_fill_samples
            .fetch_add(1, Ordering::Relaxed);
        let compression_ratio_per_mille = ratio_to_per_mille(compression_ratio);
        let _previous = self
            .inner
            .compression_ratio_per_mille_sum
            .fetch_add(compression_ratio_per_mille, Ordering::Relaxed);
        let _previous = self
            .inner
            .compression_ratio_samples
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_retry(&self) {
        self.record_retry_for_topic(None);
    }

    /// Record a record-send retry, attributing it to `topic` for per-topic metrics.
    pub(crate) fn record_retry_for_topic(&self, topic: Option<&str>) {
        let _previous = self
            .inner
            .produce_retry_count
            .fetch_add(1, Ordering::Relaxed);
        self.inner.sender_registry.record_retry(topic);
    }

    pub(crate) fn record_error(&self) {
        self.record_error_for_topic(None);
    }

    /// Record a record-send error, attributing it to `topic` for per-topic metrics.
    pub(crate) fn record_error_for_topic(&self, topic: Option<&str>) {
        let _previous = self
            .inner
            .produce_error_count
            .fetch_add(1, Ordering::Relaxed);
        self.inner.sender_registry.record_error(topic);
    }

    /// Snapshot the Kafka-named (Kafka `SenderMetricsRegistry`) producer metrics.
    pub(crate) fn kafka_metrics(&self) -> BTreeMap<String, f64> {
        self.inner.sender_registry.kafka_metrics()
    }

    /// Record a produce request round-trip latency (Kafka request-latency).
    pub(crate) fn record_request_latency(&self, latency: Duration) {
        self.inner
            .sender_registry
            .record_request_latency(duration_to_ms_f64(latency));
    }

    /// Record a broker-imposed throttle window (Kafka produce-throttle-time).
    pub(crate) fn record_throttle_time(&self, throttle: Duration) {
        self.inner
            .sender_registry
            .record_throttle_time(duration_to_ms_f64(throttle));
    }

    /// Record the time a batch spent buffered before drain (Kafka record-queue-time).
    pub(crate) fn record_queue_time(&self, queued: Duration) {
        self.inner
            .sender_registry
            .record_queue_time(duration_to_ms_f64(queued));
    }

    /// Record all serialized record sizes for one produce batch in a single
    /// locked pass (avoids per-record lock + clock overhead on the send path).
    pub(crate) fn record_record_sizes(&self, sizes: &[usize]) {
        self.inner.sender_registry.record_record_sizes(sizes);
    }

    /// Update the in-flight request gauge (Kafka requests-in-flight).
    pub(crate) fn set_requests_in_flight(&self, in_flight: usize) {
        self.inner.sender_registry.set_requests_in_flight(in_flight);
    }

    /// Update the metadata-age gauge in seconds (Kafka metadata-age).
    pub(crate) fn set_metadata_age(&self, age: Duration) {
        self.inner
            .sender_registry
            .set_metadata_age(age.as_secs_f64());
    }

    /// Update the available-buffer-memory + waiting-threads gauges (Kafka
    /// buffer-available-bytes / waiting-threads).
    pub(crate) fn set_buffer_gauges(&self, available_bytes: usize) {
        self.inner
            .sender_registry
            .set_buffer_available_bytes(available_bytes);
        self.inner
            .sender_registry
            .set_waiting_threads(self.inner.waiting_threads.load(Ordering::Relaxed));
    }

    pub(crate) fn record_requeue(&self) {
        let _previous = self.inner.requeue_count.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_in_flight_stall(&self) {
        let _previous = self
            .inner
            .in_flight_stall_count
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn start_buffer_wait(&self) -> ProducerBufferWaitGuard {
        let _previous = self.inner.waiting_threads.fetch_add(1, Ordering::Relaxed);
        ProducerBufferWaitGuard {
            metrics: self.clone(),
            started_at: std::time::Instant::now(),
        }
    }

    /// Record a produce request grouping split forced by `max.request.size`.
    ///
    /// This is a kacrab-only local packing decision; Java has no metric for it, so it
    /// deliberately does not feed the Java-named `batch-split` meter.
    pub(crate) fn record_request_split(&self) {
        let _previous = self
            .inner
            .produce_request_split_count
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record a record batch split forced by a broker `MESSAGE_TOO_LARGE` response —
    /// the sole feed of the Java-named `producer-metrics:batch-split-*` meter.
    pub(crate) fn record_batch_split(&self) {
        let _previous = self
            .inner
            .record_batch_split_count
            .fetch_add(1, Ordering::Relaxed);
        self.inner.sender_registry.record_split();
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

    #[expect(
        clippy::too_many_lines,
        reason = "Snapshot construction intentionally lists every public producer metric field."
    )]
    pub(crate) fn snapshot(&self, queue: ProducerQueueMetrics) -> ProducerMetricsSnapshot {
        let batch_fill_samples = self.inner.batch_fill_samples.load(Ordering::Relaxed);
        let batch_fill_sum = self.inner.batch_fill_per_mille_sum.load(Ordering::Relaxed);
        let average_batch_fill_ratio = if batch_fill_samples == 0 {
            0.0
        } else {
            let average_per_mille = batch_fill_sum.checked_div(batch_fill_samples).unwrap_or(0);
            let average_per_mille = u32::try_from(average_per_mille).unwrap_or(1_000);
            f64::from(average_per_mille) / 1_000.0
        };
        let compression_ratio_samples =
            self.inner.compression_ratio_samples.load(Ordering::Relaxed);
        let compression_ratio_sum = self
            .inner
            .compression_ratio_per_mille_sum
            .load(Ordering::Relaxed);
        let average_compression_ratio = if compression_ratio_samples == 0 {
            0.0
        } else {
            let average_per_mille = compression_ratio_sum
                .checked_div(compression_ratio_samples)
                .unwrap_or(0);
            let average_per_mille = u32::try_from(average_per_mille).unwrap_or(u32::MAX);
            f64::from(average_per_mille) / 1_000.0
        };
        let produce_record_count = self.inner.produce_record_count.load(Ordering::Relaxed);
        let queued_records = u64::try_from(queue.queue_depth_records).unwrap_or(u64::MAX);
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
            produce_request_bytes: self.inner.produce_request_bytes.load(Ordering::Relaxed),
            produce_batch_count: self.inner.produce_batch_count.load(Ordering::Relaxed),
            produce_batch_bytes: self.inner.produce_batch_bytes.load(Ordering::Relaxed),
            produce_request_payload_bytes: self
                .inner
                .produce_request_payload_bytes
                .load(Ordering::Relaxed),
            produce_request_split_count: self
                .inner
                .produce_request_split_count
                .load(Ordering::Relaxed),
            record_batch_split_count: self.inner.record_batch_split_count.load(Ordering::Relaxed),
            produce_record_count,
            produce_retry_count: self.inner.produce_retry_count.load(Ordering::Relaxed),
            produce_error_count: self.inner.produce_error_count.load(Ordering::Relaxed),
            requeue_count: self.inner.requeue_count.load(Ordering::Relaxed),
            in_flight_stall_count: self.inner.in_flight_stall_count.load(Ordering::Relaxed),
            queue_depth_bytes: queue.queue_depth_bytes,
            queue_depth_records: queue.queue_depth_records,
            buffer_available_bytes: queue.buffer_available_bytes,
            waiting_threads: self.inner.waiting_threads.load(Ordering::Relaxed),
            incomplete_batches: queue.incomplete_batches,
            in_flight_dispatches: queue.in_flight_dispatches,
            average_batch_fill_ratio,
            average_compression_ratio,
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

pub(crate) struct ProducerBufferWaitGuard {
    metrics: ProducerMetrics,
    started_at: std::time::Instant,
}

impl Drop for ProducerBufferWaitGuard {
    fn drop(&mut self) {
        let _previous = self
            .metrics
            .inner
            .waiting_threads
            .fetch_sub(1, Ordering::Relaxed);
        // Kafka BufferPool: a blocked append is a buffer-exhausted event; record it
        // plus the time spent waiting for space allocation (bufferpool-wait-time).
        let wait_ms = self.started_at.elapsed().as_secs_f64() * 1000.0;
        self.metrics
            .inner
            .sender_registry
            .record_buffer_exhausted(wait_ms);
    }
}

fn duration_nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "Metrics store compression ratios as per-mille counters for lock-free averaging."
)]
fn ratio_to_per_mille(ratio: f64) -> u64 {
    if !ratio.is_finite() || ratio.is_sign_negative() {
        return 1_000;
    }
    (ratio * 1_000.0).round() as u64
}

#[expect(
    clippy::cast_precision_loss,
    reason = "Metrics latency values are coarse observability samples; ms precision loss is fine."
)]
fn duration_to_ms_f64(duration: Duration) -> f64 {
    duration.as_nanos() as f64 / 1_000_000.0
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::expect_used,
        clippy::missing_assert_message,
        clippy::unwrap_used,
        reason = "Unit test fixtures fail fastest with contextual unwrap/expect calls."
    )]

    use std::time::Duration;

    use super::{
        ProducerMetricValue, ProducerMetrics, ProducerMetricsSnapshot, ProducerQueueMetrics,
        producer_metric_description,
    };

    /// Every field of [`ProducerMetricsSnapshot`], read back out of its derived
    /// `Debug` output.
    ///
    /// The snapshot is `#[non_exhaustive]`, so a test cannot destructure it to
    /// force a compile error on a new field, and Rust has no field reflection.
    /// The pretty `Debug` shape (`Name {\n    field: value,\n    ...\n}`) is the
    /// one place the *actual* field list is observable at runtime, which is what
    /// makes this an exhaustiveness check rather than another hand-kept list.
    fn snapshot_field_names() -> Vec<String> {
        format!("{:#?}", ProducerMetricsSnapshot::ZERO)
            .lines()
            .filter_map(|line| line.trim().split_once(": "))
            .map(|(field, _value)| field.to_owned())
            .collect()
    }

    #[test]
    fn every_snapshot_field_is_a_named_metric_with_a_description() {
        let fields = snapshot_field_names();
        assert!(
            fields.len() >= 35,
            "Debug parsing found only {} fields, so this test is not checking anything",
            fields.len()
        );

        for field in &fields {
            assert!(
                ProducerMetricsSnapshot::ZERO.metric(field).is_some(),
                "snapshot field '{field}' is not readable through metric()"
            );
            assert!(
                ProducerMetricsSnapshot::is_internal_metric_name(field),
                "snapshot field '{field}' is not recognised as an internal metric name"
            );
            assert!(
                !producer_metric_description(field).is_empty(),
                "snapshot field '{field}' has no OTLP description"
            );
        }

        let metric_map = ProducerMetricsSnapshot::ZERO.as_metric_map();
        let mapped = metric_map
            .keys()
            .map(|name| (*name).to_owned())
            .collect::<Vec<_>>();
        let mut expected = fields;
        expected.sort();
        assert_eq!(
            mapped, expected,
            "as_metric_map must expose exactly the snapshot fields"
        );
    }

    #[test]
    fn producer_metrics_expose_average_compression_ratio_like_java() {
        let metrics = ProducerMetrics::default();

        metrics.record_produce_batch(64, 128, 1);
        let snapshot = metrics.snapshot(ProducerQueueMetrics::default());

        assert_eq!(
            snapshot.metric("average_compression_ratio"),
            Some(ProducerMetricValue::Ratio(1.0))
        );
        assert!(
            snapshot
                .as_metric_map()
                .contains_key("average_compression_ratio")
        );
    }

    #[test]
    fn producer_metrics_average_observed_compression_ratios_like_java() {
        let metrics = ProducerMetrics::default();

        metrics.record_produce_batch_with_compression_ratio("orders", 64, 128, 1, 0.50);
        metrics.record_produce_batch_with_compression_ratio("orders", 96, 128, 1, 0.75);
        let snapshot = metrics.snapshot(ProducerQueueMetrics::default());

        assert_eq!(
            snapshot.metric("average_compression_ratio"),
            Some(ProducerMetricValue::Ratio(0.625))
        );
    }

    #[test]
    fn producer_snapshot_serializes_otlp_metrics_data_for_telemetry_push() {
        let snapshot = ProducerMetricsSnapshot {
            records_appended: 3,
            produce_request_count: 2,
            produce_request_bytes: 300,
            produce_batch_count: 2,
            produce_batch_bytes: 256,
            produce_request_payload_bytes: 256,
            produce_request_split_count: 1,
            record_batch_split_count: 1,
            produce_record_count: 3,
            produce_retry_count: 1,
            queue_depth_bytes: 128,
            queue_depth_records: 4,
            buffer_available_bytes: 512,
            waiting_threads: 3,
            incomplete_batches: 2,
            in_flight_dispatches: 1,
            average_batch_fill_ratio: 0.5,
            average_compression_ratio: 1.0,
            flush_count: 1,
            flush_total_latency: Duration::from_millis(2),
            ..ProducerMetricsSnapshot::ZERO
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
                .windows(b"average_compression_ratio".len())
                .any(|window| window == b"average_compression_ratio")
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

    #[test]
    fn snapshot_delta_subtracts_counters_and_clamps_a_backwards_baseline() {
        let baseline = ProducerMetricsSnapshot {
            records_appended: 10,
            produce_request_count: 40,
            flush_total_latency: Duration::from_millis(30),
            transaction_commit_total_latency: Duration::from_millis(9),
            ..ProducerMetricsSnapshot::ZERO
        };
        let current = ProducerMetricsSnapshot {
            records_appended: 25,
            produce_request_count: 7,
            flush_total_latency: Duration::from_millis(50),
            transaction_commit_total_latency: Duration::from_millis(4),
            ..ProducerMetricsSnapshot::ZERO
        };

        let delta = current.delta_since(&baseline);

        assert_eq!(delta.records_appended, 15);
        assert_eq!(delta.produce_request_count, 0);
        assert_eq!(delta.flush_total_latency, Duration::from_millis(20));
        assert_eq!(delta.transaction_commit_total_latency, Duration::ZERO);
    }

    #[test]
    fn snapshot_delta_reports_gauges_at_their_current_value() {
        let baseline = ProducerMetricsSnapshot {
            queue_depth_bytes: 4096,
            queue_depth_records: 64,
            buffer_available_bytes: 8192,
            waiting_threads: 5,
            incomplete_batches: 9,
            in_flight_dispatches: 3,
            average_batch_fill_ratio: 0.9,
            average_compression_ratio: 2.5,
            ..ProducerMetricsSnapshot::ZERO
        };
        let current = ProducerMetricsSnapshot {
            queue_depth_bytes: 128,
            queue_depth_records: 2,
            buffer_available_bytes: 512,
            waiting_threads: 1,
            incomplete_batches: 4,
            in_flight_dispatches: 2,
            average_batch_fill_ratio: 0.25,
            average_compression_ratio: 1.5,
            ..ProducerMetricsSnapshot::ZERO
        };

        let delta = current.delta_since(&baseline);

        assert_eq!(delta.queue_depth_bytes, current.queue_depth_bytes);
        assert_eq!(delta.queue_depth_records, current.queue_depth_records);
        assert_eq!(delta.buffer_available_bytes, current.buffer_available_bytes);
        assert_eq!(delta.waiting_threads, current.waiting_threads);
        assert_eq!(delta.incomplete_batches, current.incomplete_batches);
        assert_eq!(delta.in_flight_dispatches, current.in_flight_dispatches);
        assert!(
            (delta.average_batch_fill_ratio - current.average_batch_fill_ratio).abs()
                < f64::EPSILON
        );
        assert!(
            (delta.average_compression_ratio - current.average_compression_ratio).abs()
                < f64::EPSILON
        );
    }
}
