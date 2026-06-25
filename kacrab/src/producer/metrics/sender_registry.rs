#![expect(
    clippy::cast_precision_loss,
    reason = "Metric counts/bytes are coarse observability samples; f64 mantissa loss is acceptable."
)]
//! Java-compatible producer metrics, mirroring Kafka's `SenderMetricsRegistry`.
//!
//! The producer's primary metrics facade is the lock-free [`super::super::metrics::ProducerMetrics`]
//! atomic counter set, exposed under Rust-native names. This registry additionally
//! publishes the same measurements under Kafka's metric names and semantics
//! (windowed Rate, Avg, Max) on the `producer-metrics` group plus per-topic
//! instances on the `producer-topic-metrics` group, so applications that query by
//! Kafka metric name see the expected sensors.

use std::{
    collections::{BTreeMap, HashMap},
    sync::Mutex,
    time::{SystemTime, UNIX_EPOCH},
};

use super::registry::{MetricName, Metrics, SensorId};

const CLIENT_GROUP: &str = "producer-metrics";
const TOPIC_GROUP: &str = "producer-topic-metrics";

fn now_ms() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_millis()),
    )
    .unwrap_or(u64::MAX)
}

/// Java-named producer client-level and per-topic metrics.
#[derive(Debug)]
pub(crate) struct SenderMetricsRegistry {
    inner: Mutex<RegistryInner>,
}

#[derive(Debug)]
struct RegistryInner {
    metrics: Metrics,
    client: ClientSensors,
    topics: HashMap<String, TopicSensors>,
}

/// Client-level (`producer-metrics`) sensor handles.
#[derive(Debug)]
struct ClientSensors {
    records_sent: SensorId,
    record_errors: SensorId,
    record_retries: SensorId,
    batch_split: SensorId,
    bytes: SensorId,
    compression_rate: SensorId,
    batch_size: SensorId,
    records_per_request: SensorId,
    request_latency: SensorId,
    record_size: SensorId,
    produce_throttle_time: SensorId,
    record_queue_time: SensorId,
    requests_in_flight: SensorId,
}

/// Per-topic (`producer-topic-metrics`) sensor handles.
#[derive(Debug, Clone, Copy)]
struct TopicSensors {
    records_sent: SensorId,
    bytes: SensorId,
    compression_rate: SensorId,
    record_retries: SensorId,
    record_errors: SensorId,
}

impl Default for SenderMetricsRegistry {
    fn default() -> Self {
        let mut metrics = Metrics::new();
        let client = ClientSensors::register(&mut metrics);
        Self {
            inner: Mutex::new(RegistryInner {
                metrics,
                client,
                topics: HashMap::new(),
            }),
        }
    }
}

impl ClientSensors {
    #[expect(
        clippy::too_many_lines,
        reason = "Registers the full Kafka client-level sensor set in one place."
    )]
    fn register(metrics: &mut Metrics) -> Self {
        let records_sent = meter(
            metrics,
            "records-sent",
            "record-send-rate",
            "The average number of records sent per second.",
            "record-send-total",
            "The total number of records sent.",
        );
        let record_errors = meter(
            metrics,
            "record-errors",
            "record-error-rate",
            "The average per-second number of record sends that resulted in errors.",
            "record-error-total",
            "The total number of record sends that resulted in errors.",
        );
        let record_retries = meter(
            metrics,
            "record-retries",
            "record-retry-rate",
            "The average per-second number of retried record sends.",
            "record-retry-total",
            "The total number of retried record sends.",
        );
        let batch_split = meter(
            metrics,
            "batch-split",
            "batch-split-rate",
            "The average number of batch splits per second.",
            "batch-split-total",
            "The total number of batch splits.",
        );
        let bytes = meter(
            metrics,
            "bytes",
            "byte-rate",
            "The average number of bytes sent per second.",
            "byte-total",
            "The total number of bytes sent.",
        );
        let compression_rate = avg_only(
            metrics,
            "compression-rate",
            "compression-rate-avg",
            "The average compression rate of record batches.",
        );
        let batch_size = avg_max(
            metrics,
            "batch-size",
            "batch-size-avg",
            "The average number of bytes sent per partition per-request.",
            "batch-size-max",
            "The max number of bytes sent per partition per-request.",
        );
        let records_per_request = avg_only(
            metrics,
            "records-per-request",
            "records-per-request-avg",
            "The average number of records per request.",
        );
        let request_latency = avg_max(
            metrics,
            "request-latency",
            "request-latency-avg",
            "The average request latency in ms.",
            "request-latency-max",
            "The maximum request latency in ms.",
        );
        let record_size = avg_max(
            metrics,
            "record-size",
            "record-size-avg",
            "The average record size.",
            "record-size-max",
            "The maximum record size.",
        );
        let produce_throttle_time = avg_max(
            metrics,
            "produce-throttle-time",
            "produce-throttle-time-avg",
            "The average time in ms a request was throttled by a broker.",
            "produce-throttle-time-max",
            "The maximum time in ms a request was throttled by a broker.",
        );
        let record_queue_time = avg_max(
            metrics,
            "record-queue-time",
            "record-queue-time-avg",
            "The average time in ms record batches spent in the send buffer.",
            "record-queue-time-max",
            "The maximum time in ms record batches spent in the send buffer.",
        );
        let requests_in_flight = value_only(
            metrics,
            "requests-in-flight",
            "requests-in-flight",
            "The current number of in-flight requests awaiting a response.",
        );
        Self {
            records_sent,
            record_errors,
            record_retries,
            batch_split,
            bytes,
            compression_rate,
            batch_size,
            records_per_request,
            request_latency,
            record_size,
            produce_throttle_time,
            record_queue_time,
            requests_in_flight,
        }
    }
}

impl RegistryInner {
    fn topic_sensors(&mut self, topic: &str) -> TopicSensors {
        if let Some(sensors) = self.topics.get(topic) {
            return *sensors;
        }
        let sensors = TopicSensors::register(&mut self.metrics, topic);
        let _previous = self.topics.insert(topic.to_owned(), sensors);
        sensors
    }
}

impl TopicSensors {
    fn register(metrics: &mut Metrics, topic: &str) -> Self {
        let records_sent = topic_meter(
            metrics,
            topic,
            "records-sent",
            "record-send-rate",
            "The average number of records sent per second for a topic.",
            "record-send-total",
            "The total number of records sent for a topic.",
        );
        let bytes = topic_meter(
            metrics,
            topic,
            "bytes",
            "byte-rate",
            "The average number of bytes sent per second for a topic.",
            "byte-total",
            "The total number of bytes sent for a topic.",
        );
        let compression_rate = topic_avg(
            metrics,
            topic,
            "compression-rate",
            "compression-rate",
            "The average compression rate of record batches for a topic.",
        );
        let record_retries = topic_meter(
            metrics,
            topic,
            "record-retries",
            "record-retry-rate",
            "The average per-second number of retried record sends for a topic.",
            "record-retry-total",
            "The total number of retried record sends for a topic.",
        );
        let record_errors = topic_meter(
            metrics,
            topic,
            "record-errors",
            "record-error-rate",
            "The average per-second number of record sends that resulted in errors for a topic.",
            "record-error-total",
            "The total number of record sends that resulted in errors for a topic.",
        );
        Self {
            records_sent,
            bytes,
            compression_rate,
            record_retries,
            record_errors,
        }
    }
}

impl SenderMetricsRegistry {
    fn record(&self, select: impl Fn(&ClientSensors) -> SensorId, value: f64) {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let sensor = select(&inner.client);
        let _ignored = inner.metrics.record_at_ms(sensor, value, now_ms());
    }

    /// Record a sent batch (records, bytes, compression) for the client and topic.
    pub(crate) fn record_batch(&self, topic: &str, records: u64, bytes: u64, compression_ratio: f64) {
        let now = now_ms();
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let records = records as f64;
        let bytes = bytes as f64;
        let client = [
            (inner.client.records_sent, records),
            (inner.client.bytes, bytes),
            (inner.client.batch_size, bytes),
            (inner.client.compression_rate, compression_ratio),
        ];
        for (sensor, value) in client {
            let _ignored = inner.metrics.record_at_ms(sensor, value, now);
        }
        let topic_sensors = inner.topic_sensors(topic);
        let topic_records = [
            (topic_sensors.records_sent, records),
            (topic_sensors.bytes, bytes),
            (topic_sensors.compression_rate, compression_ratio),
        ];
        for (sensor, value) in topic_records {
            let _ignored = inner.metrics.record_at_ms(sensor, value, now);
        }
    }

    /// Record the number of records carried by one produce request.
    pub(crate) fn record_records_per_request(&self, records: u64) {
        self.record(|client| client.records_per_request, records as f64);
    }

    /// Record a produce request round-trip latency in milliseconds.
    pub(crate) fn record_request_latency(&self, latency_ms: f64) {
        self.record(|client| client.request_latency, latency_ms);
    }

    /// Record one serialized record size in bytes.
    pub(crate) fn record_record_size(&self, size: f64) {
        self.record(|client| client.record_size, size);
    }

    /// Record a broker-imposed throttle window in milliseconds.
    pub(crate) fn record_throttle_time(&self, throttle_ms: f64) {
        self.record(|client| client.produce_throttle_time, throttle_ms);
    }

    /// Record the time a batch spent buffered before being drained.
    pub(crate) fn record_queue_time(&self, queue_ms: f64) {
        self.record(|client| client.record_queue_time, queue_ms);
    }

    /// Update the current in-flight request gauge.
    pub(crate) fn set_requests_in_flight(&self, in_flight: usize) {
        self.record(|client| client.requests_in_flight, in_flight as f64);
    }

    /// Record a record-send error for the client and (optionally) a topic.
    pub(crate) fn record_error(&self, topic: Option<&str>) {
        let now = now_ms();
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let client = inner.client.record_errors;
        let _ignored = inner.metrics.record_at_ms(client, 1.0, now);
        if let Some(topic) = topic {
            let sensor = inner.topic_sensors(topic).record_errors;
            let _ignored = inner.metrics.record_at_ms(sensor, 1.0, now);
        }
    }

    /// Record a record-send retry for the client and (optionally) a topic.
    pub(crate) fn record_retry(&self, topic: Option<&str>) {
        let now = now_ms();
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let client = inner.client.record_retries;
        let _ignored = inner.metrics.record_at_ms(client, 1.0, now);
        if let Some(topic) = topic {
            let sensor = inner.topic_sensors(topic).record_retries;
            let _ignored = inner.metrics.record_at_ms(sensor, 1.0, now);
        }
    }

    /// Record a batch split.
    pub(crate) fn record_split(&self) {
        self.record(|client| client.batch_split, 1.0);
    }

    /// Snapshot all registered Kafka-named metrics as `"group:name[:tag=value]" -> value`.
    pub(crate) fn kafka_metrics(&self) -> BTreeMap<String, f64> {
        let inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        inner
            .metrics
            .registered_metrics()
            .map(|(name, metric)| (metric_key(name), metric.metric_value()))
            .collect()
    }
}

fn metric_key(name: &MetricName) -> String {
    use std::fmt::Write as _;
    let mut key = format!("{}:{}", name.group(), name.name());
    for (tag_key, tag_value) in name.tags() {
        let _ignored = write!(key, ":{tag_key}={tag_value}");
    }
    key
}

// `MetricName::tags()` returns a `&BTreeMap<String, String>`, already in sorted
// key order, so `metric_key` produces a stable string for each metric.

#[expect(
    clippy::too_many_arguments,
    reason = "Sensor builder threads sensor + rate/total metric names and descriptions."
)]
fn meter(
    metrics: &mut Metrics,
    sensor_name: &str,
    rate_name: &str,
    rate_desc: &str,
    total_name: &str,
    total_desc: &str,
) -> SensorId {
    let sensor = metrics.sensor(sensor_name);
    let rate = metrics.metric_name(rate_name, CLIENT_GROUP, rate_desc);
    let total = metrics.metric_name(total_name, CLIENT_GROUP, total_desc);
    let _ignored = metrics.sensor_add_meter(sensor, rate, total);
    sensor
}

#[expect(
    clippy::too_many_arguments,
    reason = "Sensor builder threads sensor + avg/max metric names and descriptions."
)]
fn avg_max(
    metrics: &mut Metrics,
    sensor_name: &str,
    avg_name: &str,
    avg_desc: &str,
    max_name: &str,
    max_desc: &str,
) -> SensorId {
    let sensor = metrics.sensor(sensor_name);
    let avg = metrics.metric_name(avg_name, CLIENT_GROUP, avg_desc);
    let max = metrics.metric_name(max_name, CLIENT_GROUP, max_desc);
    let _ignored = metrics.sensor_add_avg(sensor, avg);
    let _ignored = metrics.sensor_add_max(sensor, max);
    sensor
}

fn avg_only(metrics: &mut Metrics, sensor_name: &str, avg_name: &str, avg_desc: &str) -> SensorId {
    let sensor = metrics.sensor(sensor_name);
    let avg = metrics.metric_name(avg_name, CLIENT_GROUP, avg_desc);
    let _ignored = metrics.sensor_add_avg(sensor, avg);
    sensor
}

fn value_only(
    metrics: &mut Metrics,
    sensor_name: &str,
    value_name: &str,
    value_desc: &str,
) -> SensorId {
    let sensor = metrics.sensor(sensor_name);
    let value = metrics.metric_name(value_name, CLIENT_GROUP, value_desc);
    let _ignored = metrics.sensor_add_value(sensor, value);
    sensor
}

#[expect(
    clippy::too_many_arguments,
    reason = "Sensor builder threads topic + sensor + rate/total metric names and descriptions."
)]
fn topic_meter(
    metrics: &mut Metrics,
    topic: &str,
    sensor_suffix: &str,
    rate_name: &str,
    rate_desc: &str,
    total_name: &str,
    total_desc: &str,
) -> SensorId {
    let sensor = metrics.sensor(format!("topic.{topic}.{sensor_suffix}"));
    let rate = metrics.metric_name_with_tags(rate_name, TOPIC_GROUP, rate_desc, [("topic", topic)]);
    let total =
        metrics.metric_name_with_tags(total_name, TOPIC_GROUP, total_desc, [("topic", topic)]);
    let _ignored = metrics.sensor_add_meter(sensor, rate, total);
    sensor
}

fn topic_avg(
    metrics: &mut Metrics,
    topic: &str,
    sensor_suffix: &str,
    avg_name: &str,
    avg_desc: &str,
) -> SensorId {
    let sensor = metrics.sensor(format!("topic.{topic}.{sensor_suffix}"));
    let avg = metrics.metric_name_with_tags(avg_name, TOPIC_GROUP, avg_desc, [("topic", topic)]);
    let _ignored = metrics.sensor_add_avg(sensor, avg);
    sensor
}

#[cfg(test)]
mod tests {
    #![allow(clippy::float_cmp, reason = "Metric totals are exact integer sums.")]

    use super::SenderMetricsRegistry;

    #[test]
    fn exposes_java_named_client_and_topic_metrics() {
        let registry = SenderMetricsRegistry::default();
        registry.record_batch("orders", 3, 300, 0.5);
        registry.record_batch("orders", 2, 200, 0.5);
        registry.record_error(Some("orders"));
        registry.record_retry(None);
        registry.record_split();

        let metrics = registry.kafka_metrics();

        // Client-level cumulative totals (Java Meter total = sum of recorded values).
        assert_eq!(metrics.get("producer-metrics:record-send-total"), Some(&5.0));
        assert_eq!(metrics.get("producer-metrics:byte-total"), Some(&500.0));
        assert_eq!(
            metrics.get("producer-metrics:record-error-total"),
            Some(&1.0)
        );
        assert_eq!(
            metrics.get("producer-metrics:record-retry-total"),
            Some(&1.0)
        );
        assert_eq!(
            metrics.get("producer-metrics:batch-split-total"),
            Some(&1.0)
        );
        assert_eq!(
            metrics.get("producer-metrics:compression-rate-avg"),
            Some(&0.5)
        );

        // Per-topic instances under the producer-topic-metrics group.
        assert_eq!(
            metrics.get("producer-topic-metrics:record-send-total:topic=orders"),
            Some(&5.0)
        );
        assert_eq!(
            metrics.get("producer-topic-metrics:byte-total:topic=orders"),
            Some(&500.0)
        );
        assert_eq!(
            metrics.get("producer-topic-metrics:record-error-total:topic=orders"),
            Some(&1.0)
        );

        // Rate/avg/gauge sensors are registered under their Kafka names.
        for name in [
            "producer-metrics:record-send-rate",
            "producer-metrics:record-error-rate",
            "producer-metrics:records-per-request-avg",
            "producer-metrics:request-latency-avg",
            "producer-metrics:batch-size-avg",
            "producer-metrics:requests-in-flight",
        ] {
            assert!(metrics.contains_key(name), "missing metric {name}");
        }
    }
}
