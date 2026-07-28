//! Metrics registry primitives mirroring Kafka's metrics model.
//!
//! Four types here are part of the public producer API, because
//! [`Producer::register_kafka_metric_for_subscription`][register] and
//! [`Producer::add_metric_reporter`][reporter] take them: [`MetricName`],
//! [`MetricValue`], [`KafkaMetric`], and [`MetricReporter`].
//!
//! The registry itself is internal. Its only caller is
//! [`SenderMetricsRegistry`](super::SenderMetricsRegistry), which publishes the
//! producer's measurements under Kafka's metric names, and the surface below is
//! deliberately limited to what that caller needs — an application that wants a
//! general-purpose metrics library should use one.
//!
//! [register]: super::super::Producer::register_kafka_metric_for_subscription
//! [reporter]: super::super::Producer::add_metric_reporter

use std::{
    collections::{BTreeMap, HashMap},
    fmt,
    hash::{Hash, Hasher},
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

/// Kafka `MetricConfig.samples()` default: how many completed windows a
/// [`StatKind::Rate`] averages over.
const RATE_SAMPLES: usize = 2;

/// Kafka `MetricConfig.timeWindowMs()` default: the length of one rate window.
const RATE_TIME_WINDOW_MS: u64 = 30_000;

/// Numeric metric value.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum MetricValue {
    /// Floating-point metric value.
    Number(f64),
}

impl MetricValue {
    const fn as_f64(self) -> f64 {
        match self {
            Self::Number(value) => value,
        }
    }
}

/// Metric identity (Kafka's `MetricName`).
#[derive(Clone, Eq)]
pub struct MetricName {
    name: String,
    group: String,
    description: String,
    tags: BTreeMap<String, String>,
}

impl MetricName {
    /// Create a metric name with no description or tags.
    #[must_use]
    pub fn new(name: impl Into<String>, group: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            group: group.into(),
            description: String::new(),
            tags: BTreeMap::new(),
        }
    }

    /// Set the human-readable metric description.
    #[must_use]
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = description.into();
        self
    }

    /// Add or replace a metric tag.
    #[must_use]
    pub fn tag(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        let _previous = self.tags.insert(key.into(), value.into());
        self
    }

    /// Metric name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Logical metric group.
    #[must_use]
    pub fn group(&self) -> &str {
        &self.group
    }

    /// Human-readable metric description.
    #[must_use]
    pub fn description(&self) -> &str {
        &self.description
    }

    /// Metric tags.
    #[must_use]
    pub const fn tags(&self) -> &BTreeMap<String, String> {
        &self.tags
    }
}

impl PartialEq for MetricName {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.group == other.group && self.tags == other.tags
    }
}

impl Hash for MetricName {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.group.hash(state);
        self.tags.hash(state);
    }
}

impl Ord for MetricName {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.group
            .cmp(&other.group)
            .then_with(|| self.name.cmp(&other.name))
            .then_with(|| self.tags.cmp(&other.tags))
    }
}

impl PartialOrd for MetricName {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl fmt::Debug for MetricName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MetricName")
            .field("name", &self.name)
            .field("group", &self.group)
            .field("description", &self.description)
            .field("tags", &self.tags)
            .finish()
    }
}

/// Registered metric with a value provider.
#[derive(Clone)]
pub struct KafkaMetric {
    metric_name: MetricName,
    provider: Arc<dyn Fn(u64) -> MetricValue + Send + Sync>,
}

impl KafkaMetric {
    fn new_at_ms(
        metric_name: MetricName,
        provider: impl Fn(u64) -> MetricValue + Send + Sync + 'static,
    ) -> Self {
        Self {
            metric_name,
            provider: Arc::new(provider),
        }
    }

    /// Create a metric from a value provider.
    #[must_use]
    pub fn from_fn(
        metric_name: MetricName,
        provider: impl Fn() -> MetricValue + Send + Sync + 'static,
    ) -> Self {
        Self::new_at_ms(metric_name, move |_now_ms| provider())
    }

    /// Metric identity.
    #[must_use]
    pub const fn metric_name(&self) -> &MetricName {
        &self.metric_name
    }

    /// Read the current metric value.
    #[must_use]
    pub fn metric_value(&self) -> f64 {
        self.metric_value_at_ms(current_time_ms())
    }

    /// Read the metric value at an explicit millisecond timestamp.
    #[must_use]
    pub fn metric_value_at_ms(&self, time_ms: u64) -> f64 {
        (self.provider)(time_ms).as_f64()
    }
}

impl fmt::Debug for KafkaMetric {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KafkaMetric")
            .field("metric_name", &self.metric_name)
            .finish_non_exhaustive()
    }
}

/// Rust-native metrics reporter lifecycle.
pub trait MetricReporter: fmt::Debug + Send + Sync + 'static {
    /// Initialize reporter with currently registered metrics.
    fn init(&self, _metrics: &[KafkaMetric]) {}

    /// Observe a newly registered or changed metric.
    fn metric_change(&self, _metric: &KafkaMetric) {}

    /// Observe a removed metric.
    fn metric_removal(&self, _metric: &KafkaMetric) {}

    /// Release reporter resources.
    fn close(&self) {}
}

/// Opaque sensor identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct SensorId(usize);

/// The statistic a sensor keeps for one metric, mirroring Kafka's
/// `MeasurableStat` implementations.
///
/// This is the single axis that used to be spread over 26 near-identical
/// `sensor_add_*` methods; pass it to [`Metrics::sensor_add_stat`] instead. The
/// vocabulary is exactly the stats kacrab registers — Kafka also has `Min`,
/// `Count`, `TokenBucket`, and `Frequencies`, each a handful of lines to
/// reintroduce alongside its first caller.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StatKind {
    /// Running sum of every recorded value (Kafka `CumulativeSum`).
    Total,
    /// The value of the most recent record (Kafka `Value`).
    Value,
    /// Mean of every recorded value, `NaN` before the first record (Kafka `Avg`).
    Avg,
    /// Largest recorded value, `NaN` before the first record (Kafka `Max`).
    Max,
    /// Per-second rate over [`RATE_SAMPLES`] windows (Kafka `Rate`).
    Rate,
}

/// Metrics registry mirroring the part of Kafka's
/// `org.apache.kafka.common.metrics` that kacrab publishes.
#[derive(Debug, Default)]
pub(crate) struct Metrics {
    registered: BTreeMap<MetricName, KafkaMetric>,
    /// Live sensors, keyed by the numeric part of their [`SensorId`].
    ///
    /// A map rather than a slot `Vec` because sensors are evicted: a producer
    /// writing to a rotating set of topics registers and expires topic sensors
    /// indefinitely, and a `Vec` would keep a tombstone for every sensor that
    /// ever existed — the same unbounded growth the eviction is there to stop.
    sensors: HashMap<usize, SensorState>,
    sensors_by_name: HashMap<String, SensorId>,
    next_sensor_id: usize,
}

#[derive(Debug)]
struct SensorState {
    name: String,
    stats: Vec<SensorStat>,
    inactive_expiration_ms: Option<u64>,
    last_record_time_ms: u64,
}

#[derive(Debug, Clone)]
struct SensorStat {
    metric_name: MetricName,
    state: SensorStatState,
}

#[derive(Debug, Clone)]
enum SensorStatState {
    Total { value: Arc<Mutex<f64>> },
    Value { value: Arc<Mutex<f64>> },
    Avg { state: Arc<Mutex<AvgSensorStat>> },
    Max { state: Arc<Mutex<MaxSensorStat>> },
    Rate { state: Arc<Mutex<WindowedRateStat>> },
}

#[derive(Debug, Default)]
struct AvgSensorStat {
    total: f64,
    count: f64,
}

#[derive(Debug)]
struct MaxSensorStat {
    value: f64,
    count: f64,
}

#[derive(Debug)]
struct WindowedRateStat {
    samples: Vec<WindowedSample>,
    current: usize,
}

#[derive(Debug, Clone, Copy)]
struct WindowedSample {
    value: f64,
    start_time_ms: u64,
    last_event_ms: u64,
}

impl WindowedRateStat {
    const fn new() -> Self {
        Self {
            samples: Vec::new(),
            current: 0,
        }
    }

    fn record(&mut self, value: f64, time_ms: u64) {
        self.ensure_current_sample(time_ms);
        if self
            .samples
            .get(self.current)
            .is_some_and(|sample| sample.is_complete(time_ms))
        {
            self.advance(time_ms);
        }
        self.ensure_current_sample(time_ms);
        if let Some(sample) = self.samples.get_mut(self.current) {
            sample.value += value;
            sample.last_event_ms = time_ms;
        }
    }

    fn measure(&mut self, now_ms: u64) -> f64 {
        self.purge_obsolete_samples(now_ms);
        let value = self.samples.iter().map(|sample| sample.value).sum::<f64>();
        let window_size_ms = u32::try_from(self.window_size_ms(now_ms)).unwrap_or(u32::MAX);
        value / (f64::from(window_size_ms) / 1000.0)
    }

    fn ensure_current_sample(&mut self, time_ms: u64) {
        if self.samples.is_empty() {
            self.samples.push(WindowedSample::new(time_ms));
        }
        if self.current >= self.samples.len() {
            self.current = self.samples.len().saturating_sub(1);
        }
    }

    fn advance(&mut self, time_ms: u64) {
        let max_samples = RATE_SAMPLES.saturating_add(1);
        self.current = self
            .current
            .saturating_add(1)
            .checked_rem(max_samples)
            .unwrap_or(0);
        if self.current >= self.samples.len() {
            self.samples.push(WindowedSample::new(time_ms));
        } else if let Some(sample) = self.samples.get_mut(self.current) {
            sample.reset(time_ms);
        }
    }

    fn purge_obsolete_samples(&mut self, now_ms: u64) {
        let expire_age_ms = u64::try_from(RATE_SAMPLES)
            .unwrap_or(u64::MAX)
            .saturating_mul(RATE_TIME_WINDOW_MS);
        for sample in &mut self.samples {
            if now_ms.saturating_sub(sample.last_event_ms) >= expire_age_ms {
                sample.reset(now_ms);
            }
        }
    }

    fn window_size_ms(&mut self, now_ms: u64) -> u64 {
        if self.samples.is_empty() {
            self.samples.push(WindowedSample::new(now_ms));
        }
        let oldest_start_ms = self
            .samples
            .iter()
            .map(|sample| sample.start_time_ms)
            .min()
            .unwrap_or(now_ms);
        let mut total_elapsed_ms = now_ms.saturating_sub(oldest_start_ms);
        let full_windows = usize::try_from(
            total_elapsed_ms
                .checked_div(RATE_TIME_WINDOW_MS)
                .unwrap_or(0),
        )
        .unwrap_or(0);
        let min_full_windows = RATE_SAMPLES.saturating_sub(1);
        if full_windows < min_full_windows {
            let missing = min_full_windows.saturating_sub(full_windows);
            let missing_ms = u64::try_from(missing)
                .unwrap_or(u64::MAX)
                .saturating_mul(RATE_TIME_WINDOW_MS);
            total_elapsed_ms = total_elapsed_ms.saturating_add(missing_ms);
        }
        total_elapsed_ms.max(1)
    }
}

impl WindowedSample {
    const fn new(time_ms: u64) -> Self {
        Self {
            value: 0.0,
            start_time_ms: time_ms,
            last_event_ms: time_ms,
        }
    }

    const fn reset(&mut self, time_ms: u64) {
        *self = Self::new(time_ms);
    }

    const fn is_complete(self, time_ms: u64) -> bool {
        time_ms.saturating_sub(self.start_time_ms) >= RATE_TIME_WINDOW_MS
    }
}

impl SensorStat {
    fn new(metric_name: MetricName, kind: StatKind) -> (Self, KafkaMetric) {
        match kind {
            StatKind::Total | StatKind::Value => {
                let value = Arc::new(Mutex::new(0.0));
                let metric_value = Arc::clone(&value);
                let metric = KafkaMetric::new_at_ms(metric_name.clone(), move |_now_ms| {
                    let value = metric_value
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    MetricValue::Number(*value)
                });
                let state = if matches!(kind, StatKind::Total) {
                    SensorStatState::Total { value }
                } else {
                    SensorStatState::Value { value }
                };
                (Self { metric_name, state }, metric)
            },
            StatKind::Avg => {
                let state = Arc::new(Mutex::new(AvgSensorStat::default()));
                let metric_state = Arc::clone(&state);
                let metric = KafkaMetric::new_at_ms(metric_name.clone(), move |_now_ms| {
                    let state = metric_state
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    if state.count == 0.0 {
                        return MetricValue::Number(f64::NAN);
                    }
                    MetricValue::Number(state.total / state.count)
                });
                (
                    Self {
                        metric_name,
                        state: SensorStatState::Avg { state },
                    },
                    metric,
                )
            },
            StatKind::Max => {
                let state = Arc::new(Mutex::new(MaxSensorStat {
                    value: f64::NEG_INFINITY,
                    count: 0.0,
                }));
                let metric_state = Arc::clone(&state);
                let metric = KafkaMetric::new_at_ms(metric_name.clone(), move |_now_ms| {
                    let state = metric_state
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    if state.count == 0.0 {
                        return MetricValue::Number(f64::NAN);
                    }
                    MetricValue::Number(state.value)
                });
                (
                    Self {
                        metric_name,
                        state: SensorStatState::Max { state },
                    },
                    metric,
                )
            },
            StatKind::Rate => {
                let state = Arc::new(Mutex::new(WindowedRateStat::new()));
                let metric_state = Arc::clone(&state);
                let metric = KafkaMetric::new_at_ms(metric_name.clone(), move |now_ms| {
                    let mut state = metric_state
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    MetricValue::Number(state.measure(now_ms))
                });
                (
                    Self {
                        metric_name,
                        state: SensorStatState::Rate { state },
                    },
                    metric,
                )
            },
        }
    }

    fn record(&self, value: f64, time_ms: u64) {
        match &self.state {
            SensorStatState::Total { value: current } => {
                let mut current = current
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                *current += value;
            },
            SensorStatState::Value { value: current } => {
                let mut current = current
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                *current = value;
            },
            SensorStatState::Avg { state } => {
                let mut state = state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                state.total += value;
                state.count += 1.0;
            },
            SensorStatState::Max { state } => {
                let mut state = state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                state.value = state.value.max(value);
                state.count += 1.0;
            },
            SensorStatState::Rate { state } => {
                let mut state = state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                state.record(value, time_ms);
            },
        }
    }
}

/// Create a metric name.
pub(crate) fn metric_name(name: &str, group: &str, description: &str) -> MetricName {
    MetricName::new(name, group).with_description(description)
}

/// Create a metric name carrying `tags`.
pub(crate) fn metric_name_with_tags<'a, I>(
    name: &str,
    group: &str,
    description: &str,
    tags: I,
) -> MetricName
where
    I: IntoIterator<Item = (&'a str, &'a str)>,
{
    let mut metric_name = metric_name(name, group, description);
    for (key, value) in tags {
        metric_name = metric_name.tag(key, value);
    }
    metric_name
}

impl Metrics {
    /// Create an empty metrics registry.
    #[must_use]
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Return a sensor by name, creating it when missing.
    ///
    /// The sensor never expires; use [`Self::sensor_with_expiration`] for one
    /// that should be evicted once it goes quiet.
    pub(crate) fn sensor(&mut self, name: impl Into<String>) -> SensorId {
        self.insert_sensor(name.into(), None)
    }

    /// Return a sensor by name, creating it when missing, that
    /// [`Self::expire_sensors_at_ms`] removes after `inactive_expiration`
    /// without a record.
    pub(crate) fn sensor_with_expiration(
        &mut self,
        name: impl Into<String>,
        inactive_expiration: Duration,
    ) -> SensorId {
        let expiration_ms = u64::try_from(inactive_expiration.as_millis()).ok();
        self.insert_sensor(name.into(), expiration_ms)
    }

    fn insert_sensor(&mut self, name: String, inactive_expiration_ms: Option<u64>) -> SensorId {
        if let Some(sensor) = self.sensors_by_name.get(&name).copied() {
            return sensor;
        }
        let sensor = SensorId(self.next_sensor_id);
        self.next_sensor_id = self.next_sensor_id.saturating_add(1);
        // Kafka's `Sensor` constructor seeds `lastRecordTime` from the clock, so
        // a sensor is never born already expired.
        let _previous = self.sensors.insert(
            sensor.0,
            SensorState {
                name: name.clone(),
                stats: Vec::new(),
                inactive_expiration_ms,
                last_record_time_ms: current_time_ms(),
            },
        );
        let _previous = self.sensors_by_name.insert(name, sensor);
        sensor
    }

    /// Register `metric_name` on `sensor`, computed by `kind`.
    ///
    /// This is the one entry point for every sensor statistic; the wrappers
    /// below exist only because
    /// [`SenderMetricsRegistry`](super::SenderMetricsRegistry) reads better
    /// calling them by name.
    ///
    /// # Errors
    ///
    /// Returns an error when the sensor is missing, or when the metric name is
    /// already registered to a different metric. Re-adding a metric already on
    /// this sensor is a no-op.
    pub(crate) fn sensor_add_stat(
        &mut self,
        sensor: SensorId,
        metric_name: MetricName,
        kind: StatKind,
    ) -> Result<(), MetricsError> {
        if self
            .sensor_state(sensor)?
            .stats
            .iter()
            .any(|stat| stat.metric_name == metric_name)
        {
            return Ok(());
        }
        let (stat, metric) = SensorStat::new(metric_name.clone(), kind);
        self.add_kafka_metric(metric_name, metric)?;
        self.sensor_mut(sensor)?.stats.push(stat);
        Ok(())
    }

    /// Add a latest-value statistic to a sensor.
    ///
    /// # Errors
    ///
    /// As [`Self::sensor_add_stat`].
    pub(crate) fn sensor_add_value(
        &mut self,
        sensor: SensorId,
        metric_name: MetricName,
    ) -> Result<(), MetricsError> {
        self.sensor_add_stat(sensor, metric_name, StatKind::Value)
    }

    /// Add an average statistic to a sensor.
    ///
    /// # Errors
    ///
    /// As [`Self::sensor_add_stat`].
    pub(crate) fn sensor_add_avg(
        &mut self,
        sensor: SensorId,
        metric_name: MetricName,
    ) -> Result<(), MetricsError> {
        self.sensor_add_stat(sensor, metric_name, StatKind::Avg)
    }

    /// Add a maximum statistic to a sensor.
    ///
    /// # Errors
    ///
    /// As [`Self::sensor_add_stat`].
    pub(crate) fn sensor_add_max(
        &mut self,
        sensor: SensorId,
        metric_name: MetricName,
    ) -> Result<(), MetricsError> {
        self.sensor_add_stat(sensor, metric_name, StatKind::Max)
    }

    /// Add a Kafka `Meter` compound statistic: cumulative total plus rate.
    ///
    /// # Errors
    ///
    /// As [`Self::sensor_add_stat`], for either metric name.
    pub(crate) fn sensor_add_meter(
        &mut self,
        sensor: SensorId,
        rate_metric_name: MetricName,
        total_metric_name: MetricName,
    ) -> Result<(), MetricsError> {
        self.sensor_add_stat(sensor, total_metric_name, StatKind::Total)?;
        self.sensor_add_stat(sensor, rate_metric_name, StatKind::Rate)
    }

    /// Record a sensor value at an explicit millisecond timestamp.
    ///
    /// # Errors
    ///
    /// Returns an error when `sensor` does not exist.
    pub(crate) fn record_at_ms(
        &mut self,
        sensor: SensorId,
        value: f64,
        time_ms: u64,
    ) -> Result<(), MetricsError> {
        let state = self.sensor_mut(sensor)?;
        state.last_record_time_ms = time_ms;
        for stat in &state.stats {
            stat.record(value, time_ms);
        }
        Ok(())
    }

    /// Iterate every registered metric and its current value handle.
    pub(crate) fn registered_metrics(&self) -> impl Iterator<Item = (&MetricName, &KafkaMetric)> {
        self.registered.iter()
    }

    /// Return whether `sensor` is still registered.
    ///
    /// Callers that cache their own [`SensorId`]s use this after
    /// [`Self::expire_sensors_at_ms`] to drop the ones that were evicted.
    pub(crate) fn sensor_exists(&self, sensor: SensorId) -> bool {
        self.sensors.contains_key(&sensor.0)
    }

    /// Remove every sensor that has gone longer than its inactive expiration
    /// without a record, along with the metrics it owns.
    ///
    /// Returns the number of sensors removed, mirroring Kafka's expire-sensor
    /// task. Sensors created with [`Self::sensor`] have no expiration and are
    /// never removed here.
    pub(crate) fn expire_sensors_at_ms(&mut self, now_ms: u64) -> usize {
        let expired = self
            .sensors
            .iter()
            .filter(|(_id, state)| is_sensor_expired(state, now_ms))
            .map(|(id, _state)| SensorId(*id))
            .collect::<Vec<_>>();
        let removed = expired.len();
        for sensor in expired {
            self.remove_sensor_by_id(sensor);
        }
        removed
    }

    fn add_kafka_metric(
        &mut self,
        metric_name: MetricName,
        metric: KafkaMetric,
    ) -> Result<(), MetricsError> {
        if self.registered.contains_key(&metric_name) {
            return Err(MetricsError::DuplicateMetric(metric_name));
        }
        let _previous = self.registered.insert(metric_name, metric);
        Ok(())
    }

    fn sensor_state(&self, sensor: SensorId) -> Result<&SensorState, MetricsError> {
        self.sensors
            .get(&sensor.0)
            .ok_or(MetricsError::UnknownSensor { sensor })
    }

    fn sensor_mut(&mut self, sensor: SensorId) -> Result<&mut SensorState, MetricsError> {
        self.sensors
            .get_mut(&sensor.0)
            .ok_or(MetricsError::UnknownSensor { sensor })
    }

    fn remove_sensor_by_id(&mut self, sensor: SensorId) {
        let Some(state) = self.sensors.remove(&sensor.0) else {
            return;
        };
        let _removed = self.sensors_by_name.remove(&state.name);
        for stat in state.stats {
            let _removed = self.registered.remove(&stat.metric_name);
        }
    }
}

fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| u64::try_from(duration.as_millis()).ok())
        .unwrap_or(u64::MAX)
}

fn is_sensor_expired(state: &SensorState, now_ms: u64) -> bool {
    state
        .inactive_expiration_ms
        .is_some_and(|expiration| now_ms.saturating_sub(state.last_record_time_ms) > expiration)
}

/// Metrics registry error.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum MetricsError {
    /// Metric already exists.
    DuplicateMetric(MetricName),
    /// Sensor was not found.
    UnknownSensor {
        /// Missing sensor id.
        sensor: SensorId,
    },
}

impl fmt::Display for MetricsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateMetric(metric) => {
                write!(f, "metric already exists: {}", metric.name())
            },
            Self::UnknownSensor { sensor } => write!(f, "unknown sensor: {}", sensor.0),
        }
    }
}

impl std::error::Error for MetricsError {}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::expect_used,
        clippy::float_cmp,
        clippy::missing_assert_message,
        clippy::unwrap_used,
        reason = "Unit tests for the metrics registry assert exact recorded values and fail \
                  fastest with contextual expect calls."
    )]

    use std::time::Duration;

    use super::{
        KafkaMetric, MetricName, MetricValue, Metrics, MetricsError, StatKind, metric_name,
        metric_name_with_tags,
    };

    fn value_of(metrics: &Metrics, name: &MetricName) -> f64 {
        metrics
            .registered_metrics()
            .find(|(registered, _metric)| *registered == name)
            .map(|(_name, metric)| metric.metric_value())
            .expect("metric is registered")
    }

    #[test]
    fn sensor_stats_record_expected_values() {
        let mut metrics = Metrics::new();
        let sensor = metrics.sensor("throughput");

        let max = metric_name("max", "g", "max stat");
        let avg = metric_name("avg", "g", "avg stat");
        let total = metric_name("total", "g", "total stat");
        let value = metric_name("value", "g", "value stat");
        let meter_rate = metric_name("m-rate", "g", "meter rate");
        let meter_total = metric_name("m-total", "g", "meter total");

        metrics.sensor_add_max(sensor, max.clone()).expect("max");
        metrics.sensor_add_avg(sensor, avg.clone()).expect("avg");
        metrics
            .sensor_add_stat(sensor, total.clone(), StatKind::Total)
            .expect("total");
        metrics
            .sensor_add_value(sensor, value.clone())
            .expect("value");
        metrics
            .sensor_add_meter(sensor, meter_rate, meter_total.clone())
            .expect("meter");

        for sample in [2.0, 8.0, 5.0] {
            metrics.record_at_ms(sensor, sample, 1_000).expect("record");
        }

        assert_eq!(value_of(&metrics, &max), 8.0);
        assert_eq!(value_of(&metrics, &avg), 5.0);
        assert_eq!(value_of(&metrics, &total), 15.0);
        assert_eq!(value_of(&metrics, &value), 5.0);
        assert_eq!(value_of(&metrics, &meter_total), 15.0);
    }

    #[test]
    fn avg_and_max_report_nan_until_the_first_record_like_java() {
        let mut metrics = Metrics::new();
        let sensor = metrics.sensor("request-size");
        let avg = metric_name("request-size-avg", "producer-metrics", "");
        let max = metric_name("request-size-max", "producer-metrics", "");

        metrics.sensor_add_avg(sensor, avg.clone()).expect("avg");
        metrics.sensor_add_max(sensor, max.clone()).expect("max");
        assert!(value_of(&metrics, &avg).is_nan());
        assert!(value_of(&metrics, &max).is_nan());

        for sample in [7.0, 2.0, 9.0, 4.0] {
            metrics.record_at_ms(sensor, sample, 1_000).expect("record");
        }

        assert_eq!(value_of(&metrics, &avg), 5.5);
        assert_eq!(value_of(&metrics, &max), 9.0);
    }

    #[test]
    fn rate_stat_uses_the_java_window_size_rule() {
        // Kafka pads the elapsed window out to `samples - 1` full windows, so a
        // single 30.0 recorded one second in reads as 30 / 30s = 1.0, not 30/1s.
        let mut metrics = Metrics::new();
        let sensor = metrics.sensor("request-size");
        let rate = metric_name("request-size-rate", "producer-metrics", "");

        metrics
            .sensor_add_stat(sensor, rate.clone(), StatKind::Rate)
            .expect("rate");
        metrics
            .record_at_ms(sensor, 30.0, 1_000)
            .expect("record rate value");

        let metric = metrics
            .registered_metrics()
            .find(|(name, _metric)| **name == rate)
            .map(|(_name, metric)| metric)
            .expect("rate metric is registered");
        assert_eq!(metric.metric_value_at_ms(1_000), 1.0);
    }

    #[test]
    fn meter_registers_a_rate_and_a_total_like_java() {
        let mut metrics = Metrics::new();
        let sensor = metrics.sensor("request-size");
        let rate = metric_name("request-size-rate", "producer-metrics", "");
        let total = metric_name("request-size-total", "producer-metrics", "");

        metrics
            .sensor_add_meter(sensor, rate.clone(), total.clone())
            .expect("meter metrics");
        metrics
            .record_at_ms(sensor, 30.0, 1_000)
            .expect("record meter value");
        metrics
            .record_at_ms(sensor, 15.0, 31_000)
            .expect("record meter value");

        assert_eq!(value_of(&metrics, &total), 45.0);
        let metric = metrics
            .registered_metrics()
            .find(|(name, _metric)| **name == rate)
            .map(|(_name, metric)| metric)
            .expect("rate metric is registered");
        assert_eq!(metric.metric_value_at_ms(31_000), 1.5);
    }

    #[test]
    fn re_adding_the_same_metric_to_one_sensor_is_a_noop_like_java() {
        let mut metrics = Metrics::new();
        let sensor = metrics.sensor("request-size");
        let name = metric_name("request-size-total", "producer-metrics", "");

        metrics
            .sensor_add_stat(sensor, name.clone(), StatKind::Total)
            .expect("first metric");
        metrics.record_at_ms(sensor, 2.0, 1_000).expect("record");
        metrics
            .sensor_add_stat(sensor, name.clone(), StatKind::Total)
            .expect("duplicate metric should be a no-op");
        metrics.record_at_ms(sensor, 3.0, 1_000).expect("record");

        // A second stat would have double-counted the 3.0 into a fresh total.
        assert_eq!(value_of(&metrics, &name), 5.0);
        assert_eq!(metrics.registered_metrics().count(), 1);
    }

    #[test]
    fn duplicate_metric_name_on_a_second_sensor_is_an_error() {
        let mut metrics = Metrics::new();
        let first = metrics.sensor("first");
        let second = metrics.sensor("second");
        let name = metric_name("shared", "g", "shared metric");

        metrics
            .sensor_add_stat(first, name.clone(), StatKind::Total)
            .expect("first registration");
        let clash = metrics
            .sensor_add_stat(second, name.clone(), StatKind::Total)
            .expect_err("a second sensor must not silently shadow the metric");

        assert_eq!(clash, MetricsError::DuplicateMetric(name));
        assert!(format!("{clash}").contains("metric already exists"));
    }

    #[test]
    fn an_idle_sensor_is_evicted_with_its_metrics() {
        let mut metrics = Metrics::new();
        let sensor = metrics.sensor_with_expiration("ephemeral", Duration::from_millis(100));
        let name = metric_name("e", "g", "ephemeral value");
        metrics.sensor_add_value(sensor, name).expect("add value");
        metrics.record_at_ms(sensor, 1.0, 1_000).expect("record");

        assert_eq!(metrics.expire_sensors_at_ms(1_050), 0);
        assert!(metrics.sensor_exists(sensor));

        assert_eq!(metrics.expire_sensors_at_ms(2_000), 1);
        assert!(!metrics.sensor_exists(sensor));
        assert!(metrics.registered_metrics().next().is_none());
        assert!(matches!(
            metrics.record_at_ms(sensor, 1.0, 2_000),
            Err(MetricsError::UnknownSensor { sensor: missing }) if missing == sensor
        ));
    }

    #[test]
    fn a_sensor_without_an_expiration_is_never_evicted() {
        let mut metrics = Metrics::new();
        let sensor = metrics.sensor("client-level");
        let name = metric_name("c", "g", "client value");
        metrics.sensor_add_value(sensor, name).expect("add value");

        assert_eq!(metrics.expire_sensors_at_ms(u64::MAX), 0);
        assert!(metrics.sensor_exists(sensor));
    }

    #[test]
    fn re_registering_an_evicted_sensor_name_yields_a_fresh_sensor() {
        let mut metrics = Metrics::new();
        let name = metric_name("v", "g", "");
        let first = metrics.sensor_with_expiration("topic.orders", Duration::from_millis(100));
        metrics
            .sensor_add_value(first, name.clone())
            .expect("add value");
        metrics.record_at_ms(first, 1.0, 1_000).expect("record");
        assert_eq!(metrics.expire_sensors_at_ms(2_000), 1);

        let second = metrics.sensor_with_expiration("topic.orders", Duration::from_millis(100));

        assert_ne!(first, second);
        assert!(metrics.sensor_exists(second));
        // The metric name went with the old sensor, so re-registering it under
        // the new one is not a duplicate.
        metrics
            .sensor_add_value(second, name)
            .expect("re-add value");
    }

    #[test]
    fn metric_name_identity_ignores_the_description() {
        let tagged = metric_name_with_tags("lag", "grp", "d", [("topic", "orders")]);
        let same = metric_name_with_tags("lag", "grp", "other", [("topic", "orders")]);
        let other_tag = metric_name_with_tags("lag", "grp", "d", [("topic", "audit")]);

        assert_eq!(tagged, same);
        assert_ne!(tagged, other_tag);
        assert_eq!(
            tagged.tags().get("topic").map(String::as_str),
            Some("orders")
        );

        let metric = KafkaMetric::from_fn(tagged, || MetricValue::Number(1.0));
        assert!(format!("{metric:?}").contains("KafkaMetric"));
        assert_eq!(metric.metric_value(), 1.0);
    }
}
