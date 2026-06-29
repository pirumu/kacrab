//! Metrics registry primitives mirroring Kafka's metrics model.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    fmt,
    hash::{Hash, Hasher},
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

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

/// Template for creating [`MetricName`] values with a fixed tag set.
#[derive(Clone)]
pub struct MetricNameTemplate {
    name: String,
    group: String,
    description: String,
    tags: Vec<String>,
}

impl MetricNameTemplate {
    /// Create a template with tag names in preferred display order.
    #[must_use]
    pub fn new<I, T>(
        name: impl Into<String>,
        group: impl Into<String>,
        description: impl Into<String>,
        tag_names: I,
    ) -> Self
    where
        I: IntoIterator<Item = T>,
        T: Into<String>,
    {
        let mut seen = BTreeSet::new();
        let mut tags = Vec::new();
        for tag in tag_names {
            let tag = tag.into();
            if seen.insert(tag.clone()) {
                tags.push(tag);
            }
        }
        Self {
            name: name.into(),
            group: group.into(),
            description: description.into(),
            tags,
        }
    }

    /// Metric name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Metric group.
    #[must_use]
    pub fn group(&self) -> &str {
        &self.group
    }

    /// Metric description.
    #[must_use]
    pub fn description(&self) -> &str {
        &self.description
    }

    /// Ordered tag names used by this template.
    #[must_use]
    pub fn tags(&self) -> &[String] {
        &self.tags
    }

    fn tag_set(&self) -> BTreeSet<&str> {
        self.tags.iter().map(String::as_str).collect()
    }
}

impl PartialEq for MetricNameTemplate {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.group == other.group && self.tag_set() == other.tag_set()
    }
}

impl Eq for MetricNameTemplate {}

impl Hash for MetricNameTemplate {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.group.hash(state);
        for tag in self.tag_set() {
            tag.hash(state);
        }
    }
}

impl fmt::Debug for MetricNameTemplate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MetricNameTemplate")
            .field("name", &self.name)
            .field("group", &self.group)
            .field("description", &self.description)
            .field("tags", &self.tags)
            .finish()
    }
}

/// Upper or lower bound for a metric.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MetricQuota {
    bound: f64,
    upper: bool,
}

impl MetricQuota {
    /// Create an upper-bound quota.
    #[must_use]
    pub const fn upper_bound(bound: f64) -> Self {
        Self { bound, upper: true }
    }

    /// Create a lower-bound quota.
    #[must_use]
    pub const fn lower_bound(bound: f64) -> Self {
        Self {
            bound,
            upper: false,
        }
    }

    /// Return whether this quota is an upper bound.
    #[must_use]
    pub const fn is_upper_bound(self) -> bool {
        self.upper
    }

    /// Quota bound.
    #[must_use]
    pub const fn bound(self) -> f64 {
        self.bound
    }

    /// Return whether `value` is within the bound.
    #[must_use]
    pub fn acceptable(self, value: f64) -> bool {
        (self.upper && value <= self.bound) || (!self.upper && value >= self.bound)
    }
}

/// Metric configuration.
#[derive(Debug, Clone, PartialEq)]
pub struct MetricConfig {
    quota: Option<MetricQuota>,
    samples: usize,
    event_window: u64,
    time_window_ms: u64,
    tags: BTreeMap<String, String>,
    record_level: SensorRecordingLevel,
}

impl MetricConfig {
    /// Kafka default number of samples.
    pub const DEFAULT_NUM_SAMPLES: usize = 2;
    /// Kafka default time window in milliseconds.
    pub const DEFAULT_TIME_WINDOW_MS: u64 = 30_000;

    /// Create an empty metric configuration.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            quota: None,
            samples: Self::DEFAULT_NUM_SAMPLES,
            event_window: u64::MAX,
            time_window_ms: Self::DEFAULT_TIME_WINDOW_MS,
            tags: BTreeMap::new(),
            record_level: SensorRecordingLevel::Info,
        }
    }

    /// Set the metric quota.
    #[must_use]
    pub const fn with_quota(mut self, quota: MetricQuota) -> Self {
        self.quota = Some(quota);
        self
    }

    /// Configured quota, if any.
    #[must_use]
    pub const fn quota(&self) -> Option<MetricQuota> {
        self.quota
    }

    /// Kafka `MetricConfig.samples()`.
    #[must_use]
    pub const fn samples(&self) -> usize {
        self.samples
    }

    /// Set the number of samples.
    ///
    /// # Errors
    ///
    /// Returns an error when `samples` is less than one.
    pub fn with_samples(mut self, samples: usize) -> Result<Self, MetricsError> {
        if samples < 1 {
            return Err(MetricsError::InvalidMetricConfig {
                reason: "the number of samples must be at least 1".to_owned(),
            });
        }
        self.samples = samples;
        Ok(self)
    }

    /// Kafka `MetricConfig.eventWindow()`.
    #[must_use]
    pub const fn event_window(&self) -> u64 {
        self.event_window
    }

    /// Set the event window.
    #[must_use]
    pub const fn with_event_window(mut self, event_window: u64) -> Self {
        self.event_window = event_window;
        self
    }

    /// Kafka `MetricConfig.timeWindowMs()`.
    #[must_use]
    pub const fn time_window_ms(&self) -> u64 {
        self.time_window_ms
    }

    /// Set the time window in milliseconds.
    #[must_use]
    pub const fn with_time_window_ms(mut self, time_window_ms: u64) -> Self {
        self.time_window_ms = time_window_ms;
        self
    }

    /// Kafka `MetricConfig.tags()`.
    #[must_use]
    pub const fn tags(&self) -> &BTreeMap<String, String> {
        &self.tags
    }

    /// Add or replace a metric config tag.
    #[must_use]
    pub fn with_tag(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        let _previous = self.tags.insert(key.into(), value.into());
        self
    }

    /// Replace metric config tags.
    #[must_use]
    pub fn with_tags<I, K, V>(mut self, tags: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        self.tags = tags
            .into_iter()
            .map(|(key, value)| (key.into(), value.into()))
            .collect();
        self
    }

    /// Kafka `MetricConfig.recordLevel()`.
    #[must_use]
    pub const fn record_level(&self) -> SensorRecordingLevel {
        self.record_level
    }

    /// Set the recording level.
    #[must_use]
    pub const fn with_record_level(mut self, record_level: SensorRecordingLevel) -> Self {
        self.record_level = record_level;
        self
    }
}

impl Default for MetricConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// Registered metric with a value provider.
#[derive(Clone)]
pub struct KafkaMetric {
    metric_name: MetricName,
    provider: Arc<dyn Fn(u64) -> MetricValue + Send + Sync>,
    config: Arc<Mutex<MetricConfig>>,
}

impl KafkaMetric {
    fn new(
        metric_name: MetricName,
        provider: impl Fn() -> MetricValue + Send + Sync + 'static,
    ) -> Self {
        Self::new_with_config(metric_name, MetricConfig::new(), provider)
    }

    fn new_with_config(
        metric_name: MetricName,
        config: MetricConfig,
        provider: impl Fn() -> MetricValue + Send + Sync + 'static,
    ) -> Self {
        Self::new_with_shared_config(metric_name, Arc::new(Mutex::new(config)), move |_now_ms| {
            provider()
        })
    }

    fn new_with_shared_config(
        metric_name: MetricName,
        config: Arc<Mutex<MetricConfig>>,
        provider: impl Fn(u64) -> MetricValue + Send + Sync + 'static,
    ) -> Self {
        Self {
            metric_name,
            provider: Arc::new(provider),
            config,
        }
    }

    /// Create a metric from a value provider.
    #[must_use]
    pub fn from_fn(
        metric_name: MetricName,
        provider: impl Fn() -> MetricValue + Send + Sync + 'static,
    ) -> Self {
        Self::new(metric_name, provider)
    }

    /// Create a metric from a value provider and config.
    #[must_use]
    pub fn from_fn_with_config(
        metric_name: MetricName,
        config: MetricConfig,
        provider: impl Fn() -> MetricValue + Send + Sync + 'static,
    ) -> Self {
        Self::new_with_config(metric_name, config, provider)
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

    /// Return a copy of the metric config.
    #[must_use]
    pub fn metric_config(&self) -> MetricConfig {
        self.config
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Replace the metric config, matching Kafka `KafkaMetric.config(newConfig)`.
    pub fn set_metric_config(&self, config: MetricConfig) {
        *self
            .config
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = config;
    }

    fn quota(&self) -> Option<MetricQuota> {
        self.config
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .quota()
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

/// Sensor recording level.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum SensorRecordingLevel {
    /// Info level.
    #[default]
    Info,
    /// Debug level.
    Debug,
    /// Trace level.
    Trace,
}

impl SensorRecordingLevel {
    const fn should_record(self, configured: Self) -> bool {
        match configured {
            Self::Info => matches!(self, Self::Info),
            Self::Debug => matches!(self, Self::Info | Self::Debug),
            Self::Trace => true,
        }
    }
}

/// Opaque sensor identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SensorId(usize);

/// Metrics registry mirroring Kafka's `org.apache.kafka.common.metrics`.
#[derive(Debug, Default)]
pub struct Metrics {
    registered: BTreeMap<MetricName, KafkaMetric>,
    reporters: Vec<Arc<dyn MetricReporter>>,
    sensors: Vec<Option<SensorState>>,
    sensors_by_name: HashMap<String, SensorId>,
    recording_level: SensorRecordingLevel,
    default_tags: BTreeMap<String, String>,
    closed: bool,
    /// True once any registered metric carries a quota. Lets `record_inner`
    /// skip the per-record quota scan (a `BTreeMap<MetricName>` lookup per stat)
    /// entirely when no quotas are configured — the common producer case.
    any_quota: bool,
}

#[derive(Debug)]
struct SensorState {
    name: String,
    parents: Vec<SensorId>,
    stats: Vec<SensorStat>,
    recording_level: SensorRecordingLevel,
    inactive_expiration_ms: Option<u64>,
    last_record_time_ms: u64,
}

#[derive(Debug, Clone)]
struct SensorStat {
    metric_name: MetricName,
    state: SensorStatState,
}

#[derive(Debug, Clone, Copy)]
enum SensorStatRecordMode {
    Total,
    Value,
    Avg,
    Count,
    Min,
    Max,
    Rate,
    TokenBucket,
}

#[derive(Debug, Clone)]
enum SensorStatState {
    Scalar {
        value: Arc<Mutex<f64>>,
        record_mode: SensorStatRecordMode,
    },
    Avg {
        state: Arc<Mutex<AvgSensorStat>>,
    },
    Extrema {
        state: Arc<Mutex<ExtremaSensorStat>>,
        record_mode: SensorStatRecordMode,
    },
    Rate {
        state: Arc<Mutex<WindowedRateStat>>,
        config: Arc<Mutex<MetricConfig>>,
    },
    TokenBucket {
        state: Arc<Mutex<TokenBucketStat>>,
        config: Arc<Mutex<MetricConfig>>,
    },
    Frequency {
        state: Arc<Mutex<FrequencyStat>>,
        config: Arc<Mutex<MetricConfig>>,
    },
}

#[derive(Debug, Default)]
struct AvgSensorStat {
    total: f64,
    count: f64,
}

#[derive(Debug)]
struct ExtremaSensorStat {
    value: f64,
    count: f64,
}

#[derive(Debug)]
struct WindowedRateStat {
    samples: Vec<WindowedSample>,
    current: usize,
}

#[derive(Debug, Default)]
struct TokenBucketStat {
    tokens: f64,
    last_update_ms: u64,
}

#[derive(Debug)]
struct FrequencyStat {
    samples: Vec<FrequencySample>,
    current: usize,
    spec: FrequencySpec,
}

#[derive(Debug, Clone, Copy)]
struct FrequencySpec {
    center_value: f64,
    min: f64,
    max: f64,
    buckets: usize,
}

#[derive(Debug, Clone, Copy)]
struct WindowedSample {
    value: f64,
    event_count: u64,
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

    fn record(&mut self, config: &MetricConfig, value: f64, time_ms: u64) {
        self.ensure_current_sample(time_ms);
        if self
            .samples
            .get(self.current)
            .is_some_and(|sample| sample.is_complete(config, time_ms))
        {
            self.advance(config, time_ms);
        }
        self.ensure_current_sample(time_ms);
        if let Some(sample) = self.samples.get_mut(self.current) {
            sample.value += value;
            sample.event_count = sample.event_count.saturating_add(1);
            sample.last_event_ms = time_ms;
        }
    }

    fn measure(&mut self, config: &MetricConfig, now_ms: u64) -> f64 {
        self.purge_obsolete_samples(config, now_ms);
        let value = self.samples.iter().map(|sample| sample.value).sum::<f64>();
        let window_size_ms = u32::try_from(self.window_size_ms(config, now_ms)).unwrap_or(u32::MAX);
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

    fn advance(&mut self, config: &MetricConfig, time_ms: u64) {
        let max_samples = config.samples().saturating_add(1);
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

    fn purge_obsolete_samples(&mut self, config: &MetricConfig, now_ms: u64) {
        let expire_age_ms = u64::try_from(config.samples())
            .unwrap_or(u64::MAX)
            .saturating_mul(config.time_window_ms());
        for sample in &mut self.samples {
            if now_ms.saturating_sub(sample.last_event_ms) >= expire_age_ms {
                sample.reset(now_ms);
            }
        }
    }

    fn window_size_ms(&mut self, config: &MetricConfig, now_ms: u64) -> u64 {
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
        let window_ms = config.time_window_ms().max(1);
        let full_windows =
            usize::try_from(total_elapsed_ms.checked_div(window_ms).unwrap_or(0)).unwrap_or(0);
        let min_full_windows = config.samples().saturating_sub(1);
        if full_windows < min_full_windows {
            let missing = min_full_windows.saturating_sub(full_windows);
            let missing_ms = u64::try_from(missing)
                .unwrap_or(u64::MAX)
                .saturating_mul(window_ms);
            total_elapsed_ms = total_elapsed_ms.saturating_add(missing_ms);
        }
        total_elapsed_ms.max(1)
    }
}

impl WindowedSample {
    const fn new(time_ms: u64) -> Self {
        Self {
            value: 0.0,
            event_count: 0,
            start_time_ms: time_ms,
            last_event_ms: time_ms,
        }
    }

    const fn reset(&mut self, time_ms: u64) {
        *self = Self::new(time_ms);
    }

    const fn is_complete(self, config: &MetricConfig, time_ms: u64) -> bool {
        time_ms.saturating_sub(self.start_time_ms) >= config.time_window_ms()
            || self.event_count >= config.event_window()
    }
}

#[derive(Debug, Clone)]
struct FrequencySample {
    counts: Vec<f64>,
    event_count: u64,
    start_time_ms: u64,
    last_event_ms: u64,
}

impl TokenBucketStat {
    fn record(&mut self, config: &MetricConfig, value: f64, time_ms: u64) {
        let Some(quota) = config.quota() else {
            return;
        };
        let burst = Self::burst(config, quota);
        self.refill(quota.bound(), burst, time_ms);
        self.tokens = (self.tokens - value).min(burst);
    }

    fn measure(&mut self, config: &MetricConfig, time_ms: u64) -> f64 {
        let Some(quota) = config.quota() else {
            return f64::MAX;
        };
        let burst = Self::burst(config, quota);
        self.refill(quota.bound(), burst, time_ms);
        self.tokens
    }

    fn refill(&mut self, quota: f64, burst: f64, time_ms: u64) {
        let elapsed_ms = time_ms.saturating_sub(self.last_update_ms);
        self.tokens = quota
            .mul_add(millis_to_seconds(elapsed_ms), self.tokens)
            .min(burst);
        self.last_update_ms = time_ms;
    }

    fn burst(config: &MetricConfig, quota: MetricQuota) -> f64 {
        let samples = u32::try_from(config.samples()).unwrap_or(u32::MAX);
        f64::from(samples) * millis_to_seconds(config.time_window_ms()) * quota.bound()
    }
}

fn millis_to_seconds(time_ms: u64) -> f64 {
    f64::from(u32::try_from(time_ms).unwrap_or(u32::MAX)) / 1000.0
}

impl FrequencyStat {
    fn new(spec: FrequencySpec) -> Result<Self, MetricsError> {
        let FrequencySpec {
            buckets,
            min,
            max,
            center_value,
        } = spec;
        if max < min {
            return Err(MetricsError::InvalidMetricConfig {
                reason: format!("maximum value {max} must be greater than minimum value {min}"),
            });
        }
        if buckets < 1 {
            return Err(MetricsError::InvalidMetricConfig {
                reason: "must be at least 1 bucket".to_owned(),
            });
        }
        if center_value < min || center_value > max {
            return Err(MetricsError::InvalidMetricConfig {
                reason: format!(
                    "frequency center value {center_value} is not within range [{min},{max}]"
                ),
            });
        }
        Ok(Self {
            samples: Vec::new(),
            current: 0,
            spec,
        })
    }

    fn record(&mut self, config: &MetricConfig, value: f64, time_ms: u64) {
        self.ensure_current_sample(time_ms);
        if self
            .samples
            .get(self.current)
            .is_some_and(|sample| sample.is_complete(config, time_ms))
        {
            self.advance(config, time_ms);
        }
        let bin = self.to_bin(value);
        if let Some(sample) = self.samples.get_mut(self.current)
            && let Some(count) = sample.counts.get_mut(bin)
        {
            *count += 1.0;
            sample.event_count = sample.event_count.saturating_add(1);
            sample.last_event_ms = time_ms;
        }
    }

    fn measure(&mut self, config: &MetricConfig, now_ms: u64) -> f64 {
        self.purge_obsolete_samples(config, now_ms);
        let total_count = self
            .samples
            .iter()
            .map(|sample| sample.event_count)
            .sum::<u64>();
        if total_count == 0 {
            return 0.0;
        }
        let bin = self.to_bin(self.spec.center_value);
        let count = self
            .samples
            .iter()
            .filter_map(|sample| sample.counts.get(bin))
            .sum::<f64>();
        count / f64::from(u32::try_from(total_count).unwrap_or(u32::MAX))
    }

    fn ensure_current_sample(&mut self, time_ms: u64) {
        if self.samples.is_empty() {
            self.samples
                .push(FrequencySample::new(self.spec.buckets, time_ms));
        }
        if self.current >= self.samples.len() {
            self.current = self.samples.len().saturating_sub(1);
        }
    }

    fn advance(&mut self, config: &MetricConfig, time_ms: u64) {
        let max_samples = config.samples().saturating_add(1);
        self.current = self
            .current
            .saturating_add(1)
            .checked_rem(max_samples)
            .unwrap_or(0);
        if self.current >= self.samples.len() {
            self.samples
                .push(FrequencySample::new(self.spec.buckets, time_ms));
        } else if let Some(sample) = self.samples.get_mut(self.current) {
            sample.reset(self.spec.buckets, time_ms);
        }
    }

    fn purge_obsolete_samples(&mut self, config: &MetricConfig, now_ms: u64) {
        let expire_age_ms = u64::try_from(config.samples())
            .unwrap_or(u64::MAX)
            .saturating_mul(config.time_window_ms());
        for sample in &mut self.samples {
            if now_ms.saturating_sub(sample.last_event_ms) >= expire_age_ms {
                sample.reset(self.spec.buckets, now_ms);
            }
        }
    }

    fn to_bin(&self, value: f64) -> usize {
        if self.spec.buckets <= 1 || self.spec.max <= self.spec.min {
            return 0;
        }
        let denominator = self.spec.buckets.saturating_sub(1);
        let half_bucket_width = (self.spec.max - self.spec.min)
            / f64::from(u32::try_from(denominator).unwrap_or(u32::MAX))
            / 2.0;
        let min = self.spec.min - half_bucket_width;
        let max = self.spec.max + half_bucket_width;
        let bucket_width =
            (max - min) / f64::from(u32::try_from(self.spec.buckets).unwrap_or(u32::MAX));
        if !bucket_width.is_finite() || value <= min {
            return 0;
        }
        let mut upper = min + bucket_width;
        for bin in 0..self.spec.buckets.saturating_sub(1) {
            if value < upper {
                return bin;
            }
            upper += bucket_width;
        }
        self.spec.buckets.saturating_sub(1)
    }
}

impl FrequencySample {
    fn new(buckets: usize, time_ms: u64) -> Self {
        Self {
            counts: vec![0.0; buckets],
            event_count: 0,
            start_time_ms: time_ms,
            last_event_ms: time_ms,
        }
    }

    fn reset(&mut self, buckets: usize, time_ms: u64) {
        *self = Self::new(buckets, time_ms);
    }

    const fn is_complete(&self, config: &MetricConfig, time_ms: u64) -> bool {
        time_ms.saturating_sub(self.start_time_ms) >= config.time_window_ms()
            || self.event_count >= config.event_window()
    }
}

impl SensorStat {
    fn new(
        metric_name: MetricName,
        record_mode: SensorStatRecordMode,
        config: MetricConfig,
    ) -> (Self, KafkaMetric) {
        match record_mode {
            SensorStatRecordMode::Total
            | SensorStatRecordMode::Value
            | SensorStatRecordMode::Count => {
                let value = Arc::new(Mutex::new(0.0));
                let metric_value = Arc::clone(&value);
                let metric = KafkaMetric::new_with_config(metric_name.clone(), config, move || {
                    let value = metric_value
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    MetricValue::Number(*value)
                });
                (
                    Self {
                        metric_name,
                        state: SensorStatState::Scalar { value, record_mode },
                    },
                    metric,
                )
            },
            SensorStatRecordMode::Avg => {
                let state = Arc::new(Mutex::new(AvgSensorStat::default()));
                let metric_state = Arc::clone(&state);
                let metric = KafkaMetric::new_with_config(metric_name.clone(), config, move || {
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
            SensorStatRecordMode::Min | SensorStatRecordMode::Max => {
                let initial_value = if matches!(record_mode, SensorStatRecordMode::Min) {
                    f64::MAX
                } else {
                    f64::NEG_INFINITY
                };
                let state = Arc::new(Mutex::new(ExtremaSensorStat {
                    value: initial_value,
                    count: 0.0,
                }));
                let metric_state = Arc::clone(&state);
                let metric = KafkaMetric::new_with_config(metric_name.clone(), config, move || {
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
                        state: SensorStatState::Extrema { state, record_mode },
                    },
                    metric,
                )
            },
            SensorStatRecordMode::Rate => Self::new_rate(metric_name, config),
            SensorStatRecordMode::TokenBucket => Self::new_token_bucket(metric_name, config),
        }
    }

    fn new_rate(metric_name: MetricName, config: MetricConfig) -> (Self, KafkaMetric) {
        let state = Arc::new(Mutex::new(WindowedRateStat::new()));
        let metric_state = Arc::clone(&state);
        let config = Arc::new(Mutex::new(config));
        let metric_config = Arc::clone(&config);
        let metric = KafkaMetric::new_with_shared_config(
            metric_name.clone(),
            Arc::clone(&config),
            move |now_ms| {
                let config = metric_config
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clone();
                let mut state = metric_state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                MetricValue::Number(state.measure(&config, now_ms))
            },
        );
        (
            Self {
                metric_name,
                state: SensorStatState::Rate { state, config },
            },
            metric,
        )
    }

    fn new_token_bucket(metric_name: MetricName, config: MetricConfig) -> (Self, KafkaMetric) {
        let state = Arc::new(Mutex::new(TokenBucketStat::default()));
        let metric_state = Arc::clone(&state);
        let config = Arc::new(Mutex::new(config));
        let metric_config = Arc::clone(&config);
        let metric = KafkaMetric::new_with_shared_config(
            metric_name.clone(),
            Arc::clone(&config),
            move |now_ms| {
                let config = metric_config
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clone();
                let mut state = metric_state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                MetricValue::Number(state.measure(&config, now_ms))
            },
        );
        (
            Self {
                metric_name,
                state: SensorStatState::TokenBucket { state, config },
            },
            metric,
        )
    }

    fn new_frequency(
        metric_name: MetricName,
        config: MetricConfig,
        spec: FrequencySpec,
    ) -> Result<(Self, KafkaMetric), MetricsError> {
        let state = Arc::new(Mutex::new(FrequencyStat::new(spec)?));
        let metric_state = Arc::clone(&state);
        let config = Arc::new(Mutex::new(config));
        let metric_config = Arc::clone(&config);
        let metric = KafkaMetric::new_with_shared_config(
            metric_name.clone(),
            Arc::clone(&config),
            move |now_ms| {
                let config = metric_config
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clone();
                let mut state = metric_state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                MetricValue::Number(state.measure(&config, now_ms))
            },
        );
        Ok((
            Self {
                metric_name,
                state: SensorStatState::Frequency { state, config },
            },
            metric,
        ))
    }

    fn record(&self, value: f64, time_ms: u64) {
        match &self.state {
            SensorStatState::Scalar {
                value: current,
                record_mode,
            } => {
                let mut current = current
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                match record_mode {
                    SensorStatRecordMode::Total => *current += value,
                    SensorStatRecordMode::Value => *current = value,
                    SensorStatRecordMode::Count => *current += 1.0,
                    SensorStatRecordMode::Avg
                    | SensorStatRecordMode::Min
                    | SensorStatRecordMode::Max
                    | SensorStatRecordMode::Rate
                    | SensorStatRecordMode::TokenBucket => {},
                }
            },
            SensorStatState::Avg { state } => {
                let mut state = state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                state.total += value;
                state.count += 1.0;
            },
            SensorStatState::Extrema { state, record_mode } => {
                let mut state = state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                match record_mode {
                    SensorStatRecordMode::Min => state.value = state.value.min(value),
                    SensorStatRecordMode::Max => state.value = state.value.max(value),
                    SensorStatRecordMode::Total
                    | SensorStatRecordMode::Value
                    | SensorStatRecordMode::Avg
                    | SensorStatRecordMode::Count
                    | SensorStatRecordMode::Rate
                    | SensorStatRecordMode::TokenBucket => {},
                }
                state.count += 1.0;
            },
            SensorStatState::Rate { state, config } => {
                let config = config
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clone();
                let mut state = state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                state.record(&config, value, time_ms);
            },
            SensorStatState::TokenBucket { state, config } => {
                let config = config
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clone();
                let mut state = state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                state.record(&config, value, time_ms);
            },
            SensorStatState::Frequency { state, config } => {
                let config = config
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clone();
                let mut state = state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                state.record(&config, value, time_ms);
            },
        }
    }

    const fn is_token_bucket(&self) -> bool {
        matches!(self.state, SensorStatState::TokenBucket { .. })
    }
}

impl Metrics {
    /// Create an empty metrics registry.
    #[must_use]
    pub fn new() -> Self {
        Self {
            registered: BTreeMap::new(),
            reporters: Vec::new(),
            sensors: Vec::new(),
            sensors_by_name: HashMap::new(),
            recording_level: SensorRecordingLevel::Info,
            default_tags: BTreeMap::new(),
            closed: false,
            any_quota: false,
        }
    }

    /// Set the registry-wide recording level.
    #[must_use]
    pub const fn with_recording_level(mut self, recording_level: SensorRecordingLevel) -> Self {
        self.recording_level = recording_level;
        self
    }

    /// Add or replace one default tag used by [`Self::metric_name`].
    #[must_use]
    pub fn with_default_tag(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        let _previous = self.default_tags.insert(key.into(), value.into());
        self
    }

    /// Create a metric name.
    #[must_use]
    pub fn metric_name(&self, name: &str, group: &str, description: &str) -> MetricName {
        self.metric_name_with_tags(name, group, description, [])
    }

    /// Create a metric name with explicit tags overriding default tags.
    #[must_use]
    pub fn metric_name_with_tags<'a, I>(
        &self,
        name: &str,
        group: &str,
        description: &str,
        tags: I,
    ) -> MetricName
    where
        I: IntoIterator<Item = (&'a str, &'a str)>,
    {
        let mut metric_name = MetricName::new(name, group).with_description(description);
        for (key, value) in &self.default_tags {
            metric_name = metric_name.tag(key.as_str(), value.as_str());
        }
        for (key, value) in tags {
            metric_name = metric_name.tag(key, value);
        }
        metric_name
    }

    /// Create a metric name from a template and runtime tags.
    ///
    /// # Errors
    ///
    /// Returns an error when default plus runtime tag keys do not exactly
    /// match the template tag keys.
    pub fn metric_instance<'a, I>(
        &self,
        template: &MetricNameTemplate,
        tags: I,
    ) -> Result<MetricName, MetricsError>
    where
        I: IntoIterator<Item = (&'a str, &'a str)>,
    {
        let tags = tags.into_iter().collect::<Vec<_>>();
        let mut runtime_tag_keys = self
            .default_tags
            .keys()
            .map(String::as_str)
            .collect::<BTreeSet<_>>();
        runtime_tag_keys.extend(tags.iter().map(|(key, _value)| *key));
        let template_tag_keys = template.tag_set();
        if runtime_tag_keys != template_tag_keys {
            return Err(MetricsError::InvalidMetricConfig {
                reason: format!(
                    "runtime-defined metric tags do not match template tags for '{}'",
                    template.name()
                ),
            });
        }
        Ok(self.metric_name_with_tags(
            template.name(),
            template.group(),
            template.description(),
            tags,
        ))
    }

    /// Return a sensor by name, creating it when missing.
    pub fn sensor(&mut self, name: impl Into<String>) -> SensorId {
        self.sensor_with_parents(name, SensorRecordingLevel::Info, [])
    }

    /// Return a sensor by name with parents, creating it when missing.
    pub fn sensor_with_parents<I>(
        &mut self,
        name: impl Into<String>,
        recording_level: SensorRecordingLevel,
        parents: I,
    ) -> SensorId
    where
        I: IntoIterator<Item = SensorId>,
    {
        let name = name.into();
        if let Some(sensor) = self.sensors_by_name.get(&name).copied() {
            return sensor;
        }
        let sensor = SensorId(self.sensors.len());
        self.sensors.push(Some(SensorState {
            name: name.clone(),
            parents: parents.into_iter().collect(),
            stats: Vec::new(),
            recording_level,
            inactive_expiration_ms: None,
            last_record_time_ms: current_time_ms(),
        }));
        let _previous = self.sensors_by_name.insert(name, sensor);
        sensor
    }

    /// Return a sensor by name with inactive expiration, creating it when missing.
    pub fn sensor_with_expiration<I>(
        &mut self,
        name: impl Into<String>,
        recording_level: SensorRecordingLevel,
        inactive_expiration: Duration,
        parents: I,
    ) -> SensorId
    where
        I: IntoIterator<Item = SensorId>,
    {
        let name = name.into();
        if let Some(sensor) = self.sensors_by_name.get(&name).copied() {
            return sensor;
        }
        let sensor = SensorId(self.sensors.len());
        self.sensors.push(Some(SensorState {
            name: name.clone(),
            parents: parents.into_iter().collect(),
            stats: Vec::new(),
            recording_level,
            inactive_expiration_ms: u64::try_from(inactive_expiration.as_millis()).ok(),
            last_record_time_ms: 0,
        }));
        let _previous = self.sensors_by_name.insert(name, sensor);
        sensor
    }

    /// Set an existing sensor's recording level.
    ///
    /// # Errors
    ///
    /// Returns an error when `sensor` does not exist.
    pub fn sensor_set_recording_level(
        &mut self,
        sensor: SensorId,
        recording_level: SensorRecordingLevel,
    ) -> Result<(), MetricsError> {
        let state = self.sensor_mut(sensor)?;
        state.recording_level = recording_level;
        Ok(())
    }

    /// Return an existing sensor's name.
    ///
    /// # Errors
    ///
    /// Returns an error when `sensor` does not exist.
    pub fn sensor_name(&self, sensor: SensorId) -> Result<&str, MetricsError> {
        self.sensor_state(sensor).map(|state| state.name.as_str())
    }

    /// Return whether a sensor has registered metrics.
    ///
    /// # Errors
    ///
    /// Returns an error when `sensor` does not exist.
    pub fn sensor_has_metrics(&self, sensor: SensorId) -> Result<bool, MetricsError> {
        self.sensor_state(sensor)
            .map(|state| !state.stats.is_empty())
    }

    /// Return a copy of a sensor's registered metrics.
    ///
    /// # Errors
    ///
    /// Returns an error when `sensor` does not exist.
    pub fn sensor_metrics(&self, sensor: SensorId) -> Result<Vec<KafkaMetric>, MetricsError> {
        self.sensor_state(sensor).map(|state| {
            state
                .stats
                .iter()
                .filter_map(|stat| self.registered.get(&stat.metric_name).cloned())
                .collect()
        })
    }

    /// Return whether a sensor is eligible for removal at `now_ms`.
    ///
    /// # Errors
    ///
    /// Returns an error when `sensor` does not exist.
    pub fn sensor_has_expired_at_ms(
        &self,
        sensor: SensorId,
        now_ms: u64,
    ) -> Result<bool, MetricsError> {
        self.sensor_state(sensor)
            .map(|state| is_sensor_expired(state, now_ms))
    }

    /// Add a total statistic to a sensor.
    ///
    /// # Errors
    ///
    /// Returns an error when the sensor is missing or the metric already exists.
    pub fn sensor_add_total(
        &mut self,
        sensor: SensorId,
        metric_name: MetricName,
    ) -> Result<(), MetricsError> {
        self.sensor_add_total_with_config(sensor, metric_name, MetricConfig::new())
    }

    /// Add a total statistic with a metric config to a sensor.
    ///
    /// # Errors
    ///
    /// Returns an error when the sensor is missing or the metric already exists.
    pub fn sensor_add_total_with_config(
        &mut self,
        sensor: SensorId,
        metric_name: MetricName,
        config: MetricConfig,
    ) -> Result<(), MetricsError> {
        self.sensor_add_stat(sensor, metric_name, SensorStatRecordMode::Total, config)
    }

    /// Add a total statistic with a quota to a sensor.
    ///
    /// # Errors
    ///
    /// Returns an error when the sensor is missing or the metric already exists.
    pub fn sensor_add_total_with_quota(
        &mut self,
        sensor: SensorId,
        metric_name: MetricName,
        quota: MetricQuota,
    ) -> Result<(), MetricsError> {
        self.sensor_add_total_with_config(
            sensor,
            metric_name,
            MetricConfig::new().with_quota(quota),
        )
    }

    /// Add a latest-value statistic to a sensor.
    ///
    /// # Errors
    ///
    /// Returns an error when the sensor is missing or the metric already exists.
    pub fn sensor_add_value(
        &mut self,
        sensor: SensorId,
        metric_name: MetricName,
    ) -> Result<(), MetricsError> {
        self.sensor_add_value_with_config(sensor, metric_name, MetricConfig::new())
    }

    /// Add a latest-value statistic with a metric config to a sensor.
    ///
    /// # Errors
    ///
    /// Returns an error when the sensor is missing or the metric already exists.
    pub fn sensor_add_value_with_config(
        &mut self,
        sensor: SensorId,
        metric_name: MetricName,
        config: MetricConfig,
    ) -> Result<(), MetricsError> {
        self.sensor_add_stat(sensor, metric_name, SensorStatRecordMode::Value, config)
    }

    /// Add a latest-value statistic with a quota to a sensor.
    ///
    /// # Errors
    ///
    /// Returns an error when the sensor is missing or the metric already exists.
    pub fn sensor_add_value_with_quota(
        &mut self,
        sensor: SensorId,
        metric_name: MetricName,
        quota: MetricQuota,
    ) -> Result<(), MetricsError> {
        self.sensor_add_value_with_config(
            sensor,
            metric_name,
            MetricConfig::new().with_quota(quota),
        )
    }

    /// Add an average statistic to a sensor.
    ///
    /// # Errors
    ///
    /// Returns an error when the sensor is missing or the metric already exists.
    pub fn sensor_add_avg(
        &mut self,
        sensor: SensorId,
        metric_name: MetricName,
    ) -> Result<(), MetricsError> {
        self.sensor_add_avg_with_config(sensor, metric_name, MetricConfig::new())
    }

    /// Add an average statistic with a metric config to a sensor.
    ///
    /// # Errors
    ///
    /// Returns an error when the sensor is missing or the metric already exists.
    pub fn sensor_add_avg_with_config(
        &mut self,
        sensor: SensorId,
        metric_name: MetricName,
        config: MetricConfig,
    ) -> Result<(), MetricsError> {
        self.sensor_add_stat(sensor, metric_name, SensorStatRecordMode::Avg, config)
    }

    /// Add an average statistic with a quota to a sensor.
    ///
    /// # Errors
    ///
    /// Returns an error when the sensor is missing or the metric already exists.
    pub fn sensor_add_avg_with_quota(
        &mut self,
        sensor: SensorId,
        metric_name: MetricName,
        quota: MetricQuota,
    ) -> Result<(), MetricsError> {
        self.sensor_add_avg_with_config(sensor, metric_name, MetricConfig::new().with_quota(quota))
    }

    /// Add a cumulative count statistic to a sensor.
    ///
    /// # Errors
    ///
    /// Returns an error when the sensor is missing or the metric already exists.
    pub fn sensor_add_count(
        &mut self,
        sensor: SensorId,
        metric_name: MetricName,
    ) -> Result<(), MetricsError> {
        self.sensor_add_count_with_config(sensor, metric_name, MetricConfig::new())
    }

    /// Add a cumulative count statistic with a metric config to a sensor.
    ///
    /// # Errors
    ///
    /// Returns an error when the sensor is missing or the metric already exists.
    pub fn sensor_add_count_with_config(
        &mut self,
        sensor: SensorId,
        metric_name: MetricName,
        config: MetricConfig,
    ) -> Result<(), MetricsError> {
        self.sensor_add_stat(sensor, metric_name, SensorStatRecordMode::Count, config)
    }

    /// Add a cumulative count statistic with a quota to a sensor.
    ///
    /// # Errors
    ///
    /// Returns an error when the sensor is missing or the metric already exists.
    pub fn sensor_add_count_with_quota(
        &mut self,
        sensor: SensorId,
        metric_name: MetricName,
        quota: MetricQuota,
    ) -> Result<(), MetricsError> {
        self.sensor_add_count_with_config(
            sensor,
            metric_name,
            MetricConfig::new().with_quota(quota),
        )
    }

    /// Add a Kafka `Rate` statistic to a sensor using seconds as the unit.
    ///
    /// # Errors
    ///
    /// Returns an error when the sensor is missing or the metric already exists.
    pub fn sensor_add_rate(
        &mut self,
        sensor: SensorId,
        metric_name: MetricName,
    ) -> Result<(), MetricsError> {
        self.sensor_add_rate_with_config(sensor, metric_name, MetricConfig::new())
    }

    /// Add a Kafka `Rate` statistic with a metric config.
    ///
    /// # Errors
    ///
    /// Returns an error when the sensor is missing or the metric already exists.
    pub fn sensor_add_rate_with_config(
        &mut self,
        sensor: SensorId,
        metric_name: MetricName,
        config: MetricConfig,
    ) -> Result<(), MetricsError> {
        self.sensor_add_stat(sensor, metric_name, SensorStatRecordMode::Rate, config)
    }

    /// Add a Kafka `TokenBucket` statistic to a sensor.
    ///
    /// # Errors
    ///
    /// Returns an error when the sensor is missing or the metric already exists.
    pub fn sensor_add_token_bucket(
        &mut self,
        sensor: SensorId,
        metric_name: MetricName,
    ) -> Result<(), MetricsError> {
        self.sensor_add_token_bucket_with_config(sensor, metric_name, MetricConfig::new())
    }

    /// Add a Kafka `TokenBucket` statistic with a metric config.
    ///
    /// The quota bound is the refill rate in tokens per second. The effective
    /// burst is `samples * time_window * bound`, matching Kafka `TokenBucket`.
    ///
    /// # Errors
    ///
    /// Returns an error when the sensor is missing or the metric already exists.
    pub fn sensor_add_token_bucket_with_config(
        &mut self,
        sensor: SensorId,
        metric_name: MetricName,
        config: MetricConfig,
    ) -> Result<(), MetricsError> {
        self.sensor_add_stat(
            sensor,
            metric_name,
            SensorStatRecordMode::TokenBucket,
            config,
        )
    }

    /// Add Kafka `Frequencies.forBooleanValues(falseMetric, trueMetric)`.
    ///
    /// Pass `None` for either metric name to skip that side. At least one
    /// metric name must be present, matching Kafka's null-name validation.
    ///
    /// # Errors
    ///
    /// Returns an error when the sensor is missing, both metric names are
    /// absent, or a metric already exists globally.
    pub fn sensor_add_boolean_frequencies(
        &mut self,
        sensor: SensorId,
        false_metric_name: Option<MetricName>,
        true_metric_name: Option<MetricName>,
    ) -> Result<(), MetricsError> {
        self.sensor_add_boolean_frequencies_with_config(
            sensor,
            false_metric_name,
            true_metric_name,
            MetricConfig::new(),
        )
    }

    /// Add Kafka `Frequencies.forBooleanValues` with a metric config.
    ///
    /// # Errors
    ///
    /// Returns an error when the sensor is missing, both metric names are
    /// absent, or a metric already exists globally.
    pub fn sensor_add_boolean_frequencies_with_config(
        &mut self,
        sensor: SensorId,
        false_metric_name: Option<MetricName>,
        true_metric_name: Option<MetricName>,
        config: MetricConfig,
    ) -> Result<(), MetricsError> {
        if false_metric_name.is_none() && true_metric_name.is_none() {
            return Err(MetricsError::InvalidMetricConfig {
                reason: "must specify at least one metric name".to_owned(),
            });
        }
        if let Some(metric_name) = false_metric_name {
            self.sensor_add_frequency_with_config(
                sensor,
                metric_name,
                config.clone(),
                FrequencySpec {
                    buckets: 2,
                    min: 0.0,
                    max: 1.0,
                    center_value: 0.0,
                },
            )?;
        }
        if let Some(metric_name) = true_metric_name {
            self.sensor_add_frequency_with_config(
                sensor,
                metric_name,
                config,
                FrequencySpec {
                    buckets: 2,
                    min: 0.0,
                    max: 1.0,
                    center_value: 1.0,
                },
            )?;
        }
        Ok(())
    }

    /// Add a Kafka `Meter` compound statistic: cumulative total plus rate.
    ///
    /// # Errors
    ///
    /// Returns an error when the sensor is missing or either metric already exists.
    pub fn sensor_add_meter(
        &mut self,
        sensor: SensorId,
        rate_metric_name: MetricName,
        total_metric_name: MetricName,
    ) -> Result<(), MetricsError> {
        self.sensor_add_meter_with_config(
            sensor,
            rate_metric_name,
            total_metric_name,
            MetricConfig::new(),
        )
    }

    /// Add a Kafka `Meter` compound statistic with a metric config.
    ///
    /// # Errors
    ///
    /// Returns an error when the sensor is missing or either metric already exists.
    pub fn sensor_add_meter_with_config(
        &mut self,
        sensor: SensorId,
        rate_metric_name: MetricName,
        total_metric_name: MetricName,
        config: MetricConfig,
    ) -> Result<(), MetricsError> {
        self.sensor_add_total_with_config(sensor, total_metric_name, config.clone())?;
        self.sensor_add_rate_with_config(sensor, rate_metric_name, config)
    }

    /// Add a minimum statistic to a sensor.
    ///
    /// # Errors
    ///
    /// Returns an error when the sensor is missing or the metric already exists.
    pub fn sensor_add_min(
        &mut self,
        sensor: SensorId,
        metric_name: MetricName,
    ) -> Result<(), MetricsError> {
        self.sensor_add_min_with_config(sensor, metric_name, MetricConfig::new())
    }

    /// Add a minimum statistic with a metric config to a sensor.
    ///
    /// # Errors
    ///
    /// Returns an error when the sensor is missing or the metric already exists.
    pub fn sensor_add_min_with_config(
        &mut self,
        sensor: SensorId,
        metric_name: MetricName,
        config: MetricConfig,
    ) -> Result<(), MetricsError> {
        self.sensor_add_stat(sensor, metric_name, SensorStatRecordMode::Min, config)
    }

    /// Add a minimum statistic with a quota to a sensor.
    ///
    /// # Errors
    ///
    /// Returns an error when the sensor is missing or the metric already exists.
    pub fn sensor_add_min_with_quota(
        &mut self,
        sensor: SensorId,
        metric_name: MetricName,
        quota: MetricQuota,
    ) -> Result<(), MetricsError> {
        self.sensor_add_min_with_config(sensor, metric_name, MetricConfig::new().with_quota(quota))
    }

    /// Add a maximum statistic to a sensor.
    ///
    /// # Errors
    ///
    /// Returns an error when the sensor is missing or the metric already exists.
    pub fn sensor_add_max(
        &mut self,
        sensor: SensorId,
        metric_name: MetricName,
    ) -> Result<(), MetricsError> {
        self.sensor_add_max_with_config(sensor, metric_name, MetricConfig::new())
    }

    /// Add a maximum statistic with a metric config to a sensor.
    ///
    /// # Errors
    ///
    /// Returns an error when the sensor is missing or the metric already exists.
    pub fn sensor_add_max_with_config(
        &mut self,
        sensor: SensorId,
        metric_name: MetricName,
        config: MetricConfig,
    ) -> Result<(), MetricsError> {
        self.sensor_add_stat(sensor, metric_name, SensorStatRecordMode::Max, config)
    }

    /// Add a maximum statistic with a quota to a sensor.
    ///
    /// # Errors
    ///
    /// Returns an error when the sensor is missing or the metric already exists.
    pub fn sensor_add_max_with_quota(
        &mut self,
        sensor: SensorId,
        metric_name: MetricName,
        quota: MetricQuota,
    ) -> Result<(), MetricsError> {
        self.sensor_add_max_with_config(sensor, metric_name, MetricConfig::new().with_quota(quota))
    }

    fn sensor_add_stat(
        &mut self,
        sensor: SensorId,
        metric_name: MetricName,
        record_mode: SensorStatRecordMode,
        config: MetricConfig,
    ) -> Result<(), MetricsError> {
        if self
            .sensor_state(sensor)?
            .stats
            .iter()
            .any(|stat| stat.metric_name == metric_name)
        {
            return Ok(());
        }
        let (stat, metric) = SensorStat::new(metric_name.clone(), record_mode, config);
        self.add_kafka_metric(metric_name, metric)?;
        self.sensor_mut(sensor)?.stats.push(stat);
        Ok(())
    }

    fn sensor_add_frequency_with_config(
        &mut self,
        sensor: SensorId,
        metric_name: MetricName,
        config: MetricConfig,
        spec: FrequencySpec,
    ) -> Result<(), MetricsError> {
        if self
            .sensor_state(sensor)?
            .stats
            .iter()
            .any(|stat| stat.metric_name == metric_name)
        {
            return Ok(());
        }
        let (stat, metric) = SensorStat::new_frequency(metric_name.clone(), config, spec)?;
        self.add_kafka_metric(metric_name, metric)?;
        self.sensor_mut(sensor)?.stats.push(stat);
        Ok(())
    }

    /// Record a sensor value and propagate it to parent sensors.
    ///
    /// # Errors
    ///
    /// Returns an error when `sensor` does not exist or the record violates a quota.
    pub fn record(&mut self, sensor: SensorId, value: f64) -> Result<(), MetricsError> {
        self.record_at_ms(sensor, value, current_time_ms())
    }

    /// Record a sensor value with explicit quota enforcement, matching Kafka
    /// `Sensor.record(value, timeMs, checkQuotas)`.
    ///
    /// # Errors
    ///
    /// Returns an error when `sensor` does not exist or the record violates a
    /// quota while `check_quotas` is true.
    pub fn record_with_quota_check(
        &mut self,
        sensor: SensorId,
        value: f64,
        check_quotas: bool,
    ) -> Result<(), MetricsError> {
        self.record_with_quota_check_at_ms(sensor, value, current_time_ms(), check_quotas)
    }

    /// Record a sensor value at an explicit millisecond timestamp.
    ///
    /// # Errors
    ///
    /// Returns an error when `sensor` does not exist or the record violates a quota.
    pub fn record_at_ms(
        &mut self,
        sensor: SensorId,
        value: f64,
        time_ms: u64,
    ) -> Result<(), MetricsError> {
        self.record_with_quota_check_at_ms(sensor, value, time_ms, true)
    }

    /// Record a sensor value at an explicit timestamp with explicit quota enforcement.
    ///
    /// # Errors
    ///
    /// Returns an error when `sensor` does not exist or the record violates a
    /// quota while `check_quotas` is true.
    pub fn record_with_quota_check_at_ms(
        &mut self,
        sensor: SensorId,
        value: f64,
        time_ms: u64,
        check_quotas: bool,
    ) -> Result<(), MetricsError> {
        self.record_inner(sensor, value, time_ms, check_quotas)
    }

    /// Record one occurrence, matching Kafka `Sensor.record()`.
    ///
    /// # Errors
    ///
    /// Returns an error when `sensor` does not exist.
    pub fn record_once(&mut self, sensor: SensorId) -> Result<(), MetricsError> {
        self.record(sensor, 1.0)
    }

    /// Check all configured sensor stat quotas against current measured values.
    ///
    /// # Errors
    ///
    /// Returns an error when `sensor` does not exist, a sensor metric is missing,
    /// or the first metric value violates its configured quota.
    pub fn check_sensor_quotas(&self, sensor: SensorId) -> Result<(), MetricsError> {
        self.check_sensor_quotas_at_ms(sensor, current_time_ms())
    }

    /// Check sensor stat quotas at an explicit millisecond timestamp.
    ///
    /// # Errors
    ///
    /// Returns an error when `sensor` does not exist, a sensor metric is missing,
    /// or the first metric value violates its configured quota.
    pub fn check_sensor_quotas_at_ms(
        &self,
        sensor: SensorId,
        time_ms: u64,
    ) -> Result<(), MetricsError> {
        for stat in &self.sensor_state(sensor)?.stats {
            let metric = self
                .registered
                .get(&stat.metric_name)
                .ok_or_else(|| MetricsError::UnknownMetric(stat.metric_name.clone()))?;
            let Some(quota) = metric.quota() else {
                continue;
            };
            let value = metric.metric_value_at_ms(time_ms);
            if stat.is_token_bucket() {
                if value >= 0.0 {
                    continue;
                }
            } else if quota.acceptable(value) {
                continue;
            }
            {
                return Err(MetricsError::QuotaViolation {
                    metric_name: stat.metric_name.clone(),
                    value,
                    bound: quota.bound(),
                });
            }
        }
        Ok(())
    }

    /// Add a standalone metric.
    ///
    /// # Errors
    ///
    /// Returns an error when a metric with the same Kafka identity already exists.
    pub fn add_metric(
        &mut self,
        metric_name: MetricName,
        provider: impl Fn() -> MetricValue + Send + Sync + 'static,
    ) -> Result<(), MetricsError> {
        let metric = KafkaMetric::new(metric_name.clone(), provider);
        self.add_kafka_metric(metric_name, metric)
    }

    fn add_kafka_metric(
        &mut self,
        metric_name: MetricName,
        metric: KafkaMetric,
    ) -> Result<(), MetricsError> {
        if self.registered.contains_key(&metric_name) {
            return Err(MetricsError::DuplicateMetric(metric_name));
        }
        if metric.quota().is_some() {
            self.any_quota = true;
        }
        for reporter in &self.reporters {
            reporter.metric_change(&metric);
        }
        let _previous = self.registered.insert(metric_name, metric);
        Ok(())
    }

    /// Add a standalone metric unless one with the same Kafka identity exists.
    ///
    /// Returns the existing metric when present, matching Kafka `Metrics.addMetricIfAbsent`.
    pub fn add_metric_if_absent(
        &mut self,
        metric_name: MetricName,
        provider: impl Fn() -> MetricValue + Send + Sync + 'static,
    ) -> KafkaMetric {
        if let Some(metric) = self.registered.get(&metric_name) {
            return metric.clone();
        }
        let metric = KafkaMetric::new(metric_name.clone(), provider);
        for reporter in &self.reporters {
            reporter.metric_change(&metric);
        }
        let _previous = self.registered.insert(metric_name, metric.clone());
        metric
    }

    /// Add a metrics reporter and initialize it with current metrics.
    pub fn add_reporter(&mut self, reporter: impl MetricReporter) {
        let reporter = Arc::new(reporter);
        let metrics = self.registered.values().cloned().collect::<Vec<_>>();
        reporter.init(&metrics);
        self.reporters.push(reporter);
    }

    /// Remove a metric.
    ///
    /// # Errors
    ///
    /// Returns an error when the metric does not exist.
    pub fn remove_metric(&mut self, metric_name: &MetricName) -> Result<KafkaMetric, MetricsError> {
        let Some(metric) = self.registered.remove(metric_name) else {
            return Err(MetricsError::UnknownMetric(metric_name.clone()));
        };
        for reporter in &self.reporters {
            reporter.metric_removal(&metric);
        }
        Ok(metric)
    }

    /// Remove a metric when present.
    ///
    /// Returns `None` when the metric does not exist, matching Kafka `Metrics.removeMetric`.
    #[must_use]
    pub fn remove_metric_if_present(&mut self, metric_name: &MetricName) -> Option<KafkaMetric> {
        let metric = self.registered.remove(metric_name)?;
        for reporter in &self.reporters {
            reporter.metric_removal(&metric);
        }
        Some(metric)
    }

    /// Remove a sensor and its child sensors, including their registered metrics.
    ///
    /// Returns `false` when no sensor with `name` exists, matching Kafka's no-op removal.
    pub fn remove_sensor(&mut self, name: &str) -> bool {
        let Some(sensor) = self.sensors_by_name.remove(name) else {
            return false;
        };
        self.remove_sensor_by_id(sensor)
    }

    /// Remove expired sensors and their children, matching Kafka's expire sensor task.
    ///
    /// Returns the number of sensors removed, including children removed with an
    /// expired parent.
    pub fn expire_sensors_at_ms(&mut self, now_ms: u64) -> usize {
        let expired = self
            .sensors
            .iter()
            .enumerate()
            .filter_map(|(index, state)| {
                state
                    .as_ref()
                    .and_then(|state| is_sensor_expired(state, now_ms).then_some(SensorId(index)))
            })
            .collect::<Vec<_>>();
        let before = self.sensors.iter().flatten().count();
        for sensor in expired {
            let _removed = self.remove_sensor_by_id(sensor);
        }
        let after = self.sensors.iter().flatten().count();
        before.saturating_sub(after)
    }

    /// Return a registered metric.
    #[must_use]
    pub fn metric(&self, metric_name: &MetricName) -> Option<&KafkaMetric> {
        self.registered.get(metric_name)
    }

    /// Iterate every registered metric and its current value handle.
    pub fn registered_metrics(&self) -> impl Iterator<Item = (&MetricName, &KafkaMetric)> {
        self.registered.iter()
    }

    /// Close all reporters once.
    pub fn close(&mut self) {
        if self.closed {
            return;
        }
        self.closed = true;
        for reporter in &self.reporters {
            reporter.close();
        }
    }

    fn record_inner(
        &mut self,
        sensor: SensorId,
        value: f64,
        time_ms: u64,
        check_quotas: bool,
    ) -> Result<(), MetricsError> {
        let should_record = self
            .sensor_state(sensor)?
            .recording_level
            .should_record(self.recording_level);
        if !should_record {
            return Ok(());
        }
        let parents = {
            let state = self.sensor_mut(sensor)?;
            state.last_record_time_ms = time_ms;
            for stat in &state.stats {
                stat.record(value, time_ms);
            }
            state.parents.clone()
        };
        if check_quotas && self.any_quota {
            self.check_sensor_quotas_at_ms(sensor, time_ms)?;
        }
        for parent in parents {
            self.record_inner(parent, value, time_ms, check_quotas)?;
        }
        Ok(())
    }

    fn sensor_state(&self, sensor: SensorId) -> Result<&SensorState, MetricsError> {
        self.sensors
            .get(sensor.0)
            .and_then(Option::as_ref)
            .ok_or(MetricsError::UnknownSensor { sensor })
    }

    fn sensor_mut(&mut self, sensor: SensorId) -> Result<&mut SensorState, MetricsError> {
        self.sensors
            .get_mut(sensor.0)
            .and_then(Option::as_mut)
            .ok_or(MetricsError::UnknownSensor { sensor })
    }

    fn remove_sensor_by_id(&mut self, sensor: SensorId) -> bool {
        let child_sensors = self.child_sensors(sensor);
        let Some(state) = self.sensors.get_mut(sensor.0).and_then(Option::take) else {
            return false;
        };
        let _removed = self.sensors_by_name.remove(&state.name);
        for stat in state.stats {
            let _removed = self.remove_metric(&stat.metric_name);
        }
        for child in child_sensors {
            let _removed = self.remove_sensor_by_id(child);
        }
        self.remove_parent_link(sensor);
        true
    }

    fn child_sensors(&self, parent: SensorId) -> Vec<SensorId> {
        self.sensors
            .iter()
            .enumerate()
            .filter_map(|(index, state)| {
                state
                    .as_ref()
                    .and_then(|state| state.parents.contains(&parent).then_some(SensorId(index)))
            })
            .collect()
    }

    fn remove_parent_link(&mut self, removed: SensorId) {
        for state in &mut self.sensors {
            let Some(state) = state else {
                continue;
            };
            state.parents.retain(|parent| *parent != removed);
        }
    }
}

impl Drop for Metrics {
    fn drop(&mut self) {
        self.close();
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
pub enum MetricsError {
    /// Metric already exists.
    DuplicateMetric(MetricName),
    /// Metric was not found.
    UnknownMetric(MetricName),
    /// Sensor was not found.
    UnknownSensor {
        /// Missing sensor id.
        sensor: SensorId,
    },
    /// Metric value is outside its configured quota.
    QuotaViolation {
        /// Metric that violated its quota.
        metric_name: MetricName,
        /// Measured value.
        value: f64,
        /// Configured quota bound.
        bound: f64,
    },
    /// Metric configuration is invalid.
    InvalidMetricConfig {
        /// Human-readable validation reason.
        reason: String,
    },
}

impl fmt::Display for MetricsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateMetric(metric) => {
                write!(f, "metric already exists: {}", metric.name())
            },
            Self::UnknownMetric(metric) => write!(f, "unknown metric: {}", metric.name()),
            Self::UnknownSensor { sensor } => write!(f, "unknown sensor: {}", sensor.0),
            Self::QuotaViolation {
                metric_name,
                value,
                bound,
            } => write!(
                f,
                "metric '{}' violated quota: actual {}, bound {}",
                metric_name.name(),
                value,
                bound
            ),
            Self::InvalidMetricConfig { reason } => write!(f, "invalid metric config: {reason}"),
        }
    }
}

impl std::error::Error for MetricsError {}

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
