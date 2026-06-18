//! Public producer facade built from accumulator and dispatcher components.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc, RwLock,
        atomic::{AtomicBool, Ordering},
    },
};

use ahash::AHashSet;
use bytes::Bytes;
use kacrab_protocol::{
    KafkaUuid,
    generated::{
        ApiKey, ErrorCode, GetTelemetrySubscriptionsRequestData,
        GetTelemetrySubscriptionsResponseData, PushTelemetryRequestData, PushTelemetryResponseData,
    },
    version::client_api_info,
};
use tokio::task::{JoinError, JoinSet};

use super::{
    accumulator::{
        AppendStatus, RECORD_BATCH_OVERHEAD_BYTES, RecordAccumulator, estimate_record_batch_bytes,
    },
    api::{
        ConsumerGroupMetadata, OffsetAndMetadata, ProducerMetricSubscription,
        ProducerPartitionInfo, TopicPartition,
    },
    config::ProducerRuntimeConfig,
    dispatcher::{DispatchOutcome, ProducerDispatcher},
    error::{ProducerError, Result},
    interceptor::{ProducerInterceptor, ProducerInterceptors},
    metrics::{
        KafkaMetric, MetricName, MetricReporter, ProducerMetricValue, ProducerMetrics,
        ProducerMetricsSnapshot,
    },
    partitioner::{ProducerPartitioner, ProducerPartitionerHandle},
    record::{BatchSendFuture, ProducerRecord, RecordMetadata, SendFuture},
    serializer::{ConfiguredProducerSerializer, ProducerSerializer, TypedProducer},
};
use crate::{
    config::{ClientConfig, ConfigKey, ConfigValue, ProducerConfig, Properties},
    wire::{
        BrokerEndpoint, ClusterMetadata, SaslClientAuthenticator, SaslClientAuthenticatorFactory,
        SaslClientAuthenticatorFactoryHandle, SaslClientAuthenticatorHandle, WireClient, WireError,
    },
};

/// Batched Kafka producer facade.
#[derive(Debug)]
pub struct Producer {
    accumulator: RecordAccumulator,
    dispatcher: ProducerDispatcher,
    in_flight: JoinSet<TimedDispatchOutcome>,
    in_flight_partitions: AHashSet<InFlightPartitionKey>,
    max_in_flight_requests: usize,
    idempotent_ordering: bool,
    max_block: std::time::Duration,
    max_request_size: usize,
    accumulator_batch_size: usize,
    metrics: ProducerMetrics,
    metrics_enabled: bool,
    dispatch_latency_samples: Option<Vec<std::time::Duration>>,
    client_instance_id: RwLock<KafkaUuid>,
    telemetry_subscription: RwLock<Option<TelemetrySubscription>>,
    enable_metrics_push: bool,
    telemetry_disabled: AtomicBool,
    metric_subscriptions: BTreeSet<String>,
    application_metrics: BTreeMap<MetricName, KafkaMetric>,
    interceptors: ProducerInterceptors,
    partitioner: ProducerPartitionerHandle,
    metric_reporters: Vec<Arc<dyn MetricReporter>>,
}

#[derive(Debug, Clone)]
struct TelemetrySubscription {
    client_instance_id: KafkaUuid,
    subscription_id: i32,
    accepted_compression_types: Vec<i8>,
    telemetry_max_bytes: i32,
}

#[derive(Debug)]
struct TimedDispatchOutcome {
    outcome: DispatchOutcome,
    latency: std::time::Duration,
    partitions: Vec<InFlightPartitionKey>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct InFlightPartitionKey {
    topic: String,
    partition: i32,
}

#[derive(Debug)]
struct DispatchSelection {
    dispatchable: Vec<super::ReadyBatch>,
    deferred: Vec<super::ReadyBatch>,
    partitions: Vec<InFlightPartitionKey>,
}

#[derive(Debug)]
struct AppendPollBudget {
    ready_batches: usize,
    threshold: usize,
}

#[derive(Debug)]
enum StickyTopicSelection {
    Single(Arc<str>),
    PerRecord(Vec<Option<Arc<str>>>),
}

#[derive(Debug)]
struct BatchTopicPlan {
    topics: Vec<Arc<str>>,
    sticky_topics: StickyTopicSelection,
}

const DENSE_READY_BATCH_RECORDS: usize = 32;

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

impl StickyTopicSelection {
    fn topic_for(&self, index: usize) -> Option<&str> {
        match self {
            Self::Single(topic) => Some(topic.as_ref()),
            Self::PerRecord(topics) => topics.get(index).and_then(Option::as_deref),
        }
    }
}

impl Producer {
    /// Build a producer from an existing wire client and runtime config.
    #[must_use]
    pub fn from_parts(wire: WireClient, config: ProducerRuntimeConfig) -> Self {
        let max_in_flight_requests = config.max_in_flight_requests_per_connection.max(1);
        let idempotent_ordering = max_in_flight_requests == 1;
        let max_block = config.max_block;
        let max_request_size = config.max_request_size;
        let enable_metrics_push = config.enable_metrics_push;
        let accumulator_config = config.accumulator;
        let accumulator_batch_size = accumulator_config.batch_size;
        let dispatcher = ProducerDispatcher::with_config(wire, config);
        let metrics = dispatcher.metrics_handle();
        Self {
            accumulator: RecordAccumulator::new(accumulator_config),
            dispatcher,
            in_flight: JoinSet::new(),
            in_flight_partitions: AHashSet::new(),
            max_in_flight_requests,
            idempotent_ordering,
            max_block,
            max_request_size,
            accumulator_batch_size,
            metrics,
            metrics_enabled: false,
            dispatch_latency_samples: None,
            client_instance_id: RwLock::new(KafkaUuid::ZERO),
            telemetry_subscription: RwLock::new(None),
            enable_metrics_push,
            telemetry_disabled: AtomicBool::new(false),
            metric_subscriptions: BTreeSet::new(),
            application_metrics: BTreeMap::new(),
            interceptors: ProducerInterceptors::default(),
            partitioner: ProducerPartitionerHandle::default(),
            metric_reporters: Vec::new(),
        }
    }

    /// Creates a Java-style producer builder.
    #[must_use]
    pub fn builder() -> ProducerBuilder {
        ProducerBuilder::new()
    }

    /// Build a producer from an ergonomic Java-style client config.
    ///
    /// # Errors
    ///
    /// Returns an error when config validation, DNS resolution, or connection
    /// setup preparation fails.
    pub async fn new(config: ClientConfig) -> Result<Self> {
        Self::from_client_config(&config).await
    }

    /// Build a producer from borrowed Java-style client config.
    ///
    /// # Errors
    ///
    /// Returns an error when config validation, DNS resolution, or connection
    /// setup preparation fails.
    pub async fn from_client_config(config: &ClientConfig) -> Result<Self> {
        let config = client_config_without_byte_array_serializer_class_configs(config);
        let config = client_config_without_empty_interceptor_class_configs(&config);
        let config = config
            .producer_config()
            .map_err(|error| ProducerError::Config { error })?;
        Self::from_config(config).await
    }

    async fn from_client_config_with_native_serializers(config: &ClientConfig) -> Result<Self> {
        let config = client_config_without_serializer_class_configs(config);
        Self::from_client_config(&config).await
    }

    async fn from_client_config_with_configured_serializers<K, V, KS, VS>(
        config: &ClientConfig,
    ) -> Result<TypedProducer<K, V, KS, VS>>
    where
        K: Sync,
        V: Sync,
        KS: ConfiguredProducerSerializer<K>,
        VS: ConfiguredProducerSerializer<V>,
    {
        validate_configured_serializer_class::<K, KS>(config, "key.serializer")?;
        validate_configured_serializer_class::<V, VS>(config, "value.serializer")?;
        let key_serializer = KS::from_client_config(config, true)?;
        let value_serializer = VS::from_client_config(config, false)?;
        let producer = Self::from_client_config_with_native_serializers(config).await?;
        Ok(Self::from_parts_with_serializers(
            producer,
            key_serializer,
            value_serializer,
        ))
    }

    /// Build a producer from Java-style `Properties`.
    ///
    /// # Errors
    ///
    /// Returns an error when config validation, DNS resolution, or connection
    /// setup preparation fails.
    pub async fn from_properties(properties: Properties) -> Result<Self> {
        let config = ClientConfig::from(properties);
        Self::from_client_config(&config).await
    }

    /// Build a producer from a Java-style map/iterator of config entries.
    ///
    /// # Errors
    ///
    /// Returns an error when config validation, DNS resolution, or connection
    /// setup preparation fails.
    pub async fn from_map<I, K, V>(entries: I) -> Result<Self>
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<ConfigKey>,
        V: Into<ConfigValue>,
    {
        let config: ClientConfig = entries.into_iter().collect();
        Self::from_client_config(&config).await
    }

    /// Java-style constructor shape that accepts key/value serializers.
    ///
    /// # Errors
    ///
    /// Returns an error when config validation, DNS resolution, or connection
    /// setup preparation fails.
    pub async fn from_properties_with_serializers<K, V, KS, VS>(
        properties: Properties,
        key_serializer: KS,
        value_serializer: VS,
    ) -> Result<TypedProducer<K, V, KS, VS>>
    where
        K: Sync,
        V: Sync,
        KS: ProducerSerializer<K>,
        VS: ProducerSerializer<V>,
    {
        let config = ClientConfig::from(properties);
        let producer = Self::from_client_config_with_native_serializers(&config).await?;
        Ok(Self::from_parts_with_serializers(
            producer,
            key_serializer,
            value_serializer,
        ))
    }

    /// Java-style map constructor shape that accepts key/value serializers.
    ///
    /// # Errors
    ///
    /// Returns an error when config validation, DNS resolution, or connection
    /// setup preparation fails.
    pub async fn from_map_with_serializers<I, CK, CV, K, V, KS, VS>(
        entries: I,
        key_serializer: KS,
        value_serializer: VS,
    ) -> Result<TypedProducer<K, V, KS, VS>>
    where
        I: IntoIterator<Item = (CK, CV)>,
        CK: Into<ConfigKey>,
        CV: Into<ConfigValue>,
        K: Sync,
        V: Sync,
        KS: ProducerSerializer<K>,
        VS: ProducerSerializer<V>,
    {
        let config: ClientConfig = entries.into_iter().collect();
        let producer = Self::from_client_config_with_native_serializers(&config).await?;
        Ok(Self::from_parts_with_serializers(
            producer,
            key_serializer,
            value_serializer,
        ))
    }

    /// Java-style map constructor that loads built-in native serializers from
    /// `key.serializer` and `value.serializer` class names.
    ///
    /// # Errors
    ///
    /// Returns an error when configured serializer class names are missing or
    /// do not match the requested native serializers.
    pub async fn from_map_with_configured_serializers<I, CK, CV, K, V, KS, VS>(
        entries: I,
    ) -> Result<TypedProducer<K, V, KS, VS>>
    where
        I: IntoIterator<Item = (CK, CV)>,
        CK: Into<ConfigKey>,
        CV: Into<ConfigValue>,
        K: Sync,
        V: Sync,
        KS: ConfiguredProducerSerializer<K>,
        VS: ConfiguredProducerSerializer<V>,
    {
        let config: ClientConfig = entries.into_iter().collect();
        Self::from_client_config_with_configured_serializers(&config).await
    }

    /// Java-style properties constructor that loads built-in native serializers
    /// from `key.serializer` and `value.serializer` class names.
    ///
    /// # Errors
    ///
    /// Returns an error when configured serializer class names are missing or
    /// do not match the requested native serializers.
    pub async fn from_properties_with_configured_serializers<K, V, KS, VS>(
        properties: Properties,
    ) -> Result<TypedProducer<K, V, KS, VS>>
    where
        K: Sync,
        V: Sync,
        KS: ConfiguredProducerSerializer<K>,
        VS: ConfiguredProducerSerializer<V>,
    {
        let config = ClientConfig::from(properties);
        Self::from_client_config_with_configured_serializers(&config).await
    }

    /// Wrap an existing byte-oriented producer with key/value serializers.
    #[must_use]
    pub const fn from_parts_with_serializers<K, V, KS, VS>(
        producer: Self,
        key_serializer: KS,
        value_serializer: VS,
    ) -> TypedProducer<K, V, KS, VS>
    where
        K: Sync,
        V: Sync,
        KS: ProducerSerializer<K>,
        VS: ProducerSerializer<V>,
    {
        TypedProducer::from_parts(producer, key_serializer, value_serializer)
    }

    /// Resolve bootstrap servers and build a producer from public typed config.
    ///
    /// # Errors
    ///
    /// Returns an error when runtime config validation fails, bootstrap DNS
    /// resolution fails, or no bootstrap endpoint resolves to a socket address.
    pub async fn from_config(config: ProducerConfig) -> Result<Self> {
        let runtime = config.to_producer_runtime_config()?;
        let endpoints = resolve_bootstrap_brokers(&config).await?;
        let wire = WireClient::connect_with_brokers(
            config.to_connection_config(),
            config.client_id,
            endpoints,
        );
        Ok(Self::from_parts(wire, runtime))
    }

    /// Append one record, then dispatch any batches that are already ready.
    ///
    /// # Errors
    ///
    /// Returns producer backpressure or errors from pumping ready batches.
    pub async fn send(&mut self, record: ProducerRecord) -> Result<SendFuture> {
        self.dispatcher.fail_if_transaction_error().await?;
        let now = std::time::Instant::now();
        let mut record = self.intercept_on_send(record);
        let mut error_record = record.clone();
        let result = async {
            let sticky_topic = self
                .uses_sticky_partitioner(&record)
                .then(|| record.topic.to_string());
            if !record.has_assigned_partition() {
                let metadata_started_at = self.metrics_enabled.then(std::time::Instant::now);
                self.assign_partition(&mut record).await?;
                if let Some(metadata_started_at) = metadata_started_at {
                    self.metrics
                        .record_metadata_wait(metadata_started_at.elapsed());
                }
            }
            error_record = record.clone();
            let ack_headers = self.interceptor_headers(&record);
            let (delivery, status) = self.append_for_delivery_with_max_block(record, now).await?;
            self.register_interceptor_ack(&delivery, ack_headers);
            Ok((delivery, status, sticky_topic))
        }
        .await;
        let (delivery, status, sticky_topic) = match result {
            Ok(result) => result,
            Err(error) => {
                self.interceptors.on_error(&error_record, &error);
                return Err(error);
            },
        };
        if status.batch_ready {
            if let Some(topic) = sticky_topic.as_deref() {
                self.dispatcher.mark_sticky_batch_ready(topic).await;
            }
            self.poll().await?;
        }
        self.collect_finished()?;
        Ok(delivery)
    }

    /// Append one record with a Java-style completion callback.
    ///
    /// The returned delivery handle can still be awaited by the caller, matching
    /// Java producer's `send(record, callback)` shape where a future is returned
    /// even when a callback is supplied.
    ///
    /// # Errors
    ///
    /// Returns producer backpressure or errors from pumping ready batches.
    pub async fn send_with_callback<F>(
        &mut self,
        record: ProducerRecord,
        callback: F,
    ) -> Result<SendFuture>
    where
        F: FnOnce(Result<RecordMetadata>) + Send + 'static,
    {
        self.dispatcher.fail_if_transaction_error().await?;
        let now = std::time::Instant::now();
        let mut record = self.intercept_on_send(record);
        let mut error_record = record.clone();
        let mut callback = Some(callback);
        let result = async {
            let sticky_topic = self
                .uses_sticky_partitioner(&record)
                .then(|| record.topic.to_string());
            if !record.has_assigned_partition() {
                let metadata_started_at = self.metrics_enabled.then(std::time::Instant::now);
                self.assign_partition(&mut record).await?;
                if let Some(metadata_started_at) = metadata_started_at {
                    self.metrics
                        .record_metadata_wait(metadata_started_at.elapsed());
                }
            }
            error_record = record.clone();
            let ack_headers = self.interceptor_headers(&record);
            let (delivery, status) = self.append_for_delivery_with_max_block(record, now).await?;
            self.register_interceptor_ack(&delivery, ack_headers);
            if let Some(callback) = callback.take() {
                delivery.register_callback(Box::new(callback));
            }
            Ok((delivery, status, sticky_topic))
        }
        .await;
        let (delivery, status, sticky_topic) = match result {
            Ok(result) => result,
            Err(error) => {
                if let (Some(callback_error), Some(callback)) =
                    (producer_error_for_callback(&error), callback.take())
                {
                    callback(Err(callback_error));
                }
                self.interceptors.on_error(&error_record, &error);
                return Err(error);
            },
        };
        if status.batch_ready {
            if let Some(topic) = sticky_topic.as_deref() {
                self.dispatcher.mark_sticky_batch_ready(topic).await;
            }
            self.poll().await?;
        }
        self.collect_finished()?;
        Ok(delivery)
    }

    /// Append one record with a Java `Callback.onCompletion(metadata, exception)` shape.
    ///
    /// # Errors
    ///
    /// Returns producer backpressure or errors from pumping ready batches.
    pub async fn send_with_java_callback<C>(
        &mut self,
        record: ProducerRecord,
        callback: C,
    ) -> Result<SendFuture>
    where
        C: super::record::Callback,
    {
        self.send_with_callback(record, super::record::java_delivery_callback(callback))
            .await
    }

    /// Append multiple records, then dispatch any batches that are already ready.
    ///
    /// # Errors
    ///
    /// Returns producer backpressure or errors from pumping ready batches.
    pub async fn send_batch<I>(&mut self, records: I) -> Result<BatchSendFuture>
    where
        I: IntoIterator<Item = ProducerRecord>,
    {
        self.dispatcher.fail_if_transaction_error().await?;
        let now = std::time::Instant::now();
        let mut records = records.into_iter();
        let (lower_bound, _upper_bound) = records.size_hint();
        let batch_delivery_metadata_capacity = lower_bound.saturating_add(1);
        let deadline = self.append_deadline();
        let mut deliveries = Vec::with_capacity(batch_delivery_metadata_capacity);
        let mut poll_budget = AppendPollBudget::new(self.max_in_flight_requests);
        while let Some(record) = records.next() {
            if record.has_assigned_partition() {
                let (delivery, status) = self
                    .append_for_batch_delivery_until(
                        record,
                        now,
                        deadline,
                        batch_delivery_metadata_capacity,
                    )
                    .await?;
                if let Some(delivery) = delivery {
                    deliveries.push(delivery);
                }
                if poll_budget.observe(status) {
                    self.poll().await?;
                }
                continue;
            }

            let mut pending = Vec::with_capacity(lower_bound.saturating_add(1));
            pending.push(record);
            pending.extend(records);
            let topic_plan = self.topic_plan_for_batch(&pending);
            let metadata_started_at = std::time::Instant::now();
            let metadata = self
                .dispatcher
                .metadata_for_topics(topic_plan.topics.iter().map(Arc::as_ref))
                .await?;
            self.assign_default_partitions_for_batch(&topic_plan, &metadata, &mut pending)
                .await?;
            self.metrics
                .record_metadata_wait(metadata_started_at.elapsed());
            for (record_index, mut record) in pending.into_iter().enumerate() {
                if self.partitioner.is_some() {
                    self.assign_partition_with_metadata(&metadata, &mut record)
                        .await?;
                }
                let (delivery, status) = self
                    .append_for_batch_delivery_until(
                        record,
                        now,
                        deadline,
                        batch_delivery_metadata_capacity,
                    )
                    .await?;
                if let Some(delivery) = delivery {
                    deliveries.push(delivery);
                }
                if status.batch_ready
                    && let Some(topic) = topic_plan.sticky_topics.topic_for(record_index)
                {
                    self.dispatcher.mark_sticky_batch_ready(topic).await;
                }
                if poll_budget.observe(status) {
                    self.poll().await?;
                }
            }
            break;
        }
        self.poll().await.map(|()| BatchSendFuture::new(deliveries))
    }

    /// Append multiple records with one callback registered for each record.
    ///
    /// This keeps Java-style tracked delivery semantics while amortizing
    /// metadata lookup, partition assignment, and sender polling across the
    /// supplied records.
    ///
    /// # Errors
    ///
    /// Returns producer backpressure or errors from pumping ready batches.
    pub async fn send_batch_with_callback<I, F>(&mut self, records: I, callback: F) -> Result<()>
    where
        I: IntoIterator<Item = ProducerRecord>,
        F: Fn(Result<RecordMetadata>) + Send + Sync + 'static,
    {
        self.dispatcher.fail_if_transaction_error().await?;
        let now = std::time::Instant::now();
        let mut records = records.into_iter();
        let (lower_bound, _upper_bound) = records.size_hint();
        let batch_delivery_metadata_capacity = lower_bound.saturating_add(1);
        let deadline = self.append_deadline();
        let mut poll_budget = AppendPollBudget::new(self.max_in_flight_requests);
        let callback: super::record::SharedDeliveryCallback = Arc::new(callback);
        while let Some(record) = records.next() {
            if record.has_assigned_partition() {
                let (delivery, status) = self
                    .append_for_batch_delivery_until(
                        record,
                        now,
                        deadline,
                        batch_delivery_metadata_capacity,
                    )
                    .await?;
                if let Some(delivery) = delivery {
                    delivery.register_batch_callback(Arc::clone(&callback));
                }
                if poll_budget.observe(status) {
                    self.poll().await?;
                }
                continue;
            }

            let mut pending = Vec::with_capacity(lower_bound.saturating_add(1));
            pending.push(record);
            pending.extend(records);
            let topic_plan = self.topic_plan_for_batch(&pending);
            let metadata_started_at = std::time::Instant::now();
            let metadata = self
                .dispatcher
                .metadata_for_topics(topic_plan.topics.iter().map(Arc::as_ref))
                .await?;
            self.assign_default_partitions_for_batch(&topic_plan, &metadata, &mut pending)
                .await?;
            self.metrics
                .record_metadata_wait(metadata_started_at.elapsed());
            for (record_index, mut record) in pending.into_iter().enumerate() {
                if self.partitioner.is_some() {
                    self.assign_partition_with_metadata(&metadata, &mut record)
                        .await?;
                }
                let (delivery, status) = self
                    .append_for_batch_delivery_until(
                        record,
                        now,
                        deadline,
                        batch_delivery_metadata_capacity,
                    )
                    .await?;
                if let Some(delivery) = delivery {
                    delivery.register_batch_callback(Arc::clone(&callback));
                }
                if status.batch_ready
                    && let Some(topic) = topic_plan.sticky_topics.topic_for(record_index)
                {
                    self.dispatcher.mark_sticky_batch_ready(topic).await;
                }
                if poll_budget.observe(status) {
                    self.poll().await?;
                }
            }
            break;
        }
        self.poll().await
    }

    /// Append multiple records without creating per-record delivery handles.
    ///
    /// Use [`Self::flush`] or [`Self::close`] to wait for broker acknowledgement
    /// of all outstanding untracked records.
    ///
    /// # Errors
    ///
    /// Returns producer backpressure or errors from pumping ready batches.
    pub async fn send_batch_untracked<I>(&mut self, records: I) -> Result<()>
    where
        I: IntoIterator<Item = ProducerRecord>,
    {
        self.dispatcher.fail_if_transaction_error().await?;
        let now = std::time::Instant::now();
        let mut records = records.into_iter();
        let (lower_bound, _upper_bound) = records.size_hint();
        let mut poll_budget = AppendPollBudget::new(self.max_in_flight_requests);
        while let Some(record) = records.next() {
            if record.has_assigned_partition() {
                let status = self.append_untracked_with_max_block(record, now).await?;
                if poll_budget.observe(status) {
                    self.poll().await?;
                }
                continue;
            }

            let mut pending = Vec::with_capacity(lower_bound.saturating_add(1));
            pending.push(record);
            pending.extend(records);
            let topic_plan = self.topic_plan_for_batch(&pending);
            let metadata_started_at = std::time::Instant::now();
            let metadata = self
                .dispatcher
                .metadata_for_topics(topic_plan.topics.iter().map(Arc::as_ref))
                .await?;
            if !self.partitioner.is_some() {
                if let [topic] = topic_plan.topics.as_slice() {
                    self.dispatcher
                        .refresh_topic_load_stats_with_metadata(
                            &self.accumulator,
                            &metadata,
                            topic.as_ref(),
                        )
                        .await?;
                    self.dispatcher
                        .assign_topic_partitions_with_metadata(
                            &metadata,
                            topic.as_ref(),
                            &mut pending,
                        )
                        .await?;
                } else {
                    self.dispatcher
                        .refresh_partition_load_stats_with_metadata(
                            &self.accumulator,
                            &metadata,
                            topic_plan.topics.iter().map(Arc::as_ref),
                        )
                        .await?;
                    self.dispatcher
                        .assign_partitions_with_metadata(&metadata, &mut pending)
                        .await?;
                }
            }
            self.metrics
                .record_metadata_wait(metadata_started_at.elapsed());
            for (record_index, mut record) in pending.into_iter().enumerate() {
                if self.partitioner.is_some() {
                    self.assign_partition_with_metadata(&metadata, &mut record)
                        .await?;
                }
                let status = self.append_untracked_with_max_block(record, now).await?;
                if status.batch_ready
                    && let Some(topic) = topic_plan.sticky_topics.topic_for(record_index)
                {
                    self.dispatcher.mark_sticky_batch_ready(topic).await;
                }
                if poll_budget.observe(status) {
                    self.poll().await?;
                }
            }
            break;
        }
        self.poll().await
    }

    /// Dispatch batches that are ready by size or linger.
    ///
    /// # Errors
    ///
    /// Returns routing, encoding, broker, timeout, or lower-level wire errors.
    pub async fn poll(&mut self) -> Result<()> {
        self.collect_finished()?;
        let now = std::time::Instant::now();
        let batches = self.accumulator.drain_ready(now);
        if !batches.is_empty() {
            while self.in_flight.len() >= self.max_in_flight_requests {
                self.wait_for_one().await?;
            }
            let mut selection = self.select_dispatchable_batches(batches);
            if let Err(error) = self
                .dispatcher
                .prepare_drained_batches(&mut selection.dispatchable)
                .await
            {
                selection.dispatchable.extend(selection.deferred);
                self.accumulator.requeue_front(selection.dispatchable);
                return Err(error);
            }
            if !selection.deferred.is_empty() {
                self.accumulator.requeue_front(selection.deferred);
            }
            if !selection.dispatchable.is_empty() {
                self.spawn_dispatch(selection.dispatchable, now, selection.partitions);
            }
        }
        Ok(())
    }

    /// Force-dispatch every buffered batch regardless of linger or batch size.
    ///
    /// # Errors
    ///
    /// Returns an error when a buffered batch cannot be routed or delivered.
    pub async fn flush(&mut self) -> Result<()> {
        if super::record::in_delivery_callback() {
            return Err(ProducerError::CallbackOperation { operation: "flush" });
        }
        let started_at = std::time::Instant::now();
        let result = self.flush_inner().await;
        if result.is_ok() {
            self.metrics.record_flush(started_at.elapsed());
        }
        result
    }

    async fn flush_inner(&mut self) -> Result<()> {
        loop {
            self.collect_finished_for_flush()?;
            if self.accumulator.buffered_bytes() == 0 {
                break;
            }
            while self.in_flight.len() >= self.max_in_flight_requests {
                self.wait_for_one_for_flush().await?;
            }
            let batches = self.accumulator.drain_all();
            let mut selection = self.select_dispatchable_batches(batches);
            if let Err(error) = self
                .dispatcher
                .prepare_drained_batches(&mut selection.dispatchable)
                .await
            {
                selection.dispatchable.extend(selection.deferred);
                self.accumulator.requeue_front(selection.dispatchable);
                return Err(error);
            }
            if !selection.deferred.is_empty() {
                self.accumulator.requeue_front(selection.deferred);
            }
            if selection.dispatchable.is_empty() {
                if self.in_flight.is_empty() {
                    return Err(ProducerError::FlushIncomplete);
                }
                self.wait_for_one_for_flush().await?;
                continue;
            }
            self.spawn_dispatch(
                selection.dispatchable,
                std::time::Instant::now(),
                selection.partitions,
            );
        }
        while !self.in_flight.is_empty() {
            self.wait_for_one_for_flush().await?;
        }
        Ok(())
    }

    /// Initialize transactional producer state with the transaction coordinator.
    ///
    /// # Errors
    ///
    /// Returns a producer error when `transactional.id` is not configured or
    /// the coordinator rejects `InitProducerId`.
    pub async fn init_transactions(&self) -> Result<()> {
        let started_at = std::time::Instant::now();
        let result = self.init_transactions_with_max_block().await;
        if result.is_ok() {
            self.metrics.record_transaction_init(started_at.elapsed());
        }
        result
    }

    async fn init_transactions_with_max_block(&self) -> Result<()> {
        let dispatcher = self.dispatcher.clone();
        let mut task = tokio::spawn(async move { dispatcher.init_transactions().await });
        match tokio::time::timeout(self.max_block, &mut task).await {
            Ok(joined) => joined.map_err(|error| ProducerError::DispatchTask(error.to_string()))?,
            Err(_elapsed) => {
                self.dispatcher.mark_init_transactions_timed_out().await;
                Err(ProducerError::DispatchTask(
                    "InitTransactions timed out - did not complete InitProducerId with the \
                     transaction coordinator within max.block.ms"
                        .to_owned(),
                ))
            },
        }
    }

    /// Begin a producer transaction.
    ///
    /// # Errors
    ///
    /// Returns an error when transactions are not configured, not initialized,
    /// or another transaction is already open.
    pub fn begin_transaction(&self) -> Result<()> {
        let started_at = std::time::Instant::now();
        let result = self.dispatcher.begin_transaction();
        if result.is_ok() {
            self.metrics.record_transaction_begin(started_at.elapsed());
        }
        result
    }

    /// Flush pending records and commit the open transaction.
    ///
    /// # Errors
    ///
    /// Returns an error from flushing records or `EndTxn`.
    pub async fn commit_transaction(&mut self) -> Result<()> {
        let started_at = std::time::Instant::now();
        let retry_pending_commit = self
            .dispatcher
            .pending_end_transaction_matches(true)
            .await?;
        if !retry_pending_commit {
            self.dispatcher.validate_commit_transaction_start()?;
            self.flush().await?;
        }
        let result = self
            .end_transaction_with_max_block(true, "CommitTransaction")
            .await;
        if result.is_ok() {
            self.metrics.record_transaction_commit(started_at.elapsed());
        }
        result
    }

    /// Abort the open transaction.
    ///
    /// # Errors
    ///
    /// Returns an error from `EndTxn`.
    pub async fn abort_transaction(&mut self) -> Result<()> {
        let started_at = std::time::Instant::now();
        let retry_pending_abort = self
            .dispatcher
            .pending_end_transaction_matches(false)
            .await?;
        if !retry_pending_abort {
            let _dropped_batches = self.accumulator.drain_all();
            while !self.in_flight.is_empty() {
                self.wait_for_one_for_flush().await?;
            }
        }
        let result = self
            .end_transaction_with_max_block(false, "AbortTransaction")
            .await;
        if result.is_ok() {
            self.metrics.record_transaction_abort(started_at.elapsed());
        }
        result
    }

    async fn end_transaction_with_max_block(
        &self,
        committed: bool,
        operation: &'static str,
    ) -> Result<()> {
        let dispatcher = self.dispatcher.clone();
        let mut task = tokio::spawn(async move { dispatcher.end_transaction(committed).await });
        match tokio::time::timeout(self.max_block, &mut task).await {
            Ok(joined) => joined.map_err(|error| ProducerError::DispatchTask(error.to_string()))?,
            Err(_elapsed) => {
                self.dispatcher
                    .mark_end_transaction_timed_out(committed)
                    .await;
                Err(ProducerError::DispatchTask(format!(
                    "{operation} timed out - did not complete EndTxn with the transaction \
                     coordinator within max.block.ms"
                )))
            },
        }
    }

    /// Add consumer offsets to the active transaction.
    ///
    /// # Errors
    ///
    /// Returns an error when transactions are not configured, not open, or the
    /// transaction/group coordinator rejects `AddOffsetsToTxn` or
    /// `TxnOffsetCommit`.
    pub async fn send_offsets_to_transaction<I>(
        &self,
        offsets: I,
        group_metadata: ConsumerGroupMetadata,
    ) -> Result<()>
    where
        I: IntoIterator<Item = (TopicPartition, OffsetAndMetadata)>,
    {
        let offsets: Vec<_> = offsets.into_iter().collect();
        if offsets.is_empty() {
            return self
                .dispatcher
                .send_offsets_to_transaction(offsets, group_metadata)
                .await;
        }
        let started_at = std::time::Instant::now();
        let result = self
            .send_offsets_to_transaction_with_max_block(offsets, group_metadata)
            .await;
        if result.is_ok() {
            self.metrics
                .record_send_offsets_to_transaction(started_at.elapsed());
        }
        result
    }

    async fn send_offsets_to_transaction_with_max_block(
        &self,
        offsets: Vec<(TopicPartition, OffsetAndMetadata)>,
        group_metadata: ConsumerGroupMetadata,
    ) -> Result<()> {
        let dispatcher = self.dispatcher.clone();
        let mut task = tokio::spawn(async move {
            dispatcher
                .send_offsets_to_transaction(offsets, group_metadata)
                .await
        });
        match tokio::time::timeout(self.max_block, &mut task).await {
            Ok(joined) => joined.map_err(|error| ProducerError::DispatchTask(error.to_string()))?,
            Err(_elapsed) => {
                self.dispatcher
                    .mark_send_offsets_to_transaction_timed_out()
                    .await;
                Err(ProducerError::DispatchTask(
                    "SendOffsetsToTransaction timed out - did not reach the coordinator or \
                     receive the TxnOffsetCommit/AddOffsetsToTxn response within max.block.ms"
                        .to_owned(),
                ))
            },
        }
    }

    /// Flush buffered records and consume the producer.
    ///
    /// # Errors
    ///
    /// Returns any error from [`Self::flush`].
    pub async fn close(mut self) -> Result<()> {
        if super::record::in_delivery_callback() {
            return Ok(());
        }
        self.flush().await
    }

    /// Close immediately without waiting for buffered records or in-flight dispatches.
    ///
    /// This mirrors Java producer close with a zero timeout: pending records may
    /// be dropped by consuming the producer.
    pub fn close_now(self) {
        drop(self);
    }

    /// Flush buffered records and consume the producer, bounded by `timeout`.
    ///
    /// # Errors
    ///
    /// Returns any error from [`Self::flush`], or a timeout error if the flush
    /// does not complete within the requested duration.
    pub async fn close_timeout(mut self, timeout: std::time::Duration) -> Result<()> {
        if super::record::in_delivery_callback() {
            return Ok(());
        }
        if timeout.is_zero() {
            return Ok(());
        }
        tokio::time::timeout(timeout, self.flush())
            .await
            .map_err(|_elapsed| {
                ProducerError::DispatchTask("producer close timeout expired".to_owned())
            })?
    }

    /// Flush buffered records and consume the producer using a Java-style
    /// signed millisecond timeout.
    ///
    /// # Errors
    ///
    /// Returns [`ProducerError::InvalidCloseTimeout`] when `timeout_ms` is
    /// negative, or any error from [`Self::close_timeout`].
    pub async fn close_timeout_ms(self, timeout_ms: i64) -> Result<()> {
        let timeout_ms = u64::try_from(timeout_ms)
            .map_err(|_negative| ProducerError::InvalidCloseTimeout { timeout_ms })?;
        self.close_timeout(std::time::Duration::from_millis(timeout_ms))
            .await
    }

    /// Estimated bytes currently buffered in the producer accumulator.
    #[must_use]
    pub const fn buffered_bytes(&self) -> usize {
        self.accumulator.buffered_bytes()
    }

    /// Point-in-time producer metrics snapshot.
    #[must_use]
    pub fn metrics(&self) -> ProducerMetricsSnapshot {
        self.metrics.snapshot(
            self.accumulator.buffered_bytes(),
            self.accumulator.buffered_records(),
            self.in_flight.len(),
        )
    }

    /// Point-in-time named producer metrics, similar to Java's metrics map.
    #[must_use]
    pub fn metrics_registry(&self) -> BTreeMap<&'static str, ProducerMetricValue> {
        self.metrics().as_metric_map()
    }

    /// Serialize producer and registered application metrics as OTLP `MetricsData`.
    #[must_use]
    pub fn otlp_metrics_data(&self, time_unix_nanos: u64) -> Bytes {
        self.metrics().to_otlp_metrics_data_with_kafka_metrics(
            time_unix_nanos,
            self.application_metrics.values(),
        )
    }

    /// Fetch partition metadata for one topic through the wire metadata cache.
    ///
    /// # Errors
    ///
    /// Returns an error when metadata fetch fails or the topic is not present
    /// in the resulting metadata snapshot.
    pub async fn partitions_for(&self, topic: &str) -> Result<Vec<ProducerPartitionInfo>> {
        let metadata_started_at = std::time::Instant::now();
        let metadata = self.dispatcher.metadata_for_topics([topic]).await?;
        self.metrics
            .record_metadata_wait(metadata_started_at.elapsed());
        let topic_metadata = metadata
            .topic(topic)
            .ok_or_else(|| ProducerError::UnknownTopic(topic.to_owned()))?;
        Ok(topic_metadata
            .partitions
            .iter()
            .map(|partition| ProducerPartitionInfo {
                topic: topic_metadata.name.clone(),
                topic_id: topic_metadata.topic_id,
                partition: partition.partition_index,
                leader_id: partition.leader_id,
                leader_epoch: partition.leader_epoch,
                replica_nodes: partition.replica_nodes.clone(),
                isr_nodes: partition.isr_nodes.clone(),
                offline_replicas: partition.offline_replicas.clone(),
            })
            .collect())
    }

    /// Return this producer instance's Kafka-negotiated client-instance id.
    ///
    /// # Errors
    ///
    /// Returns [`ProducerError::TelemetryDisabled`] when
    /// `enable.metrics.push=false`, matching Java `KafkaProducer`.
    pub async fn client_instance_id(&self, timeout: std::time::Duration) -> Result<KafkaUuid> {
        if !self.telemetry_is_enabled() {
            return Err(ProducerError::TelemetryDisabled);
        }
        let cached = {
            let cached = self
                .client_instance_id
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            *cached
        };
        if cached != KafkaUuid::ZERO {
            return Ok(cached);
        }

        let subscription = tokio::time::timeout(timeout, self.fetch_telemetry_subscription())
            .await
            .map_err(|_elapsed| {
                ProducerError::DispatchTask(
                    "producer client_instance_id timeout expired".to_owned(),
                )
            })??;
        let client_instance_id = subscription.client_instance_id;
        let mut cached = self
            .client_instance_id
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if *cached == KafkaUuid::ZERO {
            *cached = client_instance_id;
        }
        Ok(*cached)
    }

    /// Return this producer instance's Kafka-negotiated client-instance id.
    ///
    /// This Java-style overload accepts a signed millisecond timeout so callers
    /// can receive a local validation error for negative durations.
    ///
    /// # Errors
    ///
    /// Returns [`ProducerError::InvalidTelemetryTimeout`] when `timeout_ms` is
    /// negative, or any error from [`Self::client_instance_id`].
    pub async fn client_instance_id_timeout_ms(&self, timeout_ms: i64) -> Result<KafkaUuid> {
        let timeout_ms = u64::try_from(timeout_ms)
            .map_err(|_negative| ProducerError::InvalidTelemetryTimeout { timeout_ms })?;
        self.client_instance_id(std::time::Duration::from_millis(timeout_ms))
            .await
    }

    /// Push one uncompressed OpenTelemetry metrics payload using the broker subscription.
    ///
    /// # Errors
    ///
    /// Returns [`ProducerError::TelemetryDisabled`] when telemetry push is disabled,
    /// [`ProducerError::InvalidTelemetrySubscription`] for invalid subscription
    /// data, and [`ProducerError::Telemetry`] when the broker rejects the push.
    pub async fn push_telemetry(
        &self,
        metrics: Bytes,
        terminating: bool,
        timeout: std::time::Duration,
    ) -> Result<()> {
        if !self.telemetry_is_enabled() {
            return Err(ProducerError::TelemetryDisabled);
        }
        let push = async {
            let mut retried_subscription_refresh = false;
            loop {
                let subscription = self.fetch_telemetry_subscription().await?;
                if !subscription.accepted_compression_types.contains(&0) {
                    return Err(ProducerError::InvalidTelemetrySubscription(
                        "broker does not accept uncompressed telemetry",
                    ));
                }
                let metrics_len = i32::try_from(metrics.len()).map_err(|_overflow| {
                    ProducerError::InvalidTelemetrySubscription(
                        "metrics payload exceeds telemetry_max_bytes",
                    )
                })?;
                if subscription.telemetry_max_bytes >= 0
                    && metrics_len > subscription.telemetry_max_bytes
                {
                    return Err(ProducerError::InvalidTelemetrySubscription(
                        "metrics payload exceeds telemetry_max_bytes",
                    ));
                }

                let broker_id = self.dispatcher.any_broker_id()?;
                let request = PushTelemetryRequestData {
                    client_instance_id: subscription.client_instance_id,
                    subscription_id: subscription.subscription_id,
                    terminating,
                    compression_type: 0,
                    metrics: metrics.clone(),
                    _unknown_tagged_fields: Vec::new(),
                };
                let version = client_api_info(ApiKey::PushTelemetry).max_version;
                let response: PushTelemetryResponseData = self
                    .dispatcher
                    .send_control_request(broker_id, ApiKey::PushTelemetry, version, &request)
                    .await?;
                let error = ErrorCode::from(response.error_code);
                if !error.is_error() {
                    return Ok(());
                }
                if matches!(
                    error,
                    ErrorCode::UnknownSubscriptionId | ErrorCode::UnsupportedCompressionType
                ) {
                    self.clear_telemetry_subscription();
                    if !retried_subscription_refresh {
                        retried_subscription_refresh = true;
                        continue;
                    }
                }
                if is_fatal_telemetry_error(error) {
                    self.disable_telemetry();
                }
                return Err(ProducerError::Telemetry {
                    operation: "push_telemetry",
                    error,
                });
            }
        };
        tokio::time::timeout(timeout, push)
            .await
            .map_err(|_elapsed| {
                ProducerError::DispatchTask("producer push_telemetry timeout expired".to_owned())
            })?
    }

    /// Aggregate current producer metrics and push them as uncompressed OTLP.
    ///
    /// # Errors
    ///
    /// Returns the same telemetry, subscription, and timeout errors as
    /// [`Self::push_telemetry`].
    pub async fn push_current_telemetry(
        &self,
        terminating: bool,
        timeout: std::time::Duration,
    ) -> Result<()> {
        let metrics = self.otlp_metrics_data(current_unix_time_nanos());
        self.push_telemetry(metrics, terminating, timeout).await
    }

    async fn fetch_telemetry_subscription(&self) -> Result<TelemetrySubscription> {
        if !self.telemetry_is_enabled() {
            return Err(ProducerError::TelemetryDisabled);
        }
        let cached_subscription = {
            let cached_subscription = self
                .telemetry_subscription
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            cached_subscription.clone()
        };
        if let Some(subscription) = cached_subscription {
            return Ok(subscription);
        }

        let cached_client_instance_id = {
            let cached = self
                .client_instance_id
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            *cached
        };
        let broker_id = self.dispatcher.any_broker_id()?;
        let request = GetTelemetrySubscriptionsRequestData {
            client_instance_id: cached_client_instance_id,
            _unknown_tagged_fields: Vec::new(),
        };
        let version = client_api_info(ApiKey::GetTelemetrySubscriptions).max_version;
        let response: GetTelemetrySubscriptionsResponseData = self
            .dispatcher
            .send_control_request(
                broker_id,
                ApiKey::GetTelemetrySubscriptions,
                version,
                &request,
            )
            .await?;
        let error = ErrorCode::from(response.error_code);
        if error.is_error() {
            if is_fatal_telemetry_error(error) {
                self.disable_telemetry();
            }
            return Err(ProducerError::Telemetry {
                operation: "get_telemetry_subscriptions",
                error,
            });
        }
        let client_instance_id = if response.client_instance_id == KafkaUuid::ZERO {
            cached_client_instance_id
        } else {
            response.client_instance_id
        };
        if client_instance_id == KafkaUuid::ZERO {
            return Err(ProducerError::InvalidTelemetrySubscription(
                "client_instance_id must be non-zero",
            ));
        }

        let subscription = TelemetrySubscription {
            client_instance_id,
            subscription_id: response.subscription_id,
            accepted_compression_types: response.accepted_compression_types,
            telemetry_max_bytes: response.telemetry_max_bytes,
        };
        {
            let mut cached = self
                .client_instance_id
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if *cached == KafkaUuid::ZERO {
                *cached = client_instance_id;
            }
        }
        {
            let mut cached_subscription = self
                .telemetry_subscription
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if cached_subscription.is_none() {
                *cached_subscription = Some(subscription.clone());
            }
        }
        Ok(subscription)
    }

    fn clear_telemetry_subscription(&self) {
        let mut cached_subscription = self
            .telemetry_subscription
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *cached_subscription = None;
    }

    fn disable_telemetry(&self) {
        self.telemetry_disabled.store(true, Ordering::Relaxed);
        self.clear_telemetry_subscription();
    }

    fn telemetry_is_enabled(&self) -> bool {
        self.enable_metrics_push && !self.telemetry_disabled.load(Ordering::Relaxed)
    }

    /// Enable producer metrics that require per-batch/request accounting.
    pub const fn enable_metrics(&mut self) {
        self.metrics_enabled = true;
        self.dispatcher.enable_metrics();
    }

    /// Register one metric name for future client-metrics subscription pushes.
    pub fn register_metric_for_subscription(&mut self, subscription: ProducerMetricSubscription) {
        if !self.enable_metrics_push {
            return;
        }
        if ProducerMetricsSnapshot::is_internal_metric_name(&subscription.name) {
            return;
        }
        let _inserted = self.metric_subscriptions.insert(subscription.name);
    }

    /// Register one application Kafka metric for client telemetry subscription.
    ///
    /// Rust cannot JVM-load arbitrary `KafkaMetric` instances, so this native API
    /// mirrors Java's reporter lifecycle for caller-provided metrics.
    pub fn register_kafka_metric_for_subscription(&mut self, metric: KafkaMetric) {
        if !self.enable_metrics_push {
            return;
        }
        if ProducerMetricsSnapshot::is_internal_metric_name(metric.metric_name().name()) {
            return;
        }
        for reporter in &self.metric_reporters {
            reporter.metric_change(&metric);
        }
        let _inserted = self
            .metric_subscriptions
            .insert(metric.metric_name().name().to_owned());
        let _previous = self
            .application_metrics
            .insert(metric.metric_name().clone(), metric);
    }

    /// Remove one metric name from the client-metrics subscription set.
    pub fn unregister_metric_from_subscription(
        &mut self,
        subscription: &ProducerMetricSubscription,
    ) {
        if !self.enable_metrics_push {
            return;
        }
        if ProducerMetricsSnapshot::is_internal_metric_name(&subscription.name) {
            return;
        }
        let _removed = self.metric_subscriptions.remove(subscription.name.as_str());
    }

    /// Remove one application Kafka metric from client telemetry subscription.
    pub fn unregister_kafka_metric_from_subscription(&mut self, metric_name: &MetricName) {
        if !self.enable_metrics_push {
            return;
        }
        if ProducerMetricsSnapshot::is_internal_metric_name(metric_name.name()) {
            return;
        }
        let Some(metric) = self.application_metrics.remove(metric_name) else {
            return;
        };
        for reporter in &self.metric_reporters {
            reporter.metric_removal(&metric);
        }
        let _removed = self.metric_subscriptions.remove(metric_name.name());
    }

    /// Enable dispatch latency collection for benchmark/diagnostic runs.
    ///
    /// Samples are measured from the earliest append timestamp in a drained
    /// dispatch group until the broker response has been handled. This avoids
    /// per-record delivery handles on the untracked throughput path.
    pub fn enable_dispatch_latency_metrics(&mut self) {
        let _samples = self.dispatch_latency_samples.get_or_insert_with(Vec::new);
    }

    /// Add a producer interceptor to this producer instance.
    pub fn add_interceptor(&mut self, interceptor: impl ProducerInterceptor) {
        self.interceptors.push(interceptor);
    }

    /// Add a Rust-native metrics reporter.
    pub fn add_metric_reporter(&mut self, reporter: impl MetricReporter) {
        let reporter = Arc::new(reporter);
        reporter.init(&[]);
        self.metric_reporters.push(reporter);
    }

    /// Set a Rust-native producer partitioner for unassigned records.
    pub fn set_partitioner(&mut self, partitioner: impl ProducerPartitioner) {
        self.partitioner = ProducerPartitionerHandle::new(partitioner);
    }

    /// Take and clear collected dispatch latency samples.
    #[must_use]
    pub fn take_dispatch_latency_samples(&mut self) -> Vec<std::time::Duration> {
        self.dispatch_latency_samples
            .as_mut()
            .map(core::mem::take)
            .unwrap_or_default()
    }

    fn collect_finished(&mut self) -> Result<()> {
        while let Some(result) = self.in_flight.try_join_next() {
            self.dispatch_task_result(result, false)?;
        }
        Ok(())
    }

    fn collect_finished_for_flush(&mut self) -> Result<()> {
        while let Some(result) = self.in_flight.try_join_next() {
            self.dispatch_task_result(result, true)?;
        }
        Ok(())
    }

    async fn wait_for_one(&mut self) -> Result<()> {
        let Some(result) = self.in_flight.join_next().await else {
            return Ok(());
        };
        self.dispatch_task_result(result, false)
    }

    async fn wait_for_one_for_flush(&mut self) -> Result<()> {
        let Some(result) = self.in_flight.join_next().await else {
            return Ok(());
        };
        self.dispatch_task_result(result, true)
    }

    fn spawn_dispatch(
        &mut self,
        batches: Vec<super::ReadyBatch>,
        now: std::time::Instant,
        partitions: Vec<InFlightPartitionKey>,
    ) {
        if self.metrics_enabled {
            self.record_batch_metrics(&batches);
        }
        for partition in &partitions {
            let _inserted = self.in_flight_partitions.insert(partition.clone());
        }
        let started_at = batches
            .iter()
            .map(|batch| batch.first_append_at)
            .min()
            .unwrap_or(now);
        let dispatcher = self.dispatcher.clone();
        let abort_handle = self.in_flight.spawn(async move {
            let outcome = dispatcher.dispatch_drained(batches, now).await;
            TimedDispatchOutcome {
                outcome,
                latency: started_at.elapsed(),
                partitions,
            }
        });
        drop(abort_handle);
    }

    fn select_dispatchable_batches(&self, batches: Vec<super::ReadyBatch>) -> DispatchSelection {
        if !self.idempotent_ordering {
            return DispatchSelection {
                dispatchable: batches,
                deferred: Vec::new(),
                partitions: Vec::new(),
            };
        }

        let mut dispatchable = Vec::with_capacity(batches.len());
        let mut deferred = Vec::new();
        let mut partitions = Vec::new();
        let mut reserved = AHashSet::new();
        for batch in batches {
            let key = InFlightPartitionKey::from(&batch);
            if self.in_flight_partitions.contains(&key) {
                deferred.push(batch);
                continue;
            }
            if reserved.insert(key.clone()) {
                partitions.push(key);
            }
            dispatchable.push(batch);
        }
        DispatchSelection {
            dispatchable,
            deferred,
            partitions,
        }
    }

    async fn append_for_delivery_with_max_block(
        &mut self,
        record: ProducerRecord,
        now: std::time::Instant,
    ) -> Result<(SendFuture, AppendStatus)> {
        self.validate_record_size(&record)?;
        let deadline = std::time::Instant::now()
            .checked_add(self.max_block)
            .unwrap_or_else(std::time::Instant::now);
        loop {
            let can_wait = self.accumulator.buffered_bytes() > 0 || !self.in_flight.is_empty();
            if !self.accumulator.has_available_memory_for(&record) {
                if can_wait && std::time::Instant::now() < deadline {
                    self.poll().await?;
                    self.wait_for_buffer(deadline).await?;
                    continue;
                }
                return Err(ProducerError::Backpressure);
            }
            return self
                .accumulator
                .append_for_delivery_with_status_at(record, now);
        }
    }

    async fn append_for_batch_delivery_until(
        &mut self,
        record: ProducerRecord,
        now: std::time::Instant,
        deadline: std::time::Instant,
        metadata_capacity: usize,
    ) -> Result<(Option<SendFuture>, AppendStatus)> {
        self.validate_record_size(&record)?;
        loop {
            let can_wait = self.accumulator.buffered_bytes() > 0 || !self.in_flight.is_empty();
            if !self.accumulator.has_available_memory_for(&record) {
                if can_wait && std::time::Instant::now() < deadline {
                    self.poll().await?;
                    self.wait_for_buffer(deadline).await?;
                    continue;
                }
                return Err(ProducerError::Backpressure);
            }
            return self
                .accumulator
                .append_for_batch_delivery_with_status_at_capacity(record, now, metadata_capacity);
        }
    }

    fn append_deadline(&self) -> std::time::Instant {
        std::time::Instant::now()
            .checked_add(self.max_block)
            .unwrap_or_else(std::time::Instant::now)
    }

    async fn append_untracked_with_max_block(
        &mut self,
        record: ProducerRecord,
        now: std::time::Instant,
    ) -> Result<AppendStatus> {
        self.validate_record_size(&record)?;
        let deadline = std::time::Instant::now()
            .checked_add(self.max_block)
            .unwrap_or_else(std::time::Instant::now);
        loop {
            let can_wait = self.accumulator.buffered_bytes() > 0 || !self.in_flight.is_empty();
            match self.accumulator.append_with_status_at(record.clone(), now) {
                Ok(status) => return Ok(status),
                Err(ProducerError::Backpressure)
                    if can_wait && std::time::Instant::now() < deadline =>
                {
                    self.poll().await?;
                    self.wait_for_buffer(deadline).await?;
                },
                Err(error) => return Err(error),
            }
        }
    }

    async fn wait_for_buffer(&mut self, deadline: std::time::Instant) -> Result<()> {
        if !self.in_flight.is_empty() {
            self.wait_for_one().await?;
            return Ok(());
        }
        let now = std::time::Instant::now();
        if now >= deadline {
            return Ok(());
        }
        let remaining = deadline.duration_since(now);
        tokio::time::sleep(remaining.min(std::time::Duration::from_millis(1))).await;
        Ok(())
    }

    fn dispatch_task_result(
        &mut self,
        result: core::result::Result<TimedDispatchOutcome, JoinError>,
        requeue_is_error: bool,
    ) -> Result<()> {
        match result {
            Ok(TimedDispatchOutcome {
                outcome: DispatchOutcome::Delivered(result),
                latency,
                partitions,
            }) => {
                self.release_dispatch_partitions(partitions);
                self.record_dispatch_latency(latency);
                result.map(|_receipts| ())
            },
            Ok(TimedDispatchOutcome {
                outcome: DispatchOutcome::Requeue(batches),
                partitions,
                ..
            }) => {
                self.release_dispatch_partitions(partitions);
                if self.metrics_enabled {
                    self.metrics.record_requeue();
                }
                self.accumulator.requeue_front(batches);
                if requeue_is_error {
                    Err(ProducerError::FlushIncomplete)
                } else {
                    Ok(())
                }
            },
            Err(error) => Err(ProducerError::DispatchTask(error.to_string())),
        }
    }

    fn release_dispatch_partitions(&mut self, partitions: Vec<InFlightPartitionKey>) {
        for partition in partitions {
            let _removed = self.in_flight_partitions.remove(&partition);
        }
    }

    fn record_batch_metrics(&self, batches: &[super::ReadyBatch]) {
        for batch in batches {
            self.metrics.record_produce_batch(
                batch.bytes,
                self.accumulator_batch_size,
                batch.records.len(),
            );
        }
    }

    fn record_dispatch_latency(&mut self, latency: std::time::Duration) {
        if let Some(samples) = &mut self.dispatch_latency_samples {
            samples.push(latency);
        }
    }

    fn intercept_on_send(&self, record: ProducerRecord) -> ProducerRecord {
        if self.interceptors.is_empty() {
            return record;
        }
        self.interceptors.on_send(record)
    }

    async fn assign_default_partitions_for_batch(
        &self,
        topic_plan: &BatchTopicPlan,
        metadata: &ClusterMetadata,
        pending: &mut [ProducerRecord],
    ) -> Result<()> {
        if self.partitioner.is_some() {
            return Ok(());
        }
        if let [topic] = topic_plan.topics.as_slice() {
            self.dispatcher
                .refresh_topic_load_stats_with_metadata(&self.accumulator, metadata, topic.as_ref())
                .await?;
            if let StickyTopicSelection::Single(_) = &topic_plan.sticky_topics {
                self.dispatcher
                    .assign_sticky_topic_partitions_with_metadata(metadata, topic.as_ref(), pending)
                    .await
            } else {
                self.dispatcher
                    .assign_topic_partitions_with_metadata(metadata, topic.as_ref(), pending)
                    .await
            }
        } else {
            self.dispatcher
                .refresh_partition_load_stats_with_metadata(
                    &self.accumulator,
                    metadata,
                    topic_plan.topics.iter().map(Arc::as_ref),
                )
                .await?;
            self.dispatcher
                .assign_partitions_with_metadata(metadata, pending)
                .await
        }
    }

    fn interceptor_headers(
        &self,
        record: &ProducerRecord,
    ) -> Option<Vec<kacrab_protocol::record::RecordHeader>> {
        (!self.interceptors.is_empty()).then(|| record.headers.clone())
    }

    fn register_interceptor_ack(
        &self,
        delivery: &SendFuture,
        headers: Option<Vec<kacrab_protocol::record::RecordHeader>>,
    ) {
        let Some(headers) = headers else {
            return;
        };
        let interceptors = self.interceptors.clone();
        delivery.register_callback(Box::new(move |result| match result {
            Ok(metadata) => interceptors.on_ack(Some(&metadata), None, &headers),
            Err(error) => interceptors.on_ack(None, Some(&error), &headers),
        }));
    }

    fn validate_record_size(&self, record: &ProducerRecord) -> Result<()> {
        let estimated_size =
            RECORD_BATCH_OVERHEAD_BYTES.saturating_add(estimate_record_batch_bytes(record));
        if estimated_size > self.max_request_size {
            return Err(ProducerError::RecordTooLarge {
                size: estimated_size,
                max_request_size: self.max_request_size,
            });
        }
        Ok(())
    }

    const fn uses_sticky_partitioner(&self, record: &ProducerRecord) -> bool {
        !self.partitioner.is_some() && self.dispatcher.uses_sticky_partitioner(record)
    }

    fn topic_plan_for_batch(&self, records: &[ProducerRecord]) -> BatchTopicPlan {
        if let Some(first) = records.first() {
            let first_topic = Arc::clone(&first.topic);
            let mut has_unassigned_record = false;
            let mut single_sticky_topic = true;
            let mut all_same_topic = true;

            for record in records {
                if !Arc::ptr_eq(&first_topic, &record.topic)
                    && first_topic.as_ref() != record.topic.as_ref()
                {
                    all_same_topic = false;
                    break;
                }
                has_unassigned_record |= !record.has_assigned_partition();
                single_sticky_topic &= self.uses_sticky_partitioner(record);
            }

            if all_same_topic {
                let topics = if has_unassigned_record {
                    vec![Arc::clone(&first_topic)]
                } else {
                    Vec::new()
                };
                let sticky_topics = if single_sticky_topic {
                    StickyTopicSelection::Single(first_topic)
                } else {
                    StickyTopicSelection::PerRecord(
                        records
                            .iter()
                            .map(|record| {
                                self.uses_sticky_partitioner(record)
                                    .then(|| Arc::clone(&record.topic))
                            })
                            .collect(),
                    )
                };
                return BatchTopicPlan {
                    topics,
                    sticky_topics,
                };
            }
        }

        let mut seen = AHashSet::new();
        let mut topics = Vec::new();
        let mut sticky_topic = None::<Arc<str>>;
        let mut single_sticky_topic = true;

        for record in records {
            if !record.has_assigned_partition() && seen.insert(Arc::clone(&record.topic)) {
                topics.push(Arc::clone(&record.topic));
            }
            if !single_sticky_topic {
                continue;
            }
            if !self.uses_sticky_partitioner(record) {
                single_sticky_topic = false;
                continue;
            }
            if let Some(topic) = &sticky_topic {
                if topic.as_ref() != record.topic.as_ref() {
                    single_sticky_topic = false;
                }
            } else {
                sticky_topic = Some(Arc::clone(&record.topic));
            }
        }

        let sticky_topics = if single_sticky_topic {
            sticky_topic.map_or_else(
                || StickyTopicSelection::PerRecord(Vec::new()),
                StickyTopicSelection::Single,
            )
        } else {
            StickyTopicSelection::PerRecord(
                records
                    .iter()
                    .map(|record| {
                        self.uses_sticky_partitioner(record)
                            .then(|| Arc::clone(&record.topic))
                    })
                    .collect(),
            )
        };

        BatchTopicPlan {
            topics,
            sticky_topics,
        }
    }

    async fn assign_partition(&self, record: &mut ProducerRecord) -> Result<()> {
        if record.has_assigned_partition() {
            return Ok(());
        }
        if !self.partitioner.is_some() {
            return self
                .dispatcher
                .assign_partition_with_accumulator(&self.accumulator, record)
                .await;
        }
        let metadata = self
            .dispatcher
            .metadata_for_topics([record.topic.as_ref()])
            .await?;
        self.assign_partition_with_metadata(&metadata, record).await
    }

    async fn assign_partition_with_metadata(
        &self,
        metadata: &ClusterMetadata,
        record: &mut ProducerRecord,
    ) -> Result<()> {
        if record.has_assigned_partition() {
            return Ok(());
        }
        let Some(partition) = self.partitioner.partition(record, metadata).transpose()? else {
            return self
                .dispatcher
                .assign_partition_with_metadata(metadata, record)
                .await;
        };
        validate_selected_partition(metadata, record, partition)?;
        record.partition = partition;
        Ok(())
    }
}

impl Drop for Producer {
    fn drop(&mut self) {
        self.interceptors.close();
        self.partitioner.close();
        close_metric_reporters(&self.metric_reporters);
    }
}

fn validate_selected_partition(
    metadata: &ClusterMetadata,
    record: &ProducerRecord,
    partition: i32,
) -> Result<()> {
    let topic_metadata = metadata
        .topic(record.topic.as_ref())
        .ok_or_else(|| ProducerError::UnknownTopic(record.topic.to_string()))?;
    if topic_metadata
        .partitions
        .iter()
        .any(|partition_metadata| partition_metadata.partition_index == partition)
    {
        return Ok(());
    }
    Err(ProducerError::UnknownPartition {
        topic: record.topic.to_string(),
        partition,
    })
}

const fn is_fatal_telemetry_error(error: ErrorCode) -> bool {
    matches!(
        error,
        ErrorCode::InvalidRequest | ErrorCode::InvalidRecord | ErrorCode::UnsupportedVersion
    )
}

#[expect(
    clippy::too_many_lines,
    reason = "Callback error cloning is intentionally exhaustive across public producer errors."
)]
fn producer_error_for_callback(error: &ProducerError) -> Option<ProducerError> {
    match error {
        ProducerError::Backpressure => Some(ProducerError::Backpressure),
        ProducerError::InvalidRecord { field, message } => {
            Some(ProducerError::InvalidRecord { field, message })
        },
        ProducerError::RecordTooLarge {
            size,
            max_request_size,
        } => Some(ProducerError::RecordTooLarge {
            size: *size,
            max_request_size: *max_request_size,
        }),
        ProducerError::FlushIncomplete => Some(ProducerError::FlushIncomplete),
        ProducerError::CallbackOperation { operation } => {
            Some(ProducerError::CallbackOperation { operation })
        },
        ProducerError::DeliveryTimeout { topic, partition } => {
            Some(ProducerError::DeliveryTimeout {
                topic: topic.clone(),
                partition: *partition,
            })
        },
        ProducerError::UnknownTopic(topic) => Some(ProducerError::UnknownTopic(topic.clone())),
        ProducerError::UnknownPartition { topic, partition } => {
            Some(ProducerError::UnknownPartition {
                topic: topic.clone(),
                partition: *partition,
            })
        },
        ProducerError::LeaderNotFound {
            topic,
            partition,
            leader_id,
        } => Some(ProducerError::LeaderNotFound {
            topic: topic.clone(),
            partition: *partition,
            leader_id: *leader_id,
        }),
        ProducerError::MissingProduceResponse { topic, partition } => {
            Some(ProducerError::MissingProduceResponse {
                topic: topic.clone(),
                partition: *partition,
            })
        },
        ProducerError::Broker {
            topic,
            partition,
            error,
        } => Some(ProducerError::Broker {
            topic: topic.clone(),
            partition: *partition,
            error: *error,
        }),
        ProducerError::Transaction { operation, error } => Some(ProducerError::Transaction {
            operation,
            error: *error,
        }),
        ProducerError::TransactionalIdRequired => Some(ProducerError::TransactionalIdRequired),
        ProducerError::InvalidTransactionState(message) => {
            Some(ProducerError::InvalidTransactionState(message))
        },
        ProducerError::TransactionStateBusy => Some(ProducerError::TransactionStateBusy),
        ProducerError::InvalidConsumerGroupMetadata(message) => {
            Some(ProducerError::InvalidConsumerGroupMetadata(message))
        },
        ProducerError::SequenceOverflow { topic, partition } => {
            Some(ProducerError::SequenceOverflow {
                topic: topic.clone(),
                partition: *partition,
            })
        },
        ProducerError::UnresolvedSequence { topic, partition } => {
            Some(ProducerError::UnresolvedSequence {
                topic: topic.clone(),
                partition: *partition,
            })
        },
        ProducerError::DispatchTask(message) => Some(ProducerError::DispatchTask(message.clone())),
        ProducerError::DeliveryDropped => Some(ProducerError::DeliveryDropped),
        ProducerError::UnsupportedOperation(operation) => {
            Some(ProducerError::UnsupportedOperation(operation))
        },
        ProducerError::TelemetryDisabled => Some(ProducerError::TelemetryDisabled),
        ProducerError::Telemetry { operation, error } => Some(ProducerError::Telemetry {
            operation,
            error: *error,
        }),
        ProducerError::InvalidTelemetrySubscription(message) => {
            Some(ProducerError::InvalidTelemetrySubscription(message))
        },
        ProducerError::InvalidTelemetryTimeout { timeout_ms } => {
            Some(ProducerError::InvalidTelemetryTimeout {
                timeout_ms: *timeout_ms,
            })
        },
        ProducerError::InvalidCloseTimeout { timeout_ms } => {
            Some(ProducerError::InvalidCloseTimeout {
                timeout_ms: *timeout_ms,
            })
        },
        ProducerError::InvalidConfig { key, value } => Some(ProducerError::InvalidConfig {
            key,
            value: value.clone(),
        }),
        ProducerError::Wire(_) | ProducerError::Record(_) | ProducerError::Config { .. } => None,
    }
}

impl From<&super::ReadyBatch> for InFlightPartitionKey {
    fn from(batch: &super::ReadyBatch) -> Self {
        Self {
            topic: batch.topic.clone(),
            partition: batch.partition,
        }
    }
}

/// Java-style builder for [`Producer`].
#[derive(Clone, Debug, Default)]
pub struct ProducerBuilder {
    config: ClientConfig,
    sasl_client_authenticator: Option<SaslClientAuthenticatorHandle>,
    sasl_client_authenticator_factory: Option<SaslClientAuthenticatorFactoryHandle>,
    interceptors: ProducerInterceptors,
    partitioner: ProducerPartitionerHandle,
    metric_reporters: Vec<Arc<dyn MetricReporter>>,
}

impl ProducerBuilder {
    /// Creates an empty producer builder.
    #[must_use]
    pub fn new() -> Self {
        Self {
            config: ClientConfig::new(),
            sasl_client_authenticator: None,
            sasl_client_authenticator_factory: None,
            interceptors: ProducerInterceptors::default(),
            partitioner: ProducerPartitionerHandle::default(),
            metric_reporters: Vec::new(),
        }
    }

    /// Sets a Kafka producer property.
    #[must_use]
    pub fn set(mut self, key: impl Into<ConfigKey>, value: impl Into<ConfigValue>) -> Self {
        self.config = self.config.set(key, value);
        self
    }

    /// Sets a native Rust SASL client authenticator.
    #[must_use]
    pub fn sasl_client_authenticator(
        mut self,
        authenticator: impl SaslClientAuthenticator,
    ) -> Self {
        self.sasl_client_authenticator = Some(SaslClientAuthenticatorHandle::new(authenticator));
        self
    }

    /// Sets a native Rust SASL client authenticator factory.
    #[must_use]
    pub fn sasl_client_authenticator_factory(
        mut self,
        factory: impl SaslClientAuthenticatorFactory,
    ) -> Self {
        self.sasl_client_authenticator_factory =
            Some(SaslClientAuthenticatorFactoryHandle::new(factory));
        self
    }

    /// Adds a producer interceptor.
    #[must_use]
    pub fn interceptor(mut self, interceptor: impl ProducerInterceptor) -> Self {
        self.interceptors.push(interceptor);
        self
    }

    /// Sets a native Rust partitioner for unassigned records.
    ///
    /// This replaces `partitioner.class` JVM plugin loading with an explicit
    /// Rust implementation.
    #[must_use]
    pub fn partitioner(mut self, partitioner: impl ProducerPartitioner) -> Self {
        self.partitioner = ProducerPartitionerHandle::new(partitioner);
        self
    }

    /// Adds a native Rust metrics reporter.
    #[must_use]
    pub fn metric_reporter(mut self, reporter: impl MetricReporter) -> Self {
        self.metric_reporters.push(Arc::new(reporter));
        self
    }

    /// Returns the underlying Java-style client config.
    #[must_use]
    pub const fn client_config(&self) -> &ClientConfig {
        &self.config
    }

    /// Builds a producer.
    ///
    /// # Errors
    ///
    /// Returns an error when config validation, DNS resolution, or connection
    /// setup preparation fails.
    pub async fn build(self) -> Result<Producer> {
        let Self {
            config,
            sasl_client_authenticator,
            sasl_client_authenticator_factory,
            interceptors,
            partitioner,
            metric_reporters,
        } = self;
        let config = client_config_without_byte_array_serializer_class_configs(&config);
        let config = client_config_without_native_plugin_class_configs(
            &config,
            NativePluginClassStrip::default()
                .interceptors_if(!interceptors.is_empty())
                .partitioner_if(partitioner.is_some())
                .metric_reporters_if(!metric_reporters.is_empty()),
        );
        let config = config
            .producer_config()
            .map_err(|error| ProducerError::Config { error })?;
        let runtime = config.to_producer_runtime_config()?;
        let endpoints = resolve_bootstrap_brokers(&config).await?;
        let mut connection = config.to_connection_config();
        connection.sasl.client_authenticator = sasl_client_authenticator;
        connection.sasl.client_authenticator_factory = sasl_client_authenticator_factory;
        let wire = WireClient::connect_with_brokers(connection, config.client_id, endpoints);
        let mut producer = Producer::from_parts(wire, runtime);
        producer.interceptors = interceptors;
        producer.partitioner = partitioner;
        initialize_metric_reporters(&metric_reporters);
        producer.metric_reporters = metric_reporters;
        Ok(producer)
    }

    /// Builds a typed producer with key/value serializers.
    ///
    /// # Errors
    ///
    /// Returns an error when config validation, DNS resolution, or connection
    /// setup preparation fails.
    pub async fn build_with_serializers<K, V, KS, VS>(
        self,
        key_serializer: KS,
        value_serializer: VS,
    ) -> Result<TypedProducer<K, V, KS, VS>>
    where
        K: Sync,
        V: Sync,
        KS: ProducerSerializer<K>,
        VS: ProducerSerializer<V>,
    {
        let Self {
            config,
            sasl_client_authenticator,
            sasl_client_authenticator_factory,
            interceptors,
            partitioner,
            metric_reporters,
        } = self;
        let config = client_config_without_native_plugin_class_configs(
            &config,
            NativePluginClassStrip::default()
                .serializers()
                .interceptors_if(!interceptors.is_empty())
                .partitioner_if(partitioner.is_some())
                .metric_reporters_if(!metric_reporters.is_empty()),
        );
        let config = config
            .producer_config()
            .map_err(|error| ProducerError::Config { error })?;
        let runtime = config.to_producer_runtime_config()?;
        let endpoints = resolve_bootstrap_brokers(&config).await?;
        let mut connection = config.to_connection_config();
        connection.sasl.client_authenticator = sasl_client_authenticator;
        connection.sasl.client_authenticator_factory = sasl_client_authenticator_factory;
        let wire = WireClient::connect_with_brokers(connection, config.client_id, endpoints);
        let mut producer = Producer::from_parts(wire, runtime);
        producer.interceptors = interceptors;
        producer.partitioner = partitioner;
        initialize_metric_reporters(&metric_reporters);
        producer.metric_reporters = metric_reporters;
        Ok(Producer::from_parts_with_serializers(
            producer,
            key_serializer,
            value_serializer,
        ))
    }

    /// Builds a typed producer by loading built-in native serializers from
    /// `key.serializer` and `value.serializer` class names.
    ///
    /// # Errors
    ///
    /// Returns an error when configured serializer class names are missing or
    /// do not match the requested native serializers.
    pub async fn build_with_configured_serializers<K, V, KS, VS>(
        self,
    ) -> Result<TypedProducer<K, V, KS, VS>>
    where
        K: Sync,
        V: Sync,
        KS: ConfiguredProducerSerializer<K>,
        VS: ConfiguredProducerSerializer<V>,
    {
        let Self {
            config,
            sasl_client_authenticator,
            sasl_client_authenticator_factory,
            interceptors,
            partitioner,
            metric_reporters,
        } = self;
        validate_configured_serializer_class::<K, KS>(&config, "key.serializer")?;
        validate_configured_serializer_class::<V, VS>(&config, "value.serializer")?;
        let key_serializer = KS::from_client_config(&config, true)?;
        let value_serializer = VS::from_client_config(&config, false)?;
        let config = client_config_without_native_plugin_class_configs(
            &config,
            NativePluginClassStrip::default()
                .serializers()
                .interceptors_if(!interceptors.is_empty())
                .partitioner_if(partitioner.is_some())
                .metric_reporters_if(!metric_reporters.is_empty()),
        );
        let config = config
            .producer_config()
            .map_err(|error| ProducerError::Config { error })?;
        let runtime = config.to_producer_runtime_config()?;
        let endpoints = resolve_bootstrap_brokers(&config).await?;
        let mut connection = config.to_connection_config();
        connection.sasl.client_authenticator = sasl_client_authenticator;
        connection.sasl.client_authenticator_factory = sasl_client_authenticator_factory;
        let wire = WireClient::connect_with_brokers(connection, config.client_id, endpoints);
        let mut producer = Producer::from_parts(wire, runtime);
        producer.interceptors = interceptors;
        producer.partitioner = partitioner;
        initialize_metric_reporters(&metric_reporters);
        producer.metric_reporters = metric_reporters;
        Ok(Producer::from_parts_with_serializers(
            producer,
            key_serializer,
            value_serializer,
        ))
    }
}

fn validate_configured_serializer_class<T, S>(
    config: &ClientConfig,
    key: &'static str,
) -> Result<()>
where
    S: ConfiguredProducerSerializer<T>,
{
    let value = config
        .get(key)
        .map(ConfigValue::as_str)
        .unwrap_or_default()
        .trim();
    if value.is_empty() || !S::JAVA_CLASS_NAMES.contains(&value) {
        return Err(ProducerError::InvalidConfig {
            key,
            value: value.to_owned(),
        });
    }
    Ok(())
}

fn client_config_without_serializer_class_configs(config: &ClientConfig) -> ClientConfig {
    client_config_without_native_plugin_class_configs(
        config,
        NativePluginClassStrip::default().serializers(),
    )
}

fn client_config_without_empty_interceptor_class_configs(config: &ClientConfig) -> ClientConfig {
    client_config_without_native_plugin_class_configs(config, NativePluginClassStrip::default())
}

fn client_config_without_byte_array_serializer_class_configs(
    config: &ClientConfig,
) -> ClientConfig {
    let properties: Properties = config
        .properties()
        .iter()
        .filter(|(key, value)| {
            !is_byte_or_bytes_serializer_class_config(key.as_str(), value.as_str())
        })
        .map(|(key, value)| (key.as_str().to_owned(), value.as_str().to_owned()))
        .collect();
    ClientConfig::from(properties)
}

fn client_config_without_native_plugin_class_configs(
    config: &ClientConfig,
    strip: NativePluginClassStrip,
) -> ClientConfig {
    let properties: Properties = config
        .properties()
        .iter()
        .filter(|(key, value)| {
            let key = key.as_str();
            !(strip.serializers_enabled() && is_serializer_class_config(key)
                || should_strip_interceptor_class_config(
                    key,
                    value.as_str(),
                    strip.interceptors_enabled(),
                )
                || strip.partitioner_enabled() && is_partitioner_class_config(key)
                || strip.metric_reporters_enabled() && is_metric_reporters_config(key))
        })
        .map(|(key, value)| (key.as_str().to_owned(), value.as_str().to_owned()))
        .collect();
    ClientConfig::from(properties)
}

#[derive(Debug, Clone, Copy, Default)]
struct NativePluginClassStrip {
    mask: u8,
}

impl NativePluginClassStrip {
    const SERIALIZERS: u8 = 1;
    const INTERCEPTORS: u8 = 2;
    const PARTITIONER: u8 = 4;
    const METRIC_REPORTERS: u8 = 8;

    const fn serializers(mut self) -> Self {
        self.mask |= Self::SERIALIZERS;
        self
    }

    const fn interceptors_if(mut self, enabled: bool) -> Self {
        if enabled {
            self.mask |= Self::INTERCEPTORS;
        }
        self
    }

    const fn partitioner_if(mut self, enabled: bool) -> Self {
        if enabled {
            self.mask |= Self::PARTITIONER;
        }
        self
    }

    const fn metric_reporters_if(mut self, enabled: bool) -> Self {
        if enabled {
            self.mask |= Self::METRIC_REPORTERS;
        }
        self
    }

    const fn serializers_enabled(self) -> bool {
        self.mask & Self::SERIALIZERS != 0
    }

    const fn interceptors_enabled(self) -> bool {
        self.mask & Self::INTERCEPTORS != 0
    }

    const fn partitioner_enabled(self) -> bool {
        self.mask & Self::PARTITIONER != 0
    }

    const fn metric_reporters_enabled(self) -> bool {
        self.mask & Self::METRIC_REPORTERS != 0
    }
}

const fn is_serializer_class_config(key: &str) -> bool {
    matches!(key.as_bytes(), b"key.serializer" | b"value.serializer")
}

const fn is_byte_or_bytes_serializer_class_config(key: &str, value: &str) -> bool {
    is_serializer_class_config(key)
        && matches!(
            value.as_bytes(),
            b"org.apache.kafka.common.serialization.ByteArraySerializer"
                | b"ByteArraySerializer"
                | b"org.apache.kafka.common.serialization.ByteBufferSerializer"
                | b"ByteBufferSerializer"
                | b"org.apache.kafka.common.serialization.BytesSerializer"
                | b"BytesSerializer"
        )
}

fn should_strip_interceptor_class_config(key: &str, value: &str, strip_interceptors: bool) -> bool {
    is_interceptor_class_config(key) && (strip_interceptors || value.trim().is_empty())
}

const fn is_interceptor_class_config(key: &str) -> bool {
    matches!(key.as_bytes(), b"interceptor.classes")
}

const fn is_partitioner_class_config(key: &str) -> bool {
    matches!(key.as_bytes(), b"partitioner.class")
}

const fn is_metric_reporters_config(key: &str) -> bool {
    matches!(key.as_bytes(), b"metric.reporters")
}

fn initialize_metric_reporters(reporters: &[Arc<dyn MetricReporter>]) {
    let metrics: &[KafkaMetric] = &[];
    for reporter in reporters {
        reporter.init(metrics);
    }
}

fn close_metric_reporters(reporters: &[Arc<dyn MetricReporter>]) {
    for reporter in reporters {
        reporter.close();
    }
}

fn current_unix_time_nanos() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
        })
}

async fn resolve_bootstrap_brokers(config: &ProducerConfig) -> Result<Vec<BrokerEndpoint>> {
    let mut endpoints = Vec::new();
    for (index, server) in config.bootstrap_servers.as_slice().iter().enumerate() {
        let node_id = i32::try_from(index).map_err(|_error| ProducerError::InvalidConfig {
            key: ProducerConfig::BOOTSTRAP_SERVERS_CONFIG,
            value: server.clone(),
        })?;
        let (host, port) = parse_bootstrap_server(server)?;
        let mut addresses = tokio::net::lookup_host((host.as_str(), port))
            .await
            .map_err(WireError::from)?;
        let addr = addresses.next();
        drop(addresses);
        if let Some(addr) = addr {
            endpoints.push(BrokerEndpoint::from_resolved(node_id, host, port, addr));
        }
    }
    if endpoints.is_empty() {
        return Err(ProducerError::InvalidConfig {
            key: ProducerConfig::BOOTSTRAP_SERVERS_CONFIG,
            value: String::new(),
        });
    }
    Ok(endpoints)
}

fn parse_bootstrap_server(server: &str) -> Result<(String, u16)> {
    let (host, port) = server
        .rsplit_once(':')
        .ok_or_else(|| ProducerError::InvalidConfig {
            key: ProducerConfig::BOOTSTRAP_SERVERS_CONFIG,
            value: server.to_owned(),
        })?;
    let port = port
        .parse::<u16>()
        .map_err(|_error| ProducerError::InvalidConfig {
            key: ProducerConfig::BOOTSTRAP_SERVERS_CONFIG,
            value: server.to_owned(),
        })?;
    let host = host
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(host);
    if host.is_empty() {
        return Err(ProducerError::InvalidConfig {
            key: ProducerConfig::BOOTSTRAP_SERVERS_CONFIG,
            value: server.to_owned(),
        });
    }
    Ok((host.to_owned(), port))
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::expect_used,
        clippy::missing_assert_message,
        clippy::panic,
        clippy::unwrap_used,
        reason = "Unit test fixtures fail fastest with contextual unwrap/expect calls."
    )]

    use std::{
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
        time::{Duration, Instant},
    };

    use bytes::Bytes;
    use kacrab_protocol::{KafkaUuid, generated::ErrorCode};

    use super::{
        AppendPollBudget, DispatchOutcome, InFlightPartitionKey, Producer, ProducerBuilder,
        TimedDispatchOutcome, resolve_bootstrap_brokers,
    };
    use crate::{
        config::ClientConfig,
        producer::{
            AccumulatorConfig, ConsumerGroupMetadata, KafkaMetric, MetricName, MetricReporter,
            MetricValue, ProducerError, ProducerIdempotenceConfig, ProducerIdentity,
            ProducerInterceptor, ProducerMetricSubscription, ProducerMetricValue, ProducerRecord,
            ProducerRuntimeConfig, RecordMetadata, SendFuture, TopicPartition,
            accumulator::AppendStatus,
        },
        wire::{
            BrokerEndpoint, ConnectionConfig, SaslClientAction, SaslClientAuthenticator,
            SaslMechanism, WireClient,
        },
    };

    #[derive(Debug)]
    struct BuilderSaslAuthenticator;

    impl SaslClientAuthenticator for BuilderSaslAuthenticator {
        fn mechanism(&self) -> SaslMechanism {
            SaslMechanism::Plain
        }

        fn start(&self) -> crate::wire::Result<SaslClientAction> {
            Ok(SaslClientAction::Send(Bytes::from_static(b"builder")))
        }

        fn next(&self, _challenge: &[u8]) -> crate::wire::Result<SaslClientAction> {
            Ok(SaslClientAction::Complete)
        }
    }

    fn test_wire() -> WireClient {
        WireClient::connect_with_brokers(
            ConnectionConfig::default(),
            "producer-test",
            [BrokerEndpoint::new(
                7,
                "127.0.0.1:9092".parse().expect("valid socket address"),
            )],
        )
    }

    fn runtime_config(max_in_flight: usize) -> ProducerRuntimeConfig {
        ProducerRuntimeConfig {
            accumulator: AccumulatorConfig::default()
                .batch_size(usize::MAX)
                .linger(Duration::from_mins(1)),
            max_in_flight_requests_per_connection: max_in_flight,
            idempotence: ProducerIdempotenceConfig {
                enabled: false,
                ..ProducerIdempotenceConfig::default()
            },
            ..ProducerRuntimeConfig::default()
        }
    }

    fn producer(max_in_flight: usize) -> Producer {
        Producer::from_parts(test_wire(), runtime_config(max_in_flight))
    }

    #[test]
    fn idempotent_producer_keeps_configured_dispatch_task_concurrency() {
        let producer = Producer::from_parts(
            test_wire(),
            ProducerRuntimeConfig {
                max_in_flight_requests_per_connection: 5,
                idempotence: ProducerIdempotenceConfig {
                    enabled: true,
                    ..ProducerIdempotenceConfig::default()
                },
                ..ProducerRuntimeConfig::default()
            },
        );

        assert_eq!(producer.max_in_flight_requests, 5);
    }

    #[test]
    fn producer_builder_accepts_native_sasl_authenticator() {
        let builder = ProducerBuilder::new().sasl_client_authenticator(BuilderSaslAuthenticator);

        assert_eq!(
            builder
                .sasl_client_authenticator
                .as_ref()
                .map(crate::wire::SaslClientAuthenticatorHandle::mechanism),
            Some(SaslMechanism::Plain)
        );
    }

    #[tokio::test]
    async fn serializer_constructors_build_typed_producers() {
        let typed = Producer::from_map_with_serializers(
            [
                ("bootstrap.servers", "127.0.0.1:9092"),
                (
                    "key.serializer",
                    "org.apache.kafka.common.serialization.StringSerializer",
                ),
                (
                    "value.serializer",
                    "org.apache.kafka.common.serialization.ByteArraySerializer",
                ),
            ],
            crate::producer::StringSerializer::default(),
            crate::producer::BytesSerializer,
        )
        .await
        .expect("typed producer from map");
        assert_eq!(typed.producer().buffered_bytes(), 0);

        let typed = ProducerBuilder::new()
            .set("bootstrap.servers", "127.0.0.1:9092")
            .set(
                "key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer",
            )
            .set(
                "value.serializer",
                "org.apache.kafka.common.serialization.ByteArraySerializer",
            )
            .build_with_serializers(
                crate::producer::StringSerializer::default(),
                crate::producer::BytesSerializer,
            )
            .await
            .expect("typed producer from builder");
        assert_eq!(typed.producer().buffered_bytes(), 0);
    }

    #[tokio::test]
    async fn configured_serializer_constructors_load_builtin_java_serializers() {
        let typed: crate::producer::TypedProducer<
            String,
            String,
            crate::producer::StringSerializer,
            crate::producer::StringSerializer,
        > = Producer::from_map_with_configured_serializers([
            ("bootstrap.servers", "127.0.0.1:9092"),
            (
                "key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer",
            ),
            (
                "value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer",
            ),
        ])
        .await
        .expect("typed producer from configured serializer classes");
        assert_eq!(typed.producer().buffered_bytes(), 0);

        let typed: crate::producer::TypedProducer<
            Bytes,
            Bytes,
            crate::producer::BytesSerializer,
            crate::producer::BytesSerializer,
        > = ProducerBuilder::new()
            .set("bootstrap.servers", "127.0.0.1:9092")
            .set(
                "key.serializer",
                "org.apache.kafka.common.serialization.BytesSerializer",
            )
            .set(
                "value.serializer",
                "org.apache.kafka.common.serialization.BytesSerializer",
            )
            .build_with_configured_serializers()
            .await
            .expect("typed producer from configured builder serializers");
        assert_eq!(typed.producer().buffered_bytes(), 0);

        let typed: crate::producer::TypedProducer<
            i32,
            i64,
            crate::producer::IntegerSerializer,
            crate::producer::LongSerializer,
        > = Producer::from_map_with_configured_serializers([
            ("bootstrap.servers", "127.0.0.1:9092"),
            (
                "key.serializer",
                "org.apache.kafka.common.serialization.IntegerSerializer",
            ),
            (
                "value.serializer",
                "org.apache.kafka.common.serialization.LongSerializer",
            ),
        ])
        .await
        .expect("typed producer from configured primitive serializer classes");
        assert_eq!(typed.producer().buffered_bytes(), 0);

        let typed: crate::producer::TypedProducer<
            uuid::Uuid,
            uuid::Uuid,
            crate::producer::UuidSerializer,
            crate::producer::UuidSerializer,
        > = Producer::from_map_with_configured_serializers([
            ("bootstrap.servers", "127.0.0.1:9092"),
            (
                "key.serializer",
                "org.apache.kafka.common.serialization.UUIDSerializer",
            ),
            (
                "value.serializer",
                "org.apache.kafka.common.serialization.UUIDSerializer",
            ),
        ])
        .await
        .expect("typed producer from configured uuid serializer classes");
        assert_eq!(typed.producer().buffered_bytes(), 0);
    }

    #[tokio::test]
    async fn configured_serializer_constructors_reject_mismatched_java_serializers() {
        let error = ProducerBuilder::new()
            .set("bootstrap.servers", "127.0.0.1:9092")
            .set(
                "key.serializer",
                "org.apache.kafka.common.serialization.ByteArraySerializer",
            )
            .set(
                "value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer",
            )
            .build_with_configured_serializers::<
                String,
                String,
                crate::producer::StringSerializer,
                crate::producer::StringSerializer,
            >()
            .await
            .expect_err("mismatched configured serializer should fail");

        assert!(matches!(
            error,
            ProducerError::InvalidConfig {
                key: "key.serializer",
                ..
            }
        ));
    }

    #[tokio::test]
    async fn byte_producer_accepts_java_byte_array_serializer_configs() {
        let producer = Producer::from_map([
            ("bootstrap.servers", "127.0.0.1:9092"),
            (
                "key.serializer",
                "org.apache.kafka.common.serialization.ByteArraySerializer",
            ),
            (
                "value.serializer",
                "org.apache.kafka.common.serialization.ByteArraySerializer",
            ),
        ])
        .await
        .expect("byte producer should accept byte array serializer configs");
        assert_eq!(producer.buffered_bytes(), 0);

        let producer = ProducerBuilder::new()
            .set("bootstrap.servers", "127.0.0.1:9092")
            .set(
                "key.serializer",
                "org.apache.kafka.common.serialization.ByteArraySerializer",
            )
            .set(
                "value.serializer",
                "org.apache.kafka.common.serialization.ByteArraySerializer",
            )
            .build()
            .await
            .expect("builder byte producer should accept byte array serializer configs");
        assert_eq!(producer.buffered_bytes(), 0);
    }

    #[tokio::test]
    async fn byte_producer_accepts_java_bytes_serializer_configs() {
        let producer = Producer::from_map([
            ("bootstrap.servers", "127.0.0.1:9092"),
            (
                "key.serializer",
                "org.apache.kafka.common.serialization.BytesSerializer",
            ),
            (
                "value.serializer",
                "org.apache.kafka.common.serialization.BytesSerializer",
            ),
        ])
        .await
        .expect("byte producer should accept bytes serializer configs");
        assert_eq!(producer.buffered_bytes(), 0);

        let producer = ProducerBuilder::new()
            .set("bootstrap.servers", "127.0.0.1:9092")
            .set(
                "key.serializer",
                "org.apache.kafka.common.serialization.BytesSerializer",
            )
            .set(
                "value.serializer",
                "org.apache.kafka.common.serialization.BytesSerializer",
            )
            .build()
            .await
            .expect("builder byte producer should accept bytes serializer configs");
        assert_eq!(producer.buffered_bytes(), 0);
    }

    #[tokio::test]
    async fn byte_producer_accepts_java_byte_buffer_serializer_configs() {
        let producer = Producer::from_map([
            ("bootstrap.servers", "127.0.0.1:9092"),
            (
                "key.serializer",
                "org.apache.kafka.common.serialization.ByteBufferSerializer",
            ),
            (
                "value.serializer",
                "org.apache.kafka.common.serialization.ByteBufferSerializer",
            ),
        ])
        .await
        .expect("byte producer should accept byte buffer serializer configs");
        assert_eq!(producer.buffered_bytes(), 0);

        let producer = ProducerBuilder::new()
            .set("bootstrap.servers", "127.0.0.1:9092")
            .set(
                "key.serializer",
                "org.apache.kafka.common.serialization.ByteBufferSerializer",
            )
            .set(
                "value.serializer",
                "org.apache.kafka.common.serialization.ByteBufferSerializer",
            )
            .build()
            .await
            .expect("builder byte producer should accept byte buffer serializer configs");
        assert_eq!(producer.buffered_bytes(), 0);
    }

    #[tokio::test]
    async fn builder_with_native_interceptor_ignores_java_interceptor_classes() {
        let producer = ProducerBuilder::new()
            .set("bootstrap.servers", "127.0.0.1:9092")
            .set(
                "interceptor.classes",
                "org.apache.kafka.clients.producer.MockProducerInterceptor",
            )
            .interceptor(PartitionRewriteInterceptor)
            .build()
            .await
            .expect("native interceptor should replace Java interceptor class loading");

        assert_eq!(producer.buffered_bytes(), 0);
        assert!(!producer.interceptors.is_empty());
    }

    #[tokio::test]
    async fn builder_with_native_metric_reporter_ignores_java_metric_reporters() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let producer = ProducerBuilder::new()
            .set("bootstrap.servers", "127.0.0.1:9092")
            .set(
                "metric.reporters",
                "org.apache.kafka.common.metrics.JmxReporter",
            )
            .metric_reporter(RecordingMetricReporter {
                events: Arc::clone(&events),
            })
            .build()
            .await
            .expect("native metric reporter should replace Java metric reporter loading");

        assert_eq!(producer.buffered_bytes(), 0);
        producer.close_now();
        let events = events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        assert_eq!(events, vec!["init:0".to_owned(), "close".to_owned()]);
    }

    #[tokio::test]
    async fn producer_kafka_metric_subscription_notifies_native_reporters_like_java() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let mut producer = ProducerBuilder::new()
            .set("bootstrap.servers", "127.0.0.1:9092")
            .set("enable.metrics.push", "true")
            .metric_reporter(RecordingMetricReporter {
                events: Arc::clone(&events),
            })
            .build()
            .await
            .expect("producer with native metric reporter");
        assert!(producer.enable_metrics_push);
        assert_eq!(producer.metric_reporters.len(), 1);
        let metric_name = MetricName::new("orders.sent", "app")
            .with_description("application orders sent")
            .tag("client-id", "orders-producer");
        let metric = KafkaMetric::from_fn(metric_name.clone(), || MetricValue::Number(42.0));

        producer.register_kafka_metric_for_subscription(metric.clone());
        assert!(producer.metric_subscriptions.contains("orders.sent"));
        producer.register_kafka_metric_for_subscription(metric);
        producer.unregister_kafka_metric_from_subscription(&metric_name);
        producer.unregister_kafka_metric_from_subscription(&metric_name);
        producer.register_kafka_metric_for_subscription(KafkaMetric::from_fn(
            MetricName::new("produce_request_count", "producer-metrics"),
            || MetricValue::Number(1.0),
        ));
        producer.close_now();

        let events = events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        assert_eq!(
            events,
            vec![
                "init:0".to_owned(),
                "change:orders.sent".to_owned(),
                "change:orders.sent".to_owned(),
                "remove:orders.sent".to_owned(),
                "close".to_owned(),
            ]
        );
    }

    #[test]
    fn producer_otlp_metrics_data_includes_registered_kafka_metrics_like_java() {
        let mut producer = producer(1);
        producer.register_kafka_metric_for_subscription(KafkaMetric::from_fn(
            MetricName::new("orders.sent", "app")
                .with_description("application orders sent")
                .tag("client-id", "orders-producer"),
            || MetricValue::Number(42.0),
        ));

        let payload = producer.otlp_metrics_data(42);

        assert!(
            payload
                .windows(b"queue_depth_bytes".len())
                .any(|window| window == b"queue_depth_bytes")
        );
        assert!(
            payload
                .windows(b"orders.sent".len())
                .any(|window| window == b"orders.sent")
        );
        assert!(
            payload
                .windows(b"application orders sent".len())
                .any(|window| window == b"application orders sent")
        );
        assert!(
            payload
                .windows(b"client-id".len())
                .any(|window| window == b"client-id")
        );
        assert!(
            payload
                .windows(b"orders-producer".len())
                .any(|window| window == b"orders-producer")
        );
    }

    #[tokio::test]
    async fn empty_interceptor_classes_config_is_noop_like_java_default() {
        let producer = Producer::from_map([
            ("bootstrap.servers", "127.0.0.1:9092"),
            ("interceptor.classes", ""),
        ])
        .await
        .expect("empty interceptor.classes should match Java default list");
        assert_eq!(producer.buffered_bytes(), 0);
        assert!(producer.interceptors.is_empty());

        let producer = ProducerBuilder::new()
            .set("bootstrap.servers", "127.0.0.1:9092")
            .set("interceptor.classes", "  ")
            .build()
            .await
            .expect("blank interceptor.classes should match Java default list");
        assert_eq!(producer.buffered_bytes(), 0);
        assert!(producer.interceptors.is_empty());

        let error = Producer::from_map([
            ("bootstrap.servers", "127.0.0.1:9092"),
            (
                "interceptor.classes",
                "org.apache.kafka.clients.producer.MockProducerInterceptor",
            ),
        ])
        .await
        .expect_err("non-empty JVM interceptor classes still need native interceptors");
        assert!(matches!(
            error,
            ProducerError::Config {
                error: crate::config::ConfigError::JavaOnly { key, .. }
            } if key == "interceptor.classes"
        ));
    }

    #[tokio::test]
    async fn interceptor_on_send_mutates_record_before_append() {
        let mut producer = producer(1);
        producer.add_interceptor(PartitionRewriteInterceptor);

        let _delivery = producer
            .send(ProducerRecord::new("orders", 1).value(Bytes::from_static(b"value")))
            .await
            .expect("send");
        let batches = producer.accumulator.drain_all();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].partition, 0);
        assert_eq!(batches[0].records[0].partition, 0);
    }

    #[tokio::test]
    async fn interceptor_on_send_error_is_ignored_like_java() {
        let error_count = Arc::new(AtomicUsize::new(0));
        let mut producer = producer(1);
        producer.add_interceptor(PartitionRewriteInterceptor);
        producer.add_interceptor(RejectingInterceptor {
            error_count: Arc::clone(&error_count),
        });

        let _delivery = producer
            .send(ProducerRecord::new("orders", 1).value(Bytes::from_static(b"value")))
            .await
            .expect("interceptor errors are ignored");
        let batches = producer.accumulator.drain_all();

        assert_eq!(error_count.load(Ordering::Relaxed), 1);
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].partition, 0);
    }

    #[tokio::test]
    async fn interceptor_send_error_receives_record_metadata_like_java() {
        let metadata = Arc::new(Mutex::new(None));
        let mut config = runtime_config(1);
        config.max_request_size = 8;
        let mut producer = Producer::from_parts(test_wire(), config);
        producer.add_interceptor(ErrorMetadataInterceptor {
            metadata: Arc::clone(&metadata),
        });

        let error = producer
            .send(ProducerRecord::new("orders", 3).value(Bytes::from_static(b"value")))
            .await
            .expect_err("record should exceed max.request.size");
        let metadata = metadata
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
            .expect("interceptor metadata");

        assert!(matches!(error, ProducerError::RecordTooLarge { .. }));
        assert_eq!(metadata.topic.as_ref(), "orders");
        assert_eq!(metadata.partition, 3);
        assert_eq!(metadata.leader_id, -1);
        assert_eq!(metadata.offset, -1);
        assert_eq!(metadata.timestamp_ms, -1);
        assert_eq!(metadata.serialized_key_size, -1);
        assert_eq!(metadata.serialized_value_size, -1);
    }

    #[tokio::test]
    async fn interceptor_on_send_panic_is_ignored_like_java() {
        let mut producer = producer(1);
        producer.add_interceptor(PartitionRewriteInterceptor);
        producer.add_interceptor(PanickingOnSendInterceptor);
        producer.add_interceptor(PartitionRewriteInterceptor);

        let _delivery = producer
            .send(ProducerRecord::new("orders", 1).value(Bytes::from_static(b"value")))
            .await
            .expect("interceptor panics are ignored");
        let batches = producer.accumulator.drain_all();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].partition, 0);
    }

    #[tokio::test]
    async fn interceptor_ack_runs_before_user_callback() {
        let order = Arc::new(AtomicUsize::new(0));
        let mut producer = producer(1);
        producer.add_interceptor(OrderingInterceptor {
            order: Arc::clone(&order),
        });
        let callback_order = Arc::clone(&order);

        let _delivery = producer
            .send_with_callback(
                ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")),
                move |result| {
                    let _metadata = result.expect("callback receipt");
                    assert_eq!(callback_order.load(Ordering::Relaxed), 1);
                    callback_order.store(2, Ordering::Relaxed);
                },
            )
            .await
            .expect("send with callback");
        let mut batches = producer.accumulator.drain_all();
        let sender = batches
            .first_mut()
            .and_then(|batch| batch.delivery.take())
            .expect("delivery sender");

        sender.send(record_metadata(40));

        assert_eq!(order.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn send_with_callback_invokes_callback_on_local_api_error_like_java() {
        let callback_error = Arc::new(AtomicUsize::new(0));
        let callback_error_sink = Arc::clone(&callback_error);
        let mut config = runtime_config(1);
        config.max_request_size = 8;
        let mut producer = Producer::from_parts(test_wire(), config);

        let error = producer
            .send_with_callback(
                ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")),
                move |result| {
                    if matches!(result, Err(ProducerError::RecordTooLarge { .. })) {
                        callback_error_sink.store(1, Ordering::Relaxed);
                    }
                },
            )
            .await
            .expect_err("record should exceed max.request.size");

        assert!(matches!(error, ProducerError::RecordTooLarge { .. }));
        assert_eq!(callback_error.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn send_with_callback_local_api_error_runs_user_callback_before_interceptor_like_java() {
        let order = Arc::new(AtomicUsize::new(0));
        let callback_order = Arc::clone(&order);
        let mut config = runtime_config(1);
        config.max_request_size = 8;
        let mut producer = Producer::from_parts(test_wire(), config);
        producer.add_interceptor(ErrorOrderingInterceptor {
            order: Arc::clone(&order),
        });

        let error = producer
            .send_with_callback(
                ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")),
                move |result| {
                    assert!(matches!(result, Err(ProducerError::RecordTooLarge { .. })));
                    assert_eq!(callback_order.load(Ordering::Relaxed), 0);
                    callback_order.store(1, Ordering::Relaxed);
                },
            )
            .await
            .expect_err("record should exceed max.request.size");

        assert!(matches!(error, ProducerError::RecordTooLarge { .. }));
        assert_eq!(order.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn interceptor_ack_panic_does_not_skip_later_interceptors_like_java() {
        let ack_count = Arc::new(AtomicUsize::new(0));
        let mut producer = producer(1);
        producer.add_interceptor(PanickingAckInterceptor);
        producer.add_interceptor(CountingAckInterceptor {
            ack_count: Arc::clone(&ack_count),
        });

        let _delivery = producer
            .send(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")))
            .await
            .expect("send");
        let mut batches = producer.accumulator.drain_all();
        let sender = batches
            .first_mut()
            .and_then(|batch| batch.delivery.take())
            .expect("delivery sender");

        sender.send(record_metadata(40));

        assert_eq!(ack_count.load(Ordering::Relaxed), 1);
    }

    #[derive(Debug)]
    struct PartitionRewriteInterceptor;

    impl ProducerInterceptor for PartitionRewriteInterceptor {
        fn on_send(&self, mut record: ProducerRecord) -> crate::producer::Result<ProducerRecord> {
            record.partition = 0;
            Ok(record)
        }
    }

    #[derive(Debug)]
    struct RecordingMetricReporter {
        events: Arc<Mutex<Vec<String>>>,
    }

    impl MetricReporter for RecordingMetricReporter {
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

    impl RecordingMetricReporter {
        fn push(&self, event: String) {
            self.events
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(event);
        }
    }

    #[derive(Debug)]
    struct RejectingInterceptor {
        error_count: Arc<AtomicUsize>,
    }

    impl ProducerInterceptor for RejectingInterceptor {
        fn on_send(&self, _record: ProducerRecord) -> crate::producer::Result<ProducerRecord> {
            let _previous = self.error_count.fetch_add(1, Ordering::Relaxed);
            Err(ProducerError::InvalidRecord {
                field: "interceptor",
                message: "rejected by test interceptor",
            })
        }
    }

    #[derive(Debug)]
    struct ErrorMetadataInterceptor {
        metadata: Arc<Mutex<Option<RecordMetadata>>>,
    }

    impl ProducerInterceptor for ErrorMetadataInterceptor {
        fn on_ack(
            &self,
            metadata: Option<&RecordMetadata>,
            error: Option<&ProducerError>,
            _headers: &[kacrab_protocol::record::RecordHeader],
        ) {
            assert!(error.is_some());
            *self
                .metadata
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = metadata.cloned();
        }
    }

    #[derive(Debug)]
    struct PanickingOnSendInterceptor;

    impl ProducerInterceptor for PanickingOnSendInterceptor {
        fn on_send(&self, _record: ProducerRecord) -> crate::producer::Result<ProducerRecord> {
            panic!("injected on_send panic");
        }
    }

    #[derive(Debug)]
    struct OrderingInterceptor {
        order: Arc<AtomicUsize>,
    }

    impl ProducerInterceptor for OrderingInterceptor {
        fn on_ack(
            &self,
            metadata: Option<&RecordMetadata>,
            error: Option<&ProducerError>,
            _headers: &[kacrab_protocol::record::RecordHeader],
        ) {
            assert!(metadata.is_some());
            assert!(error.is_none());
            assert_eq!(self.order.load(Ordering::Relaxed), 0);
            self.order.store(1, Ordering::Relaxed);
        }
    }

    #[derive(Debug)]
    struct ErrorOrderingInterceptor {
        order: Arc<AtomicUsize>,
    }

    impl ProducerInterceptor for ErrorOrderingInterceptor {
        fn on_ack(
            &self,
            metadata: Option<&RecordMetadata>,
            error: Option<&ProducerError>,
            _headers: &[kacrab_protocol::record::RecordHeader],
        ) {
            assert!(metadata.is_some());
            assert!(error.is_some());
            assert_eq!(self.order.load(Ordering::Relaxed), 1);
            self.order.store(2, Ordering::Relaxed);
        }
    }

    #[derive(Debug)]
    struct PanickingAckInterceptor;

    impl ProducerInterceptor for PanickingAckInterceptor {
        fn on_ack(
            &self,
            _metadata: Option<&RecordMetadata>,
            _error: Option<&ProducerError>,
            _headers: &[kacrab_protocol::record::RecordHeader],
        ) {
            panic!("injected on_ack panic");
        }
    }

    #[derive(Debug)]
    struct CountingAckInterceptor {
        ack_count: Arc<AtomicUsize>,
    }

    impl ProducerInterceptor for CountingAckInterceptor {
        fn on_ack(
            &self,
            _metadata: Option<&RecordMetadata>,
            _error: Option<&ProducerError>,
            _headers: &[kacrab_protocol::record::RecordHeader],
        ) {
            let _previous = self.ack_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[derive(Debug)]
    struct ClosingInterceptor {
        close_count: Arc<AtomicUsize>,
    }

    impl ProducerInterceptor for ClosingInterceptor {
        fn close(&self) {
            let _previous = self.close_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[derive(Debug)]
    struct PanickingCloseInterceptor;

    impl ProducerInterceptor for PanickingCloseInterceptor {
        fn close(&self) {
            panic!("injected close panic");
        }
    }

    fn ready_batch() -> super::super::ReadyBatch {
        ready_batch_for_partition("orders", 0)
    }

    fn ready_batch_for_partition(topic: &str, partition: i32) -> super::super::ReadyBatch {
        let mut accumulator = crate::producer::RecordAccumulator::new(
            AccumulatorConfig::default()
                .batch_size(1)
                .linger(Duration::from_mins(1)),
        );
        accumulator
            .append_at(
                ProducerRecord::new(topic, partition).value(Bytes::from_static(b"value")),
                Instant::now(),
            )
            .expect("append record");
        accumulator
            .drain_ready(Instant::now())
            .pop()
            .expect("ready batch")
    }

    fn timed(outcome: DispatchOutcome) -> TimedDispatchOutcome {
        TimedDispatchOutcome {
            outcome,
            latency: Duration::ZERO,
            partitions: Vec::new(),
        }
    }

    fn record_metadata(offset: i64) -> RecordMetadata {
        RecordMetadata {
            topic: Arc::from("orders"),
            partition: 0,
            leader_id: 7,
            offset,
            timestamp_ms: -1,
            serialized_key_size: -1,
            serialized_value_size: -1,
        }
    }

    #[test]
    fn append_poll_budget_triggers_after_in_flight_sized_ready_batches() {
        let mut budget = AppendPollBudget::new(3);
        let not_ready = AppendStatus {
            batch_ready: false,
            ready_batch_records: 0,
        };
        let ready = AppendStatus {
            batch_ready: true,
            ready_batch_records: 2,
        };

        assert!(!budget.observe(not_ready));
        assert!(!budget.observe(ready));
        assert!(!budget.observe(ready));
        assert!(budget.observe(ready));
        assert!(!budget.observe(ready));
    }

    #[test]
    fn append_poll_budget_triggers_immediately_for_dense_ready_batches() {
        let mut budget = AppendPollBudget::new(5);
        let dense_ready = AppendStatus {
            batch_ready: true,
            ready_batch_records: 32,
        };

        assert!(budget.observe(dense_ready));
    }

    #[test]
    fn idempotent_selection_defers_batches_for_in_flight_partitions_when_order_is_guaranteed() {
        let mut config = runtime_config(1);
        config.idempotence = ProducerIdempotenceConfig {
            enabled: true,
            ..ProducerIdempotenceConfig::default()
        };
        let mut producer = Producer::from_parts(test_wire(), config);
        let in_flight = InFlightPartitionKey {
            topic: "orders".to_owned(),
            partition: 0,
        };
        let _inserted = producer.in_flight_partitions.insert(in_flight);

        let selection = producer.select_dispatchable_batches(vec![
            ready_batch_for_partition("orders", 0),
            ready_batch_for_partition("orders", 1),
        ]);

        assert_eq!(selection.dispatchable.len(), 1);
        assert_eq!(selection.dispatchable[0].partition, 1);
        assert_eq!(selection.deferred.len(), 1);
        assert_eq!(selection.deferred[0].partition, 0);
    }

    #[test]
    fn idempotent_selection_allows_same_partition_in_flight_when_pipeline_depth_exceeds_one() {
        let mut config = runtime_config(5);
        config.idempotence = ProducerIdempotenceConfig {
            enabled: true,
            ..ProducerIdempotenceConfig::default()
        };
        let mut producer = Producer::from_parts(test_wire(), config);
        let in_flight = InFlightPartitionKey {
            topic: "orders".to_owned(),
            partition: 0,
        };
        let _inserted = producer.in_flight_partitions.insert(in_flight);

        let selection = producer.select_dispatchable_batches(vec![
            ready_batch_for_partition("orders", 0),
            ready_batch_for_partition("orders", 1),
        ]);

        assert_eq!(selection.dispatchable.len(), 2);
        assert!(selection.deferred.is_empty());
    }

    #[test]
    fn guaranteed_order_selection_keeps_same_partition_batches_in_one_dispatch() {
        let mut config = runtime_config(1);
        config.idempotence = ProducerIdempotenceConfig {
            enabled: true,
            ..ProducerIdempotenceConfig::default()
        };
        let producer = Producer::from_parts(test_wire(), config);

        let selection = producer.select_dispatchable_batches(vec![
            ready_batch_for_partition("orders", 0),
            ready_batch_for_partition("orders", 0),
        ]);

        assert_eq!(selection.dispatchable.len(), 2);
        assert!(selection.deferred.is_empty());
        assert_eq!(selection.partitions.len(), 1);
    }

    #[tokio::test]
    async fn poll_waits_for_one_in_flight_slot_before_spawning_ready_batch() {
        let mut producer = producer(1);
        producer
            .accumulator
            .append_at(
                ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")),
                Instant::now(),
            )
            .expect("append buffered record");
        let _abort = producer.in_flight.spawn(async {
            tokio::time::sleep(Duration::from_millis(1)).await;
            timed(DispatchOutcome::Delivered(Ok(Vec::new())))
        });

        producer.poll().await.expect("poll waits for a slot");

        assert_eq!(producer.in_flight.len(), 1);
    }

    #[tokio::test]
    async fn poll_waits_until_blocked_in_flight_task_completes() {
        let mut producer = producer(1);
        producer
            .accumulator
            .append_at(
                ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")),
                Instant::now(),
            )
            .expect("append buffered record");
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let _abort = producer.in_flight.spawn(async {
            let _released = release_rx.await;
            timed(DispatchOutcome::Delivered(Ok(Vec::new())))
        });
        let release_task = tokio::spawn(async move {
            tokio::task::yield_now().await;
            release_tx.send(()).expect("release in-flight task");
        });

        producer.poll().await.expect("poll waits for a slot");
        release_task.await.expect("release task");

        assert_eq!(producer.in_flight.len(), 1);
    }

    #[tokio::test]
    async fn flush_waits_for_in_flight_slot_and_reports_local_delivery_timeout() {
        let mut config = runtime_config(1);
        config.delivery_timeout = Duration::ZERO;
        let mut producer = Producer::from_parts(test_wire(), config);
        producer
            .accumulator
            .append_at(
                ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")),
                Instant::now(),
            )
            .expect("append buffered record");
        let _abort = producer.in_flight.spawn(async {
            tokio::time::sleep(Duration::from_millis(1)).await;
            timed(DispatchOutcome::Delivered(Ok(Vec::new())))
        });

        assert!(matches!(
            producer.flush().await,
            Err(ProducerError::DeliveryTimeout { topic, partition })
                if topic == "orders" && partition == 0
        ));
        assert_eq!(producer.metrics().flush_count, 0);
    }

    #[tokio::test]
    async fn wait_for_one_helpers_return_ok_when_no_task_exists() {
        let mut producer = producer(1);

        producer.wait_for_one().await.expect("empty wait is ok");
        producer
            .wait_for_one_for_flush()
            .await
            .expect("empty flush wait is ok");
    }

    #[tokio::test]
    async fn wait_for_one_helpers_process_completed_tasks() {
        let mut producer = producer(1);
        let _abort = producer
            .in_flight
            .spawn(async { timed(DispatchOutcome::Delivered(Ok(Vec::new()))) });

        producer.wait_for_one().await.expect("wait consumes task");

        let _abort = producer
            .in_flight
            .spawn(async { timed(DispatchOutcome::Delivered(Ok(Vec::new()))) });
        producer
            .wait_for_one_for_flush()
            .await
            .expect("flush wait consumes task");
    }

    #[tokio::test]
    async fn collect_finished_for_flush_consumes_completed_tasks() {
        let mut producer = producer(1);
        let _abort = producer
            .in_flight
            .spawn(async { timed(DispatchOutcome::Delivered(Ok(Vec::new()))) });
        tokio::task::yield_now().await;

        producer
            .collect_finished_for_flush()
            .expect("flush collect consumes task");
    }

    #[tokio::test]
    async fn collect_finished_consumes_successful_and_panicked_tasks() {
        let mut producer = producer(1);
        let _abort = producer
            .in_flight
            .spawn(async { timed(DispatchOutcome::Delivered(Ok(Vec::new()))) });
        tokio::task::yield_now().await;

        producer
            .collect_finished()
            .expect("successful task is consumed");

        let _abort = producer.in_flight.spawn(async {
            tokio::task::yield_now().await;
            panic!("dispatch task panic");
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if matches!(
                    producer.collect_finished(),
                    Err(ProducerError::DispatchTask(_))
                ) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("panicked task is collected");
    }

    #[test]
    fn dispatch_task_result_requeues_batches_or_errors_for_flush() {
        let mut producer = producer(1);
        let batch = ready_batch();

        producer
            .dispatch_task_result(Ok(timed(DispatchOutcome::Requeue(vec![batch]))), false)
            .expect("non-flush requeue is retained");
        assert!(producer.buffered_bytes() > 0);

        let batch = producer
            .accumulator
            .drain_all()
            .pop()
            .expect("requeued batch");
        assert!(matches!(
            producer.dispatch_task_result(Ok(timed(DispatchOutcome::Requeue(vec![batch]))), true),
            Err(ProducerError::FlushIncomplete)
        ));
    }

    #[test]
    fn dispatch_task_result_records_latency_when_metrics_are_enabled() {
        let mut producer = producer(1);
        let latency = Duration::from_millis(3);
        producer.enable_dispatch_latency_metrics();

        producer
            .dispatch_task_result(
                Ok(TimedDispatchOutcome {
                    outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
                    latency,
                    partitions: Vec::new(),
                }),
                false,
            )
            .expect("delivered dispatch result");

        assert_eq!(producer.take_dispatch_latency_samples(), vec![latency]);
        assert!(producer.take_dispatch_latency_samples().is_empty());
    }

    #[tokio::test]
    async fn facade_transaction_wrappers_fail_locally_without_transactional_id() {
        let mut producer = producer(1);

        assert!(matches!(
            producer.init_transactions().await,
            Err(ProducerError::TransactionalIdRequired)
        ));
        assert_eq!(producer.metrics().transaction_init_count, 0);
        assert!(matches!(
            producer.begin_transaction(),
            Err(ProducerError::TransactionalIdRequired)
        ));
        assert_eq!(producer.metrics().transaction_begin_count, 0);
        assert!(matches!(
            producer.abort_transaction().await,
            Err(ProducerError::TransactionalIdRequired)
        ));
        assert_eq!(producer.metrics().transaction_abort_count, 0);
        assert!(matches!(
            producer.commit_transaction().await,
            Err(ProducerError::TransactionalIdRequired)
        ));
        assert_eq!(producer.metrics().transaction_commit_count, 0);
        producer.close().await.expect("closing empty producer");
    }

    #[tokio::test]
    async fn commit_transaction_reports_transaction_error_before_flush_like_java() {
        let mut config = runtime_config(1);
        config.idempotence = ProducerIdempotenceConfig {
            enabled: true,
            transactional_id: Some("txn-orders".to_owned()),
            transaction_timeout_ms: 60_000,
            transaction_two_phase_commit: false,
        };
        let mut producer = Producer::from_parts(test_wire(), config);
        producer
            .dispatcher
            .set_abortable_transaction_error_for_test(ErrorCode::UnknownProducerId)
            .await;
        producer
            .accumulator
            .append(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")))
            .expect("buffered record");

        assert!(matches!(
            producer.commit_transaction().await,
            Err(ProducerError::Transaction {
                operation: "transaction_state",
                error: ErrorCode::UnknownProducerId,
            })
        ));
        assert_eq!(producer.metrics().transaction_commit_count, 0);
    }

    #[tokio::test]
    async fn commit_transaction_reports_local_transaction_state_before_flush_like_java() {
        let mut producer = producer(1);
        producer
            .accumulator
            .append(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")))
            .expect("buffered record");

        assert!(matches!(
            producer.commit_transaction().await,
            Err(ProducerError::TransactionalIdRequired)
        ));
        assert_eq!(producer.metrics().transaction_commit_count, 0);

        let mut config = runtime_config(1);
        config.idempotence = ProducerIdempotenceConfig {
            enabled: true,
            transactional_id: Some("txn-orders".to_owned()),
            transaction_timeout_ms: 60_000,
            transaction_two_phase_commit: false,
        };
        let mut producer = Producer::from_parts(test_wire(), config);
        producer
            .accumulator
            .append(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")))
            .expect("buffered record");

        assert!(matches!(
            producer.commit_transaction().await,
            Err(ProducerError::InvalidTransactionState(
                "no transaction is open"
            ))
        ));
        assert_eq!(producer.metrics().transaction_commit_count, 0);
    }

    #[tokio::test]
    async fn abort_transaction_drops_buffered_records_like_java() {
        let mut config = runtime_config(1);
        config.idempotence = ProducerIdempotenceConfig {
            enabled: true,
            transactional_id: Some("txn-orders".to_owned()),
            transaction_timeout_ms: 60_000,
            transaction_two_phase_commit: false,
        };
        let mut producer = Producer::from_parts(test_wire(), config);
        producer
            .dispatcher
            .set_open_transaction_for_test(
                ProducerIdentity {
                    producer_id: 11,
                    producer_epoch: 2,
                },
                false,
            )
            .await;
        let delivery = producer
            .send(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")))
            .await
            .expect("buffered record");

        producer
            .abort_transaction()
            .await
            .expect("abort empty transaction");

        assert_eq!(producer.buffered_bytes(), 0);
        assert!(matches!(
            delivery.await,
            Err(ProducerError::DeliveryDropped)
        ));
        assert_eq!(producer.metrics().transaction_abort_count, 1);
    }

    #[tokio::test]
    async fn client_instance_id_is_stable_per_producer() {
        let producer = producer(1);
        let cached_id = KafkaUuid::from_parts(0x1111_2222_3333_4444, 0x5555_6666_7777_8888);
        {
            let mut client_instance_id = producer
                .client_instance_id
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            *client_instance_id = cached_id;
        }
        let first = producer
            .client_instance_id(Duration::from_secs(1))
            .await
            .expect("client instance id");
        let second = producer
            .client_instance_id(Duration::from_secs(1))
            .await
            .expect("client instance id");

        assert_eq!(first, second);
        assert_eq!(first, cached_id);
        assert!(!first.is_reserved());
    }

    #[tokio::test]
    async fn client_instance_id_reports_disabled_telemetry_like_java() {
        let producer = Producer::from_parts(
            test_wire(),
            ProducerRuntimeConfig {
                enable_metrics_push: false,
                ..runtime_config(1)
            },
        );

        assert!(matches!(
            producer.client_instance_id(Duration::ZERO).await,
            Err(ProducerError::TelemetryDisabled)
        ));
    }

    #[tokio::test]
    async fn client_instance_id_timeout_ms_rejects_negative_timeout_like_java() {
        let producer = producer(1);

        assert!(matches!(
            producer.client_instance_id_timeout_ms(-1).await,
            Err(ProducerError::InvalidTelemetryTimeout { timeout_ms: -1 })
        ));
    }

    #[test]
    fn register_metric_subscription_is_noop_when_telemetry_disabled_like_java() {
        let mut producer = Producer::from_parts(
            test_wire(),
            ProducerRuntimeConfig {
                enable_metrics_push: false,
                ..runtime_config(1)
            },
        );
        let subscription = ProducerMetricSubscription::new("io.wait.ratio");

        producer.register_metric_for_subscription(subscription);

        assert!(producer.metric_subscriptions.is_empty());
    }

    #[test]
    fn register_and_unregister_metric_subscription_updates_local_registry() {
        let mut producer = producer(1);
        let subscription = ProducerMetricSubscription::new("io.wait.ratio");

        producer.register_metric_for_subscription(subscription.clone());
        assert!(producer.metric_subscriptions.contains("io.wait.ratio"));

        producer.unregister_metric_from_subscription(&subscription);
        assert!(!producer.metric_subscriptions.contains("io.wait.ratio"));
    }

    #[test]
    fn metric_subscription_skips_existing_producer_metrics_like_java() {
        let mut producer = producer(1);
        let subscription = ProducerMetricSubscription::new("produce_request_count");

        producer.register_metric_for_subscription(subscription.clone());
        assert!(
            !producer
                .metric_subscriptions
                .contains("produce_request_count")
        );

        let _inserted = producer
            .metric_subscriptions
            .insert("produce_request_count".to_owned());
        producer.unregister_metric_from_subscription(&subscription);
        assert!(
            producer
                .metric_subscriptions
                .contains("produce_request_count")
        );
    }

    #[test]
    fn metrics_registry_exposes_named_snapshot_values_like_java_metrics_map() {
        let mut producer = producer(1);
        producer
            .accumulator
            .append(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")))
            .expect("buffered record");

        let registry = producer.metrics_registry();

        assert_eq!(
            registry.get("records_appended"),
            Some(&ProducerMetricValue::Count(1))
        );
        assert_eq!(
            registry.get("queue_depth_records"),
            Some(&ProducerMetricValue::Gauge(1))
        );
        assert!(matches!(
            registry.get("flush_total_latency"),
            Some(ProducerMetricValue::Duration(_))
        ));
        assert!(matches!(
            registry.get("average_batch_fill_ratio"),
            Some(ProducerMetricValue::Ratio(0.0))
        ));
        assert!(!registry.contains_key("custom.orders.sent"));
    }

    #[tokio::test]
    async fn close_timeout_succeeds_for_empty_producer() {
        let producer = producer(1);

        producer
            .close_timeout(Duration::from_millis(10))
            .await
            .expect("closing empty producer with timeout");
    }

    #[tokio::test]
    async fn close_timeout_ms_rejects_negative_timeout_like_java() {
        let producer = producer(1);

        assert!(matches!(
            producer.close_timeout_ms(-1).await,
            Err(ProducerError::InvalidCloseTimeout { timeout_ms: -1 })
        ));
    }

    #[test]
    fn flush_from_delivery_callback_is_rejected_like_java() {
        let producer = Arc::new(Mutex::new(producer(1)));
        let runtime = Arc::new(
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("test runtime"),
        );
        let rejected = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let (sender, delivery) = SendFuture::channel();
        let callback_producer = Arc::clone(&producer);
        let callback_runtime = Arc::clone(&runtime);
        let callback_rejected = Arc::clone(&rejected);
        delivery.register_callback(Box::new(move |_result| {
            let result = {
                let mut producer = callback_producer
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                callback_runtime.block_on(producer.flush())
            };
            callback_rejected.store(
                matches!(
                    result,
                    Err(ProducerError::CallbackOperation { operation: "flush" })
                ),
                Ordering::Relaxed,
            );
        }));

        sender.send(RecordMetadata {
            topic: Arc::from("orders"),
            partition: 0,
            leader_id: 7,
            offset: 0,
            timestamp_ms: 0,
            serialized_key_size: -1,
            serialized_value_size: -1,
        });
        assert!(rejected.load(Ordering::Relaxed));
    }

    #[test]
    fn close_timeout_from_delivery_callback_forces_zero_timeout_like_java() {
        let producer = Arc::new(Mutex::new(Some(producer(1))));
        let runtime = Arc::new(
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("test runtime"),
        );
        let forced_close = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let (sender, delivery) = SendFuture::channel();
        let callback_producer = Arc::clone(&producer);
        let callback_runtime = Arc::clone(&runtime);
        let callback_forced_close = Arc::clone(&forced_close);
        delivery.register_callback(Box::new(move |_result| {
            let producer = callback_producer
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take()
                .expect("producer available to callback");
            let result = callback_runtime.block_on(producer.close_timeout(Duration::from_secs(30)));
            callback_forced_close.store(result.is_ok(), Ordering::Relaxed);
        }));

        sender.send(record_metadata(0));

        assert!(forced_close.load(Ordering::Relaxed));
        assert!(
            producer
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_none()
        );
    }

    #[test]
    fn close_from_delivery_callback_forces_zero_timeout_like_java() {
        let producer = Arc::new(Mutex::new(Some(producer(1))));
        let runtime = Arc::new(
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("test runtime"),
        );
        let forced_close = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let (sender, delivery) = SendFuture::channel();
        let callback_producer = Arc::clone(&producer);
        let callback_runtime = Arc::clone(&runtime);
        let callback_forced_close = Arc::clone(&forced_close);
        delivery.register_callback(Box::new(move |_result| {
            let producer = callback_producer
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take()
                .expect("producer available to callback");
            let result = callback_runtime.block_on(producer.close());
            callback_forced_close.store(result.is_ok(), Ordering::Relaxed);
        }));

        sender.send(record_metadata(0));

        assert!(forced_close.load(Ordering::Relaxed));
        assert!(
            producer
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_none()
        );
    }

    #[tokio::test]
    async fn close_now_drops_buffered_records_without_flushing() {
        let mut producer = producer(1);
        let _delivery = producer
            .send(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")))
            .await
            .expect("send buffered record");

        producer.close_now();
    }

    #[test]
    fn close_now_closes_native_interceptors_like_java() {
        let close_count = Arc::new(AtomicUsize::new(0));
        let mut producer = producer(1);
        producer.add_interceptor(ClosingInterceptor {
            close_count: Arc::clone(&close_count),
        });

        producer.close_now();

        assert_eq!(close_count.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn close_now_ignores_interceptor_close_panic_like_java() {
        let close_count = Arc::new(AtomicUsize::new(0));
        let mut producer = producer(1);
        producer.add_interceptor(PanickingCloseInterceptor);
        producer.add_interceptor(ClosingInterceptor {
            close_count: Arc::clone(&close_count),
        });

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            producer.close_now();
        }));

        assert!(result.is_ok());
        assert_eq!(close_count.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn close_timeout_zero_drops_buffered_records_like_java() {
        let mut producer = producer(1);
        let _delivery = producer
            .send(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")))
            .await
            .expect("send buffered record");

        producer
            .close_timeout(Duration::ZERO)
            .await
            .expect("zero-timeout close should force close without flushing");
    }

    #[tokio::test]
    async fn flush_records_local_metrics_like_java() {
        let mut producer = producer(1);

        assert_eq!(producer.metrics().flush_count, 0);
        producer.flush().await.expect("empty flush");

        assert_eq!(producer.metrics().flush_count, 1);
        assert!(producer.metrics().flush_total_latency >= Duration::ZERO);
    }

    #[tokio::test]
    async fn send_offsets_to_transaction_requires_transactional_id() {
        let producer = producer(1);

        assert!(matches!(
            producer
                .send_offsets_to_transaction(
                    [(
                        TopicPartition::new("orders", 0),
                        crate::producer::OffsetAndMetadata::new(7)
                    )],
                    ConsumerGroupMetadata::new("group-a"),
                )
                .await,
            Err(ProducerError::TransactionalIdRequired)
        ));
        assert_eq!(producer.metrics().send_offsets_to_transaction_count, 0);
    }

    #[tokio::test]
    async fn send_offsets_to_transaction_empty_offsets_still_requires_transactional_id_like_java() {
        let producer = producer(1);

        assert!(matches!(
            producer
                .send_offsets_to_transaction([], ConsumerGroupMetadata::new("group-a"),)
                .await,
            Err(ProducerError::TransactionalIdRequired)
        ));
        assert_eq!(producer.metrics().send_offsets_to_transaction_count, 0);
    }

    #[tokio::test]
    async fn send_offsets_to_transaction_empty_offsets_validate_group_metadata_first_like_java() {
        let producer = producer(1);

        assert!(matches!(
            producer
                .send_offsets_to_transaction(
                    [],
                    ConsumerGroupMetadata::new("group-a").generation_id(42),
                )
                .await,
            Err(ProducerError::InvalidConsumerGroupMetadata(message))
                if message == "generation_id > 0 requires a known member_id"
        ));
        assert_eq!(producer.metrics().send_offsets_to_transaction_count, 0);
    }

    #[tokio::test]
    async fn send_offsets_to_transaction_requires_initialized_open_transaction() {
        let mut config = runtime_config(1);
        config.idempotence = ProducerIdempotenceConfig {
            enabled: true,
            transactional_id: Some("txn-orders".to_owned()),
            transaction_timeout_ms: 60_000,
            transaction_two_phase_commit: false,
        };
        let producer = Producer::from_parts(test_wire(), config);

        assert!(matches!(
            producer
                .send_offsets_to_transaction(
                    [(
                        TopicPartition::new("orders", 0),
                        crate::producer::OffsetAndMetadata::new(7)
                    )],
                    ConsumerGroupMetadata::new("group-a"),
                )
                .await,
            Err(ProducerError::InvalidTransactionState(
                "init_transactions must run before send_offsets_to_transaction"
            ))
        ));
    }

    #[tokio::test]
    async fn send_and_batch_apis_surface_backpressure_before_dispatch() {
        let mut config = runtime_config(1);
        config.accumulator = AccumulatorConfig::default().buffer_memory(1);
        let mut producer = Producer::from_parts(test_wire(), config);

        assert!(matches!(
            producer
                .send(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")))
                .await,
            Err(ProducerError::Backpressure)
        ));
        assert!(matches!(
            producer
                .send_batch([ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value"))])
                .await,
            Err(ProducerError::Backpressure)
        ));
        assert!(matches!(
            producer
                .send_batch_untracked([
                    ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value"))
                ])
                .await,
            Err(ProducerError::Backpressure)
        ));
    }

    #[tokio::test]
    async fn send_apis_reject_records_larger_than_max_request_size() {
        let mut config = runtime_config(1);
        config.max_request_size = 8;
        let mut producer = Producer::from_parts(test_wire(), config);

        let error = producer
            .send(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")))
            .await
            .expect_err("record should exceed max.request.size");

        assert!(matches!(
            error,
            ProducerError::RecordTooLarge {
                max_request_size: 8,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn send_waits_until_max_block_before_reporting_buffer_backpressure() {
        let mut config = runtime_config(1);
        config.accumulator = AccumulatorConfig::default()
            .batch_size(usize::MAX)
            .linger(Duration::from_mins(1))
            .buffer_memory(80);
        config.max_block = Duration::from_millis(10);
        let mut producer = Producer::from_parts(test_wire(), config);

        let first = producer
            .send(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")))
            .await;
        let started = Instant::now();
        let second = producer
            .send(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"b")))
            .await;

        assert!(first.is_ok());
        assert!(matches!(second, Err(ProducerError::Backpressure)));
        assert!(started.elapsed() >= Duration::from_millis(10));
    }

    #[tokio::test]
    async fn builder_exposes_client_config_and_build_errors_on_missing_bootstrap() {
        let builder = ProducerBuilder::new().set("client.id", "producer-a");

        assert_eq!(
            builder.client_config().get("client.id"),
            Some(&crate::config::ConfigValue::new("producer-a"))
        );
        assert!(matches!(
            Producer::builder().build().await,
            Err(ProducerError::Config { .. })
        ));
        assert!(matches!(
            Producer::new(ClientConfig::new()).await,
            Err(ProducerError::Config { .. })
        ));
    }

    #[tokio::test]
    async fn resolve_bootstrap_brokers_reports_dns_errors() {
        let config = crate::config::ProducerConfig::builder()
            .bootstrap_servers("invalid.invalid:9092")
            .build()
            .expect("producer config");

        assert!(matches!(
            resolve_bootstrap_brokers(&config).await,
            Err(ProducerError::Wire(_))
        ));
    }
}
