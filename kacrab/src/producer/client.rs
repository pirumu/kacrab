//! Public producer facade built from accumulator and dispatcher components.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc, OnceLock, RwLock,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use bytes::Bytes;
use kacrab_protocol::{
    KafkaUuid,
    generated::{
        ApiKey, ErrorCode, GetTelemetrySubscriptionsRequestData,
        GetTelemetrySubscriptionsResponseData, PushTelemetryRequestData, PushTelemetryResponseData,
    },
    version::client_api_info,
};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

#[cfg(test)]
use super::dispatcher::DispatchOutcome;
#[cfg(test)]
use super::sender::{CompletedDispatch, TimedDispatchOutcome};
use super::{
    accumulator::{RECORD_BATCH_OVERHEAD_BYTES, estimate_record_batch_bytes},
    api::{
        ConsumerGroupMetadata, OffsetAndMetadata, ProducerMetricSubscription,
        ProducerPartitionInfo, TopicPartition,
    },
    config::ProducerRuntimeConfig,
    dispatcher::ProducerDispatcher,
    error::{ProducerError, Result},
    interceptor::{ClusterResource, InterceptorConfigs, ProducerInterceptor, ProducerInterceptors},
    metrics::{
        KafkaMetric, MetricName, MetricReporter, ProducerMetricValue, ProducerMetrics,
        ProducerMetricsSnapshot, ProducerQueueMetrics,
    },
    partitioner::{ProducerPartitioner, ProducerPartitionerHandle},
    record::{
        DeliveryCallback, DeliverySender, ProducerRecord, RecordMetadata, SendFuture,
        clone_producer_error_for_delivery,
    },
    sender::{
        AppendCallbackDeliveryRecord, ProducerSender, ProducerSenderRuntime,
        ReadyDispatchObservers, SenderQueueSnapshot,
    },
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
    control_dispatcher: ProducerDispatcher,
    sender: ProducerSenderRuntime,
    max_block: std::time::Duration,
    max_request_size: usize,
    buffer_memory: usize,
    metrics: ProducerMetrics,
    metrics_enabled: bool,
    dispatch_latency_samples: std::sync::Mutex<Option<Vec<std::time::Duration>>>,
    client_instance_id: RwLock<KafkaUuid>,
    telemetry_subscription: RwLock<Option<TelemetrySubscription>>,
    enable_metrics_push: bool,
    telemetry_disabled: AtomicBool,
    metric_subscriptions: BTreeSet<String>,
    application_metrics: BTreeMap<MetricName, KafkaMetric>,
    interceptors: ProducerInterceptors,
    // Producer config handed to interceptor `configure` (client.id), and the last
    // cluster id reported to interceptor `on_update` (deduped so on_update fires only
    // when the cluster id first resolves or changes — Kafka ClusterResourceListener).
    interceptor_configs: InterceptorConfigs,
    last_cluster_id: Arc<RwLock<Option<String>>>,
    partitioner: ProducerPartitionerHandle,
    metric_reporters: Vec<Arc<dyn MetricReporter>>,
    // Lazily-spawned FIFO drain for the rare synchronous-send slow path (cold
    // metadata, buffer-full, or custom-partitioner records).
    // `send`/`send_with_callback` are synchronous like Kafka's `Producer.send`;
    // records that cannot append synchronously are handed to this drain so
    // per-partition append order is preserved without blocking the caller thread.
    slow_send: OnceLock<SlowSendHandle>,
}

#[derive(Debug, Clone)]
struct TelemetrySubscription {
    client_instance_id: KafkaUuid,
    subscription_id: i32,
    accepted_compression_types: Vec<i8>,
    telemetry_max_bytes: i32,
}

impl Producer {
    /// Build a producer from an existing wire client and runtime config.
    #[must_use]
    pub fn from_parts(wire: WireClient, config: ProducerRuntimeConfig) -> Self {
        let max_block = config.max_block;
        let max_request_size = config.max_request_size;
        let enable_metrics_push = config.enable_metrics_push;
        let accumulator_config = config.accumulator;
        let buffer_memory = accumulator_config.buffer_memory;
        let max_in_flight_requests = config.max_in_flight_requests_per_connection;
        let idempotent_ordering = config.idempotence.enabled;
        let dispatcher = ProducerDispatcher::with_config(wire, config);
        let (sender, metrics) = ProducerSenderRuntime::with_dispatcher(
            accumulator_config,
            max_in_flight_requests,
            idempotent_ordering,
            dispatcher.clone(),
        );
        Self {
            control_dispatcher: dispatcher,
            sender,
            max_block,
            max_request_size,
            buffer_memory,
            metrics,
            metrics_enabled: false,
            dispatch_latency_samples: std::sync::Mutex::new(None),
            client_instance_id: RwLock::new(KafkaUuid::ZERO),
            telemetry_subscription: RwLock::new(None),
            enable_metrics_push,
            telemetry_disabled: AtomicBool::new(false),
            metric_subscriptions: BTreeSet::new(),
            application_metrics: BTreeMap::new(),
            interceptors: ProducerInterceptors::default(),
            interceptor_configs: InterceptorConfigs::default(),
            last_cluster_id: Arc::new(RwLock::new(None)),
            partitioner: ProducerPartitionerHandle::default(),
            metric_reporters: Vec::new(),
            slow_send: OnceLock::new(),
        }
    }

    fn ensure_background_sender_loop(&self) {
        self.sender.ensure_loop_running();
    }

    fn control_dispatcher(&self) -> ProducerDispatcher {
        self.control_dispatcher.clone()
    }

    /// Creates a producer builder.
    #[must_use]
    pub fn builder() -> ProducerBuilder {
        ProducerBuilder::new()
    }

    /// Build a producer from an ergonomic Kafka client config.
    ///
    /// # Errors
    ///
    /// Returns an error when config validation, DNS resolution, or connection
    /// setup preparation fails.
    pub async fn new(config: ClientConfig) -> Result<Self> {
        Self::from_client_config(&config).await
    }

    /// Build a producer from borrowed Kafka client config.
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
        let key_serializer = KS::from_client_config(config, true)?;
        let value_serializer = VS::from_client_config(config, false)?;
        let producer = Self::from_client_config_with_native_serializers(config).await?;
        Ok(Self::from_parts_with_serializers(
            producer,
            key_serializer,
            value_serializer,
        ))
    }

    /// Build a producer from `Properties`-style entries.
    ///
    /// # Errors
    ///
    /// Returns an error when config validation, DNS resolution, or connection
    /// setup preparation fails.
    pub async fn from_properties(properties: Properties) -> Result<Self> {
        let config = ClientConfig::from(properties);
        Self::from_client_config(&config).await
    }

    /// Build a producer from a map/iterator of Kafka config entries.
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

    /// Constructor accepting key/value serializers.
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

    /// Map constructor accepting key/value serializers.
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

    /// Map constructor that loads built-in native serializers from
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

    /// Properties constructor that loads built-in native serializers
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
        let connection = config
            .to_connection_config()
            .map_err(|error| ProducerError::Config { error })?;
        let wire = WireClient::connect_with_brokers(connection, config.client_id, endpoints);
        Ok(Self::from_parts(wire, runtime))
    }

    /// Append one record, returning a delivery future — synchronous like Kafka's
    /// `Producer.send(record)`. A record whose partition resolves synchronously is
    /// appended inline with zero per-record `.await`; the rare record that needs
    /// the network (cold metadata), must wait for buffer space, or belongs to a
    /// custom-partitioner producer is handed to a FIFO drain that preserves
    /// per-partition order without blocking the caller's thread.
    ///
    /// # Errors
    ///
    /// Returns producer backpressure or record-validation errors.
    pub fn send(&self, record: ProducerRecord) -> Result<SendFuture> {
        self.send_with_optional_callback(record, None)
    }

    /// Append one record with a completion callback — synchronous like
    /// Kafka's `Producer.send(record, callback)`; the returned future can still be
    /// awaited.
    ///
    /// # Errors
    ///
    /// Returns producer backpressure or record-validation errors.
    pub fn send_with_callback<F>(&self, record: ProducerRecord, callback: F) -> Result<SendFuture>
    where
        F: FnOnce(Result<RecordMetadata>) + Send + 'static,
    {
        self.send_with_optional_callback(record, Some(Box::new(callback)))
    }

    fn send_with_optional_callback(
        &self,
        record: ProducerRecord,
        callback: Option<DeliveryCallback>,
    ) -> Result<SendFuture> {
        self.ensure_background_sender_loop();
        let mut record = self.intercept_on_send(record);
        // Kafka throws fatal transaction errors synchronously from send(); guard
        // before appending. On momentary lock contention, take the slow drain
        // (which performs the awaiting guard).
        if self.control_dispatcher.is_transactional() {
            match self.control_dispatcher.try_fail_if_transaction_error_now() {
                Some(Ok(())) => {},
                Some(Err(error)) => {
                    let error_record = self.error_record_snapshot(&record);
                    self.run_local_send_error(callback, error_record.as_ref(), &error);
                    return Err(error);
                },
                None => return self.enqueue_slow_send(record, callback),
            }
        }
        // Fast synchronous path: default partitioner, nothing queued ahead, and the
        // partition resolves synchronously (cached sticky reuse, rotation, keyed).
        // Transactional sends use this path too, so their records are appended
        // synchronously before any abort can drop the buffer.
        if self.can_append_synchronously() && self.try_assign_partition_now(&mut record) {
            return self.append_assigned_now(record, callback);
        }
        // Slow path: hand off to the FIFO drain so cold-metadata / custom-partitioner
        // / contended records keep per-partition order without blocking the caller.
        self.enqueue_slow_send(record, callback)
    }

    fn can_append_synchronously(&self) -> bool {
        !self.partitioner.is_some()
            && self
                .slow_send
                .get()
                .is_none_or(|handle| handle.pending.load(Ordering::Acquire) == 0)
    }

    /// Synchronously assign this record's partition with the real sticky
    /// partitioner (cached sticky reuse, rotation, or keyed) without blocking.
    /// Returns `false` only on genuinely cold metadata or momentary lock
    /// contention — the caller then takes the slow drain path.
    fn try_assign_partition_now(&self, record: &mut ProducerRecord) -> bool {
        record.has_assigned_partition()
            || self
                .control_dispatcher
                .try_assign_cached_sticky_partition_now(record)
    }

    /// Run a record's user callback (when the error is callback-eligible) and then
    /// the `on_error` interceptor, matching Kafka's local-error ordering where the
    /// completion callback fires before interceptors observe the failure.
    fn run_local_send_error(
        &self,
        callback: Option<DeliveryCallback>,
        error_record: Option<&ProducerRecord>,
        error: &ProducerError,
    ) {
        if let (Some(callback_error), Some(callback)) =
            (producer_error_for_callback(error), callback)
        {
            callback(Err(callback_error));
        }
        if let Some(error_record) = error_record {
            self.interceptors.on_error(error_record, error);
        }
    }

    /// Append a record whose partition is already assigned, synchronously: the
    /// lock-free bypass append plus an immediately-returned delivery future, with
    /// zero per-record `.await`. Surfaces a still-full buffer as backpressure and
    /// runs the user callback + `on_error` interceptor on any local failure.
    fn append_assigned_now(
        &self,
        record: ProducerRecord,
        callback: Option<DeliveryCallback>,
    ) -> Result<SendFuture> {
        let now = std::time::Instant::now();
        debug_assert!(
            record.has_assigned_partition(),
            "append_assigned_now requires a pre-assigned partition"
        );
        let error_record = self.error_record_snapshot(&record);
        if let Err(error) = self.validate_record_size(&record) {
            self.run_local_send_error(callback, error_record.as_ref(), &error);
            return Err(error);
        }
        let deadline = now.checked_add(self.max_block).unwrap_or(now);
        let mut ack_headers = self.interceptor_headers(&record);
        let interceptors = self.interceptors.clone();
        let mut callback = callback;
        let before_dispatch = |delivery: &SendFuture| {
            register_delivery_observers(
                delivery,
                ack_headers.take(),
                &interceptors,
                callback.take(),
            );
        };
        let append = AppendCallbackDeliveryRecord::new(record, now, deadline, None);
        let result = self
            .sender
            .append_callback_now(append, before_dispatch)
            .unwrap_or(Err(ProducerError::Backpressure));
        if let Err(error) = &result {
            // `before_dispatch` only runs once a delivery is created, so on a local
            // failure (e.g. buffer backpressure) the callback was not consumed.
            self.run_local_send_error(callback.take(), error_record.as_ref(), error);
        }
        result
    }

    fn enqueue_slow_send(
        &self,
        record: ProducerRecord,
        callback: Option<DeliveryCallback>,
    ) -> Result<SendFuture> {
        let handle = self.slow_send.get_or_init(|| self.spawn_slow_send_drain());
        let (proxy_sender, proxy) =
            SendFuture::channel_for_record_with_metadata_capacity(&record, 1);
        let error_record = self.error_record_snapshot(&record);
        // Count this record as queued BEFORE it is sent so a concurrent fast-path
        // send observes a non-zero `pending` and queues behind it, preserving
        // per-partition append order. The drain decrements only after appending.
        let _previous = handle.pending.fetch_add(1, Ordering::AcqRel);
        let slow = SlowSend {
            record,
            callback,
            error_record,
            proxy: proxy_sender,
            enqueued_at: std::time::Instant::now(),
        };
        if handle.tx.send(slow).is_err() {
            let _previous = handle.pending.fetch_sub(1, Ordering::AcqRel);
            return Err(ProducerError::Backpressure);
        }
        Ok(proxy)
    }

    fn spawn_slow_send_drain(&self) -> SlowSendHandle {
        let pending = Arc::new(AtomicUsize::new(0));
        let (tx, rx) = unbounded_channel::<SlowSend>();
        let context = SlowSendContext {
            control_dispatcher: self.control_dispatcher.clone(),
            sender: self.sender.shared_sender(),
            partitioner: self.partitioner.clone(),
            interceptors: self.interceptors.clone(),
            last_cluster_id: Arc::clone(&self.last_cluster_id),
            max_block: self.max_block,
            max_request_size: self.max_request_size,
            buffer_memory: self.buffer_memory,
        };
        let _drain = tokio::spawn(run_slow_send_drain(rx, context, Arc::clone(&pending)));
        SlowSendHandle { tx, pending }
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
        self.ensure_background_sender_loop();
        // Drain any records queued on the synchronous-send slow path so they are
        // appended to the accumulator before the flush dispatches buffered batches
        // (Kafka flush() waits for every prior send to complete).
        self.wait_for_slow_send_drain().await;
        self.drive_flush_until_complete().await
    }

    /// Yield until every record handed to the synchronous-send slow drain has been
    /// appended (or failed). A non-zero pending count also gates the fast path, so
    /// waiting here lets `flush` reset both back to the inline append path.
    async fn wait_for_slow_send_drain(&self) {
        if let Some(handle) = self.slow_send.get() {
            while handle.pending.load(Ordering::Acquire) > 0 {
                tokio::task::yield_now().await;
            }
        }
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
        let dispatcher = self.control_dispatcher();
        let timeout_dispatcher = dispatcher.clone();
        let mut task = tokio::spawn(async move { dispatcher.init_transactions().await });
        match tokio::time::timeout(self.max_block, &mut task).await {
            Ok(joined) => joined.map_err(|error| ProducerError::DispatchTask(error.to_string()))?,
            Err(_elapsed) => {
                timeout_dispatcher.mark_init_transactions_timed_out().await;
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
        let result = self.control_dispatcher.begin_transaction();
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
        let dispatcher = self.control_dispatcher();
        let retry_pending_commit = dispatcher.pending_end_transaction_matches(true).await?;
        if !retry_pending_commit {
            dispatcher.validate_commit_transaction_start()?;
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
        let dispatcher = self.control_dispatcher();
        let retry_pending_abort = dispatcher.pending_end_transaction_matches(false).await?;
        if !retry_pending_abort {
            dispatcher
                .fail_if_fatal_transaction_error_for_abort()
                .await?;
            let _dropped_batches = self.sender.lock().await.discard_buffered_batches();
            self.wait_for_abort_completion().await?;
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
        let dispatcher = self.control_dispatcher();
        let timeout_dispatcher = dispatcher.clone();
        let mut task = tokio::spawn(async move { dispatcher.end_transaction(committed).await });
        match tokio::time::timeout(self.max_block, &mut task).await {
            Ok(joined) => joined.map_err(|error| ProducerError::DispatchTask(error.to_string()))?,
            Err(_elapsed) => {
                timeout_dispatcher
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
            let dispatcher = self.control_dispatcher();
            return dispatcher
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
        let dispatcher = self.control_dispatcher();
        let timeout_dispatcher = dispatcher.clone();
        let mut task = tokio::spawn(async move {
            dispatcher
                .send_offsets_to_transaction(offsets, group_metadata)
                .await
        });
        match tokio::time::timeout(self.max_block, &mut task).await {
            Ok(joined) => joined.map_err(|error| ProducerError::DispatchTask(error.to_string()))?,
            Err(_elapsed) => {
                timeout_dispatcher
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
    /// Mirrors Kafka producer close with a zero timeout: buffered records are
    /// aborted with a [`ProducerError::ProducerClosed`] error (Kafka's forced
    /// close fails incomplete batches) rather than silently dropped.
    pub fn close_now(self) {
        if let Ok(sender) = self.sender.try_lock() {
            let _aborted = sender.fail_buffered_batches(&ProducerError::ProducerClosed);
        }
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

    /// Flush buffered records and consume the producer using a
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
    pub fn buffered_bytes(&self) -> usize {
        self.sender
            .try_lock()
            .map_or(0, |sender| sender.buffered_bytes())
    }

    /// Point-in-time producer metrics snapshot.
    #[must_use]
    pub fn metrics(&self) -> ProducerMetricsSnapshot {
        let queue = self.sender.try_lock().map_or_else(
            |_| SenderQueueSnapshot::default(),
            |sender| sender.queue_snapshot(),
        );
        self.metrics.snapshot(ProducerQueueMetrics {
            queue_depth_bytes: queue.buffered_bytes,
            queue_depth_records: queue.buffered_records,
            buffer_available_bytes: queue.buffer_available_bytes,
            incomplete_batches: queue.incomplete_batches,
            in_flight_dispatches: queue.in_flight_dispatches,
        })
    }

    /// Point-in-time named producer metrics, similar to Kafka's metrics map.
    #[must_use]
    pub fn metrics_registry(&self) -> BTreeMap<&'static str, ProducerMetricValue> {
        self.metrics().as_metric_map()
    }

    /// Snapshot producer metrics under their Kafka metric names, mirroring Kafka's
    /// `SenderMetricsRegistry`. Keys are formatted as `group:name[:tag=value]`
    /// (e.g. `producer-metrics:record-send-rate`,
    /// `producer-topic-metrics:record-send-total:topic=orders`).
    #[must_use]
    pub fn kafka_metrics(&self) -> BTreeMap<String, f64> {
        // Refresh point-in-time gauges from the current sender queue + metadata.
        if let Ok(sender) = self.sender.try_lock() {
            let snapshot = sender.queue_snapshot();
            self.metrics
                .set_requests_in_flight(snapshot.in_flight_dispatches);
            self.metrics
                .set_buffer_gauges(snapshot.buffer_available_bytes);
        }
        if let Some(age) = self.control_dispatcher.metadata_age() {
            self.metrics.set_metadata_age(age);
        }
        self.metrics.kafka_metrics()
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
        let metadata = self
            .sender
            .lock()
            .await
            .metadata_for_topics([topic])
            .await?;
        self.metrics
            .record_metadata_wait(metadata_started_at.elapsed());
        self.notify_cluster_metadata(&metadata);
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
    /// `enable.metrics.push=false`, matching Kafka `KafkaProducer`.
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
    /// This overload accepts a signed millisecond timeout so callers
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

                let dispatcher = {
                    let sender = self.sender.lock().await;
                    sender.control_dispatcher()
                };
                let broker_id = dispatcher.any_broker_id()?;
                let request = PushTelemetryRequestData {
                    client_instance_id: subscription.client_instance_id,
                    subscription_id: subscription.subscription_id,
                    terminating,
                    compression_type: 0,
                    metrics: metrics.clone(),
                    _unknown_tagged_fields: Vec::new(),
                };
                let version = client_api_info(ApiKey::PushTelemetry).max_version;
                let response: PushTelemetryResponseData = dispatcher
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
        let dispatcher = {
            let sender = self.sender.lock().await;
            sender.control_dispatcher()
        };
        let broker_id = dispatcher.any_broker_id()?;
        let request = GetTelemetrySubscriptionsRequestData {
            client_instance_id: cached_client_instance_id,
            _unknown_tagged_fields: Vec::new(),
        };
        let version = client_api_info(ApiKey::GetTelemetrySubscriptions).max_version;
        let response: GetTelemetrySubscriptionsResponseData = dispatcher
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
    pub fn enable_metrics(&mut self) {
        self.metrics_enabled = true;
        self.sender.enable_loop_metrics();
        self.control_dispatcher.enable_metrics();
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
    /// mirrors Kafka's reporter lifecycle for caller-provided metrics.
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
        let samples = self
            .dispatch_latency_samples
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if samples.is_none() {
            *samples = Some(Vec::new());
        }
    }

    /// Add a producer interceptor to this producer instance. The interceptor is
    /// `configure`d immediately with this producer's config (Kafka configures each
    /// interceptor once when it is created).
    pub fn add_interceptor(&mut self, interceptor: impl ProducerInterceptor) {
        self.interceptors
            .push_and_configure(interceptor, &self.interceptor_configs);
    }

    /// Notify interceptors of a cluster-metadata update (Kafka
    /// `ClusterResourceListener.onUpdate`), deduplicated so it fires only when the
    /// cluster id first resolves or changes. No-op when there are no interceptors.
    fn notify_cluster_metadata(&self, metadata: &ClusterMetadata) {
        notify_interceptors_cluster_update(&self.interceptors, &self.last_cluster_id, metadata);
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
    pub fn take_dispatch_latency_samples(&self) -> Vec<std::time::Duration> {
        self.dispatch_latency_samples
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_mut()
            .map(std::mem::take)
            .unwrap_or_default()
    }

    #[cfg(test)]
    fn collect_finished(&self) -> Result<()> {
        self.handle_finished_dispatches(false)
    }

    #[cfg(test)]
    fn collect_finished_for_flush(&self) -> Result<()> {
        self.handle_finished_dispatches(true)
    }

    #[cfg(test)]
    async fn wait_for_one(&self) -> Result<()> {
        self.wait_for_handled_dispatch(false).await
    }

    #[cfg(test)]
    async fn wait_for_one_for_flush(&self) -> Result<()> {
        self.wait_for_handled_dispatch(true).await
    }

    #[expect(
        clippy::needless_pass_by_ref_mut,
        reason = "part of the &mut self flush/abort control-plane surface; dispatch latency \
                  samples are now interior-mutable so no field is mutated directly here"
    )]
    async fn wait_for_abort_completion(&mut self) -> Result<()> {
        let dispatch_latency_samples = &self.dispatch_latency_samples;
        let metrics_enabled = self.metrics_enabled;
        let metrics = &self.metrics;
        self.sender
            .lock()
            .await
            .wait_for_abort_completion(
                |latency| {
                    push_dispatch_latency_sample(dispatch_latency_samples, latency);
                },
                || {
                    if metrics_enabled {
                        metrics.record_requeue();
                    }
                },
            )
            .await
    }

    #[expect(
        clippy::needless_pass_by_ref_mut,
        reason = "part of the &mut self flush control-plane surface; dispatch latency samples are \
                  now interior-mutable so no field is mutated directly here"
    )]
    async fn drive_flush_until_complete(&mut self) -> Result<()> {
        let dispatch_latency_samples = &self.dispatch_latency_samples;
        let metrics_enabled = self.metrics_enabled;
        let metrics = &self.metrics;
        self.sender
            .lock()
            .await
            .drive_flush_until_complete(ReadyDispatchObservers::new(
                |latency| {
                    push_dispatch_latency_sample(dispatch_latency_samples, latency);
                },
                || {
                    if metrics_enabled {
                        metrics.record_requeue();
                    }
                },
                |_: &[super::ReadyBatch]| {},
            ))
            .await
    }

    #[cfg(test)]
    fn dispatch_task_result(
        &self,
        result: Result<TimedDispatchOutcome>,
        requeue_is_error: bool,
    ) -> Result<()> {
        let dispatch_latency_samples = &self.dispatch_latency_samples;
        let metrics_enabled = self.metrics_enabled;
        let metrics = &self.metrics;
        let Ok(sender) = self.sender.try_lock() else {
            return Err(ProducerError::DispatchTask(
                "producer sender is busy".to_owned(),
            ));
        };
        sender.handle_completed_dispatch(
            CompletedDispatch::new(result, requeue_is_error),
            |latency| {
                push_dispatch_latency_sample(dispatch_latency_samples, latency);
            },
            || {
                if metrics_enabled {
                    metrics.record_requeue();
                }
            },
        )
    }

    #[cfg(test)]
    fn handle_finished_dispatches(&self, requeue_is_error: bool) -> Result<()> {
        let dispatch_latency_samples = &self.dispatch_latency_samples;
        let metrics_enabled = self.metrics_enabled;
        let metrics = &self.metrics;
        let Ok(mut sender) = self.sender.try_lock() else {
            return Err(ProducerError::DispatchTask(
                "producer sender is busy".to_owned(),
            ));
        };
        sender.handle_finished_dispatches(
            requeue_is_error,
            |latency| {
                push_dispatch_latency_sample(dispatch_latency_samples, latency);
            },
            || {
                if metrics_enabled {
                    metrics.record_requeue();
                }
            },
        )
    }

    #[cfg(test)]
    async fn wait_for_handled_dispatch(&self, requeue_is_error: bool) -> Result<()> {
        let dispatch_latency_samples = &self.dispatch_latency_samples;
        let metrics_enabled = self.metrics_enabled;
        let metrics = &self.metrics;
        self.sender
            .lock()
            .await
            .wait_for_handled_dispatch(
                requeue_is_error,
                |latency| {
                    push_dispatch_latency_sample(dispatch_latency_samples, latency);
                },
                || {
                    if metrics_enabled {
                        metrics.record_requeue();
                    }
                },
            )
            .await
    }

    fn intercept_on_send(&self, record: ProducerRecord) -> ProducerRecord {
        if self.interceptors.is_empty() {
            return record;
        }
        self.interceptors.on_send(record)
    }

    fn error_record_snapshot(&self, record: &ProducerRecord) -> Option<ProducerRecord> {
        (!self.interceptors.is_empty()).then(|| record.clone())
    }

    fn interceptor_headers(
        &self,
        record: &ProducerRecord,
    ) -> Option<Vec<kacrab_protocol::record::RecordHeader>> {
        (!self.interceptors.is_empty()).then(|| record.headers.clone())
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
        // Kafka ensureValidRecordSize: a record larger than the whole buffer can
        // never be allocated, so fail fast instead of blocking until max.block.ms.
        if estimated_size > self.buffer_memory {
            return Err(ProducerError::RecordExceedsBufferMemory {
                size: estimated_size,
                buffer_memory: self.buffer_memory,
            });
        }
        Ok(())
    }
}

/// Handle to the lazily-spawned synchronous-send slow-path drain.
#[derive(Debug)]
struct SlowSendHandle {
    tx: UnboundedSender<SlowSend>,
    /// Records enqueued-but-not-yet-appended; a non-zero count makes new
    /// fast-path sends queue behind them so per-partition append order is kept.
    pending: Arc<AtomicUsize>,
}

/// A record routed to the slow drain because it could not be appended
/// synchronously (cold metadata, full buffer, or custom partitioner).
struct SlowSend {
    record: ProducerRecord,
    callback: Option<DeliveryCallback>,
    error_record: Option<ProducerRecord>,
    proxy: DeliverySender,
    enqueued_at: std::time::Instant,
}

/// Deliver Kafka `ClusterResourceListener.onUpdate` to the interceptors, deduplicated
/// against the last-seen cluster id so it fires only when the cluster id first
/// resolves or changes. No-op when there are no interceptors.
fn notify_interceptors_cluster_update(
    interceptors: &ProducerInterceptors,
    last_cluster_id: &RwLock<Option<String>>,
    metadata: &ClusterMetadata,
) {
    if interceptors.is_empty() {
        return;
    }
    let cluster_id = metadata.cluster_id.clone();
    {
        let last = last_cluster_id
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if *last == cluster_id {
            return;
        }
    }
    let mut last = last_cluster_id
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if *last == cluster_id {
        return;
    }
    last.clone_from(&cluster_id);
    drop(last);
    interceptors.on_cluster_update(&ClusterResource { cluster_id });
}

/// Cloned handles the slow drain needs to assign + append without owning the
/// (non-`Clone`) producer runtime.
struct SlowSendContext {
    control_dispatcher: ProducerDispatcher,
    sender: Arc<tokio::sync::Mutex<ProducerSender>>,
    partitioner: ProducerPartitionerHandle,
    interceptors: ProducerInterceptors,
    last_cluster_id: Arc<RwLock<Option<String>>>,
    max_block: std::time::Duration,
    max_request_size: usize,
    buffer_memory: usize,
}

async fn run_slow_send_drain(
    mut rx: UnboundedReceiver<SlowSend>,
    context: SlowSendContext,
    pending: Arc<AtomicUsize>,
) {
    while let Some(slow) = rx.recv().await {
        context.process(slow).await;
        // Decrement only after the record has been appended (or failed), so a
        // fast-path send that observes `pending == 0` knows every queued record
        // is already ordered ahead of it in the accumulator.
        let _previous = pending.fetch_sub(1, Ordering::AcqRel);
    }
}

impl SlowSendContext {
    async fn process(&self, slow: SlowSend) {
        let SlowSend {
            mut record,
            callback,
            mut error_record,
            proxy,
            enqueued_at,
        } = slow;
        if let Err(error) = self.fail_if_transaction_error().await {
            self.complete_with_error(proxy, callback, error_record.as_ref(), &error);
            return;
        }
        // Assign the partition before validating size so a too-large record reports
        // its assigned partition to the on_error interceptor, matching Kafka's doSend
        // ordering (partition first, ensureValidRecordSize second).
        if let Err(error) = self.assign(&mut record).await {
            self.complete_with_error(proxy, callback, error_record.as_ref(), &error);
            return;
        }
        // Re-snapshot the record now that its partition is assigned so the on_error
        // interceptor observes the resolved partition.
        if !self.interceptors.is_empty() {
            error_record = Some(record.clone());
        }
        if let Err(error) = self.validate_record_size(&record) {
            self.complete_with_error(proxy, callback, error_record.as_ref(), &error);
            return;
        }
        let deadline = std::time::Instant::now()
            .checked_add(self.max_block)
            .unwrap_or(enqueued_at);
        let ack_headers = (!self.interceptors.is_empty()).then(|| record.headers.clone());
        // Register interceptor on_ack + user callback + proxy forwarding before
        // the batch can be dispatched. If the append fails before the delivery is
        // created, `before_dispatch` never runs and `pending_reg` is still `Some`,
        // so the proxy + callback are completed below instead of leaking forever.
        let mut pending_reg = Some((callback, proxy, ack_headers));
        let interceptors = self.interceptors.clone();
        let before_dispatch = |delivery: &SendFuture| {
            if let Some((callback, proxy, ack_headers)) = pending_reg.take() {
                register_delivery_observers(delivery, ack_headers, &interceptors, callback);
                delivery.register_callback(Box::new(move |result| match result {
                    Ok(metadata) => proxy.send(&metadata),
                    Err(error) => proxy.send_error(&error),
                }));
            }
        };
        let append = AppendCallbackDeliveryRecord::new(record, enqueued_at, deadline, None);
        let result = self
            .sender
            .lock()
            .await
            .append_callback_delivery_record_then_apply_dispatch(
                append,
                before_dispatch,
                ReadyDispatchObservers::new(|_| {}, || {}, |_: &[super::ReadyBatch]| {}),
            )
            .await;
        if let Some((callback, proxy, _)) = pending_reg.take() {
            let error = result.err().unwrap_or(ProducerError::Backpressure);
            proxy.send_error(&error);
            if let Some(callback) = callback {
                callback(Err(clone_producer_error_for_delivery(&error)));
            }
            if let Some(error_record) = error_record.as_ref() {
                self.interceptors.on_error(error_record, &error);
            }
        }
    }

    async fn fail_if_transaction_error(&self) -> Result<()> {
        if !self.control_dispatcher.is_transactional() {
            return Ok(());
        }
        self.control_dispatcher.fail_if_transaction_error().await
    }

    async fn assign(&self, record: &mut ProducerRecord) -> Result<()> {
        if record.has_assigned_partition() {
            return Ok(());
        }
        if !self.partitioner.is_some() {
            return self
                .sender
                .lock()
                .await
                .assign_partition_with_accumulator(record)
                .await;
        }
        let metadata = self
            .sender
            .lock()
            .await
            .metadata_for_topics([record.topic.as_ref()])
            .await?;
        notify_interceptors_cluster_update(&self.interceptors, &self.last_cluster_id, &metadata);
        if let Some(partition) = self.partitioner.partition(record, &metadata).transpose()? {
            validate_selected_partition(&metadata, record, partition)?;
            record.partition = partition;
        }
        if record.has_assigned_partition() {
            return Ok(());
        }
        self.sender
            .lock()
            .await
            .assign_partition_with_metadata(&metadata, record)
            .await
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
        if estimated_size > self.buffer_memory {
            return Err(ProducerError::RecordExceedsBufferMemory {
                size: estimated_size,
                buffer_memory: self.buffer_memory,
            });
        }
        Ok(())
    }

    fn complete_with_error(
        &self,
        proxy: DeliverySender,
        callback: Option<DeliveryCallback>,
        error_record: Option<&ProducerRecord>,
        error: &ProducerError,
    ) {
        proxy.send_error(error);
        if let Some(callback) = callback {
            callback(Err(clone_producer_error_for_delivery(error)));
        }
        if let Some(error_record) = error_record {
            self.interceptors.on_error(error_record, error);
        }
    }
}

fn register_delivery_observers(
    delivery: &SendFuture,
    ack_headers: Option<Vec<kacrab_protocol::record::RecordHeader>>,
    interceptors: &ProducerInterceptors,
    callback: Option<DeliveryCallback>,
) {
    if let Some(headers) = ack_headers {
        let interceptors = interceptors.clone();
        delivery.register_callback(Box::new(move |result| match result {
            Ok(metadata) => interceptors.on_ack(Some(&metadata), None, &headers),
            Err(error) => interceptors.on_ack(None, Some(&error), &headers),
        }));
    }
    if let Some(callback) = callback {
        delivery.register_callback(callback);
    }
}

impl Drop for Producer {
    fn drop(&mut self) {
        self.interceptors.close();
        self.partitioner.close();
        close_metric_reporters(&self.metric_reporters);
    }
}

/// Append a dispatch-latency sample to the shared diagnostic buffer, if latency
/// collection is enabled. Lock-poison and the disabled (`None`) state are both
/// treated as "skip", so the hot dispatch path never panics on this path.
fn push_dispatch_latency_sample(
    samples: &std::sync::Mutex<Option<Vec<std::time::Duration>>>,
    latency: std::time::Duration,
) {
    if let Ok(mut guard) = samples.lock()
        && let Some(samples) = guard.as_mut()
    {
        samples.push(latency);
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
        ProducerError::ProducerClosed => Some(ProducerError::ProducerClosed),
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
        ProducerError::RecordExceedsBufferMemory {
            size,
            buffer_memory,
        } => Some(ProducerError::RecordExceedsBufferMemory {
            size: *size,
            buffer_memory: *buffer_memory,
        }),
        ProducerError::FlushIncomplete => Some(ProducerError::FlushIncomplete),
        ProducerError::BatchLifecycle(message) => Some(ProducerError::BatchLifecycle(message)),
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

/// Builder for [`Producer`].
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

    /// Returns the underlying Kafka client config.
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
        let mut connection = config
            .to_connection_config()
            .map_err(|error| ProducerError::Config { error })?;
        connection.sasl.client_authenticator = sasl_client_authenticator;
        connection.sasl.client_authenticator_factory = sasl_client_authenticator_factory;
        let interceptor_configs = InterceptorConfigs {
            client_id: Some(config.client_id.clone()),
        };
        let wire = WireClient::connect_with_brokers(connection, config.client_id, endpoints);
        let mut producer = Producer::from_parts(wire, runtime);
        producer.interceptors = interceptors;
        // Kafka Configurable.configure: configure each interceptor once at construction,
        // passing the producer config (client.id).
        producer.interceptors.configure(&interceptor_configs);
        producer.interceptor_configs = interceptor_configs;
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
        let mut connection = config
            .to_connection_config()
            .map_err(|error| ProducerError::Config { error })?;
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
        let mut connection = config
            .to_connection_config()
            .map_err(|error| ProducerError::Config { error })?;
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
mod tests;
