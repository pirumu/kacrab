//! Public producer facade built from accumulator and dispatcher components.

use tokio::task::{JoinError, JoinSet};

use super::{
    accumulator::RecordAccumulator,
    config::ProducerRuntimeConfig,
    dispatcher::{DispatchOutcome, ProducerDispatcher},
    error::{ProducerError, Result},
    record::{Delivery, ProducerRecord},
};
use crate::{
    config::{ClientConfig, ConfigKey, ConfigValue, ProducerConfig},
    wire::{
        BrokerEndpoint, SaslClientAuthenticator, SaslClientAuthenticatorFactory,
        SaslClientAuthenticatorFactoryHandle, SaslClientAuthenticatorHandle, WireClient, WireError,
    },
};

/// Batched Kafka producer facade.
#[derive(Debug)]
pub struct KafkaProducer {
    accumulator: RecordAccumulator,
    dispatcher: ProducerDispatcher,
    in_flight: JoinSet<DispatchOutcome>,
    max_in_flight_requests: usize,
    max_block: std::time::Duration,
}

impl KafkaProducer {
    /// Build a producer from an existing wire client and runtime config.
    #[must_use]
    pub fn from_parts(wire: WireClient, config: ProducerRuntimeConfig) -> Self {
        let max_in_flight_requests = config.max_in_flight_requests_per_connection.max(1);
        let max_block = config.max_block;
        Self {
            accumulator: RecordAccumulator::new(config.accumulator),
            dispatcher: ProducerDispatcher::with_config(wire, config),
            in_flight: JoinSet::new(),
            max_in_flight_requests,
            max_block,
        }
    }

    /// Creates a Java-style producer builder.
    #[must_use]
    pub const fn builder() -> KafkaProducerBuilder {
        KafkaProducerBuilder::new()
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
        let config = config
            .producer_config()
            .map_err(|error| ProducerError::Config { error })?;
        Self::from_config(config).await
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
    pub async fn send(&mut self, record: ProducerRecord) -> Result<Delivery> {
        let mut record = record;
        self.dispatcher.assign_partition(&mut record).await?;
        let delivery = self.append_for_delivery_with_max_block(record).await?;
        self.poll().await.map(|()| delivery)
    }

    /// Append multiple records, then dispatch any batches that are already ready.
    ///
    /// # Errors
    ///
    /// Returns producer backpressure or errors from pumping ready batches.
    pub async fn send_batch<I>(&mut self, records: I) -> Result<Vec<Delivery>>
    where
        I: IntoIterator<Item = ProducerRecord>,
    {
        let records = records.into_iter();
        let (lower_bound, _upper_bound) = records.size_hint();
        let mut deliveries = Vec::with_capacity(lower_bound);
        for record in records {
            let mut record = record;
            self.dispatcher.assign_partition(&mut record).await?;
            deliveries.push(self.append_for_delivery_with_max_block(record).await?);
        }
        self.poll().await.map(|()| deliveries)
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
        let now = std::time::Instant::now();
        for record in records {
            let mut record = record;
            self.dispatcher.assign_partition(&mut record).await?;
            self.append_untracked_with_max_block(record, now).await?;
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
            self.spawn_dispatch(batches, now);
        }
        Ok(())
    }

    /// Force-dispatch every buffered batch regardless of linger or batch size.
    ///
    /// # Errors
    ///
    /// Returns an error when a buffered batch cannot be routed or delivered.
    pub async fn flush(&mut self) -> Result<()> {
        self.collect_finished_for_flush()?;
        if self.accumulator.buffered_bytes() > 0 {
            while self.in_flight.len() >= self.max_in_flight_requests {
                self.wait_for_one_for_flush().await?;
            }
            let _receipts = self.dispatcher.dispatch_all(&mut self.accumulator).await?;
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
        self.dispatcher.init_transactions().await
    }

    /// Begin a producer transaction.
    ///
    /// # Errors
    ///
    /// Returns an error when transactions are not configured, not initialized,
    /// or another transaction is already open.
    pub fn begin_transaction(&self) -> Result<()> {
        self.dispatcher.begin_transaction()
    }

    /// Flush pending records and commit the open transaction.
    ///
    /// # Errors
    ///
    /// Returns an error from flushing records or `EndTxn`.
    pub async fn commit_transaction(&mut self) -> Result<()> {
        self.flush().await?;
        self.dispatcher.end_transaction(true).await
    }

    /// Abort the open transaction.
    ///
    /// # Errors
    ///
    /// Returns an error from `EndTxn`.
    pub async fn abort_transaction(&self) -> Result<()> {
        self.dispatcher.end_transaction(false).await
    }

    /// Flush buffered records and consume the producer.
    ///
    /// # Errors
    ///
    /// Returns any error from [`Self::flush`].
    pub async fn close(mut self) -> Result<()> {
        self.flush().await
    }

    /// Estimated bytes currently buffered in the producer accumulator.
    #[must_use]
    pub const fn buffered_bytes(&self) -> usize {
        self.accumulator.buffered_bytes()
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

    fn spawn_dispatch(&mut self, batches: Vec<super::ReadyBatch>, now: std::time::Instant) {
        let dispatcher = self.dispatcher.clone();
        let abort_handle = self
            .in_flight
            .spawn(async move { dispatcher.dispatch_drained(batches, now).await });
        drop(abort_handle);
    }

    async fn append_for_delivery_with_max_block(
        &mut self,
        record: ProducerRecord,
    ) -> Result<Delivery> {
        let deadline = std::time::Instant::now()
            .checked_add(self.max_block)
            .unwrap_or_else(std::time::Instant::now);
        loop {
            let can_wait = self.accumulator.buffered_bytes() > 0 || !self.in_flight.is_empty();
            match self.accumulator.append_for_delivery(record.clone()) {
                Ok(delivery) => return Ok(delivery),
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

    async fn append_untracked_with_max_block(
        &mut self,
        record: ProducerRecord,
        now: std::time::Instant,
    ) -> Result<()> {
        let deadline = std::time::Instant::now()
            .checked_add(self.max_block)
            .unwrap_or_else(std::time::Instant::now);
        loop {
            let can_wait = self.accumulator.buffered_bytes() > 0 || !self.in_flight.is_empty();
            match self.accumulator.append_at(record.clone(), now) {
                Ok(()) => return Ok(()),
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
        result: core::result::Result<DispatchOutcome, JoinError>,
        requeue_is_error: bool,
    ) -> Result<()> {
        match result {
            Ok(DispatchOutcome::Delivered(result)) => result.map(|_receipts| ()),
            Ok(DispatchOutcome::Requeue(batches)) => {
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
}

/// Java-style builder for [`KafkaProducer`].
#[derive(Clone, Debug, Default)]
pub struct KafkaProducerBuilder {
    config: ClientConfig,
    sasl_client_authenticator: Option<SaslClientAuthenticatorHandle>,
    sasl_client_authenticator_factory: Option<SaslClientAuthenticatorFactoryHandle>,
}

impl KafkaProducerBuilder {
    /// Creates an empty producer builder.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            config: ClientConfig::new(),
            sasl_client_authenticator: None,
            sasl_client_authenticator_factory: None,
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
    pub async fn build(self) -> Result<KafkaProducer> {
        let config = self
            .config
            .producer_config()
            .map_err(|error| ProducerError::Config { error })?;
        let runtime = config.to_producer_runtime_config()?;
        let endpoints = resolve_bootstrap_brokers(&config).await?;
        let mut connection = config.to_connection_config();
        connection.sasl.client_authenticator = self.sasl_client_authenticator;
        connection.sasl.client_authenticator_factory = self.sasl_client_authenticator_factory;
        let wire = WireClient::connect_with_brokers(connection, config.client_id, endpoints);
        Ok(KafkaProducer::from_parts(wire, runtime))
    }
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

    use std::time::{Duration, Instant};

    use bytes::Bytes;

    use super::{DispatchOutcome, KafkaProducer, KafkaProducerBuilder, resolve_bootstrap_brokers};
    use crate::{
        config::ClientConfig,
        producer::{AccumulatorConfig, ProducerError, ProducerRecord, ProducerRuntimeConfig},
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
            ..ProducerRuntimeConfig::default()
        }
    }

    fn producer(max_in_flight: usize) -> KafkaProducer {
        KafkaProducer::from_parts(test_wire(), runtime_config(max_in_flight))
    }

    #[test]
    fn producer_builder_accepts_native_sasl_authenticator() {
        let builder =
            KafkaProducerBuilder::new().sasl_client_authenticator(BuilderSaslAuthenticator);

        assert_eq!(
            builder
                .sasl_client_authenticator
                .as_ref()
                .map(crate::wire::SaslClientAuthenticatorHandle::mechanism),
            Some(SaslMechanism::Plain)
        );
    }

    fn ready_batch() -> super::super::ReadyBatch {
        let mut accumulator = crate::producer::RecordAccumulator::new(
            AccumulatorConfig::default()
                .batch_size(1)
                .linger(Duration::from_mins(1)),
        );
        accumulator
            .append_at(
                ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")),
                Instant::now(),
            )
            .expect("append record");
        accumulator
            .drain_ready(Instant::now())
            .pop()
            .expect("ready batch")
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
            DispatchOutcome::Delivered(Ok(Vec::new()))
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
            DispatchOutcome::Delivered(Ok(Vec::new()))
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
        let mut producer = KafkaProducer::from_parts(test_wire(), config);
        producer
            .accumulator
            .append_at(
                ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")),
                Instant::now(),
            )
            .expect("append buffered record");
        let _abort = producer.in_flight.spawn(async {
            tokio::time::sleep(Duration::from_millis(1)).await;
            DispatchOutcome::Delivered(Ok(Vec::new()))
        });

        assert!(matches!(
            producer.flush().await,
            Err(ProducerError::DeliveryTimeout { topic, partition })
                if topic == "orders" && partition == 0
        ));
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
            .spawn(async { DispatchOutcome::Delivered(Ok(Vec::new())) });

        producer.wait_for_one().await.expect("wait consumes task");

        let _abort = producer
            .in_flight
            .spawn(async { DispatchOutcome::Delivered(Ok(Vec::new())) });
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
            .spawn(async { DispatchOutcome::Delivered(Ok(Vec::new())) });
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
            .spawn(async { DispatchOutcome::Delivered(Ok(Vec::new())) });
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
            .dispatch_task_result(Ok(DispatchOutcome::Requeue(vec![batch])), false)
            .expect("non-flush requeue is retained");
        assert!(producer.buffered_bytes() > 0);

        let batch = producer
            .accumulator
            .drain_all()
            .pop()
            .expect("requeued batch");
        assert!(matches!(
            producer.dispatch_task_result(Ok(DispatchOutcome::Requeue(vec![batch])), true),
            Err(ProducerError::FlushIncomplete)
        ));
    }

    #[tokio::test]
    async fn facade_transaction_wrappers_fail_locally_without_transactional_id() {
        let mut producer = producer(1);

        assert!(matches!(
            producer.begin_transaction(),
            Err(ProducerError::TransactionalIdRequired)
        ));
        assert!(matches!(
            producer.abort_transaction().await,
            Err(ProducerError::TransactionalIdRequired)
        ));
        assert!(matches!(
            producer.commit_transaction().await,
            Err(ProducerError::TransactionalIdRequired)
        ));
        producer.close().await.expect("closing empty producer");
    }

    #[tokio::test]
    async fn send_and_batch_apis_surface_backpressure_before_dispatch() {
        let mut config = runtime_config(1);
        config.accumulator = AccumulatorConfig::default().buffer_memory(1);
        let mut producer = KafkaProducer::from_parts(test_wire(), config);

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
    async fn send_waits_until_max_block_before_reporting_buffer_backpressure() {
        let mut config = runtime_config(1);
        config.accumulator = AccumulatorConfig::default()
            .batch_size(usize::MAX)
            .linger(Duration::from_mins(1))
            .buffer_memory(80);
        config.max_block = Duration::from_millis(10);
        let mut producer = KafkaProducer::from_parts(test_wire(), config);

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
        let builder = KafkaProducerBuilder::new().set("client.id", "producer-a");

        assert_eq!(
            builder.client_config().get("client.id"),
            Some(&crate::config::ConfigValue::new("producer-a"))
        );
        assert!(matches!(
            KafkaProducer::builder().build().await,
            Err(ProducerError::Config { .. })
        ));
        assert!(matches!(
            KafkaProducer::new(ClientConfig::new()).await,
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
