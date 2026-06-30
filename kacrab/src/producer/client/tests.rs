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
    DispatchOutcome, Producer, ProducerBuilder, TimedDispatchOutcome, resolve_bootstrap_brokers,
};
use crate::{
    config::ClientConfig,
    producer::{
        AccumulatorConfig, ConsumerGroupMetadata, KafkaMetric, MetricName, MetricReporter,
        MetricValue, ProducerError, ProducerIdempotenceConfig, ProducerIdentity,
        ProducerInterceptor, ProducerMetricSubscription, ProducerMetricValue, ProducerRecord,
        ProducerRuntimeConfig, ReadyBatch, RecordMetadata, SendFuture, TopicPartition,
        accumulator::ReadyBatchIdentity,
    },
    wire::{
        BrokerEndpoint, ConnectionConfig, SaslClientAction, SaslClientAuthenticator, SaslMechanism,
        WireClient,
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
    const TEST_LARGE_BATCH_SIZE: usize = 16 * 1024;
    ProducerRuntimeConfig {
        accumulator: AccumulatorConfig::default()
            .batch_size(TEST_LARGE_BATCH_SIZE)
            .linger(Duration::from_mins(1))
            .buffer_memory(TEST_LARGE_BATCH_SIZE * 4),
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
fn producer_built_outside_runtime_starts_sender_loop_lazily() {
    let producer = producer(5);
    assert!(!producer.sender.loop_is_running());

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    runtime.block_on(async {
        producer.ensure_background_sender_loop();
    });

    assert!(producer.sender.loop_is_running());
}

#[test]
fn background_sender_loop_body_is_owned_by_sender_module() {
    let source = include_str!("../client.rs")
        .split_once("#[cfg(test)]\nmod tests")
        .expect("client test module marker")
        .0;

    assert!(
        source.contains("ProducerSenderRuntime::with_dispatcher("),
        "Producer facade should create the background loop through the sender runtime"
    );
    assert!(
        source.contains("self.sender.ensure_loop_running("),
        "Producer facade should delegate lazy loop startup to the sender runtime"
    );
    assert!(
        !source.contains("ProducerSender::spawn_background_loop("),
        "Producer facade should not call the raw sender-loop task spawner"
    );
    assert!(
        !source.contains("fn spawn_background_sender_loop("),
        "Producer facade should not define a background sender loop wrapper"
    );
    assert!(
        !source.contains("drive_wake_until_waiting("),
        "Producer facade should not own the background sender loop body"
    );
    assert!(
        !source.contains("SenderLoopWait::"),
        "Producer facade should not switch on sender-loop wait states"
    );
}

#[test]
fn producer_facade_does_not_own_raw_sender_loop_abort_handle() {
    let source = include_str!("../client.rs")
        .split_once("#[cfg(test)]\nmod tests")
        .expect("client test module marker")
        .0;

    assert!(
        !source.contains("task::AbortHandle"),
        "Producer facade should not import the raw sender-loop abort handle"
    );
    assert!(
        !source.contains("sender_loop: Option<AbortHandle>"),
        "Producer facade should store a sender-owned loop handle wrapper"
    );
    assert!(
        source.contains("sender: ProducerSenderRuntime"),
        "Producer facade should name the sender runtime explicitly"
    );
    assert!(
        !source.contains("sender_loop.abort();"),
        "Producer facade drop should rely on the sender-loop handle owner"
    );
}

#[test]
fn producer_facade_stores_sender_runtime_instead_of_loop_parts() {
    let source = include_str!("../client.rs")
        .split_once("#[cfg(test)]\nmod tests")
        .expect("client test module marker")
        .0;

    assert!(
        source.contains("sender: ProducerSenderRuntime"),
        "Producer facade should store the sender runtime as one owned component"
    );
    assert!(
        !source.contains("sender: Arc<AsyncMutex<ProducerSender>>"),
        "Producer facade should not own the raw shared sender lock"
    );
    assert!(
        !source.contains("sender_loop: ProducerSenderLoop"),
        "Producer facade should not store the sender loop separately"
    );
    assert!(
        !source.contains("sender_loop_metrics_enabled"),
        "Producer facade should not store sender-loop metrics state separately"
    );
    assert!(
        !source.contains("accumulator_batch_size: usize"),
        "Producer facade should not store sender-loop batch sizing separately"
    );
}

#[test]
fn producer_facade_delegates_sender_construction_to_runtime() {
    let source = include_str!("../client.rs")
        .split_once("#[cfg(test)]\nmod tests")
        .expect("client test module marker")
        .0;

    assert!(
        source.contains("ProducerSenderRuntime::with_dispatcher("),
        "Producer facade should construct sender internals through the runtime"
    );
    assert!(
        !source.contains("ProducerSender::with_dispatcher("),
        "Producer facade should not construct ProducerSender directly"
    );
    assert!(
        source.contains("self.sender.shared_sender()"),
        "the synchronous-send slow drain should obtain the shared ProducerSender from the \
         runtime, not construct it"
    );
}

#[test]
fn producer_facade_does_not_pass_metrics_into_sender_loop_restart() {
    let source = include_str!("../client.rs")
        .split_once("#[cfg(test)]\nmod tests")
        .expect("client test module marker")
        .0;
    let sender_source = include_str!("../sender.rs");

    assert!(
        source.contains("self.sender.ensure_loop_running();"),
        "Producer facade should ask the sender runtime to ensure its own loop"
    );
    assert!(
        !source.contains("self.sender.ensure_loop_running(self.metrics.clone())"),
        "Producer facade should not pass metrics into sender-loop restart"
    );
    assert!(
        sender_source.contains("metrics: ProducerMetrics"),
        "ProducerSenderRuntime should retain the metrics handle used by its loop"
    );
}

#[test]
fn producer_facade_routes_delivery_append_through_sender_runtime() {
    let source = include_str!("../client.rs")
        .split_once("#[cfg(test)]\nmod tests")
        .expect("client test module marker")
        .0;

    assert!(
        source.contains(".append_callback_now("),
        "the hot synchronous send path should append through the sender runtime's lock-free bypass"
    );
    assert!(
        source.contains(".append_callback_delivery_record_then_apply_dispatch("),
        "the slow-send drain should append callback delivery records through the shared sender's \
         awaiting path"
    );
}

#[test]
fn send_family_transaction_error_guards_route_through_sender() {
    let source = include_str!("../client.rs");
    let (_, after_start) = source
        .split_once("fn send_with_optional_callback(")
        .expect("missing send_with_optional_callback marker");
    let (body, _) = after_start
        .split_once("\n    fn can_append_synchronously(")
        .expect("missing send_with_optional_callback end marker");

    assert!(
        !body.contains("self.dispatcher.fail_if_transaction_error()"),
        "the synchronous send path should not guard transactions through Producer::dispatcher \
         directly"
    );
    assert!(
        body.contains("self.control_dispatcher.try_fail_if_transaction_error_now()"),
        "the synchronous send path should guard fatal transaction errors synchronously before \
         appending, like Kafka's send()"
    );
}

#[test]
fn producer_public_send_api_matches_java_surface() {
    let source = include_str!("../client.rs");

    assert!(
        !source.contains(concat!("pub async fn ", "send_batch")),
        "producer public API should rely on per-record send and internal batching"
    );
    assert!(
        !source.contains(concat!("pub async fn ", "send_batch_with_callback")),
        "producer public API should not expose a Rust batch callback send extension"
    );
    assert!(
        !source.contains(concat!("pub async fn ", "send_batch_untracked")),
        "producer public API should not expose an untracked batch send extension"
    );
    assert!(
        !source.contains(concat!("pub async fn ", "poll(&mut self)")),
        "producer public API should not expose manual producer polling"
    );
    assert!(
        !source.contains(concat!("pub async fn ", "send_with_java_callback")),
        "producer public API should expose the Kafka send(record, callback) overload as \
         send_with_callback only"
    );
}

#[test]
fn partitions_for_fetches_metadata_through_sender_owned_dispatcher() {
    let source = include_str!("../client.rs");
    let (_, after_start) = source
        .split_once("pub async fn partitions_for(&self, topic: &str)")
        .expect("partitions_for should exist");
    let (body, _) = after_start
        .split_once("/// Return this producer instance's Kafka-negotiated client-instance id.")
        .expect("partitions_for should end before client_instance_id");

    assert!(
        !body.contains("self.dispatcher.metadata_for_topics"),
        "partitions_for should not bypass ProducerSender for metadata fetches"
    );
    assert!(
        body.contains("self\n            .sender")
            && body.contains(".lock()\n            .await")
            && body.contains(".metadata_for_topics([topic])"),
        "partitions_for should fetch metadata through the sender-owned dispatcher"
    );
}

#[test]
fn telemetry_control_requests_route_through_sender_owned_dispatcher() {
    let source = include_str!("../client.rs");
    let cases = [
        (
            "push_telemetry",
            "pub async fn push_telemetry(",
            "/// Aggregate current producer metrics and push them as uncompressed OTLP.",
        ),
        (
            "fetch_telemetry_subscription",
            "async fn fetch_telemetry_subscription(&self)",
            "fn clear_telemetry_subscription(&self)",
        ),
    ];

    for (name, start_marker, end_marker) in cases {
        let (_, after_start) = source
            .split_once(start_marker)
            .unwrap_or_else(|| panic!("missing {name} start marker"));
        let (body, _) = after_start
            .split_once(end_marker)
            .unwrap_or_else(|| panic!("missing {name} end marker"));

        assert!(
            !body.contains("self.dispatcher"),
            "{name} should not bypass ProducerSender for telemetry control requests"
        );
        assert!(
            body.contains(".sender.lock().await") && body.contains("sender.control_dispatcher()"),
            "{name} should snapshot the sender-owned dispatcher under the sender lock"
        );
        assert!(
            !body.contains(".send_control_request(\n")
                || body.contains("dispatcher\n                    .send_control_request(")
                || body.contains("dispatcher\n            .send_control_request("),
            "{name} should not await control requests on the locked sender"
        );
    }
}

#[test]
fn producer_facade_names_remaining_dispatcher_clone_as_control_plane() {
    let source = include_str!("../client.rs");
    let (struct_body, after_struct) = source
        .split_once("struct TelemetrySubscription")
        .expect("Producer struct should appear before telemetry subscription");

    assert!(
        !struct_body
            .lines()
            .any(|line| line.trim() == "dispatcher: ProducerDispatcher,"),
        "Producer facade should not expose an ambiguous dispatcher field"
    );
    assert!(
        struct_body
            .lines()
            .any(|line| line.trim() == "control_dispatcher: ProducerDispatcher,"),
        "Producer facade should name the non-hot-path dispatcher clone as control plane"
    );
    assert!(
        !after_struct
            .split_once("#[cfg(test)]")
            .map_or(after_struct, |(production_source, _tests)| {
                production_source
            })
            .contains("self.dispatcher"),
        "Producer methods should use control_dispatcher or ProducerSender, not self.dispatcher"
    );
}

#[test]
fn async_transaction_control_uses_nonblocking_dispatcher_snapshot() {
    let source = include_str!("../client.rs");
    let cases = [
        (
            "init_transactions_with_max_block",
            "async fn init_transactions_with_max_block(&self) -> Result<()> {",
            "/// Begin a producer transaction.",
        ),
        (
            "commit_transaction",
            "pub async fn commit_transaction(&mut self) -> Result<()> {",
            "/// Abort the open transaction.",
        ),
        (
            "abort_transaction",
            "pub async fn abort_transaction(&mut self) -> Result<()> {",
            "async fn end_transaction_with_max_block(",
        ),
        (
            "end_transaction_with_max_block",
            "async fn end_transaction_with_max_block(",
            "/// Add consumer offsets to the active transaction.",
        ),
        (
            "send_offsets_to_transaction",
            "pub async fn send_offsets_to_transaction<I>(",
            "async fn send_offsets_to_transaction_with_max_block(",
        ),
        (
            "send_offsets_to_transaction_with_max_block",
            "async fn send_offsets_to_transaction_with_max_block(",
            "/// Flush buffered records and consume the producer.",
        ),
    ];

    for (name, start_marker, end_marker) in cases {
        let (_, after_start) = source
            .split_once(start_marker)
            .unwrap_or_else(|| panic!("missing {name} start marker"));
        let (body, _) = after_start
            .split_once(end_marker)
            .unwrap_or_else(|| panic!("missing {name} end marker"));

        assert!(
            !body.contains("self.dispatcher"),
            "{name} should not use scattered Producer::dispatcher calls for async transaction \
             control"
        );
        assert!(
            body.contains("self.control_dispatcher()"),
            "{name} should snapshot the shared control dispatcher before transaction control IO"
        );
        let (before_snapshot, _) = body
            .split_once("self.control_dispatcher()")
            .unwrap_or_else(|| panic!("missing dispatcher snapshot for {name}"));
        assert!(
            !before_snapshot.contains(".sender.lock().await"),
            "{name} should not wait on the sender lock before transaction control snapshot"
        );
    }
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

    let (max_in_flight_requests, uses_idempotent_ordering, pipelined_same_partition) = {
        let mut sender = producer.sender.try_lock().expect("sender");
        let in_flight = ReadyBatch {
            identity: ReadyBatchIdentity::test(0),
            topic: "orders".to_owned(),
            partition: 0,
            records: vec![ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a"))],
            delivery: None,
            bytes: 1,
            pooled_buffer_bytes: 1,
            first_append_at: Instant::now(),
            producer_state: None,
        };
        let ready = ReadyBatch {
            identity: ReadyBatchIdentity::test(1),
            topic: "orders".to_owned(),
            partition: 0,
            records: vec![ProducerRecord::new("orders", 0).value(Bytes::from_static(b"b"))],
            delivery: None,
            bytes: 1,
            pooled_buffer_bytes: 1,
            first_append_at: Instant::now(),
            producer_state: None,
        };
        sender
            .state
            .reserve_partitions_for_dispatch(std::slice::from_ref(&in_flight));
        let selection = sender.state.select_dispatchable_batches(vec![ready]);
        (
            sender.state.max_in_flight_requests(),
            sender.state.uses_idempotent_ordering(),
            // depth 1 < max.in.flight=5 -> the same partition pipelines another request.
            selection.dispatchable.len() == 1 && selection.deferred.is_empty(),
        )
    };
    assert_eq!(max_in_flight_requests, 5);
    assert!(uses_idempotent_ordering);
    assert!(pipelined_same_partition);
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
        .expect("native interceptor should replace JVM interceptor class loading");

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
        .expect("native metric reporter should replace JVM metric reporter loading");

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
    .expect("empty interceptor.classes should match Kafka's default list");
    assert_eq!(producer.buffered_bytes(), 0);
    assert!(producer.interceptors.is_empty());

    let producer = ProducerBuilder::new()
        .set("bootstrap.servers", "127.0.0.1:9092")
        .set("interceptor.classes", "  ")
        .build()
        .await
        .expect("blank interceptor.classes should match Kafka's default list");
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
        .expect("send");
    let batches = producer.sender.lock().await.accumulator.drain_all();

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
        .expect("interceptor errors are ignored");
    let batches = producer.sender.lock().await.accumulator.drain_all();

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
        .expect("interceptor panics are ignored");
    let batches = producer.sender.lock().await.accumulator.drain_all();

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
        .expect("send with callback");
    let mut batches = producer.sender.lock().await.accumulator.drain_all();
    let sender = batches
        .first_mut()
        .and_then(|batch| batch.delivery.take())
        .expect("delivery sender");

    sender.send(&record_metadata(40));

    assert_eq!(order.load(Ordering::Relaxed), 2);
}

#[tokio::test]
async fn send_with_callback_invokes_callback_on_local_api_error_like_java() {
    let callback_error = Arc::new(AtomicUsize::new(0));
    let callback_error_sink = Arc::clone(&callback_error);
    let mut config = runtime_config(1);
    config.max_request_size = 8;
    let producer = Producer::from_parts(test_wire(), config);

    let error = producer
        .send_with_callback(
            ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")),
            move |result| {
                if matches!(result, Err(ProducerError::RecordTooLarge { .. })) {
                    callback_error_sink.store(1, Ordering::Relaxed);
                }
            },
        )
        .expect_err("record should exceed max.request.size");

    assert!(matches!(error, ProducerError::RecordTooLarge { .. }));
    assert_eq!(callback_error.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn send_with_callback_without_interceptors_does_not_clone_successful_record() {
    let producer = producer(1);
    ProducerRecord::reset_clone_count_for_test();

    let _delivery = producer
        .send_with_callback(
            ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")),
            |_result| {},
        )
        .expect("send with callback");

    assert_eq!(ProducerRecord::clone_count_for_test(), 0);
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
        .expect("send");
    let mut batches = producer.sender.lock().await.accumulator.drain_all();
    let sender = batches
        .first_mut()
        .and_then(|batch| batch.delivery.take())
        .expect("delivery sender");

    sender.send(&record_metadata(40));

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

fn ready_batch_for_partition(topic: &str, partition: i32) -> ReadyBatch {
    let accumulator = crate::producer::SharedAccumulator::with_config(
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
fn idempotent_selection_defers_batches_for_in_flight_partitions_when_order_is_guaranteed() {
    let mut config = runtime_config(1);
    config.idempotence = ProducerIdempotenceConfig {
        enabled: true,
        ..ProducerIdempotenceConfig::default()
    };
    let producer = Producer::from_parts(test_wire(), config);
    let selection = {
        let mut sender = producer.sender.try_lock().expect("sender");
        sender
            .state
            .reserve_partitions_for_dispatch(&[ready_batch_for_partition("orders", 0)]);
        sender.state.select_dispatchable_batches(vec![
            ready_batch_for_partition("orders", 0),
            ready_batch_for_partition("orders", 1),
        ])
    };

    assert_eq!(selection.dispatchable.len(), 1);
    assert_eq!(selection.dispatchable[0].partition, 1);
    assert_eq!(selection.deferred.len(), 1);
    assert_eq!(selection.deferred[0].partition, 0);
}

#[test]
fn idempotent_selection_pipelines_same_partition_up_to_max_in_flight() {
    let mut config = runtime_config(5);
    config.idempotence = ProducerIdempotenceConfig {
        enabled: true,
        ..ProducerIdempotenceConfig::default()
    };
    let producer = Producer::from_parts(test_wire(), config);
    // A partition with one in-flight request (depth 1 < max.in.flight=5) still pipelines
    // another request — Kafka parity for idempotent producers.
    let pipelined = {
        let mut sender = producer.sender.try_lock().expect("sender");
        sender
            .state
            .reserve_partitions_for_dispatch(&[ready_batch_for_partition("orders", 0)]);
        sender
            .state
            .select_dispatchable_batches(vec![ready_batch_for_partition("orders", 0)])
    };
    // Reserve up to the max in-flight depth (5 total); now the partition defers.
    let deferred = {
        let mut sender = producer.sender.try_lock().expect("sender");
        sender.state.reserve_partitions_for_dispatch(&[
            ready_batch_for_partition("orders", 0),
            ready_batch_for_partition("orders", 0),
            ready_batch_for_partition("orders", 0),
            ready_batch_for_partition("orders", 0),
        ]);
        sender
            .state
            .select_dispatchable_batches(vec![ready_batch_for_partition("orders", 0)])
    };

    assert_eq!(pipelined.dispatchable.len(), 1);
    assert_eq!(pipelined.dispatchable[0].partition, 0);
    assert!(pipelined.deferred.is_empty());
    assert!(deferred.dispatchable.is_empty());
    assert_eq!(deferred.deferred.len(), 1);
    assert_eq!(deferred.deferred[0].partition, 0);
}

#[test]
fn guaranteed_order_selection_emits_one_batch_per_partition_per_cycle() {
    let mut config = runtime_config(5);
    config.idempotence = ProducerIdempotenceConfig {
        enabled: true,
        ..ProducerIdempotenceConfig::default()
    };
    let producer = Producer::from_parts(test_wire(), config);

    // Two ready batches for the same partition are split across cycles: at most one new
    // request per partition per selection, so each becomes its own concurrent dispatch
    // task (pipelining across the outer in-flight JoinSet) rather than one coalesced task.
    let selection = {
        let sender = producer.sender.try_lock().expect("sender");
        sender.state.select_dispatchable_batches(vec![
            ready_batch_for_partition("orders", 0),
            ready_batch_for_partition("orders", 0),
        ])
    };

    assert_eq!(selection.dispatchable.len(), 1);
    assert_eq!(selection.dispatchable[0].partition, 0);
    assert_eq!(selection.deferred.len(), 1);
    assert_eq!(selection.deferred[0].partition, 0);
    assert_eq!(selection.partitions.len(), 1);
}

#[tokio::test]
async fn flush_waits_for_in_flight_slot_and_reports_local_delivery_timeout() {
    let mut config = runtime_config(1);
    config.delivery_timeout = Duration::ZERO;
    let mut producer = Producer::from_parts(test_wire(), config);
    producer
        .sender
        .lock()
        .await
        .accumulator
        .append_at(
            ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")),
            Instant::now(),
        )
        .expect("append buffered record");
    let _abort = {
        let mut sender = producer.sender.lock().await;
        sender.state.spawn_in_flight(async {
            tokio::time::sleep(Duration::from_millis(1)).await;
            timed(DispatchOutcome::Delivered(Ok(Vec::new())))
        })
    };

    assert!(matches!(
        producer.flush().await,
        Err(ProducerError::DeliveryTimeout { topic, partition })
            if topic == "orders" && partition == 0
    ));
    assert_eq!(producer.metrics().flush_count, 0);
}

#[tokio::test]
async fn wait_for_one_helpers_return_ok_when_no_task_exists() {
    let producer = producer(1);

    producer.wait_for_one().await.expect("empty wait is ok");
    producer
        .wait_for_one_for_flush()
        .await
        .expect("empty flush wait is ok");
}

#[tokio::test]
async fn wait_for_one_helpers_process_completed_tasks() {
    let producer = producer(1);
    let _abort = {
        let mut sender = producer.sender.lock().await;
        sender
            .state
            .spawn_in_flight(async { timed(DispatchOutcome::Delivered(Ok(Vec::new()))) })
    };

    producer.wait_for_one().await.expect("wait consumes task");

    let _abort = {
        let mut sender = producer.sender.lock().await;
        sender
            .state
            .spawn_in_flight(async { timed(DispatchOutcome::Delivered(Ok(Vec::new()))) })
    };
    producer
        .wait_for_one_for_flush()
        .await
        .expect("flush wait consumes task");
}

#[tokio::test]
async fn collect_finished_for_flush_consumes_completed_tasks() {
    let producer = producer(1);
    let _abort = {
        let mut sender = producer.sender.lock().await;
        sender
            .state
            .spawn_in_flight(async { timed(DispatchOutcome::Delivered(Ok(Vec::new()))) })
    };
    tokio::task::yield_now().await;

    producer
        .collect_finished_for_flush()
        .expect("flush collect consumes task");
}

#[tokio::test]
async fn collect_finished_consumes_successful_and_panicked_tasks() {
    let producer = producer(1);
    let _abort = {
        let mut sender = producer.sender.lock().await;
        sender
            .state
            .spawn_in_flight(async { timed(DispatchOutcome::Delivered(Ok(Vec::new()))) })
    };
    tokio::task::yield_now().await;

    producer
        .collect_finished()
        .expect("successful task is consumed");

    let _abort = {
        let mut sender = producer.sender.lock().await;
        sender.state.spawn_in_flight(async {
            tokio::task::yield_now().await;
            panic!("dispatch task panic");
        })
    };
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
    let producer = producer(1);
    let batch = {
        let sender = producer.sender.try_lock().expect("sender");
        sender
            .accumulator
            .append_at(
                ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")),
                Instant::now(),
            )
            .expect("append producer-owned batch");
        sender
            .accumulator
            .drain_all()
            .pop()
            .expect("producer-owned drained batch")
    };

    producer
        .dispatch_task_result(Ok(timed(DispatchOutcome::Requeue(vec![batch]))), false)
        .expect("non-flush requeue is retained");
    assert!(producer.buffered_bytes() > 0);

    let batch = producer
        .sender
        .try_lock()
        .expect("sender")
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
        .control_dispatcher
        .set_abortable_transaction_error_for_test(ErrorCode::UnknownProducerId)
        .await;
    producer
        .sender
        .lock()
        .await
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
        .sender
        .lock()
        .await
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
        .sender
        .lock()
        .await
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
        .control_dispatcher
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
    let producer = producer(1);
    producer
        .sender
        .try_lock()
        .expect("sender")
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
    assert_eq!(
        registry.get("incomplete_batches"),
        Some(&ProducerMetricValue::Gauge(1))
    );
    assert!(matches!(
        registry.get("buffer_available_bytes"),
        Some(ProducerMetricValue::Gauge(available)) if *available > 0
    ));
    assert_eq!(
        registry.get("waiting_threads"),
        Some(&ProducerMetricValue::Gauge(0))
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

#[test]
fn kafka_metrics_exposes_java_named_producer_metrics() {
    let producer = producer(1);
    let metrics = producer.kafka_metrics();

    // The client-level SenderMetricsRegistry sensors are registered under
    // their Kafka metric names on the producer-metrics group.
    for name in [
        "producer-metrics:record-send-rate",
        "producer-metrics:record-send-total",
        "producer-metrics:record-error-rate",
        "producer-metrics:record-retry-rate",
        "producer-metrics:byte-rate",
        "producer-metrics:batch-split-total",
        "producer-metrics:compression-rate-avg",
        "producer-metrics:records-per-request-avg",
        "producer-metrics:request-latency-avg",
        "producer-metrics:request-latency-max",
        "producer-metrics:batch-size-avg",
        "producer-metrics:record-size-avg",
        "producer-metrics:produce-throttle-time-avg",
        "producer-metrics:record-queue-time-avg",
        "producer-metrics:requests-in-flight",
        "producer-metrics:metadata-age",
    ] {
        assert!(metrics.contains_key(name), "missing kafka metric {name}");
    }
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

    sender.send(&RecordMetadata {
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

    sender.send(&record_metadata(0));

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

    sender.send(&record_metadata(0));

    assert!(forced_close.load(Ordering::Relaxed));
    assert!(
        producer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_none()
    );
}

#[tokio::test]
async fn close_now_aborts_buffered_records_with_producer_closed_like_java() {
    let producer = producer(1);
    let delivery = producer
        .send(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")))
        .expect("send buffered record");

    producer.close_now();

    // Kafka forceClose fails incomplete batches with an explicit error.
    assert!(matches!(delivery.await, Err(ProducerError::ProducerClosed)));
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
    let producer = producer(1);
    let _delivery = producer
        .send(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")))
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
async fn send_api_rejects_record_larger_than_buffer_before_dispatch() {
    // A record that cannot fit the whole buffer is rejected up front (Kafka
    // ensureValidRecordSize / G17) rather than blocking or surfacing generic
    // backpressure. Buffer-full backpressure for fitting records is covered
    // at the accumulator level.
    let mut config = runtime_config(1);
    config.accumulator = AccumulatorConfig::default().buffer_memory(1);
    let producer = Producer::from_parts(test_wire(), config);

    assert!(matches!(
        producer.send(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value"))),
        Err(ProducerError::RecordExceedsBufferMemory { .. })
    ));
}

#[tokio::test]
async fn send_apis_reject_records_larger_than_max_request_size() {
    let mut config = runtime_config(1);
    config.max_request_size = 8;
    let producer = Producer::from_parts(test_wire(), config);

    let error = producer
        .send(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")))
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
        .batch_size(80)
        .linger(Duration::from_mins(1))
        .buffer_memory(80);
    config.max_block = Duration::from_millis(10);
    let producer = Producer::from_parts(test_wire(), config);

    let first = producer.send(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")));
    let started = Instant::now();
    let second = producer.send(ProducerRecord::new("orders", 1).value(Bytes::from_static(b"b")));

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
