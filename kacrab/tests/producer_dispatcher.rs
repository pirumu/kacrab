#![cfg(feature = "producer")]
//! Producer dispatcher integration tests.

#![allow(
    clippy::expect_used,
    clippy::missing_assert_message,
    clippy::unwrap_used,
    reason = "Integration test fixtures fail fastest with contextual unwrap/expect calls."
)]

use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use bytes::{Bytes, BytesMut};
use kacrab::{
    producer::{
        Producer, ProducerCompression, ProducerInterceptor, ProducerPartitioner, ProducerRecord,
        RecordMetadata,
        internals::{
            AccumulatorConfig, ProducerDispatcher, ProducerIdempotenceConfig,
            ProducerRuntimeConfig, SharedAccumulator,
        },
    },
    wire::{BrokerEndpoint, ClusterMetadata, ConnectionConfig, WireClient},
};
#[cfg(feature = "lz4")]
use kacrab_protocol::compression::Compression;
use kacrab_protocol::{
    KafkaString, KafkaUuid, frame,
    generated::{
        AddOffsetsToTxnRequestData, AddOffsetsToTxnResponseData, AddPartitionsToTxnRequestData,
        AddPartitionsToTxnResponseData, AddPartitionsToTxnResult, AddPartitionsToTxnTopicResult,
        ApiKey, ApiVersion, ApiVersionsResponseData, EndTxnRequestData, EndTxnResponseData,
        ErrorCode, FindCoordinatorRequestData, FindCoordinatorResponseData,
        GetTelemetrySubscriptionsRequestData, GetTelemetrySubscriptionsResponseData,
        InitProducerIdRequestData, InitProducerIdResponseData, MetadataResponseBroker,
        MetadataResponseData, MetadataResponsePartition, MetadataResponseTopic,
        PartitionProduceResponse, ProduceRequestData, ProduceResponseData,
        PushTelemetryRequestData, PushTelemetryResponseData, RequestHeaderData, ResponseHeaderData,
        TopicProduceResponse, TxnOffsetCommitRequestData, TxnOffsetCommitResponseData,
        TxnOffsetCommitResponsePartition, TxnOffsetCommitResponseTopic,
        produce_response::{
            LeaderIdAndEpoch as ProduceLeaderIdAndEpoch, NodeEndpoint as ProduceNodeEndpoint,
        },
    },
    record::{RecordBatch, decode_batches},
    version::response_header_version,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

const TOPIC_ID: KafkaUuid = KafkaUuid::from_parts(0x1111_2222_3333_4444, 0x5555_6666_7777_8888);

fn assert_partition_base_offsets(produce: &ProduceRequestData, expected: &[i64]) {
    let topic = produce.topic_data.first().expect("topic data");
    let partition = topic.partition_data.first().expect("partition data");
    let mut records = partition.records.clone().expect("records");
    let batches = decode_batches(&mut records).expect("record batches");
    let base_offsets: Vec<_> = batches.iter().map(|batch| batch.base_offset).collect();
    assert_eq!(base_offsets, expected);
}

fn assert_single_idempotent_produce(
    produce: &ProduceRequestData,
    expected_partition: i32,
    expected_base_sequence: i32,
) {
    assert_eq!(produce.acks, -1);
    assert_eq!(produce.topic_data.len(), 1);
    let topic_data = produce.topic_data.first().expect("topic produce data");
    assert_eq!(topic_data.topic_id, TOPIC_ID);
    assert_eq!(topic_data.partition_data.len(), 1);
    let partition_data = topic_data
        .partition_data
        .first()
        .expect("partition produce data");
    assert_eq!(partition_data.index, expected_partition);
    let mut records = partition_data.records.clone().expect("records");
    let batch = RecordBatch::decode(&mut records).expect("record batch");
    assert_eq!(batch.producer_id, 42);
    assert_eq!(batch.producer_epoch, 3);
    assert_eq!(batch.base_sequence, expected_base_sequence);
}

async fn wait_for_buffered_bytes(producer: &Producer) -> usize {
    let deadline = Instant::now()
        .checked_add(Duration::from_millis(250))
        .unwrap_or_else(Instant::now);
    loop {
        let buffered = producer.buffered_bytes();
        if buffered > 0 || Instant::now() >= deadline {
            return buffered;
        }
        tokio::task::yield_now().await;
    }
}

#[tokio::test]
async fn kafka_producer_send_buffers_until_flush() {
    let leader_7 = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::Produce as i16);
            let produce = ProduceRequestData::read(&mut request, header.request_api_version)
                .expect("produce request");
            assert_eq!(produce.topic_data.len(), 1);
            assert_eq!(produce.topic_data[0].topic_id, TOPIC_ID);
            assert_eq!(produce.topic_data[0].partition_data.len(), 1);
            assert_eq!(produce.topic_data[0].partition_data[0].index, 0);
            assert_partition_base_offsets(&produce, &[0]);
            produce_response_frame_for_request(&header, 0, 40)
        }),
    ])
    .await;
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new({
            let leader_7 = leader_7.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::Metadata as i16);
                let response = metadata_response([(7, leader_7)]);
                response_frame(ApiKey::Metadata, 13, header.correlation_id, &response)
            }
        }),
    ])
    .await;

    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default(),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let mut producer = Producer::from_parts(
        wire,
        ProducerRuntimeConfig {
            accumulator: AccumulatorConfig::default()
                .batch_size(16 * 1024)
                .linger(Duration::from_mins(1))
                .buffer_memory(16 * 1024),
            acks: 1,
            timeout_ms: 30_000,
            retry_attempts: 0,
            retry_backoff: Duration::from_millis(100),
            retry_backoff_max: Duration::from_secs(1),
            delivery_timeout: Duration::from_mins(2),
            max_block: Duration::from_mins(1),
            partitioner_ignore_keys: false,
            partitioner_adaptive_partitioning_enable: true,
            partitioner_availability_timeout: Duration::ZERO,
            max_in_flight_requests_per_connection: 5,
            max_request_size: 1_048_576,
            enable_metrics_push: true,
            compression: ProducerCompression::default(),
            idempotence: idempotence_disabled(),
        },
    );
    producer.enable_metrics();

    let delivery = producer
        .send(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")))
        .unwrap();
    assert!(wait_for_buffered_bytes(&producer).await > 0);

    producer.flush().await.unwrap();
    let receipt = delivery.await.unwrap();

    assert_eq!(receipt.partition, 0);
    assert_eq!(receipt.leader_id, 7);
    assert_eq!(receipt.offset, 40);
    assert_eq!(producer.buffered_bytes(), 0);
    assert_eq!(bootstrap.join().await, 2);
    assert_eq!(leader_7.join().await, 2);
}

#[tokio::test]
async fn kafka_producer_background_sender_dispatches_after_linger_without_flush() {
    let leader_7 = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::Produce as i16);
            let produce = ProduceRequestData::read(&mut request, header.request_api_version)
                .expect("produce request");
            assert_eq!(produce.topic_data.len(), 1);
            assert_eq!(produce.topic_data[0].topic_id, TOPIC_ID);
            assert_eq!(produce.topic_data[0].partition_data.len(), 1);
            assert_eq!(produce.topic_data[0].partition_data[0].index, 0);
            assert_partition_base_offsets(&produce, &[0]);
            produce_response_frame_for_request(&header, 0, 40)
        }),
    ])
    .await;
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new({
            let leader_7 = leader_7.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::Metadata as i16);
                let response = metadata_response([(7, leader_7)]);
                response_frame(ApiKey::Metadata, 13, header.correlation_id, &response)
            }
        }),
    ])
    .await;

    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default(),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let producer = Producer::from_parts(
        wire,
        ProducerRuntimeConfig {
            accumulator: AccumulatorConfig::default()
                .batch_size(16 * 1024)
                .linger(Duration::from_millis(10))
                .buffer_memory(16 * 1024),
            acks: 1,
            timeout_ms: 30_000,
            retry_attempts: 0,
            retry_backoff: Duration::from_millis(100),
            retry_backoff_max: Duration::from_secs(1),
            delivery_timeout: Duration::from_secs(1),
            max_block: Duration::from_secs(1),
            partitioner_ignore_keys: false,
            partitioner_adaptive_partitioning_enable: true,
            partitioner_availability_timeout: Duration::ZERO,
            max_in_flight_requests_per_connection: 5,
            max_request_size: 1_048_576,
            enable_metrics_push: true,
            compression: ProducerCompression::default(),
            idempotence: idempotence_disabled(),
        },
    );

    let delivery = producer
        .send_with_callback(
            ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")),
            |_| {},
        )
        .unwrap();

    let receipt = tokio::time::timeout(Duration::from_millis(250), delivery)
        .await
        .expect("background sender should dispatch after linger without flush")
        .expect("delivery should succeed");

    assert_eq!(receipt.offset, 40);
    assert_eq!(producer.buffered_bytes(), 0);
    assert_eq!(bootstrap.join().await, 2);
    assert_eq!(leader_7.join().await, 2);
}

#[tokio::test]
async fn kafka_producer_background_sender_dispatches_ready_batch_without_linger_or_flush() {
    let leader_7 = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::Produce as i16);
            let produce = ProduceRequestData::read(&mut request, header.request_api_version)
                .expect("produce request");
            assert_eq!(produce.topic_data.len(), 1);
            assert_eq!(produce.topic_data[0].topic_id, TOPIC_ID);
            assert_eq!(produce.topic_data[0].partition_data.len(), 1);
            assert_eq!(produce.topic_data[0].partition_data[0].index, 0);
            assert_partition_base_offsets(&produce, &[0]);
            produce_response_frame_for_request(&header, 0, 40)
        }),
    ])
    .await;
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new({
            let leader_7 = leader_7.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::Metadata as i16);
                let response = metadata_response([(7, leader_7)]);
                response_frame(ApiKey::Metadata, 13, header.correlation_id, &response)
            }
        }),
    ])
    .await;

    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default(),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let producer = Producer::from_parts(
        wire,
        ProducerRuntimeConfig {
            accumulator: AccumulatorConfig::default()
                .batch_size(1)
                .linger(Duration::ZERO)
                .buffer_memory(16 * 1024),
            acks: 1,
            timeout_ms: 30_000,
            retry_attempts: 0,
            retry_backoff: Duration::from_millis(100),
            retry_backoff_max: Duration::from_secs(1),
            delivery_timeout: Duration::from_secs(1),
            max_block: Duration::from_secs(1),
            partitioner_ignore_keys: false,
            partitioner_adaptive_partitioning_enable: true,
            partitioner_availability_timeout: Duration::ZERO,
            max_in_flight_requests_per_connection: 5,
            max_request_size: 1_048_576,
            enable_metrics_push: true,
            compression: ProducerCompression::default(),
            idempotence: idempotence_disabled(),
        },
    );

    let delivery = producer
        .send_with_callback(
            ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")),
            |_| {},
        )
        .unwrap();

    let receipt = tokio::time::timeout(Duration::from_millis(250), delivery)
        .await
        .expect("background sender should dispatch ready batches without flush")
        .expect("delivery should succeed");

    assert_eq!(receipt.offset, 40);
    assert_eq!(producer.buffered_bytes(), 0);
    assert_eq!(bootstrap.join().await, 2);
    assert_eq!(leader_7.join().await, 2);
}

#[tokio::test]
async fn kafka_producer_send_with_callback_invokes_callback_and_returns_delivery() {
    let leader_7 = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::Produce as i16);
            produce_response_frame_for_request(&header, 0, 40)
        }),
    ])
    .await;
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new({
            let leader_7 = leader_7.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::Metadata as i16);
                let response = metadata_response([(7, leader_7)]);
                response_frame(ApiKey::Metadata, 13, header.correlation_id, &response)
            }
        }),
    ])
    .await;

    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default(),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let mut producer = Producer::from_parts(
        wire,
        ProducerRuntimeConfig {
            accumulator: AccumulatorConfig::default()
                .batch_size(16 * 1024)
                .linger(Duration::from_mins(1))
                .buffer_memory(16 * 1024),
            acks: 1,
            timeout_ms: 30_000,
            retry_attempts: 0,
            retry_backoff: Duration::from_millis(100),
            retry_backoff_max: Duration::from_secs(1),
            delivery_timeout: Duration::from_mins(2),
            max_block: Duration::from_mins(1),
            partitioner_ignore_keys: false,
            partitioner_adaptive_partitioning_enable: true,
            partitioner_availability_timeout: Duration::ZERO,
            max_in_flight_requests_per_connection: 5,
            max_request_size: 1_048_576,
            enable_metrics_push: true,
            compression: ProducerCompression::default(),
            idempotence: idempotence_disabled(),
        },
    );
    let callback_receipts = Arc::new(Mutex::new(Vec::new()));
    let callback_sink = Arc::clone(&callback_receipts);

    let delivery = producer
        .send_with_callback(
            ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")),
            move |result| {
                callback_sink
                    .lock()
                    .expect("callback receipts")
                    .push(result.expect("callback receipt"));
            },
        )
        .unwrap();

    producer.flush().await.unwrap();
    let receipt = delivery.await.unwrap();
    let callback_receipts = {
        let callback_receipts = callback_receipts.lock().expect("callback receipts");
        callback_receipts.clone()
    };

    assert_eq!(receipt.offset, 40);
    assert_eq!(callback_receipts.len(), 1);
    assert_eq!(callback_receipts[0], receipt);
    assert_eq!(bootstrap.join().await, 2);
    assert_eq!(leader_7.join().await, 2);
}

#[tokio::test]
async fn kafka_producer_builder_accepts_java_style_config() {
    let leader_7 = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::Produce as i16);
            produce_response_frame_for_request(&header, 0, 40)
        }),
    ])
    .await;
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new({
            let leader_7 = leader_7.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::Metadata as i16);
                let response = metadata_response([(7, leader_7)]);
                response_frame(ApiKey::Metadata, 13, header.correlation_id, &response)
            }
        }),
    ])
    .await;

    let mut producer = Producer::builder()
        .set("bootstrap.servers", bootstrap.addr().to_string())
        .set("enable.idempotence", "false")
        .set("batch.size", "1")
        .set("buffer.memory", "16384")
        .set("acks", "1")
        .set("max.in.flight.requests.per.connection", "2")
        .build()
        .await
        .unwrap();

    let delivery = producer
        .send(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")))
        .unwrap();

    producer.flush().await.unwrap();
    let receipt = delivery.await.unwrap();

    assert_eq!(receipt.offset, 40);
    assert_eq!(bootstrap.join().await, 2);
    assert_eq!(leader_7.join().await, 2);
}

#[tokio::test]
async fn kafka_producer_client_instance_id_uses_telemetry_subscription_like_java() {
    let expected_id = KafkaUuid::from_parts(0x2222_3333_4444_5555, 0x6666_7777_8888_9999);
    let broker = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(move |mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(
                header.request_api_key,
                ApiKey::GetTelemetrySubscriptions as i16
            );
            let body =
                GetTelemetrySubscriptionsRequestData::read(&mut request, 0).expect("telemetry");
            assert_eq!(body.client_instance_id, KafkaUuid::ZERO);
            get_telemetry_subscriptions_response_frame(header.correlation_id, expected_id)
        }),
    ])
    .await;
    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default(),
        "kacrab-test",
        [BrokerEndpoint::new(1, broker.addr())],
    );
    let producer = Producer::from_parts(
        wire,
        ProducerRuntimeConfig {
            enable_metrics_push: true,
            idempotence: idempotence_disabled(),
            ..ProducerRuntimeConfig::default()
        },
    );

    let client_instance_id = producer
        .client_instance_id(Duration::from_secs(1))
        .await
        .expect("client instance id");

    assert_eq!(client_instance_id, expected_id);
    assert_eq!(broker.join().await, 2);
}

#[tokio::test]
async fn kafka_producer_push_telemetry_uses_subscription_identity_like_java() {
    let expected_id = KafkaUuid::from_parts(0xabc, 0xdef);
    let broker = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(move |mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(
                header.request_api_key,
                ApiKey::GetTelemetrySubscriptions as i16
            );
            let body =
                GetTelemetrySubscriptionsRequestData::read(&mut request, 0).expect("telemetry");
            assert_eq!(body.client_instance_id, KafkaUuid::ZERO);
            get_telemetry_subscriptions_response_frame(header.correlation_id, expected_id)
        }),
        Box::new(move |mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::PushTelemetry as i16);
            let body = PushTelemetryRequestData::read(&mut request, 0).expect("push telemetry");
            assert_eq!(body.client_instance_id, expected_id);
            assert_eq!(body.subscription_id, 7);
            assert!(!body.terminating);
            assert_eq!(body.compression_type, 0);
            assert_eq!(body.metrics, Bytes::from_static(b"otlp-metrics"));
            push_telemetry_response_frame(header.correlation_id, ErrorCode::None)
        }),
    ])
    .await;
    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default(),
        "kacrab-test",
        [BrokerEndpoint::new(1, broker.addr())],
    );
    let producer = Producer::from_parts(
        wire,
        ProducerRuntimeConfig {
            enable_metrics_push: true,
            idempotence: idempotence_disabled(),
            ..ProducerRuntimeConfig::default()
        },
    );

    producer
        .push_telemetry(
            Bytes::from_static(b"otlp-metrics"),
            false,
            Duration::from_secs(1),
        )
        .await
        .unwrap();

    assert_eq!(broker.join().await, 3);
}

#[tokio::test]
async fn kafka_producer_push_current_telemetry_aggregates_registered_metrics_like_java() {
    let expected_id = KafkaUuid::from_parts(0xabc, 0xdef);
    let broker = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(move |mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(
                header.request_api_key,
                ApiKey::GetTelemetrySubscriptions as i16
            );
            get_telemetry_subscriptions_response_frame(header.correlation_id, expected_id)
        }),
        Box::new(move |mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::PushTelemetry as i16);
            let body = PushTelemetryRequestData::read(&mut request, 0).expect("push telemetry");
            assert_eq!(body.client_instance_id, expected_id);
            assert_eq!(body.subscription_id, 7);
            assert!(!body.terminating);
            assert!(
                body.metrics
                    .windows(b"queue_depth_bytes".len())
                    .any(|window| window == b"queue_depth_bytes")
            );
            assert!(
                body.metrics
                    .windows(b"orders.sent".len())
                    .any(|window| window == b"orders.sent")
            );
            assert!(
                body.metrics
                    .windows(b"orders-producer".len())
                    .any(|window| window == b"orders-producer")
            );
            push_telemetry_response_frame(header.correlation_id, ErrorCode::None)
        }),
    ])
    .await;
    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default(),
        "kacrab-test",
        [BrokerEndpoint::new(1, broker.addr())],
    );
    let mut producer = Producer::from_parts(
        wire,
        ProducerRuntimeConfig {
            enable_metrics_push: true,
            idempotence: idempotence_disabled(),
            ..ProducerRuntimeConfig::default()
        },
    );
    producer.register_kafka_metric_for_subscription(kacrab::producer::KafkaMetric::from_fn(
        kacrab::producer::MetricName::new("orders.sent", "app").tag("client-id", "orders-producer"),
        || kacrab::producer::MetricValue::Number(42.0),
    ));

    producer
        .push_current_telemetry(false, Duration::from_secs(1))
        .await
        .unwrap();

    assert_eq!(broker.join().await, 3);
}

#[tokio::test]
async fn kafka_producer_push_telemetry_retries_unknown_subscription_like_java() {
    let expected_id = KafkaUuid::from_parts(0xabc, 0xdef);
    let broker = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(move |mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(
                header.request_api_key,
                ApiKey::GetTelemetrySubscriptions as i16
            );
            let body =
                GetTelemetrySubscriptionsRequestData::read(&mut request, 0).expect("telemetry");
            assert_eq!(body.client_instance_id, KafkaUuid::ZERO);
            get_telemetry_subscriptions_response_frame_with_subscription(
                header.correlation_id,
                expected_id,
                7,
            )
        }),
        Box::new(move |mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::PushTelemetry as i16);
            let body = PushTelemetryRequestData::read(&mut request, 0).expect("push telemetry");
            assert_eq!(body.client_instance_id, expected_id);
            assert_eq!(body.subscription_id, 7);
            push_telemetry_response_frame(header.correlation_id, ErrorCode::UnknownSubscriptionId)
        }),
        Box::new(move |mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(
                header.request_api_key,
                ApiKey::GetTelemetrySubscriptions as i16
            );
            let body =
                GetTelemetrySubscriptionsRequestData::read(&mut request, 0).expect("telemetry");
            assert_eq!(body.client_instance_id, expected_id);
            get_telemetry_subscriptions_response_frame_with_subscription(
                header.correlation_id,
                KafkaUuid::ZERO,
                9,
            )
        }),
        Box::new(move |mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::PushTelemetry as i16);
            let body = PushTelemetryRequestData::read(&mut request, 0).expect("push telemetry");
            assert_eq!(body.client_instance_id, expected_id);
            assert_eq!(body.subscription_id, 9);
            assert_eq!(body.metrics, Bytes::from_static(b"first-payload"));
            push_telemetry_response_frame(header.correlation_id, ErrorCode::None)
        }),
    ])
    .await;
    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default(),
        "kacrab-test",
        [BrokerEndpoint::new(1, broker.addr())],
    );
    let producer = Producer::from_parts(
        wire,
        ProducerRuntimeConfig {
            enable_metrics_push: true,
            idempotence: idempotence_disabled(),
            ..ProducerRuntimeConfig::default()
        },
    );

    producer
        .push_telemetry(
            Bytes::from_static(b"first-payload"),
            false,
            Duration::from_secs(1),
        )
        .await
        .expect("unknown subscription should refresh and retry once");

    assert_eq!(broker.join().await, 5);
}

#[tokio::test]
async fn kafka_producer_push_telemetry_disables_after_invalid_request_like_java() {
    let expected_id = KafkaUuid::from_parts(0xabc, 0xdef);
    let broker = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(move |mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(
                header.request_api_key,
                ApiKey::GetTelemetrySubscriptions as i16
            );
            let body =
                GetTelemetrySubscriptionsRequestData::read(&mut request, 0).expect("telemetry");
            assert_eq!(body.client_instance_id, KafkaUuid::ZERO);
            get_telemetry_subscriptions_response_frame(header.correlation_id, expected_id)
        }),
        Box::new(move |mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::PushTelemetry as i16);
            let body = PushTelemetryRequestData::read(&mut request, 0).expect("push telemetry");
            assert_eq!(body.client_instance_id, expected_id);
            assert_eq!(body.subscription_id, 7);
            push_telemetry_response_frame(header.correlation_id, ErrorCode::InvalidRequest)
        }),
    ])
    .await;
    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default(),
        "kacrab-test",
        [BrokerEndpoint::new(1, broker.addr())],
    );
    let producer = Producer::from_parts(
        wire,
        ProducerRuntimeConfig {
            enable_metrics_push: true,
            idempotence: idempotence_disabled(),
            ..ProducerRuntimeConfig::default()
        },
    );

    let first = producer
        .push_telemetry(
            Bytes::from_static(b"first-payload"),
            false,
            Duration::from_secs(1),
        )
        .await
        .expect_err("invalid telemetry request should be surfaced once");
    assert!(matches!(
        first,
        kacrab::producer::ProducerError::Telemetry {
            operation: "push_telemetry",
            error: ErrorCode::InvalidRequest
        }
    ));

    let second = producer
        .push_telemetry(
            Bytes::from_static(b"second-payload"),
            false,
            Duration::from_secs(1),
        )
        .await
        .expect_err("invalid request should disable future telemetry pushes");
    assert!(matches!(
        second,
        kacrab::producer::ProducerError::TelemetryDisabled
    ));

    assert_eq!(broker.join().await, 3);
}

#[tokio::test]
async fn kafka_producer_get_telemetry_subscriptions_disables_after_invalid_request_like_java() {
    let broker = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(move |mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(
                header.request_api_key,
                ApiKey::GetTelemetrySubscriptions as i16
            );
            let body =
                GetTelemetrySubscriptionsRequestData::read(&mut request, 0).expect("telemetry");
            assert_eq!(body.client_instance_id, KafkaUuid::ZERO);
            get_telemetry_subscriptions_error_response_frame(
                header.correlation_id,
                ErrorCode::InvalidRequest,
            )
        }),
    ])
    .await;
    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default(),
        "kacrab-test",
        [BrokerEndpoint::new(1, broker.addr())],
    );
    let producer = Producer::from_parts(
        wire,
        ProducerRuntimeConfig {
            enable_metrics_push: true,
            idempotence: idempotence_disabled(),
            ..ProducerRuntimeConfig::default()
        },
    );

    let first = producer
        .client_instance_id(Duration::from_secs(1))
        .await
        .expect_err("invalid telemetry subscription request should be surfaced once");
    assert!(matches!(
        first,
        kacrab::producer::ProducerError::Telemetry {
            operation: "get_telemetry_subscriptions",
            error: ErrorCode::InvalidRequest
        }
    ));

    let second = producer
        .client_instance_id(Duration::from_secs(1))
        .await
        .expect_err("invalid subscription request should disable future telemetry calls");
    assert!(matches!(
        second,
        kacrab::producer::ProducerError::TelemetryDisabled
    ));

    assert_eq!(broker.join().await, 2);
}

#[tokio::test]
async fn kafka_producer_partitions_for_returns_topic_metadata() {
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::Metadata as i16);
            let response = metadata_response([(7, "127.0.0.1:9092".parse().expect("socket addr"))]);
            response_frame(ApiKey::Metadata, 13, header.correlation_id, &response)
        }),
    ])
    .await;

    let producer = Producer::builder()
        .set("bootstrap.servers", bootstrap.addr().to_string())
        .set("enable.idempotence", "false")
        .build()
        .await
        .unwrap();

    assert_eq!(producer.metrics().metadata_wait_count, 0);
    let partitions = producer.partitions_for("orders").await.unwrap();
    let metrics = producer.metrics();

    assert_eq!(partitions.len(), 2);
    assert_eq!(partitions[0].topic, "orders");
    assert_eq!(partitions[0].partition, 0);
    assert_eq!(partitions[0].leader_id, 7);
    assert_eq!(partitions[1].partition, 1);
    assert_eq!(partitions[1].leader_id, 8);
    assert_eq!(metrics.metadata_wait_count, 1);
    assert!(metrics.metadata_wait_total_latency >= Duration::ZERO);
    assert_eq!(bootstrap.join().await, 2);
}

#[tokio::test]
async fn kafka_producer_interceptor_send_error_uses_assigned_partition_like_java() {
    let captured = Arc::new(Mutex::new(None));
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::Metadata as i16);
            let response = metadata_response([(7, "127.0.0.1:9092".parse().expect("socket addr"))]);
            response_frame(ApiKey::Metadata, 13, header.correlation_id, &response)
        }),
    ])
    .await;
    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default(),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let mut producer = Producer::from_parts(
        wire,
        ProducerRuntimeConfig {
            accumulator: AccumulatorConfig::default(),
            max_request_size: 8,
            partitioner_ignore_keys: false,
            partitioner_adaptive_partitioning_enable: false,
            idempotence: idempotence_disabled(),
            ..ProducerRuntimeConfig::default()
        },
    );
    producer.add_interceptor(CaptureSendErrorMetadata {
        metadata: Arc::clone(&captured),
    });

    // A keyed, unassigned record on a cold-metadata topic resolves its partition
    // asynchronously, so the too-large error surfaces on the returned delivery
    // future (after the partition is assigned) rather than from the sync send call.
    let error = producer
        .send(
            ProducerRecord::unassigned("orders")
                .key(Bytes::from_static(b"customer-42"))
                .value(Bytes::from_static(b"value")),
        )
        .expect("oversized record is enqueued for async partition assignment")
        .await
        .expect_err("record should exceed max.request.size");
    let metadata = captured
        .lock()
        .unwrap()
        .clone()
        .expect("interceptor metadata");

    assert!(matches!(
        error,
        kacrab::producer::ProducerError::RecordTooLarge { .. }
    ));
    assert_eq!(metadata.topic.as_ref(), "orders");
    assert_eq!(metadata.partition, 1);
    assert_eq!(metadata.offset, -1);
    assert_eq!(bootstrap.join().await, 2);
}

#[tokio::test]
async fn kafka_producer_builder_uses_native_partitioner_instead_of_jvm_class_loading() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let leader_8 = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::Produce as i16);
            let produce = ProduceRequestData::read(&mut request, header.request_api_version)
                .expect("produce request");
            assert_eq!(produce.topic_data[0].partition_data[0].index, 1);
            produce_response_frame_for_request(&header, 1, 80)
        }),
    ])
    .await;
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new({
            let leader_8 = leader_8.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::Metadata as i16);
                let response = metadata_response([
                    (7, "127.0.0.1:9092".parse().expect("socket addr")),
                    (8, leader_8),
                ]);
                response_frame(ApiKey::Metadata, 13, header.correlation_id, &response)
            }
        }),
    ])
    .await;

    let mut producer = Producer::builder()
        .set("bootstrap.servers", bootstrap.addr().to_string())
        .set("enable.idempotence", "false")
        .set("batch.size", "1")
        .set("buffer.memory", "16384")
        .set("acks", "1")
        .set("max.in.flight.requests.per.connection", "2")
        .set(
            "partitioner.class",
            "org.apache.kafka.clients.producer.RoundRobinPartitioner",
        )
        .partitioner(FixedNativePartitioner {
            partition: 1,
            calls: Arc::clone(&calls),
        })
        .build()
        .await
        .unwrap();

    let delivery = producer
        .send(ProducerRecord::unassigned("orders").value(Bytes::from_static(b"a")))
        .unwrap();

    producer.flush().await.unwrap();
    let receipt = delivery.await.unwrap();
    let calls = {
        let calls = calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        calls.clone()
    };

    assert_eq!(calls.as_slice(), &["orders"]);
    assert_eq!(receipt.partition, 1);
    assert_eq!(receipt.leader_id, 8);
    assert_eq!(receipt.offset, 80);
    assert_eq!(bootstrap.join().await, 2);
    assert_eq!(leader_8.join().await, 2);
}

#[tokio::test]
async fn kafka_producer_send_auto_batches_per_record_sends_until_flush() {
    let leader_7 = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::Produce as i16);
            let produce = ProduceRequestData::read(&mut request, header.request_api_version)
                .expect("produce request");
            assert_eq!(produce.topic_data.len(), 1);
            assert_eq!(produce.topic_data[0].topic_id, TOPIC_ID);
            assert_eq!(produce.topic_data[0].partition_data.len(), 1);
            assert_eq!(produce.topic_data[0].partition_data[0].index, 0);
            assert_partition_base_offsets(&produce, &[0]);
            let mut records = produce.topic_data[0].partition_data[0]
                .records
                .clone()
                .expect("records");
            let batches = decode_batches(&mut records).expect("record batches");
            assert_eq!(batches.len(), 1);
            assert_eq!(batches[0].records.len(), 2);
            produce_response_frame_for_request(&header, 0, 40)
        }),
    ])
    .await;
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new({
            let leader_7 = leader_7.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::Metadata as i16);
                let response = metadata_response([(7, leader_7)]);
                response_frame(ApiKey::Metadata, 13, header.correlation_id, &response)
            }
        }),
    ])
    .await;

    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default(),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let mut producer = Producer::from_parts(
        wire,
        ProducerRuntimeConfig {
            accumulator: AccumulatorConfig::default()
                .batch_size(16 * 1024)
                .linger(Duration::from_mins(1))
                .buffer_memory(16 * 1024),
            acks: 1,
            timeout_ms: 30_000,
            retry_attempts: 0,
            retry_backoff: Duration::from_millis(100),
            retry_backoff_max: Duration::from_secs(1),
            delivery_timeout: Duration::from_mins(2),
            max_block: Duration::from_mins(1),
            partitioner_ignore_keys: false,
            partitioner_adaptive_partitioning_enable: true,
            partitioner_availability_timeout: Duration::ZERO,
            max_in_flight_requests_per_connection: 5,
            max_request_size: 1_048_576,
            enable_metrics_push: true,
            compression: ProducerCompression::default(),
            idempotence: idempotence_disabled(),
        },
    );

    let first = producer
        .send(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")))
        .unwrap();
    let second = producer
        .send(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"b")))
        .unwrap();

    producer.flush().await.unwrap();

    let first = first.await.unwrap();
    let second = second.await.unwrap();
    assert_eq!(first.offset, 40);
    assert_eq!(second.offset, 41);
    assert_eq!(producer.buffered_bytes(), 0);
    assert_eq!(bootstrap.join().await, 2);
    assert_eq!(leader_7.join().await, 2);
}

#[derive(Debug)]
struct FixedNativePartitioner {
    partition: i32,
    calls: Arc<Mutex<Vec<String>>>,
}

impl ProducerPartitioner for FixedNativePartitioner {
    fn partition(
        &self,
        record: &ProducerRecord,
        metadata: &ClusterMetadata,
    ) -> kacrab::producer::Result<i32> {
        assert!(metadata.topic(record.topic.as_ref()).is_some());
        self.calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(record.topic.to_string());
        Ok(self.partition)
    }
}

#[tokio::test]
async fn kafka_producer_send_with_callback_auto_batches_until_flush() {
    let leader_7 = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::Produce as i16);
            let produce = ProduceRequestData::read(&mut request, header.request_api_version)
                .expect("produce request");
            assert_eq!(produce.topic_data.len(), 1);
            assert_eq!(produce.topic_data[0].topic_id, TOPIC_ID);
            assert_eq!(produce.topic_data[0].partition_data.len(), 1);
            assert_eq!(produce.topic_data[0].partition_data[0].index, 0);
            assert_partition_base_offsets(&produce, &[0]);
            let mut records = produce.topic_data[0].partition_data[0]
                .records
                .clone()
                .expect("records");
            let batches = decode_batches(&mut records).expect("record batches");
            assert_eq!(batches.len(), 1);
            assert_eq!(batches[0].records.len(), 2);
            produce_response_frame_for_request(&header, 0, 40)
        }),
    ])
    .await;
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new({
            let leader_7 = leader_7.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::Metadata as i16);
                let response = metadata_response([(7, leader_7)]);
                response_frame(ApiKey::Metadata, 13, header.correlation_id, &response)
            }
        }),
    ])
    .await;

    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default(),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let mut producer = Producer::from_parts(
        wire,
        ProducerRuntimeConfig {
            accumulator: AccumulatorConfig::default()
                .batch_size(16 * 1024)
                .linger(Duration::from_mins(1))
                .buffer_memory(16 * 1024),
            acks: 1,
            timeout_ms: 30_000,
            retry_attempts: 0,
            retry_backoff: Duration::from_millis(100),
            retry_backoff_max: Duration::from_secs(1),
            delivery_timeout: Duration::from_mins(2),
            max_block: Duration::from_mins(1),
            partitioner_ignore_keys: false,
            partitioner_adaptive_partitioning_enable: true,
            partitioner_availability_timeout: Duration::ZERO,
            max_in_flight_requests_per_connection: 5,
            max_request_size: 1_048_576,
            enable_metrics_push: true,
            compression: ProducerCompression::default(),
            idempotence: idempotence_disabled(),
        },
    );

    let delivered = Arc::new(AtomicUsize::new(0));
    let first_delivered = Arc::clone(&delivered);
    let _first_delivery = producer
        .send_with_callback(
            ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")),
            move |result| {
                assert!(result.is_ok());
                let _previous = first_delivered.fetch_add(1, Ordering::Relaxed);
            },
        )
        .unwrap();
    let second_delivered = Arc::clone(&delivered);
    let _second_delivery = producer
        .send_with_callback(
            ProducerRecord::new("orders", 0).value(Bytes::from_static(b"b")),
            move |result| {
                assert!(result.is_ok());
                let _previous = second_delivered.fetch_add(1, Ordering::Relaxed);
            },
        )
        .unwrap();
    producer.flush().await.unwrap();

    assert_eq!(delivered.load(Ordering::Relaxed), 2);
    assert_eq!(producer.buffered_bytes(), 0);
    assert_eq!(bootstrap.join().await, 2);
    assert_eq!(leader_7.join().await, 2);
}

#[tokio::test]
async fn kafka_producer_pipelines_ready_batches_until_flush() {
    let leader_7 = MockBroker::serve_pipelined_produce(2).await;
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new({
            let leader_7 = leader_7.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::Metadata as i16);
                let response = metadata_response([(7, leader_7)]);
                response_frame(ApiKey::Metadata, 13, header.correlation_id, &response)
            }
        }),
    ])
    .await;

    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default()
            .max_in_flight_requests_per_connection(2)
            .broker_queue_capacity(2)
            .request_timeout(Duration::from_secs(1)),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let mut producer = Producer::from_parts(
        wire,
        ProducerRuntimeConfig {
            accumulator: AccumulatorConfig::default()
                .batch_size(1)
                .buffer_memory(16 * 1024),
            acks: 1,
            timeout_ms: 30_000,
            retry_attempts: 0,
            retry_backoff: Duration::from_millis(100),
            retry_backoff_max: Duration::from_secs(1),
            delivery_timeout: Duration::from_mins(2),
            max_block: Duration::from_mins(1),
            partitioner_ignore_keys: false,
            partitioner_adaptive_partitioning_enable: true,
            partitioner_availability_timeout: Duration::ZERO,
            max_in_flight_requests_per_connection: 2,
            max_request_size: 1_048_576,
            enable_metrics_push: true,
            compression: ProducerCompression::default(),
            idempotence: idempotence_disabled(),
        },
    );

    let first_delivery = producer
        .send(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")))
        .unwrap();
    let second_delivery = producer
        .send(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"b")))
        .unwrap();

    producer.flush().await.unwrap();
    let mut receipts = [
        first_delivery.await.unwrap(),
        second_delivery.await.unwrap(),
    ];
    receipts.sort_by_key(|receipt| receipt.offset);

    assert_eq!(receipts.len(), 2);
    assert_eq!(receipts[0].offset, 40);
    assert_eq!(receipts[1].offset, 41);
    assert_eq!(producer.buffered_bytes(), 0);
    assert_eq!(bootstrap.join().await, 2);
    assert_eq!(leader_7.join().await, 3);
}

#[tokio::test]
async fn kafka_producer_single_send_budget_coalesces_ready_partitions() {
    let leader_7 = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::Produce as i16);
            let produce = ProduceRequestData::read(&mut request, header.request_api_version)
                .expect("produce request");
            assert_eq!(produce.topic_data.len(), 1);
            let topic = produce.topic_data.first().expect("topic produce data");
            let mut partitions = topic
                .partition_data
                .iter()
                .map(|partition| partition.index)
                .collect::<Vec<_>>();
            partitions.sort_unstable();
            assert_eq!(partitions, vec![0, 1]);
            produce_response_frame_for_partitions(&header, &[(0, 40), (1, 41)])
        }),
    ])
    .await;
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new({
            let leader_7 = leader_7.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::Metadata as i16);
                let response = metadata_response_same_leader(7, leader_7);
                response_frame(ApiKey::Metadata, 13, header.correlation_id, &response)
            }
        }),
    ])
    .await;

    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default()
            .max_in_flight_requests_per_connection(2)
            .broker_queue_capacity(2)
            .request_timeout(Duration::from_secs(1)),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let mut producer = Producer::from_parts(
        wire,
        ProducerRuntimeConfig {
            accumulator: AccumulatorConfig::default()
                .batch_size(1)
                .buffer_memory(16 * 1024),
            acks: 1,
            timeout_ms: 30_000,
            retry_attempts: 0,
            retry_backoff: Duration::from_millis(100),
            retry_backoff_max: Duration::from_secs(1),
            delivery_timeout: Duration::from_mins(2),
            max_block: Duration::from_mins(1),
            partitioner_ignore_keys: false,
            partitioner_adaptive_partitioning_enable: true,
            partitioner_availability_timeout: Duration::ZERO,
            max_in_flight_requests_per_connection: 2,
            max_request_size: 1_048_576,
            enable_metrics_push: true,
            compression: ProducerCompression::default(),
            idempotence: idempotence_disabled(),
        },
    );
    producer.enable_metrics();

    let first_delivery = producer
        .send_with_callback(
            ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")),
            |_| {},
        )
        .unwrap();
    assert_eq!(producer.metrics().produce_request_count, 0);

    let second_delivery = producer
        .send_with_callback(
            ProducerRecord::new("orders", 1).value(Bytes::from_static(b"b")),
            |_| {},
        )
        .unwrap();

    producer.flush().await.unwrap();
    let mut receipts = [
        first_delivery.await.unwrap(),
        second_delivery.await.unwrap(),
    ];
    receipts.sort_by_key(|receipt| receipt.partition);

    assert_eq!(receipts[0].partition, 0);
    assert_eq!(receipts[0].offset, 40);
    assert_eq!(receipts[1].partition, 1);
    assert_eq!(receipts[1].offset, 41);
    let metrics = producer.metrics();
    assert_eq!(metrics.produce_request_count, 1);
    assert_eq!(metrics.produce_batch_count, 2);
    assert_eq!(metrics.produce_record_count, 2);
    assert_eq!(bootstrap.join().await, 2);
    assert_eq!(leader_7.join().await, 2);
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "Observable max.request.size proof needs mock broker, metadata, producer, and \
              assertion setup in one integration scenario."
)]
async fn kafka_producer_10kib_records_keep_observed_requests_under_max_request_size() {
    const PARTITIONS: usize = 120;
    const MAX_REQUEST_SIZE: usize = 1_048_576;
    const VALUE_SIZE: usize = 10 * 1024;

    let observed_request_lengths = Arc::new(Mutex::new(Vec::new()));
    let observed_partition_groups = Arc::new(Mutex::new(Vec::new()));
    let leader_7 = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        capture_produce_request(
            Arc::clone(&observed_request_lengths),
            Arc::clone(&observed_partition_groups),
        ),
        capture_produce_request(
            Arc::clone(&observed_request_lengths),
            Arc::clone(&observed_partition_groups),
        ),
    ])
    .await;
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new({
            let leader_7 = leader_7.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::Metadata as i16);
                let response = metadata_response_same_leader_partitions(7, leader_7, PARTITIONS);
                response_frame(ApiKey::Metadata, 13, header.correlation_id, &response)
            }
        }),
    ])
    .await;

    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default()
            .max_in_flight_requests_per_connection(5)
            .broker_queue_capacity(5)
            .request_timeout(Duration::from_secs(1)),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let mut producer = Producer::from_parts(
        wire,
        ProducerRuntimeConfig {
            accumulator: AccumulatorConfig::default()
                .batch_size(16 * 1024)
                .linger(Duration::from_mins(1))
                .buffer_memory(2 * MAX_REQUEST_SIZE),
            acks: 1,
            timeout_ms: 30_000,
            retry_attempts: 0,
            retry_backoff: Duration::from_millis(100),
            retry_backoff_max: Duration::from_secs(1),
            delivery_timeout: Duration::from_mins(2),
            max_block: Duration::from_mins(1),
            partitioner_ignore_keys: false,
            partitioner_adaptive_partitioning_enable: true,
            partitioner_availability_timeout: Duration::ZERO,
            max_in_flight_requests_per_connection: 5,
            max_request_size: MAX_REQUEST_SIZE,
            enable_metrics_push: true,
            compression: ProducerCompression::default(),
            idempotence: idempotence_disabled(),
        },
    );
    producer.enable_metrics();

    let value = Bytes::from(vec![b'x'; VALUE_SIZE]);
    let mut deliveries = Vec::with_capacity(PARTITIONS);
    for partition in 0..PARTITIONS {
        let partition = i32::try_from(partition).expect("partition id should fit i32");
        let delivery = producer
            .send(ProducerRecord::new("orders", partition).value(value.clone()))
            .expect("send 10KiB record");
        deliveries.push(delivery);
    }

    producer.flush().await.expect("flush 10KiB partition batch");
    let mut receipts = Vec::with_capacity(deliveries.len());
    for delivery in deliveries {
        receipts.push(delivery.await.expect("delivery receipt"));
    }

    let observed_request_lengths = observed_request_lengths
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    let observed_partition_groups = observed_partition_groups
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();

    assert_eq!(receipts.len(), PARTITIONS);
    assert_eq!(observed_request_lengths.len(), 2);
    assert!(
        observed_request_lengths
            .iter()
            .all(|length| *length <= MAX_REQUEST_SIZE)
    );
    assert_eq!(
        observed_partition_groups
            .iter()
            .map(Vec::len)
            .sum::<usize>(),
        PARTITIONS
    );
    let mut observed_partitions = observed_partition_groups
        .iter()
        .flatten()
        .copied()
        .collect::<Vec<_>>();
    observed_partitions.sort_unstable();
    assert_eq!(
        observed_partitions,
        (0..PARTITIONS)
            .map(|partition| i32::try_from(partition).expect("partition id should fit i32"))
            .collect::<Vec<_>>()
    );
    let metrics = producer.metrics();
    assert_eq!(metrics.produce_request_count, 2);
    assert_eq!(metrics.produce_request_split_count, 1);
    assert_eq!(metrics.produce_retry_count, 0);
    assert_eq!(metrics.produce_error_count, 0);
    assert_eq!(bootstrap.join().await, 2);
    assert_eq!(leader_7.join().await, 3);
}

#[tokio::test]
async fn idempotent_kafka_producer_pipelines_different_partitions_until_flush() {
    let leader_7 = MockBroker::serve_pipelined_idempotent_produce(vec![0, 1]).await;
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new({
            let leader_7 = leader_7.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::Metadata as i16);
                let response = metadata_response_same_leader(7, leader_7);
                response_frame(ApiKey::Metadata, 13, header.correlation_id, &response)
            }
        }),
    ])
    .await;

    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default()
            .max_in_flight_requests_per_connection(2)
            .broker_queue_capacity(2)
            .request_timeout(Duration::from_secs(1)),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let mut producer = Producer::from_parts(
        wire,
        ProducerRuntimeConfig {
            accumulator: AccumulatorConfig::default()
                .batch_size(1)
                .buffer_memory(16 * 1024),
            acks: -1,
            timeout_ms: 30_000,
            retry_attempts: 0,
            retry_backoff: Duration::from_millis(100),
            retry_backoff_max: Duration::from_secs(1),
            delivery_timeout: Duration::from_mins(2),
            max_block: Duration::from_mins(1),
            partitioner_ignore_keys: false,
            partitioner_adaptive_partitioning_enable: true,
            partitioner_availability_timeout: Duration::ZERO,
            max_in_flight_requests_per_connection: 2,
            max_request_size: 1_048_576,
            enable_metrics_push: true,
            compression: ProducerCompression::default(),
            idempotence: ProducerIdempotenceConfig {
                enabled: true,
                transactional_id: None,
                transaction_timeout_ms: 60_000,
                transaction_two_phase_commit: false,
            },
        },
    );
    let partitions = producer.partitions_for("orders").await.unwrap();
    assert_eq!(partitions.len(), 2);

    let first_delivery = producer
        .send(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")))
        .unwrap();
    let second_delivery = producer
        .send(ProducerRecord::new("orders", 1).value(Bytes::from_static(b"b")))
        .unwrap();

    producer.flush().await.unwrap();
    let mut receipts = [
        first_delivery.await.unwrap(),
        second_delivery.await.unwrap(),
    ];
    receipts.sort_by_key(|receipt| receipt.partition);

    assert_eq!(receipts[0].partition, 0);
    assert_eq!(receipts[0].offset, 40);
    assert_eq!(receipts[1].partition, 1);
    assert_eq!(receipts[1].offset, 41);
    assert_eq!(producer.buffered_bytes(), 0);
    assert_eq!(bootstrap.join().await, 2);
    // handshake + InitProducerId + one coalesced Produce request for both partitions.
    assert_eq!(leader_7.join().await, 3);
}

#[tokio::test]
async fn idempotent_kafka_producer_maps_reordered_pipelined_responses_by_correlation() {
    let leader_7 = MockBroker::serve_pipelined_idempotent_produce_reversed(vec![0, 1]).await;
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new({
            let leader_7 = leader_7.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::Metadata as i16);
                let response = metadata_response_same_leader(7, leader_7);
                response_frame(ApiKey::Metadata, 13, header.correlation_id, &response)
            }
        }),
    ])
    .await;

    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default()
            .max_in_flight_requests_per_connection(2)
            .broker_queue_capacity(2)
            .request_timeout(Duration::from_secs(1)),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let mut producer = Producer::from_parts(
        wire,
        ProducerRuntimeConfig {
            accumulator: AccumulatorConfig::default()
                .batch_size(1)
                .buffer_memory(16 * 1024),
            acks: -1,
            timeout_ms: 30_000,
            retry_attempts: 0,
            retry_backoff: Duration::from_millis(100),
            retry_backoff_max: Duration::from_secs(1),
            delivery_timeout: Duration::from_mins(2),
            max_block: Duration::from_mins(1),
            partitioner_ignore_keys: false,
            partitioner_adaptive_partitioning_enable: true,
            partitioner_availability_timeout: Duration::ZERO,
            max_in_flight_requests_per_connection: 2,
            max_request_size: 1_048_576,
            enable_metrics_push: true,
            compression: ProducerCompression::default(),
            idempotence: ProducerIdempotenceConfig {
                enabled: true,
                transactional_id: None,
                transaction_timeout_ms: 60_000,
                transaction_two_phase_commit: false,
            },
        },
    );
    let partitions = producer.partitions_for("orders").await.unwrap();
    assert_eq!(partitions.len(), 2);

    let first_delivery = producer
        .send(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")))
        .unwrap();
    let second_delivery = producer
        .send(ProducerRecord::new("orders", 1).value(Bytes::from_static(b"b")))
        .unwrap();

    producer.flush().await.unwrap();
    let first = first_delivery.await.unwrap();
    let second = second_delivery.await.unwrap();

    assert_eq!(first.partition, 0);
    assert_eq!(first.offset, 40);
    assert_eq!(second.partition, 1);
    assert_eq!(second.offset, 41);
    assert_eq!(producer.buffered_bytes(), 0);
    assert_eq!(bootstrap.join().await, 2);
    // handshake + InitProducerId + one coalesced Produce request for both partitions.
    assert_eq!(leader_7.join().await, 3);
}

#[tokio::test]
async fn idempotent_kafka_producer_retries_disconnected_in_flight_batch_with_same_sequence() {
    let leader_7 = MockBroker::serve_idempotent_disconnect_then_retry().await;
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new({
            let leader_7 = leader_7.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::Metadata as i16);
                let response = metadata_response_same_leader(7, leader_7);
                response_frame(ApiKey::Metadata, 13, header.correlation_id, &response)
            }
        }),
    ])
    .await;
    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default()
            .max_in_flight_requests_per_connection(1)
            .broker_queue_capacity(2)
            .request_timeout(Duration::from_millis(100)),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let mut producer = Producer::from_parts(
        wire,
        ProducerRuntimeConfig {
            accumulator: AccumulatorConfig::default()
                .batch_size(1)
                .buffer_memory(16 * 1024),
            acks: -1,
            timeout_ms: 30_000,
            retry_attempts: 1,
            retry_backoff: Duration::from_millis(1),
            retry_backoff_max: Duration::from_millis(1),
            delivery_timeout: Duration::from_secs(2),
            max_block: Duration::from_mins(1),
            partitioner_ignore_keys: false,
            partitioner_adaptive_partitioning_enable: true,
            partitioner_availability_timeout: Duration::ZERO,
            max_in_flight_requests_per_connection: 1,
            max_request_size: 1_048_576,
            enable_metrics_push: true,
            compression: ProducerCompression::default(),
            idempotence: ProducerIdempotenceConfig {
                enabled: true,
                transactional_id: None,
                transaction_timeout_ms: 60_000,
                transaction_two_phase_commit: false,
            },
        },
    );

    let delivery = producer
        .send(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")))
        .unwrap();

    producer.flush().await.unwrap();
    let receipt = delivery.await.unwrap();

    assert_eq!(receipt.partition, 0);
    assert_eq!(receipt.offset, 40);
    assert_eq!(producer.buffered_bytes(), 0);
    assert_eq!(bootstrap.join().await, 2);
    assert_eq!(leader_7.join().await, 5);
}

#[tokio::test]
async fn idempotent_kafka_producer_recovers_unresolved_sequence_after_delivery_timeout_like_java() {
    let leader_7 = MockBroker::serve_idempotent_timeout_then_epoch_bump_recovery().await;
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new({
            let leader_7 = leader_7.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::Metadata as i16);
                let response = metadata_response_same_leader(7, leader_7);
                response_frame(ApiKey::Metadata, 13, header.correlation_id, &response)
            }
        }),
    ])
    .await;
    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default()
            .max_in_flight_requests_per_connection(1)
            .broker_queue_capacity(2)
            .request_timeout(Duration::from_millis(10)),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let mut producer = Producer::from_parts(
        wire,
        ProducerRuntimeConfig {
            accumulator: AccumulatorConfig::default()
                .batch_size(1)
                .buffer_memory(16 * 1024),
            acks: -1,
            timeout_ms: 30_000,
            retry_attempts: 1,
            retry_backoff: Duration::from_millis(1),
            retry_backoff_max: Duration::from_millis(1),
            delivery_timeout: Duration::from_millis(1),
            max_block: Duration::from_mins(1),
            partitioner_ignore_keys: false,
            partitioner_adaptive_partitioning_enable: true,
            partitioner_availability_timeout: Duration::ZERO,
            max_in_flight_requests_per_connection: 1,
            max_request_size: 1_048_576,
            enable_metrics_push: true,
            compression: ProducerCompression::default(),
            idempotence: ProducerIdempotenceConfig {
                enabled: true,
                transactional_id: None,
                transaction_timeout_ms: 60_000,
                transaction_two_phase_commit: false,
            },
        },
    );

    let first_delivery = producer
        .send(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")))
        .unwrap();
    let first_error = producer
        .flush()
        .await
        .expect_err("first sent batch should hit delivery timeout");
    assert!(matches!(
        first_error,
        kacrab::producer::ProducerError::DeliveryTimeout { .. }
    ));
    assert!(first_delivery.await.is_err());

    let second_delivery = producer
        .send(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"b")))
        .unwrap();
    producer.flush().await.unwrap();
    let receipt = second_delivery.await.unwrap();

    assert_eq!(receipt.partition, 0);
    assert_eq!(receipt.offset, 41);
    assert_eq!(producer.buffered_bytes(), 0);
    assert_eq!(bootstrap.join().await, 2);
    assert_eq!(leader_7.join().await, 6);
}

#[tokio::test]
async fn idempotent_kafka_producer_resends_multi_inflight_batches_in_sequence_order_after_retry() {
    // End-to-end fault injection for the firstInFlightSequence gate: two batches are
    // pipelined in flight to one partition (multi-in-flight), the connection drops, and
    // the producer must re-send them strictly in base-sequence order on retry.
    let leader_7 = MockBroker::serve_idempotent_two_inflight_disconnect_then_inorder_retry().await;
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new({
            let leader_7 = leader_7.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::Metadata as i16);
                let response = metadata_response_same_leader(7, leader_7);
                response_frame(ApiKey::Metadata, 13, header.correlation_id, &response)
            }
        }),
    ])
    .await;
    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default()
            .max_in_flight_requests_per_connection(5)
            .broker_queue_capacity(8)
            .request_timeout(Duration::from_secs(30)),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let mut producer = Producer::from_parts(
        wire,
        ProducerRuntimeConfig {
            accumulator: AccumulatorConfig::default()
                .batch_size(1)
                .buffer_memory(16 * 1024),
            acks: -1,
            timeout_ms: 30_000,
            retry_attempts: 5,
            retry_backoff: Duration::from_millis(1),
            retry_backoff_max: Duration::from_millis(1),
            // Generous so the disconnect is a plain wire retry, not a delivery timeout
            // (which would bump the epoch and reset the sequences).
            delivery_timeout: Duration::from_secs(30),
            max_block: Duration::from_mins(1),
            partitioner_ignore_keys: false,
            partitioner_adaptive_partitioning_enable: true,
            partitioner_availability_timeout: Duration::ZERO,
            max_in_flight_requests_per_connection: 5,
            max_request_size: 1_048_576,
            enable_metrics_push: true,
            compression: ProducerCompression::default(),
            idempotence: ProducerIdempotenceConfig {
                enabled: true,
                transactional_id: None,
                transaction_timeout_ms: 60_000,
                transaction_two_phase_commit: false,
            },
        },
    );

    let first_delivery = producer
        .send(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")))
        .unwrap();
    let second_delivery = producer
        .send(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"b")))
        .unwrap();
    producer.flush().await.unwrap();

    let first_receipt = first_delivery.await.unwrap();
    let second_receipt = second_delivery.await.unwrap();
    assert_eq!(first_receipt.partition, 0);
    assert_eq!(first_receipt.offset, 40);
    assert_eq!(second_receipt.partition, 0);
    assert_eq!(second_receipt.offset, 41);
    assert_eq!(producer.buffered_bytes(), 0);
    assert_eq!(bootstrap.join().await, 2);
    // The mock asserts in-order base sequences (0 then 1) on BOTH the initial in-flight
    // pair and the retry; reaching its full handler count proves the ordering held.
    assert_eq!(leader_7.join().await, 6);
}

#[tokio::test]
async fn idempotent_kafka_producer_retries_leadership_error_with_current_leader_same_sequence() {
    let leader_8 = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::Produce as i16);
            let produce = ProduceRequestData::read(&mut request, header.request_api_version)
                .expect("produce request");
            assert_single_idempotent_produce(&produce, 0, 0);
            produce_response_frame_for_request(&header, 0, 88)
        }),
    ])
    .await;
    let leader_7 = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::InitProducerId as i16);
            init_producer_id_response_frame(header.correlation_id, 42, 3)
        }),
        Box::new({
            let leader_8 = leader_8.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::Produce as i16);
                let produce = ProduceRequestData::read(&mut request, header.request_api_version)
                    .expect("produce request");
                assert_single_idempotent_produce(&produce, 0, 0);
                produce_leader_change_error_response_frame_for_request(&header, 0, 8, 4, leader_8)
            }
        }),
    ])
    .await;
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new({
            let leader_7 = leader_7.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::Metadata as i16);
                let response = metadata_response([(7, leader_7)]);
                response_frame(ApiKey::Metadata, 13, header.correlation_id, &response)
            }
        }),
    ])
    .await;
    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default()
            .max_in_flight_requests_per_connection(1)
            .broker_queue_capacity(2)
            .request_timeout(Duration::from_millis(100)),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let mut producer = Producer::from_parts(
        wire,
        ProducerRuntimeConfig {
            accumulator: AccumulatorConfig::default()
                .batch_size(1)
                .buffer_memory(16 * 1024),
            acks: -1,
            timeout_ms: 30_000,
            retry_attempts: 1,
            retry_backoff: Duration::from_millis(1),
            retry_backoff_max: Duration::from_millis(1),
            delivery_timeout: Duration::from_secs(2),
            max_block: Duration::from_mins(1),
            partitioner_ignore_keys: false,
            partitioner_adaptive_partitioning_enable: true,
            partitioner_availability_timeout: Duration::ZERO,
            max_in_flight_requests_per_connection: 1,
            max_request_size: 1_048_576,
            enable_metrics_push: true,
            compression: ProducerCompression::default(),
            idempotence: ProducerIdempotenceConfig {
                enabled: true,
                transactional_id: None,
                transaction_timeout_ms: 60_000,
                transaction_two_phase_commit: false,
            },
        },
    );

    let delivery = producer
        .send(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")))
        .unwrap();

    producer.flush().await.unwrap();
    let receipt = delivery.await.unwrap();

    assert_eq!(receipt.partition, 0);
    assert_eq!(receipt.leader_id, 8);
    assert_eq!(receipt.offset, 88);
    assert_eq!(producer.buffered_bytes(), 0);
    assert_eq!(bootstrap.join().await, 2);
    assert_eq!(leader_7.join().await, 3);
    assert_eq!(leader_8.join().await, 2);
}

#[tokio::test]
async fn kafka_producer_requeues_in_flight_batch_when_metadata_is_missing() {
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::Metadata as i16);
            response_frame(
                ApiKey::Metadata,
                13,
                header.correlation_id,
                &empty_metadata_response(),
            )
        }),
    ])
    .await;
    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default(),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let mut producer = Producer::from_parts(
        wire,
        ProducerRuntimeConfig {
            accumulator: AccumulatorConfig::default()
                .batch_size(1)
                .buffer_memory(16 * 1024),
            acks: 1,
            timeout_ms: 30_000,
            retry_attempts: 0,
            retry_backoff: Duration::from_millis(100),
            retry_backoff_max: Duration::from_secs(1),
            delivery_timeout: Duration::from_mins(2),
            max_block: Duration::from_mins(1),
            partitioner_ignore_keys: false,
            partitioner_adaptive_partitioning_enable: true,
            partitioner_availability_timeout: Duration::ZERO,
            max_in_flight_requests_per_connection: 5,
            max_request_size: 1_048_576,
            enable_metrics_push: true,
            compression: ProducerCompression::default(),
            idempotence: idempotence_disabled(),
        },
    );
    producer.enable_metrics();

    let _delivery = producer
        .send(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")))
        .unwrap();

    let error = producer.flush().await.unwrap_err();

    assert!(matches!(
        error,
        kacrab::producer::ProducerError::FlushIncomplete
    ));
    assert!(wait_for_buffered_bytes(&producer).await > 0);
    assert_eq!(bootstrap.join().await, 2);
}

#[tokio::test]
async fn kafka_producer_metrics_snapshot_reports_queue_and_dispatch_counters() {
    let leader_7 = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::Produce as i16);
            produce_response_frame_for_request(&header, 0, 40)
        }),
    ])
    .await;
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new({
            let leader_7 = leader_7.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::Metadata as i16);
                let response = metadata_response([(7, leader_7)]);
                response_frame(ApiKey::Metadata, 13, header.correlation_id, &response)
            }
        }),
    ])
    .await;

    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default(),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let mut producer = Producer::from_parts(
        wire,
        ProducerRuntimeConfig {
            accumulator: AccumulatorConfig::default()
                .batch_size(16 * 1024)
                .linger(Duration::from_mins(1))
                .buffer_memory(16 * 1024),
            acks: 1,
            timeout_ms: 30_000,
            retry_attempts: 0,
            retry_backoff: Duration::from_millis(100),
            retry_backoff_max: Duration::from_secs(1),
            delivery_timeout: Duration::from_mins(2),
            max_block: Duration::from_mins(1),
            partitioner_ignore_keys: false,
            partitioner_adaptive_partitioning_enable: true,
            partitioner_availability_timeout: Duration::ZERO,
            max_in_flight_requests_per_connection: 5,
            max_request_size: 1_048_576,
            enable_metrics_push: true,
            compression: ProducerCompression::default(),
            idempotence: idempotence_disabled(),
        },
    );
    producer.enable_metrics();

    let _delivery = producer
        .send(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")))
        .unwrap();
    let queued = producer.metrics();
    assert_eq!(queued.records_appended, 1);
    assert_eq!(queued.queue_depth_records, 1);
    assert_eq!(queued.incomplete_batches, 1);
    assert!(queued.queue_depth_bytes > 0);
    assert_eq!(queued.produce_request_count, 0);

    producer.flush().await.unwrap();
    let flushed = producer.metrics();
    assert_eq!(flushed.queue_depth_records, 0);
    assert_eq!(flushed.queue_depth_bytes, 0);
    assert_eq!(flushed.incomplete_batches, 0);
    assert_eq!(flushed.produce_request_count, 1);
    assert_eq!(flushed.produce_record_count, 1);
    assert!(flushed.average_batch_fill_ratio > 0.0);
    assert_eq!(flushed.in_flight_dispatches, 0);
    assert_eq!(bootstrap.join().await, 2);
    assert_eq!(leader_7.join().await, 2);
}

#[tokio::test]
async fn dispatcher_records_batch_metrics_after_request_build_with_actual_encoded_bytes() {
    let encoded_records_len = Arc::new(AtomicUsize::new(0));
    let leader_7 = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new({
            let encoded_records_len = Arc::clone(&encoded_records_len);
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::Produce as i16);
                let produce = ProduceRequestData::read(&mut request, header.request_api_version)
                    .expect("produce request");
                let records = produce.topic_data[0].partition_data[0]
                    .records
                    .clone()
                    .expect("records");
                encoded_records_len.store(records.len(), Ordering::SeqCst);
                produce_response_frame_for_request(&header, 0, 40)
            }
        }),
    ])
    .await;
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new({
            let leader_7 = leader_7.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::Metadata as i16);
                let response = metadata_response([(7, leader_7)]);
                response_frame(ApiKey::Metadata, 13, header.correlation_id, &response)
            }
        }),
    ])
    .await;
    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default(),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let dispatcher = ProducerDispatcher::with_config(
        wire,
        ProducerRuntimeConfig {
            idempotence: idempotence_disabled(),
            ..ProducerRuntimeConfig::default()
        },
    );
    dispatcher.enable_metrics();

    let receipts = dispatcher
        .dispatch_ready_batches(
            ready_batches_for_value(b"value", Instant::now()),
            Instant::now(),
        )
        .await
        .unwrap();

    assert_eq!(receipts[0].offset, 40);
    let metrics = dispatcher.metrics();
    assert_eq!(metrics.produce_request_count, 1);
    assert_eq!(metrics.produce_batch_count, 1);
    assert_eq!(metrics.produce_record_count, 1);
    assert_eq!(
        metrics.produce_batch_bytes,
        u64::try_from(encoded_records_len.load(Ordering::SeqCst)).unwrap()
    );
    assert!((metrics.average_compression_ratio - 1.0).abs() < f64::EPSILON);
    assert_eq!(bootstrap.join().await, 2);
    assert_eq!(leader_7.join().await, 2);
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "Transaction wire-flow fixture keeps ordered broker handlers inline for readability."
)]
async fn kafka_producer_commits_transactional_send() {
    let coordinator = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::InitProducerId as i16);
            let init = InitProducerIdRequestData::read(&mut request, 5).expect("init producer id");
            assert_eq!(
                init.transactional_id,
                Some(KafkaString::from("txn-orders".to_owned()))
            );
            init_producer_id_response_frame(header.correlation_id, 77, 4)
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::AddPartitionsToTxn as i16);
            let add = AddPartitionsToTxnRequestData::read(&mut request, 5)
                .expect("add partitions to txn");
            let transaction = add.transactions.first().expect("transaction");
            assert_eq!(
                transaction.transactional_id,
                KafkaString::from("txn-orders".to_owned())
            );
            assert_eq!(transaction.producer_id, 77);
            assert_eq!(transaction.producer_epoch, 4);
            assert_eq!(
                transaction.topics[0].name,
                KafkaString::from("orders".to_owned())
            );
            assert_eq!(transaction.topics[0].partitions, vec![0]);
            add_partitions_to_txn_response_frame(header.correlation_id)
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::EndTxn as i16);
            let end =
                EndTxnRequestData::read(&mut request, header.request_api_version).expect("end txn");
            assert_eq!(
                end.transactional_id,
                KafkaString::from("txn-orders".to_owned())
            );
            assert_eq!(end.producer_id, 77);
            assert_eq!(end.producer_epoch, 4);
            assert!(end.committed);
            end_txn_response_frame_for_request(&header)
        }),
    ])
    .await;
    let leader_7 = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::Produce as i16);
            let produce = ProduceRequestData::read(&mut request, header.request_api_version)
                .expect("produce request");
            assert_eq!(
                produce.transactional_id,
                Some(KafkaString::from("txn-orders".to_owned()))
            );
            let mut records = produce.topic_data[0].partition_data[0]
                .records
                .clone()
                .expect("records");
            let batch = RecordBatch::decode(&mut records).expect("record batch");
            assert_eq!(batch.producer_id, 77);
            assert_eq!(batch.producer_epoch, 4);
            assert_eq!(batch.base_sequence, 0);
            produce_response_frame_for_request(&header, 0, 90)
        }),
    ])
    .await;
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new({
            let coordinator = coordinator.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::FindCoordinator as i16);
                let find =
                    FindCoordinatorRequestData::read(&mut request, 6).expect("find coordinator");
                assert_eq!(
                    find.coordinator_keys,
                    vec![KafkaString::from("txn-orders".to_owned())]
                );
                find_coordinator_response_frame(header.correlation_id, 9, coordinator)
            }
        }),
        Box::new({
            let leader_7 = leader_7.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::Metadata as i16);
                let response = metadata_response([(7, leader_7)]);
                response_frame(ApiKey::Metadata, 13, header.correlation_id, &response)
            }
        }),
    ])
    .await;

    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default(),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let mut producer = Producer::from_parts(
        wire,
        ProducerRuntimeConfig {
            accumulator: AccumulatorConfig::default()
                .batch_size(1)
                .buffer_memory(16 * 1024),
            acks: -1,
            timeout_ms: 30_000,
            retry_attempts: 0,
            retry_backoff: Duration::from_millis(100),
            retry_backoff_max: Duration::from_secs(1),
            delivery_timeout: Duration::from_mins(2),
            max_block: Duration::from_mins(1),
            partitioner_ignore_keys: false,
            partitioner_adaptive_partitioning_enable: true,
            partitioner_availability_timeout: Duration::ZERO,
            max_in_flight_requests_per_connection: 5,
            max_request_size: 1_048_576,
            enable_metrics_push: true,
            compression: ProducerCompression::default(),
            idempotence: ProducerIdempotenceConfig {
                enabled: true,
                transactional_id: Some("txn-orders".to_owned()),
                transaction_timeout_ms: 60_000,
                transaction_two_phase_commit: false,
            },
        },
    );

    assert_eq!(producer.metrics().transaction_init_count, 0);
    producer.init_transactions().await.unwrap();
    let metrics = producer.metrics();
    assert_eq!(metrics.transaction_init_count, 1);
    assert!(metrics.transaction_init_total_latency >= Duration::ZERO);
    assert_eq!(metrics.transaction_begin_count, 0);
    producer.begin_transaction().unwrap();
    let metrics = producer.metrics();
    assert_eq!(metrics.transaction_begin_count, 1);
    assert!(metrics.transaction_begin_total_latency >= Duration::ZERO);
    let delivery = producer
        .send(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")))
        .unwrap();
    producer.flush().await.unwrap();
    assert_eq!(producer.metrics().transaction_commit_count, 0);
    producer.commit_transaction().await.unwrap();

    assert_eq!(delivery.await.unwrap().offset, 90);
    let metrics = producer.metrics();
    assert_eq!(metrics.transaction_commit_count, 1);
    assert!(metrics.transaction_commit_total_latency >= Duration::ZERO);
    assert_eq!(bootstrap.join().await, 3);
    assert_eq!(coordinator.join().await, 4);
    assert_eq!(leader_7.join().await, 2);
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "Transaction timeout retry fixture keeps ordered broker handlers inline."
)]
async fn kafka_producer_commit_timeout_can_retry_same_operation_like_java() {
    let coordinator = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::InitProducerId as i16);
            init_producer_id_response_frame(header.correlation_id, 77, 4)
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::AddPartitionsToTxn as i16);
            add_partitions_to_txn_response_frame(header.correlation_id)
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::EndTxn as i16);
            let end =
                EndTxnRequestData::read(&mut request, header.request_api_version).expect("end txn");
            assert!(end.committed);
            std::thread::sleep(Duration::from_millis(50));
            end_txn_response_frame_for_request(&header)
        }),
    ])
    .await;
    let leader_7 = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::Produce as i16);
            produce_response_frame_for_request(&header, 0, 90)
        }),
    ])
    .await;
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new({
            let coordinator = coordinator.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::FindCoordinator as i16);
                find_coordinator_response_frame(header.correlation_id, 9, coordinator)
            }
        }),
        Box::new({
            let leader_7 = leader_7.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::Metadata as i16);
                response_frame(
                    ApiKey::Metadata,
                    13,
                    header.correlation_id,
                    &metadata_response([(7, leader_7)]),
                )
            }
        }),
    ])
    .await;

    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default(),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let mut producer = Producer::from_parts(
        wire,
        ProducerRuntimeConfig {
            accumulator: AccumulatorConfig::default()
                .batch_size(1)
                .buffer_memory(16 * 1024),
            acks: -1,
            timeout_ms: 30_000,
            retry_attempts: 0,
            retry_backoff: Duration::from_millis(100),
            retry_backoff_max: Duration::from_secs(1),
            delivery_timeout: Duration::from_mins(2),
            max_block: Duration::from_millis(30),
            partitioner_ignore_keys: false,
            partitioner_adaptive_partitioning_enable: true,
            partitioner_availability_timeout: Duration::ZERO,
            max_in_flight_requests_per_connection: 5,
            max_request_size: 1_048_576,
            enable_metrics_push: true,
            compression: ProducerCompression::default(),
            idempotence: ProducerIdempotenceConfig {
                enabled: true,
                transactional_id: Some("txn-orders".to_owned()),
                transaction_timeout_ms: 60_000,
                transaction_two_phase_commit: false,
            },
        },
    );

    producer.init_transactions().await.unwrap();
    producer.begin_transaction().unwrap();
    let delivery = producer
        .send(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")))
        .unwrap();
    producer.flush().await.unwrap();

    assert!(matches!(
        producer
            .commit_transaction()
            .await
            .expect_err("first commit should time out while EndTxn is still in flight"),
        kacrab::producer::ProducerError::DispatchTask(message)
            if message.contains("CommitTransaction timed out")
    ));
    assert!(matches!(
        producer
            .abort_transaction()
            .await
            .expect_err("abort must not replace the pending commit result"),
        kacrab::producer::ProducerError::InvalidTransactionState(message)
            if message == "previous transaction operation is pending and must be retried"
    ));
    producer
        .commit_transaction()
        .await
        .expect("retrying the same commit should await cached EndTxn result");

    assert_eq!(delivery.await.unwrap().offset, 90);
    assert_eq!(producer.metrics().transaction_commit_count, 1);
    assert_eq!(bootstrap.join().await, 3);
    assert_eq!(coordinator.join().await, 4);
    assert_eq!(leader_7.join().await, 2);
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "Abort timeout retry fixture keeps ordered broker handlers inline."
)]
async fn kafka_producer_abort_timeout_can_retry_same_operation_like_java() {
    let coordinator = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::InitProducerId as i16);
            init_producer_id_response_frame(header.correlation_id, 77, 4)
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::AddPartitionsToTxn as i16);
            add_partitions_to_txn_response_frame(header.correlation_id)
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::EndTxn as i16);
            let end =
                EndTxnRequestData::read(&mut request, header.request_api_version).expect("end txn");
            assert!(!end.committed);
            std::thread::sleep(Duration::from_millis(50));
            end_txn_response_frame_for_request(&header)
        }),
    ])
    .await;
    let leader_7 = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::Produce as i16);
            produce_response_frame_for_request(&header, 0, 90)
        }),
    ])
    .await;
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new({
            let coordinator = coordinator.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::FindCoordinator as i16);
                find_coordinator_response_frame(header.correlation_id, 9, coordinator)
            }
        }),
        Box::new({
            let leader_7 = leader_7.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::Metadata as i16);
                response_frame(
                    ApiKey::Metadata,
                    13,
                    header.correlation_id,
                    &metadata_response([(7, leader_7)]),
                )
            }
        }),
    ])
    .await;

    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default(),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let mut producer = Producer::from_parts(
        wire,
        ProducerRuntimeConfig {
            accumulator: AccumulatorConfig::default()
                .batch_size(1)
                .buffer_memory(16 * 1024),
            acks: -1,
            timeout_ms: 30_000,
            retry_attempts: 0,
            retry_backoff: Duration::from_millis(100),
            retry_backoff_max: Duration::from_secs(1),
            delivery_timeout: Duration::from_mins(2),
            max_block: Duration::from_millis(30),
            partitioner_ignore_keys: false,
            partitioner_adaptive_partitioning_enable: true,
            partitioner_availability_timeout: Duration::ZERO,
            max_in_flight_requests_per_connection: 5,
            max_request_size: 1_048_576,
            enable_metrics_push: true,
            compression: ProducerCompression::default(),
            idempotence: ProducerIdempotenceConfig {
                enabled: true,
                transactional_id: Some("txn-orders".to_owned()),
                transaction_timeout_ms: 60_000,
                transaction_two_phase_commit: false,
            },
        },
    );

    producer.init_transactions().await.unwrap();
    producer.begin_transaction().unwrap();
    let delivery = producer
        .send(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")))
        .unwrap();
    producer.flush().await.unwrap();

    assert!(matches!(
        producer
            .abort_transaction()
            .await
            .expect_err("first abort should time out while EndTxn is still in flight"),
        kacrab::producer::ProducerError::DispatchTask(message)
            if message.contains("AbortTransaction timed out")
    ));
    assert!(matches!(
        producer
            .commit_transaction()
            .await
            .expect_err("commit must not replace the pending abort result"),
        kacrab::producer::ProducerError::InvalidTransactionState(message)
            if message == "previous transaction operation is pending and must be retried"
    ));
    producer
        .abort_transaction()
        .await
        .expect("retrying the same abort should await cached EndTxn result");

    assert_eq!(delivery.await.unwrap().offset, 90);
    assert_eq!(producer.metrics().transaction_abort_count, 1);
    assert_eq!(bootstrap.join().await, 3);
    assert_eq!(coordinator.join().await, 4);
    assert_eq!(leader_7.join().await, 2);
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "Abort drain-order fixture needs separate coordinator, leader, and bootstrap \
              handlers."
)]
async fn kafka_producer_abort_holds_end_txn_until_in_flight_batches_drain_like_java() {
    let produce_done = Arc::new(AtomicBool::new(false));
    let coordinator = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::InitProducerId as i16);
            init_producer_id_response_frame(header.correlation_id, 77, 4)
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::AddPartitionsToTxn as i16);
            add_partitions_to_txn_response_frame(header.correlation_id)
        }),
        Box::new({
            let produce_done = Arc::clone(&produce_done);
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::EndTxn as i16);
                assert!(
                    produce_done.load(Ordering::SeqCst),
                    "EndTxn must not be sent before in-flight Produce response is drained"
                );
                let end = EndTxnRequestData::read(&mut request, header.request_api_version)
                    .expect("end txn");
                assert!(!end.committed);
                end_txn_response_frame_for_request(&header)
            }
        }),
    ])
    .await;
    let leader_7 = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new({
            let produce_done = Arc::clone(&produce_done);
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::Produce as i16);
                std::thread::sleep(Duration::from_millis(80));
                produce_done.store(true, Ordering::SeqCst);
                produce_response_frame_for_request(&header, 0, 90)
            }
        }),
    ])
    .await;
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new({
            let coordinator = coordinator.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::FindCoordinator as i16);
                find_coordinator_response_frame(header.correlation_id, 9, coordinator)
            }
        }),
        Box::new({
            let leader_7 = leader_7.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::Metadata as i16);
                response_frame(
                    ApiKey::Metadata,
                    13,
                    header.correlation_id,
                    &metadata_response([(7, leader_7)]),
                )
            }
        }),
    ])
    .await;

    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default(),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let mut producer = Producer::from_parts(
        wire,
        ProducerRuntimeConfig {
            accumulator: AccumulatorConfig::default()
                .batch_size(1)
                .buffer_memory(16 * 1024),
            acks: -1,
            timeout_ms: 30_000,
            retry_attempts: 0,
            retry_backoff: Duration::from_millis(100),
            retry_backoff_max: Duration::from_secs(1),
            delivery_timeout: Duration::from_mins(2),
            max_block: Duration::from_secs(1),
            partitioner_ignore_keys: false,
            partitioner_adaptive_partitioning_enable: true,
            partitioner_availability_timeout: Duration::ZERO,
            max_in_flight_requests_per_connection: 5,
            max_request_size: 1_048_576,
            enable_metrics_push: true,
            compression: ProducerCompression::default(),
            idempotence: ProducerIdempotenceConfig {
                enabled: true,
                transactional_id: Some("txn-orders".to_owned()),
                transaction_timeout_ms: 60_000,
                transaction_two_phase_commit: false,
            },
        },
    );

    producer.init_transactions().await.unwrap();
    producer.begin_transaction().unwrap();
    let delivery = producer
        .send(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")))
        .unwrap();

    // The synchronous send appends the (batch.size=1, immediately full) batch but
    // dispatch happens on the background loop, so drive it until the batch is in
    // flight — its Produce request is sent and the broker holds the response for
    // 80ms. This is the precondition the test exercises: abort must hold EndTxn
    // until the already-in-flight batch drains (like Java's Sender thread sending
    // the batch before the abort completes), rather than discarding a buffered
    // batch that was never sent.
    for _ in 0..1_000 {
        if producer.buffered_bytes() == 0 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(
        producer.buffered_bytes(),
        0,
        "record batch should be in flight before abort"
    );

    producer.abort_transaction().await.unwrap();

    assert_eq!(delivery.await.unwrap().offset, 90);
    assert_eq!(bootstrap.join().await, 3);
    assert_eq!(coordinator.join().await, 4);
    assert_eq!(leader_7.join().await, 2);
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "Transaction V2 produce fixture keeps ordered broker handlers inline."
)]
async fn kafka_producer_transaction_v2_skips_add_partitions_to_txn_like_java() {
    let coordinator = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::InitProducerId as i16);
            let init = InitProducerIdRequestData::read(&mut request, header.request_api_version)
                .expect("init producer id");
            assert_eq!(
                init.transactional_id,
                Some(KafkaString::from("txn-orders".to_owned()))
            );
            assert!(init.enable2_pc);
            init_producer_id_response_frame_for_request(&header, 77, 4)
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::EndTxn as i16);
            let end =
                EndTxnRequestData::read(&mut request, header.request_api_version).expect("end txn");
            assert_eq!(
                end.transactional_id,
                KafkaString::from("txn-orders".to_owned())
            );
            assert_eq!(end.producer_id, 77);
            assert_eq!(end.producer_epoch, 4);
            assert!(end.committed);
            end_txn_response_frame_for_request(&header)
        }),
    ])
    .await;
    let leader_7 = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::Produce as i16);
            assert!(
                header.request_api_version > 11,
                "transaction V2 produce must not use transaction V1 request cap"
            );
            let produce = ProduceRequestData::read(&mut request, header.request_api_version)
                .expect("produce request");
            assert_eq!(
                produce.transactional_id,
                Some(KafkaString::from("txn-orders".to_owned()))
            );
            assert_eq!(produce.topic_data[0].topic_id, TOPIC_ID);
            let mut records = produce.topic_data[0].partition_data[0]
                .records
                .clone()
                .expect("records");
            let batch = RecordBatch::decode(&mut records).expect("record batch");
            assert_eq!(batch.producer_id, 77);
            assert_eq!(batch.producer_epoch, 4);
            assert_eq!(batch.base_sequence, 0);
            produce_response_frame_for_request(&header, 0, 90)
        }),
    ])
    .await;
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new({
            let coordinator = coordinator.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::FindCoordinator as i16);
                let find =
                    FindCoordinatorRequestData::read(&mut request, 6).expect("find coordinator");
                assert_eq!(
                    find.coordinator_keys,
                    vec![KafkaString::from("txn-orders".to_owned())]
                );
                find_coordinator_response_frame(header.correlation_id, 9, coordinator)
            }
        }),
        Box::new({
            let leader_7 = leader_7.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::Metadata as i16);
                let response = metadata_response([(7, leader_7)]);
                response_frame(ApiKey::Metadata, 13, header.correlation_id, &response)
            }
        }),
    ])
    .await;

    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default(),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let mut producer = transaction_v2_test_producer(wire);

    producer.init_transactions().await.unwrap();
    producer.begin_transaction().unwrap();
    let delivery = producer
        .send(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")))
        .unwrap();
    producer.flush().await.unwrap();
    producer.commit_transaction().await.unwrap();

    assert_eq!(delivery.await.unwrap().offset, 90);
    assert_eq!(bootstrap.join().await, 3);
    assert_eq!(coordinator.join().await, 3);
    assert_eq!(leader_7.join().await, 2);
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "Transaction V2 epoch update fixture keeps ordered broker handlers inline."
)]
async fn kafka_producer_transaction_v2_installs_end_txn_epoch_like_java() {
    let coordinator = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::InitProducerId as i16);
            let init = InitProducerIdRequestData::read(&mut request, header.request_api_version)
                .expect("init producer id");
            assert!(init.enable2_pc);
            init_producer_id_response_frame_for_request(&header, 77, 4)
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::EndTxn as i16);
            let end =
                EndTxnRequestData::read(&mut request, header.request_api_version).expect("end txn");
            assert_eq!(end.producer_id, 77);
            assert_eq!(end.producer_epoch, 4);
            assert!(end.committed);
            end_txn_response_frame_for_request_with_identity(&header, 77, 5)
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::EndTxn as i16);
            let end =
                EndTxnRequestData::read(&mut request, header.request_api_version).expect("end txn");
            assert_eq!(end.producer_id, 77);
            assert_eq!(end.producer_epoch, 5);
            assert!(end.committed);
            end_txn_response_frame_for_request_with_identity(&header, 77, 6)
        }),
    ])
    .await;
    let leader_7 = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::Produce as i16);
            let produce = ProduceRequestData::read(&mut request, header.request_api_version)
                .expect("produce request");
            let mut records = produce.topic_data[0].partition_data[0]
                .records
                .clone()
                .expect("records");
            let batch = RecordBatch::decode(&mut records).expect("record batch");
            assert_eq!(batch.producer_id, 77);
            assert_eq!(batch.producer_epoch, 4);
            assert_eq!(batch.base_sequence, 0);
            produce_response_frame_for_request(&header, 0, 90)
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::Produce as i16);
            let produce = ProduceRequestData::read(&mut request, header.request_api_version)
                .expect("produce request");
            let mut records = produce.topic_data[0].partition_data[0]
                .records
                .clone()
                .expect("records");
            let batch = RecordBatch::decode(&mut records).expect("record batch");
            assert_eq!(batch.producer_id, 77);
            assert_eq!(batch.producer_epoch, 5);
            assert_eq!(batch.base_sequence, 0);
            produce_response_frame_for_request(&header, 0, 91)
        }),
    ])
    .await;
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new({
            let coordinator = coordinator.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::FindCoordinator as i16);
                find_coordinator_response_frame(header.correlation_id, 9, coordinator)
            }
        }),
        Box::new({
            let leader_7 = leader_7.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::Metadata as i16);
                let response = metadata_response([(7, leader_7)]);
                response_frame(ApiKey::Metadata, 13, header.correlation_id, &response)
            }
        }),
    ])
    .await;

    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default(),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let mut producer = transaction_v2_test_producer(wire);

    producer.init_transactions().await.unwrap();
    producer.begin_transaction().unwrap();
    let first_delivery = producer
        .send(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")))
        .unwrap();
    producer.flush().await.unwrap();
    producer.commit_transaction().await.unwrap();

    producer.begin_transaction().unwrap();
    let second_delivery = producer
        .send(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"b")))
        .unwrap();
    producer.flush().await.unwrap();
    producer.commit_transaction().await.unwrap();

    assert_eq!(first_delivery.await.unwrap().offset, 90);
    assert_eq!(second_delivery.await.unwrap().offset, 91);
    assert_eq!(bootstrap.join().await, 3);
    assert_eq!(coordinator.join().await, 4);
    assert_eq!(leader_7.join().await, 3);
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "Abortable transaction recovery fixture verifies ordered epoch bump and retry flow."
)]
async fn kafka_producer_transactional_unknown_producer_id_is_abortable_like_java() {
    let coordinator = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::InitProducerId as i16);
            init_producer_id_response_frame(header.correlation_id, 77, 4)
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::AddPartitionsToTxn as i16);
            add_partitions_to_txn_response_frame(header.correlation_id)
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::EndTxn as i16);
            let _end =
                EndTxnRequestData::read(&mut request, header.request_api_version).expect("end txn");
            end_txn_response_frame_for_request(&header)
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::InitProducerId as i16);
            let init = InitProducerIdRequestData::read(&mut request, 5).expect("init producer id");
            assert_eq!(init.producer_id, 77);
            assert_eq!(init.producer_epoch, 4);
            init_producer_id_response_frame(header.correlation_id, 77, 5)
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::AddPartitionsToTxn as i16);
            let add = AddPartitionsToTxnRequestData::read(&mut request, 5)
                .expect("add partitions to txn");
            let transaction = add.transactions.first().expect("transaction");
            assert_eq!(transaction.producer_id, 77);
            assert_eq!(transaction.producer_epoch, 5);
            add_partitions_to_txn_response_frame(header.correlation_id)
        }),
    ])
    .await;
    let leader_7 = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::Produce as i16);
            produce_error_response_frame_for_request(&header, 0, ErrorCode::UnknownProducerId)
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::Produce as i16);
            let produce = ProduceRequestData::read(&mut request, header.request_api_version)
                .expect("produce request");
            let mut records = produce.topic_data[0].partition_data[0]
                .records
                .clone()
                .expect("records");
            let batch = RecordBatch::decode(&mut records).expect("record batch");
            assert_eq!(batch.producer_id, 77);
            assert_eq!(batch.producer_epoch, 5);
            assert_eq!(batch.base_sequence, 0);
            produce_response_frame_for_request(&header, 0, 91)
        }),
    ])
    .await;
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new({
            let coordinator = coordinator.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::FindCoordinator as i16);
                find_coordinator_response_frame(header.correlation_id, 9, coordinator)
            }
        }),
        Box::new({
            let leader_7 = leader_7.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::Metadata as i16);
                let response = metadata_response([(7, leader_7)]);
                response_frame(ApiKey::Metadata, 13, header.correlation_id, &response)
            }
        }),
    ])
    .await;

    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default(),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let mut producer = transactional_test_producer(wire);

    producer.init_transactions().await.unwrap();
    producer.begin_transaction().unwrap();
    let delivery = producer
        .send(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")))
        .expect("send returns a delivery future before the async produce response");

    let send_error = producer
        .commit_transaction()
        .await
        .expect_err("commit flush should surface the produce error");
    assert!(matches!(
        send_error,
        kacrab::producer::ProducerError::Broker {
            error: ErrorCode::UnknownProducerId,
            ..
        }
    ));
    let _delivery_result = delivery.await;

    let commit_error = producer
        .commit_transaction()
        .await
        .expect_err("abortable produce error must block commit");
    assert!(matches!(
        commit_error,
        kacrab::producer::ProducerError::Transaction {
            operation: "transaction_state",
            error: ErrorCode::UnknownProducerId,
        }
    ));
    producer.abort_transaction().await.unwrap();
    producer.begin_transaction().unwrap();
    let recovered_delivery = producer
        .send(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"b")))
        .expect("send after abort should use bumped epoch");
    producer.flush().await.unwrap();
    assert_eq!(recovered_delivery.await.unwrap().offset, 91);

    assert_eq!(bootstrap.join().await, 3);
    assert_eq!(coordinator.join().await, 6);
    assert_eq!(leader_7.join().await, 3);
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "Abortable unsupported-format fixture verifies EndTxn epoch-bump request."
)]
async fn kafka_producer_transactional_unsupported_message_format_is_abortable_like_java() {
    let coordinator = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::InitProducerId as i16);
            init_producer_id_response_frame(header.correlation_id, 77, 4)
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::AddPartitionsToTxn as i16);
            add_partitions_to_txn_response_frame(header.correlation_id)
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::EndTxn as i16);
            let _end =
                EndTxnRequestData::read(&mut request, header.request_api_version).expect("end txn");
            end_txn_response_frame_for_request(&header)
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::InitProducerId as i16);
            let init = InitProducerIdRequestData::read(&mut request, 5).expect("init producer id");
            assert_eq!(init.producer_id, 77);
            assert_eq!(init.producer_epoch, 4);
            init_producer_id_response_frame(header.correlation_id, 77, 5)
        }),
    ])
    .await;
    let leader_7 = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::Produce as i16);
            produce_error_response_frame_for_request(
                &header,
                0,
                ErrorCode::UnsupportedForMessageFormat,
            )
        }),
    ])
    .await;
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new({
            let coordinator = coordinator.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::FindCoordinator as i16);
                find_coordinator_response_frame(header.correlation_id, 9, coordinator)
            }
        }),
        Box::new({
            let leader_7 = leader_7.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::Metadata as i16);
                let response = metadata_response([(7, leader_7)]);
                response_frame(ApiKey::Metadata, 13, header.correlation_id, &response)
            }
        }),
    ])
    .await;

    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default(),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let mut producer = transactional_test_producer(wire);

    producer.init_transactions().await.unwrap();
    producer.begin_transaction().unwrap();
    let delivery = producer
        .send(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")))
        .expect("send returns a delivery future before the async produce response");

    let send_error = producer
        .commit_transaction()
        .await
        .expect_err("commit flush should surface the produce error");
    assert!(matches!(
        send_error,
        kacrab::producer::ProducerError::Broker {
            error: ErrorCode::UnsupportedForMessageFormat,
            ..
        }
    ));
    let _delivery_result = delivery.await;

    let commit_error = producer
        .commit_transaction()
        .await
        .expect_err("abortable produce error must block commit");
    assert!(matches!(
        commit_error,
        kacrab::producer::ProducerError::Transaction {
            operation: "transaction_state",
            error: ErrorCode::UnsupportedForMessageFormat,
        }
    ));
    producer.abort_transaction().await.unwrap();

    assert_eq!(bootstrap.join().await, 3);
    assert_eq!(coordinator.join().await, 5);
    assert_eq!(leader_7.join().await, 2);
}

#[tokio::test]
async fn kafka_producer_add_partitions_fatal_error_blocks_abort() {
    let coordinator = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::InitProducerId as i16);
            init_producer_id_response_frame(header.correlation_id, 77, 4)
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::AddPartitionsToTxn as i16);
            add_partitions_to_txn_error_response_frame(
                header.correlation_id,
                ErrorCode::InvalidTxnState,
            )
        }),
    ])
    .await;
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new({
            let coordinator = coordinator.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::FindCoordinator as i16);
                find_coordinator_response_frame(header.correlation_id, 9, coordinator)
            }
        }),
        Box::new({
            let leader = "127.0.0.1:9092"
                .parse::<std::net::SocketAddr>()
                .expect("leader addr");
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::Metadata as i16);
                let response = metadata_response([(7, leader)]);
                response_frame(ApiKey::Metadata, 13, header.correlation_id, &response)
            }
        }),
    ])
    .await;
    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default(),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let mut producer = transactional_test_producer(wire);

    producer.init_transactions().await.unwrap();
    producer.begin_transaction().unwrap();
    // The synchronous send appends and returns a future; AddPartitionsToTxn runs
    // during background dispatch (like Java's Sender thread), so its fatal
    // InvalidTxnState surfaces on the delivery future rather than the send call.
    let send_error = producer
        .send(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")))
        .expect("send appends before the background dispatch observes the fatal error")
        .await
        .unwrap_err();
    assert!(matches!(
        send_error,
        kacrab::producer::ProducerError::Transaction {
            operation: "add_partitions_to_txn",
            error: ErrorCode::InvalidTxnState,
        }
    ));

    let abort_after_fatal_add_partitions =
        tokio::time::timeout(Duration::from_millis(200), producer.abort_transaction())
            .await
            .expect("abort after fatal add partitions error should fail locally");
    assert!(matches!(
        abort_after_fatal_add_partitions,
        Err(kacrab::producer::ProducerError::Transaction {
            operation: "transaction_state",
            error: ErrorCode::InvalidTxnState,
        })
    ));
    assert_eq!(bootstrap.join().await, 3);
    assert_eq!(coordinator.join().await, 3);
}

#[tokio::test]
async fn kafka_producer_fatal_transaction_error_blocks_later_send_like_java() {
    let coordinator = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::InitProducerId as i16);
            init_producer_id_response_frame(header.correlation_id, 77, 4)
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::AddPartitionsToTxn as i16);
            add_partitions_to_txn_error_response_frame(
                header.correlation_id,
                ErrorCode::InvalidTxnState,
            )
        }),
    ])
    .await;
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new({
            let coordinator = coordinator.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::FindCoordinator as i16);
                find_coordinator_response_frame(header.correlation_id, 9, coordinator)
            }
        }),
        Box::new({
            let leader = "127.0.0.1:9092"
                .parse::<std::net::SocketAddr>()
                .expect("leader addr");
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::Metadata as i16);
                let response = metadata_response([(7, leader)]);
                response_frame(ApiKey::Metadata, 13, header.correlation_id, &response)
            }
        }),
    ])
    .await;
    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default(),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let mut producer = transactional_test_producer_with_batch_size_and_linger(
        wire,
        16 * 1024,
        Duration::from_mins(1),
    );

    producer.init_transactions().await.unwrap();
    producer.begin_transaction().unwrap();
    let _delivery = producer
        .send(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")))
        .expect("first send only buffers before flush");
    let flush_error = producer.flush().await.unwrap_err();
    assert!(matches!(
        flush_error,
        kacrab::producer::ProducerError::Transaction {
            operation: "add_partitions_to_txn",
            error: ErrorCode::InvalidTxnState,
        }
    ));

    let later_send =
        producer.send(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"b")));
    assert!(matches!(
        later_send,
        Err(kacrab::producer::ProducerError::Transaction {
            operation: "transaction_state",
            error: ErrorCode::InvalidTxnState,
        })
    ));
    assert_eq!(bootstrap.join().await, 3);
    assert_eq!(coordinator.join().await, 3);
}

#[tokio::test]
async fn kafka_producer_retries_retriable_add_partitions_error() {
    let leader_7 = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::Produce as i16);
            produce_response_frame_for_request(&header, 0, 90)
        }),
    ])
    .await;
    let coordinator = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::InitProducerId as i16);
            init_producer_id_response_frame(header.correlation_id, 77, 4)
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::AddPartitionsToTxn as i16);
            add_partitions_to_txn_error_response_frame(
                header.correlation_id,
                ErrorCode::CoordinatorLoadInProgress,
            )
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::AddPartitionsToTxn as i16);
            add_partitions_to_txn_response_frame(header.correlation_id)
        }),
    ])
    .await;
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new({
            let coordinator = coordinator.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::FindCoordinator as i16);
                find_coordinator_response_frame(header.correlation_id, 9, coordinator)
            }
        }),
        Box::new({
            let leader_7 = leader_7.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::Metadata as i16);
                let response = metadata_response([(7, leader_7)]);
                response_frame(ApiKey::Metadata, 13, header.correlation_id, &response)
            }
        }),
    ])
    .await;
    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default(),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let mut producer = transactional_test_producer_with_retries(wire, 1);

    producer.init_transactions().await.unwrap();
    producer.begin_transaction().unwrap();
    let delivery = producer
        .send(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")))
        .unwrap();
    producer.flush().await.unwrap();

    assert_eq!(delivery.await.unwrap().offset, 90);
    assert_eq!(bootstrap.join().await, 3);
    assert_eq!(coordinator.join().await, 4);
    assert_eq!(leader_7.join().await, 2);
}

#[tokio::test]
async fn kafka_producer_retries_concurrent_transactions_add_partitions_error_like_java() {
    let leader_7 = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::Produce as i16);
            produce_response_frame_for_request(&header, 0, 90)
        }),
    ])
    .await;
    let coordinator = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::InitProducerId as i16);
            init_producer_id_response_frame(header.correlation_id, 77, 4)
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::AddPartitionsToTxn as i16);
            add_partitions_to_txn_error_response_frame(
                header.correlation_id,
                ErrorCode::ConcurrentTransactions,
            )
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::AddPartitionsToTxn as i16);
            add_partitions_to_txn_response_frame(header.correlation_id)
        }),
    ])
    .await;
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new({
            let coordinator = coordinator.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::FindCoordinator as i16);
                find_coordinator_response_frame(header.correlation_id, 9, coordinator)
            }
        }),
        Box::new({
            let leader_7 = leader_7.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::Metadata as i16);
                let response = metadata_response([(7, leader_7)]);
                response_frame(ApiKey::Metadata, 13, header.correlation_id, &response)
            }
        }),
    ])
    .await;
    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default(),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let mut producer = transactional_test_producer_with_retries(wire, 1);

    producer.init_transactions().await.unwrap();
    producer.begin_transaction().unwrap();
    let delivery = producer
        .send(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")))
        .unwrap();
    producer.flush().await.unwrap();

    assert_eq!(delivery.await.unwrap().offset, 90);
    assert_eq!(bootstrap.join().await, 3);
    assert_eq!(coordinator.join().await, 4);
    assert_eq!(leader_7.join().await, 2);
}

#[tokio::test]
async fn kafka_producer_add_partitions_reloads_transaction_coordinator() {
    let leader_7 = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::Produce as i16);
            produce_response_frame_for_request(&header, 0, 90)
        }),
    ])
    .await;
    let stale_coordinator = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::InitProducerId as i16);
            init_producer_id_response_frame(header.correlation_id, 77, 4)
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::AddPartitionsToTxn as i16);
            add_partitions_to_txn_error_response_frame(
                header.correlation_id,
                ErrorCode::NotCoordinator,
            )
        }),
    ])
    .await;
    let refreshed_coordinator = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::AddPartitionsToTxn as i16);
            add_partitions_to_txn_response_frame(header.correlation_id)
        }),
    ])
    .await;
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new({
            let coordinator = stale_coordinator.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::FindCoordinator as i16);
                find_coordinator_response_frame(header.correlation_id, 9, coordinator)
            }
        }),
        Box::new({
            let leader_7 = leader_7.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::Metadata as i16);
                let response = metadata_response([(7, leader_7)]);
                response_frame(ApiKey::Metadata, 13, header.correlation_id, &response)
            }
        }),
        Box::new({
            let coordinator = refreshed_coordinator.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::FindCoordinator as i16);
                find_coordinator_response_frame(header.correlation_id, 10, coordinator)
            }
        }),
    ])
    .await;
    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default(),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let mut producer = transactional_test_producer_with_retries(wire, 1);

    producer.init_transactions().await.unwrap();
    producer.begin_transaction().unwrap();
    let delivery = producer
        .send(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")))
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), producer.flush())
        .await
        .expect("flush should complete after add partitions retry")
        .unwrap();

    assert_eq!(delivery.await.unwrap().offset, 90);
    assert_eq!(bootstrap.join().await, 4);
    assert_eq!(stale_coordinator.join().await, 3);
    assert_eq!(refreshed_coordinator.join().await, 2);
    assert_eq!(leader_7.join().await, 2);
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "Transaction offset commit fixture keeps ordered broker handlers inline."
)]
async fn kafka_producer_sends_offsets_to_transaction_before_commit() {
    let coordinator = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::InitProducerId as i16);
            init_producer_id_response_frame(header.correlation_id, 77, 4)
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::AddOffsetsToTxn as i16);
            let add =
                AddOffsetsToTxnRequestData::read(&mut request, 4).expect("add offsets to txn");
            assert_eq!(
                add.transactional_id,
                KafkaString::from("txn-orders".to_owned())
            );
            assert_eq!(add.producer_id, 77);
            assert_eq!(add.producer_epoch, 4);
            assert_eq!(add.group_id, KafkaString::from("group-a".to_owned()));
            add_offsets_to_txn_response_frame(header.correlation_id, ErrorCode::None)
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::TxnOffsetCommit as i16);
            let commit = TxnOffsetCommitRequestData::read(&mut request, header.request_api_version)
                .expect("txn offset commit");
            assert_eq!(
                commit.transactional_id,
                KafkaString::from("txn-orders".to_owned())
            );
            assert_eq!(commit.group_id, KafkaString::from("group-a".to_owned()));
            assert_eq!(commit.producer_id, 77);
            assert_eq!(commit.producer_epoch, 4);
            assert_eq!(commit.generation_id, 42);
            assert_eq!(commit.member_id, KafkaString::from("member-a".to_owned()));
            assert_eq!(
                commit.group_instance_id,
                Some(KafkaString::from("instance-a".to_owned()))
            );
            assert_eq!(commit.topics.len(), 1);
            assert_eq!(
                commit.topics[0].name,
                KafkaString::from("orders".to_owned())
            );
            assert_eq!(commit.topics[0].partitions.len(), 1);
            assert_eq!(commit.topics[0].partitions[0].partition_index, 0);
            assert_eq!(commit.topics[0].partitions[0].committed_offset, 42);
            assert_eq!(commit.topics[0].partitions[0].committed_leader_epoch, 9);
            assert_eq!(
                commit.topics[0].partitions[0].committed_metadata,
                Some(KafkaString::from(String::new()))
            );
            txn_offset_commit_response_frame(header.correlation_id, ErrorCode::None)
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::EndTxn as i16);
            end_txn_response_frame_for_request(&header)
        }),
    ])
    .await;
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new({
            let coordinator = coordinator.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::FindCoordinator as i16);
                let find =
                    FindCoordinatorRequestData::read(&mut request, 6).expect("find coordinator");
                assert_eq!(
                    find.coordinator_keys,
                    vec![KafkaString::from("txn-orders".to_owned())]
                );
                find_coordinator_response_frame(header.correlation_id, 9, coordinator)
            }
        }),
        Box::new({
            let coordinator = coordinator.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::FindCoordinator as i16);
                let find =
                    FindCoordinatorRequestData::read(&mut request, 6).expect("find coordinator");
                assert_eq!(
                    find.coordinator_keys,
                    vec![KafkaString::from("group-a".to_owned())]
                );
                find_group_coordinator_response_frame(header.correlation_id, 9, coordinator)
            }
        }),
    ])
    .await;

    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default(),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let mut producer = Producer::from_parts(
        wire,
        ProducerRuntimeConfig {
            accumulator: AccumulatorConfig::default()
                .batch_size(1)
                .buffer_memory(16 * 1024),
            acks: -1,
            timeout_ms: 30_000,
            retry_attempts: 0,
            retry_backoff: Duration::from_millis(100),
            retry_backoff_max: Duration::from_secs(1),
            delivery_timeout: Duration::from_mins(2),
            max_block: Duration::from_mins(1),
            partitioner_ignore_keys: false,
            partitioner_adaptive_partitioning_enable: true,
            partitioner_availability_timeout: Duration::ZERO,
            max_in_flight_requests_per_connection: 5,
            max_request_size: 1_048_576,
            enable_metrics_push: true,
            compression: ProducerCompression::default(),
            idempotence: ProducerIdempotenceConfig {
                enabled: true,
                transactional_id: Some("txn-orders".to_owned()),
                transaction_timeout_ms: 60_000,
                transaction_two_phase_commit: false,
            },
        },
    );

    producer.init_transactions().await.unwrap();
    producer.begin_transaction().unwrap();
    producer
        .send_offsets_to_transaction(
            [(
                kacrab::producer::TopicPartition::new("orders", 0),
                kacrab::producer::OffsetAndMetadata::new(42).leader_epoch(9),
            )],
            kacrab::producer::ConsumerGroupMetadata::new("group-a")
                .generation_id(42)
                .member_id("member-a")
                .group_instance_id("instance-a"),
        )
        .await
        .unwrap();
    let metrics = producer.metrics();
    assert_eq!(metrics.send_offsets_to_transaction_count, 1);
    assert!(metrics.send_offsets_to_transaction_total_latency >= Duration::ZERO);
    producer.commit_transaction().await.unwrap();

    assert_eq!(bootstrap.join().await, 3);
    assert_eq!(coordinator.join().await, 5);
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "Transaction V2 offset commit fixture keeps ordered broker handlers inline."
)]
async fn kafka_producer_transaction_v2_skips_add_offsets_to_txn_like_java() {
    let coordinator = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::InitProducerId as i16);
            let init = InitProducerIdRequestData::read(&mut request, header.request_api_version)
                .expect("init producer id");
            assert_eq!(
                init.transactional_id,
                Some(KafkaString::from("txn-orders".to_owned()))
            );
            assert!(init.enable2_pc);
            init_producer_id_response_frame_for_request(&header, 77, 4)
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::TxnOffsetCommit as i16);
            let commit = TxnOffsetCommitRequestData::read(&mut request, header.request_api_version)
                .expect("txn offset commit");
            assert_eq!(
                commit.transactional_id,
                KafkaString::from("txn-orders".to_owned())
            );
            assert_eq!(commit.group_id, KafkaString::from("group-a".to_owned()));
            assert_eq!(commit.producer_id, 77);
            assert_eq!(commit.producer_epoch, 4);
            assert_eq!(commit.generation_id, 42);
            assert_eq!(commit.member_id, KafkaString::from("member-a".to_owned()));
            assert_eq!(
                commit.group_instance_id,
                Some(KafkaString::from("instance-a".to_owned()))
            );
            assert_eq!(commit.topics.len(), 1);
            assert_eq!(
                commit.topics[0].name,
                KafkaString::from("orders".to_owned())
            );
            assert_eq!(commit.topics[0].partitions.len(), 1);
            assert_eq!(commit.topics[0].partitions[0].partition_index, 0);
            assert_eq!(commit.topics[0].partitions[0].committed_offset, 42);
            txn_offset_commit_response_frame(header.correlation_id, ErrorCode::None)
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::EndTxn as i16);
            end_txn_response_frame_for_request(&header)
        }),
    ])
    .await;
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new({
            let coordinator = coordinator.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::FindCoordinator as i16);
                let find =
                    FindCoordinatorRequestData::read(&mut request, 6).expect("find coordinator");
                assert_eq!(
                    find.coordinator_keys,
                    vec![KafkaString::from("txn-orders".to_owned())]
                );
                find_coordinator_response_frame(header.correlation_id, 9, coordinator)
            }
        }),
        Box::new({
            let coordinator = coordinator.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::FindCoordinator as i16);
                let find =
                    FindCoordinatorRequestData::read(&mut request, 6).expect("find coordinator");
                assert_eq!(
                    find.coordinator_keys,
                    vec![KafkaString::from("group-a".to_owned())]
                );
                find_group_coordinator_response_frame(header.correlation_id, 9, coordinator)
            }
        }),
    ])
    .await;

    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default(),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let mut producer = transaction_v2_test_producer(wire);

    producer.init_transactions().await.unwrap();
    producer.begin_transaction().unwrap();
    producer
        .send_offsets_to_transaction(
            [(
                kacrab::producer::TopicPartition::new("orders", 0),
                kacrab::producer::OffsetAndMetadata::new(42),
            )],
            kacrab::producer::ConsumerGroupMetadata::new("group-a")
                .generation_id(42)
                .member_id("member-a")
                .group_instance_id("instance-a"),
        )
        .await
        .unwrap();
    producer.commit_transaction().await.unwrap();

    assert_eq!(bootstrap.join().await, 3);
    assert_eq!(coordinator.join().await, 4);
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "Transaction offset error fixture keeps ordered broker handlers inline."
)]
async fn kafka_producer_send_offsets_to_transaction_reports_commit_error() {
    let coordinator = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::InitProducerId as i16);
            init_producer_id_response_frame(header.correlation_id, 77, 4)
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::AddOffsetsToTxn as i16);
            add_offsets_to_txn_response_frame(header.correlation_id, ErrorCode::None)
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::TxnOffsetCommit as i16);
            txn_offset_commit_response_frame(
                header.correlation_id,
                ErrorCode::GroupAuthorizationFailed,
            )
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::EndTxn as i16);
            let end =
                EndTxnRequestData::read(&mut request, header.request_api_version).expect("end txn");
            assert!(!end.committed);
            end_txn_response_frame_for_request(&header)
        }),
    ])
    .await;
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new({
            let coordinator = coordinator.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::FindCoordinator as i16);
                find_coordinator_response_frame(header.correlation_id, 9, coordinator)
            }
        }),
        Box::new({
            let coordinator = coordinator.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::FindCoordinator as i16);
                find_group_coordinator_response_frame(header.correlation_id, 9, coordinator)
            }
        }),
    ])
    .await;
    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default(),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let mut producer = Producer::from_parts(
        wire,
        ProducerRuntimeConfig {
            accumulator: AccumulatorConfig::default()
                .batch_size(1)
                .buffer_memory(16 * 1024),
            acks: -1,
            timeout_ms: 30_000,
            retry_attempts: 0,
            retry_backoff: Duration::from_millis(100),
            retry_backoff_max: Duration::from_secs(1),
            delivery_timeout: Duration::from_mins(2),
            max_block: Duration::from_mins(1),
            partitioner_ignore_keys: false,
            partitioner_adaptive_partitioning_enable: true,
            partitioner_availability_timeout: Duration::ZERO,
            max_in_flight_requests_per_connection: 5,
            max_request_size: 1_048_576,
            enable_metrics_push: true,
            compression: ProducerCompression::default(),
            idempotence: ProducerIdempotenceConfig {
                enabled: true,
                transactional_id: Some("txn-orders".to_owned()),
                transaction_timeout_ms: 60_000,
                transaction_two_phase_commit: false,
            },
        },
    );

    producer.init_transactions().await.unwrap();
    producer.begin_transaction().unwrap();
    let error = producer
        .send_offsets_to_transaction(
            [(
                kacrab::producer::TopicPartition::new("orders", 0),
                kacrab::producer::OffsetAndMetadata::new(42),
            )],
            kacrab::producer::ConsumerGroupMetadata::new("group-a"),
        )
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        kacrab::producer::ProducerError::Transaction {
            operation: "txn_offset_commit",
            error: ErrorCode::GroupAuthorizationFailed,
        }
    ));
    let commit_after_group_lookup_error =
        tokio::time::timeout(Duration::from_millis(200), producer.commit_transaction())
            .await
            .expect("commit after group lookup error should fail locally");
    assert!(matches!(
        commit_after_group_lookup_error,
        Err(kacrab::producer::ProducerError::Transaction {
            operation: "transaction_state",
            error: ErrorCode::GroupAuthorizationFailed,
        })
    ));
    producer.abort_transaction().await.unwrap();
    let metrics = producer.metrics();
    assert_eq!(metrics.transaction_abort_count, 1);
    assert!(metrics.transaction_abort_total_latency >= Duration::ZERO);
    assert_eq!(bootstrap.join().await, 3);
    assert_eq!(coordinator.join().await, 5);
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "Send-offsets timeout fixture keeps ordered broker handlers inline."
)]
async fn kafka_producer_send_offsets_timeout_can_retry_same_operation_like_java() {
    let coordinator = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::InitProducerId as i16);
            init_producer_id_response_frame(header.correlation_id, 77, 4)
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::AddOffsetsToTxn as i16);
            add_offsets_to_txn_response_frame(header.correlation_id, ErrorCode::None)
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::TxnOffsetCommit as i16);
            std::thread::sleep(Duration::from_millis(50));
            txn_offset_commit_response_frame(header.correlation_id, ErrorCode::None)
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::EndTxn as i16);
            end_txn_response_frame_for_request(&header)
        }),
    ])
    .await;
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new({
            let coordinator = coordinator.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::FindCoordinator as i16);
                find_coordinator_response_frame(header.correlation_id, 9, coordinator)
            }
        }),
        Box::new({
            let coordinator = coordinator.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::FindCoordinator as i16);
                find_group_coordinator_response_frame(header.correlation_id, 9, coordinator)
            }
        }),
    ])
    .await;
    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default(),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let mut producer = Producer::from_parts(
        wire,
        ProducerRuntimeConfig {
            accumulator: AccumulatorConfig::default()
                .batch_size(1)
                .buffer_memory(16 * 1024),
            acks: -1,
            timeout_ms: 30_000,
            retry_attempts: 0,
            retry_backoff: Duration::from_millis(100),
            retry_backoff_max: Duration::from_secs(1),
            delivery_timeout: Duration::from_mins(2),
            max_block: Duration::from_millis(30),
            partitioner_ignore_keys: false,
            partitioner_adaptive_partitioning_enable: true,
            partitioner_availability_timeout: Duration::ZERO,
            max_in_flight_requests_per_connection: 5,
            max_request_size: 1_048_576,
            enable_metrics_push: true,
            compression: ProducerCompression::default(),
            idempotence: ProducerIdempotenceConfig {
                enabled: true,
                transactional_id: Some("txn-orders".to_owned()),
                transaction_timeout_ms: 60_000,
                transaction_two_phase_commit: false,
            },
        },
    );

    producer.init_transactions().await.unwrap();
    producer.begin_transaction().unwrap();
    let offsets = [(
        kacrab::producer::TopicPartition::new("orders", 0),
        kacrab::producer::OffsetAndMetadata::new(42),
    )];
    let group = kacrab::producer::ConsumerGroupMetadata::new("group-a");

    assert!(matches!(
        producer
            .send_offsets_to_transaction(offsets.clone(), group.clone())
            .await
            .expect_err("first offset commit should time out while request remains in flight"),
        kacrab::producer::ProducerError::DispatchTask(message)
            if message.contains("SendOffsetsToTransaction timed out")
    ));
    assert!(matches!(
        producer
            .commit_transaction()
            .await
            .expect_err("commit must not replace pending send-offsets result"),
        kacrab::producer::ProducerError::InvalidTransactionState(message)
            if message == "previous transaction operation is pending and must be retried"
    ));
    producer
        .send_offsets_to_transaction(offsets, group)
        .await
        .expect("retrying same offset operation should await cached result");

    producer.commit_transaction().await.unwrap();
    assert_eq!(producer.metrics().send_offsets_to_transaction_count, 1);
    assert_eq!(bootstrap.join().await, 3);
    assert_eq!(coordinator.join().await, 5);
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "Transaction control UnknownProducerId fixture verifies abort epoch bump flow."
)]
async fn kafka_producer_add_offsets_unknown_producer_id_bumps_epoch_after_abort_like_java() {
    let coordinator = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::InitProducerId as i16);
            init_producer_id_response_frame(header.correlation_id, 77, 4)
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::AddOffsetsToTxn as i16);
            add_offsets_to_txn_response_frame(header.correlation_id, ErrorCode::UnknownProducerId)
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::InitProducerId as i16);
            let init = InitProducerIdRequestData::read(&mut request, header.request_api_version)
                .expect("init producer id");
            assert_eq!(init.producer_id, 77);
            assert_eq!(init.producer_epoch, 4);
            init_producer_id_response_frame(header.correlation_id, 77, 5)
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::AddPartitionsToTxn as i16);
            let add = AddPartitionsToTxnRequestData::read(&mut request, 5)
                .expect("add partitions to txn");
            let transaction = add.transactions.first().expect("transaction");
            assert_eq!(transaction.producer_id, 77);
            assert_eq!(transaction.producer_epoch, 5);
            add_partitions_to_txn_response_frame(header.correlation_id)
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::EndTxn as i16);
            let end =
                EndTxnRequestData::read(&mut request, header.request_api_version).expect("end txn");
            assert_eq!(end.producer_id, 77);
            assert_eq!(end.producer_epoch, 5);
            assert!(end.committed);
            end_txn_response_frame_for_request(&header)
        }),
    ])
    .await;
    let leader_7 = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::Produce as i16);
            let produce = ProduceRequestData::read(&mut request, header.request_api_version)
                .expect("produce request");
            let mut records = produce.topic_data[0].partition_data[0]
                .records
                .clone()
                .expect("records");
            let batch = RecordBatch::decode(&mut records).expect("record batch");
            assert_eq!(batch.producer_id, 77);
            assert_eq!(batch.producer_epoch, 5);
            assert_eq!(batch.base_sequence, 0);
            produce_response_frame_for_request(&header, 0, 92)
        }),
    ])
    .await;
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new({
            let coordinator = coordinator.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::FindCoordinator as i16);
                find_coordinator_response_frame(header.correlation_id, 9, coordinator)
            }
        }),
        Box::new({
            let leader_7 = leader_7.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::Metadata as i16);
                let response = metadata_response([(7, leader_7)]);
                response_frame(ApiKey::Metadata, 13, header.correlation_id, &response)
            }
        }),
    ])
    .await;
    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default(),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let mut producer = transactional_test_producer(wire);

    producer.init_transactions().await.unwrap();
    producer.begin_transaction().unwrap();
    let error = producer
        .send_offsets_to_transaction(
            [(
                kacrab::producer::TopicPartition::new("orders", 0),
                kacrab::producer::OffsetAndMetadata::new(42),
            )],
            kacrab::producer::ConsumerGroupMetadata::new("group-a"),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        kacrab::producer::ProducerError::Transaction {
            operation: "add_offsets_to_txn",
            error: ErrorCode::UnknownProducerId,
        }
    ));

    producer.abort_transaction().await.unwrap();
    producer.begin_transaction().unwrap();
    let delivery = producer
        .send(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"recovered")))
        .unwrap();
    producer.flush().await.unwrap();
    producer.commit_transaction().await.unwrap();

    assert_eq!(delivery.await.unwrap().offset, 92);
    assert_eq!(bootstrap.join().await, 3);
    assert_eq!(coordinator.join().await, 6);
    assert_eq!(leader_7.join().await, 2);
}

#[tokio::test]
async fn kafka_producer_send_offsets_group_coordinator_auth_failure_is_abortable_like_java() {
    let coordinator = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::InitProducerId as i16);
            init_producer_id_response_frame(header.correlation_id, 77, 4)
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::AddOffsetsToTxn as i16);
            add_offsets_to_txn_response_frame(header.correlation_id, ErrorCode::None)
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::EndTxn as i16);
            let end =
                EndTxnRequestData::read(&mut request, header.request_api_version).expect("end txn");
            assert!(!end.committed);
            end_txn_response_frame_for_request(&header)
        }),
    ])
    .await;
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new({
            let coordinator = coordinator.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::FindCoordinator as i16);
                find_coordinator_response_frame(header.correlation_id, 9, coordinator)
            }
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::FindCoordinator as i16);
            find_group_coordinator_error_response_frame(
                header.correlation_id,
                ErrorCode::GroupAuthorizationFailed,
            )
        }),
    ])
    .await;
    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default(),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let mut producer = transactional_test_producer(wire);

    producer.init_transactions().await.unwrap();
    producer.begin_transaction().unwrap();
    let error = producer
        .send_offsets_to_transaction(
            [(
                kacrab::producer::TopicPartition::new("orders", 0),
                kacrab::producer::OffsetAndMetadata::new(42),
            )],
            kacrab::producer::ConsumerGroupMetadata::new("group-a"),
        )
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        kacrab::producer::ProducerError::Transaction {
            operation: "find_coordinator",
            error: ErrorCode::GroupAuthorizationFailed,
        }
    ));
    let commit_after_group_lookup_error =
        tokio::time::timeout(Duration::from_millis(200), producer.commit_transaction())
            .await
            .expect("commit after group lookup error should fail locally");
    assert!(matches!(
        commit_after_group_lookup_error,
        Err(kacrab::producer::ProducerError::Transaction {
            operation: "transaction_state",
            error: ErrorCode::GroupAuthorizationFailed,
        })
    ));
    producer.abort_transaction().await.unwrap();
    assert_eq!(bootstrap.join().await, 3);
    assert_eq!(coordinator.join().await, 4);
}

#[tokio::test]
async fn kafka_producer_send_offsets_to_transaction_fatal_commit_error_blocks_abort() {
    let coordinator = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::InitProducerId as i16);
            init_producer_id_response_frame(header.correlation_id, 77, 4)
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::AddOffsetsToTxn as i16);
            add_offsets_to_txn_response_frame(header.correlation_id, ErrorCode::None)
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::TxnOffsetCommit as i16);
            txn_offset_commit_response_frame(
                header.correlation_id,
                ErrorCode::UnsupportedForMessageFormat,
            )
        }),
    ])
    .await;
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new({
            let coordinator = coordinator.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::FindCoordinator as i16);
                find_coordinator_response_frame(header.correlation_id, 9, coordinator)
            }
        }),
        Box::new({
            let coordinator = coordinator.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::FindCoordinator as i16);
                find_group_coordinator_response_frame(header.correlation_id, 9, coordinator)
            }
        }),
    ])
    .await;
    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default(),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let mut producer = transactional_test_producer(wire);

    producer.init_transactions().await.unwrap();
    producer.begin_transaction().unwrap();
    let error = producer
        .send_offsets_to_transaction(
            [(
                kacrab::producer::TopicPartition::new("orders", 0),
                kacrab::producer::OffsetAndMetadata::new(42),
            )],
            kacrab::producer::ConsumerGroupMetadata::new("group-a"),
        )
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        kacrab::producer::ProducerError::Transaction {
            operation: "txn_offset_commit",
            error: ErrorCode::UnsupportedForMessageFormat,
        }
    ));

    let abort_after_fatal_offsets =
        tokio::time::timeout(Duration::from_millis(200), producer.abort_transaction())
            .await
            .expect("abort after fatal offsets error should fail locally");
    assert!(matches!(
        abort_after_fatal_offsets,
        Err(kacrab::producer::ProducerError::Transaction {
            operation: "transaction_state",
            error: ErrorCode::UnsupportedForMessageFormat,
        })
    ));
    assert_eq!(bootstrap.join().await, 3);
    assert_eq!(coordinator.join().await, 4);
}

#[tokio::test]
async fn kafka_producer_txn_offset_commit_fenced_instance_id_allows_abort() {
    let coordinator = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::InitProducerId as i16);
            init_producer_id_response_frame(header.correlation_id, 77, 4)
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::AddOffsetsToTxn as i16);
            add_offsets_to_txn_response_frame(header.correlation_id, ErrorCode::None)
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::TxnOffsetCommit as i16);
            txn_offset_commit_response_frame(header.correlation_id, ErrorCode::FencedInstanceId)
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::EndTxn as i16);
            let end =
                EndTxnRequestData::read(&mut request, header.request_api_version).expect("end txn");
            assert!(!end.committed);
            end_txn_response_frame_for_request(&header)
        }),
    ])
    .await;
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new({
            let coordinator = coordinator.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::FindCoordinator as i16);
                find_coordinator_response_frame(header.correlation_id, 9, coordinator)
            }
        }),
        Box::new({
            let coordinator = coordinator.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::FindCoordinator as i16);
                find_group_coordinator_response_frame(header.correlation_id, 9, coordinator)
            }
        }),
    ])
    .await;
    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default(),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let mut producer = transactional_test_producer(wire);

    producer.init_transactions().await.unwrap();
    producer.begin_transaction().unwrap();
    let error = producer
        .send_offsets_to_transaction(
            [(
                kacrab::producer::TopicPartition::new("orders", 0),
                kacrab::producer::OffsetAndMetadata::new(42),
            )],
            kacrab::producer::ConsumerGroupMetadata::new("group-a"),
        )
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        kacrab::producer::ProducerError::Transaction {
            operation: "txn_offset_commit",
            error: ErrorCode::FencedInstanceId,
        }
    ));
    producer.abort_transaction().await.unwrap();
    assert_eq!(bootstrap.join().await, 3);
    assert_eq!(coordinator.join().await, 5);
}

#[tokio::test]
async fn kafka_producer_txn_offset_commit_invalid_epoch_is_producer_fenced_like_java() {
    let coordinator = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::InitProducerId as i16);
            init_producer_id_response_frame(header.correlation_id, 77, 4)
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::AddOffsetsToTxn as i16);
            add_offsets_to_txn_response_frame(header.correlation_id, ErrorCode::None)
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::TxnOffsetCommit as i16);
            txn_offset_commit_response_frame(header.correlation_id, ErrorCode::InvalidProducerEpoch)
        }),
    ])
    .await;
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new({
            let coordinator = coordinator.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::FindCoordinator as i16);
                find_coordinator_response_frame(header.correlation_id, 9, coordinator)
            }
        }),
        Box::new({
            let coordinator = coordinator.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::FindCoordinator as i16);
                find_group_coordinator_response_frame(header.correlation_id, 9, coordinator)
            }
        }),
    ])
    .await;
    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default(),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let mut producer = transactional_test_producer(wire);

    producer.init_transactions().await.unwrap();
    producer.begin_transaction().unwrap();
    let error = producer
        .send_offsets_to_transaction(
            [(
                kacrab::producer::TopicPartition::new("orders", 0),
                kacrab::producer::OffsetAndMetadata::new(42),
            )],
            kacrab::producer::ConsumerGroupMetadata::new("group-a"),
        )
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        kacrab::producer::ProducerError::Transaction {
            operation: "txn_offset_commit",
            error: ErrorCode::ProducerFenced,
        }
    ));
    let abort_after_invalid_epoch =
        tokio::time::timeout(Duration::from_millis(200), producer.abort_transaction())
            .await
            .expect("abort after invalid epoch should fail locally");
    assert!(matches!(
        abort_after_invalid_epoch,
        Err(kacrab::producer::ProducerError::Transaction {
            operation: "transaction_state",
            error: ErrorCode::ProducerFenced,
        })
    ));
    assert_eq!(bootstrap.join().await, 3);
    assert_eq!(coordinator.join().await, 4);
}

#[tokio::test]
async fn kafka_producer_txn_offset_commit_fatal_partition_error_wins_over_reload_like_java() {
    let transaction_coordinator = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::InitProducerId as i16);
            init_producer_id_response_frame(header.correlation_id, 77, 4)
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::AddOffsetsToTxn as i16);
            add_offsets_to_txn_response_frame(header.correlation_id, ErrorCode::None)
        }),
    ])
    .await;
    let stale_group_coordinator = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::TxnOffsetCommit as i16);
            txn_offset_commit_multi_partition_response_frame(
                header.correlation_id,
                [
                    (0, ErrorCode::RequestTimedOut),
                    (1, ErrorCode::ProducerFenced),
                ],
            )
        }),
    ])
    .await;
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new({
            let transaction_coordinator = transaction_coordinator.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::FindCoordinator as i16);
                find_coordinator_response_frame(header.correlation_id, 9, transaction_coordinator)
            }
        }),
        Box::new({
            let stale_group_coordinator = stale_group_coordinator.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::FindCoordinator as i16);
                find_group_coordinator_response_frame(
                    header.correlation_id,
                    10,
                    stale_group_coordinator,
                )
            }
        }),
    ])
    .await;
    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default(),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let mut producer = transactional_test_producer_with_retries(wire, 1);

    producer.init_transactions().await.unwrap();
    producer.begin_transaction().unwrap();
    let send_offsets = tokio::time::timeout(
        Duration::from_millis(500),
        producer.send_offsets_to_transaction(
            [
                (
                    kacrab::producer::TopicPartition::new("orders", 0),
                    kacrab::producer::OffsetAndMetadata::new(42),
                ),
                (
                    kacrab::producer::TopicPartition::new("orders", 1),
                    kacrab::producer::OffsetAndMetadata::new(43),
                ),
            ],
            kacrab::producer::ConsumerGroupMetadata::new("group-a"),
        ),
    )
    .await
    .expect("txn offset commit should classify fatal partition error without reloading");
    assert!(matches!(
        send_offsets,
        Err(kacrab::producer::ProducerError::Transaction {
            operation: "txn_offset_commit",
            error: ErrorCode::ProducerFenced,
        })
    ));
    let abort_after_fatal_commit =
        tokio::time::timeout(Duration::from_millis(200), producer.abort_transaction())
            .await
            .expect("abort after fatal txn offset commit error should fail locally");
    assert!(matches!(
        abort_after_fatal_commit,
        Err(kacrab::producer::ProducerError::Transaction {
            operation: "transaction_state",
            error: ErrorCode::ProducerFenced,
        })
    ));

    assert_eq!(bootstrap.join().await, 3);
    assert_eq!(transaction_coordinator.join().await, 3);
    assert_eq!(stale_group_coordinator.join().await, 2);
}

#[tokio::test]
async fn kafka_producer_txn_offset_commit_reloads_group_coordinator() {
    let transaction_coordinator = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::InitProducerId as i16);
            init_producer_id_response_frame(header.correlation_id, 77, 4)
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::AddOffsetsToTxn as i16);
            add_offsets_to_txn_response_frame(header.correlation_id, ErrorCode::None)
        }),
    ])
    .await;
    let stale_group_coordinator = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::TxnOffsetCommit as i16);
            txn_offset_commit_response_frame(header.correlation_id, ErrorCode::NotCoordinator)
        }),
    ])
    .await;
    let refreshed_group_coordinator = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::TxnOffsetCommit as i16);
            let commit = TxnOffsetCommitRequestData::read(&mut request, header.request_api_version)
                .expect("txn offset commit");
            assert_eq!(commit.group_id, KafkaString::from("group-a".to_owned()));
            txn_offset_commit_response_frame(header.correlation_id, ErrorCode::None)
        }),
    ])
    .await;
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new({
            let transaction_coordinator = transaction_coordinator.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::FindCoordinator as i16);
                find_coordinator_response_frame(header.correlation_id, 9, transaction_coordinator)
            }
        }),
        Box::new({
            let stale_group_coordinator = stale_group_coordinator.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::FindCoordinator as i16);
                find_group_coordinator_response_frame(
                    header.correlation_id,
                    10,
                    stale_group_coordinator,
                )
            }
        }),
        Box::new({
            let refreshed_group_coordinator = refreshed_group_coordinator.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::FindCoordinator as i16);
                find_group_coordinator_response_frame(
                    header.correlation_id,
                    11,
                    refreshed_group_coordinator,
                )
            }
        }),
    ])
    .await;
    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default(),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let producer = transactional_test_producer_with_retries(wire, 1);

    producer.init_transactions().await.unwrap();
    producer.begin_transaction().unwrap();
    let send_offsets = tokio::time::timeout(
        Duration::from_secs(1),
        producer.send_offsets_to_transaction(
            [(
                kacrab::producer::TopicPartition::new("orders", 0),
                kacrab::producer::OffsetAndMetadata::new(42),
            )],
            kacrab::producer::ConsumerGroupMetadata::new("group-a"),
        ),
    )
    .await
    .expect("txn offset commit should reload coordinator before timing out");
    send_offsets.unwrap();

    assert_eq!(bootstrap.join().await, 4);
    assert_eq!(transaction_coordinator.join().await, 3);
    assert_eq!(stale_group_coordinator.join().await, 2);
    assert_eq!(refreshed_group_coordinator.join().await, 2);
}

#[tokio::test]
async fn kafka_producer_txn_offset_commit_refresh_group_auth_failure_is_abortable_like_java() {
    let transaction_coordinator = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::InitProducerId as i16);
            init_producer_id_response_frame(header.correlation_id, 77, 4)
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::AddOffsetsToTxn as i16);
            add_offsets_to_txn_response_frame(header.correlation_id, ErrorCode::None)
        }),
    ])
    .await;
    let stale_group_coordinator = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::TxnOffsetCommit as i16);
            txn_offset_commit_response_frame(header.correlation_id, ErrorCode::NotCoordinator)
        }),
    ])
    .await;
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new({
            let transaction_coordinator = transaction_coordinator.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::FindCoordinator as i16);
                find_coordinator_response_frame(header.correlation_id, 9, transaction_coordinator)
            }
        }),
        Box::new({
            let stale_group_coordinator = stale_group_coordinator.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::FindCoordinator as i16);
                find_group_coordinator_response_frame(
                    header.correlation_id,
                    10,
                    stale_group_coordinator,
                )
            }
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::FindCoordinator as i16);
            find_group_coordinator_error_response_frame(
                header.correlation_id,
                ErrorCode::GroupAuthorizationFailed,
            )
        }),
    ])
    .await;
    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default(),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let mut producer = transactional_test_producer_with_retries(wire, 1);

    producer.init_transactions().await.unwrap();
    producer.begin_transaction().unwrap();
    let error = producer
        .send_offsets_to_transaction(
            [(
                kacrab::producer::TopicPartition::new("orders", 0),
                kacrab::producer::OffsetAndMetadata::new(42),
            )],
            kacrab::producer::ConsumerGroupMetadata::new("group-a"),
        )
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        kacrab::producer::ProducerError::Transaction {
            operation: "find_coordinator",
            error: ErrorCode::GroupAuthorizationFailed,
        }
    ));
    let commit_after_group_lookup_error =
        tokio::time::timeout(Duration::from_millis(200), producer.commit_transaction())
            .await
            .expect("commit after group lookup error should fail locally");
    assert!(matches!(
        commit_after_group_lookup_error,
        Err(kacrab::producer::ProducerError::Transaction {
            operation: "transaction_state",
            error: ErrorCode::GroupAuthorizationFailed,
        })
    ));
    assert_eq!(bootstrap.join().await, 4);
    assert_eq!(transaction_coordinator.join().await, 3);
    assert_eq!(stale_group_coordinator.join().await, 2);
}

#[tokio::test]
async fn kafka_producer_add_offsets_refresh_fatal_error_blocks_abort_like_java() {
    let transaction_coordinator = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::InitProducerId as i16);
            init_producer_id_response_frame(header.correlation_id, 77, 4)
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::AddOffsetsToTxn as i16);
            add_offsets_to_txn_response_frame(header.correlation_id, ErrorCode::NotCoordinator)
        }),
    ])
    .await;
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new({
            let transaction_coordinator = transaction_coordinator.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::FindCoordinator as i16);
                find_coordinator_response_frame(header.correlation_id, 9, transaction_coordinator)
            }
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::FindCoordinator as i16);
            find_coordinator_error_response_frame(
                header.correlation_id,
                ErrorCode::TransactionalIdAuthorizationFailed,
            )
        }),
    ])
    .await;
    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default(),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let mut producer = transactional_test_producer_with_retries(wire, 1);

    producer.init_transactions().await.unwrap();
    producer.begin_transaction().unwrap();
    let error = producer
        .send_offsets_to_transaction(
            [(
                kacrab::producer::TopicPartition::new("orders", 0),
                kacrab::producer::OffsetAndMetadata::new(42),
            )],
            kacrab::producer::ConsumerGroupMetadata::new("group-a"),
        )
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        kacrab::producer::ProducerError::Transaction {
            operation: "find_coordinator",
            error: ErrorCode::TransactionalIdAuthorizationFailed,
        }
    ));
    assert!(matches!(
        producer.abort_transaction().await,
        Err(kacrab::producer::ProducerError::Transaction {
            operation: "transaction_state",
            error: ErrorCode::TransactionalIdAuthorizationFailed,
        })
    ));
    assert_eq!(bootstrap.join().await, 3);
    assert_eq!(transaction_coordinator.join().await, 3);
}

#[tokio::test]
async fn kafka_producer_add_offsets_reloads_transaction_coordinator() {
    let stale_transaction_coordinator = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::InitProducerId as i16);
            init_producer_id_response_frame(header.correlation_id, 77, 4)
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::AddOffsetsToTxn as i16);
            add_offsets_to_txn_response_frame(header.correlation_id, ErrorCode::NotCoordinator)
        }),
    ])
    .await;
    let refreshed_transaction_coordinator = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::AddOffsetsToTxn as i16);
            add_offsets_to_txn_response_frame(header.correlation_id, ErrorCode::None)
        }),
    ])
    .await;
    let group_coordinator = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::TxnOffsetCommit as i16);
            txn_offset_commit_response_frame(header.correlation_id, ErrorCode::None)
        }),
    ])
    .await;
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new({
            let stale_transaction_coordinator = stale_transaction_coordinator.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::FindCoordinator as i16);
                find_coordinator_response_frame(
                    header.correlation_id,
                    9,
                    stale_transaction_coordinator,
                )
            }
        }),
        Box::new({
            let refreshed_transaction_coordinator = refreshed_transaction_coordinator.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::FindCoordinator as i16);
                find_coordinator_response_frame(
                    header.correlation_id,
                    10,
                    refreshed_transaction_coordinator,
                )
            }
        }),
        Box::new({
            let group_coordinator = group_coordinator.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::FindCoordinator as i16);
                find_group_coordinator_response_frame(header.correlation_id, 11, group_coordinator)
            }
        }),
    ])
    .await;
    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default(),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let producer = transactional_test_producer_with_retries(wire, 1);

    producer.init_transactions().await.unwrap();
    producer.begin_transaction().unwrap();
    let send_offsets = tokio::time::timeout(
        Duration::from_secs(1),
        producer.send_offsets_to_transaction(
            [(
                kacrab::producer::TopicPartition::new("orders", 0),
                kacrab::producer::OffsetAndMetadata::new(42),
            )],
            kacrab::producer::ConsumerGroupMetadata::new("group-a"),
        ),
    )
    .await
    .expect("add offsets should reload transaction coordinator before timing out");
    send_offsets.unwrap();

    assert_eq!(bootstrap.join().await, 4);
    assert_eq!(stale_transaction_coordinator.join().await, 3);
    assert_eq!(refreshed_transaction_coordinator.join().await, 2);
    assert_eq!(group_coordinator.join().await, 2);
}

#[tokio::test]
async fn kafka_producer_init_transactions_retries_retriable_coordinator_lookup() {
    let coordinator = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::InitProducerId as i16);
            init_producer_id_error_response_frame(
                header.correlation_id,
                ErrorCode::CoordinatorLoadInProgress,
            )
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::InitProducerId as i16);
            init_producer_id_response_frame(header.correlation_id, 78, 5)
        }),
    ])
    .await;
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::FindCoordinator as i16);
            find_coordinator_error_response_frame(
                header.correlation_id,
                ErrorCode::CoordinatorNotAvailable,
            )
        }),
        Box::new({
            let coordinator = coordinator.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::FindCoordinator as i16);
                find_coordinator_response_frame(header.correlation_id, 9, coordinator)
            }
        }),
    ])
    .await;

    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default(),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let producer = Producer::from_parts(
        wire,
        ProducerRuntimeConfig {
            accumulator: AccumulatorConfig::default(),
            acks: -1,
            timeout_ms: 30_000,
            retry_attempts: 1,
            retry_backoff: Duration::from_millis(100),
            retry_backoff_max: Duration::from_secs(1),
            delivery_timeout: Duration::from_mins(2),
            max_block: Duration::from_mins(1),
            partitioner_ignore_keys: false,
            partitioner_adaptive_partitioning_enable: true,
            partitioner_availability_timeout: Duration::ZERO,
            max_in_flight_requests_per_connection: 5,
            max_request_size: 1_048_576,
            enable_metrics_push: true,
            compression: ProducerCompression::default(),
            idempotence: ProducerIdempotenceConfig {
                enabled: true,
                transactional_id: Some("txn-orders".to_owned()),
                transaction_timeout_ms: 60_000,
                transaction_two_phase_commit: false,
            },
        },
    );

    producer.init_transactions().await.unwrap();

    assert_eq!(bootstrap.join().await, 3);
    assert_eq!(coordinator.join().await, 3);
}

#[tokio::test]
async fn kafka_producer_init_transactions_timeout_can_retry_same_operation_like_java() {
    let coordinator = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::InitProducerId as i16);
            std::thread::sleep(Duration::from_millis(50));
            init_producer_id_response_frame_for_version(
                header.request_api_version,
                header.correlation_id,
                78,
                5,
            )
        }),
    ])
    .await;
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new({
            let coordinator = coordinator.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::FindCoordinator as i16);
                find_coordinator_response_frame(header.correlation_id, 9, coordinator)
            }
        }),
    ])
    .await;

    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default(),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let producer = Producer::from_parts(
        wire,
        ProducerRuntimeConfig {
            accumulator: AccumulatorConfig::default(),
            acks: -1,
            timeout_ms: 30_000,
            retry_attempts: 0,
            retry_backoff: Duration::from_millis(100),
            retry_backoff_max: Duration::from_secs(1),
            delivery_timeout: Duration::from_mins(2),
            max_block: Duration::from_millis(30),
            partitioner_ignore_keys: false,
            partitioner_adaptive_partitioning_enable: true,
            partitioner_availability_timeout: Duration::ZERO,
            max_in_flight_requests_per_connection: 5,
            max_request_size: 1_048_576,
            enable_metrics_push: true,
            compression: ProducerCompression::default(),
            idempotence: ProducerIdempotenceConfig {
                enabled: true,
                transactional_id: Some("txn-orders".to_owned()),
                transaction_timeout_ms: 60_000,
                transaction_two_phase_commit: false,
            },
        },
    );

    assert!(matches!(
        producer
            .init_transactions()
            .await
            .expect_err("first init should time out while InitProducerId is still in flight"),
        kacrab::producer::ProducerError::DispatchTask(message)
            if message.contains("InitTransactions timed out")
    ));
    producer
        .init_transactions()
        .await
        .expect("retrying the same init should await cached InitProducerId result");

    assert_eq!(producer.metrics().transaction_init_count, 1);
    assert_eq!(bootstrap.join().await, 2);
    assert_eq!(coordinator.join().await, 2);
}

#[tokio::test]
async fn kafka_producer_commit_transaction_reports_end_txn_broker_error() {
    let coordinator = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::InitProducerId as i16);
            init_producer_id_response_frame(header.correlation_id, 77, 4)
        }),
        add_offsets_to_txn_ok_handler(),
        txn_offset_commit_ok_handler(),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::EndTxn as i16);
            end_txn_error_response_frame_for_request(&header, ErrorCode::InvalidTxnState)
        }),
    ])
    .await;
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new({
            let coordinator = coordinator.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::FindCoordinator as i16);
                find_coordinator_response_frame(header.correlation_id, 9, coordinator)
            }
        }),
        Box::new({
            let coordinator = coordinator.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::FindCoordinator as i16);
                find_group_coordinator_response_frame(header.correlation_id, 9, coordinator)
            }
        }),
    ])
    .await;
    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default(),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let mut producer = Producer::from_parts(
        wire,
        ProducerRuntimeConfig {
            accumulator: AccumulatorConfig::default(),
            acks: -1,
            timeout_ms: 30_000,
            retry_attempts: 0,
            retry_backoff: Duration::from_millis(100),
            retry_backoff_max: Duration::from_secs(1),
            delivery_timeout: Duration::from_mins(2),
            max_block: Duration::from_mins(1),
            partitioner_ignore_keys: false,
            partitioner_adaptive_partitioning_enable: true,
            partitioner_availability_timeout: Duration::ZERO,
            max_in_flight_requests_per_connection: 5,
            max_request_size: 1_048_576,
            enable_metrics_push: true,
            compression: ProducerCompression::default(),
            idempotence: ProducerIdempotenceConfig {
                enabled: true,
                transactional_id: Some("txn-orders".to_owned()),
                transaction_timeout_ms: 60_000,
                transaction_two_phase_commit: false,
            },
        },
    );

    producer.init_transactions().await.unwrap();
    producer.begin_transaction().unwrap();
    send_offsets_to_started_transaction(&producer).await;

    assert!(matches!(
        producer.commit_transaction().await,
        Err(kacrab::producer::ProducerError::Transaction {
            operation: "end_txn",
            error: ErrorCode::InvalidTxnState
        })
    ));
    assert_eq!(producer.metrics().transaction_commit_count, 0);

    let abort_after_fatal_commit =
        tokio::time::timeout(Duration::from_millis(200), producer.abort_transaction())
            .await
            .expect("abort after fatal commit error should fail locally");
    assert!(matches!(
        abort_after_fatal_commit,
        Err(kacrab::producer::ProducerError::Transaction {
            operation: "transaction_state",
            error: ErrorCode::InvalidTxnState
        })
    ));
    assert_eq!(producer.metrics().transaction_abort_count, 0);
    assert_eq!(bootstrap.join().await, 3);
    assert_eq!(coordinator.join().await, 5);
}

#[tokio::test]
async fn kafka_producer_abort_transaction_abortable_error_becomes_fatal_like_java() {
    let coordinator = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::InitProducerId as i16);
            init_producer_id_response_frame(header.correlation_id, 77, 4)
        }),
        add_offsets_to_txn_ok_handler(),
        txn_offset_commit_ok_handler(),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::EndTxn as i16);
            let end_txn = EndTxnRequestData::read(&mut request, header.request_api_version)
                .expect("end txn request");
            assert!(!end_txn.committed);
            end_txn_error_response_frame_for_request(&header, ErrorCode::TransactionAbortable)
        }),
    ])
    .await;
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new({
            let coordinator = coordinator.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::FindCoordinator as i16);
                find_coordinator_response_frame(header.correlation_id, 9, coordinator)
            }
        }),
        Box::new({
            let coordinator = coordinator.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::FindCoordinator as i16);
                find_group_coordinator_response_frame(header.correlation_id, 9, coordinator)
            }
        }),
    ])
    .await;
    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default(),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let mut producer = Producer::from_parts(
        wire,
        ProducerRuntimeConfig {
            accumulator: AccumulatorConfig::default(),
            acks: -1,
            timeout_ms: 30_000,
            retry_attempts: 0,
            retry_backoff: Duration::from_millis(100),
            retry_backoff_max: Duration::from_secs(1),
            delivery_timeout: Duration::from_mins(2),
            max_block: Duration::from_mins(1),
            partitioner_ignore_keys: false,
            partitioner_adaptive_partitioning_enable: true,
            partitioner_availability_timeout: Duration::ZERO,
            max_in_flight_requests_per_connection: 5,
            max_request_size: 1_048_576,
            enable_metrics_push: true,
            compression: ProducerCompression::default(),
            idempotence: ProducerIdempotenceConfig {
                enabled: true,
                transactional_id: Some("txn-orders".to_owned()),
                transaction_timeout_ms: 60_000,
                transaction_two_phase_commit: false,
            },
        },
    );

    producer.init_transactions().await.unwrap();
    producer.begin_transaction().unwrap();
    send_offsets_to_started_transaction(&producer).await;

    assert!(matches!(
        producer.abort_transaction().await,
        Err(kacrab::producer::ProducerError::Transaction {
            operation: "end_txn",
            error: ErrorCode::TransactionAbortable
        })
    ));

    let second_abort =
        tokio::time::timeout(Duration::from_millis(200), producer.abort_transaction())
            .await
            .expect("second abort should fail locally without broker IO");
    assert!(matches!(
        second_abort,
        Err(kacrab::producer::ProducerError::Transaction {
            operation: "transaction_state",
            error: ErrorCode::TransactionAbortable
        })
    ));
    assert_eq!(bootstrap.join().await, 3);
    assert_eq!(coordinator.join().await, 5);
}

#[tokio::test]
async fn kafka_producer_commit_transaction_reloads_transaction_coordinator() {
    let stale_coordinator = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::InitProducerId as i16);
            init_producer_id_response_frame(header.correlation_id, 77, 4)
        }),
        add_offsets_to_txn_ok_handler(),
        txn_offset_commit_ok_handler(),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::EndTxn as i16);
            end_txn_error_response_frame_for_request(&header, ErrorCode::NotCoordinator)
        }),
    ])
    .await;
    let refreshed_coordinator = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::EndTxn as i16);
            let end_txn = EndTxnRequestData::read(&mut request, header.request_api_version)
                .expect("end txn request");
            assert!(end_txn.committed);
            end_txn_response_frame_for_request(&header)
        }),
    ])
    .await;
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new({
            let coordinator = stale_coordinator.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::FindCoordinator as i16);
                find_coordinator_response_frame(header.correlation_id, 9, coordinator)
            }
        }),
        Box::new({
            let coordinator = stale_coordinator.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::FindCoordinator as i16);
                find_group_coordinator_response_frame(header.correlation_id, 9, coordinator)
            }
        }),
        Box::new({
            let coordinator = refreshed_coordinator.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::FindCoordinator as i16);
                find_coordinator_response_frame(header.correlation_id, 10, coordinator)
            }
        }),
    ])
    .await;
    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default(),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let mut producer = transactional_test_producer_with_retries(wire, 1);

    producer.init_transactions().await.unwrap();
    producer.begin_transaction().unwrap();
    send_offsets_to_started_transaction(&producer).await;
    tokio::time::timeout(Duration::from_secs(1), producer.commit_transaction())
        .await
        .expect("commit should reload transaction coordinator before timing out")
        .unwrap();

    assert_eq!(bootstrap.join().await, 4);
    assert_eq!(stale_coordinator.join().await, 5);
    assert_eq!(refreshed_coordinator.join().await, 2);
}

#[tokio::test]
async fn kafka_producer_commit_transaction_retries_retriable_end_txn_error() {
    let coordinator = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::InitProducerId as i16);
            init_producer_id_response_frame(header.correlation_id, 77, 4)
        }),
        add_offsets_to_txn_ok_handler(),
        txn_offset_commit_ok_handler(),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::EndTxn as i16);
            end_txn_error_response_frame(
                header.correlation_id,
                ErrorCode::CoordinatorLoadInProgress,
            )
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::EndTxn as i16);
            end_txn_response_frame_for_request(&header)
        }),
    ])
    .await;
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new({
            let coordinator = coordinator.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::FindCoordinator as i16);
                find_coordinator_response_frame(header.correlation_id, 9, coordinator)
            }
        }),
        Box::new({
            let coordinator = coordinator.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::FindCoordinator as i16);
                find_group_coordinator_response_frame(header.correlation_id, 9, coordinator)
            }
        }),
    ])
    .await;
    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default(),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let mut producer = transactional_test_producer_with_retries(wire, 1);

    producer.init_transactions().await.unwrap();
    producer.begin_transaction().unwrap();
    send_offsets_to_started_transaction(&producer).await;
    producer.commit_transaction().await.unwrap();

    assert_eq!(bootstrap.join().await, 3);
    assert_eq!(coordinator.join().await, 6);
}

#[tokio::test]
async fn kafka_producer_init_transactions_reloads_transaction_coordinator() {
    let stale_coordinator = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::InitProducerId as i16);
            init_producer_id_error_response_frame(header.correlation_id, ErrorCode::NotCoordinator)
        }),
    ])
    .await;
    let refreshed_coordinator = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::InitProducerId as i16);
            init_producer_id_response_frame(header.correlation_id, 78, 5)
        }),
    ])
    .await;
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new({
            let coordinator = stale_coordinator.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::FindCoordinator as i16);
                find_coordinator_response_frame(header.correlation_id, 9, coordinator)
            }
        }),
        Box::new({
            let coordinator = refreshed_coordinator.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::FindCoordinator as i16);
                find_coordinator_response_frame(header.correlation_id, 10, coordinator)
            }
        }),
    ])
    .await;

    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default(),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let producer = transactional_test_producer_with_retries(wire, 1);

    tokio::time::timeout(Duration::from_secs(1), producer.init_transactions())
        .await
        .expect("init_transactions should reload transaction coordinator before timing out")
        .unwrap();

    assert_eq!(bootstrap.join().await, 3);
    assert_eq!(stale_coordinator.join().await, 2);
    assert_eq!(refreshed_coordinator.join().await, 2);
}

#[tokio::test]
async fn kafka_producer_init_transactions_reports_unretriable_init_producer_error() {
    let coordinator = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::InitProducerId as i16);
            init_producer_id_error_response_frame(header.correlation_id, ErrorCode::InvalidTxnState)
        }),
    ])
    .await;
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new({
            let coordinator = coordinator.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::FindCoordinator as i16);
                find_coordinator_response_frame(header.correlation_id, 9, coordinator)
            }
        }),
    ])
    .await;
    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default(),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let producer = Producer::from_parts(
        wire,
        ProducerRuntimeConfig {
            accumulator: AccumulatorConfig::default(),
            acks: -1,
            timeout_ms: 30_000,
            retry_attempts: 0,
            retry_backoff: Duration::from_millis(100),
            retry_backoff_max: Duration::from_secs(1),
            delivery_timeout: Duration::from_mins(2),
            max_block: Duration::from_mins(1),
            partitioner_ignore_keys: false,
            partitioner_adaptive_partitioning_enable: true,
            partitioner_availability_timeout: Duration::ZERO,
            max_in_flight_requests_per_connection: 5,
            max_request_size: 1_048_576,
            enable_metrics_push: true,
            compression: ProducerCompression::default(),
            idempotence: ProducerIdempotenceConfig {
                enabled: true,
                transactional_id: Some("txn-orders".to_owned()),
                transaction_timeout_ms: 60_000,
                transaction_two_phase_commit: false,
            },
        },
    );

    assert!(matches!(
        producer.init_transactions().await,
        Err(kacrab::producer::ProducerError::Transaction {
            operation: "init_producer_id",
            error: ErrorCode::InvalidTxnState
        })
    ));
    assert_eq!(bootstrap.join().await, 2);
    assert_eq!(coordinator.join().await, 2);
}

#[tokio::test]
async fn kafka_producer_init_transactions_fatal_error_blocks_reinit() {
    let coordinator = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::InitProducerId as i16);
            init_producer_id_error_response_frame(header.correlation_id, ErrorCode::ProducerFenced)
        }),
    ])
    .await;
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new({
            let coordinator = coordinator.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::FindCoordinator as i16);
                find_coordinator_response_frame(header.correlation_id, 9, coordinator)
            }
        }),
    ])
    .await;
    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default(),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let producer = transactional_test_producer(wire);

    assert!(matches!(
        producer.init_transactions().await,
        Err(kacrab::producer::ProducerError::Transaction {
            operation: "init_producer_id",
            error: ErrorCode::ProducerFenced,
        })
    ));

    let reinit = tokio::time::timeout(Duration::from_millis(200), producer.init_transactions())
        .await
        .expect("reinit after fatal init error should fail locally");
    assert!(matches!(
        reinit,
        Err(kacrab::producer::ProducerError::Transaction {
            operation: "transaction_state",
            error: ErrorCode::ProducerFenced,
        })
    ));
    assert_eq!(bootstrap.join().await, 2);
    assert_eq!(coordinator.join().await, 2);
}

#[tokio::test]
async fn kafka_producer_init_transactions_reports_unretriable_find_coordinator_error() {
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::FindCoordinator as i16);
            find_coordinator_error_response_frame(header.correlation_id, ErrorCode::InvalidTxnState)
        }),
    ])
    .await;
    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default(),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let producer = Producer::from_parts(
        wire,
        ProducerRuntimeConfig {
            accumulator: AccumulatorConfig::default(),
            acks: -1,
            timeout_ms: 30_000,
            retry_attempts: 0,
            retry_backoff: Duration::from_millis(100),
            retry_backoff_max: Duration::from_secs(1),
            delivery_timeout: Duration::from_mins(2),
            max_block: Duration::from_mins(1),
            partitioner_ignore_keys: false,
            partitioner_adaptive_partitioning_enable: true,
            partitioner_availability_timeout: Duration::ZERO,
            max_in_flight_requests_per_connection: 5,
            max_request_size: 1_048_576,
            enable_metrics_push: true,
            compression: ProducerCompression::default(),
            idempotence: ProducerIdempotenceConfig {
                enabled: true,
                transactional_id: Some("txn-orders".to_owned()),
                transaction_timeout_ms: 60_000,
                transaction_two_phase_commit: false,
            },
        },
    );

    assert!(matches!(
        producer.init_transactions().await,
        Err(kacrab::producer::ProducerError::Transaction {
            operation: "find_coordinator",
            error: ErrorCode::InvalidTxnState
        })
    ));
    assert_eq!(bootstrap.join().await, 2);
}

#[tokio::test]
async fn kafka_producer_init_transactions_fatal_find_coordinator_error_blocks_begin_like_java() {
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::FindCoordinator as i16);
            find_coordinator_error_response_frame(
                header.correlation_id,
                ErrorCode::TransactionalIdAuthorizationFailed,
            )
        }),
    ])
    .await;
    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default(),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let producer = transactional_test_producer(wire);

    assert!(matches!(
        producer.init_transactions().await,
        Err(kacrab::producer::ProducerError::Transaction {
            operation: "find_coordinator",
            error: ErrorCode::TransactionalIdAuthorizationFailed
        })
    ));

    assert!(matches!(
        producer.begin_transaction(),
        Err(kacrab::producer::ProducerError::Transaction {
            operation: "transaction_state",
            error: ErrorCode::TransactionalIdAuthorizationFailed,
        })
    ));
    assert_eq!(bootstrap.join().await, 2);
}

#[tokio::test]
async fn kafka_producer_init_transactions_rejects_invalid_coordinator_port() {
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::FindCoordinator as i16);
            find_coordinator_invalid_port_response_frame(header.correlation_id)
        }),
    ])
    .await;
    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default(),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let producer = Producer::from_parts(
        wire,
        ProducerRuntimeConfig {
            accumulator: AccumulatorConfig::default(),
            acks: -1,
            timeout_ms: 30_000,
            retry_attempts: 0,
            retry_backoff: Duration::from_millis(100),
            retry_backoff_max: Duration::from_secs(1),
            delivery_timeout: Duration::from_mins(2),
            max_block: Duration::from_mins(1),
            partitioner_ignore_keys: false,
            partitioner_adaptive_partitioning_enable: true,
            partitioner_availability_timeout: Duration::ZERO,
            max_in_flight_requests_per_connection: 5,
            max_request_size: 1_048_576,
            enable_metrics_push: true,
            compression: ProducerCompression::default(),
            idempotence: ProducerIdempotenceConfig {
                enabled: true,
                transactional_id: Some("txn-orders".to_owned()),
                transaction_timeout_ms: 60_000,
                transaction_two_phase_commit: false,
            },
        },
    );

    assert!(matches!(
        producer.init_transactions().await,
        Err(kacrab::producer::ProducerError::InvalidTransactionState(
            "transaction coordinator returned invalid port"
        ))
    ));
    assert_eq!(bootstrap.join().await, 2);
}

#[tokio::test]
async fn dispatcher_drains_ready_batches_by_leader_broker() {
    let leader_7 = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::Produce as i16);
            let produce = ProduceRequestData::read(&mut request, header.request_api_version)
                .expect("produce request");
            assert_eq!(produce.topic_data.len(), 1);
            assert_eq!(produce.topic_data[0].topic_id, TOPIC_ID);
            assert_eq!(produce.topic_data[0].partition_data.len(), 1);
            assert_eq!(produce.topic_data[0].partition_data[0].index, 0);
            produce_response_frame_for_request(&header, 0, 40)
        }),
    ])
    .await;
    let leader_8 = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::Produce as i16);
            let produce = ProduceRequestData::read(&mut request, header.request_api_version)
                .expect("produce request");
            assert_eq!(produce.topic_data.len(), 1);
            assert_eq!(produce.topic_data[0].topic_id, TOPIC_ID);
            assert_eq!(produce.topic_data[0].partition_data.len(), 1);
            assert_eq!(produce.topic_data[0].partition_data[0].index, 1);
            produce_response_frame_for_request(&header, 1, 80)
        }),
    ])
    .await;
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new({
            let leader_7 = leader_7.addr();
            let leader_8 = leader_8.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::Metadata as i16);
                let response = metadata_response([(7, leader_7), (8, leader_8)]);
                response_frame(ApiKey::Metadata, 13, header.correlation_id, &response)
            }
        }),
    ])
    .await;

    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default().buffer_pool_capacity(16),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let dispatcher = ProducerDispatcher::new(wire.clone());
    let accumulator = SharedAccumulator::with_config(
        AccumulatorConfig::default()
            .batch_size(1)
            .buffer_memory(16 * 1024),
    );
    accumulator
        .append(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")))
        .unwrap();
    accumulator
        .append(ProducerRecord::new("orders", 1).value(Bytes::from_static(b"b")))
        .unwrap();

    let before_dispatch = wire.buffer_pool_stats();
    let mut receipts = dispatcher
        .dispatch_ready(&accumulator, Instant::now())
        .await
        .unwrap();
    receipts.sort_by_key(|receipt| receipt.partition);

    assert_eq!(receipts.len(), 2);
    assert_eq!(receipts[0].partition, 0);
    assert_eq!(receipts[0].leader_id, 7);
    assert_eq!(receipts[0].offset, 40);
    assert_eq!(receipts[1].partition, 1);
    assert_eq!(receipts[1].leader_id, 8);
    assert_eq!(receipts[1].offset, 80);
    assert_eq!(accumulator.buffered_bytes(), 0);
    assert_eq!(bootstrap.join().await, 2);
    assert_eq!(leader_7.join().await, 2);
    assert_eq!(leader_8.join().await, 2);
    let stats = wire.buffer_pool_stats();
    assert!(
        stats.write_acquired >= before_dispatch.write_acquired + 2,
        "expected dispatch to acquire at least two encoded record buffers; stats: {stats:?}, \
         before: {before_dispatch:?}"
    );
    assert!(
        stats.write_released >= before_dispatch.write_released + 2,
        "expected dispatch to release at least two encoded record buffers; stats: {stats:?}, \
         before: {before_dispatch:?}"
    );
    assert!(
        stats.write_released >= stats.write_acquired,
        "expected all acquired write buffers to be released after dispatch; stats: {stats:?}"
    );
}

#[tokio::test]
async fn dispatcher_pipelines_owned_batches_to_same_broker() {
    let leader_7 = MockBroker::serve_pipelined_produce(2).await;
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new({
            let leader_7 = leader_7.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::Metadata as i16);
                let response = metadata_response([(7, leader_7)]);
                response_frame(ApiKey::Metadata, 13, header.correlation_id, &response)
            }
        }),
    ])
    .await;

    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default()
            .max_in_flight_requests_per_connection(2)
            .broker_queue_capacity(2)
            .request_timeout(Duration::from_secs(1)),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let dispatcher = ProducerDispatcher::new(wire);
    let now = Instant::now();
    let first_batch = ready_batches_for_value(b"a", now);
    let second_batch = ready_batches_for_value(b"b", now);

    let (first, second) = tokio::join!(
        dispatcher.dispatch_ready_batches(first_batch, now),
        dispatcher.dispatch_ready_batches(second_batch, now),
    );
    let first = first.unwrap();
    let second = second.unwrap();

    assert_eq!(first.len(), 1);
    assert_eq!(first[0].partition, 0);
    assert_eq!(first[0].leader_id, 7);
    assert_eq!(first[0].offset, 40);
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].partition, 0);
    assert_eq!(second[0].leader_id, 7);
    assert_eq!(second[0].offset, 41);
    assert_eq!(bootstrap.join().await, 2);
    assert_eq!(leader_7.join().await, 3);
}

#[tokio::test]
async fn dispatcher_initializes_idempotent_producer_and_sequences_partition_batches() {
    let leader_7 = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::InitProducerId as i16);
            let init = InitProducerIdRequestData::read(&mut request, 5).expect("init producer id");
            assert_eq!(init.transactional_id, None);
            assert_eq!(init.transaction_timeout_ms, 60_000);
            init_producer_id_response_frame(header.correlation_id, 42, 3)
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::Produce as i16);
            let produce = ProduceRequestData::read(&mut request, header.request_api_version)
                .expect("produce request");
            assert_eq!(produce.transactional_id, None);
            let mut records = produce.topic_data[0].partition_data[0]
                .records
                .clone()
                .expect("records");
            let batch = RecordBatch::decode(&mut records).expect("record batch");
            assert_eq!(batch.producer_id, 42);
            assert_eq!(batch.producer_epoch, 3);
            assert_eq!(batch.base_sequence, 0);
            produce_response_frame_for_request(&header, 0, 40)
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::Produce as i16);
            let produce = ProduceRequestData::read(&mut request, header.request_api_version)
                .expect("produce request");
            let mut records = produce.topic_data[0].partition_data[0]
                .records
                .clone()
                .expect("records");
            let batch = RecordBatch::decode(&mut records).expect("record batch");
            assert_eq!(batch.producer_id, 42);
            assert_eq!(batch.producer_epoch, 3);
            assert_eq!(batch.base_sequence, 1);
            produce_response_frame_for_request(&header, 0, 41)
        }),
    ])
    .await;
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new({
            let leader_7 = leader_7.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::Metadata as i16);
                let response = metadata_response([(7, leader_7)]);
                response_frame(ApiKey::Metadata, 13, header.correlation_id, &response)
            }
        }),
    ])
    .await;

    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default(),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let dispatcher = ProducerDispatcher::with_config(
        wire,
        ProducerRuntimeConfig {
            accumulator: AccumulatorConfig::default(),
            acks: -1,
            timeout_ms: 30_000,
            retry_attempts: 0,
            retry_backoff: Duration::from_millis(100),
            retry_backoff_max: Duration::from_secs(1),
            delivery_timeout: Duration::from_mins(2),
            max_block: Duration::from_mins(1),
            partitioner_ignore_keys: false,
            partitioner_adaptive_partitioning_enable: true,
            partitioner_availability_timeout: Duration::ZERO,
            max_in_flight_requests_per_connection: 5,
            max_request_size: 1_048_576,
            enable_metrics_push: true,
            compression: ProducerCompression::default(),
            idempotence: ProducerIdempotenceConfig {
                enabled: true,
                transactional_id: None,
                transaction_timeout_ms: 60_000,
                transaction_two_phase_commit: false,
            },
        },
    );
    let now = Instant::now();

    let first = dispatcher
        .dispatch_ready_batches(ready_batches_for_value(b"a", now), now)
        .await
        .unwrap();
    let second = dispatcher
        .dispatch_ready_batches(ready_batches_for_value(b"b", now), now)
        .await
        .unwrap();

    assert_eq!(first[0].offset, 40);
    assert_eq!(second[0].offset, 41);
    assert_eq!(bootstrap.join().await, 2);
    assert_eq!(leader_7.join().await, 4);
}

#[tokio::test]
async fn dispatcher_completes_duplicate_sequence_number_as_success_like_java() {
    let leader_7 = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::InitProducerId as i16);
            let init = InitProducerIdRequestData::read(&mut request, 5).expect("init producer id");
            assert_eq!(init.producer_id, -1);
            assert_eq!(init.producer_epoch, -1);
            init_producer_id_response_frame(header.correlation_id, 42, 3)
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::Produce as i16);
            let produce = ProduceRequestData::read(&mut request, header.request_api_version)
                .expect("produce request");
            let mut records = produce.topic_data[0].partition_data[0]
                .records
                .clone()
                .expect("records");
            let batch = RecordBatch::decode(&mut records).expect("record batch");
            assert_eq!(batch.producer_id, 42);
            assert_eq!(batch.producer_epoch, 3);
            assert_eq!(batch.base_sequence, 0);
            produce_error_response_frame_for_request(&header, 0, ErrorCode::DuplicateSequenceNumber)
        }),
    ])
    .await;
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new({
            let leader_7 = leader_7.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::Metadata as i16);
                let response = metadata_response([(7, leader_7)]);
                response_frame(ApiKey::Metadata, 13, header.correlation_id, &response)
            }
        }),
    ])
    .await;
    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default(),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let dispatcher = ProducerDispatcher::with_config(
        wire,
        ProducerRuntimeConfig {
            accumulator: AccumulatorConfig::default(),
            acks: -1,
            timeout_ms: 30_000,
            retry_attempts: 0,
            retry_backoff: Duration::from_millis(100),
            retry_backoff_max: Duration::from_secs(1),
            delivery_timeout: Duration::from_mins(2),
            max_block: Duration::from_mins(1),
            partitioner_ignore_keys: false,
            partitioner_adaptive_partitioning_enable: true,
            partitioner_availability_timeout: Duration::ZERO,
            max_in_flight_requests_per_connection: 1,
            max_request_size: 1_048_576,
            enable_metrics_push: true,
            compression: ProducerCompression::default(),
            idempotence: ProducerIdempotenceConfig {
                enabled: true,
                transactional_id: None,
                transaction_timeout_ms: 60_000,
                transaction_two_phase_commit: false,
            },
        },
    );
    let now = Instant::now();

    let receipts = dispatcher
        .dispatch_ready_batches(ready_batches_for_value(b"a", now), now)
        .await
        .unwrap();

    assert_eq!(receipts.len(), 1);
    assert_eq!(receipts[0].offset, -1);
    assert_eq!(receipts[0].timestamp_ms, -1);
    assert_eq!(bootstrap.join().await, 2);
    assert_eq!(leader_7.join().await, 3);
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "Idempotent local-failure sequence recovery fixture keeps broker handlers inline."
)]
async fn dispatcher_does_not_consume_sequence_after_local_record_too_large_error_like_java() {
    let leader_7 = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::InitProducerId as i16);
            let init = InitProducerIdRequestData::read(&mut request, 5).expect("init producer id");
            assert_eq!(init.producer_id, -1);
            assert_eq!(init.producer_epoch, -1);
            init_producer_id_response_frame(header.correlation_id, 42, 3)
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::Produce as i16);
            let produce = ProduceRequestData::read(&mut request, header.request_api_version)
                .expect("produce request");
            let mut records = produce.topic_data[0].partition_data[0]
                .records
                .clone()
                .expect("records");
            let batch = RecordBatch::decode(&mut records).expect("record batch");
            assert_eq!(batch.producer_id, 42);
            assert_eq!(batch.producer_epoch, 3);
            assert_eq!(batch.base_sequence, 0);
            produce_response_frame_for_request(&header, 0, 40)
        }),
    ])
    .await;
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new({
            let leader_7 = leader_7.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::Metadata as i16);
                let response = metadata_response([(7, leader_7)]);
                response_frame(ApiKey::Metadata, 13, header.correlation_id, &response)
            }
        }),
    ])
    .await;
    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default(),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let dispatcher = ProducerDispatcher::with_config(
        wire,
        ProducerRuntimeConfig {
            accumulator: AccumulatorConfig::default(),
            acks: -1,
            timeout_ms: 30_000,
            retry_attempts: 0,
            retry_backoff: Duration::from_millis(100),
            retry_backoff_max: Duration::from_secs(1),
            delivery_timeout: Duration::from_mins(2),
            max_block: Duration::from_mins(1),
            partitioner_ignore_keys: false,
            partitioner_adaptive_partitioning_enable: true,
            partitioner_availability_timeout: Duration::ZERO,
            max_in_flight_requests_per_connection: 1,
            max_request_size: 220,
            enable_metrics_push: true,
            compression: ProducerCompression::default(),
            idempotence: ProducerIdempotenceConfig {
                enabled: true,
                transactional_id: None,
                transaction_timeout_ms: 60_000,
                transaction_two_phase_commit: false,
            },
        },
    );
    let now = Instant::now();
    let accumulator = SharedAccumulator::with_config(
        AccumulatorConfig::default()
            .batch_size(1)
            .buffer_memory(16 * 1024),
    );
    accumulator
        .append_at(
            ProducerRecord::new("orders", 0).value(Bytes::from(vec![b'a'; 1024])),
            now,
        )
        .expect("append oversize local batch");

    let first_error = dispatcher
        .dispatch_ready_batches(accumulator.drain_ready(now), now)
        .await
        .expect_err("oversize batch should fail before produce send");
    assert!(matches!(
        first_error,
        kacrab::producer::ProducerError::RecordTooLarge { .. }
    ));

    let receipts = dispatcher
        .dispatch_ready_batches(
            ready_batches_for_value(b"b", Instant::now()),
            Instant::now(),
        )
        .await
        .unwrap();

    assert_eq!(receipts[0].offset, 40);
    assert_eq!(bootstrap.join().await, 2);
    assert_eq!(leader_7.join().await, 3);
}

#[tokio::test]
async fn dispatcher_releases_encoded_buffers_after_later_local_record_too_large_error() {
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::Metadata as i16);
            let leader: std::net::SocketAddr = "127.0.0.1:9".parse().expect("leader addr");
            let response = metadata_response([(7, leader)]);
            response_frame(ApiKey::Metadata, 13, header.correlation_id, &response)
        }),
    ])
    .await;
    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default().buffer_pool_capacity(8),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let dispatcher = ProducerDispatcher::with_config(
        wire.clone(),
        ProducerRuntimeConfig {
            accumulator: AccumulatorConfig::default(),
            acks: 1,
            timeout_ms: 30_000,
            retry_attempts: 0,
            retry_backoff: Duration::from_millis(100),
            retry_backoff_max: Duration::from_secs(1),
            delivery_timeout: Duration::from_mins(2),
            max_block: Duration::from_mins(1),
            partitioner_ignore_keys: false,
            partitioner_adaptive_partitioning_enable: true,
            partitioner_availability_timeout: Duration::ZERO,
            max_in_flight_requests_per_connection: 1,
            max_request_size: 220,
            enable_metrics_push: true,
            compression: ProducerCompression::default(),
            idempotence: ProducerIdempotenceConfig {
                enabled: false,
                transactional_id: None,
                transaction_timeout_ms: 60_000,
                transaction_two_phase_commit: false,
            },
        },
    );
    let now = Instant::now();
    let accumulator = SharedAccumulator::with_config(
        AccumulatorConfig::default()
            .batch_size(1)
            .buffer_memory(16 * 1024),
    );
    accumulator
        .append_at(
            ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")),
            now,
        )
        .expect("append first batch");
    accumulator
        .append_at(
            ProducerRecord::new("orders", 0).value(Bytes::from(vec![b'b'; 1024])),
            now,
        )
        .expect("append oversize second batch");
    let before_dispatch = wire.buffer_pool_stats();

    let error = dispatcher
        .dispatch_ready_batches(accumulator.drain_ready(now), now)
        .await
        .expect_err("second batch should fail before any Produce request is sent");

    assert!(matches!(
        error,
        kacrab::producer::ProducerError::RecordTooLarge { .. }
    ));
    let after_dispatch = wire.buffer_pool_stats();
    assert!(
        after_dispatch.write_acquired >= before_dispatch.write_acquired + 2,
        "expected both batches to be encoded before local sizing failure; before: \
         {before_dispatch:?}, after: {after_dispatch:?}"
    );
    assert_eq!(
        after_dispatch.write_released, after_dispatch.write_acquired,
        "encoded buffers must be returned to the write pool after local dispatch failure"
    );
    assert_eq!(bootstrap.join().await, 2);
}

#[tokio::test]
async fn dispatcher_splits_and_requeues_message_too_large_multi_record_batch_like_java() {
    let leader_7 = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::Produce as i16);
            let produce = ProduceRequestData::read(&mut request, header.request_api_version)
                .expect("produce request");
            let mut records = produce.topic_data[0].partition_data[0]
                .records
                .clone()
                .expect("records");
            let batch = RecordBatch::decode(&mut records).expect("record batch");
            assert_eq!(batch.records.len(), 2);
            produce_error_response_frame_for_request(&header, 0, ErrorCode::MessageTooLarge)
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::Produce as i16);
            let produce = ProduceRequestData::read(&mut request, header.request_api_version)
                .expect("produce request");
            let mut records = produce.topic_data[0].partition_data[0]
                .records
                .clone()
                .expect("records");
            let batch = RecordBatch::decode(&mut records).expect("record batch");
            assert_eq!(batch.records.len(), 2);
            produce_response_frame_for_request(&header, 0, 40)
        }),
    ])
    .await;
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new({
            let leader_7 = leader_7.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::Metadata as i16);
                let response = metadata_response([(7, leader_7)]);
                response_frame(ApiKey::Metadata, 13, header.correlation_id, &response)
            }
        }),
    ])
    .await;
    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default(),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let dispatcher = ProducerDispatcher::new(wire);
    let now = Instant::now();
    let accumulator = SharedAccumulator::with_config(
        AccumulatorConfig::default()
            .batch_size(16 * 1024)
            .linger(Duration::ZERO)
            .buffer_memory(16 * 1024),
    );
    for value in [b"a".as_slice(), b"b".as_slice()] {
        accumulator
            .append_at(
                ProducerRecord::new("orders", 0).value(Bytes::copy_from_slice(value)),
                now,
            )
            .expect("append record");
    }

    let first = dispatcher.dispatch_ready(&accumulator, now).await.unwrap();
    let second = dispatcher.dispatch_ready(&accumulator, now).await.unwrap();

    assert!(first.is_empty());
    assert_eq!(second.len(), 2);
    assert_eq!(second[0].offset, 40);
    assert_eq!(second[1].offset, 41);
    assert_eq!(bootstrap.join().await, 2);
    assert_eq!(leader_7.join().await, 3);
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "Idempotent UnknownProducerId epoch-bump fixture keeps ordered broker handlers \
              inline."
)]
async fn dispatcher_bumps_epoch_and_retries_unknown_producer_id() {
    let leader_7 = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::InitProducerId as i16);
            let init = InitProducerIdRequestData::read(&mut request, 5).expect("init producer id");
            assert_eq!(init.producer_id, -1);
            assert_eq!(init.producer_epoch, -1);
            init_producer_id_response_frame(header.correlation_id, 42, 3)
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::Produce as i16);
            let produce = ProduceRequestData::read(&mut request, header.request_api_version)
                .expect("produce request");
            let mut records = produce.topic_data[0].partition_data[0]
                .records
                .clone()
                .expect("records");
            let batch = RecordBatch::decode(&mut records).expect("record batch");
            assert_eq!(batch.producer_id, 42);
            assert_eq!(batch.producer_epoch, 3);
            assert_eq!(batch.base_sequence, 0);
            produce_error_response_frame_with_log_start_offset_for_request(
                &header,
                0,
                ErrorCode::UnknownProducerId,
                0,
            )
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::InitProducerId as i16);
            let init = InitProducerIdRequestData::read(&mut request, 5).expect("init producer id");
            assert_eq!(init.producer_id, 42);
            assert_eq!(init.producer_epoch, 3);
            init_producer_id_response_frame(header.correlation_id, 42, 4)
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::Produce as i16);
            let produce = ProduceRequestData::read(&mut request, header.request_api_version)
                .expect("produce request");
            let mut records = produce.topic_data[0].partition_data[0]
                .records
                .clone()
                .expect("records");
            let batch = RecordBatch::decode(&mut records).expect("record batch");
            assert_eq!(batch.producer_id, 42);
            assert_eq!(batch.producer_epoch, 4);
            assert_eq!(batch.base_sequence, 0);
            produce_response_frame_for_request(&header, 0, 40)
        }),
    ])
    .await;
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new({
            let leader_7 = leader_7.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::Metadata as i16);
                let response = metadata_response([(7, leader_7)]);
                response_frame(ApiKey::Metadata, 13, header.correlation_id, &response)
            }
        }),
    ])
    .await;
    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default(),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let dispatcher = ProducerDispatcher::with_config(
        wire,
        ProducerRuntimeConfig {
            accumulator: AccumulatorConfig::default(),
            acks: -1,
            timeout_ms: 30_000,
            retry_attempts: 1,
            retry_backoff: Duration::from_millis(100),
            retry_backoff_max: Duration::from_secs(1),
            delivery_timeout: Duration::from_mins(2),
            max_block: Duration::from_mins(1),
            partitioner_ignore_keys: false,
            partitioner_adaptive_partitioning_enable: true,
            partitioner_availability_timeout: Duration::ZERO,
            max_in_flight_requests_per_connection: 1,
            max_request_size: 1_048_576,
            enable_metrics_push: true,
            compression: ProducerCompression::default(),
            idempotence: ProducerIdempotenceConfig {
                enabled: true,
                transactional_id: None,
                transaction_timeout_ms: 60_000,
                transaction_two_phase_commit: false,
            },
        },
    );
    let now = Instant::now();

    let receipts = dispatcher
        .dispatch_ready_batches(ready_batches_for_value(b"a", now), now)
        .await
        .unwrap();

    assert_eq!(receipts[0].offset, 40);
    assert_eq!(bootstrap.join().await, 2);
    assert_eq!(leader_7.join().await, 5);
}

#[tokio::test]
async fn dispatcher_retries_unknown_producer_id_without_epoch_bump_when_log_start_unknown_like_java()
 {
    let leader_7 = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::InitProducerId as i16);
            let init = InitProducerIdRequestData::read(&mut request, 5).expect("init producer id");
            assert_eq!(init.producer_id, -1);
            assert_eq!(init.producer_epoch, -1);
            init_producer_id_response_frame(header.correlation_id, 42, 3)
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::Produce as i16);
            let produce = ProduceRequestData::read(&mut request, header.request_api_version)
                .expect("produce request");
            let mut records = produce.topic_data[0].partition_data[0]
                .records
                .clone()
                .expect("records");
            let batch = RecordBatch::decode(&mut records).expect("record batch");
            assert_eq!(batch.producer_id, 42);
            assert_eq!(batch.producer_epoch, 3);
            assert_eq!(batch.base_sequence, 0);
            produce_error_response_frame_with_log_start_offset_for_request(
                &header,
                0,
                ErrorCode::UnknownProducerId,
                -1,
            )
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::Produce as i16);
            let produce = ProduceRequestData::read(&mut request, header.request_api_version)
                .expect("produce request");
            let mut records = produce.topic_data[0].partition_data[0]
                .records
                .clone()
                .expect("records");
            let batch = RecordBatch::decode(&mut records).expect("record batch");
            assert_eq!(batch.producer_id, 42);
            assert_eq!(batch.producer_epoch, 3);
            assert_eq!(batch.base_sequence, 0);
            produce_response_frame_for_request(&header, 0, 40)
        }),
    ])
    .await;
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new({
            let leader_7 = leader_7.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::Metadata as i16);
                let response = metadata_response([(7, leader_7)]);
                response_frame(ApiKey::Metadata, 13, header.correlation_id, &response)
            }
        }),
    ])
    .await;
    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default(),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let dispatcher = ProducerDispatcher::with_config(
        wire,
        ProducerRuntimeConfig {
            accumulator: AccumulatorConfig::default(),
            acks: -1,
            timeout_ms: 30_000,
            retry_attempts: 1,
            retry_backoff: Duration::from_millis(100),
            retry_backoff_max: Duration::from_secs(1),
            delivery_timeout: Duration::from_mins(2),
            max_block: Duration::from_mins(1),
            partitioner_ignore_keys: false,
            partitioner_adaptive_partitioning_enable: true,
            partitioner_availability_timeout: Duration::ZERO,
            max_in_flight_requests_per_connection: 1,
            max_request_size: 1_048_576,
            enable_metrics_push: true,
            compression: ProducerCompression::default(),
            idempotence: ProducerIdempotenceConfig {
                enabled: true,
                transactional_id: None,
                transaction_timeout_ms: 60_000,
                transaction_two_phase_commit: false,
            },
        },
    );
    let now = Instant::now();

    let receipts = dispatcher
        .dispatch_ready_batches(ready_batches_for_value(b"a", now), now)
        .await
        .unwrap();

    assert_eq!(receipts[0].offset, 40);
    assert_eq!(bootstrap.join().await, 2);
    assert_eq!(leader_7.join().await, 4);
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "Idempotent sequence recovery fixture keeps ordered broker handlers inline."
)]
async fn dispatcher_releases_sequence_after_leadership_retry_timeout_like_java() {
    let leader_7 = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::InitProducerId as i16);
            init_producer_id_response_frame(header.correlation_id, 42, 3)
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::Produce as i16);
            let produce = ProduceRequestData::read(&mut request, header.request_api_version)
                .expect("produce request");
            let mut records = produce.topic_data[0].partition_data[0]
                .records
                .clone()
                .expect("records");
            let batch = RecordBatch::decode(&mut records).expect("record batch");
            assert_eq!(batch.producer_id, 42);
            assert_eq!(batch.producer_epoch, 3);
            assert_eq!(batch.base_sequence, 0);
            produce_error_response_frame_for_request(&header, 0, ErrorCode::NotLeaderOrFollower)
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::Produce as i16);
            let produce = ProduceRequestData::read(&mut request, header.request_api_version)
                .expect("produce request");
            let mut records = produce.topic_data[0].partition_data[0]
                .records
                .clone()
                .expect("records");
            let batch = RecordBatch::decode(&mut records).expect("record batch");
            assert_eq!(batch.producer_id, 42);
            assert_eq!(batch.producer_epoch, 3);
            assert_eq!(batch.base_sequence, 0);
            produce_response_frame_for_request(&header, 0, 41)
        }),
    ])
    .await;
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new({
            let leader_7 = leader_7.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::Metadata as i16);
                let response = metadata_response([(7, leader_7)]);
                response_frame(ApiKey::Metadata, 13, header.correlation_id, &response)
            }
        }),
        Box::new({
            let leader_7 = leader_7.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::Metadata as i16);
                let response = metadata_response([(7, leader_7)]);
                response_frame(ApiKey::Metadata, 13, header.correlation_id, &response)
            }
        }),
    ])
    .await;
    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default(),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let dispatcher = ProducerDispatcher::with_config(
        wire,
        ProducerRuntimeConfig {
            accumulator: AccumulatorConfig::default(),
            acks: -1,
            timeout_ms: 30_000,
            retry_attempts: 1,
            retry_backoff: Duration::from_millis(100),
            retry_backoff_max: Duration::from_secs(1),
            delivery_timeout: Duration::from_millis(1),
            max_block: Duration::from_mins(1),
            partitioner_ignore_keys: false,
            partitioner_adaptive_partitioning_enable: true,
            partitioner_availability_timeout: Duration::ZERO,
            max_in_flight_requests_per_connection: 1,
            max_request_size: 1_048_576,
            enable_metrics_push: true,
            compression: ProducerCompression::default(),
            idempotence: ProducerIdempotenceConfig {
                enabled: true,
                transactional_id: None,
                transaction_timeout_ms: 60_000,
                transaction_two_phase_commit: false,
            },
        },
    );
    let now = Instant::now();
    let first_error = dispatcher
        .dispatch_ready_batches(ready_batches_for_value(b"a", now), now)
        .await
        .expect_err("retry wait should hit delivery timeout");
    assert!(matches!(
        first_error,
        kacrab::producer::ProducerError::DeliveryTimeout { .. }
    ));

    let receipts = dispatcher
        .dispatch_ready_batches(
            ready_batches_for_value(b"b", Instant::now()),
            Instant::now(),
        )
        .await
        .unwrap();

    assert_eq!(receipts[0].offset, 41);
    assert_eq!(bootstrap.join().await, 3);
    assert_eq!(leader_7.join().await, 4);
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "Idempotent unresolved sequence timeout fixture keeps ordered broker handlers inline."
)]
async fn dispatcher_recovers_unknown_producer_id_timeout_with_unknown_log_start_like_java() {
    let leader_7 = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::InitProducerId as i16);
            let init = InitProducerIdRequestData::read(&mut request, 5).expect("init producer id");
            assert_eq!(init.producer_id, -1);
            assert_eq!(init.producer_epoch, -1);
            init_producer_id_response_frame(header.correlation_id, 42, 3)
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::Produce as i16);
            let produce = ProduceRequestData::read(&mut request, header.request_api_version)
                .expect("produce request");
            let mut records = produce.topic_data[0].partition_data[0]
                .records
                .clone()
                .expect("records");
            let batch = RecordBatch::decode(&mut records).expect("record batch");
            assert_eq!(batch.producer_id, 42);
            assert_eq!(batch.producer_epoch, 3);
            assert_eq!(batch.base_sequence, 0);
            produce_error_response_frame_with_log_start_offset_for_request(
                &header,
                0,
                ErrorCode::UnknownProducerId,
                -1,
            )
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::InitProducerId as i16);
            let init = InitProducerIdRequestData::read(&mut request, 5).expect("init producer id");
            assert_eq!(init.producer_id, 42);
            assert_eq!(init.producer_epoch, 3);
            init_producer_id_response_frame(header.correlation_id, 42, 4)
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::Produce as i16);
            let produce = ProduceRequestData::read(&mut request, header.request_api_version)
                .expect("produce request");
            let mut records = produce.topic_data[0].partition_data[0]
                .records
                .clone()
                .expect("records");
            let batch = RecordBatch::decode(&mut records).expect("record batch");
            assert_eq!(batch.producer_id, 42);
            assert_eq!(batch.producer_epoch, 4);
            assert_eq!(batch.base_sequence, 0);
            produce_response_frame_for_request(&header, 0, 41)
        }),
    ])
    .await;
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new({
            let leader_7 = leader_7.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::Metadata as i16);
                let response = metadata_response([(7, leader_7)]);
                response_frame(ApiKey::Metadata, 13, header.correlation_id, &response)
            }
        }),
    ])
    .await;
    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default(),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let dispatcher = ProducerDispatcher::with_config(
        wire,
        ProducerRuntimeConfig {
            accumulator: AccumulatorConfig::default(),
            acks: -1,
            timeout_ms: 30_000,
            retry_attempts: 1,
            retry_backoff: Duration::from_millis(100),
            retry_backoff_max: Duration::from_secs(1),
            delivery_timeout: Duration::from_millis(1),
            max_block: Duration::from_mins(1),
            partitioner_ignore_keys: false,
            partitioner_adaptive_partitioning_enable: true,
            partitioner_availability_timeout: Duration::ZERO,
            max_in_flight_requests_per_connection: 1,
            max_request_size: 1_048_576,
            enable_metrics_push: true,
            compression: ProducerCompression::default(),
            idempotence: ProducerIdempotenceConfig {
                enabled: true,
                transactional_id: None,
                transaction_timeout_ms: 60_000,
                transaction_two_phase_commit: false,
            },
        },
    );
    let now = Instant::now();
    let first_error = dispatcher
        .dispatch_ready_batches(ready_batches_for_value(b"a", now), now)
        .await
        .expect_err("retry wait should hit delivery timeout");
    assert!(matches!(
        first_error,
        kacrab::producer::ProducerError::DeliveryTimeout { .. }
    ));

    let receipts = dispatcher
        .dispatch_ready_batches(
            ready_batches_for_value(b"b", Instant::now()),
            Instant::now(),
        )
        .await
        .unwrap();

    assert_eq!(receipts[0].offset, 41);
    assert_eq!(bootstrap.join().await, 2);
    assert_eq!(leader_7.join().await, 5);
}

#[tokio::test]
#[cfg(feature = "lz4")]
async fn dispatcher_sends_compressed_record_batches_from_runtime_config() {
    let leader_7 = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::Produce as i16);
            let produce = ProduceRequestData::read(&mut request, header.request_api_version)
                .expect("produce request");
            let mut records = produce.topic_data[0].partition_data[0]
                .records
                .clone()
                .expect("produce records");
            let decoded = RecordBatch::decode(&mut records).expect("compressed record batch");
            assert_eq!(
                decoded.compression().expect("compression"),
                Compression::Lz4
            );
            let record = decoded.records.first().expect("record");
            assert_eq!(record.value, Some(Bytes::from_static(b"compressed-value")));
            produce_response_frame_for_request(&header, 0, 40)
        }),
    ])
    .await;
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new({
            let leader_7 = leader_7.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::Metadata as i16);
                let response = metadata_response([(7, leader_7)]);
                response_frame(ApiKey::Metadata, 13, header.correlation_id, &response)
            }
        }),
    ])
    .await;

    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default(),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let dispatcher = ProducerDispatcher::with_config(
        wire,
        ProducerRuntimeConfig {
            accumulator: AccumulatorConfig::default(),
            acks: 1,
            timeout_ms: 30_000,
            retry_attempts: 0,
            retry_backoff: Duration::from_millis(100),
            retry_backoff_max: Duration::from_secs(1),
            delivery_timeout: Duration::from_mins(2),
            max_block: Duration::from_mins(1),
            partitioner_ignore_keys: false,
            partitioner_adaptive_partitioning_enable: true,
            partitioner_availability_timeout: Duration::ZERO,
            max_in_flight_requests_per_connection: 5,
            max_request_size: 1_048_576,
            enable_metrics_push: true,
            compression: ProducerCompression {
                codec: Compression::Lz4,
                level: Some(9),
            },
            idempotence: idempotence_disabled(),
        },
    );
    let accumulator = SharedAccumulator::with_config(
        AccumulatorConfig::default()
            .batch_size(1)
            .buffer_memory(16 * 1024),
    );
    accumulator
        .append(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"compressed-value")))
        .unwrap();

    let receipts = dispatcher
        .dispatch_ready(&accumulator, Instant::now())
        .await
        .unwrap();

    assert_eq!(receipts.len(), 1);
    assert_eq!(receipts[0].offset, 40);
    assert_eq!(bootstrap.join().await, 2);
    assert_eq!(leader_7.join().await, 2);
}

#[tokio::test]
async fn dispatcher_invalidates_metadata_on_leadership_error() {
    let leader_7 = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::Produce as i16);
            produce_error_response_frame_for_request(&header, 0, ErrorCode::NotLeaderOrFollower)
        }),
    ])
    .await;
    let leader_8 = MockBroker::serve_many(vec![Box::new(api_versions_response_frame)]).await;
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new({
            let leader_7 = leader_7.addr();
            let leader_8 = leader_8.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::Metadata as i16);
                let response = metadata_response([(7, leader_7), (8, leader_8)]);
                response_frame(ApiKey::Metadata, 13, header.correlation_id, &response)
            }
        }),
        Box::new({
            let leader_8 = leader_8.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::Metadata as i16);
                let response = moved_metadata_response(8, leader_8);
                response_frame(ApiKey::Metadata, 13, header.correlation_id, &response)
            }
        }),
    ])
    .await;

    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default(),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let dispatcher = ProducerDispatcher::new(wire.clone());
    let accumulator = SharedAccumulator::with_config(
        AccumulatorConfig::default()
            .batch_size(1)
            .buffer_memory(16 * 1024),
    );
    accumulator
        .append(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")))
        .unwrap();

    let error = dispatcher
        .dispatch_ready(&accumulator, Instant::now())
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        kacrab::producer::ProducerError::Broker {
            error: ErrorCode::NotLeaderOrFollower,
            ..
        }
    ));

    let metadata = wire.metadata_for_topics(["orders"]).await.unwrap();

    assert_eq!(
        metadata
            .leader_for("orders", 0)
            .map(|broker| broker.node_id),
        Some(8)
    );
    assert_eq!(bootstrap.join().await, 3);
    assert_eq!(leader_7.join().await, 2);
}

#[tokio::test]
async fn dispatcher_retries_leadership_error_after_metadata_refresh() {
    let leader_7 = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::Produce as i16);
            produce_error_response_frame_for_request(&header, 0, ErrorCode::NotLeaderOrFollower)
        }),
    ])
    .await;
    let leader_8 = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::Produce as i16);
            let produce = ProduceRequestData::read(&mut request, header.request_api_version)
                .expect("produce request");
            assert_eq!(produce.topic_data.len(), 1);
            assert_eq!(produce.topic_data[0].partition_data.len(), 1);
            assert_eq!(produce.topic_data[0].partition_data[0].index, 0);
            produce_response_frame_for_request(&header, 0, 88)
        }),
    ])
    .await;
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new({
            let leader_7 = leader_7.addr();
            let leader_8 = leader_8.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::Metadata as i16);
                let response = metadata_response([(7, leader_7), (8, leader_8)]);
                response_frame(ApiKey::Metadata, 13, header.correlation_id, &response)
            }
        }),
        Box::new({
            let leader_8 = leader_8.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::Metadata as i16);
                let response = moved_metadata_response(8, leader_8);
                response_frame(ApiKey::Metadata, 13, header.correlation_id, &response)
            }
        }),
    ])
    .await;
    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default(),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let dispatcher = ProducerDispatcher::new(wire).retry_attempts(1);
    dispatcher.enable_metrics();
    let accumulator = SharedAccumulator::with_config(
        AccumulatorConfig::default()
            .batch_size(1)
            .buffer_memory(16 * 1024),
    );
    accumulator
        .append(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")))
        .unwrap();

    let receipts = dispatcher
        .dispatch_ready(&accumulator, Instant::now())
        .await
        .unwrap();
    let metrics = dispatcher.metrics();

    assert_eq!(receipts.len(), 1);
    assert_eq!(receipts[0].leader_id, 8);
    assert_eq!(receipts[0].offset, 88);
    assert_eq!(metrics.produce_request_count, 2);
    assert_eq!(metrics.produce_retry_count, 1);
    assert_eq!(metrics.produce_error_count, 1);
    assert_eq!(bootstrap.join().await, 3);
    assert_eq!(leader_7.join().await, 2);
    assert_eq!(leader_8.join().await, 2);
}

#[tokio::test]
async fn dispatcher_requeues_batch_when_metadata_is_missing() {
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::Metadata as i16);
            response_frame(
                ApiKey::Metadata,
                13,
                header.correlation_id,
                &empty_metadata_response(),
            )
        }),
    ])
    .await;
    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default(),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let dispatcher = ProducerDispatcher::new(wire);
    dispatcher.enable_metrics();
    let accumulator = SharedAccumulator::with_config(
        AccumulatorConfig::default()
            .batch_size(1)
            .buffer_memory(16 * 1024),
    );
    accumulator
        .append(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")))
        .unwrap();

    let receipts = dispatcher
        .dispatch_ready(&accumulator, Instant::now())
        .await
        .unwrap();
    let metrics = dispatcher.metrics();

    assert!(receipts.is_empty());
    assert!(accumulator.buffered_bytes() > 0);
    assert_eq!(metrics.requeue_count, 1);
    assert_eq!(bootstrap.join().await, 2);
}

#[tokio::test]
async fn dispatcher_owned_batches_reports_flush_incomplete_when_metadata_is_missing() {
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::Metadata as i16);
            response_frame(
                ApiKey::Metadata,
                13,
                header.correlation_id,
                &empty_metadata_response(),
            )
        }),
    ])
    .await;
    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default(),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let dispatcher = ProducerDispatcher::new(wire);
    let error = dispatcher
        .dispatch_ready_batches(
            ready_batches_for_value(b"a", Instant::now()),
            Instant::now(),
        )
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        kacrab::producer::ProducerError::FlushIncomplete
    ));
    assert_eq!(bootstrap.join().await, 2);
}

#[tokio::test]
async fn dispatcher_dispatch_all_requeues_and_reports_flush_incomplete() {
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::Metadata as i16);
            response_frame(
                ApiKey::Metadata,
                13,
                header.correlation_id,
                &empty_metadata_response(),
            )
        }),
    ])
    .await;
    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default(),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let dispatcher = ProducerDispatcher::new(wire);
    let accumulator = SharedAccumulator::with_config(
        AccumulatorConfig::default()
            .batch_size(16 * 1024)
            .buffer_memory(16 * 1024),
    );
    accumulator
        .append(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")))
        .unwrap();

    let error = dispatcher.dispatch_all(&accumulator).await.unwrap_err();

    assert!(matches!(
        error,
        kacrab::producer::ProducerError::FlushIncomplete
    ));
    assert!(accumulator.buffered_bytes() > 0);
    assert_eq!(bootstrap.join().await, 2);
}

#[tokio::test]
async fn kafka_producer_delivery_future_receives_terminal_broker_error_like_java() {
    let leader_7 = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::Produce as i16);
            produce_error_response_frame_for_request(&header, 0, ErrorCode::NotLeaderOrFollower)
        }),
    ])
    .await;
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new({
            let leader_7 = leader_7.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::Metadata as i16);
                let response = metadata_response([(7, leader_7)]);
                response_frame(ApiKey::Metadata, 13, header.correlation_id, &response)
            }
        }),
    ])
    .await;
    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default(),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let mut producer = Producer::from_parts(
        wire,
        ProducerRuntimeConfig {
            accumulator: AccumulatorConfig::default()
                .batch_size(1)
                .buffer_memory(16 * 1024),
            acks: 1,
            timeout_ms: 30_000,
            retry_attempts: 0,
            retry_backoff: Duration::from_millis(100),
            retry_backoff_max: Duration::from_secs(1),
            delivery_timeout: Duration::from_mins(2),
            max_block: Duration::from_mins(1),
            partitioner_ignore_keys: false,
            partitioner_adaptive_partitioning_enable: true,
            partitioner_availability_timeout: Duration::ZERO,
            max_in_flight_requests_per_connection: 1,
            max_request_size: 1_048_576,
            enable_metrics_push: true,
            compression: ProducerCompression::default(),
            idempotence: idempotence_disabled(),
        },
    );

    let delivery = producer
        .send(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")))
        .expect("send should append before broker response");
    let flush_error = producer
        .flush()
        .await
        .expect_err("broker error should fail flush");
    let delivery_error = delivery
        .await
        .expect_err("delivery should receive the broker error, not DeliveryDropped");

    assert!(matches!(
        flush_error,
        kacrab::producer::ProducerError::Broker {
            topic,
            partition: 0,
            error: ErrorCode::NotLeaderOrFollower,
        } if topic == "orders"
    ));
    assert!(matches!(
        delivery_error,
        kacrab::producer::ProducerError::Broker {
            topic,
            partition: 0,
            error: ErrorCode::NotLeaderOrFollower,
        } if topic == "orders"
    ));
    assert_eq!(producer.buffered_bytes(), 0);
    assert_eq!(bootstrap.join().await, 2);
    assert_eq!(leader_7.join().await, 2);
}

#[tokio::test]
async fn kafka_producer_send_with_callback_receives_terminal_broker_error_like_java() {
    let leader_7 = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::Produce as i16);
            produce_error_response_frame_for_request(&header, 0, ErrorCode::NotLeaderOrFollower)
        }),
    ])
    .await;
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new({
            let leader_7 = leader_7.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::Metadata as i16);
                let response = metadata_response([(7, leader_7)]);
                response_frame(ApiKey::Metadata, 13, header.correlation_id, &response)
            }
        }),
    ])
    .await;
    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default(),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let mut producer = Producer::from_parts(
        wire,
        ProducerRuntimeConfig {
            accumulator: AccumulatorConfig::default()
                .batch_size(1)
                .buffer_memory(16 * 1024),
            acks: 1,
            timeout_ms: 30_000,
            retry_attempts: 0,
            retry_backoff: Duration::from_millis(100),
            retry_backoff_max: Duration::from_secs(1),
            delivery_timeout: Duration::from_mins(2),
            max_block: Duration::from_mins(1),
            partitioner_ignore_keys: false,
            partitioner_adaptive_partitioning_enable: true,
            partitioner_availability_timeout: Duration::ZERO,
            max_in_flight_requests_per_connection: 1,
            max_request_size: 1_048_576,
            enable_metrics_push: true,
            compression: ProducerCompression::default(),
            idempotence: idempotence_disabled(),
        },
    );
    let callback_errors = Arc::new(Mutex::new(Vec::new()));
    let callback_sink = Arc::clone(&callback_errors);

    let delivery = producer
        .send_with_callback(
            ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")),
            move |result| match result {
                Err(kacrab::producer::ProducerError::Broker {
                    topic,
                    partition: 0,
                    error: ErrorCode::NotLeaderOrFollower,
                }) if topic == "orders" => callback_sink
                    .lock()
                    .expect("callback errors")
                    .push(ErrorCode::NotLeaderOrFollower),
                other => panic!("callback should receive broker error, got {other:?}"),
            },
        )
        .expect("send should append before broker response");
    let flush_error = producer
        .flush()
        .await
        .expect_err("broker error should fail flush");
    let delivery_error = delivery
        .await
        .expect_err("delivery should receive the broker error, not DeliveryDropped");
    let callback_errors = callback_errors.lock().expect("callback errors").clone();

    assert!(matches!(
        flush_error,
        kacrab::producer::ProducerError::Broker {
            topic,
            partition: 0,
            error: ErrorCode::NotLeaderOrFollower,
        } if topic == "orders"
    ));
    assert!(matches!(
        delivery_error,
        kacrab::producer::ProducerError::Broker {
            topic,
            partition: 0,
            error: ErrorCode::NotLeaderOrFollower,
        } if topic == "orders"
    ));
    assert_eq!(callback_errors, vec![ErrorCode::NotLeaderOrFollower]);
    assert_eq!(producer.buffered_bytes(), 0);
    assert_eq!(bootstrap.join().await, 2);
    assert_eq!(leader_7.join().await, 2);
}

#[tokio::test]
async fn kafka_producer_delivery_future_preserves_terminal_wire_connection_closed_error() {
    let leader_7 = MockBroker::serve_many(vec![Box::new(api_versions_response_frame)]).await;
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new({
            let leader_7 = leader_7.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::Metadata as i16);
                let response = metadata_response([(7, leader_7)]);
                response_frame(ApiKey::Metadata, 13, header.correlation_id, &response)
            }
        }),
    ])
    .await;
    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default()
            .request_timeout(Duration::from_millis(100))
            .reconnect_backoff_initial(Duration::from_secs(30)),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let mut producer = Producer::from_parts(
        wire,
        ProducerRuntimeConfig {
            accumulator: AccumulatorConfig::default()
                .batch_size(1)
                .buffer_memory(16 * 1024),
            acks: 1,
            timeout_ms: 30_000,
            retry_attempts: 0,
            retry_backoff: Duration::from_millis(100),
            retry_backoff_max: Duration::from_secs(1),
            delivery_timeout: Duration::from_secs(2),
            max_block: Duration::from_mins(1),
            partitioner_ignore_keys: false,
            partitioner_adaptive_partitioning_enable: true,
            partitioner_availability_timeout: Duration::ZERO,
            max_in_flight_requests_per_connection: 1,
            max_request_size: 1_048_576,
            enable_metrics_push: true,
            compression: ProducerCompression::default(),
            idempotence: idempotence_disabled(),
        },
    );

    let delivery = producer
        .send(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")))
        .expect("send should append before produce dispatch");
    let flush_error = producer
        .flush()
        .await
        .expect_err("wire backpressure should fail flush");
    let delivery_error = delivery
        .await
        .expect_err("delivery should preserve terminal wire error");

    assert!(matches!(
        flush_error,
        kacrab::producer::ProducerError::Wire(kacrab::wire::WireError::ConnectionClosed)
    ));
    assert!(matches!(
        delivery_error,
        kacrab::producer::ProducerError::Wire(kacrab::wire::WireError::ConnectionClosed)
    ));
    assert_eq!(producer.buffered_bytes(), 0);
    assert_eq!(bootstrap.join().await, 2);
    assert_eq!(leader_7.join().await, 1);
}

#[tokio::test]
async fn dispatcher_owned_batches_report_leadership_error_without_retry() {
    let leader_7 = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::Produce as i16);
            produce_error_response_frame_for_request(&header, 0, ErrorCode::NotLeaderOrFollower)
        }),
    ])
    .await;
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new({
            let leader_7 = leader_7.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::Metadata as i16);
                let response = metadata_response([(7, leader_7)]);
                response_frame(ApiKey::Metadata, 13, header.correlation_id, &response)
            }
        }),
    ])
    .await;
    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default(),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let dispatcher = ProducerDispatcher::new(wire);

    let error = dispatcher
        .dispatch_ready_batches(
            ready_batches_for_value(b"a", Instant::now()),
            Instant::now(),
        )
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        kacrab::producer::ProducerError::Broker {
            error: ErrorCode::NotLeaderOrFollower,
            ..
        }
    ));
    assert_eq!(bootstrap.join().await, 2);
    assert_eq!(leader_7.join().await, 2);
}

#[tokio::test]
async fn dispatcher_fails_expired_batch_with_delivery_timeout() {
    let appended_at = Instant::now();
    let dispatch_at = appended_at
        .checked_add(Duration::from_millis(2))
        .expect("test instant should not overflow");
    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default(),
        "kacrab-test",
        [BrokerEndpoint::new(1, "127.0.0.1:1".parse().unwrap())],
    );
    let dispatcher = ProducerDispatcher::new(wire).delivery_timeout(Duration::from_millis(1));
    let accumulator = SharedAccumulator::with_config(
        AccumulatorConfig::default()
            .batch_size(1)
            .buffer_memory(16 * 1024),
    );
    accumulator
        .append_at(
            ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")),
            appended_at,
        )
        .unwrap();

    let error = dispatcher
        .dispatch_ready(&accumulator, dispatch_at)
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        kacrab::producer::ProducerError::DeliveryTimeout {
            ref topic,
            partition: 0,
        } if topic == "orders"
    ));
    assert_eq!(accumulator.buffered_bytes(), 0);
}

fn api_versions_response_frame(mut request: Bytes) -> BytesMut {
    let header = RequestHeaderData::read(&mut request, 2).expect("request header");
    response_frame(
        ApiKey::ApiVersions,
        3,
        header.correlation_id,
        &ApiVersionsResponseData {
            error_code: 0,
            api_keys: vec![
                ApiVersion {
                    api_key: ApiKey::ApiVersions as i16,
                    min_version: 0,
                    max_version: 4,
                    _unknown_tagged_fields: Vec::new(),
                },
                ApiVersion {
                    api_key: ApiKey::Metadata as i16,
                    min_version: 0,
                    max_version: 13,
                    _unknown_tagged_fields: Vec::new(),
                },
                ApiVersion {
                    api_key: ApiKey::Produce as i16,
                    min_version: 3,
                    max_version: 13,
                    _unknown_tagged_fields: Vec::new(),
                },
                ApiVersion {
                    api_key: ApiKey::InitProducerId as i16,
                    min_version: 0,
                    max_version: 6,
                    _unknown_tagged_fields: Vec::new(),
                },
                ApiVersion {
                    api_key: ApiKey::FindCoordinator as i16,
                    min_version: 0,
                    max_version: 6,
                    _unknown_tagged_fields: Vec::new(),
                },
                ApiVersion {
                    api_key: ApiKey::AddPartitionsToTxn as i16,
                    min_version: 0,
                    max_version: 5,
                    _unknown_tagged_fields: Vec::new(),
                },
                ApiVersion {
                    api_key: ApiKey::AddOffsetsToTxn as i16,
                    min_version: 0,
                    max_version: 4,
                    _unknown_tagged_fields: Vec::new(),
                },
                ApiVersion {
                    api_key: ApiKey::TxnOffsetCommit as i16,
                    min_version: 0,
                    max_version: 5,
                    _unknown_tagged_fields: Vec::new(),
                },
                ApiVersion {
                    api_key: ApiKey::EndTxn as i16,
                    min_version: 0,
                    max_version: 5,
                    _unknown_tagged_fields: Vec::new(),
                },
                ApiVersion {
                    api_key: ApiKey::GetTelemetrySubscriptions as i16,
                    min_version: 0,
                    max_version: 0,
                    _unknown_tagged_fields: Vec::new(),
                },
                ApiVersion {
                    api_key: ApiKey::PushTelemetry as i16,
                    min_version: 0,
                    max_version: 0,
                    _unknown_tagged_fields: Vec::new(),
                },
            ],
            ..ApiVersionsResponseData::default()
        },
    )
}

fn metadata_response<const N: usize>(
    brokers: [(i32, std::net::SocketAddr); N],
) -> MetadataResponseData {
    MetadataResponseData {
        brokers: brokers
            .into_iter()
            .map(|(node_id, addr)| MetadataResponseBroker {
                node_id,
                host: KafkaString::from(addr.ip().to_string()),
                port: i32::from(addr.port()),
                rack: None,
                _unknown_tagged_fields: Vec::new(),
            })
            .collect(),
        topics: vec![MetadataResponseTopic {
            error_code: 0,
            name: Some(KafkaString::from("orders".to_owned())),
            topic_id: TOPIC_ID,
            partitions: vec![
                MetadataResponsePartition {
                    error_code: 0,
                    partition_index: 0,
                    leader_id: 7,
                    leader_epoch: 3,
                    replica_nodes: vec![7],
                    isr_nodes: vec![7],
                    offline_replicas: Vec::new(),
                    _unknown_tagged_fields: Vec::new(),
                },
                MetadataResponsePartition {
                    error_code: 0,
                    partition_index: 1,
                    leader_id: 8,
                    leader_epoch: 3,
                    replica_nodes: vec![8],
                    isr_nodes: vec![8],
                    offline_replicas: Vec::new(),
                    _unknown_tagged_fields: Vec::new(),
                },
            ],
            ..MetadataResponseTopic::default()
        }],
        ..MetadataResponseData::default()
    }
}

fn metadata_response_same_leader(
    leader_id: i32,
    leader_addr: std::net::SocketAddr,
) -> MetadataResponseData {
    MetadataResponseData {
        brokers: vec![MetadataResponseBroker {
            node_id: leader_id,
            host: KafkaString::from(leader_addr.ip().to_string()),
            port: i32::from(leader_addr.port()),
            rack: None,
            _unknown_tagged_fields: Vec::new(),
        }],
        topics: vec![MetadataResponseTopic {
            error_code: 0,
            name: Some(KafkaString::from("orders".to_owned())),
            topic_id: TOPIC_ID,
            partitions: vec![
                MetadataResponsePartition {
                    error_code: 0,
                    partition_index: 0,
                    leader_id,
                    leader_epoch: 3,
                    replica_nodes: vec![leader_id],
                    isr_nodes: vec![leader_id],
                    offline_replicas: Vec::new(),
                    _unknown_tagged_fields: Vec::new(),
                },
                MetadataResponsePartition {
                    error_code: 0,
                    partition_index: 1,
                    leader_id,
                    leader_epoch: 3,
                    replica_nodes: vec![leader_id],
                    isr_nodes: vec![leader_id],
                    offline_replicas: Vec::new(),
                    _unknown_tagged_fields: Vec::new(),
                },
            ],
            ..MetadataResponseTopic::default()
        }],
        ..MetadataResponseData::default()
    }
}

fn metadata_response_same_leader_partitions(
    leader_id: i32,
    leader_addr: std::net::SocketAddr,
    partition_count: usize,
) -> MetadataResponseData {
    MetadataResponseData {
        brokers: vec![MetadataResponseBroker {
            node_id: leader_id,
            host: KafkaString::from(leader_addr.ip().to_string()),
            port: i32::from(leader_addr.port()),
            rack: None,
            _unknown_tagged_fields: Vec::new(),
        }],
        topics: vec![MetadataResponseTopic {
            error_code: 0,
            name: Some(KafkaString::from("orders".to_owned())),
            topic_id: TOPIC_ID,
            partitions: (0..partition_count)
                .map(|partition| {
                    let partition =
                        i32::try_from(partition).expect("partition index should fit i32");
                    MetadataResponsePartition {
                        error_code: 0,
                        partition_index: partition,
                        leader_id,
                        leader_epoch: 3,
                        replica_nodes: vec![leader_id],
                        isr_nodes: vec![leader_id],
                        offline_replicas: Vec::new(),
                        _unknown_tagged_fields: Vec::new(),
                    }
                })
                .collect(),
            ..MetadataResponseTopic::default()
        }],
        ..MetadataResponseData::default()
    }
}

fn moved_metadata_response(
    leader_id: i32,
    leader_addr: std::net::SocketAddr,
) -> MetadataResponseData {
    MetadataResponseData {
        brokers: vec![MetadataResponseBroker {
            node_id: leader_id,
            host: KafkaString::from(leader_addr.ip().to_string()),
            port: i32::from(leader_addr.port()),
            rack: None,
            _unknown_tagged_fields: Vec::new(),
        }],
        topics: vec![MetadataResponseTopic {
            error_code: 0,
            name: Some(KafkaString::from("orders".to_owned())),
            topic_id: TOPIC_ID,
            partitions: vec![MetadataResponsePartition {
                error_code: 0,
                partition_index: 0,
                leader_id,
                leader_epoch: 4,
                replica_nodes: vec![leader_id],
                isr_nodes: vec![leader_id],
                offline_replicas: Vec::new(),
                _unknown_tagged_fields: Vec::new(),
            }],
            ..MetadataResponseTopic::default()
        }],
        ..MetadataResponseData::default()
    }
}

fn empty_metadata_response() -> MetadataResponseData {
    MetadataResponseData::default()
}

#[derive(Debug)]
struct CaptureSendErrorMetadata {
    metadata: Arc<Mutex<Option<RecordMetadata>>>,
}

impl ProducerInterceptor for CaptureSendErrorMetadata {
    fn on_ack(
        &self,
        metadata: Option<&RecordMetadata>,
        error: Option<&kacrab::producer::ProducerError>,
        _headers: &[kacrab_protocol::record::RecordHeader],
    ) {
        assert!(error.is_some());
        *self.metadata.lock().unwrap() = metadata.cloned();
    }
}

fn ready_batches_for_value(
    value: &'static [u8],
    now: Instant,
) -> Vec<kacrab::producer::internals::ReadyBatch> {
    let accumulator = SharedAccumulator::with_config(
        AccumulatorConfig::default()
            .batch_size(1)
            .buffer_memory(16 * 1024),
    );
    accumulator
        .append_at(
            ProducerRecord::new("orders", 0).value(Bytes::from_static(value)),
            now,
        )
        .unwrap();
    accumulator.drain_ready(now)
}

fn produce_request_partitions(produce: &ProduceRequestData) -> Vec<i32> {
    let mut partitions = produce
        .topic_data
        .iter()
        .flat_map(|topic| topic.partition_data.iter())
        .map(|partition| partition.index)
        .collect::<Vec<_>>();
    partitions.sort_unstable();
    partitions
}

fn capture_produce_request(
    observed_request_lengths: Arc<Mutex<Vec<usize>>>,
    observed_partition_groups: Arc<Mutex<Vec<Vec<i32>>>>,
) -> Box<dyn FnOnce(Bytes) -> BytesMut + Send> {
    Box::new(move |mut request| {
        observed_request_lengths
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(request.len());
        let header = RequestHeaderData::read(&mut request, 2).expect("request header");
        assert_eq!(header.request_api_key, ApiKey::Produce as i16);
        let produce = ProduceRequestData::read(&mut request, header.request_api_version)
            .expect("produce request");
        let partitions = produce_request_partitions(&produce);
        observed_partition_groups
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(partitions.clone());
        let offsets = partitions
            .into_iter()
            .map(|partition| (partition, i64::from(partition)))
            .collect::<Vec<_>>();
        produce_response_frame_for_partitions(&header, &offsets)
    })
}

fn produce_response_frame(correlation_id: i32, partition: i32, base_offset: i64) -> BytesMut {
    produce_response_frame_for_version(13, correlation_id, partition, base_offset)
}

fn produce_response_frame_for_request(
    header: &RequestHeaderData,
    partition: i32,
    base_offset: i64,
) -> BytesMut {
    produce_response_frame_for_version(
        header.request_api_version,
        header.correlation_id,
        partition,
        base_offset,
    )
}

fn produce_response_frame_for_partitions(
    header: &RequestHeaderData,
    partitions: &[(i32, i64)],
) -> BytesMut {
    let mut topic = TopicProduceResponse::default();
    if header.request_api_version >= 13 {
        topic.topic_id = TOPIC_ID;
    } else {
        topic.name = KafkaString::from("orders".to_owned());
    }
    topic.partition_responses = partitions
        .iter()
        .copied()
        .map(|(partition, base_offset)| PartitionProduceResponse {
            index: partition,
            error_code: 0,
            base_offset,
            log_append_time_ms: -1,
            log_start_offset: base_offset,
            ..PartitionProduceResponse::default()
        })
        .collect();
    let response = ProduceResponseData {
        responses: vec![topic],
        ..ProduceResponseData::default()
    };
    response_frame(
        ApiKey::Produce,
        header.request_api_version,
        header.correlation_id,
        &response,
    )
}

fn produce_response_frame_for_version(
    version: i16,
    correlation_id: i32,
    partition: i32,
    base_offset: i64,
) -> BytesMut {
    let response = ProduceResponseData {
        responses: vec![topic_produce_response(
            version,
            partition,
            0,
            base_offset,
            base_offset,
        )],
        ..ProduceResponseData::default()
    };
    response_frame(ApiKey::Produce, version, correlation_id, &response)
}

fn produce_error_response_frame_for_request(
    header: &RequestHeaderData,
    partition: i32,
    error: ErrorCode,
) -> BytesMut {
    produce_error_response_frame_with_log_start_offset_for_request(header, partition, error, -1)
}

fn produce_error_response_frame_with_log_start_offset_for_request(
    header: &RequestHeaderData,
    partition: i32,
    error: ErrorCode,
    log_start_offset: i64,
) -> BytesMut {
    produce_error_response_frame_with_log_start_offset_for_version(
        header.request_api_version,
        header.correlation_id,
        partition,
        error,
        log_start_offset,
    )
}

fn produce_leader_change_error_response_frame_for_request(
    header: &RequestHeaderData,
    partition: i32,
    leader_id: i32,
    leader_epoch: i32,
    leader_addr: std::net::SocketAddr,
) -> BytesMut {
    let mut topic = topic_produce_response(
        header.request_api_version,
        partition,
        ErrorCode::NotLeaderOrFollower.code(),
        -1,
        -1,
    );
    topic
        .partition_responses
        .first_mut()
        .expect("partition response")
        .current_leader = ProduceLeaderIdAndEpoch {
        leader_id,
        leader_epoch,
        _unknown_tagged_fields: Vec::new(),
    };
    let response = ProduceResponseData {
        responses: vec![topic],
        node_endpoints: vec![ProduceNodeEndpoint {
            node_id: leader_id,
            host: KafkaString::from(leader_addr.ip().to_string()),
            port: i32::from(leader_addr.port()),
            rack: None,
            _unknown_tagged_fields: Vec::new(),
        }],
        ..ProduceResponseData::default()
    };
    response_frame(
        ApiKey::Produce,
        header.request_api_version,
        header.correlation_id,
        &response,
    )
}

fn produce_error_response_frame_with_log_start_offset_for_version(
    version: i16,
    correlation_id: i32,
    partition: i32,
    error: ErrorCode,
    log_start_offset: i64,
) -> BytesMut {
    let response = ProduceResponseData {
        responses: vec![topic_produce_response(
            version,
            partition,
            error.code(),
            -1,
            log_start_offset,
        )],
        ..ProduceResponseData::default()
    };
    response_frame(ApiKey::Produce, version, correlation_id, &response)
}

fn topic_produce_response(
    version: i16,
    partition: i32,
    error_code: i16,
    base_offset: i64,
    log_start_offset: i64,
) -> TopicProduceResponse {
    let mut response = TopicProduceResponse {
        partition_responses: vec![PartitionProduceResponse {
            index: partition,
            error_code,
            base_offset,
            log_append_time_ms: -1,
            log_start_offset,
            ..PartitionProduceResponse::default()
        }],
        ..TopicProduceResponse::default()
    };
    if version >= 13 {
        response.topic_id = TOPIC_ID;
    } else {
        response.name = KafkaString::from("orders".to_owned());
    }
    response
}

fn init_producer_id_response_frame(
    correlation_id: i32,
    producer_id: i64,
    producer_epoch: i16,
) -> BytesMut {
    init_producer_id_response_frame_for_version(5, correlation_id, producer_id, producer_epoch)
}

fn init_producer_id_response_frame_for_request(
    header: &RequestHeaderData,
    producer_id: i64,
    producer_epoch: i16,
) -> BytesMut {
    init_producer_id_response_frame_for_version(
        header.request_api_version,
        header.correlation_id,
        producer_id,
        producer_epoch,
    )
}

fn init_producer_id_response_frame_for_version(
    version: i16,
    correlation_id: i32,
    producer_id: i64,
    producer_epoch: i16,
) -> BytesMut {
    let response = InitProducerIdResponseData {
        error_code: 0,
        producer_id,
        producer_epoch,
        ..InitProducerIdResponseData::default()
    };
    response_frame(ApiKey::InitProducerId, version, correlation_id, &response)
}

fn init_producer_id_error_response_frame(correlation_id: i32, error: ErrorCode) -> BytesMut {
    let response = InitProducerIdResponseData {
        error_code: error.code(),
        ..InitProducerIdResponseData::default()
    };
    response_frame(ApiKey::InitProducerId, 5, correlation_id, &response)
}

fn find_coordinator_response_frame(
    correlation_id: i32,
    node_id: i32,
    addr: std::net::SocketAddr,
) -> BytesMut {
    let response = FindCoordinatorResponseData {
        coordinators: vec![kacrab_protocol::generated::Coordinator {
            key: KafkaString::from("txn-orders".to_owned()),
            node_id,
            host: KafkaString::from(addr.ip().to_string()),
            port: i32::from(addr.port()),
            error_code: 0,
            error_message: None,
            _unknown_tagged_fields: Vec::new(),
        }],
        ..FindCoordinatorResponseData::default()
    };
    response_frame(ApiKey::FindCoordinator, 6, correlation_id, &response)
}

fn find_group_coordinator_response_frame(
    correlation_id: i32,
    node_id: i32,
    addr: std::net::SocketAddr,
) -> BytesMut {
    let response = FindCoordinatorResponseData {
        coordinators: vec![kacrab_protocol::generated::Coordinator {
            key: KafkaString::from("group-a".to_owned()),
            node_id,
            host: KafkaString::from(addr.ip().to_string()),
            port: i32::from(addr.port()),
            error_code: 0,
            error_message: None,
            _unknown_tagged_fields: Vec::new(),
        }],
        ..FindCoordinatorResponseData::default()
    };
    response_frame(ApiKey::FindCoordinator, 6, correlation_id, &response)
}

fn find_coordinator_error_response_frame(correlation_id: i32, error: ErrorCode) -> BytesMut {
    let response = FindCoordinatorResponseData {
        coordinators: vec![kacrab_protocol::generated::Coordinator {
            key: KafkaString::from("txn-orders".to_owned()),
            error_code: error.code(),
            ..kacrab_protocol::generated::Coordinator::default()
        }],
        ..FindCoordinatorResponseData::default()
    };
    response_frame(ApiKey::FindCoordinator, 6, correlation_id, &response)
}

fn find_group_coordinator_error_response_frame(correlation_id: i32, error: ErrorCode) -> BytesMut {
    let response = FindCoordinatorResponseData {
        coordinators: vec![kacrab_protocol::generated::Coordinator {
            key: KafkaString::from("group-a".to_owned()),
            error_code: error.code(),
            ..kacrab_protocol::generated::Coordinator::default()
        }],
        ..FindCoordinatorResponseData::default()
    };
    response_frame(ApiKey::FindCoordinator, 6, correlation_id, &response)
}

fn find_coordinator_invalid_port_response_frame(correlation_id: i32) -> BytesMut {
    let response = FindCoordinatorResponseData {
        coordinators: vec![kacrab_protocol::generated::Coordinator {
            key: KafkaString::from("txn-orders".to_owned()),
            node_id: 9,
            host: KafkaString::from("127.0.0.1".to_owned()),
            port: -1,
            error_code: 0,
            error_message: None,
            _unknown_tagged_fields: Vec::new(),
        }],
        ..FindCoordinatorResponseData::default()
    };
    response_frame(ApiKey::FindCoordinator, 6, correlation_id, &response)
}

fn add_partitions_to_txn_response_frame(correlation_id: i32) -> BytesMut {
    let response = AddPartitionsToTxnResponseData {
        error_code: 0,
        results_by_transaction: vec![AddPartitionsToTxnResult {
            transactional_id: KafkaString::from("txn-orders".to_owned()),
            topic_results: vec![AddPartitionsToTxnTopicResult {
                name: KafkaString::from("orders".to_owned()),
                results_by_partition: vec![
                    kacrab_protocol::generated::AddPartitionsToTxnPartitionResult {
                        partition_index: 0,
                        partition_error_code: 0,
                        _unknown_tagged_fields: Vec::new(),
                    },
                ],
                _unknown_tagged_fields: Vec::new(),
            }],
            _unknown_tagged_fields: Vec::new(),
        }],
        ..AddPartitionsToTxnResponseData::default()
    };
    response_frame(ApiKey::AddPartitionsToTxn, 5, correlation_id, &response)
}

fn add_partitions_to_txn_error_response_frame(correlation_id: i32, error: ErrorCode) -> BytesMut {
    let response = AddPartitionsToTxnResponseData {
        error_code: 0,
        results_by_transaction: vec![AddPartitionsToTxnResult {
            transactional_id: KafkaString::from("txn-orders".to_owned()),
            topic_results: vec![AddPartitionsToTxnTopicResult {
                name: KafkaString::from("orders".to_owned()),
                results_by_partition: vec![
                    kacrab_protocol::generated::AddPartitionsToTxnPartitionResult {
                        partition_index: 0,
                        partition_error_code: error.code(),
                        _unknown_tagged_fields: Vec::new(),
                    },
                ],
                _unknown_tagged_fields: Vec::new(),
            }],
            _unknown_tagged_fields: Vec::new(),
        }],
        ..AddPartitionsToTxnResponseData::default()
    };
    response_frame(ApiKey::AddPartitionsToTxn, 5, correlation_id, &response)
}

fn add_offsets_to_txn_response_frame(correlation_id: i32, error: ErrorCode) -> BytesMut {
    let response = AddOffsetsToTxnResponseData {
        error_code: error.code(),
        ..AddOffsetsToTxnResponseData::default()
    };
    response_frame(ApiKey::AddOffsetsToTxn, 4, correlation_id, &response)
}

fn add_offsets_to_txn_ok_handler() -> Box<dyn FnOnce(Bytes) -> BytesMut + Send> {
    Box::new(|mut request| {
        let header = RequestHeaderData::read(&mut request, 2).expect("request header");
        assert_eq!(header.request_api_key, ApiKey::AddOffsetsToTxn as i16);
        let add_offsets =
            AddOffsetsToTxnRequestData::read(&mut request, 4).expect("add offsets to txn");
        assert_eq!(
            add_offsets.transactional_id,
            KafkaString::from("txn-orders".to_owned())
        );
        assert_eq!(
            add_offsets.group_id,
            KafkaString::from("group-a".to_owned())
        );
        add_offsets_to_txn_response_frame(header.correlation_id, ErrorCode::None)
    })
}

fn txn_offset_commit_response_frame(correlation_id: i32, error: ErrorCode) -> BytesMut {
    let response = TxnOffsetCommitResponseData {
        topics: vec![TxnOffsetCommitResponseTopic {
            name: KafkaString::from("orders".to_owned()),
            partitions: vec![TxnOffsetCommitResponsePartition {
                partition_index: 0,
                error_code: error.code(),
                _unknown_tagged_fields: Vec::new(),
            }],
            _unknown_tagged_fields: Vec::new(),
        }],
        ..TxnOffsetCommitResponseData::default()
    };
    response_frame(ApiKey::TxnOffsetCommit, 5, correlation_id, &response)
}

fn txn_offset_commit_multi_partition_response_frame<const N: usize>(
    correlation_id: i32,
    partition_errors: [(i32, ErrorCode); N],
) -> BytesMut {
    let response = TxnOffsetCommitResponseData {
        topics: vec![TxnOffsetCommitResponseTopic {
            name: KafkaString::from("orders".to_owned()),
            partitions: partition_errors
                .into_iter()
                .map(
                    |(partition_index, error)| TxnOffsetCommitResponsePartition {
                        partition_index,
                        error_code: error.code(),
                        _unknown_tagged_fields: Vec::new(),
                    },
                )
                .collect(),
            _unknown_tagged_fields: Vec::new(),
        }],
        ..TxnOffsetCommitResponseData::default()
    };
    response_frame(ApiKey::TxnOffsetCommit, 5, correlation_id, &response)
}

fn txn_offset_commit_ok_handler() -> Box<dyn FnOnce(Bytes) -> BytesMut + Send> {
    Box::new(|mut request| {
        let header = RequestHeaderData::read(&mut request, 2).expect("request header");
        assert_eq!(header.request_api_key, ApiKey::TxnOffsetCommit as i16);
        let commit = TxnOffsetCommitRequestData::read(&mut request, header.request_api_version)
            .expect("txn offset commit");
        assert_eq!(commit.group_id, KafkaString::from("group-a".to_owned()));
        assert_eq!(commit.topics.len(), 1);
        let topic = commit.topics.first().expect("txn offset commit topic");
        let partition = topic
            .partitions
            .first()
            .expect("txn offset commit partition");
        assert_eq!(topic.name, KafkaString::from("orders".to_owned()));
        assert_eq!(partition.partition_index, 0);
        assert_eq!(partition.committed_offset, 7);
        txn_offset_commit_response_frame(header.correlation_id, ErrorCode::None)
    })
}

async fn send_offsets_to_started_transaction(producer: &Producer) {
    producer
        .send_offsets_to_transaction(
            [(
                kacrab::producer::TopicPartition::new("orders", 0),
                kacrab::producer::OffsetAndMetadata::new(7),
            )],
            kacrab::producer::ConsumerGroupMetadata::new("group-a"),
        )
        .await
        .expect("offset commit should start transaction");
}

fn end_txn_response_frame_for_request(header: &RequestHeaderData) -> BytesMut {
    end_txn_response_frame_for_version(header.request_api_version, header.correlation_id)
}

fn end_txn_response_frame_for_request_with_identity(
    header: &RequestHeaderData,
    producer_id: i64,
    producer_epoch: i16,
) -> BytesMut {
    end_txn_response_frame_for_version_with_identity(
        header.request_api_version,
        header.correlation_id,
        producer_id,
        producer_epoch,
    )
}

fn end_txn_response_frame_for_version(version: i16, correlation_id: i32) -> BytesMut {
    let response = end_txn_response_data(version, ErrorCode::None);
    response_frame(ApiKey::EndTxn, version, correlation_id, &response)
}

fn end_txn_response_frame_for_version_with_identity(
    version: i16,
    correlation_id: i32,
    producer_id: i64,
    producer_epoch: i16,
) -> BytesMut {
    let mut response = end_txn_response_data(version, ErrorCode::None);
    if version >= 5 {
        response.producer_id = producer_id;
        response.producer_epoch = producer_epoch;
    }
    response_frame(ApiKey::EndTxn, version, correlation_id, &response)
}

fn end_txn_error_response_frame(correlation_id: i32, error: ErrorCode) -> BytesMut {
    end_txn_error_response_frame_for_version(5, correlation_id, error)
}

fn end_txn_error_response_frame_for_request(
    header: &RequestHeaderData,
    error: ErrorCode,
) -> BytesMut {
    end_txn_error_response_frame_for_version(
        header.request_api_version,
        header.correlation_id,
        error,
    )
}

fn end_txn_error_response_frame_for_version(
    version: i16,
    correlation_id: i32,
    error: ErrorCode,
) -> BytesMut {
    let response = end_txn_response_data(version, error);
    response_frame(ApiKey::EndTxn, version, correlation_id, &response)
}

fn end_txn_response_data(version: i16, error: ErrorCode) -> EndTxnResponseData {
    let mut response = EndTxnResponseData {
        error_code: error.code(),
        ..EndTxnResponseData::default()
    };
    if version >= 5 {
        response.producer_id = 77;
        response.producer_epoch = 4;
    }
    response
}

fn get_telemetry_subscriptions_response_frame(
    correlation_id: i32,
    client_instance_id: KafkaUuid,
) -> BytesMut {
    get_telemetry_subscriptions_response_frame_with_subscription(
        correlation_id,
        client_instance_id,
        7,
    )
}

fn get_telemetry_subscriptions_response_frame_with_subscription(
    correlation_id: i32,
    client_instance_id: KafkaUuid,
    subscription_id: i32,
) -> BytesMut {
    let response = GetTelemetrySubscriptionsResponseData {
        error_code: 0,
        client_instance_id,
        subscription_id,
        accepted_compression_types: vec![0],
        push_interval_ms: 60_000,
        telemetry_max_bytes: 1024 * 1024,
        requested_metrics: vec![KafkaString::from(String::new())],
        ..GetTelemetrySubscriptionsResponseData::default()
    };
    response_frame(
        ApiKey::GetTelemetrySubscriptions,
        0,
        correlation_id,
        &response,
    )
}

fn get_telemetry_subscriptions_error_response_frame(
    correlation_id: i32,
    error: ErrorCode,
) -> BytesMut {
    let response = GetTelemetrySubscriptionsResponseData {
        error_code: i16::from(error),
        ..GetTelemetrySubscriptionsResponseData::default()
    };
    response_frame(
        ApiKey::GetTelemetrySubscriptions,
        0,
        correlation_id,
        &response,
    )
}

fn push_telemetry_response_frame(correlation_id: i32, error: ErrorCode) -> BytesMut {
    let response = PushTelemetryResponseData {
        error_code: i16::from(error),
        ..PushTelemetryResponseData::default()
    };
    response_frame(ApiKey::PushTelemetry, 0, correlation_id, &response)
}

fn response_frame(
    api_key: ApiKey,
    api_version: i16,
    correlation_id: i32,
    response: &impl WriteResponse,
) -> BytesMut {
    let mut header = BytesMut::new();
    ResponseHeaderData {
        correlation_id,
        _unknown_tagged_fields: Vec::new(),
    }
    .write(
        &mut header,
        response_header_version(api_key as i16, api_version),
    )
    .expect("response header write");

    let mut body = BytesMut::new();
    response.write_response(&mut body, api_version);
    frame::encode_request(&header, &body).expect("response frame")
}

trait WriteResponse {
    fn write_response(&self, buf: &mut BytesMut, version: i16);
}

impl WriteResponse for ApiVersionsResponseData {
    fn write_response(&self, buf: &mut BytesMut, version: i16) {
        self.write(buf, version).expect("api versions response");
    }
}

impl WriteResponse for MetadataResponseData {
    fn write_response(&self, buf: &mut BytesMut, version: i16) {
        self.write(buf, version).expect("metadata response");
    }
}

impl WriteResponse for ProduceResponseData {
    fn write_response(&self, buf: &mut BytesMut, version: i16) {
        self.write(buf, version).expect("produce response");
    }
}

impl WriteResponse for InitProducerIdResponseData {
    fn write_response(&self, buf: &mut BytesMut, version: i16) {
        self.write(buf, version).expect("init producer id response");
    }
}

impl WriteResponse for FindCoordinatorResponseData {
    fn write_response(&self, buf: &mut BytesMut, version: i16) {
        self.write(buf, version).expect("find coordinator response");
    }
}

impl WriteResponse for AddPartitionsToTxnResponseData {
    fn write_response(&self, buf: &mut BytesMut, version: i16) {
        self.write(buf, version)
            .expect("add partitions to txn response");
    }
}

impl WriteResponse for AddOffsetsToTxnResponseData {
    fn write_response(&self, buf: &mut BytesMut, version: i16) {
        self.write(buf, version)
            .expect("add offsets to txn response");
    }
}

impl WriteResponse for TxnOffsetCommitResponseData {
    fn write_response(&self, buf: &mut BytesMut, version: i16) {
        self.write(buf, version)
            .expect("txn offset commit response");
    }
}

impl WriteResponse for EndTxnResponseData {
    fn write_response(&self, buf: &mut BytesMut, version: i16) {
        self.write(buf, version).expect("end txn response");
    }
}

impl WriteResponse for GetTelemetrySubscriptionsResponseData {
    fn write_response(&self, buf: &mut BytesMut, version: i16) {
        self.write(buf, version)
            .expect("get telemetry subscriptions response");
    }
}

impl WriteResponse for PushTelemetryResponseData {
    fn write_response(&self, buf: &mut BytesMut, version: i16) {
        self.write(buf, version).expect("push telemetry response");
    }
}

const fn idempotence_disabled() -> ProducerIdempotenceConfig {
    ProducerIdempotenceConfig {
        enabled: false,
        transactional_id: None,
        transaction_timeout_ms: 60_000,
        transaction_two_phase_commit: false,
    }
}

fn transactional_test_producer(wire: WireClient) -> Producer {
    transactional_test_producer_with_retries(wire, 0)
}

fn transactional_test_producer_with_retries(wire: WireClient, retry_attempts: usize) -> Producer {
    transactional_test_producer_with_retries_and_batch_size(wire, retry_attempts, 1)
}

fn transactional_test_producer_with_batch_size_and_linger(
    wire: WireClient,
    batch_size: usize,
    linger: Duration,
) -> Producer {
    transactional_test_producer_with_retries_batch_size_and_linger(wire, 0, batch_size, linger)
}

fn transactional_test_producer_with_retries_and_batch_size(
    wire: WireClient,
    retry_attempts: usize,
    batch_size: usize,
) -> Producer {
    transactional_test_producer_with_retries_batch_size_and_linger(
        wire,
        retry_attempts,
        batch_size,
        Duration::ZERO,
    )
}

fn transactional_test_producer_with_retries_batch_size_and_linger(
    wire: WireClient,
    retry_attempts: usize,
    batch_size: usize,
    linger: Duration,
) -> Producer {
    Producer::from_parts(
        wire,
        ProducerRuntimeConfig {
            accumulator: AccumulatorConfig::default()
                .batch_size(batch_size)
                .linger(linger)
                .buffer_memory(16 * 1024),
            acks: -1,
            timeout_ms: 30_000,
            retry_attempts,
            retry_backoff: Duration::from_millis(100),
            retry_backoff_max: Duration::from_secs(1),
            delivery_timeout: Duration::from_mins(2),
            max_block: Duration::from_mins(1),
            partitioner_ignore_keys: false,
            partitioner_adaptive_partitioning_enable: true,
            partitioner_availability_timeout: Duration::ZERO,
            max_in_flight_requests_per_connection: 5,
            max_request_size: 1_048_576,
            enable_metrics_push: true,
            compression: ProducerCompression::default(),
            idempotence: ProducerIdempotenceConfig {
                enabled: true,
                transactional_id: Some("txn-orders".to_owned()),
                transaction_timeout_ms: 60_000,
                transaction_two_phase_commit: false,
            },
        },
    )
}

fn transaction_v2_test_producer(wire: WireClient) -> Producer {
    Producer::from_parts(
        wire,
        ProducerRuntimeConfig {
            accumulator: AccumulatorConfig::default()
                .batch_size(1)
                .buffer_memory(16 * 1024),
            acks: -1,
            timeout_ms: 30_000,
            retry_attempts: 0,
            retry_backoff: Duration::from_millis(100),
            retry_backoff_max: Duration::from_secs(1),
            delivery_timeout: Duration::from_mins(2),
            max_block: Duration::from_mins(1),
            partitioner_ignore_keys: false,
            partitioner_adaptive_partitioning_enable: true,
            partitioner_availability_timeout: Duration::ZERO,
            max_in_flight_requests_per_connection: 5,
            max_request_size: 1_048_576,
            enable_metrics_push: true,
            compression: ProducerCompression::default(),
            idempotence: ProducerIdempotenceConfig {
                enabled: true,
                transactional_id: Some("txn-orders".to_owned()),
                transaction_timeout_ms: 60_000,
                transaction_two_phase_commit: true,
            },
        },
    )
}

struct MockBroker {
    addr: std::net::SocketAddr,
    join: tokio::task::JoinHandle<usize>,
}

impl MockBroker {
    async fn serve_many(handlers: Vec<Box<dyn FnOnce(Bytes) -> BytesMut + Send>>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let join = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let handled = handlers.len();
            for handler in handlers {
                let request = read_frame(&mut socket).await;
                let response = handler(request);
                socket.write_all(&response).await.unwrap();
            }
            handled
        });
        Self { addr, join }
    }

    async fn serve_pipelined_produce(produce_requests: usize) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let join = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let handshake = read_frame(&mut socket).await;
            socket
                .write_all(&api_versions_response_frame(handshake))
                .await
                .unwrap();
            let mut correlation_ids = Vec::with_capacity(produce_requests);
            for _ in 0..produce_requests {
                let mut request = read_frame(&mut socket).await;
                let header = RequestHeaderData::read(&mut request, 2).expect("produce header");
                assert_eq!(header.request_api_key, ApiKey::Produce as i16);
                let produce = ProduceRequestData::read(&mut request, header.request_api_version)
                    .expect("produce request");
                assert_eq!(produce.topic_data.len(), 1);
                let topic_data = produce.topic_data.first().expect("topic produce data");
                assert_eq!(topic_data.topic_id, TOPIC_ID);
                assert_eq!(topic_data.partition_data.len(), 1);
                let partition_data = topic_data
                    .partition_data
                    .first()
                    .expect("partition produce data");
                assert_eq!(partition_data.index, 0);
                correlation_ids.push(header.correlation_id);
            }
            for (index, correlation_id) in correlation_ids.into_iter().enumerate() {
                let offset = 40_i64
                    .checked_add(i64::try_from(index).expect("offset index"))
                    .expect("offset should fit");
                socket
                    .write_all(&produce_response_frame(correlation_id, 0, offset))
                    .await
                    .unwrap();
            }
            produce_requests
                .checked_add(1)
                .expect("handled request count should fit")
        });
        Self { addr, join }
    }

    async fn serve_pipelined_idempotent_produce(partitions: Vec<i32>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let join = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let handshake = read_frame(&mut socket).await;
            socket
                .write_all(&api_versions_response_frame(handshake))
                .await
                .unwrap();

            let mut init_request = read_frame(&mut socket).await;
            let init_header =
                RequestHeaderData::read(&mut init_request, 2).expect("init producer header");
            assert_eq!(init_header.request_api_key, ApiKey::InitProducerId as i16);
            let init =
                InitProducerIdRequestData::read(&mut init_request, 5).expect("init producer id");
            assert_eq!(init.transactional_id, None);
            socket
                .write_all(&init_producer_id_response_frame(
                    init_header.correlation_id,
                    42,
                    3,
                ))
                .await
                .unwrap();

            // The producer groups all ready batches for a broker into ONE
            // ProduceRequest carrying every partition (Java RecordAccumulator.drain
            // -> one request per node), so the pipelined partitions arrive in a
            // single coalesced request rather than one request per partition.
            let mut request = read_frame(&mut socket).await;
            let header = RequestHeaderData::read(&mut request, 2).expect("produce header");
            assert_eq!(header.request_api_key, ApiKey::Produce as i16);
            let produce = ProduceRequestData::read(&mut request, header.request_api_version)
                .expect("produce request");
            assert_eq!(produce.acks, -1);
            assert_eq!(produce.topic_data.len(), 1);
            let topic_data = produce.topic_data.first().expect("topic produce data");
            assert_eq!(topic_data.topic_id, TOPIC_ID);
            assert_eq!(topic_data.partition_data.len(), partitions.len());
            let mut offsets = Vec::with_capacity(partitions.len());
            for (index, expected_partition) in partitions.iter().enumerate() {
                let partition_data = topic_data
                    .partition_data
                    .iter()
                    .find(|entry| entry.index == *expected_partition)
                    .expect("partition produce data");
                let mut records = partition_data.records.clone().expect("records");
                let batch = RecordBatch::decode(&mut records).expect("record batch");
                assert_eq!(batch.producer_id, 42);
                assert_eq!(batch.producer_epoch, 3);
                assert_eq!(batch.base_sequence, 0);
                let offset = 40_i64
                    .checked_add(i64::try_from(index).expect("offset index"))
                    .expect("offset should fit");
                offsets.push((*expected_partition, offset));
            }
            socket
                .write_all(&produce_response_frame_for_partitions(&header, &offsets))
                .await
                .unwrap();
            // handshake + InitProducerId + one coalesced Produce.
            3
        });
        Self { addr, join }
    }

    async fn serve_pipelined_idempotent_produce_reversed(partitions: Vec<i32>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let join = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let handshake = read_frame(&mut socket).await;
            socket
                .write_all(&api_versions_response_frame(handshake))
                .await
                .unwrap();

            let mut init_request = read_frame(&mut socket).await;
            let init_header =
                RequestHeaderData::read(&mut init_request, 2).expect("init producer header");
            assert_eq!(init_header.request_api_key, ApiKey::InitProducerId as i16);
            let init =
                InitProducerIdRequestData::read(&mut init_request, 5).expect("init producer id");
            assert_eq!(init.transactional_id, None);
            socket
                .write_all(&init_producer_id_response_frame(
                    init_header.correlation_id,
                    42,
                    3,
                ))
                .await
                .unwrap();

            // Both partitions coalesce into one ProduceRequest (Java groups a
            // broker's batches into a single request), so the partition responses
            // are mapped back to their per-partition deliveries inside one frame
            // rather than across reordered separate responses.
            let mut request = read_frame(&mut socket).await;
            let header = RequestHeaderData::read(&mut request, 2).expect("produce header");
            assert_eq!(header.request_api_key, ApiKey::Produce as i16);
            let produce = ProduceRequestData::read(&mut request, header.request_api_version)
                .expect("produce request");
            assert_eq!(produce.acks, -1);
            assert_eq!(produce.topic_data.len(), 1);
            let topic_data = produce.topic_data.first().expect("topic produce data");
            assert_eq!(topic_data.topic_id, TOPIC_ID);
            assert_eq!(topic_data.partition_data.len(), partitions.len());
            // Reverse the per-partition response order within the coalesced frame so
            // the producer must still map each partition response to its delivery.
            let mut offsets = Vec::with_capacity(partitions.len());
            for expected_partition in partitions.iter().rev() {
                let partition_data = topic_data
                    .partition_data
                    .iter()
                    .find(|entry| entry.index == *expected_partition)
                    .expect("partition produce data");
                let mut records = partition_data.records.clone().expect("records");
                let batch = RecordBatch::decode(&mut records).expect("record batch");
                assert_eq!(batch.producer_id, 42);
                assert_eq!(batch.producer_epoch, 3);
                assert_eq!(batch.base_sequence, 0);
                let offset = 40_i64
                    .checked_add(i64::from(*expected_partition))
                    .expect("offset should fit");
                offsets.push((*expected_partition, offset));
            }
            socket
                .write_all(&produce_response_frame_for_partitions(&header, &offsets))
                .await
                .unwrap();
            // handshake + InitProducerId + one coalesced Produce.
            3
        });
        Self { addr, join }
    }

    async fn serve_idempotent_disconnect_then_retry() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let join = tokio::spawn(async move {
            let (mut first_socket, _) = listener.accept().await.unwrap();
            let handshake = read_frame(&mut first_socket).await;
            first_socket
                .write_all(&api_versions_response_frame(handshake))
                .await
                .unwrap();

            let mut init_request = read_frame(&mut first_socket).await;
            let init_header =
                RequestHeaderData::read(&mut init_request, 2).expect("init producer header");
            assert_eq!(init_header.request_api_key, ApiKey::InitProducerId as i16);
            let init =
                InitProducerIdRequestData::read(&mut init_request, 5).expect("init producer id");
            assert_eq!(init.transactional_id, None);
            first_socket
                .write_all(&init_producer_id_response_frame(
                    init_header.correlation_id,
                    42,
                    3,
                ))
                .await
                .unwrap();

            let mut produce_request = read_frame(&mut first_socket).await;
            let produce_header =
                RequestHeaderData::read(&mut produce_request, 2).expect("produce header");
            assert_eq!(produce_header.request_api_key, ApiKey::Produce as i16);
            let produce =
                ProduceRequestData::read(&mut produce_request, produce_header.request_api_version)
                    .expect("produce request");
            assert_single_idempotent_produce(&produce, 0, 0);
            drop(first_socket);

            let (mut retry_socket, _) = listener.accept().await.unwrap();
            let handshake = read_frame(&mut retry_socket).await;
            retry_socket
                .write_all(&api_versions_response_frame(handshake))
                .await
                .unwrap();
            let mut retry_request = read_frame(&mut retry_socket).await;
            let retry_header =
                RequestHeaderData::read(&mut retry_request, 2).expect("retry produce header");
            assert_eq!(retry_header.request_api_key, ApiKey::Produce as i16);
            let retry =
                ProduceRequestData::read(&mut retry_request, retry_header.request_api_version)
                    .expect("retry produce request");
            assert_single_idempotent_produce(&retry, 0, 0);
            retry_socket
                .write_all(&produce_response_frame(retry_header.correlation_id, 0, 40))
                .await
                .unwrap();
            5
        });
        Self { addr, join }
    }

    /// Two idempotent batches (base sequence 0 and 1) are pipelined IN FLIGHT to one
    /// partition, then the connection drops so both re-enqueue for retry. The retry
    /// must re-send them strictly in base-sequence order (Java firstInFlightSequence):
    /// seq 0 alone, ack it, THEN seq 1 — never seq 1 before seq 0.
    async fn serve_idempotent_two_inflight_disconnect_then_inorder_retry() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let join = tokio::spawn(async move {
            let (mut first, _) = listener.accept().await.unwrap();
            let handshake = read_frame(&mut first).await;
            first
                .write_all(&api_versions_response_frame(handshake))
                .await
                .unwrap();

            let mut init_request = read_frame(&mut first).await;
            let init_header =
                RequestHeaderData::read(&mut init_request, 2).expect("init producer header");
            assert_eq!(init_header.request_api_key, ApiKey::InitProducerId as i16);
            first
                .write_all(&init_producer_id_response_frame(
                    init_header.correlation_id,
                    42,
                    3,
                ))
                .await
                .unwrap();

            // Two produce requests held in flight (not yet answered), in ascending
            // base-sequence order: 0 then 1.
            for expected_base_sequence in [0_i32, 1] {
                let mut request = read_frame(&mut first).await;
                let header = RequestHeaderData::read(&mut request, 2).expect("produce header");
                assert_eq!(header.request_api_key, ApiKey::Produce as i16);
                let produce =
                    ProduceRequestData::read(&mut request, header.request_api_version)
                        .expect("produce request");
                assert_single_idempotent_produce(&produce, 0, expected_base_sequence);
            }
            // Drop both in-flight requests so the producer re-enqueues them.
            drop(first);

            // On retry the producer must re-send seq 0 first (alone), wait for its ack,
            // then seq 1 — the firstInFlightSequence ordering gate. No re-InitProducerId
            // (the producer id is cached) and the same epoch 3 (a plain wire retry does
            // not bump the epoch).
            let (mut retry, _) = listener.accept().await.unwrap();
            let handshake = read_frame(&mut retry).await;
            retry
                .write_all(&api_versions_response_frame(handshake))
                .await
                .unwrap();
            for (expected_base_sequence, offset) in [(0_i32, 40_i64), (1, 41)] {
                let mut request = read_frame(&mut retry).await;
                let header =
                    RequestHeaderData::read(&mut request, 2).expect("retry produce header");
                assert_eq!(header.request_api_key, ApiKey::Produce as i16);
                let produce =
                    ProduceRequestData::read(&mut request, header.request_api_version)
                        .expect("retry produce request");
                assert_single_idempotent_produce(&produce, 0, expected_base_sequence);
                retry
                    .write_all(&produce_response_frame(header.correlation_id, 0, offset))
                    .await
                    .unwrap();
            }
            // handshake + InitProducerId + 2 produce (conn 1) + handshake + 2 produce (conn 2).
            6
        });
        Self { addr, join }
    }

    async fn serve_idempotent_timeout_then_epoch_bump_recovery() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let join = tokio::spawn(async move {
            let (mut first_socket, _) = listener.accept().await.unwrap();
            let handshake = read_frame(&mut first_socket).await;
            first_socket
                .write_all(&api_versions_response_frame(handshake))
                .await
                .unwrap();

            let mut init_request = read_frame(&mut first_socket).await;
            let init_header =
                RequestHeaderData::read(&mut init_request, 2).expect("init producer header");
            assert_eq!(init_header.request_api_key, ApiKey::InitProducerId as i16);
            let init =
                InitProducerIdRequestData::read(&mut init_request, 5).expect("init producer id");
            assert_eq!(init.transactional_id, None);
            assert_eq!(init.producer_id, -1);
            assert_eq!(init.producer_epoch, -1);
            first_socket
                .write_all(&init_producer_id_response_frame(
                    init_header.correlation_id,
                    42,
                    3,
                ))
                .await
                .unwrap();

            let mut produce_request = read_frame(&mut first_socket).await;
            let produce_header =
                RequestHeaderData::read(&mut produce_request, 2).expect("produce header");
            assert_eq!(produce_header.request_api_key, ApiKey::Produce as i16);
            let produce =
                ProduceRequestData::read(&mut produce_request, produce_header.request_api_version)
                    .expect("produce request");
            assert_single_idempotent_produce(&produce, 0, 0);
            drop(first_socket);

            let (mut recovery_socket, _) = listener.accept().await.unwrap();
            let handshake = read_frame(&mut recovery_socket).await;
            recovery_socket
                .write_all(&api_versions_response_frame(handshake))
                .await
                .unwrap();

            let mut bump_request = read_frame(&mut recovery_socket).await;
            let bump_header =
                RequestHeaderData::read(&mut bump_request, 2).expect("epoch bump header");
            assert_eq!(bump_header.request_api_key, ApiKey::InitProducerId as i16);
            let bump =
                InitProducerIdRequestData::read(&mut bump_request, 5).expect("epoch bump request");
            assert_eq!(bump.transactional_id, None);
            assert_eq!(bump.producer_id, 42);
            assert_eq!(bump.producer_epoch, 3);
            recovery_socket
                .write_all(&init_producer_id_response_frame(
                    bump_header.correlation_id,
                    42,
                    4,
                ))
                .await
                .unwrap();

            let mut retry_request = read_frame(&mut recovery_socket).await;
            let retry_header =
                RequestHeaderData::read(&mut retry_request, 2).expect("recovered produce header");
            assert_eq!(retry_header.request_api_key, ApiKey::Produce as i16);
            let retry =
                ProduceRequestData::read(&mut retry_request, retry_header.request_api_version)
                    .expect("recovered produce request");
            assert_eq!(retry.acks, -1);
            let topic_data = retry.topic_data.first().expect("topic produce data");
            let partition_data = topic_data
                .partition_data
                .first()
                .expect("partition produce data");
            let mut records = partition_data.records.clone().expect("records");
            let batch = RecordBatch::decode(&mut records).expect("record batch");
            assert_eq!(batch.producer_id, 42);
            assert_eq!(batch.producer_epoch, 4);
            assert_eq!(batch.base_sequence, 0);
            recovery_socket
                .write_all(&produce_response_frame(retry_header.correlation_id, 0, 41))
                .await
                .unwrap();
            6
        });
        Self { addr, join }
    }

    const fn addr(&self) -> std::net::SocketAddr {
        self.addr
    }

    async fn join(self) -> usize {
        self.join.await.unwrap()
    }
}

async fn read_frame(socket: &mut TcpStream) -> Bytes {
    let len = socket.read_i32().await.unwrap();
    let len = usize::try_from(len).unwrap();
    let mut bytes = vec![0; len];
    let _bytes_read = socket.read_exact(&mut bytes).await.unwrap();
    Bytes::from(bytes)
}
