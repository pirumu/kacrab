#![cfg(feature = "producer")]
//! Producer dispatcher integration tests.

#![allow(
    clippy::expect_used,
    clippy::missing_assert_message,
    clippy::unwrap_used,
    reason = "Integration test fixtures fail fastest with contextual unwrap/expect calls."
)]

use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use kacrab::{
    producer::{
        AccumulatorConfig, KafkaProducer, ProducerCompression, ProducerDispatcher,
        ProducerIdempotenceConfig, ProducerRecord, ProducerRuntimeConfig, RecordAccumulator,
    },
    wire::{BrokerEndpoint, ConnectionConfig, WireClient},
};
#[cfg(feature = "lz4")]
use kacrab_protocol::compression::Compression;
use kacrab_protocol::{
    KafkaString, KafkaUuid, frame,
    generated::{
        AddPartitionsToTxnRequestData, AddPartitionsToTxnResponseData, AddPartitionsToTxnResult,
        AddPartitionsToTxnTopicResult, ApiKey, ApiVersion, ApiVersionsResponseData,
        EndTxnRequestData, EndTxnResponseData, ErrorCode, FindCoordinatorRequestData,
        FindCoordinatorResponseData, InitProducerIdRequestData, InitProducerIdResponseData,
        MetadataResponseBroker, MetadataResponseData, MetadataResponsePartition,
        MetadataResponseTopic, PartitionProduceResponse, ProduceRequestData, ProduceResponseData,
        RequestHeaderData, ResponseHeaderData, TopicProduceResponse,
    },
    record::RecordBatch,
    version::response_header_version,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

const TOPIC_ID: KafkaUuid = KafkaUuid::from_parts(0x1111_2222_3333_4444, 0x5555_6666_7777_8888);

#[tokio::test]
async fn kafka_producer_send_buffers_until_flush() {
    let leader_7 = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::Produce as i16);
            let produce = ProduceRequestData::read(&mut request, 13).expect("produce request");
            assert_eq!(produce.topic_data.len(), 1);
            assert_eq!(produce.topic_data[0].topic_id, TOPIC_ID);
            assert_eq!(produce.topic_data[0].partition_data.len(), 1);
            assert_eq!(produce.topic_data[0].partition_data[0].index, 0);
            produce_response_frame(header.correlation_id, 0, 40)
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
    let mut producer = KafkaProducer::from_parts(
        wire,
        ProducerRuntimeConfig {
            accumulator: AccumulatorConfig::default()
                .batch_size(16 * 1024)
                .linger(Duration::from_mins(1))
                .buffer_memory(16 * 1024),
            acks: 1,
            timeout_ms: 30_000,
            retry_attempts: 0,
            delivery_timeout: Duration::from_mins(2),
            max_block: Duration::from_mins(1),
            partitioner_ignore_keys: false,
            max_in_flight_requests_per_connection: 5,
            compression: ProducerCompression::default(),
            idempotence: idempotence_disabled(),
        },
    );

    let delivery = producer
        .send(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")))
        .await
        .unwrap();
    assert!(producer.buffered_bytes() > 0);

    producer.flush().await.unwrap();
    let receipt = delivery.await.unwrap();

    assert_eq!(receipt.partition, 0);
    assert_eq!(receipt.leader_id, 7);
    assert_eq!(receipt.base_offset, 40);
    assert_eq!(producer.buffered_bytes(), 0);
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
            produce_response_frame(header.correlation_id, 0, 40)
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

    let mut producer = KafkaProducer::builder()
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
        .await
        .unwrap();

    producer.flush().await.unwrap();
    let receipt = delivery.await.unwrap();

    assert_eq!(receipt.base_offset, 40);
    assert_eq!(bootstrap.join().await, 2);
    assert_eq!(leader_7.join().await, 2);
}

#[tokio::test]
async fn kafka_producer_send_batch_returns_delivery_handles() {
    let leader_7 = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::Produce as i16);
            let produce = ProduceRequestData::read(&mut request, 13).expect("produce request");
            assert_eq!(produce.topic_data.len(), 1);
            assert_eq!(produce.topic_data[0].topic_id, TOPIC_ID);
            assert_eq!(produce.topic_data[0].partition_data.len(), 1);
            assert_eq!(produce.topic_data[0].partition_data[0].index, 0);
            produce_response_frame(header.correlation_id, 0, 40)
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
    let mut producer = KafkaProducer::from_parts(
        wire,
        ProducerRuntimeConfig {
            accumulator: AccumulatorConfig::default()
                .batch_size(1)
                .buffer_memory(16 * 1024),
            acks: 1,
            timeout_ms: 30_000,
            retry_attempts: 0,
            delivery_timeout: Duration::from_mins(2),
            max_block: Duration::from_mins(1),
            partitioner_ignore_keys: false,
            max_in_flight_requests_per_connection: 5,
            compression: ProducerCompression::default(),
            idempotence: idempotence_disabled(),
        },
    );

    let mut deliveries = producer
        .send_batch([
            ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")),
            ProducerRecord::new("orders", 0).value(Bytes::from_static(b"b")),
        ])
        .await
        .unwrap();

    assert_eq!(deliveries.len(), 2);
    producer.flush().await.unwrap();

    let second = deliveries.pop().expect("second delivery").await.unwrap();
    let first = deliveries.pop().expect("first delivery").await.unwrap();
    assert_eq!(first.base_offset, 40);
    assert_eq!(second.base_offset, 40);
    assert_eq!(producer.buffered_bytes(), 0);
    assert_eq!(bootstrap.join().await, 2);
    assert_eq!(leader_7.join().await, 2);
}

#[tokio::test]
async fn kafka_producer_send_batch_untracked_skips_delivery_handles() {
    let leader_7 = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::Produce as i16);
            let produce = ProduceRequestData::read(&mut request, 13).expect("produce request");
            assert_eq!(produce.topic_data.len(), 1);
            assert_eq!(produce.topic_data[0].topic_id, TOPIC_ID);
            assert_eq!(produce.topic_data[0].partition_data.len(), 1);
            assert_eq!(produce.topic_data[0].partition_data[0].index, 0);
            produce_response_frame(header.correlation_id, 0, 40)
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
    let mut producer = KafkaProducer::from_parts(
        wire,
        ProducerRuntimeConfig {
            accumulator: AccumulatorConfig::default()
                .batch_size(1)
                .buffer_memory(16 * 1024),
            acks: 1,
            timeout_ms: 30_000,
            retry_attempts: 0,
            delivery_timeout: Duration::from_mins(2),
            max_block: Duration::from_mins(1),
            partitioner_ignore_keys: false,
            max_in_flight_requests_per_connection: 5,
            compression: ProducerCompression::default(),
            idempotence: idempotence_disabled(),
        },
    );

    producer
        .send_batch_untracked([
            ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")),
            ProducerRecord::new("orders", 0).value(Bytes::from_static(b"b")),
        ])
        .await
        .unwrap();
    producer.flush().await.unwrap();

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
    let mut producer = KafkaProducer::from_parts(
        wire,
        ProducerRuntimeConfig {
            accumulator: AccumulatorConfig::default()
                .batch_size(1)
                .buffer_memory(16 * 1024),
            acks: 1,
            timeout_ms: 30_000,
            retry_attempts: 0,
            delivery_timeout: Duration::from_mins(2),
            max_block: Duration::from_mins(1),
            partitioner_ignore_keys: false,
            max_in_flight_requests_per_connection: 2,
            compression: ProducerCompression::default(),
            idempotence: idempotence_disabled(),
        },
    );

    let first_delivery = tokio::time::timeout(
        Duration::from_millis(200),
        producer.send(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a"))),
    )
    .await
    .expect("first send should not wait for broker ack")
    .unwrap();
    let second_delivery = tokio::time::timeout(
        Duration::from_millis(200),
        producer.send(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"b"))),
    )
    .await
    .expect("second send should not wait for broker ack")
    .unwrap();

    producer.flush().await.unwrap();
    let mut receipts = [
        first_delivery.await.unwrap(),
        second_delivery.await.unwrap(),
    ];
    receipts.sort_by_key(|receipt| receipt.base_offset);

    assert_eq!(receipts.len(), 2);
    assert_eq!(receipts[0].base_offset, 40);
    assert_eq!(receipts[1].base_offset, 41);
    assert_eq!(producer.buffered_bytes(), 0);
    assert_eq!(bootstrap.join().await, 2);
    assert_eq!(leader_7.join().await, 3);
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
    let mut producer = KafkaProducer::from_parts(
        wire,
        ProducerRuntimeConfig {
            accumulator: AccumulatorConfig::default()
                .batch_size(1)
                .buffer_memory(16 * 1024),
            acks: 1,
            timeout_ms: 30_000,
            retry_attempts: 0,
            delivery_timeout: Duration::from_mins(2),
            max_block: Duration::from_mins(1),
            partitioner_ignore_keys: false,
            max_in_flight_requests_per_connection: 5,
            compression: ProducerCompression::default(),
            idempotence: idempotence_disabled(),
        },
    );

    let _delivery = producer
        .send(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")))
        .await
        .unwrap();

    let error = producer.flush().await.unwrap_err();

    assert!(matches!(
        error,
        kacrab::producer::ProducerError::FlushIncomplete
    ));
    assert!(producer.buffered_bytes() > 0);
    assert_eq!(bootstrap.join().await, 2);
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
            let end = EndTxnRequestData::read(&mut request, 5).expect("end txn");
            assert_eq!(
                end.transactional_id,
                KafkaString::from("txn-orders".to_owned())
            );
            assert_eq!(end.producer_id, 77);
            assert_eq!(end.producer_epoch, 4);
            assert!(end.committed);
            end_txn_response_frame(header.correlation_id)
        }),
    ])
    .await;
    let leader_7 = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::Produce as i16);
            let produce = ProduceRequestData::read(&mut request, 13).expect("produce request");
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
            produce_response_frame(header.correlation_id, 0, 90)
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
    let mut producer = KafkaProducer::from_parts(
        wire,
        ProducerRuntimeConfig {
            accumulator: AccumulatorConfig::default()
                .batch_size(1)
                .buffer_memory(16 * 1024),
            acks: -1,
            timeout_ms: 30_000,
            retry_attempts: 0,
            delivery_timeout: Duration::from_mins(2),
            max_block: Duration::from_mins(1),
            partitioner_ignore_keys: false,
            max_in_flight_requests_per_connection: 5,
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
        .await
        .unwrap();
    producer.flush().await.unwrap();
    producer.commit_transaction().await.unwrap();

    assert_eq!(delivery.await.unwrap().base_offset, 90);
    assert_eq!(bootstrap.join().await, 3);
    assert_eq!(coordinator.join().await, 4);
    assert_eq!(leader_7.join().await, 2);
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
    let producer = KafkaProducer::from_parts(
        wire,
        ProducerRuntimeConfig {
            accumulator: AccumulatorConfig::default(),
            acks: -1,
            timeout_ms: 30_000,
            retry_attempts: 1,
            delivery_timeout: Duration::from_mins(2),
            max_block: Duration::from_mins(1),
            partitioner_ignore_keys: false,
            max_in_flight_requests_per_connection: 5,
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
async fn kafka_producer_commit_transaction_reports_end_txn_broker_error() {
    let coordinator = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::InitProducerId as i16);
            init_producer_id_response_frame(header.correlation_id, 77, 4)
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::EndTxn as i16);
            end_txn_error_response_frame(header.correlation_id, ErrorCode::InvalidTxnState)
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
    let mut producer = KafkaProducer::from_parts(
        wire,
        ProducerRuntimeConfig {
            accumulator: AccumulatorConfig::default(),
            acks: -1,
            timeout_ms: 30_000,
            retry_attempts: 0,
            delivery_timeout: Duration::from_mins(2),
            max_block: Duration::from_mins(1),
            partitioner_ignore_keys: false,
            max_in_flight_requests_per_connection: 5,
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

    assert!(matches!(
        producer.commit_transaction().await,
        Err(kacrab::producer::ProducerError::Transaction {
            operation: "end_txn",
            error: ErrorCode::InvalidTxnState
        })
    ));
    assert_eq!(bootstrap.join().await, 2);
    assert_eq!(coordinator.join().await, 3);
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
    let producer = KafkaProducer::from_parts(
        wire,
        ProducerRuntimeConfig {
            accumulator: AccumulatorConfig::default(),
            acks: -1,
            timeout_ms: 30_000,
            retry_attempts: 0,
            delivery_timeout: Duration::from_mins(2),
            max_block: Duration::from_mins(1),
            partitioner_ignore_keys: false,
            max_in_flight_requests_per_connection: 5,
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
    let producer = KafkaProducer::from_parts(
        wire,
        ProducerRuntimeConfig {
            accumulator: AccumulatorConfig::default(),
            acks: -1,
            timeout_ms: 30_000,
            retry_attempts: 0,
            delivery_timeout: Duration::from_mins(2),
            max_block: Duration::from_mins(1),
            partitioner_ignore_keys: false,
            max_in_flight_requests_per_connection: 5,
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
    let producer = KafkaProducer::from_parts(
        wire,
        ProducerRuntimeConfig {
            accumulator: AccumulatorConfig::default(),
            acks: -1,
            timeout_ms: 30_000,
            retry_attempts: 0,
            delivery_timeout: Duration::from_mins(2),
            max_block: Duration::from_mins(1),
            partitioner_ignore_keys: false,
            max_in_flight_requests_per_connection: 5,
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
            let produce = ProduceRequestData::read(&mut request, 13).expect("produce request");
            assert_eq!(produce.topic_data.len(), 1);
            assert_eq!(produce.topic_data[0].topic_id, TOPIC_ID);
            assert_eq!(produce.topic_data[0].partition_data.len(), 1);
            assert_eq!(produce.topic_data[0].partition_data[0].index, 0);
            produce_response_frame(header.correlation_id, 0, 40)
        }),
    ])
    .await;
    let leader_8 = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::Produce as i16);
            let produce = ProduceRequestData::read(&mut request, 13).expect("produce request");
            assert_eq!(produce.topic_data.len(), 1);
            assert_eq!(produce.topic_data[0].topic_id, TOPIC_ID);
            assert_eq!(produce.topic_data[0].partition_data.len(), 1);
            assert_eq!(produce.topic_data[0].partition_data[0].index, 1);
            produce_response_frame(header.correlation_id, 1, 80)
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
        ConnectionConfig::default(),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let dispatcher = ProducerDispatcher::new(wire);
    let mut accumulator = RecordAccumulator::new(
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

    let mut receipts = dispatcher
        .dispatch_ready(&mut accumulator, Instant::now())
        .await
        .unwrap();
    receipts.sort_by_key(|receipt| receipt.partition);

    assert_eq!(receipts.len(), 2);
    assert_eq!(receipts[0].partition, 0);
    assert_eq!(receipts[0].leader_id, 7);
    assert_eq!(receipts[0].base_offset, 40);
    assert_eq!(receipts[1].partition, 1);
    assert_eq!(receipts[1].leader_id, 8);
    assert_eq!(receipts[1].base_offset, 80);
    assert_eq!(accumulator.buffered_bytes(), 0);
    assert_eq!(bootstrap.join().await, 2);
    assert_eq!(leader_7.join().await, 2);
    assert_eq!(leader_8.join().await, 2);
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
    assert_eq!(first[0].base_offset, 40);
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].partition, 0);
    assert_eq!(second[0].leader_id, 7);
    assert_eq!(second[0].base_offset, 41);
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
            let produce = ProduceRequestData::read(&mut request, 13).expect("produce request");
            assert_eq!(produce.transactional_id, None);
            let mut records = produce.topic_data[0].partition_data[0]
                .records
                .clone()
                .expect("records");
            let batch = RecordBatch::decode(&mut records).expect("record batch");
            assert_eq!(batch.producer_id, 42);
            assert_eq!(batch.producer_epoch, 3);
            assert_eq!(batch.base_sequence, 0);
            produce_response_frame(header.correlation_id, 0, 40)
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::Produce as i16);
            let produce = ProduceRequestData::read(&mut request, 13).expect("produce request");
            let mut records = produce.topic_data[0].partition_data[0]
                .records
                .clone()
                .expect("records");
            let batch = RecordBatch::decode(&mut records).expect("record batch");
            assert_eq!(batch.producer_id, 42);
            assert_eq!(batch.producer_epoch, 3);
            assert_eq!(batch.base_sequence, 1);
            produce_response_frame(header.correlation_id, 0, 41)
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
            delivery_timeout: Duration::from_mins(2),
            max_block: Duration::from_mins(1),
            partitioner_ignore_keys: false,
            max_in_flight_requests_per_connection: 5,
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

    assert_eq!(first[0].base_offset, 40);
    assert_eq!(second[0].base_offset, 41);
    assert_eq!(bootstrap.join().await, 2);
    assert_eq!(leader_7.join().await, 4);
}

#[tokio::test]
#[cfg(feature = "lz4")]
async fn dispatcher_sends_compressed_record_batches_from_runtime_config() {
    let leader_7 = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::Produce as i16);
            let produce = ProduceRequestData::read(&mut request, 13).expect("produce request");
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
            produce_response_frame(header.correlation_id, 0, 40)
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
            delivery_timeout: Duration::from_mins(2),
            max_block: Duration::from_mins(1),
            partitioner_ignore_keys: false,
            max_in_flight_requests_per_connection: 5,
            compression: ProducerCompression {
                codec: Compression::Lz4,
                level: Some(9),
            },
            idempotence: idempotence_disabled(),
        },
    );
    let mut accumulator = RecordAccumulator::new(
        AccumulatorConfig::default()
            .batch_size(1)
            .buffer_memory(16 * 1024),
    );
    accumulator
        .append(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"compressed-value")))
        .unwrap();

    let receipts = dispatcher
        .dispatch_ready(&mut accumulator, Instant::now())
        .await
        .unwrap();

    assert_eq!(receipts.len(), 1);
    assert_eq!(receipts[0].base_offset, 40);
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
            produce_error_response_frame(header.correlation_id, 0, ErrorCode::NotLeaderOrFollower)
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
    let mut accumulator = RecordAccumulator::new(
        AccumulatorConfig::default()
            .batch_size(1)
            .buffer_memory(16 * 1024),
    );
    accumulator
        .append(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")))
        .unwrap();

    let error = dispatcher
        .dispatch_ready(&mut accumulator, Instant::now())
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
            produce_error_response_frame(header.correlation_id, 0, ErrorCode::NotLeaderOrFollower)
        }),
    ])
    .await;
    let leader_8 = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::Produce as i16);
            let produce = ProduceRequestData::read(&mut request, 13).expect("produce request");
            assert_eq!(produce.topic_data.len(), 1);
            assert_eq!(produce.topic_data[0].partition_data.len(), 1);
            assert_eq!(produce.topic_data[0].partition_data[0].index, 0);
            produce_response_frame(header.correlation_id, 0, 88)
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
    let mut accumulator = RecordAccumulator::new(
        AccumulatorConfig::default()
            .batch_size(1)
            .buffer_memory(16 * 1024),
    );
    accumulator
        .append(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")))
        .unwrap();

    let receipts = dispatcher
        .dispatch_ready(&mut accumulator, Instant::now())
        .await
        .unwrap();

    assert_eq!(receipts.len(), 1);
    assert_eq!(receipts[0].leader_id, 8);
    assert_eq!(receipts[0].base_offset, 88);
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
    let mut accumulator = RecordAccumulator::new(
        AccumulatorConfig::default()
            .batch_size(1)
            .buffer_memory(16 * 1024),
    );
    accumulator
        .append(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")))
        .unwrap();

    let receipts = dispatcher
        .dispatch_ready(&mut accumulator, Instant::now())
        .await
        .unwrap();

    assert!(receipts.is_empty());
    assert!(accumulator.buffered_bytes() > 0);
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
    let mut accumulator = RecordAccumulator::new(
        AccumulatorConfig::default()
            .batch_size(usize::MAX)
            .buffer_memory(16 * 1024),
    );
    accumulator
        .append(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")))
        .unwrap();

    let error = dispatcher.dispatch_all(&mut accumulator).await.unwrap_err();

    assert!(matches!(
        error,
        kacrab::producer::ProducerError::FlushIncomplete
    ));
    assert!(accumulator.buffered_bytes() > 0);
    assert_eq!(bootstrap.join().await, 2);
}

#[tokio::test]
async fn dispatcher_owned_batches_report_leadership_error_without_retry() {
    let leader_7 = MockBroker::serve_many(vec![
        Box::new(api_versions_response_frame),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::Produce as i16);
            produce_error_response_frame(header.correlation_id, 0, ErrorCode::NotLeaderOrFollower)
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
    let mut accumulator = RecordAccumulator::new(
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
        .dispatch_ready(&mut accumulator, dispatch_at)
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
                    api_key: ApiKey::EndTxn as i16,
                    min_version: 0,
                    max_version: 5,
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

fn ready_batches_for_value(
    value: &'static [u8],
    now: Instant,
) -> Vec<kacrab::producer::ReadyBatch> {
    let mut accumulator = RecordAccumulator::new(
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

fn produce_response_frame(correlation_id: i32, partition: i32, base_offset: i64) -> BytesMut {
    let response = ProduceResponseData {
        responses: vec![TopicProduceResponse {
            topic_id: TOPIC_ID,
            partition_responses: vec![PartitionProduceResponse {
                index: partition,
                error_code: 0,
                base_offset,
                log_append_time_ms: -1,
                log_start_offset: base_offset,
                ..PartitionProduceResponse::default()
            }],
            ..TopicProduceResponse::default()
        }],
        ..ProduceResponseData::default()
    };
    response_frame(ApiKey::Produce, 13, correlation_id, &response)
}

fn produce_error_response_frame(correlation_id: i32, partition: i32, error: ErrorCode) -> BytesMut {
    let response = ProduceResponseData {
        responses: vec![TopicProduceResponse {
            topic_id: TOPIC_ID,
            partition_responses: vec![PartitionProduceResponse {
                index: partition,
                error_code: error.code(),
                base_offset: -1,
                log_append_time_ms: -1,
                log_start_offset: -1,
                ..PartitionProduceResponse::default()
            }],
            ..TopicProduceResponse::default()
        }],
        ..ProduceResponseData::default()
    };
    response_frame(ApiKey::Produce, 13, correlation_id, &response)
}

fn init_producer_id_response_frame(
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
    response_frame(ApiKey::InitProducerId, 5, correlation_id, &response)
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

fn end_txn_response_frame(correlation_id: i32) -> BytesMut {
    let response = EndTxnResponseData {
        error_code: 0,
        producer_id: 77,
        producer_epoch: 4,
        ..EndTxnResponseData::default()
    };
    response_frame(ApiKey::EndTxn, 5, correlation_id, &response)
}

fn end_txn_error_response_frame(correlation_id: i32, error: ErrorCode) -> BytesMut {
    let response = EndTxnResponseData {
        error_code: error.code(),
        producer_id: 77,
        producer_epoch: 4,
        ..EndTxnResponseData::default()
    };
    response_frame(ApiKey::EndTxn, 5, correlation_id, &response)
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

impl WriteResponse for EndTxnResponseData {
    fn write_response(&self, buf: &mut BytesMut, version: i16) {
        self.write(buf, version).expect("end txn response");
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
                let produce = ProduceRequestData::read(&mut request, 13).expect("produce request");
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
