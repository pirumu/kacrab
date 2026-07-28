//! Multi-broker producer dispatcher throughput benchmark over local mock brokers.

#![allow(
    clippy::arithmetic_side_effects,
    clippy::expect_used,
    clippy::indexing_slicing,
    missing_docs,
    reason = "Benchmark fixtures fail fastest; Criterion macros generate public entrypoints."
)]

use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};
use kacrab::{
    producer::{
        ProducerRecord,
        internals::{AccumulatorConfig, ProducerDispatcher, SharedAccumulator},
    },
    wire::{BrokerEndpoint, ConnectionConfig, WireClient},
};
use kacrab_benches::{MockBroker, read_frame, response_frame};
use kacrab_protocol::{
    KafkaString, KafkaUuid,
    generated::{
        ApiKey, ApiVersion, ApiVersionsResponseData, MetadataResponseBroker, MetadataResponseData,
        MetadataResponsePartition, MetadataResponseTopic, PartitionProduceResponse,
        ProduceRequestData, ProduceResponseData, RequestHeaderData, TopicProduceResponse,
    },
};
use tokio::{io::AsyncWriteExt, runtime::Builder};

const BROKERS: usize = 4;
const RECORDS_PER_ITERATION: u64 = 16_384;
const TOPIC_ID: KafkaUuid = KafkaUuid::from_parts(0x1111_2222_3333_4444, 0x5555_6666_7777_8888);

fn bench_producer_dispatcher(c: &mut Criterion) {
    let runtime = Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
        .expect("benchmark runtime");
    let mut group = c.benchmark_group("producer_dispatcher");
    let _group = group.throughput(Throughput::Elements(RECORDS_PER_ITERATION));
    let _group = group.measurement_time(Duration::from_secs(15));
    let _group = group.bench_function("multi_broker_dispatch", |b| {
        b.to_async(&runtime).iter_custom(|iters| async move {
            let started = Instant::now();
            run_dispatcher_sample(usize::try_from(iters).unwrap_or(usize::MAX)).await;
            started.elapsed()
        });
    });
    group.finish();
}

async fn run_dispatcher_sample(iterations: usize) {
    let leaders = [
        serve_produce_leader(7).await,
        serve_produce_leader(8).await,
        serve_produce_leader(9).await,
        serve_produce_leader(10).await,
    ];
    let bootstrap = serve_bootstrap([
        (7, leaders[0].addr()),
        (8, leaders[1].addr()),
        (9, leaders[2].addr()),
        (10, leaders[3].addr()),
    ])
    .await;
    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default()
            .max_in_flight_requests_per_connection(BROKERS)
            .broker_queue_capacity(BROKERS)
            .request_timeout(Duration::from_secs(30))
            .read_buffer_capacity(4096)
            .buffer_pool_capacity(128),
        "kacrab-bench",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    // Match the dispatcher's in-flight cap to the wire client above (BROKERS);
    // a larger dispatcher cap would over-enqueue and hit WireError::Backpressure.
    let dispatcher = ProducerDispatcher::new(wire).max_in_flight_requests_per_connection(BROKERS);
    let records = records_for_iteration();
    for _ in 0..iterations {
        let accumulator = SharedAccumulator::with_config(
            AccumulatorConfig::default()
                .batch_size(1)
                .buffer_memory(128 * 1024 * 1024),
        );
        let now = Instant::now();
        for record in records.iter().cloned() {
            accumulator
                .append_at(record, now)
                .expect("benchmark append should fit");
        }
        let receipts = dispatcher
            .dispatch_ready(&accumulator, now)
            .await
            .expect("benchmark dispatch should succeed");
        let _receipts = black_box(receipts);
    }
    let _bootstrap_handled = bootstrap.join().await;
    // The leaders serve produce responses until the client stops sending; the
    // client is done, so abort them rather than joining (the wire keeps the
    // sockets open, so a join would block on a read that never returns EOF).
    for leader in leaders {
        leader.abort();
    }
}

fn records_for_iteration() -> Vec<ProducerRecord> {
    (0..RECORDS_PER_ITERATION)
        .map(|index| {
            let partition =
                i32::try_from(index % u64::try_from(BROKERS).unwrap_or(1)).unwrap_or_default();
            ProducerRecord::new("orders", partition)
                .key(Bytes::from_static(b"customer-42"))
                .value(Bytes::from_static(b"created"))
        })
        .collect()
}

/// Answers the handshake and one `Metadata` request naming `brokers`, then stops.
async fn serve_bootstrap<const N: usize>(brokers: [(i32, std::net::SocketAddr); N]) -> MockBroker {
    MockBroker::serve_with(move |listener| async move {
        let (mut socket, _) = listener.accept().await.expect("accept bootstrap");
        let handshake = read_frame(&mut socket)
            .await
            .expect("bootstrap handshake frame");
        socket
            .write_all(&api_versions_response(handshake))
            .await
            .expect("write bootstrap handshake");
        let mut request = read_frame(&mut socket)
            .await
            .expect("bootstrap metadata frame");
        let header = RequestHeaderData::read(&mut request, 2).expect("metadata header");
        let response = metadata_response(brokers);
        socket
            .write_all(&response_frame(
                ApiKey::Metadata,
                13,
                header.correlation_id,
                &response,
            ))
            .await
            .expect("write metadata");
        2
    })
    .await
}

/// Serves every produce request the client sends until it disconnects.
///
/// The dispatcher decides how many requests each drain becomes, so a fixed count
/// would race the actual pipelining shape.
async fn serve_produce_leader(node_id: i32) -> MockBroker {
    MockBroker::serve_with(move |listener| async move {
        let (mut socket, _) = listener.accept().await.expect("accept leader");
        let handshake = read_frame(&mut socket)
            .await
            .expect("leader handshake frame");
        socket
            .write_all(&api_versions_response(handshake))
            .await
            .expect("write leader handshake");
        let mut served = 0usize;
        while let Some(mut request) = read_frame(&mut socket).await {
            let header = RequestHeaderData::read(&mut request, 2).expect("produce header");
            let produce = ProduceRequestData::read(&mut request, 13).expect("produce request");
            let partition = produce.topic_data[0].partition_data[0].index;
            let response = produce_response(partition, i64::from(node_id));
            socket
                .write_all(&response_frame(
                    ApiKey::Produce,
                    13,
                    header.correlation_id,
                    &response,
                ))
                .await
                .expect("write produce response");
            served = served.saturating_add(1);
        }
        served
    })
    .await
}

fn api_versions_response(mut request: Bytes) -> BytesMut {
    let header = RequestHeaderData::read(&mut request, 2).expect("request header");
    let response = ApiVersionsResponseData {
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
        ],
        ..ApiVersionsResponseData::default()
    };
    response_frame(ApiKey::ApiVersions, 3, header.correlation_id, &response)
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
            partitions: (0..BROKERS)
                .map(|partition| {
                    let leader_id = 7 + i32::try_from(partition).unwrap_or_default();
                    MetadataResponsePartition {
                        error_code: 0,
                        partition_index: i32::try_from(partition).unwrap_or_default(),
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

fn produce_response(partition: i32, base_offset: i64) -> ProduceResponseData {
    ProduceResponseData {
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
    }
}

criterion_group!(benches, bench_producer_dispatcher);
criterion_main!(benches);
