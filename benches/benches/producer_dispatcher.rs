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
use kacrab_protocol::{
    KafkaString, KafkaUuid, frame,
    generated::{
        ApiKey, ApiVersion, ApiVersionsResponseData, MetadataResponseBroker, MetadataResponseData,
        MetadataResponsePartition, MetadataResponseTopic, PartitionProduceResponse,
        ProduceRequestData, ProduceResponseData, RequestHeaderData, ResponseHeaderData,
        TopicProduceResponse,
    },
    version::response_header_version,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    runtime::Builder,
};

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
        MockBroker::serve_produce_leader(7).await,
        MockBroker::serve_produce_leader(8).await,
        MockBroker::serve_produce_leader(9).await,
        MockBroker::serve_produce_leader(10).await,
    ];
    let bootstrap = MockBroker::serve_bootstrap([
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

struct MockBroker {
    addr: std::net::SocketAddr,
    join: tokio::task::JoinHandle<usize>,
}

impl MockBroker {
    async fn serve_bootstrap<const N: usize>(brokers: [(i32, std::net::SocketAddr); N]) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind bootstrap");
        let addr = listener.local_addr().expect("bootstrap addr");
        let join = tokio::spawn(async move {
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
        });
        Self { addr, join }
    }

    async fn serve_produce_leader(node_id: i32) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind leader");
        let addr = listener.local_addr().expect("leader addr");
        let join = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept leader");
            let handshake = read_frame(&mut socket)
                .await
                .expect("leader handshake frame");
            socket
                .write_all(&api_versions_response(handshake))
                .await
                .expect("write leader handshake");
            // Serve every produce request the client sends until it disconnects.
            // The dispatcher decides how many requests each drain becomes, so a
            // fixed count would race the actual pipelining shape.
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
        });
        Self { addr, join }
    }

    fn abort(self) {
        self.join.abort();
    }

    const fn addr(&self) -> std::net::SocketAddr {
        self.addr
    }

    async fn join(self) -> usize {
        self.join.await.expect("mock broker join")
    }
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

async fn read_frame(socket: &mut TcpStream) -> Option<Bytes> {
    // Returns None once the client disconnects (clean EOF on the length prefix),
    // so serve loops terminate instead of panicking on the closed socket.
    let len = socket.read_i32().await.ok()?;
    let len = usize::try_from(len).expect("positive frame length");
    let mut bytes = vec![0; len];
    let _bytes_read = socket.read_exact(&mut bytes).await.expect("frame payload");
    Some(Bytes::from(bytes))
}

criterion_group!(benches, bench_producer_dispatcher);
criterion_main!(benches);
