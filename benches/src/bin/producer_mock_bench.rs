//! Mock-broker producer benchmark through the dispatcher hot path.

#![allow(
    clippy::arithmetic_side_effects,
    clippy::cast_possible_truncation,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::print_stdout,
    clippy::unwrap_used,
    missing_docs,
    reason = "Benchmark fixtures prefer direct fail-fast setup and explicit output."
)]

use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
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
    sync::watch,
};

const PARTITIONS: usize = 3;
const TOPIC_ID: KafkaUuid = KafkaUuid::from_parts(0x1111_2222_3333_4444, 0x5555_6666_7777_8888);

fn main() {
    let runtime = Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
        .expect("benchmark runtime");
    runtime.block_on(async {
        run_scenario(Scenario {
            name: "5,000,000 messages x 10 bytes",
            messages: 5_000_000,
            value_size: 10,
            batch_messages: 16_384,
        })
        .await;
        run_scenario(Scenario {
            name: "100,000 messages x 10 KiB",
            messages: 100_000,
            value_size: 10 * 1024,
            batch_messages: 96,
        })
        .await;
    });
}

#[derive(Debug, Clone, Copy)]
struct Scenario {
    name: &'static str,
    messages: usize,
    value_size: usize,
    batch_messages: usize,
}

async fn run_scenario(scenario: Scenario) {
    let outer_chunks = scenario
        .messages
        .checked_add(scenario.batch_messages.saturating_sub(1))
        .expect("scenario chunk count should not overflow")
        / scenario.batch_messages;
    let broker = MockBroker::serve().await;
    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default()
            .max_in_flight_requests_per_connection(1)
            .broker_queue_capacity(1)
            .request_timeout(Duration::from_secs(30))
            .read_buffer_capacity(1024 * 1024)
            .buffer_pool_capacity(128),
        "kacrab-producer-mock-bench",
        [BrokerEndpoint::new(1, broker.addr())],
    );
    let dispatcher = ProducerDispatcher::new(wire);
    dispatcher.enable_metrics();
    let value = Bytes::from(vec![b'x'; scenario.value_size]);
    let started = Instant::now();
    let mut sent = 0usize;
    while sent < scenario.messages {
        let batch_messages = scenario
            .batch_messages
            .min(scenario.messages.saturating_sub(sent));
        let accumulator = SharedAccumulator::with_config(
            AccumulatorConfig::default()
                .batch_size(16 * 1024)
                .buffer_memory(batch_buffer_memory(batch_messages, scenario.value_size)),
        );
        let now = Instant::now();
        for index in 0..batch_messages {
            let partition = i32::try_from((sent + index) % PARTITIONS).unwrap_or_default();
            accumulator
                .append_at(
                    ProducerRecord::new("orders", partition).value(value.clone()),
                    now,
                )
                .expect("benchmark append should fit");
        }
        let receipts = dispatcher
            .dispatch_ready(&accumulator, now)
            .await
            .expect("benchmark dispatch should succeed");
        assert!(
            !receipts.is_empty(),
            "mock broker should acknowledge at least one partition per produce request"
        );
        sent = sent.saturating_add(batch_messages);
    }
    let elapsed = started.elapsed();
    let metrics = dispatcher.metrics();
    let handled = broker.join().await;
    assert_eq!(
        handled,
        usize::try_from(metrics.produce_request_count)
            .expect("benchmark produce request count should fit")
            .saturating_add(2),
        "mock broker should handle handshake, metadata, and every broker produce request"
    );
    print_result(
        scenario,
        elapsed,
        outer_chunks,
        metrics.produce_request_count,
    );
}

fn batch_buffer_memory(batch_messages: usize, value_size: usize) -> usize {
    batch_messages
        .checked_mul(value_size.saturating_add(128))
        .and_then(|bytes| bytes.checked_add(1024 * 1024))
        .expect("scenario buffer memory should not overflow")
}

fn print_result(
    scenario: Scenario,
    elapsed: Duration,
    outer_chunks: usize,
    broker_produce_requests: u64,
) {
    let seconds = elapsed.as_secs_f64();
    let messages_u32 =
        u32::try_from(scenario.messages).expect("scenario message count should fit in u32");
    let messages_per_second = f64::from(messages_u32) / seconds;
    let megabytes = scenario
        .messages
        .checked_mul(scenario.value_size)
        .and_then(|bytes| u32::try_from(bytes).ok())
        .map(|bytes| f64::from(bytes) / (1024.0 * 1024.0))
        .expect("scenario bytes should not overflow");
    let megabytes_per_second = megabytes / seconds;
    println!(
        "{}: {:.0} messages/s, {:.3} MiB/s ({:.3}s, {} API chunks, {} mock broker requests)",
        scenario.name,
        messages_per_second,
        megabytes_per_second,
        seconds,
        outer_chunks,
        broker_produce_requests
    );
}

struct MockBroker {
    addr: std::net::SocketAddr,
    stop: watch::Sender<bool>,
    join: tokio::task::JoinHandle<usize>,
}

impl MockBroker {
    async fn serve() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind broker");
        let addr = listener.local_addr().expect("broker addr");
        let (stop, mut stop_rx) = watch::channel(false);
        let join = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept broker");
            let handshake = read_frame(&mut socket).await;
            socket
                .write_all(&api_versions_response(handshake))
                .await
                .expect("write handshake");
            let mut request = read_frame(&mut socket).await;
            let header = RequestHeaderData::read(&mut request, 2).expect("metadata header");
            socket
                .write_all(&response_frame(
                    ApiKey::Metadata,
                    13,
                    header.correlation_id,
                    &metadata_response(addr),
                ))
                .await
                .expect("write metadata");
            let mut produce_requests = 0usize;
            loop {
                tokio::select! {
                    result = stop_rx.changed() => {
                        result.expect("benchmark stop channel should stay open");
                        if *stop_rx.borrow() {
                            break;
                        }
                    },
                    request = read_frame(&mut socket) => {
                        let mut request = request;
                        let header = RequestHeaderData::read(&mut request, 2).expect("produce header");
                        let produce = ProduceRequestData::read(&mut request, 13).expect("produce request");
                        socket
                            .write_all(&response_frame(
                                ApiKey::Produce,
                                13,
                                header.correlation_id,
                                &produce_response(&produce),
                            ))
                            .await
                            .expect("write produce response");
                        produce_requests = produce_requests.saturating_add(1);
                    },
                }
            }
            produce_requests.saturating_add(2)
        });
        Self { addr, stop, join }
    }

    const fn addr(&self) -> std::net::SocketAddr {
        self.addr
    }

    async fn join(self) -> usize {
        self.stop
            .send(true)
            .expect("benchmark broker stop signal should send");
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

fn metadata_response(addr: std::net::SocketAddr) -> MetadataResponseData {
    MetadataResponseData {
        brokers: vec![MetadataResponseBroker {
            node_id: 1,
            host: KafkaString::from(addr.ip().to_string()),
            port: i32::from(addr.port()),
            rack: None,
            _unknown_tagged_fields: Vec::new(),
        }],
        topics: vec![MetadataResponseTopic {
            error_code: 0,
            name: Some(KafkaString::from("orders".to_owned())),
            topic_id: TOPIC_ID,
            partitions: (0..PARTITIONS)
                .map(|partition| MetadataResponsePartition {
                    error_code: 0,
                    partition_index: i32::try_from(partition).unwrap_or_default(),
                    leader_id: 1,
                    leader_epoch: 3,
                    replica_nodes: vec![1],
                    isr_nodes: vec![1],
                    offline_replicas: Vec::new(),
                    _unknown_tagged_fields: Vec::new(),
                })
                .collect(),
            ..MetadataResponseTopic::default()
        }],
        ..MetadataResponseData::default()
    }
}

fn produce_response(request: &ProduceRequestData) -> ProduceResponseData {
    ProduceResponseData {
        responses: request
            .topic_data
            .iter()
            .map(|topic| TopicProduceResponse {
                topic_id: topic.topic_id,
                partition_responses: topic
                    .partition_data
                    .iter()
                    .map(|partition| PartitionProduceResponse {
                        index: partition.index,
                        error_code: 0,
                        base_offset: 0,
                        log_append_time_ms: -1,
                        log_start_offset: 0,
                        ..PartitionProduceResponse::default()
                    })
                    .collect(),
                ..TopicProduceResponse::default()
            })
            .collect(),
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

async fn read_frame(socket: &mut TcpStream) -> Bytes {
    let len = socket.read_i32().await.expect("frame length");
    let len = usize::try_from(len).expect("positive frame length");
    let mut bytes = vec![0; len];
    let _bytes_read = socket.read_exact(&mut bytes).await.expect("frame body");
    Bytes::from(bytes)
}
