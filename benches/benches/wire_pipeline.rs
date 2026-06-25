//! Wire request pipeline benchmarks over a local mock broker.

#![allow(
    clippy::expect_used,
    clippy::missing_assert_message,
    missing_docs,
    reason = "Benchmark fixtures fail fastest; Criterion macros generate public entrypoints."
)]

use std::time::Duration;

use bytes::{Bytes, BytesMut};
use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};
use kacrab::wire::{BrokerEndpoint, ConnectionConfig, WireClient};
use kacrab_protocol::{
    KafkaString, frame,
    generated::{
        ApiKey, ApiVersion, ApiVersionsRequestData, ApiVersionsResponseData, RequestHeaderData,
        ResponseHeaderData,
    },
    version::response_header_version,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    runtime::Builder,
};

const REQUESTS: u64 = 1_024;

fn bench_wire_pipeline(c: &mut Criterion) {
    let runtime = Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
        .expect("benchmark runtime");
    let mut group = c.benchmark_group("wire_pipeline");
    let _group = group.throughput(Throughput::Elements(REQUESTS));
    let _group = group.measurement_time(Duration::from_secs(15));
    let _group = group.bench_function("api_versions_send_to_broker", |b| {
        b.to_async(&runtime).iter_custom(|iters| async move {
            let started = std::time::Instant::now();
            for _ in 0..iters {
                run_pipeline_once(usize::try_from(REQUESTS).unwrap_or(usize::MAX)).await;
            }
            started.elapsed()
        });
    });
    group.finish();
}

async fn run_pipeline_once(requests: usize) {
    let server = MockBroker::serve_pipelined_api_versions(requests).await;
    let client = WireClient::connect_with_brokers(
        ConnectionConfig::default()
            .max_in_flight_requests_per_connection(requests)
            .broker_queue_capacity(requests)
            .request_timeout(Duration::from_secs(30))
            .read_buffer_capacity(4096)
            .buffer_pool_capacity(128),
        "kacrab-bench",
        [BrokerEndpoint::new(7, server.addr())],
    );
    let request = ApiVersionsRequestData {
        client_software_name: KafkaString::from("kacrab".to_owned()),
        client_software_version: KafkaString::from("0.0.1".to_owned()),
        _unknown_tagged_fields: Vec::new(),
    };
    let mut tasks = Vec::with_capacity(requests);
    for _ in 0..requests {
        let client = client.clone();
        let request = request.clone();
        tasks.push(tokio::spawn(async move {
            client
                .send_to_broker::<_, ApiVersionsResponseData>(7, ApiKey::ApiVersions, 3, &request)
                .await
        }));
    }
    for task in tasks {
        let response = task
            .await
            .expect("pipeline task should join")
            .expect("pipeline request should succeed");
        let _response = black_box(response);
    }
    let handled = server.join().await;
    debug_assert_eq!(handled, requests.saturating_add(1));
}

struct MockBroker {
    addr: std::net::SocketAddr,
    join: tokio::task::JoinHandle<usize>,
}

impl MockBroker {
    async fn serve_pipelined_api_versions(requests: usize) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock broker");
        let addr = listener.local_addr().expect("mock broker addr");
        let join = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept mock client");
            let handshake = read_frame(&mut socket).await;
            let response = api_versions_response(handshake);
            socket.write_all(&response).await.expect("write handshake");

            let mut correlation_ids = Vec::with_capacity(requests);
            for _ in 0..requests {
                let mut request = read_frame(&mut socket).await;
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                correlation_ids.push(header.correlation_id);
            }
            for correlation_id in correlation_ids {
                let response = ApiVersionsResponseData {
                    error_code: 0,
                    api_keys: vec![ApiVersion {
                        api_key: ApiKey::ApiVersions as i16,
                        min_version: 0,
                        max_version: 4,
                        _unknown_tagged_fields: Vec::new(),
                    }],
                    ..ApiVersionsResponseData::default()
                };
                let frame = response_frame(ApiKey::ApiVersions, 3, correlation_id, &response);
                socket.write_all(&frame).await.expect("write response");
            }
            requests.saturating_add(1)
        });
        Self { addr, join }
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
        api_keys: vec![ApiVersion {
            api_key: ApiKey::ApiVersions as i16,
            min_version: 0,
            max_version: 4,
            _unknown_tagged_fields: Vec::new(),
        }],
        ..ApiVersionsResponseData::default()
    };
    response_frame(ApiKey::ApiVersions, 3, header.correlation_id, &response)
}

fn response_frame(
    api_key: ApiKey,
    api_version: i16,
    correlation_id: i32,
    response: &ApiVersionsResponseData,
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
    response
        .write(&mut body, api_version)
        .expect("response body");
    frame::encode_request(&header, &body).expect("response frame")
}

async fn read_frame(socket: &mut TcpStream) -> Bytes {
    let len = socket.read_i32().await.expect("frame length");
    let len = usize::try_from(len).expect("positive frame length");
    let mut bytes = vec![0; len];
    let _bytes_read = socket.read_exact(&mut bytes).await.expect("frame payload");
    Bytes::from(bytes)
}

criterion_group!(benches, bench_wire_pipeline);
criterion_main!(benches);
