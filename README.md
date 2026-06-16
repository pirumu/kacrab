# Kacrab

A high-performance Kafka client for Rust, built in pure Rust from the Kafka
protocol up.

* **Java-compatible auth and producer**: the authentication and producer
  surfaces use Java client property names, defaults, protocol flow, and wire
  semantics as the compatibility target.
* **Pure Rust runtime**: no `librdkafka`, no C client bindings, and workspace
  `unsafe_code` is forbidden.
* **Generated protocol**: Kafka request/response structs are generated from
  Apache Kafka schemas and checked against the Kafka Java client oracle.
* **Throughput-focused producer**: batching, linger, bounded memory,
  idempotence, transactions, compression, metadata routing, and multi-broker
  dispatch are first-class design points.
* **Tokio-native wire layer**: async broker sessions, ApiVersions negotiation,
  metadata refresh, bounded in-flight requests, request timeouts, and explicit
  connection cleanup.

[![MIT licensed][mit-badge]][mit-url]
[![Apache-2.0 licensed][apache-badge]][apache-url]

[mit-badge]: https://img.shields.io/badge/license-MIT-blue.svg
[mit-url]: LICENSE-MIT
[apache-badge]: https://img.shields.io/badge/license-Apache--2.0-blue.svg
[apache-url]: LICENSE-APACHE

## Status

> Warning: `kacrab` is pre-release software. It has useful protocol, wire,
> auth, and producer coverage, but the public API and runtime behavior are not
> stable release guarantees yet.

Protocol, wire, auth, and producer now have a usable baseline. The current
focus remains wire + producer hardening before consumer work: multi-broker
behavior, bounded hot paths, routing refresh, stress testing, and benchmarks
that make the 3M messages/sec target measurable.

Auth and producer are treated as **100% Java-compatible targets** for the
implemented surface:

- Java-style config keys work through `ClientConfig` and
  `KafkaProducer::builder().set(...)`.
- `security.protocol`, TLS, SASL, idempotence, transactions, batching, request
  timeout, delivery timeout, compression, and in-flight limits map through the
  same Kafka property names users know from the Java client.
- Generated request/response encoding uses `kacrab-protocol`, not handwritten
  byte patches.
- JVM-only callback handler classes cannot be loaded inside Rust; use the
  native Rust SASL authenticator hook for custom auth flows.

## Current Status

- [x] Protocol foundation
  - [x] Kafka protocol primitives, record batches, compression codecs, and
        generated request/response structs.
  - [x] Apache Kafka 4.3.0 schema snapshots and Java oracle compatibility
        matrix for generated wire messages.
- [x] Core config and auth foundation
  - [x] Java-style configuration facade and typed client configs.
  - [x] TLS/SASL properties for `SSL`, `SASL_SSL`, and `SASL_PLAINTEXT`.
  - [x] SASL `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`, `OAUTHBEARER`,
        feature-gated `GSSAPI`, and native Rust custom auth hooks.
- [x] Wire usable baseline
  - [x] Broker sessions with TCP/TLS/SASL, ApiVersions negotiation, request
        encoding, response dispatch, and metadata fetch.
  - [x] Bounded in-flight requests, request timeouts, connection-closed cleanup,
        broker dispatch, and per-session write-buffer reuse.
  - [ ] Production hardening: lower-allocation correlation storage, leadership
        error invalidation, reconnect/backoff policy, and sustained multi-broker
        stress tests.
- [x] Producer usable baseline
  - [x] Public `KafkaProducer` API with Java-style config keys.
  - [x] Batching by topic-partition, linger, bounded memory, `max.block.ms`,
        compression hooks, and delivery handles.
  - [x] Metadata routing with default partition assignment, keyed murmur2
        partitioning, round-robin unkeyed assignment, and multi-broker dispatch.
  - [x] Retry backoff, delivery timeout across retries, broker response error
        propagation, and leadership-error retry path.
  - [x] Idempotent producer identity/sequence fields and transactional control
        flow through coordinator lookup, `InitProducerId`, `AddPartitionsToTxn`,
        and `EndTxn`.
  - [ ] Production acceptance: sustained stress, memory soak, leadership-change
        refresh coverage, latency percentiles, and 3M messages/sec benchmark
        gates on realistic batching and multi-broker workloads.
- [ ] Consumer
  - [ ] Manual assignment, fetch, offsets, and committed offset handling.
  - [ ] Group coordination: join, sync, heartbeat, rebalance, and offset commit.
  - [ ] Backpressure and multi-broker fetch scheduling shaped for the existing
        wire reactor.
- [ ] Admin
  - [ ] Topic, partition, ACL, config, and cluster metadata operations.
  - [ ] Java-style admin config facade wired through the same auth/transport
        stack.
- [ ] Streams
  - [ ] Topology API, processor runtime, repartitioning, state stores, and
        changelog topics.
  - [ ] Exactly-once stream processing on top of the producer transaction path.

## Install

Until the first crates.io release, depend on the workspace or the git repo:

```toml
[dependencies]
kacrab = { git = "https://github.com/pirumu/kacrab", features = ["producer"] }
tokio = { version = "1", features = ["macros", "rt"] }
```

Enable compression codecs as needed:

```toml
kacrab = {
  git = "https://github.com/pirumu/kacrab",
  features = ["producer", "gzip", "lz4", "snappy", "zstd"]
}
```

GSSAPI/Kerberos support is behind the `gssapi` feature.

## Producer

The public producer API is intentionally close to Java/Kafka config style
while keeping Rust delivery handles explicit:

```rust
use kacrab::producer::{KafkaProducer, ProducerRecord};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut producer = KafkaProducer::builder()
        .set("bootstrap.servers", "127.0.0.1:9092")
        .set("client.id", "orders-writer")
        .set("acks", "all")
        .set("enable.idempotence", "true")
        .set("batch.size", "16384")
        .set("linger.ms", "5")
        .set("compression.type", "none")
        .build()
        .await?;

    let delivery = producer
        .send(
            ProducerRecord::new("orders", 0)
                .key("order-42")
                .value("created"),
        )
        .await?;

    producer.flush().await?;
    let receipt = delivery.await?;
    println!(
        "topic={} partition={} offset={}",
        receipt.topic, receipt.partition, receipt.base_offset
    );

    producer.close().await?;
    Ok(())
}
```

`send_batch` returns one `Delivery` per record. `send_batch_untracked` skips
per-record delivery handles and relies on `flush` or `close` to wait for broker
acknowledgement.

Transactions use the same producer:

```rust
let mut producer = KafkaProducer::builder()
    .set("bootstrap.servers", "127.0.0.1:9092")
    .set("transactional.id", "orders-tx-1")
    .build()
    .await?;

producer.init_transactions().await?;
producer.begin_transaction()?;
producer.send(ProducerRecord::new("orders", 0).value("created")).await?;
producer.commit_transaction().await?;
```

## Auth

Auth uses Kafka-compatible property names. For built-in `PLAIN` and `SCRAM`,
kacrab only reads the credential options; it does not load Java login modules:

```rust
let producer = KafkaProducer::builder()
    .set("bootstrap.servers", "broker-1:9093")
    .set("security.protocol", "SASL_SSL")
    .set("ssl.truststore.location", "/etc/kafka/client.truststore.p12")
    .set("ssl.truststore.password", "secret")
    .set("ssl.truststore.type", "PKCS12")
    .set("sasl.mechanism", "SCRAM-SHA-512")
    .set("sasl.jaas.config", r#"username="user" password="pass";"#)
    .build()
    .await?;
```

Supported runtime paths include:

- `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, and `SASL_SSL`.
- TLS trust and identity material from PEM, JKS, and PKCS12 stores.
- SASL `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`, `OAUTHBEARER`, and
  feature-gated `GSSAPI`.
- OAuth bearer tokens from JAAS tokens, files, HTTP(S) token endpoints, and
  locally signed JWT assertions.
- Native Rust custom SASL authenticators via
  `KafkaProducerBuilder::sasl_client_authenticator(...)`.

Full Java JAAS strings are accepted for migration compatibility, but the Rust
runtime parses only the options it supports.

## Protocol Compatibility

`kacrab-protocol` generates Kafka protocol structs from Apache Kafka schemas.
The ignored Java oracle matrix compiles a small Java helper against
`org.apache.kafka:kafka-clients:4.3.0` and checks byte-for-byte round trips
between Rust-generated messages and Kafka Java messages.

```bash
make test-protocol-java
make test-protocol-java-matrix
```

The Java matrix is intentionally not part of the default `make test` path
because it needs Java, Maven, and the Kafka client jar.

## Test Coverage

Default coverage is split by layer so protocol correctness, runtime behavior,
and generated-code compatibility can move independently:

- `make test` runs the workspace test suite with all features enabled.
- `make test-codegen` covers schema parsing and Rust lowering in
  `kacrab-codegen`.
- `make test-protocol` covers protocol primitives, generated round trips, and
  runtime helpers in `kacrab-protocol`.
- `make test-protocol-java` compiles the ignored Java interop tests.
- `make test-protocol-java-matrix` runs the full byte-for-byte Java oracle
  matrix for generated Kafka messages.

The runtime crate has focused integration tests under
[`kacrab/tests/`](kacrab/tests/) for config validation, producer accumulation
and dispatch, and mock TCP wire sessions. Real Kafka smoke tests are ignored by
default and are meant to be run explicitly with the local compose cluster when
touching Kafka-facing behavior:

```bash
docker compose -f docker-compose.kafka.yml up -d
cargo test -p kacrab --test real_kafka_producer --all-features -- --ignored --nocapture
docker compose -f docker-compose.kafka.yml down
```

For line coverage, [`tarpaulin.toml`](tarpaulin.toml) keeps generated Kafka
artifacts and benchmark fixtures out of the maintained-source coverage signal.
Coverage is a regression signal for code we maintain; protocol compatibility is
still gated by generated round trips and the Java oracle matrix.

```bash
cargo tarpaulin --workspace --all-features --config tarpaulin.toml
```

Latest measured coverage on 2026-06-16:

- `cargo tarpaulin --workspace --all-features --config tarpaulin.toml --out Stdout`
- Maintained-source line coverage: 82.65%.
- Covered lines: 5,942 / 7,189.
- Java oracle fixture inventory: 6 release-grade fixture families × 625
  schema/version cases = 3,750 generated fixture cases.

## Benchmarks

The producer target is production-grade throughput, not a toy wrapper. Local
benchmark hooks live in [`benches/`](benches/) and include accumulator,
wire-pipeline, mock multi-broker producer dispatch, and real Kafka smoke
benchmarks through the public `KafkaProducer` API.

Benchmark host for the 2026-06-16 local baselines:

- MacBook Pro, model identifier `Mac15,6`.
- Apple M3 Pro base chip: 11-core CPU (5 performance, 6 efficiency), 14-core
  GPU, 16-core Neural Engine.
- 18GB unified memory; M3 Pro memory bandwidth: 150GB/s.

```bash
cargo bench -p kacrab-benches --bench producer_accumulator
cargo bench -p kacrab-benches --bench wire_pipeline
cargo bench -p kacrab-benches --bench producer_dispatcher
cargo run -p kacrab-benches --release --bin producer_mock_bench
KACRAB_BENCH_SMOKE=1 cargo run -p kacrab-benches --release --bin producer_kafka_bench
```

Latest recorded local baselines are from 2026-06-16 on this development
machine. Treat these as measurement checkpoints, not release guarantees:

- Producer, public API against `apache/kafka:4.3.0` single-node KRaft:
  - `producer_kafka_bench`, 5M × 10B, in-flight 8: 5.09-5.24M
    messages/sec, 48.6-50.0 MiB/sec.
  - `producer_kafka_bench`, 100K × 10 KiB, in-flight 8: 39.2-43.6K
    messages/sec, 383-426 MiB/sec.
- Producer dispatcher, local mock multi-broker:
  - `producer_dispatcher/multi_broker_dispatch`: 4.08-4.56M messages/sec.
- Producer accumulator micro-benchmarks:
  - `producer_accumulator/append_and_drain/1024`: 14.67-20.00M records/sec.
  - `producer_accumulator/append_and_drain/16384`: 15.48-21.98M records/sec.
- Producer mock broker executable:
  - `producer_mock_bench`, 5M × 10B: 5.38M messages/sec, 51.27 MiB/sec.
  - `producer_mock_bench`, 100K × 10 KiB: 249K messages/sec, 2.38 GiB/sec.
- Wire pipeline:
  - `wire_pipeline/api_versions_send_to_broker`: 150-166K requests/sec.

Exact benchmark run notes are documented in
[`benches/README.md`](benches/README.md).

## Workspace

- `kacrab/` - public runtime crate: config, wire, producer.
- `kacrab-protocol/` - protocol primitives, generated Kafka schemas, record
  batch codecs, compression, and Java interop tests.
- `kacrab-codegen/` - protocol and config code generation from upstream Kafka.
- `kacrab-macros/` - helper macros for generated/typed config surfaces.
- `examples/` - runnable config and producer examples.
- `benches/` - internal benchmark crate.

## Development

Prefer the Makefile targets:

```bash
make fmt-check
make clippy
make test
```

For strict dependency and license checks:

```bash
make deny
```

## Author

`kacrab` is authored and maintained by `pirumu`.

## License

This project is licensed under either of:

- [MIT license](LICENSE-MIT)
- [Apache License, Version 2.0](LICENSE-APACHE)
