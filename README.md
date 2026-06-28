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
  idempotence (multi-in-flight per partition with Java-faithful sequence
  ordering and failure recovery), transactions, compression, metadata routing,
  interceptors, Kafka-named metrics, and multi-broker dispatch are first-class
  design points. On a single-node broker it outruns the Java client's
  throughput on the same workload (trading some latency for it — see
  [Benchmarks](#benchmarks)).
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

Protocol, wire, auth, and producer now have a usable baseline. The producer is
the most mature surface: on a single-node broker it sustains ~4.7M records/sec
at `acks=all` + idempotence, beating the Java client's throughput on the same
workload (at higher latency — see [Benchmarks](#benchmarks)). The current focus remains wire + producer hardening
before consumer work: multi-broker behavior, bounded hot paths, routing refresh,
and sustained stress testing.

Producer module test coverage is **~92% line** (`cargo llvm-cov`, 600+ unit and
integration tests). The append/dispatch/idempotent-recovery hot paths, the
murmur2 partitioner (byte-exact against the Java client for every key length),
transactions, interceptors, and metrics are covered; the remaining gaps are
mechanical error-clone arms and rare defensive branches.

Auth and producer are treated as **Java-compatible targets** for the
implemented surface:

- Java-style config keys work through `ClientConfig` and
  `Producer::builder().set(...)`.
- `security.protocol`, TLS, SASL, idempotence, transactions, batching, request
  timeout, delivery timeout, compression, and in-flight limits map through the
  same Kafka property names users know from the Java client.
- The idempotent/transactional path follows the Java client's real algorithms:
  per-partition multi-in-flight with `inflightBatchesBySequence` ordering,
  `maybeResolveSequences` epoch handling, duplicate-sequence dedup, and ordered
  re-send on retry. Behavior is outcome-faithful to Java; the remaining
  differences are concurrency-model details (async tasks vs the Java sender
  thread), not protocol or correctness gaps.
- `ProducerInterceptor` mirrors Java's `configure`/`onSend`/`onAcknowledgement`/
  `close` plus the `ClusterResourceListener.onUpdate` hook, and metrics are
  published under their Kafka names (`producer-metrics:*` /
  `producer-topic-metrics:*`).
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
  - [x] Public `Producer` API with Java-style config keys and synchronous
        `send`/`send_with_callback` (Java `Producer.send` shape) returning a
        `SendFuture`.
  - [x] Batching by topic-partition, linger, bounded memory, `max.block.ms`,
        compression hooks, and delivery handles.
  - [x] Metadata routing with default partition assignment, keyed murmur2
        partitioning, sticky/adaptive unkeyed assignment, and multi-broker
        dispatch.
  - [x] Retry backoff, delivery timeout across retries, broker response error
        propagation, and leadership-error retry path.
  - [x] Java-faithful idempotent producer: per-partition multi-in-flight,
        `firstInFlightSequence` ordered retry, `maybeResolveSequences` deferred
        epoch bump, stale-epoch re-stamp, single-bump recovery, sequence
        wraparound, and duplicate-sequence dedup.
  - [x] Transactional control flow through coordinator lookup, `InitProducerId`,
        `AddPartitionsToTxn`, `TxnOffsetCommit`, and `EndTxn`.
  - [x] `ProducerInterceptor` lifecycle (`configure`/`on_send`/`on_ack`/
        `on_error`/`on_update`/`close`) and Kafka-named producer + per-topic
        metrics including the buffer-pool gauges.
  - [x] Beats the Java client throughput on a single-node broker at the default
        `acks=all` + idempotence config (see [Benchmarks](#benchmarks)).
  - [ ] Production acceptance: sustained multi-broker stress, memory soak,
        leadership-change refresh coverage, and latency-percentile gates on
        realistic multi-broker workloads.
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
use kacrab::producer::{Producer, ProducerRecord};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut producer = Producer::builder()
        .set("bootstrap.servers", "127.0.0.1:9092")
        .set("client.id", "orders-writer")
        .set("acks", "all")
        .set("enable.idempotence", "true")
        .set("batch.size", "16384")
        .set("linger.ms", "5")
        .set("compression.type", "none")
        .build()
        .await?;

    // `send` is synchronous like Java's `Producer.send`: it returns immediately
    // with a `SendFuture` you await for the broker acknowledgement.
    let delivery = producer.send(
        ProducerRecord::new("orders", 0)
            .key("order-42")
            .value("created"),
    )?;

    producer.flush().await?;
    let receipt = delivery.await?;
    println!(
        "topic={} partition={} offset={}",
        receipt.topic, receipt.partition, receipt.offset
    );

    producer.close().await?;
    Ok(())
}
```

`send` returns a per-record `SendFuture` synchronously (no `await` on the call
itself), matching Java's thread-safe `Producer.send`. `send_with_callback`
additionally invokes a callback on acknowledgement. Batching is automatic inside
the producer accumulator/sender based on `batch.size`, `linger.ms`, buffer
memory, partition routing, and flush/close boundaries; there is no separate
public batch send API.

Interceptors and Kafka-named metrics use the Java surface:

```rust
// ProducerInterceptor: configure(client.id), on_send / on_ack / on_error,
// on_update(cluster id), close — all panic-isolated like the Java chain.
producer.add_interceptor(my_interceptor);

// Metrics under their Kafka names, e.g. "producer-metrics:record-send-rate",
// "producer-metrics:buffer-available-bytes",
// "producer-topic-metrics:byte-total:topic=orders".
let metrics = producer.kafka_metrics();
```

Transactions use the same producer:

```rust
let mut producer = Producer::builder()
    .set("bootstrap.servers", "127.0.0.1:9092")
    .set("transactional.id", "orders-tx-1")
    .build()
    .await?;

producer.init_transactions().await?;
producer.begin_transaction()?;
let _delivery = producer.send(ProducerRecord::new("orders", 0).value("created"))?;
producer.commit_transaction().await?;
```

## Auth

Auth uses Kafka-compatible property names. For built-in `PLAIN` and `SCRAM`,
kacrab only reads the credential options; it does not load Java login modules:

```rust
let producer = Producer::builder()
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
  `ProducerBuilder::sasl_client_authenticator(...)`.

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

The producer targets production-grade throughput. Local benchmark hooks live in
[`benches/`](benches/); `producer_kafka_bench` drives a real broker through the
public synchronous `send` path.

Benchmark host (2026-06-28 measurements):

- MacBook Pro, model identifier `Mac15,6`.
- Apple M3 Pro: 11-core CPU (5 performance, 6 efficiency), 18GB unified memory.
- Native single-node Apache Kafka 4.3.0, RF=1, sharing the client machine.

Run the real-Kafka throughput benchmark against a local broker:

```bash
# kacrab — synchronous send, default acks=all + idempotence
KACRAB_BOOTSTRAP=127.0.0.1:9092 \
  KACRAB_BENCH_SYNC_SEND=1 KACRAB_BENCH_TOPIC=kacrab-16p KACRAB_BENCH_MESSAGES=5000000 \
  cargo run -q -p kacrab-benches --release --bin producer_kafka_bench

# Java reference — same broker, topic, and config
kafka-producer-perf-test.sh --topic kacrab-16p --num-records 5000000 \
  --record-size 10 --throughput -1 \
  --command-property bootstrap.servers=127.0.0.1:9092 \
  --command-property acks=all --command-property enable.idempotence=true
```

Head-to-head on the same single-node broker, topic, and config (`acks=all` +
`enable.idempotence=true`), 5 runs, `max.in.flight=5`, no compression:

| Scenario | kacrab `producer_kafka_bench` | Java `kafka-producer-perf-test.sh` |
| --- | ---: | ---: |
| 5M x 10B, 16 partitions | **4.70M rec/sec**; 44.8 MiB/sec; lat avg ~1.3 ms, p99 ~8 ms (depth 5; see below); retries 0, errors 0, in-flight stalls 0 | 4.21M records/sec; 40.1 MB/sec; lat avg **0.27 ms**, p99 **1 ms** |
| 2M x 10B, 1 partition | **4.69M rec/sec**; 44.7 MiB/sec; lat avg ~0.2 ms, p99 ~5 ms; retries 0, errors 0 | -- |

Throughput vs latency, honestly: kacrab is ~12% faster on the 16-partition
workload while staying fully idempotent-correct (zero retries/errors), **but Java
has a lower typical latency** (avg ~0.3 ms, p99 ~1 ms vs kacrab avg ~1.3 ms,
p99 ~8 ms; same accounting — per-record send-to-ack via the callback). We dug
into the gap, and it is **a tunable tradeoff plus a shared broker artifact, not a
client cost**:

- **Most of it is pipeline depth.** kacrab's synchronous send keeps the
  per-partition pipeline filled toward `max.in.flight=5`. Drop it to
  `max.in.flight=1` and kacrab's p99 falls to **~2 ms at the same ~4.7M
  throughput** — on a single low-RTT broker the per-broker request coalescing
  already saturates the connection, so the extra depth only adds queue latency
  here. It pays off across multiple brokers / higher RTT, where depth hides
  round-trip time.
- **The depth buys broker-pause resilience.** The co-located single-node JVM
  broker periodically pauses (GC/fsync) — Java sees it too (its max latency
  spiked to 129 ms in the same runs). At depth 5 a pause on one in-flight
  request lets the others drain, so kacrab's p99.9 stays ~10 ms; at depth 1 the
  single slot blocks behind the pause and p99.9 jumps to ~100 ms.

The gap shrinks in production (real broker off the client machine, real network
RTT). For latency-sensitive single-broker use, lower
`max.in.flight.requests.per.connection` / `linger.ms`. Single-partition
steady-state latency is ~0.2 ms avg, already close to Java's; the first
1-partition run includes a cold metadata/connection warmup (~15 ms) that the
steady-state runs do not.

CPU + peak memory for the same 5M-record run (`/usr/bin/time -l`):

| Resource | kacrab | Java | Java overhead |
| --- | ---: | ---: | ---: |
| Peak RSS | ~68 MiB | ~268 MiB | ~3.9x more |
| Total CPU (user+sys) | ~2.7 s | ~4.1 s | ~1.5x more |

The ~12% throughput edge is modest because throughput here is **broker-bound** —
both clients spend most of the run waiting on `acks=all` round-trips, so the
client language barely moves that number. The native-vs-JVM advantage shows up
instead in efficiency: kacrab uses **~4x less memory** (no JVM heap/metaspace)
and **~1.5x less CPU per record**. (Java's CPU includes one-time JVM startup +
JIT warmup; the peak-RSS gap is steady-state.)

Bench knobs: `KACRAB_BENCH_TOPIC`, `KACRAB_BENCH_MESSAGES`, `KACRAB_BENCH_RUNS`,
`KACRAB_BENCH_ACKS1` (acks=1), `KACRAB_BENCH_BATCH_SIZE`. The 16- and
1-partition topics (`kacrab-16p`, `kacrab-1p`) must exist on the broker. The
in-process criterion microbenchmarks under `benches/` exercise the accumulator,
wire pipeline, and dispatcher CPU paths in isolation.

Limits: these are local single-node RF=1 smoke measurements on one Mac with the
broker sharing the client machine. They are not release gates and do not include
CPU/allocator profiles or broker disk metrics. kacrab reports payload MiB/sec;
the Java perf tool reports decimal MB/sec. Cross-DC / high-RTT links -- where
Java's deeper per-connection pipelining helps most -- are not represented here.


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
