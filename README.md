<div align="center">
  <img src="assets/logo.png" alt="kacrab logo" width="200"/>

# kacrab

**A Rust-native Apache Kafka client — producer, consumer, and admin — built
from the Kafka protocol up. Not a `librdkafka` wrapper.**

[![MIT licensed][mit-badge]][mit-url]
[![Apache-2.0 licensed][apache-badge]][apache-url]

</div>

[mit-badge]: https://img.shields.io/badge/license-MIT-blue.svg
[mit-url]: LICENSE-MIT
[apache-badge]: https://img.shields.io/badge/license-Apache--2.0-blue.svg
[apache-url]: LICENSE-APACHE

## Highlights

- **Java-compatible by design** — auth, producer, admin, and consumer use Kafka
  property names, defaults, protocol flow, and wire semantics as the
  compatibility target. Config keys you know from the Java client work here.
- **Producer** — batching, linger, bounded memory, compression
  (`gzip`/`snappy`/`lz4`/`zstd`), murmur2 + sticky/adaptive partitioning,
  multi-broker dispatch with leadership-change failover, transactions, and a
  Kafka-faithful idempotent path (per-partition multi-in-flight, ordered retry,
  deferred epoch bump, sequence wraparound). Interceptors and Kafka-named
  metrics included.
- **Consumer** — full Apache Kafka 4.3.0 feature parity: manual assignment,
  topic and pattern (regex) subscription, classic groups
  (`range`/`roundrobin`/`sticky` eager + incremental `cooperative-sticky`,
  KIP-429) and the KIP-848 server-side protocol; topic-id-keyed fetch
  (KIP-516, up to v18), incremental fetch sessions (KIP-227), truncation
  detection (KIP-320), `commit_sync`/`commit_async`/
  auto-commit, background heartbeat, static membership, typed deserializers,
  interceptors, and `metrics()`.
- **Admin** — the full Apache Kafka 4.3.0 `Admin` operation surface (62
  operations): topics, configs (incremental), ACLs, groups & offsets,
  transactions, delegation tokens, quotas, SCRAM, reassignments, KRaft quorum,
  and the 4.x share/streams group families.
- **Auth** — `PLAINTEXT`/`SSL`/`SASL_PLAINTEXT`/`SASL_SSL`; SASL `PLAIN`,
  `SCRAM-SHA-256/512`, `OAUTHBEARER`, feature-gated `GSSAPI`; PEM/JKS/PKCS12
  stores and mutual TLS; native Rust custom-authenticator hooks. Handshake and
  auth failures fail fast with the broker's reason, matching Java.
- **Fast and lean** — beats the Java client on the same broker and defaults:
  producer **+25–28%** throughput at ~4x less memory; consumer **1.9–4x**
  throughput at ~16–20x less memory. See [Benchmarks](#benchmarks).
- **Native Rust** — protocol, wire, and client logic are pure Rust with
  workspace `unsafe_code` forbidden. Caveat: the default TLS provider
  (`rustls` + `aws-lc-rs`) is C/assembly, and the optional `zstd`, `lz4-hc`,
  and `gssapi` features add C; a C-free build uses a pure-Rust `rustls`
  provider and the `gzip`/`snappy`/`lz4` codecs.
- **Generated protocol** — request/response structs are generated from Apache
  Kafka schemas and checked byte-for-byte against the Kafka Java client oracle.
- **Real-broker verified** — every client surface (producer, consumer, admin,
  every SASL mechanism and TLS mode, every compression codec, 3-broker
  failover) is verified end-to-end against real Apache Kafka 4.3.0 brokers.

## Documentation

- **[Design & Internals book](https://pirumu.github.io/kacrab/)** — architecture
  and algorithm deep dives (idempotent producer state machine, consumer
  rebalancing and fetching, SASL/TLS handshakes, protocol codegen, benchmark
  methodology). Source in [`docs-book/`](docs-book/).
- **API reference** — [docs.rs/kacrab](https://docs.rs/kacrab) (after the first
  crates.io release).

## Status

> **Warning:** `kacrab` is pre-release software. The public API and runtime
> behavior are not stable release guarantees yet.

Protocol, wire, auth, producer, consumer, and admin all have a verified,
usable baseline. What remains before calling this production-ready is
**measurement under load, not correctness** — sustained multi-broker stress,
cross-DC/high-RTT coverage, memory soak, and latency-percentile gates. The
concrete plan lives in [`ROADMAP.md`](ROADMAP.md).

**Kafka Streams is a non-goal.** kacrab is a Kafka *client* library — the
`KafkaProducer`/`KafkaConsumer`/`Admin` equivalents — not a stream-processing
framework. A streams runtime (topology API, state stores, changelog topics)
would be a separate project; kacrab deliberately provides the primitives it
would build on (transactions, consumer groups, offsets) and stops there.

Test coverage (`cargo llvm-cov`): **~87% maintained-source** line coverage
(generated protocol excluded), producer module ~92%. The raw whole-workspace
figure is lower only because it counts generated protocol structs for APIs not
yet wired (streams).

## Install

Until the first crates.io release, depend on the git repo:

```toml
[dependencies]
kacrab = { git = "https://github.com/pirumu/kacrab", features = ["producer"] }
tokio = { version = "1", features = ["macros", "rt"] }
```

Features: `producer`, `consumer`, `admin`; compression codecs `gzip`, `lz4`,
`snappy`, `zstd`; Kerberos via `gssapi`.

## Producer

`send` is synchronous like Kafka's `Producer.send`: it returns a `SendFuture`
immediately, which you await for the broker acknowledgement. Batching is
automatic (`batch.size`, `linger.ms`, buffer memory, flush/close boundaries).

```rust
use kacrab::producer::{Producer, ProducerRecord};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut producer = Producer::builder()
        .set("bootstrap.servers", "127.0.0.1:9092")
        .set("acks", "all")
        .set("enable.idempotence", "true")
        .set("linger.ms", "5")
        .build()
        .await?;

    let delivery = producer.send(
        ProducerRecord::new("orders", 0).key("order-42").value("created"),
    )?;

    producer.flush().await?;
    let receipt = delivery.await?;
    println!("{}-{}@{}", receipt.topic, receipt.partition, receipt.offset);

    producer.close().await?;
    Ok(())
}
```

Transactions use the same producer (`transactional.id` +
`init_transactions`/`begin_transaction`/`commit_transaction`). Interceptors
(`add_interceptor`) and Kafka-named metrics (`kafka_metrics()`, e.g.
`producer-metrics:record-send-rate`) mirror the Java surface. Serializers are
a compile-time Rust trait (`ProducerSerializer<T>` via
`build_with_serializers`), not `key.serializer` class names — see
[`examples/typed_serializer.rs`](examples/typed_serializer.rs).

## Consumer

Supports manual `assign` + `seek`/`position`/`pause`, topic subscription, and
regex `subscribe_pattern`, with both group protocols:

```rust
use std::time::Duration;
use kacrab::consumer::{Consumer, StringDeserializer};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut consumer = Consumer::from_map([
        ("bootstrap.servers", "localhost:9092"),
        ("group.id", "orders-workers"),
        ("auto.offset.reset", "earliest"),
        // Incremental rebalancing; use ("group.protocol", "consumer") for KIP-848.
        ("partition.assignment.strategy", "cooperative-sticky"),
    ])
    .await?;
    consumer.subscribe(["orders"])?;

    let (keys, values) = (StringDeserializer, StringDeserializer);
    loop {
        let records = consumer.poll(Duration::from_secs(1)).await?;
        for record in &records {
            let (key, value) = record.deserialized(&keys, &values)?;
            println!(
                "{}-{}@{}: {key:?} = {value:?}",
                record.topic, record.partition, record.offset
            );
        }
        consumer.commit_sync().await?;
    }
}
```

Records are bytes-first (`ConsumerRecord.key/value: Option<Bytes>`) with a
typed `ConsumerDeserializer` layer on top. Offsets commit sync, async, or
automatically (leader-epoch aware); `ConsumerInterceptor`s and `metrics()`
round out the surface. See the book's
[consumer chapter](docs-book/src/consumer.md) for the rebalancing and fetching
deep dives.

## Admin

Mirrors Java's `Admin` with snake_case methods and per-call options structs;
built from the same Kafka config keys (including `security.protocol`/TLS/SASL):

```rust
use kacrab::admin::{AdminClient, CreateTopicsOptions, NewTopic};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let admin = AdminClient::from_map([("bootstrap.servers", "localhost:9092")]).await?;

    admin
        .create_topics(vec![NewTopic::new("orders", 6, 3)], CreateTopicsOptions::default())
        .await?;

    for topic in admin.list_topics(Default::default()).await? {
        println!("{}", topic.name);
    }
    Ok(())
}
```

All 62 operations are verified against a real broker across every routing path
(controller, coordinator with transient-error retry, per-leader, broadcast).
Shared `org.apache.kafka.common` domain types (`TopicPartition`, `Node`, ...)
live in `kacrab::common`. Runnable tour in
[`examples/admin.rs`](examples/admin.rs).

## Auth

Kafka-compatible property names; JAAS strings are accepted for migration, but
kacrab parses only the credential options (it never loads Java login modules):

```rust
let producer = Producer::builder()
    .set("bootstrap.servers", "broker-1:9093")
    .set("security.protocol", "SASL_SSL")
    .set("ssl.truststore.location", "/etc/kafka/client.truststore.p12")
    .set("ssl.truststore.password", "secret")
    .set("sasl.mechanism", "SCRAM-SHA-512")
    .set("sasl.jaas.config", r#"username="user" password="pass";"#)
    .build()
    .await?;
```

OAuth bearer tokens come from JAAS options, files, HTTP(S) token endpoints, or
locally signed JWT assertions; custom SASL flows plug in via
`sasl_client_authenticator(...)`.

## Benchmarks

Head-to-head against the Java client on the same native single-node Apache
Kafka 4.3.0 broker, topic, and defaults (`acks=all` + idempotence; consumer at
`max.poll.records=500`). Host: MacBook Pro M3 Pro (11-core, 18 GB), broker
co-located with the client. Full methodology, reproduction commands, and
caveats: [`benches/README.md`](benches/README.md) and the book's
[benchmarks chapter](docs-book/src/benchmarks.md).

**Producer** (2026-07-02):

| Scenario | kacrab | Java `kafka-producer-perf-test` |
| --- | ---: | ---: |
| 5M × 10 B, 16 partitions | **4.79–4.86M rec/s** | 3.80–3.84M rec/s |
| 100K × 10 KiB, 3 partitions | **~542 MiB/s** | 417–453 MB/s |
| Peak RSS / CPU (10 B run) | **~68 MiB / ~2.7 s** | ~268 MiB / ~4.1 s |

**Consumer** (2026-07-02):

| Scenario | kacrab | Java `kafka-consumer-perf-test` |
| --- | ---: | ---: |
| 5M × 10 B, 16 partitions | **~17.6M rec/s** | ~9.3M rec/s |
| 100K × 10 KiB, 3 partitions | **~5.3 GB/s** | ~1.3 GB/s |
| Peak RSS / poll() max (10 B run) | **~18 MiB / ~8 ms** | ~286 MiB / ~111 ms |

Read these honestly:

- Single-node, RF=1, broker co-located with the client — a client-efficiency
  signal, not a production throughput claim. 10-byte rows inflate records/sec;
  read the byte-rate columns for the meaningful figure.
- Latency is closed-loop saturation latency, not open-loop SLA latency. Java
  keeps a lower typical producer latency on the 16-partition workload — a
  pipeline-depth tradeoff (`max.in.flight=1` brings kacrab's p99 to ~2 ms at
  the same throughput); at 1–3 partitions kacrab's latency is at or below
  Java's.
- Zero retries/errors on every kacrab run (fully idempotent-correct).

## Testing

```bash
make fmt-check clippy test    # workspace suite, all features
make deny                     # dependency & license checks
```

Real-broker smoke tests are ignored by default and run against the local
compose files (`docker-compose.{kafka,kafka-admin,auth,gssapi,tls,cluster}.yml`):

```bash
docker compose -f docker-compose.kafka.yml up -d
cargo test -p kacrab --test real_kafka_producer --all-features -- --ignored --nocapture
```

Protocol compatibility is additionally gated by a byte-for-byte Java oracle
matrix (`make test-protocol-java-matrix`; needs Java + Maven). Line coverage
runs via `cargo llvm-cov` with generated artifacts excluded — see
[`Makefile`](Makefile) and [`benches/README.md`](benches/README.md).

## Workspace

- [`kacrab/`](kacrab/) — public runtime crate: config, wire, common, producer,
  consumer, admin.
- [`kacrab-protocol/`](kacrab-protocol/) — protocol primitives, generated Kafka
  schemas, record batch codecs, compression, Java interop tests.
- [`kacrab-codegen/`](kacrab-codegen/) — protocol and config code generation
  from upstream Kafka.
- [`kacrab-macros/`](kacrab-macros/) — helper macros for typed config surfaces.
- [`examples/`](examples/) — runnable producer/consumer/admin examples.
- [`benches/`](benches/) — internal benchmark crate (real-Kafka harnesses +
  microbenchmarks).

## License

Authored and maintained by `pirumu`. Licensed under either of:

- [MIT license](LICENSE-MIT)
- [Apache License, Version 2.0](LICENSE-APACHE)
