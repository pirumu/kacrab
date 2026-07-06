# kacrab

A Rust-native Apache Kafka client — producer, consumer, and admin — built from
the Kafka protocol up. Not a `librdkafka` wrapper.

[![CI](https://github.com/pirumu/kacrab/actions/workflows/ci.yml/badge.svg?branch=master)](https://github.com/pirumu/kacrab/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/kacrab.svg)](https://crates.io/crates/kacrab)
[![docs.rs](https://docs.rs/kacrab/badge.svg)](https://docs.rs/kacrab)
[![MSRV](https://img.shields.io/crates/msrv/kacrab.svg)](https://crates.io/crates/kacrab)
[![license](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](https://github.com/pirumu/kacrab#license)

[Repository](https://github.com/pirumu/kacrab) ·
[Design & Internals book](https://pirumu.github.io/kacrab/) ·
[API reference](https://docs.rs/kacrab)

The protocol, wire, and client logic are pure Rust with `unsafe_code` forbidden
workspace-wide. The dependency tree is not fully C-free, though: the default TLS
provider (`rustls` + `aws-lc-rs`) is C/assembly, and the optional `zstd`,
`lz4-hc`, and `gssapi` features add C. For a C-free build, use a pure-Rust
`rustls` provider and the `gzip`/`snappy`/`lz4` codecs.

## Install

```toml
[dependencies]
kacrab = { version = "0.1", features = ["producer", "consumer", "admin"] }
tokio = { version = "1", features = ["macros", "rt"] }
```

The crate compiles almost nothing by default (`default = []`) — a bare
`kacrab = "0.1"` gives you no producer, consumer, or admin API. Opt into the
surfaces you use:

- `producer` — the producer API.
- `consumer` — the consumer API; pulls in the `compression` codecs (fetched
  batches must decompress) and `regex` for pattern subscription.
- `admin` — the `AdminClient` API.
- `gzip`, `snappy`, `lz4` — pure-Rust record-batch compression codecs (no C
  toolchain).
- `zstd` — compression via the C `libzstd` (`zstd-sys`); needs a C compiler at
  build time. The `compression` meta-feature enables all four
  (`gzip` + `snappy` + `lz4` + `zstd`), so it needs one too — for a pure-Rust
  build, enable only the first three.
- `lz4-hc` — C-FFI LZ4 backend adding high-compression levels
  (`compression.lz4.level` 3..=12); plain `lz4` is fast-mode only.
- `gssapi` — Kerberos/GSSAPI through platform Kerberos libraries.
- `macros` — re-exports the config macro helper.

## Surface

- `config` — Java-style `ClientConfig`, typed producer/consumer/admin configs,
  official Kafka config metadata, and strict validation.
- `common` — shared `org.apache.kafka.common` domain types (`TopicPartition`,
  `OffsetAndMetadata`, `ConsumerGroupMetadata`, `Node`), always compiled and
  re-exported by `producer`/`consumer`/`admin`.
- `wire` — Tokio broker sessions, `ApiVersions` negotiation, TLS, SASL,
  metadata, bounded in-flight requests, and request/response dispatch.
- `producer` — Java-style producer builder, batching, linger, bounded memory,
  compression, murmur2 + sticky/adaptive partitioning, multi-broker dispatch
  with leadership-change failover, transactions, and a Kafka-faithful
  idempotent path (per-partition multi-in-flight, ordered retry, deferred epoch
  bump, sequence wraparound). Interceptors and Kafka-named metrics included.
- `consumer` — full Apache Kafka 4.3.0 feature parity: manual assignment, topic
  and pattern (regex) subscription, classic groups
  (`range`/`roundrobin`/`sticky` eager + incremental `cooperative-sticky`,
  KIP-429) and the KIP-848 server-side protocol; topic-id-keyed fetch (KIP-516,
  up to v18), incremental fetch sessions (KIP-227), truncation detection
  (KIP-320), `commit_sync`/`commit_async`/auto-commit, background heartbeat,
  static membership, typed deserializers, interceptors, and `metrics()`.
- `admin` — the full Apache Kafka 4.3.0 `Admin` operation surface (62
  operations): topics, configs (incremental), ACLs, groups & offsets,
  transactions, delegation tokens, quotas, SCRAM, reassignments, KRaft quorum,
  and the 4.x share/streams group families.

Every client surface — producer, consumer, admin, every SASL mechanism and TLS
mode, every compression codec, 3-broker failover — is verified end-to-end
against real Apache Kafka 4.3.0 brokers. On the same broker and defaults,
producer throughput measures +25-28% over the Java client at ~4x less memory,
and consumer throughput 1.9-4x at ~16-20x less memory; methodology and caveats
in the [benchmarks chapter](https://pirumu.github.io/kacrab/benchmarks.html).

## Producer

Requires the `producer` feature. `send` is synchronous like Kafka's
`Producer.send`: it returns a `SendFuture` right away, and you await that
future for the broker acknowledgement. Batching happens automatically through
`batch.size`, `linger.ms`, buffer memory, and flush/close boundaries.

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
`init_transactions`/`begin_transaction`/`commit_transaction`). Serializers are
a compile-time Rust trait (`ProducerSerializer<T>` via
`build_with_serializers`), not `key.serializer` class names.

## Consumer

Requires the `consumer` feature.

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

Records are bytes-first (`ConsumerRecord.key/value: Option<Bytes>`), with a
typed `ConsumerDeserializer` layer on top. Offsets commit sync, async, or
automatically, with leader-epoch awareness.

## Admin

Requires the `admin` feature. Admin mirrors Java's `Admin` with `snake_case`
methods and per-call options structs:

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

## Java Compatibility

Auth, producer, consumer, and admin are outcome-faithful to the Java client for
the implemented surface — not a literal class-for-class port:

- Familiar Java client keys work as-is: `bootstrap.servers`,
  `security.protocol`, `ssl.truststore.location`, `sasl.mechanism`,
  `sasl.jaas.config`, `acks`, `enable.idempotence`, `transactional.id`,
  `batch.size`, `linger.ms`, `max.in.flight.requests.per.connection`, ...
- `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, and `SASL_SSL`; TLS trust/identity
  material in PEM, JKS, and PKCS12; SASL `PLAIN`, `SCRAM-SHA-256/512`,
  `OAUTHBEARER` (JAAS options, files, HTTP(S) token endpoints, or locally
  signed JWT assertions), and feature-gated `GSSAPI`. Handshake and auth
  failures fail fast with the broker's reason, matching Java.
- JVM login module and callback handler classes are the intentional boundary:
  Rust cannot load Java classes. `sasl.jaas.config` strings are parsed for
  their credential options only; custom SASL flows plug in through the native
  `sasl_client_authenticator(...)` hook.
- Protocol request/response structs are generated from the Apache Kafka message
  schemas and checked byte-for-byte against the Kafka Java client as an
  external oracle.

## Status

Protocol, wire, auth, producer, consumer, and admin all have a verified usable
baseline; the remaining work before production-ready is
measurement under load (sustained multi-broker stress, cross-DC/high-RTT
coverage, memory soak, latency-percentile gates), not correctness. See the
[roadmap](https://github.com/pirumu/kacrab/blob/master/ROADMAP.md).

**Kafka Streams is out of scope.** kacrab is a Kafka *client* library — the
equivalent of `KafkaProducer`/`KafkaConsumer`/`Admin`, not a stream-processing
framework. It deliberately provides the primitives a streams runtime would
build on (transactions, consumer groups, offsets) and stops there.

## Author

`kacrab` is authored and maintained by `pirumu`.

## License

This crate is licensed under either MIT or Apache-2.0, matching the workspace.
