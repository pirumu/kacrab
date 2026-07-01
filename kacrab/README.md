# kacrab

The main `kacrab` crate: a Rust-native Kafka client with Java-compatible auth,
producer, and admin surfaces. The protocol/wire/producer logic is pure Rust (not a
`librdkafka` wrapper); the dependency tree is not fully C-free, though — TLS
crypto (`rustls` + `aws-lc-rs`) is C/assembly and always present, and the
optional `zstd` / `lz4-hc` / `gssapi` features add C.

`kacrab` is pre-release. Core runtime pieces and protocol compatibility form
the current base. The active runtime surface is:

- `config` - Java-style `ClientConfig`, typed producer/consumer/admin configs,
  official Kafka config metadata, and strict validation.
- `common` - shared `org.apache.kafka.common` domain types (`TopicPartition`,
  `OffsetAndMetadata`, `ConsumerGroupMetadata`, `Node`), always compiled and
  re-exported by `producer`/`admin`.
- `wire` - Tokio broker sessions, ApiVersions negotiation, TLS, SASL,
  metadata, bounded in-flight requests, and request/response dispatch.
- `producer` - Java-style producer builder, batching, linger, bounded memory,
  compression, idempotence, transactions, routing, and multi-broker dispatch
  behind the `producer` feature.
- `admin` - Java-style `AdminClient` covering the full Apache Kafka 4.3.0
  `Admin` operation surface (62 operations), behind the `admin` feature.

The remaining product order is consumer, then streams.

## Java Compatibility

Auth, producer, and admin are Java-compatible targets for the implemented
surface (outcome-faithful to the Java client, not a literal class-for-class
port):

- Use familiar Java client keys such as `bootstrap.servers`,
  `security.protocol`, `ssl.truststore.location`, `sasl.mechanism`,
  `sasl.jaas.config`, `acks`, `enable.idempotence`, `transactional.id`,
  `batch.size`, `linger.ms`, and `max.in.flight.requests.per.connection`.
  For built-in Rust SASL mechanisms, `sasl.jaas.config` is treated as a
  credential option source; Java login module classes are not loaded.
- `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, and `SASL_SSL` map to the wire
  connection config.
- TLS supports PEM, JKS, and PKCS12 trust/identity material.
- SASL supports `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`, `OAUTHBEARER`, and
  feature-gated `GSSAPI`.
- Producer idempotence and transactions use generated Kafka protocol request
  paths including `InitProducerId`, `FindCoordinator`, `AddPartitionsToTxn`,
  and `EndTxn`.

JVM login module and callback handler classes are the intentional boundary:
Rust cannot load Java classes, so custom auth should use the native Rust
`sasl_client_authenticator(...)` hook.

## Current Status

- [x] Core runtime foundation: config, wire, auth, producer, batching,
      idempotence, transactions, and multi-broker dispatch.
- [x] Protocol foundation: primitives, record batches, generated Kafka schemas,
      compression, and Java oracle compatibility checks.
- [x] Admin: the full Apache Kafka 4.3.0 `Admin` operation surface (62 ops —
      topics/partitions/configs, ACLs, groups & offsets, transactions,
      delegation tokens, quotas, SCRAM, reassignments, `KRaft` quorum, share &
      streams groups) through the same auth/transport stack, verified against a
      real broker.
- [ ] Consumer: manual assignment, fetch, offsets, group coordination,
      rebalance, and backpressure.
- [ ] Streams: topology runtime, state stores, repartitioning, changelog topics,
      and exactly-once processing on producer transactions.

## Features

```toml
kacrab = { git = "https://github.com/pirumu/kacrab", features = ["producer"] }
```

Optional runtime features:

- `producer` - enables the producer API.
- `admin` - enables the `AdminClient` API.
- `gzip`, `snappy`, `lz4` - pure-Rust record-batch compression codecs (no C
  toolchain).
- `zstd` - record-batch compression via the C `libzstd` (`zstd-sys`); needs a C
  compiler at build time. The `compression` meta-feature enables all four
  (`gzip` + `snappy` + `lz4` + `zstd`), so it requires a C compiler too — for a
  pure-Rust build, enable only the first three.
- `lz4-hc` - C-FFI LZ4 backend adding high-compression levels
  (`compression.lz4.level` 3..=12); needs a C compiler at build time. Plain
  `lz4` is fast-mode only.
- `gssapi` - enables Kerberos/GSSAPI through platform Kerberos credentials.
- `macros` - re-exports the config macro helper.

`default = ["std"]`; the crate still carries
`#![cfg_attr(not(feature = "std"), no_std)]`, while the active wire and producer
runtime currently use `std` and Tokio.

## Producer Example

```rust
use kacrab::producer::{Producer, ProducerRecord};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut producer = Producer::builder()
        .set("bootstrap.servers", "127.0.0.1:9092")
        .set("client.id", "kacrab-example")
        .set("acks", "all")
        .set("enable.idempotence", "true")
        .set("batch.size", "16384")
        .set("linger.ms", "5")
        .build()
        .await?;

    // `send` is synchronous (Kafka `Producer.send` shape): it enqueues the
    // record and returns a future you await for the broker acknowledgement.
    let delivery = producer
        .send(ProducerRecord::new("orders", 0).key("k").value("v"))?;

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

See [`../examples/producer.rs`](../examples/producer.rs) for single-send,
tracked batch, untracked batch, idempotence, transaction, and auth examples.

## Verification

Use workspace Makefile targets from the repo root:

```bash
make fmt-check
make clippy
make test
```

Protocol compatibility with Kafka Java is checked by the ignored Java oracle
matrix:

```bash
make test-protocol-java-matrix
```

## Author

`kacrab` is authored and maintained by `pirumu`.

## License

This crate is licensed under either MIT or Apache-2.0, matching the workspace.
