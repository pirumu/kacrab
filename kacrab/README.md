# kacrab

The main `kacrab` crate: a pure Rust Kafka client runtime with Java-compatible
auth and producer surfaces.

`kacrab` is pre-release. Core runtime pieces and protocol compatibility form
the current base. The active runtime surface is:

- `config` - Java-style `ClientConfig`, typed producer/consumer/admin configs,
  official Kafka config metadata, and strict validation.
- `wire` - Tokio broker sessions, ApiVersions negotiation, TLS, SASL,
  metadata, bounded in-flight requests, and request/response dispatch.
- `producer` - Java-style producer builder, batching, linger, bounded memory,
  compression, idempotence, transactions, routing, and multi-broker dispatch
  behind the `producer` feature.

The remaining product order is consumer, then admin, then streams.

## Java Compatibility

Auth and producer are 100% Java-compatible targets for the implemented surface:

- Use familiar Java client keys such as `bootstrap.servers`,
  `security.protocol`, `ssl.truststore.location`, `sasl.mechanism`,
  `sasl.jaas.config`, `acks`, `enable.idempotence`, `transactional.id`,
  `batch.size`, `linger.ms`, and `max.in.flight.requests.per.connection`.
- `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, and `SASL_SSL` map to the wire
  connection config.
- TLS supports PEM, JKS, and PKCS12 trust/identity material.
- SASL supports `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`, `OAUTHBEARER`, and
  feature-gated `GSSAPI`.
- Producer idempotence and transactions use generated Kafka protocol request
  paths including `InitProducerId`, `FindCoordinator`, `AddPartitionsToTxn`,
  and `EndTxn`.

JVM callback handler classes are the one intentional boundary: Rust cannot load
Java classes, so custom auth should use the native Rust
`sasl_client_authenticator(...)` hook.

## Current Status

- [x] Core runtime foundation: config, wire, auth, producer, batching,
      idempotence, transactions, and multi-broker dispatch.
- [x] Protocol foundation: primitives, record batches, generated Kafka schemas,
      compression, and Java oracle compatibility checks.
- [ ] Consumer: manual assignment, fetch, offsets, group coordination,
      rebalance, and backpressure.
- [ ] Admin: topic, partition, ACL, config, and cluster operations through the
      same auth/transport stack.
- [ ] Streams: topology runtime, state stores, repartitioning, changelog topics,
      and exactly-once processing on producer transactions.

## Features

```toml
kacrab = { git = "https://github.com/pirumu/kacrab", features = ["producer"] }
```

Optional runtime features:

- `producer` - enables the producer API.
- `gzip`, `snappy`, `lz4`, `zstd` - enables record-batch compression codecs.
- `gssapi` - enables Kerberos/GSSAPI through platform Kerberos credentials.
- `macros` - re-exports the config macro helper.

`default = ["std"]`; the crate still carries
`#![cfg_attr(not(feature = "std"), no_std)]`, while the active wire and producer
runtime currently use `std` and Tokio.

## Producer Example

```rust
use kacrab::producer::{KafkaProducer, ProducerRecord};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut producer = KafkaProducer::builder()
        .set("bootstrap.servers", "127.0.0.1:9092")
        .set("client.id", "kacrab-example")
        .set("acks", "all")
        .set("enable.idempotence", "true")
        .set("batch.size", "16384")
        .set("linger.ms", "5")
        .build()
        .await?;

    let delivery = producer
        .send(ProducerRecord::new("orders", 0).key("k").value("v"))
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
