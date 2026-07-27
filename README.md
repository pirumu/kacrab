<div align="center">
  <img src="assets/logo.png" alt="kacrab logo" width="200"/>

# kacrab

**A Rust-native Apache Kafka client for producer, consumer, and admin use cases,
built from the Kafka protocol up. It is not a `librdkafka` wrapper.**

[![CI][ci-badge]][ci-url]
[![real-broker][real-broker-badge]][real-broker-url]
[![crates.io][crates-badge]][crates-url]
[![docs.rs][docs-badge]][docs-url]
[![MSRV][msrv-badge]][msrv-url]
[![MIT licensed][mit-badge]][mit-url]
[![Apache-2.0 licensed][apache-badge]][apache-url]

</div>

[ci-badge]: https://github.com/pirumu/kacrab/actions/workflows/ci.yml/badge.svg?branch=master
[ci-url]: https://github.com/pirumu/kacrab/actions/workflows/ci.yml
[real-broker-badge]: https://github.com/pirumu/kacrab/actions/workflows/real-broker.yml/badge.svg?branch=master
[real-broker-url]: https://github.com/pirumu/kacrab/actions/workflows/real-broker.yml
[real-broker-auth-url]: https://github.com/pirumu/kacrab/actions/workflows/real-broker-auth.yml
[fuzz-url]: https://github.com/pirumu/kacrab/actions/workflows/fuzz.yml
[crates-badge]: https://img.shields.io/crates/v/kacrab.svg
[crates-url]: https://crates.io/crates/kacrab
[docs-badge]: https://docs.rs/kacrab/badge.svg
[docs-url]: https://docs.rs/kacrab
[msrv-badge]: https://img.shields.io/crates/msrv/kacrab.svg
[msrv-url]: https://crates.io/crates/kacrab
[mit-badge]: https://img.shields.io/badge/license-MIT-blue.svg
[mit-url]: LICENSE-MIT
[apache-badge]: https://img.shields.io/badge/license-Apache--2.0-blue.svg
[apache-url]: LICENSE-APACHE

## Highlights

- **Designed to feel familiar if you know the Java client**: auth, producer,
  admin, and consumer follow Kafka property names, defaults, protocol flow, and
  wire semantics. The config keys you already know from the Java client work
  here too.
- **Producer**: batching, linger, bounded memory, compression
  (`gzip`/`snappy`/`lz4`/`zstd`), murmur2 + sticky/adaptive partitioning,
  multi-broker dispatch with failover on leadership changes, transactions, and
  a Kafka-faithful idempotent path (per-partition multi-in-flight, ordered
  retry, deferred epoch bump, sequence wraparound). Interceptors and
  Kafka-named metrics are included.
- **Consumer**: full Apache Kafka 4.3.0 feature parity: manual assignment,
  topic and pattern (regex) subscription, classic groups
  (`range`/`roundrobin`/`sticky` eager + incremental `cooperative-sticky`,
  KIP-429) and the KIP-848 server-side protocol; topic-id-keyed fetch
  (KIP-516, up to v18), incremental fetch sessions (KIP-227), truncation
  detection (KIP-320), `commit_sync`/`commit_async`/auto-commit, background
  heartbeat, static membership, typed deserializers, interceptors, and
  `metrics()`.
- **Share consumer (KIP-932)**: the queue-shaped consuming surface, behind the
  `share-consumer` feature. `ShareGroupHeartbeat`/`ShareFetch`/`ShareAcknowledge`
  with per-record `Accept`/`Release`/`Reject` acknowledgement, delivery-count
  tracking for poison messages, implicit and explicit acknowledgement modes, and
  more consumers than partitions in one group. See [Share consumer](#share-consumer).
- **Admin**: the full Apache Kafka 4.3.0 `Admin` surface (62 operations):
  topics, configs (incremental), ACLs, groups & offsets, transactions,
  delegation tokens, quotas, SCRAM, reassignments, KRaft quorum, and the 4.x
  share/streams group families.
- **Auth**: `PLAINTEXT`/`SSL`/`SASL_PLAINTEXT`/`SASL_SSL`; SASL `PLAIN`,
  `SCRAM-SHA-256/512`, `OAUTHBEARER`, feature-gated `GSSAPI`; PEM/JKS/PKCS12
  stores and mutual TLS; native Rust custom-authenticator hooks. Handshake and
  auth failures fail fast with the broker's reason, matching Java.
- **Fast and lean**: on the same broker and defaults, producer throughput is
  **+35%** over Java on small records (**+15%** on 10 KiB, **2.2x** when broker
  `max.message.bytes` forces batch splitting) with about 4x less memory, and at
  Java's own offered load kacrab's latency is lower or tied at every percentile,
  with a ~30x lower maximum; consumer
  throughput is **1.9-4x** higher with about 16-20x less memory. See
  [Benchmarks](#benchmarks).
- **Native Rust**: protocol, wire, and client logic are pure Rust, and the
  workspace forbids `unsafe_code`. Caveat: the default TLS provider
  (`rustls` + `aws-lc-rs`) uses C/assembly, and the optional `zstd`, `lz4-hc`,
  and `gssapi` features add C. For a C-free build, use a pure-Rust `rustls`
  provider and the `gzip`/`snappy`/`lz4` codecs.
- **Generated protocol**: request/response structs are generated from Apache
  Kafka schemas and checked byte-for-byte against the Kafka Java client oracle.
- **Verified with real brokers, in CI**: every client surface (producer,
  consumer, admin, every SASL mechanism and TLS mode, every compression codec,
  3-broker failover) runs end-to-end against real Apache Kafka 4.3.0 containers
  as a merge gate. The producer, consumer, compression, admin, and cluster-
  failover suites run on
  [every pull request and every push to `master`][real-broker-url]; the
  authenticated-listener suites (SASL, TLS, GSSAPI against a real MIT KDC) run
  [weekly and on any PR touching the auth stack][real-broker-auth-url], because
  they need a KDC and generated certificates.

## How this compares to the other Rust Kafka clients

If you are already in Rust, the question is not "kacrab vs the Java client" — it
is "why not `rust-rdkafka`". Verified against each project's own documentation
and release history as of 2026-07-27:

| | kacrab 0.3 | rust-rdkafka 0.39 | rskafka 0.6 | kafka-rust 0.10 |
| --- | --- | --- | --- | --- |
| Implementation | Rust protocol stack | wraps librdkafka 2.12.1 (C) | Rust | Rust |
| Build needs | `cargo` alone | C toolchain, GNU make, pthreads (or the `cmake-build` feature); librdkafka sources vendored and statically linked by default | `cargo` alone | `cargo` alone |
| Async model | tokio-native | tokio via `StreamConsumer`/`FutureProducer` over librdkafka's own poll threads | tokio-native | blocking |
| Consumer groups | classic + KIP-848 | classic + KIP-848 (GA in librdkafka 2.12, opt in via `group.protocol`) | none — explicit non-goal | classic |
| Transactions | yes | yes | none — explicit non-goal | not documented |
| Admin API | 62 operations | yes | none | not documented |
| Broker versions | targets 4.3.0 | broad | not stated | tested 0.8.2–3.1 |
| Latest release | 2026-07 | 2026-01 | 2025-03 | 2023-09 |

rskafka states its own scope plainly — *"No support for offset tracking,
consumer groups, transactions, etc."* — it is a deliberately minimal
produce/consume client, not a competitor on surface area. kafka-rust warns
*"Use it in production at your own risk"* and has not released since 2023.

So the real comparison is with `rust-rdkafka`, and it is narrower than the table
makes it look — feature-wise the two are close, because both track the same
protocol.

**Choose `rust-rdkafka` if** you need battle-tested code with years of
production mileage, need a Kafka version older than 4.x, or want the ecosystem
that has grown around librdkafka. It is the safe default and this project does
not pretend otherwise.

**Choose kacrab if** a C toolchain in your build is a real cost — cross
compilation, musl static builds, `cargo install` for downstream users, audit
surface — or you want the client to be Rust the whole way down: `forbid(unsafe_code)`
across the workspace, tokio-native backpressure instead of a wrapped poll loop,
and Kafka errors as typed Rust enums rather than C return codes. See
[Cancellation & drop semantics](#cancellation--drop-semantics) for the async
behaviour that follows from that, and
[When not to use kacrab](#when-not-to-use-kacrab) for the honest limits.

## Documentation

- **[Design & Internals book](https://pirumu.github.io/kacrab/)**: architecture
  and deeper implementation notes: idempotent producer state machine, consumer
  rebalancing and fetching, SASL/TLS handshakes, protocol codegen, and benchmark
  methodology. Source lives in [`docs-book/`](docs-book/).
- **API reference**: [docs.rs/kacrab](https://docs.rs/kacrab).

## Status

Protocol, wire, auth, producer, consumer, and admin all have a verified usable
baseline. The remaining work before calling this production-ready is
**measurement under load, not correctness**: sustained multi-broker stress,
cross-DC/high-RTT coverage, memory soak, and latency-percentile gates. The
concrete plan is in [`ROADMAP.md`](ROADMAP.md).

**Kafka Streams is out of scope.** kacrab is a Kafka *client* library, the
equivalent of `KafkaProducer`/`KafkaConsumer`/`Admin`, not a stream-processing
framework. A streams runtime (topology API, state stores, changelog topics)
would be a separate project. kacrab deliberately provides the primitives that
runtime would build on, such as transactions, consumer groups, and offsets, and
stops there.

Test coverage (`cargo llvm-cov`): **~87% maintained-source** line coverage
(generated protocol excluded), with the producer module at about 92%. The raw
whole-workspace number is lower because codegen covers the entire Kafka message
set — all 93 API keys — so the Java oracle check stays exhaustive, while the
client wires the 70 a client actually issues. Of the 23 unwired, 21 are
broker-internal or KRaft controller RPCs no client ever sends (`LeaderAndIsr`,
`StopReplica`, `UpdateMetadata`, the broker-registration and Raft-quorum
families, the share-coordinator state RPCs); the other 2 are client-facing
surfaces kacrab does not implement — `DescribeTopicPartitions` and the Streams
member protocol (`StreamsGroupHeartbeat`), which is out of scope per above. The
admin-side share and streams group operations *are* wired, and so is the share
consumer itself (`ShareGroupHeartbeat`/`ShareFetch`/`ShareAcknowledge`).

## Install

Nothing is enabled by default (`default = []`) — turn on the surfaces you use:

```toml
[dependencies]
kacrab = { version = "0.3", features = ["producer", "consumer", "admin"] }
tokio = { version = "1", features = ["macros", "rt"] }
```

Available features: `producer`, `consumer`, `admin`, `share-consumer` (each
example below names the one it needs); compression codecs `gzip`, `lz4`,
`snappy`, `zstd` (or the `compression` meta-feature for all four); Kerberos via
`gssapi`; config macro helpers via `macros`.

## Producer

Requires the `producer` feature. `send` is synchronous like Kafka's
`Producer.send`: it returns a `SendFuture`
right away, and you await that future for the broker acknowledgement. Batching
happens automatically through `batch.size`, `linger.ms`, buffer memory, and
flush/close boundaries.

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
(`add_interceptor`) and Kafka-named metrics (`kafka_metrics()`, for example
`producer-metrics:record-send-rate`) mirror the Java surface. Serializers are a
compile-time Rust trait (`ProducerSerializer<T>` via `build_with_serializers`),
not `key.serializer` class names. See
[`examples/typed_serializer.rs`](examples/typed_serializer.rs).

## Consumer

Requires the `consumer` feature. Manual `assign` + `seek`/`position`/`pause`,
topic subscription, regex `subscribe_pattern`, and both group protocols are
supported:

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
typed `ConsumerDeserializer` layer on top. Offsets can be committed sync, async,
or automatically, with leader-epoch awareness. `ConsumerInterceptor`s and
`metrics()` round out the surface. See the book's
[consumer chapter](docs-book/src/consumer.md) for the rebalancing and fetching
deep dives.

## Share consumer

Requires the `share-consumer` feature. A share group (KIP-932) turns a topic into
a work queue: records are *acquired* under a broker-held lock rather than read at
a position, so more consumers than partitions can share a topic and each record
is disposed of on its own instead of by committing an offset. `ShareConsumer` is
a separate type from `Consumer` because none of `assign`, `seek`, `position`, or
`commit_sync` means anything in that model.

```rust
use std::time::Duration;

use kacrab::consumer::{AcknowledgeType, ShareConsumer};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut consumer = ShareConsumer::from_map([
        ("bootstrap.servers", "127.0.0.1:9092"),
        ("group.id", "work-queue"),
        ("share.acknowledgement.mode", "explicit"),
    ])
    .await?;
    consumer.subscribe(["jobs"])?;

    loop {
        let records = consumer.poll(Duration::from_millis(500)).await?;
        for record in records.iter() {
            let outcome = if record.delivery_count > 5 {
                // Poison message: archive it instead of retrying forever.
                AcknowledgeType::Reject
            } else if handle(record).is_ok() {
                AcknowledgeType::Accept
            } else {
                AcknowledgeType::Release
            };
            consumer.acknowledge(record, outcome)?;
        }
        consumer.commit().await?;
    }
}
# fn handle(_record: &kacrab::consumer::ShareRecord) -> Result<(), ()> { Ok(()) }
```

`share.acknowledgement.mode=implicit` (the Kafka default) acknowledges the whole
batch with `Accept` on the next `poll`/`commit` and makes `acknowledge` an error;
`explicit` requires every delivered record to be acknowledged before the next
`poll`/`commit`. `share.acquire.mode` controls whether a poll may exceed
`max.poll.records` to land on batch boundaries. Acknowledgements are batched and
piggy-backed onto the next `ShareFetch`, so the acknowledgement path costs no
extra round trip per record.

The surface maps one-to-one onto Java's `ShareConsumer`:

| Java | kacrab |
| --- | --- |
| `subscription()` / `subscribe` / `unsubscribe` | same names |
| `poll(Duration)` | `poll(Duration)` |
| `acknowledge(record)` | `accept(record)` |
| `acknowledge(record, type)` | `acknowledge(record, type)` |
| `acknowledge(topic, partition, offset, type)` | `acknowledge_offset(&partition, offset, type)` |
| `commitSync()` / `commitSync(Duration)` | `commit()` / `commit_timeout(Duration)` |
| `commitAsync()` | `commit_async()` |
| `setAcknowledgementCommitCallback` | `set_acknowledgement_commit_callback` |
| `acquisitionLockTimeoutMs()` | `acquisition_lock_timeout()` |
| `clientInstanceId(Duration)` | `client_instance_id()` |
| `metrics()` | `metrics()` |
| `close()` / `close(Duration)` | `close()` / `close_timeout(Duration)` |
| `wakeup()` | `wakeup()` |

`commit_async` sends nothing by itself, and that is deliberate rather than a
shortcut: a share session is a strictly ordered epoch sequence per broker, so a
second request racing the poll loop would invalidate the session. Java does the
same — the acknowledgements ride the next `ShareFetch` and the broker's verdict
reaches the acknowledgement callback. Registering that callback also moves
rejected acknowledgements off the `poll`/`commit` return value and into the
callback, matching Java; without one they surface as an error, because dropping
them would hide that those records are about to be redelivered.
`registerMetricForSubscription` has no analogue here, the same way it has none on
`Consumer`.

The admin-side view of the same groups (`describe_share_groups`,
`list_share_group_offsets`, `alter_share_group_offsets`,
`delete_share_group_offsets`, `delete_share_groups`) lives on `AdminClient` and
needs only the `admin` feature.

## Admin

Requires the `admin` feature. Admin mirrors Java's `Admin` with `snake_case`
methods and per-call options structs. It uses the same Kafka config keys,
including `security.protocol`/TLS/SASL:

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
live in `kacrab::common`. There is a runnable tour in
[`examples/admin.rs`](examples/admin.rs).

## Auth

Kafka-compatible property names are used throughout. JAAS strings are accepted
for migration, but kacrab only parses the credential options; it never loads
Java login modules:

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

OAuth bearer tokens can come from JAAS options, files, HTTP(S) token endpoints,
or locally signed JWT assertions. Custom SASL flows plug in through
`sasl_client_authenticator(...)`.

## Benchmarks

These numbers compare kacrab with the Java client on the same native
single-node Apache Kafka 4.3.0 broker, topic, and defaults (`acks=all` +
idempotence; consumer at `max.poll.records=500`). Host: MacBook Pro M3 Pro
(11-core, 18 GB), with the broker co-located with the client. Full methodology,
reproduction commands, and caveats are in [`benches/README.md`](benches/README.md)
and the book's [benchmarks chapter](docs-book/src/benchmarks.md).

**Producer** (2026-07-27; medians of 5 interleaved kacrab/Java pairs. Both
throughput columns come from the same `records sent, … MB/sec` summary line, which
Java and kacrab compute identically — see the note below). The first two rows are
the default parity scenarios; the third drives the `MESSAGE_TOO_LARGE` batch-split
path, which kacrab clears in one broker round trip per batch fewer than Java:

| Scenario | kacrab | Java `kafka-producer-perf-test` |
| --- | ---: | ---: |
| 5M x 10 B, 16 partitions | **5.00M rec/s (47.6 MB/s)** | 3.70M rec/s (35.3 MB/s) |
| 100K x 10 KiB, 3 partitions | **49.5K rec/s (483 MB/s)** | 43.0K rec/s (420 MB/s) |
| 20K x 4 KiB, `max.message.bytes` below `batch.size` (batch-split path) | **59.3K rec/s (232 MB/s)** | 26.9K rec/s (105 MB/s) |
| Latency at Java's own rate (10 B) | **0.11 ms avg / 2 ms p99 / 4 ms max** | 0.32 ms / 2 ms / 131-140 ms |
| Peak RSS / CPU (10 B run, 2026-06-28) | **~68 MiB / ~2.7 s** | ~268 MiB / ~4.1 s |

**Consumer** (2026-07-02):

| Scenario | kacrab | Java `kafka-consumer-perf-test` |
| --- | ---: | ---: |
| 5M x 10 B, 16 partitions | **~17.6M rec/s** | ~9.3M rec/s |
| 100K x 10 KiB, 3 partitions | **~5.3 GB/s** | ~1.3 GB/s |
| Peak RSS / poll() max (10 B run) | **~18 MiB / ~8 ms** | ~286 MiB / ~111 ms |

Read the numbers with the caveats in mind:

- Single-node, RF=1, broker co-located with the client: this is a
  client-efficiency signal, not a production throughput claim. 10-byte rows
  inflate records/sec; the byte-rate columns are the more useful comparison.
- Both byte-rate columns are mebibytes despite the `MB/sec` label — Java's
  `ProducerPerformance` computes `bytes / elapsed / (1024 * 1024)` and prints it as
  `MB/sec` (`ProducerPerformance.java:508`), and kacrab's port does the same. They
  are directly comparable. An earlier revision of these tables quoted kacrab's own
  separate `MiB/s` summary line against Java's, which mixed two different lines and
  overstated the 10 KiB gap.
- **Latency is closed-loop saturation latency, and the two clients are not at the
  same offered load.** Each runs flat out, so each measures latency at its own
  saturation point — and kacrab is pushing ~35% more throughput than Java. Pinned
  to Java's own rate, kacrab wins or ties every latency metric: 0.11 ms avg,
  2 ms p99, 4 ms max against Java's 0.32 / 2 / 131-140 (5M x 10 B, 16 partitions,
  interleaved, 2026-07-27). Java's ~130 ms maxima are JVM client pauses; kacrab on
  the same broker stays under 16 ms. See
  [`benches/README.md`](benches/README.md#matched-load-latency).
- Every kacrab run above had zero retries/errors, with fully correct
  idempotence.

## Cancellation & drop semantics

Async Kafka clients get raced in `tokio::select!` and dropped on shutdown paths,
so here is what each public future does when it is cancelled — dropped before it
resolves — and what each client does when it is dropped without `close()`.

| Future | Cancel-safe | What a mid-await drop costs |
| --- | --- | --- |
| `Consumer::poll` | Yes, for records | No fetched record, position, or fetch session is lost. An in-flight `Fetch` stays owned by the consumer and is folded in by the next `poll`. |
| `Producer::send`'s `SendFuture` | Yes | Nothing. The record is already in the accumulator (`send` is a plain `fn`, not `async`) and still delivers; a registered callback still fires. Same semantics as dropping Java's returned `Future`. |
| `Producer::flush` / `close` | No | Dispatch continues, but you lose the guarantee that every prior send completed. Re-`flush` before relying on it. |
| `Consumer::commit_async` | No | If dropped during the coordinator lookup, the commit is never enqueued and the callback never fires. Once enqueued the handoff is synchronous and cannot be cancelled. |
| `Consumer::commit_sync` | No | The `OffsetCommit` may have reached the broker and applied. Treat the offset as indeterminate and re-commit. |
| `ShareConsumer::poll` | No | The `ShareFetch` response is discarded, so records the broker acquired for this member stay locked until the acquisition lock expires (`group.share.record.lock.duration.ms`) and are then redelivered. Nothing is lost; a record can be delivered twice. |
| `ShareConsumer::commit` | No | The `ShareAcknowledge` may have reached the broker and applied. Unapplied acknowledgements are not retried — those records keep their lock and are redelivered. |

Two caveats on `poll` specifically. It is cancel-safe with respect to **records**,
which is the property `select!` users need — a cancelled `poll` never drops
records on the floor, and never advances a position past a record you did not
receive. It is not *transactionally* cancel-safe: a drop can land mid-rebalance
or mid-auto-commit, which the next `poll` re-drives. And a drop during
auto-commit still consumes that `auto.commit.interval.ms` window, so the commit
slips to the next interval. If you need neither hazard, run `poll` in its own
`tokio::spawn` and use [`Consumer::wakeup`](#consumer) to break it out, which is
the Java-equivalent shape.

Dropping a client without closing it:

- **`Producer`** — buffered records do not vanish silently. Every incomplete
  delivery resolves as `Err(ProducerError::DeliveryDropped)`, waking pending
  `SendFuture`s and firing registered callbacks with that error. This is
  stricter than Java, where a garbage-collected producer loses buffered records
  with no notification. Use `close()` to flush them, or `close_now()` to fail
  them explicitly with `ProducerError::ProducerClosed`.
- **`Consumer`** — the heartbeat, async-commit, and in-flight fetch tasks are
  aborted, so no broker connection is kept alive by a detached task. Nothing is
  committed and the group is not left: the group waits out
  `session.timeout.ms` before rebalancing. Use `close()` to auto-commit and
  leave the group promptly.
- **`ShareConsumer`** — nothing is acknowledged and no share session is closed,
  so records still under acquisition become re-deliverable when their lock
  expires rather than being silently lost, and the group waits out the session
  timeout before reassigning. Use `close()` to flush pending acknowledgements,
  close the share sessions (which releases the rest immediately instead of at
  lock expiry), and leave the group.

## When not to use kacrab

- **You need brokers older than Kafka 4.x.** kacrab targets and is tested
  against 4.3.0 only. Version negotiation exists, but nothing older is
  exercised in CI, so treat old-broker support as unverified. Use
  `rust-rdkafka`.
- **You need Kafka Streams.** Out of scope, permanently — this is a client
  library. See [Status](#status).
- **You need years of production mileage.** kacrab is pre-1.0 and first
  published in July 2026. The public API can change between minor versions.
  `rust-rdkafka` wraps a library that has been in production for over a decade;
  that difference is real and no benchmark closes it.
- **Your workload is cross-DC or high-RTT.** Every number in this repo comes
  from a co-located broker at sub-millisecond RTT. Timeout, backoff, and
  in-flight interactions that only appear at 50–200 ms RTT are untested —
  that is item B2 in [`ROADMAP.md`](ROADMAP.md), and it is not done.
- **You need a long-soak guarantee.** The one published multi-hour run
  ([`SOAK-REPORT.md`](SOAK-REPORT.md)) went 4 h 17 m healthy across ~25
  broker-kill cycles, then hit a terminal wedge during a compound infra
  failure. The defects it exposed are fixed in code, but the confirming
  compound re-run has not happened yet. Memory soak (B3) is also outstanding.
- **You need bindings for another language.** There are none, and none are
  planned.

## Testing

```bash
make fmt-check clippy test    # workspace suite, all features
make check-features           # every feature selection a user can actually make
make deny                     # dependency & license checks
```

`check-features` exists because every other gate runs `--all-features`, which is
the one configuration nobody ships. An internal gated on the wrong surface
compiles there and breaks for whoever enables a single feature — which is exactly
how `--features consumer` alone stayed broken. CI runs it as its own job.

Real-broker smoke tests are ignored by default and run against the local compose
files (`docker-compose.{kafka,kafka-admin,auth,gssapi,tls,cluster}.yml`):

```bash
docker compose -f docker-compose.kafka.yml up -d
cargo test -p kacrab --test real_kafka_producer --all-features -- --ignored --nocapture
```

Protocol compatibility is also gated by a byte-for-byte Java oracle matrix
(`make test-protocol-java-matrix`; needs Java + Maven). Line coverage runs via
`cargo llvm-cov` with generated artifacts excluded. See [`Makefile`](Makefile)
and [`benches/README.md`](benches/README.md).

The Java oracle proves the decoders are correct on *well-formed* input. The
decoders that parse untrusted broker bytes are separately fuzzed for the other
half — garbage, truncation, hostile length prefixes — because `forbid(unsafe_code)`
rules out memory corruption but not a panic, an unbounded allocation, or a
non-terminating loop, and a panic on a client's decode path is a denial of
service. Eleven [`cargo-fuzz`](https://github.com/rust-fuzz/cargo-fuzz) targets
cover every parser that reads untrusted bytes, from the socket inward: the
length-prefixed frame, record-batch decoding, the generated response structs,
consumer-group member metadata, every compression codec, the SASL handshake, and
the OAUTHBEARER token endpoint:

```bash
cargo +nightly fuzz run record_batch_framed \
  fuzz/corpus/record_batch_framed fuzz/seeds/record_batch_framed -- \
  -dict=fuzz/kafka.dict -max_total_time=60
```

**The seeds and the dictionary do more work than the runtime does.** The Kafka
wire format is length-prefixed and version-gated, so an unseeded run spends its
budget rediscovering framing instead of exercising decoders. The committed seed
corpus in [`fuzz/seeds/`](fuzz/seeds/) is generated from the *same fixtures the
Java oracle uses* — every generated message, at every schema version, across six
fixture shapes — then minimised with `cargo fuzz cmin` to the subset that
carries the coverage. Regenerate it after adding a schema version:

```bash
cargo test -p kacrab-protocol --test java_interop -- \
  --ignored --nocapture generate_fuzz_corpus
```

[`fuzz/kafka.dict`](fuzz/kafka.dict) supplies the tokens random mutation will
never find, above all Kafka's `-1` length prefix: null is encoded as a negative
length, so without that token the fuzzer only ever explores the non-null side of
every nullable field. Measured effect, edges covered:

| target | unseeded | seeded + dictionary |
| --- | ---: | ---: |
| `record_batch_decode` | 150 | **984** |
| `record_batch_framed` | 774 | **1591** |
| `response_decode` | — | **14740** |
| `decompress` | — | **1230** |
| `frame_decode` | — | **79** |
| `consumer_protocol_metadata` | — | **386** |
| `oauth_http_response` | — | **1013** |
| `scram_server_first` | — | **245** |
| `scram_server_first_nonced` | 199 | **743** |
| `scram_server_final` | — | **322** |
| `jaas_option` | — | **137** |

`consumer_protocol_metadata` sits at a trust boundary none of the others do.
Every other parser here reads bytes from the broker or the operator; this one
reads bytes *another consumer wrote*. `ConsumerProtocolSubscription` is decoded
by the group leader for every member, and `ConsumerProtocolAssignment` by every
follower and by any admin client describing the group — so anyone authorised to
join a group can feed the decoder of every other member. `response_decode` does
not reach it: these travel as opaque `Bytes` inside `JoinGroupResponse` and
`SyncGroupResponse`, with their own version prefix.

Three libFuzzer flags are set deliberately rather than left to default, and the
reasons are worth repeating because two of them bit this suite:

- **`-max_len`** does *not* default to 4096 when a corpus is supplied — it
  becomes the size of the largest file in it. The committed seeds are small, so
  leaving it unset had silently capped `frame_decode` at **12 bytes**, making the
  seeded target worse than an unseeded one.
- **`-malloc_limit_mb`** bounds a single allocation and defaults to the rss
  limit. At 4096 it could not see the ~120 MB preallocation a 49-byte record
  batch can request. It is 64 MiB for every target that does not decompress, and
  above `MAX_DECOMPRESSED_LEN` for those that do, since a 1 GiB expansion there
  is by design.
- **`-timeout`** catches CPU amplification, which surfaces as a hang, not a
  crash. Tuned per target: the SCRAM targets legitimately run PBKDF2 up to
  `MAX_SCRAM_ITERATIONS`.

**The SASL targets are the ones that matter most.** SCRAM is mutual
authentication, but the client only proves the *server* at server-final — so
everything the server-first parser touches is reachable by anyone who can answer
on the broker's address. Those parsers are `pub(crate)`, so `kacrab` exposes them
to `fuzz/` as `fn(&[u8])` shims behind an internal `__fuzzing` feature that is
`#[doc(hidden)]`, off by default, and exempt from semver.

`scram_server_first` gets the same two-target treatment as record batches, for
the same reason. `client_final` rejects any server-first whose nonce does not
extend the client's own randomly generated nonce, which a fuzzer cannot guess, so
raw bytes stall at 199 edges and never reach the salt decode or the PBKDF2
derivation. `scram_server_first_nonced` satisfies that gate in the harness so
mutations land on the fields behind it — which is how the unbounded iteration
count below became reproducible.

Record batches get two targets, and the reason is worth stating because it is
the difference between fuzzing and the appearance of it. `decode_next_batch`
validates CRC32C *before* it reads the magic byte, the record count, the varint
record headers, or the compressed blob. Random bytes clear a CRC32C check with
probability 2^-32, so raw bytes alone only ever exercise framing and rejection.
`record_batch_framed` hands the fuzzer the CRC-covered region and builds correct
framing around it, so every mutation lands inside the decoder — that is what
found the header-count OOM fixed in `Record::decode`. Both targets are kept: the
framed one constructs CRC and length prefixes correctly by definition, so it can
never find a bug in them.

None of this makes the decoders *proven* safe. It makes them survivors of
~20M structured inputs per campaign, which is a different and weaker claim.

They run [nightly in CI][fuzz-url] at 15 minutes per target, and as a 60-second
smoke on any PR touching `kacrab-protocol/`. The fuzz crate lives outside the
workspace ([`fuzz/`](fuzz/)) because cargo-fuzz needs nightly and a sanitizer,
which the pinned stable toolchain cannot provide. Decompression is bounded by
`MAX_DECOMPRESSED_LEN` in every codec, so a declared-size zip bomb is rejected
rather than allocated; the `decompress` target asserts that bound holds.

## Workspace

Published on crates.io:

- [`kacrab/`](kacrab/): public runtime crate: config, wire, common, producer,
  consumer, admin —
  [crates.io](https://crates.io/crates/kacrab) ·
  [docs.rs](https://docs.rs/kacrab)
- [`kacrab-protocol/`](kacrab-protocol/): protocol primitives, generated Kafka
  schemas, record batch codecs, compression, Java interop tests —
  [crates.io](https://crates.io/crates/kacrab-protocol) ·
  [docs.rs](https://docs.rs/kacrab-protocol)
- [`kacrab-macros/`](kacrab-macros/): helper macros for typed config surfaces
  (use the re-export from `kacrab` rather than depending on it directly) —
  [crates.io](https://crates.io/crates/kacrab-macros) ·
  [docs.rs](https://docs.rs/kacrab-macros)

Internal (not published):

- [`kacrab-codegen/`](kacrab-codegen/): protocol and config code generation
  from upstream Kafka.
- [`examples/`](examples/): runnable producer/consumer/admin examples.
- [`benches/`](benches/): internal benchmark crate: real-Kafka harnesses and
  microbenchmarks.
- [`fuzz/`](fuzz/): `cargo-fuzz` targets for the decoders that parse untrusted
  broker bytes. Outside the workspace — it needs nightly plus a sanitizer.

## License

Authored and maintained by `pirumu`. Licensed under either of:

- [MIT license](LICENSE-MIT)
- [Apache License, Version 2.0](LICENSE-APACHE)
