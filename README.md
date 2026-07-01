# Kacrab

A Rust-native Kafka client (producer, admin, and consumer), built from the Kafka
protocol up — a native implementation, not a `librdkafka` wrapper. The producer is
wire-compatible with the Java client — including the idempotent sequence/epoch
recovery state machine — and is memory- and CPU-efficient (native, no JVM). The
admin client covers the full Apache Kafka 4.3.0 `Admin` operation surface (62
operations) and is verified against a real broker. The consumer supports manual
assignment, topic and pattern subscription, and both group protocols — classic
(eager and `cooperative-sticky`) and KIP-848 server-side — with fetch, offsets,
interceptors, and metrics, verified end-to-end against a real broker.

* **Java-compatible auth, producer, admin, and consumer**: the authentication,
  producer, admin, and consumer surfaces use Kafka property names, defaults,
  protocol flow, and wire semantics as the compatibility target.
* **Full admin client** (`admin` feature): topics, partitions, configs
  (incremental), ACLs, consumer groups & offsets, transactions, delegation
  tokens, quotas, SCRAM credentials, partition reassignments, `KRaft` quorum,
  and the Kafka 4.x share/streams group families — 62 operations at Apache Kafka
  4.3.0 parity, verified end-to-end against a real broker.
* **Consumer client** (`consumer` feature): manual partition assignment,
  topic and pattern (regex) subscription, and both group protocols — the classic
  `JoinGroup`/`SyncGroup` protocol with the `range`/`roundrobin`/`sticky` eager
  assignors and the incremental `cooperative-sticky` assignor, plus the KIP-848
  server-side protocol (`group.protocol=consumer`, single `ConsumerGroupHeartbeat`
  RPC). `Fetch` with `auto.offset.reset`, `max.poll.records`, incremental fetch
  sessions (KIP-227), and `seek`/`pause`/`resume`; offset
  `commit_sync`/`commit_async`/`committed` with background auto-commit;
  OffsetForLeaderEpoch truncation detection (KIP-320); a background heartbeat
  task; typed deserializers; `ConsumerInterceptor`s; and `metrics()`. Verified
  end-to-end against a real Apache Kafka 4.3.0 broker across ten scenarios
  (single subscriber, two-consumer rebalance, cooperative-sticky, roundrobin,
  pattern, interceptors, offset queries, and KIP-848).
* **Native Rust, not a `librdkafka` wrapper**: the Kafka protocol, wire, and
  producer logic are pure Rust, with workspace `unsafe_code` forbidden. The
  dependency tree is not entirely C-free, though: the TLS crypto provider
  (`rustls` + `aws-lc-rs`) is C/assembly and is always pulled in, and the
  optional `zstd`, `lz4-hc`, and `gssapi` features add C. A fully C-free build
  uses a pure-Rust `rustls` provider and only the `gzip`/`snappy`/`lz4` codecs.
* **Generated protocol**: Kafka request/response structs are generated from
  Apache Kafka schemas and checked against the Kafka Java client oracle.
* **Efficient producer**: batching, linger, bounded memory, idempotence
  (multi-in-flight per partition with Kafka-faithful sequence ordering and
  failure recovery), transactions, compression, metadata routing, interceptors,
  Kafka-named metrics, and multi-broker dispatch are first-class design points.
  On a single-node broker it holds throughput parity with the Java client while
  using ~4x less memory and ~1.5x less CPU (native vs JVM) — see
  [Benchmarks](#benchmarks).
* **Tokio-native wire layer**: async broker sessions, ApiVersions negotiation,
  metadata refresh, bounded in-flight requests, request timeouts, and explicit
  connection cleanup.

[![MIT licensed][mit-badge]][mit-url]
[![Apache-2.0 licensed][apache-badge]][apache-url]

[mit-badge]: https://img.shields.io/badge/license-MIT-blue.svg
[mit-url]: LICENSE-MIT
[apache-badge]: https://img.shields.io/badge/license-Apache--2.0-blue.svg
[apache-url]: LICENSE-APACHE

## Documentation

- **[Design & Internals book](https://pirumu.github.io/kacrab/)** — architecture,
  the idempotent producer state machine, the SASL/TLS handshakes, protocol
  codegen, and how every path is verified against real brokers. Source in
  [`docs-book/`](docs-book/).
- **API reference** — [docs.rs/kacrab](https://docs.rs/kacrab) (after the first
  crates.io release).

## Status

> Warning: `kacrab` is pre-release software. It has useful protocol, wire,
> auth, producer, and admin coverage, but the public API and runtime behavior
> are not stable release guarantees yet.

Protocol, wire, auth, producer, and admin now have a usable baseline. The
producer is the most mature surface: on a single-node broker it holds throughput
parity with
the Java client at `acks=all` + idempotence (~4.7M × 10B records/sec ≈ 45 MiB/s;
the ~12% records/sec edge is broker-bound noise, not a language win) while using
~4x less memory and ~1.5x less CPU — Java keeps a lower tail latency (see
[Benchmarks](#benchmarks)). Multi-broker dispatch, leadership-change recovery,
the SASL/TLS surface, and every compression codec are verified end-to-end
against real brokers. The admin client implements the full Apache Kafka 4.3.0 `Admin` operation surface
(62 operations) and is verified against a real broker across every routing path
(controller, coordinator with transient-error retry, per-leader, and broadcast);
see [Admin](#admin). The consumer supports manual assignment and classic
group subscription (see [Consumer](#consumer)); the remaining broad focus is
sustained stress / latency testing (see
[Production acceptance](#production-acceptance)).

Test coverage (`cargo llvm-cov`): **~87% maintained-source** line coverage
(generated protocol excluded), with the **producer module at ~92%**.
The append/dispatch/idempotent-recovery hot paths, the murmur2 partitioner
(byte-exact for every key length), the transaction state machine, interceptors,
and the Kafka-style metrics library (sensors, stats, quotas, reporters) are
directly tested; remaining gaps are mechanical error-clone arms and rare
defensive branches. The raw whole-workspace figure (~66%) is lower only because
it counts generated `kacrab-protocol` message structs for APIs not yet wired
(streams, and the KIP-848 consumer group protocol).

Auth, producer, and admin are treated as **Java-compatible targets** for the
implemented surface:

- Kafka config keys work through `ClientConfig` and
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
- `ProducerInterceptor` mirrors Kafka's `configure`/`onSend`/`onAcknowledgement`/
  `close` plus the `ClusterResourceListener.onUpdate` hook, and metrics are
  published under their Kafka names (`producer-metrics:*` /
  `producer-topic-metrics:*`).
- Generated request/response encoding uses `kacrab-protocol`, not handwritten
  byte patches.
- JVM-only callback handler classes cannot be loaded inside Rust; use the
  native Rust SASL authenticator hook for custom auth flows.
- The whole SASL/TLS surface is verified end-to-end against real Apache Kafka
  4.3.0 brokers — every SASL mechanism over both plaintext and TLS, plus
  one-way `SSL` and mutual TLS — and handshake/auth failures fail fast with the
  broker's reason instead of retrying until the request timeout, matching Java's
  non-retriable `SaslAuthenticationException` / `SslAuthenticationException`.

## Current Status

- [x] Protocol foundation
  - [x] Kafka protocol primitives, record batches, compression codecs, and
        generated request/response structs.
  - [x] Apache Kafka 4.3.0 schema snapshots and Java oracle compatibility
        matrix for generated wire messages.
- [x] Core config and auth foundation
  - [x] Kafka configuration facade and typed client configs.
  - [x] TLS/SASL properties for `SSL`, `SASL_SSL`, and `SASL_PLAINTEXT`.
  - [x] SASL `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`, `OAUTHBEARER`,
        feature-gated `GSSAPI`, and native Rust custom auth hooks.
  - [x] Every SASL mechanism (`PLAIN`, `SCRAM-SHA-256/512`, `OAUTHBEARER`,
        `GSSAPI`) and TLS mode (`SSL`, `SASL_SSL`, mutual TLS) verified
        end-to-end against real Apache Kafka 4.3.0 brokers, including fail-fast
        rejection of bad credentials, expired tokens, and untrusted
        certificates (`docker-compose.{auth,gssapi,tls}.yml`).
- [x] Wire usable baseline
  - [x] Broker sessions with TCP/TLS/SASL, ApiVersions negotiation, request
        encoding, response dispatch, and metadata fetch.
  - [x] Bounded in-flight requests, request timeouts, connection-closed cleanup,
        broker dispatch, and per-session write-buffer reuse.
  - [x] Lower-allocation correlation storage (fixed-slot in-flight pipeline, no
        per-request map), leadership-error invalidation (metadata refresh on
        leader change), and a reconnect/backoff policy (exponential + jitter,
        resets on a successful connection).
  - [x] Multi-broker dispatch and leadership-change failover verified against a
        real 3-broker KRaft cluster (`docker-compose.cluster.yml`): records route
        to every broker's leaders, and a broker loss re-routes affected
        partitions to their new leaders without wedging co-batched ones.
  - [ ] Sustained multi-broker stress tests and cross-DC / high-RTT coverage
        (functionally verified, but not yet load- or latency-tested).
- [x] Producer usable baseline
  - [x] Public `Producer` API with Kafka config keys and synchronous
        `send`/`send_with_callback` (Kafka `Producer.send` shape) returning a
        `SendFuture`.
  - [x] Batching by topic-partition, linger, bounded memory, `max.block.ms`,
        compression (`gzip`/`snappy`/`lz4`/`zstd`, each round-tripped through a
        real broker), and delivery handles.
  - [x] Metadata routing with default partition assignment, keyed murmur2
        partitioning, sticky/adaptive unkeyed assignment, and multi-broker
        dispatch.
  - [x] Retry backoff, delivery timeout across retries, broker response error
        propagation, and leadership-error retry path.
  - [x] Kafka-faithful idempotent producer: per-partition multi-in-flight,
        `firstInFlightSequence` ordered retry, `maybeResolveSequences` deferred
        epoch bump, stale-epoch re-stamp, single-bump recovery, sequence
        wraparound, and duplicate-sequence dedup.
  - [x] Transactional control flow through coordinator lookup, `InitProducerId`,
        `AddPartitionsToTxn`, `TxnOffsetCommit`, and `EndTxn`.
  - [x] `ProducerInterceptor` lifecycle (`configure`/`on_send`/`on_ack`/
        `on_error`/`on_update`/`close`) and Kafka-named producer + per-topic
        metrics including the buffer-pool gauges.
  - [x] Throughput parity with the Java client on a single-node broker at the
        default `acks=all` + idempotence config, at ~4x less memory / ~1.5x less
        CPU (see [Benchmarks](#benchmarks)).
  - [x] Leadership-change refresh: a concurrent multi-partition burst recovers
        when a broker is lost (a wire disconnect invalidates the stale leader so
        the retry re-fetches metadata and re-routes), verified against a real
        3-broker cluster.
  - [ ] Production acceptance: sustained multi-broker stress, memory soak, and
        latency-percentile gates on realistic multi-broker workloads (scoped
        under [Production acceptance](#production-acceptance)).
- [x] Consumer usable baseline (`consumer` feature)
  - [x] Manual assignment, `Fetch` (per-leader), `auto.offset.reset`
        (`earliest`/`latest`/`none` via `ListOffsets`), `max.poll.records`, and
        `seek`/`seek_to_beginning`/`seek_to_end`/`position`/`pause`/`resume`/
        `wakeup`. Records are bytes-first (`ConsumerRecord.key/value:
        Option<Bytes>`).
  - [x] Classic group coordination: `subscribe`, `FindCoordinator`,
        `JoinGroup`/`SyncGroup`/`Heartbeat`/`LeaveGroup`, the `range` assignor
        with eager rebalancing, resume-from-committed-offset on assignment, and
        poll-throttled heartbeats with rejoin on rebalance signals.
  - [x] Offsets: `commit_sync`/`commit_sync_offsets`/`committed` (leader-epoch
        aware) and `group_metadata`, against the group coordinator.
  - [x] `Consumer::new`/`from_client_config`/`from_properties`/`from_map`, wired
        through the same auth/transport stack (SASL/TLS) as the producer.
  - [x] Verified end-to-end against a real Apache Kafka 4.3.0 broker
        (`kacrab/tests/real_kafka_consumer.rs`): manual assign + commit, a single
        subscriber owning both partitions, and two consumers rebalancing to one
        partition each.
  - [ ] Refinements: a dedicated background heartbeat task, cooperative-sticky
        assignment, incremental fetch sessions (KIP-227), `OffsetForLeaderEpoch`
        validation, `offsets_for_times`/`beginning`/`end_offsets`, and the
        KIP-848 consumer group protocol.
- [x] Admin usable baseline (`admin` feature)
  - [x] Full Apache Kafka 4.3.0 `Admin` operation surface — 62 operations:
        topics/partitions, describe/incremental-alter configs, ACLs, consumer
        groups & offsets, `list_offsets`/`delete_records`/`elect_leaders`,
        producers/transactions (`describe`/`list`/`abort`/`fence`), partition
        reassignments, delegation tokens, client quotas, user SCRAM credentials,
        log dirs, features, `KRaft` quorum/voters, and the Kafka 4.x share &
        streams group families, plus `client_instance_id`/`metrics` accessors.
  - [x] `metrics()` returns a typed `AdminMetricsSnapshot` (request totals,
        error totals, average/max request latency, and the shared wire
        buffer-pool counters) — kacrab's native analogue of Java's
        `Admin.metrics()`.
  - [x] `AdminClient::new`/`from_client_config`/`from_properties`/`from_map` and
        `ClientConfig::create_admin`, wired through the same auth/transport stack
        (SASL/TLS) as the producer.
  - [x] Routing: controller routing with `NOT_CONTROLLER` refresh-retry,
        `FindCoordinator` group/transaction coordinator routing with
        transient-coordinator-error retry, per-leader request batching, and
        broadcast; topic-id resolution for the v10 offset APIs.
  - [x] Verified end-to-end against a real Apache Kafka 4.3.0 broker across all
        routing paths, including ACLs, quotas, SCRAM, and delegation tokens over
        SASL (`kacrab/tests/real_kafka_admin*.rs`,
        `docker-compose.kafka{,-admin}.yml` + `docker-compose.auth.yml`).
- [ ] Streams
  - [ ] Topology API, processor runtime, repartitioning, state stores, and
        changelog topics.
  - [ ] Exactly-once stream processing on top of the producer transaction path.

## Production acceptance

The functional multi-broker work is done and verified against a real 3-broker
KRaft cluster: multi-broker dispatch routes to every broker's leaders, and a
broker loss re-routes affected partitions to their new leaders without wedging
co-batched ones (`kacrab/tests/real_kafka_cluster.rs`,
`docker-compose.cluster.yml`).

What remains is **measurement and acceptance under load**, not correctness. None
of it is a quick `[x]` — each needs dedicated infrastructure, a time budget, and
in some cases an SLO threshold that is a product decision, not something the
implementation can assert for you. The items below are scoped so they can be
picked up deliberately rather than rushed.

> A few minutes of `docker compose` proves a path *works*; proving it *holds up*
> needs hours of load, a tuned network, and agreed pass/fail gates.

### B1 — Sustained multi-broker stress

**Goal:** confirm the multi-broker dispatch path stays correct and bounded under
high, continuous load across all brokers (no unbounded memory growth, no stuck
partitions, no reordering/duplication with idempotence on).

**Why not done:** the existing benches are short single-shot runs on a single
node. Multi-broker behavior under sustained pressure (queue depth, in-flight
caps, backpressure, metadata churn) is unmeasured over time.

**Proposed approach:**
- Drive `producer_kafka_bench` (or a new long-run harness) against
  `docker-compose.cluster.yml` for a fixed duration (e.g. 1–4h) at a target
  rate, fanning records across all 6 partitions (leaders spread via a PREFERRED
  election).
- Mix in periodic faults: rolling broker restarts, leader elections, and a
  partition-reassignment, so dispatch is re-routing continuously.
- Assert: zero delivery failures (acks=all + idempotence), `retries` bounded,
  buffered bytes return to ~0 between bursts, and broker-side log end-to-end
  record counts match what was sent.

**Decisions needed:** target rate, record size, duration, fault cadence.

**Acceptance:** no lost/duplicated/reordered records over the full run;
steady-state memory and in-flight counts; no permanently stuck partition.

### B2 — Cross-DC / high-RTT coverage

**Goal:** verify behavior when broker links have real latency, jitter, and loss
(the dispatch/retry/timeout tuning is currently only exercised at ~0 RTT).

**Why not done:** co-located brokers on one machine have sub-millisecond RTT, so
timeout/backoff/in-flight interactions that only appear at 50–200 ms RTT are
untested. This needs network emulation, not just more brokers.

**Proposed approach:**
- Add `tc netem` (delay + jitter + small loss) on the broker containers'
  interfaces, or run brokers behind a latency-injecting proxy (e.g. `toxiproxy`),
  to emulate inter-DC links.
- Re-run B1's workload at representative RTTs (e.g. 50 ms, 150 ms) and confirm:
  throughput degrades gracefully (pipelining keeps up via `max.in.flight`),
  `request.timeout.ms` / `delivery.timeout.ms` are not tripped spuriously, and
  retries/backoff behave under jitter and loss.
- Confirm a high-RTT leader change still recovers within `delivery.timeout.ms`.

**Decisions needed:** RTT/jitter/loss profiles to target; whether `toxiproxy`
(portable, scriptable) or `tc netem` (closer to the kernel path) is preferred.

**Acceptance:** correct delivery and bounded retries at each profile; no spurious
timeouts attributable to the client rather than the emulated link.

### B3 — Memory soak

**Goal:** prove there is no leak or unbounded growth over a long run (buffer
pool, in-flight maps, idempotent state, metadata cache).

**Why not done:** runs so far are short; slow growth (per-connection,
per-leader-change, per-metadata-refresh) would not show up.

**Proposed approach:**
- Run B1's workload for an extended period (e.g. 8–24h) with RSS sampled
  periodically and the buffer-pool / in-flight gauges scraped from the producer
  metrics.
- Include churn (reconnects, leader changes, topic metadata refreshes) so any
  per-event allocation that is never freed accumulates visibly.
- Optionally run under a leak detector / heap profiler for a shorter window.

**Decisions needed:** soak duration; acceptable RSS ceiling/slope.

**Acceptance:** RSS plateaus (no upward trend) after warm-up; buffer-pool and
in-flight gauges return to baseline between bursts.

### B4 — Latency-percentile gates

**Goal:** turn latency from an anecdote into a gated metric (p50/p99/p999
end-to-end produce latency) so regressions are caught.

**Why not done:** benches report throughput and a memory/CPU comparison, but
there is no percentile measurement harness and — more importantly — **no agreed
SLO thresholds**. A "gate" needs target numbers, which are a product call.

**Proposed approach:**
- Extend the bench harness to record per-record send→ack latency and emit
  p50/p99/p999 (HdrHistogram-style), under both single-node and multi-broker.
- Compare against the Java client on the same hardware/workload to set realistic
  targets (the README already notes Java keeps a lower tail latency — quantify
  it).
- Wire the percentiles into CI as a soft gate first (report + alert on
  regression), then a hard gate once thresholds are agreed.

**Decisions needed:** the p99/p999 targets (absolute and/or relative-to-Java),
the workload they apply to, and soft-vs-hard gating in CI.

**Acceptance:** percentiles measured and reported per run; CI flags a regression
beyond the agreed threshold.

### Shared prerequisites

- A longer-lived cluster than the throwaway compose (or the compose with
  `KAFKA_HEAP_OPTS` raised and data volumes sized for a multi-hour run).
- A machine/runner that can sustain the target rate without itself becoming the
  bottleneck (co-located brokers share CPU with the client — for real numbers,
  brokers and client should be on separate hosts).
- Network emulation tooling for B2 (`toxiproxy` or `tc netem`).
- Agreed thresholds for B3/B4 before they can be "gates" rather than reports.

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

The admin client is behind the `admin` feature; GSSAPI/Kerberos support is
behind the `gssapi` feature.

```toml
kacrab = { git = "https://github.com/pirumu/kacrab", features = ["producer", "admin"] }
```

## Producer

The public producer API is intentionally close to Kafka config style
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

    // `send` is synchronous like Kafka's `Producer.send`: it returns immediately
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
itself), matching Kafka's thread-safe `Producer.send`. `send_with_callback`
additionally invokes a callback on acknowledgement. Batching is automatic inside
the producer accumulator/sender based on `batch.size`, `linger.ms`, buffer
memory, partition routing, and flush/close boundaries; there is no separate
public batch send API.

Interceptors and Kafka-named metrics use the Kafka surface:

```rust
// ProducerInterceptor: configure(client.id), on_send / on_ack / on_error,
// on_update(cluster id), close — all panic-isolated like the Kafka interceptor chain.
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

### Custom serializers

Serializers are a plain Rust trait, `ProducerSerializer<T>` — implement it for
your own type and pass the serializer value to `build_with_serializers`. There
is no `key.serializer` / `value.serializer` class-name configuration: the key
and value types are checked at compile time. Built-in serializers
(`StringSerializer`, `IntegerSerializer`, `BytesSerializer`, `ListSerializer`,
…) are provided for the common cases.

```rust
use bytes::Bytes;
use kacrab::producer::{
    Producer, ProducerRecord, ProducerSerializer, RecordHeader, Result, StringSerializer,
};

struct OrderEvent { order_id: u64, amount_cents: u64 }

struct OrderEventSerializer;
impl ProducerSerializer<OrderEvent> for OrderEventSerializer {
    fn serialize(
        &self,
        _topic: &str,
        _headers: &mut Vec<RecordHeader>,
        value: Option<&OrderEvent>,
    ) -> Result<Option<Bytes>> {
        Ok(value.map(|e| {
            let mut bytes = Vec::with_capacity(16);
            bytes.extend_from_slice(&e.order_id.to_be_bytes());
            bytes.extend_from_slice(&e.amount_cents.to_be_bytes());
            Bytes::from(bytes)
        }))
    }
}

// Key uses the built-in StringSerializer; value uses the custom one. K = String
// and V = OrderEvent are inferred from the serializer types.
let mut producer = Producer::builder()
    .set("bootstrap.servers", "127.0.0.1:9092")
    .build_with_serializers(StringSerializer::default(), OrderEventSerializer)
    .await?;

let event = OrderEvent { order_id: 42, amount_cents: 1_999 };
let _delivery = producer.send(
    ProducerRecord::new("orders", 0),
    Some(&"order-42".to_owned()),
    Some(&event),
)?;
```

The consumer returns bytes-first records (`ConsumerRecord.key/value:
Option<Bytes>`); a typed **deserializer** layer on top is a planned refinement
(see [Current Status](#current-status)). A runnable version of the serializer
example above lives in
[`examples/typed_serializer.rs`](examples/typed_serializer.rs).

## Admin

The admin client (`admin` feature) mirrors Java's `Admin` with snake_case
methods that return plain `Result<T>` / `Result<()>` and a per-call options
struct. Build it from the same Kafka config keys as the producer (including
`security.protocol`/`sasl.*`/TLS), then call any of the 62 operations:

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

    let cluster = admin.describe_cluster().await?;
    println!("controller: {:?}", cluster.controller.map(|node| node.id));
    Ok(())
}
```

The full surface — configs (incremental), ACLs, consumer/share/streams groups &
offsets, transactions, delegation tokens, quotas, SCRAM credentials, partition
reassignments, `KRaft` quorum/voters, and more — is covered; every operation is
verified against a real Apache Kafka 4.3.0 broker
(`kacrab/tests/real_kafka_admin*.rs`). Shared `org.apache.kafka.common` domain
types (`TopicPartition`, `OffsetAndMetadata`, `Node`, ...) live in
`kacrab::common` and are re-exported by both `producer` and `admin`.

A runnable version — describe cluster, create/list/describe topics, alter
configs, add partitions, list offsets, and delete — lives in
[`examples/admin.rs`](examples/admin.rs)
(`cargo run -p kacrab-examples --example admin`).

## Consumer

The consumer client (`consumer` feature) mirrors Java's `Consumer` with
snake_case methods and the same constructors as the other clients. It supports
manual partition assignment and classic consumer-group subscription:

```rust
use std::time::Duration;
use kacrab::{common::TopicPartition, consumer::Consumer};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Group subscription (rebalances across other members in the same group):
    let mut consumer = Consumer::from_map([
        ("bootstrap.servers", "localhost:9092"),
        ("group.id", "orders-workers"),
        ("auto.offset.reset", "earliest"),
    ])
    .await?;
    consumer.subscribe(["orders"])?;

    loop {
        let records = consumer.poll(Duration::from_secs(1)).await?;
        for record in &records {
            println!("{}-{}@{}", record.topic, record.partition, record.offset);
        }
        consumer.commit_sync().await?;
    }
}
```

Or take direct control with `assign(vec![TopicPartition::new("orders", 0)])` and
`seek`/`position`/`pause`. Records are bytes-first (`ConsumerRecord.key/value:
Option<Bytes>`). The classic group path runs `FindCoordinator` +
`JoinGroup`/`SyncGroup`/`Heartbeat` with the `range` assignor and eager
rebalancing; `commit_sync`/`committed` carry the leader epoch. Everything is
verified end-to-end against a real Apache Kafka 4.3.0 broker
(`kacrab/tests/real_kafka_consumer.rs`). See the book's
[consumer chapter](docs-book/src/consumer.md) and `docs/consumer-design.md` for
the design and the phased plan.

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

For line coverage, the CI gate runs [`cargo llvm-cov`][llvm-cov] and excludes
generated Kafka artifacts and benchmark fixtures via `--ignore-filename-regex`,
so the signal reflects maintained source. LLVM source-based coverage runs the
tests at near-native speed, so the suite's many timeout/blocking-based async
tests stay reliable under instrumentation (tarpaulin's slowdown did not).
Coverage is a regression signal for code we maintain; protocol compatibility is
still gated by generated round trips and the Java oracle matrix.

```bash
cargo llvm-cov --workspace --all-features \
  --ignore-filename-regex '(benches/|kacrab-codegen/src/main\.rs|kacrab-macros/src/lib\.rs|kacrab/src/config/catalog\.rs|kacrab-protocol/src/generated)'
```

Latest measured coverage:

- Maintained-source line coverage: **~87.5%** (27,860 / 31,849 lines), generated
  protocol excluded. Producer module ~92%.
- The raw all-files figure is ~63%, dominated by generated `kacrab-protocol`
  message structs for APIs not yet wired (consumer/admin/streams).

[llvm-cov]: https://github.com/taiki-e/cargo-llvm-cov
- Java oracle fixture inventory: 6 release-grade fixture families × 625
  schema/version cases = 3,750 generated fixture cases.

## Benchmarks

Local benchmark hooks live in [`benches/`](benches/); `producer_kafka_bench`
drives a real broker through the public synchronous `send` path. These are
single-node, RF=1, broker co-located with the client — read them as a
client-efficiency signal, not a production throughput claim (no multi-broker /
cross-DC numbers yet).

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

# Java reference — same broker, topic, and config (Kafka 4.3.0: --command-property
# takes space-separated PROP=VALUE; --producer-props is deprecated)
kafka-producer-perf-test.sh --topic kacrab-16p --num-records 5000000 \
  --record-size 10 --throughput -1 \
  --command-property bootstrap.servers=127.0.0.1:9092 acks=all enable.idempotence=true
```

Head-to-head on the same single-node broker, topic, and config (`acks=all` +
`enable.idempotence=true`), 5 runs, `max.in.flight=5`, no compression:

| Scenario | kacrab `producer_kafka_bench` | Java `kafka-producer-perf-test.sh` |
| --- | ---: | ---: |
| 5M x 10B, 16 partitions | **4.70M rec/sec**; 44.8 MiB/sec; lat avg ~1.3 ms, p99 ~8 ms (depth 5; see below); retries 0, errors 0, in-flight stalls 0 | 4.21M records/sec; 40.1 MB/sec; lat avg **0.27 ms**, p99 **1 ms** |
| 2M x 10B, 1 partition | **4.69M rec/sec**; 44.7 MiB/sec; lat avg ~0.2 ms, p99 ~5 ms; retries 0, errors 0 | -- |
| 100K x 10 KiB, 16 partitions (batch.size=256 KB) | **~613 MiB/sec** (62.8K rec/sec); lat avg 68 ms, p99 ~150 ms; retries 0, errors 0 | 515 MB/sec (52.8K records/sec); lat avg 43 ms, p99 104 ms |

Large records (not just tiny ones): at 10 KiB, kacrab sustains **~613 MiB/sec**
(≈ Java's 515 MB/sec, slightly ahead on bytes, behind on latency — the same
depth tradeoff). **Caveat that matters:** a 10 KiB record exceeds half of the
default 16 KiB `batch.size`, so it lands one-record-per-batch → one record per
`acks=all` produce request → broker-fsync-bound (~3.7 MiB/sec). Raising
`batch.size` to 256 KB (≈ 24 records/batch) restores ~165x throughput. This is
standard Kafka tuning, not a client quirk — the Java client behaves identically.

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
| Total CPU (user+sys) | ~2.7 s | ~4.1 s | ~1.5x more\* |

\*Java's CPU includes one-time JVM startup + JIT warmup, not fully amortized over
a 5M-record run — the ratio narrows on longer runs. Peak RSS is steady-state.

The ~12% throughput edge is modest because throughput here is **broker-bound** —
both clients spend most of the run waiting on `acks=all` round-trips, so the
client language barely moves that number. The native-vs-JVM advantage shows up
instead in efficiency: kacrab uses **~4x less memory** (no JVM heap/metaspace)
and meaningfully less CPU per record.

Bench knobs: `KACRAB_BENCH_TOPIC`, `KACRAB_BENCH_MESSAGES`, `KACRAB_BENCH_RUNS`,
`KACRAB_BENCH_ACKS1` (acks=1), `KACRAB_BENCH_BATCH_SIZE`. The 16- and
1-partition topics (`kacrab-16p`, `kacrab-1p`) must exist on the broker. The
in-process criterion microbenchmarks under `benches/` exercise the accumulator,
wire pipeline, and dispatcher CPU paths in isolation.

Limits, read these before trusting the headline:

- The headline rows use **10-byte records**, so `records/sec` is inflated by tiny
  records — read the **MiB/sec** column (~45 MiB/s) as the meaningful figure. The
  10 KiB row (~613 MiB/s) is the realistic large-record number; sizes between
  10 B and 10 KiB are not separately charted.
- **Latency is closed-loop saturation latency** (`--throughput -1`), measured
  from just-before-send to the ack callback. It is not an open-loop SLA latency
  at a fixed offered rate; under saturation it reflects queueing, not service
  time, for both clients.
- Single-node, RF=1, broker co-located with the client. Not release gates; no
  CPU/allocator profiles or broker disk metrics. Cross-DC / high-RTT links —
  where deeper per-connection pipelining helps most — are not represented.
- kacrab reports payload MiB/sec; the Java perf tool reports decimal MB/sec.


## Workspace

- `kacrab/` - public runtime crate: config, wire, common, producer, admin.
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
