# Consumer Module Design

Status: **design / not yet implemented**. Date: 2026-07-01. Branch target: a fresh
`feat/consumer-*` branch off `master`.

Reference: Apache Kafka 4.3.0 Java consumer, ground-truth source in
`upstream/kafka-4.3.0/clients/src/main/java/org/apache/kafka/clients/consumer/`.
"Java-compatible" means Kafka-protocol/behaviour-compatible, not a literal port —
the same lens used for the producer and admin (see the book's design-decisions
chapter).

This document is the plan of record: module layout, public API, internal
components, concurrency model, and a phased implementation sequence. It is meant
to be executable one phase at a time, each phase landing behind the `consumer`
cargo feature and verified against a real broker before the next starts.

---

## 1. Scope decisions (decided up front)

1. **Classic group protocol first; KIP-848 (`group.protocol=consumer`) is Phase 5.**
   Classic (`JoinGroup`/`SyncGroup`/`Heartbeat`/`LeaveGroup` + client-side
   assignors) is the 4.3.0 default and is what every broker in our test matrix
   speaks without extra features. KIP-848 moves assignment server-side and
   collapses the three RPCs into one `ConsumerGroupHeartbeat`, but Java implements
   it on a different, event-loop consumer (`AsyncKafkaConsumer`) — a larger
   architecture. We ship classic end-to-end first, then add the new protocol as a
   parallel coordinator behind the same facade.

2. **Bytes-first core, typed access via a deserializer trait.** Mirror the
   producer: the on-the-wire record is `key/value: Option<Bytes>`
   (`producer/record.rs:256`). Typed consumption rides a `ConsumerDeserializer<T>`
   trait that mirrors `ProducerSerializer<T>` (`producer/serializer.rs:12`) with
   `Bytes`/`Vec<u8>`/`String` impls. `poll()` returns byte records; a thin typed
   wrapper deserializes on demand. This keeps the hot path allocation-light and
   avoids baking generics through the whole fetch pipeline.

3. **`poll(Duration)` is an async method on a single-owner `Consumer`.** The Java
   consumer is explicitly not thread-safe and runs the user's poll loop on one
   thread; we keep that contract (`Consumer` is `Send` but effectively single
   owner, not `Sync`). All fetch/coordination I/O is driven from `poll()` plus one
   background heartbeat task (classic needs liveness independent of poll cadence,
   exactly like Java's `AbstractCoordinator.HeartbeatThread`).

4. **Reuse everything shared.** Wire client, metadata cache, coordinator-lookup
   pattern, backoff, config, and `common::{TopicPartition, OffsetAndMetadata,
   ConsumerGroupMetadata, Node}` are all already present and are reused verbatim —
   no duplication of request framing/correlation/retry (same discipline confirmed
   for producer vs admin).

---

## 2. What already exists (no new work)

- **Config**: `ConsumerConfig` is fully declared in `config/clients.rs:1671` with
  all 40+ keys (`group.id`, `group.protocol`, `auto.offset.reset`,
  `enable.auto.commit`, `auto.commit.interval.ms`, `fetch.min/max.bytes`,
  `fetch.max.wait.ms`, `max.partition.fetch.bytes`, `max.poll.records`,
  `max.poll.interval.ms`, `session.timeout.ms`, `heartbeat.interval.ms`,
  `isolation.level`, `allow.auto.create.topics`, `client.rack`, …). The accessor
  `ClientConfig::consumer_config()` exists (`config/client.rs:88`). We add a
  `ConsumerRuntimeConfig` that validates + snapshots these (mirror
  `ProducerRuntimeConfig`).

- **Wire primitives** (`wire/client.rs`): `send_to_broker` (:134),
  `send_to_broker_without_response` (:167), `enqueue_to_broker` (:150, currently
  producer-gated — ungate for `any(producer, consumer)`), `metadata_for_topics`
  (:184), `negotiated_version` (:102), `upsert_broker`, `invalidate_topic_partition`,
  buffer pool. Version negotiation and SASL/TLS are automatic on first connect.

- **Coordinator discovery**: the producer's `FindCoordinator` flow
  (`producer/dispatcher.rs:3431`) is the exact template — `any_broker_id` →
  `FindCoordinator(key=group.id, key_type=0)` → `upsert_broker` → coordinator id.
  Extract a shared helper if convenient, otherwise copy the ~30-line pattern.

- **RPCs already registered** (`wire/message.rs`): `OffsetFetch` (:297),
  `OffsetCommit` (:298), `ListOffsets` (:302), `LeaveGroup` (:325),
  `ConsumerGroupDescribe` (:326).

- **Backoff/retry**: `wire::{BackoffPolicy, BackoffState}` (`wire/backoff.rs`).

- **Generated schemas exist** for every RPC we still need: `fetch_*`,
  `join_group_*`, `sync_group_*`, `heartbeat_*`, `consumer_group_heartbeat_*`,
  `offset_for_leader_epoch_*` (verified under `kacrab-protocol/src/generated/`).

## 3. Wire changes (Phase 0, tiny)

Register the missing consumer RPCs by adding one `impl_passthrough_message!` block
in `wire/message.rs` and their imports:

```
FetchRequestData                  => FetchResponseData
JoinGroupRequestData              => JoinGroupResponseData
SyncGroupRequestData              => SyncGroupResponseData
HeartbeatRequestData              => HeartbeatResponseData
OffsetForLeaderEpochRequestData   => OffsetForLeaderEpochResponseData
ConsumerGroupHeartbeatRequestData => ConsumerGroupHeartbeatResponseData   # Phase 5
```

Registration is always-compiled (not feature-gated), matching how the admin RPCs
were added. Watch the strict-codec **topic-name-vs-topic-id at max version** trap
that bit the admin offset paths: `Fetch` (topic id in v13+), `OffsetFetch`/
`OffsetCommit` v10 are id-keyed — resolve topic ids from metadata and send the
name only when the id is unknown (reuse the admin `resolve_topic_ids` approach).

---

## 4. Module layout

File+dir style, no `mod.rs`, mirroring `producer/` and `admin/`:

```
kacrab/src/consumer.rs            # feature gate, mod decls, pub use re-exports
kacrab/src/consumer/
  config.rs        # ConsumerRuntimeConfig (validate + snapshot ConsumerConfig)
  error.rs         # ConsumerError + Result<T> (mirror producer/error.rs)
  client.rs        # Consumer facade: new/from_*; subscribe/assign/poll/commit/seek/...
  subscription.rs  # SubscriptionState + TopicPartitionState (the heart of the client)
  fetch.rs         # Fetcher: build Fetch reqs, fetch sessions, response parse
  fetch_buffer.rs  # completed-fetch queue + record iteration + max.poll.records
  offsets.rs       # OffsetManager: reset (auto.offset.reset), list/beginning/end, commit/committed
  coordinator.rs   # classic group coordinator: find/join/sync/heartbeat/leave, auto-commit, assignors
  assignor.rs      # RangeAssignor, RoundRobinAssignor, CooperativeStickyAssignor (+ trait)
  record.rs        # ConsumerRecord, ConsumerRecords, RecordHeader (bytes core)
  deserializer.rs  # ConsumerDeserializer<T> trait + Bytes/Vec<u8>/String impls
  rebalance.rs     # ConsumerRebalanceListener trait + assignment/revocation plumbing
  metrics.rs       # ConsumerMetricsSnapshot (Phase 4)
  membership.rs    # KIP-848 membership manager + ConsumerGroupHeartbeat (Phase 5)
```

`Cargo.toml`: add a `consumer` feature (like `admin`). `common` is already
non-gated. Ungate `enqueue_to_broker` and any producer-only wire helpers the
consumer needs to `any(feature = "producer", feature = "consumer")`.

---

## 5. Public API (facade)

Constructors mirror producer/admin exactly:

```rust
impl Consumer {
    pub async fn new(config: ClientConfig) -> Result<Self>;
    pub async fn from_client_config(config: &ClientConfig) -> Result<Self>;
    pub async fn from_properties(properties: Properties) -> Result<Self>;
    pub async fn from_map<I, K, V>(entries: I) -> Result<Self>;
}
```

Core methods (async where they do I/O; snake_case Java mirrors; plain `Result<T>`):

| kacrab | Java | Phase |
|--------|------|-------|
| `subscribe(topics)` / `subscribe_with_listener(topics, l)` | `subscribe` | 2 |
| `subscribe_pattern(regex, …)` | `subscribe(Pattern)` | 2 (basic), refine later |
| `assign(partitions)` | `assign` | 1 |
| `unsubscribe()` | `unsubscribe` | 2 |
| `subscription()` / `assignment()` | same | 1–2 |
| `poll(timeout) -> ConsumerRecords` | `poll(Duration)` | 1 |
| `commit_sync()` / `commit_sync_offsets(map)` | `commitSync` | 2 |
| `commit_async(cb)` / `commit_async_offsets(map, cb)` | `commitAsync` | 2 |
| `committed(partitions)` | `committed` | 2 |
| `seek(tp, offset)` / `seek_with_metadata(tp, oam)` | `seek` | 1 |
| `seek_to_beginning(tps)` / `seek_to_end(tps)` | same | 1 |
| `position(tp)` | `position` | 1 |
| `pause(tps)` / `resume(tps)` / `paused()` | same | 1 |
| `beginning_offsets(tps)` / `end_offsets(tps)` | same | 3 |
| `offsets_for_times(map)` | `offsetsForTimes` | 3 |
| `partitions_for(topic)` / `list_topics()` | same | 1 (via metadata) |
| `current_lag(tp)` | `currentLag` | 3 |
| `group_metadata()` | `groupMetadata` | 2 |
| `enforce_rebalance(reason)` | `enforceRebalance` | 2 |
| `wakeup()` | `wakeup` | 1 |
| `close()` / `close_with(opts)` | `close` | 2 |
| `metrics()` | `metrics` | 4 |
| `client_instance_id(timeout)` | `clientInstanceId` | 5 (reuse admin path) |

`ConsumerRecords` is a per-partition-grouped, iterable batch of `ConsumerRecord`
(topic, partition, offset, timestamp + type, key/value `Option<Bytes>`, headers,
leader epoch). Typed access: `record.deserialized::<String, String>(&kd, &vd)` or
a typed `TypedConsumer<K, V>` wrapper — decide during Phase 4; bytes API ships
first.

---

## 6. Internal components

### 6.1 SubscriptionState (`subscription.rs`) — the core
Faithful port of Java `SubscriptionState`. Owns:
- `SubscriptionType`: `None | AutoTopics | AutoPattern | UserAssigned`.
- subscribed topic set / compiled pattern; the assigned partition set.
- per-partition `TopicPartitionState`: fetch position (offset + leader epoch),
  last consumed position, the `SeekUnvalidated/AwaitingReset/Fetching` state,
  paused flag, and the per-partition reset strategy.
- `default_reset_strategy` from `auto.offset.reset`.
- the rebalance listener handle.

Everything else reads/writes positions here. This is single-owner state guarded by
the poll thread; the background heartbeat task only needs a snapshot of the
assignment + group state, so this lives behind an `Arc<Mutex<…>>` split from the
group-membership state to avoid poll/heartbeat contention.

### 6.2 Fetcher + FetchBuffer (`fetch.rs`, `fetch_buffer.rs`)
- **Build**: group fetchable partitions (assigned, not paused, position
  validated) by leader from metadata; one `Fetch` per leader. Honour
  `fetch.min.bytes`, `fetch.max.bytes`, `fetch.max.wait.ms`,
  `max.partition.fetch.bytes`, `isolation.level`, `client.rack`.
- **Fetch sessions (KIP-227)**: a per-broker `FetchSessionHandler` sends a full
  fetch first, then incremental (only changed partitions/offsets), tracking
  session id/epoch. Start with a correct **full-fetch-every-time** fallback in
  Phase 1, add incremental sessions in Phase 3 (it's an optimization, not a
  correctness requirement).
- **Response**: decompress (reuse the record-batch/compression code the producer
  already exercises), optional CRC check (`check.crcs`), buffer per-partition
  `CompletedFetch` lazily (parse on iteration).
- **FetchBuffer**: queue of completed fetches; `collect(max_poll_records)` drains
  across partitions, deserializes into `ConsumerRecord`, and advances the consumed
  position. Prefetch: keep at most one in-flight fetch per partition; issue the
  next fetch as soon as a partition's buffer is drained (Java's pipelining).
- **pause/seek interaction**: paused partitions are excluded from new fetches;
  buffered data for a partition is discarded on `seek` (position moved) but kept
  on `pause`.

### 6.3 OffsetManager (`offsets.rs`)
- **Reset** (`auto.offset.reset`): partitions with no valid position issue
  `ListOffsets` (-2 earliest / -1 latest / by-timestamp for `by_duration`); `none`
  surfaces a `NoOffsetForPartition` error.
- **Public offset queries**: `beginning_offsets`/`end_offsets`/`offsets_for_times`
  = `ListOffsets` fan-out grouped by leader (the admin `list_offsets` code is a
  near-identical template).
- **Commit/committed**: `OffsetCommit`/`OffsetFetch` carrying leader epoch in
  `OffsetAndMetadata`. Topic-id vs name at v10 as noted above.
- **Position validation / log-truncation** (`OffsetForLeaderEpoch`, KIP-290):
  Phase 3; Phase 1 skips epoch validation (functionally fine on a stable leader).

### 6.4 Coordinator — classic (`coordinator.rs`, `assignor.rs`)
- `FindCoordinator` (producer template) → group coordinator node.
- Member state machine `Unjoined → PreparingRebalance → CompletingRebalance →
  Stable` driven by `JoinGroup` (leader election + collect subscriptions) →
  leader runs the assignor → `SyncGroup` (distribute assignments) → steady-state
  `Heartbeat`.
- **Assignors** (`assignor.rs`): `RangeAssignor` + `CooperativeStickyAssignor`
  (the 4.3.0 default pair) first; `RoundRobin`/`Sticky` after. A
  `PartitionAssignor` trait matches Java's `ConsumerPartitionAssignor` so custom
  assignors are possible later. Assignment metadata uses the same
  `ConsumerProtocol` subscription/assignment blobs the admin describe path already
  decodes (`decode_member_assignment` in `admin/client.rs`) — reuse the codec.
- **Auto-commit**: a timer (`auto.commit.interval.ms`) commits assigned positions;
  also commit on rebalance (before revoke) and on close when enabled.
- **Rebalance listener**: `on_partitions_revoked`/`assigned`/`lost` invoked around
  SyncGroup, honouring cooperative (incremental) vs eager semantics.
- **Background heartbeat task**: one `tokio::spawn`ed loop per consumer sends
  `Heartbeat` every `heartbeat.interval.ms`, watches `session.timeout.ms` and
  `max.poll.interval.ms` (poll liveness signalled from `poll()`), and requests a
  rejoin on `REBALANCE_IN_PROGRESS`. Shares group state via `Arc<Mutex<…>>`.

### 6.5 Concurrency model
```
 caller thread ──> Consumer::poll(timeout)
        │   1. update_assignment_metadata_if_needed()
        │        - coordinator.ensure_coordinator() / ensure_active_group()  (join/sync if needed)
        │        - offsets.reset_positions_if_needed() / validate_positions_if_needed()
        │   2. fetcher.send_fetches()          (issue Fetch to each leader)
        │   3. fetcher.collect(max_poll_records) -> ConsumerRecords   (drain buffer)
        │   4. maybe auto-commit
        ▼
 background heartbeat task (tokio::spawn):
        loop { sleep(heartbeat.interval); coordinator.heartbeat(); check timeouts }
```
Only two execution contexts (poll thread + heartbeat task), matching Java's
classic consumer (user thread + `HeartbeatThread`). `wakeup()` trips an atomic
flag + notifies, causing an in-flight `poll()` to return `WakeupError`.

---

## 7. Offset & commit semantics (faithful points)
- `OffsetAndMetadata` carries `leader_epoch` (already in `common`).
- Auto-commit interval starts after first successful join; commit failures that
  are retriable are retried next interval, fatal ones fence the member.
- Commit on rebalance (before partitions are revoked) and on close when
  `enable.auto.commit=true`.
- No committed offset → apply `auto.offset.reset`.

---

## 8. Phased plan (each phase: code → lib tests → real-broker verify → commit)

- **Phase 0 — wire wiring.** Register Fetch/JoinGroup/SyncGroup/Heartbeat/
  OffsetForLeaderEpoch pass-throughs; ungate `enqueue_to_broker` for consumer;
  add `consumer` feature + empty module skeleton. *(tiny)*

- **Phase 1 — assign + fetch (no group).** SubscriptionState, Fetcher (full
  fetch), FetchBuffer/collect, OffsetManager reset + ListOffsets, ConsumerRecord,
  and `assign/poll/seek/position/pause/resume/assignment/partitions_for/wakeup`.
  Verify: manual-assignment consume of records a kacrab producer wrote, against a
  real 4.3.0 broker. **This is the milestone that proves the design.**

- **Phase 2 — classic group coordination.** FindCoordinator + Join/Sync/Heartbeat/
  Leave, Range + CooperativeSticky assignors, rebalance listener, auto-commit,
  `subscribe/commit_sync/commit_async/committed/group_metadata/enforce_rebalance/
  close`. Verify: two consumers in a group rebalance a topic; offsets commit and
  resume across restart.

- **Phase 3 — correctness/perf hardening.** Incremental fetch sessions (KIP-227),
  `OffsetForLeaderEpoch` position validation (KIP-290), `beginning/end_offsets`,
  `offsets_for_times`, `current_lag`, pattern subscription refresh, RoundRobin/
  Sticky assignors.

- **Phase 4 — ergonomics.** Typed deserializer wrapper, `ConsumerInterceptor`
  hooks, `ConsumerMetricsSnapshot` (Kafka-named), README/book/example docs.

- **Phase 5 — KIP-848 (`group.protocol=consumer`).** `membership.rs` +
  `ConsumerGroupHeartbeat` server-side-assignment path behind the same facade,
  selected by the `group.protocol` config; `client_instance_id` (reuse the admin
  telemetry-negotiation path).

---

## 9. Open questions / risks
- **Incremental fetch sessions**: correctness is fine with full fetches; sessions
  are the throughput lever. Sequence after Phase 1 proves the pipeline.
- **Cooperative vs eager rebalancing**: the default assignor pair is cooperative;
  the listener contract differs (incremental revoke). Implement cooperative
  faithfully in Phase 2 rather than retrofitting.
- **Deserializer generics vs bytes**: ship bytes-first; the typed layer must not
  force generics through Fetcher/Buffer (keep `T` at the edge only).
- **Decompression reuse**: confirm the record-batch reader used by tests can be
  driven in the read direction for all four codecs (gzip/snappy/lz4/zstd) — it
  should, since round-trip compression tests exist.
- **`max.poll.interval.ms` enforcement**: needs poll-liveness signalling into the
  heartbeat task; get the plumbing right in Phase 2, not bolted on later.
