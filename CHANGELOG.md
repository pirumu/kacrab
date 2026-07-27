# Changelog

All notable changes to this project should be documented in this file.

This project is pre-1.0; minor releases may still change public APIs.

The format is based on human-readable release notes. Each entry includes the
release date and links to relevant pull requests or issues.

## Unreleased

### Fixed

- **A hostile record header count could OOM the client.** `Record::decode` sized
  its header `Vec` straight from the wire's `headerCount` varint, checking only
  that it was non-negative. A 90-byte record declaring ~486M headers reached
  `malloc(7.8 GB)` before the decode loop read a single header and failed, so a
  corrupt or malicious broker response could OOM-kill any kacrab client with a
  handful of bytes — the batch level had guarded its own count with
  `MAX_RECORDS_PER_BATCH` for exactly this reason, but the per-record path had
  no equivalent. The speculative allocation is now clamped by what the remaining
  buffer can hold (a header is at least two varints), which cannot reject a
  satisfiable count. Found by the new `record_batch_framed` fuzz target; covered
  by `absurd_header_count_fails_without_a_giant_allocation`.
- `Consumer::poll` is now cancel-safe with respect to records. `reap_fetch` moved
  the in-flight `Fetch` handle out of the consumer before awaiting it, so
  dropping a `poll` future mid-await — the ordinary fate of the losing arm of a
  `tokio::select!` — detached the task and discarded whatever it had fetched,
  along with the partition positions it carried and the KIP-227 incremental
  fetch sessions, forcing the next fetch to re-open full sessions. The handle is
  now joined through `&mut` and stays owned by the consumer, so a cancelled poll
  costs nothing and the next poll folds the fetch in. Covered by
  `reap_fetch_survives_a_cancelled_await`.

### Added

- `cargo-fuzz` targets for the decoders that parse untrusted broker bytes:
  `record_batch_decode`, `record_batch_framed`, `response_decode`, and
  `decompress`. They run nightly in CI at 15 minutes per target and as a
  60-second smoke on any PR touching `kacrab-protocol/`. The fuzz crate lives
  outside the workspace because cargo-fuzz needs nightly plus a sanitizer.
  `record_batch_framed` exists because raw-byte fuzzing of a record batch is
  nearly useless on its own: CRC32C is validated before the magic byte, the
  record count, the varints, or the compressed blob, so random input passes that
  gate with probability 2^-32 and never reaches the decoder. Building correct
  framing around fuzzer-controlled bytes took coverage from 150 to 774 edges and
  is what surfaced the OOM above.

### Documentation

- New README sections: a verified comparison against `rust-rdkafka`, `rskafka`,
  and `kafka-rust`; "Cancellation & drop semantics" documenting the cancel-safety
  of every public future and what dropping a client without `close()` does; and
  "When not to use kacrab".
- Corrected the Highlights latency claim from "lower at every percentile" to
  "lower or tied", matching the matched-load table and the Caveats section, and
  documented why the latency *average* differs while p50/p95/p99 tie (both sides
  quantize to integer milliseconds; the average is the fraction above 0 ms).
- Replaced the coverage footnote's "(streams)" with the real breakdown of the 26
  unwired generated APIs, and synced the design book to `0.3.0` and the current
  benchmark figures.

## 0.3.0 — 2026-07-27

Producer batch-split release. A topic whose `max.message.bytes` sits below the
producer's `batch.size` could not deliver anything at all; fixing that exposed
four further defects in the split and idempotent-sequence paths that the default
scenarios never reach. The split path now delivers every record with no retries
and no errors, and is **2.2x Java's throughput** on the same workload. Includes
one breaking metrics API change (`ProducerMetricsSnapshot` is `#[non_exhaustive]`),
so this is a minor version bump under the pre-1.0 convention.
([#52](https://github.com/pirumu/kacrab/pull/52))

### Added

- `ProducerMetricsSnapshot::record_batch_split_count` — record-batch splits
  forced by a broker `MESSAGE_TOO_LARGE` response, counted one per split event
  the way Java's `Sender.completeBatch` does. The producer parity bench prints
  it as the `batch_splits` column, so that column is now a real both-sides
  comparison against `kafka-producer-perf-test.sh --print-metrics`.
- `ProducerMetricsSnapshot::delta_since` — the difference between two snapshots,
  the companion to the `#[non_exhaustive]` change below. Monotonic counters (and
  the `*_total_latency` durations) are subtracted with saturation; gauges
  (`queue_depth_*`, `buffer_available_bytes`, `waiting_threads`,
  `incomplete_batches`, `in_flight_dispatches`, and the `average_*` ratios) are
  point-in-time readings and keep the current value. Downstream crates that used
  to compute this with a struct expression should call it instead of assigning
  fields one by one, so a metric added later cannot be silently left at zero.

### Changed

- **Breaking:** `ProducerMetricsSnapshot` is now `#[non_exhaustive]`.
  Downstream crates can no longer build one with a struct expression
  (including functional-update `..base` syntax); start from the new
  `ProducerMetricsSnapshot::ZERO` associated constant — it is usable in const
  context — and assign the fields you need. Reading fields is unaffected.
- **Behaviour change:** `producer-metrics:batch-split-rate` and
  `producer-metrics:batch-split-total` now count `MESSAGE_TOO_LARGE`
  record-batch splits, matching Java's semantics. They were previously fed by
  the `max.request.size` Produce-request grouping split, a kacrab-specific
  event with no Java equivalent. That grouping count is unchanged and remains
  available as `ProducerMetricsSnapshot::produce_request_split_count`, now
  without a Java-named meter. Workloads that only hit request-grouping splits
  will see the Java-named `batch-split-*` metrics drop toward zero.
- **Producer dispatch:** once a broker is receiving a produce request, the head
  batch of every partition that broker leads now rides along on the same
  request, instead of each partition waiting out its own `linger.ms` or filling
  to `batch.size` first. This is Kafka's `RecordAccumulator.drainBatchesForOneNode`
  behaviour, which takes each partition's head batch with no readiness check once
  the node is already being sent to. Swept batches pass through the same
  in-flight reservation and idempotent-sequence bookkeeping as normally drained
  ones, and the sweep only fires when partition leadership is already in the
  metadata cache, so it never adds a metadata fetch ahead of a ready batch.

  Measured against a native single-node Kafka 4.3.0 at 5M x 10 B over 16
  partitions, this raised throughput and cut latency at the same time:
  43.9 -> 47.6 MB/s, 1.65 -> 0.61 ms average latency, 10 -> 6 ms p99, with the
  produce-request count roughly halving as more partitions coalesce into each
  request.

### Fixed

- **Nothing was delivered to a topic whose `max.message.bytes` is below the
  producer's `batch.size`** — every oversized batch failed with `DeliveryTimeout`.
  A batch rejected with `MESSAGE_TOO_LARGE` was re-split against a constant
  `batch.size` target, but the accumulator already caps every batch at
  `batch.size`, so the split regrouped the same records into a child of the same
  size for the broker to reject again, forever. Re-splits now halve the target the
  way Kafka's `RecordAccumulator.splitAndReenqueue` does — a batch that is itself
  a split child targets `max(largest record, estimated batch size / 2)` — so the
  pieces shrink geometrically until they fit. The `max(largest record, ..)` floor
  keeps a batch holding one large record from halving forever, and a single-record
  batch that still does not fit stays a terminal failure, as in Java.
- **A split that produced more than one child dropped every record outside the
  first child.** The whole batch's delivery state went to child 0 and the other
  children were left with none, so their records were reported as
  `DeliveryDropped` and never received a receipt. Each child now completes its own
  slice of the shared delivery state. Previously only reachable when the
  compression-ratio estimate shrank the split target; the halving above makes
  multi-child splits the normal case.
- **A batch that had to be split more than once could not be requeued at all.**
  The second split minted child identities by position, colliding with the first
  split's siblings, and the accumulator's requeue guard rejected the batch with
  `BatchLifecycle`, stalling the partition. Split children now carry a unique
  identity and an explicit link to the batch they were split from, so a split
  chain of any depth stays consistent with the buffered and incomplete batch
  bookkeeping.
- **Records could be delivered out of sequence, or a flush fail with
  `FlushIncomplete`, on any partition where a batch was requeued.** The idempotent
  in-flight set was released when a batch was requeued, but Kafka's
  `inflightBatchesBySequence` tracks every batch that holds a sequence and has not
  terminally completed, *including one waiting in the accumulator to be retried*.
  The drain gate (`shouldStopDrainBatchesForPartition`) compares a retried batch
  against that set, so releasing early made the batch measure itself against
  other, higher sequences and defer indefinitely while fresh batches kept going
  out — the broker saw the partition's sequence run backwards. The registration
  now survives a requeue; a split hands it from parent to children the way
  `RecordAccumulator.splitAndReenqueue` does; and the two paths that abandon
  batches instead of re-dispatching them (a flush with no accumulator to hand them
  back to, an abort that discards buffered work) release it explicitly. Most
  visible on the split path above, which produced 309 `OutOfOrderSequenceNumber`
  responses per run.
- **A partition could stop making progress after a split even though its batches
  were still buffered**, surfacing as `FlushIncomplete` with records left unsent.
  Only one produce request is started per partition per selection, and the drain
  gate admits only the batch holding the partition's first in-flight sequence; the
  selection picked whichever batch the drain returned first, which after a split
  re-enqueues several children at once could be a higher-sequence sibling. The
  gate then deferred that pick every cycle while the batch that would unblock the
  partition was never selected. Selection now takes the lowest base sequence,
  matching Kafka's deque head.
- **The producer parity benchmark dropped buffer-wait time from exactly the
  records that waited longest.** It took its per-record latency timestamp inside
  the send loop, and a `Backpressure` result did not advance the record counter,
  so the retry captured a fresh timestamp. Java has no such gap:
  `ProducerPerformance` captures `sendStartMs` once and `KafkaProducer.send`
  blocks inside that window when the accumulator is full. Benchmark-only; no
  client behaviour changed.
- **Published producer byte-rate figures compared two different lines.** kacrab's
  own `MiB/s` scenario line was quoted against Java's `MB/sec` line. Both tools
  compute `bytes / elapsed / (1024 * 1024)` on their `records sent, ... MB/sec`
  line, so that is the comparable one; read from it, the 10 KiB throughput lead is
  +15% rather than the +25-30% previously published. The 10 B lead is +35%.

### Performance

- **The producer no longer pays a broker round trip for a split that splits
  nothing.** The first split of an accumulator batch targets `batch.size` — the
  size the accumulator already packed the batch to — so regrouping against it
  hands back a single child holding every record the parent held, which the broker
  rejects for the same reason. Java discovers this by sending the identical child
  and waiting; the grouping is local, so kacrab checks it instead and halves on
  the spot until the batch really divides. The floor terminates the loop: at
  `max(largest record, 1)` a second record can never share a group with the first,
  so a batch of two or more records always yields two or more groups.

Measured against a real broker — 20,000 x 4 KiB into a one-partition topic with
`max.message.bytes=65536`, producer at `batch.size=262144`, medians of 5
interleaved kacrab/Java pairs with the topic recreated before every pass:

| | kacrab | Java |
| --- | ---: | ---: |
| throughput | **59,347 rec/s (232 MB/s)** | 26,881 rec/s (105 MB/s) |
| average latency | **109 ms** | 194 ms |
| produce requests | **3,173** | 3,492 |
| batch splits | **952** | 1,270 |
| retries / errors | 0 / 0 | 0 / 0 |

Before this release the same workload delivered nothing. kacrab led in all 5
pairs; its slowest round (39,062 rec/s) still beat Java's fastest (30,816 rec/s).
The default scenarios are unaffected — they never reach the split path — and were
re-measured to confirm it: 45.4 MB/s at 5M x 10 B and 524 MB/s at 100K x 10 KiB,
against Java's 34.8 and 435.

## 0.2.0 — 2026-07-07

Outage-resilience release. The producer and consumer now recover from
prolonged and total broker outages instead of wedging permanently. Includes
one breaking consumer API change (subscription-mode exclusivity), so this is a
minor version bump under the pre-1.0 convention.

### Added

- `Consumer::close_timeout(Duration)` — close with a caller-chosen bound on
  the final commit-and-leave work, the analogue of Java's `close(Duration)`.
  `close()` keeps its `request.timeout.ms` bound.
  ([#45](https://github.com/pirumu/kacrab/pull/45))
- Soak test harness (`benches/src/bin/soak_bench.rs`): sustained load with
  broker-kill and consumer-bounce chaos and a per-partition continuity verdict,
  for measuring resilience under fault injection.
  ([#46](https://github.com/pirumu/kacrab/pull/46))

### Changed

- **Breaking:** `Consumer::assign` now returns `Result`. Subscription modes are
  mutually exclusive, matching Java's `SubscriptionState`: mixing a manual
  `assign` with `subscribe` / `subscribe_pattern` (in either order, or switching
  between topic and pattern subscriptions) returns `ConsumerError::InvalidState`
  instead of silently replacing the previous mode. Call `unsubscribe` to switch
  modes. An empty `assign` is treated as `unsubscribe` (Java parity).
  ([#45](https://github.com/pirumu/kacrab/pull/45))

### Fixed

- The producer no longer wedges permanently on a total-cluster outage longer
  than `delivery.timeout.ms`. The background sender loop treated a transient
  error from its drive pass (a metadata/wire `Timeout` while every broker was
  down) as fatal and parked; once the producer's appends dried up nothing woke
  it again, even after the cluster recovered with ready batches still buffered.
  It now retries on the retry backoff instead of parking, mirroring Kafka
  `Sender.runOnce`. ([#48](https://github.com/pirumu/kacrab/pull/48))
- The producer recovers from prolonged broker outages instead of wedging:
  requeued batches retry on a timer rather than waiting for new traffic, and the
  background sender pump no longer wedges on a single expired batch.
  ([#47](https://github.com/pirumu/kacrab/pull/47))
- The consumer recovers from a coordinator-broker outage instead of
  livelocking: it clears a stale cached coordinator on `Wire(Timeout)` /
  `ConnectionClosed` / `Io`, and JoinGroup/SyncGroup are bounded by the
  rebalance timeout. ([#47](https://github.com/pirumu/kacrab/pull/47))
- Wire: a fenced-broker handshake is bounded by `request.timeout.ms` (a
  restarted-but-fenced broker that accepts TCP but answers nothing no longer
  parks the broker task forever), and the broker reader task is aborted on drop
  and on consumer close so sockets do not linger `ESTABLISHED`.
  ([#47](https://github.com/pirumu/kacrab/pull/47))
- `init_transactions` retries a still-loading transaction coordinator
  (`COORDINATOR_LOAD_IN_PROGRESS` on a freshly-started broker) for the full
  `max.block.ms`, matching Java's blocking `initTransactions`, instead of
  giving up after the produce `retries` count. ([#51](https://github.com/pirumu/kacrab/pull/51))

## 0.1.2 — 2026-07-07

Wire-pipeline correctness fix
([#43](https://github.com/pirumu/kacrab/pull/43)).

### Fixed

- A stray response frame — one whose correlation id parsed but matched no
  in-flight request, typically a late arrival for a request already failed
  by its timeout — no longer fails an unrelated request. It previously
  completed the oldest in-flight slot with `CorrelationIdMismatch`, and the
  misfire cascaded: each subsequent in-order response found its own slot
  consumed and landed one slot off its target until the connection drained.
  Such frames are now dropped; frames too short to carry a correlation id
  still fail the oldest waiter so a garbled stream surfaces a decode error
  instead of waiting out the request timeout.

### Changed

- Request-pipeline slot lookup resolves with one modular add instead of
  walking the ring, making correlation scans and failure sweeps linear in
  the number of in-flight requests instead of quadratic. Only noticeable
  when `max.in.flight.requests.per.connection` is raised well above the
  default of 5.

## 0.1.1 — 2026-07-06

Hardening release: every finding from an external review of 0.1.0, fixed and
real-broker verified ([#39](https://github.com/pirumu/kacrab/pull/39)).

### Security

- Generated protocol decoders no longer trust wire-claimed array lengths for
  `Vec` preallocation. A hostile or corrupt response claiming `i32::MAX`
  elements previously reserved gigabytes up front and aborted the process
  under `panic = "abort"`; the preallocation is now clamped by the bytes
  actually remaining and a fixed budget (`array_read_capacity`), and a
  truncated hostile-length array fails decode cleanly.
- Decompression output is bounded. gzip and zstd decoded to `Vec` with no
  output cap, lz4 capped each 64 KiB block but not the frame total, and
  snappy trusted the raw format's claimed length (allocated up front) — a
  crafted batch could inflate a tiny payload until the allocator gave out.
  All four codecs now refuse to produce more than
  `compression::MAX_DECOMPRESSED_LEN` (1 GiB, ~10:1 over the 100 MiB wire
  frame cap) and surface the new
  `CompressionErrorKind::DecompressedTooLarge` instead of dying.

### Fixed

- A synchronous commit can no longer be overtaken by queued asynchronous
  commits: `commit_sync` / auto-commit / `close` drain the async-commit queue
  through an ordering barrier before committing, so a later sync commit
  cannot be overwritten by an earlier queued one and the committed offset
  never regresses (Java's `commitSync` semantics).
- Asynchronous commits heal across a coordinator move: the commit worker
  re-finds the coordinator once and retries on
  `NOT_COORDINATOR`/`COORDINATOR_NOT_AVAILABLE`/`COORDINATOR_LOAD_IN_PROGRESS`,
  matching the synchronous paths, instead of failing every subsequent
  `commit_async` until the consumer was rebuilt.
- `Consumer::close` applies queued asynchronous commits (firing their
  callbacks) before stopping the commit worker instead of silently dropping
  them.
- One unreachable leader no longer fails the whole `poll` and discards the
  data already fetched from the other leaders that round: the failed leader's
  partitions are flagged for a metadata refresh and retried next poll, per
  Java's per-node fetch handlers. Terminal TLS/SASL setup failures still
  surface.
- A short `poll` timeout is no longer overshot by the idle backoff — the
  empty-round wait is clamped to the remaining poll budget.

### Added

- Consumer `retry.backoff.ms` (default 100 ms) and `retry.backoff.max.ms`
  (default 1 s) as typed config. The idle-poll wait follows
  `retry.backoff.ms` (was a fixed 50 ms), and coordinator lookups retry under
  the exponential policy (base doubling to max, 20% jitter) matching Java
  `AbstractCoordinator`'s `ExponentialBackoff` (was a fixed 500 ms).
- `kacrab-protocol`: per-codec `decompress_bounded` and
  `MAX_DECOMPRESSED_LEN` for callers that want an explicit decompression
  budget; `primitives::array_read_capacity`.
- Real-broker regression tests for the commit-ordering barrier and for
  consumer-side decompression of broker-compressed batches across all four
  codecs (the CLI helpers honor `KACRAB_KAFKA_BIN` for hosts where
  `127.0.0.1:9092` is a native broker rather than the compose container).

## 0.1.0 — 2026-07-02

First crates.io release: `kacrab`, `kacrab-protocol`, and `kacrab-macros`
([#36](https://github.com/pirumu/kacrab/pull/36)).

### Added

- Consumer topic-id-keyed `Fetch` (KIP-516): fetches now negotiate up to the
  broker's `Fetch` version (v18 on Kafka 4.3) instead of capping at the
  name-keyed v12. Topic ids are resolved from the routing metadata, responses
  map ids back to names via the request's id set (Java's `sessionTopicNames`),
  fetch sessions carry their ids into the forgotten list, and a topic without
  an id — or a pre-v13 broker — downgrades that fetch to v12 exactly like
  Java's `AbstractFetch`. `UNKNOWN_TOPIC_ID`/`INCONSISTENT_TOPIC_ID` are
  handled as retriable per-partition metadata refreshes, and a session whose
  topic ids changed (recreated topic) or whose keying mode flipped re-opens
  with a full fetch. Verified against a real Apache Kafka 4.3.0 broker
  (negotiates v18) across the full consumer suite, throughput-neutral on the
  consumer benchmark.
- Consumer cross-poll fetch buffering (Java's `CompletedFetches`): raw fetch
  responses are buffered client-side, `poll` drains them `max.poll.records` at
  a time, and a partition is only re-fetched once its buffer runs dry.
  Buffered data is invalidated lazily on seek/reset/revoke and retained across
  pause. Previously each poll re-fetched — and the broker re-served — the
  response surplus past `max.poll.records`, which capped small-record
  consumption at ~132K records/sec (~13 Fetch RPCs per 5M-record run now,
  down from 10,000).
- Consumer background prefetch (Java's network thread): the next `Fetch` runs
  as a spawned task while `poll` serves buffered records; an empty-buffer poll
  awaits it only up to its own timeout. Fetches skip nodes still hosting
  buffered partitions (Java's buffered-node gate), which both protects the
  broker's fetch-session cache and avoids a caught-up-partitions-only request
  long-polling `fetch.max.wait.ms` mid-pipeline.
- Consumer lazy per-batch record decode (`decode_next_batch` in
  `kacrab-protocol`): buffered blobs decode one record batch at a time as
  drained, holding raw blobs plus ~one batch of records in memory instead of
  materializing whole responses (which cost ~536 MiB of allocator churn on a
  5M-record run; now ~18 MiB peak RSS).
- With all three, the consumer head-to-head at identical defaults now reads:
  10 B records ~17.6M vs Java ~9.3M records/sec (~1.9x), 10 KiB ~540K vs
  ~136K records/sec (~4x, ~5.3 GB/s), at ~16-20x less peak memory, ~9-17x less
  CPU, ~15x faster group joins, and a poll() max 14-25x lower; per-poll
  latency percentiles are printed by both the Rust bench and a compiled Java
  probe in the baseline wrapper.
- Real-Kafka consumer benchmark (`consumer_kafka_bench`) mirroring Java's
  `kafka-consumer-perf-test.sh` (same tool props, poll loop, timeout semantics,
  and final CSV columns), with a `KACRAB_BENCH_PREFILL=1` topic prefill, a Java
  baseline wrapper (`benches/scripts/consumer_default_matrix.sh`), and
  `make bench-kafka-consumer` / `bench-kafka-consumer-java-default` targets.
  Head-to-head at identical defaults (2026-07-02, native Kafka 4.3.0): kacrab
  consumes 10 B records ~28% faster than Java (~11.8M vs ~9.25M records/sec)
  and 10 KiB records ~3x faster (~4.7-5.0 GB/s vs ~1.5 GB/s) at a fraction of
  the CPU, with ~10x-faster group joins; caveats (peak-RSS churn on tiny-record
  bursts) in `benches/README.md`.

- Consumer client (`consumer` feature): `kacrab::consumer::Consumer` with manual
  partition assignment and classic consumer-group subscription. Fetch with
  `auto.offset.reset`, `max.poll.records`, and `seek`/`seek_to_beginning`/
  `seek_to_end`/`position`/`pause`/`resume`/`wakeup`; `FindCoordinator` +
  `JoinGroup`/`SyncGroup`/`Heartbeat`/`LeaveGroup` with the `range` assignor and
  eager rebalancing; `commit_sync`/`commit_sync_offsets`/`committed`/
  `group_metadata` (leader-epoch aware). Bytes-first records
  (`ConsumerRecord.key/value: Option<Bytes>`). Verified end-to-end against a real
  Apache Kafka 4.3.0 broker (manual assign + commit, a single subscriber, and two
  consumers rebalancing a topic).
- Consumer group parity: the `roundrobin`, `sticky`, and incremental
  `cooperative-sticky` assignors (`partition.assignment.strategy`, default aligned
  to Java's `range,cooperative-sticky`); the KIP-848 server-side protocol
  (`group.protocol=consumer`, a single `ConsumerGroupHeartbeat` RPC with
  server-computed, topic-id-keyed assignments reconciled incrementally); a
  dedicated background heartbeat task; static membership (`group.instance.id`);
  and `enforce_rebalance`.
- Consumer offset and fetch parity: offset queries
  (`beginning_offsets`/`end_offsets`/`offsets_for_times`/`current_lag`),
  `commit_async` with background auto-commit, incremental fetch sessions
  (KIP-227), and OffsetForLeaderEpoch position validation / truncation detection
  (KIP-320).
- Consumer surface parity: topic pattern subscription (`subscribe_pattern`, regex,
  honouring `exclude.internal.topics`), typed `ConsumerDeserializer`s
  (bytes/byte-array/string), `ConsumerInterceptor`s (`on_consume`/`on_commit`),
  `client_instance_id`, and `metrics()`. All verified end-to-end across ten
  scenarios against a real Apache Kafka 4.3.0 broker (including cooperative-sticky,
  pattern, interceptors, and KIP-848).
- Config drift guard (`kacrab/tests/config_drift.rs`) cross-checking the typed
  `config/clients.rs` against the generated `config/catalog.rs`, so a Kafka
  version bump is regenerate-and-reconcile.
- `client.dns.lookup` is now honoured: broker hostnames are resolved on connect
  and every resolved address is tried under `use_all_dns_ips`.
- Consumer chapters in the book (overview, fetching, rebalancing).

### Changed

- `ConsumerRecord.topic` is now `Arc<str>` (was `String`), matching the
  producer's `RecordMetadata`: records in a poll share one topic handle
  instead of heap-allocating the name once per record (5M allocations per
  5M-record run). `record.topic.as_ref()` / deref coercion covers `&str`
  uses; construction sites need `Arc::from(...)`.

- Broker DNS resolution moved into the wire layer (IPv4-first, multi-address
  fallback), replacing per-client address selection in the producer and consumer
  coordinator lookups.
- The three per-client `to_connection_config` methods now share one
  `connection_config_fields!` macro (~115 fewer lines), so a wire connection
  config is added in one place.

### Fixed

- The config-metadata generator now extracts `ConfigDef.define(...)` calls that
  Kafka breaks across lines (`).\n define(`), so `bootstrap.controllers` is
  cataloged.
- A group coordinator advertised as `localhost` resolving to an unreachable IPv6
  loopback no longer hangs the connection (see the wire DNS change above).

### Security

- Nothing yet.
