# Changelog

All notable changes to this project should be documented in this file.

This project is pre-1.0; minor releases may still change public APIs.

The format is based on human-readable release notes. Each entry includes the
release date and links to relevant pull requests or issues.

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
