# kacrab Direction

This document tracks current project direction. It is not a dated delivery
plan, and it intentionally avoids milestone labels that imply release
readiness.

The goal remains a high-performance, 100% pure Rust Kafka client with no
`librdkafka`, no C client bindings, and no unsafe code in the workspace.

## Current Baseline

- `kacrab-protocol` generates Kafka request/response structs from Apache Kafka
  4.3.0 schemas.
- Protocol primitives, generated message encode/decode, record batches,
  compression codecs, and Java oracle compatibility tests exist.
- `kacrab-codegen` regenerates protocol modules and Kafka config catalogs from
  pinned upstream Kafka sources.
- `config` exposes Java-style `ClientConfig`, typed producer/consumer/admin
  config builders, official Kafka config metadata, and validation.
- `wire` owns TCP/TLS/SASL broker sessions, ApiVersions negotiation, generated
  request encoding, response dispatch, metadata fetch, bounded in-flight
  requests, request timeouts, and connection cleanup.
- `producer` owns the public `KafkaProducer` API, batching by topic-partition,
  linger, bounded memory, compression hooks, default/keyed partitioning,
  metadata routing, multi-broker dispatch, idempotent producer state,
  transactions, retries, delivery timeout, and delivery handles.
- `consumer` owns the public `Consumer` API: manual assignment, topic and
  regex subscription, classic groups (eager + incremental cooperative-sticky)
  and the KIP-848 protocol, incremental fetch sessions, truncation detection,
  sync/async/auto commit, static membership, typed deserializers,
  interceptors, and metrics. `ShareConsumer` covers the KIP-932 queue-shaped
  surface behind the `share-consumer` feature.
- `admin` owns the `AdminClient` API — the full Apache Kafka 4.3.0 `Admin`
  operation surface (62 operations) with controller/coordinator/per-leader/
  broadcast routing, verified against a real broker.
- `benches` contains accumulator, wire-pipeline, producer-dispatcher, mock
  broker, and real Kafka benchmark hooks with local baselines, plus the
  soak/chaos harness (`soak_bench`).
- Verification gates: a byte-for-byte Java oracle matrix for the generated
  protocol, eleven `cargo-fuzz` targets over every untrusted-byte parser
  (nightly), and real-broker suites (producer, consumer, admin, auth,
  compression, 3-broker failover) that run in CI as a merge gate.

## Active Priorities

1. Close the one open correctness branch: the compound-failure re-run. The
   2026-07-07 soak ([`SOAK-REPORT.md`](SOAK-REPORT.md)) passed steady-state
   and single-fault chaos but failed the compound phase; the defects it
   exposed are fixed in code (F3 verified with a deterministic reproducer),
   so what remains is a fresh multi-hour compound soak (VM ≥ 6 GB) to
   confirm F1/F4.
2. Execute the remaining measurement work in the
   [Production Acceptance Plan](#production-acceptance-plan): B2 (high-RTT
   emulation), B3 (8–24 h memory soak — the natural extension of the
   compound re-run), and B4's CI gates once thresholds are agreed.
3. Keep public API ergonomics close to Kafka Java where that helps users, while
   preserving explicit Rust ownership and error handling.

Earlier priorities — wire-layer hardening (reconnect/backoff, metadata
invalidation on leadership errors, in-flight cleanup) and producer hardening
(retry semantics, delivery timeout accounting, idempotence, transactions) —
are delivered and verified against real brokers; the soak findings F1/F3/F4
drove the last round of fixes. The original 3M messages/sec target is
exceeded (currently ~5M rec/s on the 10 B benchmark).

## Non-Goals

- **Kafka Streams.** kacrab is a Kafka client library, not a stream-processing
  framework. A streams runtime (topology API, processor runtime, state stores,
  changelog topics, exactly-once processing) is a different product with a
  different lifecycle; if one is ever built, it should be a separate project
  that consumes kacrab's public producer/consumer/transaction APIs. Declaring
  this a non-goal keeps the crate's scope — and its review surface — bounded.
- **Wrapping `librdkafka` or any C client.** The native-protocol implementation
  is the point of the project.

## Not Ready Yet

- Release stability is not guaranteed.
- Local benchmark baselines are not production acceptance claims; the
  measurement work needed is scoped under
  [Production Acceptance Plan](#production-acceptance-plan).

## Production Acceptance Plan

The functional multi-broker work is done and verified against a real 3-broker
KRaft cluster: multi-broker dispatch routes to every broker's leaders, and a
broker loss re-routes affected partitions to their new leaders without wedging
co-batched ones (`kacrab/tests/real_kafka_cluster.rs`,
`docker-compose.cluster.yml`).

What remains is **measurement and acceptance under load** — plus one
correctness confirmation: the compound-failure fixes (F1/F4) await their
confirming re-run. Each item needs dedicated infrastructure, a time budget,
and in some cases an SLO threshold that is a product decision, not something
the implementation can assert for you.

> A few minutes of `docker compose` proves a path *works*; proving it *holds
> up* needs hours of load, a tuned network, and agreed pass/fail gates.

### B1 — Sustained multi-broker stress

**Goal:** confirm the multi-broker dispatch path stays correct and bounded
under high, continuous load across all brokers (no unbounded memory growth, no
stuck partitions, no reordering/duplication with idempotence on).

**Status:** run once — the overnight soak of 2026-07-07
([`SOAK-REPORT.md`](SOAK-REPORT.md)): 4 h 17 m healthy across ~25 rotating
broker-kill cycles at 1,000 rec/s; producer PASS (0.002% delivery errors, all
loud; flat RSS), consumer group PASS. The compound phase (a broker lost for
tens of minutes, then restored) FAILED and produced findings F1–F4: F2 was
verified not to be a client defect, F3 is fixed and verified with a
deterministic reproducer, F1/F4 are fixed in code (PR #47) pending
confirmation.

**Remaining:** a fresh multi-hour compound soak with the VM at ≥ 6 GB to
confirm F1/F4, then longer runs at higher target rates — the first run's
1,000 rec/s validates correctness under chaos, not throughput under sustained
pressure.

**Decisions needed:** target rate, record size, duration, fault cadence.

**Acceptance:** no lost/duplicated/reordered records over the full run;
steady-state memory and in-flight counts; no permanently stuck partition.

### B2 — Cross-DC / high-RTT coverage

**Goal:** verify behavior when broker links have real latency, jitter, and
loss (dispatch/retry/timeout tuning is currently only exercised at ~0 RTT).

**Status:** not started — no `netem`/`toxiproxy` tooling exists in the repo
yet. Co-located brokers have sub-millisecond RTT, so timeout/backoff/in-flight
interactions that only appear at 50–200 ms RTT are untested. This needs
network emulation, not just more brokers.

**Approach:** add `tc netem` (delay + jitter + small loss) on the broker
containers, or run brokers behind a latency-injecting proxy (`toxiproxy`).
Re-run B1's workload at representative RTTs (50 ms, 150 ms) and confirm
throughput degrades gracefully (pipelining via `max.in.flight`), timeouts are
not tripped spuriously, retries/backoff behave under jitter and loss, and a
high-RTT leader change recovers within `delivery.timeout.ms`.

**Decisions needed:** RTT/jitter/loss profiles; `toxiproxy` (portable,
scriptable) vs `tc netem` (closer to the kernel path).

**Acceptance:** correct delivery and bounded retries at each profile; no
spurious timeouts attributable to the client rather than the emulated link.

### B3 — Memory soak

**Goal:** prove there is no leak or unbounded growth over a long run (buffer
pool, in-flight maps, idempotent state, metadata cache).

**Status:** first signal only. The 4 h 17 m soak showed client RSS at
17 MiB start / 37 MiB max / 13.4 MiB end with no growth trend across ~25
broker-kill cycles — but slow growth (per-connection, per-leader-change,
per-metadata-refresh) needs the full 8–24 h window with the buffer-pool and
in-flight gauges scraped, which has not happened. The natural vehicle is
extending B1's compound re-run.

**Approach:** run B1's workload for 8–24h with RSS sampled periodically and
the buffer-pool / in-flight gauges scraped from producer metrics. Include
churn (reconnects, leader changes, metadata refreshes) so any per-event
allocation that is never freed accumulates visibly; optionally run under a
heap profiler for a shorter window.

**Decisions needed:** soak duration; acceptable RSS ceiling/slope.

**Acceptance:** RSS plateaus after warm-up; buffer-pool and in-flight gauges
return to baseline between bursts.

### B4 — Latency-percentile gates

**Goal:** turn latency from an anecdote into a gated metric (p50/p99/p999
end-to-end produce latency) so regressions are caught.

**Status:** measurement exists, gating does not. The producer and consumer
bench binaries record per-record send→ack latency and emit p50/p99/p999, and
a matched-load comparison against Java is published in `benches/README.md`.
What is missing is the gate itself: **agreed SLO thresholds** (a product
call, not an implementation one) and a CI job that fails on regression.

**Approach:** wire the existing percentiles into CI as a soft gate first,
then a hard gate once thresholds are agreed — set relative to the published
Java comparison on the same hardware.

**Decisions needed:** p99/p999 targets (absolute and/or relative-to-Java), the
workload they apply to, and soft-vs-hard gating in CI.

**Acceptance:** percentiles measured and reported per run; CI flags a
regression beyond the agreed threshold.

### Shared prerequisites

- A longer-lived cluster than the throwaway compose (or the compose with
  `KAFKA_HEAP_OPTS` raised and data volumes sized for a multi-hour run).
- A runner that can sustain the target rate without becoming the bottleneck
  (for real numbers, brokers and client should be on separate hosts).
- Network emulation tooling for B2 (`toxiproxy` or `tc netem`).
- Agreed thresholds for B3/B4 before they can be gates rather than reports.

## Release Bar

Met today:

- protocol compatibility checks for generated schemas against Kafka Java
  (the byte-for-byte oracle matrix);
- mock broker and real Kafka integration tests for every request path;
- explicit timeout, disconnect, retry, and leadership-change behavior
  (documented in the README's cancellation & drop semantics);
- documented config compatibility boundaries against Kafka Java (the
  generated config catalog and the book's field guide);
- clear public API stability policy and changelogged release notes.

Outstanding — both resolve to the
[Production Acceptance Plan](#production-acceptance-plan):

- bounded memory and in-flight behavior under sustained load (B1's compound
  re-run, B3's long soak);
- reproducible benchmarks on realistic batching and multi-broker workloads
  (B1 at an agreed target rate, B4's gates).

## Development Rules

- Prefer generated protocol structs and `kacrab-protocol` helpers over
  handwritten byte offsets.
- Keep `wire` responsible for connection ownership and request/response
  dispatch.
- Keep `producer` responsible for accumulation, routing, record-batch
  construction, dispatch, retries, and delivery reporting.
- Keep generated protocol files under `kacrab-protocol/src/generated/`
  untouched unless the generator changes.
- Keep new modules in the repo's facade-plus-directory style:
  `src/foo.rs` plus `src/foo/*.rs`, not `src/foo/mod.rs`.
- Use Makefile targets for normal verification:
  `make fmt-check`, `make clippy`, `make deny`, and `make test`.

