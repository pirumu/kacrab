# kacrab Direction

This document tracks current project direction. It is not a dated delivery
plan, and it intentionally avoids milestone labels that imply release
readiness.

`kacrab` is pre-release. The goal remains a high-performance, 100% pure Rust
Kafka client with no `librdkafka`, no C client bindings, and no unsafe code in
the workspace.

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
- `admin` owns the `AdminClient` API — the full Apache Kafka 4.3.0 `Admin`
  operation surface (62 operations) with controller/coordinator/per-leader/
  broadcast routing, verified against a real broker.
- `benches` contains accumulator, wire-pipeline, producer-dispatcher, mock
  broker, and real Kafka benchmark hooks with local baselines.

## Active Priorities

1. Harden the wire layer for sustained multi-broker workloads:
   reconnect/backoff behavior, metadata invalidation on leadership errors,
   predictable in-flight cleanup, and lower-allocation dispatch.
2. Harden producer behavior under load:
   batching efficiency, retry semantics, delivery timeout accounting, memory
   pressure, partition routing, leadership changes, idempotence, and
   transactions.
3. Expand stress and benchmark coverage:
   multi-broker real Kafka runs, leadership movement, soak tests, latency
   percentiles, and regression thresholds for the 3M messages/sec target.
4. Keep public API ergonomics close to Kafka Java where that helps users, while
   preserving explicit Rust ownership and error handling.

## Not Ready Yet

- Consumer APIs are not implemented.
- Streams APIs are not implemented.
- Release stability is not guaranteed.
- Local benchmark baselines are not production acceptance claims.

Consumer work should wait until the wire and producer layers have the measured
batching, backpressure, routing, and multi-broker behavior they need.

## Release Bar

Before calling this production-ready, the project needs:

- protocol compatibility checks for generated schemas against Kafka Java;
- mock broker and real Kafka integration tests for every request path;
- bounded memory and in-flight behavior under sustained load;
- explicit timeout, disconnect, retry, and leadership-change behavior;
- documented config compatibility boundaries against Kafka Java;
- reproducible benchmarks on realistic batching and multi-broker workloads;
- clear public API stability policy and changelogged release notes.

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

