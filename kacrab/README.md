# kacrab

The main `kacrab` crate — the public entry point users depend on.

> ⚠️ Pre-alpha. Public API is empty pending Phase 0 (protocol
> primitives). Not yet usable. See the [workspace ROADMAP](../ROADMAP.md).

## What lives here

This crate is the assembly point for the Kafka client. As phases land,
modules are added and re-exported from `lib.rs`:

| Phase | Module | What it adds |
|---|---|---|
| 0 | `protocol` | Primitive types: `int8`, `varint`, `compact_string`, tagged fields, `uuid`, ... |
| 1 | `wire` | TCP framing, request/response headers, correlation manager, generic `send`. |
| 2 | `metadata` | `Cluster`, `Broker`, `TopicMetadata`; record batch v2 codec. |
| 3 | `producer` | Topic→partition routing, ack=1 producer. |
| 4 | `consumer` | Manual-assignment consumer, `Fetch`, `ListOffsets`. |
| 5 | `consumer::group` | `JoinGroup`/`SyncGroup`/`Heartbeat`/`OffsetCommit` state machine. |
| 6 | `producer::idempotent` | Per-partition sequence numbers, `InitProducerId`. |
| 7 | `transaction` | `AddPartitionsToTxn`/`EndTxn`/transactional offset commit. |
| 8 | (cross-cutting) | Batching, compression, buffer pool, pipelining, zero-copy. |
| 9 | (cross-cutting) | Reconnection, backpressure, observability, TLS, SASL. |
| 10 | — | Public API polish, examples, docs, release. |

See [`../ROADMAP.md`](../ROADMAP.md) for the full task breakdown,
validation criteria, and common traps per phase.

## Constraints

- `#![no_std]` where possible (protocol primitives are pure functions
  over byte buffers).
- `unsafe_code = "forbid"` workspace-wide. No exceptions.
- Strict lints (clippy `all`/`pedantic`/`nursery`/`cargo` at `deny`,
  ~35 restriction lints opted in).

## Sub-crates

The runtime API is split across several crates so that downstream
projects can take only what they need:

- [`kacrab-macros`](../kacrab-macros/) — derives consumed via the
  optional `macros` feature (not yet wired up).
- [`kacrab-util`](../kacrab-util/) — adapters and helpers that build
  on top of `kacrab`'s public API.
- [`kacrab-test`](../kacrab-test/) — fixtures and mocks for downstream
  test code.

## License

MIT — see [LICENSE](../LICENSE).
