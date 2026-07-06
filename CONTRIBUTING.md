# Contributing to kacrab

Thanks for taking the time to contribute.

`kacrab` is a 100% pure Rust Kafka client. Protocol generation,
wire sessions, auth, producer, consumer, and admin are usable and verified
against real brokers; the project is now hardening sustained-load behavior and
release guarantees. Please keep changes focused and production-minded:
correctness first, then measurable throughput.

## Before You Start

- Read [README.md](README.md) and [ROADMAP.md](ROADMAP.md) for current status
  and priorities.
- Read the [Design & Internals book](https://pirumu.github.io/kacrab/)
  ([`docs-book/`](docs-book/)) — it is the architecture onboarding: every
  subsystem's algorithms, invariants, and verification strategy are written
  down there so understanding the code does not require asking the maintainer.
- The pinned Apache Kafka Java source under `upstream/` is the ground truth
  for compatibility questions; cite it in Java-parity discussions.
- Check existing issues or open a short proposal before large design changes.
- Keep generated protocol files under `kacrab-protocol/src/generated/`
  untouched unless the generator changes.

## Where Help Is Wanted

Good self-contained entry points, roughly in order of impact:

- **Production acceptance** (`ROADMAP.md` → Production Acceptance Plan):
  sustained multi-broker stress, cross-DC/high-RTT emulation, memory soak, and
  latency-percentile harnesses. Infrastructure- and measurement-heavy, well
  scoped, and largely independent of the client internals.
- **Real-broker test coverage**: new scenarios in `kacrab/tests/real_kafka_*.rs`
  against the `docker-compose.*.yml` fixtures (failover, auth edge cases,
  compression matrices).
- **Benchmarks**: additional workloads in `benches/` (record-size sweeps,
  open-loop latency, multi-broker) with honest methodology notes.
- **Docs**: corrections and deep-dive chapters in `docs-book/`.

## Development Setup

This is a Rust workspace. Use the pinned toolchain in
[rust-toolchain.toml](rust-toolchain.toml).

Common commands:

```sh
make fmt
make fmt-check
make clippy
make test
```

For protocol work, prefer generated protocol structs and compatibility tests over
hand-written byte expectations.

## Coding Standards

- Follow the facade-plus-directory module style used in the repo:
  `src/foo.rs` plus `src/foo/*.rs`. Do not add `src/foo/mod.rs`.
- Keep public re-exports in the facade file.
- Keep implementation files narrow: `error.rs`, `client.rs`, `session.rs`,
  `routing.rs`, `batch.rs`, `response.rs`, and similar focused names.
- Keep client surfaces behind their explicit cargo features (`producer`,
  `consumer`, `admin`); every feature must also build standalone.
- Keep the project pure Rust. Do not add C client bindings, `librdkafka`
  wrappers, or native Kafka client dependencies.
- Do not add unsafe code. The workspace forbids `unsafe_code`.
- Use `Bytes` and `BytesMut` deliberately in hot paths.
- Avoid unbounded queues, global hot-path mutexes, and one allocation per
  message where batching can amortize work.

## Testing Expectations

Add focused tests with the behavior you are changing.

Before opening a pull request, run:

```sh
make fmt-check
make clippy
make test
```

If your change affects wire compatibility, producer routing, batching,
backpressure, retries, or timeouts, include integration coverage with mock TCP
brokers where practical. If you make a performance claim, add or update a
benchmark.

## Pull Request Checklist

- The change is scoped to one concern.
- Tests cover the new or changed behavior.
- Benchmarks are included for performance-sensitive claims.
- Public API changes are intentional and documented.
- Generated files changed only because generator semantics changed.
- `make fmt-check`, `make clippy`, and `make test` have been run recently.

## Licensing

By contributing, you agree that your contribution is licensed under the same
terms as the project: MIT OR Apache-2.0.
