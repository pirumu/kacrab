# Contributing to kacrab

Thanks for taking the time to contribute.

`kacrab` is a pre-release, 100% pure Rust Kafka client. Protocol generation,
wire sessions, auth, and producer paths are usable, but the project is still
hardening batching, routing, multi-broker behavior, and release guarantees.
Please keep changes focused and production-minded: correctness first, then
measurable throughput.

## Before You Start

- Read [README.md](README.md) and [ROADMAP.md](ROADMAP.md) for current status
  and priorities.
- Check existing issues or open a short proposal before large design changes.
- Keep generated protocol files under `kacrab-protocol/src/generated/`
  untouched unless the generator changes.
- Do not start consumer work until the wire and producer layers have the
  batching, backpressure, routing, and benchmark shape needed for it.

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
- Preserve `no_std` by default in `kacrab`; runtime support should be behind
  explicit features.
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
