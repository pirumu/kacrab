# Testing, coverage & CI

An expedition's claims are worth what its instruments can prove. Everything
asserted in Parts I–V rests on three layers of evidence, each catching what
the layer below cannot:

| Layer | What it proves | Where it runs |
|---|---|---|
| In-process tests | kacrab is internally consistent: state machines, framing, routing, backoff | every push / PR (uninstrumented) |
| Java oracle fixtures | kacrab is *byte-for-byte* compatible with Apache Kafka's own algorithms | same suite (committed fixtures) |
| Real-broker verification | kacrab is *Kafka*-consistent: it talks to real brokers, not just to itself | on demand (`#[ignore]` + docker) |

The first two are the bulk of the suite and gate every change. The third —
SASL/TLS, compression on disk, multi-broker failover — is covered in
[Verification against real brokers](./verification.md).

## The Java oracle

A Rust-only round trip is a closed loop: kacrab encodes and kacrab decodes, so a
subtly wrong CRC, varint, or murmur2 seed passes anyway. The oracle breaks the
loop by pinning kacrab's output to values produced by Kafka's *own* Java code:
murmur2 hashes for every key length, CRC32C frames, zig-zag varints, the sticky
partitioner's distribution. These are committed fixtures, so the parity check
runs with no Java toolchain in CI. See
[Design decisions & Java parity](./design-decisions.md).

## Coverage — measured with `cargo-llvm-cov`

CI gates line coverage with [`cargo-llvm-cov`][llvm-cov]. Maintained-source
coverage is **~87.5%** (generated protocol/config artifacts excluded via
`--ignore-filename-regex`), with the producer module around **92%**. Generated
code is held to a different standard — it is validated by the generator's own
tests and the Java oracle, not by line coverage — so counting it would only
dilute the signal (the raw all-files figure is ~63%, dominated by message
structs for APIs not yet wired).

```bash
cargo llvm-cov --workspace --all-features \
  --ignore-filename-regex '(benches/|kacrab-codegen/src/main\.rs|kacrab-macros/src/lib\.rs|kacrab/src/config/catalog\.rs|kacrab-protocol/src/generated)'
```

> **Why not tarpaulin**
>
> The coverage tool *itself* turned out to matter. kacrab's test suite leans
> heavily on real timeouts and blocking windows — `max.block.ms`,
> `delivery.timeout.ms`, leadership-retry deadlines — often set to a few
> milliseconds so a test can assert the timeout *fires*. `cargo-tarpaulin`'s
> instrumentation slows execution by ~10–50×, enough to blow through those
> windows: a `FindCoordinator` + `InitProducerId` round trip that takes
> microseconds bare would exceed a 30 ms budget under instrumentation, so the
> producer correctly — but spuriously — timed out. The result was a *shifting*
> set of flaky failures, different tests from one run to the next.
>
> `cargo-llvm-cov` uses LLVM source-based coverage and runs the tests at
> near-native speed. The same timeout tests that flaked under tarpaulin finish
> in 0.07 s instrumented versus 0.06 s bare — the timing windows hold, so
> coverage is both **reliable** and a **real gate**, not a best-effort report.

The lesson generalises: for an async, timeout-driven codebase, prefer a coverage
tool that doesn't perturb timing. A coverage job that flakes is worse than none
— it trains everyone to ignore red.

## The CI pipeline

Three jobs run on every push to `master` and every pull request:

| Job | Enforces |
|---|---|
| `fmt · clippy · test` | nightly `rustfmt`, the strict clippy lint set, and the **full** suite — uninstrumented, so it is the authoritative correctness gate |
| `coverage (llvm-cov)` | the `--fail-under-lines` floor described above, and publishes a Cobertura report |
| `cargo-deny` | license, advisory (RUSTSEC), and dependency-ban policy |

Two deliberate refinements keep the pipeline honest and cheap:

- **The test job is the source of truth, not coverage.** It runs the whole suite
  at native speed; the coverage job measures the same suite but is judged only on
  the coverage floor. Correctness and measurement are separated on purpose.
- **Docs-only changes skip the code CI.** A `paths-ignore` filter means editing
  this book, a README, or a license file does not trigger the ~20-minute
  fmt/clippy/test/coverage run. The book has its own deploy
  (`docs.yml` → GitHub Pages); a change that touches both code and docs still
  runs the full pipeline.

[llvm-cov]: https://github.com/taiki-e/cargo-llvm-cov
