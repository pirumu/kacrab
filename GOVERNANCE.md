# Governance

`kacrab` is currently maintained by the repository owner — a **single
maintainer**, stated plainly so nobody has to infer it. The mitigations for
that bus factor are structural, not aspirational:

- **All design knowledge lives in the repo.** The
  [Design & Internals book](https://pirumu.github.io/kacrab/) documents every
  subsystem's algorithms and invariants; `ROADMAP.md` records direction and
  non-goals; the pinned Apache Kafka source under `upstream/` is the
  compatibility oracle. Nothing load-bearing exists only in the maintainer's
  head.
- **Correctness is machine-checked, not remembered.** CI gates (fmt, clippy,
  tests, coverage), the byte-for-byte Java oracle matrix, and the real-broker
  test suites let a new maintainer change code with confidence.
- **The maintainer path below is real** — the explicit goal is at least two
  people with merge rights once the project has sustained contributors.

The project is pre-release, so governance is intentionally small and practical.
The main priority is building a correct, high-throughput, 100% pure Rust Kafka
client without unsafe code or native Kafka client bindings, while protecting
the wire and producer architecture.

## Decision Making

Maintainers make final decisions after weighing:

- Kafka protocol correctness.
- Keeping the project pure Rust and unsafe-free.
- Public API stability.
- Throughput and allocation impact.
- Backpressure and failure behavior.
- Testability against mock brokers and real Kafka.
- Fit with the current project priorities.

Large changes should start with an issue or design note before implementation.

## Maintainer Responsibilities

Maintainers are responsible for:

- Reviewing issues and pull requests.
- Protecting the wire and producer architecture from premature shortcuts.
- Keeping generated protocol output reproducible.
- Requiring tests and benchmarks for risky changes.
- Enforcing the code of conduct.

## Contributor Path

Consistent contributors may be invited to help triage issues, review pull
requests, or maintain focused areas such as protocol generation, wire sessions,
producer batching, consumer fetching/rebalancing, admin, or benchmarks.

The path to maintainership is concrete:

1. **Contributor** — merged pull requests that meet the testing bar in
   [CONTRIBUTING.md](CONTRIBUTING.md). See its "Where Help Is Wanted" section
   for scoped entry points.
2. **Area reviewer** — after several substantial contributions in one area,
   review requests for that area are routed to you.
3. **Maintainer** — sustained contributions and reviews over a few months,
   demonstrated judgment on correctness/performance trade-offs, and agreement
   with the non-goals in `ROADMAP.md`. Maintainers get merge rights; adding a
   second maintainer is an explicit project goal, not a hypothetical.

Trust is based on sustained quality, clear communication, and respect for the
project's performance and correctness goals.
