# Governance

`kacrab` is currently maintained by the repository owner.

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
producer batching, or benchmarks.

Trust is based on sustained quality, clear communication, and respect for the
project's performance and correctness goals.
