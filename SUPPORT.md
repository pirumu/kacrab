# Support

The project goal is a 100% pure Rust Kafka client with no unsafe code. Issues or
requests that propose native Kafka client bindings are outside that direction.

## Where to Ask

- Use GitHub issues for reproducible bugs, design questions, and focused feature
  requests.
- Use pull requests for concrete code or documentation changes.
- Keep broad direction discussion tied to [ROADMAP.md](ROADMAP.md).

## Good Bug Reports

Please include:

- `kacrab` commit or version.
- Rust toolchain version.
- Kafka broker version.
- Minimal code or test case.
- Expected behavior.
- Actual behavior, including logs or error output.
- Whether the issue reproduces against a mock broker, real Kafka, or both.

## Feature Requests

Feature requests should explain the Kafka API, producer behavior, or wire-layer
capability needed. For now, wire and producer hardening take priority over new
consumer functionality.

## Performance Reports

Performance reports are useful when they include:

- Hardware and OS.
- Kafka broker topology.
- Message size and count.
- Batch settings and in-flight settings.
- Command or benchmark used.
- Throughput and latency numbers.

Functional success alone is not enough for throughput claims.
