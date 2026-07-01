# Summary

[Introduction](./introduction.md)

# Architecture

- [System overview](./overview.md)
- [The wire layer](./wire.md)
- [The producer pipeline](./producer/pipeline.md)
- [The admin client](./admin.md)

# Algorithms (the deep dives)

- [Idempotency & transactions](./producer/idempotency.md)
- [Partitioning (murmur2 + sticky/adaptive)](./producer/partitioning.md)
- [Compression](./compression.md)
- [Security: SASL & TLS](./security.md)
- [Protocol code generation](./codegen.md)

# Correctness & performance

- [Failure modes](./failure-modes.md)
- [Testing, coverage & CI](./testing-and-ci.md)
- [Verification against real brokers](./verification.md)
- [Benchmarks & methodology](./benchmarks.md)

# Reference

- [Design decisions & Java parity](./design-decisions.md)
- [Glossary](./glossary.md)
