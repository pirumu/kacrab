# kacrab-test

Test fixtures and mocks for downstream users of [`kacrab`](../kacrab/).

> ⚠️ Pre-alpha. Empty scaffold.

## Scope

Test ergonomics for code that depends on `kacrab`. Likely contents:

- A mock broker (`MockBroker`) — listens on a local port, accepts the
  protocol, and lets tests script responses for specific API keys.
- Fixtures for record batches, metadata responses, and consumer group
  state.
- `proptest` strategies for protocol primitives (re-usable in
  downstream property tests).
- Helpers for spinning up a Docker-Compose Kafka cluster from a test.

## Usage

```toml
[dev-dependencies]
kacrab-test = "0.1"
```

## License

MIT — see [LICENSE](../LICENSE).
