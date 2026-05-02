# tests-integration (`kacrab-tests-integration`)

Cross-crate integration tests for the kacrab workspace. **Not published.**

> ⚠️ Empty scaffold. First tests land in Phase 1 (ApiVersions handshake
> against a real broker).

## Scope

Tests that don't fit inside any single crate's `tests/` directory —
either because they exercise multiple workspace crates together, or
because they require a real Kafka broker reachable on localhost.

Per ROADMAP, the validation suite includes:

- **Phase 1** — connect, complete `ApiVersions` handshake, log
  supported versions, compare against `kcat -L`.
- **Phase 2** — produce a hand-crafted record batch, consume back with
  `kafka-console-consumer`, bytes match.
- **Phase 3** — produce 10 000 messages to a 3-partition topic, count
  matches on consume, no reordering within a partition.
- **Phase 4** — cross-test producer / consumer interop (this client ↔
  `kafka-console-*`).
- **Phase 5** — consumer group rebalance scenarios (kill member, add
  member, hung member).
- **Phase 6** — idempotence under injected network failures
  (toxiproxy).
- **Phase 9** — chaos suite: broker hangs, slow network, DNS failure,
  coordinator dies, disk full, clock skew.

## Running

```bash
# Local broker required (KRaft mode, port 9092).
docker compose up -d
cargo test -p kacrab-tests-integration
```

CI runs the same suite against a Docker-Compose Kafka cluster.

## License

MIT — see [LICENSE](../LICENSE).
