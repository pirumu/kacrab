# examples (`kacrab-examples`)

Internal example programs for the kacrab workspace. **Not published.**

> ⚠️ Empty scaffold. Real examples land alongside Phase 3 (producer)
> and Phase 4 (consumer).

## Planned examples

Per ROADMAP §Phase 10:

- `simple_producer` — produce N messages to a topic, log result.
- `simple_consumer` — consume from `(topic, partition, offset)`,
  print records.
- `consumer_group` — join a group, handle rebalances.
- `transactional` — read-from-A → transform → write-to-B atomically.
- `with_tls` — `rustls` integration against a TLS-enabled broker.
- `custom_partitioner` — supply a partitioner closure to the producer.

These also serve as the integration-guide reference for "I'm coming
from `rdkafka`, what do I need to know?"

## Running

```bash
cargo run -p kacrab-examples --example simple_producer
cargo run -p kacrab-examples --example simple_consumer -- --topic test --partition 0
```

A local Kafka broker (KRaft mode, port 9092) is assumed. The repo's
`compose.yaml` (Phase 1) provides one.

## License

MIT — see [LICENSE](../LICENSE).
