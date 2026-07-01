# kacrab-examples

Runnable examples for the public `kacrab` API. **Not published.**

## Scope

- `producer` - public `Producer` usage, batching, idempotence,
  transactions, and auth configuration examples.
- `admin` - public `AdminClient` usage: describe cluster, create/list/describe
  topics, describe/incremental-alter configs, add partitions, list offsets,
  list consumer groups, and delete topics.
- `config` - Kafka config facade and typed config conversion examples.
- `typed_serializer` - a custom `ProducerSerializer<T>` wired through
  `build_with_serializers` to send a strongly-typed value.

## Running

Run examples from the workspace root (the `producer`/`admin` examples need a
local broker, e.g. `docker compose -f docker-compose.kafka.yml up -d`):

```bash
cargo run -p kacrab-examples --example producer
cargo run -p kacrab-examples --example admin
cargo run -p kacrab-examples --example config
cargo run -p kacrab-examples --example typed_serializer
```

The `admin` example takes optional positional args `bootstrap topic partitions`:

```bash
cargo run -p kacrab-examples --example admin -- 127.0.0.1:9092 my-topic 3
```

## Author

`kacrab-examples` is authored and maintained by `pirumu`.

## License

This crate is licensed under either MIT or Apache-2.0, matching the workspace.
