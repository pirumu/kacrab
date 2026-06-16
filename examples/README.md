# kacrab-examples

Runnable examples for the public `kacrab` API. **Not published.**

## Scope

- `producer` - public `KafkaProducer` usage, batching, idempotence,
  transactions, and auth configuration examples.
- `config` - Java-style config facade and typed config conversion examples.

## Running

Run examples from the workspace root:

```bash
cargo run -p kacrab-examples --example producer
cargo run -p kacrab-examples --example config
```

## Author

`kacrab-examples` is authored and maintained by `pirumu`.

## License

This crate is licensed under either MIT or Apache-2.0, matching the workspace.
