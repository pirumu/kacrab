# kacrab-macros

Procedural macros for [`kacrab`](../kacrab/).

> ⚠️ Pre-alpha. No macros exported yet.

## Scope

This crate exists because procedural macros must live in a separate
`proc-macro = true` crate (a Rust compilation constraint, not a design
choice). It is not intended to be used directly — consume it through
the main `kacrab` crate's `macros` feature once that lands.

Likely contents as the project progresses:

- `#[derive(Encodable)]` / `#[derive(Decodable)]` — Kafka wire-format
  derives for protocol message types.
- `#[derive(KafkaError)]` — codegen for the error-code enum from the
  Kafka protocol's official error list.
- Helper attribute macros for tagged-field handling (KIP-482).

## Usage

```toml
# Don't add this dependency directly. Use `kacrab` with the `macros` feature:
[dependencies]
kacrab = { version = "0.1", features = ["macros"] }
```

## License

MIT — see [LICENSE](../LICENSE).
