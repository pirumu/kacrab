# Protocol code generation

> **Draft**
>
> Outline chapter.

The wire types are generated, not hand-written, and checked against the Java
client as an external oracle. Planned coverage:

- **`kacrab-codegen`** — a maintainer tool (not published) that parses the
  upstream Apache Kafka 4.3.0 message schemas and emits Rust request/response
  structs into `kacrab-protocol`, plus typed config metadata from the config
  docs.
- **The pipeline** — schema parse → Rust codegen → format, with version-aware
  field handling (nullable, flexible/compact, tagged fields, nested schemas).
- **The Java oracle matrix** — generated test fixtures are encoded by Rust and
  decoded by the real Kafka Java client (and vice-versa), proving cross-language
  byte compatibility. Six fixture families — `null_optionals`, `populated`,
  `empty_collections`, `multi_element_collections`, `numeric_boundaries`,
  `tagged_fields` — at 625 cases each.
- **Why an oracle** — a Rust-only round trip passes even if Rust consistently
  writes the wrong shape and reads its own wrong shape back. The Java client is
  the external source of truth for Kafka's wire contract.
