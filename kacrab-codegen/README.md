# kacrab-codegen

Maintainer code generator for Kafka protocol schemas and config metadata.
**Not published as a runtime dependency.**

## Overview

`kacrab-codegen` reads upstream [Kafka message JSON specs][specs] and emits one
Rust module per message type. The generated source is committed under
[`kacrab-protocol/src/generated/`][gen-dir], which gives downstream users:

- A fast, clean build with no `build.rs` step.
- Schema bumps as reviewable diffs in `git`.
- IDE jump-to-def without running any tooling first.

This crate is maintainer build tooling, not a runtime dependency. End users of
`kacrab` consume the committed output and never invoke the binary.

The protocol pipeline is split into four stages, each owning its own error
type: `parser`, `codegen`, `format`, plus an optional `errors_java` side
channel for the Kafka error-code table.

The same CLI also exposes a `config` subcommand, which extracts client
configuration metadata from upstream Kafka Java `ConfigDef` declarations
into a JSON snapshot. Prefer that snapshot over hand-maintaining large
config catalogs: pin the upstream source ref, regenerate, then review
the diff.

## Usage

Regenerate the protocol crate from the same pinned Kafka release used by
the config catalog:

```sh
cargo run -p kacrab-codegen -- protocol \
    --kafka-ref 4.3.0 \
    --output-dir kacrab-protocol/src/generated \
    --schema-snapshot-dir kacrab-codegen/schemas
```

Remote protocol mode downloads the Kafka source archive from GitHub's
codeload endpoint, reads
`clients/src/main/resources/common/message`, and automatically emits the
`(code, name, retriable)` table from upstream's `Errors.java`.
`--schema-snapshot-dir` refreshes the bundled offline snapshot from the
same upstream tree, including `SOURCE_REF`, `VERSION`, and `Errors.java`.

Pass `--dry-run` to print to stdout instead of writing files. For
offline work or CI jobs that already provide a Kafka checkout, pass
`--kafka-root` and an explicit `--source-ref` instead of `--kafka-ref`.
The protocol command derives both message schemas and `Errors.java` from
that root.

If you only want to regenerate from the bundled protocol snapshot, pass
`--schemas-dir`, `--source-ref`, and optionally `--errors-java`:

```sh
cargo run -p kacrab-codegen -- protocol \
    --schemas-dir kacrab-codegen/schemas \
    --source-ref apache/kafka@4.3.0 \
    --errors-java kacrab-codegen/schemas/Errors.java \
    --output-dir kacrab-protocol/src/generated
```

Run with `--help` for the full surface.

Extract Kafka client config metadata from a pinned Kafka release:

```sh
cargo run -p kacrab-codegen -- config \
    --kafka-ref 4.3.0 \
    --output kacrab-codegen/schemas/config/4.3.0.json \
    --native-schema kacrab/src/config/clients.rs \
    --rust-catalog-output kacrab/src/config/catalog.rs
```

The remote mode downloads the Kafka source archive from GitHub's codeload
endpoint into a temporary directory, then removes the temporary tree when
the command exits. It uses the system `curl` and `tar` binaries.

For offline config work or CI jobs that already provide a Kafka checkout,
pass `--kafka-root` and an explicit `--source-ref` instead of
`--kafka-ref`; the config command derives `clients/src/main/java` from
that root.

For release branches/tags, keep `--kafka-ref` or `--source-ref` pinned
to the Kafka tag the release targets. For `master`/development, use an
explicit upstream commit SHA instead of a floating branch name so
generated diffs remain reproducible.

`--native-schema` lets the generator classify already-exposed typed
fields as `ConfigStatus::Native`; official keys that are present in
Kafka but not yet exposed stay reviewable or feature-gated instead of
silently becoming stable Rust API.

## When to regenerate

Re-run the codegen and commit the diff whenever:

- The targeted Kafka protocol source ref changes.
- An upstream schema bumps version or gains/loses a field.
- The codegen itself emits Rust differently (template change,
  `prettyplease` upgrade, etc.).
- The project bumps its targeted Kafka config source ref.

Don't hand-edit the generated files — edit the codegen, not the output.

## Getting Help

Open an [issue][issues] or start a [discussion][discussions] on the
main `kacrab` repository.

## Contributing

Contributions are welcome. Please follow the workspace's
[`CONTRIBUTING.md`](../CONTRIBUTING.md) and open an issue first to discuss
non-trivial changes.

## Supported Rust Versions

`kacrab-codegen` tracks the rest of the workspace; see the root
[`Cargo.toml`](../Cargo.toml) for the current `rust-version` (currently
`1.95`).

## Contribution Licensing

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in `kacrab-codegen` by you, as defined in the Apache-2.0 license,
shall be dual-licensed as above, without any additional terms or conditions.

## Author

`kacrab-codegen` is authored and maintained by `pirumu`.

## License

This crate is licensed under either MIT or Apache-2.0, matching the workspace.

[specs]: https://github.com/apache/kafka/tree/trunk/clients/src/main/resources/common/message
[gen-dir]: ../kacrab-protocol/src/generated
[issues]: https://github.com/pirumu/kacrab/issues
[discussions]: https://github.com/pirumu/kacrab/discussions
