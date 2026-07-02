# kacrab-protocol

Kafka wire protocol primitives, generated request/response structs, record
batches, compression, framing, and version negotiation for `kacrab`.

## Scope

- `primitives`, `string`, `bytes_io`, `uuid`, and `tagged` implement Kafka wire
  encoding helpers.
- `frame` owns request/response frame helpers.
- `record` owns Kafka record batch v2 encoding and decoding.
- `compression` owns gzip, snappy, lz4, and zstd codec dispatch behind feature
  flags.
- `version` resolves supported API and header versions.
- `generated` contains committed output from `kacrab-codegen`; do not edit it
  by hand.

## Features

- `gzip`, `snappy`, `lz4`, and `zstd` enable record-batch compression codecs.
- `lz4-hc` enables the C-FFI high-compression LZ4 backend.
- `compression` enables `gzip`, `snappy`, `lz4`, and `zstd`.
- `message-enums` enables generated request/response wrapper enums over the
  per-message structs.

The default feature set is empty so the protocol crate can stay small for
callers that only need primitive/generated message support.

## Verification

Run protocol tests from the workspace root:

```bash
make test-protocol
make test-protocol-java
make test-protocol-java-matrix
```

The Java matrix is ignored by default because it requires Java, Maven, and the
Kafka client jar. It is the compatibility oracle for generated Kafka message
wire shapes.

## Author

`kacrab-protocol` is authored and maintained by `pirumu`.

## License

This crate is licensed under either MIT or Apache-2.0, matching the workspace.
