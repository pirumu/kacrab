# Compression

> **Draft**
>
> Outline chapter; the end-to-end proof is in [Verification](./verification.md).

Record-batch compression, feature-gated per codec. Planned coverage:

- **Codecs** — `gzip` (flate2), `snappy` (snap), `lz4` (pure-Rust `lz4_flex`,
  fast mode), `lz4-hc` (C-FFI liblz4, high-compression levels 3..=12), `zstd`.
  The `compression` meta-feature enables all four pure-Rust codecs.
- **Framing** — the codec sits inside record-batch v2; the CRC32C is computed
  over the *compressed* payload, so the framing must match Kafka exactly. This
  is why it is [verified on a real broker](./verification.md), not just
  round-tripped in process.
- **Levels** — `compression.{gzip,lz4,zstd}.level`. With pure-Rust `lz4` the
  level is ignored (fast mode only); `lz4-hc` honors 3..=12.
- **Selection** — `compression.type` is the wire codec (`gzip`/`snappy`/`lz4`/
  `zstd`/`none`); there is no separate `lz4-hc` wire type — it is an encoder
  choice that still emits a standard LZ4 frame.
