# Traveling light: compression

A caravan moves faster carrying less, and so does a Kafka pipeline — compressed
batches multiply effective network *and broker disk* throughput. Compression
happens at the record-batch level: a batch of records is serialized, the record
block is compressed, and the compressed bytes go inside a record-batch v2
envelope with a CRC over them. Every Kafka client and broker must frame this
identically — get it *almost* right and your own process reads the bytes back
fine while the broker rejects them — which is why kacrab's codecs are
[proven on a real broker](./verification.md), not just round-tripped in
process.

## Codecs

Each codec is a feature, so you only pull in the backends you use:

| Feature | Backend | Notes |
|---|---|---|
| `gzip` | `flate2` (pure Rust) | standard DEFLATE/gzip |
| `snappy` | `snap` (pure Rust) | Kafka's block framing |
| `lz4` | `lz4_flex` (pure Rust) | LZ4 frame, **fast mode only** |
| `lz4-hc` | `lz4` (C-FFI to liblz4) | adds high-compression levels 3..=12; needs a C compiler |
| `zstd` | `zstd` (C) | best ratio; the modern Kafka default |

The `compression` meta-feature turns on `gzip` + `snappy` + `lz4` + `zstd`. The
first three are pure Rust; **`zstd` uses the C libzstd** (via `zstd-sys`), so
enabling `compression` — or `zstd` on its own — needs a **C compiler at build
time**. `lz4-hc` is a separate opt-in for the same reason (C-FFI to liblz4);
when both `lz4` and `lz4-hc` are enabled, the C-FFI backend wins.

For a fully pure-Rust, no-C-toolchain build (e.g. easy cross-compilation),
enable only `gzip` + `snappy` + `lz4` and skip `zstd` / `lz4-hc` / the
`compression` meta-feature.

## Selecting a codec

`compression.type` is the wire codec — `gzip` / `snappy` / `lz4` / `zstd` /
`none`:

```rust
Producer::builder()
    .set("compression.type", "zstd")
    .set("compression.zstd.level", "6")   // optional
    .build().await?;
```

There is no `lz4-hc` *wire* type — high-compression is an encoder choice that
still emits a standard LZ4 frame, so any broker/consumer reads it back normally.

## Levels

`compression.{gzip,lz4,zstd}.level` set the codec level, matching the Java
client's keys.

> **lz4 level needs `lz4-hc`**
>
> With the pure-Rust `lz4` backend the level is **ignored** (fast mode only). To
> honor `compression.lz4.level` (3..=12) you must enable the `lz4-hc` feature,
> which routes those levels through liblz4's high-compression mode.

## Framing — why it must be exact

Inside record-batch v2 the layout is roughly: `[batch header][CRC32C][compressed
record block]`, and the **CRC is computed over the compressed bytes**. Get the
codec framing subtly wrong — Snappy's block format, LZ4's frame descriptor and
content checksum, the trailing markers — and the bytes still decompress in your
own process (you wrote them) but the broker rejects them or a real consumer can't
read them.

That gap is exactly why the [verification](./verification.md) test does three
things end to end: produce each codec, confirm with `kafka-dump-log` that the
on-disk batch is stored with the **right codec** (a silently-uncompressed send
fails here), and consume the payloads back through `kafka-console-consumer`.

## Field notes

- Default recommendation: `compression.type=zstd` for ratio, `lz4` when CPU
  is the scarce resource. Both are covered in the
  [producer field guide](./field-guide/producer.md).
- `compression.lz4.level` silently does nothing without the `lz4-hc`
  feature — the pure-Rust backend is fast-mode only.
- Consumers need no codec config: the batch header names its codec, and the
  `consumer` feature already enables all four backends.
- Building without a C toolchain (cross-compilation, minimal images): take
  `gzip` + `snappy` + `lz4` only, and skip `zstd`/`lz4-hc`/the
  `compression` meta-feature.
