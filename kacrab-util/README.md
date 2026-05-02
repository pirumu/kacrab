# kacrab-util

Utilities and adapters built on top of [`kacrab`](../kacrab/).

> ⚠️ Pre-alpha. Empty scaffold.

## Scope

Helpers that are useful but not core to the protocol or runtime.
Examples of what this crate may host as the project progresses:

- Custom partitioner adapters (consistent hashing, sticky, key-based
  routing strategies beyond the default murmur2).
- Codec helpers — Avro / JSON / Protobuf serialization layered on top
  of `kacrab`'s `Record` type.
- `tracing` integrations and span helpers.
- A small `kcat`-style CLI helper for ad-hoc debugging.

Anything that doesn't belong in the core `kacrab` crate but ships from
this workspace lives here.

## License

MIT — see [LICENSE](../LICENSE).
