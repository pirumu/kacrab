# kacrab

A Kafka client for Rust, built from the wire protocol up.

* **Correct**: every primitive is property-tested and round-trips exactly per the Kafka spec.
* **Async**: Tokio-native, non-blocking I/O end to end.
* **Lean**: minimal dependencies, no `unsafe` in the public API.

[![MIT licensed][mit-badge]][mit-url]

[mit-badge]: https://img.shields.io/badge/license-MIT-blue.svg
[mit-url]: https://github.com/pirumu/kacrab/blob/main/LICENSE

> **Status:** pre-alpha. Phase 0 (protocol primitives) in progress. Not ready for use yet. See [ROADMAP.md](ROADMAP.md).

## Overview

`kacrab` is a Kafka client implemented from the wire format up: protocol
primitives, TCP framing and correlation, metadata, record batches,
producer, consumer, consumer groups, idempotence, transactions. The
goal is a correctness-first client that stays within 2× of `librdkafka`
on throughput once Phase 8 lands.

This is also a learning project — it is built without any LLM
assistance. The constraints and references are in [ROADMAP.md](ROADMAP.md).

## Example

> Pre-alpha — the producer API below is the target shape, not yet
> functional. Tracking issue lives in `ROADMAP.md` (Phase 3).

```rust,no_run,ignore
use kacrab::Producer;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let producer = Producer::connect("localhost:9092").await?;
    producer.send("my-topic", Some(b"key"), b"value").await?;
    Ok(())
}
```

## License

This project is licensed under the [MIT license](LICENSE).
