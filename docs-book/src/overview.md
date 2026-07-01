# System overview

kacrab is three layers, each a crate or module with a sharp boundary.

```mermaid
flowchart TB
  subgraph app[Your application]
    P["Producer API<br/>send / flush / transactions"]
  end
  subgraph kacrab[kacrab crate]
    CFG["config<br/>Java-style ClientConfig,<br/>typed configs, validation"]
    PROD["producer<br/>accumulator → dispatch →<br/>idempotent state machine"]
    WIRE["wire<br/>broker sessions, request pipeline,<br/>SASL/TLS, metadata, reconnect"]
  end
  subgraph proto[kacrab-protocol crate]
    GEN["generated request/response types,<br/>record batch v2, compression, CRC32C"]
  end
  P --> PROD --> WIRE --> GEN
  PROD -.uses.-> CFG
  WIRE -.uses.-> GEN
```

- **`config`** — the Kafka property surface (`bootstrap.servers`, `acks`,
  `compression.type`, `sasl.*`, `ssl.*`, …) with typed parsing and validation.
  Generated from official Kafka config metadata. See
  [Protocol code generation](./codegen.md).
- **`wire`** — the async transport: per-broker Tokio sessions, a bounded
  in-flight request pipeline, the SASL/TLS handshakes, metadata fetch +
  leader-change invalidation, and a reconnect/backoff policy. See
  [The wire layer](./wire.md) and [Security](./security.md).
- **`common`** — shared `org.apache.kafka.common` domain types
  (`TopicPartition`, `OffsetAndMetadata`, `Node`, …), always compiled and
  re-exported by `producer`/`admin`.
- **`producer`** — batching accumulator, the dispatch path that groups batches
  per broker leader, and the idempotent/transactional state machine. See
  [The producer pipeline](./producer/pipeline.md) and
  [Idempotency & transactions](./producer/idempotency.md).
- **`admin`** — the `AdminClient`: the full Apache Kafka 4.3.0 `Admin` operation
  surface (62 operations) with controller/coordinator/per-leader/broadcast
  routing. See [The admin client](./admin.md).
- **`kacrab-protocol`** — the generated wire types, record-batch v2 encode/decode,
  compression codecs, and CRC32C. Zero hand-written byte patching.

The consumer and streams surfaces are not implemented yet; the wire and
protocol foundations they will sit on already exist.
