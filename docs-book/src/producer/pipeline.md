# The producer pipeline

> **Draft**
>
> This chapter is an outline; the sequencing details live in
> [Idempotency & transactions](./idempotency.md).

From `producer.send(record)` to bytes on a socket. Planned coverage:

- **The accumulator** — per-topic-partition record batching, `linger.ms`,
  `batch.size`, bounded `buffer.memory` / `max.block.ms`.
- **Synchronous `send`** — the Kafka `Producer.send` shape: a plain `fn` that
  enqueues and returns a `SendFuture`, not an `async fn`.
- **Drain → dispatch** — draining ready batches, grouping them per broker leader,
  splitting on `max.request.size`, and the `EnqueueSequencer` ordering.
- **Delivery** — `RecordMetadata` receipts, callbacks, `flush`, and `close`.
- **Interceptors & metrics** — `ProducerInterceptor` lifecycle and Kafka-named
  metrics (including the buffer-pool gauges).
