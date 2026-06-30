# Partitioning

> **Draft**
>
> Outline chapter.

How a record's partition is chosen. Planned coverage:

- **Keyed records** — `murmur2(key) % partitions`, **byte-exact** with the Java
  client for every key length (verified directly).
- **Null-key records** — the sticky/adaptive partitioner: stick to one partition
  per batch, rotate on batch completion, and weight by partition availability,
  matching Java's `BuiltInPartitioner`.
- **Explicit partition** — `ProducerRecord::new(topic, partition)` bypasses the
  partitioner.
- **Custom partitioners** — the `ProducerPartitioner` hook.
