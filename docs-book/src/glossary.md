# Glossary

- **Base sequence** — the sequence number stamped on the first record of a batch;
  per-partition it must ascend without gaps.
- **Epoch** — the producer epoch, bumped to fence stale idempotent state after an
  ambiguous failure.
- **Ambiguous loss** — a failure with no broker response, where the client cannot
  know if the write landed; resolved by an epoch bump.
- **EnqueueSequencer** — kacrab's ticket-based ordering that reconstructs Java's
  single-Sender-thread enqueue order across concurrent dispatch tasks.
- **`inflightBatchesBySequence`** — the per-partition map of sent-but-unacked
  batches, ordered by base sequence.
- **`firstInFlightSequence`** — the oldest unacked sequence on a partition; the
  retry gate.
- **Record batch v2** — the Kafka record-batch format (magic byte 2) that wraps
  compressed records with a CRC32C over the compressed payload.
- **Oracle matrix** — the Java-client-checked test fixtures that prove Rust's
  generated wire encoding matches Kafka byte-for-byte.
- **KRaft** — Kafka's ZooKeeper-less consensus mode, used by every test cluster
  here.
