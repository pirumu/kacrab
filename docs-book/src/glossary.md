# Glossary

Terms coined or leaned on along the journey.

## Producer side

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
- **Sticky partitioning** — batching-friendly null-key partitioning: stick to one
  partition until the batch fills, then rotate (adaptively weighted by queue
  depth).

## Consumer side

- **Position** — a partition's monotonic fetch cursor; advances only past
  records actually delivered to the caller.
- **Committed offset** — the group's durable "read up to here" mark at the
  coordinator; where a member resumes after restart or rebalance.
- **Coordinator** — the broker owning a group's slice of `__consumer_offsets`;
  target of joins, heartbeats, and commits. It can move; clients must
  re-discover it.
- **Assignor** — the algorithm dividing partitions among group members
  (`range`, `roundrobin`, `sticky`, `cooperative-sticky`).
- **Cooperative rebalance (KIP-429)** — the two-round incremental handoff that
  moves only the partitions that change hands, never double-owning one.
- **Member epoch (KIP-848)** — the server-side protocol's generation counter,
  advanced by the coordinator on each assignment change.
- **Fetch session (KIP-227)** — per-broker state letting incremental fetches
  send only changed partitions instead of the full set.
- **Static membership** — a `group.instance.id` identity that survives
  restarts, so a quick bounce triggers no rebalance.

## Wire & protocol

- **Record batch v2** — the Kafka record-batch format (magic byte 2) that wraps
  compressed records with a CRC32C over the compressed payload.
- **Topic id (KIP-516)** — the UUID that keys newer Fetch versions in place of
  the topic name.
- **Oracle matrix** — the Java-client-checked test fixtures that prove Rust's
  generated wire encoding matches Kafka byte-for-byte.
- **KRaft** — Kafka's ZooKeeper-less consensus mode, used by every test cluster
  here.
