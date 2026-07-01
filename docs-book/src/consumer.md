# The consumer client

The `consumer` feature adds `kacrab::consumer::Consumer`, a native implementation
of Apache Kafka's `Consumer`, built on the same wire/session layer (and therefore
the same TLS/SASL auth) as the producer and admin clients. Like the rest of
kacrab, "Java-compatible" means Kafka-protocol- and behaviour-compatible, not a
literal port.

It supports both **manual partition assignment** and **classic consumer-group
subscription** (eager rebalancing with the `range` assignor), record fetching,
and offset commit/fetch. The design of record and the phased plan live in
`docs/consumer-design.md`.

## Shape and conventions

The consumer follows the producer/admin conventions rather than Java's:

- methods are `snake_case` mirrors of Java (`subscribe`, `assign`, `poll`,
  `commit_sync`, `seek`, `position`, `pause`, …);
- construction mirrors the other clients: `Consumer::new` / `from_client_config`
  / `from_properties` / `from_map`;
- keys and values are raw bytes (`ConsumerRecord.key/value: Option<Bytes>`),
  matching the producer's bytes-first `ProducerRecord`; a typed deserializer
  layer rides on top rather than threading generics through the fetch pipeline;
- domain types (`TopicPartition`, `OffsetAndMetadata`, `ConsumerGroupMetadata`)
  come from the shared `kacrab::common` module, so they are the same types the
  producer and admin use.

The consumer is single-owner and not `Sync`: `poll(Duration)` drives all fetch
and coordination I/O on the caller's task — including poll-throttled heartbeats —
mirroring the Java consumer's user-thread model.

## Manual assignment and fetching

`assign(partitions)` takes direct control of a partition set (no group). `poll`
then, each round:

1. refreshes metadata for the assigned topics;
2. resolves an initial position for any partition without one via
   `auto.offset.reset` (`earliest`/`latest` through `ListOffsets`, or a
   `NoOffsetForPartition` error under `none`);
3. issues one `Fetch` per partition leader and decodes the returned record
   batches into `ConsumerRecord`s, advancing each partition's position and
   respecting `max.poll.records`.

`seek`/`seek_to_beginning`/`seek_to_end`, `position`, and `pause`/`resume`/
`paused` give the usual position control; `wakeup` interrupts a blocking `poll`.

`Fetch` is negotiated no higher than v12 so partitions stay keyed by topic
*name* — v13+ switches to topic ids under the strict codec, and fetch sessions
(KIP-227) are a later optimization.

## Group subscription and rebalancing

`subscribe(topics)` (requires a `group.id`) joins the classic consumer group on
the next `poll`. The membership flow is the eager Kafka protocol:

- `FindCoordinator` locates the group coordinator (retried with backoff while a
  freshly started broker loads `__consumer_offsets`);
- `JoinGroup` registers the member (transparently retrying the
  `MEMBER_ID_REQUIRED` round), and the elected leader runs the `range` assignor
  over every member's subscription using partition counts from metadata;
- `SyncGroup` distributes the assignment; the consumer then resumes each assigned
  partition from its committed offset before fetching;
- a poll-throttled `Heartbeat` keeps the member alive and triggers a rejoin on
  `REBALANCE_IN_PROGRESS` / `ILLEGAL_GENERATION` / `UNKNOWN_MEMBER_ID`;
- `close` sends a best-effort `LeaveGroup`.

The subscription/assignment blobs use the version-prefixed `ConsumerProtocol`
encoding the broker relays between members. A dedicated background heartbeat task
and the cooperative-sticky assignor are later refinements.

## Offsets

For a consumer carrying a `group.id` (manual-assignment or subscribed):

- `commit_sync` commits the current position of every assigned partition;
  `commit_sync_offsets(map)` commits explicit offsets. Offsets carry the leader
  epoch.
- `committed(partitions)` reads back committed offsets, omitting partitions with
  none.
- `group_metadata()` returns the `ConsumerGroupMetadata`.

`OffsetCommit` is capped at v9 and `OffsetFetch` at v7 so both stay topic-*name*
keyed, sidestepping the v10/v8 topic-id strict-codec forms (the same trap the
admin offset paths handle).

## Verification

The consumer is exercised end-to-end against a real Apache Kafka 4.3.0 broker
(`kacrab/tests/real_kafka_consumer.rs`, run with `--ignored`):

- manual assignment: consume records a kacrab producer wrote, then `commit_sync`
  and read the committed offset back;
- subscription: one subscriber owns both partitions of a two-partition topic and
  consumes every record;
- rebalance: two consumers in one group split to one partition each (driven from
  independent tasks, like real consumers) and together consume everything.

A latent connection bug surfaced during verification: a coordinator advertised as
`localhost` resolving to a dead IPv6 loopback made a pinned connection hang. The
fix centralized DNS in the wire layer — brokers are re-resolved on connect,
IPv4-first, honouring `client.dns.lookup=use_all_dns_ips` — so the producer,
admin, and consumer all benefit.
