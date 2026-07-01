# The consumer client

The `consumer` feature adds `kacrab::consumer::Consumer`, a native implementation
of Apache Kafka's `Consumer`, built on the same wire/session layer (and therefore
the same TLS/SASL auth) as the producer and admin clients. Like the rest of
kacrab, "Java-compatible" means Kafka-protocol- and behaviour-compatible, not a
literal port.

It supports **manual partition assignment**, **topic and pattern subscription**,
and **both consumer-group protocols** — the classic client-side-assignment
protocol (eager `range`/`roundrobin`/`sticky` and incremental `cooperative-sticky`
assignors) and the KIP-848 server-side protocol (`group.protocol=consumer`) —
plus record fetching with incremental fetch sessions, offset commit/fetch, and
interceptors. The design of record and the phased plan live in
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

The consumer is single-owner and not `Sync`: `poll(Duration)` drives fetch and
rebalance I/O on the caller's task (the classic protocol adds a dedicated
background heartbeat task; the KIP-848 heartbeat runs from `poll`), mirroring the
Java consumer's user-thread model.

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
*name* — v13+ switches to topic ids under the strict codec. Fetches use
incremental fetch sessions (KIP-227): the first fetch to a leader is a full fetch
that opens a session, and later fetches send only the partitions whose position
changed (plus a forgotten list for ones no longer fetchable), so the broker
returns only partitions with new data. Behaviour is identical — it is a smaller
request. Positions are also validated with `OffsetForLeaderEpoch` after a leader
change and reset on truncation (KIP-320).

## Group subscription and rebalancing

`subscribe(topics)` (requires a `group.id`) joins the consumer group on the next
`poll`; `subscribe_pattern(regex)` subscribes to every matching topic, re-matched
against the live topic list (excluding internal topics per
`exclude.internal.topics`). Which protocol runs depends on `group.protocol`.

**Classic** (`group.protocol=classic`, the default) uses the client-side
protocol:

- `FindCoordinator` locates the group coordinator (retried with backoff while a
  freshly started broker loads `__consumer_offsets`);
- `JoinGroup` registers the member (transparently retrying the
  `MEMBER_ID_REQUIRED` round), and the elected leader runs the chosen assignor —
  `range`, `roundrobin`, `sticky`, or the incremental `cooperative-sticky` — over
  every member's subscription using partition counts from metadata;
- `SyncGroup` distributes the assignment; the consumer resumes each assigned
  partition from its committed offset before fetching;
- a dedicated background task heartbeats the group (poll-independent) and flags a
  rejoin on `REBALANCE_IN_PROGRESS` / `ILLEGAL_GENERATION` / `UNKNOWN_MEMBER_ID`;
- `close` sends a best-effort `LeaveGroup`.

Under `cooperative-sticky` the subscription reports each member's currently owned
partitions and the assignor withholds any partition still owned by another member
until it is revoked, so a partition is never owned by two members at once
(KIP-429). `partition.assignment.strategy` selects the assignor and defaults to
Java's `range,cooperative-sticky`. Static membership (`group.instance.id`) and
`enforce_rebalance` are supported.

**KIP-848** (`group.protocol=consumer`) uses the server-side protocol: the
consumer generates its own member id and drives membership with a single
`ConsumerGroupHeartbeat` RPC, reporting its subscribed topics and owned partitions
each heartbeat and reconciling toward the coordinator-computed (topic-id-keyed)
target assignment incrementally. Fencing rejoins from epoch 0; `close` sends a
leaving heartbeat.

## Offsets

For a consumer carrying a `group.id` (manual-assignment or subscribed):

- `commit_sync` commits the current position of every assigned partition;
  `commit_sync_offsets(map)` commits explicit offsets; `commit_async` commits
  without blocking, invoking a callback with the result. With
  `enable.auto.commit`, positions are committed on a background interval and
  before each rebalance. Offsets carry the leader epoch, and a group member's
  commits identify themselves with the member id + generation/epoch (required by
  the coordinator, and by KIP-848 in particular).
- `committed(partitions)` reads back committed offsets, omitting partitions with
  none.
- `beginning_offsets`/`end_offsets`/`offsets_for_times` query the log bounds and
  timestamp offsets; `current_lag` reports a partition's lag.
- `group_metadata()` returns the `ConsumerGroupMetadata`.

`OffsetCommit` is capped at v9 and `OffsetFetch` at v7 so both stay topic-*name*
keyed, sidestepping the v10/v8 topic-id strict-codec forms (the same trap the
admin offset paths handle).

## Deserializers, interceptors, and metrics

Records are bytes-first; `record.deserialized(key, value)` maps them through
typed `ConsumerDeserializer`s (`BytesDeserializer`, `ByteArrayDeserializer`,
`StringDeserializer`, or your own). `add_interceptor` registers a
`ConsumerInterceptor` whose `on_consume` can rewrite or filter each poll's records
before they reach the caller and whose `on_commit` observes committed offsets;
the chain is panic-isolated. `metrics()` returns a typed snapshot
(poll/records/fetch/commit/heartbeat/rebalance totals plus the wire buffer pool),
and `client_instance_id()` returns the broker-assigned telemetry id.

## Verification

The consumer is exercised end-to-end against a real Apache Kafka 4.3.0 broker
(`kacrab/tests/real_kafka_consumer.rs`, run with `--ignored`):

- manual assignment: consume records a kacrab producer wrote, then `commit_sync`
  and read the committed offset back;
- subscription: one subscriber owns both partitions of a two-partition topic and
  consumes every record;
- rebalance: two consumers in one group split to one partition each (driven from
  independent tasks, like real consumers) and together consume everything;
- cooperative-sticky: two consumers negotiate the incremental protocol and
  converge to a clean split with no double-owned partition;
- assignors and offsets: the `roundrobin` assignor, the offset-query APIs, and
  auto/async commit;
- pattern subscription: a regex joins exactly the matching topics;
- interceptors: `on_consume`/`on_commit` observe every record and commit;
- KIP-848: a `group.protocol=consumer` subscriber joins via
  `ConsumerGroupHeartbeat`, is assigned both partitions, consumes, and commits.

Truncation detection (KIP-320) and the fetch-session state machine are covered by
unit tests, since a real truncation / leader change cannot be staged on the
single-broker compose.

A latent connection bug surfaced during verification: a coordinator advertised as
`localhost` resolving to a dead IPv6 loopback made a pinned connection hang. The
fix centralized DNS in the wire layer — brokers are re-resolved on connect,
IPv4-first, honouring `client.dns.lookup=use_all_dns_ips` — so the producer,
admin, and consumer all benefit.
