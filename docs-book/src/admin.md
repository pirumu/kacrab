# The admin client

The `admin` feature adds `kacrab::admin::AdminClient`, a native implementation of
Apache Kafka's `Admin` interface. It covers the **full Kafka 4.3.0 `Admin`
operation surface — 62 operations** — and is built on the same wire/session layer
(and therefore the same TLS/SASL auth) as the producer.

## Shape and conventions

The admin API follows the producer's conventions rather than Java's result-object
style:

- methods are `snake_case` mirrors of Java (`create_topics`, `describe_configs`,
  `list_consumer_group_offsets`, ...);
- they return plain `Result<T>` / `Result<()>` — no `.values()` / `.all()`
  result objects;
- each call takes a per-call options struct (`CreateTopicsOptions`, ...);
- the facade mirrors Java: `AdminClient::new` / `from_client_config` /
  `from_properties` / `from_map`, plus `ClientConfig::create_admin`.

Domain types Kafka places in `org.apache.kafka.common` — `TopicPartition`,
`OffsetAndMetadata`, `ConsumerGroupMetadata`, `Node` — live in the always-compiled
`kacrab::common` module and are re-exported by both `producer` and `admin`, so a
`TopicPartition` is the same type on either surface.

## Operation coverage

All 62 `Admin` operations are implemented, including topics/partitions,
describe/incremental-alter configs, ACLs, consumer groups & offsets,
`list_offsets`/`delete_records`/`elect_leaders`, producer/transaction inspection
(`describe_producers`, `describe`/`list`/`abort_transaction`, `fence_producers`),
partition reassignments, delegation tokens, client quotas, user SCRAM
credentials, log dirs, feature/`update_features`, `KRaft`
quorum/voters, the Kafka 4.x **share** and **streams** group families, and the
`client_instance_id`/`metrics` accessors. The one interface method kacrab keeps
that upstream 4.3.0 removed is the legacy `alter_configs` (its RPC still serves
older brokers).

## Routing model

Different operations reach the cluster differently, and the admin client encodes
each pattern once:

- **Controller-routed** (create/delete topics, partition & config mutations,
  ACLs, `KRaft` voters, `update_features`): sent to the metadata-reported
  controller, retrying against a freshly discovered controller on
  `NOT_CONTROLLER` (`route_to_controller`).
- **Coordinator-routed** (consumer/share/streams groups, group offsets,
  transactions): a `FindCoordinator` resolves the group/transaction coordinator,
  its endpoint is registered, and the request is sent there. Transient
  coordinator errors (`NOT_COORDINATOR`, `COORDINATOR_NOT_AVAILABLE`,
  `COORDINATOR_LOAD_IN_PROGRESS`) — from either `FindCoordinator` or the response
  — trigger a re-resolve-and-retry (`route_to_coordinator`), matching the Java
  client. This matters on a freshly started broker whose coordinator topics load
  lazily.
- **Per-leader** (`list_offsets`, `delete_records`, `describe_producers`): the
  target partitions are grouped by their current leader from metadata and one
  request is sent to each leader.
- **Any broker / broadcast** (`describe_cluster`, `list_consumer_groups`,
  `list_transactions`, config-resource listings): sent to one live broker, or
  fanned out to every broker and aggregated.

## Metrics

`AdminClient::metrics()` returns an `AdminMetricsSnapshot` — kacrab's native
analogue of Java's `Admin.metrics()`, a typed struct rather than a
`Map<MetricName, KafkaMetric>`. Every broker request funnels through one
`send_metered` helper, so the snapshot reflects all routing paths
(controller-routed, coordinator-routed, per-leader, and broadcast) uniformly:

- `request_total` / `request_error_total` — completed requests and how many
  failed at the wire/transport layer;
- `request_latency_avg_nanos` / `request_latency_max_nanos` — mean and peak
  request latency;
- `buffer_pool` — the shared wire `BufferPoolStats` (read/write buffer acquire /
  reuse / release counters) at snapshot time.

The counters live behind an `Arc`, so `AdminClient` clones report the same
aggregate.

## Wire-version pitfalls (found by real-broker verification)

Verifying against a real Apache Kafka 4.3.0 broker surfaced a few version-
sensitivity traps that the pure-unit tests could not:

- **`ApiVersions` client identity.** `describe_features` (which reuses
  `ApiVersions`) must send a non-empty `client_software_name`; v3+ brokers reject
  an empty one with `INVALID_REQUEST`.
- **Topic name vs. topic id.** `OffsetCommit`/`OffsetFetch` v10 dropped the topic
  *name* in favour of the topic *id*, and the strict codec refuses to serialize a
  stale name at v10. The admin client resolves topic ids from metadata and sends
  the name only when the id is unknown.
- **Optional-API version negotiation.** Client telemetry is optional, so a broker
  may advertise a lower max version for `GetTelemetrySubscriptions` than the
  client supports; `client_instance_id` uses the broker-negotiated version rather
  than the client maximum.

## Verification

Every operation's wire round-trip is exercised against a real broker
(`kacrab/tests/real_kafka_admin*.rs`, run with `--ignored`):

- `real_kafka_admin_smoke` — core ops over `docker-compose.kafka.yml`.
- `real_kafka_admin_extended` — ACLs, quotas, SCRAM, transactions, and the
  share/streams group families over `docker-compose.kafka-admin.yml` (a broker
  with `StandardAuthorizer` and the share/streams features enabled).
- `real_kafka_admin_token` — delegation tokens over SASL against
  `docker-compose.auth.yml`.

Operations that need cluster state the test cannot easily create (a live group
member, a hanging transaction) are asserted at the wire layer: a well-formed
broker error code proves the request encoded and the response decoded correctly,
even when the operation cannot semantically succeed.
