# Changelog

All notable changes to this project should be documented in this file.

This project is pre-release and has not published a stable release yet.

The format is based on human-readable release notes. Once releases begin, each
entry should include the release date and links to relevant pull requests or
issues.

## Unreleased

### Added

- Consumer client (`consumer` feature): `kacrab::consumer::Consumer` with manual
  partition assignment and classic consumer-group subscription. Fetch with
  `auto.offset.reset`, `max.poll.records`, and `seek`/`seek_to_beginning`/
  `seek_to_end`/`position`/`pause`/`resume`/`wakeup`; `FindCoordinator` +
  `JoinGroup`/`SyncGroup`/`Heartbeat`/`LeaveGroup` with the `range` assignor and
  eager rebalancing; `commit_sync`/`commit_sync_offsets`/`committed`/
  `group_metadata` (leader-epoch aware). Bytes-first records
  (`ConsumerRecord.key/value: Option<Bytes>`). Verified end-to-end against a real
  Apache Kafka 4.3.0 broker (manual assign + commit, a single subscriber, and two
  consumers rebalancing a topic).
- Consumer group parity: the `roundrobin`, `sticky`, and incremental
  `cooperative-sticky` assignors (`partition.assignment.strategy`, default aligned
  to Java's `range,cooperative-sticky`); the KIP-848 server-side protocol
  (`group.protocol=consumer`, a single `ConsumerGroupHeartbeat` RPC with
  server-computed, topic-id-keyed assignments reconciled incrementally); a
  dedicated background heartbeat task; static membership (`group.instance.id`);
  and `enforce_rebalance`.
- Consumer offset and fetch parity: offset queries
  (`beginning_offsets`/`end_offsets`/`offsets_for_times`/`current_lag`),
  `commit_async` with background auto-commit, incremental fetch sessions
  (KIP-227), and OffsetForLeaderEpoch position validation / truncation detection
  (KIP-320).
- Consumer surface parity: topic pattern subscription (`subscribe_pattern`, regex,
  honouring `exclude.internal.topics`), typed `ConsumerDeserializer`s
  (bytes/byte-array/string), `ConsumerInterceptor`s (`on_consume`/`on_commit`),
  `client_instance_id`, and `metrics()`. All verified end-to-end across ten
  scenarios against a real Apache Kafka 4.3.0 broker (including cooperative-sticky,
  pattern, interceptors, and KIP-848).
- Config drift guard (`kacrab/tests/config_drift.rs`) cross-checking the typed
  `config/clients.rs` against the generated `config/catalog.rs`, so a Kafka
  version bump is regenerate-and-reconcile.
- `client.dns.lookup` is now honoured: broker hostnames are resolved on connect
  and every resolved address is tried under `use_all_dns_ips`.
- Consumer chapters in the book (overview, fetching, rebalancing).

### Changed

- Broker DNS resolution moved into the wire layer (IPv4-first, multi-address
  fallback), replacing per-client address selection in the producer and consumer
  coordinator lookups.
- The three per-client `to_connection_config` methods now share one
  `connection_config_fields!` macro (~115 fewer lines), so a wire connection
  config is added in one place.

### Fixed

- The config-metadata generator now extracts `ConfigDef.define(...)` calls that
  Kafka breaks across lines (`).\n define(`), so `bootstrap.controllers` is
  cataloged.
- A group coordinator advertised as `localhost` resolving to an unreachable IPv6
  loopback no longer hangs the connection (see the wire DNS change above).

### Security

- Nothing yet.
