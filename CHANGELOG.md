# Changelog

All notable changes to this project should be documented in this file.

This project is pre-1.0; minor releases may still change public APIs.

The format is based on human-readable release notes. Each entry includes the
release date and links to relevant pull requests or issues.

## Unreleased

### Changed — breaking

- **The TLS crypto provider is now an explicit feature.** `rustls` no longer comes
  with a backend baked in. Pick one: `aws-lc-rs-tls` reproduces the previous
  behaviour, and `pure-rust-tls` puts `rustls` on `ring`, which drops `aws-lc-sys`
  — the large C/assembly dependency — from the tree entirely.
  `make check-pure-rust-tls` asserts the dependency is gone in CI rather than
  trusting that the feature compiles.

  If you use `SSL` or `SASL_SSL`, add `aws-lc-rs-tls` to your feature list. A build
  with neither now compiles no crypto backend at all, and a TLS connection returns
  `WireError::InvalidTlsConfig` naming the two features rather than failing at link
  time — a `PLAINTEXT`-only deployment should not pay for a provider it never uses.

  **One capability differs between the two.** Signing an `OAUTHBEARER` JWT
  assertion locally (`sasl.oauthbearer.assertion.private.key.file`) requires
  `aws-lc-rs-tls`; under `pure-rust-tls` it returns
  `WireError::InvalidSaslConfig` naming the feature. `jsonwebtoken` is now an
  optional dependency that only `aws-lc-rs-tls` pulls in, because its pure-Rust
  backend depends on `rsa` and RUSTSEC-2023-0071 — a key-recovery timing
  sidechannel with no fixed release. Trading a C dependency for an unpatched
  sidechannel is not what that feature is for, so `make check-pure-rust-tls` bans
  `rsa` alongside `aws-lc-sys`. The other three `OAUTHBEARER` token sources — JAAS
  option, token file, and HTTP token endpoint — are unaffected in both builds.

  Enabling both features is well defined: kacrab installs `aws-lc-rs` as the process
  default before first use, so `--all-features` builds no longer hit `rustls`'s
  "could not automatically determine the process-level `CryptoProvider`" panic. An
  application that installs its own provider first keeps it.

- **`Producer::flush`, `commit_transaction`, and `abort_transaction` take `&self`.**
  They previously took `&mut self` while `send` and `begin_transaction` took
  `&self`, so an `Arc<Producer>` — the natural translation of Java's one-producer-
  per-application model — could open a transaction but not commit it, and could not
  flush at all. The exclusive borrow was never required: nothing in the flush chain
  mutates a field, and the internals were already interior-mutable.

  `TypedProducer::send` moves from `&mut self` to `&self` for the same reason, and
  `close`/`close_timeout` no longer bind `mut self`. Existing callers keep compiling;
  `let mut producer` bindings simply become redundant. See the new
  [Sharing a producer](README.md#sharing-a-producer) section for what still needs
  exclusive access (`set_partitioner`, interceptors, metric hooks) and what needs
  ownership (`close`).

- **Benchmark diagnostics moved behind the internal `__bench` feature.**
  `SYNC_NOW_BUFFER_SPINS`, `Producer::enable_dispatch_latency_metrics`, and
  `Producer::take_dispatch_latency_samples` were reachable on the stable public API
  despite being instruments for measuring kacrab, not for using it. They are now
  `#[doc(hidden)]` behind `__bench`, off by default and exempt from semver — the
  same treatment `__fuzzing` already gets. `Producer::from_parts` is likewise
  `#[doc(hidden)]`, matching the visibility of the `ProducerRuntimeConfig` it takes.

  These sample a *dispatch-group* clock and were never the source of the published
  latency tables, which use the per-record `send_with_callback` clock described in
  [`benches/README.md`](benches/README.md). Their docs now say so, and the stale
  reference to a non-existent `send_now` method is gone.

### Added

- **Share consumer (KIP-932), behind the new `share-consumer` feature.**
  `ShareConsumer` joins a share group, acquires records under the broker's
  acquisition lock, and acknowledges them per record instead of committing
  offsets — the queue-style surface consumer groups serve badly. It closes the
  asymmetry where kacrab could already *administer* share groups
  (`describe_share_groups` and friends) but not consume from them, which forced a
  second client just to produce the traffic the admin surface managed.

  The three client-facing RPCs were already generated and are now wired:
  `ShareGroupHeartbeat` (v1) for membership and assignment, `ShareFetch` (v1–2)
  for acquire-and-fetch with piggy-backed acknowledgements, and
  `ShareAcknowledge` (v1–2) for standalone acknowledgement and share-session
  close. The share-coordinator state RPCs stay unwired; a client never issues
  them.

  The surface matches Java's `ShareConsumer` method for method:
  `accept`/`acknowledge`/`acknowledge_offset`, `commit`/`commit_timeout`/
  `commit_async`, `set_acknowledgement_commit_callback`,
  `acquisition_lock_timeout` (the broker's per-response lock budget, which is
  what tells an application how long it has before a batch becomes
  re-deliverable), `client_instance_id`, `metrics`, `close`/`close_timeout`, and
  `wakeup`. `commit_async` deliberately sends nothing on its own — a share
  session is a strictly ordered epoch sequence per broker, so a second request
  racing the poll loop would invalidate the session; Java behaves the same way,
  piggy-backing the acknowledgements onto the next `ShareFetch`.

  `AcknowledgeType::{Accept, Release, Reject, Renew}` disposes of each record,
  `ShareRecord::delivery_count` carries the KIP-932 delivery attempt count so
  poison messages can be rejected rather than retried forever, and both
  acknowledgement modes work: `implicit` (the Kafka default) accepts the batch on
  the next `poll`/`commit`, `explicit` requires every delivered record to be
  acknowledged and errors otherwise. Acknowledgements are batched into the next
  `ShareFetch`, so there is no round trip per record. Cancellation and drop
  semantics are documented in the README table: a dropped `poll` or a dropped
  consumer leaves acquired records to their acquisition lock, which redelivers
  them rather than losing them.

  `share.acknowledgement.mode` and `share.acquire.mode` are promoted from
  `ConfigStatus::NativeReview` to `Native` in the generated config catalog; they
  are now typed fields on `ConsumerConfig`. `share.acquire.mode=record_limit`
  needs `ShareFetch` v2 and is inert against a v1-only broker (`max.poll.records`
  still bounds acquisition through the request's `max_records` field).

  Verified against a real Apache Kafka 4.3.0 broker in
  `kacrab/tests/real_kafka_share_consumer.rs`, run by the `real-broker` workflow:
  both acknowledgement modes, all three dispositions (a `Release`d record comes
  back, a `Reject`ed one does not), delivery counts climbing to the broker's
  archive limit, three consumers on a one-partition topic consuming every record
  exactly once, acquisition-lock expiry redelivering an abandoned consumer's
  records, and admin interop against the live group.

### Fixed

- **`FindCoordinator` was always sent in its v4+ form, so every broker older than
  3.0 failed coordinator discovery.** The consumer, producer, and admin clients all
  filled `coordinator_keys` — the batched array KIP-699 added in v4 — while the
  version each request is actually sent at is negotiated per broker from its
  `ApiVersions` (the version passed by a call site is only a ceiling). A broker that
  negotiated v3 or lower got a request the encoder refused
  (`UnsupportedFieldVersion { field: "coordinator_keys" }`), so group membership,
  offset commits, and every transaction died before the first RPC — that alone
  pinned kacrab's real broker floor at 3.0.

  The request is now rewritten into the form the negotiated version speaks at the
  encode seam, where that version is known: the singular `key` up to v3 and
  `coordinator_keys` from v4 (mirroring Java's `FindCoordinatorRequest.Builder`), a
  batched lookup below v4 still being refused rather than silently losing keys. Both
  response shapes are read too — the flat top-level `node_id`/`host`/`port` of v0-3
  as well as the v4+ `coordinators` array.

  This is one instance of a class — a field that only exists from some version being
  filled before the negotiated version is known — and every request-build site was
  audited against the generated per-version encoders for the same mistake. The two
  further instances it turned up are fixed below; the rest either set the field only
  where Kafka's own client also raises `UnsupportedVersionException`, or are already
  version-aware.

- **`list_consumer_groups` reported share, streams, and connect groups as consumer
  groups.** The broker's `ListGroups` response carries every group it coordinates
  whatever its protocol, and Java's `listConsumerGroups` filters that response down
  to the consumer protocol (`KafkaAdminClient.maybeAddConsumerGroup` keeps a group
  only when its protocol type is `consumer` or empty, the latter being a simple
  consumer group). kacrab kept all of them, so a cluster running KIP-932 share
  groups or Kafka Streams saw those groups listed as consumer groups — and then
  failed when they were fed back into a consumer-group operation.

  `list_consumer_groups` now applies the same filter. `list_groups`, the deliberate
  "any group type" listing, is unchanged and still returns everything; the two now
  share one broker fan-out instead of holding byte-identical copies of it.

- **A leadership error on one partition dropped the whole cluster metadata cache.**
  `WireClient::invalidate_topic_partition` documented partition-scoped
  invalidation but discarded the entire `ClusterMetadata` snapshot, so a single
  `NOT_LEADER_OR_FOLLOWER` — or any producer requeue or consumer fetch recovery
  that calls it — forced every unrelated topic to refetch its metadata from the
  broker as well. On a client fanning out across many topics, one flapping
  partition turned into a full-cluster metadata storm.

  The metadata manager now records the failing partitions per topic and misses the
  cache only for the topic that owns them; every other topic keeps being served
  from the snapshot until its own `metadata.max.age.ms` / `metadata.max.idle.ms`
  expiry. The refetch itself stays topic-scoped because Kafka's `Metadata` request
  is topic-keyed and cannot ask for a single partition — but the partition keys are
  not decoration: a produce response that carries the new leader for a partition
  (`apply_partition_leader_update`) now retires that partition's invalidation in
  place, and the topic returns to the cache without a metadata round trip at all.

- **A produce response could bind receipts to the wrong topic.** Matching a
  response to its route accepted topic-id equality unconditionally, but a broker
  that reports no topic ids leaves both sides at `KafkaUuid::ZERO` — so the first
  topic in a multi-topic produce response matched *every* route and every receipt
  took its offsets. The id disjunct now only counts when the id is non-zero,
  leaving the topic name to disambiguate as it always did on older brokers.

- **The SCRAM digest could silently degrade to an empty hash.** `digest_bytes`
  produces the `stored_key` behind every client proof, but its catch-all arm
  returned `Vec::new()` where its sibling `hmac_bytes` returns
  `WireError::UnsupportedSaslMechanism`. A mechanism it cannot hash therefore
  yielded a well-formed but wrong proof, which a broker can only report as bad
  credentials. It is now fallible in exactly the same way.

- **kacrab did not compile for Android or any other unhandled unix target.** The
  hand-rolled `EINPROGRESS` table in `wire::socket` covered only the Apple/BSD
  and Linux arms, so `cargo check --target aarch64-linux-android` — a target the
  same file already handles for its post-connect socket options — failed with
  `cannot find value EINPROGRESS in module libc_errno`. The table now covers the
  Apple, BSD, and Linux lineages explicitly and falls through to `None` for
  anything else, which leaves connect-in-progress detection on its `ErrorKind`
  check rather than comparing against an errno that means something else there.
  The unit test asserts the target being built for is covered, so a future gap
  fails the test suite instead of the build.

- **Typed producer builders never configured their registered interceptors.**
  `ProducerBuilder::build_with_serializers` and
  `build_with_configured_serializers` were drifted copies of `build`'s pipeline
  that skipped the `ProducerInterceptor::configure` pass, so an interceptor
  registered on a typed producer was never handed its `client.id` — contradicting
  that method's own contract — and left `interceptor_configs` at its default, so a
  later `producer_mut().add_interceptor(...)` configured with `client_id: None`
  too. All three builders now share one pipeline, which parameterizes the only
  real difference between them: how `key.serializer` / `value.serializer` class
  configs are stripped.

- **The gate-label support map is now generated, not hand-maintained.** The
  feature-aware validation below originally shipped with a hand-written
  label→feature map in `kacrab/src/config.rs`, one more place config knowledge
  could drift. `kacrab-codegen` now emits `gate_label_supported` into the
  generated catalog from the same `GATE_LABEL_FEATURES` table that mints the
  labels in `classify_status`, and catalog generation fails with
  `UnmappedGateLabel` on any label missing from that table — so a new gate
  label cannot reach the runtime as silently-unsupported.

- **`UnknownKeyPolicy::Report` could never actually return a report.** The
  `kafka_config!`-generated `from_properties` ran an unconditional
  "every key must have a typed field" loop *after* `validate_properties`, with no
  branch on the policy. Lenient parsing therefore collected its warnings and then
  hard-errored with `ConfigError::UnsupportedKey` on the first key without a typed
  field — so `ClientConfig::producer_config_with_warnings(Report)` and the whole
  `WarningReport` plumbing behind it were dead code, and lenient mode was in
  practice stricter than strict mode's own contract.

  The loop now branches: `Deny` errors exactly as before, and `Report` records a
  `push_unsupported_key` warning and keeps parsing the typed keys. Keys absent from
  the catalog are left alone there — `validate_properties` has already warned them
  as unknown, and warning them twice would be its own regression.

- **Strict property validation silently accepted feature-gated security keys.**
  `validate_properties` matched `UnknownKeyPolicy` with the arms inverted for
  `ConfigStatus::FeatureGated`/`Future`: lenient (`Report`) mode returned
  `ConfigError::UnsupportedFeature`, while strict (`Deny`) mode — the mode
  `ClientConfig::producer_config` and friends use — accepted the key and dropped
  it. Supplying `ssl.truststore.location` to a build with no TLS provider
  therefore produced a config that connected without the trust material the
  caller asked for, and the stricter setting was the one that failed open.

  Gate handling is now feature-aware and policy-independent. A gated key whose
  backing feature is not compiled is an error in *both* modes; a gated key that
  is backed by compiled code is accepted with no warning, because it has a typed
  field and parses downstream. The catalog's gate labels are metadata rather than
  cargo feature names — `tls-rustls` predates the
  `aws-lc-rs-tls`/`pure-rust-tls` split and names no feature at all — so they are
  mapped explicitly, and an unrecognised label fails closed.

- **Two pipelined idempotent retries to one partition could re-send out of
  sequence order.** ([#2]) After a broker disconnect, each in-flight dispatch
  retried inside its own task, and an in-task retry re-enqueues with an
  enqueue-sequencer ticket that has already been served — so nothing ordered
  the re-sends, and under CPU saturation the base-sequence-1 request could
  reach the broker before the base-sequence-0 re-send. A real broker answers
  `OUT_OF_ORDER_SEQUENCE_NUMBER` and the retry path recovers, so no records
  were lost or reordered on the broker; the cost was a wasted round trip and a
  flaky ordering test. First dispatches were never affected: sequence stamping
  and ticket reservation are serialized, so first-attempt wire order was
  already correct.

  In-task retries now apply the same gate the drain path already had (Kafka
  `shouldStopDrainBatchesForPartition`'s retry clause): a batch that no longer
  holds its partition's first in-flight sequence is handed back to the
  accumulator, whose sequence-ordered queue re-admits it in order. The gate
  runs only on retry iterations — `producer_dispatcher/multi_broker_dispatch`
  throughput is unchanged (61.41 Kelem/s with vs 61.22 Kelem/s without, within
  noise), and the issue's reproducer went from 1 failure in 60 runs to 0 in
  180 under 3x-core CPU load.

[#2]: https://github.com/pirumu/kacrab/issues/2

- **Single-feature builds were broken, and nothing was checking them.**
  `--features consumer` failed to compile: `wire::{BackoffPolicy, BackoffState}`
  were re-exported only under `cfg(feature = "producer")` while
  `consumer/coordinator.rs` uses them for the `FindCoordinator` retry. Sweeping
  for the same class found two more, both in unit tests that named producer-only
  items without carrying the gate the code does
  (`wire/metadata/manager.rs`'s `apply_partition_leader_update` test and
  `wire/broker.rs`'s `BrokerHandle` literal). Every gate in the repo runs
  `--all-features`, the one configuration no user has, so none of it was visible.

  Fixed, and gated: `make check-features` builds all 17 feature selections a user
  can make — each surface alone, the documented pairs, and none at all — and runs
  as its own CI job.
- **The single-broker `docker-compose.kafka.yml` fixture could not run share
  groups.** The share coordinator auto-creates its internal
  `__share_group_state` topic at replication factor 3, which fails with
  `INVALID_REPLICATION_FACTOR` on a one-broker cluster; every share-partition
  initialization then timed out and a share consumer acquired nothing, with no
  client-visible error. Scaled to the fixture like the offsets and transaction
  logs.

- **A server could pin a client CPU through the SCRAM iteration count, and could
  silently weaken key derivation.** `ScramServerFirst::parse` accepted any
  non-zero `i=` value from the server-first message and handed it straight to
  `salted_password`, which runs one HMAC per iteration. The count is not data the
  client stores, it is work the client performs — and it arrives *before* the
  server is authenticated, since SCRAM proves the server only at server-final. A
  reply of `i=4294967295` therefore pins a core for minutes per connection, from
  anyone who can answer on the broker's address. Separately, accepting counts
  below 4096 let a server downgrade the derivation to as little as one iteration;
  Java's client rejects those (`ScramSaslClient.java:127` against
  `ScramMechanism.minIterations`), so this was also a parity gap.

  The accepted range is now `[4096, 1_000_000]`. The minimum is Kafka's own
  `minIterations`. The maximum is deliberately *not* Kafka's declared
  `maxIterations` of 16384: that ceiling is only applied by the
  `kafka-storage add-scram` tool (`ScramParser.java:189`), while the controller's
  `AlterUserScramCredentials` path checks the minimum alone
  (`ScramControlManager.java:290`), so a legitimately provisioned credential can
  exceed it and enforcing 16384 would break real deployments. 1,000,000 is 244x
  Kafka's default and 61x its tooling ceiling, so no plausible configuration is
  affected, while the work a hostile server can demand stays bounded.
- **A hostile record count could preallocate ~120 MB from a 49-byte batch.**
  `RecordBatch::decode` sized its record `Vec` from the wire's `recordCount`,
  bounded only by `MAX_RECORDS_PER_BATCH` (1,000,000) — which caps the count but
  still allows a batch of a few dozen bytes to demand hundreds of megabytes. Now
  clamped by what the payload can actually hold, the same guard `Record::decode`
  received for its header count. No behaviour change for valid input.
- **A hostile record header count could OOM the client.** `Record::decode` sized
  its header `Vec` straight from the wire's `headerCount` varint, checking only
  that it was non-negative. A 90-byte record declaring ~486M headers reached
  `malloc(7.8 GB)` before the decode loop read a single header and failed, so a
  corrupt or malicious broker response could OOM-kill any kacrab client with a
  handful of bytes — the batch level had guarded its own count with
  `MAX_RECORDS_PER_BATCH` for exactly this reason, but the per-record path had
  no equivalent. The speculative allocation is now clamped by what the remaining
  buffer can hold (a header is at least two varints), which cannot reject a
  satisfiable count. Found by the new `record_batch_framed` fuzz target; covered
  by `absurd_header_count_fails_without_a_giant_allocation`.
- `Consumer::poll` is now cancel-safe with respect to records. `reap_fetch` moved
  the in-flight `Fetch` handle out of the consumer before awaiting it, so
  dropping a `poll` future mid-await — the ordinary fate of the losing arm of a
  `tokio::select!` — detached the task and discarded whatever it had fetched,
  along with the partition positions it carried and the KIP-227 incremental
  fetch sessions, forcing the next fetch to re-open full sessions. The handle is
  now joined through `&mut` and stays owned by the consumer, so a cancelled poll
  costs nothing and the next poll folds the fetch in. Covered by
  `reap_fetch_survives_a_cancelled_await`.

### Added

- `cargo-fuzz` targets for the decoders that parse untrusted broker bytes:
  `record_batch_decode`, `record_batch_framed`, `response_decode`, and
  `decompress`. They run nightly in CI at 15 minutes per target and as a
  60-second smoke on any PR touching `kacrab-protocol/`. The fuzz crate lives
  outside the workspace because cargo-fuzz needs nightly plus a sanitizer.
  `record_batch_framed` exists because raw-byte fuzzing of a record batch is
  nearly useless on its own: CRC32C is validated before the magic byte, the
  record count, the varints, or the compressed blob, so random input passes that
  gate with probability 2^-32 and never reaches the decoder. Building correct
  framing around fuzzer-controlled bytes is what surfaced the OOM above.
- `consumer_protocol_metadata` fuzz target over `ConsumerProtocolSubscription`
  and `ConsumerProtocolAssignment` — the only decoders in the crate fed by
  another *client* rather than by the broker or the operator. The group leader
  decodes every member's subscription, and every follower plus any admin client
  decodes the assignment, so anyone authorised to join a group reaches them.
  `response_decode` could not: they travel as opaque `Bytes` inside `JoinGroup`
  and `SyncGroup` responses with their own version prefix, which both call sites
  read off the wire and pass to `read` unvalidated.
- `frame_decode` and `oauth_http_response` fuzz targets, closing the two
  remaining untrusted-input parsers. `frame_decode` covers the length-prefixed
  response frame — the first thing that touches socket bytes, ahead of every
  decoder — and confirms what inspection suggested: negative lengths rejected,
  `MAX_FRAME_LENGTH` enforced, truncation checked, and the split zero-copy rather
  than a speculative allocation. `oauth_http_response` covers the hand-written
  HTTP parser behind `sasl.oauthbearer.token.endpoint.url`, which splits headers
  on `\r\n\r\n` and takes the status by whitespace position before the body
  reaches `serde_json`. Both clean; no defects found.
- Four fuzz targets over the SASL handshake — `scram_server_first`,
  `scram_server_first_nonced`, `scram_server_final`, and `jaas_option` — reaching
  the parsers that run against a peer which has not authenticated yet. They reach
  crate-private code through a new internal `__fuzzing` feature on `kacrab` that
  exposes thin `fn(&[u8])` shims; it is `#[doc(hidden)]`, off by default, and
  exempt from semver. `scram_server_first` needs two targets for the same reason
  record batches do: the client-nonce check is unguessable, so raw bytes stall at
  199 edges while the nonce-satisfying variant reaches 742 and made the iteration
  count above reproducible.
- A committed seed corpus (`fuzz/seeds/`) and a Kafka wire-format dictionary
  (`fuzz/kafka.dict`) for the fuzz targets. The seeds are generated from the same
  fixtures the Java oracle matrix uses — every generated message, at every schema
  version, across six fixture shapes — by an ignored `generate_fuzz_corpus` test
  in `java_interop.rs`, then minimised with `cargo fuzz cmin`. The dictionary
  carries the sentinels random mutation will not find, chiefly Kafka's `-1`
  length prefix, which gates every nullable field. Together they take
  `record_batch_decode` from 150 to 984 edges and `record_batch_framed` from 774
  to 1591; `response_decode` reaches 11899. `response_decode` also now dispatches
  on the real API key byte and covers 50 client-facing response types rather than
  12 behind an arbitrary index.

### Changed

- The fuzz workflow now sets `-max_len`, `-malloc_limit_mb`, and `-timeout`
  explicitly per target, caches the working corpus between nightly runs so
  exploration compounds instead of restarting from the committed seeds, and
  minimises it with `cargo fuzz cmin` before saving. Leaving `-max_len` unset had
  been silently capping targets at the size of their largest seed — 12 bytes for
  `frame_decode`, which made committing seeds actively harmful. `response_decode`
  gained API key 71 (`GetTelemetrySubscriptions`), which was missing from its
  dispatch. The `frame_decode` guard asserted a frame *count* rather than a
  shrinking buffer, so 400,004 zero bytes tripped it against a correctly
  behaving decoder; it now asserts the real invariant. `generate_fuzz_corpus`
  clears the directories it owns so regeneration replaces rather than
  accumulates.

### Documentation

- New README sections: a verified comparison against `rust-rdkafka`, `rskafka`,
  and `kafka-rust`; "Cancellation & drop semantics" documenting the cancel-safety
  of every public future and what dropping a client without `close()` does; and
  "When not to use kacrab".
- Corrected the Highlights latency claim from "lower at every percentile" to
  "lower or tied", matching the matched-load table and the Caveats section, and
  documented why the latency *average* differs while p50/p95/p99 tie (both sides
  quantize to integer milliseconds; the average is the fraction above 0 ms).
- Replaced the coverage footnote's "(streams)" with the real breakdown of the 26
  unwired generated APIs, and synced the design book to `0.3.0` and the current
  benchmark figures.

## 0.3.0 — 2026-07-27

Producer batch-split release. A topic whose `max.message.bytes` sits below the
producer's `batch.size` could not deliver anything at all; fixing that exposed
four further defects in the split and idempotent-sequence paths that the default
scenarios never reach. The split path now delivers every record with no retries
and no errors, and is **2.2x Java's throughput** on the same workload. Includes
one breaking metrics API change (`ProducerMetricsSnapshot` is `#[non_exhaustive]`),
so this is a minor version bump under the pre-1.0 convention.
([#52](https://github.com/pirumu/kacrab/pull/52))

### Added

- `ProducerMetricsSnapshot::record_batch_split_count` — record-batch splits
  forced by a broker `MESSAGE_TOO_LARGE` response, counted one per split event
  the way Java's `Sender.completeBatch` does. The producer parity bench prints
  it as the `batch_splits` column, so that column is now a real both-sides
  comparison against `kafka-producer-perf-test.sh --print-metrics`.
- `ProducerMetricsSnapshot::delta_since` — the difference between two snapshots,
  the companion to the `#[non_exhaustive]` change below. Monotonic counters (and
  the `*_total_latency` durations) are subtracted with saturation; gauges
  (`queue_depth_*`, `buffer_available_bytes`, `waiting_threads`,
  `incomplete_batches`, `in_flight_dispatches`, and the `average_*` ratios) are
  point-in-time readings and keep the current value. Downstream crates that used
  to compute this with a struct expression should call it instead of assigning
  fields one by one, so a metric added later cannot be silently left at zero.

### Changed

- **Breaking:** `ProducerMetricsSnapshot` is now `#[non_exhaustive]`.
  Downstream crates can no longer build one with a struct expression
  (including functional-update `..base` syntax); start from the new
  `ProducerMetricsSnapshot::ZERO` associated constant — it is usable in const
  context — and assign the fields you need. Reading fields is unaffected.
- **Behaviour change:** `producer-metrics:batch-split-rate` and
  `producer-metrics:batch-split-total` now count `MESSAGE_TOO_LARGE`
  record-batch splits, matching Java's semantics. They were previously fed by
  the `max.request.size` Produce-request grouping split, a kacrab-specific
  event with no Java equivalent. That grouping count is unchanged and remains
  available as `ProducerMetricsSnapshot::produce_request_split_count`, now
  without a Java-named meter. Workloads that only hit request-grouping splits
  will see the Java-named `batch-split-*` metrics drop toward zero.
- **Producer dispatch:** once a broker is receiving a produce request, the head
  batch of every partition that broker leads now rides along on the same
  request, instead of each partition waiting out its own `linger.ms` or filling
  to `batch.size` first. This is Kafka's `RecordAccumulator.drainBatchesForOneNode`
  behaviour, which takes each partition's head batch with no readiness check once
  the node is already being sent to. Swept batches pass through the same
  in-flight reservation and idempotent-sequence bookkeeping as normally drained
  ones, and the sweep only fires when partition leadership is already in the
  metadata cache, so it never adds a metadata fetch ahead of a ready batch.

  Measured against a native single-node Kafka 4.3.0 at 5M x 10 B over 16
  partitions, this raised throughput and cut latency at the same time:
  43.9 -> 47.6 MB/s, 1.65 -> 0.61 ms average latency, 10 -> 6 ms p99, with the
  produce-request count roughly halving as more partitions coalesce into each
  request.

### Fixed

- **Nothing was delivered to a topic whose `max.message.bytes` is below the
  producer's `batch.size`** — every oversized batch failed with `DeliveryTimeout`.
  A batch rejected with `MESSAGE_TOO_LARGE` was re-split against a constant
  `batch.size` target, but the accumulator already caps every batch at
  `batch.size`, so the split regrouped the same records into a child of the same
  size for the broker to reject again, forever. Re-splits now halve the target the
  way Kafka's `RecordAccumulator.splitAndReenqueue` does — a batch that is itself
  a split child targets `max(largest record, estimated batch size / 2)` — so the
  pieces shrink geometrically until they fit. The `max(largest record, ..)` floor
  keeps a batch holding one large record from halving forever, and a single-record
  batch that still does not fit stays a terminal failure, as in Java.
- **A split that produced more than one child dropped every record outside the
  first child.** The whole batch's delivery state went to child 0 and the other
  children were left with none, so their records were reported as
  `DeliveryDropped` and never received a receipt. Each child now completes its own
  slice of the shared delivery state. Previously only reachable when the
  compression-ratio estimate shrank the split target; the halving above makes
  multi-child splits the normal case.
- **A batch that had to be split more than once could not be requeued at all.**
  The second split minted child identities by position, colliding with the first
  split's siblings, and the accumulator's requeue guard rejected the batch with
  `BatchLifecycle`, stalling the partition. Split children now carry a unique
  identity and an explicit link to the batch they were split from, so a split
  chain of any depth stays consistent with the buffered and incomplete batch
  bookkeeping.
- **Records could be delivered out of sequence, or a flush fail with
  `FlushIncomplete`, on any partition where a batch was requeued.** The idempotent
  in-flight set was released when a batch was requeued, but Kafka's
  `inflightBatchesBySequence` tracks every batch that holds a sequence and has not
  terminally completed, *including one waiting in the accumulator to be retried*.
  The drain gate (`shouldStopDrainBatchesForPartition`) compares a retried batch
  against that set, so releasing early made the batch measure itself against
  other, higher sequences and defer indefinitely while fresh batches kept going
  out — the broker saw the partition's sequence run backwards. The registration
  now survives a requeue; a split hands it from parent to children the way
  `RecordAccumulator.splitAndReenqueue` does; and the two paths that abandon
  batches instead of re-dispatching them (a flush with no accumulator to hand them
  back to, an abort that discards buffered work) release it explicitly. Most
  visible on the split path above, which produced 309 `OutOfOrderSequenceNumber`
  responses per run.
- **A partition could stop making progress after a split even though its batches
  were still buffered**, surfacing as `FlushIncomplete` with records left unsent.
  Only one produce request is started per partition per selection, and the drain
  gate admits only the batch holding the partition's first in-flight sequence; the
  selection picked whichever batch the drain returned first, which after a split
  re-enqueues several children at once could be a higher-sequence sibling. The
  gate then deferred that pick every cycle while the batch that would unblock the
  partition was never selected. Selection now takes the lowest base sequence,
  matching Kafka's deque head.
- **The producer parity benchmark dropped buffer-wait time from exactly the
  records that waited longest.** It took its per-record latency timestamp inside
  the send loop, and a `Backpressure` result did not advance the record counter,
  so the retry captured a fresh timestamp. Java has no such gap:
  `ProducerPerformance` captures `sendStartMs` once and `KafkaProducer.send`
  blocks inside that window when the accumulator is full. Benchmark-only; no
  client behaviour changed.
- **Published producer byte-rate figures compared two different lines.** kacrab's
  own `MiB/s` scenario line was quoted against Java's `MB/sec` line. Both tools
  compute `bytes / elapsed / (1024 * 1024)` on their `records sent, ... MB/sec`
  line, so that is the comparable one; read from it, the 10 KiB throughput lead is
  +15% rather than the +25-30% previously published. The 10 B lead is +35%.

### Performance

- **The producer no longer pays a broker round trip for a split that splits
  nothing.** The first split of an accumulator batch targets `batch.size` — the
  size the accumulator already packed the batch to — so regrouping against it
  hands back a single child holding every record the parent held, which the broker
  rejects for the same reason. Java discovers this by sending the identical child
  and waiting; the grouping is local, so kacrab checks it instead and halves on
  the spot until the batch really divides. The floor terminates the loop: at
  `max(largest record, 1)` a second record can never share a group with the first,
  so a batch of two or more records always yields two or more groups.

Measured against a real broker — 20,000 x 4 KiB into a one-partition topic with
`max.message.bytes=65536`, producer at `batch.size=262144`, medians of 5
interleaved kacrab/Java pairs with the topic recreated before every pass:

| | kacrab | Java |
| --- | ---: | ---: |
| throughput | **59,347 rec/s (232 MB/s)** | 26,881 rec/s (105 MB/s) |
| average latency | **109 ms** | 194 ms |
| produce requests | **3,173** | 3,492 |
| batch splits | **952** | 1,270 |
| retries / errors | 0 / 0 | 0 / 0 |

Before this release the same workload delivered nothing. kacrab led in all 5
pairs; its slowest round (39,062 rec/s) still beat Java's fastest (30,816 rec/s).
The default scenarios are unaffected — they never reach the split path — and were
re-measured to confirm it: 45.4 MB/s at 5M x 10 B and 524 MB/s at 100K x 10 KiB,
against Java's 34.8 and 435.

## 0.2.0 — 2026-07-07

Outage-resilience release. The producer and consumer now recover from
prolonged and total broker outages instead of wedging permanently. Includes
one breaking consumer API change (subscription-mode exclusivity), so this is a
minor version bump under the pre-1.0 convention.

### Added

- `Consumer::close_timeout(Duration)` — close with a caller-chosen bound on
  the final commit-and-leave work, the analogue of Java's `close(Duration)`.
  `close()` keeps its `request.timeout.ms` bound.
  ([#45](https://github.com/pirumu/kacrab/pull/45))
- Soak test harness (`benches/src/bin/soak_bench.rs`): sustained load with
  broker-kill and consumer-bounce chaos and a per-partition continuity verdict,
  for measuring resilience under fault injection.
  ([#46](https://github.com/pirumu/kacrab/pull/46))

### Changed

- **Breaking:** `Consumer::assign` now returns `Result`. Subscription modes are
  mutually exclusive, matching Java's `SubscriptionState`: mixing a manual
  `assign` with `subscribe` / `subscribe_pattern` (in either order, or switching
  between topic and pattern subscriptions) returns `ConsumerError::InvalidState`
  instead of silently replacing the previous mode. Call `unsubscribe` to switch
  modes. An empty `assign` is treated as `unsubscribe` (Java parity).
  ([#45](https://github.com/pirumu/kacrab/pull/45))

### Fixed

- The producer no longer wedges permanently on a total-cluster outage longer
  than `delivery.timeout.ms`. The background sender loop treated a transient
  error from its drive pass (a metadata/wire `Timeout` while every broker was
  down) as fatal and parked; once the producer's appends dried up nothing woke
  it again, even after the cluster recovered with ready batches still buffered.
  It now retries on the retry backoff instead of parking, mirroring Kafka
  `Sender.runOnce`. ([#48](https://github.com/pirumu/kacrab/pull/48))
- The producer recovers from prolonged broker outages instead of wedging:
  requeued batches retry on a timer rather than waiting for new traffic, and the
  background sender pump no longer wedges on a single expired batch.
  ([#47](https://github.com/pirumu/kacrab/pull/47))
- The consumer recovers from a coordinator-broker outage instead of
  livelocking: it clears a stale cached coordinator on `Wire(Timeout)` /
  `ConnectionClosed` / `Io`, and JoinGroup/SyncGroup are bounded by the
  rebalance timeout. ([#47](https://github.com/pirumu/kacrab/pull/47))
- Wire: a fenced-broker handshake is bounded by `request.timeout.ms` (a
  restarted-but-fenced broker that accepts TCP but answers nothing no longer
  parks the broker task forever), and the broker reader task is aborted on drop
  and on consumer close so sockets do not linger `ESTABLISHED`.
  ([#47](https://github.com/pirumu/kacrab/pull/47))
- `init_transactions` retries a still-loading transaction coordinator
  (`COORDINATOR_LOAD_IN_PROGRESS` on a freshly-started broker) for the full
  `max.block.ms`, matching Java's blocking `initTransactions`, instead of
  giving up after the produce `retries` count. ([#51](https://github.com/pirumu/kacrab/pull/51))

## 0.1.2 — 2026-07-07

Wire-pipeline correctness fix
([#43](https://github.com/pirumu/kacrab/pull/43)).

### Fixed

- A stray response frame — one whose correlation id parsed but matched no
  in-flight request, typically a late arrival for a request already failed
  by its timeout — no longer fails an unrelated request. It previously
  completed the oldest in-flight slot with `CorrelationIdMismatch`, and the
  misfire cascaded: each subsequent in-order response found its own slot
  consumed and landed one slot off its target until the connection drained.
  Such frames are now dropped; frames too short to carry a correlation id
  still fail the oldest waiter so a garbled stream surfaces a decode error
  instead of waiting out the request timeout.

### Changed

- Request-pipeline slot lookup resolves with one modular add instead of
  walking the ring, making correlation scans and failure sweeps linear in
  the number of in-flight requests instead of quadratic. Only noticeable
  when `max.in.flight.requests.per.connection` is raised well above the
  default of 5.

## 0.1.1 — 2026-07-06

Hardening release: every finding from an external review of 0.1.0, fixed and
real-broker verified ([#39](https://github.com/pirumu/kacrab/pull/39)).

### Security

- Generated protocol decoders no longer trust wire-claimed array lengths for
  `Vec` preallocation. A hostile or corrupt response claiming `i32::MAX`
  elements previously reserved gigabytes up front and aborted the process
  under `panic = "abort"`; the preallocation is now clamped by the bytes
  actually remaining and a fixed budget (`array_read_capacity`), and a
  truncated hostile-length array fails decode cleanly.
- Decompression output is bounded. gzip and zstd decoded to `Vec` with no
  output cap, lz4 capped each 64 KiB block but not the frame total, and
  snappy trusted the raw format's claimed length (allocated up front) — a
  crafted batch could inflate a tiny payload until the allocator gave out.
  All four codecs now refuse to produce more than
  `compression::MAX_DECOMPRESSED_LEN` (1 GiB, ~10:1 over the 100 MiB wire
  frame cap) and surface the new
  `CompressionErrorKind::DecompressedTooLarge` instead of dying.

### Fixed

- A synchronous commit can no longer be overtaken by queued asynchronous
  commits: `commit_sync` / auto-commit / `close` drain the async-commit queue
  through an ordering barrier before committing, so a later sync commit
  cannot be overwritten by an earlier queued one and the committed offset
  never regresses (Java's `commitSync` semantics).
- Asynchronous commits heal across a coordinator move: the commit worker
  re-finds the coordinator once and retries on
  `NOT_COORDINATOR`/`COORDINATOR_NOT_AVAILABLE`/`COORDINATOR_LOAD_IN_PROGRESS`,
  matching the synchronous paths, instead of failing every subsequent
  `commit_async` until the consumer was rebuilt.
- `Consumer::close` applies queued asynchronous commits (firing their
  callbacks) before stopping the commit worker instead of silently dropping
  them.
- One unreachable leader no longer fails the whole `poll` and discards the
  data already fetched from the other leaders that round: the failed leader's
  partitions are flagged for a metadata refresh and retried next poll, per
  Java's per-node fetch handlers. Terminal TLS/SASL setup failures still
  surface.
- A short `poll` timeout is no longer overshot by the idle backoff — the
  empty-round wait is clamped to the remaining poll budget.

### Added

- Consumer `retry.backoff.ms` (default 100 ms) and `retry.backoff.max.ms`
  (default 1 s) as typed config. The idle-poll wait follows
  `retry.backoff.ms` (was a fixed 50 ms), and coordinator lookups retry under
  the exponential policy (base doubling to max, 20% jitter) matching Java
  `AbstractCoordinator`'s `ExponentialBackoff` (was a fixed 500 ms).
- `kacrab-protocol`: per-codec `decompress_bounded` and
  `MAX_DECOMPRESSED_LEN` for callers that want an explicit decompression
  budget; `primitives::array_read_capacity`.
- Real-broker regression tests for the commit-ordering barrier and for
  consumer-side decompression of broker-compressed batches across all four
  codecs (the CLI helpers honor `KACRAB_KAFKA_BIN` for hosts where
  `127.0.0.1:9092` is a native broker rather than the compose container).

## 0.1.0 — 2026-07-02

First crates.io release: `kacrab`, `kacrab-protocol`, and `kacrab-macros`
([#36](https://github.com/pirumu/kacrab/pull/36)).

### Added

- Consumer topic-id-keyed `Fetch` (KIP-516): fetches now negotiate up to the
  broker's `Fetch` version (v18 on Kafka 4.3) instead of capping at the
  name-keyed v12. Topic ids are resolved from the routing metadata, responses
  map ids back to names via the request's id set (Java's `sessionTopicNames`),
  fetch sessions carry their ids into the forgotten list, and a topic without
  an id — or a pre-v13 broker — downgrades that fetch to v12 exactly like
  Java's `AbstractFetch`. `UNKNOWN_TOPIC_ID`/`INCONSISTENT_TOPIC_ID` are
  handled as retriable per-partition metadata refreshes, and a session whose
  topic ids changed (recreated topic) or whose keying mode flipped re-opens
  with a full fetch. Verified against a real Apache Kafka 4.3.0 broker
  (negotiates v18) across the full consumer suite, throughput-neutral on the
  consumer benchmark.
- Consumer cross-poll fetch buffering (Java's `CompletedFetches`): raw fetch
  responses are buffered client-side, `poll` drains them `max.poll.records` at
  a time, and a partition is only re-fetched once its buffer runs dry.
  Buffered data is invalidated lazily on seek/reset/revoke and retained across
  pause. Previously each poll re-fetched — and the broker re-served — the
  response surplus past `max.poll.records`, which capped small-record
  consumption at ~132K records/sec (~13 Fetch RPCs per 5M-record run now,
  down from 10,000).
- Consumer background prefetch (Java's network thread): the next `Fetch` runs
  as a spawned task while `poll` serves buffered records; an empty-buffer poll
  awaits it only up to its own timeout. Fetches skip nodes still hosting
  buffered partitions (Java's buffered-node gate), which both protects the
  broker's fetch-session cache and avoids a caught-up-partitions-only request
  long-polling `fetch.max.wait.ms` mid-pipeline.
- Consumer lazy per-batch record decode (`decode_next_batch` in
  `kacrab-protocol`): buffered blobs decode one record batch at a time as
  drained, holding raw blobs plus ~one batch of records in memory instead of
  materializing whole responses (which cost ~536 MiB of allocator churn on a
  5M-record run; now ~18 MiB peak RSS).
- With all three, the consumer head-to-head at identical defaults now reads:
  10 B records ~17.6M vs Java ~9.3M records/sec (~1.9x), 10 KiB ~540K vs
  ~136K records/sec (~4x, ~5.3 GB/s), at ~16-20x less peak memory, ~9-17x less
  CPU, ~15x faster group joins, and a poll() max 14-25x lower; per-poll
  latency percentiles are printed by both the Rust bench and a compiled Java
  probe in the baseline wrapper.
- Real-Kafka consumer benchmark (`consumer_kafka_bench`) mirroring Java's
  `kafka-consumer-perf-test.sh` (same tool props, poll loop, timeout semantics,
  and final CSV columns), with a `KACRAB_BENCH_PREFILL=1` topic prefill, a Java
  baseline wrapper (`benches/scripts/consumer_default_matrix.sh`), and
  `make bench-kafka-consumer` / `bench-kafka-consumer-java-default` targets.
  Head-to-head at identical defaults (2026-07-02, native Kafka 4.3.0): kacrab
  consumes 10 B records ~28% faster than Java (~11.8M vs ~9.25M records/sec)
  and 10 KiB records ~3x faster (~4.7-5.0 GB/s vs ~1.5 GB/s) at a fraction of
  the CPU, with ~10x-faster group joins; caveats (peak-RSS churn on tiny-record
  bursts) in `benches/README.md`.

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

- `ConsumerRecord.topic` is now `Arc<str>` (was `String`), matching the
  producer's `RecordMetadata`: records in a poll share one topic handle
  instead of heap-allocating the name once per record (5M allocations per
  5M-record run). `record.topic.as_ref()` / deref coercion covers `&str`
  uses; construction sites need `Arc::from(...)`.

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
