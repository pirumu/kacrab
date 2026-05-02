# Building a Production-Ready Kafka Client in Rust

---

## Ground Rules

### What "no AI" means in practice

**Allowed**
- Kafka protocol spec: <https://kafka.apache.org/protocol>
- KIPs (Kafka Improvement Proposals)
- Apache Kafka source code (Java/Scala) for behavior reference
- `librdkafka` (C), `rskafka`, `kafka-protocol-rs` source — **only after attempting yourself**
- Stack Overflow / GitHub issues for specific bugs you've already debugged
- Books, blog posts, conference talks
- Standard Rust documentation, `tokio` docs, crate docs

**Not allowed**
- Claude / ChatGPT / Gemini for code generation or design
- Copilot, Cursor tab-complete, Supermaven, Codeium, any LLM autocomplete
- Copying architectural decisions wholesale from existing implementations

**The rule of thumb**: if it's a human-curated text resource, it's fine. If it's a model generating tokens at you, it's not.

### Discipline tools

- Keep a `JOURNAL.md` in the repo. Each session, write 3-5 lines: what you got stuck on, what clicked, what surprised you. This is the single most valuable artifact you'll produce.
- Keep a `DECISIONS.md` (lightweight ADRs). When you make a non-obvious design choice, write 5 sentences: context, decision, alternatives considered, trade-offs.
- Commit often, with descriptive messages. Treat `git log` as a learning record.
- No silent rewrites. If you scrap an approach, branch it off first so you can come back and compare.

### Scope discipline

The single most common failure mode for "build Kafka client" projects is trying to implement v1 = full feature set. Each phase below is **end-to-end runnable** before the next phase starts. A working subset beats an unfinished superset.

---

## Prerequisites

Before phase 0:

- Comfortable with Rust ownership, borrowing, lifetimes (no fighting the borrow checker on basic operations).
- Have read the Tokio tutorial and built at least one small async TCP server.
- Understand `Future`, `Pin`, basic async cancellation semantics.
- Can read Wireshark / tcpdump output at a basic level.

If any of these is shaky, fix it before starting. Half-knowing async will make phase 1 miserable.

### Environment setup

- Local Kafka in Docker, **KRaft mode** (no Zookeeper). Single broker for phase 1-2, 3-broker cluster from phase 3 onward.
- Tools: `kcat` (formerly `kafkacat`), `kafka-console-producer`, `kafka-console-consumer`, `kafka-topics.sh`. Treat these as ground truth.
- Wireshark with the Kafka dissector, or at minimum `tcpdump` + manual hex inspection.
- A `compose.yaml` checked into the repo so anyone can spin up the test cluster reproducibly.

---

## Phase 0 — Protocol Primitives (Week 1-2)

**Outcome**: A `protocol` module inside the `kacrab` crate that can encode and decode every Kafka primitive type, round-trip safe, property-tested.

This is the foundation. Every bug here cascades into every higher layer. Spend the time.

### Tasks

1. Workspace is already set up — Tokio-style flat layout. Published crates: `kacrab`
   (main entry point), `kacrab-macros` (proc macros), `kacrab-util` (adapters),
   `kacrab-test` (test fixtures). Internal crates: `benches/`, `examples/`,
   `tests-integration/`. Phase code lands as **modules** inside `kacrab/src/`
   (`protocol/`, `wire/`, `metadata/`, `producer/`, `consumer/`, …), not as
   separate per-phase crates.
2. Implement encoders/decoders for primitives:
    - Fixed: `int8`, `int16`, `int32`, `int64`, `uint32`, `float64`, `boolean`
    - Variable: `varint` (zigzag), `varlong`
    - String: `string` (i16 length-prefixed), `nullable_string`, `compact_string`, `compact_nullable_string`
    - Bytes: `bytes`, `nullable_bytes`, `compact_bytes`, `compact_nullable_bytes`
    - Arrays: `array`, `compact_array`
    - `uuid` (16 raw bytes)
    - **Tagged fields** (KIP-482) — read this KIP carefully, it's required for any modern API version
3. Define a `Encodable` / `Decodable` trait pair. Decide early: do you take `&mut Vec<u8>` or `impl BufMut`? Document the choice in `DECISIONS.md`.
4. Property tests with `proptest`:
    - For every primitive: `decode(encode(x)) == x` for arbitrary `x`.
    - For every primitive: invalid inputs produce errors, not panics.

### Validation

- All `proptest` cases pass with `cases = 10_000`.
- Hand-craft a known good payload from the spec (e.g. the example bytes in the protocol doc) and decode it — bytes match.
- Run `cargo +nightly miri test -p kacrab` (scoped to the `protocol` module). No UB.

### Common traps

- Compact strings use `unsigned varint length + 1`, with `0` meaning null. Off-by-one here is a silent corruption.
- Tagged fields are written as a `varint count` followed by `(tag, length, data)` triples. Skip unknown tags on decode.
- Don't conflate "compact" with "flexible". A request version is "flexible" if it uses tagged fields and compact strings/arrays — the spec lists which versions are flexible.

**Exit criteria**: you can encode and decode any primitive without consulting the spec.

---

## Phase 1 — Wire Layer + ApiVersions Handshake (Week 2-3)

**Outcome**: A TCP client that connects to a broker, completes the `ApiVersions` handshake, and can send any request with correct framing.

### Tasks

1. TCP framing: every request is `int32 size | bytes payload`. Build a `Framed` codec on top of `tokio::net::TcpStream` using `tokio_util::codec::LengthDelimitedCodec`.
2. Request header v0/v1/v2 (the latter is flexible — has tagged fields):
    - `api_key` (i16), `api_version` (i16), `correlation_id` (i32), `client_id` (nullable_string), tagged fields (v2 only).
3. Response header v0/v1 (v1 is flexible):
    - `correlation_id` (i32), tagged fields (v1 only).
4. **Correlation manager**: every outgoing request is tagged with a monotonic `correlation_id`. Maintain a `HashMap<i32, oneshot::Sender<Response>>`. A reader task pulls responses off the wire, looks up the correlation_id, and forwards to the awaiting future.
5. Implement `ApiVersions` request/response (api_key 18). Notable wrinkle: you send v0 first to discover what versions the broker supports, then upgrade.
6. Build a generic `send<R: Request>(req: R) -> R::Response` function. Don't hardcode per-API logic in the wire layer.

### Validation

- Connect to local broker, complete handshake, log the supported API versions. Compare against `kcat -L`.
- Capture your traffic in Wireshark, confirm the dissector parses it without errors.
- Kill the broker mid-request — your client should return a clean error, not panic, not hang.

### Design notes to write down

- How does correlation_id rollover work (i32 wraps)? Decide and document.
- What's the timeout policy on a request? Per-request? Connection-level?
- How do you handle a response arriving for a correlation_id you've forgotten (e.g. timed out)?

---

## Phase 2 — Metadata + Record Batch Encoding (Week 3-5)

**Outcome**: Client can fetch cluster metadata and encode a v2 record batch correctly.

### Tasks

1. `Metadata` request (api_key 3). Multiple versions; pick a recent flexible one (v9+) and stick with it.
2. Cluster state model: `Cluster { brokers: HashMap<i32, Broker>, topics: HashMap<String, TopicMetadata> }`. Refresh policy: lazy, with a configurable max age.
3. **Record batch v2** — this is the hairy part:
    - Header: `base_offset` (i64), `batch_length` (i32), `partition_leader_epoch` (i32), `magic` (i8, must be 2), `crc` (i32, **CRC32C** not CRC32), `attributes` (i16), `last_offset_delta` (i32), `base_timestamp` (i64), `max_timestamp` (i64), `producer_id` (i64), `producer_epoch` (i16), `base_sequence` (i32), `record_count` (i32).
    - Records: each record is `length (varint) | attributes (i8) | timestamp_delta (varlong) | offset_delta (varint) | key_length (varint) | key | value_length (varint) | value | header_count (varint) | headers`.
    - CRC32C covers everything from `attributes` onward. Use the `crc32c` crate, or implement using SSE4.2 intrinsics if you want the practice.
4. Round-trip test: encode a batch, decode it, compare. Then produce one to local Kafka and consume it back with `kcat` to confirm wire compatibility.

### Validation

- `kcat -C -t test-topic -e` reads back exactly what you encoded, key and value bytes intact.
- Property test: encoding any valid `RecordBatch` produces a buffer that decodes back to the same batch.
- Edge cases: empty batch (0 records), single record, max-size record, records with null keys, records with headers.

### Common traps

- CRC32C ≠ CRC32. Wrong polynomial = silent rejection by the broker, with a generic "corrupt message" error.
- The `batch_length` field excludes the first 12 bytes (`base_offset` + `batch_length` itself).
- Timestamp delta encoding: each record's timestamp is stored as a delta from `base_timestamp`. Off-by-one here gives you records with timestamps in 1970.

---

## Phase 3 — Minimal Producer, ack=1 (Week 5-7)

**Outcome**: `producer.send(topic, key, value).await` works, end-to-end, against a real broker.

### Tasks

1. `Produce` request (api_key 0). Use a recent flexible version.
2. Topic-to-partition routing:
    - Default partitioner: hash of key (murmur2, to match Java client's default), or round-robin if no key.
    - Look up partition leader from cluster metadata; route the request to the leader's broker.
3. Connection pool: one `tokio` task per broker connection. Producer code sends a `(request, oneshot::Sender<response>)` over an `mpsc` channel to the right connection task.
4. Error handling — Kafka error codes are central:
    - Build a `KafkaError` enum from the official error code list.
    - Classify errors: retriable (`NotLeaderForPartition`, `LeaderNotAvailable`, `RequestTimedOut`) vs fatal (`TopicAuthorizationFailed`, `RecordTooLarge`).
    - On retriable errors, refresh metadata and retry with backoff.

### Validation

- Produce 10,000 messages to a 3-partition topic. Consume them all back with `kafka-console-consumer`. Counts match. No reordering within a partition.
- Kill the partition leader mid-produce. Client recovers, finishes the workload.
- Run with `tokio-console` attached. Verify no task leaks, no unbounded channel growth.

### Don't do yet

- Batching multiple records into one Produce request (phase 8).
- Compression (phase 8).
- Idempotence (phase 6).
- Transactions (phase 7).

A correctness-first producer that sends one record per request is fine for now. Performance comes later.

---

## Phase 4 — Minimal Consumer, no group (Week 7-9)

**Outcome**: `consumer.poll().await` returns records, with manual offset tracking.

### Tasks

1. `Fetch` request (api_key 1). Pick a flexible version (v12+).
2. `ListOffsets` request (api_key 2) — needed to seek to earliest/latest.
3. Manual partition assignment: caller specifies `(topic, partition, offset)` triples.
4. Decode incoming record batches. Handle the "fetch returned a batch that starts before the requested offset" case (you may receive bytes you need to skip).
5. Handle `OFFSET_OUT_OF_RANGE`, empty fetches (broker returns nothing if no new data within `max_wait_ms`), and partial batches at the end of the response (silently truncated, not an error).

### Validation

- Produce 10,000 messages with phase 3 client, consume them with this one. Bytes match. Offsets are contiguous.
- Seek to a specific offset, consume, verify first message is what you expected.
- Cross-test: produce with `kafka-console-producer`, consume with your client. And vice versa.

### Common traps

- Fetch responses can include partial record batches at the end if the response size limit was hit. The official rule: ignore them, advance offset by the bytes consumed, fetch again.
- A "batch" in the response is the same record batch v2 format from phase 2. You're reusing the decoder.
- The fetch response has nested aborted transactions metadata even if you're not using transactions yet — read past it correctly.

---

## Phase 5 — Consumer Groups (Week 9-13)

**Outcome**: Multiple consumer instances coordinate via the broker; partitions are assigned and rebalanced automatically.

This is the boss fight. State machine, edge cases, and the most common source of bugs in real-world Kafka clients.

### Choose your protocol

- **Old protocol** (eager rebalance): simpler, more docs, more reference implementations. Recommended for learning.
- **New protocol** (KIP-848, "Next Generation Consumer Rebalance Protocol"): broker-side coordination, simpler client. Promising but newer.

Implement old protocol first. Optionally migrate later as a separate exercise.

### Tasks (old protocol)

1. `FindCoordinator` (api_key 10) — find the group coordinator broker.
2. `JoinGroup` (api_key 11):
    - First member in becomes leader; receives the full member list and runs the assignment.
    - Implement `range` and `roundrobin` assignors.
    - Encode the member assignment as bytes (the protocol carries it as opaque bytes, but the leader and members must agree on the format — use the standard `ConsumerProtocolAssignment` schema).
3. `SyncGroup` (api_key 14): leader sends assignments, all members receive theirs.
4. `Heartbeat` (api_key 12): periodic, on a separate task. Missing heartbeats trigger rebalance.
5. `OffsetCommit` (api_key 8) and `OffsetFetch` (api_key 9): committed offsets stored on the broker.
6. `LeaveGroup` (api_key 13): clean shutdown.

### State machine

Draw it on paper before you code. Minimum states: `Unjoined`, `Joining`, `Awaiting Sync`, `Stable`, `Rebalancing`, `Leaving`. Every Kafka error code maps to a state transition. Document the full table in `DECISIONS.md`.

### Validation

- Spin up 3 consumer instances on a 12-partition topic. Each gets ~4 partitions. Kill one — the other two get 6 each within `session_timeout_ms`.
- Add a 4th consumer mid-stream. Rebalance happens, no messages lost, no duplicates (with proper offset commit semantics).
- Scenario: consumer hangs (simulate with sleep longer than `session_timeout_ms`). Group rebalances without it.
- Compare behavior against Java consumer in identical scenarios. Differences are bugs.

### Don't underestimate

This phase will take longer than you think. Budget 4 weeks, expect 5-6. The state machine has subtle ordering requirements (e.g. you must commit offsets before sending `LeaveGroup`, otherwise the next member to take your partitions reprocesses messages).

---

## Phase 6 — Idempotent Producer (Week 13-15)

**Outcome**: Producer guarantees no duplicates on retry, within a single session.

### Tasks

1. `InitProducerId` (api_key 22): broker assigns `(producer_id, producer_epoch)`.
2. Per-partition sequence numbers: each `RecordBatch` has `base_sequence`, incremented per record.
3. Track in-flight requests per partition. On a retry after a connection failure, ensure ordering (don't allow the retry to land after a newer batch).
4. Handle `OUT_OF_ORDER_SEQUENCE_NUMBER` and `DUPLICATE_SEQUENCE_NUMBER` errors.

### Validation

- Inject network failures (use `toxiproxy` or equivalent) during sustained production. Consumer sees no duplicates and no gaps.
- Verify with `--print-key`, `--property print.value=true` and a counter in messages: counter sequence is intact end-to-end.

---

## Phase 7 — Transactions (Week 15-18, optional but worth it)

**Outcome**: Producer can do read-from-X, transform, write-to-Y atomically.

### Tasks

1. `AddPartitionsToTxn` (api_key 24), `EndTxn` (api_key 26), `WriteTxnMarkers` (broker-only, you observe its effects).
2. Transactional `OffsetCommit` (`AddOffsetsToTxn`, api_key 25 + `TxnOffsetCommit`, api_key 28).
3. Consumer side: `read_committed` isolation level — skip records from aborted transactions using the `aborted_transactions` field in fetch responses.

### Validation

- Build a small "exactly-once" pipeline: consume from topic A, transform, produce to topic B, commit offsets, all in one transaction. Inject failures, verify no duplicates and no losses on topic B.

This is the hardest correctness bar in Kafka. Even seasoned implementations have bugs here. Treat warnings as errors.

---

## Phase 8 — Performance (Week 18-22)

Now and only now do you optimize. Profile first.

### Tasks

1. **Batching**: in the producer, accumulate records per (topic, partition) into a single Produce request. Tunables: `linger_ms`, `batch_size`. This is the single biggest throughput win.
2. **Compression**: gzip, snappy, lz4, zstd. Implement at the record batch level. Verify interop both ways.
3. **Buffer pool**: avoid `Vec` allocations on the hot path. You've done this in KaCrab — this time, do it without referring to it.
4. **Pipelining**: allow `max_in_flight_requests_per_connection > 1` (carefully, since this interacts with idempotence ordering).
5. **Zero-copy on read**: parse fetch responses with `Bytes` slices, don't copy record values into owned buffers unless the user asks.
6. **Profiling**: `cargo flamegraph`, `perf record`, `tokio-console`. Measure, don't guess.

### Validation

- Throughput target: within 2x of `librdkafka` on identical hardware and config. (Hitting parity is realistic but takes more time.)
- p99 latency under sustained load is bounded and predictable, not bursty.
- Memory usage is flat under steady-state load (no leaks, no growth).

### Benchmark suite

Write benches in the `benches/` workspace crate that:
- Produce N messages of size S to a topic with P partitions.
- Record throughput, p50/p99/p999 latency, memory footprint.
- Can be run against your client and against `librdkafka` (via `rdkafka` crate) for comparison.

Check these benches into the repo. Run them on every release.

---

## Phase 9 — Production Hardening (Week 22-26)

This is what separates "works on my machine" from "production-ready". Most of these aren't fun. Do them anyway.

### Reliability

- **Reconnection logic**: exponential backoff with jitter, per-broker. Don't thundering-herd on cluster-wide outages.
- **Metadata refresh**: on `NotLeaderForPartition`, refresh and retry. Cap retries to avoid infinite loops.
- **Cluster topology changes**: brokers added/removed, partitions reassigned. Test by adding/removing brokers from the cluster while client is running.
- **Backpressure**: bounded channels everywhere. If the producer's send queue fills, callers block or get an error — they don't OOM the process.

### Observability

- Structured logging with `tracing`. Spans for: connection, request, batch, rebalance.
- Metrics (Prometheus format): bytes in/out per broker, requests in-flight, batch fill ratio, retries by error code, rebalance duration, consumer lag.
- Health check: a single `client.health()` call that returns connection state, last metadata refresh, current group state.

### Security

- TLS: `rustls` integration. Test against a broker with TLS enabled.
- SASL: implement at least `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`. `OAUTHBEARER` if you want.
- mTLS: client certificate authentication.
- Verify against a Confluent Platform or Aiven cluster with real auth.

### Configuration

- Builder pattern with sensible defaults. Match `librdkafka` config names where possible — users coming from other clients will thank you.
- Validation at config-build time. Fail fast on invalid combos (e.g. idempotence requires `acks=all`).

### Failure modes to test explicitly

- Broker hangs (no TCP RST, just stops responding).
- Slow network (use `tc` to add 200ms latency, 1% packet loss).
- DNS resolution failure mid-session.
- Coordinator broker dies during a rebalance.
- Disk full on broker (writes start failing).
- Clock skew between client and broker.

Each of these should be a test, ideally automated in CI with `toxiproxy` or `pumba`.

---

## Phase 10 — API Polish & Release (Week 26-30)

### Public API design

- Re-read the API as a user. What's awkward? What's surprising? Iterate.
- Document every public type and function. `cargo doc --open` should produce something you'd be willing to publish.
- Examples directory: simple producer, simple consumer, transactional, with TLS, with custom partitioner.
- An integration guide: "I'm coming from `rdkafka`, what do I need to know?"

### Test coverage

- Unit tests for protocol encode/decode.
- Integration tests against a real Kafka cluster (Docker compose in CI).
- Chaos tests with `toxiproxy`.
- Long-running soak test: 24h at sustained load, no leaks, no drift.

### Release readiness checklist

- [ ] `cargo clippy --workspace --all-targets --all-features -- -D warnings` clean (`make clippy`).
- [ ] `cargo +nightly miri test -p kacrab` passes on the `protocol` module.
- [ ] No `unwrap()` or `panic!` on public code paths (search and audit every one).
- [ ] All public APIs have docs with at least one example.
- [ ] CHANGELOG.md, semver discipline.
- [ ] CI runs on Linux, macOS, Windows.
- [ ] MSRV declared and tested.
- [ ] Benchmark suite runs in CI on a fixed reference machine.
- [ ] Security audit: `cargo audit` clean, no `unsafe` without a SAFETY comment.

---

## Cross-Cutting: How to Get Unstuck Without AI

When you hit a wall (and you will, often):

1. **Re-read the spec section.** 80% of bugs in this project will be misreading a spec. The protocol page is precise; trust it.
2. **Capture the wire traffic.** Wireshark or `tcpdump -X`. Compare your bytes against what `kcat` sends/receives. Differences are the bug.
3. **Read the broker's logs.** Set `log4j` to DEBUG. The broker tells you exactly why it rejected your request.
4. **Read another client's source.** `librdkafka` for behavior, `rskafka` for clean Rust patterns. Read targeted: find the function for the API you're stuck on, read it, then close the tab. Don't browse aimlessly.
5. **Write a smaller failing test.** If your producer is broken, write a test that sends one specific known-good payload and assert the response. Shrink until the failure is obvious.
6. **Sleep on it.** Genuinely. Most consumer-group state-machine bugs become obvious 12 hours later.

---

## Resource List

### Required reading (in order)

- Kafka protocol guide: <https://kafka.apache.org/protocol>
- KIP-482 — flexible versions / tagged fields
- KIP-98 — idempotent producer & transactions
- KIP-447 — transactional offset commit
- KIP-848 — next-gen consumer protocol (read after old protocol works)

### Reference implementations (consult, don't copy)

- `librdkafka` (C) — the reference implementation, behavior-wise.
- `rskafka` — clean async Rust, smaller scope.
- `kafka-protocol-rs` — focused on protocol encode/decode in Rust.
- Apache Kafka itself (Java) — the source of truth when the spec is ambiguous.

### Books / talks

- *Kafka: The Definitive Guide* (Narkhede et al.) — for protocol context, not implementation.
- *Designing Data-Intensive Applications* (Kleppmann) — for the theory behind log-based systems.
- Jay Kreps's "The Log" essay — original motivation.
- Confluent engineering blog — many posts go into protocol-level detail.

### Tools

- `kcat` — Swiss army knife for debugging Kafka.
- Wireshark with the Kafka dissector.
- `toxiproxy` — failure injection.
- `tokio-console` — async runtime introspection.
- `cargo flamegraph`, `perf` — profiling.

---

## Realistic Timeline

Calibrated for ~15-20 hrs/week of focused work alongside a full-time job. Adjust to your reality.

| Phase | Weeks | Cumulative |
|-------|-------|------------|
| 0. Protocol primitives | 1-2 | 2 |
| 1. Wire + ApiVersions | 2-3 | 5 |
| 2. Metadata + record batch | 3-5 | 10 |
| 3. Minimal producer | 5-7 | 17 |
| 4. Minimal consumer | 7-9 | 24 |
| 5. Consumer groups | 9-13 | 37 |
| 6. Idempotent producer | 13-15 | 50 |
| 7. Transactions (optional) | 15-18 | 68 |
| 8. Performance | 18-22 | 90 |
| 9. Production hardening | 22-26 | 116 |
| 10. Polish & release | 26-30 | 146 |

**Total**: ~6-9 months part-time to a genuinely production-ready client. Skip phase 7 and you're around 5-7 months.

The first half is about correctness; the second half is about everything else. Don't be discouraged by how long phase 9 takes — that's where the real engineering lives, and it's where most "Kafka client in Rust" repos on GitHub stop.

---

## Definition of Done

You are production-ready when:

- An independent user can `cargo add` your crate, follow the README, and produce/consume to a Kafka cluster in under 10 minutes.
- The chaos test suite runs for 24 hours without a single message loss or duplicate.
- Throughput is within 2x of `librdkafka` for the same workload.
- A senior engineer who has never seen the code can read the public API docs and successfully integrate it into a service.
- You can debug a production incident in this client without referring to anything except the code and logs.
- You can explain every design decision in `DECISIONS.md` from memory.

When all six are true, you've built something real — and you'll know more about Rust async, distributed systems, and binary protocols than 99% of engineers who only ever consumed Kafka clients.

---

*Good luck. Keep the journal. Trust the spec. Don't cheat.*