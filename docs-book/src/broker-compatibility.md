# Which brokers kacrab speaks to

A Kafka client's compatibility claim is only worth the evidence behind it. This
chapter is that evidence: what the floor is and what enforces it, what the
ceiling is and why it is not a wall, which broker releases are exercised by CI
and which are only exercised offline, and which features have a floor of their
own.

Three numbers carry the story:

| | Version | What makes it true |
|---|---|---|
| **Minimum accepted** | Apache Kafka **2.4** | The `ApiVersions` v3 handshake. Older brokers are rejected on connect with a typed error naming the requirement. |
| **CI-exercised** | **3.3.2 · 3.6.2 · 3.9.0 · 4.0.0 · 4.3.0** | The broker-version matrix in `.github/workflows/real-broker.yml`, running the real suites against real containers. 3.3.2 — the first leg inside the 2.4–3.5 range — runs nightly and on push, not on PRs. |
| **Maximum** | Schemas generated from Apache Kafka **4.3.0** | Newer brokers negotiate down to 4.3.0-era versions under Kafka's own bidirectional compatibility model. Tested up to 4.3.0. |

## The mechanism: nothing is hardcoded

kacrab's request/response types are generated from the Apache Kafka **4.3.0**
message schemas (see [Learning the language](./codegen.md)), but no request
version is written down anywhere in the client. Every connection performs the
`ApiVersions` handshake, and every subsequent request asks that broker's own
advertised ranges what version to use:

- The broker's `ApiVersionsResponse` becomes a `BrokerCapabilities`
  (`kacrab/src/wire/capabilities.rs`).
- Callers pass a version *ceiling*, not a version. `write_command` resolves the
  actual version through `version_for_limit(api_key, ceiling)`
  (`kacrab/src/wire/broker.rs`), intersecting the client's range with the
  broker's.
- If the intersection is empty, the result is a typed
  `WireError::UnsupportedApiVersion` that names the API, the range kacrab
  speaks, and either the range the broker advertised or the fact that the
  broker did not advertise the API at all. Those two cases are rendered
  differently on purpose: an absent key is usually a broker predating the API,
  while disjoint ranges are a broker that dropped — or has not yet added — the
  versions kacrab speaks.

This is the same bidirectional model Kafka's own clients use, which is why a
4.3.0-generated client is not pinned to 4.3.0 brokers, and why a broker newer
than 4.3.0 is not a problem: it will advertise at least the ranges kacrab knows.

## The floor: 2.4, enforced at connect

The handshake itself is what sets the floor. kacrab pins `ApiVersions` v3
(`API_VERSIONS_HANDSHAKE_VERSION` in `kacrab/src/wire/broker.rs`), and v3 arrived
in Kafka 2.4. A broker too old to parse it answers `UNSUPPORTED_VERSION`.

kacrab deliberately does **not** retry the handshake at v0 the way the Java
client does. Instead the condition is classified as a fatal setup error and
surfaced immediately:

```text
broker does not support ApiVersions v3; kacrab requires Apache Kafka 2.4 or newer
```

The alternative — treating it as retriable — is what the client used to do, and
it meant a pre-2.4 broker produced a reconnect-backoff loop that ran until
`request.timeout.ms` and then reported a timeout, telling the operator nothing.
Two tests in `kacrab/src/wire/broker.rs` pin the behaviour: one asserts the
handshake itself fails with that message, and one asserts the run loop gives up
on a pre-2.4 broker rather than looping. The constant behind the message is
`MIN_SUPPORTED_KAFKA_RELEASE` in `kacrab/src/wire/error.rs`, so the number in the
error and the number in this table cannot drift.

Between 2.4 and 3.5, negotiation and version-aware request construction are
implemented and covered by unit and fixture tests — and since the 3.3.2 leg
landed, one release in that range is **CI-verified at 3.3.2 (core tier,
nightly)**. 3.3 is the line where KRaft went GA (KIP-833, 3.3.1), which is what
lets it reuse the repo's single-broker combined-roles fixture shape with no
ZooKeeper. Releases below 3.3 — including 2.8, the last pre-KRaft-default line,
which would need a ZooKeeper-shaped fixture — remain *accepted, not yet
CI-verified*.

## The CI matrix

`.github/workflows/real-broker.yml` runs the `#[ignore]`d `real_kafka_*` suites
against live containers, matrixed over five broker releases. Each leg sets
`KAFKA_IMAGE` for the compose file, a seam every compose file already had
(`${KAFKA_IMAGE:-apache/kafka:4.3.0}`).

| Leg | Image | Compose file | Suite tier | Gating |
|---|---|---|---|---|
| 4.3.0 | `apache/kafka:4.3.0` | `docker-compose.kafka.yml` | full | blocking |
| 4.0.0 | `apache/kafka:4.0.0` | `docker-compose.kafka.yml` | core | non-blocking |
| 3.9.0 | `apache/kafka:3.9.0` | `docker-compose.kafka.yml` | core | non-blocking |
| 3.6.2 | `bitnamilegacy/kafka:3.6.2` | `docker-compose.kafka-bitnami.yml` | core | non-blocking |
| 3.3.2 | `bitnamilegacy/kafka:3.3.2` | `docker-compose.kafka-33.yml` | core | non-blocking, nightly/push only |

**Two tiers.** *core* — producer, the classic-protocol consumer tests, a
compression round-trip, and admin smoke — runs on every leg, because those APIs
all existed well before 3.6. *full* is core plus the KIP-848 consumer and the
KIP-932 share-consumer suites, and runs on 4.3.0 only, because those APIs do not
exist on the older legs. The `admin-extended` and `cluster` jobs stay pinned to
4.3.0 for reasons of their own: the admin-extended fixture depends on 4.3.0
broker defaults (share and streams features formatted on), so pointing it at an
older image produces a broker missing the features the suite is about rather
than an older-broker signal; and the cluster suite is about dispatch and leader
failover, which is orthogonal to version negotiation.

**When each leg runs.** The suites run `--test-threads=1` — they share one broker
and name topics with millisecond nonces, so parallel runs collide — which makes
each leg a full serial pass and five legs five times the wall clock. Pull
requests therefore get two legs, 4.3.0 and 3.6.2 — newest plus the oldest of
the original matrix, where a negotiation break shows up first. The complete
matrix, including the 3.3.2 leg, runs on push-to-master, on the nightly
schedule, and on demand.

**Why the old legs are non-blocking.** They carry `continue-on-error` and cannot
fail a PR yet. That is deliberate and temporary: nobody has published the
per-surface floor table, so there is no agreed answer to "should this test pass
on 3.6?", and a red leg would be noise rather than a regression signal. They are
evidence-gathering. Once per-test `#[min_broker(..)]` gating lands and the floor
table is published, the flag comes off — leaving them permanently non-blocking
would make the whole matrix decorative.

**Reading the results.** Every leg writes a per-suite outcome table into the
GitHub step summary, so which surfaces survive which broker is visible without
opening four job logs.

### `docker-compose.kafka-bitnami.yml`

`apache/kafka` publishes no tag older than 3.7, so the 3.6 leg reaches for
Bitnami — and for `bitnamilegacy/kafka:3.6.2`, not `bitnami/kafka:3.6.2`:
Bitnami moved its back-catalogue to the `bitnamilegacy` namespace in 2025 and
the 3.6 tags were removed from the original repo. Same image, current name.

It is a second compose file rather than an overlay because Bitnami configures
the broker through `KAFKA_CFG_*` variables instead of the `KAFKA_*` shape the
Apache image reads, and a compose overlay can only add keys — it cannot drop the
Apache-shaped ones, which would then sit in the environment doing nothing while
reading as if they applied. Every setting in it is the same setting as in
`docker-compose.kafka.yml`, spelled the way that image reads it.

The 3.3.2 leg's `docker-compose.kafka-33.yml` is the same Bitnami shape once
more, with one addition: the 3.3-era image needs `KAFKA_ENABLE_KRAFT=yes`
spelled out — its scripts default to ZooKeeper mode and demand
`KAFKA_CFG_ZOOKEEPER_CONNECT` otherwise — where the 3.6-era scripts infer KRaft
from `process.roles`.

## Capability-aware admin smoke

`real_kafka_admin_smoke` runs on every leg, which means it meets brokers that
genuinely cannot express some of its operations. Rather than failing, those
operations report named `SKIPPED` lines plus a capability-summary line:

- **4.3.0** — no skips; every operation runs.
- **3.9.0** — `list_config_resources(Topic)` skipped: the broker advertises
  `ListConfigResources` (API 74) at v0 only, and v0 has no `resource_types`
  field.
- **3.6.2** — `list_config_resources(Topic)` *and*
  `list_client_metrics_resources` skipped: API 74 does not exist before 3.7.
- **3.3.2** — the same two operations skipped, for the same reason.

Two earlier failures on these legs — the API 74 `UnsupportedApiVersion` on 3.6.2
and the `UnsupportedFieldVersion { field: "resource_types", version: 0 }` on
3.9.0 and 4.0.0 — were real client bugs, not broker limitations. Both are fixed:
the v0/v1 request shape is handled, and only the two operations a broker cannot
semantically serve are skipped. The suite is verified green on 3.3.2, 3.6.2,
3.9.0 and 4.3.0.

A skip is data, not an excuse: a surface that skips 100% on some release is
exactly the raw material the per-surface floor table needs.

## Golden `ApiVersions` fixtures

Containers prove negotiation works against four releases. The fixtures prove it
without a container at all, in milliseconds, on every `cargo test`.

`kacrab/tests/fixtures/api_versions/` holds byte-for-byte copies of the
`ApiVersions` v3 response frames real brokers sent to kacrab's own wire codec —
including the 4-byte length prefix, exactly as received. Nothing there is
hand-written. Each `.bin` has a `.json` decode summary that the test regenerates
from the bytes on every run and asserts equal, so a summary can never drift from
the frame it describes.

| Fixture | Image | Frame bytes | APIs advertised |
|---|---|---:|---:|
| `kafka-3.3.2` | `bitnamilegacy/kafka:3.3.2` | 419 | 49 |
| `kafka-3.6.2` | `bitnamilegacy/kafka:3.6.2` | 461 | 55 |
| `kafka-3.9.0` | `apache/kafka:3.9.0` | 503 | 61 |
| `kafka-4.0.0` | `apache/kafka:4.0.0` | 547 | 61 |
| `kafka-4.3.0` | `apache/kafka:4.3.0` | 724 | 75 |

Captured 2026-07-28 (3.3.2: 2026-07-29) on `linux/arm64`, one broker at a time;
the exact `docker run` commands are in that directory's `README.md`, along with
the `capture_api_versions_fixture` harness for adding a new release.

`kacrab/tests/api_versions_fixtures.rs` replays each frame through the
production decode path and into `BrokerCapabilities`, asserting: the committed
summary matches the bytes; the frame round-trips through the codec; the
negotiated version for each of seven representative APIs is exactly what that
release should produce; no negotiated version ever exceeds the broker's or the
client's maximum; a caller-supplied ceiling caps negotiation without dropping
below the broker's floor; and APIs the broker does not serve report unavailable
rather than panicking.

The in-process `MockBroker` in `kacrab/tests/wire_session.rs` covers the other
half — the handshake and negotiation as a live conversation rather than a
recorded response.

## Feature floors

Some features need a broker API that simply does not exist on older releases.
kacrab gates these on **what the broker advertises**, never on a release number,
because release numbers are not the truth — see the 3.9 row below. The gates
live in `kacrab/src/consumer/capabilities.rs` and run right after the
coordinator lookup, so the failure arrives before the feature's first RPC rather
than from it.

| Feature | Requires the broker to advertise | Present in practice from |
|---|---|---|
| Producer, classic consumer groups, core admin | APIs that predate 2.4 | 2.4 (the handshake floor) |
| KIP-848 consumer group (`group.protocol=consumer`) | `ConsumerGroupHeartbeat` | 3.9 (early-access v0); v1 from 4.0 |
| Share consumer (KIP-932) | `ShareGroupHeartbeat`, `ShareFetch`, `ShareAcknowledge` | 4.3 per the captured fixtures |

The fixtures are what these rows are read off, and they are stricter than "4.x
only" would suggest:

- **3.6.2** advertises neither `ConsumerGroupHeartbeat` (68) nor `ShareFetch`
  (78). Both negotiate to unavailable.
- **3.9.0** *does* advertise `ConsumerGroupHeartbeat`, but only at v0 — KIP-848
  early access — so negotiation lands on v0 rather than the client's v1. This is
  precisely why the gate refuses to name a release: a rule of "needs 4.0 or
  newer" would turn away a broker that works.
- **4.0.0** still does not advertise `ShareFetch`. KIP-932 stayed behind an
  unstable feature flag until 4.2, so a share consumer on 4.0 must be told the
  API is missing.
- **4.3.0** advertises `ShareFetch` with `min_version: 1` — a client that only
  knew v0 would find the ranges disjoint.

When a gate refuses, the error names the mode that asked *and* the mode that
would have worked on this cluster:

- `group.protocol=consumer` on a broker without `ConsumerGroupHeartbeat` →
  *"set `group.protocol=classic` to use the JoinGroup/SyncGroup protocol this
  broker does serve"*.
- A share group on a broker without the KIP-932 APIs → *"use a `Consumer` with a
  classic or consumer group, or enable share groups on the broker"*.

## What is still open

Honest gaps, so the table above is not read for more than it says:

- **Below 3.3 is accepted but not CI-verified.** The 3.3.2 leg put the first
  real container from the 2.4–3.5 range into the matrix (core tier, nightly);
  2.4 through 3.2 — including 2.8, the last pre-KRaft-default line, which needs
  a ZooKeeper-shaped fixture — are still covered only by negotiation
  unit/fixture tests.
- **The old legs are non-blocking**, pending per-test `#[min_broker(..)]` gating
  and a published per-surface floor table.
- **No cross-client interop suite yet** beyond the byte-level Java oracle
  (`kacrab-protocol/tests/java_interop.rs`) and the compression round-trip that
  decodes batches produced by the Java CLI. A kacrab-producer/Java-consumer
  (and reverse) matrix, and a mixed consumer group, are not written.
- **Above 4.3.0 is untested**, by construction — 4.3.0 is the newest release the
  schemas are generated from. The bidirectional model says a newer broker
  negotiates down; no newer broker has existed to check that against.

See [Verification against real brokers](./verification.md) for the rest of the
real-broker evidence, and [Testing, coverage & CI](./testing-and-ci.md) for how
these suites sit in the wider gate structure.
