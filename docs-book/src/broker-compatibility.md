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
| **CI-exercised** | **3.6.2 · 3.9.0 · 4.0.0 · 4.3.0** | The broker-version matrix in `.github/workflows/real-broker.yml`, running the real suites against real containers. |
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
implemented and covered by unit and fixture tests — but those releases are **not
in the CI matrix**. Treat them as *accepted, not yet CI-verified*.

## The CI matrix

`.github/workflows/real-broker.yml` runs the `#[ignore]`d `real_kafka_*` suites
against live containers, matrixed over four broker releases. Each leg sets
`KAFKA_IMAGE` for the compose file, a seam every compose file already had
(`${KAFKA_IMAGE:-apache/kafka:4.3.0}`).

| Leg | Image | Compose file | Gating |
|---|---|---|---|
| 4.3.0 | `apache/kafka:4.3.0` | `docker-compose.kafka.yml` | blocking |
| 4.0.0 | `apache/kafka:4.0.0` | `docker-compose.kafka.yml` | blocking |
| 3.9.0 | `apache/kafka:3.9.0` | `docker-compose.kafka.yml` | blocking |
| 3.6.2 | `bitnamilegacy/kafka:3.6.2` | `docker-compose.kafka-bitnami.yml` | blocking |

**Every leg runs every suite, and every leg blocks.** The workflow carries no
test-name filters and no suite tiers: a test that needs an API an old broker
does not serve declares that itself (see
[per-test capability gating](#per-test-capability-gating) below) and self-skips
with a named `SKIPPED` line, so a red leg is always a regression signal rather
than noise. The `admin-extended` and `cluster` jobs stay pinned to 4.3.0 for
reasons of their own: the admin-extended fixture depends on 4.3.0 broker
defaults (share and streams features formatted on), so pointing it at an older
image produces a broker missing the features the suite is about rather than an
older-broker signal; and the cluster suite is about dispatch and leader
failover, which is orthogonal to version negotiation.

**When each leg runs.** The suites run `--test-threads=1` — they share one broker
and name topics with millisecond nonces, so parallel runs collide — which makes
each leg a full serial pass and four legs four times the wall clock. Pull
requests therefore get two legs, newest and oldest, where a negotiation break
shows up first. The complete matrix runs on push-to-master, on the nightly
schedule, and on demand.

**Reading the results.** Every leg writes a per-suite outcome table into the
GitHub step summary, so which surfaces survive which broker is visible without
opening four job logs.

## Per-test capability gating

A version-sensitive real-broker test opens with a one-line guard from
`kacrab/tests/common/broker_capability.rs`:

```rust,ignore
common::require_broker_api!(ApiKey::ConsumerGroupHeartbeat => 1);
```

The guard sends one `ApiVersions` request to the connected broker over kacrab's
own wire client and asks whether the broker *advertises* each named API at or
above the paired version. When it does not, the guard prints a named skip line
and returns from the test:

```text
SKIPPED: real_kafka_consumer::real_kafka_consumer_protocol_kip848 needs
ConsumerGroupHeartbeat >= v1 (the broker does not advertise the API)
```

Like the [runtime feature gates](#feature-floors), the guard judges on what the
broker advertises, never on a release number. It also deliberately ignores what
the *client* can negotiate: if a broker advertises an API and kacrab then fails
to speak it, that is a client bug the test must surface, not a reason to skip.

### The per-surface floor table

Derived from the guards; this is the agreed answer to "should this test pass on
3.6?". *runs* means the surface must be green on that leg — a failure blocks.

| Surface | Guard (broker must advertise) | 3.6.2 | 3.9.0 | 4.0.0 | 4.3.0 |
|---|---|---|---|---|---|
| Producer (`real_kafka_producer`), transactions (`real_kafka_producer_txn`) | — (classic APIs, pre-2.4) | runs | runs | runs | runs |
| Classic consumer (`real_kafka_consumer` except KIP-848, `real_kafka_consumer_ops`) | — (classic group protocol) | runs | runs | runs | runs |
| `real_kafka_consumer_protocol_kip848` | `ConsumerGroupHeartbeat` ≥ v1 | skips | skips | runs | runs |
| Share consumer (`real_kafka_share_consumer`, all tests) | `ShareGroupHeartbeat`, `ShareFetch`, `ShareAcknowledge` ≥ v1 | skips | skips | skips | runs |
| Compression round-trips (`real_kafka_compression`, both directions) | — (record batches are stored unchanged) | runs | runs | runs | runs |
| Admin smoke (`real_kafka_admin_smoke`) | per-op capability log (see below) | runs¹ | runs¹ | runs¹ | runs |
| Admin behavior (`real_kafka_admin_behavior`) | — (classic admin + KRaft describes) | runs | runs | runs | runs |

¹ All ops run except the per-op skips listed in
[Capability-aware admin smoke](#capability-aware-admin-smoke).

Two rows deserve their footnotes spelled out:

- **KIP-848 requires v1, not v0.** Kafka 3.9 advertises
  `ConsumerGroupHeartbeat` at v0 — KIP-848 early access, behind a broker
  feature flag the CI fixtures do not enable. The *client* gate accepts a
  v0-only broker (an operator may have enabled early access), but the *test*
  cannot pass against the stock 3.9 fixture, so its floor is v1.
- **Admin behavior has no guard on purpose.** Its cluster/quorum/features
  assertions describe any single-broker KRaft fixture rather than one
  release's exact shape; the suite is verified green against
  `bitnamilegacy/kafka:3.6.2`.

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

Two earlier failures on these legs — the API 74 `UnsupportedApiVersion` on 3.6.2
and the `UnsupportedFieldVersion { field: "resource_types", version: 0 }` on
3.9.0 and 4.0.0 — were real client bugs, not broker limitations. Both are fixed:
the v0/v1 request shape is handled, and only the two operations a broker cannot
semantically serve are skipped. The suite is verified green on 3.6.2, 3.9.0 and
4.3.0.

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
| `kafka-3.6.2` | `bitnamilegacy/kafka:3.6.2` | 461 | 55 |
| `kafka-3.9.0` | `apache/kafka:3.9.0` | 503 | 61 |
| `kafka-4.0.0` | `apache/kafka:4.0.0` | 547 | 61 |
| `kafka-4.3.0` | `apache/kafka:4.3.0` | 724 | 75 |

Captured 2026-07-28 on `linux/arm64`, one broker at a time; the exact
`docker run` commands are in that directory's `README.md`, along with the
`capture_api_versions_fixture` harness for adding a new release.

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

- **2.4 through 3.5 are accepted but not CI-verified.** Negotiation and
  version-aware request construction are implemented and unit/fixture-tested;
  no container in CI runs those releases.
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
