# `ApiVersions` golden fixtures

Raw `ApiVersionsResponse` frames captured from **real Kafka brokers**, plus a
decoded JSON summary of each. They back the offline negotiation tests in
`kacrab/tests/api_versions_fixtures.rs`, which run in the default suite in
milliseconds with no container.

Nothing here is hand-written. Every `.bin` is the exact byte sequence a broker
put on the wire; if a broker's answer is surprising, the fixture records the
surprise rather than smoothing it over.

## Files

| File | Contents |
| --- | --- |
| `kafka-<version>.bin` | The complete `ApiVersions` v3 response frame, **including** the 4-byte big-endian length prefix, exactly as received. |
| `kafka-<version>.json` | Decode summary: error code, throttle, feature levels, and every advertised `(api_key, name, min_version, max_version)`. |

The `.json` is not maintained by hand either — `summary_json` in the test file
regenerates it from the `.bin` on every run and asserts equality, so the summary
can never drift from the bytes.

## Provenance

Captured on **2026-07-28** on `linux/arm64` (Docker Desktop, macOS host), one
broker at a time on `127.0.0.1:9092`, each container removed before the next
started.

| Fixture | Image | Frame bytes | APIs advertised |
| --- | --- | --- | --- |
| `kafka-3.6.2` | `bitnamilegacy/kafka:3.6.2` | 461 | 55 |
| `kafka-3.9.0` | `apache/kafka:3.9.0` | 503 | 61 |
| `kafka-4.0.0` | `apache/kafka:4.0.0` | 547 | 61 |
| `kafka-4.3.0` | `apache/kafka:4.3.0` | 724 | 75 |

`apache/kafka` publishes no tag older than 3.7, so 3.6 comes from Bitnami. The
`bitnami/kafka:3.6*` tags were removed from Docker Hub when Bitnami moved its
back-catalogue to the `bitnamilegacy` namespace in 2025 — `bitnamilegacy/kafka:3.6.2`
is the same image under its current name, and it needs Bitnami's `KAFKA_CFG_*`
environment shape rather than the `KAFKA_*` shape `apache/kafka` uses.

### Brokers used

`apache/kafka` (3.7 and newer) — same env shape as `docker-compose.kafka.yml`:

```sh
docker run -d --name kacrab-fixture --hostname kafka -p 9092:9092 \
  -e CLUSTER_ID=4L6g3nShT-eMCtK--X86sw \
  -e KAFKA_NODE_ID=1 \
  -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@kafka:9093 \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT \
  -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
  -e KAFKA_LOG_DIRS=/var/lib/kafka/data \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
  -e KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_SHARE_COORDINATOR_STATE_TOPIC_MIN_ISR=1 \
  apache/kafka:4.3.0
```

(The two `SHARE_COORDINATOR` lines only apply from 4.1; they are harmless
elsewhere and were omitted for 3.9/4.0.)

`bitnamilegacy/kafka` (3.6) — Bitnami's `KAFKA_CFG_*` shape:

```sh
docker run -d --name kacrab-fixture --hostname kafka -p 9092:9092 \
  -e ALLOW_PLAINTEXT_LISTENER=yes \
  -e KAFKA_CFG_NODE_ID=1 \
  -e KAFKA_CFG_PROCESS_ROLES=broker,controller \
  -e KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=1@kafka:9093 \
  -e KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT \
  -e KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
  -e KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  -e KAFKA_CFG_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
  -e KAFKA_CFG_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_CFG_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
  -e KAFKA_CFG_TRANSACTION_STATE_LOG_MIN_ISR=1 \
  bitnamilegacy/kafka:3.6.2
```

## Capturing a new broker release

The harness is the `#[ignore]`d `capture_api_versions_fixture` test. It speaks
the same v3 `ApiVersions` handshake `wire/broker.rs` does, using kacrab's own
frame codec, and writes both files for whatever broker is listening.

```sh
# 1. boot ONE broker on 9092 (see the commands above), wait for
#    "Kafka Server started" in `docker logs`
# 2. capture
KACRAB_FIXTURE_BROKER=4.4.0 \
  cargo test -p kacrab --test api_versions_fixtures -- --ignored capture
# 3. docker rm -f kacrab-fixture   (the port is shared; one broker at a time)
```

| Variable | Default | Meaning |
| --- | --- | --- |
| `KACRAB_FIXTURE_BROKER` | *(required)* | Release label; becomes the `kafka-<label>.{bin,json}` file stem. |
| `KACRAB_FIXTURE_BOOTSTRAP` | `127.0.0.1:9092` | Broker endpoint to capture from. |

Then add a `Fixture` row to `FIXTURES` in
`kacrab/tests/api_versions_fixtures.rs` with the versions the new broker
actually negotiates, and a row to the provenance table above.

## What the captures show

Read off these exact bytes — the fixtures, not folklore, are the source:

- **3.6.2** advertises neither `ConsumerGroupHeartbeat` (68) nor `ShareFetch`
  (78): both negotiate to "unavailable".
- **3.9.0** *does* advertise `ConsumerGroupHeartbeat`, but only at v0 (KIP-848
  early access), so negotiation lands on v0 rather than the client's v1.
- **4.0.0** still does not advertise `ShareFetch`: KIP-932 stayed behind an
  unstable feature flag until 4.2, so a share consumer on 4.0 must be told the
  API is missing.
- **4.3.0** advertises `ShareFetch` with `min_version: 1` — a client that only
  knows v0 would find the ranges disjoint.
