# Foundations every client shares

Every leg of the journey ended with configuration lessons. This part of the
book collects them: not a reference dump of every key (the
[config surface](../codegen.md) is generated from Kafka's own metadata and
documented on docs.rs), but the settings that *decide outcomes* — and the
mistakes we made or watched brokers punish. This chapter covers what producer,
consumer, and admin all share; the next two tune each surface.

## One config surface, Java names

kacrab deliberately uses the **exact Kafka property names and defaults** the
Java client uses. Anything you learned from Kafka's documentation, an ops
runbook, or an existing `producer.properties` transfers verbatim:

```rust
let producer = Producer::builder()
    .set("bootstrap.servers", "kafka-1:9092,kafka-2:9092")
    .set("compression.type", "zstd")
    .build()?;

// or load the same .properties file your Java services use
let consumer = Consumer::from_properties("consumer.properties")?;
```

Values are parsed and validated into typed configs at build time — an invalid
value fails construction with the key and the accepted values, not at first
use. The typed surface is drift-checked against the upstream Kafka catalog in
CI, so a key documented here is a key that actually exists.

> **Best practice: keep configs boring**
>
> The single most reliable Kafka tuning advice is to change *few* defaults and
> know *why* for each one. Kafka's defaults (which kacrab inherits) already
> encode a decade of production learning — `acks=all`, idempotence on,
> exponential jittered backoff. Every override below states the reason to
> make; if you can't state one, don't override.

## Start with the features

kacrab ships `default = []` — a bare `kacrab = "0.1"` compiles nothing usable.
Enable every surface you call:

```toml
[dependencies]
kacrab = { version = "0.1", features = ["producer", "consumer", "admin"] }
```

- `consumer` implies the `compression` meta-feature: a consumer must decode
  whatever codec the producer chose, so all four codecs come along.
- `compression` (and `zstd` alone) needs a **C compiler at build time**
  (libzstd). For a pure-Rust build take only `gzip` + `snappy` + `lz4` — see
  [Traveling light](../compression.md).
- `gssapi` (Kerberos) is opt-in and links libgssapi.

## `bootstrap.servers`: plural, on purpose

Bootstrap servers are only used to *discover* the cluster — after the first
metadata response, the client talks to the brokers the cluster advertises.
But that first contact is a single point of failure you control:

- **List at least two brokers** (three across racks/AZs if you have them). A
  one-entry list means one rebooted broker blocks every fresh client start.
- The entries don't need to be the whole cluster — they need to be *alive at
  startup*.
- If **all** known brokers become unreachable later,
  `metadata.recovery.strategy=rebootstrap` (the default) goes back to the
  bootstrap list instead of spinning on dead cached endpoints.

> **What the broker taught us: DNS is part of your config**
>
> A coordinator advertised as `localhost` resolved to the IPv6 loopback first
> — where nothing listened — and a pinned connection hung forever. kacrab now
> re-resolves broker hostnames on every connect, tries all returned addresses
> IPv4-first, and honours `client.dns.lookup=use_all_dns_ips`. The portable
> lesson: advertise brokers by names that resolve to addresses clients can
> actually reach, and prefer `use_all_dns_ips` so a multi-A-record entry
> fails over instead of failing.

## Name yourself: `client.id`

Set `client.id` to something meaningful (`orders-api-producer`, not the
default empty string). It appears in broker request logs, quota enforcement,
and kacrab's own `ApiVersions` identification — when an SRE asks "who is
hammering partition 12?", this is the answer. One name per logical
application, shared across instances; use `group.instance.id` (consumer) for
per-instance identity.

## The timeout ladder

Kafka timeouts nest, and misordering them produces "impossible" symptoms.
From inner to outer:

| Key | Default | Bounds |
|---|---|---|
| `request.timeout.ms` | 30 s | one request/response on the wire |
| `delivery.timeout.ms` | 120 s | a produced record's whole life: batching + all retries |
| `default.api.timeout.ms` | 60 s | one admin/consumer API call, all its requests |
| `max.block.ms` | 60 s | how long `send()` may wait for buffer memory/metadata |

- Keep `delivery.timeout.ms ≥ linger.ms + request.timeout.ms` — kacrab (like
  Java) validates this, because a delivery window smaller than a single
  attempt can never succeed.
- **Bound retries with time, not counts.** Leave `retries` at its effectively
  infinite default and shrink `delivery.timeout.ms` to your real freshness
  requirement. A time budget degrades gracefully under a slow broker; a retry
  count is either too small during an incident or meaninglessly large.
- Don't raise `request.timeout.ms` to "fix" timeouts. A request that needs
  more than 30 s is telling you about broker load, an unreachable leader, or
  (historically, in this client's own past) an auth failure being silently
  retried — see below.

## Backoff: already jittered, leave it

`retry.backoff.ms` → `retry.backoff.max.ms` and `reconnect.backoff.ms` →
`reconnect.backoff.max.ms` implement exponential backoff *with jitter*,
resetting on success — the same curve as Java. The defaults are right for
almost everyone; the one case for raising `reconnect.backoff.max.ms` is a
very large fleet whose simultaneous reconnects can thundering-herd a
recovering broker.

## Security: fail fast, and mind the one hard boundary

`security.protocol` picks the transport/auth pairing (`PLAINTEXT`, `SSL`,
`SASL_PLAINTEXT`, `SASL_SSL`); `sasl.mechanism` picks PLAIN, SCRAM-SHA-256/512,
OAUTHBEARER, or GSSAPI; `ssl.truststore.*`/`ssl.keystore.*` load PEM, JKS, or
PKCS12 material with the Java key names. Every combination is
[verified against real brokers](../verification.md).

Two field lessons:

- **A rejected credential is not a transient error.** kacrab fails
  authentication *fast* with the broker's real reason. If you see
  `SaslAuthentication` at startup, fix the credential — no timeout tuning
  will help. (Early kacrab retried these under backoff and surfaced "request
  timed out" 30 seconds later; the [security chapter](../security.md) tells
  that story.)
- **`sasl.jaas.config` class names are the one thing that cannot cross.**
  JVM login modules and callback handlers can't load in a Rust process.
  Static PLAIN/SCRAM credentials configure directly; custom token flows use
  the native authenticator hook (`sasl_client_authenticator`) instead of a
  class name. See [Design decisions](../design-decisions.md).

## Metadata freshness

`metadata.max.age.ms` (5 min default) caps how stale routing metadata may
get; leader changes are additionally learned *reactively* — from error
responses and connection drops — so you rarely need to lower it. If your
cluster reassigns partitions frequently and you observe brief retry bursts
after moves, lower it toward 60 s rather than disabling anything.

## Field notes

- Set two things on day one: a plural `bootstrap.servers` and a meaningful
  `client.id`. Half of production debugging is knowing who is talking to
  whom.
- Express reliability budgets in **time** (`delivery.timeout.ms`,
  `default.api.timeout.ms`), not attempt counts.
- Treat auth failures as configuration bugs, not weather. They fail fast by
  design.
- Prefer `client.dns.lookup=use_all_dns_ips` in any environment with
  multi-homed or containerized brokers.
