# Design decisions & Java parity

Every expedition carries a compass. A handful of principles were fixed before
the first line of code and shape every file in kacrab — they explain why the
code looks the way it does, why some things that *could* be simpler aren't,
and how each fork in the road in Parts I–V was decided.

## "Java-compatible" means Kafka-protocol-compatible

The target is the **behavior and wire output of the Java client**, not a
class-for-class port. Concretely:

- The config surface uses the **same property names and defaults**
  (`acks`, `enable.idempotence`, `compression.type`, `sasl.*`, `ssl.*`, …).
- The bytes on the wire are the Java client's bytes — guaranteed for the things
  that must be byte-exact (murmur2, CRC32C, varint/zigzag, record-batch v2) by
  the [oracle matrix](./codegen.md).
- The algorithms are the *real* Java algorithms (the idempotent
  `inflightBatchesBySequence` / `firstInFlightSequence` / `maybeResolveSequences`
  machinery), not a simplified approximation.

What it is **not**: a translation of Java's class hierarchy, threading model, or
internal APIs. kacrab is idiomatic Rust underneath.

## Outcome over mechanism

Where the runtime models genuinely differ, kacrab keeps the *observable outcome*
identical and adapts the mechanism:

- Java orders enqueues with a **single Sender thread**; kacrab dispatches on
  concurrent Tokio tasks and reconstructs that order with the
  [`EnqueueSequencer`](./producer/idempotency.md).
- Java **renumbers in-flight batches in place** on an epoch bump because one
  thread owns them all; kacrab's tasks can't reach into a sibling's batches, so a
  bump does a **global epoch reset + re-stamp** — different mechanism, identical
  bytes on the wire (a fresh epoch, sequences from zero).

The test is always: *would the broker, or a Java consumer, be able to tell?* If
not, the Rust-idiomatic mechanism wins.

## Generate and verify, don't hand-write and hope

The wire types are [generated](./codegen.md) from the upstream schemas and
checked against the Java client; the security, compression, and multi-broker
paths are [verified against real brokers](./verification.md), not just
self-consistent unit tests. The recurring theme — from the byte-level oracle to
the docker-compose integration tests — is **an external source of truth**, because
a system that only checks itself can be consistently wrong.

## Safety and strictness, by default

- **`unsafe_code` is forbidden** workspace-wide.
- The lint set is strict: clippy `pedantic` + `nursery` + `cargo` denied, plus a
  curated list of restriction lints (`expect_used`, `unwrap_used`,
  `indexing_slicing`, `arithmetic_side_effects`, …) that must be justified with a
  reason when allowed.

## "Pure Rust", precisely

kacrab is a **native-Rust implementation of the Kafka client** — the protocol,
wire framing, producer, idempotency, partitioning, and the pure-Rust codecs are
Rust, with `unsafe_code` forbidden in kacrab's own crates, and it does **not**
wrap `librdkafka`. That is the claim worth making.

It is **not** a fully C-free dependency tree, and it is honest to say so:

| Component | Backend |
|---|---|
| Kafka protocol / wire / producer logic | pure Rust |
| `gzip` / `snappy` / `lz4` codecs | pure Rust (`flate2` / `snap` / `lz4_flex`) |
| CRC32C, murmur2, varint | pure Rust |
| **TLS crypto** — `aws-lc-rs-tls` (optional) | C / assembly (AWS-LC) |
| **TLS crypto** — `pure-rust-tls` (optional) | Rust + some vendored BoringSSL C (`ring`) |
| `zstd` (optional) | C (`zstd-sys` / libzstd) |
| `lz4-hc` (optional) | C (liblz4) |
| `gssapi` (optional) | C (libgssapi) |

The TLS crypto provider used to be unavoidable — `rustls` defaulted to
`aws-lc-rs`, so every build compiled `aws-lc-sys` even when it never opened a TLS
connection. Since 0.4 the backend is an explicit feature and no longer implied:

- `aws-lc-rs-tls` — the default provider, and what CI exercises.
- `pure-rust-tls` — puts `rustls` on `ring`, which removes `aws-lc-sys` from the
  tree entirely. `make check-pure-rust-tls` asserts the dependency is *gone*
  rather than trusting that the feature compiles. Note that `ring` still vendors
  some C from BoringSSL, so this is *no aws-lc*, not *zero C*.
- neither — a `PLAINTEXT`-only build compiles no crypto provider, and a TLS
  connection then fails at config validation with a message naming both features.

`pure-rust-tls` deliberately leaves `jsonwebtoken` out, which means locally
signing an `OAUTHBEARER` JWT assertion needs `aws-lc-rs-tls`. The reason is that
`jsonwebtoken`'s pure-Rust backend depends on `rsa`, which carries
[RUSTSEC-2023-0071](https://rustsec.org/advisories/RUSTSEC-2023-0071) — key
recovery through a timing sidechannel, with no fixed release. Trading a C
dependency for an unpatched sidechannel is not what a "smaller trusted C surface"
feature is for, so `rsa` is banned by the same CI check. The other three
`OAUTHBEARER` token sources — JAAS option, token file, and HTTP token endpoint —
work identically in both builds.

So the smallest tree today is `pure-rust-tls` with only the
`gzip`/`snappy`/`lz4` codecs — no `aws-lc-sys`, no `zstd`, `lz4-hc`, or `gssapi`.

## The boundary kacrab won't cross

JVM-only callback-handler and login-module classes cannot be loaded in a Rust
process — that is a hard boundary, not a missing feature. Custom authentication
uses the native Rust SASL authenticator hook
(`ProducerBuilder::sasl_client_authenticator`) instead of a `sasl.jaas.config`
class name.
