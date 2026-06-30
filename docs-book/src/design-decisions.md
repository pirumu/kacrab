# Design decisions & Java parity

A handful of principles shape every file in kacrab. They explain why the code
looks the way it does — and why some things that *could* be simpler aren't.

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

## The boundary kacrab won't cross

JVM-only callback-handler and login-module classes cannot be loaded in a Rust
process — that is a hard boundary, not a missing feature. Custom authentication
uses the native Rust SASL authenticator hook
(`ProducerBuilder::sasl_client_authenticator`) instead of a `sasl.jaas.config`
class name.
