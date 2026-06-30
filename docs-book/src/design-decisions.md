# Design decisions & Java parity

> **Draft**
>
> Outline chapter.

The principles that shape the codebase. Planned coverage:

- **"Java-compatible" means Kafka-protocol-compatible.** The target is the
  Java *client's behavior and wire output*, not a class-for-class port. kacrab
  is outcome-faithful: same property names, same defaults, same algorithms, same
  bytes — but idiomatic Rust underneath.
- **Outcome over mechanism.** Where the runtime models differ — async Tokio
  tasks vs Java's single Sender thread — kacrab keeps the observable outcome
  identical and adapts the mechanism (the `EnqueueSequencer`, the global
  epoch-reset-and-restamp instead of in-place renumbering). See
  [Idempotency](./producer/idempotency.md).
- **Generated, oracle-checked protocol.** No hand-written byte patching; the
  Java client is the external source of truth. See
  [Protocol code generation](./codegen.md).
- **`forbid(unsafe_code)`** workspace-wide, with a strict lint set (clippy
  `pedantic` + `nursery` + selected restriction lints denied).
- **The boundary kacrab won't cross.** JVM-only callback-handler classes cannot
  be loaded in Rust; custom auth uses the native Rust SASL authenticator hook
  instead.
