# Cancellation & drop semantics

Async Kafka clients get raced in `tokio::select!` and dropped on shutdown paths.
This chapter is the contract: what each public future does when it is
**cancelled** — dropped before it resolves — and what each client does when it is
**dropped** without `close()`.

None of it is incidental. A Rust client that loses records on a `select!` arm, or
silently discards a buffer on drop, is worse than a blocking one, so every row
below is a deliberate design decision rather than whatever the implementation
happened to do.

## Cancelling a future

| Future | Cancel-safe | What a mid-await drop costs |
| --- | --- | --- |
| `Consumer::poll` | Yes, for records | No fetched record, position, or fetch session is lost. An in-flight `Fetch` stays owned by the consumer and is folded in by the next `poll`. |
| `Producer::send`'s `SendFuture` | Yes | Nothing. The record is already in the accumulator (`send` is a plain `fn`, not `async`) and still delivers; a registered callback still fires. Same semantics as dropping Java's returned `Future`. |
| `Producer::flush` / `close` | No | Dispatch continues, but you lose the guarantee that every prior send completed. Re-`flush` before relying on it. |
| `Consumer::commit_async` | No | If dropped during the coordinator lookup, the commit is never enqueued and the callback never fires. Once enqueued the handoff is synchronous and cannot be cancelled. |
| `Consumer::commit_sync` | No | The `OffsetCommit` may have reached the broker and applied. Treat the offset as indeterminate and re-commit. |
| `ShareConsumer::poll` | No | The `ShareFetch` response is discarded, so records the broker acquired for this member stay locked until the acquisition lock expires (`group.share.record.lock.duration.ms`) and are then redelivered. Nothing is lost; a record can be delivered twice. |
| `ShareConsumer::commit` | No | The `ShareAcknowledge` may have reached the broker and applied. Unapplied acknowledgements are not retried — those records keep their lock and are redelivered. |
| `Producer::init_transactions` | No, but not abandoned | The coordinator round trip runs on its own task, so `InitProducerId` still completes and the producer id/epoch is still installed. You lose only the `Result`. Re-call it: the operation is marked pending, and the retry joins the in-flight one instead of issuing a second. |
| `Producer::commit_transaction` / `abort_transaction` | No, but not abandoned | Same shape: `EndTxn` still reaches the coordinator and the transaction still commits or aborts. A cancelled `commit` is **not** an implicit abort. Re-call *the same* operation to pick the result back up; calling the other one while it is pending returns `ProducerError::InvalidTransactionState`. |
| `Producer::send_offsets_to_transaction` | No, but not abandoned | `AddOffsetsToTxn` + `TxnOffsetCommit` still run to completion on their own task. Re-call it with the same offsets to observe the result. |
| `Admin::*` | No | Admin calls are plain request/response with no client-side state machine: a mid-await drop loses the response and nothing local changes. Whether the broker applied the operation is indeterminate, so re-issue it and rely on the operation being idempotent (or check with the matching `describe_*`). |

`Producer::begin_transaction` is a plain `fn`, not `async` — there is no future to
cancel, so those rows cover the whole exactly-once surface.

### The two caveats on `poll`

`Consumer::poll` is cancel-safe with respect to **records**, which is the
property `select!` users actually need: a cancelled `poll` never drops records on
the floor, and never advances a position past a record you did not receive.

It is not *transactionally* cancel-safe, in two ways:

1. A drop can land mid-rebalance or mid-auto-commit. Neither is corrupted — the
   next `poll` re-drives whatever was interrupted — but the interruption is real.
2. A drop during auto-commit still consumes that `auto.commit.interval.ms`
   window, so the commit slips to the next interval rather than being retried
   immediately.

If neither hazard is acceptable, run `poll` in its own `tokio::spawn` and break
it out with `Consumer::wakeup`, which takes `&self` for exactly this reason. That
is also the shape a Java application would use.

## Dropping a client without closing it

- **`Producer`** — buffered records do not vanish silently. Every incomplete
  delivery resolves as `Err(ProducerError::DeliveryDropped)`, waking pending
  `SendFuture`s and firing registered callbacks with that error. This is stricter
  than Java, where a garbage-collected producer loses buffered records with no
  notification at all. Use `close()` to flush them, or `close_now()` to fail them
  explicitly with `ProducerError::ProducerClosed`.
- **`Consumer`** — the heartbeat, async-commit, and in-flight fetch tasks are
  aborted, so no broker connection is kept alive by a detached task. Nothing is
  committed and the group is not left: the group waits out `session.timeout.ms`
  before rebalancing. Use `close()` to auto-commit and leave the group promptly.
- **`ShareConsumer`** — nothing is acknowledged and no share session is closed,
  so records still under acquisition become re-deliverable when their lock
  expires rather than being silently lost, and the group waits out the session
  timeout before reassigning. Use `close()` to flush pending acknowledgements,
  close the share sessions (which releases the rest immediately instead of at
  lock expiry), and leave the group.

The common thread: kacrab prefers a **loud, typed failure** over a quiet loss.
Dropping a producer mid-flight is a bug in the caller, and the client's job is to
make that bug visible at the point it happens rather than to absorb it.
