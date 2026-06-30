# Failure modes

A producer that works on a healthy cluster is a demo. A producer you can run is
one that does the *right thing* when a broker dies mid-flight, a credential is
wrong, a leader moves under load, or a request is lost with no answer. This
chapter is the catalogue of "what goes wrong and what kacrab does".

## Retryable vs fatal

The first decision on any error is whether retrying could possibly help. Getting
this wrong in either direction is a bug: retrying a fatal error wastes the whole
`request.timeout.ms` budget; failing fast on a transient one drops a recoverable
request.

| Error | Class | Action |
|---|---|---|
| `NOT_LEADER_FOR_PARTITION`, `LEADER_NOT_AVAILABLE` | retryable (definitive) | invalidate leader, re-route, retry **same** sequence |
| throttling, transient broker errors | retryable | back off, retry |
| connection reset / request timeout (no response) | retryable (**ambiguous**) | retry, and on final timeout bump the epoch |
| `MESSAGE_TOO_LARGE` | special | split the batch and requeue, or fail if unsplittable |
| `SaslAuthentication` / `SaslHandshake` / `TlsHandshake` | **fatal** | fail fast with the broker's reason |
| `InvalidSaslConfig` / `UnsupportedSaslMechanism` / `UnsupportedTlsOption` | **fatal** | fail fast |
| failed SCRAM server-signature | **fatal** | fail fast |

The fatal SASL/TLS rows are the ones that, before they were fixed, looped under
reconnect backoff until `request.timeout.ms` and surfaced as "request timed
out" — see [Security](./security.md).

## Ambiguous loss → epoch bump

The subtle one. A connection drops with no broker response: the records **may**
have been written. Blindly replaying risks a duplicate; not replaying risks a
drop. Kafka resolves it by **bumping the producer epoch**, fencing the old
in-flight state, and restarting sequences. kacrab bumps only on *ambiguous*
losses (no response) — a definitive rejection like `NotLeader` re-routes and
retries the same sequence, no bump. The full mechanism is in
[Idempotency & transactions](./producer/idempotency.md).

## Leadership change under load — the burst wedge

A real bug, found by [verifying against a 3-broker cluster](./verification.md):
a broker died while a burst of records to several partitions was in flight.

```mermaid
flowchart TD
  K["broker dies"] --> W["wire error on its partitions"]
  W --> X{retry handler<br/>invalidates metadata?}
  X -- "no (the bug)" --> Y["re-route to the DEAD broker again<br/>→ whole batch group wedges<br/>→ even healthy partitions time out"]
  X -- "yes (the fix)" --> Z["re-fetch metadata, re-route to new leaders<br/>→ all partitions deliver"]
```

Because a dispatch retries its batch group as one unit, the partitions whose
leaders were *alive* wedged alongside the dead-broker ones — a 6-partition burst
went `0/6` (60 s timeout). The fix: a wire-failure retry now invalidates the
affected partitions before retrying, so the next attempt re-fetches metadata and
re-routes — mirroring Java's `NetworkClient` requesting a metadata update on
server disconnect. After it: `6/6`.

## Delivery timeout

`delivery.timeout.ms` is the hard ceiling on a record's lifetime across all
retries. When it expires, the delivery fails with a `DeliveryTimeout` naming the
batch that actually expired first (not an arbitrary one) — a small accuracy fix
that fell out of the same investigation.

## Back-pressure, not unbounded growth

Under load the producer applies back-pressure rather than buffering without
bound: `send` blocks up to `max.block.ms` when `buffer.memory` is exhausted, and
the wire pipeline rejects with `Backpressure` when every in-flight slot is full.
The goal is bounded memory under sustained overload — the property the
[production-acceptance soak](./benchmarks.md) is meant to confirm over hours.
