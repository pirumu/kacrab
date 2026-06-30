# The wire layer

> **Draft**
>
> This chapter is an outline; the flagship deep dives are
> [Idempotency](./producer/idempotency.md) and [Security](./security.md).

The async transport that every higher layer rides. Planned coverage:

- **Per-broker session tasks** — one Tokio task per broker connection, fed by an
  mpsc command queue; `ApiVersions` capability negotiation on connect.
- **The request pipeline** — bounded in-flight requests with a fixed-slot
  correlation store (no per-request hashmap), request timeouts, and
  connection-closed cleanup.
- **Reader/writer split** — coalesced writes through a `BufWriter`; the reader
  task completes responses directly.
- **Metadata** — cluster/topic metadata fetch, leader-change invalidation, and
  rebootstrap.
- **Reconnect & backoff** — exponential backoff with jitter, reset on a
  successful connection; the fatal-vs-retryable classification (see
  [Security](./security.md) and [Failure modes](./failure-modes.md)).
