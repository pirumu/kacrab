# Failure modes

> **Draft**
>
> Outline chapter; the idempotent recovery details are in
> [Idempotency & transactions](./producer/idempotency.md).

How kacrab behaves when things go wrong — the part that separates a demo from a
client you can run. Planned coverage:

- **Retryable vs fatal** — the classification that decides whether an error
  backs off and retries or fails fast. Auth/TLS handshake failures and a failed
  server signature are fatal (see [Security](./security.md)); `NotLeader`,
  throttling, and connection drops are retryable.

  | Error | Class |
  |---|---|
  | `NOT_LEADER_FOR_PARTITION` | retryable, re-route |
  | connection reset / request timeout | retryable, **ambiguous** → epoch bump |
  | `SaslAuthentication` / `TlsHandshake` | **fatal**, fail fast |
  | `MESSAGE_TOO_LARGE` | split & requeue, or fail |

- **Ambiguous loss → epoch bump** — covered in
  [Idempotency](./producer/idempotency.md).
- **Leadership change under load** — the burst-wedge bug and its fix: a
  wire-failure retry invalidates stale leader metadata so co-batched healthy
  partitions don't wedge. Verified on a 3-broker cluster.
- **Delivery timeout** — bounded across retries by `delivery.timeout.ms`; the
  error names the batch that actually expired first.
