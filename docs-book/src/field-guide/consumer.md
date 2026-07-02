# Tuning the consumer

Producer tuning is mostly about efficiency; consumer tuning is mostly about
**semantics**. Before touching a fetch knob, decide what a crash mid-batch
should mean — everything else follows from that answer.

## Start from delivery semantics

The committed offset is the group's durable "read up to here" mark. *When*
you commit relative to *when* you process decides your semantics:

| You want | Recipe | Crash mid-batch means |
|---|---|---|
| At-least-once (the default choice) | process, **then** `commit_sync` | reprocess a few records — make handlers idempotent |
| At-most-once | commit **before** processing | lose a few records, never repeat |
| Effectively-once pipeline | transactional producer + `isolation.level=read_committed` | neither, within the pipeline |

- **`enable.auto.commit=true`** (default) commits positions on
  `auto.commit.interval.ms` and once more before each rebalance. It is
  *periodic*, not *transactional*: a crash between commits reprocesses up to
  one interval. Fine when handlers are idempotent; switch it off and commit
  manually when they aren't.
- **`commit_async` is safe to chain in kacrab**: commits apply strictly in
  call order through a single worker queue
  ([how, and why that was worth a bug fix](../consumer/fetching.md)), so a
  slow early commit can never overwrite a later one. Use it on the hot path,
  with one `commit_sync` at shutdown.

## `auto.offset.reset`: choose, don't inherit

This key answers *two* questions — where a **new** group starts, and what to
do when a committed offset **ages out of retention** — so pick it
deliberately:

- `latest` (default): new groups skip history; an aged-out position jumps to
  the live edge, **silently skipping** whatever retention deleted.
- `earliest`: new groups replay history; aged-out positions resume from the
  oldest surviving record — at-least-once friendly.
- `none`: turn the situation into an explicit `NoOffsetForPartition` error —
  right for pipelines where a silent skip or replay is worse than a page.

An out-of-range position is *routine*, not fatal: retention deletion and
seeks race the log every day. kacrab resets exactly the affected partition
and keeps the rest of the poll flowing
([one bad partition doesn't sink the poll](../consumer/fetching.md)). The
practical guard is broker-side: keep retention comfortably longer than your
worst-case consumer downtime, or an outage quietly becomes data loss under
`latest`.

## Group membership: the liveness contract

Two independent timers decide whether you're alive; know which one fired:

- **`session.timeout.ms`** (45 s) vs **`heartbeat.interval.ms`** (3 s) —
  process liveness. Heartbeats run from a background task, so a busy `poll`
  loop does *not* miss them. Keep the ratio ≥3:1; the defaults are right.
- **`max.poll.interval.ms`** (5 min) — *progress* liveness: exceed it between
  polls and the group evicts you even though heartbeats flowed. If your
  handler can be slow, either raise this honestly or shrink the batch with
  **`max.poll.records`** (500 default) so each poll's work fits the budget.
  Size it as `max.poll.interval.ms > max.poll.records × worst-case
  per-record time`, with margin.

The rebalance eviction→reassign→reprocess loop ("rebalance storm") is almost
always this arithmetic being false, not a broker problem.

## Choosing the rebalance protocol

- **`partition.assignment.strategy`** defaults to Java's
  `range, cooperative-sticky` — which negotiates to **eager** `range` in
  steady state. To get incremental rebalancing (nothing stops the world;
  only moved partitions pause), set it to `cooperative-sticky` **alone**.
  The two-round handoff and its never-double-owned invariant are the
  [rebalancing chapter](../consumer/rebalancing.md).
- **`group.protocol=consumer`** (KIP-848, Kafka 4.0+ brokers) moves
  assignment server-side: one heartbeat RPC, incremental reconciliation,
  faster convergence. kacrab supports it fully; prefer it on 4.x clusters
  for new groups.
- **`group.instance.id`** (static membership): set one stable id per pod/
  instance and a restart within `session.timeout.ms` triggers **no rebalance
  at all** — the single cheapest fix for rolling-deploy churn.
- One deliberate non-feature: the consumer is single-owner (`&mut self`, not
  `Sync`) like Java's. Scale with *more consumers in the group*, not by
  sharing one across threads.

## Fetch tuning: batching in reverse

The fetch knobs mirror the producer's linger/batch pair, in reverse:

- **`fetch.min.bytes`** (1) + **`fetch.max.wait.ms`** (500): raise
  `fetch.min.bytes` (e.g. 64 KiB) to make the *broker* batch for you — fewer,
  fuller responses, less client CPU — at up to `fetch.max.wait.ms` of added
  latency on quiet topics. kacrab clamps the broker wait to your `poll`
  timeout, so a short poll stays responsive regardless
  ([fetching](../consumer/fetching.md)).
- **`max.partition.fetch.bytes`** (1 MiB) and **`fetch.max.bytes`** (50 MiB)
  are *ceilings*, not targets — and both yield to make progress: a first
  record larger than the limit is still returned. Raise the per-partition
  cap for large-record topics rather than raising `max.poll.records`.
- **`max.poll.records`** shapes *poll-loop cadence*, not network traffic —
  fetched surplus is buffered across polls and decoded lazily, batch by
  batch, with the next fetch prefetched in the background
  ([the mechanics](../consumer/fetching.md)). Small values keep rebalance
  and shutdown latency low; they do not add RPCs.

Defaults here are genuinely good: the 4× consumer throughput edge in
[the benchmarks](../benchmarks.md) was measured at stock fetch settings.

## Reading transactional data

Set **`isolation.level=read_committed`** to see only committed transactions
(aborted records are filtered; reads stop at the last stable offset). The
cost is added end-to-end latency equal to the producers' commit cadence —
another reason producers should [keep transactions short](./producer.md).

## Watch the right numbers

- **Lag** (`current_lag`, or `kafka-consumer-groups --describe`) is the
  consumer SLO. Alert on *sustained growth*, not absolute values.
- `metrics()` exposes poll/fetch/commit/rebalance counters; a rising
  rebalance count with stable membership is the `max.poll.interval.ms`
  arithmetic failing (above).
- `pause`/`resume` give per-partition back-pressure without leaving the
  group — buffered data survives a pause; heartbeats keep flowing.

## Field notes

| Goal | Change | Leave alone |
|---|---|---|
| Correctness under crashes | manual `commit_sync` after processing; idempotent handlers | — |
| No silent skip/replay | `auto.offset.reset` chosen explicitly (consider `none`) | — |
| Calm rolling deploys | `group.instance.id` per instance; `cooperative-sticky` alone, or `group.protocol=consumer` on 4.x | `session.timeout.ms` |
| Slow handlers | `max.poll.records` down, or `max.poll.interval.ms` honestly up | `heartbeat.interval.ms` |
| Throughput | `fetch.min.bytes` up (broker-side batching) | `max.poll.records` |
| Transactions | `isolation.level=read_committed` | — |

And the habit that prevents the classic outage: whenever you change handler
logic, re-check that `max.poll.records × worst-case handler time` still fits
inside `max.poll.interval.ms`.
