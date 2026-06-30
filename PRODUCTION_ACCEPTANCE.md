# Production-acceptance plan (multi-broker hardening)

Status of the remaining `[ ]` items under **Wire** and **Producer** in the
README. The functional multi-broker work is done and verified against a real
3-broker KRaft cluster: multi-broker dispatch routes to every broker's leaders,
and a broker loss re-routes affected partitions to their new leaders without
wedging co-batched ones (`kacrab/tests/real_kafka_cluster.rs`,
`docker-compose.cluster.yml`).

What remains is **measurement and acceptance under load**, not correctness. None
of it is a quick `[x]` — each needs dedicated infrastructure, a time budget, and
in some cases an SLO threshold that is a product decision, not something the
implementation can assert for you. This document scopes each item so it can be
picked up deliberately rather than rushed.

> These are intentionally separate from functional verification. A few minutes
> of `docker compose` proves a path *works*; proving it *holds up* needs hours
> of load, a tuned network, and agreed pass/fail gates.

## B1 — Sustained multi-broker stress

**Goal:** confirm the multi-broker dispatch path stays correct and bounded under
high, continuous load across all brokers (no unbounded memory growth, no stuck
partitions, no reordering/duplication with idempotence on).

**Why not done:** the existing benches are short single-shot runs on a single
node. Multi-broker behavior under sustained pressure (queue depth, in-flight
caps, backpressure, metadata churn) is unmeasured over time.

**Proposed approach:**
- Drive `producer_kafka_bench` (or a new long-run harness) against
  `docker-compose.cluster.yml` for a fixed duration (e.g. 1–4h) at a target
  rate, fanning records across all 6 partitions (leaders spread via a PREFERRED
  election).
- Mix in periodic faults: rolling broker restarts, leader elections, and a
  partition-reassignment, so dispatch is re-routing continuously.
- Assert: zero delivery failures (acks=all + idempotence), `retries` bounded,
  buffered bytes return to ~0 between bursts, and broker-side log
  end-to-end record counts match what was sent.

**Decisions needed:** target rate, record size, duration, fault cadence.

**Acceptance:** no lost/duplicated/reordered records over the full run; steady-
state memory and in-flight counts; no permanently stuck partition.

## B2 — Cross-DC / high-RTT coverage

**Goal:** verify behavior when broker links have real latency, jitter, and loss
(the dispatch/retry/timeout tuning is currently only exercised at ~0 RTT).

**Why not done:** co-located brokers on one machine have sub-millisecond RTT, so
timeout/backoff/in-flight interactions that only appear at 50–200 ms RTT are
untested. This needs network emulation, not just more brokers.

**Proposed approach:**
- Add `tc netem` (delay + jitter + small loss) on the broker containers'
  interfaces, or run brokers behind a latency-injecting proxy
  (e.g. `toxiproxy`), to emulate inter-DC links.
- Re-run B1's workload at representative RTTs (e.g. 50 ms, 150 ms) and confirm:
  throughput degrades gracefully (pipelining keeps up via `max.in.flight`),
  `request.timeout.ms` / `delivery.timeout.ms` are not tripped spuriously, and
  retries/backoff behave under jitter and loss.
- Confirm a high-RTT leader change still recovers within `delivery.timeout.ms`.

**Decisions needed:** RTT/jitter/loss profiles to target; whether `toxiproxy`
(portable, scriptable) or `tc netem` (closer to the kernel path) is preferred.

**Acceptance:** correct delivery and bounded retries at each profile; no
spurious timeouts attributable to the client rather than the emulated link.

## B3 — Memory soak

**Goal:** prove there is no leak or unbounded growth over a long run (buffer
pool, in-flight maps, idempotent state, metadata cache).

**Why not done:** runs so far are short; slow growth (per-connection,
per-leader-change, per-metadata-refresh) would not show up.

**Proposed approach:**
- Run B1's workload for an extended period (e.g. 8–24h) with RSS sampled
  periodically and the buffer-pool / in-flight gauges scraped from the producer
  metrics.
- Include churn (reconnects, leader changes, topic metadata refreshes) so any
  per-event allocation that is never freed accumulates visibly.
- Optionally run under a leak detector / heap profiler for a shorter window.

**Decisions needed:** soak duration; acceptable RSS ceiling/slope.

**Acceptance:** RSS plateaus (no upward trend) after warm-up; buffer-pool and
in-flight gauges return to baseline between bursts.

## B4 — Latency-percentile gates

**Goal:** turn latency from an anecdote into a gated metric (p50/p99/p999
end-to-end produce latency) so regressions are caught.

**Why not done:** benches report throughput and a memory/CPU comparison, but
there is no percentile measurement harness and — more importantly — **no agreed
SLO thresholds**. A "gate" needs target numbers, which are a product call.

**Proposed approach:**
- Extend the bench harness to record per-record send→ack latency and emit
  p50/p99/p999 (HdrHistogram-style), under both single-node and multi-broker.
- Compare against the Java client on the same hardware/workload to set realistic
  targets (the README already notes Java keeps a lower tail latency — quantify
  it).
- Wire the percentiles into CI as a soft gate first (report + alert on
  regression), then a hard gate once thresholds are agreed.

**Decisions needed:** the p99/p999 targets (absolute and/or relative-to-Java),
the workload they apply to, and soft-vs-hard gating in CI.

**Acceptance:** percentiles measured and reported per run; CI flags a regression
beyond the agreed threshold.

## Shared prerequisites

- A longer-lived cluster than the throwaway compose (or the compose with
  `KAFKA_HEAP_OPTS` raised and data volumes sized for a multi-hour run).
- A machine/runner that can sustain the target rate without itself becoming the
  bottleneck (co-located brokers share CPU with the client — for real numbers,
  brokers and client should be on separate hosts).
- Network emulation tooling for B2 (`toxiproxy` or `tc netem`).
- Agreed thresholds for B3/B4 before they can be "gates" rather than reports.
