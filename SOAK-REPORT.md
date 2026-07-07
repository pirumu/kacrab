# Soak report — 2026-07-07 overnight run

First sustained-load + chaos run of the new soak harness
(`benches/src/bin/soak_bench.rs`, `feat/soak-bench`). Target: the 3-broker
KRaft compose (`docker-compose.cluster.yml`, host ports 29092/29094/29096,
apache/kafka:4.3.0, RF 3, `min.insync.replicas=2`) inside a colima VM on an
Apple-silicon host.

**Workload:** paced idempotent producer (`acks=all`, 512 B values, target
1,000 rec/s across 6 partitions) stamping per-partition sequence numbers; a
2-member consumer group (`enable.auto.commit=true`) reading them back through
a continuity tracker that classifies every sequence as consumed, duplicate
(at-least-once re-read), reordered (forward gap later refilled by a racing
handover stream), or **lost** (gap never refilled).

**Chaos:** `docker stop`/`start` of kafka2/kafka3 in rotation every 600 s
(45 s downtime); one consumer closed and recreated every 900 s; a watchdog
that force-bounces all consumers after 90 s of zero progress with backlog.

**Timeline:** planned 6 h; ran 4 h 17 m healthy, then an out-of-scenario
infra failure (see below) cascaded into a terminal client wedge and the run
was stopped at ~4 h 46 m. Raw data: `soak.csv` (10 s samples), chaos/wedge
timeline and stuck-process forensics preserved in the session scratchpad.

## Healthy phase — 4 h 17 m, ~25 broker-kill cycles

| metric | value |
|---|---|
| produced / acked | 15,192,970 / 15,172,636 (rest in flight at cutoff) |
| delivery errors | 333 (0.002%, all inside kill windows) |
| consumed (incl. re-reads) | 15,365,243 |
| duplicates (at-least-once re-reads) | 194,693 (1.28%) |
| reordered (gaps refilled) | 89,730 |
| **unfilled gaps at cutoff** | **2,400 (0.016%)** — finding F2 |
| ack latency, median window p50 / p99 | 13.7 ms / 19.9 ms |
| worst window p99 / worst single ack | 11.1 s / 12.1 s (leader-failover windows) |
| client RSS start / max / end | 17.0 / 37.0 / 13.4 MiB — **no leak** |
| consumer-group wedges | 1 (auto-healed by watchdog bounce) |
| consumer restarts (scheduled + watchdog) | 18 |

Reading: the producer path is genuinely robust — 25 rotating broker kills,
every record either acked or failed loudly, throughput recovered after each
cycle, and memory stayed flat for four hours. The consumer group survived
every scheduled bounce and all but one chaos cycle unaided.

## Cascade phase — infra failure exposes three client weaknesses

At 4 h 17 m **kafka1 died with exit 137 (OOM-killed)** — not part of the
chaos rotation; three 512 MB-heap brokers plus page cache outgrew the
default-sized colima VM. This was an environment failure, but the sequence
it triggered was a legitimate disaster drill: permanent loss of one broker
while the chaos rotation kept killing the other two (transient 1/3-broker
windows), followed 26 minutes later by a manual `docker start kafka1`
restoring the full cluster.

The client never came back:

- Consumption froze; the watchdog's consumer bounces (40 further restarts)
  briefly moved ~165 records after kafka1 returned, then froze again.
- The producer froze at its in-flight cap: 20,334 delivery futures neither
  completed nor failed for **>30 minutes** — with `delivery.timeout.ms` at
  its default 120 s, every one of them should have errored out within
  ~2 minutes.
- Process forensics at kill time: 26 ESTABLISHED TCP connections to the
  brokers (17 to the restarted kafka1) for a topology that needs ~11 — the
  connections reconnected at TCP level, but no requests completed over them.

## Findings queued for investigation

| # | severity | finding | evidence |
|---|---|---|---|
| F1 | high | Consumer group can wedge permanently (every poll `Wire(Timeout)` on a ~45 s cycle) after a coordinator-broker kill + member change; intermittent in compressed-chaos smokes, terminal in the cascade. Client recreation (bounce) does not always heal it. | shakedown run `soak-c`, cascade phase |
| F2 | high | 2,400 sequences (0.016%) were never delivered to any consumer despite the group consuming past them — at-least-once suspect, concentrated around chaos + rebalance windows. Gap-refill tracking rules out the benign reordering explanation for these. | `soak.csv` `open_gaps`, shakedown `soak-d` (239 gaps with fully drained tail) |
| F3 | high | Producer delivery futures stuck indefinitely (>30 min) under prolonged broker loss — `delivery.timeout.ms` (120 s) not enforced on that path — and produce did not resume after full cluster restoration. | cascade phase, stuck-process `lsof`/`sample` |
| F4 | medium | Suspected connection leak across consumer close/recreate cycles: 26 broker connections after 58 restarts vs ~11 expected. `Consumer::close()`/drop may leave wire clients alive. | `lsof` at kill time |
| — | infra | Run the compose VM with ≥6 GB memory for multi-hour 3-broker soaks; kafka1's OOM at 4 h 17 m ended the clean experiment. | `docker ps` exit 137 |

Harness notes for the next run: the naive `expected++`-tracker produced
false "losses" from two consumers' poll streams interleaving at handover —
gap-refill semantics (implemented) are required for a truthful verdict; the
wedge watchdog plus per-run fresh topics are likewise load-bearing.

## Verdict

- **Producer under rotating single-broker failure: PASS**, with excellent
  margins (0.002% errored, all loud; flat RSS).
- **Consumer group under the same chaos: PASS with a caveat** (one
  auto-healed wedge; 0.016% unfilled gaps pending root-cause).
- **Compound failure (broker lost for tens of minutes, then restored):
  FAIL** — F1/F3 mean the client currently requires a process restart to
  recover from a sufficiently long full-partition outage window.

The production-readiness bar set in the README ("measurement under load")
now has data: steady-state and single-fault behavior are strong; the
recovery paths after prolonged outages are where the remaining work lives.
