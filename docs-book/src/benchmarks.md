# Benchmarks & methodology

The journey's final weighing: what did going native actually buy? Numbers are
only as good as the method behind them — two of this book's performance
war stories (the SSH-tunnel trap, the `getenv` lock) are about the *harness*
lying, not the client — so this chapter gives the headline figures and, more
importantly, how they were measured and what they do *not* claim.

> **Single source of numbers.**
> [`benches/README.md`](https://github.com/pirumu/kacrab/blob/master/benches/README.md)
> is the authority: every table, every caveat, and every reproduction command
> lives there. This chapter and the project README quote it. If a figure here
> disagrees with `benches/README.md`, `benches/README.md` is right and the
> quote is stale — please open an issue.

## The headline

Measured against native Apache Kafka 4.3.0 single-node KRaft on the same machine,
through the public producer API at the **default** Kafka-compatible config
(`acks=all`, `enable.idempotence=true`, no compression):

Medians of 5 interleaved kacrab/Java pairs, 2026-07-27. Both `MB/s` columns come
from the same summary line, computed identically on both sides (Java divides by
`1024 * 1024` and labels it `MB/sec`; kacrab's port matches).

| Metric (5M × 10B, 16 partitions) | kacrab | Java `kafka-producer-perf-test` |
|---|---:|---:|
| Throughput | **5.00M rec/s (47.6 MB/s)** | 3.70M rec/s (35.3 MB/s) |
| Latency avg | 0.61 ms | **0.35 ms** |
| Latency p50 / p95 / p99 | 0 / 5 / 6 ms | **0 / 1 / 2 ms** |
| Latency max | **12 ms** | 127 ms |
| retries / errors | 0 / 0 | 0 / 0 |

| Metric (100K × 10 KiB, 3 partitions, default `batch.size`) | kacrab | Java |
|---|---:|---:|
| Throughput | **49.5K rec/s (483 MB/s)** | 43.0K rec/s (420 MB/s) |
| Latency avg | **40.5 ms** | 43.1 ms |
| Latency p50 / p95 / p99 | **39 / 48** / 85 ms | 41 / 64 / **83** ms |
| Latency max | **92 ms** | 133 ms |

The two scenarios above never come near a broker limit, so no batch is ever
rejected and the split path is untested by them. Driven deliberately — a
one-partition topic with `max.message.bytes=65536` against a producer at
`batch.size=262144`, so every full batch is rejected with `MESSAGE_TOO_LARGE` —
the gap is much wider, because kacrab spends one broker round trip per batch
fewer than Java. Java's first split of an accumulator batch targets `batch.size`,
the size the accumulator already packed the batch to, so it re-sends a single
child holding every record and waits for the broker to reject it again before it
starts halving. kacrab checks the grouping locally and halves on the spot.

| Metric (20K × 4 KiB, 1 partition, `max.message.bytes` < `batch.size`) | kacrab | Java |
|---|---:|---:|
| Throughput | **59.3K rec/s (232 MB/s)** | 26.9K rec/s (105 MB/s) |
| Latency avg | **109 ms** | 194 ms |
| Produce requests | **3,173** | 3,492 |
| Batch splits | **952** | 1,270 |
| retries / errors | 0 / 0 | 0 / 0 |

Before 0.3.0 this workload delivered nothing at all: the split regrouped each
rejected batch into a child of the same size for the broker to reject again, and
every record failed with `DeliveryTimeout`.

| Resource (same 10B workload, `/usr/bin/time -l`, 2026-06-28) | kacrab | Java | Java overhead |
|---|---:|---:|---:|
| Peak RSS | ~68 MiB | ~268 MiB | **~3.9×** |
| Total CPU (user+sys) | ~2.7 s | ~4.1 s | **~1.5×** |

## Where the throughput lead comes from

Throughput here is **broker-bound**: both clients spend most of the run waiting
on `acks=all` round trips, so cheaper per-record CPU barely moves the needle.
kacrab's records/sec edge comes from keeping the broker's write path busier —
a deeper per-partition pipeline plus coalescing one ready batch from every
partition into each produce request (on 10 KiB records, where each batch holds a
single record, that coalescing is the entire difference between ~480 MB/s and
one-record-per-round-trip collapse). The native-vs-JVM win also shows up in
efficiency: ~4× less resident memory (no JVM heap/metaspace) and ~1.5× less CPU
per record. The Java CPU figure also includes one-time JVM startup + JIT warmup
that amortizes over a long-lived producer; the peak-RSS gap is steady-state.

> **Bench against a native broker.** A broker behind a Colima/OrbStack published
> port is reached through an SSH tunnel that roughly triples request RTT — it
> silently caps every number (10 KiB throughput measured ~3× lower through the
> tunnel). And never read env vars on a per-record path in the harness itself:
> macOS `getenv` takes a global libc lock, and one `env::var` call inside the
> record factory cost ~28% of small-record throughput until it was hoisted.

## The latency tradeoff

> **The two clients are not at the same offered load.** Both run flat out, so each
> measures latency at its own saturation point — and kacrab is ~35% further up the
> throughput curve. Pinned to Java's own rate (`KACRAB_BENCH_THROUGHPUT`), kacrab
> wins or ties every latency metric: **0.11 ms avg, 0/1/2 ms p50/p95/p99, 3 ms
> p99.9, 4 ms max** against Java's **0.32 / 0/1/2 / 5 / 131-140** (5M × 10B,
> 16 partitions, interleaved pairs, 2026-07-27) — while keeping the throughput
> headroom on top. Read the tables above as a saturation comparison, not a latency
> one.

Two notes on the shape of the distribution:

- Java's maxima of 126-140 ms are JVM client pauses, not broker pauses: kacrab
  against the same broker in the same interleaved runs stays under 16 ms. kacrab's
  latency band is wider in the body but bounded at the tail; Java's is tighter in
  the body with rare far outliers.
- **A claim previously made here was wrong.** This section stated that
  `max.in.flight=1` drops kacrab's p99 to ~2 ms at unchanged throughput. Measured:
  **260 ms avg / 493 ms p99 at 28% lower throughput.** Depth 1 lets the accumulator
  pile up behind the single busy slot (~7200 records per request instead of
  ~1900). The default depth of 5 is the best setting measured.

## The consumer head-to-head

The consumer benchmark mirrors Java's `kafka-consumer-perf-test.sh` exactly
(fresh group per run, the tool's own props, 100 ms poll slices, the same CSV
columns) against prefilled topics on the same native broker (2026-07-02):

| Metric (5M × 10B, 16 partitions) | kacrab | Java `kafka-consumer-perf-test` |
|---|---:|---:|
| Throughput | ~17.6M rec/s (~168 MB/s) | ~9.3M rec/s (~89 MB/s) |
| Rebalance (join) time | ~8 ms | ~131 ms |
| poll() p50 / p99 / max | ~0.022 / 0.04 / 8 ms | ~0.025 / 0.20 / 111 ms |
| CPU / peak RSS (one run) | ~0.28 s / ~18 MiB | ~2.5 s / ~286 MiB |

| Metric (100K × 10 KiB, 3 partitions) | kacrab | Java |
|---|---:|---:|
| Throughput | ~540K rec/s (~5,277 MB/s) | ~136K rec/s (~1,329 MB/s) |
| poll() p50 / p99 / max | ~0.54 / 0.7 / 4.2 ms | ~1.7 / 4.0 / 108 ms |
| CPU / peak RSS (one run, ~1 GB) | ~0.16 s / ~12 MiB | ~2.8 s / ~230 MiB |

kacrab consumes small records **~1.9× faster** and large records **~4×
faster** than Java at identical defaults, at ~16–20× less memory and ~9–17×
less CPU, with a poll() tail 14–25× lower (Java keeps a slightly tighter
p99.9 on 10 B records: ~1.9 ms vs ~2.5 ms). Three Java-parity mechanisms carry
it, each added after the benchmark exposed its absence: **cross-poll fetch
buffering** (`completedFetches` — before it, every poll re-fetched the response
surplus and 10 B throughput sat at ~132K rec/s), **background prefetch with the
buffered-node gate** (the network thread; without the gate, a fetch listing
only caught-up partitions long-polled `fetch.max.wait.ms` and collapsed
throughput 13×), and **lazy per-batch decode** (`CompletedFetch`'s iterator —
decoding whole blobs up front cost ~536 MiB of allocator churn; per-batch it is
~18 MiB and the p99.9 decode spike halved).

## Micro-benchmarks

Criterion benchmarks against local mock brokers cover the hot paths in
isolation: the accumulator append/drain, the wire request pipeline
(`send_to_broker` req/s), and multi-broker produce dispatch. They catch hot-path
regressions without real broker storage/replication noise.

> **Honesty about units and scope**
>
> kacrab prints payload **MiB**/s; Java's perf tool prints decimal **MB**/s —
> don't compare them as the same unit. And these are five-run smoke measurements
> on a shared host, not release gates. What is *not* measured here — sustained
> soak, cross-DC RTT, memory growth over hours, latency-percentile gates — is
> deliberately scoped in the README's **Production acceptance** plan, not claimed.
