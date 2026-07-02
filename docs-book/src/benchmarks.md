# Benchmarks & methodology

The journey's final weighing: what did going native actually buy? Numbers are
only as good as the method behind them — two of this book's performance
war stories (the SSH-tunnel trap, the `getenv` lock) are about the *harness*
lying, not the client — so this chapter gives the headline figures and, more
importantly, how they were measured and what they do *not* claim. Full
reproduction lives in
[`benches/README.md`](https://github.com/pirumu/kacrab/blob/master/benches/README.md).

## The headline

Measured against native Apache Kafka 4.3.0 single-node KRaft on the same machine,
through the public producer API at the **default** Kafka-compatible config
(`acks=all`, `enable.idempotence=true`, no compression):

| Metric (5M × 10B, 16 partitions, 2026-07-02) | kacrab | Java `kafka-producer-perf-test` |
|---|---:|---:|
| Throughput | ~4.79–4.86M rec/s (≈46.3 MiB/s) | 3.80–3.84M rec/s |
| Latency avg | ~1.7 ms | ~0.38 ms |
| Latency p99 | ~13 ms | ~3 ms |
| retries / errors | 0 / 0 | 0 / 0 |

| Metric (100K × 10 KiB, 3 partitions, default `batch.size`) | kacrab | Java |
|---|---:|---:|
| Throughput | ~542–570 MB/s (55.5–58.4K rec/s) | 417–453 MB/s (42.7–46.4K rec/s) |
| Latency avg / p99 | ~36 ms / ~78 ms | ~43 ms / ~92 ms |

| Resource (same 10B workload, `/usr/bin/time -l`, 2026-06-28) | kacrab | Java | Java overhead |
|---|---:|---:|---:|
| Peak RSS | ~68 MiB | ~268 MiB | **~3.9×** |
| Total CPU (user+sys) | ~2.7 s | ~4.1 s | **~1.5×** |

## Where the +25–28% comes from

Throughput here is **broker-bound**: both clients spend most of the run waiting
on `acks=all` round trips, so cheaper per-record CPU barely moves the needle.
kacrab's records/sec edge comes from keeping the broker's write path busier —
a deeper per-partition pipeline plus coalescing one ready batch from every
partition into each produce request (on 10 KiB records, where each batch holds a
single record, that coalescing is the entire difference between ~540 MB/s and
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

Java keeps a lower typical latency; kacrab trades it for pipeline depth.

- At `max.in.flight=5` kacrab fills the per-partition pipeline (higher p99 on a
  single low-RTT broker, where the extra depth only adds queue latency). At
  `max.in.flight=1` its p99 drops to ~2 ms at the same throughput.
- Depth pays off the other way under a broker pause: at depth 5 a GC/fsync pause
  on one in-flight request lets the others drain (p99.9 ~10 ms); at depth 1 the
  single slot blocks (p99.9 ~100 ms). The gap shrinks in production — broker off
  the client machine, real network RTT.

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
