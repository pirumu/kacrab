# Benchmarks & methodology

Numbers are only as good as the method behind them. This chapter gives the
headline figures and — more importantly — how they were measured and what they do
*not* claim. Full reproduction lives in
[`benches/README.md`](https://github.com/pirumu/kacrab/blob/master/benches/README.md).

## The headline

Measured against native Apache Kafka 4.3.0 single-node KRaft on the same machine,
through the public producer API at the **default** Kafka-compatible config
(`acks=all`, `enable.idempotence=true`, no compression):

| Metric (5M × 10B, 16 partitions) | kacrab | Java `kafka-producer-perf-test` |
|---|---:|---:|
| Throughput | ~4.70M rec/s (≈44.8 MiB/s) | 3.6–4.2M rec/s |
| Latency avg | ~1.7 ms | 0.27–0.35 ms |
| Latency p99 | ~15 ms | 1–2 ms |
| retries / errors | 0 / 0 | 0 / 0 |

| Resource (same run, `/usr/bin/time -l`) | kacrab | Java | Java overhead |
|---|---:|---:|---:|
| Peak RSS | ~68 MiB | ~268 MiB | **~3.9×** |
| Total CPU (user+sys) | ~2.7 s | ~4.1 s | **~1.5×** |

## Why parity, not a blowout

Throughput here is **broker-bound**: both clients spend most of the run waiting
on `acks=all` round trips, so the client language barely moves the throughput
needle. The real native-vs-JVM win shows up where it can: ~4× less resident
memory (no JVM heap/metaspace) and ~1.5× less CPU per record. The Java CPU figure
also includes one-time JVM startup + JIT warmup that amortizes over a long-lived
producer; the peak-RSS gap is steady-state.

## The latency tradeoff

Java keeps a lower typical latency; kacrab trades it for pipeline depth.

- At `max.in.flight=5` kacrab fills the per-partition pipeline (higher p99 on a
  single low-RTT broker, where the extra depth only adds queue latency). At
  `max.in.flight=1` its p99 drops to ~2 ms at the same throughput.
- Depth pays off the other way under a broker pause: at depth 5 a GC/fsync pause
  on one in-flight request lets the others drain (p99.9 ~10 ms); at depth 1 the
  single slot blocks (p99.9 ~100 ms). The gap shrinks in production — broker off
  the client machine, real network RTT.

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
