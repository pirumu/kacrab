# Benchmarks & methodology

> **Draft**
>
> Outline chapter; reproduction details live in `benches/README.md`.

Numbers, and how they were measured. Planned coverage:

- **The headline** — on a single-node broker at the default `acks=all` +
  idempotence config, kacrab holds throughput parity with the Java client
  (~4.7M × 10B records/sec) while using ~4× less peak memory and ~1.5× less CPU.
  Java keeps a lower tail latency.
- **Why parity, not a blowout** — throughput here is **broker-bound** (both
  clients spend the run waiting on `acks=all` round-trips), so the client
  language barely moves the throughput needle. The real native-vs-JVM win is in
  memory and CPU-per-record.
- **The latency tradeoff** — pipeline depth (`max.in.flight`) trades tail
  latency for broker-pause resilience; the gap shrinks off the client machine
  with real RTT.
- **Methodology honesty** — `producer_kafka_bench` ports Kafka Java's
  `ProducerPerformance.Stats` sampling and reports the same window/total shape;
  micro-benchmarks (Criterion) cover the accumulator, wire pipeline, and
  dispatch hot paths. What is *not* measured (sustained soak, cross-DC RTT,
  latency-percentile gates) is scoped in the README's Production acceptance plan.
> **Warning**
>
> MiB/sec (kacrab) and MB/sec (Java perf tool) are different units — don't compare
> them directly.
