# kacrab-benches

Internal benchmark suite for the kacrab workspace. **Not published.**

Benchmark targets here are measurement hooks for the wire/producer/consumer
work. They are local checkpoints, not release-throughput guarantees.

Headline results (2026-07-02, native Apache Kafka 4.3.0, default Kafka-compatible
configs on both sides — full tables under
[Real-Kafka Producer Baselines](#real-kafka-producer-baselines) and
[Real-Kafka Consumer Baselines](#real-kafka-consumer-baselines)):

- **Producer** (re-measured 2026-07-27): **+35%** records/sec over Java
  `kafka-producer-perf-test` on 5M x 10 B and **+15%** on 100K x 10 KiB, at
  `acks=all` + idempotence, with ~3.9x less peak RSS and ~1.5x less CPU. Pinned to
  Java's own rate, kacrab's latency is lower at every percentile. On the
  `MESSAGE_TOO_LARGE` batch-split path — a topic whose `max.message.bytes` is
  below the producer's `batch.size` — kacrab is **2.2x** Java's throughput at
  **1.8x lower latency**, because it does not spend a broker round trip on the
  first split, which regroups a batch into a single child of its own size. See
  [Batch-split probe](#batch-split-probe).
- **Consumer**: ~1.9x (10 B records) to ~4x (10 KiB records) Java's
  `kafka-consumer-perf-test` throughput, at ~16-20x less peak RSS and ~9-17x
  less CPU, with group joins ~15x faster.

## Layout

Criterion micro-benchmarks (`benches/`):

- `producer_accumulator` - per-topic-partition append/drain micro-benchmarks.
- `wire_pipeline` - `WireClient::send_to_broker` request/sec against local mock
  brokers.
- `producer_dispatcher` - accumulator plus multi-broker produce dispatch over
  local mock leaders.

Executable smoke benchmarks (`src/bin/`, print scenario summaries; the real
Kafka ones need a running broker):

- `producer_mock_bench` - mock broker smoke benchmark.
- `producer_kafka_bench` - real Kafka producer benchmark mirroring Java's
  `kafka-producer-perf-test.sh`.
- `consumer_kafka_bench` - real Kafka consumer benchmark mirroring Java's
  `kafka-consumer-perf-test.sh`.

Java comparison wrappers (`scripts/`):

- `producer_default_matrix.sh` / `consumer_default_matrix.sh` - run the Java
  perf tools 5x per scenario with effective-config snapshots, invoked by the
  `bench-kafka-java-default` / `bench-kafka-consumer-java-default` Makefile
  targets.
- `producer_counter_metrics.py` - parses `--print-metrics` output into the
  compact counter lines; unit-tested via `make test-bench-scripts`.

## Running

```bash
cargo bench -p kacrab-benches                                    # all Criterion benches
cargo bench -p kacrab-benches --bench producer_accumulator
cargo bench -p kacrab-benches --bench wire_pipeline
cargo bench -p kacrab-benches --bench producer_dispatcher
cargo run -p kacrab-benches --release --bin producer_mock_bench
cargo run -p kacrab-benches --release --bin producer_kafka_bench
KACRAB_ONLY_10KIB=1 cargo run -p kacrab-benches --release --bin producer_kafka_bench
KACRAB_BENCH_PREFILL=1 cargo run -p kacrab-benches --release --bin consumer_kafka_bench
make bench-kafka bench-kafka-java-default
make bench-kafka-consumer bench-kafka-consumer-java-default
```

## Broker Setup

**Benchmark against a native broker, not one behind a Docker-VM port forward.**
A Colima/OrbStack/Docker Desktop published port on macOS is a VM tunnel that
roughly triples request RTT and silently caps every number (10 KiB producer
throughput measured ~3x lower through the tunnel). The Makefile drives a native
Kafka install for exactly this reason:

```bash
make kafka-start              # kafka-server-start.sh -daemon (KAFKA_BIN + KAFKA_SERVER_PROPERTIES)
make kafka-topic-recreate     # fresh KACRAB_BENCH_TOPIC before a comparison run
make kafka-topic-describe
make kafka-data-du            # largest broker data dirs
make kafka-topic-delete
make kafka-stop
make kafka-topic-prune-delete-dirs   # rm stale *-delete dirs; stop Kafka first
```

Defaults: `KAFKA_BIN=$HOME/.local/share/kacrab-kafka/current/bin`,
`KACRAB_BOOTSTRAP=127.0.0.1:9092`, `KACRAB_BENCH_TOPIC=kacrab-bench`,
`KACRAB_PARTITIONS=3`, `KACRAB_REPLICATION_FACTOR=1` — all overridable, e.g.
`KACRAB_PARTITIONS=16 KACRAB_BENCH_TOPIC=kacrab-16p make kafka-topic-create`.

`bench-kafka` and `bench-kafka-java-default` both create `KACRAB_BENCH_TOPIC`
if it is missing. After large benchmark passes, delete the topic and stop Kafka
before pruning `*-delete` directories; this keeps local broker data from
silently growing across parity runs.

The root `docker-compose.kafka.yml` still works for functional (non-benchmark)
runs against `apache/kafka:4.3.0` — it exposes Kafka on `localhost:9092` and
creates `kacrab-bench` with 3 partitions (override with `KAFKA_HOST_PORT`,
`KAFKA_BENCH_TOPIC`, `KAFKA_BENCH_PARTITIONS`, `KAFKA_IMAGE`):

```bash
docker compose -f docker-compose.kafka.yml up -d
docker compose -f docker-compose.kafka.yml down
```

## Producer Benchmark (`producer_kafka_bench`)

Mirrors Java's `ProducerPerformance` (`kafka-producer-perf-test.sh`): a port of
its `Stats` class drives the same window sampling, total summary line, and
callback-success-only accounting, with latency measured from just before each
send to callback completion like the Java tool. The bench uses the public
`Producer::builder().set(...).build()` API, warms up metadata, the broker
session, and one outer API chunk outside the measured window, then sends
records through the Java-style public producer path: one synchronous
`send_with_callback` call per record (the send API is a plain `fn`, appending
inline when the partition resolves synchronously), while the accumulator/sender
groups records into Produce requests internally. On `Backpressure` the loop waits
for the drain and retries the same record (closed-loop), which caps the send rate
at the real drain rate instead of flooding open-loop.

The per-record latency timestamp is taken **once per record, before the first
attempt** — it is not reset by a backpressure retry. That is what makes the
comparison honest: Java never returns "buffer full" to the caller at all. Its
`send()` blocks inside the measured window (`KafkaProducer.java:1029` →
`BufferPool.allocate` → `BufferPool.java:149`, bounded by `max.block.ms`), and
`ProducerPerformance` captures `sendStartMs` once (`ProducerPerformance.java:102`)
and sends each record exactly once (`:91`, no retry loop). So Java's latency
includes buffer-wait time, and kacrab's must too. Restarting the clock per attempt
would discard the most time from exactly the records that waited longest, hiding
the p99/p99.9/max tail where congestion is worst.

> **Re-measured 2026-07-27 on the corrected bench.** The clock-reset fix turned out
> not to move these numbers: both scenarios report `requeues=0` and
> `in_flight_stalls=0`, i.e. the producer buffer never actually filled, because
> throughput is bounded by `acks=all` round trips rather than by buffer capacity.
> A re-run of the 100K x 10 KiB scenario gave ~35.8 ms avg against the ~36 ms in the
> table. The rows stand.
>
> **Read the latency rows against the throughput rows, though.** Both clients run
> flat out here, so each sits at its own saturation point — and kacrab is pushing
> ~35% more throughput than Java. Deeper queues at a higher offered load are not a
> client cost. Pinned to Java's own rate with `KACRAB_BENCH_THROUGHPUT`, kacrab wins
> or ties every latency metric; see [Matched-load latency](#matched-load-latency).

By default the binary sets only `bootstrap.servers` and `client.id` and relies
on the producer's normal Kafka-compatible defaults (`acks=all`,
`enable.idempotence=true`, no compression).

The default parity pass runs two fixed scenarios (5M x 10 B and 100K x 10 KiB),
5 runs per scenario, printing effective config snapshots before each measured
run plus a five-run `rust average counters` line in the same compact counter
schema as the per-run output. The Java wrapper parses
`kafka-producer-perf-test.sh --print-metrics` into per-run and five-run average
counter lines of the same shape. The two sides do not emit the same field set:
Rust prints 14 fields, the Java wrapper 10, so only the fields present on both
lines can be compared. `batch_splits` is one of them — it is a real both-sides
comparison, sourced from Java's `producer-metrics:batch-split-total` and from
kacrab's matching `MESSAGE_TOO_LARGE` record-batch split counter (the
`max.request.size` request-grouping splits are a separate, kacrab-only
`request_splits` column). Java producer-perf does not expose exact record-batch
counts, so `record_batches`, `records_per_batch_avg`, `in_flight_stalls` and
`request_splits` are labeled `not_exposed_by_producer_perf` — do not treat those
fields as parity proof.

Knobs (all read from the environment, so set them inline before `cargo run`):

- `KACRAB_BOOTSTRAP` — broker address (default `127.0.0.1:9092`).
- `KACRAB_BENCH_TOPIC` — topic (default `kacrab-bench`).
- `KACRAB_BENCH_ACKS1=1` — switch to `acks=1` + `enable.idempotence=false` (the
  relaxed comparison config); the default is `acks=all` + idempotence on.
- `KACRAB_BENCH_BATCH_SIZE=N` — override `batch.size` (probe whether throughput
  is round-trip / pipelining bound).
- `KACRAB_BENCH_LINGER_MS=N` — override `linger.ms` (isolate whether
  large-record throughput is linger-bound).
- `KACRAB_BENCH_BUFFER_MEMORY=N` — override `buffer.memory` (a huge buffer
  removes append backpressure, so the run measures pure drain).
- `KACRAB_BENCH_MAX_REQUEST_SIZE=N` — override `max.request.size`, lifting the
  1 MiB default so large-record runs with a bigger `batch.size` do not trip
  `RecordTooLarge` on coalesced requests.
- `KACRAB_BENCH_NO_ADAPTIVE=1` — set
  `partitioner.adaptive.partitioning.enable=false` (uniform round-robin sticky
  spread).
- `KACRAB_BENCH_SPREAD=N` — bypass the sticky partitioner with explicit
  round-robin over N partitions (isolate concurrency-bound throughput).
- `KACRAB_BENCH_SEND_CONCURRENCY=N` — number of concurrent send tasks sharing
  one `Producer` via `Arc` (default `1`), exercising the thread-safe `send(&self)`
  surface.
- `KACRAB_BENCH_CURRENT_THREAD=1` — force the single-thread Tokio runtime
  (default: multi-thread); `KACRAB_BENCH_WORKERS=N` — worker threads for the
  multi-thread runtime (default `4`).
- `KACRAB_BENCH_NO_METRICS=1` — disable the producer accounting metrics
  (Produce requests, records, retries, errors, requeues, fill ratio); enabled
  by default.
- `KACRAB_ONLY_10B=1` / `KACRAB_ONLY_10KIB=1` — run a single scenario.
- `KACRAB_BENCH_MESSAGES=N` — cap the record count: alone it runs only the 10 B
  scenario with N records; combined with `KACRAB_ONLY_10KIB=1` it caps the
  10 KiB scenario.
- `KACRAB_BENCH_RUNS=N` — number of runs per scenario (default 5).
- `KACRAB_BENCH_SPLIT_PROBE=1` — opt-in `MESSAGE_TOO_LARGE` split probe: replaces
  both default scenarios with a single 20,000 x 4 KiB run and sets
  `batch.size=262144` plus `max.request.size` (kacrab's own default, 1 MiB), so
  every full batch overflows a topic created with `max.message.bytes=65536` (see
  the probe section below). Off by default; the default `cargo run` pass and the
  default 5-run matrix are unchanged.
- `KACRAB_BENCH_PRINT_SPLIT_PROBE_CONFIG=1` — print the resolved probe sizing as
  `KEY=VALUE` lines and exit without running anything. This is how
  `producer_default_matrix.sh` learns the sizing, so the Java pass and the probe
  topic cannot drift from the kacrab pass; the same call also rejects an override
  that would stop the probe from probing.
- `KACRAB_BENCH_SYNC_SEND=1` — legacy flag: the per-record path is always the
  synchronous Java-style send now; the flag only prints the sync-now
  buffer-spin counter after the run.
- `KACRAB_BENCH_API` — accepted for old scripts but a no-op; every value
  resolves to the Java-style per-record public API.

### Batch-split probe

The two default parity scenarios (10 B and 10 KiB records) never come near any
broker limit, so no batch is ever rejected with `MESSAGE_TOO_LARGE` and both
sides print `batch_splits=0` — two zeros agreeing, which proves nothing. The
opt-in probe drives the split path for real:

```bash
make bench-kafka-split-probe
```

That target runs `benches/scripts/producer_default_matrix.sh --split-probe`
(also reachable as `KACRAB_BENCH_SPLIT_PROBE=1 benches/scripts/producer_default_matrix.sh`).
It creates `kacrab-bench-split-probe` with
`kafka-topics.sh --config max.message.bytes=65536`, then **verifies that the value
actually binds**: `--create --if-not-exists` is a silent no-op on a topic that
survived an earlier run, so a topic created with a different limit would keep the
old one and the probe would measure a limit nobody declared. The script reads the
effective value back with `kafka-configs.sh --describe --all`; on a mismatch it
reconciles the surviving topic in place with `kafka-configs.sh --alter --add-config`
(it never deletes your data), re-reads, and aborts with an explicit error if the
value still does not bind. It then runs one kacrab pass
(`KACRAB_BENCH_SPLIT_PROBE=1`, `KACRAB_BENCH_RUNS=1`) and one
`kafka-producer-perf-test.sh` pass **against that same topic**, and prints both
counter lines together. Pointing the Java pass at the normal bench topic instead
would make the two `batch_splits` values incomparable, so the script always
overrides the topic for both sides. Overridable: `KACRAB_SPLIT_PROBE_TOPIC`,
`KACRAB_SPLIT_PROBE_PARTITIONS`, `KACRAB_SPLIT_PROBE_REPLICATION_FACTOR`, and
`KAFKA_CONFIGS` (path to `kafka-configs.sh`, defaulted next to `kafka-topics.sh`).

The record count, record size, `batch.size`, `max.request.size` and the probe
topic's `max.message.bytes` are **not** declared by the script: it reads them from
`KACRAB_BENCH_PRINT_SPLIT_PROBE_CONFIG=1 cargo run --bin producer_kafka_bench`, so
the kacrab pass, the Java pass and the topic are configured from one source. The
sizing knobs that remain are read by the binary, must be `export`ed so the script's
`cargo run` inherits them, and are validated against the constraints below —
`export KACRAB_SPLIT_PROBE_MAX_MESSAGE_BYTES=1024` fails the run with an explicit
error instead of silently leaving nothing to split:

- `KACRAB_SPLIT_PROBE_MAX_MESSAGE_BYTES` — the probe topic's `max.message.bytes`;
  must stay above the record size and below `batch.size`.
- `KACRAB_BENCH_BATCH_SIZE` — must stay above the topic limit, hold more than one
  record, and stay below `max.request.size`.
- `KACRAB_BENCH_MAX_REQUEST_SIZE` — must stay above `batch.size`.

The sizing is the probe, and all three constraints have to hold at once:

- **4 KiB records**, far below the 64 KiB topic limit. A record that exceeds the
  limit on its own is *unsplittable* — kacrab returns `None` from
  `split_for_retry_with_compression_ratio` when `records.len() <= 1`, mirroring
  Java's `batch.recordCount > 1` guard, and the send fails terminally instead of
  splitting. The probe must build multi-record oversize batches.
- **`batch.size=262144`**, 4x the topic limit, so every full batch is rejected.
- **`max.request.size` at kacrab's own 1 MiB default**, comfortably above the
  256 KiB batch, so the *broker* limit binds first. If the client limit bound first
  the producer would fail locally with `RecordTooLarge` and the broker would never
  see the batch. The probe reads that default out of the effective producer config
  and sets it explicitly, so the value the invariant is checked against is the one
  the producer runs with.

Both sides are configured identically: the Rust bench sets `batch.size` and
`max.request.size` from the resolved probe config, and the Java pass gets the same
two values via `--command-property` after reading them back from the binary.

The `effective producer config:` line the Rust pass prints once per run reports the
config that actually binds — kacrab's defaults with every benchmark override already
applied — so under the probe it shows `batch.size=262144`, not the `16384` default.
It is formatted from the same override list `build_producer` applies, so the two
cannot drift; it is printed before the send loop starts and adds no per-record work.

A passing comparison is **both sides reporting a non-zero `batch_splits` that
converges**, not an equal one. The re-split geometry is the same —
`max(largest record, estimated batch size / 2)`, as Java 4.2.0+ does in
`RecordAccumulator.java:507-540` — but kacrab does not pay for Java's first
round. Java's first split of an accumulator batch targets `batch.size`, the size
the accumulator already packed the batch to, so it re-sends an identical single
child and waits for the broker to reject it again before it starts halving.
kacrab checks the grouping locally, sees it produced one child holding every
record, and halves on the spot. On the sizing above that is one broker round trip
saved per batch: **952 splits for kacrab against Java's 1270**, and 3,173 produce
requests against 3,492.

kacrab's count running *away* while Java's converges would still be a real defect
— that is a split handing back a child of the parent's size — so watch the
direction, not the equality.

Also compare `retries` and `errors`, which must both be `0`. The split path
re-enqueues batches that already hold an idempotent sequence, so a defect in the
in-flight sequence bookkeeping shows up here as `OutOfOrderSequenceNumber`
responses long before it shows up in the default scenarios.

Last measured on this workload (20,000 x 4 KiB, one partition; medians of 5
interleaved kacrab/Java pairs, the probe topic recreated before every pass):

| producer | throughput | avg latency | retries | errors | batch_splits |
| --- | ---: | ---: | ---: | ---: | ---: |
| kacrab | **59.3K rec/s (232 MB/s)** | **109 ms** | 0 | 0 | 952 |
| Java | 26.9K rec/s (105 MB/s) | 194 ms | 0 | 0 | 1270 |

kacrab was ahead in all 5 pairs; its slowest round (39.1K rec/s) still beat
Java's fastest (30.8K rec/s). Both sides warm up over the first rounds — the
spread is 39.1–63.3K for kacrab and 25.6–30.8K for Java — so compare pairs, not
absolute single runs.

Like every other real-broker path in this repo the probe is run manually; it is
not wired into `make test`.

The public API hot path is allocation-conscious rather than magically
wire-zero-copy: payloads are cloned as `Bytes` handles and topics shared as
`Arc<str>`, so input data is not copied per message. Kafka Produce still
requires serialized record batches and request frames (size fields,
record-batch CRCs) on the wire, so the client must materialize encoded bytes
before writing to the socket; `check.crcs` only skips consumer-side
verification in Java, not producer-side CRC generation.

## Consumer Benchmark (`consumer_kafka_bench`)

Mirrors Java's `ConsumerPerformance` (`kafka-consumer-perf-test.sh`) run for
run: a fresh group id per run, `auto.offset.reset=earliest`, the tool's own
props (`max.partition.fetch.bytes=1MiB`, `receive.buffer.bytes=2MiB`,
`check.crcs=false`), 100 ms poll slices until the expected record count, a 10 s
record-fetch timeout, and the same final CSV columns. Both sides read prefilled
topics (`KACRAB_BENCH_PREFILL=1` on first use writes the scenario records
through the kacrab producer), so every measured run consumes identical,
page-cache-warm broker data. Note: if the broker's log retention is short
(e.g. `log.retention.hours=1`), prefilled topics expire — re-prefill before
measuring. `make bench-kafka-consumer` and
`make bench-kafka-consumer-java-default` run the pair; the Java wrapper prints
an effective-config snapshot per run like the producer matrix does.

Knobs (all read once at startup — nothing in the poll loop touches the
environment):

- `KACRAB_BOOTSTRAP`, `KACRAB_BENCH_RUNS`, `KACRAB_ONLY_10B`,
  `KACRAB_ONLY_10KIB`, `KACRAB_BENCH_MESSAGES` — as in the producer bench.
- `KACRAB_BENCH_TOPIC` — overrides both scenario topics; defaults are
  `kacrab-bench` for 10 B and `kacrab-bench-10k` for 10 KiB.
- `KACRAB_BENCH_GROUP_PROTOCOL` — `classic` (default) or `consumer` (KIP-848).
- `KACRAB_BENCH_ASSIGN=1` + `KACRAB_BENCH_PARTITIONS=N` — manual-assign mode
  over partitions `0..N` (no group membership, auto-commit off), isolating the
  pure fetch path; Java's tool has no equivalent mode.
- `KACRAB_BENCH_MAX_POLL_RECORDS`, `KACRAB_BENCH_FETCH_SIZE`,
  `KACRAB_BENCH_FETCH_MAX_BYTES`, `KACRAB_BENCH_SOCKET_BUFFER`,
  `KACRAB_BENCH_CHECK_CRCS`, `KACRAB_BENCH_FROM_LATEST`,
  `KACRAB_BENCH_TIMEOUT_MS` — consumer/tool prop overrides.
- `KACRAB_BENCH_PREFILL=1` — write the scenario records before measuring.
- `KACRAB_BENCH_CURRENT_THREAD=1` / `KACRAB_BENCH_WORKERS=N` — Tokio runtime
  shape, as in the producer bench.

## A/B Discipline

Every number below is a *comparison*, and comparisons are where benchmarking
goes wrong. These rules are not general advice — each one was paid for during
0.4.0 prep, and together they caught a **fabricated −11% consumer regression**
that three separate runs had already "confirmed" before anyone controlled for
the harness.

- **Run an A/A control first.** Measure the same build against itself, twice,
  through the same path you plan to use for A/B. Whatever spread that produces
  is your noise floor, and no A/B delta smaller than it means anything. The −11%
  above evaporated the moment an A/A control was run: the floor was ±1.7%, and
  the "regression" was the harness, not the code.
- **All arms come from ONE executable path.** Not "the same binary rebuilt", not
  "the same command with a different flag that changes which code path runs" —
  one path, with the variable under test as the only thing that differs.
  Different entry points have different startup costs, different allocator warm
  up, and different inlining, and every one of those lands on the arm you did
  not expect.
- **Randomize or pad the environment block across runs.** The process
  environment sits below the stack, so its size shifts stack alignment for the
  whole process. During this cycle **one byte moved a microbenchmark result by
  2.4×** — the same code, the same input, one extra character in an unrelated
  environment variable. If the arms of your A/B were launched from shells whose
  environments differ in size, you are measuring alignment.
- **Gate on a quiet host.** No editor indexing, no container pulls, no other
  benchmark, no laptop on battery. Check before the run, not after the result
  looks interesting.
- **Prefer workloads long enough to dominate startup.** A 3M-record prefill beats
  a 150K one, not because throughput changes, but because at 150K the fixed
  costs — connection setup, metadata, first-batch allocation, JIT on the Java
  side — are a large enough share of the total that they swamp the difference you
  are trying to see.
- **A criterion micro delta is a hypothesis, not a finding.** Before believing
  one, run a one-variable causal experiment: change exactly the thing you think
  is responsible, predict the direction and rough size in advance, and check. If
  the prediction misses, the delta was measuring something else — usually one of
  the four items above.

The producer and consumer baselines in this file were re-measured under these
rules on 2026-07-27 and re-verified at 0.4.0: producer within ±1.5% over four
interleaved real-broker pairs, consumer inside the ±1.7% A/A noise floor, and
the accumulator microbenchmark back at baseline.

## Real-Kafka Producer Baselines

Measured 2026-07-02 against native Apache Kafka 4.3.0 single-node KRaft on the
same machine (`127.0.0.1:9092`), through the public producer API at the
**default Kafka-compatible config** (`acks=all`, `enable.idempotence=true`), no
compression. Client and broker share the host (no CPU pinning or page-cache
isolation), so treat these as local checkpoints, not production acceptance
numbers.

> **Latency rows in this section are stale.** They were measured before the
> per-record latency clock was fixed to survive backpressure retries (see the
> Producer Benchmark section above), so kacrab's latency is understated. Every
> other column — throughput, byte rate, retries/errors, CPU, RSS — is unaffected.

Benchmark host:

- MacBook Pro `Mac15,6`, Apple M3 Pro (11-core CPU: 5 performance, 6
  efficiency), 18 GB unified memory.

### Throughput + latency (5M x 10B, 16 partitions, `kacrab-16p`)

Reproduce:

```bash
# kacrab
KACRAB_BENCH_TOPIC=kacrab-16p \
  cargo run -p kacrab-benches --release --bin producer_kafka_bench

# Java, same broker/topic/config
kafka-producer-perf-test.sh --topic kacrab-16p --num-records 5000000 \
  --record-size 10 --throughput -1 --producer-props \
  bootstrap.servers=127.0.0.1:9092 acks=all enable.idempotence=true
```

Medians of 5 interleaved kacrab/Java pairs, 2026-07-27, on the corrected bench and
with the co-located head-batch sweep in place. Both `MB/sec` columns come from the
same summary line and are computed identically on both sides.

| Metric | kacrab | Java `kafka-producer-perf-test` |
| --- | ---: | ---: |
| Throughput | **5.00M rec/sec (47.6 MB/sec)** | 3.70M rec/sec (35.3 MB/sec) |
| Latency avg | 0.61 ms | **0.35 ms** |
| Latency p50 / p95 / p99 | 0 / 5 / 6 ms | **0 / 1 / 2 ms** |
| Latency max | **12 ms** | 127 ms |
| retries / errors | 0 / 0 | 0 / 0 |

Previous revision of this table (2026-07-02, before the sweep) read
~4.79-4.86M rec/sec at ~1.7 ms avg / ~13 ms p99 — the sweep raised throughput and
cut latency at the same time.

kacrab wins throughput (about +35% over Java on this workload) while staying fully
idempotent-correct. Java shows lower typical latency in this table, but the two
sides are not at the same offered load — see
[Matched-load latency](#matched-load-latency), where kacrab wins once both are
pinned to the same rate.

- **Broker-pause resilience.** The co-located single-node JVM broker pauses
  periodically (GC/fsync); Java sees it too (max latency spiked to 129 ms in the
  same runs, and 131-140 ms in the 2026-07-27 re-runs, against kacrab's 4-16 ms on
  the same broker). At depth 5 a pause on one in-flight request lets the others
  drain; at depth 1 the single slot blocks.
- **Pipeline depth — an earlier claim here was wrong.** This section used to state
  that `max.in.flight=1` brings kacrab's p99 to ~2 ms at unchanged throughput.
  Measured with `KACRAB_BENCH_MAX_IN_FLIGHT=1` (5M x 10 B, 16 partitions,
  2026-07-27): **260 ms avg / 493 ms p99 at 34.3 MB/sec** — 28% *below* the depth-5
  throughput, not equal to it. Depth 1 lets the accumulator pile up while the
  single slot is busy, so each request carries ~7200 records instead of ~1900.
  Depth 2 is unstable run to run (0.84 ms and 9.89 ms avg on consecutive runs).
  The default depth of 5 is the best of the three.

### Matched-load latency

Both clients above run unthrottled, so each measures latency at *its own*
saturation point — and kacrab's is ~35% further up the curve. To compare the
clients rather than their offered loads, `KACRAB_BENCH_THROUGHPUT` pins kacrab to
Java's rate. Interleaved kacrab/Java pairs, 5M x 10 B over 16 partitions, kacrab
pinned to 3.65M rec/sec, medians of 3 pairs (2026-07-27):

| Metric | kacrab @3.65M rec/s | Java `kafka-producer-perf-test` (~3.4M rec/s) |
| --- | ---: | ---: |
| Latency avg | **0.11 ms** | 0.32 ms |
| Latency p50 / p95 / p99 | 0 / 1 / 2 ms | 0 / 1 / 2 ms |
| Latency p99.9 | **3 ms** | 5 ms |
| Latency max | **4 ms** | 131-140 ms |

kacrab wins or ties every metric, and still has ~35% throughput headroom above
Java's ceiling on top. The headline tables are not wrong — they simply compare two
clients running at different speeds, which is the right way to read a saturation
benchmark and the wrong way to read a latency one.

> **Why avg differs while p50/p95/p99 tie.** Both sides record latency at the same
> resolution: a truncated integer millisecond. Java computes
> `int latency = (int) (now - start)` from `System.currentTimeMillis()`
> (`ProducerPerformance.java:554`); kacrab's port does
> `i32::try_from(latency.as_millis())` on an `Instant` delta
> (`producer_kafka_bench.rs:1068`). Both then report `avg = totalLatency / count`
> over those integers (`ProducerPerformance.java:515`,
> `producer_kafka_bench.rs:1186`) and take percentiles from the same
> reservoir-sampled array capped at 500,000 samples. So the avg column is a
> like-for-like comparison, not a finer instrument: it is simply the *fraction* of
> records that landed in a bucket above 0 ms. avg 0.11 means roughly 11% of kacrab's
> records took >= 1 ms; Java's 0.32 means roughly 32% of its did. That is also why
> the percentiles can tie at 0 / 1 / 2 — with ~89% of samples at 0 ms, p50 is 0 and
> p95 falls in the 1 ms bucket for both. The avg is the only column that resolves a
> difference smaller than the 1 ms quantum, which is exactly why it is here.

Reproduce:

```bash
KACRAB_BENCH_TOPIC=kacrab-16p KACRAB_ONLY_10B=1 KACRAB_BENCH_RUNS=1 \
  KACRAB_BENCH_THROUGHPUT=3650000 \
  cargo run -p kacrab-benches --release --bin producer_kafka_bench
```

On a single partition (`kacrab-1p`) kacrab measured ~0.08 ms avg — below Java's.
Lower
`max.in.flight.requests.per.connection` / `linger.ms` for lower single-broker
latency; the gap shrinks in production (broker off the client machine, real
network RTT).

### Throughput + latency (100K x 10 KiB, 3 partitions, default `batch.size`)

Medians of 5 interleaved pairs, 2026-07-27, corrected bench:

| Metric | kacrab | Java `kafka-producer-perf-test` |
| --- | ---: | ---: |
| Throughput | **49.5K rec/sec (483 MB/sec)** | 43.0K rec/sec (420 MB/sec) |
| Latency avg | **40.5 ms** | 43.1 ms |
| Latency p50 / p95 / p99 | **39 / 48** / 85 ms | 41 / 64 / **83** ms |
| Latency max | **92 ms** | 133 ms |
| retries / errors | 0 / 0 | 0 / 0 |

The 2026-07-02 revision of this row read ~542-570 MB/sec for kacrab against
417-453 MB/sec for Java. That gap was inflated: it quoted kacrab's own `MiB/s`
summary line against Java's `MB/sec` line. Read from the same line on both sides,
the 10 KiB throughput lead is **+15%**, not +25-30%. The latency figures held up
on re-measurement (~36 ms then, ~40.5 ms now, within run-to-run spread).

A 10 KiB record exceeds half of the default 16 KiB `batch.size`, so every batch
holds one record; throughput stays high because each `acks=all` produce request
coalesces one ready batch from every partition (`records_per_request` = 3 on a
3-partition topic) instead of serializing one record per round trip.

### CPU + peak memory (same 5M x 10B workload, `/usr/bin/time -l`, 2026-06-28)

| Resource | kacrab | Java | Java overhead |
| --- | ---: | ---: | ---: |
| Peak RSS | ~68 MiB | ~268 MiB | **~3.9x more** |
| Total CPU (user+sys) | ~2.7 s | ~4.1 s | **~1.5x more** |
| Wall time | ~1.1-1.8 s | ~2.4 s | -- |

This is where the native-vs-JVM gap shows. Throughput is **broker-bound** (both
clients spend most of the run waiting on `acks=all` round-trips), so the +25%
records/sec edge comes from keeping the broker busier (pipeline depth plus
one-batch-per-partition request coalescing), not from cheaper per-record CPU.
The real efficiency difference is in **memory** (no JVM heap/metaspace, ~4x
less resident) and **CPU per record** (~1.5x less work for the same 5M records,
while also pushing higher throughput). The Java CPU figure includes one-time
JVM startup + JIT warmup that amortizes over a long-lived producer; the
peak-RSS figure is steady-state and persistent.

## Real-Kafka Consumer Baselines

Measured 2026-07-02 against the same native Apache Kafka 4.3.0 single-node
KRaft broker and host as the producer baselines (M3 Pro, `127.0.0.1:9092`,
native — never through a Docker-VM port forward). Defaults on both sides:
subscribe as a group, `max.poll.records=500`, no compression.

### Throughput + latency (5,000,000 x 10 B, 16 partitions, `kacrab-c16p`)

| Metric | kacrab | Java `kafka-consumer-perf-test` |
| --- | ---: | ---: |
| Throughput | ~17.6M records/sec (~168 MB/sec) | ~9.3M records/sec (~89 MB/sec) |
| Rebalance (join) time | ~8 ms | ~131 ms |
| poll() p50 / p99 | ~0.022 ms / ~0.04 ms | ~0.025 ms / ~0.20 ms |
| poll() p99.9 / max | ~2.5 ms / ~8 ms | ~1.9 ms / ~111 ms |
| CPU (user+sys, one run) | ~0.28 s | ~2.5 s |
| Peak RSS (one run) | ~18 MiB | ~286 MiB |

### Throughput + latency (100,000 x 10 KiB, 3 partitions, `kacrab-bench-10k`)

| Metric | kacrab | Java `kafka-consumer-perf-test` |
| --- | ---: | ---: |
| Throughput | ~540K records/sec (~5,277 MB/sec) | ~136K records/sec (~1,329 MB/sec) |
| Rebalance (join) time | ~3 ms | ~128 ms |
| poll() p50 / p99 | ~0.54 ms / ~0.7 ms | ~1.7 ms / ~4.0 ms |
| poll() max | ~4.2 ms | ~108 ms |
| CPU (user+sys, one run) | ~0.16 s | ~2.8 s |
| Peak RSS (one run) | ~12 MiB | ~230 MiB |

At identical defaults (`max.poll.records=500`, the tool props above) kacrab
consumes small records **~1.9x faster** and large records **~4x faster** than
Java, at ~9-17x less CPU and **~16-20x less memory**, with group joins ~15x
faster and a poll() tail (max) 14-25x lower. Java's only remaining edge is a
slightly tighter p99.9 on the 10 B workload (~1.9 ms vs ~2.5 ms). Both
latency lines come from identical loops (the Rust bench and a compiled Java
probe time every `poll()` call; the max lands on the join poll for Java).
Poll-latency percentiles print per run (`rust poll latency:` /
`java poll latency:` lines).

Three pieces produce these numbers (all Java-parity mechanisms, 2026-07-02):

- **Cross-poll fetch buffering** (Java's `completedFetches`): raw fetch
  responses buffer client-side and `poll` drains them `max.poll.records` at a
  time; a partition is only re-fetched once dry (~13 Fetch RPCs per 5M-record
  run, down from 10,000). Before this, every poll re-fetched — and the broker
  re-served — the response surplus, capping the 10 B row at ~132K records/sec.
- **Background prefetch** (Java's network thread): the next Fetch is spawned as
  a task while the caller drains buffered records, and an empty-buffer poll
  awaits it only up to the poll budget. Fetches skip nodes that still host
  buffered partitions (Java's buffered-node gate) — without that gate a fetch
  listing only caught-up partitions long-polls `fetch.max.wait.ms` and stalls
  the pipeline (measured: throughput collapsed 13x, poll p99.9 hit the 100 ms
  poll deadline).
- **Lazy per-batch decode** (Java's `CompletedFetch` iterator): buffered blobs
  decode one record batch at a time as drained, so memory holds raw blobs plus
  ~one batch of records, and record materialization churns through small
  same-size allocations. Decoding whole blobs up front measured ~536 MiB peak
  RSS (allocator retention of large doubling-growth vectors); per-batch decode
  is ~18 MiB — and it also cut the p99.9 poll (the old blob-decode spike) from
  ~5 ms to ~2.5 ms while lifting throughput ~10%.

Variants (single runs, 10 KiB scenario): KIP-848 `group.protocol=consumer` and
manual assign track the subscribe numbers (joins ~24 ms for KIP-848, 0 for
assign).

### Consumer Comparison Caveats

- kacrab negotiates topic-id-keyed Fetch (v13+, KIP-516) like Java, up to the
  broker's max (v18 on Kafka 4.3), downgrading to the name-keyed v12 only when
  a topic id is unavailable.
- kacrab has no rebalance-listener callback, so its rebalance time is observed
  as the `assignment()` empty -> non-empty transition around `poll`, quantized
  to one poll slice (<= 100 ms overestimate); Java records the exact in-callback
  instant. kacrab's ~4-12 ms joins vs Java's ~130 ms hold well beyond that
  noise floor.
- Java's CSV labels the byte columns `MB`, but the tool computes mebibytes
  (`bytes / 1024 / 1024`); kacrab reproduces the same computation, so the
  columns compare 1:1 (and are ~5% smaller than decimal-MB figures).
- Five-run local smoke measurements on shared client/broker hardware; the same
  Limits Of This Pass caveats as the producer baselines apply.

## Mock-Broker And Criterion Numbers

`producer_mock_bench` runs two single-shot mock-broker scenarios: 5M messages ×
10 bytes and 100K messages × 10 KiB, each waiting for mock produce
acknowledgements. It reports both outer public API chunks and actual mock
broker Produce requests, because dispatcher-side batch splitting can issue more
broker requests than public per-record send loops. Useful for local hot-path
smoke testing, not a real Kafka comparison.

Last recorded Criterion samples against local mock brokers (re-run locally for
current numbers; async groups use longer measurement time instead of reduced
sample counts, and the accumulator benchmark uses `BatchSize::LargeInput`):

- `producer_dispatcher/multi_broker_dispatch`: 9.50-9.80M messages/sec.
- `producer_accumulator/append_and_drain/1024`: 26.64-26.77M records/sec.
- `producer_accumulator/append_and_drain/16384`: 28.26-28.54M records/sec.
- `wire_pipeline/api_versions_send_to_broker`: 170.86-173.37K requests/sec.

## Limits Of This Pass

- Real Kafka and Java executable numbers are five-run smoke measurements, not
  release benchmark gates.
- Client and broker share the same machine, CPU, memory, and disk. There was no
  CPU pinning, broker log-dir purge between every trial, page-cache isolation,
  or background-load control.
- The Kafka setup is single-node KRaft with RF=1 and no replication durability
  target. The baselines above run the default `acks=all` + idempotence config;
  the relaxed `acks=1` / no-idempotence config is opt-in via
  `KACRAB_BENCH_ACKS1=1`.
- The `records sent, … MB/sec` summary line is the comparable one: Java computes
  `1000 * bytes / elapsed / (1024 * 1024)` and labels it `MB/sec`
  (`ProducerPerformance.java:508`), and kacrab's port does exactly the same, so both
  columns are mebibytes under a `MB` label and compare directly. kacrab *also*
  prints its own `MiB/s` scenario line; quoting that one against Java's `MB/sec`
  mixes two different lines and overstates the gap, which an earlier revision of
  the 10 KiB table did.
- The executable Rust benches port Kafka Java `ProducerPerformance.Stats` /
  `ConsumerPerformance` sampling, window reporting, total summary, and
  callback-success-only accounting, plus a coarse `/usr/bin/time -l` CPU-time
  and peak-RSS comparison against the Java tools. They still do not collect
  sampled CPU profiles, allocator profiles, broker disk metrics, or end-to-end
  replicated durability latency.
- Mock broker and Criterion numbers are useful for client hot-path regression
  checks, but they do not include real broker storage, replication, fetch, or
  network effects.

## Author

`kacrab-benches` is authored and maintained by `pirumu`.

## License

This crate is licensed under either MIT or Apache-2.0, matching the workspace.
