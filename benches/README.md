# kacrab-benches

Internal benchmark suite for the kacrab workspace. **Not published.**

Current benchmark targets are measurement hooks for the wire/producer
architecture work. They are not release-throughput guarantees.

## Benchmark Surface

- **Throughput** - N messages of size S to a topic with P partitions.
  Records msgs/sec and bytes/sec on identical hardware.
- **Latency** - sustained load, captures p50/p99/p999.
- **Memory** - long-running soak, verifies steady-state allocation.
- **Codec micro-benchmarks** - record batch v2 encode/decode, varint
  fast paths, CRC32C throughput.
- **Producer accumulator micro-benchmarks** - append and drain throughput for
  the per-topic-partition accumulator.
- **Wire pipeline micro-benchmarks** - `WireClient::send_to_broker` request/sec
  against local mock brokers.
- **Producer dispatcher stress benchmarks** - accumulator plus multi-broker
  produce dispatch over local mock leaders.

The current real-Kafka head-to-head plus a CPU/peak-memory comparison live under
[Real-Kafka Baselines](#real-kafka-baselines): kacrab beats the Java client's
throughput and uses roughly 4x less memory and 1.5x less CPU for the same run.
Sustained production acceptance is the longer-term goal.

## Scope

- `producer_accumulator` - per-topic-partition append/drain micro-benchmarks.
- `wire_pipeline` - broker request pipeline request/sec against mock brokers.
- `producer_dispatcher` - accumulator plus multi-broker produce dispatch.
- `producer_mock_bench` - executable mock broker smoke benchmark.
- `producer_kafka_bench` - executable real Kafka smoke benchmark.

## Running

```bash
cargo bench -p kacrab-benches              # all benches (once wired up)
cargo bench -p kacrab-benches --bench producer_accumulator
cargo bench -p kacrab-benches --bench wire_pipeline
cargo bench -p kacrab-benches --bench producer_dispatcher
cargo run -p kacrab-benches --release --bin producer_mock_bench
cargo run -p kacrab-benches --release --bin producer_kafka_bench
KACRAB_ONLY_10KIB=1 cargo run -p kacrab-benches --release --bin producer_kafka_bench
```

`producer_mock_bench` runs two single-shot mock-broker scenarios: 5M messages ×
10 bytes and 100K messages × 10 KiB, each waiting for mock produce
acknowledgements. It is useful for local hot-path smoke testing, but it is not a
real Kafka comparison.

The Criterion micro-benchmarks live under `benches/`. The executable smoke
benchmarks live under `src/bin/` because they print scenario summaries and, for
the real Kafka path, depend on a running broker.

For real Kafka localhost benchmarks without Docker Desktop, use the root compose
file with Docker-compatible runtimes such as Colima or OrbStack:

```bash
docker compose -f docker-compose.kafka.yml up -d
docker compose -f docker-compose.kafka.yml ps
docker compose -f docker-compose.kafka.yml logs -f kafka-init
docker compose -f docker-compose.kafka.yml down
```

The compose file exposes Kafka on `localhost:9092` and creates
`kacrab-bench` with 3 partitions by default. Override with
`KAFKA_HOST_PORT`, `KAFKA_BENCH_TOPIC`, or `KAFKA_BENCH_PARTITIONS`.

`producer_kafka_bench` uses `KACRAB_BOOTSTRAP` and `KACRAB_BENCH_TOPIC`
when set, defaulting to `127.0.0.1:9092` and `kacrab-bench`. It uses the
public `Producer::builder().set(...).build()` API, warms up metadata, the
broker session, and one outer API chunk outside the measured window, then sends
records through the Java-style public producer path. The benchmark calls
`send_with_callback` once per record, while the producer accumulator/sender
automatically groups records into Produce requests. Callback-completion latency
is reported with the same `ProducerPerformance.Stats` window and total line
shape as Kafka Java.

Current Java-parity audit runs should use the root Makefile targets instead of
the older exploratory env matrix:

```bash
make kafka-topic-recreate
make bench-kafka
make bench-kafka-java-default
make kafka-topic-delete
make kafka-stop
make kafka-topic-prune-delete-dirs
```

`bench-kafka` and `bench-kafka-java-default` both create
`KACRAB_BENCH_TOPIC` if it is missing, with `KACRAB_PARTITIONS=3` and
`KACRAB_REPLICATION_FACTOR=1` by default. Use `make kafka-topic-recreate` for a
fresh topic before a comparison run. After large benchmark passes, delete the
topic and stop Kafka before pruning topic `*-delete` directories; this keeps
local broker data from silently growing across parity runs.

The current parity target fixes two scenarios, runs 5 times per scenario, and
prints effective config snapshots before each measured run. Rust prints the
Java producer-perf style throughput/latency line for every run, then prints a
five-run `rust average counters` line using the same compact counter schema as
the per-run output. The Java wrapper also parses
`kafka-producer-perf-test.sh --print-metrics` into per-run and five-run average
compact counter lines, including `request_size_avg` from Java
`request-size-avg` and Rust generated ProduceRequest encoded length. Java
`batch_splits` comes from producer-perf's `batch-split-total`; Rust currently
reports `batch_splits=not_tracked` and exposes ProduceRequest grouping splits
separately as `request_splits`. Rust also reports `compression_ratio` from
actual dispatcher-encoded record batches. Java producer-perf public metrics do not expose
exact record-batch count or records-per-batch count, so those fields are
labeled `not_exposed_by_producer_perf`; do not treat them as parity proof.

By default the binary sets only `bootstrap.servers` and `client.id` and relies
on the producer's normal Kafka-compatible defaults (`acks=all`,
`enable.idempotence=true`, no compression). Set `KACRAB_BENCH_ACKS1=1` for the
relaxed throughput-comparison config (`acks=1`, idempotence disabled).

Useful real-Kafka knobs (all read from the environment by `producer_kafka_bench`,
so set them inline before `cargo run`):

- `KACRAB_BOOTSTRAP` — broker address (default `127.0.0.1:9092`).
- `KACRAB_BENCH_TOPIC` — topic (default `kacrab-bench`).
- `KACRAB_BENCH_ACKS1=1` — switch to `acks=1` + `enable.idempotence=false` (the
  relaxed comparison config); the default is `acks=all` + idempotence on.
- `KACRAB_BENCH_BATCH_SIZE=N` — override producer `batch.size` (probe whether
  throughput is round-trip / pipelining bound).
- `KACRAB_BENCH_MAX_REQUEST_SIZE=N` — override `max.request.size`, lifting the
  1 MiB default so large-record runs with a bigger `batch.size` do not trip
  `RecordTooLarge` on coalesced requests.
- `KACRAB_BENCH_SYNC_SEND=1` — use the synchronous send path (real sticky
  partitioner, non-blocking partition assignment) instead of the async path.
- `KACRAB_BENCH_SEND_CONCURRENCY=N` — number of concurrent in-flight send tasks
  (default `1`).
- `KACRAB_BENCH_CURRENT_THREAD=1` — force the single-thread Tokio runtime
  (default: multi-thread).
- `KACRAB_BENCH_WORKERS=N` — worker threads for the multi-thread runtime
  (default `4`).
- `KACRAB_BENCH_NO_METRICS=1` — disable the producer accounting metrics (broker
  Produce requests, records, retries, errors, requeues, fill ratio); these are
  enabled by default.
- `KACRAB_ONLY_10B=1` — run only the 5M × 10B scenario.
- `KACRAB_ONLY_10KIB=1` — run only the 100K × 10 KiB scenario.
- `KACRAB_BENCH_MESSAGES=N` — run a single 10B scenario with `N` records.
- `KACRAB_BENCH_RUNS=N` — number of runs per scenario.
- `KACRAB_BENCH_API` — accepted for old scripts but a no-op; every value resolves
  to the Java-style per-record public API. The benchmark calls
  `send_with_callback` once per record and measures callback latency from just
  before send to callback completion, matching Kafka Java producer-perf tracking.

The public API hot path is allocation-conscious rather than magically
wire-zero-copy: payloads are cloned as `Bytes` handles, and benchmark topics are
shared as `Arc<str>` handles, so input data is not copied per message. Kafka
Produce still requires serialized record batches and request frames on the wire,
including size fields and record-batch CRCs, so the client must materialize
encoded bytes before writing to the socket. Kafka Java exposes `check.crcs` on
the consumer/fetch side to skip fetched-record CRC verification; it does not
remove producer-side CRC generation for Produce requests. Future work can
reduce this further with pooled encode buffers and fewer intermediate frame/body
moves, but it cannot skip Kafka serialization itself.

## Real-Kafka Baselines

Measured 2026-06-28 against native Apache Kafka 4.3.0 single-node KRaft on the
same machine (`127.0.0.1:9092`), through the public producer API at the
**default Kafka-compatible config** (`acks=all`, `enable.idempotence=true`), no
compression. Client and broker share the host (no CPU pinning or page-cache
isolation), so treat these as local checkpoints, not production acceptance
numbers.

Benchmark host:

- MacBook Pro `Mac15,6`, Apple M3 Pro (11-core CPU: 5 performance, 6
  efficiency), 18 GB unified memory.

### Throughput + latency (5M x 10B, 16 partitions, `kacrab-16p`)

Reproduce:

```bash
# kacrab
KACRAB_BENCH_SYNC_SEND=1 KACRAB_BENCH_TOPIC=kacrab-16p \
  cargo run -p kacrab-benches --release --bin producer_kafka_bench

# Java, same broker/topic/config
kafka-producer-perf-test.sh --topic kacrab-16p --num-records 5000000 \
  --record-size 10 --throughput -1 --producer-props \
  bootstrap.servers=127.0.0.1:9092 acks=all enable.idempotence=true
```

| Metric | kacrab | Java `kafka-producer-perf-test` |
| --- | ---: | ---: |
| Throughput | ~4.70M rec/sec (44.8 MiB/sec) | 3.6-4.2M rec/sec |
| Latency avg | ~1.7 ms | 0.27-0.35 ms |
| Latency p99 | ~15 ms | 1-2 ms |
| retries / errors | 0 / 0 | 0 / 0 |

kacrab wins throughput (about +12% over Java's best runs) while staying fully
idempotent-correct, but Java has lower typical latency. The gap is a tunable
tradeoff plus a shared broker artifact, not a client cost:

- **Pipeline depth.** kacrab's synchronous send fills the per-partition pipeline
  toward `max.in.flight=5`. At `max.in.flight=1` kacrab's p99 drops to ~2 ms at
  the same ~4.7M throughput, because on a single low-RTT broker the per-broker
  request coalescing already saturates the connection and the extra depth only
  adds queue latency. Depth pays off across multiple brokers / higher RTT.
- **Broker-pause resilience.** The co-located single-node JVM broker pauses
  periodically (GC/fsync); Java sees it too (max latency spiked to 129 ms in the
  same runs). At depth 5 a pause on one in-flight request lets the others drain
  (kacrab p99.9 ~10 ms); at depth 1 the single slot blocks and p99.9 jumps to
  ~100 ms.

On a single partition (`kacrab-1p`) kacrab latency is ~0.2 ms avg, close to Java.
Lower `max.in.flight.requests.per.connection` / `linger.ms` for lower
single-broker latency; the gap shrinks in production (broker off the client
machine, real network RTT).

### CPU + peak memory (same 5M x 10B run, `/usr/bin/time -l`)

| Resource | kacrab | Java | Java overhead |
| --- | ---: | ---: | ---: |
| Peak RSS | ~68 MiB | ~268 MiB | **~3.9x more** |
| Total CPU (user+sys) | ~2.7 s | ~4.1 s | **~1.5x more** |
| Wall time | ~1.1-1.8 s | ~2.4 s | -- |

This is where the native-vs-JVM gap shows, and it explains why the throughput
win is "only" ~12%: throughput here is **broker-bound** (both clients spend most
of the run waiting on `acks=all` round-trips), so the client language barely
moves the throughput needle. The real efficiency difference is in **memory**
(no JVM heap/metaspace, ~4x less resident) and **CPU per record** (~1.5x less
work for the same 5M records, while also pushing higher throughput). The Java
CPU figure includes one-time JVM startup + JIT warmup that amortizes over a
long-lived producer; the peak-RSS figure is steady-state and persistent.

Measured with Criterion against local mock brokers. Async groups use longer
measurement time instead of reducing sample count, and the accumulator
append/drain benchmark uses `BatchSize::LargeInput` so the per-iteration
`Vec<ProducerRecord>` setup does not get treated as a tiny input:

- `producer_dispatcher/multi_broker_dispatch`: 9.50-9.80M messages/sec.
  Criterion reported `+109.67%` to `+122.84%` throughput versus the previous
  saved sample.
- `producer_accumulator/append_and_drain/1024`: 26.64-26.77M records/sec.
- `producer_accumulator/append_and_drain/16384`: 28.26-28.54M records/sec.

Producer mock broker executable:

- `producer_mock_bench` reports both outer public API chunks and actual mock
  broker Produce requests, because dispatcher-side batch splitting can issue
  more broker requests than public per-record send loops.

Wire pipeline:

- `wire_pipeline/api_versions_send_to_broker`: 170.86-173.37K requests/sec.

### Limits Of This Pass

- Real Kafka and Java executable numbers are five-run smoke measurements, not
  release benchmark gates.
- The latest Rust latency pass was stable on throughput but still local-only:
  100K × 10 KiB kacrab std dev was 2,021 messages/sec, while the Java pass std
  dev was 5,970 records/sec.
- Client and broker share the same machine, CPU, memory, and disk. There was no
  CPU pinning, broker log-dir purge between every trial, page-cache isolation,
  or background-load control.
- The Kafka setup is single-node KRaft with RF=1 and no replication durability
  target. The baselines above run the default `acks=all` + idempotence config;
  the relaxed `acks=1` / no-idempotence config is opt-in via
  `KACRAB_BENCH_ACKS1=1`.
- Kacrab throughput prints payload MiB/sec. Kafka's Java perf tool prints
  decimal MB/sec, so MiB/sec and MB/sec values should not be compared as the
  same unit.
- The executable Rust bench ports Kafka Java `ProducerPerformance.Stats`
  sampling, window reporting, total summary, and callback-success-only
  accounting, plus a coarse `/usr/bin/time -l` CPU-time and peak-RSS comparison
  against the Java perf tool. It still does not collect sampled CPU profiles,
  allocator profiles, broker disk metrics, or end-to-end replicated durability
  latency.
- Mock broker and Criterion numbers are useful for client hot-path regression
  checks, but they do not include real broker storage, replication, fetch, or
  network effects.
- The default `producer_kafka_bench` path uses Java-style per-record
  `send_with_callback`; batching is internal to the producer.

## Author

`kacrab-benches` is authored and maintained by `pirumu`.

## License

This crate is licensed under either MIT or Apache-2.0, matching the workspace.
