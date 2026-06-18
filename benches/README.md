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

Throughput target: 3M messages/sec on realistic batching and multi-broker
workloads. Reference-client parity is the longer-term goal.

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
KACRAB_BENCH_SMOKE=1 cargo run -p kacrab-benches --release --bin producer_kafka_bench
cargo run -p kacrab-benches --release --bin producer_kafka_bench
KACRAB_ONLY_10KIB=1 cargo run -p kacrab-benches --release --bin producer_kafka_bench
KACRAB_DELIVERY_MODE=both cargo run -p kacrab-benches --release --bin producer_kafka_bench
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
scenario-sized batches through the selected delivery path. The default delivery path is
`Producer::send_batch_untracked`; set `KACRAB_DELIVERY_MODE=tracked` or
`KACRAB_DELIVERY_MODE=both` to include Java-style callback tracking overhead
separately. `KACRAB_DELIVERY_MODE=batch` measures the Rust batch-receipt
extension path.

The default benchmark profile is `kafka-default`: the binary sets only
`bootstrap.servers` and `client.id`, then relies on the producer's normal
Kafka-compatible defaults. The previous throughput-oriented local baseline is
still available with `KACRAB_BENCH_PROFILE=relaxed`.

Useful real-Kafka knobs:

- `KACRAB_BENCH_PROFILE=kafka-default|relaxed` selects the producer config
  profile. `kafka-default` is the default and applies no throughput tuning.
  `relaxed` applies the old comparison settings: idempotence disabled,
  `acks=1`, no compression, `retries=0`, explicit timeouts, `batch.size`, and
  bounded queue/pool knobs.
- `KACRAB_IN_FLIGHT` explicitly overrides
  `max.in.flight.requests.per.connection`.
- `KACRAB_ACKS` explicitly overrides `acks`. `KACRAB_ACKS=0` is a no-response
  produce path; use it only with a valid config such as
  `KACRAB_BENCH_PROFILE=relaxed KACRAB_ACKS=0`, because idempotence requires
  acknowledgement.
- `KACRAB_PARTITION_MODE=unassigned` uses the default Java-style sticky
  partitioner for null-key records. This is the default benchmark mode.
- `KACRAB_PARTITION_MODE=manual` keeps the older manual round-robin benchmark
  path for isolating partitioner overhead.
- `KACRAB_BATCH_SIZE` explicitly overrides producer `batch.size`.
- `KACRAB_BATCH_MESSAGES_10B` controls the outer API chunk size for the 10-byte
  scenario. This is benchmark harness chunking, not Kafka producer
  `batch.size`.
- `KACRAB_BATCH_MESSAGES_10KIB` controls the outer API chunk size for the
  10 KiB scenario. This is benchmark harness chunking, not Kafka producer
  `batch.size`. The default is `96`, matching the saved relaxed five-run
  baseline.
- `KACRAB_ONLY_10B=1` runs only the 5M × 10B scenario.
- `KACRAB_ONLY_10KIB=1` runs only the 100K × 10 KiB scenario.
- `KACRAB_ENABLE_METRICS=1` enables opt-in producer accounting metrics in the
  executable output: broker Produce requests, records, retries, errors,
  requeues, and batch fill ratio. The default keeps this disabled so baseline
  throughput does not pay for operational counters.
- `KACRAB_ENABLE_LATENCY=1` enables dispatch latency sampling and percentile
  output. The default keeps this disabled so throughput-only runs do not pay
  for latency accounting.
- `KACRAB_DELIVERY_MODE=untracked|tracked|batch|both|all` selects the public
  producer path. `untracked` is the baseline path used by the local summary
  table. `tracked` uses `send_with_callback` on the Java-style per-record path.
  `batch` uses the Rust `send_batch` batch-receipt extension. `both` runs
  `untracked` and `tracked`. `all` runs every mode.
- `KACRAB_TRACKED_DELIVERY_WINDOW` bounds how many callback-tracked records are
  sent before the benchmark forces a `flush`. In `batch` mode it bounds how
  many batch receipts are retained before flushing and awaiting them. The
  default is `262144`.
- `KACRAB_CUSTOM_MESSAGES`, `KACRAB_CUSTOM_VALUE_SIZE`, and
  `KACRAB_CUSTOM_BATCH_MESSAGES` run one custom payload profile instead of the
  built-in extremes.
- `KACRAB_PAYLOAD_FILE=/path/to/payload.bin` repeats bytes from a real payload
  file. If `KACRAB_CUSTOM_VALUE_SIZE` is omitted, the benchmark uses the file
  size for buffer sizing and scenario labeling; if `KACRAB_CUSTOM_MESSAGES` is
  omitted, it sends 100K records.

Example real-payload run:

```bash
KACRAB_PAYLOAD_FILE=/tmp/order-event.bin \
KACRAB_CUSTOM_MESSAGES=250000 \
KACRAB_CUSTOM_BATCH_MESSAGES=2048 \
cargo run -p kacrab-benches --release --bin producer_kafka_bench
```

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

## Local Baselines

Measured on 2026-06-17 on this development machine after the record-batch,
request-frame allocation, and opt-in metrics pass. The saved real-Kafka numbers
below are the relaxed throughput profile, not the `kafka-default` profile.
Treat these as local measurement checkpoints, not production Kafka acceptance
numbers.

Benchmark host:

- MacBook Pro, model identifier `Mac15,6`.
- Apple M3 Pro base chip: 11-core CPU (5 performance, 6 efficiency), 14-core
  GPU, 16-core Neural Engine.
- 18GB unified memory; M3 Pro memory bandwidth: 150GB/s.

Real Kafka and Java executable benchmark entries below are five-run summaries
with fresh topics per run. Criterion entries report the throughput range from
the saved local run.

Measured against native Apache Kafka 4.3.0 single-node KRaft on the same
machine, through the public producer API. Kafka was exposed on
`127.0.0.1:9092`; benchmark topics used 3 partitions and replication factor 1.
The 2026-06-17 in-flight-5 pass used fresh topics per benchmark run.

Default `kafka-default` profile, five fresh-topic runs, no latency sampling:

```bash
cargo run -p kacrab-benches --release --bin producer_kafka_bench
```

| Scenario | kacrab, 5 runs |
| --- | ---: |
| 5M × 10B | avg 3,332,394 msg/sec; median 3,501,053; min 2,229,868; max 4,401,966; std dev 771,090; 31.78 MiB/sec |
| 100K × 10 KiB | avg 14,321 msg/sec; median 14,310; min 14,009; max 14,594; std dev 237; 139.86 MiB/sec |

Raw default-profile runs:

| Run | 5M × 10B | 100K × 10 KiB |
| ---: | --- | --- |
| 1 | 3,501,053 messages/sec; 33.389 MiB/sec; 1.428s | 14,009 messages/sec; 136.804 MiB/sec; 7.138s |
| 2 | 4,401,966 messages/sec; 41.980 MiB/sec; 1.136s | 14,116 messages/sec; 137.848 MiB/sec; 7.084s |
| 3 | 3,801,057 messages/sec; 36.250 MiB/sec; 1.315s | 14,310 messages/sec; 139.750 MiB/sec; 6.988s |
| 4 | 2,728,025 messages/sec; 26.016 MiB/sec; 1.833s | 14,578 messages/sec; 142.365 MiB/sec; 6.860s |
| 5 | 2,229,868 messages/sec; 21.266 MiB/sec; 2.242s | 14,594 messages/sec; 142.517 MiB/sec; 6.852s |

Default-profile throughput is lower than the relaxed baseline because the
Kafka-compatible default path uses `acks=all` and idempotence. This snapshot
predates the per-topic-partition idempotent dispatch path; kacrab now preserves
producer sequence ordering per topic-partition while allowing independent
partitions to use the configured in-flight budget. `KACRAB_BENCH_PROFILE=relaxed`
disables idempotence and keeps the old throughput comparison path.

```bash
KACRAB_BENCH_PROFILE=relaxed \
KACRAB_ENABLE_LATENCY=1 \
cargo run -p kacrab-benches --release --bin producer_kafka_bench
```

Rust relaxed-profile `producer_kafka_bench` settings: `acks=1`, idempotence
disabled, no compression, `batch.size=16384`, 3 partitions, default
`partition_mode=unassigned` Java-style sticky partitioning, `retries=0`,
`request.timeout.ms=30000`, `delivery.timeout.ms=120000`,
`socket.read.buffer.capacity.bytes=1048576`, `broker.queue.capacity=2 ×
in_flight`, `buffer.pool.capacity=128`, and a current-thread Tokio runtime.

Kacrab latency below is dispatch latency for the untracked throughput path:
from the earliest append timestamp in a drained ProduceRequest group until the
broker response is handled. It does not allocate per-record `SendFuture` handles.
This is intentionally not the same metric as Java producer-perf latency. The
current kacrab average is higher than Java's average because the sample includes
client-side batch grouping and Tokio task scheduling around ProduceRequest
dispatch; the higher throughput indicates the client is amortizing more records
per dispatch. Treat the latency gap as an optimization target for runtime
scheduling, batch assembly timing, and linger behavior, not as proof of slower
broker append latency by itself.
Java latency is reported by `kafka-producer-perf-test.sh`.

Five-run real Kafka and Java summary:

| Scenario | kacrab, 5 runs | Java, 5 runs |
| --- | ---: | ---: |
| 5M × 10B | avg 7,976,245 msg/sec; median 7,923,087; min 7,773,087; max 8,236,373; std dev 183,688; 76.07 MiB/sec; latency avg 2.00 ms, p99 avg 4.70 ms | avg 3,594,271 records/sec; median 3,602,305; min 3,306,878; max 3,900,156; std dev 212,766; 34.28 MB/sec; latency avg 0.59 ms, p99 avg 9.00 ms |
| 100K × 10 KiB | avg 55,756 msg/sec; median 55,673; min 55,115; max 56,329; std dev 520; 544.49 MiB/sec; latency avg 1.39 ms, p99 avg 4.46 ms | avg 31,170 records/sec; median 29,214; min 25,517; max 40,274; std dev 5,970; 304.39 MB/sec; latency avg 63.31 ms, p99 avg 146.40 ms |

Shared relaxed comparison settings: in-flight `5`, `acks=1`, idempotence
disabled, no compression, `batch.size=16384`, 3 partitions, RF=1. The kacrab
100K × 10 KiB run used `KACRAB_BATCH_MESSAGES_10KIB=96`; the 5M × 10B run used
`KACRAB_BATCH_MESSAGES_10B=16384`. These `KACRAB_BATCH_MESSAGES_*` values are
outer public API chunks for the benchmark harness, not Kafka producer
`batch.size`. Each kacrab scenario warms up one outer API chunk before the
measured window.

Raw kacrab runs:

| Run | 5M × 10B | 100K × 10 KiB |
| ---: | --- | --- |
| 1 | 8,236,373 messages/sec; 78.548 MiB/sec; avg 1.95 ms; p99 4.55 ms | 56,329 messages/sec; 550.092 MiB/sec; avg 1.38 ms; p99 4.31 ms |
| 2 | 7,866,653 messages/sec; 75.022 MiB/sec; avg 2.02 ms; p99 4.63 ms | 55,673 messages/sec; 543.684 MiB/sec; avg 1.39 ms; p99 4.53 ms |
| 3 | 7,923,087 messages/sec; 75.560 MiB/sec; avg 2.00 ms; p99 4.52 ms | 56,233 messages/sec; 549.148 MiB/sec; avg 1.38 ms; p99 4.43 ms |
| 4 | 7,773,087 messages/sec; 74.130 MiB/sec; avg 2.05 ms; p99 5.30 ms | 55,115 messages/sec; 538.233 MiB/sec; avg 1.40 ms; p99 4.46 ms |
| 5 | 8,082,023 messages/sec; 77.076 MiB/sec; avg 1.97 ms; p99 4.50 ms | 55,428 messages/sec; 541.285 MiB/sec; avg 1.39 ms; p99 4.55 ms |

The printed kacrab request counts are outer public API chunks in the selected
delivery mode: 306 chunks for 5M × 10B and 1042 chunks for 100K × 10 KiB.
Internally, the producer accumulator splits same-partition batches at
`batch.size`, and the dispatcher packs those split batches across partitions in
Java-like ProduceRequest groups.

Same broker, same relaxed producer props through Apache Kafka's Java native
`kafka-producer-perf-test.sh`:

```bash
kafka-producer-perf-test.sh \
  --topic kacrab-bench \
  --num-records 100000 \
  --record-size 10240 \
  --throughput -1 \
  --producer-props \
  bootstrap.servers=127.0.0.1:9092 \
  acks=1 \
  enable.idempotence=false \
  compression.type=none \
  batch.size=16384 \
  linger.ms=5 \
  max.in.flight.requests.per.connection=5
```

For the Java 5M × 10B case, keep the same producer props and change
`--num-records 5000000 --record-size 10`.

Raw Java runs:

| Run | 5M × 10B | 100K × 10 KiB |
| ---: | --- | --- |
| 1 | 3,628,447 records/sec; 34.60 MB/sec | 25,516 records/sec; 249.19 MB/sec |
| 2 | 3,533,569 records/sec; 33.70 MB/sec | 29,214 records/sec; 285.29 MB/sec |
| 3 | 3,900,156 records/sec; 37.19 MB/sec | 27,049 records/sec; 264.15 MB/sec |
| 4 | 3,306,878 records/sec; 31.54 MB/sec | 33,795 records/sec; 330.03 MB/sec |
| 5 | 3,602,305 records/sec; 34.35 MB/sec | 40,274 records/sec; 393.30 MB/sec |

Java latency summary:

| Scenario | Avg latency | p99 avg | Max latency avg |
| --- | ---: | ---: | ---: |
| 5M × 10B | 0.59 ms | 9.00 ms | 136.00 ms |
| 100K × 10 KiB | 63.31 ms | 146.40 ms | 168.00 ms |

Kacrab latency summary:

| Scenario | Avg latency | p50 avg | p95 avg | p99 avg | p999 avg |
| --- | ---: | ---: | ---: | ---: | ---: |
| 5M × 10B | 2.00 ms | 1.86 ms | 3.00 ms | 4.70 ms | 9.83 ms |
| 100K × 10 KiB | 1.39 ms | 1.30 ms | 2.15 ms | 4.46 ms | 8.43 ms |

These Java numbers are a same-props throughput comparison, not Java client
defaults (`acks=all`, idempotence enabled, and retries enabled). They are not
an apples-to-apples comparison with public rust-rdkafka README numbers either:
that quoted benchmark used older hardware/software, decimal MB/sec reporting,
and average-over-5 semantics.

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

- `producer_mock_bench` now reports both outer public API chunks and actual
  mock broker Produce requests, because dispatcher-side batch splitting can
  issue more broker requests than `send_batch_untracked` calls.

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
- The Kafka setup is deliberately relaxed for throughput exploration:
  single-node KRaft, 3 partitions, RF=1, `acks=1`, idempotence disabled, no
  compression, and no replication durability target.
- Kacrab throughput prints payload MiB/sec. Kafka's Java perf tool prints
  decimal MB/sec, so MiB/sec and MB/sec values should not be compared as the
  same unit.
- The executable Rust bench now reports dispatch latency for the untracked
  throughput path. It still does not collect CPU profiles, allocator profiles,
  broker disk metrics, or end-to-end replicated durability latency.
- Mock broker and Criterion numbers are useful for client hot-path regression
  checks, but they do not include real broker storage, replication, fetch, or
  network effects.
- The default `producer_kafka_bench` path uses `send_batch_untracked`, so it
  does not measure delivery tracking overhead unless
  `KACRAB_DELIVERY_MODE=tracked`, `batch`, `both`, or `all` is set.

## Author

`kacrab-benches` is authored and maintained by `pirumu`.

## License

This crate is licensed under either MIT or Apache-2.0, matching the workspace.
