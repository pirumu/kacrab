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
public `KafkaProducer::builder().set(...).build()` API, warms up metadata and
the broker session outside the measured window, then sends scenario-sized
batches through `KafkaProducer::send_batch_untracked`.

Useful real-Kafka knobs:

- `KACRAB_IN_FLIGHT` controls `max.in.flight.requests.per.connection`
  (default `5`, matching the Java producer default used for local comparison).
- `KACRAB_PARTITION_MODE=unassigned` uses the default Java-style sticky
  partitioner for null-key records. This is the default benchmark mode.
- `KACRAB_PARTITION_MODE=manual` keeps the older manual round-robin benchmark
  path for isolating partitioner overhead.
- `KACRAB_BATCH_SIZE` controls producer `batch.size`; default is 16 KiB.
- `KACRAB_BATCH_MESSAGES_10B` controls the outer API chunk size for the 10-byte
  scenario.
- `KACRAB_BATCH_MESSAGES_10KIB` controls the outer API chunk size for the
  10 KiB scenario. The default is `96`; the 2026-06-17 large-payload pass also
  tested `192` and `288`.
- `KACRAB_ONLY_10B=1` runs only the 5M × 10B scenario.
- `KACRAB_ONLY_10KIB=1` runs only the 100K × 10 KiB scenario.

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

Measured on 2026-06-17 on this development machine after the record-batch and
request-frame allocation pass, with kacrab and Java both using in-flight `5`.
Treat these as local measurement checkpoints, not production Kafka acceptance
numbers.

Benchmark host:

- MacBook Pro, model identifier `Mac15,6`.
- Apple M3 Pro base chip: 11-core CPU (5 performance, 6 efficiency), 14-core
  GPU, 16-core Neural Engine.
- 18GB unified memory; M3 Pro memory bandwidth: 150GB/s.

Real Kafka and Java executable benchmark entries below are five-run summaries
with fresh topics per run. Criterion entries use `--sample-size 10` and report
the throughput range from that Criterion run.

Measured against native Apache Kafka 4.3.0 single-node KRaft on the same
machine, through the public producer API. Kafka was exposed on
`127.0.0.1:9092`; benchmark topics used 3 partitions and replication factor 1.
The 2026-06-17 in-flight-5 pass used fresh topics per benchmark run.

```bash
KACRAB_IN_FLIGHT=5 \
cargo run -p kacrab-benches --release --bin producer_kafka_bench
```

Rust `producer_kafka_bench` settings: `acks=1`, idempotence disabled, no
compression, `batch.size=16384`, 3 partitions, default
`partition_mode=unassigned` Java-style sticky partitioning, `retries=0`,
`request.timeout.ms=30000`, `delivery.timeout.ms=120000`,
`socket.read.buffer.capacity.bytes=1048576`, `broker.queue.capacity=2 ×
in_flight`, `buffer.pool.capacity=128`, and a current-thread Tokio runtime.

Kacrab latency below is dispatch latency for the untracked throughput path:
from the earliest append timestamp in a drained ProduceRequest group until the
broker response is handled. It does not allocate per-record `Delivery` handles.
Java latency is reported by `kafka-producer-perf-test.sh`.

Five-run real Kafka and Java summary:

| Scenario | kacrab, 5 runs | Java, 5 runs |
| --- | ---: | ---: |
| 5M × 10B | avg 7,894,985 msg/sec; median 7,868,837; min 7,582,362; max 8,146,794; std dev 199,440; 75.29 MiB/sec; latency avg 2.02 ms, p99 avg 5.25 ms | avg 3,594,271 records/sec; median 3,602,305; min 3,306,878; max 3,900,156; std dev 212,766; 34.28 MB/sec; latency avg 0.59 ms, p99 avg 9.00 ms |
| 100K × 10 KiB | avg 46,802 msg/sec; median 47,295; min 44,627; max 48,736; std dev 1,424; 457.06 MiB/sec; latency avg 1.63 ms, p99 avg 6.72 ms | avg 31,170 records/sec; median 29,214; min 25,517; max 40,274; std dev 5,970; 304.39 MB/sec; latency avg 63.31 ms, p99 avg 146.40 ms |

Shared comparison settings: in-flight `5`, `acks=1`, idempotence disabled,
no compression, `batch.size=16384`, 3 partitions, RF=1. The kacrab
100K × 10 KiB run used the default `KACRAB_BATCH_MESSAGES_10KIB=96`; the
5M × 10B run used the default `KACRAB_BATCH_MESSAGES_10B=16384`.

Raw kacrab runs:

| Run | 5M × 10B | 100K × 10 KiB |
| ---: | --- | --- |
| 1 | 7,809,477 messages/sec; 74.477 MiB/sec; avg 2.05 ms; p99 5.14 ms | 44,627 messages/sec; 435.812 MiB/sec; avg 1.69 ms; p99 7.88 ms |
| 2 | 7,868,837 messages/sec; 75.043 MiB/sec; avg 2.04 ms; p99 6.21 ms | 45,841 messages/sec; 447.665 MiB/sec; avg 1.65 ms; p99 6.62 ms |
| 3 | 8,067,454 messages/sec; 76.937 MiB/sec; avg 1.99 ms; p99 4.93 ms | 47,295 messages/sec; 461.869 MiB/sec; avg 1.61 ms; p99 6.61 ms |
| 4 | 8,146,794 messages/sec; 77.694 MiB/sec; avg 1.96 ms; p99 4.96 ms | 48,736 messages/sec; 475.935 MiB/sec; avg 1.57 ms; p99 6.16 ms |
| 5 | 7,582,362 messages/sec; 72.311 MiB/sec; avg 2.08 ms; p99 4.99 ms | 47,513 messages/sec; 463.999 MiB/sec; avg 1.61 ms; p99 6.32 ms |

The printed kacrab request counts are outer `send_batch_untracked` chunks:
306 chunks for 5M × 10B and 1042 chunks for 100K × 10 KiB. Internally, the
producer accumulator now splits same-partition batches at `batch.size`, and
the dispatcher packs those split batches across partitions in Java-like
ProduceRequest groups.

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
| 5M × 10B | 2.02 ms | 1.88 ms | 3.06 ms | 5.25 ms | 9.64 ms |
| 100K × 10 KiB | 1.63 ms | 1.44 ms | 3.65 ms | 6.72 ms | 12.86 ms |

These Java numbers are a same-props throughput comparison, not Java client
defaults (`acks=all`, idempotence enabled, and retries enabled). They are not
an apples-to-apples comparison with public rust-rdkafka README numbers either:
that quoted benchmark used older hardware/software, decimal MB/sec reporting,
and average-over-5 semantics.

Measured with Criterion `--sample-size 10` against local mock brokers:

- `producer_dispatcher/multi_broker_dispatch`: 9.50-9.80M messages/sec.
  Criterion reported `+109.67%` to `+122.84%` throughput versus the previous
  saved sample.
- `producer_accumulator/append_and_drain/1024`: 26.64-26.77M records/sec.
- `producer_accumulator/append_and_drain/16384`: 28.26-28.54M records/sec.

Producer mock broker executable:

- `producer_mock_bench`, 5M × 10B, 1 run: 11.15M messages/sec,
  106.33 MiB/sec, 0.448s, 306 mock produce requests.
- `producer_mock_bench`, 100K × 10 KiB, 1 run: 366K messages/sec,
  3574.44 MiB/sec, 0.273s, 1042 mock produce requests.

Wire pipeline:

- `wire_pipeline/api_versions_send_to_broker`, Criterion `--sample-size 10`:
  170.86-173.37K requests/sec.

### Limits Of This Pass

- Real Kafka and Java executable numbers are five-run smoke measurements, not
  release benchmark gates.
- The latest Rust latency pass was stable on throughput but still local-only:
  100K × 10 KiB kacrab std dev was 4,818 messages/sec, while the Java pass std
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
- `producer_kafka_bench` uses `send_batch_untracked`, so it does not measure
  per-record delivery handle allocation and wakeup overhead.

## Author

`kacrab-benches` is authored and maintained by `pirumu`.

## License

This crate is licensed under either MIT or Apache-2.0, matching the workspace.
