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
  (the current local sweet spot is `2` for a single native broker).
- `KACRAB_PARTITION_MODE=unassigned` uses the default Java-style sticky
  partitioner for null-key records. This is the default benchmark mode.
- `KACRAB_PARTITION_MODE=manual` keeps the older manual round-robin benchmark
  path for isolating partitioner overhead.
- `KACRAB_BATCH_SIZE` controls producer `batch.size`; default is 16 KiB.
- `KACRAB_BATCH_MESSAGES_10B` controls the outer API chunk size for the 10-byte
  scenario.
- `KACRAB_ONLY_10B=1` runs only the 5M × 10B scenario.

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

Measured on 2026-06-17 on this development machine. Treat these as local
measurement checkpoints, not production Kafka acceptance numbers.

Benchmark host:

- MacBook Pro, model identifier `Mac15,6`.
- Apple M3 Pro base chip: 11-core CPU (5 performance, 6 efficiency), 14-core
  GPU, 16-core Neural Engine.
- 18GB unified memory; M3 Pro memory bandwidth: 150GB/s.

Measured against native Apache Kafka 4.3.0 single-node KRaft on the same
machine, through the public producer API:

```bash
KACRAB_IN_FLIGHT=2 cargo run -p kacrab-benches --release --bin producer_kafka_bench
```

Rust `producer_kafka_bench` settings: `acks=1`, idempotence disabled, no
compression, `batch.size=16384`, 3 partitions, default
`partition_mode=unassigned` Java-style sticky partitioning.

- `producer_kafka_bench`, 5M × 10B: 5.08M messages/sec, 48.4 MiB/sec.
- `producer_kafka_bench`, 100K × 10 KiB: 133.9K messages/sec, 1.28 GiB/sec.

Same broker, same relaxed producer props through Apache Kafka's Java native
`kafka-producer-perf-test.sh`:

```bash
kafka-producer-perf-test.sh \
  --topic kacrab-bench \
  --num-records 5000000 \
  --record-size 10 \
  --throughput -1 \
  --producer-props \
  bootstrap.servers=127.0.0.1:9092 \
  acks=1 \
  enable.idempotence=false \
  compression.type=none \
  batch.size=16384 \
  linger.ms=5 \
  max.in.flight.requests.per.connection=2
```

- Java producer perf, 5M × 10B: 3.30M records/sec, 31.43 MB/sec.
- Java producer perf, 100K × 10 KiB: 35.4K records/sec, 345.69 MB/sec.

For the Java 100K × 10 KiB case, keep the same producer props and change
`--num-records 100000 --record-size 10240`.

These Java numbers are a same-props throughput comparison, not Java client
defaults (`acks=all`, idempotence enabled, and `max.in.flight=5`).

Measured with Criterion `--sample-size 10` against local mock brokers:

- `producer_dispatcher/multi_broker_dispatch`: 4.08-4.56M messages/sec.
- `producer_accumulator/append_and_drain/1024`: 14.67-20.00M records/sec.
- `producer_accumulator/append_and_drain/16384`: 15.48-21.98M records/sec.

Producer mock broker executable:

- `producer_mock_bench`, 5M × 10B: 5.38M messages/sec, 51.27 MiB/sec.
- `producer_mock_bench`, 100K × 10 KiB: 249K messages/sec, 2.38 GiB/sec.

Wire pipeline:

- `wire_pipeline/api_versions_send_to_broker`: 150-166K requests/sec.

## Author

`kacrab-benches` is authored and maintained by `pirumu`.

## License

This crate is licensed under either MIT or Apache-2.0, matching the workspace.
