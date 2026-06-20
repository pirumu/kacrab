# Benchmark parity audit: Java vs kacrab producer

Muc tieu cua file nay: khoa lai nhung diem phai giong nhau 100% truoc khi dung benchmark de ket luan nhanh/cham. Neu config, producer semantics, hoac metric semantics khac nhau thi throughput/latency chi la so tham khao.

Nguon da doi chieu:

- Apache Kafka 4.3 Producer Configs: https://kafka.apache.org/43/configuration/producer-configs/
- Local Java producer defaults snapshot: `/Users/nat/.local/share/kacrab-kafka/current/config/producer.properties`
- Rust config path: `ProducerBuilder -> ClientConfig -> ProducerConfig -> ProducerRuntimeConfig`
- Rust bench path: `benches/src/bin/producer_kafka_bench.rs`

## 1. Default config cua Java

Java producer default dang can benchmark theo Kafka 4.3 defaults, chi set nhung gia tri bat buoc de chay benchmark.

| Config | Java default / bench value | Ghi chu parity |
| --- | ---: | --- |
| `bootstrap.servers` | required, bench set `127.0.0.1:9092` | Khong co default that. |
| `client.id` | `""`, bench set `kafka-producer-perf-test`/Makefile client id | Nen in ra trong output. |
| `acks` | `all` | Kafka 4.3 default. |
| `enable.idempotence` | `true` | Keo theo constraint `acks=all`, `retries>0`, `max.in.flight<=5`. |
| `retries` | `2147483647` | Java gan nhu retry den khi `delivery.timeout.ms` het. |
| `max.in.flight.requests.per.connection` | `5` | Default Java va limit idempotent producer. |
| `batch.size` | `16384` bytes | Per partition batch target. |
| `linger.ms` | `5` ms | Kafka 4.0+ doi default tu 0 sang 5. |
| `buffer.memory` | `33554432` bytes | 32 MiB producer buffer. |
| `compression.type` | `none` | Khong nen set compression rieng trong baseline. |
| `delivery.timeout.ms` | `120000` ms | Outer delivery bound. |
| `request.timeout.ms` | `30000` ms | Broker response timeout. |
| `max.block.ms` | `60000` ms | Cho metadata / buffer memory. |
| `max.request.size` | `1048576` bytes | Anh huong kich ban 10 KiB. |
| `send.buffer.bytes` | `131072` bytes | Socket send buffer. |
| `receive.buffer.bytes` | `32768` bytes | Socket receive buffer. |
| `metadata.max.age.ms` | `300000` ms | Metadata refresh age. |
| `partitioner.adaptive.partitioning.enable` | `true` | Java co adaptive sticky partitioning. |
| `partitioner.availability.timeout.ms` | `0` | Availability exclusion disabled by default. |
| `metric.reporters` | `org.apache.kafka.common.metrics.JmxReporter` | Java metrics backend khac Rust. |
| `metrics.num.samples` | `2` | Java client metrics, khong phai producer-perf latency summary. |
| `metrics.sample.window.ms` | `30000` | Java client metrics window. |
| `metrics.recording.level` | `INFO` | Default metrics recording. |
| `enable.metrics.push` | `true` | Client telemetry push default. |

Java benchmark scenario can giu dung:

- `5,000,000` records, `10` bytes, wait all acked.
- `100,000` records, `10 KiB`, wait all acked.
- `--throughput -1`.
- Khong set producer properties ngoai nhung cai bat buoc de chay.

## 2. Default config cua Rust/kacrab

Bench Rust hien tai build producer bang:

```text
Producer::builder()
  .set("bootstrap.servers", ...)
  .set("client.id", "kacrab-producer-kafka-bench")
  .build()
```

Duong public builder nay di qua typed `ProducerConfig`, nen effective default cua benchmark la:

| Config | Rust effective default / bench value | Trang thai parity |
| --- | ---: | --- |
| `bootstrap.servers` | bench set `127.0.0.1:9092` | Giong Java ve y nghia. |
| `client.id` | bench set `kacrab-producer-kafka-bench` | Khac ten, nen in ra. |
| `acks` | `all` | Giong Java. |
| `enable.idempotence` | `true` | Giong Java tren config. |
| `retries` | `2147483647` -> runtime `usize` | Giong y nghia neu retry path tuan theo delivery timeout. |
| `max.in.flight.requests.per.connection` | `5` | Giong Java. |
| `batch.size` | `16384` bytes | Giong Java. |
| `linger.ms` | `5` ms qua `ProducerConfig` | Giong Java trong public builder. |
| raw `AccumulatorConfig::default().linger` | `0` ms | Khac Java, nhung khong phai path bench public. Can canh giac neu test dung `Producer::from_parts`/runtime config. |
| `buffer.memory` | `33554432` bytes | Giong Java ve value; implementation memory accounting chua chac giong Java. |
| `compression.type` | `none` | Giong Java. |
| `delivery.timeout.ms` | `120000` ms | Giong Java. |
| `request.timeout.ms` | `30000` ms | Giong Java. |
| `max.block.ms` | `60000` ms | Giong Java. |
| `max.request.size` | `1048576` bytes | Giong Java. |
| `send.buffer.bytes` | `131072` bytes | Giong typed config. |
| `receive.buffer.bytes` | `32768` bytes | Giong typed config. |
| `metadata.max.age.ms` | `300000` ms | Giong typed config. |
| `partitioner.adaptive.partitioning.enable` | `true` | Config giong, implementation can verify bang metric. |
| `partitioner.availability.timeout.ms` | `0` | Giong Java. |
| `enable.metrics.push` | `true` | Config giong, telemetry implementation khong dong nghia metric parity. |

Ket luan config: baseline public builder cua Rust da gan config Java ve cac key chinh. Diem can khoa lai bang code/output la effective config dump truoc bench, vi internal raw default `AccumulatorConfig` co `linger=0` va co the bi dung nham trong test/bench khac.

## 3. Java producer co gi ma Rust chua chac co

Day la cac diem lam so bench khac nhau du config giong nhau:

- Java co `RecordAccumulator` mature voi per-partition `ProducerBatch`, memory buffer pool, batch append truc tiep vao buffer, va semantics block theo `buffer.memory`/`max.block.ms`.
- Java co background `Sender` + `NetworkClient` + selector nonblocking, quan ly in-flight theo broker, metadata refresh, retry/backoff, delivery timeout va node readiness rat lau nam.
- Java co sticky/adaptive partitioner da duoc toi uu va do bang queue load/broker drain behavior. Rust co config va logic lien quan, nhung can metric chung minh behavior tuong duong.
- Java producer co idempotence sequence/producer id/retry ordering semantics day du trong hot path. Rust co idempotence config/state, nhung can stress test retry/leadership error de goi la parity.
- Java co serializers/interceptors/partitioner/plugin ecosystem va JMX metrics reporters. Rust co native equivalent rieng, khong phai cung backend.
- Java `kafka-producer-perf-test` goi send theo tung record va callback tung record. Rust bench default hien tai dung `KACRAB_BENCH_API=per-record`, goi public `send_with_callback` tung record; batched API chi con la opt-in diagnostic path.
- Java memory allocation path da duoc amortize quanh `ByteBuffer`/batch pool. Rust per-record bench van tao `ProducerRecord` va clone `Bytes` handle tung record; batched diagnostic path con gom record vao `Vec<ProducerRecord>`.

Ket luan producer feature: config gan giong chua du. Muon bench co gia tri 100%, Rust phai do cung send semantics hoac phai ghi ro benchmark dang do "batched public API" chu khong phai Java `send(record)` parity.

## 4. Metric Rust da giong Java chua

Chua giong 100%.

Nhung diem da gan:

- Rust bench da co 2 scenario co dinh: `5M x 10B` va `100k x 10KiB`.
- Rust bench da wait ack bang tracked callback + final `flush()`.
- Rust default bench API la per-record, in `delivery_mode=per-record`, va co opt-in `KACRAB_BENCH_API=batched` cho diagnostic khong dung de so Java producer-perf mac dinh.
- Rust latency metric da dung start timestamp tung record va chi record completion thanh cong, bao ve bang unit tests cho per-record va batched accounting.
- Rust output summary format dang bat chuoc `kafka-producer-perf-test`: records/sec, MB/sec, avg/max/p50/p95/p99/p99.9 latency.
- MB/sec dang dung MiB divisor `1024 * 1024`, phu hop voi output Java producer-perf.
- Java wrapper da chay 5 runs/scenario, in effective config moi run, va in `java average counters`; Rust bench cung in five-run average counters.

Nhung diem chua parity:

- Rust producer semantics van khac Java: Java co background `Sender` + `NetworkClient` mature, trong khi Rust public send path van con tham gia append/dispatch cadence va sender loop hien chua tach hoan toan append fast path khoi async dispatch owner.
- Rust average va Java average da cung 5 runs, nhung so sanh throughput van phai dung cung-machine fresh Rust + Java output cung counters; khong duoc lay Rust-only snapshot lam parity proof.
- Rust custom `ProducerPerformanceStats` chi bat chuoc producer-perf output, khong phai Java client `Metrics`/JMX sensors.
- Rust metric noi bo nhu broker requests, retries, flush latency la he metric rieng; chua map 1-1 sang Java metrics names.

Ket luan metric: harness da gan hon ve API shape, latency timestamp, counters va 5-run average. Van khong duoc ket luan "parity" hay "nhanh/cham hon Java" neu khong co fresh Rust + Java run cung may, cung topic/config, va counters khop.

## Viec can lam de bench co gia tri 100%

1. In effective config snapshot o dau moi run cho ca Java va Rust: tat ca key trong bang tren, khong chi noi "default".
2. Giu Rust latency metric theo timestamp tung record; moi thay doi callback delivery/bench send loop phai giu tests accounting xanh.
3. Chon mot trong hai baseline API:
   - parity voi Java producer-perf: Rust benchmark default phai tiep tuc goi public per-record `send_with_callback`, de producer tu batch ben trong;
   - batched API benchmark chi dung khi co Java benchmark rieng co semantics tuong duong, khong dung producer-perf mac dinh de so truc tiep.
4. Cho Java cung chay 5 runs moi scenario va average cung cong thuc voi Rust. Trang thai hien tai: Make target Java da lam viec nay; can giu khi sua wrapper.
5. Emit batch/request counters o ca hai ben: produce requests, record batches, records/batch, bytes/request, retries, errors, in-flight stalls. Trang thai hien tai: hai ben da co compact counters, nhung mot so truong khong 1-1 (`batch_splits`, exact Java record batch count).
6. Them test bao ve bench shape: chi 2 scenario, default config only, no env variant lam thay doi config nong.
7. Verify rieng 10 KiB scenario: request split theo `max.request.size`, so ProduceRequest, so batch/partition, va error/retry count.

Trang thai hien tai: config baseline, benchmark API shape, per-record latency timestamp, Java 5-run average va compact counters da gan hon nhieu. Producer internals van chua Java parity, dac biet sender/accumulator/background IO-owner shape; Rust khong duoc report parity.
