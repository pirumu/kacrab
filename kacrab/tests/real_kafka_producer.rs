//! Real Kafka producer integration tests.

#![allow(
    clippy::arithmetic_side_effects,
    clippy::expect_used,
    clippy::panic,
    clippy::print_stdout,
    clippy::unwrap_used,
    reason = "Ignored real-broker tests are explicit smoke checks with direct failure output; \
              arithmetic runs over small bounded test counters."
)]

use std::{
    env,
    net::SocketAddr,
    process::Command,
    sync::atomic::{AtomicU32, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use kacrab::producer::{Producer, ProducerError, ProducerRecord};

#[tokio::test]
#[ignore = "requires local Kafka from docker-compose.kafka.yml"]
async fn real_kafka_commits_transactional_send() {
    let bootstrap = bootstrap_addr();
    let topic = topic();
    let transactional_id = transactional_id();
    println!(
        "real Kafka transactional smoke: bootstrap={bootstrap}, topic={topic}, \
         transactional.id={transactional_id}"
    );

    let producer = Producer::builder()
        .set("bootstrap.servers", bootstrap.to_string())
        .set("client.id", "kacrab-real-kafka-transaction-test")
        .set("transactional.id", transactional_id)
        .set("enable.idempotence", "true")
        .set("acks", "all")
        .set("retries", "3")
        .set("max.in.flight.requests.per.connection", "5")
        .set("request.timeout.ms", "30000")
        .set("delivery.timeout.ms", "120000")
        .set("transaction.timeout.ms", "60000")
        .set("batch.size", "1")
        .set("buffer.memory", "1048576")
        .build()
        .await
        .expect("producer should connect to local Kafka");

    producer
        .init_transactions()
        .await
        .expect("InitProducerId should succeed");
    producer
        .begin_transaction()
        .expect("transaction should begin after init");

    let delivery = producer
        .send(ProducerRecord::new(topic, 0).value(Bytes::from_static(b"kacrab-txn-smoke")))
        .expect("transactional send should enqueue and dispatch");

    producer
        .commit_transaction()
        .await
        .expect("EndTxn commit should succeed");

    let receipt = delivery.await.expect("delivery receipt should complete");
    assert_eq!(receipt.partition, 0);
    assert!(receipt.offset >= 0);
}

fn bootstrap_addr() -> SocketAddr {
    env::var("KACRAB_BOOTSTRAP")
        .unwrap_or_else(|_error| "127.0.0.1:9092".to_owned())
        .parse()
        .expect("KACRAB_BOOTSTRAP must be a socket address")
}

fn topic() -> String {
    env::var("KACRAB_BENCH_TOPIC").unwrap_or_else(|_error| "kacrab-bench".to_owned())
}

/// Process-unique suffix for topic/group names so concurrent and repeated runs
/// against the shared broker never collide.
fn unique_suffix() -> String {
    static COUNTER: AtomicU32 = AtomicU32::new(0);
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after unix epoch")
        .as_millis();
    let count = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{}-{millis}-{count}", std::process::id())
}

/// Create `topic` on the broker (compose disables auto topic creation): via the
/// compose container's CLI by default, or a native install when
/// `KACRAB_KAFKA_BIN` points at its `bin/`. `KACRAB_KAFKA_CONTAINER` retargets
/// the `docker exec` at a secondary fixture (default `kacrab-kafka`), and
/// `KACRAB_KAFKA_CONTAINER_BOOTSTRAP` sets the address that works from inside
/// it (default `localhost:9092`; a fixture on a non-default `KAFKA_HOST_PORT`
/// needs its INTERNAL listener, `kafka:29092`).
fn create_topic(topic: &str, partitions: u32) {
    let mut command = env::var("KACRAB_KAFKA_BIN").map_or_else(
        |_error| {
            let container = env::var("KACRAB_KAFKA_CONTAINER")
                .unwrap_or_else(|_error| "kacrab-kafka".to_owned());
            let container_bootstrap = env::var("KACRAB_KAFKA_CONTAINER_BOOTSTRAP")
                .unwrap_or_else(|_error| "localhost:9092".to_owned());
            let mut command = Command::new("docker");
            let _args = command
                .args(["exec", &container, "/opt/kafka/bin/kafka-topics.sh"])
                .args(["--bootstrap-server", &container_bootstrap]);
            command
        },
        |bin| {
            let mut command = Command::new(format!("{bin}/kafka-topics.sh"));
            let _args = command.args(["--bootstrap-server", &bootstrap_addr().to_string()]);
            command
        },
    );
    let status = command
        .args(["--create", "--topic", topic])
        .args(["--partitions", &partitions.to_string()])
        .args(["--replication-factor", "1"])
        .status()
        .expect("kafka-topics.sh should run");
    assert!(status.success(), "topic creation failed for {topic}");
}

fn now_millis() -> i64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after unix epoch")
        .as_millis();
    i64::try_from(millis).expect("epoch millis fit an i64")
}

fn transactional_id() -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after unix epoch")
        .as_millis();
    format!("kacrab-real-kafka-txn-{}-{millis}", std::process::id())
}

/// The EOS guarantee itself, asserted from both sides so a broken
/// implementation cannot pass:
///
/// - `read_committed` must see every committed record and NO aborted record and NO control marker —
///   this is the assertion whose absence let the producer ship without the KIP-98 transactional
///   attribute bit (markers were written but governed nothing, so aborted data stayed visible).
/// - `read_uncommitted` must SEE the aborted record — the negative control proving this test can
///   distinguish a working implementation from a broken one, rather than passing vacuously.
#[cfg(feature = "consumer")]
#[tokio::test]
#[ignore = "requires the broker from docker-compose.kafka.yml"]
async fn real_kafka_aborted_transaction_is_invisible_to_read_committed() {
    use std::time::Duration;

    use kacrab::{common::TopicPartition, consumer::Consumer};

    let bootstrap = bootstrap_addr();
    let topic = format!("kacrab-eos-{}", unique_suffix());
    let transactional_id = format!("kacrab-eos-txn-{}", unique_suffix());
    // The compose broker disables auto topic creation; producing to a
    // nonexistent topic never resolves (see the oversized/basic tests' setup).
    create_topic(&topic, 1);

    let producer = Producer::builder()
        .set("bootstrap.servers", bootstrap.to_string())
        .set("client.id", "kacrab-real-kafka-eos-test")
        .set("transactional.id", transactional_id)
        .set("enable.idempotence", "true")
        .set("acks", "all")
        .set("transaction.timeout.ms", "60000")
        .build()
        .await
        .expect("producer should connect to local Kafka");
    producer
        .init_transactions()
        .await
        .expect("InitProducerId should succeed");

    producer.begin_transaction().expect("begin commit txn");
    for value in ["committed-1", "committed-2"] {
        let delivery = producer
            .send(ProducerRecord::new(topic.clone(), 0).value(Bytes::from(value)))
            .expect("send");
        // Await the receipt so the record provably reached the log — an
        // unawaited aborted send would make the invisibility check vacuous.
        let _receipt = delivery.await.expect("committed delivery");
    }
    producer.commit_transaction().await.expect("commit");

    producer.begin_transaction().expect("begin abort txn");
    let delivery = producer
        .send(ProducerRecord::new(topic.clone(), 0).value(Bytes::from_static(b"aborted-1")))
        .expect("send");
    let _receipt = delivery.await.expect("aborted delivery reached the log");
    producer.abort_transaction().await.expect("abort");

    let read = |isolation: &'static str| {
        let bootstrap = bootstrap.to_string();
        let topic = topic.clone();
        async move {
            let group = format!("kacrab-eos-{isolation}-{}", unique_suffix());
            let mut consumer = Consumer::from_map([
                ("bootstrap.servers", bootstrap.as_str()),
                ("client.id", "kacrab-real-kafka-eos-reader"),
                ("group.id", group.as_str()),
                ("auto.offset.reset", "earliest"),
                ("enable.auto.commit", "false"),
                ("isolation.level", isolation),
            ])
            .await
            .expect("consumer should connect");
            consumer
                .assign([TopicPartition::new(topic.clone(), 0)])
                .expect("assign");
            let mut values: Vec<String> = Vec::new();
            let deadline = std::time::Instant::now() + Duration::from_secs(30);
            while std::time::Instant::now() < deadline {
                let records = consumer.poll(Duration::from_secs(2)).await.expect("poll");
                for record in records {
                    values.push(
                        record
                            .value
                            .map(|v| String::from_utf8_lossy(&v).into_owned())
                            .unwrap_or_default(),
                    );
                }
                // read_uncommitted terminates on seeing the aborted record;
                // read_committed terminates once both committed records landed
                // and one extra empty poll passed (giving a broken filter the
                // chance to leak the aborted record).
                if values.iter().any(|v| v == "aborted-1")
                    || (values.len() >= 2 && isolation == "read_committed")
                {
                    break;
                }
            }
            consumer.close().await;
            values
        }
    };

    let committed_view = read("read_committed").await;
    assert_eq!(
        committed_view,
        vec!["committed-1".to_owned(), "committed-2".to_owned()],
        "read_committed must see exactly the committed records — no aborted data, no control \
         markers"
    );

    let uncommitted_view = read("read_uncommitted").await;
    assert!(
        uncommitted_view.iter().any(|v| v == "aborted-1"),
        "negative control: read_uncommitted must see the aborted record, or this test could pass \
         against a producer that never wrote it: {uncommitted_view:?}"
    );
}

/// The basic produce contract: an explicit partition is honored exactly, a
/// fresh partition's delivery reports start at offset 0 and advance by exactly
/// one per record (so a receipt is the record's real log position, not a
/// fabricated value), and the receipt carries the documented metadata — real
/// serialized sizes, and for a `CreateTime` topic a `timestamp_ms` echoing the
/// record's create time (the send-time stamp when the user set none), matching
/// Java's `RecordMetadata.timestamp()`.
#[tokio::test]
#[ignore = "requires the broker from docker-compose.kafka.yml"]
async fn real_kafka_basic_produce_reports_real_offsets_and_partitions() {
    let topic = format!("kacrab-prod-basic-{}", unique_suffix());
    create_topic(&topic, 3);

    let producer = Producer::builder()
        .set("bootstrap.servers", bootstrap_addr().to_string())
        .set("client.id", "kacrab-real-kafka-basic-test")
        .set("acks", "all")
        .build()
        .await
        .expect("producer should connect to local Kafka");

    for partition in 0..3_i32 {
        let before_send_ms = now_millis();
        let receipt = producer
            .send(
                ProducerRecord::new(topic.clone(), partition)
                    .key(Bytes::from_static(b"k"))
                    .value(Bytes::from(format!("first-{partition}"))),
            )
            .expect("send should enqueue")
            .await
            .expect("delivery should complete");
        assert_eq!(
            receipt.partition, partition,
            "the explicitly assigned partition must be honored"
        );
        assert_eq!(
            receipt.topic.as_ref(),
            topic.as_str(),
            "receipt names the produced topic"
        );
        assert_eq!(
            receipt.offset, 0,
            "the first record on a fresh partition sits at offset 0"
        );
        assert_eq!(receipt.serialized_key_size, 1, "key was 1 byte");
        assert_eq!(
            receipt.serialized_value_size,
            i32::try_from(format!("first-{partition}").len()).unwrap(),
            "value size matches the payload"
        );
        let after_send_ms = now_millis();
        assert!(
            (before_send_ms..=after_send_ms).contains(&receipt.timestamp_ms),
            "a CreateTime receipt echoes the send-time stamp (Java parity): {before_send_ms} <= \
             {} <= {after_send_ms}",
            receipt.timestamp_ms
        );
    }

    for expected_offset in 1..=5_i64 {
        let receipt = producer
            .send(
                ProducerRecord::new(topic.clone(), 0)
                    .value(Bytes::from(format!("seq-{expected_offset}"))),
            )
            .expect("send should enqueue")
            .await
            .expect("delivery should complete");
        assert_eq!(
            receipt.offset, expected_offset,
            "sequential awaited sends advance the offset by exactly one — no gaps, no repeats"
        );
    }
}

/// `flush` returns only after every previously sent record has a delivery
/// result: after `flush().await`, each outstanding `SendFuture` must already be
/// resolved (polled with a zero timeout, so a flush that returned early would
/// fail here rather than being hidden by the await doing the waiting).
#[tokio::test]
#[ignore = "requires the broker from docker-compose.kafka.yml"]
async fn real_kafka_flush_completes_all_outstanding_deliveries() {
    let topic = format!("kacrab-prod-flush-{}", unique_suffix());
    create_topic(&topic, 1);

    let producer = Producer::builder()
        .set("bootstrap.servers", bootstrap_addr().to_string())
        .set("client.id", "kacrab-real-kafka-flush-test")
        .set("acks", "all")
        .set("linger.ms", "5000")
        .build()
        .await
        .expect("producer should connect to local Kafka");

    let count = 50_usize;
    let mut deliveries = Vec::with_capacity(count);
    for index in 0..count {
        deliveries.push(
            producer
                .send(
                    ProducerRecord::new(topic.clone(), 0).value(Bytes::from(format!("f-{index}"))),
                )
                .expect("send should enqueue"),
        );
    }

    producer.flush().await.expect("flush should succeed");

    let mut offsets = Vec::with_capacity(count);
    for (index, delivery) in deliveries.into_iter().enumerate() {
        let receipt = tokio::time::timeout(Duration::ZERO, delivery)
            .await
            .unwrap_or_else(|_elapsed| {
                panic!("record {index} was still undelivered after flush returned")
            })
            .expect("delivery should have succeeded");
        offsets.push(receipt.offset);
    }
    let expected: Vec<i64> = (0..i64::try_from(count).unwrap()).collect();
    assert_eq!(
        offsets, expected,
        "flushed records occupy exactly offsets 0..count in send order"
    );
}

/// The default partitioner's keyed contract: a record key alone determines the
/// partition (murmur2, stable across records and across producer instances),
/// while distinct keys actually spread over the partition space — the negative
/// control proving partition choice is key-driven rather than constant.
#[tokio::test]
#[ignore = "requires the broker from docker-compose.kafka.yml"]
async fn real_kafka_keyed_partitioning_is_stable_and_spreads() {
    let topic = format!("kacrab-prod-keyed-{}", unique_suffix());
    create_topic(&topic, 6);

    let build = || async {
        Producer::builder()
            .set("bootstrap.servers", bootstrap_addr().to_string())
            .set("client.id", "kacrab-real-kafka-keyed-test")
            .set("acks", "all")
            .build()
            .await
            .expect("producer should connect to local Kafka")
    };
    let producer = build().await;

    let mut fixed_key_partitions = std::collections::HashSet::new();
    for index in 0..20_usize {
        let receipt = producer
            .send(
                ProducerRecord::unassigned(topic.clone())
                    .key(Bytes::from_static(b"stable-key"))
                    .value(Bytes::from(format!("same-{index}"))),
            )
            .expect("send should enqueue")
            .await
            .expect("delivery should complete");
        let _new = fixed_key_partitions.insert(receipt.partition);
    }
    assert_eq!(
        fixed_key_partitions.len(),
        1,
        "every send with the same key must land on one partition: {fixed_key_partitions:?}"
    );

    let second_producer = build().await;
    let receipt = second_producer
        .send(
            ProducerRecord::unassigned(topic.clone())
                .key(Bytes::from_static(b"stable-key"))
                .value(Bytes::from_static(b"other-producer")),
        )
        .expect("send should enqueue")
        .await
        .expect("delivery should complete");
    assert!(
        fixed_key_partitions.contains(&receipt.partition),
        "keyed partitioning is a function of the key, not the producer instance"
    );

    let mut spread_partitions = std::collections::HashSet::new();
    for index in 0..64_usize {
        let receipt = producer
            .send(
                ProducerRecord::unassigned(topic.clone())
                    .key(Bytes::from(format!("spread-key-{index}")))
                    .value(Bytes::from(format!("spread-{index}"))),
            )
            .expect("send should enqueue")
            .await
            .expect("delivery should complete");
        assert!(
            (0..6).contains(&receipt.partition),
            "keyed partition must exist in the topic"
        );
        let _new = spread_partitions.insert(receipt.partition);
    }
    assert!(
        spread_partitions.len() >= 3,
        "64 distinct keys over 6 partitions must spread (negative control against a constant \
         partitioner); hit only {spread_partitions:?}"
    );
}

/// A record over `max.request.size` fails with the documented
/// [`ProducerError::RecordTooLarge`] and must not wedge its partition: the
/// idempotent sequence may not be advanced (or left unresolved) by a record
/// that never reached the wire, so the next normal send still delivers at the
/// very next offset. Guards the fixed in-task-retry sequence-gap bug.
#[tokio::test]
#[ignore = "requires the broker from docker-compose.kafka.yml"]
async fn real_kafka_oversized_record_fails_without_wedging_the_partition() {
    let topic = format!("kacrab-prod-oversize-{}", unique_suffix());
    create_topic(&topic, 1);

    let producer = Producer::builder()
        .set("bootstrap.servers", bootstrap_addr().to_string())
        .set("client.id", "kacrab-real-kafka-oversize-test")
        .set("acks", "all")
        .set("enable.idempotence", "true")
        .set("max.request.size", "16384")
        .build()
        .await
        .expect("producer should connect to local Kafka");

    let receipt = producer
        .send(ProducerRecord::new(topic.clone(), 0).value(Bytes::from_static(b"before")))
        .expect("send should enqueue")
        .await
        .expect("delivery should complete");
    assert_eq!(receipt.offset, 0, "warm-up record sits at offset 0");

    let oversized = ProducerRecord::new(topic.clone(), 0).value(Bytes::from(vec![0_u8; 65_536]));
    let error = match producer.send(oversized) {
        Err(error) => error,
        Ok(delivery) => delivery
            .await
            .expect_err("a record above max.request.size must not be delivered"),
    };
    assert!(
        matches!(
            error,
            ProducerError::RecordTooLarge {
                max_request_size: 16_384,
                ..
            }
        ),
        "oversized record must fail with RecordTooLarge naming the configured bound: {error}"
    );

    let receipt = tokio::time::timeout(
        Duration::from_secs(30),
        producer
            .send(ProducerRecord::new(topic.clone(), 0).value(Bytes::from_static(b"after")))
            .expect("send after the oversized failure should enqueue"),
    )
    .await
    .expect("the partition must not be wedged by the failed record")
    .expect("delivery after the oversized failure should complete");
    assert_eq!(
        receipt.offset, 1,
        "the failed record must not consume an idempotent sequence number or a log offset"
    );
}

/// Headers and an explicit timestamp survive the full produce→broker→consume
/// path byte-for-byte: order and duplicate keys preserved, null header value
/// kept distinct from empty, and the consumed record reports the producer's
/// exact `CreateTime` timestamp.
#[cfg(feature = "consumer")]
#[tokio::test]
#[ignore = "requires the broker from docker-compose.kafka.yml"]
async fn real_kafka_headers_and_timestamp_roundtrip() {
    use kacrab::{
        common::TopicPartition,
        consumer::{Consumer, TimestampType},
        producer::RecordHeader,
    };

    let topic = format!("kacrab-prod-headers-{}", unique_suffix());
    create_topic(&topic, 1);
    let timestamp_ms = now_millis() - 12_345;

    let producer = Producer::builder()
        .set("bootstrap.servers", bootstrap_addr().to_string())
        .set("client.id", "kacrab-real-kafka-headers-test")
        .set("acks", "all")
        .build()
        .await
        .expect("producer should connect to local Kafka");

    let record = ProducerRecord::new(topic.clone(), 0)
        .key(Bytes::from_static(b"header-key"))
        .value(Bytes::from_static(b"header-value"))
        .try_timestamp_ms(timestamp_ms)
        .expect("explicit timestamp is valid")
        .header(Bytes::from_static(b"h1"), Bytes::from_static(b"v1"))
        .header_null(Bytes::from_static(b"h-null"))
        .header(Bytes::from_static(b"h1"), Bytes::from_static(b"v2"))
        .header(Bytes::from_static(b"h-empty"), Bytes::new());
    let receipt = producer
        .send(record)
        .expect("send should enqueue")
        .await
        .expect("delivery should complete");
    assert_eq!(
        receipt.timestamp_ms, timestamp_ms,
        "the receipt echoes an explicit create-time timestamp exactly (Java parity)"
    );

    let group = format!("kacrab-headers-{}", unique_suffix());
    let mut consumer = Consumer::from_map([
        ("bootstrap.servers", bootstrap_addr().to_string().as_str()),
        ("group.id", group.as_str()),
        ("auto.offset.reset", "earliest"),
        ("enable.auto.commit", "false"),
    ])
    .await
    .expect("consumer should connect");
    consumer
        .assign([TopicPartition::new(topic.clone(), 0)])
        .expect("assign");

    let mut collected = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while collected.is_empty() && std::time::Instant::now() < deadline {
        let records = consumer.poll(Duration::from_secs(2)).await.expect("poll");
        collected.extend(records);
    }
    consumer.close().await;

    let record = collected
        .first()
        .expect("the produced record must come back");
    assert_eq!(
        record.key.as_deref(),
        Some(b"header-key".as_slice()),
        "key round-trips"
    );
    assert_eq!(
        record.value.as_deref(),
        Some(b"header-value".as_slice()),
        "value round-trips"
    );
    assert_eq!(
        record.timestamp, timestamp_ms,
        "the consumed record carries the producer's explicit timestamp, not broker time"
    );
    assert_eq!(
        record.timestamp_type,
        TimestampType::CreateTime,
        "an explicit producer timestamp is CreateTime"
    );
    assert_eq!(
        record.headers,
        vec![
            RecordHeader {
                key: Bytes::from_static(b"h1"),
                value: Some(Bytes::from_static(b"v1")),
            },
            RecordHeader {
                key: Bytes::from_static(b"h-null"),
                value: None,
            },
            RecordHeader {
                key: Bytes::from_static(b"h1"),
                value: Some(Bytes::from_static(b"v2")),
            },
            RecordHeader {
                key: Bytes::from_static(b"h-empty"),
                value: Some(Bytes::new()),
            },
        ],
        "headers round-trip exactly: order and duplicate keys preserved, null distinct from empty"
    );
}

/// The idempotence guarantee under a concurrent burst: thousands of unawaited
/// sends across partitions (so batches race, retry, and interleave) yield
/// exactly one log record per send — per-partition offsets are gapless from 0
/// (nothing lost) and every payload appears exactly once (nothing duplicated),
/// verified by consuming the whole topic back rather than trusting receipts.
#[cfg(feature = "consumer")]
#[tokio::test]
#[ignore = "requires the broker from docker-compose.kafka.yml"]
#[expect(
    clippy::too_many_lines,
    reason = "The burst, receipt, and consume-back phases form one indivisible invariant check."
)]
async fn real_kafka_idempotent_burst_delivers_exactly_once() {
    use std::collections::{HashMap, HashSet};

    use kacrab::{common::TopicPartition, consumer::Consumer};

    const PARTITIONS: i32 = 3;
    const TOTAL: usize = 3000;

    let topic = format!("kacrab-prod-idem-{}", unique_suffix());
    create_topic(&topic, u32::try_from(PARTITIONS).unwrap());

    let producer = Producer::builder()
        .set("bootstrap.servers", bootstrap_addr().to_string())
        .set("client.id", "kacrab-real-kafka-idem-test")
        .set("acks", "all")
        .set("enable.idempotence", "true")
        .set("max.in.flight.requests.per.connection", "5")
        .set("linger.ms", "2")
        .build()
        .await
        .expect("producer should connect to local Kafka");

    let mut deliveries = Vec::with_capacity(TOTAL);
    for index in 0..TOTAL {
        let partition = i32::try_from(index).unwrap() % PARTITIONS;
        deliveries.push(
            producer
                .send(
                    ProducerRecord::new(topic.clone(), partition)
                        .value(Bytes::from(format!("burst-{index}"))),
                )
                .expect("send should enqueue"),
        );
    }
    producer.flush().await.expect("flush should succeed");

    let mut receipt_slots: HashSet<(i32, i64)> = HashSet::new();
    for delivery in deliveries {
        let receipt = delivery.await.expect("every burst delivery must succeed");
        assert!(
            receipt_slots.insert((receipt.partition, receipt.offset)),
            "two receipts claimed the same log slot {}-{}",
            receipt.partition,
            receipt.offset
        );
    }
    assert_eq!(receipt_slots.len(), TOTAL, "one receipt per record");

    let group = format!("kacrab-idem-{}", unique_suffix());
    let mut consumer = Consumer::from_map([
        ("bootstrap.servers", bootstrap_addr().to_string().as_str()),
        ("group.id", group.as_str()),
        ("auto.offset.reset", "earliest"),
        ("enable.auto.commit", "false"),
    ])
    .await
    .expect("consumer should connect");
    consumer
        .assign((0..PARTITIONS).map(|partition| TopicPartition::new(topic.clone(), partition)))
        .expect("assign");

    let mut seen_values: HashSet<String> = HashSet::new();
    let mut consumed_slots: HashSet<(i32, i64)> = HashSet::new();
    let mut per_partition: HashMap<i32, Vec<i64>> = HashMap::new();
    let deadline = std::time::Instant::now() + Duration::from_mins(1);
    let mut drained_extra_poll = false;
    while std::time::Instant::now() < deadline {
        let records = consumer.poll(Duration::from_secs(2)).await.expect("poll");
        let empty = records.is_empty();
        for record in records {
            let value = record.value.expect("burst records carry a value");
            assert!(
                seen_values.insert(String::from_utf8(value.to_vec()).expect("utf-8 payload")),
                "duplicate payload consumed at {}-{}",
                record.partition,
                record.offset
            );
            assert!(
                consumed_slots.insert((record.partition, record.offset)),
                "duplicate log slot consumed at {}-{}",
                record.partition,
                record.offset
            );
            per_partition
                .entry(record.partition)
                .or_default()
                .push(record.offset);
        }
        // One extra empty poll past TOTAL gives a duplicating producer the
        // chance to surface its extra records before the count assertion.
        if seen_values.len() >= TOTAL {
            if drained_extra_poll {
                break;
            }
            drained_extra_poll = empty;
        }
    }
    consumer.close().await;

    assert_eq!(
        seen_values.len(),
        TOTAL,
        "the burst must yield exactly one record per send"
    );
    for index in 0..TOTAL {
        assert!(
            seen_values.contains(&format!("burst-{index}")),
            "record burst-{index} was lost"
        );
    }
    assert_eq!(
        consumed_slots, receipt_slots,
        "the consumed log slots must be exactly the acknowledged ones"
    );
    for (partition, mut offsets) in per_partition {
        offsets.sort_unstable();
        let expected: Vec<i64> =
            (0..i64::try_from(TOTAL).unwrap() / i64::from(PARTITIONS)).collect();
        assert_eq!(
            offsets, expected,
            "partition {partition} offsets must be gapless from 0 — a gap means a sequence was \
             burned without a record, a repeat means a duplicate"
        );
    }
}

/// Regression: producing to a topic that does not exist (auto-create is off on
/// every compose fixture) must fail the delivery with `DeliveryTimeout` once
/// `delivery.timeout.ms` elapses. The unroutable batch used to requeue
/// unboundedly — the delivery future neither resolved nor errored for tens of
/// minutes (both CI base-broker legs hit their 30-minute job timeout on
/// exactly this) — because the requeue path never checked the delivery
/// deadline that every retry path enforces.
#[tokio::test]
#[ignore = "requires local Kafka from docker-compose.kafka.yml"]
async fn real_kafka_missing_topic_send_fails_after_delivery_timeout() {
    let bootstrap = bootstrap_addr();
    let topic = format!("kacrab-missing-{}", unique_suffix());

    let producer = Producer::builder()
        .set("bootstrap.servers", bootstrap.to_string())
        .set("client.id", "kacrab-missing-topic-test")
        .set("request.timeout.ms", "2000")
        .set("linger.ms", "0")
        .set("delivery.timeout.ms", "5000")
        .build()
        .await
        .expect("producer should connect to local Kafka");

    let started = std::time::Instant::now();
    let delivery = producer
        .send(ProducerRecord::new(topic.clone(), 0).value(Bytes::from_static(b"never-lands")))
        .expect("send should enqueue");
    let result = tokio::time::timeout(Duration::from_mins(1), delivery)
        .await
        .expect("the delivery future must resolve — an unbounded hang is the fixed bug");
    let elapsed = started.elapsed();
    match result {
        Err(ProducerError::DeliveryTimeout {
            topic: failed_topic,
            partition,
        }) => {
            assert_eq!(
                failed_topic, topic,
                "the timeout names the unroutable topic"
            );
            assert_eq!(partition, 0);
        },
        other => panic!("expected DeliveryTimeout for the missing topic, got {other:?}"),
    }
    assert!(
        elapsed >= Duration::from_secs(5),
        "the failure must honor delivery.timeout.ms, not fail eagerly: {elapsed:?}"
    );
}
