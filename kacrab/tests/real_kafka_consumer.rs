#![cfg(all(feature = "producer", feature = "consumer", feature = "admin"))]
//! Real Kafka consumer integration test (manual assignment + fetch + commit).
//!
//! Creates a topic (admin), produces a known set of records with the kacrab
//! producer, consumes them back with the kacrab consumer via manual partition
//! assignment, then commits and reads the committed offset (Phase 2a), against a
//! real Apache Kafka 4.3.0 broker from
//! `docker-compose.kafka.yml` (which disables broker auto topic creation). Run:
//! `cargo test --features producer,consumer,admin --test real_kafka_consumer -- --ignored
//! --nocapture`.

#![allow(
    clippy::expect_used,
    clippy::print_stdout,
    clippy::unwrap_used,
    reason = "Ignored real-broker test is an explicit smoke check with direct failure output."
)]

use std::{
    env,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use kacrab::{
    admin::{AdminClient, CreateTopicsOptions, NewTopic},
    common::{OffsetAndMetadata, TopicPartition},
    consumer::{Consumer, ConsumerInterceptor, ConsumerRecords, InterceptorConfigs},
    producer::{Producer, ProducerRecord},
};

const RECORD_COUNT: usize = 10;

#[tokio::test]
#[ignore = "requires local Kafka from docker-compose.kafka.yml"]
async fn real_kafka_consumes_produced_records() {
    let bootstrap = bootstrap();
    let topic = topic();
    println!("real Kafka consumer smoke: bootstrap={bootstrap}, topic={topic}");

    // --- create the topic (broker auto-create is disabled) ---
    let admin = AdminClient::from_map([("bootstrap.servers", bootstrap.as_str())])
        .await
        .expect("admin should connect to local Kafka");
    admin
        .create_topics(
            vec![NewTopic::new(topic.clone(), 1, 1)],
            CreateTopicsOptions::default(),
        )
        .await
        .expect("create_topics should succeed");

    // --- produce a known set of records to partition 0 ---
    let producer = Producer::builder()
        .set("bootstrap.servers", bootstrap.clone())
        .set("client.id", "kacrab-real-kafka-consumer-producer")
        .set("enable.idempotence", "true")
        .set("acks", "all")
        .set("batch.size", "1")
        .build()
        .await
        .expect("producer should connect to local Kafka");

    for i in 0..RECORD_COUNT {
        let record = ProducerRecord::new(topic.clone(), 0)
            .key(Bytes::from(format!("k{i}")))
            .value(Bytes::from(format!("v{i}")));
        let delivery = producer.send(record).expect("send should enqueue");
        let receipt = delivery.await.expect("delivery should complete");
        println!("  produced offset={} for k{i}", receipt.offset);
    }

    // --- consume them back with manual assignment ---
    let group_id = format!("group-{topic}");
    let mut consumer = Consumer::from_map([
        ("bootstrap.servers", bootstrap.as_str()),
        ("client.id", "kacrab-real-kafka-consumer"),
        ("group.id", group_id.as_str()),
        ("auto.offset.reset", "earliest"),
        ("enable.auto.commit", "false"),
    ])
    .await
    .expect("consumer should connect to local Kafka");
    assert_eq!(consumer.group_metadata().group_id, group_id);

    let partition = TopicPartition::new(topic.clone(), 0);
    consumer.assign([partition.clone()]);
    assert_eq!(consumer.assignment(), vec![partition.clone()]);

    let mut collected: Vec<(Option<String>, Option<String>, i64)> = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while collected.len() < RECORD_COUNT && std::time::Instant::now() < deadline {
        let records = consumer
            .poll(Duration::from_secs(2))
            .await
            .expect("poll should succeed");
        for record in &records {
            collected.push((
                record.key.as_ref().map(bytes_to_string),
                record.value.as_ref().map(bytes_to_string),
                record.offset,
            ));
        }
    }

    println!("  consumed {} records", collected.len());
    assert_eq!(
        collected.len(),
        RECORD_COUNT,
        "should consume every produced record"
    );
    for (i, (key, value, offset)) in collected.iter().enumerate() {
        assert_eq!(key.as_deref(), Some(format!("k{i}").as_str()));
        assert_eq!(value.as_deref(), Some(format!("v{i}").as_str()));
        println!("  record[{i}] offset={offset} key={key:?} value={value:?}");
    }

    // Position advanced past the last consumed record.
    let position = consumer
        .position(&partition)
        .await
        .expect("position should resolve");
    assert_eq!(usize::try_from(position).unwrap(), RECORD_COUNT);

    // --- commit and read back the committed offset (Phase 2a) ---
    consumer
        .commit_sync()
        .await
        .expect("commit_sync should succeed");
    let committed = consumer
        .committed(std::slice::from_ref(&partition))
        .await
        .expect("committed should succeed");
    let committed_offset = committed
        .get(&partition)
        .expect("committed offset should be present after commit");
    println!("  committed offset={}", committed_offset.offset);
    assert_eq!(
        usize::try_from(committed_offset.offset).unwrap(),
        RECORD_COUNT
    );

    consumer.close().await;
    println!("real Kafka consumer smoke: ALL OK");
}

fn bytes_to_string(bytes: &Bytes) -> String {
    String::from_utf8(bytes.to_vec()).expect("record payload should be utf-8")
}

fn bootstrap() -> String {
    env::var("KACRAB_BOOTSTRAP").unwrap_or_else(|_error| "127.0.0.1:9092".to_owned())
}

fn topic() -> String {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be after epoch")
        .as_millis();
    format!("kacrab-consumer-smoke-{nonce}")
}

/// One consumer subscribing to a two-partition topic should be assigned both
/// partitions (via JoinGroup/SyncGroup + the range assignor) and consume every
/// record.
#[tokio::test]
#[ignore = "requires local Kafka from docker-compose.kafka.yml"]
async fn real_kafka_subscribe_consumes_all_partitions() {
    let bootstrap = bootstrap();
    let topic = topic();
    let per_partition = 5;
    println!("real Kafka subscribe smoke: bootstrap={bootstrap}, topic={topic}");

    create_topic(&bootstrap, &topic, 2).await;
    produce(&bootstrap, &topic, 0, per_partition).await;
    produce(&bootstrap, &topic, 1, per_partition).await;

    let group_id = format!("group-sub-{topic}");
    let mut consumer = Consumer::from_map([
        ("bootstrap.servers", bootstrap.as_str()),
        ("client.id", "kacrab-subscribe-consumer"),
        ("group.id", group_id.as_str()),
        ("auto.offset.reset", "earliest"),
        ("enable.auto.commit", "false"),
    ])
    .await
    .expect("consumer should connect");
    consumer
        .subscribe([topic.clone()])
        .expect("subscribe should succeed");
    assert_eq!(consumer.subscription(), vec![topic.clone()]);

    let expected = per_partition * 2;
    let mut total = 0;
    let deadline = std::time::Instant::now() + Duration::from_secs(45);
    while total < expected && std::time::Instant::now() < deadline {
        let records = consumer
            .poll(Duration::from_secs(2))
            .await
            .expect("poll should succeed");
        total += records.count();
    }

    println!("  assignment={:?} consumed={total}", consumer.assignment());
    assert_eq!(total, expected, "subscriber should consume every record");
    assert_eq!(
        consumer.assignment().len(),
        2,
        "single subscriber owns both partitions"
    );

    consumer.close().await;
    println!("real Kafka subscribe smoke: ALL OK");
}

/// Two consumers in one group subscribing to a two-partition topic should end up
/// with one partition each after the rebalance, and together consume everything.
#[tokio::test]
#[ignore = "requires local Kafka from docker-compose.kafka.yml"]
async fn real_kafka_two_consumers_split_partitions() {
    let bootstrap = bootstrap();
    let topic = topic();
    let per_partition = 5;
    println!("real Kafka rebalance smoke: bootstrap={bootstrap}, topic={topic}");

    create_topic(&bootstrap, &topic, 2).await;
    produce(&bootstrap, &topic, 0, per_partition).await;
    produce(&bootstrap, &topic, 1, per_partition).await;

    let group_id = format!("group-rebal-{topic}");
    let expected = per_partition * 2;
    // Real consumers poll on independent threads: while one blocks in JoinGroup
    // (the coordinator holds it until every member rejoins), the other must keep
    // polling to rejoin. Drive each in its own task to mirror that.
    let total = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let run = |client: &'static str| {
        let bootstrap = bootstrap.clone();
        let group_id = group_id.clone();
        let topic = topic.clone();
        let total = std::sync::Arc::clone(&total);
        tokio::spawn(async move {
            let mut consumer = Consumer::from_map([
                ("bootstrap.servers", bootstrap.as_str()),
                ("client.id", client),
                ("group.id", group_id.as_str()),
                ("auto.offset.reset", "earliest"),
                ("enable.auto.commit", "false"),
                ("heartbeat.interval.ms", "300"),
            ])
            .await
            .expect("consumer should connect");
            consumer.subscribe([topic]).expect("subscribe");
            let deadline = std::time::Instant::now() + Duration::from_secs(50);
            while total.load(std::sync::atomic::Ordering::SeqCst) < expected
                && std::time::Instant::now() < deadline
            {
                let count = consumer
                    .poll(Duration::from_secs(1))
                    .await
                    .expect("poll")
                    .count();
                let _prev = total.fetch_add(count, std::sync::atomic::Ordering::SeqCst);
            }
            let assignment = consumer.assignment();
            consumer.close().await;
            assignment
        })
    };

    let task_a = run("kacrab-rebal-a");
    let task_b = run("kacrab-rebal-b");
    let a_assign = task_a.await.expect("task a");
    let b_assign = task_b.await.expect("task b");

    let consumed = total.load(std::sync::atomic::Ordering::SeqCst);
    println!("  a={a_assign:?} b={b_assign:?} consumed={consumed}");
    assert_eq!(consumed, expected, "the pair should consume every record");
    assert_eq!(
        a_assign.len(),
        1,
        "consumer a owns one partition after rebalance"
    );
    assert_eq!(
        b_assign.len(),
        1,
        "consumer b owns one partition after rebalance"
    );
    assert_ne!(
        a_assign, b_assign,
        "the two consumers own different partitions"
    );
    println!("real Kafka rebalance smoke: ALL OK");
}

/// A pattern subscription joins only the topics whose names match the regex,
/// resolved from the cluster's live topic list, and consumes their records.
#[tokio::test]
#[ignore = "requires local Kafka from docker-compose.kafka.yml"]
async fn real_kafka_pattern_subscription_matches_topics() {
    let bootstrap = bootstrap();
    let nonce = topic();
    let matched_a = format!("{nonce}-pat-a");
    let matched_b = format!("{nonce}-pat-b");
    let unmatched = format!("{nonce}-other");
    println!("real Kafka pattern smoke: prefix={nonce}");

    create_topic(&bootstrap, &matched_a, 1).await;
    create_topic(&bootstrap, &matched_b, 1).await;
    create_topic(&bootstrap, &unmatched, 1).await;
    produce(&bootstrap, &matched_a, 0, 3).await;
    produce(&bootstrap, &matched_b, 0, 4).await;
    produce(&bootstrap, &unmatched, 0, 5).await;

    let mut consumer = Consumer::from_map([
        ("bootstrap.servers", bootstrap.as_str()),
        ("group.id", format!("group-pat-{nonce}").as_str()),
        ("auto.offset.reset", "earliest"),
        ("enable.auto.commit", "false"),
    ])
    .await
    .expect("consumer should connect");
    // Match only the `-pat-*` topics, not the `-other` one.
    consumer
        .subscribe_pattern(&format!("^{}-pat-.*$", regex_escape(&nonce)))
        .expect("valid pattern");

    let expected = 3 + 4;
    let mut total = 0;
    let deadline = std::time::Instant::now() + Duration::from_secs(45);
    while total < expected && std::time::Instant::now() < deadline {
        total += consumer
            .poll(Duration::from_secs(2))
            .await
            .expect("poll")
            .count();
    }

    let mut topics: Vec<String> = consumer
        .assignment()
        .into_iter()
        .map(|partition| partition.topic)
        .collect();
    topics.sort();
    topics.dedup();
    println!("  pattern matched topics={topics:?} consumed={total}");
    assert_eq!(topics, vec![matched_a.clone(), matched_b.clone()]);
    assert_eq!(
        total, expected,
        "only matching topics' records are consumed"
    );
    consumer.close().await;
    println!("real Kafka pattern smoke: ALL OK");
}

/// Escape regex metacharacters in a literal topic prefix (the nonce contains
/// none in practice, but keep the pattern anchored to exactly this run).
fn regex_escape(literal: &str) -> String {
    literal
        .chars()
        .flat_map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' {
                vec![c]
            } else {
                vec!['\\', c]
            }
        })
        .collect()
}

/// A subscriber using the KIP-848 protocol (`group.protocol=consumer`) joins via
/// `ConsumerGroupHeartbeat`, gets a server-computed assignment, and consumes
/// every record of a two-partition topic.
#[tokio::test]
#[ignore = "requires local Kafka from docker-compose.kafka.yml"]
async fn real_kafka_consumer_protocol_kip848() {
    let bootstrap = bootstrap();
    let topic = topic();
    let per_partition = 5;
    println!("real Kafka KIP-848 smoke: topic={topic}");

    create_topic(&bootstrap, &topic, 2).await;
    produce(&bootstrap, &topic, 0, per_partition).await;
    produce(&bootstrap, &topic, 1, per_partition).await;

    let mut consumer = Consumer::from_map([
        ("bootstrap.servers", bootstrap.as_str()),
        ("group.id", format!("group-kip848-{topic}").as_str()),
        ("group.protocol", "consumer"),
        ("auto.offset.reset", "earliest"),
        ("enable.auto.commit", "false"),
    ])
    .await
    .expect("consumer should connect");
    consumer.subscribe([topic.clone()]).expect("subscribe");

    let expected = per_partition * 2;
    let mut total = 0;
    let deadline = std::time::Instant::now() + Duration::from_secs(45);
    while total < expected && std::time::Instant::now() < deadline {
        total += consumer
            .poll(Duration::from_secs(2))
            .await
            .expect("poll")
            .count();
    }

    println!(
        "  KIP-848 assignment={:?} consumed={total}",
        consumer.assignment().len()
    );
    assert_eq!(total, expected, "the consumer should read every record");
    assert_eq!(consumer.assignment().len(), 2, "assigned both partitions");
    consumer.commit_sync().await.expect("commit");
    consumer.close().await;
    println!("real Kafka KIP-848 smoke: ALL OK");
}

/// `poll(timeout)` returns near the timeout even when `fetch.max.wait.ms` is much
/// larger — the long-poll is clamped to the remaining poll budget (B4).
#[tokio::test]
#[ignore = "requires local Kafka from docker-compose.kafka.yml"]
async fn real_kafka_poll_respects_short_timeout() {
    let bootstrap = bootstrap();
    let topic = topic();
    create_topic(&bootstrap, &topic, 1).await;
    produce(&bootstrap, &topic, 0, 3).await;

    let mut consumer = Consumer::from_map([
        ("bootstrap.servers", bootstrap.as_str()),
        ("group.id", format!("group-polltmo-{topic}").as_str()),
        ("auto.offset.reset", "earliest"),
        ("enable.auto.commit", "false"),
        // A deliberately long broker wait; the poll timeout must still win.
        ("fetch.max.wait.ms", "2000"),
    ])
    .await
    .expect("consumer should connect");
    consumer.assign([TopicPartition::new(topic.clone(), 0)]);

    // Drain the existing records so the next poll finds no new data.
    let mut drained = 0;
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    while drained < 3 && std::time::Instant::now() < deadline {
        drained += consumer
            .poll(Duration::from_secs(2))
            .await
            .expect("poll")
            .count();
    }

    // With nothing to fetch, a 200ms poll must return in well under the 2s
    // fetch.max.wait — proving the wait was clamped to the poll budget.
    let start = std::time::Instant::now();
    let empty = consumer
        .poll(Duration::from_millis(200))
        .await
        .expect("poll");
    let elapsed = start.elapsed();
    println!("  empty poll returned in {elapsed:?}");
    assert!(empty.is_empty());
    assert!(
        elapsed < Duration::from_secs(1),
        "poll(200ms) must not block for fetch.max.wait.ms=2000 (took {elapsed:?})"
    );
    consumer.close().await;
    println!("real Kafka poll-timeout smoke: ALL OK");
}

/// Asynchronous commits are applied in call order by a single worker, so a later
/// commit never loses to an earlier one — the final committed offset is the last
/// one issued and the callbacks fire in order. Verifies B3.
#[tokio::test]
#[ignore = "requires local Kafka from docker-compose.kafka.yml"]
async fn real_kafka_async_commits_apply_in_order() {
    let bootstrap = bootstrap();
    let topic = topic();
    create_topic(&bootstrap, &topic, 1).await;
    produce(&bootstrap, &topic, 0, 20).await;

    let mut consumer = Consumer::from_map([
        ("bootstrap.servers", bootstrap.as_str()),
        ("group.id", format!("group-asynccommit-{topic}").as_str()),
        ("auto.offset.reset", "earliest"),
        ("enable.auto.commit", "false"),
    ])
    .await
    .expect("consumer should connect");
    let partition = TopicPartition::new(topic.clone(), 0);
    consumer.assign([partition.clone()]);

    let order = std::sync::Arc::new(std::sync::Mutex::new(Vec::<i64>::new()));
    let done = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    // Issue ten commits with strictly increasing offsets, back to back.
    for offset in 1..=10_i64 {
        let mut offsets = std::collections::HashMap::new();
        let _prev = offsets.insert(partition.clone(), OffsetAndMetadata::new(offset));
        let order = std::sync::Arc::clone(&order);
        let done = std::sync::Arc::clone(&done);
        consumer
            .commit_async_offsets(
                offsets,
                Box::new(move |result| {
                    result.expect("async commit should succeed");
                    order.lock().unwrap().push(offset);
                    let _prev = done.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }),
            )
            .await
            .expect("enqueue");
    }

    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while done.load(std::sync::atomic::Ordering::SeqCst) < 10
        && std::time::Instant::now() < deadline
    {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let recorded = order.lock().unwrap().clone();
    println!("  async commit callback order = {recorded:?}");
    assert_eq!(
        recorded,
        (1..=10).collect::<Vec<i64>>(),
        "callbacks fire in call order (serialized worker)"
    );
    let committed = consumer
        .committed(std::slice::from_ref(&partition))
        .await
        .expect("committed");
    assert_eq!(
        committed.get(&partition).map(|meta| meta.offset),
        Some(10),
        "the last-issued commit wins; no regression"
    );
    consumer.close().await;
    println!("real Kafka async-commit-order smoke: ALL OK");
}

/// A synchronous commit issued after `commit_async` must not overtake the
/// queued async commit at the coordinator: `commit_sync_offsets` drains the
/// async queue first, so the final committed offset is the last one *called*
/// (the sync one), even when it is numerically smaller.
#[tokio::test]
#[ignore = "requires local Kafka from docker-compose.kafka.yml"]
async fn real_kafka_sync_commit_never_overtakes_queued_async_commits() {
    let bootstrap = bootstrap();
    let topic = topic();
    create_topic(&bootstrap, &topic, 1).await;

    let mut consumer = Consumer::from_map([
        ("bootstrap.servers", bootstrap.as_str()),
        ("group.id", format!("group-syncbarrier-{topic}").as_str()),
        ("auto.offset.reset", "earliest"),
        ("enable.auto.commit", "false"),
    ])
    .await
    .expect("consumer should connect");
    let partition = TopicPartition::new(topic.clone(), 0);
    consumer.assign([partition.clone()]);

    // Queue an async commit at offset 7...
    let async_applied = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let flag = std::sync::Arc::clone(&async_applied);
    let mut offsets = std::collections::HashMap::new();
    let _prev = offsets.insert(partition.clone(), OffsetAndMetadata::new(7));
    consumer
        .commit_async_offsets(
            offsets,
            Box::new(move |result| {
                result.expect("async commit should succeed");
                flag.store(true, std::sync::atomic::Ordering::SeqCst);
            }),
        )
        .await
        .expect("enqueue");

    // ...then immediately commit offset 3 synchronously. Call order makes the
    // sync commit newer, so 3 must win — without the flush barrier the sync
    // commit could land first and be overwritten by the queued 7.
    let mut offsets = std::collections::HashMap::new();
    let _prev = offsets.insert(partition.clone(), OffsetAndMetadata::new(3));
    consumer
        .commit_sync_offsets(offsets)
        .await
        .expect("sync commit");
    assert!(
        async_applied.load(std::sync::atomic::Ordering::SeqCst),
        "the queued async commit is applied (callback fired) before the sync commit is sent"
    );

    let committed = consumer
        .committed(std::slice::from_ref(&partition))
        .await
        .expect("committed");
    assert_eq!(
        committed.get(&partition).map(|meta| meta.offset),
        Some(3),
        "the sync commit (last call) wins over the earlier queued async commit"
    );
    consumer.close().await;
    println!("real Kafka sync-after-async barrier smoke: ALL OK");
}

/// Seeking past the log end makes the broker answer `OFFSET_OUT_OF_RANGE`; the
/// consumer must reset via `auto.offset.reset` and recover, not error forever.
/// Verifies the per-partition fetch error handling (B1) end to end.
#[tokio::test]
#[ignore = "requires local Kafka from docker-compose.kafka.yml"]
async fn real_kafka_out_of_range_resets_and_recovers() {
    let bootstrap = bootstrap();
    let topic = topic();
    let count = 6;
    create_topic(&bootstrap, &topic, 1).await;
    produce(&bootstrap, &topic, 0, count).await;

    let mut consumer = Consumer::from_map([
        ("bootstrap.servers", bootstrap.as_str()),
        ("group.id", format!("group-oor-{topic}").as_str()),
        ("auto.offset.reset", "earliest"),
        ("enable.auto.commit", "false"),
    ])
    .await
    .expect("consumer should connect");
    let partition = TopicPartition::new(topic.clone(), 0);
    consumer.assign([partition.clone()]);
    // Position the fetch well past the log end → OFFSET_OUT_OF_RANGE on fetch.
    consumer.seek(&partition, 9_999).expect("seek");

    let mut total = 0;
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while total < count && std::time::Instant::now() < deadline {
        total += consumer
            .poll(Duration::from_secs(2))
            .await
            .expect("poll must recover, not error")
            .count();
    }
    println!("  out-of-range recovered, consumed={total}");
    assert_eq!(total, count, "reset to earliest and consumed every record");
    consumer.close().await;
    println!("real Kafka out-of-range smoke: ALL OK");
}

async fn create_topic(bootstrap: &str, topic: &str, partitions: i32) {
    let admin = AdminClient::from_map([("bootstrap.servers", bootstrap)])
        .await
        .expect("admin should connect");
    admin
        .create_topics(
            vec![NewTopic::new(topic.to_owned(), partitions, 1)],
            CreateTopicsOptions::default(),
        )
        .await
        .expect("create_topics should succeed");
}

async fn produce(bootstrap: &str, topic: &str, partition: i32, count: usize) {
    let producer = Producer::builder()
        .set("bootstrap.servers", bootstrap.to_owned())
        .set("enable.idempotence", "true")
        .set("acks", "all")
        .set("batch.size", "1")
        .build()
        .await
        .expect("producer should connect");
    for i in 0..count {
        let record = ProducerRecord::new(topic.to_owned(), partition)
            .value(Bytes::from(format!("p{partition}-v{i}")));
        let _receipt = producer
            .send(record)
            .expect("send")
            .await
            .expect("delivery");
    }
}

/// Two consumers negotiating `cooperative-sticky` split a two-partition topic
/// one apiece and together consume every record. Exercises the incremental
/// rebalance path: when the second member joins, its target partition is
/// withheld until the first member sees it drop from its own assignment, revokes
/// it, and rejoins so the coordinator can hand it over (KIP-429) — no partition
/// is ever owned by both at once.
#[tokio::test]
#[ignore = "requires local Kafka from docker-compose.kafka.yml"]
async fn real_kafka_cooperative_sticky_incremental() {
    let bootstrap = bootstrap();
    let topic = topic();
    let per_partition = 5;
    println!("real Kafka cooperative-sticky smoke: topic={topic}");

    create_topic(&bootstrap, &topic, 2).await;
    produce(&bootstrap, &topic, 0, per_partition).await;
    produce(&bootstrap, &topic, 1, per_partition).await;

    let group_id = format!("group-coop-{topic}");
    let expected = per_partition * 2;
    // Deduplicate by (partition, offset): a partition handed over before its
    // records were committed is legitimately re-read by the new owner
    // (at-least-once), so a raw count over-counts on slow hosts where the
    // second member joins after the first has already consumed everything.
    let seen = std::sync::Arc::new(std::sync::Mutex::new(std::collections::HashSet::new()));
    let run = |client: &'static str| {
        let bootstrap = bootstrap.clone();
        let group_id = group_id.clone();
        let topic = topic.clone();
        let seen = std::sync::Arc::clone(&seen);
        tokio::spawn(async move {
            let mut consumer = Consumer::from_map([
                ("bootstrap.servers", bootstrap.as_str()),
                ("client.id", client),
                ("group.id", group_id.as_str()),
                ("auto.offset.reset", "earliest"),
                ("enable.auto.commit", "false"),
                ("heartbeat.interval.ms", "300"),
                ("partition.assignment.strategy", "cooperative-sticky"),
            ])
            .await
            .expect("consumer should connect");
            consumer.subscribe([topic]).expect("subscribe");
            // Keep polling until every record is accounted for AND this member
            // has settled at exactly one partition — i.e. it has observed the
            // incremental handover complete. Snapshotting only then keeps the
            // final asserts race-free: exiting as soon as the count is reached
            // lets one member leave before the other ever saw the 1/1 split
            // (the survivor then rebalances to own both partitions).
            let deadline = std::time::Instant::now() + Duration::from_secs(50);
            let mut assignment = consumer.assignment();
            while std::time::Instant::now() < deadline {
                let records = consumer.poll(Duration::from_secs(1)).await.expect("poll");
                let all_seen = {
                    let mut seen = seen.lock().expect("seen lock");
                    for record in &records {
                        let _new = seen.insert((record.partition, record.offset));
                    }
                    seen.len() >= expected
                };
                assignment = consumer.assignment();
                if all_seen && assignment.len() == 1 {
                    break;
                }
            }
            consumer.close().await;
            assignment
        })
    };

    let task_a = run("kacrab-coop-a");
    let task_b = run("kacrab-coop-b");
    let a_assign = task_a.await.expect("task a");
    let b_assign = task_b.await.expect("task b");

    let consumed = seen.lock().expect("seen lock").len();
    println!("  a={a_assign:?} b={b_assign:?} consumed={consumed}");
    assert_eq!(consumed, expected, "the pair should consume every record");
    assert_eq!(a_assign.len(), 1, "consumer a owns one partition");
    assert_eq!(b_assign.len(), 1, "consumer b owns one partition");
    assert_ne!(
        a_assign, b_assign,
        "the two consumers own different partitions"
    );
    println!("real Kafka cooperative-sticky smoke: ALL OK");
}

/// `beginning_offsets`/`end_offsets`, `offsets_for_times`, and `current_lag`
/// against a topic with a known number of records.
#[tokio::test]
#[ignore = "requires local Kafka from docker-compose.kafka.yml"]
async fn real_kafka_offset_queries() {
    let bootstrap = bootstrap();
    let topic = topic();
    let count = 8;
    println!("real Kafka offset-queries smoke: topic={topic}");

    create_topic(&bootstrap, &topic, 1).await;
    produce(&bootstrap, &topic, 0, count).await;

    let mut consumer = Consumer::from_map([
        ("bootstrap.servers", bootstrap.as_str()),
        ("group.id", format!("group-off-{topic}").as_str()),
        ("auto.offset.reset", "earliest"),
        ("enable.auto.commit", "false"),
    ])
    .await
    .expect("consumer should connect");

    let partition = TopicPartition::new(topic.clone(), 0);
    let parts = std::slice::from_ref(&partition);

    let begin = consumer
        .beginning_offsets(parts)
        .await
        .expect("beginning_offsets");
    let end = consumer.end_offsets(parts).await.expect("end_offsets");
    println!(
        "  begin={:?} end={:?}",
        begin.get(&partition),
        end.get(&partition)
    );
    assert_eq!(begin.get(&partition), Some(&0));
    assert_eq!(
        end.get(&partition).copied(),
        Some(i64::try_from(count).unwrap())
    );

    // offsets_for_times at time 0 should return the first record (offset 0).
    let mut want = std::collections::HashMap::new();
    let _ = want.insert(partition.clone(), 0_i64);
    let times = consumer
        .offsets_for_times(want)
        .await
        .expect("offsets_for_times");
    assert_eq!(times.get(&partition).map(|o| o.offset), Some(0));

    // current_lag: assign + seek to 0 → lag == count.
    consumer.assign([partition.clone()]);
    consumer.seek(&partition, 0).expect("seek");
    let lag = consumer.current_lag(&partition).await.expect("current_lag");
    println!("  current_lag={lag:?}");
    assert_eq!(lag, Some(i64::try_from(count).unwrap()));

    consumer.close().await;
    println!("real Kafka offset-queries smoke: ALL OK");
}

/// Auto-commit (background, on interval) and `commit_async` both persist offsets
/// that a fresh consumer in the same group reads back.
#[tokio::test]
#[ignore = "requires local Kafka from docker-compose.kafka.yml"]
async fn real_kafka_auto_and_async_commit() {
    let bootstrap = bootstrap();
    let topic = topic();
    let count = 6;
    create_topic(&bootstrap, &topic, 1).await;
    produce(&bootstrap, &topic, 0, count).await;
    let partition = TopicPartition::new(topic.clone(), 0);
    let group = format!("group-ac-{topic}");

    // --- auto-commit ---
    let mut consumer = Consumer::from_map([
        ("bootstrap.servers", bootstrap.as_str()),
        ("group.id", group.as_str()),
        ("auto.offset.reset", "earliest"),
        ("enable.auto.commit", "true"),
        ("auto.commit.interval.ms", "200"),
    ])
    .await
    .expect("consumer");
    consumer.assign([partition.clone()]);
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    let mut total = 0;
    while total < count && std::time::Instant::now() < deadline {
        total += consumer
            .poll(Duration::from_millis(500))
            .await
            .expect("poll")
            .count();
    }
    // Poll again after the interval so the background auto-commit fires.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let _records = consumer
        .poll(Duration::from_millis(200))
        .await
        .expect("poll");
    consumer.close().await; // also auto-commits on close

    let mut checker = Consumer::from_map([
        ("bootstrap.servers", bootstrap.as_str()),
        ("group.id", group.as_str()),
        ("enable.auto.commit", "false"),
    ])
    .await
    .expect("checker");
    let committed = checker
        .committed(std::slice::from_ref(&partition))
        .await
        .expect("committed");
    println!(
        "  auto-commit committed={:?}",
        committed.get(&partition).map(|o| o.offset)
    );
    assert_eq!(
        committed.get(&partition).map(|o| o.offset),
        Some(i64::try_from(count).unwrap())
    );

    // --- commit_async ---
    checker.assign([partition.clone()]);
    checker.seek(&partition, 3).expect("seek");
    let done = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let flag = std::sync::Arc::clone(&done);
    checker
        .commit_async(Box::new(move |result| {
            result.expect("async commit ok");
            flag.store(true, std::sync::atomic::Ordering::SeqCst);
        }))
        .await
        .expect("commit_async dispatch");
    for _ in 0..50 {
        if done.load(std::sync::atomic::Ordering::SeqCst) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        done.load(std::sync::atomic::Ordering::SeqCst),
        "async commit callback ran"
    );
    let committed = checker
        .committed(std::slice::from_ref(&partition))
        .await
        .expect("committed");
    println!(
        "  async committed={:?}",
        committed.get(&partition).map(|o| o.offset)
    );
    assert_eq!(committed.get(&partition).map(|o| o.offset), Some(3));

    checker.close().await;
    println!("real Kafka auto/async commit smoke: ALL OK");
}

/// A subscriber with the `roundrobin` assignor negotiates that protocol and is
/// assigned every partition of a four-partition topic.
#[tokio::test]
#[ignore = "requires local Kafka from docker-compose.kafka.yml"]
async fn real_kafka_roundrobin_assignor() {
    let bootstrap = bootstrap();
    let topic = topic();
    create_topic(&bootstrap, &topic, 4).await;
    for p in 0..4 {
        produce(&bootstrap, &topic, p, 2).await;
    }
    let mut consumer = Consumer::from_map([
        ("bootstrap.servers", bootstrap.as_str()),
        ("group.id", format!("group-rr-{topic}").as_str()),
        ("auto.offset.reset", "earliest"),
        ("enable.auto.commit", "false"),
        ("partition.assignment.strategy", "roundrobin"),
    ])
    .await
    .expect("consumer");
    consumer.subscribe([topic.clone()]).expect("subscribe");
    let mut total = 0;
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while total < 8 && std::time::Instant::now() < deadline {
        total += consumer
            .poll(Duration::from_secs(2))
            .await
            .expect("poll")
            .count();
    }
    println!(
        "  roundrobin assignment={:?} consumed={total}",
        consumer.assignment().len()
    );
    assert_eq!(consumer.assignment().len(), 4);
    assert_eq!(total, 8);
    consumer.close().await;
    println!("real Kafka roundrobin smoke: ALL OK");
}

/// A registered [`ConsumerInterceptor`] sees every polled record (`on_consume`)
/// and every committed offset (`on_commit`) end to end against a real broker.
/// (The trait is implemented for the local struct directly — the orphan rule
/// forbids implementing a foreign trait for `Arc<T>` in this test crate — so
/// shared `Arc` counters expose what the interceptor observed after it is moved
/// into the consumer.)
struct CountingInterceptor {
    consumed: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    committed: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    configured_group: std::sync::Arc<std::sync::Mutex<Option<String>>>,
}

impl ConsumerInterceptor for CountingInterceptor {
    fn configure(&self, configs: &InterceptorConfigs) {
        self.configured_group
            .lock()
            .unwrap()
            .clone_from(&configs.group_id);
    }

    fn on_consume(&self, records: ConsumerRecords) -> ConsumerRecords {
        let _prev = self
            .consumed
            .fetch_add(records.count(), std::sync::atomic::Ordering::SeqCst);
        records
    }

    fn on_commit(&self, offsets: &std::collections::HashMap<TopicPartition, OffsetAndMetadata>) {
        let _prev = self
            .committed
            .fetch_add(offsets.len(), std::sync::atomic::Ordering::SeqCst);
    }
}

#[tokio::test]
#[ignore = "requires local Kafka from docker-compose.kafka.yml"]
async fn real_kafka_interceptor_observes_records_and_commits() {
    let bootstrap = bootstrap();
    let topic = topic();
    let count = 6;
    create_topic(&bootstrap, &topic, 1).await;
    produce(&bootstrap, &topic, 0, count).await;

    let group_id = format!("group-intercept-{topic}");
    let mut consumer = Consumer::from_map([
        ("bootstrap.servers", bootstrap.as_str()),
        ("group.id", group_id.as_str()),
        ("auto.offset.reset", "earliest"),
        ("enable.auto.commit", "false"),
    ])
    .await
    .expect("consumer should connect");

    let record_hits = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let commit_hits = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let configured_group = std::sync::Arc::new(std::sync::Mutex::new(None));
    consumer.add_interceptor(CountingInterceptor {
        consumed: std::sync::Arc::clone(&record_hits),
        committed: std::sync::Arc::clone(&commit_hits),
        configured_group: std::sync::Arc::clone(&configured_group),
    });
    assert_eq!(
        *configured_group.lock().unwrap(),
        Some(group_id.clone()),
        "configure delivers the group.id"
    );

    consumer.assign([TopicPartition::new(topic.clone(), 0)]);
    let mut total = 0;
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while total < count && std::time::Instant::now() < deadline {
        total += consumer
            .poll(Duration::from_secs(2))
            .await
            .expect("poll")
            .count();
    }
    consumer.commit_sync().await.expect("commit");

    let seen = record_hits.load(std::sync::atomic::Ordering::SeqCst);
    let commits = commit_hits.load(std::sync::atomic::Ordering::SeqCst);
    println!("  interceptor consumed={seen} committed_partitions={commits}");
    assert_eq!(seen, count, "on_consume saw every record");
    assert_eq!(commits, 1, "on_commit saw the one committed partition");
    consumer.close().await;
    println!("real Kafka interceptor smoke: ALL OK");
}
