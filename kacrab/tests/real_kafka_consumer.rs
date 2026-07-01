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
    common::TopicPartition,
    consumer::Consumer,
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
