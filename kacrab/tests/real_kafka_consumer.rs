#![cfg(all(feature = "producer", feature = "consumer", feature = "admin"))]
//! Real Kafka consumer integration test (Phase 1: manual assignment + fetch).
//!
//! Creates a topic (admin), produces a known set of records with the kacrab
//! producer, then consumes them back with the kacrab consumer via manual
//! partition assignment against a real Apache Kafka 4.3.0 broker from
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
    let mut consumer = Consumer::from_map([
        ("bootstrap.servers", bootstrap.as_str()),
        ("client.id", "kacrab-real-kafka-consumer"),
        ("auto.offset.reset", "earliest"),
        ("enable.auto.commit", "false"),
    ])
    .await
    .expect("consumer should connect to local Kafka");

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

    consumer.close();
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
