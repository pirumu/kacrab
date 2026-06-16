#![cfg(feature = "producer")]
//! Real Kafka producer integration tests.

#![allow(
    clippy::expect_used,
    clippy::print_stdout,
    clippy::unwrap_used,
    reason = "Ignored real-broker tests are explicit smoke checks with direct failure output."
)]

use std::{
    env,
    net::SocketAddr,
    time::{SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use kacrab::producer::{KafkaProducer, ProducerRecord};

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

    let mut producer = KafkaProducer::builder()
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
        .await
        .expect("transactional send should enqueue and dispatch");

    producer
        .commit_transaction()
        .await
        .expect("EndTxn commit should succeed");

    let receipt = delivery.await.expect("delivery receipt should complete");
    assert_eq!(receipt.partition, 0);
    assert!(receipt.base_offset >= 0);
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

fn transactional_id() -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after unix epoch")
        .as_millis();
    format!("kacrab-real-kafka-txn-{}-{millis}", std::process::id())
}
