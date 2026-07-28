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
use kacrab::producer::{Producer, ProducerRecord};

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

fn unique_suffix() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after unix epoch")
        .as_millis()
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
#[cfg(all(feature = "consumer", feature = "admin"))]
#[tokio::test]
#[ignore = "requires the broker from docker-compose.kafka.yml"]
async fn real_kafka_aborted_transaction_is_invisible_to_read_committed() {
    use std::time::Duration;

    use kacrab::{
        admin::{AdminClient, CreateTopicsOptions, NewTopic},
        common::TopicPartition,
        consumer::Consumer,
    };

    let bootstrap = bootstrap_addr();
    let topic = format!("kacrab-eos-{}", unique_suffix());
    let transactional_id = format!("kacrab-eos-txn-{}", unique_suffix());

    // The compose fixtures run with auto.create.topics.enable=false, so a
    // fresh topic must be created explicitly — producing to a missing topic
    // never resolves metadata and the delivery future waits forever.
    let admin = AdminClient::from_map([("bootstrap.servers", bootstrap.to_string().as_str())])
        .await
        .expect("admin should connect");
    admin
        .create_topics(
            vec![NewTopic::new(topic.clone(), 1, 1)],
            CreateTopicsOptions::default(),
        )
        .await
        .expect("create_topics should succeed");

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
