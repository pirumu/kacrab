#![cfg(all(
    feature = "producer",
    feature = "gzip",
    feature = "snappy",
    feature = "lz4",
    feature = "zstd"
))]
//! Real Kafka compression round-trip.
//!
//! Produces records with each codec (gzip, snappy, lz4, zstd) to a real broker
//! and consumes them back with `kafka-console-consumer` (run inside the broker
//! container). A unit-only `compress`→`decompress` round-trip would pass even if
//! kacrab framed the codec in a non-Kafka way, since kacrab is on both ends; a
//! real broker accepting the produce AND the consumer decompressing it on read
//! is what proves the compressed record batches are Kafka-compatible.
//!
//! Run after `docker compose -f docker-compose.kafka.yml up -d`, built with the
//! compression features:
//!   `cargo test -p kacrab --features producer,gzip,snappy,lz4,zstd \
//!      --test real_kafka_compression -- --ignored --nocapture`

#![allow(
    clippy::arithmetic_side_effects,
    clippy::expect_used,
    clippy::panic,
    clippy::print_stdout,
    reason = "Ignored real-broker tests are explicit smoke checks; the dump-log parser indexes \
              over a small bounded token list."
)]

use std::{
    env,
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use kacrab::producer::{Producer, ProducerRecord};

const CODECS: [&str; 4] = ["gzip", "snappy", "lz4", "zstd"];
const RECORDS_PER_CODEC: usize = 3;
const CONTAINER: &str = "kacrab-kafka";

#[tokio::test]
#[ignore = "requires the broker from docker-compose.kafka.yml and the docker CLI"]
async fn real_kafka_compression_roundtrips_every_codec() {
    let bootstrap =
        env::var("KACRAB_BOOTSTRAP").unwrap_or_else(|_error| "127.0.0.1:9092".to_owned());
    let topic = unique_topic();
    create_topic(&topic);
    println!("compression round-trip: bootstrap={bootstrap}, topic={topic}");

    let mut expected = Vec::new();
    for codec in CODECS {
        let producer = build_producer(&bootstrap, codec).await;
        for index in 0..RECORDS_PER_CODEC {
            let value = marker(codec, index);
            let receipt = producer
                .send(ProducerRecord::new(topic.clone(), 0).value(Bytes::from(value.clone())))
                .expect("send should enqueue")
                .await
                .unwrap_or_else(|error| panic!("broker should accept the {codec} batch: {error}"));
            assert!(
                receipt.offset >= 0,
                "{codec} record should get a real offset"
            );
            expected.push(value);
        }
        println!("{codec}: produced {RECORDS_PER_CODEC} records (broker accepted the batch)");
        producer
            .close()
            .await
            .expect("producer should close cleanly");
    }

    // Prove the batches were actually stored compressed with the right codec
    // (a round-trip alone would also pass if kacrab silently sent them
    // uncompressed). kafka-dump-log reports the on-disk codec per batch.
    let stored = stored_codecs(&topic);
    println!("on-disk batch codecs: {stored:?}");
    for codec in CODECS {
        assert!(
            stored.iter().any(|line| line.eq_ignore_ascii_case(codec)),
            "broker did not store a {codec} batch (stored codecs: {stored:?})"
        );
    }

    let consumed = consume_all(&topic, expected.len());
    for value in &expected {
        assert!(
            consumed.contains(value.as_str()),
            "consumed output is missing {value}: the broker/consumer could not decompress \
             kacrab's batch"
        );
    }
    delete_topic(&topic);
    println!(
        "all {} compressed records round-tripped through the broker and CLI consumer",
        expected.len()
    );
}

async fn build_producer(bootstrap: &str, codec: &str) -> Producer {
    Producer::builder()
        .set("bootstrap.servers", bootstrap.to_owned())
        .set("client.id", format!("kacrab-compression-{codec}"))
        .set("acks", "all")
        .set("enable.idempotence", "true")
        .set("compression.type", codec)
        .build()
        .await
        .unwrap_or_else(|error| {
            panic!("producer with compression.type={codec} should build: {error}")
        })
}

/// A compressible, codec-tagged value so it is identifiable in the consumer
/// output and actually exercises the codec (the repeated tail compresses well).
fn marker(codec: &str, index: usize) -> String {
    format!("kacrab-compress-{codec}-{index}-{}", "payload".repeat(64))
}

fn unique_topic() -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after unix epoch")
        .as_millis();
    format!("kacrab-compression-{}-{millis}", std::process::id())
}

fn create_topic(topic: &str) {
    kafka_cli(&[
        "/opt/kafka/bin/kafka-topics.sh",
        "--bootstrap-server",
        "localhost:9092",
        "--create",
        "--topic",
        topic,
        "--partitions",
        "1",
        "--replication-factor",
        "1",
    ]);
}

fn delete_topic(topic: &str) {
    // Best-effort cleanup; ignore failures so a flaky delete does not fail the test.
    let _output = Command::new("docker")
        .args([
            "exec",
            CONTAINER,
            "/opt/kafka/bin/kafka-topics.sh",
            "--bootstrap-server",
            "localhost:9092",
            "--delete",
            "--topic",
            topic,
        ])
        .output();
}

/// Dumps partition 0's log segment and returns the `compresscodec` of each
/// stored record batch (e.g. `GZIP`, `SNAPPY`, `LZ4`, `ZSTD`, `NONE`).
fn stored_codecs(topic: &str) -> Vec<String> {
    let dump = format!(
        "/opt/kafka/bin/kafka-dump-log.sh --print-data-log --files \
         /var/lib/kafka/data/{topic}-0/*.log"
    );
    let output = Command::new("docker")
        .args(["exec", CONTAINER, "sh", "-c", &dump])
        .output()
        .expect("kafka-dump-log should run");
    let text = String::from_utf8_lossy(&output.stdout);
    // dump-log prints `... compresscodec: GZIP ...` per batch; capture the value
    // whether or not there is a space after the colon.
    let tokens: Vec<&str> = text.split_whitespace().collect();
    let mut codecs = Vec::new();
    for (index, token) in tokens.iter().enumerate() {
        if let Some(rest) = token.strip_prefix("compresscodec:") {
            if rest.is_empty() {
                if let Some(next) = tokens.get(index + 1) {
                    codecs.push((*next).to_owned());
                }
            } else {
                codecs.push(rest.to_owned());
            }
        }
    }
    codecs
}

fn consume_all(topic: &str, count: usize) -> String {
    let output = Command::new("docker")
        .args([
            "exec",
            CONTAINER,
            "/opt/kafka/bin/kafka-console-consumer.sh",
            "--bootstrap-server",
            "localhost:9092",
            "--topic",
            topic,
            "--from-beginning",
            "--max-messages",
            &count.to_string(),
            "--timeout-ms",
            "20000",
        ])
        .output()
        .expect("kafka-console-consumer should run");
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn kafka_cli(args: &[&str]) {
    let mut full = vec!["exec", CONTAINER];
    full.extend_from_slice(args);
    let status = Command::new("docker")
        .args(&full)
        .status()
        .expect("docker exec should run");
    assert!(status.success(), "kafka CLI command failed: {args:?}");
}
