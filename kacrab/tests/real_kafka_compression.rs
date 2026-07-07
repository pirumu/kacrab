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

/// The kacrab CONSUMER must decode broker-stored compressed batches it did not
/// produce: records go in through the broker's own console producer (inside
/// the container) per codec, and come back out through kacrab's fetch → batch
/// decode → bounded decompression path.
#[cfg(feature = "consumer")]
#[tokio::test]
#[ignore = "requires the broker from docker-compose.kafka.yml and the docker CLI"]
async fn real_kafka_consumer_decompresses_cli_produced_batches() {
    let bootstrap =
        env::var("KACRAB_BOOTSTRAP").unwrap_or_else(|_error| "127.0.0.1:9092".to_owned());
    for codec in CODECS {
        let topic = unique_topic();
        create_topic(&topic);
        // A compressible, codec-tagged payload, one record per line.
        let expected: Vec<String> = (0..RECORDS_PER_CODEC)
            .map(|index| format!("cli-{codec}-{index}-{}", "payload".repeat(64)))
            .collect();
        let lines = format!("{}\n", expected.join("\n"));
        kafka_cli_stdin(
            "kafka-console-producer.sh",
            &["--topic", &topic, "--compression-codec", codec],
            &lines,
        );

        let mut consumer = kacrab::consumer::Consumer::from_map([
            ("bootstrap.servers", bootstrap.as_str()),
            ("group.id", format!("cli-read-{topic}").as_str()),
            ("auto.offset.reset", "earliest"),
            ("enable.auto.commit", "false"),
        ])
        .await
        .expect("consumer should connect");
        consumer
            .assign([kacrab::common::TopicPartition::new(topic.clone(), 0)])
            .expect("assign");

        let mut received: Vec<String> = Vec::new();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
        while received.len() < expected.len() && std::time::Instant::now() < deadline {
            let records = match consumer.poll(std::time::Duration::from_millis(500)).await {
                Ok(records) => records,
                // Topic metadata can lag briefly right after the CLI creates
                // the topic; retry within the deadline.
                Err(kacrab::consumer::ConsumerError::Wire(error)) => {
                    println!("  transient wire error while polling: {error}");
                    continue;
                },
                Err(error) => panic!("poll should succeed: {error}"),
            };
            for record in &records {
                let value = record.value.as_ref().expect("record should carry a value");
                received.push(String::from_utf8(value.to_vec()).expect("utf-8 payload"));
            }
        }
        assert_eq!(
            received, expected,
            "kacrab consumer should decode every CLI-produced {codec} record"
        );
        consumer.close().await;
        delete_topic(&topic);
        println!(
            "{codec}: kacrab consumer decoded {} CLI-produced records",
            expected.len()
        );
    }
    println!("kacrab consumer decompressed every codec's CLI-produced batches: ALL OK");
}

/// Run a Kafka CLI tool with `payload` on stdin and `--bootstrap-server`
/// appended; see [`cli_command`] for where the tool runs.
#[cfg(feature = "consumer")]
fn kafka_cli_stdin(script: &str, args: &[&str], payload: &str) {
    use std::io::Write as _;

    let mut child = cli_command(script, true)
        .args(args)
        .stdin(std::process::Stdio::piped())
        .spawn()
        .expect("kafka CLI should spawn");
    child
        .stdin
        .as_mut()
        .expect("stdin should be piped")
        .write_all(payload.as_bytes())
        .expect("payload should be written");
    let status = child.wait().expect("kafka CLI should run");
    assert!(
        status.success(),
        "kafka CLI command failed: {script} {args:?}"
    );
}

/// Build a command for a Kafka CLI script, pointed at the same broker the
/// kacrab clients use: `docker exec` into the compose container by default, or
/// a native install's `bin/` when `KACRAB_KAFKA_BIN` is set (for environments
/// where `127.0.0.1:9092` is served by a local broker rather than the compose
/// container). `--bootstrap-server` is pre-set accordingly.
fn cli_command(script: &str, interactive: bool) -> Command {
    if let Ok(bin) = env::var("KACRAB_KAFKA_BIN") {
        let bootstrap =
            env::var("KACRAB_BOOTSTRAP").unwrap_or_else(|_error| "127.0.0.1:9092".to_owned());
        let mut command = Command::new(format!("{bin}/{script}"));
        let _args = command.args(["--bootstrap-server", &bootstrap]);
        return command;
    }
    let mut command = Command::new("docker");
    let _args = command.arg("exec");
    if interactive {
        let _arg = command.arg("-i");
    }
    let _args = command
        .arg(CONTAINER)
        .arg(format!("/opt/kafka/bin/{script}"))
        .args(["--bootstrap-server", "localhost:9092"]);
    command
}

fn unique_topic() -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after unix epoch")
        .as_millis();
    format!("kacrab-compression-{}-{millis}", std::process::id())
}

fn create_topic(topic: &str) {
    kafka_cli(
        "kafka-topics.sh",
        &[
            "--create",
            "--topic",
            topic,
            "--partitions",
            "1",
            "--replication-factor",
            "1",
        ],
    );
}

fn delete_topic(topic: &str) {
    // Best-effort cleanup; ignore failures so a flaky delete does not fail the test.
    let _output = cli_command("kafka-topics.sh", false)
        .args(["--delete", "--topic", topic])
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

/// Run a Kafka CLI tool with `--bootstrap-server` appended; see
/// [`cli_command`] for where the tool runs.
fn kafka_cli(script: &str, args: &[&str]) {
    let status = cli_command(script, false)
        .args(args)
        .status()
        .expect("kafka CLI should run");
    assert!(
        status.success(),
        "kafka CLI command failed: {script} {args:?}"
    );
}
