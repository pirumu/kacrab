//! Cross-client interop suite: the JVM Kafka client on one side, kacrab on the
//! other, against a real broker. The JVM peer is the tooling that ships inside
//! the compose broker container (`kafka-verifiable-producer.sh`,
//! `kafka-verifiable-consumer.sh`, `kafka-console-producer.sh`,
//! `kafka-console-consumer.sh`, `kafka-consumer-groups.sh`), run via `docker
//! exec`. Every direction asserts payload equality — keys, values, headers, and
//! timestamps where the JVM tool can express or print them — never just record
//! counts, so an attribute-encoding divergence between the clients cannot hide
//! behind matching totals.
//!
//! Tool-capability limits (Kafka 4.3.0 tooling), and how each is handled:
//!
//! - `kafka-console-producer.sh` cannot set an explicit record timestamp (`LineMessageReader`
//!   parses headers/key/value only), so the JVM→kacrab explicit-timestamp leg rides
//!   `kafka-verifiable-producer.sh --message-create-time`. That tool sets `CreateTime` to the given
//!   base plus elapsed wall time — deterministic bounds, not an exact value — so the
//!   console-producer test additionally asserts exact timestamp agreement between the two CLIENTS
//!   (kacrab's consumed timestamp vs the JVM console consumer's printed timestamp) instead of
//!   against a chosen constant.
//! - The verifiable producer/consumer cannot express headers; headers ride the console tools in
//!   both directions.
//! - The container ships no transactional CLI *producer* (`kafka-transactions.sh` is an admin
//!   tool), so transactional interop is kacrab-producing and the JVM console consumer reading under
//!   BOTH isolation levels — `read_uncommitted` seeing the aborted record is the negative control
//!   proving `read_committed` actively filtered it.
//!
//! Run after `docker compose -f docker-compose.kafka.yml up -d`. Suites share
//! one broker, so single-threaded:
//!   `cargo test -p kacrab --features producer,consumer,gzip \
//!      --test real_kafka_interop -- --ignored --test-threads=1`

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
    collections::HashMap,
    env,
    io::Write as _,
    process::{Command, Stdio},
    sync::atomic::{AtomicU32, Ordering},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use kacrab::{
    common::{OffsetAndMetadata, TopicPartition},
    consumer::{Consumer, ConsumerRecord, RecordHeader, TimestampType},
    producer::{Producer, ProducerRecord},
};

const CONTAINER: &str = "kacrab-kafka";

fn bootstrap() -> String {
    env::var("KACRAB_BOOTSTRAP").unwrap_or_else(|_error| "127.0.0.1:9092".to_owned())
}

/// Process-unique suffix for topic/group names so concurrent and repeated runs
/// against the shared broker never collide.
fn unique(prefix: &str) -> String {
    static COUNTER: AtomicU32 = AtomicU32::new(0);
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after unix epoch")
        .as_millis();
    let count = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}-{}-{millis}-{count}", std::process::id())
}

/// Build a command for a Kafka CLI script, pointed at the same broker the
/// kacrab clients use: `docker exec` into the compose container by default, or
/// a native install's `bin/` when `KACRAB_KAFKA_BIN` is set. The JVM tool run
/// this way IS the interop peer — the Java client library doing real
/// produce/consume against the broker.
fn cli_command(script: &str, interactive: bool) -> Command {
    if let Ok(bin) = env::var("KACRAB_KAFKA_BIN") {
        let bootstrap = bootstrap();
        let mut command = Command::new(format!("{bin}/{script}"));
        let _args = command.args(["--bootstrap-server", &bootstrap]);
        return command;
    }
    let mut command = Command::new("docker");
    let _arg = command.arg("exec");
    if interactive {
        let _arg = command.arg("-i");
    }
    let _args = command
        .arg(CONTAINER)
        .arg(format!("/opt/kafka/bin/{script}"))
        .args(["--bootstrap-server", "localhost:9092"]);
    command
}

/// Run a Kafka CLI tool to completion, asserting success.
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

/// Run a Kafka CLI tool with `payload` on stdin (the console-producer path).
fn kafka_cli_stdin(script: &str, args: &[&str], payload: &str) {
    let mut child = cli_command(script, true)
        .args(args)
        .stdin(Stdio::piped())
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
    // Best-effort cleanup; a flaky delete must not fail the test.
    let _output = cli_command("kafka-topics.sh", false)
        .args(["--delete", "--topic", topic])
        .output();
}

/// Read a topic from the beginning with the JVM console consumer and return
/// its stdout lines (the KIP-848 banner and the "Processed a total of" trailer
/// go to stderr).
///
/// No exit-status assert on purpose: the `read_committed` negative control
/// deliberately over-asks (`--max-messages` above what is visible) and exits
/// non-zero via `--timeout-ms`; the content assertions are the real check.
fn console_consume(
    extra_args: &[&str],
    formatter_props: &[&str],
    max_messages: usize,
    timeout_ms: u32,
) -> Vec<String> {
    let mut command = cli_command("kafka-console-consumer.sh", false);
    let _args = command.args(extra_args).args([
        "--from-beginning",
        "--max-messages",
        &max_messages.to_string(),
        "--timeout-ms",
        &timeout_ms.to_string(),
    ]);
    for prop in formatter_props {
        let _args = command.args(["--formatter-property", prop]);
    }
    let output = command.output().expect("kafka-console-consumer should run");
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter(|line| !line.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

/// Run a verifiable tool to completion and parse its JSON-line events.
fn run_verifiable(script: &str, args: &[&str]) -> Vec<serde_json::Value> {
    let output = cli_command(script, false)
        .args(args)
        .output()
        .expect("verifiable tool should run");
    assert!(
        output.status.success(),
        "verifiable tool failed: {script} {args:?}"
    );
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter(|line| line.starts_with('{'))
        .map(|line| serde_json::from_str(line).expect("verifiable tools emit one JSON per line"))
        .collect()
}

/// The events with the given `name` field, in emission order.
fn events_named<'events>(
    events: &'events [serde_json::Value],
    name: &str,
) -> Vec<&'events serde_json::Value> {
    events
        .iter()
        .filter(|event| event.get("name").and_then(serde_json::Value::as_str) == Some(name))
        .collect()
}

/// `(key, value, offset)` from a verifiable-tool record event
/// (`producer_send_success` and `record_data` share the field names).
fn record_triple(event: &serde_json::Value) -> (String, String, i64) {
    let field = |key: &str| {
        event
            .get(key)
            .and_then(serde_json::Value::as_str)
            .unwrap_or_else(|| panic!("event should carry string field {key}: {event}"))
            .to_owned()
    };
    let offset = event
        .get("offset")
        .and_then(serde_json::Value::as_i64)
        .unwrap_or_else(|| panic!("event should carry an offset: {event}"));
    (field("key"), field("value"), offset)
}

/// `CURRENT-OFFSET` for `group`/`topic` partition 0 as printed by the JVM
/// `kafka-consumer-groups.sh --describe` (columns: GROUP TOPIC PARTITION
/// CURRENT-OFFSET ...).
fn described_current_offset(group: &str, topic: &str) -> String {
    let output = cli_command("kafka-consumer-groups.sh", false)
        .args(["--describe", "--group", group])
        .output()
        .expect("kafka-consumer-groups.sh should run");
    assert!(
        output.status.success(),
        "kafka-consumer-groups.sh --describe failed for {group}"
    );
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let row = stdout
        .lines()
        .find(|line| {
            let mut fields = line.split_whitespace();
            fields.next() == Some(group) && fields.next() == Some(topic)
        })
        .unwrap_or_else(|| panic!("describe output has no row for {group}/{topic}:\n{stdout}"));
    row.split_whitespace()
        .nth(3)
        .expect("describe row should have a CURRENT-OFFSET column")
        .to_owned()
}

async fn kacrab_producer(client_id: &str, compression: Option<&str>) -> Producer {
    let mut builder = Producer::builder()
        .set("bootstrap.servers", bootstrap())
        .set("client.id", client_id)
        .set("enable.idempotence", "true")
        .set("acks", "all");
    if let Some(codec) = compression {
        builder = builder.set("compression.type", codec);
    }
    builder.build().await.expect("producer should connect")
}

async fn kacrab_consumer(group: &str) -> Consumer {
    Consumer::from_map([
        ("bootstrap.servers", bootstrap().as_str()),
        ("group.id", group),
        ("auto.offset.reset", "earliest"),
        ("enable.auto.commit", "false"),
    ])
    .await
    .expect("consumer should connect")
}

/// Poll until exactly `expect` records arrived (asserting the count) and
/// return them in arrival order.
async fn collect_records(consumer: &mut Consumer, expect: usize) -> Vec<ConsumerRecord> {
    let mut collected = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(30);
    while collected.len() < expect && Instant::now() < deadline {
        let records = consumer
            .poll(Duration::from_secs(1))
            .await
            .expect("poll should succeed");
        collected.extend(records);
    }
    assert_eq!(
        collected.len(),
        expect,
        "every expected record should arrive before the deadline"
    );
    collected
}

/// Render kacrab's view of a consumed record in the JVM console consumer's
/// `print.timestamp,print.headers,print.key` line format, so the two clients'
/// views can be compared as byte-identical lines.
fn as_console_line(record: &ConsumerRecord) -> String {
    let headers = if record.headers.is_empty() {
        "NO_HEADERS".to_owned()
    } else {
        record
            .headers
            .iter()
            .map(|header| {
                let value = header.value.as_ref().map_or_else(String::new, |value| {
                    String::from_utf8_lossy(value).into_owned()
                });
                format!("{}:{value}", String::from_utf8_lossy(&header.key))
            })
            .collect::<Vec<_>>()
            .join(",")
    };
    let key = record.key.as_ref().map_or_else(
        || "null".to_owned(),
        |key| String::from_utf8_lossy(key).into_owned(),
    );
    let value = record.value.as_ref().map_or_else(String::new, |value| {
        String::from_utf8_lossy(value).into_owned()
    });
    format!("CreateTime:{}\t{headers}\t{key}\t{value}", record.timestamp)
}

/// Dump partition 0's log segment and return each stored batch's
/// `compresscodec` (e.g. `GZIP`, `NONE`) — proves what the broker stored, not
/// just what round-tripped.
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

/// kacrab producer → JVM console consumer: keys, values, two headers per
/// record, and explicit `CreateTime` timestamps all arrive byte-exact. The
/// expected console lines are built from the values kacrab was ASKED to
/// produce, so any re-encoding on kacrab's side shows up as a line diff.
#[tokio::test]
#[ignore = "requires the broker from docker-compose.kafka.yml"]
async fn real_kafka_interop_kacrab_payloads_reach_jvm_console_consumer() {
    let topic = unique("kacrab-interop-out");
    create_topic(&topic);
    let base_ts: i64 = 1_700_000_000_000;

    let producer = kacrab_producer("kacrab-interop-out", None).await;
    let mut expected_lines = Vec::new();
    for index in 0..3_i64 {
        let ts = base_ts + index * 1_000;
        let record = ProducerRecord::new(topic.clone(), 0)
            .key(Bytes::from(format!("ik{index}")))
            .value(Bytes::from(format!("iv{index}")))
            .header("h1", format!("one-{index}"))
            .header("h2", format!("two-{index}"))
            .try_timestamp_ms(ts)
            .expect("valid timestamp");
        let receipt = producer
            .send(record)
            .expect("send should enqueue")
            .await
            .expect("delivery should complete");
        assert_eq!(receipt.offset, index, "receipts pin the produced order");
        expected_lines.push(format!(
            "CreateTime:{ts}\th1:one-{index},h2:two-{index}\tik{index}\tiv{index}"
        ));
    }
    producer.close().await.expect("producer should close");

    let lines = console_consume(
        &["--topic", &topic],
        &[
            "print.timestamp=true",
            "print.key=true",
            "print.headers=true",
        ],
        3,
        30_000,
    );
    assert_eq!(
        lines, expected_lines,
        "the JVM console consumer must print every timestamp/header/key/value byte kacrab produced"
    );
    delete_topic(&topic);
    println!("kacrab → JVM console consumer: payloads byte-exact: ALL OK");
}

/// kacrab producer with `compression.type=gzip` → JVM console consumer: the
/// broker stores a real GZIP batch (kafka-dump-log says so — a round-trip
/// alone would also pass if kacrab silently sent uncompressed) and the JVM
/// consumer decompresses it back to the exact keys/values/timestamps.
#[tokio::test]
#[ignore = "requires the broker from docker-compose.kafka.yml"]
async fn real_kafka_interop_kacrab_gzip_batch_reaches_jvm_console_consumer() {
    let topic = unique("kacrab-interop-gzip");
    create_topic(&topic);
    let base_ts: i64 = 1_700_000_100_000;

    let producer = kacrab_producer("kacrab-interop-gzip", Some("gzip")).await;
    let mut expected_lines = Vec::new();
    for index in 0..3_i64 {
        let ts = base_ts + index * 1_000;
        // A compressible tail so the codec actually does work.
        let value = format!("gzip-{index}-{}", "payload".repeat(32));
        let record = ProducerRecord::new(topic.clone(), 0)
            .key(Bytes::from(format!("gk{index}")))
            .value(Bytes::from(value.clone()))
            .header("codec", "gzip")
            .try_timestamp_ms(ts)
            .expect("valid timestamp");
        let _receipt = producer
            .send(record)
            .expect("send should enqueue")
            .await
            .expect("delivery should complete");
        expected_lines.push(format!("CreateTime:{ts}\tcodec:gzip\tgk{index}\t{value}"));
    }
    producer.close().await.expect("producer should close");

    let stored = stored_codecs(&topic);
    assert!(
        stored
            .iter()
            .any(|codec| codec.eq_ignore_ascii_case("gzip")),
        "the broker did not store a GZIP batch (stored codecs: {stored:?})"
    );

    let lines = console_consume(
        &["--topic", &topic],
        &[
            "print.timestamp=true",
            "print.key=true",
            "print.headers=true",
        ],
        3,
        30_000,
    );
    assert_eq!(
        lines, expected_lines,
        "the JVM console consumer must decompress kacrab's gzip batch to the exact payloads"
    );
    delete_topic(&topic);
    println!("kacrab gzip → JVM console consumer: stored GZIP, payloads byte-exact: ALL OK");
}

/// JVM console producer (keys + headers via `LineMessageReader`) → kacrab
/// consumer: keys, values, and headers arrive byte-exact and in order. The
/// tool cannot SET an explicit timestamp, so the timestamp assertion is
/// cross-client agreement instead: kacrab's consumed records, re-rendered in
/// the console consumer's line format, must equal the JVM console consumer's
/// own output byte for byte (same `CreateTime`, same everything).
#[tokio::test]
#[ignore = "requires the broker from docker-compose.kafka.yml"]
async fn real_kafka_interop_jvm_console_producer_payloads_reach_kacrab() {
    let topic = unique("kacrab-interop-in");
    create_topic(&topic);

    // `parse.headers=true` + `parse.key=true` line shape: "h1:v1,h2:v2\tkey\tvalue".
    let payload = (0..3_usize)
        .map(|index| format!("ha:va-{index},hb:vb-{index}\tjk{index}\tjv{index}"))
        .collect::<Vec<_>>()
        .join("\n");
    kafka_cli_stdin(
        "kafka-console-producer.sh",
        &[
            "--topic",
            &topic,
            "--reader-property",
            "parse.key=true",
            "--reader-property",
            "parse.headers=true",
        ],
        &format!("{payload}\n"),
    );

    let mut consumer = kacrab_consumer(&unique("kacrab-interop-in-group")).await;
    consumer
        .assign([TopicPartition::new(topic.clone(), 0)])
        .expect("assign");
    let records = collect_records(&mut consumer, 3).await;
    for (index, record) in records.iter().enumerate() {
        let offset = i64::try_from(index).expect("small index");
        assert_eq!(record.offset, offset, "records arrive in produced order");
        assert_eq!(
            record.key.as_deref(),
            Some(format!("jk{index}").as_bytes()),
            "key produced by the JVM console producer arrives byte-exact"
        );
        assert_eq!(
            record.value.as_deref(),
            Some(format!("jv{index}").as_bytes()),
            "value produced by the JVM console producer arrives byte-exact"
        );
        assert_eq!(
            record.headers,
            vec![
                RecordHeader {
                    key: Bytes::from_static(b"ha"),
                    value: Some(Bytes::from(format!("va-{index}"))),
                },
                RecordHeader {
                    key: Bytes::from_static(b"hb"),
                    value: Some(Bytes::from(format!("vb-{index}"))),
                },
            ],
            "headers produced by the JVM console producer arrive byte-exact and ordered"
        );
        assert_eq!(
            record.timestamp_type,
            TimestampType::CreateTime,
            "the console producer stamps CreateTime"
        );
    }
    consumer.close().await;

    // Cross-client timestamp equality: both clients must report the same
    // stored CreateTime (and everything else) for every record.
    let jvm_lines = console_consume(
        &["--topic", &topic],
        &[
            "print.timestamp=true",
            "print.key=true",
            "print.headers=true",
        ],
        3,
        30_000,
    );
    let kacrab_lines: Vec<String> = records.iter().map(as_console_line).collect();
    assert_eq!(
        kacrab_lines, jvm_lines,
        "kacrab and the JVM consumer must agree byte-for-byte on the stored records, timestamps \
         included"
    );
    delete_topic(&topic);
    println!("JVM console producer → kacrab: payloads byte-exact, timestamps agree: ALL OK");
}

/// JVM verifiable producer (`--message-create-time`, `--repeating-keys`) →
/// kacrab consumer: the exact `(key, value, offset)` triples the JVM producer
/// reported acked are what kacrab observes, in order, and every record carries
/// a `CreateTime` inside the tool's deterministic bounds (base + elapsed wall
/// time), monotonically non-decreasing.
#[tokio::test]
#[ignore = "requires the broker from docker-compose.kafka.yml"]
async fn real_kafka_interop_jvm_verifiable_producer_acks_match_kacrab_view() {
    let topic = unique("kacrab-interop-verif");
    create_topic(&topic);
    let base_ts: i64 = 1_700_000_200_000;

    let events = run_verifiable(
        "kafka-verifiable-producer.sh",
        &[
            "--topic",
            &topic,
            "--max-messages",
            "6",
            "--repeating-keys",
            "3",
            "--message-create-time",
            &base_ts.to_string(),
            "--acks",
            "-1",
        ],
    );
    let acked: Vec<(String, String, i64)> = events_named(&events, "producer_send_success")
        .into_iter()
        .map(record_triple)
        .collect();
    assert_eq!(acked.len(), 6, "the JVM producer must ack all 6 sends");

    let mut consumer = kacrab_consumer(&unique("kacrab-interop-verif-group")).await;
    consumer
        .assign([TopicPartition::new(topic.clone(), 0)])
        .expect("assign");
    let records = collect_records(&mut consumer, 6).await;
    let seen: Vec<(String, String, i64)> = records
        .iter()
        .map(|record| {
            (
                String::from_utf8_lossy(record.key.as_deref().expect("verifiable producer keys"))
                    .into_owned(),
                String::from_utf8_lossy(record.value.as_deref().expect("verifiable values"))
                    .into_owned(),
                record.offset,
            )
        })
        .collect();
    assert_eq!(
        seen, acked,
        "kacrab must observe exactly the (key, value, offset) triples the JVM producer acked"
    );

    let mut previous = base_ts;
    for record in &records {
        assert_eq!(
            record.timestamp_type,
            TimestampType::CreateTime,
            "--message-create-time stamps CreateTime"
        );
        assert!(
            record.timestamp >= base_ts && record.timestamp <= base_ts + 120_000,
            "CreateTime must be base + elapsed (base {base_ts}, got {})",
            record.timestamp
        );
        assert!(
            record.timestamp >= previous,
            "CreateTimes advance monotonically ({previous} then {})",
            record.timestamp
        );
        previous = record.timestamp;
    }
    consumer.close().await;
    delete_topic(&topic);
    println!("JVM verifiable producer → kacrab: acks, payloads and timestamps agree: ALL OK");
}

/// Group offset interop, kacrab committing: kacrab commits offset 3 of 5; the
/// JVM `kafka-consumer-groups.sh --describe` must report exactly 3; a JVM
/// verifiable consumer joining the SAME group must resume at offset 3 and
/// consume exactly the remaining payloads, after which its own commit (5) is
/// what the describe tool reports.
#[tokio::test]
#[ignore = "requires the broker from docker-compose.kafka.yml"]
async fn real_kafka_interop_kacrab_commit_drives_jvm_group_tools() {
    let topic = unique("kacrab-interop-offsets");
    let group = unique("kacrab-interop-offsets-group");
    create_topic(&topic);

    let producer = kacrab_producer("kacrab-interop-offsets", None).await;
    for index in 0..5_i64 {
        let record = ProducerRecord::new(topic.clone(), 0)
            .key(Bytes::from(format!("gk{index}")))
            .value(Bytes::from(format!("gv{index}")));
        let _receipt = producer
            .send(record)
            .expect("send should enqueue")
            .await
            .expect("delivery should complete");
    }
    producer.close().await.expect("producer should close");

    let partition = TopicPartition::new(topic.clone(), 0);
    let mut consumer = kacrab_consumer(&group).await;
    consumer.assign([partition.clone()]).expect("assign");
    let _records = collect_records(&mut consumer, 5).await;
    let offsets = HashMap::from([(partition.clone(), OffsetAndMetadata::new(3))]);
    consumer
        .commit_sync_offsets(offsets)
        .await
        .expect("commit_sync_offsets should succeed");
    let committed = consumer
        .committed(std::slice::from_ref(&partition))
        .await
        .expect("committed should succeed");
    assert_eq!(
        committed.get(&partition).map(|meta| meta.offset),
        Some(3),
        "kacrab's own read-back sees the commit before the JVM tool is asked"
    );
    consumer.close().await;

    assert_eq!(
        described_current_offset(&group, &topic),
        "3",
        "kafka-consumer-groups.sh --describe must report the exact offset kacrab committed"
    );

    // A JVM consumer in the SAME group resumes from kacrab's committed offset.
    let events = run_verifiable(
        "kafka-verifiable-consumer.sh",
        &[
            "--topic",
            &topic,
            "--group-id",
            &group,
            "--max-messages",
            "2",
            "--verbose",
        ],
    );
    let resumed: Vec<(String, String, i64)> = events_named(&events, "record_data")
        .into_iter()
        .map(record_triple)
        .collect();
    assert_eq!(
        resumed,
        vec![
            ("gk3".to_owned(), "gv3".to_owned(), 3),
            ("gk4".to_owned(), "gv4".to_owned(), 4),
        ],
        "the JVM consumer must resume at kacrab's committed offset and see the exact remainder"
    );
    let last_commit = events_named(&events, "offsets_committed")
        .into_iter()
        .next_back()
        .expect("the JVM consumer commits what it consumed");
    assert_eq!(
        last_commit
            .get("success")
            .and_then(serde_json::Value::as_bool),
        Some(true),
        "the JVM consumer's commit must succeed: {last_commit}"
    );
    assert_eq!(
        described_current_offset(&group, &topic),
        "5",
        "after the JVM consumer drains the topic, describe reports the log end"
    );
    delete_topic(&topic);
    println!("kacrab commit → JVM describe + resume: exact offsets: ALL OK");
}

/// Group offset interop, the JVM committing: a JVM verifiable consumer
/// consumes 4 records and commits; kacrab `committed()` must report the exact
/// offset the JVM tool said it committed, and a kacrab consumer subscribing in
/// the same group must resume from it (first new record at that offset).
#[tokio::test]
#[ignore = "requires the broker from docker-compose.kafka.yml"]
async fn real_kafka_interop_jvm_commit_agrees_with_kacrab_committed() {
    let topic = unique("kacrab-interop-jvmcommit");
    let group = unique("kacrab-interop-jvmcommit-group");
    create_topic(&topic);

    let producer = kacrab_producer("kacrab-interop-jvmcommit", None).await;
    for index in 0..4_i64 {
        let record = ProducerRecord::new(topic.clone(), 0)
            .key(Bytes::from(format!("jck{index}")))
            .value(Bytes::from(format!("jcv{index}")));
        let _receipt = producer
            .send(record)
            .expect("send should enqueue")
            .await
            .expect("delivery should complete");
    }

    let events = run_verifiable(
        "kafka-verifiable-consumer.sh",
        &[
            "--topic",
            &topic,
            "--group-id",
            &group,
            "--max-messages",
            "4",
            "--verbose",
        ],
    );
    let consumed: Vec<(String, String, i64)> = events_named(&events, "record_data")
        .into_iter()
        .map(record_triple)
        .collect();
    let expected: Vec<(String, String, i64)> = (0..4_i64)
        .map(|index| (format!("jck{index}"), format!("jcv{index}"), index))
        .collect();
    assert_eq!(
        consumed, expected,
        "the JVM consumer must see exactly the payloads kacrab produced"
    );
    let last_commit = events_named(&events, "offsets_committed")
        .into_iter()
        .next_back()
        .expect("the JVM consumer commits what it consumed");
    assert_eq!(
        last_commit
            .get("success")
            .and_then(serde_json::Value::as_bool),
        Some(true),
        "the JVM consumer's commit must succeed: {last_commit}"
    );
    let jvm_committed = last_commit
        .get("offsets")
        .and_then(serde_json::Value::as_array)
        .and_then(|offsets| offsets.first())
        .and_then(|entry| entry.get("offset"))
        .and_then(serde_json::Value::as_i64)
        .expect("offsets_committed carries the committed offset");
    assert_eq!(jvm_committed, 4, "the JVM consumer committed the log end");

    // kacrab agrees on the exact committed offset.
    let partition = TopicPartition::new(topic.clone(), 0);
    let mut reader = kacrab_consumer(&group).await;
    reader.assign([partition.clone()]).expect("assign");
    let committed = reader
        .committed(std::slice::from_ref(&partition))
        .await
        .expect("committed should succeed");
    assert_eq!(
        committed.get(&partition).map(|meta| meta.offset),
        Some(jvm_committed),
        "kacrab committed() must report the exact offset the JVM consumer committed"
    );
    reader.close().await;

    // And a kacrab group member resumes from the JVM-committed offset: two new
    // records land at offsets 4..5, and a fresh subscriber in the same group
    // must see exactly those (starting at 0 instead would mean the JVM commit
    // was ignored). `subscribe` is used deliberately: it is the path that
    // initializes positions from the group's committed offsets.
    for index in 4..6_i64 {
        let record = ProducerRecord::new(topic.clone(), 0)
            .key(Bytes::from(format!("jck{index}")))
            .value(Bytes::from(format!("jcv{index}")));
        let _receipt = producer
            .send(record)
            .expect("send should enqueue")
            .await
            .expect("delivery should complete");
    }
    producer.close().await.expect("producer should close");

    let mut member = kacrab_consumer(&group).await;
    member.subscribe([topic.clone()]).expect("subscribe");
    let records = collect_records(&mut member, 2).await;
    let resumed: Vec<(Option<&[u8]>, i64)> = records
        .iter()
        .map(|record| (record.value.as_deref(), record.offset))
        .collect();
    assert_eq!(
        resumed,
        vec![(Some(b"jcv4".as_slice()), 4), (Some(b"jcv5".as_slice()), 5),],
        "kacrab must resume from the offset the JVM consumer committed"
    );
    member.close().await;
    delete_topic(&topic);
    println!("JVM commit → kacrab committed() + resume: exact offsets: ALL OK");
}

/// Transactional interop: kacrab commits, aborts, then commits again; the JVM
/// console consumer under `read_committed` sees exactly the two committed
/// records (by offset, key, and value). The negative control is the SAME tool
/// under `read_uncommitted` seeing the aborted record at its receipt offset —
/// proving the aborted data reached the log and was actively filtered, not
/// never written.
#[tokio::test]
#[ignore = "requires the broker from docker-compose.kafka.yml"]
async fn real_kafka_interop_kacrab_transactions_respect_jvm_isolation_levels() {
    let topic = unique("kacrab-interop-txn");
    create_topic(&topic);

    let producer = Producer::builder()
        .set("bootstrap.servers", bootstrap())
        .set("client.id", "kacrab-interop-txn")
        .set("transactional.id", unique("kacrab-interop-txn-id"))
        .set("enable.idempotence", "true")
        .set("acks", "all")
        .set("transaction.timeout.ms", "60000")
        .build()
        .await
        .expect("transactional producer should connect");
    producer.init_transactions().await.expect("init");

    producer.begin_transaction().expect("begin first txn");
    let receipt = producer
        .send(
            ProducerRecord::new(topic.clone(), 0)
                .key(Bytes::from_static(b"tk1"))
                .value(Bytes::from_static(b"txn-committed-1")),
        )
        .expect("send")
        .await
        .expect("delivery");
    assert_eq!(receipt.offset, 0, "first committed record at offset 0");
    producer.commit_transaction().await.expect("first commit");

    producer.begin_transaction().expect("begin aborted txn");
    let receipt = producer
        .send(
            ProducerRecord::new(topic.clone(), 0)
                .key(Bytes::from_static(b"tk2"))
                .value(Bytes::from_static(b"txn-aborted")),
        )
        .expect("send")
        .await
        .expect("the aborted record still reaches the log");
    assert_eq!(
        receipt.offset, 2,
        "aborted record follows the first commit marker"
    );
    producer.abort_transaction().await.expect("abort");

    producer.begin_transaction().expect("begin second txn");
    let receipt = producer
        .send(
            ProducerRecord::new(topic.clone(), 0)
                .key(Bytes::from_static(b"tk3"))
                .value(Bytes::from_static(b"txn-committed-2")),
        )
        .expect("send")
        .await
        .expect("delivery");
    assert_eq!(
        receipt.offset, 4,
        "second committed record follows the abort marker"
    );
    producer.commit_transaction().await.expect("second commit");
    producer.close().await.expect("producer should close");

    // Negative control: read_uncommitted MUST see the aborted record — the
    // exact record, at the receipt's offset.
    let uncommitted = console_consume(
        &["--topic", &topic, "--isolation-level", "read_uncommitted"],
        &["print.offset=true", "print.key=true"],
        3,
        30_000,
    );
    assert_eq!(
        uncommitted,
        vec![
            "Offset:0\ttk1\ttxn-committed-1".to_owned(),
            "Offset:2\ttk2\ttxn-aborted".to_owned(),
            "Offset:4\ttk3\ttxn-committed-2".to_owned(),
        ],
        "read_uncommitted must see all three records, aborted one included"
    );

    // read_committed deliberately over-asks (3) and exits on --timeout-ms:
    // exactly the two committed records may appear.
    let committed = console_consume(
        &["--topic", &topic, "--isolation-level", "read_committed"],
        &["print.offset=true", "print.key=true"],
        3,
        15_000,
    );
    assert_eq!(
        committed,
        vec![
            "Offset:0\ttk1\ttxn-committed-1".to_owned(),
            "Offset:4\ttk3\ttxn-committed-2".to_owned(),
        ],
        "read_committed must see exactly the committed records — the aborted one filtered"
    );
    delete_topic(&topic);
    println!("kacrab transactions → JVM isolation levels: exact visibility: ALL OK");
}
