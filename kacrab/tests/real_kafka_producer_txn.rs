//! Real Kafka transactional-producer integration tests: transactional offset
//! commits (consume-transform-produce), producer fencing, and abort-then-commit
//! reuse, against the broker from `docker-compose.kafka.yml`. Run:
//! `cargo test -p kacrab --features producer,consumer --test real_kafka_producer_txn \
//!  -- --ignored --test-threads=1`

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
    process::Command,
    sync::atomic::{AtomicU32, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use kacrab::{
    common::{OffsetAndMetadata, TopicPartition},
    consumer::Consumer,
    producer::{Producer, ProducerError, ProducerRecord},
};
use kacrab_protocol::generated::ErrorCode;

fn bootstrap() -> String {
    env::var("KACRAB_BOOTSTRAP").unwrap_or_else(|_error| "127.0.0.1:9092".to_owned())
}

/// Process-unique suffix for topic/group/transactional ids so concurrent and
/// repeated runs against the shared broker never collide.
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
/// `KACRAB_KAFKA_BIN` points at its `bin/`.
fn create_topic(topic: &str, partitions: u32) {
    let mut command = env::var("KACRAB_KAFKA_BIN").map_or_else(
        |_error| {
            let mut command = Command::new("docker");
            let _args = command
                .args(["exec", "kacrab-kafka", "/opt/kafka/bin/kafka-topics.sh"])
                .args(["--bootstrap-server", "localhost:9092"]);
            command
        },
        |bin| {
            let mut command = Command::new(format!("{bin}/kafka-topics.sh"));
            let _args = command.args(["--bootstrap-server", &bootstrap()]);
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

async fn transactional_producer(transactional_id: &str, client_id: &str) -> Producer {
    Producer::builder()
        .set("bootstrap.servers", bootstrap())
        .set("client.id", client_id)
        .set("transactional.id", transactional_id)
        .set("enable.idempotence", "true")
        .set("acks", "all")
        .set("transaction.timeout.ms", "60000")
        .build()
        .await
        .expect("producer should connect to local Kafka")
}

/// Read `partition` 0 of `topic` from the beginning under `isolation`, until
/// `enough(values)` says the terminal state was observed or 30s elapse.
async fn read_values(
    topic: &str,
    isolation: &str,
    enough: impl Fn(&[String]) -> bool,
) -> Vec<String> {
    let group = format!("kacrab-txn-read-{}", unique_suffix());
    let mut consumer = Consumer::from_map([
        ("bootstrap.servers", bootstrap().as_str()),
        ("group.id", group.as_str()),
        ("auto.offset.reset", "earliest"),
        ("enable.auto.commit", "false"),
        ("isolation.level", isolation),
    ])
    .await
    .expect("consumer should connect");
    consumer
        .assign([TopicPartition::new(topic.to_owned(), 0)])
        .expect("assign");
    let mut values = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while !enough(&values) && std::time::Instant::now() < deadline {
        let records = consumer.poll(Duration::from_secs(2)).await.expect("poll");
        for record in records {
            values.push(
                record
                    .value
                    .map(|value| String::from_utf8_lossy(&value).into_owned())
                    .unwrap_or_default(),
            );
        }
    }
    consumer.close().await;
    values
}

/// `send_offsets_to_transaction` end to end (consume-transform-produce): the
/// group offset commit rides the producer transaction, so it is invisible to
/// `committed()` while the transaction is open — the negative control — and
/// becomes visible, together with the transformed output records under
/// `read_committed`, only after `commit_transaction`.
#[tokio::test]
#[ignore = "requires the broker from docker-compose.kafka.yml"]
#[expect(
    clippy::too_many_lines,
    reason = "Seed, transform, negative control, and post-commit checks form one atomicity story."
)]
async fn real_kafka_send_offsets_to_transaction_commits_group_offset_atomically() {
    let suffix = unique_suffix();
    let topic_in = format!("kacrab-txn-in-{suffix}");
    let topic_out = format!("kacrab-txn-out-{suffix}");
    let group = format!("kacrab-txn-group-{suffix}");
    create_topic(&topic_in, 1);
    create_topic(&topic_out, 1);
    let partition_in = TopicPartition::new(topic_in.clone(), 0);

    let seed = Producer::builder()
        .set("bootstrap.servers", bootstrap())
        .set("client.id", "kacrab-txn-seed")
        .set("acks", "all")
        .build()
        .await
        .expect("seed producer should connect");
    for index in 0..3_usize {
        let _receipt = seed
            .send(
                ProducerRecord::new(topic_in.clone(), 0).value(Bytes::from(format!("in-{index}"))),
            )
            .expect("send")
            .await
            .expect("seed delivery");
    }

    let mut consumer = Consumer::from_map([
        ("bootstrap.servers", bootstrap().as_str()),
        ("group.id", group.as_str()),
        ("auto.offset.reset", "earliest"),
        ("enable.auto.commit", "false"),
    ])
    .await
    .expect("consumer should connect");
    consumer.assign([partition_in.clone()]).expect("assign");
    let mut inputs: Vec<String> = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while inputs.len() < 3 && std::time::Instant::now() < deadline {
        let records = consumer.poll(Duration::from_secs(2)).await.expect("poll");
        for record in records {
            let value = record.value.expect("input records carry a value");
            inputs.push(String::from_utf8(value.to_vec()).expect("utf-8 payload"));
        }
    }
    assert_eq!(inputs.len(), 3, "the transform must see every input record");
    let position = consumer
        .position(&partition_in)
        .await
        .expect("position should resolve");
    assert_eq!(position, 3, "all inputs consumed");

    let producer =
        transactional_producer(&format!("kacrab-txn-offsets-{suffix}"), "kacrab-txn-ctp").await;
    producer.init_transactions().await.expect("init");
    producer.begin_transaction().expect("begin");
    for input in &inputs {
        // Await the receipt so the transformed record provably reached the log
        // before the offsets ride the transaction.
        let receipt = producer
            .send(
                ProducerRecord::new(topic_out.clone(), 0)
                    .value(Bytes::from(format!("out-{input}"))),
            )
            .expect("transactional send")
            .await
            .expect("transactional delivery");
        assert_eq!(
            receipt.topic.as_ref(),
            topic_out.as_str(),
            "receipt names the output topic"
        );
    }
    producer
        .send_offsets_to_transaction(
            [(partition_in.clone(), OffsetAndMetadata::new(position))],
            consumer.group_metadata(),
        )
        .await
        .expect("send_offsets_to_transaction");

    // Negative control: with the transaction still open, the offset commit and
    // the output records must both be invisible.
    let committed = consumer
        .committed(std::slice::from_ref(&partition_in))
        .await
        .expect("committed pre-commit");
    assert!(
        !committed.contains_key(&partition_in),
        "the transactional offset commit leaked out of the open transaction: {committed:?}"
    );

    producer.commit_transaction().await.expect("commit");

    let outputs = read_values(&topic_out, "read_committed", |values| values.len() >= 3).await;
    assert_eq!(
        outputs,
        vec![
            "out-in-0".to_owned(),
            "out-in-1".to_owned(),
            "out-in-2".to_owned()
        ],
        "read_committed sees exactly the transformed records after commit"
    );

    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let committed_offset = loop {
        let committed = consumer
            .committed(std::slice::from_ref(&partition_in))
            .await
            .expect("committed post-commit");
        if let Some(offset_and_metadata) = committed.get(&partition_in) {
            break offset_and_metadata.offset;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the committed transaction never made the group offset visible"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    assert_eq!(
        committed_offset, 3,
        "the group offset committed through the transaction is the consumed position"
    );
    consumer.close().await;
}

/// True when `error` is the fenced-producer signal a zombie must receive after
/// another producer initialized the same `transactional.id` (KIP-98 fencing:
/// `ProducerFenced`, or the raw `InvalidProducerEpoch` it is derived from).
const fn is_fenced(error: &ProducerError) -> bool {
    matches!(
        error,
        ProducerError::Transaction {
            error: ErrorCode::ProducerFenced | ErrorCode::InvalidProducerEpoch,
            ..
        } | ProducerError::Broker {
            error: ErrorCode::ProducerFenced | ErrorCode::InvalidProducerEpoch,
            ..
        }
    )
}

/// Zombie fencing: initializing a second producer with the same
/// `transactional.id` bumps the epoch, so the first producer's next
/// transactional round fails with the fenced error instead of silently
/// writing — while the fencing producer keeps working, the positive control
/// proving the id itself stayed usable.
#[tokio::test]
#[ignore = "requires the broker from docker-compose.kafka.yml"]
async fn real_kafka_second_init_fences_prior_producer() {
    let suffix = unique_suffix();
    let topic = format!("kacrab-txn-fence-{suffix}");
    let transactional_id = format!("kacrab-txn-fence-id-{suffix}");
    create_topic(&topic, 1);

    let first = transactional_producer(&transactional_id, "kacrab-fence-first").await;
    first.init_transactions().await.expect("first init");
    first.begin_transaction().expect("first begin");
    let _receipt = first
        .send(ProducerRecord::new(topic.clone(), 0).value(Bytes::from_static(b"first-epoch")))
        .expect("send")
        .await
        .expect("pre-fence delivery");
    first.commit_transaction().await.expect("pre-fence commit");

    let second = transactional_producer(&transactional_id, "kacrab-fence-second").await;
    second
        .init_transactions()
        .await
        .expect("second init with the same transactional.id must fence, not fail");

    // The zombie's next transactional round must surface the fenced error at
    // one of its steps; if every step succeeds, fencing is not enforced.
    let mut fenced = first.begin_transaction().err();
    if fenced.is_none() {
        fenced = match first
            .send(ProducerRecord::new(topic.clone(), 0).value(Bytes::from_static(b"zombie-write")))
        {
            Err(error) => Some(error),
            Ok(delivery) => delivery.await.err(),
        };
    }
    if fenced.is_none() {
        fenced = first.commit_transaction().await.err();
    }
    let error = fenced.expect(
        "the fenced producer completed a whole transactional round after another producer took \
         its transactional.id",
    );
    assert!(
        is_fenced(&error),
        "the zombie must fail with the fenced/epoch error, got: {error}"
    );

    second.begin_transaction().expect("second begin");
    let _receipt = second
        .send(ProducerRecord::new(topic.clone(), 0).value(Bytes::from_static(b"second-epoch")))
        .expect("send")
        .await
        .expect("fencing producer delivery");
    second
        .commit_transaction()
        .await
        .expect("the fencing producer must keep working");

    let committed = read_values(&topic, "read_committed", |values| values.len() >= 2).await;
    assert!(
        committed.contains(&"first-epoch".to_owned())
            && committed.contains(&"second-epoch".to_owned()),
        "both committed epochs' records are visible: {committed:?}"
    );
    assert!(
        !committed.contains(&"zombie-write".to_owned()),
        "a fenced producer's write must never become visible: {committed:?}"
    );
}

/// Abort recovery on one producer instance: after `abort_transaction` the same
/// producer opens a fresh transaction that commits cleanly, and `read_committed`
/// sees only the second transaction's record. The receipt offsets pin the log
/// layout (aborted record, abort marker, committed record), proving the aborted
/// data reached the log rather than the abort being a no-op.
#[tokio::test]
#[ignore = "requires the broker from docker-compose.kafka.yml"]
async fn real_kafka_abort_then_next_transaction_commits_cleanly() {
    let suffix = unique_suffix();
    let topic = format!("kacrab-txn-abort-{suffix}");
    create_topic(&topic, 1);

    let producer =
        transactional_producer(&format!("kacrab-txn-abort-id-{suffix}"), "kacrab-txn-abort").await;
    producer.init_transactions().await.expect("init");

    producer.begin_transaction().expect("begin abort txn");
    let aborted_receipt = producer
        .send(ProducerRecord::new(topic.clone(), 0).value(Bytes::from_static(b"first-aborted")))
        .expect("send")
        .await
        .expect("aborted record reaches the log");
    assert_eq!(
        aborted_receipt.offset, 0,
        "aborted record occupies offset 0"
    );
    producer.abort_transaction().await.expect("abort");

    producer.begin_transaction().expect("begin commit txn");
    let committed_receipt = producer
        .send(ProducerRecord::new(topic.clone(), 0).value(Bytes::from_static(b"second-committed")))
        .expect("send after abort")
        .await
        .expect("post-abort delivery");
    assert_eq!(
        committed_receipt.offset, 2,
        "the committed record follows the aborted record and its abort marker"
    );
    producer.commit_transaction().await.expect("commit");

    // Terminal state: the committed record observed AND either the aborted one
    // leaked (bug, assert below) or one further poll stayed clean.
    let committed_view = read_values(&topic, "read_committed", |values| {
        values.iter().any(|value| value == "first-aborted")
            || values.iter().any(|value| value == "second-committed")
    })
    .await;
    assert_eq!(
        committed_view,
        vec!["second-committed".to_owned()],
        "read_committed sees exactly the post-abort transaction's record"
    );
}
