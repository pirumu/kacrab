//! Classic consumer group example: subscribe, poll, commit.
//!
//! Reads back what `--example producer` wrote. Run against a local Kafka broker
//! (e.g. `docker-compose.kafka.yml`), producing first so there is something to
//! read:
//!
//! ```text
//! cargo run -p kacrab-examples --example producer
//! cargo run -p kacrab-examples --example consumer
//! ```
//!
//! Optional positional arguments (bootstrap, topic, group, messages):
//!
//! ```text
//! cargo run -p kacrab-examples --example consumer -- \
//!   127.0.0.1:9092 kacrab-example kacrab-example-group 12
//! ```
//!
//! `messages` is how many records to wait for before committing and exiting; the
//! poll loop also gives up after [`POLL_BUDGET`] so the example always
//! terminates.

use std::{
    env,
    error::Error,
    time::{Duration, Instant},
};

use bytes::Bytes;
use kacrab::{
    common::TopicPartition,
    consumer::{Consumer, ConsumerRecord},
};

const CLIENT_ID: &str = "kacrab-example-consumer";
/// Start at the beginning of the log when the group has no committed offset.
const AUTO_OFFSET_RESET: &str = "earliest";
/// Commits are made explicitly below. Flip this to `"true"` (and set
/// `auto.commit.interval.ms`) to let the client commit the polled position on an
/// interval in the background — and once more on `close()` — which trades the
/// "commit only after the work succeeded" guarantee for less code.
const ENABLE_AUTO_COMMIT: &str = "false";
const AUTO_COMMIT_INTERVAL_MS: &str = "5000";
/// How long one `poll` waits for the broker before returning an empty batch.
const POLL_TIMEOUT: Duration = Duration::from_secs(2);
/// Total time the loop spends waiting for `messages` records.
const POLL_BUDGET: Duration = Duration::from_secs(30);

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = ExampleArgs::parse(env::args().skip(1))?;
    println!(
        "consumer example: bootstrap={} topic={} group={} messages={}",
        args.bootstrap, args.topic, args.group, args.messages
    );

    let mut consumer = build_consumer(&args).await?;

    // Subscribe: the group coordinator assigns partitions, so this member's
    // assignment only exists after the first poll completes the join.
    consumer.subscribe([args.topic.clone()])?;
    println!("subscription: {:?}", consumer.subscription());

    let total = poll_loop(&mut consumer, args.messages).await?;
    println!(
        "assignment after join: {:?}",
        partitions_of(&consumer.assignment())
    );

    // Manual synchronous commit: the offsets are only durable once the work
    // above succeeded, which is what makes this at-least-once rather than
    // at-most-once. `commit_sync` commits the current position of every assigned
    // partition; `commit_sync_offsets` takes an explicit offset map instead.
    consumer.commit_sync().await?;
    print_committed(&mut consumer).await?;

    // `close` leaves the group (so the rest of the group rebalances promptly
    // instead of waiting for the session to time out) and, with auto-commit on,
    // commits one last time.
    consumer.close().await;
    println!("consumed {total} record(s) — done");
    Ok(())
}

async fn build_consumer(args: &ExampleArgs) -> Result<Consumer, Box<dyn Error>> {
    // The consumer is configured from Kafka's own property names, so a config
    // lifted from a Java client works here unchanged.
    Consumer::from_map([
        ("bootstrap.servers", args.bootstrap.as_str()),
        ("client.id", CLIENT_ID),
        ("group.id", args.group.as_str()),
        ("auto.offset.reset", AUTO_OFFSET_RESET),
        ("enable.auto.commit", ENABLE_AUTO_COMMIT),
        ("auto.commit.interval.ms", AUTO_COMMIT_INTERVAL_MS),
    ])
    .await
    .map_err(Into::into)
    // TLS, SASL, and Kerberos use the same property names as the producer:
    //     ("security.protocol", "SASL_SSL"),
    //     ("sasl.mechanism", "PLAIN"),
}

/// Poll until `wanted` records have been seen or [`POLL_BUDGET`] runs out.
async fn poll_loop(consumer: &mut Consumer, wanted: usize) -> Result<usize, Box<dyn Error>> {
    let deadline = Instant::now()
        .checked_add(POLL_BUDGET)
        .ok_or("poll deadline overflowed")?;
    let mut total = 0_usize;

    while total < wanted && Instant::now() < deadline {
        // One poll returns a batch grouped by partition; iterating yields the
        // records in partition then offset order.
        let records = consumer.poll(POLL_TIMEOUT).await?;
        for record in &records {
            print_record(record);
            total = total.saturating_add(1);
        }
    }

    Ok(total)
}

fn print_record(record: &ConsumerRecord) {
    println!(
        "  {}-{} offset={} timestamp={} key={:?} value={:?}",
        record.topic,
        record.partition,
        record.offset,
        record.timestamp,
        record.key.as_ref().map(text),
        record.value.as_ref().map(text)
    );
}

async fn print_committed(consumer: &mut Consumer) -> Result<(), Box<dyn Error>> {
    let assignment = consumer.assignment();
    let committed = consumer.committed(&assignment).await?;
    for partition in &assignment {
        println!(
            "committed: {}-{} -> {:?}",
            partition.topic,
            partition.partition,
            committed.get(partition).map(|offset| offset.offset)
        );
    }
    Ok(())
}

/// Record payloads are raw bytes; decode them for display only. A real
/// application would go through a `ConsumerDeserializer` instead — see
/// `--example typed_serializer` for the producer-side mirror of that trait.
fn text(bytes: &Bytes) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

fn partitions_of(assignment: &[TopicPartition]) -> Vec<String> {
    assignment
        .iter()
        .map(|partition| format!("{}-{}", partition.topic, partition.partition))
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ExampleArgs {
    bootstrap: String,
    topic: String,
    group: String,
    messages: usize,
}

impl ExampleArgs {
    fn parse(args: impl IntoIterator<Item = String>) -> Result<Self, Box<dyn Error>> {
        let mut args = args.into_iter();
        let bootstrap = args.next().unwrap_or_else(|| "127.0.0.1:9092".to_owned());
        let topic = args.next().unwrap_or_else(|| "kacrab-example".to_owned());
        let group = args
            .next()
            .unwrap_or_else(|| "kacrab-example-group".to_owned());
        let messages = match args.next() {
            Some(value) => value.parse()?,
            None => 12,
        };
        Ok(Self {
            bootstrap,
            topic,
            group,
            messages,
        })
    }
}
