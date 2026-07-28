//! Exactly-once (EOS) consume-transform-produce with a transactional producer.
//!
//! Reads from an input topic with a `read_committed` consumer, writes the
//! transformed records to an output topic, and commits the *input offsets* into
//! the same transaction with `send_offsets_to_transaction`. Because the output
//! records and the consumed position are one atomic unit, a crash anywhere in
//! between leaves neither half applied: the transaction aborts, the offsets stay
//! where they were, and the partial output is never visible to a `read_committed`
//! reader.
//!
//! `transactional.id` is what makes that safe across restarts — zombie fencing.
//! `init_transactions` bumps the producer epoch registered for that id, so any
//! older instance still holding the id (a "zombie" that was partitioned off
//! rather than stopped) is fenced: its writes and its offset commits are rejected
//! with `INVALID_PRODUCER_EPOCH`, and its in-flight transaction is aborted. The
//! id must therefore be *stable per logical task* and never shared by two
//! instances that are meant to run concurrently.
//!
//! Run against a local Kafka broker, producing some input first:
//!
//! ```text
//! cargo run -p kacrab-examples --example producer
//! cargo run -p kacrab-examples --example transactions
//! ```
//!
//! Optional positional arguments (bootstrap, input topic, output topic, group,
//! messages):
//!
//! ```text
//! cargo run -p kacrab-examples --example transactions -- \
//!   127.0.0.1:9092 kacrab-example kacrab-example-out kacrab-eos-group 12
//! ```
//!
//! The example finishes by aborting a second transaction and re-reading the
//! output topic with a `read_committed` consumer, printing whether the committed
//! records are visible and the aborted one is not.

use std::{
    collections::HashMap,
    env,
    error::Error,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use kacrab::{
    common::{OffsetAndMetadata, TopicPartition},
    consumer::{Consumer, ConsumerRecord},
    producer::{Producer, ProducerRecord},
};

/// Boxed error, `Send + Sync` rather than the bare `Box<dyn Error>` the smaller
/// examples use: the helpers below hold an error across an `await`, and a
/// non-`Send` error would make their futures non-`Send`.
type ExampleResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

/// Stable per logical task — see the zombie-fencing note above.
const TRANSACTIONAL_ID: &str = "kacrab-example-eos";
const CONSUMER_CLIENT_ID: &str = "kacrab-example-eos-consumer";
const PRODUCER_CLIENT_ID: &str = "kacrab-example-eos-producer";
/// Only records from committed transactions are returned to the application.
const ISOLATION_LEVEL: &str = "read_committed";
const POLL_TIMEOUT: Duration = Duration::from_secs(2);
const POLL_BUDGET: Duration = Duration::from_secs(30);

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExampleResult<()> {
    let args = ExampleArgs::parse(env::args().skip(1))?;
    // Tags this run's output so the visibility check below ignores records left
    // behind by earlier runs of the example.
    let run_id = run_id()?;
    println!(
        "transactions example: bootstrap={} in={} out={} group={} messages={} run={run_id}",
        args.bootstrap, args.input_topic, args.output_topic, args.group, args.messages
    );

    let mut consumer = build_consumer(&args).await?;
    consumer.subscribe([args.input_topic.clone()])?;

    let producer = build_producer(&args.bootstrap).await?;
    // Registers `transactional.id` with the coordinator, bumps the epoch (fencing
    // any zombie holding the same id), and aborts whatever that epoch left open.
    producer.init_transactions().await?;

    let batch = poll_input(&mut consumer, args.messages).await?;
    println!("polled {} input record(s)", batch.len());

    let committed = transform_and_commit(&producer, &consumer, &args, &run_id, &batch)
        .await
        .inspect_err(|error| eprintln!("transaction failed: {error}"))?;
    let aborted = write_and_abort(&producer, &args, &run_id).await?;

    producer.close().await?;
    consumer.close().await;

    verify_visibility(&args, &committed, &aborted).await
}

/// The happy path: everything the transaction writes — output records *and* the
/// input offsets — commits together, or the whole thing aborts.
async fn transform_and_commit(
    producer: &Producer,
    consumer: &Consumer,
    args: &ExampleArgs,
    run_id: &str,
    batch: &[ConsumerRecord],
) -> ExampleResult<Vec<String>> {
    producer.begin_transaction()?;
    match write_transformed(producer, consumer, args, run_id, batch).await {
        Ok(values) => {
            // Ends the transaction. Only now do the output records and the
            // staged input offsets become visible, together.
            producer.commit_transaction().await?;
            println!("commit_transaction: {} record(s) committed", values.len());
            Ok(values)
        },
        Err(error) => {
            // The abort path: drop the buffered writes and the staged offsets, so
            // the input is re-read from the last committed position next time.
            if let Err(abort_error) = producer.abort_transaction().await {
                eprintln!("abort_transaction also failed: {abort_error}");
            }
            Err(error)
        },
    }
}

/// Produce the transformed records and stage the consumed offsets, all inside
/// the transaction opened by the caller.
async fn write_transformed(
    producer: &Producer,
    consumer: &Consumer,
    args: &ExampleArgs,
    run_id: &str,
    batch: &[ConsumerRecord],
) -> ExampleResult<Vec<String>> {
    let mut values = Vec::with_capacity(batch.len());
    let mut deliveries = Vec::with_capacity(batch.len());
    for record in batch {
        let value = format!("{run_id}|committed|{}", text(record.value.as_ref()));
        deliveries.push(
            producer.send(
                ProducerRecord::new(args.output_topic.clone(), 0)
                    .key(record.offset.to_string())
                    .value(value.clone()),
            )?,
        );
        values.push(value);
    }

    // Await every delivery before staging the input position: a `SendFuture` is
    // the record's delivery handle, so it is held until the broker has taken the
    // write. Only then is it true that the offsets about to be staged describe
    // work that is already in the transaction.
    for delivery in deliveries {
        let receipt = delivery.await?;
        println!(
            "  wrote {}-{} offset={}",
            receipt.topic, receipt.partition, receipt.offset
        );
    }

    // The input position, committed *by the producer* into this transaction
    // rather than by the consumer on its own. `group_metadata` carries the
    // generation and member id the coordinator fences stale members on.
    let offsets = next_offsets(batch);
    for (partition, offset) in &offsets {
        println!(
            "staging offset {}-{} -> {}",
            partition.topic, partition.partition, offset.offset
        );
    }
    producer
        .send_offsets_to_transaction(offsets, consumer.group_metadata())
        .await?;

    Ok(values)
}

/// Write one record and abort, to show that a `read_committed` reader never sees
/// it. This is the failure path of the block above, forced on purpose.
async fn write_and_abort(
    producer: &Producer,
    args: &ExampleArgs,
    run_id: &str,
) -> ExampleResult<String> {
    let value = format!("{run_id}|aborted|this-must-not-be-visible");
    producer.begin_transaction()?;
    let delivery = producer.send(
        ProducerRecord::new(args.output_topic.clone(), 0)
            .key("aborted".to_owned())
            .value(value.clone()),
    )?;
    // Awaited on purpose: the record really does reach the partition (a
    // `read_uncommitted` reader would see it), which is what makes the
    // visibility check below meaningful rather than vacuous.
    let receipt = delivery.await?;
    println!(
        "  wrote {}-{} offset={} (about to be aborted)",
        receipt.topic, receipt.partition, receipt.offset
    );
    producer.abort_transaction().await?;
    println!("abort_transaction: 1 record written then aborted");
    Ok(value)
}

/// Re-read the output topic with a `read_committed` consumer and report whether
/// the committed records are visible and the aborted one is not.
async fn verify_visibility(
    args: &ExampleArgs,
    committed: &[String],
    aborted: &str,
) -> ExampleResult<()> {
    let visible = read_output(args).await?;
    let committed_visible = committed.iter().all(|value| visible.contains(value));
    let aborted_visible = visible.iter().any(|value| value == aborted);

    let from_this_run = visible
        .iter()
        .filter(|value| committed.contains(value))
        .count();
    println!("read_committed sees {from_this_run} record(s) from this run");
    println!("  all committed records visible: {committed_visible}");
    println!("  aborted record visible:        {aborted_visible}");

    if committed_visible && !aborted_visible {
        println!("EOS check: OK");
        Ok(())
    } else {
        Err(
            "EOS check FAILED: a read_committed reader disagrees with the transaction outcome"
                .into(),
        )
    }
}

/// Read the whole output partition from offset 0 with `isolation.level =
/// read_committed`, using manual assignment so no group state is involved.
async fn read_output(args: &ExampleArgs) -> ExampleResult<Vec<String>> {
    let mut consumer = Consumer::from_map([
        ("bootstrap.servers", args.bootstrap.as_str()),
        ("client.id", "kacrab-example-eos-verifier"),
        ("group.id", format!("{}-verify", args.group).as_str()),
        ("isolation.level", ISOLATION_LEVEL),
        ("enable.auto.commit", "false"),
    ])
    .await?;
    let partition = TopicPartition::new(args.output_topic.clone(), 0);
    consumer.assign([partition.clone()])?;
    consumer.seek(&partition, 0)?;

    // Read to the log end rather than to a record count: the output topic may
    // already hold records from earlier runs, which the run id filters out.
    // Under `read_committed` this is the last *stable* offset, so it already
    // excludes the aborted transaction's records.
    let end = consumer
        .end_offsets(std::slice::from_ref(&partition))
        .await?
        .get(&partition)
        .copied()
        .ok_or("end_offsets did not report the output partition")?;
    let mut values = Vec::new();
    let deadline = Instant::now()
        .checked_add(POLL_BUDGET)
        .ok_or("poll deadline overflowed")?;
    while consumer.position(&partition).await? < end && Instant::now() < deadline {
        for record in &consumer.poll(POLL_TIMEOUT).await? {
            values.push(text(record.value.as_ref()));
        }
    }

    consumer.close().await;
    Ok(values)
}

async fn build_consumer(args: &ExampleArgs) -> ExampleResult<Consumer> {
    Consumer::from_map([
        ("bootstrap.servers", args.bootstrap.as_str()),
        ("client.id", CONSUMER_CLIENT_ID),
        ("group.id", args.group.as_str()),
        ("auto.offset.reset", "earliest"),
        // Both are required for EOS: the transaction owns the offsets, so the
        // consumer must never commit them itself, and it must only read records
        // whose own producing transaction committed.
        ("enable.auto.commit", "false"),
        ("isolation.level", ISOLATION_LEVEL),
    ])
    .await
    .map_err(Into::into)
}

async fn build_producer(bootstrap: &str) -> ExampleResult<Producer> {
    Producer::builder()
        .set("bootstrap.servers", bootstrap)
        .set("client.id", PRODUCER_CLIENT_ID)
        // `transactional.id` implies idempotence and `acks=all`; both are set
        // explicitly here so the requirement is visible.
        .set("transactional.id", TRANSACTIONAL_ID)
        .set("enable.idempotence", "true")
        .set("acks", "all")
        .set("transaction.timeout.ms", "60000")
        .build()
        .await
        .map_err(Into::into)
}

async fn poll_input(consumer: &mut Consumer, wanted: usize) -> ExampleResult<Vec<ConsumerRecord>> {
    let deadline = Instant::now()
        .checked_add(POLL_BUDGET)
        .ok_or("poll deadline overflowed")?;
    let mut batch = Vec::with_capacity(wanted);
    while batch.len() < wanted && Instant::now() < deadline {
        batch.extend(consumer.poll(POLL_TIMEOUT).await?);
    }
    if batch.is_empty() {
        return Err("no input records — run `--example producer` first".into());
    }
    Ok(batch)
}

/// The offset to resume from per partition: one past the highest record read.
fn next_offsets(batch: &[ConsumerRecord]) -> HashMap<TopicPartition, OffsetAndMetadata> {
    let mut offsets: HashMap<TopicPartition, OffsetAndMetadata> = HashMap::new();
    for record in batch {
        let next = record.offset.saturating_add(1);
        let entry = offsets
            .entry(record.topic_partition())
            .or_insert_with(|| OffsetAndMetadata::new(next));
        if entry.offset < next {
            *entry = OffsetAndMetadata::new(next);
        }
    }
    offsets
}

fn text(value: Option<&Bytes>) -> String {
    value.map_or_else(String::new, |bytes| {
        String::from_utf8_lossy(bytes).into_owned()
    })
}

fn run_id() -> ExampleResult<String> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_millis()
        .to_string())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ExampleArgs {
    bootstrap: String,
    input_topic: String,
    output_topic: String,
    group: String,
    messages: usize,
}

impl ExampleArgs {
    fn parse(args: impl IntoIterator<Item = String>) -> ExampleResult<Self> {
        let mut args = args.into_iter();
        let bootstrap = args.next().unwrap_or_else(|| "127.0.0.1:9092".to_owned());
        let input_topic = args.next().unwrap_or_else(|| "kacrab-example".to_owned());
        let output_topic = args
            .next()
            .unwrap_or_else(|| "kacrab-example-out".to_owned());
        let group = args.next().unwrap_or_else(|| "kacrab-eos-group".to_owned());
        let messages = match args.next() {
            Some(value) => value.parse()?,
            None => 12,
        };
        Ok(Self {
            bootstrap,
            input_topic,
            output_topic,
            group,
            messages,
        })
    }
}
