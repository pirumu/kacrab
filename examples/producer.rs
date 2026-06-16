//! Producer example covering the common public API paths.
//!
//! Run against a local Kafka broker:
//!
//! ```text
//! cargo run -p kacrab-examples --example producer
//! ```
//!
//! Optional positional arguments:
//!
//! ```text
//! cargo run -p kacrab-examples --example producer -- \
//!   127.0.0.1:9092 kacrab-example 0 10
//! ```

use std::{env, error::Error};

use kacrab::producer::{Delivery, KafkaProducer, ProduceReceipt, ProducerRecord};

const CLIENT_ID: &str = "kacrab-example-producer";
const ACKS: &str = "all";
const ENABLE_IDEMPOTENCE: &str = "true";
const RETRIES: &str = "3";
const MAX_IN_FLIGHT: &str = "5";
const BATCH_SIZE: &str = "16384";
const LINGER_MS: &str = "5";
const BUFFER_MEMORY: &str = "33554432";
const COMPRESSION_TYPE: &str = "none";
const REQUEST_TIMEOUT_MS: &str = "30000";
const DELIVERY_TIMEOUT_MS: &str = "120000";

// Set this to `Some("your-transactional-id")` to run the same writes inside a
// transaction. Leave it as `None` for the normal idempotent producer path.
const TRANSACTIONAL_ID: Option<&str> = None;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = ExampleArgs::parse(env::args().skip(1))?;
    let mut producer = build_producer(&args.bootstrap).await?;

    if TRANSACTIONAL_ID.is_some() {
        producer.init_transactions().await?;
        producer.begin_transaction()?;
    }

    let deliveries = match write_records(&mut producer, &args).await {
        Ok(deliveries) => deliveries,
        Err(error) => {
            abort_transaction_if_open(&producer).await;
            return Err(error);
        },
    };

    if TRANSACTIONAL_ID.is_some() {
        producer.commit_transaction().await?;
    } else {
        producer.flush().await?;
    }

    for delivery in deliveries {
        print_receipt(&delivery.await?);
    }

    producer.close().await?;
    Ok(())
}

async fn build_producer(bootstrap: &str) -> Result<KafkaProducer, Box<dyn Error>> {
    let mut builder = KafkaProducer::builder()
        .set("bootstrap.servers", bootstrap)
        .set("client.id", CLIENT_ID)
        .set("acks", ACKS)
        .set("enable.idempotence", ENABLE_IDEMPOTENCE)
        .set("retries", RETRIES)
        .set("max.in.flight.requests.per.connection", MAX_IN_FLIGHT)
        .set("batch.size", BATCH_SIZE)
        .set("linger.ms", LINGER_MS)
        .set("buffer.memory", BUFFER_MEMORY)
        .set("compression.type", COMPRESSION_TYPE)
        .set("request.timeout.ms", REQUEST_TIMEOUT_MS)
        .set("delivery.timeout.ms", DELIVERY_TIMEOUT_MS);

    if let Some(transactional_id) = TRANSACTIONAL_ID {
        builder = builder.set("transactional.id", transactional_id);
    }

    // TLS, SASL, Kerberos, and custom auth use the same builder:
    // builder = builder
    //     .set("security.protocol", "SASL_SSL")
    //     .set("sasl.mechanism", "PLAIN")
    //     .set("sasl.jaas.config", "...");

    builder.build().await.map_err(Into::into)
}

async fn write_records(
    producer: &mut KafkaProducer,
    args: &ExampleArgs,
) -> Result<Vec<Delivery>, Box<dyn Error>> {
    let mut deliveries = Vec::with_capacity(args.messages.saturating_add(1));

    let first = producer
        .send(record(&args.topic, args.partition, 0, "single-send"))
        .await?;
    deliveries.push(first);

    let tracked_records = (1..=args.messages)
        .map(|sequence| record(&args.topic, args.partition, sequence, "tracked-send-batch"));
    deliveries.extend(producer.send_batch(tracked_records).await?);

    let untracked_records = (0..2).map(|sequence| {
        record(
            &args.topic,
            args.partition,
            sequence,
            "untracked-send-batch",
        )
    });
    producer.send_batch_untracked(untracked_records).await?;

    Ok(deliveries)
}

fn record(topic: &str, partition: i32, sequence: usize, prefix: &str) -> ProducerRecord {
    ProducerRecord::new(topic.to_owned(), partition)
        .key(sequence.to_string())
        .value(format!("{prefix}-{sequence}"))
}

async fn abort_transaction_if_open(producer: &KafkaProducer) {
    if TRANSACTIONAL_ID.is_none() {
        return;
    }
    if let Err(error) = producer.abort_transaction().await {
        eprintln!("failed to abort transaction after produce error: {error}");
    }
}

fn print_receipt(receipt: &ProduceReceipt) {
    println!(
        "topic={} partition={} leader={} offset={} log_append_time_ms={}",
        receipt.topic,
        receipt.partition,
        receipt.leader_id,
        receipt.base_offset,
        receipt.log_append_time_ms
    );
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ExampleArgs {
    bootstrap: String,
    topic: String,
    partition: i32,
    messages: usize,
}

impl ExampleArgs {
    fn parse(args: impl IntoIterator<Item = String>) -> Result<Self, Box<dyn Error>> {
        let mut args = args.into_iter();
        let bootstrap = args.next().unwrap_or_else(|| "127.0.0.1:9092".to_owned());
        let topic = args.next().unwrap_or_else(|| "kacrab-example".to_owned());
        let partition = match args.next() {
            Some(value) => value.parse()?,
            None => 0,
        };
        let messages = match args.next() {
            Some(value) => value.parse()?,
            None => 10,
        };
        Ok(Self {
            bootstrap,
            topic,
            partition,
            messages,
        })
    }
}
