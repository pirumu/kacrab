//! Share consumer example (KIP-932): join a share group, poll, acknowledge.
//!
//! A share group is Kafka's queue-shaped consuming surface. Members do not own
//! partitions: the broker *acquires* individual records for whoever polls, holds
//! an acquisition lock on them, and expects an acknowledgement. So more consumers
//! than partitions all make progress, and a record can be handed back for another
//! attempt instead of blocking the partition behind it.
//!
//! Requires **Apache Kafka 4.3+** (share groups are a production feature from
//! 4.2 onward — `ShareVersion.LATEST_PRODUCTION = SV_1` — so a 4.3 broker has
//! them on by default; against an older broker the first heartbeat comes back
//! `UNSUPPORTED_VERSION`). `docker-compose.kafka.yml` pins 4.3.0.
//!
//! Acknowledgement modes: **implicit** (the default) accepts every polled record
//! automatically on the next poll or `commit`, while **explicit** — used below —
//! requires a per-record `accept` / `release` / `reject`, which is what lets a
//! failed record be retried or dropped instead of silently counted as done.
//!
//! A share group initialises its start offset at the **log end**, so records
//! produced before the group's first poll are not delivered. Either set the
//! group config first (one terminal):
//!
//! ```text
//! docker exec kacrab-kafka /opt/kafka/bin/kafka-configs.sh \
//!   --bootstrap-server localhost:9092 --alter \
//!   --entity-type groups --entity-name kacrab-share-example-group \
//!   --add-config share.auto.offset.reset=earliest
//! cargo run -p kacrab-examples --example producer -- 127.0.0.1:9092 kacrab-share-example
//! cargo run -p kacrab-examples --example share_consumer
//! ```
//!
//! (`AdminClient::incremental_alter_configs` on a `ConfigResource` with
//! `ResourceType::Group` does the same thing from Rust.) Or start this example
//! first and produce into the topic while it polls.
//!
//! Optional positional arguments (bootstrap, topic, group, messages):
//!
//! ```text
//! cargo run -p kacrab-examples --example share_consumer -- \
//!   127.0.0.1:9092 kacrab-share-example kacrab-share-example-group 12
//! ```

use std::{
    env,
    error::Error,
    time::{Duration, Instant},
};

use bytes::Bytes;
use kacrab::consumer::{AcknowledgeType, ShareConsumer, ShareRecord};

const CLIENT_ID: &str = "kacrab-example-share-consumer";
/// `explicit` makes every acknowledgement a deliberate call; `implicit` (the
/// default) accepts each polled record on the next poll or `commit`.
const ACKNOWLEDGEMENT_MODE: &str = "explicit";
/// How long one `poll` waits for the broker to acquire records.
const POLL_TIMEOUT: Duration = Duration::from_millis(500);
/// Total time the loop spends waiting for `messages` records.
const POLL_BUDGET: Duration = Duration::from_secs(30);

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = ExampleArgs::parse(env::args().skip(1))?;
    println!(
        "share consumer example: bootstrap={} topic={} group={} messages={}",
        args.bootstrap, args.topic, args.group, args.messages
    );

    let mut consumer = ShareConsumer::from_map([
        ("bootstrap.servers", args.bootstrap.as_str()),
        ("client.id", CLIENT_ID),
        ("group.id", args.group.as_str()),
        ("share.acknowledgement.mode", ACKNOWLEDGEMENT_MODE),
    ])
    .await?;

    // Joining the group is a `ShareGroupHeartbeat`; the assignment arrives with
    // it, and is *not* exclusive — other members can hold the same partition.
    consumer.subscribe([args.topic.clone()])?;
    println!("subscription: {:?}", consumer.subscription());

    let accepted = consume(&mut consumer, args.messages).await?;

    // `commit` flushes the acknowledgements gathered above. Without it, records
    // that were accepted client-side stay locked at the broker until the
    // acquisition lock expires — and are then redelivered.
    consumer.commit().await?;
    if let Some(lock) = consumer.acquisition_lock_timeout() {
        println!("broker acquisition lock budget: {lock:?}");
    }
    let assignment: Vec<String> = consumer
        .assignment()
        .iter()
        .map(|partition| format!("{}-{}", partition.topic, partition.partition))
        .collect();
    println!("assignment: {assignment:?}");

    // `close` acknowledges anything still pending and closes the share sessions,
    // releasing what was not acknowledged immediately instead of at lock expiry.
    consumer.close().await;
    println!("accepted {accepted} record(s) — done");
    Ok(())
}

/// Poll until `wanted` records have been acquired or [`POLL_BUDGET`] runs out,
/// accepting each one. Returns how many were accepted.
async fn consume(consumer: &mut ShareConsumer, wanted: usize) -> Result<usize, Box<dyn Error>> {
    let deadline = Instant::now()
        .checked_add(POLL_BUDGET)
        .ok_or("poll deadline overflowed")?;
    let mut accepted = 0_usize;

    while accepted < wanted && Instant::now() < deadline {
        let records = consumer.poll(POLL_TIMEOUT).await?;
        for record in &records {
            print_record(record);
            // The three dispositions, all through the same call:
            //   `Accept`  — done, retire the record (`accept` is the shorthand).
            //   `Release` — hand it back for another attempt; the redelivery
            //               arrives with `delivery_count` bumped by one.
            //   `Reject`  — archive it without redelivery (poison message).
            // A record released until it hits the broker's
            // `group.share.delivery.count.limit` is archived automatically.
            consumer.acknowledge(record, AcknowledgeType::Accept)?;
            accepted = accepted.saturating_add(1);
        }
    }

    Ok(accepted)
}

fn print_record(record: &ShareRecord) {
    println!(
        "  {}-{} offset={} delivery_count={} value={:?}",
        record.record.topic,
        record.record.partition,
        record.offset(),
        record.delivery_count,
        record.record.value.as_ref().map(text)
    );
}

/// Record payloads are raw bytes; decode them for display only.
fn text(bytes: &Bytes) -> String {
    String::from_utf8_lossy(bytes).into_owned()
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
        let topic = args
            .next()
            .unwrap_or_else(|| "kacrab-share-example".to_owned());
        let group = args
            .next()
            .unwrap_or_else(|| "kacrab-share-example-group".to_owned());
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
