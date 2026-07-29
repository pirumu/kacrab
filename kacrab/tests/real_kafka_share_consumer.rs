//! Real Kafka share consumer integration test (KIP-932).
//!
//! Share groups are the one Kafka 4.x surface where nothing can be verified
//! without a broker: the acquisition lock, the delivery count, the redelivery of
//! a released record, and the fact that more consumers than partitions actually
//! make progress all live broker-side. These tests run against the Apache Kafka
//! 4.3.0 broker from `docker-compose.kafka.yml`, whose `share.version` feature
//! finalizes at `1` (`ShareVersion.LATEST_PRODUCTION = SV_1`), so share groups
//! are on without any extra fixture. Run:
//! `cargo test --features producer,share-consumer,admin --test
//! real_kafka_share_consumer -- --ignored --test-threads=1 --nocapture`.
//!
//! Every test sets `share.auto.offset.reset=earliest` on its own group before
//! the group exists: a share group otherwise initializes its share-partition
//! start offset at the log end, and would never see records produced before the
//! first poll.

#![allow(
    clippy::arithmetic_side_effects,
    clippy::expect_used,
    clippy::print_stdout,
    clippy::unwrap_used,
    reason = "Ignored real-broker test is an explicit smoke check with direct failure output; \
              arithmetic is deadline/counter bookkeeping on small bounded values. (The in-test \
              relaxation clippy applies on its own stops working in functions that expand the \
              `require_broker_api!` guard, so the allowance is spelled out.)"
)]

use std::{
    collections::{BTreeSet, HashMap},
    env,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use kacrab::{
    admin::{
        AdminClient, AlterConfigOp, AlterConfigsOptions, ConfigResource, CreateTopicsOptions,
        NewTopic, ResourceType,
    },
    common::TopicPartition,
    consumer::{AcknowledgeType, ShareConsumer, ShareRecord, ShareRecords},
    producer::{Producer, ProducerRecord},
};
use kacrab_protocol::generated::ApiKey;

mod common;

/// Every test in this suite drives the same three KIP-932 APIs — membership,
/// the acquiring fetch, and the acknowledgement — none of which a broker below
/// 4.2 advertises (the CI matrix first sees them on 4.3.0, at `min_version` 1).
macro_rules! require_share_group_apis {
    () => {
        common::require_broker_api!(
            ApiKey::ShareGroupHeartbeat => 1,
            ApiKey::ShareFetch => 1,
            ApiKey::ShareAcknowledge => 1,
        );
    };
}

/// The broker's `group.share.delivery.count.limit` default: a record is archived
/// once it has been delivered this many times.
const DELIVERY_COUNT_LIMIT: i16 = 5;

/// The shortest per-group record lock the broker accepts
/// (`group.share.min.record.lock.duration.ms` defaults to 15 s).
const MIN_RECORD_LOCK_MS: &str = "15000";

/// Implicit acknowledgement, end to end: every polled record is accepted on the
/// next poll/commit, so a second consumer in the same group sees nothing. Also
/// checks that the admin share-group surface describes the live group.
#[tokio::test]
#[ignore = "requires local Kafka from docker-compose.kafka.yml"]
async fn real_kafka_share_consumer_implicit_acknowledgement() {
    require_share_group_apis!();
    let bootstrap = bootstrap();
    let topic = topic("implicit");
    let group = format!("share-{topic}");
    println!("share consumer implicit smoke: bootstrap={bootstrap}, topic={topic}");

    let admin = admin(&bootstrap).await;
    create_topic(&admin, &topic, 1).await;
    set_group_config(&admin, &group, &[("share.auto.offset.reset", "earliest")]).await;
    produce(&bootstrap, &topic, 0, 10).await;

    let mut consumer = share_consumer(&bootstrap, &group, &[]).await;
    consumer.subscribe([topic.clone()]).expect("subscribe");
    assert_eq!(consumer.subscription(), vec![topic.clone()]);

    let acquired = poll_until(&mut consumer, 10, Duration::from_secs(45)).await;
    assert_eq!(
        acquired.len(),
        10,
        "every produced record should be acquired"
    );
    for record in &acquired {
        assert_eq!(
            record.delivery_count, 1,
            "a first delivery has delivery count 1"
        );
    }
    assert_eq!(
        values(&acquired),
        (0..10).map(|index| format!("v{index}")).collect::<Vec<_>>()
    );

    // The assignment covers the topic's partition even though nothing was
    // "owned" in the consumer-group sense.
    assert_eq!(
        consumer.assignment(),
        vec![TopicPartition::new(topic.clone(), 0)]
    );

    // The broker reports how long it holds the acquisition lock; the client only
    // learns it from a response that acquired something.
    let lock_timeout = consumer
        .acquisition_lock_timeout()
        .expect("the broker reports the acquisition lock budget with acquired records");
    println!("  acquisition lock timeout: {lock_timeout:?}");
    assert!(
        lock_timeout >= Duration::from_secs(1),
        "an acquisition lock shorter than a second would be a broker bug"
    );

    // Client telemetry id, like `Consumer::client_instance_id`. The broker may
    // have telemetry disabled, which is a legitimate answer rather than a
    // failure — assert only that the call round-trips one way or the other.
    match consumer.client_instance_id().await {
        Ok(id) => println!("  client instance id: {id}"),
        Err(error) => println!("  client telemetry unavailable: {error}"),
    }

    // Admin interop against the live group.
    let described = admin
        .describe_share_groups(vec![group.clone()])
        .await
        .expect("describe_share_groups");
    assert_eq!(described.len(), 1);
    assert_eq!(described[0].group_id, group);
    assert!(
        !described[0].members.is_empty(),
        "the live member should be listed"
    );

    // `commit` flushes the implicit accepts; `close` then leaves the group.
    consumer.commit().await.expect("commit");
    consumer.close().await;

    // The admin surface reads the same live group back. Only the shape is
    // asserted: the persisted share-partition start offset advances on the
    // coordinator's own schedule (the Java `kafka-share-groups.sh --offsets`
    // CLI reports the same value at the same moment), so pinning it to a number
    // here would be asserting broker timing, not kacrab behaviour. That the
    // records really were accepted is what the second consumer below proves.
    let partition = TopicPartition::new(topic.clone(), 0);
    let offsets = admin
        .list_share_group_offsets(&group, vec![partition.clone()])
        .await
        .expect("list_share_group_offsets");
    println!("  share group offsets: {offsets:?}");
    let start = offsets
        .iter()
        .find(|entry| entry.partition == partition)
        .expect("the consumed partition should be reported");
    assert!(
        start.offset.offset >= 0,
        "the share-partition start offset should be readable"
    );

    // A fresh member of the same group sees nothing: the records were accepted.
    let mut second = share_consumer(&bootstrap, &group, &[]).await;
    second.subscribe([topic.clone()]).expect("subscribe");
    let redelivered = poll_until(&mut second, 1, Duration::from_secs(10)).await;
    assert!(
        redelivered.is_empty(),
        "accepted records must not be redelivered, got {:?}",
        values(&redelivered)
    );
    second.close().await;
    println!("share consumer implicit smoke: ALL OK");
}

/// Explicit acknowledgement with all three dispositions: `Accept` retires a
/// record, `Release` hands it back for another delivery attempt (with a bumped
/// delivery count), and `Reject` archives it without redelivery.
#[tokio::test]
#[ignore = "requires local Kafka from docker-compose.kafka.yml"]
async fn real_kafka_share_consumer_accept_release_reject() {
    require_share_group_apis!();
    let bootstrap = bootstrap();
    let topic = topic("dispositions");
    let group = format!("share-{topic}");
    println!("share consumer dispositions smoke: bootstrap={bootstrap}, topic={topic}");

    let admin = admin(&bootstrap).await;
    create_topic(&admin, &topic, 1).await;
    set_group_config(&admin, &group, &[("share.auto.offset.reset", "earliest")]).await;
    produce(&bootstrap, &topic, 0, 3).await;

    let mut consumer = share_consumer(
        &bootstrap,
        &group,
        &[("share.acknowledgement.mode", "explicit")],
    )
    .await;
    consumer.subscribe([topic.clone()]).expect("subscribe");

    let acquired = poll_until(&mut consumer, 3, Duration::from_secs(45)).await;
    assert_eq!(acquired.len(), 3);
    // `accept` is the one-argument form; the other two go through `acknowledge`.
    consumer.accept(&acquired[0]).expect("accept");
    for (record, disposition) in acquired[1..]
        .iter()
        .zip([AcknowledgeType::Release, AcknowledgeType::Reject])
    {
        println!(
            "  offset={} value={:?} -> {disposition:?}",
            record.offset(),
            value_of(record)
        );
        consumer
            .acknowledge(record, disposition)
            .expect("acknowledge");
    }
    consumer.commit().await.expect("commit");

    // Only the released record comes back, on its second delivery attempt.
    let redelivered = poll_until(&mut consumer, 1, Duration::from_secs(30)).await;
    assert_eq!(
        values(&redelivered),
        vec!["v1".to_owned()],
        "only the Released record is redelivered"
    );
    assert_eq!(
        redelivered[0].delivery_count, 2,
        "a redelivered record reports its second delivery"
    );

    consumer
        .acknowledge(&redelivered[0], AcknowledgeType::Accept)
        .expect("acknowledge");
    consumer.commit().await.expect("commit");

    let settled = poll_until(&mut consumer, 1, Duration::from_secs(10)).await;
    assert!(
        settled.is_empty(),
        "nothing is left once every record is accepted or rejected, got {:?}",
        values(&settled)
    );
    consumer.close().await;
    println!("share consumer dispositions smoke: ALL OK");
}

/// Delivery counts climb across redeliveries until the broker's
/// `group.share.delivery.count.limit` archives the record — the poison-message
/// path a consumer group cannot express.
#[tokio::test]
#[ignore = "requires local Kafka from docker-compose.kafka.yml"]
async fn real_kafka_share_consumer_delivery_count_reaches_the_limit() {
    require_share_group_apis!();
    let bootstrap = bootstrap();
    let topic = topic("poison");
    let group = format!("share-{topic}");
    println!("share consumer delivery-count smoke: bootstrap={bootstrap}, topic={topic}");

    let admin = admin(&bootstrap).await;
    create_topic(&admin, &topic, 1).await;
    set_group_config(&admin, &group, &[("share.auto.offset.reset", "earliest")]).await;
    produce(&bootstrap, &topic, 0, 1).await;

    let mut consumer = share_consumer(
        &bootstrap,
        &group,
        &[("share.acknowledgement.mode", "explicit")],
    )
    .await;
    consumer.subscribe([topic.clone()]).expect("subscribe");

    let mut seen_counts = Vec::new();
    for _attempt in 0..=DELIVERY_COUNT_LIMIT {
        let acquired = poll_until(&mut consumer, 1, Duration::from_secs(30)).await;
        let Some(record) = acquired.first() else {
            break;
        };
        seen_counts.push(record.delivery_count);
        consumer
            .acknowledge(record, AcknowledgeType::Release)
            .expect("acknowledge");
        consumer.commit().await.expect("commit");
    }

    println!("  delivery counts seen: {seen_counts:?}");
    assert_eq!(
        seen_counts,
        (1..=DELIVERY_COUNT_LIMIT).collect::<Vec<_>>(),
        "each Release bumps the delivery count until the broker archives the record"
    );
    consumer.close().await;
    println!("share consumer delivery-count smoke: ALL OK");
}

/// Three consumers on a one-partition topic — the case a consumer group cannot
/// serve. Every record is delivered exactly once across the group.
#[tokio::test]
#[ignore = "requires local Kafka from docker-compose.kafka.yml"]
async fn real_kafka_share_consumer_more_consumers_than_partitions() {
    require_share_group_apis!();
    let bootstrap = bootstrap();
    let topic = topic("fanout");
    let group = format!("share-{topic}");
    let total = 30;
    println!("share consumer fan-out smoke: bootstrap={bootstrap}, topic={topic}");

    let admin = admin(&bootstrap).await;
    create_topic(&admin, &topic, 1).await;
    set_group_config(&admin, &group, &[("share.auto.offset.reset", "earliest")]).await;
    produce(&bootstrap, &topic, 0, total).await;

    let mut consumers = Vec::new();
    for index in 0..3 {
        let mut consumer = share_consumer(
            &bootstrap,
            &group,
            &[("client.id", &format!("kacrab-share-{index}"))],
        )
        .await;
        consumer.subscribe([topic.clone()]).expect("subscribe");
        consumers.push(consumer);
    }

    let mut offsets: Vec<i64> = Vec::new();
    let deadline = Instant::now() + Duration::from_mins(1);
    while offsets.len() < total && Instant::now() < deadline {
        for consumer in &mut consumers {
            let records = consumer
                .poll(Duration::from_millis(500))
                .await
                .expect("poll");
            offsets.extend(records.iter().map(ShareRecord::offset));
        }
    }
    // Every member must flush its implicit accepts before the group is torn down.
    for consumer in &mut consumers {
        consumer.commit().await.expect("commit");
    }

    let unique: BTreeSet<i64> = offsets.iter().copied().collect();
    println!(
        "  {} records across 3 consumers on 1 partition ({} unique)",
        offsets.len(),
        unique.len()
    );
    assert_eq!(
        offsets.len(),
        total,
        "the group should consume every record exactly once"
    );
    assert_eq!(unique.len(), total, "no record should be delivered twice");

    for consumer in consumers {
        consumer.close().await;
    }
    println!("share consumer fan-out smoke: ALL OK");
}

/// A consumer that goes away without acknowledging leaves its records locked;
/// once the acquisition lock expires they are redelivered rather than lost.
#[tokio::test]
#[ignore = "requires local Kafka from docker-compose.kafka.yml"]
async fn real_kafka_share_consumer_lock_expiry_redelivers() {
    require_share_group_apis!();
    let bootstrap = bootstrap();
    let topic = topic("lock-expiry");
    let group = format!("share-{topic}");
    println!("share consumer lock-expiry smoke: bootstrap={bootstrap}, topic={topic}");

    let admin = admin(&bootstrap).await;
    create_topic(&admin, &topic, 1).await;
    set_group_config(
        &admin,
        &group,
        &[
            ("share.auto.offset.reset", "earliest"),
            ("share.record.lock.duration.ms", MIN_RECORD_LOCK_MS),
        ],
    )
    .await;
    produce(&bootstrap, &topic, 0, 2).await;

    let mut abandoned = share_consumer(
        &bootstrap,
        &group,
        &[("share.acknowledgement.mode", "explicit")],
    )
    .await;
    abandoned.subscribe([topic.clone()]).expect("subscribe");
    let acquired = poll_until(&mut abandoned, 2, Duration::from_secs(45)).await;
    assert_eq!(acquired.len(), 2, "both records should be acquired first");

    // Dropped, not closed: nothing is acknowledged and no session is closed, so
    // the records stay locked until the acquisition lock expires.
    drop(abandoned);

    let mut successor = share_consumer(&bootstrap, &group, &[]).await;
    successor.subscribe([topic.clone()]).expect("subscribe");
    let redelivered = poll_until(&mut successor, 2, Duration::from_secs(90)).await;
    println!(
        "  redelivered {:?} with delivery counts {:?}",
        values(&redelivered),
        redelivered
            .iter()
            .map(|record| record.delivery_count)
            .collect::<Vec<_>>()
    );
    assert_eq!(
        redelivered.len(),
        2,
        "records abandoned under lock must come back once the lock expires"
    );
    for record in &redelivered {
        assert!(
            record.delivery_count >= 2,
            "a redelivery after lock expiry reports at least a second attempt"
        );
    }
    successor.commit().await.expect("commit");
    successor.close().await;
    println!("share consumer lock-expiry smoke: ALL OK");
}

// --- helpers ---

async fn admin(bootstrap: &str) -> AdminClient {
    AdminClient::from_map([("bootstrap.servers", bootstrap)])
        .await
        .expect("admin should connect to local Kafka")
}

async fn create_topic(admin: &AdminClient, topic: &str, partitions: i32) {
    admin
        .create_topics(
            vec![NewTopic::new(topic.to_owned(), partitions, 1)],
            CreateTopicsOptions::default(),
        )
        .await
        .expect("create_topics should succeed");
}

/// Set dynamic group configs before the group exists. A share group reads them
/// when it initializes its share-partition state.
async fn set_group_config(admin: &AdminClient, group: &str, entries: &[(&str, &str)]) {
    let ops = entries
        .iter()
        .map(|(key, value)| AlterConfigOp::set(*key, *value))
        .collect();
    admin
        .incremental_alter_configs(
            vec![(
                ConfigResource {
                    resource_type: ResourceType::Group,
                    name: group.to_owned(),
                },
                ops,
            )],
            AlterConfigsOptions::default(),
        )
        .await
        .expect("incremental_alter_configs should set the group config");
}

async fn produce(bootstrap: &str, topic: &str, partition: i32, count: usize) {
    let producer = Producer::builder()
        .set("bootstrap.servers", bootstrap.to_owned())
        .set("client.id", "kacrab-share-consumer-producer")
        .set("enable.idempotence", "true")
        .set("acks", "all")
        .set("batch.size", "1")
        .build()
        .await
        .expect("producer should connect to local Kafka");
    for index in 0..count {
        let record = ProducerRecord::new(topic.to_owned(), partition)
            .key(Bytes::from(format!("k{index}")))
            .value(Bytes::from(format!("v{index}")));
        let delivery = producer.send(record).expect("send should enqueue");
        let _receipt = delivery.await.expect("delivery should complete");
    }
    let _closed = producer.close().await;
}

async fn share_consumer(bootstrap: &str, group: &str, extra: &[(&str, &str)]) -> ShareConsumer {
    let mut entries: HashMap<String, String> = HashMap::from([
        ("bootstrap.servers".to_owned(), bootstrap.to_owned()),
        ("group.id".to_owned(), group.to_owned()),
        ("client.id".to_owned(), "kacrab-share-consumer".to_owned()),
    ]);
    for (key, value) in extra {
        let _previous = entries.insert((*key).to_owned(), (*value).to_owned());
    }
    ShareConsumer::from_map(entries)
        .await
        .expect("share consumer should connect to local Kafka")
}

/// Poll until `want` records have been acquired or the deadline passes. Returns
/// everything acquired, which may be fewer than `want` — the callers assert on
/// the count themselves, including the "expect nothing" cases.
async fn poll_until(
    consumer: &mut ShareConsumer,
    want: usize,
    budget: Duration,
) -> Vec<ShareRecord> {
    let deadline = Instant::now()
        .checked_add(budget)
        .expect("deadline should not overflow");
    let mut acquired: Vec<ShareRecord> = Vec::new();
    while acquired.len() < want && Instant::now() < deadline {
        let records: ShareRecords = consumer
            .poll(Duration::from_millis(500))
            .await
            .expect("poll should succeed");
        acquired.extend(records);
    }
    acquired
}

fn values(records: &[ShareRecord]) -> Vec<String> {
    records.iter().filter_map(value_of).collect()
}

fn value_of(record: &ShareRecord) -> Option<String> {
    record
        .record
        .value
        .as_ref()
        .map(|bytes| String::from_utf8(bytes.to_vec()).expect("record payload should be utf-8"))
}

fn bootstrap() -> String {
    env::var("KACRAB_BOOTSTRAP").unwrap_or_else(|_error| "127.0.0.1:9092".to_owned())
}

fn topic(kind: &str) -> String {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be after epoch")
        .as_millis();
    format!("kacrab-share-{kind}-{nonce}")
}
