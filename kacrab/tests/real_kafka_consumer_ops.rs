//! Real Kafka consumer operations integration tests.
//!
//! Covers the consumer surface the base `real_kafka_consumer` suite does not:
//! group-less manual assignment, exact `seek`/`seek_to_beginning`/`seek_to_end`
//! positioning, `pause`/`resume` gating, `offsets_for_times` against known
//! producer timestamps, record headers on the consume side, static membership
//! (`group.instance.id`), `auto.offset.reset=latest` on a fresh group, and
//! `enforce_rebalance`. Runs against a real Apache Kafka 4.3.0 broker from
//! `docker-compose.kafka.yml` (broker auto topic creation disabled). Run:
//! `cargo test --features producer,consumer,admin --test real_kafka_consumer_ops
//! -- --ignored --nocapture`.

#![allow(
    clippy::expect_used,
    clippy::print_stdout,
    clippy::unwrap_used,
    reason = "Ignored real-broker test is an explicit smoke check with direct failure output."
)]

use std::{
    collections::HashMap,
    env, process,
    sync::atomic::{AtomicUsize, Ordering},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use kacrab::{
    admin::{AdminClient, CreateTopicsOptions, NewTopic},
    common::TopicPartition,
    consumer::{Consumer, ConsumerError, RecordHeader, TimestampType},
    producer::{Producer, ProducerRecord},
};

fn bootstrap() -> String {
    env::var("KACRAB_BOOTSTRAP").unwrap_or_else(|_error| "127.0.0.1:9092".to_owned())
}

/// Unique per-test resource name: pid + in-process counter + wall clock, so
/// parallel suites and repeated runs never collide on topics or groups.
fn unique(prefix: &str) -> String {
    static COUNTER: AtomicUsize = AtomicUsize::new(0);
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be after epoch")
        .as_millis();
    let seq = COUNTER.fetch_add(1, Ordering::SeqCst);
    format!("{prefix}-{}-{seq}-{nonce}", process::id())
}

async fn create_topic(bootstrap: &str, topic: &str, partitions: i32) {
    let admin = AdminClient::from_map([("bootstrap.servers", bootstrap)])
        .await
        .expect("admin should connect");
    admin
        .create_topics(
            vec![NewTopic::new(topic.to_owned(), partitions, 1)],
            CreateTopicsOptions::default(),
        )
        .await
        .expect("create_topics should succeed");
}

async fn connect_producer(bootstrap: &str) -> Producer {
    Producer::builder()
        .set("bootstrap.servers", bootstrap.to_owned())
        .set("enable.idempotence", "true")
        .set("acks", "all")
        .set("batch.size", "1")
        .build()
        .await
        .expect("producer should connect")
}

async fn produce_values(bootstrap: &str, topic: &str, partition: i32, count: usize) {
    let producer = connect_producer(bootstrap).await;
    for i in 0..count {
        let record = ProducerRecord::new(topic.to_owned(), partition)
            .value(Bytes::from(format!("p{partition}-v{i}")));
        let _receipt = producer
            .send(record)
            .expect("send")
            .await
            .expect("delivery");
    }
}

/// Poll until `expect` records arrived (or `deadline_secs` elapsed), returning
/// every record's offset in arrival order.
async fn collect_offsets(consumer: &mut Consumer, expect: usize, deadline_secs: u64) -> Vec<i64> {
    let mut offsets = Vec::new();
    let deadline = Instant::now()
        .checked_add(Duration::from_secs(deadline_secs))
        .expect("deadline fits in an Instant");
    while offsets.len() < expect && Instant::now() < deadline {
        let records = consumer
            .poll(Duration::from_secs(1))
            .await
            .expect("poll should succeed");
        offsets.extend(records.into_iter().map(|record| record.offset));
    }
    offsets
}

/// A consumer with no `group.id` can still `assign` explicit partitions,
/// consume everything, and report a `position` past the last record — the
/// group protocol is never involved. Negative control: `subscribe` on the same
/// consumer must fail with `InvalidState`, proving no group is configured.
#[tokio::test]
#[ignore = "requires local Kafka from docker-compose.kafka.yml"]
async fn real_kafka_manual_assign_without_group() {
    let bootstrap = bootstrap();
    let topic = unique("kacrab-ops-nogroup");
    let count = 6;
    println!("real Kafka group-less assign smoke: topic={topic}");

    create_topic(&bootstrap, &topic, 1).await;
    produce_values(&bootstrap, &topic, 0, count).await;

    // No `group.id` on purpose.
    let mut consumer = Consumer::from_map([
        ("bootstrap.servers", bootstrap.as_str()),
        ("client.id", "kacrab-ops-nogroup-consumer"),
        ("auto.offset.reset", "earliest"),
        ("enable.auto.commit", "false"),
    ])
    .await
    .expect("consumer should connect");
    assert!(consumer.group_metadata().group_id.is_empty());

    // Negative control: subscribing without a group must be rejected — if this
    // ever succeeds the test would no longer prove group-less consumption.
    let error = consumer
        .subscribe([topic.clone()])
        .expect_err("subscribe without group.id must fail");
    assert!(
        matches!(error, ConsumerError::InvalidState(_)),
        "unexpected error: {error:?}"
    );

    let partition = TopicPartition::new(topic.clone(), 0);
    consumer.assign([partition.clone()]).expect("assign");
    assert_eq!(consumer.assignment(), vec![partition.clone()]);

    let offsets = collect_offsets(&mut consumer, count, 30).await;
    println!("  collected offsets={offsets:?}");
    assert_eq!(
        offsets,
        (0..i64::try_from(count).unwrap()).collect::<Vec<_>>()
    );

    let position = consumer
        .position(&partition)
        .await
        .expect("position should resolve");
    assert_eq!(usize::try_from(position).unwrap(), count);

    consumer.close().await;
    println!("real Kafka group-less assign smoke: ALL OK");
}

/// `seek` to a mid-topic offset yields exactly the records from that offset on
/// (never an earlier one), `seek_to_beginning` rewinds to offset 0, and
/// `seek_to_end` positions at the log end where a poll returns nothing.
#[tokio::test]
#[ignore = "requires local Kafka from docker-compose.kafka.yml"]
async fn real_kafka_seek_positions_are_exact() {
    let bootstrap = bootstrap();
    let topic = unique("kacrab-ops-seek");
    let count = 10_i64;
    println!("real Kafka seek smoke: topic={topic}");

    create_topic(&bootstrap, &topic, 1).await;
    produce_values(&bootstrap, &topic, 0, usize::try_from(count).unwrap()).await;

    let mut consumer = Consumer::from_map([
        ("bootstrap.servers", bootstrap.as_str()),
        ("group.id", unique("group-seek").as_str()),
        ("auto.offset.reset", "earliest"),
        ("enable.auto.commit", "false"),
    ])
    .await
    .expect("consumer should connect");
    let partition = TopicPartition::new(topic.clone(), 0);
    consumer.assign([partition.clone()]).expect("assign");

    // Mid-topic seek: exactly offsets 4..10, in order, nothing earlier. A seek
    // that was silently ignored would deliver offset 0 first and fail here.
    consumer.seek(&partition, 4).expect("seek");
    let after_seek = collect_offsets(&mut consumer, 6, 30).await;
    println!("  after seek(4): {after_seek:?}");
    assert_eq!(after_seek, (4..count).collect::<Vec<_>>());

    // Rewind to the beginning: every record again, starting at offset 0.
    consumer
        .seek_to_beginning(std::slice::from_ref(&partition))
        .await
        .expect("seek_to_beginning");
    let from_start = collect_offsets(&mut consumer, usize::try_from(count).unwrap(), 30).await;
    println!("  after seek_to_beginning: {from_start:?}");
    assert_eq!(from_start, (0..count).collect::<Vec<_>>());

    // Log end: position == count and there is nothing to fetch.
    consumer
        .seek_to_end(std::slice::from_ref(&partition))
        .await
        .expect("seek_to_end");
    let position = consumer.position(&partition).await.expect("position");
    assert_eq!(position, count, "seek_to_end lands at the log end offset");
    let empty = consumer
        .poll(Duration::from_millis(500))
        .await
        .expect("poll");
    assert!(empty.is_empty(), "no records exist past the log end");

    consumer.close().await;
    println!("real Kafka seek smoke: ALL OK");
}

/// A paused partition delivers nothing while its sibling keeps flowing —
/// including across extra polls after the sibling is drained (negative
/// control) — and `resume` delivers the paused partition's full backlog.
/// `paused()` tracks the set through both transitions.
#[tokio::test]
#[ignore = "requires local Kafka from docker-compose.kafka.yml"]
async fn real_kafka_pause_and_resume_gate_fetching() {
    let bootstrap = bootstrap();
    let topic = unique("kacrab-ops-pause");
    let per_partition = 4;
    println!("real Kafka pause/resume smoke: topic={topic}");

    create_topic(&bootstrap, &topic, 2).await;
    produce_values(&bootstrap, &topic, 0, per_partition).await;
    produce_values(&bootstrap, &topic, 1, per_partition).await;

    let mut consumer = Consumer::from_map([
        ("bootstrap.servers", bootstrap.as_str()),
        ("group.id", unique("group-pause").as_str()),
        ("auto.offset.reset", "earliest"),
        ("enable.auto.commit", "false"),
    ])
    .await
    .expect("consumer should connect");
    let paused_partition = TopicPartition::new(topic.clone(), 0);
    let flowing_partition = TopicPartition::new(topic.clone(), 1);
    consumer
        .assign([paused_partition.clone(), flowing_partition.clone()])
        .expect("assign");

    consumer.pause(std::slice::from_ref(&paused_partition));
    assert_eq!(consumer.paused(), vec![paused_partition.clone()]);

    // Drain partition 1 completely; partition 0 must stay silent throughout.
    let mut flowing = 0;
    let mut leaked = 0;
    let deadline = Instant::now() + Duration::from_secs(30);
    while flowing < per_partition && Instant::now() < deadline {
        let records = consumer.poll(Duration::from_secs(1)).await.expect("poll");
        for record in &records {
            if record.partition == 0 {
                leaked += 1;
            } else {
                flowing += 1;
            }
        }
    }
    assert_eq!(flowing, per_partition, "unpaused partition keeps flowing");

    // Negative control: with partition 1 drained, further polls can only ever
    // surface partition 0 — a broken pause would leak its records right here.
    for _ in 0..3 {
        let records = consumer
            .poll(Duration::from_millis(500))
            .await
            .expect("poll");
        leaked += records
            .into_iter()
            .filter(|record| record.partition == 0)
            .count();
    }
    assert_eq!(leaked, 0, "paused partition must deliver nothing");

    // Resume: the backlog arrives in full, from offset 0.
    consumer.resume(std::slice::from_ref(&paused_partition));
    assert!(consumer.paused().is_empty());
    let mut resumed = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(30);
    while resumed.len() < per_partition && Instant::now() < deadline {
        let records = consumer.poll(Duration::from_secs(1)).await.expect("poll");
        resumed.extend(
            records
                .into_iter()
                .filter(|record| record.partition == 0)
                .map(|record| record.offset),
        );
    }
    println!("  resumed backlog offsets={resumed:?}");
    assert_eq!(
        resumed,
        (0..i64::try_from(per_partition).unwrap()).collect::<Vec<_>>(),
        "resume delivers the paused partition's whole backlog"
    );

    consumer.close().await;
    println!("real Kafka pause/resume smoke: ALL OK");
}

/// `offsets_for_times` resolves producer-supplied `CreateTime` timestamps to
/// the earliest offset at-or-after the queried time: an exact hit, a
/// between-records time (rounds up to the next record), and a time past the
/// last record (partition omitted from the result — negative control).
/// Consumed records carry those exact timestamps as `CreateTime`.
#[tokio::test]
#[ignore = "requires local Kafka from docker-compose.kafka.yml"]
async fn real_kafka_offsets_for_times_known_timestamps() {
    let bootstrap = bootstrap();
    let topic = unique("kacrab-ops-times");
    println!("real Kafka offsets_for_times smoke: topic={topic}");

    create_topic(&bootstrap, &topic, 1).await;

    // Five records with explicit, strictly increasing timestamps.
    let timestamps: Vec<i64> = (1..=5).map(|i| i * 10_000).collect();
    let producer = connect_producer(&bootstrap).await;
    for (i, ts) in timestamps.iter().enumerate() {
        let record = ProducerRecord::new(topic.clone(), 0)
            .value(Bytes::from(format!("t{i}")))
            .try_timestamp_ms(*ts)
            .expect("valid timestamp");
        let _receipt = producer
            .send(record)
            .expect("send")
            .await
            .expect("delivery");
    }

    let mut consumer = Consumer::from_map([
        ("bootstrap.servers", bootstrap.as_str()),
        ("group.id", unique("group-times").as_str()),
        ("auto.offset.reset", "earliest"),
        ("enable.auto.commit", "false"),
    ])
    .await
    .expect("consumer should connect");
    let partition = TopicPartition::new(topic.clone(), 0);

    let lookup = |time: i64| {
        let mut query = HashMap::new();
        let _prev = query.insert(partition.clone(), time);
        query
    };

    // Exact hit on the first record.
    let resolved = consumer
        .offsets_for_times(lookup(10_000))
        .await
        .expect("offsets_for_times");
    let hit = resolved.get(&partition).expect("offset for t=10000");
    assert_eq!((hit.offset, hit.timestamp), (0, 10_000));

    // Between records 0 and 1: rounds up to record 1.
    let resolved = consumer
        .offsets_for_times(lookup(15_000))
        .await
        .expect("offsets_for_times");
    let hit = resolved.get(&partition).expect("offset for t=15000");
    assert_eq!((hit.offset, hit.timestamp), (1, 20_000));

    // Exact hit on the last record.
    let resolved = consumer
        .offsets_for_times(lookup(50_000))
        .await
        .expect("offsets_for_times");
    let hit = resolved.get(&partition).expect("offset for t=50000");
    assert_eq!((hit.offset, hit.timestamp), (4, 50_000));

    // Negative control: past every record — no offset qualifies, so the
    // partition is omitted. An implementation echoing the query blindly (or
    // returning the log end as a "hit") fails here.
    let resolved = consumer
        .offsets_for_times(lookup(50_001))
        .await
        .expect("offsets_for_times");
    assert!(
        !resolved.contains_key(&partition),
        "no record at or after t=50001, got {:?}",
        resolved.get(&partition)
    );

    // The consumed records carry the produced timestamps as CreateTime.
    consumer.assign([partition.clone()]).expect("assign");
    let mut collected = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(30);
    while collected.len() < timestamps.len() && Instant::now() < deadline {
        let records = consumer.poll(Duration::from_secs(1)).await.expect("poll");
        collected.extend(
            records
                .into_iter()
                .map(|record| (record.timestamp, record.timestamp_type)),
        );
    }
    println!("  collected timestamps={collected:?}");
    assert_eq!(
        collected,
        timestamps
            .iter()
            .map(|ts| (*ts, TimestampType::CreateTime))
            .collect::<Vec<_>>()
    );

    consumer.close().await;
    println!("real Kafka offsets_for_times smoke: ALL OK");
}

/// Headers set on a produced record arrive on the consumed record byte-exact
/// and in order — including a null-valued header and a duplicate key — while a
/// headerless record consumes with an empty header list (negative control
/// against headers bleeding across records).
#[tokio::test]
#[ignore = "requires local Kafka from docker-compose.kafka.yml"]
async fn real_kafka_headers_reach_the_consumer() {
    let bootstrap = bootstrap();
    let topic = unique("kacrab-ops-headers");
    println!("real Kafka consume-side headers smoke: topic={topic}");

    create_topic(&bootstrap, &topic, 1).await;
    let producer = connect_producer(&bootstrap).await;
    let with_headers = ProducerRecord::new(topic.clone(), 0)
        .key(Bytes::from_static(b"hk"))
        .value(Bytes::from_static(b"hv"))
        .header("h1", "v1")
        .header_null("h2")
        .header("h1", "v3"); // duplicate key is legal and order-preserved
    let _receipt = producer
        .send(with_headers)
        .expect("send")
        .await
        .expect("delivery");
    let without_headers = ProducerRecord::new(topic.clone(), 0).value(Bytes::from_static(b"plain"));
    let _receipt = producer
        .send(without_headers)
        .expect("send")
        .await
        .expect("delivery");

    let mut consumer = Consumer::from_map([
        ("bootstrap.servers", bootstrap.as_str()),
        ("group.id", unique("group-headers").as_str()),
        ("auto.offset.reset", "earliest"),
        ("enable.auto.commit", "false"),
    ])
    .await
    .expect("consumer should connect");
    consumer
        .assign([TopicPartition::new(topic.clone(), 0)])
        .expect("assign");

    let mut collected = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(30);
    while collected.len() < 2 && Instant::now() < deadline {
        let records = consumer.poll(Duration::from_secs(1)).await.expect("poll");
        collected.extend(records);
    }
    assert_eq!(collected.len(), 2, "both records consumed");

    let first = collected.first().expect("record 0");
    println!("  record 0 headers={:?}", first.headers);
    assert_eq!(first.key.as_deref(), Some(b"hk".as_slice()));
    assert_eq!(first.value.as_deref(), Some(b"hv".as_slice()));
    assert_eq!(
        first.headers,
        vec![
            RecordHeader {
                key: Bytes::from_static(b"h1"),
                value: Some(Bytes::from_static(b"v1")),
            },
            RecordHeader {
                key: Bytes::from_static(b"h2"),
                value: None,
            },
            RecordHeader {
                key: Bytes::from_static(b"h1"),
                value: Some(Bytes::from_static(b"v3")),
            },
        ],
        "headers survive byte-exact, ordered, with null value and duplicate key"
    );

    let second = collected.get(1).expect("record 1");
    assert_eq!(second.value.as_deref(), Some(b"plain".as_slice()));
    assert!(
        second.headers.is_empty(),
        "headerless record must not inherit headers: {:?}",
        second.headers
    );

    consumer.close().await;
    println!("real Kafka consume-side headers smoke: ALL OK");
}

/// A static member (`group.instance.id`) that closes and rejoins under the same
/// instance id retakes its old assignment at the *same* group generation — the
/// coordinator swaps the member id without triggering a rebalance — and does so
/// promptly, not after the departed member's session expires. A dynamic member
/// would bump the generation, so the equality is the non-rebalance proof.
#[tokio::test]
#[ignore = "requires local Kafka from docker-compose.kafka.yml"]
async fn real_kafka_static_membership_rejoins_without_rebalance() {
    let bootstrap = bootstrap();
    let topic = unique("kacrab-ops-static");
    let group = unique("group-static");
    let instance = unique("instance");
    let session_timeout = Duration::from_secs(30);
    println!("real Kafka static membership smoke: topic={topic} instance={instance}");

    create_topic(&bootstrap, &topic, 1).await;
    produce_values(&bootstrap, &topic, 0, 3).await;

    let connect = |client: &'static str| {
        let bootstrap = bootstrap.clone();
        let group = group.clone();
        let instance = instance.clone();
        async move {
            Consumer::from_map([
                ("bootstrap.servers", bootstrap.as_str()),
                ("client.id", client),
                ("group.id", group.as_str()),
                ("group.instance.id", instance.as_str()),
                ("session.timeout.ms", "30000"),
                ("heartbeat.interval.ms", "1000"),
                ("auto.offset.reset", "earliest"),
                ("enable.auto.commit", "false"),
            ])
            .await
            .expect("consumer should connect")
        }
    };

    // First incarnation: join, take the partition, note the generation.
    let mut first = connect("kacrab-static-first").await;
    first.subscribe([topic.clone()]).expect("subscribe");
    let offsets = collect_offsets(&mut first, 3, 30).await;
    assert_eq!(offsets, vec![0, 1, 2], "first incarnation consumes");
    let first_assignment = first.assignment();
    let first_generation = first.group_metadata().generation_id;
    assert_eq!(first_assignment.len(), 1);
    assert!(first_generation >= 1, "joined generation is known");
    // Static members stay registered across close (no LeaveGroup is sent).
    first.close().await;

    // Second incarnation under the same instance id.
    let rejoin_started = Instant::now();
    let mut second = connect("kacrab-static-second").await;
    second.subscribe([topic.clone()]).expect("subscribe");
    let deadline = rejoin_started + Duration::from_secs(20);
    while second.assignment().is_empty() && Instant::now() < deadline {
        let _records = second.poll(Duration::from_millis(500)).await.expect("poll");
    }
    let rejoin_elapsed = rejoin_started.elapsed();
    let second_generation = second.group_metadata().generation_id;
    println!(
        "  rejoin took {rejoin_elapsed:?}; generation {first_generation} -> {second_generation}"
    );

    assert_eq!(
        second.assignment(),
        first_assignment,
        "the static member retakes its old assignment"
    );
    assert_eq!(
        second_generation, first_generation,
        "same generation: the rejoin must not have rebalanced the group"
    );
    // Promptness: the takeover must not have waited out the old session.
    assert!(
        rejoin_elapsed < session_timeout / 2,
        "static rejoin should complete well within the {session_timeout:?} session timeout (took \
         {rejoin_elapsed:?})"
    );

    // The retaken assignment fetches: newly produced records arrive.
    produce_values(&bootstrap, &topic, 0, 2).await;
    let retaken = first_assignment.first().expect("partition").clone();
    second.seek(&retaken, 3).expect("seek");
    let offsets = collect_offsets(&mut second, 2, 30).await;
    assert_eq!(
        offsets,
        vec![3, 4],
        "second incarnation consumes new records"
    );

    second.close().await;
    println!("real Kafka static membership smoke: ALL OK");
}

/// On a fresh group, `auto.offset.reset=latest` positions at the log end — the
/// consumer never sees pre-existing records, only ones produced afterwards —
/// while `earliest` on another fresh group reads the full log (negative
/// control proving the history really was there to skip). `committed` reports
/// nothing for a group that has never committed.
#[tokio::test]
#[ignore = "requires local Kafka from docker-compose.kafka.yml"]
async fn real_kafka_auto_offset_reset_latest_skips_history() {
    let bootstrap = bootstrap();
    let topic = unique("kacrab-ops-latest");
    let old = 5_i64;
    let new = 3_i64;
    println!("real Kafka auto.offset.reset smoke: topic={topic}");

    create_topic(&bootstrap, &topic, 1).await;
    produce_values(&bootstrap, &topic, 0, usize::try_from(old).unwrap()).await;

    let partition = TopicPartition::new(topic.clone(), 0);
    let mut latest = Consumer::from_map([
        ("bootstrap.servers", bootstrap.as_str()),
        ("group.id", unique("group-latest").as_str()),
        ("auto.offset.reset", "latest"),
        ("enable.auto.commit", "false"),
    ])
    .await
    .expect("consumer should connect");

    // A fresh group has no committed offset to resume from.
    let committed = latest
        .committed(std::slice::from_ref(&partition))
        .await
        .expect("committed");
    assert!(
        !committed.contains_key(&partition),
        "fresh group must have no committed offset, got {:?}",
        committed.get(&partition)
    );

    // `latest` resolves the initial position to the log end, past the history.
    latest.assign([partition.clone()]).expect("assign");
    let position = latest.position(&partition).await.expect("position");
    assert_eq!(position, old, "latest resets to the log end offset");

    // Only records produced after the reset are delivered.
    produce_values(&bootstrap, &topic, 0, usize::try_from(new).unwrap()).await;
    let offsets = collect_offsets(&mut latest, usize::try_from(new).unwrap(), 30).await;
    println!("  latest consumer offsets={offsets:?}");
    assert_eq!(
        offsets,
        (old..old + new).collect::<Vec<_>>(),
        "latest sees exactly the post-reset records"
    );
    latest.close().await;

    // Negative control: earliest on another fresh group reads the whole log,
    // proving the skipped history existed and `latest` genuinely skipped it.
    let mut earliest = Consumer::from_map([
        ("bootstrap.servers", bootstrap.as_str()),
        ("group.id", unique("group-earliest").as_str()),
        ("auto.offset.reset", "earliest"),
        ("enable.auto.commit", "false"),
    ])
    .await
    .expect("consumer should connect");
    earliest.assign([partition.clone()]).expect("assign");
    let offsets = collect_offsets(&mut earliest, usize::try_from(old + new).unwrap(), 30).await;
    println!("  earliest consumer offsets={offsets:?}");
    assert_eq!(offsets, (0..old + new).collect::<Vec<_>>());
    earliest.close().await;

    println!("real Kafka auto.offset.reset smoke: ALL OK");
}

/// `enforce_rebalance` makes the member rejoin on the next poll: the group
/// generation advances, the assignment is retaken, and consumption continues.
#[tokio::test]
#[ignore = "requires local Kafka from docker-compose.kafka.yml"]
async fn real_kafka_enforce_rebalance_rejoins_and_resumes() {
    let bootstrap = bootstrap();
    let topic = unique("kacrab-ops-enforce");
    println!("real Kafka enforce_rebalance smoke: topic={topic}");

    create_topic(&bootstrap, &topic, 1).await;
    produce_values(&bootstrap, &topic, 0, 3).await;

    let mut consumer = Consumer::from_map([
        ("bootstrap.servers", bootstrap.as_str()),
        ("group.id", unique("group-enforce").as_str()),
        ("auto.offset.reset", "earliest"),
        ("enable.auto.commit", "false"),
    ])
    .await
    .expect("consumer should connect");
    consumer.subscribe([topic.clone()]).expect("subscribe");

    let offsets = collect_offsets(&mut consumer, 3, 30).await;
    assert_eq!(offsets, vec![0, 1, 2]);
    let before = consumer.group_metadata().generation_id;
    assert!(before >= 1, "member joined a known generation");

    consumer.enforce_rebalance();
    let deadline = Instant::now() + Duration::from_secs(30);
    while consumer.group_metadata().generation_id <= before && Instant::now() < deadline {
        let _records = consumer
            .poll(Duration::from_millis(500))
            .await
            .expect("poll");
    }
    let after = consumer.group_metadata().generation_id;
    println!("  generation {before} -> {after}");
    assert!(
        after > before,
        "enforce_rebalance must drive a rejoin that bumps the generation"
    );
    assert_eq!(consumer.assignment().len(), 1, "assignment retaken");

    // Consumption continues after the rejoin.
    produce_values(&bootstrap, &topic, 0, 2).await;
    let offsets = collect_offsets(&mut consumer, 2, 30).await;
    assert_eq!(
        offsets,
        vec![3, 4],
        "records produced after the rejoin arrive"
    );

    consumer.close().await;
    println!("real Kafka enforce_rebalance smoke: ALL OK");
}
