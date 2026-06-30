#![cfg(feature = "producer")]
//! Real 3-broker cluster integration tests: multi-broker dispatch and
//! leadership-change recovery against `docker-compose.cluster.yml`.
//!
//! The cluster topic has 6 partitions, replication factor 3, with preferred
//! leaders spread across all three brokers (run a PREFERRED leader election
//! after start so leadership is not all on one node). Producing to every
//! partition therefore dispatches to every broker. The failover test stops one
//! broker mid-run and confirms the producer refreshes metadata, re-routes to
//! the new leaders, and still delivers every record.
//!
//! Run after:
//!   `docker compose -f docker-compose.cluster.yml up -d`
//!   `docker exec kacrab-kafka1 /opt/kafka/bin/kafka-leader-election.sh \
//!      --bootstrap-server kafka1:29092 --election-type PREFERRED \
//!      --all-topic-partitions`
//!   `cargo test -p kacrab --test real_kafka_cluster -- --ignored --nocapture`

#![allow(
    clippy::expect_used,
    clippy::panic,
    clippy::print_stdout,
    reason = "Ignored real-broker tests are explicit smoke checks with direct failure output."
)]

use std::{env, process::Command};

use bytes::Bytes;
use kacrab::producer::{Producer, ProducerRecord, RecordMetadata, Result as ProducerResult};

const PARTITIONS: i32 = 6;

#[tokio::test]
#[ignore = "requires the 3-broker cluster from docker-compose.cluster.yml"]
async fn real_kafka_cluster_dispatches_across_brokers() {
    let producer = build_producer().await;
    println!(
        "multi-broker dispatch: bootstrap={}, topic={}, partitions=0..{PARTITIONS}",
        bootstrap(),
        topic()
    );

    let receipts = produce_to_all_partitions(&producer, "dispatch").await;
    for (partition, offset) in &receipts {
        println!("partition {partition} -> offset {offset}");
        assert!(
            *offset >= 0,
            "partition {partition} should get a real offset"
        );
    }
    assert_eq!(
        receipts.len(),
        PARTITIONS as usize,
        "every partition should accept a record"
    );
}

#[tokio::test]
#[ignore = "requires the 3-broker cluster and stops a broker via docker"]
async fn real_kafka_cluster_survives_broker_loss() {
    let victim =
        env::var("KACRAB_CLUSTER_VICTIM").unwrap_or_else(|_error| "kacrab-kafka2".to_owned());
    let producer = build_producer().await;

    // Baseline round while all three brokers are up.
    let baseline = produce_to_all_partitions(&producer, "baseline").await;
    assert_eq!(
        baseline.len(),
        PARTITIONS as usize,
        "baseline should deliver"
    );
    println!("baseline delivered across all partitions; stopping broker {victim}");

    // Take a broker down: partitions it led must fail over to an ISR replica,
    // and the producer must notice (NotLeader / connection drop), refresh
    // metadata, and re-route.
    docker(&["stop", &victim]);

    // Recovery round. Collect every result BEFORE restarting/asserting so the
    // broker is always restored, even if delivery fails.
    let results = produce_to_all_partitions_collect(&producer, "after-loss").await;

    // Restore the broker so the env stays usable for repeat runs.
    docker(&["start", &victim]);

    let delivered = results.iter().filter(|result| result.is_ok()).count();
    for result in &results {
        if let Err(error) = result {
            println!("delivery failed after broker loss: {error}");
        }
    }
    assert_eq!(
        delivered, PARTITIONS as usize,
        "every partition should still deliver after a broker is lost ({delivered}/{PARTITIONS})"
    );
    println!("all {PARTITIONS} partitions delivered after losing broker {victim}");
}

#[tokio::test]
#[ignore = "diagnostic: does an unaffected partition keep delivering when an unrelated broker dies"]
async fn real_kafka_cluster_unaffected_partition_during_broker_loss() {
    // Partition 0's leader is broker 1; we stop broker 2. Partition 0 should be
    // completely unaffected. Produce to it once per second across the stop.
    let victim =
        env::var("KACRAB_CLUSTER_VICTIM").unwrap_or_else(|_error| "kacrab-kafka2".to_owned());
    let partition: i32 = env::var("KACRAB_CLUSTER_PARTITION")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(0);
    let producer = build_producer().await;
    let mut ok = 0_u32;
    for round in 0..20 {
        if round == 5 {
            println!("stopping {victim} at round {round}");
            docker(&["stop", &victim]);
        }
        let value = Bytes::from(format!("kacrab-p{partition}-round-{round}"));
        match producer
            .send(ProducerRecord::new(topic(), partition).value(value))
            .expect("send should enqueue")
            .await
        {
            Ok(metadata) => {
                ok += 1;
                println!(
                    "round {round}: partition {partition} -> offset {}",
                    metadata.offset
                );
            },
            Err(error) => println!("round {round}: partition {partition} FAILED: {error}"),
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
    docker(&["start", &victim]);
    println!("partition {partition} delivered {ok}/20 across the broker loss");
    assert!(
        ok >= 18,
        "partition {partition} should keep delivering; got {ok}/20"
    );
}

#[tokio::test]
#[ignore = "diagnostic: burst to live-broker-only partitions after an unrelated broker dies"]
async fn real_kafka_cluster_live_only_burst_during_broker_loss() {
    // After stopping broker 2 (leads partitions 1 and 5), produce a burst to the
    // partitions whose leaders are on the still-alive brokers (0,3 on broker 1;
    // 2,4 on broker 3). If these stall, a dead partition is not required to wedge
    // the flush; if they deliver, the wedge is head-of-line blocking from the
    // dead-broker partitions sharing a coalesced flush.
    let victim =
        env::var("KACRAB_CLUSTER_VICTIM").unwrap_or_else(|_error| "kacrab-kafka2".to_owned());
    let live_partitions = [0_i32, 2, 3, 4];
    let producer = build_producer().await;

    docker(&["stop", &victim]);
    println!("stopped {victim}; bursting to live-broker partitions {live_partitions:?}");

    let mut futures = Vec::new();
    for partition in live_partitions {
        let value = Bytes::from(format!("kacrab-live-p{partition}"));
        futures.push((
            partition,
            producer
                .send(ProducerRecord::new(topic(), partition).value(value))
                .expect("send should enqueue"),
        ));
    }
    let mut ok = 0_u32;
    for (partition, future) in futures {
        match future.await {
            Ok(metadata) => {
                ok += 1;
                println!("partition {partition} -> offset {}", metadata.offset);
            },
            Err(error) => println!("partition {partition} FAILED: {error}"),
        }
    }
    docker(&["start", &victim]);
    println!(
        "live-broker partitions delivered {ok}/{}",
        live_partitions.len()
    );
    assert_eq!(
        ok as usize,
        live_partitions.len(),
        "partitions on alive brokers should deliver even when batched after a broker loss"
    );
}

async fn build_producer() -> Producer {
    Producer::builder()
        .set("bootstrap.servers", bootstrap())
        .set("client.id", "kacrab-real-kafka-cluster-test")
        .set("acks", "all")
        .set(
            "enable.idempotence",
            env::var("KACRAB_IDEMPOTENCE").as_deref().unwrap_or("true"),
        )
        .set("retries", "2147483647")
        .set("max.in.flight.requests.per.connection", "5")
        .set("request.timeout.ms", "15000")
        .set("delivery.timeout.ms", "60000")
        .set("batch.size", "16384")
        .set("linger.ms", "5")
        .set("buffer.memory", "33554432")
        .build()
        .await
        .expect("producer should connect to the local cluster")
}

/// Produces one record to every partition and asserts each delivery succeeds,
/// returning `(partition, offset)` pairs.
async fn produce_to_all_partitions(producer: &Producer, tag: &str) -> Vec<(i32, i64)> {
    produce_to_all_partitions_collect(producer, tag)
        .await
        .into_iter()
        .enumerate()
        .map(|(partition, result)| {
            let metadata = result.unwrap_or_else(|error| {
                panic!("partition {partition} delivery should succeed: {error}")
            });
            (metadata.partition, metadata.offset)
        })
        .collect()
}

/// Produces one record to every partition and returns the raw delivery results
/// (so the caller can restore infrastructure before asserting).
async fn produce_to_all_partitions_collect(
    producer: &Producer,
    tag: &str,
) -> Vec<ProducerResult<RecordMetadata>> {
    let mut futures = Vec::with_capacity(PARTITIONS as usize);
    for partition in 0..PARTITIONS {
        let value = Bytes::from(format!("kacrab-cluster-{tag}-p{partition}"));
        let future = producer
            .send(ProducerRecord::new(topic(), partition).value(value))
            .expect("send should enqueue");
        futures.push(future);
    }
    let mut results = Vec::with_capacity(futures.len());
    for (partition, future) in futures.into_iter().enumerate() {
        let result = future.await;
        match &result {
            Ok(metadata) => println!(
                "  [{tag}] partition {partition} -> offset {}",
                metadata.offset
            ),
            Err(error) => println!("  [{tag}] partition {partition} FAILED: {error}"),
        }
        results.push(result);
    }
    results
}

fn docker(args: &[&str]) {
    let status = Command::new("docker")
        .args(args)
        .status()
        .expect("docker command should run");
    assert!(status.success(), "docker {args:?} should succeed");
}

fn bootstrap() -> String {
    env::var("KACRAB_CLUSTER_BOOTSTRAP")
        .unwrap_or_else(|_error| "127.0.0.1:9092,127.0.0.1:9094,127.0.0.1:9096".to_owned())
}

fn topic() -> String {
    env::var("KACRAB_CLUSTER_TOPIC").unwrap_or_else(|_error| "kacrab-cluster".to_owned())
}
