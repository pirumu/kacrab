//! Real Kafka producer benchmark through the public producer API.

#![allow(
    clippy::arithmetic_side_effects,
    clippy::cast_possible_truncation,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::print_stdout,
    clippy::unwrap_used,
    missing_docs,
    reason = "Benchmark binaries prefer direct fail-fast setup and explicit output."
)]

use std::{
    env,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::Bytes;
use kacrab::producer::{KafkaProducer, ProducerRecord};
use tokio::runtime::Builder;

const PARTITIONS: usize = 3;
const DEFAULT_PIPELINED_IN_FLIGHT: usize = 8;

fn main() {
    let runtime = Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
        .expect("benchmark runtime");
    runtime.block_on(async {
        let bootstrap = bootstrap_addr();
        let topic = topic();
        let scenarios = scenarios();
        let partitions = partitions();
        let in_flight = in_flight();
        let acks = acks();
        let batch_size = batch_size();
        let partition_mode = partition_mode();
        println!(
            "real Kafka benchmark: bootstrap={bootstrap}, topic={topic}, partitions={partitions}, \
             in_flight={in_flight}, acks={acks}, batch_size={batch_size}, \
             partition_mode={partition_mode}"
        );
        for scenario in scenarios {
            run_scenario(BenchmarkRun {
                bootstrap,
                topic: &topic,
                scenario,
                partitions,
                in_flight,
                acks: &acks,
                batch_size,
                partition_mode,
            })
            .await;
        }
    });
}

#[derive(Debug, Clone, Copy)]
struct Scenario {
    name: &'static str,
    messages: usize,
    value_size: usize,
    batch_messages: usize,
}

#[derive(Debug, Clone, Copy)]
struct BenchmarkRun<'a> {
    bootstrap: SocketAddr,
    scenario: Scenario,
    topic: &'a str,
    partitions: usize,
    in_flight: usize,
    acks: &'a str,
    batch_size: usize,
    partition_mode: PartitionMode,
}

async fn run_scenario(run: BenchmarkRun<'_>) {
    let mut producer = KafkaProducer::builder()
        .set("bootstrap.servers", run.bootstrap.to_string())
        .set("client.id", "kacrab-producer-kafka-bench")
        .set("enable.idempotence", "false")
        .set("acks", run.acks)
        .set("compression.type", "none")
        .set("retries", "0")
        .set("request.timeout.ms", "30000")
        .set("delivery.timeout.ms", "120000")
        .set("batch.size", run.batch_size.to_string())
        .set(
            "buffer.memory",
            batch_buffer_memory(run.scenario.batch_messages, run.scenario.value_size).to_string(),
        )
        .set(
            "max.in.flight.requests.per.connection",
            run.in_flight.to_string(),
        )
        .set(
            "socket.read.buffer.capacity.bytes",
            (1024 * 1024).to_string(),
        )
        .set(
            "broker.queue.capacity",
            run.in_flight.saturating_mul(2).to_string(),
        )
        .set("buffer.pool.capacity", "128")
        .build()
        .await
        .expect("benchmark producer config should build");
    let value = Bytes::from(vec![b'x'; run.scenario.value_size]);
    warm_up_producer(&mut producer, run.topic, value.clone()).await;
    let topic = Arc::<str>::from(run.topic);
    let started = Instant::now();
    let mut sent = 0usize;
    let mut produce_requests = 0usize;
    while sent < run.scenario.messages {
        let batch_messages = run
            .scenario
            .batch_messages
            .min(run.scenario.messages.saturating_sub(sent));
        producer
            .send_batch_untracked((0..batch_messages).map(|index| {
                benchmark_record(
                    Arc::clone(&topic),
                    sent + index,
                    run.partitions,
                    run.partition_mode,
                )
                .value(value.clone())
            }))
            .await
            .expect("benchmark send should fit and dispatch");
        sent = sent.saturating_add(batch_messages);
        produce_requests = produce_requests.saturating_add(1);
    }
    producer
        .flush()
        .await
        .expect("benchmark flush should succeed");
    print_result(run.scenario, started.elapsed(), produce_requests);
}

async fn warm_up_producer(producer: &mut KafkaProducer, topic: &str, value: Bytes) {
    producer
        .send_batch_untracked([ProducerRecord::new(topic.to_owned(), 0).value(value)])
        .await
        .expect("benchmark warmup send should dispatch");
    producer
        .flush()
        .await
        .expect("benchmark warmup flush should succeed");
}

#[derive(Debug, Clone, Copy)]
enum PartitionMode {
    Unassigned,
    ManualRoundRobin,
}

impl std::fmt::Display for PartitionMode {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unassigned => formatter.write_str("unassigned"),
            Self::ManualRoundRobin => formatter.write_str("manual-round-robin"),
        }
    }
}

fn benchmark_record(
    topic: Arc<str>,
    index: usize,
    partitions: usize,
    partition_mode: PartitionMode,
) -> ProducerRecord {
    match partition_mode {
        PartitionMode::Unassigned => ProducerRecord::unassigned(topic),
        PartitionMode::ManualRoundRobin => {
            let partition = i32::try_from(index % partitions).unwrap_or_default();
            ProducerRecord::new(topic, partition)
        },
    }
}

fn scenarios() -> Vec<Scenario> {
    if env::var("KACRAB_ONLY_10B").ok().as_deref() == Some("1") {
        return vec![Scenario {
            name: "5,000,000 messages x 10 bytes",
            messages: 5_000_000,
            value_size: 10,
            batch_messages: env_usize("KACRAB_BATCH_MESSAGES_10B").unwrap_or(16_384),
        }];
    }
    if env::var("KACRAB_BENCH_SMOKE").ok().as_deref() == Some("1") {
        return vec![
            Scenario {
                name: "smoke: 10,000 messages x 10 bytes",
                messages: 10_000,
                value_size: 10,
                batch_messages: 1024,
            },
            Scenario {
                name: "smoke: 1,000 messages x 10 KiB",
                messages: 1_000,
                value_size: 10 * 1024,
                batch_messages: 96,
            },
        ];
    }
    vec![
        Scenario {
            name: "5,000,000 messages x 10 bytes",
            messages: 5_000_000,
            value_size: 10,
            batch_messages: env_usize("KACRAB_BATCH_MESSAGES_10B").unwrap_or(16_384),
        },
        Scenario {
            name: "100,000 messages x 10 KiB",
            messages: 100_000,
            value_size: 10 * 1024,
            batch_messages: 96,
        },
    ]
}

fn partitions() -> usize {
    env_usize("KACRAB_PARTITIONS").unwrap_or(PARTITIONS)
}

fn in_flight() -> usize {
    env_usize("KACRAB_IN_FLIGHT").unwrap_or(DEFAULT_PIPELINED_IN_FLIGHT)
}

fn acks() -> String {
    env::var("KACRAB_ACKS").unwrap_or_else(|_error| "1".to_owned())
}

fn batch_size() -> usize {
    env_usize("KACRAB_BATCH_SIZE").unwrap_or(16_384)
}

fn partition_mode() -> PartitionMode {
    match env::var("KACRAB_PARTITION_MODE")
        .unwrap_or_else(|_error| "unassigned".to_owned())
        .as_str()
    {
        "manual" | "manual-round-robin" | "round-robin" => PartitionMode::ManualRoundRobin,
        _ => PartitionMode::Unassigned,
    }
}

fn env_usize(name: &str) -> Option<usize> {
    env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
}

fn bootstrap_addr() -> SocketAddr {
    env::var("KACRAB_BOOTSTRAP")
        .unwrap_or_else(|_error| "127.0.0.1:9092".to_owned())
        .parse()
        .expect("KACRAB_BOOTSTRAP must be a socket address")
}

fn topic() -> String {
    env::var("KACRAB_BENCH_TOPIC").unwrap_or_else(|_error| "kacrab-bench".to_owned())
}

fn batch_buffer_memory(batch_messages: usize, value_size: usize) -> usize {
    batch_messages
        .checked_mul(value_size.saturating_add(128))
        .and_then(|bytes| bytes.checked_add(1024 * 1024))
        .expect("scenario buffer memory should not overflow")
}

fn print_result(scenario: Scenario, elapsed: Duration, produce_requests: usize) {
    let seconds = elapsed.as_secs_f64();
    let messages_u32 =
        u32::try_from(scenario.messages).expect("scenario message count should fit in u32");
    let messages_per_second = f64::from(messages_u32) / seconds;
    let megabytes = scenario
        .messages
        .checked_mul(scenario.value_size)
        .and_then(|bytes| u32::try_from(bytes).ok())
        .map(|bytes| f64::from(bytes) / (1024.0 * 1024.0))
        .expect("scenario bytes should not overflow");
    let megabytes_per_second = megabytes / seconds;
    println!(
        "{}: {:.0} messages/s, {:.3} MiB/s ({:.3}s, {} produce requests)",
        scenario.name, messages_per_second, megabytes_per_second, seconds, produce_requests
    );
}
