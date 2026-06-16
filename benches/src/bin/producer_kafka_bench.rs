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
    time::{Duration, Instant},
};

use bytes::Bytes;
use kacrab::producer::{KafkaProducer, ProducerRecord};
use tokio::runtime::Builder;

const PARTITIONS: usize = 3;
const PIPELINED_IN_FLIGHT: usize = 8;

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
        println!(
            "real Kafka benchmark: bootstrap={bootstrap}, topic={topic}, partitions={PARTITIONS}, \
             in_flight={PIPELINED_IN_FLIGHT}"
        );
        for scenario in scenarios {
            run_scenario(bootstrap, &topic, scenario).await;
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

async fn run_scenario(bootstrap: SocketAddr, topic: &str, scenario: Scenario) {
    let mut producer = KafkaProducer::builder()
        .set("bootstrap.servers", bootstrap.to_string())
        .set("client.id", "kacrab-producer-kafka-bench")
        .set("enable.idempotence", "false")
        .set("acks", "1")
        .set("compression.type", "none")
        .set("retries", "0")
        .set("request.timeout.ms", "30000")
        .set("delivery.timeout.ms", "120000")
        .set("batch.size", "1")
        .set(
            "buffer.memory",
            batch_buffer_memory(scenario.batch_messages, scenario.value_size).to_string(),
        )
        .set(
            "max.in.flight.requests.per.connection",
            PIPELINED_IN_FLIGHT.to_string(),
        )
        .set(
            "socket.read.buffer.capacity.bytes",
            (1024 * 1024).to_string(),
        )
        .set(
            "broker.queue.capacity",
            PIPELINED_IN_FLIGHT.saturating_mul(2).to_string(),
        )
        .set("buffer.pool.capacity", "128")
        .build()
        .await
        .expect("benchmark producer config should build");
    let value = Bytes::from(vec![b'x'; scenario.value_size]);
    warm_up_producer(&mut producer, topic, value.clone()).await;
    let started = Instant::now();
    let mut sent = 0usize;
    let mut produce_requests = 0usize;
    while sent < scenario.messages {
        let batch_messages = scenario
            .batch_messages
            .min(scenario.messages.saturating_sub(sent));
        producer
            .send_batch_untracked((0..batch_messages).map(|index| {
                let partition = i32::try_from((sent + index) % PARTITIONS).unwrap_or_default();
                ProducerRecord::new(topic.to_owned(), partition).value(value.clone())
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
    print_result(scenario, started.elapsed(), produce_requests);
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

fn scenarios() -> Vec<Scenario> {
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
            batch_messages: 16_384,
        },
        Scenario {
            name: "100,000 messages x 10 KiB",
            messages: 100_000,
            value_size: 10 * 1024,
            batch_messages: 96,
        },
    ]
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
