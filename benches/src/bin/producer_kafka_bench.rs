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
use kacrab::producer::{BatchSendFuture, Producer, ProducerMetricsSnapshot, ProducerRecord};
use tokio::runtime::Builder;

const PARTITIONS: usize = 3;
const DEFAULT_PIPELINED_IN_FLIGHT: usize = 5;

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
        let profile = bench_profile();
        let in_flight = in_flight_override();
        let acks = acks_override();
        let batch_size = batch_size_override();
        let partition_mode = partition_mode();
        let delivery_modes = delivery_modes();
        let tracked_delivery_window = tracked_delivery_window();
        println!(
            "real Kafka benchmark: bootstrap={bootstrap}, topic={topic}, partitions={partitions}, \
             profile={profile}, in_flight={}, acks={}, batch_size={}, \
             partition_mode={partition_mode}, delivery_modes={}, \
             tracked_delivery_window={tracked_delivery_window}",
            display_optional_usize(in_flight),
            display_optional_str(acks.as_deref()),
            display_optional_usize(batch_size),
            display_delivery_modes(&delivery_modes)
        );
        for scenario in scenarios {
            for delivery_mode in delivery_modes.iter().copied() {
                run_scenario(BenchmarkRun {
                    bootstrap,
                    topic: &topic,
                    scenario: scenario.clone(),
                    partitions,
                    profile,
                    in_flight,
                    acks: acks.clone(),
                    batch_size,
                    partition_mode,
                    delivery_mode,
                    tracked_delivery_window,
                })
                .await;
            }
        }
    });
}

#[derive(Debug, Clone)]
struct Scenario {
    name: String,
    messages: usize,
    value_size: usize,
    batch_messages: usize,
}

#[derive(Debug, Clone)]
struct BenchmarkRun<'a> {
    bootstrap: SocketAddr,
    scenario: Scenario,
    topic: &'a str,
    partitions: usize,
    profile: BenchProfile,
    in_flight: Option<usize>,
    acks: Option<String>,
    batch_size: Option<usize>,
    partition_mode: PartitionMode,
    delivery_mode: DeliveryMode,
    tracked_delivery_window: usize,
}

async fn run_scenario(run: BenchmarkRun<'_>) {
    let value = payload_value(run.scenario.value_size);
    let value_size = value.len();
    let metrics_enabled = metrics_enabled();
    let latency_enabled = latency_enabled();
    let mut producer = build_producer(&run, value_size, metrics_enabled, latency_enabled).await;
    warm_up_producer(&mut producer, &run, value.clone()).await;
    let _warmup_latencies = producer.take_dispatch_latency_samples();
    let warmup_metrics = producer.metrics();
    let send = run_send_loop(&mut producer, &run, value).await;
    let latency = latency_summary(producer.take_dispatch_latency_samples());
    let metrics = metrics_delta(producer.metrics(), warmup_metrics);
    print_result(&BenchmarkResult {
        scenario: &run.scenario,
        value_size,
        elapsed: send.elapsed,
        outer_chunks: send.outer_chunks,
        latency,
        metrics,
        metrics_enabled,
        delivery_mode: run.delivery_mode,
    });
}

async fn build_producer(
    run: &BenchmarkRun<'_>,
    value_size: usize,
    metrics_enabled: bool,
    latency_enabled: bool,
) -> Producer {
    let mut producer = Producer::builder()
        .set("bootstrap.servers", run.bootstrap.to_string())
        .set("client.id", "kacrab-producer-kafka-bench");
    if run.profile == BenchProfile::Relaxed {
        let in_flight = run.in_flight.unwrap_or(DEFAULT_PIPELINED_IN_FLIGHT);
        producer = producer
            .set("enable.idempotence", "false")
            .set("acks", run.acks.as_deref().unwrap_or("1"))
            .set("compression.type", "none")
            .set("retries", "0")
            .set("request.timeout.ms", "30000")
            .set("delivery.timeout.ms", "120000")
            .set("batch.size", run.batch_size.unwrap_or(16_384).to_string())
            .set(
                "buffer.memory",
                batch_buffer_memory(run.scenario.batch_messages, value_size).to_string(),
            )
            .set(
                "max.in.flight.requests.per.connection",
                in_flight.to_string(),
            )
            .set(
                "socket.read.buffer.capacity.bytes",
                (1024 * 1024).to_string(),
            )
            .set(
                "broker.queue.capacity",
                in_flight.saturating_mul(2).to_string(),
            )
            .set("buffer.pool.capacity", "128");
    } else {
        if let Some(acks) = &run.acks {
            producer = producer.set("acks", acks.as_str());
        }
        if let Some(batch_size) = run.batch_size {
            producer = producer.set("batch.size", batch_size.to_string());
        }
        if let Some(in_flight) = run.in_flight {
            producer = producer.set(
                "max.in.flight.requests.per.connection",
                in_flight.to_string(),
            );
        }
    }
    let mut producer = producer
        .build()
        .await
        .expect("benchmark producer config should build");
    if latency_enabled {
        producer.enable_dispatch_latency_metrics();
    }
    if metrics_enabled {
        producer.enable_metrics();
    }
    producer
}

#[derive(Debug, Clone, Copy)]
struct SendLoopResult {
    outer_chunks: usize,
    elapsed: Duration,
}

async fn run_send_loop(
    producer: &mut Producer,
    run: &BenchmarkRun<'_>,
    value: Bytes,
) -> SendLoopResult {
    match run.delivery_mode {
        DeliveryMode::Untracked => run_untracked_send_loop(producer, run, value).await,
        DeliveryMode::Tracked => run_callback_tracked_send_loop(producer, run, value).await,
        DeliveryMode::Batch => run_batch_receipt_send_loop(producer, run, value).await,
    }
}

async fn run_untracked_send_loop(
    producer: &mut Producer,
    run: &BenchmarkRun<'_>,
    value: Bytes,
) -> SendLoopResult {
    let topic = Arc::<str>::from(run.topic);
    let started = Instant::now();
    let mut sent = 0usize;
    let mut produce_requests = 0usize;
    while sent < run.scenario.messages {
        let batch_messages = run
            .scenario
            .batch_messages
            .min(run.scenario.messages.saturating_sub(sent));
        let records = (0..batch_messages).map(|index| {
            benchmark_record(
                Arc::clone(&topic),
                sent + index,
                run.partitions,
                run.partition_mode,
            )
            .value(value.clone())
        });
        producer
            .send_batch_untracked(records)
            .await
            .expect("benchmark send should fit and dispatch");
        sent = sent.saturating_add(batch_messages);
        produce_requests = produce_requests.saturating_add(1);
    }
    producer
        .flush()
        .await
        .expect("benchmark flush should succeed");
    SendLoopResult {
        outer_chunks: produce_requests,
        elapsed: started.elapsed(),
    }
}

async fn run_callback_tracked_send_loop(
    producer: &mut Producer,
    run: &BenchmarkRun<'_>,
    value: Bytes,
) -> SendLoopResult {
    let topic = Arc::<str>::from(run.topic);
    let started = Instant::now();
    let mut sent = 0usize;
    let mut produce_requests = 0usize;
    let mut pending_since_flush = 0usize;
    while sent < run.scenario.messages {
        let batch_messages = run
            .scenario
            .batch_messages
            .min(run.scenario.messages.saturating_sub(sent));
        for index in 0..batch_messages {
            let record = benchmark_record(
                Arc::clone(&topic),
                sent + index,
                run.partitions,
                run.partition_mode,
            )
            .value(value.clone());
            let _delivery = producer
                .send_with_callback(record, |_result| {})
                .await
                .expect("benchmark tracked callback send should fit and dispatch");
            produce_requests = produce_requests.saturating_add(1);
            pending_since_flush = pending_since_flush.saturating_add(1);
        }
        if pending_since_flush >= run.tracked_delivery_window {
            producer
                .flush()
                .await
                .expect("benchmark tracked flush should succeed");
            pending_since_flush = 0;
        }
        sent = sent.saturating_add(batch_messages);
    }
    producer
        .flush()
        .await
        .expect("benchmark tracked final flush should succeed");
    SendLoopResult {
        outer_chunks: produce_requests,
        elapsed: started.elapsed(),
    }
}

async fn run_batch_receipt_send_loop(
    producer: &mut Producer,
    run: &BenchmarkRun<'_>,
    value: Bytes,
) -> SendLoopResult {
    let topic = Arc::<str>::from(run.topic);
    let mut deliveries = Vec::with_capacity(
        run.tracked_delivery_window
            .min(run.scenario.messages)
            .min(run.scenario.batch_messages),
    );
    let started = Instant::now();
    let mut sent = 0usize;
    let mut produce_requests = 0usize;
    while sent < run.scenario.messages {
        let batch_messages = run
            .scenario
            .batch_messages
            .min(run.scenario.messages.saturating_sub(sent));
        let records = (0..batch_messages).map(|index| {
            benchmark_record(
                Arc::clone(&topic),
                sent + index,
                run.partitions,
                run.partition_mode,
            )
            .value(value.clone())
        });
        deliveries.push(
            producer
                .send_batch(records)
                .await
                .expect("benchmark batch receipt send should fit and dispatch"),
        );
        if deliveries.len() >= run.tracked_delivery_window {
            producer
                .flush()
                .await
                .expect("benchmark batch receipt flush should succeed");
            await_deliveries(&mut deliveries).await;
        }
        sent = sent.saturating_add(batch_messages);
        produce_requests = produce_requests.saturating_add(1);
    }
    producer
        .flush()
        .await
        .expect("benchmark batch receipt final flush should succeed");
    await_deliveries(&mut deliveries).await;
    SendLoopResult {
        outer_chunks: produce_requests,
        elapsed: started.elapsed(),
    }
}

async fn warm_up_producer(producer: &mut Producer, run: &BenchmarkRun<'_>, value: Bytes) {
    let topic = Arc::<str>::from(run.topic);
    let warmup_messages = warmup_record_count(run);
    match run.delivery_mode {
        DeliveryMode::Untracked => {
            let records = (0..warmup_messages).map(|index| {
                benchmark_record(
                    Arc::clone(&topic),
                    index,
                    run.partitions,
                    run.partition_mode,
                )
                .value(value.clone())
            });
            producer
                .send_batch_untracked(records)
                .await
                .expect("benchmark warmup send should dispatch");
        },
        DeliveryMode::Tracked => {
            for index in 0..warmup_messages {
                let record = benchmark_record(
                    Arc::clone(&topic),
                    index,
                    run.partitions,
                    run.partition_mode,
                )
                .value(value.clone());
                let _delivery = producer
                    .send_with_callback(record, |_result| {})
                    .await
                    .expect("benchmark tracked warmup send should dispatch");
            }
            producer
                .flush()
                .await
                .expect("benchmark tracked warmup flush should succeed");
            return;
        },
        DeliveryMode::Batch => {
            let records = (0..warmup_messages).map(|index| {
                benchmark_record(
                    Arc::clone(&topic),
                    index,
                    run.partitions,
                    run.partition_mode,
                )
                .value(value.clone())
            });
            let delivery = producer
                .send_batch(records)
                .await
                .expect("benchmark batch receipt warmup send should dispatch");
            producer
                .flush()
                .await
                .expect("benchmark batch receipt warmup flush should succeed");
            let _receipts = delivery
                .await
                .expect("benchmark batch receipt warmup delivery should succeed");
            return;
        },
    }
    producer
        .flush()
        .await
        .expect("benchmark warmup flush should succeed");
}

fn warmup_record_count(run: &BenchmarkRun<'_>) -> usize {
    run.scenario
        .batch_messages
        .min(run.scenario.messages)
        .clamp(1, 16_384)
}

async fn await_deliveries(deliveries: &mut Vec<BatchSendFuture>) {
    for delivery in deliveries.drain(..) {
        let _receipts = delivery.await.expect("benchmark delivery should succeed");
    }
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BenchProfile {
    KafkaDefault,
    Relaxed,
}

impl std::fmt::Display for BenchProfile {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::KafkaDefault => formatter.write_str("kafka-default"),
            Self::Relaxed => formatter.write_str("relaxed"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DeliveryMode {
    Untracked,
    Tracked,
    Batch,
}

impl std::fmt::Display for DeliveryMode {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Untracked => formatter.write_str("untracked"),
            Self::Tracked => formatter.write_str("tracked"),
            Self::Batch => formatter.write_str("batch"),
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
    let selection = ScenarioSelection {
        only_10b: env::var("KACRAB_ONLY_10B").ok().as_deref() == Some("1"),
        only_10kib: env::var("KACRAB_ONLY_10KIB").ok().as_deref() == Some("1"),
        smoke: env::var("KACRAB_BENCH_SMOKE").ok().as_deref() == Some("1"),
        custom_messages: env_usize("KACRAB_CUSTOM_MESSAGES"),
        custom_value_size: env_usize("KACRAB_CUSTOM_VALUE_SIZE"),
        custom_batch_messages: env_usize("KACRAB_CUSTOM_BATCH_MESSAGES"),
        payload_file_size: payload_file_size(),
        batch_messages_10b: env_usize("KACRAB_BATCH_MESSAGES_10B"),
        batch_messages_10kib: env_usize("KACRAB_BATCH_MESSAGES_10KIB"),
    };
    scenarios_for_selection(selection)
}

#[derive(Debug, Clone, Copy)]
struct ScenarioSelection {
    only_10b: bool,
    only_10kib: bool,
    smoke: bool,
    custom_messages: Option<usize>,
    custom_value_size: Option<usize>,
    custom_batch_messages: Option<usize>,
    payload_file_size: Option<usize>,
    batch_messages_10b: Option<usize>,
    batch_messages_10kib: Option<usize>,
}

fn scenarios_for_selection(selection: ScenarioSelection) -> Vec<Scenario> {
    if let Some(scenario) = custom_payload_scenario(selection) {
        return vec![scenario];
    }
    if selection.only_10b {
        return vec![small_payload_scenario(selection.batch_messages_10b)];
    }
    if selection.only_10kib {
        return vec![large_payload_scenario(selection.batch_messages_10kib)];
    }
    if selection.smoke {
        return vec![
            Scenario {
                name: "smoke: 10,000 messages x 10 bytes".to_owned(),
                messages: 10_000,
                value_size: 10,
                batch_messages: 1024,
            },
            Scenario {
                name: "smoke: 1,000 messages x 10 KiB".to_owned(),
                messages: 1_000,
                value_size: 10 * 1024,
                batch_messages: 96,
            },
        ];
    }
    vec![
        small_payload_scenario(selection.batch_messages_10b),
        large_payload_scenario(selection.batch_messages_10kib),
    ]
}

fn small_payload_scenario(batch_messages: Option<usize>) -> Scenario {
    Scenario {
        name: "5,000,000 messages x 10 bytes".to_owned(),
        messages: 5_000_000,
        value_size: 10,
        batch_messages: batch_messages.unwrap_or(16_384),
    }
}

fn large_payload_scenario(batch_messages: Option<usize>) -> Scenario {
    Scenario {
        name: "100,000 messages x 10 KiB".to_owned(),
        messages: 100_000,
        value_size: 10 * 1024,
        batch_messages: batch_messages.unwrap_or(96),
    }
}

fn custom_payload_scenario(selection: ScenarioSelection) -> Option<Scenario> {
    let value_size = selection
        .custom_value_size
        .or(selection.payload_file_size)?;
    let messages = match selection.custom_messages {
        Some(messages) => messages,
        None if selection.payload_file_size.is_some() => 100_000,
        None => return None,
    };
    let batch_messages = selection
        .custom_batch_messages
        .unwrap_or(if value_size >= 1024 { 96 } else { 16_384 });
    Some(Scenario {
        name: format!(
            "custom: {} messages x {} bytes",
            format_count(messages),
            value_size
        ),
        messages,
        value_size,
        batch_messages,
    })
}

fn partitions() -> usize {
    env_usize("KACRAB_PARTITIONS").unwrap_or(PARTITIONS)
}

fn bench_profile() -> BenchProfile {
    bench_profile_for(env::var("KACRAB_BENCH_PROFILE").ok().as_deref())
}

fn bench_profile_for(value: Option<&str>) -> BenchProfile {
    match value {
        Some("relaxed" | "throughput" | "throughput-relaxed") => BenchProfile::Relaxed,
        _ => BenchProfile::KafkaDefault,
    }
}

fn in_flight_override() -> Option<usize> {
    env_usize("KACRAB_IN_FLIGHT")
}

fn acks_override() -> Option<String> {
    env::var("KACRAB_ACKS").ok()
}

fn batch_size_override() -> Option<usize> {
    env_usize("KACRAB_BATCH_SIZE")
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

fn metrics_enabled() -> bool {
    env::var("KACRAB_ENABLE_METRICS").ok().as_deref() == Some("1")
}

fn latency_enabled() -> bool {
    env::var("KACRAB_ENABLE_LATENCY").ok().as_deref() == Some("1")
}

fn delivery_modes() -> Vec<DeliveryMode> {
    match env::var("KACRAB_DELIVERY_MODE")
        .unwrap_or_else(|_error| "untracked".to_owned())
        .as_str()
    {
        "tracked" | "callback" => vec![DeliveryMode::Tracked],
        "batch" | "batch-tracked" => vec![DeliveryMode::Batch],
        "both" => vec![DeliveryMode::Untracked, DeliveryMode::Tracked],
        "all" => vec![
            DeliveryMode::Untracked,
            DeliveryMode::Tracked,
            DeliveryMode::Batch,
        ],
        _ => vec![DeliveryMode::Untracked],
    }
}

fn display_delivery_modes(modes: &[DeliveryMode]) -> String {
    modes
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(",")
}

fn display_optional_usize(value: Option<usize>) -> String {
    value.map_or_else(|| "kafka-default".to_owned(), |value| value.to_string())
}

fn display_optional_str(value: Option<&str>) -> String {
    value.map_or_else(|| "kafka-default".to_owned(), ToOwned::to_owned)
}

fn tracked_delivery_window() -> usize {
    env_usize("KACRAB_TRACKED_DELIVERY_WINDOW")
        .unwrap_or(262_144)
        .max(1)
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

fn payload_file_size() -> Option<usize> {
    let path = env::var("KACRAB_PAYLOAD_FILE").ok()?;
    let bytes = std::fs::metadata(path).ok()?.len();
    usize::try_from(bytes).ok().filter(|bytes| *bytes > 0)
}

fn payload_value(default_size: usize) -> Bytes {
    if let Ok(path) = env::var("KACRAB_PAYLOAD_FILE") {
        let bytes = std::fs::read(path).expect("KACRAB_PAYLOAD_FILE must be readable");
        assert!(!bytes.is_empty(), "KACRAB_PAYLOAD_FILE must not be empty");
        return Bytes::from(bytes);
    }
    Bytes::from(vec![b'x'; default_size])
}

fn format_count(value: usize) -> String {
    let digits = value.to_string();
    let mut formatted = String::with_capacity(digits.len().saturating_add(digits.len() / 3));
    for (index, ch) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index).checked_rem(3).unwrap_or(0) == 0 {
            formatted.push(',');
        }
        formatted.push(ch);
    }
    formatted
}

fn batch_buffer_memory(batch_messages: usize, value_size: usize) -> usize {
    batch_messages
        .checked_mul(value_size.saturating_add(128))
        .and_then(|bytes| bytes.checked_add(1024 * 1024))
        .expect("scenario buffer memory should not overflow")
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct LatencySummary {
    samples: usize,
    avg_ms: f64,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    p999_ms: f64,
    max_ms: f64,
}

fn latency_summary<I>(samples: I) -> Option<LatencySummary>
where
    I: IntoIterator<Item = Duration>,
{
    let mut samples: Vec<_> = samples.into_iter().collect();
    if samples.is_empty() {
        return None;
    }
    samples.sort_unstable();
    let total_ms: f64 = samples.iter().copied().map(duration_ms).sum();
    let sample_count = samples.len();
    let avg_ms = total_ms / f64::from(u32::try_from(sample_count).ok()?);
    let max_ms = duration_ms(*samples.last()?);
    Some(LatencySummary {
        samples: sample_count,
        avg_ms,
        p50_ms: percentile_ms(&samples, 500),
        p95_ms: percentile_ms(&samples, 950),
        p99_ms: percentile_ms(&samples, 990),
        p999_ms: percentile_ms(&samples, 999),
        max_ms,
    })
}

fn percentile_ms(samples: &[Duration], per_mille: usize) -> f64 {
    let len = samples.len();
    let rank = per_mille
        .checked_mul(len)
        .and_then(|scaled| scaled.checked_add(999))
        .map_or(len, |scaled| scaled / 1000);
    let index = rank.saturating_sub(1).min(len.saturating_sub(1));
    duration_ms(samples[index])
}

fn duration_ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}

const fn metrics_delta(
    current: ProducerMetricsSnapshot,
    baseline: ProducerMetricsSnapshot,
) -> ProducerMetricsSnapshot {
    ProducerMetricsSnapshot {
        records_appended: current
            .records_appended
            .saturating_sub(baseline.records_appended),
        produce_request_count: current
            .produce_request_count
            .saturating_sub(baseline.produce_request_count),
        produce_record_count: current
            .produce_record_count
            .saturating_sub(baseline.produce_record_count),
        produce_retry_count: current
            .produce_retry_count
            .saturating_sub(baseline.produce_retry_count),
        produce_error_count: current
            .produce_error_count
            .saturating_sub(baseline.produce_error_count),
        requeue_count: current.requeue_count.saturating_sub(baseline.requeue_count),
        queue_depth_bytes: current.queue_depth_bytes,
        queue_depth_records: current.queue_depth_records,
        in_flight_dispatches: current.in_flight_dispatches,
        average_batch_fill_ratio: current.average_batch_fill_ratio,
        flush_count: current.flush_count.saturating_sub(baseline.flush_count),
        flush_total_latency: current
            .flush_total_latency
            .saturating_sub(baseline.flush_total_latency),
        metadata_wait_count: current
            .metadata_wait_count
            .saturating_sub(baseline.metadata_wait_count),
        metadata_wait_total_latency: current
            .metadata_wait_total_latency
            .saturating_sub(baseline.metadata_wait_total_latency),
        transaction_init_count: current
            .transaction_init_count
            .saturating_sub(baseline.transaction_init_count),
        transaction_init_total_latency: current
            .transaction_init_total_latency
            .saturating_sub(baseline.transaction_init_total_latency),
        transaction_begin_count: current
            .transaction_begin_count
            .saturating_sub(baseline.transaction_begin_count),
        transaction_begin_total_latency: current
            .transaction_begin_total_latency
            .saturating_sub(baseline.transaction_begin_total_latency),
        send_offsets_to_transaction_count: current
            .send_offsets_to_transaction_count
            .saturating_sub(baseline.send_offsets_to_transaction_count),
        send_offsets_to_transaction_total_latency: current
            .send_offsets_to_transaction_total_latency
            .saturating_sub(baseline.send_offsets_to_transaction_total_latency),
        transaction_commit_count: current
            .transaction_commit_count
            .saturating_sub(baseline.transaction_commit_count),
        transaction_commit_total_latency: current
            .transaction_commit_total_latency
            .saturating_sub(baseline.transaction_commit_total_latency),
        transaction_abort_count: current
            .transaction_abort_count
            .saturating_sub(baseline.transaction_abort_count),
        transaction_abort_total_latency: current
            .transaction_abort_total_latency
            .saturating_sub(baseline.transaction_abort_total_latency),
    }
}

#[derive(Debug, Clone, Copy)]
struct BenchmarkResult<'a> {
    scenario: &'a Scenario,
    value_size: usize,
    outer_chunks: usize,
    latency: Option<LatencySummary>,
    metrics: ProducerMetricsSnapshot,
    metrics_enabled: bool,
    delivery_mode: DeliveryMode,
    elapsed: Duration,
}

fn print_result(result: &BenchmarkResult<'_>) {
    let scenario = result.scenario;
    let value_size = result.value_size;
    let outer_chunks = result.outer_chunks;
    let latency = result.latency;
    let metrics = result.metrics;
    let metrics_enabled = result.metrics_enabled;
    let delivery_mode = result.delivery_mode;
    let elapsed = result.elapsed;
    let seconds = elapsed.as_secs_f64();
    let messages_u32 =
        u32::try_from(scenario.messages).expect("scenario message count should fit in u32");
    let messages_per_second = f64::from(messages_u32) / seconds;
    let megabytes = scenario
        .messages
        .checked_mul(value_size)
        .and_then(|bytes| u32::try_from(bytes).ok())
        .map(|bytes| f64::from(bytes) / (1024.0 * 1024.0))
        .expect("scenario bytes should not overflow");
    let megabytes_per_second = megabytes / seconds;
    if let Some(latency) = latency {
        if metrics_enabled {
            println!(
                "{} [{}]: {:.0} messages/s, {:.3} MiB/s ({:.3}s, {} produce requests, latency \
                 samples={}, avg={:.2} ms, p50={:.2} ms, p95={:.2} ms, p99={:.2} ms, p999={:.2} \
                 ms, max={:.2} ms, broker_requests={}, records={}, retries={}, errors={}, \
                 requeues={}, batch_fill={:.3})",
                scenario.name,
                delivery_mode,
                messages_per_second,
                megabytes_per_second,
                seconds,
                outer_chunks,
                latency.samples,
                latency.avg_ms,
                latency.p50_ms,
                latency.p95_ms,
                latency.p99_ms,
                latency.p999_ms,
                latency.max_ms,
                metrics.produce_request_count,
                metrics.produce_record_count,
                metrics.produce_retry_count,
                metrics.produce_error_count,
                metrics.requeue_count,
                metrics.average_batch_fill_ratio
            );
            return;
        }
        println!(
            "{} [{}]: {:.0} messages/s, {:.3} MiB/s ({:.3}s, {} produce requests, latency \
             samples={}, avg={:.2} ms, p50={:.2} ms, p95={:.2} ms, p99={:.2} ms, p999={:.2} ms, \
             max={:.2} ms)",
            scenario.name,
            delivery_mode,
            messages_per_second,
            megabytes_per_second,
            seconds,
            outer_chunks,
            latency.samples,
            latency.avg_ms,
            latency.p50_ms,
            latency.p95_ms,
            latency.p99_ms,
            latency.p999_ms,
            latency.max_ms
        );
    } else {
        if metrics_enabled {
            println!(
                "{} [{}]: {:.0} messages/s, {:.3} MiB/s ({:.3}s, {} produce requests, \
                 broker_requests={}, records={}, retries={}, errors={}, requeues={}, \
                 batch_fill={:.3})",
                scenario.name,
                delivery_mode,
                messages_per_second,
                megabytes_per_second,
                seconds,
                outer_chunks,
                metrics.produce_request_count,
                metrics.produce_record_count,
                metrics.produce_retry_count,
                metrics.produce_error_count,
                metrics.requeue_count,
                metrics.average_batch_fill_ratio
            );
            return;
        }
        println!(
            "{} [{}]: {:.0} messages/s, {:.3} MiB/s ({:.3}s, {} produce requests)",
            scenario.name,
            delivery_mode,
            messages_per_second,
            megabytes_per_second,
            seconds,
            outer_chunks
        );
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{
        BenchProfile, ScenarioSelection, bench_profile_for, latency_summary,
        scenarios_for_selection,
    };

    #[test]
    fn scenario_selection_can_run_large_payload_only() {
        let scenarios = scenarios_for_selection(ScenarioSelection {
            only_10b: false,
            only_10kib: true,
            smoke: false,
            custom_messages: None,
            custom_value_size: None,
            custom_batch_messages: None,
            payload_file_size: None,
            batch_messages_10b: None,
            batch_messages_10kib: Some(192),
        });

        assert_eq!(scenarios.len(), 1);
        assert_eq!(scenarios[0].name, "100,000 messages x 10 KiB");
        assert_eq!(scenarios[0].messages, 100_000);
        assert_eq!(scenarios[0].value_size, 10 * 1024);
        assert_eq!(scenarios[0].batch_messages, 192);
    }

    #[test]
    fn scenario_selection_defaults_large_payload_to_baseline_outer_chunks() {
        let scenarios = scenarios_for_selection(ScenarioSelection {
            only_10b: false,
            only_10kib: true,
            smoke: false,
            custom_messages: None,
            custom_value_size: None,
            custom_batch_messages: None,
            payload_file_size: None,
            batch_messages_10b: None,
            batch_messages_10kib: None,
        });

        assert_eq!(scenarios.len(), 1);
        assert_eq!(scenarios[0].batch_messages, 96);
    }

    #[test]
    fn scenario_selection_can_run_custom_payload_profile() {
        let scenarios = scenarios_for_selection(ScenarioSelection {
            only_10b: false,
            only_10kib: false,
            smoke: false,
            custom_messages: Some(250_000),
            custom_value_size: Some(512),
            custom_batch_messages: Some(2048),
            payload_file_size: None,
            batch_messages_10b: None,
            batch_messages_10kib: None,
        });

        assert_eq!(scenarios.len(), 1);
        assert_eq!(scenarios[0].name, "custom: 250,000 messages x 512 bytes");
        assert_eq!(scenarios[0].messages, 250_000);
        assert_eq!(scenarios[0].value_size, 512);
        assert_eq!(scenarios[0].batch_messages, 2048);
    }

    #[test]
    fn bench_profile_defaults_to_kafka_defaults() {
        assert_eq!(bench_profile_for(None), BenchProfile::KafkaDefault);
        assert_eq!(
            bench_profile_for(Some("unknown")),
            BenchProfile::KafkaDefault
        );
    }

    #[test]
    fn bench_profile_relaxed_is_explicit_opt_in() {
        assert_eq!(bench_profile_for(Some("relaxed")), BenchProfile::Relaxed);
        assert_eq!(bench_profile_for(Some("throughput")), BenchProfile::Relaxed);
    }

    #[test]
    fn latency_summary_reports_nearest_rank_percentiles() {
        let summary = latency_summary([
            Duration::from_millis(5),
            Duration::from_millis(1),
            Duration::from_millis(3),
            Duration::from_millis(2),
            Duration::from_millis(4),
        ])
        .expect("latency summary");

        assert_eq!(summary.samples, 5);
        assert_float_eq(summary.avg_ms, 3.0);
        assert_float_eq(summary.p50_ms, 3.0);
        assert_float_eq(summary.p95_ms, 5.0);
        assert_float_eq(summary.p99_ms, 5.0);
        assert_float_eq(summary.p999_ms, 5.0);
        assert_float_eq(summary.max_ms, 5.0);
    }

    fn assert_float_eq(actual: f64, expected: f64) {
        assert!((actual - expected).abs() < f64::EPSILON);
    }
}
