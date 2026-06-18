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
    fmt::Write as _,
    net::SocketAddr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicI32, AtomicI64, AtomicU64, AtomicUsize, Ordering},
    },
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
        let reporting_interval = reporting_interval();
        println!(
            "real Kafka benchmark: bootstrap={bootstrap}, topic={topic}, partitions={partitions}, \
             profile={profile}, in_flight={}, acks={}, batch_size={}, \
             partition_mode={partition_mode}, delivery_modes={}, \
             tracked_delivery_window={tracked_delivery_window}, reporting_interval_ms={}",
            display_optional_usize(in_flight),
            display_optional_str(acks.as_deref()),
            display_optional_usize(batch_size),
            display_delivery_modes(&delivery_modes),
            reporting_interval.as_millis()
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
                    reporting_interval,
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
    reporting_interval: Duration,
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
        java_perf: send.java_perf,
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
    java_perf: Option<ProducerPerformanceSummary>,
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
        java_perf: None,
    }
}

async fn run_callback_tracked_send_loop(
    producer: &mut Producer,
    run: &BenchmarkRun<'_>,
    value: Bytes,
) -> SendLoopResult {
    let topic = Arc::<str>::from(run.topic);
    let started = Instant::now();
    let java_perf = ProducerPerformanceStatsHandle::new(ProducerPerformanceStats::new(
        run.scenario.messages,
        run.reporting_interval,
        false,
    ));
    let mut sent = 0usize;
    let mut pending_since_flush = 0usize;
    while sent < run.scenario.messages {
        let send_started = Instant::now();
        let stats = java_perf.clone();
        let value_size = value.len();
        let _delivery = producer
            .send_with_callback(
                benchmark_record(Arc::clone(&topic), sent, run.partitions, run.partition_mode)
                    .value(value.clone()),
                move |result| {
                    if result.is_ok() {
                        if let Some(line) =
                            stats.record_completion(send_started, Instant::now(), value_size)
                        {
                            println!("{line}");
                        }
                    } else {
                        eprintln!("producer callback reported delivery error: {result:?}");
                    }
                },
            )
            .await
            .expect("benchmark tracked callback send should fit and dispatch");
        pending_since_flush = pending_since_flush.saturating_add(1);
        if pending_since_flush >= run.tracked_delivery_window {
            producer
                .flush()
                .await
                .expect("benchmark tracked flush should succeed");
            pending_since_flush = 0;
        }
        sent = sent.saturating_add(1);
    }
    producer
        .flush()
        .await
        .expect("benchmark tracked final flush should succeed");
    let elapsed = started.elapsed();
    let java_perf = java_perf.summary(elapsed);
    SendLoopResult {
        outer_chunks: sent,
        elapsed,
        java_perf: Some(java_perf),
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
        java_perf: None,
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
                let _delivery = producer
                    .send_with_callback(
                        benchmark_record(
                            Arc::clone(&topic),
                            index,
                            run.partitions,
                            run.partition_mode,
                        )
                        .value(value.clone()),
                        |_result| {},
                    )
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
        .unwrap_or(usize::MAX)
        .max(1)
}

fn reporting_interval() -> Duration {
    Duration::from_millis(env_usize("KACRAB_REPORTING_INTERVAL_MS").unwrap_or(5_000) as u64)
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

const JAVA_LATENCY_SAMPLE_CAP: usize = 500_000;

#[derive(Debug, Clone)]
struct ProducerPerformanceStatsHandle {
    inner: Arc<ProducerPerformanceStats>,
}

impl ProducerPerformanceStatsHandle {
    fn new(stats: ProducerPerformanceStats) -> Self {
        Self {
            inner: Arc::new(stats),
        }
    }

    fn record_completion(
        &self,
        started: Instant,
        completed: Instant,
        value_size: usize,
    ) -> Option<String> {
        self.inner.record_completion(started, completed, value_size)
    }

    fn summary(&self, elapsed: Duration) -> ProducerPerformanceSummary {
        self.inner.summary(elapsed)
    }
}

#[derive(Debug)]
struct ProducerPerformanceStats {
    start: Instant,
    latencies: Vec<AtomicI32>,
    sampling: usize,
    reporting_interval: Duration,
    iteration: AtomicUsize,
    index: AtomicUsize,
    count: AtomicUsize,
    bytes: AtomicUsize,
    max_latency: AtomicI32,
    total_latency: AtomicI64,
    window_count: AtomicUsize,
    window_max_latency: AtomicI32,
    window_total_latency: AtomicI64,
    window_bytes: AtomicUsize,
    window_start_ms: AtomicU64,
    window_report_lock: Mutex<()>,
    is_steady_state: bool,
    suppress_print: bool,
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct ProducerPerformanceSummary {
    records: usize,
    bytes: usize,
    samples: usize,
    elapsed: Duration,
    records_per_second: f64,
    mebibytes_per_second: f64,
    avg_ms: f64,
    max_ms: i32,
    p50_ms: i32,
    p95_ms: i32,
    p99_ms: i32,
    p999_ms: i32,
}

impl ProducerPerformanceStats {
    fn new(num_records: usize, reporting_interval: Duration, is_steady_state: bool) -> Self {
        let now = Instant::now();
        let sampling = num_records / num_records.min(JAVA_LATENCY_SAMPLE_CAP);
        let sample_slots = (num_records / sampling).saturating_add(1);
        Self {
            start: now,
            latencies: (0..sample_slots).map(|_slot| AtomicI32::new(0)).collect(),
            sampling,
            reporting_interval,
            iteration: AtomicUsize::new(0),
            index: AtomicUsize::new(0),
            count: AtomicUsize::new(0),
            bytes: AtomicUsize::new(0),
            max_latency: AtomicI32::new(0),
            total_latency: AtomicI64::new(0),
            window_count: AtomicUsize::new(0),
            window_max_latency: AtomicI32::new(0),
            window_total_latency: AtomicI64::new(0),
            window_bytes: AtomicUsize::new(0),
            window_start_ms: AtomicU64::new(0),
            window_report_lock: Mutex::new(()),
            is_steady_state,
            suppress_print: false,
        }
    }

    fn record_completion(
        &self,
        started: Instant,
        completed: Instant,
        value_size: usize,
    ) -> Option<String> {
        let latency = completed.saturating_duration_since(started);
        let latency = i32::try_from(latency.as_millis()).unwrap_or(i32::MAX);
        let count = self.count.fetch_add(1, Ordering::Relaxed).saturating_add(1);
        let _previous = self.bytes.fetch_add(value_size, Ordering::Relaxed);
        let _previous = self
            .total_latency
            .fetch_add(i64::from(latency), Ordering::Relaxed);
        let _previous = self.max_latency.fetch_max(latency, Ordering::Relaxed);
        let window_count = self
            .window_count
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        let _previous = self.window_bytes.fetch_add(value_size, Ordering::Relaxed);
        let _previous = self
            .window_total_latency
            .fetch_add(i64::from(latency), Ordering::Relaxed);
        let _previous = self
            .window_max_latency
            .fetch_max(latency, Ordering::Relaxed);
        let iteration = self.iteration.fetch_add(1, Ordering::Relaxed);
        if iteration.checked_rem(self.sampling).unwrap_or(0) == 0 {
            let index = self.index.fetch_add(1, Ordering::Relaxed);
            if let Some(sample) = self.latencies.get(index) {
                sample.store(latency, Ordering::Relaxed);
            }
        }
        self.window_report(completed, count, window_count)
    }

    fn window_report(
        &self,
        completed: Instant,
        count: usize,
        window_count: usize,
    ) -> Option<String> {
        let now_ms = elapsed_millis_since(self.start, completed);
        let window_start_ms = self.window_start_ms.load(Ordering::Relaxed);
        if now_ms.saturating_sub(window_start_ms) < duration_millis(self.reporting_interval) {
            return None;
        }
        let _guard = self
            .window_report_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let window_start_ms = self.window_start_ms.load(Ordering::Relaxed);
        if now_ms.saturating_sub(window_start_ms) < duration_millis(self.reporting_interval) {
            return None;
        }
        let window_bytes = self.window_bytes.swap(0, Ordering::Relaxed);
        let window_total_latency = self.window_total_latency.swap(0, Ordering::Relaxed);
        let window_max_latency = self.window_max_latency.swap(0, Ordering::Relaxed);
        let window_count = self
            .window_count
            .swap(0, Ordering::Relaxed)
            .max(window_count);
        self.window_start_ms.store(now_ms, Ordering::Relaxed);

        let mut lines = Vec::with_capacity(2);
        if self.is_steady_state && count == window_count {
            lines.push("In steady state.".to_owned());
        }
        if !self.suppress_print {
            lines.push(Self::window_line(
                window_count,
                window_bytes,
                window_total_latency,
                window_max_latency,
                now_ms.saturating_sub(window_start_ms),
            ));
        }
        let line = lines.join("\n");
        (!line.is_empty()).then_some(line)
    }

    fn window_line(
        window_count: usize,
        window_bytes: usize,
        window_total_latency: i64,
        window_max_latency: i32,
        elapsed_ms: u64,
    ) -> String {
        let elapsed_ms = elapsed_ms.max(1);
        let elapsed_ms_f64 = f64_from_u64(elapsed_ms);
        let records_per_second = 1000.0 * f64_from_usize(window_count) / elapsed_ms_f64;
        let mebibytes_per_second =
            1000.0 * f64_from_usize(window_bytes) / elapsed_ms_f64 / (1024.0 * 1024.0);
        format!(
            "{} records sent, {:.1} records/sec ({:.2} MB/sec), {:.1} ms avg latency, {:.1} ms \
             max latency.",
            window_count,
            records_per_second,
            mebibytes_per_second,
            f64_from_i64(window_total_latency) / f64_from_usize(window_count.max(1)),
            f64::from(window_max_latency)
        )
    }

    fn summary(&self, _elapsed: Duration) -> ProducerPerformanceSummary {
        let elapsed = self.start.elapsed();
        let elapsed_ms = u64::try_from(elapsed.as_millis())
            .unwrap_or(u64::MAX)
            .max(1);
        let elapsed_ms_f64 = f64_from_u64(elapsed_ms);
        let count = self.count.load(Ordering::Relaxed);
        let bytes = self.bytes.load(Ordering::Relaxed);
        let total_latency = self.total_latency.load(Ordering::Relaxed);
        let max_latency = self.max_latency.load(Ordering::Relaxed);
        let samples = self.index.load(Ordering::Relaxed).min(self.latencies.len());
        let records_per_second = 1000.0 * f64_from_usize(count) / elapsed_ms_f64;
        let mebibytes_per_second =
            1000.0 * f64_from_usize(bytes) / elapsed_ms_f64 / (1024.0 * 1024.0);
        let percentile_values = self.percentiles();
        ProducerPerformanceSummary {
            records: count,
            bytes,
            samples,
            elapsed,
            records_per_second,
            mebibytes_per_second,
            avg_ms: f64_from_i64(total_latency) / f64_from_usize(count.max(1)),
            max_ms: max_latency,
            p50_ms: percentile_values[0],
            p95_ms: percentile_values[1],
            p99_ms: percentile_values[2],
            p999_ms: percentile_values[3],
        }
    }

    fn percentiles(&self) -> [i32; 4] {
        let size = self.index.load(Ordering::Relaxed).min(self.latencies.len());
        if size == 0 {
            return [0; 4];
        }
        let mut latencies = self
            .latencies
            .iter()
            .take(size)
            .map(|latency| latency.load(Ordering::Relaxed))
            .collect::<Vec<_>>();
        latencies[..size].sort_unstable();
        [
            percentile_latency(&latencies, size, 500),
            percentile_latency(&latencies, size, 950),
            percentile_latency(&latencies, size, 990),
            percentile_latency(&latencies, size, 999),
        ]
    }
}

fn percentile_latency(latencies: &[i32], size: usize, per_mille: usize) -> i32 {
    let index = per_mille
        .checked_mul(size)
        .map_or_else(|| size.saturating_sub(1), |scaled| scaled / 1000)
        .min(size.saturating_sub(1));
    latencies[index]
}

fn elapsed_millis_since(start: Instant, completed: Instant) -> u64 {
    u64::try_from(completed.saturating_duration_since(start).as_millis()).unwrap_or(u64::MAX)
}

fn duration_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn f64_from_usize(value: usize) -> f64 {
    f64::from(u32::try_from(value).unwrap_or(u32::MAX))
}

fn f64_from_u64(value: u64) -> f64 {
    f64::from(u32::try_from(value).unwrap_or(u32::MAX))
}

fn f64_from_i64(value: i64) -> f64 {
    f64::from(i32::try_from(value).unwrap_or_else(|_error| {
        if value.is_negative() {
            i32::MIN
        } else {
            i32::MAX
        }
    }))
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
    java_perf: Option<ProducerPerformanceSummary>,
    metrics: ProducerMetricsSnapshot,
    metrics_enabled: bool,
    delivery_mode: DeliveryMode,
    elapsed: Duration,
}

fn print_result(result: &BenchmarkResult<'_>) {
    println!("{}", format_result_line(result));
}

fn format_result_line(result: &BenchmarkResult<'_>) -> String {
    if let Some(java_perf) = result.java_perf {
        return format_java_perf_result_line(result, java_perf);
    }
    let (messages_per_second, megabytes_per_second) = scenario_throughput(result);
    if let Some(latency) = result.latency {
        return format_dispatch_latency_result_line(
            result,
            latency,
            messages_per_second,
            megabytes_per_second,
        );
    }
    format_throughput_result_line(result, messages_per_second, megabytes_per_second)
}

fn scenario_throughput(result: &BenchmarkResult<'_>) -> (f64, f64) {
    let seconds = result.elapsed.as_secs_f64();
    let messages = f64::from(
        u32::try_from(result.scenario.messages).expect("scenario message count should fit in u32"),
    );
    let megabytes = result
        .scenario
        .messages
        .checked_mul(result.value_size)
        .and_then(|bytes| u32::try_from(bytes).ok())
        .map(|bytes| f64::from(bytes) / (1024.0 * 1024.0))
        .expect("scenario bytes should not overflow");
    (messages / seconds, megabytes / seconds)
}

fn format_java_perf_result_line(
    result: &BenchmarkResult<'_>,
    java_perf: ProducerPerformanceSummary,
) -> String {
    let mut line = format!(
        "{} records sent, {:.6} records/sec ({:.2} MB/sec), {:.2} ms avg latency, {:.2} ms max \
         latency, {} ms 50th, {} ms 95th, {} ms 99th, {} ms 99.9th.",
        java_perf.records,
        java_perf.records_per_second,
        java_perf.mebibytes_per_second,
        java_perf.avg_ms,
        f64::from(java_perf.max_ms),
        java_perf.p50_ms,
        java_perf.p95_ms,
        java_perf.p99_ms,
        java_perf.p999_ms
    );
    if result.metrics_enabled {
        line.push_str(" (");
        append_metrics(&mut line, result.metrics);
        line.push(')');
    }
    line
}

fn format_dispatch_latency_result_line(
    result: &BenchmarkResult<'_>,
    latency: LatencySummary,
    messages_per_second: f64,
    megabytes_per_second: f64,
) -> String {
    if result.metrics_enabled {
        return format!(
            "{} [{}]: {:.0} messages/s, {:.3} MiB/s ({:.3}s, api_chunks={}, \
             dispatch_latency_samples={}, dispatch_latency_avg={:.2} ms, \
             dispatch_latency_p50={:.2} ms, dispatch_latency_p95={:.2} ms, \
             dispatch_latency_p99={:.2} ms, dispatch_latency_p999={:.2} ms, \
             dispatch_latency_max={:.2} ms, broker_requests={}, records={}, retries={}, \
             errors={}, requeues={}, batch_fill={:.3})",
            result.scenario.name,
            result.delivery_mode,
            messages_per_second,
            megabytes_per_second,
            result.elapsed.as_secs_f64(),
            result.outer_chunks,
            latency.samples,
            latency.avg_ms,
            latency.p50_ms,
            latency.p95_ms,
            latency.p99_ms,
            latency.p999_ms,
            latency.max_ms,
            result.metrics.produce_request_count,
            result.metrics.produce_record_count,
            result.metrics.produce_retry_count,
            result.metrics.produce_error_count,
            result.metrics.requeue_count,
            result.metrics.average_batch_fill_ratio
        );
    }
    format!(
        "{} [{}]: {:.0} messages/s, {:.3} MiB/s ({:.3}s, api_chunks={}, \
         dispatch_latency_samples={}, dispatch_latency_avg={:.2} ms, dispatch_latency_p50={:.2} \
         ms, dispatch_latency_p95={:.2} ms, dispatch_latency_p99={:.2} ms, \
         dispatch_latency_p999={:.2} ms, dispatch_latency_max={:.2} ms)",
        result.scenario.name,
        result.delivery_mode,
        messages_per_second,
        megabytes_per_second,
        result.elapsed.as_secs_f64(),
        result.outer_chunks,
        latency.samples,
        latency.avg_ms,
        latency.p50_ms,
        latency.p95_ms,
        latency.p99_ms,
        latency.p999_ms,
        latency.max_ms
    )
}

fn format_throughput_result_line(
    result: &BenchmarkResult<'_>,
    messages_per_second: f64,
    megabytes_per_second: f64,
) -> String {
    if result.metrics_enabled {
        return format!(
            "{} [{}]: {:.0} messages/s, {:.3} MiB/s ({:.3}s, api_chunks={}, broker_requests={}, \
             records={}, retries={}, errors={}, requeues={}, batch_fill={:.3})",
            result.scenario.name,
            result.delivery_mode,
            messages_per_second,
            megabytes_per_second,
            result.elapsed.as_secs_f64(),
            result.outer_chunks,
            result.metrics.produce_request_count,
            result.metrics.produce_record_count,
            result.metrics.produce_retry_count,
            result.metrics.produce_error_count,
            result.metrics.requeue_count,
            result.metrics.average_batch_fill_ratio
        );
    }
    format!(
        "{} [{}]: {:.0} messages/s, {:.3} MiB/s ({:.3}s, api_chunks={})",
        result.scenario.name,
        result.delivery_mode,
        messages_per_second,
        megabytes_per_second,
        result.elapsed.as_secs_f64(),
        result.outer_chunks
    )
}

fn append_metrics(line: &mut String, metrics: ProducerMetricsSnapshot) {
    let _result = write!(
        line,
        "broker_requests={}, records={}, retries={}, errors={}, requeues={}, batch_fill={:.3}",
        metrics.produce_request_count,
        metrics.produce_record_count,
        metrics.produce_retry_count,
        metrics.produce_error_count,
        metrics.requeue_count,
        metrics.average_batch_fill_ratio
    );
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::{
        BenchProfile, BenchmarkResult, DeliveryMode, LatencySummary, ProducerMetricsSnapshot,
        ProducerPerformanceStats, Scenario, ScenarioSelection, bench_profile_for,
        format_result_line, latency_summary, scenarios_for_selection,
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

    #[test]
    fn formatted_result_names_rust_latency_as_dispatch_latency() {
        let scenario = Scenario {
            name: "test scenario".to_owned(),
            messages: 1_000,
            value_size: 10,
            batch_messages: 100,
        };
        let line = format_result_line(&BenchmarkResult {
            scenario: &scenario,
            value_size: 10,
            outer_chunks: 10,
            latency: Some(LatencySummary {
                samples: 4,
                avg_ms: 1.0,
                p50_ms: 1.0,
                p95_ms: 2.0,
                p99_ms: 3.0,
                p999_ms: 4.0,
                max_ms: 5.0,
            }),
            java_perf: None,
            metrics: empty_metrics(),
            metrics_enabled: false,
            delivery_mode: DeliveryMode::Untracked,
            elapsed: Duration::from_secs(1),
        });

        assert!(line.contains("api_chunks=10"));
        assert!(line.contains("dispatch_latency_samples=4"));
        assert!(line.contains("dispatch_latency_avg=1.00 ms"));
        assert!(!line.contains("latency samples="));
    }

    #[test]
    fn tracked_result_reports_java_producer_performance_total_line() {
        let scenario = Scenario {
            name: "tracked scenario".to_owned(),
            messages: 1_000,
            value_size: 10,
            batch_messages: 100,
        };
        let stats = ProducerPerformanceStats::new(1_000, Duration::from_secs(5), false);
        let started = Instant::now();
        let _report = stats.record_completion(started, started + Duration::from_millis(5), 10);
        let _report = stats.record_completion(started, started + Duration::from_millis(1), 10);
        let line = format_result_line(&BenchmarkResult {
            scenario: &scenario,
            value_size: 10,
            outer_chunks: 1_000,
            latency: None,
            java_perf: Some(stats.summary(Duration::from_secs(1))),
            metrics: empty_metrics(),
            metrics_enabled: false,
            delivery_mode: DeliveryMode::Tracked,
            elapsed: Duration::from_secs(1),
        });

        assert!(line.starts_with("2 records sent, "));
        assert!(line.contains("3.00 ms avg latency"));
        assert!(line.contains("5.00 ms max latency"));
        assert!(line.contains("5 ms 50th"));
        assert!(line.contains("5 ms 95th"));
        assert!(line.contains("5 ms 99th"));
        assert!(line.contains("5 ms 99.9th."));
        assert!(!line.contains("dispatch_latency"));
    }

    const fn empty_metrics() -> ProducerMetricsSnapshot {
        ProducerMetricsSnapshot {
            records_appended: 0,
            produce_request_count: 0,
            produce_record_count: 0,
            produce_retry_count: 0,
            produce_error_count: 0,
            requeue_count: 0,
            queue_depth_bytes: 0,
            queue_depth_records: 0,
            in_flight_dispatches: 0,
            average_batch_fill_ratio: 0.0,
            flush_count: 0,
            flush_total_latency: Duration::ZERO,
            metadata_wait_count: 0,
            metadata_wait_total_latency: Duration::ZERO,
            transaction_init_count: 0,
            transaction_init_total_latency: Duration::ZERO,
            transaction_begin_count: 0,
            transaction_begin_total_latency: Duration::ZERO,
            send_offsets_to_transaction_count: 0,
            send_offsets_to_transaction_total_latency: Duration::ZERO,
            transaction_commit_count: 0,
            transaction_commit_total_latency: Duration::ZERO,
            transaction_abort_count: 0,
            transaction_abort_total_latency: Duration::ZERO,
        }
    }

    fn assert_float_eq(actual: f64, expected: f64) {
        assert!((actual - expected).abs() < f64::EPSILON);
    }
}
