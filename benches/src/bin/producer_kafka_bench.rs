//! Real Kafka producer benchmark through the public producer API.

#![allow(
    clippy::arithmetic_side_effects,
    clippy::cast_possible_truncation,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
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
use kacrab::{
    config::{ClientConfig, ProducerConfig},
    producer::{Producer, ProducerError, ProducerMetricsSnapshot, ProducerRecord, RecordMetadata},
    wire::WireError,
};
use tokio::runtime::Builder;

const BENCH_MESSAGES: usize = 5_000_000;
const LARGE_BENCH_MESSAGES: usize = 100_000;
const SMALL_VALUE_SIZE: usize = 10;
const LARGE_VALUE_SIZE: usize = 10 * 1024;
const TRACKED_API_CHUNK_RECORDS: usize = 16_384;
const BENCH_RUNS: usize = 5;

fn main() {
    // Default to a multi-thread runtime so the background sender + in-flight
    // produce tasks can run concurrently with the send loop (better pipelining).
    // Set KACRAB_BENCH_CURRENT_THREAD=1 to force the old single-thread runtime.
    let current_thread = env::var("KACRAB_BENCH_CURRENT_THREAD")
        .is_ok_and(|value| value == "1" || value.eq_ignore_ascii_case("true"));
    let runtime = if current_thread {
        Builder::new_current_thread()
            .enable_io()
            .enable_time()
            .build()
            .expect("benchmark runtime")
    } else {
        let workers = env::var("KACRAB_BENCH_WORKERS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(4);
        Builder::new_multi_thread()
            .worker_threads(workers)
            .enable_io()
            .enable_time()
            .build()
            .expect("benchmark runtime")
    };
    runtime.block_on(async {
        let bootstrap = bootstrap_addr();
        let topic = topic();
        let scenarios = scenarios();
        let delivery_mode = benchmark_api();
        let reporting_interval = reporting_interval();
        println!(
            "real Kafka benchmark: bootstrap={bootstrap}, topic={topic}, \
             producer_config=kafka-defaults, delivery_mode={delivery_mode}, \
             reporting_interval_ms={}",
            reporting_interval.as_millis()
        );
        let runs = bench_runs();
        for scenario in scenarios {
            let mut summaries = Vec::with_capacity(runs);
            let mut metrics = Vec::with_capacity(runs);
            for run_index in 1..=runs {
                println!("scenario=\"{}\", run={run_index}/{runs}", scenario.name);
                let summary = run_scenario(BenchmarkRun {
                    bootstrap,
                    topic: &topic,
                    scenario: scenario.clone(),
                    delivery_mode,
                    reporting_interval,
                })
                .await;
                summaries.push(summary.java_perf);
                metrics.push(summary.metrics);
            }
            print_average_result(&scenario, &summaries);
            print_average_counters(&metrics);
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
    delivery_mode: DeliveryMode,
    reporting_interval: Duration,
}

#[derive(Debug, Clone, Copy)]
struct BenchmarkRunSummary {
    java_perf: ProducerPerformanceSummary,
    metrics: ProducerMetricsSnapshot,
}

async fn run_scenario(run: BenchmarkRun<'_>) -> BenchmarkRunSummary {
    let value = payload_value(run.scenario.value_size);
    let value_size = value.len();
    let producer_config = benchmark_producer_config(run.bootstrap);
    println!("{}", format_effective_config_snapshot(&producer_config));
    let mut producer = build_producer(&producer_config).await;
    if env::var("KACRAB_BENCH_NO_METRICS").is_err() {
        producer.enable_metrics();
    }
    warm_up_producer(&mut producer, &run, value.clone()).await;
    let warmup_metrics = producer.metrics();
    let concurrency = send_concurrency();
    let send = if concurrency > 1 {
        let (result, recovered) =
            run_per_record_tracked_send_loop_concurrent(producer, &run, value, concurrency).await;
        producer = recovered;
        result
    } else {
        run_per_record_tracked_send_loop(&mut producer, &run, value).await
    };
    let current_metrics = producer.metrics();
    let metrics = metrics_delta(&current_metrics, &warmup_metrics);
    let java_perf = send
        .java_perf
        .expect("tracked benchmark should produce Java-style stats");
    print_result(&BenchmarkResult {
        scenario: &run.scenario,
        value_size,
        elapsed: send.elapsed,
        outer_chunks: send.outer_chunks,
        latency: None,
        java_perf: Some(java_perf),
        metrics,
        metrics_enabled: true,
        delivery_mode: run.delivery_mode,
    });
    BenchmarkRunSummary { java_perf, metrics }
}

fn benchmark_client_config(bootstrap: SocketAddr) -> ClientConfig {
    ClientConfig::new()
        .set("bootstrap.servers", bootstrap.to_string())
        .set("client.id", "kacrab-producer-kafka-bench")
}

fn benchmark_producer_config(bootstrap: SocketAddr) -> ProducerConfig {
    benchmark_client_config(bootstrap)
        .producer_config()
        .expect("benchmark producer config should parse")
}

async fn build_producer(config: &ProducerConfig) -> Producer {
    let mut builder = Producer::builder()
        .set(
            "bootstrap.servers",
            config.bootstrap_servers.as_slice().join(","),
        )
        .set("client.id", config.client_id.as_str());
    // KACRAB_BENCH_BATCH_SIZE overrides batch.size to confirm whether throughput is
    // round-trip/pipelining bound (more records per request -> higher rate if so).
    if let Ok(batch_size) = env::var("KACRAB_BENCH_BATCH_SIZE") {
        builder = builder.set("batch.size", batch_size.as_str());
    }
    // KACRAB_BENCH_MAX_REQUEST_SIZE lifts the 1 MiB default so large-record runs
    // with a bigger batch.size do not trip RecordTooLarge on coalesced requests.
    if let Ok(max_request_size) = env::var("KACRAB_BENCH_MAX_REQUEST_SIZE") {
        builder = builder.set("max.request.size", max_request_size.as_str());
    }
    if env::var("KACRAB_BENCH_ACKS1").is_ok() {
        builder = builder.set("acks", "1").set("enable.idempotence", "false");
    }
    builder
        .build()
        .await
        .expect("benchmark producer config should build")
}

fn format_effective_config_snapshot(config: &ProducerConfig) -> String {
    let bootstrap = config.bootstrap_servers.as_slice().join(",");
    format!(
        "effective producer config: bootstrap.servers={bootstrap}, client.id={}, acks={}, \
         enable.idempotence={}, retries={}, max.in.flight.requests.per.connection={}, \
         batch.size={}, linger.ms={}, buffer.memory={}, compression.type={}, \
         delivery.timeout.ms={}, request.timeout.ms={}, max.block.ms={}, max.request.size={}, \
         send.buffer.bytes={}, receive.buffer.bytes={}, metadata.max.age.ms={}, \
         partitioner.adaptive.partitioning.enable={}, partitioner.availability.timeout.ms={}, \
         enable.metrics.push={}",
        config.client_id,
        config.acks,
        config.enable_idempotence,
        config.retries,
        config.max_in_flight_requests_per_connection,
        config.batch_size.get(),
        config.linger_ms.as_millis(),
        config.buffer_memory.get(),
        config.compression_type,
        config.delivery_timeout_ms.as_millis(),
        config.request_timeout_ms.as_millis(),
        config.max_block_ms.as_millis(),
        config.max_request_size.get(),
        config.send_buffer_bytes,
        config.receive_buffer_bytes,
        config.metadata_max_age_ms.as_millis(),
        config.partitioner_adaptive_partitioning_enable,
        config.partitioner_availability_timeout_ms.as_millis(),
        config.enable_metrics_push
    )
}

#[derive(Debug, Clone, Copy)]
struct SendLoopResult {
    outer_chunks: usize,
    elapsed: Duration,
    java_perf: Option<ProducerPerformanceSummary>,
}

async fn run_per_record_tracked_send_loop(
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
    // KACRAB_BENCH_SYNC_SEND=1 routes through the synchronous send
    // (send_with_callback_now, no per-record .await, no sender mutex). The record's
    // partition is assigned by the REAL sticky partitioner via try_assign_partition_now
    // (sync, non-blocking); the ~1-in-900 rotation records fall back to the async path.
    let sync_send = env::var("KACRAB_BENCH_SYNC_SEND").is_ok();
    let mut sent = 0usize;
    while sent < run.scenario.messages {
        let send_started = Instant::now();
        let stats = java_perf.clone();
        let value_size = value.len();
        let callback = move |result: kacrab::producer::Result<RecordMetadata>| {
            let completed = Instant::now();
            if let Some(line) = record_tracked_callback_completion(
                &result,
                TrackedCompletionStart::Single(send_started),
                &stats,
                completed,
                value_size,
            ) {
                println!("{line}");
            }
            if result.is_err() {
                eprintln!("producer callback reported delivery error: {result:?}");
            }
        };
        // `send_with_callback` is now synchronous (Java-style): it appends inline
        // when the partition resolves synchronously and only hands the rare record
        // (cold metadata / buffer-full) to the internal FIFO drain. No per-record
        // `.await`, no manual partition assignment.
        let record = benchmark_record(Arc::clone(&topic), sent).value(value.clone());
        match producer.send_with_callback(record, callback) {
            Ok(_delivery) => sent = sent.saturating_add(1),
            // Closed-loop backpressure. The producer buffer (Backpressure) or a
            // broker connection's in-flight queue (Wire(Backpressure)) is full —
            // common with large records, which fill the 32 MiB buffer long before
            // the drain catches up. Wait for the drain to free space and retry the
            // same record instead of flooding open-loop and panicking. This caps
            // the send rate at the real drain rate and keeps `send_started`
            // measuring service time, not an unbounded enqueue backlog.
            Err(ProducerError::Backpressure | ProducerError::Wire(WireError::Backpressure)) => {
                tokio::time::sleep(Duration::from_micros(50)).await;
            },
            Err(error) => panic!("benchmark send failed: {error:?}"),
        }
    }
    if sync_send {
        eprintln!(
            "sync-now buffer spins: {}",
            kacrab::producer::SYNC_NOW_BUFFER_SPINS.load(Ordering::Relaxed)
        );
    }
    producer
        .flush()
        .await
        .expect("benchmark per-record final flush should succeed");
    let elapsed = started.elapsed();
    let java_perf = java_perf.summary(elapsed);
    SendLoopResult {
        outer_chunks: sent,
        elapsed,
        java_perf: Some(java_perf),
    }
}

fn send_concurrency() -> usize {
    env::var("KACRAB_BENCH_SEND_CONCURRENCY")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(1)
}

/// Drive the per-record tracked send path from `concurrency` concurrent tasks
/// that share one `Producer` through `Arc`. This exercises the Java-style
/// thread-safe `send(&self)` surface to measure whether concurrent appends lift
/// the single-send-loop throughput ceiling.
async fn run_per_record_tracked_send_loop_concurrent(
    producer: Producer,
    run: &BenchmarkRun<'_>,
    value: Bytes,
    concurrency: usize,
) -> (SendLoopResult, Producer) {
    let producer = Arc::new(producer);
    let topic = Arc::<str>::from(run.topic);
    let total = run.scenario.messages;
    let per_task = total.div_ceil(concurrency);
    let started = Instant::now();
    let java_perf = ProducerPerformanceStatsHandle::new(ProducerPerformanceStats::new(
        total,
        run.reporting_interval,
        false,
    ));
    let mut handles = Vec::with_capacity(concurrency);
    for task in 0..concurrency {
        let start_index = task.saturating_mul(per_task);
        let end_index = start_index.saturating_add(per_task).min(total);
        if start_index >= end_index {
            break;
        }
        let producer = Arc::clone(&producer);
        let topic = Arc::clone(&topic);
        let value = value.clone();
        let java_perf = java_perf.clone();
        handles.push(tokio::spawn(async move {
            let value_size = value.len();
            for index in start_index..end_index {
                let send_started = Instant::now();
                let stats = java_perf.clone();
                let _delivery = producer
                    .send_with_callback(
                        benchmark_record(Arc::clone(&topic), index).value(value.clone()),
                        move |result| {
                            let completed = Instant::now();
                            if let Some(line) = record_tracked_callback_completion(
                                &result,
                                TrackedCompletionStart::Single(send_started),
                                &stats,
                                completed,
                                value_size,
                            ) {
                                println!("{line}");
                            }
                            if result.is_err() {
                                eprintln!("producer callback reported delivery error: {result:?}");
                            }
                        },
                    )
                    .expect("benchmark concurrent send should fit and dispatch");
            }
        }));
    }
    for handle in handles {
        handle
            .await
            .expect("benchmark concurrent send task should finish");
    }
    let mut producer =
        Arc::into_inner(producer).expect("producer should be unique after concurrent send join");
    producer
        .flush()
        .await
        .expect("benchmark concurrent final flush should succeed");
    let elapsed = started.elapsed();
    let java_perf = java_perf.summary(elapsed);
    (
        SendLoopResult {
            outer_chunks: total,
            elapsed,
            java_perf: Some(java_perf),
        },
        producer,
    )
}

#[derive(Debug, Clone, Copy)]
enum TrackedCompletionStart {
    Single(Instant),
}

impl TrackedCompletionStart {
    const fn next(self) -> Instant {
        match self {
            Self::Single(started) => started,
        }
    }
}

fn record_tracked_callback_completion(
    result: &kacrab::producer::Result<RecordMetadata>,
    completion_start: TrackedCompletionStart,
    performance_stats: &ProducerPerformanceStatsHandle,
    completed: Instant,
    value_size: usize,
) -> Option<String> {
    if result.is_err() {
        return None;
    }
    let started = completion_start.next();
    performance_stats.record_completion(started, completed, value_size)
}

async fn warm_up_producer(producer: &mut Producer, run: &BenchmarkRun<'_>, value: Bytes) {
    let topic = Arc::<str>::from(run.topic);
    let warmup_messages = warmup_record_count(run);
    for index in 0..warmup_messages {
        let _delivery = producer
            .send_with_callback(
                benchmark_record(Arc::clone(&topic), index).value(value.clone()),
                |_result| {},
            )
            .expect("benchmark per-record warmup send should dispatch");
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DeliveryMode {
    PerRecord,
}

impl std::fmt::Display for DeliveryMode {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PerRecord => formatter.write_str("per-record"),
        }
    }
}

fn benchmark_api() -> DeliveryMode {
    benchmark_api_for(env::var("KACRAB_BENCH_API").ok().as_deref())
}

const fn benchmark_api_for(value: Option<&str>) -> DeliveryMode {
    let _ = value;
    DeliveryMode::PerRecord
}

fn benchmark_record(topic: Arc<str>, _index: usize) -> ProducerRecord {
    ProducerRecord::unassigned(topic)
}

fn scenarios() -> Vec<Scenario> {
    // KACRAB_BENCH_MESSAGES bounds the small-payload run to a fixed record count and
    // skips the large-payload scenario — used to profile a single hot partition without
    // the default 5,000,000-record flood overrunning delivery.timeout.ms.
    if env::var("KACRAB_ONLY_10KIB").is_ok() {
        return vec![large_payload_scenario()];
    }
    if let Some(messages) = env::var("KACRAB_BENCH_MESSAGES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
    {
        let mut scenario = small_payload_scenario();
        scenario.messages = messages;
        return vec![scenario];
    }
    if env::var("KACRAB_ONLY_10B").is_ok() {
        return vec![small_payload_scenario()];
    }
    vec![small_payload_scenario(), large_payload_scenario()]
}

fn bench_runs() -> usize {
    env::var("KACRAB_BENCH_RUNS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|runs| *runs > 0)
        .unwrap_or(BENCH_RUNS)
}

const fn reporting_interval() -> Duration {
    Duration::from_secs(5)
}

fn small_payload_scenario() -> Scenario {
    Scenario {
        name: "5,000,000 messages x 10 bytes".to_owned(),
        messages: BENCH_MESSAGES,
        value_size: SMALL_VALUE_SIZE,
        batch_messages: TRACKED_API_CHUNK_RECORDS,
    }
}

fn large_payload_scenario() -> Scenario {
    Scenario {
        name: "100,000 messages x 10 KiB".to_owned(),
        messages: LARGE_BENCH_MESSAGES,
        value_size: LARGE_VALUE_SIZE,
        batch_messages: TRACKED_API_CHUNK_RECORDS.min(96),
    }
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

fn payload_value(default_size: usize) -> Bytes {
    Bytes::from(vec![b'x'; default_size])
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

#[cfg(test)]
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

#[cfg(test)]
fn percentile_ms(samples: &[Duration], per_mille: usize) -> f64 {
    let len = samples.len();
    let rank = per_mille
        .checked_mul(len)
        .and_then(|scaled| scaled.checked_add(999))
        .map_or(len, |scaled| scaled / 1000);
    let index = rank.saturating_sub(1).min(len.saturating_sub(1));
    duration_ms(samples[index])
}

#[cfg(test)]
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
    let high = u32::try_from(value >> 32).unwrap_or(u32::MAX);
    let low = u32::try_from(value & u64::from(u32::MAX)).unwrap_or(u32::MAX);
    f64::from(high) * 4_294_967_296.0 + f64::from(low)
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
    current: &ProducerMetricsSnapshot,
    baseline: &ProducerMetricsSnapshot,
) -> ProducerMetricsSnapshot {
    ProducerMetricsSnapshot {
        records_appended: current
            .records_appended
            .saturating_sub(baseline.records_appended),
        produce_request_count: current
            .produce_request_count
            .saturating_sub(baseline.produce_request_count),
        produce_request_bytes: current
            .produce_request_bytes
            .saturating_sub(baseline.produce_request_bytes),
        produce_batch_count: current
            .produce_batch_count
            .saturating_sub(baseline.produce_batch_count),
        produce_batch_bytes: current
            .produce_batch_bytes
            .saturating_sub(baseline.produce_batch_bytes),
        produce_request_payload_bytes: current
            .produce_request_payload_bytes
            .saturating_sub(baseline.produce_request_payload_bytes),
        produce_request_split_count: current
            .produce_request_split_count
            .saturating_sub(baseline.produce_request_split_count),
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
        in_flight_stall_count: current
            .in_flight_stall_count
            .saturating_sub(baseline.in_flight_stall_count),
        queue_depth_bytes: current.queue_depth_bytes,
        queue_depth_records: current.queue_depth_records,
        buffer_available_bytes: current.buffer_available_bytes,
        waiting_threads: current.waiting_threads,
        incomplete_batches: current.incomplete_batches,
        in_flight_dispatches: current.in_flight_dispatches,
        average_batch_fill_ratio: current.average_batch_fill_ratio,
        average_compression_ratio: current.average_compression_ratio,
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

fn print_average_result(scenario: &Scenario, summaries: &[ProducerPerformanceSummary]) {
    println!("{}", format_average_result_line(scenario, summaries));
}

fn print_average_counters(metrics: &[ProducerMetricsSnapshot]) {
    println!("{}", format_average_counter_line(metrics));
}

fn format_average_result_line(
    scenario: &Scenario,
    summaries: &[ProducerPerformanceSummary],
) -> String {
    let runs = summaries.len().max(1);
    let runs_f64 = f64::from(u32::try_from(runs).expect("benchmark run count should fit in u32"));
    let records_per_second = summaries
        .iter()
        .map(|summary| summary.records_per_second)
        .sum::<f64>()
        / runs_f64;
    let mebibytes_per_second = summaries
        .iter()
        .map(|summary| summary.mebibytes_per_second)
        .sum::<f64>()
        / runs_f64;
    format!(
        "{}: {:.0} messages/s, {:.3} MB/s (average over {} runs)",
        scenario.name, records_per_second, mebibytes_per_second, runs
    )
}

fn format_average_counter_line(metrics: &[ProducerMetricsSnapshot]) -> String {
    let mut line = String::from("rust average counters: ");
    append_average_metrics(&mut line, metrics);
    line
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
        append_metrics(&mut line, &result.metrics);
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
        let mut line = format!(
            "{} [{}]: {:.0} messages/s, {:.3} MiB/s ({:.3}s, api_chunks={}, \
             dispatch_latency_samples={}, dispatch_latency_avg={:.2} ms, \
             dispatch_latency_p50={:.2} ms, dispatch_latency_p95={:.2} ms, \
             dispatch_latency_p99={:.2} ms, dispatch_latency_p999={:.2} ms, \
             dispatch_latency_max={:.2} ms, ",
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
        );
        append_metrics(&mut line, &result.metrics);
        line.push(')');
        return line;
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
        let mut line = format!(
            "{} [{}]: {:.0} messages/s, {:.3} MiB/s ({:.3}s, api_chunks={}, ",
            result.scenario.name,
            result.delivery_mode,
            messages_per_second,
            megabytes_per_second,
            result.elapsed.as_secs_f64(),
            result.outer_chunks
        );
        append_metrics(&mut line, &result.metrics);
        line.push(')');
        return line;
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

fn append_metrics(line: &mut String, metrics: &ProducerMetricsSnapshot) {
    let records_per_batch_avg =
        average_counter(metrics.produce_record_count, metrics.produce_batch_count);
    let records_per_request_avg =
        average_counter(metrics.produce_record_count, metrics.produce_request_count);
    let request_size_avg =
        average_counter(metrics.produce_request_bytes, metrics.produce_request_count);
    let batch_payload_bytes_per_request_avg = average_counter(
        metrics.produce_request_payload_bytes,
        metrics.produce_request_count,
    );
    let _result = write!(
        line,
        "produce_requests={}, record_batches={}, records_per_batch_avg={:.3}, \
         records_per_request_avg={:.3}, request_size_avg={:.3}, \
         record_batch_payload_bytes_per_request_avg={:.3}, retries={}, errors={}, \
         in_flight_stalls={}, batch_splits=not_tracked, request_splits={}, requeues={}, \
         batch_fill={:.3}, compression_ratio={:.3}",
        metrics.produce_request_count,
        metrics.produce_batch_count,
        records_per_batch_avg,
        records_per_request_avg,
        request_size_avg,
        batch_payload_bytes_per_request_avg,
        metrics.produce_retry_count,
        metrics.produce_error_count,
        metrics.in_flight_stall_count,
        metrics.produce_request_split_count,
        metrics.requeue_count,
        metrics.average_batch_fill_ratio,
        metrics.average_compression_ratio
    );
}

fn append_average_metrics(line: &mut String, metrics: &[ProducerMetricsSnapshot]) {
    let runs = f64_from_usize(metrics.len().max(1));
    let produce_requests = metrics
        .iter()
        .map(|snapshot| snapshot.produce_request_count)
        .sum::<u64>();
    let record_batches = metrics
        .iter()
        .map(|snapshot| snapshot.produce_batch_count)
        .sum::<u64>();
    let records = metrics
        .iter()
        .map(|snapshot| snapshot.produce_record_count)
        .sum::<u64>();
    let request_bytes = metrics
        .iter()
        .map(|snapshot| snapshot.produce_request_bytes)
        .sum::<u64>();
    let request_payload_bytes = metrics
        .iter()
        .map(|snapshot| snapshot.produce_request_payload_bytes)
        .sum::<u64>();
    let retries = metrics
        .iter()
        .map(|snapshot| snapshot.produce_retry_count)
        .sum::<u64>();
    let errors = metrics
        .iter()
        .map(|snapshot| snapshot.produce_error_count)
        .sum::<u64>();
    let in_flight_stalls = metrics
        .iter()
        .map(|snapshot| snapshot.in_flight_stall_count)
        .sum::<u64>();
    let request_splits = metrics
        .iter()
        .map(|snapshot| snapshot.produce_request_split_count)
        .sum::<u64>();
    let requeues = metrics
        .iter()
        .map(|snapshot| snapshot.requeue_count)
        .sum::<u64>();
    let batch_fill = metrics
        .iter()
        .map(|snapshot| snapshot.average_batch_fill_ratio)
        .sum::<f64>()
        / runs;
    let compression_ratio = metrics
        .iter()
        .map(|snapshot| snapshot.average_compression_ratio)
        .sum::<f64>()
        / runs;

    let _result = write!(
        line,
        "produce_requests={:.3}, record_batches={:.3}, records_per_batch_avg={:.3}, \
         records_per_request_avg={:.3}, request_size_avg={:.3}, \
         record_batch_payload_bytes_per_request_avg={:.3}, retries={:.3}, errors={:.3}, \
         in_flight_stalls={:.3}, batch_splits=not_tracked, request_splits={:.3}, requeues={:.3}, \
         batch_fill={:.3}, compression_ratio={:.3}",
        f64_from_u64(produce_requests) / runs,
        f64_from_u64(record_batches) / runs,
        average_counter(records, record_batches),
        average_counter(records, produce_requests),
        average_counter(request_bytes, produce_requests),
        average_counter(request_payload_bytes, produce_requests),
        f64_from_u64(retries) / runs,
        f64_from_u64(errors) / runs,
        f64_from_u64(in_flight_stalls) / runs,
        f64_from_u64(request_splits) / runs,
        f64_from_u64(requeues) / runs,
        batch_fill,
        compression_ratio
    );
}

fn average_counter(total: u64, count: u64) -> f64 {
    if count == 0 {
        0.0
    } else {
        f64_from_u64(total) / f64_from_u64(count)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::Arc,
        time::{Duration, Instant},
    };

    use kacrab::producer::{ProducerError, RecordMetadata};

    use super::{
        BENCH_RUNS, BenchmarkResult, DeliveryMode, LatencySummary, ProducerMetricsSnapshot,
        ProducerPerformanceStats, ProducerPerformanceStatsHandle, Scenario, TrackedCompletionStart,
        benchmark_api_for, benchmark_producer_config, format_average_counter_line,
        format_effective_config_snapshot, format_result_line, latency_summary,
        record_tracked_callback_completion, scenarios,
    };

    #[test]
    fn scenarios_are_fixed_five_million_record_payloads() {
        let scenarios = scenarios();

        assert_eq!(scenarios.len(), 2);
        assert_eq!(scenarios[0].messages, 5_000_000);
        assert_eq!(scenarios[0].value_size, 10);
        assert_eq!(scenarios[1].messages, 100_000);
        assert_eq!(scenarios[1].value_size, 10 * 1024);
    }

    #[test]
    fn benchmark_averages_over_five_runs() {
        assert_eq!(BENCH_RUNS, 5);
    }

    #[test]
    fn benchmark_api_defaults_to_per_record_java_parity() {
        assert_eq!(benchmark_api_for(None), DeliveryMode::PerRecord);
        assert_eq!(DeliveryMode::PerRecord.to_string(), "per-record");
    }

    #[test]
    fn benchmark_api_ignores_removed_batched_public_api() {
        assert_eq!(benchmark_api_for(Some("batched")), DeliveryMode::PerRecord);
        assert_eq!(
            benchmark_api_for(Some("send-batch")),
            DeliveryMode::PerRecord
        );
    }

    #[test]
    fn effective_config_snapshot_reports_java_default_parity_keys() {
        let bootstrap = "127.0.0.1:9092".parse().expect("socket address");
        let config = benchmark_producer_config(bootstrap);
        let snapshot = format_effective_config_snapshot(&config);

        assert!(snapshot.starts_with("effective producer config: "));
        assert!(snapshot.contains("bootstrap.servers=127.0.0.1:9092"));
        assert!(snapshot.contains("client.id=kacrab-producer-kafka-bench"));
        assert!(snapshot.contains("acks=all"));
        assert!(snapshot.contains("enable.idempotence=true"));
        assert!(snapshot.contains("retries=2147483647"));
        assert!(snapshot.contains("max.in.flight.requests.per.connection=5"));
        assert!(snapshot.contains("batch.size=16384"));
        assert!(snapshot.contains("linger.ms=5"));
        assert!(snapshot.contains("buffer.memory=33554432"));
        assert!(snapshot.contains("compression.type=none"));
        assert!(snapshot.contains("delivery.timeout.ms=120000"));
        assert!(snapshot.contains("request.timeout.ms=30000"));
        assert!(snapshot.contains("max.block.ms=60000"));
        assert!(snapshot.contains("max.request.size=1048576"));
        assert!(snapshot.contains("send.buffer.bytes=131072"));
        assert!(snapshot.contains("receive.buffer.bytes=32768"));
        assert!(snapshot.contains("metadata.max.age.ms=300000"));
        assert!(snapshot.contains("partitioner.adaptive.partitioning.enable=true"));
        assert!(snapshot.contains("partitioner.availability.timeout.ms=0"));
        assert!(snapshot.contains("enable.metrics.push=true"));
    }

    #[test]
    fn tracked_callback_accounting_counts_successes_and_skips_failures() {
        let started = Instant::now();
        let performance_stats = ProducerPerformanceStatsHandle::new(ProducerPerformanceStats::new(
            1,
            Duration::from_secs(5),
            false,
        ));
        let failure = Err(ProducerError::InvalidRecord {
            field: "value",
            message: "forced failure",
        });

        let failed_line = record_tracked_callback_completion(
            &failure,
            TrackedCompletionStart::Single(started),
            &performance_stats,
            started,
            10,
        );

        assert_eq!(failed_line, None);

        let success = Ok(RecordMetadata {
            topic: Arc::from("bench"),
            partition: 0,
            leader_id: 0,
            offset: 0,
            timestamp_ms: -1,
            serialized_key_size: -1,
            serialized_value_size: 10,
        });

        let first_success_line = record_tracked_callback_completion(
            &success,
            TrackedCompletionStart::Single(started),
            &performance_stats,
            started + Duration::from_millis(5),
            10,
        );
        let summary = performance_stats.summary(Duration::from_secs(1));

        assert_eq!(first_success_line, None);
        assert_eq!(summary.records, 1);
        assert_float_eq(summary.avg_ms, 5.0);
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
            delivery_mode: DeliveryMode::PerRecord,
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
            delivery_mode: DeliveryMode::PerRecord,
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

    #[test]
    fn tracked_result_metrics_use_parity_counter_schema() {
        let scenario = Scenario {
            name: "tracked scenario".to_owned(),
            messages: 1_000,
            value_size: 10,
            batch_messages: 100,
        };
        let stats = ProducerPerformanceStats::new(1_000, Duration::from_secs(5), false);
        let started = Instant::now();
        let _report = stats.record_completion(started, started + Duration::from_millis(5), 10);
        let mut metrics = empty_metrics();
        metrics.produce_request_count = 2;
        metrics.produce_request_bytes = 2_000;
        metrics.produce_batch_count = 4;
        metrics.produce_request_payload_bytes = 2_000;
        metrics.produce_request_split_count = 0;
        metrics.produce_record_count = 1_000;
        metrics.produce_retry_count = 1;
        metrics.produce_error_count = 0;
        metrics.in_flight_stall_count = 3;
        metrics.requeue_count = 1;
        metrics.average_batch_fill_ratio = 0.5;
        metrics.average_compression_ratio = 0.75;

        let line = format_result_line(&BenchmarkResult {
            scenario: &scenario,
            value_size: 10,
            outer_chunks: 1_000,
            latency: None,
            java_perf: Some(stats.summary(Duration::from_secs(1))),
            metrics,
            metrics_enabled: true,
            delivery_mode: DeliveryMode::PerRecord,
            elapsed: Duration::from_secs(1),
        });

        assert!(line.contains("produce_requests=2"));
        assert!(line.contains("record_batches=4"));
        assert!(line.contains("records_per_batch_avg=250.000"));
        assert!(line.contains("records_per_request_avg=500.000"));
        assert!(line.contains("request_size_avg=1000.000"));
        assert!(line.contains("record_batch_payload_bytes_per_request_avg=1000.000"));
        assert!(line.contains("retries=1"));
        assert!(line.contains("errors=0"));
        assert!(line.contains("in_flight_stalls=3"));
        assert!(line.contains("batch_splits=not_tracked"));
        assert!(line.contains("request_splits=0"));
        assert!(line.contains("compression_ratio=0.750"));
    }

    #[test]
    fn average_counter_line_reports_run_averaged_parity_schema() {
        let mut first = empty_metrics();
        first.produce_request_count = 2;
        first.produce_request_bytes = 2_000;
        first.produce_batch_count = 4;
        first.produce_request_payload_bytes = 1_800;
        first.produce_record_count = 1_000;
        first.produce_retry_count = 1;
        first.produce_error_count = 0;
        first.in_flight_stall_count = 2;
        first.produce_request_split_count = 0;
        first.requeue_count = 1;
        first.average_batch_fill_ratio = 0.5;
        first.average_compression_ratio = 0.5;

        let mut second = empty_metrics();
        second.produce_request_count = 4;
        second.produce_request_bytes = 4_400;
        second.produce_batch_count = 6;
        second.produce_request_payload_bytes = 3_900;
        second.produce_record_count = 1_000;
        second.produce_retry_count = 3;
        second.produce_error_count = 2;
        second.in_flight_stall_count = 4;
        second.produce_request_split_count = 2;
        second.requeue_count = 3;
        second.average_batch_fill_ratio = 0.7;
        second.average_compression_ratio = 0.9;

        let line = format_average_counter_line(&[first, second]);

        assert!(line.starts_with("rust average counters: "));
        assert!(line.contains("produce_requests=3.000"));
        assert!(line.contains("record_batches=5.000"));
        assert!(line.contains("records_per_batch_avg=200.000"));
        assert!(line.contains("records_per_request_avg=333.333"));
        assert!(line.contains("request_size_avg=1066.667"));
        assert!(line.contains("record_batch_payload_bytes_per_request_avg=950.000"));
        assert!(line.contains("retries=2.000"));
        assert!(line.contains("errors=1.000"));
        assert!(line.contains("in_flight_stalls=3.000"));
        assert!(line.contains("batch_splits=not_tracked"));
        assert!(line.contains("request_splits=1.000"));
        assert!(line.contains("requeues=2.000"));
        assert!(line.contains("batch_fill=0.600"));
        assert!(line.contains("compression_ratio=0.700"));
    }

    #[test]
    fn average_counter_line_does_not_saturate_large_request_bytes() {
        let mut metrics = empty_metrics();
        metrics.produce_request_count = 200_000;
        metrics.produce_request_bytes = 6_000_000_000;
        metrics.produce_request_payload_bytes = 5_800_000_000;
        metrics.produce_batch_count = 200_000;
        metrics.produce_record_count = 200_000;

        let line = format_average_counter_line(&[metrics]);

        assert!(line.contains("request_size_avg=30000.000"));
        assert!(line.contains("record_batch_payload_bytes_per_request_avg=29000.000"));
    }

    const fn empty_metrics() -> ProducerMetricsSnapshot {
        ProducerMetricsSnapshot {
            records_appended: 0,
            produce_request_count: 0,
            produce_request_bytes: 0,
            produce_batch_count: 0,
            produce_batch_bytes: 0,
            produce_request_payload_bytes: 0,
            produce_request_split_count: 0,
            produce_record_count: 0,
            produce_retry_count: 0,
            produce_error_count: 0,
            requeue_count: 0,
            in_flight_stall_count: 0,
            queue_depth_bytes: 0,
            queue_depth_records: 0,
            buffer_available_bytes: 0,
            waiting_threads: 0,
            incomplete_batches: 0,
            in_flight_dispatches: 0,
            average_batch_fill_ratio: 0.0,
            average_compression_ratio: 0.0,
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
