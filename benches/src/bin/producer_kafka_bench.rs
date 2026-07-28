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
// Opt-in MESSAGE_TOO_LARGE split probe (KACRAB_BENCH_SPLIT_PROBE=1). The sizing is the
// whole point of the scenario: 4 KiB records stay far below the probe topic's
// max.message.bytes so no record is oversize on its own (a one-record batch is
// unsplittable), 256 KiB batches overflow that topic limit by 4x so the broker rejects
// them, and 256 KiB also stays under kacrab's own max.request.size so the broker limit
// binds before the client-side one. These defaults plus kacrab's max.request.size are the
// single source of truth for the probe: the run configures the producer from them and
// KACRAB_BENCH_PRINT_SPLIT_PROBE_CONFIG=1 dumps them for producer_default_matrix.sh, so
// the Java pass and the probe topic cannot drift away from the kacrab pass.
const SPLIT_PROBE_MESSAGES: usize = 20_000;
const SPLIT_PROBE_VALUE_SIZE: usize = 4 * 1024;
const SPLIT_PROBE_BATCH_SIZE: usize = 256 * 1024;
const SPLIT_PROBE_TOPIC_MAX_MESSAGE_BYTES: usize = 64 * 1024;

fn main() {
    // KACRAB_BENCH_PRINT_SPLIT_PROBE_CONFIG=1 prints the resolved probe sizing as KEY=VALUE
    // lines and exits, so the matrix script consumes these values instead of mirroring them.
    if env::var("KACRAB_BENCH_PRINT_SPLIT_PROBE_CONFIG").is_ok() {
        print!(
            "{}",
            split_probe_config_dump(&resolved_split_probe_config())
        );
        return;
    }
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
        if split_probe_enabled() {
            let probe = resolved_split_probe_config();
            println!(
                "split probe enabled: batch.size={}, record_size={}, max.request.size={} (must \
                 not bind first), topic must be created with max.message.bytes={}",
                probe.batch_size,
                probe.value_size,
                probe.max_request_size,
                probe.topic_max_message_bytes
            );
        }
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
    // Print the config that actually binds — defaults plus every benchmark override,
    // including the split probe's batch.size. Once per run, before the measured window.
    let producer_config = effective_producer_config(run.bootstrap);
    println!("{}", format_effective_config_snapshot(&producer_config));
    let mut producer = build_producer(&producer_config).await;
    if env::var("KACRAB_BENCH_NO_METRICS").is_err() {
        producer.enable_metrics();
    }
    warm_up_producer(&producer, &run, value.clone()).await;
    let warmup_metrics = producer.metrics();
    let concurrency = send_concurrency();
    let send = if concurrency > 1 {
        let (result, recovered) =
            run_per_record_tracked_send_loop_concurrent(producer, &run, value, concurrency).await;
        producer = recovered;
        result
    } else {
        run_per_record_tracked_send_loop(&producer, &run, value).await
    };
    let current_metrics = producer.metrics();
    let metrics = current_metrics.delta_since(&warmup_metrics);
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

/// kacrab's parsed defaults for this bootstrap, with no benchmark override applied.
///
/// Deliberately override-free: `kafka_max_request_size` reads kacrab's *own* effective
/// `max.request.size` out of this to validate the probe invariants, so folding
/// `KACRAB_BENCH_MAX_REQUEST_SIZE` in here would make the probe validate an override
/// against itself. Use [`effective_producer_config`] for the config the run binds.
fn benchmark_producer_config(bootstrap: SocketAddr) -> ProducerConfig {
    benchmark_client_config(bootstrap)
        .producer_config()
        .expect("benchmark producer config should parse")
}

/// The config the run actually binds: kacrab's defaults plus every benchmark override.
///
/// This is what the `effective producer config:` line reports. It is built from the same
/// override list `build_producer` applies, so the log cannot claim a `batch.size` the
/// producer does not run with (the split probe raises it to 256 KiB).
fn effective_producer_config(bootstrap: SocketAddr) -> ProducerConfig {
    producer_config_with_overrides(bootstrap, &benchmark_producer_overrides())
}

fn producer_config_with_overrides(
    bootstrap: SocketAddr,
    overrides: &[(&'static str, String)],
) -> ProducerConfig {
    let mut config = benchmark_client_config(bootstrap);
    for (key, value) in overrides {
        config = config.set(*key, value.as_str());
    }
    config
        .producer_config()
        .expect("benchmark producer config should parse")
}

/// Every producer setting the benchmark overrides on top of kacrab's defaults.
///
/// Resolved once per run, outside the measured send loop. `build_producer` applies exactly
/// this list and `effective_producer_config` parses exactly this list, which is what keeps
/// the printed config and the built producer from drifting apart.
fn benchmark_producer_overrides() -> Vec<(&'static str, String)> {
    let split_probe = split_probe_enabled().then(resolved_split_probe_config);
    benchmark_producer_overrides_from(split_probe.as_ref(), |key| env::var(key).ok())
}

/// The pure half of [`benchmark_producer_overrides`]: the env-var -> producer-setting
/// mapping with the lookup injected, so tests pin the mapping without mutating
/// process-global env (which would race every other test in this binary).
/// Target offered load in records/sec from `KACRAB_BENCH_THROUGHPUT`, or `None` for
/// unthrottled (the default).
///
/// Java's `kafka-producer-perf-test` takes `--throughput`; kacrab's bench had no equivalent,
/// which made every published latency comparison suspect: both clients ran flat out, so each
/// sat at its OWN saturation point and the two latency columns described different offered
/// loads. Pinning both sides to the same rate is the only way to attribute a latency
/// difference to the client rather than to how hard it happened to be pushing.
fn throughput_target_records_per_sec() -> Option<f64> {
    env::var("KACRAB_BENCH_THROUGHPUT")
        .ok()
        .and_then(|value| value.trim().parse::<f64>().ok())
        .filter(|target| target.is_finite() && *target > 0.0)
}

/// Pace the send loop to `target` records/sec, mirroring Java's `ThroughputThrottler`:
/// sleep only when the loop is running ahead of the schedule implied by the target.
async fn throttle_to_target(target: f64, sent: usize, started: Instant) {
    let expected_elapsed = f64_from_usize(sent) / target;
    let actual_elapsed = started.elapsed().as_secs_f64();
    let ahead_by = expected_elapsed - actual_elapsed;
    if ahead_by > 0.0 {
        tokio::time::sleep(Duration::from_secs_f64(ahead_by)).await;
    }
}

fn benchmark_producer_overrides_from(
    split_probe: Option<&SplitProbeConfig>,
    lookup: impl Fn(&str) -> Option<String>,
) -> Vec<(&'static str, String)> {
    let mut overrides = Vec::new();
    if let Some(probe) = split_probe {
        // The probe owns both sizing knobs that bind: batch.size several times the probe
        // topic's max.message.bytes so every full batch is rejected with MESSAGE_TOO_LARGE,
        // and max.request.size high enough that the broker limit binds before the client
        // one. KACRAB_BENCH_BATCH_SIZE / KACRAB_BENCH_MAX_REQUEST_SIZE are folded into the
        // probe config, so an override that would defeat the probe is rejected there
        // instead of being applied here.
        overrides.extend(split_probe_producer_settings(probe));
    } else {
        // KACRAB_BENCH_BATCH_SIZE overrides batch.size to confirm whether throughput is
        // round-trip/pipelining bound (more records per request -> higher rate if so).
        if let Some(batch_size) = lookup("KACRAB_BENCH_BATCH_SIZE") {
            overrides.push(("batch.size", batch_size));
        }
        // KACRAB_BENCH_MAX_REQUEST_SIZE lifts the 1 MiB default so large-record runs
        // with a bigger batch.size do not trip RecordTooLarge on coalesced requests.
        if let Some(max_request_size) = lookup("KACRAB_BENCH_MAX_REQUEST_SIZE") {
            overrides.push(("max.request.size", max_request_size));
        }
    }
    if lookup("KACRAB_BENCH_ACKS1").is_some() {
        overrides.push(("acks", "1".to_owned()));
        overrides.push(("enable.idempotence", "false".to_owned()));
    }
    // KACRAB_BENCH_NO_ADAPTIVE disables adaptive partitioning to test uniform
    // round-robin sticky spread across all partitions.
    if lookup("KACRAB_BENCH_NO_ADAPTIVE").is_some() {
        overrides.push((
            "partitioner.adaptive.partitioning.enable",
            "false".to_owned(),
        ));
    }
    // KACRAB_BENCH_LINGER_MS overrides linger.ms to isolate whether large-record
    // throughput is linger-bound (1 record/batch waits the full linger).
    if let Some(linger) = lookup("KACRAB_BENCH_LINGER_MS") {
        overrides.push(("linger.ms", linger));
    }
    // KACRAB_BENCH_MAX_IN_FLIGHT overrides max.in.flight.requests.per.connection. Pipeline
    // depth trades latency for throughput, and benches/README.md claims depth 1 brings p99 to
    // ~2 ms at unchanged throughput -- a claim no knob could test until now.
    if let Some(max_in_flight) = lookup("KACRAB_BENCH_MAX_IN_FLIGHT") {
        overrides.push(("max.in.flight.requests.per.connection", max_in_flight));
    }
    // KACRAB_BENCH_BUFFER_MEMORY isolates the buffer-full append spin: a huge buffer
    // lets every record enqueue without backpressure, so the run measures pure drain.
    if let Some(buffer) = lookup("KACRAB_BENCH_BUFFER_MEMORY") {
        overrides.push(("buffer.memory", buffer));
    }
    overrides
}

async fn build_producer(config: &ProducerConfig) -> Producer {
    let mut builder = Producer::builder()
        .set(
            "bootstrap.servers",
            config.bootstrap_servers.as_slice().join(","),
        )
        .set("client.id", config.client_id.as_str());
    for (key, value) in benchmark_producer_overrides() {
        builder = builder.set(key, value.as_str());
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
    producer: &Producer,
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
    // KACRAB_BENCH_SYNC_SEND=1 only turns on the buffer-spin report printed after the
    // loop; it does NOT select a send path. `send_with_callback` is synchronous
    // unconditionally now (see the comment on the call below), so there is nothing left
    // to switch between.
    let sync_send = env::var("KACRAB_BENCH_SYNC_SEND").is_ok();
    // Resolved ONCE, never inside the loop: a per-record `env::var` costs ~28% of
    // small-record throughput on macOS because `getenv` takes a global libc lock
    // (benches/README.md records that regression). `None` is the default and adds no
    // per-record work beyond one already-hot `Option` check.
    let throughput_target = throughput_target_records_per_sec();
    let mut sent = 0usize;
    while sent < run.scenario.messages {
        // Java parity, and the reason this timestamp lives OUTSIDE the retry loop
        // below. `ProducerPerformance` captures `sendStartMs` once per record
        // (ProducerPerformance.java:102) and calls `producer.send(...)` exactly once
        // — its loop at :91 has no retry. `KafkaProducer.send` then BLOCKS inside
        // that measured window whenever the accumulator is full
        // (KafkaProducer.java:1029 -> RecordAccumulator.append -> BufferPool.allocate
        // -> BufferPool.java:149 `moreMemory.await(...)`, bounded by `max.block.ms`),
        // so Java's per-record latency INCLUDES time spent waiting for buffer memory.
        //
        // kacrab's `send_with_callback` returns `Backpressure` instead of blocking, so
        // that same wait happens in the retry loop below. Taking the timestamp per
        // ATTEMPT would discard it — and discard the most from exactly the records that
        // waited longest, truncating the p99/p99.9/max tail where congestion is worst.
        // One `Instant::now()` per record, never per attempt: this is also strictly
        // fewer clock reads than a per-attempt timestamp under backpressure.
        let send_started = Instant::now();
        loop {
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
                Ok(_delivery) => {
                    sent = sent.saturating_add(1);
                    break;
                },
                // Closed-loop backpressure. The producer buffer (Backpressure) or a
                // broker connection's in-flight queue (Wire(Backpressure)) is full —
                // common with large records, which fill the 32 MiB buffer long before
                // the drain catches up. Wait for the drain to free space and retry the
                // same record instead of flooding open-loop and panicking. This caps
                // the send rate at the real drain rate; the wait stays inside
                // `send_started`, matching what Java's blocking `send()` accrues.
                Err(ProducerError::Backpressure | ProducerError::Wire(WireError::Backpressure)) => {
                    tokio::time::sleep(Duration::from_micros(50)).await;
                },
                Err(error) => panic!("benchmark send failed: {error:?}"),
            }
        }
        if let Some(target) = throughput_target {
            throttle_to_target(target, sent, started).await;
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
    let producer =
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

async fn warm_up_producer(producer: &Producer, run: &BenchmarkRun<'_>, value: Bytes) {
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

fn benchmark_record(topic: Arc<str>, index: usize) -> ProducerRecord {
    // KACRAB_BENCH_SPREAD=N forces explicit round-robin over N partitions to
    // isolate whether throughput is concurrency-bound (1-in-flight × N partitions).
    // Read the env var ONCE: this function runs per record, and macOS `getenv`
    // takes a global libc lock (`__findenv_locked`) that serializes the send
    // loop — calling it 5M times cost ~28% of small-record throughput.
    static SPREAD: std::sync::OnceLock<Option<usize>> = std::sync::OnceLock::new();
    let spread = SPREAD.get_or_init(|| {
        env::var("KACRAB_BENCH_SPREAD")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|partitions| *partitions > 0)
    });
    if let Some(partitions) = spread {
        let partition = i32::try_from(index % partitions).unwrap_or(0);
        return ProducerRecord::new(topic.as_ref(), partition);
    }
    ProducerRecord::unassigned(topic)
}

fn scenarios() -> Vec<Scenario> {
    // KACRAB_BENCH_SPLIT_PROBE=1 replaces the default parity scenarios with the opt-in
    // oversize probe, the only scenario that drives the broker into MESSAGE_TOO_LARGE so
    // the `batch_splits` column is non-zero on both sides of the comparison.
    if split_probe_enabled() {
        return vec![split_probe_scenario()];
    }
    // KACRAB_BENCH_MESSAGES bounds the small-payload run to a fixed record count and
    // skips the large-payload scenario — used to profile a single hot partition without
    // the default 5,000,000-record flood overrunning delivery.timeout.ms.
    if env::var("KACRAB_ONLY_10KIB").is_ok() {
        let mut scenario = large_payload_scenario();
        if let Some(messages) = env::var("KACRAB_BENCH_MESSAGES")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
        {
            scenario.messages = messages;
        }
        return vec![scenario];
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

fn split_probe_enabled() -> bool {
    env::var("KACRAB_BENCH_SPLIT_PROBE")
        .is_ok_and(|value| value == "1" || value.eq_ignore_ascii_case("true"))
}

/// Every sizing value the split probe binds: what the producer is configured with, what the
/// Java pass is configured with, and what the probe topic is created with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SplitProbeConfig {
    messages: usize,
    value_size: usize,
    batch_size: usize,
    max_request_size: usize,
    topic_max_message_bytes: usize,
}

/// Operator overrides for the values the probe binds. Each one replaces a value that
/// configures the run, so each one is validated against the probe invariants.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct SplitProbeOverrides {
    batch_size: Option<usize>,
    max_request_size: Option<usize>,
    topic_max_message_bytes: Option<usize>,
}

fn split_probe_overrides() -> Result<SplitProbeOverrides, String> {
    split_probe_overrides_from(|key| env::var(key).ok())
}

/// The pure half of [`split_probe_overrides`]: the env-var -> field mapping with the lookup
/// injected, so tests pin which variable feeds which field (and the parse-error branch)
/// without mutating process-global env, which would race every other test in this binary.
fn split_probe_overrides_from(
    lookup: impl Fn(&str) -> Option<String>,
) -> Result<SplitProbeOverrides, String> {
    Ok(SplitProbeOverrides {
        batch_size: byte_count_override("KACRAB_BENCH_BATCH_SIZE", &lookup)?,
        max_request_size: byte_count_override("KACRAB_BENCH_MAX_REQUEST_SIZE", &lookup)?,
        topic_max_message_bytes: byte_count_override(
            "KACRAB_SPLIT_PROBE_MAX_MESSAGE_BYTES",
            &lookup,
        )?,
    })
}

fn byte_count_override(
    key: &str,
    lookup: impl Fn(&str) -> Option<String>,
) -> Result<Option<usize>, String> {
    lookup(key).map_or(Ok(None), |value| {
        value
            .parse::<usize>()
            .map(Some)
            .map_err(|_error| format!("{key}={value} is not a byte count"))
    })
}

/// Resolves the probe sizing from the values that actually bind and rejects any override
/// that would stop the probe from probing.
fn split_probe_config(
    kafka_max_request_size: usize,
    overrides: SplitProbeOverrides,
) -> Result<SplitProbeConfig, String> {
    let config = SplitProbeConfig {
        messages: SPLIT_PROBE_MESSAGES,
        value_size: SPLIT_PROBE_VALUE_SIZE,
        batch_size: overrides.batch_size.unwrap_or(SPLIT_PROBE_BATCH_SIZE),
        max_request_size: overrides.max_request_size.unwrap_or(kafka_max_request_size),
        topic_max_message_bytes: overrides
            .topic_max_message_bytes
            .unwrap_or(SPLIT_PROBE_TOPIC_MAX_MESSAGE_BYTES),
    };
    config.validate()?;
    Ok(config)
}

impl SplitProbeConfig {
    fn validate(&self) -> Result<(), String> {
        if self.value_size >= self.topic_max_message_bytes {
            return Err(format!(
                "split probe: record size {} must stay below the probe topic's max.message.bytes \
                 {}, otherwise a record is oversize on its own and the batch is unsplittable \
                 (KACRAB_SPLIT_PROBE_MAX_MESSAGE_BYTES)",
                self.value_size, self.topic_max_message_bytes
            ));
        }
        if self.topic_max_message_bytes >= self.batch_size {
            return Err(format!(
                "split probe: the probe topic's max.message.bytes {} must stay below batch.size \
                 {}, otherwise no batch overflows the broker limit and nothing splits \
                 (KACRAB_SPLIT_PROBE_MAX_MESSAGE_BYTES, KACRAB_BENCH_BATCH_SIZE)",
                self.topic_max_message_bytes, self.batch_size
            ));
        }
        if self.batch_size / self.value_size <= 1 {
            return Err(format!(
                "split probe: batch.size {} must hold more than one {}-byte record, otherwise the \
                 oversize batch cannot be split (KACRAB_BENCH_BATCH_SIZE)",
                self.batch_size, self.value_size
            ));
        }
        if self.batch_size >= self.max_request_size {
            return Err(format!(
                "split probe: batch.size {} must stay below max.request.size {}, otherwise the \
                 client rejects the batch with RecordTooLarge before the broker sees it \
                 (KACRAB_BENCH_BATCH_SIZE, KACRAB_BENCH_MAX_REQUEST_SIZE)",
                self.batch_size, self.max_request_size
            ));
        }
        Ok(())
    }
}

/// The producer settings the probe applies — the values the sizing invariants are about.
fn split_probe_producer_settings(config: &SplitProbeConfig) -> [(&'static str, String); 2] {
    [
        ("batch.size", config.batch_size.to_string()),
        ("max.request.size", config.max_request_size.to_string()),
    ]
}

/// KEY=VALUE dump consumed by `producer_default_matrix.sh` so the Java pass and the probe
/// topic are configured from the same values as the kacrab pass.
fn split_probe_config_dump(config: &SplitProbeConfig) -> String {
    let mut dump = String::new();
    for (key, value) in [
        ("SPLIT_PROBE_MESSAGES", config.messages),
        ("SPLIT_PROBE_RECORD_SIZE", config.value_size),
        ("SPLIT_PROBE_BATCH_SIZE", config.batch_size),
        ("SPLIT_PROBE_MAX_REQUEST_SIZE", config.max_request_size),
        (
            "SPLIT_PROBE_MAX_MESSAGE_BYTES",
            config.topic_max_message_bytes,
        ),
    ] {
        writeln!(dump, "{key}={value}").expect("writing to a String cannot fail");
    }
    dump
}

fn resolved_split_probe_config() -> SplitProbeConfig {
    // The client limit that binds is kacrab's own effective max.request.size, read through
    // the public producer config rather than copied into this file.
    let kafka_max_request_size = kafka_max_request_size(bootstrap_addr());
    split_probe_overrides()
        .and_then(|overrides| split_probe_config(kafka_max_request_size, overrides))
        .unwrap_or_else(|error| panic!("{error}"))
}

fn kafka_max_request_size(bootstrap: SocketAddr) -> usize {
    usize::try_from(benchmark_producer_config(bootstrap).max_request_size.get())
        .expect("max.request.size should fit in usize")
}

fn split_probe_scenario() -> Scenario {
    Scenario {
        name: "split probe: 20,000 messages x 4 KiB".to_owned(),
        messages: SPLIT_PROBE_MESSAGES,
        value_size: SPLIT_PROBE_VALUE_SIZE,
        batch_messages: TRACKED_API_CHUNK_RECORDS.min(256),
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
         in_flight_stalls={}, batch_splits={}, request_splits={}, requeues={}, batch_fill={:.3}, \
         compression_ratio={:.3}",
        metrics.produce_request_count,
        metrics.produce_batch_count,
        records_per_batch_avg,
        records_per_request_avg,
        request_size_avg,
        batch_payload_bytes_per_request_avg,
        metrics.produce_retry_count,
        metrics.produce_error_count,
        metrics.in_flight_stall_count,
        metrics.record_batch_split_count,
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
    let batch_splits = metrics
        .iter()
        .map(|snapshot| snapshot.record_batch_split_count)
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
         in_flight_stalls={:.3}, batch_splits={:.3}, request_splits={:.3}, requeues={:.3}, \
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
        f64_from_u64(batch_splits) / runs,
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
        net::SocketAddr,
        sync::Arc,
        time::{Duration, Instant},
    };

    use kacrab::producer::{ProducerError, RecordMetadata};

    use super::{
        BENCH_RUNS, BenchmarkResult, DeliveryMode, LatencySummary, ProducerMetricsSnapshot,
        ProducerPerformanceStats, ProducerPerformanceStatsHandle, Scenario, SplitProbeOverrides,
        TrackedCompletionStart, benchmark_api_for, benchmark_producer_config,
        benchmark_producer_overrides_from, format_average_counter_line,
        format_effective_config_snapshot, format_result_line, kafka_max_request_size,
        latency_summary, producer_config_with_overrides, record_tracked_callback_completion,
        scenarios, split_probe_config, split_probe_config_dump, split_probe_overrides_from,
        split_probe_producer_settings, split_probe_scenario,
    };

    /// A stand-in for `env::var` over a fixed table, so the env-var -> field/setting mappings
    /// are tested without touching process-global env.
    fn lookup_from(entries: Vec<(&'static str, &'static str)>) -> impl Fn(&str) -> Option<String> {
        move |key| {
            entries
                .iter()
                .find(|(name, _value)| *name == key)
                .map(|(_name, value)| (*value).to_owned())
        }
    }

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
            metrics: ProducerMetricsSnapshot::ZERO,
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
            metrics: ProducerMetricsSnapshot::ZERO,
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

    fn probe_bootstrap() -> SocketAddr {
        "127.0.0.1:9092".parse().expect("bootstrap address")
    }

    fn numeric_setting(settings: &[(&str, String)], key: &str) -> usize {
        settings
            .iter()
            .find(|(name, _value)| *name == key)
            .unwrap_or_else(|| panic!("the split probe should configure {key}"))
            .1
            .parse()
            .expect("split probe producer settings should be byte counts")
    }

    #[test]
    fn split_probe_scenario_sizes_multi_record_batches_over_only_the_broker_limit() {
        let kafka_limit = kafka_max_request_size(probe_bootstrap());
        let config = split_probe_config(kafka_limit, SplitProbeOverrides::default())
            .expect("the default split probe sizing should satisfy the probe invariants");
        let scenario = split_probe_scenario();
        let settings = split_probe_producer_settings(&config);
        let batch_size = numeric_setting(&settings, "batch.size");
        let max_request_size = numeric_setting(&settings, "max.request.size");

        assert_eq!(scenario.messages, config.messages);
        assert_eq!(scenario.value_size, config.value_size);
        // A record that is oversize on its own cannot be split (records.len() <= 1), so the
        // payload must stay below the limit the probe topic is actually created with.
        assert!(scenario.value_size < config.topic_max_message_bytes);
        // The batch.size the producer is actually configured with must overflow that topic
        // limit and must hold more than one record.
        assert!(config.topic_max_message_bytes < batch_size);
        assert!(batch_size / scenario.value_size > 1);
        // The max.request.size the producer actually uses must not bind before the broker
        // limit, and it is kacrab's own effective default rather than a copy kept here.
        assert!(batch_size < max_request_size);
        assert_eq!(max_request_size, kafka_limit);
    }

    #[test]
    fn split_probe_rejects_a_broker_limit_below_the_record_size() {
        let error = split_probe_config(
            kafka_max_request_size(probe_bootstrap()),
            SplitProbeOverrides {
                topic_max_message_bytes: Some(1024),
                ..SplitProbeOverrides::default()
            },
        )
        .expect_err("a max.message.bytes below the record size must be rejected");

        assert!(
            error.contains("KACRAB_SPLIT_PROBE_MAX_MESSAGE_BYTES"),
            "{error}"
        );
        assert!(error.contains("unsplittable"), "{error}");
    }

    #[test]
    fn split_probe_rejects_a_broker_limit_no_batch_can_overflow() {
        let error = split_probe_config(
            kafka_max_request_size(probe_bootstrap()),
            SplitProbeOverrides {
                topic_max_message_bytes: Some(512 * 1024),
                ..SplitProbeOverrides::default()
            },
        )
        .expect_err("a max.message.bytes above batch.size must be rejected");

        assert!(
            error.contains("no batch overflows the broker limit"),
            "{error}"
        );
    }

    #[test]
    fn split_probe_rejects_a_client_limit_that_binds_before_the_broker() {
        let error = split_probe_config(64 * 1024, SplitProbeOverrides::default())
            .expect_err("a max.request.size below batch.size must be rejected");

        assert!(error.contains("RecordTooLarge"), "{error}");
    }

    #[test]
    fn split_probe_config_dump_carries_every_value_the_matrix_script_reads() {
        let config = split_probe_config(
            kafka_max_request_size(probe_bootstrap()),
            SplitProbeOverrides::default(),
        )
        .expect("the default split probe sizing should satisfy the probe invariants");

        let dump = split_probe_config_dump(&config);
        let entries: Vec<(&str, usize)> = dump
            .lines()
            .map(|line| {
                let (key, value) = line.split_once('=').expect("dump lines are KEY=VALUE");
                (key, value.parse().expect("dump values are byte counts"))
            })
            .collect();

        assert_eq!(
            entries,
            vec![
                ("SPLIT_PROBE_MESSAGES", config.messages),
                ("SPLIT_PROBE_RECORD_SIZE", config.value_size),
                ("SPLIT_PROBE_BATCH_SIZE", config.batch_size),
                ("SPLIT_PROBE_MAX_REQUEST_SIZE", config.max_request_size),
                (
                    "SPLIT_PROBE_MAX_MESSAGE_BYTES",
                    config.topic_max_message_bytes
                ),
            ]
        );
    }

    #[test]
    fn split_probe_overrides_map_each_env_var_to_its_own_field() {
        let overrides = split_probe_overrides_from(lookup_from(vec![
            ("KACRAB_BENCH_BATCH_SIZE", "111"),
            ("KACRAB_BENCH_MAX_REQUEST_SIZE", "222"),
            ("KACRAB_SPLIT_PROBE_MAX_MESSAGE_BYTES", "333"),
        ]))
        .expect("byte counts should parse");

        // Distinct values on purpose: swapping any two lookups fails this assertion.
        assert_eq!(
            overrides,
            SplitProbeOverrides {
                batch_size: Some(111),
                max_request_size: Some(222),
                topic_max_message_bytes: Some(333),
            }
        );
    }

    #[test]
    fn split_probe_overrides_are_absent_when_no_variable_is_set() {
        let overrides =
            split_probe_overrides_from(lookup_from(vec![])).expect("no override is not an error");

        assert_eq!(overrides, SplitProbeOverrides::default());
    }

    #[test]
    fn split_probe_overrides_reject_a_value_that_is_not_a_byte_count() {
        for key in [
            "KACRAB_BENCH_BATCH_SIZE",
            "KACRAB_BENCH_MAX_REQUEST_SIZE",
            "KACRAB_SPLIT_PROBE_MAX_MESSAGE_BYTES",
        ] {
            let error = split_probe_overrides_from(lookup_from(vec![(key, "256k")]))
                .expect_err("a non-numeric override must be rejected, not silently ignored");

            assert_eq!(error, format!("{key}=256k is not a byte count"));
        }
    }

    #[test]
    fn producer_overrides_map_each_env_var_to_the_setting_it_binds() {
        let overrides = benchmark_producer_overrides_from(
            None,
            lookup_from(vec![
                ("KACRAB_BENCH_BATCH_SIZE", "111"),
                ("KACRAB_BENCH_MAX_REQUEST_SIZE", "222"),
                ("KACRAB_BENCH_ACKS1", "1"),
                ("KACRAB_BENCH_NO_ADAPTIVE", "1"),
                ("KACRAB_BENCH_LINGER_MS", "33"),
                ("KACRAB_BENCH_BUFFER_MEMORY", "444"),
            ]),
        );

        assert_eq!(
            overrides,
            vec![
                ("batch.size", "111".to_owned()),
                ("max.request.size", "222".to_owned()),
                ("acks", "1".to_owned()),
                ("enable.idempotence", "false".to_owned()),
                (
                    "partitioner.adaptive.partitioning.enable",
                    "false".to_owned()
                ),
                ("linger.ms", "33".to_owned()),
                ("buffer.memory", "444".to_owned()),
            ]
        );
    }

    #[test]
    fn producer_overrides_are_empty_when_no_variable_is_set() {
        assert!(benchmark_producer_overrides_from(None, lookup_from(vec![])).is_empty());
    }

    #[test]
    fn effective_config_snapshot_reports_env_overrides_instead_of_the_defaults() {
        let overrides = benchmark_producer_overrides_from(
            None,
            lookup_from(vec![
                ("KACRAB_BENCH_BATCH_SIZE", "131072"),
                ("KACRAB_BENCH_LINGER_MS", "0"),
                ("KACRAB_BENCH_ACKS1", "1"),
            ]),
        );
        let snapshot = format_effective_config_snapshot(&producer_config_with_overrides(
            probe_bootstrap(),
            &overrides,
        ));

        assert!(snapshot.contains("batch.size=131072"), "{snapshot}");
        assert!(snapshot.contains("linger.ms=0"), "{snapshot}");
        assert!(snapshot.contains("acks=1"), "{snapshot}");
        assert!(snapshot.contains("enable.idempotence=false"), "{snapshot}");
        assert!(!snapshot.contains("batch.size=16384"), "{snapshot}");
    }

    #[test]
    fn effective_config_snapshot_reports_the_split_probe_sizing_that_binds() {
        let bootstrap = probe_bootstrap();
        let probe = split_probe_config(
            kafka_max_request_size(bootstrap),
            SplitProbeOverrides::default(),
        )
        .expect("the default split probe sizing should satisfy the probe invariants");
        // The probe branch ignores the env lookup entirely: the sizing comes from the
        // validated probe config, which is exactly what `build_producer` applies.
        let overrides = benchmark_producer_overrides_from(Some(&probe), lookup_from(vec![]));
        let snapshot = format_effective_config_snapshot(&producer_config_with_overrides(
            bootstrap, &overrides,
        ));

        assert_eq!(overrides, split_probe_producer_settings(&probe).to_vec());
        assert!(
            snapshot.contains(&format!("batch.size={}", probe.batch_size)),
            "{snapshot}"
        );
        assert!(
            snapshot.contains(&format!("max.request.size={}", probe.max_request_size)),
            "{snapshot}"
        );
        // The untouched default the log used to print under the probe.
        assert!(!snapshot.contains("batch.size=16384"), "{snapshot}");
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
        let mut metrics = ProducerMetricsSnapshot::ZERO;
        metrics.produce_request_count = 2;
        metrics.produce_request_bytes = 2_000;
        metrics.produce_batch_count = 4;
        metrics.produce_request_payload_bytes = 2_000;
        metrics.produce_request_split_count = 0;
        metrics.produce_record_count = 1_000;
        metrics.produce_retry_count = 1;
        metrics.produce_error_count = 0;
        metrics.in_flight_stall_count = 3;
        metrics.record_batch_split_count = 2;
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
        assert!(line.contains("batch_splits=2"));
        assert!(line.contains("request_splits=0"));
        assert!(line.contains("in_flight_stalls=3, batch_splits=2, request_splits=0"));
        assert!(line.contains("compression_ratio=0.750"));
    }

    #[test]
    fn average_counter_line_reports_run_averaged_parity_schema() {
        let mut first = ProducerMetricsSnapshot::ZERO;
        first.produce_request_count = 2;
        first.produce_request_bytes = 2_000;
        first.produce_batch_count = 4;
        first.produce_request_payload_bytes = 1_800;
        first.produce_record_count = 1_000;
        first.produce_retry_count = 1;
        first.produce_error_count = 0;
        first.in_flight_stall_count = 2;
        first.record_batch_split_count = 1;
        first.produce_request_split_count = 0;
        first.requeue_count = 1;
        first.average_batch_fill_ratio = 0.5;
        first.average_compression_ratio = 0.5;

        let mut second = ProducerMetricsSnapshot::ZERO;
        second.produce_request_count = 4;
        second.produce_request_bytes = 4_400;
        second.produce_batch_count = 6;
        second.produce_request_payload_bytes = 3_900;
        second.produce_record_count = 1_000;
        second.produce_retry_count = 3;
        second.produce_error_count = 2;
        second.in_flight_stall_count = 4;
        second.record_batch_split_count = 4;
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
        assert!(line.contains("batch_splits=2.500"));
        assert!(line.contains("request_splits=1.000"));
        assert!(line.contains("in_flight_stalls=3.000, batch_splits=2.500, request_splits=1.000"));
        assert!(line.contains("requeues=2.000"));
        assert!(line.contains("batch_fill=0.600"));
        assert!(line.contains("compression_ratio=0.700"));
    }

    #[test]
    fn average_counter_line_does_not_saturate_large_request_bytes() {
        let mut metrics = ProducerMetricsSnapshot::ZERO;
        metrics.produce_request_count = 200_000;
        metrics.produce_request_bytes = 6_000_000_000;
        metrics.produce_request_payload_bytes = 5_800_000_000;
        metrics.produce_batch_count = 200_000;
        metrics.produce_record_count = 200_000;

        let line = format_average_counter_line(&[metrics]);

        assert!(line.contains("request_size_avg=30000.000"));
        assert!(line.contains("record_batch_payload_bytes_per_request_avg=29000.000"));
    }

    fn assert_float_eq(actual: f64, expected: f64) {
        assert!((actual - expected).abs() < f64::EPSILON);
    }
}
