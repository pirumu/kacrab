//! Real Kafka consumer benchmark through the public consumer API.
//!
//! Mirrors Apache Kafka's `ConsumerPerformance` tool
//! (`kafka-consumer-perf-test.sh`): subscribe to the topic as a fresh group,
//! poll in 100 ms slices until the expected record count is read (or the
//! record-fetch timeout trips), track rebalance time separately, and print the
//! same final CSV line so kacrab and Java runs diff cleanly. Java's tool labels
//! its byte column `MB` but computes mebibytes (`bytes / 1024 / 1024`); this
//! binary reproduces that computation, quirk included.
//!
//! One intentional divergence: kacrab has no rebalance-listener callback, so
//! rebalance time is observed as the `assignment()` empty -> non-empty
//! transition around each `poll`, which quantizes it to one poll slice
//! (<= 100 ms) instead of the exact in-callback instant Java records.

#![allow(
    clippy::arithmetic_side_effects,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::print_stdout,
    clippy::struct_excessive_bools,
    clippy::unwrap_used,
    missing_docs,
    reason = "Benchmark binaries prefer direct fail-fast setup and explicit output."
)]

use std::{
    env,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use kacrab::{
    common::TopicPartition,
    config::ClientConfig,
    consumer::{
        AutoOffsetReset, Consumer, ConsumerMetricsSnapshot, ConsumerRuntimeConfig, GroupProtocol,
        IsolationLevel,
    },
    producer::{Producer, ProducerError, ProducerRecord},
    wire::WireError,
};
use tokio::runtime::Builder;

const SMALL_BENCH_RECORDS: usize = 5_000_000;
const LARGE_BENCH_RECORDS: usize = 100_000;
const SMALL_VALUE_SIZE: usize = 10;
const LARGE_VALUE_SIZE: usize = 10 * 1024;
const BENCH_RUNS: usize = 5;
/// Java's `ConsumerPerformance` polls in fixed 100 ms slices.
const POLL_TIMEOUT: Duration = Duration::from_millis(100);
/// Java's `--timeout` default: give up after 10 s without a single record.
const DEFAULT_RECORD_FETCH_TIMEOUT_MS: u64 = 10_000;
/// Java's `--fetch-size` default (`max.partition.fetch.bytes`).
const DEFAULT_FETCH_SIZE: u64 = 1024 * 1024;
/// Java's `--socket-buffer-size` default (`receive.buffer.bytes`).
const DEFAULT_SOCKET_BUFFER: u64 = 2 * 1024 * 1024;

fn main() {
    let options = BenchOptions::from_env();
    // Default to a multi-thread runtime so the background heartbeat/auto-commit
    // tasks run concurrently with the poll loop, like the Java consumer's
    // network thread. KACRAB_BENCH_CURRENT_THREAD=1 forces single-thread.
    let runtime = if options.current_thread {
        Builder::new_current_thread()
            .enable_io()
            .enable_time()
            .build()
            .expect("benchmark runtime")
    } else {
        Builder::new_multi_thread()
            .worker_threads(options.workers)
            .enable_io()
            .enable_time()
            .build()
            .expect("benchmark runtime")
    };
    runtime.block_on(async {
        println!(
            "real Kafka consumer benchmark: bootstrap={}, mode={}, group.protocol={}, \
             record_fetch_timeout_ms={}",
            options.bootstrap,
            options.mode_label(),
            options.group_protocol,
            options.record_fetch_timeout.as_millis()
        );
        println!("{}", java_perf_header());
        for scenario in &options.scenarios {
            if options.prefill {
                prefill_topic(&options, scenario).await;
            }
            let mut summaries = Vec::with_capacity(options.runs);
            let mut counters = Vec::with_capacity(options.runs);
            for run_index in 1..=options.runs {
                println!(
                    "scenario=\"{}\", topic={}, run={run_index}/{}",
                    scenario.name, scenario.topic, options.runs
                );
                let (summary, metrics) = run_scenario(&options, scenario, run_index).await;
                println!("{}", summary.java_perf_line());
                println!("{}", format_counter_line(&metrics));
                summaries.push(summary);
                counters.push(metrics);
            }
            println!("{}", format_average_result_line(scenario, &summaries));
            println!("{}", format_average_counter_line(&counters));
        }
    });
}

#[derive(Debug, Clone)]
struct Scenario {
    name: String,
    topic: String,
    records: usize,
    value_size: usize,
}

#[derive(Debug, Clone)]
struct BenchOptions {
    bootstrap: String,
    runs: usize,
    scenarios: Vec<Scenario>,
    group_protocol: String,
    /// `Some(partition_count)` switches to manual `assign` over partitions
    /// `0..partition_count` (no group membership, auto-commit off) to isolate
    /// the pure fetch path. Java's tool has no equivalent mode.
    manual_assign: Option<i32>,
    fetch_size: u64,
    socket_buffer: u64,
    max_poll_records: Option<String>,
    fetch_max_bytes: Option<String>,
    check_crcs: bool,
    from_latest: bool,
    record_fetch_timeout: Duration,
    prefill: bool,
    current_thread: bool,
    workers: usize,
}

impl BenchOptions {
    /// Read every knob exactly once, before any measured work. Nothing in the
    /// poll loop touches the environment (macOS `getenv` takes a global libc
    /// lock; per-record reads poisoned the producer bench by ~28%).
    fn from_env() -> Self {
        let manual_assign = env_flag("KACRAB_BENCH_ASSIGN").then(|| {
            env::var("KACRAB_BENCH_PARTITIONS")
                .ok()
                .and_then(|value| value.parse::<i32>().ok())
                .filter(|partitions| *partitions > 0)
                .expect("KACRAB_BENCH_ASSIGN=1 requires KACRAB_BENCH_PARTITIONS=<count>")
        });
        Self {
            bootstrap: env::var("KACRAB_BOOTSTRAP")
                .unwrap_or_else(|_error| "127.0.0.1:9092".to_owned()),
            runs: env::var("KACRAB_BENCH_RUNS")
                .ok()
                .and_then(|value| value.parse::<usize>().ok())
                .filter(|runs| *runs > 0)
                .unwrap_or(BENCH_RUNS),
            scenarios: scenarios_for(
                env_flag("KACRAB_ONLY_10B"),
                env_flag("KACRAB_ONLY_10KIB"),
                env::var("KACRAB_BENCH_MESSAGES")
                    .ok()
                    .and_then(|value| value.parse::<usize>().ok()),
                env::var("KACRAB_BENCH_TOPIC").ok().as_deref(),
            ),
            group_protocol: env::var("KACRAB_BENCH_GROUP_PROTOCOL")
                .unwrap_or_else(|_error| "classic".to_owned()),
            manual_assign,
            fetch_size: env::var("KACRAB_BENCH_FETCH_SIZE")
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(DEFAULT_FETCH_SIZE),
            socket_buffer: env::var("KACRAB_BENCH_SOCKET_BUFFER")
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(DEFAULT_SOCKET_BUFFER),
            max_poll_records: env::var("KACRAB_BENCH_MAX_POLL_RECORDS").ok(),
            fetch_max_bytes: env::var("KACRAB_BENCH_FETCH_MAX_BYTES").ok(),
            check_crcs: env_flag("KACRAB_BENCH_CHECK_CRCS"),
            from_latest: env_flag("KACRAB_BENCH_FROM_LATEST"),
            record_fetch_timeout: Duration::from_millis(
                env::var("KACRAB_BENCH_TIMEOUT_MS")
                    .ok()
                    .and_then(|value| value.parse::<u64>().ok())
                    .unwrap_or(DEFAULT_RECORD_FETCH_TIMEOUT_MS),
            ),
            prefill: env_flag("KACRAB_BENCH_PREFILL"),
            current_thread: env_flag("KACRAB_BENCH_CURRENT_THREAD"),
            workers: env::var("KACRAB_BENCH_WORKERS")
                .ok()
                .and_then(|value| value.parse::<usize>().ok())
                .filter(|value| *value > 0)
                .unwrap_or(4),
        }
    }

    fn mode_label(&self) -> String {
        self.manual_assign.map_or_else(
            || "subscribe".to_owned(),
            |partitions| format!("assign({partitions} partitions)"),
        )
    }
}

fn env_flag(key: &str) -> bool {
    env::var(key).is_ok_and(|value| value == "1" || value.eq_ignore_ascii_case("true"))
}

/// Fixed scenarios matching the producer bench: the record counts are what a
/// prior prefill wrote, so `data.consumed.in.MB` reflects real record bytes.
fn scenarios_for(
    only_10b: bool,
    only_10kib: bool,
    messages_override: Option<usize>,
    topic_override: Option<&str>,
) -> Vec<Scenario> {
    let small_topic = topic_override.unwrap_or("kacrab-bench").to_owned();
    let large_topic = topic_override.unwrap_or("kacrab-bench-10k").to_owned();
    let mut small = Scenario {
        name: "5,000,000 records x 10 bytes".to_owned(),
        topic: small_topic,
        records: SMALL_BENCH_RECORDS,
        value_size: SMALL_VALUE_SIZE,
    };
    let mut large = Scenario {
        name: "100,000 records x 10 KiB".to_owned(),
        topic: large_topic,
        records: LARGE_BENCH_RECORDS,
        value_size: LARGE_VALUE_SIZE,
    };
    if only_10kib {
        if let Some(messages) = messages_override {
            large.records = messages;
        }
        return vec![large];
    }
    if let Some(messages) = messages_override {
        small.records = messages;
        return vec![small];
    }
    if only_10b {
        return vec![small];
    }
    vec![small, large]
}

fn fresh_group_id(run_index: usize) -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    format!("perf-consumer-kacrab-{millis}-{run_index}")
}

/// The consumer config for one run, mirroring the Java tool's `props()`:
/// fresh group, earliest reset, 1 MiB partition fetch, 2 MiB socket buffer,
/// CRC checks off, `perf-consumer-client` client id.
fn benchmark_client_config(options: &BenchOptions, group_id: &str) -> ClientConfig {
    let mut config = ClientConfig::new()
        .set("bootstrap.servers", options.bootstrap.clone())
        .set("client.id", "perf-consumer-client")
        .set("group.id", group_id)
        .set(
            "auto.offset.reset",
            if options.from_latest {
                "latest"
            } else {
                "earliest"
            },
        )
        .set("max.partition.fetch.bytes", options.fetch_size.to_string())
        .set("receive.buffer.bytes", options.socket_buffer.to_string())
        .set(
            "check.crcs",
            if options.check_crcs { "true" } else { "false" },
        )
        .set("group.protocol", options.group_protocol.as_str());
    if options.manual_assign.is_some() {
        // Manual assignment probes the pure fetch path; a background
        // auto-commit would add coordinator traffic Java's tool never has.
        config = config.set("enable.auto.commit", "false");
    }
    if let Some(max_poll_records) = &options.max_poll_records {
        config = config.set("max.poll.records", max_poll_records.as_str());
    }
    if let Some(fetch_max_bytes) = &options.fetch_max_bytes {
        config = config.set("fetch.max.bytes", fetch_max_bytes.as_str());
    }
    config
}

fn format_effective_config_snapshot(
    runtime: &ConsumerRuntimeConfig,
    options: &BenchOptions,
) -> String {
    format!(
        "effective consumer config: bootstrap.servers={}, client.id={}, group.id={}, \
         group.protocol={}, auto.offset.reset={}, partition.assignment.strategy={}, \
         isolation.level={}, fetch.min.bytes={}, fetch.max.bytes={}, fetch.max.wait.ms={}, \
         max.partition.fetch.bytes={}, max.poll.records={}, check.crcs={}, enable.auto.commit={}, \
         receive.buffer.bytes={}",
        options.bootstrap,
        runtime.client_id,
        runtime.group_id,
        group_protocol_label(runtime.group_protocol),
        auto_offset_reset_label(runtime.auto_offset_reset),
        runtime.partition_assignment_strategy.join(","),
        isolation_level_label(runtime.isolation_level),
        runtime.fetch_min_bytes,
        runtime.fetch_max_bytes,
        runtime.fetch_max_wait_ms,
        runtime.max_partition_fetch_bytes,
        runtime.max_poll_records,
        runtime.check_crcs,
        runtime.enable_auto_commit,
        options.socket_buffer
    )
}

const fn group_protocol_label(protocol: GroupProtocol) -> &'static str {
    match protocol {
        GroupProtocol::Classic => "classic",
        GroupProtocol::Consumer => "consumer",
    }
}

const fn auto_offset_reset_label(reset: AutoOffsetReset) -> &'static str {
    match reset {
        AutoOffsetReset::Earliest => "earliest",
        AutoOffsetReset::Latest => "latest",
        AutoOffsetReset::None => "none",
    }
}

const fn isolation_level_label(isolation: IsolationLevel) -> &'static str {
    match isolation {
        IsolationLevel::ReadUncommitted => "read_uncommitted",
        IsolationLevel::ReadCommitted => "read_committed",
    }
}

async fn run_scenario(
    options: &BenchOptions,
    scenario: &Scenario,
    run_index: usize,
) -> (ConsumerPerfSummary, ConsumerMetricsSnapshot) {
    let group_id = fresh_group_id(run_index);
    let client_config = benchmark_client_config(options, &group_id);
    let typed = client_config
        .consumer_config()
        .expect("benchmark consumer config should parse");
    let runtime_config =
        ConsumerRuntimeConfig::from_config(&typed).expect("benchmark runtime config should map");
    println!(
        "{}",
        format_effective_config_snapshot(&runtime_config, options)
    );
    let mut consumer = Consumer::from_client_config(&client_config)
        .await
        .expect("benchmark consumer should build");

    let group_mode = options.manual_assign.is_none();
    if let Some(partitions) = options.manual_assign {
        let assigned: Vec<_> = (0..partitions)
            .map(|partition| TopicPartition::new(scenario.topic.clone(), partition))
            .collect();
        consumer.assign(assigned.clone());
        consumer
            .seek_to_beginning(&assigned)
            .await
            .expect("benchmark seek_to_beginning should succeed");
    } else {
        consumer
            .subscribe([scenario.topic.clone()])
            .expect("benchmark subscribe should succeed");
    }

    let summary = consume(
        &mut consumer,
        scenario.records,
        options.record_fetch_timeout,
        group_mode,
    )
    .await;
    let metrics = consumer.metrics();
    // Leave the group outside the measured window (endMs is already taken).
    consumer.close().await;
    (summary, metrics)
}

/// The Java tool's `consume()` loop: poll in 100 ms slices until `num_records`
/// are read, bail out when no record arrived for `record_fetch_timeout`, count
/// key + value bytes only.
async fn consume(
    consumer: &mut Consumer,
    num_records: usize,
    record_fetch_timeout: Duration,
    group_mode: bool,
) -> ConsumerPerfSummary {
    let start_system = SystemTime::now();
    let start = Instant::now();
    let mut join = JoinTimeTracker::new(start);
    let mut records_read = 0usize;
    let mut bytes_read = 0usize;
    let mut last_consumed = start;
    while records_read < num_records && last_consumed.elapsed() <= record_fetch_timeout {
        let records = consumer
            .poll(POLL_TIMEOUT)
            .await
            .expect("benchmark poll should succeed");
        let now = Instant::now();
        if group_mode {
            join.observe(!consumer.assignment().is_empty(), now);
        }
        if !records.is_empty() {
            last_consumed = now;
        }
        for record in records.iter() {
            records_read += 1;
            bytes_read += record.key.as_ref().map_or(0, Bytes::len)
                + record.value.as_ref().map_or(0, Bytes::len);
        }
    }
    let elapsed = start.elapsed();
    let end_system = SystemTime::now();
    if records_read < num_records {
        println!(
            "WARNING: Exiting before consuming the expected number of records: timeout ({} ms) \
             exceeded. You can use KACRAB_BENCH_TIMEOUT_MS to increase the timeout.",
            record_fetch_timeout.as_millis()
        );
    }
    ConsumerPerfSummary {
        start: start_system,
        end: end_system,
        records: records_read,
        bytes: bytes_read,
        elapsed,
        join_time: join.total,
    }
}

/// Mirrors the Java tool's `ConsumerPerfRebListener` without a listener API:
/// the time spent with an empty assignment (initially and after a
/// revoke-to-empty) counts as rebalance time, closed out on the first poll
/// that observes a non-empty assignment.
#[derive(Debug)]
struct JoinTimeTracker {
    join_start: Instant,
    assigned: bool,
    total: Duration,
}

impl JoinTimeTracker {
    const fn new(join_start: Instant) -> Self {
        Self {
            join_start,
            assigned: false,
            total: Duration::ZERO,
        }
    }

    fn observe(&mut self, assigned_now: bool, now: Instant) {
        if !self.assigned && assigned_now {
            self.total += now.saturating_duration_since(self.join_start);
        }
        if self.assigned && !assigned_now {
            self.join_start = now;
        }
        self.assigned = assigned_now;
    }
}

#[derive(Debug, Clone, Copy)]
struct ConsumerPerfSummary {
    start: SystemTime,
    end: SystemTime,
    records: usize,
    bytes: usize,
    elapsed: Duration,
    join_time: Duration,
}

impl ConsumerPerfSummary {
    /// Java's label says `MB` but the computation is mebibytes; reproduced as-is.
    fn total_mb(&self) -> f64 {
        f64_from_usize(self.bytes) / (1024.0 * 1024.0)
    }

    const fn elapsed_secs(&self) -> f64 {
        self.elapsed.as_secs_f64().max(0.001)
    }

    fn fetch_time(&self) -> Duration {
        self.elapsed
            .saturating_sub(self.join_time)
            .max(Duration::from_millis(1))
    }

    fn records_per_sec(&self) -> f64 {
        f64_from_usize(self.records) / self.elapsed_secs()
    }

    fn mb_per_sec(&self) -> f64 {
        self.total_mb() / self.elapsed_secs()
    }

    fn fetch_records_per_sec(&self) -> f64 {
        f64_from_usize(self.records) / self.fetch_time().as_secs_f64()
    }

    fn fetch_mb_per_sec(&self) -> f64 {
        self.total_mb() / self.fetch_time().as_secs_f64()
    }

    /// The Java tool's final stats line, column for column.
    fn java_perf_line(&self) -> String {
        format!(
            "{}, {}, {:.4}, {:.4}, {}, {:.4}, {}, {}, {:.4}, {:.4}",
            format_utc(self.start),
            format_utc(self.end),
            self.total_mb(),
            self.mb_per_sec(),
            self.records,
            self.records_per_sec(),
            self.join_time.as_millis(),
            self.fetch_time().as_millis(),
            self.fetch_mb_per_sec(),
            self.fetch_records_per_sec()
        )
    }
}

fn java_perf_header() -> String {
    "start.time, end.time, data.consumed.in.MB, MB.sec, data.consumed.in.nMsg, nMsg.sec, \
     rebalance.time.ms, fetch.time.ms, fetch.MB.sec, fetch.nMsg.sec"
        .to_owned()
}

fn format_average_result_line(scenario: &Scenario, summaries: &[ConsumerPerfSummary]) -> String {
    let runs = f64_from_usize(summaries.len().max(1));
    let records_per_sec = summaries
        .iter()
        .map(ConsumerPerfSummary::records_per_sec)
        .sum::<f64>()
        / runs;
    let mb_per_sec = summaries
        .iter()
        .map(ConsumerPerfSummary::mb_per_sec)
        .sum::<f64>()
        / runs;
    let fetch_records_per_sec = summaries
        .iter()
        .map(ConsumerPerfSummary::fetch_records_per_sec)
        .sum::<f64>()
        / runs;
    let fetch_mb_per_sec = summaries
        .iter()
        .map(ConsumerPerfSummary::fetch_mb_per_sec)
        .sum::<f64>()
        / runs;
    let rebalance_ms = summaries
        .iter()
        .map(|summary| f64_from_u128(summary.join_time.as_millis()))
        .sum::<f64>()
        / runs;
    format!(
        "{}: {:.0} records/s, {:.3} MB/s, fetch {:.0} records/s, {:.3} MB/s, \
         rebalance_avg={rebalance_ms:.0} ms (average over {} runs)",
        scenario.name,
        records_per_sec,
        mb_per_sec,
        fetch_records_per_sec,
        fetch_mb_per_sec,
        summaries.len()
    )
}

fn format_counter_line(metrics: &ConsumerMetricsSnapshot) -> String {
    format!(
        "rust consumer counters: polls={}, fetch_requests={}, records_consumed={}, \
         records_per_poll_avg={:.3}, records_per_fetch_avg={:.3}, commits={}, heartbeats={}, \
         rebalances={}",
        metrics.poll_total,
        metrics.fetch_total,
        metrics.records_consumed_total,
        average_counter(metrics.records_consumed_total, metrics.poll_total),
        average_counter(metrics.records_consumed_total, metrics.fetch_total),
        metrics.commit_total,
        metrics.heartbeat_total,
        metrics.rebalance_total
    )
}

fn format_average_counter_line(counters: &[ConsumerMetricsSnapshot]) -> String {
    let runs = f64_from_usize(counters.len().max(1));
    let polls = counters
        .iter()
        .map(|snapshot| snapshot.poll_total)
        .sum::<u64>();
    let fetches = counters
        .iter()
        .map(|snapshot| snapshot.fetch_total)
        .sum::<u64>();
    let records = counters
        .iter()
        .map(|snapshot| snapshot.records_consumed_total)
        .sum::<u64>();
    let commits = counters
        .iter()
        .map(|snapshot| snapshot.commit_total)
        .sum::<u64>();
    let heartbeats = counters
        .iter()
        .map(|snapshot| snapshot.heartbeat_total)
        .sum::<u64>();
    let rebalances = counters
        .iter()
        .map(|snapshot| snapshot.rebalance_total)
        .sum::<u64>();
    format!(
        "rust average counters: polls={:.3}, fetch_requests={:.3}, records_consumed={:.3}, \
         records_per_poll_avg={:.3}, records_per_fetch_avg={:.3}, commits={:.3}, \
         heartbeats={:.3}, rebalances={:.3}",
        f64_from_u64(polls) / runs,
        f64_from_u64(fetches) / runs,
        f64_from_u64(records) / runs,
        average_counter(records, polls),
        average_counter(records, fetches),
        f64_from_u64(commits) / runs,
        f64_from_u64(heartbeats) / runs,
        f64_from_u64(rebalances) / runs
    )
}

/// Fill the scenario topic once so every measured run (kacrab and Java) reads
/// the same broker data. Uses the producer's default Kafka-compatible config
/// (`acks=all`, idempotence on) with backpressure-aware retries.
async fn prefill_topic(options: &BenchOptions, scenario: &Scenario) {
    println!(
        "prefill: topic={}, records={}, value_size={}",
        scenario.topic, scenario.records, scenario.value_size
    );
    let mut producer = Producer::builder()
        .set("bootstrap.servers", options.bootstrap.clone())
        .set("client.id", "kacrab-consumer-bench-prefill")
        .build()
        .await
        .expect("prefill producer should build");
    let topic = Arc::<str>::from(scenario.topic.as_str());
    let value = Bytes::from(vec![b'x'; scenario.value_size]);
    let started = Instant::now();
    let mut sent = 0usize;
    while sent < scenario.records {
        let record = ProducerRecord::unassigned(Arc::clone(&topic)).value(value.clone());
        match producer.send_with_callback(record, |result| {
            if result.is_err() {
                eprintln!("prefill delivery error: {result:?}");
            }
        }) {
            Ok(_delivery) => sent += 1,
            Err(ProducerError::Backpressure | ProducerError::Wire(WireError::Backpressure)) => {
                tokio::time::sleep(Duration::from_micros(50)).await;
            },
            Err(error) => panic!("prefill send failed: {error:?}"),
        }
    }
    producer
        .flush()
        .await
        .expect("prefill flush should succeed");
    println!("prefill: done in {:.3}s", started.elapsed().as_secs_f64());
}

fn average_counter(total: u64, count: u64) -> f64 {
    if count == 0 {
        0.0
    } else {
        f64_from_u64(total) / f64_from_u64(count)
    }
}

fn f64_from_usize(value: usize) -> f64 {
    f64_from_u64(u64::try_from(value).unwrap_or(u64::MAX))
}

fn f64_from_u64(value: u64) -> f64 {
    let high = u32::try_from(value >> 32).unwrap_or(u32::MAX);
    let low = u32::try_from(value & u64::from(u32::MAX)).unwrap_or(u32::MAX);
    f64::from(high) * 4_294_967_296.0 + f64::from(low)
}

fn f64_from_u128(value: u128) -> f64 {
    f64_from_u64(u64::try_from(value).unwrap_or(u64::MAX))
}

/// `yyyy-MM-dd HH:mm:ss:SSS` in UTC — the Java tool's default date format
/// (which prints broker-host local time; only the two rate columns are
/// compared numerically).
fn format_utc(time: SystemTime) -> String {
    let since_epoch = time.duration_since(UNIX_EPOCH).unwrap_or_default();
    let millis = since_epoch.subsec_millis();
    let secs = i64::try_from(since_epoch.as_secs()).unwrap_or(i64::MAX);
    let days = secs.div_euclid(86_400);
    let secs_of_day = secs.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    format!(
        "{year:04}-{month:02}-{day:02} {:02}:{:02}:{:02}:{millis:03}",
        secs_of_day / 3600,
        (secs_of_day % 3600) / 60,
        secs_of_day % 60
    )
}

/// Days-since-epoch to (year, month, day), Howard Hinnant's `civil_from_days`.
const fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let shifted = days + 719_468;
    let era = shifted.div_euclid(146_097);
    let day_of_era = shifted.rem_euclid(146_097);
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_point = (5 * day_of_year + 2) / 153;
    let day = (day_of_year - (153 * month_point + 2) / 5 + 1) as u32;
    let month = if month_point < 10 {
        month_point + 3
    } else {
        month_point - 9
    } as u32;
    let year = year_of_era + era * 400 + if month <= 2 { 1 } else { 0 };
    (year, month, day)
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant, UNIX_EPOCH};

    use super::{
        BENCH_RUNS, ConsumerPerfSummary, JoinTimeTracker, Scenario, civil_from_days,
        format_average_result_line, format_utc, fresh_group_id, java_perf_header, scenarios_for,
    };

    #[test]
    fn scenarios_are_fixed_prefill_matched_payloads() {
        let scenarios = scenarios_for(false, false, None, None);

        assert_eq!(scenarios.len(), 2);
        assert_eq!(scenarios[0].records, 5_000_000);
        assert_eq!(scenarios[0].value_size, 10);
        assert_eq!(scenarios[0].topic, "kacrab-bench");
        assert_eq!(scenarios[1].records, 100_000);
        assert_eq!(scenarios[1].value_size, 10 * 1024);
        assert_eq!(scenarios[1].topic, "kacrab-bench-10k");
    }

    #[test]
    fn scenario_selection_honours_only_and_message_overrides() {
        let only_small = scenarios_for(true, false, None, None);
        assert_eq!(only_small.len(), 1);
        assert_eq!(only_small[0].value_size, 10);

        let only_large = scenarios_for(false, true, Some(42), None);
        assert_eq!(only_large.len(), 1);
        assert_eq!(only_large[0].value_size, 10 * 1024);
        assert_eq!(only_large[0].records, 42);

        let bounded_small = scenarios_for(false, false, Some(1_000), None);
        assert_eq!(bounded_small.len(), 1);
        assert_eq!(bounded_small[0].value_size, 10);
        assert_eq!(bounded_small[0].records, 1_000);
    }

    #[test]
    fn topic_override_applies_to_both_scenarios() {
        let scenarios = scenarios_for(false, false, None, Some("kacrab-16p"));
        assert_eq!(scenarios[0].topic, "kacrab-16p");
        assert_eq!(scenarios[1].topic, "kacrab-16p");
    }

    #[test]
    fn benchmark_averages_over_five_runs() {
        assert_eq!(BENCH_RUNS, 5);
    }

    #[test]
    fn join_tracker_counts_initial_join_and_rejoin_rounds() {
        let start = Instant::now();
        let mut tracker = JoinTimeTracker::new(start);

        // Two polls before the assignment lands: nothing accrues yet.
        tracker.observe(false, start + Duration::from_millis(100));
        assert_eq!(tracker.total, Duration::ZERO);

        // Assignment lands 250 ms in: the whole empty window counts.
        tracker.observe(true, start + Duration::from_millis(250));
        assert_eq!(tracker.total, Duration::from_millis(250));

        // Steady state accrues nothing.
        tracker.observe(true, start + Duration::from_millis(400));
        assert_eq!(tracker.total, Duration::from_millis(250));

        // Revoked-to-empty then reassigned: the second round adds its window.
        tracker.observe(false, start + Duration::from_millis(500));
        tracker.observe(true, start + Duration::from_millis(650));
        assert_eq!(tracker.total, Duration::from_millis(400));
    }

    #[test]
    fn java_perf_line_matches_tool_column_layout() {
        let start = UNIX_EPOCH + Duration::from_millis(86_400_123);
        let summary = ConsumerPerfSummary {
            start,
            end: start + Duration::from_secs(2),
            records: 1_000,
            bytes: 2 * 1024 * 1024,
            elapsed: Duration::from_secs(2),
            join_time: Duration::from_millis(500),
        };

        let line = summary.java_perf_line();

        assert!(line.starts_with("1970-01-02 00:00:00:123, 1970-01-02 00:00:02:123, "));
        // 2 MiB over 2 s wall = 1 MB.sec; over 1.5 s fetch = 1.3333 fetch.MB.sec.
        assert!(line.contains(", 2.0000, 1.0000, 1000, 500.0000, 500, 1500, 1.3333, 666.6667"));
        assert_eq!(
            line.split(", ").count(),
            java_perf_header().split(", ").count()
        );
    }

    #[test]
    fn fetch_time_never_collapses_to_zero() {
        let summary = ConsumerPerfSummary {
            start: UNIX_EPOCH,
            end: UNIX_EPOCH,
            records: 0,
            bytes: 0,
            elapsed: Duration::from_millis(10),
            join_time: Duration::from_millis(50),
        };

        assert_eq!(summary.fetch_time(), Duration::from_millis(1));
    }

    #[test]
    fn average_line_reports_run_averaged_rates() {
        let scenario = Scenario {
            name: "test scenario".to_owned(),
            topic: "t".to_owned(),
            records: 100,
            value_size: 10,
        };
        let base = ConsumerPerfSummary {
            start: UNIX_EPOCH,
            end: UNIX_EPOCH + Duration::from_secs(1),
            records: 1_000,
            bytes: 1024 * 1024,
            elapsed: Duration::from_secs(1),
            join_time: Duration::from_millis(200),
        };
        let slower = ConsumerPerfSummary {
            elapsed: Duration::from_secs(2),
            end: UNIX_EPOCH + Duration::from_secs(2),
            ..base
        };

        let line = format_average_result_line(&scenario, &[base, slower]);

        // (1000/1 + 1000/2) / 2 = 750 records/s.
        assert!(line.starts_with("test scenario: 750 records/s"));
        assert!(line.contains("rebalance_avg=200 ms"));
        assert!(line.contains("(average over 2 runs)"));
    }

    #[test]
    fn utc_formatting_matches_java_default_pattern() {
        assert_eq!(format_utc(UNIX_EPOCH), "1970-01-01 00:00:00:000");
        assert_eq!(
            format_utc(UNIX_EPOCH + Duration::from_millis(86_400_000 + 123)),
            "1970-01-02 00:00:00:123"
        );
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        // 2026-07-02 is 20_636 days after the epoch.
        assert_eq!(civil_from_days(20_636), (2026, 7, 2));
    }

    #[test]
    fn fresh_group_ids_embed_run_index() {
        let first = fresh_group_id(1);
        let second = fresh_group_id(2);

        assert!(first.starts_with("perf-consumer-kacrab-"));
        assert!(first.ends_with("-1"));
        assert!(second.ends_with("-2"));
    }

    #[test]
    fn perf_header_matches_java_tool() {
        assert!(java_perf_header().starts_with("start.time, end.time, data.consumed.in.MB"));
        assert!(java_perf_header().ends_with("fetch.MB.sec, fetch.nMsg.sec"));
    }
}
