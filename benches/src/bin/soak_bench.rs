//! Sustained-load soak harness against the 3-broker compose cluster
//! (`docker-compose.cluster.yml`), with broker-kill and consumer-bounce chaos.
//!
//! A paced idempotent producer writes per-partition sequence numbers while a
//! consumer group reads them back and checks per-partition continuity: a
//! re-read after an uncommitted handover counts as a duplicate (at-least-once,
//! expected around chaos); a forward jump opens gap entries that a late
//! delivery from a racing handover stream can refill (counted `reordered`);
//! a gap that is NEVER refilled is a real LOSS and fails the run.
//! Every `KACRAB_SOAK_SAMPLE_SECS` a row of counters, latency
//! percentiles for the window, and the process RSS is appended to `soak.csv`;
//! chaos events go to `events.log`; a final `report.md` summarizes the run and
//! the exit code reflects the correctness verdict.
//!
//! Environment (defaults target the parameterized cluster compose brought up
//! with `KAFKA{1,2,3}_HOST_PORT=29092/29094/29096`):
//!
//! ```text
//! KACRAB_SOAK_BOOTSTRAP              127.0.0.1:29092,127.0.0.1:29094,127.0.0.1:29096
//! KACRAB_SOAK_TOPIC                  kacrab-soak
//! KACRAB_SOAK_PARTITIONS             6
//! KACRAB_SOAK_DURATION_SECS          25200 (7h)
//! KACRAB_SOAK_RATE                   1000 records/s
//! KACRAB_SOAK_VALUE_SIZE             512
//! KACRAB_SOAK_SAMPLE_SECS            10
//! KACRAB_SOAK_CHAOS_INTERVAL_SECS    600 (0 disables broker kills)
//! KACRAB_SOAK_CHAOS_DOWNTIME_SECS    45
//! KACRAB_SOAK_CHAOS_CONTAINERS       kacrab-kafka2,kacrab-kafka3
//! KACRAB_SOAK_CONSUMER_BOUNCE_SECS   900 (0 disables consumer bounces)
//! KACRAB_SOAK_OUT_DIR                benches/soak-out
//! ```

#![allow(
    clippy::arithmetic_side_effects,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::print_stdout,
    clippy::print_stderr,
    clippy::too_many_lines,
    clippy::unwrap_used,
    missing_docs,
    reason = "Soak harness binary prefers direct fail-fast setup and explicit output."
)]

use std::{
    env,
    fs::{self, File},
    io::Write as _,
    process::Command,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use bytes::Bytes;
use kacrab::{
    admin::{AdminClient, CreateTopicsOptions, NewTopic},
    consumer::Consumer,
    producer::{Producer, ProducerRecord},
};
use tokio::{runtime::Builder, task::JoinSet};

struct Config {
    bootstrap: String,
    topic: String,
    partitions: i32,
    duration: Duration,
    rate: u64,
    value_size: usize,
    sample_secs: u64,
    chaos_interval_secs: u64,
    chaos_downtime_secs: u64,
    chaos_containers: Vec<String>,
    consumer_bounce_secs: u64,
    consumers: usize,
    out_dir: String,
}

impl Config {
    fn from_env() -> Self {
        Self {
            bootstrap: var(
                "KACRAB_SOAK_BOOTSTRAP",
                "127.0.0.1:29092,127.0.0.1:29094,127.0.0.1:29096",
            ),
            topic: var("KACRAB_SOAK_TOPIC", "kacrab-soak"),
            partitions: var("KACRAB_SOAK_PARTITIONS", "6")
                .parse()
                .expect("partitions"),
            duration: Duration::from_secs(
                var("KACRAB_SOAK_DURATION_SECS", "25200")
                    .parse()
                    .expect("duration"),
            ),
            rate: var("KACRAB_SOAK_RATE", "1000").parse().expect("rate"),
            value_size: var("KACRAB_SOAK_VALUE_SIZE", "512")
                .parse()
                .expect("value size"),
            sample_secs: var("KACRAB_SOAK_SAMPLE_SECS", "10")
                .parse()
                .expect("sample secs"),
            chaos_interval_secs: var("KACRAB_SOAK_CHAOS_INTERVAL_SECS", "600")
                .parse()
                .expect("chaos interval"),
            chaos_downtime_secs: var("KACRAB_SOAK_CHAOS_DOWNTIME_SECS", "45")
                .parse()
                .expect("chaos downtime"),
            chaos_containers: var(
                "KACRAB_SOAK_CHAOS_CONTAINERS",
                "kacrab-kafka2,kacrab-kafka3",
            )
            .split(',')
            .map(str::to_owned)
            .collect(),
            consumer_bounce_secs: var("KACRAB_SOAK_CONSUMER_BOUNCE_SECS", "900")
                .parse()
                .expect("bounce secs"),
            consumers: var("KACRAB_SOAK_CONSUMERS", "2")
                .parse()
                .expect("consumers"),
            out_dir: var("KACRAB_SOAK_OUT_DIR", "benches/soak-out"),
        }
    }
}

fn var(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_error| default.to_owned())
}

#[derive(Default)]
struct Counters {
    produced: AtomicU64,
    acked: AtomicU64,
    send_rejects: AtomicU64,
    delivery_errors: AtomicU64,
    consumed: AtomicU64,
    duplicates: AtomicU64,
    reordered: AtomicU64,
    parse_errors: AtomicU64,
    consumer_restarts: AtomicU64,
    wedges: AtomicU64,
}

struct Shared {
    counters: Counters,
    latencies_us: Mutex<Vec<u64>>,
    error_samples: Mutex<Vec<String>>,
    events: Mutex<Vec<String>>,
    /// Per-partition next sequence the producer will write.
    produced_next: Vec<AtomicI64>,
    /// Per-partition continuity state: the next expected sequence plus any
    /// forward-gap sequences still awaiting a late (reordered or handed-over)
    /// delivery. A gap that is never refilled is a real loss.
    continuity: Vec<Mutex<Continuity>>,
    /// Per-consumer-slot rebalance totals (mirrors `ConsumerMetrics`).
    rebalances: Vec<AtomicU64>,
    /// LOSS lines already emitted to the event log (capped to avoid floods).
    loss_events: AtomicU64,
    /// Producer stops appending (drain begins).
    stop_producing: AtomicBool,
    /// Everything else winds down (after the drain grace).
    shutdown: AtomicBool,
    started: Instant,
}

impl Shared {
    fn new(partitions: i32, consumers: usize) -> Self {
        Self {
            counters: Counters::default(),
            latencies_us: Mutex::new(Vec::new()),
            error_samples: Mutex::new(Vec::new()),
            events: Mutex::new(Vec::new()),
            produced_next: (0..partitions).map(|_i| AtomicI64::new(0)).collect(),
            continuity: (0..partitions)
                .map(|_i| Mutex::new(Continuity::default()))
                .collect(),
            rebalances: (0..consumers).map(|_i| AtomicU64::new(0)).collect(),
            loss_events: AtomicU64::new(0),
            stop_producing: AtomicBool::new(false),
            shutdown: AtomicBool::new(false),
            started: Instant::now(),
        }
    }

    fn event(&self, message: &str) {
        let stamp = self.started.elapsed().as_secs();
        let line = format!("[{stamp:>6}s] {message}");
        println!("{line}");
        self.events.lock().expect("events lock").push(line);
    }

    fn note_error(&self, context: &str, error: &dyn std::fmt::Debug) {
        let stamp = self.started.elapsed().as_secs();
        let line = format!("[{stamp:>6}s] {context}: {error:?}");
        let mut samples = self.error_samples.lock().expect("error samples lock");
        if samples.len() < 200 {
            samples.push(line);
        }
    }
}

fn main() {
    let config = Config::from_env();
    let runtime = Builder::new_multi_thread()
        .worker_threads(4)
        .enable_io()
        .enable_time()
        .build()
        .expect("soak runtime");
    let verdict_ok = runtime.block_on(run(config));
    std::process::exit(i32::from(!verdict_ok));
}

async fn run(config: Config) -> bool {
    fs::create_dir_all(&config.out_dir).expect("create out dir");
    let shared = Arc::new(Shared::new(config.partitions, config.consumers));
    let config = Arc::new(config);

    println!(
        "soak: bootstrap={} topic={} partitions={} duration={}s rate={}/s value={}B chaos={}s/{}s \
         on {:?} bounce={}s out={}",
        config.bootstrap,
        config.topic,
        config.partitions,
        config.duration.as_secs(),
        config.rate,
        config.value_size,
        config.chaos_interval_secs,
        config.chaos_downtime_secs,
        config.chaos_containers,
        config.consumer_bounce_secs,
        config.out_dir,
    );

    create_topic(&config).await;

    let producer = Arc::new(
        Producer::builder()
            .set("bootstrap.servers", config.bootstrap.clone())
            .set("client.id", "kacrab-soak-producer")
            .set("enable.idempotence", "true")
            .set("acks", "all")
            .set("linger.ms", "5")
            .build()
            .await
            .expect("producer connects"),
    );

    let bounce_flags: Arc<[AtomicBool]> = (0..config.consumers)
        .map(|_i| AtomicBool::new(false))
        .collect();
    let mut workers: JoinSet<()> = JoinSet::new();

    for slot in 0..config.consumers {
        let shared = Arc::clone(&shared);
        let config = Arc::clone(&config);
        let bounce_flags = Arc::clone(&bounce_flags);
        let _worker = workers
            .spawn(async move { consumer_loop(slot, &shared, &config, &bounce_flags).await });
    }
    {
        let shared = Arc::clone(&shared);
        let config = Arc::clone(&config);
        let _worker = workers.spawn(async move { broker_chaos_loop(&shared, &config).await });
    }
    {
        let shared = Arc::clone(&shared);
        let config = Arc::clone(&config);
        let bounce_flags = Arc::clone(&bounce_flags);
        let _worker = workers
            .spawn(async move { consumer_bounce_loop(&shared, &config, &bounce_flags).await });
    }
    {
        let shared = Arc::clone(&shared);
        let bounce_flags = Arc::clone(&bounce_flags);
        let _worker =
            workers.spawn(async move { wedge_watchdog_loop(&shared, &bounce_flags).await });
    }
    let sampler = {
        let shared = Arc::clone(&shared);
        let config = Arc::clone(&config);
        let producer = Arc::clone(&producer);
        tokio::spawn(async move { sampler_loop(&shared, &config, &producer).await })
    };

    // Produce until the duration elapses or Ctrl-C, then drain in-flight sends.
    produce_until_deadline(&shared, &config, &producer).await;
    shared.event("producer stopped; draining consumer tail");

    // Give the consumer group a grace window to drain the tail.
    let grace_deadline = Instant::now() + Duration::from_secs(90);
    while Instant::now() < grace_deadline {
        if tail_deficit(&shared) == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    shared.shutdown.store(true, Ordering::SeqCst);
    while workers.join_next().await.is_some() {}
    sampler.await.expect("sampler task");

    write_report(&shared, &config, &producer)
}

async fn create_topic(config: &Config) {
    let admin = AdminClient::from_map([("bootstrap.servers", config.bootstrap.as_str())])
        .await
        .expect("admin connects");
    let topic = NewTopic::new(config.topic.clone(), config.partitions, 3)
        // Keep partitions writable through a single-broker kill with acks=all.
        .config("min.insync.replicas", Some("2".to_owned()))
        // Bound broker disk across a long run.
        .config("retention.ms", Some("3600000".to_owned()));
    match admin
        .create_topics(vec![topic], CreateTopicsOptions::default())
        .await
    {
        Ok(_created) => println!("soak: created topic {}", config.topic),
        Err(error) => {
            let text = format!("{error:?}");
            assert!(
                text.contains("TopicAlreadyExists"),
                "create_topics failed: {text}"
            );
            println!("soak: topic {} already exists", config.topic);
        },
    }
}

async fn produce_until_deadline(
    shared: &Arc<Shared>,
    config: &Arc<Config>,
    producer: &Arc<Producer>,
) {
    const TICK_MS: u64 = 50;
    let per_tick = (config.rate * TICK_MS / 1000).max(1) as usize;
    let mut in_flight: JoinSet<()> = JoinSet::new();
    let mut tick = tokio::time::interval(Duration::from_millis(TICK_MS));
    let deadline = Instant::now() + config.duration;
    let mut partition_cursor: i32 = 0;
    let ctrl_c = tokio::signal::ctrl_c();
    tokio::pin!(ctrl_c);

    loop {
        tokio::select! {
            _instant = tick.tick() => {},
            _signal = &mut ctrl_c => {
                shared.event("Ctrl-C: stopping producer early");
                break;
            },
        }
        if Instant::now() >= deadline {
            break;
        }
        for _n in 0..per_tick {
            let partition = partition_cursor;
            partition_cursor = (partition_cursor + 1) % config.partitions;
            let seq = shared.produced_next[partition as usize].fetch_add(1, Ordering::SeqCst);
            let mut value = format!("s{seq};");
            if value.len() < config.value_size {
                value.push_str(&"x".repeat(config.value_size - value.len()));
            }
            let record =
                ProducerRecord::new(config.topic.clone(), partition).value(Bytes::from(value));
            let enqueued = Instant::now();
            match producer.send(record) {
                Ok(delivery) => {
                    let _prev = shared.counters.produced.fetch_add(1, Ordering::Relaxed);
                    let shared = Arc::clone(shared);
                    let _delivery = in_flight.spawn(async move {
                        match delivery.await {
                            Ok(_receipt) => {
                                let _prev = shared.counters.acked.fetch_add(1, Ordering::Relaxed);
                                let micros = enqueued.elapsed().as_micros() as u64;
                                shared
                                    .latencies_us
                                    .lock()
                                    .expect("latency lock")
                                    .push(micros);
                            },
                            Err(error) => {
                                let _prev = shared
                                    .counters
                                    .delivery_errors
                                    .fetch_add(1, Ordering::Relaxed);
                                shared.note_error("delivery", &error);
                            },
                        }
                    });
                },
                Err(error) => {
                    // The sequence number was reserved but never sent; the
                    // consumer side will see a forward gap unless we mark it.
                    // Rejected sends are producer-side backpressure/fatal
                    // errors — count them and re-produce the same sequence so
                    // the stream stays gapless.
                    let _prev = shared.counters.send_rejects.fetch_add(1, Ordering::Relaxed);
                    shared.note_error("send", &error);
                    let _rewind =
                        shared.produced_next[partition as usize].fetch_sub(1, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(100)).await;
                },
            }
            // Bound the in-flight delivery futures.
            while in_flight.len() > 20_000 {
                let _done = in_flight.join_next().await;
            }
        }
        // Opportunistically reap finished deliveries.
        while let Some(_done) = in_flight.try_join_next() {}
    }

    shared.stop_producing.store(true, Ordering::SeqCst);
    while in_flight.join_next().await.is_some() {}
}

async fn consumer_loop(
    slot: usize,
    shared: &Arc<Shared>,
    config: &Arc<Config>,
    bounce_flags: &Arc<[AtomicBool]>,
) {
    let group = format!("kacrab-soak-{}", config.topic);
    loop {
        if shared.shutdown.load(Ordering::SeqCst) {
            return;
        }
        let client_id = format!("kacrab-soak-consumer-{slot}");
        let mut consumer = match Consumer::from_map([
            ("bootstrap.servers", config.bootstrap.as_str()),
            ("client.id", client_id.as_str()),
            ("group.id", group.as_str()),
            ("auto.offset.reset", "earliest"),
            ("enable.auto.commit", "true"),
        ])
        .await
        {
            Ok(consumer) => consumer,
            Err(error) => {
                shared.note_error("consumer build", &error);
                tokio::time::sleep(Duration::from_secs(2)).await;
                continue;
            },
        };
        if let Err(error) = consumer.subscribe([config.topic.clone()]) {
            shared.note_error("subscribe", &error);
            tokio::time::sleep(Duration::from_secs(2)).await;
            continue;
        }

        loop {
            if shared.shutdown.load(Ordering::SeqCst) {
                consumer.close().await;
                return;
            }
            if bounce_flags[slot].swap(false, Ordering::SeqCst) {
                shared.event(&format!("chaos: bouncing consumer {slot}"));
                consumer.close().await;
                let _prev = shared
                    .counters
                    .consumer_restarts
                    .fetch_add(1, Ordering::Relaxed);
                break; // outer loop recreates the consumer
            }
            match consumer.poll(Duration::from_millis(500)).await {
                Ok(records) => {
                    for record in &records {
                        let _prev = shared.counters.consumed.fetch_add(1, Ordering::Relaxed);
                        track_continuity(shared, record.partition, record.value.as_ref());
                    }
                },
                Err(error) => {
                    shared.note_error("poll", &error);
                    tokio::time::sleep(Duration::from_millis(200)).await;
                },
            }
            let rebalances = consumer.metrics().rebalance_total;
            shared.rebalances[slot].store(rebalances, Ordering::Relaxed);
        }
    }
}

#[derive(Default)]
struct Continuity {
    /// Sequences 0..expected are accounted for, except those in `gaps`.
    expected: i64,
    /// Sequences skipped by a forward jump, awaiting a late delivery.
    gaps: std::collections::BTreeSet<i64>,
}

fn track_continuity(shared: &Shared, partition: i32, value: Option<&Bytes>) {
    let Some(seq) = value.and_then(parse_seq) else {
        let _prev = shared.counters.parse_errors.fetch_add(1, Ordering::Relaxed);
        return;
    };
    let mut state = shared.continuity[partition as usize]
        .lock()
        .expect("continuity lock");
    match seq.cmp(&state.expected) {
        std::cmp::Ordering::Equal => state.expected = seq + 1,
        std::cmp::Ordering::Less => {
            if state.gaps.remove(&seq) {
                // A forward jump raced two delivery streams (e.g. an eager
                // handover); the late stream just filled the hole — reordered
                // observation, not a loss.
                let _prev = shared.counters.reordered.fetch_add(1, Ordering::Relaxed);
            } else {
                // Genuine re-read (at-least-once), expected around chaos.
                let _prev = shared.counters.duplicates.fetch_add(1, Ordering::Relaxed);
            }
        },
        std::cmp::Ordering::Greater => {
            for missing in state.expected..seq {
                let _new = state.gaps.insert(missing);
            }
            if shared.loss_events.fetch_add(1, Ordering::Relaxed) < 50 {
                shared.event(&format!(
                    "GAP: partition {partition} jumped from seq {} to {seq}",
                    state.expected
                ));
            }
            state.expected = seq + 1;
        },
    }
}

/// Compress the unfilled gaps into human-readable per-partition ranges for
/// the report — the forensic starting point for a loss investigation.
fn open_gap_ranges(shared: &Shared, cap: usize) -> String {
    let mut lines = Vec::new();
    for (partition, slot) in shared.continuity.iter().enumerate() {
        let gaps: Vec<i64> = {
            let state = slot.lock().expect("continuity lock");
            state.gaps.iter().copied().collect()
        };
        let mut run_start: Option<(i64, i64)> = None;
        for seq in gaps {
            match run_start {
                Some((start, end)) if seq == end + 1 => run_start = Some((start, seq)),
                Some((start, end)) => {
                    lines.push(format!("p{partition}: {start}..={end}"));
                    run_start = Some((seq, seq));
                },
                None => run_start = Some((seq, seq)),
            }
            if lines.len() >= cap {
                lines.push("… (truncated)".to_owned());
                return lines.join("; ");
            }
        }
        if let Some((start, end)) = run_start {
            lines.push(format!("p{partition}: {start}..={end}"));
        }
    }
    if lines.is_empty() {
        "none".to_owned()
    } else {
        lines.join("; ")
    }
}

/// Sequences that were skipped and never refilled — real losses.
fn open_gaps(shared: &Shared) -> u64 {
    shared
        .continuity
        .iter()
        .map(|slot| slot.lock().expect("continuity lock").gaps.len() as u64)
        .sum()
}

fn parse_seq(bytes: &Bytes) -> Option<i64> {
    let rest = bytes.strip_prefix(b"s")?;
    let end = rest.iter().position(|byte| *byte == b';')?;
    std::str::from_utf8(&rest[..end]).ok()?.parse().ok()
}

/// Detects a wedged consumer group — no consumption progress for three
/// consecutive 30s checks while records remain — logs it, and force-bounces
/// every consumer (what an operator would do). The wedge count and whether
/// the restart healed it are the interesting outputs.
async fn wedge_watchdog_loop(shared: &Arc<Shared>, bounce_flags: &Arc<[AtomicBool]>) {
    let mut last_consumed = 0u64;
    let mut stalled_checks = 0u32;
    loop {
        if sleep_or_shutdown_only(shared, 30).await {
            return;
        }
        let consumed = shared.counters.consumed.load(Ordering::Relaxed);
        let backlog = !shared.stop_producing.load(Ordering::SeqCst) || tail_deficit(shared) > 0;
        if consumed == last_consumed && backlog {
            stalled_checks += 1;
        } else {
            stalled_checks = 0;
        }
        last_consumed = consumed;
        if stalled_checks >= 3 {
            stalled_checks = 0;
            let _prev = shared.counters.wedges.fetch_add(1, Ordering::Relaxed);
            shared.event("WEDGE: no consumption progress for 90s — bouncing all consumers");
            for flag in bounce_flags.iter() {
                flag.store(true, Ordering::SeqCst);
            }
        }
    }
}

/// Like [`sleep_or_shutdown`] but only obeys the hard shutdown flag — the
/// watchdog must stay alive through the post-producer drain.
async fn sleep_or_shutdown_only(shared: &Shared, secs: u64) -> bool {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while Instant::now() < deadline {
        if shared.shutdown.load(Ordering::SeqCst) {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    shared.shutdown.load(Ordering::SeqCst)
}

async fn broker_chaos_loop(shared: &Arc<Shared>, config: &Arc<Config>) {
    if config.chaos_interval_secs == 0 || config.chaos_containers.is_empty() {
        return;
    }
    let mut victim_cursor = 0usize;
    loop {
        if sleep_or_shutdown(shared, config.chaos_interval_secs).await {
            return;
        }
        let victim = &config.chaos_containers[victim_cursor % config.chaos_containers.len()];
        victim_cursor += 1;
        shared.event(&format!("chaos: stopping broker {victim}"));
        docker(&["stop", victim]).await;
        if sleep_or_shutdown(shared, config.chaos_downtime_secs).await {
            // Never leave a broker down at shutdown.
            docker(&["start", victim]).await;
            shared.event(&format!("chaos: restarted broker {victim} during shutdown"));
            return;
        }
        docker(&["start", victim]).await;
        shared.event(&format!("chaos: started broker {victim}"));
    }
}

async fn consumer_bounce_loop(
    shared: &Arc<Shared>,
    config: &Arc<Config>,
    bounce_flags: &Arc<[AtomicBool]>,
) {
    if config.consumer_bounce_secs == 0 {
        return;
    }
    let mut slot_cursor = 0usize;
    loop {
        if sleep_or_shutdown(shared, config.consumer_bounce_secs).await {
            return;
        }
        bounce_flags[slot_cursor % bounce_flags.len()].store(true, Ordering::SeqCst);
        slot_cursor += 1;
    }
}

/// Sleep `secs` in shutdown-aware slices; returns true when shutting down.
async fn sleep_or_shutdown(shared: &Shared, secs: u64) -> bool {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while Instant::now() < deadline {
        if shared.shutdown.load(Ordering::SeqCst) || shared.stop_producing.load(Ordering::SeqCst) {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    shared.shutdown.load(Ordering::SeqCst) || shared.stop_producing.load(Ordering::SeqCst)
}

async fn docker(args: &[&str]) {
    let args: Vec<String> = args.iter().map(|&arg| arg.to_owned()).collect();
    let printable = args.join(" ");
    let output = tokio::task::spawn_blocking(move || {
        Command::new("docker")
            .args(&args)
            .output()
            .expect("docker command runs")
    })
    .await
    .expect("docker task");
    if !output.status.success() {
        eprintln!(
            "docker {printable} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

async fn sampler_loop(shared: &Arc<Shared>, config: &Arc<Config>, producer: &Arc<Producer>) {
    let csv_path = format!("{}/soak.csv", config.out_dir);
    let mut csv = File::create(&csv_path).expect("create soak.csv");
    writeln!(
        csv,
        "elapsed_s,produced,acked,send_rejects,delivery_errors,consumed,duplicates,reordered,\
         open_gaps,parse_errors,win_p50_us,win_p99_us,win_max_us,rss_mib,producer_retries,\
         producer_lib_errors,rebalances,consumer_restarts,wedges"
    )
    .expect("csv header");

    loop {
        tokio::time::sleep(Duration::from_secs(config.sample_secs)).await;
        let window: Vec<u64> = {
            let mut latencies = shared.latencies_us.lock().expect("latency lock");
            std::mem::take(&mut *latencies)
        };
        let (p50, p99, max) = percentiles(window);
        let counters = &shared.counters;
        let metrics = producer.metrics();
        let rebalances: u64 = shared
            .rebalances
            .iter()
            .map(|slot| slot.load(Ordering::Relaxed))
            .sum();
        let row = format!(
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{:.1},{},{},{},{},{}",
            shared.started.elapsed().as_secs(),
            counters.produced.load(Ordering::Relaxed),
            counters.acked.load(Ordering::Relaxed),
            counters.send_rejects.load(Ordering::Relaxed),
            counters.delivery_errors.load(Ordering::Relaxed),
            counters.consumed.load(Ordering::Relaxed),
            counters.duplicates.load(Ordering::Relaxed),
            counters.reordered.load(Ordering::Relaxed),
            open_gaps(shared),
            counters.parse_errors.load(Ordering::Relaxed),
            p50,
            p99,
            max,
            rss_mib(),
            metrics.produce_retry_count,
            metrics.produce_error_count,
            rebalances,
            counters.consumer_restarts.load(Ordering::Relaxed),
            counters.wedges.load(Ordering::Relaxed),
        );
        writeln!(csv, "{row}").expect("csv row");
        csv.flush().expect("csv flush");
        if shared.shutdown.load(Ordering::SeqCst) {
            return;
        }
    }
}

fn percentiles(mut window: Vec<u64>) -> (u64, u64, u64) {
    if window.is_empty() {
        return (0, 0, 0);
    }
    window.sort_unstable();
    let index = |q: f64| window[((window.len() - 1) as f64 * q) as usize];
    (index(0.50), index(0.99), window[window.len() - 1])
}

fn rss_mib() -> f64 {
    let pid = std::process::id().to_string();
    let output = Command::new("ps")
        .args(["-o", "rss=", "-p", &pid])
        .output()
        .expect("ps runs");
    let kib: f64 = String::from_utf8_lossy(&output.stdout)
        .trim()
        .parse()
        .unwrap_or(0.0);
    kib / 1024.0
}

fn tail_deficit(shared: &Shared) -> i64 {
    shared
        .produced_next
        .iter()
        .zip(&shared.continuity)
        .map(|(produced, continuity)| {
            let expected = continuity.lock().expect("continuity lock").expected;
            (produced.load(Ordering::SeqCst) - expected).max(0)
        })
        .sum()
}

fn write_report(shared: &Shared, config: &Config, producer: &Arc<Producer>) -> bool {
    let counters = &shared.counters;
    let losses = open_gaps(shared);
    let deficit = tail_deficit(shared);
    let verdict_ok = losses == 0 && deficit == 0;
    let metrics = producer.metrics();

    let (events_text, events_count) = {
        let events = shared.events.lock().expect("events lock");
        let mut text = events.join("\n");
        text.push('\n');
        (text, events.len())
    };
    let events_path = format!("{}/events.log", config.out_dir);
    fs::write(&events_path, events_text).expect("write events.log");
    let mut errors_text = {
        let error_samples = shared.error_samples.lock().expect("error samples lock");
        error_samples.join("\n")
    };
    if !errors_text.is_empty() {
        errors_text.push('\n');
        let errors_path = format!("{}/errors.log", config.out_dir);
        fs::write(&errors_path, errors_text).expect("write errors.log");
    }

    let report = format!(
        "# kacrab soak run\n\n- duration: {}s (rate target {}/s, value {}B, {} partitions, RF3 \
         min.insync=2)\n- chaos: broker kill every {}s ({}s downtime) on {:?}; consumer bounce \
         every {}s\n\n## Verdict: {}\n\n| metric | value |\n|---|---|\n| produced | {} |\n| acked \
         | {} |\n| send rejects | {} |\n| delivery errors | {} |\n| consumed | {} |\n| duplicates \
         (at-least-once re-reads) | {} |\n| reordered (gaps later refilled) | {} |\n| **losses \
         (gaps never refilled)** | **{}** |\n| unconsumed tail at end | {} |\n| parse errors | {} \
         |\n| producer retries (lib) | {} |\n| producer errors (lib) | {} |\n| rebalances \
         observed | {} |\n| consumer restarts | {} |\n| consumer-group wedges | {} |\n| chaos \
         events | {} |\n\nTime series in `soak.csv`; chaos timeline in `events.log`.\n\nUnfilled \
         gap ranges: {}\n",
        shared.started.elapsed().as_secs(),
        config.rate,
        config.value_size,
        config.partitions,
        config.chaos_interval_secs,
        config.chaos_downtime_secs,
        config.chaos_containers,
        config.consumer_bounce_secs,
        if verdict_ok {
            "PASS — no record loss"
        } else {
            "FAIL"
        },
        counters.produced.load(Ordering::Relaxed),
        counters.acked.load(Ordering::Relaxed),
        counters.send_rejects.load(Ordering::Relaxed),
        counters.delivery_errors.load(Ordering::Relaxed),
        counters.consumed.load(Ordering::Relaxed),
        counters.duplicates.load(Ordering::Relaxed),
        counters.reordered.load(Ordering::Relaxed),
        losses,
        deficit,
        counters.parse_errors.load(Ordering::Relaxed),
        metrics.produce_retry_count,
        metrics.produce_error_count,
        shared
            .rebalances
            .iter()
            .map(|slot| slot.load(Ordering::Relaxed))
            .sum::<u64>(),
        counters.consumer_restarts.load(Ordering::Relaxed),
        counters.wedges.load(Ordering::Relaxed),
        events_count,
        open_gap_ranges(shared, 50),
    );
    let report_path = format!("{}/report.md", config.out_dir);
    fs::write(&report_path, &report).expect("write report.md");
    println!("\n{report}");
    println!("soak: report at {report_path}");
    verdict_ok
}
