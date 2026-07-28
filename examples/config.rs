//! Show the public Kafka-compatible config surface.
//!
//! Run:
//!
//! ```text
//! cargo run -p kacrab-examples --example config
//! ```
//!
//! Limit output to one client family:
//!
//! ```text
//! cargo run -p kacrab-examples --example config -- producer
//! cargo run -p kacrab-examples --example config -- consumer
//! cargo run -p kacrab-examples --example config -- admin
//! ```

use std::env;

use kacrab::config::{
    AdminConfig, ClientConfig, ClientKind, ConfigEntry, ConfigOrigin, ConfigStatus, ConsumerConfig,
    KAFKA_CONFIG_SOURCE_REF, ProducerConfig, UnknownKeyPolicy, catalog_for,
};

/// Boxed error so every fallible call below can use `?` directly: the config
/// error types implement `std::error::Error`, so no `map_err(|e| e.to_string())`
/// shim is needed to reach `main`'s error type.
type ExampleResult = Result<(), Box<dyn std::error::Error>>;

fn main() -> ExampleResult {
    let filter = env::args().nth(1);

    println!("Kafka config source: {KAFKA_CONFIG_SOURCE_REF}");
    println!();

    show_client_config_facade()?;
    show_typed_config_builders()?;
    show_catalogs(filter.as_deref())?;

    Ok(())
}

fn show_client_config_facade() -> ExampleResult {
    println!("== Java-style ClientConfig facade ==");

    let config = ClientConfig::new()
        .set("bootstrap.servers", "127.0.0.1:9092")
        .set("client.id", "config-example")
        .set("acks", "all")
        .set("enable.idempotence", "true")
        .set("batch.size", "16384")
        .set("linger.ms", "5");

    for (key, value) in config.properties().iter() {
        println!("{key} = {}", value.as_str());
    }

    let producer = config.producer_config()?;
    println!(
        "typed producer: bootstrap={:?}, client.id={}, acks={}, idempotence={}",
        producer.bootstrap_servers.as_slice(),
        producer.client_id,
        producer.acks,
        producer.enable_idempotence
    );

    // Lenient parsing: unknown keys and catalogued keys without a typed field
    // become structured warnings instead of errors, so a config copied from a
    // Java client can be adopted incrementally. (Strict `producer_config()`
    // would reject both extra keys below with a `ConfigError`.)
    let lenient = ClientConfig::new()
        .set("bootstrap.servers", "127.0.0.1:9092")
        .set("metrics.sample.window.ms", "30000")
        .set("some.unknown.key", "value");
    let (_producer, report) = lenient.producer_config_with_warnings(UnknownKeyPolicy::Report)?;
    println!(
        "warnings collected with report policy: {}",
        report.warnings().len()
    );
    for warning in report.warnings() {
        println!("  warning: {}", warning.message);
    }
    println!();

    Ok(())
}

fn show_typed_config_builders() -> ExampleResult {
    println!("== Typed config builders ==");

    let producer = ProducerConfig::builder()
        .bootstrap_servers("127.0.0.1:9092")
        .client_id("typed-producer")
        .acks("all")
        .enable_idempotence(true)
        .build()?;
    println!(
        "ProducerConfig: bootstrap={:?}, client.id={}, acks={}",
        producer.bootstrap_servers.as_slice(),
        producer.client_id,
        producer.acks
    );

    let consumer = ConsumerConfig::builder()
        .bootstrap_servers("127.0.0.1:9092")
        .group_id("config-example-group")
        .client_id("typed-consumer")
        .build()?;
    println!(
        "ConsumerConfig: bootstrap={:?}, group.id={}, client.id={}",
        consumer.bootstrap_servers.as_slice(),
        consumer.group_id,
        consumer.client_id
    );

    let admin = AdminConfig::builder()
        .bootstrap_servers("127.0.0.1:9092")
        .client_id("typed-admin")
        .build()?;
    println!(
        "AdminConfig: bootstrap={:?}, client.id={}",
        admin.bootstrap_servers.as_slice(),
        admin.client_id
    );
    println!();

    Ok(())
}

fn show_catalogs(filter: Option<&str>) -> ExampleResult {
    println!("== Full config catalog ==");
    println!(
        "status: native = typed now, native-review = exposed but review-targeted, feature/future \
         = not always usable, java-only = rejected"
    );
    println!();

    match filter {
        None | Some("all") => {
            show_catalog(ClientKind::Producer);
            show_catalog(ClientKind::Consumer);
            show_catalog(ClientKind::Admin);
        },
        Some("producer") => show_catalog(ClientKind::Producer),
        Some("consumer") => show_catalog(ClientKind::Consumer),
        Some("admin") => show_catalog(ClientKind::Admin),
        Some(other) => {
            return Err(format!(
                "unknown client kind `{other}`; use producer, consumer, admin, or all"
            )
            .into());
        },
    }

    Ok(())
}

fn show_catalog(client: ClientKind) {
    let entries = catalog_for(client);
    println!(
        "-- {} configs ({} keys) --",
        client_label(client),
        entries.len()
    );
    for entry in entries {
        print_entry(entry);
    }
    println!();
}

fn print_entry(entry: &ConfigEntry) {
    println!(
        "{key} | type={kafka_type} | default={default} | status={status} | rust={rust_field} | \
         origin={origin} | feature={feature}",
        key = entry.key,
        kafka_type = entry.kafka_type,
        default = entry.default,
        status = status_label(entry.status),
        rust_field = entry.rust_field,
        origin = origin_label(entry.origin),
        feature = entry.feature.unwrap_or("-")
    );
    println!("  {}", entry.comment);
    println!("  {}", entry.source);
}

const fn client_label(client: ClientKind) -> &'static str {
    match client {
        ClientKind::Producer => "producer",
        ClientKind::Consumer => "consumer",
        ClientKind::Admin => "admin",
    }
}

const fn origin_label(origin: ConfigOrigin) -> &'static str {
    match origin {
        ConfigOrigin::Kafka => "kafka",
        ConfigOrigin::KacrabRuntime => "kacrab-runtime",
    }
}

fn status_label(status: ConfigStatus) -> String {
    match status {
        ConfigStatus::Native => "native".to_owned(),
        ConfigStatus::NativeReview => "native-review".to_owned(),
        ConfigStatus::FeatureGated { feature } => format!("feature-gated({feature})"),
        ConfigStatus::Future { feature } => format!("future({feature})"),
        ConfigStatus::SkipJavaOnly => "java-only".to_owned(),
        _ => "unknown".to_owned(),
    }
}
