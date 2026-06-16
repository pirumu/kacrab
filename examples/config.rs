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

fn main() -> Result<(), String> {
    let filter = env::args().nth(1);

    println!("Kafka config source: {KAFKA_CONFIG_SOURCE_REF}");
    println!();

    show_client_config_facade()?;
    show_typed_config_builders()?;
    show_catalogs(filter.as_deref())?;

    Ok(())
}

fn show_client_config_facade() -> Result<(), String> {
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

    let producer = config
        .producer_config()
        .map_err(|error| error.to_string())?;
    println!(
        "typed producer: bootstrap={:?}, client.id={}, acks={}, idempotence={}",
        producer.bootstrap_servers.as_slice(),
        producer.client_id,
        producer.acks,
        producer.enable_idempotence
    );

    let (_producer, report) = config
        .producer_config_with_warnings(UnknownKeyPolicy::Report)
        .map_err(|error| error.to_string())?;
    println!(
        "warnings collected with report policy: {}",
        report.warnings().len()
    );
    println!();

    Ok(())
}

fn show_typed_config_builders() -> Result<(), String> {
    println!("== Typed config builders ==");

    let producer = ProducerConfig::builder()
        .bootstrap_servers("127.0.0.1:9092")
        .client_id("typed-producer")
        .acks("all")
        .enable_idempotence(true)
        .build()
        .map_err(|error| error.to_string())?;
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
        .build()
        .map_err(|error| error.to_string())?;
    println!(
        "ConsumerConfig: bootstrap={:?}, group.id={}, client.id={}",
        consumer.bootstrap_servers.as_slice(),
        consumer.group_id,
        consumer.client_id
    );

    let admin = AdminConfig::builder()
        .bootstrap_servers("127.0.0.1:9092")
        .client_id("typed-admin")
        .build()
        .map_err(|error| error.to_string())?;
    println!(
        "AdminConfig: bootstrap={:?}, client.id={}",
        admin.bootstrap_servers.as_slice(),
        admin.client_id
    );
    println!();

    Ok(())
}

fn show_catalogs(filter: Option<&str>) -> Result<(), String> {
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
            ));
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
