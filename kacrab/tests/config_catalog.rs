//! Public config catalog behavior.

use kacrab::config::{
    CONFIG_CATALOG, ClientKind, ConfigError, ConfigOrigin, ConfigStatus, KAFKA_CONFIG_SOURCE_REF,
    Properties, UnknownKeyPolicy, WarningReport, WarningSeverity, catalog_for, validate_properties,
};

#[test]
fn catalog_covers_official_kafka_43_config_pages() {
    assert_eq!(KAFKA_CONFIG_SOURCE_REF, "apache/kafka@4.3.0");
    assert_eq!(catalog_for(ClientKind::Producer).len(), 122);
    assert_eq!(catalog_for(ClientKind::Consumer).len(), 122);
    assert_eq!(catalog_for(ClientKind::Admin).len(), 98);

    assert_eq!(CONFIG_CATALOG.len(), 342);
    assert!(
        CONFIG_CATALOG
            .iter()
            .all(|entry| !entry.documentation.is_empty()),
        "generated catalog entries should keep official Kafka documentation"
    );
}

#[test]
fn catalog_merges_kacrab_runtime_socket_overlay() {
    let quickack = catalog_for(ClientKind::Producer)
        .iter()
        .find(|entry| entry.key == "socket.tcp.quickack")
        .expect("producer socket.tcp.quickack must be cataloged from runtime overlay");

    assert_eq!(quickack.origin, ConfigOrigin::KacrabRuntime);
    assert_eq!(quickack.status, ConfigStatus::Native);
    assert_eq!(quickack.rust_field, "socket_tcp_quickack");
    assert_eq!(
        quickack.platforms,
        &["linux", "android", "fuchsia", "cygwin"]
    );
    assert_eq!(quickack.feature, Some("socket2"));
    assert!(quickack.comment.contains("Available on linux"));
    assert_eq!(
        quickack.source,
        "kacrab-runtime://config/socket.tcp.quickack"
    );
}

#[test]
fn catalog_entries_keep_source_links_and_rust_decisions() {
    let bootstrap = catalog_for(ClientKind::Producer)
        .iter()
        .find(|entry| entry.key == "bootstrap.servers")
        .expect("producer bootstrap.servers must be cataloged");

    assert_eq!(bootstrap.rust_field, "bootstrap_servers");
    assert_eq!(bootstrap.status, ConfigStatus::Native);
    assert_eq!(
        bootstrap.source,
        "https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_bootstrap.servers"
    );
    assert!(bootstrap.comment.contains("typed Rust field"));
    assert!(bootstrap.documentation.contains("Kafka cluster"));

    let serializer = catalog_for(ClientKind::Producer)
        .iter()
        .find(|entry| entry.key == "key.serializer")
        .expect("producer key.serializer must be cataloged");

    assert_eq!(serializer.status, ConfigStatus::SkipJavaOnly);
    assert!(serializer.comment.contains("Java/JVM class"));
}

#[test]
fn warning_report_keeps_lenient_parse_feedback_structured() {
    let mut report = WarningReport::new();
    report.push_unknown_key(ClientKind::Producer, "unknown.kafka.key");
    report.push_unsupported_feature(
        ClientKind::Producer,
        "ssl.truststore.location",
        "tls-rustls",
    );

    assert_eq!(report.warnings().len(), 2);
    assert_eq!(report.warnings()[0].severity, WarningSeverity::Warning);
    assert_eq!(report.warnings()[0].client, ClientKind::Producer);
    assert_eq!(report.warnings()[0].key, "unknown.kafka.key");
    assert!(report.warnings()[0].message.contains("unknown"));

    assert_eq!(report.warnings()[1].key, "ssl.truststore.location");
    assert!(report.warnings()[1].message.contains("tls-rustls"));
}

#[test]
fn strict_property_validation_rejects_unknown_keys() {
    let properties = Properties::from_iter([("unknown.kafka.key", "value")]);

    let error = validate_properties(ClientKind::Producer, &properties, UnknownKeyPolicy::Deny)
        .expect_err("strict validation must reject unknown keys");

    assert_eq!(
        error,
        ConfigError::UnknownKey {
            client: ClientKind::Producer,
            key: "unknown.kafka.key".into()
        }
    );
}

#[test]
fn lenient_property_validation_reports_unknown_and_java_only_keys() {
    let properties = Properties::from_iter([
        ("unknown.kafka.key", "value"),
        (
            "key.serializer",
            "org.apache.kafka.common.serialization.StringSerializer",
        ),
    ]);

    let report = validate_properties(ClientKind::Producer, &properties, UnknownKeyPolicy::Report)
        .expect("lenient validation should collect warnings");

    assert_eq!(report.warnings().len(), 2);
    assert_eq!(report.warnings()[0].key, "key.serializer");
    assert!(report.warnings()[0].message.contains("Java-only"));
    assert_eq!(report.warnings()[1].key, "unknown.kafka.key");
    assert!(report.warnings()[1].message.contains("unknown"));
}

#[test]
fn feature_gated_security_keys_are_errors_even_when_lenient() {
    let properties = Properties::from_iter([("ssl.truststore.location", "/tmp/truststore.pem")]);

    let error = validate_properties(ClientKind::Producer, &properties, UnknownKeyPolicy::Report)
        .expect_err("security credentials must not be silently ignored");

    assert_eq!(
        error,
        ConfigError::UnsupportedFeature {
            client: ClientKind::Producer,
            key: "ssl.truststore.location".into(),
            feature: "tls-rustls",
        }
    );
}

#[test]
fn missing_required_config_error_mentions_key() {
    let error = ConfigError::MissingRequired {
        client: ClientKind::Producer,
        key: "bootstrap.servers",
    };

    assert!(error.to_string().contains("bootstrap.servers"));
    assert!(error.to_string().contains("required"));
}
