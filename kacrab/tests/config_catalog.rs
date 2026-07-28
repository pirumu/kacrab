//! Public config catalog behavior.

use kacrab::config::{
    AdminConfig, CONFIG_CATALOG, ClientConfig, ClientKind, ConfigError, ConfigOrigin, ConfigStatus,
    KAFKA_CONFIG_SOURCE_REF, ProducerConfig, Properties, UnknownKeyPolicy, WarningReport,
    WarningSeverity, catalog_for, validate_properties,
};

/// Producer key that is cataloged but has no typed `ProducerConfig` field.
///
/// Chosen from the three producer catalog entries whose status is `Native` or
/// `NativeReview` and whose `rust_field` has no matching typed field
/// (`metrics.sample.window.ms`, `metrics.num.samples`,
/// `metrics.recording.level`). It carries `feature: None`, so — unlike the
/// `ssl.*`/`sasl.*` gates — it behaves identically under every feature set and
/// reaches the generated unmatched-key loop in both policies.
const UNTYPED_PRODUCER_KEY: &str = "metrics.sample.window.ms";

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

/// Pins the set of gate labels the catalog uses for `FeatureGated`/`Future`
/// statuses.
///
/// The label→feature support map is generated into `catalog.rs` from
/// `GATE_LABEL_FEATURES` in `kacrab-codegen/src/kafka_config/rust_catalog.rs`,
/// and generation fails on a label missing from that table. This pin is the
/// second line of defense: it catches a committed catalog whose label set
/// changed without this crate's tests being reviewed.
#[test]
fn catalog_gate_labels_are_mapped_by_feature_support() {
    let mut labels: Vec<&str> = CONFIG_CATALOG
        .iter()
        .filter_map(|entry| match entry.status {
            ConfigStatus::FeatureGated { feature } | ConfigStatus::Future { feature } => {
                Some(feature)
            },
            _ => None,
        })
        .collect();
    labels.sort_unstable();
    labels.dedup();

    assert_eq!(
        labels,
        ["sasl", "tls-rustls"],
        "the catalog's gate-label set changed; review GATE_LABEL_FEATURES in \
         kacrab-codegen/src/kafka_config/rust_catalog.rs and the feature-gating tests in this \
         file, then update this pin"
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
    report.push_unsupported_key(ClientKind::Producer, UNTYPED_PRODUCER_KEY);

    assert_eq!(report.warnings().len(), 3);
    assert_eq!(report.warnings()[0].severity, WarningSeverity::Warning);
    assert_eq!(report.warnings()[0].client, ClientKind::Producer);
    assert_eq!(report.warnings()[0].key, "unknown.kafka.key");
    assert!(report.warnings()[0].message.contains("unknown"));

    assert_eq!(report.warnings()[1].key, "ssl.truststore.location");
    assert!(report.warnings()[1].message.contains("tls-rustls"));

    assert_eq!(report.warnings()[2].severity, WarningSeverity::Warning);
    assert_eq!(report.warnings()[2].client, ClientKind::Producer);
    assert_eq!(report.warnings()[2].key, UNTYPED_PRODUCER_KEY);
    assert_eq!(
        report.warnings()[2].message,
        format!(
            "Kafka config key `{UNTYPED_PRODUCER_KEY}` is not supported by the typed config yet"
        )
    );
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

#[cfg(not(any(feature = "aws-lc-rs-tls", feature = "pure-rust-tls")))]
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

#[cfg(not(any(feature = "aws-lc-rs-tls", feature = "pure-rust-tls")))]
#[test]
fn feature_gated_security_keys_are_errors_when_strict() {
    let properties = Properties::from_iter([("ssl.truststore.location", "/tmp/truststore.pem")]);

    let error = validate_properties(ClientKind::Producer, &properties, UnknownKeyPolicy::Deny)
        .expect_err("strict mode must not silently accept unsupported security credentials");

    assert_eq!(
        error,
        ConfigError::UnsupportedFeature {
            client: ClientKind::Producer,
            key: "ssl.truststore.location".into(),
            feature: "tls-rustls",
        }
    );
}

#[cfg(any(feature = "aws-lc-rs-tls", feature = "pure-rust-tls"))]
#[test]
fn feature_gated_security_keys_are_accepted_when_tls_is_compiled() {
    let properties = Properties::from_iter([("ssl.truststore.location", "/tmp/truststore.pem")]);

    for policy in [UnknownKeyPolicy::Deny, UnknownKeyPolicy::Report] {
        let report = validate_properties(ClientKind::Producer, &properties, policy)
            .expect("a compiled TLS provider makes ssl.* keys supported");

        assert!(
            report.warnings().is_empty(),
            "supported security keys must not warn under {policy:?}"
        );
    }
}

#[test]
fn sasl_gated_keys_are_accepted_under_every_policy() {
    let properties = Properties::from_iter([("sasl.kerberos.service.name", "kafka")]);

    for policy in [UnknownKeyPolicy::Deny, UnknownKeyPolicy::Report] {
        let report = validate_properties(ClientKind::Producer, &properties, policy)
            .expect("SASL cores are always compiled, so sasl-gated keys are supported");

        assert!(
            report.warnings().is_empty(),
            "supported SASL keys must not warn under {policy:?}"
        );
    }
}

#[test]
fn lenient_producer_parsing_reports_catalogued_but_untyped_keys() {
    let properties = Properties::from_iter([
        ("bootstrap.servers", "localhost:9092"),
        (UNTYPED_PRODUCER_KEY, "30000"),
    ]);

    let (config, report) = ProducerConfig::from_properties(&properties, UnknownKeyPolicy::Report)
        .expect("lenient parsing must report untyped keys instead of erroring");

    assert_eq!(config.bootstrap_servers.as_slice(), ["localhost:9092"]);
    assert_eq!(report.warnings().len(), 1);
    assert_eq!(report.warnings()[0].severity, WarningSeverity::Warning);
    assert_eq!(report.warnings()[0].client, ClientKind::Producer);
    assert_eq!(report.warnings()[0].key, UNTYPED_PRODUCER_KEY);
    assert!(report.warnings()[0].message.contains("not supported"));
}

#[test]
fn lenient_producer_parsing_warns_unknown_keys_exactly_once() {
    let properties = Properties::from_iter([
        ("bootstrap.servers", "localhost:9092"),
        ("unknown.kafka.key", "value"),
    ]);

    let (_config, report) = ProducerConfig::from_properties(&properties, UnknownKeyPolicy::Report)
        .expect("lenient parsing must report unknown keys instead of erroring");

    assert_eq!(
        report.warnings().len(),
        1,
        "an unknown key is warned by validate_properties only, never again by the typed parser"
    );
    assert_eq!(report.warnings()[0].key, "unknown.kafka.key");
    assert!(report.warnings()[0].message.contains("unknown"));
}

#[test]
fn strict_producer_parsing_still_rejects_catalogued_but_untyped_keys() {
    let properties = Properties::from_iter([
        ("bootstrap.servers", "localhost:9092"),
        (UNTYPED_PRODUCER_KEY, "30000"),
    ]);

    let error = ProducerConfig::from_properties(&properties, UnknownKeyPolicy::Deny)
        .expect_err("strict parsing must reject keys without a typed field");

    assert_eq!(
        error,
        ConfigError::UnsupportedKey {
            client: ClientKind::Producer,
            key: UNTYPED_PRODUCER_KEY.into(),
        }
    );
}

#[test]
fn client_config_producer_with_warnings_surfaces_the_report() {
    let client = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set(UNTYPED_PRODUCER_KEY, "30000");

    let (_config, report) = client
        .producer_config_with_warnings(UnknownKeyPolicy::Report)
        .expect("the lenient facade must surface warnings rather than erroring");

    assert_eq!(report.warnings().len(), 1);
    assert_eq!(report.warnings()[0].key, UNTYPED_PRODUCER_KEY);
}

#[test]
fn lenient_admin_parsing_warns_unknown_keys_exactly_once() {
    let properties = Properties::from_iter([
        ("bootstrap.servers", "localhost:9092"),
        ("unknown.kafka.key", "value"),
    ]);

    let (_config, report) = AdminConfig::from_properties(&properties, UnknownKeyPolicy::Report)
        .expect("the macro fix must cover every generated client, not just the producer");

    assert_eq!(report.warnings().len(), 1);
    assert_eq!(report.warnings()[0].client, ClientKind::Admin);
    assert_eq!(report.warnings()[0].key, "unknown.kafka.key");
    assert!(report.warnings()[0].message.contains("unknown"));
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
