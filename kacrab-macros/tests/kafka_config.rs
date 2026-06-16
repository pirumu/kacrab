//! `kafka_config!` macro behavior.

use std::string::String;

use kacrab::config::{
    ClientKind, ConfigError, ConfigStatus, ConfigValue, ParseConfigValue, ParseConfigValueError,
};
use kacrab_macros::kafka_config;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Acks {
    All,
}

impl ParseConfigValue for Acks {
    fn parse_config_value(value: &ConfigValue) -> Result<Self, ParseConfigValueError> {
        if value.as_str() == "all" {
            Ok(Self::All)
        } else {
            Err(ParseConfigValueError::new("Acks", value.as_str()))
        }
    }
}

kafka_config! {
    #[client(Producer)]
    pub ExampleProducerConfig {
        #[key("bootstrap.servers")]
        #[required]
        #[kafka_type("list")]
        #[kafka_default("")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_bootstrap.servers")]
        #[comment("Rust: parsed into BootstrapServers; used only for bootstrap discovery.")]
        bootstrap_servers: String,

        #[key("acks")]
        #[default(Acks::All)]
        #[kafka_type("string")]
        #[kafka_default("all")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_acks")]
        #[comment("Rust: typed producer acknowledgement policy.")]
        acks: Acks,

        #[key("ssl.truststore.location")]
        #[default(String::new())]
        #[kafka_type("string")]
        #[kafka_default("null")]
        #[status(feature_gated("tls-rustls"))]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_ssl.truststore.location")]
        #[comment("Rust: available only when TLS support is enabled.")]
        ssl_truststore_location: String,
    }
}

#[test]
fn macro_generates_builder_constants_and_metadata() {
    assert_eq!(
        ExampleProducerConfig::BOOTSTRAP_SERVERS_CONFIG,
        "bootstrap.servers"
    );
    assert_eq!(ExampleProducerConfig::ACKS_CONFIG, "acks");

    let config = ExampleProducerConfig::builder()
        .bootstrap_servers("localhost:9092")
        .build()
        .expect("required field set");

    assert_eq!(config.bootstrap_servers, "localhost:9092");
    assert_eq!(config.acks, Acks::All);

    assert_eq!(ExampleProducerConfig::CONFIG_KEYS.len(), 3);
    assert_eq!(
        ExampleProducerConfig::CONFIG_KEYS[0].client,
        ClientKind::Producer
    );
    assert_eq!(
        ExampleProducerConfig::CONFIG_KEYS[0].status,
        ConfigStatus::Native
    );
    assert_eq!(
        ExampleProducerConfig::CONFIG_KEYS[0].source,
        "https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_bootstrap.servers"
    );
    assert_eq!(
        ExampleProducerConfig::CONFIG_KEYS[2].status,
        ConfigStatus::FeatureGated {
            feature: "tls-rustls"
        }
    );
}

#[test]
fn macro_builder_rejects_missing_required_fields() {
    let error = ExampleProducerConfig::builder()
        .build()
        .expect_err("bootstrap.servers is required");

    assert_eq!(
        error,
        ConfigError::MissingRequired {
            client: ClientKind::Producer,
            key: "bootstrap.servers",
        }
    );
}
