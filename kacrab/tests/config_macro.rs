//! `kacrab::kafka_config!` re-export behavior.

#![cfg(feature = "macros")]

use std::string::String;

use kacrab::{
    config::{
        ClientKind, ConfigError, ConfigStatus, ConfigValue, ParseConfigValue, ParseConfigValueError,
    },
    kafka_config,
};

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
    pub ReexportedProducerConfig {
        #[key("bootstrap.servers")]
        #[required]
        #[kafka_type("list")]
        #[kafka_default("")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_bootstrap.servers")]
        #[comment("Rust: parsed into bootstrap server addresses; used only for bootstrap discovery.")]
        bootstrap_servers: String,

        #[key("acks")]
        #[default(Acks::All)]
        #[kafka_type("string")]
        #[kafka_default("all")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_acks")]
        #[comment("Rust: typed producer acknowledgement policy.")]
        acks: Acks,
    }
}

#[test]
fn reexported_macro_generates_public_config_api() {
    let config = ReexportedProducerConfig::builder()
        .bootstrap_servers("localhost:9092")
        .build()
        .expect("bootstrap.servers is provided");

    assert_eq!(
        ReexportedProducerConfig::BOOTSTRAP_SERVERS_CONFIG,
        "bootstrap.servers"
    );
    assert_eq!(config.acks, Acks::All);
    assert_eq!(
        ReexportedProducerConfig::CONFIG_KEYS[0].client,
        ClientKind::Producer
    );
    assert_eq!(
        ReexportedProducerConfig::CONFIG_KEYS[0].status,
        ConfigStatus::Native
    );
}

#[test]
fn reexported_macro_reports_missing_required_field() {
    assert_eq!(
        ReexportedProducerConfig::builder()
            .build()
            .expect_err("bootstrap.servers is required"),
        ConfigError::MissingRequired {
            client: ClientKind::Producer,
            key: "bootstrap.servers",
        }
    );
}
