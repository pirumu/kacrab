//! Configuration metadata and Java-style property interoperability.
//!
//! The public client configs are Rust-typed builders. This module keeps the
//! Java property names as an interoperability surface and as source metadata
//! for generated docs.

pub mod catalog;
mod client;
mod clients;
mod error;
mod metadata;
mod properties;
mod report;
mod value;

pub use catalog::{
    ADMIN_CONFIGS, CONFIG_CATALOG, CONSUMER_CONFIGS, KAFKA_CONFIG_SOURCE_REF, PRODUCER_CONFIGS,
    catalog_for,
};
pub use client::ClientConfig;
pub use clients::{
    AdminConfig, AdminConfigBuilder, ConsumerConfig, ConsumerConfigBuilder, ProducerConfig,
    ProducerConfigBuilder,
};
pub use error::{ConfigError, ParseConfigValueError};
pub use metadata::{ClientKind, ConfigEntry, ConfigOrigin, ConfigStatus};
pub use properties::{ConfigKey, ConfigValue, Properties};
pub use report::{ConfigWarning, WarningReport, WarningSeverity};
pub use value::{ByteSize, ConfigList, DurationMs, ParseConfigValue, TcpCongestionControl};

/// Unknown-key handling mode for Java-style property parsing.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnknownKeyPolicy {
    /// Unknown keys are errors.
    Deny,
    /// Unknown keys are collected into a report.
    Report,
}

/// Finds catalog metadata for a client/key pair.
#[must_use]
pub fn find_config(client: ClientKind, key: &str) -> Option<&'static ConfigEntry> {
    catalog_for(client).iter().find(|entry| entry.key == key)
}

/// Validates raw Java-style properties against the official config catalog.
///
/// This only validates key support/classification. Typed value parsing happens
/// in client-specific builders.
///
/// # Errors
///
/// Returns [`ConfigError`] when strict mode sees an unknown/Java-only key, or
/// when a security-sensitive feature-gated key is supplied without support.
pub fn validate_properties(
    client: ClientKind,
    properties: &Properties,
    unknown_key_policy: UnknownKeyPolicy,
) -> Result<WarningReport, ConfigError> {
    let mut report = WarningReport::new();

    for (key, _value) in properties.iter() {
        let key = key.as_str();
        let Some(entry) = find_config(client, key) else {
            match unknown_key_policy {
                UnknownKeyPolicy::Deny => {
                    return Err(ConfigError::UnknownKey {
                        client,
                        key: key.into(),
                    });
                },
                UnknownKeyPolicy::Report => report.push_unknown_key(client, key),
            }
            continue;
        };

        match entry.status {
            ConfigStatus::Native | ConfigStatus::NativeReview => {},
            ConfigStatus::FeatureGated { feature } | ConfigStatus::Future { feature } => {
                match unknown_key_policy {
                    UnknownKeyPolicy::Deny => {},
                    UnknownKeyPolicy::Report => {
                        return Err(ConfigError::UnsupportedFeature {
                            client,
                            key: key.into(),
                            feature,
                        });
                    },
                }
            },
            ConfigStatus::SkipJavaOnly => match unknown_key_policy {
                UnknownKeyPolicy::Deny => {
                    return Err(ConfigError::JavaOnly {
                        client,
                        key: key.into(),
                        reason: entry.comment,
                    });
                },
                UnknownKeyPolicy::Report => report.push_java_only_key(client, key, entry.comment),
            },
        }
    }

    Ok(report)
}
