//! Configuration errors.

use std::string::String;

use thiserror::Error;

use super::ClientKind;

/// Error returned by strict or security-sensitive config validation.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum ConfigError {
    /// Property key is not present in the official catalog for the client.
    #[error("unknown Kafka config key `{key}`")]
    UnknownKey {
        /// Client family being validated.
        client: ClientKind,
        /// Unknown Kafka property key.
        key: String,
    },
    /// Required property was not supplied.
    #[error("required Kafka config key `{key}` is missing")]
    MissingRequired {
        /// Client family being built.
        client: ClientKind,
        /// Missing Kafka property key.
        key: &'static str,
    },
    /// Property key is Java/JVM specific and has no faithful Rust property form.
    #[error("Java-only Kafka config key `{key}` is not supported: {reason}")]
    JavaOnly {
        /// Client family being validated.
        client: ClientKind,
        /// Java-only Kafka property key.
        key: String,
        /// Explanation of why the key is skipped.
        reason: &'static str,
    },
    /// Property key requires a disabled feature.
    #[error("Kafka config key `{key}` requires feature `{feature}`")]
    UnsupportedFeature {
        /// Client family being validated.
        client: ClientKind,
        /// Kafka property key.
        key: String,
        /// Required Cargo feature.
        feature: &'static str,
    },
    /// Property key is cataloged but not yet exposed by this typed config.
    #[error("Kafka config key `{key}` is not modeled by this typed config")]
    UnsupportedKey {
        /// Client family being parsed.
        client: ClientKind,
        /// Kafka property key.
        key: String,
    },
    /// Property value cannot be parsed into the typed Rust config field.
    #[error("failed to parse Kafka config key `{key}` value `{value}` as {target}")]
    InvalidValue {
        /// Client family being parsed.
        client: ClientKind,
        /// Kafka property key.
        key: &'static str,
        /// Target Rust type name.
        target: &'static str,
        /// Original raw value.
        value: String,
    },
}

/// Error returned when parsing a raw config value into a typed Rust value.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("failed to parse Kafka config value `{value}` as {target}")]
pub struct ParseConfigValueError {
    /// Target Rust type name.
    pub target: &'static str,
    /// Original raw value.
    pub value: String,
}

impl ParseConfigValueError {
    /// Creates a parse error.
    #[must_use]
    pub fn new(target: &'static str, value: impl Into<String>) -> Self {
        Self {
            target,
            value: value.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::expect_used,
        clippy::missing_assert_message,
        clippy::unwrap_used,
        reason = "Unit test fixtures fail fastest with contextual unwrap/expect calls."
    )]

    use super::{ConfigError, ParseConfigValueError};
    use crate::config::ClientKind;

    #[test]
    fn config_error_display_names_each_variant() {
        let errors = [
            ConfigError::UnknownKey {
                client: ClientKind::Producer,
                key: "unknown".to_owned(),
            },
            ConfigError::MissingRequired {
                client: ClientKind::Producer,
                key: "bootstrap.servers",
            },
            ConfigError::JavaOnly {
                client: ClientKind::Producer,
                key: "ssl.engine.factory.class".to_owned(),
                reason: "JVM class hook",
            },
            ConfigError::UnsupportedFeature {
                client: ClientKind::Producer,
                key: "sasl.mechanism".to_owned(),
                feature: "sasl",
            },
            ConfigError::UnsupportedKey {
                client: ClientKind::Producer,
                key: "interceptor.classes".to_owned(),
            },
            ConfigError::InvalidValue {
                client: ClientKind::Producer,
                key: "linger.ms",
                target: "duration milliseconds",
                value: "bad".to_owned(),
            },
        ];

        let rendered: Vec<_> = errors.iter().map(ToString::to_string).collect();

        assert!(rendered[0].contains("unknown Kafka config key"));
        assert!(rendered[1].contains("required Kafka config key"));
        assert!(rendered[2].contains("Java-only Kafka config key"));
        assert!(rendered[3].contains("requires feature"));
        assert!(rendered[4].contains("not modeled"));
        assert!(rendered[5].contains("failed to parse"));
    }

    #[test]
    fn parse_config_value_error_display_includes_target_and_value() {
        let error = ParseConfigValueError::new("usize", "abc");

        assert_eq!(
            error.to_string(),
            "failed to parse Kafka config value `abc` as usize"
        );
    }

    #[test]
    fn config_errors_are_std_errors() {
        const fn assert_std_error<E: std::error::Error>() {}

        assert_std_error::<ConfigError>();
        assert_std_error::<ParseConfigValueError>();

        // The payoff: a config failure converts into the `Box<dyn Error>` a
        // `main` returns, so callers stop writing `map_err(|e| e.to_string())`.
        let boxed: Box<dyn std::error::Error> = Box::new(ConfigError::MissingRequired {
            client: ClientKind::Producer,
            key: "bootstrap.servers",
        });

        assert_eq!(
            boxed.to_string(),
            "required Kafka config key `bootstrap.servers` is missing"
        );
        assert!(boxed.source().is_none());
    }
}
