//! Configuration errors.

extern crate alloc;

use alloc::string::String;
use core::fmt;

use super::ClientKind;

/// Error returned by strict or security-sensitive config validation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConfigError {
    /// Property key is not present in the official catalog for the client.
    UnknownKey {
        /// Client family being validated.
        client: ClientKind,
        /// Unknown Kafka property key.
        key: String,
    },
    /// Required property was not supplied.
    MissingRequired {
        /// Client family being built.
        client: ClientKind,
        /// Missing Kafka property key.
        key: &'static str,
    },
    /// Property key is Java/JVM specific and has no faithful Rust property form.
    JavaOnly {
        /// Client family being validated.
        client: ClientKind,
        /// Java-only Kafka property key.
        key: String,
        /// Explanation of why the key is skipped.
        reason: &'static str,
    },
    /// Property key requires a disabled feature.
    UnsupportedFeature {
        /// Client family being validated.
        client: ClientKind,
        /// Kafka property key.
        key: String,
        /// Required Cargo feature.
        feature: &'static str,
    },
    /// Property key is cataloged but not yet exposed by this typed config.
    UnsupportedKey {
        /// Client family being parsed.
        client: ClientKind,
        /// Kafka property key.
        key: String,
    },
    /// Property value cannot be parsed into the typed Rust config field.
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

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownKey { key, .. } => write!(f, "unknown Kafka config key `{key}`"),
            Self::MissingRequired { key, .. } => {
                write!(f, "required Kafka config key `{key}` is missing")
            },
            Self::JavaOnly { key, reason, .. } => {
                write!(
                    f,
                    "Java-only Kafka config key `{key}` is not supported: {reason}"
                )
            },
            Self::UnsupportedFeature { key, feature, .. } => {
                write!(f, "Kafka config key `{key}` requires feature `{feature}`")
            },
            Self::UnsupportedKey { key, .. } => {
                write!(
                    f,
                    "Kafka config key `{key}` is not modeled by this typed config"
                )
            },
            Self::InvalidValue {
                key, target, value, ..
            } => {
                write!(
                    f,
                    "failed to parse Kafka config key `{key}` value `{value}` as {target}"
                )
            },
        }
    }
}

/// Error returned when parsing a raw config value into a typed Rust value.
#[derive(Clone, Debug, Eq, PartialEq)]
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

impl fmt::Display for ParseConfigValueError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "failed to parse Kafka config value `{}` as {}",
            self.value, self.target
        )
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
}
