//! Structured warnings returned by lenient config parsing.

use std::{format, string::String, vec::Vec};

use super::ClientKind;

/// Warning severity emitted by lenient config parsing.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WarningSeverity {
    /// Non-fatal warning.
    Warning,
}

/// One structured config warning.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfigWarning {
    /// Warning severity.
    pub severity: WarningSeverity,
    /// Client family affected by the warning.
    pub client: ClientKind,
    /// Kafka property key.
    pub key: String,
    /// Human-readable warning message.
    pub message: String,
}

/// Structured warnings returned by lenient property parsing.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct WarningReport {
    warnings: Vec<ConfigWarning>,
}

impl WarningReport {
    /// Creates an empty report.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            warnings: Vec::new(),
        }
    }

    /// Adds a warning for an unknown key.
    pub fn push_unknown_key(&mut self, client: ClientKind, key: impl Into<String>) {
        let key = key.into();
        self.warnings.push(ConfigWarning {
            severity: WarningSeverity::Warning,
            client,
            message: format!("unknown Kafka config key `{key}`"),
            key,
        });
    }

    /// Adds a warning for a Java-only key.
    pub fn push_java_only_key(&mut self, client: ClientKind, key: impl Into<String>, reason: &str) {
        let key = key.into();
        self.warnings.push(ConfigWarning {
            severity: WarningSeverity::Warning,
            client,
            message: format!("Java-only Kafka config key `{key}` ignored: {reason}"),
            key,
        });
    }

    /// Adds a warning for a key gated behind a disabled feature.
    pub fn push_unsupported_feature(
        &mut self,
        client: ClientKind,
        key: impl Into<String>,
        feature: &str,
    ) {
        let key = key.into();
        self.warnings.push(ConfigWarning {
            severity: WarningSeverity::Warning,
            client,
            message: format!("Kafka config key `{key}` requires feature `{feature}`"),
            key,
        });
    }

    /// Returns collected warnings.
    #[must_use]
    pub const fn warnings(&self) -> &[ConfigWarning] {
        self.warnings.as_slice()
    }

    /// Returns whether the report has no warnings.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.warnings.is_empty()
    }
}
