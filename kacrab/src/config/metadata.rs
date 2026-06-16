//! Static metadata for official Kafka configuration keys.

/// Kafka client family that owns a configuration key.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ClientKind {
    /// Producer client configuration.
    Producer,
    /// Consumer client configuration.
    Consumer,
    /// Admin client configuration.
    Admin,
}

/// Source family for a configuration key.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ConfigOrigin {
    /// Extracted from Apache Kafka Java `ConfigDef`.
    Kafka,
    /// Kacrab runtime-specific configuration overlay.
    KacrabRuntime,
}

/// Implementation status for an official Kafka configuration key.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConfigStatus {
    /// Model this key as a native typed Rust field.
    Native,
    /// Include this official key, but review exact Rust type/default before
    /// exposing it as stable API.
    NativeReview,
    /// Model this key only when the named feature is enabled.
    FeatureGated {
        /// Cargo feature that enables the key.
        feature: &'static str,
    },
    /// Keep the key in the catalog for a future feature.
    Future {
        /// Planned feature that would enable the key.
        feature: &'static str,
    },
    /// Do not accept this Java/JVM-specific key as a Rust property.
    SkipJavaOnly,
}

/// Static metadata for one official Kafka configuration key.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConfigEntry {
    /// Client family this key belongs to.
    pub client: ClientKind,
    /// Source family this key belongs to.
    pub origin: ConfigOrigin,
    /// Kafka Java property key.
    pub key: &'static str,
    /// Candidate Rust field name.
    pub rust_field: &'static str,
    /// Kafka docs type string.
    pub kafka_type: &'static str,
    /// Kafka docs default value.
    pub default: &'static str,
    /// Rust implementation status.
    pub status: ConfigStatus,
    /// Short Rust decision comment.
    pub comment: &'static str,
    /// Official Apache Kafka documentation text for this key.
    pub documentation: &'static str,
    /// Official Apache Kafka documentation URL for this key.
    pub source: &'static str,
    /// Platform names where this key applies; empty means all supported targets.
    pub platforms: &'static [&'static str],
    /// Optional Cargo/runtime feature associated with this key.
    pub feature: Option<&'static str>,
}
