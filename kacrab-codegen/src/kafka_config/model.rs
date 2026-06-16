//! Serializable intermediate model for Kafka client configuration metadata.

use serde::{Deserialize, Serialize};

/// Kafka client family that owns a public config surface.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum KafkaConfigClient {
    /// Producer client configuration.
    Producer,
    /// Consumer client configuration.
    Consumer,
    /// Admin client configuration.
    Admin,
}

impl KafkaConfigClient {
    pub(crate) const fn java_class(self) -> &'static str {
        match self {
            Self::Producer => "ProducerConfig",
            Self::Consumer => "ConsumerConfig",
            Self::Admin => "AdminClientConfig",
        }
    }
}

/// Source family for one config declaration.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ConfigOrigin {
    /// Extracted from Apache Kafka Java `ConfigDef`.
    #[default]
    Kafka,
    /// Kacrab runtime-specific config overlay.
    KacrabRuntime,
}

impl ConfigOrigin {
    #[expect(
        clippy::trivially_copy_pass_by_ref,
        reason = "serde skip_serializing_if requires a function that accepts a reference"
    )]
    pub(crate) const fn is_kafka(&self) -> bool {
        matches!(self, Self::Kafka)
    }
}

/// Machine-readable snapshot generated from a pinned upstream Kafka source ref.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ConfigCatalogDocument {
    /// Pinned upstream source identifier, for example `apache/kafka@4.3.0`.
    pub source_ref: String,
    /// Per-client config declarations extracted from Java `ConfigDef` builders.
    pub clients: Vec<ConfigClientDocument>,
}

/// Config declarations extracted for one Kafka client class.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ConfigClientDocument {
    /// Client family represented by this Java class.
    pub client: KafkaConfigClient,
    /// Java class simple name, for example `ProducerConfig`.
    pub java_class: String,
    /// Config keys declared by the class's `ConfigDef`.
    pub configs: Vec<ConfigKeyDocument>,
}

/// One Kafka configuration key declared in an upstream Java `ConfigDef`.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ConfigKeyDocument {
    /// Source family for this declaration.
    #[serde(default, skip_serializing_if = "ConfigOrigin::is_kafka")]
    pub origin: ConfigOrigin,
    /// Public Kafka property key, for example `bootstrap.servers`.
    pub key: String,
    /// Java constant passed as the first `define(...)` argument.
    pub java_constant: String,
    /// Candidate Rust field name, when supplied by a non-Java overlay.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rust_field: Option<String>,
    /// Java `ConfigDef.Type` declared by Kafka.
    pub java_type: JavaConfigType,
    /// Java default value when the overload declares one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default: Option<ConfigValueDefault>,
    /// Java `ConfigDef.Importance` declared by Kafka.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub importance: Option<String>,
    /// Upstream documentation text when it can be resolved from Java constants.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub documentation: Option<String>,
    /// Platform names where this option is available.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub platforms: Vec<String>,
    /// Optional Cargo/runtime feature associated with this option.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub feature: Option<String>,
}

/// Kacrab runtime overlay merged into the upstream Kafka config catalog.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RuntimeConfigOverlayDocument {
    /// Pinned overlay source identifier.
    pub source_ref: String,
    /// Runtime config declarations to expand into selected client families.
    pub configs: Vec<RuntimeConfigKeyDocument>,
}

/// One Kacrab runtime config declaration before expansion into client docs.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RuntimeConfigKeyDocument {
    /// Public Kacrab runtime property key.
    pub key: String,
    /// Rust field emitted into typed client configs.
    pub rust_field: String,
    /// Config value type.
    #[serde(rename = "type")]
    pub java_type: JavaConfigType,
    /// Default value shown in generated metadata.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default: Option<ConfigValueDefault>,
    /// Client families that should expose this runtime key.
    pub clients: Vec<KafkaConfigClient>,
    /// Platform names where this option is available.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub platforms: Vec<String>,
    /// Optional Cargo/runtime feature associated with this option.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub feature: Option<String>,
    /// Generated documentation text.
    pub documentation: String,
}

/// Kafka's Java `ConfigDef.Type`.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum JavaConfigType {
    /// Boolean config.
    Boolean,
    /// Short integer config.
    Short,
    /// Integer config.
    Int,
    /// Long integer config.
    Long,
    /// Double precision floating point config.
    Double,
    /// String config.
    String,
    /// List config.
    List,
    /// Class-name config.
    Class,
    /// Password config.
    Password,
}

/// Parsed default value from a Java `ConfigDef.define(...)` call.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum ConfigValueDefault {
    /// Java `null`.
    Null,
    /// Boolean literal.
    Boolean(bool),
    /// Integer literal.
    Integer(i64),
    /// Java string literal.
    String(String),
    /// Java symbol/expression retained for maintainer review.
    Symbol(String),
}
