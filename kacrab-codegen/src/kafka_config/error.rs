//! Errors from the optional Kafka config source extractor.

use std::path::PathBuf;

use super::model::KafkaConfigClient;

/// Anything that can go wrong while extracting Kafka config metadata.
#[derive(Debug, thiserror::Error)]
#[error("failed to extract Kafka config metadata")]
#[non_exhaustive]
pub struct KafkaConfigError {
    /// Underlying cause; preserved in the [`std::error::Error::source`] chain.
    #[source]
    pub kind: KafkaConfigErrorKind,
}

/// Reason the Kafka config extractor bailed.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum KafkaConfigErrorKind {
    /// Disk I/O while reading upstream Java sources.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// A required upstream client config class was not found.
    #[error("missing Java source for {client:?} config class {class_name}")]
    MissingClientSource {
        /// Client family being extracted.
        client: KafkaConfigClient,
        /// Java class simple name.
        class_name: &'static str,
    },
    /// A client class invoked a known helper, but that helper's Java source was not found.
    #[error("missing Java source for helper {helper_method} in class {class_name}")]
    MissingHelperSource {
        /// Helper method invoked by a client `ConfigDef` chain.
        helper_method: &'static str,
        /// Java class simple name that owns the helper definitions.
        class_name: &'static str,
    },
    /// A config definition used a Java `ConfigDef.Type` this extractor does not know.
    #[error("unsupported Java config type {raw:?}")]
    UnsupportedType {
        /// Raw Java type token.
        raw: String,
    },
    /// The parser found a malformed Java `define(...)` invocation.
    #[error("malformed ConfigDef.define invocation: {raw}")]
    MalformedDefine {
        /// Raw invocation body.
        raw: String,
    },
    /// The source ref is empty, which would make the generated snapshot unreproducible.
    #[error("source ref must not be empty")]
    EmptySourceRef,
    /// The Java source root did not contain any `.java` files.
    #[error("no Java sources found under {root}")]
    EmptyJavaRoot {
        /// Root path searched recursively.
        root: PathBuf,
    },
}

impl KafkaConfigError {
    /// Build a full extractor error from its kind.
    pub fn new(kind: impl Into<KafkaConfigErrorKind>) -> Self {
        Self { kind: kind.into() }
    }
}
