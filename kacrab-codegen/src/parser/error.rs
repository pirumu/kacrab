//! Errors from the schema-loading stage.

use std::path::PathBuf;

use crate::ir::{field, version_range};

/// Anything that can go wrong while reading a `*.json` schema file.
#[derive(Debug, thiserror::Error)]
#[error("failed to parse schema at {path}")]
#[non_exhaustive]
pub struct ParseSchemaError {
    /// File the failure refers to.
    pub path: PathBuf,
    /// Underlying cause; preserved in the [`std::error::Error::source`] chain.
    #[source]
    pub kind: ParseSchemaErrorKind,
}

/// Reason a schema file couldn't be parsed.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ParseSchemaErrorKind {
    /// Disk I/O — file missing, permission denied, etc.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// Bytes were read but didn't deserialize as JSON.
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    /// Required JSON key was absent.
    #[error("missing required field: {name}")]
    MissingField {
        /// Name of the missing key (for nested fields, prefixed with the parent).
        name: String,
    },
    /// `type` field held a value other than `request`/`response`/`data`/`header`.
    #[error("invalid message type: {value:?}")]
    InvalidMessageType {
        /// Raw value from the spec.
        value: String,
    },
    /// A field's `type` string didn't match any known Kafka primitive or struct.
    #[error(transparent)]
    InvalidFieldType(#[from] field::ParseError),
    /// A version range string (`"none"`, `"N+"`, `"N-M"`) failed to parse.
    #[error(transparent)]
    InvalidVersionRange(#[from] version_range::ParseError),
    /// An integer literal in the spec didn't fit the target Rust width.
    #[error("integer overflow: {source}")]
    IntegerOverflow {
        /// The underlying conversion error.
        #[from]
        source: std::num::TryFromIntError,
    },
}

impl ParseSchemaError {
    /// Glue a `path` onto a `kind` to build the full error.
    pub fn new(path: impl Into<PathBuf>, kind: impl Into<ParseSchemaErrorKind>) -> Self {
        Self {
            path: path.into(),
            kind: kind.into(),
        }
    }
}
