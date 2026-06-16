//! Error types for [`crate::version`].
//!
//! Single failure mode (the negotiated range was empty / version unsupported)
//! so this is a struct, not an enum. Context fields (`api_key`, `version`) are
//! always populated.

/// API version negotiation produced no mutually-supported version, or a
/// generated decoder rejected an unknown version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("unsupported version: api_key={api_key}, version={version}")]
#[non_exhaustive]
pub struct UnsupportedVersion {
    /// Kafka API key (numeric form).
    pub api_key: i16,
    /// The version that was rejected.
    pub version: i16,
}

impl UnsupportedVersion {
    /// Construct a new `UnsupportedVersion`.
    #[must_use]
    pub const fn new(api_key: i16, version: i16) -> Self {
        Self { api_key, version }
    }
}

/// A field was set while encoding a protocol version where that field does not
/// exist.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("unsupported field version: api_key={api_key}, field={field}, version={version}")]
#[non_exhaustive]
pub struct UnsupportedFieldVersion {
    /// Kafka API key (numeric form).
    pub api_key: i16,
    /// Generated Rust field name.
    pub field: &'static str,
    /// The version that does not carry this field.
    pub version: i16,
}

impl UnsupportedFieldVersion {
    /// Construct a new `UnsupportedFieldVersion`.
    #[must_use]
    pub const fn new(api_key: i16, field: &'static str, version: i16) -> Self {
        Self {
            api_key,
            field,
            version,
        }
    }
}
