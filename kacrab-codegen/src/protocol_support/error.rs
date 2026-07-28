//! Errors from the protocol support reporter.

/// Anything that can go wrong while building a protocol support report.
#[derive(Debug, thiserror::Error)]
#[error("failed to read the generated client_api_info table")]
#[non_exhaustive]
pub struct ProtocolSupportError {
    /// Underlying cause; preserved in the [`std::error::Error::source`] chain.
    #[source]
    pub kind: ProtocolSupportErrorKind,
}

impl ProtocolSupportError {
    /// Build a full reporter error from its kind.
    pub fn new(kind: impl Into<ProtocolSupportErrorKind>) -> Self {
        Self { kind: kind.into() }
    }
}

/// Reason the protocol support reporter bailed.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ProtocolSupportErrorKind {
    /// The generated file is not parseable Rust.
    #[error(transparent)]
    Syntax(#[from] syn::Error),
    /// The generated file has no `client_api_info` function.
    #[error("generated source has no `client_api_info` function")]
    MissingClientApiInfo,
    /// The `client_api_info` body is not the expected `match` expression.
    #[error("`client_api_info` does not dispatch through a `match` expression")]
    MissingMatchExpression,
    /// A match arm did not use the expected `ApiKey::Variant` pattern.
    #[error("unsupported `client_api_info` match pattern: {pattern}")]
    UnsupportedArmPattern {
        /// Rendered arm pattern.
        pattern: String,
    },
    /// A match arm did not return an `ApiInfo` struct literal.
    #[error("`client_api_info` arm for {api} does not return an `ApiInfo` struct literal")]
    UnsupportedArmBody {
        /// `ApiKey` variant the arm matched.
        api: String,
    },
    /// An `ApiInfo` literal carried a field this reporter does not model.
    #[error("`client_api_info` arm for {api} has unknown `ApiInfo` field {field}")]
    UnknownApiInfoField {
        /// `ApiKey` variant the arm matched.
        api: String,
        /// Field name found in the struct literal.
        field: String,
    },
    /// An `ApiInfo` literal omitted a field this reporter needs.
    #[error("`client_api_info` arm for {api} is missing `ApiInfo` field {field}")]
    MissingApiInfoField {
        /// `ApiKey` variant the arm matched.
        api: String,
        /// Field name that should have been present.
        field: &'static str,
    },
    /// An `ApiInfo` field was neither an `i16` literal nor `i16::MAX`.
    #[error("`client_api_info` arm for {api} has non-`i16` value for field {field}")]
    UnsupportedApiInfoValue {
        /// `ApiKey` variant the arm matched.
        api: String,
        /// Field name that held the unsupported value.
        field: String,
    },
}
