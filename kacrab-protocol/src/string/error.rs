//! Error types for [`crate::string`].

use crate::primitives::PrimitiveError;

/// Error returned by string read/write operations.
#[derive(Debug, thiserror::Error)]
#[error("kafka string codec failed")]
#[non_exhaustive]
pub struct StringError {
    /// What specifically went wrong.
    #[source]
    pub kind: StringErrorKind,
}

impl StringError {
    /// Construct a `StringError` from its kind.
    #[must_use]
    pub const fn new(kind: StringErrorKind) -> Self {
        Self { kind }
    }
}

impl From<StringErrorKind> for StringError {
    fn from(kind: StringErrorKind) -> Self {
        Self::new(kind)
    }
}

impl From<PrimitiveError> for StringError {
    fn from(err: PrimitiveError) -> Self {
        Self::new(StringErrorKind::Primitive(err))
    }
}

/// Specific reason a string read/write failed.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum StringErrorKind {
    /// Underlying primitive read failed (length prefix or bytes).
    #[error(transparent)]
    Primitive(#[from] PrimitiveError),

    /// String bytes are not valid UTF-8.
    #[error("invalid UTF-8 string: {source}")]
    InvalidUtf8 {
        /// UTF-8 validation failure.
        #[source]
        source: std::str::Utf8Error,
    },

    /// Non-nullable variant got the null marker (negative length / `0` varint).
    #[error("non-nullable string has null marker; use the nullable variant")]
    UnexpectedNull,

    /// Length prefix is negative on a non-nullable encoding.
    #[error("negative length {length} on non-nullable string")]
    NegativeLength {
        /// The negative length read.
        length: i32,
    },

    /// String length exceeds the maximum encodable length.
    #[error("string length {length} exceeds maximum {max}")]
    TooLong {
        /// Offending length: the value being encoded, or `usize::MAX` as a
        /// sentinel when a wire length prefix did not fit in `usize`.
        length: usize,
        /// Protocol-constant maximum (e.g. `i32::MAX`), not a tunable.
        max: usize,
    },
}
