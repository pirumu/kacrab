//! Error types for [`crate::bytes_io`].

use crate::primitives::PrimitiveError;

/// Error returned by raw bytes read/write operations.
#[derive(Debug, thiserror::Error)]
#[error("Kafka bytes codec failed")]
#[non_exhaustive]
pub struct BytesError {
    /// What specifically went wrong.
    #[source]
    pub kind: BytesErrorKind,
}

impl BytesError {
    /// Construct a `BytesError` from its kind.
    #[must_use]
    pub const fn new(kind: BytesErrorKind) -> Self {
        Self { kind }
    }
}

impl From<BytesErrorKind> for BytesError {
    fn from(kind: BytesErrorKind) -> Self {
        Self::new(kind)
    }
}

impl From<PrimitiveError> for BytesError {
    fn from(err: PrimitiveError) -> Self {
        Self::new(BytesErrorKind::Primitive(err))
    }
}

/// Specific reason a bytes read/write failed.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum BytesErrorKind {
    /// Underlying primitive read failed (length prefix).
    #[error(transparent)]
    Primitive(#[from] PrimitiveError),

    /// Non-nullable variant got the null marker.
    #[error("non-nullable bytes has null marker; use the nullable variant")]
    UnexpectedNull,

    /// Length prefix is negative on a non-nullable encoding.
    #[error("negative length {length} on non-nullable bytes")]
    NegativeLength {
        /// The negative length read.
        length: i32,
    },

    /// Bytes payload exceeds the configured maximum length.
    #[error("bytes length {length} exceeds maximum {max}")]
    TooLong {
        /// Length read from the wire.
        length: usize,
        /// Configured maximum.
        max: usize,
    },
}
