//! Error types for [`crate::primitives`].
//!
//! Two failure modes: insufficient buffer or malformed varint. Both are
//! deterministic from the byte stream so a position-counting context is not
//! tracked here — callers higher up (record, frame) wrap with their own
//! context if needed.

/// Error returned by primitive read operations.
#[derive(Debug, thiserror::Error)]
#[error("primitive decode failed")]
#[non_exhaustive]
pub struct PrimitiveError {
    /// What specifically went wrong.
    #[source]
    pub kind: PrimitiveErrorKind,
}

impl PrimitiveError {
    /// Construct a `PrimitiveError` from its kind.
    #[must_use]
    pub const fn new(kind: PrimitiveErrorKind) -> Self {
        Self { kind }
    }
}

impl From<PrimitiveErrorKind> for PrimitiveError {
    fn from(kind: PrimitiveErrorKind) -> Self {
        Self::new(kind)
    }
}

/// Specific reason a primitive read failed.
#[derive(Debug, thiserror::Error)]
#[expect(
    variant_size_differences,
    reason = "InsufficientData carries usize×2 context; InvalidVarint a single u8. Boxing the \
              larger variant would slow the hot path for negligible memory savings on a \
              short-lived error."
)]
#[non_exhaustive]
pub enum PrimitiveErrorKind {
    /// Buffer ran out before the requested width was satisfied.
    #[error("insufficient data: needed {needed} bytes, only {available} available")]
    InsufficientData {
        /// Bytes the read needed.
        needed: usize,
        /// Bytes actually remaining in the buffer.
        available: usize,
    },

    /// Varint exceeded the maximum allowed byte length (5 for u32, 10 for u64).
    #[error("invalid varint: continuation bit set past byte {max_bytes}")]
    InvalidVarint {
        /// Maximum bytes the varint encoding allows.
        max_bytes: u8,
    },

    /// A compact length prefix decoded to a value that overflows `i32`.
    #[error("compact length {length} exceeds the i32 maximum")]
    LengthOutOfRange {
        /// The decoded length that could not fit in an `i32`.
        length: u32,
    },
}
