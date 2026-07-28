//! Error types for [`crate::frame`].

use crate::primitives::PrimitiveError;

/// Error from frame read/write.
#[derive(Debug, thiserror::Error)]
#[error("kafka frame codec failed")]
#[non_exhaustive]
pub struct FrameError {
    /// What specifically went wrong.
    #[source]
    pub kind: FrameErrorKind,
}

impl FrameError {
    /// Construct a `FrameError` from its kind.
    #[must_use]
    pub const fn new(kind: FrameErrorKind) -> Self {
        Self { kind }
    }
}

impl From<FrameErrorKind> for FrameError {
    fn from(kind: FrameErrorKind) -> Self {
        Self::new(kind)
    }
}

impl From<PrimitiveError> for FrameError {
    fn from(err: PrimitiveError) -> Self {
        Self::new(FrameErrorKind::Primitive(err))
    }
}

/// Specific reason a frame operation failed.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum FrameErrorKind {
    /// Underlying primitive read failed (length prefix).
    #[error(transparent)]
    Primitive(#[from] PrimitiveError),

    /// Length prefix is negative.
    #[error("negative frame length: {length}")]
    NegativeLength {
        /// The negative length read.
        length: i32,
    },

    /// Frame payload exceeds [`crate::frame::MAX_FRAME_LENGTH`] — a length prefix
    /// read from the wire, or a `header + body` too large to encode.
    #[error("frame length {length} exceeds maximum {max}")]
    TooLarge {
        /// Offending length: read from the wire when decoding, the `header + body`
        /// size when encoding. `i32::MAX` is a sentinel for an encode-side length
        /// that overflowed `usize` or does not fit `i32`, where the real value
        /// cannot be represented in this field at all.
        length: i32,
        /// Configured maximum.
        max: i32,
    },

    /// Buffer ran out before the declared payload was consumed.
    #[error("frame truncated: needed {needed} bytes, only {available} available")]
    Truncated {
        /// Bytes the frame declared.
        needed: usize,
        /// Bytes actually remaining.
        available: usize,
    },
}
