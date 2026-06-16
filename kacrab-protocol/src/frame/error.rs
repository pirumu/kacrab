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

    /// Length prefix exceeds [`crate::frame::MAX_FRAME_LENGTH`].
    #[error("frame length {length} exceeds maximum {max}")]
    TooLarge {
        /// Length read from the wire.
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
