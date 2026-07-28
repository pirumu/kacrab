//! Error types for [`crate::tagged`].

use crate::primitives::PrimitiveError;

/// Error from tagged-field section read/write.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum TaggedFieldError {
    /// Underlying primitive read failed (varint count / tag / size).
    #[error(transparent)]
    Primitive(#[from] PrimitiveError),

    /// Tag numbers were not strictly ascending.
    #[error("tagged fields out of order: tag {tag} after tag {prev_tag}")]
    OutOfOrder {
        /// The tag that violated the order.
        tag: u32,
        /// The previous (greater-or-equal) tag.
        prev_tag: u32,
    },

    /// Declared field size exceeds the remaining buffer.
    #[error("tagged field {tag} size {size} exceeds remaining {remaining}")]
    SizeOverflow {
        /// The tag whose size was bad.
        tag: u32,
        /// Declared size. `usize::MAX` is a sentinel for a wire size varint wider
        /// than this platform's address space, where the real value cannot be
        /// represented in a `usize` at all.
        size: usize,
        /// Bytes actually remaining in the buffer.
        remaining: usize,
    },

    /// A tagged-field count does not fit — this platform's address space when
    /// decoding, or the `u32` varint count prefix when encoding.
    #[error("tagged field count {count} exceeds maximum {max}")]
    CountOverflow {
        /// Offending count: read from the wire when decoding. `u32::MAX` is a
        /// sentinel for the encode side, where the real count is the section's
        /// field count precisely because it does not fit `u32`.
        count: u32,
        /// Maximum the count had to fit into: the largest representable `usize`
        /// when decoding, the largest encodable count (`u32::MAX`) when encoding.
        max: usize,
    },

    /// A tagged-field payload is too large for the Kafka varint size prefix.
    #[error("tagged field {tag} size {size} exceeds maximum {max}")]
    FieldTooLarge {
        /// The tag whose payload is too large.
        tag: u32,
        /// Payload size.
        size: usize,
        /// Maximum encodable payload size.
        max: usize,
    },
}
