//! Error types for [`crate::record`].
//!
//! Demonstrates the canonical `struct + Kind` shape from the project research:
//! `RecordError` carries shared context (`base_offset`) that's useful for any
//! variant; `RecordErrorKind` enumerates the specific failure modes.

use crate::{compression::CompressionError, crc::CrcMismatch, primitives::PrimitiveError};

/// Error from record-batch / record / header read or write.
#[derive(Debug, thiserror::Error)]
#[error("record batch failed at base_offset {base_offset}")]
#[non_exhaustive]
pub struct RecordError {
    /// `base_offset` of the batch being processed, or `-1` if unknown.
    pub base_offset: i64,
    /// What specifically went wrong.
    #[source]
    pub kind: RecordErrorKind,
}

impl RecordError {
    /// Construct a `RecordError` with a known `base_offset`.
    #[must_use]
    pub const fn at_offset(base_offset: i64, kind: RecordErrorKind) -> Self {
        Self { base_offset, kind }
    }

    /// Construct a `RecordError` when the offset is not yet known.
    #[must_use]
    pub const fn unknown_offset(kind: RecordErrorKind) -> Self {
        Self {
            base_offset: -1,
            kind,
        }
    }
}

impl From<RecordErrorKind> for RecordError {
    fn from(kind: RecordErrorKind) -> Self {
        Self::unknown_offset(kind)
    }
}

impl From<PrimitiveError> for RecordError {
    fn from(err: PrimitiveError) -> Self {
        Self::unknown_offset(RecordErrorKind::Primitive(err))
    }
}

impl From<CrcMismatch> for RecordError {
    fn from(err: CrcMismatch) -> Self {
        Self::unknown_offset(RecordErrorKind::Crc(err))
    }
}

impl From<CompressionError> for RecordError {
    fn from(err: CompressionError) -> Self {
        Self::unknown_offset(RecordErrorKind::Compression(err))
    }
}

/// Specific reason a record-batch operation failed.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum RecordErrorKind {
    /// Underlying primitive read failed.
    #[error(transparent)]
    Primitive(#[from] PrimitiveError),

    /// CRC32C verification failed.
    #[error(transparent)]
    Crc(#[from] CrcMismatch),

    /// Compression codec dispatch / decode failed.
    #[error(transparent)]
    Compression(#[from] CompressionError),

    /// Magic byte is not 2 (v2 record batches only).
    #[error("unsupported magic byte {0}, expected 2")]
    UnsupportedMagic(i8),

    /// `batchLength` is below the minimum (`BATCH_HEADER_SIZE = 49`).
    #[error("batch length {got} below minimum {min}")]
    BatchTooSmall {
        /// Length declared on the wire.
        got: i32,
        /// Minimum legal length.
        min: i32,
    },

    /// `recordCount` exceeds [`crate::record::MAX_RECORDS_PER_BATCH`].
    #[error("record count {got} exceeds maximum {max}")]
    RecordCountTooLarge {
        /// Count declared on the wire.
        got: i32,
        /// Configured maximum.
        max: usize,
    },

    /// A varint length field was negative where it must be `>= 0`.
    #[error("negative {field} length {length}")]
    NegativeLength {
        /// Which field had the bad length (e.g. `"header key"`, `"record body"`).
        field: &'static str,
        /// The negative length read.
        length: i32,
    },

    /// A length field exceeds the remaining buffer.
    #[error("{field} length {got} exceeds remaining {remaining}")]
    LengthOverflow {
        /// Which field overflowed.
        field: &'static str,
        /// Length declared on the wire.
        got: usize,
        /// Bytes actually remaining in the buffer.
        remaining: usize,
    },
}
