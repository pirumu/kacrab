//! Error types for [`crate::crc`].
//!
//! A single failure mode (`expected != actual`) so this is a plain struct,
//! not the `struct + Kind` shape.

/// CRC32C checksum mismatch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("CRC mismatch: expected {expected:#010x}, actual {actual:#010x}")]
#[non_exhaustive]
pub struct CrcMismatch {
    /// CRC value read from the wire.
    pub expected: u32,
    /// CRC value computed locally.
    pub actual: u32,
}
