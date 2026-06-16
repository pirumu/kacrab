//! CRC32C checksum compute + validation.
//!
//! Kafka record batches use CRC32C (Castagnoli polynomial), not CRC32. The
//! checksum covers the bytes from `attributes` through the end of the batch.

pub mod error;

pub use self::error::CrcMismatch;

/// Compute the CRC32C checksum of `bytes`.
#[must_use]
pub fn crc32c(bytes: &[u8]) -> u32 {
    ::crc32c::crc32c(bytes)
}

/// Validate that `crc32c(bytes) == expected`. Returns [`CrcMismatch`] otherwise.
pub fn validate_crc32c(bytes: &[u8], expected: u32) -> Result<(), CrcMismatch> {
    let actual = crc32c(bytes);
    if actual == expected {
        Ok(())
    } else {
        Err(CrcMismatch { expected, actual })
    }
}
