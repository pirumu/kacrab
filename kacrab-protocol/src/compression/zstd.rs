//! Zstandard codec (`zstd` crate).
//!
//! The module path `crate::compression::zstd` shadows the external `zstd`
//! crate inside this file — use a fully-qualified `::zstd::...` import to
//! reach the codec when implementing the bodies below.

use super::{Compression, CompressionError, CompressionErrorKind, Result};

const DEFAULT_ZSTD_LEVEL: i32 = 3;

/// Compress `payload` at the given level (`None` -> codec default `3`, range `1..=22`).
pub fn compress_with_level(payload: &[u8], level: Option<i32>) -> Result<Vec<u8>> {
    let lvl = level.unwrap_or(DEFAULT_ZSTD_LEVEL);
    ::zstd::bulk::compress(payload, lvl).map_err(|e| encode_err(e.to_string()))
}

/// Decompress `payload`.
pub fn decompress(payload: &[u8]) -> Result<Vec<u8>> {
    ::zstd::stream::decode_all(payload).map_err(|e| decode_err(e.to_string()))
}

const fn encode_err(message: String) -> CompressionError {
    CompressionError::new(
        Compression::Zstd,
        CompressionErrorKind::EncodeFailed { message },
    )
}

const fn decode_err(message: String) -> CompressionError {
    CompressionError::new(
        Compression::Zstd,
        CompressionErrorKind::DecodeFailed { message },
    )
}
