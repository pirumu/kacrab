//! Gzip codec (`flate2` Rust backend).

use std::io::{Read, Write};

use super::{Compression, CompressionError, CompressionErrorKind, Result};

const DEFAULT_GZIP_LEVEL: u32 = 6;

/// Compress `payload` at the given level (`None` -> codec default `6`, range `0..=9`).
pub fn compress_with_level(payload: &[u8], level: Option<i32>) -> Result<Vec<u8>> {
    let lvl = level.map_or(DEFAULT_GZIP_LEVEL, |l| l.max(0).cast_unsigned());
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::new(lvl));
    encoder
        .write_all(payload)
        .map_err(|e| encode_err(e.to_string()))?;
    encoder.finish().map_err(|e| encode_err(e.to_string()))
}

/// Decompress `payload`.
pub fn decompress(payload: &[u8]) -> Result<Vec<u8>> {
    let mut decoder = flate2::read::GzDecoder::new(payload);
    let mut output = Vec::new();
    let _read = decoder
        .read_to_end(&mut output)
        .map_err(|e| decode_err(e.to_string()))?;
    Ok(output)
}

const fn encode_err(message: String) -> CompressionError {
    CompressionError::new(
        Compression::Gzip,
        CompressionErrorKind::EncodeFailed { message },
    )
}

const fn decode_err(message: String) -> CompressionError {
    CompressionError::new(
        Compression::Gzip,
        CompressionErrorKind::DecodeFailed { message },
    )
}
