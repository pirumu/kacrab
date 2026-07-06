//! Gzip codec (`flate2` Rust backend).

use std::io::Write;

use super::{Compression, CompressionError, CompressionErrorKind, Result};

const DEFAULT_GZIP_LEVEL: u32 = 6;

/// Compress `payload` at the given level (`None` -> codec default `6`). Only the lower bound is
/// clamped (negative levels become `0`); higher values pass through to `flate2` unchanged.
pub fn compress_with_level(payload: &[u8], level: Option<i32>) -> Result<Vec<u8>> {
    let lvl = level.map_or(DEFAULT_GZIP_LEVEL, |l| l.max(0).cast_unsigned());
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::new(lvl));
    encoder
        .write_all(payload)
        .map_err(|e| encode_err(e.to_string()))?;
    encoder.finish().map_err(|e| encode_err(e.to_string()))
}

/// Decompress `payload`, bounded by [`super::MAX_DECOMPRESSED_LEN`].
pub fn decompress(payload: &[u8]) -> Result<Vec<u8>> {
    decompress_bounded(payload, super::MAX_DECOMPRESSED_LEN)
}

/// Decompress `payload`, refusing to produce more than `max_len` bytes —
/// gzip expands up to ~1000:1, so an unbounded read is a decompression bomb.
pub fn decompress_bounded(payload: &[u8], max_len: usize) -> Result<Vec<u8>> {
    let decoder = flate2::read::GzDecoder::new(payload);
    super::read_to_end_bounded(decoder, max_len, Compression::Gzip)
}

const fn encode_err(message: String) -> CompressionError {
    CompressionError::new(
        Compression::Gzip,
        CompressionErrorKind::EncodeFailed { message },
    )
}

#[cfg(test)]
mod tests {
    use super::{super::CompressionErrorKind, compress_with_level, decompress, decompress_bounded};

    #[test]
    fn decompress_bounded_rejects_a_decompression_bomb() {
        // Highly repetitive data compresses to a tiny payload that would
        // expand far past the bound.
        let payload = vec![0u8; 4096];
        let compressed = compress_with_level(&payload, None).unwrap();

        let err = decompress_bounded(&compressed, 64).unwrap_err();
        assert!(
            matches!(
                err.kind,
                CompressionErrorKind::DecompressedTooLarge { limit: 64 }
            ),
            "expected DecompressedTooLarge, got {:?}",
            err.kind
        );
    }

    #[test]
    fn decompress_bounded_allows_output_at_exactly_the_limit() {
        let payload = vec![0u8; 4096];
        let compressed = compress_with_level(&payload, None).unwrap();

        assert_eq!(decompress_bounded(&compressed, 4096).unwrap(), payload);
        assert_eq!(decompress(&compressed).unwrap(), payload);
    }
}
