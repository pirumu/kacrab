//! Snappy codec (`snap` Rust backend). Level is accepted but ignored.
//!
//! Kafka uses the xerial-snappy framing format (NOT the standard snappy framing
//! format). This consists of a 16-byte magic header followed by a sequence of
//! snappy-compressed blocks, each preceded by a big-endian `u32` length.

use super::{Compression, CompressionError, CompressionErrorKind, Result};

const XERIAL_HEADER: [u8; 16] = [
    0x82, b'S', b'N', b'A', b'P', b'P', b'Y', 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01,
];

const BLOCK_SIZE: usize = 32 * 1024;

/// Compress `payload`. The `level` parameter is accepted for API symmetry but
/// has no effect — Snappy has no tunable level.
pub fn compress_with_level(payload: &[u8], level: Option<i32>) -> Result<Vec<u8>> {
    let _ = level;
    let capacity = XERIAL_HEADER
        .len()
        .checked_add(payload.len())
        .ok_or_else(|| encode_err("snappy input too large".into()))?;
    let mut output = Vec::with_capacity(capacity);
    output.extend_from_slice(&XERIAL_HEADER);

    if payload.is_empty() {
        return Ok(output);
    }

    let mut encoder = snap::raw::Encoder::new();
    for chunk in payload.chunks(BLOCK_SIZE) {
        let compressed = encoder
            .compress_vec(chunk)
            .map_err(|e| encode_err(e.to_string()))?;
        let len = u32::try_from(compressed.len())
            .map_err(|_| encode_err("snappy block exceeds u32".into()))?;
        output.extend_from_slice(&len.to_be_bytes());
        output.extend_from_slice(&compressed);
    }

    Ok(output)
}

/// Decompress `payload`, bounded by [`super::MAX_DECOMPRESSED_LEN`].
pub fn decompress(payload: &[u8]) -> Result<Vec<u8>> {
    decompress_bounded(payload, super::MAX_DECOMPRESSED_LEN)
}

/// Decompress `payload`, refusing to produce more than `max_len` bytes.
///
/// The snappy raw format carries a claimed decompressed length that `snap`
/// allocates up front, and the xerial frame carries arbitrarily many blocks —
/// both must be checked against the bound before decoding.
pub fn decompress_bounded(payload: &[u8], max_len: usize) -> Result<Vec<u8>> {
    if payload.len() < XERIAL_HEADER.len()
        || payload.get(..XERIAL_HEADER.len()) != Some(&XERIAL_HEADER)
    {
        check_claimed_len(payload, max_len, 0)?;
        return snap::raw::Decoder::new()
            .decompress_vec(payload)
            .map_err(|e| decode_err(e.to_string()));
    }

    let mut decoder = snap::raw::Decoder::new();
    let mut output = Vec::new();
    let mut pos = XERIAL_HEADER.len();

    while pos < payload.len() {
        let length_end = pos
            .checked_add(4)
            .ok_or_else(|| decode_err("snappy: block length offset overflow".into()))?;
        let Some(len_bytes) = payload.get(pos..length_end).and_then(|s| s.try_into().ok()) else {
            return Err(decode_err("snappy: truncated block length".into()));
        };
        let block_len = u32::from_be_bytes(len_bytes) as usize;
        pos = length_end;

        let block_end = pos
            .checked_add(block_len)
            .ok_or_else(|| decode_err("snappy: block length offset overflow".into()))?;
        if block_end > payload.len() {
            let remaining = payload.len().saturating_sub(pos);
            return Err(decode_err(format!(
                "snappy: block length {block_len} extends past input (remaining: {remaining})"
            )));
        }

        let Some(block) = payload.get(pos..block_end) else {
            return Err(decode_err("snappy: block out of range".into()));
        };
        check_claimed_len(block, max_len, output.len())?;
        let decompressed = decoder
            .decompress_vec(block)
            .map_err(|e| decode_err(e.to_string()))?;
        output.extend_from_slice(&decompressed);
        pos = block_end;
    }

    Ok(output)
}

/// Reject a snappy raw block whose claimed decompressed length would push the
/// total output past `max_len` — before `snap` allocates the claimed size.
fn check_claimed_len(block: &[u8], max_len: usize, already_produced: usize) -> Result<()> {
    let claimed = snap::raw::decompress_len(block).map_err(|e| decode_err(e.to_string()))?;
    if already_produced.saturating_add(claimed) > max_len {
        return Err(CompressionError::new(
            Compression::Snappy,
            CompressionErrorKind::DecompressedTooLarge { limit: max_len },
        ));
    }
    Ok(())
}

const fn encode_err(message: String) -> CompressionError {
    CompressionError::new(
        Compression::Snappy,
        CompressionErrorKind::EncodeFailed { message },
    )
}

const fn decode_err(message: String) -> CompressionError {
    CompressionError::new(
        Compression::Snappy,
        CompressionErrorKind::DecodeFailed { message },
    )
}

#[cfg(test)]
mod tests {
    use super::{super::CompressionErrorKind, compress_with_level, decompress, decompress_bounded};

    #[test]
    fn decompress_bounded_rejects_a_decompression_bomb() {
        // Multi-block xerial frame whose total claimed output exceeds the bound.
        let payload = vec![0u8; 96 * 1024];
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
    fn decompress_bounded_rejects_a_raw_block_with_a_hostile_claimed_length() {
        // A bare raw block (no xerial header) claiming a huge decompressed
        // length must be rejected before `snap` allocates the claimed size.
        let raw = snap::raw::Encoder::new()
            .compress_vec(&[0u8; 1024])
            .unwrap();

        let err = decompress_bounded(&raw, 64).unwrap_err();
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
        let payload = vec![0u8; 96 * 1024];
        let compressed = compress_with_level(&payload, None).unwrap();

        assert_eq!(decompress_bounded(&compressed, 96 * 1024).unwrap(), payload);
        assert_eq!(decompress(&compressed).unwrap(), payload);
    }
}
