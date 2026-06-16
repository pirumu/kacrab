//! Kafka-compatible LZ4 frame codec.
//!
//! ## Wire format
//!
//! Kafka uses the standard LZ4 frame format defined by the LZ4 reference
//! implementation, configured for Kafka traffic:
//!
//! ```text
//! magic (4 bytes LE = 0x184D2204)
//! FLG  (1 byte)  — version=01, block_independence=1, no checksums
//! BD   (1 byte)  — block max size = 64 KiB
//! HC   (1 byte)  — (xxh32([FLG, BD], 0) >> 8) & 0xFF
//! [block]+       — repeated until end-of-stream marker
//! end  (4 bytes LE = 0)
//! ```
//!
//! A block is a 4-byte LE size prefix followed by the bytes:
//!
//! * if the size's MSB (`0x8000_0000`) is set → bytes are uncompressed, payload length = `size &
//!   0x7FFF_FFFF`.
//! * otherwise → bytes are LZ4 block-compressed.
//!
//! ## Backend
//!
//! Selected at compile time:
//!
//! * `feature = "lz4-hc"` → `lz4` crate (FFI to `liblz4`). Levels `1..=2` route through fast mode;
//!   `3..=12` route through HC mode; values above `12` clamp to `12`; negative values clamp to `1`.
//! * `feature = "lz4"` only → `lz4_flex` block API (pure Rust, fast mode only). The `level`
//!   argument is ignored.
//! * Both → `lz4-hc` wins; `lz4_flex` is linked but unused.
//!
//! Pre-0.10 Kafka brokers used a slightly different header layout with a
//! known `XXHash` bug (KIP-57). Modern brokers (0.10+) accept the
//! standard frame format implemented here.

use bytes::{BufMut, BytesMut};

use super::{Compression, CompressionError, CompressionErrorKind, Result};

// --- Frame constants ---------------------------------------------------------

/// LZ4 frame magic number, written little-endian.
const MAGIC: u32 = 0x184D_2204;

/// Length of the frame header: 4 magic + FLG + BD + HC.
const HEADER_LEN: usize = 7;

/// Length of a block size prefix (LE u32).
const SIZE_PREFIX_LEN: usize = 4;

/// Maximum payload bytes per LZ4 block (matches BD = 4 below).
const MAX_BLOCK_SIZE: usize = 64 * 1024;

/// Block-size flag value `4` → 64 KiB (standard LZ4 frame BD encoding).
const BD_BLOCK_SIZE_64KB: u8 = 4;

/// FLG byte: `version=01` (bits 7-6), `block_independence=1` (bit 5),
/// no block/content checksums, no content size, no dictionary ID.
const FLG: u8 = 0b0110_0000;

/// BD byte: 64 KiB block max in bits 6-4, all other bits zero.
const BD: u8 = BD_BLOCK_SIZE_64KB << 4;

/// MSB of the 4-byte block size prefix; set → block is uncompressed.
const INCOMPRESSIBLE_BIT: u32 = 0x8000_0000;

/// Default level (fast mode acceleration factor 1).
const DEFAULT_LEVEL: i32 = 1;

// --- xxh32 primes (RFC: github.com/Cyan4973/xxHash) --------------------------

const XXH32_PRIME_1: u32 = 0x9E37_79B1;
const XXH32_PRIME_2: u32 = 0x85EB_CA77;
const XXH32_PRIME_3: u32 = 0xC2B2_AE3D;
const XXH32_PRIME_4: u32 = 0x27D4_EB2F;
const XXH32_PRIME_5: u32 = 0x1656_67B1;

// --- Public API --------------------------------------------------------------

/// Compress `input` at the codec default level (fast mode).
pub fn compress(input: &[u8]) -> Result<Vec<u8>> {
    compress_with_level(input, None)
}

/// Compress `input` at the given level.
///
/// Level handling depends on the active backend; see the module doc.
pub fn compress_with_level(input: &[u8], level: Option<i32>) -> Result<Vec<u8>> {
    let estimated = HEADER_LEN
        .saturating_add(input.len())
        .saturating_add(input.len() >> 6)
        .saturating_add(SIZE_PREFIX_LEN);
    let mut out = BytesMut::with_capacity(estimated);
    write_frame_header(&mut out);

    let mut offset: usize = 0;
    while offset < input.len() {
        let remaining = input.len().saturating_sub(offset);
        let block_size = remaining.min(MAX_BLOCK_SIZE);
        let end = offset.saturating_add(block_size);
        let block = input
            .get(offset..end)
            .ok_or_else(|| encode_err("block slice out of bounds".to_owned()))?;
        write_block(&mut out, block, level)?;
        offset = end;
    }

    out.put_u32_le(0);
    Ok(out.to_vec())
}

/// Decompress a Kafka-style LZ4 frame.
pub fn decompress(input: &[u8]) -> Result<Vec<u8>> {
    let mut offset = read_frame_header(input)?;
    let mut output: Vec<u8> = Vec::new();

    loop {
        let next = offset.saturating_add(SIZE_PREFIX_LEN);
        let size_bytes = input
            .get(offset..next)
            .ok_or_else(|| decode_err("incomplete block size prefix".to_owned()))?;
        let raw_arr: [u8; 4] = match size_bytes {
            &[a, b, c, d] => [a, b, c, d],
            _ => return Err(decode_err("block size prefix wrong length".to_owned())),
        };
        let raw = u32::from_le_bytes(raw_arr);
        offset = next;
        if raw == 0 {
            break;
        }

        let is_compressed = (raw & INCOMPRESSIBLE_BIT) == 0;
        let block_len_u32 = raw & !INCOMPRESSIBLE_BIT;
        let block_len = usize::try_from(block_len_u32)
            .map_err(|_| decode_err("block length overflows usize".to_owned()))?;
        let block_end = offset
            .checked_add(block_len)
            .ok_or_else(|| decode_err("block end offset overflows".to_owned()))?;
        let block = input.get(offset..block_end).ok_or_else(|| {
            decode_err(format!(
                "incomplete block: expected {block_len}, got {}",
                input.len().saturating_sub(offset)
            ))
        })?;
        offset = block_end;

        if is_compressed {
            let decompressed = decompress_block(block)?;
            output.extend_from_slice(&decompressed);
        } else {
            output.extend_from_slice(block);
        }
    }

    Ok(output)
}

// --- Header helpers ----------------------------------------------------------

fn write_frame_header(out: &mut BytesMut) {
    out.put_u32_le(MAGIC);
    out.put_u8(FLG);
    out.put_u8(BD);
    out.put_u8(header_checksum_byte());
}

fn header_checksum_byte() -> u8 {
    // Standard LZ4 frame: HC byte = byte at position 1 of the
    // little-endian xxh32 of [FLG, BD]. The `& 0xFF` mask makes the
    // truncating cast lossless and clippy can prove it.
    let hash = xxh32(&[FLG, BD], 0);
    ((hash >> 8) & 0xFF) as u8
}

fn read_frame_header(input: &[u8]) -> Result<usize> {
    let header = input
        .get(..HEADER_LEN)
        .ok_or_else(|| decode_err("incomplete frame header".to_owned()))?;
    let magic_arr: [u8; 4] = match header.get(..4) {
        Some(&[a, b, c, d]) => [a, b, c, d],
        _ => return Err(decode_err("frame header missing magic".to_owned())),
    };
    let magic = u32::from_le_bytes(magic_arr);
    if magic != MAGIC {
        return Err(decode_err(format!(
            "invalid LZ4 frame magic: expected {MAGIC:#010x}, got {magic:#010x}"
        )));
    }
    Ok(HEADER_LEN)
}

// --- Block I/O ---------------------------------------------------------------

fn write_block(out: &mut BytesMut, block: &[u8], level: Option<i32>) -> Result<()> {
    let compressed = compress_block(block, level)?;
    if compressed.len() < block.len() {
        let len = u32::try_from(compressed.len())
            .map_err(|_| encode_err("compressed block exceeds u32".to_owned()))?;
        out.put_u32_le(len);
        out.extend_from_slice(&compressed);
    } else {
        let len = u32::try_from(block.len())
            .map_err(|_| encode_err("uncompressed block exceeds u32".to_owned()))?;
        out.put_u32_le(len | INCOMPRESSIBLE_BIT);
        out.extend_from_slice(block);
    }
    Ok(())
}

// --- Backends ----------------------------------------------------------------

#[cfg(feature = "lz4-hc")]
fn compress_block(block: &[u8], level: Option<i32>) -> Result<Vec<u8>> {
    use lz4::block::CompressionMode;

    let raw = level.unwrap_or(DEFAULT_LEVEL);
    let mode = if raw <= 2 {
        CompressionMode::FAST(raw.max(1))
    } else {
        CompressionMode::HIGHCOMPRESSION(raw.min(12))
    };
    lz4::block::compress(block, Some(mode), false)
        .map_err(|e: std::io::Error| encode_err(e.to_string()))
}

#[cfg(all(feature = "lz4", not(feature = "lz4-hc")))]
#[expect(
    clippy::unnecessary_wraps,
    reason = "signature mirrors the fallible C-FFI HC backend"
)]
fn compress_block(block: &[u8], _level: Option<i32>) -> Result<Vec<u8>> {
    // `lz4_flex::block::compress` is infallible: LZ4 fast mode cannot
    // fail on valid input, so the crate returns `Vec<u8>` directly.
    let _ = DEFAULT_LEVEL;
    Ok(lz4_flex::block::compress(block))
}

#[cfg(feature = "lz4-hc")]
fn decompress_block(block: &[u8]) -> Result<Vec<u8>> {
    let max_size = i32::try_from(MAX_BLOCK_SIZE)
        .map_err(|_| decode_err("MAX_BLOCK_SIZE overflows i32".to_owned()))?;
    lz4::block::decompress(block, Some(max_size))
        .map_err(|e: std::io::Error| decode_err(e.to_string()))
}

#[cfg(all(feature = "lz4", not(feature = "lz4-hc")))]
fn decompress_block(block: &[u8]) -> Result<Vec<u8>> {
    lz4_flex::block::decompress(block, MAX_BLOCK_SIZE).map_err(|e| decode_err(e.to_string()))
}

// --- xxh32 (short-input, sufficient for the 2-byte header descriptor) -------

/// `XXHash32` — algorithmically complete for inputs shorter than 16 bytes.
///
/// The full xxh32 spec has a 4-lane fast path for inputs of 16 bytes or
/// more. We omit it: the only caller in this module hashes the 2-byte
/// `[FLG, BD]` descriptor, which always falls into the tail loop below.
fn xxh32(input: &[u8], seed: u32) -> u32 {
    let len_u32 = u32::try_from(input.len()).unwrap_or(u32::MAX);
    let mut hash = seed.wrapping_add(XXH32_PRIME_5).wrapping_add(len_u32);

    let mut chunks = input.chunks_exact(4);
    for chunk in chunks.by_ref() {
        // `chunks_exact(4)` yields slices of length 4 by contract; the
        // catch-all arm exists only to satisfy exhaustiveness without
        // panicking, and is dead code.
        let word = match chunk {
            &[b0, b1, b2, b3] => u32::from_le_bytes([b0, b1, b2, b3]),
            _ => return hash,
        };
        hash = hash
            .wrapping_add(word.wrapping_mul(XXH32_PRIME_3))
            .rotate_left(17)
            .wrapping_mul(XXH32_PRIME_4);
    }

    for &byte in chunks.remainder() {
        hash = hash
            .wrapping_add(u32::from(byte).wrapping_mul(XXH32_PRIME_5))
            .rotate_left(11)
            .wrapping_mul(XXH32_PRIME_1);
    }

    hash ^= hash >> 15;
    hash = hash.wrapping_mul(XXH32_PRIME_2);
    hash ^= hash >> 13;
    hash = hash.wrapping_mul(XXH32_PRIME_3);
    hash ^= hash >> 16;
    hash
}

// --- Error constructors ------------------------------------------------------

const fn encode_err(message: String) -> CompressionError {
    CompressionError::new(
        Compression::Lz4,
        CompressionErrorKind::EncodeFailed { message },
    )
}

const fn decode_err(message: String) -> CompressionError {
    CompressionError::new(
        Compression::Lz4,
        CompressionErrorKind::DecodeFailed { message },
    )
}

#[cfg(test)]
mod tests {
    use super::{BD, FLG, MAGIC, XXH32_PRIME_5, compress, decompress, xxh32};

    #[test]
    fn frame_starts_with_kafka_lz4_magic() {
        let compressed = compress(b"Hello, Kafka!").unwrap();
        assert!(compressed.len() >= 7, "compressed output too short");
        let magic =
            u32::from_le_bytes([compressed[0], compressed[1], compressed[2], compressed[3]]);
        assert_eq!(magic, MAGIC, "unexpected LZ4 frame magic");
        assert_eq!(compressed[4], FLG);
        assert_eq!(compressed[5], BD);
    }

    #[test]
    fn roundtrip_short_payload() {
        let payload = b"Hello, Kafka! This is a test message.";
        let compressed = compress(payload).unwrap();
        let decompressed = decompress(&compressed).unwrap();
        assert_eq!(payload.as_slice(), decompressed.as_slice());
    }

    #[test]
    fn roundtrip_empty() {
        let compressed = compress(b"").unwrap();
        let decompressed = decompress(&compressed).unwrap();
        assert!(decompressed.is_empty());
    }

    #[test]
    fn roundtrip_multi_block() {
        // Larger than MAX_BLOCK_SIZE (64 KiB) → forces multi-block path.
        let payload = vec![b'x'; 256 * 1024];
        let compressed = compress(&payload).unwrap();
        let decompressed = decompress(&compressed).unwrap();
        assert_eq!(payload.as_slice(), decompressed.as_slice());
        // Highly repetitive data should compress to a tiny fraction.
        assert!(compressed.len() < payload.len() / 10);
    }

    #[test]
    fn xxh32_empty_matches_reference() {
        // Reference vector from the xxh32 spec: hash of "" with seed 0.
        let h = xxh32(b"", 0);
        assert_eq!(h, 0x02CC_5D05);
    }

    #[test]
    fn xxh32_short_input_matches_reference() {
        // Reference vector: xxh32("a", 0) = 0x550D7456.
        let h = xxh32(b"a", 0);
        assert_eq!(h, 0x550D_7456);
    }

    #[test]
    fn xxh32_uses_prime5_in_tail() {
        // Sanity check that the tail loop is wired up: compare a single-byte
        // hash against the open-coded reference computation.
        let byte: u8 = 0xAB;
        let mut expected = 0u32.wrapping_add(XXH32_PRIME_5).wrapping_add(1);
        expected = expected
            .wrapping_add(u32::from(byte).wrapping_mul(XXH32_PRIME_5))
            .rotate_left(11)
            .wrapping_mul(super::XXH32_PRIME_1);
        expected ^= expected >> 15;
        expected = expected.wrapping_mul(super::XXH32_PRIME_2);
        expected ^= expected >> 13;
        expected = expected.wrapping_mul(super::XXH32_PRIME_3);
        expected ^= expected >> 16;
        assert_eq!(xxh32(&[byte], 0), expected);
    }
}
