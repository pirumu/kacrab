//! Decompression over arbitrary bytes, one codec per input.
//!
//! A compressed blob in a record batch declares its own decompressed size, so a
//! malicious or corrupt broker response can claim an enormous expansion (a zip
//! bomb) or a size that disagrees with the payload. `kacrab_protocol` bounds
//! every codec by `MAX_DECOMPRESSED_LEN` and reports
//! `CompressionErrorKind::DecompressedTooLarge`; this target exists to keep that
//! bound honest against inputs no test writes by hand, and to prove the codec
//! wrappers do not panic on truncated or structurally invalid frames.

#![no_main]

use kacrab_protocol::compression::{Compression, MAX_DECOMPRESSED_LEN};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let Some((selector, payload)) = data.split_first() else {
        return;
    };
    // Four codecs, not five: `Compression::None::decompress` is `payload.to_vec()`
    // (compression.rs:183), so including it spent one execution in five on a
    // memcpy that produces no coverage.
    let codec = match selector % 4 {
        0 => Compression::Gzip,
        1 => Compression::Snappy,
        2 => Compression::Lz4,
        _ => Compression::Zstd,
    };

    if let Ok(decompressed) = codec.decompress(payload) {
        assert!(
            decompressed.len() <= MAX_DECOMPRESSED_LEN,
            "{codec:?} produced {} bytes, past the {MAX_DECOMPRESSED_LEN}-byte bound",
            decompressed.len(),
        );
    }
});
