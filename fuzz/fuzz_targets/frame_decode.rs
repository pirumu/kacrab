//! The length-prefixed response frame — the first thing that touches socket
//! bytes.
//!
//! Every response the client ever parses passes through here before any decoder
//! sees it, so a defect at this layer is reachable by any peer. The parser reads
//! a 4-byte big-endian length and splits that many bytes off the buffer, which
//! is exactly the shape that goes wrong through a negative length, an
//! attacker-chosen huge length driving an allocation, or a length that disagrees
//! with what actually arrived.
//!
//! It reads as sound: negative lengths are rejected, `MAX_FRAME_LENGTH` caps the
//! value at 100 MiB, truncation is checked against `remaining()`, and the split
//! is zero-copy over an existing `Bytes` rather than a fresh allocation. This
//! target exists to keep those four properties true rather than because they are
//! suspected false — the layer is too exposed to leave on inspection alone.

#![no_main]

use bytes::Bytes;
use kacrab_protocol::frame::{MAX_FRAME_LENGTH, decode_response_frame, read_frame_length};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let mut buf = Bytes::copy_from_slice(data);
    if let Ok(length) = read_frame_length(&mut buf) {
        assert!(
            length >= 0 && length <= MAX_FRAME_LENGTH,
            "read_frame_length returned {length}, outside 0..={MAX_FRAME_LENGTH}",
        );
    }

    // Drain the buffer frame by frame the way a session loop does. A frame that
    // decodes must consume its own length plus the 4-byte prefix; anything else
    // either desynchronises the stream or spins forever.
    let mut buf = Bytes::copy_from_slice(data);
    let mut guard = 0_u32;
    while let Ok(frame) = decode_response_frame(&mut buf) {
        guard += 1;
        assert!(
            guard < 100_000,
            "decode_response_frame yielded 100k frames from {} bytes — it is not \
             consuming the buffer",
            data.len(),
        );
        let _payload = frame;
    }
});
