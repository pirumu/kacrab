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

    // Drain the buffer frame by frame the way a session loop does.
    //
    // The invariant is that every successful decode strictly shrinks the buffer,
    // which is what makes a session loop terminate. Asserting a *frame count*
    // instead would be wrong: a zero-length frame is legal and consumes exactly
    // its 4-byte prefix, so 400_004 bytes of zeroes legitimately yields 100_001
    // frames. An earlier version of this target capped the count at 100_000 and
    // fired on precisely that input — a manufactured finding against a decoder
    // that was behaving correctly.
    let mut buf = Bytes::copy_from_slice(data);
    loop {
        let before = buf.len();
        let Ok(frame) = decode_response_frame(&mut buf) else {
            break;
        };
        assert!(
            buf.len() < before,
            "decode_response_frame returned a {}-byte frame while leaving the buffer \
             at {before} bytes — a session loop over this would spin forever",
            frame.len(),
        );
    }
});
