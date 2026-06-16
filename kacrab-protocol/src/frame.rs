//! TCP wire framing.
//!
//! Every Kafka TCP message is wrapped in a 4-byte big-endian `i32` length
//! prefix. This module provides the encode/decode helpers for that envelope
//! plus a [`MAX_FRAME_LENGTH`] cap to prevent OOM on hostile or corrupt input.

mod codec;
pub mod error;

use bytes::{Buf, BufMut, Bytes, BytesMut};

pub use self::{
    codec::{
        RequestFrameSpec, ResponseEnvelope, decode_response_envelope, encode_request_frame,
        encode_request_frame_body, encode_request_frame_body_with_buffer,
        encode_request_frame_with_buffer,
    },
    error::{FrameError, FrameErrorKind},
};
use crate::primitives::check_remaining;

/// Result alias for frame operations.
pub type Result<T> = core::result::Result<T, FrameError>;

/// Maximum accepted frame payload length (header + body, excluding the
/// 4-byte length prefix itself). Defaults to 100 MiB, matching the Kafka
/// broker's `socket.request.max.bytes` default.
pub const MAX_FRAME_LENGTH: i32 = 100 * 1024 * 1024;

/// Read the 4-byte big-endian frame length prefix.
///
/// Consumes exactly 4 bytes. Rejects negative lengths and lengths above
/// [`MAX_FRAME_LENGTH`].
pub fn read_frame_length(buf: &mut Bytes) -> Result<i32> {
    check_remaining(buf, 4)?;
    let len = buf.get_i32();
    if len < 0 {
        return Err(FrameErrorKind::NegativeLength { length: len }.into());
    }
    if len > MAX_FRAME_LENGTH {
        return Err(FrameErrorKind::TooLarge {
            length: len,
            max: MAX_FRAME_LENGTH,
        }
        .into());
    }
    Ok(len)
}

/// Decode one length-prefixed response frame.
///
/// Reads the length via [`read_frame_length`] then splits exactly that many
/// bytes off `buf` as the payload (header + body).
pub fn decode_response_frame(buf: &mut Bytes) -> Result<Bytes> {
    let frame_len = read_frame_length(buf)?;
    let len = usize::try_from(frame_len)
        .map_err(|_| FrameErrorKind::NegativeLength { length: frame_len })?;
    let available = buf.remaining();
    if available < len {
        return Err(FrameErrorKind::Truncated {
            needed: len,
            available,
        }
        .into());
    }
    Ok(buf.split_to(len))
}

/// Encode a request as `[i32-BE length][header bytes][body bytes]`.
///
/// The length covers `header + body` only — it does NOT include the 4-byte
/// prefix itself (matching the Kafka wire format).
///
/// Header serialization is the caller's responsibility; this helper takes
/// the already-encoded header bytes so it does not depend on
/// `crate::generated::request_header`.
pub fn encode_request(header: &[u8], body: &[u8]) -> Result<BytesMut> {
    let payload_len = header
        .len()
        .checked_add(body.len())
        .ok_or(FrameErrorKind::TooLarge {
            length: i32::MAX,
            max: MAX_FRAME_LENGTH,
        })?;
    let max_frame_length = usize::try_from(MAX_FRAME_LENGTH).unwrap_or(usize::MAX);
    if payload_len > max_frame_length {
        return Err(FrameErrorKind::TooLarge {
            length: i32::try_from(payload_len).unwrap_or(i32::MAX),
            max: MAX_FRAME_LENGTH,
        }
        .into());
    }
    let capacity = 4usize
        .checked_add(payload_len)
        .ok_or(FrameErrorKind::TooLarge {
            length: i32::MAX,
            max: MAX_FRAME_LENGTH,
        })?;
    let payload_len_i32 = i32::try_from(payload_len).map_err(|_| FrameErrorKind::TooLarge {
        length: i32::MAX,
        max: MAX_FRAME_LENGTH,
    })?;
    let mut out = BytesMut::with_capacity(capacity);
    out.put_i32(payload_len_i32);
    out.extend_from_slice(header);
    out.extend_from_slice(body);
    Ok(out)
}
