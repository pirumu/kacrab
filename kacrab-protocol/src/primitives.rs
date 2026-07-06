//! Fixed-width and varint primitive read/write helpers.
//!
//! Every Kafka schema field eventually decomposes to one of these primitives.
//! Helpers operate on `bytes::Bytes` / `bytes::BytesMut` and return
//! [`PrimitiveError`] on insufficient data or malformed varints.

pub mod error;

use bytes::{Buf, BufMut, Bytes, BytesMut};

pub use self::error::{PrimitiveError, PrimitiveErrorKind};

/// Result alias for primitive read operations.
pub type Result<T> = core::result::Result<T, PrimitiveError>;

const VARINT_MAX_BYTES: u8 = 5;
const VARLONG_MAX_BYTES: u8 = 10;

pub(crate) fn check_remaining(buf: &Bytes, needed: usize) -> Result<()> {
    let available = buf.remaining();
    if available < needed {
        return Err(PrimitiveErrorKind::InsufficientData { needed, available }.into());
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Fixed-width integers / float / bool — big-endian on the wire.
// ---------------------------------------------------------------------------

/// Read a boolean (1 byte: 0 = false, nonzero = true).
pub fn read_bool(buf: &mut Bytes) -> Result<bool> {
    check_remaining(buf, 1)?;
    Ok(buf.get_u8() != 0)
}

/// Read a signed 8-bit integer.
pub fn read_i8(buf: &mut Bytes) -> Result<i8> {
    check_remaining(buf, 1)?;
    Ok(buf.get_i8())
}

/// Read a signed 16-bit integer (big-endian).
pub fn read_i16(buf: &mut Bytes) -> Result<i16> {
    check_remaining(buf, 2)?;
    Ok(buf.get_i16())
}

/// Read a signed 32-bit integer (big-endian).
pub fn read_i32(buf: &mut Bytes) -> Result<i32> {
    check_remaining(buf, 4)?;
    Ok(buf.get_i32())
}

/// Read a signed 64-bit integer (big-endian).
pub fn read_i64(buf: &mut Bytes) -> Result<i64> {
    check_remaining(buf, 8)?;
    Ok(buf.get_i64())
}

/// Read an unsigned 16-bit integer (big-endian).
pub fn read_u16(buf: &mut Bytes) -> Result<u16> {
    check_remaining(buf, 2)?;
    Ok(buf.get_u16())
}

/// Read an unsigned 32-bit integer (big-endian).
pub fn read_u32(buf: &mut Bytes) -> Result<u32> {
    check_remaining(buf, 4)?;
    Ok(buf.get_u32())
}

/// Read a 64-bit IEEE-754 float (big-endian).
pub fn read_f64(buf: &mut Bytes) -> Result<f64> {
    check_remaining(buf, 8)?;
    Ok(buf.get_f64())
}

/// Write a boolean (1 byte: 0 or 1).
pub fn write_bool(buf: &mut BytesMut, value: bool) {
    buf.put_u8(u8::from(value));
}

/// Write a signed 8-bit integer.
pub fn write_i8(buf: &mut BytesMut, value: i8) {
    buf.put_i8(value);
}

/// Write a signed 16-bit integer (big-endian).
pub fn write_i16(buf: &mut BytesMut, value: i16) {
    buf.put_i16(value);
}

/// Write a signed 32-bit integer (big-endian).
pub fn write_i32(buf: &mut BytesMut, value: i32) {
    buf.put_i32(value);
}

/// Write a signed 64-bit integer (big-endian).
pub fn write_i64(buf: &mut BytesMut, value: i64) {
    buf.put_i64(value);
}

/// Write an unsigned 16-bit integer (big-endian).
pub fn write_u16(buf: &mut BytesMut, value: u16) {
    buf.put_u16(value);
}

/// Write an unsigned 32-bit integer (big-endian).
pub fn write_u32(buf: &mut BytesMut, value: u32) {
    buf.put_u32(value);
}

/// Write a 64-bit IEEE-754 float (big-endian).
pub fn write_f64(buf: &mut BytesMut, value: f64) {
    buf.put_f64(value);
}

// ---------------------------------------------------------------------------
// Varints (Protocol Buffers style, MSB continuation bit).
// ---------------------------------------------------------------------------

/// Read an unsigned varint (1–5 bytes, `u32` payload).
pub fn read_unsigned_varint(buf: &mut Bytes) -> Result<u32> {
    let mut value: u32 = 0;
    for i in 0..VARINT_MAX_BYTES {
        check_remaining(buf, 1)?;
        let byte = buf.get_u8();
        value |= u32::from(byte & 0x7f) << (u32::from(i) * 7);
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(PrimitiveErrorKind::InvalidVarint {
        max_bytes: VARINT_MAX_BYTES,
    }
    .into())
}

/// Write an unsigned varint (1–5 bytes, `u32` payload).
pub fn write_unsigned_varint(buf: &mut BytesMut, mut value: u32) {
    loop {
        let low = value.to_le_bytes()[0] & 0x7f;
        value >>= 7;
        if value == 0 {
            buf.put_u8(low);
            return;
        }
        buf.put_u8(low | 0x80);
    }
}

/// Encoded length of an unsigned varint (1–5 bytes, `u32` payload).
#[must_use]
pub const fn unsigned_varint_len(value: u32) -> usize {
    match value {
        0x0..=0x7f => 1,
        0x80..=0x3fff => 2,
        0x4000..=0x1f_ffff => 3,
        0x20_0000..=0xfff_ffff => 4,
        0x1000_0000..=u32::MAX => 5,
    }
}

/// Encoded length of an unsigned varlong (1–10 bytes, `u64` payload).
#[must_use]
pub const fn unsigned_varlong_len(value: u64) -> usize {
    match value {
        0x0..=0x7f => 1,
        0x80..=0x3fff => 2,
        0x4000..=0x1f_ffff => 3,
        0x20_0000..=0xfff_ffff => 4,
        0x1000_0000..=0x7_ffff_ffff => 5,
        0x8_0000_0000..=0x3ff_ffff_ffff => 6,
        0x400_0000_0000..=0x1_ffff_ffff_ffff => 7,
        0x2_0000_0000_0000..=0xff_ffff_ffff_ffff => 8,
        0x100_0000_0000_0000..=0x7fff_ffff_ffff_ffff => 9,
        0x8000_0000_0000_0000..=u64::MAX => 10,
    }
}

/// Read an unsigned varlong (1–10 bytes, `u64` payload).
pub fn read_unsigned_varlong(buf: &mut Bytes) -> Result<u64> {
    let mut value: u64 = 0;
    for i in 0..VARLONG_MAX_BYTES {
        check_remaining(buf, 1)?;
        let byte = buf.get_u8();
        value |= u64::from(byte & 0x7f) << (u32::from(i) * 7);
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(PrimitiveErrorKind::InvalidVarint {
        max_bytes: VARLONG_MAX_BYTES,
    }
    .into())
}

/// Write an unsigned varlong (1–10 bytes, `u64` payload).
pub fn write_unsigned_varlong(buf: &mut BytesMut, mut value: u64) {
    loop {
        let low = value.to_le_bytes()[0] & 0x7f;
        value >>= 7;
        if value == 0 {
            buf.put_u8(low);
            return;
        }
        buf.put_u8(low | 0x80);
    }
}

/// Read a signed varint (zigzag-decoded `i32`).
pub fn read_signed_varint(buf: &mut Bytes) -> Result<i32> {
    let v = read_unsigned_varint(buf)?;
    let magnitude = (v >> 1).cast_signed();
    let sign_mask = (v & 1).cast_signed().wrapping_neg();
    Ok(magnitude ^ sign_mask)
}

/// Write a signed varint (zigzag-encoded `i32`).
pub fn write_signed_varint(buf: &mut BytesMut, value: i32) {
    let encoded = ((value << 1) ^ (value >> 31)).cast_unsigned();
    write_unsigned_varint(buf, encoded);
}

/// Encoded length of a signed varint (zigzag-encoded `i32`).
#[must_use]
pub const fn signed_varint_len(value: i32) -> usize {
    let encoded = ((value << 1) ^ (value >> 31)).cast_unsigned();
    unsigned_varint_len(encoded)
}

/// Read a signed varlong (zigzag-decoded `i64`).
pub fn read_signed_varlong(buf: &mut Bytes) -> Result<i64> {
    let v = read_unsigned_varlong(buf)?;
    let magnitude = (v >> 1).cast_signed();
    let sign_mask = (v & 1).cast_signed().wrapping_neg();
    Ok(magnitude ^ sign_mask)
}

/// Write a signed varlong (zigzag-encoded `i64`).
pub fn write_signed_varlong(buf: &mut BytesMut, value: i64) {
    let encoded = ((value << 1) ^ (value >> 63)).cast_unsigned();
    write_unsigned_varlong(buf, encoded);
}

/// Encoded length of a signed varlong (zigzag-encoded `i64`).
#[must_use]
pub const fn signed_varlong_len(value: i64) -> usize {
    let encoded = ((value << 1) ^ (value >> 63)).cast_unsigned();
    unsigned_varlong_len(encoded)
}

// ---------------------------------------------------------------------------
// Array length helpers (used by both fixed-width and compact array encoding).
// ---------------------------------------------------------------------------

/// Read a non-flexible array length (`i32`). `-1` indicates a null array.
pub fn read_array_length(buf: &mut Bytes) -> Result<i32> {
    read_i32(buf)
}

/// Write a non-flexible array length (`i32`).
pub fn write_array_length(buf: &mut BytesMut, len: i32) {
    write_i32(buf, len);
}

/// Encoded length of a non-flexible array length prefix.
#[must_use]
pub const fn array_length_len() -> usize {
    4
}

/// Read a compact array length (varint of `len + 1`, `0 → -1` (null)).
pub fn read_compact_array_length(buf: &mut Bytes) -> Result<i32> {
    let raw = read_unsigned_varint(buf)?;
    if raw == 0 {
        Ok(-1)
    } else {
        let len = raw
            .checked_sub(1)
            .ok_or(PrimitiveErrorKind::InvalidVarint {
                max_bytes: VARINT_MAX_BYTES,
            })?;
        i32::try_from(len).map_err(|_| PrimitiveErrorKind::LengthOutOfRange { length: len }.into())
    }
}

/// Write a compact array length (varint of `len + 1`, negative → `0` (null)).
pub fn write_compact_array_length(buf: &mut BytesMut, len: i32) {
    if len < 0 {
        write_unsigned_varint(buf, 0);
    } else {
        let encoded = len.cast_unsigned().saturating_add(1);
        write_unsigned_varint(buf, encoded);
    }
}

/// Encoded length of a compact array length prefix.
#[must_use]
pub const fn compact_array_length_len(len: i32) -> usize {
    if len < 0 {
        unsigned_varint_len(0)
    } else {
        unsigned_varint_len(len.cast_unsigned().saturating_add(1))
    }
}

/// Initial `Vec` capacity for a decoded array: the claimed element count
/// clamped by the bytes actually remaining in the buffer (every element costs
/// at least one wire byte) and a fixed budget. The claimed length comes off
/// the wire, so it must never be trusted for allocation — a hostile or corrupt
/// length of `i32::MAX` would otherwise reserve gigabytes up front and abort
/// the process under `panic = "abort"`. Arrays longer than the budget grow on
/// demand as elements are actually decoded.
#[must_use]
pub fn array_read_capacity(len: i32, remaining: usize) -> usize {
    /// Elements worth of `Vec` capacity we are willing to reserve up front.
    const MAX_PREALLOC_ELEMENTS: usize = 1024;
    usize::try_from(len)
        .unwrap_or(0)
        .min(remaining)
        .min(MAX_PREALLOC_ELEMENTS)
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::missing_assert_message,
        reason = "Primitive helper tests keep assertions compact."
    )]

    use bytes::BytesMut;

    use super::{
        array_read_capacity, signed_varint_len, signed_varlong_len, write_signed_varint,
        write_signed_varlong,
    };

    #[test]
    fn array_read_capacity_never_trusts_the_claimed_length() {
        // A hostile length is clamped by the bytes actually remaining.
        assert_eq!(array_read_capacity(i32::MAX, 7), 7);
        // Negative (null-array sentinel leaking through) reserves nothing.
        assert_eq!(array_read_capacity(-1, 100), 0);
        // A sane length within the budget is used as-is.
        assert_eq!(array_read_capacity(3, 100), 3);
        // Huge-but-plausible lengths stop at the fixed budget.
        assert_eq!(array_read_capacity(1_000_000, usize::MAX), 1024);
    }

    #[test]
    fn signed_varint_len_matches_written_bytes() {
        for value in [0, -1, 1, 63, 64, -64, i32::MAX, i32::MIN] {
            let mut bytes = BytesMut::new();
            write_signed_varint(&mut bytes, value);

            assert_eq!(signed_varint_len(value), bytes.len());
        }
    }

    #[test]
    fn signed_varlong_len_matches_written_bytes() {
        for value in [0, -1, 1, 63, 64, -64, i64::MAX, i64::MIN] {
            let mut bytes = BytesMut::new();
            write_signed_varlong(&mut bytes, value);

            assert_eq!(signed_varlong_len(value), bytes.len());
        }
    }
}
