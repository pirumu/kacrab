//! Kafka length-prefixed byte payloads.
//!
//! This module handles opaque bytes only. UTF-8 validation belongs in
//! [`crate::string`].

pub mod error;

use bytes::{BufMut, Bytes, BytesMut};

pub use self::error::{BytesError, BytesErrorKind};
use crate::primitives::{check_remaining, read_i32, read_unsigned_varint, write_unsigned_varint};

/// Result alias for raw bytes read operations.
pub type Result<T> = core::result::Result<T, BytesError>;

fn i32_max_len() -> usize {
    usize::try_from(i32::MAX).unwrap_or(usize::MAX)
}

fn compact_max_len() -> usize {
    usize::try_from(u32::MAX)
        .unwrap_or(usize::MAX)
        .saturating_sub(1)
}

fn i32_len_to_usize(len: i32) -> Result<usize> {
    usize::try_from(len).map_err(|_| {
        BytesErrorKind::TooLong {
            length: usize::MAX,
            max: i32_max_len(),
        }
        .into()
    })
}

fn compact_len_to_usize(raw: u32) -> Result<usize> {
    let raw_len = raw.checked_sub(1).ok_or(BytesErrorKind::UnexpectedNull)?;
    usize::try_from(raw_len).map_err(|_| {
        BytesErrorKind::TooLong {
            length: usize::MAX,
            max: compact_max_len(),
        }
        .into()
    })
}

fn read_i32_len(buf: &mut Bytes) -> Result<usize> {
    let len = read_i32(buf)?;
    if len < 0 {
        return Err(BytesErrorKind::NegativeLength { length: len }.into());
    }
    i32_len_to_usize(len)
}

fn read_nullable_i32_len(buf: &mut Bytes) -> Result<Option<usize>> {
    let len = read_i32(buf)?;
    if len < 0 {
        return Ok(None);
    }
    i32_len_to_usize(len).map(Some)
}

fn read_compact_len(buf: &mut Bytes) -> Result<usize> {
    let raw = read_unsigned_varint(buf)?;
    if raw == 0 {
        return Err(BytesErrorKind::UnexpectedNull.into());
    }
    compact_len_to_usize(raw)
}

fn read_nullable_compact_len(buf: &mut Bytes) -> Result<Option<usize>> {
    let raw = read_unsigned_varint(buf)?;
    if raw == 0 {
        return Ok(None);
    }
    compact_len_to_usize(raw).map(Some)
}

fn split_bytes(buf: &mut Bytes, len: usize) -> Result<Bytes> {
    check_remaining(buf, len)?;
    Ok(buf.split_to(len))
}

fn compact_len_plus_one(len: usize) -> Result<u32> {
    len.checked_add(1)
        .and_then(|len| u32::try_from(len).ok())
        .ok_or_else(|| {
            BytesError::new(BytesErrorKind::TooLong {
                length: len,
                max: compact_max_len(),
            })
        })
}

/// Read non-flexible bytes (`i32` length prefix).
pub fn read_bytes(buf: &mut Bytes) -> Result<Bytes> {
    let len = read_i32_len(buf)?;
    split_bytes(buf, len)
}

/// Write non-flexible bytes (`i32` length prefix).
pub fn write_bytes(buf: &mut BytesMut, value: &[u8]) -> Result<()> {
    let len = i32::try_from(value.len()).map_err(|_| {
        BytesError::new(BytesErrorKind::TooLong {
            length: value.len(),
            max: i32_max_len(),
        })
    })?;
    buf.put_i32(len);
    buf.extend_from_slice(value);
    Ok(())
}

/// Read compact bytes (unsigned varint of `len + 1`).
pub fn read_compact_bytes(buf: &mut Bytes) -> Result<Bytes> {
    let len = read_compact_len(buf)?;
    split_bytes(buf, len)
}

/// Write compact bytes (unsigned varint of `len + 1`).
pub fn write_compact_bytes(buf: &mut BytesMut, value: &[u8]) -> Result<()> {
    let len_plus_one = compact_len_plus_one(value.len())?;
    write_unsigned_varint(buf, len_plus_one);
    buf.extend_from_slice(value);
    Ok(())
}

/// Read nullable non-flexible bytes (`i32`, `-1` = null).
pub fn read_nullable_bytes(buf: &mut Bytes) -> Result<Option<Bytes>> {
    let Some(len) = read_nullable_i32_len(buf)? else {
        return Ok(None);
    };
    split_bytes(buf, len).map(Some)
}

/// Write nullable non-flexible bytes (`i32`, `-1` = null).
pub fn write_nullable_bytes(buf: &mut BytesMut, value: Option<&[u8]>) -> Result<()> {
    match value {
        None => {
            buf.put_i32(-1);
            Ok(())
        },
        Some(bytes) => write_bytes(buf, bytes),
    }
}

/// Read nullable compact bytes (unsigned varint, `0` = null).
pub fn read_compact_nullable_bytes(buf: &mut Bytes) -> Result<Option<Bytes>> {
    let Some(len) = read_nullable_compact_len(buf)? else {
        return Ok(None);
    };
    split_bytes(buf, len).map(Some)
}

/// Write nullable compact bytes (unsigned varint, `0` = null).
pub fn write_compact_nullable_bytes(buf: &mut BytesMut, value: Option<&[u8]>) -> Result<()> {
    match value {
        None => {
            write_unsigned_varint(buf, 0);
            Ok(())
        },
        Some(bytes) => write_compact_bytes(buf, bytes),
    }
}
