//! UTF-8 strings and Kafka's fixed/compact length-prefixed encodings.
//!
//! [`KafkaString`] is backed by [`Bytes`] so decoded messages can borrow from
//! the frame buffer without copying.
//!
//! Kafka uses four string shapes:
//!
//! | Variant                        | Length encoding              |
//! |--------------------------------|------------------------------|
//! | `read_string` / `write_string` | `i16`                        |
//! | `read_compact_string` / …      | unsigned varint of `len + 1` |
//! | `read_nullable_string` / …     | `i16` (`-1` = null)          |
//! | `read_compact_nullable_string` | varint (`0` = null)          |

pub mod error;

use std::fmt;

use bytes::{BufMut, Bytes, BytesMut};

pub use self::error::{StringError, StringErrorKind};
use crate::primitives::{
    check_remaining, read_i16, read_unsigned_varint, unsigned_varint_len, write_unsigned_varint,
};

/// Result alias for string read operations.
pub type Result<T> = core::result::Result<T, StringError>;

/// A Kafka protocol string backed by `Bytes` for zero-copy access.
///
/// UTF-8 is validated on construction so [`Self::as_str`] is infallible
/// without `unsafe` (re-validation is cheap and `unsafe` is forbidden by
/// the workspace lint set).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct KafkaString {
    inner: Bytes,
}

impl KafkaString {
    /// Construct from raw bytes, validating UTF-8.
    pub fn new(b: Bytes) -> Result<Self> {
        if let Err(source) = std::str::from_utf8(&b) {
            return Err(StringErrorKind::InvalidUtf8 { source }.into());
        }
        Ok(Self { inner: b })
    }

    /// Construct from a `'static` string literal. Infallible.
    #[must_use]
    pub const fn from_static(s: &'static str) -> Self {
        Self {
            inner: Bytes::from_static(s.as_bytes()),
        }
    }

    /// Borrow as `&str`.
    #[must_use]
    pub fn as_str(&self) -> &str {
        std::str::from_utf8(&self.inner).unwrap_or_else(|_| {
            debug_assert!(false, "KafkaString contains invalid UTF-8");
            ""
        })
    }

    /// Borrow the raw UTF-8 bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.inner
    }

    /// Length in bytes (not characters).
    #[must_use]
    pub const fn len(&self) -> usize {
        self.inner.len()
    }

    /// `true` if the string has zero bytes.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

impl fmt::Display for KafkaString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl Default for KafkaString {
    fn default() -> Self {
        Self {
            inner: Bytes::new(),
        }
    }
}

impl From<String> for KafkaString {
    fn from(s: String) -> Self {
        Self {
            inner: Bytes::from(s.into_bytes()),
        }
    }
}

fn i16_max_len() -> usize {
    usize::try_from(i16::MAX).unwrap_or(usize::MAX)
}

fn compact_max_len() -> usize {
    usize::try_from(u32::MAX)
        .unwrap_or(usize::MAX)
        .saturating_sub(1)
}

fn i16_len_to_usize(len: i16) -> Result<usize> {
    usize::try_from(len).map_err(|_| {
        StringErrorKind::TooLong {
            length: usize::MAX,
            max: i16_max_len(),
        }
        .into()
    })
}

fn compact_len_to_usize(raw: u32) -> Result<usize> {
    let raw_len = raw.checked_sub(1).ok_or(StringErrorKind::UnexpectedNull)?;
    usize::try_from(raw_len).map_err(|_| {
        StringErrorKind::TooLong {
            length: usize::MAX,
            max: compact_max_len(),
        }
        .into()
    })
}

fn read_i16_len(buf: &mut Bytes) -> Result<usize> {
    let len = read_i16(buf)?;
    if len < 0 {
        return Err(StringErrorKind::NegativeLength {
            length: i32::from(len),
        }
        .into());
    }
    i16_len_to_usize(len)
}

fn read_nullable_i16_len(buf: &mut Bytes) -> Result<Option<usize>> {
    let len = read_i16(buf)?;
    if len < 0 {
        return Ok(None);
    }
    i16_len_to_usize(len).map(Some)
}

fn read_compact_len(buf: &mut Bytes) -> Result<usize> {
    let raw = read_unsigned_varint(buf)?;
    if raw == 0 {
        return Err(StringErrorKind::UnexpectedNull.into());
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

fn split_string(buf: &mut Bytes, len: usize) -> Result<KafkaString> {
    check_remaining(buf, len)?;
    KafkaString::new(buf.split_to(len))
}

fn compact_len_plus_one(len: usize) -> Result<u32> {
    len.checked_add(1)
        .and_then(|len| u32::try_from(len).ok())
        .ok_or_else(|| {
            StringError::new(StringErrorKind::TooLong {
                length: len,
                max: compact_max_len(),
            })
        })
}

/// Read a non-flexible Kafka string (`i16` length prefix).
pub fn read_string(buf: &mut Bytes) -> Result<KafkaString> {
    let len = read_i16_len(buf)?;
    split_string(buf, len)
}

/// Write a non-flexible Kafka string (`i16` length prefix).
pub fn write_string(buf: &mut BytesMut, value: &KafkaString) -> Result<()> {
    let len = i16::try_from(value.len()).map_err(|_| {
        StringError::new(StringErrorKind::TooLong {
            length: value.len(),
            max: i16_max_len(),
        })
    })?;
    buf.put_i16(len);
    buf.extend_from_slice(value.as_bytes());
    Ok(())
}

/// Encoded length of a non-flexible Kafka string (`i16` length prefix).
pub fn string_len(value: &KafkaString) -> Result<usize> {
    let _len = i16::try_from(value.len()).map_err(|_| {
        StringError::new(StringErrorKind::TooLong {
            length: value.len(),
            max: i16_max_len(),
        })
    })?;
    value.len().checked_add(2).ok_or_else(|| {
        StringError::new(StringErrorKind::TooLong {
            length: value.len(),
            max: i16_max_len(),
        })
    })
}

/// Read a compact Kafka string (unsigned varint of `len + 1`).
pub fn read_compact_string(buf: &mut Bytes) -> Result<KafkaString> {
    let len = read_compact_len(buf)?;
    split_string(buf, len)
}

/// Write a compact Kafka string (unsigned varint of `len + 1`).
pub fn write_compact_string(buf: &mut BytesMut, value: &KafkaString) -> Result<()> {
    write_unsigned_varint(buf, compact_len_plus_one(value.len())?);
    buf.extend_from_slice(value.as_bytes());
    Ok(())
}

/// Encoded length of a compact Kafka string.
pub fn compact_string_len(value: &KafkaString) -> Result<usize> {
    let len_plus_one = compact_len_plus_one(value.len())?;
    value
        .len()
        .checked_add(unsigned_varint_len(len_plus_one))
        .ok_or_else(|| {
            StringError::new(StringErrorKind::TooLong {
                length: value.len(),
                max: compact_max_len(),
            })
        })
}

/// Read a nullable non-flexible Kafka string (`i16`, `-1` = null).
pub fn read_nullable_string(buf: &mut Bytes) -> Result<Option<KafkaString>> {
    let Some(len) = read_nullable_i16_len(buf)? else {
        return Ok(None);
    };
    split_string(buf, len).map(Some)
}

/// Write a nullable non-flexible Kafka string (`i16`, `-1` = null).
pub fn write_nullable_string(buf: &mut BytesMut, value: Option<&KafkaString>) -> Result<()> {
    match value {
        None => {
            buf.put_i16(-1);
            Ok(())
        },
        Some(s) => write_string(buf, s),
    }
}

/// Encoded length of a nullable non-flexible Kafka string.
pub fn nullable_string_len(value: Option<&KafkaString>) -> Result<usize> {
    value.map_or(Ok(2), string_len)
}

/// Read a nullable compact Kafka string (unsigned varint, `0` = null).
pub fn read_compact_nullable_string(buf: &mut Bytes) -> Result<Option<KafkaString>> {
    let Some(len) = read_nullable_compact_len(buf)? else {
        return Ok(None);
    };
    split_string(buf, len).map(Some)
}

/// Write a nullable compact Kafka string (unsigned varint, `0` = null).
pub fn write_compact_nullable_string(
    buf: &mut BytesMut,
    value: Option<&KafkaString>,
) -> Result<()> {
    match value {
        None => {
            write_unsigned_varint(buf, 0);
            Ok(())
        },
        Some(s) => write_compact_string(buf, s),
    }
}

/// Encoded length of a nullable compact Kafka string.
pub fn compact_nullable_string_len(value: Option<&KafkaString>) -> Result<usize> {
    value.map_or(Ok(1), compact_string_len)
}
