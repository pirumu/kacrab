//! Tagged-fields section for flexible Kafka message versions.
//!
//! Wire format: `unsigned_varint(count)` then for each field
//! `unsigned_varint(tag), unsigned_varint(size), <size bytes>`. Tags MUST be
//! strictly ascending; readers reject duplicates and out-of-order tags.

pub mod error;

use bytes::{Buf, Bytes, BytesMut};

pub use self::error::TaggedFieldError;
use crate::primitives::{read_unsigned_varint, write_unsigned_varint};

/// Result alias for tagged-field operations.
pub type Result<T> = core::result::Result<T, TaggedFieldError>;

/// A single raw tagged field. The body bytes are stored verbatim — interpretation
/// is the caller's responsibility (the schema dictates the encoding per tag).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawTaggedField {
    /// Tag number.
    pub tag: u32,
    /// Raw payload bytes.
    pub data: Bytes,
}

/// Read the tagged-fields section.
///
/// Enforces ascending tag order and that each declared `size` is available
/// in the buffer.
pub fn read_tagged_fields(buf: &mut Bytes) -> Result<Vec<RawTaggedField>> {
    let count = read_unsigned_varint(buf)?;
    let count_usize = usize::try_from(count).map_err(|_| TaggedFieldError::CountOverflow {
        count,
        max: usize::MAX,
    })?;
    let mut fields = Vec::with_capacity(count_usize);
    let mut prev_tag: Option<u32> = None;

    for _ in 0..count {
        let tag = read_unsigned_varint(buf)?;
        if let Some(prev) = prev_tag
            && tag <= prev
        {
            return Err(TaggedFieldError::OutOfOrder {
                tag,
                prev_tag: prev,
            });
        }
        prev_tag = Some(tag);

        let raw_size = read_unsigned_varint(buf)?;
        let size = usize::try_from(raw_size).map_err(|_| TaggedFieldError::SizeOverflow {
            tag,
            size: usize::MAX,
            remaining: buf.remaining(),
        })?;
        let remaining = buf.remaining();
        if size > remaining {
            return Err(TaggedFieldError::SizeOverflow {
                tag,
                size,
                remaining,
            });
        }
        let bytes = buf.split_to(size);
        fields.push(RawTaggedField { tag, data: bytes });
    }

    Ok(fields)
}

/// Write the tagged-fields section.
///
/// Caller must supply fields sorted by ascending tag; this is asserted, not
/// re-sorted, to keep the wire encoding deterministic without surprise costs.
pub fn write_tagged_fields(buf: &mut BytesMut, fields: &[RawTaggedField]) -> Result<()> {
    let mut prev_tag: Option<u32> = None;
    for field in fields {
        if let Some(prev) = prev_tag
            && field.tag <= prev
        {
            return Err(TaggedFieldError::OutOfOrder {
                tag: field.tag,
                prev_tag: prev,
            });
        }
        prev_tag = Some(field.tag);
    }

    let field_count = u32::try_from(fields.len()).map_err(|_| TaggedFieldError::CountOverflow {
        count: u32::MAX,
        max: usize::try_from(u32::MAX).unwrap_or(usize::MAX),
    })?;
    write_unsigned_varint(buf, field_count);
    for field in fields {
        write_unsigned_varint(buf, field.tag);
        let field_len =
            u32::try_from(field.data.len()).map_err(|_| TaggedFieldError::FieldTooLarge {
                tag: field.tag,
                size: field.data.len(),
                max: usize::try_from(u32::MAX).unwrap_or(usize::MAX),
            })?;
        write_unsigned_varint(buf, field_len);
        buf.extend_from_slice(&field.data);
    }
    Ok(())
}
