//! Individual records inside a [`crate::record::RecordBatch`].
//!
//! Wire layout (all integers are zigzag varints):
//!
//! ```text
//! length (varint) | attributes (i8) | timestampDelta (varlong)
//! offsetDelta (varint) | keyLen (varint) | key | valueLen (varint) | value
//! headerCount (varint) | headers[…]
//! ```

use bytes::{Buf, Bytes, BytesMut};

use super::{RecordError, RecordErrorKind, RecordHeader, Result};
use crate::primitives::{
    read_i8, read_signed_varint, read_signed_varlong, write_i8, write_signed_varint,
    write_signed_varlong,
};

/// A single record in a v2 [`crate::record::RecordBatch`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    /// Reserved — currently always 0.
    pub attributes: i8,
    /// Signed varlong delta from the batch's `first_timestamp`.
    pub timestamp_delta: i64,
    /// Signed varint delta from the batch's `base_offset`.
    pub offset_delta: i32,
    /// Record key. `None` when the on-wire key length is `-1`.
    pub key: Option<Bytes>,
    /// Record value. `None` when the on-wire value length is `-1`.
    pub value: Option<Bytes>,
    /// Record headers.
    pub headers: Vec<RecordHeader>,
}

impl Record {
    /// Encode this record into `buf` — body length + body bytes.
    pub fn encode(&self, buf: &mut BytesMut) -> Result<()> {
        let mut body = BytesMut::new();
        write_i8(&mut body, self.attributes);
        write_signed_varlong(&mut body, self.timestamp_delta);
        write_signed_varint(&mut body, self.offset_delta);

        super::write_nullable_bytes_field(&mut body, "record key", self.key.as_ref())?;
        super::write_nullable_bytes_field(&mut body, "record value", self.value.as_ref())?;

        let hdr_count = i32::try_from(self.headers.len()).map_err(|_| {
            RecordError::unknown_offset(RecordErrorKind::LengthOverflow {
                field: "header count",
                got: self.headers.len(),
                remaining: usize::try_from(i32::MAX).unwrap_or(usize::MAX),
            })
        })?;
        write_signed_varint(&mut body, hdr_count);
        for header in &self.headers {
            header.encode(&mut body)?;
        }

        let body_len = i32::try_from(body.len()).map_err(|_| {
            RecordError::unknown_offset(RecordErrorKind::LengthOverflow {
                field: "record body",
                got: body.len(),
                remaining: usize::try_from(i32::MAX).unwrap_or(usize::MAX),
            })
        })?;
        write_signed_varint(buf, body_len);
        buf.extend_from_slice(&body);
        Ok(())
    }

    /// Decode one record from `buf`. Validates that the body length fits in
    /// the remaining buffer before splitting.
    pub fn decode(buf: &mut Bytes) -> Result<Self> {
        let body_length = read_signed_varint(buf)?;
        if body_length < 0 {
            return Err(RecordError::unknown_offset(
                RecordErrorKind::NegativeLength {
                    field: "record body",
                    length: body_length,
                },
            ));
        }
        let body_len = usize::try_from(body_length).map_err(|_| {
            RecordError::unknown_offset(RecordErrorKind::LengthOverflow {
                field: "record body",
                got: usize::MAX,
                remaining: buf.remaining(),
            })
        })?;
        let remaining = buf.remaining();
        if body_len > remaining {
            return Err(RecordError::unknown_offset(
                RecordErrorKind::LengthOverflow {
                    field: "record body",
                    got: body_len,
                    remaining,
                },
            ));
        }
        let mut record_buf = buf.split_to(body_len);

        let attributes = read_i8(&mut record_buf)?;
        let timestamp_delta = read_signed_varlong(&mut record_buf)?;
        let offset_delta = read_signed_varint(&mut record_buf)?;

        let key = super::read_nullable_bytes_field(&mut record_buf, "record key")?;
        let value = super::read_nullable_bytes_field(&mut record_buf, "record value")?;

        let header_count = read_signed_varint(&mut record_buf)?;
        if header_count < 0 {
            return Err(RecordError::unknown_offset(
                RecordErrorKind::NegativeLength {
                    field: "header count",
                    length: header_count,
                },
            ));
        }
        let header_count_usize = usize::try_from(header_count).map_err(|_| {
            RecordError::unknown_offset(RecordErrorKind::LengthOverflow {
                field: "header count",
                got: usize::MAX,
                remaining: record_buf.remaining(),
            })
        })?;
        let mut headers = Vec::with_capacity(header_count_usize);
        for _ in 0..header_count {
            headers.push(RecordHeader::decode(&mut record_buf)?);
        }

        Ok(Self {
            attributes,
            timestamp_delta,
            offset_delta,
            key,
            value,
            headers,
        })
    }
}
