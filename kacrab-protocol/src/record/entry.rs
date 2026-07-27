//! Individual records inside a [`crate::record::RecordBatch`].
//!
//! Wire layout (integers are zigzag varints, except `attributes` which is a
//! fixed `i8`):
//!
//! ```text
//! length (varint) | attributes (i8) | timestampDelta (varlong)
//! offsetDelta (varint) | keyLen (varint) | key | valueLen (varint) | value
//! headerCount (varint) | headers[…]
//! ```

use bytes::{Buf, Bytes, BytesMut};

use super::{RecordError, RecordErrorKind, RecordHeader, Result};
use crate::primitives::{
    read_i8, read_signed_varint, read_signed_varlong, signed_varint_len, signed_varlong_len,
    write_i8, write_signed_varint, write_signed_varlong,
};

/// Smallest number of bytes a single [`RecordHeader`] can occupy on the wire:
/// a zero-length key varint plus a null value varint, one byte each.
///
/// Used to bound the speculative `Vec::with_capacity` in [`Record::decode`]
/// against a hostile `headerCount`, the per-record counterpart to
/// [`super::MAX_RECORDS_PER_BATCH`].
const MIN_HEADER_ENCODED_LEN: usize = 2;

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
    /// Return the exact encoded length of this record, including its body
    /// length prefix.
    pub fn encoded_len(&self) -> Result<usize> {
        let body_len = self.body_encoded_len()?;
        let body_len_i32 = super::encode_len("record body", body_len)?;
        super::add_encoded_len("record body", signed_varint_len(body_len_i32), body_len)
    }

    /// Encode this record into `buf` — body length + body bytes.
    pub fn encode(&self, buf: &mut BytesMut) -> Result<()> {
        let body_len = self.body_encoded_len()?;
        write_signed_varint(buf, super::encode_len("record body", body_len)?);
        self.encode_body(buf)
    }

    fn encode_body(&self, buf: &mut BytesMut) -> Result<()> {
        write_i8(buf, self.attributes);
        write_signed_varlong(buf, self.timestamp_delta);
        write_signed_varint(buf, self.offset_delta);

        super::write_nullable_bytes_field(buf, "record key", self.key.as_ref())?;
        super::write_nullable_bytes_field(buf, "record value", self.value.as_ref())?;

        let hdr_count = i32::try_from(self.headers.len()).map_err(|_| {
            RecordError::unknown_offset(RecordErrorKind::LengthOverflow {
                field: "header count",
                got: self.headers.len(),
                remaining: usize::try_from(i32::MAX).unwrap_or(usize::MAX),
            })
        })?;
        write_signed_varint(buf, hdr_count);
        for header in &self.headers {
            header.encode(buf)?;
        }
        Ok(())
    }

    fn body_encoded_len(&self) -> Result<usize> {
        let mut len = 1;
        len = super::add_encoded_len(
            "record timestamp delta",
            len,
            signed_varlong_len(self.timestamp_delta),
        )?;
        len = super::add_encoded_len(
            "record offset delta",
            len,
            signed_varint_len(self.offset_delta),
        )?;
        len = super::add_encoded_len(
            "record key",
            len,
            super::nullable_bytes_field_len("record key", self.key.as_ref())?,
        )?;
        len = super::add_encoded_len(
            "record value",
            len,
            super::nullable_bytes_field_len("record value", self.value.as_ref())?,
        )?;
        let hdr_count = i32::try_from(self.headers.len()).map_err(|_| {
            RecordError::unknown_offset(RecordErrorKind::LengthOverflow {
                field: "header count",
                got: self.headers.len(),
                remaining: usize::try_from(i32::MAX).unwrap_or(usize::MAX),
            })
        })?;
        len = super::add_encoded_len("header count", len, signed_varint_len(hdr_count))?;
        for header in &self.headers {
            len = super::add_encoded_len("record header", len, header.encoded_len()?)?;
        }
        Ok(len)
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
        // `header_count` is an attacker-controlled varint, so it must never size
        // an allocation on its own: a 90-byte record declaring ~486M headers
        // otherwise reaches `malloc(7.8 GB)` before the loop below reads a
        // single header and fails. The batch level guards its own count with
        // `MAX_RECORDS_PER_BATCH` for exactly this reason; this is the
        // per-record equivalent, bounded by the buffer instead of a constant.
        //
        // A header encodes as at least two varints (key length, value length),
        // so `remaining / 2` is a hard ceiling on how many can possibly follow.
        // Clamping to it can never reject a satisfiable count — a count the
        // buffer can actually hold still preallocates exactly — while an
        // unsatisfiable one now allocates in proportion to the real input and
        // fails in the loop, as it always did.
        let capacity = header_count_usize.min(record_buf.remaining() / MIN_HEADER_ENCODED_LEN);
        let mut headers = Vec::with_capacity(capacity);
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

#[cfg(test)]
mod tests {
    #![allow(
        clippy::expect_used,
        clippy::missing_assert_message,
        reason = "Record encoding tests fail fastest with contextual expect calls."
    )]

    use bytes::{Bytes, BytesMut};

    use super::{MIN_HEADER_ENCODED_LEN, Record, RecordHeader};

    /// A record declaring far more headers than its buffer can hold must fail
    /// on the truncated read, not on a multi-gigabyte allocation first.
    ///
    /// Found by the `record_batch_framed` fuzz target: a 90-byte input reached
    /// `malloc(7_784_624_320)` because `headerCount` sized the `Vec` directly.
    /// Any client parsing a hostile or corrupt broker response could be
    /// OOM-killed by a handful of bytes.
    #[test]
    fn absurd_header_count_fails_without_a_giant_allocation() {
        // length | attributes | timestampDelta | offsetDelta | keyLen(null)
        // | valueLen(null) | headerCount
        let mut body = BytesMut::new();
        crate::primitives::write_i8(&mut body, 0);
        crate::primitives::write_signed_varlong(&mut body, 0);
        crate::primitives::write_signed_varint(&mut body, 0);
        crate::primitives::write_signed_varint(&mut body, -1);
        crate::primitives::write_signed_varint(&mut body, -1);
        // ~486M headers, the shape the fuzzer landed on.
        crate::primitives::write_signed_varint(&mut body, 486_539_020);

        let mut framed = BytesMut::new();
        let body_len = i32::try_from(body.len()).expect("body length fits i32");
        crate::primitives::write_signed_varint(&mut framed, body_len);
        framed.extend_from_slice(&body);

        let mut buf = framed.freeze();
        let decoded = Record::decode(&mut buf);

        assert!(
            decoded.is_err(),
            "a header count the buffer cannot satisfy must be rejected",
        );
    }

    /// The clamp must not cost a legitimate record its exact preallocation.
    #[test]
    fn satisfiable_header_count_still_preallocates_exactly() {
        let record = Record {
            attributes: 0,
            timestamp_delta: 0,
            offset_delta: 0,
            key: None,
            value: None,
            headers: vec![
                RecordHeader {
                    key: Bytes::from_static(b"a"),
                    value: Some(Bytes::from_static(b"1")),
                },
                RecordHeader {
                    key: Bytes::from_static(b"b"),
                    value: None,
                },
            ],
        };
        let encoded_len = record.encoded_len().expect("record encoded len");
        let mut bytes = BytesMut::with_capacity(encoded_len);
        record.encode(&mut bytes).expect("record encode");

        let mut buf = bytes.freeze();
        let decoded = Record::decode(&mut buf).expect("record decode");

        assert_eq!(decoded.headers.len(), 2);
        assert_eq!(decoded.headers, record.headers);
        // Two headers are well under the buffer-derived ceiling, so the clamp
        // is a no-op here — the guard only bites on unsatisfiable counts.
        assert!(2 <= encoded_len / MIN_HEADER_ENCODED_LEN);
    }

    #[test]
    fn record_encoded_len_matches_encoded_bytes_with_headers_and_nulls() {
        let record = Record {
            attributes: 0,
            timestamp_delta: 300,
            offset_delta: 127,
            key: Some(Bytes::from_static(b"key")),
            value: None,
            headers: vec![RecordHeader {
                key: Bytes::from_static(b"h"),
                value: Some(Bytes::from_static(b"value")),
            }],
        };
        let encoded_len = record.encoded_len().expect("record encoded len");
        let mut bytes = BytesMut::with_capacity(encoded_len);

        record.encode(&mut bytes).expect("record encode");

        assert_eq!(encoded_len, bytes.len());
        assert_eq!(encoded_len, bytes.capacity());
    }
}
