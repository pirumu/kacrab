//! Kafka record batch v2 support.
//!
//! Record batches are fairly self-contained: [`RecordBatch::encode`] computes
//! its own CRC32C and applies any compression chosen via the `attributes`
//! field; [`RecordBatch::decode`] validates the CRC and decompresses before
//! parsing inner records.

use bytes::{Buf, Bytes, BytesMut};

use crate::primitives::{read_signed_varint, signed_varint_len, write_signed_varint};

pub mod batch;
pub mod entry;
pub mod error;
pub mod header;

pub use self::{
    batch::{RecordBatch, TimestampType, decode_batches},
    entry::Record,
    error::{RecordError, RecordErrorKind},
    header::RecordHeader,
};

/// Result alias for record-batch operations.
pub type Result<T> = core::result::Result<T, RecordError>;

/// Maximum number of records permitted in a single batch. Used to cap
/// `Vec::with_capacity` so a hostile broker cannot trigger a 2 GiB
/// allocation by sending `record_count = i32::MAX`.
pub const MAX_RECORDS_PER_BATCH: usize = 1_000_000;

fn max_i32_len() -> usize {
    usize::try_from(i32::MAX).unwrap_or(usize::MAX)
}

const fn length_overflow(field: &'static str, got: usize, remaining: usize) -> RecordError {
    RecordError::unknown_offset(RecordErrorKind::LengthOverflow {
        field,
        got,
        remaining,
    })
}

fn encode_len(field: &'static str, len: usize) -> Result<i32> {
    i32::try_from(len).map_err(|_| length_overflow(field, len, max_i32_len()))
}

pub(super) fn add_encoded_len(field: &'static str, current: usize, addend: usize) -> Result<usize> {
    current
        .checked_add(addend)
        .ok_or_else(|| length_overflow(field, usize::MAX, max_i32_len()))
}

pub(super) fn bytes_field_len(field: &'static str, bytes: &Bytes) -> Result<usize> {
    let len = encode_len(field, bytes.len())?;
    add_encoded_len(field, signed_varint_len(len), bytes.len())
}

pub(super) fn nullable_bytes_field_len(
    field: &'static str,
    bytes: Option<&Bytes>,
) -> Result<usize> {
    bytes.map_or_else(
        || Ok(signed_varint_len(-1)),
        |bytes| bytes_field_len(field, bytes),
    )
}

fn split_exact(buf: &mut Bytes, field: &'static str, len: usize) -> Result<Bytes> {
    let remaining = buf.remaining();
    if len > remaining {
        return Err(length_overflow(field, len, remaining));
    }
    Ok(buf.split_to(len))
}

pub(super) fn read_bytes_field(buf: &mut Bytes, field: &'static str) -> Result<Bytes> {
    let length = read_signed_varint(buf)?;
    if length < 0 {
        return Err(RecordError::unknown_offset(
            RecordErrorKind::NegativeLength { field, length },
        ));
    }
    let len =
        usize::try_from(length).map_err(|_| length_overflow(field, usize::MAX, buf.remaining()))?;
    split_exact(buf, field, len)
}

pub(super) fn read_nullable_bytes_field(
    buf: &mut Bytes,
    field: &'static str,
) -> Result<Option<Bytes>> {
    let length = read_signed_varint(buf)?;
    if length < 0 {
        return Ok(None);
    }
    let len =
        usize::try_from(length).map_err(|_| length_overflow(field, usize::MAX, buf.remaining()))?;
    split_exact(buf, field, len).map(Some)
}

pub(super) fn write_bytes_field(
    buf: &mut BytesMut,
    field: &'static str,
    bytes: &Bytes,
) -> Result<()> {
    write_signed_varint(buf, encode_len(field, bytes.len())?);
    buf.extend_from_slice(bytes);
    Ok(())
}

pub(super) fn write_nullable_bytes_field(
    buf: &mut BytesMut,
    field: &'static str,
    bytes: Option<&Bytes>,
) -> Result<()> {
    match bytes {
        None => {
            write_signed_varint(buf, -1);
            Ok(())
        },
        Some(bytes) => write_bytes_field(buf, field, bytes),
    }
}
