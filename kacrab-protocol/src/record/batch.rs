//! Record-batch v2 container.
//!
//! Layout (after the 12-byte log overhead):
//!
//! ```text
//! baseOffset (8) | batchLength (4)
//!   ─────── log overhead ────────
//! partitionLeaderEpoch (4) | magic (1) | crc (4)
//!   ─────── CRC-covered region ────────
//!     attributes (2) | lastOffsetDelta (4) | firstTimestamp (8)
//!     maxTimestamp (8) | producerId (8) | producerEpoch (2)
//!     baseSequence (4) | recordCount (4) | records[…]
//! ```

use bytes::{Buf, BufMut, Bytes, BytesMut};

use super::{MAX_RECORDS_PER_BATCH, Record, RecordError, RecordErrorKind, Result};
use crate::{
    compression::Compression,
    crc,
    primitives::{PrimitiveError, PrimitiveErrorKind},
};

const MAGIC_V2: i8 = 2;
const LOG_OVERHEAD: usize = 12;
const BATCH_HEADER_SIZE: i32 = 49;
const BATCH_HEADER_SIZE_USIZE: usize = 49;

/// Kafka timestamp type — derived from bit 3 of the batch `attributes` field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum TimestampType {
    /// Producer-assigned timestamp (default).
    CreateTime,
    /// Broker-assigned timestamp at log append.
    LogAppendTime,
}

/// A Kafka record batch (message format v2).
///
/// `batchLength` and `crc` are not stored — they are derived during encode
/// and validated during decode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordBatch {
    /// Offset of the first record in the batch.
    pub base_offset: i64,
    /// Partition leader epoch (KIP-101).
    pub partition_leader_epoch: i32,
    /// Format version. Always 2 for v2 batches.
    pub magic: i8,
    /// Bit-packed flags: compression (bits 0–2), timestamp type (bit 3),
    /// transactional (bit 4), control (bit 5).
    pub attributes: i16,
    /// Difference between the first and last record offsets.
    pub last_offset_delta: i32,
    /// Wall-clock timestamp of the first record.
    pub first_timestamp: i64,
    /// Wall-clock timestamp of the record with the highest timestamp.
    pub max_timestamp: i64,
    /// Producer ID (idempotent / transactional producer).
    pub producer_id: i64,
    /// Producer epoch (idempotent / transactional producer).
    pub producer_epoch: i16,
    /// First sequence number in the batch (idempotent producer).
    pub base_sequence: i32,
    /// Records in the batch.
    pub records: Vec<Record>,
}

impl RecordBatch {
    /// Encode this batch into `buf`, including the 12-byte log overhead,
    /// CRC32C, and any compression dictated by `self.attributes`.
    pub fn encode(&self, buf: &mut BytesMut) -> Result<()> {
        self.encode_with_compression_level(buf, None)
    }

    /// Encode this batch, passing an optional codec-specific compression level.
    pub fn encode_with_compression_level(
        &self,
        buf: &mut BytesMut,
        compression_level: Option<i32>,
    ) -> Result<()> {
        let codec =
            Compression::from_attributes(self.attributes).map_err(|e| self.lift_compression(e))?;
        if codec == Compression::None {
            let records_payload_len = self.records_encoded_len()?;
            return self.write_batch(buf, records_payload_len, |buf| self.write_records(buf));
        }

        let mut records_buf = BytesMut::with_capacity(self.records_encoded_len()?);
        self.write_records(&mut records_buf)?;
        let records_payload = Bytes::from(
            codec
                .compress_with_level(&records_buf, compression_level)
                .map_err(|e| self.lift_compression(e))?,
        );
        self.write_batch(buf, records_payload.len(), |buf| {
            buf.extend_from_slice(&records_payload);
            Ok(())
        })
    }

    /// Return the exact encoded length when records are written without
    /// compression.
    pub fn uncompressed_encoded_len(&self) -> Result<usize> {
        let len = super::add_encoded_len("record batch", LOG_OVERHEAD, BATCH_HEADER_SIZE_USIZE)?;
        super::add_encoded_len("record batch", len, self.records_encoded_len()?)
    }

    fn records_encoded_len(&self) -> Result<usize> {
        let mut len = 0;
        for record in &self.records {
            len = super::add_encoded_len(
                "record batch records",
                len,
                record
                    .encoded_len()
                    .map_err(|e| RecordError::at_offset(self.base_offset, e.kind))?,
            )?;
        }
        Ok(len)
    }

    fn write_records(&self, buf: &mut BytesMut) -> Result<()> {
        for record in &self.records {
            record
                .encode(buf)
                .map_err(|e| RecordError::at_offset(self.base_offset, e.kind))?;
        }
        Ok(())
    }

    fn write_batch<F>(
        &self,
        buf: &mut BytesMut,
        records_payload_len: usize,
        write_records_payload: F,
    ) -> Result<()>
    where
        F: FnOnce(&mut BytesMut) -> Result<()>,
    {
        buf.put_i64(self.base_offset);

        buf.put_i32(self.batch_length(records_payload_len)?);

        buf.put_i32(self.partition_leader_epoch);
        buf.put_i8(self.magic);

        let crc_field_pos = buf.len();
        buf.put_u32(0);

        let crc_start = buf.len();
        buf.put_i16(self.attributes);
        buf.put_i32(self.last_offset_delta);
        buf.put_i64(self.first_timestamp);
        buf.put_i64(self.max_timestamp);
        buf.put_i64(self.producer_id);
        buf.put_i16(self.producer_epoch);
        buf.put_i32(self.base_sequence);
        buf.put_i32(self.record_count()?);
        write_records_payload(buf)?;

        let crc = crc::crc32c(buf.get(crc_start..).unwrap_or(&[]));
        let crc_bytes = crc.to_be_bytes();
        let crc_field_end = crc_field_pos.checked_add(4).ok_or_else(|| {
            RecordError::at_offset(
                self.base_offset,
                RecordErrorKind::LengthOverflow {
                    field: "crc field",
                    got: crc_field_pos,
                    remaining: buf.len(),
                },
            )
        })?;
        let Some(slot) = buf.get_mut(crc_field_pos..crc_field_end) else {
            return Err(RecordError::at_offset(
                self.base_offset,
                RecordErrorKind::LengthOverflow {
                    field: "crc field",
                    got: crc_field_end,
                    remaining: buf.len(),
                },
            ));
        };
        slot.copy_from_slice(&crc_bytes);

        Ok(())
    }

    fn batch_length(&self, records_payload_len: usize) -> Result<i32> {
        let payload_len = i32::try_from(records_payload_len).map_err(|_| {
            RecordError::at_offset(
                self.base_offset,
                RecordErrorKind::LengthOverflow {
                    field: "compressed records",
                    got: records_payload_len,
                    remaining: usize::try_from(i32::MAX).unwrap_or(usize::MAX),
                },
            )
        })?;
        BATCH_HEADER_SIZE.checked_add(payload_len).ok_or_else(|| {
            RecordError::at_offset(
                self.base_offset,
                RecordErrorKind::LengthOverflow {
                    field: "batch length",
                    got: records_payload_len,
                    remaining: usize::try_from(i32::MAX).unwrap_or(usize::MAX),
                },
            )
        })
    }

    fn record_count(&self) -> Result<i32> {
        i32::try_from(self.records.len()).map_err(|_| {
            RecordError::at_offset(
                self.base_offset,
                RecordErrorKind::RecordCountTooLarge {
                    got: i32::MAX,
                    max: MAX_RECORDS_PER_BATCH,
                },
            )
        })
    }

    /// Decode one batch from `buf`. Validates CRC32C, decompresses if needed,
    /// and rejects a `record_count` above [`super::MAX_RECORDS_PER_BATCH`].
    #[expect(
        clippy::too_many_lines,
        reason = "Record-batch decoding mirrors the Kafka wire layout step-by-step; splitting it \
                  before generated protocol decode is stable would obscure validation order."
    )]
    pub fn decode(buf: &mut Bytes) -> Result<Self> {
        let available = buf.remaining();
        if available < LOG_OVERHEAD {
            return Err(RecordError::unknown_offset(RecordErrorKind::Primitive(
                PrimitiveError::from(PrimitiveErrorKind::InsufficientData {
                    needed: LOG_OVERHEAD,
                    available,
                }),
            )));
        }
        let base_offset = buf.get_i64();
        let batch_length = buf.get_i32();

        if batch_length < BATCH_HEADER_SIZE {
            return Err(RecordError::at_offset(
                base_offset,
                RecordErrorKind::BatchTooSmall {
                    got: batch_length,
                    min: BATCH_HEADER_SIZE,
                },
            ));
        }
        let batch_len = usize::try_from(batch_length).map_err(|_| {
            RecordError::at_offset(
                base_offset,
                RecordErrorKind::LengthOverflow {
                    field: "batch payload",
                    got: usize::MAX,
                    remaining: buf.remaining(),
                },
            )
        })?;
        let remaining = buf.remaining();
        if batch_len > remaining {
            return Err(RecordError::at_offset(
                base_offset,
                RecordErrorKind::LengthOverflow {
                    field: "batch payload",
                    got: batch_len,
                    remaining,
                },
            ));
        }

        let mut batch_data = buf.split_to(batch_len);

        let crc_slice = batch_data.get(5..9).ok_or_else(|| {
            RecordError::at_offset(
                base_offset,
                RecordErrorKind::LengthOverflow {
                    field: "crc field",
                    got: 9,
                    remaining: batch_data.len(),
                },
            )
        })?;
        let crc_bytes: [u8; 4] = crc_slice.try_into().map_err(|_| {
            RecordError::at_offset(
                base_offset,
                RecordErrorKind::LengthOverflow {
                    field: "crc field",
                    got: 4,
                    remaining: crc_slice.len(),
                },
            )
        })?;
        let stored_crc = u32::from_be_bytes(crc_bytes);
        let crc_payload = batch_data.get(9..).unwrap_or(&[]);
        crc::validate_crc32c(crc_payload, stored_crc)
            .map_err(|e| RecordError::at_offset(base_offset, RecordErrorKind::Crc(e)))?;

        let partition_leader_epoch = batch_data.get_i32();
        let magic = batch_data.get_i8();
        if magic != MAGIC_V2 {
            return Err(RecordError::at_offset(
                base_offset,
                RecordErrorKind::UnsupportedMagic(magic),
            ));
        }
        let _crc = batch_data.get_u32();
        let attributes = batch_data.get_i16();
        let last_offset_delta = batch_data.get_i32();
        let first_timestamp = batch_data.get_i64();
        let max_timestamp = batch_data.get_i64();
        let producer_id = batch_data.get_i64();
        let producer_epoch = batch_data.get_i16();
        let base_sequence = batch_data.get_i32();
        let record_count = batch_data.get_i32();

        if record_count < 0 {
            return Err(RecordError::at_offset(
                base_offset,
                RecordErrorKind::NegativeLength {
                    field: "record count",
                    length: record_count,
                },
            ));
        }
        let record_count_usize = usize::try_from(record_count).map_err(|_| {
            RecordError::at_offset(
                base_offset,
                RecordErrorKind::LengthOverflow {
                    field: "record count",
                    got: usize::MAX,
                    remaining: MAX_RECORDS_PER_BATCH,
                },
            )
        })?;
        if record_count_usize > MAX_RECORDS_PER_BATCH {
            return Err(RecordError::at_offset(
                base_offset,
                RecordErrorKind::RecordCountTooLarge {
                    got: record_count,
                    max: MAX_RECORDS_PER_BATCH,
                },
            ));
        }

        let codec = Compression::from_attributes(attributes)
            .map_err(|e| RecordError::at_offset(base_offset, RecordErrorKind::Compression(e)))?;
        let mut records_data = if codec == Compression::None {
            batch_data
        } else {
            let decompressed = codec.decompress(&batch_data).map_err(|e| {
                RecordError::at_offset(base_offset, RecordErrorKind::Compression(e))
            })?;
            Bytes::from(decompressed)
        };

        let mut records = Vec::with_capacity(record_count_usize);
        for _ in 0..record_count {
            let rec = Record::decode(&mut records_data)
                .map_err(|e| RecordError::at_offset(base_offset, e.kind))?;
            records.push(rec);
        }

        Ok(Self {
            base_offset,
            partition_leader_epoch,
            magic,
            attributes,
            last_offset_delta,
            first_timestamp,
            max_timestamp,
            producer_id,
            producer_epoch,
            base_sequence,
            records,
        })
    }

    /// Compression codec selected by bits 0–2 of `attributes`.
    pub fn compression(&self) -> Result<Compression> {
        Compression::from_attributes(self.attributes).map_err(|e| self.lift_compression(e))
    }

    /// Timestamp type from bit 3 of `attributes`.
    #[must_use]
    pub const fn timestamp_type(&self) -> TimestampType {
        if self.attributes & 0x08 != 0 {
            TimestampType::LogAppendTime
        } else {
            TimestampType::CreateTime
        }
    }

    /// `true` if bit 4 of `attributes` is set.
    #[must_use]
    pub const fn is_transactional(&self) -> bool {
        self.attributes & 0x10 != 0
    }

    /// `true` if bit 5 of `attributes` is set.
    #[must_use]
    pub const fn is_control_batch(&self) -> bool {
        self.attributes & 0x20 != 0
    }

    const fn lift_compression(&self, err: crate::compression::CompressionError) -> RecordError {
        RecordError::at_offset(self.base_offset, RecordErrorKind::Compression(err))
    }
}

/// Decode every batch in a contiguous buffer.
///
/// Stops cleanly on a truncated trailing batch (returns the batches decoded so
/// far). Returns an error only if a batch is *malformed* — not just incomplete.
pub fn decode_batches(buf: &mut Bytes) -> Result<Vec<RecordBatch>> {
    let mut batches = Vec::new();
    while buf.remaining() >= LOG_OVERHEAD {
        let Some(len_slice) = buf.get(8..12) else {
            break;
        };
        let len_bytes: [u8; 4] = match len_slice.try_into() {
            Ok(arr) => arr,
            Err(_) => break,
        };
        let batch_length = i32::from_be_bytes(len_bytes);
        if batch_length < 0 {
            break;
        }
        let Ok(batch_len) = usize::try_from(batch_length) else {
            break;
        };
        let needed = LOG_OVERHEAD.saturating_add(batch_len);
        if buf.remaining() < needed {
            break;
        }
        batches.push(RecordBatch::decode(buf)?);
    }
    Ok(batches)
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::expect_used,
        clippy::missing_assert_message,
        reason = "Record-batch encoding tests fail fastest with contextual expect calls."
    )]

    use bytes::{Bytes, BytesMut};

    use super::{Record, RecordBatch};
    use crate::record::RecordHeader;

    #[test]
    fn batch_uncompressed_encoded_len_matches_encoded_bytes() {
        let batch = RecordBatch {
            base_offset: 0,
            partition_leader_epoch: -1,
            magic: 2,
            attributes: 0,
            last_offset_delta: 1,
            first_timestamp: 10,
            max_timestamp: 11,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            records: vec![
                Record {
                    attributes: 0,
                    timestamp_delta: 0,
                    offset_delta: 0,
                    key: Some(Bytes::from_static(b"key-0")),
                    value: Some(Bytes::from_static(b"value-0")),
                    headers: Vec::new(),
                },
                Record {
                    attributes: 0,
                    timestamp_delta: 1,
                    offset_delta: 1,
                    key: None,
                    value: Some(Bytes::from_static(b"value-1")),
                    headers: vec![RecordHeader {
                        key: Bytes::from_static(b"h"),
                        value: None,
                    }],
                },
            ],
        };
        let encoded_len = batch.uncompressed_encoded_len().expect("batch encoded len");
        let mut bytes = BytesMut::with_capacity(encoded_len);

        batch.encode(&mut bytes).expect("batch encode");

        assert_eq!(encoded_len, bytes.len());
        assert_eq!(encoded_len, bytes.capacity());
        let decoded = RecordBatch::decode(&mut bytes.freeze()).expect("record batch decode");
        assert_eq!(decoded.records.len(), 2);
    }
}
