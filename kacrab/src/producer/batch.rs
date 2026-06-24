//! Record-batch construction for minimal produce requests.

use std::time::{SystemTime, UNIX_EPOCH};

use bytes::{Bytes, BytesMut};
use kacrab_protocol::record::{Record, RecordBatch};

use super::{
    config::ProducerCompression, error::Result, record::ProducerRecord,
    transaction::ProducerBatchState,
};

/// Kafka record-batch magic v2 is required for producer id, epoch, sequence,
/// timestamp fields, and compression attributes used by modern brokers.
const RECORD_BATCH_MAGIC_V2: i8 = 2;
/// Kafka sentinel for non-idempotent producer id/epoch/base sequence.
const NO_PRODUCER_ID: i64 = -1;
/// Kafka sentinel for non-idempotent producer epoch/base sequence.
const NO_PRODUCER_EPOCH_OR_SEQUENCE: i16 = -1;
/// Kacrab does not set per-record attributes yet; batch-level compression owns
/// the current attribute bits.
const DEFAULT_RECORD_ATTRIBUTES: i8 = 0;
/// Kafka uses zero base offset for request-side record batches; brokers assign
/// the final log offset in produce responses.
#[cfg(test)]
const REQUEST_RECORD_BATCH_BASE_OFFSET: i64 = 0;
/// Partition leader epoch is unknown on produce requests from clients.
const UNKNOWN_PARTITION_LEADER_EPOCH: i32 = -1;

#[cfg(all(test, feature = "lz4"))]
pub(crate) fn encode_record_batch_with_compression(
    records: &[ProducerRecord],
    compression: ProducerCompression,
) -> Result<Bytes> {
    let (records, timestamp_base) = producer_records(records);
    encode_records(
        records,
        compression,
        None,
        REQUEST_RECORD_BATCH_BASE_OFFSET,
        timestamp_base,
    )
}

pub(crate) fn encode_record_batch_with_producer_state_at_offset(
    records: &[ProducerRecord],
    compression: ProducerCompression,
    producer_state: Option<ProducerBatchState>,
    base_offset: i64,
) -> Result<Bytes> {
    let (records, timestamp_base) = producer_records(records);
    encode_records(
        records,
        compression,
        producer_state,
        base_offset,
        timestamp_base,
    )
}

#[cfg(test)]
pub(crate) fn encode_record_batch_with_producer_state_at_offset_into(
    records: &[ProducerRecord],
    compression: ProducerCompression,
    producer_state: Option<ProducerBatchState>,
    base_offset: i64,
    bytes: &mut BytesMut,
) -> Result<Bytes> {
    let (records, timestamp_base) = producer_records(records);
    let batch = record_batch(
        records,
        compression,
        producer_state,
        base_offset,
        timestamp_base,
    );
    encode_record_batch_into(&batch, compression, bytes)
}

pub(crate) fn encode_record_batch_with_producer_state_at_offset_into_buffer(
    records: &[ProducerRecord],
    compression: ProducerCompression,
    producer_state: Option<ProducerBatchState>,
    base_offset: i64,
    bytes: &mut BytesMut,
) -> Result<()> {
    let (records, timestamp_base) = producer_records(records);
    let batch = record_batch(
        records,
        compression,
        producer_state,
        base_offset,
        timestamp_base,
    );
    encode_record_batch_into_buffer(&batch, compression, bytes)
}

fn encode_records(
    records: Vec<Record>,
    compression: ProducerCompression,
    producer_state: Option<ProducerBatchState>,
    base_offset: i64,
    timestamp_base: RecordBatchTimestamps,
) -> Result<Bytes> {
    let mut bytes = BytesMut::new();
    let batch = record_batch(
        records,
        compression,
        producer_state,
        base_offset,
        timestamp_base,
    );
    encode_record_batch_into(&batch, compression, &mut bytes)
}

fn record_batch(
    records: Vec<Record>,
    compression: ProducerCompression,
    producer_state: Option<ProducerBatchState>,
    base_offset: i64,
    timestamp_base: RecordBatchTimestamps,
) -> RecordBatch {
    let (producer_id, producer_epoch, base_sequence) = producer_state.map_or_else(
        || {
            (
                NO_PRODUCER_ID,
                NO_PRODUCER_EPOCH_OR_SEQUENCE,
                i32::from(NO_PRODUCER_EPOCH_OR_SEQUENCE),
            )
        },
        |state| {
            (
                state.identity.producer_id,
                state.identity.producer_epoch,
                state.base_sequence,
            )
        },
    );
    RecordBatch {
        base_offset,
        partition_leader_epoch: UNKNOWN_PARTITION_LEADER_EPOCH,
        magic: RECORD_BATCH_MAGIC_V2,
        attributes: compression.codec as i16,
        last_offset_delta: i32::try_from(records.len().saturating_sub(1)).unwrap_or(i32::MAX),
        first_timestamp: timestamp_base.first_timestamp,
        max_timestamp: timestamp_base.max_timestamp,
        producer_id,
        producer_epoch,
        base_sequence,
        records,
    }
}

fn encode_record_batch_into(
    batch: &RecordBatch,
    compression: ProducerCompression,
    bytes: &mut BytesMut,
) -> Result<Bytes> {
    encode_record_batch_into_buffer(batch, compression, bytes)?;
    Ok(bytes.split_to(bytes.len()).freeze())
}

fn encode_record_batch_into_buffer(
    batch: &RecordBatch,
    compression: ProducerCompression,
    bytes: &mut BytesMut,
) -> Result<()> {
    bytes.clear();
    bytes.reserve(batch.uncompressed_encoded_len()?);
    batch.encode_with_compression_level(bytes, compression.level)?;
    Ok(())
}

fn producer_records(records: &[ProducerRecord]) -> (Vec<Record>, RecordBatchTimestamps) {
    let fallback = current_time_ms();
    let first_timestamp = records
        .first()
        .and_then(|record| record.timestamp_ms)
        .unwrap_or(fallback);
    let mut max_timestamp = first_timestamp;
    let records = records
        .iter()
        .enumerate()
        .map(|(index, record)| {
            max_timestamp = max_timestamp.max(record.timestamp_ms.unwrap_or(fallback));
            let timestamp_delta = record
                .timestamp_ms
                .map_or(0, |timestamp| timestamp.saturating_sub(first_timestamp));
            Record {
                attributes: DEFAULT_RECORD_ATTRIBUTES,
                timestamp_delta,
                offset_delta: i32::try_from(index).unwrap_or(i32::MAX),
                key: record.key.clone(),
                value: record.value.clone(),
                headers: clone_record_headers(record),
            }
        })
        .collect();
    (
        records,
        RecordBatchTimestamps {
            first_timestamp,
            max_timestamp,
        },
    )
}

fn clone_record_headers(record: &ProducerRecord) -> Vec<kacrab_protocol::record::RecordHeader> {
    if record.headers.is_empty() {
        Vec::new()
    } else {
        record.headers.clone()
    }
}

#[derive(Debug, Clone, Copy)]
struct RecordBatchTimestamps {
    first_timestamp: i64,
    max_timestamp: i64,
}

pub(crate) fn current_time_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
        })
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::expect_used,
        clippy::missing_assert_message,
        clippy::unwrap_used,
        reason = "Unit test fixtures fail fastest with contextual unwrap/expect calls."
    )]

    use std::time::{Duration, Instant};

    use bytes::{Bytes, BytesMut};
    use kacrab_protocol::record::RecordBatch;

    use super::{
        REQUEST_RECORD_BATCH_BASE_OFFSET, encode_record_batch_with_producer_state_at_offset,
        encode_record_batch_with_producer_state_at_offset_into,
    };
    use crate::producer::{
        AccumulatorConfig, ProducerBatchState, ProducerCompression, ProducerIdentity,
        ProducerRecord, RecordAccumulator,
    };

    #[test]
    fn record_batch_sets_idempotent_producer_fields() {
        let encoded = encode_record_batch_with_producer_state_at_offset(
            &[ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value"))],
            ProducerCompression::default(),
            Some(ProducerBatchState {
                identity: ProducerIdentity {
                    producer_id: 42,
                    producer_epoch: 3,
                },
                base_sequence: 7,
            }),
            REQUEST_RECORD_BATCH_BASE_OFFSET,
        )
        .expect("idempotent batch should encode");
        let mut encoded = encoded;
        let decoded = RecordBatch::decode(&mut encoded).expect("record batch");

        assert_eq!(decoded.producer_id, 42);
        assert_eq!(decoded.producer_epoch, 3);
        assert_eq!(decoded.base_sequence, 7);
    }

    #[test]
    fn record_batch_can_set_request_side_base_offset() {
        let encoded = encode_record_batch_with_producer_state_at_offset(
            &[ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value"))],
            ProducerCompression::default(),
            None,
            9,
        )
        .expect("batch should encode");
        let mut encoded = encoded;
        let decoded = RecordBatch::decode(&mut encoded).expect("record batch");

        assert_eq!(decoded.base_offset, 9);
    }

    #[test]
    fn record_batch_encoder_can_write_into_reusable_buffer() {
        let records = [ProducerRecord::new("orders", 0)
            .try_timestamp_ms(1_000)
            .expect("timestamp")
            .value(Bytes::from_static(b"value"))];
        let expected = encode_record_batch_with_producer_state_at_offset(
            &records,
            ProducerCompression::default(),
            None,
            REQUEST_RECORD_BATCH_BASE_OFFSET,
        )
        .expect("batch should encode");
        let mut buffer = BytesMut::with_capacity(4 * 1024);
        buffer.extend_from_slice(b"stale");

        let encoded = encode_record_batch_with_producer_state_at_offset_into(
            &records,
            ProducerCompression::default(),
            None,
            REQUEST_RECORD_BATCH_BASE_OFFSET,
            &mut buffer,
        )
        .expect("batch should encode into caller buffer");

        assert_eq!(encoded, expected);
        assert!(buffer.is_empty());
    }

    #[test]
    fn record_batch_sets_create_time_timestamp_from_clock() {
        let before = super::current_time_ms();
        let encoded = encode_record_batch_with_producer_state_at_offset(
            &[ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value"))],
            ProducerCompression::default(),
            None,
            REQUEST_RECORD_BATCH_BASE_OFFSET,
        )
        .expect("batch should encode");
        let after = super::current_time_ms();
        let mut encoded = encoded;
        let decoded = RecordBatch::decode(&mut encoded).expect("record batch");

        assert!(decoded.first_timestamp >= before);
        assert!(decoded.first_timestamp <= after);
        assert_eq!(decoded.max_timestamp, decoded.first_timestamp);
        assert_eq!(
            decoded.records.first().expect("one record").timestamp_delta,
            0
        );
    }

    #[test]
    fn accumulator_append_freezes_create_time_before_later_encode_like_java() {
        let mut accumulator = RecordAccumulator::new(
            AccumulatorConfig::default()
                .batch_size(1)
                .buffer_memory(1024)
                .linger(Duration::from_secs(1)),
        );
        let before_append = super::current_time_ms();
        accumulator
            .append_at(
                ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")),
                Instant::now(),
            )
            .expect("append record without user timestamp");
        let after_append = super::current_time_ms();

        std::thread::sleep(Duration::from_millis(15));
        let drained = accumulator.drain_all();
        let encoded = encode_record_batch_with_producer_state_at_offset(
            &drained[0].records,
            ProducerCompression::default(),
            None,
            REQUEST_RECORD_BATCH_BASE_OFFSET,
        )
        .expect("batch should encode");
        let mut encoded = encoded;
        let decoded = RecordBatch::decode(&mut encoded).expect("record batch");

        assert!(decoded.first_timestamp >= before_append);
        assert!(decoded.first_timestamp <= after_append);
        assert_eq!(decoded.max_timestamp, decoded.first_timestamp);
    }

    #[test]
    fn record_batch_encodes_user_timestamps_and_headers() {
        let encoded = encode_record_batch_with_producer_state_at_offset(
            &[
                ProducerRecord::new("orders", 0)
                    .try_timestamp_ms(1_000)
                    .expect("first timestamp")
                    .header("trace-id", Bytes::from_static(b"abc"))
                    .value(Bytes::from_static(b"first")),
                ProducerRecord::new("orders", 0)
                    .try_timestamp_ms(1_025)
                    .expect("second timestamp")
                    .header_null("null-header")
                    .value(Bytes::from_static(b"second")),
            ],
            ProducerCompression::default(),
            None,
            REQUEST_RECORD_BATCH_BASE_OFFSET,
        )
        .expect("batch should encode");
        let mut encoded = encoded;
        let decoded = RecordBatch::decode(&mut encoded).expect("record batch");

        assert_eq!(decoded.first_timestamp, 1_000);
        assert_eq!(decoded.max_timestamp, 1_025);
        assert_eq!(decoded.records[0].timestamp_delta, 0);
        assert_eq!(decoded.records[1].timestamp_delta, 25);
        assert_eq!(decoded.records[0].headers.len(), 1);
        assert_eq!(
            decoded.records[0].headers[0].key,
            Bytes::from_static(b"trace-id")
        );
        assert_eq!(
            decoded.records[0].headers[0].value,
            Some(Bytes::from_static(b"abc"))
        );
        assert_eq!(decoded.records[1].headers.len(), 1);
        assert_eq!(
            decoded.records[1].headers[0].key,
            Bytes::from_static(b"null-header")
        );
        assert_eq!(decoded.records[1].headers[0].value, None);
    }

    #[cfg(feature = "lz4")]
    #[test]
    fn record_batch_sets_and_round_trips_compression_codec() {
        use kacrab_protocol::compression::Compression;

        use super::encode_record_batch_with_compression;

        let encoded = encode_record_batch_with_compression(
            &[ProducerRecord::new("orders", 0)
                .key(Bytes::from_static(b"k"))
                .value(Bytes::from_static(b"value"))],
            ProducerCompression {
                codec: Compression::Lz4,
                level: Some(9),
            },
        )
        .expect("compressed batch should encode");
        let mut encoded = encoded;
        let decoded = RecordBatch::decode(&mut encoded).expect("compressed batch should decode");

        assert_eq!(
            decoded.compression().expect("compression"),
            Compression::Lz4
        );
        let record = decoded.records.first().expect("one record");
        assert_eq!(record.key, Some(Bytes::from_static(b"k")));
        assert_eq!(record.value, Some(Bytes::from_static(b"value")));
    }
}
