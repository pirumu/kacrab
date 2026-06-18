//! Per topic-partition record accumulation for producer batching.

use std::{
    collections::VecDeque,
    sync::Arc,
    time::{Duration, Instant},
};

use ahash::AHashMap;
use kacrab_protocol::{signed_varint_len, signed_varlong_len};

use super::{
    error::{ProducerError, Result},
    record::{DeliverySender, ProducerRecord, SendFuture},
    transaction::ProducerBatchState,
};
use crate::wire::{PartitionMetadata, TopicMetadata};

/// Kafka default `batch.size`: 16 KiB is the Java producer baseline that gives
/// useful batching without forcing large per-partition buffers.
const DEFAULT_BATCH_SIZE: usize = 16_384;
/// Kafka default `linger.ms` is zero for the raw accumulator; typed
/// `ProducerConfig` can raise this to Kafka's current producer default.
const DEFAULT_LINGER: Duration = Duration::ZERO;
/// Kafka default `buffer.memory`: 32 MiB bounds queued records while leaving
/// enough room for many topic-partition batches.
const DEFAULT_BUFFER_MEMORY: usize = 33_554_432;
/// Per-record accumulator accounting overhead. It reserves space for the record
/// struct, delivery bookkeeping, and hash-map queue metadata so backpressure
/// trips before payload bytes alone exhaust memory.
const ESTIMATED_RECORD_OVERHEAD_BYTES: usize = 64;
pub(crate) const RECORD_BATCH_OVERHEAD_BYTES: usize = 61;

/// Configuration for producer record accumulation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AccumulatorConfig {
    /// Target batch size in estimated buffered bytes.
    pub batch_size: usize,
    /// Time to wait before a non-full partition batch becomes ready.
    pub linger: Duration,
    /// Total estimated producer memory available to buffered records.
    pub buffer_memory: usize,
}

impl Default for AccumulatorConfig {
    fn default() -> Self {
        Self {
            batch_size: DEFAULT_BATCH_SIZE,
            linger: DEFAULT_LINGER,
            buffer_memory: DEFAULT_BUFFER_MEMORY,
        }
    }
}

impl AccumulatorConfig {
    /// Set the target batch size in estimated buffered bytes.
    #[must_use]
    pub const fn batch_size(mut self, bytes: usize) -> Self {
        self.batch_size = if bytes == 0 { 1 } else { bytes };
        self
    }

    /// Set the linger duration.
    #[must_use]
    pub const fn linger(mut self, linger: Duration) -> Self {
        self.linger = linger;
        self
    }

    /// Set the total estimated buffer memory.
    #[must_use]
    pub const fn buffer_memory(mut self, bytes: usize) -> Self {
        self.buffer_memory = bytes;
        self
    }
}

/// A drained topic-partition batch ready for request construction.
#[derive(Debug)]
pub struct ReadyBatch {
    /// Topic name.
    pub topic: String,
    /// Partition index.
    pub partition: i32,
    /// Records accumulated for this topic-partition.
    pub records: Vec<ProducerRecord>,
    /// Batch delivery state waiting on this topic-partition ack.
    pub(crate) delivery: Option<DeliverySender>,
    /// Estimated bytes released from the accumulator.
    pub bytes: usize,
    /// Timestamp for the first record in this batch.
    pub first_append_at: Instant,
    /// Idempotent producer fields assigned once for this drained batch.
    pub(crate) producer_state: Option<ProducerBatchState>,
}

/// Per-partition queue sizes used by adaptive sticky partitioning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PartitionQueueLoad {
    pub(crate) queue_sizes: Vec<i32>,
    pub(crate) partition_ids: Vec<i32>,
    pub(crate) length: usize,
}

/// Result metadata for an append operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AppendStatus {
    pub(crate) batch_ready: bool,
    pub(crate) ready_batch_records: usize,
}

/// Bounded producer record accumulator keyed by topic-partition.
#[derive(Debug)]
pub struct RecordAccumulator {
    config: AccumulatorConfig,
    partitions: AHashMap<TopicPartition, PartitionQueue>,
    buffered_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct TopicPartition {
    topic: Arc<str>,
    partition: i32,
}

#[derive(Debug)]
struct PartitionQueue {
    batches: VecDeque<PartitionBatch>,
}

#[derive(Debug)]
struct PartitionBatch {
    records: Vec<ProducerRecord>,
    delivery: Option<DeliverySender>,
    producer_state: Option<ProducerBatchState>,
    buffer_bytes: usize,
    batch_bytes: usize,
    sealed: bool,
    first_append_at: Instant,
}

impl RecordAccumulator {
    /// Create an empty accumulator.
    #[must_use]
    pub fn new(config: AccumulatorConfig) -> Self {
        Self {
            config,
            partitions: AHashMap::new(),
            buffered_bytes: 0,
        }
    }

    /// Estimated buffered bytes currently held by the accumulator.
    #[must_use]
    pub const fn buffered_bytes(&self) -> usize {
        self.buffered_bytes
    }

    pub(crate) fn has_available_memory_for(&self, record: &ProducerRecord) -> bool {
        let buffer_bytes = estimate_record_bytes(record);
        buffer_bytes
            <= self
                .config
                .buffer_memory
                .saturating_sub(self.buffered_bytes)
    }

    /// Records currently buffered in the producer accumulator.
    #[must_use]
    pub fn buffered_records(&self) -> usize {
        self.partitions
            .values()
            .flat_map(|queue| queue.batches.iter())
            .map(|batch| batch.records.len())
            .sum()
    }

    /// Build Java-style queue load stats for one topic while excluding partitions
    /// whose leaders are temporarily unavailable for adaptive sticky routing.
    pub(crate) fn partition_queue_load_with_availability<F>(
        &self,
        topic_metadata: &TopicMetadata,
        mut is_partition_available: F,
    ) -> Option<PartitionQueueLoad>
    where
        F: FnMut(&PartitionMetadata) -> bool,
    {
        let partition_count = topic_metadata.partitions.len();
        if partition_count < 2 {
            return None;
        }
        let mut queue_sizes = vec![0; partition_count];
        let mut partition_ids = vec![0; partition_count];
        let mut length = 0;
        for partition in &topic_metadata.partitions {
            let key = TopicPartition {
                topic: Arc::<str>::from(topic_metadata.name.as_str()),
                partition: partition.partition_index,
            };
            let queue = self.partitions.get(&key)?;
            if partition.leader_id < 0 {
                continue;
            }
            let size = i32::try_from(queue.batches.len()).ok()?;
            if is_partition_available(partition) {
                *queue_sizes.get_mut(length)? = size;
                *partition_ids.get_mut(length)? = partition.partition_index;
                length = length.checked_add(1)?;
            }
        }
        Some(PartitionQueueLoad {
            queue_sizes,
            partition_ids,
            length,
        })
    }

    /// Append a record using the current clock.
    pub fn append(&mut self, record: ProducerRecord) -> Result<()> {
        self.append_internal(record, Instant::now())
            .map(|_status| ())
    }

    /// Append a record at a supplied timestamp. Useful for deterministic tests.
    pub fn append_at(&mut self, record: ProducerRecord, now: Instant) -> Result<()> {
        self.append_internal(record, now).map(|_status| ())
    }

    /// Append a record at a supplied timestamp and report whether a batch became ready.
    pub(crate) fn append_with_status_at(
        &mut self,
        record: ProducerRecord,
        now: Instant,
    ) -> Result<AppendStatus> {
        self.append_internal(record, now)
    }

    /// Append a record and return a delivery handle for its eventual broker ack.
    pub fn append_for_delivery(&mut self, record: ProducerRecord) -> Result<SendFuture> {
        let (delivery, _status) =
            self.append_internal_for_delivery(record, Instant::now(), true, 1)?;
        delivery.ok_or(ProducerError::DeliveryDropped)
    }

    pub(crate) fn append_for_delivery_with_status_at(
        &mut self,
        record: ProducerRecord,
        now: Instant,
    ) -> Result<(SendFuture, AppendStatus)> {
        let (delivery, status) = self.append_internal_for_delivery(record, now, true, 1)?;
        let Some(delivery) = delivery else {
            return Err(ProducerError::DeliveryDropped);
        };
        Ok((delivery, status))
    }

    #[cfg(test)]
    pub(crate) fn append_for_batch_delivery_with_status_at(
        &mut self,
        record: ProducerRecord,
        now: Instant,
    ) -> Result<(Option<SendFuture>, AppendStatus)> {
        self.append_for_batch_delivery_with_status_at_capacity(record, now, 1)
    }

    pub(crate) fn append_for_batch_delivery_with_status_at_capacity(
        &mut self,
        record: ProducerRecord,
        now: Instant,
        metadata_capacity: usize,
    ) -> Result<(Option<SendFuture>, AppendStatus)> {
        self.append_internal_for_delivery(record, now, false, metadata_capacity)
    }

    fn append_internal(&mut self, record: ProducerRecord, now: Instant) -> Result<AppendStatus> {
        let buffer_bytes = estimate_record_bytes(&record);
        let available = self
            .config
            .buffer_memory
            .saturating_sub(self.buffered_bytes);
        if buffer_bytes > available {
            return Err(ProducerError::Backpressure);
        }

        let key = TopicPartition {
            topic: Arc::<str>::clone(&record.topic),
            partition: record.partition,
        };
        let queue = self
            .partitions
            .entry(key)
            .or_insert_with(|| PartitionQueue {
                batches: VecDeque::new(),
            });
        let batch_size = self.config.batch_size.max(1);
        let (sealed_previous_records, record_batch_bytes) =
            ensure_append_target(queue, &record, now, batch_size);
        let Some(batch) = queue.batches.back_mut() else {
            return Err(ProducerError::Backpressure);
        };
        batch.buffer_bytes = batch.buffer_bytes.saturating_add(buffer_bytes);
        batch.batch_bytes = batch.batch_bytes.saturating_add(record_batch_bytes);
        let current_batch_records = batch.records.len().saturating_add(1);
        let current_batch_ready = batch.batch_bytes >= batch_size;
        batch.records.push(record);
        self.buffered_bytes = self.buffered_bytes.saturating_add(buffer_bytes);
        Ok(append_status(
            sealed_previous_records,
            current_batch_ready,
            current_batch_records,
        ))
    }

    fn append_internal_for_delivery(
        &mut self,
        record: ProducerRecord,
        now: Instant,
        per_record_delivery: bool,
        metadata_capacity: usize,
    ) -> Result<(Option<SendFuture>, AppendStatus)> {
        let buffer_bytes = estimate_record_bytes(&record);
        let available = self
            .config
            .buffer_memory
            .saturating_sub(self.buffered_bytes);
        if buffer_bytes > available {
            return Err(ProducerError::Backpressure);
        }

        let key = TopicPartition {
            topic: Arc::<str>::clone(&record.topic),
            partition: record.partition,
        };
        let queue = self
            .partitions
            .entry(key)
            .or_insert_with(|| PartitionQueue {
                batches: VecDeque::new(),
            });
        let batch_size = self.config.batch_size.max(1);
        let (sealed_previous_records, record_batch_bytes) =
            ensure_append_target(queue, &record, now, batch_size);
        let Some(batch) = queue.batches.back_mut() else {
            return Err(ProducerError::Backpressure);
        };
        batch.buffer_bytes = batch.buffer_bytes.saturating_add(buffer_bytes);
        batch.batch_bytes = batch.batch_bytes.saturating_add(record_batch_bytes);
        let delivery = if let Some(sender) = &mut batch.delivery {
            if per_record_delivery {
                Some(sender.delivery_for_record(&record))
            } else {
                sender.record_for_batch_callback(&record);
                None
            }
        } else {
            let (sender, delivery) =
                SendFuture::channel_for_record_with_metadata_capacity(&record, metadata_capacity);
            batch.delivery = Some(sender);
            Some(delivery)
        };
        let current_batch_records = batch.records.len().saturating_add(1);
        let current_batch_ready = batch.batch_bytes >= batch_size;
        batch.records.push(record);
        self.buffered_bytes = self.buffered_bytes.saturating_add(buffer_bytes);
        Ok((
            delivery,
            append_status(
                sealed_previous_records,
                current_batch_ready,
                current_batch_records,
            ),
        ))
    }

    /// Drain topic-partition batches that are ready by size or linger timeout.
    pub fn drain_ready(&mut self, now: Instant) -> Vec<ReadyBatch> {
        let batch_size = self.config.batch_size;
        let linger = self.config.linger;
        let ready_keys: Vec<_> = self
            .partitions
            .iter()
            .filter_map(|(key, queue)| {
                queue
                    .batches
                    .front()
                    .is_some_and(|batch| batch_is_ready(batch, now, batch_size, linger))
                    .then_some(key.clone())
            })
            .collect();
        let mut ready = Vec::with_capacity(ready_keys.len());
        for key in ready_keys {
            if let Some(queue) = self.partitions.get_mut(&key) {
                while queue
                    .batches
                    .front()
                    .is_some_and(|batch| batch_is_ready(batch, now, batch_size, linger))
                {
                    let Some(batch) = queue.batches.pop_front() else {
                        break;
                    };
                    self.buffered_bytes = self.buffered_bytes.saturating_sub(batch.buffer_bytes);
                    ready.push(ReadyBatch {
                        topic: key.topic.to_string(),
                        partition: key.partition,
                        records: batch.records,
                        delivery: batch.delivery,
                        bytes: batch.buffer_bytes,
                        first_append_at: batch.first_append_at,
                        producer_state: batch.producer_state,
                    });
                }
            }
        }
        ready
    }

    /// Drain every buffered topic-partition batch regardless of size or linger.
    pub fn drain_all(&mut self) -> Vec<ReadyBatch> {
        let partitions = core::mem::take(&mut self.partitions);
        let mut batches = Vec::with_capacity(partitions.len());
        for (key, queue) in partitions {
            for batch in queue.batches {
                batches.push(ReadyBatch {
                    topic: key.topic.to_string(),
                    partition: key.partition,
                    records: batch.records,
                    delivery: batch.delivery,
                    bytes: batch.buffer_bytes,
                    first_append_at: batch.first_append_at,
                    producer_state: batch.producer_state,
                });
            }
        }
        self.buffered_bytes = 0;
        batches
    }

    /// Return drained batches to the accumulator without re-estimating record sizes.
    pub fn requeue_front(&mut self, batches: Vec<ReadyBatch>) {
        for batch in batches.into_iter().rev() {
            let key = TopicPartition {
                topic: batch.topic.into(),
                partition: batch.partition,
            };
            let entry = self
                .partitions
                .entry(key)
                .or_insert_with(|| PartitionQueue {
                    batches: VecDeque::new(),
                });
            let batch_bytes = estimate_ready_batch_encoded_bytes(&batch.records);
            entry.batches.push_front(PartitionBatch {
                records: batch.records,
                delivery: batch.delivery,
                producer_state: batch.producer_state,
                buffer_bytes: batch.bytes,
                batch_bytes,
                sealed: true,
                first_append_at: batch.first_append_at,
            });
            self.buffered_bytes = self.buffered_bytes.saturating_add(batch.bytes);
        }
    }
}

fn batch_is_ready(
    batch: &PartitionBatch,
    now: Instant,
    batch_size: usize,
    linger: Duration,
) -> bool {
    batch.sealed
        || batch.batch_bytes >= batch_size
        || now.duration_since(batch.first_append_at) >= linger
}

pub(crate) fn estimate_record_bytes(record: &ProducerRecord) -> usize {
    let key_bytes = record.key.as_ref().map_or(0, bytes::Bytes::len);
    let value_bytes = record.value.as_ref().map_or(0, bytes::Bytes::len);
    let header_bytes = estimate_headers_bytes(record);
    ESTIMATED_RECORD_OVERHEAD_BYTES
        .checked_add(record.topic.len())
        .and_then(|bytes| bytes.checked_add(key_bytes))
        .and_then(|bytes| bytes.checked_add(value_bytes))
        .and_then(|bytes| bytes.checked_add(header_bytes))
        .unwrap_or(usize::MAX)
}

pub(crate) fn estimate_record_batch_bytes(record: &ProducerRecord) -> usize {
    estimate_record_batch_bytes_at_offset(record, 0)
}

const fn append_status(
    sealed_previous_records: usize,
    current_batch_ready: bool,
    current_batch_records: usize,
) -> AppendStatus {
    let ready_batch_records = if sealed_previous_records > 0 {
        sealed_previous_records
    } else if current_batch_ready {
        current_batch_records
    } else {
        0
    };
    AppendStatus {
        batch_ready: ready_batch_records > 0,
        ready_batch_records,
    }
}

fn ensure_append_target(
    queue: &mut PartitionQueue,
    record: &ProducerRecord,
    now: Instant,
    batch_size: usize,
) -> (usize, usize) {
    let should_open = match queue.batches.back_mut() {
        None => {
            let record_batch_bytes = estimate_record_batch_bytes_at_offset(record, 0);
            queue
                .batches
                .push_back(new_partition_batch(now, batch_size, record_batch_bytes));
            return (0, record_batch_bytes);
        },
        Some(batch) => {
            let next_record_bytes =
                estimate_record_batch_bytes_at_offset(record, batch.records.len());
            let cannot_fit = !batch.records.is_empty()
                && batch.batch_bytes.saturating_add(next_record_bytes) > batch_size;
            if cannot_fit {
                batch.sealed = true;
            }
            if cannot_fit {
                batch.records.len()
            } else {
                return (0, next_record_bytes);
            }
        },
    };
    let record_batch_bytes = estimate_record_batch_bytes_at_offset(record, 0);
    if should_open > 0 {
        queue
            .batches
            .push_back(new_partition_batch(now, batch_size, record_batch_bytes));
    }
    (should_open, record_batch_bytes)
}

fn new_partition_batch(
    now: Instant,
    batch_size: usize,
    first_record_bytes: usize,
) -> PartitionBatch {
    PartitionBatch {
        records: Vec::with_capacity(estimated_batch_record_capacity(
            batch_size,
            first_record_bytes,
        )),
        delivery: None,
        producer_state: None,
        buffer_bytes: 0,
        batch_bytes: RECORD_BATCH_OVERHEAD_BYTES,
        sealed: false,
        first_append_at: now,
    }
}

fn estimated_batch_record_capacity(batch_size: usize, first_record_bytes: usize) -> usize {
    const MAX_PREALLOCATED_RECORDS: usize = 4096;

    let record_bytes = first_record_bytes.max(1);
    let payload_budget = batch_size
        .saturating_sub(RECORD_BATCH_OVERHEAD_BYTES)
        .max(record_bytes);
    payload_budget
        .checked_div(record_bytes)
        .unwrap_or(1)
        .clamp(1, MAX_PREALLOCATED_RECORDS)
}

fn estimate_ready_batch_encoded_bytes(records: &[ProducerRecord]) -> usize {
    records
        .iter()
        .enumerate()
        .fold(RECORD_BATCH_OVERHEAD_BYTES, |bytes, (offset, record)| {
            bytes.saturating_add(estimate_record_batch_bytes_at_offset(record, offset))
        })
}

fn estimate_record_batch_bytes_at_offset(record: &ProducerRecord, offset_delta: usize) -> usize {
    let key_bytes = record.key.as_ref().map_or(0, bytes::Bytes::len);
    let value_bytes = record.value.as_ref().map_or(0, bytes::Bytes::len);
    let offset_delta = i32::try_from(offset_delta).unwrap_or(i32::MAX);
    let timestamp_delta = record.timestamp_ms.unwrap_or(0);
    let header_count = i32::try_from(record.headers.len()).unwrap_or(i32::MAX);
    let body_len = 1usize
        .saturating_add(signed_varlong_len(timestamp_delta))
        .saturating_add(signed_varint_len(offset_delta))
        .saturating_add(nullable_record_bytes_len(key_bytes, record.key.is_some()))
        .saturating_add(nullable_record_bytes_len(
            value_bytes,
            record.value.is_some(),
        ))
        .saturating_add(signed_varint_len(header_count))
        .saturating_add(estimate_headers_bytes(record));
    let body_len = i32::try_from(body_len).unwrap_or(i32::MAX);
    signed_varint_len(body_len).saturating_add(usize::try_from(body_len).unwrap_or(usize::MAX))
}

fn estimate_headers_bytes(record: &ProducerRecord) -> usize {
    if record.headers.is_empty() {
        return 0;
    }
    record.headers.iter().fold(0usize, |bytes, header| {
        bytes
            .saturating_add(record_bytes_len(header.key.len()))
            .saturating_add(nullable_record_bytes_len(
                header.value.as_ref().map_or(0, bytes::Bytes::len),
                header.value.is_some(),
            ))
    })
}

fn record_bytes_len(bytes: usize) -> usize {
    let len = i32::try_from(bytes).unwrap_or(i32::MAX);
    signed_varint_len(len).saturating_add(bytes)
}

fn nullable_record_bytes_len(bytes: usize, is_some: bool) -> usize {
    if is_some {
        record_bytes_len(bytes)
    } else {
        signed_varint_len(-1)
    }
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::expect_used,
        clippy::missing_assert_message,
        clippy::unwrap_used,
        reason = "Unit test fixtures fail fastest with contextual unwrap/expect calls."
    )]

    use std::{
        sync::{Arc, Mutex},
        time::{Duration, Instant},
    };

    use bytes::Bytes;

    use super::{AccumulatorConfig, ProducerError, RecordAccumulator};
    use crate::producer::{ProducerRecord, RecordMetadata};

    #[test]
    fn config_builder_clamps_zero_batch_size() {
        let config = AccumulatorConfig::default()
            .batch_size(0)
            .linger(Duration::from_millis(2))
            .buffer_memory(128);

        assert_eq!(config.batch_size, 1);
        assert_eq!(config.linger, Duration::from_millis(2));
        assert_eq!(config.buffer_memory, 128);
    }

    #[test]
    fn append_for_delivery_rejects_when_buffer_memory_is_full() {
        let mut accumulator = RecordAccumulator::new(AccumulatorConfig::default().buffer_memory(1));

        let error = accumulator
            .append_for_delivery(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"v")))
            .expect_err("small buffer should apply backpressure");

        assert!(matches!(error, ProducerError::Backpressure));
    }

    #[test]
    fn batch_delivery_callback_tracks_every_record_in_accumulated_batch() {
        let now = Instant::now();
        let mut accumulator = RecordAccumulator::new(
            AccumulatorConfig::default()
                .batch_size(16 * 1024)
                .linger(Duration::from_secs(1)),
        );
        let offsets = Arc::new(Mutex::new(Vec::new()));
        let callback_offsets = Arc::clone(&offsets);
        let (delivery, _status) = accumulator
            .append_for_batch_delivery_with_status_at(
                ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")),
                now,
            )
            .expect("first append");
        let delivery = delivery.expect("first append creates delivery");
        delivery.register_batch_callback(Arc::new(move |result| {
            callback_offsets
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(result.expect("callback receipt").offset);
        }));

        let (delivery, _status) = accumulator
            .append_for_batch_delivery_with_status_at(
                ProducerRecord::new("orders", 0).value(Bytes::from_static(b"b")),
                now,
            )
            .expect("second append");
        assert!(delivery.is_none());
        let (delivery, _status) = accumulator
            .append_for_batch_delivery_with_status_at(
                ProducerRecord::new("orders", 0).value(Bytes::from_static(b"c")),
                now,
            )
            .expect("third append");
        assert!(delivery.is_none());

        let mut batches = accumulator.drain_all();
        let sender = batches[0].delivery.take().expect("delivery sender");
        sender.send(RecordMetadata {
            topic: Arc::from("orders"),
            partition: 0,
            leader_id: 7,
            offset: 40,
            timestamp_ms: -1,
            serialized_key_size: -1,
            serialized_value_size: -1,
        });

        let offsets = offsets
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        assert_eq!(offsets, [40, 41, 42]);
    }

    #[test]
    fn requeue_front_prepends_records_and_preserves_earliest_linger_time() {
        let now = Instant::now();
        let later = now.checked_add(Duration::from_millis(5)).unwrap_or(now);
        let mut accumulator = RecordAccumulator::new(
            AccumulatorConfig::default()
                .batch_size(usize::MAX)
                .linger(Duration::from_secs(1)),
        );
        accumulator
            .append_at(
                ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")),
                later,
            )
            .expect("append later record");
        let existing = accumulator.drain_all();
        accumulator
            .append_at(
                ProducerRecord::new("orders", 0).value(Bytes::from_static(b"b")),
                later,
            )
            .expect("append existing record");

        accumulator.requeue_front(existing);
        let batches = accumulator.drain_all();
        let values: Vec<_> = batches
            .iter()
            .flat_map(|batch| batch.records.iter())
            .filter_map(|record| record.value.as_ref())
            .cloned()
            .collect();

        assert_eq!(values, [Bytes::from_static(b"a"), Bytes::from_static(b"b")]);
    }

    #[test]
    fn requeue_front_preserves_multiple_batch_order_for_same_partition() {
        let mut accumulator = RecordAccumulator::new(
            AccumulatorConfig::default()
                .batch_size(1)
                .linger(Duration::from_secs(1)),
        );
        accumulator
            .append(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")))
            .expect("append first record");
        accumulator
            .append(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"b")))
            .expect("append second record");
        let drained = accumulator.drain_all();

        accumulator.requeue_front(drained);
        let batches = accumulator.drain_all();
        let values: Vec<_> = batches
            .iter()
            .filter_map(|batch| batch.records.first())
            .filter_map(|record| record.value.as_ref())
            .cloned()
            .collect();

        assert_eq!(values, [Bytes::from_static(b"a"), Bytes::from_static(b"b")]);
    }

    #[test]
    fn append_status_reports_when_next_record_seals_ready_batch() {
        let now = Instant::now();
        let mut accumulator = RecordAccumulator::new(
            AccumulatorConfig::default()
                .batch_size(128)
                .linger(Duration::from_secs(1)),
        );
        for _ in 0..8 {
            let status = accumulator
                .append_with_status_at(
                    ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")),
                    now,
                )
                .expect("append record");
            assert!(!status.batch_ready);
        }

        let status = accumulator
            .append_with_status_at(
                ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")),
                now,
            )
            .expect("append record");

        assert!(status.batch_ready);
    }
}
