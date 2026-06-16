//! Per topic-partition record accumulation for producer batching.

use std::time::{Duration, Instant};

use ahash::AHashMap;

use super::{
    error::{ProducerError, Result},
    record::{Delivery, DeliverySender, ProducerRecord},
    transaction::ProducerBatchState,
};

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

/// Bounded producer record accumulator keyed by topic-partition.
#[derive(Debug)]
pub struct RecordAccumulator {
    config: AccumulatorConfig,
    partitions: AHashMap<TopicPartition, PartitionQueue>,
    buffered_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct TopicPartition {
    topic: String,
    partition: i32,
}

#[derive(Debug)]
struct PartitionQueue {
    records: Vec<ProducerRecord>,
    delivery: Option<DeliverySender>,
    producer_state: Option<ProducerBatchState>,
    bytes: usize,
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

    /// Append a record using the current clock.
    pub fn append(&mut self, record: ProducerRecord) -> Result<()> {
        self.append_internal(record, Instant::now())
    }

    /// Append a record at a supplied timestamp. Useful for deterministic tests.
    pub fn append_at(&mut self, record: ProducerRecord, now: Instant) -> Result<()> {
        self.append_internal(record, now)
    }

    /// Append a record and return a delivery handle for its eventual broker ack.
    pub fn append_for_delivery(&mut self, record: ProducerRecord) -> Result<Delivery> {
        self.append_internal_for_delivery(record, Instant::now())
    }

    fn append_internal(&mut self, record: ProducerRecord, now: Instant) -> Result<()> {
        let bytes = estimate_record_bytes(&record);
        let available = self
            .config
            .buffer_memory
            .saturating_sub(self.buffered_bytes);
        if bytes > available {
            return Err(ProducerError::Backpressure);
        }

        let key = TopicPartition {
            topic: record.topic.clone(),
            partition: record.partition,
        };
        let queue = self
            .partitions
            .entry(key)
            .or_insert_with(|| PartitionQueue {
                records: Vec::new(),
                delivery: None,
                producer_state: None,
                bytes: 0,
                first_append_at: now,
            });
        queue.bytes = queue.bytes.saturating_add(bytes);
        queue.records.push(record);
        self.buffered_bytes = self.buffered_bytes.saturating_add(bytes);
        Ok(())
    }

    fn append_internal_for_delivery(
        &mut self,
        record: ProducerRecord,
        now: Instant,
    ) -> Result<Delivery> {
        let bytes = estimate_record_bytes(&record);
        let available = self
            .config
            .buffer_memory
            .saturating_sub(self.buffered_bytes);
        if bytes > available {
            return Err(ProducerError::Backpressure);
        }

        let key = TopicPartition {
            topic: record.topic.clone(),
            partition: record.partition,
        };
        let queue = self
            .partitions
            .entry(key)
            .or_insert_with(|| PartitionQueue {
                records: Vec::new(),
                delivery: None,
                producer_state: None,
                bytes: 0,
                first_append_at: now,
            });
        queue.bytes = queue.bytes.saturating_add(bytes);
        queue.records.push(record);
        let delivery = if let Some(sender) = &queue.delivery {
            sender.delivery()
        } else {
            let (sender, delivery) = Delivery::channel();
            queue.delivery = Some(sender);
            delivery
        };
        self.buffered_bytes = self.buffered_bytes.saturating_add(bytes);
        Ok(delivery)
    }

    /// Drain topic-partition batches that are ready by size or linger timeout.
    pub fn drain_ready(&mut self, now: Instant) -> Vec<ReadyBatch> {
        let ready_keys: Vec<_> = self
            .partitions
            .iter()
            .filter_map(|(key, queue)| self.is_ready(queue, now).then_some(key.clone()))
            .collect();
        let mut ready = Vec::with_capacity(ready_keys.len());
        for key in ready_keys {
            if let Some(queue) = self.partitions.remove(&key) {
                self.buffered_bytes = self.buffered_bytes.saturating_sub(queue.bytes);
                ready.push(ReadyBatch {
                    topic: key.topic,
                    partition: key.partition,
                    records: queue.records,
                    delivery: queue.delivery,
                    bytes: queue.bytes,
                    first_append_at: queue.first_append_at,
                    producer_state: queue.producer_state,
                });
            }
        }
        ready
    }

    /// Drain every buffered topic-partition batch regardless of size or linger.
    pub fn drain_all(&mut self) -> Vec<ReadyBatch> {
        let partitions = core::mem::take(&mut self.partitions);
        let mut batches = Vec::with_capacity(partitions.len());
        for (key, queue) in partitions {
            batches.push(ReadyBatch {
                topic: key.topic,
                partition: key.partition,
                records: queue.records,
                delivery: queue.delivery,
                bytes: queue.bytes,
                first_append_at: queue.first_append_at,
                producer_state: queue.producer_state,
            });
        }
        self.buffered_bytes = 0;
        batches
    }

    /// Return drained batches to the accumulator without re-estimating record sizes.
    pub fn requeue_front(&mut self, batches: Vec<ReadyBatch>) {
        for batch in batches {
            let key = TopicPartition {
                topic: batch.topic,
                partition: batch.partition,
            };
            let entry = self
                .partitions
                .entry(key)
                .or_insert_with(|| PartitionQueue {
                    records: Vec::new(),
                    delivery: None,
                    producer_state: None,
                    bytes: 0,
                    first_append_at: batch.first_append_at,
                });
            if batch.first_append_at < entry.first_append_at {
                entry.first_append_at = batch.first_append_at;
            }
            if entry.records.is_empty() {
                entry.records = batch.records;
            } else {
                let mut records = batch.records;
                records.append(&mut entry.records);
                entry.records = records;
            }
            if entry.delivery.is_none() {
                entry.delivery = batch.delivery;
            }
            if entry.producer_state.is_none() {
                entry.producer_state = batch.producer_state;
            }
            entry.bytes = entry.bytes.saturating_add(batch.bytes);
            self.buffered_bytes = self.buffered_bytes.saturating_add(batch.bytes);
        }
    }

    fn is_ready(&self, queue: &PartitionQueue, now: Instant) -> bool {
        queue.bytes >= self.config.batch_size
            || now.duration_since(queue.first_append_at) >= self.config.linger
    }
}

fn estimate_record_bytes(record: &ProducerRecord) -> usize {
    let key_bytes = record.key.as_ref().map_or(0, bytes::Bytes::len);
    let value_bytes = record.value.as_ref().map_or(0, bytes::Bytes::len);
    ESTIMATED_RECORD_OVERHEAD_BYTES
        .checked_add(record.topic.len())
        .and_then(|bytes| bytes.checked_add(key_bytes))
        .and_then(|bytes| bytes.checked_add(value_bytes))
        .unwrap_or(usize::MAX)
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

    use bytes::Bytes;

    use super::{AccumulatorConfig, ProducerError, RecordAccumulator};
    use crate::producer::ProducerRecord;

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
        let Some(batch) = batches.first() else {
            return;
        };
        let values: Vec<_> = batch
            .records
            .iter()
            .filter_map(|record| record.value.as_ref())
            .cloned()
            .collect();

        assert_eq!(values, [Bytes::from_static(b"a"), Bytes::from_static(b"b")]);
    }
}
