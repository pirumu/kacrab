//! Per topic-partition record accumulation for producer batching.

use std::{
    collections::VecDeque,
    sync::Arc,
    time::{Duration, Instant},
};

use ahash::{AHashMap, AHashSet};
use kacrab_protocol::{signed_varint_len, signed_varlong_len};

use super::{
    batch::current_time_ms,
    error::{ProducerError, Result},
    record::{DeliverySender, ProducerRecord, SendFuture},
    transaction::ProducerBatchState,
};
use crate::wire::{PartitionMetadata, TopicMetadata};

/// Kafka default `batch.size`: 16 KiB is the Kafka producer baseline that gives
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
const COMPRESSION_RATE_ESTIMATION_FACTOR: f32 = 1.05;

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
    pub(crate) identity: ReadyBatchIdentity,
    /// Topic name.
    pub topic: String,
    /// Partition index.
    pub partition: i32,
    /// Records accumulated for this topic-partition.
    pub records: Vec<ProducerRecord>,
    /// Batch delivery state waiting on this topic-partition ack.
    pub(crate) delivery: Option<DeliverySender>,
    /// Estimated batch bytes used for produce-batch metrics.
    pub bytes: usize,
    /// Bytes currently held against pooled buffer memory.
    pub(crate) pooled_buffer_bytes: usize,
    /// Timestamp for the first record in this batch.
    pub first_append_at: Instant,
    /// Idempotent producer fields assigned once for this drained batch.
    pub(crate) producer_state: Option<ProducerBatchState>,
}

impl ReadyBatch {
    #[cfg(test)]
    pub(crate) const fn identity(&self) -> ReadyBatchIdentity {
        self.identity
    }

    pub(crate) const fn pooled_buffer_bytes(&self) -> usize {
        self.pooled_buffer_bytes
    }

    pub(crate) fn split_for_retry_with_compression_ratio(
        self,
        target_batch_bytes: usize,
        compression_ratio: f32,
    ) -> Option<Vec<Self>> {
        if self.records.len() <= 1 {
            return None;
        }
        let identity = self.identity;
        let topic = self.topic;
        let partition = self.partition;
        let mut delivery = self.delivery;
        let first_append_at = self.first_append_at;
        let producer_state = self.producer_state;
        let original_bytes = self.bytes;
        let split_groups =
            split_records_by_batch_target(self.records, target_batch_bytes, compression_ratio);
        let mut remaining_bytes = original_bytes;
        let last_index = split_groups.len().saturating_sub(1);
        let split = split_groups
            .into_iter()
            .enumerate()
            .map(|(index, group)| {
                let bytes = if index == last_index {
                    remaining_bytes
                } else {
                    let bytes = group.bytes.min(remaining_bytes);
                    remaining_bytes = remaining_bytes.saturating_sub(bytes);
                    bytes
                };
                Self {
                    identity: identity.split_child(u32::try_from(index).unwrap_or(u32::MAX)),
                    topic: topic.clone(),
                    partition,
                    records: group.records,
                    delivery: if index == 0 { delivery.take() } else { None },
                    bytes,
                    pooled_buffer_bytes: 0,
                    first_append_at,
                    producer_state: split_producer_state(producer_state, group.first_record_index),
                }
            })
            .collect();
        Some(split)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum ReadyBatchIdentity {
    Accumulator(u64),
    Split {
        parent: u64,
        index: u32,
    },
    #[cfg(test)]
    Test(u64),
}

impl ReadyBatchIdentity {
    const fn split_child(self, index: u32) -> Self {
        match self {
            Self::Accumulator(parent) | Self::Split { parent, .. } => Self::Split { parent, index },
            #[cfg(test)]
            Self::Test(parent) => Self::Split { parent, index },
        }
    }

    const fn split_parent(self) -> Option<Self> {
        match self {
            Self::Split { parent, .. } => Some(Self::Accumulator(parent)),
            Self::Accumulator(_) => None,
            #[cfg(test)]
            Self::Test(_) => None,
        }
    }

    #[cfg(test)]
    pub(crate) const fn test(id: u64) -> Self {
        Self::Test(id)
    }
}

struct SplitRecordGroup {
    first_record_index: usize,
    records: Vec<ProducerRecord>,
    bytes: usize,
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
    pub(crate) starts_new_batch: bool,
}

/// Bounded producer record accumulator keyed by topic-partition.
#[derive(Debug)]
pub struct RecordAccumulator {
    config: AccumulatorConfig,
    partitions: AHashMap<TopicPartition, PartitionQueue>,
    buffered_batch_identities: AHashSet<ReadyBatchIdentity>,
    incomplete_batch_identities: AHashSet<ReadyBatchIdentity>,
    buffered_bytes: usize,
    next_batch_id: u64,
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
    identity: ReadyBatchIdentity,
    records: Vec<ProducerRecord>,
    delivery: Option<DeliverySender>,
    producer_state: Option<ProducerBatchState>,
    buffer_bytes: usize,
    batch_bytes: usize,
    compression_sizing: CompressionSizing,
    sealed: bool,
    first_append_at: Instant,
}

#[derive(Debug, Clone, Copy)]
struct AppendTarget {
    sealed_previous_records: usize,
    record_batch_bytes: usize,
    starts_new_batch: bool,
    reserved_buffer_bytes: usize,
}

#[derive(Debug, Clone, Copy)]
struct CompressionSizing {
    ratio: f32,
}

impl CompressionSizing {
    const NONE: Self = Self { ratio: 1.0 };

    fn new(ratio: f32) -> Self {
        if ratio.is_finite() && ratio > 0.0 {
            Self { ratio }
        } else {
            Self::NONE
        }
    }

    const fn uses_estimate(self) -> bool {
        (self.ratio - 1.0).abs() > f32::EPSILON
    }
}

impl RecordAccumulator {
    /// Create an empty accumulator.
    #[must_use]
    pub fn new(config: AccumulatorConfig) -> Self {
        Self {
            config,
            partitions: AHashMap::new(),
            buffered_batch_identities: AHashSet::new(),
            incomplete_batch_identities: AHashSet::new(),
            buffered_bytes: 0,
            next_batch_id: 0,
        }
    }

    /// Estimated buffered bytes currently held by the accumulator.
    #[must_use]
    pub const fn buffered_bytes(&self) -> usize {
        self.buffered_bytes
    }

    pub(crate) const fn buffer_memory(&self) -> usize {
        self.config.buffer_memory
    }

    pub(crate) fn has_available_memory_for_reserved_with_compression_ratio(
        &self,
        record: &ProducerRecord,
        reserved_bytes: usize,
        compression_ratio: f32,
    ) -> bool {
        let append_reservation =
            self.append_buffer_reservation_with_compression_ratio(record, compression_ratio);
        append_reservation
            <= self
                .config
                .buffer_memory
                .saturating_sub(self.buffered_bytes)
                .saturating_sub(reserved_bytes)
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

    /// Batches currently owned by the producer accumulator.
    #[must_use]
    pub(crate) fn buffered_batches(&self) -> usize {
        self.partitions
            .values()
            .map(|queue| queue.batches.len())
            .sum()
    }

    /// Build queue load stats for one topic while excluding partitions
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
    #[cfg(test)]
    pub(crate) fn append_with_status_at(
        &mut self,
        record: ProducerRecord,
        now: Instant,
    ) -> Result<AppendStatus> {
        self.append_internal(record, now)
    }

    #[cfg(test)]
    pub(crate) fn append_with_status_at_compression_ratio(
        &mut self,
        record: ProducerRecord,
        now: Instant,
        compression_ratio: f32,
    ) -> Result<AppendStatus> {
        self.append_internal_with_compression_sizing(
            record,
            now,
            CompressionSizing::new(compression_ratio),
        )
    }

    /// Append a record and return a delivery handle for its eventual broker ack.
    pub fn append_for_delivery(&mut self, record: ProducerRecord) -> Result<SendFuture> {
        let (delivery, _status) =
            self.append_internal_for_delivery(record, Instant::now(), 1, CompressionSizing::NONE)?;
        delivery.ok_or(ProducerError::DeliveryDropped)
    }

    pub(crate) fn append_for_delivery_with_status_at_compression_ratio(
        &mut self,
        record: ProducerRecord,
        now: Instant,
        compression_ratio: f32,
    ) -> Result<(SendFuture, AppendStatus)> {
        let (delivery, status) = self.append_internal_for_delivery(
            record,
            now,
            1,
            CompressionSizing::new(compression_ratio),
        )?;
        let Some(delivery) = delivery else {
            return Err(ProducerError::DeliveryDropped);
        };
        Ok((delivery, status))
    }

    fn append_internal(&mut self, record: ProducerRecord, now: Instant) -> Result<AppendStatus> {
        self.append_internal_with_compression_sizing(record, now, CompressionSizing::NONE)
    }

    fn append_internal_with_compression_sizing(
        &mut self,
        record: ProducerRecord,
        now: Instant,
        compression_sizing: CompressionSizing,
    ) -> Result<AppendStatus> {
        let record = record_with_append_timestamp(record);
        let key = TopicPartition {
            topic: Arc::<str>::clone(&record.topic),
            partition: record.partition,
        };
        let batch_size = self.config.batch_size.max(1);
        let available = self
            .config
            .buffer_memory
            .saturating_sub(self.buffered_bytes);
        // Single hash lookup: take the (mutable) partition queue up front and
        // compute the append target from it, instead of an immutable get()
        // followed by a separate entry() — both hash the topic Arc<str> on every
        // append. An empty queue plans the same target as a missing one.
        let queue = self
            .partitions
            .entry(key)
            .or_insert_with(|| PartitionQueue {
                batches: VecDeque::new(),
            });
        let target = planned_append_target(Some(&*queue), &record, batch_size, compression_sizing);
        if target.reserved_buffer_bytes > available {
            return Err(ProducerError::Backpressure);
        }
        let next_identity = &mut self.next_batch_id;
        if let Some(identity) = apply_append_target(queue, now, batch_size, target, next_identity) {
            let _inserted = self.buffered_batch_identities.insert(identity);
            let _inserted = self.incomplete_batch_identities.insert(identity);
        }
        let Some(batch) = queue.batches.back_mut() else {
            return Err(ProducerError::Backpressure);
        };
        batch.compression_sizing = compression_sizing;
        batch.batch_bytes = batch.batch_bytes.saturating_add(target.record_batch_bytes);
        let current_batch_records = batch.records.len().saturating_add(1);
        let current_batch_ready =
            estimated_batch_bytes_for_sizing(batch.batch_bytes, compression_sizing) >= batch_size;
        batch.records.push(record);
        self.buffered_bytes = self
            .buffered_bytes
            .saturating_add(target.reserved_buffer_bytes);
        Ok(append_status(
            target.sealed_previous_records,
            current_batch_ready,
            current_batch_records,
            target.starts_new_batch,
        ))
    }

    fn append_internal_for_delivery(
        &mut self,
        record: ProducerRecord,
        now: Instant,
        metadata_capacity: usize,
        compression_sizing: CompressionSizing,
    ) -> Result<(Option<SendFuture>, AppendStatus)> {
        let record = record_with_append_timestamp(record);
        let key = TopicPartition {
            topic: Arc::<str>::clone(&record.topic),
            partition: record.partition,
        };
        let batch_size = self.config.batch_size.max(1);
        let available = self
            .config
            .buffer_memory
            .saturating_sub(self.buffered_bytes);
        // Single hash lookup: take the (mutable) partition queue up front and
        // compute the append target from it, instead of an immutable get()
        // followed by a separate entry() — both hash the topic Arc<str> on every
        // append. An empty queue plans the same target as a missing one.
        let queue = self
            .partitions
            .entry(key)
            .or_insert_with(|| PartitionQueue {
                batches: VecDeque::new(),
            });
        let target = planned_append_target(Some(&*queue), &record, batch_size, compression_sizing);
        if target.reserved_buffer_bytes > available {
            return Err(ProducerError::Backpressure);
        }
        let next_identity = &mut self.next_batch_id;
        if let Some(identity) = apply_append_target(queue, now, batch_size, target, next_identity) {
            let _inserted = self.buffered_batch_identities.insert(identity);
            let _inserted = self.incomplete_batch_identities.insert(identity);
        }
        let Some(batch) = queue.batches.back_mut() else {
            return Err(ProducerError::Backpressure);
        };
        batch.compression_sizing = compression_sizing;
        batch.batch_bytes = batch.batch_bytes.saturating_add(target.record_batch_bytes);
        let delivery = if let Some(sender) = &mut batch.delivery {
            Some(sender.delivery_for_record(&record))
        } else {
            let (sender, delivery) =
                SendFuture::channel_for_record_with_metadata_capacity(&record, metadata_capacity);
            batch.delivery = Some(sender);
            Some(delivery)
        };
        let current_batch_records = batch.records.len().saturating_add(1);
        let current_batch_ready =
            estimated_batch_bytes_for_sizing(batch.batch_bytes, compression_sizing) >= batch_size;
        batch.records.push(record);
        self.buffered_bytes = self
            .buffered_bytes
            .saturating_add(target.reserved_buffer_bytes);
        Ok((
            delivery,
            append_status(
                target.sealed_previous_records,
                current_batch_ready,
                current_batch_records,
                target.starts_new_batch,
            ),
        ))
    }

    fn append_buffer_reservation_with_compression_ratio(
        &self,
        record: &ProducerRecord,
        compression_ratio: f32,
    ) -> usize {
        let key = TopicPartition {
            topic: Arc::<str>::clone(&record.topic),
            partition: record.partition,
        };
        let batch_size = self.config.batch_size.max(1);
        planned_append_target(
            self.partitions.get(&key),
            record,
            batch_size,
            CompressionSizing::new(compression_ratio),
        )
        .reserved_buffer_bytes
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
                    let bytes = ready_batch_bytes(&batch);
                    let _removed = self.buffered_batch_identities.remove(&batch.identity);
                    self.buffered_bytes = self.buffered_bytes.saturating_sub(batch.buffer_bytes);
                    ready.push(ReadyBatch {
                        identity: batch.identity,
                        topic: key.topic.to_string(),
                        partition: key.partition,
                        records: batch.records,
                        delivery: batch.delivery,
                        bytes,
                        pooled_buffer_bytes: batch.buffer_bytes,
                        first_append_at: batch.first_append_at,
                        producer_state: batch.producer_state,
                    });
                }
            }
        }
        ready
    }

    /// Return the next time any buffered batch should be considered ready.
    pub fn next_ready_at(&self, now: Instant) -> Option<Instant> {
        let batch_size = self.config.batch_size;
        let linger = self.config.linger;
        self.partitions
            .values()
            .filter_map(|queue| queue.batches.front())
            .map(|batch| batch_next_ready_at(batch, now, batch_size, linger))
            .min()
    }

    /// Drain every buffered topic-partition batch regardless of size or linger.
    pub fn drain_all(&mut self) -> Vec<ReadyBatch> {
        let partitions = std::mem::take(&mut self.partitions);
        let mut batches = Vec::with_capacity(partitions.len());
        for (key, queue) in partitions {
            for batch in queue.batches {
                let bytes = ready_batch_bytes(&batch);
                batches.push(ReadyBatch {
                    identity: batch.identity,
                    topic: key.topic.to_string(),
                    partition: key.partition,
                    records: batch.records,
                    delivery: batch.delivery,
                    bytes,
                    pooled_buffer_bytes: batch.buffer_bytes,
                    first_append_at: batch.first_append_at,
                    producer_state: batch.producer_state,
                });
            }
        }
        self.buffered_batch_identities.clear();
        self.buffered_bytes = 0;
        batches
    }

    /// Drain and complete every buffered batch for abort/force-close paths.
    pub(crate) fn discard_all(&mut self) -> Vec<ReadyBatch> {
        let batches = self.drain_all();
        let identities = batches.iter().map(|batch| batch.identity);
        let _completed = self.complete_batch_identities(identities);
        batches
    }

    /// Return drained batches to the accumulator without re-estimating record sizes.
    pub fn requeue_front(&mut self, batches: Vec<ReadyBatch>) -> Result<()> {
        self.validate_requeue_identities(&batches)?;
        let split_parents: Vec<_> = batches
            .iter()
            .filter_map(|batch| batch.identity.split_parent())
            .filter(|parent| self.incomplete_batch_identities.contains(parent))
            .collect();
        for batch in batches.into_iter().rev() {
            let identity = batch.identity;
            let pooled_buffer_bytes = batch.pooled_buffer_bytes;
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
            let queued_batch = PartitionBatch {
                identity: batch.identity,
                records: batch.records,
                delivery: batch.delivery,
                producer_state: batch.producer_state,
                buffer_bytes: pooled_buffer_bytes,
                batch_bytes: batch.bytes,
                compression_sizing: CompressionSizing::NONE,
                sealed: true,
                first_append_at: batch.first_append_at,
            };
            insert_requeued_batch(&mut entry.batches, queued_batch);
            let _inserted = self.buffered_batch_identities.insert(identity);
            let _inserted = self.incomplete_batch_identities.insert(identity);
            self.buffered_bytes = self.buffered_bytes.saturating_add(pooled_buffer_bytes);
        }
        let _completed = self.complete_batch_identities(split_parents);
        Ok(())
    }

    pub(crate) fn complete_batch_identities<I>(&mut self, identities: I) -> usize
    where
        I: IntoIterator<Item = ReadyBatchIdentity>,
    {
        let mut completed = 0usize;
        for identity in identities {
            if self.incomplete_batch_identities.remove(&identity) {
                completed = completed.saturating_add(1);
            }
        }
        completed
    }

    #[cfg(test)]
    pub(crate) fn split_and_requeue_front(&mut self, batch: ReadyBatch) -> usize {
        let Some(split) = batch.split_for_retry_with_compression_ratio(self.config.batch_size, 1.0)
        else {
            return 0;
        };
        let count = split.len();
        self.requeue_front(split)
            .expect("split retry batches should have unique identities");
        count
    }

    fn validate_requeue_identities(&self, batches: &[ReadyBatch]) -> Result<()> {
        let mut seen = AHashSet::with_capacity(batches.len());
        for batch in batches {
            if self.buffered_batch_identities.contains(&batch.identity)
                || !seen.insert(batch.identity)
                || !self.can_requeue_identity(batch.identity)
            {
                return Err(ProducerError::BatchLifecycle(
                    "duplicate ready batch identity requeued",
                ));
            }
        }
        Ok(())
    }

    fn can_requeue_identity(&self, identity: ReadyBatchIdentity) -> bool {
        #[cfg(test)]
        if matches!(identity, ReadyBatchIdentity::Test(_)) {
            return true;
        }
        self.incomplete_batch_identities.contains(&identity)
            || identity
                .split_parent()
                .is_some_and(|parent| self.incomplete_batch_identities.contains(&parent))
    }
}

/// Thread-safe wrapper around [`RecordAccumulator`].
///
/// The inner accumulator keeps its single-threaded logic unchanged; this wrapper
/// guards it with a short `std::sync::Mutex` held only across synchronous
/// accumulator operations. That lets concurrent `send(&self)` appends use the
/// accumulator directly without going through the producer's async sender mutex
/// (which serialized every append). All accumulator methods are synchronous, so
/// the guard is never held across an `.await`.
#[derive(Debug)]
pub struct SharedAccumulator {
    inner: std::sync::Mutex<RecordAccumulator>,
}

impl SharedAccumulator {
    pub(crate) const fn new(accumulator: RecordAccumulator) -> Self {
        Self {
            inner: std::sync::Mutex::new(accumulator),
        }
    }

    /// Build a shared accumulator from a config.
    pub fn with_config(config: AccumulatorConfig) -> Self {
        Self::new(RecordAccumulator::new(config))
    }

    /// Append a record (delegates to the inner accumulator under the lock).
    pub fn append(&self, record: ProducerRecord) -> Result<()> {
        self.lock().append(record)
    }

    /// Append a record at a supplied timestamp.
    pub fn append_at(&self, record: ProducerRecord, now: Instant) -> Result<()> {
        self.lock().append_at(record, now)
    }

    /// Append a record and return its delivery future.
    pub fn append_for_delivery(&self, record: ProducerRecord) -> Result<SendFuture> {
        self.lock().append_for_delivery(record)
    }

    #[cfg(test)]
    pub(crate) fn append_with_status_at(
        &self,
        record: ProducerRecord,
        now: Instant,
    ) -> Result<AppendStatus> {
        self.lock().append_with_status_at(record, now)
    }

    /// Lock the accumulator for a short synchronous critical section. Never hold
    /// the returned guard across an `.await`.
    pub(crate) fn lock(&self) -> std::sync::MutexGuard<'_, RecordAccumulator> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    // --- Delegates: each acquires the short lock and forwards to the inner
    // accumulator, so callers keep their existing call sites (only the parameter
    // type changes from `&mut RecordAccumulator` to `&SharedAccumulator`). ---

    pub(crate) fn append_for_delivery_with_status_at_compression_ratio(
        &self,
        record: ProducerRecord,
        now: Instant,
        compression_ratio: f32,
    ) -> Result<(SendFuture, AppendStatus)> {
        self.lock()
            .append_for_delivery_with_status_at_compression_ratio(record, now, compression_ratio)
    }

    #[cfg(test)]
    pub(crate) fn append_with_status_at_compression_ratio(
        &self,
        record: ProducerRecord,
        now: Instant,
        compression_ratio: f32,
    ) -> Result<AppendStatus> {
        self.lock()
            .append_with_status_at_compression_ratio(record, now, compression_ratio)
    }

    pub(crate) fn buffer_memory(&self) -> usize {
        self.lock().buffer_memory()
    }

    pub(crate) fn partition_queue_load_with_availability<F>(
        &self,
        topic_metadata: &TopicMetadata,
        is_partition_available: F,
    ) -> Option<PartitionQueueLoad>
    where
        F: FnMut(&PartitionMetadata) -> bool,
    {
        self.lock()
            .partition_queue_load_with_availability(topic_metadata, is_partition_available)
    }

    pub(crate) fn buffered_batches(&self) -> usize {
        self.lock().buffered_batches()
    }

    /// Bytes currently buffered across all partitions.
    pub fn buffered_bytes(&self) -> usize {
        self.lock().buffered_bytes()
    }

    pub(crate) fn buffered_records(&self) -> usize {
        self.lock().buffered_records()
    }

    pub(crate) fn complete_batch_identities<I>(&self, identities: I) -> usize
    where
        I: IntoIterator<Item = ReadyBatchIdentity>,
    {
        self.lock().complete_batch_identities(identities)
    }

    pub(crate) fn discard_all(&self) -> Vec<ReadyBatch> {
        self.lock().discard_all()
    }

    pub(crate) fn drain_all(&self) -> Vec<ReadyBatch> {
        self.lock().drain_all()
    }

    /// Drain all batches that are ready to dispatch.
    pub fn drain_ready(&self, now: Instant) -> Vec<ReadyBatch> {
        self.lock().drain_ready(now)
    }

    pub(crate) fn has_available_memory_for_reserved_with_compression_ratio(
        &self,
        record: &ProducerRecord,
        reserved_bytes: usize,
        compression_ratio: f32,
    ) -> bool {
        self.lock()
            .has_available_memory_for_reserved_with_compression_ratio(
                record,
                reserved_bytes,
                compression_ratio,
            )
    }

    pub(crate) fn next_ready_at(&self, now: Instant) -> Option<Instant> {
        self.lock().next_ready_at(now)
    }

    pub(crate) fn requeue_front(&self, batches: Vec<ReadyBatch>) -> Result<()> {
        self.lock().requeue_front(batches)
    }
}

fn split_producer_state(
    producer_state: Option<ProducerBatchState>,
    record_index: usize,
) -> Option<ProducerBatchState> {
    let mut state = producer_state?;
    let offset = i32::try_from(record_index).unwrap_or(i32::MAX);
    state.base_sequence = state.base_sequence.checked_add(offset).unwrap_or(i32::MAX);
    Some(state)
}

fn insert_requeued_batch(queue: &mut VecDeque<PartitionBatch>, batch: PartitionBatch) {
    let Some(producer_state) = batch.producer_state else {
        queue.push_front(batch);
        return;
    };
    let insert_at = queue
        .iter()
        .position(|existing| {
            existing.producer_state.is_none_or(|existing_state| {
                existing_state.base_sequence >= producer_state.base_sequence
            })
        })
        .unwrap_or(queue.len());
    queue.insert(insert_at, batch);
}

fn ready_batch_bytes(batch: &PartitionBatch) -> usize {
    estimated_batch_bytes_for_sizing(batch.batch_bytes, batch.compression_sizing)
}

fn batch_is_ready(
    batch: &PartitionBatch,
    now: Instant,
    batch_size: usize,
    linger: Duration,
) -> bool {
    batch.sealed
        || estimated_batch_bytes_for_sizing(batch.batch_bytes, batch.compression_sizing)
            >= batch_size
        || now.duration_since(batch.first_append_at) >= linger
}

fn batch_next_ready_at(
    batch: &PartitionBatch,
    now: Instant,
    batch_size: usize,
    linger: Duration,
) -> Instant {
    if batch.sealed
        || estimated_batch_bytes_for_sizing(batch.batch_bytes, batch.compression_sizing)
            >= batch_size
    {
        return now;
    }
    let Some(deadline) = batch.first_append_at.checked_add(linger) else {
        return now;
    };
    if deadline <= now { now } else { deadline }
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
    estimate_first_record_batch_bytes(record)
}

const fn append_status(
    sealed_previous_records: usize,
    current_batch_ready: bool,
    current_batch_records: usize,
    starts_new_batch: bool,
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
        starts_new_batch,
    }
}

fn record_with_append_timestamp(mut record: ProducerRecord) -> ProducerRecord {
    if record.timestamp_ms.is_none() {
        record.timestamp_ms = Some(current_time_ms());
    }
    record
}

fn planned_append_target(
    queue: Option<&PartitionQueue>,
    record: &ProducerRecord,
    batch_size: usize,
    compression_sizing: CompressionSizing,
) -> AppendTarget {
    queue.and_then(|queue| queue.batches.back()).map_or_else(
        || new_batch_append_target(0, record, batch_size),
        |batch| {
            if batch.sealed {
                return new_batch_append_target(batch.records.len(), record, batch_size);
            }
            let next_record_bytes = estimate_next_record_batch_bytes(&batch.records, record);
            let estimated_current_batch_bytes =
                estimated_batch_bytes_for_sizing(batch.batch_bytes, compression_sizing);
            let cannot_fit = !batch.records.is_empty()
                && estimated_current_batch_bytes.saturating_add(next_record_bytes) > batch_size;
            if cannot_fit {
                new_batch_append_target(batch.records.len(), record, batch_size)
            } else {
                AppendTarget {
                    sealed_previous_records: 0,
                    record_batch_bytes: next_record_bytes,
                    starts_new_batch: false,
                    reserved_buffer_bytes: 0,
                }
            }
        },
    )
}

#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    reason = "Kafka compression ratio estimates are f32 and only influence conservative sizing."
)]
fn estimated_batch_bytes_for_sizing(
    uncompressed_batch_bytes: usize,
    compression_sizing: CompressionSizing,
) -> usize {
    if !compression_sizing.uses_estimate()
        || uncompressed_batch_bytes <= RECORD_BATCH_OVERHEAD_BYTES
    {
        return uncompressed_batch_bytes;
    }
    let uncompressed_records_bytes =
        uncompressed_batch_bytes.saturating_sub(RECORD_BATCH_OVERHEAD_BYTES);
    RECORD_BATCH_OVERHEAD_BYTES.saturating_add(
        ((uncompressed_records_bytes as f32)
            * compression_sizing.ratio
            * COMPRESSION_RATE_ESTIMATION_FACTOR)
            .ceil() as usize,
    )
}

fn new_batch_append_target(
    sealed_previous_records: usize,
    record: &ProducerRecord,
    batch_size: usize,
) -> AppendTarget {
    let record_batch_bytes = estimate_first_record_batch_bytes(record);
    AppendTarget {
        sealed_previous_records,
        record_batch_bytes,
        starts_new_batch: true,
        reserved_buffer_bytes: batch_buffer_reservation(batch_size, record_batch_bytes),
    }
}

fn apply_append_target(
    queue: &mut PartitionQueue,
    now: Instant,
    batch_size: usize,
    target: AppendTarget,
    next_identity: &mut u64,
) -> Option<ReadyBatchIdentity> {
    if !target.starts_new_batch {
        return None;
    }
    if target.sealed_previous_records > 0
        && let Some(batch) = queue.batches.back_mut()
    {
        batch.sealed = true;
    }
    let identity = allocate_ready_batch_identity(next_identity);
    queue.batches.push_back(new_partition_batch(
        identity,
        now,
        batch_size,
        target.record_batch_bytes,
        target.reserved_buffer_bytes,
    ));
    Some(identity)
}

const fn batch_buffer_reservation(batch_size: usize, first_record_bytes: usize) -> usize {
    if first_record_bytes > batch_size {
        first_record_bytes
    } else {
        batch_size
    }
}

fn new_partition_batch(
    identity: ReadyBatchIdentity,
    now: Instant,
    batch_size: usize,
    first_record_bytes: usize,
    buffer_bytes: usize,
) -> PartitionBatch {
    PartitionBatch {
        identity,
        records: Vec::with_capacity(estimated_batch_record_capacity(
            batch_size,
            first_record_bytes,
        )),
        delivery: None,
        producer_state: None,
        buffer_bytes,
        batch_bytes: RECORD_BATCH_OVERHEAD_BYTES,
        compression_sizing: CompressionSizing::NONE,
        sealed: false,
        first_append_at: now,
    }
}

const fn allocate_ready_batch_identity(next_identity: &mut u64) -> ReadyBatchIdentity {
    let identity = ReadyBatchIdentity::Accumulator(*next_identity);
    *next_identity = next_identity.saturating_add(1);
    identity
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
fn split_records_by_batch_target(
    records: Vec<ProducerRecord>,
    target_batch_bytes: usize,
    compression_ratio: f32,
) -> Vec<SplitRecordGroup> {
    let target_batch_bytes = target_batch_bytes.max(1);
    let compression_ratio = compression_ratio.max(1.0);
    let mut groups = Vec::new();
    let mut current = SplitRecordGroup {
        first_record_index: 0,
        records: Vec::new(),
        bytes: 0,
    };
    let mut current_batch_bytes = RECORD_BATCH_OVERHEAD_BYTES;

    for (record_index, record) in records.into_iter().enumerate() {
        let record_batch_bytes = estimate_next_record_batch_bytes(&current.records, &record);
        let adjusted_record_batch_bytes =
            apply_compression_ratio_estimate(record_batch_bytes, compression_ratio);
        if !current.records.is_empty()
            && current_batch_bytes.saturating_add(adjusted_record_batch_bytes) > target_batch_bytes
        {
            groups.push(current);
            current = SplitRecordGroup {
                first_record_index: record_index,
                records: Vec::new(),
                bytes: 0,
            };
            current_batch_bytes = RECORD_BATCH_OVERHEAD_BYTES;
        }

        let record_batch_bytes = estimate_next_record_batch_bytes(&current.records, &record);
        let adjusted_record_batch_bytes =
            apply_compression_ratio_estimate(record_batch_bytes, compression_ratio);
        current_batch_bytes = current_batch_bytes.saturating_add(adjusted_record_batch_bytes);
        current.bytes = current.bytes.saturating_add(estimate_record_bytes(&record));
        current.records.push(record);
    }

    if !current.records.is_empty() {
        groups.push(current);
    }
    groups
}

#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    reason = "Kafka compression ratio estimates are f32; split grouping only needs conservative \
              byte estimates."
)]
fn apply_compression_ratio_estimate(bytes: usize, compression_ratio: f32) -> usize {
    ((bytes as f32) * compression_ratio).ceil() as usize
}

#[cfg(test)]
fn estimate_ready_batch_encoded_bytes(records: &[ProducerRecord]) -> usize {
    let first_timestamp_ms = records.first().and_then(|record| record.timestamp_ms);
    records
        .iter()
        .enumerate()
        .fold(RECORD_BATCH_OVERHEAD_BYTES, |bytes, (offset, record)| {
            bytes.saturating_add(estimate_record_batch_bytes_at_offset(
                record,
                offset,
                first_timestamp_ms,
            ))
        })
}

fn estimate_first_record_batch_bytes(record: &ProducerRecord) -> usize {
    estimate_record_batch_bytes_at_offset(record, 0, record.timestamp_ms)
}

fn estimate_next_record_batch_bytes(
    current_records: &[ProducerRecord],
    record: &ProducerRecord,
) -> usize {
    let first_timestamp_ms = current_records
        .first()
        .and_then(|record| record.timestamp_ms)
        .or(record.timestamp_ms);
    estimate_record_batch_bytes_at_offset(record, current_records.len(), first_timestamp_ms)
}

fn estimate_record_batch_bytes_at_offset(
    record: &ProducerRecord,
    offset_delta: usize,
    first_timestamp_ms: Option<i64>,
) -> usize {
    let key_bytes = record.key.as_ref().map_or(0, bytes::Bytes::len);
    let value_bytes = record.value.as_ref().map_or(0, bytes::Bytes::len);
    let offset_delta = i32::try_from(offset_delta).unwrap_or(i32::MAX);
    let timestamp_delta = record.timestamp_ms.map_or(0, |timestamp| {
        first_timestamp_ms.map_or(timestamp, |first_timestamp| {
            timestamp.saturating_sub(first_timestamp)
        })
    });
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

    use std::time::{Duration, Instant};

    use bytes::Bytes;

    use super::{
        AccumulatorConfig, CompressionSizing, ProducerError, RECORD_BATCH_OVERHEAD_BYTES,
        ReadyBatch, ReadyBatchIdentity, RecordAccumulator, estimate_ready_batch_encoded_bytes,
        estimate_record_batch_bytes, estimated_batch_bytes_for_sizing,
    };
    use crate::producer::{
        ProducerCompression, ProducerIdentity, ProducerRecord, transaction::ProducerBatchState,
    };

    const TEST_LARGE_BATCH_SIZE: usize = 16 * 1024;
    const TEST_PRODUCER_IDENTITY: ProducerIdentity = ProducerIdentity {
        producer_id: 42,
        producer_epoch: 3,
    };

    fn ready_batch_for_requeue(
        topic: &'static str,
        partition: i32,
        value: &'static [u8],
        identity: u64,
        now: Instant,
    ) -> ReadyBatch {
        let record = ProducerRecord::new(topic, partition).value(Bytes::from_static(value));
        let bytes = estimate_ready_batch_encoded_bytes(std::slice::from_ref(&record));
        ReadyBatch {
            identity: ReadyBatchIdentity::test(identity),
            topic: topic.to_owned(),
            partition,
            records: vec![record],
            delivery: None,
            bytes,
            pooled_buffer_bytes: 128,
            first_append_at: now,
            producer_state: None,
        }
    }

    trait ReadyBatchTestExt {
        fn with_producer_base_sequence(self, base_sequence: i32) -> ReadyBatch;
    }

    impl ReadyBatchTestExt for ReadyBatch {
        fn with_producer_base_sequence(mut self, base_sequence: i32) -> ReadyBatch {
            self.producer_state = Some(ProducerBatchState {
                identity: TEST_PRODUCER_IDENTITY,
                base_sequence,
            });
            self
        }
    }

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
    fn append_reserves_one_batch_buffer_per_topic_partition_like_java() {
        let now = Instant::now();
        let mut accumulator = RecordAccumulator::new(
            AccumulatorConfig::default()
                .batch_size(128)
                .linger(Duration::from_secs(1))
                .buffer_memory(128),
        );

        accumulator
            .append_at(
                ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")),
                now,
            )
            .expect("first record reserves the batch buffer");
        assert_eq!(accumulator.buffered_bytes(), 128);

        accumulator
            .append_at(
                ProducerRecord::new("orders", 0).value(Bytes::from_static(b"b")),
                now,
            )
            .expect("same open batch should not reserve another buffer");
        assert_eq!(accumulator.buffered_bytes(), 128);

        let error = accumulator
            .append_at(
                ProducerRecord::new("orders", 1).value(Bytes::from_static(b"c")),
                now,
            )
            .expect_err("different partition needs another batch buffer");
        assert!(matches!(error, ProducerError::Backpressure));

        let drained = accumulator.drain_all();
        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0].pooled_buffer_bytes(), 128);
        assert_eq!(
            drained[0].bytes,
            estimate_ready_batch_encoded_bytes(&drained[0].records)
        );
        assert_eq!(accumulator.buffered_bytes(), 0);
    }

    #[test]
    fn drain_reports_encoded_batch_bytes_separately_from_pooled_buffer() {
        let now = Instant::now();
        let mut accumulator = RecordAccumulator::new(
            AccumulatorConfig::default()
                .batch_size(256)
                .linger(Duration::from_secs(1))
                .buffer_memory(256),
        );
        accumulator
            .append_at(
                ProducerRecord::new("orders", 0)
                    .try_timestamp_ms(1_000)
                    .expect("timestamp")
                    .header("trace-id", Bytes::from_static(b"abc"))
                    .value(Bytes::from_static(b"value")),
                now,
            )
            .expect("first record reserves the batch buffer");

        let drained = accumulator.drain_all();
        let encoded_bytes = estimate_ready_batch_encoded_bytes(&drained[0].records);

        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0].bytes, encoded_bytes);
        assert_eq!(drained[0].pooled_buffer_bytes(), 256);
        assert_ne!(drained[0].bytes, drained[0].pooled_buffer_bytes());
    }

    #[test]
    fn append_with_compression_ratio_uses_memory_records_has_room_estimate() {
        let now = Instant::now();
        let record = ProducerRecord::new("orders", 0).value(Bytes::from(vec![b'x'; 128]));
        let record_bytes = estimate_record_batch_bytes(&record);
        let batch_size = RECORD_BATCH_OVERHEAD_BYTES
            .saturating_add(record_bytes)
            .saturating_add(record_bytes * 3 / 4);

        let mut raw = RecordAccumulator::new(
            AccumulatorConfig::default()
                .batch_size(batch_size)
                .buffer_memory(batch_size * 4)
                .linger(Duration::from_secs(1)),
        );
        let _first_raw = raw
            .append_with_status_at_compression_ratio(record.clone(), now, 1.0)
            .expect("append first raw record");
        let raw_status = raw
            .append_with_status_at_compression_ratio(record.clone(), now, 1.0)
            .expect("append second raw record");

        let mut compressed = RecordAccumulator::new(
            AccumulatorConfig::default()
                .batch_size(batch_size)
                .buffer_memory(batch_size * 4)
                .linger(Duration::from_secs(1)),
        );
        let _first_compressed = compressed
            .append_with_status_at_compression_ratio(record.clone(), now, 0.50)
            .expect("append first compressed record");
        let compressed_status = compressed
            .append_with_status_at_compression_ratio(record, now, 0.50)
            .expect("append second compressed record");

        assert!(raw_status.starts_new_batch);
        assert_eq!(raw.buffered_batches(), 2);
        assert!(!compressed_status.starts_new_batch);
        assert_eq!(compressed.buffered_batches(), 1);
    }

    #[test]
    fn drain_ready_uses_compression_ratio_has_room_estimate() {
        let now = Instant::now();
        let record = ProducerRecord::new("orders", 0).value(Bytes::from(vec![b'x'; 128]));
        let record_bytes = estimate_record_batch_bytes(&record);
        let batch_size = RECORD_BATCH_OVERHEAD_BYTES
            .saturating_add(record_bytes)
            .saturating_add(record_bytes * 3 / 4);
        let mut accumulator = RecordAccumulator::new(
            AccumulatorConfig::default()
                .batch_size(batch_size)
                .buffer_memory(batch_size * 4)
                .linger(Duration::from_secs(1)),
        );

        let _first_status = accumulator
            .append_with_status_at_compression_ratio(record.clone(), now, 0.50)
            .expect("append first compressed record");
        let status = accumulator
            .append_with_status_at_compression_ratio(record, now, 0.50)
            .expect("append second compressed record");
        let ready = accumulator.drain_ready(now);

        assert!(!status.batch_ready);
        assert!(ready.is_empty());
        assert_eq!(accumulator.buffered_batches(), 1);
    }

    #[test]
    fn drain_reports_ratio_aware_estimated_batch_bytes_like_java() {
        let now = Instant::now();
        let record = ProducerRecord::new("orders", 0).value(Bytes::from(vec![b'x'; 128]));
        let mut accumulator = RecordAccumulator::new(
            AccumulatorConfig::default()
                .batch_size(1024)
                .buffer_memory(4096)
                .linger(Duration::from_secs(1)),
        );

        let _first_status = accumulator
            .append_with_status_at_compression_ratio(record.clone(), now, 0.50)
            .expect("append first compressed record");
        let _second_status = accumulator
            .append_with_status_at_compression_ratio(record, now, 0.50)
            .expect("append second compressed record");
        let drained = accumulator.drain_all();
        let raw_bytes = estimate_ready_batch_encoded_bytes(&drained[0].records);
        let estimated_bytes =
            estimated_batch_bytes_for_sizing(raw_bytes, CompressionSizing::new(0.50));

        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0].bytes, estimated_bytes);
        assert_ne!(drained[0].bytes, raw_bytes);
    }

    #[test]
    fn requeue_front_preserves_ratio_aware_estimated_batch_bytes_like_java() {
        let now = Instant::now();
        let record = ProducerRecord::new("orders", 0).value(Bytes::from(vec![b'x'; 128]));
        let mut accumulator = RecordAccumulator::new(
            AccumulatorConfig::default()
                .batch_size(1024)
                .buffer_memory(4096)
                .linger(Duration::from_secs(1)),
        );

        let _first_status = accumulator
            .append_with_status_at_compression_ratio(record.clone(), now, 0.50)
            .expect("append first compressed record");
        let _second_status = accumulator
            .append_with_status_at_compression_ratio(record, now, 0.50)
            .expect("append second compressed record");
        let drained = accumulator.drain_all();
        let expected_bytes = drained[0].bytes;

        accumulator
            .requeue_front(drained)
            .expect("requeue should preserve batch accounting");
        let requeued = accumulator.drain_all();
        let raw_bytes = estimate_ready_batch_encoded_bytes(&requeued[0].records);

        assert_eq!(requeued.len(), 1);
        assert_eq!(requeued[0].bytes, expected_bytes);
        assert_ne!(requeued[0].bytes, raw_bytes);
    }

    #[test]
    fn requeue_front_prepends_records_and_preserves_earliest_linger_time() {
        let now = Instant::now();
        let later = now.checked_add(Duration::from_millis(5)).unwrap_or(now);
        let mut accumulator = RecordAccumulator::new(
            AccumulatorConfig::default()
                .batch_size(TEST_LARGE_BATCH_SIZE)
                .buffer_memory(TEST_LARGE_BATCH_SIZE * 4)
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

        accumulator
            .requeue_front(existing)
            .expect("requeue should preserve existing batch identity");
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
    fn append_after_requeue_starts_new_batch_instead_of_mutating_retry_batch() {
        let now = Instant::now();
        let mut accumulator = RecordAccumulator::new(
            AccumulatorConfig::default()
                .batch_size(TEST_LARGE_BATCH_SIZE)
                .buffer_memory(TEST_LARGE_BATCH_SIZE * 4)
                .linger(Duration::from_secs(1)),
        );
        accumulator
            .append_at(
                ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")),
                now,
            )
            .expect("append retry candidate");
        let retry = accumulator.drain_all();
        accumulator
            .requeue_front(retry)
            .expect("requeue should preserve retry batch identity");

        let status = accumulator
            .append_with_status_at(
                ProducerRecord::new("orders", 0).value(Bytes::from_static(b"b")),
                now,
            )
            .expect("append new record after retry batch");
        let batches = accumulator.drain_all();
        let values: Vec<_> = batches
            .iter()
            .map(|batch| {
                batch
                    .records
                    .iter()
                    .filter_map(|record| record.value.as_ref())
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .collect();

        assert!(status.starts_new_batch);
        assert_eq!(batches.len(), 2);
        assert_eq!(
            values,
            [
                vec![Bytes::from_static(b"a")],
                vec![Bytes::from_static(b"b")]
            ]
        );
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

        accumulator
            .requeue_front(drained)
            .expect("requeue should preserve distinct batch identities");
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
    fn requeue_front_inserts_idempotent_batches_by_sequence_like_java() {
        let now = Instant::now();
        let mut accumulator = RecordAccumulator::new(
            AccumulatorConfig::default()
                .batch_size(1)
                .buffer_memory(TEST_LARGE_BATCH_SIZE),
        );
        let later =
            ready_batch_for_requeue("orders", 0, b"b", 2, now).with_producer_base_sequence(1);
        let earlier =
            ready_batch_for_requeue("orders", 0, b"a", 1, now).with_producer_base_sequence(0);

        accumulator
            .requeue_front(vec![later, earlier])
            .expect("requeue should accept distinct idempotent batches");
        let batches = accumulator.drain_all();
        let sequences: Vec<_> = batches
            .iter()
            .filter_map(|batch| batch.producer_state.map(|state| state.base_sequence))
            .collect();

        assert_eq!(sequences, [0, 1]);
    }

    #[test]
    fn requeue_front_rejects_duplicate_batch_identity_without_double_counting() {
        let now = Instant::now();
        let mut accumulator = RecordAccumulator::new(
            AccumulatorConfig::default()
                .batch_size(TEST_LARGE_BATCH_SIZE)
                .buffer_memory(TEST_LARGE_BATCH_SIZE * 4)
                .linger(Duration::from_secs(1)),
        );
        accumulator
            .append_at(
                ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")),
                now,
            )
            .expect("append record");
        let batch = accumulator.drain_all().pop().expect("drained batch");
        let duplicate = ReadyBatch {
            identity: batch.identity,
            topic: batch.topic.clone(),
            partition: batch.partition,
            records: vec![ProducerRecord::new("orders", 0).value(Bytes::from_static(b"b"))],
            delivery: None,
            bytes: batch.bytes,
            pooled_buffer_bytes: batch.pooled_buffer_bytes,
            first_append_at: batch.first_append_at,
            producer_state: None,
        };

        accumulator
            .requeue_front(vec![batch])
            .expect("first requeue should succeed");
        let buffered_bytes = accumulator.buffered_bytes();
        let error = accumulator
            .requeue_front(vec![duplicate])
            .expect_err("duplicate identity should fail like incomplete-batch invariant");

        assert!(matches!(error, ProducerError::BatchLifecycle(_)));
        assert_eq!(accumulator.buffered_bytes(), buffered_bytes);
        assert_eq!(accumulator.buffered_batches(), 1);
    }

    #[test]
    fn requeue_front_rejects_completed_batch_identity_without_double_counting() {
        let now = Instant::now();
        let mut accumulator = RecordAccumulator::new(
            AccumulatorConfig::default()
                .batch_size(TEST_LARGE_BATCH_SIZE)
                .buffer_memory(TEST_LARGE_BATCH_SIZE * 4)
                .linger(Duration::from_secs(1)),
        );
        accumulator
            .append_at(
                ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")),
                now,
            )
            .expect("append record");
        let batch = accumulator.drain_all().pop().expect("drained batch");
        let identity = batch.identity;
        assert_eq!(accumulator.complete_batch_identities([identity]), 1);
        let buffered_bytes = accumulator.buffered_bytes();
        let error = accumulator
            .requeue_front(vec![batch])
            .expect_err("completed batch identity should not be requeued");

        assert!(matches!(error, ProducerError::BatchLifecycle(_)));
        assert_eq!(accumulator.buffered_bytes(), buffered_bytes);
        assert_eq!(accumulator.buffered_batches(), 0);
    }

    #[test]
    fn complete_batch_identities_reports_actual_completed_count() {
        let now = Instant::now();
        let mut accumulator = RecordAccumulator::new(
            AccumulatorConfig::default()
                .batch_size(TEST_LARGE_BATCH_SIZE)
                .buffer_memory(TEST_LARGE_BATCH_SIZE * 4)
                .linger(Duration::from_secs(1)),
        );
        accumulator
            .append_at(
                ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")),
                now,
            )
            .expect("append record");
        let batch = accumulator.drain_all().pop().expect("drained batch");
        let identity = batch.identity;

        let completed = accumulator.complete_batch_identities([identity]);
        let duplicate_completed = accumulator.complete_batch_identities([identity]);
        let stale_completed = accumulator.complete_batch_identities([ReadyBatchIdentity::test(99)]);

        assert_eq!(completed, 1);
        assert_eq!(duplicate_completed, 0);
        assert_eq!(stale_completed, 0);
    }

    #[test]
    fn discard_all_completes_batch_identities_without_double_counting() {
        let now = Instant::now();
        let mut accumulator = RecordAccumulator::new(
            AccumulatorConfig::default()
                .batch_size(TEST_LARGE_BATCH_SIZE)
                .buffer_memory(TEST_LARGE_BATCH_SIZE * 4)
                .linger(Duration::from_secs(1)),
        );
        accumulator
            .append_at(
                ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")),
                now,
            )
            .expect("append record");

        let batch = accumulator.discard_all().pop().expect("discarded batch");
        let buffered_bytes = accumulator.buffered_bytes();
        let error = accumulator
            .requeue_front(vec![batch])
            .expect_err("discarded batch identity should be completed");

        assert!(matches!(error, ProducerError::BatchLifecycle(_)));
        assert_eq!(accumulator.buffered_bytes(), buffered_bytes);
        assert_eq!(accumulator.buffered_batches(), 0);
    }

    #[test]
    fn split_and_requeue_front_rebuilds_multi_record_batch_for_retry() {
        let now = Instant::now();
        let mut accumulator = RecordAccumulator::new(
            AccumulatorConfig::default()
                .batch_size(TEST_LARGE_BATCH_SIZE)
                .buffer_memory(TEST_LARGE_BATCH_SIZE * 4)
                .linger(Duration::from_secs(1)),
        );
        accumulator
            .append_at(
                ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")),
                now,
            )
            .expect("append first record");
        accumulator
            .append_at(
                ProducerRecord::new("orders", 0).value(Bytes::from_static(b"b")),
                now,
            )
            .expect("append second record");
        let batch = accumulator
            .drain_all()
            .pop()
            .expect("drained oversized batch");

        let split_count = accumulator.split_and_requeue_front(batch);
        let split = accumulator.drain_all();
        let values: Vec<_> = split
            .iter()
            .flat_map(|batch| batch.records.iter())
            .filter_map(|record| record.value.as_ref())
            .cloned()
            .collect();

        assert_eq!(split_count, 1);
        assert_eq!(split.len(), 1);
        assert_eq!(split[0].records.len(), 2);
        assert_eq!(values, [Bytes::from_static(b"a"), Bytes::from_static(b"b")]);
    }

    #[test]
    fn split_and_requeue_front_deallocates_parent_pooled_buffer_like_java() {
        let now = Instant::now();
        let mut accumulator = RecordAccumulator::new(
            AccumulatorConfig::default()
                .batch_size(TEST_LARGE_BATCH_SIZE)
                .buffer_memory(TEST_LARGE_BATCH_SIZE * 4)
                .linger(Duration::from_secs(1)),
        );
        for value in [b"a".as_slice(), b"bb".as_slice(), b"ccc".as_slice()] {
            accumulator
                .append_at(
                    ProducerRecord::new("orders", 0).value(Bytes::copy_from_slice(value)),
                    now,
                )
                .expect("append record");
        }
        let batch = accumulator
            .drain_all()
            .pop()
            .expect("drained oversized batch");

        let split_count = accumulator.split_and_requeue_front(batch);

        assert_eq!(split_count, 1);
        assert_eq!(accumulator.buffered_bytes(), 0);
        assert_eq!(accumulator.buffered_batches(), 1);
    }

    #[test]
    fn ready_batch_encoded_byte_estimate_matches_encoder_for_timestamped_records() {
        let records = vec![
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
        ];

        let estimated = estimate_ready_batch_encoded_bytes(&records);
        let encoded = super::super::batch::encode_record_batch_with_producer_state_at_offset(
            &records,
            ProducerCompression::default(),
            None,
            0,
        )
        .expect("batch should encode");

        assert_eq!(estimated, encoded.len());
    }

    #[test]
    fn ready_batch_split_for_retry_groups_records_by_target_batch_size() {
        let now = Instant::now();
        let mut accumulator = RecordAccumulator::new(
            AccumulatorConfig::default()
                .batch_size(TEST_LARGE_BATCH_SIZE)
                .buffer_memory(TEST_LARGE_BATCH_SIZE * 4)
                .linger(Duration::from_secs(1)),
        );
        for value in [b"a".as_slice(), b"b".as_slice(), b"c".as_slice()] {
            accumulator
                .append_at(
                    ProducerRecord::new("orders", 0).value(Bytes::copy_from_slice(value)),
                    now,
                )
                .expect("append record");
        }
        let batch = accumulator
            .drain_all()
            .pop()
            .expect("drained oversized batch");

        let split = batch
            .split_for_retry_with_compression_ratio(78, 1.0)
            .expect("multi-record batch should split for retry");

        assert_eq!(split.len(), 2);
        assert_eq!(split[0].records.len(), 2);
        assert_eq!(split[1].records.len(), 1);
    }

    #[test]
    fn ready_batch_split_for_retry_applies_compression_ratio_to_target() {
        let now = Instant::now();
        let mut accumulator = RecordAccumulator::new(
            AccumulatorConfig::default()
                .batch_size(TEST_LARGE_BATCH_SIZE)
                .buffer_memory(TEST_LARGE_BATCH_SIZE * 4)
                .linger(Duration::from_secs(1)),
        );
        for value in [b"a".as_slice(), b"b".as_slice(), b"c".as_slice()] {
            accumulator
                .append_at(
                    ProducerRecord::new("orders", 0).value(Bytes::copy_from_slice(value)),
                    now,
                )
                .expect("append record");
        }
        let batch = accumulator
            .drain_all()
            .pop()
            .expect("drained oversized batch");

        let split = batch
            .split_for_retry_with_compression_ratio(78, 2.0)
            .expect("multi-record batch should split for retry");

        assert_eq!(split.len(), 3);
        assert!(split.iter().all(|batch| batch.records.len() == 1));
    }

    #[test]
    fn ready_batch_split_for_retry_assigns_distinct_child_identities() {
        let now = Instant::now();
        let mut accumulator = RecordAccumulator::new(
            AccumulatorConfig::default()
                .batch_size(TEST_LARGE_BATCH_SIZE)
                .buffer_memory(TEST_LARGE_BATCH_SIZE * 4)
                .linger(Duration::from_secs(1)),
        );
        for value in [b"a".as_slice(), b"b".as_slice(), b"c".as_slice()] {
            accumulator
                .append_at(
                    ProducerRecord::new("orders", 0).value(Bytes::copy_from_slice(value)),
                    now,
                )
                .expect("append record");
        }
        let batch = accumulator
            .drain_all()
            .pop()
            .expect("drained oversized batch");
        let parent_identity = batch.identity();

        let split = batch
            .split_for_retry_with_compression_ratio(78, 2.0)
            .expect("multi-record batch should split for retry");

        assert_eq!(split.len(), 3);
        assert!(
            split
                .iter()
                .all(|batch| batch.identity() != parent_identity)
        );
        assert_ne!(split[0].identity(), split[1].identity());
        assert_ne!(split[1].identity(), split[2].identity());
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

    #[test]
    fn append_status_reports_new_batch_for_linger_wakeup_policy() {
        let now = Instant::now();
        let mut accumulator = RecordAccumulator::new(
            AccumulatorConfig::default()
                .batch_size(128)
                .linger(Duration::from_secs(1)),
        );

        let first = accumulator
            .append_with_status_at(
                ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")),
                now,
            )
            .expect("append first record");
        let second = accumulator
            .append_with_status_at(
                ProducerRecord::new("orders", 0).value(Bytes::from_static(b"b")),
                now,
            )
            .expect("append second record");

        assert!(first.starts_new_batch);
        assert!(!second.starts_new_batch);
    }

    #[test]
    fn next_ready_at_reports_earliest_linger_deadline() {
        let base = Instant::now();
        let later_append = base
            .checked_add(Duration::from_millis(5))
            .expect("later append instant");
        let first_deadline = base
            .checked_add(Duration::from_millis(10))
            .expect("first linger deadline");
        let before_deadline = base
            .checked_add(Duration::from_millis(6))
            .expect("before linger deadline");
        let after_deadline = base
            .checked_add(Duration::from_millis(11))
            .expect("after linger deadline");
        let mut accumulator = RecordAccumulator::new(
            AccumulatorConfig::default()
                .batch_size(TEST_LARGE_BATCH_SIZE)
                .buffer_memory(TEST_LARGE_BATCH_SIZE * 4)
                .linger(Duration::from_millis(10)),
        );
        accumulator
            .append_at(
                ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")),
                base,
            )
            .expect("append first partition");
        accumulator
            .append_at(
                ProducerRecord::new("orders", 1).value(Bytes::from_static(b"b")),
                later_append,
            )
            .expect("append second partition");

        assert_eq!(
            accumulator.next_ready_at(before_deadline),
            Some(first_deadline)
        );
        assert_eq!(
            accumulator.next_ready_at(after_deadline),
            Some(after_deadline)
        );
    }
}
