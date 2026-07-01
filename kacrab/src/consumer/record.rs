//! Records returned to the caller by [`Consumer::poll`](super::Consumer::poll).

use std::collections::BTreeMap;

use bytes::Bytes;
pub use kacrab_protocol::record::RecordHeader;

use crate::common::TopicPartition;

/// How a record's timestamp was assigned, mirroring Kafka's `TimestampType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimestampType {
    /// The producer set the timestamp (`CreateTime`).
    CreateTime,
    /// The broker set the timestamp on append (`LogAppendTime`).
    LogAppendTime,
}

/// A single consumed record, the analogue of Kafka's `ConsumerRecord`.
///
/// Keys and values are raw bytes (`Option<Bytes>`); typed access rides a
/// deserializer, mirroring the producer's bytes-first `ProducerRecord`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerRecord {
    /// Topic the record came from.
    pub topic: String,
    /// Partition the record came from.
    pub partition: i32,
    /// Absolute offset of the record within its partition.
    pub offset: i64,
    /// Record timestamp.
    pub timestamp: i64,
    /// How `timestamp` was assigned.
    pub timestamp_type: TimestampType,
    /// Record key (`None` for a null key).
    pub key: Option<Bytes>,
    /// Record value (`None` for a tombstone / null value).
    pub value: Option<Bytes>,
    /// Record headers.
    pub headers: Vec<RecordHeader>,
    /// Leader epoch of the record's batch, when known (KIP-101/KIP-320).
    pub leader_epoch: Option<i32>,
}

impl ConsumerRecord {
    /// The record's topic and partition as a [`TopicPartition`] key.
    #[must_use]
    pub fn topic_partition(&self) -> TopicPartition {
        TopicPartition::new(self.topic.clone(), self.partition)
    }
}

/// The batch of records returned by one [`Consumer::poll`](super::Consumer::poll)
/// call, grouped by partition and iterable in partition order — the analogue of
/// Kafka's `ConsumerRecords`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ConsumerRecords {
    by_partition: BTreeMap<(String, i32), Vec<ConsumerRecord>>,
    count: usize,
}

impl ConsumerRecords {
    /// Build an empty batch.
    #[must_use]
    pub fn empty() -> Self {
        Self::default()
    }

    /// Append records for one partition (kept in the given order).
    pub(crate) fn push_partition(
        &mut self,
        topic: String,
        partition: i32,
        records: Vec<ConsumerRecord>,
    ) {
        if records.is_empty() {
            return;
        }
        self.count = self.count.saturating_add(records.len());
        self.by_partition
            .entry((topic, partition))
            .or_default()
            .extend(records);
    }

    /// Total number of records across all partitions.
    #[must_use]
    pub const fn count(&self) -> usize {
        self.count
    }

    /// Whether this batch has no records.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// The set of partitions with at least one record in this batch.
    #[must_use]
    pub fn partitions(&self) -> Vec<TopicPartition> {
        self.by_partition
            .keys()
            .map(|(topic, partition)| TopicPartition::new(topic.clone(), *partition))
            .collect()
    }

    /// Records for a single partition, in offset order.
    #[must_use]
    pub fn records(&self, partition: &TopicPartition) -> &[ConsumerRecord] {
        self.by_partition
            .get(&(partition.topic.clone(), partition.partition))
            .map_or(&[], Vec::as_slice)
    }

    /// Iterate every record across all partitions, in partition then offset order.
    pub fn iter(&self) -> impl Iterator<Item = &ConsumerRecord> {
        self.by_partition.values().flatten()
    }
}

impl<'a> IntoIterator for &'a ConsumerRecords {
    type Item = &'a ConsumerRecord;
    type IntoIter = std::iter::Flatten<
        std::collections::btree_map::Values<'a, (String, i32), Vec<ConsumerRecord>>,
    >;

    fn into_iter(self) -> Self::IntoIter {
        self.by_partition.values().flatten()
    }
}

impl IntoIterator for ConsumerRecords {
    type Item = ConsumerRecord;
    type IntoIter = std::iter::Flatten<
        std::collections::btree_map::IntoValues<(String, i32), Vec<ConsumerRecord>>,
    >;

    fn into_iter(self) -> Self::IntoIter {
        self.by_partition.into_values().flatten()
    }
}
