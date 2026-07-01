//! Records returned to the caller by [`Consumer::poll`](super::Consumer::poll).

use std::collections::BTreeMap;

use bytes::Bytes;
pub use kacrab_protocol::record::RecordHeader;

use crate::common::TopicPartition;

/// An offset resolved for a timestamp, returned by
/// [`Consumer::offsets_for_times`](super::Consumer::offsets_for_times) — the
/// analogue of Kafka's `OffsetAndTimestamp`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OffsetAndTimestamp {
    /// The earliest offset whose record timestamp is `>=` the requested time.
    pub offset: i64,
    /// The timestamp of that record.
    pub timestamp: i64,
    /// The leader epoch of that record, when known.
    pub leader_epoch: Option<i32>,
}

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

    /// Deserialize this record's key and value with the given deserializers.
    ///
    /// # Errors
    /// Returns [`ConsumerError::Deserialization`](super::ConsumerError::Deserialization)
    /// when either the key or value bytes cannot be decoded.
    pub fn deserialized<K, V>(
        &self,
        key: &impl super::ConsumerDeserializer<K>,
        value: &impl super::ConsumerDeserializer<V>,
    ) -> super::Result<(Option<K>, Option<V>)> {
        Ok((
            key.deserialize(&self.topic, self.key.as_ref())?,
            value.deserialize(&self.topic, self.value.as_ref())?,
        ))
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

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    fn record(topic: &str, partition: i32, offset: i64) -> ConsumerRecord {
        ConsumerRecord {
            topic: topic.to_owned(),
            partition,
            offset,
            timestamp: offset,
            timestamp_type: TimestampType::CreateTime,
            key: None,
            value: Some(Bytes::from(format!("v{offset}"))),
            headers: Vec::new(),
            leader_epoch: Some(4),
        }
    }

    #[test]
    fn records_group_by_partition_and_iterate_in_order() {
        let mut records = ConsumerRecords::empty();
        assert!(records.is_empty());
        // Empty pushes are ignored.
        records.push_partition("t".to_owned(), 0, Vec::new());
        assert!(records.is_empty());

        records.push_partition("t".to_owned(), 1, vec![record("t", 1, 5)]);
        records.push_partition(
            "t".to_owned(),
            0,
            vec![record("t", 0, 0), record("t", 0, 1)],
        );
        assert_eq!(records.count(), 3);
        assert!(!records.is_empty());

        // Partitions come back in sorted (topic, partition) order.
        assert_eq!(
            records.partitions(),
            vec![TopicPartition::new("t", 0), TopicPartition::new("t", 1)]
        );
        assert_eq!(records.records(&TopicPartition::new("t", 0)).len(), 2);
        assert!(records.records(&TopicPartition::new("t", 9)).is_empty());

        // `iter` and both `IntoIterator`s walk every record in partition order.
        let offsets: Vec<i64> = records.iter().map(|record| record.offset).collect();
        assert_eq!(offsets, vec![0, 1, 5]);
        let by_ref: Vec<i64> = (&records).into_iter().map(|record| record.offset).collect();
        assert_eq!(by_ref, vec![0, 1, 5]);
        let owned: Vec<i64> = records.into_iter().map(|record| record.offset).collect();
        assert_eq!(owned, vec![0, 1, 5]);
    }

    #[test]
    fn record_exposes_topic_partition_and_timestamp() {
        let record = record("orders", 3, 9);
        assert_eq!(record.topic_partition(), TopicPartition::new("orders", 3));
        assert_eq!(record.leader_epoch, Some(4));
        let timestamp = OffsetAndTimestamp {
            offset: 9,
            timestamp: 100,
            leader_epoch: Some(4),
        };
        assert_eq!(timestamp.offset, 9);
    }
}
