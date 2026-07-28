//! Records returned by [`ShareConsumer::poll`](super::ShareConsumer::poll).
//!
//! A share group delivers *acquired* records, not a position: each record comes
//! with an acquisition lock held by this member and a delivery count saying how
//! many times it has been handed to some member of the group. That is why these
//! are their own types rather than the consumer's
//! [`ConsumerRecords`](crate::consumer::ConsumerRecords) — a share record without
//! its delivery count cannot answer the question share groups exist to answer
//! ("is this a poison message?").

use std::collections::BTreeMap;

use crate::{common::TopicPartition, consumer::record::ConsumerRecord};

/// How the application disposes of one delivered record, mirroring Kafka's
/// `AcknowledgeType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AcknowledgeType {
    /// Consumed successfully; do not deliver it again.
    Accept,
    /// Not consumed; release the acquisition lock so it can be delivered again.
    Release,
    /// Not consumed and not worth retrying; archive it without redelivery.
    Reject,
    /// Still being processed; renew the acquisition lock.
    Renew,
}

impl AcknowledgeType {
    /// The wire byte for the `acknowledge_types` array.
    #[must_use]
    pub const fn wire(self) -> i8 {
        match self {
            Self::Accept => ACKNOWLEDGE_ACCEPT,
            Self::Release => ACKNOWLEDGE_RELEASE,
            Self::Reject => ACKNOWLEDGE_REJECT,
            Self::Renew => ACKNOWLEDGE_RENEW,
        }
    }
}

/// `acknowledge_types` byte for an offset the broker acquired for us but that
/// carried no record (compacted away, or a control record). The client owes the
/// broker this acknowledgement even though the application never sees the
/// offset; without it the offset stays acquired until its lock expires.
pub(super) const ACKNOWLEDGE_GAP: i8 = 0;
const ACKNOWLEDGE_ACCEPT: i8 = 1;
const ACKNOWLEDGE_RELEASE: i8 = 2;
const ACKNOWLEDGE_REJECT: i8 = 3;
/// `Renew` acknowledgements put the whole request into KIP-1222's renew-only
/// mode (`is_renew_ack`), which the broker only accepts with every fetch sizing
/// field zeroed. kacrab therefore sends them in a dedicated `ShareAcknowledge`.
const ACKNOWLEDGE_RENEW: i8 = 4;

/// One acquired record: the record itself plus its KIP-932 delivery count.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareRecord {
    /// The record as delivered by the broker.
    pub record: ConsumerRecord,
    /// How many times this record has been delivered to the share group,
    /// counting this delivery. `1` on a first delivery; higher values mean an
    /// earlier delivery was released or its acquisition lock expired.
    pub delivery_count: i16,
}

impl ShareRecord {
    /// The record's topic and partition as a [`TopicPartition`] key.
    #[must_use]
    pub fn topic_partition(&self) -> TopicPartition {
        self.record.topic_partition()
    }

    /// The record's offset within its partition.
    #[must_use]
    pub const fn offset(&self) -> i64 {
        self.record.offset
    }
}

/// The batch of acquired records returned by one
/// [`ShareConsumer::poll`](super::ShareConsumer::poll), grouped by partition and
/// iterable in partition then offset order.
///
/// Unlike [`ConsumerRecords`](crate::consumer::ConsumerRecords) this is not a
/// slice of a larger client-side buffer: every record in it is locked to this
/// member at the broker, so the whole acquisition is handed over at once.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ShareRecords {
    by_partition: BTreeMap<TopicPartition, Vec<ShareRecord>>,
    count: usize,
}

impl ShareRecords {
    /// Build an empty batch.
    #[must_use]
    pub fn empty() -> Self {
        Self::default()
    }

    /// Append records for one partition (kept in the given order).
    pub(super) fn push_partition(
        &mut self,
        topic: String,
        partition: i32,
        records: Vec<ShareRecord>,
    ) {
        if records.is_empty() {
            return;
        }
        self.count = self.count.saturating_add(records.len());
        match self
            .by_partition
            .entry(TopicPartition::new(topic, partition))
        {
            std::collections::btree_map::Entry::Vacant(entry) => {
                let _records = entry.insert(records);
            },
            std::collections::btree_map::Entry::Occupied(mut entry) => {
                entry.get_mut().extend(records);
            },
        }
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
        self.by_partition.keys().cloned().collect()
    }

    /// Records for a single partition, in offset order.
    #[must_use]
    pub fn records(&self, partition: &TopicPartition) -> &[ShareRecord] {
        self.by_partition.get(partition).map_or(&[], Vec::as_slice)
    }

    /// Iterate every record across all partitions, in partition then offset order.
    pub fn iter(&self) -> impl Iterator<Item = &ShareRecord> {
        self.by_partition.values().flatten()
    }
}

impl<'a> IntoIterator for &'a ShareRecords {
    type Item = &'a ShareRecord;
    type IntoIter = std::iter::Flatten<
        std::collections::btree_map::Values<'a, TopicPartition, Vec<ShareRecord>>,
    >;

    fn into_iter(self) -> Self::IntoIter {
        self.by_partition.values().flatten()
    }
}

impl IntoIterator for ShareRecords {
    type Item = ShareRecord;
    type IntoIter = std::iter::Flatten<
        std::collections::btree_map::IntoValues<TopicPartition, Vec<ShareRecord>>,
    >;

    fn into_iter(self) -> Self::IntoIter {
        self.by_partition.into_values().flatten()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::consumer::TimestampType;

    fn share_record(topic: &str, partition: i32, offset: i64, delivery: i16) -> ShareRecord {
        ShareRecord {
            record: ConsumerRecord {
                topic: Arc::from(topic),
                partition,
                offset,
                timestamp: offset,
                timestamp_type: TimestampType::CreateTime,
                key: None,
                value: None,
                headers: Vec::new(),
                leader_epoch: None,
            },
            delivery_count: delivery,
        }
    }

    #[test]
    fn acknowledge_types_map_to_the_kafka_wire_bytes() {
        assert_eq!(AcknowledgeType::Accept.wire(), 1);
        assert_eq!(AcknowledgeType::Release.wire(), 2);
        assert_eq!(AcknowledgeType::Reject.wire(), 3);
        assert_eq!(AcknowledgeType::Renew.wire(), 4);
        assert_eq!(ACKNOWLEDGE_GAP, 0);
    }

    #[test]
    fn records_group_by_partition_and_carry_delivery_counts() {
        let mut records = ShareRecords::empty();
        assert!(records.is_empty());
        records.push_partition("jobs".to_owned(), 0, Vec::new());
        assert!(records.is_empty());

        records.push_partition("jobs".to_owned(), 1, vec![share_record("jobs", 1, 5, 3)]);
        records.push_partition(
            "jobs".to_owned(),
            0,
            vec![share_record("jobs", 0, 0, 1), share_record("jobs", 0, 1, 1)],
        );
        assert_eq!(records.count(), 3);
        assert_eq!(
            records.partitions(),
            vec![
                TopicPartition::new("jobs", 0),
                TopicPartition::new("jobs", 1)
            ]
        );
        assert_eq!(records.records(&TopicPartition::new("jobs", 0)).len(), 2);
        assert!(records.records(&TopicPartition::new("jobs", 9)).is_empty());

        let redelivered = records.records(&TopicPartition::new("jobs", 1))[0].clone();
        assert_eq!(redelivered.delivery_count, 3);
        assert_eq!(redelivered.offset(), 5);
        assert_eq!(
            redelivered.topic_partition(),
            TopicPartition::new("jobs", 1)
        );

        let offsets: Vec<i64> = records.iter().map(ShareRecord::offset).collect();
        assert_eq!(offsets, vec![0, 1, 5]);
        let by_ref: Vec<i64> = (&records).into_iter().map(ShareRecord::offset).collect();
        assert_eq!(by_ref, vec![0, 1, 5]);
        let owned: Vec<i64> = records.into_iter().map(|record| record.offset()).collect();
        assert_eq!(owned, vec![0, 1, 5]);
    }

    #[test]
    fn a_second_push_for_one_partition_appends() {
        let mut records = ShareRecords::empty();
        records.push_partition("jobs".to_owned(), 0, vec![share_record("jobs", 0, 0, 1)]);
        records.push_partition("jobs".to_owned(), 0, vec![share_record("jobs", 0, 1, 1)]);
        assert_eq!(records.count(), 2);
        assert_eq!(records.records(&TopicPartition::new("jobs", 0)).len(), 2);
    }
}
