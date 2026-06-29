//! Rust-native producer partitioner hooks.

use std::{
    collections::HashMap,
    fmt,
    sync::{Arc, Mutex},
};

use super::{ProducerRecord, Result, error::ProducerError};
use crate::wire::ClusterMetadata;

/// Rust-native hook for selecting a partition for unassigned producer records.
///
/// This mirrors Kafka's `Partitioner` extension point at the contract
/// level while staying native to Rust. `partitioner.class` is not JVM-loaded;
/// callers install implementations through [`super::ProducerBuilder::partitioner`]
/// or [`super::Producer::set_partitioner`].
pub trait ProducerPartitioner: Send + Sync + 'static {
    /// Select a concrete partition for `record` using the current metadata snapshot.
    ///
    /// Implementations are called only for records without an explicit
    /// partition. The returned partition must exist in `metadata` for the
    /// record topic.
    ///
    /// # Errors
    ///
    /// Returns a producer error when the partition cannot be selected.
    fn partition(&self, record: &ProducerRecord, metadata: &ClusterMetadata) -> Result<i32>;

    /// Release partitioner resources when the producer is closed.
    fn close(&self) {}
}

/// Built-in partitioner mirroring Kafka's `RoundRobinPartitioner`.
///
/// It spreads records evenly across a topic's available partitions regardless of
/// the record key, using a per-topic counter. Unlike the default sticky
/// partitioner this switches partition on every record rather than per batch.
/// Install it with [`super::ProducerBuilder::partitioner`] /
/// [`super::Producer::set_partitioner`].
#[derive(Debug, Default)]
pub struct RoundRobinPartitioner {
    counters: Mutex<HashMap<String, u32>>,
}

impl RoundRobinPartitioner {
    /// Create a new round-robin partitioner.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "The counter map guard is held only for the get-and-increment."
    )]
    fn next_counter(&self, topic: &str) -> usize {
        let mut counters = self
            .counters
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let counter = counters.entry(topic.to_owned()).or_insert(0);
        let current = *counter;
        *counter = counter.wrapping_add(1);
        current as usize
    }
}

impl ProducerPartitioner for RoundRobinPartitioner {
    fn partition(&self, record: &ProducerRecord, metadata: &ClusterMetadata) -> Result<i32> {
        let topic = metadata
            .topic(&record.topic)
            .filter(|topic| !topic.partitions.is_empty())
            .ok_or_else(|| ProducerError::UnknownTopic(record.topic.to_string()))?;
        let counter = self.next_counter(&record.topic);
        // Prefer partitions with a live leader (Kafka availablePartitions); fall
        // back to all partitions when none are currently available.
        let available: Vec<i32> = topic
            .partitions
            .iter()
            .filter(|partition| partition.leader_id >= 0)
            .map(|partition| partition.partition_index)
            .collect();
        let selected = if available.is_empty() {
            counter
                .checked_rem(topic.partitions.len())
                .and_then(|index| topic.partitions.get(index))
                .map(|partition| partition.partition_index)
        } else {
            counter
                .checked_rem(available.len())
                .and_then(|index| available.get(index))
                .copied()
        };
        selected.ok_or_else(|| ProducerError::UnknownTopic(record.topic.to_string()))
    }
}

#[derive(Clone, Default)]
pub(crate) struct ProducerPartitionerHandle {
    inner: Option<Arc<dyn ProducerPartitioner>>,
}

impl ProducerPartitionerHandle {
    pub(crate) fn new(partitioner: impl ProducerPartitioner) -> Self {
        Self {
            inner: Some(Arc::new(partitioner)),
        }
    }

    pub(crate) const fn is_some(&self) -> bool {
        self.inner.is_some()
    }

    pub(crate) fn partition(
        &self,
        record: &ProducerRecord,
        metadata: &ClusterMetadata,
    ) -> Option<Result<i32>> {
        self.inner
            .as_ref()
            .map(|partitioner| partitioner.partition(record, metadata))
    }

    pub(crate) fn close(&self) {
        if let Some(partitioner) = &self.inner {
            partitioner.close();
        }
    }
}

impl fmt::Debug for ProducerPartitionerHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProducerPartitionerHandle")
            .field("installed", &self.inner.is_some())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::expect_used,
        clippy::missing_assert_message,
        reason = "Unit test fixtures fail fastest with contextual expect calls."
    )]

    use kacrab_protocol::KafkaUuid;

    use super::{ProducerPartitioner, ProducerRecord, RoundRobinPartitioner};
    use crate::wire::{ClusterMetadata, PartitionMetadata, TopicMetadata};

    fn metadata(partitions: &[(i32, i32)]) -> ClusterMetadata {
        ClusterMetadata {
            cluster_id: None,
            controller_id: 1,
            brokers: Vec::new(),
            topics: vec![TopicMetadata {
                name: "orders".to_owned(),
                topic_id: KafkaUuid::ZERO,
                partitions: partitions
                    .iter()
                    .map(|&(partition_index, leader_id)| PartitionMetadata {
                        partition_index,
                        leader_id,
                        leader_epoch: 0,
                        replica_nodes: Vec::new(),
                        isr_nodes: Vec::new(),
                        offline_replicas: Vec::new(),
                    })
                    .collect(),
            }],
        }
    }

    #[test]
    fn round_robin_partitioner_cycles_available_partitions_like_java() {
        let partitioner = RoundRobinPartitioner::new();
        let metadata = metadata(&[(0, 7), (1, 7), (2, 7)]);
        let record = ProducerRecord::new("orders", 0);

        let selected: Vec<i32> = (0..6)
            .map(|_| {
                partitioner
                    .partition(&record, &metadata)
                    .expect("round-robin partition")
            })
            .collect();

        assert_eq!(selected, vec![0, 1, 2, 0, 1, 2]);
    }

    #[test]
    fn round_robin_partitioner_skips_partitions_without_a_leader() {
        let partitioner = RoundRobinPartitioner::new();
        // Partition 1 has no live leader and must be skipped.
        let metadata = metadata(&[(0, 7), (1, -1), (2, 7)]);
        let record = ProducerRecord::new("orders", 0);

        let selected: Vec<i32> = (0..4)
            .map(|_| {
                partitioner
                    .partition(&record, &metadata)
                    .expect("round-robin partition")
            })
            .collect();

        assert_eq!(selected, vec![0, 2, 0, 2]);
    }
}
