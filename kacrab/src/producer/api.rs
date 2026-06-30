//! Public Kafka-compatible producer API helper types.

use std::fmt;

use kacrab_protocol::KafkaUuid;

/// Metadata for one topic partition returned by [`super::Producer::partitions_for`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProducerPartitionInfo {
    /// Topic name.
    pub topic: String,
    /// Stable Kafka topic ID.
    pub topic_id: KafkaUuid,
    /// Partition index.
    pub partition: i32,
    /// Current leader broker id.
    pub leader_id: i32,
    /// Current leader epoch.
    pub leader_epoch: i32,
    /// Replica broker ids.
    pub replica_nodes: Vec<i32>,
    /// In-sync replica broker ids.
    pub isr_nodes: Vec<i32>,
    /// Offline replica broker ids.
    pub offline_replicas: Vec<i32>,
}

/// Kafka-compatible alias for partition metadata returned by producer metadata APIs.
pub type PartitionInfo = ProducerPartitionInfo;

impl ProducerPartitionInfo {
    /// Create partition metadata from node id arrays.
    #[must_use]
    #[expect(
        clippy::too_many_arguments,
        reason = "Constructor mirrors Kafka's PartitionInfo(topic, partition, leader, replicas, \
                  isr, offlineReplicas)."
    )]
    pub fn new(
        topic: impl Into<String>,
        partition: i32,
        leader_id: i32,
        replica_nodes: impl IntoIterator<Item = i32>,
        in_sync_replicas: impl IntoIterator<Item = i32>,
        offline_replicas: impl IntoIterator<Item = i32>,
    ) -> Self {
        Self {
            topic: topic.into(),
            topic_id: KafkaUuid::ZERO,
            partition,
            leader_id,
            leader_epoch: -1,
            replica_nodes: replica_nodes.into_iter().collect(),
            isr_nodes: in_sync_replicas.into_iter().collect(),
            offline_replicas: offline_replicas.into_iter().collect(),
        }
    }

    /// Set the Kafka topic id carried by newer metadata responses.
    #[must_use]
    pub const fn with_topic_id(mut self, topic_id: KafkaUuid) -> Self {
        self.topic_id = topic_id;
        self
    }

    /// Set the leader epoch carried by newer metadata responses.
    #[must_use]
    pub const fn with_leader_epoch(mut self, leader_epoch: i32) -> Self {
        self.leader_epoch = leader_epoch;
        self
    }

    /// The topic name.
    #[must_use]
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// The stable Kafka topic id.
    #[must_use]
    pub const fn topic_id(&self) -> KafkaUuid {
        self.topic_id
    }

    /// The partition id.
    #[must_use]
    pub const fn partition(&self) -> i32 {
        self.partition
    }

    /// The leader broker id, or `None` when there is no leader.
    #[must_use]
    pub const fn leader(&self) -> Option<i32> {
        if self.leader_id < 0 {
            None
        } else {
            Some(self.leader_id)
        }
    }

    /// The raw leader broker id.
    #[must_use]
    pub const fn leader_id(&self) -> i32 {
        self.leader_id
    }

    /// The current leader epoch.
    #[must_use]
    pub const fn leader_epoch(&self) -> i32 {
        self.leader_epoch
    }

    /// Replica broker ids.
    #[must_use]
    pub fn replicas(&self) -> &[i32] {
        &self.replica_nodes
    }

    /// In-sync replica broker ids.
    #[must_use]
    pub fn in_sync_replicas(&self) -> &[i32] {
        &self.isr_nodes
    }

    /// Offline replica broker ids.
    #[must_use]
    pub fn offline_replicas(&self) -> &[i32] {
        &self.offline_replicas
    }
}

impl fmt::Display for ProducerPartitionInfo {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Partition(topic = {}, partition = {}, leader = {}, replicas = {}, isr = {}, \
             offlineReplicas = {})",
            self.topic,
            self.partition,
            self.leader()
                .map_or_else(|| "none".to_owned(), |leader| leader.to_string()),
            NodeIdList(&self.replica_nodes),
            NodeIdList(&self.isr_nodes),
            NodeIdList(&self.offline_replicas)
        )
    }
}

struct NodeIdList<'a>(&'a [i32]);

impl fmt::Display for NodeIdList<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("[")?;
        for (index, node_id) in self.0.iter().enumerate() {
            if index > 0 {
                formatter.write_str(",")?;
            }
            write!(formatter, "{node_id}")?;
        }
        formatter.write_str("]")
    }
}

// `TopicPartition`, `OffsetAndMetadata`, and `ConsumerGroupMetadata` live in
// `kacrab::common` (always compiled). They are re-exported here so existing paths
// such as `kacrab::producer::TopicPartition` keep working.
pub use crate::common::{ConsumerGroupMetadata, OffsetAndMetadata, TopicPartition};

/// Metric registration token for Kafka-compatible client telemetry APIs.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ProducerMetricSubscription {
    /// Metric name.
    pub name: String,
}

impl ProducerMetricSubscription {
    /// Create a metric subscription token.
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

#[cfg(test)]
mod tests {
    use kacrab_protocol::KafkaUuid;

    use super::PartitionInfo;

    #[test]
    fn partition_info_accessors_and_display_match_java_shape() {
        let info = PartitionInfo::new("orders", 2, 7, [7, 8], [7], [8])
            .with_topic_id(KafkaUuid::from_parts(1, 2))
            .with_leader_epoch(42);

        assert_eq!(info.topic(), "orders");
        assert_eq!(info.topic_id(), KafkaUuid::from_parts(1, 2));
        assert_eq!(info.partition(), 2);
        assert_eq!(info.leader(), Some(7));
        assert_eq!(info.leader_id(), 7);
        assert_eq!(info.leader_epoch(), 42);
        assert_eq!(info.replicas(), &[7, 8]);
        assert_eq!(info.in_sync_replicas(), &[7]);
        assert_eq!(info.offline_replicas(), &[8]);
        assert_eq!(
            info.to_string(),
            "Partition(topic = orders, partition = 2, leader = 7, replicas = [7,8], isr = [7], \
             offlineReplicas = [8])"
        );
        assert_eq!(
            PartitionInfo::new("orders", 0, -1, [7], [], []).to_string(),
            "Partition(topic = orders, partition = 0, leader = none, replicas = [7], isr = [], \
             offlineReplicas = [])"
        );
    }
}
