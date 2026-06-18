//! Public Java-compatible producer API helper types.

use core::fmt;

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

/// Java-compatible alias for partition metadata returned by producer metadata APIs.
pub type PartitionInfo = ProducerPartitionInfo;

impl ProducerPartitionInfo {
    /// Create partition metadata from Java-style node id arrays.
    #[must_use]
    #[expect(
        clippy::too_many_arguments,
        reason = "Constructor mirrors Java PartitionInfo(topic, partition, leader, replicas, isr, \
                  offlineReplicas)."
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

/// Topic/partition key used by transactional offset commits.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TopicPartition {
    /// Topic name.
    pub topic: String,
    /// Partition index.
    pub partition: i32,
}

impl TopicPartition {
    /// Create a topic/partition key.
    #[must_use]
    pub fn new(topic: impl Into<String>, partition: i32) -> Self {
        Self {
            topic: topic.into(),
            partition,
        }
    }
}

/// Offset metadata used by transactional offset commits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetAndMetadata {
    /// Committed offset.
    pub offset: i64,
    /// Optional leader epoch for log truncation detection.
    pub leader_epoch: Option<i32>,
    /// Optional user metadata.
    pub metadata: Option<String>,
}

impl OffsetAndMetadata {
    /// Create offset metadata without user metadata.
    #[must_use]
    pub const fn new(offset: i64) -> Self {
        Self {
            offset,
            leader_epoch: None,
            metadata: None,
        }
    }

    /// Set the leader epoch of the last consumed record.
    #[must_use]
    pub const fn leader_epoch(mut self, leader_epoch: i32) -> Self {
        self.leader_epoch = Some(leader_epoch);
        self
    }

    /// Set user metadata.
    #[must_use]
    pub fn metadata(mut self, metadata: impl Into<String>) -> Self {
        self.metadata = Some(metadata.into());
        self
    }
}

/// Consumer group metadata used by transactional offset commits.
#[derive(Debug, Clone, PartialEq, Eq)]
#[expect(
    clippy::struct_field_names,
    reason = "Field names intentionally mirror Java ConsumerGroupMetadata accessors."
)]
pub struct ConsumerGroupMetadata {
    /// Consumer group id.
    pub group_id: String,
    /// Consumer group generation id, or `-1` when unknown.
    pub generation_id: i32,
    /// Consumer group member id, or an empty string when unknown.
    pub member_id: String,
    /// Optional static group instance id.
    pub group_instance_id: Option<String>,
}

impl ConsumerGroupMetadata {
    /// Create consumer group metadata.
    #[must_use]
    pub fn new(group_id: impl Into<String>) -> Self {
        Self {
            group_id: group_id.into(),
            generation_id: -1,
            member_id: String::new(),
            group_instance_id: None,
        }
    }

    /// Create consumer group metadata with the full Java constructor shape.
    #[must_use]
    pub fn from_parts(
        group_id: impl Into<String>,
        generation_id: i32,
        member_id: impl Into<String>,
        group_instance_id: Option<String>,
    ) -> Self {
        Self {
            group_id: group_id.into(),
            generation_id,
            member_id: member_id.into(),
            group_instance_id,
        }
    }

    /// Set the consumer group generation id.
    #[must_use]
    pub const fn generation_id(mut self, generation_id: i32) -> Self {
        self.generation_id = generation_id;
        self
    }

    /// Set the consumer group member id.
    #[must_use]
    pub fn member_id(mut self, member_id: impl Into<String>) -> Self {
        self.member_id = member_id.into();
        self
    }

    /// Set the optional static group instance id.
    #[must_use]
    pub fn group_instance_id(mut self, group_instance_id: impl Into<String>) -> Self {
        self.group_instance_id = Some(group_instance_id.into());
        self
    }
}

impl fmt::Display for ConsumerGroupMetadata {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "GroupMetadata(groupId = {}, generationId = {}, memberId = {}, groupInstanceId = {})",
            self.group_id,
            self.generation_id,
            self.member_id,
            self.group_instance_id.as_deref().unwrap_or("")
        )
    }
}

/// Metric registration token for Java-compatible client telemetry APIs.
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

    use super::{ConsumerGroupMetadata, PartitionInfo};

    #[test]
    fn consumer_group_metadata_display_matches_java_to_string_shape() {
        let metadata = ConsumerGroupMetadata::new("group-a")
            .generation_id(42)
            .member_id("member-a")
            .group_instance_id("instance-a");

        assert_eq!(
            metadata.to_string(),
            "GroupMetadata(groupId = group-a, generationId = 42, memberId = member-a, \
             groupInstanceId = instance-a)"
        );

        assert_eq!(
            ConsumerGroupMetadata::new("group-a").to_string(),
            "GroupMetadata(groupId = group-a, generationId = -1, memberId = , groupInstanceId = )"
        );
    }

    #[test]
    fn consumer_group_metadata_from_parts_matches_java_full_constructor_shape() {
        let metadata =
            ConsumerGroupMetadata::from_parts("group-a", 42, "member-a", Some("instance-a".into()));

        assert_eq!(metadata.group_id, "group-a");
        assert_eq!(metadata.generation_id, 42);
        assert_eq!(metadata.member_id, "member-a");
        assert_eq!(metadata.group_instance_id.as_deref(), Some("instance-a"));
    }

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
