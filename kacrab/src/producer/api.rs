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
    use super::ConsumerGroupMetadata;

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
}
