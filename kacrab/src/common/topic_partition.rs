//! Topic/partition key and committed-offset metadata.

/// Topic/partition key used by offset commits and partition-scoped operations.
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

/// Offset metadata used by offset commits.
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
