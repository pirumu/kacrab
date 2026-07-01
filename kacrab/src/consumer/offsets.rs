//! Offset lookup and `auto.offset.reset` handling via `ListOffsets`.
//!
//! Phase 1 needs two things from offsets: resolving an initial fetch position
//! for freshly assigned partitions (`auto.offset.reset`), and the
//! `seek_to_beginning`/`seek_to_end` primitives — both are `ListOffsets` queries
//! grouped by partition leader. Committed-offset fetch and commit arrive with
//! group coordination in Phase 2.

use std::collections::HashMap;

use kacrab_protocol::{
    generated::{
        ApiKey, ErrorCode, ListOffsetsPartition, ListOffsetsRequestData, ListOffsetsResponseData,
        ListOffsetsTopic,
    },
    version::client_api_info,
};

use super::{
    config::ConsumerRuntimeConfig,
    error::{ConsumerError, Result},
    subscription::FetchPosition,
};
use crate::{
    common::TopicPartition,
    wire::{ClusterMetadata, WireClient},
};

/// `ListOffsets` timestamp sentinel for the earliest available offset.
pub(super) const EARLIEST_TIMESTAMP: i64 = -2;
/// `ListOffsets` timestamp sentinel for the latest offset (log end).
pub(super) const LATEST_TIMESTAMP: i64 = -1;

/// A resolved offset for a partition, with the leader epoch the broker reported.
#[derive(Debug, Clone, Copy)]
pub(super) struct ResolvedOffset {
    pub offset: i64,
    pub leader_epoch: Option<i32>,
}

impl ResolvedOffset {
    pub(super) const fn into_position(self) -> FetchPosition {
        FetchPosition::new(self.offset, self.leader_epoch)
    }
}

/// Resolve one offset per partition at the given timestamp sentinel, routing each
/// partition to its current leader and aggregating the per-leader responses.
pub(super) async fn list_offsets(
    wire: &WireClient,
    config: &ConsumerRuntimeConfig,
    metadata: &ClusterMetadata,
    entries: &[(TopicPartition, i64)],
) -> Result<HashMap<TopicPartition, ResolvedOffset>> {
    if entries.is_empty() {
        return Ok(HashMap::new());
    }

    let mut by_leader: HashMap<i32, Vec<(TopicPartition, i64)>> = HashMap::new();
    for (partition, timestamp) in entries {
        let leader = partition_leader(metadata, &partition.topic, partition.partition)
            .ok_or_else(|| leader_unavailable(partition))?;
        by_leader
            .entry(leader)
            .or_default()
            .push((partition.clone(), *timestamp));
    }

    let version = client_api_info(ApiKey::ListOffsets).max_version;
    let request_timeout_ms = i32::try_from(config.request_timeout.as_millis()).unwrap_or(i32::MAX);
    let mut resolved = HashMap::with_capacity(entries.len());

    for (leader, leader_entries) in by_leader {
        let request = ListOffsetsRequestData {
            replica_id: -1,
            isolation_level: config.isolation_level.wire(),
            topics: list_offsets_topics(&leader_entries),
            timeout_ms: request_timeout_ms,
            _unknown_tagged_fields: Vec::new(),
        };
        let response: ListOffsetsResponseData = wire
            .send_to_broker(leader, ApiKey::ListOffsets, version, &request)
            .await?;
        for topic in response.topics {
            for partition in topic.partitions {
                let error = ErrorCode::from(partition.error_code);
                if error.is_error() {
                    return Err(ConsumerError::broker(
                        "list_offsets",
                        error,
                        format!(
                            "{}-{} list_offsets failed",
                            topic.name.as_str(),
                            partition.partition_index
                        ),
                    ));
                }
                let _previous = resolved.insert(
                    TopicPartition::new(topic.name.as_str().to_owned(), partition.partition_index),
                    ResolvedOffset {
                        offset: partition.offset,
                        leader_epoch: (partition.leader_epoch >= 0)
                            .then_some(partition.leader_epoch),
                    },
                );
            }
        }
    }
    Ok(resolved)
}

/// Build one `ListOffsets` topic list from `(partition, timestamp)` entries.
fn list_offsets_topics(entries: &[(TopicPartition, i64)]) -> Vec<ListOffsetsTopic> {
    let mut topics: Vec<ListOffsetsTopic> = Vec::new();
    for (partition, timestamp) in entries {
        let wire_partition = ListOffsetsPartition {
            partition_index: partition.partition,
            current_leader_epoch: -1,
            timestamp: *timestamp,
            _unknown_tagged_fields: Vec::new(),
        };
        if let Some(topic) = topics
            .iter_mut()
            .find(|topic| topic.name.as_str() == partition.topic)
        {
            topic.partitions.push(wire_partition);
        } else {
            topics.push(ListOffsetsTopic {
                name: partition.topic.clone().into(),
                partitions: vec![wire_partition],
                _unknown_tagged_fields: Vec::new(),
            });
        }
    }
    topics
}

/// The current leader broker id for a topic partition, or `None` when unknown.
pub(super) fn partition_leader(
    metadata: &ClusterMetadata,
    topic: &str,
    partition: i32,
) -> Option<i32> {
    metadata
        .topic(topic)?
        .partitions
        .iter()
        .find(|entry| entry.partition_index == partition)
        .map(|entry| entry.leader_id)
        .filter(|leader| *leader >= 0)
}

fn leader_unavailable(partition: &TopicPartition) -> ConsumerError {
    ConsumerError::broker(
        "list_offsets",
        ErrorCode::LeaderNotAvailable,
        format!(
            "no known leader for {}-{}",
            partition.topic, partition.partition
        ),
    )
}
