//! Offset lookup and `auto.offset.reset` handling via `ListOffsets`.
//!
//! Resolves an initial fetch position for freshly assigned partitions
//! (`auto.offset.reset`) and backs `seek_to_beginning`/`seek_to_end` — both are
//! `ListOffsets` queries grouped by partition leader. Also validates positions
//! against a leader's epoch history via `OffsetForLeaderEpoch` (KIP-320
//! truncation detection). Committed-offset commit/fetch live in `coordinator`.

use std::collections::HashMap;

use kacrab_protocol::{
    generated::{
        ApiKey, ErrorCode, ListOffsetsPartition, ListOffsetsRequestData, ListOffsetsResponseData,
        ListOffsetsTopic, OffsetForLeaderEpochRequestData, OffsetForLeaderEpochResponseData,
        offset_for_leader_epoch_request::{OffsetForLeaderPartition, OffsetForLeaderTopic},
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

/// A resolved offset for a partition, with the leader epoch and (for
/// timestamp lookups) the record timestamp the broker reported.
#[derive(Debug, Clone, Copy)]
pub(super) struct ResolvedOffset {
    pub offset: i64,
    pub leader_epoch: Option<i32>,
    pub timestamp: i64,
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
                        timestamp: partition.timestamp,
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

/// The outcome of validating a fetch position against its partition leader's
/// epoch history (KIP-320 truncation detection).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PositionValidation {
    /// The position is still valid under the current leader; its recorded epoch
    /// is advanced to `leader_epoch` so it is not re-validated.
    Valid { leader_epoch: i32 },
    /// The log was truncated below the position; reset to `offset` (the end
    /// offset of the largest epoch at or below the position's epoch).
    Truncated {
        offset: i64,
        leader_epoch: Option<i32>,
    },
}

/// The current leader epoch of a topic partition from cluster metadata, or `None`
/// when unknown.
pub(super) fn partition_leader_epoch(
    metadata: &ClusterMetadata,
    topic: &str,
    partition: i32,
) -> Option<i32> {
    metadata
        .topic(topic)?
        .partitions
        .iter()
        .find(|entry| entry.partition_index == partition)
        .map(|entry| entry.leader_epoch)
        .filter(|epoch| *epoch >= 0)
}

/// Validate fetch positions against their leaders via `OffsetForLeaderEpoch`
/// (KIP-320). Each entry is `(partition, position, current_leader_epoch)`. A
/// leader whose epoch history ends below our position signals truncation. Errors
/// for individual partitions are skipped (retried on the next poll).
pub(super) async fn validate_offsets(
    wire: &WireClient,
    metadata: &ClusterMetadata,
    entries: &[(TopicPartition, FetchPosition, i32)],
) -> Result<HashMap<TopicPartition, PositionValidation>> {
    if entries.is_empty() {
        return Ok(HashMap::new());
    }

    let mut by_leader: HashMap<i32, Vec<(TopicPartition, FetchPosition, i32)>> = HashMap::new();
    for entry in entries {
        let Some(leader) = partition_leader(metadata, &entry.0.topic, entry.0.partition) else {
            continue;
        };
        by_leader.entry(leader).or_default().push(entry.clone());
    }

    let version = client_api_info(ApiKey::OffsetForLeaderEpoch).max_version;
    let mut out = HashMap::new();
    for (leader, leader_entries) in by_leader {
        let request = OffsetForLeaderEpochRequestData {
            replica_id: -1,
            topics: leader_epoch_topics(&leader_entries),
            _unknown_tagged_fields: Vec::new(),
        };
        let response: OffsetForLeaderEpochResponseData = wire
            .send_to_broker(leader, ApiKey::OffsetForLeaderEpoch, version, &request)
            .await?;
        for topic in response.topics {
            for partition in topic.partitions {
                if ErrorCode::from(partition.error_code).is_error() {
                    continue;
                }
                let tp = TopicPartition::new(topic.topic.as_str().to_owned(), partition.partition);
                let Some(position) = leader_entries
                    .iter()
                    .find(|entry| entry.0 == tp)
                    .map(|entry| entry.1)
                else {
                    continue;
                };
                let outcome = classify_epoch_end(
                    partition.end_offset,
                    partition.leader_epoch,
                    position,
                    entry_current_epoch(&leader_entries, &tp),
                );
                let _previous = out.insert(tp, outcome);
            }
        }
    }
    Ok(out)
}

/// Decide whether a leader's reported epoch end offset means our position was
/// truncated. Truncation is an epoch end offset at or below zero-based bounds and
/// strictly below our position; otherwise the position is valid and its recorded
/// epoch advances to the current leader epoch.
const fn classify_epoch_end(
    end_offset: i64,
    response_leader_epoch: i32,
    position: FetchPosition,
    current_epoch: i32,
) -> PositionValidation {
    if end_offset >= 0 && end_offset < position.offset {
        PositionValidation::Truncated {
            offset: end_offset,
            leader_epoch: if response_leader_epoch >= 0 {
                Some(response_leader_epoch)
            } else {
                None
            },
        }
    } else {
        PositionValidation::Valid {
            leader_epoch: current_epoch,
        }
    }
}

/// The `current_leader_epoch` we asked to validate a partition against.
fn entry_current_epoch(
    entries: &[(TopicPartition, FetchPosition, i32)],
    partition: &TopicPartition,
) -> i32 {
    entries
        .iter()
        .find(|entry| &entry.0 == partition)
        .map_or(-1, |entry| entry.2)
}

/// Build `OffsetForLeaderEpoch` topics from `(partition, position, current_epoch)`
/// entries.
fn leader_epoch_topics(
    entries: &[(TopicPartition, FetchPosition, i32)],
) -> Vec<OffsetForLeaderTopic> {
    let mut topics: Vec<OffsetForLeaderTopic> = Vec::new();
    for (partition, position, current_epoch) in entries {
        let wire_partition = OffsetForLeaderPartition {
            partition: partition.partition,
            current_leader_epoch: *current_epoch,
            leader_epoch: position.leader_epoch.unwrap_or(-1),
            _unknown_tagged_fields: Vec::new(),
        };
        if let Some(topic) = topics
            .iter_mut()
            .find(|topic| topic.topic.as_str() == partition.topic)
        {
            topic.partitions.push(wire_partition);
        } else {
            topics.push(OffsetForLeaderTopic {
                topic: partition.topic.clone().into(),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn epoch_end_below_position_is_truncation() {
        // Log's epoch ended at 40 but we were at 50 → truncated back to 40.
        let outcome = classify_epoch_end(40, 3, FetchPosition::new(50, Some(5)), 7);
        assert_eq!(
            outcome,
            PositionValidation::Truncated {
                offset: 40,
                leader_epoch: Some(3),
            }
        );
    }

    #[test]
    fn epoch_end_at_or_above_position_is_valid() {
        // End offset 50 >= our position 50 → no truncation; epoch advances to 7.
        let outcome = classify_epoch_end(50, 5, FetchPosition::new(50, Some(5)), 7);
        assert_eq!(outcome, PositionValidation::Valid { leader_epoch: 7 });
        // An unknown (-1) end offset is treated as valid, too.
        let unknown = classify_epoch_end(-1, -1, FetchPosition::new(50, Some(5)), 7);
        assert_eq!(unknown, PositionValidation::Valid { leader_epoch: 7 });
    }

    #[test]
    fn leader_epoch_topics_group_partitions_and_carry_epochs() {
        let entries = vec![
            (
                TopicPartition::new("t", 0),
                FetchPosition::new(10, Some(4)),
                6,
            ),
            (TopicPartition::new("t", 1), FetchPosition::new(20, None), 6),
        ];
        let topics = leader_epoch_topics(&entries);
        assert_eq!(topics.len(), 1);
        let partitions = &topics[0].partitions;
        assert_eq!(partitions.len(), 2);
        assert_eq!(partitions[0].current_leader_epoch, 6);
        assert_eq!(partitions[0].leader_epoch, 4);
        // A position with no recorded epoch asks for -1.
        assert_eq!(partitions[1].leader_epoch, -1);
    }
}
