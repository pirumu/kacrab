//! Group coordinator lookup and committed-offset commit/fetch.
//!
//! Phase 2a wires the offset side of group membership: find the group
//! coordinator (`FindCoordinator`), commit offsets (`OffsetCommit`), and read
//! committed offsets (`OffsetFetch`). This works for a manual-assignment
//! consumer that carries a `group.id` — no join/sync is required to own offsets.
//! `OffsetCommit` is capped at v9 and `OffsetFetch` at v7 so both stay
//! topic-*name* keyed, sidestepping the v10/v8 topic-id strict-codec forms
//! (the same trap seen on the admin offset paths). Join/sync/heartbeat and
//! server-side membership arrive in Phase 2b.

use std::collections::HashMap;

use kacrab_protocol::{
    KafkaString,
    generated::{
        ApiKey, ErrorCode, FindCoordinatorRequestData, FindCoordinatorResponseData,
        OffsetCommitRequestData, OffsetCommitRequestPartition, OffsetCommitRequestTopic,
        OffsetCommitResponseData, OffsetFetchRequestData, OffsetFetchRequestTopic,
        OffsetFetchResponseData,
    },
    version::client_api_info,
};

use super::error::{ConsumerError, Result};
use crate::{
    common::{OffsetAndMetadata, TopicPartition},
    wire::{BrokerEndpoint, WireClient, WireError},
};

/// `FindCoordinator` key type for a consumer group coordinator.
const COORDINATOR_KEY_TYPE_GROUP: i8 = 0;
/// Highest `OffsetCommit` version to negotiate; v10 switched to topic ids.
const OFFSET_COMMIT_MAX_VERSION: i16 = 9;
/// Highest `OffsetFetch` version to negotiate; v8 switched to the `groups` form.
const OFFSET_FETCH_MAX_VERSION: i16 = 7;
/// Max `FindCoordinator` attempts while the coordinator is loading.
const FIND_COORDINATOR_MAX_ATTEMPTS: u32 = 20;
/// Backoff between `FindCoordinator` retries.
const FIND_COORDINATOR_BACKOFF: std::time::Duration = std::time::Duration::from_millis(500);

/// Resolve the group coordinator, register its endpoint, and return its node id.
///
/// A freshly started broker loads `__consumer_offsets` lazily, so the first
/// lookups can return `COORDINATOR_NOT_AVAILABLE` / `COORDINATOR_LOAD_IN_PROGRESS`
/// — those are retried with backoff, matching the Java client.
pub(super) async fn find_coordinator(wire: &WireClient, group_id: &str) -> Result<i32> {
    let request = FindCoordinatorRequestData {
        key_type: COORDINATOR_KEY_TYPE_GROUP,
        coordinator_keys: vec![KafkaString::from(group_id.to_owned())],
        ..FindCoordinatorRequestData::default()
    };
    let version = client_api_info(ApiKey::FindCoordinator).max_version;

    let mut attempts_remaining = FIND_COORDINATOR_MAX_ATTEMPTS;
    let coordinator = loop {
        let broker_id = wire.any_broker_id()?;
        let response: FindCoordinatorResponseData = wire
            .send_to_broker(broker_id, ApiKey::FindCoordinator, version, &request)
            .await?;
        let coordinator = response
            .coordinators
            .into_iter()
            .find(|coordinator| coordinator.key.to_string() == group_id)
            .ok_or_else(|| {
                ConsumerError::broker(
                    "find_coordinator",
                    ErrorCode::CoordinatorNotAvailable,
                    "coordinator response was missing the requested group",
                )
            })?;
        let error = ErrorCode::from(coordinator.error_code);
        if !error.is_error() {
            break coordinator;
        }
        if attempts_remaining == 0 || !error.is_retriable() {
            return Err(ConsumerError::broker(
                "find_coordinator",
                error,
                "group coordinator lookup failed",
            ));
        }
        attempts_remaining = attempts_remaining.saturating_sub(1);
        tokio::time::sleep(FIND_COORDINATOR_BACKOFF).await;
    };
    let port = u16::try_from(coordinator.port).map_err(|_error| {
        ConsumerError::broker(
            "find_coordinator",
            ErrorCode::CoordinatorNotAvailable,
            "coordinator returned an invalid port",
        )
    })?;
    let host = coordinator.host.to_string();
    let mut addresses = tokio::net::lookup_host((host.as_str(), port))
        .await
        .map_err(WireError::from)?;
    let addr = addresses.next().ok_or_else(|| {
        ConsumerError::broker(
            "find_coordinator",
            ErrorCode::CoordinatorNotAvailable,
            "coordinator host did not resolve",
        )
    })?;
    drop(addresses);
    wire.upsert_broker(BrokerEndpoint::from_resolved(
        coordinator.node_id,
        host,
        port,
        addr,
    ));
    Ok(coordinator.node_id)
}

/// Commit the given offsets for `group_id` to its coordinator, using the
/// "no active generation" values (generation `-1`, empty member) that a
/// manual-assignment consumer uses.
pub(super) async fn commit_offsets(
    wire: &WireClient,
    coordinator_id: i32,
    group_id: &str,
    offsets: &HashMap<TopicPartition, OffsetAndMetadata>,
) -> Result<()> {
    if offsets.is_empty() {
        return Ok(());
    }
    let request = OffsetCommitRequestData {
        group_id: group_id.to_owned().into(),
        generation_id_or_member_epoch: -1,
        member_id: KafkaString::default(),
        group_instance_id: None,
        retention_time_ms: -1,
        topics: commit_topics(offsets),
        _unknown_tagged_fields: Vec::new(),
    };
    let version = client_api_info(ApiKey::OffsetCommit)
        .max_version
        .min(OFFSET_COMMIT_MAX_VERSION);
    let response: OffsetCommitResponseData = wire
        .send_to_broker(coordinator_id, ApiKey::OffsetCommit, version, &request)
        .await?;
    for topic in response.topics {
        for partition in topic.partitions {
            let error = ErrorCode::from(partition.error_code);
            if error.is_error() {
                return Err(ConsumerError::broker(
                    "commit",
                    error,
                    format!(
                        "{}-{} offset commit failed",
                        topic.name.as_str(),
                        partition.partition_index
                    ),
                ));
            }
        }
    }
    Ok(())
}

/// Fetch committed offsets for `partitions` in `group_id`. Partitions with no
/// committed offset (broker returns `-1`) are omitted from the result.
pub(super) async fn fetch_committed(
    wire: &WireClient,
    coordinator_id: i32,
    group_id: &str,
    partitions: &[TopicPartition],
) -> Result<HashMap<TopicPartition, OffsetAndMetadata>> {
    if partitions.is_empty() {
        return Ok(HashMap::new());
    }
    let request = OffsetFetchRequestData {
        group_id: group_id.to_owned().into(),
        topics: Some(fetch_topics(partitions)),
        groups: Vec::new(),
        require_stable: false,
        _unknown_tagged_fields: Vec::new(),
    };
    let version = client_api_info(ApiKey::OffsetFetch)
        .max_version
        .min(OFFSET_FETCH_MAX_VERSION);
    let response: OffsetFetchResponseData = wire
        .send_to_broker(coordinator_id, ApiKey::OffsetFetch, version, &request)
        .await?;
    let top_level = ErrorCode::from(response.error_code);
    if top_level.is_error() {
        return Err(ConsumerError::broker(
            "committed",
            top_level,
            "offset fetch failed",
        ));
    }
    let mut committed = HashMap::new();
    for topic in response.topics {
        for partition in topic.partitions {
            let error = ErrorCode::from(partition.error_code);
            if error.is_error() {
                return Err(ConsumerError::broker(
                    "committed",
                    error,
                    format!(
                        "{}-{} offset fetch failed",
                        topic.name.as_str(),
                        partition.partition_index
                    ),
                ));
            }
            if partition.committed_offset < 0 {
                continue;
            }
            let mut metadata = OffsetAndMetadata::new(partition.committed_offset);
            if partition.committed_leader_epoch >= 0 {
                metadata = metadata.leader_epoch(partition.committed_leader_epoch);
            }
            if let Some(meta) = partition.metadata.filter(|meta| !meta.as_str().is_empty()) {
                metadata = metadata.metadata(meta.to_string());
            }
            let _previous = committed.insert(
                TopicPartition::new(topic.name.as_str().to_owned(), partition.partition_index),
                metadata,
            );
        }
    }
    Ok(committed)
}

fn commit_topics(
    offsets: &HashMap<TopicPartition, OffsetAndMetadata>,
) -> Vec<OffsetCommitRequestTopic> {
    let mut topics: Vec<OffsetCommitRequestTopic> = Vec::new();
    for (partition, offset) in offsets {
        let wire_partition = OffsetCommitRequestPartition {
            partition_index: partition.partition,
            committed_offset: offset.offset,
            committed_leader_epoch: offset.leader_epoch.unwrap_or(-1),
            committed_metadata: Some(offset.metadata.clone().unwrap_or_default().into()),
            _unknown_tagged_fields: Vec::new(),
        };
        if let Some(topic) = topics
            .iter_mut()
            .find(|topic| topic.name.as_str() == partition.topic)
        {
            topic.partitions.push(wire_partition);
        } else {
            topics.push(OffsetCommitRequestTopic {
                name: partition.topic.clone().into(),
                topic_id: kacrab_protocol::KafkaUuid::default(),
                partitions: vec![wire_partition],
                _unknown_tagged_fields: Vec::new(),
            });
        }
    }
    topics
}

fn fetch_topics(partitions: &[TopicPartition]) -> Vec<OffsetFetchRequestTopic> {
    let mut topics: Vec<OffsetFetchRequestTopic> = Vec::new();
    for partition in partitions {
        if let Some(topic) = topics
            .iter_mut()
            .find(|topic| topic.name.as_str() == partition.topic)
        {
            topic.partition_indexes.push(partition.partition);
        } else {
            topics.push(OffsetFetchRequestTopic {
                name: partition.topic.clone().into(),
                partition_indexes: vec![partition.partition],
                _unknown_tagged_fields: Vec::new(),
            });
        }
    }
    topics
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commit_topics_groups_partitions_by_topic() {
        let mut offsets = HashMap::new();
        let _p0 = offsets.insert(
            TopicPartition::new("a", 0),
            OffsetAndMetadata::new(5).leader_epoch(2),
        );
        let _p1 = offsets.insert(TopicPartition::new("a", 1), OffsetAndMetadata::new(6));
        let _p2 = offsets.insert(TopicPartition::new("b", 0), OffsetAndMetadata::new(7));
        let topics = commit_topics(&offsets);
        assert_eq!(topics.len(), 2);
        let a = topics.iter().find(|t| t.name.as_str() == "a").unwrap();
        assert_eq!(a.partitions.len(), 2);
        let epoch_for = |index: i32| {
            a.partitions
                .iter()
                .find(|p| p.partition_index == index)
                .map(|p| p.committed_leader_epoch)
        };
        assert_eq!(epoch_for(0), Some(2));
        assert_eq!(epoch_for(1), Some(-1));
    }

    #[test]
    fn fetch_topics_groups_partition_indexes_by_topic() {
        let partitions = vec![
            TopicPartition::new("a", 0),
            TopicPartition::new("a", 2),
            TopicPartition::new("b", 1),
        ];
        let topics = fetch_topics(&partitions);
        assert_eq!(topics.len(), 2);
        let a = topics.iter().find(|t| t.name.as_str() == "a").unwrap();
        assert_eq!(a.partition_indexes, vec![0, 2]);
    }
}
