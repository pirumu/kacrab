//! Group coordinator lookup, offset commit/fetch, and classic group membership.
//!
//! Finds the group coordinator (`FindCoordinator`), commits and reads committed
//! offsets (`OffsetCommit`/`OffsetFetch`, carrying the member identity), and runs
//! the classic membership RPCs (`JoinGroup`/`SyncGroup`/`Heartbeat`/`LeaveGroup`).
//! A manual-assignment consumer that carries a `group.id` can commit/fetch offsets
//! without joining. `OffsetCommit` is capped at v9 and `OffsetFetch` at v7 so both
//! stay topic-*name* keyed, sidestepping the v10/v8 topic-id strict-codec forms
//! (the same trap seen on the admin offset paths). The KIP-848 server-side
//! protocol lives in `next_gen`.

use std::collections::HashMap;

use bytes::Bytes;
use kacrab_protocol::{
    KafkaString,
    generated::{
        ApiKey, ErrorCode, FindCoordinatorRequestData, FindCoordinatorResponseData,
        HeartbeatRequestData, HeartbeatResponseData, JoinGroupRequestData, JoinGroupResponseData,
        LeaveGroupRequestData, LeaveGroupResponseData, OffsetCommitRequestData,
        OffsetCommitRequestPartition, OffsetCommitRequestTopic, OffsetCommitResponseData,
        OffsetFetchRequestData, OffsetFetchRequestTopic, OffsetFetchResponseData,
        SyncGroupRequestData, SyncGroupResponseData, join_group_request::JoinGroupRequestProtocol,
        sync_group_request::SyncGroupRequestAssignment,
    },
    version::client_api_info,
};

use super::{
    assignor::{self, MemberSubscription},
    error::{ConsumerError, Result},
};
use crate::{
    common::{OffsetAndMetadata, TopicPartition},
    wire::{BackoffPolicy, BackoffState, BrokerEndpoint, WireClient, WireError},
};

/// `FindCoordinator` key type for a consumer group coordinator.
const COORDINATOR_KEY_TYPE_GROUP: i8 = 0;
/// Highest `OffsetCommit` version to negotiate; v10 switched to topic ids.
const OFFSET_COMMIT_MAX_VERSION: i16 = 9;
/// Highest `OffsetFetch` version to negotiate; v8 switched to the `groups` form.
const OFFSET_FETCH_MAX_VERSION: i16 = 7;
/// Max `FindCoordinator` attempts while the coordinator is loading.
const FIND_COORDINATOR_MAX_ATTEMPTS: u32 = 20;

/// Resolve the group coordinator, register its endpoint, and return its node id.
///
/// A freshly started broker loads `__consumer_offsets` lazily, so the first
/// lookups can return `COORDINATOR_NOT_AVAILABLE` / `COORDINATOR_LOAD_IN_PROGRESS`
/// — those are retried under `retry_backoff` (exponential `retry.backoff.ms` →
/// `retry.backoff.max.ms` with jitter, Java's `AbstractCoordinator` policy).
pub(super) async fn find_coordinator(
    wire: &WireClient,
    group_id: &str,
    retry_backoff: BackoffPolicy,
) -> Result<i32> {
    let request = FindCoordinatorRequestData {
        key_type: COORDINATOR_KEY_TYPE_GROUP,
        coordinator_keys: vec![KafkaString::from(group_id.to_owned())],
        ..FindCoordinatorRequestData::default()
    };
    let version = client_api_info(ApiKey::FindCoordinator).max_version;

    let mut attempts_remaining = FIND_COORDINATOR_MAX_ATTEMPTS;
    let mut backoff = BackoffState::new(retry_backoff);
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
        tokio::time::sleep(backoff.next_delay()?).await;
    };
    let port = u16::try_from(coordinator.port).map_err(|_error| {
        ConsumerError::broker(
            "find_coordinator",
            ErrorCode::CoordinatorNotAvailable,
            "coordinator returned an invalid port",
        )
    })?;
    // The wire re-resolves the host (IPv4-first, honoring `client.dns.lookup`)
    // when it connects, so this seed address is only a fallback.
    let host = coordinator.host.to_string();
    let addr = tokio::net::lookup_host((host.as_str(), port))
        .await
        .map_err(WireError::from)?
        .next()
        .ok_or_else(|| {
            ConsumerError::broker(
                "find_coordinator",
                ErrorCode::CoordinatorNotAvailable,
                "coordinator host did not resolve",
            )
        })?;
    wire.upsert_broker(BrokerEndpoint::from_resolved(
        coordinator.node_id,
        host,
        port,
        addr,
    ));
    Ok(coordinator.node_id)
}

/// Who is committing: the coordinator to route to, the group, and the member
/// identity. A manual-assignment consumer uses generation `-1` and an empty
/// member id; a group member (classic or KIP-848) uses its generation/epoch and
/// member id, which the coordinator requires.
#[derive(Debug, Clone, Copy)]
#[expect(
    clippy::struct_field_names,
    reason = "These are the Kafka commit-identity fields; the shared suffix is theirs."
)]
pub(super) struct CommitTarget<'a> {
    pub coordinator_id: i32,
    pub group_id: &'a str,
    pub generation_id: i32,
    pub member_id: &'a str,
}

/// Commit the given offsets for a group to its coordinator with the member
/// identity in `target`.
pub(super) async fn commit_offsets(
    wire: &WireClient,
    target: &CommitTarget<'_>,
    offsets: &HashMap<TopicPartition, OffsetAndMetadata>,
) -> Result<()> {
    if offsets.is_empty() {
        return Ok(());
    }
    let request = OffsetCommitRequestData {
        group_id: target.group_id.to_owned().into(),
        generation_id_or_member_epoch: target.generation_id,
        member_id: target.member_id.to_owned().into(),
        group_instance_id: None,
        retention_time_ms: -1,
        topics: commit_topics(offsets),
        _unknown_tagged_fields: Vec::new(),
    };
    let version = client_api_info(ApiKey::OffsetCommit)
        .max_version
        .min(OFFSET_COMMIT_MAX_VERSION);
    let response: OffsetCommitResponseData = wire
        .send_to_broker(
            target.coordinator_id,
            ApiKey::OffsetCommit,
            version,
            &request,
        )
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

/// Highest `LeaveGroup` version to negotiate; v3+ moves to the batch `members`
/// form, so v2 keeps the single top-level `member_id`.
const LEAVE_GROUP_MAX_VERSION: i16 = 2;

/// The routing context shared by group-membership RPCs: the wire client, the
/// coordinator broker id, and the group id.
#[derive(Debug, Clone, Copy)]
pub(super) struct GroupContext<'a> {
    pub wire: &'a WireClient,
    pub coordinator_id: i32,
    pub group_id: &'a str,
    /// Static membership id (`group.instance.id`), or `None`.
    pub group_instance_id: Option<&'a str>,
}

/// Convert a static-membership id into the wire's optional `KafkaString`.
fn instance_id(group_instance_id: Option<&str>) -> Option<KafkaString> {
    group_instance_id
        .filter(|id| !id.is_empty())
        .map(|id| id.to_owned().into())
}

/// The outcome of a `JoinGroup` round.
#[derive(Debug)]
pub(super) struct JoinResult {
    /// The group generation this member joined.
    pub generation_id: i32,
    /// The member id the coordinator assigned (or echoed).
    pub member_id: String,
    /// The assignor protocol the coordinator selected for this generation.
    pub protocol_name: String,
    /// Whether this member is the group leader (runs the assignor).
    pub leader: bool,
    /// Decoded member subscriptions — populated only for the leader.
    pub members: Vec<MemberSubscription>,
}

/// The per-round `JoinGroup` inputs beyond the routing context.
pub(super) struct JoinRequest<'a> {
    pub member_id: &'a str,
    pub session_timeout_ms: i32,
    pub rebalance_timeout_ms: i32,
    pub topics: &'a [String],
    pub assignors: &'a [&'a str],
    /// Partitions this member currently owns (for the cooperative assignor).
    pub owned: &'a [TopicPartition],
}

/// Drive the classic `JoinGroup` handshake to completion, retrying as needed: it
/// adopts the coordinator-assigned member id on `MEMBER_ID_REQUIRED`, and resets
/// to a fresh member id on `UNKNOWN_MEMBER_ID`/`ILLEGAL_GENERATION`, looping until
/// the broker accepts the join or returns another error.
pub(super) async fn join_group(
    context: &GroupContext<'_>,
    join: &JoinRequest<'_>,
) -> Result<JoinResult> {
    let metadata = assignor::encode_subscription(join.topics, join.owned);
    let protocols: Vec<JoinGroupRequestProtocol> = join
        .assignors
        .iter()
        .map(|name| JoinGroupRequestProtocol {
            name: (*name).to_owned().into(),
            metadata: metadata.clone(),
            _unknown_tagged_fields: Vec::new(),
        })
        .collect();
    let version = client_api_info(ApiKey::JoinGroup).max_version;
    let mut member_id = join.member_id.to_owned();
    loop {
        let request = JoinGroupRequestData {
            group_id: context.group_id.to_owned().into(),
            session_timeout_ms: join.session_timeout_ms,
            rebalance_timeout_ms: join.rebalance_timeout_ms,
            member_id: member_id.clone().into(),
            group_instance_id: instance_id(context.group_instance_id),
            protocol_type: assignor::PROTOCOL_TYPE.to_owned().into(),
            protocols: protocols.clone(),
            reason: None,
            _unknown_tagged_fields: Vec::new(),
        };
        let response: JoinGroupResponseData = context
            .wire
            .send_to_broker(context.coordinator_id, ApiKey::JoinGroup, version, &request)
            .await?;
        let error = ErrorCode::from(response.error_code);
        if error == ErrorCode::MemberIdRequired {
            member_id = response.member_id.to_string();
            continue;
        }
        // A fenced member id / stale generation self-heals: drop the id and let
        // the broker assign a fresh one on the retry.
        if matches!(
            error,
            ErrorCode::UnknownMemberId | ErrorCode::IllegalGeneration
        ) {
            member_id = String::new();
            continue;
        }
        if error.is_error() {
            return Err(ConsumerError::broker(
                "join_group",
                error,
                "join group failed",
            ));
        }
        let leader = response.leader.as_str() == response.member_id.as_str();
        let members = if leader {
            response
                .members
                .into_iter()
                .map(|member| MemberSubscription {
                    member_id: member.member_id.to_string(),
                    topics: assignor::decode_subscription(&member.metadata),
                    owned: assignor::decode_owned(&member.metadata),
                })
                .collect()
        } else {
            Vec::new()
        };
        return Ok(JoinResult {
            generation_id: response.generation_id,
            member_id: response.member_id.to_string(),
            protocol_name: response
                .protocol_name
                .map(|name| name.to_string())
                .unwrap_or_default(),
            leader,
            members,
        });
    }
}

/// Send `SyncGroup` (with the leader's computed assignments, or empty for a
/// follower) and return this member's assigned partitions.
pub(super) async fn sync_group(
    context: &GroupContext<'_>,
    generation_id: i32,
    member_id: &str,
    protocol_name: &str,
    assignments: Vec<(String, Bytes)>,
) -> Result<Vec<TopicPartition>> {
    let request = SyncGroupRequestData {
        group_id: context.group_id.to_owned().into(),
        generation_id,
        member_id: member_id.to_owned().into(),
        group_instance_id: instance_id(context.group_instance_id),
        protocol_type: Some(assignor::PROTOCOL_TYPE.to_owned().into()),
        protocol_name: Some(protocol_name.to_owned().into()),
        assignments: assignments
            .into_iter()
            .map(|(member, assignment)| SyncGroupRequestAssignment {
                member_id: member.into(),
                assignment,
                _unknown_tagged_fields: Vec::new(),
            })
            .collect(),
        _unknown_tagged_fields: Vec::new(),
    };
    let version = client_api_info(ApiKey::SyncGroup).max_version;
    let response: SyncGroupResponseData = context
        .wire
        .send_to_broker(context.coordinator_id, ApiKey::SyncGroup, version, &request)
        .await?;
    let error = ErrorCode::from(response.error_code);
    if error.is_error() {
        return Err(ConsumerError::broker(
            "sync_group",
            error,
            "sync group failed",
        ));
    }
    Ok(assignor::decode_assignment(&response.assignment))
}

/// Send one `Heartbeat` and return the broker error code (`NONE` when healthy;
/// `REBALANCE_IN_PROGRESS`/`ILLEGAL_GENERATION`/`UNKNOWN_MEMBER_ID` signal a
/// rejoin).
pub(super) async fn heartbeat(
    context: &GroupContext<'_>,
    generation_id: i32,
    member_id: &str,
) -> Result<ErrorCode> {
    let request = HeartbeatRequestData {
        group_id: context.group_id.to_owned().into(),
        generation_id,
        member_id: member_id.to_owned().into(),
        group_instance_id: instance_id(context.group_instance_id),
        _unknown_tagged_fields: Vec::new(),
    };
    let version = client_api_info(ApiKey::Heartbeat).max_version;
    let response: HeartbeatResponseData = context
        .wire
        .send_to_broker(context.coordinator_id, ApiKey::Heartbeat, version, &request)
        .await?;
    Ok(ErrorCode::from(response.error_code))
}

/// Best-effort `LeaveGroup` on close; failures are ignored.
pub(super) async fn leave_group(
    wire: &WireClient,
    coordinator_id: i32,
    group_id: &str,
    member_id: &str,
) {
    if member_id.is_empty() {
        return;
    }
    let request = LeaveGroupRequestData {
        group_id: group_id.to_owned().into(),
        member_id: member_id.to_owned().into(),
        members: Vec::new(),
        _unknown_tagged_fields: Vec::new(),
    };
    let version = client_api_info(ApiKey::LeaveGroup)
        .max_version
        .min(LEAVE_GROUP_MAX_VERSION);
    let _outcome: Result<LeaveGroupResponseData> = wire
        .send_to_broker(coordinator_id, ApiKey::LeaveGroup, version, &request)
        .await
        .map_err(Into::into);
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
