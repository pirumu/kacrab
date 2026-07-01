//! The KIP-848 consumer group protocol (`group.protocol=consumer`).
//!
//! Where the classic protocol has the group leader compute assignments over
//! `JoinGroup`/`SyncGroup`, KIP-848 moves assignment to the group coordinator and
//! collapses membership into a single `ConsumerGroupHeartbeat` RPC. The client
//! generates its own member id, reports its subscribed topics and currently owned
//! partitions on each heartbeat, and reconciles toward the target assignment the
//! coordinator sends back (incrementally, like cooperative rebalancing, so a
//! partition is never double-owned). Assignments are keyed by topic id, resolved
//! to names against cluster metadata.
//!
//! This module owns the wire RPC and the small membership-state machine; the
//! [`Consumer`](super::Consumer) facade drives reconciliation against its
//! subscription (it holds the positions and metadata).

use std::time::Duration;

use kacrab_protocol::{
    KafkaString, KafkaUuid,
    generated::{
        ApiKey, ConsumerGroupHeartbeatRequestData, ConsumerGroupHeartbeatResponseData, ErrorCode,
        consumer_group_heartbeat_request::TopicPartitions as OwnedTopicPartitions,
    },
    version::client_api_info,
};

use super::error::{ConsumerError, Result};
use crate::wire::WireClient;

/// Member epoch sent to join a fresh group.
pub(super) const EPOCH_JOINING: i32 = 0;
/// Member epoch sent to leave the group.
pub(super) const EPOCH_LEAVING: i32 = -1;

/// The membership state a KIP-848 consumer keeps for the lifetime of the process.
#[derive(Debug, Clone)]
pub(super) struct ModernGroupState {
    /// Client-generated member id, kept for the whole consumer lifetime.
    pub member_id: String,
    /// Current member epoch (`0` before the first heartbeat is acknowledged).
    pub member_epoch: i32,
    /// Heartbeat cadence the coordinator asked for.
    pub heartbeat_interval: Duration,
}

impl ModernGroupState {
    /// Start fresh membership with a new client-generated member id.
    pub(super) fn new(default_interval: Duration) -> Result<Self> {
        let member_id = KafkaUuid::random()
            .map_err(|_error| {
                ConsumerError::InvalidState("could not generate a consumer member id")
            })?
            .to_string();
        Ok(Self {
            member_id,
            member_epoch: EPOCH_JOINING,
            heartbeat_interval: default_interval,
        })
    }
}

/// One topic's partitions in an assignment or owned set, keyed by topic id.
#[derive(Debug, Clone)]
pub(super) struct AssignedTopic {
    pub topic_id: KafkaUuid,
    pub partitions: Vec<i32>,
}

/// The per-heartbeat inputs beyond the routing context.
pub(super) struct HeartbeatRequest<'a> {
    pub group_id: &'a str,
    pub member_id: &'a str,
    pub member_epoch: i32,
    pub instance_id: Option<&'a str>,
    pub rack_id: Option<&'a str>,
    pub rebalance_timeout_ms: i32,
    pub subscribed_topics: &'a [String],
    pub server_assignor: Option<&'a str>,
    pub owned: &'a [AssignedTopic],
}

/// The parsed outcome of one `ConsumerGroupHeartbeat`.
#[derive(Debug)]
pub(super) struct HeartbeatOutcome {
    /// The member id the coordinator echoed (may replace ours at v0).
    pub member_id: Option<String>,
    /// The new member epoch.
    pub member_epoch: i32,
    /// The heartbeat cadence the coordinator asked for.
    pub heartbeat_interval: Duration,
    /// The target assignment, when the coordinator sent one this round.
    pub assignment: Option<Vec<AssignedTopic>>,
    /// The top-level error code (fencing/coordinator signals are surfaced, not
    /// turned into hard errors, so the caller can rejoin).
    pub error: ErrorCode,
}

/// Send one `ConsumerGroupHeartbeat` to the coordinator and parse the response.
///
/// Fencing (`FENCED_MEMBER_EPOCH`/`UNKNOWN_MEMBER_ID`) and coordinator-availability
/// codes are returned in [`HeartbeatOutcome::error`] for the caller to recover
/// from; only unexpected fatal codes become a [`ConsumerError`].
pub(super) async fn heartbeat(
    wire: &WireClient,
    coordinator_id: i32,
    request: &HeartbeatRequest<'_>,
) -> Result<HeartbeatOutcome> {
    let owned = request
        .owned
        .iter()
        .map(|topic| OwnedTopicPartitions {
            topic_id: topic.topic_id,
            partitions: topic.partitions.clone(),
            _unknown_tagged_fields: Vec::new(),
        })
        .collect();
    let wire_request = ConsumerGroupHeartbeatRequestData {
        group_id: request.group_id.to_owned().into(),
        member_id: request.member_id.to_owned().into(),
        member_epoch: request.member_epoch,
        instance_id: request
            .instance_id
            .filter(|id| !id.is_empty())
            .map(|id| id.to_owned().into()),
        rack_id: request
            .rack_id
            .filter(|rack| !rack.is_empty())
            .map(|rack| rack.to_owned().into()),
        rebalance_timeout_ms: request.rebalance_timeout_ms,
        subscribed_topic_names: Some(
            request
                .subscribed_topics
                .iter()
                .map(|topic| KafkaString::from(topic.clone()))
                .collect(),
        ),
        subscribed_topic_regex: None,
        server_assignor: request
            .server_assignor
            .map(|assignor| assignor.to_owned().into()),
        topic_partitions: Some(owned),
        _unknown_tagged_fields: Vec::new(),
    };
    let version = client_api_info(ApiKey::ConsumerGroupHeartbeat).max_version;
    let response: ConsumerGroupHeartbeatResponseData = wire
        .send_to_broker(
            coordinator_id,
            ApiKey::ConsumerGroupHeartbeat,
            version,
            &wire_request,
        )
        .await?;
    let error = ErrorCode::from(response.error_code);
    let assignment = response.assignment.map(|assignment| {
        assignment
            .topic_partitions
            .into_iter()
            .map(|topic| AssignedTopic {
                topic_id: topic.topic_id,
                partitions: topic.partitions,
            })
            .collect()
    });
    let interval = u64::try_from(response.heartbeat_interval_ms.max(0)).unwrap_or(0);
    Ok(HeartbeatOutcome {
        member_id: response.member_id.map(|id| id.to_string()),
        member_epoch: response.member_epoch,
        heartbeat_interval: Duration::from_millis(interval),
        assignment,
        error,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_state_generates_a_member_id_and_joins_at_epoch_zero() {
        let state = ModernGroupState::new(Duration::from_secs(3)).expect("member id");
        assert!(!state.member_id.is_empty());
        assert_eq!(state.member_epoch, EPOCH_JOINING);
        assert_eq!(state.heartbeat_interval, Duration::from_secs(3));
        // Two members get distinct ids.
        let other = ModernGroupState::new(Duration::from_secs(3)).expect("member id");
        assert_ne!(state.member_id, other.member_id);
    }
}
