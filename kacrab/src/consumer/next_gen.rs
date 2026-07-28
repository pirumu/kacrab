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
//! This module owns the wire RPC; the membership state it drives is the one
//! [`membership`](super::membership) also gives share groups. The
//! [`Consumer`](super::Consumer) facade drives reconciliation against its
//! subscription (it holds the positions and metadata).

use kacrab_protocol::{
    KafkaString,
    generated::{
        ApiKey, ConsumerGroupHeartbeatRequestData, ConsumerGroupHeartbeatResponseData, ErrorCode,
        consumer_group_heartbeat_request::TopicPartitions as OwnedTopicPartitions,
    },
    version::client_api_info,
};

use super::{
    error::Result,
    membership::{AssignedTopic, HeartbeatOutcome, heartbeat_interval},
};
use crate::wire::WireClient;

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

/// Send one `ConsumerGroupHeartbeat` to the coordinator and parse the response.
///
/// Fencing (`FENCED_MEMBER_EPOCH`/`UNKNOWN_MEMBER_ID`) and coordinator-availability
/// codes are returned in [`HeartbeatOutcome::error`] for the caller to recover
/// from; only unexpected fatal codes become a
/// [`ConsumerError`](super::error::ConsumerError).
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
    // The ceiling kacrab speaks, not the version sent: the connection resolves it
    // against the broker's advertised `ApiVersions` range before framing, so a
    // Kafka 3.9 cluster serving only v0 (KIP-848 early access) gets a v0 request.
    let version = client_api_info(ApiKey::ConsumerGroupHeartbeat).max_version;
    let response: ConsumerGroupHeartbeatResponseData = wire
        .send_to_broker(
            coordinator_id,
            ApiKey::ConsumerGroupHeartbeat,
            version,
            &wire_request,
        )
        .await?;
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
    Ok(HeartbeatOutcome {
        member_id: response.member_id.map(|id| id.to_string()),
        member_epoch: response.member_epoch,
        heartbeat_interval: heartbeat_interval(response.heartbeat_interval_ms),
        assignment,
        error: ErrorCode::from(response.error_code),
    })
}
