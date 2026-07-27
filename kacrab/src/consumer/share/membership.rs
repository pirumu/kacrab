//! Share-group membership: the `ShareGroupHeartbeat` RPC (KIP-932).
//!
//! Membership works like the KIP-848 consumer protocol in
//! [`next_gen`](crate::consumer::next_gen): the client generates its own member
//! id, keeps it for the process lifetime, and sends a single heartbeat RPC that
//! carries the subscription and returns the coordinator-computed assignment.
//!
//! Two things differ. There is no client-side assignor at all — share groups
//! have no `partition.assignment.strategy` analogue and no `server_assignor`
//! field — and the heartbeat never reports an *owned* set, because share-group
//! assignment is not exclusive: the whole point is that several members may hold
//! the same partition at once, each acquiring a disjoint set of its records. So
//! there is nothing to reconcile incrementally; the target assignment is simply
//! adopted.

use std::time::Duration;

use kacrab_protocol::{
    KafkaString, KafkaUuid,
    generated::{
        ApiKey, ErrorCode, ShareGroupHeartbeatRequestData, ShareGroupHeartbeatResponseData,
    },
    version::client_api_info,
};

use crate::{
    consumer::{
        error::{ConsumerError, Result},
        next_gen::AssignedTopic,
    },
    wire::WireClient,
};

/// Member epoch sent to join a fresh share group.
pub(super) const EPOCH_JOINING: i32 = 0;
/// Member epoch sent to leave the share group.
pub(super) const EPOCH_LEAVING: i32 = -1;

/// The membership state a share consumer keeps for the lifetime of the process.
#[derive(Debug, Clone)]
pub(super) struct ShareGroupState {
    /// Client-generated member id, kept for the whole consumer lifetime. It is
    /// also the share-session identity in every `ShareFetch`/`ShareAcknowledge`,
    /// so the broker rejects anything that is not a Kafka UUID string.
    pub member_id: String,
    /// Current member epoch (`0` before the first heartbeat is acknowledged).
    pub member_epoch: i32,
    /// Heartbeat cadence the coordinator asked for.
    pub heartbeat_interval: Duration,
}

impl ShareGroupState {
    /// Start fresh membership with a new client-generated member id.
    pub(super) fn new(default_interval: Duration) -> Result<Self> {
        let member_id = KafkaUuid::random()
            .map_err(|_error| {
                ConsumerError::InvalidState("could not generate a share consumer member id")
            })?
            .to_string();
        Ok(Self {
            member_id,
            member_epoch: EPOCH_JOINING,
            heartbeat_interval: default_interval,
        })
    }
}

/// The per-heartbeat inputs beyond the routing context.
pub(super) struct ShareHeartbeatRequest<'a> {
    pub group_id: &'a str,
    pub member_id: &'a str,
    pub member_epoch: i32,
    pub rack_id: Option<&'a str>,
    pub subscribed_topics: &'a [String],
}

/// The parsed outcome of one `ShareGroupHeartbeat`.
#[derive(Debug)]
pub(super) struct ShareHeartbeatOutcome {
    /// The member id the coordinator echoed, when it sent one.
    pub member_id: Option<String>,
    /// The new member epoch.
    pub member_epoch: i32,
    /// The heartbeat cadence the coordinator asked for.
    pub heartbeat_interval: Duration,
    /// The target assignment, when the coordinator sent one this round.
    pub assignment: Option<Vec<AssignedTopic>>,
    /// The top-level error code. Fencing and coordinator-availability signals
    /// are surfaced here rather than turned into hard errors, so the caller can
    /// rejoin.
    pub error: ErrorCode,
}

/// Send one `ShareGroupHeartbeat` to the coordinator and parse the response.
///
/// # Errors
/// Returns a wire error when the RPC itself fails. Broker error codes are
/// reported in [`ShareHeartbeatOutcome::error`], never as an `Err`.
pub(super) async fn heartbeat(
    wire: &WireClient,
    coordinator_id: i32,
    request: &ShareHeartbeatRequest<'_>,
) -> Result<ShareHeartbeatOutcome> {
    let wire_request = ShareGroupHeartbeatRequestData {
        group_id: request.group_id.to_owned().into(),
        member_id: request.member_id.to_owned().into(),
        member_epoch: request.member_epoch,
        rack_id: request
            .rack_id
            .filter(|rack| !rack.is_empty())
            .map(|rack| rack.to_owned().into()),
        subscribed_topic_names: Some(
            request
                .subscribed_topics
                .iter()
                .map(|topic| KafkaString::from(topic.clone()))
                .collect(),
        ),
        _unknown_tagged_fields: Vec::new(),
    };
    let version = client_api_info(ApiKey::ShareGroupHeartbeat).max_version;
    let response: ShareGroupHeartbeatResponseData = wire
        .send_to_broker(
            coordinator_id,
            ApiKey::ShareGroupHeartbeat,
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
    let interval = u64::try_from(response.heartbeat_interval_ms.max(0)).unwrap_or(0);
    Ok(ShareHeartbeatOutcome {
        member_id: response.member_id.map(|id| id.to_string()),
        member_epoch: response.member_epoch,
        heartbeat_interval: Duration::from_millis(interval),
        assignment,
        error: ErrorCode::from(response.error_code),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_state_generates_a_member_id_and_joins_at_epoch_zero() {
        let state = ShareGroupState::new(Duration::from_secs(5)).expect("member id");
        assert!(!state.member_id.is_empty());
        assert_eq!(state.member_epoch, EPOCH_JOINING);
        assert_eq!(state.heartbeat_interval, Duration::from_secs(5));

        let other = ShareGroupState::new(Duration::from_secs(5)).expect("member id");
        assert_ne!(state.member_id, other.member_id);
    }

    #[test]
    fn the_member_id_is_a_kafka_uuid_string_the_broker_accepts() {
        let state = ShareGroupState::new(Duration::from_secs(5)).expect("member id");
        // `KafkaApis.isMemberIdValid` rejects an empty id and anything longer
        // than a human-readable UUID, and the share-session code parses it back
        // with `Uuid.fromString`.
        assert!(!state.member_id.is_empty());
        assert!(state.member_id.len() <= 36);
        let parsed = KafkaUuid::from_base64(&state.member_id).expect("round-trips");
        assert_eq!(parsed.to_string(), state.member_id);
    }
}
