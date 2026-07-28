//! Share-group membership: the `ShareGroupHeartbeat` RPC (KIP-932).
//!
//! Membership works like the KIP-848 consumer protocol in
//! [`next_gen`](crate::consumer::next_gen), and the two share their state,
//! epoch sentinels and parsed outcome — see
//! [`membership`](crate::consumer::membership). The client generates its own
//! member id, keeps it for the process lifetime, and sends a single heartbeat RPC
//! that carries the subscription and returns the coordinator-computed assignment.
//!
//! Two things differ. There is no client-side assignor at all — share groups
//! have no `partition.assignment.strategy` analogue and no `server_assignor`
//! field — and the heartbeat never reports an *owned* set, because share-group
//! assignment is not exclusive: the whole point is that several members may hold
//! the same partition at once, each acquiring a disjoint set of its records. So
//! there is nothing to reconcile incrementally; the target assignment is simply
//! adopted.

use kacrab_protocol::{
    KafkaString,
    generated::{
        ApiKey, ErrorCode, ShareGroupHeartbeatRequestData, ShareGroupHeartbeatResponseData,
    },
    version::client_api_info,
};

use crate::{
    consumer::{
        error::Result,
        membership::{AssignedTopic, HeartbeatOutcome, heartbeat_interval},
    },
    wire::WireClient,
};

/// The per-heartbeat inputs beyond the routing context.
pub(super) struct ShareHeartbeatRequest<'a> {
    pub group_id: &'a str,
    pub member_id: &'a str,
    pub member_epoch: i32,
    pub rack_id: Option<&'a str>,
    pub subscribed_topics: &'a [String],
}

/// Send one `ShareGroupHeartbeat` to the coordinator and parse the response.
///
/// # Errors
/// Returns a wire error when the RPC itself fails. Broker error codes are
/// reported in [`HeartbeatOutcome::error`], never as an `Err`.
pub(super) async fn heartbeat(
    wire: &WireClient,
    coordinator_id: i32,
    request: &ShareHeartbeatRequest<'_>,
) -> Result<HeartbeatOutcome> {
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
    // The ceiling kacrab speaks, not the version sent: the connection resolves it
    // against the broker's advertised `ApiVersions` range before framing.
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
    Ok(HeartbeatOutcome {
        member_id: response.member_id.map(|id| id.to_string()),
        member_epoch: response.member_epoch,
        heartbeat_interval: heartbeat_interval(response.heartbeat_interval_ms),
        assignment,
        error: ErrorCode::from(response.error_code),
    })
}
