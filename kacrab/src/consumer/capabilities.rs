//! Broker capability gates for the consuming modes that need a modern broker.
//!
//! The classic `JoinGroup`/`SyncGroup` protocol works on every broker kacrab
//! supports, but KIP-848 (`group.protocol=consumer`) and KIP-932 (share groups)
//! each depend on APIs a broker may simply not serve. Without a gate the client
//! discovers that from the first feature RPC, which is both late — a coordinator
//! lookup and, for the consumer protocol, a metadata refresh have already gone
//! out — and unhelpful: the resulting error names the API but not the mode that
//! asked for it or the mode that would have worked.
//!
//! These checks run against the negotiated `ApiVersions` data and nothing else.
//! No wire version is written down here and no broker release is named, because
//! release numbers are not the truth: Kafka 3.9 advertises
//! `ConsumerGroupHeartbeat` v0 under KIP-848 early access, so "needs 4.0 or
//! newer" would refuse a broker that works. What the broker advertises is the
//! only thing that decides.

use kacrab_protocol::generated::ApiKey;

use super::error::{ConsumerError, Result};
use crate::wire::{BrokerCapabilities, WireClient};

/// The API a KIP-848 group member cannot do without: it is the whole membership
/// protocol, replacing `JoinGroup`/`SyncGroup`/`Heartbeat` with one RPC.
const CONSUMER_PROTOCOL_APIS: [ApiKey; 1] = [ApiKey::ConsumerGroupHeartbeat];

/// The APIs a KIP-932 share-group member cannot do without: membership, the
/// acquiring fetch, and the acknowledgement that releases what it acquired.
#[cfg(feature = "share-consumer")]
const SHARE_GROUP_APIS: [ApiKey; 3] = [
    ApiKey::ShareGroupHeartbeat,
    ApiKey::ShareFetch,
    ApiKey::ShareAcknowledge,
];

/// Refuse `group.protocol=consumer` on a broker that cannot serve it, naming the
/// classic protocol as the way to consume from this cluster today.
pub(super) fn require_consumer_protocol(wire: &WireClient, coordinator: i32) -> Result<()> {
    require(
        wire,
        coordinator,
        &CONSUMER_PROTOCOL_APIS,
        "group.protocol=consumer",
        "set group.protocol=classic to use the JoinGroup/SyncGroup protocol this broker does serve",
    )
}

/// Refuse a share group on a broker that cannot serve it, naming the ordinary
/// consumer as the way to consume from this cluster today.
#[cfg(feature = "share-consumer")]
pub(super) fn require_share_group(wire: &WireClient, coordinator: i32) -> Result<()> {
    require(
        wire,
        coordinator,
        &SHARE_GROUP_APIS,
        "the share-group protocol (KIP-932)",
        "use a Consumer with a classic or consumer group, or enable share groups on the broker",
    )
}

/// Check `apis` against what the cluster advertised, reporting the first one it
/// cannot serve.
///
/// A `None` capability set means no connection has finished its `ApiVersions`
/// handshake yet, so there is nothing to judge against. That is not a licence to
/// refuse: the caller proceeds and the RPC path reports whatever it finds. In
/// practice the gate is called after the coordinator lookup, which cannot
/// succeed without a handshake, so the `None` arm is a guard rather than a
/// routine outcome.
fn require(
    wire: &WireClient,
    coordinator: i32,
    apis: &[ApiKey],
    mode: &'static str,
    fallback: &'static str,
) -> Result<()> {
    let Some(capabilities) = wire.negotiated_capabilities(coordinator) else {
        return Ok(());
    };
    first_unsupported(&capabilities, apis).map_or(Ok(()), |source| {
        Err(ConsumerError::UnsupportedGroupProtocol {
            mode,
            fallback,
            source,
        })
    })
}

/// The negotiation failure for the first API in `apis` the broker cannot serve,
/// or `None` when it can serve all of them.
///
/// Reuses the wire layer's own resolution so the reported ranges are exactly the
/// ones the request path would have used — a mode is unusable both when the
/// broker never advertised the API and when the advertised range does not
/// overlap what kacrab speaks, and the error already distinguishes the two.
fn first_unsupported(
    capabilities: &BrokerCapabilities,
    apis: &[ApiKey],
) -> Option<crate::wire::WireError> {
    apis.iter().find_map(|&api_key| {
        capabilities
            .resolve_version_for_limit(api_key, i16::MAX)
            .err()
    })
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::expect_used,
        clippy::missing_assert_message,
        reason = "Unit test fixtures fail fastest with contextual expect calls."
    )]

    use kacrab_protocol::generated::{ApiKey, ApiVersion, ApiVersionsResponseData};

    use super::{BrokerCapabilities, CONSUMER_PROTOCOL_APIS, first_unsupported};
    use crate::wire::WireError;

    fn capabilities(advertised: &[(ApiKey, i16, i16)]) -> BrokerCapabilities {
        BrokerCapabilities::from_response(&ApiVersionsResponseData {
            api_keys: advertised
                .iter()
                .map(|&(api_key, min_version, max_version)| ApiVersion {
                    api_key: api_key as i16,
                    min_version,
                    max_version,
                    _unknown_tagged_fields: Vec::new(),
                })
                .collect(),
            ..ApiVersionsResponseData::default()
        })
    }

    /// Kafka 3.9 ships `ConsumerGroupHeartbeat` at v0 only (KIP-848 early
    /// access) while kacrab speaks up to v1. The gate must let that through:
    /// judging on the advertised range is the whole point, and a release-number
    /// rule ("4.0 or newer") would refuse a broker that serves the protocol.
    #[test]
    fn a_broker_advertising_only_the_early_access_heartbeat_is_accepted() {
        let early_access = capabilities(&[(ApiKey::ConsumerGroupHeartbeat, 0, 0)]);

        assert!(first_unsupported(&early_access, &CONSUMER_PROTOCOL_APIS).is_none());
    }

    /// Absent key and disjoint range both make the mode unusable, and the two
    /// need different fixes — so the gate must forward the distinction the wire
    /// error already draws rather than flattening it.
    #[test]
    fn an_absent_or_disjoint_heartbeat_is_reported_with_the_broker_range() {
        let no_kip_848 = capabilities(&[(ApiKey::Metadata, 0, 12)]);
        let error = first_unsupported(&no_kip_848, &CONSUMER_PROTOCOL_APIS)
            .expect("a broker without ConsumerGroupHeartbeat cannot serve KIP-848");
        assert!(matches!(
            error,
            WireError::UnsupportedApiVersion {
                api_key: ApiKey::ConsumerGroupHeartbeat,
                broker: None,
                ..
            }
        ));

        // A broker whose floor is above everything kacrab speaks: the API is
        // advertised, so the range has to travel with the error.
        let too_new = capabilities(&[(ApiKey::ConsumerGroupHeartbeat, 9, 9)]);
        let error = first_unsupported(&too_new, &CONSUMER_PROTOCOL_APIS)
            .expect("kacrab cannot speak ConsumerGroupHeartbeat v9");
        assert!(matches!(
            error,
            WireError::UnsupportedApiVersion {
                api_key: ApiKey::ConsumerGroupHeartbeat,
                broker: Some(_),
                ..
            }
        ));
    }

    /// The share gate names the *first* missing API so the message stays one
    /// actionable line, and reports nothing when all three are present.
    #[cfg(feature = "share-consumer")]
    #[test]
    fn the_share_gate_reports_the_first_missing_api_and_passes_a_full_set() {
        use super::SHARE_GROUP_APIS;

        let partial = capabilities(&[
            (ApiKey::ShareGroupHeartbeat, 1, 1),
            (ApiKey::ShareFetch, 1, 2),
        ]);
        let error = first_unsupported(&partial, &SHARE_GROUP_APIS)
            .expect("a share group needs ShareAcknowledge too");
        assert!(matches!(
            error,
            WireError::UnsupportedApiVersion {
                api_key: ApiKey::ShareAcknowledge,
                ..
            }
        ));

        let complete = capabilities(&[
            (ApiKey::ShareGroupHeartbeat, 1, 1),
            (ApiKey::ShareFetch, 1, 2),
            (ApiKey::ShareAcknowledge, 1, 2),
        ]);
        assert!(first_unsupported(&complete, &SHARE_GROUP_APIS).is_none());
    }
}
