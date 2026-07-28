//! The kind of coordinator a `FindCoordinator` request resolves, mirroring
//! Kafka's `FindCoordinatorRequest.CoordinatorType` (`GROUP` / `TRANSACTION`),
//! and the reader that turns either `FindCoordinator` response shape into one
//! coordinator entry.

use kacrab_protocol::generated::{Coordinator, FindCoordinatorResponseData};

/// The coordinator a `FindCoordinator` request targets, mirroring the `key_type`
/// byte of Kafka's `FindCoordinatorRequest`: a consumer group coordinator or a
/// transaction coordinator.
#[cfg(any(feature = "producer", feature = "admin"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CoordinatorType {
    /// Consumer group coordinator (`key_type` 0).
    Group,
    /// Transaction coordinator (`key_type` 1).
    Transaction,
}

#[cfg(any(feature = "producer", feature = "admin"))]
impl CoordinatorType {
    /// The `FindCoordinator` `key_type` wire byte (`Group` = 0, `Transaction` = 1).
    #[must_use]
    pub(crate) const fn key_type(self) -> i8 {
        match self {
            Self::Group => 0,
            Self::Transaction => 1,
        }
    }
}

/// Read the coordinator answered for `key` out of either `FindCoordinator`
/// response shape.
///
/// v4+ (KIP-699) answers with the batched `coordinators` array, keyed by the
/// requested key. v0-3 answers with a single coordinator in the top-level
/// `node_id`/`host`/`port`/`error_code` fields and leaves the array empty
/// (`find_coordinator_response.rs` only decodes `coordinators` from v4), so that
/// flat form is folded into the same entry — the request only ever carries one
/// key, so the requested key is the key that was answered.
///
/// Returns `None` when the batched array does not name `key`, and when a flat
/// response carries neither a host nor an error, so a truncated or empty
/// response is reported as "no coordinator" instead of node 0.
pub(crate) fn coordinator_for_key(
    response: FindCoordinatorResponseData,
    key: &str,
) -> Option<Coordinator> {
    if !response.coordinators.is_empty() {
        return response
            .coordinators
            .into_iter()
            .find(|coordinator| coordinator.key.as_str() == key);
    }
    if response.host.as_str().is_empty() && response.error_code == 0 {
        return None;
    }
    Some(Coordinator {
        key: key.to_owned().into(),
        node_id: response.node_id,
        host: response.host,
        port: response.port,
        error_code: response.error_code,
        error_message: response.error_message,
        _unknown_tagged_fields: Vec::new(),
    })
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::expect_used,
        clippy::missing_assert_message,
        clippy::unwrap_used,
        reason = "Unit test fixtures fail fastest with contextual unwrap/expect calls."
    )]

    use kacrab_protocol::KafkaString;

    use super::{Coordinator, FindCoordinatorResponseData, coordinator_for_key};

    #[test]
    fn coordinator_for_key_reads_the_batched_response_shape() {
        let response = FindCoordinatorResponseData {
            coordinators: vec![
                Coordinator {
                    key: KafkaString::from("group-b".to_owned()),
                    node_id: 4,
                    ..Coordinator::default()
                },
                Coordinator {
                    key: KafkaString::from("group-a".to_owned()),
                    node_id: 9,
                    host: KafkaString::from("host-a".to_owned()),
                    port: 9092,
                    ..Coordinator::default()
                },
            ],
            ..FindCoordinatorResponseData::default()
        };

        let coordinator = coordinator_for_key(response, "group-a").expect("coordinator");

        assert_eq!(coordinator.node_id, 9);
        assert_eq!(coordinator.host.as_str(), "host-a");
        assert_eq!(coordinator.port, 9092);
    }

    #[test]
    fn coordinator_for_key_reads_the_flat_pre_batched_response_shape() {
        let response = FindCoordinatorResponseData {
            node_id: 9,
            host: KafkaString::from("host-a".to_owned()),
            port: 9092,
            ..FindCoordinatorResponseData::default()
        };

        let coordinator = coordinator_for_key(response, "group-a").expect("coordinator");

        assert_eq!(coordinator.key.as_str(), "group-a");
        assert_eq!(coordinator.node_id, 9);
        assert_eq!(coordinator.host.as_str(), "host-a");
        assert_eq!(coordinator.port, 9092);
        assert_eq!(coordinator.error_code, 0);
    }

    #[test]
    fn coordinator_for_key_keeps_the_flat_response_error() {
        let response = FindCoordinatorResponseData {
            error_code: 15,
            error_message: Some(KafkaString::from("loading".to_owned())),
            ..FindCoordinatorResponseData::default()
        };

        let coordinator = coordinator_for_key(response, "group-a").expect("coordinator");

        assert_eq!(coordinator.error_code, 15);
    }

    #[test]
    fn coordinator_for_key_reports_a_missing_and_an_empty_answer() {
        let batched = FindCoordinatorResponseData {
            coordinators: vec![Coordinator {
                key: KafkaString::from("group-b".to_owned()),
                ..Coordinator::default()
            }],
            ..FindCoordinatorResponseData::default()
        };

        assert!(coordinator_for_key(batched, "group-a").is_none());
        assert!(coordinator_for_key(FindCoordinatorResponseData::default(), "group-a").is_none());
    }
}
