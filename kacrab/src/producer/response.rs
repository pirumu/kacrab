//! Produce response normalization.

use std::sync::Arc;

use kacrab_protocol::{
    KafkaUuid,
    generated::{ErrorCode, PartitionProduceResponse, ProduceResponseData},
};

use super::{error::ProducerError, record::RecordMetadata, routing::ProduceRoute};
use crate::wire::BrokerMetadata;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PartitionLeaderUpdate {
    pub(crate) topic: String,
    pub(crate) partition: i32,
    pub(crate) leader_id: i32,
    pub(crate) leader_epoch: i32,
}

pub(crate) fn current_leader_updates(response: &ProduceResponseData) -> Vec<PartitionLeaderUpdate> {
    let mut updates = Vec::new();
    for topic_response in &response.responses {
        let topic = topic_response.name.as_str();
        for partition_response in &topic_response.partition_responses {
            let error = ErrorCode::from(partition_response.error_code);
            if !matches!(
                error,
                ErrorCode::NotLeaderOrFollower | ErrorCode::FencedLeaderEpoch
            ) {
                continue;
            }
            let leader = &partition_response.current_leader;
            if leader.leader_id >= 0 && leader.leader_epoch >= 0 {
                updates.push(PartitionLeaderUpdate {
                    topic: topic.to_owned(),
                    partition: partition_response.index,
                    leader_id: leader.leader_id,
                    leader_epoch: leader.leader_epoch,
                });
            }
        }
    }
    updates
}

pub(crate) fn node_endpoint_updates(response: &ProduceResponseData) -> Vec<BrokerMetadata> {
    response
        .node_endpoints
        .iter()
        .filter(|endpoint| endpoint.node_id >= 0)
        .map(|endpoint| BrokerMetadata {
            node_id: endpoint.node_id,
            host: endpoint.host.to_string(),
            port: endpoint.port,
            rack: endpoint.rack.as_ref().map(ToString::to_string),
        })
        .collect()
}

#[cfg(test)]
pub(crate) fn produce_receipts(
    response: &ProduceResponseData,
    routes: &[ProduceRoute],
) -> super::error::Result<Vec<RecordMetadata>> {
    produce_receipts_with_error_details(response, routes).map_err(Into::into)
}

pub(crate) fn produce_receipts_with_error_details(
    response: &ProduceResponseData,
    routes: &[ProduceRoute],
) -> Result<Vec<RecordMetadata>, ProduceReceiptError> {
    let mut receipts = Vec::with_capacity(routes.len());
    for route in routes {
        receipts.push(produce_receipt(response, route)?);
    }
    Ok(receipts)
}

#[derive(Debug)]
pub(crate) enum ProduceReceiptError {
    Broker(ProduceBrokerError),
    Producer(ProducerError),
}

#[derive(Debug)]
pub(crate) struct ProduceBrokerError {
    pub(crate) topic: String,
    pub(crate) partition: i32,
    pub(crate) error: ErrorCode,
    pub(crate) log_start_offset: i64,
}

impl From<ProducerError> for ProduceReceiptError {
    fn from(error: ProducerError) -> Self {
        Self::Producer(error)
    }
}

impl From<ProduceReceiptError> for ProducerError {
    fn from(error: ProduceReceiptError) -> Self {
        match error {
            ProduceReceiptError::Broker(error) => Self::Broker {
                topic: error.topic,
                partition: error.partition,
                error: error.error,
            },
            ProduceReceiptError::Producer(error) => error,
        }
    }
}

fn produce_receipt(
    response: &ProduceResponseData,
    route: &ProduceRoute,
) -> Result<RecordMetadata, ProduceReceiptError> {
    let topic_response = response
        .responses
        .iter()
        .find(|topic_response| matches_topic(topic_response.topic_id, &topic_response.name, route))
        .ok_or_else(|| ProducerError::MissingProduceResponse {
            topic: route.topic.clone(),
            partition: route.partition,
        })?;
    let partition_response = topic_response
        .partition_responses
        .iter()
        .find(|partition_response| partition_response.index == route.partition)
        .ok_or_else(|| ProducerError::MissingProduceResponse {
            topic: route.topic.clone(),
            partition: route.partition,
        })?;
    check_partition_error(partition_response, route)?;
    Ok(RecordMetadata {
        topic: Arc::from(route.topic.as_str()),
        partition: route.partition,
        leader_id: route.leader_id,
        offset: partition_response
            .base_offset
            .saturating_add(route.request_offset_delta),
        timestamp_ms: partition_response.log_append_time_ms,
        serialized_key_size: -1,
        serialized_value_size: -1,
    })
}

fn check_partition_error(
    partition_response: &PartitionProduceResponse,
    route: &ProduceRoute,
) -> Result<(), ProduceReceiptError> {
    let error = ErrorCode::from(partition_response.error_code);
    if error.is_error() && error != ErrorCode::DuplicateSequenceNumber {
        return Err(ProduceReceiptError::Broker(ProduceBrokerError {
            topic: route.topic.clone(),
            partition: route.partition,
            error,
            log_start_offset: partition_response.log_start_offset,
        }));
    }
    Ok(())
}

fn matches_topic(
    topic_id: KafkaUuid,
    name: &kacrab_protocol::KafkaString,
    route: &ProduceRoute,
) -> bool {
    topic_id == route.topic_id || name.as_str() == route.topic.as_str()
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::expect_used,
        clippy::missing_assert_message,
        clippy::unwrap_used,
        reason = "Unit test fixtures fail fastest with contextual unwrap/expect calls."
    )]

    use kacrab_protocol::{
        KafkaString, KafkaUuid,
        generated::{
            ErrorCode, PartitionProduceResponse, ProduceResponseData, TopicProduceResponse,
            produce_response::{LeaderIdAndEpoch, NodeEndpoint},
        },
    };

    use super::produce_receipts;
    use crate::producer::{ProducerError, routing::ProduceRoute};

    const TOPIC_ID: KafkaUuid = KafkaUuid::from_parts(0x1111_2222_3333_4444, 0x5555_6666_7777_8888);

    #[test]
    fn produce_receipts_normalizes_multi_partition_response_without_consuming_it() {
        let response = ProduceResponseData {
            responses: vec![TopicProduceResponse {
                name: KafkaString::from("orders".to_owned()),
                topic_id: TOPIC_ID,
                partition_responses: vec![
                    PartitionProduceResponse {
                        index: 0,
                        error_code: 0,
                        base_offset: 11,
                        log_append_time_ms: -1,
                        ..PartitionProduceResponse::default()
                    },
                    PartitionProduceResponse {
                        index: 1,
                        error_code: 0,
                        base_offset: 12,
                        log_append_time_ms: -1,
                        ..PartitionProduceResponse::default()
                    },
                ],
                _unknown_tagged_fields: Vec::new(),
            }],
            ..ProduceResponseData::default()
        };
        let routes = vec![route("orders", 0), route("orders", 1)];

        let receipts = produce_receipts(&response, &routes).expect("receipts");

        assert_eq!(receipts.len(), 2);
        assert_eq!(receipts[0].offset, 11);
        assert_eq!(receipts[1].offset, 12);
        assert_eq!(response.responses.len(), 1);
    }

    #[test]
    fn produce_response_extracts_current_leader_updates() {
        let response = ProduceResponseData {
            responses: vec![TopicProduceResponse {
                name: KafkaString::from("orders".to_owned()),
                topic_id: TOPIC_ID,
                partition_responses: vec![PartitionProduceResponse {
                    index: 0,
                    error_code: i16::from(ErrorCode::NotLeaderOrFollower),
                    current_leader: LeaderIdAndEpoch {
                        leader_id: 9,
                        leader_epoch: 4,
                        _unknown_tagged_fields: Vec::new(),
                    },
                    ..PartitionProduceResponse::default()
                }],
                _unknown_tagged_fields: Vec::new(),
            }],
            ..ProduceResponseData::default()
        };

        let updates = super::current_leader_updates(&response);

        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].topic, "orders");
        assert_eq!(updates[0].partition, 0);
        assert_eq!(updates[0].leader_id, 9);
        assert_eq!(updates[0].leader_epoch, 4);
    }

    #[test]
    fn produce_response_ignores_current_leader_for_non_java_update_errors() {
        let response = ProduceResponseData {
            responses: vec![TopicProduceResponse {
                name: KafkaString::from("orders".to_owned()),
                topic_id: TOPIC_ID,
                partition_responses: vec![
                    PartitionProduceResponse {
                        index: 0,
                        error_code: 0,
                        current_leader: LeaderIdAndEpoch {
                            leader_id: 9,
                            leader_epoch: 4,
                            _unknown_tagged_fields: Vec::new(),
                        },
                        ..PartitionProduceResponse::default()
                    },
                    PartitionProduceResponse {
                        index: 1,
                        error_code: i16::from(ErrorCode::LeaderNotAvailable),
                        current_leader: LeaderIdAndEpoch {
                            leader_id: 9,
                            leader_epoch: 4,
                            _unknown_tagged_fields: Vec::new(),
                        },
                        ..PartitionProduceResponse::default()
                    },
                ],
                _unknown_tagged_fields: Vec::new(),
            }],
            ..ProduceResponseData::default()
        };

        assert!(super::current_leader_updates(&response).is_empty());
    }

    #[test]
    fn produce_response_extracts_fenced_leader_epoch_current_leader_update() {
        let response = ProduceResponseData {
            responses: vec![TopicProduceResponse {
                name: KafkaString::from("orders".to_owned()),
                topic_id: TOPIC_ID,
                partition_responses: vec![PartitionProduceResponse {
                    index: 0,
                    error_code: i16::from(ErrorCode::FencedLeaderEpoch),
                    current_leader: LeaderIdAndEpoch {
                        leader_id: 9,
                        leader_epoch: 4,
                        _unknown_tagged_fields: Vec::new(),
                    },
                    ..PartitionProduceResponse::default()
                }],
                _unknown_tagged_fields: Vec::new(),
            }],
            ..ProduceResponseData::default()
        };

        assert_eq!(super::current_leader_updates(&response).len(), 1);
    }

    #[test]
    fn produce_response_extracts_node_endpoint_updates() {
        let response = ProduceResponseData {
            node_endpoints: vec![NodeEndpoint {
                node_id: 9,
                host: KafkaString::from("broker-9.example.test".to_owned()),
                port: 19_092,
                rack: Some(KafkaString::from("rack-a".to_owned())),
                _unknown_tagged_fields: Vec::new(),
            }],
            ..ProduceResponseData::default()
        };

        let endpoints = super::node_endpoint_updates(&response);

        assert_eq!(endpoints.len(), 1);
        assert_eq!(endpoints[0].node_id, 9);
        assert_eq!(endpoints[0].host, "broker-9.example.test");
        assert_eq!(endpoints[0].port, 19_092);
        assert_eq!(endpoints[0].rack.as_deref(), Some("rack-a"));
    }

    #[test]
    fn produce_receipts_reports_missing_topic_or_partition_response() {
        let empty = ProduceResponseData::default();
        let missing_topic =
            produce_receipts(&empty, &[route("orders", 0)]).expect_err("missing topic response");
        let missing_partition = produce_receipts(
            &ProduceResponseData {
                responses: vec![TopicProduceResponse {
                    name: KafkaString::from("orders".to_owned()),
                    topic_id: TOPIC_ID,
                    partition_responses: Vec::new(),
                    _unknown_tagged_fields: Vec::new(),
                }],
                ..ProduceResponseData::default()
            },
            &[route("orders", 0)],
        )
        .expect_err("missing partition response");

        assert!(matches!(
            missing_topic,
            ProducerError::MissingProduceResponse { .. }
        ));
        assert!(matches!(
            missing_partition,
            ProducerError::MissingProduceResponse { .. }
        ));
    }

    #[test]
    fn produce_receipts_reports_partition_error() {
        let response = ProduceResponseData {
            responses: vec![TopicProduceResponse {
                name: KafkaString::from("orders".to_owned()),
                topic_id: TOPIC_ID,
                partition_responses: vec![PartitionProduceResponse {
                    index: 0,
                    error_code: i16::from(ErrorCode::NotLeaderOrFollower),
                    ..PartitionProduceResponse::default()
                }],
                _unknown_tagged_fields: Vec::new(),
            }],
            ..ProduceResponseData::default()
        };

        let error = produce_receipts(&response, &[route("orders", 0)])
            .expect_err("broker error should fail receipt");

        assert!(matches!(
            error,
            ProducerError::Broker {
                error: ErrorCode::NotLeaderOrFollower,
                ..
            }
        ));
    }

    #[test]
    fn produce_receipts_offsets_duplicate_partition_routes_by_request_delta() {
        let response = ProduceResponseData {
            responses: vec![TopicProduceResponse {
                name: KafkaString::from("orders".to_owned()),
                topic_id: TOPIC_ID,
                partition_responses: vec![PartitionProduceResponse {
                    index: 0,
                    error_code: 0,
                    base_offset: 40,
                    log_append_time_ms: -1,
                    ..PartitionProduceResponse::default()
                }],
                _unknown_tagged_fields: Vec::new(),
            }],
            ..ProduceResponseData::default()
        };
        let mut first_route = route("orders", 0);
        first_route.record_count = 2;
        let mut second_route = route("orders", 0);
        second_route.request_offset_delta = 2;
        second_route.record_count = 1;

        let receipts = produce_receipts(&response, &[first_route, second_route]).expect("receipts");

        assert_eq!(receipts.len(), 2);
        assert_eq!(receipts[0].offset, 40);
        assert_eq!(receipts[1].offset, 42);
    }

    fn route(topic: &str, partition: i32) -> ProduceRoute {
        ProduceRoute {
            topic: topic.to_owned(),
            partition,
            topic_id: TOPIC_ID,
            leader_id: 7,
            base_sequence: None,
            request_offset_delta: 0,
            record_count: 0,
        }
    }
}
