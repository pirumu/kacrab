//! Produce response normalization.

use kacrab_protocol::{
    KafkaUuid,
    generated::{ErrorCode, PartitionProduceResponse, ProduceResponseData},
};

use super::{
    error::{ProducerError, Result},
    record::ProduceReceipt,
    routing::ProduceRoute,
};

pub(crate) fn produce_receipts(
    response: &ProduceResponseData,
    routes: &[ProduceRoute],
) -> Result<Vec<ProduceReceipt>> {
    let mut receipts = Vec::with_capacity(routes.len());
    for route in routes {
        receipts.push(produce_receipt(response, route)?);
    }
    Ok(receipts)
}

fn produce_receipt(response: &ProduceResponseData, route: &ProduceRoute) -> Result<ProduceReceipt> {
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
    Ok(ProduceReceipt {
        topic: route.topic.clone(),
        partition: route.partition,
        leader_id: route.leader_id,
        base_offset: partition_response.base_offset,
        log_append_time_ms: partition_response.log_append_time_ms,
    })
}

fn check_partition_error(
    partition_response: &PartitionProduceResponse,
    route: &ProduceRoute,
) -> Result<()> {
    let error = ErrorCode::from(partition_response.error_code);
    if error.is_error() {
        return Err(ProducerError::Broker {
            topic: route.topic.clone(),
            partition: route.partition,
            error,
        });
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
        assert_eq!(receipts[0].base_offset, 11);
        assert_eq!(receipts[1].base_offset, 12);
        assert_eq!(response.responses.len(), 1);
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

    fn route(topic: &str, partition: i32) -> ProduceRoute {
        ProduceRoute {
            topic: topic.to_owned(),
            partition,
            topic_id: TOPIC_ID,
            leader_id: 7,
        }
    }
}
