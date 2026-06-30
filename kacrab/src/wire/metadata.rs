//! Metadata API client and normalized cluster metadata types.

pub(crate) mod manager;

use kacrab_protocol::{
    KafkaString, KafkaUuid,
    generated::{ErrorCode, MetadataRequestData, MetadataRequestTopic, MetadataResponseData},
};
#[cfg(feature = "producer")]
pub(crate) use manager::PartitionLeaderChange;
pub(crate) use manager::{MetadataManager, MetadataRecoveryAction};

use super::error::{Result, WireError};

pub(crate) fn metadata_request<I, S>(
    topics: I,
    allow_auto_topic_creation: bool,
) -> MetadataRequestData
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    MetadataRequestData {
        topics: Some(
            topics
                .into_iter()
                .map(|topic| MetadataRequestTopic {
                    topic_id: KafkaUuid::ZERO,
                    name: Some(KafkaString::from(topic.as_ref().to_owned())),
                    _unknown_tagged_fields: Vec::new(),
                })
                .collect(),
        ),
        allow_auto_topic_creation,
        include_cluster_authorized_operations: false,
        include_topic_authorized_operations: false,
        _unknown_tagged_fields: Vec::new(),
    }
}

/// Build a metadata request for every topic in the cluster. A `None` topic list
/// is the Kafka wire convention for "all topics", used by admin discovery and
/// full-cluster listing. Auto topic creation is never meaningful here.
#[cfg(feature = "admin")]
pub(crate) const fn metadata_request_all() -> MetadataRequestData {
    MetadataRequestData {
        topics: None,
        allow_auto_topic_creation: false,
        include_cluster_authorized_operations: false,
        include_topic_authorized_operations: false,
        _unknown_tagged_fields: Vec::new(),
    }
}

/// Cluster metadata needed by producer/consumer routing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterMetadata {
    /// Cluster id, present for metadata response v2+ when broker supplies it.
    pub cluster_id: Option<String>,
    /// Controller broker id.
    pub controller_id: i32,
    /// Brokers known to the cluster.
    pub brokers: Vec<BrokerMetadata>,
    /// Topic metadata returned by the broker.
    pub topics: Vec<TopicMetadata>,
}

/// Broker endpoint metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerMetadata {
    /// Broker node id.
    pub node_id: i32,
    /// Broker host.
    pub host: String,
    /// Broker port.
    pub port: i32,
    /// Optional broker rack.
    pub rack: Option<String>,
}

/// Topic metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicMetadata {
    /// Topic name.
    pub name: String,
    /// Stable Kafka topic ID.
    pub topic_id: KafkaUuid,
    /// Whether the broker flags this as an internal topic (e.g.
    /// `__consumer_offsets`), surfaced for admin topic listings.
    pub is_internal: bool,
    /// Partition metadata for this topic.
    pub partitions: Vec<PartitionMetadata>,
}

/// Topic metadata status retained separately from usable routing metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MetadataTopicState {
    pub(crate) topic: String,
    pub(crate) status: MetadataTopicStatus,
}

/// Java-style topic bookkeeping buckets from metadata responses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MetadataTopicStatus {
    Usable { is_internal: bool },
    Invalid(ErrorCode),
    Unauthorized(ErrorCode),
    Error(ErrorCode),
}

/// Partition leader and replica metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionMetadata {
    /// Partition index.
    pub partition_index: i32,
    /// Leader broker id.
    pub leader_id: i32,
    /// Leader epoch.
    pub leader_epoch: i32,
    /// Replica broker ids.
    pub replica_nodes: Vec<i32>,
    /// In-sync replica broker ids.
    pub isr_nodes: Vec<i32>,
    /// Offline replica broker ids.
    pub offline_replicas: Vec<i32>,
}

impl ClusterMetadata {
    /// Find topic metadata by name.
    #[must_use]
    pub fn topic(&self, name: &str) -> Option<&TopicMetadata> {
        self.topics.iter().find(|topic| topic.name == name)
    }

    /// Find the leader broker for a topic partition.
    #[must_use]
    pub fn leader_for(&self, topic: &str, partition: i32) -> Option<&BrokerMetadata> {
        let partition = self
            .topic(topic)?
            .partitions
            .iter()
            .find(|partition_metadata| partition_metadata.partition_index == partition)?;
        self.brokers
            .iter()
            .find(|broker| broker.node_id == partition.leader_id)
    }
}

pub(crate) fn map_metadata(response: MetadataResponseData) -> Result<ClusterMetadata> {
    let top_level_error = ErrorCode::from(response.error_code);
    if top_level_error.is_error() {
        return Err(WireError::Kafka(top_level_error));
    }

    let brokers = response
        .brokers
        .into_iter()
        .map(|broker| BrokerMetadata {
            node_id: broker.node_id,
            host: broker.host.to_string(),
            port: broker.port,
            rack: broker.rack.map(|rack| rack.to_string()),
        })
        .collect();

    let mut topics = Vec::with_capacity(response.topics.len());
    for topic in response.topics {
        let error = ErrorCode::from(topic.error_code);
        let name = topic.name.unwrap_or_default().to_string();
        if error.is_error() {
            return Err(WireError::MetadataTopic { topic: name, error });
        }
        let topic_id = topic.topic_id;
        let is_internal = topic.is_internal;
        let mut partitions = Vec::with_capacity(topic.partitions.len());
        for partition in topic.partitions {
            let error = ErrorCode::from(partition.error_code);
            if error.is_error() {
                return Err(WireError::MetadataPartition {
                    topic: name,
                    partition: partition.partition_index,
                    error,
                });
            }
            partitions.push(PartitionMetadata {
                partition_index: partition.partition_index,
                leader_id: partition.leader_id,
                leader_epoch: partition.leader_epoch,
                replica_nodes: partition.replica_nodes,
                isr_nodes: partition.isr_nodes,
                offline_replicas: partition.offline_replicas,
            });
        }
        topics.push(TopicMetadata {
            name,
            topic_id,
            is_internal,
            partitions,
        });
    }

    Ok(ClusterMetadata {
        cluster_id: response.cluster_id.map(|cluster_id| cluster_id.to_string()),
        controller_id: response.controller_id,
        brokers,
        topics,
    })
}

pub(crate) fn metadata_topic_states(response: &MetadataResponseData) -> Vec<MetadataTopicState> {
    response
        .topics
        .iter()
        .filter_map(|topic| {
            let topic_name = topic.name.as_ref()?.to_string();
            let error = ErrorCode::from(topic.error_code);
            let status = if error == ErrorCode::TopicAuthorizationFailed {
                MetadataTopicStatus::Unauthorized(error)
            } else if error == ErrorCode::InvalidTopicException {
                MetadataTopicStatus::Invalid(error)
            } else if error.is_error() {
                MetadataTopicStatus::Error(error)
            } else {
                MetadataTopicStatus::Usable {
                    is_internal: topic.is_internal,
                }
            };
            Some(MetadataTopicState {
                topic: topic_name,
                status,
            })
        })
        .collect()
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
            ErrorCode, MetadataResponseBroker, MetadataResponseData, MetadataResponsePartition,
            MetadataResponseTopic,
        },
    };

    use super::{MetadataTopicStatus, map_metadata, metadata_request, metadata_topic_states};
    use crate::wire::WireError;

    #[test]
    fn metadata_request_encodes_named_topics_without_auto_creation() {
        let request = metadata_request(["orders", "payments"], false);
        let topics = request.topics.expect("topics");

        assert!(!request.allow_auto_topic_creation);
        assert_eq!(topics.len(), 2);
        assert_eq!(
            topics
                .iter()
                .filter_map(|topic| topic.name.as_ref())
                .map(KafkaString::as_str)
                .collect::<Vec<_>>(),
            ["orders", "payments"]
        );
    }

    #[test]
    fn metadata_request_encodes_empty_topic_list_explicitly() {
        let request = metadata_request(std::iter::empty::<&str>(), false);

        assert_eq!(request.topics.expect("topics"), []);
    }

    #[test]
    fn map_metadata_normalizes_brokers_topics_and_leaders() {
        let metadata = map_metadata(response_with_error_codes(0, 0, 0)).expect("metadata");

        assert_eq!(metadata.cluster_id.as_deref(), Some("cluster-a"));
        assert_eq!(metadata.brokers.len(), 1);
        assert!(metadata.topic("orders").is_some());
        assert_eq!(
            metadata
                .leader_for("orders", 0)
                .map(|broker| broker.host.as_str()),
            Some("localhost")
        );
        assert_eq!(metadata.leader_for("orders", 1), None);
    }

    #[test]
    fn map_metadata_propagates_response_topic_and_partition_errors() {
        assert!(matches!(
            map_metadata(response_with_error_codes(
                i16::from(ErrorCode::BrokerNotAvailable),
                0,
                0
            )),
            Err(WireError::Kafka(ErrorCode::BrokerNotAvailable))
        ));
        assert!(matches!(
            map_metadata(response_with_error_codes(
                0,
                i16::from(ErrorCode::UnknownTopicOrPartition),
                0
            )),
            Err(WireError::MetadataTopic {
                topic,
                error: ErrorCode::UnknownTopicOrPartition,
            }) if topic == "orders"
        ));
        assert!(matches!(
            map_metadata(response_with_error_codes(
                0,
                0,
                i16::from(ErrorCode::LeaderNotAvailable)
            )),
            Err(WireError::MetadataPartition {
                topic,
                partition: 0,
                error: ErrorCode::LeaderNotAvailable,
            }) if topic == "orders"
        ));
    }

    #[test]
    fn metadata_topic_states_classify_internal_invalid_and_unauthorized_topics() {
        let mut response = response_with_error_codes(0, 0, 0);
        response.topics[0].is_internal = true;
        response.topics.push(MetadataResponseTopic {
            error_code: i16::from(ErrorCode::InvalidTopicException),
            name: Some(KafkaString::from("bad topic".to_owned())),
            ..MetadataResponseTopic::default()
        });
        response.topics.push(MetadataResponseTopic {
            error_code: i16::from(ErrorCode::TopicAuthorizationFailed),
            name: Some(KafkaString::from("secret".to_owned())),
            ..MetadataResponseTopic::default()
        });

        let states = metadata_topic_states(&response);

        assert!(states.iter().any(|state| {
            state.topic == "orders"
                && state.status == MetadataTopicStatus::Usable { is_internal: true }
        }));
        assert!(states.iter().any(|state| {
            state.topic == "bad topic"
                && state.status == MetadataTopicStatus::Invalid(ErrorCode::InvalidTopicException)
        }));
        assert!(states.iter().any(|state| {
            state.topic == "secret"
                && state.status
                    == MetadataTopicStatus::Unauthorized(ErrorCode::TopicAuthorizationFailed)
        }));
    }

    fn response_with_error_codes(
        response_error: i16,
        topic_error: i16,
        partition_error: i16,
    ) -> MetadataResponseData {
        MetadataResponseData {
            error_code: response_error,
            cluster_id: Some(KafkaString::from("cluster-a".to_owned())),
            controller_id: 7,
            brokers: vec![MetadataResponseBroker {
                node_id: 7,
                host: KafkaString::from("localhost".to_owned()),
                port: 9092,
                rack: None,
                _unknown_tagged_fields: Vec::new(),
            }],
            topics: vec![MetadataResponseTopic {
                error_code: topic_error,
                name: Some(KafkaString::from("orders".to_owned())),
                topic_id: KafkaUuid::ZERO,
                is_internal: false,
                partitions: vec![MetadataResponsePartition {
                    error_code: partition_error,
                    partition_index: 0,
                    leader_id: 7,
                    leader_epoch: 1,
                    replica_nodes: vec![7],
                    isr_nodes: vec![7],
                    offline_replicas: Vec::new(),
                    _unknown_tagged_fields: Vec::new(),
                }],
                topic_authorized_operations: 0,
                _unknown_tagged_fields: Vec::new(),
            }],
            throttle_time_ms: 0,
            cluster_authorized_operations: 0,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
