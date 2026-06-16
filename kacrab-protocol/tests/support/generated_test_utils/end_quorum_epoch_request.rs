use bytes::{Bytes, BytesMut};
use kacrab_protocol::{generated::end_quorum_epoch_request::*, *};

use crate::TestInstance;

impl TestInstance for EndQuorumEpochRequestData {
    fn test_populated() -> Self {
        Self {
            cluster_id: Some(KafkaString::from("test".to_owned())),
            topics: vec![<TopicData as TestInstance>::test_populated()],
            leader_endpoints: vec![<LeaderEndpoint as TestInstance>::test_populated()],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals() -> Self {
        Self {
            cluster_id: None,
            topics: vec![<TopicData as TestInstance>::test_null_optionals()],
            leader_endpoints: vec![<LeaderEndpoint as TestInstance>::test_null_optionals()],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            cluster_id: Some(KafkaString::default()),
            topics: Vec::new(),
            leader_endpoints: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            cluster_id: Some(KafkaString::from("test-2".to_owned())),
            topics: vec![
                <TopicData as TestInstance>::test_populated(),
                <TopicData as TestInstance>::test_multi_element_collections(),
            ],
            leader_endpoints: vec![
                <LeaderEndpoint as TestInstance>::test_populated(),
                <LeaderEndpoint as TestInstance>::test_multi_element_collections(),
            ],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            cluster_id: Some(KafkaString::from("boundary".to_owned())),
            topics: vec![<TopicData as TestInstance>::test_numeric_boundaries()],
            leader_endpoints: vec![<LeaderEndpoint as TestInstance>::test_numeric_boundaries()],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            cluster_id: Some(KafkaString::from("test".to_owned())),
            topics: vec![<TopicData as TestInstance>::test_tagged_fields()],
            leader_endpoints: vec![<LeaderEndpoint as TestInstance>::test_tagged_fields()],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for TopicData {
    fn test_populated() -> Self {
        Self {
            topic_name: KafkaString::from("test".to_owned()),
            partitions: vec![<PartitionData as TestInstance>::test_populated()],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals() -> Self {
        drop(Self::default());
        Self {
            topic_name: KafkaString::default(),
            partitions: vec![<PartitionData as TestInstance>::test_null_optionals()],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            topic_name: KafkaString::default(),
            partitions: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            topic_name: KafkaString::from("test-2".to_owned()),
            partitions: vec![
                <PartitionData as TestInstance>::test_populated(),
                <PartitionData as TestInstance>::test_multi_element_collections(),
            ],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            topic_name: KafkaString::from("boundary".to_owned()),
            partitions: vec![<PartitionData as TestInstance>::test_numeric_boundaries()],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            topic_name: KafkaString::from("test".to_owned()),
            partitions: vec![<PartitionData as TestInstance>::test_tagged_fields()],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for PartitionData {
    fn test_populated() -> Self {
        Self {
            partition_index: 12345_i32,
            leader_id: 12345_i32,
            leader_epoch: 12345_i32,
            preferred_successors: vec![12345_i32],
            preferred_candidates: vec![<ReplicaInfo as TestInstance>::test_populated()],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals() -> Self {
        drop(Self::default());
        Self {
            partition_index: 0_i32,
            leader_id: 0_i32,
            leader_epoch: 0_i32,
            preferred_successors: vec![0_i32],
            preferred_candidates: vec![<ReplicaInfo as TestInstance>::test_null_optionals()],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            partition_index: 0_i32,
            leader_id: 0_i32,
            leader_epoch: 0_i32,
            preferred_successors: Vec::new(),
            preferred_candidates: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            partition_index: 23456_i32,
            leader_id: 23456_i32,
            leader_epoch: 23456_i32,
            preferred_successors: vec![12345_i32, 23456_i32],
            preferred_candidates: vec![
                <ReplicaInfo as TestInstance>::test_populated(),
                <ReplicaInfo as TestInstance>::test_multi_element_collections(),
            ],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            partition_index: i32::MIN,
            leader_id: i32::MIN,
            leader_epoch: i32::MIN,
            preferred_successors: vec![i32::MIN],
            preferred_candidates: vec![<ReplicaInfo as TestInstance>::test_numeric_boundaries()],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            partition_index: 12345_i32,
            leader_id: 12345_i32,
            leader_epoch: 12345_i32,
            preferred_successors: vec![12345_i32],
            preferred_candidates: vec![<ReplicaInfo as TestInstance>::test_tagged_fields()],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for ReplicaInfo {
    fn test_populated() -> Self {
        Self {
            candidate_id: 12345_i32,
            candidate_directory_id: KafkaUuid::ONE,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals() -> Self {
        drop(Self::default());
        Self {
            candidate_id: 0_i32,
            candidate_directory_id: KafkaUuid::ZERO,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            candidate_id: 0_i32,
            candidate_directory_id: KafkaUuid::ZERO,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            candidate_id: 23456_i32,
            candidate_directory_id: KafkaUuid::from_parts(2, 3),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            candidate_id: i32::MIN,
            candidate_directory_id: KafkaUuid::ONE,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            candidate_id: 12345_i32,
            candidate_directory_id: KafkaUuid::ONE,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for LeaderEndpoint {
    fn test_populated() -> Self {
        Self {
            name: KafkaString::from("test".to_owned()),
            host: KafkaString::from("test".to_owned()),
            port: 42_u16,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals() -> Self {
        drop(Self::default());
        Self {
            name: KafkaString::default(),
            host: KafkaString::default(),
            port: 0_u16,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            name: KafkaString::default(),
            host: KafkaString::default(),
            port: 0_u16,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            name: KafkaString::from("test-2".to_owned()),
            host: KafkaString::from("test-2".to_owned()),
            port: 43_u16,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            name: KafkaString::from("boundary".to_owned()),
            host: KafkaString::from("boundary".to_owned()),
            port: u16::MAX,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            name: KafkaString::from("test".to_owned()),
            host: KafkaString::from("test".to_owned()),
            port: 42_u16,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
fn encode_populated(version: i16) -> crate::MatrixResult<String> {
    let message = <EndQuorumEpochRequestData as TestInstance>::test_populated();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_populated(version: i16) -> crate::MatrixResult<usize> {
    let message = <EndQuorumEpochRequestData as TestInstance>::test_populated();
    Ok(message.encoded_len(version)?)
}
fn encode_null_optionals(version: i16) -> crate::MatrixResult<String> {
    let message = <EndQuorumEpochRequestData as TestInstance>::test_null_optionals();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_null_optionals(version: i16) -> crate::MatrixResult<usize> {
    let message = <EndQuorumEpochRequestData as TestInstance>::test_null_optionals();
    Ok(message.encoded_len(version)?)
}
fn encode_empty_collections(version: i16) -> crate::MatrixResult<String> {
    let message = <EndQuorumEpochRequestData as TestInstance>::test_empty_collections();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_empty_collections(version: i16) -> crate::MatrixResult<usize> {
    let message = <EndQuorumEpochRequestData as TestInstance>::test_empty_collections();
    Ok(message.encoded_len(version)?)
}
fn encode_multi_element_collections(version: i16) -> crate::MatrixResult<String> {
    let message = <EndQuorumEpochRequestData as TestInstance>::test_multi_element_collections();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_multi_element_collections(version: i16) -> crate::MatrixResult<usize> {
    let message = <EndQuorumEpochRequestData as TestInstance>::test_multi_element_collections();
    Ok(message.encoded_len(version)?)
}
fn encode_numeric_boundaries(version: i16) -> crate::MatrixResult<String> {
    let message = <EndQuorumEpochRequestData as TestInstance>::test_numeric_boundaries();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_numeric_boundaries(version: i16) -> crate::MatrixResult<usize> {
    let message = <EndQuorumEpochRequestData as TestInstance>::test_numeric_boundaries();
    Ok(message.encoded_len(version)?)
}
fn encode_tagged_fields(version: i16) -> crate::MatrixResult<String> {
    let message = <EndQuorumEpochRequestData as TestInstance>::test_tagged_fields();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_tagged_fields(version: i16) -> crate::MatrixResult<usize> {
    let message = <EndQuorumEpochRequestData as TestInstance>::test_tagged_fields();
    Ok(message.encoded_len(version)?)
}
fn reencode(version: i16, hex_input: &str) -> crate::MatrixResult<String> {
    let mut input = Bytes::from(crate::decode_hex(hex_input)?);
    let message = EndQuorumEpochRequestData::read(&mut input, version)?;
    crate::ensure_input_consumed(&input)?;
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
const MATRIX_CASES: &[crate::MatrixCase] = &[
    crate::MatrixCase {
        schema_name: "EndQuorumEpochRequest",
        java_class: "org.apache.kafka.common.message.EndQuorumEpochRequestData",
        version: 0i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "EndQuorumEpochRequest",
        java_class: "org.apache.kafka.common.message.EndQuorumEpochRequestData",
        version: 1i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "EndQuorumEpochRequest",
        java_class: "org.apache.kafka.common.message.EndQuorumEpochRequestData",
        version: 0i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "EndQuorumEpochRequest",
        java_class: "org.apache.kafka.common.message.EndQuorumEpochRequestData",
        version: 1i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "EndQuorumEpochRequest",
        java_class: "org.apache.kafka.common.message.EndQuorumEpochRequestData",
        version: 0i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "EndQuorumEpochRequest",
        java_class: "org.apache.kafka.common.message.EndQuorumEpochRequestData",
        version: 0i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "EndQuorumEpochRequest",
        java_class: "org.apache.kafka.common.message.EndQuorumEpochRequestData",
        version: 0i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "EndQuorumEpochRequest",
        java_class: "org.apache.kafka.common.message.EndQuorumEpochRequestData",
        version: 0i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "EndQuorumEpochRequest",
        java_class: "org.apache.kafka.common.message.EndQuorumEpochRequestData",
        version: 1i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "EndQuorumEpochRequest",
        java_class: "org.apache.kafka.common.message.EndQuorumEpochRequestData",
        version: 1i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "EndQuorumEpochRequest",
        java_class: "org.apache.kafka.common.message.EndQuorumEpochRequestData",
        version: 1i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "EndQuorumEpochRequest",
        java_class: "org.apache.kafka.common.message.EndQuorumEpochRequestData",
        version: 1i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
];
pub(crate) fn append_protocol_cases(cases: &mut Vec<crate::MatrixCase>) {
    cases.extend_from_slice(MATRIX_CASES);
}
