use bytes::{Bytes, BytesMut};
use kacrab_protocol::{generated::describe_quorum_response::*, *};

use crate::TestInstance;

impl TestInstance for DescribeQuorumResponseData {
    fn test_populated() -> Self {
        Self {
            error_code: 42_i16,
            error_message: Some(KafkaString::from("test".to_owned())),
            topics: vec![<TopicData as TestInstance>::test_populated()],
            nodes: vec![<Node as TestInstance>::test_populated()],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals() -> Self {
        Self {
            error_code: 0_i16,
            error_message: None,
            topics: vec![<TopicData as TestInstance>::test_null_optionals()],
            nodes: vec![<Node as TestInstance>::test_null_optionals()],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            error_code: 0_i16,
            error_message: Some(KafkaString::default()),
            topics: Vec::new(),
            nodes: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            error_code: 43_i16,
            error_message: Some(KafkaString::from("test-2".to_owned())),
            topics: vec![
                <TopicData as TestInstance>::test_populated(),
                <TopicData as TestInstance>::test_multi_element_collections(),
            ],
            nodes: vec![
                <Node as TestInstance>::test_populated(),
                <Node as TestInstance>::test_multi_element_collections(),
            ],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            error_code: i16::MIN,
            error_message: Some(KafkaString::from("boundary".to_owned())),
            topics: vec![<TopicData as TestInstance>::test_numeric_boundaries()],
            nodes: vec![<Node as TestInstance>::test_numeric_boundaries()],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            error_code: 42_i16,
            error_message: Some(KafkaString::from("test".to_owned())),
            topics: vec![<TopicData as TestInstance>::test_tagged_fields()],
            nodes: vec![<Node as TestInstance>::test_tagged_fields()],
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
            error_code: 42_i16,
            error_message: Some(KafkaString::from("test".to_owned())),
            leader_id: 12345_i32,
            leader_epoch: 12345_i32,
            high_watermark: 9_876_543_210_i64,
            current_voters: vec![<ReplicaState as TestInstance>::test_populated()],
            observers: vec![<ReplicaState as TestInstance>::test_populated()],
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
            error_code: 0_i16,
            error_message: None,
            leader_id: 0_i32,
            leader_epoch: 0_i32,
            high_watermark: 0_i64,
            current_voters: vec![<ReplicaState as TestInstance>::test_null_optionals()],
            observers: vec![<ReplicaState as TestInstance>::test_null_optionals()],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            partition_index: 0_i32,
            error_code: 0_i16,
            error_message: Some(KafkaString::default()),
            leader_id: 0_i32,
            leader_epoch: 0_i32,
            high_watermark: 0_i64,
            current_voters: Vec::new(),
            observers: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            partition_index: 23456_i32,
            error_code: 43_i16,
            error_message: Some(KafkaString::from("test-2".to_owned())),
            leader_id: 23456_i32,
            leader_epoch: 23456_i32,
            high_watermark: 9_876_543_211_i64,
            current_voters: vec![
                <ReplicaState as TestInstance>::test_populated(),
                <ReplicaState as TestInstance>::test_multi_element_collections(),
            ],
            observers: vec![
                <ReplicaState as TestInstance>::test_populated(),
                <ReplicaState as TestInstance>::test_multi_element_collections(),
            ],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            partition_index: i32::MIN,
            error_code: i16::MIN,
            error_message: Some(KafkaString::from("boundary".to_owned())),
            leader_id: i32::MIN,
            leader_epoch: i32::MIN,
            high_watermark: i64::MIN,
            current_voters: vec![<ReplicaState as TestInstance>::test_numeric_boundaries()],
            observers: vec![<ReplicaState as TestInstance>::test_numeric_boundaries()],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            partition_index: 12345_i32,
            error_code: 42_i16,
            error_message: Some(KafkaString::from("test".to_owned())),
            leader_id: 12345_i32,
            leader_epoch: 12345_i32,
            high_watermark: 9_876_543_210_i64,
            current_voters: vec![<ReplicaState as TestInstance>::test_tagged_fields()],
            observers: vec![<ReplicaState as TestInstance>::test_tagged_fields()],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for Node {
    fn test_populated() -> Self {
        Self {
            node_id: 12345_i32,
            listeners: vec![<Listener as TestInstance>::test_populated()],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals() -> Self {
        drop(Self::default());
        Self {
            node_id: 0_i32,
            listeners: vec![<Listener as TestInstance>::test_null_optionals()],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            node_id: 0_i32,
            listeners: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            node_id: 23456_i32,
            listeners: vec![
                <Listener as TestInstance>::test_populated(),
                <Listener as TestInstance>::test_multi_element_collections(),
            ],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            node_id: i32::MIN,
            listeners: vec![<Listener as TestInstance>::test_numeric_boundaries()],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            node_id: 12345_i32,
            listeners: vec![<Listener as TestInstance>::test_tagged_fields()],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for Listener {
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
impl TestInstance for ReplicaState {
    fn test_populated() -> Self {
        Self {
            replica_id: 12345_i32,
            replica_directory_id: KafkaUuid::ONE,
            log_end_offset: 9_876_543_210_i64,
            last_fetch_timestamp: 9_876_543_210_i64,
            last_caught_up_timestamp: 9_876_543_210_i64,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals() -> Self {
        drop(Self::default());
        Self {
            replica_id: 0_i32,
            replica_directory_id: KafkaUuid::ZERO,
            log_end_offset: 0_i64,
            last_fetch_timestamp: 0_i64,
            last_caught_up_timestamp: 0_i64,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            replica_id: 0_i32,
            replica_directory_id: KafkaUuid::ZERO,
            log_end_offset: 0_i64,
            last_fetch_timestamp: 0_i64,
            last_caught_up_timestamp: 0_i64,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            replica_id: 23456_i32,
            replica_directory_id: KafkaUuid::from_parts(2, 3),
            log_end_offset: 9_876_543_211_i64,
            last_fetch_timestamp: 9_876_543_211_i64,
            last_caught_up_timestamp: 9_876_543_211_i64,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            replica_id: i32::MIN,
            replica_directory_id: KafkaUuid::ONE,
            log_end_offset: i64::MIN,
            last_fetch_timestamp: i64::MIN,
            last_caught_up_timestamp: i64::MIN,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            replica_id: 12345_i32,
            replica_directory_id: KafkaUuid::ONE,
            log_end_offset: 9_876_543_210_i64,
            last_fetch_timestamp: 9_876_543_210_i64,
            last_caught_up_timestamp: 9_876_543_210_i64,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
fn encode_populated(version: i16) -> crate::MatrixResult<String> {
    let message = <DescribeQuorumResponseData as TestInstance>::test_populated();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_populated(version: i16) -> crate::MatrixResult<usize> {
    let message = <DescribeQuorumResponseData as TestInstance>::test_populated();
    Ok(message.encoded_len(version)?)
}
fn encode_null_optionals(version: i16) -> crate::MatrixResult<String> {
    let message = <DescribeQuorumResponseData as TestInstance>::test_null_optionals();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_null_optionals(version: i16) -> crate::MatrixResult<usize> {
    let message = <DescribeQuorumResponseData as TestInstance>::test_null_optionals();
    Ok(message.encoded_len(version)?)
}
fn encode_empty_collections(version: i16) -> crate::MatrixResult<String> {
    let message = <DescribeQuorumResponseData as TestInstance>::test_empty_collections();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_empty_collections(version: i16) -> crate::MatrixResult<usize> {
    let message = <DescribeQuorumResponseData as TestInstance>::test_empty_collections();
    Ok(message.encoded_len(version)?)
}
fn encode_multi_element_collections(version: i16) -> crate::MatrixResult<String> {
    let message = <DescribeQuorumResponseData as TestInstance>::test_multi_element_collections();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_multi_element_collections(version: i16) -> crate::MatrixResult<usize> {
    let message = <DescribeQuorumResponseData as TestInstance>::test_multi_element_collections();
    Ok(message.encoded_len(version)?)
}
fn encode_numeric_boundaries(version: i16) -> crate::MatrixResult<String> {
    let message = <DescribeQuorumResponseData as TestInstance>::test_numeric_boundaries();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_numeric_boundaries(version: i16) -> crate::MatrixResult<usize> {
    let message = <DescribeQuorumResponseData as TestInstance>::test_numeric_boundaries();
    Ok(message.encoded_len(version)?)
}
fn encode_tagged_fields(version: i16) -> crate::MatrixResult<String> {
    let message = <DescribeQuorumResponseData as TestInstance>::test_tagged_fields();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_tagged_fields(version: i16) -> crate::MatrixResult<usize> {
    let message = <DescribeQuorumResponseData as TestInstance>::test_tagged_fields();
    Ok(message.encoded_len(version)?)
}
fn reencode(version: i16, hex_input: &str) -> crate::MatrixResult<String> {
    let mut input = Bytes::from(crate::decode_hex(hex_input)?);
    let message = DescribeQuorumResponseData::read(&mut input, version)?;
    crate::ensure_input_consumed(&input)?;
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
const MATRIX_CASES: &[crate::MatrixCase] = &[
    crate::MatrixCase {
        schema_name: "DescribeQuorumResponse",
        java_class: "org.apache.kafka.common.message.DescribeQuorumResponseData",
        version: 0i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeQuorumResponse",
        java_class: "org.apache.kafka.common.message.DescribeQuorumResponseData",
        version: 1i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeQuorumResponse",
        java_class: "org.apache.kafka.common.message.DescribeQuorumResponseData",
        version: 2i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeQuorumResponse",
        java_class: "org.apache.kafka.common.message.DescribeQuorumResponseData",
        version: 0i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeQuorumResponse",
        java_class: "org.apache.kafka.common.message.DescribeQuorumResponseData",
        version: 1i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeQuorumResponse",
        java_class: "org.apache.kafka.common.message.DescribeQuorumResponseData",
        version: 2i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeQuorumResponse",
        java_class: "org.apache.kafka.common.message.DescribeQuorumResponseData",
        version: 0i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeQuorumResponse",
        java_class: "org.apache.kafka.common.message.DescribeQuorumResponseData",
        version: 0i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeQuorumResponse",
        java_class: "org.apache.kafka.common.message.DescribeQuorumResponseData",
        version: 0i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeQuorumResponse",
        java_class: "org.apache.kafka.common.message.DescribeQuorumResponseData",
        version: 0i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeQuorumResponse",
        java_class: "org.apache.kafka.common.message.DescribeQuorumResponseData",
        version: 1i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeQuorumResponse",
        java_class: "org.apache.kafka.common.message.DescribeQuorumResponseData",
        version: 1i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeQuorumResponse",
        java_class: "org.apache.kafka.common.message.DescribeQuorumResponseData",
        version: 1i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeQuorumResponse",
        java_class: "org.apache.kafka.common.message.DescribeQuorumResponseData",
        version: 1i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeQuorumResponse",
        java_class: "org.apache.kafka.common.message.DescribeQuorumResponseData",
        version: 2i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeQuorumResponse",
        java_class: "org.apache.kafka.common.message.DescribeQuorumResponseData",
        version: 2i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeQuorumResponse",
        java_class: "org.apache.kafka.common.message.DescribeQuorumResponseData",
        version: 2i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeQuorumResponse",
        java_class: "org.apache.kafka.common.message.DescribeQuorumResponseData",
        version: 2i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
];
pub(crate) fn append_protocol_cases(cases: &mut Vec<crate::MatrixCase>) {
    cases.extend_from_slice(MATRIX_CASES);
}
