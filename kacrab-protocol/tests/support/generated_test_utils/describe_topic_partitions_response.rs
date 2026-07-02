#![allow(
    clippy::all,
    clippy::pedantic,
    clippy::nursery,
    reason = "Generated test fixtures mirror Kafka's schema shape and trade hand-written lint \
              style for reproducible output, matching the generated protocol modules."
)]
use bytes::{Bytes, BytesMut};
use kacrab_protocol::{generated::describe_topic_partitions_response::*, *};

use crate::TestInstance;

impl TestInstance for DescribeTopicPartitionsResponseData {
    fn test_populated(version: i16) -> Self {
        Self {
            throttle_time_ms: 12345_i32,
            topics: vec![
                <DescribeTopicPartitionsResponseTopic as TestInstance>::test_populated(version),
            ],
            next_cursor: Some(Box::new(<Cursor as TestInstance>::test_populated(version))),
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals(version: i16) -> Self {
        drop(<Cursor as TestInstance>::test_null_optionals(version));
        Self {
            throttle_time_ms: 0_i32,
            topics: vec![
                <DescribeTopicPartitionsResponseTopic as TestInstance>::test_null_optionals(
                    version,
                ),
            ],
            next_cursor: None,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections(version: i16) -> Self {
        Self {
            throttle_time_ms: 0_i32,
            topics: Vec::new(),
            next_cursor: Some(Box::new(<Cursor as TestInstance>::test_null_optionals(
                version,
            ))),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections(version: i16) -> Self {
        Self {
            throttle_time_ms: 23456_i32,
            topics: vec![
                < DescribeTopicPartitionsResponseTopic as TestInstance >
                ::test_populated(version), < DescribeTopicPartitionsResponseTopic as
                TestInstance > ::test_multi_element_collections(version)
            ],
            next_cursor: Some(Box::new(
                <Cursor as TestInstance>::test_multi_element_collections(version),
            )),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries(version: i16) -> Self {
        Self {
            throttle_time_ms: i32::MIN,
            topics: vec![
                <DescribeTopicPartitionsResponseTopic as TestInstance>::test_numeric_boundaries(
                    version,
                ),
            ],
            next_cursor: Some(Box::new(<Cursor as TestInstance>::test_numeric_boundaries(
                version,
            ))),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields(version: i16) -> Self {
        Self {
            throttle_time_ms: 12345_i32,
            topics: vec![
                <DescribeTopicPartitionsResponseTopic as TestInstance>::test_tagged_fields(version),
            ],
            next_cursor: Some(Box::new(<Cursor as TestInstance>::test_tagged_fields(
                version,
            ))),
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for DescribeTopicPartitionsResponseTopic {
    fn test_populated(version: i16) -> Self {
        Self {
            error_code: 42_i16,
            name: Some(KafkaString::from("test".to_owned())),
            topic_id: KafkaUuid::ONE,
            is_internal: true,
            partitions: vec![
                <DescribeTopicPartitionsResponsePartition as TestInstance>::test_populated(version),
            ],
            topic_authorized_operations: 12345_i32,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals(version: i16) -> Self {
        drop(Self::default());
        Self {
            error_code: 0_i16,
            name: None,
            topic_id: KafkaUuid::ZERO,
            is_internal: false,
            partitions: vec![
                <DescribeTopicPartitionsResponsePartition as TestInstance>::test_null_optionals(
                    version,
                ),
            ],
            topic_authorized_operations: 0_i32,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections(_version: i16) -> Self {
        Self {
            error_code: 0_i16,
            name: Some(KafkaString::default()),
            topic_id: KafkaUuid::ZERO,
            is_internal: false,
            partitions: Vec::new(),
            topic_authorized_operations: 0_i32,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections(version: i16) -> Self {
        Self {
            error_code: 43_i16,
            name: Some(KafkaString::from("test-2".to_owned())),
            topic_id: KafkaUuid::from_parts(2, 3),
            is_internal: false,
            partitions: vec![
                < DescribeTopicPartitionsResponsePartition as TestInstance >
                ::test_populated(version), < DescribeTopicPartitionsResponsePartition as
                TestInstance > ::test_multi_element_collections(version)
            ],
            topic_authorized_operations: 23456_i32,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries(version: i16) -> Self {
        Self {
            error_code: i16::MIN,
            name: Some(KafkaString::from("boundary".to_owned())),
            topic_id: KafkaUuid::ONE,
            is_internal: true,
            partitions: vec![
                <DescribeTopicPartitionsResponsePartition as TestInstance>::test_numeric_boundaries(
                    version,
                ),
            ],
            topic_authorized_operations: i32::MIN,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields(version: i16) -> Self {
        Self {
            error_code: 42_i16,
            name: Some(KafkaString::from("test".to_owned())),
            topic_id: KafkaUuid::ONE,
            is_internal: true,
            partitions: vec![
                <DescribeTopicPartitionsResponsePartition as TestInstance>::test_tagged_fields(
                    version,
                ),
            ],
            topic_authorized_operations: 12345_i32,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for DescribeTopicPartitionsResponsePartition {
    fn test_populated(_version: i16) -> Self {
        Self {
            error_code: 42_i16,
            partition_index: 12345_i32,
            leader_id: 12345_i32,
            leader_epoch: 12345_i32,
            replica_nodes: vec![12345_i32],
            isr_nodes: vec![12345_i32],
            eligible_leader_replicas: Some(vec![12345_i32]),
            last_known_elr: Some(vec![12345_i32]),
            offline_replicas: vec![12345_i32],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals(_version: i16) -> Self {
        drop(Self::default());
        Self {
            error_code: 0_i16,
            partition_index: 0_i32,
            leader_id: 0_i32,
            leader_epoch: 0_i32,
            replica_nodes: vec![0_i32],
            isr_nodes: vec![0_i32],
            eligible_leader_replicas: None,
            last_known_elr: None,
            offline_replicas: vec![0_i32],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections(_version: i16) -> Self {
        Self {
            error_code: 0_i16,
            partition_index: 0_i32,
            leader_id: 0_i32,
            leader_epoch: 0_i32,
            replica_nodes: Vec::new(),
            isr_nodes: Vec::new(),
            eligible_leader_replicas: Some(Vec::new()),
            last_known_elr: Some(Vec::new()),
            offline_replicas: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections(_version: i16) -> Self {
        Self {
            error_code: 43_i16,
            partition_index: 23456_i32,
            leader_id: 23456_i32,
            leader_epoch: 23456_i32,
            replica_nodes: vec![12345_i32, 23456_i32],
            isr_nodes: vec![12345_i32, 23456_i32],
            eligible_leader_replicas: Some(vec![12345_i32, 23456_i32]),
            last_known_elr: Some(vec![12345_i32, 23456_i32]),
            offline_replicas: vec![12345_i32, 23456_i32],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries(_version: i16) -> Self {
        Self {
            error_code: i16::MIN,
            partition_index: i32::MIN,
            leader_id: i32::MIN,
            leader_epoch: i32::MIN,
            replica_nodes: vec![i32::MIN],
            isr_nodes: vec![i32::MIN],
            eligible_leader_replicas: Some(vec![i32::MIN]),
            last_known_elr: Some(vec![i32::MIN]),
            offline_replicas: vec![i32::MIN],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields(_version: i16) -> Self {
        Self {
            error_code: 42_i16,
            partition_index: 12345_i32,
            leader_id: 12345_i32,
            leader_epoch: 12345_i32,
            replica_nodes: vec![12345_i32],
            isr_nodes: vec![12345_i32],
            eligible_leader_replicas: Some(vec![12345_i32]),
            last_known_elr: Some(vec![12345_i32]),
            offline_replicas: vec![12345_i32],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for Cursor {
    fn test_populated(_version: i16) -> Self {
        Self {
            topic_name: KafkaString::from("test".to_owned()),
            partition_index: 12345_i32,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals(_version: i16) -> Self {
        drop(Self::default());
        Self {
            topic_name: KafkaString::default(),
            partition_index: 0_i32,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections(_version: i16) -> Self {
        Self {
            topic_name: KafkaString::default(),
            partition_index: 0_i32,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections(_version: i16) -> Self {
        Self {
            topic_name: KafkaString::from("test-2".to_owned()),
            partition_index: 23456_i32,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries(_version: i16) -> Self {
        Self {
            topic_name: KafkaString::from("boundary".to_owned()),
            partition_index: i32::MIN,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields(_version: i16) -> Self {
        Self {
            topic_name: KafkaString::from("test".to_owned()),
            partition_index: 12345_i32,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
fn encode_populated(version: i16) -> crate::MatrixResult<String> {
    let message = <DescribeTopicPartitionsResponseData as TestInstance>::test_populated(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_populated(version: i16) -> crate::MatrixResult<usize> {
    let message = <DescribeTopicPartitionsResponseData as TestInstance>::test_populated(version);
    Ok(message.encoded_len(version)?)
}
fn encode_null_optionals(version: i16) -> crate::MatrixResult<String> {
    let message =
        <DescribeTopicPartitionsResponseData as TestInstance>::test_null_optionals(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_null_optionals(version: i16) -> crate::MatrixResult<usize> {
    let message =
        <DescribeTopicPartitionsResponseData as TestInstance>::test_null_optionals(version);
    Ok(message.encoded_len(version)?)
}
fn encode_empty_collections(version: i16) -> crate::MatrixResult<String> {
    let message =
        <DescribeTopicPartitionsResponseData as TestInstance>::test_empty_collections(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_empty_collections(version: i16) -> crate::MatrixResult<usize> {
    let message =
        <DescribeTopicPartitionsResponseData as TestInstance>::test_empty_collections(version);
    Ok(message.encoded_len(version)?)
}
fn encode_multi_element_collections(version: i16) -> crate::MatrixResult<String> {
    let message =
        <DescribeTopicPartitionsResponseData as TestInstance>::test_multi_element_collections(
            version,
        );
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_multi_element_collections(version: i16) -> crate::MatrixResult<usize> {
    let message =
        <DescribeTopicPartitionsResponseData as TestInstance>::test_multi_element_collections(
            version,
        );
    Ok(message.encoded_len(version)?)
}
fn encode_numeric_boundaries(version: i16) -> crate::MatrixResult<String> {
    let message =
        <DescribeTopicPartitionsResponseData as TestInstance>::test_numeric_boundaries(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_numeric_boundaries(version: i16) -> crate::MatrixResult<usize> {
    let message =
        <DescribeTopicPartitionsResponseData as TestInstance>::test_numeric_boundaries(version);
    Ok(message.encoded_len(version)?)
}
fn encode_tagged_fields(version: i16) -> crate::MatrixResult<String> {
    let message =
        <DescribeTopicPartitionsResponseData as TestInstance>::test_tagged_fields(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_tagged_fields(version: i16) -> crate::MatrixResult<usize> {
    let message =
        <DescribeTopicPartitionsResponseData as TestInstance>::test_tagged_fields(version);
    Ok(message.encoded_len(version)?)
}
fn reencode(version: i16, hex_input: &str) -> crate::MatrixResult<String> {
    let mut input = Bytes::from(crate::decode_hex(hex_input)?);
    let message = DescribeTopicPartitionsResponseData::read(&mut input, version)?;
    crate::ensure_input_consumed(&input)?;
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
const MATRIX_CASES: &[crate::MatrixCase] = &[
    crate::MatrixCase {
        schema_name: "DescribeTopicPartitionsResponse",
        java_class: "org.apache.kafka.common.message.DescribeTopicPartitionsResponseData",
        version: 0i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeTopicPartitionsResponse",
        java_class: "org.apache.kafka.common.message.DescribeTopicPartitionsResponseData",
        version: 0i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeTopicPartitionsResponse",
        java_class: "org.apache.kafka.common.message.DescribeTopicPartitionsResponseData",
        version: 0i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeTopicPartitionsResponse",
        java_class: "org.apache.kafka.common.message.DescribeTopicPartitionsResponseData",
        version: 0i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeTopicPartitionsResponse",
        java_class: "org.apache.kafka.common.message.DescribeTopicPartitionsResponseData",
        version: 0i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeTopicPartitionsResponse",
        java_class: "org.apache.kafka.common.message.DescribeTopicPartitionsResponseData",
        version: 0i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
];
pub(crate) fn append_protocol_cases(cases: &mut Vec<crate::MatrixCase>) {
    cases.extend_from_slice(MATRIX_CASES);
}
