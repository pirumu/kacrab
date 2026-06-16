use bytes::{Bytes, BytesMut};
use kacrab_protocol::{generated::consumer_group_heartbeat_request::*, *};

use crate::TestInstance;

impl TestInstance for ConsumerGroupHeartbeatRequestData {
    fn test_populated() -> Self {
        Self {
            group_id: KafkaString::from("test".to_owned()),
            member_id: KafkaString::from("test".to_owned()),
            member_epoch: 12345_i32,
            instance_id: Some(KafkaString::from("test".to_owned())),
            rack_id: Some(KafkaString::from("test".to_owned())),
            rebalance_timeout_ms: 12345_i32,
            subscribed_topic_names: Some(vec![KafkaString::from("test".to_owned())]),
            subscribed_topic_regex: Some(KafkaString::from("test".to_owned())),
            server_assignor: Some(KafkaString::from("test".to_owned())),
            topic_partitions: Some(vec![<TopicPartitions as TestInstance>::test_populated()]),
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals() -> Self {
        drop(<TopicPartitions as TestInstance>::test_null_optionals());
        Self {
            group_id: KafkaString::default(),
            member_id: KafkaString::default(),
            member_epoch: 0_i32,
            instance_id: None,
            rack_id: None,
            rebalance_timeout_ms: 0_i32,
            subscribed_topic_names: None,
            subscribed_topic_regex: None,
            server_assignor: None,
            topic_partitions: None,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            group_id: KafkaString::default(),
            member_id: KafkaString::default(),
            member_epoch: 0_i32,
            instance_id: Some(KafkaString::default()),
            rack_id: Some(KafkaString::default()),
            rebalance_timeout_ms: 0_i32,
            subscribed_topic_names: Some(Vec::new()),
            subscribed_topic_regex: Some(KafkaString::default()),
            server_assignor: Some(KafkaString::default()),
            topic_partitions: Some(Vec::new()),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            group_id: KafkaString::from("test-2".to_owned()),
            member_id: KafkaString::from("test-2".to_owned()),
            member_epoch: 23456_i32,
            instance_id: Some(KafkaString::from("test-2".to_owned())),
            rack_id: Some(KafkaString::from("test-2".to_owned())),
            rebalance_timeout_ms: 23456_i32,
            subscribed_topic_names: Some(vec![
                KafkaString::from("test".to_owned()),
                KafkaString::from("test-2".to_owned()),
            ]),
            subscribed_topic_regex: Some(KafkaString::from("test-2".to_owned())),
            server_assignor: Some(KafkaString::from("test-2".to_owned())),
            topic_partitions: Some(vec![
                <TopicPartitions as TestInstance>::test_populated(),
                <TopicPartitions as TestInstance>::test_multi_element_collections(),
            ]),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            group_id: KafkaString::from("boundary".to_owned()),
            member_id: KafkaString::from("boundary".to_owned()),
            member_epoch: i32::MIN,
            instance_id: Some(KafkaString::from("boundary".to_owned())),
            rack_id: Some(KafkaString::from("boundary".to_owned())),
            rebalance_timeout_ms: i32::MIN,
            subscribed_topic_names: Some(vec![KafkaString::from("boundary".to_owned())]),
            subscribed_topic_regex: Some(KafkaString::from("boundary".to_owned())),
            server_assignor: Some(KafkaString::from("boundary".to_owned())),
            topic_partitions: Some(vec![
                <TopicPartitions as TestInstance>::test_numeric_boundaries(),
            ]),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            group_id: KafkaString::from("test".to_owned()),
            member_id: KafkaString::from("test".to_owned()),
            member_epoch: 12345_i32,
            instance_id: Some(KafkaString::from("test".to_owned())),
            rack_id: Some(KafkaString::from("test".to_owned())),
            rebalance_timeout_ms: 12345_i32,
            subscribed_topic_names: Some(vec![KafkaString::from("test".to_owned())]),
            subscribed_topic_regex: Some(KafkaString::from("test".to_owned())),
            server_assignor: Some(KafkaString::from("test".to_owned())),
            topic_partitions: Some(vec![<TopicPartitions as TestInstance>::test_tagged_fields()]),
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for TopicPartitions {
    fn test_populated() -> Self {
        Self {
            topic_id: KafkaUuid::ONE,
            partitions: vec![12345_i32],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals() -> Self {
        drop(Self::default());
        Self {
            topic_id: KafkaUuid::ZERO,
            partitions: vec![0_i32],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            topic_id: KafkaUuid::ZERO,
            partitions: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            topic_id: KafkaUuid::from_parts(2, 3),
            partitions: vec![12345_i32, 23456_i32],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            topic_id: KafkaUuid::ONE,
            partitions: vec![i32::MIN],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            topic_id: KafkaUuid::ONE,
            partitions: vec![12345_i32],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
fn encode_populated(version: i16) -> crate::MatrixResult<String> {
    let message = <ConsumerGroupHeartbeatRequestData as TestInstance>::test_populated();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_populated(version: i16) -> crate::MatrixResult<usize> {
    let message = <ConsumerGroupHeartbeatRequestData as TestInstance>::test_populated();
    Ok(message.encoded_len(version)?)
}
fn encode_null_optionals(version: i16) -> crate::MatrixResult<String> {
    let message = <ConsumerGroupHeartbeatRequestData as TestInstance>::test_null_optionals();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_null_optionals(version: i16) -> crate::MatrixResult<usize> {
    let message = <ConsumerGroupHeartbeatRequestData as TestInstance>::test_null_optionals();
    Ok(message.encoded_len(version)?)
}
fn encode_empty_collections(version: i16) -> crate::MatrixResult<String> {
    let message = <ConsumerGroupHeartbeatRequestData as TestInstance>::test_empty_collections();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_empty_collections(version: i16) -> crate::MatrixResult<usize> {
    let message = <ConsumerGroupHeartbeatRequestData as TestInstance>::test_empty_collections();
    Ok(message.encoded_len(version)?)
}
fn encode_multi_element_collections(version: i16) -> crate::MatrixResult<String> {
    let message =
        <ConsumerGroupHeartbeatRequestData as TestInstance>::test_multi_element_collections();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_multi_element_collections(version: i16) -> crate::MatrixResult<usize> {
    let message =
        <ConsumerGroupHeartbeatRequestData as TestInstance>::test_multi_element_collections();
    Ok(message.encoded_len(version)?)
}
fn encode_numeric_boundaries(version: i16) -> crate::MatrixResult<String> {
    let message = <ConsumerGroupHeartbeatRequestData as TestInstance>::test_numeric_boundaries();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_numeric_boundaries(version: i16) -> crate::MatrixResult<usize> {
    let message = <ConsumerGroupHeartbeatRequestData as TestInstance>::test_numeric_boundaries();
    Ok(message.encoded_len(version)?)
}
fn encode_tagged_fields(version: i16) -> crate::MatrixResult<String> {
    let message = <ConsumerGroupHeartbeatRequestData as TestInstance>::test_tagged_fields();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_tagged_fields(version: i16) -> crate::MatrixResult<usize> {
    let message = <ConsumerGroupHeartbeatRequestData as TestInstance>::test_tagged_fields();
    Ok(message.encoded_len(version)?)
}
fn reencode(version: i16, hex_input: &str) -> crate::MatrixResult<String> {
    let mut input = Bytes::from(crate::decode_hex(hex_input)?);
    let message = ConsumerGroupHeartbeatRequestData::read(&mut input, version)?;
    crate::ensure_input_consumed(&input)?;
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
const MATRIX_CASES: &[crate::MatrixCase] = &[
    crate::MatrixCase {
        schema_name: "ConsumerGroupHeartbeatRequest",
        java_class: "org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData",
        version: 0i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ConsumerGroupHeartbeatRequest",
        java_class: "org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData",
        version: 1i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ConsumerGroupHeartbeatRequest",
        java_class: "org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData",
        version: 0i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ConsumerGroupHeartbeatRequest",
        java_class: "org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData",
        version: 1i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ConsumerGroupHeartbeatRequest",
        java_class: "org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData",
        version: 0i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ConsumerGroupHeartbeatRequest",
        java_class: "org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData",
        version: 0i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ConsumerGroupHeartbeatRequest",
        java_class: "org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData",
        version: 0i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ConsumerGroupHeartbeatRequest",
        java_class: "org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData",
        version: 0i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ConsumerGroupHeartbeatRequest",
        java_class: "org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData",
        version: 1i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ConsumerGroupHeartbeatRequest",
        java_class: "org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData",
        version: 1i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ConsumerGroupHeartbeatRequest",
        java_class: "org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData",
        version: 1i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ConsumerGroupHeartbeatRequest",
        java_class: "org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData",
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
