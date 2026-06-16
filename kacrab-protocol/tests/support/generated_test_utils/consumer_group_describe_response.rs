use bytes::{Bytes, BytesMut};
use kacrab_protocol::{generated::consumer_group_describe_response::*, *};

use crate::TestInstance;

impl TestInstance for ConsumerGroupDescribeResponseData {
    fn test_populated() -> Self {
        Self {
            throttle_time_ms: 12345_i32,
            groups: vec![<DescribedGroup as TestInstance>::test_populated()],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals() -> Self {
        Self {
            throttle_time_ms: 0_i32,
            groups: vec![<DescribedGroup as TestInstance>::test_null_optionals()],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            throttle_time_ms: 0_i32,
            groups: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            throttle_time_ms: 23456_i32,
            groups: vec![
                <DescribedGroup as TestInstance>::test_populated(),
                <DescribedGroup as TestInstance>::test_multi_element_collections(),
            ],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            throttle_time_ms: i32::MIN,
            groups: vec![<DescribedGroup as TestInstance>::test_numeric_boundaries()],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            throttle_time_ms: 12345_i32,
            groups: vec![<DescribedGroup as TestInstance>::test_tagged_fields()],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for DescribedGroup {
    fn test_populated() -> Self {
        Self {
            error_code: 42_i16,
            error_message: Some(KafkaString::from("test".to_owned())),
            group_id: KafkaString::from("test".to_owned()),
            group_state: KafkaString::from("test".to_owned()),
            group_epoch: 12345_i32,
            assignment_epoch: 12345_i32,
            assignor_name: KafkaString::from("test".to_owned()),
            members: vec![<Member as TestInstance>::test_populated()],
            authorized_operations: 12345_i32,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals() -> Self {
        drop(Self::default());
        Self {
            error_code: 0_i16,
            error_message: None,
            group_id: KafkaString::default(),
            group_state: KafkaString::default(),
            group_epoch: 0_i32,
            assignment_epoch: 0_i32,
            assignor_name: KafkaString::default(),
            members: vec![<Member as TestInstance>::test_null_optionals()],
            authorized_operations: 0_i32,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            error_code: 0_i16,
            error_message: Some(KafkaString::default()),
            group_id: KafkaString::default(),
            group_state: KafkaString::default(),
            group_epoch: 0_i32,
            assignment_epoch: 0_i32,
            assignor_name: KafkaString::default(),
            members: Vec::new(),
            authorized_operations: 0_i32,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            error_code: 43_i16,
            error_message: Some(KafkaString::from("test-2".to_owned())),
            group_id: KafkaString::from("test-2".to_owned()),
            group_state: KafkaString::from("test-2".to_owned()),
            group_epoch: 23456_i32,
            assignment_epoch: 23456_i32,
            assignor_name: KafkaString::from("test-2".to_owned()),
            members: vec![
                <Member as TestInstance>::test_populated(),
                <Member as TestInstance>::test_multi_element_collections(),
            ],
            authorized_operations: 23456_i32,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            error_code: i16::MIN,
            error_message: Some(KafkaString::from("boundary".to_owned())),
            group_id: KafkaString::from("boundary".to_owned()),
            group_state: KafkaString::from("boundary".to_owned()),
            group_epoch: i32::MIN,
            assignment_epoch: i32::MIN,
            assignor_name: KafkaString::from("boundary".to_owned()),
            members: vec![<Member as TestInstance>::test_numeric_boundaries()],
            authorized_operations: i32::MIN,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            error_code: 42_i16,
            error_message: Some(KafkaString::from("test".to_owned())),
            group_id: KafkaString::from("test".to_owned()),
            group_state: KafkaString::from("test".to_owned()),
            group_epoch: 12345_i32,
            assignment_epoch: 12345_i32,
            assignor_name: KafkaString::from("test".to_owned()),
            members: vec![<Member as TestInstance>::test_tagged_fields()],
            authorized_operations: 12345_i32,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for Member {
    fn test_populated() -> Self {
        Self {
            member_id: KafkaString::from("test".to_owned()),
            instance_id: Some(KafkaString::from("test".to_owned())),
            rack_id: Some(KafkaString::from("test".to_owned())),
            member_epoch: 12345_i32,
            client_id: KafkaString::from("test".to_owned()),
            client_host: KafkaString::from("test".to_owned()),
            subscribed_topic_names: vec![KafkaString::from("test".to_owned())],
            subscribed_topic_regex: Some(KafkaString::from("test".to_owned())),
            assignment: <Assignment as TestInstance>::test_populated(),
            target_assignment: <Assignment as TestInstance>::test_populated(),
            member_type: 7_i8,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals() -> Self {
        drop(Self::default());
        Self {
            member_id: KafkaString::default(),
            instance_id: None,
            rack_id: None,
            member_epoch: 0_i32,
            client_id: KafkaString::default(),
            client_host: KafkaString::default(),
            subscribed_topic_names: vec![KafkaString::default()],
            subscribed_topic_regex: None,
            assignment: <Assignment as TestInstance>::test_null_optionals(),
            target_assignment: <Assignment as TestInstance>::test_null_optionals(),
            member_type: 0_i8,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            member_id: KafkaString::default(),
            instance_id: Some(KafkaString::default()),
            rack_id: Some(KafkaString::default()),
            member_epoch: 0_i32,
            client_id: KafkaString::default(),
            client_host: KafkaString::default(),
            subscribed_topic_names: Vec::new(),
            subscribed_topic_regex: Some(KafkaString::default()),
            assignment: <Assignment as TestInstance>::test_null_optionals(),
            target_assignment: <Assignment as TestInstance>::test_null_optionals(),
            member_type: 0_i8,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            member_id: KafkaString::from("test-2".to_owned()),
            instance_id: Some(KafkaString::from("test-2".to_owned())),
            rack_id: Some(KafkaString::from("test-2".to_owned())),
            member_epoch: 23456_i32,
            client_id: KafkaString::from("test-2".to_owned()),
            client_host: KafkaString::from("test-2".to_owned()),
            subscribed_topic_names: vec![
                KafkaString::from("test".to_owned()),
                KafkaString::from("test-2".to_owned()),
            ],
            subscribed_topic_regex: Some(KafkaString::from("test-2".to_owned())),
            assignment: <Assignment as TestInstance>::test_multi_element_collections(),
            target_assignment: <Assignment as TestInstance>::test_multi_element_collections(),
            member_type: 8_i8,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            member_id: KafkaString::from("boundary".to_owned()),
            instance_id: Some(KafkaString::from("boundary".to_owned())),
            rack_id: Some(KafkaString::from("boundary".to_owned())),
            member_epoch: i32::MIN,
            client_id: KafkaString::from("boundary".to_owned()),
            client_host: KafkaString::from("boundary".to_owned()),
            subscribed_topic_names: vec![KafkaString::from("boundary".to_owned())],
            subscribed_topic_regex: Some(KafkaString::from("boundary".to_owned())),
            assignment: <Assignment as TestInstance>::test_numeric_boundaries(),
            target_assignment: <Assignment as TestInstance>::test_numeric_boundaries(),
            member_type: i8::MIN,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            member_id: KafkaString::from("test".to_owned()),
            instance_id: Some(KafkaString::from("test".to_owned())),
            rack_id: Some(KafkaString::from("test".to_owned())),
            member_epoch: 12345_i32,
            client_id: KafkaString::from("test".to_owned()),
            client_host: KafkaString::from("test".to_owned()),
            subscribed_topic_names: vec![KafkaString::from("test".to_owned())],
            subscribed_topic_regex: Some(KafkaString::from("test".to_owned())),
            assignment: <Assignment as TestInstance>::test_tagged_fields(),
            target_assignment: <Assignment as TestInstance>::test_tagged_fields(),
            member_type: 7_i8,
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
            topic_name: KafkaString::from("test".to_owned()),
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
            topic_name: KafkaString::default(),
            partitions: vec![0_i32],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            topic_id: KafkaUuid::ZERO,
            topic_name: KafkaString::default(),
            partitions: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            topic_id: KafkaUuid::from_parts(2, 3),
            topic_name: KafkaString::from("test-2".to_owned()),
            partitions: vec![12345_i32, 23456_i32],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            topic_id: KafkaUuid::ONE,
            topic_name: KafkaString::from("boundary".to_owned()),
            partitions: vec![i32::MIN],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            topic_id: KafkaUuid::ONE,
            topic_name: KafkaString::from("test".to_owned()),
            partitions: vec![12345_i32],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for Assignment {
    fn test_populated() -> Self {
        Self {
            topic_partitions: vec![<TopicPartitions as TestInstance>::test_populated()],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals() -> Self {
        drop(Self::default());
        Self {
            topic_partitions: vec![<TopicPartitions as TestInstance>::test_null_optionals()],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            topic_partitions: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            topic_partitions: vec![
                <TopicPartitions as TestInstance>::test_populated(),
                <TopicPartitions as TestInstance>::test_multi_element_collections(),
            ],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            topic_partitions: vec![<TopicPartitions as TestInstance>::test_numeric_boundaries()],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            topic_partitions: vec![<TopicPartitions as TestInstance>::test_tagged_fields()],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
fn encode_populated(version: i16) -> crate::MatrixResult<String> {
    let message = <ConsumerGroupDescribeResponseData as TestInstance>::test_populated();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_populated(version: i16) -> crate::MatrixResult<usize> {
    let message = <ConsumerGroupDescribeResponseData as TestInstance>::test_populated();
    Ok(message.encoded_len(version)?)
}
fn encode_null_optionals(version: i16) -> crate::MatrixResult<String> {
    let message = <ConsumerGroupDescribeResponseData as TestInstance>::test_null_optionals();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_null_optionals(version: i16) -> crate::MatrixResult<usize> {
    let message = <ConsumerGroupDescribeResponseData as TestInstance>::test_null_optionals();
    Ok(message.encoded_len(version)?)
}
fn encode_empty_collections(version: i16) -> crate::MatrixResult<String> {
    let message = <ConsumerGroupDescribeResponseData as TestInstance>::test_empty_collections();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_empty_collections(version: i16) -> crate::MatrixResult<usize> {
    let message = <ConsumerGroupDescribeResponseData as TestInstance>::test_empty_collections();
    Ok(message.encoded_len(version)?)
}
fn encode_multi_element_collections(version: i16) -> crate::MatrixResult<String> {
    let message =
        <ConsumerGroupDescribeResponseData as TestInstance>::test_multi_element_collections();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_multi_element_collections(version: i16) -> crate::MatrixResult<usize> {
    let message =
        <ConsumerGroupDescribeResponseData as TestInstance>::test_multi_element_collections();
    Ok(message.encoded_len(version)?)
}
fn encode_numeric_boundaries(version: i16) -> crate::MatrixResult<String> {
    let message = <ConsumerGroupDescribeResponseData as TestInstance>::test_numeric_boundaries();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_numeric_boundaries(version: i16) -> crate::MatrixResult<usize> {
    let message = <ConsumerGroupDescribeResponseData as TestInstance>::test_numeric_boundaries();
    Ok(message.encoded_len(version)?)
}
fn encode_tagged_fields(version: i16) -> crate::MatrixResult<String> {
    let message = <ConsumerGroupDescribeResponseData as TestInstance>::test_tagged_fields();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_tagged_fields(version: i16) -> crate::MatrixResult<usize> {
    let message = <ConsumerGroupDescribeResponseData as TestInstance>::test_tagged_fields();
    Ok(message.encoded_len(version)?)
}
fn reencode(version: i16, hex_input: &str) -> crate::MatrixResult<String> {
    let mut input = Bytes::from(crate::decode_hex(hex_input)?);
    let message = ConsumerGroupDescribeResponseData::read(&mut input, version)?;
    crate::ensure_input_consumed(&input)?;
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
const MATRIX_CASES: &[crate::MatrixCase] = &[
    crate::MatrixCase {
        schema_name: "ConsumerGroupDescribeResponse",
        java_class: "org.apache.kafka.common.message.ConsumerGroupDescribeResponseData",
        version: 0i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ConsumerGroupDescribeResponse",
        java_class: "org.apache.kafka.common.message.ConsumerGroupDescribeResponseData",
        version: 1i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ConsumerGroupDescribeResponse",
        java_class: "org.apache.kafka.common.message.ConsumerGroupDescribeResponseData",
        version: 0i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ConsumerGroupDescribeResponse",
        java_class: "org.apache.kafka.common.message.ConsumerGroupDescribeResponseData",
        version: 1i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ConsumerGroupDescribeResponse",
        java_class: "org.apache.kafka.common.message.ConsumerGroupDescribeResponseData",
        version: 0i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ConsumerGroupDescribeResponse",
        java_class: "org.apache.kafka.common.message.ConsumerGroupDescribeResponseData",
        version: 0i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ConsumerGroupDescribeResponse",
        java_class: "org.apache.kafka.common.message.ConsumerGroupDescribeResponseData",
        version: 0i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ConsumerGroupDescribeResponse",
        java_class: "org.apache.kafka.common.message.ConsumerGroupDescribeResponseData",
        version: 0i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ConsumerGroupDescribeResponse",
        java_class: "org.apache.kafka.common.message.ConsumerGroupDescribeResponseData",
        version: 1i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ConsumerGroupDescribeResponse",
        java_class: "org.apache.kafka.common.message.ConsumerGroupDescribeResponseData",
        version: 1i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ConsumerGroupDescribeResponse",
        java_class: "org.apache.kafka.common.message.ConsumerGroupDescribeResponseData",
        version: 1i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ConsumerGroupDescribeResponse",
        java_class: "org.apache.kafka.common.message.ConsumerGroupDescribeResponseData",
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
