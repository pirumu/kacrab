use bytes::{Bytes, BytesMut};
use kacrab_protocol::{generated::consumer_protocol_subscription::*, *};

use crate::TestInstance;

impl TestInstance for ConsumerProtocolSubscriptionData {
    fn test_populated() -> Self {
        Self {
            topics: vec![KafkaString::from("test".to_owned())],
            user_data: Some(Bytes::from_static(b"\xca\xfe")),
            owned_partitions: vec![<TopicPartition as TestInstance>::test_populated()],
            generation_id: 12345_i32,
            rack_id: Some(KafkaString::from("test".to_owned())),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_null_optionals() -> Self {
        Self {
            topics: vec![KafkaString::default()],
            user_data: None,
            owned_partitions: vec![<TopicPartition as TestInstance>::test_null_optionals()],
            generation_id: 0_i32,
            rack_id: None,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            topics: Vec::new(),
            user_data: Some(Bytes::new()),
            owned_partitions: Vec::new(),
            generation_id: 0_i32,
            rack_id: Some(KafkaString::default()),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            topics: vec![
                KafkaString::from("test".to_owned()),
                KafkaString::from("test-2".to_owned()),
            ],
            user_data: Some(Bytes::from_static(b"\x00\xff")),
            owned_partitions: vec![
                <TopicPartition as TestInstance>::test_populated(),
                <TopicPartition as TestInstance>::test_multi_element_collections(),
            ],
            generation_id: 23456_i32,
            rack_id: Some(KafkaString::from("test-2".to_owned())),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            topics: vec![KafkaString::from("boundary".to_owned())],
            user_data: Some(Bytes::from_static(b"\x00\xff")),
            owned_partitions: vec![<TopicPartition as TestInstance>::test_numeric_boundaries()],
            generation_id: i32::MIN,
            rack_id: Some(KafkaString::from("boundary".to_owned())),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            topics: vec![KafkaString::from("test".to_owned())],
            user_data: Some(Bytes::from_static(b"\xca\xfe")),
            owned_partitions: vec![<TopicPartition as TestInstance>::test_tagged_fields()],
            generation_id: 12345_i32,
            rack_id: Some(KafkaString::from("test".to_owned())),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl TestInstance for TopicPartition {
    fn test_populated() -> Self {
        Self {
            topic: KafkaString::from("test".to_owned()),
            partitions: vec![12345_i32],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_null_optionals() -> Self {
        drop(Self::default());
        Self {
            topic: KafkaString::default(),
            partitions: vec![0_i32],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            topic: KafkaString::default(),
            partitions: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            topic: KafkaString::from("test-2".to_owned()),
            partitions: vec![12345_i32, 23456_i32],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            topic: KafkaString::from("boundary".to_owned()),
            partitions: vec![i32::MIN],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            topic: KafkaString::from("test".to_owned()),
            partitions: vec![12345_i32],
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
fn encode_populated(version: i16) -> crate::MatrixResult<String> {
    let message = <ConsumerProtocolSubscriptionData as TestInstance>::test_populated();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_populated(version: i16) -> crate::MatrixResult<usize> {
    let message = <ConsumerProtocolSubscriptionData as TestInstance>::test_populated();
    Ok(message.encoded_len(version)?)
}
fn encode_null_optionals(version: i16) -> crate::MatrixResult<String> {
    let message = <ConsumerProtocolSubscriptionData as TestInstance>::test_null_optionals();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_null_optionals(version: i16) -> crate::MatrixResult<usize> {
    let message = <ConsumerProtocolSubscriptionData as TestInstance>::test_null_optionals();
    Ok(message.encoded_len(version)?)
}
fn encode_empty_collections(version: i16) -> crate::MatrixResult<String> {
    let message = <ConsumerProtocolSubscriptionData as TestInstance>::test_empty_collections();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_empty_collections(version: i16) -> crate::MatrixResult<usize> {
    let message = <ConsumerProtocolSubscriptionData as TestInstance>::test_empty_collections();
    Ok(message.encoded_len(version)?)
}
fn encode_multi_element_collections(version: i16) -> crate::MatrixResult<String> {
    let message =
        <ConsumerProtocolSubscriptionData as TestInstance>::test_multi_element_collections();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_multi_element_collections(version: i16) -> crate::MatrixResult<usize> {
    let message =
        <ConsumerProtocolSubscriptionData as TestInstance>::test_multi_element_collections();
    Ok(message.encoded_len(version)?)
}
fn encode_numeric_boundaries(version: i16) -> crate::MatrixResult<String> {
    let message = <ConsumerProtocolSubscriptionData as TestInstance>::test_numeric_boundaries();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_numeric_boundaries(version: i16) -> crate::MatrixResult<usize> {
    let message = <ConsumerProtocolSubscriptionData as TestInstance>::test_numeric_boundaries();
    Ok(message.encoded_len(version)?)
}
fn encode_tagged_fields(version: i16) -> crate::MatrixResult<String> {
    let message = <ConsumerProtocolSubscriptionData as TestInstance>::test_tagged_fields();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_tagged_fields(version: i16) -> crate::MatrixResult<usize> {
    let message = <ConsumerProtocolSubscriptionData as TestInstance>::test_tagged_fields();
    Ok(message.encoded_len(version)?)
}
fn reencode(version: i16, hex_input: &str) -> crate::MatrixResult<String> {
    let mut input = Bytes::from(crate::decode_hex(hex_input)?);
    let message = ConsumerProtocolSubscriptionData::read(&mut input, version)?;
    crate::ensure_input_consumed(&input)?;
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
const MATRIX_CASES: &[crate::MatrixCase] = &[
    crate::MatrixCase {
        schema_name: "ConsumerProtocolSubscription",
        java_class: "org.apache.kafka.common.message.ConsumerProtocolSubscription",
        version: 0i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ConsumerProtocolSubscription",
        java_class: "org.apache.kafka.common.message.ConsumerProtocolSubscription",
        version: 1i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ConsumerProtocolSubscription",
        java_class: "org.apache.kafka.common.message.ConsumerProtocolSubscription",
        version: 2i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ConsumerProtocolSubscription",
        java_class: "org.apache.kafka.common.message.ConsumerProtocolSubscription",
        version: 3i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ConsumerProtocolSubscription",
        java_class: "org.apache.kafka.common.message.ConsumerProtocolSubscription",
        version: 0i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ConsumerProtocolSubscription",
        java_class: "org.apache.kafka.common.message.ConsumerProtocolSubscription",
        version: 1i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ConsumerProtocolSubscription",
        java_class: "org.apache.kafka.common.message.ConsumerProtocolSubscription",
        version: 2i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ConsumerProtocolSubscription",
        java_class: "org.apache.kafka.common.message.ConsumerProtocolSubscription",
        version: 3i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ConsumerProtocolSubscription",
        java_class: "org.apache.kafka.common.message.ConsumerProtocolSubscription",
        version: 0i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ConsumerProtocolSubscription",
        java_class: "org.apache.kafka.common.message.ConsumerProtocolSubscription",
        version: 0i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ConsumerProtocolSubscription",
        java_class: "org.apache.kafka.common.message.ConsumerProtocolSubscription",
        version: 0i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ConsumerProtocolSubscription",
        java_class: "org.apache.kafka.common.message.ConsumerProtocolSubscription",
        version: 0i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ConsumerProtocolSubscription",
        java_class: "org.apache.kafka.common.message.ConsumerProtocolSubscription",
        version: 1i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ConsumerProtocolSubscription",
        java_class: "org.apache.kafka.common.message.ConsumerProtocolSubscription",
        version: 1i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ConsumerProtocolSubscription",
        java_class: "org.apache.kafka.common.message.ConsumerProtocolSubscription",
        version: 1i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ConsumerProtocolSubscription",
        java_class: "org.apache.kafka.common.message.ConsumerProtocolSubscription",
        version: 1i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ConsumerProtocolSubscription",
        java_class: "org.apache.kafka.common.message.ConsumerProtocolSubscription",
        version: 2i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ConsumerProtocolSubscription",
        java_class: "org.apache.kafka.common.message.ConsumerProtocolSubscription",
        version: 2i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ConsumerProtocolSubscription",
        java_class: "org.apache.kafka.common.message.ConsumerProtocolSubscription",
        version: 2i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ConsumerProtocolSubscription",
        java_class: "org.apache.kafka.common.message.ConsumerProtocolSubscription",
        version: 2i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ConsumerProtocolSubscription",
        java_class: "org.apache.kafka.common.message.ConsumerProtocolSubscription",
        version: 3i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ConsumerProtocolSubscription",
        java_class: "org.apache.kafka.common.message.ConsumerProtocolSubscription",
        version: 3i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ConsumerProtocolSubscription",
        java_class: "org.apache.kafka.common.message.ConsumerProtocolSubscription",
        version: 3i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ConsumerProtocolSubscription",
        java_class: "org.apache.kafka.common.message.ConsumerProtocolSubscription",
        version: 3i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
];
pub(crate) fn append_protocol_cases(cases: &mut Vec<crate::MatrixCase>) {
    cases.extend_from_slice(MATRIX_CASES);
}
