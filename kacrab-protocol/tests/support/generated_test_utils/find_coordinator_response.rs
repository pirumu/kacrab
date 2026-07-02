#![allow(
    clippy::all,
    clippy::pedantic,
    clippy::nursery,
    reason = "Generated test fixtures mirror Kafka's schema shape and trade hand-written lint \
              style for reproducible output, matching the generated protocol modules."
)]
use bytes::{Bytes, BytesMut};
use kacrab_protocol::{generated::find_coordinator_response::*, *};

use crate::TestInstance;

impl TestInstance for FindCoordinatorResponseData {
    fn test_populated(version: i16) -> Self {
        Self {
            throttle_time_ms: if version >= 1 { 12345_i32 } else { 0_i32 },
            error_code: if version <= 3 { 42_i16 } else { 0_i16 },
            error_message: (version >= 1 && version <= 3)
                .then(|| Some(KafkaString::from("test".to_owned())))
                .flatten(),
            node_id: if version <= 3 { 12345_i32 } else { 0_i32 },
            host: if version <= 3 {
                KafkaString::from("test".to_owned())
            } else {
                KafkaString::default()
            },
            port: if version <= 3 { 12345_i32 } else { 0_i32 },
            coordinators: if version >= 4 {
                vec![<Coordinator as TestInstance>::test_populated(version)]
            } else {
                Vec::new()
            },
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals(version: i16) -> Self {
        Self {
            throttle_time_ms: 0_i32,
            error_code: 0_i16,
            error_message: None,
            node_id: 0_i32,
            host: KafkaString::default(),
            port: 0_i32,
            coordinators: if version >= 4 {
                vec![<Coordinator as TestInstance>::test_null_optionals(version)]
            } else {
                Vec::new()
            },
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections(version: i16) -> Self {
        Self {
            throttle_time_ms: 0_i32,
            error_code: 0_i16,
            error_message: (version >= 1 && version <= 3)
                .then(|| Some(KafkaString::default()))
                .flatten(),
            node_id: 0_i32,
            host: KafkaString::default(),
            port: 0_i32,
            coordinators: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections(version: i16) -> Self {
        Self {
            throttle_time_ms: if version >= 1 { 23456_i32 } else { 0_i32 },
            error_code: if version <= 3 { 43_i16 } else { 0_i16 },
            error_message: (version >= 1 && version <= 3)
                .then(|| Some(KafkaString::from("test-2".to_owned())))
                .flatten(),
            node_id: if version <= 3 { 23456_i32 } else { 0_i32 },
            host: if version <= 3 {
                KafkaString::from("test-2".to_owned())
            } else {
                KafkaString::default()
            },
            port: if version <= 3 { 23456_i32 } else { 0_i32 },
            coordinators: if version >= 4 {
                vec![
                    <Coordinator as TestInstance>::test_populated(version),
                    <Coordinator as TestInstance>::test_multi_element_collections(version),
                ]
            } else {
                Vec::new()
            },
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries(version: i16) -> Self {
        Self {
            throttle_time_ms: if version >= 1 { i32::MIN } else { 0_i32 },
            error_code: if version <= 3 { i16::MIN } else { 0_i16 },
            error_message: (version >= 1 && version <= 3)
                .then(|| Some(KafkaString::from("boundary".to_owned())))
                .flatten(),
            node_id: if version <= 3 { i32::MIN } else { 0_i32 },
            host: if version <= 3 {
                KafkaString::from("boundary".to_owned())
            } else {
                KafkaString::default()
            },
            port: if version <= 3 { i32::MIN } else { 0_i32 },
            coordinators: if version >= 4 {
                vec![<Coordinator as TestInstance>::test_numeric_boundaries(
                    version,
                )]
            } else {
                Vec::new()
            },
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields(version: i16) -> Self {
        Self {
            throttle_time_ms: if version >= 1 { 12345_i32 } else { 0_i32 },
            error_code: if version <= 3 { 42_i16 } else { 0_i16 },
            error_message: (version >= 1 && version <= 3)
                .then(|| Some(KafkaString::from("test".to_owned())))
                .flatten(),
            node_id: if version <= 3 { 12345_i32 } else { 0_i32 },
            host: if version <= 3 {
                KafkaString::from("test".to_owned())
            } else {
                KafkaString::default()
            },
            port: if version <= 3 { 12345_i32 } else { 0_i32 },
            coordinators: if version >= 4 {
                vec![<Coordinator as TestInstance>::test_tagged_fields(version)]
            } else {
                Vec::new()
            },
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for Coordinator {
    fn test_populated(_version: i16) -> Self {
        Self {
            key: KafkaString::from("test".to_owned()),
            node_id: 12345_i32,
            host: KafkaString::from("test".to_owned()),
            port: 12345_i32,
            error_code: 42_i16,
            error_message: Some(KafkaString::from("test".to_owned())),
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals(_version: i16) -> Self {
        drop(Self::default());
        Self {
            key: KafkaString::default(),
            node_id: 0_i32,
            host: KafkaString::default(),
            port: 0_i32,
            error_code: 0_i16,
            error_message: None,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections(_version: i16) -> Self {
        Self {
            key: KafkaString::default(),
            node_id: 0_i32,
            host: KafkaString::default(),
            port: 0_i32,
            error_code: 0_i16,
            error_message: Some(KafkaString::default()),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections(_version: i16) -> Self {
        Self {
            key: KafkaString::from("test-2".to_owned()),
            node_id: 23456_i32,
            host: KafkaString::from("test-2".to_owned()),
            port: 23456_i32,
            error_code: 43_i16,
            error_message: Some(KafkaString::from("test-2".to_owned())),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries(_version: i16) -> Self {
        Self {
            key: KafkaString::from("boundary".to_owned()),
            node_id: i32::MIN,
            host: KafkaString::from("boundary".to_owned()),
            port: i32::MIN,
            error_code: i16::MIN,
            error_message: Some(KafkaString::from("boundary".to_owned())),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields(_version: i16) -> Self {
        Self {
            key: KafkaString::from("test".to_owned()),
            node_id: 12345_i32,
            host: KafkaString::from("test".to_owned()),
            port: 12345_i32,
            error_code: 42_i16,
            error_message: Some(KafkaString::from("test".to_owned())),
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
fn encode_populated(version: i16) -> crate::MatrixResult<String> {
    let message = <FindCoordinatorResponseData as TestInstance>::test_populated(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_populated(version: i16) -> crate::MatrixResult<usize> {
    let message = <FindCoordinatorResponseData as TestInstance>::test_populated(version);
    Ok(message.encoded_len(version)?)
}
fn encode_null_optionals(version: i16) -> crate::MatrixResult<String> {
    let message = <FindCoordinatorResponseData as TestInstance>::test_null_optionals(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_null_optionals(version: i16) -> crate::MatrixResult<usize> {
    let message = <FindCoordinatorResponseData as TestInstance>::test_null_optionals(version);
    Ok(message.encoded_len(version)?)
}
fn encode_empty_collections(version: i16) -> crate::MatrixResult<String> {
    let message = <FindCoordinatorResponseData as TestInstance>::test_empty_collections(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_empty_collections(version: i16) -> crate::MatrixResult<usize> {
    let message = <FindCoordinatorResponseData as TestInstance>::test_empty_collections(version);
    Ok(message.encoded_len(version)?)
}
fn encode_multi_element_collections(version: i16) -> crate::MatrixResult<String> {
    let message =
        <FindCoordinatorResponseData as TestInstance>::test_multi_element_collections(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_multi_element_collections(version: i16) -> crate::MatrixResult<usize> {
    let message =
        <FindCoordinatorResponseData as TestInstance>::test_multi_element_collections(version);
    Ok(message.encoded_len(version)?)
}
fn encode_numeric_boundaries(version: i16) -> crate::MatrixResult<String> {
    let message = <FindCoordinatorResponseData as TestInstance>::test_numeric_boundaries(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_numeric_boundaries(version: i16) -> crate::MatrixResult<usize> {
    let message = <FindCoordinatorResponseData as TestInstance>::test_numeric_boundaries(version);
    Ok(message.encoded_len(version)?)
}
fn encode_tagged_fields(version: i16) -> crate::MatrixResult<String> {
    let message = <FindCoordinatorResponseData as TestInstance>::test_tagged_fields(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_tagged_fields(version: i16) -> crate::MatrixResult<usize> {
    let message = <FindCoordinatorResponseData as TestInstance>::test_tagged_fields(version);
    Ok(message.encoded_len(version)?)
}
fn reencode(version: i16, hex_input: &str) -> crate::MatrixResult<String> {
    let mut input = Bytes::from(crate::decode_hex(hex_input)?);
    let message = FindCoordinatorResponseData::read(&mut input, version)?;
    crate::ensure_input_consumed(&input)?;
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
const MATRIX_CASES: &[crate::MatrixCase] = &[
    crate::MatrixCase {
        schema_name: "FindCoordinatorResponse",
        java_class: "org.apache.kafka.common.message.FindCoordinatorResponseData",
        version: 0i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FindCoordinatorResponse",
        java_class: "org.apache.kafka.common.message.FindCoordinatorResponseData",
        version: 1i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FindCoordinatorResponse",
        java_class: "org.apache.kafka.common.message.FindCoordinatorResponseData",
        version: 2i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FindCoordinatorResponse",
        java_class: "org.apache.kafka.common.message.FindCoordinatorResponseData",
        version: 3i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FindCoordinatorResponse",
        java_class: "org.apache.kafka.common.message.FindCoordinatorResponseData",
        version: 4i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FindCoordinatorResponse",
        java_class: "org.apache.kafka.common.message.FindCoordinatorResponseData",
        version: 5i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FindCoordinatorResponse",
        java_class: "org.apache.kafka.common.message.FindCoordinatorResponseData",
        version: 6i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FindCoordinatorResponse",
        java_class: "org.apache.kafka.common.message.FindCoordinatorResponseData",
        version: 0i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FindCoordinatorResponse",
        java_class: "org.apache.kafka.common.message.FindCoordinatorResponseData",
        version: 1i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FindCoordinatorResponse",
        java_class: "org.apache.kafka.common.message.FindCoordinatorResponseData",
        version: 2i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FindCoordinatorResponse",
        java_class: "org.apache.kafka.common.message.FindCoordinatorResponseData",
        version: 3i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FindCoordinatorResponse",
        java_class: "org.apache.kafka.common.message.FindCoordinatorResponseData",
        version: 4i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FindCoordinatorResponse",
        java_class: "org.apache.kafka.common.message.FindCoordinatorResponseData",
        version: 5i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FindCoordinatorResponse",
        java_class: "org.apache.kafka.common.message.FindCoordinatorResponseData",
        version: 6i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FindCoordinatorResponse",
        java_class: "org.apache.kafka.common.message.FindCoordinatorResponseData",
        version: 0i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FindCoordinatorResponse",
        java_class: "org.apache.kafka.common.message.FindCoordinatorResponseData",
        version: 0i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FindCoordinatorResponse",
        java_class: "org.apache.kafka.common.message.FindCoordinatorResponseData",
        version: 0i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FindCoordinatorResponse",
        java_class: "org.apache.kafka.common.message.FindCoordinatorResponseData",
        version: 0i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FindCoordinatorResponse",
        java_class: "org.apache.kafka.common.message.FindCoordinatorResponseData",
        version: 1i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FindCoordinatorResponse",
        java_class: "org.apache.kafka.common.message.FindCoordinatorResponseData",
        version: 1i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FindCoordinatorResponse",
        java_class: "org.apache.kafka.common.message.FindCoordinatorResponseData",
        version: 1i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FindCoordinatorResponse",
        java_class: "org.apache.kafka.common.message.FindCoordinatorResponseData",
        version: 1i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FindCoordinatorResponse",
        java_class: "org.apache.kafka.common.message.FindCoordinatorResponseData",
        version: 2i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FindCoordinatorResponse",
        java_class: "org.apache.kafka.common.message.FindCoordinatorResponseData",
        version: 2i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FindCoordinatorResponse",
        java_class: "org.apache.kafka.common.message.FindCoordinatorResponseData",
        version: 2i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FindCoordinatorResponse",
        java_class: "org.apache.kafka.common.message.FindCoordinatorResponseData",
        version: 2i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FindCoordinatorResponse",
        java_class: "org.apache.kafka.common.message.FindCoordinatorResponseData",
        version: 3i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FindCoordinatorResponse",
        java_class: "org.apache.kafka.common.message.FindCoordinatorResponseData",
        version: 3i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FindCoordinatorResponse",
        java_class: "org.apache.kafka.common.message.FindCoordinatorResponseData",
        version: 3i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FindCoordinatorResponse",
        java_class: "org.apache.kafka.common.message.FindCoordinatorResponseData",
        version: 3i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FindCoordinatorResponse",
        java_class: "org.apache.kafka.common.message.FindCoordinatorResponseData",
        version: 4i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FindCoordinatorResponse",
        java_class: "org.apache.kafka.common.message.FindCoordinatorResponseData",
        version: 4i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FindCoordinatorResponse",
        java_class: "org.apache.kafka.common.message.FindCoordinatorResponseData",
        version: 4i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FindCoordinatorResponse",
        java_class: "org.apache.kafka.common.message.FindCoordinatorResponseData",
        version: 4i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FindCoordinatorResponse",
        java_class: "org.apache.kafka.common.message.FindCoordinatorResponseData",
        version: 5i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FindCoordinatorResponse",
        java_class: "org.apache.kafka.common.message.FindCoordinatorResponseData",
        version: 5i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FindCoordinatorResponse",
        java_class: "org.apache.kafka.common.message.FindCoordinatorResponseData",
        version: 5i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FindCoordinatorResponse",
        java_class: "org.apache.kafka.common.message.FindCoordinatorResponseData",
        version: 5i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FindCoordinatorResponse",
        java_class: "org.apache.kafka.common.message.FindCoordinatorResponseData",
        version: 6i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FindCoordinatorResponse",
        java_class: "org.apache.kafka.common.message.FindCoordinatorResponseData",
        version: 6i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FindCoordinatorResponse",
        java_class: "org.apache.kafka.common.message.FindCoordinatorResponseData",
        version: 6i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FindCoordinatorResponse",
        java_class: "org.apache.kafka.common.message.FindCoordinatorResponseData",
        version: 6i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
];
pub(crate) fn append_protocol_cases(cases: &mut Vec<crate::MatrixCase>) {
    cases.extend_from_slice(MATRIX_CASES);
}
