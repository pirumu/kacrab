#![allow(
    clippy::all,
    clippy::pedantic,
    clippy::nursery,
    reason = "Generated test fixtures mirror Kafka's schema shape and trade hand-written lint \
              style for reproducible output, matching the generated protocol modules."
)]
use bytes::{Bytes, BytesMut};
use kacrab_protocol::{generated::delete_acls_response::*, *};

use crate::TestInstance;

impl TestInstance for DeleteAclsResponseData {
    fn test_populated(version: i16) -> Self {
        Self {
            throttle_time_ms: 12345_i32,
            filter_results: vec![<DeleteAclsFilterResult as TestInstance>::test_populated(
                version,
            )],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals(version: i16) -> Self {
        Self {
            throttle_time_ms: 0_i32,
            filter_results: vec![
                <DeleteAclsFilterResult as TestInstance>::test_null_optionals(version),
            ],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections(_version: i16) -> Self {
        Self {
            throttle_time_ms: 0_i32,
            filter_results: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections(version: i16) -> Self {
        Self {
            throttle_time_ms: 23456_i32,
            filter_results: vec![
                <DeleteAclsFilterResult as TestInstance>::test_populated(version),
                <DeleteAclsFilterResult as TestInstance>::test_multi_element_collections(version),
            ],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries(version: i16) -> Self {
        Self {
            throttle_time_ms: i32::MIN,
            filter_results: vec![
                <DeleteAclsFilterResult as TestInstance>::test_numeric_boundaries(version),
            ],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields(version: i16) -> Self {
        Self {
            throttle_time_ms: 12345_i32,
            filter_results: vec![
                <DeleteAclsFilterResult as TestInstance>::test_tagged_fields(version),
            ],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for DeleteAclsFilterResult {
    fn test_populated(version: i16) -> Self {
        Self {
            error_code: 42_i16,
            error_message: Some(KafkaString::from("test".to_owned())),
            matching_acls: vec![<DeleteAclsMatchingAcl as TestInstance>::test_populated(
                version,
            )],
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
            error_message: None,
            matching_acls: vec![
                <DeleteAclsMatchingAcl as TestInstance>::test_null_optionals(version),
            ],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections(_version: i16) -> Self {
        Self {
            error_code: 0_i16,
            error_message: Some(KafkaString::default()),
            matching_acls: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections(version: i16) -> Self {
        Self {
            error_code: 43_i16,
            error_message: Some(KafkaString::from("test-2".to_owned())),
            matching_acls: vec![
                <DeleteAclsMatchingAcl as TestInstance>::test_populated(version),
                <DeleteAclsMatchingAcl as TestInstance>::test_multi_element_collections(version),
            ],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries(version: i16) -> Self {
        Self {
            error_code: i16::MIN,
            error_message: Some(KafkaString::from("boundary".to_owned())),
            matching_acls: vec![
                <DeleteAclsMatchingAcl as TestInstance>::test_numeric_boundaries(version),
            ],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields(version: i16) -> Self {
        Self {
            error_code: 42_i16,
            error_message: Some(KafkaString::from("test".to_owned())),
            matching_acls: vec![<DeleteAclsMatchingAcl as TestInstance>::test_tagged_fields(
                version,
            )],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for DeleteAclsMatchingAcl {
    fn test_populated(_version: i16) -> Self {
        Self {
            error_code: 42_i16,
            error_message: Some(KafkaString::from("test".to_owned())),
            resource_type: 7_i8,
            resource_name: KafkaString::from("test".to_owned()),
            pattern_type: 7_i8,
            principal: KafkaString::from("test".to_owned()),
            host: KafkaString::from("test".to_owned()),
            operation: 7_i8,
            permission_type: 7_i8,
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
            error_message: None,
            resource_type: 0_i8,
            resource_name: KafkaString::default(),
            pattern_type: 0_i8,
            principal: KafkaString::default(),
            host: KafkaString::default(),
            operation: 0_i8,
            permission_type: 0_i8,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections(_version: i16) -> Self {
        Self {
            error_code: 0_i16,
            error_message: Some(KafkaString::default()),
            resource_type: 0_i8,
            resource_name: KafkaString::default(),
            pattern_type: 0_i8,
            principal: KafkaString::default(),
            host: KafkaString::default(),
            operation: 0_i8,
            permission_type: 0_i8,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections(_version: i16) -> Self {
        Self {
            error_code: 43_i16,
            error_message: Some(KafkaString::from("test-2".to_owned())),
            resource_type: 8_i8,
            resource_name: KafkaString::from("test-2".to_owned()),
            pattern_type: 8_i8,
            principal: KafkaString::from("test-2".to_owned()),
            host: KafkaString::from("test-2".to_owned()),
            operation: 8_i8,
            permission_type: 8_i8,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries(_version: i16) -> Self {
        Self {
            error_code: i16::MIN,
            error_message: Some(KafkaString::from("boundary".to_owned())),
            resource_type: i8::MIN,
            resource_name: KafkaString::from("boundary".to_owned()),
            pattern_type: i8::MIN,
            principal: KafkaString::from("boundary".to_owned()),
            host: KafkaString::from("boundary".to_owned()),
            operation: i8::MIN,
            permission_type: i8::MIN,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields(_version: i16) -> Self {
        Self {
            error_code: 42_i16,
            error_message: Some(KafkaString::from("test".to_owned())),
            resource_type: 7_i8,
            resource_name: KafkaString::from("test".to_owned()),
            pattern_type: 7_i8,
            principal: KafkaString::from("test".to_owned()),
            host: KafkaString::from("test".to_owned()),
            operation: 7_i8,
            permission_type: 7_i8,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
fn encode_populated(version: i16) -> crate::MatrixResult<String> {
    let message = <DeleteAclsResponseData as TestInstance>::test_populated(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_populated(version: i16) -> crate::MatrixResult<usize> {
    let message = <DeleteAclsResponseData as TestInstance>::test_populated(version);
    Ok(message.encoded_len(version)?)
}
fn encode_null_optionals(version: i16) -> crate::MatrixResult<String> {
    let message = <DeleteAclsResponseData as TestInstance>::test_null_optionals(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_null_optionals(version: i16) -> crate::MatrixResult<usize> {
    let message = <DeleteAclsResponseData as TestInstance>::test_null_optionals(version);
    Ok(message.encoded_len(version)?)
}
fn encode_empty_collections(version: i16) -> crate::MatrixResult<String> {
    let message = <DeleteAclsResponseData as TestInstance>::test_empty_collections(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_empty_collections(version: i16) -> crate::MatrixResult<usize> {
    let message = <DeleteAclsResponseData as TestInstance>::test_empty_collections(version);
    Ok(message.encoded_len(version)?)
}
fn encode_multi_element_collections(version: i16) -> crate::MatrixResult<String> {
    let message = <DeleteAclsResponseData as TestInstance>::test_multi_element_collections(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_multi_element_collections(version: i16) -> crate::MatrixResult<usize> {
    let message = <DeleteAclsResponseData as TestInstance>::test_multi_element_collections(version);
    Ok(message.encoded_len(version)?)
}
fn encode_numeric_boundaries(version: i16) -> crate::MatrixResult<String> {
    let message = <DeleteAclsResponseData as TestInstance>::test_numeric_boundaries(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_numeric_boundaries(version: i16) -> crate::MatrixResult<usize> {
    let message = <DeleteAclsResponseData as TestInstance>::test_numeric_boundaries(version);
    Ok(message.encoded_len(version)?)
}
fn encode_tagged_fields(version: i16) -> crate::MatrixResult<String> {
    let message = <DeleteAclsResponseData as TestInstance>::test_tagged_fields(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_tagged_fields(version: i16) -> crate::MatrixResult<usize> {
    let message = <DeleteAclsResponseData as TestInstance>::test_tagged_fields(version);
    Ok(message.encoded_len(version)?)
}
fn reencode(version: i16, hex_input: &str) -> crate::MatrixResult<String> {
    let mut input = Bytes::from(crate::decode_hex(hex_input)?);
    let message = DeleteAclsResponseData::read(&mut input, version)?;
    crate::ensure_input_consumed(&input)?;
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
const MATRIX_CASES: &[crate::MatrixCase] = &[
    crate::MatrixCase {
        schema_name: "DeleteAclsResponse",
        java_class: "org.apache.kafka.common.message.DeleteAclsResponseData",
        version: 1i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DeleteAclsResponse",
        java_class: "org.apache.kafka.common.message.DeleteAclsResponseData",
        version: 2i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DeleteAclsResponse",
        java_class: "org.apache.kafka.common.message.DeleteAclsResponseData",
        version: 3i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DeleteAclsResponse",
        java_class: "org.apache.kafka.common.message.DeleteAclsResponseData",
        version: 1i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DeleteAclsResponse",
        java_class: "org.apache.kafka.common.message.DeleteAclsResponseData",
        version: 2i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DeleteAclsResponse",
        java_class: "org.apache.kafka.common.message.DeleteAclsResponseData",
        version: 3i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DeleteAclsResponse",
        java_class: "org.apache.kafka.common.message.DeleteAclsResponseData",
        version: 1i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DeleteAclsResponse",
        java_class: "org.apache.kafka.common.message.DeleteAclsResponseData",
        version: 1i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DeleteAclsResponse",
        java_class: "org.apache.kafka.common.message.DeleteAclsResponseData",
        version: 1i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DeleteAclsResponse",
        java_class: "org.apache.kafka.common.message.DeleteAclsResponseData",
        version: 1i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DeleteAclsResponse",
        java_class: "org.apache.kafka.common.message.DeleteAclsResponseData",
        version: 2i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DeleteAclsResponse",
        java_class: "org.apache.kafka.common.message.DeleteAclsResponseData",
        version: 2i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DeleteAclsResponse",
        java_class: "org.apache.kafka.common.message.DeleteAclsResponseData",
        version: 2i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DeleteAclsResponse",
        java_class: "org.apache.kafka.common.message.DeleteAclsResponseData",
        version: 2i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DeleteAclsResponse",
        java_class: "org.apache.kafka.common.message.DeleteAclsResponseData",
        version: 3i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DeleteAclsResponse",
        java_class: "org.apache.kafka.common.message.DeleteAclsResponseData",
        version: 3i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DeleteAclsResponse",
        java_class: "org.apache.kafka.common.message.DeleteAclsResponseData",
        version: 3i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DeleteAclsResponse",
        java_class: "org.apache.kafka.common.message.DeleteAclsResponseData",
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
