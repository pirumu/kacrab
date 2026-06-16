use bytes::{Bytes, BytesMut};
use kacrab_protocol::{generated::join_group_response::*, *};

use crate::TestInstance;

impl TestInstance for JoinGroupResponseData {
    fn test_populated() -> Self {
        Self {
            throttle_time_ms: 12345_i32,
            error_code: 42_i16,
            generation_id: 12345_i32,
            protocol_type: Some(KafkaString::from("test".to_owned())),
            protocol_name: Some(KafkaString::from("test".to_owned())),
            leader: KafkaString::from("test".to_owned()),
            skip_assignment: true,
            member_id: KafkaString::from("test".to_owned()),
            members: vec![<JoinGroupResponseMember as TestInstance>::test_populated()],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals() -> Self {
        Self {
            throttle_time_ms: 0_i32,
            error_code: 0_i16,
            generation_id: 0_i32,
            protocol_type: None,
            protocol_name: None,
            leader: KafkaString::default(),
            skip_assignment: false,
            member_id: KafkaString::default(),
            members: vec![<JoinGroupResponseMember as TestInstance>::test_null_optionals()],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            throttle_time_ms: 0_i32,
            error_code: 0_i16,
            generation_id: 0_i32,
            protocol_type: Some(KafkaString::default()),
            protocol_name: Some(KafkaString::default()),
            leader: KafkaString::default(),
            skip_assignment: false,
            member_id: KafkaString::default(),
            members: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            throttle_time_ms: 23456_i32,
            error_code: 43_i16,
            generation_id: 23456_i32,
            protocol_type: Some(KafkaString::from("test-2".to_owned())),
            protocol_name: Some(KafkaString::from("test-2".to_owned())),
            leader: KafkaString::from("test-2".to_owned()),
            skip_assignment: false,
            member_id: KafkaString::from("test-2".to_owned()),
            members: vec![
                <JoinGroupResponseMember as TestInstance>::test_populated(),
                <JoinGroupResponseMember as TestInstance>::test_multi_element_collections(),
            ],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            throttle_time_ms: i32::MIN,
            error_code: i16::MIN,
            generation_id: i32::MIN,
            protocol_type: Some(KafkaString::from("boundary".to_owned())),
            protocol_name: Some(KafkaString::from("boundary".to_owned())),
            leader: KafkaString::from("boundary".to_owned()),
            skip_assignment: true,
            member_id: KafkaString::from("boundary".to_owned()),
            members: vec![<JoinGroupResponseMember as TestInstance>::test_numeric_boundaries()],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            throttle_time_ms: 12345_i32,
            error_code: 42_i16,
            generation_id: 12345_i32,
            protocol_type: Some(KafkaString::from("test".to_owned())),
            protocol_name: Some(KafkaString::from("test".to_owned())),
            leader: KafkaString::from("test".to_owned()),
            skip_assignment: true,
            member_id: KafkaString::from("test".to_owned()),
            members: vec![<JoinGroupResponseMember as TestInstance>::test_tagged_fields()],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for JoinGroupResponseMember {
    fn test_populated() -> Self {
        Self {
            member_id: KafkaString::from("test".to_owned()),
            group_instance_id: Some(KafkaString::from("test".to_owned())),
            metadata: Bytes::from_static(b"\xca\xfe"),
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
            group_instance_id: None,
            metadata: Bytes::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            member_id: KafkaString::default(),
            group_instance_id: Some(KafkaString::default()),
            metadata: Bytes::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            member_id: KafkaString::from("test-2".to_owned()),
            group_instance_id: Some(KafkaString::from("test-2".to_owned())),
            metadata: Bytes::from_static(b"\x00\xff"),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            member_id: KafkaString::from("boundary".to_owned()),
            group_instance_id: Some(KafkaString::from("boundary".to_owned())),
            metadata: Bytes::from_static(b"\x00\xff"),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            member_id: KafkaString::from("test".to_owned()),
            group_instance_id: Some(KafkaString::from("test".to_owned())),
            metadata: Bytes::from_static(b"\xca\xfe"),
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
fn encode_populated(version: i16) -> crate::MatrixResult<String> {
    let message = <JoinGroupResponseData as TestInstance>::test_populated();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_populated(version: i16) -> crate::MatrixResult<usize> {
    let message = <JoinGroupResponseData as TestInstance>::test_populated();
    Ok(message.encoded_len(version)?)
}
fn encode_null_optionals(version: i16) -> crate::MatrixResult<String> {
    let message = <JoinGroupResponseData as TestInstance>::test_null_optionals();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_null_optionals(version: i16) -> crate::MatrixResult<usize> {
    let message = <JoinGroupResponseData as TestInstance>::test_null_optionals();
    Ok(message.encoded_len(version)?)
}
fn encode_empty_collections(version: i16) -> crate::MatrixResult<String> {
    let message = <JoinGroupResponseData as TestInstance>::test_empty_collections();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_empty_collections(version: i16) -> crate::MatrixResult<usize> {
    let message = <JoinGroupResponseData as TestInstance>::test_empty_collections();
    Ok(message.encoded_len(version)?)
}
fn encode_multi_element_collections(version: i16) -> crate::MatrixResult<String> {
    let message = <JoinGroupResponseData as TestInstance>::test_multi_element_collections();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_multi_element_collections(version: i16) -> crate::MatrixResult<usize> {
    let message = <JoinGroupResponseData as TestInstance>::test_multi_element_collections();
    Ok(message.encoded_len(version)?)
}
fn encode_numeric_boundaries(version: i16) -> crate::MatrixResult<String> {
    let message = <JoinGroupResponseData as TestInstance>::test_numeric_boundaries();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_numeric_boundaries(version: i16) -> crate::MatrixResult<usize> {
    let message = <JoinGroupResponseData as TestInstance>::test_numeric_boundaries();
    Ok(message.encoded_len(version)?)
}
fn encode_tagged_fields(version: i16) -> crate::MatrixResult<String> {
    let message = <JoinGroupResponseData as TestInstance>::test_tagged_fields();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_tagged_fields(version: i16) -> crate::MatrixResult<usize> {
    let message = <JoinGroupResponseData as TestInstance>::test_tagged_fields();
    Ok(message.encoded_len(version)?)
}
fn reencode(version: i16, hex_input: &str) -> crate::MatrixResult<String> {
    let mut input = Bytes::from(crate::decode_hex(hex_input)?);
    let message = JoinGroupResponseData::read(&mut input, version)?;
    crate::ensure_input_consumed(&input)?;
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
const MATRIX_CASES: &[crate::MatrixCase] = &[
    crate::MatrixCase {
        schema_name: "JoinGroupResponse",
        java_class: "org.apache.kafka.common.message.JoinGroupResponseData",
        version: 0i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "JoinGroupResponse",
        java_class: "org.apache.kafka.common.message.JoinGroupResponseData",
        version: 1i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "JoinGroupResponse",
        java_class: "org.apache.kafka.common.message.JoinGroupResponseData",
        version: 2i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "JoinGroupResponse",
        java_class: "org.apache.kafka.common.message.JoinGroupResponseData",
        version: 3i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "JoinGroupResponse",
        java_class: "org.apache.kafka.common.message.JoinGroupResponseData",
        version: 4i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "JoinGroupResponse",
        java_class: "org.apache.kafka.common.message.JoinGroupResponseData",
        version: 5i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "JoinGroupResponse",
        java_class: "org.apache.kafka.common.message.JoinGroupResponseData",
        version: 6i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "JoinGroupResponse",
        java_class: "org.apache.kafka.common.message.JoinGroupResponseData",
        version: 7i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "JoinGroupResponse",
        java_class: "org.apache.kafka.common.message.JoinGroupResponseData",
        version: 8i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "JoinGroupResponse",
        java_class: "org.apache.kafka.common.message.JoinGroupResponseData",
        version: 9i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "JoinGroupResponse",
        java_class: "org.apache.kafka.common.message.JoinGroupResponseData",
        version: 0i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "JoinGroupResponse",
        java_class: "org.apache.kafka.common.message.JoinGroupResponseData",
        version: 1i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "JoinGroupResponse",
        java_class: "org.apache.kafka.common.message.JoinGroupResponseData",
        version: 2i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "JoinGroupResponse",
        java_class: "org.apache.kafka.common.message.JoinGroupResponseData",
        version: 3i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "JoinGroupResponse",
        java_class: "org.apache.kafka.common.message.JoinGroupResponseData",
        version: 4i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "JoinGroupResponse",
        java_class: "org.apache.kafka.common.message.JoinGroupResponseData",
        version: 5i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "JoinGroupResponse",
        java_class: "org.apache.kafka.common.message.JoinGroupResponseData",
        version: 6i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "JoinGroupResponse",
        java_class: "org.apache.kafka.common.message.JoinGroupResponseData",
        version: 7i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "JoinGroupResponse",
        java_class: "org.apache.kafka.common.message.JoinGroupResponseData",
        version: 8i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "JoinGroupResponse",
        java_class: "org.apache.kafka.common.message.JoinGroupResponseData",
        version: 9i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "JoinGroupResponse",
        java_class: "org.apache.kafka.common.message.JoinGroupResponseData",
        version: 0i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "JoinGroupResponse",
        java_class: "org.apache.kafka.common.message.JoinGroupResponseData",
        version: 0i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "JoinGroupResponse",
        java_class: "org.apache.kafka.common.message.JoinGroupResponseData",
        version: 0i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "JoinGroupResponse",
        java_class: "org.apache.kafka.common.message.JoinGroupResponseData",
        version: 0i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "JoinGroupResponse",
        java_class: "org.apache.kafka.common.message.JoinGroupResponseData",
        version: 1i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "JoinGroupResponse",
        java_class: "org.apache.kafka.common.message.JoinGroupResponseData",
        version: 1i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "JoinGroupResponse",
        java_class: "org.apache.kafka.common.message.JoinGroupResponseData",
        version: 1i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "JoinGroupResponse",
        java_class: "org.apache.kafka.common.message.JoinGroupResponseData",
        version: 1i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "JoinGroupResponse",
        java_class: "org.apache.kafka.common.message.JoinGroupResponseData",
        version: 2i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "JoinGroupResponse",
        java_class: "org.apache.kafka.common.message.JoinGroupResponseData",
        version: 2i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "JoinGroupResponse",
        java_class: "org.apache.kafka.common.message.JoinGroupResponseData",
        version: 2i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "JoinGroupResponse",
        java_class: "org.apache.kafka.common.message.JoinGroupResponseData",
        version: 2i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "JoinGroupResponse",
        java_class: "org.apache.kafka.common.message.JoinGroupResponseData",
        version: 3i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "JoinGroupResponse",
        java_class: "org.apache.kafka.common.message.JoinGroupResponseData",
        version: 3i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "JoinGroupResponse",
        java_class: "org.apache.kafka.common.message.JoinGroupResponseData",
        version: 3i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "JoinGroupResponse",
        java_class: "org.apache.kafka.common.message.JoinGroupResponseData",
        version: 3i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "JoinGroupResponse",
        java_class: "org.apache.kafka.common.message.JoinGroupResponseData",
        version: 4i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "JoinGroupResponse",
        java_class: "org.apache.kafka.common.message.JoinGroupResponseData",
        version: 4i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "JoinGroupResponse",
        java_class: "org.apache.kafka.common.message.JoinGroupResponseData",
        version: 4i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "JoinGroupResponse",
        java_class: "org.apache.kafka.common.message.JoinGroupResponseData",
        version: 4i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "JoinGroupResponse",
        java_class: "org.apache.kafka.common.message.JoinGroupResponseData",
        version: 5i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "JoinGroupResponse",
        java_class: "org.apache.kafka.common.message.JoinGroupResponseData",
        version: 5i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "JoinGroupResponse",
        java_class: "org.apache.kafka.common.message.JoinGroupResponseData",
        version: 5i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "JoinGroupResponse",
        java_class: "org.apache.kafka.common.message.JoinGroupResponseData",
        version: 5i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "JoinGroupResponse",
        java_class: "org.apache.kafka.common.message.JoinGroupResponseData",
        version: 6i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "JoinGroupResponse",
        java_class: "org.apache.kafka.common.message.JoinGroupResponseData",
        version: 6i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "JoinGroupResponse",
        java_class: "org.apache.kafka.common.message.JoinGroupResponseData",
        version: 6i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "JoinGroupResponse",
        java_class: "org.apache.kafka.common.message.JoinGroupResponseData",
        version: 6i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "JoinGroupResponse",
        java_class: "org.apache.kafka.common.message.JoinGroupResponseData",
        version: 7i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "JoinGroupResponse",
        java_class: "org.apache.kafka.common.message.JoinGroupResponseData",
        version: 7i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "JoinGroupResponse",
        java_class: "org.apache.kafka.common.message.JoinGroupResponseData",
        version: 7i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "JoinGroupResponse",
        java_class: "org.apache.kafka.common.message.JoinGroupResponseData",
        version: 7i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "JoinGroupResponse",
        java_class: "org.apache.kafka.common.message.JoinGroupResponseData",
        version: 8i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "JoinGroupResponse",
        java_class: "org.apache.kafka.common.message.JoinGroupResponseData",
        version: 8i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "JoinGroupResponse",
        java_class: "org.apache.kafka.common.message.JoinGroupResponseData",
        version: 8i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "JoinGroupResponse",
        java_class: "org.apache.kafka.common.message.JoinGroupResponseData",
        version: 8i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "JoinGroupResponse",
        java_class: "org.apache.kafka.common.message.JoinGroupResponseData",
        version: 9i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "JoinGroupResponse",
        java_class: "org.apache.kafka.common.message.JoinGroupResponseData",
        version: 9i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "JoinGroupResponse",
        java_class: "org.apache.kafka.common.message.JoinGroupResponseData",
        version: 9i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "JoinGroupResponse",
        java_class: "org.apache.kafka.common.message.JoinGroupResponseData",
        version: 9i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
];
pub(crate) fn append_protocol_cases(cases: &mut Vec<crate::MatrixCase>) {
    cases.extend_from_slice(MATRIX_CASES);
}
