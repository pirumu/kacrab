use bytes::{Bytes, BytesMut};
use kacrab_protocol::{generated::delete_topics_request::*, *};

use crate::TestInstance;

impl TestInstance for DeleteTopicsRequestData {
    fn test_populated() -> Self {
        Self {
            topics: vec![<DeleteTopicState as TestInstance>::test_populated()],
            topic_names: vec![KafkaString::from("test".to_owned())],
            timeout_ms: 12345_i32,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals() -> Self {
        Self {
            topics: vec![<DeleteTopicState as TestInstance>::test_null_optionals()],
            topic_names: vec![KafkaString::default()],
            timeout_ms: 0_i32,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            topics: Vec::new(),
            topic_names: Vec::new(),
            timeout_ms: 0_i32,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            topics: vec![
                <DeleteTopicState as TestInstance>::test_populated(),
                <DeleteTopicState as TestInstance>::test_multi_element_collections(),
            ],
            topic_names: vec![
                KafkaString::from("test".to_owned()),
                KafkaString::from("test-2".to_owned()),
            ],
            timeout_ms: 23456_i32,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            topics: vec![<DeleteTopicState as TestInstance>::test_numeric_boundaries()],
            topic_names: vec![KafkaString::from("boundary".to_owned())],
            timeout_ms: i32::MIN,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            topics: vec![<DeleteTopicState as TestInstance>::test_tagged_fields()],
            topic_names: vec![KafkaString::from("test".to_owned())],
            timeout_ms: 12345_i32,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for DeleteTopicState {
    fn test_populated() -> Self {
        Self {
            name: Some(KafkaString::from("test".to_owned())),
            topic_id: KafkaUuid::ONE,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals() -> Self {
        drop(Self::default());
        Self {
            name: None,
            topic_id: KafkaUuid::ZERO,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            name: Some(KafkaString::default()),
            topic_id: KafkaUuid::ZERO,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            name: Some(KafkaString::from("test-2".to_owned())),
            topic_id: KafkaUuid::from_parts(2, 3),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            name: Some(KafkaString::from("boundary".to_owned())),
            topic_id: KafkaUuid::ONE,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            name: Some(KafkaString::from("test".to_owned())),
            topic_id: KafkaUuid::ONE,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
fn encode_populated(version: i16) -> crate::MatrixResult<String> {
    let message = <DeleteTopicsRequestData as TestInstance>::test_populated();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_null_optionals(version: i16) -> crate::MatrixResult<String> {
    let message = <DeleteTopicsRequestData as TestInstance>::test_null_optionals();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_empty_collections(version: i16) -> crate::MatrixResult<String> {
    let message = <DeleteTopicsRequestData as TestInstance>::test_empty_collections();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_multi_element_collections(version: i16) -> crate::MatrixResult<String> {
    let message = <DeleteTopicsRequestData as TestInstance>::test_multi_element_collections();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_numeric_boundaries(version: i16) -> crate::MatrixResult<String> {
    let message = <DeleteTopicsRequestData as TestInstance>::test_numeric_boundaries();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_tagged_fields(version: i16) -> crate::MatrixResult<String> {
    let message = <DeleteTopicsRequestData as TestInstance>::test_tagged_fields();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn reencode(version: i16, hex_input: &str) -> crate::MatrixResult<String> {
    let mut input = Bytes::from(crate::decode_hex(hex_input)?);
    let message = DeleteTopicsRequestData::read(&mut input, version)?;
    crate::ensure_input_consumed(&input)?;
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
const MATRIX_CASES: &[crate::MatrixCase] = &[
    crate::MatrixCase {
        schema_name: "DeleteTopicsRequest",
        java_class: "org.apache.kafka.common.message.DeleteTopicsRequestData",
        version: 1i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DeleteTopicsRequest",
        java_class: "org.apache.kafka.common.message.DeleteTopicsRequestData",
        version: 2i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DeleteTopicsRequest",
        java_class: "org.apache.kafka.common.message.DeleteTopicsRequestData",
        version: 3i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DeleteTopicsRequest",
        java_class: "org.apache.kafka.common.message.DeleteTopicsRequestData",
        version: 4i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DeleteTopicsRequest",
        java_class: "org.apache.kafka.common.message.DeleteTopicsRequestData",
        version: 5i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DeleteTopicsRequest",
        java_class: "org.apache.kafka.common.message.DeleteTopicsRequestData",
        version: 6i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DeleteTopicsRequest",
        java_class: "org.apache.kafka.common.message.DeleteTopicsRequestData",
        version: 1i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DeleteTopicsRequest",
        java_class: "org.apache.kafka.common.message.DeleteTopicsRequestData",
        version: 2i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DeleteTopicsRequest",
        java_class: "org.apache.kafka.common.message.DeleteTopicsRequestData",
        version: 3i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DeleteTopicsRequest",
        java_class: "org.apache.kafka.common.message.DeleteTopicsRequestData",
        version: 4i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DeleteTopicsRequest",
        java_class: "org.apache.kafka.common.message.DeleteTopicsRequestData",
        version: 5i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DeleteTopicsRequest",
        java_class: "org.apache.kafka.common.message.DeleteTopicsRequestData",
        version: 6i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DeleteTopicsRequest",
        java_class: "org.apache.kafka.common.message.DeleteTopicsRequestData",
        version: 1i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DeleteTopicsRequest",
        java_class: "org.apache.kafka.common.message.DeleteTopicsRequestData",
        version: 1i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DeleteTopicsRequest",
        java_class: "org.apache.kafka.common.message.DeleteTopicsRequestData",
        version: 1i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DeleteTopicsRequest",
        java_class: "org.apache.kafka.common.message.DeleteTopicsRequestData",
        version: 1i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DeleteTopicsRequest",
        java_class: "org.apache.kafka.common.message.DeleteTopicsRequestData",
        version: 2i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DeleteTopicsRequest",
        java_class: "org.apache.kafka.common.message.DeleteTopicsRequestData",
        version: 2i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DeleteTopicsRequest",
        java_class: "org.apache.kafka.common.message.DeleteTopicsRequestData",
        version: 2i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DeleteTopicsRequest",
        java_class: "org.apache.kafka.common.message.DeleteTopicsRequestData",
        version: 2i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DeleteTopicsRequest",
        java_class: "org.apache.kafka.common.message.DeleteTopicsRequestData",
        version: 3i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DeleteTopicsRequest",
        java_class: "org.apache.kafka.common.message.DeleteTopicsRequestData",
        version: 3i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DeleteTopicsRequest",
        java_class: "org.apache.kafka.common.message.DeleteTopicsRequestData",
        version: 3i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DeleteTopicsRequest",
        java_class: "org.apache.kafka.common.message.DeleteTopicsRequestData",
        version: 3i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DeleteTopicsRequest",
        java_class: "org.apache.kafka.common.message.DeleteTopicsRequestData",
        version: 4i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DeleteTopicsRequest",
        java_class: "org.apache.kafka.common.message.DeleteTopicsRequestData",
        version: 4i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DeleteTopicsRequest",
        java_class: "org.apache.kafka.common.message.DeleteTopicsRequestData",
        version: 4i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DeleteTopicsRequest",
        java_class: "org.apache.kafka.common.message.DeleteTopicsRequestData",
        version: 4i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DeleteTopicsRequest",
        java_class: "org.apache.kafka.common.message.DeleteTopicsRequestData",
        version: 5i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DeleteTopicsRequest",
        java_class: "org.apache.kafka.common.message.DeleteTopicsRequestData",
        version: 5i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DeleteTopicsRequest",
        java_class: "org.apache.kafka.common.message.DeleteTopicsRequestData",
        version: 5i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DeleteTopicsRequest",
        java_class: "org.apache.kafka.common.message.DeleteTopicsRequestData",
        version: 5i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DeleteTopicsRequest",
        java_class: "org.apache.kafka.common.message.DeleteTopicsRequestData",
        version: 6i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DeleteTopicsRequest",
        java_class: "org.apache.kafka.common.message.DeleteTopicsRequestData",
        version: 6i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DeleteTopicsRequest",
        java_class: "org.apache.kafka.common.message.DeleteTopicsRequestData",
        version: 6i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DeleteTopicsRequest",
        java_class: "org.apache.kafka.common.message.DeleteTopicsRequestData",
        version: 6i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
];
pub(crate) fn append_protocol_cases(cases: &mut Vec<crate::MatrixCase>) {
    cases.extend_from_slice(MATRIX_CASES);
}
