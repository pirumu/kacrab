use bytes::{Bytes, BytesMut};
use kacrab_protocol::{generated::create_topics_response::*, *};

use crate::TestInstance;

impl TestInstance for CreateTopicsResponseData {
    fn test_populated() -> Self {
        Self {
            throttle_time_ms: 12345_i32,
            topics: vec![<CreatableTopicResult as TestInstance>::test_populated()],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals() -> Self {
        Self {
            throttle_time_ms: 0_i32,
            topics: vec![<CreatableTopicResult as TestInstance>::test_null_optionals()],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            throttle_time_ms: 0_i32,
            topics: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            throttle_time_ms: 23456_i32,
            topics: vec![
                <CreatableTopicResult as TestInstance>::test_populated(),
                <CreatableTopicResult as TestInstance>::test_multi_element_collections(),
            ],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            throttle_time_ms: i32::MIN,
            topics: vec![<CreatableTopicResult as TestInstance>::test_numeric_boundaries()],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            throttle_time_ms: 12345_i32,
            topics: vec![<CreatableTopicResult as TestInstance>::test_tagged_fields()],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for CreatableTopicResult {
    fn test_populated() -> Self {
        Self {
            name: KafkaString::from("test".to_owned()),
            topic_id: KafkaUuid::ONE,
            error_code: 42_i16,
            error_message: Some(KafkaString::from("test".to_owned())),
            topic_config_error_code: 42_i16,
            num_partitions: 12345_i32,
            replication_factor: 42_i16,
            configs: Some(vec![
                <CreatableTopicConfigs as TestInstance>::test_populated(),
            ]),
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals() -> Self {
        drop(Self::default());
        drop(<CreatableTopicConfigs as TestInstance>::test_null_optionals());
        Self {
            name: KafkaString::default(),
            topic_id: KafkaUuid::ZERO,
            error_code: 0_i16,
            error_message: None,
            topic_config_error_code: 0_i16,
            num_partitions: 0_i32,
            replication_factor: 0_i16,
            configs: None,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            name: KafkaString::default(),
            topic_id: KafkaUuid::ZERO,
            error_code: 0_i16,
            error_message: Some(KafkaString::default()),
            topic_config_error_code: 0_i16,
            num_partitions: 0_i32,
            replication_factor: 0_i16,
            configs: Some(Vec::new()),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            name: KafkaString::from("test-2".to_owned()),
            topic_id: KafkaUuid::from_parts(2, 3),
            error_code: 43_i16,
            error_message: Some(KafkaString::from("test-2".to_owned())),
            topic_config_error_code: 43_i16,
            num_partitions: 23456_i32,
            replication_factor: 43_i16,
            configs: Some(vec![
                <CreatableTopicConfigs as TestInstance>::test_populated(),
                <CreatableTopicConfigs as TestInstance>::test_multi_element_collections(),
            ]),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            name: KafkaString::from("boundary".to_owned()),
            topic_id: KafkaUuid::ONE,
            error_code: i16::MIN,
            error_message: Some(KafkaString::from("boundary".to_owned())),
            topic_config_error_code: i16::MIN,
            num_partitions: i32::MIN,
            replication_factor: i16::MIN,
            configs: Some(vec![
                <CreatableTopicConfigs as TestInstance>::test_numeric_boundaries(),
            ]),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            name: KafkaString::from("test".to_owned()),
            topic_id: KafkaUuid::ONE,
            error_code: 42_i16,
            error_message: Some(KafkaString::from("test".to_owned())),
            topic_config_error_code: 42_i16,
            num_partitions: 12345_i32,
            replication_factor: 42_i16,
            configs: Some(vec![
                <CreatableTopicConfigs as TestInstance>::test_tagged_fields(),
            ]),
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for CreatableTopicConfigs {
    fn test_populated() -> Self {
        Self {
            name: KafkaString::from("test".to_owned()),
            value: Some(KafkaString::from("test".to_owned())),
            read_only: true,
            config_source: 7_i8,
            is_sensitive: true,
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
            value: None,
            read_only: false,
            config_source: 0_i8,
            is_sensitive: false,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            name: KafkaString::default(),
            value: Some(KafkaString::default()),
            read_only: false,
            config_source: 0_i8,
            is_sensitive: false,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            name: KafkaString::from("test-2".to_owned()),
            value: Some(KafkaString::from("test-2".to_owned())),
            read_only: false,
            config_source: 8_i8,
            is_sensitive: false,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            name: KafkaString::from("boundary".to_owned()),
            value: Some(KafkaString::from("boundary".to_owned())),
            read_only: true,
            config_source: i8::MIN,
            is_sensitive: true,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            name: KafkaString::from("test".to_owned()),
            value: Some(KafkaString::from("test".to_owned())),
            read_only: true,
            config_source: 7_i8,
            is_sensitive: true,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
fn encode_populated(version: i16) -> crate::MatrixResult<String> {
    let message = <CreateTopicsResponseData as TestInstance>::test_populated();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_null_optionals(version: i16) -> crate::MatrixResult<String> {
    let message = <CreateTopicsResponseData as TestInstance>::test_null_optionals();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_empty_collections(version: i16) -> crate::MatrixResult<String> {
    let message = <CreateTopicsResponseData as TestInstance>::test_empty_collections();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_multi_element_collections(version: i16) -> crate::MatrixResult<String> {
    let message = <CreateTopicsResponseData as TestInstance>::test_multi_element_collections();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_numeric_boundaries(version: i16) -> crate::MatrixResult<String> {
    let message = <CreateTopicsResponseData as TestInstance>::test_numeric_boundaries();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_tagged_fields(version: i16) -> crate::MatrixResult<String> {
    let message = <CreateTopicsResponseData as TestInstance>::test_tagged_fields();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn reencode(version: i16, hex_input: &str) -> crate::MatrixResult<String> {
    let mut input = Bytes::from(crate::decode_hex(hex_input)?);
    let message = CreateTopicsResponseData::read(&mut input, version)?;
    crate::ensure_input_consumed(&input)?;
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
const MATRIX_CASES: &[crate::MatrixCase] = &[
    crate::MatrixCase {
        schema_name: "CreateTopicsResponse",
        java_class: "org.apache.kafka.common.message.CreateTopicsResponseData",
        version: 2i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "CreateTopicsResponse",
        java_class: "org.apache.kafka.common.message.CreateTopicsResponseData",
        version: 3i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "CreateTopicsResponse",
        java_class: "org.apache.kafka.common.message.CreateTopicsResponseData",
        version: 4i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "CreateTopicsResponse",
        java_class: "org.apache.kafka.common.message.CreateTopicsResponseData",
        version: 5i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "CreateTopicsResponse",
        java_class: "org.apache.kafka.common.message.CreateTopicsResponseData",
        version: 6i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "CreateTopicsResponse",
        java_class: "org.apache.kafka.common.message.CreateTopicsResponseData",
        version: 7i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "CreateTopicsResponse",
        java_class: "org.apache.kafka.common.message.CreateTopicsResponseData",
        version: 2i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "CreateTopicsResponse",
        java_class: "org.apache.kafka.common.message.CreateTopicsResponseData",
        version: 3i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "CreateTopicsResponse",
        java_class: "org.apache.kafka.common.message.CreateTopicsResponseData",
        version: 4i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "CreateTopicsResponse",
        java_class: "org.apache.kafka.common.message.CreateTopicsResponseData",
        version: 5i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "CreateTopicsResponse",
        java_class: "org.apache.kafka.common.message.CreateTopicsResponseData",
        version: 6i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "CreateTopicsResponse",
        java_class: "org.apache.kafka.common.message.CreateTopicsResponseData",
        version: 7i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "CreateTopicsResponse",
        java_class: "org.apache.kafka.common.message.CreateTopicsResponseData",
        version: 2i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "CreateTopicsResponse",
        java_class: "org.apache.kafka.common.message.CreateTopicsResponseData",
        version: 2i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "CreateTopicsResponse",
        java_class: "org.apache.kafka.common.message.CreateTopicsResponseData",
        version: 2i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "CreateTopicsResponse",
        java_class: "org.apache.kafka.common.message.CreateTopicsResponseData",
        version: 2i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "CreateTopicsResponse",
        java_class: "org.apache.kafka.common.message.CreateTopicsResponseData",
        version: 3i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "CreateTopicsResponse",
        java_class: "org.apache.kafka.common.message.CreateTopicsResponseData",
        version: 3i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "CreateTopicsResponse",
        java_class: "org.apache.kafka.common.message.CreateTopicsResponseData",
        version: 3i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "CreateTopicsResponse",
        java_class: "org.apache.kafka.common.message.CreateTopicsResponseData",
        version: 3i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "CreateTopicsResponse",
        java_class: "org.apache.kafka.common.message.CreateTopicsResponseData",
        version: 4i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "CreateTopicsResponse",
        java_class: "org.apache.kafka.common.message.CreateTopicsResponseData",
        version: 4i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "CreateTopicsResponse",
        java_class: "org.apache.kafka.common.message.CreateTopicsResponseData",
        version: 4i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "CreateTopicsResponse",
        java_class: "org.apache.kafka.common.message.CreateTopicsResponseData",
        version: 4i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "CreateTopicsResponse",
        java_class: "org.apache.kafka.common.message.CreateTopicsResponseData",
        version: 5i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "CreateTopicsResponse",
        java_class: "org.apache.kafka.common.message.CreateTopicsResponseData",
        version: 5i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "CreateTopicsResponse",
        java_class: "org.apache.kafka.common.message.CreateTopicsResponseData",
        version: 5i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "CreateTopicsResponse",
        java_class: "org.apache.kafka.common.message.CreateTopicsResponseData",
        version: 5i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "CreateTopicsResponse",
        java_class: "org.apache.kafka.common.message.CreateTopicsResponseData",
        version: 6i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "CreateTopicsResponse",
        java_class: "org.apache.kafka.common.message.CreateTopicsResponseData",
        version: 6i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "CreateTopicsResponse",
        java_class: "org.apache.kafka.common.message.CreateTopicsResponseData",
        version: 6i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "CreateTopicsResponse",
        java_class: "org.apache.kafka.common.message.CreateTopicsResponseData",
        version: 6i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "CreateTopicsResponse",
        java_class: "org.apache.kafka.common.message.CreateTopicsResponseData",
        version: 7i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "CreateTopicsResponse",
        java_class: "org.apache.kafka.common.message.CreateTopicsResponseData",
        version: 7i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "CreateTopicsResponse",
        java_class: "org.apache.kafka.common.message.CreateTopicsResponseData",
        version: 7i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "CreateTopicsResponse",
        java_class: "org.apache.kafka.common.message.CreateTopicsResponseData",
        version: 7i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
];
pub(crate) fn append_protocol_cases(cases: &mut Vec<crate::MatrixCase>) {
    cases.extend_from_slice(MATRIX_CASES);
}
