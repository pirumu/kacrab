use bytes::{Bytes, BytesMut};
use kacrab_protocol::{generated::describe_configs_request::*, *};

use crate::TestInstance;

impl TestInstance for DescribeConfigsRequestData {
    fn test_populated() -> Self {
        Self {
            resources: vec![<DescribeConfigsResource as TestInstance>::test_populated()],
            include_synonyms: true,
            include_documentation: true,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals() -> Self {
        Self {
            resources: vec![<DescribeConfigsResource as TestInstance>::test_null_optionals()],
            include_synonyms: false,
            include_documentation: false,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            resources: Vec::new(),
            include_synonyms: false,
            include_documentation: false,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            resources: vec![
                <DescribeConfigsResource as TestInstance>::test_populated(),
                <DescribeConfigsResource as TestInstance>::test_multi_element_collections(),
            ],
            include_synonyms: false,
            include_documentation: false,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            resources: vec![<DescribeConfigsResource as TestInstance>::test_numeric_boundaries()],
            include_synonyms: true,
            include_documentation: true,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            resources: vec![<DescribeConfigsResource as TestInstance>::test_tagged_fields()],
            include_synonyms: true,
            include_documentation: true,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for DescribeConfigsResource {
    fn test_populated() -> Self {
        Self {
            resource_type: 7_i8,
            resource_name: KafkaString::from("test".to_owned()),
            configuration_keys: Some(vec![KafkaString::from("test".to_owned())]),
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals() -> Self {
        drop(Self::default());
        Self {
            resource_type: 0_i8,
            resource_name: KafkaString::default(),
            configuration_keys: None,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            resource_type: 0_i8,
            resource_name: KafkaString::default(),
            configuration_keys: Some(Vec::new()),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            resource_type: 8_i8,
            resource_name: KafkaString::from("test-2".to_owned()),
            configuration_keys: Some(vec![
                KafkaString::from("test".to_owned()),
                KafkaString::from("test-2".to_owned()),
            ]),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            resource_type: i8::MIN,
            resource_name: KafkaString::from("boundary".to_owned()),
            configuration_keys: Some(vec![KafkaString::from("boundary".to_owned())]),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            resource_type: 7_i8,
            resource_name: KafkaString::from("test".to_owned()),
            configuration_keys: Some(vec![KafkaString::from("test".to_owned())]),
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
fn encode_populated(version: i16) -> crate::MatrixResult<String> {
    let message = <DescribeConfigsRequestData as TestInstance>::test_populated();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_null_optionals(version: i16) -> crate::MatrixResult<String> {
    let message = <DescribeConfigsRequestData as TestInstance>::test_null_optionals();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_empty_collections(version: i16) -> crate::MatrixResult<String> {
    let message = <DescribeConfigsRequestData as TestInstance>::test_empty_collections();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_multi_element_collections(version: i16) -> crate::MatrixResult<String> {
    let message = <DescribeConfigsRequestData as TestInstance>::test_multi_element_collections();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_numeric_boundaries(version: i16) -> crate::MatrixResult<String> {
    let message = <DescribeConfigsRequestData as TestInstance>::test_numeric_boundaries();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_tagged_fields(version: i16) -> crate::MatrixResult<String> {
    let message = <DescribeConfigsRequestData as TestInstance>::test_tagged_fields();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn reencode(version: i16, hex_input: &str) -> crate::MatrixResult<String> {
    let mut input = Bytes::from(crate::decode_hex(hex_input)?);
    let message = DescribeConfigsRequestData::read(&mut input, version)?;
    crate::ensure_input_consumed(&input)?;
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
const MATRIX_CASES: &[crate::MatrixCase] = &[
    crate::MatrixCase {
        schema_name: "DescribeConfigsRequest",
        java_class: "org.apache.kafka.common.message.DescribeConfigsRequestData",
        version: 1i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeConfigsRequest",
        java_class: "org.apache.kafka.common.message.DescribeConfigsRequestData",
        version: 2i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeConfigsRequest",
        java_class: "org.apache.kafka.common.message.DescribeConfigsRequestData",
        version: 3i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeConfigsRequest",
        java_class: "org.apache.kafka.common.message.DescribeConfigsRequestData",
        version: 4i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeConfigsRequest",
        java_class: "org.apache.kafka.common.message.DescribeConfigsRequestData",
        version: 1i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeConfigsRequest",
        java_class: "org.apache.kafka.common.message.DescribeConfigsRequestData",
        version: 2i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeConfigsRequest",
        java_class: "org.apache.kafka.common.message.DescribeConfigsRequestData",
        version: 3i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeConfigsRequest",
        java_class: "org.apache.kafka.common.message.DescribeConfigsRequestData",
        version: 4i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeConfigsRequest",
        java_class: "org.apache.kafka.common.message.DescribeConfigsRequestData",
        version: 1i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeConfigsRequest",
        java_class: "org.apache.kafka.common.message.DescribeConfigsRequestData",
        version: 1i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeConfigsRequest",
        java_class: "org.apache.kafka.common.message.DescribeConfigsRequestData",
        version: 1i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeConfigsRequest",
        java_class: "org.apache.kafka.common.message.DescribeConfigsRequestData",
        version: 1i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeConfigsRequest",
        java_class: "org.apache.kafka.common.message.DescribeConfigsRequestData",
        version: 2i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeConfigsRequest",
        java_class: "org.apache.kafka.common.message.DescribeConfigsRequestData",
        version: 2i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeConfigsRequest",
        java_class: "org.apache.kafka.common.message.DescribeConfigsRequestData",
        version: 2i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeConfigsRequest",
        java_class: "org.apache.kafka.common.message.DescribeConfigsRequestData",
        version: 2i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeConfigsRequest",
        java_class: "org.apache.kafka.common.message.DescribeConfigsRequestData",
        version: 3i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeConfigsRequest",
        java_class: "org.apache.kafka.common.message.DescribeConfigsRequestData",
        version: 3i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeConfigsRequest",
        java_class: "org.apache.kafka.common.message.DescribeConfigsRequestData",
        version: 3i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeConfigsRequest",
        java_class: "org.apache.kafka.common.message.DescribeConfigsRequestData",
        version: 3i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeConfigsRequest",
        java_class: "org.apache.kafka.common.message.DescribeConfigsRequestData",
        version: 4i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeConfigsRequest",
        java_class: "org.apache.kafka.common.message.DescribeConfigsRequestData",
        version: 4i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeConfigsRequest",
        java_class: "org.apache.kafka.common.message.DescribeConfigsRequestData",
        version: 4i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeConfigsRequest",
        java_class: "org.apache.kafka.common.message.DescribeConfigsRequestData",
        version: 4i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
];
pub(crate) fn append_protocol_cases(cases: &mut Vec<crate::MatrixCase>) {
    cases.extend_from_slice(MATRIX_CASES);
}
