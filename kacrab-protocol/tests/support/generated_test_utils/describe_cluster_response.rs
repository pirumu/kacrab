use bytes::{Bytes, BytesMut};
use kacrab_protocol::{generated::describe_cluster_response::*, *};

use crate::TestInstance;

impl TestInstance for DescribeClusterResponseData {
    fn test_populated() -> Self {
        Self {
            throttle_time_ms: 12345_i32,
            error_code: 42_i16,
            error_message: Some(KafkaString::from("test".to_owned())),
            endpoint_type: 7_i8,
            cluster_id: KafkaString::from("test".to_owned()),
            controller_id: 12345_i32,
            brokers: vec![<DescribeClusterBroker as TestInstance>::test_populated()],
            cluster_authorized_operations: 12345_i32,
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
            error_message: None,
            endpoint_type: 0_i8,
            cluster_id: KafkaString::default(),
            controller_id: 0_i32,
            brokers: vec![<DescribeClusterBroker as TestInstance>::test_null_optionals()],
            cluster_authorized_operations: 0_i32,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            throttle_time_ms: 0_i32,
            error_code: 0_i16,
            error_message: Some(KafkaString::default()),
            endpoint_type: 0_i8,
            cluster_id: KafkaString::default(),
            controller_id: 0_i32,
            brokers: Vec::new(),
            cluster_authorized_operations: 0_i32,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            throttle_time_ms: 23456_i32,
            error_code: 43_i16,
            error_message: Some(KafkaString::from("test-2".to_owned())),
            endpoint_type: 8_i8,
            cluster_id: KafkaString::from("test-2".to_owned()),
            controller_id: 23456_i32,
            brokers: vec![
                <DescribeClusterBroker as TestInstance>::test_populated(),
                <DescribeClusterBroker as TestInstance>::test_multi_element_collections(),
            ],
            cluster_authorized_operations: 23456_i32,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            throttle_time_ms: i32::MIN,
            error_code: i16::MIN,
            error_message: Some(KafkaString::from("boundary".to_owned())),
            endpoint_type: i8::MIN,
            cluster_id: KafkaString::from("boundary".to_owned()),
            controller_id: i32::MIN,
            brokers: vec![<DescribeClusterBroker as TestInstance>::test_numeric_boundaries()],
            cluster_authorized_operations: i32::MIN,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            throttle_time_ms: 12345_i32,
            error_code: 42_i16,
            error_message: Some(KafkaString::from("test".to_owned())),
            endpoint_type: 7_i8,
            cluster_id: KafkaString::from("test".to_owned()),
            controller_id: 12345_i32,
            brokers: vec![<DescribeClusterBroker as TestInstance>::test_tagged_fields()],
            cluster_authorized_operations: 12345_i32,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for DescribeClusterBroker {
    fn test_populated() -> Self {
        Self {
            broker_id: 12345_i32,
            host: KafkaString::from("test".to_owned()),
            port: 12345_i32,
            rack: Some(KafkaString::from("test".to_owned())),
            is_fenced: true,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals() -> Self {
        drop(Self::default());
        Self {
            broker_id: 0_i32,
            host: KafkaString::default(),
            port: 0_i32,
            rack: None,
            is_fenced: false,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            broker_id: 0_i32,
            host: KafkaString::default(),
            port: 0_i32,
            rack: Some(KafkaString::default()),
            is_fenced: false,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            broker_id: 23456_i32,
            host: KafkaString::from("test-2".to_owned()),
            port: 23456_i32,
            rack: Some(KafkaString::from("test-2".to_owned())),
            is_fenced: false,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            broker_id: i32::MIN,
            host: KafkaString::from("boundary".to_owned()),
            port: i32::MIN,
            rack: Some(KafkaString::from("boundary".to_owned())),
            is_fenced: true,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            broker_id: 12345_i32,
            host: KafkaString::from("test".to_owned()),
            port: 12345_i32,
            rack: Some(KafkaString::from("test".to_owned())),
            is_fenced: true,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
fn encode_populated(version: i16) -> crate::MatrixResult<String> {
    let message = <DescribeClusterResponseData as TestInstance>::test_populated();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_populated(version: i16) -> crate::MatrixResult<usize> {
    let message = <DescribeClusterResponseData as TestInstance>::test_populated();
    Ok(message.encoded_len(version)?)
}
fn encode_null_optionals(version: i16) -> crate::MatrixResult<String> {
    let message = <DescribeClusterResponseData as TestInstance>::test_null_optionals();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_null_optionals(version: i16) -> crate::MatrixResult<usize> {
    let message = <DescribeClusterResponseData as TestInstance>::test_null_optionals();
    Ok(message.encoded_len(version)?)
}
fn encode_empty_collections(version: i16) -> crate::MatrixResult<String> {
    let message = <DescribeClusterResponseData as TestInstance>::test_empty_collections();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_empty_collections(version: i16) -> crate::MatrixResult<usize> {
    let message = <DescribeClusterResponseData as TestInstance>::test_empty_collections();
    Ok(message.encoded_len(version)?)
}
fn encode_multi_element_collections(version: i16) -> crate::MatrixResult<String> {
    let message = <DescribeClusterResponseData as TestInstance>::test_multi_element_collections();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_multi_element_collections(version: i16) -> crate::MatrixResult<usize> {
    let message = <DescribeClusterResponseData as TestInstance>::test_multi_element_collections();
    Ok(message.encoded_len(version)?)
}
fn encode_numeric_boundaries(version: i16) -> crate::MatrixResult<String> {
    let message = <DescribeClusterResponseData as TestInstance>::test_numeric_boundaries();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_numeric_boundaries(version: i16) -> crate::MatrixResult<usize> {
    let message = <DescribeClusterResponseData as TestInstance>::test_numeric_boundaries();
    Ok(message.encoded_len(version)?)
}
fn encode_tagged_fields(version: i16) -> crate::MatrixResult<String> {
    let message = <DescribeClusterResponseData as TestInstance>::test_tagged_fields();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_tagged_fields(version: i16) -> crate::MatrixResult<usize> {
    let message = <DescribeClusterResponseData as TestInstance>::test_tagged_fields();
    Ok(message.encoded_len(version)?)
}
fn reencode(version: i16, hex_input: &str) -> crate::MatrixResult<String> {
    let mut input = Bytes::from(crate::decode_hex(hex_input)?);
    let message = DescribeClusterResponseData::read(&mut input, version)?;
    crate::ensure_input_consumed(&input)?;
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
const MATRIX_CASES: &[crate::MatrixCase] = &[
    crate::MatrixCase {
        schema_name: "DescribeClusterResponse",
        java_class: "org.apache.kafka.common.message.DescribeClusterResponseData",
        version: 0i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeClusterResponse",
        java_class: "org.apache.kafka.common.message.DescribeClusterResponseData",
        version: 1i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeClusterResponse",
        java_class: "org.apache.kafka.common.message.DescribeClusterResponseData",
        version: 2i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeClusterResponse",
        java_class: "org.apache.kafka.common.message.DescribeClusterResponseData",
        version: 0i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeClusterResponse",
        java_class: "org.apache.kafka.common.message.DescribeClusterResponseData",
        version: 1i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeClusterResponse",
        java_class: "org.apache.kafka.common.message.DescribeClusterResponseData",
        version: 2i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeClusterResponse",
        java_class: "org.apache.kafka.common.message.DescribeClusterResponseData",
        version: 0i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeClusterResponse",
        java_class: "org.apache.kafka.common.message.DescribeClusterResponseData",
        version: 0i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeClusterResponse",
        java_class: "org.apache.kafka.common.message.DescribeClusterResponseData",
        version: 0i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeClusterResponse",
        java_class: "org.apache.kafka.common.message.DescribeClusterResponseData",
        version: 0i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeClusterResponse",
        java_class: "org.apache.kafka.common.message.DescribeClusterResponseData",
        version: 1i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeClusterResponse",
        java_class: "org.apache.kafka.common.message.DescribeClusterResponseData",
        version: 1i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeClusterResponse",
        java_class: "org.apache.kafka.common.message.DescribeClusterResponseData",
        version: 1i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeClusterResponse",
        java_class: "org.apache.kafka.common.message.DescribeClusterResponseData",
        version: 1i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeClusterResponse",
        java_class: "org.apache.kafka.common.message.DescribeClusterResponseData",
        version: 2i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeClusterResponse",
        java_class: "org.apache.kafka.common.message.DescribeClusterResponseData",
        version: 2i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeClusterResponse",
        java_class: "org.apache.kafka.common.message.DescribeClusterResponseData",
        version: 2i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeClusterResponse",
        java_class: "org.apache.kafka.common.message.DescribeClusterResponseData",
        version: 2i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
];
pub(crate) fn append_protocol_cases(cases: &mut Vec<crate::MatrixCase>) {
    cases.extend_from_slice(MATRIX_CASES);
}
