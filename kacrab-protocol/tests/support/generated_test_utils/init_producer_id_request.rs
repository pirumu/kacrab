use bytes::{Bytes, BytesMut};
use kacrab_protocol::{generated::init_producer_id_request::*, *};

use crate::TestInstance;

impl TestInstance for InitProducerIdRequestData {
    fn test_populated() -> Self {
        Self {
            transactional_id: Some(KafkaString::from("test".to_owned())),
            transaction_timeout_ms: 12345_i32,
            producer_id: 9_876_543_210_i64,
            producer_epoch: 42_i16,
            enable2_pc: true,
            keep_prepared_txn: true,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals() -> Self {
        Self {
            transactional_id: None,
            transaction_timeout_ms: 0_i32,
            producer_id: 0_i64,
            producer_epoch: 0_i16,
            enable2_pc: false,
            keep_prepared_txn: false,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            transactional_id: Some(KafkaString::default()),
            transaction_timeout_ms: 0_i32,
            producer_id: 0_i64,
            producer_epoch: 0_i16,
            enable2_pc: false,
            keep_prepared_txn: false,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            transactional_id: Some(KafkaString::from("test-2".to_owned())),
            transaction_timeout_ms: 23456_i32,
            producer_id: 9_876_543_211_i64,
            producer_epoch: 43_i16,
            enable2_pc: false,
            keep_prepared_txn: false,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            transactional_id: Some(KafkaString::from("boundary".to_owned())),
            transaction_timeout_ms: i32::MIN,
            producer_id: i64::MIN,
            producer_epoch: i16::MIN,
            enable2_pc: true,
            keep_prepared_txn: true,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            transactional_id: Some(KafkaString::from("test".to_owned())),
            transaction_timeout_ms: 12345_i32,
            producer_id: 9_876_543_210_i64,
            producer_epoch: 42_i16,
            enable2_pc: true,
            keep_prepared_txn: true,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
fn encode_populated(version: i16) -> crate::MatrixResult<String> {
    let message = <InitProducerIdRequestData as TestInstance>::test_populated();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_null_optionals(version: i16) -> crate::MatrixResult<String> {
    let message = <InitProducerIdRequestData as TestInstance>::test_null_optionals();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_empty_collections(version: i16) -> crate::MatrixResult<String> {
    let message = <InitProducerIdRequestData as TestInstance>::test_empty_collections();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_multi_element_collections(version: i16) -> crate::MatrixResult<String> {
    let message = <InitProducerIdRequestData as TestInstance>::test_multi_element_collections();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_numeric_boundaries(version: i16) -> crate::MatrixResult<String> {
    let message = <InitProducerIdRequestData as TestInstance>::test_numeric_boundaries();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_tagged_fields(version: i16) -> crate::MatrixResult<String> {
    let message = <InitProducerIdRequestData as TestInstance>::test_tagged_fields();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn reencode(version: i16, hex_input: &str) -> crate::MatrixResult<String> {
    let mut input = Bytes::from(crate::decode_hex(hex_input)?);
    let message = InitProducerIdRequestData::read(&mut input, version)?;
    crate::ensure_input_consumed(&input)?;
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
const MATRIX_CASES: &[crate::MatrixCase] = &[
    crate::MatrixCase {
        schema_name: "InitProducerIdRequest",
        java_class: "org.apache.kafka.common.message.InitProducerIdRequestData",
        version: 0i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "InitProducerIdRequest",
        java_class: "org.apache.kafka.common.message.InitProducerIdRequestData",
        version: 1i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "InitProducerIdRequest",
        java_class: "org.apache.kafka.common.message.InitProducerIdRequestData",
        version: 2i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "InitProducerIdRequest",
        java_class: "org.apache.kafka.common.message.InitProducerIdRequestData",
        version: 3i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "InitProducerIdRequest",
        java_class: "org.apache.kafka.common.message.InitProducerIdRequestData",
        version: 4i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "InitProducerIdRequest",
        java_class: "org.apache.kafka.common.message.InitProducerIdRequestData",
        version: 5i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "InitProducerIdRequest",
        java_class: "org.apache.kafka.common.message.InitProducerIdRequestData",
        version: 6i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "InitProducerIdRequest",
        java_class: "org.apache.kafka.common.message.InitProducerIdRequestData",
        version: 0i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "InitProducerIdRequest",
        java_class: "org.apache.kafka.common.message.InitProducerIdRequestData",
        version: 1i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "InitProducerIdRequest",
        java_class: "org.apache.kafka.common.message.InitProducerIdRequestData",
        version: 2i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "InitProducerIdRequest",
        java_class: "org.apache.kafka.common.message.InitProducerIdRequestData",
        version: 3i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "InitProducerIdRequest",
        java_class: "org.apache.kafka.common.message.InitProducerIdRequestData",
        version: 4i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "InitProducerIdRequest",
        java_class: "org.apache.kafka.common.message.InitProducerIdRequestData",
        version: 5i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "InitProducerIdRequest",
        java_class: "org.apache.kafka.common.message.InitProducerIdRequestData",
        version: 6i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "InitProducerIdRequest",
        java_class: "org.apache.kafka.common.message.InitProducerIdRequestData",
        version: 0i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "InitProducerIdRequest",
        java_class: "org.apache.kafka.common.message.InitProducerIdRequestData",
        version: 0i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "InitProducerIdRequest",
        java_class: "org.apache.kafka.common.message.InitProducerIdRequestData",
        version: 0i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "InitProducerIdRequest",
        java_class: "org.apache.kafka.common.message.InitProducerIdRequestData",
        version: 0i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "InitProducerIdRequest",
        java_class: "org.apache.kafka.common.message.InitProducerIdRequestData",
        version: 1i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "InitProducerIdRequest",
        java_class: "org.apache.kafka.common.message.InitProducerIdRequestData",
        version: 1i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "InitProducerIdRequest",
        java_class: "org.apache.kafka.common.message.InitProducerIdRequestData",
        version: 1i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "InitProducerIdRequest",
        java_class: "org.apache.kafka.common.message.InitProducerIdRequestData",
        version: 1i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "InitProducerIdRequest",
        java_class: "org.apache.kafka.common.message.InitProducerIdRequestData",
        version: 2i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "InitProducerIdRequest",
        java_class: "org.apache.kafka.common.message.InitProducerIdRequestData",
        version: 2i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "InitProducerIdRequest",
        java_class: "org.apache.kafka.common.message.InitProducerIdRequestData",
        version: 2i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "InitProducerIdRequest",
        java_class: "org.apache.kafka.common.message.InitProducerIdRequestData",
        version: 2i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "InitProducerIdRequest",
        java_class: "org.apache.kafka.common.message.InitProducerIdRequestData",
        version: 3i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "InitProducerIdRequest",
        java_class: "org.apache.kafka.common.message.InitProducerIdRequestData",
        version: 3i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "InitProducerIdRequest",
        java_class: "org.apache.kafka.common.message.InitProducerIdRequestData",
        version: 3i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "InitProducerIdRequest",
        java_class: "org.apache.kafka.common.message.InitProducerIdRequestData",
        version: 3i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "InitProducerIdRequest",
        java_class: "org.apache.kafka.common.message.InitProducerIdRequestData",
        version: 4i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "InitProducerIdRequest",
        java_class: "org.apache.kafka.common.message.InitProducerIdRequestData",
        version: 4i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "InitProducerIdRequest",
        java_class: "org.apache.kafka.common.message.InitProducerIdRequestData",
        version: 4i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "InitProducerIdRequest",
        java_class: "org.apache.kafka.common.message.InitProducerIdRequestData",
        version: 4i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "InitProducerIdRequest",
        java_class: "org.apache.kafka.common.message.InitProducerIdRequestData",
        version: 5i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "InitProducerIdRequest",
        java_class: "org.apache.kafka.common.message.InitProducerIdRequestData",
        version: 5i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "InitProducerIdRequest",
        java_class: "org.apache.kafka.common.message.InitProducerIdRequestData",
        version: 5i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "InitProducerIdRequest",
        java_class: "org.apache.kafka.common.message.InitProducerIdRequestData",
        version: 5i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "InitProducerIdRequest",
        java_class: "org.apache.kafka.common.message.InitProducerIdRequestData",
        version: 6i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "InitProducerIdRequest",
        java_class: "org.apache.kafka.common.message.InitProducerIdRequestData",
        version: 6i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "InitProducerIdRequest",
        java_class: "org.apache.kafka.common.message.InitProducerIdRequestData",
        version: 6i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "InitProducerIdRequest",
        java_class: "org.apache.kafka.common.message.InitProducerIdRequestData",
        version: 6i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
];
pub(crate) fn append_protocol_cases(cases: &mut Vec<crate::MatrixCase>) {
    cases.extend_from_slice(MATRIX_CASES);
}
