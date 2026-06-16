use bytes::{Bytes, BytesMut};
use kacrab_protocol::{generated::add_offsets_to_txn_request::*, *};

use crate::TestInstance;

impl TestInstance for AddOffsetsToTxnRequestData {
    fn test_populated() -> Self {
        Self {
            transactional_id: KafkaString::from("test".to_owned()),
            producer_id: 9_876_543_210_i64,
            producer_epoch: 42_i16,
            group_id: KafkaString::from("test".to_owned()),
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals() -> Self {
        Self {
            transactional_id: KafkaString::default(),
            producer_id: 0_i64,
            producer_epoch: 0_i16,
            group_id: KafkaString::default(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            transactional_id: KafkaString::default(),
            producer_id: 0_i64,
            producer_epoch: 0_i16,
            group_id: KafkaString::default(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            transactional_id: KafkaString::from("test-2".to_owned()),
            producer_id: 9_876_543_211_i64,
            producer_epoch: 43_i16,
            group_id: KafkaString::from("test-2".to_owned()),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            transactional_id: KafkaString::from("boundary".to_owned()),
            producer_id: i64::MIN,
            producer_epoch: i16::MIN,
            group_id: KafkaString::from("boundary".to_owned()),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            transactional_id: KafkaString::from("test".to_owned()),
            producer_id: 9_876_543_210_i64,
            producer_epoch: 42_i16,
            group_id: KafkaString::from("test".to_owned()),
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
fn encode_populated(version: i16) -> crate::MatrixResult<String> {
    let message = <AddOffsetsToTxnRequestData as TestInstance>::test_populated();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_null_optionals(version: i16) -> crate::MatrixResult<String> {
    let message = <AddOffsetsToTxnRequestData as TestInstance>::test_null_optionals();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_empty_collections(version: i16) -> crate::MatrixResult<String> {
    let message = <AddOffsetsToTxnRequestData as TestInstance>::test_empty_collections();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_multi_element_collections(version: i16) -> crate::MatrixResult<String> {
    let message = <AddOffsetsToTxnRequestData as TestInstance>::test_multi_element_collections();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_numeric_boundaries(version: i16) -> crate::MatrixResult<String> {
    let message = <AddOffsetsToTxnRequestData as TestInstance>::test_numeric_boundaries();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_tagged_fields(version: i16) -> crate::MatrixResult<String> {
    let message = <AddOffsetsToTxnRequestData as TestInstance>::test_tagged_fields();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn reencode(version: i16, hex_input: &str) -> crate::MatrixResult<String> {
    let mut input = Bytes::from(crate::decode_hex(hex_input)?);
    let message = AddOffsetsToTxnRequestData::read(&mut input, version)?;
    crate::ensure_input_consumed(&input)?;
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
const MATRIX_CASES: &[crate::MatrixCase] = &[
    crate::MatrixCase {
        schema_name: "AddOffsetsToTxnRequest",
        java_class: "org.apache.kafka.common.message.AddOffsetsToTxnRequestData",
        version: 0i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AddOffsetsToTxnRequest",
        java_class: "org.apache.kafka.common.message.AddOffsetsToTxnRequestData",
        version: 1i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AddOffsetsToTxnRequest",
        java_class: "org.apache.kafka.common.message.AddOffsetsToTxnRequestData",
        version: 2i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AddOffsetsToTxnRequest",
        java_class: "org.apache.kafka.common.message.AddOffsetsToTxnRequestData",
        version: 3i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AddOffsetsToTxnRequest",
        java_class: "org.apache.kafka.common.message.AddOffsetsToTxnRequestData",
        version: 4i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AddOffsetsToTxnRequest",
        java_class: "org.apache.kafka.common.message.AddOffsetsToTxnRequestData",
        version: 0i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AddOffsetsToTxnRequest",
        java_class: "org.apache.kafka.common.message.AddOffsetsToTxnRequestData",
        version: 1i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AddOffsetsToTxnRequest",
        java_class: "org.apache.kafka.common.message.AddOffsetsToTxnRequestData",
        version: 2i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AddOffsetsToTxnRequest",
        java_class: "org.apache.kafka.common.message.AddOffsetsToTxnRequestData",
        version: 3i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AddOffsetsToTxnRequest",
        java_class: "org.apache.kafka.common.message.AddOffsetsToTxnRequestData",
        version: 4i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AddOffsetsToTxnRequest",
        java_class: "org.apache.kafka.common.message.AddOffsetsToTxnRequestData",
        version: 0i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AddOffsetsToTxnRequest",
        java_class: "org.apache.kafka.common.message.AddOffsetsToTxnRequestData",
        version: 0i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AddOffsetsToTxnRequest",
        java_class: "org.apache.kafka.common.message.AddOffsetsToTxnRequestData",
        version: 0i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AddOffsetsToTxnRequest",
        java_class: "org.apache.kafka.common.message.AddOffsetsToTxnRequestData",
        version: 0i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AddOffsetsToTxnRequest",
        java_class: "org.apache.kafka.common.message.AddOffsetsToTxnRequestData",
        version: 1i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AddOffsetsToTxnRequest",
        java_class: "org.apache.kafka.common.message.AddOffsetsToTxnRequestData",
        version: 1i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AddOffsetsToTxnRequest",
        java_class: "org.apache.kafka.common.message.AddOffsetsToTxnRequestData",
        version: 1i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AddOffsetsToTxnRequest",
        java_class: "org.apache.kafka.common.message.AddOffsetsToTxnRequestData",
        version: 1i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AddOffsetsToTxnRequest",
        java_class: "org.apache.kafka.common.message.AddOffsetsToTxnRequestData",
        version: 2i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AddOffsetsToTxnRequest",
        java_class: "org.apache.kafka.common.message.AddOffsetsToTxnRequestData",
        version: 2i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AddOffsetsToTxnRequest",
        java_class: "org.apache.kafka.common.message.AddOffsetsToTxnRequestData",
        version: 2i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AddOffsetsToTxnRequest",
        java_class: "org.apache.kafka.common.message.AddOffsetsToTxnRequestData",
        version: 2i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AddOffsetsToTxnRequest",
        java_class: "org.apache.kafka.common.message.AddOffsetsToTxnRequestData",
        version: 3i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AddOffsetsToTxnRequest",
        java_class: "org.apache.kafka.common.message.AddOffsetsToTxnRequestData",
        version: 3i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AddOffsetsToTxnRequest",
        java_class: "org.apache.kafka.common.message.AddOffsetsToTxnRequestData",
        version: 3i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AddOffsetsToTxnRequest",
        java_class: "org.apache.kafka.common.message.AddOffsetsToTxnRequestData",
        version: 3i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AddOffsetsToTxnRequest",
        java_class: "org.apache.kafka.common.message.AddOffsetsToTxnRequestData",
        version: 4i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AddOffsetsToTxnRequest",
        java_class: "org.apache.kafka.common.message.AddOffsetsToTxnRequestData",
        version: 4i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AddOffsetsToTxnRequest",
        java_class: "org.apache.kafka.common.message.AddOffsetsToTxnRequestData",
        version: 4i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AddOffsetsToTxnRequest",
        java_class: "org.apache.kafka.common.message.AddOffsetsToTxnRequestData",
        version: 4i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
];
pub(crate) fn append_protocol_cases(cases: &mut Vec<crate::MatrixCase>) {
    cases.extend_from_slice(MATRIX_CASES);
}
