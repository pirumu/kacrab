use bytes::{Bytes, BytesMut};
use kacrab_protocol::{generated::broker_heartbeat_request::*, *};

use crate::TestInstance;

impl TestInstance for BrokerHeartbeatRequestData {
    fn test_populated() -> Self {
        Self {
            broker_id: 12345_i32,
            broker_epoch: 9_876_543_210_i64,
            current_metadata_offset: 9_876_543_210_i64,
            want_fence: true,
            want_shut_down: true,
            offline_log_dirs: vec![KafkaUuid::ONE],
            cordoned_log_dirs: Some(vec![KafkaUuid::ONE]),
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals() -> Self {
        Self {
            broker_id: 0_i32,
            broker_epoch: 0_i64,
            current_metadata_offset: 0_i64,
            want_fence: false,
            want_shut_down: false,
            offline_log_dirs: Vec::new(),
            cordoned_log_dirs: None,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            broker_id: 0_i32,
            broker_epoch: 0_i64,
            current_metadata_offset: 0_i64,
            want_fence: false,
            want_shut_down: false,
            offline_log_dirs: Vec::new(),
            cordoned_log_dirs: Some(Vec::new()),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            broker_id: 23456_i32,
            broker_epoch: 9_876_543_211_i64,
            current_metadata_offset: 9_876_543_211_i64,
            want_fence: false,
            want_shut_down: false,
            offline_log_dirs: vec![KafkaUuid::ONE, KafkaUuid::from_parts(2, 3)],
            cordoned_log_dirs: Some(vec![KafkaUuid::ONE, KafkaUuid::from_parts(2, 3)]),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            broker_id: i32::MIN,
            broker_epoch: i64::MIN,
            current_metadata_offset: i64::MIN,
            want_fence: true,
            want_shut_down: true,
            offline_log_dirs: vec![KafkaUuid::ONE],
            cordoned_log_dirs: Some(vec![KafkaUuid::ONE]),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            broker_id: 12345_i32,
            broker_epoch: 9_876_543_210_i64,
            current_metadata_offset: 9_876_543_210_i64,
            want_fence: true,
            want_shut_down: true,
            offline_log_dirs: vec![KafkaUuid::ONE],
            cordoned_log_dirs: Some(vec![KafkaUuid::ONE]),
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
fn encode_populated(version: i16) -> crate::MatrixResult<String> {
    let message = <BrokerHeartbeatRequestData as TestInstance>::test_populated();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_populated(version: i16) -> crate::MatrixResult<usize> {
    let message = <BrokerHeartbeatRequestData as TestInstance>::test_populated();
    Ok(message.encoded_len(version)?)
}
fn encode_null_optionals(version: i16) -> crate::MatrixResult<String> {
    let message = <BrokerHeartbeatRequestData as TestInstance>::test_null_optionals();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_null_optionals(version: i16) -> crate::MatrixResult<usize> {
    let message = <BrokerHeartbeatRequestData as TestInstance>::test_null_optionals();
    Ok(message.encoded_len(version)?)
}
fn encode_empty_collections(version: i16) -> crate::MatrixResult<String> {
    let message = <BrokerHeartbeatRequestData as TestInstance>::test_empty_collections();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_empty_collections(version: i16) -> crate::MatrixResult<usize> {
    let message = <BrokerHeartbeatRequestData as TestInstance>::test_empty_collections();
    Ok(message.encoded_len(version)?)
}
fn encode_multi_element_collections(version: i16) -> crate::MatrixResult<String> {
    let message = <BrokerHeartbeatRequestData as TestInstance>::test_multi_element_collections();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_multi_element_collections(version: i16) -> crate::MatrixResult<usize> {
    let message = <BrokerHeartbeatRequestData as TestInstance>::test_multi_element_collections();
    Ok(message.encoded_len(version)?)
}
fn encode_numeric_boundaries(version: i16) -> crate::MatrixResult<String> {
    let message = <BrokerHeartbeatRequestData as TestInstance>::test_numeric_boundaries();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_numeric_boundaries(version: i16) -> crate::MatrixResult<usize> {
    let message = <BrokerHeartbeatRequestData as TestInstance>::test_numeric_boundaries();
    Ok(message.encoded_len(version)?)
}
fn encode_tagged_fields(version: i16) -> crate::MatrixResult<String> {
    let message = <BrokerHeartbeatRequestData as TestInstance>::test_tagged_fields();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_tagged_fields(version: i16) -> crate::MatrixResult<usize> {
    let message = <BrokerHeartbeatRequestData as TestInstance>::test_tagged_fields();
    Ok(message.encoded_len(version)?)
}
fn reencode(version: i16, hex_input: &str) -> crate::MatrixResult<String> {
    let mut input = Bytes::from(crate::decode_hex(hex_input)?);
    let message = BrokerHeartbeatRequestData::read(&mut input, version)?;
    crate::ensure_input_consumed(&input)?;
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
const MATRIX_CASES: &[crate::MatrixCase] = &[
    crate::MatrixCase {
        schema_name: "BrokerHeartbeatRequest",
        java_class: "org.apache.kafka.common.message.BrokerHeartbeatRequestData",
        version: 0i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "BrokerHeartbeatRequest",
        java_class: "org.apache.kafka.common.message.BrokerHeartbeatRequestData",
        version: 1i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "BrokerHeartbeatRequest",
        java_class: "org.apache.kafka.common.message.BrokerHeartbeatRequestData",
        version: 2i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "BrokerHeartbeatRequest",
        java_class: "org.apache.kafka.common.message.BrokerHeartbeatRequestData",
        version: 0i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "BrokerHeartbeatRequest",
        java_class: "org.apache.kafka.common.message.BrokerHeartbeatRequestData",
        version: 1i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "BrokerHeartbeatRequest",
        java_class: "org.apache.kafka.common.message.BrokerHeartbeatRequestData",
        version: 2i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "BrokerHeartbeatRequest",
        java_class: "org.apache.kafka.common.message.BrokerHeartbeatRequestData",
        version: 0i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "BrokerHeartbeatRequest",
        java_class: "org.apache.kafka.common.message.BrokerHeartbeatRequestData",
        version: 0i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "BrokerHeartbeatRequest",
        java_class: "org.apache.kafka.common.message.BrokerHeartbeatRequestData",
        version: 0i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "BrokerHeartbeatRequest",
        java_class: "org.apache.kafka.common.message.BrokerHeartbeatRequestData",
        version: 0i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "BrokerHeartbeatRequest",
        java_class: "org.apache.kafka.common.message.BrokerHeartbeatRequestData",
        version: 1i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "BrokerHeartbeatRequest",
        java_class: "org.apache.kafka.common.message.BrokerHeartbeatRequestData",
        version: 1i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "BrokerHeartbeatRequest",
        java_class: "org.apache.kafka.common.message.BrokerHeartbeatRequestData",
        version: 1i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "BrokerHeartbeatRequest",
        java_class: "org.apache.kafka.common.message.BrokerHeartbeatRequestData",
        version: 1i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "BrokerHeartbeatRequest",
        java_class: "org.apache.kafka.common.message.BrokerHeartbeatRequestData",
        version: 2i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "BrokerHeartbeatRequest",
        java_class: "org.apache.kafka.common.message.BrokerHeartbeatRequestData",
        version: 2i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "BrokerHeartbeatRequest",
        java_class: "org.apache.kafka.common.message.BrokerHeartbeatRequestData",
        version: 2i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "BrokerHeartbeatRequest",
        java_class: "org.apache.kafka.common.message.BrokerHeartbeatRequestData",
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
