use bytes::{Bytes, BytesMut};
use kacrab_protocol::{generated::add_partitions_to_txn_request::*, *};

use crate::TestInstance;

impl TestInstance for AddPartitionsToTxnRequestData {
    fn test_populated() -> Self {
        Self {
            transactions: vec![<AddPartitionsToTxnTransaction as TestInstance>::test_populated()],
            v3_and_below_transactional_id: KafkaString::from("test".to_owned()),
            v3_and_below_producer_id: 9_876_543_210_i64,
            v3_and_below_producer_epoch: 42_i16,
            v3_and_below_topics: vec![<AddPartitionsToTxnTopic as TestInstance>::test_populated()],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals() -> Self {
        Self {
            transactions: vec![
                <AddPartitionsToTxnTransaction as TestInstance>::test_null_optionals(),
            ],
            v3_and_below_transactional_id: KafkaString::default(),
            v3_and_below_producer_id: 0_i64,
            v3_and_below_producer_epoch: 0_i16,
            v3_and_below_topics: vec![
                <AddPartitionsToTxnTopic as TestInstance>::test_null_optionals(),
            ],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            transactions: Vec::new(),
            v3_and_below_transactional_id: KafkaString::default(),
            v3_and_below_producer_id: 0_i64,
            v3_and_below_producer_epoch: 0_i16,
            v3_and_below_topics: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            transactions: vec![
                <AddPartitionsToTxnTransaction as TestInstance>::test_populated(),
                <AddPartitionsToTxnTransaction as TestInstance>::test_multi_element_collections(),
            ],
            v3_and_below_transactional_id: KafkaString::from("test-2".to_owned()),
            v3_and_below_producer_id: 9_876_543_211_i64,
            v3_and_below_producer_epoch: 43_i16,
            v3_and_below_topics: vec![
                <AddPartitionsToTxnTopic as TestInstance>::test_populated(),
                <AddPartitionsToTxnTopic as TestInstance>::test_multi_element_collections(),
            ],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            transactions: vec![
                <AddPartitionsToTxnTransaction as TestInstance>::test_numeric_boundaries(),
            ],
            v3_and_below_transactional_id: KafkaString::from("boundary".to_owned()),
            v3_and_below_producer_id: i64::MIN,
            v3_and_below_producer_epoch: i16::MIN,
            v3_and_below_topics: vec![
                <AddPartitionsToTxnTopic as TestInstance>::test_numeric_boundaries(),
            ],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            transactions: vec![
                <AddPartitionsToTxnTransaction as TestInstance>::test_tagged_fields(),
            ],
            v3_and_below_transactional_id: KafkaString::from("test".to_owned()),
            v3_and_below_producer_id: 9_876_543_210_i64,
            v3_and_below_producer_epoch: 42_i16,
            v3_and_below_topics: vec![
                <AddPartitionsToTxnTopic as TestInstance>::test_tagged_fields(),
            ],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for AddPartitionsToTxnTransaction {
    fn test_populated() -> Self {
        Self {
            transactional_id: KafkaString::from("test".to_owned()),
            producer_id: 9_876_543_210_i64,
            producer_epoch: 42_i16,
            verify_only: true,
            topics: vec![<AddPartitionsToTxnTopic as TestInstance>::test_populated()],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals() -> Self {
        drop(Self::default());
        Self {
            transactional_id: KafkaString::default(),
            producer_id: 0_i64,
            producer_epoch: 0_i16,
            verify_only: false,
            topics: vec![<AddPartitionsToTxnTopic as TestInstance>::test_null_optionals()],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            transactional_id: KafkaString::default(),
            producer_id: 0_i64,
            producer_epoch: 0_i16,
            verify_only: false,
            topics: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            transactional_id: KafkaString::from("test-2".to_owned()),
            producer_id: 9_876_543_211_i64,
            producer_epoch: 43_i16,
            verify_only: false,
            topics: vec![
                <AddPartitionsToTxnTopic as TestInstance>::test_populated(),
                <AddPartitionsToTxnTopic as TestInstance>::test_multi_element_collections(),
            ],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            transactional_id: KafkaString::from("boundary".to_owned()),
            producer_id: i64::MIN,
            producer_epoch: i16::MIN,
            verify_only: true,
            topics: vec![<AddPartitionsToTxnTopic as TestInstance>::test_numeric_boundaries()],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            transactional_id: KafkaString::from("test".to_owned()),
            producer_id: 9_876_543_210_i64,
            producer_epoch: 42_i16,
            verify_only: true,
            topics: vec![<AddPartitionsToTxnTopic as TestInstance>::test_tagged_fields()],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for AddPartitionsToTxnTopic {
    fn test_populated() -> Self {
        Self {
            name: KafkaString::from("test".to_owned()),
            partitions: vec![12345_i32],
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
            partitions: vec![0_i32],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            name: KafkaString::default(),
            partitions: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            name: KafkaString::from("test-2".to_owned()),
            partitions: vec![12345_i32, 23456_i32],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            name: KafkaString::from("boundary".to_owned()),
            partitions: vec![i32::MIN],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            name: KafkaString::from("test".to_owned()),
            partitions: vec![12345_i32],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
fn encode_populated(version: i16) -> crate::MatrixResult<String> {
    let message = <AddPartitionsToTxnRequestData as TestInstance>::test_populated();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_populated(version: i16) -> crate::MatrixResult<usize> {
    let message = <AddPartitionsToTxnRequestData as TestInstance>::test_populated();
    Ok(message.encoded_len(version)?)
}
fn encode_null_optionals(version: i16) -> crate::MatrixResult<String> {
    let message = <AddPartitionsToTxnRequestData as TestInstance>::test_null_optionals();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_null_optionals(version: i16) -> crate::MatrixResult<usize> {
    let message = <AddPartitionsToTxnRequestData as TestInstance>::test_null_optionals();
    Ok(message.encoded_len(version)?)
}
fn encode_empty_collections(version: i16) -> crate::MatrixResult<String> {
    let message = <AddPartitionsToTxnRequestData as TestInstance>::test_empty_collections();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_empty_collections(version: i16) -> crate::MatrixResult<usize> {
    let message = <AddPartitionsToTxnRequestData as TestInstance>::test_empty_collections();
    Ok(message.encoded_len(version)?)
}
fn encode_multi_element_collections(version: i16) -> crate::MatrixResult<String> {
    let message = <AddPartitionsToTxnRequestData as TestInstance>::test_multi_element_collections();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_multi_element_collections(version: i16) -> crate::MatrixResult<usize> {
    let message = <AddPartitionsToTxnRequestData as TestInstance>::test_multi_element_collections();
    Ok(message.encoded_len(version)?)
}
fn encode_numeric_boundaries(version: i16) -> crate::MatrixResult<String> {
    let message = <AddPartitionsToTxnRequestData as TestInstance>::test_numeric_boundaries();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_numeric_boundaries(version: i16) -> crate::MatrixResult<usize> {
    let message = <AddPartitionsToTxnRequestData as TestInstance>::test_numeric_boundaries();
    Ok(message.encoded_len(version)?)
}
fn encode_tagged_fields(version: i16) -> crate::MatrixResult<String> {
    let message = <AddPartitionsToTxnRequestData as TestInstance>::test_tagged_fields();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_tagged_fields(version: i16) -> crate::MatrixResult<usize> {
    let message = <AddPartitionsToTxnRequestData as TestInstance>::test_tagged_fields();
    Ok(message.encoded_len(version)?)
}
fn reencode(version: i16, hex_input: &str) -> crate::MatrixResult<String> {
    let mut input = Bytes::from(crate::decode_hex(hex_input)?);
    let message = AddPartitionsToTxnRequestData::read(&mut input, version)?;
    crate::ensure_input_consumed(&input)?;
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
const MATRIX_CASES: &[crate::MatrixCase] = &[
    crate::MatrixCase {
        schema_name: "AddPartitionsToTxnRequest",
        java_class: "org.apache.kafka.common.message.AddPartitionsToTxnRequestData",
        version: 0i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AddPartitionsToTxnRequest",
        java_class: "org.apache.kafka.common.message.AddPartitionsToTxnRequestData",
        version: 1i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AddPartitionsToTxnRequest",
        java_class: "org.apache.kafka.common.message.AddPartitionsToTxnRequestData",
        version: 2i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AddPartitionsToTxnRequest",
        java_class: "org.apache.kafka.common.message.AddPartitionsToTxnRequestData",
        version: 3i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AddPartitionsToTxnRequest",
        java_class: "org.apache.kafka.common.message.AddPartitionsToTxnRequestData",
        version: 4i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AddPartitionsToTxnRequest",
        java_class: "org.apache.kafka.common.message.AddPartitionsToTxnRequestData",
        version: 5i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AddPartitionsToTxnRequest",
        java_class: "org.apache.kafka.common.message.AddPartitionsToTxnRequestData",
        version: 0i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AddPartitionsToTxnRequest",
        java_class: "org.apache.kafka.common.message.AddPartitionsToTxnRequestData",
        version: 1i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AddPartitionsToTxnRequest",
        java_class: "org.apache.kafka.common.message.AddPartitionsToTxnRequestData",
        version: 2i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AddPartitionsToTxnRequest",
        java_class: "org.apache.kafka.common.message.AddPartitionsToTxnRequestData",
        version: 3i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AddPartitionsToTxnRequest",
        java_class: "org.apache.kafka.common.message.AddPartitionsToTxnRequestData",
        version: 4i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AddPartitionsToTxnRequest",
        java_class: "org.apache.kafka.common.message.AddPartitionsToTxnRequestData",
        version: 5i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AddPartitionsToTxnRequest",
        java_class: "org.apache.kafka.common.message.AddPartitionsToTxnRequestData",
        version: 0i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AddPartitionsToTxnRequest",
        java_class: "org.apache.kafka.common.message.AddPartitionsToTxnRequestData",
        version: 0i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AddPartitionsToTxnRequest",
        java_class: "org.apache.kafka.common.message.AddPartitionsToTxnRequestData",
        version: 0i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AddPartitionsToTxnRequest",
        java_class: "org.apache.kafka.common.message.AddPartitionsToTxnRequestData",
        version: 0i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AddPartitionsToTxnRequest",
        java_class: "org.apache.kafka.common.message.AddPartitionsToTxnRequestData",
        version: 1i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AddPartitionsToTxnRequest",
        java_class: "org.apache.kafka.common.message.AddPartitionsToTxnRequestData",
        version: 1i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AddPartitionsToTxnRequest",
        java_class: "org.apache.kafka.common.message.AddPartitionsToTxnRequestData",
        version: 1i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AddPartitionsToTxnRequest",
        java_class: "org.apache.kafka.common.message.AddPartitionsToTxnRequestData",
        version: 1i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AddPartitionsToTxnRequest",
        java_class: "org.apache.kafka.common.message.AddPartitionsToTxnRequestData",
        version: 2i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AddPartitionsToTxnRequest",
        java_class: "org.apache.kafka.common.message.AddPartitionsToTxnRequestData",
        version: 2i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AddPartitionsToTxnRequest",
        java_class: "org.apache.kafka.common.message.AddPartitionsToTxnRequestData",
        version: 2i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AddPartitionsToTxnRequest",
        java_class: "org.apache.kafka.common.message.AddPartitionsToTxnRequestData",
        version: 2i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AddPartitionsToTxnRequest",
        java_class: "org.apache.kafka.common.message.AddPartitionsToTxnRequestData",
        version: 3i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AddPartitionsToTxnRequest",
        java_class: "org.apache.kafka.common.message.AddPartitionsToTxnRequestData",
        version: 3i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AddPartitionsToTxnRequest",
        java_class: "org.apache.kafka.common.message.AddPartitionsToTxnRequestData",
        version: 3i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AddPartitionsToTxnRequest",
        java_class: "org.apache.kafka.common.message.AddPartitionsToTxnRequestData",
        version: 3i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AddPartitionsToTxnRequest",
        java_class: "org.apache.kafka.common.message.AddPartitionsToTxnRequestData",
        version: 4i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AddPartitionsToTxnRequest",
        java_class: "org.apache.kafka.common.message.AddPartitionsToTxnRequestData",
        version: 4i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AddPartitionsToTxnRequest",
        java_class: "org.apache.kafka.common.message.AddPartitionsToTxnRequestData",
        version: 4i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AddPartitionsToTxnRequest",
        java_class: "org.apache.kafka.common.message.AddPartitionsToTxnRequestData",
        version: 4i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AddPartitionsToTxnRequest",
        java_class: "org.apache.kafka.common.message.AddPartitionsToTxnRequestData",
        version: 5i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AddPartitionsToTxnRequest",
        java_class: "org.apache.kafka.common.message.AddPartitionsToTxnRequestData",
        version: 5i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AddPartitionsToTxnRequest",
        java_class: "org.apache.kafka.common.message.AddPartitionsToTxnRequestData",
        version: 5i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AddPartitionsToTxnRequest",
        java_class: "org.apache.kafka.common.message.AddPartitionsToTxnRequestData",
        version: 5i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
];
pub(crate) fn append_protocol_cases(cases: &mut Vec<crate::MatrixCase>) {
    cases.extend_from_slice(MATRIX_CASES);
}
