#![allow(
    clippy::all,
    clippy::pedantic,
    clippy::nursery,
    reason = "Generated test fixtures mirror Kafka's schema shape and trade hand-written lint \
              style for reproducible output, matching the generated protocol modules."
)]
use bytes::{Bytes, BytesMut};
use kacrab_protocol::{generated::add_partitions_to_txn_request::*, *};

use crate::TestInstance;

impl TestInstance for AddPartitionsToTxnRequestData {
    fn test_populated(version: i16) -> Self {
        Self {
            transactions: if version >= 4 {
                vec![<AddPartitionsToTxnTransaction as TestInstance>::test_populated(version)]
            } else {
                Vec::new()
            },
            v3_and_below_transactional_id: if version <= 3 {
                KafkaString::from("test".to_owned())
            } else {
                KafkaString::default()
            },
            v3_and_below_producer_id: if version <= 3 {
                9_876_543_210_i64
            } else {
                0_i64
            },
            v3_and_below_producer_epoch: if version <= 3 { 42_i16 } else { 0_i16 },
            v3_and_below_topics: if version <= 3 {
                vec![<AddPartitionsToTxnTopic as TestInstance>::test_populated(
                    version,
                )]
            } else {
                Vec::new()
            },
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals(version: i16) -> Self {
        Self {
            transactions: if version >= 4 {
                vec![<AddPartitionsToTxnTransaction as TestInstance>::test_null_optionals(version)]
            } else {
                Vec::new()
            },
            v3_and_below_transactional_id: KafkaString::default(),
            v3_and_below_producer_id: 0_i64,
            v3_and_below_producer_epoch: 0_i16,
            v3_and_below_topics: if version <= 3 {
                vec![<AddPartitionsToTxnTopic as TestInstance>::test_null_optionals(version)]
            } else {
                Vec::new()
            },
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections(_version: i16) -> Self {
        Self {
            transactions: Vec::new(),
            v3_and_below_transactional_id: KafkaString::default(),
            v3_and_below_producer_id: 0_i64,
            v3_and_below_producer_epoch: 0_i16,
            v3_and_below_topics: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections(version: i16) -> Self {
        Self {
            transactions: if version >= 4 {
                vec![
                    <AddPartitionsToTxnTransaction as TestInstance>::test_populated(version),
                    <AddPartitionsToTxnTransaction as TestInstance>::test_multi_element_collections(
                        version,
                    ),
                ]
            } else {
                Vec::new()
            },
            v3_and_below_transactional_id: if version <= 3 {
                KafkaString::from("test-2".to_owned())
            } else {
                KafkaString::default()
            },
            v3_and_below_producer_id: if version <= 3 {
                9_876_543_211_i64
            } else {
                0_i64
            },
            v3_and_below_producer_epoch: if version <= 3 { 43_i16 } else { 0_i16 },
            v3_and_below_topics: if version <= 3 {
                vec![
                    <AddPartitionsToTxnTopic as TestInstance>::test_populated(version),
                    <AddPartitionsToTxnTopic as TestInstance>::test_multi_element_collections(
                        version,
                    ),
                ]
            } else {
                Vec::new()
            },
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries(version: i16) -> Self {
        Self {
            transactions: if version >= 4 {
                vec![
                    <AddPartitionsToTxnTransaction as TestInstance>::test_numeric_boundaries(
                        version,
                    ),
                ]
            } else {
                Vec::new()
            },
            v3_and_below_transactional_id: if version <= 3 {
                KafkaString::from("boundary".to_owned())
            } else {
                KafkaString::default()
            },
            v3_and_below_producer_id: if version <= 3 { i64::MIN } else { 0_i64 },
            v3_and_below_producer_epoch: if version <= 3 { i16::MIN } else { 0_i16 },
            v3_and_below_topics: if version <= 3 {
                vec![<AddPartitionsToTxnTopic as TestInstance>::test_numeric_boundaries(version)]
            } else {
                Vec::new()
            },
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields(version: i16) -> Self {
        Self {
            transactions: if version >= 4 {
                vec![<AddPartitionsToTxnTransaction as TestInstance>::test_tagged_fields(version)]
            } else {
                Vec::new()
            },
            v3_and_below_transactional_id: if version <= 3 {
                KafkaString::from("test".to_owned())
            } else {
                KafkaString::default()
            },
            v3_and_below_producer_id: if version <= 3 {
                9_876_543_210_i64
            } else {
                0_i64
            },
            v3_and_below_producer_epoch: if version <= 3 { 42_i16 } else { 0_i16 },
            v3_and_below_topics: if version <= 3 {
                vec![<AddPartitionsToTxnTopic as TestInstance>::test_tagged_fields(version)]
            } else {
                Vec::new()
            },
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for AddPartitionsToTxnTransaction {
    fn test_populated(version: i16) -> Self {
        Self {
            transactional_id: KafkaString::from("test".to_owned()),
            producer_id: 9_876_543_210_i64,
            producer_epoch: 42_i16,
            verify_only: true,
            topics: vec![<AddPartitionsToTxnTopic as TestInstance>::test_populated(
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
            transactional_id: KafkaString::default(),
            producer_id: 0_i64,
            producer_epoch: 0_i16,
            verify_only: false,
            topics: vec![<AddPartitionsToTxnTopic as TestInstance>::test_null_optionals(version)],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections(_version: i16) -> Self {
        Self {
            transactional_id: KafkaString::default(),
            producer_id: 0_i64,
            producer_epoch: 0_i16,
            verify_only: false,
            topics: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections(version: i16) -> Self {
        Self {
            transactional_id: KafkaString::from("test-2".to_owned()),
            producer_id: 9_876_543_211_i64,
            producer_epoch: 43_i16,
            verify_only: false,
            topics: vec![
                <AddPartitionsToTxnTopic as TestInstance>::test_populated(version),
                <AddPartitionsToTxnTopic as TestInstance>::test_multi_element_collections(version),
            ],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries(version: i16) -> Self {
        Self {
            transactional_id: KafkaString::from("boundary".to_owned()),
            producer_id: i64::MIN,
            producer_epoch: i16::MIN,
            verify_only: true,
            topics: vec![
                <AddPartitionsToTxnTopic as TestInstance>::test_numeric_boundaries(version),
            ],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields(version: i16) -> Self {
        Self {
            transactional_id: KafkaString::from("test".to_owned()),
            producer_id: 9_876_543_210_i64,
            producer_epoch: 42_i16,
            verify_only: true,
            topics: vec![<AddPartitionsToTxnTopic as TestInstance>::test_tagged_fields(version)],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for AddPartitionsToTxnTopic {
    fn test_populated(_version: i16) -> Self {
        Self {
            name: KafkaString::from("test".to_owned()),
            partitions: vec![12345_i32],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals(_version: i16) -> Self {
        drop(Self::default());
        Self {
            name: KafkaString::default(),
            partitions: vec![0_i32],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections(_version: i16) -> Self {
        Self {
            name: KafkaString::default(),
            partitions: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections(_version: i16) -> Self {
        Self {
            name: KafkaString::from("test-2".to_owned()),
            partitions: vec![12345_i32, 23456_i32],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries(_version: i16) -> Self {
        Self {
            name: KafkaString::from("boundary".to_owned()),
            partitions: vec![i32::MIN],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields(_version: i16) -> Self {
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
    let message = <AddPartitionsToTxnRequestData as TestInstance>::test_populated(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_populated(version: i16) -> crate::MatrixResult<usize> {
    let message = <AddPartitionsToTxnRequestData as TestInstance>::test_populated(version);
    Ok(message.encoded_len(version)?)
}
fn encode_null_optionals(version: i16) -> crate::MatrixResult<String> {
    let message = <AddPartitionsToTxnRequestData as TestInstance>::test_null_optionals(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_null_optionals(version: i16) -> crate::MatrixResult<usize> {
    let message = <AddPartitionsToTxnRequestData as TestInstance>::test_null_optionals(version);
    Ok(message.encoded_len(version)?)
}
fn encode_empty_collections(version: i16) -> crate::MatrixResult<String> {
    let message = <AddPartitionsToTxnRequestData as TestInstance>::test_empty_collections(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_empty_collections(version: i16) -> crate::MatrixResult<usize> {
    let message = <AddPartitionsToTxnRequestData as TestInstance>::test_empty_collections(version);
    Ok(message.encoded_len(version)?)
}
fn encode_multi_element_collections(version: i16) -> crate::MatrixResult<String> {
    let message =
        <AddPartitionsToTxnRequestData as TestInstance>::test_multi_element_collections(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_multi_element_collections(version: i16) -> crate::MatrixResult<usize> {
    let message =
        <AddPartitionsToTxnRequestData as TestInstance>::test_multi_element_collections(version);
    Ok(message.encoded_len(version)?)
}
fn encode_numeric_boundaries(version: i16) -> crate::MatrixResult<String> {
    let message = <AddPartitionsToTxnRequestData as TestInstance>::test_numeric_boundaries(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_numeric_boundaries(version: i16) -> crate::MatrixResult<usize> {
    let message = <AddPartitionsToTxnRequestData as TestInstance>::test_numeric_boundaries(version);
    Ok(message.encoded_len(version)?)
}
fn encode_tagged_fields(version: i16) -> crate::MatrixResult<String> {
    let message = <AddPartitionsToTxnRequestData as TestInstance>::test_tagged_fields(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_tagged_fields(version: i16) -> crate::MatrixResult<usize> {
    let message = <AddPartitionsToTxnRequestData as TestInstance>::test_tagged_fields(version);
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
