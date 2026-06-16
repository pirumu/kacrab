use bytes::{Bytes, BytesMut};
use kacrab_protocol::{generated::txn_offset_commit_request::*, *};

use crate::TestInstance;

impl TestInstance for TxnOffsetCommitRequestData {
    fn test_populated() -> Self {
        Self {
            transactional_id: KafkaString::from("test".to_owned()),
            group_id: KafkaString::from("test".to_owned()),
            producer_id: 9_876_543_210_i64,
            producer_epoch: 42_i16,
            generation_id: 12345_i32,
            member_id: KafkaString::from("test".to_owned()),
            group_instance_id: Some(KafkaString::from("test".to_owned())),
            topics: vec![<TxnOffsetCommitRequestTopic as TestInstance>::test_populated()],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals() -> Self {
        Self {
            transactional_id: KafkaString::default(),
            group_id: KafkaString::default(),
            producer_id: 0_i64,
            producer_epoch: 0_i16,
            generation_id: 0_i32,
            member_id: KafkaString::default(),
            group_instance_id: None,
            topics: vec![<TxnOffsetCommitRequestTopic as TestInstance>::test_null_optionals()],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            transactional_id: KafkaString::default(),
            group_id: KafkaString::default(),
            producer_id: 0_i64,
            producer_epoch: 0_i16,
            generation_id: 0_i32,
            member_id: KafkaString::default(),
            group_instance_id: Some(KafkaString::default()),
            topics: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            transactional_id: KafkaString::from("test-2".to_owned()),
            group_id: KafkaString::from("test-2".to_owned()),
            producer_id: 9_876_543_211_i64,
            producer_epoch: 43_i16,
            generation_id: 23456_i32,
            member_id: KafkaString::from("test-2".to_owned()),
            group_instance_id: Some(KafkaString::from("test-2".to_owned())),
            topics: vec![
                <TxnOffsetCommitRequestTopic as TestInstance>::test_populated(),
                <TxnOffsetCommitRequestTopic as TestInstance>::test_multi_element_collections(),
            ],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            transactional_id: KafkaString::from("boundary".to_owned()),
            group_id: KafkaString::from("boundary".to_owned()),
            producer_id: i64::MIN,
            producer_epoch: i16::MIN,
            generation_id: i32::MIN,
            member_id: KafkaString::from("boundary".to_owned()),
            group_instance_id: Some(KafkaString::from("boundary".to_owned())),
            topics: vec![<TxnOffsetCommitRequestTopic as TestInstance>::test_numeric_boundaries()],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            transactional_id: KafkaString::from("test".to_owned()),
            group_id: KafkaString::from("test".to_owned()),
            producer_id: 9_876_543_210_i64,
            producer_epoch: 42_i16,
            generation_id: 12345_i32,
            member_id: KafkaString::from("test".to_owned()),
            group_instance_id: Some(KafkaString::from("test".to_owned())),
            topics: vec![<TxnOffsetCommitRequestTopic as TestInstance>::test_tagged_fields()],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for TxnOffsetCommitRequestTopic {
    fn test_populated() -> Self {
        Self {
            name: KafkaString::from("test".to_owned()),
            partitions: vec![<TxnOffsetCommitRequestPartition as TestInstance>::test_populated()],
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
            partitions: vec![
                <TxnOffsetCommitRequestPartition as TestInstance>::test_null_optionals(),
            ],
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
            partitions: vec![
                <TxnOffsetCommitRequestPartition as TestInstance>::test_populated(),
                <TxnOffsetCommitRequestPartition as TestInstance>::test_multi_element_collections(),
            ],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            name: KafkaString::from("boundary".to_owned()),
            partitions: vec![
                <TxnOffsetCommitRequestPartition as TestInstance>::test_numeric_boundaries(),
            ],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            name: KafkaString::from("test".to_owned()),
            partitions: vec![
                <TxnOffsetCommitRequestPartition as TestInstance>::test_tagged_fields(),
            ],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for TxnOffsetCommitRequestPartition {
    fn test_populated() -> Self {
        Self {
            partition_index: 12345_i32,
            committed_offset: 9_876_543_210_i64,
            committed_leader_epoch: 12345_i32,
            committed_metadata: Some(KafkaString::from("test".to_owned())),
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals() -> Self {
        drop(Self::default());
        Self {
            partition_index: 0_i32,
            committed_offset: 0_i64,
            committed_leader_epoch: 0_i32,
            committed_metadata: None,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            partition_index: 0_i32,
            committed_offset: 0_i64,
            committed_leader_epoch: 0_i32,
            committed_metadata: Some(KafkaString::default()),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            partition_index: 23456_i32,
            committed_offset: 9_876_543_211_i64,
            committed_leader_epoch: 23456_i32,
            committed_metadata: Some(KafkaString::from("test-2".to_owned())),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            partition_index: i32::MIN,
            committed_offset: i64::MIN,
            committed_leader_epoch: i32::MIN,
            committed_metadata: Some(KafkaString::from("boundary".to_owned())),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            partition_index: 12345_i32,
            committed_offset: 9_876_543_210_i64,
            committed_leader_epoch: 12345_i32,
            committed_metadata: Some(KafkaString::from("test".to_owned())),
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
fn encode_populated(version: i16) -> crate::MatrixResult<String> {
    let message = <TxnOffsetCommitRequestData as TestInstance>::test_populated();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_populated(version: i16) -> crate::MatrixResult<usize> {
    let message = <TxnOffsetCommitRequestData as TestInstance>::test_populated();
    Ok(message.encoded_len(version)?)
}
fn encode_null_optionals(version: i16) -> crate::MatrixResult<String> {
    let message = <TxnOffsetCommitRequestData as TestInstance>::test_null_optionals();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_null_optionals(version: i16) -> crate::MatrixResult<usize> {
    let message = <TxnOffsetCommitRequestData as TestInstance>::test_null_optionals();
    Ok(message.encoded_len(version)?)
}
fn encode_empty_collections(version: i16) -> crate::MatrixResult<String> {
    let message = <TxnOffsetCommitRequestData as TestInstance>::test_empty_collections();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_empty_collections(version: i16) -> crate::MatrixResult<usize> {
    let message = <TxnOffsetCommitRequestData as TestInstance>::test_empty_collections();
    Ok(message.encoded_len(version)?)
}
fn encode_multi_element_collections(version: i16) -> crate::MatrixResult<String> {
    let message = <TxnOffsetCommitRequestData as TestInstance>::test_multi_element_collections();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_multi_element_collections(version: i16) -> crate::MatrixResult<usize> {
    let message = <TxnOffsetCommitRequestData as TestInstance>::test_multi_element_collections();
    Ok(message.encoded_len(version)?)
}
fn encode_numeric_boundaries(version: i16) -> crate::MatrixResult<String> {
    let message = <TxnOffsetCommitRequestData as TestInstance>::test_numeric_boundaries();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_numeric_boundaries(version: i16) -> crate::MatrixResult<usize> {
    let message = <TxnOffsetCommitRequestData as TestInstance>::test_numeric_boundaries();
    Ok(message.encoded_len(version)?)
}
fn encode_tagged_fields(version: i16) -> crate::MatrixResult<String> {
    let message = <TxnOffsetCommitRequestData as TestInstance>::test_tagged_fields();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_tagged_fields(version: i16) -> crate::MatrixResult<usize> {
    let message = <TxnOffsetCommitRequestData as TestInstance>::test_tagged_fields();
    Ok(message.encoded_len(version)?)
}
fn reencode(version: i16, hex_input: &str) -> crate::MatrixResult<String> {
    let mut input = Bytes::from(crate::decode_hex(hex_input)?);
    let message = TxnOffsetCommitRequestData::read(&mut input, version)?;
    crate::ensure_input_consumed(&input)?;
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
const MATRIX_CASES: &[crate::MatrixCase] = &[
    crate::MatrixCase {
        schema_name: "TxnOffsetCommitRequest",
        java_class: "org.apache.kafka.common.message.TxnOffsetCommitRequestData",
        version: 0i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "TxnOffsetCommitRequest",
        java_class: "org.apache.kafka.common.message.TxnOffsetCommitRequestData",
        version: 1i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "TxnOffsetCommitRequest",
        java_class: "org.apache.kafka.common.message.TxnOffsetCommitRequestData",
        version: 2i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "TxnOffsetCommitRequest",
        java_class: "org.apache.kafka.common.message.TxnOffsetCommitRequestData",
        version: 3i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "TxnOffsetCommitRequest",
        java_class: "org.apache.kafka.common.message.TxnOffsetCommitRequestData",
        version: 4i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "TxnOffsetCommitRequest",
        java_class: "org.apache.kafka.common.message.TxnOffsetCommitRequestData",
        version: 5i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "TxnOffsetCommitRequest",
        java_class: "org.apache.kafka.common.message.TxnOffsetCommitRequestData",
        version: 0i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "TxnOffsetCommitRequest",
        java_class: "org.apache.kafka.common.message.TxnOffsetCommitRequestData",
        version: 1i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "TxnOffsetCommitRequest",
        java_class: "org.apache.kafka.common.message.TxnOffsetCommitRequestData",
        version: 2i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "TxnOffsetCommitRequest",
        java_class: "org.apache.kafka.common.message.TxnOffsetCommitRequestData",
        version: 3i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "TxnOffsetCommitRequest",
        java_class: "org.apache.kafka.common.message.TxnOffsetCommitRequestData",
        version: 4i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "TxnOffsetCommitRequest",
        java_class: "org.apache.kafka.common.message.TxnOffsetCommitRequestData",
        version: 5i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "TxnOffsetCommitRequest",
        java_class: "org.apache.kafka.common.message.TxnOffsetCommitRequestData",
        version: 0i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "TxnOffsetCommitRequest",
        java_class: "org.apache.kafka.common.message.TxnOffsetCommitRequestData",
        version: 0i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "TxnOffsetCommitRequest",
        java_class: "org.apache.kafka.common.message.TxnOffsetCommitRequestData",
        version: 0i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "TxnOffsetCommitRequest",
        java_class: "org.apache.kafka.common.message.TxnOffsetCommitRequestData",
        version: 0i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "TxnOffsetCommitRequest",
        java_class: "org.apache.kafka.common.message.TxnOffsetCommitRequestData",
        version: 1i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "TxnOffsetCommitRequest",
        java_class: "org.apache.kafka.common.message.TxnOffsetCommitRequestData",
        version: 1i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "TxnOffsetCommitRequest",
        java_class: "org.apache.kafka.common.message.TxnOffsetCommitRequestData",
        version: 1i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "TxnOffsetCommitRequest",
        java_class: "org.apache.kafka.common.message.TxnOffsetCommitRequestData",
        version: 1i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "TxnOffsetCommitRequest",
        java_class: "org.apache.kafka.common.message.TxnOffsetCommitRequestData",
        version: 2i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "TxnOffsetCommitRequest",
        java_class: "org.apache.kafka.common.message.TxnOffsetCommitRequestData",
        version: 2i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "TxnOffsetCommitRequest",
        java_class: "org.apache.kafka.common.message.TxnOffsetCommitRequestData",
        version: 2i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "TxnOffsetCommitRequest",
        java_class: "org.apache.kafka.common.message.TxnOffsetCommitRequestData",
        version: 2i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "TxnOffsetCommitRequest",
        java_class: "org.apache.kafka.common.message.TxnOffsetCommitRequestData",
        version: 3i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "TxnOffsetCommitRequest",
        java_class: "org.apache.kafka.common.message.TxnOffsetCommitRequestData",
        version: 3i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "TxnOffsetCommitRequest",
        java_class: "org.apache.kafka.common.message.TxnOffsetCommitRequestData",
        version: 3i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "TxnOffsetCommitRequest",
        java_class: "org.apache.kafka.common.message.TxnOffsetCommitRequestData",
        version: 3i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "TxnOffsetCommitRequest",
        java_class: "org.apache.kafka.common.message.TxnOffsetCommitRequestData",
        version: 4i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "TxnOffsetCommitRequest",
        java_class: "org.apache.kafka.common.message.TxnOffsetCommitRequestData",
        version: 4i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "TxnOffsetCommitRequest",
        java_class: "org.apache.kafka.common.message.TxnOffsetCommitRequestData",
        version: 4i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "TxnOffsetCommitRequest",
        java_class: "org.apache.kafka.common.message.TxnOffsetCommitRequestData",
        version: 4i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "TxnOffsetCommitRequest",
        java_class: "org.apache.kafka.common.message.TxnOffsetCommitRequestData",
        version: 5i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "TxnOffsetCommitRequest",
        java_class: "org.apache.kafka.common.message.TxnOffsetCommitRequestData",
        version: 5i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "TxnOffsetCommitRequest",
        java_class: "org.apache.kafka.common.message.TxnOffsetCommitRequestData",
        version: 5i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "TxnOffsetCommitRequest",
        java_class: "org.apache.kafka.common.message.TxnOffsetCommitRequestData",
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
