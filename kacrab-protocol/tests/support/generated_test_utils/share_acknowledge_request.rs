#![allow(
    clippy::all,
    clippy::pedantic,
    clippy::nursery,
    reason = "Generated test fixtures mirror Kafka's schema shape and trade hand-written lint \
              style for reproducible output, matching the generated protocol modules."
)]
use bytes::{Bytes, BytesMut};
use kacrab_protocol::{generated::share_acknowledge_request::*, *};

use crate::TestInstance;

impl TestInstance for ShareAcknowledgeRequestData {
    fn test_populated(version: i16) -> Self {
        Self {
            group_id: Some(KafkaString::from("test".to_owned())),
            member_id: Some(KafkaString::from("test".to_owned())),
            share_session_epoch: 12345_i32,
            is_renew_ack: if version >= 2 { true } else { false },
            topics: vec![<AcknowledgeTopic as TestInstance>::test_populated(version)],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals(version: i16) -> Self {
        Self {
            group_id: None,
            member_id: None,
            share_session_epoch: 0_i32,
            is_renew_ack: false,
            topics: vec![<AcknowledgeTopic as TestInstance>::test_null_optionals(
                version,
            )],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections(_version: i16) -> Self {
        Self {
            group_id: Some(KafkaString::default()),
            member_id: Some(KafkaString::default()),
            share_session_epoch: 0_i32,
            is_renew_ack: false,
            topics: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections(version: i16) -> Self {
        Self {
            group_id: Some(KafkaString::from("test-2".to_owned())),
            member_id: Some(KafkaString::from("test-2".to_owned())),
            share_session_epoch: 23456_i32,
            is_renew_ack: false,
            topics: vec![
                <AcknowledgeTopic as TestInstance>::test_populated(version),
                <AcknowledgeTopic as TestInstance>::test_multi_element_collections(version),
            ],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries(version: i16) -> Self {
        Self {
            group_id: Some(KafkaString::from("boundary".to_owned())),
            member_id: Some(KafkaString::from("boundary".to_owned())),
            share_session_epoch: i32::MIN,
            is_renew_ack: if version >= 2 { true } else { false },
            topics: vec![<AcknowledgeTopic as TestInstance>::test_numeric_boundaries(
                version,
            )],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields(version: i16) -> Self {
        Self {
            group_id: Some(KafkaString::from("test".to_owned())),
            member_id: Some(KafkaString::from("test".to_owned())),
            share_session_epoch: 12345_i32,
            is_renew_ack: if version >= 2 { true } else { false },
            topics: vec![<AcknowledgeTopic as TestInstance>::test_tagged_fields(
                version,
            )],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for AcknowledgeTopic {
    fn test_populated(version: i16) -> Self {
        Self {
            topic_id: KafkaUuid::ONE,
            partitions: vec![<AcknowledgePartition as TestInstance>::test_populated(
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
            topic_id: KafkaUuid::ZERO,
            partitions: vec![<AcknowledgePartition as TestInstance>::test_null_optionals(
                version,
            )],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections(_version: i16) -> Self {
        Self {
            topic_id: KafkaUuid::ZERO,
            partitions: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections(version: i16) -> Self {
        Self {
            topic_id: KafkaUuid::from_parts(2, 3),
            partitions: vec![
                <AcknowledgePartition as TestInstance>::test_populated(version),
                <AcknowledgePartition as TestInstance>::test_multi_element_collections(version),
            ],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries(version: i16) -> Self {
        Self {
            topic_id: KafkaUuid::ONE,
            partitions: vec![
                <AcknowledgePartition as TestInstance>::test_numeric_boundaries(version),
            ],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields(version: i16) -> Self {
        Self {
            topic_id: KafkaUuid::ONE,
            partitions: vec![<AcknowledgePartition as TestInstance>::test_tagged_fields(
                version,
            )],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for AcknowledgePartition {
    fn test_populated(version: i16) -> Self {
        Self {
            partition_index: 12345_i32,
            acknowledgement_batches: vec![<AcknowledgementBatch as TestInstance>::test_populated(
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
            partition_index: 0_i32,
            acknowledgement_batches: vec![
                <AcknowledgementBatch as TestInstance>::test_null_optionals(version),
            ],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections(_version: i16) -> Self {
        Self {
            partition_index: 0_i32,
            acknowledgement_batches: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections(version: i16) -> Self {
        Self {
            partition_index: 23456_i32,
            acknowledgement_batches: vec![
                <AcknowledgementBatch as TestInstance>::test_populated(version),
                <AcknowledgementBatch as TestInstance>::test_multi_element_collections(version),
            ],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries(version: i16) -> Self {
        Self {
            partition_index: i32::MIN,
            acknowledgement_batches: vec![
                <AcknowledgementBatch as TestInstance>::test_numeric_boundaries(version),
            ],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields(version: i16) -> Self {
        Self {
            partition_index: 12345_i32,
            acknowledgement_batches: vec![
                <AcknowledgementBatch as TestInstance>::test_tagged_fields(version),
            ],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for AcknowledgementBatch {
    fn test_populated(_version: i16) -> Self {
        Self {
            first_offset: 9_876_543_210_i64,
            last_offset: 9_876_543_210_i64,
            acknowledge_types: vec![7_i8],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals(_version: i16) -> Self {
        drop(Self::default());
        Self {
            first_offset: 0_i64,
            last_offset: 0_i64,
            acknowledge_types: vec![0_i8],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections(_version: i16) -> Self {
        Self {
            first_offset: 0_i64,
            last_offset: 0_i64,
            acknowledge_types: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections(_version: i16) -> Self {
        Self {
            first_offset: 9_876_543_211_i64,
            last_offset: 9_876_543_211_i64,
            acknowledge_types: vec![7_i8, 8_i8],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries(_version: i16) -> Self {
        Self {
            first_offset: i64::MIN,
            last_offset: i64::MIN,
            acknowledge_types: vec![i8::MIN],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields(_version: i16) -> Self {
        Self {
            first_offset: 9_876_543_210_i64,
            last_offset: 9_876_543_210_i64,
            acknowledge_types: vec![7_i8],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
fn encode_populated(version: i16) -> crate::MatrixResult<String> {
    let message = <ShareAcknowledgeRequestData as TestInstance>::test_populated(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_populated(version: i16) -> crate::MatrixResult<usize> {
    let message = <ShareAcknowledgeRequestData as TestInstance>::test_populated(version);
    Ok(message.encoded_len(version)?)
}
fn encode_null_optionals(version: i16) -> crate::MatrixResult<String> {
    let message = <ShareAcknowledgeRequestData as TestInstance>::test_null_optionals(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_null_optionals(version: i16) -> crate::MatrixResult<usize> {
    let message = <ShareAcknowledgeRequestData as TestInstance>::test_null_optionals(version);
    Ok(message.encoded_len(version)?)
}
fn encode_empty_collections(version: i16) -> crate::MatrixResult<String> {
    let message = <ShareAcknowledgeRequestData as TestInstance>::test_empty_collections(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_empty_collections(version: i16) -> crate::MatrixResult<usize> {
    let message = <ShareAcknowledgeRequestData as TestInstance>::test_empty_collections(version);
    Ok(message.encoded_len(version)?)
}
fn encode_multi_element_collections(version: i16) -> crate::MatrixResult<String> {
    let message =
        <ShareAcknowledgeRequestData as TestInstance>::test_multi_element_collections(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_multi_element_collections(version: i16) -> crate::MatrixResult<usize> {
    let message =
        <ShareAcknowledgeRequestData as TestInstance>::test_multi_element_collections(version);
    Ok(message.encoded_len(version)?)
}
fn encode_numeric_boundaries(version: i16) -> crate::MatrixResult<String> {
    let message = <ShareAcknowledgeRequestData as TestInstance>::test_numeric_boundaries(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_numeric_boundaries(version: i16) -> crate::MatrixResult<usize> {
    let message = <ShareAcknowledgeRequestData as TestInstance>::test_numeric_boundaries(version);
    Ok(message.encoded_len(version)?)
}
fn encode_tagged_fields(version: i16) -> crate::MatrixResult<String> {
    let message = <ShareAcknowledgeRequestData as TestInstance>::test_tagged_fields(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_tagged_fields(version: i16) -> crate::MatrixResult<usize> {
    let message = <ShareAcknowledgeRequestData as TestInstance>::test_tagged_fields(version);
    Ok(message.encoded_len(version)?)
}
fn reencode(version: i16, hex_input: &str) -> crate::MatrixResult<String> {
    let mut input = Bytes::from(crate::decode_hex(hex_input)?);
    let message = ShareAcknowledgeRequestData::read(&mut input, version)?;
    crate::ensure_input_consumed(&input)?;
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
const MATRIX_CASES: &[crate::MatrixCase] = &[
    crate::MatrixCase {
        schema_name: "ShareAcknowledgeRequest",
        java_class: "org.apache.kafka.common.message.ShareAcknowledgeRequestData",
        version: 1i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ShareAcknowledgeRequest",
        java_class: "org.apache.kafka.common.message.ShareAcknowledgeRequestData",
        version: 2i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ShareAcknowledgeRequest",
        java_class: "org.apache.kafka.common.message.ShareAcknowledgeRequestData",
        version: 1i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ShareAcknowledgeRequest",
        java_class: "org.apache.kafka.common.message.ShareAcknowledgeRequestData",
        version: 2i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ShareAcknowledgeRequest",
        java_class: "org.apache.kafka.common.message.ShareAcknowledgeRequestData",
        version: 1i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ShareAcknowledgeRequest",
        java_class: "org.apache.kafka.common.message.ShareAcknowledgeRequestData",
        version: 1i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ShareAcknowledgeRequest",
        java_class: "org.apache.kafka.common.message.ShareAcknowledgeRequestData",
        version: 1i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ShareAcknowledgeRequest",
        java_class: "org.apache.kafka.common.message.ShareAcknowledgeRequestData",
        version: 1i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ShareAcknowledgeRequest",
        java_class: "org.apache.kafka.common.message.ShareAcknowledgeRequestData",
        version: 2i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ShareAcknowledgeRequest",
        java_class: "org.apache.kafka.common.message.ShareAcknowledgeRequestData",
        version: 2i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ShareAcknowledgeRequest",
        java_class: "org.apache.kafka.common.message.ShareAcknowledgeRequestData",
        version: 2i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ShareAcknowledgeRequest",
        java_class: "org.apache.kafka.common.message.ShareAcknowledgeRequestData",
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
