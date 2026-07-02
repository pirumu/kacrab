#![allow(
    clippy::all,
    clippy::pedantic,
    clippy::nursery,
    reason = "Generated test fixtures mirror Kafka's schema shape and trade hand-written lint \
              style for reproducible output, matching the generated protocol modules."
)]
use bytes::{Bytes, BytesMut};
use kacrab_protocol::{generated::offset_fetch_response::*, *};

use crate::TestInstance;

impl TestInstance for OffsetFetchResponseData {
    fn test_populated(version: i16) -> Self {
        Self {
            throttle_time_ms: if version >= 3 { 12345_i32 } else { 0_i32 },
            topics: if version <= 7 {
                vec![<OffsetFetchResponseTopic as TestInstance>::test_populated(
                    version,
                )]
            } else {
                Vec::new()
            },
            error_code: if version >= 2 && version <= 7 {
                42_i16
            } else {
                0i16
            },
            groups: if version >= 8 {
                vec![<OffsetFetchResponseGroup as TestInstance>::test_populated(
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
            throttle_time_ms: 0_i32,
            topics: if version <= 7 {
                vec![<OffsetFetchResponseTopic as TestInstance>::test_null_optionals(version)]
            } else {
                Vec::new()
            },
            error_code: if version >= 2 && version <= 7 {
                0_i16
            } else {
                0i16
            },
            groups: if version >= 8 {
                vec![<OffsetFetchResponseGroup as TestInstance>::test_null_optionals(version)]
            } else {
                Vec::new()
            },
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections(version: i16) -> Self {
        Self {
            throttle_time_ms: 0_i32,
            topics: Vec::new(),
            error_code: if version >= 2 && version <= 7 {
                0_i16
            } else {
                0i16
            },
            groups: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections(version: i16) -> Self {
        Self {
            throttle_time_ms: if version >= 3 { 23456_i32 } else { 0_i32 },
            topics: if version <= 7 {
                vec![
                    <OffsetFetchResponseTopic as TestInstance>::test_populated(version),
                    <OffsetFetchResponseTopic as TestInstance>::test_multi_element_collections(
                        version,
                    ),
                ]
            } else {
                Vec::new()
            },
            error_code: if version >= 2 && version <= 7 {
                43_i16
            } else {
                0i16
            },
            groups: if version >= 8 {
                vec![
                    <OffsetFetchResponseGroup as TestInstance>::test_populated(version),
                    <OffsetFetchResponseGroup as TestInstance>::test_multi_element_collections(
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
            throttle_time_ms: if version >= 3 { i32::MIN } else { 0_i32 },
            topics: if version <= 7 {
                vec![<OffsetFetchResponseTopic as TestInstance>::test_numeric_boundaries(version)]
            } else {
                Vec::new()
            },
            error_code: if version >= 2 && version <= 7 {
                i16::MIN
            } else {
                0i16
            },
            groups: if version >= 8 {
                vec![<OffsetFetchResponseGroup as TestInstance>::test_numeric_boundaries(version)]
            } else {
                Vec::new()
            },
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields(version: i16) -> Self {
        Self {
            throttle_time_ms: if version >= 3 { 12345_i32 } else { 0_i32 },
            topics: if version <= 7 {
                vec![<OffsetFetchResponseTopic as TestInstance>::test_tagged_fields(version)]
            } else {
                Vec::new()
            },
            error_code: if version >= 2 && version <= 7 {
                42_i16
            } else {
                0i16
            },
            groups: if version >= 8 {
                vec![<OffsetFetchResponseGroup as TestInstance>::test_tagged_fields(version)]
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
impl TestInstance for OffsetFetchResponseTopic {
    fn test_populated(version: i16) -> Self {
        Self {
            name: KafkaString::from("test".to_owned()),
            partitions: vec![
                <OffsetFetchResponsePartition as TestInstance>::test_populated(version),
            ],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals(version: i16) -> Self {
        drop(Self::default());
        Self {
            name: KafkaString::default(),
            partitions: vec![
                <OffsetFetchResponsePartition as TestInstance>::test_null_optionals(version),
            ],
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
    fn test_multi_element_collections(version: i16) -> Self {
        Self {
            name: KafkaString::from("test-2".to_owned()),
            partitions: vec![
                <OffsetFetchResponsePartition as TestInstance>::test_populated(version),
                <OffsetFetchResponsePartition as TestInstance>::test_multi_element_collections(
                    version,
                ),
            ],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries(version: i16) -> Self {
        Self {
            name: KafkaString::from("boundary".to_owned()),
            partitions: vec![
                <OffsetFetchResponsePartition as TestInstance>::test_numeric_boundaries(version),
            ],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields(version: i16) -> Self {
        Self {
            name: KafkaString::from("test".to_owned()),
            partitions: vec![
                <OffsetFetchResponsePartition as TestInstance>::test_tagged_fields(version),
            ],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for OffsetFetchResponsePartition {
    fn test_populated(version: i16) -> Self {
        Self {
            partition_index: 12345_i32,
            committed_offset: 9_876_543_210_i64,
            committed_leader_epoch: if version >= 5 { 12345_i32 } else { -1i32 },
            metadata: Some(KafkaString::from("test".to_owned())),
            error_code: 42_i16,
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
            committed_offset: 0_i64,
            committed_leader_epoch: if version >= 5 { 0_i32 } else { -1i32 },
            metadata: None,
            error_code: 0_i16,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections(version: i16) -> Self {
        Self {
            partition_index: 0_i32,
            committed_offset: 0_i64,
            committed_leader_epoch: if version >= 5 { 0_i32 } else { -1i32 },
            metadata: Some(KafkaString::default()),
            error_code: 0_i16,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections(version: i16) -> Self {
        Self {
            partition_index: 23456_i32,
            committed_offset: 9_876_543_211_i64,
            committed_leader_epoch: if version >= 5 { 23456_i32 } else { -1i32 },
            metadata: Some(KafkaString::from("test-2".to_owned())),
            error_code: 43_i16,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries(version: i16) -> Self {
        Self {
            partition_index: i32::MIN,
            committed_offset: i64::MIN,
            committed_leader_epoch: if version >= 5 { i32::MIN } else { -1i32 },
            metadata: Some(KafkaString::from("boundary".to_owned())),
            error_code: i16::MIN,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields(version: i16) -> Self {
        Self {
            partition_index: 12345_i32,
            committed_offset: 9_876_543_210_i64,
            committed_leader_epoch: if version >= 5 { 12345_i32 } else { -1i32 },
            metadata: Some(KafkaString::from("test".to_owned())),
            error_code: 42_i16,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for OffsetFetchResponseGroup {
    fn test_populated(version: i16) -> Self {
        Self {
            group_id: KafkaString::from("test".to_owned()),
            topics: vec![<OffsetFetchResponseTopics as TestInstance>::test_populated(
                version,
            )],
            error_code: 42_i16,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals(version: i16) -> Self {
        drop(Self::default());
        Self {
            group_id: KafkaString::default(),
            topics: vec![<OffsetFetchResponseTopics as TestInstance>::test_null_optionals(version)],
            error_code: 0_i16,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections(_version: i16) -> Self {
        Self {
            group_id: KafkaString::default(),
            topics: Vec::new(),
            error_code: 0_i16,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections(version: i16) -> Self {
        Self {
            group_id: KafkaString::from("test-2".to_owned()),
            topics: vec![
                <OffsetFetchResponseTopics as TestInstance>::test_populated(version),
                <OffsetFetchResponseTopics as TestInstance>::test_multi_element_collections(
                    version,
                ),
            ],
            error_code: 43_i16,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries(version: i16) -> Self {
        Self {
            group_id: KafkaString::from("boundary".to_owned()),
            topics: vec![
                <OffsetFetchResponseTopics as TestInstance>::test_numeric_boundaries(version),
            ],
            error_code: i16::MIN,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields(version: i16) -> Self {
        Self {
            group_id: KafkaString::from("test".to_owned()),
            topics: vec![<OffsetFetchResponseTopics as TestInstance>::test_tagged_fields(version)],
            error_code: 42_i16,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for OffsetFetchResponseTopics {
    fn test_populated(version: i16) -> Self {
        Self {
            name: if version <= 9 {
                KafkaString::from("test".to_owned())
            } else {
                KafkaString::default()
            },
            topic_id: if version >= 10 {
                KafkaUuid::ONE
            } else {
                KafkaUuid::ZERO
            },
            partitions: vec![
                <OffsetFetchResponsePartitions as TestInstance>::test_populated(version),
            ],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals(version: i16) -> Self {
        drop(Self::default());
        Self {
            name: KafkaString::default(),
            topic_id: KafkaUuid::ZERO,
            partitions: vec![
                <OffsetFetchResponsePartitions as TestInstance>::test_null_optionals(version),
            ],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections(_version: i16) -> Self {
        Self {
            name: KafkaString::default(),
            topic_id: KafkaUuid::ZERO,
            partitions: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections(version: i16) -> Self {
        Self {
            name: if version <= 9 {
                KafkaString::from("test-2".to_owned())
            } else {
                KafkaString::default()
            },
            topic_id: if version >= 10 {
                KafkaUuid::from_parts(2, 3)
            } else {
                KafkaUuid::ZERO
            },
            partitions: vec![
                <OffsetFetchResponsePartitions as TestInstance>::test_populated(version),
                <OffsetFetchResponsePartitions as TestInstance>::test_multi_element_collections(
                    version,
                ),
            ],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries(version: i16) -> Self {
        Self {
            name: if version <= 9 {
                KafkaString::from("boundary".to_owned())
            } else {
                KafkaString::default()
            },
            topic_id: if version >= 10 {
                KafkaUuid::ONE
            } else {
                KafkaUuid::ZERO
            },
            partitions: vec![
                <OffsetFetchResponsePartitions as TestInstance>::test_numeric_boundaries(version),
            ],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields(version: i16) -> Self {
        Self {
            name: if version <= 9 {
                KafkaString::from("test".to_owned())
            } else {
                KafkaString::default()
            },
            topic_id: if version >= 10 {
                KafkaUuid::ONE
            } else {
                KafkaUuid::ZERO
            },
            partitions: vec![
                <OffsetFetchResponsePartitions as TestInstance>::test_tagged_fields(version),
            ],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for OffsetFetchResponsePartitions {
    fn test_populated(_version: i16) -> Self {
        Self {
            partition_index: 12345_i32,
            committed_offset: 9_876_543_210_i64,
            committed_leader_epoch: 12345_i32,
            metadata: Some(KafkaString::from("test".to_owned())),
            error_code: 42_i16,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals(_version: i16) -> Self {
        drop(Self::default());
        Self {
            partition_index: 0_i32,
            committed_offset: 0_i64,
            committed_leader_epoch: 0_i32,
            metadata: None,
            error_code: 0_i16,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections(_version: i16) -> Self {
        Self {
            partition_index: 0_i32,
            committed_offset: 0_i64,
            committed_leader_epoch: 0_i32,
            metadata: Some(KafkaString::default()),
            error_code: 0_i16,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections(_version: i16) -> Self {
        Self {
            partition_index: 23456_i32,
            committed_offset: 9_876_543_211_i64,
            committed_leader_epoch: 23456_i32,
            metadata: Some(KafkaString::from("test-2".to_owned())),
            error_code: 43_i16,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries(_version: i16) -> Self {
        Self {
            partition_index: i32::MIN,
            committed_offset: i64::MIN,
            committed_leader_epoch: i32::MIN,
            metadata: Some(KafkaString::from("boundary".to_owned())),
            error_code: i16::MIN,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields(_version: i16) -> Self {
        Self {
            partition_index: 12345_i32,
            committed_offset: 9_876_543_210_i64,
            committed_leader_epoch: 12345_i32,
            metadata: Some(KafkaString::from("test".to_owned())),
            error_code: 42_i16,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
fn encode_populated(version: i16) -> crate::MatrixResult<String> {
    let message = <OffsetFetchResponseData as TestInstance>::test_populated(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_populated(version: i16) -> crate::MatrixResult<usize> {
    let message = <OffsetFetchResponseData as TestInstance>::test_populated(version);
    Ok(message.encoded_len(version)?)
}
fn encode_null_optionals(version: i16) -> crate::MatrixResult<String> {
    let message = <OffsetFetchResponseData as TestInstance>::test_null_optionals(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_null_optionals(version: i16) -> crate::MatrixResult<usize> {
    let message = <OffsetFetchResponseData as TestInstance>::test_null_optionals(version);
    Ok(message.encoded_len(version)?)
}
fn encode_empty_collections(version: i16) -> crate::MatrixResult<String> {
    let message = <OffsetFetchResponseData as TestInstance>::test_empty_collections(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_empty_collections(version: i16) -> crate::MatrixResult<usize> {
    let message = <OffsetFetchResponseData as TestInstance>::test_empty_collections(version);
    Ok(message.encoded_len(version)?)
}
fn encode_multi_element_collections(version: i16) -> crate::MatrixResult<String> {
    let message =
        <OffsetFetchResponseData as TestInstance>::test_multi_element_collections(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_multi_element_collections(version: i16) -> crate::MatrixResult<usize> {
    let message =
        <OffsetFetchResponseData as TestInstance>::test_multi_element_collections(version);
    Ok(message.encoded_len(version)?)
}
fn encode_numeric_boundaries(version: i16) -> crate::MatrixResult<String> {
    let message = <OffsetFetchResponseData as TestInstance>::test_numeric_boundaries(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_numeric_boundaries(version: i16) -> crate::MatrixResult<usize> {
    let message = <OffsetFetchResponseData as TestInstance>::test_numeric_boundaries(version);
    Ok(message.encoded_len(version)?)
}
fn encode_tagged_fields(version: i16) -> crate::MatrixResult<String> {
    let message = <OffsetFetchResponseData as TestInstance>::test_tagged_fields(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_tagged_fields(version: i16) -> crate::MatrixResult<usize> {
    let message = <OffsetFetchResponseData as TestInstance>::test_tagged_fields(version);
    Ok(message.encoded_len(version)?)
}
fn reencode(version: i16, hex_input: &str) -> crate::MatrixResult<String> {
    let mut input = Bytes::from(crate::decode_hex(hex_input)?);
    let message = OffsetFetchResponseData::read(&mut input, version)?;
    crate::ensure_input_consumed(&input)?;
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
const MATRIX_CASES: &[crate::MatrixCase] = &[
    crate::MatrixCase {
        schema_name: "OffsetFetchResponse",
        java_class: "org.apache.kafka.common.message.OffsetFetchResponseData",
        version: 1i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchResponse",
        java_class: "org.apache.kafka.common.message.OffsetFetchResponseData",
        version: 2i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchResponse",
        java_class: "org.apache.kafka.common.message.OffsetFetchResponseData",
        version: 3i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchResponse",
        java_class: "org.apache.kafka.common.message.OffsetFetchResponseData",
        version: 4i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchResponse",
        java_class: "org.apache.kafka.common.message.OffsetFetchResponseData",
        version: 5i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchResponse",
        java_class: "org.apache.kafka.common.message.OffsetFetchResponseData",
        version: 6i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchResponse",
        java_class: "org.apache.kafka.common.message.OffsetFetchResponseData",
        version: 7i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchResponse",
        java_class: "org.apache.kafka.common.message.OffsetFetchResponseData",
        version: 8i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchResponse",
        java_class: "org.apache.kafka.common.message.OffsetFetchResponseData",
        version: 9i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchResponse",
        java_class: "org.apache.kafka.common.message.OffsetFetchResponseData",
        version: 10i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchResponse",
        java_class: "org.apache.kafka.common.message.OffsetFetchResponseData",
        version: 1i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchResponse",
        java_class: "org.apache.kafka.common.message.OffsetFetchResponseData",
        version: 2i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchResponse",
        java_class: "org.apache.kafka.common.message.OffsetFetchResponseData",
        version: 3i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchResponse",
        java_class: "org.apache.kafka.common.message.OffsetFetchResponseData",
        version: 4i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchResponse",
        java_class: "org.apache.kafka.common.message.OffsetFetchResponseData",
        version: 5i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchResponse",
        java_class: "org.apache.kafka.common.message.OffsetFetchResponseData",
        version: 6i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchResponse",
        java_class: "org.apache.kafka.common.message.OffsetFetchResponseData",
        version: 7i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchResponse",
        java_class: "org.apache.kafka.common.message.OffsetFetchResponseData",
        version: 8i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchResponse",
        java_class: "org.apache.kafka.common.message.OffsetFetchResponseData",
        version: 9i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchResponse",
        java_class: "org.apache.kafka.common.message.OffsetFetchResponseData",
        version: 10i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchResponse",
        java_class: "org.apache.kafka.common.message.OffsetFetchResponseData",
        version: 1i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchResponse",
        java_class: "org.apache.kafka.common.message.OffsetFetchResponseData",
        version: 1i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchResponse",
        java_class: "org.apache.kafka.common.message.OffsetFetchResponseData",
        version: 1i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchResponse",
        java_class: "org.apache.kafka.common.message.OffsetFetchResponseData",
        version: 1i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchResponse",
        java_class: "org.apache.kafka.common.message.OffsetFetchResponseData",
        version: 2i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchResponse",
        java_class: "org.apache.kafka.common.message.OffsetFetchResponseData",
        version: 2i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchResponse",
        java_class: "org.apache.kafka.common.message.OffsetFetchResponseData",
        version: 2i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchResponse",
        java_class: "org.apache.kafka.common.message.OffsetFetchResponseData",
        version: 2i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchResponse",
        java_class: "org.apache.kafka.common.message.OffsetFetchResponseData",
        version: 3i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchResponse",
        java_class: "org.apache.kafka.common.message.OffsetFetchResponseData",
        version: 3i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchResponse",
        java_class: "org.apache.kafka.common.message.OffsetFetchResponseData",
        version: 3i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchResponse",
        java_class: "org.apache.kafka.common.message.OffsetFetchResponseData",
        version: 3i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchResponse",
        java_class: "org.apache.kafka.common.message.OffsetFetchResponseData",
        version: 4i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchResponse",
        java_class: "org.apache.kafka.common.message.OffsetFetchResponseData",
        version: 4i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchResponse",
        java_class: "org.apache.kafka.common.message.OffsetFetchResponseData",
        version: 4i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchResponse",
        java_class: "org.apache.kafka.common.message.OffsetFetchResponseData",
        version: 4i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchResponse",
        java_class: "org.apache.kafka.common.message.OffsetFetchResponseData",
        version: 5i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchResponse",
        java_class: "org.apache.kafka.common.message.OffsetFetchResponseData",
        version: 5i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchResponse",
        java_class: "org.apache.kafka.common.message.OffsetFetchResponseData",
        version: 5i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchResponse",
        java_class: "org.apache.kafka.common.message.OffsetFetchResponseData",
        version: 5i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchResponse",
        java_class: "org.apache.kafka.common.message.OffsetFetchResponseData",
        version: 6i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchResponse",
        java_class: "org.apache.kafka.common.message.OffsetFetchResponseData",
        version: 6i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchResponse",
        java_class: "org.apache.kafka.common.message.OffsetFetchResponseData",
        version: 6i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchResponse",
        java_class: "org.apache.kafka.common.message.OffsetFetchResponseData",
        version: 6i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchResponse",
        java_class: "org.apache.kafka.common.message.OffsetFetchResponseData",
        version: 7i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchResponse",
        java_class: "org.apache.kafka.common.message.OffsetFetchResponseData",
        version: 7i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchResponse",
        java_class: "org.apache.kafka.common.message.OffsetFetchResponseData",
        version: 7i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchResponse",
        java_class: "org.apache.kafka.common.message.OffsetFetchResponseData",
        version: 7i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchResponse",
        java_class: "org.apache.kafka.common.message.OffsetFetchResponseData",
        version: 8i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchResponse",
        java_class: "org.apache.kafka.common.message.OffsetFetchResponseData",
        version: 8i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchResponse",
        java_class: "org.apache.kafka.common.message.OffsetFetchResponseData",
        version: 8i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchResponse",
        java_class: "org.apache.kafka.common.message.OffsetFetchResponseData",
        version: 8i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchResponse",
        java_class: "org.apache.kafka.common.message.OffsetFetchResponseData",
        version: 9i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchResponse",
        java_class: "org.apache.kafka.common.message.OffsetFetchResponseData",
        version: 9i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchResponse",
        java_class: "org.apache.kafka.common.message.OffsetFetchResponseData",
        version: 9i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchResponse",
        java_class: "org.apache.kafka.common.message.OffsetFetchResponseData",
        version: 9i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchResponse",
        java_class: "org.apache.kafka.common.message.OffsetFetchResponseData",
        version: 10i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchResponse",
        java_class: "org.apache.kafka.common.message.OffsetFetchResponseData",
        version: 10i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchResponse",
        java_class: "org.apache.kafka.common.message.OffsetFetchResponseData",
        version: 10i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchResponse",
        java_class: "org.apache.kafka.common.message.OffsetFetchResponseData",
        version: 10i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
];
pub(crate) fn append_protocol_cases(cases: &mut Vec<crate::MatrixCase>) {
    cases.extend_from_slice(MATRIX_CASES);
}
