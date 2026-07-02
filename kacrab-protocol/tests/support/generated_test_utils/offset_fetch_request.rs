#![allow(
    clippy::all,
    clippy::pedantic,
    clippy::nursery,
    reason = "Generated test fixtures mirror Kafka's schema shape and trade hand-written lint \
              style for reproducible output, matching the generated protocol modules."
)]
use bytes::{Bytes, BytesMut};
use kacrab_protocol::{generated::offset_fetch_request::*, *};

use crate::TestInstance;

impl TestInstance for OffsetFetchRequestData {
    fn test_populated(version: i16) -> Self {
        Self {
            group_id: if version <= 7 {
                KafkaString::from("test".to_owned())
            } else {
                KafkaString::default()
            },
            topics: (version <= 7)
                .then(|| {
                    Some(vec![
                        <OffsetFetchRequestTopic as TestInstance>::test_populated(version),
                    ])
                })
                .flatten(),
            groups: if version >= 8 {
                vec![<OffsetFetchRequestGroup as TestInstance>::test_populated(
                    version,
                )]
            } else {
                Vec::new()
            },
            require_stable: if version >= 7 { true } else { false },
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals(version: i16) -> Self {
        drop(<OffsetFetchRequestTopic as TestInstance>::test_null_optionals(version));
        Self {
            group_id: KafkaString::default(),
            topics: None,
            groups: if version >= 8 {
                vec![<OffsetFetchRequestGroup as TestInstance>::test_null_optionals(version)]
            } else {
                Vec::new()
            },
            require_stable: false,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections(version: i16) -> Self {
        Self {
            group_id: KafkaString::default(),
            topics: (version <= 7).then(|| Some(Vec::new())).flatten(),
            groups: Vec::new(),
            require_stable: false,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections(version: i16) -> Self {
        Self {
            group_id: if version <= 7 {
                KafkaString::from("test-2".to_owned())
            } else {
                KafkaString::default()
            },
            topics: (version <= 7)
                .then(|| {
                    Some(vec![
                        <OffsetFetchRequestTopic as TestInstance>::test_populated(version),
                        <OffsetFetchRequestTopic as TestInstance>::test_multi_element_collections(
                            version,
                        ),
                    ])
                })
                .flatten(),
            groups: if version >= 8 {
                vec![
                    <OffsetFetchRequestGroup as TestInstance>::test_populated(version),
                    <OffsetFetchRequestGroup as TestInstance>::test_multi_element_collections(
                        version,
                    ),
                ]
            } else {
                Vec::new()
            },
            require_stable: false,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries(version: i16) -> Self {
        Self {
            group_id: if version <= 7 {
                KafkaString::from("boundary".to_owned())
            } else {
                KafkaString::default()
            },
            topics: (version <= 7)
                .then(|| {
                    Some(vec![
                        <OffsetFetchRequestTopic as TestInstance>::test_numeric_boundaries(version),
                    ])
                })
                .flatten(),
            groups: if version >= 8 {
                vec![<OffsetFetchRequestGroup as TestInstance>::test_numeric_boundaries(version)]
            } else {
                Vec::new()
            },
            require_stable: if version >= 7 { true } else { false },
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields(version: i16) -> Self {
        Self {
            group_id: if version <= 7 {
                KafkaString::from("test".to_owned())
            } else {
                KafkaString::default()
            },
            topics: (version <= 7)
                .then(|| {
                    Some(vec![
                        <OffsetFetchRequestTopic as TestInstance>::test_tagged_fields(version),
                    ])
                })
                .flatten(),
            groups: if version >= 8 {
                vec![<OffsetFetchRequestGroup as TestInstance>::test_tagged_fields(version)]
            } else {
                Vec::new()
            },
            require_stable: if version >= 7 { true } else { false },
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for OffsetFetchRequestTopic {
    fn test_populated(_version: i16) -> Self {
        Self {
            name: KafkaString::from("test".to_owned()),
            partition_indexes: vec![12345_i32],
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
            partition_indexes: vec![0_i32],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections(_version: i16) -> Self {
        Self {
            name: KafkaString::default(),
            partition_indexes: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections(_version: i16) -> Self {
        Self {
            name: KafkaString::from("test-2".to_owned()),
            partition_indexes: vec![12345_i32, 23456_i32],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries(_version: i16) -> Self {
        Self {
            name: KafkaString::from("boundary".to_owned()),
            partition_indexes: vec![i32::MIN],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields(_version: i16) -> Self {
        Self {
            name: KafkaString::from("test".to_owned()),
            partition_indexes: vec![12345_i32],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for OffsetFetchRequestGroup {
    fn test_populated(version: i16) -> Self {
        Self {
            group_id: KafkaString::from("test".to_owned()),
            member_id: (version >= 9)
                .then(|| Some(KafkaString::from("test".to_owned())))
                .flatten(),
            member_epoch: if version >= 9 { 12345_i32 } else { -1i32 },
            topics: Some(vec![
                <OffsetFetchRequestTopics as TestInstance>::test_populated(version),
            ]),
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals(version: i16) -> Self {
        drop(Self::default());
        drop(<OffsetFetchRequestTopics as TestInstance>::test_null_optionals(version));
        Self {
            group_id: KafkaString::default(),
            member_id: None,
            member_epoch: if version >= 9 { 0_i32 } else { -1i32 },
            topics: None,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections(version: i16) -> Self {
        Self {
            group_id: KafkaString::default(),
            member_id: (version >= 9)
                .then(|| Some(KafkaString::default()))
                .flatten(),
            member_epoch: if version >= 9 { 0_i32 } else { -1i32 },
            topics: Some(Vec::new()),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections(version: i16) -> Self {
        Self {
            group_id: KafkaString::from("test-2".to_owned()),
            member_id: (version >= 9)
                .then(|| Some(KafkaString::from("test-2".to_owned())))
                .flatten(),
            member_epoch: if version >= 9 { 23456_i32 } else { -1i32 },
            topics: Some(vec![
                <OffsetFetchRequestTopics as TestInstance>::test_populated(version),
                <OffsetFetchRequestTopics as TestInstance>::test_multi_element_collections(version),
            ]),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries(version: i16) -> Self {
        Self {
            group_id: KafkaString::from("boundary".to_owned()),
            member_id: (version >= 9)
                .then(|| Some(KafkaString::from("boundary".to_owned())))
                .flatten(),
            member_epoch: if version >= 9 { i32::MIN } else { -1i32 },
            topics: Some(vec![
                <OffsetFetchRequestTopics as TestInstance>::test_numeric_boundaries(version),
            ]),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields(version: i16) -> Self {
        Self {
            group_id: KafkaString::from("test".to_owned()),
            member_id: (version >= 9)
                .then(|| Some(KafkaString::from("test".to_owned())))
                .flatten(),
            member_epoch: if version >= 9 { 12345_i32 } else { -1i32 },
            topics: Some(vec![
                <OffsetFetchRequestTopics as TestInstance>::test_tagged_fields(version),
            ]),
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for OffsetFetchRequestTopics {
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
            partition_indexes: vec![12345_i32],
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
            topic_id: KafkaUuid::ZERO,
            partition_indexes: vec![0_i32],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections(_version: i16) -> Self {
        Self {
            name: KafkaString::default(),
            topic_id: KafkaUuid::ZERO,
            partition_indexes: Vec::new(),
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
            partition_indexes: vec![12345_i32, 23456_i32],
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
            partition_indexes: vec![i32::MIN],
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
            partition_indexes: vec![12345_i32],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
fn encode_populated(version: i16) -> crate::MatrixResult<String> {
    let message = <OffsetFetchRequestData as TestInstance>::test_populated(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_populated(version: i16) -> crate::MatrixResult<usize> {
    let message = <OffsetFetchRequestData as TestInstance>::test_populated(version);
    Ok(message.encoded_len(version)?)
}
fn encode_null_optionals(version: i16) -> crate::MatrixResult<String> {
    let message = <OffsetFetchRequestData as TestInstance>::test_null_optionals(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_null_optionals(version: i16) -> crate::MatrixResult<usize> {
    let message = <OffsetFetchRequestData as TestInstance>::test_null_optionals(version);
    Ok(message.encoded_len(version)?)
}
fn encode_empty_collections(version: i16) -> crate::MatrixResult<String> {
    let message = <OffsetFetchRequestData as TestInstance>::test_empty_collections(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_empty_collections(version: i16) -> crate::MatrixResult<usize> {
    let message = <OffsetFetchRequestData as TestInstance>::test_empty_collections(version);
    Ok(message.encoded_len(version)?)
}
fn encode_multi_element_collections(version: i16) -> crate::MatrixResult<String> {
    let message = <OffsetFetchRequestData as TestInstance>::test_multi_element_collections(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_multi_element_collections(version: i16) -> crate::MatrixResult<usize> {
    let message = <OffsetFetchRequestData as TestInstance>::test_multi_element_collections(version);
    Ok(message.encoded_len(version)?)
}
fn encode_numeric_boundaries(version: i16) -> crate::MatrixResult<String> {
    let message = <OffsetFetchRequestData as TestInstance>::test_numeric_boundaries(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_numeric_boundaries(version: i16) -> crate::MatrixResult<usize> {
    let message = <OffsetFetchRequestData as TestInstance>::test_numeric_boundaries(version);
    Ok(message.encoded_len(version)?)
}
fn encode_tagged_fields(version: i16) -> crate::MatrixResult<String> {
    let message = <OffsetFetchRequestData as TestInstance>::test_tagged_fields(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_tagged_fields(version: i16) -> crate::MatrixResult<usize> {
    let message = <OffsetFetchRequestData as TestInstance>::test_tagged_fields(version);
    Ok(message.encoded_len(version)?)
}
fn reencode(version: i16, hex_input: &str) -> crate::MatrixResult<String> {
    let mut input = Bytes::from(crate::decode_hex(hex_input)?);
    let message = OffsetFetchRequestData::read(&mut input, version)?;
    crate::ensure_input_consumed(&input)?;
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
const MATRIX_CASES: &[crate::MatrixCase] = &[
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 1i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 2i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 3i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 4i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 5i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 6i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 7i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 8i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 9i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 10i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 1i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 2i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 3i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 4i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 5i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 6i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 7i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 8i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 9i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 10i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 1i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 1i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 1i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 1i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 2i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 2i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 2i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 2i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 3i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 3i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 3i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 3i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 4i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 4i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 4i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 4i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 5i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 5i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 5i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 5i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 6i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 6i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 6i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 6i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 7i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 7i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 7i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 7i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 8i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 8i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 8i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 8i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 9i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 9i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 9i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 9i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 10i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 10i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 10i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
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
