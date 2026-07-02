#![allow(
    clippy::all,
    clippy::pedantic,
    clippy::nursery,
    reason = "Generated test fixtures mirror Kafka's schema shape and trade hand-written lint \
              style for reproducible output, matching the generated protocol modules."
)]
use bytes::{Bytes, BytesMut};
use kacrab_protocol::{generated::fetch_request::*, *};

use crate::TestInstance;

impl TestInstance for FetchRequestData {
    fn test_populated(version: i16) -> Self {
        Self {
            cluster_id: (version >= 12)
                .then(|| Some(KafkaString::from("test".to_owned())))
                .flatten(),
            replica_id: if version <= 14 { 12345_i32 } else { -1i32 },
            replica_state: if version >= 15 {
                <ReplicaState as TestInstance>::test_populated(version)
            } else {
                ReplicaState::default()
            },
            max_wait_ms: 12345_i32,
            min_bytes: 12345_i32,
            max_bytes: 12345_i32,
            isolation_level: 7_i8,
            session_id: if version >= 7 { 12345_i32 } else { 0i32 },
            session_epoch: if version >= 7 { 12345_i32 } else { -1i32 },
            topics: vec![<FetchTopic as TestInstance>::test_populated(version)],
            forgotten_topics_data: if version >= 7 {
                vec![<ForgottenTopic as TestInstance>::test_populated(version)]
            } else {
                Vec::new()
            },
            rack_id: if version >= 11 {
                KafkaString::from("test".to_owned())
            } else {
                KafkaString::default()
            },
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals(version: i16) -> Self {
        drop(<ReplicaState as TestInstance>::test_null_optionals(version));
        Self {
            cluster_id: None,
            replica_id: if version <= 14 { 0_i32 } else { -1i32 },
            replica_state: ReplicaState::default(),
            max_wait_ms: 0_i32,
            min_bytes: 0_i32,
            max_bytes: 0_i32,
            isolation_level: 0_i8,
            session_id: if version >= 7 { 0_i32 } else { 0i32 },
            session_epoch: if version >= 7 { 0_i32 } else { -1i32 },
            topics: vec![<FetchTopic as TestInstance>::test_null_optionals(version)],
            forgotten_topics_data: if version >= 7 {
                vec![<ForgottenTopic as TestInstance>::test_null_optionals(
                    version,
                )]
            } else {
                Vec::new()
            },
            rack_id: KafkaString::default(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections(version: i16) -> Self {
        Self {
            cluster_id: (version >= 12)
                .then(|| Some(KafkaString::default()))
                .flatten(),
            replica_id: if version <= 14 { 0_i32 } else { -1i32 },
            replica_state: if version >= 15 {
                <ReplicaState as TestInstance>::test_null_optionals(version)
            } else {
                ReplicaState::default()
            },
            max_wait_ms: 0_i32,
            min_bytes: 0_i32,
            max_bytes: 0_i32,
            isolation_level: 0_i8,
            session_id: if version >= 7 { 0_i32 } else { 0i32 },
            session_epoch: if version >= 7 { 0_i32 } else { -1i32 },
            topics: Vec::new(),
            forgotten_topics_data: Vec::new(),
            rack_id: KafkaString::default(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections(version: i16) -> Self {
        Self {
            cluster_id: (version >= 12)
                .then(|| Some(KafkaString::from("test-2".to_owned())))
                .flatten(),
            replica_id: if version <= 14 { 23456_i32 } else { -1i32 },
            replica_state: if version >= 15 {
                <ReplicaState as TestInstance>::test_multi_element_collections(version)
            } else {
                ReplicaState::default()
            },
            max_wait_ms: 23456_i32,
            min_bytes: 23456_i32,
            max_bytes: 23456_i32,
            isolation_level: 8_i8,
            session_id: if version >= 7 { 23456_i32 } else { 0i32 },
            session_epoch: if version >= 7 { 23456_i32 } else { -1i32 },
            topics: vec![
                <FetchTopic as TestInstance>::test_populated(version),
                <FetchTopic as TestInstance>::test_multi_element_collections(version),
            ],
            forgotten_topics_data: if version >= 7 {
                vec![
                    <ForgottenTopic as TestInstance>::test_populated(version),
                    <ForgottenTopic as TestInstance>::test_multi_element_collections(version),
                ]
            } else {
                Vec::new()
            },
            rack_id: if version >= 11 {
                KafkaString::from("test-2".to_owned())
            } else {
                KafkaString::default()
            },
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries(version: i16) -> Self {
        Self {
            cluster_id: (version >= 12)
                .then(|| Some(KafkaString::from("boundary".to_owned())))
                .flatten(),
            replica_id: if version <= 14 { i32::MIN } else { -1i32 },
            replica_state: if version >= 15 {
                <ReplicaState as TestInstance>::test_numeric_boundaries(version)
            } else {
                ReplicaState::default()
            },
            max_wait_ms: i32::MIN,
            min_bytes: i32::MIN,
            max_bytes: i32::MIN,
            isolation_level: i8::MIN,
            session_id: if version >= 7 { i32::MIN } else { 0i32 },
            session_epoch: if version >= 7 { i32::MIN } else { -1i32 },
            topics: vec![<FetchTopic as TestInstance>::test_numeric_boundaries(
                version,
            )],
            forgotten_topics_data: if version >= 7 {
                vec![<ForgottenTopic as TestInstance>::test_numeric_boundaries(
                    version,
                )]
            } else {
                Vec::new()
            },
            rack_id: if version >= 11 {
                KafkaString::from("boundary".to_owned())
            } else {
                KafkaString::default()
            },
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields(version: i16) -> Self {
        Self {
            cluster_id: (version >= 12)
                .then(|| Some(KafkaString::from("test".to_owned())))
                .flatten(),
            replica_id: if version <= 14 { 12345_i32 } else { -1i32 },
            replica_state: if version >= 15 {
                <ReplicaState as TestInstance>::test_tagged_fields(version)
            } else {
                ReplicaState::default()
            },
            max_wait_ms: 12345_i32,
            min_bytes: 12345_i32,
            max_bytes: 12345_i32,
            isolation_level: 7_i8,
            session_id: if version >= 7 { 12345_i32 } else { 0i32 },
            session_epoch: if version >= 7 { 12345_i32 } else { -1i32 },
            topics: vec![<FetchTopic as TestInstance>::test_tagged_fields(version)],
            forgotten_topics_data: if version >= 7 {
                vec![<ForgottenTopic as TestInstance>::test_tagged_fields(
                    version,
                )]
            } else {
                Vec::new()
            },
            rack_id: if version >= 11 {
                KafkaString::from("test".to_owned())
            } else {
                KafkaString::default()
            },
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for ReplicaState {
    fn test_populated(_version: i16) -> Self {
        Self {
            replica_id: 12345_i32,
            replica_epoch: 9_876_543_210_i64,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals(_version: i16) -> Self {
        drop(Self::default());
        Self {
            replica_id: 0_i32,
            replica_epoch: 0_i64,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections(_version: i16) -> Self {
        Self {
            replica_id: 0_i32,
            replica_epoch: 0_i64,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections(_version: i16) -> Self {
        Self {
            replica_id: 23456_i32,
            replica_epoch: 9_876_543_211_i64,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries(_version: i16) -> Self {
        Self {
            replica_id: i32::MIN,
            replica_epoch: i64::MIN,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields(_version: i16) -> Self {
        Self {
            replica_id: 12345_i32,
            replica_epoch: 9_876_543_210_i64,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for FetchTopic {
    fn test_populated(version: i16) -> Self {
        Self {
            topic: if version <= 12 {
                KafkaString::from("test".to_owned())
            } else {
                KafkaString::default()
            },
            topic_id: if version >= 13 {
                KafkaUuid::ONE
            } else {
                KafkaUuid::ZERO
            },
            partitions: vec![<FetchPartition as TestInstance>::test_populated(version)],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals(version: i16) -> Self {
        drop(Self::default());
        Self {
            topic: KafkaString::default(),
            topic_id: KafkaUuid::ZERO,
            partitions: vec![<FetchPartition as TestInstance>::test_null_optionals(
                version,
            )],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections(_version: i16) -> Self {
        Self {
            topic: KafkaString::default(),
            topic_id: KafkaUuid::ZERO,
            partitions: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections(version: i16) -> Self {
        Self {
            topic: if version <= 12 {
                KafkaString::from("test-2".to_owned())
            } else {
                KafkaString::default()
            },
            topic_id: if version >= 13 {
                KafkaUuid::from_parts(2, 3)
            } else {
                KafkaUuid::ZERO
            },
            partitions: vec![
                <FetchPartition as TestInstance>::test_populated(version),
                <FetchPartition as TestInstance>::test_multi_element_collections(version),
            ],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries(version: i16) -> Self {
        Self {
            topic: if version <= 12 {
                KafkaString::from("boundary".to_owned())
            } else {
                KafkaString::default()
            },
            topic_id: if version >= 13 {
                KafkaUuid::ONE
            } else {
                KafkaUuid::ZERO
            },
            partitions: vec![<FetchPartition as TestInstance>::test_numeric_boundaries(
                version,
            )],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields(version: i16) -> Self {
        Self {
            topic: if version <= 12 {
                KafkaString::from("test".to_owned())
            } else {
                KafkaString::default()
            },
            topic_id: if version >= 13 {
                KafkaUuid::ONE
            } else {
                KafkaUuid::ZERO
            },
            partitions: vec![<FetchPartition as TestInstance>::test_tagged_fields(
                version,
            )],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for FetchPartition {
    fn test_populated(version: i16) -> Self {
        Self {
            partition: 12345_i32,
            current_leader_epoch: if version >= 9 { 12345_i32 } else { -1i32 },
            fetch_offset: 9_876_543_210_i64,
            last_fetched_epoch: if version >= 12 { 12345_i32 } else { -1i32 },
            log_start_offset: if version >= 5 {
                9_876_543_210_i64
            } else {
                -1i64
            },
            partition_max_bytes: 12345_i32,
            replica_directory_id: if version >= 17 {
                KafkaUuid::ONE
            } else {
                KafkaUuid::ZERO
            },
            high_watermark: if version >= 18 {
                9_876_543_210_i64
            } else {
                i64::MAX
            },
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals(version: i16) -> Self {
        drop(Self::default());
        Self {
            partition: 0_i32,
            current_leader_epoch: if version >= 9 { 0_i32 } else { -1i32 },
            fetch_offset: 0_i64,
            last_fetched_epoch: if version >= 12 { 0_i32 } else { -1i32 },
            log_start_offset: if version >= 5 { 0_i64 } else { -1i64 },
            partition_max_bytes: 0_i32,
            replica_directory_id: KafkaUuid::ZERO,
            high_watermark: i64::MAX,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections(version: i16) -> Self {
        Self {
            partition: 0_i32,
            current_leader_epoch: if version >= 9 { 0_i32 } else { -1i32 },
            fetch_offset: 0_i64,
            last_fetched_epoch: if version >= 12 { 0_i32 } else { -1i32 },
            log_start_offset: if version >= 5 { 0_i64 } else { -1i64 },
            partition_max_bytes: 0_i32,
            replica_directory_id: KafkaUuid::ZERO,
            high_watermark: if version >= 18 { 0_i64 } else { i64::MAX },
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections(version: i16) -> Self {
        Self {
            partition: 23456_i32,
            current_leader_epoch: if version >= 9 { 23456_i32 } else { -1i32 },
            fetch_offset: 9_876_543_211_i64,
            last_fetched_epoch: if version >= 12 { 23456_i32 } else { -1i32 },
            log_start_offset: if version >= 5 {
                9_876_543_211_i64
            } else {
                -1i64
            },
            partition_max_bytes: 23456_i32,
            replica_directory_id: if version >= 17 {
                KafkaUuid::from_parts(2, 3)
            } else {
                KafkaUuid::ZERO
            },
            high_watermark: if version >= 18 {
                9_876_543_211_i64
            } else {
                i64::MAX
            },
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries(version: i16) -> Self {
        Self {
            partition: i32::MIN,
            current_leader_epoch: if version >= 9 { i32::MIN } else { -1i32 },
            fetch_offset: i64::MIN,
            last_fetched_epoch: if version >= 12 { i32::MIN } else { -1i32 },
            log_start_offset: if version >= 5 { i64::MIN } else { -1i64 },
            partition_max_bytes: i32::MIN,
            replica_directory_id: if version >= 17 {
                KafkaUuid::ONE
            } else {
                KafkaUuid::ZERO
            },
            high_watermark: if version >= 18 { i64::MIN } else { i64::MAX },
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields(version: i16) -> Self {
        Self {
            partition: 12345_i32,
            current_leader_epoch: if version >= 9 { 12345_i32 } else { -1i32 },
            fetch_offset: 9_876_543_210_i64,
            last_fetched_epoch: if version >= 12 { 12345_i32 } else { -1i32 },
            log_start_offset: if version >= 5 {
                9_876_543_210_i64
            } else {
                -1i64
            },
            partition_max_bytes: 12345_i32,
            replica_directory_id: if version >= 17 {
                KafkaUuid::ONE
            } else {
                KafkaUuid::ZERO
            },
            high_watermark: if version >= 18 {
                9_876_543_210_i64
            } else {
                i64::MAX
            },
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for ForgottenTopic {
    fn test_populated(version: i16) -> Self {
        Self {
            topic: if version <= 12 {
                KafkaString::from("test".to_owned())
            } else {
                KafkaString::default()
            },
            topic_id: if version >= 13 {
                KafkaUuid::ONE
            } else {
                KafkaUuid::ZERO
            },
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
            topic: KafkaString::default(),
            topic_id: KafkaUuid::ZERO,
            partitions: vec![0_i32],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections(_version: i16) -> Self {
        Self {
            topic: KafkaString::default(),
            topic_id: KafkaUuid::ZERO,
            partitions: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections(version: i16) -> Self {
        Self {
            topic: if version <= 12 {
                KafkaString::from("test-2".to_owned())
            } else {
                KafkaString::default()
            },
            topic_id: if version >= 13 {
                KafkaUuid::from_parts(2, 3)
            } else {
                KafkaUuid::ZERO
            },
            partitions: vec![12345_i32, 23456_i32],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries(version: i16) -> Self {
        Self {
            topic: if version <= 12 {
                KafkaString::from("boundary".to_owned())
            } else {
                KafkaString::default()
            },
            topic_id: if version >= 13 {
                KafkaUuid::ONE
            } else {
                KafkaUuid::ZERO
            },
            partitions: vec![i32::MIN],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields(version: i16) -> Self {
        Self {
            topic: if version <= 12 {
                KafkaString::from("test".to_owned())
            } else {
                KafkaString::default()
            },
            topic_id: if version >= 13 {
                KafkaUuid::ONE
            } else {
                KafkaUuid::ZERO
            },
            partitions: vec![12345_i32],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
fn encode_populated(version: i16) -> crate::MatrixResult<String> {
    let message = <FetchRequestData as TestInstance>::test_populated(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_populated(version: i16) -> crate::MatrixResult<usize> {
    let message = <FetchRequestData as TestInstance>::test_populated(version);
    Ok(message.encoded_len(version)?)
}
fn encode_null_optionals(version: i16) -> crate::MatrixResult<String> {
    let message = <FetchRequestData as TestInstance>::test_null_optionals(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_null_optionals(version: i16) -> crate::MatrixResult<usize> {
    let message = <FetchRequestData as TestInstance>::test_null_optionals(version);
    Ok(message.encoded_len(version)?)
}
fn encode_empty_collections(version: i16) -> crate::MatrixResult<String> {
    let message = <FetchRequestData as TestInstance>::test_empty_collections(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_empty_collections(version: i16) -> crate::MatrixResult<usize> {
    let message = <FetchRequestData as TestInstance>::test_empty_collections(version);
    Ok(message.encoded_len(version)?)
}
fn encode_multi_element_collections(version: i16) -> crate::MatrixResult<String> {
    let message = <FetchRequestData as TestInstance>::test_multi_element_collections(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_multi_element_collections(version: i16) -> crate::MatrixResult<usize> {
    let message = <FetchRequestData as TestInstance>::test_multi_element_collections(version);
    Ok(message.encoded_len(version)?)
}
fn encode_numeric_boundaries(version: i16) -> crate::MatrixResult<String> {
    let message = <FetchRequestData as TestInstance>::test_numeric_boundaries(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_numeric_boundaries(version: i16) -> crate::MatrixResult<usize> {
    let message = <FetchRequestData as TestInstance>::test_numeric_boundaries(version);
    Ok(message.encoded_len(version)?)
}
fn encode_tagged_fields(version: i16) -> crate::MatrixResult<String> {
    let message = <FetchRequestData as TestInstance>::test_tagged_fields(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_tagged_fields(version: i16) -> crate::MatrixResult<usize> {
    let message = <FetchRequestData as TestInstance>::test_tagged_fields(version);
    Ok(message.encoded_len(version)?)
}
fn reencode(version: i16, hex_input: &str) -> crate::MatrixResult<String> {
    let mut input = Bytes::from(crate::decode_hex(hex_input)?);
    let message = FetchRequestData::read(&mut input, version)?;
    crate::ensure_input_consumed(&input)?;
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
const MATRIX_CASES: &[crate::MatrixCase] = &[
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 4i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 5i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 6i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 7i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 8i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 9i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 10i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 11i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 12i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 13i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 14i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 15i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 16i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 17i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 18i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 4i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 5i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 6i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 7i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 8i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 9i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 10i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 11i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 12i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 13i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 14i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 15i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 16i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 17i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 18i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 4i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 4i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 4i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 4i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 5i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 5i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 5i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 5i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 6i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 6i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 6i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 6i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 7i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 7i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 7i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 7i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 8i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 8i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 8i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 8i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 9i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 9i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 9i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 9i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 10i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 10i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 10i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 10i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 11i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 11i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 11i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 11i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 12i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 12i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 12i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 12i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 13i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 13i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 13i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 13i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 14i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 14i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 14i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 14i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 15i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 15i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 15i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 15i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 16i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 16i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 16i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 16i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 17i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 17i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 17i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 17i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 18i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 18i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 18i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchRequest",
        java_class: "org.apache.kafka.common.message.FetchRequestData",
        version: 18i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
];
pub(crate) fn append_protocol_cases(cases: &mut Vec<crate::MatrixCase>) {
    cases.extend_from_slice(MATRIX_CASES);
}
