#![allow(
    clippy::all,
    clippy::pedantic,
    clippy::nursery,
    reason = "Generated test fixtures mirror Kafka's schema shape and trade hand-written lint \
              style for reproducible output, matching the generated protocol modules."
)]
use bytes::{Bytes, BytesMut};
use kacrab_protocol::{generated::fetch_snapshot_request::*, *};

use crate::TestInstance;

impl TestInstance for FetchSnapshotRequestData {
    fn test_populated(version: i16) -> Self {
        Self {
            cluster_id: Some(KafkaString::from("test".to_owned())),
            replica_id: 12345_i32,
            max_bytes: 12345_i32,
            topics: vec![<TopicSnapshot as TestInstance>::test_populated(version)],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals(version: i16) -> Self {
        Self {
            cluster_id: None,
            replica_id: 0_i32,
            max_bytes: 0_i32,
            topics: vec![<TopicSnapshot as TestInstance>::test_null_optionals(
                version,
            )],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections(_version: i16) -> Self {
        Self {
            cluster_id: Some(KafkaString::default()),
            replica_id: 0_i32,
            max_bytes: 0_i32,
            topics: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections(version: i16) -> Self {
        Self {
            cluster_id: Some(KafkaString::from("test-2".to_owned())),
            replica_id: 23456_i32,
            max_bytes: 23456_i32,
            topics: vec![
                <TopicSnapshot as TestInstance>::test_populated(version),
                <TopicSnapshot as TestInstance>::test_multi_element_collections(version),
            ],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries(version: i16) -> Self {
        Self {
            cluster_id: Some(KafkaString::from("boundary".to_owned())),
            replica_id: i32::MIN,
            max_bytes: i32::MIN,
            topics: vec![<TopicSnapshot as TestInstance>::test_numeric_boundaries(
                version,
            )],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields(version: i16) -> Self {
        Self {
            cluster_id: Some(KafkaString::from("test".to_owned())),
            replica_id: 12345_i32,
            max_bytes: 12345_i32,
            topics: vec![<TopicSnapshot as TestInstance>::test_tagged_fields(version)],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for TopicSnapshot {
    fn test_populated(version: i16) -> Self {
        Self {
            name: KafkaString::from("test".to_owned()),
            partitions: vec![<PartitionSnapshot as TestInstance>::test_populated(version)],
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
            partitions: vec![<PartitionSnapshot as TestInstance>::test_null_optionals(
                version,
            )],
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
                <PartitionSnapshot as TestInstance>::test_populated(version),
                <PartitionSnapshot as TestInstance>::test_multi_element_collections(version),
            ],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries(version: i16) -> Self {
        Self {
            name: KafkaString::from("boundary".to_owned()),
            partitions: vec![<PartitionSnapshot as TestInstance>::test_numeric_boundaries(version)],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields(version: i16) -> Self {
        Self {
            name: KafkaString::from("test".to_owned()),
            partitions: vec![<PartitionSnapshot as TestInstance>::test_tagged_fields(
                version,
            )],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for PartitionSnapshot {
    fn test_populated(version: i16) -> Self {
        Self {
            partition: 12345_i32,
            current_leader_epoch: 12345_i32,
            snapshot_id: <SnapshotId as TestInstance>::test_populated(version),
            position: 9_876_543_210_i64,
            replica_directory_id: if version >= 1 {
                KafkaUuid::ONE
            } else {
                KafkaUuid::ZERO
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
            current_leader_epoch: 0_i32,
            snapshot_id: <SnapshotId as TestInstance>::test_null_optionals(version),
            position: 0_i64,
            replica_directory_id: KafkaUuid::ZERO,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections(version: i16) -> Self {
        Self {
            partition: 0_i32,
            current_leader_epoch: 0_i32,
            snapshot_id: <SnapshotId as TestInstance>::test_null_optionals(version),
            position: 0_i64,
            replica_directory_id: KafkaUuid::ZERO,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections(version: i16) -> Self {
        Self {
            partition: 23456_i32,
            current_leader_epoch: 23456_i32,
            snapshot_id: <SnapshotId as TestInstance>::test_multi_element_collections(version),
            position: 9_876_543_211_i64,
            replica_directory_id: if version >= 1 {
                KafkaUuid::from_parts(2, 3)
            } else {
                KafkaUuid::ZERO
            },
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries(version: i16) -> Self {
        Self {
            partition: i32::MIN,
            current_leader_epoch: i32::MIN,
            snapshot_id: <SnapshotId as TestInstance>::test_numeric_boundaries(version),
            position: i64::MIN,
            replica_directory_id: if version >= 1 {
                KafkaUuid::ONE
            } else {
                KafkaUuid::ZERO
            },
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields(version: i16) -> Self {
        Self {
            partition: 12345_i32,
            current_leader_epoch: 12345_i32,
            snapshot_id: <SnapshotId as TestInstance>::test_tagged_fields(version),
            position: 9_876_543_210_i64,
            replica_directory_id: if version >= 1 {
                KafkaUuid::ONE
            } else {
                KafkaUuid::ZERO
            },
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for SnapshotId {
    fn test_populated(_version: i16) -> Self {
        Self {
            end_offset: 9_876_543_210_i64,
            epoch: 12345_i32,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals(_version: i16) -> Self {
        drop(Self::default());
        Self {
            end_offset: 0_i64,
            epoch: 0_i32,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections(_version: i16) -> Self {
        Self {
            end_offset: 0_i64,
            epoch: 0_i32,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections(_version: i16) -> Self {
        Self {
            end_offset: 9_876_543_211_i64,
            epoch: 23456_i32,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries(_version: i16) -> Self {
        Self {
            end_offset: i64::MIN,
            epoch: i32::MIN,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields(_version: i16) -> Self {
        Self {
            end_offset: 9_876_543_210_i64,
            epoch: 12345_i32,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
fn encode_populated(version: i16) -> crate::MatrixResult<String> {
    let message = <FetchSnapshotRequestData as TestInstance>::test_populated(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_populated(version: i16) -> crate::MatrixResult<usize> {
    let message = <FetchSnapshotRequestData as TestInstance>::test_populated(version);
    Ok(message.encoded_len(version)?)
}
fn encode_null_optionals(version: i16) -> crate::MatrixResult<String> {
    let message = <FetchSnapshotRequestData as TestInstance>::test_null_optionals(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_null_optionals(version: i16) -> crate::MatrixResult<usize> {
    let message = <FetchSnapshotRequestData as TestInstance>::test_null_optionals(version);
    Ok(message.encoded_len(version)?)
}
fn encode_empty_collections(version: i16) -> crate::MatrixResult<String> {
    let message = <FetchSnapshotRequestData as TestInstance>::test_empty_collections(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_empty_collections(version: i16) -> crate::MatrixResult<usize> {
    let message = <FetchSnapshotRequestData as TestInstance>::test_empty_collections(version);
    Ok(message.encoded_len(version)?)
}
fn encode_multi_element_collections(version: i16) -> crate::MatrixResult<String> {
    let message =
        <FetchSnapshotRequestData as TestInstance>::test_multi_element_collections(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_multi_element_collections(version: i16) -> crate::MatrixResult<usize> {
    let message =
        <FetchSnapshotRequestData as TestInstance>::test_multi_element_collections(version);
    Ok(message.encoded_len(version)?)
}
fn encode_numeric_boundaries(version: i16) -> crate::MatrixResult<String> {
    let message = <FetchSnapshotRequestData as TestInstance>::test_numeric_boundaries(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_numeric_boundaries(version: i16) -> crate::MatrixResult<usize> {
    let message = <FetchSnapshotRequestData as TestInstance>::test_numeric_boundaries(version);
    Ok(message.encoded_len(version)?)
}
fn encode_tagged_fields(version: i16) -> crate::MatrixResult<String> {
    let message = <FetchSnapshotRequestData as TestInstance>::test_tagged_fields(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_tagged_fields(version: i16) -> crate::MatrixResult<usize> {
    let message = <FetchSnapshotRequestData as TestInstance>::test_tagged_fields(version);
    Ok(message.encoded_len(version)?)
}
fn reencode(version: i16, hex_input: &str) -> crate::MatrixResult<String> {
    let mut input = Bytes::from(crate::decode_hex(hex_input)?);
    let message = FetchSnapshotRequestData::read(&mut input, version)?;
    crate::ensure_input_consumed(&input)?;
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
const MATRIX_CASES: &[crate::MatrixCase] = &[
    crate::MatrixCase {
        schema_name: "FetchSnapshotRequest",
        java_class: "org.apache.kafka.common.message.FetchSnapshotRequestData",
        version: 0i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchSnapshotRequest",
        java_class: "org.apache.kafka.common.message.FetchSnapshotRequestData",
        version: 1i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchSnapshotRequest",
        java_class: "org.apache.kafka.common.message.FetchSnapshotRequestData",
        version: 0i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchSnapshotRequest",
        java_class: "org.apache.kafka.common.message.FetchSnapshotRequestData",
        version: 1i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchSnapshotRequest",
        java_class: "org.apache.kafka.common.message.FetchSnapshotRequestData",
        version: 0i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchSnapshotRequest",
        java_class: "org.apache.kafka.common.message.FetchSnapshotRequestData",
        version: 0i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchSnapshotRequest",
        java_class: "org.apache.kafka.common.message.FetchSnapshotRequestData",
        version: 0i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchSnapshotRequest",
        java_class: "org.apache.kafka.common.message.FetchSnapshotRequestData",
        version: 0i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchSnapshotRequest",
        java_class: "org.apache.kafka.common.message.FetchSnapshotRequestData",
        version: 1i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchSnapshotRequest",
        java_class: "org.apache.kafka.common.message.FetchSnapshotRequestData",
        version: 1i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchSnapshotRequest",
        java_class: "org.apache.kafka.common.message.FetchSnapshotRequestData",
        version: 1i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchSnapshotRequest",
        java_class: "org.apache.kafka.common.message.FetchSnapshotRequestData",
        version: 1i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
];
pub(crate) fn append_protocol_cases(cases: &mut Vec<crate::MatrixCase>) {
    cases.extend_from_slice(MATRIX_CASES);
}
