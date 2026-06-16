use bytes::{Bytes, BytesMut};
use kacrab_protocol::{generated::fetch_snapshot_request::*, *};

use crate::TestInstance;

impl TestInstance for FetchSnapshotRequestData {
    fn test_populated() -> Self {
        Self {
            cluster_id: Some(KafkaString::from("test".to_owned())),
            replica_id: 12345_i32,
            max_bytes: 12345_i32,
            topics: vec![<TopicSnapshot as TestInstance>::test_populated()],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals() -> Self {
        Self {
            cluster_id: None,
            replica_id: 0_i32,
            max_bytes: 0_i32,
            topics: vec![<TopicSnapshot as TestInstance>::test_null_optionals()],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            cluster_id: Some(KafkaString::default()),
            replica_id: 0_i32,
            max_bytes: 0_i32,
            topics: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            cluster_id: Some(KafkaString::from("test-2".to_owned())),
            replica_id: 23456_i32,
            max_bytes: 23456_i32,
            topics: vec![
                <TopicSnapshot as TestInstance>::test_populated(),
                <TopicSnapshot as TestInstance>::test_multi_element_collections(),
            ],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            cluster_id: Some(KafkaString::from("boundary".to_owned())),
            replica_id: i32::MIN,
            max_bytes: i32::MIN,
            topics: vec![<TopicSnapshot as TestInstance>::test_numeric_boundaries()],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            cluster_id: Some(KafkaString::from("test".to_owned())),
            replica_id: 12345_i32,
            max_bytes: 12345_i32,
            topics: vec![<TopicSnapshot as TestInstance>::test_tagged_fields()],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for TopicSnapshot {
    fn test_populated() -> Self {
        Self {
            name: KafkaString::from("test".to_owned()),
            partitions: vec![<PartitionSnapshot as TestInstance>::test_populated()],
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
            partitions: vec![<PartitionSnapshot as TestInstance>::test_null_optionals()],
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
                <PartitionSnapshot as TestInstance>::test_populated(),
                <PartitionSnapshot as TestInstance>::test_multi_element_collections(),
            ],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            name: KafkaString::from("boundary".to_owned()),
            partitions: vec![<PartitionSnapshot as TestInstance>::test_numeric_boundaries()],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            name: KafkaString::from("test".to_owned()),
            partitions: vec![<PartitionSnapshot as TestInstance>::test_tagged_fields()],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for PartitionSnapshot {
    fn test_populated() -> Self {
        Self {
            partition: 12345_i32,
            current_leader_epoch: 12345_i32,
            snapshot_id: <SnapshotId as TestInstance>::test_populated(),
            position: 9_876_543_210_i64,
            replica_directory_id: KafkaUuid::ONE,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals() -> Self {
        drop(Self::default());
        Self {
            partition: 0_i32,
            current_leader_epoch: 0_i32,
            snapshot_id: <SnapshotId as TestInstance>::test_null_optionals(),
            position: 0_i64,
            replica_directory_id: KafkaUuid::ZERO,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            partition: 0_i32,
            current_leader_epoch: 0_i32,
            snapshot_id: <SnapshotId as TestInstance>::test_null_optionals(),
            position: 0_i64,
            replica_directory_id: KafkaUuid::ZERO,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            partition: 23456_i32,
            current_leader_epoch: 23456_i32,
            snapshot_id: <SnapshotId as TestInstance>::test_multi_element_collections(),
            position: 9_876_543_211_i64,
            replica_directory_id: KafkaUuid::from_parts(2, 3),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            partition: i32::MIN,
            current_leader_epoch: i32::MIN,
            snapshot_id: <SnapshotId as TestInstance>::test_numeric_boundaries(),
            position: i64::MIN,
            replica_directory_id: KafkaUuid::ONE,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            partition: 12345_i32,
            current_leader_epoch: 12345_i32,
            snapshot_id: <SnapshotId as TestInstance>::test_tagged_fields(),
            position: 9_876_543_210_i64,
            replica_directory_id: KafkaUuid::ONE,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for SnapshotId {
    fn test_populated() -> Self {
        Self {
            end_offset: 9_876_543_210_i64,
            epoch: 12345_i32,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals() -> Self {
        drop(Self::default());
        Self {
            end_offset: 0_i64,
            epoch: 0_i32,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            end_offset: 0_i64,
            epoch: 0_i32,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            end_offset: 9_876_543_211_i64,
            epoch: 23456_i32,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            end_offset: i64::MIN,
            epoch: i32::MIN,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
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
    let message = <FetchSnapshotRequestData as TestInstance>::test_populated();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_null_optionals(version: i16) -> crate::MatrixResult<String> {
    let message = <FetchSnapshotRequestData as TestInstance>::test_null_optionals();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_empty_collections(version: i16) -> crate::MatrixResult<String> {
    let message = <FetchSnapshotRequestData as TestInstance>::test_empty_collections();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_multi_element_collections(version: i16) -> crate::MatrixResult<String> {
    let message = <FetchSnapshotRequestData as TestInstance>::test_multi_element_collections();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_numeric_boundaries(version: i16) -> crate::MatrixResult<String> {
    let message = <FetchSnapshotRequestData as TestInstance>::test_numeric_boundaries();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_tagged_fields(version: i16) -> crate::MatrixResult<String> {
    let message = <FetchSnapshotRequestData as TestInstance>::test_tagged_fields();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
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
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchSnapshotRequest",
        java_class: "org.apache.kafka.common.message.FetchSnapshotRequestData",
        version: 1i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchSnapshotRequest",
        java_class: "org.apache.kafka.common.message.FetchSnapshotRequestData",
        version: 0i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchSnapshotRequest",
        java_class: "org.apache.kafka.common.message.FetchSnapshotRequestData",
        version: 1i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchSnapshotRequest",
        java_class: "org.apache.kafka.common.message.FetchSnapshotRequestData",
        version: 0i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchSnapshotRequest",
        java_class: "org.apache.kafka.common.message.FetchSnapshotRequestData",
        version: 0i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchSnapshotRequest",
        java_class: "org.apache.kafka.common.message.FetchSnapshotRequestData",
        version: 0i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchSnapshotRequest",
        java_class: "org.apache.kafka.common.message.FetchSnapshotRequestData",
        version: 0i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchSnapshotRequest",
        java_class: "org.apache.kafka.common.message.FetchSnapshotRequestData",
        version: 1i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchSnapshotRequest",
        java_class: "org.apache.kafka.common.message.FetchSnapshotRequestData",
        version: 1i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchSnapshotRequest",
        java_class: "org.apache.kafka.common.message.FetchSnapshotRequestData",
        version: 1i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchSnapshotRequest",
        java_class: "org.apache.kafka.common.message.FetchSnapshotRequestData",
        version: 1i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
];
pub(crate) fn append_protocol_cases(cases: &mut Vec<crate::MatrixCase>) {
    cases.extend_from_slice(MATRIX_CASES);
}
