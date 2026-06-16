use bytes::{Bytes, BytesMut};
use kacrab_protocol::{generated::fetch_snapshot_response::*, *};

use crate::TestInstance;

impl TestInstance for FetchSnapshotResponseData {
    fn test_populated() -> Self {
        Self {
            throttle_time_ms: 12345_i32,
            error_code: 42_i16,
            topics: vec![<TopicSnapshot as TestInstance>::test_populated()],
            node_endpoints: vec![<NodeEndpoint as TestInstance>::test_populated()],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals() -> Self {
        drop(<NodeEndpoint as TestInstance>::test_null_optionals());
        Self {
            throttle_time_ms: 0_i32,
            error_code: 0_i16,
            topics: vec![<TopicSnapshot as TestInstance>::test_null_optionals()],
            node_endpoints: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            throttle_time_ms: 0_i32,
            error_code: 0_i16,
            topics: Vec::new(),
            node_endpoints: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            throttle_time_ms: 23456_i32,
            error_code: 43_i16,
            topics: vec![
                <TopicSnapshot as TestInstance>::test_populated(),
                <TopicSnapshot as TestInstance>::test_multi_element_collections(),
            ],
            node_endpoints: vec![
                <NodeEndpoint as TestInstance>::test_populated(),
                <NodeEndpoint as TestInstance>::test_multi_element_collections(),
            ],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            throttle_time_ms: i32::MIN,
            error_code: i16::MIN,
            topics: vec![<TopicSnapshot as TestInstance>::test_numeric_boundaries()],
            node_endpoints: vec![<NodeEndpoint as TestInstance>::test_numeric_boundaries()],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            throttle_time_ms: 12345_i32,
            error_code: 42_i16,
            topics: vec![<TopicSnapshot as TestInstance>::test_tagged_fields()],
            node_endpoints: vec![<NodeEndpoint as TestInstance>::test_tagged_fields()],
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
            index: 12345_i32,
            error_code: 42_i16,
            snapshot_id: <SnapshotId as TestInstance>::test_populated(),
            current_leader: <LeaderIdAndEpoch as TestInstance>::test_populated(),
            size: 9_876_543_210_i64,
            position: 9_876_543_210_i64,
            unaligned_records: Some(Bytes::new()),
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals() -> Self {
        drop(Self::default());
        drop(<LeaderIdAndEpoch as TestInstance>::test_null_optionals());
        Self {
            index: 0_i32,
            error_code: 0_i16,
            snapshot_id: <SnapshotId as TestInstance>::test_null_optionals(),
            current_leader: LeaderIdAndEpoch::default(),
            size: 0_i64,
            position: 0_i64,
            unaligned_records: Some(Bytes::new()),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            index: 0_i32,
            error_code: 0_i16,
            snapshot_id: <SnapshotId as TestInstance>::test_null_optionals(),
            current_leader: <LeaderIdAndEpoch as TestInstance>::test_null_optionals(),
            size: 0_i64,
            position: 0_i64,
            unaligned_records: Some(Bytes::new()),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            index: 23456_i32,
            error_code: 43_i16,
            snapshot_id: <SnapshotId as TestInstance>::test_multi_element_collections(),
            current_leader: <LeaderIdAndEpoch as TestInstance>::test_multi_element_collections(),
            size: 9_876_543_211_i64,
            position: 9_876_543_211_i64,
            unaligned_records: Some(Bytes::new()),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            index: i32::MIN,
            error_code: i16::MIN,
            snapshot_id: <SnapshotId as TestInstance>::test_numeric_boundaries(),
            current_leader: <LeaderIdAndEpoch as TestInstance>::test_numeric_boundaries(),
            size: i64::MIN,
            position: i64::MIN,
            unaligned_records: Some(Bytes::new()),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            index: 12345_i32,
            error_code: 42_i16,
            snapshot_id: <SnapshotId as TestInstance>::test_tagged_fields(),
            current_leader: <LeaderIdAndEpoch as TestInstance>::test_tagged_fields(),
            size: 9_876_543_210_i64,
            position: 9_876_543_210_i64,
            unaligned_records: Some(Bytes::new()),
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
impl TestInstance for LeaderIdAndEpoch {
    fn test_populated() -> Self {
        Self {
            leader_id: 12345_i32,
            leader_epoch: 12345_i32,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals() -> Self {
        drop(Self::default());
        Self {
            leader_id: 0_i32,
            leader_epoch: 0_i32,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            leader_id: 0_i32,
            leader_epoch: 0_i32,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            leader_id: 23456_i32,
            leader_epoch: 23456_i32,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            leader_id: i32::MIN,
            leader_epoch: i32::MIN,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            leader_id: 12345_i32,
            leader_epoch: 12345_i32,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for NodeEndpoint {
    fn test_populated() -> Self {
        Self {
            node_id: 12345_i32,
            host: KafkaString::from("test".to_owned()),
            port: 42_u16,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals() -> Self {
        drop(Self::default());
        Self {
            node_id: 0_i32,
            host: KafkaString::default(),
            port: 0_u16,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            node_id: 0_i32,
            host: KafkaString::default(),
            port: 0_u16,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            node_id: 23456_i32,
            host: KafkaString::from("test-2".to_owned()),
            port: 43_u16,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            node_id: i32::MIN,
            host: KafkaString::from("boundary".to_owned()),
            port: u16::MAX,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            node_id: 12345_i32,
            host: KafkaString::from("test".to_owned()),
            port: 42_u16,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
fn encode_populated(version: i16) -> crate::MatrixResult<String> {
    let message = <FetchSnapshotResponseData as TestInstance>::test_populated();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_populated(version: i16) -> crate::MatrixResult<usize> {
    let message = <FetchSnapshotResponseData as TestInstance>::test_populated();
    Ok(message.encoded_len(version)?)
}
fn encode_null_optionals(version: i16) -> crate::MatrixResult<String> {
    let message = <FetchSnapshotResponseData as TestInstance>::test_null_optionals();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_null_optionals(version: i16) -> crate::MatrixResult<usize> {
    let message = <FetchSnapshotResponseData as TestInstance>::test_null_optionals();
    Ok(message.encoded_len(version)?)
}
fn encode_empty_collections(version: i16) -> crate::MatrixResult<String> {
    let message = <FetchSnapshotResponseData as TestInstance>::test_empty_collections();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_empty_collections(version: i16) -> crate::MatrixResult<usize> {
    let message = <FetchSnapshotResponseData as TestInstance>::test_empty_collections();
    Ok(message.encoded_len(version)?)
}
fn encode_multi_element_collections(version: i16) -> crate::MatrixResult<String> {
    let message = <FetchSnapshotResponseData as TestInstance>::test_multi_element_collections();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_multi_element_collections(version: i16) -> crate::MatrixResult<usize> {
    let message = <FetchSnapshotResponseData as TestInstance>::test_multi_element_collections();
    Ok(message.encoded_len(version)?)
}
fn encode_numeric_boundaries(version: i16) -> crate::MatrixResult<String> {
    let message = <FetchSnapshotResponseData as TestInstance>::test_numeric_boundaries();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_numeric_boundaries(version: i16) -> crate::MatrixResult<usize> {
    let message = <FetchSnapshotResponseData as TestInstance>::test_numeric_boundaries();
    Ok(message.encoded_len(version)?)
}
fn encode_tagged_fields(version: i16) -> crate::MatrixResult<String> {
    let message = <FetchSnapshotResponseData as TestInstance>::test_tagged_fields();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_tagged_fields(version: i16) -> crate::MatrixResult<usize> {
    let message = <FetchSnapshotResponseData as TestInstance>::test_tagged_fields();
    Ok(message.encoded_len(version)?)
}
fn reencode(version: i16, hex_input: &str) -> crate::MatrixResult<String> {
    let mut input = Bytes::from(crate::decode_hex(hex_input)?);
    let message = FetchSnapshotResponseData::read(&mut input, version)?;
    crate::ensure_input_consumed(&input)?;
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
const MATRIX_CASES: &[crate::MatrixCase] = &[
    crate::MatrixCase {
        schema_name: "FetchSnapshotResponse",
        java_class: "org.apache.kafka.common.message.FetchSnapshotResponseData",
        version: 0i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchSnapshotResponse",
        java_class: "org.apache.kafka.common.message.FetchSnapshotResponseData",
        version: 1i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchSnapshotResponse",
        java_class: "org.apache.kafka.common.message.FetchSnapshotResponseData",
        version: 0i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchSnapshotResponse",
        java_class: "org.apache.kafka.common.message.FetchSnapshotResponseData",
        version: 1i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchSnapshotResponse",
        java_class: "org.apache.kafka.common.message.FetchSnapshotResponseData",
        version: 0i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchSnapshotResponse",
        java_class: "org.apache.kafka.common.message.FetchSnapshotResponseData",
        version: 0i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchSnapshotResponse",
        java_class: "org.apache.kafka.common.message.FetchSnapshotResponseData",
        version: 0i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchSnapshotResponse",
        java_class: "org.apache.kafka.common.message.FetchSnapshotResponseData",
        version: 0i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchSnapshotResponse",
        java_class: "org.apache.kafka.common.message.FetchSnapshotResponseData",
        version: 1i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchSnapshotResponse",
        java_class: "org.apache.kafka.common.message.FetchSnapshotResponseData",
        version: 1i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchSnapshotResponse",
        java_class: "org.apache.kafka.common.message.FetchSnapshotResponseData",
        version: 1i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchSnapshotResponse",
        java_class: "org.apache.kafka.common.message.FetchSnapshotResponseData",
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
