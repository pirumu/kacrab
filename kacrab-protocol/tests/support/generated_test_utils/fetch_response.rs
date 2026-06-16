use bytes::{Bytes, BytesMut};
use kacrab_protocol::{generated::fetch_response::*, *};

use crate::TestInstance;

impl TestInstance for FetchResponseData {
    fn test_populated() -> Self {
        Self {
            throttle_time_ms: 12345_i32,
            error_code: 42_i16,
            session_id: 12345_i32,
            responses: vec![<FetchableTopicResponse as TestInstance>::test_populated()],
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
            session_id: 0_i32,
            responses: vec![<FetchableTopicResponse as TestInstance>::test_null_optionals()],
            node_endpoints: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            throttle_time_ms: 0_i32,
            error_code: 0_i16,
            session_id: 0_i32,
            responses: Vec::new(),
            node_endpoints: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            throttle_time_ms: 23456_i32,
            error_code: 43_i16,
            session_id: 23456_i32,
            responses: vec![
                <FetchableTopicResponse as TestInstance>::test_populated(),
                <FetchableTopicResponse as TestInstance>::test_multi_element_collections(),
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
            session_id: i32::MIN,
            responses: vec![<FetchableTopicResponse as TestInstance>::test_numeric_boundaries()],
            node_endpoints: vec![<NodeEndpoint as TestInstance>::test_numeric_boundaries()],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            throttle_time_ms: 12345_i32,
            error_code: 42_i16,
            session_id: 12345_i32,
            responses: vec![<FetchableTopicResponse as TestInstance>::test_tagged_fields()],
            node_endpoints: vec![<NodeEndpoint as TestInstance>::test_tagged_fields()],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for FetchableTopicResponse {
    fn test_populated() -> Self {
        Self {
            topic: KafkaString::from("test".to_owned()),
            topic_id: KafkaUuid::ONE,
            partitions: vec![<PartitionData as TestInstance>::test_populated()],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals() -> Self {
        drop(Self::default());
        Self {
            topic: KafkaString::default(),
            topic_id: KafkaUuid::ZERO,
            partitions: vec![<PartitionData as TestInstance>::test_null_optionals()],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            topic: KafkaString::default(),
            topic_id: KafkaUuid::ZERO,
            partitions: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            topic: KafkaString::from("test-2".to_owned()),
            topic_id: KafkaUuid::from_parts(2, 3),
            partitions: vec![
                <PartitionData as TestInstance>::test_populated(),
                <PartitionData as TestInstance>::test_multi_element_collections(),
            ],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            topic: KafkaString::from("boundary".to_owned()),
            topic_id: KafkaUuid::ONE,
            partitions: vec![<PartitionData as TestInstance>::test_numeric_boundaries()],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            topic: KafkaString::from("test".to_owned()),
            topic_id: KafkaUuid::ONE,
            partitions: vec![<PartitionData as TestInstance>::test_tagged_fields()],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for PartitionData {
    fn test_populated() -> Self {
        Self {
            partition_index: 12345_i32,
            error_code: 42_i16,
            high_watermark: 9_876_543_210_i64,
            last_stable_offset: 9_876_543_210_i64,
            log_start_offset: 9_876_543_210_i64,
            diverging_epoch: <EpochEndOffset as TestInstance>::test_populated(),
            current_leader: <LeaderIdAndEpoch as TestInstance>::test_populated(),
            snapshot_id: <SnapshotId as TestInstance>::test_populated(),
            aborted_transactions: Some(
                vec![<AbortedTransaction as TestInstance>::test_populated()],
            ),
            preferred_read_replica: 12345_i32,
            records: Some(Bytes::from_static(b"\x00")),
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals() -> Self {
        drop(Self::default());
        drop(<EpochEndOffset as TestInstance>::test_null_optionals());
        drop(<LeaderIdAndEpoch as TestInstance>::test_null_optionals());
        drop(<SnapshotId as TestInstance>::test_null_optionals());
        drop(<AbortedTransaction as TestInstance>::test_null_optionals());
        Self {
            partition_index: 0_i32,
            error_code: 0_i16,
            high_watermark: 0_i64,
            last_stable_offset: 0_i64,
            log_start_offset: 0_i64,
            diverging_epoch: EpochEndOffset::default(),
            current_leader: LeaderIdAndEpoch::default(),
            snapshot_id: SnapshotId::default(),
            aborted_transactions: None,
            preferred_read_replica: 0_i32,
            records: None,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            partition_index: 0_i32,
            error_code: 0_i16,
            high_watermark: 0_i64,
            last_stable_offset: 0_i64,
            log_start_offset: 0_i64,
            diverging_epoch: <EpochEndOffset as TestInstance>::test_null_optionals(),
            current_leader: <LeaderIdAndEpoch as TestInstance>::test_null_optionals(),
            snapshot_id: <SnapshotId as TestInstance>::test_null_optionals(),
            aborted_transactions: Some(Vec::new()),
            preferred_read_replica: 0_i32,
            records: Some(Bytes::new()),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            partition_index: 23456_i32,
            error_code: 43_i16,
            high_watermark: 9_876_543_211_i64,
            last_stable_offset: 9_876_543_211_i64,
            log_start_offset: 9_876_543_211_i64,
            diverging_epoch: <EpochEndOffset as TestInstance>::test_multi_element_collections(),
            current_leader: <LeaderIdAndEpoch as TestInstance>::test_multi_element_collections(),
            snapshot_id: <SnapshotId as TestInstance>::test_multi_element_collections(),
            aborted_transactions: Some(vec![
                <AbortedTransaction as TestInstance>::test_populated(),
                <AbortedTransaction as TestInstance>::test_multi_element_collections(),
            ]),
            preferred_read_replica: 23456_i32,
            records: Some(Bytes::new()),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            partition_index: i32::MIN,
            error_code: i16::MIN,
            high_watermark: i64::MIN,
            last_stable_offset: i64::MIN,
            log_start_offset: i64::MIN,
            diverging_epoch: <EpochEndOffset as TestInstance>::test_numeric_boundaries(),
            current_leader: <LeaderIdAndEpoch as TestInstance>::test_numeric_boundaries(),
            snapshot_id: <SnapshotId as TestInstance>::test_numeric_boundaries(),
            aborted_transactions: Some(vec![
                <AbortedTransaction as TestInstance>::test_numeric_boundaries(),
            ]),
            preferred_read_replica: i32::MIN,
            records: Some(Bytes::new()),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            partition_index: 12345_i32,
            error_code: 42_i16,
            high_watermark: 9_876_543_210_i64,
            last_stable_offset: 9_876_543_210_i64,
            log_start_offset: 9_876_543_210_i64,
            diverging_epoch: <EpochEndOffset as TestInstance>::test_tagged_fields(),
            current_leader: <LeaderIdAndEpoch as TestInstance>::test_tagged_fields(),
            snapshot_id: <SnapshotId as TestInstance>::test_tagged_fields(),
            aborted_transactions: Some(vec![
                <AbortedTransaction as TestInstance>::test_tagged_fields(),
            ]),
            preferred_read_replica: 12345_i32,
            records: Some(Bytes::new()),
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for EpochEndOffset {
    fn test_populated() -> Self {
        Self {
            epoch: 12345_i32,
            end_offset: 9_876_543_210_i64,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals() -> Self {
        drop(Self::default());
        Self {
            epoch: 0_i32,
            end_offset: 0_i64,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            epoch: 0_i32,
            end_offset: 0_i64,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            epoch: 23456_i32,
            end_offset: 9_876_543_211_i64,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            epoch: i32::MIN,
            end_offset: i64::MIN,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            epoch: 12345_i32,
            end_offset: 9_876_543_210_i64,
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
impl TestInstance for AbortedTransaction {
    fn test_populated() -> Self {
        Self {
            producer_id: 9_876_543_210_i64,
            first_offset: 9_876_543_210_i64,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals() -> Self {
        drop(Self::default());
        Self {
            producer_id: 0_i64,
            first_offset: 0_i64,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            producer_id: 0_i64,
            first_offset: 0_i64,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            producer_id: 9_876_543_211_i64,
            first_offset: 9_876_543_211_i64,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            producer_id: i64::MIN,
            first_offset: i64::MIN,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            producer_id: 9_876_543_210_i64,
            first_offset: 9_876_543_210_i64,
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
            port: 12345_i32,
            rack: Some(KafkaString::from("test".to_owned())),
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
            port: 0_i32,
            rack: None,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            node_id: 0_i32,
            host: KafkaString::default(),
            port: 0_i32,
            rack: Some(KafkaString::default()),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            node_id: 23456_i32,
            host: KafkaString::from("test-2".to_owned()),
            port: 23456_i32,
            rack: Some(KafkaString::from("test-2".to_owned())),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            node_id: i32::MIN,
            host: KafkaString::from("boundary".to_owned()),
            port: i32::MIN,
            rack: Some(KafkaString::from("boundary".to_owned())),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            node_id: 12345_i32,
            host: KafkaString::from("test".to_owned()),
            port: 12345_i32,
            rack: Some(KafkaString::from("test".to_owned())),
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
fn encode_populated(version: i16) -> crate::MatrixResult<String> {
    let message = <FetchResponseData as TestInstance>::test_populated();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_populated(version: i16) -> crate::MatrixResult<usize> {
    let message = <FetchResponseData as TestInstance>::test_populated();
    Ok(message.encoded_len(version)?)
}
fn encode_null_optionals(version: i16) -> crate::MatrixResult<String> {
    let message = <FetchResponseData as TestInstance>::test_null_optionals();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_null_optionals(version: i16) -> crate::MatrixResult<usize> {
    let message = <FetchResponseData as TestInstance>::test_null_optionals();
    Ok(message.encoded_len(version)?)
}
fn encode_empty_collections(version: i16) -> crate::MatrixResult<String> {
    let message = <FetchResponseData as TestInstance>::test_empty_collections();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_empty_collections(version: i16) -> crate::MatrixResult<usize> {
    let message = <FetchResponseData as TestInstance>::test_empty_collections();
    Ok(message.encoded_len(version)?)
}
fn encode_multi_element_collections(version: i16) -> crate::MatrixResult<String> {
    let message = <FetchResponseData as TestInstance>::test_multi_element_collections();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_multi_element_collections(version: i16) -> crate::MatrixResult<usize> {
    let message = <FetchResponseData as TestInstance>::test_multi_element_collections();
    Ok(message.encoded_len(version)?)
}
fn encode_numeric_boundaries(version: i16) -> crate::MatrixResult<String> {
    let message = <FetchResponseData as TestInstance>::test_numeric_boundaries();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_numeric_boundaries(version: i16) -> crate::MatrixResult<usize> {
    let message = <FetchResponseData as TestInstance>::test_numeric_boundaries();
    Ok(message.encoded_len(version)?)
}
fn encode_tagged_fields(version: i16) -> crate::MatrixResult<String> {
    let message = <FetchResponseData as TestInstance>::test_tagged_fields();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_tagged_fields(version: i16) -> crate::MatrixResult<usize> {
    let message = <FetchResponseData as TestInstance>::test_tagged_fields();
    Ok(message.encoded_len(version)?)
}
fn reencode(version: i16, hex_input: &str) -> crate::MatrixResult<String> {
    let mut input = Bytes::from(crate::decode_hex(hex_input)?);
    let message = FetchResponseData::read(&mut input, version)?;
    crate::ensure_input_consumed(&input)?;
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
const MATRIX_CASES: &[crate::MatrixCase] = &[
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 4i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 5i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 6i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 7i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 8i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 9i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 10i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 11i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 12i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 13i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 14i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 15i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 16i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 17i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 18i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 4i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 5i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 6i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 7i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 8i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 9i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 10i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 11i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 12i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 13i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 14i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 15i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 16i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 17i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 18i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 4i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 4i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 4i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 4i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 5i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 5i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 5i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 5i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 6i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 6i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 6i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 6i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 7i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 7i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 7i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 7i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 8i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 8i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 8i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 8i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 9i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 9i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 9i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 9i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 10i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 10i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 10i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 10i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 11i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 11i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 11i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 11i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 12i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 12i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 12i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 12i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 13i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 13i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 13i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 13i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 14i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 14i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 14i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 14i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 15i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 15i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 15i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 15i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 16i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 16i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 16i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 16i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 17i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 17i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 17i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 17i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 18i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 18i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
        version: 18i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "FetchResponse",
        java_class: "org.apache.kafka.common.message.FetchResponseData",
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
