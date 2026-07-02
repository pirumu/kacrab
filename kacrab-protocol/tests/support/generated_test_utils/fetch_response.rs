#![allow(
    clippy::all,
    clippy::pedantic,
    clippy::nursery,
    reason = "Generated test fixtures mirror Kafka's schema shape and trade hand-written lint \
              style for reproducible output, matching the generated protocol modules."
)]
use bytes::{Bytes, BytesMut};
use kacrab_protocol::{generated::fetch_response::*, *};

use crate::TestInstance;

impl TestInstance for FetchResponseData {
    fn test_populated(version: i16) -> Self {
        Self {
            throttle_time_ms: 12345_i32,
            error_code: if version >= 7 { 42_i16 } else { 0_i16 },
            session_id: if version >= 7 { 12345_i32 } else { 0i32 },
            responses: vec![<FetchableTopicResponse as TestInstance>::test_populated(
                version,
            )],
            node_endpoints: if version >= 16 {
                vec![<NodeEndpoint as TestInstance>::test_populated(version)]
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
        drop(<NodeEndpoint as TestInstance>::test_null_optionals(version));
        Self {
            throttle_time_ms: 0_i32,
            error_code: 0_i16,
            session_id: if version >= 7 { 0_i32 } else { 0i32 },
            responses: vec![<FetchableTopicResponse as TestInstance>::test_null_optionals(version)],
            node_endpoints: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections(version: i16) -> Self {
        Self {
            throttle_time_ms: 0_i32,
            error_code: 0_i16,
            session_id: if version >= 7 { 0_i32 } else { 0i32 },
            responses: Vec::new(),
            node_endpoints: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections(version: i16) -> Self {
        Self {
            throttle_time_ms: 23456_i32,
            error_code: if version >= 7 { 43_i16 } else { 0_i16 },
            session_id: if version >= 7 { 23456_i32 } else { 0i32 },
            responses: vec![
                <FetchableTopicResponse as TestInstance>::test_populated(version),
                <FetchableTopicResponse as TestInstance>::test_multi_element_collections(version),
            ],
            node_endpoints: if version >= 16 {
                vec![
                    <NodeEndpoint as TestInstance>::test_populated(version),
                    <NodeEndpoint as TestInstance>::test_multi_element_collections(version),
                ]
            } else {
                Vec::new()
            },
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries(version: i16) -> Self {
        Self {
            throttle_time_ms: i32::MIN,
            error_code: if version >= 7 { i16::MIN } else { 0_i16 },
            session_id: if version >= 7 { i32::MIN } else { 0i32 },
            responses: vec![
                <FetchableTopicResponse as TestInstance>::test_numeric_boundaries(version),
            ],
            node_endpoints: if version >= 16 {
                vec![<NodeEndpoint as TestInstance>::test_numeric_boundaries(
                    version,
                )]
            } else {
                Vec::new()
            },
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields(version: i16) -> Self {
        Self {
            throttle_time_ms: 12345_i32,
            error_code: if version >= 7 { 42_i16 } else { 0_i16 },
            session_id: if version >= 7 { 12345_i32 } else { 0i32 },
            responses: vec![<FetchableTopicResponse as TestInstance>::test_tagged_fields(version)],
            node_endpoints: if version >= 16 {
                vec![<NodeEndpoint as TestInstance>::test_tagged_fields(version)]
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
impl TestInstance for FetchableTopicResponse {
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
            partitions: vec![<PartitionData as TestInstance>::test_populated(version)],
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
            partitions: vec![<PartitionData as TestInstance>::test_null_optionals(
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
                <PartitionData as TestInstance>::test_populated(version),
                <PartitionData as TestInstance>::test_multi_element_collections(version),
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
            partitions: vec![<PartitionData as TestInstance>::test_numeric_boundaries(
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
            partitions: vec![<PartitionData as TestInstance>::test_tagged_fields(version)],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for PartitionData {
    fn test_populated(version: i16) -> Self {
        Self {
            partition_index: 12345_i32,
            error_code: 42_i16,
            high_watermark: 9_876_543_210_i64,
            last_stable_offset: 9_876_543_210_i64,
            log_start_offset: if version >= 5 {
                9_876_543_210_i64
            } else {
                -1i64
            },
            diverging_epoch: if version >= 12 {
                <EpochEndOffset as TestInstance>::test_populated(version)
            } else {
                EpochEndOffset::default()
            },
            current_leader: if version >= 12 {
                <LeaderIdAndEpoch as TestInstance>::test_populated(version)
            } else {
                LeaderIdAndEpoch::default()
            },
            snapshot_id: if version >= 12 {
                <SnapshotId as TestInstance>::test_populated(version)
            } else {
                SnapshotId::default()
            },
            aborted_transactions: Some(vec![<AbortedTransaction as TestInstance>::test_populated(
                version,
            )]),
            preferred_read_replica: if version >= 11 { 12345_i32 } else { -1i32 },
            records: Some(Bytes::from_static(b"\x00")),
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals(version: i16) -> Self {
        drop(Self::default());
        drop(<EpochEndOffset as TestInstance>::test_null_optionals(
            version,
        ));
        drop(<LeaderIdAndEpoch as TestInstance>::test_null_optionals(
            version,
        ));
        drop(<SnapshotId as TestInstance>::test_null_optionals(version));
        drop(<AbortedTransaction as TestInstance>::test_null_optionals(
            version,
        ));
        Self {
            partition_index: 0_i32,
            error_code: 0_i16,
            high_watermark: 0_i64,
            last_stable_offset: 0_i64,
            log_start_offset: if version >= 5 { 0_i64 } else { -1i64 },
            diverging_epoch: EpochEndOffset::default(),
            current_leader: LeaderIdAndEpoch::default(),
            snapshot_id: SnapshotId::default(),
            aborted_transactions: None,
            preferred_read_replica: if version >= 11 { 0_i32 } else { -1i32 },
            records: None,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections(version: i16) -> Self {
        Self {
            partition_index: 0_i32,
            error_code: 0_i16,
            high_watermark: 0_i64,
            last_stable_offset: 0_i64,
            log_start_offset: if version >= 5 { 0_i64 } else { -1i64 },
            diverging_epoch: if version >= 12 {
                <EpochEndOffset as TestInstance>::test_null_optionals(version)
            } else {
                EpochEndOffset::default()
            },
            current_leader: if version >= 12 {
                <LeaderIdAndEpoch as TestInstance>::test_null_optionals(version)
            } else {
                LeaderIdAndEpoch::default()
            },
            snapshot_id: if version >= 12 {
                <SnapshotId as TestInstance>::test_null_optionals(version)
            } else {
                SnapshotId::default()
            },
            aborted_transactions: Some(Vec::new()),
            preferred_read_replica: if version >= 11 { 0_i32 } else { -1i32 },
            records: Some(Bytes::new()),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections(version: i16) -> Self {
        Self {
            partition_index: 23456_i32,
            error_code: 43_i16,
            high_watermark: 9_876_543_211_i64,
            last_stable_offset: 9_876_543_211_i64,
            log_start_offset: if version >= 5 {
                9_876_543_211_i64
            } else {
                -1i64
            },
            diverging_epoch: if version >= 12 {
                <EpochEndOffset as TestInstance>::test_multi_element_collections(version)
            } else {
                EpochEndOffset::default()
            },
            current_leader: if version >= 12 {
                <LeaderIdAndEpoch as TestInstance>::test_multi_element_collections(version)
            } else {
                LeaderIdAndEpoch::default()
            },
            snapshot_id: if version >= 12 {
                <SnapshotId as TestInstance>::test_multi_element_collections(version)
            } else {
                SnapshotId::default()
            },
            aborted_transactions: Some(vec![
                <AbortedTransaction as TestInstance>::test_populated(version),
                <AbortedTransaction as TestInstance>::test_multi_element_collections(version),
            ]),
            preferred_read_replica: if version >= 11 { 23456_i32 } else { -1i32 },
            records: Some(Bytes::new()),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries(version: i16) -> Self {
        Self {
            partition_index: i32::MIN,
            error_code: i16::MIN,
            high_watermark: i64::MIN,
            last_stable_offset: i64::MIN,
            log_start_offset: if version >= 5 { i64::MIN } else { -1i64 },
            diverging_epoch: if version >= 12 {
                <EpochEndOffset as TestInstance>::test_numeric_boundaries(version)
            } else {
                EpochEndOffset::default()
            },
            current_leader: if version >= 12 {
                <LeaderIdAndEpoch as TestInstance>::test_numeric_boundaries(version)
            } else {
                LeaderIdAndEpoch::default()
            },
            snapshot_id: if version >= 12 {
                <SnapshotId as TestInstance>::test_numeric_boundaries(version)
            } else {
                SnapshotId::default()
            },
            aborted_transactions: Some(vec![
                <AbortedTransaction as TestInstance>::test_numeric_boundaries(version),
            ]),
            preferred_read_replica: if version >= 11 { i32::MIN } else { -1i32 },
            records: Some(Bytes::new()),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields(version: i16) -> Self {
        Self {
            partition_index: 12345_i32,
            error_code: 42_i16,
            high_watermark: 9_876_543_210_i64,
            last_stable_offset: 9_876_543_210_i64,
            log_start_offset: if version >= 5 {
                9_876_543_210_i64
            } else {
                -1i64
            },
            diverging_epoch: if version >= 12 {
                <EpochEndOffset as TestInstance>::test_tagged_fields(version)
            } else {
                EpochEndOffset::default()
            },
            current_leader: if version >= 12 {
                <LeaderIdAndEpoch as TestInstance>::test_tagged_fields(version)
            } else {
                LeaderIdAndEpoch::default()
            },
            snapshot_id: if version >= 12 {
                <SnapshotId as TestInstance>::test_tagged_fields(version)
            } else {
                SnapshotId::default()
            },
            aborted_transactions: Some(vec![
                <AbortedTransaction as TestInstance>::test_tagged_fields(version),
            ]),
            preferred_read_replica: if version >= 11 { 12345_i32 } else { -1i32 },
            records: Some(Bytes::new()),
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for EpochEndOffset {
    fn test_populated(_version: i16) -> Self {
        Self {
            epoch: 12345_i32,
            end_offset: 9_876_543_210_i64,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals(_version: i16) -> Self {
        drop(Self::default());
        Self {
            epoch: 0_i32,
            end_offset: 0_i64,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections(_version: i16) -> Self {
        Self {
            epoch: 0_i32,
            end_offset: 0_i64,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections(_version: i16) -> Self {
        Self {
            epoch: 23456_i32,
            end_offset: 9_876_543_211_i64,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries(_version: i16) -> Self {
        Self {
            epoch: i32::MIN,
            end_offset: i64::MIN,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields(_version: i16) -> Self {
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
    fn test_populated(_version: i16) -> Self {
        Self {
            leader_id: 12345_i32,
            leader_epoch: 12345_i32,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals(_version: i16) -> Self {
        drop(Self::default());
        Self {
            leader_id: 0_i32,
            leader_epoch: 0_i32,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections(_version: i16) -> Self {
        Self {
            leader_id: 0_i32,
            leader_epoch: 0_i32,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections(_version: i16) -> Self {
        Self {
            leader_id: 23456_i32,
            leader_epoch: 23456_i32,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries(_version: i16) -> Self {
        Self {
            leader_id: i32::MIN,
            leader_epoch: i32::MIN,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields(_version: i16) -> Self {
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
impl TestInstance for AbortedTransaction {
    fn test_populated(_version: i16) -> Self {
        Self {
            producer_id: 9_876_543_210_i64,
            first_offset: 9_876_543_210_i64,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals(_version: i16) -> Self {
        drop(Self::default());
        Self {
            producer_id: 0_i64,
            first_offset: 0_i64,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections(_version: i16) -> Self {
        Self {
            producer_id: 0_i64,
            first_offset: 0_i64,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections(_version: i16) -> Self {
        Self {
            producer_id: 9_876_543_211_i64,
            first_offset: 9_876_543_211_i64,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries(_version: i16) -> Self {
        Self {
            producer_id: i64::MIN,
            first_offset: i64::MIN,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields(_version: i16) -> Self {
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
    fn test_populated(_version: i16) -> Self {
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
    fn test_null_optionals(_version: i16) -> Self {
        drop(Self::default());
        Self {
            node_id: 0_i32,
            host: KafkaString::default(),
            port: 0_i32,
            rack: None,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections(_version: i16) -> Self {
        Self {
            node_id: 0_i32,
            host: KafkaString::default(),
            port: 0_i32,
            rack: Some(KafkaString::default()),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections(_version: i16) -> Self {
        Self {
            node_id: 23456_i32,
            host: KafkaString::from("test-2".to_owned()),
            port: 23456_i32,
            rack: Some(KafkaString::from("test-2".to_owned())),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries(_version: i16) -> Self {
        Self {
            node_id: i32::MIN,
            host: KafkaString::from("boundary".to_owned()),
            port: i32::MIN,
            rack: Some(KafkaString::from("boundary".to_owned())),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields(_version: i16) -> Self {
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
    let message = <FetchResponseData as TestInstance>::test_populated(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_populated(version: i16) -> crate::MatrixResult<usize> {
    let message = <FetchResponseData as TestInstance>::test_populated(version);
    Ok(message.encoded_len(version)?)
}
fn encode_null_optionals(version: i16) -> crate::MatrixResult<String> {
    let message = <FetchResponseData as TestInstance>::test_null_optionals(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_null_optionals(version: i16) -> crate::MatrixResult<usize> {
    let message = <FetchResponseData as TestInstance>::test_null_optionals(version);
    Ok(message.encoded_len(version)?)
}
fn encode_empty_collections(version: i16) -> crate::MatrixResult<String> {
    let message = <FetchResponseData as TestInstance>::test_empty_collections(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_empty_collections(version: i16) -> crate::MatrixResult<usize> {
    let message = <FetchResponseData as TestInstance>::test_empty_collections(version);
    Ok(message.encoded_len(version)?)
}
fn encode_multi_element_collections(version: i16) -> crate::MatrixResult<String> {
    let message = <FetchResponseData as TestInstance>::test_multi_element_collections(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_multi_element_collections(version: i16) -> crate::MatrixResult<usize> {
    let message = <FetchResponseData as TestInstance>::test_multi_element_collections(version);
    Ok(message.encoded_len(version)?)
}
fn encode_numeric_boundaries(version: i16) -> crate::MatrixResult<String> {
    let message = <FetchResponseData as TestInstance>::test_numeric_boundaries(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_numeric_boundaries(version: i16) -> crate::MatrixResult<usize> {
    let message = <FetchResponseData as TestInstance>::test_numeric_boundaries(version);
    Ok(message.encoded_len(version)?)
}
fn encode_tagged_fields(version: i16) -> crate::MatrixResult<String> {
    let message = <FetchResponseData as TestInstance>::test_tagged_fields(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_tagged_fields(version: i16) -> crate::MatrixResult<usize> {
    let message = <FetchResponseData as TestInstance>::test_tagged_fields(version);
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
