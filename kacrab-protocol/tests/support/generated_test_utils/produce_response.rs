#![allow(
    clippy::all,
    clippy::pedantic,
    clippy::nursery,
    reason = "Generated test fixtures mirror Kafka's schema shape and trade hand-written lint \
              style for reproducible output, matching the generated protocol modules."
)]
use bytes::{Bytes, BytesMut};
use kacrab_protocol::{generated::produce_response::*, *};

use crate::TestInstance;

impl TestInstance for ProduceResponseData {
    fn test_populated(version: i16) -> Self {
        Self {
            responses: vec![<TopicProduceResponse as TestInstance>::test_populated(
                version,
            )],
            throttle_time_ms: 12345_i32,
            node_endpoints: if version >= 10 {
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
            responses: vec![<TopicProduceResponse as TestInstance>::test_null_optionals(
                version,
            )],
            throttle_time_ms: 0_i32,
            node_endpoints: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections(_version: i16) -> Self {
        Self {
            responses: Vec::new(),
            throttle_time_ms: 0_i32,
            node_endpoints: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections(version: i16) -> Self {
        Self {
            responses: vec![
                <TopicProduceResponse as TestInstance>::test_populated(version),
                <TopicProduceResponse as TestInstance>::test_multi_element_collections(version),
            ],
            throttle_time_ms: 23456_i32,
            node_endpoints: if version >= 10 {
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
            responses: vec![
                <TopicProduceResponse as TestInstance>::test_numeric_boundaries(version),
            ],
            throttle_time_ms: i32::MIN,
            node_endpoints: if version >= 10 {
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
            responses: vec![<TopicProduceResponse as TestInstance>::test_tagged_fields(
                version,
            )],
            throttle_time_ms: 12345_i32,
            node_endpoints: if version >= 10 {
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
impl TestInstance for TopicProduceResponse {
    fn test_populated(version: i16) -> Self {
        Self {
            name: if version <= 12 {
                KafkaString::from("test".to_owned())
            } else {
                KafkaString::default()
            },
            topic_id: if version >= 13 {
                KafkaUuid::ONE
            } else {
                KafkaUuid::ZERO
            },
            partition_responses: vec![<PartitionProduceResponse as TestInstance>::test_populated(
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
            name: KafkaString::default(),
            topic_id: KafkaUuid::ZERO,
            partition_responses: vec![
                <PartitionProduceResponse as TestInstance>::test_null_optionals(version),
            ],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections(_version: i16) -> Self {
        Self {
            name: KafkaString::default(),
            topic_id: KafkaUuid::ZERO,
            partition_responses: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections(version: i16) -> Self {
        Self {
            name: if version <= 12 {
                KafkaString::from("test-2".to_owned())
            } else {
                KafkaString::default()
            },
            topic_id: if version >= 13 {
                KafkaUuid::from_parts(2, 3)
            } else {
                KafkaUuid::ZERO
            },
            partition_responses: vec![
                <PartitionProduceResponse as TestInstance>::test_populated(version),
                <PartitionProduceResponse as TestInstance>::test_multi_element_collections(version),
            ],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries(version: i16) -> Self {
        Self {
            name: if version <= 12 {
                KafkaString::from("boundary".to_owned())
            } else {
                KafkaString::default()
            },
            topic_id: if version >= 13 {
                KafkaUuid::ONE
            } else {
                KafkaUuid::ZERO
            },
            partition_responses: vec![
                <PartitionProduceResponse as TestInstance>::test_numeric_boundaries(version),
            ],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields(version: i16) -> Self {
        Self {
            name: if version <= 12 {
                KafkaString::from("test".to_owned())
            } else {
                KafkaString::default()
            },
            topic_id: if version >= 13 {
                KafkaUuid::ONE
            } else {
                KafkaUuid::ZERO
            },
            partition_responses: vec![
                <PartitionProduceResponse as TestInstance>::test_tagged_fields(version),
            ],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for PartitionProduceResponse {
    fn test_populated(version: i16) -> Self {
        Self {
            index: 12345_i32,
            error_code: 42_i16,
            base_offset: 9_876_543_210_i64,
            log_append_time_ms: 9_876_543_210_i64,
            log_start_offset: if version >= 5 {
                9_876_543_210_i64
            } else {
                -1i64
            },
            record_errors: if version >= 8 {
                vec![<BatchIndexAndErrorMessage as TestInstance>::test_populated(
                    version,
                )]
            } else {
                Vec::new()
            },
            error_message: (version >= 8)
                .then(|| Some(KafkaString::from("test".to_owned())))
                .flatten(),
            current_leader: if version >= 10 {
                <LeaderIdAndEpoch as TestInstance>::test_populated(version)
            } else {
                LeaderIdAndEpoch::default()
            },
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals(version: i16) -> Self {
        drop(Self::default());
        drop(<LeaderIdAndEpoch as TestInstance>::test_null_optionals(
            version,
        ));
        Self {
            index: 0_i32,
            error_code: 0_i16,
            base_offset: 0_i64,
            log_append_time_ms: 0_i64,
            log_start_offset: if version >= 5 { 0_i64 } else { -1i64 },
            record_errors: if version >= 8 {
                vec![<BatchIndexAndErrorMessage as TestInstance>::test_null_optionals(version)]
            } else {
                Vec::new()
            },
            error_message: None,
            current_leader: LeaderIdAndEpoch::default(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections(version: i16) -> Self {
        Self {
            index: 0_i32,
            error_code: 0_i16,
            base_offset: 0_i64,
            log_append_time_ms: 0_i64,
            log_start_offset: if version >= 5 { 0_i64 } else { -1i64 },
            record_errors: Vec::new(),
            error_message: (version >= 8)
                .then(|| Some(KafkaString::default()))
                .flatten(),
            current_leader: if version >= 10 {
                <LeaderIdAndEpoch as TestInstance>::test_null_optionals(version)
            } else {
                LeaderIdAndEpoch::default()
            },
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections(version: i16) -> Self {
        Self {
            index: 23456_i32,
            error_code: 43_i16,
            base_offset: 9_876_543_211_i64,
            log_append_time_ms: 9_876_543_211_i64,
            log_start_offset: if version >= 5 {
                9_876_543_211_i64
            } else {
                -1i64
            },
            record_errors: if version >= 8 {
                vec![
                    <BatchIndexAndErrorMessage as TestInstance>::test_populated(version),
                    <BatchIndexAndErrorMessage as TestInstance>::test_multi_element_collections(
                        version,
                    ),
                ]
            } else {
                Vec::new()
            },
            error_message: (version >= 8)
                .then(|| Some(KafkaString::from("test-2".to_owned())))
                .flatten(),
            current_leader: if version >= 10 {
                <LeaderIdAndEpoch as TestInstance>::test_multi_element_collections(version)
            } else {
                LeaderIdAndEpoch::default()
            },
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries(version: i16) -> Self {
        Self {
            index: i32::MIN,
            error_code: i16::MIN,
            base_offset: i64::MIN,
            log_append_time_ms: i64::MIN,
            log_start_offset: if version >= 5 { i64::MIN } else { -1i64 },
            record_errors: if version >= 8 {
                vec![<BatchIndexAndErrorMessage as TestInstance>::test_numeric_boundaries(version)]
            } else {
                Vec::new()
            },
            error_message: (version >= 8)
                .then(|| Some(KafkaString::from("boundary".to_owned())))
                .flatten(),
            current_leader: if version >= 10 {
                <LeaderIdAndEpoch as TestInstance>::test_numeric_boundaries(version)
            } else {
                LeaderIdAndEpoch::default()
            },
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields(version: i16) -> Self {
        Self {
            index: 12345_i32,
            error_code: 42_i16,
            base_offset: 9_876_543_210_i64,
            log_append_time_ms: 9_876_543_210_i64,
            log_start_offset: if version >= 5 {
                9_876_543_210_i64
            } else {
                -1i64
            },
            record_errors: if version >= 8 {
                vec![<BatchIndexAndErrorMessage as TestInstance>::test_tagged_fields(version)]
            } else {
                Vec::new()
            },
            error_message: (version >= 8)
                .then(|| Some(KafkaString::from("test".to_owned())))
                .flatten(),
            current_leader: if version >= 10 {
                <LeaderIdAndEpoch as TestInstance>::test_tagged_fields(version)
            } else {
                LeaderIdAndEpoch::default()
            },
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for BatchIndexAndErrorMessage {
    fn test_populated(_version: i16) -> Self {
        Self {
            batch_index: 12345_i32,
            batch_index_error_message: Some(KafkaString::from("test".to_owned())),
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals(_version: i16) -> Self {
        drop(Self::default());
        Self {
            batch_index: 0_i32,
            batch_index_error_message: None,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections(_version: i16) -> Self {
        Self {
            batch_index: 0_i32,
            batch_index_error_message: Some(KafkaString::default()),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections(_version: i16) -> Self {
        Self {
            batch_index: 23456_i32,
            batch_index_error_message: Some(KafkaString::from("test-2".to_owned())),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries(_version: i16) -> Self {
        Self {
            batch_index: i32::MIN,
            batch_index_error_message: Some(KafkaString::from("boundary".to_owned())),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields(_version: i16) -> Self {
        Self {
            batch_index: 12345_i32,
            batch_index_error_message: Some(KafkaString::from("test".to_owned())),
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
    let message = <ProduceResponseData as TestInstance>::test_populated(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_populated(version: i16) -> crate::MatrixResult<usize> {
    let message = <ProduceResponseData as TestInstance>::test_populated(version);
    Ok(message.encoded_len(version)?)
}
fn encode_null_optionals(version: i16) -> crate::MatrixResult<String> {
    let message = <ProduceResponseData as TestInstance>::test_null_optionals(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_null_optionals(version: i16) -> crate::MatrixResult<usize> {
    let message = <ProduceResponseData as TestInstance>::test_null_optionals(version);
    Ok(message.encoded_len(version)?)
}
fn encode_empty_collections(version: i16) -> crate::MatrixResult<String> {
    let message = <ProduceResponseData as TestInstance>::test_empty_collections(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_empty_collections(version: i16) -> crate::MatrixResult<usize> {
    let message = <ProduceResponseData as TestInstance>::test_empty_collections(version);
    Ok(message.encoded_len(version)?)
}
fn encode_multi_element_collections(version: i16) -> crate::MatrixResult<String> {
    let message = <ProduceResponseData as TestInstance>::test_multi_element_collections(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_multi_element_collections(version: i16) -> crate::MatrixResult<usize> {
    let message = <ProduceResponseData as TestInstance>::test_multi_element_collections(version);
    Ok(message.encoded_len(version)?)
}
fn encode_numeric_boundaries(version: i16) -> crate::MatrixResult<String> {
    let message = <ProduceResponseData as TestInstance>::test_numeric_boundaries(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_numeric_boundaries(version: i16) -> crate::MatrixResult<usize> {
    let message = <ProduceResponseData as TestInstance>::test_numeric_boundaries(version);
    Ok(message.encoded_len(version)?)
}
fn encode_tagged_fields(version: i16) -> crate::MatrixResult<String> {
    let message = <ProduceResponseData as TestInstance>::test_tagged_fields(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_tagged_fields(version: i16) -> crate::MatrixResult<usize> {
    let message = <ProduceResponseData as TestInstance>::test_tagged_fields(version);
    Ok(message.encoded_len(version)?)
}
fn reencode(version: i16, hex_input: &str) -> crate::MatrixResult<String> {
    let mut input = Bytes::from(crate::decode_hex(hex_input)?);
    let message = ProduceResponseData::read(&mut input, version)?;
    crate::ensure_input_consumed(&input)?;
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
const MATRIX_CASES: &[crate::MatrixCase] = &[
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 3i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 4i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 5i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 6i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 7i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 8i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 9i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 10i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 11i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 12i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 13i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 3i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 4i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 5i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 6i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 7i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 8i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 9i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 10i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 11i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 12i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 13i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 3i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 3i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 3i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 3i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 4i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 4i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 4i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 4i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 5i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 5i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 5i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 5i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 6i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 6i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 6i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 6i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 7i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 7i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 7i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 7i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 8i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 8i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 8i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 8i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 9i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 9i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 9i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 9i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 10i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 10i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 10i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 10i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 11i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 11i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 11i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 11i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 12i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 12i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 12i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 12i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 13i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 13i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 13i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 13i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
];
pub(crate) fn append_protocol_cases(cases: &mut Vec<crate::MatrixCase>) {
    cases.extend_from_slice(MATRIX_CASES);
}
