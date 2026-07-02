#![allow(
    clippy::all,
    clippy::pedantic,
    clippy::nursery,
    reason = "Generated test fixtures mirror Kafka's schema shape and trade hand-written lint \
              style for reproducible output, matching the generated protocol modules."
)]
use bytes::{Bytes, BytesMut};
use kacrab_protocol::{generated::begin_quorum_epoch_response::*, *};

use crate::TestInstance;

impl TestInstance for BeginQuorumEpochResponseData {
    fn test_populated(version: i16) -> Self {
        Self {
            error_code: 42_i16,
            topics: vec![<TopicData as TestInstance>::test_populated(version)],
            node_endpoints: if version >= 1 {
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
            error_code: 0_i16,
            topics: vec![<TopicData as TestInstance>::test_null_optionals(version)],
            node_endpoints: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections(_version: i16) -> Self {
        Self {
            error_code: 0_i16,
            topics: Vec::new(),
            node_endpoints: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections(version: i16) -> Self {
        Self {
            error_code: 43_i16,
            topics: vec![
                <TopicData as TestInstance>::test_populated(version),
                <TopicData as TestInstance>::test_multi_element_collections(version),
            ],
            node_endpoints: if version >= 1 {
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
            error_code: i16::MIN,
            topics: vec![<TopicData as TestInstance>::test_numeric_boundaries(
                version,
            )],
            node_endpoints: if version >= 1 {
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
            error_code: 42_i16,
            topics: vec![<TopicData as TestInstance>::test_tagged_fields(version)],
            node_endpoints: if version >= 1 {
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
impl TestInstance for TopicData {
    fn test_populated(version: i16) -> Self {
        Self {
            topic_name: KafkaString::from("test".to_owned()),
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
            topic_name: KafkaString::default(),
            partitions: vec![<PartitionData as TestInstance>::test_null_optionals(
                version,
            )],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections(_version: i16) -> Self {
        Self {
            topic_name: KafkaString::default(),
            partitions: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections(version: i16) -> Self {
        Self {
            topic_name: KafkaString::from("test-2".to_owned()),
            partitions: vec![
                <PartitionData as TestInstance>::test_populated(version),
                <PartitionData as TestInstance>::test_multi_element_collections(version),
            ],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries(version: i16) -> Self {
        Self {
            topic_name: KafkaString::from("boundary".to_owned()),
            partitions: vec![<PartitionData as TestInstance>::test_numeric_boundaries(
                version,
            )],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields(version: i16) -> Self {
        Self {
            topic_name: KafkaString::from("test".to_owned()),
            partitions: vec![<PartitionData as TestInstance>::test_tagged_fields(version)],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for PartitionData {
    fn test_populated(_version: i16) -> Self {
        Self {
            partition_index: 12345_i32,
            error_code: 42_i16,
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
            partition_index: 0_i32,
            error_code: 0_i16,
            leader_id: 0_i32,
            leader_epoch: 0_i32,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections(_version: i16) -> Self {
        Self {
            partition_index: 0_i32,
            error_code: 0_i16,
            leader_id: 0_i32,
            leader_epoch: 0_i32,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections(_version: i16) -> Self {
        Self {
            partition_index: 23456_i32,
            error_code: 43_i16,
            leader_id: 23456_i32,
            leader_epoch: 23456_i32,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries(_version: i16) -> Self {
        Self {
            partition_index: i32::MIN,
            error_code: i16::MIN,
            leader_id: i32::MIN,
            leader_epoch: i32::MIN,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields(_version: i16) -> Self {
        Self {
            partition_index: 12345_i32,
            error_code: 42_i16,
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
            port: 42_u16,
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
            port: 0_u16,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections(_version: i16) -> Self {
        Self {
            node_id: 0_i32,
            host: KafkaString::default(),
            port: 0_u16,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections(_version: i16) -> Self {
        Self {
            node_id: 23456_i32,
            host: KafkaString::from("test-2".to_owned()),
            port: 43_u16,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries(_version: i16) -> Self {
        Self {
            node_id: i32::MIN,
            host: KafkaString::from("boundary".to_owned()),
            port: u16::MAX,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields(_version: i16) -> Self {
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
    let message = <BeginQuorumEpochResponseData as TestInstance>::test_populated(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_populated(version: i16) -> crate::MatrixResult<usize> {
    let message = <BeginQuorumEpochResponseData as TestInstance>::test_populated(version);
    Ok(message.encoded_len(version)?)
}
fn encode_null_optionals(version: i16) -> crate::MatrixResult<String> {
    let message = <BeginQuorumEpochResponseData as TestInstance>::test_null_optionals(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_null_optionals(version: i16) -> crate::MatrixResult<usize> {
    let message = <BeginQuorumEpochResponseData as TestInstance>::test_null_optionals(version);
    Ok(message.encoded_len(version)?)
}
fn encode_empty_collections(version: i16) -> crate::MatrixResult<String> {
    let message = <BeginQuorumEpochResponseData as TestInstance>::test_empty_collections(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_empty_collections(version: i16) -> crate::MatrixResult<usize> {
    let message = <BeginQuorumEpochResponseData as TestInstance>::test_empty_collections(version);
    Ok(message.encoded_len(version)?)
}
fn encode_multi_element_collections(version: i16) -> crate::MatrixResult<String> {
    let message =
        <BeginQuorumEpochResponseData as TestInstance>::test_multi_element_collections(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_multi_element_collections(version: i16) -> crate::MatrixResult<usize> {
    let message =
        <BeginQuorumEpochResponseData as TestInstance>::test_multi_element_collections(version);
    Ok(message.encoded_len(version)?)
}
fn encode_numeric_boundaries(version: i16) -> crate::MatrixResult<String> {
    let message = <BeginQuorumEpochResponseData as TestInstance>::test_numeric_boundaries(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_numeric_boundaries(version: i16) -> crate::MatrixResult<usize> {
    let message = <BeginQuorumEpochResponseData as TestInstance>::test_numeric_boundaries(version);
    Ok(message.encoded_len(version)?)
}
fn encode_tagged_fields(version: i16) -> crate::MatrixResult<String> {
    let message = <BeginQuorumEpochResponseData as TestInstance>::test_tagged_fields(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_tagged_fields(version: i16) -> crate::MatrixResult<usize> {
    let message = <BeginQuorumEpochResponseData as TestInstance>::test_tagged_fields(version);
    Ok(message.encoded_len(version)?)
}
fn reencode(version: i16, hex_input: &str) -> crate::MatrixResult<String> {
    let mut input = Bytes::from(crate::decode_hex(hex_input)?);
    let message = BeginQuorumEpochResponseData::read(&mut input, version)?;
    crate::ensure_input_consumed(&input)?;
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
const MATRIX_CASES: &[crate::MatrixCase] = &[
    crate::MatrixCase {
        schema_name: "BeginQuorumEpochResponse",
        java_class: "org.apache.kafka.common.message.BeginQuorumEpochResponseData",
        version: 0i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "BeginQuorumEpochResponse",
        java_class: "org.apache.kafka.common.message.BeginQuorumEpochResponseData",
        version: 1i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "BeginQuorumEpochResponse",
        java_class: "org.apache.kafka.common.message.BeginQuorumEpochResponseData",
        version: 0i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "BeginQuorumEpochResponse",
        java_class: "org.apache.kafka.common.message.BeginQuorumEpochResponseData",
        version: 1i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "BeginQuorumEpochResponse",
        java_class: "org.apache.kafka.common.message.BeginQuorumEpochResponseData",
        version: 0i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "BeginQuorumEpochResponse",
        java_class: "org.apache.kafka.common.message.BeginQuorumEpochResponseData",
        version: 0i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "BeginQuorumEpochResponse",
        java_class: "org.apache.kafka.common.message.BeginQuorumEpochResponseData",
        version: 0i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "BeginQuorumEpochResponse",
        java_class: "org.apache.kafka.common.message.BeginQuorumEpochResponseData",
        version: 0i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "BeginQuorumEpochResponse",
        java_class: "org.apache.kafka.common.message.BeginQuorumEpochResponseData",
        version: 1i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "BeginQuorumEpochResponse",
        java_class: "org.apache.kafka.common.message.BeginQuorumEpochResponseData",
        version: 1i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "BeginQuorumEpochResponse",
        java_class: "org.apache.kafka.common.message.BeginQuorumEpochResponseData",
        version: 1i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "BeginQuorumEpochResponse",
        java_class: "org.apache.kafka.common.message.BeginQuorumEpochResponseData",
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
