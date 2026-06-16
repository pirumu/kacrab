use bytes::{Bytes, BytesMut};
use kacrab_protocol::{generated::produce_response::*, *};

use crate::TestInstance;

impl TestInstance for ProduceResponseData {
    fn test_populated() -> Self {
        Self {
            responses: vec![<TopicProduceResponse as TestInstance>::test_populated()],
            throttle_time_ms: 12345_i32,
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
            responses: vec![<TopicProduceResponse as TestInstance>::test_null_optionals()],
            throttle_time_ms: 0_i32,
            node_endpoints: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            responses: Vec::new(),
            throttle_time_ms: 0_i32,
            node_endpoints: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            responses: vec![
                <TopicProduceResponse as TestInstance>::test_populated(),
                <TopicProduceResponse as TestInstance>::test_multi_element_collections(),
            ],
            throttle_time_ms: 23456_i32,
            node_endpoints: vec![
                <NodeEndpoint as TestInstance>::test_populated(),
                <NodeEndpoint as TestInstance>::test_multi_element_collections(),
            ],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            responses: vec![<TopicProduceResponse as TestInstance>::test_numeric_boundaries()],
            throttle_time_ms: i32::MIN,
            node_endpoints: vec![<NodeEndpoint as TestInstance>::test_numeric_boundaries()],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            responses: vec![<TopicProduceResponse as TestInstance>::test_tagged_fields()],
            throttle_time_ms: 12345_i32,
            node_endpoints: vec![<NodeEndpoint as TestInstance>::test_tagged_fields()],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for TopicProduceResponse {
    fn test_populated() -> Self {
        Self {
            name: KafkaString::from("test".to_owned()),
            topic_id: KafkaUuid::ONE,
            partition_responses: vec![<PartitionProduceResponse as TestInstance>::test_populated()],
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
            topic_id: KafkaUuid::ZERO,
            partition_responses: vec![
                <PartitionProduceResponse as TestInstance>::test_null_optionals(),
            ],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            name: KafkaString::default(),
            topic_id: KafkaUuid::ZERO,
            partition_responses: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            name: KafkaString::from("test-2".to_owned()),
            topic_id: KafkaUuid::from_parts(2, 3),
            partition_responses: vec![
                <PartitionProduceResponse as TestInstance>::test_populated(),
                <PartitionProduceResponse as TestInstance>::test_multi_element_collections(),
            ],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            name: KafkaString::from("boundary".to_owned()),
            topic_id: KafkaUuid::ONE,
            partition_responses: vec![
                <PartitionProduceResponse as TestInstance>::test_numeric_boundaries(),
            ],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            name: KafkaString::from("test".to_owned()),
            topic_id: KafkaUuid::ONE,
            partition_responses: vec![
                <PartitionProduceResponse as TestInstance>::test_tagged_fields(),
            ],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for PartitionProduceResponse {
    fn test_populated() -> Self {
        Self {
            index: 12345_i32,
            error_code: 42_i16,
            base_offset: 9_876_543_210_i64,
            log_append_time_ms: 9_876_543_210_i64,
            log_start_offset: 9_876_543_210_i64,
            record_errors: vec![<BatchIndexAndErrorMessage as TestInstance>::test_populated()],
            error_message: Some(KafkaString::from("test".to_owned())),
            current_leader: <LeaderIdAndEpoch as TestInstance>::test_populated(),
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
            base_offset: 0_i64,
            log_append_time_ms: 0_i64,
            log_start_offset: 0_i64,
            record_errors: vec![<BatchIndexAndErrorMessage as TestInstance>::test_null_optionals()],
            error_message: None,
            current_leader: LeaderIdAndEpoch::default(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            index: 0_i32,
            error_code: 0_i16,
            base_offset: 0_i64,
            log_append_time_ms: 0_i64,
            log_start_offset: 0_i64,
            record_errors: Vec::new(),
            error_message: Some(KafkaString::default()),
            current_leader: <LeaderIdAndEpoch as TestInstance>::test_null_optionals(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            index: 23456_i32,
            error_code: 43_i16,
            base_offset: 9_876_543_211_i64,
            log_append_time_ms: 9_876_543_211_i64,
            log_start_offset: 9_876_543_211_i64,
            record_errors: vec![
                <BatchIndexAndErrorMessage as TestInstance>::test_populated(),
                <BatchIndexAndErrorMessage as TestInstance>::test_multi_element_collections(),
            ],
            error_message: Some(KafkaString::from("test-2".to_owned())),
            current_leader: <LeaderIdAndEpoch as TestInstance>::test_multi_element_collections(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            index: i32::MIN,
            error_code: i16::MIN,
            base_offset: i64::MIN,
            log_append_time_ms: i64::MIN,
            log_start_offset: i64::MIN,
            record_errors: vec![
                <BatchIndexAndErrorMessage as TestInstance>::test_numeric_boundaries(),
            ],
            error_message: Some(KafkaString::from("boundary".to_owned())),
            current_leader: <LeaderIdAndEpoch as TestInstance>::test_numeric_boundaries(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            index: 12345_i32,
            error_code: 42_i16,
            base_offset: 9_876_543_210_i64,
            log_append_time_ms: 9_876_543_210_i64,
            log_start_offset: 9_876_543_210_i64,
            record_errors: vec![<BatchIndexAndErrorMessage as TestInstance>::test_tagged_fields()],
            error_message: Some(KafkaString::from("test".to_owned())),
            current_leader: <LeaderIdAndEpoch as TestInstance>::test_tagged_fields(),
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for BatchIndexAndErrorMessage {
    fn test_populated() -> Self {
        Self {
            batch_index: 12345_i32,
            batch_index_error_message: Some(KafkaString::from("test".to_owned())),
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals() -> Self {
        drop(Self::default());
        Self {
            batch_index: 0_i32,
            batch_index_error_message: None,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            batch_index: 0_i32,
            batch_index_error_message: Some(KafkaString::default()),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            batch_index: 23456_i32,
            batch_index_error_message: Some(KafkaString::from("test-2".to_owned())),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            batch_index: i32::MIN,
            batch_index_error_message: Some(KafkaString::from("boundary".to_owned())),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
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
    let message = <ProduceResponseData as TestInstance>::test_populated();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_null_optionals(version: i16) -> crate::MatrixResult<String> {
    let message = <ProduceResponseData as TestInstance>::test_null_optionals();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_empty_collections(version: i16) -> crate::MatrixResult<String> {
    let message = <ProduceResponseData as TestInstance>::test_empty_collections();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_multi_element_collections(version: i16) -> crate::MatrixResult<String> {
    let message = <ProduceResponseData as TestInstance>::test_multi_element_collections();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_numeric_boundaries(version: i16) -> crate::MatrixResult<String> {
    let message = <ProduceResponseData as TestInstance>::test_numeric_boundaries();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_tagged_fields(version: i16) -> crate::MatrixResult<String> {
    let message = <ProduceResponseData as TestInstance>::test_tagged_fields();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
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
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 4i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 5i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 6i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 7i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 8i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 9i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 10i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 11i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 12i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 13i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 3i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 4i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 5i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 6i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 7i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 8i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 9i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 10i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 11i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 12i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 13i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 3i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 3i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 3i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 3i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 4i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 4i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 4i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 4i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 5i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 5i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 5i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 5i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 6i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 6i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 6i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 6i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 7i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 7i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 7i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 7i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 8i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 8i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 8i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 8i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 9i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 9i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 9i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 9i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 10i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 10i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 10i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 10i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 11i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 11i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 11i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 11i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 12i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 12i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 12i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 12i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 13i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 13i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 13i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ProduceResponse",
        java_class: "org.apache.kafka.common.message.ProduceResponseData",
        version: 13i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
];
pub(crate) fn append_protocol_cases(cases: &mut Vec<crate::MatrixCase>) {
    cases.extend_from_slice(MATRIX_CASES);
}
