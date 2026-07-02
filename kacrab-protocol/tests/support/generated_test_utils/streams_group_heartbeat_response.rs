#![allow(
    clippy::all,
    clippy::pedantic,
    clippy::nursery,
    reason = "Generated test fixtures mirror Kafka's schema shape and trade hand-written lint \
              style for reproducible output, matching the generated protocol modules."
)]
use bytes::{Bytes, BytesMut};
use kacrab_protocol::{generated::streams_group_heartbeat_response::*, *};

use crate::TestInstance;

impl TestInstance for StreamsGroupHeartbeatResponseData {
    fn test_populated(version: i16) -> Self {
        Self {
            throttle_time_ms: 12345_i32,
            error_code: 42_i16,
            error_message: Some(KafkaString::from("test".to_owned())),
            member_id: KafkaString::from("test".to_owned()),
            member_epoch: 12345_i32,
            heartbeat_interval_ms: 12345_i32,
            acceptable_recovery_lag: 12345_i32,
            task_offset_interval_ms: 12345_i32,
            status: Some(vec![<Status as TestInstance>::test_populated(version)]),
            active_tasks: Some(vec![<TaskIds as TestInstance>::test_populated(version)]),
            standby_tasks: Some(vec![<TaskIds as TestInstance>::test_populated(version)]),
            warmup_tasks: Some(vec![<TaskIds as TestInstance>::test_populated(version)]),
            endpoint_information_epoch: 12345_i32,
            partitions_by_user_endpoint: Some(vec![
                <EndpointToPartitions as TestInstance>::test_populated(version),
            ]),
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals(version: i16) -> Self {
        drop(<Status as TestInstance>::test_null_optionals(version));
        drop(<TaskIds as TestInstance>::test_null_optionals(version));
        drop(<TaskIds as TestInstance>::test_null_optionals(version));
        drop(<TaskIds as TestInstance>::test_null_optionals(version));
        drop(<EndpointToPartitions as TestInstance>::test_null_optionals(
            version,
        ));
        Self {
            throttle_time_ms: 0_i32,
            error_code: 0_i16,
            error_message: None,
            member_id: KafkaString::default(),
            member_epoch: 0_i32,
            heartbeat_interval_ms: 0_i32,
            acceptable_recovery_lag: 0_i32,
            task_offset_interval_ms: 0_i32,
            status: None,
            active_tasks: None,
            standby_tasks: None,
            warmup_tasks: None,
            endpoint_information_epoch: 0_i32,
            partitions_by_user_endpoint: None,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections(_version: i16) -> Self {
        Self {
            throttle_time_ms: 0_i32,
            error_code: 0_i16,
            error_message: Some(KafkaString::default()),
            member_id: KafkaString::default(),
            member_epoch: 0_i32,
            heartbeat_interval_ms: 0_i32,
            acceptable_recovery_lag: 0_i32,
            task_offset_interval_ms: 0_i32,
            status: Some(Vec::new()),
            active_tasks: Some(Vec::new()),
            standby_tasks: Some(Vec::new()),
            warmup_tasks: Some(Vec::new()),
            endpoint_information_epoch: 0_i32,
            partitions_by_user_endpoint: Some(Vec::new()),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections(version: i16) -> Self {
        Self {
            throttle_time_ms: 23456_i32,
            error_code: 43_i16,
            error_message: Some(KafkaString::from("test-2".to_owned())),
            member_id: KafkaString::from("test-2".to_owned()),
            member_epoch: 23456_i32,
            heartbeat_interval_ms: 23456_i32,
            acceptable_recovery_lag: 23456_i32,
            task_offset_interval_ms: 23456_i32,
            status: Some(vec![
                <Status as TestInstance>::test_populated(version),
                <Status as TestInstance>::test_multi_element_collections(version),
            ]),
            active_tasks: Some(vec![
                <TaskIds as TestInstance>::test_populated(version),
                <TaskIds as TestInstance>::test_multi_element_collections(version),
            ]),
            standby_tasks: Some(vec![
                <TaskIds as TestInstance>::test_populated(version),
                <TaskIds as TestInstance>::test_multi_element_collections(version),
            ]),
            warmup_tasks: Some(vec![
                <TaskIds as TestInstance>::test_populated(version),
                <TaskIds as TestInstance>::test_multi_element_collections(version),
            ]),
            endpoint_information_epoch: 23456_i32,
            partitions_by_user_endpoint: Some(vec![
                <EndpointToPartitions as TestInstance>::test_populated(version),
                <EndpointToPartitions as TestInstance>::test_multi_element_collections(version),
            ]),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries(version: i16) -> Self {
        Self {
            throttle_time_ms: i32::MIN,
            error_code: i16::MIN,
            error_message: Some(KafkaString::from("boundary".to_owned())),
            member_id: KafkaString::from("boundary".to_owned()),
            member_epoch: i32::MIN,
            heartbeat_interval_ms: i32::MIN,
            acceptable_recovery_lag: i32::MIN,
            task_offset_interval_ms: i32::MIN,
            status: Some(vec![<Status as TestInstance>::test_numeric_boundaries(
                version,
            )]),
            active_tasks: Some(vec![<TaskIds as TestInstance>::test_numeric_boundaries(
                version,
            )]),
            standby_tasks: Some(vec![<TaskIds as TestInstance>::test_numeric_boundaries(
                version,
            )]),
            warmup_tasks: Some(vec![<TaskIds as TestInstance>::test_numeric_boundaries(
                version,
            )]),
            endpoint_information_epoch: i32::MIN,
            partitions_by_user_endpoint: Some(vec![
                <EndpointToPartitions as TestInstance>::test_numeric_boundaries(version),
            ]),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields(version: i16) -> Self {
        Self {
            throttle_time_ms: 12345_i32,
            error_code: 42_i16,
            error_message: Some(KafkaString::from("test".to_owned())),
            member_id: KafkaString::from("test".to_owned()),
            member_epoch: 12345_i32,
            heartbeat_interval_ms: 12345_i32,
            acceptable_recovery_lag: 12345_i32,
            task_offset_interval_ms: 12345_i32,
            status: Some(vec![<Status as TestInstance>::test_tagged_fields(version)]),
            active_tasks: Some(vec![<TaskIds as TestInstance>::test_tagged_fields(version)]),
            standby_tasks: Some(vec![<TaskIds as TestInstance>::test_tagged_fields(version)]),
            warmup_tasks: Some(vec![<TaskIds as TestInstance>::test_tagged_fields(version)]),
            endpoint_information_epoch: 12345_i32,
            partitions_by_user_endpoint: Some(vec![
                <EndpointToPartitions as TestInstance>::test_tagged_fields(version),
            ]),
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for EndpointToPartitions {
    fn test_populated(version: i16) -> Self {
        Self {
            user_endpoint: <Endpoint as TestInstance>::test_populated(version),
            active_partitions: vec![<TopicPartition as TestInstance>::test_populated(version)],
            standby_partitions: vec![<TopicPartition as TestInstance>::test_populated(version)],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals(version: i16) -> Self {
        drop(Self::default());
        Self {
            user_endpoint: <Endpoint as TestInstance>::test_null_optionals(version),
            active_partitions: vec![<TopicPartition as TestInstance>::test_null_optionals(
                version,
            )],
            standby_partitions: vec![<TopicPartition as TestInstance>::test_null_optionals(
                version,
            )],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections(version: i16) -> Self {
        Self {
            user_endpoint: <Endpoint as TestInstance>::test_null_optionals(version),
            active_partitions: Vec::new(),
            standby_partitions: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections(version: i16) -> Self {
        Self {
            user_endpoint: <Endpoint as TestInstance>::test_multi_element_collections(version),
            active_partitions: vec![
                <TopicPartition as TestInstance>::test_populated(version),
                <TopicPartition as TestInstance>::test_multi_element_collections(version),
            ],
            standby_partitions: vec![
                <TopicPartition as TestInstance>::test_populated(version),
                <TopicPartition as TestInstance>::test_multi_element_collections(version),
            ],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries(version: i16) -> Self {
        Self {
            user_endpoint: <Endpoint as TestInstance>::test_numeric_boundaries(version),
            active_partitions: vec![<TopicPartition as TestInstance>::test_numeric_boundaries(
                version,
            )],
            standby_partitions: vec![<TopicPartition as TestInstance>::test_numeric_boundaries(
                version,
            )],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields(version: i16) -> Self {
        Self {
            user_endpoint: <Endpoint as TestInstance>::test_tagged_fields(version),
            active_partitions: vec![<TopicPartition as TestInstance>::test_tagged_fields(
                version,
            )],
            standby_partitions: vec![<TopicPartition as TestInstance>::test_tagged_fields(
                version,
            )],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for Status {
    fn test_populated(_version: i16) -> Self {
        Self {
            status_code: 7_i8,
            status_detail: KafkaString::from("test".to_owned()),
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals(_version: i16) -> Self {
        drop(Self::default());
        Self {
            status_code: 0_i8,
            status_detail: KafkaString::default(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections(_version: i16) -> Self {
        Self {
            status_code: 0_i8,
            status_detail: KafkaString::default(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections(_version: i16) -> Self {
        Self {
            status_code: 8_i8,
            status_detail: KafkaString::from("test-2".to_owned()),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries(_version: i16) -> Self {
        Self {
            status_code: i8::MIN,
            status_detail: KafkaString::from("boundary".to_owned()),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields(_version: i16) -> Self {
        Self {
            status_code: 7_i8,
            status_detail: KafkaString::from("test".to_owned()),
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for TopicPartition {
    fn test_populated(_version: i16) -> Self {
        Self {
            topic: KafkaString::from("test".to_owned()),
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
            partitions: vec![0_i32],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections(_version: i16) -> Self {
        Self {
            topic: KafkaString::default(),
            partitions: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections(_version: i16) -> Self {
        Self {
            topic: KafkaString::from("test-2".to_owned()),
            partitions: vec![12345_i32, 23456_i32],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries(_version: i16) -> Self {
        Self {
            topic: KafkaString::from("boundary".to_owned()),
            partitions: vec![i32::MIN],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields(_version: i16) -> Self {
        Self {
            topic: KafkaString::from("test".to_owned()),
            partitions: vec![12345_i32],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for TaskIds {
    fn test_populated(_version: i16) -> Self {
        Self {
            subtopology_id: KafkaString::from("test".to_owned()),
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
            subtopology_id: KafkaString::default(),
            partitions: vec![0_i32],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections(_version: i16) -> Self {
        Self {
            subtopology_id: KafkaString::default(),
            partitions: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections(_version: i16) -> Self {
        Self {
            subtopology_id: KafkaString::from("test-2".to_owned()),
            partitions: vec![12345_i32, 23456_i32],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries(_version: i16) -> Self {
        Self {
            subtopology_id: KafkaString::from("boundary".to_owned()),
            partitions: vec![i32::MIN],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields(_version: i16) -> Self {
        Self {
            subtopology_id: KafkaString::from("test".to_owned()),
            partitions: vec![12345_i32],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for Endpoint {
    fn test_populated(_version: i16) -> Self {
        Self {
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
            host: KafkaString::default(),
            port: 0_u16,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections(_version: i16) -> Self {
        Self {
            host: KafkaString::default(),
            port: 0_u16,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections(_version: i16) -> Self {
        Self {
            host: KafkaString::from("test-2".to_owned()),
            port: 43_u16,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries(_version: i16) -> Self {
        Self {
            host: KafkaString::from("boundary".to_owned()),
            port: u16::MAX,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields(_version: i16) -> Self {
        Self {
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
    let message = <StreamsGroupHeartbeatResponseData as TestInstance>::test_populated(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_populated(version: i16) -> crate::MatrixResult<usize> {
    let message = <StreamsGroupHeartbeatResponseData as TestInstance>::test_populated(version);
    Ok(message.encoded_len(version)?)
}
fn encode_null_optionals(version: i16) -> crate::MatrixResult<String> {
    let message = <StreamsGroupHeartbeatResponseData as TestInstance>::test_null_optionals(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_null_optionals(version: i16) -> crate::MatrixResult<usize> {
    let message = <StreamsGroupHeartbeatResponseData as TestInstance>::test_null_optionals(version);
    Ok(message.encoded_len(version)?)
}
fn encode_empty_collections(version: i16) -> crate::MatrixResult<String> {
    let message =
        <StreamsGroupHeartbeatResponseData as TestInstance>::test_empty_collections(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_empty_collections(version: i16) -> crate::MatrixResult<usize> {
    let message =
        <StreamsGroupHeartbeatResponseData as TestInstance>::test_empty_collections(version);
    Ok(message.encoded_len(version)?)
}
fn encode_multi_element_collections(version: i16) -> crate::MatrixResult<String> {
    let message =
        <StreamsGroupHeartbeatResponseData as TestInstance>::test_multi_element_collections(
            version,
        );
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_multi_element_collections(version: i16) -> crate::MatrixResult<usize> {
    let message =
        <StreamsGroupHeartbeatResponseData as TestInstance>::test_multi_element_collections(
            version,
        );
    Ok(message.encoded_len(version)?)
}
fn encode_numeric_boundaries(version: i16) -> crate::MatrixResult<String> {
    let message =
        <StreamsGroupHeartbeatResponseData as TestInstance>::test_numeric_boundaries(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_numeric_boundaries(version: i16) -> crate::MatrixResult<usize> {
    let message =
        <StreamsGroupHeartbeatResponseData as TestInstance>::test_numeric_boundaries(version);
    Ok(message.encoded_len(version)?)
}
fn encode_tagged_fields(version: i16) -> crate::MatrixResult<String> {
    let message = <StreamsGroupHeartbeatResponseData as TestInstance>::test_tagged_fields(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_tagged_fields(version: i16) -> crate::MatrixResult<usize> {
    let message = <StreamsGroupHeartbeatResponseData as TestInstance>::test_tagged_fields(version);
    Ok(message.encoded_len(version)?)
}
fn reencode(version: i16, hex_input: &str) -> crate::MatrixResult<String> {
    let mut input = Bytes::from(crate::decode_hex(hex_input)?);
    let message = StreamsGroupHeartbeatResponseData::read(&mut input, version)?;
    crate::ensure_input_consumed(&input)?;
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
const MATRIX_CASES: &[crate::MatrixCase] = &[
    crate::MatrixCase {
        schema_name: "StreamsGroupHeartbeatResponse",
        java_class: "org.apache.kafka.common.message.StreamsGroupHeartbeatResponseData",
        version: 0i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "StreamsGroupHeartbeatResponse",
        java_class: "org.apache.kafka.common.message.StreamsGroupHeartbeatResponseData",
        version: 0i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "StreamsGroupHeartbeatResponse",
        java_class: "org.apache.kafka.common.message.StreamsGroupHeartbeatResponseData",
        version: 0i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "StreamsGroupHeartbeatResponse",
        java_class: "org.apache.kafka.common.message.StreamsGroupHeartbeatResponseData",
        version: 0i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "StreamsGroupHeartbeatResponse",
        java_class: "org.apache.kafka.common.message.StreamsGroupHeartbeatResponseData",
        version: 0i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "StreamsGroupHeartbeatResponse",
        java_class: "org.apache.kafka.common.message.StreamsGroupHeartbeatResponseData",
        version: 0i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
];
pub(crate) fn append_protocol_cases(cases: &mut Vec<crate::MatrixCase>) {
    cases.extend_from_slice(MATRIX_CASES);
}
