use bytes::{Bytes, BytesMut};
use kacrab_protocol::{generated::list_partition_reassignments_response::*, *};

use crate::TestInstance;

impl TestInstance for ListPartitionReassignmentsResponseData {
    fn test_populated() -> Self {
        Self {
            throttle_time_ms: 12345_i32,
            error_code: 42_i16,
            error_message: Some(KafkaString::from("test".to_owned())),
            topics: vec![<OngoingTopicReassignment as TestInstance>::test_populated()],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals() -> Self {
        Self {
            throttle_time_ms: 0_i32,
            error_code: 0_i16,
            error_message: None,
            topics: vec![<OngoingTopicReassignment as TestInstance>::test_null_optionals()],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            throttle_time_ms: 0_i32,
            error_code: 0_i16,
            error_message: Some(KafkaString::default()),
            topics: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            throttle_time_ms: 23456_i32,
            error_code: 43_i16,
            error_message: Some(KafkaString::from("test-2".to_owned())),
            topics: vec![
                <OngoingTopicReassignment as TestInstance>::test_populated(),
                <OngoingTopicReassignment as TestInstance>::test_multi_element_collections(),
            ],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            throttle_time_ms: i32::MIN,
            error_code: i16::MIN,
            error_message: Some(KafkaString::from("boundary".to_owned())),
            topics: vec![<OngoingTopicReassignment as TestInstance>::test_numeric_boundaries()],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            throttle_time_ms: 12345_i32,
            error_code: 42_i16,
            error_message: Some(KafkaString::from("test".to_owned())),
            topics: vec![<OngoingTopicReassignment as TestInstance>::test_tagged_fields()],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for OngoingTopicReassignment {
    fn test_populated() -> Self {
        Self {
            name: KafkaString::from("test".to_owned()),
            partitions: vec![<OngoingPartitionReassignment as TestInstance>::test_populated()],
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
            partitions: vec![<OngoingPartitionReassignment as TestInstance>::test_null_optionals()],
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
                <OngoingPartitionReassignment as TestInstance>::test_populated(),
                <OngoingPartitionReassignment as TestInstance>::test_multi_element_collections(),
            ],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            name: KafkaString::from("boundary".to_owned()),
            partitions: vec![
                <OngoingPartitionReassignment as TestInstance>::test_numeric_boundaries(),
            ],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            name: KafkaString::from("test".to_owned()),
            partitions: vec![<OngoingPartitionReassignment as TestInstance>::test_tagged_fields()],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for OngoingPartitionReassignment {
    fn test_populated() -> Self {
        Self {
            partition_index: 12345_i32,
            replicas: vec![12345_i32],
            adding_replicas: vec![12345_i32],
            removing_replicas: vec![12345_i32],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals() -> Self {
        drop(Self::default());
        Self {
            partition_index: 0_i32,
            replicas: vec![0_i32],
            adding_replicas: vec![0_i32],
            removing_replicas: vec![0_i32],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            partition_index: 0_i32,
            replicas: Vec::new(),
            adding_replicas: Vec::new(),
            removing_replicas: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            partition_index: 23456_i32,
            replicas: vec![12345_i32, 23456_i32],
            adding_replicas: vec![12345_i32, 23456_i32],
            removing_replicas: vec![12345_i32, 23456_i32],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            partition_index: i32::MIN,
            replicas: vec![i32::MIN],
            adding_replicas: vec![i32::MIN],
            removing_replicas: vec![i32::MIN],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            partition_index: 12345_i32,
            replicas: vec![12345_i32],
            adding_replicas: vec![12345_i32],
            removing_replicas: vec![12345_i32],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
fn encode_populated(version: i16) -> crate::MatrixResult<String> {
    let message = <ListPartitionReassignmentsResponseData as TestInstance>::test_populated();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_null_optionals(version: i16) -> crate::MatrixResult<String> {
    let message = <ListPartitionReassignmentsResponseData as TestInstance>::test_null_optionals();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_empty_collections(version: i16) -> crate::MatrixResult<String> {
    let message =
        <ListPartitionReassignmentsResponseData as TestInstance>::test_empty_collections();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_multi_element_collections(version: i16) -> crate::MatrixResult<String> {
    let message =
        <ListPartitionReassignmentsResponseData as TestInstance>::test_multi_element_collections();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_numeric_boundaries(version: i16) -> crate::MatrixResult<String> {
    let message =
        <ListPartitionReassignmentsResponseData as TestInstance>::test_numeric_boundaries();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_tagged_fields(version: i16) -> crate::MatrixResult<String> {
    let message = <ListPartitionReassignmentsResponseData as TestInstance>::test_tagged_fields();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn reencode(version: i16, hex_input: &str) -> crate::MatrixResult<String> {
    let mut input = Bytes::from(crate::decode_hex(hex_input)?);
    let message = ListPartitionReassignmentsResponseData::read(&mut input, version)?;
    crate::ensure_input_consumed(&input)?;
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
const MATRIX_CASES: &[crate::MatrixCase] = &[
    crate::MatrixCase {
        schema_name: "ListPartitionReassignmentsResponse",
        java_class: "org.apache.kafka.common.message.ListPartitionReassignmentsResponseData",
        version: 0i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ListPartitionReassignmentsResponse",
        java_class: "org.apache.kafka.common.message.ListPartitionReassignmentsResponseData",
        version: 0i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ListPartitionReassignmentsResponse",
        java_class: "org.apache.kafka.common.message.ListPartitionReassignmentsResponseData",
        version: 0i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ListPartitionReassignmentsResponse",
        java_class: "org.apache.kafka.common.message.ListPartitionReassignmentsResponseData",
        version: 0i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ListPartitionReassignmentsResponse",
        java_class: "org.apache.kafka.common.message.ListPartitionReassignmentsResponseData",
        version: 0i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ListPartitionReassignmentsResponse",
        java_class: "org.apache.kafka.common.message.ListPartitionReassignmentsResponseData",
        version: 0i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
];
pub(crate) fn append_protocol_cases(cases: &mut Vec<crate::MatrixCase>) {
    cases.extend_from_slice(MATRIX_CASES);
}
