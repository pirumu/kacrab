use bytes::{Bytes, BytesMut};
use kacrab_protocol::{generated::metadata_response::*, *};

use crate::TestInstance;

impl TestInstance for MetadataResponseData {
    fn test_populated() -> Self {
        Self {
            throttle_time_ms: 12345_i32,
            brokers: vec![<MetadataResponseBroker as TestInstance>::test_populated()],
            cluster_id: Some(KafkaString::from("test".to_owned())),
            controller_id: 12345_i32,
            topics: vec![<MetadataResponseTopic as TestInstance>::test_populated()],
            cluster_authorized_operations: 12345_i32,
            error_code: 42_i16,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals() -> Self {
        Self {
            throttle_time_ms: 0_i32,
            brokers: vec![<MetadataResponseBroker as TestInstance>::test_null_optionals()],
            cluster_id: None,
            controller_id: 0_i32,
            topics: vec![<MetadataResponseTopic as TestInstance>::test_null_optionals()],
            cluster_authorized_operations: 0_i32,
            error_code: 0_i16,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            throttle_time_ms: 0_i32,
            brokers: Vec::new(),
            cluster_id: Some(KafkaString::default()),
            controller_id: 0_i32,
            topics: Vec::new(),
            cluster_authorized_operations: 0_i32,
            error_code: 0_i16,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            throttle_time_ms: 23456_i32,
            brokers: vec![
                <MetadataResponseBroker as TestInstance>::test_populated(),
                <MetadataResponseBroker as TestInstance>::test_multi_element_collections(),
            ],
            cluster_id: Some(KafkaString::from("test-2".to_owned())),
            controller_id: 23456_i32,
            topics: vec![
                <MetadataResponseTopic as TestInstance>::test_populated(),
                <MetadataResponseTopic as TestInstance>::test_multi_element_collections(),
            ],
            cluster_authorized_operations: 23456_i32,
            error_code: 43_i16,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            throttle_time_ms: i32::MIN,
            brokers: vec![<MetadataResponseBroker as TestInstance>::test_numeric_boundaries()],
            cluster_id: Some(KafkaString::from("boundary".to_owned())),
            controller_id: i32::MIN,
            topics: vec![<MetadataResponseTopic as TestInstance>::test_numeric_boundaries()],
            cluster_authorized_operations: i32::MIN,
            error_code: i16::MIN,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            throttle_time_ms: 12345_i32,
            brokers: vec![<MetadataResponseBroker as TestInstance>::test_tagged_fields()],
            cluster_id: Some(KafkaString::from("test".to_owned())),
            controller_id: 12345_i32,
            topics: vec![<MetadataResponseTopic as TestInstance>::test_tagged_fields()],
            cluster_authorized_operations: 12345_i32,
            error_code: 42_i16,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for MetadataResponseBroker {
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
impl TestInstance for MetadataResponseTopic {
    fn test_populated() -> Self {
        Self {
            error_code: 42_i16,
            name: Some(KafkaString::from("test".to_owned())),
            topic_id: KafkaUuid::ONE,
            is_internal: true,
            partitions: vec![<MetadataResponsePartition as TestInstance>::test_populated()],
            topic_authorized_operations: 12345_i32,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals() -> Self {
        drop(Self::default());
        Self {
            error_code: 0_i16,
            name: None,
            topic_id: KafkaUuid::ZERO,
            is_internal: false,
            partitions: vec![<MetadataResponsePartition as TestInstance>::test_null_optionals()],
            topic_authorized_operations: 0_i32,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            error_code: 0_i16,
            name: Some(KafkaString::default()),
            topic_id: KafkaUuid::ZERO,
            is_internal: false,
            partitions: Vec::new(),
            topic_authorized_operations: 0_i32,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            error_code: 43_i16,
            name: Some(KafkaString::from("test-2".to_owned())),
            topic_id: KafkaUuid::from_parts(2, 3),
            is_internal: false,
            partitions: vec![
                <MetadataResponsePartition as TestInstance>::test_populated(),
                <MetadataResponsePartition as TestInstance>::test_multi_element_collections(),
            ],
            topic_authorized_operations: 23456_i32,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            error_code: i16::MIN,
            name: Some(KafkaString::from("boundary".to_owned())),
            topic_id: KafkaUuid::ONE,
            is_internal: true,
            partitions: vec![
                <MetadataResponsePartition as TestInstance>::test_numeric_boundaries(),
            ],
            topic_authorized_operations: i32::MIN,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            error_code: 42_i16,
            name: Some(KafkaString::from("test".to_owned())),
            topic_id: KafkaUuid::ONE,
            is_internal: true,
            partitions: vec![<MetadataResponsePartition as TestInstance>::test_tagged_fields()],
            topic_authorized_operations: 12345_i32,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for MetadataResponsePartition {
    fn test_populated() -> Self {
        Self {
            error_code: 42_i16,
            partition_index: 12345_i32,
            leader_id: 12345_i32,
            leader_epoch: 12345_i32,
            replica_nodes: vec![12345_i32],
            isr_nodes: vec![12345_i32],
            offline_replicas: vec![12345_i32],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals() -> Self {
        drop(Self::default());
        Self {
            error_code: 0_i16,
            partition_index: 0_i32,
            leader_id: 0_i32,
            leader_epoch: 0_i32,
            replica_nodes: vec![0_i32],
            isr_nodes: vec![0_i32],
            offline_replicas: vec![0_i32],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            error_code: 0_i16,
            partition_index: 0_i32,
            leader_id: 0_i32,
            leader_epoch: 0_i32,
            replica_nodes: Vec::new(),
            isr_nodes: Vec::new(),
            offline_replicas: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            error_code: 43_i16,
            partition_index: 23456_i32,
            leader_id: 23456_i32,
            leader_epoch: 23456_i32,
            replica_nodes: vec![12345_i32, 23456_i32],
            isr_nodes: vec![12345_i32, 23456_i32],
            offline_replicas: vec![12345_i32, 23456_i32],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            error_code: i16::MIN,
            partition_index: i32::MIN,
            leader_id: i32::MIN,
            leader_epoch: i32::MIN,
            replica_nodes: vec![i32::MIN],
            isr_nodes: vec![i32::MIN],
            offline_replicas: vec![i32::MIN],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            error_code: 42_i16,
            partition_index: 12345_i32,
            leader_id: 12345_i32,
            leader_epoch: 12345_i32,
            replica_nodes: vec![12345_i32],
            isr_nodes: vec![12345_i32],
            offline_replicas: vec![12345_i32],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
fn encode_populated(version: i16) -> crate::MatrixResult<String> {
    let message = <MetadataResponseData as TestInstance>::test_populated();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_null_optionals(version: i16) -> crate::MatrixResult<String> {
    let message = <MetadataResponseData as TestInstance>::test_null_optionals();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_empty_collections(version: i16) -> crate::MatrixResult<String> {
    let message = <MetadataResponseData as TestInstance>::test_empty_collections();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_multi_element_collections(version: i16) -> crate::MatrixResult<String> {
    let message = <MetadataResponseData as TestInstance>::test_multi_element_collections();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_numeric_boundaries(version: i16) -> crate::MatrixResult<String> {
    let message = <MetadataResponseData as TestInstance>::test_numeric_boundaries();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_tagged_fields(version: i16) -> crate::MatrixResult<String> {
    let message = <MetadataResponseData as TestInstance>::test_tagged_fields();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn reencode(version: i16, hex_input: &str) -> crate::MatrixResult<String> {
    let mut input = Bytes::from(crate::decode_hex(hex_input)?);
    let message = MetadataResponseData::read(&mut input, version)?;
    crate::ensure_input_consumed(&input)?;
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
const MATRIX_CASES: &[crate::MatrixCase] = &[
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 0i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 1i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 2i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 3i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 4i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 5i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 6i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 7i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 8i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 9i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 10i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 11i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 12i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 13i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 0i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 1i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 2i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 3i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 4i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 5i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 6i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 7i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 8i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 9i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 10i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 11i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 12i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 13i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 0i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 0i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 0i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 0i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 1i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 1i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 1i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 1i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 2i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 2i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 2i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 2i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 3i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 3i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 3i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 3i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 4i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 4i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 4i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 4i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 5i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 5i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 5i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 5i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 6i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 6i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 6i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 6i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 7i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 7i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 7i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 7i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 8i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 8i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 8i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 8i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 9i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 9i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 9i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 9i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 10i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 10i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 10i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 10i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 11i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 11i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 11i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 11i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 12i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 12i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 12i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 12i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 13i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 13i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 13i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 13i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
];
pub(crate) fn append_protocol_cases(cases: &mut Vec<crate::MatrixCase>) {
    cases.extend_from_slice(MATRIX_CASES);
}
