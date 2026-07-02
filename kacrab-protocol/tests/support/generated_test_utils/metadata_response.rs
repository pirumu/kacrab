#![allow(
    clippy::all,
    clippy::pedantic,
    clippy::nursery,
    reason = "Generated test fixtures mirror Kafka's schema shape and trade hand-written lint \
              style for reproducible output, matching the generated protocol modules."
)]
use bytes::{Bytes, BytesMut};
use kacrab_protocol::{generated::metadata_response::*, *};

use crate::TestInstance;

impl TestInstance for MetadataResponseData {
    fn test_populated(version: i16) -> Self {
        Self {
            throttle_time_ms: if version >= 3 { 12345_i32 } else { 0_i32 },
            brokers: vec![<MetadataResponseBroker as TestInstance>::test_populated(
                version,
            )],
            cluster_id: (version >= 2)
                .then(|| Some(KafkaString::from("test".to_owned())))
                .flatten(),
            controller_id: if version >= 1 { 12345_i32 } else { -1i32 },
            topics: vec![<MetadataResponseTopic as TestInstance>::test_populated(
                version,
            )],
            cluster_authorized_operations: if version >= 8 && version <= 10 {
                12345_i32
            } else {
                i32::MIN
            },
            error_code: if version >= 13 { 42_i16 } else { 0_i16 },
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals(version: i16) -> Self {
        Self {
            throttle_time_ms: 0_i32,
            brokers: vec![<MetadataResponseBroker as TestInstance>::test_null_optionals(version)],
            cluster_id: None,
            controller_id: if version >= 1 { 0_i32 } else { -1i32 },
            topics: vec![<MetadataResponseTopic as TestInstance>::test_null_optionals(version)],
            cluster_authorized_operations: if version >= 8 && version <= 10 {
                0_i32
            } else {
                i32::MIN
            },
            error_code: 0_i16,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections(version: i16) -> Self {
        Self {
            throttle_time_ms: 0_i32,
            brokers: Vec::new(),
            cluster_id: (version >= 2)
                .then(|| Some(KafkaString::default()))
                .flatten(),
            controller_id: if version >= 1 { 0_i32 } else { -1i32 },
            topics: Vec::new(),
            cluster_authorized_operations: if version >= 8 && version <= 10 {
                0_i32
            } else {
                i32::MIN
            },
            error_code: 0_i16,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections(version: i16) -> Self {
        Self {
            throttle_time_ms: if version >= 3 { 23456_i32 } else { 0_i32 },
            brokers: vec![
                <MetadataResponseBroker as TestInstance>::test_populated(version),
                <MetadataResponseBroker as TestInstance>::test_multi_element_collections(version),
            ],
            cluster_id: (version >= 2)
                .then(|| Some(KafkaString::from("test-2".to_owned())))
                .flatten(),
            controller_id: if version >= 1 { 23456_i32 } else { -1i32 },
            topics: vec![
                <MetadataResponseTopic as TestInstance>::test_populated(version),
                <MetadataResponseTopic as TestInstance>::test_multi_element_collections(version),
            ],
            cluster_authorized_operations: if version >= 8 && version <= 10 {
                23456_i32
            } else {
                i32::MIN
            },
            error_code: if version >= 13 { 43_i16 } else { 0_i16 },
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries(version: i16) -> Self {
        Self {
            throttle_time_ms: if version >= 3 { i32::MIN } else { 0_i32 },
            brokers: vec![
                <MetadataResponseBroker as TestInstance>::test_numeric_boundaries(version),
            ],
            cluster_id: (version >= 2)
                .then(|| Some(KafkaString::from("boundary".to_owned())))
                .flatten(),
            controller_id: if version >= 1 { i32::MIN } else { -1i32 },
            topics: vec![<MetadataResponseTopic as TestInstance>::test_numeric_boundaries(version)],
            cluster_authorized_operations: i32::MIN,
            error_code: if version >= 13 { i16::MIN } else { 0_i16 },
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields(version: i16) -> Self {
        Self {
            throttle_time_ms: if version >= 3 { 12345_i32 } else { 0_i32 },
            brokers: vec![<MetadataResponseBroker as TestInstance>::test_tagged_fields(version)],
            cluster_id: (version >= 2)
                .then(|| Some(KafkaString::from("test".to_owned())))
                .flatten(),
            controller_id: if version >= 1 { 12345_i32 } else { -1i32 },
            topics: vec![<MetadataResponseTopic as TestInstance>::test_tagged_fields(
                version,
            )],
            cluster_authorized_operations: if version >= 8 && version <= 10 {
                12345_i32
            } else {
                i32::MIN
            },
            error_code: if version >= 13 { 42_i16 } else { 0_i16 },
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for MetadataResponseBroker {
    fn test_populated(version: i16) -> Self {
        Self {
            node_id: 12345_i32,
            host: KafkaString::from("test".to_owned()),
            port: 12345_i32,
            rack: (version >= 1)
                .then(|| Some(KafkaString::from("test".to_owned())))
                .flatten(),
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
    fn test_empty_collections(version: i16) -> Self {
        Self {
            node_id: 0_i32,
            host: KafkaString::default(),
            port: 0_i32,
            rack: (version >= 1)
                .then(|| Some(KafkaString::default()))
                .flatten(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections(version: i16) -> Self {
        Self {
            node_id: 23456_i32,
            host: KafkaString::from("test-2".to_owned()),
            port: 23456_i32,
            rack: (version >= 1)
                .then(|| Some(KafkaString::from("test-2".to_owned())))
                .flatten(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries(version: i16) -> Self {
        Self {
            node_id: i32::MIN,
            host: KafkaString::from("boundary".to_owned()),
            port: i32::MIN,
            rack: (version >= 1)
                .then(|| Some(KafkaString::from("boundary".to_owned())))
                .flatten(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields(version: i16) -> Self {
        Self {
            node_id: 12345_i32,
            host: KafkaString::from("test".to_owned()),
            port: 12345_i32,
            rack: (version >= 1)
                .then(|| Some(KafkaString::from("test".to_owned())))
                .flatten(),
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for MetadataResponseTopic {
    fn test_populated(version: i16) -> Self {
        Self {
            error_code: 42_i16,
            name: Some(KafkaString::from("test".to_owned())),
            topic_id: if version >= 10 {
                KafkaUuid::ONE
            } else {
                KafkaUuid::ZERO
            },
            is_internal: if version >= 1 { true } else { false },
            partitions: vec![<MetadataResponsePartition as TestInstance>::test_populated(
                version,
            )],
            topic_authorized_operations: if version >= 8 { 12345_i32 } else { i32::MIN },
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals(version: i16) -> Self {
        drop(Self::default());
        Self {
            error_code: 0_i16,
            name: None,
            topic_id: KafkaUuid::ZERO,
            is_internal: false,
            partitions: vec![
                <MetadataResponsePartition as TestInstance>::test_null_optionals(version),
            ],
            topic_authorized_operations: if version >= 8 { 0_i32 } else { i32::MIN },
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections(version: i16) -> Self {
        Self {
            error_code: 0_i16,
            name: Some(KafkaString::default()),
            topic_id: KafkaUuid::ZERO,
            is_internal: false,
            partitions: Vec::new(),
            topic_authorized_operations: if version >= 8 { 0_i32 } else { i32::MIN },
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections(version: i16) -> Self {
        Self {
            error_code: 43_i16,
            name: Some(KafkaString::from("test-2".to_owned())),
            topic_id: if version >= 10 {
                KafkaUuid::from_parts(2, 3)
            } else {
                KafkaUuid::ZERO
            },
            is_internal: false,
            partitions: vec![
                <MetadataResponsePartition as TestInstance>::test_populated(version),
                <MetadataResponsePartition as TestInstance>::test_multi_element_collections(
                    version,
                ),
            ],
            topic_authorized_operations: if version >= 8 { 23456_i32 } else { i32::MIN },
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries(version: i16) -> Self {
        Self {
            error_code: i16::MIN,
            name: Some(KafkaString::from("boundary".to_owned())),
            topic_id: if version >= 10 {
                KafkaUuid::ONE
            } else {
                KafkaUuid::ZERO
            },
            is_internal: if version >= 1 { true } else { false },
            partitions: vec![
                <MetadataResponsePartition as TestInstance>::test_numeric_boundaries(version),
            ],
            topic_authorized_operations: i32::MIN,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields(version: i16) -> Self {
        Self {
            error_code: 42_i16,
            name: Some(KafkaString::from("test".to_owned())),
            topic_id: if version >= 10 {
                KafkaUuid::ONE
            } else {
                KafkaUuid::ZERO
            },
            is_internal: if version >= 1 { true } else { false },
            partitions: vec![
                <MetadataResponsePartition as TestInstance>::test_tagged_fields(version),
            ],
            topic_authorized_operations: if version >= 8 { 12345_i32 } else { i32::MIN },
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for MetadataResponsePartition {
    fn test_populated(version: i16) -> Self {
        Self {
            error_code: 42_i16,
            partition_index: 12345_i32,
            leader_id: 12345_i32,
            leader_epoch: if version >= 7 { 12345_i32 } else { -1i32 },
            replica_nodes: vec![12345_i32],
            isr_nodes: vec![12345_i32],
            offline_replicas: if version >= 5 {
                vec![12345_i32]
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
        drop(Self::default());
        Self {
            error_code: 0_i16,
            partition_index: 0_i32,
            leader_id: 0_i32,
            leader_epoch: if version >= 7 { 0_i32 } else { -1i32 },
            replica_nodes: vec![0_i32],
            isr_nodes: vec![0_i32],
            offline_replicas: if version >= 5 {
                vec![0_i32]
            } else {
                Vec::new()
            },
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections(version: i16) -> Self {
        Self {
            error_code: 0_i16,
            partition_index: 0_i32,
            leader_id: 0_i32,
            leader_epoch: if version >= 7 { 0_i32 } else { -1i32 },
            replica_nodes: Vec::new(),
            isr_nodes: Vec::new(),
            offline_replicas: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections(version: i16) -> Self {
        Self {
            error_code: 43_i16,
            partition_index: 23456_i32,
            leader_id: 23456_i32,
            leader_epoch: if version >= 7 { 23456_i32 } else { -1i32 },
            replica_nodes: vec![12345_i32, 23456_i32],
            isr_nodes: vec![12345_i32, 23456_i32],
            offline_replicas: if version >= 5 {
                vec![12345_i32, 23456_i32]
            } else {
                Vec::new()
            },
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries(version: i16) -> Self {
        Self {
            error_code: i16::MIN,
            partition_index: i32::MIN,
            leader_id: i32::MIN,
            leader_epoch: if version >= 7 { i32::MIN } else { -1i32 },
            replica_nodes: vec![i32::MIN],
            isr_nodes: vec![i32::MIN],
            offline_replicas: if version >= 5 {
                vec![i32::MIN]
            } else {
                Vec::new()
            },
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields(version: i16) -> Self {
        Self {
            error_code: 42_i16,
            partition_index: 12345_i32,
            leader_id: 12345_i32,
            leader_epoch: if version >= 7 { 12345_i32 } else { -1i32 },
            replica_nodes: vec![12345_i32],
            isr_nodes: vec![12345_i32],
            offline_replicas: if version >= 5 {
                vec![12345_i32]
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
fn encode_populated(version: i16) -> crate::MatrixResult<String> {
    let message = <MetadataResponseData as TestInstance>::test_populated(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_populated(version: i16) -> crate::MatrixResult<usize> {
    let message = <MetadataResponseData as TestInstance>::test_populated(version);
    Ok(message.encoded_len(version)?)
}
fn encode_null_optionals(version: i16) -> crate::MatrixResult<String> {
    let message = <MetadataResponseData as TestInstance>::test_null_optionals(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_null_optionals(version: i16) -> crate::MatrixResult<usize> {
    let message = <MetadataResponseData as TestInstance>::test_null_optionals(version);
    Ok(message.encoded_len(version)?)
}
fn encode_empty_collections(version: i16) -> crate::MatrixResult<String> {
    let message = <MetadataResponseData as TestInstance>::test_empty_collections(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_empty_collections(version: i16) -> crate::MatrixResult<usize> {
    let message = <MetadataResponseData as TestInstance>::test_empty_collections(version);
    Ok(message.encoded_len(version)?)
}
fn encode_multi_element_collections(version: i16) -> crate::MatrixResult<String> {
    let message = <MetadataResponseData as TestInstance>::test_multi_element_collections(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_multi_element_collections(version: i16) -> crate::MatrixResult<usize> {
    let message = <MetadataResponseData as TestInstance>::test_multi_element_collections(version);
    Ok(message.encoded_len(version)?)
}
fn encode_numeric_boundaries(version: i16) -> crate::MatrixResult<String> {
    let message = <MetadataResponseData as TestInstance>::test_numeric_boundaries(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_numeric_boundaries(version: i16) -> crate::MatrixResult<usize> {
    let message = <MetadataResponseData as TestInstance>::test_numeric_boundaries(version);
    Ok(message.encoded_len(version)?)
}
fn encode_tagged_fields(version: i16) -> crate::MatrixResult<String> {
    let message = <MetadataResponseData as TestInstance>::test_tagged_fields(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_tagged_fields(version: i16) -> crate::MatrixResult<usize> {
    let message = <MetadataResponseData as TestInstance>::test_tagged_fields(version);
    Ok(message.encoded_len(version)?)
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
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 1i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 2i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 3i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 4i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 5i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 6i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 7i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 8i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 9i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 10i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 11i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 12i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 13i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 0i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 1i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 2i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 3i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 4i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 5i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 6i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 7i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 8i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 9i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 10i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 11i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 12i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 13i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 0i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 0i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 0i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 0i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 1i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 1i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 1i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 1i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 2i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 2i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 2i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 2i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 3i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 3i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 3i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 3i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 4i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 4i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 4i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 4i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 5i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 5i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 5i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 5i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 6i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 6i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 6i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 6i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 7i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 7i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 7i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 7i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 8i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 8i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 8i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 8i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 9i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 9i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 9i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 9i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 10i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 10i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 10i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 10i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 11i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 11i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 11i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 11i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 12i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 12i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 12i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 12i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 13i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 13i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
        version: 13i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataResponse",
        java_class: "org.apache.kafka.common.message.MetadataResponseData",
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
