use bytes::{Bytes, BytesMut};
use kacrab_protocol::{generated::offset_fetch_request::*, *};

use crate::TestInstance;

impl TestInstance for OffsetFetchRequestData {
    fn test_populated() -> Self {
        Self {
            group_id: KafkaString::from("test".to_owned()),
            topics: Some(vec![
                <OffsetFetchRequestTopic as TestInstance>::test_populated(),
            ]),
            groups: vec![<OffsetFetchRequestGroup as TestInstance>::test_populated()],
            require_stable: true,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals() -> Self {
        drop(<OffsetFetchRequestTopic as TestInstance>::test_null_optionals());
        Self {
            group_id: KafkaString::default(),
            topics: None,
            groups: vec![<OffsetFetchRequestGroup as TestInstance>::test_null_optionals()],
            require_stable: false,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            group_id: KafkaString::default(),
            topics: Some(Vec::new()),
            groups: Vec::new(),
            require_stable: false,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            group_id: KafkaString::from("test-2".to_owned()),
            topics: Some(vec![
                <OffsetFetchRequestTopic as TestInstance>::test_populated(),
                <OffsetFetchRequestTopic as TestInstance>::test_multi_element_collections(),
            ]),
            groups: vec![
                <OffsetFetchRequestGroup as TestInstance>::test_populated(),
                <OffsetFetchRequestGroup as TestInstance>::test_multi_element_collections(),
            ],
            require_stable: false,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            group_id: KafkaString::from("boundary".to_owned()),
            topics: Some(vec![
                <OffsetFetchRequestTopic as TestInstance>::test_numeric_boundaries(),
            ]),
            groups: vec![<OffsetFetchRequestGroup as TestInstance>::test_numeric_boundaries()],
            require_stable: true,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            group_id: KafkaString::from("test".to_owned()),
            topics: Some(vec![
                <OffsetFetchRequestTopic as TestInstance>::test_tagged_fields(),
            ]),
            groups: vec![<OffsetFetchRequestGroup as TestInstance>::test_tagged_fields()],
            require_stable: true,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for OffsetFetchRequestTopic {
    fn test_populated() -> Self {
        Self {
            name: KafkaString::from("test".to_owned()),
            partition_indexes: vec![12345_i32],
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
            partition_indexes: vec![0_i32],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            name: KafkaString::default(),
            partition_indexes: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            name: KafkaString::from("test-2".to_owned()),
            partition_indexes: vec![12345_i32, 23456_i32],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            name: KafkaString::from("boundary".to_owned()),
            partition_indexes: vec![i32::MIN],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            name: KafkaString::from("test".to_owned()),
            partition_indexes: vec![12345_i32],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for OffsetFetchRequestGroup {
    fn test_populated() -> Self {
        Self {
            group_id: KafkaString::from("test".to_owned()),
            member_id: Some(KafkaString::from("test".to_owned())),
            member_epoch: 12345_i32,
            topics: Some(vec![
                <OffsetFetchRequestTopics as TestInstance>::test_populated(),
            ]),
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals() -> Self {
        drop(Self::default());
        drop(<OffsetFetchRequestTopics as TestInstance>::test_null_optionals());
        Self {
            group_id: KafkaString::default(),
            member_id: None,
            member_epoch: 0_i32,
            topics: None,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            group_id: KafkaString::default(),
            member_id: Some(KafkaString::default()),
            member_epoch: 0_i32,
            topics: Some(Vec::new()),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            group_id: KafkaString::from("test-2".to_owned()),
            member_id: Some(KafkaString::from("test-2".to_owned())),
            member_epoch: 23456_i32,
            topics: Some(vec![
                <OffsetFetchRequestTopics as TestInstance>::test_populated(),
                <OffsetFetchRequestTopics as TestInstance>::test_multi_element_collections(),
            ]),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            group_id: KafkaString::from("boundary".to_owned()),
            member_id: Some(KafkaString::from("boundary".to_owned())),
            member_epoch: i32::MIN,
            topics: Some(vec![
                <OffsetFetchRequestTopics as TestInstance>::test_numeric_boundaries(),
            ]),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            group_id: KafkaString::from("test".to_owned()),
            member_id: Some(KafkaString::from("test".to_owned())),
            member_epoch: 12345_i32,
            topics: Some(vec![
                <OffsetFetchRequestTopics as TestInstance>::test_tagged_fields(),
            ]),
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for OffsetFetchRequestTopics {
    fn test_populated() -> Self {
        Self {
            name: KafkaString::from("test".to_owned()),
            topic_id: KafkaUuid::ONE,
            partition_indexes: vec![12345_i32],
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
            partition_indexes: vec![0_i32],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            name: KafkaString::default(),
            topic_id: KafkaUuid::ZERO,
            partition_indexes: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            name: KafkaString::from("test-2".to_owned()),
            topic_id: KafkaUuid::from_parts(2, 3),
            partition_indexes: vec![12345_i32, 23456_i32],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            name: KafkaString::from("boundary".to_owned()),
            topic_id: KafkaUuid::ONE,
            partition_indexes: vec![i32::MIN],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            name: KafkaString::from("test".to_owned()),
            topic_id: KafkaUuid::ONE,
            partition_indexes: vec![12345_i32],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
fn encode_populated(version: i16) -> crate::MatrixResult<String> {
    let message = <OffsetFetchRequestData as TestInstance>::test_populated();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_null_optionals(version: i16) -> crate::MatrixResult<String> {
    let message = <OffsetFetchRequestData as TestInstance>::test_null_optionals();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_empty_collections(version: i16) -> crate::MatrixResult<String> {
    let message = <OffsetFetchRequestData as TestInstance>::test_empty_collections();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_multi_element_collections(version: i16) -> crate::MatrixResult<String> {
    let message = <OffsetFetchRequestData as TestInstance>::test_multi_element_collections();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_numeric_boundaries(version: i16) -> crate::MatrixResult<String> {
    let message = <OffsetFetchRequestData as TestInstance>::test_numeric_boundaries();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_tagged_fields(version: i16) -> crate::MatrixResult<String> {
    let message = <OffsetFetchRequestData as TestInstance>::test_tagged_fields();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn reencode(version: i16, hex_input: &str) -> crate::MatrixResult<String> {
    let mut input = Bytes::from(crate::decode_hex(hex_input)?);
    let message = OffsetFetchRequestData::read(&mut input, version)?;
    crate::ensure_input_consumed(&input)?;
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
const MATRIX_CASES: &[crate::MatrixCase] = &[
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 1i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 2i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 3i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 4i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 5i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 6i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 7i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 8i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 9i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 10i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 1i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 2i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 3i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 4i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 5i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 6i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 7i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 8i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 9i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 10i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 1i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 1i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 1i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 1i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 2i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 2i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 2i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 2i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 3i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 3i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 3i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 3i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 4i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 4i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 4i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 4i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 5i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 5i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 5i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 5i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 6i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 6i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 6i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 6i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 7i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 7i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 7i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 7i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 8i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 8i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 8i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 8i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 9i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 9i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 9i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 9i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 10i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 10i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 10i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "OffsetFetchRequest",
        java_class: "org.apache.kafka.common.message.OffsetFetchRequestData",
        version: 10i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
];
pub(crate) fn append_protocol_cases(cases: &mut Vec<crate::MatrixCase>) {
    cases.extend_from_slice(MATRIX_CASES);
}
