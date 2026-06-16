use bytes::{Bytes, BytesMut};
use kacrab_protocol::{generated::metadata_request::*, *};

use crate::TestInstance;

impl TestInstance for MetadataRequestData {
    fn test_populated() -> Self {
        Self {
            topics: Some(vec![
                <MetadataRequestTopic as TestInstance>::test_populated(),
            ]),
            allow_auto_topic_creation: true,
            include_cluster_authorized_operations: true,
            include_topic_authorized_operations: true,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals() -> Self {
        drop(<MetadataRequestTopic as TestInstance>::test_null_optionals());
        Self {
            topics: None,
            allow_auto_topic_creation: false,
            include_cluster_authorized_operations: false,
            include_topic_authorized_operations: false,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            topics: Some(Vec::new()),
            allow_auto_topic_creation: false,
            include_cluster_authorized_operations: false,
            include_topic_authorized_operations: false,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            topics: Some(vec![
                <MetadataRequestTopic as TestInstance>::test_populated(),
                <MetadataRequestTopic as TestInstance>::test_multi_element_collections(),
            ]),
            allow_auto_topic_creation: false,
            include_cluster_authorized_operations: false,
            include_topic_authorized_operations: false,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            topics: Some(vec![
                <MetadataRequestTopic as TestInstance>::test_numeric_boundaries(),
            ]),
            allow_auto_topic_creation: true,
            include_cluster_authorized_operations: true,
            include_topic_authorized_operations: true,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            topics: Some(vec![
                <MetadataRequestTopic as TestInstance>::test_tagged_fields(),
            ]),
            allow_auto_topic_creation: true,
            include_cluster_authorized_operations: true,
            include_topic_authorized_operations: true,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for MetadataRequestTopic {
    fn test_populated() -> Self {
        Self {
            topic_id: KafkaUuid::ONE,
            name: Some(KafkaString::from("test".to_owned())),
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals() -> Self {
        drop(Self::default());
        Self {
            topic_id: KafkaUuid::ZERO,
            name: None,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            topic_id: KafkaUuid::ZERO,
            name: Some(KafkaString::default()),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            topic_id: KafkaUuid::from_parts(2, 3),
            name: Some(KafkaString::from("test-2".to_owned())),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            topic_id: KafkaUuid::ONE,
            name: Some(KafkaString::from("boundary".to_owned())),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            topic_id: KafkaUuid::ONE,
            name: Some(KafkaString::from("test".to_owned())),
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
fn encode_populated(version: i16) -> crate::MatrixResult<String> {
    let message = <MetadataRequestData as TestInstance>::test_populated();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_null_optionals(version: i16) -> crate::MatrixResult<String> {
    let message = <MetadataRequestData as TestInstance>::test_null_optionals();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_empty_collections(version: i16) -> crate::MatrixResult<String> {
    let message = <MetadataRequestData as TestInstance>::test_empty_collections();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_multi_element_collections(version: i16) -> crate::MatrixResult<String> {
    let message = <MetadataRequestData as TestInstance>::test_multi_element_collections();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_numeric_boundaries(version: i16) -> crate::MatrixResult<String> {
    let message = <MetadataRequestData as TestInstance>::test_numeric_boundaries();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_tagged_fields(version: i16) -> crate::MatrixResult<String> {
    let message = <MetadataRequestData as TestInstance>::test_tagged_fields();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn reencode(version: i16, hex_input: &str) -> crate::MatrixResult<String> {
    let mut input = Bytes::from(crate::decode_hex(hex_input)?);
    let message = MetadataRequestData::read(&mut input, version)?;
    crate::ensure_input_consumed(&input)?;
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
const MATRIX_CASES: &[crate::MatrixCase] = &[
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 0i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 1i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 2i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 3i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 4i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 5i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 6i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 7i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 8i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 9i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 10i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 11i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 12i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 13i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 0i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 1i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 2i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 3i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 4i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 5i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 6i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 7i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 8i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 9i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 10i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 11i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 12i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 13i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 0i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 0i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 0i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 0i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 1i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 1i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 1i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 1i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 2i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 2i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 2i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 2i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 3i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 3i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 3i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 3i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 4i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 4i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 4i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 4i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 5i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 5i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 5i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 5i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 6i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 6i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 6i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 6i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 7i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 7i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 7i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 7i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 8i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 8i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 8i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 8i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 9i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 9i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 9i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 9i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 10i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 10i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 10i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 10i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 11i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 11i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 11i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 11i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 12i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 12i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 12i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 12i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 13i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 13i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 13i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 13i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
];
pub(crate) fn append_protocol_cases(cases: &mut Vec<crate::MatrixCase>) {
    cases.extend_from_slice(MATRIX_CASES);
}
