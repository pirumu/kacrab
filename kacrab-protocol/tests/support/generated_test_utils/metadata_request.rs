#![allow(
    clippy::all,
    clippy::pedantic,
    clippy::nursery,
    reason = "Generated test fixtures mirror Kafka's schema shape and trade hand-written lint \
              style for reproducible output, matching the generated protocol modules."
)]
use bytes::{Bytes, BytesMut};
use kacrab_protocol::{generated::metadata_request::*, *};

use crate::TestInstance;

impl TestInstance for MetadataRequestData {
    fn test_populated(version: i16) -> Self {
        Self {
            topics: Some(vec![
                <MetadataRequestTopic as TestInstance>::test_populated(version),
            ]),
            allow_auto_topic_creation: true,
            include_cluster_authorized_operations: if version >= 8 && version <= 10 {
                true
            } else {
                false
            },
            include_topic_authorized_operations: if version >= 8 { true } else { false },
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals(version: i16) -> Self {
        drop(<MetadataRequestTopic as TestInstance>::test_null_optionals(
            version,
        ));
        Self {
            topics: None,
            allow_auto_topic_creation: if version >= 4 { false } else { true },
            include_cluster_authorized_operations: false,
            include_topic_authorized_operations: false,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections(version: i16) -> Self {
        Self {
            topics: Some(Vec::new()),
            allow_auto_topic_creation: if version >= 4 { false } else { true },
            include_cluster_authorized_operations: false,
            include_topic_authorized_operations: false,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections(version: i16) -> Self {
        Self {
            topics: Some(vec![
                <MetadataRequestTopic as TestInstance>::test_populated(version),
                <MetadataRequestTopic as TestInstance>::test_multi_element_collections(version),
            ]),
            allow_auto_topic_creation: if version >= 4 { false } else { true },
            include_cluster_authorized_operations: false,
            include_topic_authorized_operations: false,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries(version: i16) -> Self {
        Self {
            topics: Some(vec![
                <MetadataRequestTopic as TestInstance>::test_numeric_boundaries(version),
            ]),
            allow_auto_topic_creation: true,
            include_cluster_authorized_operations: if version >= 8 && version <= 10 {
                true
            } else {
                false
            },
            include_topic_authorized_operations: if version >= 8 { true } else { false },
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields(version: i16) -> Self {
        Self {
            topics: Some(vec![
                <MetadataRequestTopic as TestInstance>::test_tagged_fields(version),
            ]),
            allow_auto_topic_creation: true,
            include_cluster_authorized_operations: if version >= 8 && version <= 10 {
                true
            } else {
                false
            },
            include_topic_authorized_operations: if version >= 8 { true } else { false },
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for MetadataRequestTopic {
    fn test_populated(version: i16) -> Self {
        Self {
            topic_id: if version >= 10 {
                KafkaUuid::ONE
            } else {
                KafkaUuid::ZERO
            },
            name: Some(KafkaString::from("test".to_owned())),
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals(_version: i16) -> Self {
        drop(Self::default());
        Self {
            topic_id: KafkaUuid::ZERO,
            name: None,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections(_version: i16) -> Self {
        Self {
            topic_id: KafkaUuid::ZERO,
            name: Some(KafkaString::default()),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections(version: i16) -> Self {
        Self {
            topic_id: if version >= 10 {
                KafkaUuid::from_parts(2, 3)
            } else {
                KafkaUuid::ZERO
            },
            name: Some(KafkaString::from("test-2".to_owned())),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries(version: i16) -> Self {
        Self {
            topic_id: if version >= 10 {
                KafkaUuid::ONE
            } else {
                KafkaUuid::ZERO
            },
            name: Some(KafkaString::from("boundary".to_owned())),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields(version: i16) -> Self {
        Self {
            topic_id: if version >= 10 {
                KafkaUuid::ONE
            } else {
                KafkaUuid::ZERO
            },
            name: Some(KafkaString::from("test".to_owned())),
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
fn encode_populated(version: i16) -> crate::MatrixResult<String> {
    let message = <MetadataRequestData as TestInstance>::test_populated(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_populated(version: i16) -> crate::MatrixResult<usize> {
    let message = <MetadataRequestData as TestInstance>::test_populated(version);
    Ok(message.encoded_len(version)?)
}
fn encode_null_optionals(version: i16) -> crate::MatrixResult<String> {
    let message = <MetadataRequestData as TestInstance>::test_null_optionals(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_null_optionals(version: i16) -> crate::MatrixResult<usize> {
    let message = <MetadataRequestData as TestInstance>::test_null_optionals(version);
    Ok(message.encoded_len(version)?)
}
fn encode_empty_collections(version: i16) -> crate::MatrixResult<String> {
    let message = <MetadataRequestData as TestInstance>::test_empty_collections(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_empty_collections(version: i16) -> crate::MatrixResult<usize> {
    let message = <MetadataRequestData as TestInstance>::test_empty_collections(version);
    Ok(message.encoded_len(version)?)
}
fn encode_multi_element_collections(version: i16) -> crate::MatrixResult<String> {
    let message = <MetadataRequestData as TestInstance>::test_multi_element_collections(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_multi_element_collections(version: i16) -> crate::MatrixResult<usize> {
    let message = <MetadataRequestData as TestInstance>::test_multi_element_collections(version);
    Ok(message.encoded_len(version)?)
}
fn encode_numeric_boundaries(version: i16) -> crate::MatrixResult<String> {
    let message = <MetadataRequestData as TestInstance>::test_numeric_boundaries(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_numeric_boundaries(version: i16) -> crate::MatrixResult<usize> {
    let message = <MetadataRequestData as TestInstance>::test_numeric_boundaries(version);
    Ok(message.encoded_len(version)?)
}
fn encode_tagged_fields(version: i16) -> crate::MatrixResult<String> {
    let message = <MetadataRequestData as TestInstance>::test_tagged_fields(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_tagged_fields(version: i16) -> crate::MatrixResult<usize> {
    let message = <MetadataRequestData as TestInstance>::test_tagged_fields(version);
    Ok(message.encoded_len(version)?)
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
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 1i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 2i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 3i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 4i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 5i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 6i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 7i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 8i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 9i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 10i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 11i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 12i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 13i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 0i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 1i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 2i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 3i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 4i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 5i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 6i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 7i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 8i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 9i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 10i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 11i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 12i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 13i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 0i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 0i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 0i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 0i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 1i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 1i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 1i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 1i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 2i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 2i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 2i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 2i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 3i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 3i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 3i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 3i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 4i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 4i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 4i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 4i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 5i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 5i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 5i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 5i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 6i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 6i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 6i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 6i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 7i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 7i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 7i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 7i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 8i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 8i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 8i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 8i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 9i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 9i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 9i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 9i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 10i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 10i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 10i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 10i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 11i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 11i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 11i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 11i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 12i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 12i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 12i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 12i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 13i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 13i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
        version: 13i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "MetadataRequest",
        java_class: "org.apache.kafka.common.message.MetadataRequestData",
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
