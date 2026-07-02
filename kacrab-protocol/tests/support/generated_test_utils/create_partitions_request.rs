#![allow(
    clippy::all,
    clippy::pedantic,
    clippy::nursery,
    reason = "Generated test fixtures mirror Kafka's schema shape and trade hand-written lint \
              style for reproducible output, matching the generated protocol modules."
)]
use bytes::{Bytes, BytesMut};
use kacrab_protocol::{generated::create_partitions_request::*, *};

use crate::TestInstance;

impl TestInstance for CreatePartitionsRequestData {
    fn test_populated(version: i16) -> Self {
        Self {
            topics: vec![<CreatePartitionsTopic as TestInstance>::test_populated(
                version,
            )],
            timeout_ms: 12345_i32,
            validate_only: true,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals(version: i16) -> Self {
        Self {
            topics: vec![<CreatePartitionsTopic as TestInstance>::test_null_optionals(version)],
            timeout_ms: 0_i32,
            validate_only: false,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections(_version: i16) -> Self {
        Self {
            topics: Vec::new(),
            timeout_ms: 0_i32,
            validate_only: false,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections(version: i16) -> Self {
        Self {
            topics: vec![
                <CreatePartitionsTopic as TestInstance>::test_populated(version),
                <CreatePartitionsTopic as TestInstance>::test_multi_element_collections(version),
            ],
            timeout_ms: 23456_i32,
            validate_only: false,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries(version: i16) -> Self {
        Self {
            topics: vec![<CreatePartitionsTopic as TestInstance>::test_numeric_boundaries(version)],
            timeout_ms: i32::MIN,
            validate_only: true,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields(version: i16) -> Self {
        Self {
            topics: vec![<CreatePartitionsTopic as TestInstance>::test_tagged_fields(
                version,
            )],
            timeout_ms: 12345_i32,
            validate_only: true,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for CreatePartitionsTopic {
    fn test_populated(version: i16) -> Self {
        Self {
            name: KafkaString::from("test".to_owned()),
            count: 12345_i32,
            assignments: Some(vec![
                <CreatePartitionsAssignment as TestInstance>::test_populated(version),
            ]),
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals(version: i16) -> Self {
        drop(Self::default());
        drop(<CreatePartitionsAssignment as TestInstance>::test_null_optionals(version));
        Self {
            name: KafkaString::default(),
            count: 0_i32,
            assignments: None,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections(_version: i16) -> Self {
        Self {
            name: KafkaString::default(),
            count: 0_i32,
            assignments: Some(Vec::new()),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections(version: i16) -> Self {
        Self {
            name: KafkaString::from("test-2".to_owned()),
            count: 23456_i32,
            assignments: Some(vec![
                <CreatePartitionsAssignment as TestInstance>::test_populated(version),
                <CreatePartitionsAssignment as TestInstance>::test_multi_element_collections(
                    version,
                ),
            ]),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries(version: i16) -> Self {
        Self {
            name: KafkaString::from("boundary".to_owned()),
            count: i32::MIN,
            assignments: Some(vec![
                <CreatePartitionsAssignment as TestInstance>::test_numeric_boundaries(version),
            ]),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields(version: i16) -> Self {
        Self {
            name: KafkaString::from("test".to_owned()),
            count: 12345_i32,
            assignments: Some(vec![
                <CreatePartitionsAssignment as TestInstance>::test_tagged_fields(version),
            ]),
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for CreatePartitionsAssignment {
    fn test_populated(_version: i16) -> Self {
        Self {
            broker_ids: vec![12345_i32],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals(_version: i16) -> Self {
        drop(Self::default());
        Self {
            broker_ids: vec![0_i32],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections(_version: i16) -> Self {
        Self {
            broker_ids: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections(_version: i16) -> Self {
        Self {
            broker_ids: vec![12345_i32, 23456_i32],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries(_version: i16) -> Self {
        Self {
            broker_ids: vec![i32::MIN],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields(_version: i16) -> Self {
        Self {
            broker_ids: vec![12345_i32],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
fn encode_populated(version: i16) -> crate::MatrixResult<String> {
    let message = <CreatePartitionsRequestData as TestInstance>::test_populated(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_populated(version: i16) -> crate::MatrixResult<usize> {
    let message = <CreatePartitionsRequestData as TestInstance>::test_populated(version);
    Ok(message.encoded_len(version)?)
}
fn encode_null_optionals(version: i16) -> crate::MatrixResult<String> {
    let message = <CreatePartitionsRequestData as TestInstance>::test_null_optionals(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_null_optionals(version: i16) -> crate::MatrixResult<usize> {
    let message = <CreatePartitionsRequestData as TestInstance>::test_null_optionals(version);
    Ok(message.encoded_len(version)?)
}
fn encode_empty_collections(version: i16) -> crate::MatrixResult<String> {
    let message = <CreatePartitionsRequestData as TestInstance>::test_empty_collections(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_empty_collections(version: i16) -> crate::MatrixResult<usize> {
    let message = <CreatePartitionsRequestData as TestInstance>::test_empty_collections(version);
    Ok(message.encoded_len(version)?)
}
fn encode_multi_element_collections(version: i16) -> crate::MatrixResult<String> {
    let message =
        <CreatePartitionsRequestData as TestInstance>::test_multi_element_collections(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_multi_element_collections(version: i16) -> crate::MatrixResult<usize> {
    let message =
        <CreatePartitionsRequestData as TestInstance>::test_multi_element_collections(version);
    Ok(message.encoded_len(version)?)
}
fn encode_numeric_boundaries(version: i16) -> crate::MatrixResult<String> {
    let message = <CreatePartitionsRequestData as TestInstance>::test_numeric_boundaries(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_numeric_boundaries(version: i16) -> crate::MatrixResult<usize> {
    let message = <CreatePartitionsRequestData as TestInstance>::test_numeric_boundaries(version);
    Ok(message.encoded_len(version)?)
}
fn encode_tagged_fields(version: i16) -> crate::MatrixResult<String> {
    let message = <CreatePartitionsRequestData as TestInstance>::test_tagged_fields(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_tagged_fields(version: i16) -> crate::MatrixResult<usize> {
    let message = <CreatePartitionsRequestData as TestInstance>::test_tagged_fields(version);
    Ok(message.encoded_len(version)?)
}
fn reencode(version: i16, hex_input: &str) -> crate::MatrixResult<String> {
    let mut input = Bytes::from(crate::decode_hex(hex_input)?);
    let message = CreatePartitionsRequestData::read(&mut input, version)?;
    crate::ensure_input_consumed(&input)?;
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
const MATRIX_CASES: &[crate::MatrixCase] = &[
    crate::MatrixCase {
        schema_name: "CreatePartitionsRequest",
        java_class: "org.apache.kafka.common.message.CreatePartitionsRequestData",
        version: 0i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "CreatePartitionsRequest",
        java_class: "org.apache.kafka.common.message.CreatePartitionsRequestData",
        version: 1i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "CreatePartitionsRequest",
        java_class: "org.apache.kafka.common.message.CreatePartitionsRequestData",
        version: 2i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "CreatePartitionsRequest",
        java_class: "org.apache.kafka.common.message.CreatePartitionsRequestData",
        version: 3i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "CreatePartitionsRequest",
        java_class: "org.apache.kafka.common.message.CreatePartitionsRequestData",
        version: 0i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "CreatePartitionsRequest",
        java_class: "org.apache.kafka.common.message.CreatePartitionsRequestData",
        version: 1i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "CreatePartitionsRequest",
        java_class: "org.apache.kafka.common.message.CreatePartitionsRequestData",
        version: 2i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "CreatePartitionsRequest",
        java_class: "org.apache.kafka.common.message.CreatePartitionsRequestData",
        version: 3i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "CreatePartitionsRequest",
        java_class: "org.apache.kafka.common.message.CreatePartitionsRequestData",
        version: 0i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "CreatePartitionsRequest",
        java_class: "org.apache.kafka.common.message.CreatePartitionsRequestData",
        version: 0i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "CreatePartitionsRequest",
        java_class: "org.apache.kafka.common.message.CreatePartitionsRequestData",
        version: 0i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "CreatePartitionsRequest",
        java_class: "org.apache.kafka.common.message.CreatePartitionsRequestData",
        version: 0i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "CreatePartitionsRequest",
        java_class: "org.apache.kafka.common.message.CreatePartitionsRequestData",
        version: 1i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "CreatePartitionsRequest",
        java_class: "org.apache.kafka.common.message.CreatePartitionsRequestData",
        version: 1i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "CreatePartitionsRequest",
        java_class: "org.apache.kafka.common.message.CreatePartitionsRequestData",
        version: 1i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "CreatePartitionsRequest",
        java_class: "org.apache.kafka.common.message.CreatePartitionsRequestData",
        version: 1i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "CreatePartitionsRequest",
        java_class: "org.apache.kafka.common.message.CreatePartitionsRequestData",
        version: 2i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "CreatePartitionsRequest",
        java_class: "org.apache.kafka.common.message.CreatePartitionsRequestData",
        version: 2i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "CreatePartitionsRequest",
        java_class: "org.apache.kafka.common.message.CreatePartitionsRequestData",
        version: 2i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "CreatePartitionsRequest",
        java_class: "org.apache.kafka.common.message.CreatePartitionsRequestData",
        version: 2i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "CreatePartitionsRequest",
        java_class: "org.apache.kafka.common.message.CreatePartitionsRequestData",
        version: 3i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "CreatePartitionsRequest",
        java_class: "org.apache.kafka.common.message.CreatePartitionsRequestData",
        version: 3i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "CreatePartitionsRequest",
        java_class: "org.apache.kafka.common.message.CreatePartitionsRequestData",
        version: 3i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "CreatePartitionsRequest",
        java_class: "org.apache.kafka.common.message.CreatePartitionsRequestData",
        version: 3i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
];
pub(crate) fn append_protocol_cases(cases: &mut Vec<crate::MatrixCase>) {
    cases.extend_from_slice(MATRIX_CASES);
}
