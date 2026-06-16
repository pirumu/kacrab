use bytes::{Bytes, BytesMut};
use kacrab_protocol::{generated::describe_log_dirs_response::*, *};

use crate::TestInstance;

impl TestInstance for DescribeLogDirsResponseData {
    fn test_populated() -> Self {
        Self {
            throttle_time_ms: 12345_i32,
            error_code: 42_i16,
            results: vec![<DescribeLogDirsResult as TestInstance>::test_populated()],
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
            results: vec![<DescribeLogDirsResult as TestInstance>::test_null_optionals()],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            throttle_time_ms: 0_i32,
            error_code: 0_i16,
            results: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            throttle_time_ms: 23456_i32,
            error_code: 43_i16,
            results: vec![
                <DescribeLogDirsResult as TestInstance>::test_populated(),
                <DescribeLogDirsResult as TestInstance>::test_multi_element_collections(),
            ],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            throttle_time_ms: i32::MIN,
            error_code: i16::MIN,
            results: vec![<DescribeLogDirsResult as TestInstance>::test_numeric_boundaries()],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            throttle_time_ms: 12345_i32,
            error_code: 42_i16,
            results: vec![<DescribeLogDirsResult as TestInstance>::test_tagged_fields()],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for DescribeLogDirsResult {
    fn test_populated() -> Self {
        Self {
            error_code: 42_i16,
            log_dir: KafkaString::from("test".to_owned()),
            topics: vec![<DescribeLogDirsTopic as TestInstance>::test_populated()],
            total_bytes: 9_876_543_210_i64,
            usable_bytes: 9_876_543_210_i64,
            is_cordoned: true,
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
            log_dir: KafkaString::default(),
            topics: vec![<DescribeLogDirsTopic as TestInstance>::test_null_optionals()],
            total_bytes: 0_i64,
            usable_bytes: 0_i64,
            is_cordoned: false,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            error_code: 0_i16,
            log_dir: KafkaString::default(),
            topics: Vec::new(),
            total_bytes: 0_i64,
            usable_bytes: 0_i64,
            is_cordoned: false,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            error_code: 43_i16,
            log_dir: KafkaString::from("test-2".to_owned()),
            topics: vec![
                <DescribeLogDirsTopic as TestInstance>::test_populated(),
                <DescribeLogDirsTopic as TestInstance>::test_multi_element_collections(),
            ],
            total_bytes: 9_876_543_211_i64,
            usable_bytes: 9_876_543_211_i64,
            is_cordoned: false,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            error_code: i16::MIN,
            log_dir: KafkaString::from("boundary".to_owned()),
            topics: vec![<DescribeLogDirsTopic as TestInstance>::test_numeric_boundaries()],
            total_bytes: i64::MIN,
            usable_bytes: i64::MIN,
            is_cordoned: true,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            error_code: 42_i16,
            log_dir: KafkaString::from("test".to_owned()),
            topics: vec![<DescribeLogDirsTopic as TestInstance>::test_tagged_fields()],
            total_bytes: 9_876_543_210_i64,
            usable_bytes: 9_876_543_210_i64,
            is_cordoned: true,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for DescribeLogDirsTopic {
    fn test_populated() -> Self {
        Self {
            name: KafkaString::from("test".to_owned()),
            partitions: vec![<DescribeLogDirsPartition as TestInstance>::test_populated()],
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
            partitions: vec![<DescribeLogDirsPartition as TestInstance>::test_null_optionals()],
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
                <DescribeLogDirsPartition as TestInstance>::test_populated(),
                <DescribeLogDirsPartition as TestInstance>::test_multi_element_collections(),
            ],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            name: KafkaString::from("boundary".to_owned()),
            partitions: vec![<DescribeLogDirsPartition as TestInstance>::test_numeric_boundaries()],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            name: KafkaString::from("test".to_owned()),
            partitions: vec![<DescribeLogDirsPartition as TestInstance>::test_tagged_fields()],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for DescribeLogDirsPartition {
    fn test_populated() -> Self {
        Self {
            partition_index: 12345_i32,
            partition_size: 9_876_543_210_i64,
            offset_lag: 9_876_543_210_i64,
            is_future_key: true,
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
            partition_size: 0_i64,
            offset_lag: 0_i64,
            is_future_key: false,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            partition_index: 0_i32,
            partition_size: 0_i64,
            offset_lag: 0_i64,
            is_future_key: false,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            partition_index: 23456_i32,
            partition_size: 9_876_543_211_i64,
            offset_lag: 9_876_543_211_i64,
            is_future_key: false,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            partition_index: i32::MIN,
            partition_size: i64::MIN,
            offset_lag: i64::MIN,
            is_future_key: true,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            partition_index: 12345_i32,
            partition_size: 9_876_543_210_i64,
            offset_lag: 9_876_543_210_i64,
            is_future_key: true,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
fn encode_populated(version: i16) -> crate::MatrixResult<String> {
    let message = <DescribeLogDirsResponseData as TestInstance>::test_populated();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_populated(version: i16) -> crate::MatrixResult<usize> {
    let message = <DescribeLogDirsResponseData as TestInstance>::test_populated();
    Ok(message.encoded_len(version)?)
}
fn encode_null_optionals(version: i16) -> crate::MatrixResult<String> {
    let message = <DescribeLogDirsResponseData as TestInstance>::test_null_optionals();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_null_optionals(version: i16) -> crate::MatrixResult<usize> {
    let message = <DescribeLogDirsResponseData as TestInstance>::test_null_optionals();
    Ok(message.encoded_len(version)?)
}
fn encode_empty_collections(version: i16) -> crate::MatrixResult<String> {
    let message = <DescribeLogDirsResponseData as TestInstance>::test_empty_collections();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_empty_collections(version: i16) -> crate::MatrixResult<usize> {
    let message = <DescribeLogDirsResponseData as TestInstance>::test_empty_collections();
    Ok(message.encoded_len(version)?)
}
fn encode_multi_element_collections(version: i16) -> crate::MatrixResult<String> {
    let message = <DescribeLogDirsResponseData as TestInstance>::test_multi_element_collections();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_multi_element_collections(version: i16) -> crate::MatrixResult<usize> {
    let message = <DescribeLogDirsResponseData as TestInstance>::test_multi_element_collections();
    Ok(message.encoded_len(version)?)
}
fn encode_numeric_boundaries(version: i16) -> crate::MatrixResult<String> {
    let message = <DescribeLogDirsResponseData as TestInstance>::test_numeric_boundaries();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_numeric_boundaries(version: i16) -> crate::MatrixResult<usize> {
    let message = <DescribeLogDirsResponseData as TestInstance>::test_numeric_boundaries();
    Ok(message.encoded_len(version)?)
}
fn encode_tagged_fields(version: i16) -> crate::MatrixResult<String> {
    let message = <DescribeLogDirsResponseData as TestInstance>::test_tagged_fields();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_tagged_fields(version: i16) -> crate::MatrixResult<usize> {
    let message = <DescribeLogDirsResponseData as TestInstance>::test_tagged_fields();
    Ok(message.encoded_len(version)?)
}
fn reencode(version: i16, hex_input: &str) -> crate::MatrixResult<String> {
    let mut input = Bytes::from(crate::decode_hex(hex_input)?);
    let message = DescribeLogDirsResponseData::read(&mut input, version)?;
    crate::ensure_input_consumed(&input)?;
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
const MATRIX_CASES: &[crate::MatrixCase] = &[
    crate::MatrixCase {
        schema_name: "DescribeLogDirsResponse",
        java_class: "org.apache.kafka.common.message.DescribeLogDirsResponseData",
        version: 1i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeLogDirsResponse",
        java_class: "org.apache.kafka.common.message.DescribeLogDirsResponseData",
        version: 2i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeLogDirsResponse",
        java_class: "org.apache.kafka.common.message.DescribeLogDirsResponseData",
        version: 3i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeLogDirsResponse",
        java_class: "org.apache.kafka.common.message.DescribeLogDirsResponseData",
        version: 4i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeLogDirsResponse",
        java_class: "org.apache.kafka.common.message.DescribeLogDirsResponseData",
        version: 5i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeLogDirsResponse",
        java_class: "org.apache.kafka.common.message.DescribeLogDirsResponseData",
        version: 1i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeLogDirsResponse",
        java_class: "org.apache.kafka.common.message.DescribeLogDirsResponseData",
        version: 2i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeLogDirsResponse",
        java_class: "org.apache.kafka.common.message.DescribeLogDirsResponseData",
        version: 3i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeLogDirsResponse",
        java_class: "org.apache.kafka.common.message.DescribeLogDirsResponseData",
        version: 4i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeLogDirsResponse",
        java_class: "org.apache.kafka.common.message.DescribeLogDirsResponseData",
        version: 5i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeLogDirsResponse",
        java_class: "org.apache.kafka.common.message.DescribeLogDirsResponseData",
        version: 1i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeLogDirsResponse",
        java_class: "org.apache.kafka.common.message.DescribeLogDirsResponseData",
        version: 1i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeLogDirsResponse",
        java_class: "org.apache.kafka.common.message.DescribeLogDirsResponseData",
        version: 1i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeLogDirsResponse",
        java_class: "org.apache.kafka.common.message.DescribeLogDirsResponseData",
        version: 1i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeLogDirsResponse",
        java_class: "org.apache.kafka.common.message.DescribeLogDirsResponseData",
        version: 2i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeLogDirsResponse",
        java_class: "org.apache.kafka.common.message.DescribeLogDirsResponseData",
        version: 2i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeLogDirsResponse",
        java_class: "org.apache.kafka.common.message.DescribeLogDirsResponseData",
        version: 2i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeLogDirsResponse",
        java_class: "org.apache.kafka.common.message.DescribeLogDirsResponseData",
        version: 2i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeLogDirsResponse",
        java_class: "org.apache.kafka.common.message.DescribeLogDirsResponseData",
        version: 3i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeLogDirsResponse",
        java_class: "org.apache.kafka.common.message.DescribeLogDirsResponseData",
        version: 3i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeLogDirsResponse",
        java_class: "org.apache.kafka.common.message.DescribeLogDirsResponseData",
        version: 3i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeLogDirsResponse",
        java_class: "org.apache.kafka.common.message.DescribeLogDirsResponseData",
        version: 3i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeLogDirsResponse",
        java_class: "org.apache.kafka.common.message.DescribeLogDirsResponseData",
        version: 4i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeLogDirsResponse",
        java_class: "org.apache.kafka.common.message.DescribeLogDirsResponseData",
        version: 4i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeLogDirsResponse",
        java_class: "org.apache.kafka.common.message.DescribeLogDirsResponseData",
        version: 4i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeLogDirsResponse",
        java_class: "org.apache.kafka.common.message.DescribeLogDirsResponseData",
        version: 4i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeLogDirsResponse",
        java_class: "org.apache.kafka.common.message.DescribeLogDirsResponseData",
        version: 5i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeLogDirsResponse",
        java_class: "org.apache.kafka.common.message.DescribeLogDirsResponseData",
        version: 5i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeLogDirsResponse",
        java_class: "org.apache.kafka.common.message.DescribeLogDirsResponseData",
        version: 5i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeLogDirsResponse",
        java_class: "org.apache.kafka.common.message.DescribeLogDirsResponseData",
        version: 5i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
];
pub(crate) fn append_protocol_cases(cases: &mut Vec<crate::MatrixCase>) {
    cases.extend_from_slice(MATRIX_CASES);
}
