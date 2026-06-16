use bytes::{Bytes, BytesMut};
use kacrab_protocol::{generated::api_versions_response::*, *};

use crate::TestInstance;

impl TestInstance for ApiVersionsResponseData {
    fn test_populated() -> Self {
        Self {
            error_code: 42_i16,
            api_keys: vec![<ApiVersion as TestInstance>::test_populated()],
            throttle_time_ms: 12345_i32,
            supported_features: vec![<SupportedFeatureKey as TestInstance>::test_populated()],
            finalized_features_epoch: 9_876_543_210_i64,
            finalized_features: vec![<FinalizedFeatureKey as TestInstance>::test_populated()],
            zk_migration_ready: true,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals() -> Self {
        drop(<SupportedFeatureKey as TestInstance>::test_null_optionals());
        drop(<FinalizedFeatureKey as TestInstance>::test_null_optionals());
        Self {
            error_code: 0_i16,
            api_keys: vec![<ApiVersion as TestInstance>::test_null_optionals()],
            throttle_time_ms: 0_i32,
            supported_features: Vec::new(),
            finalized_features_epoch: -1i64,
            finalized_features: Vec::new(),
            zk_migration_ready: false,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            error_code: 0_i16,
            api_keys: Vec::new(),
            throttle_time_ms: 0_i32,
            supported_features: Vec::new(),
            finalized_features_epoch: 0_i64,
            finalized_features: Vec::new(),
            zk_migration_ready: false,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            error_code: 43_i16,
            api_keys: vec![
                <ApiVersion as TestInstance>::test_populated(),
                <ApiVersion as TestInstance>::test_multi_element_collections(),
            ],
            throttle_time_ms: 23456_i32,
            supported_features: vec![
                <SupportedFeatureKey as TestInstance>::test_populated(),
                <SupportedFeatureKey as TestInstance>::test_multi_element_collections(),
            ],
            finalized_features_epoch: 9_876_543_211_i64,
            finalized_features: vec![
                <FinalizedFeatureKey as TestInstance>::test_populated(),
                <FinalizedFeatureKey as TestInstance>::test_multi_element_collections(),
            ],
            zk_migration_ready: false,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            error_code: i16::MIN,
            api_keys: vec![<ApiVersion as TestInstance>::test_numeric_boundaries()],
            throttle_time_ms: i32::MIN,
            supported_features: vec![
                <SupportedFeatureKey as TestInstance>::test_numeric_boundaries(),
            ],
            finalized_features_epoch: i64::MIN,
            finalized_features: vec![
                <FinalizedFeatureKey as TestInstance>::test_numeric_boundaries(),
            ],
            zk_migration_ready: true,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            error_code: 42_i16,
            api_keys: vec![<ApiVersion as TestInstance>::test_tagged_fields()],
            throttle_time_ms: 12345_i32,
            supported_features: vec![<SupportedFeatureKey as TestInstance>::test_tagged_fields()],
            finalized_features_epoch: 9_876_543_210_i64,
            finalized_features: vec![<FinalizedFeatureKey as TestInstance>::test_tagged_fields()],
            zk_migration_ready: true,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for ApiVersion {
    fn test_populated() -> Self {
        Self {
            api_key: 42_i16,
            min_version: 42_i16,
            max_version: 42_i16,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals() -> Self {
        drop(Self::default());
        Self {
            api_key: 0_i16,
            min_version: 0_i16,
            max_version: 0_i16,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            api_key: 0_i16,
            min_version: 0_i16,
            max_version: 0_i16,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            api_key: 43_i16,
            min_version: 43_i16,
            max_version: 43_i16,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            api_key: i16::MIN,
            min_version: i16::MIN,
            max_version: i16::MIN,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            api_key: 42_i16,
            min_version: 42_i16,
            max_version: 42_i16,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for SupportedFeatureKey {
    fn test_populated() -> Self {
        Self {
            name: KafkaString::from("test".to_owned()),
            min_version: 42_i16,
            max_version: 42_i16,
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
            min_version: 0_i16,
            max_version: 0_i16,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            name: KafkaString::default(),
            min_version: 0_i16,
            max_version: 0_i16,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            name: KafkaString::from("test-2".to_owned()),
            min_version: 43_i16,
            max_version: 43_i16,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            name: KafkaString::from("boundary".to_owned()),
            min_version: i16::MIN,
            max_version: i16::MIN,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            name: KafkaString::from("test".to_owned()),
            min_version: 42_i16,
            max_version: 42_i16,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for FinalizedFeatureKey {
    fn test_populated() -> Self {
        Self {
            name: KafkaString::from("test".to_owned()),
            max_version_level: 42_i16,
            min_version_level: 42_i16,
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
            max_version_level: 0_i16,
            min_version_level: 0_i16,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            name: KafkaString::default(),
            max_version_level: 0_i16,
            min_version_level: 0_i16,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            name: KafkaString::from("test-2".to_owned()),
            max_version_level: 43_i16,
            min_version_level: 43_i16,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            name: KafkaString::from("boundary".to_owned()),
            max_version_level: i16::MIN,
            min_version_level: i16::MIN,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            name: KafkaString::from("test".to_owned()),
            max_version_level: 42_i16,
            min_version_level: 42_i16,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
fn encode_populated(version: i16) -> crate::MatrixResult<String> {
    let message = <ApiVersionsResponseData as TestInstance>::test_populated();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_populated(version: i16) -> crate::MatrixResult<usize> {
    let message = <ApiVersionsResponseData as TestInstance>::test_populated();
    Ok(message.encoded_len(version)?)
}
fn encode_null_optionals(version: i16) -> crate::MatrixResult<String> {
    let message = <ApiVersionsResponseData as TestInstance>::test_null_optionals();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_null_optionals(version: i16) -> crate::MatrixResult<usize> {
    let message = <ApiVersionsResponseData as TestInstance>::test_null_optionals();
    Ok(message.encoded_len(version)?)
}
fn encode_empty_collections(version: i16) -> crate::MatrixResult<String> {
    let message = <ApiVersionsResponseData as TestInstance>::test_empty_collections();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_empty_collections(version: i16) -> crate::MatrixResult<usize> {
    let message = <ApiVersionsResponseData as TestInstance>::test_empty_collections();
    Ok(message.encoded_len(version)?)
}
fn encode_multi_element_collections(version: i16) -> crate::MatrixResult<String> {
    let message = <ApiVersionsResponseData as TestInstance>::test_multi_element_collections();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_multi_element_collections(version: i16) -> crate::MatrixResult<usize> {
    let message = <ApiVersionsResponseData as TestInstance>::test_multi_element_collections();
    Ok(message.encoded_len(version)?)
}
fn encode_numeric_boundaries(version: i16) -> crate::MatrixResult<String> {
    let message = <ApiVersionsResponseData as TestInstance>::test_numeric_boundaries();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_numeric_boundaries(version: i16) -> crate::MatrixResult<usize> {
    let message = <ApiVersionsResponseData as TestInstance>::test_numeric_boundaries();
    Ok(message.encoded_len(version)?)
}
fn encode_tagged_fields(version: i16) -> crate::MatrixResult<String> {
    let message = <ApiVersionsResponseData as TestInstance>::test_tagged_fields();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_tagged_fields(version: i16) -> crate::MatrixResult<usize> {
    let message = <ApiVersionsResponseData as TestInstance>::test_tagged_fields();
    Ok(message.encoded_len(version)?)
}
fn reencode(version: i16, hex_input: &str) -> crate::MatrixResult<String> {
    let mut input = Bytes::from(crate::decode_hex(hex_input)?);
    let message = ApiVersionsResponseData::read(&mut input, version)?;
    crate::ensure_input_consumed(&input)?;
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
const MATRIX_CASES: &[crate::MatrixCase] = &[
    crate::MatrixCase {
        schema_name: "ApiVersionsResponse",
        java_class: "org.apache.kafka.common.message.ApiVersionsResponseData",
        version: 0i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ApiVersionsResponse",
        java_class: "org.apache.kafka.common.message.ApiVersionsResponseData",
        version: 1i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ApiVersionsResponse",
        java_class: "org.apache.kafka.common.message.ApiVersionsResponseData",
        version: 2i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ApiVersionsResponse",
        java_class: "org.apache.kafka.common.message.ApiVersionsResponseData",
        version: 3i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ApiVersionsResponse",
        java_class: "org.apache.kafka.common.message.ApiVersionsResponseData",
        version: 4i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ApiVersionsResponse",
        java_class: "org.apache.kafka.common.message.ApiVersionsResponseData",
        version: 0i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ApiVersionsResponse",
        java_class: "org.apache.kafka.common.message.ApiVersionsResponseData",
        version: 1i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ApiVersionsResponse",
        java_class: "org.apache.kafka.common.message.ApiVersionsResponseData",
        version: 2i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ApiVersionsResponse",
        java_class: "org.apache.kafka.common.message.ApiVersionsResponseData",
        version: 3i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ApiVersionsResponse",
        java_class: "org.apache.kafka.common.message.ApiVersionsResponseData",
        version: 4i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ApiVersionsResponse",
        java_class: "org.apache.kafka.common.message.ApiVersionsResponseData",
        version: 0i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ApiVersionsResponse",
        java_class: "org.apache.kafka.common.message.ApiVersionsResponseData",
        version: 0i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ApiVersionsResponse",
        java_class: "org.apache.kafka.common.message.ApiVersionsResponseData",
        version: 0i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ApiVersionsResponse",
        java_class: "org.apache.kafka.common.message.ApiVersionsResponseData",
        version: 0i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ApiVersionsResponse",
        java_class: "org.apache.kafka.common.message.ApiVersionsResponseData",
        version: 1i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ApiVersionsResponse",
        java_class: "org.apache.kafka.common.message.ApiVersionsResponseData",
        version: 1i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ApiVersionsResponse",
        java_class: "org.apache.kafka.common.message.ApiVersionsResponseData",
        version: 1i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ApiVersionsResponse",
        java_class: "org.apache.kafka.common.message.ApiVersionsResponseData",
        version: 1i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ApiVersionsResponse",
        java_class: "org.apache.kafka.common.message.ApiVersionsResponseData",
        version: 2i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ApiVersionsResponse",
        java_class: "org.apache.kafka.common.message.ApiVersionsResponseData",
        version: 2i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ApiVersionsResponse",
        java_class: "org.apache.kafka.common.message.ApiVersionsResponseData",
        version: 2i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ApiVersionsResponse",
        java_class: "org.apache.kafka.common.message.ApiVersionsResponseData",
        version: 2i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ApiVersionsResponse",
        java_class: "org.apache.kafka.common.message.ApiVersionsResponseData",
        version: 3i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ApiVersionsResponse",
        java_class: "org.apache.kafka.common.message.ApiVersionsResponseData",
        version: 3i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ApiVersionsResponse",
        java_class: "org.apache.kafka.common.message.ApiVersionsResponseData",
        version: 3i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ApiVersionsResponse",
        java_class: "org.apache.kafka.common.message.ApiVersionsResponseData",
        version: 3i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ApiVersionsResponse",
        java_class: "org.apache.kafka.common.message.ApiVersionsResponseData",
        version: 4i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ApiVersionsResponse",
        java_class: "org.apache.kafka.common.message.ApiVersionsResponseData",
        version: 4i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ApiVersionsResponse",
        java_class: "org.apache.kafka.common.message.ApiVersionsResponseData",
        version: 4i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "ApiVersionsResponse",
        java_class: "org.apache.kafka.common.message.ApiVersionsResponseData",
        version: 4i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
];
pub(crate) fn append_protocol_cases(cases: &mut Vec<crate::MatrixCase>) {
    cases.extend_from_slice(MATRIX_CASES);
}
