use bytes::{Bytes, BytesMut};
use kacrab_protocol::{generated::update_features_request::*, *};

use crate::TestInstance;

impl TestInstance for UpdateFeaturesRequestData {
    fn test_populated() -> Self {
        Self {
            timeout_ms: 12345_i32,
            feature_updates: vec![<FeatureUpdateKey as TestInstance>::test_populated()],
            validate_only: true,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals() -> Self {
        Self {
            timeout_ms: 0_i32,
            feature_updates: vec![<FeatureUpdateKey as TestInstance>::test_null_optionals()],
            validate_only: false,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            timeout_ms: 0_i32,
            feature_updates: Vec::new(),
            validate_only: false,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            timeout_ms: 23456_i32,
            feature_updates: vec![
                <FeatureUpdateKey as TestInstance>::test_populated(),
                <FeatureUpdateKey as TestInstance>::test_multi_element_collections(),
            ],
            validate_only: false,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            timeout_ms: i32::MIN,
            feature_updates: vec![<FeatureUpdateKey as TestInstance>::test_numeric_boundaries()],
            validate_only: true,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            timeout_ms: 12345_i32,
            feature_updates: vec![<FeatureUpdateKey as TestInstance>::test_tagged_fields()],
            validate_only: true,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for FeatureUpdateKey {
    fn test_populated() -> Self {
        Self {
            feature: KafkaString::from("test".to_owned()),
            max_version_level: 42_i16,
            allow_downgrade: true,
            upgrade_type: 7_i8,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals() -> Self {
        drop(Self::default());
        Self {
            feature: KafkaString::default(),
            max_version_level: 0_i16,
            allow_downgrade: false,
            upgrade_type: 0_i8,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            feature: KafkaString::default(),
            max_version_level: 0_i16,
            allow_downgrade: false,
            upgrade_type: 0_i8,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            feature: KafkaString::from("test-2".to_owned()),
            max_version_level: 43_i16,
            allow_downgrade: false,
            upgrade_type: 8_i8,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            feature: KafkaString::from("boundary".to_owned()),
            max_version_level: i16::MIN,
            allow_downgrade: true,
            upgrade_type: i8::MIN,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            feature: KafkaString::from("test".to_owned()),
            max_version_level: 42_i16,
            allow_downgrade: true,
            upgrade_type: 7_i8,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
fn encode_populated(version: i16) -> crate::MatrixResult<String> {
    let message = <UpdateFeaturesRequestData as TestInstance>::test_populated();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_null_optionals(version: i16) -> crate::MatrixResult<String> {
    let message = <UpdateFeaturesRequestData as TestInstance>::test_null_optionals();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_empty_collections(version: i16) -> crate::MatrixResult<String> {
    let message = <UpdateFeaturesRequestData as TestInstance>::test_empty_collections();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_multi_element_collections(version: i16) -> crate::MatrixResult<String> {
    let message = <UpdateFeaturesRequestData as TestInstance>::test_multi_element_collections();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_numeric_boundaries(version: i16) -> crate::MatrixResult<String> {
    let message = <UpdateFeaturesRequestData as TestInstance>::test_numeric_boundaries();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_tagged_fields(version: i16) -> crate::MatrixResult<String> {
    let message = <UpdateFeaturesRequestData as TestInstance>::test_tagged_fields();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn reencode(version: i16, hex_input: &str) -> crate::MatrixResult<String> {
    let mut input = Bytes::from(crate::decode_hex(hex_input)?);
    let message = UpdateFeaturesRequestData::read(&mut input, version)?;
    crate::ensure_input_consumed(&input)?;
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
const MATRIX_CASES: &[crate::MatrixCase] = &[
    crate::MatrixCase {
        schema_name: "UpdateFeaturesRequest",
        java_class: "org.apache.kafka.common.message.UpdateFeaturesRequestData",
        version: 0i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "UpdateFeaturesRequest",
        java_class: "org.apache.kafka.common.message.UpdateFeaturesRequestData",
        version: 1i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "UpdateFeaturesRequest",
        java_class: "org.apache.kafka.common.message.UpdateFeaturesRequestData",
        version: 2i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "UpdateFeaturesRequest",
        java_class: "org.apache.kafka.common.message.UpdateFeaturesRequestData",
        version: 0i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "UpdateFeaturesRequest",
        java_class: "org.apache.kafka.common.message.UpdateFeaturesRequestData",
        version: 1i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "UpdateFeaturesRequest",
        java_class: "org.apache.kafka.common.message.UpdateFeaturesRequestData",
        version: 2i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "UpdateFeaturesRequest",
        java_class: "org.apache.kafka.common.message.UpdateFeaturesRequestData",
        version: 0i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "UpdateFeaturesRequest",
        java_class: "org.apache.kafka.common.message.UpdateFeaturesRequestData",
        version: 0i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "UpdateFeaturesRequest",
        java_class: "org.apache.kafka.common.message.UpdateFeaturesRequestData",
        version: 0i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "UpdateFeaturesRequest",
        java_class: "org.apache.kafka.common.message.UpdateFeaturesRequestData",
        version: 0i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "UpdateFeaturesRequest",
        java_class: "org.apache.kafka.common.message.UpdateFeaturesRequestData",
        version: 1i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "UpdateFeaturesRequest",
        java_class: "org.apache.kafka.common.message.UpdateFeaturesRequestData",
        version: 1i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "UpdateFeaturesRequest",
        java_class: "org.apache.kafka.common.message.UpdateFeaturesRequestData",
        version: 1i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "UpdateFeaturesRequest",
        java_class: "org.apache.kafka.common.message.UpdateFeaturesRequestData",
        version: 1i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "UpdateFeaturesRequest",
        java_class: "org.apache.kafka.common.message.UpdateFeaturesRequestData",
        version: 2i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "UpdateFeaturesRequest",
        java_class: "org.apache.kafka.common.message.UpdateFeaturesRequestData",
        version: 2i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "UpdateFeaturesRequest",
        java_class: "org.apache.kafka.common.message.UpdateFeaturesRequestData",
        version: 2i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "UpdateFeaturesRequest",
        java_class: "org.apache.kafka.common.message.UpdateFeaturesRequestData",
        version: 2i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
];
pub(crate) fn append_protocol_cases(cases: &mut Vec<crate::MatrixCase>) {
    cases.extend_from_slice(MATRIX_CASES);
}
