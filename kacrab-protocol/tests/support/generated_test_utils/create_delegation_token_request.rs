use bytes::{Bytes, BytesMut};
use kacrab_protocol::{generated::create_delegation_token_request::*, *};

use crate::TestInstance;

impl TestInstance for CreateDelegationTokenRequestData {
    fn test_populated() -> Self {
        Self {
            owner_principal_type: Some(KafkaString::from("test".to_owned())),
            owner_principal_name: Some(KafkaString::from("test".to_owned())),
            renewers: vec![<CreatableRenewers as TestInstance>::test_populated()],
            max_lifetime_ms: 9_876_543_210_i64,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals() -> Self {
        Self {
            owner_principal_type: None,
            owner_principal_name: None,
            renewers: vec![<CreatableRenewers as TestInstance>::test_null_optionals()],
            max_lifetime_ms: 0_i64,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            owner_principal_type: Some(KafkaString::default()),
            owner_principal_name: Some(KafkaString::default()),
            renewers: Vec::new(),
            max_lifetime_ms: 0_i64,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            owner_principal_type: Some(KafkaString::from("test-2".to_owned())),
            owner_principal_name: Some(KafkaString::from("test-2".to_owned())),
            renewers: vec![
                <CreatableRenewers as TestInstance>::test_populated(),
                <CreatableRenewers as TestInstance>::test_multi_element_collections(),
            ],
            max_lifetime_ms: 9_876_543_211_i64,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            owner_principal_type: Some(KafkaString::from("boundary".to_owned())),
            owner_principal_name: Some(KafkaString::from("boundary".to_owned())),
            renewers: vec![<CreatableRenewers as TestInstance>::test_numeric_boundaries()],
            max_lifetime_ms: i64::MIN,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            owner_principal_type: Some(KafkaString::from("test".to_owned())),
            owner_principal_name: Some(KafkaString::from("test".to_owned())),
            renewers: vec![<CreatableRenewers as TestInstance>::test_tagged_fields()],
            max_lifetime_ms: 9_876_543_210_i64,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for CreatableRenewers {
    fn test_populated() -> Self {
        Self {
            principal_type: KafkaString::from("test".to_owned()),
            principal_name: KafkaString::from("test".to_owned()),
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals() -> Self {
        drop(Self::default());
        Self {
            principal_type: KafkaString::default(),
            principal_name: KafkaString::default(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            principal_type: KafkaString::default(),
            principal_name: KafkaString::default(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            principal_type: KafkaString::from("test-2".to_owned()),
            principal_name: KafkaString::from("test-2".to_owned()),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            principal_type: KafkaString::from("boundary".to_owned()),
            principal_name: KafkaString::from("boundary".to_owned()),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            principal_type: KafkaString::from("test".to_owned()),
            principal_name: KafkaString::from("test".to_owned()),
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
fn encode_populated(version: i16) -> crate::MatrixResult<String> {
    let message = <CreateDelegationTokenRequestData as TestInstance>::test_populated();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_null_optionals(version: i16) -> crate::MatrixResult<String> {
    let message = <CreateDelegationTokenRequestData as TestInstance>::test_null_optionals();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_empty_collections(version: i16) -> crate::MatrixResult<String> {
    let message = <CreateDelegationTokenRequestData as TestInstance>::test_empty_collections();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_multi_element_collections(version: i16) -> crate::MatrixResult<String> {
    let message =
        <CreateDelegationTokenRequestData as TestInstance>::test_multi_element_collections();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_numeric_boundaries(version: i16) -> crate::MatrixResult<String> {
    let message = <CreateDelegationTokenRequestData as TestInstance>::test_numeric_boundaries();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_tagged_fields(version: i16) -> crate::MatrixResult<String> {
    let message = <CreateDelegationTokenRequestData as TestInstance>::test_tagged_fields();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn reencode(version: i16, hex_input: &str) -> crate::MatrixResult<String> {
    let mut input = Bytes::from(crate::decode_hex(hex_input)?);
    let message = CreateDelegationTokenRequestData::read(&mut input, version)?;
    crate::ensure_input_consumed(&input)?;
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
const MATRIX_CASES: &[crate::MatrixCase] = &[
    crate::MatrixCase {
        schema_name: "CreateDelegationTokenRequest",
        java_class: "org.apache.kafka.common.message.CreateDelegationTokenRequestData",
        version: 1i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "CreateDelegationTokenRequest",
        java_class: "org.apache.kafka.common.message.CreateDelegationTokenRequestData",
        version: 2i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "CreateDelegationTokenRequest",
        java_class: "org.apache.kafka.common.message.CreateDelegationTokenRequestData",
        version: 3i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "CreateDelegationTokenRequest",
        java_class: "org.apache.kafka.common.message.CreateDelegationTokenRequestData",
        version: 1i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "CreateDelegationTokenRequest",
        java_class: "org.apache.kafka.common.message.CreateDelegationTokenRequestData",
        version: 2i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "CreateDelegationTokenRequest",
        java_class: "org.apache.kafka.common.message.CreateDelegationTokenRequestData",
        version: 3i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "CreateDelegationTokenRequest",
        java_class: "org.apache.kafka.common.message.CreateDelegationTokenRequestData",
        version: 1i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "CreateDelegationTokenRequest",
        java_class: "org.apache.kafka.common.message.CreateDelegationTokenRequestData",
        version: 1i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "CreateDelegationTokenRequest",
        java_class: "org.apache.kafka.common.message.CreateDelegationTokenRequestData",
        version: 1i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "CreateDelegationTokenRequest",
        java_class: "org.apache.kafka.common.message.CreateDelegationTokenRequestData",
        version: 1i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "CreateDelegationTokenRequest",
        java_class: "org.apache.kafka.common.message.CreateDelegationTokenRequestData",
        version: 2i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "CreateDelegationTokenRequest",
        java_class: "org.apache.kafka.common.message.CreateDelegationTokenRequestData",
        version: 2i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "CreateDelegationTokenRequest",
        java_class: "org.apache.kafka.common.message.CreateDelegationTokenRequestData",
        version: 2i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "CreateDelegationTokenRequest",
        java_class: "org.apache.kafka.common.message.CreateDelegationTokenRequestData",
        version: 2i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "CreateDelegationTokenRequest",
        java_class: "org.apache.kafka.common.message.CreateDelegationTokenRequestData",
        version: 3i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "CreateDelegationTokenRequest",
        java_class: "org.apache.kafka.common.message.CreateDelegationTokenRequestData",
        version: 3i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "CreateDelegationTokenRequest",
        java_class: "org.apache.kafka.common.message.CreateDelegationTokenRequestData",
        version: 3i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "CreateDelegationTokenRequest",
        java_class: "org.apache.kafka.common.message.CreateDelegationTokenRequestData",
        version: 3i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
];
pub(crate) fn append_protocol_cases(cases: &mut Vec<crate::MatrixCase>) {
    cases.extend_from_slice(MATRIX_CASES);
}
