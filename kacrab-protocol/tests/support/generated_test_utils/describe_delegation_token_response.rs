use bytes::{Bytes, BytesMut};
use kacrab_protocol::{generated::describe_delegation_token_response::*, *};

use crate::TestInstance;

impl TestInstance for DescribeDelegationTokenResponseData {
    fn test_populated() -> Self {
        Self {
            error_code: 42_i16,
            tokens: vec![<DescribedDelegationToken as TestInstance>::test_populated()],
            throttle_time_ms: 12345_i32,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals() -> Self {
        Self {
            error_code: 0_i16,
            tokens: vec![<DescribedDelegationToken as TestInstance>::test_null_optionals()],
            throttle_time_ms: 0_i32,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            error_code: 0_i16,
            tokens: Vec::new(),
            throttle_time_ms: 0_i32,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            error_code: 43_i16,
            tokens: vec![
                <DescribedDelegationToken as TestInstance>::test_populated(),
                <DescribedDelegationToken as TestInstance>::test_multi_element_collections(),
            ],
            throttle_time_ms: 23456_i32,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            error_code: i16::MIN,
            tokens: vec![<DescribedDelegationToken as TestInstance>::test_numeric_boundaries()],
            throttle_time_ms: i32::MIN,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            error_code: 42_i16,
            tokens: vec![<DescribedDelegationToken as TestInstance>::test_tagged_fields()],
            throttle_time_ms: 12345_i32,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for DescribedDelegationToken {
    fn test_populated() -> Self {
        Self {
            principal_type: KafkaString::from("test".to_owned()),
            principal_name: KafkaString::from("test".to_owned()),
            token_requester_principal_type: KafkaString::from("test".to_owned()),
            token_requester_principal_name: KafkaString::from("test".to_owned()),
            issue_timestamp: 9_876_543_210_i64,
            expiry_timestamp: 9_876_543_210_i64,
            max_timestamp: 9_876_543_210_i64,
            token_id: KafkaString::from("test".to_owned()),
            hmac: Bytes::from_static(b"\xca\xfe"),
            renewers: vec![<DescribedDelegationTokenRenewer as TestInstance>::test_populated()],
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
            token_requester_principal_type: KafkaString::default(),
            token_requester_principal_name: KafkaString::default(),
            issue_timestamp: 0_i64,
            expiry_timestamp: 0_i64,
            max_timestamp: 0_i64,
            token_id: KafkaString::default(),
            hmac: Bytes::new(),
            renewers: vec![
                <DescribedDelegationTokenRenewer as TestInstance>::test_null_optionals(),
            ],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            principal_type: KafkaString::default(),
            principal_name: KafkaString::default(),
            token_requester_principal_type: KafkaString::default(),
            token_requester_principal_name: KafkaString::default(),
            issue_timestamp: 0_i64,
            expiry_timestamp: 0_i64,
            max_timestamp: 0_i64,
            token_id: KafkaString::default(),
            hmac: Bytes::new(),
            renewers: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            principal_type: KafkaString::from("test-2".to_owned()),
            principal_name: KafkaString::from("test-2".to_owned()),
            token_requester_principal_type: KafkaString::from("test-2".to_owned()),
            token_requester_principal_name: KafkaString::from("test-2".to_owned()),
            issue_timestamp: 9_876_543_211_i64,
            expiry_timestamp: 9_876_543_211_i64,
            max_timestamp: 9_876_543_211_i64,
            token_id: KafkaString::from("test-2".to_owned()),
            hmac: Bytes::from_static(b"\x00\xff"),
            renewers: vec![
                <DescribedDelegationTokenRenewer as TestInstance>::test_populated(),
                <DescribedDelegationTokenRenewer as TestInstance>::test_multi_element_collections(),
            ],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            principal_type: KafkaString::from("boundary".to_owned()),
            principal_name: KafkaString::from("boundary".to_owned()),
            token_requester_principal_type: KafkaString::from("boundary".to_owned()),
            token_requester_principal_name: KafkaString::from("boundary".to_owned()),
            issue_timestamp: i64::MIN,
            expiry_timestamp: i64::MIN,
            max_timestamp: i64::MIN,
            token_id: KafkaString::from("boundary".to_owned()),
            hmac: Bytes::from_static(b"\x00\xff"),
            renewers: vec![
                <DescribedDelegationTokenRenewer as TestInstance>::test_numeric_boundaries(),
            ],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            principal_type: KafkaString::from("test".to_owned()),
            principal_name: KafkaString::from("test".to_owned()),
            token_requester_principal_type: KafkaString::from("test".to_owned()),
            token_requester_principal_name: KafkaString::from("test".to_owned()),
            issue_timestamp: 9_876_543_210_i64,
            expiry_timestamp: 9_876_543_210_i64,
            max_timestamp: 9_876_543_210_i64,
            token_id: KafkaString::from("test".to_owned()),
            hmac: Bytes::from_static(b"\xca\xfe"),
            renewers: vec![<DescribedDelegationTokenRenewer as TestInstance>::test_tagged_fields()],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for DescribedDelegationTokenRenewer {
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
    let message = <DescribeDelegationTokenResponseData as TestInstance>::test_populated();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_null_optionals(version: i16) -> crate::MatrixResult<String> {
    let message = <DescribeDelegationTokenResponseData as TestInstance>::test_null_optionals();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_empty_collections(version: i16) -> crate::MatrixResult<String> {
    let message = <DescribeDelegationTokenResponseData as TestInstance>::test_empty_collections();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_multi_element_collections(version: i16) -> crate::MatrixResult<String> {
    let message =
        <DescribeDelegationTokenResponseData as TestInstance>::test_multi_element_collections();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_numeric_boundaries(version: i16) -> crate::MatrixResult<String> {
    let message = <DescribeDelegationTokenResponseData as TestInstance>::test_numeric_boundaries();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_tagged_fields(version: i16) -> crate::MatrixResult<String> {
    let message = <DescribeDelegationTokenResponseData as TestInstance>::test_tagged_fields();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn reencode(version: i16, hex_input: &str) -> crate::MatrixResult<String> {
    let mut input = Bytes::from(crate::decode_hex(hex_input)?);
    let message = DescribeDelegationTokenResponseData::read(&mut input, version)?;
    crate::ensure_input_consumed(&input)?;
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
const MATRIX_CASES: &[crate::MatrixCase] = &[
    crate::MatrixCase {
        schema_name: "DescribeDelegationTokenResponse",
        java_class: "org.apache.kafka.common.message.DescribeDelegationTokenResponseData",
        version: 1i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeDelegationTokenResponse",
        java_class: "org.apache.kafka.common.message.DescribeDelegationTokenResponseData",
        version: 2i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeDelegationTokenResponse",
        java_class: "org.apache.kafka.common.message.DescribeDelegationTokenResponseData",
        version: 3i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeDelegationTokenResponse",
        java_class: "org.apache.kafka.common.message.DescribeDelegationTokenResponseData",
        version: 1i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeDelegationTokenResponse",
        java_class: "org.apache.kafka.common.message.DescribeDelegationTokenResponseData",
        version: 2i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeDelegationTokenResponse",
        java_class: "org.apache.kafka.common.message.DescribeDelegationTokenResponseData",
        version: 3i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeDelegationTokenResponse",
        java_class: "org.apache.kafka.common.message.DescribeDelegationTokenResponseData",
        version: 1i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeDelegationTokenResponse",
        java_class: "org.apache.kafka.common.message.DescribeDelegationTokenResponseData",
        version: 1i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeDelegationTokenResponse",
        java_class: "org.apache.kafka.common.message.DescribeDelegationTokenResponseData",
        version: 1i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeDelegationTokenResponse",
        java_class: "org.apache.kafka.common.message.DescribeDelegationTokenResponseData",
        version: 1i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeDelegationTokenResponse",
        java_class: "org.apache.kafka.common.message.DescribeDelegationTokenResponseData",
        version: 2i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeDelegationTokenResponse",
        java_class: "org.apache.kafka.common.message.DescribeDelegationTokenResponseData",
        version: 2i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeDelegationTokenResponse",
        java_class: "org.apache.kafka.common.message.DescribeDelegationTokenResponseData",
        version: 2i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeDelegationTokenResponse",
        java_class: "org.apache.kafka.common.message.DescribeDelegationTokenResponseData",
        version: 2i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeDelegationTokenResponse",
        java_class: "org.apache.kafka.common.message.DescribeDelegationTokenResponseData",
        version: 3i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeDelegationTokenResponse",
        java_class: "org.apache.kafka.common.message.DescribeDelegationTokenResponseData",
        version: 3i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeDelegationTokenResponse",
        java_class: "org.apache.kafka.common.message.DescribeDelegationTokenResponseData",
        version: 3i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "DescribeDelegationTokenResponse",
        java_class: "org.apache.kafka.common.message.DescribeDelegationTokenResponseData",
        version: 3i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
];
pub(crate) fn append_protocol_cases(cases: &mut Vec<crate::MatrixCase>) {
    cases.extend_from_slice(MATRIX_CASES);
}
