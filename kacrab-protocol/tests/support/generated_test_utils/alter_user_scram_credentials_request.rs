use bytes::{Bytes, BytesMut};
use kacrab_protocol::{generated::alter_user_scram_credentials_request::*, *};

use crate::TestInstance;

impl TestInstance for AlterUserScramCredentialsRequestData {
    fn test_populated() -> Self {
        Self {
            deletions: vec![<ScramCredentialDeletion as TestInstance>::test_populated()],
            upsertions: vec![<ScramCredentialUpsertion as TestInstance>::test_populated()],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals() -> Self {
        Self {
            deletions: vec![<ScramCredentialDeletion as TestInstance>::test_null_optionals()],
            upsertions: vec![<ScramCredentialUpsertion as TestInstance>::test_null_optionals()],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            deletions: Vec::new(),
            upsertions: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            deletions: vec![
                <ScramCredentialDeletion as TestInstance>::test_populated(),
                <ScramCredentialDeletion as TestInstance>::test_multi_element_collections(),
            ],
            upsertions: vec![
                <ScramCredentialUpsertion as TestInstance>::test_populated(),
                <ScramCredentialUpsertion as TestInstance>::test_multi_element_collections(),
            ],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            deletions: vec![<ScramCredentialDeletion as TestInstance>::test_numeric_boundaries()],
            upsertions: vec![<ScramCredentialUpsertion as TestInstance>::test_numeric_boundaries()],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            deletions: vec![<ScramCredentialDeletion as TestInstance>::test_tagged_fields()],
            upsertions: vec![<ScramCredentialUpsertion as TestInstance>::test_tagged_fields()],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for ScramCredentialDeletion {
    fn test_populated() -> Self {
        Self {
            name: KafkaString::from("test".to_owned()),
            mechanism: 7_i8,
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
            mechanism: 0_i8,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            name: KafkaString::default(),
            mechanism: 0_i8,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            name: KafkaString::from("test-2".to_owned()),
            mechanism: 8_i8,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            name: KafkaString::from("boundary".to_owned()),
            mechanism: i8::MIN,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            name: KafkaString::from("test".to_owned()),
            mechanism: 7_i8,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for ScramCredentialUpsertion {
    fn test_populated() -> Self {
        Self {
            name: KafkaString::from("test".to_owned()),
            mechanism: 7_i8,
            iterations: 12345_i32,
            salt: Bytes::from_static(b"\xca\xfe"),
            salted_password: Bytes::from_static(b"\xca\xfe"),
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
            mechanism: 0_i8,
            iterations: 0_i32,
            salt: Bytes::new(),
            salted_password: Bytes::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            name: KafkaString::default(),
            mechanism: 0_i8,
            iterations: 0_i32,
            salt: Bytes::new(),
            salted_password: Bytes::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            name: KafkaString::from("test-2".to_owned()),
            mechanism: 8_i8,
            iterations: 23456_i32,
            salt: Bytes::from_static(b"\x00\xff"),
            salted_password: Bytes::from_static(b"\x00\xff"),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            name: KafkaString::from("boundary".to_owned()),
            mechanism: i8::MIN,
            iterations: i32::MIN,
            salt: Bytes::from_static(b"\x00\xff"),
            salted_password: Bytes::from_static(b"\x00\xff"),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            name: KafkaString::from("test".to_owned()),
            mechanism: 7_i8,
            iterations: 12345_i32,
            salt: Bytes::from_static(b"\xca\xfe"),
            salted_password: Bytes::from_static(b"\xca\xfe"),
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
fn encode_populated(version: i16) -> crate::MatrixResult<String> {
    let message = <AlterUserScramCredentialsRequestData as TestInstance>::test_populated();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_null_optionals(version: i16) -> crate::MatrixResult<String> {
    let message = <AlterUserScramCredentialsRequestData as TestInstance>::test_null_optionals();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_empty_collections(version: i16) -> crate::MatrixResult<String> {
    let message = <AlterUserScramCredentialsRequestData as TestInstance>::test_empty_collections();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_multi_element_collections(version: i16) -> crate::MatrixResult<String> {
    let message =
        <AlterUserScramCredentialsRequestData as TestInstance>::test_multi_element_collections();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_numeric_boundaries(version: i16) -> crate::MatrixResult<String> {
    let message = <AlterUserScramCredentialsRequestData as TestInstance>::test_numeric_boundaries();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_tagged_fields(version: i16) -> crate::MatrixResult<String> {
    let message = <AlterUserScramCredentialsRequestData as TestInstance>::test_tagged_fields();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn reencode(version: i16, hex_input: &str) -> crate::MatrixResult<String> {
    let mut input = Bytes::from(crate::decode_hex(hex_input)?);
    let message = AlterUserScramCredentialsRequestData::read(&mut input, version)?;
    crate::ensure_input_consumed(&input)?;
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
const MATRIX_CASES: &[crate::MatrixCase] = &[
    crate::MatrixCase {
        schema_name: "AlterUserScramCredentialsRequest",
        java_class: "org.apache.kafka.common.message.AlterUserScramCredentialsRequestData",
        version: 0i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AlterUserScramCredentialsRequest",
        java_class: "org.apache.kafka.common.message.AlterUserScramCredentialsRequestData",
        version: 0i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AlterUserScramCredentialsRequest",
        java_class: "org.apache.kafka.common.message.AlterUserScramCredentialsRequestData",
        version: 0i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AlterUserScramCredentialsRequest",
        java_class: "org.apache.kafka.common.message.AlterUserScramCredentialsRequestData",
        version: 0i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AlterUserScramCredentialsRequest",
        java_class: "org.apache.kafka.common.message.AlterUserScramCredentialsRequestData",
        version: 0i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AlterUserScramCredentialsRequest",
        java_class: "org.apache.kafka.common.message.AlterUserScramCredentialsRequestData",
        version: 0i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
];
pub(crate) fn append_protocol_cases(cases: &mut Vec<crate::MatrixCase>) {
    cases.extend_from_slice(MATRIX_CASES);
}
