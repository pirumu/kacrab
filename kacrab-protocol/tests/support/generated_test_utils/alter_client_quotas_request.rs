use bytes::{Bytes, BytesMut};
use kacrab_protocol::{generated::alter_client_quotas_request::*, *};

use crate::TestInstance;

impl TestInstance for AlterClientQuotasRequestData {
    fn test_populated() -> Self {
        Self {
            entries: vec![<EntryData as TestInstance>::test_populated()],
            validate_only: true,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals() -> Self {
        Self {
            entries: vec![<EntryData as TestInstance>::test_null_optionals()],
            validate_only: false,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            entries: Vec::new(),
            validate_only: false,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            entries: vec![
                <EntryData as TestInstance>::test_populated(),
                <EntryData as TestInstance>::test_multi_element_collections(),
            ],
            validate_only: false,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            entries: vec![<EntryData as TestInstance>::test_numeric_boundaries()],
            validate_only: true,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            entries: vec![<EntryData as TestInstance>::test_tagged_fields()],
            validate_only: true,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for EntryData {
    fn test_populated() -> Self {
        Self {
            entity: vec![<EntityData as TestInstance>::test_populated()],
            ops: vec![<OpData as TestInstance>::test_populated()],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals() -> Self {
        drop(Self::default());
        Self {
            entity: vec![<EntityData as TestInstance>::test_null_optionals()],
            ops: vec![<OpData as TestInstance>::test_null_optionals()],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            entity: Vec::new(),
            ops: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            entity: vec![
                <EntityData as TestInstance>::test_populated(),
                <EntityData as TestInstance>::test_multi_element_collections(),
            ],
            ops: vec![
                <OpData as TestInstance>::test_populated(),
                <OpData as TestInstance>::test_multi_element_collections(),
            ],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            entity: vec![<EntityData as TestInstance>::test_numeric_boundaries()],
            ops: vec![<OpData as TestInstance>::test_numeric_boundaries()],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            entity: vec![<EntityData as TestInstance>::test_tagged_fields()],
            ops: vec![<OpData as TestInstance>::test_tagged_fields()],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for EntityData {
    fn test_populated() -> Self {
        Self {
            entity_type: KafkaString::from("test".to_owned()),
            entity_name: Some(KafkaString::from("test".to_owned())),
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals() -> Self {
        drop(Self::default());
        Self {
            entity_type: KafkaString::default(),
            entity_name: None,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            entity_type: KafkaString::default(),
            entity_name: Some(KafkaString::default()),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            entity_type: KafkaString::from("test-2".to_owned()),
            entity_name: Some(KafkaString::from("test-2".to_owned())),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            entity_type: KafkaString::from("boundary".to_owned()),
            entity_name: Some(KafkaString::from("boundary".to_owned())),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            entity_type: KafkaString::from("test".to_owned()),
            entity_name: Some(KafkaString::from("test".to_owned())),
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for OpData {
    fn test_populated() -> Self {
        Self {
            key: KafkaString::from("test".to_owned()),
            value: 6.25_f64,
            remove: true,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals() -> Self {
        drop(Self::default());
        Self {
            key: KafkaString::default(),
            value: 0.0_f64,
            remove: false,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            key: KafkaString::default(),
            value: 0.0_f64,
            remove: false,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            key: KafkaString::from("test-2".to_owned()),
            value: 7.5_f64,
            remove: false,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            key: KafkaString::from("boundary".to_owned()),
            value: f64::MIN,
            remove: true,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            key: KafkaString::from("test".to_owned()),
            value: 6.25_f64,
            remove: true,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
fn encode_populated(version: i16) -> crate::MatrixResult<String> {
    let message = <AlterClientQuotasRequestData as TestInstance>::test_populated();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_null_optionals(version: i16) -> crate::MatrixResult<String> {
    let message = <AlterClientQuotasRequestData as TestInstance>::test_null_optionals();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_empty_collections(version: i16) -> crate::MatrixResult<String> {
    let message = <AlterClientQuotasRequestData as TestInstance>::test_empty_collections();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_multi_element_collections(version: i16) -> crate::MatrixResult<String> {
    let message = <AlterClientQuotasRequestData as TestInstance>::test_multi_element_collections();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_numeric_boundaries(version: i16) -> crate::MatrixResult<String> {
    let message = <AlterClientQuotasRequestData as TestInstance>::test_numeric_boundaries();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_tagged_fields(version: i16) -> crate::MatrixResult<String> {
    let message = <AlterClientQuotasRequestData as TestInstance>::test_tagged_fields();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn reencode(version: i16, hex_input: &str) -> crate::MatrixResult<String> {
    let mut input = Bytes::from(crate::decode_hex(hex_input)?);
    let message = AlterClientQuotasRequestData::read(&mut input, version)?;
    crate::ensure_input_consumed(&input)?;
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
const MATRIX_CASES: &[crate::MatrixCase] = &[
    crate::MatrixCase {
        schema_name: "AlterClientQuotasRequest",
        java_class: "org.apache.kafka.common.message.AlterClientQuotasRequestData",
        version: 0i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AlterClientQuotasRequest",
        java_class: "org.apache.kafka.common.message.AlterClientQuotasRequestData",
        version: 1i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AlterClientQuotasRequest",
        java_class: "org.apache.kafka.common.message.AlterClientQuotasRequestData",
        version: 0i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AlterClientQuotasRequest",
        java_class: "org.apache.kafka.common.message.AlterClientQuotasRequestData",
        version: 1i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AlterClientQuotasRequest",
        java_class: "org.apache.kafka.common.message.AlterClientQuotasRequestData",
        version: 0i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AlterClientQuotasRequest",
        java_class: "org.apache.kafka.common.message.AlterClientQuotasRequestData",
        version: 0i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AlterClientQuotasRequest",
        java_class: "org.apache.kafka.common.message.AlterClientQuotasRequestData",
        version: 0i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AlterClientQuotasRequest",
        java_class: "org.apache.kafka.common.message.AlterClientQuotasRequestData",
        version: 0i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AlterClientQuotasRequest",
        java_class: "org.apache.kafka.common.message.AlterClientQuotasRequestData",
        version: 1i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AlterClientQuotasRequest",
        java_class: "org.apache.kafka.common.message.AlterClientQuotasRequestData",
        version: 1i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AlterClientQuotasRequest",
        java_class: "org.apache.kafka.common.message.AlterClientQuotasRequestData",
        version: 1i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AlterClientQuotasRequest",
        java_class: "org.apache.kafka.common.message.AlterClientQuotasRequestData",
        version: 1i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
];
pub(crate) fn append_protocol_cases(cases: &mut Vec<crate::MatrixCase>) {
    cases.extend_from_slice(MATRIX_CASES);
}
