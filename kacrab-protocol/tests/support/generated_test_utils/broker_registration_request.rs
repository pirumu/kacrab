#![allow(
    clippy::all,
    clippy::pedantic,
    clippy::nursery,
    reason = "Generated test fixtures mirror Kafka's schema shape and trade hand-written lint \
              style for reproducible output, matching the generated protocol modules."
)]
use bytes::{Bytes, BytesMut};
use kacrab_protocol::{generated::broker_registration_request::*, *};

use crate::TestInstance;

impl TestInstance for BrokerRegistrationRequestData {
    fn test_populated(version: i16) -> Self {
        Self {
            broker_id: 12345_i32,
            cluster_id: KafkaString::from("test".to_owned()),
            incarnation_id: KafkaUuid::ONE,
            listeners: vec![<Listener as TestInstance>::test_populated(version)],
            features: vec![<Feature as TestInstance>::test_populated(version)],
            rack: Some(KafkaString::from("test".to_owned())),
            is_migrating_zk_broker: if version >= 1 { true } else { false },
            log_dirs: if version >= 2 {
                vec![KafkaUuid::ONE]
            } else {
                Vec::new()
            },
            previous_broker_epoch: if version >= 3 {
                9_876_543_210_i64
            } else {
                -1i64
            },
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals(version: i16) -> Self {
        Self {
            broker_id: 0_i32,
            cluster_id: KafkaString::default(),
            incarnation_id: KafkaUuid::ZERO,
            listeners: vec![<Listener as TestInstance>::test_null_optionals(version)],
            features: vec![<Feature as TestInstance>::test_null_optionals(version)],
            rack: None,
            is_migrating_zk_broker: false,
            log_dirs: if version >= 2 {
                vec![KafkaUuid::ZERO]
            } else {
                Vec::new()
            },
            previous_broker_epoch: if version >= 3 { 0_i64 } else { -1i64 },
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections(version: i16) -> Self {
        Self {
            broker_id: 0_i32,
            cluster_id: KafkaString::default(),
            incarnation_id: KafkaUuid::ZERO,
            listeners: Vec::new(),
            features: Vec::new(),
            rack: Some(KafkaString::default()),
            is_migrating_zk_broker: false,
            log_dirs: Vec::new(),
            previous_broker_epoch: if version >= 3 { 0_i64 } else { -1i64 },
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections(version: i16) -> Self {
        Self {
            broker_id: 23456_i32,
            cluster_id: KafkaString::from("test-2".to_owned()),
            incarnation_id: KafkaUuid::from_parts(2, 3),
            listeners: vec![
                <Listener as TestInstance>::test_populated(version),
                <Listener as TestInstance>::test_multi_element_collections(version),
            ],
            features: vec![
                <Feature as TestInstance>::test_populated(version),
                <Feature as TestInstance>::test_multi_element_collections(version),
            ],
            rack: Some(KafkaString::from("test-2".to_owned())),
            is_migrating_zk_broker: false,
            log_dirs: if version >= 2 {
                vec![KafkaUuid::ONE, KafkaUuid::from_parts(2, 3)]
            } else {
                Vec::new()
            },
            previous_broker_epoch: if version >= 3 {
                9_876_543_211_i64
            } else {
                -1i64
            },
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries(version: i16) -> Self {
        Self {
            broker_id: i32::MIN,
            cluster_id: KafkaString::from("boundary".to_owned()),
            incarnation_id: KafkaUuid::ONE,
            listeners: vec![<Listener as TestInstance>::test_numeric_boundaries(version)],
            features: vec![<Feature as TestInstance>::test_numeric_boundaries(version)],
            rack: Some(KafkaString::from("boundary".to_owned())),
            is_migrating_zk_broker: if version >= 1 { true } else { false },
            log_dirs: if version >= 2 {
                vec![KafkaUuid::ONE]
            } else {
                Vec::new()
            },
            previous_broker_epoch: if version >= 3 { i64::MIN } else { -1i64 },
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields(version: i16) -> Self {
        Self {
            broker_id: 12345_i32,
            cluster_id: KafkaString::from("test".to_owned()),
            incarnation_id: KafkaUuid::ONE,
            listeners: vec![<Listener as TestInstance>::test_tagged_fields(version)],
            features: vec![<Feature as TestInstance>::test_tagged_fields(version)],
            rack: Some(KafkaString::from("test".to_owned())),
            is_migrating_zk_broker: if version >= 1 { true } else { false },
            log_dirs: if version >= 2 {
                vec![KafkaUuid::ONE]
            } else {
                Vec::new()
            },
            previous_broker_epoch: if version >= 3 {
                9_876_543_210_i64
            } else {
                -1i64
            },
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for Listener {
    fn test_populated(_version: i16) -> Self {
        Self {
            name: KafkaString::from("test".to_owned()),
            host: KafkaString::from("test".to_owned()),
            port: 42_u16,
            security_protocol: 42_i16,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals(_version: i16) -> Self {
        drop(Self::default());
        Self {
            name: KafkaString::default(),
            host: KafkaString::default(),
            port: 0_u16,
            security_protocol: 0_i16,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections(_version: i16) -> Self {
        Self {
            name: KafkaString::default(),
            host: KafkaString::default(),
            port: 0_u16,
            security_protocol: 0_i16,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections(_version: i16) -> Self {
        Self {
            name: KafkaString::from("test-2".to_owned()),
            host: KafkaString::from("test-2".to_owned()),
            port: 43_u16,
            security_protocol: 43_i16,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries(_version: i16) -> Self {
        Self {
            name: KafkaString::from("boundary".to_owned()),
            host: KafkaString::from("boundary".to_owned()),
            port: u16::MAX,
            security_protocol: i16::MIN,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields(_version: i16) -> Self {
        Self {
            name: KafkaString::from("test".to_owned()),
            host: KafkaString::from("test".to_owned()),
            port: 42_u16,
            security_protocol: 42_i16,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for Feature {
    fn test_populated(_version: i16) -> Self {
        Self {
            name: KafkaString::from("test".to_owned()),
            min_supported_version: 42_i16,
            max_supported_version: 42_i16,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals(_version: i16) -> Self {
        drop(Self::default());
        Self {
            name: KafkaString::default(),
            min_supported_version: 0_i16,
            max_supported_version: 0_i16,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections(_version: i16) -> Self {
        Self {
            name: KafkaString::default(),
            min_supported_version: 0_i16,
            max_supported_version: 0_i16,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections(_version: i16) -> Self {
        Self {
            name: KafkaString::from("test-2".to_owned()),
            min_supported_version: 43_i16,
            max_supported_version: 43_i16,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries(_version: i16) -> Self {
        Self {
            name: KafkaString::from("boundary".to_owned()),
            min_supported_version: i16::MIN,
            max_supported_version: i16::MIN,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields(_version: i16) -> Self {
        Self {
            name: KafkaString::from("test".to_owned()),
            min_supported_version: 42_i16,
            max_supported_version: 42_i16,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
fn encode_populated(version: i16) -> crate::MatrixResult<String> {
    let message = <BrokerRegistrationRequestData as TestInstance>::test_populated(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_populated(version: i16) -> crate::MatrixResult<usize> {
    let message = <BrokerRegistrationRequestData as TestInstance>::test_populated(version);
    Ok(message.encoded_len(version)?)
}
fn encode_null_optionals(version: i16) -> crate::MatrixResult<String> {
    let message = <BrokerRegistrationRequestData as TestInstance>::test_null_optionals(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_null_optionals(version: i16) -> crate::MatrixResult<usize> {
    let message = <BrokerRegistrationRequestData as TestInstance>::test_null_optionals(version);
    Ok(message.encoded_len(version)?)
}
fn encode_empty_collections(version: i16) -> crate::MatrixResult<String> {
    let message = <BrokerRegistrationRequestData as TestInstance>::test_empty_collections(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_empty_collections(version: i16) -> crate::MatrixResult<usize> {
    let message = <BrokerRegistrationRequestData as TestInstance>::test_empty_collections(version);
    Ok(message.encoded_len(version)?)
}
fn encode_multi_element_collections(version: i16) -> crate::MatrixResult<String> {
    let message =
        <BrokerRegistrationRequestData as TestInstance>::test_multi_element_collections(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_multi_element_collections(version: i16) -> crate::MatrixResult<usize> {
    let message =
        <BrokerRegistrationRequestData as TestInstance>::test_multi_element_collections(version);
    Ok(message.encoded_len(version)?)
}
fn encode_numeric_boundaries(version: i16) -> crate::MatrixResult<String> {
    let message = <BrokerRegistrationRequestData as TestInstance>::test_numeric_boundaries(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_numeric_boundaries(version: i16) -> crate::MatrixResult<usize> {
    let message = <BrokerRegistrationRequestData as TestInstance>::test_numeric_boundaries(version);
    Ok(message.encoded_len(version)?)
}
fn encode_tagged_fields(version: i16) -> crate::MatrixResult<String> {
    let message = <BrokerRegistrationRequestData as TestInstance>::test_tagged_fields(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_tagged_fields(version: i16) -> crate::MatrixResult<usize> {
    let message = <BrokerRegistrationRequestData as TestInstance>::test_tagged_fields(version);
    Ok(message.encoded_len(version)?)
}
fn reencode(version: i16, hex_input: &str) -> crate::MatrixResult<String> {
    let mut input = Bytes::from(crate::decode_hex(hex_input)?);
    let message = BrokerRegistrationRequestData::read(&mut input, version)?;
    crate::ensure_input_consumed(&input)?;
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
const MATRIX_CASES: &[crate::MatrixCase] = &[
    crate::MatrixCase {
        schema_name: "BrokerRegistrationRequest",
        java_class: "org.apache.kafka.common.message.BrokerRegistrationRequestData",
        version: 0i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "BrokerRegistrationRequest",
        java_class: "org.apache.kafka.common.message.BrokerRegistrationRequestData",
        version: 1i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "BrokerRegistrationRequest",
        java_class: "org.apache.kafka.common.message.BrokerRegistrationRequestData",
        version: 2i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "BrokerRegistrationRequest",
        java_class: "org.apache.kafka.common.message.BrokerRegistrationRequestData",
        version: 3i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "BrokerRegistrationRequest",
        java_class: "org.apache.kafka.common.message.BrokerRegistrationRequestData",
        version: 4i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "BrokerRegistrationRequest",
        java_class: "org.apache.kafka.common.message.BrokerRegistrationRequestData",
        version: 0i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "BrokerRegistrationRequest",
        java_class: "org.apache.kafka.common.message.BrokerRegistrationRequestData",
        version: 1i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "BrokerRegistrationRequest",
        java_class: "org.apache.kafka.common.message.BrokerRegistrationRequestData",
        version: 2i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "BrokerRegistrationRequest",
        java_class: "org.apache.kafka.common.message.BrokerRegistrationRequestData",
        version: 3i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "BrokerRegistrationRequest",
        java_class: "org.apache.kafka.common.message.BrokerRegistrationRequestData",
        version: 4i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "BrokerRegistrationRequest",
        java_class: "org.apache.kafka.common.message.BrokerRegistrationRequestData",
        version: 0i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "BrokerRegistrationRequest",
        java_class: "org.apache.kafka.common.message.BrokerRegistrationRequestData",
        version: 0i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "BrokerRegistrationRequest",
        java_class: "org.apache.kafka.common.message.BrokerRegistrationRequestData",
        version: 0i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "BrokerRegistrationRequest",
        java_class: "org.apache.kafka.common.message.BrokerRegistrationRequestData",
        version: 0i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "BrokerRegistrationRequest",
        java_class: "org.apache.kafka.common.message.BrokerRegistrationRequestData",
        version: 1i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "BrokerRegistrationRequest",
        java_class: "org.apache.kafka.common.message.BrokerRegistrationRequestData",
        version: 1i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "BrokerRegistrationRequest",
        java_class: "org.apache.kafka.common.message.BrokerRegistrationRequestData",
        version: 1i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "BrokerRegistrationRequest",
        java_class: "org.apache.kafka.common.message.BrokerRegistrationRequestData",
        version: 1i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "BrokerRegistrationRequest",
        java_class: "org.apache.kafka.common.message.BrokerRegistrationRequestData",
        version: 2i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "BrokerRegistrationRequest",
        java_class: "org.apache.kafka.common.message.BrokerRegistrationRequestData",
        version: 2i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "BrokerRegistrationRequest",
        java_class: "org.apache.kafka.common.message.BrokerRegistrationRequestData",
        version: 2i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "BrokerRegistrationRequest",
        java_class: "org.apache.kafka.common.message.BrokerRegistrationRequestData",
        version: 2i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "BrokerRegistrationRequest",
        java_class: "org.apache.kafka.common.message.BrokerRegistrationRequestData",
        version: 3i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "BrokerRegistrationRequest",
        java_class: "org.apache.kafka.common.message.BrokerRegistrationRequestData",
        version: 3i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "BrokerRegistrationRequest",
        java_class: "org.apache.kafka.common.message.BrokerRegistrationRequestData",
        version: 3i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "BrokerRegistrationRequest",
        java_class: "org.apache.kafka.common.message.BrokerRegistrationRequestData",
        version: 3i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "BrokerRegistrationRequest",
        java_class: "org.apache.kafka.common.message.BrokerRegistrationRequestData",
        version: 4i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "BrokerRegistrationRequest",
        java_class: "org.apache.kafka.common.message.BrokerRegistrationRequestData",
        version: 4i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "BrokerRegistrationRequest",
        java_class: "org.apache.kafka.common.message.BrokerRegistrationRequestData",
        version: 4i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "BrokerRegistrationRequest",
        java_class: "org.apache.kafka.common.message.BrokerRegistrationRequestData",
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
