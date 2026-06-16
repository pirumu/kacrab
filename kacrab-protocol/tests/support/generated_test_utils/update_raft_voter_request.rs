use bytes::{Bytes, BytesMut};
use kacrab_protocol::{generated::update_raft_voter_request::*, *};

use crate::TestInstance;

impl TestInstance for UpdateRaftVoterRequestData {
    fn test_populated() -> Self {
        Self {
            cluster_id: Some(KafkaString::from("test".to_owned())),
            current_leader_epoch: 12345_i32,
            voter_id: 12345_i32,
            voter_directory_id: KafkaUuid::ONE,
            listeners: vec![<Listener as TestInstance>::test_populated()],
            k_raft_version_feature: <KRaftVersionFeature as TestInstance>::test_populated(),
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals() -> Self {
        Self {
            cluster_id: None,
            current_leader_epoch: 0_i32,
            voter_id: 0_i32,
            voter_directory_id: KafkaUuid::ZERO,
            listeners: vec![<Listener as TestInstance>::test_null_optionals()],
            k_raft_version_feature: <KRaftVersionFeature as TestInstance>::test_null_optionals(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            cluster_id: Some(KafkaString::default()),
            current_leader_epoch: 0_i32,
            voter_id: 0_i32,
            voter_directory_id: KafkaUuid::ZERO,
            listeners: Vec::new(),
            k_raft_version_feature: <KRaftVersionFeature as TestInstance>::test_null_optionals(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            cluster_id: Some(KafkaString::from("test-2".to_owned())),
            current_leader_epoch: 23456_i32,
            voter_id: 23456_i32,
            voter_directory_id: KafkaUuid::from_parts(2, 3),
            listeners: vec![
                <Listener as TestInstance>::test_populated(),
                <Listener as TestInstance>::test_multi_element_collections(),
            ],
            k_raft_version_feature:
                <KRaftVersionFeature as TestInstance>::test_multi_element_collections(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            cluster_id: Some(KafkaString::from("boundary".to_owned())),
            current_leader_epoch: i32::MIN,
            voter_id: i32::MIN,
            voter_directory_id: KafkaUuid::ONE,
            listeners: vec![<Listener as TestInstance>::test_numeric_boundaries()],
            k_raft_version_feature: <KRaftVersionFeature as TestInstance>::test_numeric_boundaries(
            ),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            cluster_id: Some(KafkaString::from("test".to_owned())),
            current_leader_epoch: 12345_i32,
            voter_id: 12345_i32,
            voter_directory_id: KafkaUuid::ONE,
            listeners: vec![<Listener as TestInstance>::test_tagged_fields()],
            k_raft_version_feature: <KRaftVersionFeature as TestInstance>::test_tagged_fields(),
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for Listener {
    fn test_populated() -> Self {
        Self {
            name: KafkaString::from("test".to_owned()),
            host: KafkaString::from("test".to_owned()),
            port: 42_u16,
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
            host: KafkaString::default(),
            port: 0_u16,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            name: KafkaString::default(),
            host: KafkaString::default(),
            port: 0_u16,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            name: KafkaString::from("test-2".to_owned()),
            host: KafkaString::from("test-2".to_owned()),
            port: 43_u16,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            name: KafkaString::from("boundary".to_owned()),
            host: KafkaString::from("boundary".to_owned()),
            port: u16::MAX,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            name: KafkaString::from("test".to_owned()),
            host: KafkaString::from("test".to_owned()),
            port: 42_u16,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for KRaftVersionFeature {
    fn test_populated() -> Self {
        Self {
            min_supported_version: 42_i16,
            max_supported_version: 42_i16,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals() -> Self {
        drop(Self::default());
        Self {
            min_supported_version: 0_i16,
            max_supported_version: 0_i16,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            min_supported_version: 0_i16,
            max_supported_version: 0_i16,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            min_supported_version: 43_i16,
            max_supported_version: 43_i16,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            min_supported_version: i16::MIN,
            max_supported_version: i16::MIN,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
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
    let message = <UpdateRaftVoterRequestData as TestInstance>::test_populated();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_populated(version: i16) -> crate::MatrixResult<usize> {
    let message = <UpdateRaftVoterRequestData as TestInstance>::test_populated();
    Ok(message.encoded_len(version)?)
}
fn encode_null_optionals(version: i16) -> crate::MatrixResult<String> {
    let message = <UpdateRaftVoterRequestData as TestInstance>::test_null_optionals();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_null_optionals(version: i16) -> crate::MatrixResult<usize> {
    let message = <UpdateRaftVoterRequestData as TestInstance>::test_null_optionals();
    Ok(message.encoded_len(version)?)
}
fn encode_empty_collections(version: i16) -> crate::MatrixResult<String> {
    let message = <UpdateRaftVoterRequestData as TestInstance>::test_empty_collections();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_empty_collections(version: i16) -> crate::MatrixResult<usize> {
    let message = <UpdateRaftVoterRequestData as TestInstance>::test_empty_collections();
    Ok(message.encoded_len(version)?)
}
fn encode_multi_element_collections(version: i16) -> crate::MatrixResult<String> {
    let message = <UpdateRaftVoterRequestData as TestInstance>::test_multi_element_collections();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_multi_element_collections(version: i16) -> crate::MatrixResult<usize> {
    let message = <UpdateRaftVoterRequestData as TestInstance>::test_multi_element_collections();
    Ok(message.encoded_len(version)?)
}
fn encode_numeric_boundaries(version: i16) -> crate::MatrixResult<String> {
    let message = <UpdateRaftVoterRequestData as TestInstance>::test_numeric_boundaries();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_numeric_boundaries(version: i16) -> crate::MatrixResult<usize> {
    let message = <UpdateRaftVoterRequestData as TestInstance>::test_numeric_boundaries();
    Ok(message.encoded_len(version)?)
}
fn encode_tagged_fields(version: i16) -> crate::MatrixResult<String> {
    let message = <UpdateRaftVoterRequestData as TestInstance>::test_tagged_fields();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_tagged_fields(version: i16) -> crate::MatrixResult<usize> {
    let message = <UpdateRaftVoterRequestData as TestInstance>::test_tagged_fields();
    Ok(message.encoded_len(version)?)
}
fn reencode(version: i16, hex_input: &str) -> crate::MatrixResult<String> {
    let mut input = Bytes::from(crate::decode_hex(hex_input)?);
    let message = UpdateRaftVoterRequestData::read(&mut input, version)?;
    crate::ensure_input_consumed(&input)?;
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
const MATRIX_CASES: &[crate::MatrixCase] = &[
    crate::MatrixCase {
        schema_name: "UpdateRaftVoterRequest",
        java_class: "org.apache.kafka.common.message.UpdateRaftVoterRequestData",
        version: 0i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "UpdateRaftVoterRequest",
        java_class: "org.apache.kafka.common.message.UpdateRaftVoterRequestData",
        version: 0i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "UpdateRaftVoterRequest",
        java_class: "org.apache.kafka.common.message.UpdateRaftVoterRequestData",
        version: 0i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "UpdateRaftVoterRequest",
        java_class: "org.apache.kafka.common.message.UpdateRaftVoterRequestData",
        version: 0i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "UpdateRaftVoterRequest",
        java_class: "org.apache.kafka.common.message.UpdateRaftVoterRequestData",
        version: 0i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "UpdateRaftVoterRequest",
        java_class: "org.apache.kafka.common.message.UpdateRaftVoterRequestData",
        version: 0i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
];
pub(crate) fn append_protocol_cases(cases: &mut Vec<crate::MatrixCase>) {
    cases.extend_from_slice(MATRIX_CASES);
}
