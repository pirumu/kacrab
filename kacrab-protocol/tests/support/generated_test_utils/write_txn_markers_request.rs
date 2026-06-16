use bytes::{Bytes, BytesMut};
use kacrab_protocol::{generated::write_txn_markers_request::*, *};

use crate::TestInstance;

impl TestInstance for WriteTxnMarkersRequestData {
    fn test_populated() -> Self {
        Self {
            markers: vec![<WritableTxnMarker as TestInstance>::test_populated()],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals() -> Self {
        Self {
            markers: vec![<WritableTxnMarker as TestInstance>::test_null_optionals()],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            markers: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            markers: vec![
                <WritableTxnMarker as TestInstance>::test_populated(),
                <WritableTxnMarker as TestInstance>::test_multi_element_collections(),
            ],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            markers: vec![<WritableTxnMarker as TestInstance>::test_numeric_boundaries()],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            markers: vec![<WritableTxnMarker as TestInstance>::test_tagged_fields()],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for WritableTxnMarker {
    fn test_populated() -> Self {
        Self {
            producer_id: 9_876_543_210_i64,
            producer_epoch: 42_i16,
            transaction_result: true,
            topics: vec![<WritableTxnMarkerTopic as TestInstance>::test_populated()],
            coordinator_epoch: 12345_i32,
            transaction_version: 7_i8,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals() -> Self {
        drop(Self::default());
        Self {
            producer_id: 0_i64,
            producer_epoch: 0_i16,
            transaction_result: false,
            topics: vec![<WritableTxnMarkerTopic as TestInstance>::test_null_optionals()],
            coordinator_epoch: 0_i32,
            transaction_version: 0_i8,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            producer_id: 0_i64,
            producer_epoch: 0_i16,
            transaction_result: false,
            topics: Vec::new(),
            coordinator_epoch: 0_i32,
            transaction_version: 0_i8,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            producer_id: 9_876_543_211_i64,
            producer_epoch: 43_i16,
            transaction_result: false,
            topics: vec![
                <WritableTxnMarkerTopic as TestInstance>::test_populated(),
                <WritableTxnMarkerTopic as TestInstance>::test_multi_element_collections(),
            ],
            coordinator_epoch: 23456_i32,
            transaction_version: 8_i8,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            producer_id: i64::MIN,
            producer_epoch: i16::MIN,
            transaction_result: true,
            topics: vec![<WritableTxnMarkerTopic as TestInstance>::test_numeric_boundaries()],
            coordinator_epoch: i32::MIN,
            transaction_version: i8::MIN,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            producer_id: 9_876_543_210_i64,
            producer_epoch: 42_i16,
            transaction_result: true,
            topics: vec![<WritableTxnMarkerTopic as TestInstance>::test_tagged_fields()],
            coordinator_epoch: 12345_i32,
            transaction_version: 7_i8,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for WritableTxnMarkerTopic {
    fn test_populated() -> Self {
        Self {
            name: KafkaString::from("test".to_owned()),
            partition_indexes: vec![12345_i32],
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
            partition_indexes: vec![0_i32],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            name: KafkaString::default(),
            partition_indexes: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            name: KafkaString::from("test-2".to_owned()),
            partition_indexes: vec![12345_i32, 23456_i32],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            name: KafkaString::from("boundary".to_owned()),
            partition_indexes: vec![i32::MIN],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            name: KafkaString::from("test".to_owned()),
            partition_indexes: vec![12345_i32],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
fn encode_populated(version: i16) -> crate::MatrixResult<String> {
    let message = <WriteTxnMarkersRequestData as TestInstance>::test_populated();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_null_optionals(version: i16) -> crate::MatrixResult<String> {
    let message = <WriteTxnMarkersRequestData as TestInstance>::test_null_optionals();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_empty_collections(version: i16) -> crate::MatrixResult<String> {
    let message = <WriteTxnMarkersRequestData as TestInstance>::test_empty_collections();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_multi_element_collections(version: i16) -> crate::MatrixResult<String> {
    let message = <WriteTxnMarkersRequestData as TestInstance>::test_multi_element_collections();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_numeric_boundaries(version: i16) -> crate::MatrixResult<String> {
    let message = <WriteTxnMarkersRequestData as TestInstance>::test_numeric_boundaries();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_tagged_fields(version: i16) -> crate::MatrixResult<String> {
    let message = <WriteTxnMarkersRequestData as TestInstance>::test_tagged_fields();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn reencode(version: i16, hex_input: &str) -> crate::MatrixResult<String> {
    let mut input = Bytes::from(crate::decode_hex(hex_input)?);
    let message = WriteTxnMarkersRequestData::read(&mut input, version)?;
    crate::ensure_input_consumed(&input)?;
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
const MATRIX_CASES: &[crate::MatrixCase] = &[
    crate::MatrixCase {
        schema_name: "WriteTxnMarkersRequest",
        java_class: "org.apache.kafka.common.message.WriteTxnMarkersRequestData",
        version: 1i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "WriteTxnMarkersRequest",
        java_class: "org.apache.kafka.common.message.WriteTxnMarkersRequestData",
        version: 2i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "WriteTxnMarkersRequest",
        java_class: "org.apache.kafka.common.message.WriteTxnMarkersRequestData",
        version: 1i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "WriteTxnMarkersRequest",
        java_class: "org.apache.kafka.common.message.WriteTxnMarkersRequestData",
        version: 2i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "WriteTxnMarkersRequest",
        java_class: "org.apache.kafka.common.message.WriteTxnMarkersRequestData",
        version: 1i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "WriteTxnMarkersRequest",
        java_class: "org.apache.kafka.common.message.WriteTxnMarkersRequestData",
        version: 1i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "WriteTxnMarkersRequest",
        java_class: "org.apache.kafka.common.message.WriteTxnMarkersRequestData",
        version: 1i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "WriteTxnMarkersRequest",
        java_class: "org.apache.kafka.common.message.WriteTxnMarkersRequestData",
        version: 1i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "WriteTxnMarkersRequest",
        java_class: "org.apache.kafka.common.message.WriteTxnMarkersRequestData",
        version: 2i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "WriteTxnMarkersRequest",
        java_class: "org.apache.kafka.common.message.WriteTxnMarkersRequestData",
        version: 2i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "WriteTxnMarkersRequest",
        java_class: "org.apache.kafka.common.message.WriteTxnMarkersRequestData",
        version: 2i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "WriteTxnMarkersRequest",
        java_class: "org.apache.kafka.common.message.WriteTxnMarkersRequestData",
        version: 2i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
];
pub(crate) fn append_protocol_cases(cases: &mut Vec<crate::MatrixCase>) {
    cases.extend_from_slice(MATRIX_CASES);
}
