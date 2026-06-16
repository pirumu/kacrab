use bytes::{Bytes, BytesMut};
use kacrab_protocol::{generated::alter_partition_request::*, *};

use crate::TestInstance;

impl TestInstance for AlterPartitionRequestData {
    fn test_populated() -> Self {
        Self {
            broker_id: 12345_i32,
            broker_epoch: 9_876_543_210_i64,
            topics: vec![<TopicData as TestInstance>::test_populated()],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals() -> Self {
        Self {
            broker_id: 0_i32,
            broker_epoch: 0_i64,
            topics: vec![<TopicData as TestInstance>::test_null_optionals()],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            broker_id: 0_i32,
            broker_epoch: 0_i64,
            topics: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            broker_id: 23456_i32,
            broker_epoch: 9_876_543_211_i64,
            topics: vec![
                <TopicData as TestInstance>::test_populated(),
                <TopicData as TestInstance>::test_multi_element_collections(),
            ],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            broker_id: i32::MIN,
            broker_epoch: i64::MIN,
            topics: vec![<TopicData as TestInstance>::test_numeric_boundaries()],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            broker_id: 12345_i32,
            broker_epoch: 9_876_543_210_i64,
            topics: vec![<TopicData as TestInstance>::test_tagged_fields()],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for TopicData {
    fn test_populated() -> Self {
        Self {
            topic_id: KafkaUuid::ONE,
            partitions: vec![<PartitionData as TestInstance>::test_populated()],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals() -> Self {
        drop(Self::default());
        Self {
            topic_id: KafkaUuid::ZERO,
            partitions: vec![<PartitionData as TestInstance>::test_null_optionals()],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            topic_id: KafkaUuid::ZERO,
            partitions: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            topic_id: KafkaUuid::from_parts(2, 3),
            partitions: vec![
                <PartitionData as TestInstance>::test_populated(),
                <PartitionData as TestInstance>::test_multi_element_collections(),
            ],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            topic_id: KafkaUuid::ONE,
            partitions: vec![<PartitionData as TestInstance>::test_numeric_boundaries()],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            topic_id: KafkaUuid::ONE,
            partitions: vec![<PartitionData as TestInstance>::test_tagged_fields()],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for PartitionData {
    fn test_populated() -> Self {
        Self {
            partition_index: 12345_i32,
            leader_epoch: 12345_i32,
            new_isr: vec![12345_i32],
            new_isr_with_epochs: vec![<BrokerState as TestInstance>::test_populated()],
            leader_recovery_state: 7_i8,
            partition_epoch: 12345_i32,
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
            leader_epoch: 0_i32,
            new_isr: vec![0_i32],
            new_isr_with_epochs: vec![<BrokerState as TestInstance>::test_null_optionals()],
            leader_recovery_state: 0_i8,
            partition_epoch: 0_i32,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            partition_index: 0_i32,
            leader_epoch: 0_i32,
            new_isr: Vec::new(),
            new_isr_with_epochs: Vec::new(),
            leader_recovery_state: 0_i8,
            partition_epoch: 0_i32,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            partition_index: 23456_i32,
            leader_epoch: 23456_i32,
            new_isr: vec![12345_i32, 23456_i32],
            new_isr_with_epochs: vec![
                <BrokerState as TestInstance>::test_populated(),
                <BrokerState as TestInstance>::test_multi_element_collections(),
            ],
            leader_recovery_state: 8_i8,
            partition_epoch: 23456_i32,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            partition_index: i32::MIN,
            leader_epoch: i32::MIN,
            new_isr: vec![i32::MIN],
            new_isr_with_epochs: vec![<BrokerState as TestInstance>::test_numeric_boundaries()],
            leader_recovery_state: i8::MIN,
            partition_epoch: i32::MIN,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            partition_index: 12345_i32,
            leader_epoch: 12345_i32,
            new_isr: vec![12345_i32],
            new_isr_with_epochs: vec![<BrokerState as TestInstance>::test_tagged_fields()],
            leader_recovery_state: 7_i8,
            partition_epoch: 12345_i32,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for BrokerState {
    fn test_populated() -> Self {
        Self {
            broker_id: 12345_i32,
            broker_epoch: 9_876_543_210_i64,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals() -> Self {
        drop(Self::default());
        Self {
            broker_id: 0_i32,
            broker_epoch: 0_i64,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections() -> Self {
        Self {
            broker_id: 0_i32,
            broker_epoch: 0_i64,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections() -> Self {
        Self {
            broker_id: 23456_i32,
            broker_epoch: 9_876_543_211_i64,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries() -> Self {
        Self {
            broker_id: i32::MIN,
            broker_epoch: i64::MIN,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields() -> Self {
        Self {
            broker_id: 12345_i32,
            broker_epoch: 9_876_543_210_i64,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
fn encode_populated(version: i16) -> crate::MatrixResult<String> {
    let message = <AlterPartitionRequestData as TestInstance>::test_populated();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_null_optionals(version: i16) -> crate::MatrixResult<String> {
    let message = <AlterPartitionRequestData as TestInstance>::test_null_optionals();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_empty_collections(version: i16) -> crate::MatrixResult<String> {
    let message = <AlterPartitionRequestData as TestInstance>::test_empty_collections();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_multi_element_collections(version: i16) -> crate::MatrixResult<String> {
    let message = <AlterPartitionRequestData as TestInstance>::test_multi_element_collections();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_numeric_boundaries(version: i16) -> crate::MatrixResult<String> {
    let message = <AlterPartitionRequestData as TestInstance>::test_numeric_boundaries();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encode_tagged_fields(version: i16) -> crate::MatrixResult<String> {
    let message = <AlterPartitionRequestData as TestInstance>::test_tagged_fields();
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn reencode(version: i16, hex_input: &str) -> crate::MatrixResult<String> {
    let mut input = Bytes::from(crate::decode_hex(hex_input)?);
    let message = AlterPartitionRequestData::read(&mut input, version)?;
    crate::ensure_input_consumed(&input)?;
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
const MATRIX_CASES: &[crate::MatrixCase] = &[
    crate::MatrixCase {
        schema_name: "AlterPartitionRequest",
        java_class: "org.apache.kafka.common.message.AlterPartitionRequestData",
        version: 2i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AlterPartitionRequest",
        java_class: "org.apache.kafka.common.message.AlterPartitionRequestData",
        version: 3i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AlterPartitionRequest",
        java_class: "org.apache.kafka.common.message.AlterPartitionRequestData",
        version: 2i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AlterPartitionRequest",
        java_class: "org.apache.kafka.common.message.AlterPartitionRequestData",
        version: 3i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AlterPartitionRequest",
        java_class: "org.apache.kafka.common.message.AlterPartitionRequestData",
        version: 2i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AlterPartitionRequest",
        java_class: "org.apache.kafka.common.message.AlterPartitionRequestData",
        version: 2i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AlterPartitionRequest",
        java_class: "org.apache.kafka.common.message.AlterPartitionRequestData",
        version: 2i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AlterPartitionRequest",
        java_class: "org.apache.kafka.common.message.AlterPartitionRequestData",
        version: 2i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AlterPartitionRequest",
        java_class: "org.apache.kafka.common.message.AlterPartitionRequestData",
        version: 3i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AlterPartitionRequest",
        java_class: "org.apache.kafka.common.message.AlterPartitionRequestData",
        version: 3i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AlterPartitionRequest",
        java_class: "org.apache.kafka.common.message.AlterPartitionRequestData",
        version: 3i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "AlterPartitionRequest",
        java_class: "org.apache.kafka.common.message.AlterPartitionRequestData",
        version: 3i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_reencode: reencode,
    },
];
pub(crate) fn append_protocol_cases(cases: &mut Vec<crate::MatrixCase>) {
    cases.extend_from_slice(MATRIX_CASES);
}
