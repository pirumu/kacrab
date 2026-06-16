//! Generated from AddPartitionsToTxnResponse.json - DO NOT EDIT
#![allow(
    missing_docs,
    clippy::all,
    clippy::pedantic,
    clippy::nursery,
    reason = "Generated protocol modules mirror Kafka's schema shape and intentionally trade \
              hand-written lint style for reproducible wire-code output."
)]
use bytes::{Bytes, BytesMut};

use crate::*;

#[derive(Debug, Clone, PartialEq)]
pub struct AddPartitionsToTxnResponseData {
    /// Duration in milliseconds for which the request was throttled due to a quota violation, or
    /// zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// The response top level error code.
    pub error_code: i16,
    /// Results categorized by transactional ID.
    pub results_by_transaction: Vec<AddPartitionsToTxnResult>,
    /// The results for each topic.
    pub results_by_topic_v3_and_below: Vec<AddPartitionsToTxnTopicResult>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for AddPartitionsToTxnResponseData {
    fn default() -> Self {
        Self {
            throttle_time_ms: 0_i32,
            error_code: 0_i16,
            results_by_transaction: Vec::new(),
            results_by_topic_v3_and_below: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl AddPartitionsToTxnResponseData {
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 0 || version > 5 {
            return Err(UnsupportedVersion::new(24, version).into());
        }
        let throttle_time_ms;
        let mut error_code = 0_i16;
        let mut results_by_transaction = Vec::new();
        let mut results_by_topic_v3_and_below = Vec::new();
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        throttle_time_ms = read_i32(buf)?;
        if version >= 4 {
            error_code = read_i16(buf)?;
        }
        if version >= 4 {
            results_by_transaction = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(AddPartitionsToTxnResult::read(buf, version)?);
                }
                arr
            };
        }
        if version <= 3 {
            if version >= 3 {
                results_by_topic_v3_and_below = {
                    let len = read_compact_array_length(buf)?;
                    let mut arr = Vec::with_capacity(len.max(0) as usize);
                    for _ in 0..len {
                        arr.push(AddPartitionsToTxnTopicResult::read(buf, version)?);
                    }
                    arr
                };
            } else {
                results_by_topic_v3_and_below = {
                    let len = read_array_length(buf)?;
                    let mut arr = Vec::with_capacity(len.max(0) as usize);
                    for _ in 0..len {
                        arr.push(AddPartitionsToTxnTopicResult::read(buf, version)?);
                    }
                    arr
                };
            }
        }
        if version >= 3 {
            let tagged_fields = read_tagged_fields(buf)?;
            for field in &tagged_fields {
                match field.tag {
                    _ => {
                        _unknown_tagged_fields.push(field.clone());
                    },
                }
            }
        }
        Ok(Self {
            throttle_time_ms,
            error_code,
            results_by_transaction,
            results_by_topic_v3_and_below,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 0 || version > 5 {
            return Err(UnsupportedVersion::new(24, version).into());
        }
        write_i32(buf, self.throttle_time_ms);
        if version >= 4 {
            write_i16(buf, self.error_code);
        }
        if version >= 4 {
            write_compact_array_length(buf, self.results_by_transaction.len() as i32);
            for el in &self.results_by_transaction {
                el.write(buf, version)?;
            }
        }
        if version <= 3 {
            if version >= 3 {
                write_compact_array_length(buf, self.results_by_topic_v3_and_below.len() as i32);
                for el in &self.results_by_topic_v3_and_below {
                    el.write(buf, version)?;
                }
            } else {
                write_array_length(buf, self.results_by_topic_v3_and_below.len() as i32);
                for el in &self.results_by_topic_v3_and_below {
                    el.write(buf, version)?;
                }
            }
        }
        if version >= 3 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct AddPartitionsToTxnResult {
    /// The transactional id corresponding to the transaction.
    pub transactional_id: KafkaString,
    /// The results for each topic.
    pub topic_results: Vec<AddPartitionsToTxnTopicResult>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for AddPartitionsToTxnResult {
    fn default() -> Self {
        Self {
            transactional_id: KafkaString::default(),
            topic_results: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl AddPartitionsToTxnResult {
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let transactional_id;
        let topic_results;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        transactional_id = read_compact_string(buf)?;
        topic_results = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(len.max(0) as usize);
            for _ in 0..len {
                arr.push(AddPartitionsToTxnTopicResult::read(buf, version)?);
            }
            arr
        };
        let tagged_fields = read_tagged_fields(buf)?;
        for field in &tagged_fields {
            match field.tag {
                _ => {
                    _unknown_tagged_fields.push(field.clone());
                },
            }
        }
        Ok(Self {
            transactional_id,
            topic_results,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        write_compact_string(buf, &self.transactional_id)?;
        write_compact_array_length(buf, self.topic_results.len() as i32);
        for el in &self.topic_results {
            el.write(buf, version)?;
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct AddPartitionsToTxnTopicResult {
    /// The topic name.
    pub name: KafkaString,
    /// The results for each partition.
    pub results_by_partition: Vec<AddPartitionsToTxnPartitionResult>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for AddPartitionsToTxnTopicResult {
    fn default() -> Self {
        Self {
            name: KafkaString::default(),
            results_by_partition: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl AddPartitionsToTxnTopicResult {
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let name;
        let results_by_partition;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 3 {
            name = read_compact_string(buf)?;
        } else {
            name = read_string(buf)?;
        }
        if version >= 3 {
            results_by_partition = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(AddPartitionsToTxnPartitionResult::read(buf, version)?);
                }
                arr
            };
        } else {
            results_by_partition = {
                let len = read_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(AddPartitionsToTxnPartitionResult::read(buf, version)?);
                }
                arr
            };
        }
        if version >= 3 {
            let tagged_fields = read_tagged_fields(buf)?;
            for field in &tagged_fields {
                match field.tag {
                    _ => {
                        _unknown_tagged_fields.push(field.clone());
                    },
                }
            }
        }
        Ok(Self {
            name,
            results_by_partition,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version >= 3 {
            write_compact_string(buf, &self.name)?;
        } else {
            write_string(buf, &self.name)?;
        }
        if version >= 3 {
            write_compact_array_length(buf, self.results_by_partition.len() as i32);
            for el in &self.results_by_partition {
                el.write(buf, version)?;
            }
        } else {
            write_array_length(buf, self.results_by_partition.len() as i32);
            for el in &self.results_by_partition {
                el.write(buf, version)?;
            }
        }
        if version >= 3 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct AddPartitionsToTxnPartitionResult {
    /// The partition indexes.
    pub partition_index: i32,
    /// The response error code.
    pub partition_error_code: i16,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for AddPartitionsToTxnPartitionResult {
    fn default() -> Self {
        Self {
            partition_index: 0_i32,
            partition_error_code: 0_i16,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl AddPartitionsToTxnPartitionResult {
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let partition_index;
        let partition_error_code;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        partition_index = read_i32(buf)?;
        partition_error_code = read_i16(buf)?;
        if version >= 3 {
            let tagged_fields = read_tagged_fields(buf)?;
            for field in &tagged_fields {
                match field.tag {
                    _ => {
                        _unknown_tagged_fields.push(field.clone());
                    },
                }
            }
        }
        Ok(Self {
            partition_index,
            partition_error_code,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        write_i32(buf, self.partition_index);
        write_i16(buf, self.partition_error_code);
        if version >= 3 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
}
