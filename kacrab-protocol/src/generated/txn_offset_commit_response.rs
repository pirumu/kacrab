//! Generated from TxnOffsetCommitResponse.json - DO NOT EDIT
#![allow(
    missing_docs,
    clippy::all,
    clippy::pedantic,
    clippy::nursery,
    clippy::arithmetic_side_effects,
    reason = "Generated protocol modules mirror Kafka's schema shape and intentionally trade \
              hand-written lint style for reproducible wire-code output."
)]
use bytes::{Bytes, BytesMut};

use crate::*;

#[derive(Debug, Clone, PartialEq)]
pub struct TxnOffsetCommitResponseData {
    /// The duration in milliseconds for which the request was throttled due to a quota violation,
    /// or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// The responses for each topic.
    pub topics: Vec<TxnOffsetCommitResponseTopic>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for TxnOffsetCommitResponseData {
    fn default() -> Self {
        Self {
            throttle_time_ms: 0_i32,
            topics: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl TxnOffsetCommitResponseData {
    pub fn with_throttle_time_ms(mut self, value: i32) -> Self {
        self.throttle_time_ms = value;
        self
    }
    pub fn with_topics(mut self, value: Vec<TxnOffsetCommitResponseTopic>) -> Self {
        self.topics = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 0 || version > 5 {
            return Err(UnsupportedVersion::new(28, version).into());
        }
        let throttle_time_ms;
        let topics;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        throttle_time_ms = read_i32(buf)?;
        if version >= 3 {
            topics = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(TxnOffsetCommitResponseTopic::read(buf, version)?);
                }
                arr
            };
        } else {
            topics = {
                let len = read_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(TxnOffsetCommitResponseTopic::read(buf, version)?);
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
            throttle_time_ms,
            topics,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 0 || version > 5 {
            return Err(UnsupportedVersion::new(28, version).into());
        }
        write_i32(buf, self.throttle_time_ms);
        if version >= 3 {
            write_compact_array_length(buf, self.topics.len() as i32);
            for el in &self.topics {
                el.write(buf, version)?;
            }
        } else {
            write_array_length(buf, self.topics.len() as i32);
            for el in &self.topics {
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
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        if version < 0 || version > 5 {
            return Err(UnsupportedVersion::new(28, version).into());
        }
        let mut len: usize = 0;
        len += 4;
        if version >= 3 {
            len += compact_array_length_len(self.topics.len() as i32);
            for el in &self.topics {
                len += el.encoded_len(version)?;
            }
        } else {
            len += array_length_len();
            for el in &self.topics {
                len += el.encoded_len(version)?;
            }
        }
        if version >= 3 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            len += tagged_fields_len(&all_tags)?;
        }
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct TxnOffsetCommitResponseTopic {
    /// The topic name.
    pub name: KafkaString,
    /// The responses for each partition in the topic.
    pub partitions: Vec<TxnOffsetCommitResponsePartition>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for TxnOffsetCommitResponseTopic {
    fn default() -> Self {
        Self {
            name: KafkaString::default(),
            partitions: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl TxnOffsetCommitResponseTopic {
    pub fn with_name(mut self, value: KafkaString) -> Self {
        self.name = value;
        self
    }
    pub fn with_partitions(mut self, value: Vec<TxnOffsetCommitResponsePartition>) -> Self {
        self.partitions = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let name;
        let partitions;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 3 {
            name = read_compact_string(buf)?;
        } else {
            name = read_string(buf)?;
        }
        if version >= 3 {
            partitions = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(TxnOffsetCommitResponsePartition::read(buf, version)?);
                }
                arr
            };
        } else {
            partitions = {
                let len = read_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(TxnOffsetCommitResponsePartition::read(buf, version)?);
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
            partitions,
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
            write_compact_array_length(buf, self.partitions.len() as i32);
            for el in &self.partitions {
                el.write(buf, version)?;
            }
        } else {
            write_array_length(buf, self.partitions.len() as i32);
            for el in &self.partitions {
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
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        let mut len: usize = 0;
        if version >= 3 {
            len += compact_string_len(&self.name)?;
        } else {
            len += string_len(&self.name)?;
        }
        if version >= 3 {
            len += compact_array_length_len(self.partitions.len() as i32);
            for el in &self.partitions {
                len += el.encoded_len(version)?;
            }
        } else {
            len += array_length_len();
            for el in &self.partitions {
                len += el.encoded_len(version)?;
            }
        }
        if version >= 3 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            len += tagged_fields_len(&all_tags)?;
        }
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct TxnOffsetCommitResponsePartition {
    /// The partition index.
    pub partition_index: i32,
    /// The error code, or 0 if there was no error.
    pub error_code: i16,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for TxnOffsetCommitResponsePartition {
    fn default() -> Self {
        Self {
            partition_index: 0_i32,
            error_code: 0_i16,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl TxnOffsetCommitResponsePartition {
    pub fn with_partition_index(mut self, value: i32) -> Self {
        self.partition_index = value;
        self
    }
    pub fn with_error_code(mut self, value: i16) -> Self {
        self.error_code = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let partition_index;
        let error_code;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        partition_index = read_i32(buf)?;
        error_code = read_i16(buf)?;
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
            error_code,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        write_i32(buf, self.partition_index);
        write_i16(buf, self.error_code);
        if version >= 3 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        let mut len: usize = 0;
        len += 4;
        len += 2;
        if version >= 3 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            len += tagged_fields_len(&all_tags)?;
        }
        Ok(len)
    }
}
