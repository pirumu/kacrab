//! Generated from OffsetDeleteRequest.json - DO NOT EDIT
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
pub struct OffsetDeleteRequestData {
    /// The unique group identifier.
    pub group_id: KafkaString,
    /// The topics to delete offsets for.
    pub topics: Vec<OffsetDeleteRequestTopic>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for OffsetDeleteRequestData {
    fn default() -> Self {
        Self {
            group_id: KafkaString::default(),
            topics: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl OffsetDeleteRequestData {
    pub fn with_group_id(mut self, value: KafkaString) -> Self {
        self.group_id = value;
        self
    }
    pub fn with_topics(mut self, value: Vec<OffsetDeleteRequestTopic>) -> Self {
        self.topics = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 0 || version > 0 {
            return Err(UnsupportedVersion::new(47, version).into());
        }
        let group_id;
        let topics;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        group_id = read_string(buf)?;
        topics = {
            let len = read_array_length(buf)?;
            let mut arr = Vec::with_capacity(len.max(0) as usize);
            for _ in 0..len {
                arr.push(OffsetDeleteRequestTopic::read(buf, version)?);
            }
            arr
        };
        Ok(Self {
            group_id,
            topics,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 0 || version > 0 {
            return Err(UnsupportedVersion::new(47, version).into());
        }
        write_string(buf, &self.group_id)?;
        write_array_length(buf, self.topics.len() as i32);
        for el in &self.topics {
            el.write(buf, version)?;
        }
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        if version < 0 || version > 0 {
            return Err(UnsupportedVersion::new(47, version).into());
        }
        let mut len: usize = 0;
        len += string_len(&self.group_id)?;
        len += array_length_len();
        for el in &self.topics {
            len += el.encoded_len(version)?;
        }
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct OffsetDeleteRequestTopic {
    /// The topic name.
    pub name: KafkaString,
    /// Each partition to delete offsets for.
    pub partitions: Vec<OffsetDeleteRequestPartition>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for OffsetDeleteRequestTopic {
    fn default() -> Self {
        Self {
            name: KafkaString::default(),
            partitions: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl OffsetDeleteRequestTopic {
    pub fn with_name(mut self, value: KafkaString) -> Self {
        self.name = value;
        self
    }
    pub fn with_partitions(mut self, value: Vec<OffsetDeleteRequestPartition>) -> Self {
        self.partitions = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let name;
        let partitions;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        name = read_string(buf)?;
        partitions = {
            let len = read_array_length(buf)?;
            let mut arr = Vec::with_capacity(len.max(0) as usize);
            for _ in 0..len {
                arr.push(OffsetDeleteRequestPartition::read(buf, version)?);
            }
            arr
        };
        Ok(Self {
            name,
            partitions,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        write_string(buf, &self.name)?;
        write_array_length(buf, self.partitions.len() as i32);
        for el in &self.partitions {
            el.write(buf, version)?;
        }
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        let mut len: usize = 0;
        len += string_len(&self.name)?;
        len += array_length_len();
        for el in &self.partitions {
            len += el.encoded_len(version)?;
        }
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct OffsetDeleteRequestPartition {
    /// The partition index.
    pub partition_index: i32,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for OffsetDeleteRequestPartition {
    fn default() -> Self {
        Self {
            partition_index: 0_i32,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl OffsetDeleteRequestPartition {
    pub fn with_partition_index(mut self, value: i32) -> Self {
        self.partition_index = value;
        self
    }
    pub fn read(buf: &mut Bytes, _version: i16) -> Result<Self> {
        let partition_index;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        partition_index = read_i32(buf)?;
        Ok(Self {
            partition_index,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, _version: i16) -> Result<()> {
        write_i32(buf, self.partition_index);
        Ok(())
    }
    pub fn encoded_len(&self, _version: i16) -> Result<usize> {
        let mut len: usize = 0;
        len += 4;
        Ok(len)
    }
}
