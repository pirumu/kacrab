//! Generated from DescribeQuorumRequest.json - DO NOT EDIT
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
pub struct DescribeQuorumRequestData {
    /// The topics to describe.
    pub topics: Vec<TopicData>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for DescribeQuorumRequestData {
    fn default() -> Self {
        Self {
            topics: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl DescribeQuorumRequestData {
    pub fn with_topics(mut self, value: Vec<TopicData>) -> Self {
        self.topics = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 0 || version > 2 {
            return Err(UnsupportedVersion::new(55, version).into());
        }
        let topics;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        topics = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
            for _ in 0..len {
                arr.push(TopicData::read(buf, version)?);
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
            topics,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 0 || version > 2 {
            return Err(UnsupportedVersion::new(55, version).into());
        }
        write_compact_array_length(buf, self.topics.len() as i32);
        for el in &self.topics {
            el.write(buf, version)?;
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        if version < 0 || version > 2 {
            return Err(UnsupportedVersion::new(55, version).into());
        }
        let mut len: usize = 0;
        len += compact_array_length_len(self.topics.len() as i32);
        for el in &self.topics {
            len += el.encoded_len(version)?;
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct TopicData {
    /// The topic name.
    pub topic_name: KafkaString,
    /// The partitions to describe.
    pub partitions: Vec<PartitionData>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for TopicData {
    fn default() -> Self {
        Self {
            topic_name: KafkaString::default(),
            partitions: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl TopicData {
    pub fn with_topic_name(mut self, value: KafkaString) -> Self {
        self.topic_name = value;
        self
    }
    pub fn with_partitions(mut self, value: Vec<PartitionData>) -> Self {
        self.partitions = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let topic_name;
        let partitions;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        topic_name = read_compact_string(buf)?;
        partitions = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
            for _ in 0..len {
                arr.push(PartitionData::read(buf, version)?);
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
            topic_name,
            partitions,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        write_compact_string(buf, &self.topic_name)?;
        write_compact_array_length(buf, self.partitions.len() as i32);
        for el in &self.partitions {
            el.write(buf, version)?;
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        let mut len: usize = 0;
        len += compact_string_len(&self.topic_name)?;
        len += compact_array_length_len(self.partitions.len() as i32);
        for el in &self.partitions {
            len += el.encoded_len(version)?;
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct PartitionData {
    /// The partition index.
    pub partition_index: i32,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for PartitionData {
    fn default() -> Self {
        Self {
            partition_index: 0_i32,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl PartitionData {
    pub fn with_partition_index(mut self, value: i32) -> Self {
        self.partition_index = value;
        self
    }
    pub fn read(buf: &mut Bytes, _version: i16) -> Result<Self> {
        let partition_index;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        partition_index = read_i32(buf)?;
        let tagged_fields = read_tagged_fields(buf)?;
        for field in &tagged_fields {
            match field.tag {
                _ => {
                    _unknown_tagged_fields.push(field.clone());
                },
            }
        }
        Ok(Self {
            partition_index,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, _version: i16) -> Result<()> {
        write_i32(buf, self.partition_index);
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
    pub fn encoded_len(&self, _version: i16) -> Result<usize> {
        let mut len: usize = 0;
        len += 4;
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
