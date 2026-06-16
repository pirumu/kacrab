//! Generated from DescribeTopicPartitionsRequest.json - DO NOT EDIT
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
pub struct DescribeTopicPartitionsRequestData {
    /// The topics to fetch details for.
    pub topics: Vec<TopicRequest>,
    /// The maximum number of partitions included in the response.
    pub response_partition_limit: i32,
    /// The first topic and partition index to fetch details for.
    pub cursor: Option<Box<Cursor>>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for DescribeTopicPartitionsRequestData {
    fn default() -> Self {
        Self {
            topics: Vec::new(),
            response_partition_limit: 2000i32,
            cursor: None,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl DescribeTopicPartitionsRequestData {
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 0 || version > 0 {
            return Err(UnsupportedVersion::new(75, version).into());
        }
        let topics;
        let response_partition_limit;
        let cursor;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        topics = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(len.max(0) as usize);
            for _ in 0..len {
                arr.push(TopicRequest::read(buf, version)?);
            }
            arr
        };
        response_partition_limit = read_i32(buf)?;
        cursor = {
            let marker = read_i8(buf)?;
            if marker < 0 {
                None
            } else {
                Some(Box::new(Cursor::read(buf, version)?))
            }
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
            response_partition_limit,
            cursor,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 0 || version > 0 {
            return Err(UnsupportedVersion::new(75, version).into());
        }
        write_compact_array_length(buf, self.topics.len() as i32);
        for el in &self.topics {
            el.write(buf, version)?;
        }
        write_i32(buf, self.response_partition_limit);
        match &self.cursor {
            None => {
                write_i8(buf, -1);
            },
            Some(v) => {
                write_i8(buf, 1);
                v.write(buf, version)?;
            },
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct TopicRequest {
    /// The topic name.
    pub name: KafkaString,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for TopicRequest {
    fn default() -> Self {
        Self {
            name: KafkaString::default(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl TopicRequest {
    pub fn read(buf: &mut Bytes, _version: i16) -> Result<Self> {
        let name;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        name = read_compact_string(buf)?;
        let tagged_fields = read_tagged_fields(buf)?;
        for field in &tagged_fields {
            match field.tag {
                _ => {
                    _unknown_tagged_fields.push(field.clone());
                },
            }
        }
        Ok(Self {
            name,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, _version: i16) -> Result<()> {
        write_compact_string(buf, &self.name)?;
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct Cursor {
    /// The name for the first topic to process.
    pub topic_name: KafkaString,
    /// The partition index to start with.
    pub partition_index: i32,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for Cursor {
    fn default() -> Self {
        Self {
            topic_name: KafkaString::default(),
            partition_index: 0_i32,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl Cursor {
    pub fn read(buf: &mut Bytes, _version: i16) -> Result<Self> {
        let topic_name;
        let partition_index;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        topic_name = read_compact_string(buf)?;
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
            topic_name,
            partition_index,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, _version: i16) -> Result<()> {
        write_compact_string(buf, &self.topic_name)?;
        write_i32(buf, self.partition_index);
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
}
