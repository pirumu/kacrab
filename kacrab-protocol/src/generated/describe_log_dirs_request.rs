//! Generated from DescribeLogDirsRequest.json - DO NOT EDIT
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
pub struct DescribeLogDirsRequestData {
    /// Each topic that we want to describe log directories for, or null for all topics.
    pub topics: Option<Vec<DescribableLogDirTopic>>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for DescribeLogDirsRequestData {
    fn default() -> Self {
        Self {
            topics: None,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl DescribeLogDirsRequestData {
    pub fn with_topics(mut self, value: Option<Vec<DescribableLogDirTopic>>) -> Self {
        self.topics = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 1 || version > 5 {
            return Err(UnsupportedVersion::new(35, version).into());
        }
        let topics;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 2 {
            topics = {
                let len = read_compact_array_length(buf)?;
                if len < 0 {
                    None
                } else {
                    let mut arr = Vec::with_capacity(len as usize);
                    for _ in 0..len {
                        arr.push(DescribableLogDirTopic::read(buf, version)?);
                    }
                    Some(arr)
                }
            };
        } else {
            topics = {
                let len = read_array_length(buf)?;
                if len < 0 {
                    None
                } else {
                    let mut arr = Vec::with_capacity(len as usize);
                    for _ in 0..len {
                        arr.push(DescribableLogDirTopic::read(buf, version)?);
                    }
                    Some(arr)
                }
            };
        }
        if version >= 2 {
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
            topics,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 1 || version > 5 {
            return Err(UnsupportedVersion::new(35, version).into());
        }
        if version >= 2 {
            match &self.topics {
                None => {
                    write_compact_array_length(buf, -1);
                },
                Some(arr) => {
                    write_compact_array_length(buf, arr.len() as i32);
                    for el in arr {
                        el.write(buf, version)?;
                    }
                },
            }
        } else {
            match &self.topics {
                None => {
                    write_array_length(buf, -1);
                },
                Some(arr) => {
                    write_array_length(buf, arr.len() as i32);
                    for el in arr {
                        el.write(buf, version)?;
                    }
                },
            }
        }
        if version >= 2 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        if version < 1 || version > 5 {
            return Err(UnsupportedVersion::new(35, version).into());
        }
        let mut len: usize = 0;
        if version >= 2 {
            match &self.topics {
                None => {
                    len += compact_array_length_len(-1);
                },
                Some(arr) => {
                    len += compact_array_length_len(arr.len() as i32);
                    for el in arr {
                        len += el.encoded_len(version)?;
                    }
                },
            }
        } else {
            match &self.topics {
                None => {
                    len += array_length_len();
                },
                Some(arr) => {
                    len += array_length_len();
                    for el in arr {
                        len += el.encoded_len(version)?;
                    }
                },
            }
        }
        if version >= 2 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            len += tagged_fields_len(&all_tags)?;
        }
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct DescribableLogDirTopic {
    /// The topic name.
    pub topic: KafkaString,
    /// The partition indexes.
    pub partitions: Vec<i32>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for DescribableLogDirTopic {
    fn default() -> Self {
        Self {
            topic: KafkaString::default(),
            partitions: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl DescribableLogDirTopic {
    pub fn with_topic(mut self, value: KafkaString) -> Self {
        self.topic = value;
        self
    }
    pub fn with_partitions(mut self, value: Vec<i32>) -> Self {
        self.partitions = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let topic;
        let partitions;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 2 {
            topic = read_compact_string(buf)?;
        } else {
            topic = read_string(buf)?;
        }
        if version >= 2 {
            partitions = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(read_i32(buf)?);
                }
                arr
            };
        } else {
            partitions = {
                let len = read_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(read_i32(buf)?);
                }
                arr
            };
        }
        if version >= 2 {
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
            topic,
            partitions,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version >= 2 {
            write_compact_string(buf, &self.topic)?;
        } else {
            write_string(buf, &self.topic)?;
        }
        if version >= 2 {
            write_compact_array_length(buf, self.partitions.len() as i32);
            for el in &self.partitions {
                write_i32(buf, *el);
            }
        } else {
            write_array_length(buf, self.partitions.len() as i32);
            for el in &self.partitions {
                write_i32(buf, *el);
            }
        }
        if version >= 2 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        let mut len: usize = 0;
        if version >= 2 {
            len += compact_string_len(&self.topic)?;
        } else {
            len += string_len(&self.topic)?;
        }
        if version >= 2 {
            len += compact_array_length_len(self.partitions.len() as i32);
            len += self.partitions.len() * 4usize;
        } else {
            len += array_length_len();
            len += self.partitions.len() * 4usize;
        }
        if version >= 2 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            len += tagged_fields_len(&all_tags)?;
        }
        Ok(len)
    }
}
