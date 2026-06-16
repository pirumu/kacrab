//! Generated from ListPartitionReassignmentsRequest.json - DO NOT EDIT
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
pub struct ListPartitionReassignmentsRequestData {
    /// The time in ms to wait for the request to complete.
    pub timeout_ms: i32,
    /// The topics to list partition reassignments for, or null to list everything.
    pub topics: Option<Vec<ListPartitionReassignmentsTopics>>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for ListPartitionReassignmentsRequestData {
    fn default() -> Self {
        Self {
            timeout_ms: 60000i32,
            topics: None,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl ListPartitionReassignmentsRequestData {
    pub fn with_timeout_ms(mut self, value: i32) -> Self {
        self.timeout_ms = value;
        self
    }
    pub fn with_topics(mut self, value: Option<Vec<ListPartitionReassignmentsTopics>>) -> Self {
        self.topics = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 0 || version > 0 {
            return Err(UnsupportedVersion::new(46, version).into());
        }
        let timeout_ms;
        let topics;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        timeout_ms = read_i32(buf)?;
        topics = {
            let len = read_compact_array_length(buf)?;
            if len < 0 {
                None
            } else {
                let mut arr = Vec::with_capacity(len as usize);
                for _ in 0..len {
                    arr.push(ListPartitionReassignmentsTopics::read(buf, version)?);
                }
                Some(arr)
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
            timeout_ms,
            topics,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 0 || version > 0 {
            return Err(UnsupportedVersion::new(46, version).into());
        }
        write_i32(buf, self.timeout_ms);
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
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        if version < 0 || version > 0 {
            return Err(UnsupportedVersion::new(46, version).into());
        }
        let mut len: usize = 0;
        len += 4;
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
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct ListPartitionReassignmentsTopics {
    /// The topic name.
    pub name: KafkaString,
    /// The partitions to list partition reassignments for.
    pub partition_indexes: Vec<i32>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for ListPartitionReassignmentsTopics {
    fn default() -> Self {
        Self {
            name: KafkaString::default(),
            partition_indexes: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl ListPartitionReassignmentsTopics {
    pub fn with_name(mut self, value: KafkaString) -> Self {
        self.name = value;
        self
    }
    pub fn with_partition_indexes(mut self, value: Vec<i32>) -> Self {
        self.partition_indexes = value;
        self
    }
    pub fn read(buf: &mut Bytes, _version: i16) -> Result<Self> {
        let name;
        let partition_indexes;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        name = read_compact_string(buf)?;
        partition_indexes = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(len.max(0) as usize);
            for _ in 0..len {
                arr.push(read_i32(buf)?);
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
            name,
            partition_indexes,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, _version: i16) -> Result<()> {
        write_compact_string(buf, &self.name)?;
        write_compact_array_length(buf, self.partition_indexes.len() as i32);
        for el in &self.partition_indexes {
            write_i32(buf, *el);
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
    pub fn encoded_len(&self, _version: i16) -> Result<usize> {
        let mut len: usize = 0;
        len += compact_string_len(&self.name)?;
        len += compact_array_length_len(self.partition_indexes.len() as i32);
        len += self.partition_indexes.len() * 4usize;
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
