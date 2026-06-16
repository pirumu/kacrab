//! Generated from DescribeShareGroupOffsetsResponse.json - DO NOT EDIT
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
pub struct DescribeShareGroupOffsetsResponseData {
    /// The duration in milliseconds for which the request was throttled due to a quota violation,
    /// or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// The results for each group.
    pub groups: Vec<DescribeShareGroupOffsetsResponseGroup>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for DescribeShareGroupOffsetsResponseData {
    fn default() -> Self {
        Self {
            throttle_time_ms: 0_i32,
            groups: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl DescribeShareGroupOffsetsResponseData {
    pub fn with_throttle_time_ms(mut self, value: i32) -> Self {
        self.throttle_time_ms = value;
        self
    }
    pub fn with_groups(mut self, value: Vec<DescribeShareGroupOffsetsResponseGroup>) -> Self {
        self.groups = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 0 || version > 1 {
            return Err(UnsupportedVersion::new(90, version).into());
        }
        let throttle_time_ms;
        let groups;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        throttle_time_ms = read_i32(buf)?;
        groups = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(len.max(0) as usize);
            for _ in 0..len {
                arr.push(DescribeShareGroupOffsetsResponseGroup::read(buf, version)?);
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
            throttle_time_ms,
            groups,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 0 || version > 1 {
            return Err(UnsupportedVersion::new(90, version).into());
        }
        write_i32(buf, self.throttle_time_ms);
        write_compact_array_length(buf, self.groups.len() as i32);
        for el in &self.groups {
            el.write(buf, version)?;
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        if version < 0 || version > 1 {
            return Err(UnsupportedVersion::new(90, version).into());
        }
        let mut len: usize = 0;
        len += 4;
        len += compact_array_length_len(self.groups.len() as i32);
        for el in &self.groups {
            len += el.encoded_len(version)?;
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct DescribeShareGroupOffsetsResponseGroup {
    /// The group identifier.
    pub group_id: KafkaString,
    /// The results for each topic.
    pub topics: Vec<DescribeShareGroupOffsetsResponseTopic>,
    /// The group-level error code, or 0 if there was no error.
    pub error_code: i16,
    /// The group-level error message, or null if there was no error.
    pub error_message: Option<KafkaString>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for DescribeShareGroupOffsetsResponseGroup {
    fn default() -> Self {
        Self {
            group_id: KafkaString::default(),
            topics: Vec::new(),
            error_code: 0_i16,
            error_message: None,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl DescribeShareGroupOffsetsResponseGroup {
    pub fn with_group_id(mut self, value: KafkaString) -> Self {
        self.group_id = value;
        self
    }
    pub fn with_topics(mut self, value: Vec<DescribeShareGroupOffsetsResponseTopic>) -> Self {
        self.topics = value;
        self
    }
    pub fn with_error_code(mut self, value: i16) -> Self {
        self.error_code = value;
        self
    }
    pub fn with_error_message(mut self, value: Option<KafkaString>) -> Self {
        self.error_message = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let group_id;
        let topics;
        let error_code;
        let error_message;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        group_id = read_compact_string(buf)?;
        topics = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(len.max(0) as usize);
            for _ in 0..len {
                arr.push(DescribeShareGroupOffsetsResponseTopic::read(buf, version)?);
            }
            arr
        };
        error_code = read_i16(buf)?;
        error_message = read_compact_nullable_string(buf)?;
        let tagged_fields = read_tagged_fields(buf)?;
        for field in &tagged_fields {
            match field.tag {
                _ => {
                    _unknown_tagged_fields.push(field.clone());
                },
            }
        }
        Ok(Self {
            group_id,
            topics,
            error_code,
            error_message,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        write_compact_string(buf, &self.group_id)?;
        write_compact_array_length(buf, self.topics.len() as i32);
        for el in &self.topics {
            el.write(buf, version)?;
        }
        write_i16(buf, self.error_code);
        write_compact_nullable_string(buf, self.error_message.as_ref())?;
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        let mut len: usize = 0;
        len += compact_string_len(&self.group_id)?;
        len += compact_array_length_len(self.topics.len() as i32);
        for el in &self.topics {
            len += el.encoded_len(version)?;
        }
        len += 2;
        len += compact_nullable_string_len(self.error_message.as_ref())?;
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct DescribeShareGroupOffsetsResponseTopic {
    /// The topic name.
    pub topic_name: KafkaString,
    /// The unique topic ID.
    pub topic_id: KafkaUuid,
    pub partitions: Vec<DescribeShareGroupOffsetsResponsePartition>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for DescribeShareGroupOffsetsResponseTopic {
    fn default() -> Self {
        Self {
            topic_name: KafkaString::default(),
            topic_id: KafkaUuid::ZERO,
            partitions: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl DescribeShareGroupOffsetsResponseTopic {
    pub fn with_topic_name(mut self, value: KafkaString) -> Self {
        self.topic_name = value;
        self
    }
    pub fn with_topic_id(mut self, value: KafkaUuid) -> Self {
        self.topic_id = value;
        self
    }
    pub fn with_partitions(
        mut self,
        value: Vec<DescribeShareGroupOffsetsResponsePartition>,
    ) -> Self {
        self.partitions = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let topic_name;
        let topic_id;
        let partitions;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        topic_name = read_compact_string(buf)?;
        topic_id = read_uuid(buf)?;
        partitions = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(len.max(0) as usize);
            for _ in 0..len {
                arr.push(DescribeShareGroupOffsetsResponsePartition::read(
                    buf, version,
                )?);
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
            topic_id,
            partitions,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        write_compact_string(buf, &self.topic_name)?;
        write_uuid(buf, &self.topic_id);
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
        len += 16;
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
pub struct DescribeShareGroupOffsetsResponsePartition {
    /// The partition index.
    pub partition_index: i32,
    /// The share-partition start offset.
    pub start_offset: i64,
    /// The leader epoch of the partition.
    pub leader_epoch: i32,
    /// The share-partition lag.
    pub lag: i64,
    /// The partition-level error code, or 0 if there was no error.
    pub error_code: i16,
    /// The partition-level error message, or null if there was no error.
    pub error_message: Option<KafkaString>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for DescribeShareGroupOffsetsResponsePartition {
    fn default() -> Self {
        Self {
            partition_index: 0_i32,
            start_offset: 0_i64,
            leader_epoch: 0_i32,
            lag: -1i64,
            error_code: 0_i16,
            error_message: None,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl DescribeShareGroupOffsetsResponsePartition {
    pub fn with_partition_index(mut self, value: i32) -> Self {
        self.partition_index = value;
        self
    }
    pub fn with_start_offset(mut self, value: i64) -> Self {
        self.start_offset = value;
        self
    }
    pub fn with_leader_epoch(mut self, value: i32) -> Self {
        self.leader_epoch = value;
        self
    }
    pub fn with_lag(mut self, value: i64) -> Self {
        self.lag = value;
        self
    }
    pub fn with_error_code(mut self, value: i16) -> Self {
        self.error_code = value;
        self
    }
    pub fn with_error_message(mut self, value: Option<KafkaString>) -> Self {
        self.error_message = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let partition_index;
        let start_offset;
        let leader_epoch;
        let mut lag = -1i64;
        let error_code;
        let error_message;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        partition_index = read_i32(buf)?;
        start_offset = read_i64(buf)?;
        leader_epoch = read_i32(buf)?;
        if version >= 1 {
            lag = read_i64(buf)?;
        }
        error_code = read_i16(buf)?;
        error_message = read_compact_nullable_string(buf)?;
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
            start_offset,
            leader_epoch,
            lag,
            error_code,
            error_message,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        write_i32(buf, self.partition_index);
        write_i64(buf, self.start_offset);
        write_i32(buf, self.leader_epoch);
        if version >= 1 {
            write_i64(buf, self.lag);
        } else if self.lag != -1i64 {
            return Err(UnsupportedFieldVersion::new(90, "lag", version).into());
        }
        write_i16(buf, self.error_code);
        write_compact_nullable_string(buf, self.error_message.as_ref())?;
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        let mut len: usize = 0;
        len += 4;
        len += 8;
        len += 4;
        if version >= 1 {
            len += 8;
        } else if self.lag != -1i64 {
            return Err(UnsupportedFieldVersion::new(90, "lag", version).into());
        }
        len += 2;
        len += compact_nullable_string_len(self.error_message.as_ref())?;
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
