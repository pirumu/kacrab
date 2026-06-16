//! Generated from OffsetFetchResponse.json - DO NOT EDIT
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
pub struct OffsetFetchResponseData {
    /// The duration in milliseconds for which the request was throttled due to a quota violation,
    /// or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// The responses per topic.
    pub topics: Vec<OffsetFetchResponseTopic>,
    /// The top-level error code, or 0 if there was no error.
    pub error_code: i16,
    /// The responses per group id.
    pub groups: Vec<OffsetFetchResponseGroup>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for OffsetFetchResponseData {
    fn default() -> Self {
        Self {
            throttle_time_ms: 0_i32,
            topics: Vec::new(),
            error_code: 0i16,
            groups: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl OffsetFetchResponseData {
    pub fn with_throttle_time_ms(mut self, value: i32) -> Self {
        self.throttle_time_ms = value;
        self
    }
    pub fn with_topics(mut self, value: Vec<OffsetFetchResponseTopic>) -> Self {
        self.topics = value;
        self
    }
    pub fn with_error_code(mut self, value: i16) -> Self {
        self.error_code = value;
        self
    }
    pub fn with_groups(mut self, value: Vec<OffsetFetchResponseGroup>) -> Self {
        self.groups = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 1 || version > 10 {
            return Err(UnsupportedVersion::new(9, version).into());
        }
        let mut throttle_time_ms = 0_i32;
        let mut topics = Vec::new();
        let mut error_code = 0i16;
        let mut groups = Vec::new();
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 3 {
            throttle_time_ms = read_i32(buf)?;
        }
        if version <= 7 {
            if version >= 6 {
                topics = {
                    let len = read_compact_array_length(buf)?;
                    let mut arr = Vec::with_capacity(len.max(0) as usize);
                    for _ in 0..len {
                        arr.push(OffsetFetchResponseTopic::read(buf, version)?);
                    }
                    arr
                };
            } else {
                topics = {
                    let len = read_array_length(buf)?;
                    let mut arr = Vec::with_capacity(len.max(0) as usize);
                    for _ in 0..len {
                        arr.push(OffsetFetchResponseTopic::read(buf, version)?);
                    }
                    arr
                };
            }
        }
        if version >= 2 && version <= 7 {
            error_code = read_i16(buf)?;
        }
        if version >= 8 {
            groups = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(OffsetFetchResponseGroup::read(buf, version)?);
                }
                arr
            };
        }
        if version >= 6 {
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
            error_code,
            groups,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 1 || version > 10 {
            return Err(UnsupportedVersion::new(9, version).into());
        }
        if version >= 3 {
            write_i32(buf, self.throttle_time_ms);
        } else if self.throttle_time_ms != 0_i32 {
            return Err(UnsupportedFieldVersion::new(9, "throttle_time_ms", version).into());
        }
        if version <= 7 {
            if version >= 6 {
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
        } else if self.topics != Vec::new() {
            return Err(UnsupportedFieldVersion::new(9, "topics", version).into());
        }
        if version >= 2 && version <= 7 {
            write_i16(buf, self.error_code);
        } else if self.error_code != 0i16 {
            return Err(UnsupportedFieldVersion::new(9, "error_code", version).into());
        }
        if version >= 8 {
            write_compact_array_length(buf, self.groups.len() as i32);
            for el in &self.groups {
                el.write(buf, version)?;
            }
        } else if self.groups != Vec::new() {
            return Err(UnsupportedFieldVersion::new(9, "groups", version).into());
        }
        if version >= 6 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        if version < 1 || version > 10 {
            return Err(UnsupportedVersion::new(9, version).into());
        }
        let mut len: usize = 0;
        if version >= 3 {
            len += 4;
        } else if self.throttle_time_ms != 0_i32 {
            return Err(UnsupportedFieldVersion::new(9, "throttle_time_ms", version).into());
        }
        if version <= 7 {
            if version >= 6 {
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
        } else if self.topics != Vec::new() {
            return Err(UnsupportedFieldVersion::new(9, "topics", version).into());
        }
        if version >= 2 && version <= 7 {
            len += 2;
        } else if self.error_code != 0i16 {
            return Err(UnsupportedFieldVersion::new(9, "error_code", version).into());
        }
        if version >= 8 {
            len += compact_array_length_len(self.groups.len() as i32);
            for el in &self.groups {
                len += el.encoded_len(version)?;
            }
        } else if self.groups != Vec::new() {
            return Err(UnsupportedFieldVersion::new(9, "groups", version).into());
        }
        if version >= 6 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            len += tagged_fields_len(&all_tags)?;
        }
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct OffsetFetchResponseTopic {
    /// The topic name.
    pub name: KafkaString,
    /// The responses per partition.
    pub partitions: Vec<OffsetFetchResponsePartition>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for OffsetFetchResponseTopic {
    fn default() -> Self {
        Self {
            name: KafkaString::default(),
            partitions: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl OffsetFetchResponseTopic {
    pub fn with_name(mut self, value: KafkaString) -> Self {
        self.name = value;
        self
    }
    pub fn with_partitions(mut self, value: Vec<OffsetFetchResponsePartition>) -> Self {
        self.partitions = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let name;
        let partitions;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 6 {
            name = read_compact_string(buf)?;
        } else {
            name = read_string(buf)?;
        }
        if version >= 6 {
            partitions = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(OffsetFetchResponsePartition::read(buf, version)?);
                }
                arr
            };
        } else {
            partitions = {
                let len = read_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(OffsetFetchResponsePartition::read(buf, version)?);
                }
                arr
            };
        }
        if version >= 6 {
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
        if version >= 6 {
            write_compact_string(buf, &self.name)?;
        } else {
            write_string(buf, &self.name)?;
        }
        if version >= 6 {
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
        if version >= 6 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        let mut len: usize = 0;
        if version >= 6 {
            len += compact_string_len(&self.name)?;
        } else {
            len += string_len(&self.name)?;
        }
        if version >= 6 {
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
        if version >= 6 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            len += tagged_fields_len(&all_tags)?;
        }
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct OffsetFetchResponsePartition {
    /// The partition index.
    pub partition_index: i32,
    /// The committed message offset.
    pub committed_offset: i64,
    /// The leader epoch.
    pub committed_leader_epoch: i32,
    /// The partition metadata.
    pub metadata: Option<KafkaString>,
    /// The error code, or 0 if there was no error.
    pub error_code: i16,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for OffsetFetchResponsePartition {
    fn default() -> Self {
        Self {
            partition_index: 0_i32,
            committed_offset: 0_i64,
            committed_leader_epoch: -1i32,
            metadata: None,
            error_code: 0_i16,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl OffsetFetchResponsePartition {
    pub fn with_partition_index(mut self, value: i32) -> Self {
        self.partition_index = value;
        self
    }
    pub fn with_committed_offset(mut self, value: i64) -> Self {
        self.committed_offset = value;
        self
    }
    pub fn with_committed_leader_epoch(mut self, value: i32) -> Self {
        self.committed_leader_epoch = value;
        self
    }
    pub fn with_metadata(mut self, value: Option<KafkaString>) -> Self {
        self.metadata = value;
        self
    }
    pub fn with_error_code(mut self, value: i16) -> Self {
        self.error_code = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let partition_index;
        let committed_offset;
        let mut committed_leader_epoch = -1i32;
        let metadata;
        let error_code;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        partition_index = read_i32(buf)?;
        committed_offset = read_i64(buf)?;
        if version >= 5 {
            committed_leader_epoch = read_i32(buf)?;
        }
        if version >= 6 {
            metadata = read_compact_nullable_string(buf)?;
        } else {
            metadata = read_nullable_string(buf)?;
        }
        error_code = read_i16(buf)?;
        if version >= 6 {
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
            committed_offset,
            committed_leader_epoch,
            metadata,
            error_code,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        write_i32(buf, self.partition_index);
        write_i64(buf, self.committed_offset);
        if version >= 5 {
            write_i32(buf, self.committed_leader_epoch);
        } else if self.committed_leader_epoch != -1i32 {
            return Err(UnsupportedFieldVersion::new(9, "committed_leader_epoch", version).into());
        }
        if version >= 6 {
            write_compact_nullable_string(buf, self.metadata.as_ref())?;
        } else {
            write_nullable_string(buf, self.metadata.as_ref())?;
        }
        write_i16(buf, self.error_code);
        if version >= 6 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        let mut len: usize = 0;
        len += 4;
        len += 8;
        if version >= 5 {
            len += 4;
        } else if self.committed_leader_epoch != -1i32 {
            return Err(UnsupportedFieldVersion::new(9, "committed_leader_epoch", version).into());
        }
        if version >= 6 {
            len += compact_nullable_string_len(self.metadata.as_ref())?;
        } else {
            len += nullable_string_len(self.metadata.as_ref())?;
        }
        len += 2;
        if version >= 6 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            len += tagged_fields_len(&all_tags)?;
        }
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct OffsetFetchResponseGroup {
    /// The group ID.
    pub group_id: KafkaString,
    /// The responses per topic.
    pub topics: Vec<OffsetFetchResponseTopics>,
    /// The group-level error code, or 0 if there was no error.
    pub error_code: i16,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for OffsetFetchResponseGroup {
    fn default() -> Self {
        Self {
            group_id: KafkaString::default(),
            topics: Vec::new(),
            error_code: 0i16,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl OffsetFetchResponseGroup {
    pub fn with_group_id(mut self, value: KafkaString) -> Self {
        self.group_id = value;
        self
    }
    pub fn with_topics(mut self, value: Vec<OffsetFetchResponseTopics>) -> Self {
        self.topics = value;
        self
    }
    pub fn with_error_code(mut self, value: i16) -> Self {
        self.error_code = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let group_id;
        let topics;
        let error_code;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        group_id = read_compact_string(buf)?;
        topics = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(len.max(0) as usize);
            for _ in 0..len {
                arr.push(OffsetFetchResponseTopics::read(buf, version)?);
            }
            arr
        };
        error_code = read_i16(buf)?;
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
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct OffsetFetchResponseTopics {
    /// The topic name.
    pub name: KafkaString,
    /// The topic ID.
    pub topic_id: KafkaUuid,
    /// The responses per partition.
    pub partitions: Vec<OffsetFetchResponsePartitions>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for OffsetFetchResponseTopics {
    fn default() -> Self {
        Self {
            name: KafkaString::default(),
            topic_id: KafkaUuid::ZERO,
            partitions: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl OffsetFetchResponseTopics {
    pub fn with_name(mut self, value: KafkaString) -> Self {
        self.name = value;
        self
    }
    pub fn with_topic_id(mut self, value: KafkaUuid) -> Self {
        self.topic_id = value;
        self
    }
    pub fn with_partitions(mut self, value: Vec<OffsetFetchResponsePartitions>) -> Self {
        self.partitions = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let mut name = KafkaString::default();
        let mut topic_id = KafkaUuid::ZERO;
        let partitions;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version <= 9 {
            name = read_compact_string(buf)?;
        }
        if version >= 10 {
            topic_id = read_uuid(buf)?;
        }
        partitions = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(len.max(0) as usize);
            for _ in 0..len {
                arr.push(OffsetFetchResponsePartitions::read(buf, version)?);
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
            topic_id,
            partitions,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version <= 9 {
            write_compact_string(buf, &self.name)?;
        } else if self.name != KafkaString::default() {
            return Err(UnsupportedFieldVersion::new(9, "name", version).into());
        }
        if version >= 10 {
            write_uuid(buf, &self.topic_id);
        } else if self.topic_id != KafkaUuid::ZERO {
            return Err(UnsupportedFieldVersion::new(9, "topic_id", version).into());
        }
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
        if version <= 9 {
            len += compact_string_len(&self.name)?;
        } else if self.name != KafkaString::default() {
            return Err(UnsupportedFieldVersion::new(9, "name", version).into());
        }
        if version >= 10 {
            len += 16;
        } else if self.topic_id != KafkaUuid::ZERO {
            return Err(UnsupportedFieldVersion::new(9, "topic_id", version).into());
        }
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
pub struct OffsetFetchResponsePartitions {
    /// The partition index.
    pub partition_index: i32,
    /// The committed message offset.
    pub committed_offset: i64,
    /// The leader epoch.
    pub committed_leader_epoch: i32,
    /// The partition metadata.
    pub metadata: Option<KafkaString>,
    /// The partition-level error code, or 0 if there was no error.
    pub error_code: i16,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for OffsetFetchResponsePartitions {
    fn default() -> Self {
        Self {
            partition_index: 0_i32,
            committed_offset: 0_i64,
            committed_leader_epoch: -1i32,
            metadata: None,
            error_code: 0_i16,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl OffsetFetchResponsePartitions {
    pub fn with_partition_index(mut self, value: i32) -> Self {
        self.partition_index = value;
        self
    }
    pub fn with_committed_offset(mut self, value: i64) -> Self {
        self.committed_offset = value;
        self
    }
    pub fn with_committed_leader_epoch(mut self, value: i32) -> Self {
        self.committed_leader_epoch = value;
        self
    }
    pub fn with_metadata(mut self, value: Option<KafkaString>) -> Self {
        self.metadata = value;
        self
    }
    pub fn with_error_code(mut self, value: i16) -> Self {
        self.error_code = value;
        self
    }
    pub fn read(buf: &mut Bytes, _version: i16) -> Result<Self> {
        let partition_index;
        let committed_offset;
        let committed_leader_epoch;
        let metadata;
        let error_code;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        partition_index = read_i32(buf)?;
        committed_offset = read_i64(buf)?;
        committed_leader_epoch = read_i32(buf)?;
        metadata = read_compact_nullable_string(buf)?;
        error_code = read_i16(buf)?;
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
            committed_offset,
            committed_leader_epoch,
            metadata,
            error_code,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, _version: i16) -> Result<()> {
        write_i32(buf, self.partition_index);
        write_i64(buf, self.committed_offset);
        write_i32(buf, self.committed_leader_epoch);
        write_compact_nullable_string(buf, self.metadata.as_ref())?;
        write_i16(buf, self.error_code);
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
    pub fn encoded_len(&self, _version: i16) -> Result<usize> {
        let mut len: usize = 0;
        len += 4;
        len += 8;
        len += 4;
        len += compact_nullable_string_len(self.metadata.as_ref())?;
        len += 2;
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
