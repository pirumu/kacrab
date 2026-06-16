//! Generated from OffsetCommitRequest.json - DO NOT EDIT
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
pub struct OffsetCommitRequestData {
    /// The unique group identifier.
    pub group_id: KafkaString,
    /// The generation of the group if using the classic group protocol or the member epoch if
    /// using the consumer protocol.
    pub generation_id_or_member_epoch: i32,
    /// The member ID assigned by the group coordinator.
    pub member_id: KafkaString,
    /// The unique identifier of the consumer instance provided by end user.
    pub group_instance_id: Option<KafkaString>,
    /// The time period in ms to retain the offset.
    pub retention_time_ms: i64,
    /// The topics to commit offsets for.
    pub topics: Vec<OffsetCommitRequestTopic>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for OffsetCommitRequestData {
    fn default() -> Self {
        Self {
            group_id: KafkaString::default(),
            generation_id_or_member_epoch: -1i32,
            member_id: KafkaString::default(),
            group_instance_id: None,
            retention_time_ms: -1i64,
            topics: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl OffsetCommitRequestData {
    pub fn with_group_id(mut self, value: KafkaString) -> Self {
        self.group_id = value;
        self
    }
    pub fn with_generation_id_or_member_epoch(mut self, value: i32) -> Self {
        self.generation_id_or_member_epoch = value;
        self
    }
    pub fn with_member_id(mut self, value: KafkaString) -> Self {
        self.member_id = value;
        self
    }
    pub fn with_group_instance_id(mut self, value: Option<KafkaString>) -> Self {
        self.group_instance_id = value;
        self
    }
    pub fn with_retention_time_ms(mut self, value: i64) -> Self {
        self.retention_time_ms = value;
        self
    }
    pub fn with_topics(mut self, value: Vec<OffsetCommitRequestTopic>) -> Self {
        self.topics = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 2 || version > 10 {
            return Err(UnsupportedVersion::new(8, version).into());
        }
        let group_id;
        let generation_id_or_member_epoch;
        let member_id;
        let mut group_instance_id = None;
        let mut retention_time_ms = -1i64;
        let topics;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 8 {
            group_id = read_compact_string(buf)?;
        } else {
            group_id = read_string(buf)?;
        }
        generation_id_or_member_epoch = read_i32(buf)?;
        if version >= 8 {
            member_id = read_compact_string(buf)?;
        } else {
            member_id = read_string(buf)?;
        }
        if version >= 7 {
            if version >= 8 {
                group_instance_id = read_compact_nullable_string(buf)?;
            } else {
                group_instance_id = read_nullable_string(buf)?;
            }
        }
        if version <= 4 {
            retention_time_ms = read_i64(buf)?;
        }
        if version >= 8 {
            topics = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(OffsetCommitRequestTopic::read(buf, version)?);
                }
                arr
            };
        } else {
            topics = {
                let len = read_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(OffsetCommitRequestTopic::read(buf, version)?);
                }
                arr
            };
        }
        if version >= 8 {
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
            group_id,
            generation_id_or_member_epoch,
            member_id,
            group_instance_id,
            retention_time_ms,
            topics,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 2 || version > 10 {
            return Err(UnsupportedVersion::new(8, version).into());
        }
        if version >= 8 {
            write_compact_string(buf, &self.group_id)?;
        } else {
            write_string(buf, &self.group_id)?;
        }
        write_i32(buf, self.generation_id_or_member_epoch);
        if version >= 8 {
            write_compact_string(buf, &self.member_id)?;
        } else {
            write_string(buf, &self.member_id)?;
        }
        if version >= 7 {
            if version >= 8 {
                write_compact_nullable_string(buf, self.group_instance_id.as_ref())?;
            } else {
                write_nullable_string(buf, self.group_instance_id.as_ref())?;
            }
        } else if self.group_instance_id != None {
            return Err(UnsupportedFieldVersion::new(8, "group_instance_id", version).into());
        }
        if version <= 4 {
            write_i64(buf, self.retention_time_ms);
        } else if self.retention_time_ms != -1i64 {
            return Err(UnsupportedFieldVersion::new(8, "retention_time_ms", version).into());
        }
        if version >= 8 {
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
        if version >= 8 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        if version < 2 || version > 10 {
            return Err(UnsupportedVersion::new(8, version).into());
        }
        let mut len: usize = 0;
        if version >= 8 {
            len += compact_string_len(&self.group_id)?;
        } else {
            len += string_len(&self.group_id)?;
        }
        len += 4;
        if version >= 8 {
            len += compact_string_len(&self.member_id)?;
        } else {
            len += string_len(&self.member_id)?;
        }
        if version >= 7 {
            if version >= 8 {
                len += compact_nullable_string_len(self.group_instance_id.as_ref())?;
            } else {
                len += nullable_string_len(self.group_instance_id.as_ref())?;
            }
        } else if self.group_instance_id != None {
            return Err(UnsupportedFieldVersion::new(8, "group_instance_id", version).into());
        }
        if version <= 4 {
            len += 8;
        } else if self.retention_time_ms != -1i64 {
            return Err(UnsupportedFieldVersion::new(8, "retention_time_ms", version).into());
        }
        if version >= 8 {
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
        if version >= 8 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            len += tagged_fields_len(&all_tags)?;
        }
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct OffsetCommitRequestTopic {
    /// The topic name.
    pub name: KafkaString,
    /// The topic ID.
    pub topic_id: KafkaUuid,
    /// Each partition to commit offsets for.
    pub partitions: Vec<OffsetCommitRequestPartition>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for OffsetCommitRequestTopic {
    fn default() -> Self {
        Self {
            name: KafkaString::default(),
            topic_id: KafkaUuid::ZERO,
            partitions: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl OffsetCommitRequestTopic {
    pub fn with_name(mut self, value: KafkaString) -> Self {
        self.name = value;
        self
    }
    pub fn with_topic_id(mut self, value: KafkaUuid) -> Self {
        self.topic_id = value;
        self
    }
    pub fn with_partitions(mut self, value: Vec<OffsetCommitRequestPartition>) -> Self {
        self.partitions = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let mut name = KafkaString::default();
        let mut topic_id = KafkaUuid::ZERO;
        let partitions;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version <= 9 {
            if version >= 8 {
                name = read_compact_string(buf)?;
            } else {
                name = read_string(buf)?;
            }
        }
        if version >= 10 {
            topic_id = read_uuid(buf)?;
        }
        if version >= 8 {
            partitions = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(OffsetCommitRequestPartition::read(buf, version)?);
                }
                arr
            };
        } else {
            partitions = {
                let len = read_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(OffsetCommitRequestPartition::read(buf, version)?);
                }
                arr
            };
        }
        if version >= 8 {
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
            topic_id,
            partitions,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version <= 9 {
            if version >= 8 {
                write_compact_string(buf, &self.name)?;
            } else {
                write_string(buf, &self.name)?;
            }
        } else if self.name != KafkaString::default() {
            return Err(UnsupportedFieldVersion::new(8, "name", version).into());
        }
        if version >= 10 {
            write_uuid(buf, &self.topic_id);
        } else if self.topic_id != KafkaUuid::ZERO {
            return Err(UnsupportedFieldVersion::new(8, "topic_id", version).into());
        }
        if version >= 8 {
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
        if version >= 8 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        let mut len: usize = 0;
        if version <= 9 {
            if version >= 8 {
                len += compact_string_len(&self.name)?;
            } else {
                len += string_len(&self.name)?;
            }
        } else if self.name != KafkaString::default() {
            return Err(UnsupportedFieldVersion::new(8, "name", version).into());
        }
        if version >= 10 {
            len += 16;
        } else if self.topic_id != KafkaUuid::ZERO {
            return Err(UnsupportedFieldVersion::new(8, "topic_id", version).into());
        }
        if version >= 8 {
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
        if version >= 8 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            len += tagged_fields_len(&all_tags)?;
        }
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct OffsetCommitRequestPartition {
    /// The partition index.
    pub partition_index: i32,
    /// The message offset to be committed.
    pub committed_offset: i64,
    /// The leader epoch of this partition.
    pub committed_leader_epoch: i32,
    /// Any associated metadata the client wants to keep.
    pub committed_metadata: Option<KafkaString>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for OffsetCommitRequestPartition {
    fn default() -> Self {
        Self {
            partition_index: 0_i32,
            committed_offset: 0_i64,
            committed_leader_epoch: -1i32,
            committed_metadata: None,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl OffsetCommitRequestPartition {
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
    pub fn with_committed_metadata(mut self, value: Option<KafkaString>) -> Self {
        self.committed_metadata = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let partition_index;
        let committed_offset;
        let mut committed_leader_epoch = -1i32;
        let committed_metadata;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        partition_index = read_i32(buf)?;
        committed_offset = read_i64(buf)?;
        if version >= 6 {
            committed_leader_epoch = read_i32(buf)?;
        }
        if version >= 8 {
            committed_metadata = read_compact_nullable_string(buf)?;
        } else {
            committed_metadata = read_nullable_string(buf)?;
        }
        if version >= 8 {
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
            committed_metadata,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        write_i32(buf, self.partition_index);
        write_i64(buf, self.committed_offset);
        if version >= 6 {
            write_i32(buf, self.committed_leader_epoch);
        } else if self.committed_leader_epoch != -1i32 {
            return Err(UnsupportedFieldVersion::new(8, "committed_leader_epoch", version).into());
        }
        if version >= 8 {
            write_compact_nullable_string(buf, self.committed_metadata.as_ref())?;
        } else {
            write_nullable_string(buf, self.committed_metadata.as_ref())?;
        }
        if version >= 8 {
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
        if version >= 6 {
            len += 4;
        } else if self.committed_leader_epoch != -1i32 {
            return Err(UnsupportedFieldVersion::new(8, "committed_leader_epoch", version).into());
        }
        if version >= 8 {
            len += compact_nullable_string_len(self.committed_metadata.as_ref())?;
        } else {
            len += nullable_string_len(self.committed_metadata.as_ref())?;
        }
        if version >= 8 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            len += tagged_fields_len(&all_tags)?;
        }
        Ok(len)
    }
}
