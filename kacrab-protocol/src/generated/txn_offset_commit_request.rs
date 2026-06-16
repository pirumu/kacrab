//! Generated from TxnOffsetCommitRequest.json - DO NOT EDIT
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
pub struct TxnOffsetCommitRequestData {
    /// The ID of the transaction.
    pub transactional_id: KafkaString,
    /// The ID of the group.
    pub group_id: KafkaString,
    /// The current producer ID in use by the transactional ID.
    pub producer_id: i64,
    /// The current epoch associated with the producer ID.
    pub producer_epoch: i16,
    /// The generation of the consumer.
    pub generation_id: i32,
    /// The member ID assigned by the group coordinator.
    pub member_id: KafkaString,
    /// The unique identifier of the consumer instance provided by end user.
    pub group_instance_id: Option<KafkaString>,
    /// Each topic that we want to commit offsets for.
    pub topics: Vec<TxnOffsetCommitRequestTopic>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for TxnOffsetCommitRequestData {
    fn default() -> Self {
        Self {
            transactional_id: KafkaString::default(),
            group_id: KafkaString::default(),
            producer_id: 0_i64,
            producer_epoch: 0_i16,
            generation_id: -1i32,
            member_id: KafkaString::default(),
            group_instance_id: None,
            topics: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl TxnOffsetCommitRequestData {
    pub fn with_transactional_id(mut self, value: KafkaString) -> Self {
        self.transactional_id = value;
        self
    }
    pub fn with_group_id(mut self, value: KafkaString) -> Self {
        self.group_id = value;
        self
    }
    pub fn with_producer_id(mut self, value: i64) -> Self {
        self.producer_id = value;
        self
    }
    pub fn with_producer_epoch(mut self, value: i16) -> Self {
        self.producer_epoch = value;
        self
    }
    pub fn with_generation_id(mut self, value: i32) -> Self {
        self.generation_id = value;
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
    pub fn with_topics(mut self, value: Vec<TxnOffsetCommitRequestTopic>) -> Self {
        self.topics = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 0 || version > 5 {
            return Err(UnsupportedVersion::new(28, version).into());
        }
        let transactional_id;
        let group_id;
        let producer_id;
        let producer_epoch;
        let mut generation_id = -1i32;
        let mut member_id = KafkaString::default();
        let mut group_instance_id = None;
        let topics;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 3 {
            transactional_id = read_compact_string(buf)?;
        } else {
            transactional_id = read_string(buf)?;
        }
        if version >= 3 {
            group_id = read_compact_string(buf)?;
        } else {
            group_id = read_string(buf)?;
        }
        producer_id = read_i64(buf)?;
        producer_epoch = read_i16(buf)?;
        if version >= 3 {
            generation_id = read_i32(buf)?;
        }
        if version >= 3 {
            member_id = read_compact_string(buf)?;
        }
        if version >= 3 {
            group_instance_id = read_compact_nullable_string(buf)?;
        }
        if version >= 3 {
            topics = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(TxnOffsetCommitRequestTopic::read(buf, version)?);
                }
                arr
            };
        } else {
            topics = {
                let len = read_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(TxnOffsetCommitRequestTopic::read(buf, version)?);
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
            transactional_id,
            group_id,
            producer_id,
            producer_epoch,
            generation_id,
            member_id,
            group_instance_id,
            topics,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 0 || version > 5 {
            return Err(UnsupportedVersion::new(28, version).into());
        }
        if version >= 3 {
            write_compact_string(buf, &self.transactional_id)?;
        } else {
            write_string(buf, &self.transactional_id)?;
        }
        if version >= 3 {
            write_compact_string(buf, &self.group_id)?;
        } else {
            write_string(buf, &self.group_id)?;
        }
        write_i64(buf, self.producer_id);
        write_i16(buf, self.producer_epoch);
        if version >= 3 {
            write_i32(buf, self.generation_id);
        } else if self.generation_id != -1i32 {
            return Err(UnsupportedFieldVersion::new(28, "generation_id", version).into());
        }
        if version >= 3 {
            write_compact_string(buf, &self.member_id)?;
        } else if self.member_id != KafkaString::default() {
            return Err(UnsupportedFieldVersion::new(28, "member_id", version).into());
        }
        if version >= 3 {
            write_compact_nullable_string(buf, self.group_instance_id.as_ref())?;
        } else if self.group_instance_id != None {
            return Err(UnsupportedFieldVersion::new(28, "group_instance_id", version).into());
        }
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
        if version >= 3 {
            len += compact_string_len(&self.transactional_id)?;
        } else {
            len += string_len(&self.transactional_id)?;
        }
        if version >= 3 {
            len += compact_string_len(&self.group_id)?;
        } else {
            len += string_len(&self.group_id)?;
        }
        len += 8;
        len += 2;
        if version >= 3 {
            len += 4;
        } else if self.generation_id != -1i32 {
            return Err(UnsupportedFieldVersion::new(28, "generation_id", version).into());
        }
        if version >= 3 {
            len += compact_string_len(&self.member_id)?;
        } else if self.member_id != KafkaString::default() {
            return Err(UnsupportedFieldVersion::new(28, "member_id", version).into());
        }
        if version >= 3 {
            len += compact_nullable_string_len(self.group_instance_id.as_ref())?;
        } else if self.group_instance_id != None {
            return Err(UnsupportedFieldVersion::new(28, "group_instance_id", version).into());
        }
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
pub struct TxnOffsetCommitRequestTopic {
    /// The topic name.
    pub name: KafkaString,
    /// The partitions inside the topic that we want to commit offsets for.
    pub partitions: Vec<TxnOffsetCommitRequestPartition>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for TxnOffsetCommitRequestTopic {
    fn default() -> Self {
        Self {
            name: KafkaString::default(),
            partitions: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl TxnOffsetCommitRequestTopic {
    pub fn with_name(mut self, value: KafkaString) -> Self {
        self.name = value;
        self
    }
    pub fn with_partitions(mut self, value: Vec<TxnOffsetCommitRequestPartition>) -> Self {
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
                    arr.push(TxnOffsetCommitRequestPartition::read(buf, version)?);
                }
                arr
            };
        } else {
            partitions = {
                let len = read_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(TxnOffsetCommitRequestPartition::read(buf, version)?);
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
pub struct TxnOffsetCommitRequestPartition {
    /// The index of the partition within the topic.
    pub partition_index: i32,
    /// The message offset to be committed.
    pub committed_offset: i64,
    /// The leader epoch of the last consumed record.
    pub committed_leader_epoch: i32,
    /// Any associated metadata the client wants to keep.
    pub committed_metadata: Option<KafkaString>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for TxnOffsetCommitRequestPartition {
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
impl TxnOffsetCommitRequestPartition {
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
        if version >= 2 {
            committed_leader_epoch = read_i32(buf)?;
        }
        if version >= 3 {
            committed_metadata = read_compact_nullable_string(buf)?;
        } else {
            committed_metadata = read_nullable_string(buf)?;
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
        if version >= 2 {
            write_i32(buf, self.committed_leader_epoch);
        } else if self.committed_leader_epoch != -1i32 {
            return Err(UnsupportedFieldVersion::new(28, "committed_leader_epoch", version).into());
        }
        if version >= 3 {
            write_compact_nullable_string(buf, self.committed_metadata.as_ref())?;
        } else {
            write_nullable_string(buf, self.committed_metadata.as_ref())?;
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
        len += 4;
        len += 8;
        if version >= 2 {
            len += 4;
        } else if self.committed_leader_epoch != -1i32 {
            return Err(UnsupportedFieldVersion::new(28, "committed_leader_epoch", version).into());
        }
        if version >= 3 {
            len += compact_nullable_string_len(self.committed_metadata.as_ref())?;
        } else {
            len += nullable_string_len(self.committed_metadata.as_ref())?;
        }
        if version >= 3 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            len += tagged_fields_len(&all_tags)?;
        }
        Ok(len)
    }
}
