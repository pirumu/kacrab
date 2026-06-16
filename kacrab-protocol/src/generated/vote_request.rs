//! Generated from VoteRequest.json - DO NOT EDIT
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
pub struct VoteRequestData {
    /// The cluster id.
    pub cluster_id: Option<KafkaString>,
    /// The replica id of the voter receiving the request.
    pub voter_id: i32,
    /// The topic data.
    pub topics: Vec<TopicData>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for VoteRequestData {
    fn default() -> Self {
        Self {
            cluster_id: None,
            voter_id: -1i32,
            topics: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl VoteRequestData {
    pub fn with_cluster_id(mut self, value: Option<KafkaString>) -> Self {
        self.cluster_id = value;
        self
    }
    pub fn with_voter_id(mut self, value: i32) -> Self {
        self.voter_id = value;
        self
    }
    pub fn with_topics(mut self, value: Vec<TopicData>) -> Self {
        self.topics = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 0 || version > 2 {
            return Err(UnsupportedVersion::new(52, version).into());
        }
        let cluster_id;
        let mut voter_id = -1i32;
        let topics;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        cluster_id = read_compact_nullable_string(buf)?;
        if version >= 1 {
            voter_id = read_i32(buf)?;
        }
        topics = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(len.max(0) as usize);
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
            cluster_id,
            voter_id,
            topics,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 0 || version > 2 {
            return Err(UnsupportedVersion::new(52, version).into());
        }
        write_compact_nullable_string(buf, self.cluster_id.as_ref())?;
        if version >= 1 {
            write_i32(buf, self.voter_id);
        } else if self.voter_id != -1i32 {
            return Err(UnsupportedFieldVersion::new(52, "voter_id", version).into());
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
            return Err(UnsupportedVersion::new(52, version).into());
        }
        let mut len: usize = 0;
        len += compact_nullable_string_len(self.cluster_id.as_ref())?;
        if version >= 1 {
            len += 4;
        } else if self.voter_id != -1i32 {
            return Err(UnsupportedFieldVersion::new(52, "voter_id", version).into());
        }
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
    /// The partition data.
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
            let mut arr = Vec::with_capacity(len.max(0) as usize);
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
    /// The epoch of the voter sending the request
    pub replica_epoch: i32,
    /// The replica id of the voter sending the request
    pub replica_id: i32,
    /// The directory id of the voter sending the request
    pub replica_directory_id: KafkaUuid,
    /// The directory id of the voter receiving the request
    pub voter_directory_id: KafkaUuid,
    /// The epoch of the last record written to the metadata log.
    pub last_offset_epoch: i32,
    /// The log end offset of the metadata log of the voter sending the request.
    pub last_offset: i64,
    /// Whether the request is a PreVote request (not persisted) or not.
    pub pre_vote: bool,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for PartitionData {
    fn default() -> Self {
        Self {
            partition_index: 0_i32,
            replica_epoch: 0_i32,
            replica_id: 0_i32,
            replica_directory_id: KafkaUuid::ZERO,
            voter_directory_id: KafkaUuid::ZERO,
            last_offset_epoch: 0_i32,
            last_offset: 0_i64,
            pre_vote: false,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl PartitionData {
    pub fn with_partition_index(mut self, value: i32) -> Self {
        self.partition_index = value;
        self
    }
    pub fn with_replica_epoch(mut self, value: i32) -> Self {
        self.replica_epoch = value;
        self
    }
    pub fn with_replica_id(mut self, value: i32) -> Self {
        self.replica_id = value;
        self
    }
    pub fn with_replica_directory_id(mut self, value: KafkaUuid) -> Self {
        self.replica_directory_id = value;
        self
    }
    pub fn with_voter_directory_id(mut self, value: KafkaUuid) -> Self {
        self.voter_directory_id = value;
        self
    }
    pub fn with_last_offset_epoch(mut self, value: i32) -> Self {
        self.last_offset_epoch = value;
        self
    }
    pub fn with_last_offset(mut self, value: i64) -> Self {
        self.last_offset = value;
        self
    }
    pub fn with_pre_vote(mut self, value: bool) -> Self {
        self.pre_vote = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let partition_index;
        let replica_epoch;
        let replica_id;
        let mut replica_directory_id = KafkaUuid::ZERO;
        let mut voter_directory_id = KafkaUuid::ZERO;
        let last_offset_epoch;
        let last_offset;
        let mut pre_vote = false;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        partition_index = read_i32(buf)?;
        replica_epoch = read_i32(buf)?;
        replica_id = read_i32(buf)?;
        if version >= 1 {
            replica_directory_id = read_uuid(buf)?;
        }
        if version >= 1 {
            voter_directory_id = read_uuid(buf)?;
        }
        last_offset_epoch = read_i32(buf)?;
        last_offset = read_i64(buf)?;
        if version >= 2 {
            pre_vote = read_bool(buf)?;
        }
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
            replica_epoch,
            replica_id,
            replica_directory_id,
            voter_directory_id,
            last_offset_epoch,
            last_offset,
            pre_vote,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        write_i32(buf, self.partition_index);
        write_i32(buf, self.replica_epoch);
        write_i32(buf, self.replica_id);
        if version >= 1 {
            write_uuid(buf, &self.replica_directory_id);
        } else if self.replica_directory_id != KafkaUuid::ZERO {
            return Err(UnsupportedFieldVersion::new(52, "replica_directory_id", version).into());
        }
        if version >= 1 {
            write_uuid(buf, &self.voter_directory_id);
        } else if self.voter_directory_id != KafkaUuid::ZERO {
            return Err(UnsupportedFieldVersion::new(52, "voter_directory_id", version).into());
        }
        write_i32(buf, self.last_offset_epoch);
        write_i64(buf, self.last_offset);
        if version >= 2 {
            write_bool(buf, self.pre_vote);
        } else if self.pre_vote != false {
            return Err(UnsupportedFieldVersion::new(52, "pre_vote", version).into());
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        let mut len: usize = 0;
        len += 4;
        len += 4;
        len += 4;
        if version >= 1 {
            len += 16;
        } else if self.replica_directory_id != KafkaUuid::ZERO {
            return Err(UnsupportedFieldVersion::new(52, "replica_directory_id", version).into());
        }
        if version >= 1 {
            len += 16;
        } else if self.voter_directory_id != KafkaUuid::ZERO {
            return Err(UnsupportedFieldVersion::new(52, "voter_directory_id", version).into());
        }
        len += 4;
        len += 8;
        if version >= 2 {
            len += 1;
        } else if self.pre_vote != false {
            return Err(UnsupportedFieldVersion::new(52, "pre_vote", version).into());
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
