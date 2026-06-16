//! Generated from VoteRequest.json - DO NOT EDIT
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
        }
        if version >= 1 {
            write_uuid(buf, &self.voter_directory_id);
        }
        write_i32(buf, self.last_offset_epoch);
        write_i64(buf, self.last_offset);
        if version >= 2 {
            write_bool(buf, self.pre_vote);
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
}
