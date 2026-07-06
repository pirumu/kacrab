//! Generated from OffsetForLeaderEpochRequest.json - DO NOT EDIT
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
pub struct OffsetForLeaderEpochRequestData {
    /// The broker ID of the follower, of -1 if this request is from a consumer.
    pub replica_id: i32,
    /// Each topic to get offsets for.
    pub topics: Vec<OffsetForLeaderTopic>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for OffsetForLeaderEpochRequestData {
    fn default() -> Self {
        Self {
            replica_id: -2i32,
            topics: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl OffsetForLeaderEpochRequestData {
    pub fn with_replica_id(mut self, value: i32) -> Self {
        self.replica_id = value;
        self
    }
    pub fn with_topics(mut self, value: Vec<OffsetForLeaderTopic>) -> Self {
        self.topics = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 2 || version > 4 {
            return Err(UnsupportedVersion::new(23, version).into());
        }
        let mut replica_id = -2i32;
        let topics;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 3 {
            replica_id = read_i32(buf)?;
        }
        if version >= 4 {
            topics = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
                for _ in 0..len {
                    arr.push(OffsetForLeaderTopic::read(buf, version)?);
                }
                arr
            };
        } else {
            topics = {
                let len = read_array_length(buf)?;
                let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
                for _ in 0..len {
                    arr.push(OffsetForLeaderTopic::read(buf, version)?);
                }
                arr
            };
        }
        if version >= 4 {
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
            replica_id,
            topics,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 2 || version > 4 {
            return Err(UnsupportedVersion::new(23, version).into());
        }
        if version >= 3 {
            write_i32(buf, self.replica_id);
        } else if self.replica_id != -2i32 {
            return Err(UnsupportedFieldVersion::new(23, "replica_id", version).into());
        }
        if version >= 4 {
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
        if version >= 4 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        if version < 2 || version > 4 {
            return Err(UnsupportedVersion::new(23, version).into());
        }
        let mut len: usize = 0;
        if version >= 3 {
            len += 4;
        } else if self.replica_id != -2i32 {
            return Err(UnsupportedFieldVersion::new(23, "replica_id", version).into());
        }
        if version >= 4 {
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
        if version >= 4 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            len += tagged_fields_len(&all_tags)?;
        }
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct OffsetForLeaderTopic {
    /// The topic name.
    pub topic: KafkaString,
    /// Each partition to get offsets for.
    pub partitions: Vec<OffsetForLeaderPartition>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for OffsetForLeaderTopic {
    fn default() -> Self {
        Self {
            topic: KafkaString::default(),
            partitions: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl OffsetForLeaderTopic {
    pub fn with_topic(mut self, value: KafkaString) -> Self {
        self.topic = value;
        self
    }
    pub fn with_partitions(mut self, value: Vec<OffsetForLeaderPartition>) -> Self {
        self.partitions = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let topic;
        let partitions;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 4 {
            topic = read_compact_string(buf)?;
        } else {
            topic = read_string(buf)?;
        }
        if version >= 4 {
            partitions = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
                for _ in 0..len {
                    arr.push(OffsetForLeaderPartition::read(buf, version)?);
                }
                arr
            };
        } else {
            partitions = {
                let len = read_array_length(buf)?;
                let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
                for _ in 0..len {
                    arr.push(OffsetForLeaderPartition::read(buf, version)?);
                }
                arr
            };
        }
        if version >= 4 {
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
        if version >= 4 {
            write_compact_string(buf, &self.topic)?;
        } else {
            write_string(buf, &self.topic)?;
        }
        if version >= 4 {
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
        if version >= 4 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        let mut len: usize = 0;
        if version >= 4 {
            len += compact_string_len(&self.topic)?;
        } else {
            len += string_len(&self.topic)?;
        }
        if version >= 4 {
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
        if version >= 4 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            len += tagged_fields_len(&all_tags)?;
        }
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct OffsetForLeaderPartition {
    /// The partition index.
    pub partition: i32,
    /// An epoch used to fence consumers/replicas with old metadata. If the epoch provided by the
    /// client is larger than the current epoch known to the broker, then the UNKNOWN_LEADER_EPOCH
    /// error code will be returned. If the provided epoch is smaller, then the FENCED_LEADER_EPOCH
    /// error code will be returned.
    pub current_leader_epoch: i32,
    /// The epoch to look up an offset for.
    pub leader_epoch: i32,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for OffsetForLeaderPartition {
    fn default() -> Self {
        Self {
            partition: 0_i32,
            current_leader_epoch: -1i32,
            leader_epoch: 0_i32,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl OffsetForLeaderPartition {
    pub fn with_partition(mut self, value: i32) -> Self {
        self.partition = value;
        self
    }
    pub fn with_current_leader_epoch(mut self, value: i32) -> Self {
        self.current_leader_epoch = value;
        self
    }
    pub fn with_leader_epoch(mut self, value: i32) -> Self {
        self.leader_epoch = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let partition;
        let current_leader_epoch;
        let leader_epoch;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        partition = read_i32(buf)?;
        current_leader_epoch = read_i32(buf)?;
        leader_epoch = read_i32(buf)?;
        if version >= 4 {
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
            partition,
            current_leader_epoch,
            leader_epoch,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        write_i32(buf, self.partition);
        write_i32(buf, self.current_leader_epoch);
        write_i32(buf, self.leader_epoch);
        if version >= 4 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        let mut len: usize = 0;
        len += 4;
        len += 4;
        len += 4;
        if version >= 4 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            len += tagged_fields_len(&all_tags)?;
        }
        Ok(len)
    }
}
