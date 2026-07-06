//! Generated from WriteShareGroupStateRequest.json - DO NOT EDIT
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
pub struct WriteShareGroupStateRequestData {
    /// The group identifier.
    pub group_id: KafkaString,
    /// The data for the topics.
    pub topics: Vec<WriteStateData>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for WriteShareGroupStateRequestData {
    fn default() -> Self {
        Self {
            group_id: KafkaString::default(),
            topics: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl WriteShareGroupStateRequestData {
    pub fn with_group_id(mut self, value: KafkaString) -> Self {
        self.group_id = value;
        self
    }
    pub fn with_topics(mut self, value: Vec<WriteStateData>) -> Self {
        self.topics = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 0 || version > 1 {
            return Err(UnsupportedVersion::new(85, version).into());
        }
        let group_id;
        let topics;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        group_id = read_compact_string(buf)?;
        topics = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
            for _ in 0..len {
                arr.push(WriteStateData::read(buf, version)?);
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
            group_id,
            topics,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 0 || version > 1 {
            return Err(UnsupportedVersion::new(85, version).into());
        }
        write_compact_string(buf, &self.group_id)?;
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
        if version < 0 || version > 1 {
            return Err(UnsupportedVersion::new(85, version).into());
        }
        let mut len: usize = 0;
        len += compact_string_len(&self.group_id)?;
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
pub struct WriteStateData {
    /// The topic identifier.
    pub topic_id: KafkaUuid,
    /// The data for the partitions.
    pub partitions: Vec<PartitionData>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for WriteStateData {
    fn default() -> Self {
        Self {
            topic_id: KafkaUuid::ZERO,
            partitions: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl WriteStateData {
    pub fn with_topic_id(mut self, value: KafkaUuid) -> Self {
        self.topic_id = value;
        self
    }
    pub fn with_partitions(mut self, value: Vec<PartitionData>) -> Self {
        self.partitions = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let topic_id;
        let partitions;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        topic_id = read_uuid(buf)?;
        partitions = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
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
            topic_id,
            partitions,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
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
pub struct PartitionData {
    /// The partition index.
    pub partition: i32,
    /// The state epoch of the share-partition.
    pub state_epoch: i32,
    /// The leader epoch of the share-partition.
    pub leader_epoch: i32,
    /// The share-partition start offset, or -1 if the start offset is not being written.
    pub start_offset: i64,
    /// The number of offsets greater than or equal to share-partition start offset for which
    /// delivery has been completed.
    pub delivery_complete_count: i32,
    /// The state batches for the share-partition.
    pub state_batches: Vec<StateBatch>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for PartitionData {
    fn default() -> Self {
        Self {
            partition: 0_i32,
            state_epoch: 0_i32,
            leader_epoch: 0_i32,
            start_offset: 0_i64,
            delivery_complete_count: -1i32,
            state_batches: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl PartitionData {
    pub fn with_partition(mut self, value: i32) -> Self {
        self.partition = value;
        self
    }
    pub fn with_state_epoch(mut self, value: i32) -> Self {
        self.state_epoch = value;
        self
    }
    pub fn with_leader_epoch(mut self, value: i32) -> Self {
        self.leader_epoch = value;
        self
    }
    pub fn with_start_offset(mut self, value: i64) -> Self {
        self.start_offset = value;
        self
    }
    pub fn with_delivery_complete_count(mut self, value: i32) -> Self {
        self.delivery_complete_count = value;
        self
    }
    pub fn with_state_batches(mut self, value: Vec<StateBatch>) -> Self {
        self.state_batches = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let partition;
        let state_epoch;
        let leader_epoch;
        let start_offset;
        let mut delivery_complete_count = -1i32;
        let state_batches;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        partition = read_i32(buf)?;
        state_epoch = read_i32(buf)?;
        leader_epoch = read_i32(buf)?;
        start_offset = read_i64(buf)?;
        if version >= 1 {
            delivery_complete_count = read_i32(buf)?;
        }
        state_batches = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
            for _ in 0..len {
                arr.push(StateBatch::read(buf, version)?);
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
            partition,
            state_epoch,
            leader_epoch,
            start_offset,
            delivery_complete_count,
            state_batches,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        write_i32(buf, self.partition);
        write_i32(buf, self.state_epoch);
        write_i32(buf, self.leader_epoch);
        write_i64(buf, self.start_offset);
        if version >= 1 {
            write_i32(buf, self.delivery_complete_count);
        } else if self.delivery_complete_count != -1i32 {
            return Err(
                UnsupportedFieldVersion::new(85, "delivery_complete_count", version).into(),
            );
        }
        write_compact_array_length(buf, self.state_batches.len() as i32);
        for el in &self.state_batches {
            el.write(buf, version)?;
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
        len += 8;
        if version >= 1 {
            len += 4;
        } else if self.delivery_complete_count != -1i32 {
            return Err(
                UnsupportedFieldVersion::new(85, "delivery_complete_count", version).into(),
            );
        }
        len += compact_array_length_len(self.state_batches.len() as i32);
        for el in &self.state_batches {
            len += el.encoded_len(version)?;
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct StateBatch {
    /// The first offset of this state batch.
    pub first_offset: i64,
    /// The last offset of this state batch.
    pub last_offset: i64,
    /// The delivery state - 0:Available,2:Acked,4:Archived.
    pub delivery_state: i8,
    /// The delivery count.
    pub delivery_count: i16,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for StateBatch {
    fn default() -> Self {
        Self {
            first_offset: 0_i64,
            last_offset: 0_i64,
            delivery_state: 0_i8,
            delivery_count: 0_i16,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl StateBatch {
    pub fn with_first_offset(mut self, value: i64) -> Self {
        self.first_offset = value;
        self
    }
    pub fn with_last_offset(mut self, value: i64) -> Self {
        self.last_offset = value;
        self
    }
    pub fn with_delivery_state(mut self, value: i8) -> Self {
        self.delivery_state = value;
        self
    }
    pub fn with_delivery_count(mut self, value: i16) -> Self {
        self.delivery_count = value;
        self
    }
    pub fn read(buf: &mut Bytes, _version: i16) -> Result<Self> {
        let first_offset;
        let last_offset;
        let delivery_state;
        let delivery_count;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        first_offset = read_i64(buf)?;
        last_offset = read_i64(buf)?;
        delivery_state = read_i8(buf)?;
        delivery_count = read_i16(buf)?;
        let tagged_fields = read_tagged_fields(buf)?;
        for field in &tagged_fields {
            match field.tag {
                _ => {
                    _unknown_tagged_fields.push(field.clone());
                },
            }
        }
        Ok(Self {
            first_offset,
            last_offset,
            delivery_state,
            delivery_count,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, _version: i16) -> Result<()> {
        write_i64(buf, self.first_offset);
        write_i64(buf, self.last_offset);
        write_i8(buf, self.delivery_state);
        write_i16(buf, self.delivery_count);
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
    pub fn encoded_len(&self, _version: i16) -> Result<usize> {
        let mut len: usize = 0;
        len += 8;
        len += 8;
        len += 1;
        len += 2;
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
