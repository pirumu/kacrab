//! Generated from AlterPartitionRequest.json - DO NOT EDIT
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
pub struct AlterPartitionRequestData {
    /// The ID of the requesting broker.
    pub broker_id: i32,
    /// The epoch of the requesting broker.
    pub broker_epoch: i64,
    /// The topics to alter ISRs for.
    pub topics: Vec<TopicData>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for AlterPartitionRequestData {
    fn default() -> Self {
        Self {
            broker_id: 0_i32,
            broker_epoch: -1i64,
            topics: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl AlterPartitionRequestData {
    pub fn with_broker_id(mut self, value: i32) -> Self {
        self.broker_id = value;
        self
    }
    pub fn with_broker_epoch(mut self, value: i64) -> Self {
        self.broker_epoch = value;
        self
    }
    pub fn with_topics(mut self, value: Vec<TopicData>) -> Self {
        self.topics = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 2 || version > 3 {
            return Err(UnsupportedVersion::new(56, version).into());
        }
        let broker_id;
        let broker_epoch;
        let topics;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        broker_id = read_i32(buf)?;
        broker_epoch = read_i64(buf)?;
        topics = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
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
            broker_id,
            broker_epoch,
            topics,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 2 || version > 3 {
            return Err(UnsupportedVersion::new(56, version).into());
        }
        write_i32(buf, self.broker_id);
        write_i64(buf, self.broker_epoch);
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
        if version < 2 || version > 3 {
            return Err(UnsupportedVersion::new(56, version).into());
        }
        let mut len: usize = 0;
        len += 4;
        len += 8;
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
    /// The ID of the topic to alter ISRs for.
    pub topic_id: KafkaUuid,
    /// The partitions to alter ISRs for.
    pub partitions: Vec<PartitionData>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for TopicData {
    fn default() -> Self {
        Self {
            topic_id: KafkaUuid::ZERO,
            partitions: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl TopicData {
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
    pub partition_index: i32,
    /// The leader epoch of this partition.
    pub leader_epoch: i32,
    /// The ISR for this partition. Deprecated since version 3.
    pub new_isr: Vec<i32>,
    /// The ISR for this partition.
    pub new_isr_with_epochs: Vec<BrokerState>,
    /// 1 if the partition is recovering from an unclean leader election; 0 otherwise.
    pub leader_recovery_state: i8,
    /// The expected epoch of the partition which is being updated.
    pub partition_epoch: i32,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for PartitionData {
    fn default() -> Self {
        Self {
            partition_index: 0_i32,
            leader_epoch: 0_i32,
            new_isr: Vec::new(),
            new_isr_with_epochs: Vec::new(),
            leader_recovery_state: 0i8,
            partition_epoch: 0_i32,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl PartitionData {
    pub fn with_partition_index(mut self, value: i32) -> Self {
        self.partition_index = value;
        self
    }
    pub fn with_leader_epoch(mut self, value: i32) -> Self {
        self.leader_epoch = value;
        self
    }
    pub fn with_new_isr(mut self, value: Vec<i32>) -> Self {
        self.new_isr = value;
        self
    }
    pub fn with_new_isr_with_epochs(mut self, value: Vec<BrokerState>) -> Self {
        self.new_isr_with_epochs = value;
        self
    }
    pub fn with_leader_recovery_state(mut self, value: i8) -> Self {
        self.leader_recovery_state = value;
        self
    }
    pub fn with_partition_epoch(mut self, value: i32) -> Self {
        self.partition_epoch = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let partition_index;
        let leader_epoch;
        let mut new_isr = Vec::new();
        let mut new_isr_with_epochs = Vec::new();
        let leader_recovery_state;
        let partition_epoch;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        partition_index = read_i32(buf)?;
        leader_epoch = read_i32(buf)?;
        if version <= 2 {
            new_isr = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
                for _ in 0..len {
                    arr.push(read_i32(buf)?);
                }
                arr
            };
        }
        if version >= 3 {
            new_isr_with_epochs = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
                for _ in 0..len {
                    arr.push(BrokerState::read(buf, version)?);
                }
                arr
            };
        }
        leader_recovery_state = read_i8(buf)?;
        partition_epoch = read_i32(buf)?;
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
            leader_epoch,
            new_isr,
            new_isr_with_epochs,
            leader_recovery_state,
            partition_epoch,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        write_i32(buf, self.partition_index);
        write_i32(buf, self.leader_epoch);
        if version <= 2 {
            write_compact_array_length(buf, self.new_isr.len() as i32);
            for el in &self.new_isr {
                write_i32(buf, *el);
            }
        } else if self.new_isr != Vec::new() {
            return Err(UnsupportedFieldVersion::new(56, "new_isr", version).into());
        }
        if version >= 3 {
            write_compact_array_length(buf, self.new_isr_with_epochs.len() as i32);
            for el in &self.new_isr_with_epochs {
                el.write(buf, version)?;
            }
        } else if self.new_isr_with_epochs != Vec::new() {
            return Err(UnsupportedFieldVersion::new(56, "new_isr_with_epochs", version).into());
        }
        write_i8(buf, self.leader_recovery_state);
        write_i32(buf, self.partition_epoch);
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        let mut len: usize = 0;
        len += 4;
        len += 4;
        if version <= 2 {
            len += compact_array_length_len(self.new_isr.len() as i32);
            len += self.new_isr.len() * 4usize;
        } else if self.new_isr != Vec::new() {
            return Err(UnsupportedFieldVersion::new(56, "new_isr", version).into());
        }
        if version >= 3 {
            len += compact_array_length_len(self.new_isr_with_epochs.len() as i32);
            for el in &self.new_isr_with_epochs {
                len += el.encoded_len(version)?;
            }
        } else if self.new_isr_with_epochs != Vec::new() {
            return Err(UnsupportedFieldVersion::new(56, "new_isr_with_epochs", version).into());
        }
        len += 1;
        len += 4;
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct BrokerState {
    /// The ID of the broker.
    pub broker_id: i32,
    /// The epoch of the broker. It will be -1 if the epoch check is not supported.
    pub broker_epoch: i64,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for BrokerState {
    fn default() -> Self {
        Self {
            broker_id: 0_i32,
            broker_epoch: -1i64,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl BrokerState {
    pub fn with_broker_id(mut self, value: i32) -> Self {
        self.broker_id = value;
        self
    }
    pub fn with_broker_epoch(mut self, value: i64) -> Self {
        self.broker_epoch = value;
        self
    }
    pub fn read(buf: &mut Bytes, _version: i16) -> Result<Self> {
        let broker_id;
        let broker_epoch;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        broker_id = read_i32(buf)?;
        broker_epoch = read_i64(buf)?;
        let tagged_fields = read_tagged_fields(buf)?;
        for field in &tagged_fields {
            match field.tag {
                _ => {
                    _unknown_tagged_fields.push(field.clone());
                },
            }
        }
        Ok(Self {
            broker_id,
            broker_epoch,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, _version: i16) -> Result<()> {
        write_i32(buf, self.broker_id);
        write_i64(buf, self.broker_epoch);
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
    pub fn encoded_len(&self, _version: i16) -> Result<usize> {
        let mut len: usize = 0;
        len += 4;
        len += 8;
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
