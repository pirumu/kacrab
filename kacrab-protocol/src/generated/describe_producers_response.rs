//! Generated from DescribeProducersResponse.json - DO NOT EDIT
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
pub struct DescribeProducersResponseData {
    /// The duration in milliseconds for which the request was throttled due to a quota violation,
    /// or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// Each topic in the response.
    pub topics: Vec<TopicResponse>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for DescribeProducersResponseData {
    fn default() -> Self {
        Self {
            throttle_time_ms: 0_i32,
            topics: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl DescribeProducersResponseData {
    pub fn with_throttle_time_ms(mut self, value: i32) -> Self {
        self.throttle_time_ms = value;
        self
    }
    pub fn with_topics(mut self, value: Vec<TopicResponse>) -> Self {
        self.topics = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 0 || version > 0 {
            return Err(UnsupportedVersion::new(61, version).into());
        }
        let throttle_time_ms;
        let topics;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        throttle_time_ms = read_i32(buf)?;
        topics = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
            for _ in 0..len {
                arr.push(TopicResponse::read(buf, version)?);
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
            topics,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 0 || version > 0 {
            return Err(UnsupportedVersion::new(61, version).into());
        }
        write_i32(buf, self.throttle_time_ms);
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
        if version < 0 || version > 0 {
            return Err(UnsupportedVersion::new(61, version).into());
        }
        let mut len: usize = 0;
        len += 4;
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
pub struct TopicResponse {
    /// The topic name.
    pub name: KafkaString,
    /// Each partition in the response.
    pub partitions: Vec<PartitionResponse>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for TopicResponse {
    fn default() -> Self {
        Self {
            name: KafkaString::default(),
            partitions: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl TopicResponse {
    pub fn with_name(mut self, value: KafkaString) -> Self {
        self.name = value;
        self
    }
    pub fn with_partitions(mut self, value: Vec<PartitionResponse>) -> Self {
        self.partitions = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let name;
        let partitions;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        name = read_compact_string(buf)?;
        partitions = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
            for _ in 0..len {
                arr.push(PartitionResponse::read(buf, version)?);
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
            partitions,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        write_compact_string(buf, &self.name)?;
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
        len += compact_string_len(&self.name)?;
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
pub struct PartitionResponse {
    /// The partition index.
    pub partition_index: i32,
    /// The partition error code, or 0 if there was no error.
    pub error_code: i16,
    /// The partition error message, which may be null if no additional details are available.
    pub error_message: Option<KafkaString>,
    /// The active producers for the partition.
    pub active_producers: Vec<ProducerState>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for PartitionResponse {
    fn default() -> Self {
        Self {
            partition_index: 0_i32,
            error_code: 0_i16,
            error_message: None,
            active_producers: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl PartitionResponse {
    pub fn with_partition_index(mut self, value: i32) -> Self {
        self.partition_index = value;
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
    pub fn with_active_producers(mut self, value: Vec<ProducerState>) -> Self {
        self.active_producers = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let partition_index;
        let error_code;
        let error_message;
        let active_producers;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        partition_index = read_i32(buf)?;
        error_code = read_i16(buf)?;
        error_message = read_compact_nullable_string(buf)?;
        active_producers = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
            for _ in 0..len {
                arr.push(ProducerState::read(buf, version)?);
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
            partition_index,
            error_code,
            error_message,
            active_producers,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        write_i32(buf, self.partition_index);
        write_i16(buf, self.error_code);
        write_compact_nullable_string(buf, self.error_message.as_ref())?;
        write_compact_array_length(buf, self.active_producers.len() as i32);
        for el in &self.active_producers {
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
        len += 2;
        len += compact_nullable_string_len(self.error_message.as_ref())?;
        len += compact_array_length_len(self.active_producers.len() as i32);
        for el in &self.active_producers {
            len += el.encoded_len(version)?;
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct ProducerState {
    /// The producer id.
    pub producer_id: i64,
    /// The producer epoch.
    pub producer_epoch: i32,
    /// The last sequence number sent by the producer.
    pub last_sequence: i32,
    /// The last timestamp sent by the producer.
    pub last_timestamp: i64,
    /// The current epoch of the producer group.
    pub coordinator_epoch: i32,
    /// The current transaction start offset of the producer.
    pub current_txn_start_offset: i64,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for ProducerState {
    fn default() -> Self {
        Self {
            producer_id: 0_i64,
            producer_epoch: 0_i32,
            last_sequence: -1i32,
            last_timestamp: -1i64,
            coordinator_epoch: 0_i32,
            current_txn_start_offset: -1i64,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl ProducerState {
    pub fn with_producer_id(mut self, value: i64) -> Self {
        self.producer_id = value;
        self
    }
    pub fn with_producer_epoch(mut self, value: i32) -> Self {
        self.producer_epoch = value;
        self
    }
    pub fn with_last_sequence(mut self, value: i32) -> Self {
        self.last_sequence = value;
        self
    }
    pub fn with_last_timestamp(mut self, value: i64) -> Self {
        self.last_timestamp = value;
        self
    }
    pub fn with_coordinator_epoch(mut self, value: i32) -> Self {
        self.coordinator_epoch = value;
        self
    }
    pub fn with_current_txn_start_offset(mut self, value: i64) -> Self {
        self.current_txn_start_offset = value;
        self
    }
    pub fn read(buf: &mut Bytes, _version: i16) -> Result<Self> {
        let producer_id;
        let producer_epoch;
        let last_sequence;
        let last_timestamp;
        let coordinator_epoch;
        let current_txn_start_offset;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        producer_id = read_i64(buf)?;
        producer_epoch = read_i32(buf)?;
        last_sequence = read_i32(buf)?;
        last_timestamp = read_i64(buf)?;
        coordinator_epoch = read_i32(buf)?;
        current_txn_start_offset = read_i64(buf)?;
        let tagged_fields = read_tagged_fields(buf)?;
        for field in &tagged_fields {
            match field.tag {
                _ => {
                    _unknown_tagged_fields.push(field.clone());
                },
            }
        }
        Ok(Self {
            producer_id,
            producer_epoch,
            last_sequence,
            last_timestamp,
            coordinator_epoch,
            current_txn_start_offset,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, _version: i16) -> Result<()> {
        write_i64(buf, self.producer_id);
        write_i32(buf, self.producer_epoch);
        write_i32(buf, self.last_sequence);
        write_i64(buf, self.last_timestamp);
        write_i32(buf, self.coordinator_epoch);
        write_i64(buf, self.current_txn_start_offset);
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
    pub fn encoded_len(&self, _version: i16) -> Result<usize> {
        let mut len: usize = 0;
        len += 8;
        len += 4;
        len += 4;
        len += 8;
        len += 4;
        len += 8;
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
