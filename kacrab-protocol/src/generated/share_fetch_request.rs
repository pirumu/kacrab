//! Generated from ShareFetchRequest.json - DO NOT EDIT
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
pub struct ShareFetchRequestData {
    /// The group identifier.
    pub group_id: Option<KafkaString>,
    /// The member ID.
    pub member_id: Option<KafkaString>,
    /// The current share session epoch: 0 to open a share session; -1 to close it; otherwise
    /// increments for consecutive requests.
    pub share_session_epoch: i32,
    /// The maximum time in milliseconds to wait for the response.
    pub max_wait_ms: i32,
    /// The minimum bytes to accumulate in the response.
    pub min_bytes: i32,
    /// The maximum bytes to fetch. See KIP-74 for cases where this limit may not be honored.
    pub max_bytes: i32,
    /// The maximum number of records to fetch. This limit can be exceeded for alignment of batch
    /// boundaries.
    pub max_records: i32,
    /// The optimal number of records for batches of acquired records and acknowledgements.
    pub batch_size: i32,
    /// The acquire mode to control the fetch behavior - 0:batch-optimized,1:record-limit.
    pub share_acquire_mode: i8,
    /// Whether Renew type acknowledgements present in AcknowledgementBatches.
    pub is_renew_ack: bool,
    /// The topics to fetch.
    pub topics: Vec<FetchTopic>,
    /// The partitions to remove from this share session.
    pub forgotten_topics_data: Vec<ForgottenTopic>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for ShareFetchRequestData {
    fn default() -> Self {
        Self {
            group_id: None,
            member_id: None,
            share_session_epoch: 0_i32,
            max_wait_ms: 0_i32,
            min_bytes: 0_i32,
            max_bytes: i32::MAX,
            max_records: 0_i32,
            batch_size: 0_i32,
            share_acquire_mode: 0i8,
            is_renew_ack: false,
            topics: Vec::new(),
            forgotten_topics_data: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl ShareFetchRequestData {
    pub fn with_group_id(mut self, value: Option<KafkaString>) -> Self {
        self.group_id = value;
        self
    }
    pub fn with_member_id(mut self, value: Option<KafkaString>) -> Self {
        self.member_id = value;
        self
    }
    pub fn with_share_session_epoch(mut self, value: i32) -> Self {
        self.share_session_epoch = value;
        self
    }
    pub fn with_max_wait_ms(mut self, value: i32) -> Self {
        self.max_wait_ms = value;
        self
    }
    pub fn with_min_bytes(mut self, value: i32) -> Self {
        self.min_bytes = value;
        self
    }
    pub fn with_max_bytes(mut self, value: i32) -> Self {
        self.max_bytes = value;
        self
    }
    pub fn with_max_records(mut self, value: i32) -> Self {
        self.max_records = value;
        self
    }
    pub fn with_batch_size(mut self, value: i32) -> Self {
        self.batch_size = value;
        self
    }
    pub fn with_share_acquire_mode(mut self, value: i8) -> Self {
        self.share_acquire_mode = value;
        self
    }
    pub fn with_is_renew_ack(mut self, value: bool) -> Self {
        self.is_renew_ack = value;
        self
    }
    pub fn with_topics(mut self, value: Vec<FetchTopic>) -> Self {
        self.topics = value;
        self
    }
    pub fn with_forgotten_topics_data(mut self, value: Vec<ForgottenTopic>) -> Self {
        self.forgotten_topics_data = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 1 || version > 2 {
            return Err(UnsupportedVersion::new(78, version).into());
        }
        let group_id;
        let member_id;
        let share_session_epoch;
        let max_wait_ms;
        let min_bytes;
        let max_bytes;
        let max_records;
        let batch_size;
        let mut share_acquire_mode = 0i8;
        let mut is_renew_ack = false;
        let topics;
        let forgotten_topics_data;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        group_id = read_compact_nullable_string(buf)?;
        member_id = read_compact_nullable_string(buf)?;
        share_session_epoch = read_i32(buf)?;
        max_wait_ms = read_i32(buf)?;
        min_bytes = read_i32(buf)?;
        max_bytes = read_i32(buf)?;
        max_records = read_i32(buf)?;
        batch_size = read_i32(buf)?;
        if version >= 2 {
            share_acquire_mode = read_i8(buf)?;
        }
        if version >= 2 {
            is_renew_ack = read_bool(buf)?;
        }
        topics = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(len.max(0) as usize);
            for _ in 0..len {
                arr.push(FetchTopic::read(buf, version)?);
            }
            arr
        };
        forgotten_topics_data = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(len.max(0) as usize);
            for _ in 0..len {
                arr.push(ForgottenTopic::read(buf, version)?);
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
            member_id,
            share_session_epoch,
            max_wait_ms,
            min_bytes,
            max_bytes,
            max_records,
            batch_size,
            share_acquire_mode,
            is_renew_ack,
            topics,
            forgotten_topics_data,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 1 || version > 2 {
            return Err(UnsupportedVersion::new(78, version).into());
        }
        write_compact_nullable_string(buf, self.group_id.as_ref())?;
        write_compact_nullable_string(buf, self.member_id.as_ref())?;
        write_i32(buf, self.share_session_epoch);
        write_i32(buf, self.max_wait_ms);
        write_i32(buf, self.min_bytes);
        write_i32(buf, self.max_bytes);
        write_i32(buf, self.max_records);
        write_i32(buf, self.batch_size);
        if version >= 2 {
            write_i8(buf, self.share_acquire_mode);
        } else if self.share_acquire_mode != 0i8 {
            return Err(UnsupportedFieldVersion::new(78, "share_acquire_mode", version).into());
        }
        if version >= 2 {
            write_bool(buf, self.is_renew_ack);
        } else if self.is_renew_ack != false {
            return Err(UnsupportedFieldVersion::new(78, "is_renew_ack", version).into());
        }
        write_compact_array_length(buf, self.topics.len() as i32);
        for el in &self.topics {
            el.write(buf, version)?;
        }
        write_compact_array_length(buf, self.forgotten_topics_data.len() as i32);
        for el in &self.forgotten_topics_data {
            el.write(buf, version)?;
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        if version < 1 || version > 2 {
            return Err(UnsupportedVersion::new(78, version).into());
        }
        let mut len: usize = 0;
        len += compact_nullable_string_len(self.group_id.as_ref())?;
        len += compact_nullable_string_len(self.member_id.as_ref())?;
        len += 4;
        len += 4;
        len += 4;
        len += 4;
        len += 4;
        len += 4;
        if version >= 2 {
            len += 1;
        } else if self.share_acquire_mode != 0i8 {
            return Err(UnsupportedFieldVersion::new(78, "share_acquire_mode", version).into());
        }
        if version >= 2 {
            len += 1;
        } else if self.is_renew_ack != false {
            return Err(UnsupportedFieldVersion::new(78, "is_renew_ack", version).into());
        }
        len += compact_array_length_len(self.topics.len() as i32);
        for el in &self.topics {
            len += el.encoded_len(version)?;
        }
        len += compact_array_length_len(self.forgotten_topics_data.len() as i32);
        for el in &self.forgotten_topics_data {
            len += el.encoded_len(version)?;
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct FetchTopic {
    /// The unique topic ID.
    pub topic_id: KafkaUuid,
    /// The partitions to fetch.
    pub partitions: Vec<FetchPartition>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for FetchTopic {
    fn default() -> Self {
        Self {
            topic_id: KafkaUuid::ZERO,
            partitions: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl FetchTopic {
    pub fn with_topic_id(mut self, value: KafkaUuid) -> Self {
        self.topic_id = value;
        self
    }
    pub fn with_partitions(mut self, value: Vec<FetchPartition>) -> Self {
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
            let mut arr = Vec::with_capacity(len.max(0) as usize);
            for _ in 0..len {
                arr.push(FetchPartition::read(buf, version)?);
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
pub struct FetchPartition {
    /// The partition index.
    pub partition_index: i32,
    /// The maximum bytes to fetch from this partition. 0 when only acknowledgement with no
    /// fetching is required. See KIP-74 for cases where this limit may not be honored.
    pub partition_max_bytes: i32,
    /// Record batches to acknowledge.
    pub acknowledgement_batches: Vec<AcknowledgementBatch>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for FetchPartition {
    fn default() -> Self {
        Self {
            partition_index: 0_i32,
            partition_max_bytes: 0_i32,
            acknowledgement_batches: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl FetchPartition {
    pub fn with_partition_index(mut self, value: i32) -> Self {
        self.partition_index = value;
        self
    }
    pub fn with_partition_max_bytes(mut self, value: i32) -> Self {
        self.partition_max_bytes = value;
        self
    }
    pub fn with_acknowledgement_batches(mut self, value: Vec<AcknowledgementBatch>) -> Self {
        self.acknowledgement_batches = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let partition_index;
        let partition_max_bytes = 0_i32;
        let acknowledgement_batches;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        partition_index = read_i32(buf)?;
        acknowledgement_batches = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(len.max(0) as usize);
            for _ in 0..len {
                arr.push(AcknowledgementBatch::read(buf, version)?);
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
            partition_max_bytes,
            acknowledgement_batches,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        write_i32(buf, self.partition_index);
        write_compact_array_length(buf, self.acknowledgement_batches.len() as i32);
        for el in &self.acknowledgement_batches {
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
        len += compact_array_length_len(self.acknowledgement_batches.len() as i32);
        for el in &self.acknowledgement_batches {
            len += el.encoded_len(version)?;
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct AcknowledgementBatch {
    /// First offset of batch of records to acknowledge.
    pub first_offset: i64,
    /// Last offset (inclusive) of batch of records to acknowledge.
    pub last_offset: i64,
    /// Array of acknowledge types - 0:Gap,1:Accept,2:Release,3:Reject,4:Renew.
    pub acknowledge_types: Vec<i8>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for AcknowledgementBatch {
    fn default() -> Self {
        Self {
            first_offset: 0_i64,
            last_offset: 0_i64,
            acknowledge_types: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl AcknowledgementBatch {
    pub fn with_first_offset(mut self, value: i64) -> Self {
        self.first_offset = value;
        self
    }
    pub fn with_last_offset(mut self, value: i64) -> Self {
        self.last_offset = value;
        self
    }
    pub fn with_acknowledge_types(mut self, value: Vec<i8>) -> Self {
        self.acknowledge_types = value;
        self
    }
    pub fn read(buf: &mut Bytes, _version: i16) -> Result<Self> {
        let first_offset;
        let last_offset;
        let acknowledge_types;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        first_offset = read_i64(buf)?;
        last_offset = read_i64(buf)?;
        acknowledge_types = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(len.max(0) as usize);
            for _ in 0..len {
                arr.push(read_i8(buf)?);
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
            first_offset,
            last_offset,
            acknowledge_types,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, _version: i16) -> Result<()> {
        write_i64(buf, self.first_offset);
        write_i64(buf, self.last_offset);
        write_compact_array_length(buf, self.acknowledge_types.len() as i32);
        for el in &self.acknowledge_types {
            write_i8(buf, *el);
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
    pub fn encoded_len(&self, _version: i16) -> Result<usize> {
        let mut len: usize = 0;
        len += 8;
        len += 8;
        len += compact_array_length_len(self.acknowledge_types.len() as i32);
        len += self.acknowledge_types.len() * 1usize;
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct ForgottenTopic {
    /// The unique topic ID.
    pub topic_id: KafkaUuid,
    /// The partitions indexes to forget.
    pub partitions: Vec<i32>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for ForgottenTopic {
    fn default() -> Self {
        Self {
            topic_id: KafkaUuid::ZERO,
            partitions: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl ForgottenTopic {
    pub fn with_topic_id(mut self, value: KafkaUuid) -> Self {
        self.topic_id = value;
        self
    }
    pub fn with_partitions(mut self, value: Vec<i32>) -> Self {
        self.partitions = value;
        self
    }
    pub fn read(buf: &mut Bytes, _version: i16) -> Result<Self> {
        let topic_id;
        let partitions;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        topic_id = read_uuid(buf)?;
        partitions = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(len.max(0) as usize);
            for _ in 0..len {
                arr.push(read_i32(buf)?);
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
    pub fn write(&self, buf: &mut BytesMut, _version: i16) -> Result<()> {
        write_uuid(buf, &self.topic_id);
        write_compact_array_length(buf, self.partitions.len() as i32);
        for el in &self.partitions {
            write_i32(buf, *el);
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
    pub fn encoded_len(&self, _version: i16) -> Result<usize> {
        let mut len: usize = 0;
        len += 16;
        len += compact_array_length_len(self.partitions.len() as i32);
        len += self.partitions.len() * 4usize;
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
