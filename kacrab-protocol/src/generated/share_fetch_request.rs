//! Generated from ShareFetchRequest.json - DO NOT EDIT
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
        }
        if version >= 2 {
            write_bool(buf, self.is_renew_ack);
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
}
