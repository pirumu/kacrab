//! Generated from ReadShareGroupStateSummaryResponse.json - DO NOT EDIT
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
pub struct ReadShareGroupStateSummaryResponseData {
    /// The read results.
    pub results: Vec<ReadStateSummaryResult>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for ReadShareGroupStateSummaryResponseData {
    fn default() -> Self {
        Self {
            results: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl ReadShareGroupStateSummaryResponseData {
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 0 || version > 1 {
            return Err(UnsupportedVersion::new(87, version).into());
        }
        let results;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        results = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(len.max(0) as usize);
            for _ in 0..len {
                arr.push(ReadStateSummaryResult::read(buf, version)?);
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
            results,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 0 || version > 1 {
            return Err(UnsupportedVersion::new(87, version).into());
        }
        write_compact_array_length(buf, self.results.len() as i32);
        for el in &self.results {
            el.write(buf, version)?;
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct ReadStateSummaryResult {
    /// The topic identifier.
    pub topic_id: KafkaUuid,
    /// The results for the partitions.
    pub partitions: Vec<PartitionResult>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for ReadStateSummaryResult {
    fn default() -> Self {
        Self {
            topic_id: KafkaUuid::ZERO,
            partitions: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl ReadStateSummaryResult {
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let topic_id;
        let partitions;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        topic_id = read_uuid(buf)?;
        partitions = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(len.max(0) as usize);
            for _ in 0..len {
                arr.push(PartitionResult::read(buf, version)?);
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
pub struct PartitionResult {
    /// The partition index.
    pub partition: i32,
    /// The error code, or 0 if there was no error.
    pub error_code: i16,
    /// The error message, or null if there was no error.
    pub error_message: Option<KafkaString>,
    /// The state epoch of the share-partition.
    pub state_epoch: i32,
    /// The leader epoch of the share-partition.
    pub leader_epoch: i32,
    /// The share-partition start offset.
    pub start_offset: i64,
    /// The number of offsets greater than or equal to share-partition start offset for which
    /// delivery has been completed.
    pub delivery_complete_count: i32,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for PartitionResult {
    fn default() -> Self {
        Self {
            partition: 0_i32,
            error_code: 0_i16,
            error_message: None,
            state_epoch: 0_i32,
            leader_epoch: 0_i32,
            start_offset: 0_i64,
            delivery_complete_count: -1i32,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl PartitionResult {
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let partition;
        let error_code;
        let error_message;
        let state_epoch;
        let leader_epoch;
        let start_offset;
        let mut delivery_complete_count = -1i32;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        partition = read_i32(buf)?;
        error_code = read_i16(buf)?;
        error_message = read_compact_nullable_string(buf)?;
        state_epoch = read_i32(buf)?;
        leader_epoch = read_i32(buf)?;
        start_offset = read_i64(buf)?;
        if version >= 1 {
            delivery_complete_count = read_i32(buf)?;
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
            partition,
            error_code,
            error_message,
            state_epoch,
            leader_epoch,
            start_offset,
            delivery_complete_count,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        write_i32(buf, self.partition);
        write_i16(buf, self.error_code);
        write_compact_nullable_string(buf, self.error_message.as_ref())?;
        write_i32(buf, self.state_epoch);
        write_i32(buf, self.leader_epoch);
        write_i64(buf, self.start_offset);
        if version >= 1 {
            write_i32(buf, self.delivery_complete_count);
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
}
