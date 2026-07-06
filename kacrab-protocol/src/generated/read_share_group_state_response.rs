//! Generated from ReadShareGroupStateResponse.json - DO NOT EDIT
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
pub struct ReadShareGroupStateResponseData {
    /// The read results.
    pub results: Vec<ReadStateResult>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for ReadShareGroupStateResponseData {
    fn default() -> Self {
        Self {
            results: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl ReadShareGroupStateResponseData {
    pub fn with_results(mut self, value: Vec<ReadStateResult>) -> Self {
        self.results = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 0 || version > 0 {
            return Err(UnsupportedVersion::new(84, version).into());
        }
        let results;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        results = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
            for _ in 0..len {
                arr.push(ReadStateResult::read(buf, version)?);
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
        if version < 0 || version > 0 {
            return Err(UnsupportedVersion::new(84, version).into());
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
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        if version < 0 || version > 0 {
            return Err(UnsupportedVersion::new(84, version).into());
        }
        let mut len: usize = 0;
        len += compact_array_length_len(self.results.len() as i32);
        for el in &self.results {
            len += el.encoded_len(version)?;
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct ReadStateResult {
    /// The topic identifier.
    pub topic_id: KafkaUuid,
    /// The results for the partitions.
    pub partitions: Vec<PartitionResult>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for ReadStateResult {
    fn default() -> Self {
        Self {
            topic_id: KafkaUuid::ZERO,
            partitions: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl ReadStateResult {
    pub fn with_topic_id(mut self, value: KafkaUuid) -> Self {
        self.topic_id = value;
        self
    }
    pub fn with_partitions(mut self, value: Vec<PartitionResult>) -> Self {
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
pub struct PartitionResult {
    /// The partition index.
    pub partition: i32,
    /// The error code, or 0 if there was no error.
    pub error_code: i16,
    /// The error message, or null if there was no error.
    pub error_message: Option<KafkaString>,
    /// The state epoch of the share-partition.
    pub state_epoch: i32,
    /// The share-partition start offset, which can be -1 if it is not yet initialized.
    pub start_offset: i64,
    /// The state batches for this share-partition.
    pub state_batches: Vec<StateBatch>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for PartitionResult {
    fn default() -> Self {
        Self {
            partition: 0_i32,
            error_code: 0_i16,
            error_message: None,
            state_epoch: 0_i32,
            start_offset: 0_i64,
            state_batches: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl PartitionResult {
    pub fn with_partition(mut self, value: i32) -> Self {
        self.partition = value;
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
    pub fn with_state_epoch(mut self, value: i32) -> Self {
        self.state_epoch = value;
        self
    }
    pub fn with_start_offset(mut self, value: i64) -> Self {
        self.start_offset = value;
        self
    }
    pub fn with_state_batches(mut self, value: Vec<StateBatch>) -> Self {
        self.state_batches = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let partition;
        let error_code;
        let error_message;
        let state_epoch;
        let start_offset;
        let state_batches;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        partition = read_i32(buf)?;
        error_code = read_i16(buf)?;
        error_message = read_compact_nullable_string(buf)?;
        state_epoch = read_i32(buf)?;
        start_offset = read_i64(buf)?;
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
            error_code,
            error_message,
            state_epoch,
            start_offset,
            state_batches,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        write_i32(buf, self.partition);
        write_i16(buf, self.error_code);
        write_compact_nullable_string(buf, self.error_message.as_ref())?;
        write_i32(buf, self.state_epoch);
        write_i64(buf, self.start_offset);
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
        len += 2;
        len += compact_nullable_string_len(self.error_message.as_ref())?;
        len += 4;
        len += 8;
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
