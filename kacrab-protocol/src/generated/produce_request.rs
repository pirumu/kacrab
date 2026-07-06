//! Generated from ProduceRequest.json - DO NOT EDIT
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
pub struct ProduceRequestData {
    /// The transactional ID, or null if the producer is not transactional.
    pub transactional_id: Option<KafkaString>,
    /// The number of acknowledgments the producer requires the leader to have received before
    /// considering a request complete. Allowed values: 0 for no acknowledgments, 1 for only the
    /// leader and -1 for the full ISR.
    pub acks: i16,
    /// The timeout to await a response in milliseconds.
    pub timeout_ms: i32,
    /// Each topic to produce to.
    pub topic_data: Vec<TopicProduceData>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for ProduceRequestData {
    fn default() -> Self {
        Self {
            transactional_id: None,
            acks: 0_i16,
            timeout_ms: 0_i32,
            topic_data: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl ProduceRequestData {
    pub fn with_transactional_id(mut self, value: Option<KafkaString>) -> Self {
        self.transactional_id = value;
        self
    }
    pub fn with_acks(mut self, value: i16) -> Self {
        self.acks = value;
        self
    }
    pub fn with_timeout_ms(mut self, value: i32) -> Self {
        self.timeout_ms = value;
        self
    }
    pub fn with_topic_data(mut self, value: Vec<TopicProduceData>) -> Self {
        self.topic_data = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 3 || version > 13 {
            return Err(UnsupportedVersion::new(0, version).into());
        }
        let transactional_id;
        let acks;
        let timeout_ms;
        let topic_data;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 9 {
            transactional_id = read_compact_nullable_string(buf)?;
        } else {
            transactional_id = read_nullable_string(buf)?;
        }
        acks = read_i16(buf)?;
        timeout_ms = read_i32(buf)?;
        if version >= 9 {
            topic_data = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
                for _ in 0..len {
                    arr.push(TopicProduceData::read(buf, version)?);
                }
                arr
            };
        } else {
            topic_data = {
                let len = read_array_length(buf)?;
                let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
                for _ in 0..len {
                    arr.push(TopicProduceData::read(buf, version)?);
                }
                arr
            };
        }
        if version >= 9 {
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
            transactional_id,
            acks,
            timeout_ms,
            topic_data,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 3 || version > 13 {
            return Err(UnsupportedVersion::new(0, version).into());
        }
        if version >= 9 {
            write_compact_nullable_string(buf, self.transactional_id.as_ref())?;
        } else {
            write_nullable_string(buf, self.transactional_id.as_ref())?;
        }
        write_i16(buf, self.acks);
        write_i32(buf, self.timeout_ms);
        if version >= 9 {
            write_compact_array_length(buf, self.topic_data.len() as i32);
            for el in &self.topic_data {
                el.write(buf, version)?;
            }
        } else {
            write_array_length(buf, self.topic_data.len() as i32);
            for el in &self.topic_data {
                el.write(buf, version)?;
            }
        }
        if version >= 9 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        if version < 3 || version > 13 {
            return Err(UnsupportedVersion::new(0, version).into());
        }
        let mut len: usize = 0;
        if version >= 9 {
            len += compact_nullable_string_len(self.transactional_id.as_ref())?;
        } else {
            len += nullable_string_len(self.transactional_id.as_ref())?;
        }
        len += 2;
        len += 4;
        if version >= 9 {
            len += compact_array_length_len(self.topic_data.len() as i32);
            for el in &self.topic_data {
                len += el.encoded_len(version)?;
            }
        } else {
            len += array_length_len();
            for el in &self.topic_data {
                len += el.encoded_len(version)?;
            }
        }
        if version >= 9 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            len += tagged_fields_len(&all_tags)?;
        }
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct TopicProduceData {
    /// The topic name.
    pub name: KafkaString,
    /// The unique topic ID
    pub topic_id: KafkaUuid,
    /// Each partition to produce to.
    pub partition_data: Vec<PartitionProduceData>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for TopicProduceData {
    fn default() -> Self {
        Self {
            name: KafkaString::default(),
            topic_id: KafkaUuid::ZERO,
            partition_data: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl TopicProduceData {
    pub fn with_name(mut self, value: KafkaString) -> Self {
        self.name = value;
        self
    }
    pub fn with_topic_id(mut self, value: KafkaUuid) -> Self {
        self.topic_id = value;
        self
    }
    pub fn with_partition_data(mut self, value: Vec<PartitionProduceData>) -> Self {
        self.partition_data = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let mut name = KafkaString::default();
        let mut topic_id = KafkaUuid::ZERO;
        let partition_data;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version <= 12 {
            if version >= 9 {
                name = read_compact_string(buf)?;
            } else {
                name = read_string(buf)?;
            }
        }
        if version >= 13 {
            topic_id = read_uuid(buf)?;
        }
        if version >= 9 {
            partition_data = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
                for _ in 0..len {
                    arr.push(PartitionProduceData::read(buf, version)?);
                }
                arr
            };
        } else {
            partition_data = {
                let len = read_array_length(buf)?;
                let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
                for _ in 0..len {
                    arr.push(PartitionProduceData::read(buf, version)?);
                }
                arr
            };
        }
        if version >= 9 {
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
            name,
            topic_id,
            partition_data,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version <= 12 {
            if version >= 9 {
                write_compact_string(buf, &self.name)?;
            } else {
                write_string(buf, &self.name)?;
            }
        } else if self.name != KafkaString::default() {
            return Err(UnsupportedFieldVersion::new(0, "name", version).into());
        }
        if version >= 13 {
            write_uuid(buf, &self.topic_id);
        } else if self.topic_id != KafkaUuid::ZERO {
            return Err(UnsupportedFieldVersion::new(0, "topic_id", version).into());
        }
        if version >= 9 {
            write_compact_array_length(buf, self.partition_data.len() as i32);
            for el in &self.partition_data {
                el.write(buf, version)?;
            }
        } else {
            write_array_length(buf, self.partition_data.len() as i32);
            for el in &self.partition_data {
                el.write(buf, version)?;
            }
        }
        if version >= 9 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        let mut len: usize = 0;
        if version <= 12 {
            if version >= 9 {
                len += compact_string_len(&self.name)?;
            } else {
                len += string_len(&self.name)?;
            }
        } else if self.name != KafkaString::default() {
            return Err(UnsupportedFieldVersion::new(0, "name", version).into());
        }
        if version >= 13 {
            len += 16;
        } else if self.topic_id != KafkaUuid::ZERO {
            return Err(UnsupportedFieldVersion::new(0, "topic_id", version).into());
        }
        if version >= 9 {
            len += compact_array_length_len(self.partition_data.len() as i32);
            for el in &self.partition_data {
                len += el.encoded_len(version)?;
            }
        } else {
            len += array_length_len();
            for el in &self.partition_data {
                len += el.encoded_len(version)?;
            }
        }
        if version >= 9 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            len += tagged_fields_len(&all_tags)?;
        }
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct PartitionProduceData {
    /// The partition index.
    pub index: i32,
    /// The record data to be produced.
    pub records: Option<Bytes>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for PartitionProduceData {
    fn default() -> Self {
        Self {
            index: 0_i32,
            records: None,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl PartitionProduceData {
    pub fn with_index(mut self, value: i32) -> Self {
        self.index = value;
        self
    }
    pub fn with_records(mut self, value: Option<Bytes>) -> Self {
        self.records = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let index;
        let records;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        index = read_i32(buf)?;
        if version >= 9 {
            records = read_compact_nullable_bytes(buf)?;
        } else {
            records = read_nullable_bytes(buf)?;
        }
        if version >= 9 {
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
            index,
            records,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        write_i32(buf, self.index);
        if version >= 9 {
            write_compact_nullable_bytes(buf, self.records.as_ref().map(|b| b.as_ref()))?;
        } else {
            write_nullable_bytes(buf, self.records.as_ref().map(|b| b.as_ref()))?;
        }
        if version >= 9 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        let mut len: usize = 0;
        len += 4;
        if version >= 9 {
            len += compact_nullable_bytes_len(self.records.as_ref().map(|b| b.as_ref()))?;
        } else {
            len += nullable_bytes_len(self.records.as_ref().map(|b| b.as_ref()))?;
        }
        if version >= 9 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            len += tagged_fields_len(&all_tags)?;
        }
        Ok(len)
    }
}
