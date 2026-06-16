//! Generated from WriteTxnMarkersRequest.json - DO NOT EDIT
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
pub struct WriteTxnMarkersRequestData {
    /// The transaction markers to be written.
    pub markers: Vec<WritableTxnMarker>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for WriteTxnMarkersRequestData {
    fn default() -> Self {
        Self {
            markers: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl WriteTxnMarkersRequestData {
    pub fn with_markers(mut self, value: Vec<WritableTxnMarker>) -> Self {
        self.markers = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 1 || version > 2 {
            return Err(UnsupportedVersion::new(27, version).into());
        }
        let markers;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        markers = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(len.max(0) as usize);
            for _ in 0..len {
                arr.push(WritableTxnMarker::read(buf, version)?);
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
            markers,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 1 || version > 2 {
            return Err(UnsupportedVersion::new(27, version).into());
        }
        write_compact_array_length(buf, self.markers.len() as i32);
        for el in &self.markers {
            el.write(buf, version)?;
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        if version < 1 || version > 2 {
            return Err(UnsupportedVersion::new(27, version).into());
        }
        let mut len: usize = 0;
        len += compact_array_length_len(self.markers.len() as i32);
        for el in &self.markers {
            len += el.encoded_len(version)?;
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct WritableTxnMarker {
    /// The current producer ID.
    pub producer_id: i64,
    /// The current epoch associated with the producer ID.
    pub producer_epoch: i16,
    /// The result of the transaction to write to the partitions (false = ABORT, true = COMMIT).
    pub transaction_result: bool,
    /// Each topic that we want to write transaction marker(s) for.
    pub topics: Vec<WritableTxnMarkerTopic>,
    /// Epoch associated with the transaction state partition hosted by this transaction
    /// coordinator.
    pub coordinator_epoch: i32,
    /// Transaction version of the marker. Ex: 0/1 = legacy (TV0/TV1), 2 = TV2 etc.
    pub transaction_version: i8,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for WritableTxnMarker {
    fn default() -> Self {
        Self {
            producer_id: 0_i64,
            producer_epoch: 0_i16,
            transaction_result: false,
            topics: Vec::new(),
            coordinator_epoch: 0_i32,
            transaction_version: 0i8,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl WritableTxnMarker {
    pub fn with_producer_id(mut self, value: i64) -> Self {
        self.producer_id = value;
        self
    }
    pub fn with_producer_epoch(mut self, value: i16) -> Self {
        self.producer_epoch = value;
        self
    }
    pub fn with_transaction_result(mut self, value: bool) -> Self {
        self.transaction_result = value;
        self
    }
    pub fn with_topics(mut self, value: Vec<WritableTxnMarkerTopic>) -> Self {
        self.topics = value;
        self
    }
    pub fn with_coordinator_epoch(mut self, value: i32) -> Self {
        self.coordinator_epoch = value;
        self
    }
    pub fn with_transaction_version(mut self, value: i8) -> Self {
        self.transaction_version = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let producer_id;
        let producer_epoch;
        let transaction_result;
        let topics;
        let coordinator_epoch;
        let mut transaction_version = 0i8;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        producer_id = read_i64(buf)?;
        producer_epoch = read_i16(buf)?;
        transaction_result = read_bool(buf)?;
        topics = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(len.max(0) as usize);
            for _ in 0..len {
                arr.push(WritableTxnMarkerTopic::read(buf, version)?);
            }
            arr
        };
        coordinator_epoch = read_i32(buf)?;
        if version >= 2 {
            transaction_version = read_i8(buf)?;
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
            producer_id,
            producer_epoch,
            transaction_result,
            topics,
            coordinator_epoch,
            transaction_version,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        write_i64(buf, self.producer_id);
        write_i16(buf, self.producer_epoch);
        write_bool(buf, self.transaction_result);
        write_compact_array_length(buf, self.topics.len() as i32);
        for el in &self.topics {
            el.write(buf, version)?;
        }
        write_i32(buf, self.coordinator_epoch);
        if version >= 2 {
            write_i8(buf, self.transaction_version);
        } else if self.transaction_version != 0i8 {
            return Err(UnsupportedFieldVersion::new(27, "transaction_version", version).into());
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        let mut len: usize = 0;
        len += 8;
        len += 2;
        len += 1;
        len += compact_array_length_len(self.topics.len() as i32);
        for el in &self.topics {
            len += el.encoded_len(version)?;
        }
        len += 4;
        if version >= 2 {
            len += 1;
        } else if self.transaction_version != 0i8 {
            return Err(UnsupportedFieldVersion::new(27, "transaction_version", version).into());
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct WritableTxnMarkerTopic {
    /// The topic name.
    pub name: KafkaString,
    /// The indexes of the partitions to write transaction markers for.
    pub partition_indexes: Vec<i32>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for WritableTxnMarkerTopic {
    fn default() -> Self {
        Self {
            name: KafkaString::default(),
            partition_indexes: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl WritableTxnMarkerTopic {
    pub fn with_name(mut self, value: KafkaString) -> Self {
        self.name = value;
        self
    }
    pub fn with_partition_indexes(mut self, value: Vec<i32>) -> Self {
        self.partition_indexes = value;
        self
    }
    pub fn read(buf: &mut Bytes, _version: i16) -> Result<Self> {
        let name;
        let partition_indexes;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        name = read_compact_string(buf)?;
        partition_indexes = {
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
            name,
            partition_indexes,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, _version: i16) -> Result<()> {
        write_compact_string(buf, &self.name)?;
        write_compact_array_length(buf, self.partition_indexes.len() as i32);
        for el in &self.partition_indexes {
            write_i32(buf, *el);
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
    pub fn encoded_len(&self, _version: i16) -> Result<usize> {
        let mut len: usize = 0;
        len += compact_string_len(&self.name)?;
        len += compact_array_length_len(self.partition_indexes.len() as i32);
        len += self.partition_indexes.len() * 4usize;
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
