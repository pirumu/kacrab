//! Generated from ListOffsetsRequest.json - DO NOT EDIT
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
pub struct ListOffsetsRequestData {
    /// The broker ID of the requester, or -1 if this request is being made by a normal consumer.
    pub replica_id: i32,
    /// This setting controls the visibility of transactional records. Using READ_UNCOMMITTED
    /// (isolation_level = 0) makes all records visible. With READ_COMMITTED (isolation_level = 1),
    /// non-transactional and COMMITTED transactional records are visible. To be more concrete,
    /// READ_COMMITTED returns all data from offsets smaller than the current LSO (last stable
    /// offset), and enables the inclusion of the list of aborted transactions in the result, which
    /// allows consumers to discard ABORTED transactional records.
    pub isolation_level: i8,
    /// Each topic in the request.
    pub topics: Vec<ListOffsetsTopic>,
    /// The timeout to await a response in milliseconds for requests that require reading from
    /// remote storage for topics enabled with tiered storage.
    pub timeout_ms: i32,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for ListOffsetsRequestData {
    fn default() -> Self {
        Self {
            replica_id: 0_i32,
            isolation_level: 0_i8,
            topics: Vec::new(),
            timeout_ms: 0_i32,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl ListOffsetsRequestData {
    pub fn with_replica_id(mut self, value: i32) -> Self {
        self.replica_id = value;
        self
    }
    pub fn with_isolation_level(mut self, value: i8) -> Self {
        self.isolation_level = value;
        self
    }
    pub fn with_topics(mut self, value: Vec<ListOffsetsTopic>) -> Self {
        self.topics = value;
        self
    }
    pub fn with_timeout_ms(mut self, value: i32) -> Self {
        self.timeout_ms = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 1 || version > 11 {
            return Err(UnsupportedVersion::new(2, version).into());
        }
        let replica_id;
        let mut isolation_level = 0_i8;
        let topics;
        let mut timeout_ms = 0_i32;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        replica_id = read_i32(buf)?;
        if version >= 2 {
            isolation_level = read_i8(buf)?;
        }
        if version >= 6 {
            topics = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(ListOffsetsTopic::read(buf, version)?);
                }
                arr
            };
        } else {
            topics = {
                let len = read_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(ListOffsetsTopic::read(buf, version)?);
                }
                arr
            };
        }
        if version >= 10 {
            timeout_ms = read_i32(buf)?;
        }
        if version >= 6 {
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
            isolation_level,
            topics,
            timeout_ms,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 1 || version > 11 {
            return Err(UnsupportedVersion::new(2, version).into());
        }
        write_i32(buf, self.replica_id);
        if version >= 2 {
            write_i8(buf, self.isolation_level);
        } else if self.isolation_level != 0_i8 {
            return Err(UnsupportedFieldVersion::new(2, "isolation_level", version).into());
        }
        if version >= 6 {
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
        if version >= 10 {
            write_i32(buf, self.timeout_ms);
        } else if self.timeout_ms != 0_i32 {
            return Err(UnsupportedFieldVersion::new(2, "timeout_ms", version).into());
        }
        if version >= 6 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        if version < 1 || version > 11 {
            return Err(UnsupportedVersion::new(2, version).into());
        }
        let mut len: usize = 0;
        len += 4;
        if version >= 2 {
            len += 1;
        } else if self.isolation_level != 0_i8 {
            return Err(UnsupportedFieldVersion::new(2, "isolation_level", version).into());
        }
        if version >= 6 {
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
        if version >= 10 {
            len += 4;
        } else if self.timeout_ms != 0_i32 {
            return Err(UnsupportedFieldVersion::new(2, "timeout_ms", version).into());
        }
        if version >= 6 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            len += tagged_fields_len(&all_tags)?;
        }
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct ListOffsetsTopic {
    /// The topic name.
    pub name: KafkaString,
    /// Each partition in the request.
    pub partitions: Vec<ListOffsetsPartition>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for ListOffsetsTopic {
    fn default() -> Self {
        Self {
            name: KafkaString::default(),
            partitions: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl ListOffsetsTopic {
    pub fn with_name(mut self, value: KafkaString) -> Self {
        self.name = value;
        self
    }
    pub fn with_partitions(mut self, value: Vec<ListOffsetsPartition>) -> Self {
        self.partitions = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let name;
        let partitions;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 6 {
            name = read_compact_string(buf)?;
        } else {
            name = read_string(buf)?;
        }
        if version >= 6 {
            partitions = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(ListOffsetsPartition::read(buf, version)?);
                }
                arr
            };
        } else {
            partitions = {
                let len = read_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(ListOffsetsPartition::read(buf, version)?);
                }
                arr
            };
        }
        if version >= 6 {
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
            partitions,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version >= 6 {
            write_compact_string(buf, &self.name)?;
        } else {
            write_string(buf, &self.name)?;
        }
        if version >= 6 {
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
        if version >= 6 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        let mut len: usize = 0;
        if version >= 6 {
            len += compact_string_len(&self.name)?;
        } else {
            len += string_len(&self.name)?;
        }
        if version >= 6 {
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
        if version >= 6 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            len += tagged_fields_len(&all_tags)?;
        }
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct ListOffsetsPartition {
    /// The partition index.
    pub partition_index: i32,
    /// The current leader epoch.
    pub current_leader_epoch: i32,
    /// The current timestamp.
    pub timestamp: i64,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for ListOffsetsPartition {
    fn default() -> Self {
        Self {
            partition_index: 0_i32,
            current_leader_epoch: -1i32,
            timestamp: 0_i64,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl ListOffsetsPartition {
    pub fn with_partition_index(mut self, value: i32) -> Self {
        self.partition_index = value;
        self
    }
    pub fn with_current_leader_epoch(mut self, value: i32) -> Self {
        self.current_leader_epoch = value;
        self
    }
    pub fn with_timestamp(mut self, value: i64) -> Self {
        self.timestamp = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let partition_index;
        let mut current_leader_epoch = -1i32;
        let timestamp;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        partition_index = read_i32(buf)?;
        if version >= 4 {
            current_leader_epoch = read_i32(buf)?;
        }
        timestamp = read_i64(buf)?;
        if version >= 6 {
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
            partition_index,
            current_leader_epoch,
            timestamp,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        write_i32(buf, self.partition_index);
        if version >= 4 {
            write_i32(buf, self.current_leader_epoch);
        } else if self.current_leader_epoch != -1i32 {
            return Err(UnsupportedFieldVersion::new(2, "current_leader_epoch", version).into());
        }
        write_i64(buf, self.timestamp);
        if version >= 6 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        let mut len: usize = 0;
        len += 4;
        if version >= 4 {
            len += 4;
        } else if self.current_leader_epoch != -1i32 {
            return Err(UnsupportedFieldVersion::new(2, "current_leader_epoch", version).into());
        }
        len += 8;
        if version >= 6 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            len += tagged_fields_len(&all_tags)?;
        }
        Ok(len)
    }
}
