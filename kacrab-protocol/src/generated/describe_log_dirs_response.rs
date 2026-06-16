//! Generated from DescribeLogDirsResponse.json - DO NOT EDIT
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
pub struct DescribeLogDirsResponseData {
    /// The duration in milliseconds for which the request was throttled due to a quota violation,
    /// or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// The error code, or 0 if there was no error.
    pub error_code: i16,
    /// The log directories.
    pub results: Vec<DescribeLogDirsResult>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for DescribeLogDirsResponseData {
    fn default() -> Self {
        Self {
            throttle_time_ms: 0_i32,
            error_code: 0_i16,
            results: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl DescribeLogDirsResponseData {
    pub fn with_throttle_time_ms(mut self, value: i32) -> Self {
        self.throttle_time_ms = value;
        self
    }
    pub fn with_error_code(mut self, value: i16) -> Self {
        self.error_code = value;
        self
    }
    pub fn with_results(mut self, value: Vec<DescribeLogDirsResult>) -> Self {
        self.results = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 1 || version > 5 {
            return Err(UnsupportedVersion::new(35, version).into());
        }
        let throttle_time_ms;
        let mut error_code = 0_i16;
        let results;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        throttle_time_ms = read_i32(buf)?;
        if version >= 3 {
            error_code = read_i16(buf)?;
        }
        if version >= 2 {
            results = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(DescribeLogDirsResult::read(buf, version)?);
                }
                arr
            };
        } else {
            results = {
                let len = read_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(DescribeLogDirsResult::read(buf, version)?);
                }
                arr
            };
        }
        if version >= 2 {
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
            throttle_time_ms,
            error_code,
            results,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 1 || version > 5 {
            return Err(UnsupportedVersion::new(35, version).into());
        }
        write_i32(buf, self.throttle_time_ms);
        if version >= 3 {
            write_i16(buf, self.error_code);
        } else if self.error_code != 0_i16 {
            return Err(UnsupportedFieldVersion::new(35, "error_code", version).into());
        }
        if version >= 2 {
            write_compact_array_length(buf, self.results.len() as i32);
            for el in &self.results {
                el.write(buf, version)?;
            }
        } else {
            write_array_length(buf, self.results.len() as i32);
            for el in &self.results {
                el.write(buf, version)?;
            }
        }
        if version >= 2 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        if version < 1 || version > 5 {
            return Err(UnsupportedVersion::new(35, version).into());
        }
        let mut len: usize = 0;
        len += 4;
        if version >= 3 {
            len += 2;
        } else if self.error_code != 0_i16 {
            return Err(UnsupportedFieldVersion::new(35, "error_code", version).into());
        }
        if version >= 2 {
            len += compact_array_length_len(self.results.len() as i32);
            for el in &self.results {
                len += el.encoded_len(version)?;
            }
        } else {
            len += array_length_len();
            for el in &self.results {
                len += el.encoded_len(version)?;
            }
        }
        if version >= 2 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            len += tagged_fields_len(&all_tags)?;
        }
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct DescribeLogDirsResult {
    /// The error code, or 0 if there was no error.
    pub error_code: i16,
    /// The absolute log directory path.
    pub log_dir: KafkaString,
    /// The topics.
    pub topics: Vec<DescribeLogDirsTopic>,
    /// The total size in bytes of the volume the log directory is in. This value does not include
    /// the size of data stored in remote storage.
    pub total_bytes: i64,
    /// The usable size in bytes of the volume the log directory is in. This value does not include
    /// the size of data stored in remote storage.
    pub usable_bytes: i64,
    /// True if this log directory is cordoned.
    pub is_cordoned: bool,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for DescribeLogDirsResult {
    fn default() -> Self {
        Self {
            error_code: 0_i16,
            log_dir: KafkaString::default(),
            topics: Vec::new(),
            total_bytes: -1i64,
            usable_bytes: -1i64,
            is_cordoned: false,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl DescribeLogDirsResult {
    pub fn with_error_code(mut self, value: i16) -> Self {
        self.error_code = value;
        self
    }
    pub fn with_log_dir(mut self, value: KafkaString) -> Self {
        self.log_dir = value;
        self
    }
    pub fn with_topics(mut self, value: Vec<DescribeLogDirsTopic>) -> Self {
        self.topics = value;
        self
    }
    pub fn with_total_bytes(mut self, value: i64) -> Self {
        self.total_bytes = value;
        self
    }
    pub fn with_usable_bytes(mut self, value: i64) -> Self {
        self.usable_bytes = value;
        self
    }
    pub fn with_is_cordoned(mut self, value: bool) -> Self {
        self.is_cordoned = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let error_code;
        let log_dir;
        let topics;
        let mut total_bytes = -1i64;
        let mut usable_bytes = -1i64;
        let mut is_cordoned = false;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        error_code = read_i16(buf)?;
        if version >= 2 {
            log_dir = read_compact_string(buf)?;
        } else {
            log_dir = read_string(buf)?;
        }
        if version >= 2 {
            topics = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(DescribeLogDirsTopic::read(buf, version)?);
                }
                arr
            };
        } else {
            topics = {
                let len = read_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(DescribeLogDirsTopic::read(buf, version)?);
                }
                arr
            };
        }
        if version >= 4 {
            total_bytes = read_i64(buf)?;
        }
        if version >= 4 {
            usable_bytes = read_i64(buf)?;
        }
        if version >= 5 {
            is_cordoned = read_bool(buf)?;
        }
        if version >= 2 {
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
            error_code,
            log_dir,
            topics,
            total_bytes,
            usable_bytes,
            is_cordoned,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        write_i16(buf, self.error_code);
        if version >= 2 {
            write_compact_string(buf, &self.log_dir)?;
        } else {
            write_string(buf, &self.log_dir)?;
        }
        if version >= 2 {
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
        if version >= 4 {
            write_i64(buf, self.total_bytes);
        } else if self.total_bytes != -1i64 {
            return Err(UnsupportedFieldVersion::new(35, "total_bytes", version).into());
        }
        if version >= 4 {
            write_i64(buf, self.usable_bytes);
        } else if self.usable_bytes != -1i64 {
            return Err(UnsupportedFieldVersion::new(35, "usable_bytes", version).into());
        }
        if version >= 5 {
            write_bool(buf, self.is_cordoned);
        } else if self.is_cordoned != false {
            return Err(UnsupportedFieldVersion::new(35, "is_cordoned", version).into());
        }
        if version >= 2 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        let mut len: usize = 0;
        len += 2;
        if version >= 2 {
            len += compact_string_len(&self.log_dir)?;
        } else {
            len += string_len(&self.log_dir)?;
        }
        if version >= 2 {
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
        if version >= 4 {
            len += 8;
        } else if self.total_bytes != -1i64 {
            return Err(UnsupportedFieldVersion::new(35, "total_bytes", version).into());
        }
        if version >= 4 {
            len += 8;
        } else if self.usable_bytes != -1i64 {
            return Err(UnsupportedFieldVersion::new(35, "usable_bytes", version).into());
        }
        if version >= 5 {
            len += 1;
        } else if self.is_cordoned != false {
            return Err(UnsupportedFieldVersion::new(35, "is_cordoned", version).into());
        }
        if version >= 2 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            len += tagged_fields_len(&all_tags)?;
        }
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct DescribeLogDirsTopic {
    /// The topic name.
    pub name: KafkaString,
    /// The partitions.
    pub partitions: Vec<DescribeLogDirsPartition>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for DescribeLogDirsTopic {
    fn default() -> Self {
        Self {
            name: KafkaString::default(),
            partitions: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl DescribeLogDirsTopic {
    pub fn with_name(mut self, value: KafkaString) -> Self {
        self.name = value;
        self
    }
    pub fn with_partitions(mut self, value: Vec<DescribeLogDirsPartition>) -> Self {
        self.partitions = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let name;
        let partitions;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 2 {
            name = read_compact_string(buf)?;
        } else {
            name = read_string(buf)?;
        }
        if version >= 2 {
            partitions = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(DescribeLogDirsPartition::read(buf, version)?);
                }
                arr
            };
        } else {
            partitions = {
                let len = read_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(DescribeLogDirsPartition::read(buf, version)?);
                }
                arr
            };
        }
        if version >= 2 {
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
        if version >= 2 {
            write_compact_string(buf, &self.name)?;
        } else {
            write_string(buf, &self.name)?;
        }
        if version >= 2 {
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
        if version >= 2 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        let mut len: usize = 0;
        if version >= 2 {
            len += compact_string_len(&self.name)?;
        } else {
            len += string_len(&self.name)?;
        }
        if version >= 2 {
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
        if version >= 2 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            len += tagged_fields_len(&all_tags)?;
        }
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct DescribeLogDirsPartition {
    /// The partition index.
    pub partition_index: i32,
    /// The size of the log segments in this partition in bytes.
    pub partition_size: i64,
    /// The lag of the log's LEO w.r.t. partition's HW (if it is the current log for the partition)
    /// or current replica's LEO (if it is the future log for the partition).
    pub offset_lag: i64,
    /// True if this log is created by AlterReplicaLogDirsRequest and will replace the current log
    /// of the replica in the future.
    pub is_future_key: bool,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for DescribeLogDirsPartition {
    fn default() -> Self {
        Self {
            partition_index: 0_i32,
            partition_size: 0_i64,
            offset_lag: 0_i64,
            is_future_key: false,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl DescribeLogDirsPartition {
    pub fn with_partition_index(mut self, value: i32) -> Self {
        self.partition_index = value;
        self
    }
    pub fn with_partition_size(mut self, value: i64) -> Self {
        self.partition_size = value;
        self
    }
    pub fn with_offset_lag(mut self, value: i64) -> Self {
        self.offset_lag = value;
        self
    }
    pub fn with_is_future_key(mut self, value: bool) -> Self {
        self.is_future_key = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let partition_index;
        let partition_size;
        let offset_lag;
        let is_future_key;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        partition_index = read_i32(buf)?;
        partition_size = read_i64(buf)?;
        offset_lag = read_i64(buf)?;
        is_future_key = read_bool(buf)?;
        if version >= 2 {
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
            partition_size,
            offset_lag,
            is_future_key,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        write_i32(buf, self.partition_index);
        write_i64(buf, self.partition_size);
        write_i64(buf, self.offset_lag);
        write_bool(buf, self.is_future_key);
        if version >= 2 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        let mut len: usize = 0;
        len += 4;
        len += 8;
        len += 8;
        len += 1;
        if version >= 2 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            len += tagged_fields_len(&all_tags)?;
        }
        Ok(len)
    }
}
