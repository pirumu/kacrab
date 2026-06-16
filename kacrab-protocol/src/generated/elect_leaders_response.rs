//! Generated from ElectLeadersResponse.json - DO NOT EDIT
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
pub struct ElectLeadersResponseData {
    /// The duration in milliseconds for which the request was throttled due to a quota violation,
    /// or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// The top level response error code.
    pub error_code: i16,
    /// The election results, or an empty array if the requester did not have permission and the
    /// request asks for all partitions.
    pub replica_election_results: Vec<ReplicaElectionResult>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for ElectLeadersResponseData {
    fn default() -> Self {
        Self {
            throttle_time_ms: 0_i32,
            error_code: 0_i16,
            replica_election_results: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl ElectLeadersResponseData {
    pub fn with_throttle_time_ms(mut self, value: i32) -> Self {
        self.throttle_time_ms = value;
        self
    }
    pub fn with_error_code(mut self, value: i16) -> Self {
        self.error_code = value;
        self
    }
    pub fn with_replica_election_results(mut self, value: Vec<ReplicaElectionResult>) -> Self {
        self.replica_election_results = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 0 || version > 2 {
            return Err(UnsupportedVersion::new(43, version).into());
        }
        let throttle_time_ms;
        let mut error_code = 0_i16;
        let replica_election_results;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        throttle_time_ms = read_i32(buf)?;
        if version >= 1 {
            error_code = read_i16(buf)?;
        }
        if version >= 2 {
            replica_election_results = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(ReplicaElectionResult::read(buf, version)?);
                }
                arr
            };
        } else {
            replica_election_results = {
                let len = read_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(ReplicaElectionResult::read(buf, version)?);
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
            replica_election_results,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 0 || version > 2 {
            return Err(UnsupportedVersion::new(43, version).into());
        }
        write_i32(buf, self.throttle_time_ms);
        if version >= 1 {
            write_i16(buf, self.error_code);
        } else if self.error_code != 0_i16 {
            return Err(UnsupportedFieldVersion::new(43, "error_code", version).into());
        }
        if version >= 2 {
            write_compact_array_length(buf, self.replica_election_results.len() as i32);
            for el in &self.replica_election_results {
                el.write(buf, version)?;
            }
        } else {
            write_array_length(buf, self.replica_election_results.len() as i32);
            for el in &self.replica_election_results {
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
        if version < 0 || version > 2 {
            return Err(UnsupportedVersion::new(43, version).into());
        }
        let mut len: usize = 0;
        len += 4;
        if version >= 1 {
            len += 2;
        } else if self.error_code != 0_i16 {
            return Err(UnsupportedFieldVersion::new(43, "error_code", version).into());
        }
        if version >= 2 {
            len += compact_array_length_len(self.replica_election_results.len() as i32);
            for el in &self.replica_election_results {
                len += el.encoded_len(version)?;
            }
        } else {
            len += array_length_len();
            for el in &self.replica_election_results {
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
pub struct ReplicaElectionResult {
    /// The topic name.
    pub topic: KafkaString,
    /// The results for each partition.
    pub partition_result: Vec<PartitionResult>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for ReplicaElectionResult {
    fn default() -> Self {
        Self {
            topic: KafkaString::default(),
            partition_result: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl ReplicaElectionResult {
    pub fn with_topic(mut self, value: KafkaString) -> Self {
        self.topic = value;
        self
    }
    pub fn with_partition_result(mut self, value: Vec<PartitionResult>) -> Self {
        self.partition_result = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let topic;
        let partition_result;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 2 {
            topic = read_compact_string(buf)?;
        } else {
            topic = read_string(buf)?;
        }
        if version >= 2 {
            partition_result = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(PartitionResult::read(buf, version)?);
                }
                arr
            };
        } else {
            partition_result = {
                let len = read_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(PartitionResult::read(buf, version)?);
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
            topic,
            partition_result,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version >= 2 {
            write_compact_string(buf, &self.topic)?;
        } else {
            write_string(buf, &self.topic)?;
        }
        if version >= 2 {
            write_compact_array_length(buf, self.partition_result.len() as i32);
            for el in &self.partition_result {
                el.write(buf, version)?;
            }
        } else {
            write_array_length(buf, self.partition_result.len() as i32);
            for el in &self.partition_result {
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
            len += compact_string_len(&self.topic)?;
        } else {
            len += string_len(&self.topic)?;
        }
        if version >= 2 {
            len += compact_array_length_len(self.partition_result.len() as i32);
            for el in &self.partition_result {
                len += el.encoded_len(version)?;
            }
        } else {
            len += array_length_len();
            for el in &self.partition_result {
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
pub struct PartitionResult {
    /// The partition id.
    pub partition_id: i32,
    /// The result error, or zero if there was no error.
    pub error_code: i16,
    /// The result message, or null if there was no error.
    pub error_message: Option<KafkaString>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for PartitionResult {
    fn default() -> Self {
        Self {
            partition_id: 0_i32,
            error_code: 0_i16,
            error_message: None,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl PartitionResult {
    pub fn with_partition_id(mut self, value: i32) -> Self {
        self.partition_id = value;
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
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let partition_id;
        let error_code;
        let error_message;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        partition_id = read_i32(buf)?;
        error_code = read_i16(buf)?;
        if version >= 2 {
            error_message = read_compact_nullable_string(buf)?;
        } else {
            error_message = read_nullable_string(buf)?;
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
            partition_id,
            error_code,
            error_message,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        write_i32(buf, self.partition_id);
        write_i16(buf, self.error_code);
        if version >= 2 {
            write_compact_nullable_string(buf, self.error_message.as_ref())?;
        } else {
            write_nullable_string(buf, self.error_message.as_ref())?;
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
        len += 4;
        len += 2;
        if version >= 2 {
            len += compact_nullable_string_len(self.error_message.as_ref())?;
        } else {
            len += nullable_string_len(self.error_message.as_ref())?;
        }
        if version >= 2 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            len += tagged_fields_len(&all_tags)?;
        }
        Ok(len)
    }
}
