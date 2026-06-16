//! Generated from AlterPartitionReassignmentsResponse.json - DO NOT EDIT
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
pub struct AlterPartitionReassignmentsResponseData {
    /// The duration in milliseconds for which the request was throttled due to a quota violation,
    /// or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// The option indicating whether changing the replication factor of any given partition as
    /// part of the request was allowed.
    pub allow_replication_factor_change: bool,
    /// The top-level error code, or 0 if there was no error.
    pub error_code: i16,
    /// The top-level error message, or null if there was no error.
    pub error_message: Option<KafkaString>,
    /// The responses to topics to reassign.
    pub responses: Vec<ReassignableTopicResponse>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for AlterPartitionReassignmentsResponseData {
    fn default() -> Self {
        Self {
            throttle_time_ms: 0_i32,
            allow_replication_factor_change: true,
            error_code: 0_i16,
            error_message: None,
            responses: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl AlterPartitionReassignmentsResponseData {
    pub fn with_throttle_time_ms(mut self, value: i32) -> Self {
        self.throttle_time_ms = value;
        self
    }
    pub fn with_allow_replication_factor_change(mut self, value: bool) -> Self {
        self.allow_replication_factor_change = value;
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
    pub fn with_responses(mut self, value: Vec<ReassignableTopicResponse>) -> Self {
        self.responses = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 0 || version > 1 {
            return Err(UnsupportedVersion::new(45, version).into());
        }
        let throttle_time_ms;
        let mut allow_replication_factor_change = true;
        let error_code;
        let error_message;
        let responses;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        throttle_time_ms = read_i32(buf)?;
        if version >= 1 {
            allow_replication_factor_change = read_bool(buf)?;
        }
        error_code = read_i16(buf)?;
        error_message = read_compact_nullable_string(buf)?;
        responses = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(len.max(0) as usize);
            for _ in 0..len {
                arr.push(ReassignableTopicResponse::read(buf, version)?);
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
            allow_replication_factor_change,
            error_code,
            error_message,
            responses,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 0 || version > 1 {
            return Err(UnsupportedVersion::new(45, version).into());
        }
        write_i32(buf, self.throttle_time_ms);
        if version >= 1 {
            write_bool(buf, self.allow_replication_factor_change);
        } else if self.allow_replication_factor_change != true {
            return Err(UnsupportedFieldVersion::new(
                45,
                "allow_replication_factor_change",
                version,
            )
            .into());
        }
        write_i16(buf, self.error_code);
        write_compact_nullable_string(buf, self.error_message.as_ref())?;
        write_compact_array_length(buf, self.responses.len() as i32);
        for el in &self.responses {
            el.write(buf, version)?;
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        if version < 0 || version > 1 {
            return Err(UnsupportedVersion::new(45, version).into());
        }
        let mut len: usize = 0;
        len += 4;
        if version >= 1 {
            len += 1;
        } else if self.allow_replication_factor_change != true {
            return Err(UnsupportedFieldVersion::new(
                45,
                "allow_replication_factor_change",
                version,
            )
            .into());
        }
        len += 2;
        len += compact_nullable_string_len(self.error_message.as_ref())?;
        len += compact_array_length_len(self.responses.len() as i32);
        for el in &self.responses {
            len += el.encoded_len(version)?;
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct ReassignableTopicResponse {
    /// The topic name.
    pub name: KafkaString,
    /// The responses to partitions to reassign.
    pub partitions: Vec<ReassignablePartitionResponse>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for ReassignableTopicResponse {
    fn default() -> Self {
        Self {
            name: KafkaString::default(),
            partitions: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl ReassignableTopicResponse {
    pub fn with_name(mut self, value: KafkaString) -> Self {
        self.name = value;
        self
    }
    pub fn with_partitions(mut self, value: Vec<ReassignablePartitionResponse>) -> Self {
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
            let mut arr = Vec::with_capacity(len.max(0) as usize);
            for _ in 0..len {
                arr.push(ReassignablePartitionResponse::read(buf, version)?);
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
pub struct ReassignablePartitionResponse {
    /// The partition index.
    pub partition_index: i32,
    /// The error code for this partition, or 0 if there was no error.
    pub error_code: i16,
    /// The error message for this partition, or null if there was no error.
    pub error_message: Option<KafkaString>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for ReassignablePartitionResponse {
    fn default() -> Self {
        Self {
            partition_index: 0_i32,
            error_code: 0_i16,
            error_message: None,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl ReassignablePartitionResponse {
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
    pub fn read(buf: &mut Bytes, _version: i16) -> Result<Self> {
        let partition_index;
        let error_code;
        let error_message;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        partition_index = read_i32(buf)?;
        error_code = read_i16(buf)?;
        error_message = read_compact_nullable_string(buf)?;
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
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, _version: i16) -> Result<()> {
        write_i32(buf, self.partition_index);
        write_i16(buf, self.error_code);
        write_compact_nullable_string(buf, self.error_message.as_ref())?;
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
    pub fn encoded_len(&self, _version: i16) -> Result<usize> {
        let mut len: usize = 0;
        len += 4;
        len += 2;
        len += compact_nullable_string_len(self.error_message.as_ref())?;
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
