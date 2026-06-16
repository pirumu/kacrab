//! Generated from DeleteTopicsResponse.json - DO NOT EDIT
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
pub struct DeleteTopicsResponseData {
    /// The duration in milliseconds for which the request was throttled due to a quota violation,
    /// or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// The results for each topic we tried to delete.
    pub responses: Vec<DeletableTopicResult>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for DeleteTopicsResponseData {
    fn default() -> Self {
        Self {
            throttle_time_ms: 0_i32,
            responses: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl DeleteTopicsResponseData {
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 1 || version > 6 {
            return Err(UnsupportedVersion::new(20, version).into());
        }
        let throttle_time_ms;
        let responses;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        throttle_time_ms = read_i32(buf)?;
        if version >= 4 {
            responses = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(DeletableTopicResult::read(buf, version)?);
                }
                arr
            };
        } else {
            responses = {
                let len = read_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(DeletableTopicResult::read(buf, version)?);
                }
                arr
            };
        }
        if version >= 4 {
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
            responses,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 1 || version > 6 {
            return Err(UnsupportedVersion::new(20, version).into());
        }
        write_i32(buf, self.throttle_time_ms);
        if version >= 4 {
            write_compact_array_length(buf, self.responses.len() as i32);
            for el in &self.responses {
                el.write(buf, version)?;
            }
        } else {
            write_array_length(buf, self.responses.len() as i32);
            for el in &self.responses {
                el.write(buf, version)?;
            }
        }
        if version >= 4 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct DeletableTopicResult {
    /// The topic name.
    pub name: Option<KafkaString>,
    /// The unique topic ID.
    pub topic_id: KafkaUuid,
    /// The deletion error, or 0 if the deletion succeeded.
    pub error_code: i16,
    /// The error message, or null if there was no error.
    pub error_message: Option<KafkaString>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for DeletableTopicResult {
    fn default() -> Self {
        Self {
            name: None,
            topic_id: KafkaUuid::ZERO,
            error_code: 0_i16,
            error_message: None,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl DeletableTopicResult {
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let name;
        let mut topic_id = KafkaUuid::ZERO;
        let error_code;
        let mut error_message = None;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 6 {
            name = read_compact_nullable_string(buf)?;
        } else {
            if version >= 4 {
                name = Some(read_compact_string(buf)?);
            } else {
                name = Some(read_string(buf)?);
            }
        }
        if version >= 6 {
            topic_id = read_uuid(buf)?;
        }
        error_code = read_i16(buf)?;
        if version >= 5 {
            error_message = read_compact_nullable_string(buf)?;
        }
        if version >= 4 {
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
            error_code,
            error_message,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version >= 6 {
            write_compact_nullable_string(buf, self.name.as_ref())?;
        } else {
            {
                let _nn_default = KafkaString::default();
                let _nn_val = self.name.as_ref().unwrap_or(&_nn_default);
                if version >= 4 {
                    write_compact_string(buf, _nn_val)?;
                } else {
                    write_string(buf, _nn_val)?;
                }
            }
        }
        if version >= 6 {
            write_uuid(buf, &self.topic_id);
        }
        write_i16(buf, self.error_code);
        if version >= 5 {
            write_compact_nullable_string(buf, self.error_message.as_ref())?;
        }
        if version >= 4 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
}
