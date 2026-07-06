//! Generated from IncrementalAlterConfigsResponse.json - DO NOT EDIT
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
pub struct IncrementalAlterConfigsResponseData {
    /// Duration in milliseconds for which the request was throttled due to a quota violation, or
    /// zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// The responses for each resource.
    pub responses: Vec<AlterConfigsResourceResponse>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for IncrementalAlterConfigsResponseData {
    fn default() -> Self {
        Self {
            throttle_time_ms: 0_i32,
            responses: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl IncrementalAlterConfigsResponseData {
    pub fn with_throttle_time_ms(mut self, value: i32) -> Self {
        self.throttle_time_ms = value;
        self
    }
    pub fn with_responses(mut self, value: Vec<AlterConfigsResourceResponse>) -> Self {
        self.responses = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 0 || version > 1 {
            return Err(UnsupportedVersion::new(44, version).into());
        }
        let throttle_time_ms;
        let responses;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        throttle_time_ms = read_i32(buf)?;
        if version >= 1 {
            responses = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
                for _ in 0..len {
                    arr.push(AlterConfigsResourceResponse::read(buf, version)?);
                }
                arr
            };
        } else {
            responses = {
                let len = read_array_length(buf)?;
                let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
                for _ in 0..len {
                    arr.push(AlterConfigsResourceResponse::read(buf, version)?);
                }
                arr
            };
        }
        if version >= 1 {
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
        if version < 0 || version > 1 {
            return Err(UnsupportedVersion::new(44, version).into());
        }
        write_i32(buf, self.throttle_time_ms);
        if version >= 1 {
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
        if version >= 1 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        if version < 0 || version > 1 {
            return Err(UnsupportedVersion::new(44, version).into());
        }
        let mut len: usize = 0;
        len += 4;
        if version >= 1 {
            len += compact_array_length_len(self.responses.len() as i32);
            for el in &self.responses {
                len += el.encoded_len(version)?;
            }
        } else {
            len += array_length_len();
            for el in &self.responses {
                len += el.encoded_len(version)?;
            }
        }
        if version >= 1 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            len += tagged_fields_len(&all_tags)?;
        }
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct AlterConfigsResourceResponse {
    /// The resource error code.
    pub error_code: i16,
    /// The resource error message, or null if there was no error.
    pub error_message: Option<KafkaString>,
    /// The resource type.
    pub resource_type: i8,
    /// The resource name.
    pub resource_name: KafkaString,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for AlterConfigsResourceResponse {
    fn default() -> Self {
        Self {
            error_code: 0_i16,
            error_message: None,
            resource_type: 0_i8,
            resource_name: KafkaString::default(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl AlterConfigsResourceResponse {
    pub fn with_error_code(mut self, value: i16) -> Self {
        self.error_code = value;
        self
    }
    pub fn with_error_message(mut self, value: Option<KafkaString>) -> Self {
        self.error_message = value;
        self
    }
    pub fn with_resource_type(mut self, value: i8) -> Self {
        self.resource_type = value;
        self
    }
    pub fn with_resource_name(mut self, value: KafkaString) -> Self {
        self.resource_name = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let error_code;
        let error_message;
        let resource_type;
        let resource_name;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        error_code = read_i16(buf)?;
        if version >= 1 {
            error_message = read_compact_nullable_string(buf)?;
        } else {
            error_message = read_nullable_string(buf)?;
        }
        resource_type = read_i8(buf)?;
        if version >= 1 {
            resource_name = read_compact_string(buf)?;
        } else {
            resource_name = read_string(buf)?;
        }
        if version >= 1 {
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
            error_message,
            resource_type,
            resource_name,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        write_i16(buf, self.error_code);
        if version >= 1 {
            write_compact_nullable_string(buf, self.error_message.as_ref())?;
        } else {
            write_nullable_string(buf, self.error_message.as_ref())?;
        }
        write_i8(buf, self.resource_type);
        if version >= 1 {
            write_compact_string(buf, &self.resource_name)?;
        } else {
            write_string(buf, &self.resource_name)?;
        }
        if version >= 1 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        let mut len: usize = 0;
        len += 2;
        if version >= 1 {
            len += compact_nullable_string_len(self.error_message.as_ref())?;
        } else {
            len += nullable_string_len(self.error_message.as_ref())?;
        }
        len += 1;
        if version >= 1 {
            len += compact_string_len(&self.resource_name)?;
        } else {
            len += string_len(&self.resource_name)?;
        }
        if version >= 1 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            len += tagged_fields_len(&all_tags)?;
        }
        Ok(len)
    }
}
