//! Generated from HeartbeatResponse.json - DO NOT EDIT
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
pub struct HeartbeatResponseData {
    /// The duration in milliseconds for which the request was throttled due to a quota violation,
    /// or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// The error code, or 0 if there was no error.
    pub error_code: i16,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for HeartbeatResponseData {
    fn default() -> Self {
        Self {
            throttle_time_ms: 0_i32,
            error_code: 0_i16,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl HeartbeatResponseData {
    pub fn with_throttle_time_ms(mut self, value: i32) -> Self {
        self.throttle_time_ms = value;
        self
    }
    pub fn with_error_code(mut self, value: i16) -> Self {
        self.error_code = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 0 || version > 4 {
            return Err(UnsupportedVersion::new(12, version).into());
        }
        let mut throttle_time_ms = 0_i32;
        let error_code;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 1 {
            throttle_time_ms = read_i32(buf)?;
        }
        error_code = read_i16(buf)?;
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
            error_code,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 0 || version > 4 {
            return Err(UnsupportedVersion::new(12, version).into());
        }
        if version >= 1 {
            write_i32(buf, self.throttle_time_ms);
        } else if self.throttle_time_ms != 0_i32 {
            return Err(UnsupportedFieldVersion::new(12, "throttle_time_ms", version).into());
        }
        write_i16(buf, self.error_code);
        if version >= 4 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        if version < 0 || version > 4 {
            return Err(UnsupportedVersion::new(12, version).into());
        }
        let mut len: usize = 0;
        if version >= 1 {
            len += 4;
        } else if self.throttle_time_ms != 0_i32 {
            return Err(UnsupportedFieldVersion::new(12, "throttle_time_ms", version).into());
        }
        len += 2;
        if version >= 4 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            len += tagged_fields_len(&all_tags)?;
        }
        Ok(len)
    }
}
