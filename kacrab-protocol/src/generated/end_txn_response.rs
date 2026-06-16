//! Generated from EndTxnResponse.json - DO NOT EDIT
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
pub struct EndTxnResponseData {
    /// The duration in milliseconds for which the request was throttled due to a quota violation,
    /// or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// The error code, or 0 if there was no error.
    pub error_code: i16,
    /// The producer ID.
    pub producer_id: i64,
    /// The current epoch associated with the producer.
    pub producer_epoch: i16,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for EndTxnResponseData {
    fn default() -> Self {
        Self {
            throttle_time_ms: 0_i32,
            error_code: 0_i16,
            producer_id: -1i64,
            producer_epoch: -1i16,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl EndTxnResponseData {
    pub fn with_throttle_time_ms(mut self, value: i32) -> Self {
        self.throttle_time_ms = value;
        self
    }
    pub fn with_error_code(mut self, value: i16) -> Self {
        self.error_code = value;
        self
    }
    pub fn with_producer_id(mut self, value: i64) -> Self {
        self.producer_id = value;
        self
    }
    pub fn with_producer_epoch(mut self, value: i16) -> Self {
        self.producer_epoch = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 0 || version > 5 {
            return Err(UnsupportedVersion::new(26, version).into());
        }
        let throttle_time_ms;
        let error_code;
        let mut producer_id = -1i64;
        let mut producer_epoch = -1i16;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        throttle_time_ms = read_i32(buf)?;
        error_code = read_i16(buf)?;
        if version >= 5 {
            producer_id = read_i64(buf)?;
        }
        if version >= 5 {
            producer_epoch = read_i16(buf)?;
        }
        if version >= 3 {
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
            producer_id,
            producer_epoch,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 0 || version > 5 {
            return Err(UnsupportedVersion::new(26, version).into());
        }
        write_i32(buf, self.throttle_time_ms);
        write_i16(buf, self.error_code);
        if version >= 5 {
            write_i64(buf, self.producer_id);
        } else if self.producer_id != -1i64 {
            return Err(UnsupportedFieldVersion::new(26, "producer_id", version).into());
        }
        if version >= 5 {
            write_i16(buf, self.producer_epoch);
        } else if self.producer_epoch != -1i16 {
            return Err(UnsupportedFieldVersion::new(26, "producer_epoch", version).into());
        }
        if version >= 3 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        if version < 0 || version > 5 {
            return Err(UnsupportedVersion::new(26, version).into());
        }
        let mut len: usize = 0;
        len += 4;
        len += 2;
        if version >= 5 {
            len += 8;
        } else if self.producer_id != -1i64 {
            return Err(UnsupportedFieldVersion::new(26, "producer_id", version).into());
        }
        if version >= 5 {
            len += 2;
        } else if self.producer_epoch != -1i16 {
            return Err(UnsupportedFieldVersion::new(26, "producer_epoch", version).into());
        }
        if version >= 3 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            len += tagged_fields_len(&all_tags)?;
        }
        Ok(len)
    }
}
