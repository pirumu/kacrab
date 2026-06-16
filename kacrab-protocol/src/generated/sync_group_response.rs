//! Generated from SyncGroupResponse.json - DO NOT EDIT
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
pub struct SyncGroupResponseData {
    /// The duration in milliseconds for which the request was throttled due to a quota violation,
    /// or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// The error code, or 0 if there was no error.
    pub error_code: i16,
    /// The group protocol type.
    pub protocol_type: Option<KafkaString>,
    /// The group protocol name.
    pub protocol_name: Option<KafkaString>,
    /// The member assignment.
    pub assignment: Bytes,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for SyncGroupResponseData {
    fn default() -> Self {
        Self {
            throttle_time_ms: 0_i32,
            error_code: 0_i16,
            protocol_type: None,
            protocol_name: None,
            assignment: Bytes::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl SyncGroupResponseData {
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 0 || version > 5 {
            return Err(UnsupportedVersion::new(14, version).into());
        }
        let mut throttle_time_ms = 0_i32;
        let error_code;
        let mut protocol_type = None;
        let mut protocol_name = None;
        let assignment;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 1 {
            throttle_time_ms = read_i32(buf)?;
        }
        error_code = read_i16(buf)?;
        if version >= 5 {
            protocol_type = read_compact_nullable_string(buf)?;
        }
        if version >= 5 {
            protocol_name = read_compact_nullable_string(buf)?;
        }
        if version >= 4 {
            assignment = read_compact_bytes(buf)?;
        } else {
            assignment = read_bytes(buf)?;
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
            error_code,
            protocol_type,
            protocol_name,
            assignment,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 0 || version > 5 {
            return Err(UnsupportedVersion::new(14, version).into());
        }
        if version >= 1 {
            write_i32(buf, self.throttle_time_ms);
        }
        write_i16(buf, self.error_code);
        if version >= 5 {
            write_compact_nullable_string(buf, self.protocol_type.as_ref())?;
        }
        if version >= 5 {
            write_compact_nullable_string(buf, self.protocol_name.as_ref())?;
        }
        if version >= 4 {
            write_compact_bytes(buf, &self.assignment)?;
        } else {
            write_bytes(buf, &self.assignment)?;
        }
        if version >= 4 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
}
