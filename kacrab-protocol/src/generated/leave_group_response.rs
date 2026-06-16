//! Generated from LeaveGroupResponse.json - DO NOT EDIT
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
pub struct LeaveGroupResponseData {
    /// The duration in milliseconds for which the request was throttled due to a quota violation,
    /// or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// The error code, or 0 if there was no error.
    pub error_code: i16,
    /// List of leaving member responses.
    pub members: Vec<MemberResponse>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for LeaveGroupResponseData {
    fn default() -> Self {
        Self {
            throttle_time_ms: 0_i32,
            error_code: 0_i16,
            members: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl LeaveGroupResponseData {
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 0 || version > 5 {
            return Err(UnsupportedVersion::new(13, version).into());
        }
        let mut throttle_time_ms = 0_i32;
        let error_code;
        let mut members = Vec::new();
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 1 {
            throttle_time_ms = read_i32(buf)?;
        }
        error_code = read_i16(buf)?;
        if version >= 3 {
            if version >= 4 {
                members = {
                    let len = read_compact_array_length(buf)?;
                    let mut arr = Vec::with_capacity(len.max(0) as usize);
                    for _ in 0..len {
                        arr.push(MemberResponse::read(buf, version)?);
                    }
                    arr
                };
            } else {
                members = {
                    let len = read_array_length(buf)?;
                    let mut arr = Vec::with_capacity(len.max(0) as usize);
                    for _ in 0..len {
                        arr.push(MemberResponse::read(buf, version)?);
                    }
                    arr
                };
            }
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
            members,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 0 || version > 5 {
            return Err(UnsupportedVersion::new(13, version).into());
        }
        if version >= 1 {
            write_i32(buf, self.throttle_time_ms);
        }
        write_i16(buf, self.error_code);
        if version >= 3 {
            if version >= 4 {
                write_compact_array_length(buf, self.members.len() as i32);
                for el in &self.members {
                    el.write(buf, version)?;
                }
            } else {
                write_array_length(buf, self.members.len() as i32);
                for el in &self.members {
                    el.write(buf, version)?;
                }
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
pub struct MemberResponse {
    /// The member ID to remove from the group.
    pub member_id: KafkaString,
    /// The group instance ID to remove from the group.
    pub group_instance_id: Option<KafkaString>,
    /// The error code, or 0 if there was no error.
    pub error_code: i16,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for MemberResponse {
    fn default() -> Self {
        Self {
            member_id: KafkaString::default(),
            group_instance_id: None,
            error_code: 0_i16,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl MemberResponse {
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let member_id;
        let group_instance_id;
        let error_code;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 4 {
            member_id = read_compact_string(buf)?;
        } else {
            member_id = read_string(buf)?;
        }
        if version >= 4 {
            group_instance_id = read_compact_nullable_string(buf)?;
        } else {
            group_instance_id = read_nullable_string(buf)?;
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
            member_id,
            group_instance_id,
            error_code,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version >= 4 {
            write_compact_string(buf, &self.member_id)?;
        } else {
            write_string(buf, &self.member_id)?;
        }
        if version >= 4 {
            write_compact_nullable_string(buf, self.group_instance_id.as_ref())?;
        } else {
            write_nullable_string(buf, self.group_instance_id.as_ref())?;
        }
        write_i16(buf, self.error_code);
        if version >= 4 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
}
