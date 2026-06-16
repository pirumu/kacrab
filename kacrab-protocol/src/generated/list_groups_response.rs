//! Generated from ListGroupsResponse.json - DO NOT EDIT
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
pub struct ListGroupsResponseData {
    /// The duration in milliseconds for which the request was throttled due to a quota violation,
    /// or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// The error code, or 0 if there was no error.
    pub error_code: i16,
    /// Each group in the response.
    pub groups: Vec<ListedGroup>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for ListGroupsResponseData {
    fn default() -> Self {
        Self {
            throttle_time_ms: 0_i32,
            error_code: 0_i16,
            groups: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl ListGroupsResponseData {
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 0 || version > 5 {
            return Err(UnsupportedVersion::new(16, version).into());
        }
        let mut throttle_time_ms = 0_i32;
        let error_code;
        let groups;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 1 {
            throttle_time_ms = read_i32(buf)?;
        }
        error_code = read_i16(buf)?;
        if version >= 3 {
            groups = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(ListedGroup::read(buf, version)?);
                }
                arr
            };
        } else {
            groups = {
                let len = read_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(ListedGroup::read(buf, version)?);
                }
                arr
            };
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
            groups,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 0 || version > 5 {
            return Err(UnsupportedVersion::new(16, version).into());
        }
        if version >= 1 {
            write_i32(buf, self.throttle_time_ms);
        }
        write_i16(buf, self.error_code);
        if version >= 3 {
            write_compact_array_length(buf, self.groups.len() as i32);
            for el in &self.groups {
                el.write(buf, version)?;
            }
        } else {
            write_array_length(buf, self.groups.len() as i32);
            for el in &self.groups {
                el.write(buf, version)?;
            }
        }
        if version >= 3 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct ListedGroup {
    /// The group ID.
    pub group_id: KafkaString,
    /// The group protocol type.
    pub protocol_type: KafkaString,
    /// The group state name.
    pub group_state: KafkaString,
    /// The group type name.
    pub group_type: KafkaString,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for ListedGroup {
    fn default() -> Self {
        Self {
            group_id: KafkaString::default(),
            protocol_type: KafkaString::default(),
            group_state: KafkaString::default(),
            group_type: KafkaString::default(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl ListedGroup {
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let group_id;
        let protocol_type;
        let mut group_state = KafkaString::default();
        let mut group_type = KafkaString::default();
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 3 {
            group_id = read_compact_string(buf)?;
        } else {
            group_id = read_string(buf)?;
        }
        if version >= 3 {
            protocol_type = read_compact_string(buf)?;
        } else {
            protocol_type = read_string(buf)?;
        }
        if version >= 4 {
            group_state = read_compact_string(buf)?;
        }
        if version >= 5 {
            group_type = read_compact_string(buf)?;
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
            group_id,
            protocol_type,
            group_state,
            group_type,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version >= 3 {
            write_compact_string(buf, &self.group_id)?;
        } else {
            write_string(buf, &self.group_id)?;
        }
        if version >= 3 {
            write_compact_string(buf, &self.protocol_type)?;
        } else {
            write_string(buf, &self.protocol_type)?;
        }
        if version >= 4 {
            write_compact_string(buf, &self.group_state)?;
        }
        if version >= 5 {
            write_compact_string(buf, &self.group_type)?;
        }
        if version >= 3 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
}
