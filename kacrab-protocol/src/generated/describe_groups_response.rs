//! Generated from DescribeGroupsResponse.json - DO NOT EDIT
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
pub struct DescribeGroupsResponseData {
    /// The duration in milliseconds for which the request was throttled due to a quota violation,
    /// or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// Each described group.
    pub groups: Vec<DescribedGroup>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for DescribeGroupsResponseData {
    fn default() -> Self {
        Self {
            throttle_time_ms: 0_i32,
            groups: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl DescribeGroupsResponseData {
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 0 || version > 6 {
            return Err(UnsupportedVersion::new(15, version).into());
        }
        let mut throttle_time_ms = 0_i32;
        let groups;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 1 {
            throttle_time_ms = read_i32(buf)?;
        }
        if version >= 5 {
            groups = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(DescribedGroup::read(buf, version)?);
                }
                arr
            };
        } else {
            groups = {
                let len = read_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(DescribedGroup::read(buf, version)?);
                }
                arr
            };
        }
        if version >= 5 {
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
            groups,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 0 || version > 6 {
            return Err(UnsupportedVersion::new(15, version).into());
        }
        if version >= 1 {
            write_i32(buf, self.throttle_time_ms);
        }
        if version >= 5 {
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
        if version >= 5 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct DescribedGroup {
    /// The describe error, or 0 if there was no error.
    pub error_code: i16,
    /// The describe error message, or null if there was no error.
    pub error_message: Option<KafkaString>,
    /// The group ID string.
    pub group_id: KafkaString,
    /// The group state string, or the empty string.
    pub group_state: KafkaString,
    /// The group protocol type, or the empty string.
    pub protocol_type: KafkaString,
    /// The group protocol data, or the empty string.
    pub protocol_data: KafkaString,
    /// The group members.
    pub members: Vec<DescribedGroupMember>,
    /// 32-bit bitfield to represent authorized operations for this group.
    pub authorized_operations: i32,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for DescribedGroup {
    fn default() -> Self {
        Self {
            error_code: 0_i16,
            error_message: None,
            group_id: KafkaString::default(),
            group_state: KafkaString::default(),
            protocol_type: KafkaString::default(),
            protocol_data: KafkaString::default(),
            members: Vec::new(),
            authorized_operations: i32::MIN,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl DescribedGroup {
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let error_code;
        let mut error_message = None;
        let group_id;
        let group_state;
        let protocol_type;
        let protocol_data;
        let members;
        let mut authorized_operations = i32::MIN;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        error_code = read_i16(buf)?;
        if version >= 6 {
            error_message = read_compact_nullable_string(buf)?;
        }
        if version >= 5 {
            group_id = read_compact_string(buf)?;
        } else {
            group_id = read_string(buf)?;
        }
        if version >= 5 {
            group_state = read_compact_string(buf)?;
        } else {
            group_state = read_string(buf)?;
        }
        if version >= 5 {
            protocol_type = read_compact_string(buf)?;
        } else {
            protocol_type = read_string(buf)?;
        }
        if version >= 5 {
            protocol_data = read_compact_string(buf)?;
        } else {
            protocol_data = read_string(buf)?;
        }
        if version >= 5 {
            members = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(DescribedGroupMember::read(buf, version)?);
                }
                arr
            };
        } else {
            members = {
                let len = read_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(DescribedGroupMember::read(buf, version)?);
                }
                arr
            };
        }
        if version >= 3 {
            authorized_operations = read_i32(buf)?;
        }
        if version >= 5 {
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
            group_id,
            group_state,
            protocol_type,
            protocol_data,
            members,
            authorized_operations,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        write_i16(buf, self.error_code);
        if version >= 6 {
            write_compact_nullable_string(buf, self.error_message.as_ref())?;
        }
        if version >= 5 {
            write_compact_string(buf, &self.group_id)?;
        } else {
            write_string(buf, &self.group_id)?;
        }
        if version >= 5 {
            write_compact_string(buf, &self.group_state)?;
        } else {
            write_string(buf, &self.group_state)?;
        }
        if version >= 5 {
            write_compact_string(buf, &self.protocol_type)?;
        } else {
            write_string(buf, &self.protocol_type)?;
        }
        if version >= 5 {
            write_compact_string(buf, &self.protocol_data)?;
        } else {
            write_string(buf, &self.protocol_data)?;
        }
        if version >= 5 {
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
        if version >= 3 {
            write_i32(buf, self.authorized_operations);
        }
        if version >= 5 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct DescribedGroupMember {
    /// The member id.
    pub member_id: KafkaString,
    /// The unique identifier of the consumer instance provided by end user.
    pub group_instance_id: Option<KafkaString>,
    /// The client ID used in the member's latest join group request.
    pub client_id: KafkaString,
    /// The client host.
    pub client_host: KafkaString,
    /// The metadata corresponding to the current group protocol in use.
    pub member_metadata: Bytes,
    /// The current assignment provided by the group leader.
    pub member_assignment: Bytes,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for DescribedGroupMember {
    fn default() -> Self {
        Self {
            member_id: KafkaString::default(),
            group_instance_id: None,
            client_id: KafkaString::default(),
            client_host: KafkaString::default(),
            member_metadata: Bytes::new(),
            member_assignment: Bytes::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl DescribedGroupMember {
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let member_id;
        let mut group_instance_id = None;
        let client_id;
        let client_host;
        let member_metadata;
        let member_assignment;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 5 {
            member_id = read_compact_string(buf)?;
        } else {
            member_id = read_string(buf)?;
        }
        if version >= 4 {
            if version >= 5 {
                group_instance_id = read_compact_nullable_string(buf)?;
            } else {
                group_instance_id = read_nullable_string(buf)?;
            }
        }
        if version >= 5 {
            client_id = read_compact_string(buf)?;
        } else {
            client_id = read_string(buf)?;
        }
        if version >= 5 {
            client_host = read_compact_string(buf)?;
        } else {
            client_host = read_string(buf)?;
        }
        if version >= 5 {
            member_metadata = read_compact_bytes(buf)?;
        } else {
            member_metadata = read_bytes(buf)?;
        }
        if version >= 5 {
            member_assignment = read_compact_bytes(buf)?;
        } else {
            member_assignment = read_bytes(buf)?;
        }
        if version >= 5 {
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
            client_id,
            client_host,
            member_metadata,
            member_assignment,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version >= 5 {
            write_compact_string(buf, &self.member_id)?;
        } else {
            write_string(buf, &self.member_id)?;
        }
        if version >= 4 {
            if version >= 5 {
                write_compact_nullable_string(buf, self.group_instance_id.as_ref())?;
            } else {
                write_nullable_string(buf, self.group_instance_id.as_ref())?;
            }
        }
        if version >= 5 {
            write_compact_string(buf, &self.client_id)?;
        } else {
            write_string(buf, &self.client_id)?;
        }
        if version >= 5 {
            write_compact_string(buf, &self.client_host)?;
        } else {
            write_string(buf, &self.client_host)?;
        }
        if version >= 5 {
            write_compact_bytes(buf, &self.member_metadata)?;
        } else {
            write_bytes(buf, &self.member_metadata)?;
        }
        if version >= 5 {
            write_compact_bytes(buf, &self.member_assignment)?;
        } else {
            write_bytes(buf, &self.member_assignment)?;
        }
        if version >= 5 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
}
