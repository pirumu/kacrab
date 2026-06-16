//! Generated from JoinGroupResponse.json - DO NOT EDIT
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
pub struct JoinGroupResponseData {
    /// The duration in milliseconds for which the request was throttled due to a quota violation,
    /// or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// The error code, or 0 if there was no error.
    pub error_code: i16,
    /// The generation ID of the group.
    pub generation_id: i32,
    /// The group protocol name.
    pub protocol_type: Option<KafkaString>,
    /// The group protocol selected by the coordinator.
    pub protocol_name: Option<KafkaString>,
    /// The leader of the group.
    pub leader: KafkaString,
    /// True if the leader must skip running the assignment.
    pub skip_assignment: bool,
    /// The member ID assigned by the group coordinator.
    pub member_id: KafkaString,
    /// The group members.
    pub members: Vec<JoinGroupResponseMember>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for JoinGroupResponseData {
    fn default() -> Self {
        Self {
            throttle_time_ms: 0_i32,
            error_code: 0_i16,
            generation_id: -1i32,
            protocol_type: None,
            protocol_name: None,
            leader: KafkaString::default(),
            skip_assignment: false,
            member_id: KafkaString::default(),
            members: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl JoinGroupResponseData {
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 0 || version > 9 {
            return Err(UnsupportedVersion::new(11, version).into());
        }
        let mut throttle_time_ms = 0_i32;
        let error_code;
        let generation_id;
        let mut protocol_type = None;
        let protocol_name;
        let leader;
        let mut skip_assignment = false;
        let member_id;
        let members;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 2 {
            throttle_time_ms = read_i32(buf)?;
        }
        error_code = read_i16(buf)?;
        generation_id = read_i32(buf)?;
        if version >= 7 {
            protocol_type = read_compact_nullable_string(buf)?;
        }
        if version >= 7 {
            protocol_name = read_compact_nullable_string(buf)?;
        } else {
            if version >= 6 {
                protocol_name = Some(read_compact_string(buf)?);
            } else {
                protocol_name = Some(read_string(buf)?);
            }
        }
        if version >= 6 {
            leader = read_compact_string(buf)?;
        } else {
            leader = read_string(buf)?;
        }
        if version >= 9 {
            skip_assignment = read_bool(buf)?;
        }
        if version >= 6 {
            member_id = read_compact_string(buf)?;
        } else {
            member_id = read_string(buf)?;
        }
        if version >= 6 {
            members = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(JoinGroupResponseMember::read(buf, version)?);
                }
                arr
            };
        } else {
            members = {
                let len = read_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(JoinGroupResponseMember::read(buf, version)?);
                }
                arr
            };
        }
        if version >= 6 {
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
            generation_id,
            protocol_type,
            protocol_name,
            leader,
            skip_assignment,
            member_id,
            members,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 0 || version > 9 {
            return Err(UnsupportedVersion::new(11, version).into());
        }
        if version >= 2 {
            write_i32(buf, self.throttle_time_ms);
        }
        write_i16(buf, self.error_code);
        write_i32(buf, self.generation_id);
        if version >= 7 {
            write_compact_nullable_string(buf, self.protocol_type.as_ref())?;
        }
        if version >= 7 {
            write_compact_nullable_string(buf, self.protocol_name.as_ref())?;
        } else {
            {
                let _nn_default = KafkaString::default();
                let _nn_val = self.protocol_name.as_ref().unwrap_or(&_nn_default);
                if version >= 6 {
                    write_compact_string(buf, _nn_val)?;
                } else {
                    write_string(buf, _nn_val)?;
                }
            }
        }
        if version >= 6 {
            write_compact_string(buf, &self.leader)?;
        } else {
            write_string(buf, &self.leader)?;
        }
        if version >= 9 {
            write_bool(buf, self.skip_assignment);
        }
        if version >= 6 {
            write_compact_string(buf, &self.member_id)?;
        } else {
            write_string(buf, &self.member_id)?;
        }
        if version >= 6 {
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
        if version >= 6 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct JoinGroupResponseMember {
    /// The group member ID.
    pub member_id: KafkaString,
    /// The unique identifier of the consumer instance provided by end user.
    pub group_instance_id: Option<KafkaString>,
    /// The group member metadata.
    pub metadata: Bytes,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for JoinGroupResponseMember {
    fn default() -> Self {
        Self {
            member_id: KafkaString::default(),
            group_instance_id: None,
            metadata: Bytes::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl JoinGroupResponseMember {
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let member_id;
        let mut group_instance_id = None;
        let metadata;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 6 {
            member_id = read_compact_string(buf)?;
        } else {
            member_id = read_string(buf)?;
        }
        if version >= 5 {
            if version >= 6 {
                group_instance_id = read_compact_nullable_string(buf)?;
            } else {
                group_instance_id = read_nullable_string(buf)?;
            }
        }
        if version >= 6 {
            metadata = read_compact_bytes(buf)?;
        } else {
            metadata = read_bytes(buf)?;
        }
        if version >= 6 {
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
            metadata,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version >= 6 {
            write_compact_string(buf, &self.member_id)?;
        } else {
            write_string(buf, &self.member_id)?;
        }
        if version >= 5 {
            if version >= 6 {
                write_compact_nullable_string(buf, self.group_instance_id.as_ref())?;
            } else {
                write_nullable_string(buf, self.group_instance_id.as_ref())?;
            }
        }
        if version >= 6 {
            write_compact_bytes(buf, &self.metadata)?;
        } else {
            write_bytes(buf, &self.metadata)?;
        }
        if version >= 6 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
}
