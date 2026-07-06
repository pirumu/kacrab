//! Generated from LeaveGroupRequest.json - DO NOT EDIT
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
pub struct LeaveGroupRequestData {
    /// The ID of the group to leave.
    pub group_id: KafkaString,
    /// The member ID to remove from the group.
    pub member_id: KafkaString,
    /// List of leaving member identities.
    pub members: Vec<MemberIdentity>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for LeaveGroupRequestData {
    fn default() -> Self {
        Self {
            group_id: KafkaString::default(),
            member_id: KafkaString::default(),
            members: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl LeaveGroupRequestData {
    pub fn with_group_id(mut self, value: KafkaString) -> Self {
        self.group_id = value;
        self
    }
    pub fn with_member_id(mut self, value: KafkaString) -> Self {
        self.member_id = value;
        self
    }
    pub fn with_members(mut self, value: Vec<MemberIdentity>) -> Self {
        self.members = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 0 || version > 5 {
            return Err(UnsupportedVersion::new(13, version).into());
        }
        let group_id;
        let mut member_id = KafkaString::default();
        let mut members = Vec::new();
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 4 {
            group_id = read_compact_string(buf)?;
        } else {
            group_id = read_string(buf)?;
        }
        if version <= 2 {
            member_id = read_string(buf)?;
        }
        if version >= 3 {
            if version >= 4 {
                members = {
                    let len = read_compact_array_length(buf)?;
                    let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
                    for _ in 0..len {
                        arr.push(MemberIdentity::read(buf, version)?);
                    }
                    arr
                };
            } else {
                members = {
                    let len = read_array_length(buf)?;
                    let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
                    for _ in 0..len {
                        arr.push(MemberIdentity::read(buf, version)?);
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
            group_id,
            member_id,
            members,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 0 || version > 5 {
            return Err(UnsupportedVersion::new(13, version).into());
        }
        if version >= 4 {
            write_compact_string(buf, &self.group_id)?;
        } else {
            write_string(buf, &self.group_id)?;
        }
        if version <= 2 {
            write_string(buf, &self.member_id)?;
        } else if self.member_id != KafkaString::default() {
            return Err(UnsupportedFieldVersion::new(13, "member_id", version).into());
        }
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
        } else if self.members != Vec::new() {
            return Err(UnsupportedFieldVersion::new(13, "members", version).into());
        }
        if version >= 4 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        if version < 0 || version > 5 {
            return Err(UnsupportedVersion::new(13, version).into());
        }
        let mut len: usize = 0;
        if version >= 4 {
            len += compact_string_len(&self.group_id)?;
        } else {
            len += string_len(&self.group_id)?;
        }
        if version <= 2 {
            len += string_len(&self.member_id)?;
        } else if self.member_id != KafkaString::default() {
            return Err(UnsupportedFieldVersion::new(13, "member_id", version).into());
        }
        if version >= 3 {
            if version >= 4 {
                len += compact_array_length_len(self.members.len() as i32);
                for el in &self.members {
                    len += el.encoded_len(version)?;
                }
            } else {
                len += array_length_len();
                for el in &self.members {
                    len += el.encoded_len(version)?;
                }
            }
        } else if self.members != Vec::new() {
            return Err(UnsupportedFieldVersion::new(13, "members", version).into());
        }
        if version >= 4 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            len += tagged_fields_len(&all_tags)?;
        }
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct MemberIdentity {
    /// The member ID to remove from the group.
    pub member_id: KafkaString,
    /// The group instance ID to remove from the group.
    pub group_instance_id: Option<KafkaString>,
    /// The reason why the member left the group.
    pub reason: Option<KafkaString>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for MemberIdentity {
    fn default() -> Self {
        Self {
            member_id: KafkaString::default(),
            group_instance_id: None,
            reason: None,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl MemberIdentity {
    pub fn with_member_id(mut self, value: KafkaString) -> Self {
        self.member_id = value;
        self
    }
    pub fn with_group_instance_id(mut self, value: Option<KafkaString>) -> Self {
        self.group_instance_id = value;
        self
    }
    pub fn with_reason(mut self, value: Option<KafkaString>) -> Self {
        self.reason = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let member_id;
        let group_instance_id;
        let mut reason = None;
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
        if version >= 5 {
            reason = read_compact_nullable_string(buf)?;
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
            member_id,
            group_instance_id,
            reason,
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
        if version >= 5 {
            write_compact_nullable_string(buf, self.reason.as_ref())?;
        } else if self.reason != None {
            return Err(UnsupportedFieldVersion::new(13, "reason", version).into());
        }
        if version >= 4 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        let mut len: usize = 0;
        if version >= 4 {
            len += compact_string_len(&self.member_id)?;
        } else {
            len += string_len(&self.member_id)?;
        }
        if version >= 4 {
            len += compact_nullable_string_len(self.group_instance_id.as_ref())?;
        } else {
            len += nullable_string_len(self.group_instance_id.as_ref())?;
        }
        if version >= 5 {
            len += compact_nullable_string_len(self.reason.as_ref())?;
        } else if self.reason != None {
            return Err(UnsupportedFieldVersion::new(13, "reason", version).into());
        }
        if version >= 4 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            len += tagged_fields_len(&all_tags)?;
        }
        Ok(len)
    }
}
