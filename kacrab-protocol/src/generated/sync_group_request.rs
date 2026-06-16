//! Generated from SyncGroupRequest.json - DO NOT EDIT
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
pub struct SyncGroupRequestData {
    /// The unique group identifier.
    pub group_id: KafkaString,
    /// The generation of the group.
    pub generation_id: i32,
    /// The member ID assigned by the group.
    pub member_id: KafkaString,
    /// The unique identifier of the consumer instance provided by end user.
    pub group_instance_id: Option<KafkaString>,
    /// The group protocol type.
    pub protocol_type: Option<KafkaString>,
    /// The group protocol name.
    pub protocol_name: Option<KafkaString>,
    /// Each assignment.
    pub assignments: Vec<SyncGroupRequestAssignment>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for SyncGroupRequestData {
    fn default() -> Self {
        Self {
            group_id: KafkaString::default(),
            generation_id: 0_i32,
            member_id: KafkaString::default(),
            group_instance_id: None,
            protocol_type: None,
            protocol_name: None,
            assignments: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl SyncGroupRequestData {
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 0 || version > 5 {
            return Err(UnsupportedVersion::new(14, version).into());
        }
        let group_id;
        let generation_id;
        let member_id;
        let mut group_instance_id = None;
        let mut protocol_type = None;
        let mut protocol_name = None;
        let assignments;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 4 {
            group_id = read_compact_string(buf)?;
        } else {
            group_id = read_string(buf)?;
        }
        generation_id = read_i32(buf)?;
        if version >= 4 {
            member_id = read_compact_string(buf)?;
        } else {
            member_id = read_string(buf)?;
        }
        if version >= 3 {
            if version >= 4 {
                group_instance_id = read_compact_nullable_string(buf)?;
            } else {
                group_instance_id = read_nullable_string(buf)?;
            }
        }
        if version >= 5 {
            protocol_type = read_compact_nullable_string(buf)?;
        }
        if version >= 5 {
            protocol_name = read_compact_nullable_string(buf)?;
        }
        if version >= 4 {
            assignments = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(SyncGroupRequestAssignment::read(buf, version)?);
                }
                arr
            };
        } else {
            assignments = {
                let len = read_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(SyncGroupRequestAssignment::read(buf, version)?);
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
            group_id,
            generation_id,
            member_id,
            group_instance_id,
            protocol_type,
            protocol_name,
            assignments,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 0 || version > 5 {
            return Err(UnsupportedVersion::new(14, version).into());
        }
        if version >= 4 {
            write_compact_string(buf, &self.group_id)?;
        } else {
            write_string(buf, &self.group_id)?;
        }
        write_i32(buf, self.generation_id);
        if version >= 4 {
            write_compact_string(buf, &self.member_id)?;
        } else {
            write_string(buf, &self.member_id)?;
        }
        if version >= 3 {
            if version >= 4 {
                write_compact_nullable_string(buf, self.group_instance_id.as_ref())?;
            } else {
                write_nullable_string(buf, self.group_instance_id.as_ref())?;
            }
        }
        if version >= 5 {
            write_compact_nullable_string(buf, self.protocol_type.as_ref())?;
        }
        if version >= 5 {
            write_compact_nullable_string(buf, self.protocol_name.as_ref())?;
        }
        if version >= 4 {
            write_compact_array_length(buf, self.assignments.len() as i32);
            for el in &self.assignments {
                el.write(buf, version)?;
            }
        } else {
            write_array_length(buf, self.assignments.len() as i32);
            for el in &self.assignments {
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
pub struct SyncGroupRequestAssignment {
    /// The ID of the member to assign.
    pub member_id: KafkaString,
    /// The member assignment.
    pub assignment: Bytes,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for SyncGroupRequestAssignment {
    fn default() -> Self {
        Self {
            member_id: KafkaString::default(),
            assignment: Bytes::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl SyncGroupRequestAssignment {
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let member_id;
        let assignment;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 4 {
            member_id = read_compact_string(buf)?;
        } else {
            member_id = read_string(buf)?;
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
            member_id,
            assignment,
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
