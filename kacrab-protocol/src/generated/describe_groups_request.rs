//! Generated from DescribeGroupsRequest.json - DO NOT EDIT
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
pub struct DescribeGroupsRequestData {
    /// The names of the groups to describe.
    pub groups: Vec<KafkaString>,
    /// Whether to include authorized operations.
    pub include_authorized_operations: bool,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for DescribeGroupsRequestData {
    fn default() -> Self {
        Self {
            groups: Vec::new(),
            include_authorized_operations: false,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl DescribeGroupsRequestData {
    pub fn with_groups(mut self, value: Vec<KafkaString>) -> Self {
        self.groups = value;
        self
    }
    pub fn with_include_authorized_operations(mut self, value: bool) -> Self {
        self.include_authorized_operations = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 0 || version > 6 {
            return Err(UnsupportedVersion::new(15, version).into());
        }
        let groups;
        let mut include_authorized_operations = false;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 5 {
            groups = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(read_compact_string(buf)?);
                }
                arr
            };
        } else {
            groups = {
                let len = read_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(read_string(buf)?);
                }
                arr
            };
        }
        if version >= 3 {
            include_authorized_operations = read_bool(buf)?;
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
            groups,
            include_authorized_operations,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 0 || version > 6 {
            return Err(UnsupportedVersion::new(15, version).into());
        }
        if version >= 5 {
            write_compact_array_length(buf, self.groups.len() as i32);
            for el in &self.groups {
                write_compact_string(buf, el)?;
            }
        } else {
            write_array_length(buf, self.groups.len() as i32);
            for el in &self.groups {
                write_string(buf, el)?;
            }
        }
        if version >= 3 {
            write_bool(buf, self.include_authorized_operations);
        } else if self.include_authorized_operations != false {
            return Err(
                UnsupportedFieldVersion::new(15, "include_authorized_operations", version).into(),
            );
        }
        if version >= 5 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        if version < 0 || version > 6 {
            return Err(UnsupportedVersion::new(15, version).into());
        }
        let mut len: usize = 0;
        if version >= 5 {
            len += compact_array_length_len(self.groups.len() as i32);
            for el in &self.groups {
                len += compact_string_len(el)?;
            }
        } else {
            len += array_length_len();
            for el in &self.groups {
                len += string_len(el)?;
            }
        }
        if version >= 3 {
            len += 1;
        } else if self.include_authorized_operations != false {
            return Err(
                UnsupportedFieldVersion::new(15, "include_authorized_operations", version).into(),
            );
        }
        if version >= 5 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            len += tagged_fields_len(&all_tags)?;
        }
        Ok(len)
    }
}
