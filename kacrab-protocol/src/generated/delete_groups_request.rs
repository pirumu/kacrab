//! Generated from DeleteGroupsRequest.json - DO NOT EDIT
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
pub struct DeleteGroupsRequestData {
    /// The group names to delete.
    pub groups_names: Vec<KafkaString>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for DeleteGroupsRequestData {
    fn default() -> Self {
        Self {
            groups_names: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl DeleteGroupsRequestData {
    pub fn with_groups_names(mut self, value: Vec<KafkaString>) -> Self {
        self.groups_names = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 0 || version > 2 {
            return Err(UnsupportedVersion::new(42, version).into());
        }
        let groups_names;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 2 {
            groups_names = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
                for _ in 0..len {
                    arr.push(read_compact_string(buf)?);
                }
                arr
            };
        } else {
            groups_names = {
                let len = read_array_length(buf)?;
                let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
                for _ in 0..len {
                    arr.push(read_string(buf)?);
                }
                arr
            };
        }
        if version >= 2 {
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
            groups_names,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 0 || version > 2 {
            return Err(UnsupportedVersion::new(42, version).into());
        }
        if version >= 2 {
            write_compact_array_length(buf, self.groups_names.len() as i32);
            for el in &self.groups_names {
                write_compact_string(buf, el)?;
            }
        } else {
            write_array_length(buf, self.groups_names.len() as i32);
            for el in &self.groups_names {
                write_string(buf, el)?;
            }
        }
        if version >= 2 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        if version < 0 || version > 2 {
            return Err(UnsupportedVersion::new(42, version).into());
        }
        let mut len: usize = 0;
        if version >= 2 {
            len += compact_array_length_len(self.groups_names.len() as i32);
            for el in &self.groups_names {
                len += compact_string_len(el)?;
            }
        } else {
            len += array_length_len();
            for el in &self.groups_names {
                len += string_len(el)?;
            }
        }
        if version >= 2 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            len += tagged_fields_len(&all_tags)?;
        }
        Ok(len)
    }
}
