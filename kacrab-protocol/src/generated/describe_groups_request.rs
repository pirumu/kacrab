//! Generated from DescribeGroupsRequest.json - DO NOT EDIT
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
        }
        if version >= 5 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
}
