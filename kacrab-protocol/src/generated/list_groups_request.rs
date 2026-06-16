//! Generated from ListGroupsRequest.json - DO NOT EDIT
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
pub struct ListGroupsRequestData {
    /// The states of the groups we want to list. If empty, all groups are returned with their
    /// state.
    pub states_filter: Vec<KafkaString>,
    /// The types of the groups we want to list. If empty, all groups are returned with their type.
    pub types_filter: Vec<KafkaString>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for ListGroupsRequestData {
    fn default() -> Self {
        Self {
            states_filter: Vec::new(),
            types_filter: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl ListGroupsRequestData {
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 0 || version > 5 {
            return Err(UnsupportedVersion::new(16, version).into());
        }
        let mut states_filter = Vec::new();
        let mut types_filter = Vec::new();
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 4 {
            states_filter = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(read_compact_string(buf)?);
                }
                arr
            };
        }
        if version >= 5 {
            types_filter = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(read_compact_string(buf)?);
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
            states_filter,
            types_filter,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 0 || version > 5 {
            return Err(UnsupportedVersion::new(16, version).into());
        }
        if version >= 4 {
            write_compact_array_length(buf, self.states_filter.len() as i32);
            for el in &self.states_filter {
                write_compact_string(buf, el)?;
            }
        }
        if version >= 5 {
            write_compact_array_length(buf, self.types_filter.len() as i32);
            for el in &self.types_filter {
                write_compact_string(buf, el)?;
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
