//! Generated from ListConfigResourcesRequest.json - DO NOT EDIT
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
pub struct ListConfigResourcesRequestData {
    /// The list of resource type. If the list is empty, it uses default supported config resource
    /// types.
    pub resource_types: Vec<i8>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for ListConfigResourcesRequestData {
    fn default() -> Self {
        Self {
            resource_types: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl ListConfigResourcesRequestData {
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 0 || version > 1 {
            return Err(UnsupportedVersion::new(74, version).into());
        }
        let mut resource_types = Vec::new();
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 1 {
            resource_types = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(read_i8(buf)?);
                }
                arr
            };
        }
        let tagged_fields = read_tagged_fields(buf)?;
        for field in &tagged_fields {
            match field.tag {
                _ => {
                    _unknown_tagged_fields.push(field.clone());
                },
            }
        }
        Ok(Self {
            resource_types,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 0 || version > 1 {
            return Err(UnsupportedVersion::new(74, version).into());
        }
        if version >= 1 {
            write_compact_array_length(buf, self.resource_types.len() as i32);
            for el in &self.resource_types {
                write_i8(buf, *el);
            }
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
}
