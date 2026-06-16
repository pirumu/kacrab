//! Generated from ShareGroupDescribeRequest.json - DO NOT EDIT
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
pub struct ShareGroupDescribeRequestData {
    /// The ids of the groups to describe.
    pub group_ids: Vec<KafkaString>,
    /// Whether to include authorized operations.
    pub include_authorized_operations: bool,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for ShareGroupDescribeRequestData {
    fn default() -> Self {
        Self {
            group_ids: Vec::new(),
            include_authorized_operations: false,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl ShareGroupDescribeRequestData {
    pub fn with_group_ids(mut self, value: Vec<KafkaString>) -> Self {
        self.group_ids = value;
        self
    }
    pub fn with_include_authorized_operations(mut self, value: bool) -> Self {
        self.include_authorized_operations = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 1 || version > 1 {
            return Err(UnsupportedVersion::new(77, version).into());
        }
        let group_ids;
        let include_authorized_operations;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        group_ids = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(len.max(0) as usize);
            for _ in 0..len {
                arr.push(read_compact_string(buf)?);
            }
            arr
        };
        include_authorized_operations = read_bool(buf)?;
        let tagged_fields = read_tagged_fields(buf)?;
        for field in &tagged_fields {
            match field.tag {
                _ => {
                    _unknown_tagged_fields.push(field.clone());
                },
            }
        }
        Ok(Self {
            group_ids,
            include_authorized_operations,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 1 || version > 1 {
            return Err(UnsupportedVersion::new(77, version).into());
        }
        write_compact_array_length(buf, self.group_ids.len() as i32);
        for el in &self.group_ids {
            write_compact_string(buf, el)?;
        }
        write_bool(buf, self.include_authorized_operations);
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        if version < 1 || version > 1 {
            return Err(UnsupportedVersion::new(77, version).into());
        }
        let mut len: usize = 0;
        len += compact_array_length_len(self.group_ids.len() as i32);
        for el in &self.group_ids {
            len += compact_string_len(el)?;
        }
        len += 1;
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
