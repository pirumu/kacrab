//! Generated from ResponseHeader.json - DO NOT EDIT
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
pub struct ResponseHeaderData {
    /// The correlation ID of this response.
    pub correlation_id: i32,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for ResponseHeaderData {
    fn default() -> Self {
        Self {
            correlation_id: 0_i32,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl ResponseHeaderData {
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let correlation_id;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        correlation_id = read_i32(buf)?;
        if version >= 1 {
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
            correlation_id,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        write_i32(buf, self.correlation_id);
        if version >= 1 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
}
