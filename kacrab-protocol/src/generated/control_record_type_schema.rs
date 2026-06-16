//! Generated from ControlRecordTypeSchema.json - DO NOT EDIT
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
pub struct ControlRecordTypeSchemaData {
    /// The type of the control record, such as commit or abort
    pub r#type: i16,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for ControlRecordTypeSchemaData {
    fn default() -> Self {
        Self {
            r#type: 0_i16,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl ControlRecordTypeSchemaData {
    pub fn read(buf: &mut Bytes, _version: i16) -> Result<Self> {
        let r#type;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        r#type = read_i16(buf)?;
        Ok(Self {
            r#type,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, _version: i16) -> Result<()> {
        write_i16(buf, self.r#type);
        Ok(())
    }
}
