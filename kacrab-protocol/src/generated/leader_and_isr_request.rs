//! Generated from LeaderAndIsrRequest.json - DO NOT EDIT
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
pub struct LeaderAndIsrRequestData {
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for LeaderAndIsrRequestData {
    fn default() -> Self {
        Self {
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl LeaderAndIsrRequestData {
    pub fn read(_buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 0 || version > -1 {
            return Err(UnsupportedVersion::new(4, version).into());
        }
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        Ok(Self {
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, _buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 0 || version > -1 {
            return Err(UnsupportedVersion::new(4, version).into());
        }
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        if version < 0 || version > -1 {
            return Err(UnsupportedVersion::new(4, version).into());
        }
        let len: usize = 0;
        Ok(len)
    }
}
