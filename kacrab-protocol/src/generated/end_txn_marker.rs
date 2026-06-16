//! Generated from EndTxnMarker.json - DO NOT EDIT
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
pub struct EndTxnMarkerData {
    /// The coordinator epoch when appending the record
    pub coordinator_epoch: i32,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for EndTxnMarkerData {
    fn default() -> Self {
        Self {
            coordinator_epoch: 0_i32,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl EndTxnMarkerData {
    pub fn read(buf: &mut Bytes, _version: i16) -> Result<Self> {
        let coordinator_epoch;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        coordinator_epoch = read_i32(buf)?;
        Ok(Self {
            coordinator_epoch,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, _version: i16) -> Result<()> {
        write_i32(buf, self.coordinator_epoch);
        Ok(())
    }
}
