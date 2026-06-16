//! Generated from SnapshotHeaderRecord.json - DO NOT EDIT
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
pub struct SnapshotHeaderRecordData {
    /// The version of the snapshot header record.
    pub version: i16,
    /// The append time of the last record from the log contained in this snapshot.
    pub last_contained_log_timestamp: i64,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for SnapshotHeaderRecordData {
    fn default() -> Self {
        Self {
            version: 0_i16,
            last_contained_log_timestamp: 0_i64,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl SnapshotHeaderRecordData {
    pub fn read(buf: &mut Bytes, _version: i16) -> Result<Self> {
        let version_;
        let last_contained_log_timestamp;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        version_ = read_i16(buf)?;
        last_contained_log_timestamp = read_i64(buf)?;
        let tagged_fields = read_tagged_fields(buf)?;
        for field in &tagged_fields {
            match field.tag {
                _ => {
                    _unknown_tagged_fields.push(field.clone());
                },
            }
        }
        Ok(Self {
            version: version_,
            last_contained_log_timestamp,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, _version: i16) -> Result<()> {
        write_i16(buf, self.version);
        write_i64(buf, self.last_contained_log_timestamp);
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
}
