//! Generated from EndTxnRequest.json - DO NOT EDIT
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
pub struct EndTxnRequestData {
    /// The ID of the transaction to end.
    pub transactional_id: KafkaString,
    /// The producer ID.
    pub producer_id: i64,
    /// The current epoch associated with the producer.
    pub producer_epoch: i16,
    /// True if the transaction was committed, false if it was aborted.
    pub committed: bool,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for EndTxnRequestData {
    fn default() -> Self {
        Self {
            transactional_id: KafkaString::default(),
            producer_id: 0_i64,
            producer_epoch: 0_i16,
            committed: false,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl EndTxnRequestData {
    pub fn with_transactional_id(mut self, value: KafkaString) -> Self {
        self.transactional_id = value;
        self
    }
    pub fn with_producer_id(mut self, value: i64) -> Self {
        self.producer_id = value;
        self
    }
    pub fn with_producer_epoch(mut self, value: i16) -> Self {
        self.producer_epoch = value;
        self
    }
    pub fn with_committed(mut self, value: bool) -> Self {
        self.committed = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 0 || version > 5 {
            return Err(UnsupportedVersion::new(26, version).into());
        }
        let transactional_id;
        let producer_id;
        let producer_epoch;
        let committed;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 3 {
            transactional_id = read_compact_string(buf)?;
        } else {
            transactional_id = read_string(buf)?;
        }
        producer_id = read_i64(buf)?;
        producer_epoch = read_i16(buf)?;
        committed = read_bool(buf)?;
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
            transactional_id,
            producer_id,
            producer_epoch,
            committed,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 0 || version > 5 {
            return Err(UnsupportedVersion::new(26, version).into());
        }
        if version >= 3 {
            write_compact_string(buf, &self.transactional_id)?;
        } else {
            write_string(buf, &self.transactional_id)?;
        }
        write_i64(buf, self.producer_id);
        write_i16(buf, self.producer_epoch);
        write_bool(buf, self.committed);
        if version >= 3 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        if version < 0 || version > 5 {
            return Err(UnsupportedVersion::new(26, version).into());
        }
        let mut len: usize = 0;
        if version >= 3 {
            len += compact_string_len(&self.transactional_id)?;
        } else {
            len += string_len(&self.transactional_id)?;
        }
        len += 8;
        len += 2;
        len += 1;
        if version >= 3 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            len += tagged_fields_len(&all_tags)?;
        }
        Ok(len)
    }
}
