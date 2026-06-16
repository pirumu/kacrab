//! Generated from InitProducerIdResponse.json - DO NOT EDIT
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
pub struct InitProducerIdResponseData {
    /// The duration in milliseconds for which the request was throttled due to a quota violation,
    /// or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// The error code, or 0 if there was no error.
    pub error_code: i16,
    /// The current producer id.
    pub producer_id: i64,
    /// The current epoch associated with the producer id.
    pub producer_epoch: i16,
    /// The producer id for ongoing transaction when KeepPreparedTxn is used, -1 if there is no
    /// transaction ongoing.
    pub ongoing_txn_producer_id: i64,
    /// The epoch associated with the  producer id for ongoing transaction when KeepPreparedTxn is
    /// used, -1 if there is no transaction ongoing.
    pub ongoing_txn_producer_epoch: i16,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for InitProducerIdResponseData {
    fn default() -> Self {
        Self {
            throttle_time_ms: 0_i32,
            error_code: 0_i16,
            producer_id: -1i64,
            producer_epoch: 0_i16,
            ongoing_txn_producer_id: -1i64,
            ongoing_txn_producer_epoch: -1i16,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl InitProducerIdResponseData {
    pub fn with_throttle_time_ms(mut self, value: i32) -> Self {
        self.throttle_time_ms = value;
        self
    }
    pub fn with_error_code(mut self, value: i16) -> Self {
        self.error_code = value;
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
    pub fn with_ongoing_txn_producer_id(mut self, value: i64) -> Self {
        self.ongoing_txn_producer_id = value;
        self
    }
    pub fn with_ongoing_txn_producer_epoch(mut self, value: i16) -> Self {
        self.ongoing_txn_producer_epoch = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 0 || version > 6 {
            return Err(UnsupportedVersion::new(22, version).into());
        }
        let throttle_time_ms;
        let error_code;
        let producer_id;
        let producer_epoch;
        let mut ongoing_txn_producer_id = -1i64;
        let mut ongoing_txn_producer_epoch = -1i16;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        throttle_time_ms = read_i32(buf)?;
        error_code = read_i16(buf)?;
        producer_id = read_i64(buf)?;
        producer_epoch = read_i16(buf)?;
        if version >= 6 {
            ongoing_txn_producer_id = read_i64(buf)?;
        }
        if version >= 6 {
            ongoing_txn_producer_epoch = read_i16(buf)?;
        }
        if version >= 2 {
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
            throttle_time_ms,
            error_code,
            producer_id,
            producer_epoch,
            ongoing_txn_producer_id,
            ongoing_txn_producer_epoch,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 0 || version > 6 {
            return Err(UnsupportedVersion::new(22, version).into());
        }
        write_i32(buf, self.throttle_time_ms);
        write_i16(buf, self.error_code);
        write_i64(buf, self.producer_id);
        write_i16(buf, self.producer_epoch);
        if version >= 6 {
            write_i64(buf, self.ongoing_txn_producer_id);
        } else if self.ongoing_txn_producer_id != -1i64 {
            return Err(
                UnsupportedFieldVersion::new(22, "ongoing_txn_producer_id", version).into(),
            );
        }
        if version >= 6 {
            write_i16(buf, self.ongoing_txn_producer_epoch);
        } else if self.ongoing_txn_producer_epoch != -1i16 {
            return Err(
                UnsupportedFieldVersion::new(22, "ongoing_txn_producer_epoch", version).into(),
            );
        }
        if version >= 2 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        if version < 0 || version > 6 {
            return Err(UnsupportedVersion::new(22, version).into());
        }
        let mut len: usize = 0;
        len += 4;
        len += 2;
        len += 8;
        len += 2;
        if version >= 6 {
            len += 8;
        } else if self.ongoing_txn_producer_id != -1i64 {
            return Err(
                UnsupportedFieldVersion::new(22, "ongoing_txn_producer_id", version).into(),
            );
        }
        if version >= 6 {
            len += 2;
        } else if self.ongoing_txn_producer_epoch != -1i16 {
            return Err(
                UnsupportedFieldVersion::new(22, "ongoing_txn_producer_epoch", version).into(),
            );
        }
        if version >= 2 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            len += tagged_fields_len(&all_tags)?;
        }
        Ok(len)
    }
}
