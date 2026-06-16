//! Generated from InitProducerIdRequest.json - DO NOT EDIT
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
pub struct InitProducerIdRequestData {
    /// The transactional id, or null if the producer is not transactional.
    pub transactional_id: Option<KafkaString>,
    /// The time in ms to wait before aborting idle transactions sent by this producer. This is
    /// only relevant if a TransactionalId has been defined.
    pub transaction_timeout_ms: i32,
    /// The producer id. This is used to disambiguate requests if a transactional id is reused
    /// following its expiration.
    pub producer_id: i64,
    /// The producer's current epoch. This will be checked against the producer epoch on the
    /// broker, and the request will return an error if they do not match.
    pub producer_epoch: i16,
    /// True if the client wants to enable two-phase commit (2PC) protocol for transactions.
    pub enable2_pc: bool,
    /// True if the client wants to keep the currently ongoing transaction instead of aborting it.
    pub keep_prepared_txn: bool,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for InitProducerIdRequestData {
    fn default() -> Self {
        Self {
            transactional_id: None,
            transaction_timeout_ms: 0_i32,
            producer_id: -1i64,
            producer_epoch: -1i16,
            enable2_pc: false,
            keep_prepared_txn: false,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl InitProducerIdRequestData {
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 0 || version > 6 {
            return Err(UnsupportedVersion::new(22, version).into());
        }
        let transactional_id;
        let transaction_timeout_ms;
        let mut producer_id = -1i64;
        let mut producer_epoch = -1i16;
        let mut enable2_pc = false;
        let mut keep_prepared_txn = false;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 2 {
            transactional_id = read_compact_nullable_string(buf)?;
        } else {
            transactional_id = read_nullable_string(buf)?;
        }
        transaction_timeout_ms = read_i32(buf)?;
        if version >= 3 {
            producer_id = read_i64(buf)?;
        }
        if version >= 3 {
            producer_epoch = read_i16(buf)?;
        }
        if version >= 6 {
            enable2_pc = read_bool(buf)?;
        }
        if version >= 6 {
            keep_prepared_txn = read_bool(buf)?;
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
            transactional_id,
            transaction_timeout_ms,
            producer_id,
            producer_epoch,
            enable2_pc,
            keep_prepared_txn,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 0 || version > 6 {
            return Err(UnsupportedVersion::new(22, version).into());
        }
        if version >= 2 {
            write_compact_nullable_string(buf, self.transactional_id.as_ref())?;
        } else {
            write_nullable_string(buf, self.transactional_id.as_ref())?;
        }
        write_i32(buf, self.transaction_timeout_ms);
        if version >= 3 {
            write_i64(buf, self.producer_id);
        }
        if version >= 3 {
            write_i16(buf, self.producer_epoch);
        }
        if version >= 6 {
            write_bool(buf, self.enable2_pc);
        }
        if version >= 6 {
            write_bool(buf, self.keep_prepared_txn);
        }
        if version >= 2 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
}
