//! Generated from ListTransactionsResponse.json - DO NOT EDIT
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
pub struct ListTransactionsResponseData {
    /// The duration in milliseconds for which the request was throttled due to a quota violation,
    /// or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// The error code, or 0 if there was no error.
    pub error_code: i16,
    /// Set of state filters provided in the request which were unknown to the transaction
    /// coordinator.
    pub unknown_state_filters: Vec<KafkaString>,
    /// The current state of the transaction for the transactional id.
    pub transaction_states: Vec<TransactionState>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for ListTransactionsResponseData {
    fn default() -> Self {
        Self {
            throttle_time_ms: 0_i32,
            error_code: 0_i16,
            unknown_state_filters: Vec::new(),
            transaction_states: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl ListTransactionsResponseData {
    pub fn with_throttle_time_ms(mut self, value: i32) -> Self {
        self.throttle_time_ms = value;
        self
    }
    pub fn with_error_code(mut self, value: i16) -> Self {
        self.error_code = value;
        self
    }
    pub fn with_unknown_state_filters(mut self, value: Vec<KafkaString>) -> Self {
        self.unknown_state_filters = value;
        self
    }
    pub fn with_transaction_states(mut self, value: Vec<TransactionState>) -> Self {
        self.transaction_states = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 0 || version > 2 {
            return Err(UnsupportedVersion::new(66, version).into());
        }
        let throttle_time_ms;
        let error_code;
        let unknown_state_filters;
        let transaction_states;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        throttle_time_ms = read_i32(buf)?;
        error_code = read_i16(buf)?;
        unknown_state_filters = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
            for _ in 0..len {
                arr.push(read_compact_string(buf)?);
            }
            arr
        };
        transaction_states = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
            for _ in 0..len {
                arr.push(TransactionState::read(buf, version)?);
            }
            arr
        };
        let tagged_fields = read_tagged_fields(buf)?;
        for field in &tagged_fields {
            match field.tag {
                _ => {
                    _unknown_tagged_fields.push(field.clone());
                },
            }
        }
        Ok(Self {
            throttle_time_ms,
            error_code,
            unknown_state_filters,
            transaction_states,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 0 || version > 2 {
            return Err(UnsupportedVersion::new(66, version).into());
        }
        write_i32(buf, self.throttle_time_ms);
        write_i16(buf, self.error_code);
        write_compact_array_length(buf, self.unknown_state_filters.len() as i32);
        for el in &self.unknown_state_filters {
            write_compact_string(buf, el)?;
        }
        write_compact_array_length(buf, self.transaction_states.len() as i32);
        for el in &self.transaction_states {
            el.write(buf, version)?;
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        if version < 0 || version > 2 {
            return Err(UnsupportedVersion::new(66, version).into());
        }
        let mut len: usize = 0;
        len += 4;
        len += 2;
        len += compact_array_length_len(self.unknown_state_filters.len() as i32);
        for el in &self.unknown_state_filters {
            len += compact_string_len(el)?;
        }
        len += compact_array_length_len(self.transaction_states.len() as i32);
        for el in &self.transaction_states {
            len += el.encoded_len(version)?;
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct TransactionState {
    /// The transactional id.
    pub transactional_id: KafkaString,
    /// The producer id.
    pub producer_id: i64,
    /// The current transaction state of the producer.
    pub transaction_state: KafkaString,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for TransactionState {
    fn default() -> Self {
        Self {
            transactional_id: KafkaString::default(),
            producer_id: 0_i64,
            transaction_state: KafkaString::default(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl TransactionState {
    pub fn with_transactional_id(mut self, value: KafkaString) -> Self {
        self.transactional_id = value;
        self
    }
    pub fn with_producer_id(mut self, value: i64) -> Self {
        self.producer_id = value;
        self
    }
    pub fn with_transaction_state(mut self, value: KafkaString) -> Self {
        self.transaction_state = value;
        self
    }
    pub fn read(buf: &mut Bytes, _version: i16) -> Result<Self> {
        let transactional_id;
        let producer_id;
        let transaction_state;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        transactional_id = read_compact_string(buf)?;
        producer_id = read_i64(buf)?;
        transaction_state = read_compact_string(buf)?;
        let tagged_fields = read_tagged_fields(buf)?;
        for field in &tagged_fields {
            match field.tag {
                _ => {
                    _unknown_tagged_fields.push(field.clone());
                },
            }
        }
        Ok(Self {
            transactional_id,
            producer_id,
            transaction_state,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, _version: i16) -> Result<()> {
        write_compact_string(buf, &self.transactional_id)?;
        write_i64(buf, self.producer_id);
        write_compact_string(buf, &self.transaction_state)?;
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
    pub fn encoded_len(&self, _version: i16) -> Result<usize> {
        let mut len: usize = 0;
        len += compact_string_len(&self.transactional_id)?;
        len += 8;
        len += compact_string_len(&self.transaction_state)?;
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
