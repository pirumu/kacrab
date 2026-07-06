//! Generated from DescribeTransactionsResponse.json - DO NOT EDIT
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
pub struct DescribeTransactionsResponseData {
    /// The duration in milliseconds for which the request was throttled due to a quota violation,
    /// or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// The current state of the transaction.
    pub transaction_states: Vec<TransactionState>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for DescribeTransactionsResponseData {
    fn default() -> Self {
        Self {
            throttle_time_ms: 0_i32,
            transaction_states: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl DescribeTransactionsResponseData {
    pub fn with_throttle_time_ms(mut self, value: i32) -> Self {
        self.throttle_time_ms = value;
        self
    }
    pub fn with_transaction_states(mut self, value: Vec<TransactionState>) -> Self {
        self.transaction_states = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 0 || version > 0 {
            return Err(UnsupportedVersion::new(65, version).into());
        }
        let throttle_time_ms;
        let transaction_states;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        throttle_time_ms = read_i32(buf)?;
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
            transaction_states,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 0 || version > 0 {
            return Err(UnsupportedVersion::new(65, version).into());
        }
        write_i32(buf, self.throttle_time_ms);
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
        if version < 0 || version > 0 {
            return Err(UnsupportedVersion::new(65, version).into());
        }
        let mut len: usize = 0;
        len += 4;
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
    /// The error code.
    pub error_code: i16,
    /// The transactional id.
    pub transactional_id: KafkaString,
    /// The current transaction state of the producer.
    pub transaction_state: KafkaString,
    /// The timeout in milliseconds for the transaction.
    pub transaction_timeout_ms: i32,
    /// The start time of the transaction in milliseconds.
    pub transaction_start_time_ms: i64,
    /// The current producer id associated with the transaction.
    pub producer_id: i64,
    /// The current epoch associated with the producer id.
    pub producer_epoch: i16,
    /// The set of partitions included in the current transaction (if active). When a transaction
    /// is preparing to commit or abort, this will include only partitions which do not have
    /// markers.
    pub topics: Vec<TopicData>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for TransactionState {
    fn default() -> Self {
        Self {
            error_code: 0_i16,
            transactional_id: KafkaString::default(),
            transaction_state: KafkaString::default(),
            transaction_timeout_ms: 0_i32,
            transaction_start_time_ms: 0_i64,
            producer_id: 0_i64,
            producer_epoch: 0_i16,
            topics: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl TransactionState {
    pub fn with_error_code(mut self, value: i16) -> Self {
        self.error_code = value;
        self
    }
    pub fn with_transactional_id(mut self, value: KafkaString) -> Self {
        self.transactional_id = value;
        self
    }
    pub fn with_transaction_state(mut self, value: KafkaString) -> Self {
        self.transaction_state = value;
        self
    }
    pub fn with_transaction_timeout_ms(mut self, value: i32) -> Self {
        self.transaction_timeout_ms = value;
        self
    }
    pub fn with_transaction_start_time_ms(mut self, value: i64) -> Self {
        self.transaction_start_time_ms = value;
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
    pub fn with_topics(mut self, value: Vec<TopicData>) -> Self {
        self.topics = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let error_code;
        let transactional_id;
        let transaction_state;
        let transaction_timeout_ms;
        let transaction_start_time_ms;
        let producer_id;
        let producer_epoch;
        let topics;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        error_code = read_i16(buf)?;
        transactional_id = read_compact_string(buf)?;
        transaction_state = read_compact_string(buf)?;
        transaction_timeout_ms = read_i32(buf)?;
        transaction_start_time_ms = read_i64(buf)?;
        producer_id = read_i64(buf)?;
        producer_epoch = read_i16(buf)?;
        topics = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
            for _ in 0..len {
                arr.push(TopicData::read(buf, version)?);
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
            error_code,
            transactional_id,
            transaction_state,
            transaction_timeout_ms,
            transaction_start_time_ms,
            producer_id,
            producer_epoch,
            topics,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        write_i16(buf, self.error_code);
        write_compact_string(buf, &self.transactional_id)?;
        write_compact_string(buf, &self.transaction_state)?;
        write_i32(buf, self.transaction_timeout_ms);
        write_i64(buf, self.transaction_start_time_ms);
        write_i64(buf, self.producer_id);
        write_i16(buf, self.producer_epoch);
        write_compact_array_length(buf, self.topics.len() as i32);
        for el in &self.topics {
            el.write(buf, version)?;
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        let mut len: usize = 0;
        len += 2;
        len += compact_string_len(&self.transactional_id)?;
        len += compact_string_len(&self.transaction_state)?;
        len += 4;
        len += 8;
        len += 8;
        len += 2;
        len += compact_array_length_len(self.topics.len() as i32);
        for el in &self.topics {
            len += el.encoded_len(version)?;
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct TopicData {
    /// The topic name.
    pub topic: KafkaString,
    /// The partition ids included in the current transaction.
    pub partitions: Vec<i32>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for TopicData {
    fn default() -> Self {
        Self {
            topic: KafkaString::default(),
            partitions: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl TopicData {
    pub fn with_topic(mut self, value: KafkaString) -> Self {
        self.topic = value;
        self
    }
    pub fn with_partitions(mut self, value: Vec<i32>) -> Self {
        self.partitions = value;
        self
    }
    pub fn read(buf: &mut Bytes, _version: i16) -> Result<Self> {
        let topic;
        let partitions;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        topic = read_compact_string(buf)?;
        partitions = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
            for _ in 0..len {
                arr.push(read_i32(buf)?);
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
            topic,
            partitions,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, _version: i16) -> Result<()> {
        write_compact_string(buf, &self.topic)?;
        write_compact_array_length(buf, self.partitions.len() as i32);
        for el in &self.partitions {
            write_i32(buf, *el);
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
    pub fn encoded_len(&self, _version: i16) -> Result<usize> {
        let mut len: usize = 0;
        len += compact_string_len(&self.topic)?;
        len += compact_array_length_len(self.partitions.len() as i32);
        len += self.partitions.len() * 4usize;
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
