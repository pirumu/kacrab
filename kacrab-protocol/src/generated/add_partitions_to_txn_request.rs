//! Generated from AddPartitionsToTxnRequest.json - DO NOT EDIT
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
pub struct AddPartitionsToTxnRequestData {
    /// List of transactions to add partitions to.
    pub transactions: Vec<AddPartitionsToTxnTransaction>,
    /// The transactional id corresponding to the transaction.
    pub v3_and_below_transactional_id: KafkaString,
    /// Current producer id in use by the transactional id.
    pub v3_and_below_producer_id: i64,
    /// Current epoch associated with the producer id.
    pub v3_and_below_producer_epoch: i16,
    /// The partitions to add to the transaction.
    pub v3_and_below_topics: Vec<AddPartitionsToTxnTopic>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for AddPartitionsToTxnRequestData {
    fn default() -> Self {
        Self {
            transactions: Vec::new(),
            v3_and_below_transactional_id: KafkaString::default(),
            v3_and_below_producer_id: 0_i64,
            v3_and_below_producer_epoch: 0_i16,
            v3_and_below_topics: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl AddPartitionsToTxnRequestData {
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 0 || version > 5 {
            return Err(UnsupportedVersion::new(24, version).into());
        }
        let mut transactions = Vec::new();
        let mut v3_and_below_transactional_id = KafkaString::default();
        let mut v3_and_below_producer_id = 0_i64;
        let mut v3_and_below_producer_epoch = 0_i16;
        let mut v3_and_below_topics = Vec::new();
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 4 {
            transactions = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(AddPartitionsToTxnTransaction::read(buf, version)?);
                }
                arr
            };
        }
        if version <= 3 {
            if version >= 3 {
                v3_and_below_transactional_id = read_compact_string(buf)?;
            } else {
                v3_and_below_transactional_id = read_string(buf)?;
            }
        }
        if version <= 3 {
            v3_and_below_producer_id = read_i64(buf)?;
        }
        if version <= 3 {
            v3_and_below_producer_epoch = read_i16(buf)?;
        }
        if version <= 3 {
            if version >= 3 {
                v3_and_below_topics = {
                    let len = read_compact_array_length(buf)?;
                    let mut arr = Vec::with_capacity(len.max(0) as usize);
                    for _ in 0..len {
                        arr.push(AddPartitionsToTxnTopic::read(buf, version)?);
                    }
                    arr
                };
            } else {
                v3_and_below_topics = {
                    let len = read_array_length(buf)?;
                    let mut arr = Vec::with_capacity(len.max(0) as usize);
                    for _ in 0..len {
                        arr.push(AddPartitionsToTxnTopic::read(buf, version)?);
                    }
                    arr
                };
            }
        }
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
            transactions,
            v3_and_below_transactional_id,
            v3_and_below_producer_id,
            v3_and_below_producer_epoch,
            v3_and_below_topics,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 0 || version > 5 {
            return Err(UnsupportedVersion::new(24, version).into());
        }
        if version >= 4 {
            write_compact_array_length(buf, self.transactions.len() as i32);
            for el in &self.transactions {
                el.write(buf, version)?;
            }
        }
        if version <= 3 {
            if version >= 3 {
                write_compact_string(buf, &self.v3_and_below_transactional_id)?;
            } else {
                write_string(buf, &self.v3_and_below_transactional_id)?;
            }
        }
        if version <= 3 {
            write_i64(buf, self.v3_and_below_producer_id);
        }
        if version <= 3 {
            write_i16(buf, self.v3_and_below_producer_epoch);
        }
        if version <= 3 {
            if version >= 3 {
                write_compact_array_length(buf, self.v3_and_below_topics.len() as i32);
                for el in &self.v3_and_below_topics {
                    el.write(buf, version)?;
                }
            } else {
                write_array_length(buf, self.v3_and_below_topics.len() as i32);
                for el in &self.v3_and_below_topics {
                    el.write(buf, version)?;
                }
            }
        }
        if version >= 3 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct AddPartitionsToTxnTransaction {
    /// The transactional id corresponding to the transaction.
    pub transactional_id: KafkaString,
    /// Current producer id in use by the transactional id.
    pub producer_id: i64,
    /// Current epoch associated with the producer id.
    pub producer_epoch: i16,
    /// Boolean to signify if we want to check if the partition is in the transaction rather than
    /// add it.
    pub verify_only: bool,
    /// The partitions to add to the transaction.
    pub topics: Vec<AddPartitionsToTxnTopic>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for AddPartitionsToTxnTransaction {
    fn default() -> Self {
        Self {
            transactional_id: KafkaString::default(),
            producer_id: 0_i64,
            producer_epoch: 0_i16,
            verify_only: false,
            topics: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl AddPartitionsToTxnTransaction {
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let transactional_id;
        let producer_id;
        let producer_epoch;
        let verify_only;
        let topics;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        transactional_id = read_compact_string(buf)?;
        producer_id = read_i64(buf)?;
        producer_epoch = read_i16(buf)?;
        verify_only = read_bool(buf)?;
        topics = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(len.max(0) as usize);
            for _ in 0..len {
                arr.push(AddPartitionsToTxnTopic::read(buf, version)?);
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
            transactional_id,
            producer_id,
            producer_epoch,
            verify_only,
            topics,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        write_compact_string(buf, &self.transactional_id)?;
        write_i64(buf, self.producer_id);
        write_i16(buf, self.producer_epoch);
        write_bool(buf, self.verify_only);
        write_compact_array_length(buf, self.topics.len() as i32);
        for el in &self.topics {
            el.write(buf, version)?;
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct AddPartitionsToTxnTopic {
    /// The name of the topic.
    pub name: KafkaString,
    /// The partition indexes to add to the transaction.
    pub partitions: Vec<i32>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for AddPartitionsToTxnTopic {
    fn default() -> Self {
        Self {
            name: KafkaString::default(),
            partitions: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl AddPartitionsToTxnTopic {
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let name;
        let partitions;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 3 {
            name = read_compact_string(buf)?;
        } else {
            name = read_string(buf)?;
        }
        if version >= 3 {
            partitions = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(read_i32(buf)?);
                }
                arr
            };
        } else {
            partitions = {
                let len = read_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(read_i32(buf)?);
                }
                arr
            };
        }
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
            name,
            partitions,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version >= 3 {
            write_compact_string(buf, &self.name)?;
        } else {
            write_string(buf, &self.name)?;
        }
        if version >= 3 {
            write_compact_array_length(buf, self.partitions.len() as i32);
            for el in &self.partitions {
                write_i32(buf, *el);
            }
        } else {
            write_array_length(buf, self.partitions.len() as i32);
            for el in &self.partitions {
                write_i32(buf, *el);
            }
        }
        if version >= 3 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
}
