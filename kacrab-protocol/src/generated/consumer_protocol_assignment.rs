//! Generated from ConsumerProtocolAssignment.json - DO NOT EDIT
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
pub struct ConsumerProtocolAssignmentData {
    /// The list of topics and partitions assigned to this consumer.
    pub assigned_partitions: Vec<TopicPartition>,
    /// User data.
    pub user_data: Option<Bytes>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for ConsumerProtocolAssignmentData {
    fn default() -> Self {
        Self {
            assigned_partitions: Vec::new(),
            user_data: None,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl ConsumerProtocolAssignmentData {
    pub fn with_assigned_partitions(mut self, value: Vec<TopicPartition>) -> Self {
        self.assigned_partitions = value;
        self
    }
    pub fn with_user_data(mut self, value: Option<Bytes>) -> Self {
        self.user_data = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let assigned_partitions;
        let user_data;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        assigned_partitions = {
            let len = read_array_length(buf)?;
            let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
            for _ in 0..len {
                arr.push(TopicPartition::read(buf, version)?);
            }
            arr
        };
        user_data = read_nullable_bytes(buf)?;
        Ok(Self {
            assigned_partitions,
            user_data,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        write_array_length(buf, self.assigned_partitions.len() as i32);
        for el in &self.assigned_partitions {
            el.write(buf, version)?;
        }
        write_nullable_bytes(buf, self.user_data.as_ref().map(|b| b.as_ref()))?;
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        let mut len: usize = 0;
        len += array_length_len();
        for el in &self.assigned_partitions {
            len += el.encoded_len(version)?;
        }
        len += nullable_bytes_len(self.user_data.as_ref().map(|b| b.as_ref()))?;
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct TopicPartition {
    /// The topic name.
    pub topic: KafkaString,
    /// The list of partitions assigned to this consumer.
    pub partitions: Vec<i32>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for TopicPartition {
    fn default() -> Self {
        Self {
            topic: KafkaString::default(),
            partitions: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl TopicPartition {
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
        topic = read_string(buf)?;
        partitions = {
            let len = read_array_length(buf)?;
            let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
            for _ in 0..len {
                arr.push(read_i32(buf)?);
            }
            arr
        };
        Ok(Self {
            topic,
            partitions,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, _version: i16) -> Result<()> {
        write_string(buf, &self.topic)?;
        write_array_length(buf, self.partitions.len() as i32);
        for el in &self.partitions {
            write_i32(buf, *el);
        }
        Ok(())
    }
    pub fn encoded_len(&self, _version: i16) -> Result<usize> {
        let mut len: usize = 0;
        len += string_len(&self.topic)?;
        len += array_length_len();
        len += self.partitions.len() * 4usize;
        Ok(len)
    }
}
