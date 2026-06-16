//! Generated from ConsumerProtocolAssignment.json - DO NOT EDIT
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
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let assigned_partitions;
        let user_data;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        assigned_partitions = {
            let len = read_array_length(buf)?;
            let mut arr = Vec::with_capacity(len.max(0) as usize);
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
    pub fn read(buf: &mut Bytes, _version: i16) -> Result<Self> {
        let topic;
        let partitions;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        topic = read_string(buf)?;
        partitions = {
            let len = read_array_length(buf)?;
            let mut arr = Vec::with_capacity(len.max(0) as usize);
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
}
