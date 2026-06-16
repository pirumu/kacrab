//! Generated from ConsumerProtocolSubscription.json - DO NOT EDIT
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
pub struct ConsumerProtocolSubscriptionData {
    /// The topics that the member wants to consume.
    pub topics: Vec<KafkaString>,
    /// User data that will be passed back to the consumer.
    pub user_data: Option<Bytes>,
    /// The partitions that the member owns.
    pub owned_partitions: Vec<TopicPartition>,
    /// The generation id of the member.
    pub generation_id: i32,
    /// The rack id of the member.
    pub rack_id: Option<KafkaString>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for ConsumerProtocolSubscriptionData {
    fn default() -> Self {
        Self {
            topics: Vec::new(),
            user_data: None,
            owned_partitions: Vec::new(),
            generation_id: -1i32,
            rack_id: None,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl ConsumerProtocolSubscriptionData {
    pub fn with_topics(mut self, value: Vec<KafkaString>) -> Self {
        self.topics = value;
        self
    }
    pub fn with_user_data(mut self, value: Option<Bytes>) -> Self {
        self.user_data = value;
        self
    }
    pub fn with_owned_partitions(mut self, value: Vec<TopicPartition>) -> Self {
        self.owned_partitions = value;
        self
    }
    pub fn with_generation_id(mut self, value: i32) -> Self {
        self.generation_id = value;
        self
    }
    pub fn with_rack_id(mut self, value: Option<KafkaString>) -> Self {
        self.rack_id = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let topics;
        let user_data;
        let mut owned_partitions = Vec::new();
        let mut generation_id = -1i32;
        let mut rack_id = None;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        topics = {
            let len = read_array_length(buf)?;
            let mut arr = Vec::with_capacity(len.max(0) as usize);
            for _ in 0..len {
                arr.push(read_string(buf)?);
            }
            arr
        };
        user_data = read_nullable_bytes(buf)?;
        if version >= 1 {
            owned_partitions = {
                let len = read_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(TopicPartition::read(buf, version)?);
                }
                arr
            };
        }
        if version >= 2 {
            generation_id = read_i32(buf)?;
        }
        if version >= 3 {
            rack_id = read_nullable_string(buf)?;
        }
        Ok(Self {
            topics,
            user_data,
            owned_partitions,
            generation_id,
            rack_id,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        write_array_length(buf, self.topics.len() as i32);
        for el in &self.topics {
            write_string(buf, el)?;
        }
        write_nullable_bytes(buf, self.user_data.as_ref().map(|b| b.as_ref()))?;
        if version >= 1 {
            write_array_length(buf, self.owned_partitions.len() as i32);
            for el in &self.owned_partitions {
                el.write(buf, version)?;
            }
        }
        if version >= 2 {
            write_i32(buf, self.generation_id);
        }
        if version >= 3 {
            write_nullable_string(buf, self.rack_id.as_ref())?;
        }
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        let mut len: usize = 0;
        len += array_length_len();
        for el in &self.topics {
            len += string_len(el)?;
        }
        len += nullable_bytes_len(self.user_data.as_ref().map(|b| b.as_ref()))?;
        if version >= 1 {
            len += array_length_len();
            for el in &self.owned_partitions {
                len += el.encoded_len(version)?;
            }
        }
        if version >= 2 {
            len += 4;
        }
        if version >= 3 {
            len += nullable_string_len(self.rack_id.as_ref())?;
        }
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct TopicPartition {
    /// The topic name.
    pub topic: KafkaString,
    /// The partition ids.
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
    pub fn encoded_len(&self, _version: i16) -> Result<usize> {
        let mut len: usize = 0;
        len += string_len(&self.topic)?;
        len += array_length_len();
        len += self.partitions.len() * 4usize;
        Ok(len)
    }
}
