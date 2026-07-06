//! Generated from DescribeTopicPartitionsResponse.json - DO NOT EDIT
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
pub struct DescribeTopicPartitionsResponseData {
    /// The duration in milliseconds for which the request was throttled due to a quota violation,
    /// or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// Each topic in the response.
    pub topics: Vec<DescribeTopicPartitionsResponseTopic>,
    /// The next topic and partition index to fetch details for.
    pub next_cursor: Option<Box<Cursor>>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for DescribeTopicPartitionsResponseData {
    fn default() -> Self {
        Self {
            throttle_time_ms: 0_i32,
            topics: Vec::new(),
            next_cursor: None,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl DescribeTopicPartitionsResponseData {
    pub fn with_throttle_time_ms(mut self, value: i32) -> Self {
        self.throttle_time_ms = value;
        self
    }
    pub fn with_topics(mut self, value: Vec<DescribeTopicPartitionsResponseTopic>) -> Self {
        self.topics = value;
        self
    }
    pub fn with_next_cursor(mut self, value: Option<Box<Cursor>>) -> Self {
        self.next_cursor = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 0 || version > 0 {
            return Err(UnsupportedVersion::new(75, version).into());
        }
        let throttle_time_ms;
        let topics;
        let next_cursor;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        throttle_time_ms = read_i32(buf)?;
        topics = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
            for _ in 0..len {
                arr.push(DescribeTopicPartitionsResponseTopic::read(buf, version)?);
            }
            arr
        };
        next_cursor = {
            let marker = read_i8(buf)?;
            if marker < 0 {
                None
            } else {
                Some(Box::new(Cursor::read(buf, version)?))
            }
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
            topics,
            next_cursor,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 0 || version > 0 {
            return Err(UnsupportedVersion::new(75, version).into());
        }
        write_i32(buf, self.throttle_time_ms);
        write_compact_array_length(buf, self.topics.len() as i32);
        for el in &self.topics {
            el.write(buf, version)?;
        }
        match &self.next_cursor {
            None => {
                write_i8(buf, -1);
            },
            Some(v) => {
                write_i8(buf, 1);
                v.write(buf, version)?;
            },
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        if version < 0 || version > 0 {
            return Err(UnsupportedVersion::new(75, version).into());
        }
        let mut len: usize = 0;
        len += 4;
        len += compact_array_length_len(self.topics.len() as i32);
        for el in &self.topics {
            len += el.encoded_len(version)?;
        }
        match &self.next_cursor {
            None => {
                len += 1;
            },
            Some(v) => {
                len += 1;
                len += v.encoded_len(version)?;
            },
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct DescribeTopicPartitionsResponseTopic {
    /// The topic error, or 0 if there was no error.
    pub error_code: i16,
    /// The topic name.
    pub name: Option<KafkaString>,
    /// The topic id.
    pub topic_id: KafkaUuid,
    /// True if the topic is internal.
    pub is_internal: bool,
    /// Each partition in the topic.
    pub partitions: Vec<DescribeTopicPartitionsResponsePartition>,
    /// 32-bit bitfield to represent authorized operations for this topic.
    pub topic_authorized_operations: i32,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for DescribeTopicPartitionsResponseTopic {
    fn default() -> Self {
        Self {
            error_code: 0_i16,
            name: None,
            topic_id: KafkaUuid::ZERO,
            is_internal: false,
            partitions: Vec::new(),
            topic_authorized_operations: i32::MIN,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl DescribeTopicPartitionsResponseTopic {
    pub fn with_error_code(mut self, value: i16) -> Self {
        self.error_code = value;
        self
    }
    pub fn with_name(mut self, value: Option<KafkaString>) -> Self {
        self.name = value;
        self
    }
    pub fn with_topic_id(mut self, value: KafkaUuid) -> Self {
        self.topic_id = value;
        self
    }
    pub fn with_is_internal(mut self, value: bool) -> Self {
        self.is_internal = value;
        self
    }
    pub fn with_partitions(mut self, value: Vec<DescribeTopicPartitionsResponsePartition>) -> Self {
        self.partitions = value;
        self
    }
    pub fn with_topic_authorized_operations(mut self, value: i32) -> Self {
        self.topic_authorized_operations = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let error_code;
        let name;
        let topic_id;
        let is_internal;
        let partitions;
        let topic_authorized_operations;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        error_code = read_i16(buf)?;
        name = read_compact_nullable_string(buf)?;
        topic_id = read_uuid(buf)?;
        is_internal = read_bool(buf)?;
        partitions = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
            for _ in 0..len {
                arr.push(DescribeTopicPartitionsResponsePartition::read(
                    buf, version,
                )?);
            }
            arr
        };
        topic_authorized_operations = read_i32(buf)?;
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
            name,
            topic_id,
            is_internal,
            partitions,
            topic_authorized_operations,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        write_i16(buf, self.error_code);
        write_compact_nullable_string(buf, self.name.as_ref())?;
        write_uuid(buf, &self.topic_id);
        write_bool(buf, self.is_internal);
        write_compact_array_length(buf, self.partitions.len() as i32);
        for el in &self.partitions {
            el.write(buf, version)?;
        }
        write_i32(buf, self.topic_authorized_operations);
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        let mut len: usize = 0;
        len += 2;
        len += compact_nullable_string_len(self.name.as_ref())?;
        len += 16;
        len += 1;
        len += compact_array_length_len(self.partitions.len() as i32);
        for el in &self.partitions {
            len += el.encoded_len(version)?;
        }
        len += 4;
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct DescribeTopicPartitionsResponsePartition {
    /// The partition error, or 0 if there was no error.
    pub error_code: i16,
    /// The partition index.
    pub partition_index: i32,
    /// The ID of the leader broker.
    pub leader_id: i32,
    /// The leader epoch of this partition.
    pub leader_epoch: i32,
    /// The set of all nodes that host this partition.
    pub replica_nodes: Vec<i32>,
    /// The set of nodes that are in sync with the leader for this partition.
    pub isr_nodes: Vec<i32>,
    /// The new eligible leader replicas otherwise.
    pub eligible_leader_replicas: Option<Vec<i32>>,
    /// The last known ELR.
    pub last_known_elr: Option<Vec<i32>>,
    /// The set of offline replicas of this partition.
    pub offline_replicas: Vec<i32>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for DescribeTopicPartitionsResponsePartition {
    fn default() -> Self {
        Self {
            error_code: 0_i16,
            partition_index: 0_i32,
            leader_id: 0_i32,
            leader_epoch: -1i32,
            replica_nodes: Vec::new(),
            isr_nodes: Vec::new(),
            eligible_leader_replicas: None,
            last_known_elr: None,
            offline_replicas: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl DescribeTopicPartitionsResponsePartition {
    pub fn with_error_code(mut self, value: i16) -> Self {
        self.error_code = value;
        self
    }
    pub fn with_partition_index(mut self, value: i32) -> Self {
        self.partition_index = value;
        self
    }
    pub fn with_leader_id(mut self, value: i32) -> Self {
        self.leader_id = value;
        self
    }
    pub fn with_leader_epoch(mut self, value: i32) -> Self {
        self.leader_epoch = value;
        self
    }
    pub fn with_replica_nodes(mut self, value: Vec<i32>) -> Self {
        self.replica_nodes = value;
        self
    }
    pub fn with_isr_nodes(mut self, value: Vec<i32>) -> Self {
        self.isr_nodes = value;
        self
    }
    pub fn with_eligible_leader_replicas(mut self, value: Option<Vec<i32>>) -> Self {
        self.eligible_leader_replicas = value;
        self
    }
    pub fn with_last_known_elr(mut self, value: Option<Vec<i32>>) -> Self {
        self.last_known_elr = value;
        self
    }
    pub fn with_offline_replicas(mut self, value: Vec<i32>) -> Self {
        self.offline_replicas = value;
        self
    }
    pub fn read(buf: &mut Bytes, _version: i16) -> Result<Self> {
        let error_code;
        let partition_index;
        let leader_id;
        let leader_epoch;
        let replica_nodes;
        let isr_nodes;
        let eligible_leader_replicas;
        let last_known_elr;
        let offline_replicas;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        error_code = read_i16(buf)?;
        partition_index = read_i32(buf)?;
        leader_id = read_i32(buf)?;
        leader_epoch = read_i32(buf)?;
        replica_nodes = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
            for _ in 0..len {
                arr.push(read_i32(buf)?);
            }
            arr
        };
        isr_nodes = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
            for _ in 0..len {
                arr.push(read_i32(buf)?);
            }
            arr
        };
        eligible_leader_replicas = {
            let len = read_compact_array_length(buf)?;
            if len < 0 {
                None
            } else {
                let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
                for _ in 0..len {
                    arr.push(read_i32(buf)?);
                }
                Some(arr)
            }
        };
        last_known_elr = {
            let len = read_compact_array_length(buf)?;
            if len < 0 {
                None
            } else {
                let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
                for _ in 0..len {
                    arr.push(read_i32(buf)?);
                }
                Some(arr)
            }
        };
        offline_replicas = {
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
            error_code,
            partition_index,
            leader_id,
            leader_epoch,
            replica_nodes,
            isr_nodes,
            eligible_leader_replicas,
            last_known_elr,
            offline_replicas,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, _version: i16) -> Result<()> {
        write_i16(buf, self.error_code);
        write_i32(buf, self.partition_index);
        write_i32(buf, self.leader_id);
        write_i32(buf, self.leader_epoch);
        write_compact_array_length(buf, self.replica_nodes.len() as i32);
        for el in &self.replica_nodes {
            write_i32(buf, *el);
        }
        write_compact_array_length(buf, self.isr_nodes.len() as i32);
        for el in &self.isr_nodes {
            write_i32(buf, *el);
        }
        match &self.eligible_leader_replicas {
            None => {
                write_compact_array_length(buf, -1);
            },
            Some(arr) => {
                write_compact_array_length(buf, arr.len() as i32);
                for el in arr {
                    write_i32(buf, *el);
                }
            },
        }
        match &self.last_known_elr {
            None => {
                write_compact_array_length(buf, -1);
            },
            Some(arr) => {
                write_compact_array_length(buf, arr.len() as i32);
                for el in arr {
                    write_i32(buf, *el);
                }
            },
        }
        write_compact_array_length(buf, self.offline_replicas.len() as i32);
        for el in &self.offline_replicas {
            write_i32(buf, *el);
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
    pub fn encoded_len(&self, _version: i16) -> Result<usize> {
        let mut len: usize = 0;
        len += 2;
        len += 4;
        len += 4;
        len += 4;
        len += compact_array_length_len(self.replica_nodes.len() as i32);
        len += self.replica_nodes.len() * 4usize;
        len += compact_array_length_len(self.isr_nodes.len() as i32);
        len += self.isr_nodes.len() * 4usize;
        match &self.eligible_leader_replicas {
            None => {
                len += compact_array_length_len(-1);
            },
            Some(arr) => {
                len += compact_array_length_len(arr.len() as i32);
                len += arr.len() * 4usize;
            },
        }
        match &self.last_known_elr {
            None => {
                len += compact_array_length_len(-1);
            },
            Some(arr) => {
                len += compact_array_length_len(arr.len() as i32);
                len += arr.len() * 4usize;
            },
        }
        len += compact_array_length_len(self.offline_replicas.len() as i32);
        len += self.offline_replicas.len() * 4usize;
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct Cursor {
    /// The name for the first topic to process.
    pub topic_name: KafkaString,
    /// The partition index to start with.
    pub partition_index: i32,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for Cursor {
    fn default() -> Self {
        Self {
            topic_name: KafkaString::default(),
            partition_index: 0_i32,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl Cursor {
    pub fn with_topic_name(mut self, value: KafkaString) -> Self {
        self.topic_name = value;
        self
    }
    pub fn with_partition_index(mut self, value: i32) -> Self {
        self.partition_index = value;
        self
    }
    pub fn read(buf: &mut Bytes, _version: i16) -> Result<Self> {
        let topic_name;
        let partition_index;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        topic_name = read_compact_string(buf)?;
        partition_index = read_i32(buf)?;
        let tagged_fields = read_tagged_fields(buf)?;
        for field in &tagged_fields {
            match field.tag {
                _ => {
                    _unknown_tagged_fields.push(field.clone());
                },
            }
        }
        Ok(Self {
            topic_name,
            partition_index,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, _version: i16) -> Result<()> {
        write_compact_string(buf, &self.topic_name)?;
        write_i32(buf, self.partition_index);
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
    pub fn encoded_len(&self, _version: i16) -> Result<usize> {
        let mut len: usize = 0;
        len += compact_string_len(&self.topic_name)?;
        len += 4;
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
