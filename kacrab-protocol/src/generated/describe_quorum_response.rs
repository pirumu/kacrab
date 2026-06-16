//! Generated from DescribeQuorumResponse.json - DO NOT EDIT
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
pub struct DescribeQuorumResponseData {
    /// The top level error code.
    pub error_code: i16,
    /// The error message, or null if there was no error.
    pub error_message: Option<KafkaString>,
    /// The response from the describe quorum API.
    pub topics: Vec<TopicData>,
    /// The nodes in the quorum.
    pub nodes: Vec<Node>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for DescribeQuorumResponseData {
    fn default() -> Self {
        Self {
            error_code: 0_i16,
            error_message: None,
            topics: Vec::new(),
            nodes: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl DescribeQuorumResponseData {
    pub fn with_error_code(mut self, value: i16) -> Self {
        self.error_code = value;
        self
    }
    pub fn with_error_message(mut self, value: Option<KafkaString>) -> Self {
        self.error_message = value;
        self
    }
    pub fn with_topics(mut self, value: Vec<TopicData>) -> Self {
        self.topics = value;
        self
    }
    pub fn with_nodes(mut self, value: Vec<Node>) -> Self {
        self.nodes = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 0 || version > 2 {
            return Err(UnsupportedVersion::new(55, version).into());
        }
        let error_code;
        let mut error_message = None;
        let topics;
        let mut nodes = Vec::new();
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        error_code = read_i16(buf)?;
        if version >= 2 {
            error_message = read_compact_nullable_string(buf)?;
        }
        topics = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(len.max(0) as usize);
            for _ in 0..len {
                arr.push(TopicData::read(buf, version)?);
            }
            arr
        };
        if version >= 2 {
            nodes = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(Node::read(buf, version)?);
                }
                arr
            };
        }
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
            error_message,
            topics,
            nodes,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 0 || version > 2 {
            return Err(UnsupportedVersion::new(55, version).into());
        }
        write_i16(buf, self.error_code);
        if version >= 2 {
            write_compact_nullable_string(buf, self.error_message.as_ref())?;
        } else if self.error_message != None {
            return Err(UnsupportedFieldVersion::new(55, "error_message", version).into());
        }
        write_compact_array_length(buf, self.topics.len() as i32);
        for el in &self.topics {
            el.write(buf, version)?;
        }
        if version >= 2 {
            write_compact_array_length(buf, self.nodes.len() as i32);
            for el in &self.nodes {
                el.write(buf, version)?;
            }
        } else if self.nodes != Vec::new() {
            return Err(UnsupportedFieldVersion::new(55, "nodes", version).into());
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        if version < 0 || version > 2 {
            return Err(UnsupportedVersion::new(55, version).into());
        }
        let mut len: usize = 0;
        len += 2;
        if version >= 2 {
            len += compact_nullable_string_len(self.error_message.as_ref())?;
        } else if self.error_message != None {
            return Err(UnsupportedFieldVersion::new(55, "error_message", version).into());
        }
        len += compact_array_length_len(self.topics.len() as i32);
        for el in &self.topics {
            len += el.encoded_len(version)?;
        }
        if version >= 2 {
            len += compact_array_length_len(self.nodes.len() as i32);
            for el in &self.nodes {
                len += el.encoded_len(version)?;
            }
        } else if self.nodes != Vec::new() {
            return Err(UnsupportedFieldVersion::new(55, "nodes", version).into());
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
    pub topic_name: KafkaString,
    /// The partition data.
    pub partitions: Vec<PartitionData>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for TopicData {
    fn default() -> Self {
        Self {
            topic_name: KafkaString::default(),
            partitions: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl TopicData {
    pub fn with_topic_name(mut self, value: KafkaString) -> Self {
        self.topic_name = value;
        self
    }
    pub fn with_partitions(mut self, value: Vec<PartitionData>) -> Self {
        self.partitions = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let topic_name;
        let partitions;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        topic_name = read_compact_string(buf)?;
        partitions = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(len.max(0) as usize);
            for _ in 0..len {
                arr.push(PartitionData::read(buf, version)?);
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
            topic_name,
            partitions,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        write_compact_string(buf, &self.topic_name)?;
        write_compact_array_length(buf, self.partitions.len() as i32);
        for el in &self.partitions {
            el.write(buf, version)?;
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        let mut len: usize = 0;
        len += compact_string_len(&self.topic_name)?;
        len += compact_array_length_len(self.partitions.len() as i32);
        for el in &self.partitions {
            len += el.encoded_len(version)?;
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct PartitionData {
    /// The partition index.
    pub partition_index: i32,
    /// The partition error code.
    pub error_code: i16,
    /// The error message, or null if there was no error.
    pub error_message: Option<KafkaString>,
    /// The ID of the current leader or -1 if the leader is unknown.
    pub leader_id: i32,
    /// The latest known leader epoch.
    pub leader_epoch: i32,
    /// The high water mark.
    pub high_watermark: i64,
    /// The current voters of the partition.
    pub current_voters: Vec<ReplicaState>,
    /// The observers of the partition.
    pub observers: Vec<ReplicaState>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for PartitionData {
    fn default() -> Self {
        Self {
            partition_index: 0_i32,
            error_code: 0_i16,
            error_message: None,
            leader_id: 0_i32,
            leader_epoch: 0_i32,
            high_watermark: 0_i64,
            current_voters: Vec::new(),
            observers: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl PartitionData {
    pub fn with_partition_index(mut self, value: i32) -> Self {
        self.partition_index = value;
        self
    }
    pub fn with_error_code(mut self, value: i16) -> Self {
        self.error_code = value;
        self
    }
    pub fn with_error_message(mut self, value: Option<KafkaString>) -> Self {
        self.error_message = value;
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
    pub fn with_high_watermark(mut self, value: i64) -> Self {
        self.high_watermark = value;
        self
    }
    pub fn with_current_voters(mut self, value: Vec<ReplicaState>) -> Self {
        self.current_voters = value;
        self
    }
    pub fn with_observers(mut self, value: Vec<ReplicaState>) -> Self {
        self.observers = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let partition_index;
        let error_code;
        let mut error_message = None;
        let leader_id;
        let leader_epoch;
        let high_watermark;
        let current_voters;
        let observers;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        partition_index = read_i32(buf)?;
        error_code = read_i16(buf)?;
        if version >= 2 {
            error_message = read_compact_nullable_string(buf)?;
        }
        leader_id = read_i32(buf)?;
        leader_epoch = read_i32(buf)?;
        high_watermark = read_i64(buf)?;
        current_voters = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(len.max(0) as usize);
            for _ in 0..len {
                arr.push(ReplicaState::read(buf, version)?);
            }
            arr
        };
        observers = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(len.max(0) as usize);
            for _ in 0..len {
                arr.push(ReplicaState::read(buf, version)?);
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
            partition_index,
            error_code,
            error_message,
            leader_id,
            leader_epoch,
            high_watermark,
            current_voters,
            observers,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        write_i32(buf, self.partition_index);
        write_i16(buf, self.error_code);
        if version >= 2 {
            write_compact_nullable_string(buf, self.error_message.as_ref())?;
        } else if self.error_message != None {
            return Err(UnsupportedFieldVersion::new(55, "error_message", version).into());
        }
        write_i32(buf, self.leader_id);
        write_i32(buf, self.leader_epoch);
        write_i64(buf, self.high_watermark);
        write_compact_array_length(buf, self.current_voters.len() as i32);
        for el in &self.current_voters {
            el.write(buf, version)?;
        }
        write_compact_array_length(buf, self.observers.len() as i32);
        for el in &self.observers {
            el.write(buf, version)?;
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        let mut len: usize = 0;
        len += 4;
        len += 2;
        if version >= 2 {
            len += compact_nullable_string_len(self.error_message.as_ref())?;
        } else if self.error_message != None {
            return Err(UnsupportedFieldVersion::new(55, "error_message", version).into());
        }
        len += 4;
        len += 4;
        len += 8;
        len += compact_array_length_len(self.current_voters.len() as i32);
        for el in &self.current_voters {
            len += el.encoded_len(version)?;
        }
        len += compact_array_length_len(self.observers.len() as i32);
        for el in &self.observers {
            len += el.encoded_len(version)?;
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct Node {
    /// The ID of the associated node.
    pub node_id: i32,
    /// The listeners of this controller.
    pub listeners: Vec<Listener>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for Node {
    fn default() -> Self {
        Self {
            node_id: 0_i32,
            listeners: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl Node {
    pub fn with_node_id(mut self, value: i32) -> Self {
        self.node_id = value;
        self
    }
    pub fn with_listeners(mut self, value: Vec<Listener>) -> Self {
        self.listeners = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let node_id;
        let listeners;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        node_id = read_i32(buf)?;
        listeners = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(len.max(0) as usize);
            for _ in 0..len {
                arr.push(Listener::read(buf, version)?);
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
            node_id,
            listeners,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        write_i32(buf, self.node_id);
        write_compact_array_length(buf, self.listeners.len() as i32);
        for el in &self.listeners {
            el.write(buf, version)?;
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        let mut len: usize = 0;
        len += 4;
        len += compact_array_length_len(self.listeners.len() as i32);
        for el in &self.listeners {
            len += el.encoded_len(version)?;
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct Listener {
    /// The name of the endpoint.
    pub name: KafkaString,
    /// The hostname.
    pub host: KafkaString,
    /// The port.
    pub port: u16,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for Listener {
    fn default() -> Self {
        Self {
            name: KafkaString::default(),
            host: KafkaString::default(),
            port: 0_u16,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl Listener {
    pub fn with_name(mut self, value: KafkaString) -> Self {
        self.name = value;
        self
    }
    pub fn with_host(mut self, value: KafkaString) -> Self {
        self.host = value;
        self
    }
    pub fn with_port(mut self, value: u16) -> Self {
        self.port = value;
        self
    }
    pub fn read(buf: &mut Bytes, _version: i16) -> Result<Self> {
        let name;
        let host;
        let port;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        name = read_compact_string(buf)?;
        host = read_compact_string(buf)?;
        port = read_u16(buf)?;
        let tagged_fields = read_tagged_fields(buf)?;
        for field in &tagged_fields {
            match field.tag {
                _ => {
                    _unknown_tagged_fields.push(field.clone());
                },
            }
        }
        Ok(Self {
            name,
            host,
            port,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, _version: i16) -> Result<()> {
        write_compact_string(buf, &self.name)?;
        write_compact_string(buf, &self.host)?;
        write_u16(buf, self.port);
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
    pub fn encoded_len(&self, _version: i16) -> Result<usize> {
        let mut len: usize = 0;
        len += compact_string_len(&self.name)?;
        len += compact_string_len(&self.host)?;
        len += 2;
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct ReplicaState {
    /// The ID of the replica.
    pub replica_id: i32,
    /// The replica directory ID of the replica.
    pub replica_directory_id: KafkaUuid,
    /// The last known log end offset of the follower or -1 if it is unknown.
    pub log_end_offset: i64,
    /// The last known leader wall clock time time when a follower fetched from the leader. This is
    /// reported as -1 both for the current leader or if it is unknown for a voter.
    pub last_fetch_timestamp: i64,
    /// The leader wall clock append time of the offset for which the follower made the most recent
    /// fetch request. This is reported as the current time for the leader and -1 if unknown for a
    /// voter.
    pub last_caught_up_timestamp: i64,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for ReplicaState {
    fn default() -> Self {
        Self {
            replica_id: 0_i32,
            replica_directory_id: KafkaUuid::ZERO,
            log_end_offset: 0_i64,
            last_fetch_timestamp: -1i64,
            last_caught_up_timestamp: -1i64,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl ReplicaState {
    pub fn with_replica_id(mut self, value: i32) -> Self {
        self.replica_id = value;
        self
    }
    pub fn with_replica_directory_id(mut self, value: KafkaUuid) -> Self {
        self.replica_directory_id = value;
        self
    }
    pub fn with_log_end_offset(mut self, value: i64) -> Self {
        self.log_end_offset = value;
        self
    }
    pub fn with_last_fetch_timestamp(mut self, value: i64) -> Self {
        self.last_fetch_timestamp = value;
        self
    }
    pub fn with_last_caught_up_timestamp(mut self, value: i64) -> Self {
        self.last_caught_up_timestamp = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let replica_id;
        let mut replica_directory_id = KafkaUuid::ZERO;
        let log_end_offset;
        let mut last_fetch_timestamp = -1i64;
        let mut last_caught_up_timestamp = -1i64;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        replica_id = read_i32(buf)?;
        if version >= 2 {
            replica_directory_id = read_uuid(buf)?;
        }
        log_end_offset = read_i64(buf)?;
        if version >= 1 {
            last_fetch_timestamp = read_i64(buf)?;
        }
        if version >= 1 {
            last_caught_up_timestamp = read_i64(buf)?;
        }
        let tagged_fields = read_tagged_fields(buf)?;
        for field in &tagged_fields {
            match field.tag {
                _ => {
                    _unknown_tagged_fields.push(field.clone());
                },
            }
        }
        Ok(Self {
            replica_id,
            replica_directory_id,
            log_end_offset,
            last_fetch_timestamp,
            last_caught_up_timestamp,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        write_i32(buf, self.replica_id);
        if version >= 2 {
            write_uuid(buf, &self.replica_directory_id);
        } else if self.replica_directory_id != KafkaUuid::ZERO {
            return Err(UnsupportedFieldVersion::new(55, "replica_directory_id", version).into());
        }
        write_i64(buf, self.log_end_offset);
        if version >= 1 {
            write_i64(buf, self.last_fetch_timestamp);
        } else if self.last_fetch_timestamp != -1i64 {
            return Err(UnsupportedFieldVersion::new(55, "last_fetch_timestamp", version).into());
        }
        if version >= 1 {
            write_i64(buf, self.last_caught_up_timestamp);
        } else if self.last_caught_up_timestamp != -1i64 {
            return Err(
                UnsupportedFieldVersion::new(55, "last_caught_up_timestamp", version).into(),
            );
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        let mut len: usize = 0;
        len += 4;
        if version >= 2 {
            len += 16;
        } else if self.replica_directory_id != KafkaUuid::ZERO {
            return Err(UnsupportedFieldVersion::new(55, "replica_directory_id", version).into());
        }
        len += 8;
        if version >= 1 {
            len += 8;
        } else if self.last_fetch_timestamp != -1i64 {
            return Err(UnsupportedFieldVersion::new(55, "last_fetch_timestamp", version).into());
        }
        if version >= 1 {
            len += 8;
        } else if self.last_caught_up_timestamp != -1i64 {
            return Err(
                UnsupportedFieldVersion::new(55, "last_caught_up_timestamp", version).into(),
            );
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
