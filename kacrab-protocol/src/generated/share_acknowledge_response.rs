//! Generated from ShareAcknowledgeResponse.json - DO NOT EDIT
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
pub struct ShareAcknowledgeResponseData {
    /// The duration in milliseconds for which the request was throttled due to a quota violation,
    /// or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// The top level response error code.
    pub error_code: i16,
    /// The top-level error message, or null if there was no error.
    pub error_message: Option<KafkaString>,
    /// The time in milliseconds for which the acquired records are locked.
    pub acquisition_lock_timeout_ms: i32,
    /// The response topics.
    pub responses: Vec<ShareAcknowledgeTopicResponse>,
    /// Endpoints for all current leaders enumerated in PartitionData with error
    /// NOT_LEADER_OR_FOLLOWER.
    pub node_endpoints: Vec<NodeEndpoint>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for ShareAcknowledgeResponseData {
    fn default() -> Self {
        Self {
            throttle_time_ms: 0_i32,
            error_code: 0_i16,
            error_message: None,
            acquisition_lock_timeout_ms: 0_i32,
            responses: Vec::new(),
            node_endpoints: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl ShareAcknowledgeResponseData {
    pub fn with_throttle_time_ms(mut self, value: i32) -> Self {
        self.throttle_time_ms = value;
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
    pub fn with_acquisition_lock_timeout_ms(mut self, value: i32) -> Self {
        self.acquisition_lock_timeout_ms = value;
        self
    }
    pub fn with_responses(mut self, value: Vec<ShareAcknowledgeTopicResponse>) -> Self {
        self.responses = value;
        self
    }
    pub fn with_node_endpoints(mut self, value: Vec<NodeEndpoint>) -> Self {
        self.node_endpoints = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 1 || version > 2 {
            return Err(UnsupportedVersion::new(79, version).into());
        }
        let throttle_time_ms;
        let error_code;
        let error_message;
        let mut acquisition_lock_timeout_ms = 0_i32;
        let responses;
        let node_endpoints;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        throttle_time_ms = read_i32(buf)?;
        error_code = read_i16(buf)?;
        error_message = read_compact_nullable_string(buf)?;
        if version >= 2 {
            acquisition_lock_timeout_ms = read_i32(buf)?;
        }
        responses = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(len.max(0) as usize);
            for _ in 0..len {
                arr.push(ShareAcknowledgeTopicResponse::read(buf, version)?);
            }
            arr
        };
        node_endpoints = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(len.max(0) as usize);
            for _ in 0..len {
                arr.push(NodeEndpoint::read(buf, version)?);
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
            error_message,
            acquisition_lock_timeout_ms,
            responses,
            node_endpoints,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 1 || version > 2 {
            return Err(UnsupportedVersion::new(79, version).into());
        }
        write_i32(buf, self.throttle_time_ms);
        write_i16(buf, self.error_code);
        write_compact_nullable_string(buf, self.error_message.as_ref())?;
        if version >= 2 {
            write_i32(buf, self.acquisition_lock_timeout_ms);
        } else if self.acquisition_lock_timeout_ms != 0_i32 {
            return Err(
                UnsupportedFieldVersion::new(79, "acquisition_lock_timeout_ms", version).into(),
            );
        }
        write_compact_array_length(buf, self.responses.len() as i32);
        for el in &self.responses {
            el.write(buf, version)?;
        }
        write_compact_array_length(buf, self.node_endpoints.len() as i32);
        for el in &self.node_endpoints {
            el.write(buf, version)?;
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        if version < 1 || version > 2 {
            return Err(UnsupportedVersion::new(79, version).into());
        }
        let mut len: usize = 0;
        len += 4;
        len += 2;
        len += compact_nullable_string_len(self.error_message.as_ref())?;
        if version >= 2 {
            len += 4;
        } else if self.acquisition_lock_timeout_ms != 0_i32 {
            return Err(
                UnsupportedFieldVersion::new(79, "acquisition_lock_timeout_ms", version).into(),
            );
        }
        len += compact_array_length_len(self.responses.len() as i32);
        for el in &self.responses {
            len += el.encoded_len(version)?;
        }
        len += compact_array_length_len(self.node_endpoints.len() as i32);
        for el in &self.node_endpoints {
            len += el.encoded_len(version)?;
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct ShareAcknowledgeTopicResponse {
    /// The unique topic ID.
    pub topic_id: KafkaUuid,
    /// The topic partitions.
    pub partitions: Vec<PartitionData>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for ShareAcknowledgeTopicResponse {
    fn default() -> Self {
        Self {
            topic_id: KafkaUuid::ZERO,
            partitions: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl ShareAcknowledgeTopicResponse {
    pub fn with_topic_id(mut self, value: KafkaUuid) -> Self {
        self.topic_id = value;
        self
    }
    pub fn with_partitions(mut self, value: Vec<PartitionData>) -> Self {
        self.partitions = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let topic_id;
        let partitions;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        topic_id = read_uuid(buf)?;
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
            topic_id,
            partitions,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        write_uuid(buf, &self.topic_id);
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
        len += 16;
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
    /// The error code, or 0 if there was no error.
    pub error_code: i16,
    /// The error message, or null if there was no error.
    pub error_message: Option<KafkaString>,
    /// The current leader of the partition.
    pub current_leader: LeaderIdAndEpoch,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for PartitionData {
    fn default() -> Self {
        Self {
            partition_index: 0_i32,
            error_code: 0_i16,
            error_message: None,
            current_leader: LeaderIdAndEpoch::default(),
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
    pub fn with_current_leader(mut self, value: LeaderIdAndEpoch) -> Self {
        self.current_leader = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let partition_index;
        let error_code;
        let error_message;
        let current_leader;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        partition_index = read_i32(buf)?;
        error_code = read_i16(buf)?;
        error_message = read_compact_nullable_string(buf)?;
        current_leader = LeaderIdAndEpoch::read(buf, version)?;
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
            current_leader,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        write_i32(buf, self.partition_index);
        write_i16(buf, self.error_code);
        write_compact_nullable_string(buf, self.error_message.as_ref())?;
        self.current_leader.write(buf, version)?;
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        let mut len: usize = 0;
        len += 4;
        len += 2;
        len += compact_nullable_string_len(self.error_message.as_ref())?;
        len += self.current_leader.encoded_len(version)?;
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct LeaderIdAndEpoch {
    /// The ID of the current leader or -1 if the leader is unknown.
    pub leader_id: i32,
    /// The latest known leader epoch.
    pub leader_epoch: i32,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for LeaderIdAndEpoch {
    fn default() -> Self {
        Self {
            leader_id: 0_i32,
            leader_epoch: 0_i32,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl LeaderIdAndEpoch {
    pub fn with_leader_id(mut self, value: i32) -> Self {
        self.leader_id = value;
        self
    }
    pub fn with_leader_epoch(mut self, value: i32) -> Self {
        self.leader_epoch = value;
        self
    }
    pub fn read(buf: &mut Bytes, _version: i16) -> Result<Self> {
        let leader_id;
        let leader_epoch;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        leader_id = read_i32(buf)?;
        leader_epoch = read_i32(buf)?;
        let tagged_fields = read_tagged_fields(buf)?;
        for field in &tagged_fields {
            match field.tag {
                _ => {
                    _unknown_tagged_fields.push(field.clone());
                },
            }
        }
        Ok(Self {
            leader_id,
            leader_epoch,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, _version: i16) -> Result<()> {
        write_i32(buf, self.leader_id);
        write_i32(buf, self.leader_epoch);
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
    pub fn encoded_len(&self, _version: i16) -> Result<usize> {
        let mut len: usize = 0;
        len += 4;
        len += 4;
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct NodeEndpoint {
    /// The ID of the associated node.
    pub node_id: i32,
    /// The node's hostname.
    pub host: KafkaString,
    /// The node's port.
    pub port: i32,
    /// The rack of the node, or null if it has not been assigned to a rack.
    pub rack: Option<KafkaString>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for NodeEndpoint {
    fn default() -> Self {
        Self {
            node_id: 0_i32,
            host: KafkaString::default(),
            port: 0_i32,
            rack: None,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl NodeEndpoint {
    pub fn with_node_id(mut self, value: i32) -> Self {
        self.node_id = value;
        self
    }
    pub fn with_host(mut self, value: KafkaString) -> Self {
        self.host = value;
        self
    }
    pub fn with_port(mut self, value: i32) -> Self {
        self.port = value;
        self
    }
    pub fn with_rack(mut self, value: Option<KafkaString>) -> Self {
        self.rack = value;
        self
    }
    pub fn read(buf: &mut Bytes, _version: i16) -> Result<Self> {
        let node_id;
        let host;
        let port;
        let rack;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        node_id = read_i32(buf)?;
        host = read_compact_string(buf)?;
        port = read_i32(buf)?;
        rack = read_compact_nullable_string(buf)?;
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
            host,
            port,
            rack,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, _version: i16) -> Result<()> {
        write_i32(buf, self.node_id);
        write_compact_string(buf, &self.host)?;
        write_i32(buf, self.port);
        write_compact_nullable_string(buf, self.rack.as_ref())?;
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
    pub fn encoded_len(&self, _version: i16) -> Result<usize> {
        let mut len: usize = 0;
        len += 4;
        len += compact_string_len(&self.host)?;
        len += 4;
        len += compact_nullable_string_len(self.rack.as_ref())?;
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
