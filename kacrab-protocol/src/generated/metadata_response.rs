//! Generated from MetadataResponse.json - DO NOT EDIT
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
pub struct MetadataResponseData {
    /// The duration in milliseconds for which the request was throttled due to a quota violation,
    /// or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// A list of brokers present in the cluster.
    pub brokers: Vec<MetadataResponseBroker>,
    /// The cluster ID that responding broker belongs to.
    pub cluster_id: Option<KafkaString>,
    /// The ID of the controller broker.
    pub controller_id: i32,
    /// Each topic in the response.
    pub topics: Vec<MetadataResponseTopic>,
    /// 32-bit bitfield to represent authorized operations for this cluster.
    pub cluster_authorized_operations: i32,
    /// The top-level error code, or 0 if there was no error.
    pub error_code: i16,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for MetadataResponseData {
    fn default() -> Self {
        Self {
            throttle_time_ms: 0_i32,
            brokers: Vec::new(),
            cluster_id: None,
            controller_id: -1i32,
            topics: Vec::new(),
            cluster_authorized_operations: i32::MIN,
            error_code: 0_i16,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl MetadataResponseData {
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 0 || version > 13 {
            return Err(UnsupportedVersion::new(3, version).into());
        }
        let mut throttle_time_ms = 0_i32;
        let brokers;
        let mut cluster_id = None;
        let mut controller_id = -1i32;
        let topics;
        let mut cluster_authorized_operations = i32::MIN;
        let mut error_code = 0_i16;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 3 {
            throttle_time_ms = read_i32(buf)?;
        }
        if version >= 9 {
            brokers = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(MetadataResponseBroker::read(buf, version)?);
                }
                arr
            };
        } else {
            brokers = {
                let len = read_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(MetadataResponseBroker::read(buf, version)?);
                }
                arr
            };
        }
        if version >= 2 {
            if version >= 9 {
                cluster_id = read_compact_nullable_string(buf)?;
            } else {
                cluster_id = read_nullable_string(buf)?;
            }
        }
        if version >= 1 {
            controller_id = read_i32(buf)?;
        }
        if version >= 9 {
            topics = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(MetadataResponseTopic::read(buf, version)?);
                }
                arr
            };
        } else {
            topics = {
                let len = read_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(MetadataResponseTopic::read(buf, version)?);
                }
                arr
            };
        }
        if version >= 8 && version <= 10 {
            cluster_authorized_operations = read_i32(buf)?;
        }
        if version >= 13 {
            error_code = read_i16(buf)?;
        }
        if version >= 9 {
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
            throttle_time_ms,
            brokers,
            cluster_id,
            controller_id,
            topics,
            cluster_authorized_operations,
            error_code,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 0 || version > 13 {
            return Err(UnsupportedVersion::new(3, version).into());
        }
        if version >= 3 {
            write_i32(buf, self.throttle_time_ms);
        }
        if version >= 9 {
            write_compact_array_length(buf, self.brokers.len() as i32);
            for el in &self.brokers {
                el.write(buf, version)?;
            }
        } else {
            write_array_length(buf, self.brokers.len() as i32);
            for el in &self.brokers {
                el.write(buf, version)?;
            }
        }
        if version >= 2 {
            if version >= 9 {
                write_compact_nullable_string(buf, self.cluster_id.as_ref())?;
            } else {
                write_nullable_string(buf, self.cluster_id.as_ref())?;
            }
        }
        if version >= 1 {
            write_i32(buf, self.controller_id);
        }
        if version >= 9 {
            write_compact_array_length(buf, self.topics.len() as i32);
            for el in &self.topics {
                el.write(buf, version)?;
            }
        } else {
            write_array_length(buf, self.topics.len() as i32);
            for el in &self.topics {
                el.write(buf, version)?;
            }
        }
        if version >= 8 && version <= 10 {
            write_i32(buf, self.cluster_authorized_operations);
        }
        if version >= 13 {
            write_i16(buf, self.error_code);
        }
        if version >= 9 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct MetadataResponseBroker {
    /// The broker ID.
    pub node_id: i32,
    /// The broker hostname.
    pub host: KafkaString,
    /// The broker port.
    pub port: i32,
    /// The rack of the broker, or null if it has not been assigned to a rack.
    pub rack: Option<KafkaString>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for MetadataResponseBroker {
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
impl MetadataResponseBroker {
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let node_id;
        let host;
        let port;
        let mut rack = None;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        node_id = read_i32(buf)?;
        if version >= 9 {
            host = read_compact_string(buf)?;
        } else {
            host = read_string(buf)?;
        }
        port = read_i32(buf)?;
        if version >= 1 {
            if version >= 9 {
                rack = read_compact_nullable_string(buf)?;
            } else {
                rack = read_nullable_string(buf)?;
            }
        }
        if version >= 9 {
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
            node_id,
            host,
            port,
            rack,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        write_i32(buf, self.node_id);
        if version >= 9 {
            write_compact_string(buf, &self.host)?;
        } else {
            write_string(buf, &self.host)?;
        }
        write_i32(buf, self.port);
        if version >= 1 {
            if version >= 9 {
                write_compact_nullable_string(buf, self.rack.as_ref())?;
            } else {
                write_nullable_string(buf, self.rack.as_ref())?;
            }
        }
        if version >= 9 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct MetadataResponseTopic {
    /// The topic error, or 0 if there was no error.
    pub error_code: i16,
    /// The topic name. Null for non-existing topics queried by ID. This is never null when
    /// ErrorCode is zero. One of Name and TopicId is always populated.
    pub name: Option<KafkaString>,
    /// The topic id. Zero for non-existing topics queried by name. This is never zero when
    /// ErrorCode is zero. One of Name and TopicId is always populated.
    pub topic_id: KafkaUuid,
    /// True if the topic is internal.
    pub is_internal: bool,
    /// Each partition in the topic.
    pub partitions: Vec<MetadataResponsePartition>,
    /// 32-bit bitfield to represent authorized operations for this topic.
    pub topic_authorized_operations: i32,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for MetadataResponseTopic {
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
impl MetadataResponseTopic {
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let error_code;
        let name;
        let mut topic_id = KafkaUuid::ZERO;
        let mut is_internal = false;
        let partitions;
        let mut topic_authorized_operations = i32::MIN;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        error_code = read_i16(buf)?;
        if version >= 12 {
            name = read_compact_nullable_string(buf)?;
        } else {
            if version >= 9 {
                name = Some(read_compact_string(buf)?);
            } else {
                name = Some(read_string(buf)?);
            }
        }
        if version >= 10 {
            topic_id = read_uuid(buf)?;
        }
        if version >= 1 {
            is_internal = read_bool(buf)?;
        }
        if version >= 9 {
            partitions = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(MetadataResponsePartition::read(buf, version)?);
                }
                arr
            };
        } else {
            partitions = {
                let len = read_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(MetadataResponsePartition::read(buf, version)?);
                }
                arr
            };
        }
        if version >= 8 {
            topic_authorized_operations = read_i32(buf)?;
        }
        if version >= 9 {
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
        if version >= 12 {
            write_compact_nullable_string(buf, self.name.as_ref())?;
        } else {
            {
                let _nn_default = KafkaString::default();
                let _nn_val = self.name.as_ref().unwrap_or(&_nn_default);
                if version >= 9 {
                    write_compact_string(buf, _nn_val)?;
                } else {
                    write_string(buf, _nn_val)?;
                }
            }
        }
        if version >= 10 {
            write_uuid(buf, &self.topic_id);
        }
        if version >= 1 {
            write_bool(buf, self.is_internal);
        }
        if version >= 9 {
            write_compact_array_length(buf, self.partitions.len() as i32);
            for el in &self.partitions {
                el.write(buf, version)?;
            }
        } else {
            write_array_length(buf, self.partitions.len() as i32);
            for el in &self.partitions {
                el.write(buf, version)?;
            }
        }
        if version >= 8 {
            write_i32(buf, self.topic_authorized_operations);
        }
        if version >= 9 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct MetadataResponsePartition {
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
    /// The set of offline replicas of this partition.
    pub offline_replicas: Vec<i32>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for MetadataResponsePartition {
    fn default() -> Self {
        Self {
            error_code: 0_i16,
            partition_index: 0_i32,
            leader_id: 0_i32,
            leader_epoch: -1i32,
            replica_nodes: Vec::new(),
            isr_nodes: Vec::new(),
            offline_replicas: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl MetadataResponsePartition {
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let error_code;
        let partition_index;
        let leader_id;
        let mut leader_epoch = -1i32;
        let replica_nodes;
        let isr_nodes;
        let mut offline_replicas = Vec::new();
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        error_code = read_i16(buf)?;
        partition_index = read_i32(buf)?;
        leader_id = read_i32(buf)?;
        if version >= 7 {
            leader_epoch = read_i32(buf)?;
        }
        if version >= 9 {
            replica_nodes = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(read_i32(buf)?);
                }
                arr
            };
        } else {
            replica_nodes = {
                let len = read_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(read_i32(buf)?);
                }
                arr
            };
        }
        if version >= 9 {
            isr_nodes = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(read_i32(buf)?);
                }
                arr
            };
        } else {
            isr_nodes = {
                let len = read_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(read_i32(buf)?);
                }
                arr
            };
        }
        if version >= 5 {
            if version >= 9 {
                offline_replicas = {
                    let len = read_compact_array_length(buf)?;
                    let mut arr = Vec::with_capacity(len.max(0) as usize);
                    for _ in 0..len {
                        arr.push(read_i32(buf)?);
                    }
                    arr
                };
            } else {
                offline_replicas = {
                    let len = read_array_length(buf)?;
                    let mut arr = Vec::with_capacity(len.max(0) as usize);
                    for _ in 0..len {
                        arr.push(read_i32(buf)?);
                    }
                    arr
                };
            }
        }
        if version >= 9 {
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
            error_code,
            partition_index,
            leader_id,
            leader_epoch,
            replica_nodes,
            isr_nodes,
            offline_replicas,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        write_i16(buf, self.error_code);
        write_i32(buf, self.partition_index);
        write_i32(buf, self.leader_id);
        if version >= 7 {
            write_i32(buf, self.leader_epoch);
        }
        if version >= 9 {
            write_compact_array_length(buf, self.replica_nodes.len() as i32);
            for el in &self.replica_nodes {
                write_i32(buf, *el);
            }
        } else {
            write_array_length(buf, self.replica_nodes.len() as i32);
            for el in &self.replica_nodes {
                write_i32(buf, *el);
            }
        }
        if version >= 9 {
            write_compact_array_length(buf, self.isr_nodes.len() as i32);
            for el in &self.isr_nodes {
                write_i32(buf, *el);
            }
        } else {
            write_array_length(buf, self.isr_nodes.len() as i32);
            for el in &self.isr_nodes {
                write_i32(buf, *el);
            }
        }
        if version >= 5 {
            if version >= 9 {
                write_compact_array_length(buf, self.offline_replicas.len() as i32);
                for el in &self.offline_replicas {
                    write_i32(buf, *el);
                }
            } else {
                write_array_length(buf, self.offline_replicas.len() as i32);
                for el in &self.offline_replicas {
                    write_i32(buf, *el);
                }
            }
        }
        if version >= 9 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
}
