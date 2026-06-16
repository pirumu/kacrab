//! Generated from FetchSnapshotResponse.json - DO NOT EDIT
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
pub struct FetchSnapshotResponseData {
    /// The duration in milliseconds for which the request was throttled due to a quota violation,
    /// or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// The top level response error code.
    pub error_code: i16,
    /// The topics to fetch.
    pub topics: Vec<TopicSnapshot>,
    /// Endpoints for all current-leaders enumerated in PartitionSnapshot.
    pub node_endpoints: Vec<NodeEndpoint>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for FetchSnapshotResponseData {
    fn default() -> Self {
        Self {
            throttle_time_ms: 0_i32,
            error_code: 0_i16,
            topics: Vec::new(),
            node_endpoints: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl FetchSnapshotResponseData {
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 0 || version > 1 {
            return Err(UnsupportedVersion::new(59, version).into());
        }
        let throttle_time_ms;
        let error_code;
        let topics;
        let mut node_endpoints = Vec::new();
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        throttle_time_ms = read_i32(buf)?;
        error_code = read_i16(buf)?;
        topics = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(len.max(0) as usize);
            for _ in 0..len {
                arr.push(TopicSnapshot::read(buf, version)?);
            }
            arr
        };
        let tagged_fields = read_tagged_fields(buf)?;
        for field in &tagged_fields {
            match field.tag {
                0 => {
                    if version >= 1 {
                        let mut tag_buf = field.data.clone();
                        node_endpoints = {
                            let len = read_compact_array_length(&mut tag_buf)?;
                            let mut arr = Vec::with_capacity(len.max(0) as usize);
                            for _ in 0..len {
                                arr.push(NodeEndpoint::read(&mut tag_buf, version)?);
                            }
                            arr
                        };
                    }
                },
                _ => {
                    _unknown_tagged_fields.push(field.clone());
                },
            }
        }
        Ok(Self {
            throttle_time_ms,
            error_code,
            topics,
            node_endpoints,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 0 || version > 1 {
            return Err(UnsupportedVersion::new(59, version).into());
        }
        write_i32(buf, self.throttle_time_ms);
        write_i16(buf, self.error_code);
        write_compact_array_length(buf, self.topics.len() as i32);
        for el in &self.topics {
            el.write(buf, version)?;
        }
        let mut known_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 1 && !self.node_endpoints.is_empty() {
            let mut tag_buf = BytesMut::new();
            write_compact_array_length(&mut tag_buf, self.node_endpoints.len() as i32);
            for el in &self.node_endpoints {
                el.write(&mut tag_buf, version)?;
            }
            known_tagged_fields.push(RawTaggedField {
                tag: 0,
                data: tag_buf.freeze(),
            });
        }
        let mut all_tags = known_tagged_fields;
        all_tags.extend(self._unknown_tagged_fields.iter().cloned());
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct TopicSnapshot {
    /// The name of the topic to fetch.
    pub name: KafkaString,
    /// The partitions to fetch.
    pub partitions: Vec<PartitionSnapshot>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for TopicSnapshot {
    fn default() -> Self {
        Self {
            name: KafkaString::default(),
            partitions: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl TopicSnapshot {
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let name;
        let partitions;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        name = read_compact_string(buf)?;
        partitions = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(len.max(0) as usize);
            for _ in 0..len {
                arr.push(PartitionSnapshot::read(buf, version)?);
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
            name,
            partitions,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        write_compact_string(buf, &self.name)?;
        write_compact_array_length(buf, self.partitions.len() as i32);
        for el in &self.partitions {
            el.write(buf, version)?;
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct PartitionSnapshot {
    /// The partition index.
    pub index: i32,
    /// The error code, or 0 if there was no fetch error.
    pub error_code: i16,
    /// The snapshot endOffset and epoch fetched.
    pub snapshot_id: SnapshotId,
    /// The leader of the partition at the time of the snapshot.
    pub current_leader: LeaderIdAndEpoch,
    /// The total size of the snapshot.
    pub size: i64,
    /// The starting byte position within the snapshot included in the Bytes field.
    pub position: i64,
    /// Snapshot data in records format which may not be aligned on an offset boundary.
    pub unaligned_records: Option<Bytes>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for PartitionSnapshot {
    fn default() -> Self {
        Self {
            index: 0_i32,
            error_code: 0_i16,
            snapshot_id: SnapshotId::default(),
            current_leader: LeaderIdAndEpoch::default(),
            size: 0_i64,
            position: 0_i64,
            unaligned_records: None,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl PartitionSnapshot {
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let index;
        let error_code;
        let snapshot_id;
        let mut current_leader = LeaderIdAndEpoch::default();
        let size;
        let position;
        let unaligned_records;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        index = read_i32(buf)?;
        error_code = read_i16(buf)?;
        snapshot_id = SnapshotId::read(buf, version)?;
        size = read_i64(buf)?;
        position = read_i64(buf)?;
        unaligned_records = read_compact_nullable_bytes(buf)?;
        let tagged_fields = read_tagged_fields(buf)?;
        for field in &tagged_fields {
            match field.tag {
                0 => {
                    let mut tag_buf = field.data.clone();
                    current_leader = LeaderIdAndEpoch::read(&mut tag_buf, version)?;
                },
                _ => {
                    _unknown_tagged_fields.push(field.clone());
                },
            }
        }
        Ok(Self {
            index,
            error_code,
            snapshot_id,
            current_leader,
            size,
            position,
            unaligned_records,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        write_i32(buf, self.index);
        write_i16(buf, self.error_code);
        self.snapshot_id.write(buf, version)?;
        write_i64(buf, self.size);
        write_i64(buf, self.position);
        write_compact_nullable_bytes(buf, self.unaligned_records.as_ref().map(|b| b.as_ref()))?;
        let mut known_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if self.current_leader != LeaderIdAndEpoch::default() {
            let mut tag_buf = BytesMut::new();
            self.current_leader.write(&mut tag_buf, version)?;
            known_tagged_fields.push(RawTaggedField {
                tag: 0,
                data: tag_buf.freeze(),
            });
        }
        let mut all_tags = known_tagged_fields;
        all_tags.extend(self._unknown_tagged_fields.iter().cloned());
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct SnapshotId {
    /// The snapshot end offset.
    pub end_offset: i64,
    /// The snapshot epoch.
    pub epoch: i32,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for SnapshotId {
    fn default() -> Self {
        Self {
            end_offset: 0_i64,
            epoch: 0_i32,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl SnapshotId {
    pub fn read(buf: &mut Bytes, _version: i16) -> Result<Self> {
        let end_offset;
        let epoch;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        end_offset = read_i64(buf)?;
        epoch = read_i32(buf)?;
        let tagged_fields = read_tagged_fields(buf)?;
        for field in &tagged_fields {
            match field.tag {
                _ => {
                    _unknown_tagged_fields.push(field.clone());
                },
            }
        }
        Ok(Self {
            end_offset,
            epoch,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, _version: i16) -> Result<()> {
        write_i64(buf, self.end_offset);
        write_i32(buf, self.epoch);
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
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
}
#[derive(Debug, Clone, PartialEq)]
pub struct NodeEndpoint {
    /// The ID of the associated node.
    pub node_id: i32,
    /// The node's hostname.
    pub host: KafkaString,
    /// The node's port.
    pub port: u16,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for NodeEndpoint {
    fn default() -> Self {
        Self {
            node_id: 0_i32,
            host: KafkaString::default(),
            port: 0_u16,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl NodeEndpoint {
    pub fn read(buf: &mut Bytes, _version: i16) -> Result<Self> {
        let node_id;
        let host;
        let port;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        node_id = read_i32(buf)?;
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
            node_id,
            host,
            port,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, _version: i16) -> Result<()> {
        write_i32(buf, self.node_id);
        write_compact_string(buf, &self.host)?;
        write_u16(buf, self.port);
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
}
