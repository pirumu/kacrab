//! Generated from FetchSnapshotRequest.json - DO NOT EDIT
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
pub struct FetchSnapshotRequestData {
    /// The clusterId if known, this is used to validate metadata fetches prior to broker
    /// registration.
    pub cluster_id: Option<KafkaString>,
    /// The broker ID of the follower.
    pub replica_id: i32,
    /// The maximum bytes to fetch from all of the snapshots.
    pub max_bytes: i32,
    /// The topics to fetch.
    pub topics: Vec<TopicSnapshot>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for FetchSnapshotRequestData {
    fn default() -> Self {
        Self {
            cluster_id: None,
            replica_id: -1i32,
            max_bytes: i32::MAX,
            topics: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl FetchSnapshotRequestData {
    pub fn with_cluster_id(mut self, value: Option<KafkaString>) -> Self {
        self.cluster_id = value;
        self
    }
    pub fn with_replica_id(mut self, value: i32) -> Self {
        self.replica_id = value;
        self
    }
    pub fn with_max_bytes(mut self, value: i32) -> Self {
        self.max_bytes = value;
        self
    }
    pub fn with_topics(mut self, value: Vec<TopicSnapshot>) -> Self {
        self.topics = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 0 || version > 1 {
            return Err(UnsupportedVersion::new(59, version).into());
        }
        let mut cluster_id = None;
        let replica_id;
        let max_bytes;
        let topics;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        replica_id = read_i32(buf)?;
        max_bytes = read_i32(buf)?;
        topics = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
            for _ in 0..len {
                arr.push(TopicSnapshot::read(buf, version)?);
            }
            arr
        };
        let tagged_fields = read_tagged_fields(buf)?;
        for field in &tagged_fields {
            match field.tag {
                0 => {
                    let mut tag_buf = field.data.clone();
                    cluster_id = read_compact_nullable_string(&mut tag_buf)?;
                },
                _ => {
                    _unknown_tagged_fields.push(field.clone());
                },
            }
        }
        Ok(Self {
            cluster_id,
            replica_id,
            max_bytes,
            topics,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 0 || version > 1 {
            return Err(UnsupportedVersion::new(59, version).into());
        }
        write_i32(buf, self.replica_id);
        write_i32(buf, self.max_bytes);
        write_compact_array_length(buf, self.topics.len() as i32);
        for el in &self.topics {
            el.write(buf, version)?;
        }
        let mut known_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if self.cluster_id.is_some() {
            let mut tag_buf = BytesMut::new();
            write_compact_nullable_string(&mut tag_buf, self.cluster_id.as_ref())?;
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
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        if version < 0 || version > 1 {
            return Err(UnsupportedVersion::new(59, version).into());
        }
        let mut len: usize = 0;
        len += 4;
        len += 4;
        len += compact_array_length_len(self.topics.len() as i32);
        for el in &self.topics {
            len += el.encoded_len(version)?;
        }
        let mut known_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if self.cluster_id.is_some() {
            let mut tag_buf = BytesMut::new();
            write_compact_nullable_string(&mut tag_buf, self.cluster_id.as_ref())?;
            known_tagged_fields.push(RawTaggedField {
                tag: 0,
                data: tag_buf.freeze(),
            });
        }
        let mut all_tags = known_tagged_fields;
        all_tags.extend(self._unknown_tagged_fields.iter().cloned());
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
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
    pub fn with_name(mut self, value: KafkaString) -> Self {
        self.name = value;
        self
    }
    pub fn with_partitions(mut self, value: Vec<PartitionSnapshot>) -> Self {
        self.partitions = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let name;
        let partitions;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        name = read_compact_string(buf)?;
        partitions = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
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
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        let mut len: usize = 0;
        len += compact_string_len(&self.name)?;
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
pub struct PartitionSnapshot {
    /// The partition index.
    pub partition: i32,
    /// The current leader epoch of the partition, -1 for unknown leader epoch.
    pub current_leader_epoch: i32,
    /// The snapshot endOffset and epoch to fetch.
    pub snapshot_id: SnapshotId,
    /// The byte position within the snapshot to start fetching from.
    pub position: i64,
    /// The directory id of the follower fetching.
    pub replica_directory_id: KafkaUuid,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for PartitionSnapshot {
    fn default() -> Self {
        Self {
            partition: 0_i32,
            current_leader_epoch: 0_i32,
            snapshot_id: SnapshotId::default(),
            position: 0_i64,
            replica_directory_id: KafkaUuid::ZERO,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl PartitionSnapshot {
    pub fn with_partition(mut self, value: i32) -> Self {
        self.partition = value;
        self
    }
    pub fn with_current_leader_epoch(mut self, value: i32) -> Self {
        self.current_leader_epoch = value;
        self
    }
    pub fn with_snapshot_id(mut self, value: SnapshotId) -> Self {
        self.snapshot_id = value;
        self
    }
    pub fn with_position(mut self, value: i64) -> Self {
        self.position = value;
        self
    }
    pub fn with_replica_directory_id(mut self, value: KafkaUuid) -> Self {
        self.replica_directory_id = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let partition;
        let current_leader_epoch;
        let snapshot_id;
        let position;
        let mut replica_directory_id = KafkaUuid::ZERO;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        partition = read_i32(buf)?;
        current_leader_epoch = read_i32(buf)?;
        snapshot_id = SnapshotId::read(buf, version)?;
        position = read_i64(buf)?;
        let tagged_fields = read_tagged_fields(buf)?;
        for field in &tagged_fields {
            match field.tag {
                0 => {
                    if version >= 1 {
                        let mut tag_buf = field.data.clone();
                        replica_directory_id = read_uuid(&mut tag_buf)?;
                    }
                },
                _ => {
                    _unknown_tagged_fields.push(field.clone());
                },
            }
        }
        Ok(Self {
            partition,
            current_leader_epoch,
            snapshot_id,
            position,
            replica_directory_id,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        write_i32(buf, self.partition);
        write_i32(buf, self.current_leader_epoch);
        self.snapshot_id.write(buf, version)?;
        write_i64(buf, self.position);
        let mut known_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 1 && !self.replica_directory_id.is_nil() {
            let mut tag_buf = BytesMut::new();
            write_uuid(&mut tag_buf, &self.replica_directory_id);
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
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        let mut len: usize = 0;
        len += 4;
        len += 4;
        len += self.snapshot_id.encoded_len(version)?;
        len += 8;
        let mut known_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 1 && !self.replica_directory_id.is_nil() {
            let mut tag_buf = BytesMut::new();
            write_uuid(&mut tag_buf, &self.replica_directory_id);
            known_tagged_fields.push(RawTaggedField {
                tag: 0,
                data: tag_buf.freeze(),
            });
        }
        let mut all_tags = known_tagged_fields;
        all_tags.extend(self._unknown_tagged_fields.iter().cloned());
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct SnapshotId {
    /// The end offset of the snapshot.
    pub end_offset: i64,
    /// The epoch of the snapshot.
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
    pub fn with_end_offset(mut self, value: i64) -> Self {
        self.end_offset = value;
        self
    }
    pub fn with_epoch(mut self, value: i32) -> Self {
        self.epoch = value;
        self
    }
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
    pub fn encoded_len(&self, _version: i16) -> Result<usize> {
        let mut len: usize = 0;
        len += 8;
        len += 4;
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
