//! Generated from EndQuorumEpochRequest.json - DO NOT EDIT
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
pub struct EndQuorumEpochRequestData {
    /// The cluster id.
    pub cluster_id: Option<KafkaString>,
    /// The topics.
    pub topics: Vec<TopicData>,
    /// Endpoints for the leader.
    pub leader_endpoints: Vec<LeaderEndpoint>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for EndQuorumEpochRequestData {
    fn default() -> Self {
        Self {
            cluster_id: None,
            topics: Vec::new(),
            leader_endpoints: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl EndQuorumEpochRequestData {
    pub fn with_cluster_id(mut self, value: Option<KafkaString>) -> Self {
        self.cluster_id = value;
        self
    }
    pub fn with_topics(mut self, value: Vec<TopicData>) -> Self {
        self.topics = value;
        self
    }
    pub fn with_leader_endpoints(mut self, value: Vec<LeaderEndpoint>) -> Self {
        self.leader_endpoints = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 0 || version > 1 {
            return Err(UnsupportedVersion::new(54, version).into());
        }
        let cluster_id;
        let topics;
        let mut leader_endpoints = Vec::new();
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 1 {
            cluster_id = read_compact_nullable_string(buf)?;
        } else {
            cluster_id = read_nullable_string(buf)?;
        }
        if version >= 1 {
            topics = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
                for _ in 0..len {
                    arr.push(TopicData::read(buf, version)?);
                }
                arr
            };
        } else {
            topics = {
                let len = read_array_length(buf)?;
                let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
                for _ in 0..len {
                    arr.push(TopicData::read(buf, version)?);
                }
                arr
            };
        }
        if version >= 1 {
            leader_endpoints = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
                for _ in 0..len {
                    arr.push(LeaderEndpoint::read(buf, version)?);
                }
                arr
            };
        }
        if version >= 1 {
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
            cluster_id,
            topics,
            leader_endpoints,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 0 || version > 1 {
            return Err(UnsupportedVersion::new(54, version).into());
        }
        if version >= 1 {
            write_compact_nullable_string(buf, self.cluster_id.as_ref())?;
        } else {
            write_nullable_string(buf, self.cluster_id.as_ref())?;
        }
        if version >= 1 {
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
        if version >= 1 {
            write_compact_array_length(buf, self.leader_endpoints.len() as i32);
            for el in &self.leader_endpoints {
                el.write(buf, version)?;
            }
        } else if self.leader_endpoints != Vec::new() {
            return Err(UnsupportedFieldVersion::new(54, "leader_endpoints", version).into());
        }
        if version >= 1 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        if version < 0 || version > 1 {
            return Err(UnsupportedVersion::new(54, version).into());
        }
        let mut len: usize = 0;
        if version >= 1 {
            len += compact_nullable_string_len(self.cluster_id.as_ref())?;
        } else {
            len += nullable_string_len(self.cluster_id.as_ref())?;
        }
        if version >= 1 {
            len += compact_array_length_len(self.topics.len() as i32);
            for el in &self.topics {
                len += el.encoded_len(version)?;
            }
        } else {
            len += array_length_len();
            for el in &self.topics {
                len += el.encoded_len(version)?;
            }
        }
        if version >= 1 {
            len += compact_array_length_len(self.leader_endpoints.len() as i32);
            for el in &self.leader_endpoints {
                len += el.encoded_len(version)?;
            }
        } else if self.leader_endpoints != Vec::new() {
            return Err(UnsupportedFieldVersion::new(54, "leader_endpoints", version).into());
        }
        if version >= 1 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            len += tagged_fields_len(&all_tags)?;
        }
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct TopicData {
    /// The topic name.
    pub topic_name: KafkaString,
    /// The partitions.
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
        if version >= 1 {
            topic_name = read_compact_string(buf)?;
        } else {
            topic_name = read_string(buf)?;
        }
        if version >= 1 {
            partitions = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
                for _ in 0..len {
                    arr.push(PartitionData::read(buf, version)?);
                }
                arr
            };
        } else {
            partitions = {
                let len = read_array_length(buf)?;
                let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
                for _ in 0..len {
                    arr.push(PartitionData::read(buf, version)?);
                }
                arr
            };
        }
        if version >= 1 {
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
            topic_name,
            partitions,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version >= 1 {
            write_compact_string(buf, &self.topic_name)?;
        } else {
            write_string(buf, &self.topic_name)?;
        }
        if version >= 1 {
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
        if version >= 1 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        let mut len: usize = 0;
        if version >= 1 {
            len += compact_string_len(&self.topic_name)?;
        } else {
            len += string_len(&self.topic_name)?;
        }
        if version >= 1 {
            len += compact_array_length_len(self.partitions.len() as i32);
            for el in &self.partitions {
                len += el.encoded_len(version)?;
            }
        } else {
            len += array_length_len();
            for el in &self.partitions {
                len += el.encoded_len(version)?;
            }
        }
        if version >= 1 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            len += tagged_fields_len(&all_tags)?;
        }
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct PartitionData {
    /// The partition index.
    pub partition_index: i32,
    /// The current leader ID that is resigning.
    pub leader_id: i32,
    /// The current epoch.
    pub leader_epoch: i32,
    /// A sorted list of preferred successors to start the election.
    pub preferred_successors: Vec<i32>,
    /// A sorted list of preferred candidates to start the election.
    pub preferred_candidates: Vec<ReplicaInfo>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for PartitionData {
    fn default() -> Self {
        Self {
            partition_index: 0_i32,
            leader_id: 0_i32,
            leader_epoch: 0_i32,
            preferred_successors: Vec::new(),
            preferred_candidates: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl PartitionData {
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
    pub fn with_preferred_successors(mut self, value: Vec<i32>) -> Self {
        self.preferred_successors = value;
        self
    }
    pub fn with_preferred_candidates(mut self, value: Vec<ReplicaInfo>) -> Self {
        self.preferred_candidates = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let partition_index;
        let leader_id;
        let leader_epoch;
        let mut preferred_successors = Vec::new();
        let mut preferred_candidates = Vec::new();
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        partition_index = read_i32(buf)?;
        leader_id = read_i32(buf)?;
        leader_epoch = read_i32(buf)?;
        if version == 0 {
            preferred_successors = {
                let len = read_array_length(buf)?;
                let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
                for _ in 0..len {
                    arr.push(read_i32(buf)?);
                }
                arr
            };
        }
        if version >= 1 {
            preferred_candidates = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
                for _ in 0..len {
                    arr.push(ReplicaInfo::read(buf, version)?);
                }
                arr
            };
        }
        if version >= 1 {
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
            partition_index,
            leader_id,
            leader_epoch,
            preferred_successors,
            preferred_candidates,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        write_i32(buf, self.partition_index);
        write_i32(buf, self.leader_id);
        write_i32(buf, self.leader_epoch);
        if version == 0 {
            write_array_length(buf, self.preferred_successors.len() as i32);
            for el in &self.preferred_successors {
                write_i32(buf, *el);
            }
        } else if self.preferred_successors != Vec::new() {
            return Err(UnsupportedFieldVersion::new(54, "preferred_successors", version).into());
        }
        if version >= 1 {
            write_compact_array_length(buf, self.preferred_candidates.len() as i32);
            for el in &self.preferred_candidates {
                el.write(buf, version)?;
            }
        } else if self.preferred_candidates != Vec::new() {
            return Err(UnsupportedFieldVersion::new(54, "preferred_candidates", version).into());
        }
        if version >= 1 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        let mut len: usize = 0;
        len += 4;
        len += 4;
        len += 4;
        if version == 0 {
            len += array_length_len();
            len += self.preferred_successors.len() * 4usize;
        } else if self.preferred_successors != Vec::new() {
            return Err(UnsupportedFieldVersion::new(54, "preferred_successors", version).into());
        }
        if version >= 1 {
            len += compact_array_length_len(self.preferred_candidates.len() as i32);
            for el in &self.preferred_candidates {
                len += el.encoded_len(version)?;
            }
        } else if self.preferred_candidates != Vec::new() {
            return Err(UnsupportedFieldVersion::new(54, "preferred_candidates", version).into());
        }
        if version >= 1 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            len += tagged_fields_len(&all_tags)?;
        }
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct ReplicaInfo {
    /// The ID of the candidate replica.
    pub candidate_id: i32,
    /// The directory ID of the candidate replica.
    pub candidate_directory_id: KafkaUuid,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for ReplicaInfo {
    fn default() -> Self {
        Self {
            candidate_id: 0_i32,
            candidate_directory_id: KafkaUuid::ZERO,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl ReplicaInfo {
    pub fn with_candidate_id(mut self, value: i32) -> Self {
        self.candidate_id = value;
        self
    }
    pub fn with_candidate_directory_id(mut self, value: KafkaUuid) -> Self {
        self.candidate_directory_id = value;
        self
    }
    pub fn read(buf: &mut Bytes, _version: i16) -> Result<Self> {
        let candidate_id;
        let candidate_directory_id;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        candidate_id = read_i32(buf)?;
        candidate_directory_id = read_uuid(buf)?;
        let tagged_fields = read_tagged_fields(buf)?;
        for field in &tagged_fields {
            match field.tag {
                _ => {
                    _unknown_tagged_fields.push(field.clone());
                },
            }
        }
        Ok(Self {
            candidate_id,
            candidate_directory_id,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, _version: i16) -> Result<()> {
        write_i32(buf, self.candidate_id);
        write_uuid(buf, &self.candidate_directory_id);
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
    pub fn encoded_len(&self, _version: i16) -> Result<usize> {
        let mut len: usize = 0;
        len += 4;
        len += 16;
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct LeaderEndpoint {
    /// The name of the endpoint.
    pub name: KafkaString,
    /// The node's hostname.
    pub host: KafkaString,
    /// The node's port.
    pub port: u16,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for LeaderEndpoint {
    fn default() -> Self {
        Self {
            name: KafkaString::default(),
            host: KafkaString::default(),
            port: 0_u16,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl LeaderEndpoint {
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
