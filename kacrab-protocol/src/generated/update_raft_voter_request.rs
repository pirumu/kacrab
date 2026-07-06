//! Generated from UpdateRaftVoterRequest.json - DO NOT EDIT
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
pub struct UpdateRaftVoterRequestData {
    /// The cluster id.
    pub cluster_id: Option<KafkaString>,
    /// The current leader epoch of the partition, -1 for unknown leader epoch.
    pub current_leader_epoch: i32,
    /// The replica id of the voter getting updated in the topic partition.
    pub voter_id: i32,
    /// The directory id of the voter getting updated in the topic partition.
    pub voter_directory_id: KafkaUuid,
    /// The endpoint that can be used to communicate with the leader.
    pub listeners: Vec<Listener>,
    /// The range of versions of the protocol that the replica supports.
    pub k_raft_version_feature: KRaftVersionFeature,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for UpdateRaftVoterRequestData {
    fn default() -> Self {
        Self {
            cluster_id: None,
            current_leader_epoch: 0_i32,
            voter_id: 0_i32,
            voter_directory_id: KafkaUuid::ZERO,
            listeners: Vec::new(),
            k_raft_version_feature: KRaftVersionFeature::default(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl UpdateRaftVoterRequestData {
    pub fn with_cluster_id(mut self, value: Option<KafkaString>) -> Self {
        self.cluster_id = value;
        self
    }
    pub fn with_current_leader_epoch(mut self, value: i32) -> Self {
        self.current_leader_epoch = value;
        self
    }
    pub fn with_voter_id(mut self, value: i32) -> Self {
        self.voter_id = value;
        self
    }
    pub fn with_voter_directory_id(mut self, value: KafkaUuid) -> Self {
        self.voter_directory_id = value;
        self
    }
    pub fn with_listeners(mut self, value: Vec<Listener>) -> Self {
        self.listeners = value;
        self
    }
    pub fn with_k_raft_version_feature(mut self, value: KRaftVersionFeature) -> Self {
        self.k_raft_version_feature = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 0 || version > 0 {
            return Err(UnsupportedVersion::new(82, version).into());
        }
        let cluster_id;
        let current_leader_epoch;
        let voter_id;
        let voter_directory_id;
        let listeners;
        let k_raft_version_feature;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        cluster_id = read_compact_nullable_string(buf)?;
        current_leader_epoch = read_i32(buf)?;
        voter_id = read_i32(buf)?;
        voter_directory_id = read_uuid(buf)?;
        listeners = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
            for _ in 0..len {
                arr.push(Listener::read(buf, version)?);
            }
            arr
        };
        k_raft_version_feature = KRaftVersionFeature::read(buf, version)?;
        let tagged_fields = read_tagged_fields(buf)?;
        for field in &tagged_fields {
            match field.tag {
                _ => {
                    _unknown_tagged_fields.push(field.clone());
                },
            }
        }
        Ok(Self {
            cluster_id,
            current_leader_epoch,
            voter_id,
            voter_directory_id,
            listeners,
            k_raft_version_feature,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 0 || version > 0 {
            return Err(UnsupportedVersion::new(82, version).into());
        }
        write_compact_nullable_string(buf, self.cluster_id.as_ref())?;
        write_i32(buf, self.current_leader_epoch);
        write_i32(buf, self.voter_id);
        write_uuid(buf, &self.voter_directory_id);
        write_compact_array_length(buf, self.listeners.len() as i32);
        for el in &self.listeners {
            el.write(buf, version)?;
        }
        self.k_raft_version_feature.write(buf, version)?;
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        if version < 0 || version > 0 {
            return Err(UnsupportedVersion::new(82, version).into());
        }
        let mut len: usize = 0;
        len += compact_nullable_string_len(self.cluster_id.as_ref())?;
        len += 4;
        len += 4;
        len += 16;
        len += compact_array_length_len(self.listeners.len() as i32);
        for el in &self.listeners {
            len += el.encoded_len(version)?;
        }
        len += self.k_raft_version_feature.encoded_len(version)?;
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
pub struct KRaftVersionFeature {
    /// The minimum supported KRaft protocol version.
    pub min_supported_version: i16,
    /// The maximum supported KRaft protocol version.
    pub max_supported_version: i16,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for KRaftVersionFeature {
    fn default() -> Self {
        Self {
            min_supported_version: 0_i16,
            max_supported_version: 0_i16,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl KRaftVersionFeature {
    pub fn with_min_supported_version(mut self, value: i16) -> Self {
        self.min_supported_version = value;
        self
    }
    pub fn with_max_supported_version(mut self, value: i16) -> Self {
        self.max_supported_version = value;
        self
    }
    pub fn read(buf: &mut Bytes, _version: i16) -> Result<Self> {
        let min_supported_version;
        let max_supported_version;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        min_supported_version = read_i16(buf)?;
        max_supported_version = read_i16(buf)?;
        let tagged_fields = read_tagged_fields(buf)?;
        for field in &tagged_fields {
            match field.tag {
                _ => {
                    _unknown_tagged_fields.push(field.clone());
                },
            }
        }
        Ok(Self {
            min_supported_version,
            max_supported_version,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, _version: i16) -> Result<()> {
        write_i16(buf, self.min_supported_version);
        write_i16(buf, self.max_supported_version);
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
    pub fn encoded_len(&self, _version: i16) -> Result<usize> {
        let mut len: usize = 0;
        len += 2;
        len += 2;
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
