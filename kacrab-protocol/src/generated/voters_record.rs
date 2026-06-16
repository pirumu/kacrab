//! Generated from VotersRecord.json - DO NOT EDIT
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
pub struct VotersRecordData {
    /// The version of the voters record.
    pub version: i16,
    /// The set of voters in the quorum for this epoch.
    pub voters: Vec<Voter>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for VotersRecordData {
    fn default() -> Self {
        Self {
            version: 0_i16,
            voters: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl VotersRecordData {
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let version_;
        let voters;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        version_ = read_i16(buf)?;
        voters = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(len.max(0) as usize);
            for _ in 0..len {
                arr.push(Voter::read(buf, version)?);
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
            version: version_,
            voters,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        write_i16(buf, self.version);
        write_compact_array_length(buf, self.voters.len() as i32);
        for el in &self.voters {
            el.write(buf, version)?;
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct Voter {
    /// The replica id of the voter in the topic partition.
    pub voter_id: i32,
    /// The directory id of the voter in the topic partition.
    pub voter_directory_id: KafkaUuid,
    /// The endpoint that can be used to communicate with the voter.
    pub endpoints: Vec<Endpoint>,
    /// The range of versions of the protocol that the replica supports.
    pub k_raft_version_feature: KRaftVersionFeature,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for Voter {
    fn default() -> Self {
        Self {
            voter_id: 0_i32,
            voter_directory_id: KafkaUuid::ZERO,
            endpoints: Vec::new(),
            k_raft_version_feature: KRaftVersionFeature::default(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl Voter {
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let voter_id;
        let voter_directory_id;
        let endpoints;
        let k_raft_version_feature;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        voter_id = read_i32(buf)?;
        voter_directory_id = read_uuid(buf)?;
        endpoints = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(len.max(0) as usize);
            for _ in 0..len {
                arr.push(Endpoint::read(buf, version)?);
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
            voter_id,
            voter_directory_id,
            endpoints,
            k_raft_version_feature,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        write_i32(buf, self.voter_id);
        write_uuid(buf, &self.voter_directory_id);
        write_compact_array_length(buf, self.endpoints.len() as i32);
        for el in &self.endpoints {
            el.write(buf, version)?;
        }
        self.k_raft_version_feature.write(buf, version)?;
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct Endpoint {
    /// The name of the endpoint.
    pub name: KafkaString,
    /// The hostname.
    pub host: KafkaString,
    /// The port.
    pub port: u16,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for Endpoint {
    fn default() -> Self {
        Self {
            name: KafkaString::default(),
            host: KafkaString::default(),
            port: 0_u16,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl Endpoint {
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
}
