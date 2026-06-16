//! Generated from UpdateRaftVoterResponse.json - DO NOT EDIT
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
pub struct UpdateRaftVoterResponseData {
    /// The duration in milliseconds for which the request was throttled due to a quota violation,
    /// or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// The error code, or 0 if there was no error.
    pub error_code: i16,
    /// Details of the current Raft cluster leader.
    pub current_leader: CurrentLeader,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for UpdateRaftVoterResponseData {
    fn default() -> Self {
        Self {
            throttle_time_ms: 0_i32,
            error_code: 0_i16,
            current_leader: CurrentLeader::default(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl UpdateRaftVoterResponseData {
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 0 || version > 0 {
            return Err(UnsupportedVersion::new(82, version).into());
        }
        let throttle_time_ms;
        let error_code;
        let mut current_leader = CurrentLeader::default();
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        throttle_time_ms = read_i32(buf)?;
        error_code = read_i16(buf)?;
        let tagged_fields = read_tagged_fields(buf)?;
        for field in &tagged_fields {
            match field.tag {
                0 => {
                    let mut tag_buf = field.data.clone();
                    current_leader = CurrentLeader::read(&mut tag_buf, version)?;
                },
                _ => {
                    _unknown_tagged_fields.push(field.clone());
                },
            }
        }
        Ok(Self {
            throttle_time_ms,
            error_code,
            current_leader,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 0 || version > 0 {
            return Err(UnsupportedVersion::new(82, version).into());
        }
        write_i32(buf, self.throttle_time_ms);
        write_i16(buf, self.error_code);
        let mut known_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if self.current_leader != CurrentLeader::default() {
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
pub struct CurrentLeader {
    /// The replica id of the current leader or -1 if the leader is unknown.
    pub leader_id: i32,
    /// The latest known leader epoch.
    pub leader_epoch: i32,
    /// The node's hostname.
    pub host: KafkaString,
    /// The node's port.
    pub port: i32,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for CurrentLeader {
    fn default() -> Self {
        Self {
            leader_id: -1i32,
            leader_epoch: -1i32,
            host: KafkaString::default(),
            port: 0_i32,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl CurrentLeader {
    pub fn read(buf: &mut Bytes, _version: i16) -> Result<Self> {
        let leader_id;
        let leader_epoch;
        let host;
        let port;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        leader_id = read_i32(buf)?;
        leader_epoch = read_i32(buf)?;
        host = read_compact_string(buf)?;
        port = read_i32(buf)?;
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
            host,
            port,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, _version: i16) -> Result<()> {
        write_i32(buf, self.leader_id);
        write_i32(buf, self.leader_epoch);
        write_compact_string(buf, &self.host)?;
        write_i32(buf, self.port);
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
}
