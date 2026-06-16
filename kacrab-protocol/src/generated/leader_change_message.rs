//! Generated from LeaderChangeMessage.json - DO NOT EDIT
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
pub struct LeaderChangeMessageData {
    /// The version of the leader change message.
    pub version: i16,
    /// The ID of the newly elected leader.
    pub leader_id: i32,
    /// The set of voters in the quorum for this epoch.
    pub voters: Vec<Voter>,
    /// The voters who voted for the leader at the time of election.
    pub granting_voters: Vec<Voter>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for LeaderChangeMessageData {
    fn default() -> Self {
        Self {
            version: 0_i16,
            leader_id: 0_i32,
            voters: Vec::new(),
            granting_voters: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl LeaderChangeMessageData {
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let version_;
        let leader_id;
        let voters;
        let granting_voters;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        version_ = read_i16(buf)?;
        leader_id = read_i32(buf)?;
        voters = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(len.max(0) as usize);
            for _ in 0..len {
                arr.push(Voter::read(buf, version)?);
            }
            arr
        };
        granting_voters = {
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
            leader_id,
            voters,
            granting_voters,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        write_i16(buf, self.version);
        write_i32(buf, self.leader_id);
        write_compact_array_length(buf, self.voters.len() as i32);
        for el in &self.voters {
            el.write(buf, version)?;
        }
        write_compact_array_length(buf, self.granting_voters.len() as i32);
        for el in &self.granting_voters {
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
    /// The ID of the voter.
    pub voter_id: i32,
    /// The directory id of the voter.
    pub voter_directory_id: KafkaUuid,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for Voter {
    fn default() -> Self {
        Self {
            voter_id: 0_i32,
            voter_directory_id: KafkaUuid::ZERO,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl Voter {
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let voter_id;
        let mut voter_directory_id = KafkaUuid::ZERO;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        voter_id = read_i32(buf)?;
        if version >= 1 {
            voter_directory_id = read_uuid(buf)?;
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
            voter_id,
            voter_directory_id,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        write_i32(buf, self.voter_id);
        if version >= 1 {
            write_uuid(buf, &self.voter_directory_id);
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
}
