//! Generated from RemoveRaftVoterRequest.json - DO NOT EDIT
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
pub struct RemoveRaftVoterRequestData {
    /// The cluster id of the request.
    pub cluster_id: Option<KafkaString>,
    /// The replica id of the voter getting removed from the topic partition.
    pub voter_id: i32,
    /// The directory id of the voter getting removed from the topic partition.
    pub voter_directory_id: KafkaUuid,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for RemoveRaftVoterRequestData {
    fn default() -> Self {
        Self {
            cluster_id: None,
            voter_id: 0_i32,
            voter_directory_id: KafkaUuid::ZERO,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl RemoveRaftVoterRequestData {
    pub fn with_cluster_id(mut self, value: Option<KafkaString>) -> Self {
        self.cluster_id = value;
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
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 0 || version > 0 {
            return Err(UnsupportedVersion::new(81, version).into());
        }
        let cluster_id;
        let voter_id;
        let voter_directory_id;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        cluster_id = read_compact_nullable_string(buf)?;
        voter_id = read_i32(buf)?;
        voter_directory_id = read_uuid(buf)?;
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
            voter_id,
            voter_directory_id,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 0 || version > 0 {
            return Err(UnsupportedVersion::new(81, version).into());
        }
        write_compact_nullable_string(buf, self.cluster_id.as_ref())?;
        write_i32(buf, self.voter_id);
        write_uuid(buf, &self.voter_directory_id);
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        if version < 0 || version > 0 {
            return Err(UnsupportedVersion::new(81, version).into());
        }
        let mut len: usize = 0;
        len += compact_nullable_string_len(self.cluster_id.as_ref())?;
        len += 4;
        len += 16;
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
