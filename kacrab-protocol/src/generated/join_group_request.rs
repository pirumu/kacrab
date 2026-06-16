//! Generated from JoinGroupRequest.json - DO NOT EDIT
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
pub struct JoinGroupRequestData {
    /// The group identifier.
    pub group_id: KafkaString,
    /// The coordinator considers the consumer dead if it receives no heartbeat after this timeout
    /// in milliseconds.
    pub session_timeout_ms: i32,
    /// The maximum time in milliseconds that the coordinator will wait for each member to rejoin
    /// when rebalancing the group.
    pub rebalance_timeout_ms: i32,
    /// The member id assigned by the group coordinator.
    pub member_id: KafkaString,
    /// The unique identifier of the consumer instance provided by end user.
    pub group_instance_id: Option<KafkaString>,
    /// The unique name the for class of protocols implemented by the group we want to join.
    pub protocol_type: KafkaString,
    /// The list of protocols that the member supports.
    pub protocols: Vec<JoinGroupRequestProtocol>,
    /// The reason why the member (re-)joins the group.
    pub reason: Option<KafkaString>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for JoinGroupRequestData {
    fn default() -> Self {
        Self {
            group_id: KafkaString::default(),
            session_timeout_ms: 0_i32,
            rebalance_timeout_ms: -1i32,
            member_id: KafkaString::default(),
            group_instance_id: None,
            protocol_type: KafkaString::default(),
            protocols: Vec::new(),
            reason: None,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl JoinGroupRequestData {
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 0 || version > 9 {
            return Err(UnsupportedVersion::new(11, version).into());
        }
        let group_id;
        let session_timeout_ms;
        let mut rebalance_timeout_ms = -1i32;
        let member_id;
        let mut group_instance_id = None;
        let protocol_type;
        let protocols;
        let mut reason = None;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 6 {
            group_id = read_compact_string(buf)?;
        } else {
            group_id = read_string(buf)?;
        }
        session_timeout_ms = read_i32(buf)?;
        if version >= 1 {
            rebalance_timeout_ms = read_i32(buf)?;
        }
        if version >= 6 {
            member_id = read_compact_string(buf)?;
        } else {
            member_id = read_string(buf)?;
        }
        if version >= 5 {
            if version >= 6 {
                group_instance_id = read_compact_nullable_string(buf)?;
            } else {
                group_instance_id = read_nullable_string(buf)?;
            }
        }
        if version >= 6 {
            protocol_type = read_compact_string(buf)?;
        } else {
            protocol_type = read_string(buf)?;
        }
        if version >= 6 {
            protocols = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(JoinGroupRequestProtocol::read(buf, version)?);
                }
                arr
            };
        } else {
            protocols = {
                let len = read_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(JoinGroupRequestProtocol::read(buf, version)?);
                }
                arr
            };
        }
        if version >= 8 {
            reason = read_compact_nullable_string(buf)?;
        }
        if version >= 6 {
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
            group_id,
            session_timeout_ms,
            rebalance_timeout_ms,
            member_id,
            group_instance_id,
            protocol_type,
            protocols,
            reason,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 0 || version > 9 {
            return Err(UnsupportedVersion::new(11, version).into());
        }
        if version >= 6 {
            write_compact_string(buf, &self.group_id)?;
        } else {
            write_string(buf, &self.group_id)?;
        }
        write_i32(buf, self.session_timeout_ms);
        if version >= 1 {
            write_i32(buf, self.rebalance_timeout_ms);
        }
        if version >= 6 {
            write_compact_string(buf, &self.member_id)?;
        } else {
            write_string(buf, &self.member_id)?;
        }
        if version >= 5 {
            if version >= 6 {
                write_compact_nullable_string(buf, self.group_instance_id.as_ref())?;
            } else {
                write_nullable_string(buf, self.group_instance_id.as_ref())?;
            }
        }
        if version >= 6 {
            write_compact_string(buf, &self.protocol_type)?;
        } else {
            write_string(buf, &self.protocol_type)?;
        }
        if version >= 6 {
            write_compact_array_length(buf, self.protocols.len() as i32);
            for el in &self.protocols {
                el.write(buf, version)?;
            }
        } else {
            write_array_length(buf, self.protocols.len() as i32);
            for el in &self.protocols {
                el.write(buf, version)?;
            }
        }
        if version >= 8 {
            write_compact_nullable_string(buf, self.reason.as_ref())?;
        }
        if version >= 6 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct JoinGroupRequestProtocol {
    /// The protocol name.
    pub name: KafkaString,
    /// The protocol metadata.
    pub metadata: Bytes,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for JoinGroupRequestProtocol {
    fn default() -> Self {
        Self {
            name: KafkaString::default(),
            metadata: Bytes::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl JoinGroupRequestProtocol {
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let name;
        let metadata;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 6 {
            name = read_compact_string(buf)?;
        } else {
            name = read_string(buf)?;
        }
        if version >= 6 {
            metadata = read_compact_bytes(buf)?;
        } else {
            metadata = read_bytes(buf)?;
        }
        if version >= 6 {
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
            name,
            metadata,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version >= 6 {
            write_compact_string(buf, &self.name)?;
        } else {
            write_string(buf, &self.name)?;
        }
        if version >= 6 {
            write_compact_bytes(buf, &self.metadata)?;
        } else {
            write_bytes(buf, &self.metadata)?;
        }
        if version >= 6 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
}
