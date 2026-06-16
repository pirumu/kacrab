//! Generated from FindCoordinatorResponse.json - DO NOT EDIT
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
pub struct FindCoordinatorResponseData {
    /// The duration in milliseconds for which the request was throttled due to a quota violation,
    /// or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// The error code, or 0 if there was no error.
    pub error_code: i16,
    /// The error message, or null if there was no error.
    pub error_message: Option<KafkaString>,
    /// The node id.
    pub node_id: i32,
    /// The host name.
    pub host: KafkaString,
    /// The port.
    pub port: i32,
    /// Each coordinator result in the response.
    pub coordinators: Vec<Coordinator>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for FindCoordinatorResponseData {
    fn default() -> Self {
        Self {
            throttle_time_ms: 0_i32,
            error_code: 0_i16,
            error_message: None,
            node_id: 0_i32,
            host: KafkaString::default(),
            port: 0_i32,
            coordinators: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl FindCoordinatorResponseData {
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 0 || version > 6 {
            return Err(UnsupportedVersion::new(10, version).into());
        }
        let mut throttle_time_ms = 0_i32;
        let mut error_code = 0_i16;
        let mut error_message = None;
        let mut node_id = 0_i32;
        let mut host = KafkaString::default();
        let mut port = 0_i32;
        let mut coordinators = Vec::new();
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 1 {
            throttle_time_ms = read_i32(buf)?;
        }
        if version <= 3 {
            error_code = read_i16(buf)?;
        }
        if version >= 1 && version <= 3 {
            if version >= 3 {
                error_message = read_compact_nullable_string(buf)?;
            } else {
                error_message = read_nullable_string(buf)?;
            }
        }
        if version <= 3 {
            node_id = read_i32(buf)?;
        }
        if version <= 3 {
            if version >= 3 {
                host = read_compact_string(buf)?;
            } else {
                host = read_string(buf)?;
            }
        }
        if version <= 3 {
            port = read_i32(buf)?;
        }
        if version >= 4 {
            coordinators = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(Coordinator::read(buf, version)?);
                }
                arr
            };
        }
        if version >= 3 {
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
            error_code,
            error_message,
            node_id,
            host,
            port,
            coordinators,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 0 || version > 6 {
            return Err(UnsupportedVersion::new(10, version).into());
        }
        if version >= 1 {
            write_i32(buf, self.throttle_time_ms);
        }
        if version <= 3 {
            write_i16(buf, self.error_code);
        }
        if version >= 1 && version <= 3 {
            if version >= 3 {
                write_compact_nullable_string(buf, self.error_message.as_ref())?;
            } else {
                write_nullable_string(buf, self.error_message.as_ref())?;
            }
        }
        if version <= 3 {
            write_i32(buf, self.node_id);
        }
        if version <= 3 {
            if version >= 3 {
                write_compact_string(buf, &self.host)?;
            } else {
                write_string(buf, &self.host)?;
            }
        }
        if version <= 3 {
            write_i32(buf, self.port);
        }
        if version >= 4 {
            write_compact_array_length(buf, self.coordinators.len() as i32);
            for el in &self.coordinators {
                el.write(buf, version)?;
            }
        }
        if version >= 3 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct Coordinator {
    /// The coordinator key.
    pub key: KafkaString,
    /// The node id.
    pub node_id: i32,
    /// The host name.
    pub host: KafkaString,
    /// The port.
    pub port: i32,
    /// The error code, or 0 if there was no error.
    pub error_code: i16,
    /// The error message, or null if there was no error.
    pub error_message: Option<KafkaString>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for Coordinator {
    fn default() -> Self {
        Self {
            key: KafkaString::default(),
            node_id: 0_i32,
            host: KafkaString::default(),
            port: 0_i32,
            error_code: 0_i16,
            error_message: None,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl Coordinator {
    pub fn read(buf: &mut Bytes, _version: i16) -> Result<Self> {
        let key;
        let node_id;
        let host;
        let port;
        let error_code;
        let error_message;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        key = read_compact_string(buf)?;
        node_id = read_i32(buf)?;
        host = read_compact_string(buf)?;
        port = read_i32(buf)?;
        error_code = read_i16(buf)?;
        error_message = read_compact_nullable_string(buf)?;
        let tagged_fields = read_tagged_fields(buf)?;
        for field in &tagged_fields {
            match field.tag {
                _ => {
                    _unknown_tagged_fields.push(field.clone());
                },
            }
        }
        Ok(Self {
            key,
            node_id,
            host,
            port,
            error_code,
            error_message,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, _version: i16) -> Result<()> {
        write_compact_string(buf, &self.key)?;
        write_i32(buf, self.node_id);
        write_compact_string(buf, &self.host)?;
        write_i32(buf, self.port);
        write_i16(buf, self.error_code);
        write_compact_nullable_string(buf, self.error_message.as_ref())?;
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
}
