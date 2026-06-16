//! Generated from FindCoordinatorResponse.json - DO NOT EDIT
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
    pub fn with_throttle_time_ms(mut self, value: i32) -> Self {
        self.throttle_time_ms = value;
        self
    }
    pub fn with_error_code(mut self, value: i16) -> Self {
        self.error_code = value;
        self
    }
    pub fn with_error_message(mut self, value: Option<KafkaString>) -> Self {
        self.error_message = value;
        self
    }
    pub fn with_node_id(mut self, value: i32) -> Self {
        self.node_id = value;
        self
    }
    pub fn with_host(mut self, value: KafkaString) -> Self {
        self.host = value;
        self
    }
    pub fn with_port(mut self, value: i32) -> Self {
        self.port = value;
        self
    }
    pub fn with_coordinators(mut self, value: Vec<Coordinator>) -> Self {
        self.coordinators = value;
        self
    }
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
        } else if self.throttle_time_ms != 0_i32 {
            return Err(UnsupportedFieldVersion::new(10, "throttle_time_ms", version).into());
        }
        if version <= 3 {
            write_i16(buf, self.error_code);
        } else if self.error_code != 0_i16 {
            return Err(UnsupportedFieldVersion::new(10, "error_code", version).into());
        }
        if version >= 1 && version <= 3 {
            if version >= 3 {
                write_compact_nullable_string(buf, self.error_message.as_ref())?;
            } else {
                write_nullable_string(buf, self.error_message.as_ref())?;
            }
        } else if self.error_message != None {
            return Err(UnsupportedFieldVersion::new(10, "error_message", version).into());
        }
        if version <= 3 {
            write_i32(buf, self.node_id);
        } else if self.node_id != 0_i32 {
            return Err(UnsupportedFieldVersion::new(10, "node_id", version).into());
        }
        if version <= 3 {
            if version >= 3 {
                write_compact_string(buf, &self.host)?;
            } else {
                write_string(buf, &self.host)?;
            }
        } else if self.host != KafkaString::default() {
            return Err(UnsupportedFieldVersion::new(10, "host", version).into());
        }
        if version <= 3 {
            write_i32(buf, self.port);
        } else if self.port != 0_i32 {
            return Err(UnsupportedFieldVersion::new(10, "port", version).into());
        }
        if version >= 4 {
            write_compact_array_length(buf, self.coordinators.len() as i32);
            for el in &self.coordinators {
                el.write(buf, version)?;
            }
        } else if self.coordinators != Vec::new() {
            return Err(UnsupportedFieldVersion::new(10, "coordinators", version).into());
        }
        if version >= 3 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        if version < 0 || version > 6 {
            return Err(UnsupportedVersion::new(10, version).into());
        }
        let mut len: usize = 0;
        if version >= 1 {
            len += 4;
        } else if self.throttle_time_ms != 0_i32 {
            return Err(UnsupportedFieldVersion::new(10, "throttle_time_ms", version).into());
        }
        if version <= 3 {
            len += 2;
        } else if self.error_code != 0_i16 {
            return Err(UnsupportedFieldVersion::new(10, "error_code", version).into());
        }
        if version >= 1 && version <= 3 {
            if version >= 3 {
                len += compact_nullable_string_len(self.error_message.as_ref())?;
            } else {
                len += nullable_string_len(self.error_message.as_ref())?;
            }
        } else if self.error_message != None {
            return Err(UnsupportedFieldVersion::new(10, "error_message", version).into());
        }
        if version <= 3 {
            len += 4;
        } else if self.node_id != 0_i32 {
            return Err(UnsupportedFieldVersion::new(10, "node_id", version).into());
        }
        if version <= 3 {
            if version >= 3 {
                len += compact_string_len(&self.host)?;
            } else {
                len += string_len(&self.host)?;
            }
        } else if self.host != KafkaString::default() {
            return Err(UnsupportedFieldVersion::new(10, "host", version).into());
        }
        if version <= 3 {
            len += 4;
        } else if self.port != 0_i32 {
            return Err(UnsupportedFieldVersion::new(10, "port", version).into());
        }
        if version >= 4 {
            len += compact_array_length_len(self.coordinators.len() as i32);
            for el in &self.coordinators {
                len += el.encoded_len(version)?;
            }
        } else if self.coordinators != Vec::new() {
            return Err(UnsupportedFieldVersion::new(10, "coordinators", version).into());
        }
        if version >= 3 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            len += tagged_fields_len(&all_tags)?;
        }
        Ok(len)
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
    pub fn with_key(mut self, value: KafkaString) -> Self {
        self.key = value;
        self
    }
    pub fn with_node_id(mut self, value: i32) -> Self {
        self.node_id = value;
        self
    }
    pub fn with_host(mut self, value: KafkaString) -> Self {
        self.host = value;
        self
    }
    pub fn with_port(mut self, value: i32) -> Self {
        self.port = value;
        self
    }
    pub fn with_error_code(mut self, value: i16) -> Self {
        self.error_code = value;
        self
    }
    pub fn with_error_message(mut self, value: Option<KafkaString>) -> Self {
        self.error_message = value;
        self
    }
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
    pub fn encoded_len(&self, _version: i16) -> Result<usize> {
        let mut len: usize = 0;
        len += compact_string_len(&self.key)?;
        len += 4;
        len += compact_string_len(&self.host)?;
        len += 4;
        len += 2;
        len += compact_nullable_string_len(self.error_message.as_ref())?;
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
