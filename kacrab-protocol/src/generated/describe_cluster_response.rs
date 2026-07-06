//! Generated from DescribeClusterResponse.json - DO NOT EDIT
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
pub struct DescribeClusterResponseData {
    /// The duration in milliseconds for which the request was throttled due to a quota violation,
    /// or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// The top-level error code, or 0 if there was no error.
    pub error_code: i16,
    /// The top-level error message, or null if there was no error.
    pub error_message: Option<KafkaString>,
    /// The endpoint type that was described. 1=brokers, 2=controllers.
    pub endpoint_type: i8,
    /// The cluster ID that responding broker belongs to.
    pub cluster_id: KafkaString,
    /// The ID of the controller. When handled by a controller, returns the current voter leader
    /// ID. When handled by a broker, returns a random alive broker ID as a fallback.
    pub controller_id: i32,
    /// Each broker in the response.
    pub brokers: Vec<DescribeClusterBroker>,
    /// 32-bit bitfield to represent authorized operations for this cluster.
    pub cluster_authorized_operations: i32,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for DescribeClusterResponseData {
    fn default() -> Self {
        Self {
            throttle_time_ms: 0_i32,
            error_code: 0_i16,
            error_message: None,
            endpoint_type: 1i8,
            cluster_id: KafkaString::default(),
            controller_id: -1i32,
            brokers: Vec::new(),
            cluster_authorized_operations: i32::MIN,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl DescribeClusterResponseData {
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
    pub fn with_endpoint_type(mut self, value: i8) -> Self {
        self.endpoint_type = value;
        self
    }
    pub fn with_cluster_id(mut self, value: KafkaString) -> Self {
        self.cluster_id = value;
        self
    }
    pub fn with_controller_id(mut self, value: i32) -> Self {
        self.controller_id = value;
        self
    }
    pub fn with_brokers(mut self, value: Vec<DescribeClusterBroker>) -> Self {
        self.brokers = value;
        self
    }
    pub fn with_cluster_authorized_operations(mut self, value: i32) -> Self {
        self.cluster_authorized_operations = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 0 || version > 2 {
            return Err(UnsupportedVersion::new(60, version).into());
        }
        let throttle_time_ms;
        let error_code;
        let error_message;
        let mut endpoint_type = 1i8;
        let cluster_id;
        let controller_id;
        let brokers;
        let cluster_authorized_operations;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        throttle_time_ms = read_i32(buf)?;
        error_code = read_i16(buf)?;
        error_message = read_compact_nullable_string(buf)?;
        if version >= 1 {
            endpoint_type = read_i8(buf)?;
        }
        cluster_id = read_compact_string(buf)?;
        controller_id = read_i32(buf)?;
        brokers = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
            for _ in 0..len {
                arr.push(DescribeClusterBroker::read(buf, version)?);
            }
            arr
        };
        cluster_authorized_operations = read_i32(buf)?;
        let tagged_fields = read_tagged_fields(buf)?;
        for field in &tagged_fields {
            match field.tag {
                _ => {
                    _unknown_tagged_fields.push(field.clone());
                },
            }
        }
        Ok(Self {
            throttle_time_ms,
            error_code,
            error_message,
            endpoint_type,
            cluster_id,
            controller_id,
            brokers,
            cluster_authorized_operations,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 0 || version > 2 {
            return Err(UnsupportedVersion::new(60, version).into());
        }
        write_i32(buf, self.throttle_time_ms);
        write_i16(buf, self.error_code);
        write_compact_nullable_string(buf, self.error_message.as_ref())?;
        if version >= 1 {
            write_i8(buf, self.endpoint_type);
        } else if self.endpoint_type != 1i8 {
            return Err(UnsupportedFieldVersion::new(60, "endpoint_type", version).into());
        }
        write_compact_string(buf, &self.cluster_id)?;
        write_i32(buf, self.controller_id);
        write_compact_array_length(buf, self.brokers.len() as i32);
        for el in &self.brokers {
            el.write(buf, version)?;
        }
        write_i32(buf, self.cluster_authorized_operations);
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        if version < 0 || version > 2 {
            return Err(UnsupportedVersion::new(60, version).into());
        }
        let mut len: usize = 0;
        len += 4;
        len += 2;
        len += compact_nullable_string_len(self.error_message.as_ref())?;
        if version >= 1 {
            len += 1;
        } else if self.endpoint_type != 1i8 {
            return Err(UnsupportedFieldVersion::new(60, "endpoint_type", version).into());
        }
        len += compact_string_len(&self.cluster_id)?;
        len += 4;
        len += compact_array_length_len(self.brokers.len() as i32);
        for el in &self.brokers {
            len += el.encoded_len(version)?;
        }
        len += 4;
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct DescribeClusterBroker {
    /// The broker ID.
    pub broker_id: i32,
    /// The broker hostname.
    pub host: KafkaString,
    /// The broker port.
    pub port: i32,
    /// The rack of the broker, or null if it has not been assigned to a rack.
    pub rack: Option<KafkaString>,
    /// Whether the broker is fenced
    pub is_fenced: bool,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for DescribeClusterBroker {
    fn default() -> Self {
        Self {
            broker_id: 0_i32,
            host: KafkaString::default(),
            port: 0_i32,
            rack: None,
            is_fenced: false,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl DescribeClusterBroker {
    pub fn with_broker_id(mut self, value: i32) -> Self {
        self.broker_id = value;
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
    pub fn with_rack(mut self, value: Option<KafkaString>) -> Self {
        self.rack = value;
        self
    }
    pub fn with_is_fenced(mut self, value: bool) -> Self {
        self.is_fenced = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let broker_id;
        let host;
        let port;
        let rack;
        let mut is_fenced = false;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        broker_id = read_i32(buf)?;
        host = read_compact_string(buf)?;
        port = read_i32(buf)?;
        rack = read_compact_nullable_string(buf)?;
        if version >= 2 {
            is_fenced = read_bool(buf)?;
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
            broker_id,
            host,
            port,
            rack,
            is_fenced,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        write_i32(buf, self.broker_id);
        write_compact_string(buf, &self.host)?;
        write_i32(buf, self.port);
        write_compact_nullable_string(buf, self.rack.as_ref())?;
        if version >= 2 {
            write_bool(buf, self.is_fenced);
        } else if self.is_fenced != false {
            return Err(UnsupportedFieldVersion::new(60, "is_fenced", version).into());
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        let mut len: usize = 0;
        len += 4;
        len += compact_string_len(&self.host)?;
        len += 4;
        len += compact_nullable_string_len(self.rack.as_ref())?;
        if version >= 2 {
            len += 1;
        } else if self.is_fenced != false {
            return Err(UnsupportedFieldVersion::new(60, "is_fenced", version).into());
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
