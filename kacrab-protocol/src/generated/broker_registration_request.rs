//! Generated from BrokerRegistrationRequest.json - DO NOT EDIT
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
pub struct BrokerRegistrationRequestData {
    /// The broker ID.
    pub broker_id: i32,
    /// The cluster id of the broker process.
    pub cluster_id: KafkaString,
    /// The incarnation id of the broker process.
    pub incarnation_id: KafkaUuid,
    /// The listeners of this broker.
    pub listeners: Vec<Listener>,
    /// The features on this broker. Note: in v0-v3, features with MinSupportedVersion = 0 are
    /// omitted.
    pub features: Vec<Feature>,
    /// The rack which this broker is in.
    pub rack: Option<KafkaString>,
    /// If the required configurations for ZK migration are present, this value is set to true.
    pub is_migrating_zk_broker: bool,
    /// Log directories configured in this broker which are available.
    pub log_dirs: Vec<KafkaUuid>,
    /// The epoch before a clean shutdown.
    pub previous_broker_epoch: i64,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for BrokerRegistrationRequestData {
    fn default() -> Self {
        Self {
            broker_id: 0_i32,
            cluster_id: KafkaString::default(),
            incarnation_id: KafkaUuid::ZERO,
            listeners: Vec::new(),
            features: Vec::new(),
            rack: None,
            is_migrating_zk_broker: false,
            log_dirs: Vec::new(),
            previous_broker_epoch: -1i64,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl BrokerRegistrationRequestData {
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 0 || version > 4 {
            return Err(UnsupportedVersion::new(62, version).into());
        }
        let broker_id;
        let cluster_id;
        let incarnation_id;
        let listeners;
        let features;
        let rack;
        let mut is_migrating_zk_broker = false;
        let mut log_dirs = Vec::new();
        let mut previous_broker_epoch = -1i64;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        broker_id = read_i32(buf)?;
        cluster_id = read_compact_string(buf)?;
        incarnation_id = read_uuid(buf)?;
        listeners = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(len.max(0) as usize);
            for _ in 0..len {
                arr.push(Listener::read(buf, version)?);
            }
            arr
        };
        features = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(len.max(0) as usize);
            for _ in 0..len {
                arr.push(Feature::read(buf, version)?);
            }
            arr
        };
        rack = read_compact_nullable_string(buf)?;
        if version >= 1 {
            is_migrating_zk_broker = read_bool(buf)?;
        }
        if version >= 2 {
            log_dirs = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(read_uuid(buf)?);
                }
                arr
            };
        }
        if version >= 3 {
            previous_broker_epoch = read_i64(buf)?;
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
            cluster_id,
            incarnation_id,
            listeners,
            features,
            rack,
            is_migrating_zk_broker,
            log_dirs,
            previous_broker_epoch,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 0 || version > 4 {
            return Err(UnsupportedVersion::new(62, version).into());
        }
        write_i32(buf, self.broker_id);
        write_compact_string(buf, &self.cluster_id)?;
        write_uuid(buf, &self.incarnation_id);
        write_compact_array_length(buf, self.listeners.len() as i32);
        for el in &self.listeners {
            el.write(buf, version)?;
        }
        write_compact_array_length(buf, self.features.len() as i32);
        for el in &self.features {
            el.write(buf, version)?;
        }
        write_compact_nullable_string(buf, self.rack.as_ref())?;
        if version >= 1 {
            write_bool(buf, self.is_migrating_zk_broker);
        }
        if version >= 2 {
            write_compact_array_length(buf, self.log_dirs.len() as i32);
            for el in &self.log_dirs {
                write_uuid(buf, el);
            }
        }
        if version >= 3 {
            write_i64(buf, self.previous_broker_epoch);
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
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
    /// The security protocol.
    pub security_protocol: i16,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for Listener {
    fn default() -> Self {
        Self {
            name: KafkaString::default(),
            host: KafkaString::default(),
            port: 0_u16,
            security_protocol: 0_i16,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl Listener {
    pub fn read(buf: &mut Bytes, _version: i16) -> Result<Self> {
        let name;
        let host;
        let port;
        let security_protocol;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        name = read_compact_string(buf)?;
        host = read_compact_string(buf)?;
        port = read_u16(buf)?;
        security_protocol = read_i16(buf)?;
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
            security_protocol,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, _version: i16) -> Result<()> {
        write_compact_string(buf, &self.name)?;
        write_compact_string(buf, &self.host)?;
        write_u16(buf, self.port);
        write_i16(buf, self.security_protocol);
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct Feature {
    /// The feature name.
    pub name: KafkaString,
    /// The minimum supported feature level.
    pub min_supported_version: i16,
    /// The maximum supported feature level.
    pub max_supported_version: i16,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for Feature {
    fn default() -> Self {
        Self {
            name: KafkaString::default(),
            min_supported_version: 0_i16,
            max_supported_version: 0_i16,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl Feature {
    pub fn read(buf: &mut Bytes, _version: i16) -> Result<Self> {
        let name;
        let min_supported_version;
        let max_supported_version;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        name = read_compact_string(buf)?;
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
            name,
            min_supported_version,
            max_supported_version,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, _version: i16) -> Result<()> {
        write_compact_string(buf, &self.name)?;
        write_i16(buf, self.min_supported_version);
        write_i16(buf, self.max_supported_version);
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
}
