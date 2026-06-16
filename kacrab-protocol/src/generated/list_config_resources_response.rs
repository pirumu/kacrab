//! Generated from ListConfigResourcesResponse.json - DO NOT EDIT
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
pub struct ListConfigResourcesResponseData {
    /// The duration in milliseconds for which the request was throttled due to a quota violation,
    /// or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// The error code, or 0 if there was no error.
    pub error_code: i16,
    /// Each config resource in the response.
    pub config_resources: Vec<ConfigResource>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for ListConfigResourcesResponseData {
    fn default() -> Self {
        Self {
            throttle_time_ms: 0_i32,
            error_code: 0_i16,
            config_resources: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl ListConfigResourcesResponseData {
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 0 || version > 1 {
            return Err(UnsupportedVersion::new(74, version).into());
        }
        let throttle_time_ms;
        let error_code;
        let config_resources;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        throttle_time_ms = read_i32(buf)?;
        error_code = read_i16(buf)?;
        config_resources = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(len.max(0) as usize);
            for _ in 0..len {
                arr.push(ConfigResource::read(buf, version)?);
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
            throttle_time_ms,
            error_code,
            config_resources,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 0 || version > 1 {
            return Err(UnsupportedVersion::new(74, version).into());
        }
        write_i32(buf, self.throttle_time_ms);
        write_i16(buf, self.error_code);
        write_compact_array_length(buf, self.config_resources.len() as i32);
        for el in &self.config_resources {
            el.write(buf, version)?;
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct ConfigResource {
    /// The resource name.
    pub resource_name: KafkaString,
    /// The resource type.
    pub resource_type: i8,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for ConfigResource {
    fn default() -> Self {
        Self {
            resource_name: KafkaString::default(),
            resource_type: 16i8,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl ConfigResource {
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let resource_name;
        let mut resource_type = 16i8;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        resource_name = read_compact_string(buf)?;
        if version >= 1 {
            resource_type = read_i8(buf)?;
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
            resource_name,
            resource_type,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        write_compact_string(buf, &self.resource_name)?;
        if version >= 1 {
            write_i8(buf, self.resource_type);
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
}
