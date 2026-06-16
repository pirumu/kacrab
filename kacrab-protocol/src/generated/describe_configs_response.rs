//! Generated from DescribeConfigsResponse.json - DO NOT EDIT
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
pub struct DescribeConfigsResponseData {
    /// The duration in milliseconds for which the request was throttled due to a quota violation,
    /// or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// The results for each resource.
    pub results: Vec<DescribeConfigsResult>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for DescribeConfigsResponseData {
    fn default() -> Self {
        Self {
            throttle_time_ms: 0_i32,
            results: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl DescribeConfigsResponseData {
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 1 || version > 4 {
            return Err(UnsupportedVersion::new(32, version).into());
        }
        let throttle_time_ms;
        let results;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        throttle_time_ms = read_i32(buf)?;
        if version >= 4 {
            results = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(DescribeConfigsResult::read(buf, version)?);
                }
                arr
            };
        } else {
            results = {
                let len = read_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(DescribeConfigsResult::read(buf, version)?);
                }
                arr
            };
        }
        if version >= 4 {
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
            results,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 1 || version > 4 {
            return Err(UnsupportedVersion::new(32, version).into());
        }
        write_i32(buf, self.throttle_time_ms);
        if version >= 4 {
            write_compact_array_length(buf, self.results.len() as i32);
            for el in &self.results {
                el.write(buf, version)?;
            }
        } else {
            write_array_length(buf, self.results.len() as i32);
            for el in &self.results {
                el.write(buf, version)?;
            }
        }
        if version >= 4 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct DescribeConfigsResult {
    /// The error code, or 0 if we were able to successfully describe the configurations.
    pub error_code: i16,
    /// The error message, or null if we were able to successfully describe the configurations.
    pub error_message: Option<KafkaString>,
    /// The resource type.
    pub resource_type: i8,
    /// The resource name.
    pub resource_name: KafkaString,
    /// Each listed configuration.
    pub configs: Vec<DescribeConfigsResourceResult>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for DescribeConfigsResult {
    fn default() -> Self {
        Self {
            error_code: 0_i16,
            error_message: None,
            resource_type: 0_i8,
            resource_name: KafkaString::default(),
            configs: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl DescribeConfigsResult {
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let error_code;
        let error_message;
        let resource_type;
        let resource_name;
        let configs;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        error_code = read_i16(buf)?;
        if version >= 4 {
            error_message = read_compact_nullable_string(buf)?;
        } else {
            error_message = read_nullable_string(buf)?;
        }
        resource_type = read_i8(buf)?;
        if version >= 4 {
            resource_name = read_compact_string(buf)?;
        } else {
            resource_name = read_string(buf)?;
        }
        if version >= 4 {
            configs = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(DescribeConfigsResourceResult::read(buf, version)?);
                }
                arr
            };
        } else {
            configs = {
                let len = read_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(DescribeConfigsResourceResult::read(buf, version)?);
                }
                arr
            };
        }
        if version >= 4 {
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
            error_code,
            error_message,
            resource_type,
            resource_name,
            configs,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        write_i16(buf, self.error_code);
        if version >= 4 {
            write_compact_nullable_string(buf, self.error_message.as_ref())?;
        } else {
            write_nullable_string(buf, self.error_message.as_ref())?;
        }
        write_i8(buf, self.resource_type);
        if version >= 4 {
            write_compact_string(buf, &self.resource_name)?;
        } else {
            write_string(buf, &self.resource_name)?;
        }
        if version >= 4 {
            write_compact_array_length(buf, self.configs.len() as i32);
            for el in &self.configs {
                el.write(buf, version)?;
            }
        } else {
            write_array_length(buf, self.configs.len() as i32);
            for el in &self.configs {
                el.write(buf, version)?;
            }
        }
        if version >= 4 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct DescribeConfigsResourceResult {
    /// The configuration name.
    pub name: KafkaString,
    /// The configuration value.
    pub value: Option<KafkaString>,
    /// True if the configuration is read-only.
    pub read_only: bool,
    /// The configuration source.
    pub config_source: i8,
    /// True if this configuration is sensitive.
    pub is_sensitive: bool,
    /// The synonyms for this configuration key.
    pub synonyms: Vec<DescribeConfigsSynonym>,
    /// The configuration data type. Type can be one of the following values - BOOLEAN, STRING,
    /// INT, SHORT, LONG, DOUBLE, LIST, CLASS, PASSWORD.
    pub config_type: i8,
    /// The configuration documentation.
    pub documentation: Option<KafkaString>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for DescribeConfigsResourceResult {
    fn default() -> Self {
        Self {
            name: KafkaString::default(),
            value: None,
            read_only: false,
            config_source: -1i8,
            is_sensitive: false,
            synonyms: Vec::new(),
            config_type: 0i8,
            documentation: None,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl DescribeConfigsResourceResult {
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let name;
        let value;
        let read_only;
        let config_source;
        let is_sensitive;
        let synonyms;
        let mut config_type = 0i8;
        let mut documentation = None;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 4 {
            name = read_compact_string(buf)?;
        } else {
            name = read_string(buf)?;
        }
        if version >= 4 {
            value = read_compact_nullable_string(buf)?;
        } else {
            value = read_nullable_string(buf)?;
        }
        read_only = read_bool(buf)?;
        config_source = read_i8(buf)?;
        is_sensitive = read_bool(buf)?;
        if version >= 4 {
            synonyms = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(DescribeConfigsSynonym::read(buf, version)?);
                }
                arr
            };
        } else {
            synonyms = {
                let len = read_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(DescribeConfigsSynonym::read(buf, version)?);
                }
                arr
            };
        }
        if version >= 3 {
            config_type = read_i8(buf)?;
        }
        if version >= 3 {
            if version >= 4 {
                documentation = read_compact_nullable_string(buf)?;
            } else {
                documentation = read_nullable_string(buf)?;
            }
        }
        if version >= 4 {
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
            value,
            read_only,
            config_source,
            is_sensitive,
            synonyms,
            config_type,
            documentation,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version >= 4 {
            write_compact_string(buf, &self.name)?;
        } else {
            write_string(buf, &self.name)?;
        }
        if version >= 4 {
            write_compact_nullable_string(buf, self.value.as_ref())?;
        } else {
            write_nullable_string(buf, self.value.as_ref())?;
        }
        write_bool(buf, self.read_only);
        write_i8(buf, self.config_source);
        write_bool(buf, self.is_sensitive);
        if version >= 4 {
            write_compact_array_length(buf, self.synonyms.len() as i32);
            for el in &self.synonyms {
                el.write(buf, version)?;
            }
        } else {
            write_array_length(buf, self.synonyms.len() as i32);
            for el in &self.synonyms {
                el.write(buf, version)?;
            }
        }
        if version >= 3 {
            write_i8(buf, self.config_type);
        }
        if version >= 3 {
            if version >= 4 {
                write_compact_nullable_string(buf, self.documentation.as_ref())?;
            } else {
                write_nullable_string(buf, self.documentation.as_ref())?;
            }
        }
        if version >= 4 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct DescribeConfigsSynonym {
    /// The synonym name.
    pub name: KafkaString,
    /// The synonym value.
    pub value: Option<KafkaString>,
    /// The synonym source.
    pub source: i8,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for DescribeConfigsSynonym {
    fn default() -> Self {
        Self {
            name: KafkaString::default(),
            value: None,
            source: 0_i8,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl DescribeConfigsSynonym {
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let name;
        let value;
        let source;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 4 {
            name = read_compact_string(buf)?;
        } else {
            name = read_string(buf)?;
        }
        if version >= 4 {
            value = read_compact_nullable_string(buf)?;
        } else {
            value = read_nullable_string(buf)?;
        }
        source = read_i8(buf)?;
        if version >= 4 {
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
            value,
            source,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version >= 4 {
            write_compact_string(buf, &self.name)?;
        } else {
            write_string(buf, &self.name)?;
        }
        if version >= 4 {
            write_compact_nullable_string(buf, self.value.as_ref())?;
        } else {
            write_nullable_string(buf, self.value.as_ref())?;
        }
        write_i8(buf, self.source);
        if version >= 4 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
}
