//! Generated from DescribeConfigsResponse.json - DO NOT EDIT
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
    pub fn with_throttle_time_ms(mut self, value: i32) -> Self {
        self.throttle_time_ms = value;
        self
    }
    pub fn with_results(mut self, value: Vec<DescribeConfigsResult>) -> Self {
        self.results = value;
        self
    }
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
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        if version < 1 || version > 4 {
            return Err(UnsupportedVersion::new(32, version).into());
        }
        let mut len: usize = 0;
        len += 4;
        if version >= 4 {
            len += compact_array_length_len(self.results.len() as i32);
            for el in &self.results {
                len += el.encoded_len(version)?;
            }
        } else {
            len += array_length_len();
            for el in &self.results {
                len += el.encoded_len(version)?;
            }
        }
        if version >= 4 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            len += tagged_fields_len(&all_tags)?;
        }
        Ok(len)
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
    pub fn with_error_code(mut self, value: i16) -> Self {
        self.error_code = value;
        self
    }
    pub fn with_error_message(mut self, value: Option<KafkaString>) -> Self {
        self.error_message = value;
        self
    }
    pub fn with_resource_type(mut self, value: i8) -> Self {
        self.resource_type = value;
        self
    }
    pub fn with_resource_name(mut self, value: KafkaString) -> Self {
        self.resource_name = value;
        self
    }
    pub fn with_configs(mut self, value: Vec<DescribeConfigsResourceResult>) -> Self {
        self.configs = value;
        self
    }
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
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        let mut len: usize = 0;
        len += 2;
        if version >= 4 {
            len += compact_nullable_string_len(self.error_message.as_ref())?;
        } else {
            len += nullable_string_len(self.error_message.as_ref())?;
        }
        len += 1;
        if version >= 4 {
            len += compact_string_len(&self.resource_name)?;
        } else {
            len += string_len(&self.resource_name)?;
        }
        if version >= 4 {
            len += compact_array_length_len(self.configs.len() as i32);
            for el in &self.configs {
                len += el.encoded_len(version)?;
            }
        } else {
            len += array_length_len();
            for el in &self.configs {
                len += el.encoded_len(version)?;
            }
        }
        if version >= 4 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            len += tagged_fields_len(&all_tags)?;
        }
        Ok(len)
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
    pub fn with_name(mut self, value: KafkaString) -> Self {
        self.name = value;
        self
    }
    pub fn with_value(mut self, value: Option<KafkaString>) -> Self {
        self.value = value;
        self
    }
    pub fn with_read_only(mut self, value: bool) -> Self {
        self.read_only = value;
        self
    }
    pub fn with_config_source(mut self, value: i8) -> Self {
        self.config_source = value;
        self
    }
    pub fn with_is_sensitive(mut self, value: bool) -> Self {
        self.is_sensitive = value;
        self
    }
    pub fn with_synonyms(mut self, value: Vec<DescribeConfigsSynonym>) -> Self {
        self.synonyms = value;
        self
    }
    pub fn with_config_type(mut self, value: i8) -> Self {
        self.config_type = value;
        self
    }
    pub fn with_documentation(mut self, value: Option<KafkaString>) -> Self {
        self.documentation = value;
        self
    }
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
        } else if self.config_type != 0i8 {
            return Err(UnsupportedFieldVersion::new(32, "config_type", version).into());
        }
        if version >= 3 {
            if version >= 4 {
                write_compact_nullable_string(buf, self.documentation.as_ref())?;
            } else {
                write_nullable_string(buf, self.documentation.as_ref())?;
            }
        } else if self.documentation != None {
            return Err(UnsupportedFieldVersion::new(32, "documentation", version).into());
        }
        if version >= 4 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        let mut len: usize = 0;
        if version >= 4 {
            len += compact_string_len(&self.name)?;
        } else {
            len += string_len(&self.name)?;
        }
        if version >= 4 {
            len += compact_nullable_string_len(self.value.as_ref())?;
        } else {
            len += nullable_string_len(self.value.as_ref())?;
        }
        len += 1;
        len += 1;
        len += 1;
        if version >= 4 {
            len += compact_array_length_len(self.synonyms.len() as i32);
            for el in &self.synonyms {
                len += el.encoded_len(version)?;
            }
        } else {
            len += array_length_len();
            for el in &self.synonyms {
                len += el.encoded_len(version)?;
            }
        }
        if version >= 3 {
            len += 1;
        } else if self.config_type != 0i8 {
            return Err(UnsupportedFieldVersion::new(32, "config_type", version).into());
        }
        if version >= 3 {
            if version >= 4 {
                len += compact_nullable_string_len(self.documentation.as_ref())?;
            } else {
                len += nullable_string_len(self.documentation.as_ref())?;
            }
        } else if self.documentation != None {
            return Err(UnsupportedFieldVersion::new(32, "documentation", version).into());
        }
        if version >= 4 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            len += tagged_fields_len(&all_tags)?;
        }
        Ok(len)
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
    pub fn with_name(mut self, value: KafkaString) -> Self {
        self.name = value;
        self
    }
    pub fn with_value(mut self, value: Option<KafkaString>) -> Self {
        self.value = value;
        self
    }
    pub fn with_source(mut self, value: i8) -> Self {
        self.source = value;
        self
    }
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
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        let mut len: usize = 0;
        if version >= 4 {
            len += compact_string_len(&self.name)?;
        } else {
            len += string_len(&self.name)?;
        }
        if version >= 4 {
            len += compact_nullable_string_len(self.value.as_ref())?;
        } else {
            len += nullable_string_len(self.value.as_ref())?;
        }
        len += 1;
        if version >= 4 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            len += tagged_fields_len(&all_tags)?;
        }
        Ok(len)
    }
}
