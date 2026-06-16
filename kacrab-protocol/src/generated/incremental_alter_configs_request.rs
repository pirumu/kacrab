//! Generated from IncrementalAlterConfigsRequest.json - DO NOT EDIT
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
pub struct IncrementalAlterConfigsRequestData {
    /// The incremental updates for each resource.
    pub resources: Vec<AlterConfigsResource>,
    /// True if we should validate the request, but not change the configurations.
    pub validate_only: bool,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for IncrementalAlterConfigsRequestData {
    fn default() -> Self {
        Self {
            resources: Vec::new(),
            validate_only: false,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl IncrementalAlterConfigsRequestData {
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 0 || version > 1 {
            return Err(UnsupportedVersion::new(44, version).into());
        }
        let resources;
        let validate_only;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 1 {
            resources = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(AlterConfigsResource::read(buf, version)?);
                }
                arr
            };
        } else {
            resources = {
                let len = read_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(AlterConfigsResource::read(buf, version)?);
                }
                arr
            };
        }
        validate_only = read_bool(buf)?;
        if version >= 1 {
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
            resources,
            validate_only,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 0 || version > 1 {
            return Err(UnsupportedVersion::new(44, version).into());
        }
        if version >= 1 {
            write_compact_array_length(buf, self.resources.len() as i32);
            for el in &self.resources {
                el.write(buf, version)?;
            }
        } else {
            write_array_length(buf, self.resources.len() as i32);
            for el in &self.resources {
                el.write(buf, version)?;
            }
        }
        write_bool(buf, self.validate_only);
        if version >= 1 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct AlterConfigsResource {
    /// The resource type.
    pub resource_type: i8,
    /// The resource name.
    pub resource_name: KafkaString,
    /// The configurations.
    pub configs: Vec<AlterableConfig>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for AlterConfigsResource {
    fn default() -> Self {
        Self {
            resource_type: 0_i8,
            resource_name: KafkaString::default(),
            configs: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl AlterConfigsResource {
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let resource_type;
        let resource_name;
        let configs;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        resource_type = read_i8(buf)?;
        if version >= 1 {
            resource_name = read_compact_string(buf)?;
        } else {
            resource_name = read_string(buf)?;
        }
        if version >= 1 {
            configs = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(AlterableConfig::read(buf, version)?);
                }
                arr
            };
        } else {
            configs = {
                let len = read_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(AlterableConfig::read(buf, version)?);
                }
                arr
            };
        }
        if version >= 1 {
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
            resource_type,
            resource_name,
            configs,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        write_i8(buf, self.resource_type);
        if version >= 1 {
            write_compact_string(buf, &self.resource_name)?;
        } else {
            write_string(buf, &self.resource_name)?;
        }
        if version >= 1 {
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
        if version >= 1 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct AlterableConfig {
    /// The configuration key name.
    pub name: KafkaString,
    /// The type (Set, Delete, Append, Subtract) of operation.
    pub config_operation: i8,
    /// The value to set for the configuration key.
    pub value: Option<KafkaString>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for AlterableConfig {
    fn default() -> Self {
        Self {
            name: KafkaString::default(),
            config_operation: 0_i8,
            value: None,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl AlterableConfig {
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let name;
        let config_operation;
        let value;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 1 {
            name = read_compact_string(buf)?;
        } else {
            name = read_string(buf)?;
        }
        config_operation = read_i8(buf)?;
        if version >= 1 {
            value = read_compact_nullable_string(buf)?;
        } else {
            value = read_nullable_string(buf)?;
        }
        if version >= 1 {
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
            config_operation,
            value,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version >= 1 {
            write_compact_string(buf, &self.name)?;
        } else {
            write_string(buf, &self.name)?;
        }
        write_i8(buf, self.config_operation);
        if version >= 1 {
            write_compact_nullable_string(buf, self.value.as_ref())?;
        } else {
            write_nullable_string(buf, self.value.as_ref())?;
        }
        if version >= 1 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
}
