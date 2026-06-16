//! Generated from DescribeConfigsRequest.json - DO NOT EDIT
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
pub struct DescribeConfigsRequestData {
    /// The resources whose configurations we want to describe.
    pub resources: Vec<DescribeConfigsResource>,
    /// True if we should include all synonyms.
    pub include_synonyms: bool,
    /// True if we should include configuration documentation.
    pub include_documentation: bool,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for DescribeConfigsRequestData {
    fn default() -> Self {
        Self {
            resources: Vec::new(),
            include_synonyms: false,
            include_documentation: false,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl DescribeConfigsRequestData {
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 1 || version > 4 {
            return Err(UnsupportedVersion::new(32, version).into());
        }
        let resources;
        let include_synonyms;
        let mut include_documentation = false;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 4 {
            resources = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(DescribeConfigsResource::read(buf, version)?);
                }
                arr
            };
        } else {
            resources = {
                let len = read_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(DescribeConfigsResource::read(buf, version)?);
                }
                arr
            };
        }
        include_synonyms = read_bool(buf)?;
        if version >= 3 {
            include_documentation = read_bool(buf)?;
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
            resources,
            include_synonyms,
            include_documentation,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 1 || version > 4 {
            return Err(UnsupportedVersion::new(32, version).into());
        }
        if version >= 4 {
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
        write_bool(buf, self.include_synonyms);
        if version >= 3 {
            write_bool(buf, self.include_documentation);
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
pub struct DescribeConfigsResource {
    /// The resource type.
    pub resource_type: i8,
    /// The resource name.
    pub resource_name: KafkaString,
    /// The configuration keys to list, or null to list all configuration keys.
    pub configuration_keys: Option<Vec<KafkaString>>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for DescribeConfigsResource {
    fn default() -> Self {
        Self {
            resource_type: 0_i8,
            resource_name: KafkaString::default(),
            configuration_keys: None,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl DescribeConfigsResource {
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let resource_type;
        let resource_name;
        let configuration_keys;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        resource_type = read_i8(buf)?;
        if version >= 4 {
            resource_name = read_compact_string(buf)?;
        } else {
            resource_name = read_string(buf)?;
        }
        if version >= 4 {
            configuration_keys = {
                let len = read_compact_array_length(buf)?;
                if len < 0 {
                    None
                } else {
                    let mut arr = Vec::with_capacity(len as usize);
                    for _ in 0..len {
                        arr.push(read_compact_string(buf)?);
                    }
                    Some(arr)
                }
            };
        } else {
            configuration_keys = {
                let len = read_array_length(buf)?;
                if len < 0 {
                    None
                } else {
                    let mut arr = Vec::with_capacity(len as usize);
                    for _ in 0..len {
                        arr.push(read_string(buf)?);
                    }
                    Some(arr)
                }
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
            resource_type,
            resource_name,
            configuration_keys,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        write_i8(buf, self.resource_type);
        if version >= 4 {
            write_compact_string(buf, &self.resource_name)?;
        } else {
            write_string(buf, &self.resource_name)?;
        }
        if version >= 4 {
            match &self.configuration_keys {
                None => {
                    write_compact_array_length(buf, -1);
                },
                Some(arr) => {
                    write_compact_array_length(buf, arr.len() as i32);
                    for el in arr {
                        write_compact_string(buf, el)?;
                    }
                },
            }
        } else {
            match &self.configuration_keys {
                None => {
                    write_array_length(buf, -1);
                },
                Some(arr) => {
                    write_array_length(buf, arr.len() as i32);
                    for el in arr {
                        write_string(buf, el)?;
                    }
                },
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
