//! Generated from CreateAclsRequest.json - DO NOT EDIT
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
pub struct CreateAclsRequestData {
    /// The ACLs that we want to create.
    pub creations: Vec<AclCreation>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for CreateAclsRequestData {
    fn default() -> Self {
        Self {
            creations: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl CreateAclsRequestData {
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 1 || version > 3 {
            return Err(UnsupportedVersion::new(30, version).into());
        }
        let creations;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 2 {
            creations = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(AclCreation::read(buf, version)?);
                }
                arr
            };
        } else {
            creations = {
                let len = read_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(AclCreation::read(buf, version)?);
                }
                arr
            };
        }
        if version >= 2 {
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
            creations,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 1 || version > 3 {
            return Err(UnsupportedVersion::new(30, version).into());
        }
        if version >= 2 {
            write_compact_array_length(buf, self.creations.len() as i32);
            for el in &self.creations {
                el.write(buf, version)?;
            }
        } else {
            write_array_length(buf, self.creations.len() as i32);
            for el in &self.creations {
                el.write(buf, version)?;
            }
        }
        if version >= 2 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct AclCreation {
    /// The type of the resource.
    pub resource_type: i8,
    /// The resource name for the ACL.
    pub resource_name: KafkaString,
    /// The pattern type for the ACL.
    pub resource_pattern_type: i8,
    /// The principal for the ACL.
    pub principal: KafkaString,
    /// The host for the ACL.
    pub host: KafkaString,
    /// The operation type for the ACL (read, write, etc.).
    pub operation: i8,
    /// The permission type for the ACL (allow, deny, etc.).
    pub permission_type: i8,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for AclCreation {
    fn default() -> Self {
        Self {
            resource_type: 0_i8,
            resource_name: KafkaString::default(),
            resource_pattern_type: 3i8,
            principal: KafkaString::default(),
            host: KafkaString::default(),
            operation: 0_i8,
            permission_type: 0_i8,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl AclCreation {
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let resource_type;
        let resource_name;
        let resource_pattern_type;
        let principal;
        let host;
        let operation;
        let permission_type;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        resource_type = read_i8(buf)?;
        if version >= 2 {
            resource_name = read_compact_string(buf)?;
        } else {
            resource_name = read_string(buf)?;
        }
        resource_pattern_type = read_i8(buf)?;
        if version >= 2 {
            principal = read_compact_string(buf)?;
        } else {
            principal = read_string(buf)?;
        }
        if version >= 2 {
            host = read_compact_string(buf)?;
        } else {
            host = read_string(buf)?;
        }
        operation = read_i8(buf)?;
        permission_type = read_i8(buf)?;
        if version >= 2 {
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
            resource_pattern_type,
            principal,
            host,
            operation,
            permission_type,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        write_i8(buf, self.resource_type);
        if version >= 2 {
            write_compact_string(buf, &self.resource_name)?;
        } else {
            write_string(buf, &self.resource_name)?;
        }
        write_i8(buf, self.resource_pattern_type);
        if version >= 2 {
            write_compact_string(buf, &self.principal)?;
        } else {
            write_string(buf, &self.principal)?;
        }
        if version >= 2 {
            write_compact_string(buf, &self.host)?;
        } else {
            write_string(buf, &self.host)?;
        }
        write_i8(buf, self.operation);
        write_i8(buf, self.permission_type);
        if version >= 2 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
}
