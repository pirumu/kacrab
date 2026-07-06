//! Generated from CreateAclsRequest.json - DO NOT EDIT
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
    pub fn with_creations(mut self, value: Vec<AclCreation>) -> Self {
        self.creations = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 1 || version > 3 {
            return Err(UnsupportedVersion::new(30, version).into());
        }
        let creations;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 2 {
            creations = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
                for _ in 0..len {
                    arr.push(AclCreation::read(buf, version)?);
                }
                arr
            };
        } else {
            creations = {
                let len = read_array_length(buf)?;
                let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
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
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        if version < 1 || version > 3 {
            return Err(UnsupportedVersion::new(30, version).into());
        }
        let mut len: usize = 0;
        if version >= 2 {
            len += compact_array_length_len(self.creations.len() as i32);
            for el in &self.creations {
                len += el.encoded_len(version)?;
            }
        } else {
            len += array_length_len();
            for el in &self.creations {
                len += el.encoded_len(version)?;
            }
        }
        if version >= 2 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            len += tagged_fields_len(&all_tags)?;
        }
        Ok(len)
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
    pub fn with_resource_type(mut self, value: i8) -> Self {
        self.resource_type = value;
        self
    }
    pub fn with_resource_name(mut self, value: KafkaString) -> Self {
        self.resource_name = value;
        self
    }
    pub fn with_resource_pattern_type(mut self, value: i8) -> Self {
        self.resource_pattern_type = value;
        self
    }
    pub fn with_principal(mut self, value: KafkaString) -> Self {
        self.principal = value;
        self
    }
    pub fn with_host(mut self, value: KafkaString) -> Self {
        self.host = value;
        self
    }
    pub fn with_operation(mut self, value: i8) -> Self {
        self.operation = value;
        self
    }
    pub fn with_permission_type(mut self, value: i8) -> Self {
        self.permission_type = value;
        self
    }
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
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        let mut len: usize = 0;
        len += 1;
        if version >= 2 {
            len += compact_string_len(&self.resource_name)?;
        } else {
            len += string_len(&self.resource_name)?;
        }
        len += 1;
        if version >= 2 {
            len += compact_string_len(&self.principal)?;
        } else {
            len += string_len(&self.principal)?;
        }
        if version >= 2 {
            len += compact_string_len(&self.host)?;
        } else {
            len += string_len(&self.host)?;
        }
        len += 1;
        len += 1;
        if version >= 2 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            len += tagged_fields_len(&all_tags)?;
        }
        Ok(len)
    }
}
