//! Generated from DescribeAclsResponse.json - DO NOT EDIT
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
pub struct DescribeAclsResponseData {
    /// The duration in milliseconds for which the request was throttled due to a quota violation,
    /// or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// The error code, or 0 if there was no error.
    pub error_code: i16,
    /// The error message, or null if there was no error.
    pub error_message: Option<KafkaString>,
    /// Each Resource that is referenced in an ACL.
    pub resources: Vec<DescribeAclsResource>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for DescribeAclsResponseData {
    fn default() -> Self {
        Self {
            throttle_time_ms: 0_i32,
            error_code: 0_i16,
            error_message: None,
            resources: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl DescribeAclsResponseData {
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
    pub fn with_resources(mut self, value: Vec<DescribeAclsResource>) -> Self {
        self.resources = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 1 || version > 3 {
            return Err(UnsupportedVersion::new(29, version).into());
        }
        let throttle_time_ms;
        let error_code;
        let error_message;
        let resources;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        throttle_time_ms = read_i32(buf)?;
        error_code = read_i16(buf)?;
        if version >= 2 {
            error_message = read_compact_nullable_string(buf)?;
        } else {
            error_message = read_nullable_string(buf)?;
        }
        if version >= 2 {
            resources = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
                for _ in 0..len {
                    arr.push(DescribeAclsResource::read(buf, version)?);
                }
                arr
            };
        } else {
            resources = {
                let len = read_array_length(buf)?;
                let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
                for _ in 0..len {
                    arr.push(DescribeAclsResource::read(buf, version)?);
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
            throttle_time_ms,
            error_code,
            error_message,
            resources,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 1 || version > 3 {
            return Err(UnsupportedVersion::new(29, version).into());
        }
        write_i32(buf, self.throttle_time_ms);
        write_i16(buf, self.error_code);
        if version >= 2 {
            write_compact_nullable_string(buf, self.error_message.as_ref())?;
        } else {
            write_nullable_string(buf, self.error_message.as_ref())?;
        }
        if version >= 2 {
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
        if version >= 2 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        if version < 1 || version > 3 {
            return Err(UnsupportedVersion::new(29, version).into());
        }
        let mut len: usize = 0;
        len += 4;
        len += 2;
        if version >= 2 {
            len += compact_nullable_string_len(self.error_message.as_ref())?;
        } else {
            len += nullable_string_len(self.error_message.as_ref())?;
        }
        if version >= 2 {
            len += compact_array_length_len(self.resources.len() as i32);
            for el in &self.resources {
                len += el.encoded_len(version)?;
            }
        } else {
            len += array_length_len();
            for el in &self.resources {
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
pub struct DescribeAclsResource {
    /// The resource type.
    pub resource_type: i8,
    /// The resource name.
    pub resource_name: KafkaString,
    /// The resource pattern type.
    pub pattern_type: i8,
    /// The ACLs.
    pub acls: Vec<AclDescription>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for DescribeAclsResource {
    fn default() -> Self {
        Self {
            resource_type: 0_i8,
            resource_name: KafkaString::default(),
            pattern_type: 3i8,
            acls: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl DescribeAclsResource {
    pub fn with_resource_type(mut self, value: i8) -> Self {
        self.resource_type = value;
        self
    }
    pub fn with_resource_name(mut self, value: KafkaString) -> Self {
        self.resource_name = value;
        self
    }
    pub fn with_pattern_type(mut self, value: i8) -> Self {
        self.pattern_type = value;
        self
    }
    pub fn with_acls(mut self, value: Vec<AclDescription>) -> Self {
        self.acls = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let resource_type;
        let resource_name;
        let pattern_type;
        let acls;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        resource_type = read_i8(buf)?;
        if version >= 2 {
            resource_name = read_compact_string(buf)?;
        } else {
            resource_name = read_string(buf)?;
        }
        pattern_type = read_i8(buf)?;
        if version >= 2 {
            acls = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
                for _ in 0..len {
                    arr.push(AclDescription::read(buf, version)?);
                }
                arr
            };
        } else {
            acls = {
                let len = read_array_length(buf)?;
                let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
                for _ in 0..len {
                    arr.push(AclDescription::read(buf, version)?);
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
            resource_type,
            resource_name,
            pattern_type,
            acls,
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
        write_i8(buf, self.pattern_type);
        if version >= 2 {
            write_compact_array_length(buf, self.acls.len() as i32);
            for el in &self.acls {
                el.write(buf, version)?;
            }
        } else {
            write_array_length(buf, self.acls.len() as i32);
            for el in &self.acls {
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
        let mut len: usize = 0;
        len += 1;
        if version >= 2 {
            len += compact_string_len(&self.resource_name)?;
        } else {
            len += string_len(&self.resource_name)?;
        }
        len += 1;
        if version >= 2 {
            len += compact_array_length_len(self.acls.len() as i32);
            for el in &self.acls {
                len += el.encoded_len(version)?;
            }
        } else {
            len += array_length_len();
            for el in &self.acls {
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
pub struct AclDescription {
    /// The ACL principal.
    pub principal: KafkaString,
    /// The ACL host.
    pub host: KafkaString,
    /// The ACL operation.
    pub operation: i8,
    /// The ACL permission type.
    pub permission_type: i8,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for AclDescription {
    fn default() -> Self {
        Self {
            principal: KafkaString::default(),
            host: KafkaString::default(),
            operation: 0_i8,
            permission_type: 0_i8,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl AclDescription {
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
        let principal;
        let host;
        let operation;
        let permission_type;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
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
            principal,
            host,
            operation,
            permission_type,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
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
