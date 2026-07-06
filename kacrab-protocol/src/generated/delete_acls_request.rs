//! Generated from DeleteAclsRequest.json - DO NOT EDIT
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
pub struct DeleteAclsRequestData {
    /// The filters to use when deleting ACLs.
    pub filters: Vec<DeleteAclsFilter>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for DeleteAclsRequestData {
    fn default() -> Self {
        Self {
            filters: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl DeleteAclsRequestData {
    pub fn with_filters(mut self, value: Vec<DeleteAclsFilter>) -> Self {
        self.filters = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 1 || version > 3 {
            return Err(UnsupportedVersion::new(31, version).into());
        }
        let filters;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 2 {
            filters = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
                for _ in 0..len {
                    arr.push(DeleteAclsFilter::read(buf, version)?);
                }
                arr
            };
        } else {
            filters = {
                let len = read_array_length(buf)?;
                let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
                for _ in 0..len {
                    arr.push(DeleteAclsFilter::read(buf, version)?);
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
            filters,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 1 || version > 3 {
            return Err(UnsupportedVersion::new(31, version).into());
        }
        if version >= 2 {
            write_compact_array_length(buf, self.filters.len() as i32);
            for el in &self.filters {
                el.write(buf, version)?;
            }
        } else {
            write_array_length(buf, self.filters.len() as i32);
            for el in &self.filters {
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
            return Err(UnsupportedVersion::new(31, version).into());
        }
        let mut len: usize = 0;
        if version >= 2 {
            len += compact_array_length_len(self.filters.len() as i32);
            for el in &self.filters {
                len += el.encoded_len(version)?;
            }
        } else {
            len += array_length_len();
            for el in &self.filters {
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
pub struct DeleteAclsFilter {
    /// The resource type.
    pub resource_type_filter: i8,
    /// The resource name, or null to match any resource name.
    pub resource_name_filter: Option<KafkaString>,
    /// The pattern type.
    pub pattern_type_filter: i8,
    /// The principal filter, or null to accept all principals.
    pub principal_filter: Option<KafkaString>,
    /// The host filter, or null to accept all hosts.
    pub host_filter: Option<KafkaString>,
    /// The ACL operation.
    pub operation: i8,
    /// The permission type.
    pub permission_type: i8,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for DeleteAclsFilter {
    fn default() -> Self {
        Self {
            resource_type_filter: 0_i8,
            resource_name_filter: None,
            pattern_type_filter: 3i8,
            principal_filter: None,
            host_filter: None,
            operation: 0_i8,
            permission_type: 0_i8,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl DeleteAclsFilter {
    pub fn with_resource_type_filter(mut self, value: i8) -> Self {
        self.resource_type_filter = value;
        self
    }
    pub fn with_resource_name_filter(mut self, value: Option<KafkaString>) -> Self {
        self.resource_name_filter = value;
        self
    }
    pub fn with_pattern_type_filter(mut self, value: i8) -> Self {
        self.pattern_type_filter = value;
        self
    }
    pub fn with_principal_filter(mut self, value: Option<KafkaString>) -> Self {
        self.principal_filter = value;
        self
    }
    pub fn with_host_filter(mut self, value: Option<KafkaString>) -> Self {
        self.host_filter = value;
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
        let resource_type_filter;
        let resource_name_filter;
        let pattern_type_filter;
        let principal_filter;
        let host_filter;
        let operation;
        let permission_type;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        resource_type_filter = read_i8(buf)?;
        if version >= 2 {
            resource_name_filter = read_compact_nullable_string(buf)?;
        } else {
            resource_name_filter = read_nullable_string(buf)?;
        }
        pattern_type_filter = read_i8(buf)?;
        if version >= 2 {
            principal_filter = read_compact_nullable_string(buf)?;
        } else {
            principal_filter = read_nullable_string(buf)?;
        }
        if version >= 2 {
            host_filter = read_compact_nullable_string(buf)?;
        } else {
            host_filter = read_nullable_string(buf)?;
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
            resource_type_filter,
            resource_name_filter,
            pattern_type_filter,
            principal_filter,
            host_filter,
            operation,
            permission_type,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        write_i8(buf, self.resource_type_filter);
        if version >= 2 {
            write_compact_nullable_string(buf, self.resource_name_filter.as_ref())?;
        } else {
            write_nullable_string(buf, self.resource_name_filter.as_ref())?;
        }
        write_i8(buf, self.pattern_type_filter);
        if version >= 2 {
            write_compact_nullable_string(buf, self.principal_filter.as_ref())?;
        } else {
            write_nullable_string(buf, self.principal_filter.as_ref())?;
        }
        if version >= 2 {
            write_compact_nullable_string(buf, self.host_filter.as_ref())?;
        } else {
            write_nullable_string(buf, self.host_filter.as_ref())?;
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
            len += compact_nullable_string_len(self.resource_name_filter.as_ref())?;
        } else {
            len += nullable_string_len(self.resource_name_filter.as_ref())?;
        }
        len += 1;
        if version >= 2 {
            len += compact_nullable_string_len(self.principal_filter.as_ref())?;
        } else {
            len += nullable_string_len(self.principal_filter.as_ref())?;
        }
        if version >= 2 {
            len += compact_nullable_string_len(self.host_filter.as_ref())?;
        } else {
            len += nullable_string_len(self.host_filter.as_ref())?;
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
