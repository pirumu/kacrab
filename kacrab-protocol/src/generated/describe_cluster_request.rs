//! Generated from DescribeClusterRequest.json - DO NOT EDIT
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
pub struct DescribeClusterRequestData {
    /// Whether to include cluster authorized operations.
    pub include_cluster_authorized_operations: bool,
    /// The endpoint type to describe. 1=brokers, 2=controllers.
    pub endpoint_type: i8,
    /// Whether to include fenced brokers when listing brokers.
    pub include_fenced_brokers: bool,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for DescribeClusterRequestData {
    fn default() -> Self {
        Self {
            include_cluster_authorized_operations: false,
            endpoint_type: 1i8,
            include_fenced_brokers: false,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl DescribeClusterRequestData {
    pub fn with_include_cluster_authorized_operations(mut self, value: bool) -> Self {
        self.include_cluster_authorized_operations = value;
        self
    }
    pub fn with_endpoint_type(mut self, value: i8) -> Self {
        self.endpoint_type = value;
        self
    }
    pub fn with_include_fenced_brokers(mut self, value: bool) -> Self {
        self.include_fenced_brokers = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 0 || version > 2 {
            return Err(UnsupportedVersion::new(60, version).into());
        }
        let include_cluster_authorized_operations;
        let mut endpoint_type = 1i8;
        let mut include_fenced_brokers = false;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        include_cluster_authorized_operations = read_bool(buf)?;
        if version >= 1 {
            endpoint_type = read_i8(buf)?;
        }
        if version >= 2 {
            include_fenced_brokers = read_bool(buf)?;
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
            include_cluster_authorized_operations,
            endpoint_type,
            include_fenced_brokers,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 0 || version > 2 {
            return Err(UnsupportedVersion::new(60, version).into());
        }
        write_bool(buf, self.include_cluster_authorized_operations);
        if version >= 1 {
            write_i8(buf, self.endpoint_type);
        } else if self.endpoint_type != 1i8 {
            return Err(UnsupportedFieldVersion::new(60, "endpoint_type", version).into());
        }
        if version >= 2 {
            write_bool(buf, self.include_fenced_brokers);
        } else if self.include_fenced_brokers != false {
            return Err(UnsupportedFieldVersion::new(60, "include_fenced_brokers", version).into());
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        if version < 0 || version > 2 {
            return Err(UnsupportedVersion::new(60, version).into());
        }
        let mut len: usize = 0;
        len += 1;
        if version >= 1 {
            len += 1;
        } else if self.endpoint_type != 1i8 {
            return Err(UnsupportedFieldVersion::new(60, "endpoint_type", version).into());
        }
        if version >= 2 {
            len += 1;
        } else if self.include_fenced_brokers != false {
            return Err(UnsupportedFieldVersion::new(60, "include_fenced_brokers", version).into());
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
