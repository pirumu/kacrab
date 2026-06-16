//! Generated from ApiVersionsRequest.json - DO NOT EDIT
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
pub struct ApiVersionsRequestData {
    /// The name of the client.
    pub client_software_name: KafkaString,
    /// The version of the client.
    pub client_software_version: KafkaString,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for ApiVersionsRequestData {
    fn default() -> Self {
        Self {
            client_software_name: KafkaString::default(),
            client_software_version: KafkaString::default(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl ApiVersionsRequestData {
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 0 || version > 4 {
            return Err(UnsupportedVersion::new(18, version).into());
        }
        let mut client_software_name = KafkaString::default();
        let mut client_software_version = KafkaString::default();
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 3 {
            client_software_name = read_compact_string(buf)?;
        }
        if version >= 3 {
            client_software_version = read_compact_string(buf)?;
        }
        if version >= 3 {
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
            client_software_name,
            client_software_version,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 0 || version > 4 {
            return Err(UnsupportedVersion::new(18, version).into());
        }
        if version >= 3 {
            write_compact_string(buf, &self.client_software_name)?;
        }
        if version >= 3 {
            write_compact_string(buf, &self.client_software_version)?;
        }
        if version >= 3 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
}
