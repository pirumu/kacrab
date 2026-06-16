//! Generated from RequestHeader.json - DO NOT EDIT
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
pub struct RequestHeaderData {
    /// The API key of this request.
    pub request_api_key: i16,
    /// The API version of this request.
    pub request_api_version: i16,
    /// The correlation ID of this request.
    pub correlation_id: i32,
    /// The client ID string.
    pub client_id: Option<KafkaString>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for RequestHeaderData {
    fn default() -> Self {
        Self {
            request_api_key: 0_i16,
            request_api_version: 0_i16,
            correlation_id: 0_i32,
            client_id: None,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl RequestHeaderData {
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let request_api_key;
        let request_api_version;
        let correlation_id;
        let client_id;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        request_api_key = read_i16(buf)?;
        request_api_version = read_i16(buf)?;
        correlation_id = read_i32(buf)?;
        client_id = read_nullable_string(buf)?;
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
            request_api_key,
            request_api_version,
            correlation_id,
            client_id,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        write_i16(buf, self.request_api_key);
        write_i16(buf, self.request_api_version);
        write_i32(buf, self.correlation_id);
        write_nullable_string(buf, self.client_id.as_ref())?;
        if version >= 2 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
}
