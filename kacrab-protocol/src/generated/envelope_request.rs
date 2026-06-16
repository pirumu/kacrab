//! Generated from EnvelopeRequest.json - DO NOT EDIT
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
pub struct EnvelopeRequestData {
    /// The embedded request header and data.
    pub request_data: Bytes,
    /// Value of the initial client principal when the request is redirected by a broker.
    pub request_principal: Option<Bytes>,
    /// The original client's address in bytes.
    pub client_host_address: Bytes,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for EnvelopeRequestData {
    fn default() -> Self {
        Self {
            request_data: Bytes::new(),
            request_principal: None,
            client_host_address: Bytes::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl EnvelopeRequestData {
    pub fn with_request_data(mut self, value: Bytes) -> Self {
        self.request_data = value;
        self
    }
    pub fn with_request_principal(mut self, value: Option<Bytes>) -> Self {
        self.request_principal = value;
        self
    }
    pub fn with_client_host_address(mut self, value: Bytes) -> Self {
        self.client_host_address = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 0 || version > 0 {
            return Err(UnsupportedVersion::new(58, version).into());
        }
        let request_data;
        let request_principal;
        let client_host_address;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        request_data = read_compact_bytes(buf)?;
        request_principal = read_compact_nullable_bytes(buf)?;
        client_host_address = read_compact_bytes(buf)?;
        let tagged_fields = read_tagged_fields(buf)?;
        for field in &tagged_fields {
            match field.tag {
                _ => {
                    _unknown_tagged_fields.push(field.clone());
                },
            }
        }
        Ok(Self {
            request_data,
            request_principal,
            client_host_address,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 0 || version > 0 {
            return Err(UnsupportedVersion::new(58, version).into());
        }
        write_compact_bytes(buf, &self.request_data)?;
        write_compact_nullable_bytes(buf, self.request_principal.as_ref().map(|b| b.as_ref()))?;
        write_compact_bytes(buf, &self.client_host_address)?;
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        if version < 0 || version > 0 {
            return Err(UnsupportedVersion::new(58, version).into());
        }
        let mut len: usize = 0;
        len += compact_bytes_len(&self.request_data)?;
        len += compact_nullable_bytes_len(self.request_principal.as_ref().map(|b| b.as_ref()))?;
        len += compact_bytes_len(&self.client_host_address)?;
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
