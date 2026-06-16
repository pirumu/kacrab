//! Generated from SaslAuthenticateRequest.json - DO NOT EDIT
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
pub struct SaslAuthenticateRequestData {
    /// The SASL authentication bytes from the client, as defined by the SASL mechanism.
    pub auth_bytes: Bytes,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for SaslAuthenticateRequestData {
    fn default() -> Self {
        Self {
            auth_bytes: Bytes::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl SaslAuthenticateRequestData {
    pub fn with_auth_bytes(mut self, value: Bytes) -> Self {
        self.auth_bytes = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 0 || version > 2 {
            return Err(UnsupportedVersion::new(36, version).into());
        }
        let auth_bytes;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 2 {
            auth_bytes = read_compact_bytes(buf)?;
        } else {
            auth_bytes = read_bytes(buf)?;
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
            auth_bytes,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 0 || version > 2 {
            return Err(UnsupportedVersion::new(36, version).into());
        }
        if version >= 2 {
            write_compact_bytes(buf, &self.auth_bytes)?;
        } else {
            write_bytes(buf, &self.auth_bytes)?;
        }
        if version >= 2 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        if version < 0 || version > 2 {
            return Err(UnsupportedVersion::new(36, version).into());
        }
        let mut len: usize = 0;
        if version >= 2 {
            len += compact_bytes_len(&self.auth_bytes)?;
        } else {
            len += bytes_len(&self.auth_bytes)?;
        }
        if version >= 2 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            len += tagged_fields_len(&all_tags)?;
        }
        Ok(len)
    }
}
