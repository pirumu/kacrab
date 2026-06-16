//! Generated from SaslAuthenticateResponse.json - DO NOT EDIT
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
pub struct SaslAuthenticateResponseData {
    /// The error code, or 0 if there was no error.
    pub error_code: i16,
    /// The error message, or null if there was no error.
    pub error_message: Option<KafkaString>,
    /// The SASL authentication bytes from the server, as defined by the SASL mechanism.
    pub auth_bytes: Bytes,
    /// Number of milliseconds after which only re-authentication over the existing connection to
    /// create a new session can occur.
    pub session_lifetime_ms: i64,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for SaslAuthenticateResponseData {
    fn default() -> Self {
        Self {
            error_code: 0_i16,
            error_message: None,
            auth_bytes: Bytes::new(),
            session_lifetime_ms: 0i64,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl SaslAuthenticateResponseData {
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 0 || version > 2 {
            return Err(UnsupportedVersion::new(36, version).into());
        }
        let error_code;
        let error_message;
        let auth_bytes;
        let mut session_lifetime_ms = 0i64;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        error_code = read_i16(buf)?;
        if version >= 2 {
            error_message = read_compact_nullable_string(buf)?;
        } else {
            error_message = read_nullable_string(buf)?;
        }
        if version >= 2 {
            auth_bytes = read_compact_bytes(buf)?;
        } else {
            auth_bytes = read_bytes(buf)?;
        }
        if version >= 1 {
            session_lifetime_ms = read_i64(buf)?;
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
            error_code,
            error_message,
            auth_bytes,
            session_lifetime_ms,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 0 || version > 2 {
            return Err(UnsupportedVersion::new(36, version).into());
        }
        write_i16(buf, self.error_code);
        if version >= 2 {
            write_compact_nullable_string(buf, self.error_message.as_ref())?;
        } else {
            write_nullable_string(buf, self.error_message.as_ref())?;
        }
        if version >= 2 {
            write_compact_bytes(buf, &self.auth_bytes)?;
        } else {
            write_bytes(buf, &self.auth_bytes)?;
        }
        if version >= 1 {
            write_i64(buf, self.session_lifetime_ms);
        }
        if version >= 2 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
}
