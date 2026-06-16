//! Generated from SaslAuthenticateResponse.json - DO NOT EDIT
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
    pub fn with_error_code(mut self, value: i16) -> Self {
        self.error_code = value;
        self
    }
    pub fn with_error_message(mut self, value: Option<KafkaString>) -> Self {
        self.error_message = value;
        self
    }
    pub fn with_auth_bytes(mut self, value: Bytes) -> Self {
        self.auth_bytes = value;
        self
    }
    pub fn with_session_lifetime_ms(mut self, value: i64) -> Self {
        self.session_lifetime_ms = value;
        self
    }
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
        } else if self.session_lifetime_ms != 0i64 {
            return Err(UnsupportedFieldVersion::new(36, "session_lifetime_ms", version).into());
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
        len += 2;
        if version >= 2 {
            len += compact_nullable_string_len(self.error_message.as_ref())?;
        } else {
            len += nullable_string_len(self.error_message.as_ref())?;
        }
        if version >= 2 {
            len += compact_bytes_len(&self.auth_bytes)?;
        } else {
            len += bytes_len(&self.auth_bytes)?;
        }
        if version >= 1 {
            len += 8;
        } else if self.session_lifetime_ms != 0i64 {
            return Err(UnsupportedFieldVersion::new(36, "session_lifetime_ms", version).into());
        }
        if version >= 2 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            len += tagged_fields_len(&all_tags)?;
        }
        Ok(len)
    }
}
