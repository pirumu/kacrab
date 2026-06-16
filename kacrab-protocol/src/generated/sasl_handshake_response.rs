//! Generated from SaslHandshakeResponse.json - DO NOT EDIT
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
pub struct SaslHandshakeResponseData {
    /// The error code, or 0 if there was no error.
    pub error_code: i16,
    /// The mechanisms enabled in the server.
    pub mechanisms: Vec<KafkaString>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for SaslHandshakeResponseData {
    fn default() -> Self {
        Self {
            error_code: 0_i16,
            mechanisms: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl SaslHandshakeResponseData {
    pub fn with_error_code(mut self, value: i16) -> Self {
        self.error_code = value;
        self
    }
    pub fn with_mechanisms(mut self, value: Vec<KafkaString>) -> Self {
        self.mechanisms = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 0 || version > 1 {
            return Err(UnsupportedVersion::new(17, version).into());
        }
        let error_code;
        let mechanisms;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        error_code = read_i16(buf)?;
        mechanisms = {
            let len = read_array_length(buf)?;
            let mut arr = Vec::with_capacity(len.max(0) as usize);
            for _ in 0..len {
                arr.push(read_string(buf)?);
            }
            arr
        };
        Ok(Self {
            error_code,
            mechanisms,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 0 || version > 1 {
            return Err(UnsupportedVersion::new(17, version).into());
        }
        write_i16(buf, self.error_code);
        write_array_length(buf, self.mechanisms.len() as i32);
        for el in &self.mechanisms {
            write_string(buf, el)?;
        }
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        if version < 0 || version > 1 {
            return Err(UnsupportedVersion::new(17, version).into());
        }
        let mut len: usize = 0;
        len += 2;
        len += array_length_len();
        for el in &self.mechanisms {
            len += string_len(el)?;
        }
        Ok(len)
    }
}
