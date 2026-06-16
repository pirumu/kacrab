//! Generated from SaslHandshakeRequest.json - DO NOT EDIT
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
pub struct SaslHandshakeRequestData {
    /// The SASL mechanism chosen by the client.
    pub mechanism: KafkaString,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for SaslHandshakeRequestData {
    fn default() -> Self {
        Self {
            mechanism: KafkaString::default(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl SaslHandshakeRequestData {
    pub fn with_mechanism(mut self, value: KafkaString) -> Self {
        self.mechanism = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 0 || version > 1 {
            return Err(UnsupportedVersion::new(17, version).into());
        }
        let mechanism;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        mechanism = read_string(buf)?;
        Ok(Self {
            mechanism,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 0 || version > 1 {
            return Err(UnsupportedVersion::new(17, version).into());
        }
        write_string(buf, &self.mechanism)?;
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        if version < 0 || version > 1 {
            return Err(UnsupportedVersion::new(17, version).into());
        }
        let mut len: usize = 0;
        len += string_len(&self.mechanism)?;
        Ok(len)
    }
}
