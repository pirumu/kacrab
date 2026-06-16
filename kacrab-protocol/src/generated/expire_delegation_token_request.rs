//! Generated from ExpireDelegationTokenRequest.json - DO NOT EDIT
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
pub struct ExpireDelegationTokenRequestData {
    /// The HMAC of the delegation token to be expired.
    pub hmac: Bytes,
    /// The expiry time period in milliseconds.
    pub expiry_time_period_ms: i64,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for ExpireDelegationTokenRequestData {
    fn default() -> Self {
        Self {
            hmac: Bytes::new(),
            expiry_time_period_ms: 0_i64,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl ExpireDelegationTokenRequestData {
    pub fn with_hmac(mut self, value: Bytes) -> Self {
        self.hmac = value;
        self
    }
    pub fn with_expiry_time_period_ms(mut self, value: i64) -> Self {
        self.expiry_time_period_ms = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 1 || version > 2 {
            return Err(UnsupportedVersion::new(40, version).into());
        }
        let hmac;
        let expiry_time_period_ms;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 2 {
            hmac = read_compact_bytes(buf)?;
        } else {
            hmac = read_bytes(buf)?;
        }
        expiry_time_period_ms = read_i64(buf)?;
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
            hmac,
            expiry_time_period_ms,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 1 || version > 2 {
            return Err(UnsupportedVersion::new(40, version).into());
        }
        if version >= 2 {
            write_compact_bytes(buf, &self.hmac)?;
        } else {
            write_bytes(buf, &self.hmac)?;
        }
        write_i64(buf, self.expiry_time_period_ms);
        if version >= 2 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        if version < 1 || version > 2 {
            return Err(UnsupportedVersion::new(40, version).into());
        }
        let mut len: usize = 0;
        if version >= 2 {
            len += compact_bytes_len(&self.hmac)?;
        } else {
            len += bytes_len(&self.hmac)?;
        }
        len += 8;
        if version >= 2 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            len += tagged_fields_len(&all_tags)?;
        }
        Ok(len)
    }
}
