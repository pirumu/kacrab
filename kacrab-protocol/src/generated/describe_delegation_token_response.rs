//! Generated from DescribeDelegationTokenResponse.json - DO NOT EDIT
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
pub struct DescribeDelegationTokenResponseData {
    /// The error code, or 0 if there was no error.
    pub error_code: i16,
    /// The tokens.
    pub tokens: Vec<DescribedDelegationToken>,
    /// The duration in milliseconds for which the request was throttled due to a quota violation,
    /// or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for DescribeDelegationTokenResponseData {
    fn default() -> Self {
        Self {
            error_code: 0_i16,
            tokens: Vec::new(),
            throttle_time_ms: 0_i32,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl DescribeDelegationTokenResponseData {
    pub fn with_error_code(mut self, value: i16) -> Self {
        self.error_code = value;
        self
    }
    pub fn with_tokens(mut self, value: Vec<DescribedDelegationToken>) -> Self {
        self.tokens = value;
        self
    }
    pub fn with_throttle_time_ms(mut self, value: i32) -> Self {
        self.throttle_time_ms = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 1 || version > 3 {
            return Err(UnsupportedVersion::new(41, version).into());
        }
        let error_code;
        let tokens;
        let throttle_time_ms;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        error_code = read_i16(buf)?;
        if version >= 2 {
            tokens = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(DescribedDelegationToken::read(buf, version)?);
                }
                arr
            };
        } else {
            tokens = {
                let len = read_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(DescribedDelegationToken::read(buf, version)?);
                }
                arr
            };
        }
        throttle_time_ms = read_i32(buf)?;
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
            tokens,
            throttle_time_ms,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 1 || version > 3 {
            return Err(UnsupportedVersion::new(41, version).into());
        }
        write_i16(buf, self.error_code);
        if version >= 2 {
            write_compact_array_length(buf, self.tokens.len() as i32);
            for el in &self.tokens {
                el.write(buf, version)?;
            }
        } else {
            write_array_length(buf, self.tokens.len() as i32);
            for el in &self.tokens {
                el.write(buf, version)?;
            }
        }
        write_i32(buf, self.throttle_time_ms);
        if version >= 2 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        if version < 1 || version > 3 {
            return Err(UnsupportedVersion::new(41, version).into());
        }
        let mut len: usize = 0;
        len += 2;
        if version >= 2 {
            len += compact_array_length_len(self.tokens.len() as i32);
            for el in &self.tokens {
                len += el.encoded_len(version)?;
            }
        } else {
            len += array_length_len();
            for el in &self.tokens {
                len += el.encoded_len(version)?;
            }
        }
        len += 4;
        if version >= 2 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            len += tagged_fields_len(&all_tags)?;
        }
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct DescribedDelegationToken {
    /// The token principal type.
    pub principal_type: KafkaString,
    /// The token principal name.
    pub principal_name: KafkaString,
    /// The principal type of the requester of the token.
    pub token_requester_principal_type: KafkaString,
    /// The principal type of the requester of the token.
    pub token_requester_principal_name: KafkaString,
    /// The token issue timestamp in milliseconds.
    pub issue_timestamp: i64,
    /// The token expiry timestamp in milliseconds.
    pub expiry_timestamp: i64,
    /// The token maximum timestamp length in milliseconds.
    pub max_timestamp: i64,
    /// The token ID.
    pub token_id: KafkaString,
    /// The token HMAC.
    pub hmac: Bytes,
    /// Those who are able to renew this token before it expires.
    pub renewers: Vec<DescribedDelegationTokenRenewer>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for DescribedDelegationToken {
    fn default() -> Self {
        Self {
            principal_type: KafkaString::default(),
            principal_name: KafkaString::default(),
            token_requester_principal_type: KafkaString::default(),
            token_requester_principal_name: KafkaString::default(),
            issue_timestamp: 0_i64,
            expiry_timestamp: 0_i64,
            max_timestamp: 0_i64,
            token_id: KafkaString::default(),
            hmac: Bytes::new(),
            renewers: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl DescribedDelegationToken {
    pub fn with_principal_type(mut self, value: KafkaString) -> Self {
        self.principal_type = value;
        self
    }
    pub fn with_principal_name(mut self, value: KafkaString) -> Self {
        self.principal_name = value;
        self
    }
    pub fn with_token_requester_principal_type(mut self, value: KafkaString) -> Self {
        self.token_requester_principal_type = value;
        self
    }
    pub fn with_token_requester_principal_name(mut self, value: KafkaString) -> Self {
        self.token_requester_principal_name = value;
        self
    }
    pub fn with_issue_timestamp(mut self, value: i64) -> Self {
        self.issue_timestamp = value;
        self
    }
    pub fn with_expiry_timestamp(mut self, value: i64) -> Self {
        self.expiry_timestamp = value;
        self
    }
    pub fn with_max_timestamp(mut self, value: i64) -> Self {
        self.max_timestamp = value;
        self
    }
    pub fn with_token_id(mut self, value: KafkaString) -> Self {
        self.token_id = value;
        self
    }
    pub fn with_hmac(mut self, value: Bytes) -> Self {
        self.hmac = value;
        self
    }
    pub fn with_renewers(mut self, value: Vec<DescribedDelegationTokenRenewer>) -> Self {
        self.renewers = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let principal_type;
        let principal_name;
        let mut token_requester_principal_type = KafkaString::default();
        let mut token_requester_principal_name = KafkaString::default();
        let issue_timestamp;
        let expiry_timestamp;
        let max_timestamp;
        let token_id;
        let hmac;
        let renewers;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 2 {
            principal_type = read_compact_string(buf)?;
        } else {
            principal_type = read_string(buf)?;
        }
        if version >= 2 {
            principal_name = read_compact_string(buf)?;
        } else {
            principal_name = read_string(buf)?;
        }
        if version >= 3 {
            token_requester_principal_type = read_compact_string(buf)?;
        }
        if version >= 3 {
            token_requester_principal_name = read_compact_string(buf)?;
        }
        issue_timestamp = read_i64(buf)?;
        expiry_timestamp = read_i64(buf)?;
        max_timestamp = read_i64(buf)?;
        if version >= 2 {
            token_id = read_compact_string(buf)?;
        } else {
            token_id = read_string(buf)?;
        }
        if version >= 2 {
            hmac = read_compact_bytes(buf)?;
        } else {
            hmac = read_bytes(buf)?;
        }
        if version >= 2 {
            renewers = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(DescribedDelegationTokenRenewer::read(buf, version)?);
                }
                arr
            };
        } else {
            renewers = {
                let len = read_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(DescribedDelegationTokenRenewer::read(buf, version)?);
                }
                arr
            };
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
            principal_type,
            principal_name,
            token_requester_principal_type,
            token_requester_principal_name,
            issue_timestamp,
            expiry_timestamp,
            max_timestamp,
            token_id,
            hmac,
            renewers,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version >= 2 {
            write_compact_string(buf, &self.principal_type)?;
        } else {
            write_string(buf, &self.principal_type)?;
        }
        if version >= 2 {
            write_compact_string(buf, &self.principal_name)?;
        } else {
            write_string(buf, &self.principal_name)?;
        }
        if version >= 3 {
            write_compact_string(buf, &self.token_requester_principal_type)?;
        } else if self.token_requester_principal_type != KafkaString::default() {
            return Err(UnsupportedFieldVersion::new(
                41,
                "token_requester_principal_type",
                version,
            )
            .into());
        }
        if version >= 3 {
            write_compact_string(buf, &self.token_requester_principal_name)?;
        } else if self.token_requester_principal_name != KafkaString::default() {
            return Err(UnsupportedFieldVersion::new(
                41,
                "token_requester_principal_name",
                version,
            )
            .into());
        }
        write_i64(buf, self.issue_timestamp);
        write_i64(buf, self.expiry_timestamp);
        write_i64(buf, self.max_timestamp);
        if version >= 2 {
            write_compact_string(buf, &self.token_id)?;
        } else {
            write_string(buf, &self.token_id)?;
        }
        if version >= 2 {
            write_compact_bytes(buf, &self.hmac)?;
        } else {
            write_bytes(buf, &self.hmac)?;
        }
        if version >= 2 {
            write_compact_array_length(buf, self.renewers.len() as i32);
            for el in &self.renewers {
                el.write(buf, version)?;
            }
        } else {
            write_array_length(buf, self.renewers.len() as i32);
            for el in &self.renewers {
                el.write(buf, version)?;
            }
        }
        if version >= 2 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        let mut len: usize = 0;
        if version >= 2 {
            len += compact_string_len(&self.principal_type)?;
        } else {
            len += string_len(&self.principal_type)?;
        }
        if version >= 2 {
            len += compact_string_len(&self.principal_name)?;
        } else {
            len += string_len(&self.principal_name)?;
        }
        if version >= 3 {
            len += compact_string_len(&self.token_requester_principal_type)?;
        } else if self.token_requester_principal_type != KafkaString::default() {
            return Err(UnsupportedFieldVersion::new(
                41,
                "token_requester_principal_type",
                version,
            )
            .into());
        }
        if version >= 3 {
            len += compact_string_len(&self.token_requester_principal_name)?;
        } else if self.token_requester_principal_name != KafkaString::default() {
            return Err(UnsupportedFieldVersion::new(
                41,
                "token_requester_principal_name",
                version,
            )
            .into());
        }
        len += 8;
        len += 8;
        len += 8;
        if version >= 2 {
            len += compact_string_len(&self.token_id)?;
        } else {
            len += string_len(&self.token_id)?;
        }
        if version >= 2 {
            len += compact_bytes_len(&self.hmac)?;
        } else {
            len += bytes_len(&self.hmac)?;
        }
        if version >= 2 {
            len += compact_array_length_len(self.renewers.len() as i32);
            for el in &self.renewers {
                len += el.encoded_len(version)?;
            }
        } else {
            len += array_length_len();
            for el in &self.renewers {
                len += el.encoded_len(version)?;
            }
        }
        if version >= 2 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            len += tagged_fields_len(&all_tags)?;
        }
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct DescribedDelegationTokenRenewer {
    /// The renewer principal type.
    pub principal_type: KafkaString,
    /// The renewer principal name.
    pub principal_name: KafkaString,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for DescribedDelegationTokenRenewer {
    fn default() -> Self {
        Self {
            principal_type: KafkaString::default(),
            principal_name: KafkaString::default(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl DescribedDelegationTokenRenewer {
    pub fn with_principal_type(mut self, value: KafkaString) -> Self {
        self.principal_type = value;
        self
    }
    pub fn with_principal_name(mut self, value: KafkaString) -> Self {
        self.principal_name = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let principal_type;
        let principal_name;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 2 {
            principal_type = read_compact_string(buf)?;
        } else {
            principal_type = read_string(buf)?;
        }
        if version >= 2 {
            principal_name = read_compact_string(buf)?;
        } else {
            principal_name = read_string(buf)?;
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
            principal_type,
            principal_name,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version >= 2 {
            write_compact_string(buf, &self.principal_type)?;
        } else {
            write_string(buf, &self.principal_type)?;
        }
        if version >= 2 {
            write_compact_string(buf, &self.principal_name)?;
        } else {
            write_string(buf, &self.principal_name)?;
        }
        if version >= 2 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        let mut len: usize = 0;
        if version >= 2 {
            len += compact_string_len(&self.principal_type)?;
        } else {
            len += string_len(&self.principal_type)?;
        }
        if version >= 2 {
            len += compact_string_len(&self.principal_name)?;
        } else {
            len += string_len(&self.principal_name)?;
        }
        if version >= 2 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            len += tagged_fields_len(&all_tags)?;
        }
        Ok(len)
    }
}
