//! Generated from AlterUserScramCredentialsResponse.json - DO NOT EDIT
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
pub struct AlterUserScramCredentialsResponseData {
    /// The duration in milliseconds for which the request was throttled due to a quota violation,
    /// or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// The results for deletions and alterations, one per affected user.
    pub results: Vec<AlterUserScramCredentialsResult>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for AlterUserScramCredentialsResponseData {
    fn default() -> Self {
        Self {
            throttle_time_ms: 0_i32,
            results: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl AlterUserScramCredentialsResponseData {
    pub fn with_throttle_time_ms(mut self, value: i32) -> Self {
        self.throttle_time_ms = value;
        self
    }
    pub fn with_results(mut self, value: Vec<AlterUserScramCredentialsResult>) -> Self {
        self.results = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 0 || version > 0 {
            return Err(UnsupportedVersion::new(51, version).into());
        }
        let throttle_time_ms;
        let results;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        throttle_time_ms = read_i32(buf)?;
        results = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(len.max(0) as usize);
            for _ in 0..len {
                arr.push(AlterUserScramCredentialsResult::read(buf, version)?);
            }
            arr
        };
        let tagged_fields = read_tagged_fields(buf)?;
        for field in &tagged_fields {
            match field.tag {
                _ => {
                    _unknown_tagged_fields.push(field.clone());
                },
            }
        }
        Ok(Self {
            throttle_time_ms,
            results,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 0 || version > 0 {
            return Err(UnsupportedVersion::new(51, version).into());
        }
        write_i32(buf, self.throttle_time_ms);
        write_compact_array_length(buf, self.results.len() as i32);
        for el in &self.results {
            el.write(buf, version)?;
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        if version < 0 || version > 0 {
            return Err(UnsupportedVersion::new(51, version).into());
        }
        let mut len: usize = 0;
        len += 4;
        len += compact_array_length_len(self.results.len() as i32);
        for el in &self.results {
            len += el.encoded_len(version)?;
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct AlterUserScramCredentialsResult {
    /// The user name.
    pub user: KafkaString,
    /// The error code.
    pub error_code: i16,
    /// The error message, if any.
    pub error_message: Option<KafkaString>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for AlterUserScramCredentialsResult {
    fn default() -> Self {
        Self {
            user: KafkaString::default(),
            error_code: 0_i16,
            error_message: None,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl AlterUserScramCredentialsResult {
    pub fn with_user(mut self, value: KafkaString) -> Self {
        self.user = value;
        self
    }
    pub fn with_error_code(mut self, value: i16) -> Self {
        self.error_code = value;
        self
    }
    pub fn with_error_message(mut self, value: Option<KafkaString>) -> Self {
        self.error_message = value;
        self
    }
    pub fn read(buf: &mut Bytes, _version: i16) -> Result<Self> {
        let user;
        let error_code;
        let error_message;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        user = read_compact_string(buf)?;
        error_code = read_i16(buf)?;
        error_message = read_compact_nullable_string(buf)?;
        let tagged_fields = read_tagged_fields(buf)?;
        for field in &tagged_fields {
            match field.tag {
                _ => {
                    _unknown_tagged_fields.push(field.clone());
                },
            }
        }
        Ok(Self {
            user,
            error_code,
            error_message,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, _version: i16) -> Result<()> {
        write_compact_string(buf, &self.user)?;
        write_i16(buf, self.error_code);
        write_compact_nullable_string(buf, self.error_message.as_ref())?;
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
    pub fn encoded_len(&self, _version: i16) -> Result<usize> {
        let mut len: usize = 0;
        len += compact_string_len(&self.user)?;
        len += 2;
        len += compact_nullable_string_len(self.error_message.as_ref())?;
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
