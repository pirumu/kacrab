//! Generated from DescribeUserScramCredentialsResponse.json - DO NOT EDIT
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
pub struct DescribeUserScramCredentialsResponseData {
    /// The duration in milliseconds for which the request was throttled due to a quota violation,
    /// or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// The message-level error code, 0 except for user authorization or infrastructure issues.
    pub error_code: i16,
    /// The message-level error message, if any.
    pub error_message: Option<KafkaString>,
    /// The results for descriptions, one per user.
    pub results: Vec<DescribeUserScramCredentialsResult>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for DescribeUserScramCredentialsResponseData {
    fn default() -> Self {
        Self {
            throttle_time_ms: 0_i32,
            error_code: 0_i16,
            error_message: None,
            results: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl DescribeUserScramCredentialsResponseData {
    pub fn with_throttle_time_ms(mut self, value: i32) -> Self {
        self.throttle_time_ms = value;
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
    pub fn with_results(mut self, value: Vec<DescribeUserScramCredentialsResult>) -> Self {
        self.results = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 0 || version > 0 {
            return Err(UnsupportedVersion::new(50, version).into());
        }
        let throttle_time_ms;
        let error_code;
        let error_message;
        let results;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        throttle_time_ms = read_i32(buf)?;
        error_code = read_i16(buf)?;
        error_message = read_compact_nullable_string(buf)?;
        results = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(len.max(0) as usize);
            for _ in 0..len {
                arr.push(DescribeUserScramCredentialsResult::read(buf, version)?);
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
            error_code,
            error_message,
            results,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 0 || version > 0 {
            return Err(UnsupportedVersion::new(50, version).into());
        }
        write_i32(buf, self.throttle_time_ms);
        write_i16(buf, self.error_code);
        write_compact_nullable_string(buf, self.error_message.as_ref())?;
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
            return Err(UnsupportedVersion::new(50, version).into());
        }
        let mut len: usize = 0;
        len += 4;
        len += 2;
        len += compact_nullable_string_len(self.error_message.as_ref())?;
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
pub struct DescribeUserScramCredentialsResult {
    /// The user name.
    pub user: KafkaString,
    /// The user-level error code.
    pub error_code: i16,
    /// The user-level error message, if any.
    pub error_message: Option<KafkaString>,
    /// The mechanism and related information associated with the user's SCRAM credentials.
    pub credential_infos: Vec<CredentialInfo>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for DescribeUserScramCredentialsResult {
    fn default() -> Self {
        Self {
            user: KafkaString::default(),
            error_code: 0_i16,
            error_message: None,
            credential_infos: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl DescribeUserScramCredentialsResult {
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
    pub fn with_credential_infos(mut self, value: Vec<CredentialInfo>) -> Self {
        self.credential_infos = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let user;
        let error_code;
        let error_message;
        let credential_infos;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        user = read_compact_string(buf)?;
        error_code = read_i16(buf)?;
        error_message = read_compact_nullable_string(buf)?;
        credential_infos = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(len.max(0) as usize);
            for _ in 0..len {
                arr.push(CredentialInfo::read(buf, version)?);
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
            user,
            error_code,
            error_message,
            credential_infos,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        write_compact_string(buf, &self.user)?;
        write_i16(buf, self.error_code);
        write_compact_nullable_string(buf, self.error_message.as_ref())?;
        write_compact_array_length(buf, self.credential_infos.len() as i32);
        for el in &self.credential_infos {
            el.write(buf, version)?;
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        let mut len: usize = 0;
        len += compact_string_len(&self.user)?;
        len += 2;
        len += compact_nullable_string_len(self.error_message.as_ref())?;
        len += compact_array_length_len(self.credential_infos.len() as i32);
        for el in &self.credential_infos {
            len += el.encoded_len(version)?;
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct CredentialInfo {
    /// The SCRAM mechanism.
    pub mechanism: i8,
    /// The number of iterations used in the SCRAM credential.
    pub iterations: i32,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for CredentialInfo {
    fn default() -> Self {
        Self {
            mechanism: 0_i8,
            iterations: 0_i32,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl CredentialInfo {
    pub fn with_mechanism(mut self, value: i8) -> Self {
        self.mechanism = value;
        self
    }
    pub fn with_iterations(mut self, value: i32) -> Self {
        self.iterations = value;
        self
    }
    pub fn read(buf: &mut Bytes, _version: i16) -> Result<Self> {
        let mechanism;
        let iterations;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        mechanism = read_i8(buf)?;
        iterations = read_i32(buf)?;
        let tagged_fields = read_tagged_fields(buf)?;
        for field in &tagged_fields {
            match field.tag {
                _ => {
                    _unknown_tagged_fields.push(field.clone());
                },
            }
        }
        Ok(Self {
            mechanism,
            iterations,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, _version: i16) -> Result<()> {
        write_i8(buf, self.mechanism);
        write_i32(buf, self.iterations);
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
    pub fn encoded_len(&self, _version: i16) -> Result<usize> {
        let mut len: usize = 0;
        len += 1;
        len += 4;
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
