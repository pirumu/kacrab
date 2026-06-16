//! Generated from AlterUserScramCredentialsRequest.json - DO NOT EDIT
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
pub struct AlterUserScramCredentialsRequestData {
    /// The SCRAM credentials to remove.
    pub deletions: Vec<ScramCredentialDeletion>,
    /// The SCRAM credentials to update/insert.
    pub upsertions: Vec<ScramCredentialUpsertion>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for AlterUserScramCredentialsRequestData {
    fn default() -> Self {
        Self {
            deletions: Vec::new(),
            upsertions: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl AlterUserScramCredentialsRequestData {
    pub fn with_deletions(mut self, value: Vec<ScramCredentialDeletion>) -> Self {
        self.deletions = value;
        self
    }
    pub fn with_upsertions(mut self, value: Vec<ScramCredentialUpsertion>) -> Self {
        self.upsertions = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 0 || version > 0 {
            return Err(UnsupportedVersion::new(51, version).into());
        }
        let deletions;
        let upsertions;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        deletions = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(len.max(0) as usize);
            for _ in 0..len {
                arr.push(ScramCredentialDeletion::read(buf, version)?);
            }
            arr
        };
        upsertions = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(len.max(0) as usize);
            for _ in 0..len {
                arr.push(ScramCredentialUpsertion::read(buf, version)?);
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
            deletions,
            upsertions,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 0 || version > 0 {
            return Err(UnsupportedVersion::new(51, version).into());
        }
        write_compact_array_length(buf, self.deletions.len() as i32);
        for el in &self.deletions {
            el.write(buf, version)?;
        }
        write_compact_array_length(buf, self.upsertions.len() as i32);
        for el in &self.upsertions {
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
        len += compact_array_length_len(self.deletions.len() as i32);
        for el in &self.deletions {
            len += el.encoded_len(version)?;
        }
        len += compact_array_length_len(self.upsertions.len() as i32);
        for el in &self.upsertions {
            len += el.encoded_len(version)?;
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct ScramCredentialDeletion {
    /// The user name.
    pub name: KafkaString,
    /// The SCRAM mechanism.
    pub mechanism: i8,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for ScramCredentialDeletion {
    fn default() -> Self {
        Self {
            name: KafkaString::default(),
            mechanism: 0_i8,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl ScramCredentialDeletion {
    pub fn with_name(mut self, value: KafkaString) -> Self {
        self.name = value;
        self
    }
    pub fn with_mechanism(mut self, value: i8) -> Self {
        self.mechanism = value;
        self
    }
    pub fn read(buf: &mut Bytes, _version: i16) -> Result<Self> {
        let name;
        let mechanism;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        name = read_compact_string(buf)?;
        mechanism = read_i8(buf)?;
        let tagged_fields = read_tagged_fields(buf)?;
        for field in &tagged_fields {
            match field.tag {
                _ => {
                    _unknown_tagged_fields.push(field.clone());
                },
            }
        }
        Ok(Self {
            name,
            mechanism,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, _version: i16) -> Result<()> {
        write_compact_string(buf, &self.name)?;
        write_i8(buf, self.mechanism);
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
    pub fn encoded_len(&self, _version: i16) -> Result<usize> {
        let mut len: usize = 0;
        len += compact_string_len(&self.name)?;
        len += 1;
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct ScramCredentialUpsertion {
    /// The user name.
    pub name: KafkaString,
    /// The SCRAM mechanism.
    pub mechanism: i8,
    /// The number of iterations.
    pub iterations: i32,
    /// A random salt generated by the client.
    pub salt: Bytes,
    /// The salted password.
    pub salted_password: Bytes,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for ScramCredentialUpsertion {
    fn default() -> Self {
        Self {
            name: KafkaString::default(),
            mechanism: 0_i8,
            iterations: 0_i32,
            salt: Bytes::new(),
            salted_password: Bytes::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl ScramCredentialUpsertion {
    pub fn with_name(mut self, value: KafkaString) -> Self {
        self.name = value;
        self
    }
    pub fn with_mechanism(mut self, value: i8) -> Self {
        self.mechanism = value;
        self
    }
    pub fn with_iterations(mut self, value: i32) -> Self {
        self.iterations = value;
        self
    }
    pub fn with_salt(mut self, value: Bytes) -> Self {
        self.salt = value;
        self
    }
    pub fn with_salted_password(mut self, value: Bytes) -> Self {
        self.salted_password = value;
        self
    }
    pub fn read(buf: &mut Bytes, _version: i16) -> Result<Self> {
        let name;
        let mechanism;
        let iterations;
        let salt;
        let salted_password;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        name = read_compact_string(buf)?;
        mechanism = read_i8(buf)?;
        iterations = read_i32(buf)?;
        salt = read_compact_bytes(buf)?;
        salted_password = read_compact_bytes(buf)?;
        let tagged_fields = read_tagged_fields(buf)?;
        for field in &tagged_fields {
            match field.tag {
                _ => {
                    _unknown_tagged_fields.push(field.clone());
                },
            }
        }
        Ok(Self {
            name,
            mechanism,
            iterations,
            salt,
            salted_password,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, _version: i16) -> Result<()> {
        write_compact_string(buf, &self.name)?;
        write_i8(buf, self.mechanism);
        write_i32(buf, self.iterations);
        write_compact_bytes(buf, &self.salt)?;
        write_compact_bytes(buf, &self.salted_password)?;
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
    pub fn encoded_len(&self, _version: i16) -> Result<usize> {
        let mut len: usize = 0;
        len += compact_string_len(&self.name)?;
        len += 1;
        len += 4;
        len += compact_bytes_len(&self.salt)?;
        len += compact_bytes_len(&self.salted_password)?;
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
