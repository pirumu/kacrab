//! Generated from DescribeDelegationTokenRequest.json - DO NOT EDIT
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
pub struct DescribeDelegationTokenRequestData {
    /// Each owner that we want to describe delegation tokens for, or null to describe all tokens.
    pub owners: Option<Vec<DescribeDelegationTokenOwner>>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for DescribeDelegationTokenRequestData {
    fn default() -> Self {
        Self {
            owners: None,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl DescribeDelegationTokenRequestData {
    pub fn with_owners(mut self, value: Option<Vec<DescribeDelegationTokenOwner>>) -> Self {
        self.owners = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 1 || version > 3 {
            return Err(UnsupportedVersion::new(41, version).into());
        }
        let owners;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 2 {
            owners = {
                let len = read_compact_array_length(buf)?;
                if len < 0 {
                    None
                } else {
                    let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
                    for _ in 0..len {
                        arr.push(DescribeDelegationTokenOwner::read(buf, version)?);
                    }
                    Some(arr)
                }
            };
        } else {
            owners = {
                let len = read_array_length(buf)?;
                if len < 0 {
                    None
                } else {
                    let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
                    for _ in 0..len {
                        arr.push(DescribeDelegationTokenOwner::read(buf, version)?);
                    }
                    Some(arr)
                }
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
            owners,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 1 || version > 3 {
            return Err(UnsupportedVersion::new(41, version).into());
        }
        if version >= 2 {
            match &self.owners {
                None => {
                    write_compact_array_length(buf, -1);
                },
                Some(arr) => {
                    write_compact_array_length(buf, arr.len() as i32);
                    for el in arr {
                        el.write(buf, version)?;
                    }
                },
            }
        } else {
            match &self.owners {
                None => {
                    write_array_length(buf, -1);
                },
                Some(arr) => {
                    write_array_length(buf, arr.len() as i32);
                    for el in arr {
                        el.write(buf, version)?;
                    }
                },
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
        if version < 1 || version > 3 {
            return Err(UnsupportedVersion::new(41, version).into());
        }
        let mut len: usize = 0;
        if version >= 2 {
            match &self.owners {
                None => {
                    len += compact_array_length_len(-1);
                },
                Some(arr) => {
                    len += compact_array_length_len(arr.len() as i32);
                    for el in arr {
                        len += el.encoded_len(version)?;
                    }
                },
            }
        } else {
            match &self.owners {
                None => {
                    len += array_length_len();
                },
                Some(arr) => {
                    len += array_length_len();
                    for el in arr {
                        len += el.encoded_len(version)?;
                    }
                },
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
pub struct DescribeDelegationTokenOwner {
    /// The owner principal type.
    pub principal_type: KafkaString,
    /// The owner principal name.
    pub principal_name: KafkaString,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for DescribeDelegationTokenOwner {
    fn default() -> Self {
        Self {
            principal_type: KafkaString::default(),
            principal_name: KafkaString::default(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl DescribeDelegationTokenOwner {
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
