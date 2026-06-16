//! Generated from CreateDelegationTokenRequest.json - DO NOT EDIT
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
pub struct CreateDelegationTokenRequestData {
    /// The principal type of the owner of the token. If it's null it defaults to the token request
    /// principal.
    pub owner_principal_type: Option<KafkaString>,
    /// The principal name of the owner of the token. If it's null it defaults to the token request
    /// principal.
    pub owner_principal_name: Option<KafkaString>,
    /// A list of those who are allowed to renew this token before it expires.
    pub renewers: Vec<CreatableRenewers>,
    /// The maximum lifetime of the token in milliseconds, or -1 to use the server side default.
    pub max_lifetime_ms: i64,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for CreateDelegationTokenRequestData {
    fn default() -> Self {
        Self {
            owner_principal_type: None,
            owner_principal_name: None,
            renewers: Vec::new(),
            max_lifetime_ms: 0_i64,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl CreateDelegationTokenRequestData {
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 1 || version > 3 {
            return Err(UnsupportedVersion::new(38, version).into());
        }
        let mut owner_principal_type = None;
        let mut owner_principal_name = None;
        let renewers;
        let max_lifetime_ms;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 3 {
            owner_principal_type = read_compact_nullable_string(buf)?;
        }
        if version >= 3 {
            owner_principal_name = read_compact_nullable_string(buf)?;
        }
        if version >= 2 {
            renewers = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(CreatableRenewers::read(buf, version)?);
                }
                arr
            };
        } else {
            renewers = {
                let len = read_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(CreatableRenewers::read(buf, version)?);
                }
                arr
            };
        }
        max_lifetime_ms = read_i64(buf)?;
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
            owner_principal_type,
            owner_principal_name,
            renewers,
            max_lifetime_ms,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 1 || version > 3 {
            return Err(UnsupportedVersion::new(38, version).into());
        }
        if version >= 3 {
            write_compact_nullable_string(buf, self.owner_principal_type.as_ref())?;
        }
        if version >= 3 {
            write_compact_nullable_string(buf, self.owner_principal_name.as_ref())?;
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
        write_i64(buf, self.max_lifetime_ms);
        if version >= 2 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct CreatableRenewers {
    /// The type of the Kafka principal.
    pub principal_type: KafkaString,
    /// The name of the Kafka principal.
    pub principal_name: KafkaString,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for CreatableRenewers {
    fn default() -> Self {
        Self {
            principal_type: KafkaString::default(),
            principal_name: KafkaString::default(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl CreatableRenewers {
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
}
