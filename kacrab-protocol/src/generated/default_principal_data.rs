//! Generated from DefaultPrincipalData.json - DO NOT EDIT
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
pub struct DefaultPrincipalDataData {
    /// The principal type.
    pub r#type: KafkaString,
    /// The principal name.
    pub name: KafkaString,
    /// Whether the principal was authenticated by a delegation token on the forwarding broker.
    pub token_authenticated: bool,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for DefaultPrincipalDataData {
    fn default() -> Self {
        Self {
            r#type: KafkaString::default(),
            name: KafkaString::default(),
            token_authenticated: false,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl DefaultPrincipalDataData {
    pub fn with_type(mut self, value: KafkaString) -> Self {
        self.r#type = value;
        self
    }
    pub fn with_name(mut self, value: KafkaString) -> Self {
        self.name = value;
        self
    }
    pub fn with_token_authenticated(mut self, value: bool) -> Self {
        self.token_authenticated = value;
        self
    }
    pub fn read(buf: &mut Bytes, _version: i16) -> Result<Self> {
        let r#type;
        let name;
        let token_authenticated;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        r#type = read_compact_string(buf)?;
        name = read_compact_string(buf)?;
        token_authenticated = read_bool(buf)?;
        let tagged_fields = read_tagged_fields(buf)?;
        for field in &tagged_fields {
            match field.tag {
                _ => {
                    _unknown_tagged_fields.push(field.clone());
                },
            }
        }
        Ok(Self {
            r#type,
            name,
            token_authenticated,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, _version: i16) -> Result<()> {
        write_compact_string(buf, &self.r#type)?;
        write_compact_string(buf, &self.name)?;
        write_bool(buf, self.token_authenticated);
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
    pub fn encoded_len(&self, _version: i16) -> Result<usize> {
        let mut len: usize = 0;
        len += compact_string_len(&self.r#type)?;
        len += compact_string_len(&self.name)?;
        len += 1;
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
