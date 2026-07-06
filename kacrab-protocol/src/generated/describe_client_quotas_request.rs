//! Generated from DescribeClientQuotasRequest.json - DO NOT EDIT
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
pub struct DescribeClientQuotasRequestData {
    /// Filter components to apply to quota entities.
    pub components: Vec<ComponentData>,
    /// Whether the match is strict, i.e. should exclude entities with unspecified entity types.
    pub strict: bool,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for DescribeClientQuotasRequestData {
    fn default() -> Self {
        Self {
            components: Vec::new(),
            strict: false,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl DescribeClientQuotasRequestData {
    pub fn with_components(mut self, value: Vec<ComponentData>) -> Self {
        self.components = value;
        self
    }
    pub fn with_strict(mut self, value: bool) -> Self {
        self.strict = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 0 || version > 1 {
            return Err(UnsupportedVersion::new(48, version).into());
        }
        let components;
        let strict;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 1 {
            components = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
                for _ in 0..len {
                    arr.push(ComponentData::read(buf, version)?);
                }
                arr
            };
        } else {
            components = {
                let len = read_array_length(buf)?;
                let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
                for _ in 0..len {
                    arr.push(ComponentData::read(buf, version)?);
                }
                arr
            };
        }
        strict = read_bool(buf)?;
        if version >= 1 {
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
            components,
            strict,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 0 || version > 1 {
            return Err(UnsupportedVersion::new(48, version).into());
        }
        if version >= 1 {
            write_compact_array_length(buf, self.components.len() as i32);
            for el in &self.components {
                el.write(buf, version)?;
            }
        } else {
            write_array_length(buf, self.components.len() as i32);
            for el in &self.components {
                el.write(buf, version)?;
            }
        }
        write_bool(buf, self.strict);
        if version >= 1 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        if version < 0 || version > 1 {
            return Err(UnsupportedVersion::new(48, version).into());
        }
        let mut len: usize = 0;
        if version >= 1 {
            len += compact_array_length_len(self.components.len() as i32);
            for el in &self.components {
                len += el.encoded_len(version)?;
            }
        } else {
            len += array_length_len();
            for el in &self.components {
                len += el.encoded_len(version)?;
            }
        }
        len += 1;
        if version >= 1 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            len += tagged_fields_len(&all_tags)?;
        }
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct ComponentData {
    /// The entity type that the filter component applies to.
    pub entity_type: KafkaString,
    /// How to match the entity {0 = exact name, 1 = default name, 2 = any specified name}.
    pub match_type: i8,
    /// The string to match against, or null if unused for the match type.
    pub r#match: Option<KafkaString>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for ComponentData {
    fn default() -> Self {
        Self {
            entity_type: KafkaString::default(),
            match_type: 0_i8,
            r#match: None,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl ComponentData {
    pub fn with_entity_type(mut self, value: KafkaString) -> Self {
        self.entity_type = value;
        self
    }
    pub fn with_match_type(mut self, value: i8) -> Self {
        self.match_type = value;
        self
    }
    pub fn with_match(mut self, value: Option<KafkaString>) -> Self {
        self.r#match = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let entity_type;
        let match_type;
        let r#match;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 1 {
            entity_type = read_compact_string(buf)?;
        } else {
            entity_type = read_string(buf)?;
        }
        match_type = read_i8(buf)?;
        if version >= 1 {
            r#match = read_compact_nullable_string(buf)?;
        } else {
            r#match = read_nullable_string(buf)?;
        }
        if version >= 1 {
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
            entity_type,
            match_type,
            r#match,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version >= 1 {
            write_compact_string(buf, &self.entity_type)?;
        } else {
            write_string(buf, &self.entity_type)?;
        }
        write_i8(buf, self.match_type);
        if version >= 1 {
            write_compact_nullable_string(buf, self.r#match.as_ref())?;
        } else {
            write_nullable_string(buf, self.r#match.as_ref())?;
        }
        if version >= 1 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        let mut len: usize = 0;
        if version >= 1 {
            len += compact_string_len(&self.entity_type)?;
        } else {
            len += string_len(&self.entity_type)?;
        }
        len += 1;
        if version >= 1 {
            len += compact_nullable_string_len(self.r#match.as_ref())?;
        } else {
            len += nullable_string_len(self.r#match.as_ref())?;
        }
        if version >= 1 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            len += tagged_fields_len(&all_tags)?;
        }
        Ok(len)
    }
}
