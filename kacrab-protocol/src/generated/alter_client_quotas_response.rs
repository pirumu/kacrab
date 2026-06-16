//! Generated from AlterClientQuotasResponse.json - DO NOT EDIT
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
pub struct AlterClientQuotasResponseData {
    /// The duration in milliseconds for which the request was throttled due to a quota violation,
    /// or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// The quota configuration entries to alter.
    pub entries: Vec<EntryData>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for AlterClientQuotasResponseData {
    fn default() -> Self {
        Self {
            throttle_time_ms: 0_i32,
            entries: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl AlterClientQuotasResponseData {
    pub fn with_throttle_time_ms(mut self, value: i32) -> Self {
        self.throttle_time_ms = value;
        self
    }
    pub fn with_entries(mut self, value: Vec<EntryData>) -> Self {
        self.entries = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 0 || version > 1 {
            return Err(UnsupportedVersion::new(49, version).into());
        }
        let throttle_time_ms;
        let entries;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        throttle_time_ms = read_i32(buf)?;
        if version >= 1 {
            entries = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(EntryData::read(buf, version)?);
                }
                arr
            };
        } else {
            entries = {
                let len = read_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(EntryData::read(buf, version)?);
                }
                arr
            };
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
            throttle_time_ms,
            entries,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 0 || version > 1 {
            return Err(UnsupportedVersion::new(49, version).into());
        }
        write_i32(buf, self.throttle_time_ms);
        if version >= 1 {
            write_compact_array_length(buf, self.entries.len() as i32);
            for el in &self.entries {
                el.write(buf, version)?;
            }
        } else {
            write_array_length(buf, self.entries.len() as i32);
            for el in &self.entries {
                el.write(buf, version)?;
            }
        }
        if version >= 1 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        if version < 0 || version > 1 {
            return Err(UnsupportedVersion::new(49, version).into());
        }
        let mut len: usize = 0;
        len += 4;
        if version >= 1 {
            len += compact_array_length_len(self.entries.len() as i32);
            for el in &self.entries {
                len += el.encoded_len(version)?;
            }
        } else {
            len += array_length_len();
            for el in &self.entries {
                len += el.encoded_len(version)?;
            }
        }
        if version >= 1 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            len += tagged_fields_len(&all_tags)?;
        }
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct EntryData {
    /// The error code, or `0` if the quota alteration succeeded.
    pub error_code: i16,
    /// The error message, or `null` if the quota alteration succeeded.
    pub error_message: Option<KafkaString>,
    /// The quota entity to alter.
    pub entity: Vec<EntityData>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for EntryData {
    fn default() -> Self {
        Self {
            error_code: 0_i16,
            error_message: None,
            entity: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl EntryData {
    pub fn with_error_code(mut self, value: i16) -> Self {
        self.error_code = value;
        self
    }
    pub fn with_error_message(mut self, value: Option<KafkaString>) -> Self {
        self.error_message = value;
        self
    }
    pub fn with_entity(mut self, value: Vec<EntityData>) -> Self {
        self.entity = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let error_code;
        let error_message;
        let entity;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        error_code = read_i16(buf)?;
        if version >= 1 {
            error_message = read_compact_nullable_string(buf)?;
        } else {
            error_message = read_nullable_string(buf)?;
        }
        if version >= 1 {
            entity = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(EntityData::read(buf, version)?);
                }
                arr
            };
        } else {
            entity = {
                let len = read_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(EntityData::read(buf, version)?);
                }
                arr
            };
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
            error_code,
            error_message,
            entity,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        write_i16(buf, self.error_code);
        if version >= 1 {
            write_compact_nullable_string(buf, self.error_message.as_ref())?;
        } else {
            write_nullable_string(buf, self.error_message.as_ref())?;
        }
        if version >= 1 {
            write_compact_array_length(buf, self.entity.len() as i32);
            for el in &self.entity {
                el.write(buf, version)?;
            }
        } else {
            write_array_length(buf, self.entity.len() as i32);
            for el in &self.entity {
                el.write(buf, version)?;
            }
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
        len += 2;
        if version >= 1 {
            len += compact_nullable_string_len(self.error_message.as_ref())?;
        } else {
            len += nullable_string_len(self.error_message.as_ref())?;
        }
        if version >= 1 {
            len += compact_array_length_len(self.entity.len() as i32);
            for el in &self.entity {
                len += el.encoded_len(version)?;
            }
        } else {
            len += array_length_len();
            for el in &self.entity {
                len += el.encoded_len(version)?;
            }
        }
        if version >= 1 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            len += tagged_fields_len(&all_tags)?;
        }
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct EntityData {
    /// The entity type.
    pub entity_type: KafkaString,
    /// The name of the entity, or null if the default.
    pub entity_name: Option<KafkaString>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for EntityData {
    fn default() -> Self {
        Self {
            entity_type: KafkaString::default(),
            entity_name: None,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl EntityData {
    pub fn with_entity_type(mut self, value: KafkaString) -> Self {
        self.entity_type = value;
        self
    }
    pub fn with_entity_name(mut self, value: Option<KafkaString>) -> Self {
        self.entity_name = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let entity_type;
        let entity_name;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 1 {
            entity_type = read_compact_string(buf)?;
        } else {
            entity_type = read_string(buf)?;
        }
        if version >= 1 {
            entity_name = read_compact_nullable_string(buf)?;
        } else {
            entity_name = read_nullable_string(buf)?;
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
            entity_name,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version >= 1 {
            write_compact_string(buf, &self.entity_type)?;
        } else {
            write_string(buf, &self.entity_type)?;
        }
        if version >= 1 {
            write_compact_nullable_string(buf, self.entity_name.as_ref())?;
        } else {
            write_nullable_string(buf, self.entity_name.as_ref())?;
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
        if version >= 1 {
            len += compact_nullable_string_len(self.entity_name.as_ref())?;
        } else {
            len += nullable_string_len(self.entity_name.as_ref())?;
        }
        if version >= 1 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            len += tagged_fields_len(&all_tags)?;
        }
        Ok(len)
    }
}
