//! Generated from AlterClientQuotasRequest.json - DO NOT EDIT
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
pub struct AlterClientQuotasRequestData {
    /// The quota configuration entries to alter.
    pub entries: Vec<EntryData>,
    /// Whether the alteration should be validated, but not performed.
    pub validate_only: bool,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for AlterClientQuotasRequestData {
    fn default() -> Self {
        Self {
            entries: Vec::new(),
            validate_only: false,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl AlterClientQuotasRequestData {
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 0 || version > 1 {
            return Err(UnsupportedVersion::new(49, version).into());
        }
        let entries;
        let validate_only;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
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
        validate_only = read_bool(buf)?;
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
            entries,
            validate_only,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 0 || version > 1 {
            return Err(UnsupportedVersion::new(49, version).into());
        }
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
        write_bool(buf, self.validate_only);
        if version >= 1 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct EntryData {
    /// The quota entity to alter.
    pub entity: Vec<EntityData>,
    /// An individual quota configuration entry to alter.
    pub ops: Vec<OpData>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for EntryData {
    fn default() -> Self {
        Self {
            entity: Vec::new(),
            ops: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl EntryData {
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let entity;
        let ops;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
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
            ops = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(OpData::read(buf, version)?);
                }
                arr
            };
        } else {
            ops = {
                let len = read_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(OpData::read(buf, version)?);
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
            entity,
            ops,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
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
            write_compact_array_length(buf, self.ops.len() as i32);
            for el in &self.ops {
                el.write(buf, version)?;
            }
        } else {
            write_array_length(buf, self.ops.len() as i32);
            for el in &self.ops {
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
}
#[derive(Debug, Clone, PartialEq)]
pub struct OpData {
    /// The quota configuration key.
    pub key: KafkaString,
    /// The value to set, otherwise ignored if the value is to be removed.
    pub value: f64,
    /// Whether the quota configuration value should be removed, otherwise set.
    pub remove: bool,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for OpData {
    fn default() -> Self {
        Self {
            key: KafkaString::default(),
            value: 0.0_f64,
            remove: false,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl OpData {
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let key;
        let value;
        let remove;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 1 {
            key = read_compact_string(buf)?;
        } else {
            key = read_string(buf)?;
        }
        value = read_f64(buf)?;
        remove = read_bool(buf)?;
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
            key,
            value,
            remove,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version >= 1 {
            write_compact_string(buf, &self.key)?;
        } else {
            write_string(buf, &self.key)?;
        }
        write_f64(buf, self.value);
        write_bool(buf, self.remove);
        if version >= 1 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
}
