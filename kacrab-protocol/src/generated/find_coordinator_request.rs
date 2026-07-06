//! Generated from FindCoordinatorRequest.json - DO NOT EDIT
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
pub struct FindCoordinatorRequestData {
    /// The coordinator key.
    pub key: KafkaString,
    /// The coordinator key type. (group, transaction, share).
    pub key_type: i8,
    /// The coordinator keys.
    pub coordinator_keys: Vec<KafkaString>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for FindCoordinatorRequestData {
    fn default() -> Self {
        Self {
            key: KafkaString::default(),
            key_type: 0i8,
            coordinator_keys: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl FindCoordinatorRequestData {
    pub fn with_key(mut self, value: KafkaString) -> Self {
        self.key = value;
        self
    }
    pub fn with_key_type(mut self, value: i8) -> Self {
        self.key_type = value;
        self
    }
    pub fn with_coordinator_keys(mut self, value: Vec<KafkaString>) -> Self {
        self.coordinator_keys = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 0 || version > 6 {
            return Err(UnsupportedVersion::new(10, version).into());
        }
        let mut key = KafkaString::default();
        let mut key_type = 0i8;
        let mut coordinator_keys = Vec::new();
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version <= 3 {
            if version >= 3 {
                key = read_compact_string(buf)?;
            } else {
                key = read_string(buf)?;
            }
        }
        if version >= 1 {
            key_type = read_i8(buf)?;
        }
        if version >= 4 {
            coordinator_keys = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
                for _ in 0..len {
                    arr.push(read_compact_string(buf)?);
                }
                arr
            };
        }
        if version >= 3 {
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
            key_type,
            coordinator_keys,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 0 || version > 6 {
            return Err(UnsupportedVersion::new(10, version).into());
        }
        if version <= 3 {
            if version >= 3 {
                write_compact_string(buf, &self.key)?;
            } else {
                write_string(buf, &self.key)?;
            }
        } else if self.key != KafkaString::default() {
            return Err(UnsupportedFieldVersion::new(10, "key", version).into());
        }
        if version >= 1 {
            write_i8(buf, self.key_type);
        } else if self.key_type != 0i8 {
            return Err(UnsupportedFieldVersion::new(10, "key_type", version).into());
        }
        if version >= 4 {
            write_compact_array_length(buf, self.coordinator_keys.len() as i32);
            for el in &self.coordinator_keys {
                write_compact_string(buf, el)?;
            }
        } else if self.coordinator_keys != Vec::new() {
            return Err(UnsupportedFieldVersion::new(10, "coordinator_keys", version).into());
        }
        if version >= 3 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        if version < 0 || version > 6 {
            return Err(UnsupportedVersion::new(10, version).into());
        }
        let mut len: usize = 0;
        if version <= 3 {
            if version >= 3 {
                len += compact_string_len(&self.key)?;
            } else {
                len += string_len(&self.key)?;
            }
        } else if self.key != KafkaString::default() {
            return Err(UnsupportedFieldVersion::new(10, "key", version).into());
        }
        if version >= 1 {
            len += 1;
        } else if self.key_type != 0i8 {
            return Err(UnsupportedFieldVersion::new(10, "key_type", version).into());
        }
        if version >= 4 {
            len += compact_array_length_len(self.coordinator_keys.len() as i32);
            for el in &self.coordinator_keys {
                len += compact_string_len(el)?;
            }
        } else if self.coordinator_keys != Vec::new() {
            return Err(UnsupportedFieldVersion::new(10, "coordinator_keys", version).into());
        }
        if version >= 3 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            len += tagged_fields_len(&all_tags)?;
        }
        Ok(len)
    }
}
