//! Generated from DeleteTopicsRequest.json - DO NOT EDIT
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
pub struct DeleteTopicsRequestData {
    /// The name or topic ID of the topic.
    pub topics: Vec<DeleteTopicState>,
    /// The names of the topics to delete.
    pub topic_names: Vec<KafkaString>,
    /// The length of time in milliseconds to wait for the deletions to complete.
    pub timeout_ms: i32,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for DeleteTopicsRequestData {
    fn default() -> Self {
        Self {
            topics: Vec::new(),
            topic_names: Vec::new(),
            timeout_ms: 0_i32,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl DeleteTopicsRequestData {
    pub fn with_topics(mut self, value: Vec<DeleteTopicState>) -> Self {
        self.topics = value;
        self
    }
    pub fn with_topic_names(mut self, value: Vec<KafkaString>) -> Self {
        self.topic_names = value;
        self
    }
    pub fn with_timeout_ms(mut self, value: i32) -> Self {
        self.timeout_ms = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 1 || version > 6 {
            return Err(UnsupportedVersion::new(20, version).into());
        }
        let mut topics = Vec::new();
        let mut topic_names = Vec::new();
        let timeout_ms;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 6 {
            topics = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(DeleteTopicState::read(buf, version)?);
                }
                arr
            };
        }
        if version <= 5 {
            if version >= 4 {
                topic_names = {
                    let len = read_compact_array_length(buf)?;
                    let mut arr = Vec::with_capacity(len.max(0) as usize);
                    for _ in 0..len {
                        arr.push(read_compact_string(buf)?);
                    }
                    arr
                };
            } else {
                topic_names = {
                    let len = read_array_length(buf)?;
                    let mut arr = Vec::with_capacity(len.max(0) as usize);
                    for _ in 0..len {
                        arr.push(read_string(buf)?);
                    }
                    arr
                };
            }
        }
        timeout_ms = read_i32(buf)?;
        if version >= 4 {
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
            topics,
            topic_names,
            timeout_ms,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 1 || version > 6 {
            return Err(UnsupportedVersion::new(20, version).into());
        }
        if version >= 6 {
            write_compact_array_length(buf, self.topics.len() as i32);
            for el in &self.topics {
                el.write(buf, version)?;
            }
        } else if self.topics != Vec::new() {
            return Err(UnsupportedFieldVersion::new(20, "topics", version).into());
        }
        if version <= 5 {
            if version >= 4 {
                write_compact_array_length(buf, self.topic_names.len() as i32);
                for el in &self.topic_names {
                    write_compact_string(buf, el)?;
                }
            } else {
                write_array_length(buf, self.topic_names.len() as i32);
                for el in &self.topic_names {
                    write_string(buf, el)?;
                }
            }
        } else if self.topic_names != Vec::new() {
            return Err(UnsupportedFieldVersion::new(20, "topic_names", version).into());
        }
        write_i32(buf, self.timeout_ms);
        if version >= 4 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        if version < 1 || version > 6 {
            return Err(UnsupportedVersion::new(20, version).into());
        }
        let mut len: usize = 0;
        if version >= 6 {
            len += compact_array_length_len(self.topics.len() as i32);
            for el in &self.topics {
                len += el.encoded_len(version)?;
            }
        } else if self.topics != Vec::new() {
            return Err(UnsupportedFieldVersion::new(20, "topics", version).into());
        }
        if version <= 5 {
            if version >= 4 {
                len += compact_array_length_len(self.topic_names.len() as i32);
                for el in &self.topic_names {
                    len += compact_string_len(el)?;
                }
            } else {
                len += array_length_len();
                for el in &self.topic_names {
                    len += string_len(el)?;
                }
            }
        } else if self.topic_names != Vec::new() {
            return Err(UnsupportedFieldVersion::new(20, "topic_names", version).into());
        }
        len += 4;
        if version >= 4 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            len += tagged_fields_len(&all_tags)?;
        }
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct DeleteTopicState {
    /// The topic name.
    pub name: Option<KafkaString>,
    /// The unique topic ID.
    pub topic_id: KafkaUuid,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for DeleteTopicState {
    fn default() -> Self {
        Self {
            name: None,
            topic_id: KafkaUuid::ZERO,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl DeleteTopicState {
    pub fn with_name(mut self, value: Option<KafkaString>) -> Self {
        self.name = value;
        self
    }
    pub fn with_topic_id(mut self, value: KafkaUuid) -> Self {
        self.topic_id = value;
        self
    }
    pub fn read(buf: &mut Bytes, _version: i16) -> Result<Self> {
        let name;
        let topic_id;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        name = read_compact_nullable_string(buf)?;
        topic_id = read_uuid(buf)?;
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
            topic_id,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, _version: i16) -> Result<()> {
        write_compact_nullable_string(buf, self.name.as_ref())?;
        write_uuid(buf, &self.topic_id);
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
    pub fn encoded_len(&self, _version: i16) -> Result<usize> {
        let mut len: usize = 0;
        len += compact_nullable_string_len(self.name.as_ref())?;
        len += 16;
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
