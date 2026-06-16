//! Generated from CreateTopicsResponse.json - DO NOT EDIT
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
pub struct CreateTopicsResponseData {
    /// The duration in milliseconds for which the request was throttled due to a quota violation,
    /// or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// Results for each topic we tried to create.
    pub topics: Vec<CreatableTopicResult>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for CreateTopicsResponseData {
    fn default() -> Self {
        Self {
            throttle_time_ms: 0_i32,
            topics: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl CreateTopicsResponseData {
    pub fn with_throttle_time_ms(mut self, value: i32) -> Self {
        self.throttle_time_ms = value;
        self
    }
    pub fn with_topics(mut self, value: Vec<CreatableTopicResult>) -> Self {
        self.topics = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 2 || version > 7 {
            return Err(UnsupportedVersion::new(19, version).into());
        }
        let throttle_time_ms;
        let topics;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        throttle_time_ms = read_i32(buf)?;
        if version >= 5 {
            topics = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(CreatableTopicResult::read(buf, version)?);
                }
                arr
            };
        } else {
            topics = {
                let len = read_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(CreatableTopicResult::read(buf, version)?);
                }
                arr
            };
        }
        if version >= 5 {
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
            topics,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 2 || version > 7 {
            return Err(UnsupportedVersion::new(19, version).into());
        }
        write_i32(buf, self.throttle_time_ms);
        if version >= 5 {
            write_compact_array_length(buf, self.topics.len() as i32);
            for el in &self.topics {
                el.write(buf, version)?;
            }
        } else {
            write_array_length(buf, self.topics.len() as i32);
            for el in &self.topics {
                el.write(buf, version)?;
            }
        }
        if version >= 5 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        if version < 2 || version > 7 {
            return Err(UnsupportedVersion::new(19, version).into());
        }
        let mut len: usize = 0;
        len += 4;
        if version >= 5 {
            len += compact_array_length_len(self.topics.len() as i32);
            for el in &self.topics {
                len += el.encoded_len(version)?;
            }
        } else {
            len += array_length_len();
            for el in &self.topics {
                len += el.encoded_len(version)?;
            }
        }
        if version >= 5 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            len += tagged_fields_len(&all_tags)?;
        }
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct CreatableTopicResult {
    /// The topic name.
    pub name: KafkaString,
    /// The unique topic ID.
    pub topic_id: KafkaUuid,
    /// The error code, or 0 if there was no error.
    pub error_code: i16,
    /// The error message, or null if there was no error.
    pub error_message: Option<KafkaString>,
    /// Optional topic config error returned if configs are not returned in the response.
    pub topic_config_error_code: i16,
    /// Number of partitions of the topic.
    pub num_partitions: i32,
    /// Replication factor of the topic.
    pub replication_factor: i16,
    /// Configuration of the topic.
    pub configs: Option<Vec<CreatableTopicConfigs>>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for CreatableTopicResult {
    fn default() -> Self {
        Self {
            name: KafkaString::default(),
            topic_id: KafkaUuid::ZERO,
            error_code: 0_i16,
            error_message: None,
            topic_config_error_code: 0_i16,
            num_partitions: -1i32,
            replication_factor: -1i16,
            configs: None,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl CreatableTopicResult {
    pub fn with_name(mut self, value: KafkaString) -> Self {
        self.name = value;
        self
    }
    pub fn with_topic_id(mut self, value: KafkaUuid) -> Self {
        self.topic_id = value;
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
    pub fn with_topic_config_error_code(mut self, value: i16) -> Self {
        self.topic_config_error_code = value;
        self
    }
    pub fn with_num_partitions(mut self, value: i32) -> Self {
        self.num_partitions = value;
        self
    }
    pub fn with_replication_factor(mut self, value: i16) -> Self {
        self.replication_factor = value;
        self
    }
    pub fn with_configs(mut self, value: Option<Vec<CreatableTopicConfigs>>) -> Self {
        self.configs = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let name;
        let mut topic_id = KafkaUuid::ZERO;
        let error_code;
        let error_message;
        let mut topic_config_error_code = 0_i16;
        let mut num_partitions = -1i32;
        let mut replication_factor = -1i16;
        let mut configs = None;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 5 {
            name = read_compact_string(buf)?;
        } else {
            name = read_string(buf)?;
        }
        if version >= 7 {
            topic_id = read_uuid(buf)?;
        }
        error_code = read_i16(buf)?;
        if version >= 5 {
            error_message = read_compact_nullable_string(buf)?;
        } else {
            error_message = read_nullable_string(buf)?;
        }
        if version >= 5 {
            num_partitions = read_i32(buf)?;
        }
        if version >= 5 {
            replication_factor = read_i16(buf)?;
        }
        if version >= 5 {
            configs = {
                let len = read_compact_array_length(buf)?;
                if len < 0 {
                    None
                } else {
                    let mut arr = Vec::with_capacity(len as usize);
                    for _ in 0..len {
                        arr.push(CreatableTopicConfigs::read(buf, version)?);
                    }
                    Some(arr)
                }
            };
        }
        if version >= 5 {
            let tagged_fields = read_tagged_fields(buf)?;
            for field in &tagged_fields {
                match field.tag {
                    0 => {
                        let mut tag_buf = field.data.clone();
                        topic_config_error_code = read_i16(&mut tag_buf)?;
                    },
                    _ => {
                        _unknown_tagged_fields.push(field.clone());
                    },
                }
            }
        }
        Ok(Self {
            name,
            topic_id,
            error_code,
            error_message,
            topic_config_error_code,
            num_partitions,
            replication_factor,
            configs,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version >= 5 {
            write_compact_string(buf, &self.name)?;
        } else {
            write_string(buf, &self.name)?;
        }
        if version >= 7 {
            write_uuid(buf, &self.topic_id);
        } else if self.topic_id != KafkaUuid::ZERO {
            return Err(UnsupportedFieldVersion::new(19, "topic_id", version).into());
        }
        write_i16(buf, self.error_code);
        if version >= 5 {
            write_compact_nullable_string(buf, self.error_message.as_ref())?;
        } else {
            write_nullable_string(buf, self.error_message.as_ref())?;
        }
        if version >= 5 {
            write_i32(buf, self.num_partitions);
        } else if self.num_partitions != -1i32 {
            return Err(UnsupportedFieldVersion::new(19, "num_partitions", version).into());
        }
        if version >= 5 {
            write_i16(buf, self.replication_factor);
        } else if self.replication_factor != -1i16 {
            return Err(UnsupportedFieldVersion::new(19, "replication_factor", version).into());
        }
        if version >= 5 {
            match &self.configs {
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
        } else if self.configs != None {
            return Err(UnsupportedFieldVersion::new(19, "configs", version).into());
        }
        if version >= 5 {
            let mut known_tagged_fields: Vec<RawTaggedField> = Vec::new();
            if self.topic_config_error_code != 0_i16 {
                let mut tag_buf = BytesMut::new();
                write_i16(&mut tag_buf, self.topic_config_error_code);
                known_tagged_fields.push(RawTaggedField {
                    tag: 0,
                    data: tag_buf.freeze(),
                });
            }
            let mut all_tags = known_tagged_fields;
            all_tags.extend(self._unknown_tagged_fields.iter().cloned());
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        let mut len: usize = 0;
        if version >= 5 {
            len += compact_string_len(&self.name)?;
        } else {
            len += string_len(&self.name)?;
        }
        if version >= 7 {
            len += 16;
        } else if self.topic_id != KafkaUuid::ZERO {
            return Err(UnsupportedFieldVersion::new(19, "topic_id", version).into());
        }
        len += 2;
        if version >= 5 {
            len += compact_nullable_string_len(self.error_message.as_ref())?;
        } else {
            len += nullable_string_len(self.error_message.as_ref())?;
        }
        if version >= 5 {
            len += 4;
        } else if self.num_partitions != -1i32 {
            return Err(UnsupportedFieldVersion::new(19, "num_partitions", version).into());
        }
        if version >= 5 {
            len += 2;
        } else if self.replication_factor != -1i16 {
            return Err(UnsupportedFieldVersion::new(19, "replication_factor", version).into());
        }
        if version >= 5 {
            match &self.configs {
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
        } else if self.configs != None {
            return Err(UnsupportedFieldVersion::new(19, "configs", version).into());
        }
        if version >= 5 {
            let mut known_tagged_fields: Vec<RawTaggedField> = Vec::new();
            if self.topic_config_error_code != 0_i16 {
                let mut tag_buf = BytesMut::new();
                write_i16(&mut tag_buf, self.topic_config_error_code);
                known_tagged_fields.push(RawTaggedField {
                    tag: 0,
                    data: tag_buf.freeze(),
                });
            }
            let mut all_tags = known_tagged_fields;
            all_tags.extend(self._unknown_tagged_fields.iter().cloned());
            all_tags.sort_by_key(|f| f.tag);
            len += tagged_fields_len(&all_tags)?;
        }
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct CreatableTopicConfigs {
    /// The configuration name.
    pub name: KafkaString,
    /// The configuration value.
    pub value: Option<KafkaString>,
    /// True if the configuration is read-only.
    pub read_only: bool,
    /// The configuration source.
    pub config_source: i8,
    /// True if this configuration is sensitive.
    pub is_sensitive: bool,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for CreatableTopicConfigs {
    fn default() -> Self {
        Self {
            name: KafkaString::default(),
            value: None,
            read_only: false,
            config_source: -1i8,
            is_sensitive: false,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl CreatableTopicConfigs {
    pub fn with_name(mut self, value: KafkaString) -> Self {
        self.name = value;
        self
    }
    pub fn with_value(mut self, value: Option<KafkaString>) -> Self {
        self.value = value;
        self
    }
    pub fn with_read_only(mut self, value: bool) -> Self {
        self.read_only = value;
        self
    }
    pub fn with_config_source(mut self, value: i8) -> Self {
        self.config_source = value;
        self
    }
    pub fn with_is_sensitive(mut self, value: bool) -> Self {
        self.is_sensitive = value;
        self
    }
    pub fn read(buf: &mut Bytes, _version: i16) -> Result<Self> {
        let name;
        let value;
        let read_only;
        let config_source;
        let is_sensitive;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        name = read_compact_string(buf)?;
        value = read_compact_nullable_string(buf)?;
        read_only = read_bool(buf)?;
        config_source = read_i8(buf)?;
        is_sensitive = read_bool(buf)?;
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
            value,
            read_only,
            config_source,
            is_sensitive,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, _version: i16) -> Result<()> {
        write_compact_string(buf, &self.name)?;
        write_compact_nullable_string(buf, self.value.as_ref())?;
        write_bool(buf, self.read_only);
        write_i8(buf, self.config_source);
        write_bool(buf, self.is_sensitive);
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
    pub fn encoded_len(&self, _version: i16) -> Result<usize> {
        let mut len: usize = 0;
        len += compact_string_len(&self.name)?;
        len += compact_nullable_string_len(self.value.as_ref())?;
        len += 1;
        len += 1;
        len += 1;
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
