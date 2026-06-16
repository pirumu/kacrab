//! Generated from CreateTopicsRequest.json - DO NOT EDIT
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
pub struct CreateTopicsRequestData {
    /// The topics to create.
    pub topics: Vec<CreatableTopic>,
    /// How long to wait in milliseconds before timing out the request.
    pub timeout_ms: i32,
    /// If true, check that the topics can be created as specified, but don't create anything.
    pub validate_only: bool,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for CreateTopicsRequestData {
    fn default() -> Self {
        Self {
            topics: Vec::new(),
            timeout_ms: 60000i32,
            validate_only: false,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl CreateTopicsRequestData {
    pub fn with_topics(mut self, value: Vec<CreatableTopic>) -> Self {
        self.topics = value;
        self
    }
    pub fn with_timeout_ms(mut self, value: i32) -> Self {
        self.timeout_ms = value;
        self
    }
    pub fn with_validate_only(mut self, value: bool) -> Self {
        self.validate_only = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 2 || version > 7 {
            return Err(UnsupportedVersion::new(19, version).into());
        }
        let topics;
        let timeout_ms;
        let validate_only;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 5 {
            topics = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(CreatableTopic::read(buf, version)?);
                }
                arr
            };
        } else {
            topics = {
                let len = read_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(CreatableTopic::read(buf, version)?);
                }
                arr
            };
        }
        timeout_ms = read_i32(buf)?;
        validate_only = read_bool(buf)?;
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
            topics,
            timeout_ms,
            validate_only,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 2 || version > 7 {
            return Err(UnsupportedVersion::new(19, version).into());
        }
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
        write_i32(buf, self.timeout_ms);
        write_bool(buf, self.validate_only);
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
        len += 4;
        len += 1;
        if version >= 5 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            len += tagged_fields_len(&all_tags)?;
        }
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct CreatableTopic {
    /// The topic name.
    pub name: KafkaString,
    /// The number of partitions to create in the topic, or -1 if we are either specifying a manual
    /// partition assignment or using the default partitions.
    pub num_partitions: i32,
    /// The number of replicas to create for each partition in the topic, or -1 if we are either
    /// specifying a manual partition assignment or using the default replication factor.
    pub replication_factor: i16,
    /// The manual partition assignment, or the empty array if we are using automatic assignment.
    pub assignments: Vec<CreatableReplicaAssignment>,
    /// The custom topic configurations to set.
    pub configs: Vec<CreatableTopicConfig>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for CreatableTopic {
    fn default() -> Self {
        Self {
            name: KafkaString::default(),
            num_partitions: 0_i32,
            replication_factor: 0_i16,
            assignments: Vec::new(),
            configs: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl CreatableTopic {
    pub fn with_name(mut self, value: KafkaString) -> Self {
        self.name = value;
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
    pub fn with_assignments(mut self, value: Vec<CreatableReplicaAssignment>) -> Self {
        self.assignments = value;
        self
    }
    pub fn with_configs(mut self, value: Vec<CreatableTopicConfig>) -> Self {
        self.configs = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let name;
        let num_partitions;
        let replication_factor;
        let assignments;
        let configs;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 5 {
            name = read_compact_string(buf)?;
        } else {
            name = read_string(buf)?;
        }
        num_partitions = read_i32(buf)?;
        replication_factor = read_i16(buf)?;
        if version >= 5 {
            assignments = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(CreatableReplicaAssignment::read(buf, version)?);
                }
                arr
            };
        } else {
            assignments = {
                let len = read_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(CreatableReplicaAssignment::read(buf, version)?);
                }
                arr
            };
        }
        if version >= 5 {
            configs = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(CreatableTopicConfig::read(buf, version)?);
                }
                arr
            };
        } else {
            configs = {
                let len = read_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(CreatableTopicConfig::read(buf, version)?);
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
            name,
            num_partitions,
            replication_factor,
            assignments,
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
        write_i32(buf, self.num_partitions);
        write_i16(buf, self.replication_factor);
        if version >= 5 {
            write_compact_array_length(buf, self.assignments.len() as i32);
            for el in &self.assignments {
                el.write(buf, version)?;
            }
        } else {
            write_array_length(buf, self.assignments.len() as i32);
            for el in &self.assignments {
                el.write(buf, version)?;
            }
        }
        if version >= 5 {
            write_compact_array_length(buf, self.configs.len() as i32);
            for el in &self.configs {
                el.write(buf, version)?;
            }
        } else {
            write_array_length(buf, self.configs.len() as i32);
            for el in &self.configs {
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
        let mut len: usize = 0;
        if version >= 5 {
            len += compact_string_len(&self.name)?;
        } else {
            len += string_len(&self.name)?;
        }
        len += 4;
        len += 2;
        if version >= 5 {
            len += compact_array_length_len(self.assignments.len() as i32);
            for el in &self.assignments {
                len += el.encoded_len(version)?;
            }
        } else {
            len += array_length_len();
            for el in &self.assignments {
                len += el.encoded_len(version)?;
            }
        }
        if version >= 5 {
            len += compact_array_length_len(self.configs.len() as i32);
            for el in &self.configs {
                len += el.encoded_len(version)?;
            }
        } else {
            len += array_length_len();
            for el in &self.configs {
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
pub struct CreatableReplicaAssignment {
    /// The partition index.
    pub partition_index: i32,
    /// The brokers to place the partition on.
    pub broker_ids: Vec<i32>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for CreatableReplicaAssignment {
    fn default() -> Self {
        Self {
            partition_index: 0_i32,
            broker_ids: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl CreatableReplicaAssignment {
    pub fn with_partition_index(mut self, value: i32) -> Self {
        self.partition_index = value;
        self
    }
    pub fn with_broker_ids(mut self, value: Vec<i32>) -> Self {
        self.broker_ids = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let partition_index;
        let broker_ids;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        partition_index = read_i32(buf)?;
        if version >= 5 {
            broker_ids = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(read_i32(buf)?);
                }
                arr
            };
        } else {
            broker_ids = {
                let len = read_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(read_i32(buf)?);
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
            partition_index,
            broker_ids,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        write_i32(buf, self.partition_index);
        if version >= 5 {
            write_compact_array_length(buf, self.broker_ids.len() as i32);
            for el in &self.broker_ids {
                write_i32(buf, *el);
            }
        } else {
            write_array_length(buf, self.broker_ids.len() as i32);
            for el in &self.broker_ids {
                write_i32(buf, *el);
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
        let mut len: usize = 0;
        len += 4;
        if version >= 5 {
            len += compact_array_length_len(self.broker_ids.len() as i32);
            len += self.broker_ids.len() * 4usize;
        } else {
            len += array_length_len();
            len += self.broker_ids.len() * 4usize;
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
pub struct CreatableTopicConfig {
    /// The configuration name.
    pub name: KafkaString,
    /// The configuration value.
    pub value: Option<KafkaString>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for CreatableTopicConfig {
    fn default() -> Self {
        Self {
            name: KafkaString::default(),
            value: None,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl CreatableTopicConfig {
    pub fn with_name(mut self, value: KafkaString) -> Self {
        self.name = value;
        self
    }
    pub fn with_value(mut self, value: Option<KafkaString>) -> Self {
        self.value = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let name;
        let value;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 5 {
            name = read_compact_string(buf)?;
        } else {
            name = read_string(buf)?;
        }
        if version >= 5 {
            value = read_compact_nullable_string(buf)?;
        } else {
            value = read_nullable_string(buf)?;
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
            name,
            value,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version >= 5 {
            write_compact_string(buf, &self.name)?;
        } else {
            write_string(buf, &self.name)?;
        }
        if version >= 5 {
            write_compact_nullable_string(buf, self.value.as_ref())?;
        } else {
            write_nullable_string(buf, self.value.as_ref())?;
        }
        if version >= 5 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
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
        if version >= 5 {
            len += compact_nullable_string_len(self.value.as_ref())?;
        } else {
            len += nullable_string_len(self.value.as_ref())?;
        }
        if version >= 5 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            len += tagged_fields_len(&all_tags)?;
        }
        Ok(len)
    }
}
