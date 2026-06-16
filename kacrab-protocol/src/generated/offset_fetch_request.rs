//! Generated from OffsetFetchRequest.json - DO NOT EDIT
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
pub struct OffsetFetchRequestData {
    /// The group to fetch offsets for.
    pub group_id: KafkaString,
    /// Each topic we would like to fetch offsets for, or null to fetch offsets for all topics.
    pub topics: Option<Vec<OffsetFetchRequestTopic>>,
    /// Each group we would like to fetch offsets for.
    pub groups: Vec<OffsetFetchRequestGroup>,
    /// Whether broker should hold on returning unstable offsets but set a retriable error code for
    /// the partitions.
    pub require_stable: bool,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for OffsetFetchRequestData {
    fn default() -> Self {
        Self {
            group_id: KafkaString::default(),
            topics: None,
            groups: Vec::new(),
            require_stable: false,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl OffsetFetchRequestData {
    pub fn with_group_id(mut self, value: KafkaString) -> Self {
        self.group_id = value;
        self
    }
    pub fn with_topics(mut self, value: Option<Vec<OffsetFetchRequestTopic>>) -> Self {
        self.topics = value;
        self
    }
    pub fn with_groups(mut self, value: Vec<OffsetFetchRequestGroup>) -> Self {
        self.groups = value;
        self
    }
    pub fn with_require_stable(mut self, value: bool) -> Self {
        self.require_stable = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 1 || version > 10 {
            return Err(UnsupportedVersion::new(9, version).into());
        }
        let mut group_id = KafkaString::default();
        let mut topics = None;
        let mut groups = Vec::new();
        let mut require_stable = false;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version <= 7 {
            if version >= 6 {
                group_id = read_compact_string(buf)?;
            } else {
                group_id = read_string(buf)?;
            }
        }
        if version <= 7 {
            if version >= 2 {
                if version >= 6 {
                    topics = {
                        let len = read_compact_array_length(buf)?;
                        if len < 0 {
                            None
                        } else {
                            let mut arr = Vec::with_capacity(len as usize);
                            for _ in 0..len {
                                arr.push(OffsetFetchRequestTopic::read(buf, version)?);
                            }
                            Some(arr)
                        }
                    };
                } else {
                    topics = {
                        let len = read_array_length(buf)?;
                        if len < 0 {
                            None
                        } else {
                            let mut arr = Vec::with_capacity(len as usize);
                            for _ in 0..len {
                                arr.push(OffsetFetchRequestTopic::read(buf, version)?);
                            }
                            Some(arr)
                        }
                    };
                }
            } else {
                topics = Some({
                    let len = read_array_length(buf)?;
                    let mut arr = Vec::with_capacity(len.max(0) as usize);
                    for _ in 0..len {
                        arr.push(OffsetFetchRequestTopic::read(buf, version)?);
                    }
                    arr
                });
            }
        }
        if version >= 8 {
            groups = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(OffsetFetchRequestGroup::read(buf, version)?);
                }
                arr
            };
        }
        if version >= 7 {
            require_stable = read_bool(buf)?;
        }
        if version >= 6 {
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
            group_id,
            topics,
            groups,
            require_stable,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 1 || version > 10 {
            return Err(UnsupportedVersion::new(9, version).into());
        }
        if version <= 7 {
            if version >= 6 {
                write_compact_string(buf, &self.group_id)?;
            } else {
                write_string(buf, &self.group_id)?;
            }
        } else if self.group_id != KafkaString::default() {
            return Err(UnsupportedFieldVersion::new(9, "group_id", version).into());
        }
        if version <= 7 {
            if version >= 2 {
                if version >= 6 {
                    match &self.topics {
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
                    match &self.topics {
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
            } else {
                match &self.topics {
                    Some(arr) => {
                        write_array_length(buf, arr.len() as i32);
                        for el in arr {
                            el.write(buf, version)?;
                        }
                    },
                    None => {
                        write_array_length(buf, 0);
                    },
                }
            }
        } else if self.topics != None {
            return Err(UnsupportedFieldVersion::new(9, "topics", version).into());
        }
        if version >= 8 {
            write_compact_array_length(buf, self.groups.len() as i32);
            for el in &self.groups {
                el.write(buf, version)?;
            }
        } else if self.groups != Vec::new() {
            return Err(UnsupportedFieldVersion::new(9, "groups", version).into());
        }
        if version >= 7 {
            write_bool(buf, self.require_stable);
        } else if self.require_stable != false {
            return Err(UnsupportedFieldVersion::new(9, "require_stable", version).into());
        }
        if version >= 6 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        if version < 1 || version > 10 {
            return Err(UnsupportedVersion::new(9, version).into());
        }
        let mut len: usize = 0;
        if version <= 7 {
            if version >= 6 {
                len += compact_string_len(&self.group_id)?;
            } else {
                len += string_len(&self.group_id)?;
            }
        } else if self.group_id != KafkaString::default() {
            return Err(UnsupportedFieldVersion::new(9, "group_id", version).into());
        }
        if version <= 7 {
            if version >= 2 {
                if version >= 6 {
                    match &self.topics {
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
                    match &self.topics {
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
            } else {
                match &self.topics {
                    Some(arr) => {
                        len += array_length_len();
                        for el in arr {
                            len += el.encoded_len(version)?;
                        }
                    },
                    None => {
                        len += array_length_len();
                    },
                }
            }
        } else if self.topics != None {
            return Err(UnsupportedFieldVersion::new(9, "topics", version).into());
        }
        if version >= 8 {
            len += compact_array_length_len(self.groups.len() as i32);
            for el in &self.groups {
                len += el.encoded_len(version)?;
            }
        } else if self.groups != Vec::new() {
            return Err(UnsupportedFieldVersion::new(9, "groups", version).into());
        }
        if version >= 7 {
            len += 1;
        } else if self.require_stable != false {
            return Err(UnsupportedFieldVersion::new(9, "require_stable", version).into());
        }
        if version >= 6 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            len += tagged_fields_len(&all_tags)?;
        }
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct OffsetFetchRequestTopic {
    /// The topic name.
    pub name: KafkaString,
    /// The partition indexes we would like to fetch offsets for.
    pub partition_indexes: Vec<i32>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for OffsetFetchRequestTopic {
    fn default() -> Self {
        Self {
            name: KafkaString::default(),
            partition_indexes: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl OffsetFetchRequestTopic {
    pub fn with_name(mut self, value: KafkaString) -> Self {
        self.name = value;
        self
    }
    pub fn with_partition_indexes(mut self, value: Vec<i32>) -> Self {
        self.partition_indexes = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let name;
        let partition_indexes;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 6 {
            name = read_compact_string(buf)?;
        } else {
            name = read_string(buf)?;
        }
        if version >= 6 {
            partition_indexes = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(read_i32(buf)?);
                }
                arr
            };
        } else {
            partition_indexes = {
                let len = read_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(read_i32(buf)?);
                }
                arr
            };
        }
        if version >= 6 {
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
            partition_indexes,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version >= 6 {
            write_compact_string(buf, &self.name)?;
        } else {
            write_string(buf, &self.name)?;
        }
        if version >= 6 {
            write_compact_array_length(buf, self.partition_indexes.len() as i32);
            for el in &self.partition_indexes {
                write_i32(buf, *el);
            }
        } else {
            write_array_length(buf, self.partition_indexes.len() as i32);
            for el in &self.partition_indexes {
                write_i32(buf, *el);
            }
        }
        if version >= 6 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        let mut len: usize = 0;
        if version >= 6 {
            len += compact_string_len(&self.name)?;
        } else {
            len += string_len(&self.name)?;
        }
        if version >= 6 {
            len += compact_array_length_len(self.partition_indexes.len() as i32);
            len += self.partition_indexes.len() * 4usize;
        } else {
            len += array_length_len();
            len += self.partition_indexes.len() * 4usize;
        }
        if version >= 6 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            len += tagged_fields_len(&all_tags)?;
        }
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct OffsetFetchRequestGroup {
    /// The group ID.
    pub group_id: KafkaString,
    /// The member id.
    pub member_id: Option<KafkaString>,
    /// The member epoch if using the new consumer protocol (KIP-848).
    pub member_epoch: i32,
    /// Each topic we would like to fetch offsets for, or null to fetch offsets for all topics.
    pub topics: Option<Vec<OffsetFetchRequestTopics>>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for OffsetFetchRequestGroup {
    fn default() -> Self {
        Self {
            group_id: KafkaString::default(),
            member_id: None,
            member_epoch: -1i32,
            topics: None,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl OffsetFetchRequestGroup {
    pub fn with_group_id(mut self, value: KafkaString) -> Self {
        self.group_id = value;
        self
    }
    pub fn with_member_id(mut self, value: Option<KafkaString>) -> Self {
        self.member_id = value;
        self
    }
    pub fn with_member_epoch(mut self, value: i32) -> Self {
        self.member_epoch = value;
        self
    }
    pub fn with_topics(mut self, value: Option<Vec<OffsetFetchRequestTopics>>) -> Self {
        self.topics = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let group_id;
        let mut member_id = None;
        let mut member_epoch = -1i32;
        let topics;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        group_id = read_compact_string(buf)?;
        if version >= 9 {
            member_id = read_compact_nullable_string(buf)?;
        }
        if version >= 9 {
            member_epoch = read_i32(buf)?;
        }
        topics = {
            let len = read_compact_array_length(buf)?;
            if len < 0 {
                None
            } else {
                let mut arr = Vec::with_capacity(len as usize);
                for _ in 0..len {
                    arr.push(OffsetFetchRequestTopics::read(buf, version)?);
                }
                Some(arr)
            }
        };
        let tagged_fields = read_tagged_fields(buf)?;
        for field in &tagged_fields {
            match field.tag {
                _ => {
                    _unknown_tagged_fields.push(field.clone());
                },
            }
        }
        Ok(Self {
            group_id,
            member_id,
            member_epoch,
            topics,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        write_compact_string(buf, &self.group_id)?;
        if version >= 9 {
            write_compact_nullable_string(buf, self.member_id.as_ref())?;
        } else if self.member_id != None {
            return Err(UnsupportedFieldVersion::new(9, "member_id", version).into());
        }
        if version >= 9 {
            write_i32(buf, self.member_epoch);
        } else if self.member_epoch != -1i32 {
            return Err(UnsupportedFieldVersion::new(9, "member_epoch", version).into());
        }
        match &self.topics {
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
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        let mut len: usize = 0;
        len += compact_string_len(&self.group_id)?;
        if version >= 9 {
            len += compact_nullable_string_len(self.member_id.as_ref())?;
        } else if self.member_id != None {
            return Err(UnsupportedFieldVersion::new(9, "member_id", version).into());
        }
        if version >= 9 {
            len += 4;
        } else if self.member_epoch != -1i32 {
            return Err(UnsupportedFieldVersion::new(9, "member_epoch", version).into());
        }
        match &self.topics {
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
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct OffsetFetchRequestTopics {
    /// The topic name.
    pub name: KafkaString,
    /// The topic ID.
    pub topic_id: KafkaUuid,
    /// The partition indexes we would like to fetch offsets for.
    pub partition_indexes: Vec<i32>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for OffsetFetchRequestTopics {
    fn default() -> Self {
        Self {
            name: KafkaString::default(),
            topic_id: KafkaUuid::ZERO,
            partition_indexes: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl OffsetFetchRequestTopics {
    pub fn with_name(mut self, value: KafkaString) -> Self {
        self.name = value;
        self
    }
    pub fn with_topic_id(mut self, value: KafkaUuid) -> Self {
        self.topic_id = value;
        self
    }
    pub fn with_partition_indexes(mut self, value: Vec<i32>) -> Self {
        self.partition_indexes = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let mut name = KafkaString::default();
        let mut topic_id = KafkaUuid::ZERO;
        let partition_indexes;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version <= 9 {
            name = read_compact_string(buf)?;
        }
        if version >= 10 {
            topic_id = read_uuid(buf)?;
        }
        partition_indexes = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(len.max(0) as usize);
            for _ in 0..len {
                arr.push(read_i32(buf)?);
            }
            arr
        };
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
            partition_indexes,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version <= 9 {
            write_compact_string(buf, &self.name)?;
        } else if self.name != KafkaString::default() {
            return Err(UnsupportedFieldVersion::new(9, "name", version).into());
        }
        if version >= 10 {
            write_uuid(buf, &self.topic_id);
        } else if self.topic_id != KafkaUuid::ZERO {
            return Err(UnsupportedFieldVersion::new(9, "topic_id", version).into());
        }
        write_compact_array_length(buf, self.partition_indexes.len() as i32);
        for el in &self.partition_indexes {
            write_i32(buf, *el);
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        let mut len: usize = 0;
        if version <= 9 {
            len += compact_string_len(&self.name)?;
        } else if self.name != KafkaString::default() {
            return Err(UnsupportedFieldVersion::new(9, "name", version).into());
        }
        if version >= 10 {
            len += 16;
        } else if self.topic_id != KafkaUuid::ZERO {
            return Err(UnsupportedFieldVersion::new(9, "topic_id", version).into());
        }
        len += compact_array_length_len(self.partition_indexes.len() as i32);
        len += self.partition_indexes.len() * 4usize;
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
