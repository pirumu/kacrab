//! Generated from OffsetFetchRequest.json - DO NOT EDIT
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
        }
        if version >= 8 {
            write_compact_array_length(buf, self.groups.len() as i32);
            for el in &self.groups {
                el.write(buf, version)?;
            }
        }
        if version >= 7 {
            write_bool(buf, self.require_stable);
        }
        if version >= 6 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
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
        }
        if version >= 9 {
            write_i32(buf, self.member_epoch);
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
        }
        if version >= 10 {
            write_uuid(buf, &self.topic_id);
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
}
