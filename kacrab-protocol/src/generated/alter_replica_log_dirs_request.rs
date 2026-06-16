//! Generated from AlterReplicaLogDirsRequest.json - DO NOT EDIT
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
pub struct AlterReplicaLogDirsRequestData {
    /// The alterations to make for each directory.
    pub dirs: Vec<AlterReplicaLogDir>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for AlterReplicaLogDirsRequestData {
    fn default() -> Self {
        Self {
            dirs: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl AlterReplicaLogDirsRequestData {
    pub fn with_dirs(mut self, value: Vec<AlterReplicaLogDir>) -> Self {
        self.dirs = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 1 || version > 2 {
            return Err(UnsupportedVersion::new(34, version).into());
        }
        let dirs;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 2 {
            dirs = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(AlterReplicaLogDir::read(buf, version)?);
                }
                arr
            };
        } else {
            dirs = {
                let len = read_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(AlterReplicaLogDir::read(buf, version)?);
                }
                arr
            };
        }
        if version >= 2 {
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
            dirs,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 1 || version > 2 {
            return Err(UnsupportedVersion::new(34, version).into());
        }
        if version >= 2 {
            write_compact_array_length(buf, self.dirs.len() as i32);
            for el in &self.dirs {
                el.write(buf, version)?;
            }
        } else {
            write_array_length(buf, self.dirs.len() as i32);
            for el in &self.dirs {
                el.write(buf, version)?;
            }
        }
        if version >= 2 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        if version < 1 || version > 2 {
            return Err(UnsupportedVersion::new(34, version).into());
        }
        let mut len: usize = 0;
        if version >= 2 {
            len += compact_array_length_len(self.dirs.len() as i32);
            for el in &self.dirs {
                len += el.encoded_len(version)?;
            }
        } else {
            len += array_length_len();
            for el in &self.dirs {
                len += el.encoded_len(version)?;
            }
        }
        if version >= 2 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            len += tagged_fields_len(&all_tags)?;
        }
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct AlterReplicaLogDir {
    /// The absolute directory path.
    pub path: KafkaString,
    /// The topics to add to the directory.
    pub topics: Vec<AlterReplicaLogDirTopic>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for AlterReplicaLogDir {
    fn default() -> Self {
        Self {
            path: KafkaString::default(),
            topics: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl AlterReplicaLogDir {
    pub fn with_path(mut self, value: KafkaString) -> Self {
        self.path = value;
        self
    }
    pub fn with_topics(mut self, value: Vec<AlterReplicaLogDirTopic>) -> Self {
        self.topics = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let path;
        let topics;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 2 {
            path = read_compact_string(buf)?;
        } else {
            path = read_string(buf)?;
        }
        if version >= 2 {
            topics = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(AlterReplicaLogDirTopic::read(buf, version)?);
                }
                arr
            };
        } else {
            topics = {
                let len = read_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(AlterReplicaLogDirTopic::read(buf, version)?);
                }
                arr
            };
        }
        if version >= 2 {
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
            path,
            topics,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version >= 2 {
            write_compact_string(buf, &self.path)?;
        } else {
            write_string(buf, &self.path)?;
        }
        if version >= 2 {
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
        if version >= 2 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        let mut len: usize = 0;
        if version >= 2 {
            len += compact_string_len(&self.path)?;
        } else {
            len += string_len(&self.path)?;
        }
        if version >= 2 {
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
        if version >= 2 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            len += tagged_fields_len(&all_tags)?;
        }
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct AlterReplicaLogDirTopic {
    /// The topic name.
    pub name: KafkaString,
    /// The partition indexes.
    pub partitions: Vec<i32>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for AlterReplicaLogDirTopic {
    fn default() -> Self {
        Self {
            name: KafkaString::default(),
            partitions: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl AlterReplicaLogDirTopic {
    pub fn with_name(mut self, value: KafkaString) -> Self {
        self.name = value;
        self
    }
    pub fn with_partitions(mut self, value: Vec<i32>) -> Self {
        self.partitions = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let name;
        let partitions;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 2 {
            name = read_compact_string(buf)?;
        } else {
            name = read_string(buf)?;
        }
        if version >= 2 {
            partitions = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(read_i32(buf)?);
                }
                arr
            };
        } else {
            partitions = {
                let len = read_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(read_i32(buf)?);
                }
                arr
            };
        }
        if version >= 2 {
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
            partitions,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version >= 2 {
            write_compact_string(buf, &self.name)?;
        } else {
            write_string(buf, &self.name)?;
        }
        if version >= 2 {
            write_compact_array_length(buf, self.partitions.len() as i32);
            for el in &self.partitions {
                write_i32(buf, *el);
            }
        } else {
            write_array_length(buf, self.partitions.len() as i32);
            for el in &self.partitions {
                write_i32(buf, *el);
            }
        }
        if version >= 2 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        let mut len: usize = 0;
        if version >= 2 {
            len += compact_string_len(&self.name)?;
        } else {
            len += string_len(&self.name)?;
        }
        if version >= 2 {
            len += compact_array_length_len(self.partitions.len() as i32);
            len += self.partitions.len() * 4usize;
        } else {
            len += array_length_len();
            len += self.partitions.len() * 4usize;
        }
        if version >= 2 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            len += tagged_fields_len(&all_tags)?;
        }
        Ok(len)
    }
}
