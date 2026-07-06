//! Generated from CreatePartitionsRequest.json - DO NOT EDIT
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
pub struct CreatePartitionsRequestData {
    /// Each topic that we want to create new partitions inside.
    pub topics: Vec<CreatePartitionsTopic>,
    /// The time in ms to wait for the partitions to be created.
    pub timeout_ms: i32,
    /// If true, then validate the request, but don't actually increase the number of partitions.
    pub validate_only: bool,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for CreatePartitionsRequestData {
    fn default() -> Self {
        Self {
            topics: Vec::new(),
            timeout_ms: 0_i32,
            validate_only: false,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl CreatePartitionsRequestData {
    pub fn with_topics(mut self, value: Vec<CreatePartitionsTopic>) -> Self {
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
        if version < 0 || version > 3 {
            return Err(UnsupportedVersion::new(37, version).into());
        }
        let topics;
        let timeout_ms;
        let validate_only;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 2 {
            topics = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
                for _ in 0..len {
                    arr.push(CreatePartitionsTopic::read(buf, version)?);
                }
                arr
            };
        } else {
            topics = {
                let len = read_array_length(buf)?;
                let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
                for _ in 0..len {
                    arr.push(CreatePartitionsTopic::read(buf, version)?);
                }
                arr
            };
        }
        timeout_ms = read_i32(buf)?;
        validate_only = read_bool(buf)?;
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
            topics,
            timeout_ms,
            validate_only,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 0 || version > 3 {
            return Err(UnsupportedVersion::new(37, version).into());
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
        write_i32(buf, self.timeout_ms);
        write_bool(buf, self.validate_only);
        if version >= 2 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        if version < 0 || version > 3 {
            return Err(UnsupportedVersion::new(37, version).into());
        }
        let mut len: usize = 0;
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
        len += 4;
        len += 1;
        if version >= 2 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            len += tagged_fields_len(&all_tags)?;
        }
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct CreatePartitionsTopic {
    /// The topic name.
    pub name: KafkaString,
    /// The new partition count.
    pub count: i32,
    /// The new partition assignments.
    pub assignments: Option<Vec<CreatePartitionsAssignment>>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for CreatePartitionsTopic {
    fn default() -> Self {
        Self {
            name: KafkaString::default(),
            count: 0_i32,
            assignments: None,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl CreatePartitionsTopic {
    pub fn with_name(mut self, value: KafkaString) -> Self {
        self.name = value;
        self
    }
    pub fn with_count(mut self, value: i32) -> Self {
        self.count = value;
        self
    }
    pub fn with_assignments(mut self, value: Option<Vec<CreatePartitionsAssignment>>) -> Self {
        self.assignments = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let name;
        let count;
        let assignments;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 2 {
            name = read_compact_string(buf)?;
        } else {
            name = read_string(buf)?;
        }
        count = read_i32(buf)?;
        if version >= 2 {
            assignments = {
                let len = read_compact_array_length(buf)?;
                if len < 0 {
                    None
                } else {
                    let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
                    for _ in 0..len {
                        arr.push(CreatePartitionsAssignment::read(buf, version)?);
                    }
                    Some(arr)
                }
            };
        } else {
            assignments = {
                let len = read_array_length(buf)?;
                if len < 0 {
                    None
                } else {
                    let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
                    for _ in 0..len {
                        arr.push(CreatePartitionsAssignment::read(buf, version)?);
                    }
                    Some(arr)
                }
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
            count,
            assignments,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version >= 2 {
            write_compact_string(buf, &self.name)?;
        } else {
            write_string(buf, &self.name)?;
        }
        write_i32(buf, self.count);
        if version >= 2 {
            match &self.assignments {
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
            match &self.assignments {
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
        len += 4;
        if version >= 2 {
            match &self.assignments {
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
            match &self.assignments {
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
        if version >= 2 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            len += tagged_fields_len(&all_tags)?;
        }
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct CreatePartitionsAssignment {
    /// The assigned broker IDs.
    pub broker_ids: Vec<i32>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for CreatePartitionsAssignment {
    fn default() -> Self {
        Self {
            broker_ids: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl CreatePartitionsAssignment {
    pub fn with_broker_ids(mut self, value: Vec<i32>) -> Self {
        self.broker_ids = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let broker_ids;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 2 {
            broker_ids = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
                for _ in 0..len {
                    arr.push(read_i32(buf)?);
                }
                arr
            };
        } else {
            broker_ids = {
                let len = read_array_length(buf)?;
                let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
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
            broker_ids,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version >= 2 {
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
            len += compact_array_length_len(self.broker_ids.len() as i32);
            len += self.broker_ids.len() * 4usize;
        } else {
            len += array_length_len();
            len += self.broker_ids.len() * 4usize;
        }
        if version >= 2 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            len += tagged_fields_len(&all_tags)?;
        }
        Ok(len)
    }
}
