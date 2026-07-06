//! Generated from ElectLeadersRequest.json - DO NOT EDIT
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
pub struct ElectLeadersRequestData {
    /// Type of elections to conduct for the partition. A value of '0' elects the preferred
    /// replica. A value of '1' elects the first live replica if there are no in-sync replica.
    pub election_type: i8,
    /// The topic partitions to elect leaders.
    pub topic_partitions: Option<Vec<TopicPartitions>>,
    /// The time in ms to wait for the election to complete.
    pub timeout_ms: i32,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for ElectLeadersRequestData {
    fn default() -> Self {
        Self {
            election_type: 0_i8,
            topic_partitions: None,
            timeout_ms: 60000i32,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl ElectLeadersRequestData {
    pub fn with_election_type(mut self, value: i8) -> Self {
        self.election_type = value;
        self
    }
    pub fn with_topic_partitions(mut self, value: Option<Vec<TopicPartitions>>) -> Self {
        self.topic_partitions = value;
        self
    }
    pub fn with_timeout_ms(mut self, value: i32) -> Self {
        self.timeout_ms = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 0 || version > 2 {
            return Err(UnsupportedVersion::new(43, version).into());
        }
        let mut election_type = 0_i8;
        let topic_partitions;
        let timeout_ms;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 1 {
            election_type = read_i8(buf)?;
        }
        if version >= 2 {
            topic_partitions = {
                let len = read_compact_array_length(buf)?;
                if len < 0 {
                    None
                } else {
                    let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
                    for _ in 0..len {
                        arr.push(TopicPartitions::read(buf, version)?);
                    }
                    Some(arr)
                }
            };
        } else {
            topic_partitions = {
                let len = read_array_length(buf)?;
                if len < 0 {
                    None
                } else {
                    let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
                    for _ in 0..len {
                        arr.push(TopicPartitions::read(buf, version)?);
                    }
                    Some(arr)
                }
            };
        }
        timeout_ms = read_i32(buf)?;
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
            election_type,
            topic_partitions,
            timeout_ms,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 0 || version > 2 {
            return Err(UnsupportedVersion::new(43, version).into());
        }
        if version >= 1 {
            write_i8(buf, self.election_type);
        } else if self.election_type != 0_i8 {
            return Err(UnsupportedFieldVersion::new(43, "election_type", version).into());
        }
        if version >= 2 {
            match &self.topic_partitions {
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
            match &self.topic_partitions {
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
        write_i32(buf, self.timeout_ms);
        if version >= 2 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        if version < 0 || version > 2 {
            return Err(UnsupportedVersion::new(43, version).into());
        }
        let mut len: usize = 0;
        if version >= 1 {
            len += 1;
        } else if self.election_type != 0_i8 {
            return Err(UnsupportedFieldVersion::new(43, "election_type", version).into());
        }
        if version >= 2 {
            match &self.topic_partitions {
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
            match &self.topic_partitions {
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
        len += 4;
        if version >= 2 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            len += tagged_fields_len(&all_tags)?;
        }
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct TopicPartitions {
    /// The name of a topic.
    pub topic: KafkaString,
    /// The partitions of this topic whose leader should be elected.
    pub partitions: Vec<i32>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for TopicPartitions {
    fn default() -> Self {
        Self {
            topic: KafkaString::default(),
            partitions: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl TopicPartitions {
    pub fn with_topic(mut self, value: KafkaString) -> Self {
        self.topic = value;
        self
    }
    pub fn with_partitions(mut self, value: Vec<i32>) -> Self {
        self.partitions = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let topic;
        let partitions;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 2 {
            topic = read_compact_string(buf)?;
        } else {
            topic = read_string(buf)?;
        }
        if version >= 2 {
            partitions = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
                for _ in 0..len {
                    arr.push(read_i32(buf)?);
                }
                arr
            };
        } else {
            partitions = {
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
            topic,
            partitions,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version >= 2 {
            write_compact_string(buf, &self.topic)?;
        } else {
            write_string(buf, &self.topic)?;
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
            len += compact_string_len(&self.topic)?;
        } else {
            len += string_len(&self.topic)?;
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
