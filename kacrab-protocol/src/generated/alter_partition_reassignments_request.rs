//! Generated from AlterPartitionReassignmentsRequest.json - DO NOT EDIT
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
pub struct AlterPartitionReassignmentsRequestData {
    /// The time in ms to wait for the request to complete.
    pub timeout_ms: i32,
    /// The option indicating whether changing the replication factor of any given partition as
    /// part of this request is a valid move.
    pub allow_replication_factor_change: bool,
    /// The topics to reassign.
    pub topics: Vec<ReassignableTopic>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for AlterPartitionReassignmentsRequestData {
    fn default() -> Self {
        Self {
            timeout_ms: 60000i32,
            allow_replication_factor_change: true,
            topics: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl AlterPartitionReassignmentsRequestData {
    pub fn with_timeout_ms(mut self, value: i32) -> Self {
        self.timeout_ms = value;
        self
    }
    pub fn with_allow_replication_factor_change(mut self, value: bool) -> Self {
        self.allow_replication_factor_change = value;
        self
    }
    pub fn with_topics(mut self, value: Vec<ReassignableTopic>) -> Self {
        self.topics = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 0 || version > 1 {
            return Err(UnsupportedVersion::new(45, version).into());
        }
        let timeout_ms;
        let mut allow_replication_factor_change = true;
        let topics;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        timeout_ms = read_i32(buf)?;
        if version >= 1 {
            allow_replication_factor_change = read_bool(buf)?;
        }
        topics = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(len.max(0) as usize);
            for _ in 0..len {
                arr.push(ReassignableTopic::read(buf, version)?);
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
            timeout_ms,
            allow_replication_factor_change,
            topics,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 0 || version > 1 {
            return Err(UnsupportedVersion::new(45, version).into());
        }
        write_i32(buf, self.timeout_ms);
        if version >= 1 {
            write_bool(buf, self.allow_replication_factor_change);
        } else if self.allow_replication_factor_change != true {
            return Err(UnsupportedFieldVersion::new(
                45,
                "allow_replication_factor_change",
                version,
            )
            .into());
        }
        write_compact_array_length(buf, self.topics.len() as i32);
        for el in &self.topics {
            el.write(buf, version)?;
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        if version < 0 || version > 1 {
            return Err(UnsupportedVersion::new(45, version).into());
        }
        let mut len: usize = 0;
        len += 4;
        if version >= 1 {
            len += 1;
        } else if self.allow_replication_factor_change != true {
            return Err(UnsupportedFieldVersion::new(
                45,
                "allow_replication_factor_change",
                version,
            )
            .into());
        }
        len += compact_array_length_len(self.topics.len() as i32);
        for el in &self.topics {
            len += el.encoded_len(version)?;
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct ReassignableTopic {
    /// The topic name.
    pub name: KafkaString,
    /// The partitions to reassign.
    pub partitions: Vec<ReassignablePartition>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for ReassignableTopic {
    fn default() -> Self {
        Self {
            name: KafkaString::default(),
            partitions: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl ReassignableTopic {
    pub fn with_name(mut self, value: KafkaString) -> Self {
        self.name = value;
        self
    }
    pub fn with_partitions(mut self, value: Vec<ReassignablePartition>) -> Self {
        self.partitions = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let name;
        let partitions;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        name = read_compact_string(buf)?;
        partitions = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(len.max(0) as usize);
            for _ in 0..len {
                arr.push(ReassignablePartition::read(buf, version)?);
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
            partitions,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        write_compact_string(buf, &self.name)?;
        write_compact_array_length(buf, self.partitions.len() as i32);
        for el in &self.partitions {
            el.write(buf, version)?;
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        let mut len: usize = 0;
        len += compact_string_len(&self.name)?;
        len += compact_array_length_len(self.partitions.len() as i32);
        for el in &self.partitions {
            len += el.encoded_len(version)?;
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct ReassignablePartition {
    /// The partition index.
    pub partition_index: i32,
    /// The replicas to place the partitions on, or null to cancel a pending reassignment for this
    /// partition.
    pub replicas: Option<Vec<i32>>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for ReassignablePartition {
    fn default() -> Self {
        Self {
            partition_index: 0_i32,
            replicas: None,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl ReassignablePartition {
    pub fn with_partition_index(mut self, value: i32) -> Self {
        self.partition_index = value;
        self
    }
    pub fn with_replicas(mut self, value: Option<Vec<i32>>) -> Self {
        self.replicas = value;
        self
    }
    pub fn read(buf: &mut Bytes, _version: i16) -> Result<Self> {
        let partition_index;
        let replicas;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        partition_index = read_i32(buf)?;
        replicas = {
            let len = read_compact_array_length(buf)?;
            if len < 0 {
                None
            } else {
                let mut arr = Vec::with_capacity(len as usize);
                for _ in 0..len {
                    arr.push(read_i32(buf)?);
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
            partition_index,
            replicas,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, _version: i16) -> Result<()> {
        write_i32(buf, self.partition_index);
        match &self.replicas {
            None => {
                write_compact_array_length(buf, -1);
            },
            Some(arr) => {
                write_compact_array_length(buf, arr.len() as i32);
                for el in arr {
                    write_i32(buf, *el);
                }
            },
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
    pub fn encoded_len(&self, _version: i16) -> Result<usize> {
        let mut len: usize = 0;
        len += 4;
        match &self.replicas {
            None => {
                len += compact_array_length_len(-1);
            },
            Some(arr) => {
                len += compact_array_length_len(arr.len() as i32);
                len += arr.len() * 4usize;
            },
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
