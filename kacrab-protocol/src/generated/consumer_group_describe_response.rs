//! Generated from ConsumerGroupDescribeResponse.json - DO NOT EDIT
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
pub struct ConsumerGroupDescribeResponseData {
    /// The duration in milliseconds for which the request was throttled due to a quota violation,
    /// or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// Each described group.
    pub groups: Vec<DescribedGroup>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for ConsumerGroupDescribeResponseData {
    fn default() -> Self {
        Self {
            throttle_time_ms: 0_i32,
            groups: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl ConsumerGroupDescribeResponseData {
    pub fn with_throttle_time_ms(mut self, value: i32) -> Self {
        self.throttle_time_ms = value;
        self
    }
    pub fn with_groups(mut self, value: Vec<DescribedGroup>) -> Self {
        self.groups = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 0 || version > 1 {
            return Err(UnsupportedVersion::new(69, version).into());
        }
        let throttle_time_ms;
        let groups;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        throttle_time_ms = read_i32(buf)?;
        groups = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
            for _ in 0..len {
                arr.push(DescribedGroup::read(buf, version)?);
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
            throttle_time_ms,
            groups,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 0 || version > 1 {
            return Err(UnsupportedVersion::new(69, version).into());
        }
        write_i32(buf, self.throttle_time_ms);
        write_compact_array_length(buf, self.groups.len() as i32);
        for el in &self.groups {
            el.write(buf, version)?;
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        if version < 0 || version > 1 {
            return Err(UnsupportedVersion::new(69, version).into());
        }
        let mut len: usize = 0;
        len += 4;
        len += compact_array_length_len(self.groups.len() as i32);
        for el in &self.groups {
            len += el.encoded_len(version)?;
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct DescribedGroup {
    /// The describe error, or 0 if there was no error.
    pub error_code: i16,
    /// The top-level error message, or null if there was no error.
    pub error_message: Option<KafkaString>,
    /// The group ID string.
    pub group_id: KafkaString,
    /// The group state string, or the empty string.
    pub group_state: KafkaString,
    /// The group epoch.
    pub group_epoch: i32,
    /// The assignment epoch.
    pub assignment_epoch: i32,
    /// The selected assignor.
    pub assignor_name: KafkaString,
    /// The members.
    pub members: Vec<Member>,
    /// 32-bit bitfield to represent authorized operations for this group.
    pub authorized_operations: i32,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for DescribedGroup {
    fn default() -> Self {
        Self {
            error_code: 0_i16,
            error_message: None,
            group_id: KafkaString::default(),
            group_state: KafkaString::default(),
            group_epoch: 0_i32,
            assignment_epoch: 0_i32,
            assignor_name: KafkaString::default(),
            members: Vec::new(),
            authorized_operations: i32::MIN,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl DescribedGroup {
    pub fn with_error_code(mut self, value: i16) -> Self {
        self.error_code = value;
        self
    }
    pub fn with_error_message(mut self, value: Option<KafkaString>) -> Self {
        self.error_message = value;
        self
    }
    pub fn with_group_id(mut self, value: KafkaString) -> Self {
        self.group_id = value;
        self
    }
    pub fn with_group_state(mut self, value: KafkaString) -> Self {
        self.group_state = value;
        self
    }
    pub fn with_group_epoch(mut self, value: i32) -> Self {
        self.group_epoch = value;
        self
    }
    pub fn with_assignment_epoch(mut self, value: i32) -> Self {
        self.assignment_epoch = value;
        self
    }
    pub fn with_assignor_name(mut self, value: KafkaString) -> Self {
        self.assignor_name = value;
        self
    }
    pub fn with_members(mut self, value: Vec<Member>) -> Self {
        self.members = value;
        self
    }
    pub fn with_authorized_operations(mut self, value: i32) -> Self {
        self.authorized_operations = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let error_code;
        let error_message;
        let group_id;
        let group_state;
        let group_epoch;
        let assignment_epoch;
        let assignor_name;
        let members;
        let authorized_operations;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        error_code = read_i16(buf)?;
        error_message = read_compact_nullable_string(buf)?;
        group_id = read_compact_string(buf)?;
        group_state = read_compact_string(buf)?;
        group_epoch = read_i32(buf)?;
        assignment_epoch = read_i32(buf)?;
        assignor_name = read_compact_string(buf)?;
        members = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
            for _ in 0..len {
                arr.push(Member::read(buf, version)?);
            }
            arr
        };
        authorized_operations = read_i32(buf)?;
        let tagged_fields = read_tagged_fields(buf)?;
        for field in &tagged_fields {
            match field.tag {
                _ => {
                    _unknown_tagged_fields.push(field.clone());
                },
            }
        }
        Ok(Self {
            error_code,
            error_message,
            group_id,
            group_state,
            group_epoch,
            assignment_epoch,
            assignor_name,
            members,
            authorized_operations,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        write_i16(buf, self.error_code);
        write_compact_nullable_string(buf, self.error_message.as_ref())?;
        write_compact_string(buf, &self.group_id)?;
        write_compact_string(buf, &self.group_state)?;
        write_i32(buf, self.group_epoch);
        write_i32(buf, self.assignment_epoch);
        write_compact_string(buf, &self.assignor_name)?;
        write_compact_array_length(buf, self.members.len() as i32);
        for el in &self.members {
            el.write(buf, version)?;
        }
        write_i32(buf, self.authorized_operations);
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        let mut len: usize = 0;
        len += 2;
        len += compact_nullable_string_len(self.error_message.as_ref())?;
        len += compact_string_len(&self.group_id)?;
        len += compact_string_len(&self.group_state)?;
        len += 4;
        len += 4;
        len += compact_string_len(&self.assignor_name)?;
        len += compact_array_length_len(self.members.len() as i32);
        for el in &self.members {
            len += el.encoded_len(version)?;
        }
        len += 4;
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct Member {
    /// The member ID.
    pub member_id: KafkaString,
    /// The member instance ID.
    pub instance_id: Option<KafkaString>,
    /// The member rack ID.
    pub rack_id: Option<KafkaString>,
    /// The current member epoch.
    pub member_epoch: i32,
    /// The client ID.
    pub client_id: KafkaString,
    /// The client host.
    pub client_host: KafkaString,
    /// The subscribed topic names.
    pub subscribed_topic_names: Vec<KafkaString>,
    /// the subscribed topic regex otherwise or null of not provided.
    pub subscribed_topic_regex: Option<KafkaString>,
    /// The current assignment.
    pub assignment: Assignment,
    /// The target assignment.
    pub target_assignment: Assignment,
    /// -1 for unknown. 0 for classic member. +1 for consumer member.
    pub member_type: i8,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for Member {
    fn default() -> Self {
        Self {
            member_id: KafkaString::default(),
            instance_id: None,
            rack_id: None,
            member_epoch: 0_i32,
            client_id: KafkaString::default(),
            client_host: KafkaString::default(),
            subscribed_topic_names: Vec::new(),
            subscribed_topic_regex: None,
            assignment: Assignment::default(),
            target_assignment: Assignment::default(),
            member_type: -1i8,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl Member {
    pub fn with_member_id(mut self, value: KafkaString) -> Self {
        self.member_id = value;
        self
    }
    pub fn with_instance_id(mut self, value: Option<KafkaString>) -> Self {
        self.instance_id = value;
        self
    }
    pub fn with_rack_id(mut self, value: Option<KafkaString>) -> Self {
        self.rack_id = value;
        self
    }
    pub fn with_member_epoch(mut self, value: i32) -> Self {
        self.member_epoch = value;
        self
    }
    pub fn with_client_id(mut self, value: KafkaString) -> Self {
        self.client_id = value;
        self
    }
    pub fn with_client_host(mut self, value: KafkaString) -> Self {
        self.client_host = value;
        self
    }
    pub fn with_subscribed_topic_names(mut self, value: Vec<KafkaString>) -> Self {
        self.subscribed_topic_names = value;
        self
    }
    pub fn with_subscribed_topic_regex(mut self, value: Option<KafkaString>) -> Self {
        self.subscribed_topic_regex = value;
        self
    }
    pub fn with_assignment(mut self, value: Assignment) -> Self {
        self.assignment = value;
        self
    }
    pub fn with_target_assignment(mut self, value: Assignment) -> Self {
        self.target_assignment = value;
        self
    }
    pub fn with_member_type(mut self, value: i8) -> Self {
        self.member_type = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let member_id;
        let instance_id;
        let rack_id;
        let member_epoch;
        let client_id;
        let client_host;
        let subscribed_topic_names;
        let subscribed_topic_regex;
        let assignment;
        let target_assignment;
        let mut member_type = -1i8;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        member_id = read_compact_string(buf)?;
        instance_id = read_compact_nullable_string(buf)?;
        rack_id = read_compact_nullable_string(buf)?;
        member_epoch = read_i32(buf)?;
        client_id = read_compact_string(buf)?;
        client_host = read_compact_string(buf)?;
        subscribed_topic_names = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
            for _ in 0..len {
                arr.push(read_compact_string(buf)?);
            }
            arr
        };
        subscribed_topic_regex = read_compact_nullable_string(buf)?;
        assignment = Assignment::read(buf, version)?;
        target_assignment = Assignment::read(buf, version)?;
        if version >= 1 {
            member_type = read_i8(buf)?;
        }
        let tagged_fields = read_tagged_fields(buf)?;
        for field in &tagged_fields {
            match field.tag {
                _ => {
                    _unknown_tagged_fields.push(field.clone());
                },
            }
        }
        Ok(Self {
            member_id,
            instance_id,
            rack_id,
            member_epoch,
            client_id,
            client_host,
            subscribed_topic_names,
            subscribed_topic_regex,
            assignment,
            target_assignment,
            member_type,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        write_compact_string(buf, &self.member_id)?;
        write_compact_nullable_string(buf, self.instance_id.as_ref())?;
        write_compact_nullable_string(buf, self.rack_id.as_ref())?;
        write_i32(buf, self.member_epoch);
        write_compact_string(buf, &self.client_id)?;
        write_compact_string(buf, &self.client_host)?;
        write_compact_array_length(buf, self.subscribed_topic_names.len() as i32);
        for el in &self.subscribed_topic_names {
            write_compact_string(buf, el)?;
        }
        write_compact_nullable_string(buf, self.subscribed_topic_regex.as_ref())?;
        self.assignment.write(buf, version)?;
        self.target_assignment.write(buf, version)?;
        if version >= 1 {
            write_i8(buf, self.member_type);
        } else if self.member_type != -1i8 {
            return Err(UnsupportedFieldVersion::new(69, "member_type", version).into());
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        let mut len: usize = 0;
        len += compact_string_len(&self.member_id)?;
        len += compact_nullable_string_len(self.instance_id.as_ref())?;
        len += compact_nullable_string_len(self.rack_id.as_ref())?;
        len += 4;
        len += compact_string_len(&self.client_id)?;
        len += compact_string_len(&self.client_host)?;
        len += compact_array_length_len(self.subscribed_topic_names.len() as i32);
        for el in &self.subscribed_topic_names {
            len += compact_string_len(el)?;
        }
        len += compact_nullable_string_len(self.subscribed_topic_regex.as_ref())?;
        len += self.assignment.encoded_len(version)?;
        len += self.target_assignment.encoded_len(version)?;
        if version >= 1 {
            len += 1;
        } else if self.member_type != -1i8 {
            return Err(UnsupportedFieldVersion::new(69, "member_type", version).into());
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct TopicPartitions {
    /// The topic ID.
    pub topic_id: KafkaUuid,
    /// The topic name.
    pub topic_name: KafkaString,
    /// The partitions.
    pub partitions: Vec<i32>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for TopicPartitions {
    fn default() -> Self {
        Self {
            topic_id: KafkaUuid::ZERO,
            topic_name: KafkaString::default(),
            partitions: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl TopicPartitions {
    pub fn with_topic_id(mut self, value: KafkaUuid) -> Self {
        self.topic_id = value;
        self
    }
    pub fn with_topic_name(mut self, value: KafkaString) -> Self {
        self.topic_name = value;
        self
    }
    pub fn with_partitions(mut self, value: Vec<i32>) -> Self {
        self.partitions = value;
        self
    }
    pub fn read(buf: &mut Bytes, _version: i16) -> Result<Self> {
        let topic_id;
        let topic_name;
        let partitions;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        topic_id = read_uuid(buf)?;
        topic_name = read_compact_string(buf)?;
        partitions = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
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
            topic_id,
            topic_name,
            partitions,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, _version: i16) -> Result<()> {
        write_uuid(buf, &self.topic_id);
        write_compact_string(buf, &self.topic_name)?;
        write_compact_array_length(buf, self.partitions.len() as i32);
        for el in &self.partitions {
            write_i32(buf, *el);
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
    pub fn encoded_len(&self, _version: i16) -> Result<usize> {
        let mut len: usize = 0;
        len += 16;
        len += compact_string_len(&self.topic_name)?;
        len += compact_array_length_len(self.partitions.len() as i32);
        len += self.partitions.len() * 4usize;
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct Assignment {
    /// The assigned topic-partitions to the member.
    pub topic_partitions: Vec<TopicPartitions>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for Assignment {
    fn default() -> Self {
        Self {
            topic_partitions: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl Assignment {
    pub fn with_topic_partitions(mut self, value: Vec<TopicPartitions>) -> Self {
        self.topic_partitions = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let topic_partitions;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        topic_partitions = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
            for _ in 0..len {
                arr.push(TopicPartitions::read(buf, version)?);
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
            topic_partitions,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        write_compact_array_length(buf, self.topic_partitions.len() as i32);
        for el in &self.topic_partitions {
            el.write(buf, version)?;
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        let mut len: usize = 0;
        len += compact_array_length_len(self.topic_partitions.len() as i32);
        for el in &self.topic_partitions {
            len += el.encoded_len(version)?;
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
