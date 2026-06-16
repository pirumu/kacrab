//! Generated from StreamsGroupDescribeResponse.json - DO NOT EDIT
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
pub struct StreamsGroupDescribeResponseData {
    /// The duration in milliseconds for which the request was throttled due to a quota violation,
    /// or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// Each described group.
    pub groups: Vec<DescribedGroup>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for StreamsGroupDescribeResponseData {
    fn default() -> Self {
        Self {
            throttle_time_ms: 0_i32,
            groups: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl StreamsGroupDescribeResponseData {
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 0 || version > 0 {
            return Err(UnsupportedVersion::new(89, version).into());
        }
        let throttle_time_ms;
        let groups;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        throttle_time_ms = read_i32(buf)?;
        groups = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(len.max(0) as usize);
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
        if version < 0 || version > 0 {
            return Err(UnsupportedVersion::new(89, version).into());
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
    /// The topology metadata currently initialized for the streams application. Can be null in
    /// case of a describe error.
    pub topology: Option<Box<Topology>>,
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
            topology: None,
            members: Vec::new(),
            authorized_operations: i32::MIN,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl DescribedGroup {
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let error_code;
        let error_message;
        let group_id;
        let group_state;
        let group_epoch;
        let assignment_epoch;
        let topology;
        let members;
        let authorized_operations;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        error_code = read_i16(buf)?;
        error_message = read_compact_nullable_string(buf)?;
        group_id = read_compact_string(buf)?;
        group_state = read_compact_string(buf)?;
        group_epoch = read_i32(buf)?;
        assignment_epoch = read_i32(buf)?;
        topology = {
            let marker = read_i8(buf)?;
            if marker < 0 {
                None
            } else {
                Some(Box::new(Topology::read(buf, version)?))
            }
        };
        members = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(len.max(0) as usize);
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
            topology,
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
        match &self.topology {
            None => {
                write_i8(buf, -1);
            },
            Some(v) => {
                write_i8(buf, 1);
                v.write(buf, version)?;
            },
        }
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
}
#[derive(Debug, Clone, PartialEq)]
pub struct Topology {
    /// The epoch of the currently initialized topology for this group.
    pub epoch: i32,
    /// The subtopologies of the streams application. This contains the configured subtopologies,
    /// where the number of partitions are set and any regular expressions are resolved to actual
    /// topics. Null if the group is uninitialized, source topics are missing or incorrectly
    /// partitioned.
    pub subtopologies: Option<Vec<Subtopology>>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for Topology {
    fn default() -> Self {
        Self {
            epoch: 0_i32,
            subtopologies: None,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl Topology {
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let epoch;
        let subtopologies;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        epoch = read_i32(buf)?;
        subtopologies = {
            let len = read_compact_array_length(buf)?;
            if len < 0 {
                None
            } else {
                let mut arr = Vec::with_capacity(len as usize);
                for _ in 0..len {
                    arr.push(Subtopology::read(buf, version)?);
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
            epoch,
            subtopologies,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        write_i32(buf, self.epoch);
        match &self.subtopologies {
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
pub struct Subtopology {
    /// String to uniquely identify the subtopology.
    pub subtopology_id: KafkaString,
    /// The topics the subtopology reads from.
    pub source_topics: Vec<KafkaString>,
    /// The repartition topics the subtopology writes to.
    pub repartition_sink_topics: Vec<KafkaString>,
    /// The set of state changelog topics associated with this subtopology. Created automatically.
    pub state_changelog_topics: Vec<TopicInfo>,
    /// The set of source topics that are internally created repartition topics. Created
    /// automatically.
    pub repartition_source_topics: Vec<TopicInfo>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for Subtopology {
    fn default() -> Self {
        Self {
            subtopology_id: KafkaString::default(),
            source_topics: Vec::new(),
            repartition_sink_topics: Vec::new(),
            state_changelog_topics: Vec::new(),
            repartition_source_topics: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl Subtopology {
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let subtopology_id;
        let source_topics;
        let repartition_sink_topics;
        let state_changelog_topics;
        let repartition_source_topics;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        subtopology_id = read_compact_string(buf)?;
        source_topics = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(len.max(0) as usize);
            for _ in 0..len {
                arr.push(read_compact_string(buf)?);
            }
            arr
        };
        repartition_sink_topics = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(len.max(0) as usize);
            for _ in 0..len {
                arr.push(read_compact_string(buf)?);
            }
            arr
        };
        state_changelog_topics = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(len.max(0) as usize);
            for _ in 0..len {
                arr.push(TopicInfo::read(buf, version)?);
            }
            arr
        };
        repartition_source_topics = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(len.max(0) as usize);
            for _ in 0..len {
                arr.push(TopicInfo::read(buf, version)?);
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
            subtopology_id,
            source_topics,
            repartition_sink_topics,
            state_changelog_topics,
            repartition_source_topics,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        write_compact_string(buf, &self.subtopology_id)?;
        write_compact_array_length(buf, self.source_topics.len() as i32);
        for el in &self.source_topics {
            write_compact_string(buf, el)?;
        }
        write_compact_array_length(buf, self.repartition_sink_topics.len() as i32);
        for el in &self.repartition_sink_topics {
            write_compact_string(buf, el)?;
        }
        write_compact_array_length(buf, self.state_changelog_topics.len() as i32);
        for el in &self.state_changelog_topics {
            el.write(buf, version)?;
        }
        write_compact_array_length(buf, self.repartition_source_topics.len() as i32);
        for el in &self.repartition_source_topics {
            el.write(buf, version)?;
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct Member {
    /// The member ID.
    pub member_id: KafkaString,
    /// The member epoch.
    pub member_epoch: i32,
    /// The member instance ID for static membership.
    pub instance_id: Option<KafkaString>,
    /// The rack ID.
    pub rack_id: Option<KafkaString>,
    /// The client ID.
    pub client_id: KafkaString,
    /// The client host.
    pub client_host: KafkaString,
    /// The epoch of the topology on the client.
    pub topology_epoch: i32,
    /// Identity of the streams instance that may have multiple clients.
    pub process_id: KafkaString,
    /// User-defined endpoint for Interactive Queries. Null if not defined for this client.
    pub user_endpoint: Option<Box<Endpoint>>,
    /// Used for rack-aware assignment algorithm.
    pub client_tags: Vec<KeyValue>,
    /// Cumulative changelog offsets for tasks.
    pub task_offsets: Vec<TaskOffset>,
    /// Cumulative changelog end offsets for tasks.
    pub task_end_offsets: Vec<TaskOffset>,
    /// The current assignment.
    pub assignment: Assignment,
    /// The target assignment.
    pub target_assignment: Assignment,
    /// True for classic members that have not been upgraded yet.
    pub is_classic: bool,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for Member {
    fn default() -> Self {
        Self {
            member_id: KafkaString::default(),
            member_epoch: 0_i32,
            instance_id: None,
            rack_id: None,
            client_id: KafkaString::default(),
            client_host: KafkaString::default(),
            topology_epoch: 0_i32,
            process_id: KafkaString::default(),
            user_endpoint: None,
            client_tags: Vec::new(),
            task_offsets: Vec::new(),
            task_end_offsets: Vec::new(),
            assignment: Assignment::default(),
            target_assignment: Assignment::default(),
            is_classic: false,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl Member {
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let member_id;
        let member_epoch;
        let instance_id;
        let rack_id;
        let client_id;
        let client_host;
        let topology_epoch;
        let process_id;
        let user_endpoint;
        let client_tags;
        let task_offsets;
        let task_end_offsets;
        let assignment;
        let target_assignment;
        let is_classic;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        member_id = read_compact_string(buf)?;
        member_epoch = read_i32(buf)?;
        instance_id = read_compact_nullable_string(buf)?;
        rack_id = read_compact_nullable_string(buf)?;
        client_id = read_compact_string(buf)?;
        client_host = read_compact_string(buf)?;
        topology_epoch = read_i32(buf)?;
        process_id = read_compact_string(buf)?;
        user_endpoint = {
            let marker = read_i8(buf)?;
            if marker < 0 {
                None
            } else {
                Some(Box::new(Endpoint::read(buf, version)?))
            }
        };
        client_tags = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(len.max(0) as usize);
            for _ in 0..len {
                arr.push(KeyValue::read(buf, version)?);
            }
            arr
        };
        task_offsets = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(len.max(0) as usize);
            for _ in 0..len {
                arr.push(TaskOffset::read(buf, version)?);
            }
            arr
        };
        task_end_offsets = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(len.max(0) as usize);
            for _ in 0..len {
                arr.push(TaskOffset::read(buf, version)?);
            }
            arr
        };
        assignment = Assignment::read(buf, version)?;
        target_assignment = Assignment::read(buf, version)?;
        is_classic = read_bool(buf)?;
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
            member_epoch,
            instance_id,
            rack_id,
            client_id,
            client_host,
            topology_epoch,
            process_id,
            user_endpoint,
            client_tags,
            task_offsets,
            task_end_offsets,
            assignment,
            target_assignment,
            is_classic,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        write_compact_string(buf, &self.member_id)?;
        write_i32(buf, self.member_epoch);
        write_compact_nullable_string(buf, self.instance_id.as_ref())?;
        write_compact_nullable_string(buf, self.rack_id.as_ref())?;
        write_compact_string(buf, &self.client_id)?;
        write_compact_string(buf, &self.client_host)?;
        write_i32(buf, self.topology_epoch);
        write_compact_string(buf, &self.process_id)?;
        match &self.user_endpoint {
            None => {
                write_i8(buf, -1);
            },
            Some(v) => {
                write_i8(buf, 1);
                v.write(buf, version)?;
            },
        }
        write_compact_array_length(buf, self.client_tags.len() as i32);
        for el in &self.client_tags {
            el.write(buf, version)?;
        }
        write_compact_array_length(buf, self.task_offsets.len() as i32);
        for el in &self.task_offsets {
            el.write(buf, version)?;
        }
        write_compact_array_length(buf, self.task_end_offsets.len() as i32);
        for el in &self.task_end_offsets {
            el.write(buf, version)?;
        }
        self.assignment.write(buf, version)?;
        self.target_assignment.write(buf, version)?;
        write_bool(buf, self.is_classic);
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct Endpoint {
    /// host of the endpoint
    pub host: KafkaString,
    /// port of the endpoint
    pub port: u16,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for Endpoint {
    fn default() -> Self {
        Self {
            host: KafkaString::default(),
            port: 0_u16,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl Endpoint {
    pub fn read(buf: &mut Bytes, _version: i16) -> Result<Self> {
        let host;
        let port;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        host = read_compact_string(buf)?;
        port = read_u16(buf)?;
        let tagged_fields = read_tagged_fields(buf)?;
        for field in &tagged_fields {
            match field.tag {
                _ => {
                    _unknown_tagged_fields.push(field.clone());
                },
            }
        }
        Ok(Self {
            host,
            port,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, _version: i16) -> Result<()> {
        write_compact_string(buf, &self.host)?;
        write_u16(buf, self.port);
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct TaskOffset {
    /// The subtopology identifier.
    pub subtopology_id: KafkaString,
    /// The partition.
    pub partition: i32,
    /// The offset.
    pub offset: i64,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for TaskOffset {
    fn default() -> Self {
        Self {
            subtopology_id: KafkaString::default(),
            partition: 0_i32,
            offset: 0_i64,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl TaskOffset {
    pub fn read(buf: &mut Bytes, _version: i16) -> Result<Self> {
        let subtopology_id;
        let partition;
        let offset;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        subtopology_id = read_compact_string(buf)?;
        partition = read_i32(buf)?;
        offset = read_i64(buf)?;
        let tagged_fields = read_tagged_fields(buf)?;
        for field in &tagged_fields {
            match field.tag {
                _ => {
                    _unknown_tagged_fields.push(field.clone());
                },
            }
        }
        Ok(Self {
            subtopology_id,
            partition,
            offset,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, _version: i16) -> Result<()> {
        write_compact_string(buf, &self.subtopology_id)?;
        write_i32(buf, self.partition);
        write_i64(buf, self.offset);
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct Assignment {
    /// Active tasks for this client.
    pub active_tasks: Vec<TaskIds>,
    /// Standby tasks for this client.
    pub standby_tasks: Vec<TaskIds>,
    /// Warm-up tasks for this client.
    pub warmup_tasks: Vec<TaskIds>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for Assignment {
    fn default() -> Self {
        Self {
            active_tasks: Vec::new(),
            standby_tasks: Vec::new(),
            warmup_tasks: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl Assignment {
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let active_tasks;
        let standby_tasks;
        let warmup_tasks;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        active_tasks = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(len.max(0) as usize);
            for _ in 0..len {
                arr.push(TaskIds::read(buf, version)?);
            }
            arr
        };
        standby_tasks = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(len.max(0) as usize);
            for _ in 0..len {
                arr.push(TaskIds::read(buf, version)?);
            }
            arr
        };
        warmup_tasks = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(len.max(0) as usize);
            for _ in 0..len {
                arr.push(TaskIds::read(buf, version)?);
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
            active_tasks,
            standby_tasks,
            warmup_tasks,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        write_compact_array_length(buf, self.active_tasks.len() as i32);
        for el in &self.active_tasks {
            el.write(buf, version)?;
        }
        write_compact_array_length(buf, self.standby_tasks.len() as i32);
        for el in &self.standby_tasks {
            el.write(buf, version)?;
        }
        write_compact_array_length(buf, self.warmup_tasks.len() as i32);
        for el in &self.warmup_tasks {
            el.write(buf, version)?;
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct TaskIds {
    /// The subtopology identifier.
    pub subtopology_id: KafkaString,
    /// The partitions of the input topics processed by this member.
    pub partitions: Vec<i32>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for TaskIds {
    fn default() -> Self {
        Self {
            subtopology_id: KafkaString::default(),
            partitions: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl TaskIds {
    pub fn read(buf: &mut Bytes, _version: i16) -> Result<Self> {
        let subtopology_id;
        let partitions;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        subtopology_id = read_compact_string(buf)?;
        partitions = {
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
            subtopology_id,
            partitions,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, _version: i16) -> Result<()> {
        write_compact_string(buf, &self.subtopology_id)?;
        write_compact_array_length(buf, self.partitions.len() as i32);
        for el in &self.partitions {
            write_i32(buf, *el);
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct KeyValue {
    /// key of the config
    pub key: KafkaString,
    /// value of the config
    pub value: KafkaString,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for KeyValue {
    fn default() -> Self {
        Self {
            key: KafkaString::default(),
            value: KafkaString::default(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl KeyValue {
    pub fn read(buf: &mut Bytes, _version: i16) -> Result<Self> {
        let key;
        let value;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        key = read_compact_string(buf)?;
        value = read_compact_string(buf)?;
        let tagged_fields = read_tagged_fields(buf)?;
        for field in &tagged_fields {
            match field.tag {
                _ => {
                    _unknown_tagged_fields.push(field.clone());
                },
            }
        }
        Ok(Self {
            key,
            value,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, _version: i16) -> Result<()> {
        write_compact_string(buf, &self.key)?;
        write_compact_string(buf, &self.value)?;
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct TopicInfo {
    /// The name of the topic.
    pub name: KafkaString,
    /// The number of partitions in the topic. Can be 0 if no specific number of partitions is
    /// enforced. Always 0 for changelog topics.
    pub partitions: i32,
    /// The replication factor of the topic. Can be 0 if the default replication factor should be
    /// used.
    pub replication_factor: i16,
    /// Topic-level configurations as key-value pairs.
    pub topic_configs: Vec<KeyValue>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for TopicInfo {
    fn default() -> Self {
        Self {
            name: KafkaString::default(),
            partitions: 0_i32,
            replication_factor: 0_i16,
            topic_configs: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl TopicInfo {
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let name;
        let partitions;
        let replication_factor;
        let topic_configs;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        name = read_compact_string(buf)?;
        partitions = read_i32(buf)?;
        replication_factor = read_i16(buf)?;
        topic_configs = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(len.max(0) as usize);
            for _ in 0..len {
                arr.push(KeyValue::read(buf, version)?);
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
            replication_factor,
            topic_configs,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        write_compact_string(buf, &self.name)?;
        write_i32(buf, self.partitions);
        write_i16(buf, self.replication_factor);
        write_compact_array_length(buf, self.topic_configs.len() as i32);
        for el in &self.topic_configs {
            el.write(buf, version)?;
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
}
