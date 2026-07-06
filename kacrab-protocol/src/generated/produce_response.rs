//! Generated from ProduceResponse.json - DO NOT EDIT
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
pub struct ProduceResponseData {
    /// Each produce response.
    pub responses: Vec<TopicProduceResponse>,
    /// The duration in milliseconds for which the request was throttled due to a quota violation,
    /// or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// Endpoints for all current-leaders enumerated in PartitionProduceResponses, with errors
    /// NOT_LEADER_OR_FOLLOWER.
    pub node_endpoints: Vec<NodeEndpoint>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for ProduceResponseData {
    fn default() -> Self {
        Self {
            responses: Vec::new(),
            throttle_time_ms: 0i32,
            node_endpoints: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl ProduceResponseData {
    pub fn with_responses(mut self, value: Vec<TopicProduceResponse>) -> Self {
        self.responses = value;
        self
    }
    pub fn with_throttle_time_ms(mut self, value: i32) -> Self {
        self.throttle_time_ms = value;
        self
    }
    pub fn with_node_endpoints(mut self, value: Vec<NodeEndpoint>) -> Self {
        self.node_endpoints = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 3 || version > 13 {
            return Err(UnsupportedVersion::new(0, version).into());
        }
        let responses;
        let throttle_time_ms;
        let mut node_endpoints = Vec::new();
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 9 {
            responses = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
                for _ in 0..len {
                    arr.push(TopicProduceResponse::read(buf, version)?);
                }
                arr
            };
        } else {
            responses = {
                let len = read_array_length(buf)?;
                let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
                for _ in 0..len {
                    arr.push(TopicProduceResponse::read(buf, version)?);
                }
                arr
            };
        }
        throttle_time_ms = read_i32(buf)?;
        if version >= 9 {
            let tagged_fields = read_tagged_fields(buf)?;
            for field in &tagged_fields {
                match field.tag {
                    0 => {
                        if version >= 10 {
                            let mut tag_buf = field.data.clone();
                            node_endpoints = {
                                let len = read_compact_array_length(&mut tag_buf)?;
                                let mut arr = Vec::with_capacity(array_read_capacity(
                                    len,
                                    (&mut tag_buf).len(),
                                ));
                                for _ in 0..len {
                                    arr.push(NodeEndpoint::read(&mut tag_buf, version)?);
                                }
                                arr
                            };
                        }
                    },
                    _ => {
                        _unknown_tagged_fields.push(field.clone());
                    },
                }
            }
        }
        Ok(Self {
            responses,
            throttle_time_ms,
            node_endpoints,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 3 || version > 13 {
            return Err(UnsupportedVersion::new(0, version).into());
        }
        if version >= 9 {
            write_compact_array_length(buf, self.responses.len() as i32);
            for el in &self.responses {
                el.write(buf, version)?;
            }
        } else {
            write_array_length(buf, self.responses.len() as i32);
            for el in &self.responses {
                el.write(buf, version)?;
            }
        }
        write_i32(buf, self.throttle_time_ms);
        if version >= 9 {
            let mut known_tagged_fields: Vec<RawTaggedField> = Vec::new();
            if version >= 10 && !self.node_endpoints.is_empty() {
                let mut tag_buf = BytesMut::new();
                write_compact_array_length(&mut tag_buf, self.node_endpoints.len() as i32);
                for el in &self.node_endpoints {
                    el.write(&mut tag_buf, version)?;
                }
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
        if version < 3 || version > 13 {
            return Err(UnsupportedVersion::new(0, version).into());
        }
        let mut len: usize = 0;
        if version >= 9 {
            len += compact_array_length_len(self.responses.len() as i32);
            for el in &self.responses {
                len += el.encoded_len(version)?;
            }
        } else {
            len += array_length_len();
            for el in &self.responses {
                len += el.encoded_len(version)?;
            }
        }
        len += 4;
        if version >= 9 {
            let mut known_tagged_fields: Vec<RawTaggedField> = Vec::new();
            if version >= 10 && !self.node_endpoints.is_empty() {
                let mut tag_buf = BytesMut::new();
                write_compact_array_length(&mut tag_buf, self.node_endpoints.len() as i32);
                for el in &self.node_endpoints {
                    el.write(&mut tag_buf, version)?;
                }
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
pub struct TopicProduceResponse {
    /// The topic name.
    pub name: KafkaString,
    /// The unique topic ID
    pub topic_id: KafkaUuid,
    /// Each partition that we produced to within the topic.
    pub partition_responses: Vec<PartitionProduceResponse>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for TopicProduceResponse {
    fn default() -> Self {
        Self {
            name: KafkaString::default(),
            topic_id: KafkaUuid::ZERO,
            partition_responses: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl TopicProduceResponse {
    pub fn with_name(mut self, value: KafkaString) -> Self {
        self.name = value;
        self
    }
    pub fn with_topic_id(mut self, value: KafkaUuid) -> Self {
        self.topic_id = value;
        self
    }
    pub fn with_partition_responses(mut self, value: Vec<PartitionProduceResponse>) -> Self {
        self.partition_responses = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let mut name = KafkaString::default();
        let mut topic_id = KafkaUuid::ZERO;
        let partition_responses;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version <= 12 {
            if version >= 9 {
                name = read_compact_string(buf)?;
            } else {
                name = read_string(buf)?;
            }
        }
        if version >= 13 {
            topic_id = read_uuid(buf)?;
        }
        if version >= 9 {
            partition_responses = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
                for _ in 0..len {
                    arr.push(PartitionProduceResponse::read(buf, version)?);
                }
                arr
            };
        } else {
            partition_responses = {
                let len = read_array_length(buf)?;
                let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
                for _ in 0..len {
                    arr.push(PartitionProduceResponse::read(buf, version)?);
                }
                arr
            };
        }
        if version >= 9 {
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
            topic_id,
            partition_responses,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version <= 12 {
            if version >= 9 {
                write_compact_string(buf, &self.name)?;
            } else {
                write_string(buf, &self.name)?;
            }
        } else if self.name != KafkaString::default() {
            return Err(UnsupportedFieldVersion::new(0, "name", version).into());
        }
        if version >= 13 {
            write_uuid(buf, &self.topic_id);
        } else if self.topic_id != KafkaUuid::ZERO {
            return Err(UnsupportedFieldVersion::new(0, "topic_id", version).into());
        }
        if version >= 9 {
            write_compact_array_length(buf, self.partition_responses.len() as i32);
            for el in &self.partition_responses {
                el.write(buf, version)?;
            }
        } else {
            write_array_length(buf, self.partition_responses.len() as i32);
            for el in &self.partition_responses {
                el.write(buf, version)?;
            }
        }
        if version >= 9 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        let mut len: usize = 0;
        if version <= 12 {
            if version >= 9 {
                len += compact_string_len(&self.name)?;
            } else {
                len += string_len(&self.name)?;
            }
        } else if self.name != KafkaString::default() {
            return Err(UnsupportedFieldVersion::new(0, "name", version).into());
        }
        if version >= 13 {
            len += 16;
        } else if self.topic_id != KafkaUuid::ZERO {
            return Err(UnsupportedFieldVersion::new(0, "topic_id", version).into());
        }
        if version >= 9 {
            len += compact_array_length_len(self.partition_responses.len() as i32);
            for el in &self.partition_responses {
                len += el.encoded_len(version)?;
            }
        } else {
            len += array_length_len();
            for el in &self.partition_responses {
                len += el.encoded_len(version)?;
            }
        }
        if version >= 9 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            len += tagged_fields_len(&all_tags)?;
        }
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct PartitionProduceResponse {
    /// The partition index.
    pub index: i32,
    /// The error code, or 0 if there was no error.
    pub error_code: i16,
    /// The base offset.
    pub base_offset: i64,
    /// The timestamp returned by broker after appending the messages. If CreateTime is used for
    /// the topic, the timestamp will be -1.  If LogAppendTime is used for the topic, the timestamp
    /// will be the broker local time when the messages are appended.
    pub log_append_time_ms: i64,
    /// The log start offset.
    pub log_start_offset: i64,
    /// The batch indices of records that caused the batch to be dropped.
    pub record_errors: Vec<BatchIndexAndErrorMessage>,
    /// The global error message summarizing the common root cause of the records that caused the
    /// batch to be dropped.
    pub error_message: Option<KafkaString>,
    /// The leader broker that the producer should use for future requests.
    pub current_leader: LeaderIdAndEpoch,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for PartitionProduceResponse {
    fn default() -> Self {
        Self {
            index: 0_i32,
            error_code: 0_i16,
            base_offset: 0_i64,
            log_append_time_ms: -1i64,
            log_start_offset: -1i64,
            record_errors: Vec::new(),
            error_message: None,
            current_leader: LeaderIdAndEpoch::default(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl PartitionProduceResponse {
    pub fn with_index(mut self, value: i32) -> Self {
        self.index = value;
        self
    }
    pub fn with_error_code(mut self, value: i16) -> Self {
        self.error_code = value;
        self
    }
    pub fn with_base_offset(mut self, value: i64) -> Self {
        self.base_offset = value;
        self
    }
    pub fn with_log_append_time_ms(mut self, value: i64) -> Self {
        self.log_append_time_ms = value;
        self
    }
    pub fn with_log_start_offset(mut self, value: i64) -> Self {
        self.log_start_offset = value;
        self
    }
    pub fn with_record_errors(mut self, value: Vec<BatchIndexAndErrorMessage>) -> Self {
        self.record_errors = value;
        self
    }
    pub fn with_error_message(mut self, value: Option<KafkaString>) -> Self {
        self.error_message = value;
        self
    }
    pub fn with_current_leader(mut self, value: LeaderIdAndEpoch) -> Self {
        self.current_leader = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let index;
        let error_code;
        let base_offset;
        let log_append_time_ms;
        let mut log_start_offset = -1i64;
        let mut record_errors = Vec::new();
        let mut error_message = None;
        let mut current_leader = LeaderIdAndEpoch::default();
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        index = read_i32(buf)?;
        error_code = read_i16(buf)?;
        base_offset = read_i64(buf)?;
        log_append_time_ms = read_i64(buf)?;
        if version >= 5 {
            log_start_offset = read_i64(buf)?;
        }
        if version >= 8 {
            if version >= 9 {
                record_errors = {
                    let len = read_compact_array_length(buf)?;
                    let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
                    for _ in 0..len {
                        arr.push(BatchIndexAndErrorMessage::read(buf, version)?);
                    }
                    arr
                };
            } else {
                record_errors = {
                    let len = read_array_length(buf)?;
                    let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
                    for _ in 0..len {
                        arr.push(BatchIndexAndErrorMessage::read(buf, version)?);
                    }
                    arr
                };
            }
        }
        if version >= 8 {
            if version >= 9 {
                error_message = read_compact_nullable_string(buf)?;
            } else {
                error_message = read_nullable_string(buf)?;
            }
        }
        if version >= 9 {
            let tagged_fields = read_tagged_fields(buf)?;
            for field in &tagged_fields {
                match field.tag {
                    0 => {
                        if version >= 10 {
                            let mut tag_buf = field.data.clone();
                            current_leader = LeaderIdAndEpoch::read(&mut tag_buf, version)?;
                        }
                    },
                    _ => {
                        _unknown_tagged_fields.push(field.clone());
                    },
                }
            }
        }
        Ok(Self {
            index,
            error_code,
            base_offset,
            log_append_time_ms,
            log_start_offset,
            record_errors,
            error_message,
            current_leader,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        write_i32(buf, self.index);
        write_i16(buf, self.error_code);
        write_i64(buf, self.base_offset);
        write_i64(buf, self.log_append_time_ms);
        if version >= 5 {
            write_i64(buf, self.log_start_offset);
        } else if self.log_start_offset != -1i64 {
            return Err(UnsupportedFieldVersion::new(0, "log_start_offset", version).into());
        }
        if version >= 8 {
            if version >= 9 {
                write_compact_array_length(buf, self.record_errors.len() as i32);
                for el in &self.record_errors {
                    el.write(buf, version)?;
                }
            } else {
                write_array_length(buf, self.record_errors.len() as i32);
                for el in &self.record_errors {
                    el.write(buf, version)?;
                }
            }
        } else if self.record_errors != Vec::new() {
            return Err(UnsupportedFieldVersion::new(0, "record_errors", version).into());
        }
        if version >= 8 {
            if version >= 9 {
                write_compact_nullable_string(buf, self.error_message.as_ref())?;
            } else {
                write_nullable_string(buf, self.error_message.as_ref())?;
            }
        } else if self.error_message != None {
            return Err(UnsupportedFieldVersion::new(0, "error_message", version).into());
        }
        if version >= 9 {
            let mut known_tagged_fields: Vec<RawTaggedField> = Vec::new();
            if version >= 10 && self.current_leader != LeaderIdAndEpoch::default() {
                let mut tag_buf = BytesMut::new();
                self.current_leader.write(&mut tag_buf, version)?;
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
        len += 4;
        len += 2;
        len += 8;
        len += 8;
        if version >= 5 {
            len += 8;
        } else if self.log_start_offset != -1i64 {
            return Err(UnsupportedFieldVersion::new(0, "log_start_offset", version).into());
        }
        if version >= 8 {
            if version >= 9 {
                len += compact_array_length_len(self.record_errors.len() as i32);
                for el in &self.record_errors {
                    len += el.encoded_len(version)?;
                }
            } else {
                len += array_length_len();
                for el in &self.record_errors {
                    len += el.encoded_len(version)?;
                }
            }
        } else if self.record_errors != Vec::new() {
            return Err(UnsupportedFieldVersion::new(0, "record_errors", version).into());
        }
        if version >= 8 {
            if version >= 9 {
                len += compact_nullable_string_len(self.error_message.as_ref())?;
            } else {
                len += nullable_string_len(self.error_message.as_ref())?;
            }
        } else if self.error_message != None {
            return Err(UnsupportedFieldVersion::new(0, "error_message", version).into());
        }
        if version >= 9 {
            let mut known_tagged_fields: Vec<RawTaggedField> = Vec::new();
            if version >= 10 && self.current_leader != LeaderIdAndEpoch::default() {
                let mut tag_buf = BytesMut::new();
                self.current_leader.write(&mut tag_buf, version)?;
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
pub struct BatchIndexAndErrorMessage {
    /// The batch index of the record that caused the batch to be dropped.
    pub batch_index: i32,
    /// The error message of the record that caused the batch to be dropped.
    pub batch_index_error_message: Option<KafkaString>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for BatchIndexAndErrorMessage {
    fn default() -> Self {
        Self {
            batch_index: 0_i32,
            batch_index_error_message: None,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl BatchIndexAndErrorMessage {
    pub fn with_batch_index(mut self, value: i32) -> Self {
        self.batch_index = value;
        self
    }
    pub fn with_batch_index_error_message(mut self, value: Option<KafkaString>) -> Self {
        self.batch_index_error_message = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let batch_index;
        let batch_index_error_message;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        batch_index = read_i32(buf)?;
        if version >= 9 {
            batch_index_error_message = read_compact_nullable_string(buf)?;
        } else {
            batch_index_error_message = read_nullable_string(buf)?;
        }
        if version >= 9 {
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
            batch_index,
            batch_index_error_message,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        write_i32(buf, self.batch_index);
        if version >= 9 {
            write_compact_nullable_string(buf, self.batch_index_error_message.as_ref())?;
        } else {
            write_nullable_string(buf, self.batch_index_error_message.as_ref())?;
        }
        if version >= 9 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        let mut len: usize = 0;
        len += 4;
        if version >= 9 {
            len += compact_nullable_string_len(self.batch_index_error_message.as_ref())?;
        } else {
            len += nullable_string_len(self.batch_index_error_message.as_ref())?;
        }
        if version >= 9 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            len += tagged_fields_len(&all_tags)?;
        }
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct LeaderIdAndEpoch {
    /// The ID of the current leader or -1 if the leader is unknown.
    pub leader_id: i32,
    /// The latest known leader epoch.
    pub leader_epoch: i32,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for LeaderIdAndEpoch {
    fn default() -> Self {
        Self {
            leader_id: -1i32,
            leader_epoch: -1i32,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl LeaderIdAndEpoch {
    pub fn with_leader_id(mut self, value: i32) -> Self {
        self.leader_id = value;
        self
    }
    pub fn with_leader_epoch(mut self, value: i32) -> Self {
        self.leader_epoch = value;
        self
    }
    pub fn read(buf: &mut Bytes, _version: i16) -> Result<Self> {
        let leader_id;
        let leader_epoch;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        leader_id = read_i32(buf)?;
        leader_epoch = read_i32(buf)?;
        let tagged_fields = read_tagged_fields(buf)?;
        for field in &tagged_fields {
            match field.tag {
                _ => {
                    _unknown_tagged_fields.push(field.clone());
                },
            }
        }
        Ok(Self {
            leader_id,
            leader_epoch,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, _version: i16) -> Result<()> {
        write_i32(buf, self.leader_id);
        write_i32(buf, self.leader_epoch);
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
    pub fn encoded_len(&self, _version: i16) -> Result<usize> {
        let mut len: usize = 0;
        len += 4;
        len += 4;
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct NodeEndpoint {
    /// The ID of the associated node.
    pub node_id: i32,
    /// The node's hostname.
    pub host: KafkaString,
    /// The node's port.
    pub port: i32,
    /// The rack of the node, or null if it has not been assigned to a rack.
    pub rack: Option<KafkaString>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for NodeEndpoint {
    fn default() -> Self {
        Self {
            node_id: 0_i32,
            host: KafkaString::default(),
            port: 0_i32,
            rack: None,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl NodeEndpoint {
    pub fn with_node_id(mut self, value: i32) -> Self {
        self.node_id = value;
        self
    }
    pub fn with_host(mut self, value: KafkaString) -> Self {
        self.host = value;
        self
    }
    pub fn with_port(mut self, value: i32) -> Self {
        self.port = value;
        self
    }
    pub fn with_rack(mut self, value: Option<KafkaString>) -> Self {
        self.rack = value;
        self
    }
    pub fn read(buf: &mut Bytes, _version: i16) -> Result<Self> {
        let node_id;
        let host;
        let port;
        let rack;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        node_id = read_i32(buf)?;
        host = read_compact_string(buf)?;
        port = read_i32(buf)?;
        rack = read_compact_nullable_string(buf)?;
        let tagged_fields = read_tagged_fields(buf)?;
        for field in &tagged_fields {
            match field.tag {
                _ => {
                    _unknown_tagged_fields.push(field.clone());
                },
            }
        }
        Ok(Self {
            node_id,
            host,
            port,
            rack,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, _version: i16) -> Result<()> {
        write_i32(buf, self.node_id);
        write_compact_string(buf, &self.host)?;
        write_i32(buf, self.port);
        write_compact_nullable_string(buf, self.rack.as_ref())?;
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
    pub fn encoded_len(&self, _version: i16) -> Result<usize> {
        let mut len: usize = 0;
        len += 4;
        len += compact_string_len(&self.host)?;
        len += 4;
        len += compact_nullable_string_len(self.rack.as_ref())?;
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
