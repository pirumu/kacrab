//! Generated from FetchRequest.json - DO NOT EDIT
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
pub struct FetchRequestData {
    /// The clusterId if known. This is used to validate metadata fetches prior to broker
    /// registration.
    pub cluster_id: Option<KafkaString>,
    /// The broker ID of the follower, of -1 if this request is from a consumer.
    pub replica_id: i32,
    /// The state of the replica in the follower.
    pub replica_state: ReplicaState,
    /// The maximum time in milliseconds to wait for the response.
    pub max_wait_ms: i32,
    /// The minimum bytes to accumulate in the response.
    pub min_bytes: i32,
    /// The maximum bytes to fetch.  See KIP-74 for cases where this limit may not be honored.
    pub max_bytes: i32,
    /// This setting controls the visibility of transactional records. Using READ_UNCOMMITTED
    /// (isolation_level = 0) makes all records visible. With READ_COMMITTED (isolation_level = 1),
    /// non-transactional and COMMITTED transactional records are visible. To be more concrete,
    /// READ_COMMITTED returns all data from offsets smaller than the current LSO (last stable
    /// offset), and enables the inclusion of the list of aborted transactions in the result, which
    /// allows consumers to discard ABORTED transactional records.
    pub isolation_level: i8,
    /// The fetch session ID.
    pub session_id: i32,
    /// The fetch session epoch, which is used for ordering requests in a session.
    pub session_epoch: i32,
    /// The topics to fetch.
    pub topics: Vec<FetchTopic>,
    /// In an incremental fetch request, the partitions to remove.
    pub forgotten_topics_data: Vec<ForgottenTopic>,
    /// Rack ID of the consumer making this request.
    pub rack_id: KafkaString,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for FetchRequestData {
    fn default() -> Self {
        Self {
            cluster_id: None,
            replica_id: -1i32,
            replica_state: ReplicaState::default(),
            max_wait_ms: 0_i32,
            min_bytes: 0_i32,
            max_bytes: i32::MAX,
            isolation_level: 0i8,
            session_id: 0i32,
            session_epoch: -1i32,
            topics: Vec::new(),
            forgotten_topics_data: Vec::new(),
            rack_id: KafkaString::default(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl FetchRequestData {
    pub fn with_cluster_id(mut self, value: Option<KafkaString>) -> Self {
        self.cluster_id = value;
        self
    }
    pub fn with_replica_id(mut self, value: i32) -> Self {
        self.replica_id = value;
        self
    }
    pub fn with_replica_state(mut self, value: ReplicaState) -> Self {
        self.replica_state = value;
        self
    }
    pub fn with_max_wait_ms(mut self, value: i32) -> Self {
        self.max_wait_ms = value;
        self
    }
    pub fn with_min_bytes(mut self, value: i32) -> Self {
        self.min_bytes = value;
        self
    }
    pub fn with_max_bytes(mut self, value: i32) -> Self {
        self.max_bytes = value;
        self
    }
    pub fn with_isolation_level(mut self, value: i8) -> Self {
        self.isolation_level = value;
        self
    }
    pub fn with_session_id(mut self, value: i32) -> Self {
        self.session_id = value;
        self
    }
    pub fn with_session_epoch(mut self, value: i32) -> Self {
        self.session_epoch = value;
        self
    }
    pub fn with_topics(mut self, value: Vec<FetchTopic>) -> Self {
        self.topics = value;
        self
    }
    pub fn with_forgotten_topics_data(mut self, value: Vec<ForgottenTopic>) -> Self {
        self.forgotten_topics_data = value;
        self
    }
    pub fn with_rack_id(mut self, value: KafkaString) -> Self {
        self.rack_id = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 4 || version > 18 {
            return Err(UnsupportedVersion::new(1, version).into());
        }
        let mut cluster_id = None;
        let mut replica_id = -1i32;
        let mut replica_state = ReplicaState::default();
        let max_wait_ms;
        let min_bytes;
        let max_bytes;
        let isolation_level;
        let mut session_id = 0i32;
        let mut session_epoch = -1i32;
        let topics;
        let mut forgotten_topics_data = Vec::new();
        let mut rack_id = KafkaString::default();
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version <= 14 {
            replica_id = read_i32(buf)?;
        }
        max_wait_ms = read_i32(buf)?;
        min_bytes = read_i32(buf)?;
        max_bytes = read_i32(buf)?;
        isolation_level = read_i8(buf)?;
        if version >= 7 {
            session_id = read_i32(buf)?;
        }
        if version >= 7 {
            session_epoch = read_i32(buf)?;
        }
        if version >= 12 {
            topics = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(FetchTopic::read(buf, version)?);
                }
                arr
            };
        } else {
            topics = {
                let len = read_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(FetchTopic::read(buf, version)?);
                }
                arr
            };
        }
        if version >= 7 {
            if version >= 12 {
                forgotten_topics_data = {
                    let len = read_compact_array_length(buf)?;
                    let mut arr = Vec::with_capacity(len.max(0) as usize);
                    for _ in 0..len {
                        arr.push(ForgottenTopic::read(buf, version)?);
                    }
                    arr
                };
            } else {
                forgotten_topics_data = {
                    let len = read_array_length(buf)?;
                    let mut arr = Vec::with_capacity(len.max(0) as usize);
                    for _ in 0..len {
                        arr.push(ForgottenTopic::read(buf, version)?);
                    }
                    arr
                };
            }
        }
        if version >= 11 {
            if version >= 12 {
                rack_id = read_compact_string(buf)?;
            } else {
                rack_id = read_string(buf)?;
            }
        }
        if version >= 12 {
            let tagged_fields = read_tagged_fields(buf)?;
            for field in &tagged_fields {
                match field.tag {
                    0 => {
                        let mut tag_buf = field.data.clone();
                        cluster_id = read_compact_nullable_string(&mut tag_buf)?;
                    },
                    1 => {
                        if version >= 15 {
                            let mut tag_buf = field.data.clone();
                            replica_state = ReplicaState::read(&mut tag_buf, version)?;
                        }
                    },
                    _ => {
                        _unknown_tagged_fields.push(field.clone());
                    },
                }
            }
        }
        Ok(Self {
            cluster_id,
            replica_id,
            replica_state,
            max_wait_ms,
            min_bytes,
            max_bytes,
            isolation_level,
            session_id,
            session_epoch,
            topics,
            forgotten_topics_data,
            rack_id,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 4 || version > 18 {
            return Err(UnsupportedVersion::new(1, version).into());
        }
        if version <= 14 {
            write_i32(buf, self.replica_id);
        } else if self.replica_id != -1i32 {
            return Err(UnsupportedFieldVersion::new(1, "replica_id", version).into());
        }
        write_i32(buf, self.max_wait_ms);
        write_i32(buf, self.min_bytes);
        write_i32(buf, self.max_bytes);
        write_i8(buf, self.isolation_level);
        if version >= 7 {
            write_i32(buf, self.session_id);
        } else if self.session_id != 0i32 {
            return Err(UnsupportedFieldVersion::new(1, "session_id", version).into());
        }
        if version >= 7 {
            write_i32(buf, self.session_epoch);
        } else if self.session_epoch != -1i32 {
            return Err(UnsupportedFieldVersion::new(1, "session_epoch", version).into());
        }
        if version >= 12 {
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
        if version >= 7 {
            if version >= 12 {
                write_compact_array_length(buf, self.forgotten_topics_data.len() as i32);
                for el in &self.forgotten_topics_data {
                    el.write(buf, version)?;
                }
            } else {
                write_array_length(buf, self.forgotten_topics_data.len() as i32);
                for el in &self.forgotten_topics_data {
                    el.write(buf, version)?;
                }
            }
        } else if self.forgotten_topics_data != Vec::new() {
            return Err(UnsupportedFieldVersion::new(1, "forgotten_topics_data", version).into());
        }
        if version >= 11 {
            if version >= 12 {
                write_compact_string(buf, &self.rack_id)?;
            } else {
                write_string(buf, &self.rack_id)?;
            }
        } else if self.rack_id != KafkaString::default() {
            return Err(UnsupportedFieldVersion::new(1, "rack_id", version).into());
        }
        if version >= 12 {
            let mut known_tagged_fields: Vec<RawTaggedField> = Vec::new();
            if self.cluster_id.is_some() {
                let mut tag_buf = BytesMut::new();
                write_compact_nullable_string(&mut tag_buf, self.cluster_id.as_ref())?;
                known_tagged_fields.push(RawTaggedField {
                    tag: 0,
                    data: tag_buf.freeze(),
                });
            }
            if version >= 15 && self.replica_state != ReplicaState::default() {
                let mut tag_buf = BytesMut::new();
                self.replica_state.write(&mut tag_buf, version)?;
                known_tagged_fields.push(RawTaggedField {
                    tag: 1,
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
        if version < 4 || version > 18 {
            return Err(UnsupportedVersion::new(1, version).into());
        }
        let mut len: usize = 0;
        if version <= 14 {
            len += 4;
        } else if self.replica_id != -1i32 {
            return Err(UnsupportedFieldVersion::new(1, "replica_id", version).into());
        }
        len += 4;
        len += 4;
        len += 4;
        len += 1;
        if version >= 7 {
            len += 4;
        } else if self.session_id != 0i32 {
            return Err(UnsupportedFieldVersion::new(1, "session_id", version).into());
        }
        if version >= 7 {
            len += 4;
        } else if self.session_epoch != -1i32 {
            return Err(UnsupportedFieldVersion::new(1, "session_epoch", version).into());
        }
        if version >= 12 {
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
        if version >= 7 {
            if version >= 12 {
                len += compact_array_length_len(self.forgotten_topics_data.len() as i32);
                for el in &self.forgotten_topics_data {
                    len += el.encoded_len(version)?;
                }
            } else {
                len += array_length_len();
                for el in &self.forgotten_topics_data {
                    len += el.encoded_len(version)?;
                }
            }
        } else if self.forgotten_topics_data != Vec::new() {
            return Err(UnsupportedFieldVersion::new(1, "forgotten_topics_data", version).into());
        }
        if version >= 11 {
            if version >= 12 {
                len += compact_string_len(&self.rack_id)?;
            } else {
                len += string_len(&self.rack_id)?;
            }
        } else if self.rack_id != KafkaString::default() {
            return Err(UnsupportedFieldVersion::new(1, "rack_id", version).into());
        }
        if version >= 12 {
            let mut known_tagged_fields: Vec<RawTaggedField> = Vec::new();
            if self.cluster_id.is_some() {
                let mut tag_buf = BytesMut::new();
                write_compact_nullable_string(&mut tag_buf, self.cluster_id.as_ref())?;
                known_tagged_fields.push(RawTaggedField {
                    tag: 0,
                    data: tag_buf.freeze(),
                });
            }
            if version >= 15 && self.replica_state != ReplicaState::default() {
                let mut tag_buf = BytesMut::new();
                self.replica_state.write(&mut tag_buf, version)?;
                known_tagged_fields.push(RawTaggedField {
                    tag: 1,
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
pub struct ReplicaState {
    /// The replica ID of the follower, or -1 if this request is from a consumer.
    pub replica_id: i32,
    /// The epoch of this follower, or -1 if not available.
    pub replica_epoch: i64,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for ReplicaState {
    fn default() -> Self {
        Self {
            replica_id: -1i32,
            replica_epoch: -1i64,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl ReplicaState {
    pub fn with_replica_id(mut self, value: i32) -> Self {
        self.replica_id = value;
        self
    }
    pub fn with_replica_epoch(mut self, value: i64) -> Self {
        self.replica_epoch = value;
        self
    }
    pub fn read(buf: &mut Bytes, _version: i16) -> Result<Self> {
        let replica_id;
        let replica_epoch;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        replica_id = read_i32(buf)?;
        replica_epoch = read_i64(buf)?;
        let tagged_fields = read_tagged_fields(buf)?;
        for field in &tagged_fields {
            match field.tag {
                _ => {
                    _unknown_tagged_fields.push(field.clone());
                },
            }
        }
        Ok(Self {
            replica_id,
            replica_epoch,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, _version: i16) -> Result<()> {
        write_i32(buf, self.replica_id);
        write_i64(buf, self.replica_epoch);
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
    pub fn encoded_len(&self, _version: i16) -> Result<usize> {
        let mut len: usize = 0;
        len += 4;
        len += 8;
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct FetchTopic {
    /// The name of the topic to fetch.
    pub topic: KafkaString,
    /// The unique topic ID.
    pub topic_id: KafkaUuid,
    /// The partitions to fetch.
    pub partitions: Vec<FetchPartition>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for FetchTopic {
    fn default() -> Self {
        Self {
            topic: KafkaString::default(),
            topic_id: KafkaUuid::ZERO,
            partitions: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl FetchTopic {
    pub fn with_topic(mut self, value: KafkaString) -> Self {
        self.topic = value;
        self
    }
    pub fn with_topic_id(mut self, value: KafkaUuid) -> Self {
        self.topic_id = value;
        self
    }
    pub fn with_partitions(mut self, value: Vec<FetchPartition>) -> Self {
        self.partitions = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let mut topic = KafkaString::default();
        let mut topic_id = KafkaUuid::ZERO;
        let partitions;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version <= 12 {
            if version >= 12 {
                topic = read_compact_string(buf)?;
            } else {
                topic = read_string(buf)?;
            }
        }
        if version >= 13 {
            topic_id = read_uuid(buf)?;
        }
        if version >= 12 {
            partitions = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(FetchPartition::read(buf, version)?);
                }
                arr
            };
        } else {
            partitions = {
                let len = read_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(FetchPartition::read(buf, version)?);
                }
                arr
            };
        }
        if version >= 12 {
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
            topic_id,
            partitions,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version <= 12 {
            if version >= 12 {
                write_compact_string(buf, &self.topic)?;
            } else {
                write_string(buf, &self.topic)?;
            }
        } else if self.topic != KafkaString::default() {
            return Err(UnsupportedFieldVersion::new(1, "topic", version).into());
        }
        if version >= 13 {
            write_uuid(buf, &self.topic_id);
        } else if self.topic_id != KafkaUuid::ZERO {
            return Err(UnsupportedFieldVersion::new(1, "topic_id", version).into());
        }
        if version >= 12 {
            write_compact_array_length(buf, self.partitions.len() as i32);
            for el in &self.partitions {
                el.write(buf, version)?;
            }
        } else {
            write_array_length(buf, self.partitions.len() as i32);
            for el in &self.partitions {
                el.write(buf, version)?;
            }
        }
        if version >= 12 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        let mut len: usize = 0;
        if version <= 12 {
            if version >= 12 {
                len += compact_string_len(&self.topic)?;
            } else {
                len += string_len(&self.topic)?;
            }
        } else if self.topic != KafkaString::default() {
            return Err(UnsupportedFieldVersion::new(1, "topic", version).into());
        }
        if version >= 13 {
            len += 16;
        } else if self.topic_id != KafkaUuid::ZERO {
            return Err(UnsupportedFieldVersion::new(1, "topic_id", version).into());
        }
        if version >= 12 {
            len += compact_array_length_len(self.partitions.len() as i32);
            for el in &self.partitions {
                len += el.encoded_len(version)?;
            }
        } else {
            len += array_length_len();
            for el in &self.partitions {
                len += el.encoded_len(version)?;
            }
        }
        if version >= 12 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            len += tagged_fields_len(&all_tags)?;
        }
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct FetchPartition {
    /// The partition index.
    pub partition: i32,
    /// The current leader epoch of the partition.
    pub current_leader_epoch: i32,
    /// The message offset.
    pub fetch_offset: i64,
    /// The epoch of the last fetched record or -1 if there is none.
    pub last_fetched_epoch: i32,
    /// The earliest available offset of the follower replica.  The field is only used when the
    /// request is sent by the follower.
    pub log_start_offset: i64,
    /// The maximum bytes to fetch from this partition.  See KIP-74 for cases where this limit may
    /// not be honored.
    pub partition_max_bytes: i32,
    /// The directory id of the follower fetching.
    pub replica_directory_id: KafkaUuid,
    /// The high-watermark known by the replica. -1 if the high-watermark is not known and
    /// 9223372036854775807 if the feature is not supported.
    pub high_watermark: i64,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for FetchPartition {
    fn default() -> Self {
        Self {
            partition: 0_i32,
            current_leader_epoch: -1i32,
            fetch_offset: 0_i64,
            last_fetched_epoch: -1i32,
            log_start_offset: -1i64,
            partition_max_bytes: 0_i32,
            replica_directory_id: KafkaUuid::ZERO,
            high_watermark: i64::MAX,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl FetchPartition {
    pub fn with_partition(mut self, value: i32) -> Self {
        self.partition = value;
        self
    }
    pub fn with_current_leader_epoch(mut self, value: i32) -> Self {
        self.current_leader_epoch = value;
        self
    }
    pub fn with_fetch_offset(mut self, value: i64) -> Self {
        self.fetch_offset = value;
        self
    }
    pub fn with_last_fetched_epoch(mut self, value: i32) -> Self {
        self.last_fetched_epoch = value;
        self
    }
    pub fn with_log_start_offset(mut self, value: i64) -> Self {
        self.log_start_offset = value;
        self
    }
    pub fn with_partition_max_bytes(mut self, value: i32) -> Self {
        self.partition_max_bytes = value;
        self
    }
    pub fn with_replica_directory_id(mut self, value: KafkaUuid) -> Self {
        self.replica_directory_id = value;
        self
    }
    pub fn with_high_watermark(mut self, value: i64) -> Self {
        self.high_watermark = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let partition;
        let mut current_leader_epoch = -1i32;
        let fetch_offset;
        let mut last_fetched_epoch = -1i32;
        let mut log_start_offset = -1i64;
        let partition_max_bytes;
        let mut replica_directory_id = KafkaUuid::ZERO;
        let mut high_watermark = i64::MAX;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        partition = read_i32(buf)?;
        if version >= 9 {
            current_leader_epoch = read_i32(buf)?;
        }
        fetch_offset = read_i64(buf)?;
        if version >= 12 {
            last_fetched_epoch = read_i32(buf)?;
        }
        if version >= 5 {
            log_start_offset = read_i64(buf)?;
        }
        partition_max_bytes = read_i32(buf)?;
        if version >= 12 {
            let tagged_fields = read_tagged_fields(buf)?;
            for field in &tagged_fields {
                match field.tag {
                    0 => {
                        if version >= 17 {
                            let mut tag_buf = field.data.clone();
                            replica_directory_id = read_uuid(&mut tag_buf)?;
                        }
                    },
                    1 => {
                        if version >= 18 {
                            let mut tag_buf = field.data.clone();
                            high_watermark = read_i64(&mut tag_buf)?;
                        }
                    },
                    _ => {
                        _unknown_tagged_fields.push(field.clone());
                    },
                }
            }
        }
        Ok(Self {
            partition,
            current_leader_epoch,
            fetch_offset,
            last_fetched_epoch,
            log_start_offset,
            partition_max_bytes,
            replica_directory_id,
            high_watermark,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        write_i32(buf, self.partition);
        if version >= 9 {
            write_i32(buf, self.current_leader_epoch);
        } else if self.current_leader_epoch != -1i32 {
            return Err(UnsupportedFieldVersion::new(1, "current_leader_epoch", version).into());
        }
        write_i64(buf, self.fetch_offset);
        if version >= 12 {
            write_i32(buf, self.last_fetched_epoch);
        } else if self.last_fetched_epoch != -1i32 {
            return Err(UnsupportedFieldVersion::new(1, "last_fetched_epoch", version).into());
        }
        if version >= 5 {
            write_i64(buf, self.log_start_offset);
        } else if self.log_start_offset != -1i64 {
            return Err(UnsupportedFieldVersion::new(1, "log_start_offset", version).into());
        }
        write_i32(buf, self.partition_max_bytes);
        if version >= 12 {
            let mut known_tagged_fields: Vec<RawTaggedField> = Vec::new();
            if version >= 17 && !self.replica_directory_id.is_nil() {
                let mut tag_buf = BytesMut::new();
                write_uuid(&mut tag_buf, &self.replica_directory_id);
                known_tagged_fields.push(RawTaggedField {
                    tag: 0,
                    data: tag_buf.freeze(),
                });
            }
            if version >= 18 && self.high_watermark != 9223372036854775807_i64 {
                let mut tag_buf = BytesMut::new();
                write_i64(&mut tag_buf, self.high_watermark);
                known_tagged_fields.push(RawTaggedField {
                    tag: 1,
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
        if version >= 9 {
            len += 4;
        } else if self.current_leader_epoch != -1i32 {
            return Err(UnsupportedFieldVersion::new(1, "current_leader_epoch", version).into());
        }
        len += 8;
        if version >= 12 {
            len += 4;
        } else if self.last_fetched_epoch != -1i32 {
            return Err(UnsupportedFieldVersion::new(1, "last_fetched_epoch", version).into());
        }
        if version >= 5 {
            len += 8;
        } else if self.log_start_offset != -1i64 {
            return Err(UnsupportedFieldVersion::new(1, "log_start_offset", version).into());
        }
        len += 4;
        if version >= 12 {
            let mut known_tagged_fields: Vec<RawTaggedField> = Vec::new();
            if version >= 17 && !self.replica_directory_id.is_nil() {
                let mut tag_buf = BytesMut::new();
                write_uuid(&mut tag_buf, &self.replica_directory_id);
                known_tagged_fields.push(RawTaggedField {
                    tag: 0,
                    data: tag_buf.freeze(),
                });
            }
            if version >= 18 && self.high_watermark != 9223372036854775807_i64 {
                let mut tag_buf = BytesMut::new();
                write_i64(&mut tag_buf, self.high_watermark);
                known_tagged_fields.push(RawTaggedField {
                    tag: 1,
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
pub struct ForgottenTopic {
    /// The topic name.
    pub topic: KafkaString,
    /// The unique topic ID.
    pub topic_id: KafkaUuid,
    /// The partitions indexes to forget.
    pub partitions: Vec<i32>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for ForgottenTopic {
    fn default() -> Self {
        Self {
            topic: KafkaString::default(),
            topic_id: KafkaUuid::ZERO,
            partitions: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl ForgottenTopic {
    pub fn with_topic(mut self, value: KafkaString) -> Self {
        self.topic = value;
        self
    }
    pub fn with_topic_id(mut self, value: KafkaUuid) -> Self {
        self.topic_id = value;
        self
    }
    pub fn with_partitions(mut self, value: Vec<i32>) -> Self {
        self.partitions = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let mut topic = KafkaString::default();
        let mut topic_id = KafkaUuid::ZERO;
        let partitions;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version <= 12 {
            if version >= 12 {
                topic = read_compact_string(buf)?;
            } else {
                topic = read_string(buf)?;
            }
        }
        if version >= 13 {
            topic_id = read_uuid(buf)?;
        }
        if version >= 12 {
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
        if version >= 12 {
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
            topic_id,
            partitions,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version <= 12 {
            if version >= 12 {
                write_compact_string(buf, &self.topic)?;
            } else {
                write_string(buf, &self.topic)?;
            }
        } else if self.topic != KafkaString::default() {
            return Err(UnsupportedFieldVersion::new(1, "topic", version).into());
        }
        if version >= 13 {
            write_uuid(buf, &self.topic_id);
        } else if self.topic_id != KafkaUuid::ZERO {
            return Err(UnsupportedFieldVersion::new(1, "topic_id", version).into());
        }
        if version >= 12 {
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
        if version >= 12 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        let mut len: usize = 0;
        if version <= 12 {
            if version >= 12 {
                len += compact_string_len(&self.topic)?;
            } else {
                len += string_len(&self.topic)?;
            }
        } else if self.topic != KafkaString::default() {
            return Err(UnsupportedFieldVersion::new(1, "topic", version).into());
        }
        if version >= 13 {
            len += 16;
        } else if self.topic_id != KafkaUuid::ZERO {
            return Err(UnsupportedFieldVersion::new(1, "topic_id", version).into());
        }
        if version >= 12 {
            len += compact_array_length_len(self.partitions.len() as i32);
            len += self.partitions.len() * 4usize;
        } else {
            len += array_length_len();
            len += self.partitions.len() * 4usize;
        }
        if version >= 12 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            len += tagged_fields_len(&all_tags)?;
        }
        Ok(len)
    }
}
