//! Generated from FetchResponse.json - DO NOT EDIT
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
pub struct FetchResponseData {
    /// The duration in milliseconds for which the request was throttled due to a quota violation,
    /// or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// The top level response error code.
    pub error_code: i16,
    /// The fetch session ID, or 0 if this is not part of a fetch session.
    pub session_id: i32,
    /// The response topics.
    pub responses: Vec<FetchableTopicResponse>,
    /// Endpoints for all current-leaders enumerated in PartitionData, with errors
    /// NOT_LEADER_OR_FOLLOWER & FENCED_LEADER_EPOCH.
    pub node_endpoints: Vec<NodeEndpoint>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for FetchResponseData {
    fn default() -> Self {
        Self {
            throttle_time_ms: 0_i32,
            error_code: 0_i16,
            session_id: 0i32,
            responses: Vec::new(),
            node_endpoints: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl FetchResponseData {
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 4 || version > 18 {
            return Err(UnsupportedVersion::new(1, version).into());
        }
        let throttle_time_ms;
        let mut error_code = 0_i16;
        let mut session_id = 0i32;
        let responses;
        let mut node_endpoints = Vec::new();
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        throttle_time_ms = read_i32(buf)?;
        if version >= 7 {
            error_code = read_i16(buf)?;
        }
        if version >= 7 {
            session_id = read_i32(buf)?;
        }
        if version >= 12 {
            responses = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(FetchableTopicResponse::read(buf, version)?);
                }
                arr
            };
        } else {
            responses = {
                let len = read_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(FetchableTopicResponse::read(buf, version)?);
                }
                arr
            };
        }
        if version >= 12 {
            let tagged_fields = read_tagged_fields(buf)?;
            for field in &tagged_fields {
                match field.tag {
                    0 => {
                        if version >= 16 {
                            let mut tag_buf = field.data.clone();
                            node_endpoints = {
                                let len = read_compact_array_length(&mut tag_buf)?;
                                let mut arr = Vec::with_capacity(len.max(0) as usize);
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
            throttle_time_ms,
            error_code,
            session_id,
            responses,
            node_endpoints,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 4 || version > 18 {
            return Err(UnsupportedVersion::new(1, version).into());
        }
        write_i32(buf, self.throttle_time_ms);
        if version >= 7 {
            write_i16(buf, self.error_code);
        }
        if version >= 7 {
            write_i32(buf, self.session_id);
        }
        if version >= 12 {
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
        if version >= 12 {
            let mut known_tagged_fields: Vec<RawTaggedField> = Vec::new();
            if version >= 16 && !self.node_endpoints.is_empty() {
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
}
#[derive(Debug, Clone, PartialEq)]
pub struct FetchableTopicResponse {
    /// The topic name.
    pub topic: KafkaString,
    /// The unique topic ID.
    pub topic_id: KafkaUuid,
    /// The topic partitions.
    pub partitions: Vec<PartitionData>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for FetchableTopicResponse {
    fn default() -> Self {
        Self {
            topic: KafkaString::default(),
            topic_id: KafkaUuid::ZERO,
            partitions: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl FetchableTopicResponse {
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
                    arr.push(PartitionData::read(buf, version)?);
                }
                arr
            };
        } else {
            partitions = {
                let len = read_array_length(buf)?;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(PartitionData::read(buf, version)?);
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
        }
        if version >= 13 {
            write_uuid(buf, &self.topic_id);
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
}
#[derive(Debug, Clone, PartialEq)]
pub struct PartitionData {
    /// The partition index.
    pub partition_index: i32,
    /// The error code, or 0 if there was no fetch error.
    pub error_code: i16,
    /// The current high water mark.
    pub high_watermark: i64,
    /// The last stable offset (or LSO) of the partition. This is the last offset such that the
    /// state of all transactional records prior to this offset have been decided (ABORTED or
    /// COMMITTED).
    pub last_stable_offset: i64,
    /// The current log start offset.
    pub log_start_offset: i64,
    /// In case divergence is detected based on the `LastFetchedEpoch` and `FetchOffset` in the
    /// request, this field indicates the largest epoch and its end offset such that subsequent
    /// records are known to diverge.
    pub diverging_epoch: EpochEndOffset,
    /// The current leader of the partition.
    pub current_leader: LeaderIdAndEpoch,
    /// In the case of fetching an offset less than the LogStartOffset, this is the end offset and
    /// epoch that should be used in the FetchSnapshot request.
    pub snapshot_id: SnapshotId,
    /// The aborted transactions.
    pub aborted_transactions: Option<Vec<AbortedTransaction>>,
    /// The preferred read replica for the consumer to use on its next fetch request.
    pub preferred_read_replica: i32,
    /// The record data.
    pub records: Option<Bytes>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for PartitionData {
    fn default() -> Self {
        Self {
            partition_index: 0_i32,
            error_code: 0_i16,
            high_watermark: 0_i64,
            last_stable_offset: -1i64,
            log_start_offset: -1i64,
            diverging_epoch: EpochEndOffset::default(),
            current_leader: LeaderIdAndEpoch::default(),
            snapshot_id: SnapshotId::default(),
            aborted_transactions: None,
            preferred_read_replica: -1i32,
            records: None,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl PartitionData {
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let partition_index;
        let error_code;
        let high_watermark;
        let last_stable_offset;
        let mut log_start_offset = -1i64;
        let mut diverging_epoch = EpochEndOffset::default();
        let mut current_leader = LeaderIdAndEpoch::default();
        let mut snapshot_id = SnapshotId::default();
        let aborted_transactions;
        let mut preferred_read_replica = -1i32;
        let records;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        partition_index = read_i32(buf)?;
        error_code = read_i16(buf)?;
        high_watermark = read_i64(buf)?;
        last_stable_offset = read_i64(buf)?;
        if version >= 5 {
            log_start_offset = read_i64(buf)?;
        }
        if version >= 12 {
            aborted_transactions = {
                let len = read_compact_array_length(buf)?;
                if len < 0 {
                    None
                } else {
                    let mut arr = Vec::with_capacity(len as usize);
                    for _ in 0..len {
                        arr.push(AbortedTransaction::read(buf, version)?);
                    }
                    Some(arr)
                }
            };
        } else {
            aborted_transactions = {
                let len = read_array_length(buf)?;
                if len < 0 {
                    None
                } else {
                    let mut arr = Vec::with_capacity(len as usize);
                    for _ in 0..len {
                        arr.push(AbortedTransaction::read(buf, version)?);
                    }
                    Some(arr)
                }
            };
        }
        if version >= 11 {
            preferred_read_replica = read_i32(buf)?;
        }
        if version >= 12 {
            records = read_compact_nullable_bytes(buf)?;
        } else {
            records = read_nullable_bytes(buf)?;
        }
        if version >= 12 {
            let tagged_fields = read_tagged_fields(buf)?;
            for field in &tagged_fields {
                match field.tag {
                    0 => {
                        let mut tag_buf = field.data.clone();
                        diverging_epoch = EpochEndOffset::read(&mut tag_buf, version)?;
                    },
                    1 => {
                        let mut tag_buf = field.data.clone();
                        current_leader = LeaderIdAndEpoch::read(&mut tag_buf, version)?;
                    },
                    2 => {
                        let mut tag_buf = field.data.clone();
                        snapshot_id = SnapshotId::read(&mut tag_buf, version)?;
                    },
                    _ => {
                        _unknown_tagged_fields.push(field.clone());
                    },
                }
            }
        }
        Ok(Self {
            partition_index,
            error_code,
            high_watermark,
            last_stable_offset,
            log_start_offset,
            diverging_epoch,
            current_leader,
            snapshot_id,
            aborted_transactions,
            preferred_read_replica,
            records,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        write_i32(buf, self.partition_index);
        write_i16(buf, self.error_code);
        write_i64(buf, self.high_watermark);
        write_i64(buf, self.last_stable_offset);
        if version >= 5 {
            write_i64(buf, self.log_start_offset);
        }
        if version >= 12 {
            match &self.aborted_transactions {
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
            match &self.aborted_transactions {
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
        if version >= 11 {
            write_i32(buf, self.preferred_read_replica);
        }
        if version >= 12 {
            write_compact_nullable_bytes(buf, self.records.as_ref().map(|b| b.as_ref()))?;
        } else {
            write_nullable_bytes(buf, self.records.as_ref().map(|b| b.as_ref()))?;
        }
        if version >= 12 {
            let mut known_tagged_fields: Vec<RawTaggedField> = Vec::new();
            if self.diverging_epoch != EpochEndOffset::default() {
                let mut tag_buf = BytesMut::new();
                self.diverging_epoch.write(&mut tag_buf, version)?;
                known_tagged_fields.push(RawTaggedField {
                    tag: 0,
                    data: tag_buf.freeze(),
                });
            }
            if self.current_leader != LeaderIdAndEpoch::default() {
                let mut tag_buf = BytesMut::new();
                self.current_leader.write(&mut tag_buf, version)?;
                known_tagged_fields.push(RawTaggedField {
                    tag: 1,
                    data: tag_buf.freeze(),
                });
            }
            if self.snapshot_id != SnapshotId::default() {
                let mut tag_buf = BytesMut::new();
                self.snapshot_id.write(&mut tag_buf, version)?;
                known_tagged_fields.push(RawTaggedField {
                    tag: 2,
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
}
#[derive(Debug, Clone, PartialEq)]
pub struct EpochEndOffset {
    /// The largest epoch.
    pub epoch: i32,
    /// The end offset of the epoch.
    pub end_offset: i64,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for EpochEndOffset {
    fn default() -> Self {
        Self {
            epoch: -1i32,
            end_offset: -1i64,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl EpochEndOffset {
    pub fn read(buf: &mut Bytes, _version: i16) -> Result<Self> {
        let epoch;
        let end_offset;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        epoch = read_i32(buf)?;
        end_offset = read_i64(buf)?;
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
            end_offset,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, _version: i16) -> Result<()> {
        write_i32(buf, self.epoch);
        write_i64(buf, self.end_offset);
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
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
}
#[derive(Debug, Clone, PartialEq)]
pub struct SnapshotId {
    /// The end offset of the epoch.
    pub end_offset: i64,
    /// The largest epoch.
    pub epoch: i32,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for SnapshotId {
    fn default() -> Self {
        Self {
            end_offset: -1i64,
            epoch: -1i32,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl SnapshotId {
    pub fn read(buf: &mut Bytes, _version: i16) -> Result<Self> {
        let end_offset;
        let epoch;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        end_offset = read_i64(buf)?;
        epoch = read_i32(buf)?;
        let tagged_fields = read_tagged_fields(buf)?;
        for field in &tagged_fields {
            match field.tag {
                _ => {
                    _unknown_tagged_fields.push(field.clone());
                },
            }
        }
        Ok(Self {
            end_offset,
            epoch,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, _version: i16) -> Result<()> {
        write_i64(buf, self.end_offset);
        write_i32(buf, self.epoch);
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct AbortedTransaction {
    /// The producer id associated with the aborted transaction.
    pub producer_id: i64,
    /// The first offset in the aborted transaction.
    pub first_offset: i64,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for AbortedTransaction {
    fn default() -> Self {
        Self {
            producer_id: 0_i64,
            first_offset: 0_i64,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl AbortedTransaction {
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let producer_id;
        let first_offset;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        producer_id = read_i64(buf)?;
        first_offset = read_i64(buf)?;
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
            producer_id,
            first_offset,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        write_i64(buf, self.producer_id);
        write_i64(buf, self.first_offset);
        if version >= 12 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
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
}
