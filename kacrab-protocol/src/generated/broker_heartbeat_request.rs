//! Generated from BrokerHeartbeatRequest.json - DO NOT EDIT
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
pub struct BrokerHeartbeatRequestData {
    /// The broker ID.
    pub broker_id: i32,
    /// The broker epoch.
    pub broker_epoch: i64,
    /// The highest metadata offset which the broker has reached.
    pub current_metadata_offset: i64,
    /// True if the broker wants to be fenced, false otherwise.
    pub want_fence: bool,
    /// True if the broker wants to be shut down, false otherwise.
    pub want_shut_down: bool,
    /// Log directories that failed and went offline.
    pub offline_log_dirs: Vec<KafkaUuid>,
    /// List of log directories that are cordoned. This is null before the broker reaches the
    /// RECOVERY state.
    pub cordoned_log_dirs: Option<Vec<KafkaUuid>>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for BrokerHeartbeatRequestData {
    fn default() -> Self {
        Self {
            broker_id: 0_i32,
            broker_epoch: -1i64,
            current_metadata_offset: 0_i64,
            want_fence: false,
            want_shut_down: false,
            offline_log_dirs: Vec::new(),
            cordoned_log_dirs: None,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl BrokerHeartbeatRequestData {
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 0 || version > 2 {
            return Err(UnsupportedVersion::new(63, version).into());
        }
        let broker_id;
        let broker_epoch;
        let current_metadata_offset;
        let want_fence;
        let want_shut_down;
        let mut offline_log_dirs = Vec::new();
        let mut cordoned_log_dirs = None;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        broker_id = read_i32(buf)?;
        broker_epoch = read_i64(buf)?;
        current_metadata_offset = read_i64(buf)?;
        want_fence = read_bool(buf)?;
        want_shut_down = read_bool(buf)?;
        let tagged_fields = read_tagged_fields(buf)?;
        for field in &tagged_fields {
            match field.tag {
                0 => {
                    if version >= 1 {
                        let mut tag_buf = field.data.clone();
                        offline_log_dirs = {
                            let len = read_compact_array_length(&mut tag_buf)?;
                            let mut arr = Vec::with_capacity(len.max(0) as usize);
                            for _ in 0..len {
                                arr.push(read_uuid(&mut tag_buf)?);
                            }
                            arr
                        };
                    }
                },
                1 => {
                    if version >= 2 {
                        let mut tag_buf = field.data.clone();
                        cordoned_log_dirs = {
                            let len = read_compact_array_length(&mut tag_buf)?;
                            if len < 0 {
                                None
                            } else {
                                let mut arr = Vec::with_capacity(len as usize);
                                for _ in 0..len {
                                    arr.push(read_uuid(&mut tag_buf)?);
                                }
                                Some(arr)
                            }
                        };
                    }
                },
                _ => {
                    _unknown_tagged_fields.push(field.clone());
                },
            }
        }
        Ok(Self {
            broker_id,
            broker_epoch,
            current_metadata_offset,
            want_fence,
            want_shut_down,
            offline_log_dirs,
            cordoned_log_dirs,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 0 || version > 2 {
            return Err(UnsupportedVersion::new(63, version).into());
        }
        write_i32(buf, self.broker_id);
        write_i64(buf, self.broker_epoch);
        write_i64(buf, self.current_metadata_offset);
        write_bool(buf, self.want_fence);
        write_bool(buf, self.want_shut_down);
        let mut known_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 1 && !self.offline_log_dirs.is_empty() {
            let mut tag_buf = BytesMut::new();
            write_compact_array_length(&mut tag_buf, self.offline_log_dirs.len() as i32);
            for el in &self.offline_log_dirs {
                write_uuid(&mut tag_buf, el);
            }
            known_tagged_fields.push(RawTaggedField {
                tag: 0,
                data: tag_buf.freeze(),
            });
        }
        if version >= 2 && self.cordoned_log_dirs.is_some() {
            let mut tag_buf = BytesMut::new();
            match &self.cordoned_log_dirs {
                None => {
                    write_compact_array_length(&mut tag_buf, -1);
                },
                Some(arr) => {
                    write_compact_array_length(&mut tag_buf, arr.len() as i32);
                    for el in arr {
                        write_uuid(&mut tag_buf, el);
                    }
                },
            }
            known_tagged_fields.push(RawTaggedField {
                tag: 1,
                data: tag_buf.freeze(),
            });
        }
        let mut all_tags = known_tagged_fields;
        all_tags.extend(self._unknown_tagged_fields.iter().cloned());
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
}
