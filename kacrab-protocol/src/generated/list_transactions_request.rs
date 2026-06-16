//! Generated from ListTransactionsRequest.json - DO NOT EDIT
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
pub struct ListTransactionsRequestData {
    /// The transaction states to filter by: if empty, all transactions are returned; if non-empty,
    /// then only transactions matching one of the filtered states will be returned.
    pub state_filters: Vec<KafkaString>,
    /// The producerIds to filter by: if empty, all transactions will be returned; if non-empty,
    /// only transactions which match one of the filtered producerIds will be returned.
    pub producer_id_filters: Vec<i64>,
    /// Duration (in millis) to filter by: if < 0, all transactions will be returned; otherwise,
    /// only transactions running longer than this duration will be returned.
    pub duration_filter: i64,
    /// The transactional ID regular expression pattern to filter by: if it is empty or null, all
    /// transactions are returned; Otherwise then only the transactions matching the given regular
    /// expression will be returned.
    pub transactional_id_pattern: Option<KafkaString>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for ListTransactionsRequestData {
    fn default() -> Self {
        Self {
            state_filters: Vec::new(),
            producer_id_filters: Vec::new(),
            duration_filter: -1i64,
            transactional_id_pattern: None,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl ListTransactionsRequestData {
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 0 || version > 2 {
            return Err(UnsupportedVersion::new(66, version).into());
        }
        let state_filters;
        let producer_id_filters;
        let mut duration_filter = -1i64;
        let mut transactional_id_pattern = None;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        state_filters = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(len.max(0) as usize);
            for _ in 0..len {
                arr.push(read_compact_string(buf)?);
            }
            arr
        };
        producer_id_filters = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(len.max(0) as usize);
            for _ in 0..len {
                arr.push(read_i64(buf)?);
            }
            arr
        };
        if version >= 1 {
            duration_filter = read_i64(buf)?;
        }
        if version >= 2 {
            transactional_id_pattern = read_compact_nullable_string(buf)?;
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
            state_filters,
            producer_id_filters,
            duration_filter,
            transactional_id_pattern,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 0 || version > 2 {
            return Err(UnsupportedVersion::new(66, version).into());
        }
        write_compact_array_length(buf, self.state_filters.len() as i32);
        for el in &self.state_filters {
            write_compact_string(buf, el)?;
        }
        write_compact_array_length(buf, self.producer_id_filters.len() as i32);
        for el in &self.producer_id_filters {
            write_i64(buf, *el);
        }
        if version >= 1 {
            write_i64(buf, self.duration_filter);
        }
        if version >= 2 {
            write_compact_nullable_string(buf, self.transactional_id_pattern.as_ref())?;
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
}
