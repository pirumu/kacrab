//! Generated from DescribeTransactionsRequest.json - DO NOT EDIT
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
pub struct DescribeTransactionsRequestData {
    /// Array of transactionalIds to include in describe results. If empty, then no results will be
    /// returned.
    pub transactional_ids: Vec<KafkaString>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for DescribeTransactionsRequestData {
    fn default() -> Self {
        Self {
            transactional_ids: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl DescribeTransactionsRequestData {
    pub fn with_transactional_ids(mut self, value: Vec<KafkaString>) -> Self {
        self.transactional_ids = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 0 || version > 0 {
            return Err(UnsupportedVersion::new(65, version).into());
        }
        let transactional_ids;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        transactional_ids = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
            for _ in 0..len {
                arr.push(read_compact_string(buf)?);
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
            transactional_ids,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 0 || version > 0 {
            return Err(UnsupportedVersion::new(65, version).into());
        }
        write_compact_array_length(buf, self.transactional_ids.len() as i32);
        for el in &self.transactional_ids {
            write_compact_string(buf, el)?;
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        if version < 0 || version > 0 {
            return Err(UnsupportedVersion::new(65, version).into());
        }
        let mut len: usize = 0;
        len += compact_array_length_len(self.transactional_ids.len() as i32);
        for el in &self.transactional_ids {
            len += compact_string_len(el)?;
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
