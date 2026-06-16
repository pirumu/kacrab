//! Generated from GetTelemetrySubscriptionsRequest.json - DO NOT EDIT
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
pub struct GetTelemetrySubscriptionsRequestData {
    /// Unique id for this client instance, must be set to 0 on the first request.
    pub client_instance_id: KafkaUuid,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for GetTelemetrySubscriptionsRequestData {
    fn default() -> Self {
        Self {
            client_instance_id: KafkaUuid::ZERO,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl GetTelemetrySubscriptionsRequestData {
    pub fn with_client_instance_id(mut self, value: KafkaUuid) -> Self {
        self.client_instance_id = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 0 || version > 0 {
            return Err(UnsupportedVersion::new(71, version).into());
        }
        let client_instance_id;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        client_instance_id = read_uuid(buf)?;
        let tagged_fields = read_tagged_fields(buf)?;
        for field in &tagged_fields {
            match field.tag {
                _ => {
                    _unknown_tagged_fields.push(field.clone());
                },
            }
        }
        Ok(Self {
            client_instance_id,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 0 || version > 0 {
            return Err(UnsupportedVersion::new(71, version).into());
        }
        write_uuid(buf, &self.client_instance_id);
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        if version < 0 || version > 0 {
            return Err(UnsupportedVersion::new(71, version).into());
        }
        let mut len: usize = 0;
        len += 16;
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
