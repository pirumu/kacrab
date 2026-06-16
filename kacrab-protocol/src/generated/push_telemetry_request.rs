//! Generated from PushTelemetryRequest.json - DO NOT EDIT
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
pub struct PushTelemetryRequestData {
    /// Unique id for this client instance.
    pub client_instance_id: KafkaUuid,
    /// Unique identifier for the current subscription.
    pub subscription_id: i32,
    /// Client is terminating the connection.
    pub terminating: bool,
    /// Compression codec used to compress the metrics.
    pub compression_type: i8,
    /// Metrics encoded in OpenTelemetry MetricsData v1 protobuf format.
    pub metrics: Bytes,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for PushTelemetryRequestData {
    fn default() -> Self {
        Self {
            client_instance_id: KafkaUuid::ZERO,
            subscription_id: 0_i32,
            terminating: false,
            compression_type: 0_i8,
            metrics: Bytes::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl PushTelemetryRequestData {
    pub fn with_client_instance_id(mut self, value: KafkaUuid) -> Self {
        self.client_instance_id = value;
        self
    }
    pub fn with_subscription_id(mut self, value: i32) -> Self {
        self.subscription_id = value;
        self
    }
    pub fn with_terminating(mut self, value: bool) -> Self {
        self.terminating = value;
        self
    }
    pub fn with_compression_type(mut self, value: i8) -> Self {
        self.compression_type = value;
        self
    }
    pub fn with_metrics(mut self, value: Bytes) -> Self {
        self.metrics = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 0 || version > 0 {
            return Err(UnsupportedVersion::new(72, version).into());
        }
        let client_instance_id;
        let subscription_id;
        let terminating;
        let compression_type;
        let metrics;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        client_instance_id = read_uuid(buf)?;
        subscription_id = read_i32(buf)?;
        terminating = read_bool(buf)?;
        compression_type = read_i8(buf)?;
        metrics = read_compact_bytes(buf)?;
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
            subscription_id,
            terminating,
            compression_type,
            metrics,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 0 || version > 0 {
            return Err(UnsupportedVersion::new(72, version).into());
        }
        write_uuid(buf, &self.client_instance_id);
        write_i32(buf, self.subscription_id);
        write_bool(buf, self.terminating);
        write_i8(buf, self.compression_type);
        write_compact_bytes(buf, &self.metrics)?;
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        if version < 0 || version > 0 {
            return Err(UnsupportedVersion::new(72, version).into());
        }
        let mut len: usize = 0;
        len += 16;
        len += 4;
        len += 1;
        len += 1;
        len += compact_bytes_len(&self.metrics)?;
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
