//! Generated from GetTelemetrySubscriptionsResponse.json - DO NOT EDIT
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
pub struct GetTelemetrySubscriptionsResponseData {
    /// The duration in milliseconds for which the request was throttled due to a quota violation,
    /// or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// The error code, or 0 if there was no error.
    pub error_code: i16,
    /// Assigned client instance id if ClientInstanceId was 0 in the request, else 0.
    pub client_instance_id: KafkaUuid,
    /// Unique identifier for the current subscription set for this client instance.
    pub subscription_id: i32,
    /// Compression types that broker accepts for the PushTelemetryRequest.
    pub accepted_compression_types: Vec<i8>,
    /// Configured push interval, which is the lowest configured interval in the current
    /// subscription set.
    pub push_interval_ms: i32,
    /// The maximum bytes of binary data the broker accepts in PushTelemetryRequest.
    pub telemetry_max_bytes: i32,
    /// Flag to indicate monotonic/counter metrics are to be emitted as deltas or cumulative
    /// values.
    pub delta_temporality: bool,
    /// Requested metrics prefix string match. Empty array: No metrics subscribed, Array[0] empty
    /// string: All metrics subscribed.
    pub requested_metrics: Vec<KafkaString>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for GetTelemetrySubscriptionsResponseData {
    fn default() -> Self {
        Self {
            throttle_time_ms: 0_i32,
            error_code: 0_i16,
            client_instance_id: KafkaUuid::ZERO,
            subscription_id: 0_i32,
            accepted_compression_types: Vec::new(),
            push_interval_ms: 0_i32,
            telemetry_max_bytes: 0_i32,
            delta_temporality: false,
            requested_metrics: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl GetTelemetrySubscriptionsResponseData {
    pub fn with_throttle_time_ms(mut self, value: i32) -> Self {
        self.throttle_time_ms = value;
        self
    }
    pub fn with_error_code(mut self, value: i16) -> Self {
        self.error_code = value;
        self
    }
    pub fn with_client_instance_id(mut self, value: KafkaUuid) -> Self {
        self.client_instance_id = value;
        self
    }
    pub fn with_subscription_id(mut self, value: i32) -> Self {
        self.subscription_id = value;
        self
    }
    pub fn with_accepted_compression_types(mut self, value: Vec<i8>) -> Self {
        self.accepted_compression_types = value;
        self
    }
    pub fn with_push_interval_ms(mut self, value: i32) -> Self {
        self.push_interval_ms = value;
        self
    }
    pub fn with_telemetry_max_bytes(mut self, value: i32) -> Self {
        self.telemetry_max_bytes = value;
        self
    }
    pub fn with_delta_temporality(mut self, value: bool) -> Self {
        self.delta_temporality = value;
        self
    }
    pub fn with_requested_metrics(mut self, value: Vec<KafkaString>) -> Self {
        self.requested_metrics = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 0 || version > 0 {
            return Err(UnsupportedVersion::new(71, version).into());
        }
        let throttle_time_ms;
        let error_code;
        let client_instance_id;
        let subscription_id;
        let accepted_compression_types;
        let push_interval_ms;
        let telemetry_max_bytes;
        let delta_temporality;
        let requested_metrics;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        throttle_time_ms = read_i32(buf)?;
        error_code = read_i16(buf)?;
        client_instance_id = read_uuid(buf)?;
        subscription_id = read_i32(buf)?;
        accepted_compression_types = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(len.max(0) as usize);
            for _ in 0..len {
                arr.push(read_i8(buf)?);
            }
            arr
        };
        push_interval_ms = read_i32(buf)?;
        telemetry_max_bytes = read_i32(buf)?;
        delta_temporality = read_bool(buf)?;
        requested_metrics = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(len.max(0) as usize);
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
            throttle_time_ms,
            error_code,
            client_instance_id,
            subscription_id,
            accepted_compression_types,
            push_interval_ms,
            telemetry_max_bytes,
            delta_temporality,
            requested_metrics,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 0 || version > 0 {
            return Err(UnsupportedVersion::new(71, version).into());
        }
        write_i32(buf, self.throttle_time_ms);
        write_i16(buf, self.error_code);
        write_uuid(buf, &self.client_instance_id);
        write_i32(buf, self.subscription_id);
        write_compact_array_length(buf, self.accepted_compression_types.len() as i32);
        for el in &self.accepted_compression_types {
            write_i8(buf, *el);
        }
        write_i32(buf, self.push_interval_ms);
        write_i32(buf, self.telemetry_max_bytes);
        write_bool(buf, self.delta_temporality);
        write_compact_array_length(buf, self.requested_metrics.len() as i32);
        for el in &self.requested_metrics {
            write_compact_string(buf, el)?;
        }
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
        len += 4;
        len += 2;
        len += 16;
        len += 4;
        len += compact_array_length_len(self.accepted_compression_types.len() as i32);
        len += self.accepted_compression_types.len() * 1usize;
        len += 4;
        len += 4;
        len += 1;
        len += compact_array_length_len(self.requested_metrics.len() as i32);
        for el in &self.requested_metrics {
            len += compact_string_len(el)?;
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
