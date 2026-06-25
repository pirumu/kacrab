//! Bounded per-broker request correlation pipeline.

use std::time::{Duration, Instant};

use bytes::Bytes;
use kacrab_protocol::{frame, generated::ApiKey};
use tokio::sync::oneshot;

use super::error::{Result, WireError};

#[derive(Debug)]
pub(crate) struct ResponseEnvelope {
    pub(crate) api_version: i16,
    pub(crate) body: Bytes,
}

pub(crate) struct RequestPipeline {
    slots: Vec<Option<InFlightRequest>>,
    head: usize,
    len: usize,
    next_correlation_id: i32,
    request_timeout: Duration,
}

struct InFlightRequest {
    api_key: ApiKey,
    correlation_id: i32,
    api_version: i16,
    deadline: Instant,
    tx: oneshot::Sender<Result<ResponseEnvelope>>,
}

impl RequestPipeline {
    pub(crate) fn new(capacity: usize, request_timeout: Duration) -> Self {
        let capacity = capacity.max(1);
        let mut slots = Vec::with_capacity(capacity);
        slots.resize_with(capacity, || None);
        Self {
            slots,
            head: 0,
            len: 0,
            next_correlation_id: 1,
            request_timeout,
        }
    }

    pub(crate) fn reserve(
        &mut self,
        api_key: ApiKey,
        api_version: i16,
        tx: oneshot::Sender<Result<ResponseEnvelope>>,
    ) -> std::result::Result<i32, oneshot::Sender<Result<ResponseEnvelope>>> {
        self.trim_empty_head();
        if self.len == self.slots.len() {
            return Err(tx);
        }

        let correlation_id = self.next_correlation_id;
        self.next_correlation_id = self.next_correlation_id.wrapping_add(1);
        let index = self.slot_index(self.len);
        let Some(slot) = self.slots.get_mut(index) else {
            return Err(tx);
        };
        let deadline = Instant::now()
            .checked_add(self.request_timeout)
            .unwrap_or_else(Instant::now);
        *slot = Some(InFlightRequest {
            api_key,
            correlation_id,
            api_version,
            deadline,
            tx,
        });
        self.len = self.len.checked_add(1).unwrap_or(self.slots.len());
        Ok(correlation_id)
    }

    pub(crate) const fn next_correlation_id(&mut self) -> i32 {
        let correlation_id = self.next_correlation_id;
        self.next_correlation_id = self.next_correlation_id.wrapping_add(1);
        correlation_id
    }

    pub(crate) const fn has_capacity(&self) -> bool {
        self.len < self.slots.len()
    }

    pub(crate) const fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub(crate) fn complete_response(&mut self, bytes: Bytes) {
        self.trim_empty_head();
        if self.len == 0 {
            return;
        }

        let response_correlation_id = response_correlation_id(&bytes);
        let index = response_correlation_id
            .and_then(|correlation_id| self.slot_index_for_correlation(correlation_id))
            .unwrap_or(self.head);
        let Some(in_flight) = self.slots.get_mut(index).and_then(Option::take) else {
            self.trim_empty_head();
            return;
        };

        let response = match frame::decode_response_envelope(
            in_flight.api_key,
            in_flight.api_version,
            bytes,
        ) {
            Ok(response) if response.correlation_id == in_flight.correlation_id => {
                Ok(ResponseEnvelope {
                    api_version: in_flight.api_version,
                    body: response.body,
                })
            },
            Ok(response) => Err(WireError::CorrelationIdMismatch {
                expected: in_flight.correlation_id,
                actual: response.correlation_id,
            }),
            Err(error) => Err(WireError::from(error)),
        };
        let _ignored = in_flight.tx.send(response);
        self.trim_empty_head();
    }

    pub(crate) fn fail_correlation(&mut self, correlation_id: i32, error: WireError) {
        for offset in 0..self.len {
            let index = self.slot_index(offset);
            let Some(in_flight) = self.slots.get(index).and_then(Option::as_ref) else {
                continue;
            };
            if in_flight.correlation_id == correlation_id {
                if let Some(in_flight) = self.slots.get_mut(index).and_then(Option::take) {
                    let _ignored = in_flight.tx.send(Err(error));
                }
                self.trim_empty_head();
                return;
            }
        }
    }

    pub(crate) fn fail_expired(&mut self) {
        let now = Instant::now();
        for offset in 0..self.len {
            let index = self.slot_index(offset);
            let expired = self
                .slots
                .get(index)
                .and_then(Option::as_ref)
                .is_some_and(|in_flight| in_flight.deadline <= now);
            if expired && let Some(in_flight) = self.slots.get_mut(index).and_then(Option::take) {
                let _ignored = in_flight.tx.send(Err(WireError::Timeout));
            }
        }
        self.trim_empty_head();
    }

    pub(crate) fn fail_all(&mut self) {
        for offset in 0..self.len {
            let index = self.slot_index(offset);
            if let Some(in_flight) = self.slots.get_mut(index).and_then(Option::take) {
                let _ignored = in_flight.tx.send(Err(WireError::ConnectionClosed));
            }
        }
        self.head = 0;
        self.len = 0;
    }

    fn trim_empty_head(&mut self) {
        while self.len > 0 && self.slots.get(self.head).is_some_and(Option::is_none) {
            self.head = self.next_index(self.head);
            self.len = self.len.saturating_sub(1);
        }
    }

    fn slot_index(&self, offset: usize) -> usize {
        let mut index = self.head;
        for _ in 0..offset {
            index = self.next_index(index);
        }
        index
    }

    fn slot_index_for_correlation(&self, correlation_id: i32) -> Option<usize> {
        for offset in 0..self.len {
            let index = self.slot_index(offset);
            let Some(in_flight) = self.slots.get(index).and_then(Option::as_ref) else {
                continue;
            };
            if in_flight.correlation_id == correlation_id {
                return Some(index);
            }
        }
        None
    }

    fn next_index(&self, index: usize) -> usize {
        let next = index.checked_add(1).unwrap_or_default();
        if next == self.slots.len() { 0 } else { next }
    }
}

fn response_correlation_id(bytes: &Bytes) -> Option<i32> {
    let raw = bytes.get(..4)?;
    Some(i32::from_be_bytes(raw.try_into().ok()?))
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::expect_used,
        clippy::missing_assert_message,
        clippy::unwrap_used,
        reason = "Unit test fixtures fail fastest with contextual unwrap/expect calls."
    )]

    use std::time::Duration;

    use bytes::BytesMut;
    use kacrab_protocol::{
        generated::{ApiKey, ResponseHeaderData},
        version::response_header_version,
    };
    use tokio::sync::oneshot;

    use super::{RequestPipeline, ResponseEnvelope};
    use crate::wire::WireError;

    fn response_frame(api_key: ApiKey, api_version: i16, correlation_id: i32) -> bytes::Bytes {
        let mut bytes = BytesMut::new();
        ResponseHeaderData {
            correlation_id,
            _unknown_tagged_fields: Vec::new(),
        }
        .write(
            &mut bytes,
            response_header_version(api_key as i16, api_version),
        )
        .expect("response header");
        bytes.freeze()
    }

    fn channel() -> (
        oneshot::Sender<crate::wire::Result<ResponseEnvelope>>,
        oneshot::Receiver<crate::wire::Result<ResponseEnvelope>>,
    ) {
        oneshot::channel()
    }

    #[tokio::test]
    async fn pipeline_rejects_reserve_when_capacity_is_full() {
        let mut pipeline = RequestPipeline::new(1, Duration::from_secs(1));
        let (tx, _rx) = channel();

        let first = pipeline
            .reserve(ApiKey::ApiVersions, 3, tx)
            .expect("first reserve");
        let (tx, rx) = channel();
        let returned = pipeline
            .reserve(ApiKey::ApiVersions, 3, tx)
            .expect_err("capacity should be full");

        drop(returned);
        assert_eq!(first, 1);
        assert!(rx.await.is_err());
    }

    #[tokio::test]
    async fn pipeline_completes_matching_response_and_reuses_slot() {
        let mut pipeline = RequestPipeline::new(1, Duration::from_secs(1));
        let (tx, rx) = channel();
        let correlation_id = pipeline
            .reserve(ApiKey::ApiVersions, 3, tx)
            .expect("reserve");

        pipeline.complete_response(response_frame(ApiKey::ApiVersions, 3, correlation_id));
        let response = rx.await.expect("sender").expect("response");

        assert_eq!(response.api_version, 3);
        assert!(pipeline.is_empty());
        assert!(pipeline.has_capacity());
    }

    #[tokio::test]
    async fn pipeline_completes_out_of_order_responses_by_correlation_id() {
        let mut pipeline = RequestPipeline::new(2, Duration::from_secs(1));
        let (first_tx, first_rx) = channel();
        let (second_tx, second_rx) = channel();
        let first = pipeline
            .reserve(ApiKey::ApiVersions, 3, first_tx)
            .expect("first reserve");
        let second = pipeline
            .reserve(ApiKey::Metadata, 12, second_tx)
            .expect("second reserve");

        pipeline.complete_response(response_frame(ApiKey::Metadata, 12, second));
        let second_response = second_rx.await.expect("second sender").expect("second");
        assert_eq!(second_response.api_version, 12);

        pipeline.complete_response(response_frame(ApiKey::ApiVersions, 3, first));
        let first_response = first_rx.await.expect("first sender").expect("first");
        assert_eq!(first_response.api_version, 3);
        assert!(pipeline.is_empty());
        assert!(pipeline.has_capacity());
    }

    #[tokio::test]
    async fn pipeline_reports_correlation_mismatch() {
        let mut pipeline = RequestPipeline::new(1, Duration::from_secs(1));
        let (tx, rx) = channel();
        let correlation_id = pipeline
            .reserve(ApiKey::ApiVersions, 3, tx)
            .expect("reserve");

        pipeline.complete_response(response_frame(
            ApiKey::ApiVersions,
            3,
            correlation_id.saturating_add(1),
        ));

        assert!(matches!(
            rx.await.expect("sender"),
            Err(WireError::CorrelationIdMismatch { .. })
        ));
    }

    #[tokio::test]
    async fn pipeline_ignores_response_when_no_request_is_in_flight() {
        let mut pipeline = RequestPipeline::new(1, Duration::from_secs(1));

        pipeline.complete_response(response_frame(ApiKey::ApiVersions, 3, 1));

        assert!(pipeline.has_capacity());
    }

    #[tokio::test]
    async fn pipeline_reports_decode_errors_to_reserved_request() {
        let mut pipeline = RequestPipeline::new(1, Duration::from_secs(1));
        let (tx, rx) = channel();
        let _correlation_id = pipeline
            .reserve(ApiKey::ApiVersions, 3, tx)
            .expect("reserve");

        pipeline.complete_response(bytes::Bytes::from_static(b"\0"));

        assert!(matches!(
            rx.await.expect("sender"),
            Err(WireError::Frame(_) | WireError::Protocol(_))
        ));
    }

    #[tokio::test]
    async fn pipeline_fail_correlation_ignores_unknown_correlation_id() {
        let mut pipeline = RequestPipeline::new(1, Duration::from_secs(1));
        let (tx, rx) = channel();
        let correlation_id = pipeline
            .reserve(ApiKey::ApiVersions, 3, tx)
            .expect("reserve");

        pipeline.fail_correlation(correlation_id.saturating_add(1), WireError::Backpressure);
        pipeline.complete_response(response_frame(ApiKey::ApiVersions, 3, correlation_id));

        assert!(rx.await.expect("sender").is_ok());
    }

    #[tokio::test]
    async fn pipeline_fails_expired_and_all_requests() {
        let mut pipeline = RequestPipeline::new(2, Duration::ZERO);
        let (first_tx, first_rx) = channel();
        let (second_tx, second_rx) = channel();
        let _first = pipeline
            .reserve(ApiKey::ApiVersions, 3, first_tx)
            .expect("first");
        let second = pipeline
            .reserve(ApiKey::Metadata, 12, second_tx)
            .expect("second");

        pipeline.fail_expired();
        assert!(matches!(
            first_rx.await.expect("sender"),
            Err(WireError::Timeout)
        ));
        assert!(matches!(
            second_rx.await.expect("sender"),
            Err(WireError::Timeout)
        ));

        let (third_tx, third_rx) = channel();
        let _third = pipeline
            .reserve(ApiKey::ApiVersions, 3, third_tx)
            .expect("third");
        pipeline.fail_correlation(second, WireError::Backpressure);
        pipeline.fail_all();

        assert!(matches!(
            third_rx.await.expect("sender"),
            Err(WireError::ConnectionClosed)
        ));
    }

    #[tokio::test]
    async fn pipeline_defensively_returns_sender_when_slot_storage_is_inconsistent() {
        let mut pipeline = RequestPipeline::new(1, Duration::from_secs(1));
        let (tx, rx) = channel();

        pipeline.len = 1;
        pipeline.slots.clear();
        let returned = pipeline
            .reserve(ApiKey::ApiVersions, 3, tx)
            .expect_err("missing slot should return the sender");

        drop(returned);
        assert!(rx.await.is_err());
    }

    #[tokio::test]
    async fn pipeline_defensively_trims_missing_head_response_slot() {
        let mut pipeline = RequestPipeline::new(1, Duration::from_secs(1));

        pipeline.len = 1;
        pipeline.slots.clear();
        pipeline.complete_response(response_frame(ApiKey::ApiVersions, 3, 1));

        assert_eq!(pipeline.len, 1);
    }

    #[tokio::test]
    async fn pipeline_fail_correlation_skips_empty_slots_and_fails_match() {
        let mut pipeline = RequestPipeline::new(2, Duration::from_secs(1));
        let (first_tx, first_rx) = channel();
        let (second_tx, second_rx) = channel();
        let _first = pipeline
            .reserve(ApiKey::ApiVersions, 3, first_tx)
            .expect("first");
        let second = pipeline
            .reserve(ApiKey::Metadata, 12, second_tx)
            .expect("second");

        *pipeline.slots.get_mut(pipeline.head).expect("head slot") = None;
        pipeline.fail_correlation(second, WireError::Backpressure);

        assert!(first_rx.await.is_err());
        assert!(matches!(
            second_rx.await.expect("sender"),
            Err(WireError::Backpressure)
        ));
        assert!(pipeline.has_capacity());
    }
}
