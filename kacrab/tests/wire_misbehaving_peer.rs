//! Scripted misbehaving-peer tests (issue #3).
//!
//! Each test drives the wire client against a [`MockBroker::serve_script`]
//! broker whose per-request script misbehaves deliberately — wrong correlation
//! ids, truncated or oversized frames, garbage, stalls, slow-loris writes,
//! silent closes, and scripts that span a reconnect. Every assertion is on the
//! *client-side* contract: the typed [`WireError`] the caller sees, the bounded
//! number of requests the broker counted, and that nothing hangs (every await
//! sits under a [`tokio::time::timeout`] guard; the only sleeps are the ones a
//! script itself performs).
//!
//! No real broker is involved; scripts are fully deterministic, so these run
//! in the normal `cargo test` gate.

#![allow(
    clippy::expect_used,
    clippy::missing_assert_message,
    clippy::unwrap_used,
    reason = "Integration test fixtures fail fastest with contextual unwrap/expect calls."
)]

use std::{future::Future, time::Duration};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use kacrab::wire::{BrokerEndpoint, ConnectionConfig, WireClient, WireError};
use kacrab_protocol::{
    KafkaString, frame,
    generated::{
        ApiKey, ApiVersion, ApiVersionsRequestData, ApiVersionsResponseData, MetadataRequestData,
        MetadataResponseData, RequestHeaderData,
    },
};

use crate::common::{MockBroker, response_frame, script::ScriptAction};

mod common;

/// Broker node id used by every test; each test gets its own broker/port.
const NODE_ID: i32 = 7;

/// Upper bound on any single client-visible operation. Nothing in these tests
/// legitimately takes this long; hitting it means the client hung.
const GUARD: Duration = Duration::from_secs(5);

/// `request.timeout.ms` for the tests that drive a request into a timeout.
/// Small so the test is fast, large enough that the scripted broker's actions
/// (which involve no sleeps on the timeout paths) always land well inside it.
const SHORT_REQUEST_TIMEOUT: Duration = Duration::from_millis(200);

/// Await `future` under the anti-hang guard.
async fn guarded<F>(future: F) -> F::Output
where
    F: Future,
{
    tokio::time::timeout(GUARD, future)
        .await
        .expect("client operation hung: guard elapsed")
}

/// Connection config with reconnect backoff tightened so scripts spanning a
/// reconnect complete quickly.
fn fast_reconnect_config() -> ConnectionConfig {
    ConnectionConfig::default()
        .reconnect_backoff_initial(Duration::from_millis(10))
        .reconnect_backoff_max(Duration::from_millis(20))
}

fn client_with(config: ConnectionConfig, broker: &MockBroker) -> WireClient {
    WireClient::connect_with_brokers(
        config,
        "kacrab-misbehaving-peer-test",
        [BrokerEndpoint::new(NODE_ID, broker.addr())],
    )
}

fn api_versions_request() -> ApiVersionsRequestData {
    ApiVersionsRequestData {
        client_software_name: KafkaString::from("kacrab".to_owned()),
        client_software_version: KafkaString::from("0.0.1".to_owned()),
        _unknown_tagged_fields: Vec::new(),
    }
}

async fn send_api_versions(client: &WireClient) -> Result<ApiVersionsResponseData, WireError> {
    client
        .send_to_broker(NODE_ID, ApiKey::ApiVersions, 3, &api_versions_request())
        .await
}

/// The capability handshake: advertise `ApiVersions` v0..=4 and `Metadata`
/// v0..=13 so the client can negotiate every request these tests send.
fn handshake_reply(mut request: Bytes) -> BytesMut {
    let header = RequestHeaderData::read(&mut request, 2).expect("handshake request header");
    let response = ApiVersionsResponseData {
        error_code: 0,
        api_keys: vec![
            ApiVersion {
                api_key: ApiKey::ApiVersions as i16,
                min_version: 0,
                max_version: 4,
                _unknown_tagged_fields: Vec::new(),
            },
            ApiVersion {
                api_key: ApiKey::Metadata as i16,
                min_version: 0,
                max_version: 13,
                _unknown_tagged_fields: Vec::new(),
            },
        ],
        ..ApiVersionsResponseData::default()
    };
    response_frame(ApiKey::ApiVersions, 3, header.correlation_id, &response)
}

/// Response body whose `api_keys[0].max_version` carries `marker`, so a test
/// can prove *which* scripted frame completed the request.
fn api_versions_marker_response(marker: i16) -> ApiVersionsResponseData {
    ApiVersionsResponseData {
        error_code: 0,
        api_keys: vec![ApiVersion {
            api_key: ApiKey::ApiVersions as i16,
            min_version: 0,
            max_version: marker,
            _unknown_tagged_fields: Vec::new(),
        }],
        ..ApiVersionsResponseData::default()
    }
}

/// Echo the request's correlation id back on a marker response.
fn marker_reply(marker: i16) -> impl FnOnce(Bytes) -> BytesMut + Send {
    move |mut request| {
        let header = RequestHeaderData::read(&mut request, 2).expect("request header");
        response_frame(
            ApiKey::ApiVersions,
            3,
            header.correlation_id,
            &api_versions_marker_response(marker),
        )
    }
}

fn marker_of(response: &ApiVersionsResponseData) -> i16 {
    response
        .api_keys
        .first()
        .expect("marker response advertises one api key")
        .max_version
}

/// The client's first pipelined request after the handshake always carries
/// correlation id 1 (the handshake reserves id 0 outside the pipeline).
const FIRST_PIPELINED_CORRELATION_ID: i32 = 1;

// ---------------------------------------------------------------------------
// Scripts that span connections: retriable setup failures are retried on a
// fresh connection while the original caller keeps waiting.
// ---------------------------------------------------------------------------

/// A broker that dies at accept time is a retriable setup failure: the same
/// in-flight `send` must be answered on the next connection, after backoff,
/// without surfacing an error to the caller.
#[tokio::test]
async fn request_survives_connection_that_dies_at_accept() {
    let broker = MockBroker::serve_script(vec![
        ScriptAction::CloseNow,
        ScriptAction::Reply(Box::new(handshake_reply)),
        ScriptAction::Reply(Box::new(marker_reply(41))),
    ])
    .await;
    let client = client_with(fast_reconnect_config(), &broker);

    let response = guarded(send_api_versions(&client)).await.unwrap();

    assert_eq!(marker_of(&response), 41);
    drop(client);
    assert_eq!(
        broker.join().await,
        2,
        "dead connection served zero requests; fresh connection served handshake + request"
    );
}

/// A handshake response carrying the wrong correlation id must not be trusted
/// and must not poison the client: the connection is abandoned and the same
/// in-flight `send` is answered on the next connection.
#[tokio::test]
async fn request_survives_handshake_correlation_id_mismatch() {
    let wrong_correlation_id_handshake = |mut request: Bytes| {
        let _header = RequestHeaderData::read(&mut request, 2).expect("handshake request header");
        response_frame(ApiKey::ApiVersions, 3, 7, &api_versions_marker_response(99))
    };
    let broker = MockBroker::serve_script(vec![
        ScriptAction::Reply(Box::new(wrong_correlation_id_handshake)),
        ScriptAction::NextConnection,
        ScriptAction::Reply(Box::new(handshake_reply)),
        ScriptAction::Reply(Box::new(marker_reply(41))),
    ])
    .await;
    let client = client_with(fast_reconnect_config(), &broker);

    let response = guarded(send_api_versions(&client)).await.unwrap();

    assert_eq!(
        marker_of(&response),
        41,
        "the mismatched handshake's payload must never reach the caller"
    );
    drop(client);
    assert_eq!(broker.join().await, 3);
}

// ---------------------------------------------------------------------------
// Correlation-id discipline on an established connection.
// ---------------------------------------------------------------------------

/// A response whose correlation id matches no in-flight request is a stray and
/// must be dropped — not charged to the oldest waiter — so the real response
/// right behind it still completes the request.
#[tokio::test]
async fn stray_correlation_id_is_dropped_and_real_response_completes() {
    let stray_then_real = |mut request: Bytes| {
        let header = RequestHeaderData::read(&mut request, 2).expect("request header");
        let mut frames = response_frame(
            ApiKey::ApiVersions,
            3,
            header.correlation_id.wrapping_add(1000),
            &api_versions_marker_response(99),
        );
        frames.extend_from_slice(&response_frame(
            ApiKey::ApiVersions,
            3,
            header.correlation_id,
            &api_versions_marker_response(41),
        ));
        frames
    };
    let broker = MockBroker::serve_script(vec![
        ScriptAction::Reply(Box::new(handshake_reply)),
        ScriptAction::Reply(Box::new(stray_then_real)),
    ])
    .await;
    let client = client_with(ConnectionConfig::default(), &broker);

    let response = guarded(send_api_versions(&client)).await.unwrap();

    assert_eq!(
        marker_of(&response),
        41,
        "the stray frame's payload must not complete the request"
    );
    drop(client);
    assert_eq!(broker.join().await, 2);
}

/// Two pipelined requests answered in reverse order must each complete with
/// their own payload: responses are matched by correlation id, not arrival
/// order.
#[tokio::test]
async fn pipelined_responses_complete_out_of_order_by_correlation_id() {
    let reversed_batch = |batch: Vec<Bytes>| {
        let mut replies = Vec::new();
        for mut request in batch {
            let api_key = request.clone().get_i16();
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            if api_key == ApiKey::Metadata as i16 {
                let response = MetadataResponseData {
                    cluster_id: Some(KafkaString::from("scripted-broker".to_owned())),
                    ..MetadataResponseData::default()
                };
                replies.push(response_frame(
                    ApiKey::Metadata,
                    13,
                    header.correlation_id,
                    &response,
                ));
            } else {
                replies.push(response_frame(
                    ApiKey::ApiVersions,
                    3,
                    header.correlation_id,
                    &api_versions_marker_response(41),
                ));
            }
        }
        let mut out = BytesMut::new();
        for reply in replies.into_iter().rev() {
            out.extend_from_slice(&reply);
        }
        out
    };
    let broker = MockBroker::serve_script(vec![
        ScriptAction::Reply(Box::new(handshake_reply)),
        ScriptAction::ReplyBatch {
            count: 2,
            handler: Box::new(reversed_batch),
        },
    ])
    .await;
    let client = client_with(ConnectionConfig::default(), &broker);

    let metadata_request = MetadataRequestData::default();
    let api_versions = send_api_versions(&client);
    let metadata = client.send_to_broker::<_, MetadataResponseData>(
        NODE_ID,
        ApiKey::Metadata,
        13,
        &metadata_request,
    );
    let (api_versions, metadata) = guarded(async { tokio::join!(api_versions, metadata) }).await;

    assert_eq!(marker_of(&api_versions.unwrap()), 41);
    assert_eq!(
        metadata.unwrap().cluster_id,
        Some(KafkaString::from("scripted-broker".to_owned()))
    );
    drop(client);
    assert_eq!(broker.join().await, 3);
}

// ---------------------------------------------------------------------------
// Malformed and hostile frames.
// ---------------------------------------------------------------------------

/// A response frame cut short by a connection close must fail the in-flight
/// request with the typed disconnect error — promptly, not by waiting out
/// `request.timeout.ms`.
#[tokio::test]
async fn truncated_response_frame_fails_request_with_connection_closed() {
    let broker = MockBroker::serve_script(vec![
        ScriptAction::Reply(Box::new(handshake_reply)),
        ScriptAction::TruncatedReply {
            keep: 6,
            handler: Box::new(marker_reply(41)),
        },
    ])
    .await;
    let client = client_with(ConnectionConfig::default(), &broker);

    let error = guarded(send_api_versions(&client)).await.unwrap_err();

    assert!(
        matches!(error, WireError::ConnectionClosed),
        "expected ConnectionClosed, got {error:?}"
    );
    drop(client);
    assert_eq!(broker.join().await, 2);
}

/// A length prefix beyond `MAX_FRAME_LENGTH` must be rejected from the four
/// prefix bytes alone: the request fails promptly with the typed disconnect
/// error and the client never waits for (or buffers) the announced payload.
#[tokio::test]
async fn oversized_frame_length_fails_request_without_hanging() {
    let mut oversized = Vec::new();
    oversized.extend_from_slice(&frame::MAX_FRAME_LENGTH.saturating_add(1).to_be_bytes());
    oversized.extend_from_slice(b"not the announced 100MiB");
    let broker = MockBroker::serve_script(vec![
        ScriptAction::Reply(Box::new(handshake_reply)),
        ScriptAction::RawReply(oversized),
        ScriptAction::HoldOpen,
    ])
    .await;
    let client = client_with(ConnectionConfig::default(), &broker);

    let error = guarded(send_api_versions(&client)).await.unwrap_err();

    assert!(
        matches!(error, WireError::ConnectionClosed),
        "expected ConnectionClosed, got {error:?}"
    );
    drop(client);
    assert_eq!(broker.join().await, 2);
}

/// A well-framed garbage response whose correlation id matches nothing is
/// dropped as a stray; with no real response behind it the request must fail
/// with the typed `Timeout` at `request.timeout.ms` — not hang, and not be
/// completed by garbage.
#[tokio::test]
async fn garbage_frame_with_unknown_correlation_id_times_out_request() {
    let mut garbage = BytesMut::new();
    garbage.put_i32(4);
    garbage.put_i32(i32::MAX);
    let broker = MockBroker::serve_script(vec![
        ScriptAction::Reply(Box::new(handshake_reply)),
        ScriptAction::RawReply(garbage.to_vec()),
        ScriptAction::HoldOpen,
    ])
    .await;
    let client = client_with(
        ConnectionConfig::default().request_timeout(SHORT_REQUEST_TIMEOUT),
        &broker,
    );

    let error = guarded(send_api_versions(&client)).await.unwrap_err();

    assert!(
        matches!(error, WireError::Timeout),
        "expected Timeout, got {error:?}"
    );
    drop(client);
    assert_eq!(broker.join().await, 2);
}

/// A frame that names the in-flight correlation id but carries an undecodable
/// body must surface the typed protocol error to that request's caller — not a
/// silent timeout, and not a decode panic.
#[tokio::test]
async fn malformed_body_with_matching_correlation_id_returns_protocol_error() {
    // Hand-rolled frame: correct length prefix, the in-flight correlation id
    // (ApiVersions response headers carry no tagged fields at any version),
    // then a body whose compact-array length byte demands bytes that never
    // arrive.
    let mut malformed = BytesMut::new();
    malformed.put_i32(7);
    malformed.put_i32(FIRST_PIPELINED_CORRELATION_ID);
    malformed.put_i16(0); // error_code = 0
    malformed.put_u8(0xFF); // truncated compact-array length varint
    let broker = MockBroker::serve_script(vec![
        ScriptAction::Reply(Box::new(handshake_reply)),
        ScriptAction::RawReply(malformed.to_vec()),
        ScriptAction::HoldOpen,
    ])
    .await;
    let client = client_with(ConnectionConfig::default(), &broker);

    let error = guarded(send_api_versions(&client)).await.unwrap_err();

    assert!(
        matches!(error, WireError::Protocol(_)),
        "expected Protocol decode error, got {error:?}"
    );
    drop(client);
    assert_eq!(broker.join().await, 2);
}

// ---------------------------------------------------------------------------
// Timing hostility: slow writers, stalls, delays.
// ---------------------------------------------------------------------------

/// A response dribbled out a few bytes per write must still be reassembled
/// into one frame: the reader cannot assume a frame arrives in one read.
#[tokio::test]
async fn slow_loris_chunked_response_is_reassembled() {
    let broker = MockBroker::serve_script(vec![
        ScriptAction::Reply(Box::new(handshake_reply)),
        ScriptAction::ChunkedReply {
            chunk_len: 3,
            gap: Duration::from_millis(1),
            handler: Box::new(marker_reply(41)),
        },
    ])
    .await;
    let client = client_with(ConnectionConfig::default(), &broker);

    let response = guarded(send_api_versions(&client)).await.unwrap();

    assert_eq!(marker_of(&response), 41);
    drop(client);
    assert_eq!(broker.join().await, 2);
}

/// A peer that writes a partial frame and then stalls forever must not pin the
/// caller: the request fails with the typed `Timeout` at `request.timeout.ms`.
#[tokio::test]
async fn stalled_partial_response_times_out_request() {
    let broker = MockBroker::serve_script(vec![
        ScriptAction::Reply(Box::new(handshake_reply)),
        ScriptAction::StallAfterPartialReply {
            keep: 8,
            handler: Box::new(marker_reply(41)),
        },
    ])
    .await;
    let client = client_with(
        ConnectionConfig::default().request_timeout(SHORT_REQUEST_TIMEOUT),
        &broker,
    );

    let error = guarded(send_api_versions(&client)).await.unwrap_err();

    assert!(
        matches!(error, WireError::Timeout),
        "expected Timeout, got {error:?}"
    );
    drop(client);
    assert_eq!(broker.join().await, 2);
}

/// A request the broker never answers times out — and the timeout must not
/// tear down the connection or desynchronise the correlation pipeline: the
/// next request on the same connection completes normally.
#[tokio::test]
async fn unanswered_request_times_out_and_connection_recovers() {
    let broker = MockBroker::serve_script(vec![
        ScriptAction::Reply(Box::new(handshake_reply)),
        ScriptAction::IgnoreRequest,
        ScriptAction::Reply(Box::new(marker_reply(41))),
    ])
    .await;
    let client = client_with(
        ConnectionConfig::default().request_timeout(SHORT_REQUEST_TIMEOUT),
        &broker,
    );

    let error = guarded(send_api_versions(&client)).await.unwrap_err();
    assert!(
        matches!(error, WireError::Timeout),
        "expected Timeout, got {error:?}"
    );

    let response = guarded(send_api_versions(&client)).await.unwrap();
    assert_eq!(
        marker_of(&response),
        41,
        "the request after a timeout must complete on the same connection"
    );
    drop(client);
    assert_eq!(
        broker.join().await,
        3,
        "handshake + ignored request + answered request, all on one connection"
    );
}

/// A response delayed inside `request.timeout.ms` is not an error: the caller
/// waits it out and completes.
#[tokio::test]
async fn delayed_response_within_request_timeout_succeeds() {
    let delay = Duration::from_millis(50);
    let broker = MockBroker::serve_script(vec![
        ScriptAction::Reply(Box::new(handshake_reply)),
        ScriptAction::DelayedReply(delay, Box::new(marker_reply(41))),
    ])
    .await;
    let client = client_with(ConnectionConfig::default(), &broker);

    let started = std::time::Instant::now();
    let response = guarded(send_api_versions(&client)).await.unwrap();

    assert_eq!(marker_of(&response), 41);
    assert!(
        started.elapsed() >= delay,
        "the response cannot have arrived before the scripted delay"
    );
    drop(client);
    assert_eq!(broker.join().await, 2);
}
