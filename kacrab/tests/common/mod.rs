//! Mock-broker fixture shared by the integration tests.
//!
//! `wire_session` and `producer_dispatcher` each used to carry a private copy of
//! `MockBroker`, `read_frame` and `response_frame`. The copies had already
//! drifted — three different `read_frame` signatures (`TcpStream`-only,
//! `AsyncRead`-generic, `Option<Bytes>`-returning) and two names for the same
//! response-writing trait — so a fix applied to one did not reach the other.
//!
//! Cargo does not build `tests/common/mod.rs` as its own test binary, so each
//! test file picks this up with a plain `mod common;`.
//!
//! `MockBroker` owns exactly the part every server shares: bind an ephemeral
//! loopback port, spawn the serving task, hand back the address, and join on the
//! request count. Protocol-specific servers (pipelining, reconnect, blocking
//! mid-handshake) stay in the test file that needs them and are built on
//! [`MockBroker::serve_with`]; keeping them out of here is also what stops the
//! shared struct from growing a second inherent impl per test file.
//!
//! [`broker_capability`] is the other shared fixture: the
//! `require_broker_api!` guard the `real_kafka_*` suites use to self-skip on
//! brokers that do not advertise an API a test needs.
//!
//! [`script`] is the scripted misbehaving-peer layer: an ordered
//! [`script::ScriptAction`] list executed statefully across requests and
//! reconnects, served through [`MockBroker::serve_script`].

#![allow(
    dead_code,
    unused_imports,
    unused_macros,
    reason = "One shared fixture serves several test binaries; each uses the subset of \
              constructors, response impls, and capability guards its own protocol exercises."
)]
#![allow(
    clippy::expect_used,
    clippy::missing_assert_message,
    clippy::unwrap_used,
    reason = "Integration test fixtures fail fastest with contextual unwrap/expect calls."
)]

pub(crate) mod broker_capability;
pub(crate) mod script;

use std::{future::Future, net::SocketAddr};

pub(crate) use broker_capability::require_broker_api;
use bytes::{Bytes, BytesMut};
use kacrab_protocol::{
    frame,
    generated::{
        AddOffsetsToTxnResponseData, AddPartitionsToTxnResponseData, ApiKey,
        ApiVersionsResponseData, ConsumerGroupHeartbeatResponseData, DeleteTopicsResponseData,
        EndTxnResponseData, FindCoordinatorResponseData, GetTelemetrySubscriptionsResponseData,
        InitProducerIdResponseData, JoinGroupResponseData, LeaveGroupResponseData,
        ListConfigResourcesResponseData, MetadataResponseData, OffsetCommitResponseData,
        OffsetFetchResponseData, ProduceResponseData, PushTelemetryResponseData,
        ResponseHeaderData, SaslAuthenticateResponseData, SaslHandshakeResponseData,
        ShareGroupHeartbeatResponseData, TxnOffsetCommitResponseData,
    },
    version::response_header_version,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    task::JoinHandle,
};

/// One request handler: consumes a decoded request frame, produces a response
/// frame. Boxed because a `serve_many` script mixes closures with different
/// captures.
pub(crate) type Handler = Box<dyn FnOnce(Bytes) -> BytesMut + Send>;

/// Read one length-prefixed Kafka frame, returning the body without the length.
///
/// Generic over the reader so the same function serves plain `TcpStream` and the
/// `TlsStream` the TLS tests hand it.
pub(crate) async fn read_frame<R>(socket: &mut R) -> Bytes
where
    R: AsyncRead + Unpin,
{
    let len = socket.read_i32().await.unwrap();
    let len = usize::try_from(len).unwrap();
    let mut bytes = vec![0; len];
    let _bytes_read = socket.read_exact(&mut bytes).await.unwrap();
    Bytes::from(bytes)
}

/// Writes a generated response body at a chosen version.
///
/// The generated response types share the shape of `write` but no trait, so the
/// fixture supplies one.
pub(crate) trait WriteResponse {
    fn write_response(&self, buf: &mut BytesMut, version: i16);
}

macro_rules! impl_write_response {
    ($($response:ident),+ $(,)?) => {
        $(
            impl WriteResponse for $response {
                fn write_response(&self, buf: &mut BytesMut, version: i16) {
                    self.write(buf, version)
                        .expect(concat!(stringify!($response), " write"));
                }
            }
        )+
    };
}

impl_write_response!(
    AddOffsetsToTxnResponseData,
    AddPartitionsToTxnResponseData,
    ApiVersionsResponseData,
    ConsumerGroupHeartbeatResponseData,
    DeleteTopicsResponseData,
    EndTxnResponseData,
    FindCoordinatorResponseData,
    GetTelemetrySubscriptionsResponseData,
    InitProducerIdResponseData,
    JoinGroupResponseData,
    LeaveGroupResponseData,
    ListConfigResourcesResponseData,
    MetadataResponseData,
    OffsetCommitResponseData,
    OffsetFetchResponseData,
    ProduceResponseData,
    PushTelemetryResponseData,
    SaslAuthenticateResponseData,
    SaslHandshakeResponseData,
    ShareGroupHeartbeatResponseData,
    TxnOffsetCommitResponseData,
);

/// Encode a response header plus body into a length-prefixed frame.
///
/// The header version is derived from `(api_key, api_version)` exactly as the
/// client derives it, so a version bump that moves a response header between
/// flexible and non-flexible cannot silently desynchronise the fixture.
pub(crate) fn response_frame(
    api_key: ApiKey,
    api_version: i16,
    correlation_id: i32,
    response: &impl WriteResponse,
) -> BytesMut {
    let mut header = BytesMut::new();
    ResponseHeaderData {
        correlation_id,
        _unknown_tagged_fields: Vec::new(),
    }
    .write(
        &mut header,
        response_header_version(api_key as i16, api_version),
    )
    .expect("response header write");

    let mut body = BytesMut::new();
    response.write_response(&mut body, api_version);

    frame::encode_request(&header, &body).expect("response frame")
}

/// A mock Kafka broker listening on an ephemeral loopback port.
///
/// `join` yields the number of requests the server counted, so tests assert on
/// the request *count* the client actually issued rather than only on the reply
/// it got back.
pub(crate) struct MockBroker {
    addr: SocketAddr,
    join: JoinHandle<usize>,
}

impl MockBroker {
    /// Answer `handlers` in order, one response per request, then stop.
    pub(crate) async fn serve_many(handlers: Vec<Handler>) -> Self {
        Self::serve_with(|listener| Self::run_handlers(listener, handlers)).await
    }

    /// Run a [`script::ScriptAction`] sequence: per-request actions executed
    /// statefully across a connection and across reconnects, for
    /// misbehaving-peer tests. [`Self::join`] yields the number of requests
    /// read across every connection the script served.
    pub(crate) async fn serve_script(actions: Vec<script::ScriptAction>) -> Self {
        Self::serve_with(|listener| script::run_script(listener, actions)).await
    }

    /// `serve_many` bound to a caller-chosen address, for the tests that need a
    /// port to be predictable across a reconnect.
    pub(crate) async fn serve_many_on_addr(addr: SocketAddr, handlers: Vec<Handler>) -> Self {
        let listener = TcpListener::bind(addr).await.unwrap();
        Self::serve_with_listener(listener, |listener| Self::run_handlers(listener, handlers))
    }

    /// Bind an ephemeral loopback port and run an arbitrary serving task on it.
    ///
    /// The escape hatch for servers that are not request/response lockstep —
    /// pipelining, accepting twice, or stalling deliberately. The task's return
    /// value is what [`Self::join`] yields.
    pub(crate) async fn serve_with<F, Fut>(serve: F) -> Self
    where
        F: FnOnce(TcpListener) -> Fut,
        Fut: Future<Output = usize> + Send + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        Self::serve_with_listener(listener, serve)
    }

    fn serve_with_listener<F, Fut>(listener: TcpListener, serve: F) -> Self
    where
        F: FnOnce(TcpListener) -> Fut,
        Fut: Future<Output = usize> + Send + 'static,
    {
        let addr = listener.local_addr().unwrap();
        let join = tokio::spawn(serve(listener));
        Self { addr, join }
    }

    async fn run_handlers(listener: TcpListener, handlers: Vec<Handler>) -> usize {
        let (mut socket, _) = listener.accept().await.unwrap();
        let handled = handlers.len();
        for handler in handlers {
            let request = read_frame(&mut socket).await;
            let response = handler(request);
            socket.write_all(&response).await.unwrap();
        }
        handled
    }

    /// The loopback address the client should connect to.
    pub(crate) const fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Wait for the server to finish and return the request count it saw.
    pub(crate) async fn join(self) -> usize {
        self.join.await.unwrap()
    }
}
