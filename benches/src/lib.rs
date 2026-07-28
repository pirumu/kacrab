//! Mock-broker fixture shared by the kacrab benchmarks.
//!
//! `wire_pipeline`, `producer_dispatcher` and `producer_mock_bench` each carried
//! a private copy of `MockBroker`, `read_frame` and `response_frame`, and the
//! copies had drifted on the one thing that matters: `read_frame` returned
//! `Option<Bytes>` in the dispatcher bench, so its serve loop ends cleanly when
//! the client disconnects, while the other two `expect` the length prefix and
//! panic inside a spawned task on the same clean EOF. This module keeps the
//! EOF-tolerant signature — a caller that requires a frame says so with
//! `.expect(..)` at the point where that is actually true.
//!
//! `MockBroker` owns only what every mock server shares: bind an ephemeral
//! loopback port, spawn the serving task, expose the address, and join on the
//! request count. Each benchmark's protocol script stays in the benchmark that
//! needs it, built on [`MockBroker::serve_with`].

#![allow(
    clippy::expect_used,
    reason = "Benchmark fixtures prefer direct fail-fast setup."
)]

use std::{future::Future, net::SocketAddr};

use bytes::{Bytes, BytesMut};
use kacrab_protocol::{
    frame,
    generated::{
        ApiKey, ApiVersionsResponseData, MetadataResponseData, ProduceResponseData,
        ResponseHeaderData,
    },
    version::response_header_version,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    net::TcpListener,
    task::JoinHandle,
};

/// Read one length-prefixed Kafka frame, or `None` once the peer has closed.
///
/// A clean EOF on the length prefix is how a mock server learns the client is
/// done, so it is a normal return rather than a panic. A truncated frame *after*
/// a valid length still panics — that is a protocol error, not a disconnect.
pub async fn read_frame<R>(socket: &mut R) -> Option<Bytes>
where
    R: AsyncRead + Unpin,
{
    let len = socket.read_i32().await.ok()?;
    let len = usize::try_from(len).expect("positive frame length");
    let mut bytes = vec![0; len];
    let _bytes_read = socket.read_exact(&mut bytes).await.expect("frame payload");
    Some(Bytes::from(bytes))
}

/// Writes a generated response body at a chosen version.
///
/// The generated response types share the shape of `write` but no trait, so the
/// fixture supplies one.
pub trait WriteResponse {
    /// Encode `self` into `buf` at `version`.
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
    ApiVersionsResponseData,
    MetadataResponseData,
    ProduceResponseData,
);

/// Encode a response header plus body into a length-prefixed frame.
///
/// The header version is derived from `(api_key, api_version)` exactly as the
/// client derives it, so a version bump that moves a response header between
/// flexible and non-flexible cannot silently desynchronise the fixture.
pub fn response_frame(
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
/// `join` yields the request count the server task returned, so a benchmark can
/// assert it served everything the client issued rather than measuring a run
/// that was silently cut short.
#[derive(Debug)]
pub struct MockBroker {
    addr: SocketAddr,
    join: JoinHandle<usize>,
}

impl MockBroker {
    /// Bind an ephemeral loopback port and run `serve` on it.
    ///
    /// The task's return value is what [`Self::join`] yields.
    pub async fn serve_with<F, Fut>(serve: F) -> Self
    where
        F: FnOnce(TcpListener) -> Fut,
        Fut: Future<Output = usize> + Send + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock broker");
        let addr = listener.local_addr().expect("mock broker addr");
        let join = tokio::spawn(serve(listener));
        Self { addr, join }
    }

    /// The loopback address the client should connect to.
    #[must_use]
    pub const fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Wait for the server to finish and return the request count it saw.
    pub async fn join(self) -> usize {
        self.join.await.expect("mock broker join")
    }

    /// Drop the server without waiting, for warm-up brokers whose request count
    /// is not part of the measurement.
    pub fn abort(self) {
        self.join.abort();
    }
}
