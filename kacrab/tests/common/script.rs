//! Scripted misbehaving-broker layer over [`MockBroker`](super::MockBroker).
//!
//! `MockBroker::serve_many` answers one request with one well-formed response
//! and cannot express a peer that misbehaves *between* requests: a delayed
//! reply, a truncated frame, a silent close, a stall, or a script that spans a
//! reconnect ("first connection dies, second succeeds"). [`ScriptAction`] is
//! that missing vocabulary: an ordered list of per-request actions executed
//! statefully across a connection and across reconnects by [`run_script`].
//!
//! The runner owns connection lifecycle so scripts stay declarative:
//!
//! - actions that need a peer lazily accept a connection if none is open;
//! - actions that end the connection ([`ScriptAction::CloseNow`],
//!   [`ScriptAction::CloseAfterRequest`], [`ScriptAction::TruncatedReply`]) drop the socket, and
//!   the next action accepts a fresh one;
//! - [`ScriptAction::NextConnection`] is the explicit boundary for scripts where the *client*
//!   abandons the connection (for example after a correlation-id mismatch) and the server must not
//!   try to read from it.
//!
//! The runner's return value is the total number of request frames read across
//! every connection, surfaced through [`MockBroker::join`](super::MockBroker::join)
//! so tests can assert the client issued a bounded number of requests.

use std::time::Duration;

use bytes::{Bytes, BytesMut};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

use super::{Handler, read_frame};

/// Handler for [`ScriptAction::ReplyBatch`]: consumes every request frame read
/// by the batch and produces the bytes to write back — typically the matching
/// response frames concatenated in a deliberately different order.
pub(crate) type BatchHandler = Box<dyn FnOnce(Vec<Bytes>) -> BytesMut + Send>;

/// One step of a scripted-broker session.
///
/// Every variant that reads a request consumes exactly one frame (or `count`
/// frames for [`Self::ReplyBatch`]) so a script's shape maps one-to-one onto
/// the request sequence the client is expected to issue.
pub(crate) enum ScriptAction {
    /// Read one request and write the handler's frame(s).
    ///
    /// A handler may return several concatenated frames — for example a stray
    /// response followed by the real one — since the runner writes whatever it
    /// produces in a single `write_all`.
    Reply(Handler),
    /// Read one request, sleep for the delay, then write the handler's frame(s).
    DelayedReply(Duration, Handler),
    /// Read one request and write the handler's frame in `chunk_len`-byte
    /// chunks, flushing and pausing `gap` between chunks (a slow-loris writer).
    ChunkedReply {
        /// Bytes per write; must be non-zero.
        chunk_len: usize,
        /// Pause between chunks.
        gap: Duration,
        /// Builds the full frame that is then dribbled out.
        handler: Handler,
    },
    /// Read one request, write only the first `keep` bytes of the handler's
    /// frame, then close the connection.
    TruncatedReply {
        /// Byte prefix of the frame to deliver before closing.
        keep: usize,
        /// Builds the full frame that is then cut short.
        handler: Handler,
    },
    /// Read one request and write these bytes verbatim — garbage, a hand-rolled
    /// frame header, or an oversized length prefix. The connection stays open.
    RawReply(Vec<u8>),
    /// Read `count` requests before writing anything, then write whatever the
    /// handler builds from all of them (for example the matching responses in
    /// reverse order, to exercise out-of-order correlation).
    ReplyBatch {
        /// Requests to read before replying; must be non-zero.
        count: usize,
        /// Builds the combined reply from every request frame read.
        handler: BatchHandler,
    },
    /// Read one request and never answer it. The connection stays open and
    /// frame-aligned, so a later action can serve the client's next request.
    IgnoreRequest,
    /// Read one request, write the first `keep` bytes of the handler's frame,
    /// then hold the connection open — writing nothing further — until the
    /// client closes it. Models a peer that stalls mid-response.
    StallAfterPartialReply {
        /// Byte prefix of the frame to deliver before stalling.
        keep: usize,
        /// Builds the full frame whose prefix is delivered.
        handler: Handler,
    },
    /// Hold the current connection open — reading and discarding whatever the
    /// client sends, writing nothing — until the client closes it. Use as the
    /// final action after a reply the client will not accept, so the runner
    /// does not close the socket (and race an EOF against the behavior under
    /// test) before the client's own timeout fires.
    HoldOpen,
    /// Accept a connection if none is open, then close it immediately without
    /// reading a request. Models a broker that dies at accept time.
    CloseNow,
    /// Read one request, then close the connection without answering.
    CloseAfterRequest,
    /// Close the current connection, if any, without touching the listener.
    /// The next action that needs a peer accepts a fresh connection.
    NextConnection,
}

/// Execute `script` against `listener`, returning the total number of request
/// frames read. Built for [`MockBroker::serve_with`](super::MockBroker::serve_with):
///
/// ```ignore
/// let broker = MockBroker::serve_script(vec![
///     ScriptAction::CloseNow,
///     ScriptAction::Reply(Box::new(handshake)),
///     ScriptAction::Reply(Box::new(echo)),
/// ])
/// .await;
/// ```
pub(crate) async fn run_script(listener: TcpListener, script: Vec<ScriptAction>) -> usize {
    let mut requests: usize = 0;
    let mut connection: Option<TcpStream> = None;
    for action in script {
        if matches!(action, ScriptAction::NextConnection) {
            connection = None;
            continue;
        }
        let socket = current_connection(&listener, &mut connection).await;
        let outcome = perform(socket, action).await;
        requests = requests.saturating_add(outcome.requests_read);
        if outcome.close_connection {
            connection = None;
        }
    }
    requests
}

/// What one [`ScriptAction`] did: how many request frames it consumed and
/// whether it ended the connection (so the runner accepts a fresh one for the
/// next action).
struct ActionOutcome {
    requests_read: usize,
    close_connection: bool,
}

const fn keep_connection(requests_read: usize) -> ActionOutcome {
    ActionOutcome {
        requests_read,
        close_connection: false,
    }
}

const fn close_connection(requests_read: usize) -> ActionOutcome {
    ActionOutcome {
        requests_read,
        close_connection: true,
    }
}

/// Execute one action on the open connection.
async fn perform(socket: &mut TcpStream, action: ScriptAction) -> ActionOutcome {
    match action {
        ScriptAction::Reply(handler) => {
            let request = read_frame(socket).await;
            write_reply(socket, &handler(request)).await;
            keep_connection(1)
        },
        ScriptAction::DelayedReply(delay, handler) => {
            let request = read_frame(socket).await;
            let response = handler(request);
            tokio::time::sleep(delay).await;
            write_reply(socket, &response).await;
            keep_connection(1)
        },
        ScriptAction::ChunkedReply {
            chunk_len,
            gap,
            handler,
        } => {
            assert!(chunk_len > 0, "chunk_len must be non-zero");
            let request = read_frame(socket).await;
            let response = handler(request);
            for chunk in response.chunks(chunk_len) {
                write_reply(socket, chunk).await;
                tokio::time::sleep(gap).await;
            }
            keep_connection(1)
        },
        ScriptAction::TruncatedReply { keep, handler } => {
            let request = read_frame(socket).await;
            let mut response = handler(request);
            response.truncate(keep);
            write_reply(socket, &response).await;
            close_connection(1)
        },
        ScriptAction::RawReply(bytes) => {
            let _request = read_frame(socket).await;
            write_reply(socket, &bytes).await;
            keep_connection(1)
        },
        ScriptAction::ReplyBatch { count, handler } => {
            assert!(count > 0, "batch count must be non-zero");
            let mut batch = Vec::with_capacity(count);
            for _ in 0..count {
                batch.push(read_frame(socket).await);
            }
            write_reply(socket, &handler(batch)).await;
            keep_connection(count)
        },
        ScriptAction::IgnoreRequest => {
            let _request = read_frame(socket).await;
            keep_connection(1)
        },
        ScriptAction::StallAfterPartialReply { keep, handler } => {
            let request = read_frame(socket).await;
            let mut response = handler(request);
            response.truncate(keep);
            write_reply(socket, &response).await;
            drain_until_peer_close(socket).await;
            close_connection(1)
        },
        ScriptAction::HoldOpen => {
            drain_until_peer_close(socket).await;
            close_connection(0)
        },
        // `NextConnection` is handled by `run_script` before a connection is
        // accepted; it stays in the match so the enum remains total. Reaching
        // it here degrades to `CloseNow` semantics.
        ScriptAction::CloseNow | ScriptAction::NextConnection => close_connection(0),
        ScriptAction::CloseAfterRequest => {
            let _request = read_frame(socket).await;
            close_connection(1)
        },
    }
}

/// Write and flush one reply payload.
async fn write_reply(socket: &mut TcpStream, bytes: &[u8]) {
    socket.write_all(bytes).await.unwrap();
    socket.flush().await.unwrap();
}

/// Return the open connection, accepting a fresh one from `listener` if the
/// previous action closed it (or none was ever opened).
async fn current_connection<'connection>(
    listener: &TcpListener,
    connection: &'connection mut Option<TcpStream>,
) -> &'connection mut TcpStream {
    if connection.is_none() {
        let (socket, _addr) = listener.accept().await.unwrap();
        *connection = Some(socket);
    }
    connection.as_mut().expect("connection was just accepted")
}

/// Consume and discard everything the peer sends until it closes the
/// connection. Used by stalling actions so the script only advances once the
/// client has given up on the connection — never on a timer.
async fn drain_until_peer_close(socket: &mut TcpStream) {
    let mut sink = [0_u8; 1024];
    loop {
        match socket.read(&mut sink).await {
            Ok(0) | Err(_) => return,
            Ok(_ignored) => {},
        }
    }
}
