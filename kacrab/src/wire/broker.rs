//! Per-broker worker handle and TCP task.

use std::{
    collections::VecDeque,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::{Bytes, BytesMut};
use kacrab_protocol::{
    KafkaString, Result as ProtocolResult, frame,
    frame::RequestFrameSpec,
    generated::{
        ApiKey, ApiVersionsRequestData, ApiVersionsResponseData, ErrorCode,
        SaslAuthenticateRequestData, SaslAuthenticateResponseData, SaslHandshakeRequestData,
        SaslHandshakeResponseData,
    },
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, WriteHalf},
    sync::{mpsc, oneshot},
};

#[cfg(feature = "gssapi")]
use super::{
    GssapiAuthenticator,
    kerberos::{KerberosLoginManager, kerberos_service_name},
};
use super::{
    SaslClientAction, SaslClientAuthenticatorHandle, SaslClientSession, SaslMechanism,
    auth::{
        OAuthTokenCache, ScramExchange, oauthbearer_auth_bytes, plain_auth_bytes,
        validate_sasl_extension_hooks,
    },
    buffer::BufferPools,
    capabilities::BrokerCapabilities,
    config::{ConnectionConfig, TransportConfig},
    error::{Result, WireError},
    message::{RequestMessage, ResponseMessage},
    pipeline::{RequestPipeline, ResponseEnvelope},
    socket, tls,
};

/// `ApiVersions` v3 is the first flexible version with client software fields,
/// which lets brokers log kacrab identity during capability negotiation.
const API_VERSIONS_HANDSHAKE_VERSION: i16 = 3;
/// The handshake runs before the request pipeline exists, so correlation id
/// zero is reserved for this one synchronous capability exchange.
const HANDSHAKE_CORRELATION_ID: i32 = 0;
/// Correlation id used by the synchronous SASL mechanism handshake.
const SASL_HANDSHAKE_CORRELATION_ID: i32 = 1;
/// Correlation id used by the synchronous SASL authenticate request.
const SASL_AUTHENTICATE_CORRELATION_ID: i32 = 2;
/// Use the newest non-flexible `SaslHandshake` version generated from Kafka 4.3.
const SASL_HANDSHAKE_VERSION: i16 = 1;
/// Use the latest generated `SaslAuthenticate` version so session lifetime is available.
const SASL_AUTHENTICATE_VERSION: i16 = 2;
/// Timeout scanning should be frequent enough to clean expired requests quickly
/// but bounded so broker tasks do not spin on sub-millisecond intervals.
const MIN_TIMEOUT_TICK: Duration = Duration::from_millis(1);
/// Timeout scanning never needs to wait longer than 10ms on active sessions;
/// this keeps cleanup latency predictable under large request timeouts.
const MAX_TIMEOUT_TICK: Duration = Duration::from_millis(10);

/// Addressable Kafka broker endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerEndpoint {
    /// Broker node id from cluster metadata.
    pub node_id: i32,
    /// Advertised or configured broker host name.
    pub host: String,
    /// Advertised or configured broker port.
    pub port: u16,
    /// Plain TCP socket address for this broker.
    pub addr: SocketAddr,
}

impl BrokerEndpoint {
    /// Create a broker endpoint from a node id and socket address.
    #[must_use]
    pub fn new(node_id: i32, addr: SocketAddr) -> Self {
        Self {
            node_id,
            host: addr.ip().to_string(),
            port: addr.port(),
            addr,
        }
    }

    /// Create a broker endpoint from an advertised host, port, and resolved socket address.
    #[must_use]
    pub const fn from_resolved(node_id: i32, host: String, port: u16, addr: SocketAddr) -> Self {
        Self {
            node_id,
            host,
            port,
            addr,
        }
    }

    /// Return the advertised or configured broker host.
    #[must_use]
    pub fn host(&self) -> &str {
        &self.host
    }

    /// Return the advertised or configured broker port.
    #[must_use]
    pub const fn port(&self) -> u16 {
        self.port
    }
}

#[derive(Debug, Clone)]
pub(crate) struct BrokerHandle {
    tx: mpsc::Sender<RequestCommand>,
}

struct RequestCommand {
    api_key: ApiKey,
    max_api_version: i16,
    request: Box<dyn EncodableRequest>,
    enqueued_at: Instant,
    tx: oneshot::Sender<Result<ResponseEnvelope>>,
}

trait EncodableRequest: Send {
    fn encoded_len(&self, version: i16) -> ProtocolResult<usize>;
    fn write_body(&self, buf: &mut BytesMut, version: i16) -> ProtocolResult<()>;
}

struct OwnedRequest<Req> {
    request: Req,
}

impl<Req> EncodableRequest for OwnedRequest<Req>
where
    Req: RequestMessage + Send + Sync,
{
    fn encoded_len(&self, version: i16) -> ProtocolResult<usize> {
        self.request.encoded_len(version)
    }

    fn write_body(&self, buf: &mut BytesMut, version: i16) -> ProtocolResult<()> {
        self.request.write_request(buf, version)?;
        Ok(())
    }
}

trait BrokerIo: AsyncRead + AsyncWrite + Unpin + Send {}

impl<T> BrokerIo for T where T: AsyncRead + AsyncWrite + Unpin + Send {}

type BrokerStream = Box<dyn BrokerIo>;

impl BrokerHandle {
    pub(crate) fn spawn(
        endpoint: BrokerEndpoint,
        client_id: String,
        config: ConnectionConfig,
        buffers: Arc<BufferPools>,
        oauth_token_cache: Arc<tokio::sync::Mutex<OAuthTokenCache>>,
    ) -> Self {
        let (tx, rx) = mpsc::channel(config.broker_queue_capacity);
        #[cfg(feature = "gssapi")]
        let kerberos_login = KerberosLoginManager::new(&config.sasl);
        let task = BrokerTask {
            endpoint,
            client_id,
            config,
            buffers,
            oauth_token_cache,
            rx,
            #[cfg(feature = "gssapi")]
            kerberos_login,
        };
        let _task = tokio::spawn(task.run());
        Self { tx }
    }

    pub(crate) async fn send<Req, Resp>(
        &self,
        api_key: ApiKey,
        api_version: i16,
        request: &Req,
    ) -> Result<Resp>
    where
        Req: RequestMessage + Clone + Send + Sync + 'static,
        Resp: ResponseMessage,
    {
        let (tx, rx) = oneshot::channel();
        let command = RequestCommand {
            api_key,
            max_api_version: api_version,
            request: Box::new(OwnedRequest {
                request: request.clone(),
            }),
            enqueued_at: Instant::now(),
            tx,
        };
        self.tx.try_send(command).map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => WireError::Backpressure,
            mpsc::error::TrySendError::Closed(_) => WireError::ConnectionClosed,
        })?;

        let envelope = rx.await.map_err(|_| WireError::ConnectionClosed)??;
        decode_response::<Resp>(envelope)
    }
}

struct BrokerTask {
    endpoint: BrokerEndpoint,
    client_id: String,
    config: ConnectionConfig,
    buffers: Arc<BufferPools>,
    oauth_token_cache: Arc<tokio::sync::Mutex<OAuthTokenCache>>,
    rx: mpsc::Receiver<RequestCommand>,
    #[cfg(feature = "gssapi")]
    kerberos_login: KerberosLoginManager,
}

impl BrokerTask {
    async fn run(mut self) {
        let mut pending = VecDeque::new();
        let mut rx_open = true;
        let mut backoff = reconnect_backoff_initial(&self.config);
        loop {
            if pending.is_empty() && rx_open {
                match self.rx.recv().await {
                    Some(command) => pending.push_back(command),
                    None => return,
                }
            }

            expire_pending_commands(&mut pending, self.config.request_timeout);
            if pending.is_empty() {
                if rx_open {
                    continue;
                }
                return;
            }

            match self.connect_and_negotiate().await {
                Ok((stream, capabilities)) => {
                    backoff = reconnect_backoff_initial(&self.config);
                    if matches!(
                        self.serve_connection(stream, capabilities, &mut pending, &mut rx_open)
                            .await,
                        ServeOutcome::Closed
                    ) && pending.is_empty()
                    {
                        return;
                    }
                },
                Err(error) => {
                    if let Some(error_factory) = fatal_setup_error_factory(&error) {
                        fail_pending_setup_error(&mut pending, error_factory);
                        if pending.is_empty() && !rx_open {
                            return;
                        }
                        continue;
                    }
                    expire_pending_commands(&mut pending, self.config.request_timeout);
                    if pending.is_empty() && !rx_open {
                        return;
                    }
                    tokio::time::sleep(backoff).await;
                    backoff = next_reconnect_backoff(backoff, self.config.reconnect_backoff_max);
                },
            }
        }
    }

    async fn serve_connection(
        &mut self,
        stream: BrokerStream,
        capabilities: BrokerCapabilities,
        pending: &mut VecDeque<RequestCommand>,
        rx_open: &mut bool,
    ) -> ServeOutcome {
        let (reader, mut writer) = tokio::io::split(stream);
        let (response_tx, mut response_rx) =
            mpsc::channel(self.config.max_in_flight_requests_per_connection);
        let _reader_task = tokio::spawn(read_response_frames(
            reader,
            response_tx,
            self.config.read_buffer_capacity,
            Arc::clone(&self.buffers),
        ));

        let mut pipeline = RequestPipeline::new(
            self.config.max_in_flight_requests_per_connection,
            self.config.request_timeout,
        );
        let mut timeout_tick = tokio::time::interval(timeout_tick_duration(&self.config));

        loop {
            if self
                .flush_pending(&mut writer, &mut pipeline, pending, &capabilities)
                .await
                .is_err()
            {
                pipeline.fail_all();
                return ServeOutcome::Disconnected;
            }
            tokio::select! {
                maybe_command = self.rx.recv() => {
                    let Some(command) = maybe_command else {
                        *rx_open = false;
                        pipeline.fail_all();
                        return ServeOutcome::Closed;
                    };
                    if pipeline.has_capacity() && pending.is_empty() {
                        pending.push_back(command);
                    } else {
                        let _ignored = command.tx.send(Err(WireError::Backpressure));
                    }
                },
                maybe_response = response_rx.recv() => {
                    let Some(response) = maybe_response else {
                        pipeline.fail_all();
                        return ServeOutcome::Disconnected;
                    };
                    pipeline.complete_response(response);
                },
                _ = timeout_tick.tick() => {
                    pipeline.fail_expired();
                    expire_pending_commands(pending, self.config.request_timeout);
                },
            }
        }
    }

    async fn flush_pending(
        &self,
        writer: &mut WriteHalf<BrokerStream>,
        pipeline: &mut RequestPipeline,
        pending: &mut VecDeque<RequestCommand>,
        capabilities: &BrokerCapabilities,
    ) -> Result<()> {
        let mut wrote_any = false;
        while pipeline.has_capacity() {
            let Some(command) = pending.pop_front() else {
                break;
            };
            if self
                .write_command(writer, pipeline, command, capabilities)
                .await?
            {
                wrote_any = true;
            }
        }
        if wrote_any && let Err(error) = writer.flush().await {
            pipeline.fail_all();
            return Err(WireError::Io(error));
        }
        Ok(())
    }

    async fn write_command(
        &self,
        writer: &mut WriteHalf<BrokerStream>,
        pipeline: &mut RequestPipeline,
        command: RequestCommand,
        capabilities: &BrokerCapabilities,
    ) -> Result<bool> {
        let RequestCommand {
            api_key,
            max_api_version,
            request,
            tx,
            ..
        } = command;
        let Some(api_version) = capabilities.version_for_limit(api_key, max_api_version) else {
            let _ignored = tx.send(Err(WireError::UnsupportedApiVersion(api_key)));
            return Ok(false);
        };
        let body_len = match request.encoded_len(api_version) {
            Ok(body_len) => body_len,
            Err(error) => {
                let _ignored = tx.send(Err(error.into()));
                return Ok(false);
            },
        };
        let correlation_id = match pipeline.reserve(api_key, api_version, tx) {
            Ok(correlation_id) => correlation_id,
            Err(tx) => {
                let _ignored = tx.send(Err(WireError::Backpressure));
                return Ok(false);
            },
        };
        let spec = RequestFrameSpec {
            api_key,
            api_version,
            correlation_id,
            client_id: &self.client_id,
            capacity_hint: 0,
        };
        let frame = match self.encode_request_frame(spec, body_len, &*request) {
            Ok(frame) => frame,
            Err(error) => {
                pipeline.fail_correlation(correlation_id, error);
                return Ok(false);
            },
        };
        if let Err(error) = writer.write_all(&frame).await {
            self.buffers.release_write(frame);
            pipeline.fail_correlation(correlation_id, WireError::Io(error));
            return Err(WireError::ConnectionClosed);
        }
        self.buffers.release_write(frame);
        Ok(true)
    }

    async fn connect_and_negotiate(&self) -> Result<(BrokerStream, BrokerCapabilities)> {
        let tcp = match self.config.transport {
            TransportConfig::Plaintext => {
                socket::connect(
                    &self.config.socket,
                    self.config.socket_connection_setup_timeout,
                    self.endpoint.addr,
                )
                .await?
            },
        };
        let mut stream: BrokerStream = if self.config.security.protocol.uses_tls() {
            Box::new(tls::connect_client(tcp, &self.config.tls, &self.tls_server_name()).await?)
        } else {
            Box::new(tcp)
        };
        let capabilities = self.api_versions(&mut stream).await?;
        if self.config.security.protocol.uses_sasl() {
            self.sasl_authenticate(&mut stream).await?;
        }
        Ok((stream, capabilities))
    }

    fn tls_server_name(&self) -> String {
        self.endpoint.host.clone()
    }

    async fn api_versions(&self, stream: &mut BrokerStream) -> Result<BrokerCapabilities> {
        let request = ApiVersionsRequestData {
            client_software_name: KafkaString::from("kacrab".to_owned()),
            client_software_version: KafkaString::from(env!("CARGO_PKG_VERSION").to_owned()),
            _unknown_tagged_fields: Vec::new(),
        };
        let api_version = API_VERSIONS_HANDSHAKE_VERSION;
        let body_len = request.encoded_len(api_version)?;
        let capacity_hint = self.request_frame_capacity_hint(
            ApiKey::ApiVersions,
            api_version,
            HANDSHAKE_CORRELATION_ID,
            body_len,
        )?;
        let frame = frame::encode_request_frame(
            RequestFrameSpec {
                api_key: ApiKey::ApiVersions,
                api_version,
                correlation_id: HANDSHAKE_CORRELATION_ID,
                client_id: &self.client_id,
                capacity_hint,
            },
            |buf| {
                request.write(buf, api_version)?;
                Ok(())
            },
        )?;
        stream.write_all(&frame).await?;
        stream.flush().await?;

        let response_bytes =
            read_frame(stream, self.config.read_buffer_capacity, &self.buffers).await?;
        let mut response =
            frame::decode_response_envelope(ApiKey::ApiVersions, api_version, response_bytes)?;
        if response.correlation_id != HANDSHAKE_CORRELATION_ID {
            return Err(WireError::CorrelationIdMismatch {
                expected: HANDSHAKE_CORRELATION_ID,
                actual: response.correlation_id,
            });
        }
        let response = ApiVersionsResponseData::read(&mut response.body, api_version)?;
        let error = ErrorCode::from(response.error_code);
        if error.is_error() {
            return Err(WireError::Kafka(error));
        }
        Ok(BrokerCapabilities::from_response(&response))
    }

    async fn sasl_authenticate(&self, stream: &mut BrokerStream) -> Result<()> {
        validate_sasl_extension_hooks(&self.config.sasl)?;
        if let Some(factory) = &self.config.sasl.client_authenticator_factory {
            let session = SaslClientSession::new(
                self.endpoint.node_id,
                self.endpoint.host.clone(),
                self.endpoint.port,
                self.endpoint.addr,
            );
            let authenticator = factory.create(&session)?;
            return self.sasl_custom_authenticate(stream, &authenticator).await;
        }
        if let Some(authenticator) = &self.config.sasl.client_authenticator {
            return self.sasl_custom_authenticate(stream, authenticator).await;
        }
        let mechanism = self.config.sasl.mechanism.unwrap_or(SaslMechanism::Gssapi);
        if mechanism == SaslMechanism::Gssapi {
            #[cfg(feature = "gssapi")]
            {
                return self.sasl_gssapi_authenticate(stream).await;
            }
            #[cfg(not(feature = "gssapi"))]
            {
                return Err(WireError::GssapiBackendUnavailable);
            }
        }
        self.sasl_handshake(stream, mechanism).await?;
        let auth_bytes = match mechanism {
            SaslMechanism::Plain => plain_auth_bytes(self.config.sasl.jaas_config.as_deref())?,
            SaslMechanism::ScramSha256 | SaslMechanism::ScramSha512 => {
                return self.sasl_scram_authenticate(stream, mechanism).await;
            },
            SaslMechanism::OAuthBearer => {
                oauthbearer_auth_bytes(&self.config.sasl, &self.config.tls, &self.oauth_token_cache)
                    .await?
            },
            SaslMechanism::Gssapi => return Err(WireError::GssapiBackendUnavailable),
        };
        let _response = self.sasl_authenticate_round(stream, auth_bytes).await?;
        Ok(())
    }

    async fn sasl_custom_authenticate(
        &self,
        stream: &mut BrokerStream,
        authenticator: &SaslClientAuthenticatorHandle,
    ) -> Result<()> {
        self.sasl_handshake(stream, authenticator.mechanism())
            .await?;
        let mut action = authenticator.start()?;
        loop {
            let SaslClientAction::Send(auth_bytes) = action else {
                return Ok(());
            };
            let response = self.sasl_authenticate_round(stream, auth_bytes).await?;
            action = authenticator.next(response.auth_bytes.as_ref())?;
        }
    }

    #[cfg(feature = "gssapi")]
    async fn sasl_gssapi_authenticate(&self, stream: &mut BrokerStream) -> Result<()> {
        let service_name = kerberos_service_name(&self.config.sasl)?;
        let authenticator = SaslClientAuthenticatorHandle::new(
            GssapiAuthenticator::new(service_name, self.gssapi_host_name())
                .with_kerberos_login(self.kerberos_login.clone()),
        );
        self.sasl_custom_authenticate(stream, &authenticator).await
    }

    #[cfg(feature = "gssapi")]
    fn gssapi_host_name(&self) -> String {
        self.endpoint.host.clone()
    }

    async fn sasl_scram_authenticate(
        &self,
        stream: &mut BrokerStream,
        mechanism: SaslMechanism,
    ) -> Result<()> {
        let (exchange, client_first) =
            ScramExchange::start(mechanism, self.config.sasl.jaas_config.as_deref())?;
        let server_first = self.sasl_authenticate_round(stream, client_first).await?;
        let (client_final, expected_signature) =
            exchange.client_final(server_first.auth_bytes.as_ref())?;
        let server_final = self.sasl_authenticate_round(stream, client_final).await?;
        ScramExchange::verify_server_final(server_final.auth_bytes.as_ref(), &expected_signature)
    }

    async fn sasl_authenticate_round(
        &self,
        stream: &mut BrokerStream,
        auth_bytes: Bytes,
    ) -> Result<SaslAuthenticateResponseData> {
        let request = SaslAuthenticateRequestData {
            auth_bytes,
            _unknown_tagged_fields: Vec::new(),
        };
        let body_len = request.encoded_len(SASL_AUTHENTICATE_VERSION)?;
        let capacity_hint = self.request_frame_capacity_hint(
            ApiKey::SaslAuthenticate,
            SASL_AUTHENTICATE_VERSION,
            SASL_AUTHENTICATE_CORRELATION_ID,
            body_len,
        )?;
        let frame = frame::encode_request_frame(
            RequestFrameSpec {
                api_key: ApiKey::SaslAuthenticate,
                api_version: SASL_AUTHENTICATE_VERSION,
                correlation_id: SASL_AUTHENTICATE_CORRELATION_ID,
                client_id: &self.client_id,
                capacity_hint,
            },
            |buf| {
                request.write(buf, SASL_AUTHENTICATE_VERSION)?;
                Ok(())
            },
        )?;
        stream.write_all(&frame).await?;
        stream.flush().await?;

        let response_bytes =
            read_frame(stream, self.config.read_buffer_capacity, &self.buffers).await?;
        let mut envelope = frame::decode_response_envelope(
            ApiKey::SaslAuthenticate,
            SASL_AUTHENTICATE_VERSION,
            response_bytes,
        )?;
        if envelope.correlation_id != SASL_AUTHENTICATE_CORRELATION_ID {
            return Err(WireError::CorrelationIdMismatch {
                expected: SASL_AUTHENTICATE_CORRELATION_ID,
                actual: envelope.correlation_id,
            });
        }
        let response =
            SaslAuthenticateResponseData::read(&mut envelope.body, SASL_AUTHENTICATE_VERSION)?;
        let error = ErrorCode::from(response.error_code);
        if error.is_error() {
            let message = response
                .error_message
                .map_or_else(|| error.to_string(), |message| message.to_string());
            return Err(WireError::SaslAuthentication(message));
        }
        Ok(response)
    }

    async fn sasl_handshake(
        &self,
        stream: &mut BrokerStream,
        mechanism: SaslMechanism,
    ) -> Result<()> {
        let request = SaslHandshakeRequestData {
            mechanism: KafkaString::from(mechanism.as_str().to_owned()),
            _unknown_tagged_fields: Vec::new(),
        };
        let body_len = request.encoded_len(SASL_HANDSHAKE_VERSION)?;
        let capacity_hint = self.request_frame_capacity_hint(
            ApiKey::SaslHandshake,
            SASL_HANDSHAKE_VERSION,
            SASL_HANDSHAKE_CORRELATION_ID,
            body_len,
        )?;
        let frame = frame::encode_request_frame(
            RequestFrameSpec {
                api_key: ApiKey::SaslHandshake,
                api_version: SASL_HANDSHAKE_VERSION,
                correlation_id: SASL_HANDSHAKE_CORRELATION_ID,
                client_id: &self.client_id,
                capacity_hint,
            },
            |buf| {
                request.write(buf, SASL_HANDSHAKE_VERSION)?;
                Ok(())
            },
        )?;
        stream.write_all(&frame).await?;
        stream.flush().await?;

        let response_bytes =
            read_frame(stream, self.config.read_buffer_capacity, &self.buffers).await?;
        let mut envelope = frame::decode_response_envelope(
            ApiKey::SaslHandshake,
            SASL_HANDSHAKE_VERSION,
            response_bytes,
        )?;
        if envelope.correlation_id != SASL_HANDSHAKE_CORRELATION_ID {
            return Err(WireError::CorrelationIdMismatch {
                expected: SASL_HANDSHAKE_CORRELATION_ID,
                actual: envelope.correlation_id,
            });
        }
        let response = SaslHandshakeResponseData::read(&mut envelope.body, SASL_HANDSHAKE_VERSION)?;
        let error = ErrorCode::from(response.error_code);
        if error.is_error() {
            return Err(WireError::SaslHandshake(error.to_string()));
        }
        let supported = response
            .mechanisms
            .iter()
            .any(|supported| supported.as_str() == mechanism.as_str());
        if !supported {
            return Err(WireError::UnsupportedSaslMechanism(
                mechanism.as_str().to_owned(),
            ));
        }
        Ok(())
    }

    fn encode_request_frame(
        &self,
        spec: RequestFrameSpec<'_>,
        body_len: usize,
        request: &dyn EncodableRequest,
    ) -> Result<BytesMut> {
        let capacity_hint = frame::request_frame_capacity_hint(spec, body_len)?;
        let mut frame = self.buffers.acquire_write(capacity_hint);
        frame::encode_request_frame_with_buffer(
            &mut frame,
            RequestFrameSpec {
                capacity_hint,
                ..spec
            },
            |buf| request.write_body(buf, spec.api_version),
        )?;
        Ok(frame)
    }

    fn request_frame_capacity_hint(
        &self,
        api_key: ApiKey,
        api_version: i16,
        correlation_id: i32,
        body_len: usize,
    ) -> Result<usize> {
        Ok(frame::request_frame_capacity_hint(
            RequestFrameSpec {
                api_key,
                api_version,
                correlation_id,
                client_id: &self.client_id,
                capacity_hint: 0,
            },
            body_len,
        )?)
    }
}

enum ServeOutcome {
    Disconnected,
    Closed,
}

fn expire_pending_commands(pending: &mut VecDeque<RequestCommand>, request_timeout: Duration) {
    let now = Instant::now();
    let mut retained = VecDeque::with_capacity(pending.len());
    while let Some(command) = pending.pop_front() {
        if now.duration_since(command.enqueued_at) >= request_timeout {
            let _ignored = command.tx.send(Err(WireError::Timeout));
        } else {
            retained.push_back(command);
        }
    }
    *pending = retained;
}

fn fail_pending_setup_error(
    pending: &mut VecDeque<RequestCommand>,
    error_factory: fn() -> WireError,
) {
    while let Some(command) = pending.pop_front() {
        let _ignored = command.tx.send(Err(error_factory()));
    }
}

fn fatal_setup_error_factory(error: &WireError) -> Option<fn() -> WireError> {
    match error {
        WireError::UnsupportedTlsOption(_) => Some(unsupported_tls_backend_error),
        WireError::GssapiBackendUnavailable => Some(gssapi_backend_unavailable_error),
        WireError::InvalidSaslConfig(_) => Some(invalid_sasl_config_error),
        WireError::UnsupportedSaslMechanism(_) => Some(unsupported_sasl_mechanism_error),
        _ => None,
    }
}

fn unsupported_tls_backend_error() -> WireError {
    WireError::UnsupportedTlsOption("tls-rustls backend is not wired yet".to_owned())
}

const fn gssapi_backend_unavailable_error() -> WireError {
    WireError::GssapiBackendUnavailable
}

fn invalid_sasl_config_error() -> WireError {
    WireError::InvalidSaslConfig("invalid SASL config".to_owned())
}

fn unsupported_sasl_mechanism_error() -> WireError {
    WireError::UnsupportedSaslMechanism("unsupported SASL mechanism".to_owned())
}

fn reconnect_backoff_initial(config: &ConnectionConfig) -> Duration {
    config
        .reconnect_backoff_initial
        .max(MIN_TIMEOUT_TICK)
        .min(reconnect_backoff_max(config))
}

fn reconnect_backoff_max(config: &ConnectionConfig) -> Duration {
    config.reconnect_backoff_max.max(MIN_TIMEOUT_TICK)
}

fn next_reconnect_backoff(current: Duration, max: Duration) -> Duration {
    current.saturating_mul(2).min(max.max(MIN_TIMEOUT_TICK))
}

async fn read_response_frames<R>(
    mut reader: R,
    tx: mpsc::Sender<Bytes>,
    read_buffer_capacity: Option<usize>,
    buffers: Arc<BufferPools>,
) where
    R: AsyncRead + Unpin,
{
    loop {
        let Ok(frame) = read_frame(&mut reader, read_buffer_capacity, &buffers).await else {
            return;
        };
        if tx.send(frame).await.is_err() {
            return;
        }
    }
}

async fn read_frame<R>(
    reader: &mut R,
    read_buffer_capacity: Option<usize>,
    buffers: &BufferPools,
) -> Result<Bytes>
where
    R: AsyncRead + Unpin,
{
    let length = reader.read_i32().await?;
    if !(0..=frame::MAX_FRAME_LENGTH).contains(&length) {
        return Err(WireError::ConnectionClosed);
    }

    let length = usize::try_from(length).map_err(|_| WireError::ConnectionClosed)?;
    let capacity = read_buffer_capacity.map_or(length, |capacity| length.max(capacity));
    let mut payload = buffers.acquire_read(capacity);
    payload.resize(length, 0);
    let _bytes_read = reader.read_exact(&mut payload[..]).await?;
    let frozen = payload.split_to(length).freeze();
    buffers.release_read(payload);
    Ok(frozen)
}

fn decode_response<Resp>(mut envelope: ResponseEnvelope) -> Result<Resp>
where
    Resp: ResponseMessage,
{
    Ok(Resp::read_response(
        &mut envelope.body,
        envelope.api_version,
    )?)
}

fn timeout_tick_duration(config: &ConnectionConfig) -> Duration {
    let timeout = config.request_timeout;
    let half_timeout = timeout.checked_div(2).unwrap_or(timeout);
    half_timeout.min(MAX_TIMEOUT_TICK).max(MIN_TIMEOUT_TICK)
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::expect_used,
        clippy::missing_assert_message,
        clippy::unwrap_used,
        reason = "Unit test fixtures fail fastest with contextual unwrap/expect calls."
    )]

    use std::{
        collections::VecDeque,
        sync::Arc,
        time::{Duration, Instant},
    };

    use bytes::{Bytes, BytesMut};
    use kacrab_protocol::{
        KafkaString, frame,
        frame::RequestFrameSpec,
        generated::{
            ApiKey, ApiVersion, ApiVersionsRequestData, ApiVersionsResponseData, ErrorCode,
            RequestHeaderData, SaslAuthenticateRequestData, SaslAuthenticateResponseData,
            SaslHandshakeResponseData,
        },
        version::{request_header_version, response_header_version},
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
        sync::{mpsc, oneshot},
    };

    #[cfg(feature = "gssapi")]
    use super::KerberosLoginManager;
    use super::{
        BrokerCapabilities, BrokerEndpoint, BrokerHandle, BrokerStream, BrokerTask, BufferPools,
        EncodableRequest, OAuthTokenCache, OwnedRequest, RequestCommand, RequestPipeline,
        ResponseEnvelope, ServeOutcome, expire_pending_commands, next_reconnect_backoff,
        read_frame, read_response_frames, reconnect_backoff_initial, reconnect_backoff_max,
        timeout_tick_duration,
    };
    use crate::wire::{
        ConnectionConfig, Result as WireResult, SaslClientAction, SaslClientAuthenticator,
        SaslClientAuthenticatorFactory, SaslClientAuthenticatorHandle, SaslClientSession,
        SaslMechanism, SecurityProtocol, WireError,
    };

    #[derive(Debug)]
    struct StaticSaslAuthenticator {
        mechanism: SaslMechanism,
        payload: Bytes,
    }

    impl SaslClientAuthenticator for StaticSaslAuthenticator {
        fn mechanism(&self) -> SaslMechanism {
            self.mechanism
        }

        fn start(&self) -> WireResult<SaslClientAction> {
            Ok(SaslClientAction::Send(self.payload.clone()))
        }

        fn next(&self, _challenge: &[u8]) -> WireResult<SaslClientAction> {
            Ok(SaslClientAction::Complete)
        }
    }

    #[derive(Debug)]
    struct SessionPayloadFactory;

    impl SaslClientAuthenticatorFactory for SessionPayloadFactory {
        fn mechanism(&self) -> SaslMechanism {
            SaslMechanism::Plain
        }

        fn create(&self, session: &SaslClientSession) -> WireResult<SaslClientAuthenticatorHandle> {
            Ok(SaslClientAuthenticatorHandle::new(
                StaticSaslAuthenticator {
                    mechanism: SaslMechanism::Plain,
                    payload: Bytes::from(format!(
                        "{}:{}:{}",
                        session.node_id(),
                        session.host(),
                        session.port()
                    )),
                },
            ))
        }
    }

    fn api_versions_request() -> ApiVersionsRequestData {
        ApiVersionsRequestData {
            client_software_name: KafkaString::from("kacrab".to_owned()),
            client_software_version: KafkaString::from("0.0.1".to_owned()),
            _unknown_tagged_fields: Vec::new(),
        }
    }

    fn request_command() -> RequestCommand {
        let (tx, _rx) = oneshot::channel();
        request_command_with_sender(tx, Instant::now())
    }

    fn request_command_with_sender(
        tx: oneshot::Sender<WireResult<ResponseEnvelope>>,
        enqueued_at: Instant,
    ) -> RequestCommand {
        RequestCommand {
            api_key: ApiKey::ApiVersions,
            max_api_version: 3,
            request: Box::new(OwnedRequest {
                request: api_versions_request(),
            }),
            enqueued_at,
            tx,
        }
    }

    fn api_versions_capabilities() -> BrokerCapabilities {
        BrokerCapabilities::from_response(&ApiVersionsResponseData {
            api_keys: vec![ApiVersion {
                api_key: ApiKey::ApiVersions as i16,
                min_version: 0,
                max_version: 4,
                _unknown_tagged_fields: Vec::new(),
            }],
            ..ApiVersionsResponseData::default()
        })
    }

    fn api_versions_response(correlation_id: i32, error: ErrorCode) -> BytesMut {
        let mut header = BytesMut::new();
        kacrab_protocol::generated::ResponseHeaderData {
            correlation_id,
            _unknown_tagged_fields: Vec::new(),
        }
        .write(
            &mut header,
            response_header_version(ApiKey::ApiVersions as i16, 3),
        )
        .expect("response header");
        let mut body = BytesMut::new();
        ApiVersionsResponseData {
            error_code: error.code(),
            ..ApiVersionsResponseData::default()
        }
        .write(&mut body, 3)
        .expect("api versions response");
        frame::encode_request(&header, &body).expect("response frame")
    }

    fn sasl_handshake_response(correlation_id: i32, mechanism: &str) -> BytesMut {
        let mut header = BytesMut::new();
        kacrab_protocol::generated::ResponseHeaderData {
            correlation_id,
            _unknown_tagged_fields: Vec::new(),
        }
        .write(
            &mut header,
            response_header_version(ApiKey::SaslHandshake as i16, 1),
        )
        .expect("response header");
        let mut body = BytesMut::new();
        SaslHandshakeResponseData {
            error_code: ErrorCode::None.code(),
            mechanisms: vec![KafkaString::from(mechanism.to_owned())],
            _unknown_tagged_fields: Vec::new(),
        }
        .write(&mut body, 1)
        .expect("sasl handshake response");
        frame::encode_request(&header, &body).expect("response frame")
    }

    fn sasl_authenticate_response(correlation_id: i32) -> BytesMut {
        let mut header = BytesMut::new();
        kacrab_protocol::generated::ResponseHeaderData {
            correlation_id,
            _unknown_tagged_fields: Vec::new(),
        }
        .write(
            &mut header,
            response_header_version(ApiKey::SaslAuthenticate as i16, 2),
        )
        .expect("response header");
        let mut body = BytesMut::new();
        SaslAuthenticateResponseData {
            error_code: ErrorCode::None.code(),
            error_message: None,
            auth_bytes: Bytes::new(),
            session_lifetime_ms: 0,
            _unknown_tagged_fields: Vec::new(),
        }
        .write(&mut body, 2)
        .expect("sasl authenticate response");
        frame::encode_request(&header, &body).expect("response frame")
    }

    async fn broker_task_with_connected_stream()
    -> (BrokerTask, tokio::net::TcpStream, tokio::net::TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("listener addr");
        let client = tokio::net::TcpStream::connect(addr)
            .await
            .expect("client connect");
        let (server, _peer) = listener.accept().await.expect("server accept");
        let (_tx, rx) = mpsc::channel(1);
        let config = ConnectionConfig::default();
        #[cfg(feature = "gssapi")]
        let kerberos_login = KerberosLoginManager::new(&config.sasl);
        let task = BrokerTask {
            endpoint: BrokerEndpoint::new(7, addr),
            client_id: "client-a".to_owned(),
            config,
            buffers: Arc::new(BufferPools::new(1)),
            oauth_token_cache: Arc::new(tokio::sync::Mutex::new(OAuthTokenCache::default())),
            rx,
            #[cfg(feature = "gssapi")]
            kerberos_login,
        };
        (task, client, server)
    }

    #[test]
    fn reconnect_backoff_is_clamped_and_doubles_to_max() {
        let config = ConnectionConfig::default()
            .reconnect_backoff_initial(Duration::ZERO)
            .reconnect_backoff_max(Duration::from_millis(3));

        assert_eq!(reconnect_backoff_initial(&config), Duration::from_millis(1));
        assert_eq!(reconnect_backoff_max(&config), Duration::from_millis(3));
        assert_eq!(
            next_reconnect_backoff(Duration::from_millis(2), Duration::from_millis(3)),
            Duration::from_millis(3)
        );
    }

    #[test]
    fn timeout_tick_duration_stays_between_floor_and_ceiling() {
        assert_eq!(
            timeout_tick_duration(&ConnectionConfig::default().request_timeout(Duration::ZERO)),
            Duration::from_millis(1)
        );
        assert_eq!(
            timeout_tick_duration(
                &ConnectionConfig::default().request_timeout(Duration::from_mins(1))
            ),
            Duration::from_millis(10)
        );
    }

    #[tokio::test]
    async fn expire_pending_commands_sends_timeout_and_retains_fresh_commands() {
        let (expired_tx, expired_rx) = oneshot::channel();
        let (fresh_tx, fresh_rx) = oneshot::channel();
        let now = Instant::now();
        let expired_at = now.checked_sub(Duration::from_mins(1)).unwrap_or(now);
        let mut pending = VecDeque::from([
            request_command_with_sender(expired_tx, expired_at),
            request_command_with_sender(fresh_tx, now),
        ]);

        expire_pending_commands(&mut pending, Duration::from_secs(30));

        assert!(matches!(
            expired_rx.await.expect("expired sender"),
            Err(WireError::Timeout)
        ));
        assert_eq!(pending.len(), 1);
        drop(pending);
        assert!(fresh_rx.await.is_err());
    }

    #[tokio::test]
    async fn broker_handle_reports_full_and_closed_queue() {
        let (tx, mut rx) = mpsc::channel(1);
        tx.try_send(request_command()).expect("prefill queue");
        let full = BrokerHandle { tx };

        assert!(matches!(
            full.send::<_, ApiVersionsResponseData>(
                ApiKey::ApiVersions,
                3,
                &api_versions_request()
            )
            .await,
            Err(WireError::Backpressure)
        ));
        let _dropped = rx.recv().await;

        let (tx, rx) = mpsc::channel(1);
        drop(rx);
        let closed = BrokerHandle { tx };
        assert!(matches!(
            closed
                .send::<_, ApiVersionsResponseData>(ApiKey::ApiVersions, 3, &api_versions_request())
                .await,
            Err(WireError::ConnectionClosed)
        ));
    }

    #[test]
    fn broker_task_encode_frame_wraps_body_with_client_header() {
        let config = ConnectionConfig::default();
        #[cfg(feature = "gssapi")]
        let kerberos_login = KerberosLoginManager::new(&config.sasl);
        let task = BrokerTask {
            endpoint: BrokerEndpoint::new(7, "127.0.0.1:9092".parse().expect("socket address")),
            client_id: "client-a".to_owned(),
            config,
            buffers: Arc::new(BufferPools::new(1)),
            oauth_token_cache: Arc::new(tokio::sync::Mutex::new(OAuthTokenCache::default())),
            rx: mpsc::channel(1).1,
            #[cfg(feature = "gssapi")]
            kerberos_login,
        };

        let request = OwnedRequest {
            request: api_versions_request(),
        };
        let body_len = request.encoded_len(3).expect("body length");
        let expected_len = task
            .request_frame_capacity_hint(ApiKey::ApiVersions, 3, 9, body_len)
            .expect("frame capacity hint");
        let frame = task
            .encode_request_frame(
                RequestFrameSpec {
                    api_key: ApiKey::ApiVersions,
                    api_version: 3,
                    correlation_id: 9,
                    client_id: &task.client_id,
                    capacity_hint: 0,
                },
                body_len,
                &request,
            )
            .expect("encoded frame");

        assert_eq!(frame.len(), expected_len);
    }

    #[tokio::test]
    async fn api_versions_rejects_mismatched_correlation_and_broker_error() {
        let (task, client, mut server) = broker_task_with_connected_stream().await;
        let mut client: BrokerStream = Box::new(client);
        let server_task = tokio::spawn(async move {
            let _request_len = server.read_i32().await.expect("request length");
            server
                .write_all(&api_versions_response(99, ErrorCode::None))
                .await
                .expect("write response");
        });

        assert!(matches!(
            task.api_versions(&mut client).await,
            Err(WireError::CorrelationIdMismatch {
                expected: 0,
                actual: 99
            })
        ));
        server_task.await.expect("server task");

        let (task, client, mut server) = broker_task_with_connected_stream().await;
        let mut client: BrokerStream = Box::new(client);
        let server_task = tokio::spawn(async move {
            let _request_len = server.read_i32().await.expect("request length");
            server
                .write_all(&api_versions_response(0, ErrorCode::UnsupportedVersion))
                .await
                .expect("write response");
        });

        assert!(matches!(
            task.api_versions(&mut client).await,
            Err(WireError::Kafka(ErrorCode::UnsupportedVersion))
        ));
        server_task.await.expect("server task");
    }

    #[tokio::test]
    async fn sasl_authenticate_uses_native_rust_authenticator_payload() {
        let (mut task, client, mut server) = broker_task_with_connected_stream().await;
        task.config.security.protocol = SecurityProtocol::SaslPlaintext;
        task.config.sasl = task
            .config
            .sasl
            .clone()
            .client_authenticator(StaticSaslAuthenticator {
                mechanism: SaslMechanism::Plain,
                payload: Bytes::from_static(b"native-hook-payload"),
            });
        let mut client: BrokerStream = Box::new(client);
        let server_task = tokio::spawn(async move {
            let _handshake_request = read_frame(&mut server, None, &BufferPools::new(1))
                .await
                .expect("read handshake request");
            server
                .write_all(&sasl_handshake_response(1, "PLAIN"))
                .await
                .expect("write handshake response");
            let authenticate_request = read_frame(&mut server, None, &BufferPools::new(1))
                .await
                .expect("read authenticate request");
            let mut body = authenticate_request;
            let _header = RequestHeaderData::read(
                &mut body,
                request_header_version(ApiKey::SaslAuthenticate as i16, 2),
            )
            .expect("decode authenticate request header");
            let request = SaslAuthenticateRequestData::read(&mut body, 2)
                .expect("read authenticate request body");
            server
                .write_all(&sasl_authenticate_response(2))
                .await
                .expect("write authenticate response");
            request.auth_bytes
        });

        task.sasl_authenticate(&mut client)
            .await
            .expect("sasl authenticate");
        let auth_bytes = server_task.await.expect("server task");

        assert_eq!(auth_bytes, Bytes::from_static(b"native-hook-payload"));
    }

    #[tokio::test]
    async fn sasl_authenticate_uses_factory_session_hostname() {
        let (mut task, client, mut server) = broker_task_with_connected_stream().await;
        task.endpoint = BrokerEndpoint::from_resolved(
            7,
            "broker.example.com".to_owned(),
            9092,
            task.endpoint.addr,
        );
        task.config.security.protocol = SecurityProtocol::SaslPlaintext;
        task.config.sasl = task
            .config
            .sasl
            .clone()
            .client_authenticator_factory(SessionPayloadFactory);
        let mut client: BrokerStream = Box::new(client);
        let server_task = tokio::spawn(async move {
            let _handshake_request = read_frame(&mut server, None, &BufferPools::new(1))
                .await
                .expect("read handshake request");
            server
                .write_all(&sasl_handshake_response(1, "PLAIN"))
                .await
                .expect("write handshake response");
            let authenticate_request = read_frame(&mut server, None, &BufferPools::new(1))
                .await
                .expect("read authenticate request");
            let mut body = authenticate_request;
            let _header = RequestHeaderData::read(
                &mut body,
                request_header_version(ApiKey::SaslAuthenticate as i16, 2),
            )
            .expect("decode authenticate request header");
            let request = SaslAuthenticateRequestData::read(&mut body, 2)
                .expect("read authenticate request body");
            server
                .write_all(&sasl_authenticate_response(2))
                .await
                .expect("write authenticate response");
            request.auth_bytes
        });

        task.sasl_authenticate(&mut client)
            .await
            .expect("sasl authenticate");
        let auth_bytes = server_task.await.expect("server task");

        assert_eq!(auth_bytes, Bytes::from_static(b"7:broker.example.com:9092"));
    }

    #[tokio::test]
    async fn serve_connection_closes_when_command_channel_is_closed() {
        let (mut task, client, _server) = broker_task_with_connected_stream().await;
        let client: BrokerStream = Box::new(client);
        let mut pending = VecDeque::new();
        let mut rx_open = true;

        assert!(matches!(
            task.serve_connection(
                client,
                api_versions_capabilities(),
                &mut pending,
                &mut rx_open
            )
            .await,
            ServeOutcome::Closed
        ));
        assert!(!rx_open);
    }

    #[tokio::test]
    async fn write_command_returns_backpressure_when_pipeline_has_no_capacity() {
        let (task, client, _server) = broker_task_with_connected_stream().await;
        let client: BrokerStream = Box::new(client);
        let (_reader, mut writer) = tokio::io::split(client);
        let mut pipeline = RequestPipeline::new(1, Duration::from_secs(1));
        let (reserved_tx, _reserved_rx) = oneshot::channel();
        let _reserved = pipeline
            .reserve(ApiKey::ApiVersions, 3, reserved_tx)
            .expect("reserve only slot");
        let (tx, rx) = oneshot::channel();
        let command = request_command_with_sender(tx, Instant::now());

        let wrote = task
            .write_command(
                &mut writer,
                &mut pipeline,
                command,
                &api_versions_capabilities(),
            )
            .await
            .expect("write command");

        assert!(!wrote);
        assert!(matches!(
            rx.await.expect("backpressure response"),
            Err(WireError::Backpressure)
        ));
    }

    #[tokio::test]
    async fn read_frame_rejects_negative_and_oversized_lengths() {
        let buffers = BufferPools::new(1);
        let negative_bytes = (-1_i32).to_be_bytes();
        let oversized_bytes = (frame::MAX_FRAME_LENGTH.saturating_add(1)).to_be_bytes();
        let mut negative = &negative_bytes[..];
        let mut oversized = &oversized_bytes[..];

        assert!(matches!(
            read_frame(&mut negative, None, &buffers).await,
            Err(WireError::ConnectionClosed)
        ));
        assert!(matches!(
            read_frame(&mut oversized, None, &buffers).await,
            Err(WireError::ConnectionClosed)
        ));
    }

    #[tokio::test]
    async fn read_frame_reads_payload_and_releases_reusable_buffer() {
        let buffers = BufferPools::new(1);
        let mut framed = Vec::new();
        framed.extend_from_slice(&3_i32.to_be_bytes());
        framed.extend_from_slice(b"abc");
        let mut reader = &framed[..];

        let payload = read_frame(&mut reader, Some(16), &buffers)
            .await
            .expect("payload frame");

        assert_eq!(payload, Bytes::from_static(b"abc"));
        assert_eq!(buffers.stats().read_reused, 0);
        assert_eq!(buffers.stats().read_released, 1);
    }

    #[tokio::test]
    async fn read_response_frames_stops_when_receiver_is_closed() {
        let (_task, mut client, server) = broker_task_with_connected_stream().await;
        let (reader, _writer) = server.into_split();
        let (tx, rx) = mpsc::channel(1);
        let mut framed = Vec::new();
        framed.extend_from_slice(&3_i32.to_be_bytes());
        framed.extend_from_slice(b"abc");
        drop(rx);

        let reader_task = tokio::spawn(read_response_frames(
            reader,
            tx,
            Some(16),
            Arc::new(BufferPools::new(1)),
        ));

        client.write_all(&framed).await.expect("write frame");
        reader_task.await.expect("reader task");
    }
}
