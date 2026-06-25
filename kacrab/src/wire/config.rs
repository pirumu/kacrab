//! TCP socket configuration view used by wire sessions.

use core::time::Duration;

use super::{
    SaslClientAuthenticator, SaslClientAuthenticatorFactory, SaslConfig, SecurityConfig, TlsConfig,
};

/// Kafka default `request.timeout.ms`: long enough for normal broker latency
/// but bounded so every request path has a predictable failure time.
pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
/// Kafka default `socket.connection.setup.timeout.ms` for the first TCP connect
/// attempt before reconnect/backoff orchestration takes over.
pub const DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT: Duration = Duration::from_secs(10);
/// Kafka default `socket.connection.setup.timeout.max.ms`; caps setup backoff
/// at the same 30 second ceiling used by Java clients.
pub const DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MAX: Duration = Duration::from_secs(30);
/// Kafka default `metadata.max.age.ms`: force a refresh every five minutes to
/// discover broker/partition changes even without leadership errors.
pub const DEFAULT_METADATA_MAX_AGE: Duration = Duration::from_mins(5);
/// Kafka default `metadata.max.idle.ms`: forget idle topic metadata after five minutes.
pub const DEFAULT_METADATA_MAX_IDLE: Duration = Duration::from_mins(5);
/// Kafka default `metadata.recovery.rebootstrap.trigger.ms`.
pub const DEFAULT_METADATA_REBOOTSTRAP_TRIGGER: Duration = Duration::from_mins(5);
/// Kafka default `retry.backoff.ms`, reused by metadata refresh scheduling.
pub const DEFAULT_METADATA_REFRESH_BACKOFF_INITIAL: Duration = Duration::from_millis(100);
/// Kafka default `retry.backoff.max.ms`, capping metadata refresh retry backoff.
pub const DEFAULT_METADATA_REFRESH_BACKOFF_MAX: Duration = Duration::from_secs(1);
/// Kafka default `connections.max.idle.ms`: close idle broker sockets after
/// nine minutes while keeping the broker task alive for future reconnects.
pub(super) const DEFAULT_CONNECTIONS_MAX_IDLE: Duration = Duration::from_mins(9);
/// Kafka producer default `max.in.flight.requests.per.connection`; it preserves
/// idempotent ordering guarantees while allowing useful pipelining.
pub const DEFAULT_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION: usize = 5;
/// Internal broker command queue capacity. This is intentionally larger than
/// the in-flight window so callers can absorb short scheduler stalls while
/// still getting bounded backpressure.
pub const DEFAULT_BROKER_QUEUE_CAPACITY: usize = 1024;
/// Initial reconnect delay. Kafka's default is 50ms; kacrab uses 100ms to avoid
/// tight reconnect loops while the async task model is still coarse-grained.
pub const DEFAULT_RECONNECT_BACKOFF_INITIAL: Duration = Duration::from_millis(100);
/// Maximum reconnect delay for the current deterministic backoff path; kept
/// below request timeout so pending commands are cleaned up promptly.
pub const DEFAULT_RECONNECT_BACKOFF_MAX: Duration = Duration::from_secs(5);
/// Zero disables buffer pooling by default, leaving allocation behavior explicit
/// until users opt into the runtime memory tradeoff.
pub const DEFAULT_BUFFER_POOL_CAPACITY: usize = 0;
/// `TCP_NODELAY` stays on by default for producer latency and Kafka request/response
/// round trips; batching is handled above TCP by the producer accumulator.
pub const DEFAULT_TCP_NODELAY: bool = true;
/// `SO_REUSEADDR` is enabled for client sockets so reconnects do not get delayed
/// by local address reuse behavior across platforms.
pub const DEFAULT_REUSE_ADDRESS: bool = true;

/// Metadata recovery policy when known brokers no longer provide usable metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataRecoveryStrategy {
    /// Reset known endpoints to the configured bootstrap set after the trigger window.
    Rebootstrap,
    /// Do not reset to bootstrap endpoints; return the metadata failure to callers.
    None,
}

/// Wire connection configuration for one broker session.
#[derive(Debug, Clone, PartialEq)]
pub struct ConnectionConfig {
    /// TCP socket options used before any transport security is applied.
    pub socket: SocketConfig,
    /// Kafka security protocol and authentication mode.
    pub security: SecurityConfig,
    /// TLS configuration for `SSL` and `SASL_SSL`.
    pub tls: TlsConfig,
    /// SASL configuration for `SASL_PLAINTEXT` and `SASL_SSL`.
    pub sasl: SaslConfig,
    /// Transport security mode layered over the TCP socket.
    pub transport: TransportConfig,
    /// Broker request/response timeout.
    pub request_timeout: Duration,
    /// Timeout for one TCP connect attempt.
    pub socket_connection_setup_timeout: Duration,
    /// Maximum socket setup timeout for retry/backoff orchestration.
    pub socket_connection_setup_timeout_max: Duration,
    /// Optional initial read buffer capacity for broker frame reads.
    pub read_buffer_capacity: Option<usize>,
    /// Maximum age for cached cluster metadata before refresh.
    pub metadata_max_age: Duration,
    /// Maximum idle time before cached topic metadata is forgotten.
    pub metadata_max_idle: Duration,
    /// Recovery strategy used when no known broker can provide usable metadata.
    pub metadata_recovery_strategy: MetadataRecoveryStrategy,
    /// Time without usable metadata before bootstrap endpoints may be retried.
    pub metadata_rebootstrap_trigger: Duration,
    /// Initial delay before retrying failed metadata refreshes.
    pub metadata_refresh_backoff_initial: Duration,
    /// Maximum delay before retrying failed metadata refreshes.
    pub metadata_refresh_backoff_max: Duration,
    /// Maximum idle time before a broker socket is closed.
    pub connections_max_idle: Duration,
    /// Maximum in-flight requests allowed on one broker connection.
    pub max_in_flight_requests_per_connection: usize,
    /// Bounded pending command queue capacity for one broker task.
    pub broker_queue_capacity: usize,
    /// Initial reconnect delay after a failed connect or disconnected broker.
    pub reconnect_backoff_initial: Duration,
    /// Maximum reconnect delay after repeated failures.
    pub reconnect_backoff_max: Duration,
    /// Number of reusable read/write buffers retained per wire client.
    pub buffer_pool_capacity: usize,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            socket: SocketConfig::default(),
            security: SecurityConfig::default(),
            tls: TlsConfig::default(),
            sasl: SaslConfig::default(),
            transport: TransportConfig::Plaintext,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            socket_connection_setup_timeout: DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT,
            socket_connection_setup_timeout_max: DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MAX,
            read_buffer_capacity: None,
            metadata_max_age: DEFAULT_METADATA_MAX_AGE,
            metadata_max_idle: DEFAULT_METADATA_MAX_IDLE,
            metadata_recovery_strategy: MetadataRecoveryStrategy::Rebootstrap,
            metadata_rebootstrap_trigger: DEFAULT_METADATA_REBOOTSTRAP_TRIGGER,
            metadata_refresh_backoff_initial: DEFAULT_METADATA_REFRESH_BACKOFF_INITIAL,
            metadata_refresh_backoff_max: DEFAULT_METADATA_REFRESH_BACKOFF_MAX,
            connections_max_idle: DEFAULT_CONNECTIONS_MAX_IDLE,
            max_in_flight_requests_per_connection: DEFAULT_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
            broker_queue_capacity: DEFAULT_BROKER_QUEUE_CAPACITY,
            reconnect_backoff_initial: DEFAULT_RECONNECT_BACKOFF_INITIAL,
            reconnect_backoff_max: DEFAULT_RECONNECT_BACKOFF_MAX,
            buffer_pool_capacity: DEFAULT_BUFFER_POOL_CAPACITY,
        }
    }
}

impl From<SocketConfig> for ConnectionConfig {
    fn from(socket: SocketConfig) -> Self {
        Self {
            socket,
            ..Self::default()
        }
    }
}

impl ConnectionConfig {
    /// Set the TCP socket options for this connection.
    #[must_use]
    pub const fn socket(mut self, socket: SocketConfig) -> Self {
        self.socket = socket;
        self
    }

    /// Set the SASL configuration for this connection.
    #[must_use]
    pub fn sasl(mut self, sasl: SaslConfig) -> Self {
        self.sasl = sasl;
        self
    }

    /// Set a native Rust SASL client authenticator.
    #[must_use]
    pub fn sasl_client_authenticator(
        mut self,
        authenticator: impl SaslClientAuthenticator,
    ) -> Self {
        self.sasl = self.sasl.client_authenticator(authenticator);
        self
    }

    /// Set a native Rust SASL client authenticator factory.
    #[must_use]
    pub fn sasl_client_authenticator_factory(
        mut self,
        factory: impl SaslClientAuthenticatorFactory,
    ) -> Self {
        self.sasl = self.sasl.client_authenticator_factory(factory);
        self
    }

    /// Set the broker request/response timeout.
    #[must_use]
    pub const fn request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    /// Set the timeout for one TCP connect attempt.
    #[must_use]
    pub const fn socket_connection_setup_timeout(mut self, timeout: Duration) -> Self {
        self.socket_connection_setup_timeout = timeout;
        self
    }

    /// Set the maximum socket setup timeout for retry/backoff orchestration.
    #[must_use]
    pub const fn socket_connection_setup_timeout_max(mut self, timeout: Duration) -> Self {
        self.socket_connection_setup_timeout_max = timeout;
        self
    }

    /// Set the initial read buffer capacity for broker frame reads.
    #[must_use]
    pub const fn read_buffer_capacity(mut self, bytes: usize) -> Self {
        self.read_buffer_capacity = Some(bytes);
        self
    }

    /// Set the maximum age for cached cluster metadata.
    #[must_use]
    pub const fn metadata_max_age(mut self, timeout: Duration) -> Self {
        self.metadata_max_age = timeout;
        self
    }

    /// Set the maximum idle time before cached topic metadata is forgotten.
    #[must_use]
    pub const fn metadata_max_idle(mut self, timeout: Duration) -> Self {
        self.metadata_max_idle = timeout;
        self
    }

    /// Set the metadata recovery strategy.
    #[must_use]
    pub const fn metadata_recovery_strategy(mut self, strategy: MetadataRecoveryStrategy) -> Self {
        self.metadata_recovery_strategy = strategy;
        self
    }

    /// Set the time without usable metadata before rebootstrap may trigger.
    #[must_use]
    pub const fn metadata_rebootstrap_trigger(mut self, timeout: Duration) -> Self {
        self.metadata_rebootstrap_trigger = timeout;
        self
    }

    /// Set the initial metadata refresh retry delay.
    #[must_use]
    pub const fn metadata_refresh_backoff_initial(mut self, timeout: Duration) -> Self {
        self.metadata_refresh_backoff_initial = timeout;
        self
    }

    /// Set the maximum metadata refresh retry delay.
    #[must_use]
    pub const fn metadata_refresh_backoff_max(mut self, timeout: Duration) -> Self {
        self.metadata_refresh_backoff_max = timeout;
        self
    }

    /// Set the maximum idle time before a broker socket is closed.
    #[must_use]
    pub const fn connections_max_idle(mut self, timeout: Duration) -> Self {
        self.connections_max_idle = timeout;
        self
    }

    /// Set `max.in.flight.requests.per.connection` for broker request pipelining.
    #[must_use]
    pub const fn max_in_flight_requests_per_connection(mut self, requests: usize) -> Self {
        self.max_in_flight_requests_per_connection = if requests == 0 { 1 } else { requests };
        self
    }

    /// Set the bounded pending command queue capacity for each broker task.
    #[must_use]
    pub const fn broker_queue_capacity(mut self, requests: usize) -> Self {
        self.broker_queue_capacity = if requests == 0 { 1 } else { requests };
        self
    }

    /// Set the initial reconnect delay after a failed connect or disconnected broker.
    #[must_use]
    pub const fn reconnect_backoff_initial(mut self, timeout: Duration) -> Self {
        self.reconnect_backoff_initial = timeout;
        self
    }

    /// Set the maximum reconnect delay after repeated failures.
    #[must_use]
    pub const fn reconnect_backoff_max(mut self, timeout: Duration) -> Self {
        self.reconnect_backoff_max = timeout;
        self
    }

    /// Set the number of reusable read/write buffers retained per wire client.
    #[must_use]
    pub const fn buffer_pool_capacity(mut self, buffers: usize) -> Self {
        self.buffer_pool_capacity = buffers;
        self
    }
}

/// Transport security mode for a broker connection.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransportConfig {
    /// Plain TCP without TLS.
    Plaintext,
}

/// TCP socket configuration for broker connections.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SocketConfig {
    /// Kafka key: `send.buffer.bytes`.
    ///
    /// Positive values set `SO_SNDBUF`; `None` keeps the operating-system default.
    pub send_buffer_bytes: Option<usize>,
    /// Kafka key: `receive.buffer.bytes`.
    ///
    /// Positive values set `SO_RCVBUF`; `None` keeps the operating-system default.
    pub receive_buffer_bytes: Option<usize>,
    /// Kacrab socket key: `socket.tcp.nodelay`.
    ///
    /// Sets `TCP_NODELAY`.
    pub tcp_nodelay: bool,
    /// Kacrab socket key: `socket.tcp.keepalive`.
    ///
    /// Sets `SO_KEEPALIVE` and TCP keepalive timing. The exact keepalive timing
    /// support is platform dependent and follows `socket2::TcpKeepalive`.
    pub tcp_keepalive: Option<TcpKeepaliveConfig>,
    /// Kacrab socket key: `socket.tcp.notsent.lowat.bytes`.
    ///
    /// Linux/Android only. Sets `TCP_NOTSENT_LOWAT`; ignored at compile time on
    /// platforms where `socket2` does not expose this socket option.
    pub tcp_notsent_lowat_bytes: Option<u32>,
    /// Kacrab socket key: `socket.tcp.quickack`.
    ///
    /// Linux/Android/Fuchsia/Cygwin only. Sets `TCP_QUICKACK`; ignored at
    /// compile time on unsupported platforms.
    pub tcp_quickack: Option<bool>,
    /// Kacrab socket key: `socket.tcp.user.timeout.ms`.
    ///
    /// Linux/Android/Fuchsia/Cygwin only. Sets `TCP_USER_TIMEOUT`; ignored at
    /// compile time on unsupported platforms.
    pub tcp_user_timeout_ms: Option<Duration>,
    /// Kacrab socket key: `socket.tcp.congestion`.
    ///
    /// Linux/FreeBSD only. Sets `TCP_CONGESTION`; ignored at compile time on
    /// unsupported platforms.
    pub tcp_congestion: Option<TcpCongestionControl>,
    /// Kacrab socket key: `socket.reuse.address`.
    ///
    /// Sets `SO_REUSEADDR`.
    pub reuse_address: bool,
}

impl SocketConfig {
    /// Kafka-compatible key for `SO_SNDBUF`.
    pub const SEND_BUFFER_BYTES_CONFIG: &'static str = "send.buffer.bytes";
    /// Kafka-compatible key for `SO_RCVBUF`.
    pub const RECEIVE_BUFFER_BYTES_CONFIG: &'static str = "receive.buffer.bytes";

    /// Set `TCP_NODELAY`.
    #[must_use]
    pub const fn tcp_nodelay(mut self, enabled: bool) -> Self {
        self.tcp_nodelay = enabled;
        self
    }

    /// Set `SO_KEEPALIVE` and TCP keepalive timing.
    #[must_use]
    pub const fn tcp_keepalive(mut self, keepalive: Option<TcpKeepaliveConfig>) -> Self {
        self.tcp_keepalive = keepalive;
        self
    }

    /// Set `SO_SNDBUF`; `None` keeps the OS default.
    #[must_use]
    pub const fn send_buffer_bytes(mut self, bytes: usize) -> Self {
        self.send_buffer_bytes = Some(bytes);
        self
    }

    /// Set `SO_RCVBUF`; `None` keeps the OS default.
    #[must_use]
    pub const fn receive_buffer_bytes(mut self, bytes: usize) -> Self {
        self.receive_buffer_bytes = Some(bytes);
        self
    }

    /// Set `TCP_NOTSENT_LOWAT` where the target OS supports it.
    #[must_use]
    pub const fn tcp_notsent_lowat_bytes(mut self, bytes: u32) -> Self {
        self.tcp_notsent_lowat_bytes = Some(bytes);
        self
    }

    /// Set `TCP_QUICKACK` where the target OS supports it.
    #[must_use]
    pub const fn tcp_quickack(mut self, enabled: bool) -> Self {
        self.tcp_quickack = Some(enabled);
        self
    }

    /// Set `TCP_USER_TIMEOUT` where the target OS supports it.
    #[must_use]
    pub const fn tcp_user_timeout_ms(mut self, timeout: Option<Duration>) -> Self {
        self.tcp_user_timeout_ms = timeout;
        self
    }

    /// Set the TCP congestion control algorithm where the target OS supports it.
    #[must_use]
    pub const fn tcp_congestion(mut self, congestion: TcpCongestionControl) -> Self {
        self.tcp_congestion = Some(congestion);
        self
    }

    /// Set `SO_REUSEADDR`.
    #[must_use]
    pub const fn reuse_address(mut self, enabled: bool) -> Self {
        self.reuse_address = enabled;
        self
    }
}

impl Default for SocketConfig {
    fn default() -> Self {
        Self {
            send_buffer_bytes: None,
            receive_buffer_bytes: None,
            tcp_nodelay: DEFAULT_TCP_NODELAY,
            tcp_keepalive: None,
            tcp_notsent_lowat_bytes: None,
            tcp_quickack: None,
            tcp_user_timeout_ms: None,
            tcp_congestion: None,
            reuse_address: DEFAULT_REUSE_ADDRESS,
        }
    }
}

#[cfg(test)]
mod default_tests {
    #![allow(
        clippy::expect_used,
        clippy::missing_assert_message,
        clippy::unwrap_used,
        reason = "Unit test fixtures fail fastest with contextual unwrap/expect calls."
    )]

    use std::time::Duration;

    use super::{
        ConnectionConfig, DEFAULT_BROKER_QUEUE_CAPACITY, DEFAULT_BUFFER_POOL_CAPACITY,
        DEFAULT_CONNECTIONS_MAX_IDLE, DEFAULT_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
        DEFAULT_METADATA_MAX_AGE, DEFAULT_RECONNECT_BACKOFF_INITIAL, DEFAULT_RECONNECT_BACKOFF_MAX,
        DEFAULT_REQUEST_TIMEOUT, DEFAULT_REUSE_ADDRESS, DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT,
        DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MAX, DEFAULT_TCP_NODELAY, SocketConfig,
        TcpKeepaliveConfig,
    };

    #[test]
    fn connection_defaults_are_named_kafka_runtime_values() {
        let config = ConnectionConfig::default();

        assert_eq!(config.request_timeout, DEFAULT_REQUEST_TIMEOUT);
        assert_eq!(
            config.socket_connection_setup_timeout,
            DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT
        );
        assert_eq!(
            config.socket_connection_setup_timeout_max,
            DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MAX
        );
        assert_eq!(config.metadata_max_age, DEFAULT_METADATA_MAX_AGE);
        assert_eq!(config.connections_max_idle, DEFAULT_CONNECTIONS_MAX_IDLE);
        assert_eq!(
            config.max_in_flight_requests_per_connection,
            DEFAULT_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION
        );
        assert_eq!(config.broker_queue_capacity, DEFAULT_BROKER_QUEUE_CAPACITY);
        assert_eq!(
            config.reconnect_backoff_initial,
            DEFAULT_RECONNECT_BACKOFF_INITIAL
        );
        assert_eq!(config.reconnect_backoff_max, DEFAULT_RECONNECT_BACKOFF_MAX);
        assert_eq!(config.buffer_pool_capacity, DEFAULT_BUFFER_POOL_CAPACITY);
    }

    #[test]
    fn connection_builder_methods_set_every_runtime_field() {
        let socket = SocketConfig::default().tcp_nodelay(false);
        let config = ConnectionConfig::default()
            .socket(socket.clone())
            .request_timeout(Duration::from_millis(1))
            .socket_connection_setup_timeout(Duration::from_millis(2))
            .socket_connection_setup_timeout_max(Duration::from_millis(3))
            .read_buffer_capacity(4)
            .metadata_max_age(Duration::from_millis(5))
            .connections_max_idle(Duration::from_millis(9))
            .max_in_flight_requests_per_connection(0)
            .broker_queue_capacity(0)
            .reconnect_backoff_initial(Duration::from_millis(6))
            .reconnect_backoff_max(Duration::from_millis(7))
            .buffer_pool_capacity(8);

        assert_eq!(ConnectionConfig::from(socket.clone()).socket, socket);
        assert_eq!(config.request_timeout, Duration::from_millis(1));
        assert_eq!(
            config.socket_connection_setup_timeout,
            Duration::from_millis(2)
        );
        assert_eq!(
            config.socket_connection_setup_timeout_max,
            Duration::from_millis(3)
        );
        assert_eq!(config.read_buffer_capacity, Some(4));
        assert_eq!(config.metadata_max_age, Duration::from_millis(5));
        assert_eq!(config.connections_max_idle, Duration::from_millis(9));
        assert_eq!(config.max_in_flight_requests_per_connection, 1);
        assert_eq!(config.broker_queue_capacity, 1);
        assert_eq!(config.reconnect_backoff_initial, Duration::from_millis(6));
        assert_eq!(config.reconnect_backoff_max, Duration::from_millis(7));
        assert_eq!(config.buffer_pool_capacity, 8);
    }

    #[test]
    fn socket_defaults_and_builders_cover_tcp_options() {
        let keepalive = TcpKeepaliveConfig {
            idle: Duration::from_secs(1),
            interval: Duration::from_secs(2),
        };
        let socket = SocketConfig::default()
            .tcp_nodelay(false)
            .tcp_keepalive(Some(keepalive))
            .send_buffer_bytes(128)
            .receive_buffer_bytes(256)
            .tcp_notsent_lowat_bytes(512)
            .tcp_quickack(true)
            .tcp_user_timeout_ms(Some(Duration::from_secs(3)))
            .reuse_address(false);

        assert_eq!(SocketConfig::default().tcp_nodelay, DEFAULT_TCP_NODELAY);
        assert_eq!(SocketConfig::default().reuse_address, DEFAULT_REUSE_ADDRESS);
        assert_eq!(socket.tcp_keepalive, Some(keepalive));
        assert_eq!(socket.send_buffer_bytes, Some(128));
        assert_eq!(socket.receive_buffer_bytes, Some(256));
        assert_eq!(socket.tcp_notsent_lowat_bytes, Some(512));
        assert_eq!(socket.tcp_quickack, Some(true));
        assert_eq!(socket.tcp_user_timeout_ms, Some(Duration::from_secs(3)));
        assert!(!socket.reuse_address);
    }
}

/// TCP keepalive timing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpKeepaliveConfig {
    /// Idle duration before keepalive probes begin.
    pub idle: Duration,
    /// Interval between keepalive probes.
    pub interval: Duration,
}

/// TCP congestion control algorithm for platforms that expose per-socket control.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TcpCongestionControl {
    /// Linux BBR congestion control.
    Bbr,
    /// CUBIC congestion control.
    Cubic,
    /// Reno congestion control.
    Reno,
}

impl TcpCongestionControl {
    #[cfg(any(target_os = "freebsd", target_os = "linux"))]
    pub(crate) const fn as_bytes(self) -> &'static [u8] {
        match self {
            Self::Bbr => b"bbr",
            Self::Cubic => b"cubic",
            Self::Reno => b"reno",
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::expect_used,
        clippy::missing_assert_message,
        clippy::unwrap_used,
        reason = "Unit test fixtures fail fastest with contextual unwrap/expect calls."
    )]

    use core::time::Duration;

    use super::{ConnectionConfig, SocketConfig, TcpCongestionControl, TransportConfig};

    #[test]
    fn socket_config_uses_kafka_style_keys() {
        assert_eq!(SocketConfig::SEND_BUFFER_BYTES_CONFIG, "send.buffer.bytes");
        assert_eq!(
            SocketConfig::RECEIVE_BUFFER_BYTES_CONFIG,
            "receive.buffer.bytes"
        );
    }

    #[test]
    fn connection_config_layers_socket_and_transport() {
        let socket = SocketConfig::default().send_buffer_bytes(65_536);
        let config = ConnectionConfig::default()
            .socket(socket)
            .request_timeout(Duration::from_secs(7))
            .socket_connection_setup_timeout(Duration::from_secs(3))
            .socket_connection_setup_timeout_max(Duration::from_secs(11))
            .connections_max_idle(Duration::from_secs(13))
            .read_buffer_capacity(256 * 1024);

        assert_eq!(config.socket.send_buffer_bytes, Some(65_536));
        assert_eq!(config.transport, TransportConfig::Plaintext);
        assert_eq!(config.request_timeout, Duration::from_secs(7));
        assert_eq!(
            config.socket_connection_setup_timeout,
            Duration::from_secs(3)
        );
        assert_eq!(
            config.socket_connection_setup_timeout_max,
            Duration::from_secs(11)
        );
        assert_eq!(config.connections_max_idle, Duration::from_secs(13));
        assert_eq!(config.read_buffer_capacity, Some(256 * 1024));
        assert_eq!(config.metadata_max_age, Duration::from_mins(5));
        assert_eq!(config.max_in_flight_requests_per_connection, 5);
        assert_eq!(config.broker_queue_capacity, 1024);
    }

    #[test]
    fn socket_config_exposes_deep_tcp_knobs() {
        let config = SocketConfig::default()
            .tcp_notsent_lowat_bytes(65_536)
            .tcp_quickack(true)
            .tcp_user_timeout_ms(Some(Duration::from_secs(15)))
            .tcp_congestion(TcpCongestionControl::Bbr);

        assert_eq!(config.tcp_notsent_lowat_bytes, Some(65_536));
        assert_eq!(config.tcp_quickack, Some(true));
        assert_eq!(config.tcp_user_timeout_ms, Some(Duration::from_secs(15)));
        assert_eq!(config.tcp_congestion, Some(TcpCongestionControl::Bbr));
    }
}
