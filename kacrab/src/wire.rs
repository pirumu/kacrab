//! Runtime Kafka wire/session support.

mod auth;
mod broker;
mod buffer;
mod capabilities;
mod client;
mod config;
mod error;
#[cfg(feature = "gssapi")]
mod gssapi;
#[cfg(feature = "gssapi")]
mod kerberos;
mod message;
mod metadata;
mod pipeline;
mod sasl;
mod socket;
mod tls;

#[cfg(feature = "producer")]
pub(crate) use self::broker::PendingBrokerResponse;
#[cfg(feature = "gssapi")]
pub use self::gssapi::GssapiAuthenticator;
pub use self::{
    auth::{SaslConfig, SaslMechanism, SecurityConfig, SecurityProtocol},
    broker::BrokerEndpoint,
    buffer::BufferPoolStats,
    capabilities::BrokerCapabilities,
    client::WireClient,
    config::{
        ConnectionConfig, DEFAULT_BROKER_QUEUE_CAPACITY, DEFAULT_BUFFER_POOL_CAPACITY,
        DEFAULT_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, DEFAULT_METADATA_MAX_AGE,
        DEFAULT_RECONNECT_BACKOFF_INITIAL, DEFAULT_RECONNECT_BACKOFF_MAX, DEFAULT_REQUEST_TIMEOUT,
        DEFAULT_REUSE_ADDRESS, DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT,
        DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MAX, DEFAULT_TCP_NODELAY, SocketConfig,
        TcpCongestionControl, TcpKeepaliveConfig, TransportConfig,
    },
    error::{Result, WireError},
    message::{RequestMessage, ResponseMessage},
    metadata::{BrokerMetadata, ClusterMetadata, PartitionMetadata, TopicMetadata},
    sasl::{
        SaslClientAction, SaslClientAuthenticator, SaslClientAuthenticatorFactory,
        SaslClientAuthenticatorFactoryHandle, SaslClientAuthenticatorHandle, SaslClientSession,
    },
    tls::TlsConfig,
};
