//! Error types for the wire/session layer.

use kacrab_protocol::{
    ProtocolError, frame,
    generated::{ApiKey, ErrorCode},
};
use thiserror::Error;

/// First Apache Kafka release that answers [`ApiKey::ApiVersions`] v3, the
/// version kacrab pins for its capability handshake (KIP-511).
pub(crate) const MIN_SUPPORTED_KAFKA_RELEASE: &str = "2.4";

/// Errors from the runtime wire/session layer.
#[derive(Debug, Error)]
pub enum WireError {
    /// TCP or socket IO failed.
    #[error("wire IO failed: {0}")]
    Io(#[from] std::io::Error),
    /// Kafka protocol encoding or decoding failed.
    #[error("protocol failed: {0}")]
    Protocol(#[from] ProtocolError),
    /// Kafka frame encoding or decoding failed.
    #[error("frame failed: {0}")]
    Frame(#[from] frame::FrameError),
    /// Request timed out while waiting for a matching response.
    #[error("request timed out")]
    Timeout,
    /// Background reader task stopped before delivering a response.
    #[error("broker connection closed")]
    ConnectionClosed,
    /// Request could not be accepted because a bounded queue or in-flight set is full.
    #[error("wire backpressure")]
    Backpressure,
    /// `security.protocol` was not one of Kafka's supported protocol names.
    #[error("invalid security.protocol `{0}`")]
    InvalidSecurityProtocol(String),
    /// TLS configuration could not be used.
    #[error("invalid TLS config: {0}")]
    InvalidTlsConfig(String),
    /// TLS handshake failed.
    #[error("TLS handshake failed: {0}")]
    TlsHandshake(String),
    /// A Java TLS option is not supported by the active Rust TLS backend.
    #[error("unsupported TLS option `{0}`")]
    UnsupportedTlsOption(String),
    /// SASL configuration could not be used.
    #[error("invalid SASL config: {0}")]
    InvalidSaslConfig(String),
    /// The configured SASL mechanism is unknown or unsupported.
    #[error("unsupported SASL mechanism `{0}`")]
    UnsupportedSaslMechanism(String),
    /// Broker rejected SASL mechanism negotiation.
    #[error("SASL handshake failed: {0}")]
    SaslHandshake(String),
    /// Broker rejected SASL authentication.
    #[error("SASL authentication failed: {0}")]
    SaslAuthentication(String),
    /// SCRAM server signature verification failed.
    #[error("SASL server signature mismatch")]
    SaslServerSignatureMismatch,
    /// OAUTHBEARER token acquisition or refresh failed.
    #[error("SASL token refresh failed: {0}")]
    TokenRefresh(String),
    /// GSSAPI was configured without an available Kerberos backend.
    #[error("GSSAPI backend unavailable")]
    GssapiBackendUnavailable,
    /// No broker endpoint is registered for the requested node id.
    #[error("unknown broker id {0}")]
    UnknownBroker(i32),
    /// No known broker can be used for a metadata refresh.
    #[error("no broker endpoint is available")]
    NoBrokerAvailable,
    /// Broker metadata did not contain a usable socket endpoint.
    #[error("invalid endpoint for broker {node_id}: {host}:{port}")]
    InvalidBrokerEndpoint {
        /// Broker node id.
        node_id: i32,
        /// Host from broker metadata.
        host: String,
        /// Port from broker metadata.
        port: i32,
    },
    /// Broker returned an error code.
    #[error("kafka API returned {0}")]
    Kafka(ErrorCode),
    /// Metadata response carried a topic-level error.
    #[error("metadata for topic {topic} failed: {error}")]
    MetadataTopic {
        /// Topic name.
        topic: String,
        /// Kafka error code.
        error: ErrorCode,
    },
    /// Metadata response carried a partition-level error.
    #[error("metadata for {topic}-{partition} failed: {error}")]
    MetadataPartition {
        /// Topic name.
        topic: String,
        /// Partition index.
        partition: i32,
        /// Kafka error code.
        error: ErrorCode,
    },
    /// Secure random bytes could not be generated for jitter/backoff decisions.
    ///
    /// Carries the typed `getrandom::Error` rather than a stringified copy.
    /// (`getrandom::Error` does not implement `std::error::Error` in this
    /// version, so it is rendered via `Display` rather than a `#[source]` chain.)
    #[error("random byte generation failed: {0}")]
    RandomBytes(getrandom::Error),
    /// Broker does not support a mutually compatible API version.
    #[error("no compatible API version for {0:?}")]
    UnsupportedApiVersion(ApiKey),
    /// Broker rejected the pinned `ApiVersions` handshake version, so it
    /// predates Apache Kafka 2.4 and cannot negotiate capabilities with kacrab.
    ///
    /// kacrab deliberately does not retry the handshake at v0 the way Java's
    /// client does; the supported floor is Kafka 2.4. Reconnecting cannot fix
    /// an old broker, so this is classified as a fatal setup error and reported
    /// immediately instead of looping under reconnect backoff until
    /// `request.timeout.ms`.
    #[error(
        "broker does not support ApiVersions v{version}; kacrab requires Apache Kafka \
         {MIN_SUPPORTED_KAFKA_RELEASE} or newer"
    )]
    IncompatibleBroker {
        /// `ApiVersions` version kacrab sent in the handshake.
        version: i16,
    },
    /// Response correlation id did not match the in-flight request.
    #[error("correlation id mismatch: expected {expected}, got {actual}")]
    CorrelationIdMismatch {
        /// Correlation id assigned to the request.
        expected: i32,
        /// Correlation id decoded from the response.
        actual: i32,
    },
}

impl WireError {
    /// Whether this is a terminal connection-setup failure — TLS or SASL
    /// handshake/authentication, or a broker too old to negotiate with — that
    /// reconnecting cannot fix. Callers should fail fast with the real cause
    /// instead of looping under reconnect backoff until they time out, matching
    /// Java's non-retriable `SaslAuthenticationException` /
    /// `SslAuthenticationException` / `IllegalSaslStateException` /
    /// `UnsupportedVersionException` semantics.
    #[must_use]
    pub(crate) const fn is_fatal_setup(&self) -> bool {
        matches!(
            self,
            Self::UnsupportedTlsOption(_)
                | Self::InvalidTlsConfig(_)
                | Self::TlsHandshake(_)
                | Self::GssapiBackendUnavailable
                | Self::InvalidSaslConfig(_)
                | Self::UnsupportedSaslMechanism(_)
                | Self::SaslHandshake(_)
                | Self::SaslAuthentication(_)
                | Self::SaslServerSignatureMismatch
                | Self::IncompatibleBroker { .. }
        )
    }
}

/// Result alias for wire operations.
pub type Result<T> = std::result::Result<T, WireError>;

#[cfg(test)]
mod tests {
    #![allow(
        clippy::missing_assert_message,
        reason = "Unit test fixtures read fastest without redundant assertion messages."
    )]

    use super::WireError;

    /// The handshake failure must name the Apache Kafka release floor, since
    /// "`ApiVersions` v3" alone tells a user nothing about which broker to run.
    #[test]
    fn incompatible_broker_names_the_required_kafka_release() {
        assert_eq!(
            WireError::IncompatibleBroker { version: 3 }.to_string(),
            "broker does not support ApiVersions v3; kacrab requires Apache Kafka 2.4 or newer"
        );
    }

    /// Fail fast on an incompatible broker; keep retrying everything the
    /// reconnect loop can still recover from.
    #[test]
    fn incompatible_broker_is_a_fatal_setup_error() {
        assert!(WireError::IncompatibleBroker { version: 3 }.is_fatal_setup());
        assert!(!WireError::ConnectionClosed.is_fatal_setup());
    }
}
