//! Error types for the wire/session layer.

use kacrab_protocol::{
    ProtocolError, frame,
    generated::{ApiKey, ErrorCode},
    version::ApiVersionRange,
};
use thiserror::Error;

/// First Apache Kafka release that answers [`ApiKey::ApiVersions`] v3, the
/// version kacrab pins for its capability handshake (KIP-511).
pub(crate) const MIN_SUPPORTED_KAFKA_RELEASE: &str = "2.4";

/// Errors from the runtime wire/session layer.
///
/// `#[non_exhaustive]`: new failure modes arrive with new Kafka protocol
/// surface, so downstream `match`es must carry a wildcard arm.
#[derive(Debug, Error)]
#[non_exhaustive]
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
    /// Broker does not support a mutually compatible version of an API.
    ///
    /// Carries enough detail to act on without reading kacrab's source: which
    /// API failed to negotiate, the version range kacrab can speak, and either
    /// the range the broker advertised or the fact that the broker did not
    /// advertise the API at all. The two cases mean different things — an
    /// absent key is usually a broker that predates the API, while disjoint
    /// ranges are a broker that dropped (or has not yet added) the versions
    /// kacrab speaks — so [`Self::UnsupportedApiVersion`] renders them
    /// differently rather than collapsing both into "unsupported".
    #[error(fmt = fmt_unsupported_api_version)]
    UnsupportedApiVersion {
        /// API whose version could not be negotiated.
        api_key: ApiKey,
        /// Versions kacrab can speak for this API, already narrowed by any
        /// caller-supplied maximum.
        client: ApiVersionRange,
        /// Versions the broker advertised for this API, or `None` when the
        /// broker's `ApiVersions` response did not list the API key.
        broker: Option<ApiVersionRange>,
    },
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

/// Render [`WireError::UnsupportedApiVersion`] so the reader learns which API
/// failed, what kacrab asked for, what the broker offers, and which side needs
/// to move. thiserror's `fmt =` hook is used instead of a format string because
/// the three outcomes (API absent, broker too old, broker too new) need
/// different wording.
#[expect(
    clippy::trivially_copy_pass_by_ref,
    clippy::ref_option,
    reason = "Signature is dictated by thiserror's `#[error(fmt = ...)]` hook, which passes every \
              field by reference."
)]
fn fmt_unsupported_api_version(
    api_key: &ApiKey,
    client: &ApiVersionRange,
    broker: &Option<ApiVersionRange>,
    formatter: &mut std::fmt::Formatter<'_>,
) -> std::fmt::Result {
    write!(
        formatter,
        "no compatible {api_key:?} API version (key {key}): kacrab supports \
         v{client_min}..=v{client_max}",
        key = *api_key as i16,
        client_min = client.min_version,
        client_max = client.max_version,
    )?;
    let Some(broker) = *broker else {
        return write!(
            formatter,
            ", broker does not advertise this API; needs a broker that supports {api_key:?} \
             v{client_min} or newer",
            client_min = client.min_version,
        );
    };
    write!(
        formatter,
        ", broker supports v{broker_min}..=v{broker_max}; ",
        broker_min = broker.min_version,
        broker_max = broker.max_version,
    )?;
    if broker.max_version < client.min_version {
        write!(
            formatter,
            "needs a broker that supports {api_key:?} v{client_min} or newer",
            client_min = client.min_version,
        )
    } else {
        write!(
            formatter,
            "kacrab speaks {api_key:?} v{client_max} at most, so kacrab needs upgrading",
            client_max = client.max_version,
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

    use kacrab_protocol::{generated::ApiKey, version::ApiVersionRange};

    use super::WireError;

    const fn range(min_version: i16, max_version: i16) -> ApiVersionRange {
        ApiVersionRange {
            min_version,
            max_version,
        }
    }

    /// An incompatibility must be actionable from the message alone: which API,
    /// what kacrab speaks, what the broker offers, and what has to change.
    #[test]
    fn unsupported_api_version_names_api_and_both_ranges() {
        let absent = WireError::UnsupportedApiVersion {
            api_key: ApiKey::Metadata,
            client: range(0, 13),
            broker: None,
        };
        assert_eq!(
            absent.to_string(),
            "no compatible Metadata API version (key 3): kacrab supports v0..=v13, broker does \
             not advertise this API; needs a broker that supports Metadata v0 or newer"
        );

        let broker_too_old = WireError::UnsupportedApiVersion {
            api_key: ApiKey::Produce,
            client: range(3, 13),
            broker: Some(range(0, 2)),
        };
        assert_eq!(
            broker_too_old.to_string(),
            "no compatible Produce API version (key 0): kacrab supports v3..=v13, broker supports \
             v0..=v2; needs a broker that supports Produce v3 or newer"
        );

        let broker_too_new = WireError::UnsupportedApiVersion {
            api_key: ApiKey::Metadata,
            client: range(0, 9),
            broker: Some(range(10, 13)),
        };
        assert_eq!(
            broker_too_new.to_string(),
            "no compatible Metadata API version (key 3): kacrab supports v0..=v9, broker supports \
             v10..=v13; kacrab speaks Metadata v9 at most, so kacrab needs upgrading"
        );
    }

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
        assert!(
            !WireError::UnsupportedApiVersion {
                api_key: ApiKey::Metadata,
                client: range(0, 13),
                broker: None,
            }
            .is_fatal_setup()
        );
    }
}
