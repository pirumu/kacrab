//! Native Rust extension points for SASL authentication.

use std::{fmt, net::SocketAddr, sync::Arc};

use bytes::Bytes;

use super::{SaslMechanism, WireError};

/// Broker/session metadata passed to a native SASL authenticator factory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SaslClientSession {
    node_id: i32,
    host: String,
    port: u16,
    addr: SocketAddr,
}

impl SaslClientSession {
    /// Create SASL client session metadata from a resolved broker endpoint.
    #[must_use]
    pub const fn new(node_id: i32, host: String, port: u16, addr: SocketAddr) -> Self {
        Self {
            node_id,
            host,
            port,
            addr,
        }
    }

    /// Broker node id from cluster metadata.
    #[must_use]
    pub const fn node_id(&self) -> i32 {
        self.node_id
    }

    /// Advertised or configured broker host.
    #[must_use]
    pub fn host(&self) -> &str {
        &self.host
    }

    /// Advertised or configured broker port.
    #[must_use]
    pub const fn port(&self) -> u16 {
        self.port
    }

    /// Resolved socket address used by this connection.
    #[must_use]
    pub const fn addr(&self) -> SocketAddr {
        self.addr
    }
}

/// One action produced by a native SASL client authenticator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SaslClientAction {
    /// Send the contained auth bytes in a `SaslAuthenticate` request.
    Send(Bytes),
    /// Authentication has completed and no more client bytes are required.
    Complete,
}

/// Native Rust replacement for Java `sasl.*.callback.handler.class` hooks.
///
/// The broker session still owns Kafka framing, correlation ids, timeouts, and
/// connection cleanup. This hook only owns mechanism-specific client bytes for
/// the SASL exchange.
pub trait SaslClientAuthenticator: fmt::Debug + Send + Sync + 'static {
    /// Returns the Kafka SASL mechanism this authenticator implements.
    fn mechanism(&self) -> SaslMechanism;

    /// Starts the SASL exchange.
    ///
    /// # Errors
    ///
    /// Returns [`WireError`] when the authenticator cannot produce its first
    /// client action.
    fn start(&self) -> Result<SaslClientAction, WireError>;

    /// Advances the SASL exchange using broker challenge bytes.
    ///
    /// # Errors
    ///
    /// Returns [`WireError`] when the challenge is invalid or the authenticator
    /// cannot produce the next client action.
    fn next(&self, challenge: &[u8]) -> Result<SaslClientAction, WireError>;
}

/// Factory for per-session native SASL client authenticators.
///
/// Prefer this over sharing one [`SaslClientAuthenticator`] when the mechanism
/// carries challenge/response state. The broker session invokes the factory
/// once per connection and owns Kafka framing, timeouts, and cleanup.
pub trait SaslClientAuthenticatorFactory: fmt::Debug + Send + Sync + 'static {
    /// Returns the Kafka SASL mechanism this factory creates.
    fn mechanism(&self) -> SaslMechanism;

    /// Creates a native SASL authenticator for one broker session.
    ///
    /// # Errors
    ///
    /// Returns [`WireError`] when session-specific authenticator setup fails.
    fn create(
        &self,
        session: &SaslClientSession,
    ) -> Result<SaslClientAuthenticatorHandle, WireError>;
}

/// Cloneable handle for a native SASL client authenticator.
#[derive(Clone)]
pub struct SaslClientAuthenticatorHandle {
    inner: Arc<dyn SaslClientAuthenticator>,
}

impl SaslClientAuthenticatorHandle {
    /// Wraps a native SASL client authenticator for use in connection configs.
    pub fn new(authenticator: impl SaslClientAuthenticator) -> Self {
        Self {
            inner: Arc::new(authenticator),
        }
    }

    /// Returns the Kafka SASL mechanism implemented by this authenticator.
    pub fn mechanism(&self) -> SaslMechanism {
        self.inner.mechanism()
    }

    pub(crate) fn start(&self) -> Result<SaslClientAction, WireError> {
        self.inner.start()
    }

    pub(crate) fn next(&self, challenge: &[u8]) -> Result<SaslClientAction, WireError> {
        self.inner.next(challenge)
    }
}

impl fmt::Debug for SaslClientAuthenticatorHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SaslClientAuthenticatorHandle")
            .field("mechanism", &self.mechanism())
            .finish_non_exhaustive()
    }
}

impl PartialEq for SaslClientAuthenticatorHandle {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
}

/// Cloneable handle for a per-session SASL authenticator factory.
#[derive(Clone)]
pub struct SaslClientAuthenticatorFactoryHandle {
    inner: Arc<dyn SaslClientAuthenticatorFactory>,
}

impl SaslClientAuthenticatorFactoryHandle {
    /// Wraps a native SASL client authenticator factory for use in connection configs.
    pub fn new(factory: impl SaslClientAuthenticatorFactory) -> Self {
        Self {
            inner: Arc::new(factory),
        }
    }

    /// Returns the Kafka SASL mechanism created by this factory.
    pub fn mechanism(&self) -> SaslMechanism {
        self.inner.mechanism()
    }

    pub(crate) fn create(
        &self,
        session: &SaslClientSession,
    ) -> Result<SaslClientAuthenticatorHandle, WireError> {
        self.inner.create(session)
    }
}

impl fmt::Debug for SaslClientAuthenticatorFactoryHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SaslClientAuthenticatorFactoryHandle")
            .field("mechanism", &self.mechanism())
            .finish_non_exhaustive()
    }
}

impl PartialEq for SaslClientAuthenticatorFactoryHandle {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::missing_assert_message,
        reason = "Unit tests keep assertions compact around simple API shape."
    )]

    use std::{
        net::{IpAddr, Ipv4Addr, SocketAddr},
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use super::{
        SaslClientAction, SaslClientAuthenticator, SaslClientAuthenticatorFactory,
        SaslClientAuthenticatorFactoryHandle, SaslClientAuthenticatorHandle, SaslClientSession,
    };
    use crate::wire::{SaslMechanism, WireError};

    #[derive(Debug)]
    struct FactoryAuthenticator {
        id: usize,
        host: String,
    }

    impl SaslClientAuthenticator for FactoryAuthenticator {
        fn mechanism(&self) -> SaslMechanism {
            SaslMechanism::Plain
        }

        fn start(&self) -> Result<SaslClientAction, WireError> {
            Ok(SaslClientAction::Send(
                format!("{}:{}", self.id, self.host).into(),
            ))
        }

        fn next(&self, _challenge: &[u8]) -> Result<SaslClientAction, WireError> {
            Ok(SaslClientAction::Complete)
        }
    }

    #[derive(Debug)]
    struct CountingFactory {
        next_id: Arc<AtomicUsize>,
    }

    impl SaslClientAuthenticatorFactory for CountingFactory {
        fn mechanism(&self) -> SaslMechanism {
            SaslMechanism::Plain
        }

        fn create(
            &self,
            session: &SaslClientSession,
        ) -> Result<SaslClientAuthenticatorHandle, WireError> {
            let id = self.next_id.fetch_add(1, Ordering::Relaxed);
            Ok(SaslClientAuthenticatorHandle::new(FactoryAuthenticator {
                id,
                host: session.host().to_owned(),
            }))
        }
    }

    #[test]
    fn sasl_factory_creates_distinct_authenticators_per_session() {
        let factory = SaslClientAuthenticatorFactoryHandle::new(CountingFactory {
            next_id: Arc::new(AtomicUsize::new(1)),
        });
        let session = SaslClientSession::new(
            7,
            "broker.example.com".to_owned(),
            9092,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9092),
        );

        let first = factory.create(&session).unwrap();
        let second = factory.create(&session).unwrap();

        assert_eq!(factory.mechanism(), SaslMechanism::Plain);
        assert_eq!(
            first.start().unwrap(),
            SaslClientAction::Send("1:broker.example.com".into())
        );
        assert_eq!(
            second.start().unwrap(),
            SaslClientAction::Send("2:broker.example.com".into())
        );
    }

    #[cfg(feature = "gssapi")]
    #[test]
    fn gssapi_authenticator_uses_hostbased_kafka_service_name() {
        use crate::wire::SaslClientAuthenticator as _;

        let authenticator = crate::wire::GssapiAuthenticator::new(
            "kafka".to_owned(),
            "broker.example.com".to_owned(),
        );

        assert_eq!(authenticator.mechanism(), SaslMechanism::Gssapi);
        assert_eq!(authenticator.target_name(), "kafka@broker.example.com");
    }
}
