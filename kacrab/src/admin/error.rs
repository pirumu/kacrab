//! Error types for admin operations.

use kacrab_protocol::generated::ErrorCode;
use thiserror::Error;

use crate::{config::ConfigError, wire::WireError};

/// Errors from admin client operations.
#[derive(Debug, Error)]
pub enum AdminError {
    /// Lower-level wire/session failure.
    #[error(transparent)]
    Wire(#[from] WireError),
    /// A broker rejected an admin request with a protocol error code.
    ///
    /// `target` names the resource the error applies to (a topic name, a
    /// broker/config resource, or an empty string for top-level errors).
    #[error("admin request for {target:?} failed: {error:?}{}", .message.as_deref().map(|m| format!(" ({m})")).unwrap_or_default())]
    Broker {
        /// Resource the error applies to (topic name, config resource, etc.).
        target: String,
        /// Protocol error code returned by the broker.
        error: ErrorCode,
        /// Optional broker-supplied error message.
        message: Option<String>,
    },
    /// The cluster could not name a controller broker for a controller-only
    /// request, even after refreshing metadata.
    #[error("no controller broker is currently known")]
    NoController,
    /// A controller-routed request kept being rejected as not-controller after
    /// exhausting metadata refreshes and retries.
    #[error(
        "controller routing failed after {attempts} attempts: cluster never settled on a \
         controller"
    )]
    ControllerUnavailable {
        /// Number of routing attempts made before giving up.
        attempts: u32,
    },
    /// The broker response was missing an entry the request asked about.
    #[error("admin response omitted a result for {target:?}")]
    MissingResult {
        /// Resource whose result was expected but absent.
        target: String,
    },
    /// The group/transaction coordinator could not be located, or the address it
    /// advertised did not resolve to a connectable broker.
    #[error("coordinator for {key:?} could not be resolved")]
    CoordinatorUnavailable {
        /// The group or transactional id whose coordinator was requested.
        key: String,
    },
    /// An admin argument failed validation before any request was sent.
    #[error("invalid admin argument {field}: {message}")]
    InvalidArgument {
        /// Argument field name.
        field: &'static str,
        /// Human-readable validation message.
        message: String,
    },
    /// Public Kafka admin config could not be mapped to typed config.
    #[error("admin config error: {error}")]
    Config {
        /// Configuration validation error.
        error: ConfigError,
    },
}

impl From<ConfigError> for AdminError {
    fn from(error: ConfigError) -> Self {
        Self::Config { error }
    }
}

/// Result alias for admin operations.
pub type Result<T> = std::result::Result<T, AdminError>;
