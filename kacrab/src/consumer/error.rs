//! Error types for consumer operations.

use kacrab_protocol::generated::ErrorCode;
use thiserror::Error;

use crate::{config::ConfigError, wire::WireError};

/// Result alias for consumer operations.
pub type Result<T> = std::result::Result<T, ConsumerError>;

/// Errors from consumer operations.
#[derive(Debug, Error)]
pub enum ConsumerError {
    /// Lower-level wire/session failure.
    #[error(transparent)]
    Wire(#[from] WireError),
    /// Public Kafka consumer config could not be mapped to typed config.
    #[error("consumer config error: {error}")]
    Config {
        /// Configuration validation error.
        error: ConfigError,
    },
    /// A broker returned a non-success error code for a consumer request.
    #[error("broker returned {error:?} for {operation}: {message}")]
    Broker {
        /// The consumer operation that failed.
        operation: &'static str,
        /// The broker-reported error code.
        error: ErrorCode,
        /// Human-readable detail (broker message or a synthesized note).
        message: String,
    },
    /// A `poll` had no committed offset for a partition and
    /// `auto.offset.reset=none`.
    #[error("no committed offset for {topic}-{partition} and auto.offset.reset=none")]
    NoOffsetForPartition {
        /// Topic name.
        topic: String,
        /// Partition index.
        partition: i32,
    },
    /// An operation referenced a partition that is not currently assigned.
    #[error("partition {topic}-{partition} is not assigned to this consumer")]
    PartitionNotAssigned {
        /// Topic name.
        topic: String,
        /// Partition index.
        partition: i32,
    },
    /// A blocking call was interrupted by [`Consumer::wakeup`](super::Consumer::wakeup).
    #[error("consumer operation was interrupted by wakeup")]
    Wakeup,
    /// A consumer API precondition was violated (e.g. mixing subscribe + assign).
    #[error("invalid consumer state: {0}")]
    InvalidState(&'static str),
    /// A consumer API argument was invalid (e.g. malformed `bootstrap.servers`).
    #[error("invalid consumer argument {field}: {message}")]
    InvalidArgument {
        /// The offending argument or config field.
        field: &'static str,
        /// Human-readable detail.
        message: String,
    },
}

impl ConsumerError {
    /// Build a [`ConsumerError::Broker`] from a broker error code.
    pub(crate) fn broker(
        operation: &'static str,
        error: ErrorCode,
        message: impl Into<String>,
    ) -> Self {
        Self::Broker {
            operation,
            error,
            message: message.into(),
        }
    }
}
