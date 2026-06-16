//! Error types for producer operations.

use kacrab_protocol::{generated::ErrorCode, record::RecordError};
use thiserror::Error;

use crate::{config::ConfigError, wire::WireError};

/// Errors from producer operations.
#[derive(Debug, Error)]
pub enum ProducerError {
    /// Lower-level wire/session failure.
    #[error(transparent)]
    Wire(#[from] WireError),
    /// Record batch encoding failed.
    #[error(transparent)]
    Record(#[from] RecordError),
    /// Producer buffer memory is exhausted.
    #[error("producer buffer memory exhausted")]
    Backpressure,
    /// Flush forced out buffered records but routing metadata was still incomplete.
    #[error("flush could not route all buffered records")]
    FlushIncomplete,
    /// Batch exceeded the configured delivery timeout.
    #[error("delivery timeout expired for {topic}-{partition}")]
    DeliveryTimeout {
        /// Topic name.
        topic: String,
        /// Partition index.
        partition: i32,
    },
    /// Topic was not present in metadata.
    #[error("topic metadata not found for {0}")]
    UnknownTopic(String),
    /// Partition was not present in metadata.
    #[error("partition {partition} not found for topic {topic}")]
    UnknownPartition {
        /// Topic name.
        topic: String,
        /// Partition index.
        partition: i32,
    },
    /// Metadata named a leader broker but did not include its endpoint.
    #[error("leader broker {leader_id} not found for {topic}-{partition}")]
    LeaderNotFound {
        /// Topic name.
        topic: String,
        /// Partition index.
        partition: i32,
        /// Leader broker id.
        leader_id: i32,
    },
    /// Produce response did not include the requested topic/partition.
    #[error("produce response missing {topic}-{partition}")]
    MissingProduceResponse {
        /// Topic name.
        topic: String,
        /// Partition index.
        partition: i32,
    },
    /// Broker returned an error code for the produced partition.
    #[error("produce failed for {topic}-{partition}: {error}")]
    Broker {
        /// Topic name.
        topic: String,
        /// Partition index.
        partition: i32,
        /// Kafka error code.
        error: ErrorCode,
    },
    /// Transaction/idempotent producer control API returned an error.
    #[error("producer transaction operation {operation} failed: {error}")]
    Transaction {
        /// Producer operation name.
        operation: &'static str,
        /// Kafka error code.
        error: ErrorCode,
    },
    /// Transactional method was called without a transactional id.
    #[error("producer transaction operation requires transactional.id")]
    TransactionalIdRequired,
    /// Transaction state transition is invalid.
    #[error("invalid producer transaction state: {0}")]
    InvalidTransactionState(&'static str),
    /// Transaction state is currently locked by another operation.
    #[error("producer transaction state is busy")]
    TransactionStateBusy,
    /// Per-partition idempotent sequence counter overflowed.
    #[error("producer sequence overflow for {topic}-{partition}")]
    SequenceOverflow {
        /// Topic name.
        topic: String,
        /// Partition index.
        partition: i32,
    },
    /// Internal async dispatch task failed before returning a broker result.
    #[error("producer dispatch task failed: {0}")]
    DispatchTask(String),
    /// Delivery handle was dropped before a broker receipt was produced.
    #[error("producer delivery was dropped before completion")]
    DeliveryDropped,
    /// Public Java-style producer config could not be mapped to typed config.
    #[error("producer config error: {error}")]
    Config {
        /// Configuration validation error.
        error: ConfigError,
    },
    /// Public producer config could not be mapped to runtime settings.
    #[error("invalid producer config {key}={value}")]
    InvalidConfig {
        /// Kafka configuration key.
        key: &'static str,
        /// Invalid configured value.
        value: String,
    },
}

/// Result alias for producer operations.
pub type Result<T> = core::result::Result<T, ProducerError>;
