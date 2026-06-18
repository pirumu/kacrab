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
    /// Producer record field is invalid before serialization or append.
    #[error("invalid producer record {field}: {message}")]
    InvalidRecord {
        /// Invalid producer record field.
        field: &'static str,
        /// Human-readable validation message.
        message: &'static str,
    },
    /// Serialized record would exceed the configured producer request bound.
    #[error("serialized record is {size} bytes, larger than max.request.size={max_request_size}")]
    RecordTooLarge {
        /// Estimated serialized record-batch bytes.
        size: usize,
        /// Configured `max.request.size`.
        max_request_size: usize,
    },
    /// Flush forced out buffered records but routing metadata was still incomplete.
    #[error("flush could not route all buffered records")]
    FlushIncomplete,
    /// Producer API was called from a delivery callback where Java forbids blocking.
    #[error("producer operation {operation} cannot be invoked from a delivery callback")]
    CallbackOperation {
        /// Producer operation name.
        operation: &'static str,
    },
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
    /// Consumer group metadata is invalid for transactional offset commit.
    #[error("invalid consumer group metadata: {0}")]
    InvalidConsumerGroupMetadata(&'static str),
    /// Per-partition idempotent sequence counter overflowed.
    #[error("producer sequence overflow for {topic}-{partition}")]
    SequenceOverflow {
        /// Topic name.
        topic: String,
        /// Partition index.
        partition: i32,
    },
    /// A previous idempotent batch left the partition sequence unresolved.
    #[error("producer sequence is unresolved for {topic}-{partition}")]
    UnresolvedSequence {
        /// Topic name.
        topic: String,
        /// Partition index.
        partition: i32,
    },
    /// Internal async dispatch task failed before returning a broker result.
    #[error("producer dispatch task failed: {0}")]
    DispatchTask(String),
    /// `SendFuture` handle was dropped before a broker receipt was produced.
    #[error("producer delivery was dropped before completion")]
    DeliveryDropped,
    /// Public API exists for Java compatibility, but the backend is not wired yet.
    #[error("producer operation is not supported yet: {0}")]
    UnsupportedOperation(&'static str),
    /// Client telemetry APIs were called while `enable.metrics.push=false`.
    #[error("telemetry is not enabled; set config `enable.metrics.push` to `true`")]
    TelemetryDisabled,
    /// Broker returned an error for a client telemetry operation.
    #[error("producer telemetry operation {operation} failed: {error}")]
    Telemetry {
        /// Producer telemetry operation name.
        operation: &'static str,
        /// Kafka error code.
        error: ErrorCode,
    },
    /// Broker returned invalid client telemetry subscription data.
    #[error("invalid producer telemetry subscription: {0}")]
    InvalidTelemetrySubscription(&'static str),
    /// Client telemetry timeout argument is invalid.
    #[error("invalid producer telemetry timeout: {timeout_ms}ms")]
    InvalidTelemetryTimeout {
        /// Timeout in milliseconds supplied by the caller.
        timeout_ms: i64,
    },
    /// Producer close timeout argument is invalid.
    #[error("invalid producer close timeout: {timeout_ms}ms")]
    InvalidCloseTimeout {
        /// Timeout in milliseconds supplied by the caller.
        timeout_ms: i64,
    },
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
