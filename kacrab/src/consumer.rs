//! Kafka consumer client (`consumer` feature).
//!
//! A native, Java-compatible implementation of Apache Kafka's `Consumer`, built
//! on the same wire/session layer (and therefore the same TLS/SASL auth) as the
//! producer and admin clients. "Java-compatible" means Kafka-protocol- and
//! behaviour-compatible, not a literal port.
//!
//! See the consumer chapters of the book (`docs-book/`) for the design and
//! rationale. The consumer supports manual assignment, topic and pattern
//! subscription, both group
//! protocols (classic `JoinGroup`/`SyncGroup` and KIP-848
//! `ConsumerGroupHeartbeat`), fetch with incremental sessions, and offset
//! commit/fetch.

mod assignor;
mod client;
mod config;
mod coordinator;
mod deserializer;
mod error;
mod fetch;
mod interceptor;
mod metrics;
mod next_gen;
mod offsets;
mod record;
mod subscription;

pub use self::{
    client::{Consumer, OffsetCommitCallback},
    config::{AutoOffsetReset, ConsumerRuntimeConfig, GroupProtocol, IsolationLevel},
    deserializer::{
        ByteArrayDeserializer, BytesDeserializer, ConsumerDeserializer, StringDeserializer,
    },
    error::{ConsumerError, Result},
    interceptor::{ConsumerInterceptor, InterceptorConfigs},
    metrics::ConsumerMetricsSnapshot,
    record::{ConsumerRecord, ConsumerRecords, OffsetAndTimestamp, RecordHeader, TimestampType},
};
