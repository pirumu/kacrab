//! Kafka consumer client (`consumer` feature).
//!
//! A native, Java-compatible implementation of Apache Kafka's `Consumer`, built
//! on the same wire/session layer (and therefore the same TLS/SASL auth) as the
//! producer and admin clients. "Java-compatible" means Kafka-protocol- and
//! behaviour-compatible, not a literal port.
//!
//! See `docs/consumer-design.md` for the design of record and the phased plan.
//! This module is under active construction: Phase 1 lands manual-assignment
//! fetch (no group coordination yet).

mod client;
mod config;
mod error;
mod fetch;
mod offsets;
mod record;
mod subscription;

pub use self::{
    client::Consumer,
    config::{AutoOffsetReset, ConsumerRuntimeConfig, IsolationLevel},
    error::{ConsumerError, Result},
    record::{ConsumerRecord, ConsumerRecords, RecordHeader, TimestampType},
};
