//! # kacrab
//!
//! A Kafka client for Rust, built from the protocol up.
//!
//! This is the public entry point users depend on. The current runtime surface
//! is:
//!
//! - `common`: shared Kafka domain types (`TopicPartition`, offsets, group metadata).
//! - `config`: Java-style config facade, typed configs, metadata, and validation.
//! - `wire`: TCP/TLS/SASL sessions, `ApiVersions`, metadata, and request dispatch.
//! - `producer`: batching, routing, idempotence, transactions, and delivery handles.
//! - `consumer`: the Apache Kafka 4.3.0 `Consumer` surface (subscribe/assign, group rebalancing,
//!   fetching, offset commits, and interceptors).
//! - `admin`: the full Apache Kafka 4.3.0 `Admin` operation surface (topics, configs, groups,
//!   offsets, transactions, ACLs, quotas, tokens, and more).
//!
//! The companion crate [`kacrab-macros`] provides procedural macros.
//!
//! [`kacrab-macros`]: https://docs.rs/kacrab-macros

extern crate self as kacrab;

#[cfg(feature = "admin")]
pub mod admin;
pub mod common;
pub mod config;
#[cfg(feature = "consumer")]
pub mod consumer;
#[cfg(feature = "producer")]
pub mod producer;
pub mod wire;

#[cfg(feature = "macros")]
pub use kacrab_macros::kafka_config;
