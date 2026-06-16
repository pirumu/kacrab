//! # kacrab
//!
//! A production-oriented Kafka client for Rust, built from the protocol up.
//!
//! This is the public entry point users depend on. The current runtime surface
//! is:
//!
//! - `config`: Java-style config facade, typed configs, metadata, and validation.
//! - `wire`: TCP/TLS/SASL sessions, `ApiVersions`, metadata, and request dispatch.
//! - `producer`: batching, routing, idempotence, transactions, and delivery handles.
//!
//! The companion crate [`kacrab-macros`] provides procedural macros.
//!
//! [`kacrab-macros`]: https://docs.rs/kacrab-macros

#![cfg_attr(not(feature = "std"), no_std)]

extern crate self as kacrab;

pub mod config;
#[cfg(feature = "producer")]
pub mod producer;
pub mod wire;

#[cfg(feature = "macros")]
pub use kacrab_macros::kafka_config;
