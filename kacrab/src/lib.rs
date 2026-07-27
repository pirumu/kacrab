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

/// Internal parser surface exposed to `fuzz/` under the `__fuzzing` feature.
///
/// Not public API. Exempt from semver, undocumented on docs.rs, and unusable
/// without opting into a feature named to discourage exactly that. It exists
/// because the SASL handshake parsers are `pub(crate)` while a `cargo-fuzz`
/// target must live in a separate crate, and those parsers read bytes from a
/// peer that has not authenticated yet — the most security-relevant untrusted
/// input the client handles.
///
/// These are entry points, not re-exports: each is a thin `fn(&[u8])` shim that
/// drives one crate-private parser. Exposing shims rather than the types keeps
/// the internals `pub(crate)` and keeps this surface a handful of functions that
/// cannot be mistaken for an API.
#[cfg(feature = "__fuzzing")]
#[doc(hidden)]
pub mod __fuzz {
    use crate::wire::{SaslMechanism, auth::ScramExchange};

    /// A credential config in the form kacrab documents, so `start` always
    /// succeeds and every fuzz iteration reaches the parser under test.
    ///
    /// Deliberately *not* the `org.apache.kafka...LoginModule required ...`
    /// prefix a JVM client would use. `jaas_option` is a substring scan, so the
    /// class name is inert here — there is no JVM to load it, and the design
    /// book says so outright: "JVM `sasl.jaas.config` class names cannot cross
    /// into a Rust process". kacrab accepts the long form for operators pasting
    /// an existing Kafka config, but the short form is what the README shows and
    /// what this harness should model.
    const CREDENTIALS: &str = r#"username="fuzz" password="fuzz";"#;

    /// Parse an attacker-supplied SCRAM server-first message.
    ///
    /// This runs before the peer is authenticated: SCRAM proves the server only
    /// at server-final, so everything here is reached by anyone who can answer
    /// on the broker's address.
    pub fn scram_server_first(bytes: &[u8]) {
        let Ok((exchange, _client_first)) =
            ScramExchange::start(SaslMechanism::ScramSha256, Some(CREDENTIALS))
        else {
            return;
        };
        drop(exchange.client_final(bytes));
    }

    /// As [`scram_server_first`], but with the client nonce already satisfied.
    ///
    /// `client_final` rejects any server-first whose `r=` does not extend the
    /// client's own randomly generated nonce. That check is unguessable by
    /// construction, so the raw target above can only ever exercise the parser
    /// and the nonce comparison — the base64 salt decode, the iteration count,
    /// and the PBKDF2 derivation behind it are unreachable, exactly the way
    /// CRC32C hides the record-batch decoder from raw bytes.
    ///
    /// Here the harness writes `r=<client_nonce>` and the fuzzer supplies the
    /// rest of the message, so a real broker's reply shape (`...,s=<salt>,
    /// i=<iterations>`) is reachable and the attacker-controlled fields behind
    /// the nonce are actually explored. Keep both: this one can never find a
    /// bug in the nonce check itself.
    pub fn scram_server_first_nonced(bytes: &[u8]) {
        let Ok(tail) = core::str::from_utf8(bytes) else {
            return;
        };
        let Ok((exchange, _client_first)) =
            ScramExchange::start(SaslMechanism::ScramSha256, Some(CREDENTIALS))
        else {
            return;
        };
        let mut message = String::from("r=");
        message.push_str(exchange.fuzz_client_nonce());
        message.push_str(tail);
        drop(exchange.client_final(message.as_bytes()));
    }

    /// Parse an attacker-supplied SCRAM server-final message.
    ///
    /// The input is split into the expected signature and the message so the
    /// fuzzer controls both sides of the comparison, including mismatched
    /// lengths and empty inputs.
    pub fn scram_server_final(bytes: &[u8]) {
        let split = bytes.first().map_or(0, |byte| usize::from(*byte));
        let rest = bytes.get(1..).unwrap_or_default();
        let split = split.min(rest.len());
        let (expected_signature, server_final) = rest.split_at(split);
        drop(ScramExchange::verify_server_final(
            server_final,
            expected_signature,
        ));
    }

    /// Parse a JAAS config string, which carries the SASL credentials.
    pub fn jaas_option(bytes: &[u8]) {
        let Ok(config) = core::str::from_utf8(bytes) else {
            return;
        };
        for key in ["username", "password", "tokenauth", "unknown"] {
            drop(crate::wire::auth::jaas_option(config, key));
        }
    }
}
