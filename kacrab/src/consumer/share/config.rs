//! Runtime share-consumer configuration.
//!
//! The share consumer reuses the consumer's runtime knobs (fetch sizing, request
//! timeout, retry backoff, heartbeat cadence) and adds the two KIP-932 keys the
//! generated catalog carries: `share.acknowledgement.mode` and
//! `share.acquire.mode`.

use kacrab_protocol::generated::ApiKey;

use crate::{
    config::ConsumerConfig,
    consumer::{
        config::ConsumerRuntimeConfig,
        error::{ConsumerError, Result},
    },
};

/// The first `ShareFetch` version that carries the acquire mode (KIP-1222).
pub(super) const SHARE_ACQUIRE_MODE_MIN_VERSION: i16 = 2;

/// Whether the application acknowledges each record or the consumer does it,
/// mirroring Kafka's `share.acknowledgement.mode`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShareAcknowledgementMode {
    /// The consumer acknowledges every delivered record with
    /// [`AcknowledgeType::Accept`](super::AcknowledgeType::Accept) on the next
    /// `poll` or `commit`. Calling
    /// [`acknowledge`](super::ShareConsumer::acknowledge) is an error.
    Implicit,
    /// The application must acknowledge every delivered record before the next
    /// `poll` or `commit`; leaving one unacknowledged is an error.
    Explicit,
}

impl ShareAcknowledgementMode {
    fn parse(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "implicit" => Ok(Self::Implicit),
            "explicit" => Ok(Self::Explicit),
            _ => Err(invalid("share.acknowledgement.mode", value)),
        }
    }
}

/// How the broker chooses how much to acquire per `ShareFetch`, mirroring
/// Kafka's `share.acquire.mode`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShareAcquireMode {
    /// The broker may return more than `max.poll.records` so an acquisition
    /// lands on record-batch boundaries (the Kafka default).
    BatchOptimized,
    /// The broker never acquires more than `max.poll.records` in one fetch.
    RecordLimit,
}

impl ShareAcquireMode {
    fn parse(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "batch_optimized" => Ok(Self::BatchOptimized),
            "record_limit" => Ok(Self::RecordLimit),
            _ => Err(invalid("share.acquire.mode", value)),
        }
    }

    /// The wire byte for `ShareFetch`'s `share_acquire_mode` field.
    #[must_use]
    pub const fn wire(self) -> i8 {
        match self {
            Self::BatchOptimized => 0,
            Self::RecordLimit => 1,
        }
    }
}

/// Runtime knobs for a [`ShareConsumer`](super::ShareConsumer).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareRuntimeConfig {
    /// The consumer knobs the share consumer shares with [`Consumer`](crate::consumer::Consumer).
    pub base: ConsumerRuntimeConfig,
    /// Whether the application acknowledges records (`share.acknowledgement.mode`).
    pub acknowledgement_mode: ShareAcknowledgementMode,
    /// The broker-side acquire strategy (`share.acquire.mode`). Only reaches the
    /// broker at `ShareFetch` v2 and above; see
    /// [`acquire_mode_for_version`](Self::acquire_mode_for_version).
    pub acquire_mode: ShareAcquireMode,
}

impl ShareRuntimeConfig {
    /// Build share-consumer settings from the public typed Kafka config.
    ///
    /// # Errors
    /// Returns [`ConsumerError::InvalidArgument`] for out-of-domain values of
    /// `share.acknowledgement.mode` or `share.acquire.mode`, or for any value the
    /// shared consumer config rejects.
    pub fn from_config(config: &ConsumerConfig) -> Result<Self> {
        Ok(Self {
            base: ConsumerRuntimeConfig::from_config(config)?,
            acknowledgement_mode: ShareAcknowledgementMode::parse(
                &config.share_acknowledgement_mode,
            )?,
            acquire_mode: ShareAcquireMode::parse(&config.share_acquire_mode)?,
        })
    }

    /// The acquire mode byte to put on the wire for a negotiated `ShareFetch`
    /// version.
    ///
    /// The field arrived in v2 (KIP-1222); writing a non-zero value at v1 is a
    /// codec error, so a broker that only speaks v1 gets the default
    /// (`batch_optimized`) and `share.acquire.mode=record_limit` is inert there.
    /// `max.poll.records` still bounds the acquisition through the request's
    /// `max_records` field, which exists at v1.
    #[must_use]
    pub const fn acquire_mode_for_version(&self, version: i16) -> i8 {
        if version >= SHARE_ACQUIRE_MODE_MIN_VERSION {
            self.acquire_mode.wire()
        } else {
            ShareAcquireMode::BatchOptimized.wire()
        }
    }
}

/// The `ShareFetch`/`ShareAcknowledge` version to use with a broker, given the
/// version it negotiated. Falls back to the client's minimum when `ApiVersions`
/// has not completed yet, matching how the fetcher picks its safe default.
pub(super) fn share_api_version(negotiated: Option<i16>, api_key: ApiKey) -> i16 {
    negotiated.unwrap_or_else(|| kacrab_protocol::version::client_api_info(api_key).min_version)
}

fn invalid(key: &'static str, value: &str) -> ConsumerError {
    let expected = match key {
        "share.acknowledgement.mode" => "implicit|explicit",
        "share.acquire.mode" => "batch_optimized|record_limit",
        _ => "a supported value",
    };
    ConsumerError::InvalidArgument {
        field: key,
        message: format!("unsupported value {value:?} (expected {expected})"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ClientConfig;

    #[test]
    fn modes_parse_case_insensitively() {
        assert_eq!(
            ShareAcknowledgementMode::parse(" Implicit ").unwrap(),
            ShareAcknowledgementMode::Implicit
        );
        assert_eq!(
            ShareAcknowledgementMode::parse("EXPLICIT").unwrap(),
            ShareAcknowledgementMode::Explicit
        );
        assert!(ShareAcknowledgementMode::parse("maybe").is_err());

        assert_eq!(
            ShareAcquireMode::parse("Batch_Optimized").unwrap(),
            ShareAcquireMode::BatchOptimized
        );
        assert_eq!(
            ShareAcquireMode::parse("record_limit").unwrap(),
            ShareAcquireMode::RecordLimit
        );
        assert!(ShareAcquireMode::parse("greedy").is_err());
        assert_eq!(ShareAcquireMode::BatchOptimized.wire(), 0);
        assert_eq!(ShareAcquireMode::RecordLimit.wire(), 1);
    }

    #[test]
    fn from_config_defaults_to_implicit_and_batch_optimized() {
        let client: ClientConfig = [("bootstrap.servers", "127.0.0.1:9092"), ("group.id", "w")]
            .into_iter()
            .collect();
        let config = ShareRuntimeConfig::from_config(&client.consumer_config().expect("config"))
            .expect("runtime");
        assert_eq!(
            config.acknowledgement_mode,
            ShareAcknowledgementMode::Implicit
        );
        assert_eq!(config.acquire_mode, ShareAcquireMode::BatchOptimized);
        assert_eq!(config.base.group_id, "w");
    }

    #[test]
    fn record_limit_only_reaches_the_wire_at_v2() {
        let client: ClientConfig = [
            ("bootstrap.servers", "127.0.0.1:9092"),
            ("group.id", "w"),
            ("share.acknowledgement.mode", "explicit"),
            ("share.acquire.mode", "record_limit"),
        ]
        .into_iter()
        .collect();
        let config = ShareRuntimeConfig::from_config(&client.consumer_config().expect("config"))
            .expect("runtime");
        assert_eq!(
            config.acknowledgement_mode,
            ShareAcknowledgementMode::Explicit
        );
        assert_eq!(config.acquire_mode_for_version(2), 1);
        // v1 predates the field; writing a non-zero value there is a codec error.
        assert_eq!(config.acquire_mode_for_version(1), 0);
    }

    #[test]
    fn share_api_version_falls_back_to_the_client_minimum() {
        assert_eq!(share_api_version(Some(2), ApiKey::ShareFetch), 2);
        assert_eq!(share_api_version(None, ApiKey::ShareFetch), 1);
        assert_eq!(share_api_version(None, ApiKey::ShareAcknowledge), 1);
    }

    #[test]
    fn an_unsupported_mode_names_the_config_key() {
        let client: ClientConfig = [
            ("bootstrap.servers", "127.0.0.1:9092"),
            ("share.acknowledgement.mode", "eventually"),
        ]
        .into_iter()
        .collect();
        let error = ShareRuntimeConfig::from_config(&client.consumer_config().expect("config"))
            .expect_err("unsupported mode");
        assert!(matches!(
            error,
            ConsumerError::InvalidArgument {
                field: "share.acknowledgement.mode",
                ..
            }
        ));
    }
}
