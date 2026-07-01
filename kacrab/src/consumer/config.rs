//! Runtime consumer configuration derived from typed Kafka consumer config.

use std::time::Duration;

use super::error::{ConsumerError, Result};
use crate::config::ConsumerConfig;

/// Reset policy applied when a partition has no valid fetch position, mirroring
/// Kafka's `auto.offset.reset`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AutoOffsetReset {
    /// Reset to the earliest available offset.
    Earliest,
    /// Reset to the latest offset (the default).
    Latest,
    /// Do not reset; surface an error instead.
    None,
}

impl AutoOffsetReset {
    fn parse(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "earliest" => Ok(Self::Earliest),
            "latest" => Ok(Self::Latest),
            "none" => Ok(Self::None),
            _ => Err(invalid("auto.offset.reset", value)),
        }
    }
}

/// Transactional read visibility, mirroring Kafka's `isolation.level`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsolationLevel {
    /// Read all records up to the high watermark.
    ReadUncommitted,
    /// Read only records up to the last stable offset.
    ReadCommitted,
}

impl IsolationLevel {
    fn parse(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "read_uncommitted" => Ok(Self::ReadUncommitted),
            "read_committed" => Ok(Self::ReadCommitted),
            _ => Err(invalid("isolation.level", value)),
        }
    }

    /// The wire byte for the `Fetch`/`ListOffsets` `isolation_level` field.
    #[must_use]
    pub const fn wire(self) -> i8 {
        match self {
            Self::ReadUncommitted => 0,
            Self::ReadCommitted => 1,
        }
    }
}

/// Runtime consumer knobs used by the fetcher, offset manager, and poll loop.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerRuntimeConfig {
    /// Consumer group id (empty when consuming without group management).
    pub group_id: String,
    /// Logical client id sent to brokers.
    pub client_id: String,
    /// Rack id for rack-aware (follower) fetches.
    pub client_rack: String,
    /// Reset policy when a partition has no valid position.
    pub auto_offset_reset: AutoOffsetReset,
    /// Transactional read visibility.
    pub isolation_level: IsolationLevel,
    /// Minimum bytes the broker should accumulate before answering a fetch.
    pub fetch_min_bytes: i32,
    /// Soft cap on the total bytes returned by one fetch.
    pub fetch_max_bytes: i32,
    /// Maximum time the broker waits to satisfy `fetch_min_bytes`.
    pub fetch_max_wait_ms: i32,
    /// Maximum bytes returned per partition in one fetch.
    pub max_partition_fetch_bytes: i32,
    /// Maximum records returned from one `poll` call.
    pub max_poll_records: usize,
    /// Whether record-batch CRCs are validated on read.
    pub check_crcs: bool,
    /// Per-request timeout for consumer RPCs.
    pub request_timeout: Duration,
    /// Whether background auto-commit is enabled (used from Phase 2).
    pub enable_auto_commit: bool,
    /// Background auto-commit interval (used from Phase 2).
    pub auto_commit_interval: Duration,
}

impl ConsumerRuntimeConfig {
    /// Build runtime consumer settings from the public typed Kafka config.
    ///
    /// # Errors
    /// Returns [`ConsumerError::InvalidConfig`]-style errors for out-of-domain
    /// enum values (`auto.offset.reset`, `isolation.level`).
    pub fn from_config(config: &ConsumerConfig) -> Result<Self> {
        Ok(Self {
            group_id: config.group_id.clone(),
            client_id: config.client_id.clone(),
            client_rack: config.client_rack.clone(),
            auto_offset_reset: AutoOffsetReset::parse(&config.auto_offset_reset)?,
            isolation_level: IsolationLevel::parse(&config.isolation_level)?,
            fetch_min_bytes: config.fetch_min_bytes,
            fetch_max_bytes: clamp_i32(config.fetch_max_bytes.get()),
            fetch_max_wait_ms: clamp_i32(config.fetch_max_wait_ms.as_millis()),
            max_partition_fetch_bytes: clamp_i32(config.max_partition_fetch_bytes.get()),
            max_poll_records: usize::try_from(config.max_poll_records.max(0)).unwrap_or(0),
            check_crcs: config.check_crcs,
            request_timeout: config.request_timeout_ms.duration(),
            enable_auto_commit: config.enable_auto_commit,
            auto_commit_interval: config.auto_commit_interval_ms.duration(),
        })
    }
}

fn clamp_i32(value: impl TryInto<i32>) -> i32 {
    value.try_into().unwrap_or(i32::MAX)
}

fn invalid(key: &'static str, _value: &str) -> ConsumerError {
    ConsumerError::InvalidState(match key {
        "auto.offset.reset" => "invalid auto.offset.reset (expected earliest|latest|none)",
        "isolation.level" => "invalid isolation.level (expected read_uncommitted|read_committed)",
        _ => "invalid consumer config value",
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auto_offset_reset_parses_case_insensitively() {
        assert_eq!(
            AutoOffsetReset::parse("Earliest").unwrap(),
            AutoOffsetReset::Earliest
        );
        assert_eq!(
            AutoOffsetReset::parse("LATEST").unwrap(),
            AutoOffsetReset::Latest
        );
        assert_eq!(
            AutoOffsetReset::parse(" none ").unwrap(),
            AutoOffsetReset::None
        );
        assert!(AutoOffsetReset::parse("sideways").is_err());
    }

    #[test]
    fn isolation_level_parses_and_maps_to_wire() {
        assert_eq!(IsolationLevel::parse("read_uncommitted").unwrap().wire(), 0);
        assert_eq!(IsolationLevel::parse("READ_COMMITTED").unwrap().wire(), 1);
        assert!(IsolationLevel::parse("dirty").is_err());
    }
}
