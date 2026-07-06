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

/// Which consumer group rebalance protocol to use, mirroring Kafka's
/// `group.protocol`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupProtocol {
    /// The classic client-side-assignment protocol (`JoinGroup`/`SyncGroup`).
    Classic,
    /// The KIP-848 server-side protocol (`ConsumerGroupHeartbeat`).
    Consumer,
}

impl GroupProtocol {
    fn parse(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "classic" => Ok(Self::Classic),
            "consumer" => Ok(Self::Consumer),
            _ => Err(invalid("group.protocol", value)),
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
    /// Static group instance id (empty when not a static member).
    pub group_instance_id: String,
    /// Logical client id sent to brokers.
    pub client_id: String,
    /// Rack id for rack-aware (follower) fetches.
    pub client_rack: String,
    /// Reset policy when a partition has no valid position.
    pub auto_offset_reset: AutoOffsetReset,
    /// Ordered client-side assignor names (`partition.assignment.strategy`).
    pub partition_assignment_strategy: Vec<String>,
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
    /// Whether background auto-commit is enabled.
    pub enable_auto_commit: bool,
    /// Background auto-commit interval.
    pub auto_commit_interval: Duration,
    /// Group session timeout (`session.timeout.ms`).
    pub session_timeout: Duration,
    /// Heartbeat cadence (`heartbeat.interval.ms`).
    pub heartbeat_interval: Duration,
    /// Rebalance timeout — how long `JoinGroup` may block (`max.poll.interval.ms`).
    pub rebalance_timeout: Duration,
    /// Whether internal topics (e.g. `__consumer_offsets`) are excluded from a
    /// pattern subscription (`exclude.internal.topics`).
    pub exclude_internal_topics: bool,
    /// Rebalance protocol to use (`group.protocol`).
    pub group_protocol: GroupProtocol,
    /// Server-side assignor name for the KIP-848 protocol
    /// (`group.remote.assignor`); `None` lets the coordinator choose.
    pub group_remote_assignor: Option<String>,
    /// Initial retry backoff (`retry.backoff.ms`) — also the idle-poll wait
    /// when nothing is fetchable yet.
    pub retry_backoff: Duration,
    /// Exponential retry backoff ceiling (`retry.backoff.max.ms`).
    pub retry_backoff_max: Duration,
}

impl ConsumerRuntimeConfig {
    /// Build runtime consumer settings from the public typed Kafka config.
    ///
    /// # Errors
    /// Returns [`ConsumerError::InvalidArgument`] for out-of-domain enum values
    /// (`auto.offset.reset`, `isolation.level`, `group.protocol`).
    pub fn from_config(config: &ConsumerConfig) -> Result<Self> {
        Ok(Self {
            group_id: config.group_id.clone(),
            group_instance_id: config.group_instance_id.clone(),
            client_id: config.client_id.clone(),
            client_rack: config.client_rack.clone(),
            auto_offset_reset: AutoOffsetReset::parse(&config.auto_offset_reset)?,
            partition_assignment_strategy: config.partition_assignment_strategy.as_slice().to_vec(),
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
            session_timeout: config.session_timeout_ms.duration(),
            heartbeat_interval: config.heartbeat_interval_ms.duration(),
            rebalance_timeout: config.max_poll_interval_ms.duration(),
            exclude_internal_topics: config.exclude_internal_topics,
            group_protocol: GroupProtocol::parse(&config.group_protocol)?,
            group_remote_assignor: (!config.group_remote_assignor.is_empty())
                .then(|| config.group_remote_assignor.clone()),
            retry_backoff: config.retry_backoff_ms.duration(),
            retry_backoff_max: config.retry_backoff_max_ms.duration(),
        })
    }

    /// Java-parity exponential retry policy (`retry.backoff.ms` doubling up to
    /// `retry.backoff.max.ms`, 20% jitter — `AbstractCoordinator`'s
    /// `ExponentialBackoff`).
    pub(crate) fn retry_backoff_policy(&self) -> crate::wire::BackoffPolicy {
        crate::wire::BackoffPolicy::new(self.retry_backoff, self.retry_backoff_max)
    }
}

fn clamp_i32(value: impl TryInto<i32>) -> i32 {
    value.try_into().unwrap_or(i32::MAX)
}

fn invalid(key: &'static str, value: &str) -> ConsumerError {
    let expected = match key {
        "auto.offset.reset" => "earliest|latest|none",
        "isolation.level" => "read_uncommitted|read_committed",
        "group.protocol" => "classic|consumer",
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
    fn from_config_maps_public_config_to_runtime() {
        use crate::config::ClientConfig;

        let client: ClientConfig = [
            ("bootstrap.servers", "127.0.0.1:9092"),
            ("group.id", "g"),
            ("group.instance.id", "static-1"),
            ("group.protocol", "consumer"),
            ("group.remote.assignor", "uniform"),
            ("auto.offset.reset", "earliest"),
            ("isolation.level", "read_committed"),
            ("enable.auto.commit", "true"),
            ("partition.assignment.strategy", "roundrobin"),
            ("retry.backoff.ms", "250"),
            ("retry.backoff.max.ms", "2000"),
        ]
        .into_iter()
        .collect();
        let consumer = client.consumer_config().expect("valid consumer config");
        let runtime = ConsumerRuntimeConfig::from_config(&consumer).expect("runtime");

        assert_eq!(runtime.group_id, "g");
        assert_eq!(runtime.group_instance_id, "static-1");
        assert_eq!(runtime.group_protocol, GroupProtocol::Consumer);
        assert_eq!(runtime.group_remote_assignor.as_deref(), Some("uniform"));
        assert_eq!(runtime.auto_offset_reset, AutoOffsetReset::Earliest);
        assert_eq!(runtime.isolation_level, IsolationLevel::ReadCommitted);
        assert!(runtime.enable_auto_commit);
        assert_eq!(
            runtime.partition_assignment_strategy,
            vec!["roundrobin".to_owned()]
        );
        assert_eq!(runtime.retry_backoff, Duration::from_millis(250));
        assert_eq!(runtime.retry_backoff_max, Duration::from_secs(2));

        // An empty remote assignor maps to `None`; retry backoff takes the
        // Kafka defaults (100 ms doubling up to 1 s).
        let plain: ClientConfig = [("bootstrap.servers", "127.0.0.1:9092"), ("group.id", "g")]
            .into_iter()
            .collect();
        let plain =
            ConsumerRuntimeConfig::from_config(&plain.consumer_config().expect("valid config"))
                .expect("runtime");
        assert_eq!(plain.group_protocol, GroupProtocol::Classic);
        assert_eq!(plain.group_remote_assignor, None);
        assert_eq!(plain.retry_backoff, Duration::from_millis(100));
        assert_eq!(plain.retry_backoff_max, Duration::from_secs(1));
    }

    #[test]
    fn group_protocol_parses_classic_and_consumer() {
        assert_eq!(
            GroupProtocol::parse("classic").unwrap(),
            GroupProtocol::Classic
        );
        assert_eq!(
            GroupProtocol::parse(" Consumer ").unwrap(),
            GroupProtocol::Consumer
        );
        assert!(GroupProtocol::parse("streams").is_err());
    }

    #[test]
    fn isolation_level_parses_and_maps_to_wire() {
        assert_eq!(IsolationLevel::parse("read_uncommitted").unwrap().wire(), 0);
        assert_eq!(IsolationLevel::parse("READ_COMMITTED").unwrap().wire(), 1);
        assert!(IsolationLevel::parse("dirty").is_err());
    }
}
