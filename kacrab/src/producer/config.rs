//! Runtime producer configuration derived from typed Kafka producer config.

use std::time::Duration;

use kacrab_protocol::compression::Compression;

use super::{
    accumulator::AccumulatorConfig,
    error::{ProducerError, Result},
};

/// Kafka value for `acks=all`; represented on the wire as `-1`.
pub(super) const ACKS_ALL: i16 = -1;
/// Kafka value for `acks=0`; records are fire-and-forget.
pub(super) const ACKS_NONE: i16 = 0;
/// Kafka value for `acks=1`; the leader acknowledges after local append.
pub(super) const ACKS_LEADER: i16 = 1;
/// Kafka default `request.timeout.ms` for producer requests.
pub(super) const DEFAULT_TIMEOUT_MS: i32 = 30_000;
/// Kafka default `delivery.timeout.ms`; this is the outer delivery bound that
/// must remain larger than request timeout plus linger/retry time.
pub(super) const DEFAULT_DELIVERY_TIMEOUT: Duration = Duration::from_mins(2);
/// Kafka's effective default retries is `Integer.MAX_VALUE`; delivery timeout
/// remains the real upper bound.
pub(super) const DEFAULT_RETRY_ATTEMPTS: usize = usize::MAX;
/// Kafka default `retry.backoff.ms`.
pub(super) const DEFAULT_RETRY_BACKOFF: Duration = Duration::from_millis(100);
/// Kafka default `retry.backoff.max.ms`.
pub(super) const DEFAULT_RETRY_BACKOFF_MAX: Duration = Duration::from_secs(1);
/// Kafka idempotent-producer limit. Values above 5 can break ordering on retry.
pub(super) const IDEMPOTENT_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION: i32 = 5;
/// Kafka default `max.request.size`: 1 MiB.
pub(super) const DEFAULT_MAX_REQUEST_SIZE: usize = 1_048_576;
/// Default transaction timeout sent to `InitProducerId`; matches Kafka's
/// `transaction.timeout.ms` default.
pub(super) const DEFAULT_TRANSACTION_TIMEOUT_MS: i32 = 60_000;

/// Producer batch compression selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducerCompression {
    /// Kafka record-batch compression codec.
    pub codec: Compression,
    /// Optional codec-specific compression level.
    pub level: Option<i32>,
}

impl Default for ProducerCompression {
    fn default() -> Self {
        Self {
            codec: Compression::None,
            level: None,
        }
    }
}

/// Runtime producer knobs used by accumulator and dispatcher.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProducerRuntimeConfig {
    /// Record accumulation and backpressure settings.
    pub accumulator: AccumulatorConfig,
    /// Produce request acknowledgement mode.
    pub acks: i16,
    /// Produce request timeout in milliseconds.
    pub timeout_ms: i32,
    /// Maximum retry attempts before delivery timeout wins.
    pub retry_attempts: usize,
    /// Initial backoff before retrying retriable producer errors.
    pub retry_backoff: Duration,
    /// Maximum backoff before retrying retriable producer errors.
    pub retry_backoff_max: Duration,
    /// Upper bound for delivering an accumulated batch.
    pub delivery_timeout: Duration,
    /// Maximum time producer APIs wait for buffer memory or metadata-dependent partitioning.
    pub max_block: Duration,
    /// Maximum unacknowledged produce requests per broker connection.
    pub max_in_flight_requests_per_connection: usize,
    /// Maximum serialized record/request size accepted by the producer.
    pub max_request_size: usize,
    /// Whether Kafka client telemetry push APIs are enabled.
    pub enable_metrics_push: bool,
    /// Record-batch compression settings.
    pub compression: ProducerCompression,
    /// Whether key-based partitioning should be disabled.
    pub partitioner_ignore_keys: bool,
    /// Whether sticky partition switching uses adaptive load statistics.
    pub partitioner_adaptive_partitioning_enable: bool,
    /// How long a broker may be unable to drain before adaptive sticky excludes its partitions.
    pub partitioner_availability_timeout: Duration,
    /// Idempotent/transactional producer settings.
    pub idempotence: ProducerIdempotenceConfig,
}

/// Runtime producer idempotence and transaction configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProducerIdempotenceConfig {
    /// Whether Kafka idempotent produce semantics are enabled.
    pub enabled: bool,
    /// Optional Kafka transactional id.
    pub transactional_id: Option<String>,
    /// Transaction timeout sent to `InitProducerId`.
    pub transaction_timeout_ms: i32,
    /// Whether `InitProducerId` should request two-phase commit mode.
    pub transaction_two_phase_commit: bool,
}

impl Default for ProducerIdempotenceConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            transactional_id: None,
            transaction_timeout_ms: DEFAULT_TRANSACTION_TIMEOUT_MS,
            transaction_two_phase_commit: false,
        }
    }
}

impl Default for ProducerRuntimeConfig {
    fn default() -> Self {
        Self {
            accumulator: AccumulatorConfig::default(),
            acks: ACKS_ALL,
            timeout_ms: DEFAULT_TIMEOUT_MS,
            retry_attempts: DEFAULT_RETRY_ATTEMPTS,
            retry_backoff: DEFAULT_RETRY_BACKOFF,
            retry_backoff_max: DEFAULT_RETRY_BACKOFF_MAX,
            delivery_timeout: DEFAULT_DELIVERY_TIMEOUT,
            max_block: Duration::from_mins(1),
            max_in_flight_requests_per_connection:
                crate::wire::DEFAULT_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
            max_request_size: DEFAULT_MAX_REQUEST_SIZE,
            enable_metrics_push: true,
            compression: ProducerCompression::default(),
            partitioner_ignore_keys: false,
            partitioner_adaptive_partitioning_enable: true,
            partitioner_availability_timeout: Duration::ZERO,
            idempotence: ProducerIdempotenceConfig::default(),
        }
    }
}

impl ProducerRuntimeConfig {
    /// Build runtime producer settings from the public typed Kafka config.
    pub fn from_config(config: &crate::config::ProducerConfig) -> Result<Self> {
        validate_idempotence(config)?;
        validate_delivery_timeout(config)?;
        let max_in_flight_requests_per_connection = positive_i32_to_usize(
            config.max_in_flight_requests_per_connection,
            crate::config::ProducerConfig::MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION_CONFIG,
        )?;

        Ok(Self {
            accumulator: AccumulatorConfig {
                batch_size: byte_size_to_usize(
                    config.batch_size,
                    crate::config::ProducerConfig::BATCH_SIZE_CONFIG,
                )?,
                linger: config.linger_ms.duration(),
                buffer_memory: byte_size_to_usize(
                    config.buffer_memory,
                    crate::config::ProducerConfig::BUFFER_MEMORY_CONFIG,
                )?,
            },
            acks: parse_acks(&config.acks)?,
            timeout_ms: duration_ms_to_i32(
                config.request_timeout_ms,
                crate::config::ProducerConfig::REQUEST_TIMEOUT_MS_CONFIG,
            )?,
            retry_attempts: retries_to_usize(config.retries)?,
            retry_backoff: config.retry_backoff_ms.duration(),
            retry_backoff_max: config.retry_backoff_max_ms.duration(),
            delivery_timeout: config.delivery_timeout_ms.duration(),
            max_block: config.max_block_ms.duration(),
            max_in_flight_requests_per_connection,
            max_request_size: byte_size_to_usize(
                config.max_request_size,
                crate::config::ProducerConfig::MAX_REQUEST_SIZE_CONFIG,
            )?,
            enable_metrics_push: config.enable_metrics_push,
            compression: ProducerCompression {
                codec: parse_compression(&config.compression_type)?,
                level: compression_level(config),
            },
            partitioner_ignore_keys: config.partitioner_ignore_keys,
            partitioner_adaptive_partitioning_enable: config
                .partitioner_adaptive_partitioning_enable,
            partitioner_availability_timeout: config.partitioner_availability_timeout_ms.duration(),
            idempotence: ProducerIdempotenceConfig {
                enabled: config.enable_idempotence || !config.transactional_id.is_empty(),
                transactional_id: (!config.transactional_id.is_empty())
                    .then(|| config.transactional_id.clone()),
                transaction_timeout_ms: duration_ms_to_i32(
                    config.transaction_timeout_ms,
                    crate::config::ProducerConfig::TRANSACTION_TIMEOUT_MS_CONFIG,
                )?,
                transaction_two_phase_commit: config.transaction_two_phase_commit_enable,
            },
        })
    }
}

fn validate_delivery_timeout(config: &crate::config::ProducerConfig) -> Result<()> {
    // Kafka ProducerConfig.postProcessParsedConfig: delivery.timeout.ms must be at least
    // linger.ms + request.timeout.ms, so every record gets at least one full request
    // attempt before the delivery deadline expires.
    let delivery = config.delivery_timeout_ms.duration();
    let minimum = config
        .linger_ms
        .duration()
        .saturating_add(config.request_timeout_ms.duration());
    if delivery < minimum {
        return Err(ProducerError::InvalidConfig {
            key: crate::config::ProducerConfig::DELIVERY_TIMEOUT_MS_CONFIG,
            value: format!("{} ms", delivery.as_millis()),
        });
    }
    Ok(())
}

fn validate_idempotence(config: &crate::config::ProducerConfig) -> Result<()> {
    let idempotence_enabled = config.enable_idempotence || !config.transactional_id.is_empty();
    if !idempotence_enabled {
        return Ok(());
    }
    if parse_acks(&config.acks)? != ACKS_ALL {
        return Err(ProducerError::InvalidConfig {
            key: crate::config::ProducerConfig::ACKS_CONFIG,
            value: config.acks.clone(),
        });
    }
    if config.retries <= 0 {
        return Err(ProducerError::InvalidConfig {
            key: crate::config::ProducerConfig::RETRIES_CONFIG,
            value: config.retries.to_string(),
        });
    }
    if config.max_in_flight_requests_per_connection
        > IDEMPOTENT_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION
    {
        return Err(ProducerError::InvalidConfig {
            key: crate::config::ProducerConfig::MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION_CONFIG,
            value: config.max_in_flight_requests_per_connection.to_string(),
        });
    }
    Ok(())
}

fn parse_acks(value: &str) -> Result<i16> {
    match value {
        "all" | "-1" => Ok(ACKS_ALL),
        "0" => Ok(ACKS_NONE),
        "1" => Ok(ACKS_LEADER),
        other => Err(ProducerError::InvalidConfig {
            key: crate::config::ProducerConfig::ACKS_CONFIG,
            value: other.to_owned(),
        }),
    }
}

fn parse_compression(value: &str) -> Result<Compression> {
    match value {
        "none" => Ok(Compression::None),
        "gzip" => Ok(Compression::Gzip),
        "snappy" => Ok(Compression::Snappy),
        "lz4" => Ok(Compression::Lz4),
        "zstd" => Ok(Compression::Zstd),
        other => Err(ProducerError::InvalidConfig {
            key: crate::config::ProducerConfig::COMPRESSION_TYPE_CONFIG,
            value: other.to_owned(),
        }),
    }
}

fn compression_level(config: &crate::config::ProducerConfig) -> Option<i32> {
    match config.compression_type.as_str() {
        "gzip" => Some(config.compression_gzip_level),
        "lz4" => Some(config.compression_lz4_level),
        "zstd" => Some(config.compression_zstd_level),
        _ => None,
    }
}

fn retries_to_usize(value: i32) -> Result<usize> {
    usize::try_from(value).map_err(|_error| ProducerError::InvalidConfig {
        key: crate::config::ProducerConfig::RETRIES_CONFIG,
        value: value.to_string(),
    })
}

fn positive_i32_to_usize(value: i32, key: &'static str) -> Result<usize> {
    usize::try_from(value)
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| ProducerError::InvalidConfig {
            key,
            value: value.to_string(),
        })
}

fn duration_ms_to_i32(value: crate::config::DurationMs, key: &'static str) -> Result<i32> {
    i32::try_from(value.as_millis()).map_err(|_error| ProducerError::InvalidConfig {
        key,
        value: value.as_millis().to_string(),
    })
}

fn byte_size_to_usize(value: crate::config::ByteSize, key: &'static str) -> Result<usize> {
    usize::try_from(value.get()).map_err(|_error| ProducerError::InvalidConfig {
        key,
        value: value.get().to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::{
        ACKS_ALL, ACKS_LEADER, ACKS_NONE, DEFAULT_DELIVERY_TIMEOUT, DEFAULT_RETRY_BACKOFF,
        DEFAULT_RETRY_BACKOFF_MAX, DEFAULT_TIMEOUT_MS, DEFAULT_TRANSACTION_TIMEOUT_MS,
        ProducerCompression, ProducerIdempotenceConfig, ProducerRuntimeConfig, byte_size_to_usize,
        duration_ms_to_i32, parse_acks, parse_compression, positive_i32_to_usize, retries_to_usize,
    };
    use crate::{
        config::{ByteSize, DurationMs, ProducerConfig},
        producer::ProducerError,
    };

    #[test]
    fn defaults_match_kafka_runtime_baseline() {
        let runtime = ProducerRuntimeConfig::default();
        let idempotence = ProducerIdempotenceConfig::default();

        assert_eq!(runtime.acks, ACKS_ALL);
        assert_eq!(runtime.timeout_ms, DEFAULT_TIMEOUT_MS);
        assert_eq!(runtime.retry_backoff, DEFAULT_RETRY_BACKOFF);
        assert_eq!(runtime.retry_backoff_max, DEFAULT_RETRY_BACKOFF_MAX);
        assert_eq!(runtime.delivery_timeout, DEFAULT_DELIVERY_TIMEOUT);
        assert_eq!(
            idempotence.transaction_timeout_ms,
            DEFAULT_TRANSACTION_TIMEOUT_MS
        );
        assert_eq!(ProducerCompression::default().level, None);
    }

    #[test]
    fn parser_accepts_supported_acks_and_compressions() {
        assert_eq!(parse_acks("all").expect("acks all"), ACKS_ALL);
        assert_eq!(parse_acks("-1").expect("acks -1"), ACKS_ALL);
        assert_eq!(parse_acks("0").expect("acks 0"), ACKS_NONE);
        assert_eq!(parse_acks("1").expect("acks 1"), ACKS_LEADER);

        assert!(parse_compression("none").is_ok());
        assert!(parse_compression("gzip").is_ok());
        assert!(parse_compression("snappy").is_ok());
        assert!(parse_compression("lz4").is_ok());
        assert!(parse_compression("zstd").is_ok());
    }

    #[test]
    fn parser_rejects_invalid_acks_and_compression() {
        let acks = parse_acks("2").expect_err("invalid acks should fail");
        let compression = parse_compression("brotli").expect_err("invalid compression should fail");

        assert!(matches!(
            acks,
            ProducerError::InvalidConfig {
                key: ProducerConfig::ACKS_CONFIG,
                ..
            }
        ));
        assert!(matches!(
            compression,
            ProducerError::InvalidConfig {
                key: ProducerConfig::COMPRESSION_TYPE_CONFIG,
                ..
            }
        ));
    }

    #[test]
    fn numeric_conversion_helpers_reject_invalid_values() {
        assert_eq!(retries_to_usize(0).expect("zero retries"), 0);
        assert!(retries_to_usize(-1).is_err());
        assert!(positive_i32_to_usize(0, "x").is_err());
        assert!(positive_i32_to_usize(-1, "x").is_err());
        assert_eq!(
            duration_ms_to_i32(DurationMs::from_millis(30_000), "x").expect("duration"),
            30_000
        );
        assert_eq!(
            byte_size_to_usize(ByteSize::new(16_384), "x").expect("byte size"),
            16_384
        );
    }

    #[test]
    fn numeric_conversion_helpers_report_overflow_values() {
        let overflow_millis = u64::try_from(i32::MAX)
            .expect("i32 max fits u64")
            .saturating_add(1);
        let duration = duration_ms_to_i32(DurationMs::from_millis(overflow_millis), "duration.ms")
            .expect_err("duration overflow");

        assert!(matches!(
            duration,
            ProducerError::InvalidConfig {
                key: "duration.ms",
                ..
            }
        ));

        if usize::try_from(u64::MAX).is_err() {
            let bytes = byte_size_to_usize(ByteSize::new(u64::MAX), "bytes")
                .expect_err("byte size overflow");
            assert!(matches!(
                bytes,
                ProducerError::InvalidConfig { key: "bytes", .. }
            ));
        }
    }

    #[test]
    fn idempotence_validation_rejects_non_positive_retries_and_too_many_in_flight() {
        let invalid_retries = ProducerConfig::builder()
            .bootstrap_servers("localhost:9092")
            .enable_idempotence(true)
            .retries(0)
            .build()
            .expect("producer config");
        assert!(matches!(
            ProducerRuntimeConfig::from_config(&invalid_retries),
            Err(ProducerError::InvalidConfig {
                key: ProducerConfig::RETRIES_CONFIG,
                ..
            })
        ));

        let invalid_in_flight = ProducerConfig::builder()
            .bootstrap_servers("localhost:9092")
            .enable_idempotence(true)
            .max_in_flight_requests_per_connection(6)
            .build()
            .expect("producer config");
        assert!(matches!(
            ProducerRuntimeConfig::from_config(&invalid_in_flight),
            Err(ProducerError::InvalidConfig {
                key: ProducerConfig::MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION_CONFIG,
                ..
            })
        ));
    }

    #[test]
    fn validation_rejects_delivery_timeout_below_linger_plus_request_timeout() {
        let invalid = ProducerConfig::builder()
            .bootstrap_servers("localhost:9092")
            .linger_ms(DurationMs::from_millis(100))
            .request_timeout_ms(DurationMs::from_millis(30_000))
            .delivery_timeout_ms(DurationMs::from_millis(1_000))
            .build()
            .expect("producer config");
        assert!(matches!(
            ProducerRuntimeConfig::from_config(&invalid),
            Err(ProducerError::InvalidConfig {
                key: ProducerConfig::DELIVERY_TIMEOUT_MS_CONFIG,
                ..
            })
        ));
    }

    #[test]
    fn runtime_config_disables_idempotence_when_requested() {
        let config = ProducerConfig::builder()
            .bootstrap_servers("localhost:9092")
            .enable_idempotence(false)
            .acks("1")
            .retries(0)
            .build()
            .expect("producer config");

        let runtime = ProducerRuntimeConfig::from_config(&config).expect("runtime config");

        assert!(!runtime.idempotence.enabled);
        assert_eq!(runtime.acks, ACKS_LEADER);
        assert_eq!(runtime.retry_attempts, 0);
    }

    #[test]
    fn runtime_config_maps_retry_backoff_settings() {
        let config = ProducerConfig::builder()
            .bootstrap_servers("localhost:9092")
            .retry_backoff_ms(DurationMs::from_millis(17))
            .retry_backoff_max_ms(DurationMs::from_millis(71))
            .build()
            .expect("producer config");

        let runtime = ProducerRuntimeConfig::from_config(&config).expect("runtime config");

        assert_eq!(runtime.retry_backoff, std::time::Duration::from_millis(17));
        assert_eq!(
            runtime.retry_backoff_max,
            std::time::Duration::from_millis(71)
        );
    }
}
