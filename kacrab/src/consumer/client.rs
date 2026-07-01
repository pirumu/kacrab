//! The public [`Consumer`] facade and its `poll` loop.
//!
//! A manual-assignment consumer: `assign` + `poll` + position control
//! (`seek`, `pause`, `resume`), with `auto.offset.reset` resolved lazily on the
//! first poll. Group subscription, commits, and coordination arrive in Phase 2.
//! The consumer is single-owner and not `Sync`; `poll` drives all fetch I/O,
//! mirroring the Java consumer's thread model.

use std::{
    collections::BTreeSet,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use super::{
    config::{AutoOffsetReset, ConsumerRuntimeConfig},
    error::{ConsumerError, Result},
    fetch,
    offsets::{self, EARLIEST_TIMESTAMP, LATEST_TIMESTAMP},
    record::ConsumerRecords,
    subscription::{FetchPosition, SubscriptionState},
};
use crate::{
    common::TopicPartition,
    config::{ClientConfig, ConfigKey, ConfigValue, ConsumerConfig, Properties},
    wire::{BrokerEndpoint, ClusterMetadata, WireClient, WireError},
};

/// A native, Java-compatible Kafka consumer.
///
/// See `docs/consumer-design.md`. Phase 1 supports manual partition assignment
/// and fetching; group management is not yet wired.
#[derive(Debug)]
pub struct Consumer {
    wire: WireClient,
    config: ConsumerRuntimeConfig,
    subscription: SubscriptionState,
    wakeup: Arc<AtomicBool>,
}

impl Consumer {
    /// Build a consumer from public typed Kafka config.
    ///
    /// # Errors
    /// Returns an error when runtime config validation fails, bootstrap DNS
    /// resolution fails, or no bootstrap endpoint resolves to a socket address.
    pub async fn from_config(config: ConsumerConfig) -> Result<Self> {
        let runtime = ConsumerRuntimeConfig::from_config(&config)?;
        let endpoints = resolve_bootstrap_brokers(&config).await?;
        let connection = config.to_connection_config();
        let wire =
            WireClient::connect_with_brokers(connection, config.client_id.clone(), endpoints);
        Ok(Self {
            wire,
            subscription: SubscriptionState::new(runtime.auto_offset_reset),
            config: runtime,
            wakeup: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Build a consumer from an owned Kafka [`ClientConfig`].
    ///
    /// # Errors
    /// Returns an error when config validation, DNS resolution, or connection
    /// setup fails.
    pub async fn new(config: ClientConfig) -> Result<Self> {
        Self::from_client_config(&config).await
    }

    /// Build a consumer from a borrowed Kafka [`ClientConfig`].
    ///
    /// # Errors
    /// Returns an error when config validation, DNS resolution, or connection
    /// setup fails.
    pub async fn from_client_config(config: &ClientConfig) -> Result<Self> {
        let config = config
            .consumer_config()
            .map_err(|error| ConsumerError::Config { error })?;
        Self::from_config(config).await
    }

    /// Build a consumer from `Properties`-style entries.
    ///
    /// # Errors
    /// Returns an error when config validation, DNS resolution, or connection
    /// setup fails.
    pub async fn from_properties(properties: Properties) -> Result<Self> {
        Self::from_client_config(&ClientConfig::from(properties)).await
    }

    /// Build a consumer from a map/iterator of Kafka config entries.
    ///
    /// # Errors
    /// Returns an error when config validation, DNS resolution, or connection
    /// setup fails.
    pub async fn from_map<I, K, V>(entries: I) -> Result<Self>
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<ConfigKey>,
        V: Into<ConfigValue>,
    {
        let config: ClientConfig = entries.into_iter().collect();
        Self::from_client_config(&config).await
    }

    /// Manually assign a set of partitions to this consumer, replacing any prior
    /// assignment. This is the Phase 1 path (no group coordination).
    pub fn assign(&mut self, partitions: impl IntoIterator<Item = TopicPartition>) {
        let partitions: Vec<TopicPartition> = partitions.into_iter().collect();
        self.subscription.assign(&partitions);
    }

    /// The partitions currently assigned to this consumer.
    #[must_use]
    pub fn assignment(&self) -> Vec<TopicPartition> {
        self.subscription.assigned_partitions()
    }

    /// Override the fetch position of a partition to an absolute offset.
    ///
    /// # Errors
    /// Returns [`ConsumerError::PartitionNotAssigned`] if the partition is not
    /// currently assigned.
    pub fn seek(&mut self, partition: &TopicPartition, offset: i64) -> Result<()> {
        self.seek_with_leader_epoch(partition, offset, None)
    }

    /// Override the fetch position of a partition, recording the leader epoch the
    /// offset was derived from.
    ///
    /// # Errors
    /// Returns [`ConsumerError::PartitionNotAssigned`] if the partition is not
    /// currently assigned.
    pub fn seek_with_leader_epoch(
        &mut self,
        partition: &TopicPartition,
        offset: i64,
        leader_epoch: Option<i32>,
    ) -> Result<()> {
        self.ensure_assigned(partition)?;
        self.subscription
            .set_position(partition, FetchPosition::new(offset, leader_epoch));
        Ok(())
    }

    /// Seek the given partitions to the earliest available offset.
    ///
    /// # Errors
    /// Returns a wire/broker error, or [`ConsumerError::PartitionNotAssigned`].
    pub async fn seek_to_beginning(&mut self, partitions: &[TopicPartition]) -> Result<()> {
        self.seek_to_timestamp(partitions, EARLIEST_TIMESTAMP).await
    }

    /// Seek the given partitions to the log end (next offset to be produced).
    ///
    /// # Errors
    /// Returns a wire/broker error, or [`ConsumerError::PartitionNotAssigned`].
    pub async fn seek_to_end(&mut self, partitions: &[TopicPartition]) -> Result<()> {
        self.seek_to_timestamp(partitions, LATEST_TIMESTAMP).await
    }

    /// The current fetch position of a partition, resolving `auto.offset.reset`
    /// if the partition has not been positioned yet.
    ///
    /// # Errors
    /// Returns [`ConsumerError::PartitionNotAssigned`], a wire/broker error, or
    /// [`ConsumerError::NoOffsetForPartition`] when reset is `none`.
    pub async fn position(&mut self, partition: &TopicPartition) -> Result<i64> {
        self.ensure_assigned(partition)?;
        if let Some(position) = self.subscription.position(partition) {
            return Ok(position.offset);
        }
        let metadata = self
            .wire
            .metadata_for_topics([partition.topic.clone()])
            .await?;
        self.reset_positions(&metadata).await?;
        self.subscription
            .position(partition)
            .map(|position| position.offset)
            .ok_or_else(|| ConsumerError::PartitionNotAssigned {
                topic: partition.topic.clone(),
                partition: partition.partition,
            })
    }

    /// Suspend fetching for the given partitions.
    pub fn pause(&mut self, partitions: &[TopicPartition]) {
        self.subscription.pause(partitions);
    }

    /// Resume fetching for the given partitions.
    pub fn resume(&mut self, partitions: &[TopicPartition]) {
        self.subscription.resume(partitions);
    }

    /// The partitions currently paused.
    #[must_use]
    pub fn paused(&self) -> Vec<TopicPartition> {
        self.subscription.paused()
    }

    /// Fetch records for the assigned partitions, blocking up to `timeout`.
    ///
    /// Returns as soon as any records are available, or an empty batch when the
    /// timeout elapses first.
    ///
    /// # Errors
    /// Returns [`ConsumerError::Wakeup`] if [`Consumer::wakeup`] was called, a
    /// wire/broker error, or [`ConsumerError::NoOffsetForPartition`] when a
    /// partition needs a reset and `auto.offset.reset=none`.
    pub async fn poll(&mut self, timeout: Duration) -> Result<ConsumerRecords> {
        self.check_wakeup()?;
        let start = Instant::now();

        loop {
            let topics = self.assigned_topics();
            if topics.is_empty() {
                return Ok(ConsumerRecords::empty());
            }
            let metadata = self.wire.metadata_for_topics(topics).await?;
            self.reset_positions(&metadata).await?;

            let fetchable = self.subscription.fetchable_partitions();
            if !fetchable.is_empty() {
                let fetched = fetch::fetch(
                    &self.wire,
                    &self.config,
                    &metadata,
                    &fetchable,
                    self.config.max_poll_records,
                )
                .await?;
                let mut records = ConsumerRecords::empty();
                for partition_fetch in fetched {
                    self.subscription.advance_position(
                        &partition_fetch.partition,
                        partition_fetch.next_offset,
                        partition_fetch.next_leader_epoch,
                    );
                    records.push_partition(
                        partition_fetch.partition.topic,
                        partition_fetch.partition.partition,
                        partition_fetch.records,
                    );
                }
                if !records.is_empty() {
                    return Ok(records);
                }
            }

            self.check_wakeup()?;
            if start.elapsed() >= timeout {
                return Ok(ConsumerRecords::empty());
            }
            // Nothing fetchable this round (e.g. leaders not yet resolved) — back
            // off briefly so we don't spin against the deadline.
            if fetchable.is_empty() {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }

    /// Interrupt a blocking [`Consumer::poll`] on this consumer. The next (or
    /// in-flight) poll returns [`ConsumerError::Wakeup`].
    pub fn wakeup(&self) {
        self.wakeup.store(true, Ordering::SeqCst);
    }

    /// Close the consumer, releasing its broker connections. The wire client
    /// shuts its broker tasks down on drop.
    pub fn close(self) {
        drop(self);
    }

    fn ensure_assigned(&self, partition: &TopicPartition) -> Result<()> {
        if self.subscription.is_assigned(partition) {
            Ok(())
        } else {
            Err(ConsumerError::PartitionNotAssigned {
                topic: partition.topic.clone(),
                partition: partition.partition,
            })
        }
    }

    fn check_wakeup(&self) -> Result<()> {
        if self.wakeup.swap(false, Ordering::SeqCst) {
            Err(ConsumerError::Wakeup)
        } else {
            Ok(())
        }
    }

    fn assigned_topics(&self) -> Vec<String> {
        self.subscription
            .assigned_partitions()
            .into_iter()
            .map(|partition| partition.topic)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    async fn reset_positions(&mut self, metadata: &ClusterMetadata) -> Result<()> {
        let need = self.subscription.partitions_needing_reset();
        if need.is_empty() {
            return Ok(());
        }
        let timestamp = match self.subscription.default_reset() {
            AutoOffsetReset::Earliest => EARLIEST_TIMESTAMP,
            AutoOffsetReset::Latest => LATEST_TIMESTAMP,
            AutoOffsetReset::None => {
                if let Some(partition) = need.first() {
                    return Err(ConsumerError::NoOffsetForPartition {
                        topic: partition.topic.clone(),
                        partition: partition.partition,
                    });
                }
                return Ok(());
            },
        };
        let entries: Vec<(TopicPartition, i64)> = need
            .into_iter()
            .map(|partition| (partition, timestamp))
            .collect();
        let resolved = offsets::list_offsets(&self.wire, &self.config, metadata, &entries).await?;
        for (partition, offset) in resolved {
            self.subscription
                .set_position(&partition, offset.into_position());
        }
        Ok(())
    }

    async fn seek_to_timestamp(
        &mut self,
        partitions: &[TopicPartition],
        timestamp: i64,
    ) -> Result<()> {
        for partition in partitions {
            self.ensure_assigned(partition)?;
        }
        if partitions.is_empty() {
            return Ok(());
        }
        let topics: Vec<String> = partitions.iter().map(|p| p.topic.clone()).collect();
        let metadata = self.wire.metadata_for_topics(topics).await?;
        let entries: Vec<(TopicPartition, i64)> =
            partitions.iter().map(|p| (p.clone(), timestamp)).collect();
        let resolved = offsets::list_offsets(&self.wire, &self.config, &metadata, &entries).await?;
        for (partition, offset) in resolved {
            self.subscription
                .set_position(&partition, offset.into_position());
        }
        Ok(())
    }
}

/// Resolve `bootstrap.servers` into wire broker endpoints.
async fn resolve_bootstrap_brokers(config: &ConsumerConfig) -> Result<Vec<BrokerEndpoint>> {
    let mut endpoints = Vec::new();
    for (index, server) in config.bootstrap_servers.as_slice().iter().enumerate() {
        let node_id = i32::try_from(index).map_err(|_error| ConsumerError::InvalidArgument {
            field: "bootstrap.servers",
            message: format!("too many bootstrap servers (entry {index})"),
        })?;
        let (host, port) = parse_bootstrap_server(server)?;
        let mut addresses = tokio::net::lookup_host((host.as_str(), port))
            .await
            .map_err(WireError::from)?;
        let addr = addresses.next();
        drop(addresses);
        if let Some(addr) = addr {
            endpoints.push(BrokerEndpoint::from_resolved(node_id, host, port, addr));
        }
    }
    if endpoints.is_empty() {
        return Err(ConsumerError::InvalidArgument {
            field: "bootstrap.servers",
            message: "no bootstrap server resolved to a socket address".to_owned(),
        });
    }
    Ok(endpoints)
}

fn parse_bootstrap_server(server: &str) -> Result<(String, u16)> {
    let (host, port) = server
        .rsplit_once(':')
        .ok_or_else(|| ConsumerError::InvalidArgument {
            field: "bootstrap.servers",
            message: format!("missing port in bootstrap server {server:?}"),
        })?;
    let port = port
        .parse::<u16>()
        .map_err(|_error| ConsumerError::InvalidArgument {
            field: "bootstrap.servers",
            message: format!("invalid port in bootstrap server {server:?}"),
        })?;
    let host = host
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(host);
    if host.is_empty() {
        return Err(ConsumerError::InvalidArgument {
            field: "bootstrap.servers",
            message: format!("missing host in bootstrap server {server:?}"),
        });
    }
    Ok((host.to_owned(), port))
}
