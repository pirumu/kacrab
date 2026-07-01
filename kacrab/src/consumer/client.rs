//! The public [`Consumer`] facade and its `poll` loop.
//!
//! Supports both manual assignment (`assign`) and group `subscribe`, with
//! position control (`seek`, `pause`, `resume`), `auto.offset.reset`, offset
//! commit/fetch (`commit_sync`/`committed`), and classic eager group membership
//! (join/sync/heartbeat + the `range` assignor). The consumer is single-owner
//! and not `Sync`; `poll` drives all fetch and coordination I/O — including
//! poll-throttled heartbeats — mirroring the Java consumer's thread model. A
//! dedicated background heartbeat task is a later refinement.

use std::{
    collections::{BTreeSet, HashMap},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use bytes::Bytes;
use kacrab_protocol::generated::ErrorCode;

use super::{
    assignor::{self, MemberSubscription},
    config::{AutoOffsetReset, ConsumerRuntimeConfig},
    coordinator,
    error::{ConsumerError, Result},
    fetch,
    offsets::{self, EARLIEST_TIMESTAMP, LATEST_TIMESTAMP},
    record::ConsumerRecords,
    subscription::{FetchPosition, SubscriptionState},
};
use crate::{
    common::{ConsumerGroupMetadata, OffsetAndMetadata, TopicPartition},
    config::{ClientConfig, ConfigKey, ConfigValue, ConsumerConfig, Properties},
    wire::{BrokerEndpoint, ClusterMetadata, WireClient, WireError},
};

/// A native, Java-compatible Kafka consumer.
///
/// See `docs/consumer-design.md`. Supports manual assignment and classic group
/// subscription (eager rebalancing with the `range` assignor), fetching, and
/// offset commit/fetch.
#[derive(Debug)]
pub struct Consumer {
    wire: WireClient,
    config: ConsumerRuntimeConfig,
    subscription: SubscriptionState,
    wakeup: Arc<AtomicBool>,
    coordinator_id: Option<i32>,
    /// Topics this consumer subscribed to; empty means manual-assignment mode.
    subscribed_topics: Vec<String>,
    /// Group member id once joined (empty before the first `JoinGroup`).
    member_id: String,
    /// Current group generation, or `-1` when not a member.
    generation_id: i32,
    /// Whether a (re)join is needed before the next fetch.
    needs_rejoin: bool,
    /// When the last heartbeat was sent (throttles poll-driven heartbeats).
    last_heartbeat: Option<Instant>,
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
            coordinator_id: None,
            subscribed_topics: Vec::new(),
            member_id: String::new(),
            generation_id: -1,
            needs_rejoin: false,
            last_heartbeat: None,
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

    /// Subscribe to a set of topics, joining the consumer group on the next
    /// [`poll`](Consumer::poll). Replaces any prior subscription and clears a
    /// manual assignment.
    ///
    /// # Errors
    /// Returns [`ConsumerError::InvalidState`] if `group.id` is unset.
    pub fn subscribe(&mut self, topics: impl IntoIterator<Item = impl Into<String>>) -> Result<()> {
        if self.config.group_id.is_empty() {
            return Err(ConsumerError::InvalidState(
                "group.id must be set to subscribe to topics",
            ));
        }
        let mut topics: Vec<String> = topics.into_iter().map(Into::into).collect();
        topics.sort();
        topics.dedup();
        self.subscribed_topics = topics;
        self.subscription.assign(&[]);
        self.member_id.clear();
        self.generation_id = -1;
        self.needs_rejoin = true;
        self.last_heartbeat = None;
        Ok(())
    }

    /// Unsubscribe from all topics and drop the current assignment. Does not send
    /// a `LeaveGroup`; call [`close`](Consumer::close) to leave the group.
    pub fn unsubscribe(&mut self) {
        self.subscribed_topics.clear();
        self.subscription.assign(&[]);
        self.member_id.clear();
        self.generation_id = -1;
        self.needs_rejoin = false;
        self.last_heartbeat = None;
    }

    /// The topics this consumer is subscribed to (empty in manual-assignment
    /// mode).
    #[must_use]
    pub fn subscription(&self) -> Vec<String> {
        self.subscribed_topics.clone()
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

    /// Commit the current fetch position of every assigned partition to the group
    /// coordinator, blocking until the broker acknowledges.
    ///
    /// # Errors
    /// Returns [`ConsumerError::InvalidState`] if `group.id` is unset, or a
    /// wire/broker error.
    pub async fn commit_sync(&mut self) -> Result<()> {
        let offsets = self.current_position_offsets();
        self.commit_sync_offsets(offsets).await
    }

    /// Commit explicit offsets to the group coordinator, blocking until the
    /// broker acknowledges.
    ///
    /// # Errors
    /// Returns [`ConsumerError::InvalidState`] if `group.id` is unset, or a
    /// wire/broker error.
    pub async fn commit_sync_offsets(
        &mut self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
    ) -> Result<()> {
        if offsets.is_empty() {
            return Ok(());
        }
        let group_id = self.require_group_id()?;
        let coordinator = self.ensure_coordinator(&group_id).await?;
        coordinator::commit_offsets(&self.wire, coordinator, &group_id, &offsets).await
    }

    /// Fetch the last committed offset for each of the given partitions.
    /// Partitions with no committed offset are omitted from the result.
    ///
    /// # Errors
    /// Returns [`ConsumerError::InvalidState`] if `group.id` is unset, or a
    /// wire/broker error.
    pub async fn committed(
        &mut self,
        partitions: &[TopicPartition],
    ) -> Result<HashMap<TopicPartition, OffsetAndMetadata>> {
        if partitions.is_empty() {
            return Ok(HashMap::new());
        }
        let group_id = self.require_group_id()?;
        let coordinator = self.ensure_coordinator(&group_id).await?;
        coordinator::fetch_committed(&self.wire, coordinator, &group_id, partitions).await
    }

    /// This consumer's group metadata. For a manual-assignment consumer the
    /// generation is `-1` and the member id is empty.
    #[must_use]
    pub fn group_metadata(&self) -> ConsumerGroupMetadata {
        ConsumerGroupMetadata::new(self.config.group_id.clone())
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
            // Group members join/sync and heartbeat before fetching; manual
            // assignment skips all of this.
            if self.is_subscribed() {
                self.ensure_active_group().await?;
                self.maybe_heartbeat().await?;
            }

            let topics = self.assigned_topics();
            let mut fetchable_empty = true;
            if !topics.is_empty() {
                let metadata = self.wire.metadata_for_topics(topics).await?;
                self.reset_positions(&metadata).await?;

                let fetchable = self.subscription.fetchable_partitions();
                fetchable_empty = fetchable.is_empty();
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
            } else if !self.is_subscribed() {
                // Manual assignment with nothing assigned — nothing to do.
                return Ok(ConsumerRecords::empty());
            }

            self.check_wakeup()?;
            if start.elapsed() >= timeout {
                return Ok(ConsumerRecords::empty());
            }
            // Nothing fetchable this round (leaders unresolved, or subscribed but
            // not yet assigned) — back off briefly so we don't spin.
            if fetchable_empty {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }

    /// Interrupt a blocking [`Consumer::poll`] on this consumer. The next (or
    /// in-flight) poll returns [`ConsumerError::Wakeup`].
    pub fn wakeup(&self) {
        self.wakeup.store(true, Ordering::SeqCst);
    }

    /// Close the consumer: leave the group (best effort) and release its broker
    /// connections. The wire client shuts its broker tasks down on drop.
    pub async fn close(self) {
        if let (false, Some(coordinator)) = (self.member_id.is_empty(), self.coordinator_id) {
            coordinator::leave_group(
                &self.wire,
                coordinator,
                &self.config.group_id,
                &self.member_id,
            )
            .await;
        }
        drop(self);
    }

    /// The current fetch position of each assigned, positioned partition as an
    /// [`OffsetAndMetadata`] ready to commit.
    fn current_position_offsets(&self) -> HashMap<TopicPartition, OffsetAndMetadata> {
        self.subscription
            .assigned_partitions()
            .into_iter()
            .filter_map(|partition| {
                self.subscription.position(&partition).map(|position| {
                    let mut offset = OffsetAndMetadata::new(position.offset);
                    if let Some(leader_epoch) = position.leader_epoch {
                        offset = offset.leader_epoch(leader_epoch);
                    }
                    (partition, offset)
                })
            })
            .collect()
    }

    fn require_group_id(&self) -> Result<String> {
        if self.config.group_id.is_empty() {
            Err(ConsumerError::InvalidState(
                "group.id must be set to commit or fetch committed offsets",
            ))
        } else {
            Ok(self.config.group_id.clone())
        }
    }

    async fn ensure_coordinator(&mut self, group_id: &str) -> Result<i32> {
        if let Some(id) = self.coordinator_id {
            return Ok(id);
        }
        let id = coordinator::find_coordinator(&self.wire, group_id).await?;
        self.coordinator_id = Some(id);
        Ok(id)
    }

    const fn is_subscribed(&self) -> bool {
        !self.subscribed_topics.is_empty()
    }

    /// (Re)join the group and sync an assignment when needed, then resume each
    /// assigned partition from its committed offset.
    async fn ensure_active_group(&mut self) -> Result<()> {
        if !self.needs_rejoin && !self.member_id.is_empty() {
            return Ok(());
        }
        let group_id = self.config.group_id.clone();
        let coordinator = self.ensure_coordinator(&group_id).await?;
        let join = coordinator::join_group(
            &coordinator::GroupContext {
                wire: &self.wire,
                coordinator_id: coordinator,
                group_id: &group_id,
            },
            &self.member_id,
            clamp_ms(self.config.session_timeout),
            clamp_ms(self.config.rebalance_timeout),
            &self.subscribed_topics,
        )
        .await?;
        self.member_id.clone_from(&join.member_id);
        self.generation_id = join.generation_id;

        let assignments = if join.leader {
            self.compute_assignments(&join.members).await?
        } else {
            Vec::new()
        };
        let context = coordinator::GroupContext {
            wire: &self.wire,
            coordinator_id: coordinator,
            group_id: &group_id,
        };
        let assigned =
            coordinator::sync_group(&context, self.generation_id, &self.member_id, assignments)
                .await?;

        self.subscription.assign(&assigned);
        let committed =
            coordinator::fetch_committed(&self.wire, coordinator, &group_id, &assigned).await?;
        for (partition, offset) in committed {
            self.subscription.set_position(
                &partition,
                FetchPosition::new(offset.offset, offset.leader_epoch),
            );
        }
        self.needs_rejoin = false;
        self.last_heartbeat = Some(Instant::now());
        Ok(())
    }

    /// Leader-only: run the `range` assignor over the members' subscriptions,
    /// using cluster metadata for partition counts, and encode each member's
    /// assignment blob.
    async fn compute_assignments(
        &self,
        members: &[MemberSubscription],
    ) -> Result<Vec<(String, Bytes)>> {
        let mut topics: BTreeSet<String> = BTreeSet::new();
        for member in members {
            for topic in &member.topics {
                let _inserted = topics.insert(topic.clone());
            }
        }
        let metadata = self
            .wire
            .metadata_for_topics(topics.iter().cloned())
            .await?;
        let mut partitions_per_topic: HashMap<String, i32> = HashMap::new();
        for topic in &topics {
            let count = metadata.topic(topic).map_or(0, |topic| {
                i32::try_from(topic.partitions.len()).unwrap_or(i32::MAX)
            });
            let _previous = partitions_per_topic.insert(topic.clone(), count);
        }
        let assignment = assignor::range_assign(members, &partitions_per_topic);
        Ok(assignment
            .into_iter()
            .map(|(member, partitions)| (member, assignor::encode_assignment(&partitions)))
            .collect())
    }

    /// Send a throttled heartbeat, flagging a rejoin on the coordinator's signal.
    async fn maybe_heartbeat(&mut self) -> Result<()> {
        if self.member_id.is_empty() || self.needs_rejoin {
            return Ok(());
        }
        let due = self
            .last_heartbeat
            .is_none_or(|last| last.elapsed() >= self.config.heartbeat_interval);
        if !due {
            return Ok(());
        }
        let Some(coordinator) = self.coordinator_id else {
            return Ok(());
        };
        let group_id = self.config.group_id.clone();
        let code = coordinator::heartbeat(
            &self.wire,
            coordinator,
            &group_id,
            self.generation_id,
            &self.member_id,
        )
        .await?;
        self.last_heartbeat = Some(Instant::now());
        match code {
            ErrorCode::None => {},
            ErrorCode::RebalanceInProgress => self.needs_rejoin = true,
            ErrorCode::IllegalGeneration | ErrorCode::UnknownMemberId => {
                self.needs_rejoin = true;
                self.member_id.clear();
                self.generation_id = -1;
            },
            other if other.is_error() => {
                return Err(ConsumerError::broker(
                    "heartbeat",
                    other,
                    "heartbeat failed",
                ));
            },
            _ => {},
        }
        Ok(())
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

/// Clamp a duration to a millisecond `i32` for wire timeout fields.
fn clamp_ms(duration: Duration) -> i32 {
    i32::try_from(duration.as_millis()).unwrap_or(i32::MAX)
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
