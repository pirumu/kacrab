//! The public [`Consumer`] facade and its `poll` loop.
//!
//! Supports both manual assignment (`assign`) and group `subscribe`, with
//! position control (`seek`, `pause`, `resume`), `auto.offset.reset`, offset
//! commit/fetch (`commit_sync`/`commit_async`/`committed`, plus background
//! auto-commit), and group membership (join/sync) with both eager assignors
//! (`range`/`roundrobin`/`sticky`) and the incremental `cooperative-sticky`
//! protocol. `poll` drives fetch and rejoin on the caller's task, while a
//! dedicated background task heartbeats the group — mirroring the Java consumer's
//! user thread + `HeartbeatThread`.

use std::{
    collections::{BTreeSet, HashMap, HashSet},
    sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use bytes::Bytes;
use kacrab_protocol::{
    KafkaUuid,
    generated::{
        ApiKey, ErrorCode, GetTelemetrySubscriptionsRequestData,
        GetTelemetrySubscriptionsResponseData,
    },
    version::client_api_info,
};
use regex::Regex;
use tokio::task::JoinHandle;

use super::{
    assignor::{self, MemberSubscription},
    config::{AutoOffsetReset, ConsumerRuntimeConfig},
    coordinator,
    error::{ConsumerError, Result},
    fetch,
    interceptor::{ConsumerInterceptor, ConsumerInterceptors, InterceptorConfigs},
    metrics::{ConsumerMetrics, ConsumerMetricsSnapshot},
    offsets::{self, EARLIEST_TIMESTAMP, LATEST_TIMESTAMP},
    record::{ConsumerRecords, OffsetAndTimestamp},
    subscription::{FetchPosition, SubscriptionState},
};
use crate::{
    common::{ConsumerGroupMetadata, OffsetAndMetadata, TopicPartition},
    config::{ClientConfig, ConfigKey, ConfigValue, ConsumerConfig, Properties},
    wire::{BrokerEndpoint, ClusterMetadata, WireClient, WireError},
};

/// A native, Java-compatible Kafka consumer.
///
/// See `docs/consumer-design.md`. Supports manual assignment and group
/// subscription (eager `range`/`roundrobin`/`sticky` and incremental
/// `cooperative-sticky` rebalancing), fetching, and offset commit/fetch.
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
    /// Whether a (re)join is needed before the next fetch; set by the background
    /// heartbeat task and cleared after a successful join.
    needs_rejoin: Arc<AtomicBool>,
    /// The group context the background heartbeat task reads (`None` until
    /// joined). Updated after each (re)join.
    heartbeat_context: Arc<Mutex<Option<HeartbeatContext>>>,
    /// The background heartbeat task, aborted on close.
    heartbeat_task: JoinHandle<()>,
    /// When the last background auto-commit ran.
    last_auto_commit: Option<Instant>,
    /// Native request/record counters (Java's `Consumer.metrics()`).
    metrics: ConsumerMetrics,
    /// User interceptors run on poll/commit (Java's `ConsumerInterceptor`s).
    interceptors: ConsumerInterceptors,
    /// Config handed to a late-added interceptor's `configure`.
    interceptor_configs: InterceptorConfigs,
    /// Topic regex when subscribed by pattern (`subscribe(Pattern)`); `None` for
    /// an explicit topic subscription or manual assignment.
    subscription_pattern: Option<Regex>,
    /// When the pattern's matched-topic set was last refreshed from metadata.
    last_pattern_refresh: Option<Instant>,
}

/// Callback invoked with the result of an asynchronous offset commit.
pub type OffsetCommitCallback = Box<dyn FnOnce(Result<()>) + Send>;

/// Max join/sync rounds in one rejoin before giving up (a rebalance can restart
/// the round when another member joins mid-sync).
const MAX_REJOIN_ATTEMPTS: u32 = 10;

/// How often a pattern subscription re-matches its regex against the cluster's
/// topic list, so newly created (or deleted) topics are picked up.
const PATTERN_REFRESH_INTERVAL: Duration = Duration::from_secs(5);

/// Whether an error is the coordinator's `REBALANCE_IN_PROGRESS` signal.
const fn is_rebalance_in_progress(error: &ConsumerError) -> bool {
    matches!(
        error,
        ConsumerError::Broker {
            error: ErrorCode::RebalanceInProgress,
            ..
        }
    )
}

/// A synced assignment to apply: what we owned before, what we now hold, and
/// whether the group is rebalancing cooperatively (incremental revoke).
#[derive(Debug, Clone, Copy)]
struct Rebalance<'a> {
    owned: &'a [TopicPartition],
    assigned: &'a [TopicPartition],
    cooperative: bool,
}

/// Group identity the background heartbeat task heartbeats with.
#[derive(Debug, Clone)]
#[expect(
    clippy::struct_field_names,
    reason = "Field names mirror the Kafka group-membership identifiers."
)]
struct HeartbeatContext {
    coordinator_id: i32,
    group_id: String,
    generation_id: i32,
    member_id: String,
    group_instance_id: Option<String>,
}

impl Drop for Consumer {
    fn drop(&mut self) {
        self.heartbeat_task.abort();
    }
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
        let interceptor_configs = InterceptorConfigs {
            client_id: (!config.client_id.is_empty()).then(|| config.client_id.clone()),
            group_id: (!runtime.group_id.is_empty()).then(|| runtime.group_id.clone()),
        };
        let connection = config.to_connection_config();
        let wire =
            WireClient::connect_with_brokers(connection, config.client_id.clone(), endpoints);
        let needs_rejoin = Arc::new(AtomicBool::new(false));
        let heartbeat_context: Arc<Mutex<Option<HeartbeatContext>>> = Arc::new(Mutex::new(None));
        let metrics = ConsumerMetrics::default();
        let heartbeat_task = tokio::spawn(heartbeat_loop(
            wire.clone(),
            Arc::clone(&heartbeat_context),
            Arc::clone(&needs_rejoin),
            runtime.heartbeat_interval,
            metrics.clone(),
        ));
        Ok(Self {
            wire,
            subscription: SubscriptionState::new(runtime.auto_offset_reset),
            config: runtime,
            wakeup: Arc::new(AtomicBool::new(false)),
            coordinator_id: None,
            subscribed_topics: Vec::new(),
            member_id: String::new(),
            generation_id: -1,
            needs_rejoin,
            heartbeat_context,
            heartbeat_task,
            last_auto_commit: None,
            metrics,
            interceptors: ConsumerInterceptors::default(),
            interceptor_configs,
            subscription_pattern: None,
            last_pattern_refresh: None,
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
        self.subscription_pattern = None;
        self.subscribed_topics = topics;
        self.subscription.assign(&[]);
        self.member_id.clear();
        self.generation_id = -1;
        self.needs_rejoin.store(true, Ordering::SeqCst);
        self.set_heartbeat_context(None);
        Ok(())
    }

    /// Subscribe to every topic whose name matches `pattern`, joining the group on
    /// the next [`poll`](Consumer::poll) and re-matching as topics come and go —
    /// the analogue of Kafka's `subscribe(Pattern)`. Internal topics are excluded
    /// unless `exclude.internal.topics=false`.
    ///
    /// # Errors
    /// Returns [`ConsumerError::InvalidState`] if `group.id` is unset, or
    /// [`ConsumerError::InvalidArgument`] if `pattern` is not a valid regex.
    pub fn subscribe_pattern(&mut self, pattern: &str) -> Result<()> {
        if self.config.group_id.is_empty() {
            return Err(ConsumerError::InvalidState(
                "group.id must be set to subscribe to a pattern",
            ));
        }
        let regex = Regex::new(pattern).map_err(|error| ConsumerError::InvalidArgument {
            field: "subscribe pattern",
            message: error.to_string(),
        })?;
        self.subscription_pattern = Some(regex);
        self.subscribed_topics.clear();
        self.subscription.assign(&[]);
        self.member_id.clear();
        self.generation_id = -1;
        self.last_pattern_refresh = None;
        // The first poll resolves the pattern to concrete topics and joins.
        self.needs_rejoin.store(true, Ordering::SeqCst);
        self.set_heartbeat_context(None);
        Ok(())
    }

    /// Unsubscribe from all topics and drop the current assignment. Does not send
    /// a `LeaveGroup`; call [`close`](Consumer::close) to leave the group.
    pub fn unsubscribe(&mut self) {
        self.subscription_pattern = None;
        self.subscribed_topics.clear();
        self.subscription.assign(&[]);
        self.member_id.clear();
        self.generation_id = -1;
        self.needs_rejoin.store(false, Ordering::SeqCst);
        self.set_heartbeat_context(None);
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
        let result =
            coordinator::commit_offsets(&self.wire, coordinator, &group_id, &offsets).await;
        if result.is_ok() {
            self.metrics.record_commit();
            self.interceptors.on_commit(&offsets);
        }
        result
    }

    /// Commit the current position of every assigned partition without blocking;
    /// `callback` is invoked with the result when the commit completes.
    ///
    /// # Errors
    /// Returns [`ConsumerError::InvalidState`] if `group.id` is unset, or a
    /// coordinator-lookup error before the commit is dispatched.
    pub async fn commit_async(&mut self, callback: OffsetCommitCallback) -> Result<()> {
        let offsets = self.current_position_offsets();
        self.commit_async_offsets(offsets, callback).await
    }

    /// Commit explicit offsets without blocking; `callback` is invoked with the
    /// result when the commit completes.
    ///
    /// # Errors
    /// Returns [`ConsumerError::InvalidState`] if `group.id` is unset, or a
    /// coordinator-lookup error before the commit is dispatched.
    pub async fn commit_async_offsets(
        &mut self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
        callback: OffsetCommitCallback,
    ) -> Result<()> {
        if offsets.is_empty() {
            callback(Ok(()));
            return Ok(());
        }
        let group_id = self.require_group_id()?;
        let coordinator = self.ensure_coordinator(&group_id).await?;
        let wire = self.wire.clone();
        let metrics = self.metrics.clone();
        let interceptors = self.interceptors.clone();
        let _handle = tokio::spawn(async move {
            let result = coordinator::commit_offsets(&wire, coordinator, &group_id, &offsets).await;
            if result.is_ok() {
                metrics.record_commit();
                interceptors.on_commit(&offsets);
            }
            callback(result);
        });
        Ok(())
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
        ConsumerGroupMetadata::from_parts(
            self.config.group_id.clone(),
            self.generation_id,
            self.member_id.clone(),
            (!self.config.group_instance_id.is_empty())
                .then(|| self.config.group_instance_id.clone()),
        )
    }

    /// Force a group rebalance on the next [`poll`](Consumer::poll): the member
    /// rejoins the group. Mirrors Kafka's `enforceRebalance`.
    pub fn enforce_rebalance(&self) {
        self.needs_rejoin.store(true, Ordering::SeqCst);
    }

    /// The broker-assigned client instance id (`GetTelemetrySubscriptions`,
    /// Kafka's `clientInstanceId`).
    ///
    /// # Errors
    /// Returns a wire/broker error, or [`ConsumerError::Wire`] with
    /// `UnsupportedApiVersion` when the broker has client telemetry disabled.
    pub async fn client_instance_id(&self) -> Result<KafkaUuid> {
        let request = GetTelemetrySubscriptionsRequestData::default();
        let broker = self.wire.any_broker_id()?;
        let version = client_api_info(ApiKey::GetTelemetrySubscriptions).max_version;
        let response: GetTelemetrySubscriptionsResponseData = self
            .wire
            .send_to_broker(broker, ApiKey::GetTelemetrySubscriptions, version, &request)
            .await?;
        let error = ErrorCode::from(response.error_code);
        if error.is_error() {
            return Err(ConsumerError::broker(
                "client_instance_id",
                error,
                "GetTelemetrySubscriptions failed",
            ));
        }
        Ok(response.client_instance_id)
    }

    /// A snapshot of this consumer's metrics — poll/record/fetch/commit/heartbeat
    /// and rebalance totals, plus the wire buffer-pool counters. kacrab's native
    /// analogue of Java's `Consumer.metrics()`.
    #[must_use]
    pub fn metrics(&self) -> ConsumerMetricsSnapshot {
        self.metrics.snapshot(self.wire.buffer_pool_stats())
    }

    /// Register a [`ConsumerInterceptor`] on this consumer. It is `configure`d
    /// immediately (with the consumer's `client.id`/`group.id`) and thereafter
    /// observes each `poll`'s records (`on_consume`) and every successful commit
    /// (`on_commit`). Mirrors Kafka's `interceptor.classes`, added programmatically.
    pub fn add_interceptor(&mut self, interceptor: impl ConsumerInterceptor) {
        self.interceptors
            .push_and_configure(interceptor, &self.interceptor_configs);
    }

    /// The earliest available offset for each partition.
    ///
    /// # Errors
    /// Returns a wire/broker error, or [`ConsumerError::Broker`] with
    /// `LEADER_NOT_AVAILABLE` when a partition has no known leader.
    pub async fn beginning_offsets(
        &self,
        partitions: &[TopicPartition],
    ) -> Result<HashMap<TopicPartition, i64>> {
        self.offsets_at_timestamp(partitions, EARLIEST_TIMESTAMP)
            .await
    }

    /// The end offset (next offset to be produced) for each partition.
    ///
    /// # Errors
    /// Returns a wire/broker error, or [`ConsumerError::Broker`] with
    /// `LEADER_NOT_AVAILABLE` when a partition has no known leader.
    pub async fn end_offsets(
        &self,
        partitions: &[TopicPartition],
    ) -> Result<HashMap<TopicPartition, i64>> {
        self.offsets_at_timestamp(partitions, LATEST_TIMESTAMP)
            .await
    }

    /// For each partition, the earliest offset whose record timestamp is at or
    /// after the requested time. Partitions with no such record are omitted.
    ///
    /// # Errors
    /// Returns a wire/broker error.
    pub async fn offsets_for_times(
        &self,
        timestamps: HashMap<TopicPartition, i64>,
    ) -> Result<HashMap<TopicPartition, OffsetAndTimestamp>> {
        let entries: Vec<(TopicPartition, i64)> = timestamps.into_iter().collect();
        let resolved = self.list_offsets_for(&entries).await?;
        Ok(resolved
            .into_iter()
            .filter(|(_, offset)| offset.offset >= 0)
            .map(|(partition, offset)| {
                (
                    partition,
                    OffsetAndTimestamp {
                        offset: offset.offset,
                        timestamp: offset.timestamp,
                        leader_epoch: offset.leader_epoch,
                    },
                )
            })
            .collect())
    }

    /// The lag (end offset minus current position) of an assigned partition, or
    /// `None` when the partition has no position yet.
    ///
    /// # Errors
    /// Returns a wire/broker error.
    pub async fn current_lag(&self, partition: &TopicPartition) -> Result<Option<i64>> {
        let Some(position) = self.subscription.position(partition) else {
            return Ok(None);
        };
        let end = self.end_offsets(std::slice::from_ref(partition)).await?;
        Ok(end
            .get(partition)
            .map(|end_offset| end_offset.saturating_sub(position.offset).max(0)))
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
        self.metrics.record_poll();
        let start = Instant::now();

        loop {
            // A pattern subscription re-matches the regex against live topics
            // before joining, so new topics are folded in.
            self.refresh_pattern_subscription().await?;
            // Group members join/sync and heartbeat before fetching; manual
            // assignment skips all of this.
            if self.is_subscribed() {
                self.ensure_active_group().await?;
            }
            self.maybe_auto_commit().await;

            let topics = self.assigned_topics();
            let mut fetchable_empty = true;
            if !topics.is_empty() {
                let metadata = self.wire.metadata_for_topics(topics).await?;
                self.reset_positions(&metadata).await?;

                let fetchable = self.subscription.fetchable_partitions();
                fetchable_empty = fetchable.is_empty();
                if !fetchable.is_empty() {
                    self.metrics.record_fetch();
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
                        // Interceptors may rewrite or filter the batch before it
                        // reaches the caller (Kafka `onConsume`).
                        let records = self.interceptors.on_consume(records);
                        self.metrics.record_records(records.count());
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
    pub async fn close(mut self) {
        self.heartbeat_task.abort();
        let _committed = self.auto_commit_now().await;
        self.interceptors.close();
        // Static members (`group.instance.id`) stay in the group across a close so
        // a quick restart avoids a rebalance, matching Java.
        let dynamic_member = self.config.group_instance_id.is_empty();
        if let (true, false, Some(coordinator)) = (
            dynamic_member,
            self.member_id.is_empty(),
            self.coordinator_id,
        ) {
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

    async fn offsets_at_timestamp(
        &self,
        partitions: &[TopicPartition],
        timestamp: i64,
    ) -> Result<HashMap<TopicPartition, i64>> {
        let entries: Vec<(TopicPartition, i64)> = partitions
            .iter()
            .map(|partition| (partition.clone(), timestamp))
            .collect();
        let resolved = self.list_offsets_for(&entries).await?;
        Ok(resolved
            .into_iter()
            .map(|(partition, offset)| (partition, offset.offset))
            .collect())
    }

    async fn list_offsets_for(
        &self,
        entries: &[(TopicPartition, i64)],
    ) -> Result<HashMap<TopicPartition, offsets::ResolvedOffset>> {
        if entries.is_empty() {
            return Ok(HashMap::new());
        }
        let topics: BTreeSet<String> = entries
            .iter()
            .map(|(partition, _)| partition.topic.clone())
            .collect();
        let metadata = self.wire.metadata_for_topics(topics).await?;
        offsets::list_offsets(&self.wire, &self.config, &metadata, entries).await
    }

    const fn is_subscribed(&self) -> bool {
        !self.subscribed_topics.is_empty()
    }

    /// Re-resolve a pattern subscription against the cluster's current topic list
    /// (throttled to [`PATTERN_REFRESH_INTERVAL`]). When the matched set changes,
    /// swap in the new topics and trigger a rejoin. No-op unless subscribed by
    /// pattern.
    async fn refresh_pattern_subscription(&mut self) -> Result<()> {
        let due = self.subscription_pattern.is_some()
            && self
                .last_pattern_refresh
                .is_none_or(|last| last.elapsed() >= PATTERN_REFRESH_INTERVAL);
        if !due {
            return Ok(());
        }
        // Cheap clone (compiled program is shared) so no borrow is held across
        // the metadata fetch and the assignment mutation below.
        let Some(pattern) = self.subscription_pattern.clone() else {
            return Ok(());
        };
        self.last_pattern_refresh = Some(Instant::now());
        let exclude_internal = self.config.exclude_internal_topics;
        let metadata = self.wire.admin_metadata(None).await?;
        let mut matched: Vec<String> = metadata
            .topics
            .iter()
            .filter(|topic| !(exclude_internal && topic.is_internal))
            .filter(|topic| pattern.is_match(&topic.name))
            .map(|topic| topic.name.clone())
            .collect();
        matched.sort();
        matched.dedup();
        if matched != self.subscribed_topics {
            self.subscribed_topics = matched;
            self.needs_rejoin.store(true, Ordering::SeqCst);
        }
        Ok(())
    }

    /// (Re)join the group and sync an assignment when needed, then resume each
    /// assigned partition from its committed offset.
    async fn ensure_active_group(&mut self) -> Result<()> {
        if !self.needs_rejoin.load(Ordering::SeqCst) && !self.member_id.is_empty() {
            return Ok(());
        }
        // Pause the heartbeat task while we rejoin (stale generation would fence).
        self.set_heartbeat_context(None);
        // Commit the current positions before the assignment is revoked.
        let _committed = self.auto_commit_now().await;
        let group_id = self.config.group_id.clone();
        let group_instance_id = self.config.group_instance_id.clone();
        let instance = (!group_instance_id.is_empty()).then_some(group_instance_id.as_str());
        let coordinator = self.ensure_coordinator(&group_id).await?;
        let context = coordinator::GroupContext {
            wire: &self.wire,
            coordinator_id: coordinator,
            group_id: &group_id,
            group_instance_id: instance,
        };
        let assignors = self.advertised_assignors();
        // Partitions we currently own — reported to the coordinator so the
        // cooperative assignor knows what not to hand to someone else yet. Eager
        // assignors ignore it. Captured before the loop: it does not change until
        // we apply the new assignment below.
        let owned = self.subscription.assigned_partitions();
        // A rebalance that starts between our JoinGroup and SyncGroup makes the
        // coordinator answer REBALANCE_IN_PROGRESS; rejoin and retry.
        let mut attempts = 0_u32;
        let (assigned, cooperative) = loop {
            attempts = attempts.saturating_add(1);
            let join = coordinator::join_group(
                &context,
                &coordinator::JoinRequest {
                    member_id: &self.member_id,
                    session_timeout_ms: clamp_ms(self.config.session_timeout),
                    rebalance_timeout_ms: clamp_ms(self.config.rebalance_timeout),
                    topics: &self.subscribed_topics,
                    assignors: &assignors,
                    owned: &owned,
                },
            )
            .await?;
            self.member_id.clone_from(&join.member_id);
            self.generation_id = join.generation_id;
            let assignments = if join.leader {
                self.compute_assignments(&join.protocol_name, &join.members)
                    .await?
            } else {
                Vec::new()
            };
            let cooperative = assignor::is_cooperative(&join.protocol_name);
            match coordinator::sync_group(
                &context,
                self.generation_id,
                &self.member_id,
                &join.protocol_name,
                assignments,
            )
            .await
            {
                Ok(assigned) => break (assigned, cooperative),
                Err(error)
                    if is_rebalance_in_progress(&error) && attempts < MAX_REJOIN_ATTEMPTS => {},
                Err(error) => return Err(error),
            }
        };

        let revoked = self
            .apply_assignment(
                coordinator,
                &group_id,
                &Rebalance {
                    owned: &owned,
                    assigned: &assigned,
                    cooperative,
                },
            )
            .await?;
        self.metrics.record_rebalance();
        self.needs_rejoin.store(false, Ordering::SeqCst);
        // Cooperative rebalance: having dropped the revoked partitions from our
        // reported ownership, rejoin so the coordinator can hand them to their new
        // owner in a follow-up round (KIP-429's incremental revoke).
        if cooperative && revoked {
            self.needs_rejoin.store(true, Ordering::SeqCst);
        }
        self.set_heartbeat_context(Some(HeartbeatContext {
            coordinator_id: coordinator,
            group_id,
            generation_id: self.generation_id,
            member_id: self.member_id.clone(),
            group_instance_id: instance.map(str::to_owned),
        }));
        Ok(())
    }

    /// Apply a synced assignment to the subscription and resume each newly owned
    /// partition from its committed offset. Returns whether any previously owned
    /// partition was revoked (only meaningful under cooperative rebalance).
    ///
    /// Eager: the whole assignment is (re)owned, so every partition is refetched
    /// from its committed offset. Cooperative: partitions we keep retain their
    /// live position (rewinding to the last commit would reprocess records), so
    /// only the newly added partitions are seeded from committed offsets.
    async fn apply_assignment(
        &mut self,
        coordinator: i32,
        group_id: &str,
        rebalance: &Rebalance<'_>,
    ) -> Result<bool> {
        let Rebalance {
            owned,
            assigned,
            cooperative,
        } = *rebalance;
        let (to_position, revoked) = if cooperative {
            let assigned_set: HashSet<&TopicPartition> = assigned.iter().collect();
            let owned_set: HashSet<&TopicPartition> = owned.iter().collect();
            let added: Vec<TopicPartition> = assigned
                .iter()
                .filter(|partition| !owned_set.contains(*partition))
                .cloned()
                .collect();
            let revoked = owned
                .iter()
                .any(|partition| !assigned_set.contains(partition));
            (added, revoked)
        } else {
            (assigned.to_vec(), false)
        };
        // `assign` keeps positions for retained partitions and drops revoked ones.
        self.subscription.assign(assigned);
        if !to_position.is_empty() {
            let committed =
                coordinator::fetch_committed(&self.wire, coordinator, group_id, &to_position)
                    .await?;
            for (partition, offset) in committed {
                self.subscription.set_position(
                    &partition,
                    FetchPosition::new(offset.offset, offset.leader_epoch),
                );
            }
        }
        Ok(revoked)
    }

    /// Update the group context the background heartbeat task reads.
    fn set_heartbeat_context(&self, context: Option<HeartbeatContext>) {
        *self
            .heartbeat_context
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = context;
    }

    /// The assignor protocol names to advertise (`partition.assignment.strategy`,
    /// mapped and de-duplicated; defaults to `range`).
    fn advertised_assignors(&self) -> Vec<&'static str> {
        let mut names: Vec<&'static str> = Vec::new();
        for strategy in &self.config.partition_assignment_strategy {
            let name = assignor::protocol_name(strategy);
            if !names.contains(&name) {
                names.push(name);
            }
        }
        if names.is_empty() {
            names.push(assignor::RANGE_ASSIGNOR);
        }
        names
    }

    /// Leader-only: run the selected assignor over the members' subscriptions,
    /// using cluster metadata for partition counts, and encode each member's
    /// assignment blob.
    async fn compute_assignments(
        &self,
        protocol_name: &str,
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
        let assignment = assignor::assign(protocol_name, members, &partitions_per_topic);
        Ok(assignment
            .into_iter()
            .map(|(member, partitions)| (member, assignor::encode_assignment(&partitions)))
            .collect())
    }

    /// Best-effort background auto-commit, throttled to `auto.commit.interval.ms`.
    /// Failures are swallowed (retried on the next interval), matching Java's
    /// async auto-commit.
    async fn maybe_auto_commit(&mut self) {
        if !self.config.enable_auto_commit || self.config.group_id.is_empty() {
            return;
        }
        let due = self
            .last_auto_commit
            .is_none_or(|last| last.elapsed() >= self.config.auto_commit_interval);
        if due {
            self.last_auto_commit = Some(Instant::now());
            let _outcome = self.auto_commit_now().await;
        }
    }

    /// Commit the current positions now if auto-commit is enabled (best effort);
    /// used before a rebalance and on close.
    async fn auto_commit_now(&mut self) -> Result<()> {
        if !self.config.enable_auto_commit || self.config.group_id.is_empty() {
            return Ok(());
        }
        let offsets = self.current_position_offsets();
        if offsets.is_empty() {
            return Ok(());
        }
        let group_id = self.config.group_id.clone();
        let coordinator = self.ensure_coordinator(&group_id).await?;
        let result =
            coordinator::commit_offsets(&self.wire, coordinator, &group_id, &offsets).await;
        if result.is_ok() {
            self.metrics.record_commit();
            self.interceptors.on_commit(&offsets);
        }
        result
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

/// The background heartbeat task: while joined, send a `Heartbeat` every
/// `heartbeat.interval.ms` so the session stays alive independent of poll
/// cadence (Java's `HeartbeatThread`). Any group-level signal — rebalance, or a
/// fenced generation/member — flags a rejoin for `poll` to pick up; a transient
/// wire error is retried on the next tick.
#[expect(
    clippy::infinite_loop,
    reason = "The heartbeat task runs until the consumer aborts its JoinHandle."
)]
async fn heartbeat_loop(
    wire: WireClient,
    context: Arc<Mutex<Option<HeartbeatContext>>>,
    needs_rejoin: Arc<AtomicBool>,
    interval: Duration,
    metrics: ConsumerMetrics,
) {
    loop {
        tokio::time::sleep(interval).await;
        let snapshot = context
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        let Some(group) = snapshot else {
            continue;
        };
        let context = coordinator::GroupContext {
            wire: &wire,
            coordinator_id: group.coordinator_id,
            group_id: &group.group_id,
            group_instance_id: group.group_instance_id.as_deref(),
        };
        match coordinator::heartbeat(&context, group.generation_id, &group.member_id).await {
            Ok(ErrorCode::None) => metrics.record_heartbeat(),
            Ok(_) => needs_rejoin.store(true, Ordering::SeqCst),
            Err(_transient) => {},
        }
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
