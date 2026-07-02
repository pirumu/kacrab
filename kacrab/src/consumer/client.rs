//! The public [`Consumer`] facade and its `poll` loop.
//!
//! Supports both manual assignment (`assign`) and group `subscribe`, with
//! position control (`seek`, `pause`, `resume`), `auto.offset.reset`, offset
//! commit/fetch (`commit_sync`/`commit_async`/`committed`, plus background
//! auto-commit), and group membership under both protocols: the classic
//! client-side-assignment protocol (`JoinGroup`/`SyncGroup` with the eager
//! `range`/`roundrobin`/`sticky` and incremental `cooperative-sticky` assignors)
//! and the KIP-848 server-side protocol (`group.protocol=consumer`, a single
//! `ConsumerGroupHeartbeat` RPC). `poll` drives fetch and rejoin on the caller's
//! task; the classic path also runs a dedicated background heartbeat task,
//! mirroring the Java consumer's user thread + `HeartbeatThread`.

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
    config::{AutoOffsetReset, ConsumerRuntimeConfig, GroupProtocol},
    coordinator,
    error::{ConsumerError, Result},
    fetch,
    interceptor::{ConsumerInterceptor, ConsumerInterceptors, InterceptorConfigs},
    metrics::{ConsumerMetrics, ConsumerMetricsSnapshot},
    next_gen::{
        self, AssignedTopic, EPOCH_JOINING, EPOCH_LEAVING, HeartbeatRequest, ModernGroupState,
    },
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
/// Supports manual assignment and group
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
    /// KIP-848 membership state, present only under `group.protocol=consumer`.
    modern_group: Option<ModernGroupState>,
    /// When the last KIP-848 `ConsumerGroupHeartbeat` was sent.
    last_modern_heartbeat: Option<Instant>,
    /// Per-broker incremental fetch sessions (KIP-227).
    fetch_sessions: fetch::FetchSessions,
    /// Ordered queue of asynchronous commits, drained one at a time by
    /// `async_commit_task` so two `commit_async` calls never land out of order —
    /// otherwise a stale commit could move the committed offset backwards (Java's
    /// single network thread serializes commits the same way).
    async_commits: tokio::sync::mpsc::UnboundedSender<AsyncCommit>,
    /// The background task draining `async_commits`, aborted on close.
    async_commit_task: JoinHandle<()>,
}

/// A queued asynchronous commit, applied in send order by the commit worker.
struct AsyncCommit {
    offsets: HashMap<TopicPartition, OffsetAndMetadata>,
    callback: OffsetCommitCallback,
    coordinator_id: i32,
    group_id: String,
    generation_id: i32,
    member_id: String,
    interceptors: ConsumerInterceptors,
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

/// Whether an error means the group coordinator moved or is unavailable, so the
/// cached coordinator must be dropped and re-discovered (`FindCoordinator`).
const fn is_coordinator_moved(error: &ConsumerError) -> bool {
    matches!(
        error,
        ConsumerError::Broker {
            error: ErrorCode::NotCoordinator
                | ErrorCode::CoordinatorNotAvailable
                | ErrorCode::CoordinatorLoadInProgress,
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
        self.async_commit_task.abort();
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
        let connection = config
            .to_connection_config()
            .map_err(|error| ConsumerError::Config { error })?;
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
        let (async_commits, async_commit_rx) = tokio::sync::mpsc::unbounded_channel();
        let async_commit_task = tokio::spawn(async_commit_loop(
            async_commit_rx,
            wire.clone(),
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
            modern_group: None,
            last_modern_heartbeat: None,
            fetch_sessions: fetch::FetchSessions::default(),
            async_commits,
            async_commit_task,
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
    /// assignment. Manual assignment bypasses group coordination.
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
        let (generation_id, member_id) = self.commit_identity();
        let mut refound = false;
        let result = loop {
            let coordinator = self.ensure_coordinator(&group_id).await?;
            let attempt = coordinator::commit_offsets(
                &self.wire,
                &coordinator::CommitTarget {
                    coordinator_id: coordinator,
                    group_id: &group_id,
                    generation_id,
                    member_id: &member_id,
                },
                &offsets,
            )
            .await;
            match attempt {
                Err(error) if !refound && self.note_coordinator_error(&error) => refound = true,
                outcome => break outcome,
            }
        };
        if result.is_ok() {
            self.metrics.record_commit();
            self.interceptors.on_commit(&offsets);
        }
        result
    }

    /// The `(generation-or-epoch, member id)` a commit identifies itself with:
    /// the KIP-848 member epoch/id, else the classic generation/member id, else
    /// the `-1`/empty manual-assignment convention.
    fn commit_identity(&self) -> (i32, String) {
        self.modern_group.as_ref().map_or_else(
            || {
                if self.member_id.is_empty() {
                    (-1, String::new())
                } else {
                    (self.generation_id, self.member_id.clone())
                }
            },
            |state| (state.member_epoch, state.member_id.clone()),
        )
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
        let (generation_id, member_id) = self.commit_identity();
        // Enqueue rather than spawn, so the single worker applies commits in call
        // order and a later commit never loses to an earlier, slower one.
        let commit = AsyncCommit {
            offsets,
            callback,
            coordinator_id: coordinator,
            group_id,
            generation_id,
            member_id,
            interceptors: self.interceptors.clone(),
        };
        if let Err(tokio::sync::mpsc::error::SendError(commit)) = self.async_commits.send(commit) {
            // The worker is gone (consumer closing) — report rather than drop.
            (commit.callback)(Err(ConsumerError::InvalidState("consumer is closing")));
        }
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
        let mut refound = false;
        loop {
            let coordinator = self.ensure_coordinator(&group_id).await?;
            match coordinator::fetch_committed(&self.wire, coordinator, &group_id, partitions).await
            {
                Err(error) if !refound && self.note_coordinator_error(&error) => refound = true,
                outcome => return outcome,
            }
        }
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
            // Group members (re)join before fetching; manual assignment skips all
            // of this. KIP-848 uses the single-RPC heartbeat protocol; the classic
            // path runs JoinGroup/SyncGroup.
            if self.is_subscribed() {
                match self.config.group_protocol {
                    GroupProtocol::Consumer => self.ensure_active_group_modern().await?,
                    GroupProtocol::Classic => self.ensure_active_group().await?,
                }
            }
            self.maybe_auto_commit().await;

            let topics = self.assigned_topics();
            let mut fetchable_empty = true;
            if !topics.is_empty() {
                let metadata = self.wire.metadata_for_topics(topics).await?;
                self.reset_positions(&metadata).await?;
                self.validate_positions(&metadata).await?;

                let fetchable = self.subscription.fetchable_partitions();
                fetchable_empty = fetchable.is_empty();
                if !fetchable.is_empty() {
                    self.metrics.record_fetch();
                    // Clamp the broker's long-poll wait to what's left of the
                    // caller's poll timeout, so a short `poll` isn't blocked for
                    // the full `fetch.max.wait.ms`.
                    let remaining = clamp_ms(timeout.saturating_sub(start.elapsed()));
                    let max_wait_ms = remaining.min(self.config.fetch_max_wait_ms);
                    let progress = fetch::fetch(
                        &fetch::FetchContext {
                            wire: &self.wire,
                            config: &self.config,
                            metadata: &metadata,
                            max_wait_ms,
                        },
                        &fetchable,
                        self.config.max_poll_records,
                        &mut self.fetch_sessions,
                    )
                    .await?;
                    // Out-of-range partitions clear their position so the next poll
                    // re-resolves it via `auto.offset.reset` (KIP behaviour parity).
                    for partition in &progress.resets {
                        self.subscription.request_reset(partition);
                    }
                    // Stale-leader partitions invalidate cached metadata so the next
                    // poll re-resolves their leaders.
                    for partition in &progress.stale {
                        self.wire
                            .invalidate_topic_partition(&partition.topic, partition.partition);
                    }
                    let mut records = ConsumerRecords::empty();
                    for partition_fetch in progress.partitions {
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

    /// Close the consumer: commit (auto-commit) and leave the group (best effort)
    /// and release its broker connections. Bounded by `request.timeout.ms` so a
    /// hung coordinator cannot hang close (Java's `default.api.timeout.ms`). The
    /// wire client shuts its broker tasks down on drop.
    pub async fn close(mut self) {
        self.heartbeat_task.abort();
        self.async_commit_task.abort();
        self.interceptors.close();
        let close_timeout = self.config.request_timeout;
        let _timed_out = tokio::time::timeout(close_timeout, self.commit_and_leave()).await;
        drop(self);
    }

    /// Commit the current positions (auto-commit) and leave the group under
    /// whichever protocol is active. Awaited under a timeout by [`close`](Self::close).
    async fn commit_and_leave(&mut self) {
        let _committed = self.auto_commit_now().await;
        // Static members (`group.instance.id`) stay in the group across a close so
        // a quick restart avoids a rebalance, matching Java.
        let dynamic_member = self.config.group_instance_id.is_empty();
        match self.config.group_protocol {
            GroupProtocol::Consumer => {
                if let (true, Some(state), Some(coordinator)) = (
                    dynamic_member,
                    self.modern_group.as_ref(),
                    self.coordinator_id,
                ) {
                    let member_id = state.member_id.clone();
                    // A leaving heartbeat (epoch -1) releases the assignment.
                    let _left = next_gen::heartbeat(
                        &self.wire,
                        coordinator,
                        &HeartbeatRequest {
                            group_id: &self.config.group_id,
                            member_id: &member_id,
                            member_epoch: EPOCH_LEAVING,
                            instance_id: None,
                            rack_id: None,
                            rebalance_timeout_ms: -1,
                            subscribed_topics: &[],
                            server_assignor: None,
                            owned: &[],
                        },
                    )
                    .await;
                }
            },
            GroupProtocol::Classic => {
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
            },
        }
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

    /// If `error` means the coordinator moved, drop the cached coordinator so the
    /// next [`ensure_coordinator`](Self::ensure_coordinator) re-discovers it, and
    /// report that a re-find is warranted. A coordinator moves on broker restart
    /// or `__consumer_offsets` reassignment — otherwise the stale id would fail
    /// every commit/heartbeat/rejoin until the consumer is recreated.
    const fn note_coordinator_error(&mut self, error: &ConsumerError) -> bool {
        if is_coordinator_moved(error) {
            self.coordinator_id = None;
            true
        } else {
            false
        }
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
        let assignors = self.advertised_assignors();
        // Partitions we currently own — reported to the coordinator so the
        // cooperative assignor knows what not to hand to someone else yet. Eager
        // assignors ignore it. Captured before the loop: it does not change until
        // we apply the new assignment below.
        let owned = self.subscription.assigned_partitions();
        // A rebalance that starts between our JoinGroup and SyncGroup makes the
        // coordinator answer REBALANCE_IN_PROGRESS; a coordinator move answers
        // NOT_COORDINATOR. Both rejoin and retry — the latter after re-finding the
        // coordinator (`coordinator_id` is looked up fresh each iteration).
        let mut attempts = 0_u32;
        let (assigned, cooperative, coordinator) = loop {
            attempts = attempts.saturating_add(1);
            let coordinator = self.ensure_coordinator(&group_id).await?;
            let context = coordinator::GroupContext {
                wire: &self.wire,
                coordinator_id: coordinator,
                group_id: &group_id,
                group_instance_id: instance,
            };
            let join = match coordinator::join_group(
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
            .await
            {
                Ok(join) => join,
                Err(error) if attempts < MAX_REJOIN_ATTEMPTS && is_coordinator_moved(&error) => {
                    self.coordinator_id = None;
                    continue;
                },
                Err(error) => return Err(error),
            };
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
                Ok(assigned) => break (assigned, cooperative, coordinator),
                Err(error)
                    if is_rebalance_in_progress(&error) && attempts < MAX_REJOIN_ATTEMPTS => {},
                Err(error) if is_coordinator_moved(&error) && attempts < MAX_REJOIN_ATTEMPTS => {
                    self.coordinator_id = None;
                },
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
        // `assign_grouped` keeps positions for retained partitions, drops revoked
        // ones, and marks the subscription group-managed (`AutoAssigned`).
        self.subscription.assign_grouped(assigned);
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

    /// KIP-848 membership: send a `ConsumerGroupHeartbeat` when due and reconcile
    /// toward the coordinator-computed target assignment. Unlike the classic
    /// path, this never blocks — reconciliation is incremental across heartbeats.
    async fn ensure_active_group_modern(&mut self) -> Result<()> {
        let interval = self
            .modern_group
            .as_ref()
            .map_or(self.config.heartbeat_interval, |state| {
                state.heartbeat_interval
            });
        let due = self.modern_group.is_none()
            || self.needs_rejoin.load(Ordering::SeqCst)
            || self
                .last_modern_heartbeat
                .is_none_or(|last| last.elapsed() >= interval);
        if !due {
            return Ok(());
        }

        let group_id = self.config.group_id.clone();
        let coordinator = self.ensure_coordinator(&group_id).await?;
        if self.modern_group.is_none() {
            self.modern_group = Some(ModernGroupState::new(self.config.heartbeat_interval)?);
        }

        // Resolve topic ids for the reconciliation and the owned set we report.
        let metadata = self
            .wire
            .metadata_for_topics(self.subscribed_topics.clone())
            .await?;
        let owned = self.owned_as_topic_ids(&metadata);

        let (member_id, member_epoch) = self
            .modern_group
            .as_ref()
            .map(|state| (state.member_id.clone(), state.member_epoch))
            .unwrap_or_default();
        let instance = (!self.config.group_instance_id.is_empty())
            .then_some(self.config.group_instance_id.as_str());
        let rack =
            (!self.config.client_rack.is_empty()).then_some(self.config.client_rack.as_str());
        let assignor = self.config.group_remote_assignor.as_deref();

        let outcome = next_gen::heartbeat(
            &self.wire,
            coordinator,
            &HeartbeatRequest {
                group_id: &group_id,
                member_id: &member_id,
                member_epoch,
                instance_id: instance,
                rack_id: rack,
                rebalance_timeout_ms: clamp_ms(self.config.rebalance_timeout),
                subscribed_topics: &self.subscribed_topics,
                server_assignor: assignor,
                owned: &owned,
            },
        )
        .await?;
        self.last_modern_heartbeat = Some(Instant::now());

        match outcome.error {
            ErrorCode::None => {
                if let Some(state) = self.modern_group.as_mut() {
                    state.member_epoch = outcome.member_epoch;
                    if outcome.heartbeat_interval > Duration::ZERO {
                        state.heartbeat_interval = outcome.heartbeat_interval;
                    }
                    if let Some(id) = outcome.member_id.filter(|id| !id.is_empty()) {
                        state.member_id = id;
                    }
                }
                self.needs_rejoin.store(false, Ordering::SeqCst);
                if let Some(assignment) = outcome.assignment {
                    self.reconcile_modern(coordinator, &group_id, &metadata, assignment)
                        .await?;
                    self.metrics.record_rebalance();
                }
            },
            // Lost membership — abandon the assignment and rejoin from epoch 0.
            ErrorCode::FencedMemberEpoch => {
                if let Some(state) = self.modern_group.as_mut() {
                    state.member_epoch = EPOCH_JOINING;
                }
                self.subscription.assign(&[]);
                self.needs_rejoin.store(true, Ordering::SeqCst);
            },
            // The coordinator forgot us — start over with a fresh member id.
            ErrorCode::UnknownMemberId => {
                self.modern_group = Some(ModernGroupState::new(self.config.heartbeat_interval)?);
                self.subscription.assign(&[]);
                self.needs_rejoin.store(true, Ordering::SeqCst);
            },
            // Coordinator moved or is loading — re-find it on the next heartbeat.
            code if code.is_retriable() => {
                self.coordinator_id = None;
            },
            code => {
                return Err(ConsumerError::broker(
                    "consumer_group_heartbeat",
                    code,
                    "consumer group heartbeat failed",
                ));
            },
        }
        Ok(())
    }

    /// Reconcile the subscription toward a KIP-848 target assignment: resolve its
    /// topic ids to names, then apply it with cooperative semantics (retained
    /// partitions keep their positions; newly added ones resume from committed).
    ///
    /// The reconciliation is server-driven: the group coordinator withholds a
    /// partition from a member's target until its previous owner has revoked it
    /// (reported a reduced owned set in a heartbeat), so applying the target
    /// directly never double-owns a partition. Revocation is reflected in the next
    /// heartbeat's `owned` set. (The multi-member handoff is exercised against a
    /// real broker only for the classic cooperative path, not yet KIP-848.)
    async fn reconcile_modern(
        &mut self,
        coordinator: i32,
        group_id: &str,
        metadata: &ClusterMetadata,
        assignment: Vec<AssignedTopic>,
    ) -> Result<()> {
        let mut target: Vec<TopicPartition> = Vec::new();
        for topic in assignment {
            let Some(name) = topic_name_for_id(metadata, topic.topic_id) else {
                continue;
            };
            for partition in topic.partitions {
                target.push(TopicPartition::new(name.clone(), partition));
            }
        }
        let owned = self.subscription.assigned_partitions();
        let _revoked = self
            .apply_assignment(
                coordinator,
                group_id,
                &Rebalance {
                    owned: &owned,
                    assigned: &target,
                    cooperative: true,
                },
            )
            .await?;
        Ok(())
    }

    /// The current assignment grouped by topic id, for the heartbeat's owned set.
    fn owned_as_topic_ids(&self, metadata: &ClusterMetadata) -> Vec<AssignedTopic> {
        let mut by_id: Vec<AssignedTopic> = Vec::new();
        for partition in self.subscription.assigned_partitions() {
            let Some(topic_id) = topic_id_for_name(metadata, &partition.topic) else {
                continue;
            };
            if let Some(topic) = by_id.iter_mut().find(|topic| topic.topic_id == topic_id) {
                topic.partitions.push(partition.partition);
            } else {
                by_id.push(AssignedTopic {
                    topic_id,
                    partitions: vec![partition.partition],
                });
            }
        }
        by_id
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
        let (generation_id, member_id) = self.commit_identity();
        let mut refound = false;
        let result = loop {
            let coordinator = self.ensure_coordinator(&group_id).await?;
            let attempt = coordinator::commit_offsets(
                &self.wire,
                &coordinator::CommitTarget {
                    coordinator_id: coordinator,
                    group_id: &group_id,
                    generation_id,
                    member_id: &member_id,
                },
                &offsets,
            )
            .await;
            match attempt {
                Err(error) if !refound && self.note_coordinator_error(&error) => refound = true,
                outcome => break outcome,
            }
        };
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

    /// Validate assigned positions against their leaders' epoch history when a
    /// leader change is visible in metadata (the current leader epoch is newer
    /// than the epoch our position was fetched under), resetting any position the
    /// broker reports was truncated below it (KIP-320). Positions confirmed valid
    /// have their recorded epoch advanced so they are not re-validated.
    async fn validate_positions(&mut self, metadata: &ClusterMetadata) -> Result<()> {
        let mut to_validate: Vec<(TopicPartition, FetchPosition, i32)> = Vec::new();
        for partition in self.subscription.assigned_partitions() {
            let Some(position) = self.subscription.position(&partition) else {
                continue;
            };
            let Some(fenced) = position.leader_epoch else {
                continue;
            };
            let current =
                offsets::partition_leader_epoch(metadata, &partition.topic, partition.partition);
            if let Some(current) = current
                && current > fenced
            {
                to_validate.push((partition, position, current));
            }
        }
        if to_validate.is_empty() {
            return Ok(());
        }
        let outcomes = offsets::validate_offsets(&self.wire, metadata, &to_validate).await?;
        for (partition, outcome) in outcomes {
            match outcome {
                offsets::PositionValidation::Valid { leader_epoch } => {
                    if let Some(position) = self.subscription.position(&partition) {
                        self.subscription.set_position(
                            &partition,
                            FetchPosition::new(position.offset, Some(leader_epoch)),
                        );
                    }
                },
                offsets::PositionValidation::Truncated {
                    offset,
                    leader_epoch,
                } => {
                    self.subscription
                        .set_position(&partition, FetchPosition::new(offset, leader_epoch));
                },
            }
        }
        Ok(())
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

/// Resolve a KIP-848 assignment topic id to its name via cluster metadata.
fn topic_name_for_id(metadata: &ClusterMetadata, topic_id: KafkaUuid) -> Option<String> {
    metadata
        .topics
        .iter()
        .find(|topic| topic.topic_id == topic_id)
        .map(|topic| topic.name.clone())
}

/// Resolve a topic name to its id for the heartbeat's owned set (`None` when the
/// broker reported no stable id).
fn topic_id_for_name(metadata: &ClusterMetadata, name: &str) -> Option<KafkaUuid> {
    metadata
        .topic(name)
        .map(|topic| topic.topic_id)
        .filter(|topic_id| *topic_id != KafkaUuid::ZERO)
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

/// The asynchronous-commit worker: drain the queue and apply each commit to
/// completion before the next, so commits reach the coordinator in the order
/// `commit_async` was called (mirroring Java's single network thread). Exits when
/// the consumer drops its `async_commits` sender.
async fn async_commit_loop(
    mut receiver: tokio::sync::mpsc::UnboundedReceiver<AsyncCommit>,
    wire: WireClient,
    metrics: ConsumerMetrics,
) {
    while let Some(commit) = receiver.recv().await {
        let result = coordinator::commit_offsets(
            &wire,
            &coordinator::CommitTarget {
                coordinator_id: commit.coordinator_id,
                group_id: &commit.group_id,
                generation_id: commit.generation_id,
                member_id: &commit.member_id,
            },
            &commit.offsets,
        )
        .await;
        if result.is_ok() {
            metrics.record_commit();
            commit.interceptors.on_commit(&commit.offsets);
        }
        (commit.callback)(result);
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use bytes::Bytes;

    use super::*;
    use crate::consumer::{ConsumerRecord, ConsumerRecords, StringDeserializer, TimestampType};

    // A consumer pointed at a dead literal-IP broker: `from_map` resolves the
    // address but never connects, so every synchronous method below runs without
    // any broker I/O.
    async fn consumer_with_group() -> Consumer {
        Consumer::from_map([
            ("bootstrap.servers", "127.0.0.1:9092"),
            ("group.id", "cov-group"),
            ("auto.offset.reset", "earliest"),
            ("enable.auto.commit", "false"),
        ])
        .await
        .expect("consumer builds")
    }

    async fn consumer_no_group() -> Consumer {
        Consumer::from_map([("bootstrap.servers", "127.0.0.1:9092")])
            .await
            .expect("consumer builds")
    }

    #[tokio::test]
    async fn manual_assignment_and_position_control() {
        let mut consumer = consumer_no_group().await;
        let p0 = TopicPartition::new("t", 0);
        let p1 = TopicPartition::new("t", 1);
        consumer.assign([p0.clone(), p1.clone()]);
        assert_eq!(consumer.assignment().len(), 2);

        // Seek sets a position on an assigned partition; unassigned seeks error.
        consumer.seek(&p0, 42).expect("seek assigned");
        consumer
            .seek_with_leader_epoch(&p1, 7, Some(3))
            .expect("seek epoch");
        assert!(matches!(
            consumer.seek(&TopicPartition::new("t", 9), 0),
            Err(ConsumerError::PartitionNotAssigned { .. })
        ));

        // Pause/resume flow.
        consumer.pause(std::slice::from_ref(&p0));
        assert_eq!(consumer.paused(), vec![p0.clone()]);
        consumer.resume(std::slice::from_ref(&p0));
        assert!(consumer.paused().is_empty());
    }

    #[tokio::test]
    async fn subscribe_requires_group_and_toggles_pattern() {
        let mut no_group = consumer_no_group().await;
        assert!(matches!(
            no_group.subscribe(["t"]),
            Err(ConsumerError::InvalidState(_))
        ));
        assert!(matches!(
            no_group.subscribe_pattern("t.*"),
            Err(ConsumerError::InvalidState(_))
        ));

        let mut consumer = consumer_with_group().await;
        consumer.subscribe(["b", "a", "a"]).expect("subscribe");
        assert_eq!(
            consumer.subscription(),
            vec!["a".to_owned(), "b".to_owned()]
        );
        // A pattern subscription clears the explicit topic list until the first poll.
        consumer
            .subscribe_pattern("^prefix-.*$")
            .expect("valid regex");
        assert!(consumer.subscription().is_empty());
        assert!(matches!(
            consumer.subscribe_pattern("("),
            Err(ConsumerError::InvalidArgument { .. })
        ));
        consumer.unsubscribe();
        assert!(consumer.subscription().is_empty());
    }

    #[tokio::test]
    async fn coordinator_error_drops_the_cached_coordinator() {
        let mut consumer = consumer_with_group().await;
        consumer.coordinator_id = Some(7);
        // A non-coordinator error leaves the cache intact.
        let unrelated = ConsumerError::broker("commit", ErrorCode::InvalidGroupId, "x");
        assert!(!consumer.note_coordinator_error(&unrelated));
        assert_eq!(consumer.coordinator_id, Some(7));
        // A coordinator-moved error clears it so the next op re-discovers it.
        for code in [
            ErrorCode::NotCoordinator,
            ErrorCode::CoordinatorNotAvailable,
            ErrorCode::CoordinatorLoadInProgress,
        ] {
            consumer.coordinator_id = Some(7);
            let moved = ConsumerError::broker("commit", code, "moved");
            assert!(consumer.note_coordinator_error(&moved));
            assert_eq!(consumer.coordinator_id, None);
        }
    }

    #[tokio::test]
    async fn group_metadata_and_enforce_rebalance() {
        let consumer = consumer_with_group().await;
        let metadata = consumer.group_metadata();
        assert_eq!(metadata.group_id, "cov-group");
        assert_eq!(metadata.generation_id, -1);
        assert!(metadata.member_id.is_empty());
        // enforce_rebalance just flags a rejoin (no broker I/O).
        consumer.enforce_rebalance();
    }

    #[tokio::test]
    async fn empty_commits_and_reads_short_circuit() {
        let mut consumer = consumer_with_group().await;
        // Empty commits and reads resolve without touching the coordinator.
        consumer
            .commit_sync_offsets(HashMap::new())
            .await
            .expect("empty commit");
        assert!(
            consumer
                .committed(&[])
                .await
                .expect("empty read")
                .is_empty()
        );
        // A non-empty commit on a consumer with no group.id fails fast.
        let mut no_group = consumer_no_group().await;
        let mut offsets = HashMap::new();
        let _prev = offsets.insert(TopicPartition::new("t", 0), OffsetAndMetadata::new(1));
        assert!(matches!(
            no_group.commit_sync_offsets(offsets).await,
            Err(ConsumerError::InvalidState(_))
        ));
    }

    #[tokio::test]
    async fn poll_wakeup_and_idle_manual_assignment() {
        let mut consumer = consumer_no_group().await;
        // Nothing assigned and not subscribed — poll returns an empty batch.
        let records = consumer
            .poll(Duration::from_millis(0))
            .await
            .expect("idle poll");
        assert!(records.is_empty());
        // Wakeup makes the next poll return immediately with Wakeup.
        consumer.wakeup();
        assert!(matches!(
            consumer.poll(Duration::from_secs(5)).await,
            Err(ConsumerError::Wakeup)
        ));
    }

    #[tokio::test]
    async fn metrics_interceptors_and_deserialized_records() {
        let mut consumer = consumer_with_group().await;
        consumer.add_interceptor(NoopInterceptor);
        let snapshot = consumer.metrics();
        assert_eq!(snapshot.poll_total, 0);

        // A typed deserializer maps record bytes.
        let mut records = ConsumerRecords::empty();
        records.push_partition(
            "t".to_owned(),
            0,
            vec![ConsumerRecord {
                topic: "t".to_owned(),
                partition: 0,
                offset: 0,
                timestamp: 0,
                timestamp_type: TimestampType::CreateTime,
                key: None,
                value: Some(Bytes::from_static(b"hi")),
                headers: Vec::new(),
                leader_epoch: None,
            }],
        );
        let record = records.iter().next().expect("one record");
        let (key, value) = record
            .deserialized(&StringDeserializer, &StringDeserializer)
            .expect("deserialize");
        assert_eq!(key, None);
        assert_eq!(value, Some("hi".to_owned()));
    }

    struct NoopInterceptor;
    impl ConsumerInterceptor for NoopInterceptor {
        fn on_consume(&self, records: ConsumerRecords) -> ConsumerRecords {
            records
        }
    }
}
