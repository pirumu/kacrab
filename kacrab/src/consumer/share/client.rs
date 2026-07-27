//! The public [`ShareConsumer`] facade and its `poll` loop.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use kacrab_protocol::{
    KafkaUuid,
    generated::{
        ApiKey, ErrorCode, GetTelemetrySubscriptionsRequestData,
        GetTelemetrySubscriptionsResponseData, ShareAcknowledgeResponseData,
        ShareFetchResponseData,
        share_acknowledge_response::PartitionData as AcknowledgePartitionData,
    },
    version::client_api_info,
};

use super::{
    config::{ShareAcknowledgementMode, ShareRuntimeConfig, share_api_version},
    membership::{EPOCH_JOINING, EPOCH_LEAVING, ShareGroupState, ShareHeartbeatRequest, heartbeat},
    record::{AcknowledgeType, ShareRecord, ShareRecords},
    session::{
        Acknowledgements, ShareFetchOutcome, ShareFetchPlan, ShareSession, TopicIdPartition,
        build_share_acknowledge, build_share_fetch, decode_share_fetch, gap_acknowledgements,
        is_session_lost,
    },
};
use crate::{
    common::TopicPartition,
    config::{ClientConfig, ConfigKey, ConfigValue, ConsumerConfig, Properties},
    consumer::{
        client::resolve_bootstrap_brokers,
        coordinator,
        error::{ConsumerError, Result},
        fetch::idle_backoff,
        metrics::{ConsumerMetrics, ConsumerMetricsSnapshot},
        offsets::partition_leader,
    },
    wire::{ClusterMetadata, WireClient, WireError},
};

/// A Kafka share consumer — the client side of share groups (KIP-932).
///
/// A share group is a queue, not a log reader. Records are *acquired* under a
/// broker-held lock instead of being read at a position, so more consumers than
/// partitions can share a topic, and each record is disposed of individually with
/// [`acknowledge`](Self::acknowledge) rather than by committing an offset. That
/// is why this is a separate type from [`Consumer`](crate::consumer::Consumer)
/// and not a mode on it: there is no assignment to own, no `seek`, no position,
/// and no offset commit.
///
/// ```no_run
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// use std::time::Duration;
///
/// use kacrab::consumer::{AcknowledgeType, ShareConsumer};
///
/// let mut consumer = ShareConsumer::from_map([
///     ("bootstrap.servers", "localhost:9092"),
///     ("group.id", "work-queue"),
///     ("share.acknowledgement.mode", "explicit"),
/// ])
/// .await?;
/// consumer.subscribe(["jobs"])?;
///
/// let records = consumer.poll(Duration::from_millis(500)).await?;
/// for record in records.iter() {
///     let outcome = if record.delivery_count > 5 {
///         // Poison message: stop redelivering it.
///         AcknowledgeType::Reject
///     } else {
///         AcknowledgeType::Accept
///     };
///     consumer.acknowledge(record, outcome)?;
/// }
/// consumer.commit().await?;
/// # Ok(())
/// # }
/// ```
///
/// # Wire protocol
///
/// Three RPCs, negotiated like any other:
///
/// | RPC | key | versions | role |
/// | --- | ---: | --- | --- |
/// | `ShareGroupHeartbeat` | 76 | v1 | membership and assignment |
/// | `ShareFetch` | 78 | v1–2 | acquire records, piggy-back acknowledgements |
/// | `ShareAcknowledge` | 79 | v1–2 | standalone acknowledgements, session close |
///
/// Share groups are a production feature from Kafka 4.2 onward
/// (`ShareVersion.LATEST_PRODUCTION = SV_1`), so a 4.3 broker has them on by
/// default. Against a broker that does not, the first heartbeat comes back
/// `UNSUPPORTED_VERSION`.
///
/// # Cancellation and drop
///
/// `poll` is **not** cancel-safe in the sense of preserving work: dropping the
/// future while a `ShareFetch` is in flight discards the response, so any records
/// the broker acquired for this member stay locked until their acquisition lock
/// expires (`group.share.record.lock.duration.ms`) and are then redelivered —
/// to this member or another one. Nothing is lost, but a record can be delivered
/// twice. The same holds for dropping a [`ShareConsumer`] without
/// [`close`](Self::close): unacknowledged records become re-deliverable at lock
/// expiry instead of being acknowledged. `close` acknowledges what is pending and
/// closes the share sessions, which releases the rest immediately rather than
/// after the lock timeout.
pub struct ShareConsumer {
    wire: WireClient,
    config: ShareRuntimeConfig,
    wakeup: Arc<AtomicBool>,
    coordinator_id: Option<i32>,
    /// Topics this consumer subscribed to; empty until `subscribe`.
    subscribed_topics: Vec<String>,
    /// Membership state; `None` until the first heartbeat is attempted.
    group: Option<ShareGroupState>,
    /// When the last `ShareGroupHeartbeat` was sent.
    last_heartbeat: Option<Instant>,
    /// The partitions the coordinator assigned. Unlike a consumer group this is
    /// not exclusive ownership — several members can hold the same partition.
    assignment: Vec<TopicPartition>,
    /// Per-broker share sessions, keyed by leader broker id.
    sessions: HashMap<i32, ShareSession>,
    /// Acknowledgements collected but not yet sent, keyed by partition.
    pending: Acknowledgements,
    /// Offsets handed to the caller by the last `poll` that have not been
    /// acknowledged yet, keyed by partition.
    delivered: HashMap<TopicPartition, HashSet<i64>>,
    /// The acquisition-lock budget the broker reported on the last response
    /// that acquired anything (Java's `acquisitionLockTimeoutMs`).
    acquisition_lock_timeout: Option<Duration>,
    /// Where acknowledgement verdicts go when the caller asked for them.
    acknowledgement_callback: Option<AcknowledgementCommitCallback>,
    /// Native request/record counters (Java's `ShareConsumer.metrics()`).
    metrics: ConsumerMetrics,
}

/// A callback for the broker's verdict on a batch of acknowledgements, the
/// analogue of Java's `AcknowledgementCommitCallback`.
pub type AcknowledgementCommitCallback = Arc<dyn Fn(&[(TopicPartition, Result<()>)]) + Send + Sync>;

impl std::fmt::Debug for ShareConsumer {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ShareConsumer")
            .field("config", &self.config)
            .field("subscribed_topics", &self.subscribed_topics)
            .field("assignment", &self.assignment)
            .field("coordinator_id", &self.coordinator_id)
            .field("sessions", &self.sessions)
            .field("pending", &self.pending)
            .field("delivered", &self.delivered)
            .field("acquisition_lock_timeout", &self.acquisition_lock_timeout)
            .field(
                "acknowledgement_callback",
                &self.acknowledgement_callback.is_some(),
            )
            .finish_non_exhaustive()
    }
}

impl ShareConsumer {
    /// Build a share consumer from public typed Kafka config.
    ///
    /// # Errors
    /// Returns an error when runtime config validation fails, bootstrap DNS
    /// resolution fails, or no bootstrap endpoint resolves to a socket address.
    pub async fn from_config(config: ConsumerConfig) -> Result<Self> {
        let runtime = ShareRuntimeConfig::from_config(&config)?;
        let endpoints = resolve_bootstrap_brokers(&config).await?;
        let connection = config
            .to_connection_config()
            .map_err(|error| ConsumerError::Config { error })?;
        let wire =
            WireClient::connect_with_brokers(connection, config.client_id.clone(), endpoints);
        Ok(Self {
            wire,
            config: runtime,
            wakeup: Arc::new(AtomicBool::new(false)),
            coordinator_id: None,
            subscribed_topics: Vec::new(),
            group: None,
            last_heartbeat: None,
            assignment: Vec::new(),
            sessions: HashMap::new(),
            pending: Acknowledgements::new(),
            delivered: HashMap::new(),
            acquisition_lock_timeout: None,
            acknowledgement_callback: None,
            metrics: ConsumerMetrics::default(),
        })
    }

    /// Build a share consumer from an owned Kafka [`ClientConfig`].
    ///
    /// # Errors
    /// As [`from_config`](Self::from_config).
    pub async fn new(config: ClientConfig) -> Result<Self> {
        Self::from_client_config(&config).await
    }

    /// Build a share consumer from a borrowed Kafka [`ClientConfig`].
    ///
    /// # Errors
    /// As [`from_config`](Self::from_config).
    pub async fn from_client_config(config: &ClientConfig) -> Result<Self> {
        let config = config
            .consumer_config()
            .map_err(|error| ConsumerError::Config { error })?;
        Self::from_config(config).await
    }

    /// Build a share consumer from `Properties`-style entries.
    ///
    /// # Errors
    /// As [`from_config`](Self::from_config).
    pub async fn from_properties(properties: Properties) -> Result<Self> {
        Self::from_client_config(&ClientConfig::from(properties)).await
    }

    /// Build a share consumer from a map/iterator of Kafka config entries.
    ///
    /// # Errors
    /// As [`from_config`](Self::from_config).
    pub async fn from_map<I, K, V>(entries: I) -> Result<Self>
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<ConfigKey>,
        V: Into<ConfigValue>,
    {
        let config: ClientConfig = entries.into_iter().collect();
        Self::from_client_config(&config).await
    }

    /// Subscribe to a set of topics, joining the share group on the next
    /// [`poll`](Self::poll). Replaces any prior subscription.
    ///
    /// There is no manual-assignment or pattern form: a share group has no
    /// client-side assignor, and the coordinator assigns from the subscribed
    /// topic names alone.
    ///
    /// # Errors
    /// Returns [`ConsumerError::InvalidState`] if `group.id` is unset.
    pub fn subscribe(&mut self, topics: impl IntoIterator<Item = impl Into<String>>) -> Result<()> {
        if self.config.base.group_id.is_empty() {
            return Err(ConsumerError::InvalidState(
                "group.id must be set to subscribe to topics",
            ));
        }
        let mut topics: Vec<String> = topics.into_iter().map(Into::into).collect();
        topics.sort();
        topics.dedup();
        self.subscribed_topics = topics;
        self.assignment.clear();
        Ok(())
    }

    /// Unsubscribe from all topics and drop the current assignment. Does not
    /// leave the group or acknowledge anything; call [`close`](Self::close) for
    /// that.
    pub fn unsubscribe(&mut self) {
        self.subscribed_topics.clear();
        self.assignment.clear();
    }

    /// The topics this consumer is subscribed to.
    #[must_use]
    pub fn subscription(&self) -> Vec<String> {
        self.subscribed_topics.clone()
    }

    /// The partitions the coordinator currently assigns to this member.
    ///
    /// Unlike a consumer group's assignment this is not exclusive: other members
    /// of the same share group may hold the same partitions, each acquiring a
    /// disjoint set of its records.
    #[must_use]
    pub fn assignment(&self) -> Vec<TopicPartition> {
        self.assignment.clone()
    }

    /// The acknowledgement mode in force (`share.acknowledgement.mode`).
    #[must_use]
    pub const fn acknowledgement_mode(&self) -> ShareAcknowledgementMode {
        self.config.acknowledgement_mode
    }

    /// A snapshot of this consumer's native counters.
    #[must_use]
    pub fn metrics(&self) -> ConsumerMetricsSnapshot {
        self.metrics.snapshot(self.wire.buffer_pool_stats())
    }

    /// How long the broker holds the acquisition lock on delivered records —
    /// the budget an application has to process and acknowledge a batch before
    /// those records become re-deliverable. Kafka's
    /// `ShareConsumer.acquisitionLockTimeoutMs`.
    ///
    /// `None` until a `poll` has actually acquired something: the broker reports
    /// this per response, not at subscribe time.
    #[must_use]
    pub const fn acquisition_lock_timeout(&self) -> Option<Duration> {
        self.acquisition_lock_timeout
    }

    /// The broker-assigned client instance id (`GetTelemetrySubscriptions`,
    /// Kafka's `clientInstanceId`).
    ///
    /// # Errors
    /// Returns a wire/broker error, or a broker error with
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

    /// Fetch the records the broker acquires for this member, waiting up to
    /// `timeout`.
    ///
    /// Every record returned is locked to this member until it is acknowledged
    /// or its acquisition lock expires, so — unlike
    /// [`Consumer::poll`](crate::consumer::Consumer::poll) — the whole
    /// acquisition is handed over at once rather than sliced by
    /// `max.poll.records`; that config bounds what the broker acquires instead.
    /// With `share.acquire.mode=batch_optimized` (the default) a poll may exceed
    /// it to land on record-batch boundaries.
    ///
    /// A poll first settles the previous batch: under
    /// [`Implicit`](ShareAcknowledgementMode::Implicit) every record it returned
    /// is accepted, and under [`Explicit`](ShareAcknowledgementMode::Explicit)
    /// leaving one unacknowledged is an error.
    ///
    /// # Errors
    /// Returns [`ConsumerError::Wakeup`] if [`wakeup`](Self::wakeup) was called,
    /// [`ConsumerError::InvalidState`] if the consumer is not subscribed or a
    /// record from the previous batch is unacknowledged in explicit mode, or a
    /// wire/broker error.
    pub async fn poll(&mut self, timeout: Duration) -> Result<ShareRecords> {
        self.check_wakeup()?;
        if self.subscribed_topics.is_empty() {
            return Err(ConsumerError::InvalidState(
                "share consumer is not subscribed to any topics",
            ));
        }
        self.metrics.record_poll();
        let start = Instant::now();
        self.settle_delivered_batch()?;

        loop {
            self.ensure_active_group().await?;

            if !self.assignment.is_empty() {
                let metadata = self
                    .wire
                    .metadata_for_topics(self.subscribed_topics.clone())
                    .await?;
                let remaining = timeout.saturating_sub(start.elapsed());
                let records = match self.fetch_round(&metadata, remaining).await {
                    Ok(records) => records,
                    Err(error) => {
                        self.abandon_delivered_batch();
                        return Err(error);
                    },
                };
                if !records.is_empty() {
                    self.metrics.record_records(records.count());
                    return Ok(records);
                }
            }

            self.check_wakeup()?;
            if start.elapsed() >= timeout {
                return Ok(ShareRecords::empty());
            }
            // Nothing assigned yet (or nothing acquired): back off so an
            // unassigned member does not spin on the coordinator.
            tokio::time::sleep(idle_backoff(
                self.config.base.retry_backoff,
                timeout,
                start.elapsed(),
            ))
            .await;
        }
    }

    /// Acknowledge one record as consumed successfully — the analogue of Java's
    /// one-argument `acknowledge(record)`, which also defaults to
    /// [`AcknowledgeType::Accept`].
    ///
    /// # Errors
    /// As [`acknowledge`](Self::acknowledge).
    pub fn accept(&mut self, record: &ShareRecord) -> Result<()> {
        self.acknowledge(record, AcknowledgeType::Accept)
    }

    /// Acknowledge one record from the batch the last [`poll`](Self::poll)
    /// returned. The acknowledgement is batched and reaches the broker on the
    /// next poll or [`commit`](Self::commit).
    ///
    /// # Errors
    /// Returns [`ConsumerError::InvalidState`] when
    /// `share.acknowledgement.mode=implicit` (the consumer acknowledges for you)
    /// or when the record is not part of the batch the last poll returned —
    /// already acknowledged, or from an earlier batch whose lock this member no
    /// longer holds.
    pub fn acknowledge(&mut self, record: &ShareRecord, kind: AcknowledgeType) -> Result<()> {
        self.acknowledge_offset(&record.topic_partition(), record.offset(), kind)
    }

    /// [`acknowledge`](Self::acknowledge) addressed by coordinates instead of by
    /// record — the analogue of Java's
    /// `acknowledge(topic, partition, offset, type)`.
    ///
    /// # Errors
    /// As [`acknowledge`](Self::acknowledge).
    pub fn acknowledge_offset(
        &mut self,
        partition: &TopicPartition,
        offset: i64,
        kind: AcknowledgeType,
    ) -> Result<()> {
        if self.config.acknowledgement_mode == ShareAcknowledgementMode::Implicit {
            return Err(ConsumerError::InvalidState(
                "acknowledge() requires share.acknowledgement.mode=explicit",
            ));
        }
        let known = self
            .delivered
            .get_mut(partition)
            .is_some_and(|offsets| offsets.remove(&offset));
        if !known {
            return Err(ConsumerError::InvalidState(
                "record was not delivered by the last poll of this share consumer",
            ));
        }
        self.record_acknowledgement(partition, offset, kind.wire());
        Ok(())
    }

    /// Send every pending acknowledgement to the brokers, blocking until they
    /// answer — the share-group analogue of `commit_sync`.
    ///
    /// Like [`poll`](Self::poll) this first settles the batch the last poll
    /// returned: implicit mode accepts it, explicit mode requires it to be fully
    /// acknowledged.
    ///
    /// # Errors
    /// Returns [`ConsumerError::InvalidState`] when a record from the last batch
    /// is unacknowledged in explicit mode, or a wire/broker error. An
    /// acknowledgement the broker rejects (its acquisition lock had already
    /// expired, say) surfaces as [`ConsumerError::Broker`]; those records keep
    /// their lock and are redelivered.
    pub async fn commit(&mut self) -> Result<()> {
        let timeout = self.config.base.request_timeout;
        self.commit_timeout(timeout).await
    }

    /// [`commit`](Self::commit) with a caller-chosen bound — Java's
    /// `commitSync(Duration)`.
    ///
    /// On timeout the acknowledgements have still left this client and may have
    /// applied at the broker, so they are not retried: whatever did not apply
    /// keeps its acquisition lock and is redelivered.
    ///
    /// # Errors
    /// As [`commit`](Self::commit), plus [`ConsumerError::Wire`] with
    /// [`WireError::Timeout`] when the bound elapses first.
    pub async fn commit_timeout(&mut self, timeout: Duration) -> Result<()> {
        self.settle_delivered_batch()?;
        if self.pending.is_empty() {
            return Ok(());
        }
        let metadata = self
            .wire
            .metadata_for_topics(self.subscribed_topics.clone())
            .await?;
        let flushed = tokio::time::timeout(timeout, self.flush_acknowledgements(&metadata, false))
            .await
            .unwrap_or(Err(ConsumerError::Wire(WireError::Timeout)));
        flushed?;
        self.metrics.record_commit();
        Ok(())
    }

    /// Settle the batch the last [`poll`](Self::poll) returned without waiting
    /// for the broker — the analogue of Java's `commitAsync()`.
    ///
    /// This sends nothing on its own, and that is the point: a share session is
    /// a strictly ordered epoch sequence per broker, so a second request racing
    /// the poll loop would invalidate the session rather than speed anything up.
    /// Java does the same — `commitAsync` hands the acknowledgements to the
    /// network thread, which piggy-backs them onto the next `ShareFetch`. Here
    /// they ride the next [`poll`](Self::poll), and the broker's verdict on them
    /// reaches the
    /// [acknowledgement callback](Self::set_acknowledgement_commit_callback).
    ///
    /// # Errors
    /// Returns [`ConsumerError::InvalidState`] when a record from the last batch
    /// is unacknowledged in explicit acknowledgement mode.
    pub fn commit_async(&mut self) -> Result<()> {
        self.settle_delivered_batch()
    }

    /// Register a callback for the broker's verdict on acknowledgements, the
    /// analogue of Java's `setAcknowledgementCommitCallback`.
    ///
    /// It fires once per response that carried acknowledgements, with the
    /// per-partition outcome. Registering one also changes how a rejected
    /// acknowledgement is reported: without a callback a rejection fails the
    /// [`poll`](Self::poll) or [`commit`](Self::commit) that carried it, because
    /// silently dropping it would hide that those records are about to be
    /// redelivered; with a callback the failure goes there instead and the poll
    /// keeps returning records, which is Java's behaviour.
    pub fn set_acknowledgement_commit_callback(
        &mut self,
        callback: impl Fn(&[(TopicPartition, Result<()>)]) + Send + Sync + 'static,
    ) {
        self.acknowledgement_callback = Some(Arc::new(callback));
    }

    /// Interrupt a blocking [`poll`](Self::poll). The next (or in-flight) poll
    /// returns [`ConsumerError::Wakeup`].
    pub fn wakeup(&self) {
        self.wakeup.store(true, Ordering::SeqCst);
    }

    /// Close the consumer: acknowledge what is pending, close the share sessions
    /// so acquired-but-unacknowledged records are released immediately rather
    /// than at lock expiry, and leave the group. Bounded by `request.timeout.ms`.
    pub async fn close(self) {
        let timeout = self.config.base.request_timeout;
        self.close_timeout(timeout).await;
    }

    /// [`close`](Self::close) with a caller-chosen bound on the final
    /// acknowledge-and-leave work. A zero timeout skips it and just releases
    /// resources, leaving acquired records to their acquisition lock.
    pub async fn close_timeout(mut self, timeout: Duration) {
        let _timed_out = tokio::time::timeout(timeout, self.acknowledge_and_leave()).await;
        drop(self);
    }

    /// Acknowledge pending records, close the share sessions, and leave the
    /// group. Best effort: a broker that will not answer must not hang `close`.
    async fn acknowledge_and_leave(&mut self) {
        // Implicit mode owes an accept for whatever the last poll handed out;
        // explicit mode leaves unacknowledged records to their lock, which the
        // session close below releases anyway.
        if self.config.acknowledgement_mode == ShareAcknowledgementMode::Implicit {
            self.accept_delivered_batch();
        }
        self.delivered.clear();

        if let Ok(metadata) = self
            .wire
            .metadata_for_topics(self.subscribed_topics.clone())
            .await
        {
            let _flushed = self.flush_acknowledgements(&metadata, true).await;
        }

        let (Some(state), Some(coordinator)) = (self.group.as_ref(), self.coordinator_id) else {
            return;
        };
        let member_id = state.member_id.clone();
        let _left = heartbeat(
            &self.wire,
            coordinator,
            &ShareHeartbeatRequest {
                group_id: &self.config.base.group_id,
                member_id: &member_id,
                member_epoch: EPOCH_LEAVING,
                rack_id: None,
                subscribed_topics: &[],
            },
        )
        .await;
    }

    /// Send a `ShareGroupHeartbeat` when one is due and adopt the assignment the
    /// coordinator replies with.
    async fn ensure_active_group(&mut self) -> Result<()> {
        let interval = self
            .group
            .as_ref()
            .map_or(self.config.base.heartbeat_interval, |state| {
                state.heartbeat_interval
            });
        let due = self.group.is_none()
            || self
                .last_heartbeat
                .is_none_or(|last| last.elapsed() >= interval);
        if !due {
            return Ok(());
        }

        let group_id = self.config.base.group_id.clone();
        let coordinator = self.ensure_coordinator(&group_id).await?;
        if self.group.is_none() {
            self.group = Some(ShareGroupState::new(self.config.base.heartbeat_interval)?);
        }
        let (member_id, member_epoch) = self
            .group
            .as_ref()
            .map(|state| (state.member_id.clone(), state.member_epoch))
            .unwrap_or_default();
        let rack = (!self.config.base.client_rack.is_empty())
            .then_some(self.config.base.client_rack.as_str());

        let outcome = match heartbeat(
            &self.wire,
            coordinator,
            &ShareHeartbeatRequest {
                group_id: &group_id,
                member_id: &member_id,
                member_epoch,
                rack_id: rack,
                subscribed_topics: &self.subscribed_topics,
            },
        )
        .await
        {
            Ok(outcome) => outcome,
            Err(error) => {
                // A dead coordinator never answers `NOT_COORDINATOR`; it just
                // times out, so re-discover it rather than pinning a dead one.
                if matches!(
                    error,
                    ConsumerError::Wire(
                        WireError::Timeout | WireError::ConnectionClosed | WireError::Io(_)
                    )
                ) {
                    self.coordinator_id = None;
                }
                return Err(error);
            },
        };
        self.last_heartbeat = Some(Instant::now());
        self.metrics.record_heartbeat();

        match outcome.error {
            ErrorCode::None => {
                if let Some(state) = self.group.as_mut() {
                    state.member_epoch = outcome.member_epoch;
                    if outcome.heartbeat_interval > Duration::ZERO {
                        state.heartbeat_interval = outcome.heartbeat_interval;
                    }
                    if let Some(id) = outcome.member_id.filter(|id| !id.is_empty()) {
                        state.member_id = id;
                    }
                }
                if let Some(assignment) = outcome.assignment {
                    let metadata = self
                        .wire
                        .metadata_for_topics(self.subscribed_topics.clone())
                        .await?;
                    self.apply_assignment(&metadata, &assignment);
                    self.metrics.record_rebalance();
                }
            },
            // Lost membership: the broker has already released everything this
            // member held, so drop the sessions and rejoin from epoch 0.
            ErrorCode::FencedMemberEpoch => {
                if let Some(state) = self.group.as_mut() {
                    state.member_epoch = EPOCH_JOINING;
                }
                self.forget_acquisitions();
            },
            // The coordinator forgot us — start over with a fresh member id,
            // which is also a fresh share-session identity.
            ErrorCode::UnknownMemberId => {
                self.group = Some(ShareGroupState::new(self.config.base.heartbeat_interval)?);
                self.forget_acquisitions();
            },
            code if code.is_retriable() => {
                self.coordinator_id = None;
            },
            code => {
                return Err(ConsumerError::broker(
                    "share_group_heartbeat",
                    code,
                    "share group heartbeat failed",
                ));
            },
        }
        Ok(())
    }

    /// Adopt a coordinator-computed assignment, resolving its topic ids to names.
    fn apply_assignment(
        &mut self,
        metadata: &ClusterMetadata,
        assignment: &[crate::consumer::next_gen::AssignedTopic],
    ) {
        let mut target = Vec::new();
        for topic in assignment {
            let Some(name) = topic_name_for_id(metadata, topic.topic_id) else {
                continue;
            };
            for partition in &topic.partitions {
                target.push(TopicPartition::new(name.clone(), *partition));
            }
        }
        // Acknowledgements for partitions that left the assignment can no longer
        // be delivered: this member does not hold their locks any more.
        self.pending
            .retain(|partition, _| target.contains(partition));
        self.delivered
            .retain(|partition, _| target.contains(partition));
        self.assignment = target;
    }

    /// Run one round of `ShareFetch` across every broker leading an assigned
    /// partition, piggy-backing the pending acknowledgements for its partitions.
    async fn fetch_round(
        &mut self,
        metadata: &ClusterMetadata,
        remaining: Duration,
    ) -> Result<ShareRecords> {
        let by_leader = self.group_by_leader(metadata);
        let topic_names = topic_names_by_id(metadata);
        let Some(member_id) = self.group.as_ref().map(|state| state.member_id.clone()) else {
            return Ok(ShareRecords::empty());
        };
        let group_id = self.config.base.group_id.clone();
        let max_wait_ms = clamp_ms(remaining).min(self.config.base.fetch_max_wait_ms);

        let mut records = ShareRecords::empty();
        let mut stale = Vec::new();
        let mut acknowledge_error = None;
        for (leader, wanted) in by_leader {
            let outcome = self
                .fetch_from_leader(
                    leader,
                    &wanted,
                    &ShareFetchRound {
                        group_id: &group_id,
                        member_id: &member_id,
                        max_wait_ms,
                    },
                    &topic_names,
                )
                .await?;
            stale.extend(outcome.stale);
            if outcome.acquisition_lock_timeout.is_some() {
                self.acquisition_lock_timeout = outcome.acquisition_lock_timeout;
            }
            if acknowledge_error.is_none() {
                acknowledge_error = outcome.acknowledge_error;
            }
            for (partition, acquired) in outcome.partitions {
                // Gaps never reach the application, so they are owed their
                // acknowledgement right away.
                for (offset, kind) in gap_acknowledgements(&acquired.gaps) {
                    self.record_acknowledgement(&partition, offset, kind);
                }
                if acquired.records.is_empty() {
                    continue;
                }
                self.remember_delivered(&partition, &acquired.records);
                records.push_partition(
                    partition.topic.clone(),
                    partition.partition,
                    acquired.records,
                );
            }
        }

        for partition in stale {
            self.wire
                .invalidate_topic_partition(&partition.topic, partition.partition);
        }
        if let Some((partition, error)) = acknowledge_error {
            let rejected = ConsumerError::broker(
                "share_acknowledge",
                error,
                format!(
                    "{}-{} rejected a piggybacked acknowledgement; those records keep their \
                     acquisition lock and will be redelivered",
                    partition.topic, partition.partition
                ),
            );
            // With a callback registered the verdict goes there and the poll
            // still returns its records (Java's shape). Without one, failing the
            // poll is the only way the caller learns those records are coming
            // back — dropping it silently would not be honest.
            match self.acknowledgement_callback.clone() {
                Some(callback) => callback(&[(partition, Err(rejected))]),
                None => return Err(rejected),
            }
        }
        Ok(records)
    }

    /// Send one broker's `ShareFetch` and decode it.
    ///
    /// A request that did not land — an unreachable leader, or a share session
    /// the broker no longer knows — yields an empty outcome instead of an error;
    /// the session re-opens on the next poll.
    async fn fetch_from_leader(
        &mut self,
        leader: i32,
        wanted: &[TopicIdPartition],
        round: &ShareFetchRound<'_>,
        topic_names: &HashMap<KafkaUuid, String>,
    ) -> Result<ShareFetchOutcome> {
        let version = share_api_version(
            self.wire.negotiated_version(leader, ApiKey::ShareFetch),
            ApiKey::ShareFetch,
        );
        let acquire_mode = self.config.acquire_mode_for_version(version);
        let min_bytes = self.config.base.fetch_min_bytes;
        let max_bytes = self.config.base.fetch_max_bytes;
        let max_records = clamp_i32(self.config.base.max_poll_records);
        // A session-opening request must not carry acknowledgements: the broker
        // rejects that outright, and a session that had to be re-opened has
        // already lost the acquisitions those would settle.
        let opening = self.sessions.entry(leader).or_default().is_new();
        let acknowledgements = if opening {
            for (partition, _) in wanted {
                let _dropped = self.pending.remove(partition);
            }
            Acknowledgements::new()
        } else {
            take_acknowledgements(&mut self.pending, wanted)
        };
        let session = self.sessions.entry(leader).or_default();
        let request = build_share_fetch(
            session,
            &ShareFetchPlan {
                group_id: round.group_id,
                member_id: round.member_id,
                max_wait_ms: round.max_wait_ms,
                min_bytes,
                max_bytes,
                max_records,
                acquire_mode,
                wanted,
                acknowledgements: &acknowledgements,
            },
        );
        self.metrics.record_fetch();

        let response: ShareFetchResponseData = match self
            .wire
            .send_to_broker(leader, ApiKey::ShareFetch, version, &request)
            .await
        {
            Ok(response) => response,
            // One unreachable leader must not fail the whole poll. The broker
            // has lost our session either way, so re-open it and let the next
            // poll retry; the acquisitions it held are released at lock expiry.
            Err(error) if !error.is_fatal_setup() => {
                self.reset_session(leader, wanted);
                return Ok(ShareFetchOutcome {
                    stale: wanted
                        .iter()
                        .map(|(partition, _)| partition.clone())
                        .collect(),
                    ..ShareFetchOutcome::default()
                });
            },
            Err(error) => return Err(error.into()),
        };

        let top_level = ErrorCode::from(response.error_code);
        if is_session_lost(top_level) {
            // The broker released everything it held for this session, so there
            // is nothing stale about the metadata — just re-open next poll.
            self.reset_session(leader, wanted);
            return Ok(ShareFetchOutcome::default());
        }
        self.advance_session(leader);
        if top_level.is_error() {
            return Err(ConsumerError::broker(
                "share_fetch",
                top_level,
                "share fetch request rejected",
            ));
        }
        decode_share_fetch(response, topic_names)
    }

    /// Send every pending acknowledgement as a standalone `ShareAcknowledge`,
    /// one request per broker. With `close_sessions` the requests also carry the
    /// final share-session epoch, releasing anything still acquired.
    async fn flush_acknowledgements(
        &mut self,
        metadata: &ClusterMetadata,
        close_sessions: bool,
    ) -> Result<()> {
        let group_id = self.config.base.group_id.clone();
        let Some(member_id) = self.group.as_ref().map(|state| state.member_id.clone()) else {
            self.pending.clear();
            return Ok(());
        };
        let topic_ids = topic_ids_by_partition(metadata, &self.assignment);
        let mut by_leader: BTreeMap<i32, Vec<TopicIdPartition>> = BTreeMap::new();
        for (partition, topic_id) in &topic_ids {
            if let Some(leader) = partition_leader(metadata, &partition.topic, partition.partition)
            {
                by_leader
                    .entry(leader)
                    .or_default()
                    .push((partition.clone(), *topic_id));
            }
        }
        // A broker whose session was reset or never opened cannot be sent
        // acknowledgements, and closing an unopened session is a no-op.
        let leaders: Vec<i32> = if close_sessions {
            self.sessions.keys().copied().collect()
        } else {
            by_leader.keys().copied().collect()
        };

        let mut outcomes: Vec<(TopicPartition, Result<()>)> = Vec::new();
        for leader in leaders {
            let wanted = by_leader.get(&leader).cloned().unwrap_or_default();
            let acknowledgements = take_acknowledgements(&mut self.pending, &wanted);
            outcomes.extend(
                self.acknowledge_to_leader(
                    leader,
                    &AcknowledgeRound {
                        group_id: &group_id,
                        member_id: &member_id,
                        topic_ids: &topic_ids,
                        close_session: close_sessions,
                    },
                    &acknowledgements,
                )
                .await,
            );
        }
        // Acknowledgements are one-shot: whatever was not delivered is released
        // by the broker at lock expiry, so retrying them would double-send.
        self.pending.clear();
        if outcomes.is_empty() {
            return Ok(());
        }
        // A registered callback takes the verdict and the commit succeeds — the
        // acknowledgements did leave this client, and the caller asked to hear
        // about rejections there. Without one, the first failure is the return
        // value, because nothing else would tell the caller those records are
        // about to be redelivered.
        if let Some(callback) = self.acknowledgement_callback.clone() {
            callback(&outcomes);
            return Ok(());
        }
        outcomes
            .into_iter()
            .find_map(|(_partition, outcome)| outcome.err())
            .map_or(Ok(()), Err)
    }

    /// Send one broker's `ShareAcknowledge` and return its per-partition
    /// verdicts.
    ///
    /// A broker with no session, or one whose session was never opened, has
    /// nothing to acknowledge and nothing to close, so it yields no verdicts —
    /// the acknowledgements it would have carried are already moot broker-side.
    async fn acknowledge_to_leader(
        &mut self,
        leader: i32,
        round: &AcknowledgeRound<'_>,
        acknowledgements: &Acknowledgements,
    ) -> Vec<(TopicPartition, Result<()>)> {
        // The partitions this request speaks for, so a request-wide failure can
        // be reported against each of them rather than against nothing.
        let acknowledged: Vec<TopicPartition> = acknowledgements.keys().cloned().collect();
        let version = share_api_version(
            self.wire
                .negotiated_version(leader, ApiKey::ShareAcknowledge),
            ApiKey::ShareAcknowledge,
        );
        let Some(session) = self.sessions.get_mut(&leader) else {
            return Vec::new();
        };
        if session.is_new() {
            return Vec::new();
        }
        if round.close_session {
            session.close();
        } else if acknowledgements.is_empty() {
            return Vec::new();
        }
        let request = build_share_acknowledge(
            session,
            round.group_id,
            round.member_id,
            round.topic_ids,
            acknowledgements,
        );

        let response: ShareAcknowledgeResponseData = match self
            .wire
            .send_to_broker(leader, ApiKey::ShareAcknowledge, version, &request)
            .await
        {
            Ok(response) => response,
            Err(error) => {
                self.reset_session(leader, &[]);
                return fan_out(&acknowledged, &ConsumerError::from(error)).collect();
            },
        };

        let top_level = ErrorCode::from(response.error_code);
        if is_session_lost(top_level) {
            // The broker already released what this session held, so the
            // acknowledgements are moot rather than failed.
            self.reset_session(leader, &[]);
            return Vec::new();
        }
        if round.close_session {
            self.reset_session(leader, &[]);
        } else {
            self.advance_session(leader);
        }
        if top_level.is_error() {
            let error = ConsumerError::broker(
                "share_acknowledge",
                top_level,
                "share acknowledge request rejected",
            );
            return fan_out(&acknowledged, &error).collect();
        }
        partition_acknowledge_outcomes(&response, &acknowledged, round.topic_ids)
    }

    /// Settle the batch the last poll handed out, per acknowledgement mode.
    fn settle_delivered_batch(&mut self) -> Result<()> {
        match self.config.acknowledgement_mode {
            ShareAcknowledgementMode::Implicit => {
                self.accept_delivered_batch();
                self.delivered.clear();
                Ok(())
            },
            ShareAcknowledgementMode::Explicit => {
                if self.delivered.values().any(|offsets| !offsets.is_empty()) {
                    return Err(ConsumerError::InvalidState(
                        "all records must be acknowledged in explicit acknowledgement mode",
                    ));
                }
                self.delivered.clear();
                Ok(())
            },
        }
    }

    /// Forget the records booked as delivered by a `poll` that failed part-way.
    ///
    /// A round can acquire records from one broker and then fail on the next, so
    /// records may already be booked that the caller never receives. Left booked,
    /// the next `poll` would settle them — under implicit acknowledgement that
    /// means *accepting* records no application ever saw. Forgotten, they keep
    /// their acquisition lock and come back on a later delivery.
    ///
    /// Pending acknowledgements are kept: gaps and the acknowledgements already
    /// handed to a broker are owed regardless of how the round ended.
    fn abandon_delivered_batch(&mut self) {
        self.delivered.clear();
    }

    /// Accept every still-unacknowledged record from the last batch.
    fn accept_delivered_batch(&mut self) {
        let accepted: Vec<(TopicPartition, Vec<i64>)> = self
            .delivered
            .iter()
            .map(|(partition, offsets)| {
                let mut offsets: Vec<i64> = offsets.iter().copied().collect();
                offsets.sort_unstable();
                (partition.clone(), offsets)
            })
            .collect();
        for (partition, offsets) in accepted {
            for offset in offsets {
                self.record_acknowledgement(&partition, offset, AcknowledgeType::Accept.wire());
            }
        }
    }

    fn remember_delivered(&mut self, partition: &TopicPartition, records: &[ShareRecord]) {
        let offsets = self.delivered.entry(partition.clone()).or_default();
        for record in records {
            let _new = offsets.insert(record.offset());
        }
    }

    fn record_acknowledgement(&mut self, partition: &TopicPartition, offset: i64, kind: i8) {
        let _previous = self
            .pending
            .entry(partition.clone())
            .or_default()
            .insert(offset, kind);
    }

    /// Drop every share session and every acquisition this member believed it
    /// held — used when the coordinator fences or forgets the member, which
    /// releases its records broker-side.
    fn forget_acquisitions(&mut self) {
        self.sessions.clear();
        self.pending.clear();
        self.delivered.clear();
        self.assignment.clear();
    }

    fn reset_session(&mut self, leader: i32, wanted: &[TopicIdPartition]) {
        if let Some(session) = self.sessions.get_mut(&leader) {
            session.reset();
        }
        for (partition, _) in wanted {
            let _dropped = self.pending.remove(partition);
            let _dropped = self.delivered.remove(partition);
        }
    }

    fn advance_session(&mut self, leader: i32) {
        if let Some(session) = self.sessions.get_mut(&leader) {
            session.advance();
        }
    }

    /// The assigned partitions grouped by leader broker, with their topic ids.
    /// Partitions whose leader or topic id is not resolvable yet are skipped;
    /// the next poll re-resolves them.
    fn group_by_leader(&self, metadata: &ClusterMetadata) -> BTreeMap<i32, Vec<TopicIdPartition>> {
        let mut by_leader: BTreeMap<i32, Vec<TopicIdPartition>> = BTreeMap::new();
        for partition in &self.assignment {
            let (Some(leader), Some(topic_id)) = (
                partition_leader(metadata, &partition.topic, partition.partition),
                topic_id_for_name(metadata, &partition.topic),
            ) else {
                continue;
            };
            by_leader
                .entry(leader)
                .or_default()
                .push((partition.clone(), topic_id));
        }
        by_leader
    }

    async fn ensure_coordinator(&mut self, group_id: &str) -> Result<i32> {
        if let Some(id) = self.coordinator_id {
            return Ok(id);
        }
        let id = coordinator::find_coordinator(
            &self.wire,
            group_id,
            self.config.base.retry_backoff_policy(),
        )
        .await?;
        self.coordinator_id = Some(id);
        Ok(id)
    }

    fn check_wakeup(&self) -> Result<()> {
        if self.wakeup.swap(false, Ordering::SeqCst) {
            return Err(ConsumerError::Wakeup);
        }
        Ok(())
    }
}

/// The per-round `ShareFetch` inputs that are the same for every broker.
struct ShareFetchRound<'a> {
    group_id: &'a str,
    member_id: &'a str,
    max_wait_ms: i32,
}

/// The per-round `ShareAcknowledge` inputs that are the same for every broker.
struct AcknowledgeRound<'a> {
    group_id: &'a str,
    member_id: &'a str,
    topic_ids: &'a HashMap<TopicPartition, KafkaUuid>,
    /// Whether the request also closes the broker's share session, releasing
    /// anything still acquired instead of waiting out its lock.
    close_session: bool,
}

/// Move the acknowledgements for `wanted`'s partitions out of the pending map.
fn take_acknowledgements(
    pending: &mut Acknowledgements,
    wanted: &[TopicIdPartition],
) -> Acknowledgements {
    let mut taken = Acknowledgements::new();
    for (partition, _) in wanted {
        if let Some(acknowledgements) = pending.remove(partition) {
            let _previous = taken.insert(partition.clone(), acknowledgements);
        }
    }
    taken
}

/// Report the same failure against every partition a request spoke for.
///
/// A wire failure or a top-level rejection says nothing about which partition
/// was at fault, and all of them are equally unacknowledged, so each one gets
/// the verdict rather than the information being dropped.
fn fan_out<'a>(
    partitions: &'a [TopicPartition],
    error: &'a ConsumerError,
) -> impl Iterator<Item = (TopicPartition, Result<()>)> + 'a {
    partitions.iter().map(move |partition| {
        (
            partition.clone(),
            Err(ConsumerError::broker(
                "share_acknowledge",
                acknowledge_error_code(error),
                format!(
                    "{}-{} could not be acknowledged ({error}); those records keep their \
                     acquisition lock and will be redelivered",
                    partition.topic, partition.partition
                ),
            )),
        )
    })
}

/// The broker code behind an acknowledgement failure, or `None` for a failure
/// that never reached a broker (a wire error).
const fn acknowledge_error_code(error: &ConsumerError) -> ErrorCode {
    match error {
        ConsumerError::Broker { error, .. } => *error,
        _ => ErrorCode::None,
    }
}

/// Per-partition verdicts from a `ShareAcknowledge` response: a partition the
/// broker rejected gets its error, every other acknowledged partition gets `Ok`.
fn partition_acknowledge_outcomes(
    response: &ShareAcknowledgeResponseData,
    acknowledged: &[TopicPartition],
    topic_ids: &HashMap<TopicPartition, KafkaUuid>,
) -> Vec<(TopicPartition, Result<()>)> {
    // Keyed by (topic id, partition): the response is topic-id keyed, and two
    // topics routinely share a partition index.
    let mut rejected: HashMap<(KafkaUuid, i32), ErrorCode> = HashMap::new();
    for topic in &response.responses {
        for partition in &topic.partitions {
            let partition: &AcknowledgePartitionData = partition;
            let code = ErrorCode::from(partition.error_code);
            if code.is_error() {
                let _previous = rejected.insert((topic.topic_id, partition.partition_index), code);
            }
        }
    }
    acknowledged
        .iter()
        .map(|partition| {
            let outcome = topic_ids
                .get(partition)
                .and_then(|topic_id| rejected.get(&(*topic_id, partition.partition)))
                .map(|code| {
                    ConsumerError::broker(
                        "share_acknowledge",
                        *code,
                        format!(
                            "{}-{} rejected an acknowledgement; those records keep their \
                             acquisition lock and will be redelivered",
                            partition.topic, partition.partition
                        ),
                    )
                });
            (partition.clone(), outcome.map_or(Ok(()), Err))
        })
        .collect()
}

fn topic_name_for_id(metadata: &ClusterMetadata, topic_id: KafkaUuid) -> Option<String> {
    metadata
        .topics
        .iter()
        .find(|topic| topic.topic_id == topic_id)
        .map(|topic| topic.name.clone())
}

fn topic_id_for_name(metadata: &ClusterMetadata, name: &str) -> Option<KafkaUuid> {
    metadata
        .topic(name)
        .map(|topic| topic.topic_id)
        .filter(|id| !id.is_nil())
}

fn topic_names_by_id(metadata: &ClusterMetadata) -> HashMap<KafkaUuid, String> {
    metadata
        .topics
        .iter()
        .filter(|topic| !topic.topic_id.is_nil())
        .map(|topic| (topic.topic_id, topic.name.clone()))
        .collect()
}

fn topic_ids_by_partition(
    metadata: &ClusterMetadata,
    assignment: &[TopicPartition],
) -> HashMap<TopicPartition, KafkaUuid> {
    assignment
        .iter()
        .filter_map(|partition| {
            topic_id_for_name(metadata, &partition.topic)
                .map(|topic_id| (partition.clone(), topic_id))
        })
        .collect()
}

fn clamp_ms(duration: Duration) -> i32 {
    i32::try_from(duration.as_millis()).unwrap_or(i32::MAX)
}

fn clamp_i32(value: usize) -> i32 {
    i32::try_from(value).unwrap_or(i32::MAX)
}

#[cfg(test)]
mod tests {
    use super::{super::session::PartitionAcknowledgements, *};

    async fn share_consumer(extra: &[(&str, &str)]) -> ShareConsumer {
        let mut entries: Vec<(&str, &str)> = vec![
            ("bootstrap.servers", "127.0.0.1:9092"),
            ("group.id", "work-queue"),
        ];
        entries.extend_from_slice(extra);
        ShareConsumer::from_map(entries)
            .await
            .expect("share consumer builds against an unreachable broker")
    }

    fn record(topic: &str, partition: i32, offset: i64) -> ShareRecord {
        ShareRecord {
            record: crate::consumer::ConsumerRecord {
                topic: Arc::from(topic),
                partition,
                offset,
                timestamp: 0,
                timestamp_type: crate::consumer::TimestampType::CreateTime,
                key: None,
                value: None,
                headers: Vec::new(),
                leader_epoch: None,
            },
            delivery_count: 1,
        }
    }

    #[tokio::test]
    async fn subscribe_requires_a_group_id_and_replaces_the_topic_set() {
        let mut consumer = ShareConsumer::from_map([("bootstrap.servers", "127.0.0.1:9092")])
            .await
            .expect("builds");
        assert!(matches!(
            consumer.subscribe(["jobs"]),
            Err(ConsumerError::InvalidState(_))
        ));

        let mut consumer = share_consumer(&[]).await;
        consumer.subscribe(["b", "a", "a"]).expect("subscribes");
        assert_eq!(
            consumer.subscription(),
            vec!["a".to_owned(), "b".to_owned()]
        );
        consumer.subscribe(["c"]).expect("resubscribes");
        assert_eq!(consumer.subscription(), vec!["c".to_owned()]);
        consumer.unsubscribe();
        assert!(consumer.subscription().is_empty());
    }

    #[tokio::test]
    async fn poll_without_a_subscription_is_an_error() {
        let mut consumer = share_consumer(&[]).await;
        assert!(matches!(
            consumer.poll(Duration::from_millis(1)).await,
            Err(ConsumerError::InvalidState(_))
        ));
    }

    #[tokio::test]
    async fn wakeup_interrupts_the_next_poll_once() {
        let mut consumer = share_consumer(&[]).await;
        consumer.subscribe(["jobs"]).expect("subscribes");
        consumer.wakeup();
        assert!(matches!(
            consumer.poll(Duration::from_millis(1)).await,
            Err(ConsumerError::Wakeup)
        ));
    }

    #[tokio::test]
    async fn implicit_mode_rejects_acknowledge_and_accepts_the_batch_itself() {
        let mut consumer = share_consumer(&[]).await;
        assert_eq!(
            consumer.acknowledgement_mode(),
            ShareAcknowledgementMode::Implicit
        );
        let partition = TopicPartition::new("jobs", 0);
        let delivered = vec![record("jobs", 0, 0), record("jobs", 0, 1)];
        consumer.remember_delivered(&partition, &delivered);

        assert!(matches!(
            consumer.acknowledge(&delivered[0], AcknowledgeType::Accept),
            Err(ConsumerError::InvalidState(_))
        ));

        consumer.settle_delivered_batch().expect("implicit settles");
        assert!(consumer.delivered.is_empty());
        assert_eq!(
            consumer.pending.get(&partition).expect("acknowledged"),
            &PartitionAcknowledgements::from([
                (0, AcknowledgeType::Accept.wire()),
                (1, AcknowledgeType::Accept.wire()),
            ])
        );
    }

    #[tokio::test]
    async fn explicit_mode_requires_every_delivered_record_to_be_acknowledged() {
        let mut consumer = share_consumer(&[("share.acknowledgement.mode", "explicit")]).await;
        let partition = TopicPartition::new("jobs", 0);
        let delivered = vec![record("jobs", 0, 4), record("jobs", 0, 5)];
        consumer.remember_delivered(&partition, &delivered);

        // One acknowledged, one not: the next poll/commit must refuse.
        consumer
            .acknowledge(&delivered[0], AcknowledgeType::Accept)
            .expect("acknowledges");
        assert!(matches!(
            consumer.settle_delivered_batch(),
            Err(ConsumerError::InvalidState(_))
        ));

        consumer.remember_delivered(&partition, &delivered[1..]);
        consumer
            .acknowledge(&delivered[1], AcknowledgeType::Release)
            .expect("acknowledges");
        consumer.settle_delivered_batch().expect("all acknowledged");
        assert_eq!(
            consumer.pending.get(&partition).expect("acknowledged"),
            &PartitionAcknowledgements::from([
                (4, AcknowledgeType::Accept.wire()),
                (5, AcknowledgeType::Release.wire()),
            ])
        );
    }

    #[tokio::test]
    async fn acknowledging_twice_or_out_of_batch_is_an_error() {
        let mut consumer = share_consumer(&[("share.acknowledgement.mode", "explicit")]).await;
        let partition = TopicPartition::new("jobs", 0);
        let delivered = vec![record("jobs", 0, 0)];
        consumer.remember_delivered(&partition, &delivered);

        consumer
            .acknowledge(&delivered[0], AcknowledgeType::Accept)
            .expect("acknowledges");
        assert!(matches!(
            consumer.acknowledge(&delivered[0], AcknowledgeType::Accept),
            Err(ConsumerError::InvalidState(_))
        ));
        assert!(matches!(
            consumer.acknowledge_offset(&partition, 99, AcknowledgeType::Accept),
            Err(ConsumerError::InvalidState(_))
        ));
    }

    #[tokio::test]
    async fn an_unsupported_acknowledgement_mode_fails_construction() {
        let error = ShareConsumer::from_map([
            ("bootstrap.servers", "127.0.0.1:9092"),
            ("share.acknowledgement.mode", "whenever"),
        ])
        .await
        .expect_err("rejects the mode");
        assert!(matches!(
            error,
            ConsumerError::InvalidArgument {
                field: "share.acknowledgement.mode",
                ..
            }
        ));
    }

    #[tokio::test]
    async fn commit_without_pending_acknowledgements_short_circuits() {
        let mut consumer = share_consumer(&[]).await;
        // No membership, no acknowledgements: commit must not touch the network
        // (the bootstrap address is not listening).
        consumer.commit().await.expect("no-op commit");
    }

    #[tokio::test]
    async fn a_failed_round_never_leaves_undelivered_records_to_be_accepted() {
        let mut consumer = share_consumer(&[]).await;
        let partition = TopicPartition::new("jobs", 0);
        // A round that acquired records from one broker and then failed: the
        // records were booked, and a gap acknowledgement was already owed.
        consumer.remember_delivered(&partition, &[record("jobs", 0, 0)]);
        consumer.record_acknowledgement(&partition, 7, super::super::record::ACKNOWLEDGE_GAP);

        consumer.abandon_delivered_batch();

        // Nothing is left for the next poll to implicitly accept...
        consumer
            .settle_delivered_batch()
            .expect("nothing left to settle");
        assert!(
            consumer
                .pending
                .get(&partition)
                .is_none_or(|acknowledgements| !acknowledgements.contains_key(&0)),
            "a record the caller never saw must not be accepted"
        );
        // ...but the gap the broker is owed survives.
        assert_eq!(
            consumer.pending.get(&partition).expect("gap is still owed"),
            &PartitionAcknowledgements::from([(7, super::super::record::ACKNOWLEDGE_GAP)])
        );
    }

    #[tokio::test]
    async fn accept_is_the_one_argument_acknowledge() {
        let mut consumer = share_consumer(&[("share.acknowledgement.mode", "explicit")]).await;
        let partition = TopicPartition::new("jobs", 0);
        let delivered = vec![record("jobs", 0, 3)];
        consumer.remember_delivered(&partition, &delivered);

        consumer.accept(&delivered[0]).expect("accepts");
        assert_eq!(
            consumer.pending.get(&partition).expect("acknowledged"),
            &PartitionAcknowledgements::from([(3, AcknowledgeType::Accept.wire())])
        );
    }

    #[tokio::test]
    async fn commit_async_settles_the_batch_without_touching_the_network() {
        let mut consumer = share_consumer(&[]).await;
        let partition = TopicPartition::new("jobs", 0);
        consumer.remember_delivered(&partition, &[record("jobs", 0, 0)]);

        // Implicit mode: the batch is accepted and queued for the next request,
        // and nothing is sent (the bootstrap address is not listening).
        consumer.commit_async().expect("settles");
        assert!(consumer.delivered.is_empty());
        assert_eq!(
            consumer.pending.get(&partition).expect("queued"),
            &PartitionAcknowledgements::from([(0, AcknowledgeType::Accept.wire())])
        );
    }

    #[tokio::test]
    async fn commit_async_refuses_an_unacknowledged_batch_in_explicit_mode() {
        let mut consumer = share_consumer(&[("share.acknowledgement.mode", "explicit")]).await;
        consumer.remember_delivered(&TopicPartition::new("jobs", 0), &[record("jobs", 0, 0)]);
        assert!(matches!(
            consumer.commit_async(),
            Err(ConsumerError::InvalidState(_))
        ));
    }

    #[tokio::test]
    async fn the_acquisition_lock_timeout_is_unknown_until_something_is_acquired() {
        let mut consumer = share_consumer(&[]).await;
        assert_eq!(consumer.acquisition_lock_timeout(), None);
        // Set the way a `ShareFetch` response sets it.
        consumer.acquisition_lock_timeout = Some(Duration::from_secs(30));
        assert_eq!(
            consumer.acquisition_lock_timeout(),
            Some(Duration::from_secs(30))
        );
    }

    #[tokio::test]
    async fn a_registered_callback_takes_acknowledgement_verdicts() {
        use std::sync::Mutex;

        let mut consumer = share_consumer(&[]).await;
        let seen: Arc<Mutex<Vec<(String, bool)>>> = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&seen);
        consumer.set_acknowledgement_commit_callback(move |outcomes| {
            let mut sink = sink
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            sink.extend(outcomes.iter().map(|(partition, outcome)| {
                (
                    format!("{}-{}", partition.topic, partition.partition),
                    outcome.is_ok(),
                )
            }));
        });
        assert!(consumer.acknowledgement_callback.is_some());

        let partition = TopicPartition::new("jobs", 0);
        let error = ConsumerError::broker(
            "share_acknowledge",
            ErrorCode::InvalidRecordState,
            "lock expired",
        );
        let outcomes: Vec<(TopicPartition, Result<()>)> =
            fan_out(std::slice::from_ref(&partition), &error).collect();
        let callback = consumer
            .acknowledgement_callback
            .clone()
            .expect("registered");
        callback(&outcomes);

        let recorded = {
            let seen = seen
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            seen.clone()
        };
        assert_eq!(recorded.as_slice(), &[("jobs-0".to_owned(), false)]);
    }

    #[test]
    fn a_request_wide_failure_is_reported_against_every_partition_it_spoke_for() {
        let partitions = vec![
            TopicPartition::new("jobs", 0),
            TopicPartition::new("jobs", 1),
        ];
        let error = ConsumerError::Wire(WireError::Timeout);
        let outcomes: Vec<(TopicPartition, Result<()>)> = fan_out(&partitions, &error).collect();
        assert_eq!(outcomes.len(), 2);
        assert!(
            outcomes
                .iter()
                .all(|(_partition, outcome)| outcome.is_err())
        );
    }

    #[test]
    fn only_the_partitions_the_broker_rejected_come_back_as_errors() {
        use kacrab_protocol::generated::share_acknowledge_response::{
            PartitionData, ShareAcknowledgeTopicResponse,
        };

        let topic_id = KafkaUuid::from_parts(9, 9);
        let rejected = TopicPartition::new("jobs", 0);
        let accepted = TopicPartition::new("jobs", 1);
        let topic_ids: HashMap<TopicPartition, KafkaUuid> =
            HashMap::from([(rejected.clone(), topic_id), (accepted.clone(), topic_id)]);
        let response = ShareAcknowledgeResponseData {
            responses: vec![ShareAcknowledgeTopicResponse {
                topic_id,
                partitions: vec![PartitionData {
                    partition_index: 0,
                    error_code: ErrorCode::InvalidRecordState.code(),
                    ..PartitionData::default()
                }],
                _unknown_tagged_fields: Vec::new(),
            }],
            ..ShareAcknowledgeResponseData::default()
        };

        let outcomes = partition_acknowledge_outcomes(&response, &[rejected, accepted], &topic_ids);
        assert_eq!(outcomes.len(), 2);
        assert!(outcomes[0].1.is_err(), "the rejected partition reports it");
        assert!(outcomes[1].1.is_ok(), "the other one succeeded");
    }

    #[tokio::test]
    async fn losing_membership_forgets_every_acquisition() {
        let mut consumer = share_consumer(&[]).await;
        let partition = TopicPartition::new("jobs", 0);
        consumer.assignment = vec![partition.clone()];
        consumer.remember_delivered(&partition, &[record("jobs", 0, 0)]);
        consumer.record_acknowledgement(&partition, 0, AcknowledgeType::Accept.wire());
        let _session = consumer.sessions.entry(1).or_default();

        consumer.forget_acquisitions();
        assert!(consumer.sessions.is_empty());
        assert!(consumer.pending.is_empty());
        assert!(consumer.delivered.is_empty());
        assert!(consumer.assignment.is_empty());
    }

    #[tokio::test]
    async fn a_revoked_partition_drops_its_pending_acknowledgements() {
        let mut consumer = share_consumer(&[]).await;
        let kept = TopicPartition::new("jobs", 0);
        let revoked = TopicPartition::new("jobs", 1);
        consumer.record_acknowledgement(&kept, 0, AcknowledgeType::Accept.wire());
        consumer.record_acknowledgement(&revoked, 0, AcknowledgeType::Accept.wire());
        consumer.remember_delivered(&revoked, &[record("jobs", 1, 0)]);

        let metadata = ClusterMetadata {
            cluster_id: None,
            controller_id: 1,
            brokers: Vec::new(),
            topics: Vec::new(),
        };
        // An empty assignment revokes both; the surviving one is re-added below.
        consumer.assignment = vec![kept.clone(), revoked.clone()];
        consumer.apply_assignment(&metadata, &[]);
        assert!(consumer.pending.is_empty());
        assert!(consumer.delivered.is_empty());
        assert!(consumer.assignment.is_empty());
    }

    #[test]
    fn taking_acknowledgements_only_moves_the_requested_partitions() {
        let mut pending = Acknowledgements::new();
        let mine = TopicPartition::new("jobs", 0);
        let other = TopicPartition::new("jobs", 1);
        let _previous = pending.insert(mine.clone(), PartitionAcknowledgements::from([(0, 1)]));
        let _previous = pending.insert(other.clone(), PartitionAcknowledgements::from([(0, 1)]));

        let taken =
            take_acknowledgements(&mut pending, &[(mine.clone(), KafkaUuid::from_parts(1, 1))]);
        assert_eq!(taken.len(), 1);
        assert!(taken.contains_key(&mine));
        assert!(!pending.contains_key(&mine));
        assert!(pending.contains_key(&other));
    }
}
