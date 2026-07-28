//! Metadata cache lifecycle and recovery policy.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Duration, Instant},
};

use kacrab_protocol::generated::ErrorCode;

use super::{ClusterMetadata, MetadataTopicState, MetadataTopicStatus, TopicMetadata};
use crate::wire::{
    BrokerEndpoint, ConnectionConfig, MetadataRecoveryStrategy,
    backoff::{BackoffPolicy, BackoffState},
};

/// Recovery action after metadata cannot be obtained from known brokers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MetadataRecoveryAction {
    /// Return the metadata error to the caller.
    Fail,
    /// Restore the configured bootstrap endpoints and retry metadata discovery.
    Rebootstrap,
}

#[cfg(feature = "producer")]
#[derive(Clone, Copy)]
pub(crate) struct PartitionLeaderChange<'a> {
    pub(crate) topic: &'a str,
    pub(crate) partition_index: i32,
    pub(crate) leader_id: i32,
    pub(crate) leader_epoch: i32,
    pub(crate) leader_broker: Option<&'a super::BrokerMetadata>,
}

#[derive(Debug, Clone)]
struct MetadataSnapshot {
    metadata: Arc<ClusterMetadata>,
    updated_at: Instant,
}

/// Stateful metadata cache manager mirroring Kafka producer metadata lifecycle.
#[derive(Debug)]
pub(crate) struct MetadataManager {
    config: ConnectionConfig,
    snapshot: Option<MetadataSnapshot>,
    topic_last_used: HashMap<String, Instant>,
    /// When each cached topic was last carried by a metadata response. The
    /// snapshot is merged per topic, so `metadata.max.age.ms` has to be scored
    /// per topic too — otherwise a refresh for one topic would keep every other
    /// topic's stale entry alive forever.
    topic_updated_at: HashMap<String, Instant>,
    /// Partitions whose cached leadership is known to be wrong after a
    /// leadership error, keyed by topic. Kafka's `Metadata` RPC is topic-keyed,
    /// so any entry here forces a refetch of *that topic* — never of the whole
    /// cluster snapshot — while the partition keys let an in-place leader
    /// update (`apply_partition_leader_update`) retire the entry without an RPC.
    stale_partitions: HashMap<String, HashSet<i32>>,
    topic_errors: HashMap<String, ErrorCode>,
    invalid_topics: HashSet<String>,
    unauthorized_topics: HashSet<String>,
    internal_topics: HashSet<String>,
    bootstrap_endpoints: HashMap<i32, BrokerEndpoint>,
    no_usable_metadata_since: Option<Instant>,
    refresh_backoff: BackoffState,
    equivalent_response_backoff: BackoffState,
    next_refresh_allowed_at: Option<Instant>,
    update_requested: bool,
}

impl MetadataManager {
    pub(crate) fn new(
        config: ConnectionConfig,
        bootstrap_endpoints: impl IntoIterator<Item = BrokerEndpoint>,
    ) -> Self {
        let refresh_backoff = metadata_refresh_backoff_state(&config);
        let equivalent_response_backoff = metadata_refresh_backoff_state(&config);
        Self {
            config,
            snapshot: None,
            topic_last_used: HashMap::new(),
            topic_updated_at: HashMap::new(),
            stale_partitions: HashMap::new(),
            topic_errors: HashMap::new(),
            invalid_topics: HashSet::new(),
            unauthorized_topics: HashSet::new(),
            internal_topics: HashSet::new(),
            bootstrap_endpoints: bootstrap_endpoints
                .into_iter()
                .map(|endpoint| (endpoint.node_id, endpoint))
                .collect(),
            no_usable_metadata_since: None,
            refresh_backoff,
            equivalent_response_backoff,
            next_refresh_allowed_at: None,
            update_requested: false,
        }
    }

    /// Fold a metadata response into the cached snapshot.
    ///
    /// `requested_topics` is the topic list the refresh actually asked the
    /// broker for. A `Metadata` response only speaks for the topics it was asked
    /// about, so overwriting the snapshot with it would evict every topic the
    /// refresh happened not to name — one produce to `orders` would drop the
    /// routing for every other topic the client had already resolved. Kafka
    /// merges instead: `Metadata.handleMetadataResponse`
    /// (`clients/src/main/java/org/apache/kafka/clients/Metadata.java`, Apache
    /// Kafka 4.3.0) hands a partial update to `MetadataSnapshot.mergeWith` with
    /// the retain predicate
    /// `(topic, isInternal) -> !topics.contains(topic) && retainTopic(topic, isInternal, nowMs)`,
    /// where `topics` is what the response carried. Topics in the response are
    /// replaced; every other still-retained topic survives untouched. Brokers,
    /// controller and cluster id always come from the newest response, exactly
    /// as `mergeWith` takes `newNodes`/`newController` wholesale.
    ///
    /// The one deviation is how a deletion arrives. Java sees a deleted topic as
    /// an error entry *inside* the response, which puts its name in `topics` and
    /// so drops it from the merge. [`map_metadata`](super::map_metadata) rejects
    /// a response carrying any topic-level error outright, so the only deletion
    /// signal that ever reaches `store` is a *requested* topic the response left
    /// out — and that is evicted rather than retained.
    pub(crate) fn store(
        &mut self,
        requested_topics: &[String],
        metadata: Arc<ClusterMetadata>,
        now: Instant,
    ) -> crate::wire::Result<()> {
        let requested: HashSet<&str> = requested_topics.iter().map(String::as_str).collect();
        let equivalent_response = self.response_is_equivalent(&requested, &metadata);
        let retained = self.retained_topics(&requested, &metadata, now);
        {
            // Only the topics this response carried were re-resolved, so only
            // their invalidations are settled. A retained topic keeps the one it
            // was holding; an evicted topic leaves the snapshot entirely, and
            // dropping its entry here stops the map growing without bound.
            let retained_names: HashSet<&str> =
                retained.iter().map(|topic| topic.name.as_str()).collect();
            self.stale_partitions
                .retain(|topic, _partitions| retained_names.contains(topic.as_str()));
        }
        // Each topic the response carried restarts its own `metadata.max.age.ms`
        // window; a retained topic keeps the age it already had.
        for topic in &metadata.topics {
            let _previous = self.topic_updated_at.insert(topic.name.clone(), now);
        }
        let metadata = if retained.is_empty() {
            metadata
        } else {
            let mut merged = Arc::unwrap_or_clone(metadata);
            merged.topics.extend(retained);
            Arc::new(merged)
        };
        self.topic_last_used
            .retain(|topic, _last_used| metadata.topic(topic).is_some());
        self.topic_updated_at
            .retain(|topic, _updated_at| metadata.topic(topic).is_some());
        self.snapshot = Some(MetadataSnapshot {
            metadata,
            updated_at: now,
        });
        self.no_usable_metadata_since = None;
        self.update_requested = false;
        self.refresh_backoff.reset();
        if equivalent_response {
            let delay = self.equivalent_response_backoff.next_delay()?;
            self.next_refresh_allowed_at = now.checked_add(delay);
        } else {
            self.equivalent_response_backoff.reset();
            self.next_refresh_allowed_at = None;
        }
        Ok(())
    }

    /// Cached topics this response leaves untouched and that are still worth
    /// keeping — Kafka's `mergeWith` retain predicate (see [`store`](Self::store)).
    ///
    /// `retainTopic` is Java's "is this topic still in the client's working
    /// set", which for the producer is `ProducerMetadata`'s idle expiry; the
    /// local equivalent is `metadata.max.idle.ms` against `topic_last_used`.
    /// Age is deliberately not part of it: an age-expired topic stays in the
    /// snapshot and is refetched by its own next lookup, the same way Kafka
    /// treats `metadata.max.age.ms` as a refresh trigger rather than an eviction.
    fn retained_topics(
        &self,
        requested: &HashSet<&str>,
        response: &ClusterMetadata,
        now: Instant,
    ) -> Vec<TopicMetadata> {
        let Some(snapshot) = self.snapshot.as_ref() else {
            return Vec::new();
        };
        snapshot
            .metadata
            .topics
            .iter()
            .filter(|topic| {
                response.topic(&topic.name).is_none()
                    && !requested.contains(topic.name.as_str())
                    && self.topic_is_active(&topic.name, now)
            })
            .cloned()
            .collect()
    }

    /// Whether this response told us nothing the cache did not already hold.
    ///
    /// Scored on what the response carried rather than on the merged snapshot,
    /// because Kafka scores it the same way: `equivalentResponseCount` is
    /// incremented per update and reset from `updateLatestMetadata`, which only
    /// ever inspects partitions the response contained. Retaining or expiring a
    /// topic the response never mentioned is therefore not "news" and must not
    /// clear the backoff — otherwise one idle topic falling out of the snapshot
    /// would let an otherwise-identical response poll the broker at full rate.
    /// A *requested* topic the response dropped is news, since it evicts a
    /// cached topic.
    fn response_is_equivalent(
        &self,
        requested: &HashSet<&str>,
        response: &ClusterMetadata,
    ) -> bool {
        let Some(snapshot) = self.snapshot.as_ref() else {
            return false;
        };
        let cached = snapshot.metadata.as_ref();
        if cached.cluster_id != response.cluster_id
            || cached.controller_id != response.controller_id
            || cached.brokers != response.brokers
        {
            return false;
        }
        response
            .topics
            .iter()
            .all(|topic| cached.topic(&topic.name) == Some(topic))
            && !requested
                .iter()
                .any(|topic| cached.topic(topic).is_some() && response.topic(topic).is_none())
    }

    /// Age of the currently cached metadata snapshot, or `None` when no
    /// metadata has been stored yet. Mirrors Kafka's `metadata-age` metric.
    /// Only the producer control plane reads this (via `WireClient::metadata_age`).
    #[cfg(feature = "producer")]
    pub(crate) fn current_age(&self, now: Instant) -> Option<Duration> {
        self.snapshot
            .as_ref()
            .map(|snapshot| now.saturating_duration_since(snapshot.updated_at))
    }

    pub(crate) fn cached_for<I, S>(&self, topics: I, now: Instant) -> Option<Arc<ClusterMetadata>>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let snapshot = self.snapshot.as_ref()?;
        if now.duration_since(snapshot.updated_at) > self.config.metadata_max_age {
            return None;
        }
        if self.update_requested {
            return None;
        }
        let all_present_and_fresh = topics.into_iter().all(|topic| {
            let topic = topic.as_ref();
            snapshot.metadata.topic(topic).is_some()
                && self.topic_is_active(topic, now)
                && self.topic_is_current(topic, now)
                && !self.topic_has_stale_partition(topic)
        });
        if !all_present_and_fresh {
            return None;
        }
        Some(Arc::clone(&snapshot.metadata))
    }

    pub(crate) fn mark_topics_used<I, S>(&mut self, topics: I, now: Instant)
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        for topic in topics {
            let _previous = self.topic_last_used.insert(topic.as_ref().to_owned(), now);
        }
    }

    /// Record that `(topic, partition)` lost its leader, so the next lookup of
    /// `topic` refetches instead of routing to the stale leader.
    ///
    /// The partition identifies the trigger and is retained per partition;
    /// the resulting refetch is topic-scoped because Kafka's `Metadata` request
    /// is topic-keyed and cannot ask for a single partition. Every other topic
    /// keeps being served from the cached snapshot until its own expiry.
    pub(crate) fn invalidate_topic_partition(&mut self, topic: &str, partition: i32) {
        let _was_stale = self
            .stale_partitions
            .entry(topic.to_owned())
            .or_default()
            .insert(partition);
    }

    pub(crate) const fn request_update(&mut self) {
        self.update_requested = true;
    }

    pub(crate) fn request_update_for_missing_topics<I, S>(&mut self, topics: I)
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let Some(snapshot) = &self.snapshot else {
            self.request_update();
            return;
        };
        if topics
            .into_iter()
            .any(|topic| snapshot.metadata.topic(topic.as_ref()).is_none())
        {
            self.update_requested = true;
            self.equivalent_response_backoff.reset();
            self.next_refresh_allowed_at = None;
        }
    }

    pub(crate) fn refresh_delay(&self, now: Instant) -> Duration {
        self.next_refresh_allowed_at
            .map_or(Duration::ZERO, |allowed_at| {
                allowed_at.saturating_duration_since(now)
            })
    }

    pub(crate) const fn record_refresh_attempt(&mut self, _now: Instant) {
        self.update_requested = false;
    }

    pub(crate) fn record_refresh_failure(&mut self, now: Instant) -> crate::wire::Result<Duration> {
        let delay = self.refresh_backoff.next_delay()?;
        self.next_refresh_allowed_at = now.checked_add(delay);
        Ok(delay)
    }

    pub(crate) fn record_no_usable_metadata(&mut self, now: Instant) -> MetadataRecoveryAction {
        if self.config.metadata_recovery_strategy == MetadataRecoveryStrategy::None {
            return MetadataRecoveryAction::Fail;
        }
        let first_failure = *self.no_usable_metadata_since.get_or_insert(now);
        if now.duration_since(first_failure) >= self.config.metadata_rebootstrap_trigger {
            MetadataRecoveryAction::Rebootstrap
        } else {
            MetadataRecoveryAction::Fail
        }
    }

    pub(crate) fn bootstrap_endpoints(&self) -> impl Iterator<Item = BrokerEndpoint> + '_ {
        self.bootstrap_endpoints.values().cloned()
    }

    pub(crate) fn record_topic_states<I>(&mut self, states: I)
    where
        I: IntoIterator<Item = MetadataTopicState>,
    {
        for state in states {
            let _previous_error = self.topic_errors.remove(&state.topic);
            let _was_invalid = self.invalid_topics.remove(&state.topic);
            let _was_unauthorized = self.unauthorized_topics.remove(&state.topic);
            match state.status {
                MetadataTopicStatus::Usable { is_internal } => {
                    if is_internal {
                        let _was_internal = self.internal_topics.insert(state.topic);
                    } else {
                        let _was_internal = self.internal_topics.remove(&state.topic);
                    }
                },
                MetadataTopicStatus::Invalid(error) => {
                    let _was_internal = self.internal_topics.remove(&state.topic);
                    let _previous_error = self.topic_errors.insert(state.topic.clone(), error);
                    let _was_invalid = self.invalid_topics.insert(state.topic);
                },
                MetadataTopicStatus::Unauthorized(error) => {
                    let _was_internal = self.internal_topics.remove(&state.topic);
                    let _previous_error = self.topic_errors.insert(state.topic.clone(), error);
                    let _was_unauthorized = self.unauthorized_topics.insert(state.topic);
                },
                MetadataTopicStatus::Error(error) => {
                    let _was_internal = self.internal_topics.remove(&state.topic);
                    let _previous_error = self.topic_errors.insert(state.topic, error);
                },
            }
        }
    }

    pub(crate) fn topic_error(&self, topic: &str) -> Option<ErrorCode> {
        self.topic_errors.get(topic).copied()
    }

    pub(crate) fn is_invalid_topic(&self, topic: &str) -> bool {
        self.invalid_topics.contains(topic)
    }

    pub(crate) fn is_unauthorized_topic(&self, topic: &str) -> bool {
        self.unauthorized_topics.contains(topic)
    }

    #[cfg(test)]
    pub(crate) fn is_internal_topic(&self, topic: &str) -> bool {
        self.internal_topics.contains(topic)
    }

    #[cfg(feature = "producer")]
    pub(crate) fn apply_partition_leader_update(
        &mut self,
        change: PartitionLeaderChange<'_>,
    ) -> bool {
        let Some(snapshot) = &mut self.snapshot else {
            return false;
        };
        let metadata = Arc::make_mut(&mut snapshot.metadata);
        if change.leader_id < 0 || change.leader_epoch < 0 {
            return false;
        }
        let Some(topic_position) = metadata
            .topics
            .iter()
            .position(|metadata_topic| metadata_topic.name == change.topic)
        else {
            return false;
        };
        let Some(partition_position) = metadata
            .topics
            .get(topic_position)
            .map(|topic| &topic.partitions)
            .and_then(|partitions| {
                partitions
                    .iter()
                    .position(|partition| partition.partition_index == change.partition_index)
            })
        else {
            return false;
        };
        let Some(current_epoch) = metadata
            .topics
            .get(topic_position)
            .and_then(|topic| topic.partitions.get(partition_position))
            .map(|partition| partition.leader_epoch)
        else {
            return false;
        };
        if change.leader_epoch <= current_epoch {
            return false;
        }
        match change.leader_broker {
            Some(broker) if broker.node_id == change.leader_id => {
                if let Some(existing_broker) = metadata
                    .brokers
                    .iter_mut()
                    .find(|broker| broker.node_id == change.leader_id)
                {
                    *existing_broker = broker.clone();
                } else {
                    metadata.brokers.push(broker.clone());
                }
            },
            _ if metadata
                .brokers
                .iter()
                .any(|broker| broker.node_id == change.leader_id) => {},
            _ => return false,
        }

        let Some(partition) = metadata
            .topics
            .get_mut(topic_position)
            .and_then(|topic| topic.partitions.get_mut(partition_position))
        else {
            return false;
        };
        partition.leader_id = change.leader_id;
        partition.leader_epoch = change.leader_epoch;
        // The cached entry for this partition now names the current leader, so
        // the invalidation it was carrying is settled without a metadata RPC.
        self.clear_stale_partition(change.topic, change.partition_index);
        true
    }

    #[cfg(feature = "producer")]
    fn clear_stale_partition(&mut self, topic: &str, partition: i32) {
        let Some(partitions) = self.stale_partitions.get_mut(topic) else {
            return;
        };
        let _was_stale = partitions.remove(&partition);
        if partitions.is_empty() {
            let _removed = self.stale_partitions.remove(topic);
        }
    }

    fn topic_has_stale_partition(&self, topic: &str) -> bool {
        self.stale_partitions
            .get(topic)
            .is_some_and(|partitions| !partitions.is_empty())
    }

    /// Whether the topic is still in the client's working set — Kafka's
    /// `retainTopic`, expressed as `metadata.max.idle.ms` since the last lookup.
    fn topic_is_active(&self, topic: &str, now: Instant) -> bool {
        self.topic_last_used.get(topic).is_some_and(|last_used| {
            now.duration_since(*last_used) <= self.config.metadata_max_idle
        })
    }

    /// Whether the topic's own metadata is younger than `metadata.max.age.ms`.
    /// Scored per topic because [`store`](Self::store) merges per topic: a
    /// refresh for one topic must not renew another topic's age.
    fn topic_is_current(&self, topic: &str, now: Instant) -> bool {
        self.topic_updated_at.get(topic).is_some_and(|updated_at| {
            now.duration_since(*updated_at) <= self.config.metadata_max_age
        })
    }
}

fn metadata_refresh_backoff_state(config: &ConnectionConfig) -> BackoffState {
    BackoffState::new(BackoffPolicy::new(
        config.metadata_refresh_backoff_initial,
        config.metadata_refresh_backoff_max,
    ))
}

#[cfg(test)]
mod tests {
    use std::{
        net::{IpAddr, Ipv4Addr, SocketAddr},
        sync::Arc,
        time::{Duration, Instant},
    };

    use kacrab_protocol::{KafkaUuid, generated::ErrorCode};

    #[cfg(feature = "producer")]
    use super::PartitionLeaderChange;
    use super::{MetadataManager, MetadataRecoveryAction};
    use crate::wire::{
        BrokerEndpoint, BrokerMetadata, ClusterMetadata, ConnectionConfig,
        MetadataRecoveryStrategy, PartitionMetadata, TopicMetadata,
        metadata::{MetadataTopicState, MetadataTopicStatus},
    };

    #[test]
    fn manager_expires_topic_after_metadata_max_idle() {
        let start = Instant::now();
        let mut manager = MetadataManager::new(
            ConnectionConfig::default()
                .metadata_max_age(Duration::from_mins(5))
                .metadata_max_idle(Duration::from_millis(10)),
            [broker_endpoint(1)],
        );
        manager
            .store(
                &requested(&["orders"]),
                Arc::new(metadata_with_topic("orders", 1, 1)),
                start,
            )
            .unwrap();
        manager.mark_topics_used(["orders"], start);

        assert!(
            manager
                .cached_for(["orders"], start + Duration::from_millis(9))
                .is_some()
        );
        assert!(
            manager
                .cached_for(["orders"], start + Duration::from_millis(11))
                .is_none()
        );
    }

    #[test]
    fn manager_expires_snapshot_after_metadata_max_age() {
        let start = Instant::now();
        let mut manager = MetadataManager::new(
            ConnectionConfig::default()
                .metadata_max_age(Duration::from_millis(10))
                .metadata_max_idle(Duration::from_mins(5)),
            [broker_endpoint(1)],
        );
        manager
            .store(
                &requested(&["orders"]),
                Arc::new(metadata_with_topic("orders", 1, 1)),
                start,
            )
            .unwrap();
        manager.mark_topics_used(["orders"], start);

        assert!(
            manager
                .cached_for(["orders"], start + Duration::from_millis(9))
                .is_some()
        );
        assert!(
            manager
                .cached_for(["orders"], start + Duration::from_millis(11))
                .is_none()
        );
    }

    #[test]
    fn manager_rebootstrap_waits_for_trigger_and_respects_strategy() {
        let start = Instant::now();
        let mut manager = MetadataManager::new(
            ConnectionConfig::default()
                .metadata_recovery_strategy(MetadataRecoveryStrategy::Rebootstrap)
                .metadata_rebootstrap_trigger(Duration::from_millis(10)),
            [broker_endpoint(1)],
        );

        assert_eq!(
            manager.record_no_usable_metadata(start),
            MetadataRecoveryAction::Fail
        );
        assert_eq!(
            manager.record_no_usable_metadata(start + Duration::from_millis(9)),
            MetadataRecoveryAction::Fail
        );
        assert_eq!(
            manager.record_no_usable_metadata(start + Duration::from_millis(11)),
            MetadataRecoveryAction::Rebootstrap
        );

        let mut disabled = MetadataManager::new(
            ConnectionConfig::default()
                .metadata_recovery_strategy(MetadataRecoveryStrategy::None)
                .metadata_rebootstrap_trigger(Duration::ZERO),
            [broker_endpoint(1)],
        );
        assert_eq!(
            disabled.record_no_usable_metadata(start + Duration::from_millis(11)),
            MetadataRecoveryAction::Fail
        );
    }

    #[test]
    fn manager_tracks_metadata_topic_bookkeeping_buckets() {
        let mut manager = MetadataManager::new(ConnectionConfig::default(), [broker_endpoint(1)]);

        manager.record_topic_states([
            MetadataTopicState {
                topic: "orders".to_owned(),
                status: MetadataTopicStatus::Usable { is_internal: true },
            },
            MetadataTopicState {
                topic: "bad topic".to_owned(),
                status: MetadataTopicStatus::Invalid(ErrorCode::InvalidTopicException),
            },
            MetadataTopicState {
                topic: "secret".to_owned(),
                status: MetadataTopicStatus::Unauthorized(ErrorCode::TopicAuthorizationFailed),
            },
        ]);

        assert!(manager.is_internal_topic("orders"));
        assert!(manager.is_invalid_topic("bad topic"));
        assert!(manager.is_unauthorized_topic("secret"));
        assert_eq!(
            manager.topic_error("secret"),
            Some(ErrorCode::TopicAuthorizationFailed)
        );

        manager.record_topic_states([MetadataTopicState {
            topic: "orders".to_owned(),
            status: MetadataTopicStatus::Usable { is_internal: false },
        }]);
        assert!(!manager.is_internal_topic("orders"));
    }

    #[test]
    fn manager_applies_refresh_backoff_after_failed_metadata_refresh() {
        let start = Instant::now();
        let mut manager = MetadataManager::new(
            ConnectionConfig::default()
                .metadata_refresh_backoff_initial(Duration::from_millis(10))
                .metadata_refresh_backoff_max(Duration::from_millis(40)),
            [broker_endpoint(1)],
        );

        assert_eq!(manager.refresh_delay(start), Duration::ZERO);
        manager.record_refresh_attempt(start);
        let first_delay = manager
            .record_refresh_failure(start)
            .expect("refresh backoff");
        assert!(first_delay >= Duration::from_millis(8));
        assert!(first_delay <= Duration::from_millis(12));
        assert!(manager.refresh_delay(start) > Duration::ZERO);
        assert_eq!(manager.refresh_delay(start + first_delay), Duration::ZERO);

        manager
            .store(
                &requested(&["orders"]),
                Arc::new(metadata_with_topic("orders", 1, 1)),
                start,
            )
            .unwrap();
        assert_eq!(manager.refresh_delay(start), Duration::ZERO);
    }

    #[test]
    fn manager_backs_off_equivalent_metadata_responses_but_not_new_topics() {
        let start = Instant::now();
        let mut manager = MetadataManager::new(
            ConnectionConfig::default()
                .metadata_refresh_backoff_initial(Duration::from_millis(10))
                .metadata_refresh_backoff_max(Duration::from_millis(40)),
            [broker_endpoint(1)],
        );
        let metadata = Arc::new(metadata_with_topic("orders", 1, 1));

        manager
            .store(&requested(&["orders"]), Arc::clone(&metadata), start)
            .unwrap();
        assert_eq!(manager.refresh_delay(start), Duration::ZERO);
        manager
            .store(
                &requested(&["orders"]),
                Arc::clone(&metadata),
                start + Duration::from_millis(1),
            )
            .unwrap();
        assert!(manager.refresh_delay(start + Duration::from_millis(1)) > Duration::ZERO);

        manager.request_update_for_missing_topics(["payments"]);
        assert_eq!(
            manager.refresh_delay(start + Duration::from_millis(1)),
            Duration::ZERO
        );
    }

    // `apply_partition_leader_update` and its `PartitionLeaderChange` input only
    // exist for the producer's leadership-change path, so this test has to carry
    // the same gate the code does — otherwise a consumer-only or admin-only build
    // fails to compile its own unit tests.
    #[cfg(feature = "producer")]
    #[test]
    fn manager_applies_current_leader_update_only_when_epoch_is_current() {
        let start = Instant::now();
        let mut manager = MetadataManager::new(ConnectionConfig::default(), [broker_endpoint(1)]);
        manager
            .store(
                &requested(&["orders"]),
                Arc::new(metadata_with_topic("orders", 1, 3)),
                start,
            )
            .unwrap();
        manager.mark_topics_used(["orders"], start);

        assert!(!manager.apply_partition_leader_update(leader_change("orders", 0, 2, 2, None)));
        let broker_2 = broker_metadata(2);
        assert!(!manager.apply_partition_leader_update(leader_change(
            "orders",
            0,
            2,
            3,
            Some(&broker_2)
        )));
        assert!(!manager.apply_partition_leader_update(leader_change("orders", 0, 2, 4, None)));
        let stale = manager.cached_for(["orders"], start).expect("metadata");
        assert_eq!(
            stale.leader_for("orders", 0).map(|broker| broker.node_id),
            Some(1)
        );

        assert!(manager.apply_partition_leader_update(leader_change(
            "orders",
            0,
            2,
            4,
            Some(&broker_2)
        )));
        let updated = manager.cached_for(["orders"], start).expect("metadata");
        assert_eq!(
            updated
                .topic("orders")
                .and_then(|topic| topic.partitions.first())
                .map(|partition| (partition.leader_id, partition.leader_epoch)),
            Some((2, 4))
        );
        assert_eq!(
            updated.leader_for("orders", 0).map(|broker| broker.node_id),
            Some(2)
        );
    }

    #[test]
    fn manager_invalidation_is_scoped_to_the_failing_topic() {
        let start = Instant::now();
        let mut manager = MetadataManager::new(
            ConnectionConfig::default()
                .metadata_max_age(Duration::from_mins(5))
                .metadata_max_idle(Duration::from_mins(5)),
            [broker_endpoint(1)],
        );
        manager
            .store(
                &requested(&["orders", "payments"]),
                Arc::new(metadata_with_topics(&["orders", "payments"], 1, 1)),
                start,
            )
            .unwrap();
        manager.mark_topics_used(["orders", "payments"], start);

        manager.invalidate_topic_partition("orders", 1);

        assert!(
            manager.cached_for(["payments"], start).is_some(),
            "an unrelated topic must keep being served from the cached snapshot"
        );
        assert!(
            manager.cached_for(["orders"], start).is_none(),
            "the topic that lost its leader must be refetched"
        );
        assert!(
            manager.cached_for(["orders", "payments"], start).is_none(),
            "a lookup that includes the invalidated topic must be refetched"
        );

        manager
            .store(
                &requested(&["orders", "payments"]),
                Arc::new(metadata_with_topics(&["orders", "payments"], 2, 2)),
                start,
            )
            .unwrap();
        assert!(
            manager.cached_for(["orders"], start).is_some(),
            "a stored refresh must clear the invalidation"
        );
    }

    #[cfg(feature = "producer")]
    #[test]
    fn manager_leader_update_clears_only_the_invalidated_partition() {
        let start = Instant::now();
        let mut manager = MetadataManager::new(
            ConnectionConfig::default()
                .metadata_max_age(Duration::from_mins(5))
                .metadata_max_idle(Duration::from_mins(5)),
            [broker_endpoint(1)],
        );
        manager
            .store(
                &requested(&["orders"]),
                Arc::new(metadata_with_topics(&["orders"], 1, 3)),
                start,
            )
            .unwrap();
        manager.mark_topics_used(["orders"], start);

        manager.invalidate_topic_partition("orders", 0);
        manager.invalidate_topic_partition("orders", 1);
        assert!(manager.cached_for(["orders"], start).is_none());

        let broker_2 = broker_metadata(2);
        assert!(manager.apply_partition_leader_update(leader_change(
            "orders",
            0,
            2,
            4,
            Some(&broker_2)
        )));
        assert!(
            manager.cached_for(["orders"], start).is_none(),
            "partition 1 is still waiting for a leader"
        );

        assert!(manager.apply_partition_leader_update(leader_change(
            "orders",
            1,
            2,
            4,
            Some(&broker_2)
        )));
        assert!(
            manager.cached_for(["orders"], start).is_some(),
            "every invalidated partition learned its new leader in place"
        );
    }

    #[test]
    fn manager_store_keeps_topics_the_refresh_never_asked_for() {
        let start = Instant::now();
        let mut manager = MetadataManager::new(
            ConnectionConfig::default()
                .metadata_max_age(Duration::from_mins(5))
                .metadata_max_idle(Duration::from_mins(5)),
            [broker_endpoint(1)],
        );
        manager
            .store(
                &requested(&["orders", "payments"]),
                Arc::new(metadata_with_topics(&["orders", "payments"], 1, 1)),
                start,
            )
            .unwrap();
        manager.mark_topics_used(["orders", "payments"], start);

        // `payments` has not expired, so a refresh triggered by `orders` asks the
        // broker for `orders` alone and the response carries nothing else.
        manager
            .store(
                &requested(&["orders"]),
                Arc::new(metadata_with_topics(&["orders"], 1, 7)),
                start + Duration::from_millis(1),
            )
            .unwrap();
        manager.mark_topics_used(["orders"], start + Duration::from_millis(1));

        let cached = manager
            .cached_for(["orders", "payments"], start + Duration::from_millis(2))
            .expect("a single-topic refresh must not evict every other topic");
        assert_eq!(
            leader_epoch(&cached, "orders"),
            Some(7),
            "the refreshed topic takes the response's metadata"
        );
        assert_eq!(
            leader_epoch(&cached, "payments"),
            Some(1),
            "the untouched topic keeps the metadata it was cached with"
        );
    }

    #[test]
    fn manager_store_evicts_a_requested_topic_the_response_omits() {
        let start = Instant::now();
        let mut manager = MetadataManager::new(
            ConnectionConfig::default()
                .metadata_max_age(Duration::from_mins(5))
                .metadata_max_idle(Duration::from_mins(5)),
            [broker_endpoint(1)],
        );
        manager
            .store(
                &requested(&["orders", "payments"]),
                Arc::new(metadata_with_topics(&["orders", "payments"], 1, 1)),
                start,
            )
            .unwrap();
        manager.mark_topics_used(["orders", "payments"], start);

        // The refresh asked for both topics and the broker answered with only
        // one: `payments` no longer exists.
        manager
            .store(
                &requested(&["orders", "payments"]),
                Arc::new(metadata_with_topics(&["orders"], 1, 2)),
                start,
            )
            .unwrap();

        assert!(
            manager.cached_for(["orders"], start).is_some(),
            "the surviving topic stays cached"
        );
        assert!(
            manager.cached_for(["payments"], start).is_none(),
            "a requested topic missing from the response is deleted, not retained"
        );
    }

    #[test]
    fn manager_partial_refresh_settles_only_the_topics_it_carried() {
        let start = Instant::now();
        let mut manager = MetadataManager::new(
            ConnectionConfig::default()
                .metadata_max_age(Duration::from_mins(5))
                .metadata_max_idle(Duration::from_mins(5)),
            [broker_endpoint(1)],
        );
        manager
            .store(
                &requested(&["orders", "payments"]),
                Arc::new(metadata_with_topics(&["orders", "payments"], 1, 1)),
                start,
            )
            .unwrap();
        manager.mark_topics_used(["orders", "payments"], start);

        manager.invalidate_topic_partition("orders", 0);
        manager.invalidate_topic_partition("payments", 0);

        manager
            .store(
                &requested(&["orders"]),
                Arc::new(metadata_with_topics(&["orders"], 1, 2)),
                start,
            )
            .unwrap();

        assert!(
            manager.cached_for(["orders"], start).is_some(),
            "the refreshed topic's invalidation is settled by the response"
        );
        assert!(
            manager.cached_for(["payments"], start).is_none(),
            "a topic the response never carried keeps its pending invalidation"
        );
    }

    #[test]
    fn manager_retained_topic_keeps_its_own_metadata_max_age() {
        let start = Instant::now();
        let mut manager = MetadataManager::new(
            ConnectionConfig::default()
                .metadata_max_age(Duration::from_millis(10))
                .metadata_max_idle(Duration::from_mins(5)),
            [broker_endpoint(1)],
        );
        manager
            .store(
                &requested(&["orders", "payments"]),
                Arc::new(metadata_with_topics(&["orders", "payments"], 1, 1)),
                start,
            )
            .unwrap();
        manager.mark_topics_used(["orders", "payments"], start);

        manager
            .store(
                &requested(&["orders"]),
                Arc::new(metadata_with_topics(&["orders"], 1, 2)),
                start + Duration::from_millis(9),
            )
            .unwrap();

        let now = start + Duration::from_millis(11);
        assert!(
            manager.cached_for(["orders"], now).is_some(),
            "the refreshed topic restarts its own max-age window"
        );
        assert!(
            manager.cached_for(["payments"], now).is_none(),
            "a retained topic must not inherit the refreshed topic's age"
        );
    }

    #[test]
    fn manager_backs_off_partial_refresh_that_repeats_cached_topic_metadata() {
        let start = Instant::now();
        let mut manager = MetadataManager::new(
            ConnectionConfig::default()
                .metadata_max_age(Duration::from_mins(5))
                .metadata_max_idle(Duration::from_mins(5))
                .metadata_refresh_backoff_initial(Duration::from_millis(10))
                .metadata_refresh_backoff_max(Duration::from_millis(40)),
            [broker_endpoint(1)],
        );
        manager
            .store(
                &requested(&["orders", "payments"]),
                Arc::new(metadata_with_topics(&["orders", "payments"], 1, 1)),
                start,
            )
            .unwrap();
        manager.mark_topics_used(["orders", "payments"], start);
        assert_eq!(manager.refresh_delay(start), Duration::ZERO);

        // The response repeats what `orders` was already cached with; that the
        // merged view also drops nothing is beside the point — Kafka scores
        // equivalence on the metadata the response actually carried.
        manager
            .store(
                &requested(&["orders"]),
                Arc::new(metadata_with_topics(&["orders"], 1, 1)),
                start,
            )
            .unwrap();
        assert!(
            manager.refresh_delay(start) > Duration::ZERO,
            "a partial refresh that learns nothing new must back off"
        );

        manager
            .store(
                &requested(&["orders"]),
                Arc::new(metadata_with_topics(&["orders"], 1, 2)),
                start,
            )
            .unwrap();
        assert_eq!(
            manager.refresh_delay(start),
            Duration::ZERO,
            "a partial refresh that carries new metadata clears the backoff"
        );
    }

    fn leader_epoch(metadata: &ClusterMetadata, topic: &str) -> Option<i32> {
        metadata
            .topic(topic)
            .and_then(|topic| topic.partitions.first())
            .map(|partition| partition.leader_epoch)
    }

    /// The topic list a metadata refresh actually asked the broker for.
    fn requested(topics: &[&str]) -> Vec<String> {
        topics.iter().map(|topic| (*topic).to_owned()).collect()
    }

    fn broker_endpoint(node_id: i32) -> BrokerEndpoint {
        BrokerEndpoint::new(
            node_id,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9_092),
        )
    }

    #[cfg(feature = "producer")]
    fn broker_metadata(node_id: i32) -> BrokerMetadata {
        BrokerMetadata {
            node_id,
            host: "localhost".to_owned(),
            port: 9_092,
            rack: None,
        }
    }

    #[cfg(feature = "producer")]
    fn leader_change<'a>(
        topic: &'a str,
        partition_index: i32,
        leader_id: i32,
        leader_epoch: i32,
        leader_broker: Option<&'a BrokerMetadata>,
    ) -> PartitionLeaderChange<'a> {
        PartitionLeaderChange {
            topic,
            partition_index,
            leader_id,
            leader_epoch,
            leader_broker,
        }
    }

    fn metadata_with_topic(topic: &str, broker_id: i32, leader_epoch: i32) -> ClusterMetadata {
        metadata_with_topics(&[topic], broker_id, leader_epoch)
    }

    fn metadata_with_topics(topics: &[&str], broker_id: i32, leader_epoch: i32) -> ClusterMetadata {
        ClusterMetadata {
            cluster_id: Some("cluster-a".to_owned()),
            controller_id: broker_id,
            brokers: vec![BrokerMetadata {
                node_id: broker_id,
                host: "127.0.0.1".to_owned(),
                port: 9_092,
                rack: None,
            }],
            topics: topics
                .iter()
                .map(|topic| TopicMetadata {
                    name: (*topic).to_owned(),
                    topic_id: KafkaUuid::ZERO,
                    is_internal: false,
                    partitions: vec![
                        PartitionMetadata {
                            partition_index: 0,
                            leader_id: broker_id,
                            leader_epoch,
                            replica_nodes: vec![broker_id],
                            isr_nodes: vec![broker_id],
                            offline_replicas: Vec::new(),
                        },
                        PartitionMetadata {
                            partition_index: 1,
                            leader_id: broker_id,
                            leader_epoch,
                            replica_nodes: vec![broker_id],
                            isr_nodes: vec![broker_id],
                            offline_replicas: Vec::new(),
                        },
                    ],
                })
                .collect(),
        }
    }
}
