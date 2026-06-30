//! Metadata cache lifecycle and recovery policy.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Duration, Instant},
};

use kacrab_protocol::generated::ErrorCode;

use super::{ClusterMetadata, MetadataTopicState, MetadataTopicStatus};
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

    pub(crate) fn store(
        &mut self,
        metadata: Arc<ClusterMetadata>,
        now: Instant,
    ) -> crate::wire::Result<()> {
        let equivalent_response = self
            .snapshot
            .as_ref()
            .is_some_and(|snapshot| snapshot.metadata.as_ref() == metadata.as_ref());
        self.topic_last_used
            .retain(|topic, _last_used| metadata.topic(topic).is_some());
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
        let topics = topics
            .into_iter()
            .map(|topic| topic.as_ref().to_owned())
            .collect::<Vec<_>>();
        let snapshot = self.snapshot.as_ref()?;
        if now.duration_since(snapshot.updated_at) > self.config.metadata_max_age {
            return None;
        }
        if self.update_requested {
            return None;
        }
        let requested = topics.iter().collect::<HashSet<_>>();
        if !requested
            .iter()
            .all(|topic| snapshot.metadata.topic(topic).is_some())
        {
            return None;
        }
        if !requested
            .iter()
            .all(|topic| self.topic_is_fresh(topic, now))
        {
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

    pub(crate) fn invalidate_all(&mut self) {
        self.snapshot = None;
        self.request_update();
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
        true
    }

    fn topic_is_fresh(&self, topic: &str, now: Instant) -> bool {
        self.topic_last_used.get(topic).is_some_and(|last_used| {
            now.duration_since(*last_used) <= self.config.metadata_max_idle
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

    use super::{MetadataManager, MetadataRecoveryAction, PartitionLeaderChange};
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
            .store(Arc::new(metadata_with_topic("orders", 1, 1)), start)
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
            .store(Arc::new(metadata_with_topic("orders", 1, 1)), start)
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
            .store(Arc::new(metadata_with_topic("orders", 1, 1)), start)
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

        manager.store(Arc::clone(&metadata), start).unwrap();
        assert_eq!(manager.refresh_delay(start), Duration::ZERO);
        manager
            .store(Arc::clone(&metadata), start + Duration::from_millis(1))
            .unwrap();
        assert!(manager.refresh_delay(start + Duration::from_millis(1)) > Duration::ZERO);

        manager.request_update_for_missing_topics(["payments"]);
        assert_eq!(
            manager.refresh_delay(start + Duration::from_millis(1)),
            Duration::ZERO
        );
    }

    #[test]
    fn manager_applies_current_leader_update_only_when_epoch_is_current() {
        let start = Instant::now();
        let mut manager = MetadataManager::new(ConnectionConfig::default(), [broker_endpoint(1)]);
        manager
            .store(Arc::new(metadata_with_topic("orders", 1, 3)), start)
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

    fn broker_endpoint(node_id: i32) -> BrokerEndpoint {
        BrokerEndpoint::new(
            node_id,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9_092),
        )
    }

    fn broker_metadata(node_id: i32) -> BrokerMetadata {
        BrokerMetadata {
            node_id,
            host: "localhost".to_owned(),
            port: 9_092,
            rack: None,
        }
    }

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
        ClusterMetadata {
            cluster_id: Some("cluster-a".to_owned()),
            controller_id: broker_id,
            brokers: vec![BrokerMetadata {
                node_id: broker_id,
                host: "127.0.0.1".to_owned(),
                port: 9_092,
                rack: None,
            }],
            topics: vec![TopicMetadata {
                name: topic.to_owned(),
                topic_id: KafkaUuid::ZERO,
                is_internal: false,
                partitions: vec![PartitionMetadata {
                    partition_index: 0,
                    leader_id: broker_id,
                    leader_epoch,
                    replica_nodes: vec![broker_id],
                    isr_nodes: vec![broker_id],
                    offline_replicas: Vec::new(),
                }],
            }],
        }
    }
}
