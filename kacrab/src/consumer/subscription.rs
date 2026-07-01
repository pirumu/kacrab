//! Consumer subscription and per-partition fetch state.
//!
//! Phase 1 supports user (manual) assignment only; topic/pattern subscription
//! and group-managed assignment arrive with Phase 2. The type is deliberately
//! shaped so that group management layers on top without reworking the
//! per-partition position machinery.

use std::collections::BTreeMap;

use super::config::AutoOffsetReset;
use crate::common::TopicPartition;

/// The next position to fetch on a partition: the offset plus the leader epoch
/// it was derived from (for KIP-320 fencing, wired in a later phase).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct FetchPosition {
    /// Next offset to fetch.
    pub offset: i64,
    /// Leader epoch the offset was resolved against, when known.
    pub leader_epoch: Option<i32>,
}

impl FetchPosition {
    pub(super) const fn new(offset: i64, leader_epoch: Option<i32>) -> Self {
        Self {
            offset,
            leader_epoch,
        }
    }
}

/// Per-partition state: the fetch position (absent until reset/seek) and whether
/// the partition is paused.
#[derive(Debug, Clone, Default)]
struct TopicPartitionState {
    position: Option<FetchPosition>,
    paused: bool,
}

/// How partitions came to be assigned. Phase 1 only exposes `UserAssigned`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SubscriptionType {
    None,
    UserAssigned,
}

/// Tracks the assigned partitions and each one's fetch position, mirroring
/// Kafka's `SubscriptionState`.
#[derive(Debug)]
pub(super) struct SubscriptionState {
    subscription_type: SubscriptionType,
    assignment: BTreeMap<(String, i32), TopicPartitionState>,
    default_reset: AutoOffsetReset,
}

impl SubscriptionState {
    pub(super) const fn new(default_reset: AutoOffsetReset) -> Self {
        Self {
            subscription_type: SubscriptionType::None,
            assignment: BTreeMap::new(),
            default_reset,
        }
    }

    /// The default reset strategy (`auto.offset.reset`).
    pub(super) const fn default_reset(&self) -> AutoOffsetReset {
        self.default_reset
    }

    fn key(partition: &TopicPartition) -> (String, i32) {
        (partition.topic.clone(), partition.partition)
    }

    /// Replace the assignment with a manual (user) partition set. Positions for
    /// partitions already assigned are retained; new partitions start unpositioned.
    pub(super) fn assign(&mut self, partitions: &[TopicPartition]) {
        self.subscription_type = if partitions.is_empty() {
            SubscriptionType::None
        } else {
            SubscriptionType::UserAssigned
        };
        let mut next = BTreeMap::new();
        for partition in partitions {
            let key = Self::key(partition);
            let state = self.assignment.remove(&key).unwrap_or_default();
            let _previous = next.insert(key, state);
        }
        self.assignment = next;
    }

    /// Whether a partition is currently assigned.
    pub(super) fn is_assigned(&self, partition: &TopicPartition) -> bool {
        self.assignment.contains_key(&Self::key(partition))
    }

    /// The currently assigned partitions, in a stable order.
    pub(super) fn assigned_partitions(&self) -> Vec<TopicPartition> {
        self.assignment
            .keys()
            .map(|(topic, partition)| TopicPartition::new(topic.clone(), *partition))
            .collect()
    }

    /// Set the fetch position of a partition (used by `seek` and reset).
    pub(super) fn set_position(&mut self, partition: &TopicPartition, position: FetchPosition) {
        if let Some(state) = self.assignment.get_mut(&Self::key(partition)) {
            state.position = Some(position);
        }
    }

    /// The current fetch position (next offset) of a partition, if positioned.
    pub(super) fn position(&self, partition: &TopicPartition) -> Option<FetchPosition> {
        self.assignment.get(&Self::key(partition))?.position
    }

    /// Advance a partition's fetch position after records were delivered.
    pub(super) fn advance_position(
        &mut self,
        partition: &TopicPartition,
        next_offset: i64,
        leader_epoch: Option<i32>,
    ) {
        if let Some(state) = self.assignment.get_mut(&Self::key(partition)) {
            state.position = Some(FetchPosition::new(next_offset, leader_epoch));
        }
    }

    /// Pause a set of partitions (fetches skip them; buffered data is kept).
    pub(super) fn pause(&mut self, partitions: &[TopicPartition]) {
        for partition in partitions {
            if let Some(state) = self.assignment.get_mut(&Self::key(partition)) {
                state.paused = true;
            }
        }
    }

    /// Resume a set of partitions.
    pub(super) fn resume(&mut self, partitions: &[TopicPartition]) {
        for partition in partitions {
            if let Some(state) = self.assignment.get_mut(&Self::key(partition)) {
                state.paused = false;
            }
        }
    }

    /// The paused partitions.
    pub(super) fn paused(&self) -> Vec<TopicPartition> {
        self.assignment
            .iter()
            .filter(|(_, state)| state.paused)
            .map(|((topic, partition), _)| TopicPartition::new(topic.clone(), *partition))
            .collect()
    }

    /// Assigned, unpaused partitions that already have a fetch position — the
    /// ones a `Fetch` request may include.
    pub(super) fn fetchable_partitions(&self) -> Vec<(TopicPartition, FetchPosition)> {
        self.assignment
            .iter()
            .filter(|(_, state)| !state.paused)
            .filter_map(|((topic, partition), state)| {
                state
                    .position
                    .map(|position| (TopicPartition::new(topic.clone(), *partition), position))
            })
            .collect()
    }

    /// Assigned, unpaused partitions that still need an initial position via
    /// `auto.offset.reset`.
    pub(super) fn partitions_needing_reset(&self) -> Vec<TopicPartition> {
        self.assignment
            .iter()
            .filter(|(_, state)| !state.paused && state.position.is_none())
            .map(|((topic, partition), _)| TopicPartition::new(topic.clone(), *partition))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tp(topic: &str, partition: i32) -> TopicPartition {
        TopicPartition::new(topic, partition)
    }

    #[test]
    fn assign_marks_new_partitions_as_needing_reset() {
        let mut state = SubscriptionState::new(AutoOffsetReset::Latest);
        state.assign(&[tp("t", 0), tp("t", 1)]);
        assert_eq!(state.assigned_partitions().len(), 2);
        assert_eq!(state.partitions_needing_reset().len(), 2);
        assert!(state.fetchable_partitions().is_empty());
    }

    #[test]
    fn positioned_partition_is_fetchable_not_reset() {
        let mut state = SubscriptionState::new(AutoOffsetReset::Earliest);
        state.assign(&[tp("t", 0)]);
        state.set_position(&tp("t", 0), FetchPosition::new(42, Some(7)));
        assert!(state.partitions_needing_reset().is_empty());
        let fetchable = state.fetchable_partitions();
        assert_eq!(fetchable.len(), 1);
        assert_eq!(fetchable[0].1.offset, 42);
        assert_eq!(state.position(&tp("t", 0)).unwrap().offset, 42);
    }

    #[test]
    fn reassign_retains_existing_positions() {
        let mut state = SubscriptionState::new(AutoOffsetReset::Latest);
        state.assign(&[tp("t", 0)]);
        state.set_position(&tp("t", 0), FetchPosition::new(10, None));
        state.assign(&[tp("t", 0), tp("t", 1)]);
        assert_eq!(state.position(&tp("t", 0)).unwrap().offset, 10);
        assert_eq!(state.partitions_needing_reset(), vec![tp("t", 1)]);
    }

    #[test]
    fn paused_partition_is_neither_fetchable_nor_reset() {
        let mut state = SubscriptionState::new(AutoOffsetReset::Latest);
        state.assign(&[tp("t", 0)]);
        state.set_position(&tp("t", 0), FetchPosition::new(5, None));
        state.pause(&[tp("t", 0)]);
        assert_eq!(state.paused(), vec![tp("t", 0)]);
        assert!(state.fetchable_partitions().is_empty());
        state.resume(&[tp("t", 0)]);
        assert_eq!(state.fetchable_partitions().len(), 1);
    }

    #[test]
    fn advance_position_moves_the_fetch_offset() {
        let mut state = SubscriptionState::new(AutoOffsetReset::Latest);
        state.assign(&[tp("t", 0)]);
        state.set_position(&tp("t", 0), FetchPosition::new(0, None));
        state.advance_position(&tp("t", 0), 100, Some(3));
        let position = state.position(&tp("t", 0)).unwrap();
        assert_eq!(position.offset, 100);
        assert_eq!(position.leader_epoch, Some(3));
    }
}
