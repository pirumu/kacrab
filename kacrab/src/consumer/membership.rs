//! The membership state and heartbeat vocabulary the two server-side group
//! protocols share.
//!
//! KIP-848 consumer groups ([`next_gen`](super::next_gen)) and KIP-932 share
//! groups ([`share::membership`](super::share::membership)) differ in what their
//! heartbeat *carries* — a consumer reports its owned partitions and may name a
//! server assignor, a share member does neither — but they are the same
//! membership protocol underneath: the client generates a member id it keeps for
//! the process, starts at epoch `0`, sends one RPC on a coordinator-chosen
//! cadence, and gets back an epoch, an interval and possibly an assignment.
//!
//! So the state, the epoch sentinels, and the parsed outcome live here once. The
//! two modules keep only their own request shape and wire call.

use std::time::Duration;

use kacrab_protocol::{KafkaUuid, generated::ErrorCode};

use super::error::{ConsumerError, Result};

/// Member epoch sent to join a fresh group.
pub(super) const EPOCH_JOINING: i32 = 0;
/// Member epoch sent to leave the group.
pub(super) const EPOCH_LEAVING: i32 = -1;

/// The membership state a server-side group member keeps for the lifetime of the
/// process.
#[derive(Debug, Clone)]
pub(super) struct GroupMemberState {
    /// Client-generated member id, kept for the whole client lifetime. For a
    /// share group it is also the share-session identity in every
    /// `ShareFetch`/`ShareAcknowledge`, so the broker rejects anything that is
    /// not a Kafka UUID string.
    pub member_id: String,
    /// Current member epoch ([`EPOCH_JOINING`] before the first heartbeat is
    /// acknowledged).
    pub member_epoch: i32,
    /// Heartbeat cadence the coordinator asked for.
    pub heartbeat_interval: Duration,
}

impl GroupMemberState {
    /// Start fresh membership with a new client-generated member id.
    ///
    /// # Errors
    /// Returns [`ConsumerError::InvalidState`] when the system random source
    /// cannot produce a UUID.
    pub(super) fn new(default_interval: Duration) -> Result<Self> {
        let member_id = KafkaUuid::random()
            .map_err(|_error| ConsumerError::InvalidState("could not generate a group member id"))?
            .to_string();
        Ok(Self {
            member_id,
            member_epoch: EPOCH_JOINING,
            heartbeat_interval: default_interval,
        })
    }

    /// Adopt what a successful heartbeat reported: the new epoch, the cadence the
    /// coordinator asked for (when it named one), and the member id it echoed
    /// (when it sent one — v0 of the consumer protocol assigns the id server-side).
    pub(super) fn adopt(&mut self, outcome: &HeartbeatOutcome) {
        self.member_epoch = outcome.member_epoch;
        if outcome.heartbeat_interval > Duration::ZERO {
            self.heartbeat_interval = outcome.heartbeat_interval;
        }
        if let Some(member_id) = outcome.member_id.as_deref().filter(|id| !id.is_empty()) {
            self.member_id.clear();
            self.member_id.push_str(member_id);
        }
    }
}

/// One topic's partitions in an assignment or owned set, keyed by topic id.
#[derive(Debug, Clone)]
pub(super) struct AssignedTopic {
    pub topic_id: KafkaUuid,
    pub partitions: Vec<i32>,
}

/// The parsed outcome of one group heartbeat, consumer or share.
#[derive(Debug)]
pub(super) struct HeartbeatOutcome {
    /// The member id the coordinator echoed, when it sent one (it replaces ours
    /// at `ConsumerGroupHeartbeat` v0, where the broker assigns it).
    pub member_id: Option<String>,
    /// The new member epoch.
    pub member_epoch: i32,
    /// The heartbeat cadence the coordinator asked for.
    pub heartbeat_interval: Duration,
    /// The target assignment, when the coordinator sent one this round.
    pub assignment: Option<Vec<AssignedTopic>>,
    /// The top-level error code. Fencing and coordinator-availability signals are
    /// surfaced here rather than turned into hard errors, so the caller can
    /// rejoin.
    pub error: ErrorCode,
}

/// A broker's `heartbeat_interval_ms` as a [`Duration`], treating a negative
/// value as "unset" so it never becomes an enormous unsigned cadence.
pub(super) fn heartbeat_interval(heartbeat_interval_ms: i32) -> Duration {
    Duration::from_millis(u64::try_from(heartbeat_interval_ms.max(0)).unwrap_or(0))
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::expect_used,
        clippy::missing_assert_message,
        reason = "Unit test fixtures fail fastest with contextual expect calls."
    )]

    use super::{
        Duration, EPOCH_JOINING, ErrorCode, GroupMemberState, HeartbeatOutcome, KafkaUuid,
    };

    fn outcome(member_id: Option<&str>, epoch: i32, interval_ms: u64) -> HeartbeatOutcome {
        HeartbeatOutcome {
            member_id: member_id.map(str::to_owned),
            member_epoch: epoch,
            heartbeat_interval: Duration::from_millis(interval_ms),
            assignment: None,
            error: ErrorCode::None,
        }
    }

    #[test]
    fn new_state_generates_a_member_id_and_joins_at_epoch_zero() {
        let state = GroupMemberState::new(Duration::from_secs(3)).expect("member id");
        assert!(!state.member_id.is_empty());
        assert_eq!(state.member_epoch, EPOCH_JOINING);
        assert_eq!(state.heartbeat_interval, Duration::from_secs(3));
        // Two members get distinct ids.
        let other = GroupMemberState::new(Duration::from_secs(3)).expect("member id");
        assert_ne!(state.member_id, other.member_id);
    }

    #[test]
    fn the_member_id_is_a_kafka_uuid_string_the_broker_accepts() {
        let state = GroupMemberState::new(Duration::from_secs(5)).expect("member id");
        // `KafkaApis.isMemberIdValid` rejects an empty id and anything longer
        // than a human-readable UUID, and the share-session code parses it back
        // with `Uuid.fromString`.
        assert!(!state.member_id.is_empty());
        assert!(state.member_id.len() <= 36);
        let parsed = KafkaUuid::from_base64(&state.member_id).expect("round-trips");
        assert_eq!(parsed.to_string(), state.member_id);
    }

    #[test]
    fn adopting_an_outcome_takes_the_epoch_and_only_a_usable_interval_and_id() {
        let mut state = GroupMemberState::new(Duration::from_secs(5)).expect("member id");
        let generated = state.member_id.clone();

        // A coordinator that echoes nothing leaves the id and cadence alone.
        state.adopt(&outcome(None, 7, 0));
        assert_eq!(state.member_epoch, 7);
        assert_eq!(state.heartbeat_interval, Duration::from_secs(5));
        assert_eq!(state.member_id, generated);

        // An empty echoed id is not an id — Kafka sends one on the paths where
        // the client owns the id, and adopting it would fence the member.
        state.adopt(&outcome(Some(""), 8, 2_000));
        assert_eq!(state.member_id, generated);
        assert_eq!(state.heartbeat_interval, Duration::from_secs(2));

        // A real echoed id replaces ours (`ConsumerGroupHeartbeat` v0).
        state.adopt(&outcome(Some("broker-assigned"), 9, 0));
        assert_eq!(state.member_id, "broker-assigned");
        assert_eq!(state.member_epoch, 9);
    }
}
