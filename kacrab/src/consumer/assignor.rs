//! Client-side partition assignors and the `ConsumerProtocol` subscription /
//! assignment codec (classic group protocol).
//!
//! Ships the `range`, `roundrobin`, and `sticky` eager assignors plus the
//! incremental `cooperative-sticky` assignor, selected by
//! `partition.assignment.strategy` (advertised to the coordinator, which picks
//! the one common to all members). Subscription blobs carry each member's
//! currently *owned* partitions (v1), which the cooperative assignor uses to
//! withhold partitions still owned by another member until that member revokes
//! them — so a partition is never owned by two members at once (KIP-429).
//! Subscription/assignment blobs are the version-prefixed `ConsumerProtocol`
//! encoding the broker relays between members, decoded at whatever version the
//! group leader wrote.

use std::collections::{BTreeMap, HashMap, HashSet};

use bytes::{Buf as _, BufMut as _, Bytes, BytesMut};
use kacrab_protocol::generated::{
    ConsumerProtocolAssignmentData, ConsumerProtocolSubscriptionData,
    consumer_protocol_assignment::TopicPartition as AssignmentTopic,
    consumer_protocol_subscription::TopicPartition as SubscriptionTopic,
};

use crate::common::TopicPartition;

/// Kafka `protocol.type` for consumer groups.
pub(super) const PROTOCOL_TYPE: &str = "consumer";
/// The default `range` assignor.
pub(super) const RANGE_ASSIGNOR: &str = "range";
/// The incremental cooperative assignor (KIP-429).
pub(super) const COOPERATIVE_STICKY_ASSIGNOR: &str = "cooperative-sticky";
/// `ConsumerProtocol` version used when encoding assignment blobs.
const CONSUMER_PROTOCOL_V0: i16 = 0;
/// `ConsumerProtocol` subscription version we encode: v1 carries owned
/// partitions, which the cooperative assignor needs.
const CONSUMER_PROTOCOL_V1: i16 = 1;

/// Map a `partition.assignment.strategy` entry to the protocol name kacrab
/// advertises to the coordinator. Unknown entries fall back to `range`.
pub(super) fn protocol_name(strategy: &str) -> &'static str {
    match strategy.trim().to_ascii_lowercase().as_str() {
        "roundrobin" | "round_robin" | "roundrobinassignor" => "roundrobin",
        "sticky" | "stickyassignor" => "sticky",
        "cooperative-sticky" | "cooperativesticky" | "cooperativestickyassignor" => {
            COOPERATIVE_STICKY_ASSIGNOR
        },
        _ => RANGE_ASSIGNOR,
    }
}

/// Whether a chosen group protocol uses the incremental cooperative rebalance
/// (partitions are revoked in a follow-up round rather than all up front).
pub(super) fn is_cooperative(protocol: &str) -> bool {
    protocol == COOPERATIVE_STICKY_ASSIGNOR
}

/// Run the assignor named by the chosen group protocol.
pub(super) fn assign(
    protocol: &str,
    members: &[MemberSubscription],
    partitions_per_topic: &HashMap<String, i32>,
) -> HashMap<String, Vec<TopicPartition>> {
    match protocol {
        "roundrobin" => roundrobin_assign(members, partitions_per_topic),
        "sticky" => sticky_assign(members, partitions_per_topic),
        COOPERATIVE_STICKY_ASSIGNOR => cooperative_sticky_assign(members, partitions_per_topic),
        _ => range_assign(members, partitions_per_topic),
    }
}

/// All `(topic, partition)` pairs a set of members collectively subscribe to, in
/// `(topic, partition)` order.
fn all_partitions(
    members: &[MemberSubscription],
    partitions_per_topic: &HashMap<String, i32>,
) -> Vec<TopicPartition> {
    let mut topics: Vec<String> = members
        .iter()
        .flat_map(|member| member.topics.iter().cloned())
        .collect();
    topics.sort();
    topics.dedup();
    let mut partitions = Vec::new();
    for topic in topics {
        if let Some(&count) = partitions_per_topic.get(&topic) {
            for partition in 0..count.max(0) {
                partitions.push(TopicPartition::new(topic.clone(), partition));
            }
        }
    }
    partitions
}

/// The `roundrobin` assignor: lay every partition out over the members (sorted
/// by id) in a circle, skipping members not subscribed to the topic. Mirrors
/// Java's `RoundRobinAssignor`.
pub(super) fn roundrobin_assign(
    members: &[MemberSubscription],
    partitions_per_topic: &HashMap<String, i32>,
) -> HashMap<String, Vec<TopicPartition>> {
    let mut sorted: Vec<&MemberSubscription> = members.iter().collect();
    sorted.sort_by(|a, b| a.member_id.cmp(&b.member_id));
    let mut assignment: HashMap<String, Vec<TopicPartition>> = members
        .iter()
        .map(|member| (member.member_id.clone(), Vec::new()))
        .collect();
    if sorted.is_empty() {
        return assignment;
    }

    let count = sorted.len();
    let mut cursor = 0usize;
    for partition in all_partitions(members, partitions_per_topic) {
        // Advance to the next member subscribed to this partition's topic.
        for _ in 0..count {
            let index = cursor.checked_rem(count).unwrap_or(0);
            cursor = cursor.wrapping_add(1);
            let Some(member) = sorted.get(index) else {
                continue;
            };
            if member.topics.iter().any(|topic| topic == &partition.topic) {
                if let Some(list) = assignment.get_mut(&member.member_id) {
                    list.push(partition);
                }
                break;
            }
        }
    }
    assignment
}

/// The `sticky` assignor. On a fresh assignment (no prior owned partitions in
/// the subscription blob) this is a balanced greedy assignment — each partition
/// goes to the eligible member with the fewest so far — which matches Java's
/// `StickyAssignor` steady state. Preserving prior ownership across rebalances is
/// a later refinement.
pub(super) fn sticky_assign(
    members: &[MemberSubscription],
    partitions_per_topic: &HashMap<String, i32>,
) -> HashMap<String, Vec<TopicPartition>> {
    let mut sorted: Vec<&MemberSubscription> = members.iter().collect();
    sorted.sort_by(|a, b| a.member_id.cmp(&b.member_id));
    let mut assignment: HashMap<String, Vec<TopicPartition>> = members
        .iter()
        .map(|member| (member.member_id.clone(), Vec::new()))
        .collect();

    for partition in all_partitions(members, partitions_per_topic) {
        let target = sorted
            .iter()
            .filter(|member| member.topics.iter().any(|topic| topic == &partition.topic))
            .min_by_key(|member| assignment.get(&member.member_id).map_or(0, Vec::len));
        if let Some(member) = target
            && let Some(list) = assignment.get_mut(&member.member_id)
        {
            list.push(partition);
        }
    }
    assignment
}

/// The `cooperative-sticky` assignor (KIP-429). Computes a balanced, sticky
/// target — each member keeps the partitions it already owns, unowned partitions
/// go to the least-loaded eligible member, and the result is balanced by moving
/// partitions off overloaded members — then *withholds* any partition still owned
/// by a different member. Withheld partitions are only assigned once their
/// current owner revokes them (having seen them drop out of its own assignment)
/// and the group rebalances again, so a partition is never owned by two members
/// at once. Mirrors Java's `CooperativeStickyAssignor`.
pub(super) fn cooperative_sticky_assign(
    members: &[MemberSubscription],
    partitions_per_topic: &HashMap<String, i32>,
) -> HashMap<String, Vec<TopicPartition>> {
    let all = all_partitions(members, partitions_per_topic);
    let all_set: HashSet<&TopicPartition> = all.iter().collect();
    let mut sorted: Vec<&MemberSubscription> = members.iter().collect();
    sorted.sort_by(|a, b| a.member_id.cmp(&b.member_id));

    let subscribes = |member: &MemberSubscription, topic: &str| {
        member.topics.iter().any(|candidate| candidate == topic)
    };

    // Current ownership, keeping only partitions that still exist and whose owner
    // still subscribes to the topic (stale claims are dropped). First claim wins,
    // guarding against two members reporting the same partition.
    let mut owner_of: HashMap<TopicPartition, String> = HashMap::new();
    for member in &sorted {
        for partition in &member.owned {
            if all_set.contains(partition)
                && subscribes(member, &partition.topic)
                && !owner_of.contains_key(partition)
            {
                let _previous = owner_of.insert(partition.clone(), member.member_id.clone());
            }
        }
    }

    let mut assignment: HashMap<String, Vec<TopicPartition>> = members
        .iter()
        .map(|member| (member.member_id.clone(), Vec::new()))
        .collect();
    let mut assigned: HashSet<TopicPartition> = HashSet::new();

    // 1. Seed each member with the partitions it still validly owns.
    for member in &sorted {
        for partition in &member.owned {
            if owner_of.get(partition).map(String::as_str) == Some(member.member_id.as_str())
                && !assigned.contains(partition)
                && let Some(list) = assignment.get_mut(&member.member_id)
            {
                list.push(partition.clone());
                let _inserted = assigned.insert(partition.clone());
            }
        }
    }

    // 2. Hand every unowned partition to the least-loaded eligible member.
    for partition in &all {
        if assigned.contains(partition) {
            continue;
        }
        if let Some(member) = least_loaded(&sorted, &assignment, &partition.topic, &subscribes) {
            if let Some(list) = assignment.get_mut(&member) {
                list.push(partition.clone());
            }
            let _inserted = assigned.insert(partition.clone());
        }
    }

    // 3. Balance: move partitions off overloaded members onto lighter eligible ones (this is what
    //    hands a freshly joined member its share).
    balance(&mut assignment, &sorted, &subscribes, all.len());

    // 4. Cooperative withhold: a partition targeted at a member that a *different* member still
    //    owns is dropped this round; it lands once the owner revokes.
    for (member_id, partitions) in &mut assignment {
        partitions.retain(|partition| {
            owner_of
                .get(partition)
                .is_none_or(|owner| owner == member_id)
        });
    }

    assignment
}

/// The eligible member (subscribed to `topic`) with the fewest partitions so far,
/// breaking ties by member id.
fn least_loaded(
    sorted: &[&MemberSubscription],
    assignment: &HashMap<String, Vec<TopicPartition>>,
    topic: &str,
    subscribes: &impl Fn(&MemberSubscription, &str) -> bool,
) -> Option<String> {
    sorted
        .iter()
        .filter(|member| subscribes(member, topic))
        .min_by(|a, b| {
            let a_len = assignment.get(&a.member_id).map_or(0, Vec::len);
            let b_len = assignment.get(&b.member_id).map_or(0, Vec::len);
            a_len.cmp(&b_len).then(a.member_id.cmp(&b.member_id))
        })
        .map(|member| member.member_id.clone())
}

/// Move partitions off the most-loaded members onto lighter eligible members
/// until no member holds two more than an eligible peer — the balancing pass of
/// the sticky assignor. Bounded so it always terminates.
fn balance(
    assignment: &mut HashMap<String, Vec<TopicPartition>>,
    sorted: &[&MemberSubscription],
    subscribes: &impl Fn(&MemberSubscription, &str) -> bool,
    partition_count: usize,
) {
    let bound = partition_count
        .saturating_mul(sorted.len())
        .saturating_add(1);
    for _ in 0..bound {
        let max_len = assignment.values().map(Vec::len).max().unwrap_or(0);
        if max_len == 0 {
            return;
        }
        let mut moved = false;
        for donor in sorted {
            if assignment.get(&donor.member_id).map_or(0, Vec::len) != max_len {
                continue;
            }
            let donor_partitions = assignment
                .get(&donor.member_id)
                .cloned()
                .unwrap_or_default();
            for partition in &donor_partitions {
                // An acceptor helps only if it stays at least two lighter than the
                // donor after the move (so we never oscillate).
                let acceptor = sorted
                    .iter()
                    .filter(|member| {
                        member.member_id != donor.member_id
                            && subscribes(member, &partition.topic)
                            && assignment
                                .get(&member.member_id)
                                .map_or(0, Vec::len)
                                .saturating_add(1)
                                < max_len
                    })
                    .min_by(|a, b| {
                        let a_len = assignment.get(&a.member_id).map_or(0, Vec::len);
                        let b_len = assignment.get(&b.member_id).map_or(0, Vec::len);
                        a_len.cmp(&b_len).then(a.member_id.cmp(&b.member_id))
                    })
                    .map(|member| member.member_id.clone());
                if let Some(acceptor) = acceptor {
                    if let Some(list) = assignment.get_mut(&donor.member_id) {
                        list.retain(|held| held != partition);
                    }
                    if let Some(list) = assignment.get_mut(&acceptor) {
                        list.push(partition.clone());
                    }
                    moved = true;
                    break;
                }
            }
            if moved {
                break;
            }
        }
        if !moved {
            return;
        }
    }
}

/// Encode a `JoinGroup` subscription blob for the given topics, carrying the
/// partitions this member currently owns (used by the cooperative assignor).
pub(super) fn encode_subscription(topics: &[String], owned: &[TopicPartition]) -> Bytes {
    let subscription = ConsumerProtocolSubscriptionData {
        topics: topics.iter().map(|topic| topic.clone().into()).collect(),
        user_data: None,
        owned_partitions: owned_topics(owned),
        generation_id: -1,
        rack_id: None,
        _unknown_tagged_fields: Vec::new(),
    };
    let mut buf = BytesMut::new();
    buf.put_i16(CONSUMER_PROTOCOL_V1);
    // Infallible for this fixed, in-range payload; fall back to just the version.
    let _written = subscription.write(&mut buf, CONSUMER_PROTOCOL_V1);
    buf.freeze()
}

/// Group owned partitions by topic for the subscription blob.
fn owned_topics(owned: &[TopicPartition]) -> Vec<SubscriptionTopic> {
    let mut by_topic: BTreeMap<String, Vec<i32>> = BTreeMap::new();
    for partition in owned {
        by_topic
            .entry(partition.topic.clone())
            .or_default()
            .push(partition.partition);
    }
    by_topic
        .into_iter()
        .map(|(topic, partitions)| SubscriptionTopic {
            topic: topic.into(),
            partitions,
            _unknown_tagged_fields: Vec::new(),
        })
        .collect()
}

/// Decode the topics a member subscribed to from its subscription blob.
pub(super) fn decode_subscription(metadata: &Bytes) -> Vec<String> {
    if metadata.len() < 2 {
        return Vec::new();
    }
    let mut buf = metadata.clone();
    let version = buf.get_i16();
    let Ok(subscription) = ConsumerProtocolSubscriptionData::read(&mut buf, version) else {
        return Vec::new();
    };
    subscription
        .topics
        .into_iter()
        .map(|topic| topic.to_string())
        .collect()
}

/// Decode the partitions a member reported it currently owns (empty before v1).
pub(super) fn decode_owned(metadata: &Bytes) -> Vec<TopicPartition> {
    if metadata.len() < 2 {
        return Vec::new();
    }
    let mut buf = metadata.clone();
    let version = buf.get_i16();
    let Ok(subscription) = ConsumerProtocolSubscriptionData::read(&mut buf, version) else {
        return Vec::new();
    };
    subscription
        .owned_partitions
        .into_iter()
        .flat_map(|topic| {
            let name = topic.topic.to_string();
            topic
                .partitions
                .into_iter()
                .map(move |partition| TopicPartition::new(name.clone(), partition))
        })
        .collect()
}

/// Encode a `SyncGroup` assignment blob for one member's partitions.
pub(super) fn encode_assignment(partitions: &[TopicPartition]) -> Bytes {
    let mut by_topic: BTreeMap<String, Vec<i32>> = BTreeMap::new();
    for partition in partitions {
        by_topic
            .entry(partition.topic.clone())
            .or_default()
            .push(partition.partition);
    }
    let assigned_partitions = by_topic
        .into_iter()
        .map(|(topic, partitions)| AssignmentTopic {
            topic: topic.into(),
            partitions,
            _unknown_tagged_fields: Vec::new(),
        })
        .collect();
    let assignment = ConsumerProtocolAssignmentData {
        assigned_partitions,
        user_data: None,
        _unknown_tagged_fields: Vec::new(),
    };
    let mut buf = BytesMut::new();
    buf.put_i16(CONSUMER_PROTOCOL_V0);
    let _written = assignment.write(&mut buf, CONSUMER_PROTOCOL_V0);
    buf.freeze()
}

/// Decode a `SyncGroup` assignment blob into the assigned partitions.
pub(super) fn decode_assignment(assignment: &Bytes) -> Vec<TopicPartition> {
    if assignment.len() < 2 {
        return Vec::new();
    }
    let mut buf = assignment.clone();
    let version = buf.get_i16();
    let Ok(decoded) = ConsumerProtocolAssignmentData::read(&mut buf, version) else {
        return Vec::new();
    };
    decoded
        .assigned_partitions
        .into_iter()
        .flat_map(|topic| {
            let name = topic.topic.to_string();
            topic
                .partitions
                .into_iter()
                .map(move |partition| TopicPartition::new(name.clone(), partition))
        })
        .collect()
}

/// One group member's identity, subscribed topics, and currently owned
/// partitions (from its subscription blob), as seen by the leader.
#[derive(Debug, Clone)]
pub(super) struct MemberSubscription {
    pub member_id: String,
    pub topics: Vec<String>,
    /// Partitions this member reported it currently owns (cooperative rebalance).
    pub owned: Vec<TopicPartition>,
}

/// The `range` assignor: for each topic, lay its partitions out over the
/// subscribed members sorted by member id, giving each a contiguous range and
/// distributing any remainder to the earliest members. Mirrors Java's
/// `RangeAssignor`.
pub(super) fn range_assign(
    members: &[MemberSubscription],
    partitions_per_topic: &HashMap<String, i32>,
) -> HashMap<String, Vec<TopicPartition>> {
    let mut assignment: HashMap<String, Vec<TopicPartition>> = members
        .iter()
        .map(|member| (member.member_id.clone(), Vec::new()))
        .collect();

    // Collect the topics anyone subscribed to, in a stable order.
    let mut topics: Vec<String> = members
        .iter()
        .flat_map(|member| member.topics.iter().cloned())
        .collect();
    topics.sort();
    topics.dedup();

    for topic in topics {
        let Some(&partition_count) = partitions_per_topic.get(&topic) else {
            continue;
        };
        if partition_count <= 0 {
            continue;
        }
        let mut subscribers: Vec<&str> = members
            .iter()
            .filter(|member| member.topics.iter().any(|candidate| candidate == &topic))
            .map(|member| member.member_id.as_str())
            .collect();
        subscribers.sort_unstable();
        if subscribers.is_empty() {
            continue;
        }

        let consumer_count = i32::try_from(subscribers.len()).unwrap_or(i32::MAX);
        let per_consumer = partition_count.checked_div(consumer_count).unwrap_or(0);
        let with_extra = partition_count.checked_rem(consumer_count).unwrap_or(0);

        let mut start: i32 = 0;
        for (index, member_id) in subscribers.iter().enumerate() {
            let index = i32::try_from(index).unwrap_or(i32::MAX);
            let extra = i32::from(index < with_extra);
            let length = per_consumer.saturating_add(extra);
            if let Some(partitions) = assignment.get_mut(*member_id) {
                for partition in start..start.saturating_add(length) {
                    partitions.push(TopicPartition::new(topic.clone(), partition));
                }
            }
            start = start.saturating_add(length);
        }
    }

    assignment
}

#[cfg(test)]
mod tests {
    use super::*;

    fn member(id: &str, topics: &[&str]) -> MemberSubscription {
        MemberSubscription {
            member_id: id.to_owned(),
            topics: topics.iter().map(|topic| (*topic).to_owned()).collect(),
            owned: Vec::new(),
        }
    }

    fn owning(id: &str, topics: &[&str], owned: &[TopicPartition]) -> MemberSubscription {
        MemberSubscription {
            member_id: id.to_owned(),
            topics: topics.iter().map(|topic| (*topic).to_owned()).collect(),
            owned: owned.to_vec(),
        }
    }

    #[test]
    fn subscription_blob_round_trips() {
        let topics = vec!["a".to_owned(), "b".to_owned()];
        let blob = encode_subscription(&topics, &[]);
        assert_eq!(decode_subscription(&blob), topics);
    }

    #[test]
    fn subscription_blob_carries_owned_partitions() {
        let topics = vec!["t".to_owned()];
        let owned = vec![TopicPartition::new("t", 0), TopicPartition::new("t", 2)];
        let blob = encode_subscription(&topics, &owned);
        let mut decoded = decode_owned(&blob);
        decoded.sort_by_key(|partition| partition.partition);
        assert_eq!(decoded, owned);
    }

    #[test]
    fn cooperative_keeps_owner_and_withholds_moved_partitions() {
        // One member owns all four partitions; a second joins owning nothing.
        let all: Vec<TopicPartition> = (0..4).map(|p| TopicPartition::new("t", p)).collect();
        let members = vec![owning("m1", &["t"], &all), owning("m2", &["t"], &[])];
        let mut counts = HashMap::new();
        let _ = counts.insert("t".to_owned(), 4);
        let assignment = cooperative_sticky_assign(&members, &counts);
        // m1 keeps two, m2's two targets are withheld (still owned by m1).
        assert_eq!(assignment["m1"].len(), 2);
        assert!(assignment["m2"].is_empty());
        // No partition m1 kept overlaps what would move to m2 — no double ownership.
    }

    #[test]
    fn cooperative_assigns_freed_partitions_next_round() {
        // Round two: m1 has revoked down to two, so the other two are unowned.
        let m1_owned = vec![TopicPartition::new("t", 0), TopicPartition::new("t", 1)];
        let members = vec![owning("m1", &["t"], &m1_owned), owning("m2", &["t"], &[])];
        let mut counts = HashMap::new();
        let _ = counts.insert("t".to_owned(), 4);
        let assignment = cooperative_sticky_assign(&members, &counts);
        assert_eq!(assignment["m1"].len(), 2);
        assert_eq!(assignment["m2"].len(), 2);
        // m1 kept exactly what it owned.
        assert!(assignment["m1"].contains(&TopicPartition::new("t", 0)));
        assert!(assignment["m1"].contains(&TopicPartition::new("t", 1)));
    }

    #[test]
    fn cooperative_reclaims_partitions_of_a_departed_member() {
        // m2 left; only m1 rejoins owning its two. The freed two are unowned and
        // land immediately (no withhold needed).
        let m1_owned = vec![TopicPartition::new("t", 0), TopicPartition::new("t", 1)];
        let members = vec![owning("m1", &["t"], &m1_owned)];
        let mut counts = HashMap::new();
        let _ = counts.insert("t".to_owned(), 4);
        let assignment = cooperative_sticky_assign(&members, &counts);
        assert_eq!(assignment["m1"].len(), 4);
    }

    #[test]
    fn assignment_blob_round_trips() {
        let partitions = vec![
            TopicPartition::new("t", 0),
            TopicPartition::new("t", 2),
            TopicPartition::new("u", 1),
        ];
        let mut decoded = decode_assignment(&encode_assignment(&partitions));
        decoded
            .sort_by(|a, b| (a.topic.as_str(), a.partition).cmp(&(b.topic.as_str(), b.partition)));
        assert_eq!(decoded, partitions);
    }

    #[test]
    fn range_assign_splits_partitions_evenly() {
        let members = vec![member("m1", &["t"]), member("m2", &["t"])];
        let mut counts = HashMap::new();
        let _ = counts.insert("t".to_owned(), 4);
        let assignment = range_assign(&members, &counts);
        assert_eq!(assignment["m1"].len(), 2);
        assert_eq!(assignment["m2"].len(), 2);
        assert_eq!(assignment["m1"][0], TopicPartition::new("t", 0));
        assert_eq!(assignment["m2"][0], TopicPartition::new("t", 2));
    }

    #[test]
    fn range_assign_gives_remainder_to_earlier_members() {
        let members = vec![member("a", &["t"]), member("b", &["t"])];
        let mut counts = HashMap::new();
        let _ = counts.insert("t".to_owned(), 3);
        let assignment = range_assign(&members, &counts);
        assert_eq!(assignment["a"].len(), 2);
        assert_eq!(assignment["b"].len(), 1);
    }

    #[test]
    fn roundrobin_alternates_partitions_across_members() {
        let members = vec![member("a", &["t"]), member("b", &["t"])];
        let mut counts = HashMap::new();
        let _ = counts.insert("t".to_owned(), 4);
        let assignment = roundrobin_assign(&members, &counts);
        assert_eq!(
            assignment["a"],
            vec![TopicPartition::new("t", 0), TopicPartition::new("t", 2)]
        );
        assert_eq!(
            assignment["b"],
            vec![TopicPartition::new("t", 1), TopicPartition::new("t", 3)]
        );
    }

    #[test]
    fn roundrobin_skips_members_not_subscribed_to_the_topic() {
        let members = vec![member("a", &["t", "u"]), member("b", &["t"])];
        let mut counts = HashMap::new();
        let _ = counts.insert("t".to_owned(), 2);
        let _ = counts.insert("u".to_owned(), 2);
        let assignment = roundrobin_assign(&members, &counts);
        // Only `a` subscribes to `u`, so both its partitions go to `a`.
        assert!(assignment["a"].contains(&TopicPartition::new("u", 0)));
        assert!(assignment["a"].contains(&TopicPartition::new("u", 1)));
    }

    #[test]
    fn sticky_balances_partitions() {
        let members = vec![
            member("a", &["t"]),
            member("b", &["t"]),
            member("c", &["t"]),
        ];
        let mut counts = HashMap::new();
        let _ = counts.insert("t".to_owned(), 6);
        let assignment = sticky_assign(&members, &counts);
        assert_eq!(assignment["a"].len(), 2);
        assert_eq!(assignment["b"].len(), 2);
        assert_eq!(assignment["c"].len(), 2);
    }

    #[test]
    fn assign_dispatches_on_protocol_name() {
        let members = vec![member("a", &["t"])];
        let mut counts = HashMap::new();
        let _ = counts.insert("t".to_owned(), 2);
        assert_eq!(assign("roundrobin", &members, &counts)["a"].len(), 2);
        assert_eq!(assign("sticky", &members, &counts)["a"].len(), 2);
        assert_eq!(assign("range", &members, &counts)["a"].len(), 2);
        assert_eq!(protocol_name("RoundRobinAssignor"), "roundrobin");
        assert_eq!(protocol_name("unknown"), "range");
    }
}
