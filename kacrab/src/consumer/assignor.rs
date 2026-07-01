//! Client-side partition assignors and the `ConsumerProtocol` subscription /
//! assignment codec (classic group protocol).
//!
//! Ships eager assignment with the `range`, `roundrobin`, and `sticky` assignors,
//! selected by `partition.assignment.strategy` (advertised to the coordinator,
//! which picks the one common to all members). The incremental `cooperative-
//! sticky` protocol is a later refinement. Subscription/assignment blobs are the
//! version-prefixed `ConsumerProtocol` encoding the broker relays between
//! members, encoded at v0 (eager: no owned-partitions) and decoded at whatever
//! version the group leader wrote.

use std::collections::{BTreeMap, HashMap};

use bytes::{Buf as _, BufMut as _, Bytes, BytesMut};
use kacrab_protocol::generated::{
    ConsumerProtocolAssignmentData, ConsumerProtocolSubscriptionData,
    consumer_protocol_assignment::TopicPartition as AssignmentTopic,
};

use crate::common::TopicPartition;

/// Kafka `protocol.type` for consumer groups.
pub(super) const PROTOCOL_TYPE: &str = "consumer";
/// The default `range` assignor.
pub(super) const RANGE_ASSIGNOR: &str = "range";
/// `ConsumerProtocol` version used when encoding our own blobs (eager).
const CONSUMER_PROTOCOL_V0: i16 = 0;

/// Map a `partition.assignment.strategy` entry to the protocol name kacrab
/// advertises to the coordinator. Unknown entries fall back to `range`.
pub(super) fn protocol_name(strategy: &str) -> &'static str {
    match strategy.trim().to_ascii_lowercase().as_str() {
        "roundrobin" | "round_robin" | "roundrobinassignor" => "roundrobin",
        "sticky" | "stickyassignor" => "sticky",
        _ => RANGE_ASSIGNOR,
    }
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

/// Encode a `JoinGroup` subscription blob for the given topics.
pub(super) fn encode_subscription(topics: &[String]) -> Bytes {
    let subscription = ConsumerProtocolSubscriptionData {
        topics: topics.iter().map(|topic| topic.clone().into()).collect(),
        user_data: None,
        owned_partitions: Vec::new(),
        generation_id: -1,
        rack_id: None,
        _unknown_tagged_fields: Vec::new(),
    };
    let mut buf = BytesMut::new();
    buf.put_i16(CONSUMER_PROTOCOL_V0);
    // Infallible for this fixed, in-range payload; fall back to just the version.
    let _written = subscription.write(&mut buf, CONSUMER_PROTOCOL_V0);
    buf.freeze()
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

/// One group member's identity and subscribed topics, as seen by the leader.
#[derive(Debug, Clone)]
pub(super) struct MemberSubscription {
    pub member_id: String,
    pub topics: Vec<String>,
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
        }
    }

    #[test]
    fn subscription_blob_round_trips() {
        let topics = vec!["a".to_owned(), "b".to_owned()];
        let decoded = decode_subscription(&encode_subscription(&topics));
        assert_eq!(decoded, topics);
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
