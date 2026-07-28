//! Topic-id ↔ topic-name resolution against cluster metadata.
//!
//! Both server-side group protocols hand out assignments keyed by topic *id*
//! (KIP-516) and take an owned/subscribed set keyed the same way, so the
//! consumer and the share consumer each have to translate in both directions
//! against the routing metadata. The translation is the same one; it lives here
//! rather than once per client.
//!
//! A nil id means the broker reported no stable id for the topic — every
//! resolver here treats that as "unknown" rather than as an id, because sending
//! it would key the request against a topic no broker knows.

use std::collections::BTreeMap;

use kacrab_protocol::KafkaUuid;

use crate::wire::ClusterMetadata;

/// Resolve an assignment's topic id to its name, or `None` when the routing
/// metadata does not know the id.
pub(super) fn topic_name_for_id(metadata: &ClusterMetadata, topic_id: KafkaUuid) -> Option<String> {
    metadata
        .topics
        .iter()
        .find(|topic| topic.topic_id == topic_id)
        .map(|topic| topic.name.clone())
}

/// Resolve a topic name to its id, or `None` when the broker reported no stable
/// id for it.
pub(super) fn topic_id_for_name(metadata: &ClusterMetadata, name: &str) -> Option<KafkaUuid> {
    metadata
        .topic(name)
        .map(|topic| topic.topic_id)
        .filter(|topic_id| !topic_id.is_nil())
}

/// A sortable stand-in for a topic id, so id-keyed groupings can use the same
/// ordered [`group_by_topic`] as name-keyed ones.
///
/// [`KafkaUuid`] is deliberately not `Ord` — Kafka gives topic ids no meaningful
/// order — so this is Java's own `Uuid(mostSignificantBits,
/// leastSignificantBits)` pair, used only to make grouping deterministic.
/// [`topic_id_from_key`] takes it back, losslessly.
pub(super) const fn topic_id_key(topic_id: KafkaUuid) -> (u64, u64) {
    (
        topic_id.most_significant_bits(),
        topic_id.least_significant_bits(),
    )
}

/// The topic id a [`topic_id_key`] came from.
pub(super) const fn topic_id_from_key((most, least): (u64, u64)) -> KafkaUuid {
    KafkaUuid::from_parts(most, least)
}

/// Group `(topic, partition-shaped item)` pairs by topic and build one wire
/// struct per topic.
///
/// Kafka request bodies are almost all a list of topics each holding a list of
/// partitions, while everything upstream of them — assignments, positions,
/// committed offsets — is a flat list of partitions. Every such request used to
/// grow its own accumulate-into-a-`Vec`-and-`iter_mut().find()` loop: nine of
/// them on the consumer side alone, quadratic in the topic count and each one a
/// separate chance to get the "topic already present" branch wrong.
///
/// Grouping through a `BTreeMap` also makes the request deterministic: topics
/// come out in key order instead of first-seen order (or, for the offset-commit
/// path, `HashMap` order). Partitions keep the order they were fed in.
pub(super) fn group_by_topic<K, P, T>(
    entries: impl IntoIterator<Item = (K, P)>,
    mut build: impl FnMut(K, Vec<P>) -> T,
) -> Vec<T>
where
    K: Ord,
{
    let mut by_topic: BTreeMap<K, Vec<P>> = BTreeMap::new();
    for (topic, partition) in entries {
        by_topic.entry(topic).or_default().push(partition);
    }
    by_topic.into_iter().map(|(k, v)| build(k, v)).collect()
}

#[cfg(test)]
mod tests {
    use super::{group_by_topic, topic_id_from_key, topic_id_key};
    use crate::common::TopicPartition;

    #[test]
    fn grouping_orders_topics_by_key_and_keeps_partition_order() {
        let partitions = [
            TopicPartition::new("b", 2),
            TopicPartition::new("a", 7),
            TopicPartition::new("b", 0),
            TopicPartition::new("a", 1),
        ];

        let grouped: Vec<(String, Vec<i32>)> = group_by_topic(
            partitions
                .iter()
                .map(|partition| (partition.topic.clone(), partition.partition)),
            |topic, indexes| (topic, indexes),
        );

        assert_eq!(
            grouped,
            vec![("a".to_owned(), vec![7, 1]), ("b".to_owned(), vec![2, 0]),],
            "topics sort by name; partitions keep the order they arrived in"
        );
    }

    #[test]
    fn a_topic_id_survives_the_round_trip_through_its_ordering_key() {
        let topic_id = kacrab_protocol::KafkaUuid::random().expect("random topic id");

        assert_eq!(topic_id_from_key(topic_id_key(topic_id)), topic_id);
    }
}
