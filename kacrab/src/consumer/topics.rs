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
