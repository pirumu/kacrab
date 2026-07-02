//! Shared Kafka-compatible domain types used across clients.
//!
//! These mirror the types Kafka places in `org.apache.kafka.common`
//! ([`TopicPartition`], [`OffsetAndMetadata`], [`ConsumerGroupMetadata`],
//! [`Node`]) and are always compiled, independent of the `producer`/`admin`
//! features, so every client surface can reuse them. The producer and admin
//! modules re-export the ones they expose at their original paths (e.g.
//! `kacrab::producer::TopicPartition`, `kacrab::admin::Node`) for backwards
//! compatibility.

mod consumer_group;
#[cfg(any(feature = "producer", feature = "admin"))]
mod coordinator;
mod node;
mod topic_partition;

#[cfg(any(feature = "producer", feature = "admin"))]
pub(crate) use self::coordinator::CoordinatorType;
pub use self::{
    consumer_group::ConsumerGroupMetadata,
    node::Node,
    topic_partition::{OffsetAndMetadata, TopicPartition},
};
