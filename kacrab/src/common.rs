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
mod node;
mod topic_partition;

pub use self::{
    consumer_group::ConsumerGroupMetadata,
    node::Node,
    topic_partition::{OffsetAndMetadata, TopicPartition},
};
