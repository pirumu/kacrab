//! Kafka share consumer (`share-consumer` feature) — the client side of share
//! groups (KIP-932).
//!
//! A share group turns a topic into a work queue. Instead of owning partitions
//! and advancing a position, a member *acquires* individual records under a
//! broker-held lock and disposes of each one on its own with an acknowledgement.
//! Three consequences shape this API and separate it from
//! [`Consumer`](super::Consumer):
//!
//! - **More consumers than partitions.** Assignment is not exclusive, so several members can hold
//!   the same partition, each acquiring a disjoint set of its records. There is nothing to
//!   reconcile and nothing to revoke.
//! - **Per-record disposition.** [`AcknowledgeType::Accept`] retires a record,
//!   [`Release`](AcknowledgeType::Release) hands it back for another attempt, and
//!   [`Reject`](AcknowledgeType::Reject) archives it without redelivery — the poison-message case a
//!   consumer group cannot express.
//! - **Delivery counts.** Every record arrives with the number of times the group has tried to
//!   deliver it ([`ShareRecord::delivery_count`]), which is what makes "reject after N attempts"
//!   implementable.
//!
//! There is no `seek`, no position, and no offset commit;
//! [`ShareConsumer::commit`] flushes acknowledgements, not offsets. The
//! admin-side view of the same groups already lives on
//! [`AdminClient`](crate::admin::AdminClient) (`describe_share_groups`,
//! `list_share_group_offsets`, and friends).
//!
//! # Wire protocol
//!
//! Three RPCs, all negotiated like any other:
//!
//! | RPC | key | versions | role |
//! | --- | ---: | --- | --- |
//! | `ShareGroupHeartbeat` | 76 | v1 | membership and assignment ([`membership`]) |
//! | `ShareFetch` | 78 | v1–2 | acquire records, piggy-back acknowledgements ([`session`]) |
//! | `ShareAcknowledge` | 79 | v1–2 | standalone acknowledgements, session close ([`session`]) |
//!
//! The share-coordinator state RPCs (`ReadShareGroupState` and friends) are
//! broker-to-coordinator internals; a client never issues them.
//!
//! Share groups are a production feature from Kafka 4.2 onward
//! (`ShareVersion.LATEST_PRODUCTION = SV_1`), so a 4.3 broker has them on by
//! default. Against a broker that does not, the first heartbeat comes back
//! `UNSUPPORTED_VERSION`.

mod client;
mod config;
mod membership;
mod record;
mod session;

pub use self::{
    client::{AcknowledgementCommitCallback, ShareConsumer},
    config::{ShareAcknowledgementMode, ShareAcquireMode, ShareRuntimeConfig},
    record::{AcknowledgeType, ShareRecord, ShareRecords},
};
