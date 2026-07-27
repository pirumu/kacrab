//! Share sessions, acknowledgement batching, and `ShareFetch` decoding.
//!
//! A share session is the KIP-932 analogue of the incremental fetch session
//! (KIP-227), with two differences that matter here. It is keyed by
//! `(group, member, connection)` rather than by a broker-assigned session id, so
//! there is no `session_id` to echo — only an epoch: `0` opens the session, each
//! answered request advances it, and `-1` closes it. And it is the *only* channel
//! for acknowledgements: every `ShareFetch` after the first can piggy-back the
//! acknowledgements for the records the previous one acquired, which is what
//! keeps the acknowledgement path off a per-record round trip.
//!
//! Acknowledgements are collected per partition as `offset -> type` and lowered
//! to the wire form at send time: contiguous offset runs become one
//! `AcknowledgementBatch`, and a run whose offsets all share a type collapses to
//! a single-element `acknowledge_types` array (the broker reads a one-element
//! array as "this type, for the whole range" —
//! `SharePartition.fetchAckTypeMapForBatch`).

use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::Duration,
};

use bytes::Bytes;
use kacrab_protocol::{
    KafkaUuid,
    generated::{
        ErrorCode, ShareAcknowledgeRequestData, ShareFetchRequestData, ShareFetchResponseData,
        share_acknowledge_request::{
            AcknowledgePartition, AcknowledgeTopic, AcknowledgementBatch as AcknowledgeRequestBatch,
        },
        share_fetch_request::{
            AcknowledgementBatch as FetchRequestBatch, FetchPartition, FetchTopic, ForgottenTopic,
        },
    },
    record::decode_next_batch,
};

use super::record::{ACKNOWLEDGE_GAP, ShareRecord};
use crate::{
    common::TopicPartition,
    consumer::{
        error::{ConsumerError, Result},
        fetch::batch_records,
    },
};

/// Share session epoch that opens a session.
const INITIAL_EPOCH: i32 = 0;
/// Share session epoch that closes a session.
const FINAL_EPOCH: i32 = -1;

/// A partition together with the topic id the session knows it by. Share
/// requests are topic-id keyed from v1, so a recreated topic changes identity
/// and its old entry has to be forgotten.
pub(super) type TopicIdPartition = (TopicPartition, KafkaUuid);

/// Pending acknowledgements for one partition, ordered by offset.
pub(super) type PartitionAcknowledgements = BTreeMap<i64, i8>;

/// All pending acknowledgements, keyed by partition.
pub(super) type Acknowledgements = HashMap<TopicPartition, PartitionAcknowledgements>;

/// The next share session epoch, mirroring `ShareRequestMetadata.nextEpoch`:
/// a closed session stays closed, and the counter wraps to `1` rather than
/// through `0` (which would read as "open a new session").
const fn next_epoch(previous: i32) -> i32 {
    if previous < 0 {
        FINAL_EPOCH
    } else if previous == i32::MAX {
        1
    } else {
        previous.saturating_add(1)
    }
}

/// One broker's share session.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(super) struct ShareSession {
    epoch: i32,
    partitions: Vec<TopicIdPartition>,
}

impl ShareSession {
    /// Whether the next request opens a session rather than continuing one.
    ///
    /// The broker rejects a session-opening request that carries
    /// acknowledgements, so the caller must not piggy-back any onto it.
    pub(super) const fn is_new(&self) -> bool {
        self.epoch == INITIAL_EPOCH
    }

    /// The epoch to put on the next request.
    pub(super) const fn epoch(&self) -> i32 {
        self.epoch
    }

    /// Forget everything and re-open with a full request. Used when the broker
    /// reports the session unknown or the epoch invalid, and after a send
    /// failure — all of which mean the broker no longer holds our acquisitions.
    pub(super) fn reset(&mut self) {
        self.epoch = INITIAL_EPOCH;
        self.partitions.clear();
    }

    /// Advance after the broker answered.
    pub(super) const fn advance(&mut self) {
        self.epoch = next_epoch(self.epoch);
    }

    /// Mark the session for closing: the next request carries the final epoch.
    pub(super) const fn close(&mut self) {
        self.epoch = FINAL_EPOCH;
    }

    /// Diff `wanted` against the session, returning the partitions to send and
    /// the ones to forget, and adopting `wanted` as the new session contents.
    ///
    /// A new session sends everything. A continuing one sends only partitions
    /// the broker has not seen, forgets the ones that left the assignment, and
    /// treats a changed topic id (a recreated topic) as a forget plus an add.
    pub(super) fn plan(
        &mut self,
        wanted: &[TopicIdPartition],
    ) -> (Vec<TopicIdPartition>, Vec<TopicIdPartition>) {
        if self.is_new() {
            self.partitions = wanted.to_vec();
            return (wanted.to_vec(), Vec::new());
        }
        let mut added = Vec::new();
        let mut forgotten = Vec::new();
        for (partition, topic_id) in wanted {
            match self
                .partitions
                .iter()
                .find(|(known, _)| known == partition)
                .map(|(_, known_id)| *known_id)
            {
                Some(known_id) if known_id == *topic_id => {},
                Some(known_id) => {
                    forgotten.push((partition.clone(), known_id));
                    added.push((partition.clone(), *topic_id));
                },
                None => added.push((partition.clone(), *topic_id)),
            }
        }
        for entry in &self.partitions {
            if !wanted.iter().any(|(partition, _)| *partition == entry.0) {
                forgotten.push(entry.clone());
            }
        }
        self.partitions = wanted.to_vec();
        (added, forgotten)
    }
}

/// Lower one partition's `offset -> type` acknowledgements to wire ranges.
///
/// Contiguous offsets share a range; a range whose offsets all carry the same
/// type collapses to a one-element type array.
fn acknowledgement_ranges(
    acknowledgements: &PartitionAcknowledgements,
) -> Vec<(i64, i64, Vec<i8>)> {
    let mut ranges: Vec<(i64, i64, Vec<i8>)> = Vec::new();
    for (offset, kind) in acknowledgements {
        match ranges.last_mut() {
            Some(range) if range.1.saturating_add(1) == *offset => {
                range.1 = *offset;
                range.2.push(*kind);
            },
            _ => ranges.push((*offset, *offset, vec![*kind])),
        }
    }
    for range in &mut ranges {
        if let Some(first) = range.2.first().copied()
            && range.2.iter().all(|kind| *kind == first)
        {
            range.2.truncate(1);
        }
    }
    ranges
}

/// The inputs one broker's `ShareFetch` needs beyond its session state.
pub(super) struct ShareFetchPlan<'a> {
    pub group_id: &'a str,
    pub member_id: &'a str,
    pub max_wait_ms: i32,
    pub min_bytes: i32,
    pub max_bytes: i32,
    pub max_records: i32,
    pub acquire_mode: i8,
    /// The partitions this broker leads, with their topic ids.
    pub wanted: &'a [TopicIdPartition],
    /// Acknowledgements to piggy-back, keyed by partition. Must be empty when
    /// the session is new — the broker rejects that combination.
    pub acknowledgements: &'a Acknowledgements,
}

/// Build one broker's `ShareFetch`, advancing the session's partition set.
pub(super) fn build_share_fetch(
    session: &mut ShareSession,
    plan: &ShareFetchPlan<'_>,
) -> ShareFetchRequestData {
    let (added, forgotten) = session.plan(plan.wanted);
    let topic_ids: HashMap<&TopicPartition, KafkaUuid> = plan
        .wanted
        .iter()
        .map(|(partition, topic_id)| (partition, *topic_id))
        .collect();

    // Grouped by topic id, fetch entries first, then acknowledgement-only
    // partitions folded into the same entries (Java's `forConsumer`).
    let mut topics: FetchTopics = FetchTopics::new();
    for (partition, topic_id) in &added {
        let _entry = fetch_partition_entry(&mut topics, *topic_id, partition.partition);
    }
    for (partition, acknowledgements) in plan.acknowledgements {
        let Some(topic_id) = topic_ids.get(partition).copied() else {
            continue;
        };
        let entry = fetch_partition_entry(&mut topics, topic_id, partition.partition);
        entry.acknowledgement_batches = acknowledgement_ranges(acknowledgements)
            .into_iter()
            .map(
                |(first_offset, last_offset, acknowledge_types)| FetchRequestBatch {
                    first_offset,
                    last_offset,
                    acknowledge_types,
                    _unknown_tagged_fields: Vec::new(),
                },
            )
            .collect();
    }

    ShareFetchRequestData {
        group_id: Some(plan.group_id.to_owned().into()),
        member_id: Some(plan.member_id.to_owned().into()),
        share_session_epoch: session.epoch(),
        max_wait_ms: plan.max_wait_ms,
        min_bytes: plan.min_bytes,
        max_bytes: plan.max_bytes,
        max_records: plan.max_records,
        batch_size: plan.max_records,
        share_acquire_mode: plan.acquire_mode,
        is_renew_ack: false,
        topics: topics
            .into_iter()
            .map(|(topic_id, partitions)| FetchTopic {
                topic_id,
                partitions: partitions.into_values().collect(),
                _unknown_tagged_fields: Vec::new(),
            })
            .collect(),
        forgotten_topics_data: build_forgotten(&forgotten),
        _unknown_tagged_fields: Vec::new(),
    }
}

/// Build one broker's `ShareAcknowledge` — the standalone acknowledgement path
/// used by `commit` and by `close`, where there is no fetch to ride on.
pub(super) fn build_share_acknowledge(
    session: &ShareSession,
    group_id: &str,
    member_id: &str,
    topic_ids: &HashMap<TopicPartition, KafkaUuid>,
    acknowledgements: &Acknowledgements,
) -> ShareAcknowledgeRequestData {
    let mut topics: BTreeMap<KafkaUuid, BTreeMap<i32, AcknowledgePartition>> = BTreeMap::new();
    for (partition, partition_acknowledgements) in acknowledgements {
        let Some(topic_id) = topic_ids.get(partition).copied() else {
            continue;
        };
        let batches = acknowledgement_ranges(partition_acknowledgements)
            .into_iter()
            .map(
                |(first_offset, last_offset, acknowledge_types)| AcknowledgeRequestBatch {
                    first_offset,
                    last_offset,
                    acknowledge_types,
                    _unknown_tagged_fields: Vec::new(),
                },
            )
            .collect();
        let _previous = topics.entry(topic_id).or_default().insert(
            partition.partition,
            AcknowledgePartition {
                partition_index: partition.partition,
                acknowledgement_batches: batches,
                _unknown_tagged_fields: Vec::new(),
            },
        );
    }

    ShareAcknowledgeRequestData {
        group_id: Some(group_id.to_owned().into()),
        member_id: Some(member_id.to_owned().into()),
        share_session_epoch: session.epoch(),
        is_renew_ack: false,
        topics: topics
            .into_iter()
            .map(|(topic_id, partitions)| AcknowledgeTopic {
                topic_id,
                partitions: partitions.into_values().collect(),
                _unknown_tagged_fields: Vec::new(),
            })
            .collect(),
        _unknown_tagged_fields: Vec::new(),
    }
}

/// `ShareFetch` topics under construction, keyed by topic id then partition
/// index so a partition that is both fetched and acknowledged lands in one entry.
type FetchTopics = BTreeMap<KafkaUuid, BTreeMap<i32, FetchPartition>>;

fn fetch_partition_entry(
    topics: &mut FetchTopics,
    topic_id: KafkaUuid,
    partition_index: i32,
) -> &mut FetchPartition {
    topics
        .entry(topic_id)
        .or_default()
        .entry(partition_index)
        .or_insert_with(|| FetchPartition {
            partition_index,
            ..FetchPartition::default()
        })
}

fn build_forgotten(forgotten: &[TopicIdPartition]) -> Vec<ForgottenTopic> {
    let mut topics: Vec<ForgottenTopic> = Vec::new();
    for (partition, topic_id) in forgotten {
        match topics.iter_mut().find(|topic| topic.topic_id == *topic_id) {
            Some(topic) => topic.partitions.push(partition.partition),
            None => topics.push(ForgottenTopic {
                topic_id: *topic_id,
                partitions: vec![partition.partition],
                _unknown_tagged_fields: Vec::new(),
            }),
        }
    }
    topics
}

/// One partition's worth of a decoded `ShareFetch` response.
#[derive(Debug, Default)]
pub(super) struct SharePartitionOutcome {
    /// The acquired records, in offset order.
    pub records: Vec<ShareRecord>,
    /// Offsets the broker acquired for us that carried no deliverable record —
    /// compacted away, or a control record. They still hold an acquisition lock,
    /// so they are owed a `Gap` acknowledgement.
    pub gaps: Vec<i64>,
}

/// The aggregate outcome of one broker's `ShareFetch`.
#[derive(Debug, Default)]
pub(super) struct ShareFetchOutcome {
    /// Acquired data per partition.
    pub partitions: Vec<(TopicPartition, SharePartitionOutcome)>,
    /// Partitions whose leader or topic metadata looked stale — refresh
    /// metadata and try again on the next poll.
    pub stale: Vec<TopicPartition>,
    /// The first partition-level acknowledgement failure, if any. Surfaced to
    /// the caller: a rejected acknowledgement means those records keep their
    /// lock and will be redelivered.
    pub acknowledge_error: Option<(TopicPartition, ErrorCode)>,
    /// How long the broker holds the acquisition lock on the records in this
    /// response — the budget an application has to process and acknowledge them
    /// before they become re-deliverable (Java's `acquisitionLockTimeoutMs`).
    pub acquisition_lock_timeout: Option<Duration>,
}

/// The broker's acquisition-lock budget as a [`Duration`], or `None` when the
/// response did not carry one (the field is zero on a response that acquired
/// nothing).
pub(super) fn acquisition_lock_timeout(milliseconds: i32) -> Option<Duration> {
    u64::try_from(milliseconds)
        .ok()
        .filter(|value| *value > 0)
        .map(Duration::from_millis)
}

/// Whether the top-level code means the share session itself is gone, so it must
/// be re-opened from the initial epoch.
pub(super) const fn is_session_lost(error: ErrorCode) -> bool {
    matches!(
        error,
        ErrorCode::ShareSessionNotFound
            | ErrorCode::InvalidShareSessionEpoch
            | ErrorCode::ShareSessionLimitReached
    )
}

/// Whether a partition-level share-fetch error is recoverable by refreshing
/// metadata. Unlike the classic fetcher there is no `OFFSET_OUT_OF_RANGE` case:
/// a share consumer has no position to reset.
const fn is_retriable_partition_error(error: ErrorCode) -> bool {
    matches!(
        error,
        ErrorCode::NotLeaderOrFollower
            | ErrorCode::FencedLeaderEpoch
            | ErrorCode::UnknownLeaderEpoch
            | ErrorCode::UnknownTopicOrPartition
            | ErrorCode::UnknownTopicId
            | ErrorCode::InconsistentTopicId
            | ErrorCode::ReplicaNotAvailable
            | ErrorCode::LeaderNotAvailable
            | ErrorCode::KafkaStorageError
    )
}

/// Decode one broker's `ShareFetch` response into acquired records.
///
/// Only offsets the broker reported as *acquired* are returned: the response
/// carries whole record batches, so it can contain records that belong to
/// another member. An acquired offset with no deliverable record at it is a gap.
pub(super) fn decode_share_fetch(
    response: ShareFetchResponseData,
    topic_names: &HashMap<KafkaUuid, String>,
) -> Result<ShareFetchOutcome> {
    let mut outcome = ShareFetchOutcome {
        acquisition_lock_timeout: acquisition_lock_timeout(response.acquisition_lock_timeout_ms),
        ..ShareFetchOutcome::default()
    };
    for topic in response.responses {
        let Some(name) = topic_names.get(&topic.topic_id) else {
            continue;
        };
        let topic_handle: Arc<str> = Arc::from(name.as_str());
        for partition in topic.partitions {
            let tp = TopicPartition::new(name.clone(), partition.partition_index);
            let error = ErrorCode::from(partition.error_code);
            if error.is_error() {
                if is_retriable_partition_error(error) {
                    outcome.stale.push(tp);
                    continue;
                }
                return Err(ConsumerError::broker(
                    "share_fetch",
                    error,
                    format!("{}-{} share fetch failed", tp.topic, tp.partition),
                ));
            }
            let acknowledge_error = ErrorCode::from(partition.acknowledge_error_code);
            if acknowledge_error.is_error() && outcome.acknowledge_error.is_none() {
                outcome.acknowledge_error = Some((tp.clone(), acknowledge_error));
            }
            if partition.acquired_records.is_empty() {
                continue;
            }
            let acquired = acquired_offsets(&partition.acquired_records);
            let decoded = decode_records(
                partition.records.unwrap_or_default(),
                &topic_handle,
                partition.partition_index,
            )?;
            outcome
                .partitions
                .push((tp, select_acquired(acquired, decoded)));
        }
    }
    Ok(outcome)
}

/// Expand the acquired ranges to `(offset, delivery_count)` in offset order,
/// dropping any offset a (malformed) response listed twice.
fn acquired_offsets(
    acquired: &[kacrab_protocol::generated::share_fetch_response::AcquiredRecords],
) -> BTreeMap<i64, i16> {
    let mut offsets = BTreeMap::new();
    for range in acquired {
        let mut offset = range.first_offset;
        while offset <= range.last_offset {
            let _previous = offsets.entry(offset).or_insert(range.delivery_count);
            let Some(next) = offset.checked_add(1) else {
                break;
            };
            offset = next;
        }
    }
    offsets
}

/// Decode every record batch in the blob, keyed by offset.
///
/// Control batches are skipped: their offsets can be acquired but they carry
/// nothing an application can consume, so they end up as gaps.
fn decode_records(
    mut blob: Bytes,
    topic: &Arc<str>,
    partition: i32,
) -> Result<BTreeMap<i64, crate::consumer::record::ConsumerRecord>> {
    let mut decoded = BTreeMap::new();
    while !blob.is_empty() {
        let batch = decode_next_batch(&mut blob).map_err(|_error| {
            ConsumerError::InvalidState("failed to decode an acquired record batch")
        })?;
        // A truncated trailing batch is normal at the end of a response.
        let Some(batch) = batch else { break };
        if batch.is_control_batch() {
            continue;
        }
        let (records, _leader_epoch) = batch_records(batch, topic, partition);
        for record in records {
            let _previous = decoded.insert(record.offset, record);
        }
    }
    Ok(decoded)
}

/// Pair acquired offsets with their records; acquired offsets with no record
/// become gaps.
fn select_acquired(
    acquired: BTreeMap<i64, i16>,
    mut decoded: BTreeMap<i64, crate::consumer::record::ConsumerRecord>,
) -> SharePartitionOutcome {
    let mut outcome = SharePartitionOutcome::default();
    for (offset, delivery_count) in acquired {
        match decoded.remove(&offset) {
            Some(record) => outcome.records.push(ShareRecord {
                record,
                delivery_count,
            }),
            None => outcome.gaps.push(offset),
        }
    }
    outcome
}

/// The `Gap` acknowledgements owed for a partition's unfilled acquired offsets.
pub(super) fn gap_acknowledgements(gaps: &[i64]) -> Vec<(i64, i8)> {
    gaps.iter()
        .map(|offset| (*offset, ACKNOWLEDGE_GAP))
        .collect()
}

#[cfg(test)]
mod tests {
    use kacrab_protocol::{
        generated::share_fetch_response::{
            AcquiredRecords, PartitionData, ShareFetchableTopicResponse,
        },
        record::{Record, RecordBatch},
    };

    use super::*;
    use crate::consumer::share::record::AcknowledgeType;

    fn uuid(byte: u8) -> KafkaUuid {
        KafkaUuid::from_parts(u64::from(byte), u64::from(byte))
    }

    fn batch(base_offset: i64, count: usize) -> Bytes {
        let records = (0..count)
            .map(|index| Record {
                attributes: 0,
                offset_delta: i32::try_from(index).expect("small"),
                timestamp_delta: 0,
                key: None,
                value: Some(Bytes::from(format!("v{index}"))),
                headers: Vec::new(),
            })
            .collect();
        let batch = RecordBatch {
            base_offset,
            partition_leader_epoch: 7,
            magic: 2,
            attributes: 0,
            last_offset_delta: i32::try_from(count).expect("small").saturating_sub(1),
            first_timestamp: 100,
            max_timestamp: 100,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            records,
        };
        let mut buffer = bytes::BytesMut::new();
        batch.encode(&mut buffer).expect("encode");
        buffer.freeze()
    }

    #[test]
    fn epochs_open_advance_wrap_and_close() {
        let mut session = ShareSession::default();
        assert!(session.is_new());
        assert_eq!(session.epoch(), 0);
        session.advance();
        assert!(!session.is_new());
        assert_eq!(session.epoch(), 1);
        session.close();
        assert_eq!(session.epoch(), FINAL_EPOCH);
        // A closed session stays closed.
        session.advance();
        assert_eq!(session.epoch(), FINAL_EPOCH);
        session.reset();
        assert!(session.is_new());
        // The counter wraps past MAX to 1, never back through 0.
        assert_eq!(next_epoch(i32::MAX), 1);
    }

    #[test]
    fn a_new_session_sends_everything_and_a_continuing_one_only_the_delta() {
        let mut session = ShareSession::default();
        let first = (TopicPartition::new("jobs", 0), uuid(1));
        let second = (TopicPartition::new("jobs", 1), uuid(1));

        let (added, forgotten) = session.plan(std::slice::from_ref(&first));
        assert_eq!(added.len(), 1);
        assert!(forgotten.is_empty());

        session.advance();
        let (added, forgotten) = session.plan(&[first.clone(), second.clone()]);
        assert_eq!(added, vec![second.clone()]);
        assert!(forgotten.is_empty());

        // Dropping a partition forgets it.
        let (added, forgotten) = session.plan(std::slice::from_ref(&second));
        assert!(added.is_empty());
        assert_eq!(forgotten, vec![first.clone()]);

        // A recreated topic (new id) is a forget plus an add.
        let recreated = (TopicPartition::new("jobs", 1), uuid(2));
        let (added, forgotten) = session.plan(std::slice::from_ref(&recreated));
        assert_eq!(added, vec![recreated]);
        assert_eq!(forgotten, vec![second]);
    }

    #[test]
    fn contiguous_offsets_with_one_type_collapse_to_a_single_element_array() {
        let mut acknowledgements = PartitionAcknowledgements::new();
        for offset in 0..5 {
            let _previous = acknowledgements.insert(offset, AcknowledgeType::Accept.wire());
        }
        assert_eq!(
            acknowledgement_ranges(&acknowledgements),
            vec![(0, 4, vec![1])]
        );
    }

    #[test]
    fn mixed_types_stay_per_offset_and_a_hole_starts_a_new_range() {
        let acknowledgements: PartitionAcknowledgements = [
            (0, AcknowledgeType::Accept.wire()),
            (1, AcknowledgeType::Release.wire()),
            (2, AcknowledgeType::Reject.wire()),
            // 3 is missing: not ours, so the range breaks.
            (4, ACKNOWLEDGE_GAP),
            (5, ACKNOWLEDGE_GAP),
        ]
        .into_iter()
        .collect();
        assert_eq!(
            acknowledgement_ranges(&acknowledgements),
            vec![(0, 2, vec![1, 2, 3]), (4, 5, vec![0])]
        );
    }

    #[test]
    fn a_fetch_carries_the_session_epoch_added_partitions_and_piggybacked_acks() {
        let mut session = ShareSession::default();
        let partition = TopicPartition::new("jobs", 0);
        let wanted = vec![(partition.clone(), uuid(3))];

        let request = build_share_fetch(
            &mut session,
            &ShareFetchPlan {
                group_id: "work-queue",
                member_id: "member",
                max_wait_ms: 500,
                min_bytes: 1,
                max_bytes: 1024,
                max_records: 10,
                acquire_mode: 1,
                wanted: &wanted,
                acknowledgements: &Acknowledgements::new(),
            },
        );
        assert_eq!(request.share_session_epoch, 0);
        assert_eq!(request.topics.len(), 1);
        assert_eq!(request.topics[0].partitions.len(), 1);
        assert!(
            request.topics[0].partitions[0]
                .acknowledgement_batches
                .is_empty()
        );
        assert_eq!(request.batch_size, request.max_records);
        assert_eq!(request.share_acquire_mode, 1);
        assert!(!request.is_renew_ack);

        session.advance();
        let acknowledgements: Acknowledgements = Acknowledgements::from([(
            partition,
            PartitionAcknowledgements::from([(0, AcknowledgeType::Accept.wire())]),
        )]);
        let request = build_share_fetch(
            &mut session,
            &ShareFetchPlan {
                group_id: "work-queue",
                member_id: "member",
                max_wait_ms: 500,
                min_bytes: 1,
                max_bytes: 1024,
                max_records: 10,
                acquire_mode: 0,
                wanted: &wanted,
                acknowledgements: &acknowledgements,
            },
        );
        assert_eq!(request.share_session_epoch, 1);
        // The partition is already in the session, so it is only listed to carry
        // its acknowledgements.
        assert_eq!(
            request.topics[0].partitions[0].acknowledgement_batches,
            vec![FetchRequestBatch {
                first_offset: 0,
                last_offset: 0,
                acknowledge_types: vec![1],
                _unknown_tagged_fields: Vec::new(),
            }]
        );
    }

    #[test]
    fn acknowledge_only_partitions_group_by_topic_id() {
        let session = ShareSession::default();
        let topic_ids: HashMap<TopicPartition, KafkaUuid> = [
            (TopicPartition::new("jobs", 0), uuid(4)),
            (TopicPartition::new("jobs", 1), uuid(4)),
        ]
        .into_iter()
        .collect();
        let acknowledgements: Acknowledgements = topic_ids
            .keys()
            .map(|partition| {
                (
                    partition.clone(),
                    PartitionAcknowledgements::from([(9, AcknowledgeType::Release.wire())]),
                )
            })
            .collect();

        let request = build_share_acknowledge(
            &session,
            "work-queue",
            "member",
            &topic_ids,
            &acknowledgements,
        );
        assert_eq!(request.topics.len(), 1);
        assert_eq!(request.topics[0].partitions.len(), 2);
        assert_eq!(
            request.topics[0].partitions[0].acknowledgement_batches[0].acknowledge_types,
            vec![2]
        );
    }

    #[test]
    fn only_acquired_offsets_are_delivered_and_the_rest_become_gaps() {
        let topic_id = uuid(5);
        let response = ShareFetchResponseData {
            responses: vec![ShareFetchableTopicResponse {
                topic_id,
                partitions: vec![PartitionData {
                    partition_index: 0,
                    // The broker returns whole batches, so offsets 0..4 arrive
                    // but only 1..=2 and 7 are acquired for this member.
                    records: Some(batch(0, 5)),
                    acquired_records: vec![
                        AcquiredRecords {
                            first_offset: 1,
                            last_offset: 2,
                            delivery_count: 1,
                            _unknown_tagged_fields: Vec::new(),
                        },
                        AcquiredRecords {
                            first_offset: 7,
                            last_offset: 7,
                            delivery_count: 3,
                            _unknown_tagged_fields: Vec::new(),
                        },
                    ],
                    ..PartitionData::default()
                }],
                _unknown_tagged_fields: Vec::new(),
            }],
            ..ShareFetchResponseData::default()
        };
        let names: HashMap<KafkaUuid, String> = HashMap::from([(topic_id, "jobs".to_owned())]);

        let outcome = decode_share_fetch(response, &names).expect("decodes");
        assert_eq!(outcome.partitions.len(), 1);
        let (partition, data) = &outcome.partitions[0];
        assert_eq!(*partition, TopicPartition::new("jobs", 0));
        assert_eq!(
            data.records
                .iter()
                .map(ShareRecord::offset)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(data.records[0].delivery_count, 1);
        // Offset 7 was acquired but no record arrived for it — a gap we owe an
        // acknowledgement for.
        assert_eq!(data.gaps, vec![7]);
        assert_eq!(gap_acknowledgements(&data.gaps), vec![(7, ACKNOWLEDGE_GAP)]);
        assert!(outcome.acknowledge_error.is_none());
    }

    #[test]
    fn a_retriable_partition_error_marks_the_partition_stale_and_a_fatal_one_fails() {
        let topic_id = uuid(6);
        let names: HashMap<KafkaUuid, String> = HashMap::from([(topic_id, "jobs".to_owned())]);
        let with_error = |code: ErrorCode| ShareFetchResponseData {
            responses: vec![ShareFetchableTopicResponse {
                topic_id,
                partitions: vec![PartitionData {
                    partition_index: 0,
                    error_code: code.code(),
                    ..PartitionData::default()
                }],
                _unknown_tagged_fields: Vec::new(),
            }],
            ..ShareFetchResponseData::default()
        };

        let outcome =
            decode_share_fetch(with_error(ErrorCode::NotLeaderOrFollower), &names).expect("stale");
        assert_eq!(outcome.stale, vec![TopicPartition::new("jobs", 0)]);
        assert!(outcome.partitions.is_empty());

        let error = decode_share_fetch(with_error(ErrorCode::TopicAuthorizationFailed), &names)
            .expect_err("fatal");
        assert!(matches!(
            error,
            ConsumerError::Broker {
                error: ErrorCode::TopicAuthorizationFailed,
                ..
            }
        ));
    }

    #[test]
    fn an_acknowledge_error_is_reported_alongside_the_records() {
        let topic_id = uuid(7);
        let names: HashMap<KafkaUuid, String> = HashMap::from([(topic_id, "jobs".to_owned())]);
        let response = ShareFetchResponseData {
            responses: vec![ShareFetchableTopicResponse {
                topic_id,
                partitions: vec![PartitionData {
                    partition_index: 0,
                    acknowledge_error_code: ErrorCode::InvalidRecordState.code(),
                    records: Some(batch(0, 1)),
                    acquired_records: vec![AcquiredRecords {
                        first_offset: 0,
                        last_offset: 0,
                        delivery_count: 2,
                        _unknown_tagged_fields: Vec::new(),
                    }],
                    ..PartitionData::default()
                }],
                _unknown_tagged_fields: Vec::new(),
            }],
            ..ShareFetchResponseData::default()
        };

        let outcome = decode_share_fetch(response, &names).expect("decodes");
        assert_eq!(
            outcome.acknowledge_error,
            Some((
                TopicPartition::new("jobs", 0),
                ErrorCode::InvalidRecordState
            ))
        );
        assert_eq!(outcome.partitions[0].1.records.len(), 1);
    }

    #[test]
    fn session_loss_codes_are_recognised() {
        assert!(is_session_lost(ErrorCode::ShareSessionNotFound));
        assert!(is_session_lost(ErrorCode::InvalidShareSessionEpoch));
        assert!(is_session_lost(ErrorCode::ShareSessionLimitReached));
        assert!(!is_session_lost(ErrorCode::NotCoordinator));
    }
}
