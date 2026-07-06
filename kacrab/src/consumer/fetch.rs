//! Building `Fetch` requests, buffering responses, and decoding records.
//!
//! Fetches negotiate up to the broker's `Fetch` version. v13+ keys partitions
//! by *topic id* (KIP-516): ids are resolved from the routing metadata and the
//! response's ids are mapped back to names via the session's id→name map,
//! mirroring Java's `sessionTopicNames`. Like Java, a fetch whose topics do not
//! all have ids downgrades to v12, the last name-keyed version. Incremental
//! fetch sessions (KIP-227) are implemented per broker: the first fetch to a
//! leader is a full fetch that establishes a session, and subsequent fetches
//! send only the partitions whose position changed (plus a forgotten list for
//! removed ones), letting the broker return only partitions with new data.
//! Behaviour is identical to full fetches — it is purely a smaller request.
//!
//! Fetch responses are buffered across polls ([`FetchBuffer`], Java's
//! `completedFetches` + `nextInLineFetch`): one fetch typically returns far more
//! than `max.poll.records`, so `poll` drains the buffer in slices and a
//! partition is only re-fetched once its buffered data runs dry. Without this,
//! every poll would re-fetch — and the broker re-serve — the same data minus the
//! first 500 records.

use std::collections::{HashMap, HashSet, VecDeque};

use bytes::Bytes;
use kacrab_protocol::{
    KafkaUuid,
    generated::{
        ApiKey, ErrorCode, FetchRequestData, FetchResponseData,
        fetch_request::{FetchPartition, FetchTopic, ForgottenTopic, ReplicaState},
    },
    record::decode_next_batch,
};

use super::{
    config::ConsumerRuntimeConfig,
    error::{ConsumerError, Result},
    offsets::partition_leader,
    record::{ConsumerRecord, TimestampType},
    subscription::{FetchPosition, SubscriptionState},
};
use crate::{
    common::TopicPartition,
    wire::{ClusterMetadata, WireClient},
};

/// The last name-keyed `Fetch` version — the downgrade target when a topic id
/// is missing (topic-id fetch needs every topic's id), mirroring Java's
/// `AbstractFetch` v12 fallback. Also the safe pre-`ApiVersions` default.
const FETCH_NAME_KEYED_VERSION: i16 = 12;

/// The first `Fetch` version keyed by topic *id* instead of name (KIP-516).
const FETCH_TOPIC_ID_MIN_VERSION: i16 = 13;

/// Pick the `Fetch` version and keying mode, like Java's `AbstractFetch`: the
/// negotiated version when every topic has an id, else the v12 name-keyed cap.
const fn select_fetch_version(negotiated: i16, all_topics_have_ids: bool) -> (i16, bool) {
    if all_topics_have_ids && negotiated >= FETCH_TOPIC_ID_MIN_VERSION {
        (negotiated, true)
    } else if negotiated < FETCH_NAME_KEYED_VERSION {
        (negotiated, false)
    } else {
        (FETCH_NAME_KEYED_VERSION, false)
    }
}

/// The `LogAppendTime` bit (bit 3) in a record batch's attributes.
const LOG_APPEND_TIME_BIT: i16 = 0x0008;

/// `session_id` meaning "no fetch session" (a full, sessionless fetch).
const INVALID_SESSION_ID: i32 = 0;
/// `session_epoch` for the initial (full) fetch that opens a session.
const INITIAL_SESSION_EPOCH: i32 = 0;

/// Per-broker incremental fetch session state (KIP-227), kept across polls.
#[derive(Debug, Default, Clone)]
struct BrokerFetchSession {
    /// The broker-assigned session id (`0` = no session / full fetches).
    session_id: i32,
    /// The epoch to send next (`0` = a full fetch that (re)establishes a session).
    epoch: i32,
    /// The positions the broker currently holds for this session, so we can send
    /// only what changed and forget what is gone.
    sent: HashMap<(String, i32), FetchPosition>,
    /// Whether the session was established with topic-id-keyed fetches (v13+).
    uses_topic_ids: bool,
    /// The id each session topic was sent under (empty when name-keyed) —
    /// Java's `sessionTopicNames`, kept name-keyed to match `sent`. Forgotten
    /// topics reference these ids, and responses map ids back through them.
    topic_ids: HashMap<String, KafkaUuid>,
}

impl BrokerFetchSession {
    /// The next session epoch after a success, wrapping past `i32::MAX` to `1`
    /// (never back to the `0` full-fetch epoch), mirroring Kafka's `FetchMetadata`.
    const fn next_epoch(&self) -> i32 {
        match self.epoch.checked_add(1) {
            Some(next) => next,
            None => 1,
        }
    }

    /// Whether the next request is a full fetch (opens/reopens the session).
    const fn is_full(&self) -> bool {
        self.epoch == INITIAL_SESSION_EPOCH
    }

    /// Drop the session so the next fetch is a full one.
    fn reset(&mut self) {
        self.session_id = INVALID_SESSION_ID;
        self.epoch = INITIAL_SESSION_EPOCH;
        self.sent.clear();
        self.uses_topic_ids = false;
        self.topic_ids.clear();
    }

    /// Whether an established session can continue incrementally with this
    /// fetch's keying: the same id/name mode, and no topic whose id changed
    /// (a recreated topic). A mismatch means the broker's session cache is
    /// keyed by stale identities — the caller resets and re-opens with a full
    /// fetch, the outcome Java reaches via its "replaced" forgotten list.
    fn is_compatible(&self, use_topic_ids: bool, topic_ids: &HashMap<String, KafkaUuid>) -> bool {
        if self.uses_topic_ids != use_topic_ids {
            return false;
        }
        !use_topic_ids
            || topic_ids.iter().all(|(topic, id)| {
                self.topic_ids
                    .get(topic)
                    .is_none_or(|session_id| session_id == id)
            })
    }

    /// Record a successful response: adopt the broker's session id and, when a
    /// session exists, advance the epoch and snapshot what the broker now holds
    /// (positions and, for id-keyed sessions, the topic ids they went under).
    fn advance(
        &mut self,
        response_session_id: i32,
        entries: &[(TopicPartition, FetchPosition)],
        topic_ids: &HashMap<String, KafkaUuid>,
    ) {
        self.session_id = response_session_id;
        if response_session_id == INVALID_SESSION_ID {
            self.epoch = INITIAL_SESSION_EPOCH;
            self.sent.clear();
            self.uses_topic_ids = false;
            self.topic_ids.clear();
        } else {
            self.epoch = self.next_epoch();
            self.sent = entries
                .iter()
                .map(|(partition, position)| {
                    ((partition.topic.clone(), partition.partition), *position)
                })
                .collect();
            self.uses_topic_ids = !topic_ids.is_empty();
            self.topic_ids
                .extend(topic_ids.iter().map(|(topic, id)| (topic.clone(), *id)));
            // Topics no longer in the session don't need their id retained.
            self.topic_ids
                .retain(|topic, _| self.sent.keys().any(|(sent, _)| sent == topic));
        }
    }
}

/// All per-broker fetch sessions for one consumer, keyed by leader broker id.
#[derive(Debug, Default)]
pub(super) struct FetchSessions {
    by_broker: HashMap<i32, BrokerFetchSession>,
}

/// Records collected for one partition plus the position to advance it to.
#[derive(Debug)]
pub(super) struct PartitionFetch {
    pub partition: TopicPartition,
    pub records: Vec<ConsumerRecord>,
    pub next_offset: i64,
    pub next_leader_epoch: Option<i32>,
}

/// One partition's raw fetched record-batch blob plus the position it was
/// fetched at, queued in the [`FetchBuffer`] until drained.
#[derive(Debug)]
pub(super) struct RawPartitionFetch {
    pub partition: TopicPartition,
    /// The partition's position when the fetch was issued — the buffer entry is
    /// stale (seek/reset happened) unless the position still matches at drain.
    pub fetch_position: FetchPosition,
    /// The undecoded record batches, non-empty.
    pub records: Bytes,
}

/// The aggregate outcome of one `fetch` across every leader.
#[derive(Debug, Default)]
pub(super) struct FetchProgress {
    /// Raw fetched data, per partition, for the [`FetchBuffer`].
    pub partitions: Vec<RawPartitionFetch>,
    /// Partitions the broker reported out of range — their position must be
    /// reset via `auto.offset.reset`.
    pub resets: Vec<TopicPartition>,
    /// Partitions whose leader/metadata looked stale — the caller should refresh
    /// metadata and retry on the next poll.
    pub stale: Vec<TopicPartition>,
}

/// The partitions eligible for the next background fetch, out of the
/// subscription's fetchable `candidates`.
///
/// Skips partitions still `buffered` client-side, and — Java's
/// `AbstractFetch` buffered-node gate — every partition whose leader still
/// hosts a buffered partition: a fetch omitting the buffered partitions would
/// drop them from the broker's session cache, and one listing only caught-up
/// partitions would long-poll `fetch.max.wait.ms` while the buffer drains dry
/// behind it. Partitions without a resolved leader stay eligible; the fetch
/// itself skips them until metadata resolves.
pub(super) fn select_fetchable(
    candidates: Vec<(TopicPartition, FetchPosition)>,
    buffered: &[TopicPartition],
    leader_of: impl Fn(&TopicPartition) -> Option<i32>,
) -> Vec<(TopicPartition, FetchPosition)> {
    let buffered_set: HashSet<&TopicPartition> = buffered.iter().collect();
    let buffered_leaders: HashSet<i32> = buffered.iter().filter_map(&leader_of).collect();
    candidates
        .into_iter()
        .filter(|(partition, _position)| {
            !buffered_set.contains(partition)
                && leader_of(partition).is_none_or(|leader| !buffered_leaders.contains(&leader))
        })
        .collect()
}

/// How long an empty poll round may sleep before re-checking: the idle wait
/// (`retry.backoff.ms`, Java's spin guard) clamped to the remaining poll
/// budget, so a short `poll` timeout is never overshot by the backoff.
pub(super) fn idle_backoff(
    retry_backoff: std::time::Duration,
    timeout: std::time::Duration,
    elapsed: std::time::Duration,
) -> std::time::Duration {
    retry_backoff.min(timeout.saturating_sub(elapsed))
}

/// How a partition-level fetch error is handled. Only genuinely fatal codes fail
/// the whole `poll`; the rest are recovered per partition, mirroring Java's
/// `AbstractFetch`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PartitionErrorAction {
    /// The fetch position is invalid; reset it via `auto.offset.reset`.
    Reset,
    /// Stale leader/metadata; refresh metadata and retry next poll.
    Retriable,
    /// A fatal error to surface to the caller.
    Fatal,
}

/// Classify a partition-level fetch error code.
const fn classify_partition_error(error: ErrorCode) -> PartitionErrorAction {
    match error {
        // The committed/current offset fell outside the log (retention, truncation)
        // — a normal condition handled by re-resolving the position.
        ErrorCode::OffsetOutOfRange => PartitionErrorAction::Reset,
        // Leadership moved or the cached metadata is stale — re-resolve and retry.
        // The topic-id pair covers a topic recreated under a new id (v13+).
        ErrorCode::NotLeaderOrFollower
        | ErrorCode::FencedLeaderEpoch
        | ErrorCode::UnknownLeaderEpoch
        | ErrorCode::UnknownTopicOrPartition
        | ErrorCode::UnknownTopicId
        | ErrorCode::InconsistentTopicId
        | ErrorCode::ReplicaNotAvailable
        | ErrorCode::LeaderNotAvailable
        | ErrorCode::KafkaStorageError
        | ErrorCode::OffsetNotAvailable => PartitionErrorAction::Retriable,
        _ => PartitionErrorAction::Fatal,
    }
}

/// The read-only inputs a fetch needs: the wire client, runtime config, and the
/// cluster metadata used to route each partition to its leader.
#[derive(Debug, Clone, Copy)]
pub(super) struct FetchContext<'a> {
    pub wire: &'a WireClient,
    pub config: &'a ConsumerRuntimeConfig,
    pub metadata: &'a ClusterMetadata,
    /// The broker's max long-poll wait for this fetch — `fetch.max.wait.ms`
    /// clamped to the caller's remaining `poll` budget so a short `poll` timeout
    /// is honoured.
    pub max_wait_ms: i32,
}

/// Fetch from every fetchable partition (grouped by leader) and return each
/// partition's raw response data for the [`FetchBuffer`] — decoding happens at
/// drain time, one partition at a time. Uses incremental fetch sessions
/// (KIP-227) per broker via `sessions`.
pub(super) async fn fetch(
    context: &FetchContext<'_>,
    fetchable: &[(TopicPartition, FetchPosition)],
    sessions: &mut FetchSessions,
) -> Result<FetchProgress> {
    let mut progress = FetchProgress::default();
    if fetchable.is_empty() {
        return Ok(progress);
    }
    let FetchContext {
        wire,
        config,
        metadata,
        max_wait_ms,
    } = *context;

    let mut by_leader: HashMap<i32, Vec<(TopicPartition, FetchPosition)>> = HashMap::new();
    for (partition, position) in fetchable {
        let Some(leader) = partition_leader(metadata, &partition.topic, partition.partition) else {
            // No known leader yet — skip; the next poll re-resolves metadata.
            continue;
        };
        by_leader
            .entry(leader)
            .or_default()
            .push((partition.clone(), *position));
    }

    for (leader, entries) in by_leader {
        let (mut topic_ids, all_have_ids) = resolve_topic_ids(metadata, &entries);
        // Pick the Fetch version from this broker's negotiated `ApiVersions`
        // (falling back to the name-keyed v12 until `ApiVersions` has
        // completed), then key by topic id or name like Java's `AbstractFetch`.
        // `send_to_broker` re-clamps the ceiling against the broker's range.
        let negotiated = wire
            .negotiated_version(leader, ApiKey::Fetch)
            .unwrap_or(FETCH_NAME_KEYED_VERSION);
        let (version, use_topic_ids) = select_fetch_version(negotiated, all_have_ids);
        if !use_topic_ids {
            topic_ids.clear();
        }

        let session = sessions.by_broker.entry(leader).or_default();
        // A keying-mode flip or a changed topic id (recreated topic) leaves the
        // broker's session cache keyed by stale identities — re-open it full.
        if !session.is_full() && !session.is_compatible(use_topic_ids, &topic_ids) {
            session.reset();
        }
        let request = build_fetch_request(
            config,
            session,
            &entries,
            max_wait_ms,
            use_topic_ids.then_some(&topic_ids),
        );
        let response: FetchResponseData = match wire
            .send_to_broker(leader, ApiKey::Fetch, version, &request)
            .await
        {
            Ok(response) => response,
            // One unreachable leader must not fail the whole poll and discard
            // the data already collected from the other leaders. Flag its
            // partitions stale (metadata refresh + retry next poll, like
            // Java's per-node fetch handlers) and re-open the session full.
            // Terminal setup failures (TLS/SASL) still surface — they would
            // fail every leader identically, and silently retrying them
            // forever would mask an auth problem as an idle consumer.
            Err(error) if !error.is_fatal_setup() => {
                session.reset();
                progress
                    .stale
                    .extend(entries.iter().map(|(partition, _)| partition.clone()));
                continue;
            },
            Err(error) => return Err(error.into()),
        };
        let top_level = ErrorCode::from(response.error_code);
        // A stale/unknown session — drop it and re-establish with a full fetch on
        // the next poll (the partitions are unchanged, just the session bookkeeping).
        if matches!(
            top_level,
            ErrorCode::InvalidFetchSessionEpoch | ErrorCode::FetchSessionIdNotFound
        ) {
            session.reset();
            continue;
        }
        // A topic id in the session no longer matches the broker's (topic
        // recreated) — refresh the affected partitions' metadata and re-open
        // the session, like Java's `FETCH_SESSION_TOPIC_ID_ERROR` handling.
        if top_level == ErrorCode::FetchSessionTopicIdError {
            session.reset();
            progress
                .stale
                .extend(entries.iter().map(|(partition, _)| partition.clone()));
            continue;
        }
        if top_level.is_error() {
            return Err(ConsumerError::broker(
                "fetch",
                top_level,
                "fetch request rejected",
            ));
        }
        session.advance(response.session_id, &entries, &topic_ids);

        let want: HashMap<(String, i32), FetchPosition> = entries
            .iter()
            .map(|(tp, pos)| ((tp.topic.clone(), tp.partition), *pos))
            .collect();
        // v13+ responses key topics by id — map them back through the ids the
        // request was built with (Java's `sessionTopicNames`, set at build
        // time; a broker that declined the session still responds id-keyed).
        let topic_names: HashMap<KafkaUuid, &str> = topic_ids
            .iter()
            .map(|(name, id)| (*id, name.as_str()))
            .collect();

        collect_fetches(response, &want, &topic_names, &mut progress)?;
    }

    Ok(progress)
}

/// Resolve every topic's id from the routing metadata; topic-id fetch (v13+)
/// needs all of them, so the returned flag reports whether any id is missing
/// (downgrading that broker's fetch to v12).
fn resolve_topic_ids(
    metadata: &ClusterMetadata,
    entries: &[(TopicPartition, FetchPosition)],
) -> (HashMap<String, KafkaUuid>, bool) {
    let mut topic_ids: HashMap<String, KafkaUuid> = HashMap::new();
    let mut all_have_ids = true;
    for (partition, _) in entries {
        if topic_ids.contains_key(&partition.topic) {
            continue;
        }
        match metadata.topic(&partition.topic).map(|topic| topic.topic_id) {
            Some(id) if !id.is_nil() => {
                let _previous = topic_ids.insert(partition.topic.clone(), id);
            },
            _ => all_have_ids = false,
        }
    }
    (topic_ids, all_have_ids)
}

/// Process one broker's fetch response into raw per-partition data, recovering
/// partition-level errors per partition (reset out-of-range positions, flag
/// stale-leader partitions for a metadata refresh) and only failing on a
/// genuinely fatal code. Partitions the caller did not ask for are ignored —
/// including id-keyed topics (v13+, empty name) whose id `topic_names` cannot
/// resolve, matching Java's unresolved-id skip.
fn collect_fetches(
    response: FetchResponseData,
    want: &HashMap<(String, i32), FetchPosition>,
    topic_names: &HashMap<KafkaUuid, &str>,
    progress: &mut FetchProgress,
) -> Result<()> {
    for topic in response.responses {
        let name = if topic.topic.as_str().is_empty() {
            let Some(name) = topic_names.get(&topic.topic_id) else {
                continue;
            };
            (*name).to_owned()
        } else {
            topic.topic.as_str().to_owned()
        };
        for partition in topic.partitions {
            let tp = TopicPartition::new(name.clone(), partition.partition_index);
            let Some(position) = want.get(&(tp.topic.clone(), tp.partition)).copied() else {
                continue;
            };
            let error = ErrorCode::from(partition.error_code);
            if error.is_error() {
                match classify_partition_error(error) {
                    PartitionErrorAction::Reset => progress.resets.push(tp),
                    PartitionErrorAction::Retriable => progress.stale.push(tp),
                    PartitionErrorAction::Fatal => {
                        return Err(ConsumerError::broker(
                            "fetch",
                            error,
                            format!("{}-{} fetch failed", tp.topic, tp.partition),
                        ));
                    },
                }
                continue;
            }
            let Some(records) = partition.records.filter(|blob| !blob.is_empty()) else {
                continue;
            };
            progress.partitions.push(RawPartitionFetch {
                partition: tp,
                fetch_position: position,
                records,
            });
        }
    }
    Ok(())
}

/// Fetched-but-undrained partition data kept across polls — Java's
/// `completedFetches` queue plus `nextInLineFetch`. Raw response blobs queue up
/// FIFO; the front entry decodes lazily, one record batch at a time, as `poll`
/// drains it `max.poll.records` per slice — memory holds the raw blobs plus at
/// most about one batch of decoded records. Entries are invalidated lazily at
/// drain time: a position that no longer matches the entry (seek, reset,
/// truncation) or a partition no longer assigned (revoked, unsubscribed) drops
/// its buffered data; paused partitions keep theirs until resumed, mirroring
/// Java.
#[derive(Debug, Default)]
pub(super) struct FetchBuffer {
    buffered: VecDeque<BufferedFetch>,
}

#[derive(Debug)]
enum BufferedFetch {
    /// Not yet decoded: the raw record-batch blob as fetched.
    Raw(RawPartitionFetch),
    /// Decoded and being drained.
    Decoded(DecodedFetch),
}

impl BufferedFetch {
    const fn partition(&self) -> &TopicPartition {
        match self {
            Self::Raw(raw) => &raw.partition,
            Self::Decoded(decoded) => &decoded.partition,
        }
    }

    /// The app position this entry continues from; unless the partition's
    /// current position still equals it, the entry is stale.
    const fn position_offset(&self) -> i64 {
        match self {
            Self::Raw(raw) => raw.fetch_position.offset,
            Self::Decoded(decoded) => decoded.position_offset,
        }
    }
}

/// A partition blob mid-drain: batches decode lazily, one at a time, so memory
/// holds the raw blob plus roughly one batch of decoded records (Java's
/// `CompletedFetch` record iterator).
#[derive(Debug)]
struct DecodedFetch {
    partition: TopicPartition,
    /// Shared topic handle cloned into every record.
    topic: std::sync::Arc<str>,
    /// Raw record batches not yet decoded.
    blob: Bytes,
    /// Records from decoded batches, not yet handed to the app.
    records: VecDeque<ConsumerRecord>,
    /// The position the app sits at if this buffer is still valid.
    position_offset: i64,
    /// The position to advance to once every decoded record is drained.
    next_offset: i64,
    next_leader_epoch: Option<i32>,
}

impl DecodedFetch {
    fn new(raw: RawPartitionFetch) -> Self {
        Self {
            topic: std::sync::Arc::from(raw.partition.topic.as_str()),
            partition: raw.partition,
            blob: raw.records,
            records: VecDeque::new(),
            position_offset: raw.fetch_position.offset,
            next_offset: raw.fetch_position.offset,
            next_leader_epoch: None,
        }
    }

    /// Whether every batch is decoded and every record handed out.
    fn is_exhausted(&self) -> bool {
        self.records.is_empty() && self.blob.is_empty()
    }

    /// Decode batches until at least `budget` records are ready or the blob
    /// runs out. Records below the current position (a fetch landing mid-batch)
    /// are skipped.
    fn refill(&mut self, budget: usize) -> Result<()> {
        while self.records.len() < budget && !self.blob.is_empty() {
            let batch = decode_next_batch(&mut self.blob).map_err(|_error| {
                ConsumerError::InvalidState("failed to decode fetched record batch")
            })?;
            let Some(batch) = batch else {
                // A truncated trailing batch — normal at the end of a fetch
                // response; the records continue in the next fetch.
                self.blob = Bytes::new();
                break;
            };
            let leader_epoch =
                (batch.partition_leader_epoch >= 0).then_some(batch.partition_leader_epoch);
            self.next_leader_epoch = leader_epoch;
            let log_append_time = batch.attributes & LOG_APPEND_TIME_BIT != 0;
            let timestamp_type = if log_append_time {
                TimestampType::LogAppendTime
            } else {
                TimestampType::CreateTime
            };
            for record in batch.records {
                let offset = batch
                    .base_offset
                    .saturating_add(i64::from(record.offset_delta));
                if offset < self.position_offset {
                    continue;
                }
                let timestamp = if log_append_time {
                    batch.max_timestamp
                } else {
                    batch.first_timestamp.saturating_add(record.timestamp_delta)
                };
                self.records.push_back(ConsumerRecord {
                    topic: std::sync::Arc::clone(&self.topic),
                    partition: self.partition.partition,
                    offset,
                    timestamp,
                    timestamp_type,
                    key: record.key,
                    value: record.value,
                    headers: record.headers,
                    leader_epoch,
                });
                self.next_offset = offset.saturating_add(1);
            }
        }
        Ok(())
    }
}

impl FetchBuffer {
    /// Whether a partition has buffered data (and must not be re-fetched yet).
    #[cfg(test)]
    pub(super) fn has(&self, partition: &TopicPartition) -> bool {
        self.buffered
            .iter()
            .any(|entry| entry.partition() == partition)
    }

    /// The partitions with buffered data, for the buffered-node fetch gate.
    pub(super) fn partitions(&self) -> impl Iterator<Item = &TopicPartition> {
        self.buffered.iter().map(BufferedFetch::partition)
    }

    /// Queue one partition's raw fetch response data.
    pub(super) fn push(&mut self, raw: RawPartitionFetch) {
        self.buffered.push_back(BufferedFetch::Raw(raw));
    }

    /// Drain up to `max_records` buffered records, front entry first, returning
    /// per-partition slices with the position to advance past each. A partially
    /// drained partition stays at the front so the next poll continues it;
    /// stale entries are dropped; paused ones rotate to the back untouched.
    pub(super) fn drain(
        &mut self,
        subscription: &SubscriptionState,
        max_records: usize,
    ) -> Result<Vec<PartitionFetch>> {
        let mut out = Vec::new();
        let mut budget = max_records;
        // Visit each entry at most once per drain, so paused entries rotated to
        // the back are not re-examined in the same pass.
        let mut visits_left = self.buffered.len();
        while budget > 0 && visits_left > 0 {
            visits_left = visits_left.saturating_sub(1);
            let Some(entry) = self.buffered.pop_front() else {
                break;
            };
            let partition = entry.partition().clone();
            // `position` is `None` for revoked/unsubscribed partitions and for
            // reset-pending ones — either way the buffered data is stale.
            let Some(position) = subscription.position(&partition) else {
                continue;
            };
            if position.offset != entry.position_offset() {
                continue;
            }
            if subscription.is_paused(&partition) {
                self.buffered.push_back(entry);
                continue;
            }
            let mut decoded = match entry {
                BufferedFetch::Decoded(decoded) => decoded,
                BufferedFetch::Raw(raw) => DecodedFetch::new(raw),
            };
            decoded.refill(budget)?;
            if decoded.records.is_empty() {
                // Nothing at or past the position in this blob.
                continue;
            }
            let take = budget.min(decoded.records.len());
            let records: Vec<ConsumerRecord> = decoded.records.drain(..take).collect();
            budget = budget.saturating_sub(take);
            let finished = decoded.is_exhausted();
            let (next_offset, next_leader_epoch) = if finished {
                (decoded.next_offset, decoded.next_leader_epoch)
            } else {
                records
                    .last()
                    .map_or((decoded.next_offset, decoded.next_leader_epoch), |last| {
                        (last.offset.saturating_add(1), last.leader_epoch)
                    })
            };
            out.push(PartitionFetch {
                partition: decoded.partition.clone(),
                records,
                next_offset,
                next_leader_epoch,
            });
            if !finished {
                // The caller advances the position to `next_offset`; record it
                // so the remainder stays valid for the next poll.
                decoded.position_offset = next_offset;
                self.buffered.push_front(BufferedFetch::Decoded(decoded));
            }
        }
        Ok(out)
    }
}

fn build_fetch_request(
    config: &ConsumerRuntimeConfig,
    session: &BrokerFetchSession,
    entries: &[(TopicPartition, FetchPosition)],
    max_wait_ms: i32,
    topic_ids: Option<&HashMap<String, KafkaUuid>>,
) -> FetchRequestData {
    let full = session.is_full();
    // Grouped by name (v13+ sends only the id on the wire, so the wire struct
    // can't be the grouping key), then lowered to `FetchTopic`s at the end.
    let mut grouped: Vec<(&str, Vec<FetchPartition>)> = Vec::new();
    for (partition, position) in entries {
        // Incremental fetch: only send partitions whose position changed since the
        // broker last saw them; a full fetch sends everything.
        if !full
            && session
                .sent
                .get(&(partition.topic.clone(), partition.partition))
                == Some(position)
        {
            continue;
        }
        let wire_partition = FetchPartition {
            partition: partition.partition,
            current_leader_epoch: position.leader_epoch.unwrap_or(-1),
            fetch_offset: position.offset,
            last_fetched_epoch: -1,
            log_start_offset: -1,
            partition_max_bytes: config.max_partition_fetch_bytes,
            replica_directory_id: KafkaUuid::default(),
            high_watermark: i64::MAX,
            _unknown_tagged_fields: Vec::new(),
        };
        if let Some((_, partitions)) = grouped
            .iter_mut()
            .find(|(topic, _)| *topic == partition.topic)
        {
            partitions.push(wire_partition);
        } else {
            grouped.push((partition.topic.as_str(), vec![wire_partition]));
        }
    }
    // Topic-id keyed fetches (v13+) leave the name empty and set the id; the
    // strict codec rejects a non-default name at v13+ and vice versa.
    let topics: Vec<FetchTopic> = grouped
        .into_iter()
        .map(|(name, partitions)| match topic_ids {
            Some(ids) => FetchTopic {
                topic: kacrab_protocol::KafkaString::default(),
                topic_id: ids.get(name).copied().unwrap_or_default(),
                partitions,
                _unknown_tagged_fields: Vec::new(),
            },
            None => FetchTopic {
                topic: name.to_owned().into(),
                topic_id: KafkaUuid::default(),
                partitions,
                _unknown_tagged_fields: Vec::new(),
            },
        })
        .collect();
    let forgotten = if full {
        Vec::new()
    } else {
        build_forgotten(session, entries)
    };

    FetchRequestData {
        cluster_id: None,
        replica_id: -1,
        replica_state: ReplicaState {
            replica_id: -1,
            replica_epoch: -1,
            _unknown_tagged_fields: Vec::new(),
        },
        max_wait_ms,
        min_bytes: config.fetch_min_bytes,
        max_bytes: config.fetch_max_bytes,
        isolation_level: config.isolation_level.wire(),
        session_id: session.session_id,
        session_epoch: session.epoch,
        topics,
        forgotten_topics_data: forgotten,
        rack_id: config.client_rack.clone().into(),
        _unknown_tagged_fields: Vec::new(),
    }
}

/// The partitions the broker still holds in the session that are no longer
/// fetchable, grouped by topic — the incremental fetch's forgotten list. In an
/// id-keyed session the forgotten topics carry the id the broker knows them
/// under (`session.topic_ids`) and an empty name, per the v13+ strict codec.
fn build_forgotten(
    session: &BrokerFetchSession,
    entries: &[(TopicPartition, FetchPosition)],
) -> Vec<ForgottenTopic> {
    let current: HashSet<(&str, i32)> = entries
        .iter()
        .map(|(partition, _)| (partition.topic.as_str(), partition.partition))
        .collect();
    let mut grouped: Vec<(&str, Vec<i32>)> = Vec::new();
    for (topic, partition) in session.sent.keys() {
        if current.contains(&(topic.as_str(), *partition)) {
            continue;
        }
        if let Some((_, partitions)) = grouped.iter_mut().find(|(name, _)| name == topic) {
            partitions.push(*partition);
        } else {
            grouped.push((topic.as_str(), vec![*partition]));
        }
    }
    grouped
        .into_iter()
        .map(|(name, partitions)| {
            if session.uses_topic_ids {
                ForgottenTopic {
                    topic: kacrab_protocol::KafkaString::default(),
                    topic_id: session.topic_ids.get(name).copied().unwrap_or_default(),
                    partitions,
                    _unknown_tagged_fields: Vec::new(),
                }
            } else {
                ForgottenTopic {
                    topic: name.to_owned().into(),
                    topic_id: KafkaUuid::default(),
                    partitions,
                    _unknown_tagged_fields: Vec::new(),
                }
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;
    use kacrab_protocol::record::{Record, RecordBatch};

    use super::*;

    fn record(offset_delta: i32, key: &str, value: &str) -> Record {
        Record {
            attributes: 0,
            timestamp_delta: i64::from(offset_delta),
            offset_delta,
            key: Some(Bytes::copy_from_slice(key.as_bytes())),
            value: Some(Bytes::copy_from_slice(value.as_bytes())),
            headers: Vec::new(),
        }
    }

    fn encode_batch(base_offset: i64, count: i32) -> Bytes {
        let records: Vec<Record> = (0..count)
            .map(|i| record(i, &format!("k{i}"), &format!("v{i}")))
            .collect();
        let batch = RecordBatch {
            base_offset,
            partition_leader_epoch: 4,
            magic: 2,
            attributes: 0,
            last_offset_delta: count.saturating_sub(1),
            first_timestamp: 1_000,
            max_timestamp: 1_000_i64.saturating_add(i64::from(count.saturating_sub(1))),
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            records,
        };
        let mut buf = BytesMut::new();
        batch.encode(&mut buf).expect("encode batch");
        buf.freeze()
    }

    fn tp() -> TopicPartition {
        TopicPartition::new("t", 0)
    }

    #[test]
    fn decodes_records_with_absolute_offsets() {
        let mut decoded = DecodedFetch::new(RawPartitionFetch {
            partition: tp(),
            fetch_position: FetchPosition::new(100, None),
            records: encode_batch(100, 3),
        });
        decoded.refill(10).unwrap();
        assert_eq!(decoded.records.len(), 3);
        assert_eq!(decoded.records[0].offset, 100);
        assert_eq!(decoded.records[2].offset, 102);
        assert_eq!(decoded.records[0].value.as_deref(), Some(b"v0".as_ref()));
        assert_eq!(decoded.records[0].leader_epoch, Some(4));
        assert_eq!(decoded.next_offset, 103);
        assert!(decoded.blob.is_empty());
    }

    #[test]
    fn skips_records_before_the_fetch_offset() {
        // A fetch landing mid-batch (offset 101 inside a batch based at 100)
        // must skip the records below the position.
        let mut decoded = DecodedFetch::new(RawPartitionFetch {
            partition: tp(),
            fetch_position: FetchPosition::new(101, None),
            records: encode_batch(100, 3),
        });
        decoded.refill(10).unwrap();
        assert_eq!(decoded.records.len(), 2);
        assert_eq!(decoded.records[0].offset, 101);
        assert_eq!(decoded.next_offset, 103);
    }

    #[test]
    fn refill_decodes_lazily_per_batch() {
        // Two 3-record batches: a refill for 2 records decodes only the first
        // batch; the second stays raw until needed.
        let mut blob = BytesMut::new();
        blob.extend_from_slice(&encode_batch(100, 3));
        blob.extend_from_slice(&encode_batch(103, 3));
        let mut decoded = DecodedFetch::new(RawPartitionFetch {
            partition: tp(),
            fetch_position: FetchPosition::new(100, None),
            records: blob.freeze(),
        });

        decoded.refill(2).unwrap();
        assert_eq!(decoded.records.len(), 3);
        assert!(!decoded.blob.is_empty());

        decoded.refill(6).unwrap();
        assert_eq!(decoded.records.len(), 6);
        assert!(decoded.blob.is_empty());
        assert_eq!(decoded.next_offset, 106);
    }

    #[test]
    fn truncated_trailing_batch_is_dropped_cleanly() {
        // Fetch responses may cut the final batch short; the truncated tail is
        // discarded and the position resumes at the last whole record.
        let whole = encode_batch(100, 3);
        let mut blob = BytesMut::new();
        blob.extend_from_slice(&whole);
        blob.extend_from_slice(&whole.slice(..whole.len() / 2));
        let mut buffer = FetchBuffer::default();
        buffer.push(RawPartitionFetch {
            partition: tp(),
            fetch_position: FetchPosition::new(100, None),
            records: blob.freeze(),
        });
        let subscription = subscription_at("t", 0, 100);

        let drained = buffer.drain(&subscription, 10).expect("drain");
        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0].records.len(), 3);
        assert_eq!(drained[0].next_offset, 103);
        assert!(!buffer.has(&tp()));
    }

    fn entry(topic: &str, partition: i32, offset: i64) -> (TopicPartition, FetchPosition) {
        (
            TopicPartition::new(topic, partition),
            FetchPosition::new(offset, None),
        )
    }

    #[test]
    fn session_opens_full_then_goes_incremental() {
        let mut session = BrokerFetchSession::default();
        assert!(session.is_full());
        assert_eq!(session.session_id, INVALID_SESSION_ID);

        // A response with a real session id advances the epoch and records state.
        let entries = vec![entry("t", 0, 10), entry("t", 1, 20)];
        session.advance(42, &entries, &HashMap::new());
        assert!(!session.is_full());
        assert_eq!(session.session_id, 42);
        assert_eq!(session.epoch, 1);
        assert_eq!(session.sent.len(), 2);
        assert!(!session.uses_topic_ids);

        // A reset returns to a full fetch.
        session.reset();
        assert!(session.is_full());
        assert!(session.sent.is_empty());
    }

    #[test]
    fn session_without_broker_id_stays_full() {
        let mut session = BrokerFetchSession::default();
        session.advance(INVALID_SESSION_ID, &[entry("t", 0, 5)], &HashMap::new());
        // The broker declined a session, so we keep sending full fetches.
        assert!(session.is_full());
        assert!(session.sent.is_empty());
    }

    fn test_config() -> ConsumerRuntimeConfig {
        let client: crate::config::ClientConfig =
            [("bootstrap.servers", "127.0.0.1:9092"), ("group.id", "g")]
                .into_iter()
                .collect();
        ConsumerRuntimeConfig::from_config(&client.consumer_config().expect("config"))
            .expect("runtime")
    }

    #[test]
    fn full_fetch_sends_all_partitions_then_incremental_sends_only_changes() {
        let config = test_config();
        let mut session = BrokerFetchSession::default();

        // A full fetch (epoch 0) sends every partition and no forgotten list.
        let entries = vec![entry("t", 0, 10), entry("t", 1, 20)];
        let full = build_fetch_request(&config, &session, &entries, 500, None);
        assert_eq!(full.session_epoch, INITIAL_SESSION_EPOCH);
        assert_eq!(
            full.topics
                .iter()
                .map(|t| t.partitions.len())
                .sum::<usize>(),
            2
        );
        assert!(full.forgotten_topics_data.is_empty());

        session.advance(99, &entries, &HashMap::new());

        // Incrementally, only the partition whose offset changed is resent.
        let changed = vec![entry("t", 0, 10), entry("t", 1, 25)];
        let incremental = build_fetch_request(&config, &session, &changed, 500, None);
        assert_eq!(incremental.session_id, 99);
        assert_eq!(incremental.session_epoch, 1);
        let sent: Vec<i32> = incremental
            .topics
            .iter()
            .flat_map(|topic| topic.partitions.iter().map(|p| p.partition))
            .collect();
        assert_eq!(sent, vec![1]);
        assert!(incremental.forgotten_topics_data.is_empty());
    }

    use kacrab_protocol::generated::{
        FetchResponseData,
        fetch_response::{FetchableTopicResponse, PartitionData},
    };

    fn partition_data(index: i32, error: ErrorCode, records: Option<Bytes>) -> PartitionData {
        PartitionData {
            partition_index: index,
            error_code: error.code(),
            records,
            ..PartitionData::default()
        }
    }

    fn fetch_response(topic: &str, partitions: Vec<PartitionData>) -> FetchResponseData {
        FetchResponseData {
            responses: vec![FetchableTopicResponse {
                topic: topic.to_owned().into(),
                partitions,
                ..FetchableTopicResponse::default()
            }],
            ..FetchResponseData::default()
        }
    }

    fn want_two() -> HashMap<(String, i32), FetchPosition> {
        [
            (("t".to_owned(), 0), FetchPosition::new(100, None)),
            (("t".to_owned(), 1), FetchPosition::new(0, None)),
        ]
        .into_iter()
        .collect()
    }

    #[test]
    fn out_of_range_partition_resets_while_healthy_partition_survives() {
        // t-0 is out of range; t-1 has records. The whole fetch must NOT error —
        // t-0 is reset and t-1's raw data is still collected.
        let response = fetch_response(
            "t",
            vec![
                partition_data(0, ErrorCode::OffsetOutOfRange, None),
                partition_data(1, ErrorCode::None, Some(encode_batch(0, 2))),
            ],
        );
        let mut progress = FetchProgress::default();
        collect_fetches(response, &want_two(), &HashMap::new(), &mut progress)
            .expect("no fatal error");
        assert_eq!(progress.resets, vec![TopicPartition::new("t", 0)]);
        assert!(progress.stale.is_empty());
        assert_eq!(progress.partitions.len(), 1);
        assert_eq!(
            progress.partitions[0].partition,
            TopicPartition::new("t", 1)
        );
        assert_eq!(progress.partitions[0].fetch_position.offset, 0);
        assert!(!progress.partitions[0].records.is_empty());
    }

    #[test]
    fn stale_leader_partition_is_flagged_not_fatal() {
        let response = fetch_response(
            "t",
            vec![partition_data(0, ErrorCode::NotLeaderOrFollower, None)],
        );
        let mut progress = FetchProgress::default();
        collect_fetches(response, &want_two(), &HashMap::new(), &mut progress).expect("retriable");
        assert_eq!(progress.stale, vec![TopicPartition::new("t", 0)]);
        assert!(progress.resets.is_empty());
        assert!(progress.partitions.is_empty());
    }

    #[test]
    fn genuinely_fatal_partition_error_propagates() {
        let response = fetch_response(
            "t",
            vec![partition_data(0, ErrorCode::CorruptMessage, None)],
        );
        let mut progress = FetchProgress::default();
        assert!(collect_fetches(response, &want_two(), &HashMap::new(), &mut progress).is_err());
    }

    fn buffer_with(topic: &str, partition: i32, offset: i64, count: i32) -> FetchBuffer {
        let mut buffer = FetchBuffer::default();
        buffer.push(RawPartitionFetch {
            partition: TopicPartition::new(topic, partition),
            fetch_position: FetchPosition::new(offset, None),
            records: encode_batch(offset, count),
        });
        buffer
    }

    fn subscription_at(topic: &str, partition: i32, offset: i64) -> SubscriptionState {
        let mut subscription = SubscriptionState::new(crate::consumer::AutoOffsetReset::Earliest);
        let tp = TopicPartition::new(topic, partition);
        subscription.assign(std::slice::from_ref(&tp));
        subscription.set_position(&tp, FetchPosition::new(offset, None));
        subscription
    }

    #[test]
    fn buffer_drains_in_max_poll_slices_across_polls() {
        let mut buffer = buffer_with("t", 0, 100, 5);
        let mut subscription = subscription_at("t", 0, 100);
        let tp = TopicPartition::new("t", 0);
        assert!(buffer.has(&tp));

        // First drain: 2 of 5 records, position advances past them, remainder
        // stays buffered so the partition must not be re-fetched.
        let first = buffer.drain(&subscription, 2).expect("drain");
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].records.len(), 2);
        assert_eq!(first[0].next_offset, 102);
        subscription.advance_position(&tp, first[0].next_offset, first[0].next_leader_epoch);
        assert!(buffer.has(&tp));

        // Second drain continues from the remainder without any new fetch.
        let second = buffer.drain(&subscription, 2).expect("drain");
        assert_eq!(second[0].records.len(), 2);
        assert_eq!(second[0].records[0].offset, 102);
        subscription.advance_position(&tp, second[0].next_offset, second[0].next_leader_epoch);

        // Final drain empties the buffer and advances past the whole blob.
        let third = buffer.drain(&subscription, 2).expect("drain");
        assert_eq!(third[0].records.len(), 1);
        assert_eq!(third[0].next_offset, 105);
        assert!(!buffer.has(&tp));
    }

    #[test]
    fn seek_invalidates_buffered_records() {
        let mut buffer = buffer_with("t", 0, 100, 5);
        // The app sought elsewhere: the buffered blob no longer matches the
        // position and must be dropped, not served.
        let subscription = subscription_at("t", 0, 42);
        let drained = buffer.drain(&subscription, 10).expect("drain");
        assert!(drained.is_empty());
        assert!(!buffer.has(&TopicPartition::new("t", 0)));
    }

    #[test]
    fn revoked_partition_buffer_is_dropped() {
        let mut buffer = buffer_with("t", 0, 100, 5);
        // The partition is no longer assigned (rebalance revoked it).
        let mut subscription = subscription_at("u", 1, 0);
        subscription.set_position(&TopicPartition::new("u", 1), FetchPosition::new(0, None));
        let drained = buffer.drain(&subscription, 10).expect("drain");
        assert!(drained.is_empty());
        assert!(!buffer.has(&TopicPartition::new("t", 0)));
    }

    #[test]
    fn paused_partition_buffer_is_retained_until_resumed() {
        let mut buffer = buffer_with("t", 0, 100, 3);
        let mut subscription = subscription_at("t", 0, 100);
        let tp = TopicPartition::new("t", 0);
        subscription.pause(std::slice::from_ref(&tp));

        // Paused: nothing drains but the data is kept (Java parity).
        let drained = buffer.drain(&subscription, 10).expect("drain");
        assert!(drained.is_empty());
        assert!(buffer.has(&tp));

        subscription.resume(std::slice::from_ref(&tp));
        let drained = buffer.drain(&subscription, 10).expect("drain");
        assert_eq!(drained[0].records.len(), 3);
        assert!(!buffer.has(&tp));
    }

    #[test]
    fn paused_entry_does_not_starve_other_partitions() {
        let mut buffer = buffer_with("t", 0, 100, 3);
        buffer.push(RawPartitionFetch {
            partition: TopicPartition::new("t", 1),
            fetch_position: FetchPosition::new(0, None),
            records: encode_batch(0, 2),
        });
        let mut subscription = subscription_at("t", 0, 100);
        let other = TopicPartition::new("t", 1);
        subscription.assign(&[TopicPartition::new("t", 0), other.clone()]);
        subscription.set_position(&TopicPartition::new("t", 0), FetchPosition::new(100, None));
        subscription.set_position(&other, FetchPosition::new(0, None));
        subscription.pause(&[TopicPartition::new("t", 0)]);

        // The paused front entry rotates to the back; t-1 still drains.
        let drained = buffer.drain(&subscription, 10).expect("drain");
        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0].partition, other);
        assert_eq!(drained[0].records.len(), 2);
        assert!(buffer.has(&TopicPartition::new("t", 0)));
    }

    #[test]
    fn blob_entirely_before_position_is_dropped() {
        // A blob fetched at offset 100 whose records all precede a position of
        // 100 cannot happen, but a compacted blob can decode to zero records at
        // or past the position — it must be dropped, not looped on.
        let mut buffer = FetchBuffer::default();
        buffer.push(RawPartitionFetch {
            partition: TopicPartition::new("t", 0),
            fetch_position: FetchPosition::new(103, None),
            records: encode_batch(100, 3),
        });
        // Position matches the entry (103) but every record is below it.
        let subscription = subscription_at("t", 0, 103);
        let drained = buffer.drain(&subscription, 10).expect("drain");
        assert!(drained.is_empty());
        assert!(!buffer.has(&TopicPartition::new("t", 0)));
    }

    fn uuid(byte: u8) -> KafkaUuid {
        KafkaUuid::from_parts(u64::from(byte), u64::from(byte))
    }

    fn ids(pairs: &[(&str, KafkaUuid)]) -> HashMap<String, KafkaUuid> {
        pairs
            .iter()
            .map(|(name, id)| ((*name).to_owned(), *id))
            .collect()
    }

    #[test]
    fn version_selection_mirrors_java_downgrade() {
        // Every topic has an id and the broker speaks v13+ — use the
        // negotiated version, id-keyed.
        assert_eq!(select_fetch_version(17, true), (17, true));
        assert_eq!(select_fetch_version(13, true), (13, true));
        // A missing topic id downgrades to the v12 name-keyed cap (Java's
        // `AbstractFetch` fallback), whatever the broker supports.
        assert_eq!(select_fetch_version(17, false), (12, false));
        // Brokers below v13 stay name-keyed even with ids available.
        assert_eq!(select_fetch_version(12, true), (12, false));
        assert_eq!(select_fetch_version(4, false), (4, false));
    }

    #[test]
    fn topic_id_request_omits_names_and_encodes_at_v17() {
        let config = test_config();
        let session = BrokerFetchSession::default();
        let entries = vec![entry("t", 0, 10), entry("u", 1, 20)];
        let topic_ids = ids(&[("t", uuid(1)), ("u", uuid(2))]);

        let request = build_fetch_request(&config, &session, &entries, 500, Some(&topic_ids));
        for topic in &request.topics {
            assert!(topic.topic.as_str().is_empty());
            assert!(!topic.topic_id.is_nil());
        }
        let sent: HashSet<KafkaUuid> = request.topics.iter().map(|t| t.topic_id).collect();
        assert_eq!(sent, [uuid(1), uuid(2)].into_iter().collect());

        // The strict codec accepts the id-keyed shape at v17 and rejects it at
        // the name-keyed v12 (proof the two shapes cannot be mixed up).
        let mut buf = BytesMut::new();
        request.write(&mut buf, 17).expect("v17 encode");
        assert_eq!(buf.len(), request.encoded_len(17).expect("v17 len"));
        assert!(request.write(&mut BytesMut::new(), 12).is_err());
    }

    #[test]
    fn name_keyed_request_encodes_at_v12_but_not_v17() {
        let config = test_config();
        let session = BrokerFetchSession::default();
        let entries = vec![entry("t", 0, 10)];
        let request = build_fetch_request(&config, &session, &entries, 500, None);
        assert_eq!(request.topics[0].topic.as_str(), "t");
        assert!(request.topics[0].topic_id.is_nil());
        request.write(&mut BytesMut::new(), 12).expect("v12 encode");
        assert!(request.write(&mut BytesMut::new(), 17).is_err());
    }

    #[test]
    fn session_compatibility_tracks_ids_and_mode() {
        let mut session = BrokerFetchSession::default();
        let entries = vec![entry("t", 0, 10)];
        session.advance(42, &entries, &ids(&[("t", uuid(1))]));
        assert!(session.uses_topic_ids);

        // Same id, or a brand-new topic joining the session: compatible.
        assert!(session.is_compatible(true, &ids(&[("t", uuid(1))])));
        assert!(session.is_compatible(true, &ids(&[("t", uuid(1)), ("u", uuid(2))])));
        // The topic was recreated under a new id: incompatible.
        assert!(!session.is_compatible(true, &ids(&[("t", uuid(9))])));
        // Keying-mode flip (id-keyed session, name-keyed fetch): incompatible.
        assert!(!session.is_compatible(false, &HashMap::new()));

        // A name-keyed session is incompatible with an id-keyed fetch.
        session.reset();
        session.advance(42, &entries, &HashMap::new());
        assert!(!session.is_compatible(true, &ids(&[("t", uuid(1))])));
        assert!(session.is_compatible(false, &HashMap::new()));
    }

    #[test]
    fn forgotten_topics_carry_the_session_ids() {
        let mut session = BrokerFetchSession::default();
        session.advance(
            7,
            &[entry("t", 0, 1), entry("u", 0, 1)],
            &ids(&[("t", uuid(1)), ("u", uuid(2))]),
        );
        // u-0 dropped out of the fetchable set: it must be forgotten under the
        // id the broker's session cache knows it by, with an empty name.
        let forgotten = build_forgotten(&session, &[entry("t", 0, 1)]);
        assert_eq!(forgotten.len(), 1);
        assert!(forgotten[0].topic.as_str().is_empty());
        assert_eq!(forgotten[0].topic_id, uuid(2));
        assert_eq!(forgotten[0].partitions, vec![0]);
    }

    #[test]
    fn collect_fetches_resolves_topics_by_id() {
        // A v13+ response: the topic arrives with an empty name and an id.
        let mut response = fetch_response(
            "",
            vec![partition_data(
                0,
                ErrorCode::None,
                Some(encode_batch(100, 2)),
            )],
        );
        response.responses[0].topic_id = uuid(1);
        let topic_names: HashMap<KafkaUuid, &str> = std::iter::once((uuid(1), "t")).collect();

        let mut progress = FetchProgress::default();
        collect_fetches(response, &want_two(), &topic_names, &mut progress).expect("collect");
        assert_eq!(progress.partitions.len(), 1);
        assert_eq!(
            progress.partitions[0].partition,
            TopicPartition::new("t", 0)
        );

        // An id the request never sent cannot be resolved — skipped, like Java.
        let mut unknown = fetch_response(
            "",
            vec![partition_data(
                0,
                ErrorCode::None,
                Some(encode_batch(100, 2)),
            )],
        );
        unknown.responses[0].topic_id = uuid(9);
        let mut progress = FetchProgress::default();
        collect_fetches(unknown, &want_two(), &topic_names, &mut progress).expect("collect");
        assert!(progress.partitions.is_empty());
    }

    #[test]
    fn unknown_topic_id_errors_are_retriable() {
        // A recreated topic surfaces UNKNOWN_TOPIC_ID / INCONSISTENT_TOPIC_ID —
        // both flag the partition stale for a metadata refresh, never fatal.
        for error in [ErrorCode::UnknownTopicId, ErrorCode::InconsistentTopicId] {
            let response = fetch_response("t", vec![partition_data(0, error, None)]);
            let mut progress = FetchProgress::default();
            collect_fetches(response, &want_two(), &HashMap::new(), &mut progress)
                .expect("retriable");
            assert_eq!(progress.stale, vec![TopicPartition::new("t", 0)]);
        }
    }

    #[test]
    fn forgotten_lists_partitions_dropped_from_the_session() {
        let mut session = BrokerFetchSession::default();
        session.advance(
            7,
            &[entry("t", 0, 1), entry("t", 1, 1), entry("u", 0, 1)],
            &HashMap::new(),
        );
        // Now only t-0 remains fetchable; t-1 and u-0 must be forgotten.
        let forgotten = build_forgotten(&session, &[entry("t", 0, 1)]);
        let mut pairs: Vec<(String, i32)> = forgotten
            .into_iter()
            .flat_map(|topic| {
                let name = topic.topic.as_str().to_owned();
                topic
                    .partitions
                    .into_iter()
                    .map(move |partition| (name.clone(), partition))
            })
            .collect();
        pairs.sort();
        assert_eq!(pairs, vec![("t".to_owned(), 1), ("u".to_owned(), 0)]);
    }

    // --- select_fetchable (buffered-node gate) ---------------------------

    fn candidate(topic: &str, partition: i32) -> (TopicPartition, FetchPosition) {
        (
            TopicPartition::new(topic, partition),
            FetchPosition::new(0, None),
        )
    }

    /// Leader resolver over a fixed `(topic, partition) -> leader` table;
    /// unlisted partitions have no resolved leader.
    fn leader_table(
        entries: &[(&str, i32, i32)],
    ) -> impl Fn(&TopicPartition) -> Option<i32> + use<> {
        let table: HashMap<TopicPartition, i32> = entries
            .iter()
            .map(|(topic, partition, leader)| (TopicPartition::new(*topic, *partition), *leader))
            .collect();
        move |partition| table.get(partition).copied()
    }

    #[test]
    fn select_fetchable_skips_partitions_still_buffered() {
        let leaders = leader_table(&[("t", 0, 1), ("t", 1, 2)]);
        let fetchable = select_fetchable(
            vec![candidate("t", 0), candidate("t", 1)],
            &[TopicPartition::new("t", 0)],
            leaders,
        );
        assert_eq!(fetchable, vec![candidate("t", 1)]);
    }

    #[test]
    fn select_fetchable_gates_every_partition_on_a_buffered_leader() {
        // t-0 is buffered on leader 1; t-1 shares that leader, so it must NOT
        // be fetched (Java's buffered-node gate) — a fetch omitting t-0 would
        // evict it from the broker's session and long-poll behind the buffer.
        let leaders = leader_table(&[("t", 0, 1), ("t", 1, 1), ("u", 0, 2)]);
        let fetchable = select_fetchable(
            vec![candidate("t", 1), candidate("u", 0)],
            &[TopicPartition::new("t", 0)],
            leaders,
        );
        assert_eq!(fetchable, vec![candidate("u", 0)]);
    }

    #[test]
    fn select_fetchable_keeps_partitions_with_unresolved_leaders() {
        // No leader resolved yet — still eligible; the fetch itself skips it
        // until metadata resolves rather than stalling the whole poll.
        let leaders = leader_table(&[("t", 0, 1)]);
        let fetchable = select_fetchable(
            vec![candidate("unknown", 0)],
            &[TopicPartition::new("t", 0)],
            leaders,
        );
        assert_eq!(fetchable, vec![candidate("unknown", 0)]);
    }

    #[test]
    fn select_fetchable_passes_everything_with_an_empty_buffer() {
        let leaders = leader_table(&[("t", 0, 1), ("t", 1, 1)]);
        let fetchable = select_fetchable(vec![candidate("t", 0), candidate("t", 1)], &[], leaders);
        assert_eq!(fetchable, vec![candidate("t", 0), candidate("t", 1)]);
    }

    // --- idle_backoff -----------------------------------------------------

    #[test]
    fn idle_backoff_clamps_to_the_remaining_poll_budget() {
        use std::time::Duration;

        // Plenty of budget left: the full retry backoff.
        assert_eq!(
            idle_backoff(
                Duration::from_millis(100),
                Duration::from_secs(1),
                Duration::from_millis(0),
            ),
            Duration::from_millis(100)
        );
        // Less budget than backoff: only what remains.
        assert_eq!(
            idle_backoff(
                Duration::from_millis(100),
                Duration::from_millis(120),
                Duration::from_millis(80),
            ),
            Duration::from_millis(40)
        );
        // Budget exhausted (or overshot): no sleep at all.
        assert_eq!(
            idle_backoff(
                Duration::from_millis(100),
                Duration::from_millis(50),
                Duration::from_millis(60),
            ),
            Duration::ZERO
        );
    }
}
