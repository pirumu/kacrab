//! Building `Fetch` requests and decoding responses into records.
//!
//! Fetches are capped at Fetch v12 so partitions are keyed by topic *name* —
//! this sidesteps the v13+ topic-id strict-codec requirement. Incremental fetch
//! sessions (KIP-227) are implemented per broker: the first fetch to a leader is
//! a full fetch that establishes a session, and subsequent fetches send only the
//! partitions whose position changed (plus a forgotten list for removed ones),
//! letting the broker return only partitions with new data. Behaviour is
//! identical to full fetches — it is purely a smaller request.

use std::collections::{HashMap, HashSet};

use bytes::Bytes;
use kacrab_protocol::{
    generated::{
        ApiKey, ErrorCode, FetchRequestData, FetchResponseData,
        fetch_request::{FetchPartition, FetchTopic, ForgottenTopic, ReplicaState},
    },
    record::decode_batches,
};

use super::{
    config::ConsumerRuntimeConfig,
    error::{ConsumerError, Result},
    offsets::partition_leader,
    record::{ConsumerRecord, TimestampType},
    subscription::FetchPosition,
};
use crate::{
    common::TopicPartition,
    wire::{ClusterMetadata, WireClient},
};

/// Highest `Fetch` version we negotiate. v12 is the last name-keyed version;
/// v13+ requires topic ids under the strict codec.
const FETCH_MAX_VERSION: i16 = 12;

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
    }

    /// Record a successful response: adopt the broker's session id and, when a
    /// session exists, advance the epoch and snapshot what the broker now holds.
    fn advance(&mut self, response_session_id: i32, entries: &[(TopicPartition, FetchPosition)]) {
        self.session_id = response_session_id;
        if response_session_id == INVALID_SESSION_ID {
            self.epoch = INITIAL_SESSION_EPOCH;
            self.sent.clear();
        } else {
            self.epoch = self.next_epoch();
            self.sent = entries
                .iter()
                .map(|(partition, position)| {
                    ((partition.topic.clone(), partition.partition), *position)
                })
                .collect();
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

/// The aggregate outcome of one `fetch` across every leader.
#[derive(Debug, Default)]
pub(super) struct FetchProgress {
    /// Decoded records, per partition.
    pub partitions: Vec<PartitionFetch>,
    /// Partitions the broker reported out of range — their position must be
    /// reset via `auto.offset.reset`.
    pub resets: Vec<TopicPartition>,
    /// Partitions whose leader/metadata looked stale — the caller should refresh
    /// metadata and retry on the next poll.
    pub stale: Vec<TopicPartition>,
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
        ErrorCode::NotLeaderOrFollower
        | ErrorCode::FencedLeaderEpoch
        | ErrorCode::UnknownLeaderEpoch
        | ErrorCode::UnknownTopicOrPartition
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

/// Fetch from every fetchable partition (grouped by leader), decode the record
/// batches, and return per-partition records capped so the total does not exceed
/// `max_records`. Partitions are visited in a stable order; a partition trimmed
/// by the cap keeps the records it did not yield for the next poll (its position
/// only advances past what is returned). Uses incremental fetch sessions
/// (KIP-227) per broker via `sessions`.
pub(super) async fn fetch(
    context: &FetchContext<'_>,
    fetchable: &[(TopicPartition, FetchPosition)],
    max_records: usize,
    sessions: &mut FetchSessions,
) -> Result<FetchProgress> {
    let mut progress = FetchProgress::default();
    if fetchable.is_empty() || max_records == 0 {
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

    let mut budget = max_records;

    for (leader, entries) in by_leader {
        if budget == 0 {
            break;
        }
        // Pick the Fetch request version from this broker's negotiated
        // `ApiVersions`, capped at `FETCH_MAX_VERSION` (v12) so fetches stay
        // name-keyed (topic-id fetch is unsupported). Falls back to the v12 cap
        // until `ApiVersions` has completed. `send_to_broker` re-clamps the
        // ceiling against the broker's range, so this is behaviourally identical
        // to passing the raw cap — it just makes the negotiated version explicit.
        let version = wire
            .negotiated_version(leader, ApiKey::Fetch)
            .map_or(FETCH_MAX_VERSION, |negotiated| {
                negotiated.min(FETCH_MAX_VERSION)
            });
        let session = sessions.by_broker.entry(leader).or_default();
        let request = build_fetch_request(config, session, &entries, max_wait_ms);
        let response: FetchResponseData = wire
            .send_to_broker(leader, ApiKey::Fetch, version, &request)
            .await?;
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
        if top_level.is_error() {
            return Err(ConsumerError::broker(
                "fetch",
                top_level,
                "fetch request rejected",
            ));
        }
        session.advance(response.session_id, &entries);

        let want: HashMap<(String, i32), FetchPosition> = entries
            .iter()
            .map(|(tp, pos)| ((tp.topic.clone(), tp.partition), *pos))
            .collect();

        collect_fetches(response, &want, &mut budget, &mut progress)?;
    }

    Ok(progress)
}

/// Process one broker's fetch response into decoded records, recovering
/// partition-level errors per partition (reset out-of-range positions, flag
/// stale-leader partitions for a metadata refresh) and only failing on a
/// genuinely fatal code. Partitions the caller did not ask for are ignored.
fn collect_fetches(
    response: FetchResponseData,
    want: &HashMap<(String, i32), FetchPosition>,
    budget: &mut usize,
    progress: &mut FetchProgress,
) -> Result<()> {
    for topic in response.responses {
        for partition in topic.partitions {
            if *budget == 0 {
                return Ok(());
            }
            let tp =
                TopicPartition::new(topic.topic.as_str().to_owned(), partition.partition_index);
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
            let decoded = decode_partition(&tp, position.offset, partition.records, *budget)?;
            if decoded.records.is_empty() && decoded.next_offset == position.offset {
                continue;
            }
            *budget = budget.saturating_sub(decoded.records.len());
            progress.partitions.push(decoded);
        }
    }
    Ok(())
}

fn build_fetch_request(
    config: &ConsumerRuntimeConfig,
    session: &BrokerFetchSession,
    entries: &[(TopicPartition, FetchPosition)],
    max_wait_ms: i32,
) -> FetchRequestData {
    let full = session.is_full();
    let mut topics: Vec<FetchTopic> = Vec::new();
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
            replica_directory_id: kacrab_protocol::KafkaUuid::default(),
            high_watermark: i64::MAX,
            _unknown_tagged_fields: Vec::new(),
        };
        if let Some(topic) = topics
            .iter_mut()
            .find(|topic| topic.topic.as_str() == partition.topic)
        {
            topic.partitions.push(wire_partition);
        } else {
            topics.push(FetchTopic {
                topic: partition.topic.clone().into(),
                topic_id: kacrab_protocol::KafkaUuid::default(),
                partitions: vec![wire_partition],
                _unknown_tagged_fields: Vec::new(),
            });
        }
    }
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
/// fetchable, grouped by topic — the incremental fetch's forgotten list.
fn build_forgotten(
    session: &BrokerFetchSession,
    entries: &[(TopicPartition, FetchPosition)],
) -> Vec<ForgottenTopic> {
    let current: HashSet<(&str, i32)> = entries
        .iter()
        .map(|(partition, _)| (partition.topic.as_str(), partition.partition))
        .collect();
    let mut forgotten: Vec<ForgottenTopic> = Vec::new();
    for (topic, partition) in session.sent.keys() {
        if current.contains(&(topic.as_str(), *partition)) {
            continue;
        }
        if let Some(entry) = forgotten
            .iter_mut()
            .find(|forgotten| forgotten.topic.as_str() == topic)
        {
            entry.partitions.push(*partition);
        } else {
            forgotten.push(ForgottenTopic {
                topic: topic.clone().into(),
                topic_id: kacrab_protocol::KafkaUuid::default(),
                partitions: vec![*partition],
                _unknown_tagged_fields: Vec::new(),
            });
        }
    }
    forgotten
}

/// Decode one partition's record bytes into records at or past `fetch_offset`,
/// taking at most `budget` records.
fn decode_partition(
    partition: &TopicPartition,
    fetch_offset: i64,
    records: Option<Bytes>,
    budget: usize,
) -> Result<PartitionFetch> {
    let mut out = Vec::new();
    let mut next_offset = fetch_offset;

    let Some(mut bytes) = records.filter(|b| !b.is_empty()) else {
        return Ok(PartitionFetch {
            partition: partition.clone(),
            records: out,
            next_offset,
            next_leader_epoch: None,
        });
    };

    let batches = decode_batches(&mut bytes)
        .map_err(|_| ConsumerError::InvalidState("failed to decode fetched record batch"))?;

    let mut leader_epoch = None;
    'outer: for batch in batches {
        leader_epoch = (batch.partition_leader_epoch >= 0).then_some(batch.partition_leader_epoch);
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
            if offset < fetch_offset {
                continue;
            }
            if out.len() >= budget {
                break 'outer;
            }
            let timestamp = if log_append_time {
                batch.max_timestamp
            } else {
                batch.first_timestamp.saturating_add(record.timestamp_delta)
            };
            out.push(ConsumerRecord {
                topic: partition.topic.clone(),
                partition: partition.partition,
                offset,
                timestamp,
                timestamp_type,
                key: record.key,
                value: record.value,
                headers: record.headers,
                leader_epoch,
            });
            next_offset = offset.saturating_add(1);
        }
    }

    Ok(PartitionFetch {
        partition: partition.clone(),
        records: out,
        next_offset,
        next_leader_epoch: leader_epoch,
    })
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
        let bytes = encode_batch(100, 3);
        let result = decode_partition(&tp(), 100, Some(bytes), 10).unwrap();
        assert_eq!(result.records.len(), 3);
        assert_eq!(result.records[0].offset, 100);
        assert_eq!(result.records[2].offset, 102);
        assert_eq!(result.records[0].value.as_deref(), Some(b"v0".as_ref()));
        assert_eq!(result.records[0].leader_epoch, Some(4));
        assert_eq!(result.next_offset, 103);
    }

    #[test]
    fn skips_records_before_the_fetch_offset() {
        let bytes = encode_batch(100, 3);
        let result = decode_partition(&tp(), 101, Some(bytes), 10).unwrap();
        assert_eq!(result.records.len(), 2);
        assert_eq!(result.records[0].offset, 101);
        assert_eq!(result.next_offset, 103);
    }

    #[test]
    fn caps_records_at_the_budget() {
        let bytes = encode_batch(100, 5);
        let result = decode_partition(&tp(), 100, Some(bytes), 2).unwrap();
        assert_eq!(result.records.len(), 2);
        assert_eq!(result.next_offset, 102);
    }

    #[test]
    fn empty_records_yield_no_progress() {
        let result = decode_partition(&tp(), 50, None, 10).unwrap();
        assert!(result.records.is_empty());
        assert_eq!(result.next_offset, 50);
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
        session.advance(42, &entries);
        assert!(!session.is_full());
        assert_eq!(session.session_id, 42);
        assert_eq!(session.epoch, 1);
        assert_eq!(session.sent.len(), 2);

        // A reset returns to a full fetch.
        session.reset();
        assert!(session.is_full());
        assert!(session.sent.is_empty());
    }

    #[test]
    fn session_without_broker_id_stays_full() {
        let mut session = BrokerFetchSession::default();
        session.advance(INVALID_SESSION_ID, &[entry("t", 0, 5)]);
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
        let full = build_fetch_request(&config, &session, &entries, 500);
        assert_eq!(full.session_epoch, INITIAL_SESSION_EPOCH);
        assert_eq!(
            full.topics
                .iter()
                .map(|t| t.partitions.len())
                .sum::<usize>(),
            2
        );
        assert!(full.forgotten_topics_data.is_empty());

        session.advance(99, &entries);

        // Incrementally, only the partition whose offset changed is resent.
        let changed = vec![entry("t", 0, 10), entry("t", 1, 25)];
        let incremental = build_fetch_request(&config, &session, &changed, 500);
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
        // t-0 is reset and t-1's records are still returned.
        let response = fetch_response(
            "t",
            vec![
                partition_data(0, ErrorCode::OffsetOutOfRange, None),
                partition_data(1, ErrorCode::None, Some(encode_batch(0, 2))),
            ],
        );
        let mut budget = 100;
        let mut progress = FetchProgress::default();
        collect_fetches(response, &want_two(), &mut budget, &mut progress).expect("no fatal error");
        assert_eq!(progress.resets, vec![TopicPartition::new("t", 0)]);
        assert!(progress.stale.is_empty());
        assert_eq!(progress.partitions.len(), 1);
        assert_eq!(
            progress.partitions[0].partition,
            TopicPartition::new("t", 1)
        );
        assert_eq!(progress.partitions[0].records.len(), 2);
    }

    #[test]
    fn stale_leader_partition_is_flagged_not_fatal() {
        let response = fetch_response(
            "t",
            vec![partition_data(0, ErrorCode::NotLeaderOrFollower, None)],
        );
        let mut budget = 100;
        let mut progress = FetchProgress::default();
        collect_fetches(response, &want_two(), &mut budget, &mut progress).expect("retriable");
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
        let mut budget = 100;
        let mut progress = FetchProgress::default();
        assert!(collect_fetches(response, &want_two(), &mut budget, &mut progress).is_err());
    }

    #[test]
    fn forgotten_lists_partitions_dropped_from_the_session() {
        let mut session = BrokerFetchSession::default();
        session.advance(7, &[entry("t", 0, 1), entry("t", 1, 1), entry("u", 0, 1)]);
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
}
