//! Building `Fetch` requests and decoding responses into records.
//!
//! Phase 1 uses sessionless full fetches (session id/epoch 0) capped at Fetch
//! v12 so partitions are keyed by topic *name* — this sidesteps the v13+
//! topic-id strict-codec requirement. Incremental fetch sessions (KIP-227) and
//! topic-id fetches are a Phase 3 optimization.

use std::collections::HashMap;

use bytes::Bytes;
use kacrab_protocol::{
    generated::{
        ApiKey, ErrorCode, FetchRequestData, FetchResponseData,
        fetch_request::{FetchPartition, FetchTopic, ReplicaState},
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

/// Highest `Fetch` version Phase 1 will negotiate. v12 is the last name-keyed
/// version; v13+ requires topic ids under the strict codec.
const FETCH_MAX_VERSION_PHASE1: i16 = 12;

/// The `LogAppendTime` bit (bit 3) in a record batch's attributes.
const LOG_APPEND_TIME_BIT: i16 = 0x0008;

/// Records collected for one partition plus the position to advance it to.
#[derive(Debug)]
pub(super) struct PartitionFetch {
    pub partition: TopicPartition,
    pub records: Vec<ConsumerRecord>,
    pub next_offset: i64,
    pub next_leader_epoch: Option<i32>,
}

/// Fetch from every fetchable partition (grouped by leader), decode the record
/// batches, and return per-partition records capped so the total does not exceed
/// `max_records`. Partitions are visited in a stable order; a partition trimmed
/// by the cap keeps the records it did not yield for the next poll (its position
/// only advances past what is returned).
pub(super) async fn fetch(
    wire: &WireClient,
    config: &ConsumerRuntimeConfig,
    metadata: &ClusterMetadata,
    fetchable: &[(TopicPartition, FetchPosition)],
    max_records: usize,
) -> Result<Vec<PartitionFetch>> {
    if fetchable.is_empty() || max_records == 0 {
        return Ok(Vec::new());
    }

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

    // `send_to_broker` treats `version` as a ceiling and negotiates down against
    // each broker's advertised range, so passing the v12 cap keeps fetches
    // name-keyed regardless of how high the broker goes.
    let version = FETCH_MAX_VERSION_PHASE1;

    let mut results = Vec::new();
    let mut budget = max_records;

    for (leader, entries) in by_leader {
        if budget == 0 {
            break;
        }
        let request = build_fetch_request(config, &entries);
        let response: FetchResponseData = wire
            .send_to_broker(leader, ApiKey::Fetch, version, &request)
            .await?;
        let top_level = ErrorCode::from(response.error_code);
        if top_level.is_error() {
            return Err(ConsumerError::broker(
                "fetch",
                top_level,
                "fetch request rejected",
            ));
        }

        let want: HashMap<(String, i32), FetchPosition> = entries
            .iter()
            .map(|(tp, pos)| ((tp.topic.clone(), tp.partition), *pos))
            .collect();

        for topic in response.responses {
            for partition in topic.partitions {
                let tp =
                    TopicPartition::new(topic.topic.as_str().to_owned(), partition.partition_index);
                let Some(position) = want.get(&(tp.topic.clone(), tp.partition)).copied() else {
                    continue;
                };
                let error = ErrorCode::from(partition.error_code);
                if error.is_error() {
                    return Err(ConsumerError::broker(
                        "fetch",
                        error,
                        format!("{}-{} fetch failed", tp.topic, tp.partition),
                    ));
                }
                let decoded = decode_partition(&tp, position.offset, partition.records, budget)?;
                if decoded.records.is_empty() && decoded.next_offset == position.offset {
                    continue;
                }
                budget = budget.saturating_sub(decoded.records.len());
                results.push(decoded);
                if budget == 0 {
                    break;
                }
            }
            if budget == 0 {
                break;
            }
        }
    }

    Ok(results)
}

fn build_fetch_request(
    config: &ConsumerRuntimeConfig,
    entries: &[(TopicPartition, FetchPosition)],
) -> FetchRequestData {
    let mut topics: Vec<FetchTopic> = Vec::new();
    for (partition, position) in entries {
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

    FetchRequestData {
        cluster_id: None,
        replica_id: -1,
        replica_state: ReplicaState {
            replica_id: -1,
            replica_epoch: -1,
            _unknown_tagged_fields: Vec::new(),
        },
        max_wait_ms: config.fetch_max_wait_ms,
        min_bytes: config.fetch_min_bytes,
        max_bytes: config.fetch_max_bytes,
        isolation_level: config.isolation_level.wire(),
        session_id: 0,
        session_epoch: 0,
        topics,
        forgotten_topics_data: Vec::new(),
        rack_id: config.client_rack.clone().into(),
        _unknown_tagged_fields: Vec::new(),
    }
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
}
