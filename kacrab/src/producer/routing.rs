//! Metadata-based producer routing.

use kacrab_protocol::KafkaUuid;

use super::{
    error::{ProducerError, Result},
    record::ProducerRecord,
};
use crate::wire::ClusterMetadata;

#[derive(Debug, Clone)]
pub(crate) struct ProduceRoute {
    pub(crate) topic: String,
    pub(crate) partition: i32,
    pub(crate) topic_id: KafkaUuid,
    pub(crate) leader_id: i32,
    pub(crate) base_sequence: Option<i32>,
    pub(crate) request_offset_delta: i64,
    pub(crate) record_count: usize,
}

pub(crate) fn route(metadata: &ClusterMetadata, record: &ProducerRecord) -> Result<ProduceRoute> {
    let topic_metadata = metadata
        .topic(record.topic.as_ref())
        .ok_or_else(|| ProducerError::UnknownTopic(record.topic.to_string()))?;
    let partition_metadata = topic_metadata
        .partitions
        .iter()
        .find(|partition_metadata| partition_metadata.partition_index == record.partition)
        .ok_or_else(|| ProducerError::UnknownPartition {
            topic: record.topic.to_string(),
            partition: record.partition,
        })?;
    if metadata
        .leader_for(record.topic.as_ref(), record.partition)
        .is_none()
    {
        return Err(ProducerError::LeaderNotFound {
            topic: record.topic.to_string(),
            partition: record.partition,
            leader_id: partition_metadata.leader_id,
        });
    }
    Ok(ProduceRoute {
        topic: record.topic.to_string(),
        partition: record.partition,
        topic_id: topic_metadata.topic_id,
        leader_id: partition_metadata.leader_id,
        base_sequence: None,
        request_offset_delta: 0,
        record_count: 0,
    })
}

#[cfg(test)]
pub(crate) fn partition_for_record(
    metadata: &ClusterMetadata,
    record: &ProducerRecord,
    ignore_keys: bool,
    next_round_robin: &mut i32,
) -> Result<i32> {
    if record.has_assigned_partition() {
        return Ok(record.partition);
    }
    let topic_metadata = metadata
        .topic(record.topic.as_ref())
        .ok_or_else(|| ProducerError::UnknownTopic(record.topic.to_string()))?;
    let partition_count = topic_metadata.partitions.len();
    if partition_count == 0 {
        return Err(ProducerError::UnknownPartition {
            topic: record.topic.to_string(),
            partition: record.partition,
        });
    }
    let offset = if ignore_keys {
        next_round_robin_offset(next_round_robin, partition_count)
    } else {
        record.key.as_ref().map_or_else(
            || next_round_robin_offset(next_round_robin, partition_count),
            |key| {
                let hash = usize::try_from(murmur2_java(key.as_ref()) & 0x7fff_ffff).unwrap_or(0);
                hash.checked_rem(partition_count).unwrap_or(0)
            },
        )
    };
    topic_metadata
        .partitions
        .get(offset)
        .map(|partition| partition.partition_index)
        .ok_or_else(|| ProducerError::UnknownPartition {
            topic: record.topic.to_string(),
            partition: record.partition,
        })
}

#[cfg(test)]
fn next_round_robin_offset(next_round_robin: &mut i32, partition_count: usize) -> usize {
    let next = usize::try_from(*next_round_robin).unwrap_or(0);
    *next_round_robin = next_round_robin
        .checked_add(1)
        .filter(|value| *value >= 0)
        .unwrap_or(0);
    next.checked_rem(partition_count).unwrap_or(0)
}

pub(crate) fn murmur2_java(input: &[u8]) -> u32 {
    const SEED: u32 = 0x9747_b28c;
    const M: u32 = 0x5bd1_e995;
    const R: u32 = 24;

    let length = u32::try_from(input.len()).unwrap_or(u32::MAX);
    let mut hash = SEED ^ length;
    let mut chunks = input.chunks_exact(4);
    for chunk in &mut chunks {
        let Ok(bytes) = <[u8; 4]>::try_from(chunk) else {
            continue;
        };
        let mut k = u32::from_le_bytes(bytes);
        k = k.wrapping_mul(M);
        k ^= k >> R;
        k = k.wrapping_mul(M);

        hash = hash.wrapping_mul(M);
        hash ^= k;
    }

    match chunks.remainder() {
        [a, b, c] => {
            hash ^= u32::from(*c) << 16;
            hash ^= u32::from(*b) << 8;
            hash ^= u32::from(*a);
            hash = hash.wrapping_mul(M);
        },
        [a, b] => {
            hash ^= u32::from(*b) << 8;
            hash ^= u32::from(*a);
            hash = hash.wrapping_mul(M);
        },
        [a] => {
            hash ^= u32::from(*a);
            hash = hash.wrapping_mul(M);
        },
        _ => {},
    }

    hash ^= hash >> 13;
    hash = hash.wrapping_mul(M);
    hash ^= hash >> 15;
    hash
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::cast_possible_wrap,
        clippy::expect_used,
        clippy::missing_assert_message,
        clippy::unwrap_used,
        reason = "Unit test fixtures fail fastest with contextual unwrap/expect calls; \
                  murmur2 hashes are reinterpreted as Java's signed int for comparison."
    )]

    use bytes::Bytes;
    use kacrab_protocol::KafkaUuid;

    use super::{murmur2_java, partition_for_record, route};
    use crate::{
        producer::{ProducerError, ProducerRecord},
        wire::{BrokerMetadata, ClusterMetadata, PartitionMetadata, TopicMetadata},
    };

    fn metadata(leader_id: i32) -> ClusterMetadata {
        ClusterMetadata {
            cluster_id: Some("cluster-a".to_owned()),
            controller_id: 7,
            brokers: vec![
                BrokerMetadata {
                    node_id: 7,
                    host: "localhost".to_owned(),
                    port: 9092,
                    rack: None,
                },
                BrokerMetadata {
                    node_id: 8,
                    host: "localhost".to_owned(),
                    port: 9093,
                    rack: None,
                },
            ],
            topics: vec![TopicMetadata {
                name: "orders".to_owned(),
                topic_id: KafkaUuid::ZERO,
                partitions: vec![
                    PartitionMetadata {
                        partition_index: 0,
                        leader_id,
                        leader_epoch: 1,
                        replica_nodes: vec![leader_id],
                        isr_nodes: vec![leader_id],
                        offline_replicas: Vec::new(),
                    },
                    PartitionMetadata {
                        partition_index: 1,
                        leader_id: 8,
                        leader_epoch: 1,
                        replica_nodes: vec![8],
                        isr_nodes: vec![8],
                        offline_replicas: Vec::new(),
                    },
                ],
            }],
        }
    }

    #[test]
    fn route_errors_when_topic_or_partition_is_missing() {
        let metadata = metadata(7);

        assert!(matches!(
            route(&metadata, &ProducerRecord::new("missing", 0)),
            Err(ProducerError::UnknownTopic(topic)) if topic == "missing"
        ));
        assert!(matches!(
            route(&metadata, &ProducerRecord::new("orders", 2)),
            Err(ProducerError::UnknownPartition { topic, partition })
                if topic == "orders" && partition == 2
        ));
    }

    #[test]
    fn route_errors_when_partition_leader_is_not_known_broker() {
        let metadata = metadata(9);

        assert!(matches!(
            route(&metadata, &ProducerRecord::new("orders", 0)),
            Err(ProducerError::LeaderNotFound { leader_id: 9, .. })
        ));
    }

    #[test]
    fn partition_for_record_matches_java_murmur2_and_round_robin() {
        let metadata = metadata(7);
        let keyed = ProducerRecord::unassigned("orders").key(Bytes::from_static(b"customer-42"));
        let unkeyed = ProducerRecord::unassigned("orders");
        let mut next_round_robin = 0;

        assert_eq!(
            partition_for_record(&metadata, &keyed, false, &mut next_round_robin)
                .expect("keyed partition"),
            1
        );
        assert_eq!(
            partition_for_record(&metadata, &unkeyed, false, &mut next_round_robin)
                .expect("first round-robin partition"),
            0
        );
        assert_eq!(
            partition_for_record(&metadata, &unkeyed, false, &mut next_round_robin)
                .expect("second round-robin partition"),
            1
        );
    }

    #[test]
    fn murmur2_matches_java_for_all_remainder_lengths() {
        // Authoritative values from Apache Kafka 4.3.0 `Utils.murmur2` (UtilsTest
        // vectors plus values computed from kafka-clients-4.3.0.jar), one per
        // `chunks_exact` tail length: key length mod 4 == 0, 1, 2, and 3. Java
        // returns a signed `int`, so the `u32` hash is reinterpreted with `as i32`.
        let cases: [(&[u8], i32); 8] = [
            (b"a-little-bit-long-string", -985_981_536), // len 24, %4 == 0
            (b"lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8", -58_897_971), // len 48, %4 == 0
            (b"a", -1_563_381_124),                      // len 1, %4 == 1
            (b"hello", 2_132_663_229),                   // len 5, %4 == 1
            (b"21", -973_932_308),                       // len 2, %4 == 2
            (b"foobar", -790_332_482),                   // len 6, %4 == 2
            (b"a-little-bit-longer-string", -1_486_304_829), // len 26, %4 == 2
            (b"abc", 479_470_107),                       // len 3, %4 == 3
        ];
        for (key, expected) in cases {
            assert_eq!(
                murmur2_java(key) as i32,
                expected,
                "murmur2 mismatch for key of length {}",
                key.len()
            );
        }
    }
}
