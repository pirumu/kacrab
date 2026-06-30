#![allow(
    clippy::expect_used,
    clippy::field_reassign_with_default,
    clippy::missing_assert_message,
    clippy::significant_drop_tightening,
    clippy::unwrap_used,
    reason = "Unit test fixtures fail fastest with contextual unwrap/expect and direct state \
              setup."
)]

use std::{
    collections::VecDeque,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
    time::{Duration, Instant},
};

use ahash::AHashSet;
use bytes::Bytes;
use kacrab_protocol::{
    KafkaString, KafkaUuid,
    compression::Compression,
    generated::{
        AddPartitionsToTxnPartitionResult, AddPartitionsToTxnResponseData,
        AddPartitionsToTxnResult, AddPartitionsToTxnTopicResult, ApiKey, ErrorCode,
        ProduceRequestData,
    },
    version::client_api_info,
};

use super::{
    BrokerProduceOptions, BrokerProduceRequest, BrokerRequestPlacement, DispatchError,
    IdempotentRetryDecision, PartitionLoadRefresh, PendingTransactionOperationGuard,
    ProduceRequestSizing, ProducerDispatcher, ProducerIdempotenceState, ProducerPartitionerState,
    RECORD_BATCH_OVERHEAD_BYTES, RecordBufferRelease, SharedAccumulator, TopicPartitionKey,
    TransactionOperation, TransactionPendingOperationStart, broker_dispatch_completed_result,
    broker_request_placement_for_batch, build_partition_load_stats, choose_coordinator_addr,
    complete_deliveries, end_txn_version, estimate_record_batch_bytes,
    estimate_sticky_record_bytes, fail_pending_transaction_operation, init_producer_id_version,
    is_fatal_transaction_error, is_leadership_error, no_ack_receipts,
    pop_dispatchable_broker_request, produce_version, txn_offset_commit_version,
    uniform_partition_for_random, unique_topics, unique_unassigned_record_topics,
    validate_consumer_group_metadata,
};
use crate::{
    producer::{
        AccumulatorConfig, ConsumerGroupMetadata, ProducerCompression, ProducerError,
        ProducerIdempotenceConfig, ProducerIdentity, ProducerRecord, ProducerRuntimeConfig,
        RecordMetadata, compression_ratio::CompressionRatioEstimator,
    },
    wire::{
        BrokerEndpoint, BrokerMetadata, ClusterMetadata, ConnectionConfig, PartitionMetadata,
        TopicMetadata, WireClient,
    },
};

const TEST_LARGE_BATCH_SIZE: usize = 16 * 1024;

fn test_wire() -> WireClient {
    WireClient::connect_with_brokers(
        ConnectionConfig::default(),
        "dispatcher-test",
        [BrokerEndpoint::new(
            7,
            "127.0.0.1:9092".parse().expect("valid socket address"),
        )],
    )
}

fn transactional_dispatcher() -> ProducerDispatcher {
    ProducerDispatcher::with_config(
        test_wire(),
        ProducerRuntimeConfig {
            idempotence: ProducerIdempotenceConfig {
                enabled: true,
                transactional_id: Some("txn-a".to_owned()),
                transaction_timeout_ms: 30_000,
                transaction_two_phase_commit: false,
            },
            ..ProducerRuntimeConfig::default()
        },
    )
}

#[test]
fn dispatcher_metrics_enablement_is_shared_across_clones() {
    let dispatcher = ProducerDispatcher::new(test_wire());
    let cloned = dispatcher.clone();

    dispatcher.enable_metrics();

    assert!(cloned.metrics_are_enabled());
}

fn route(topic: &str, partition: i32) -> super::ProduceRoute {
    super::ProduceRoute {
        topic: topic.to_owned(),
        partition,
        topic_id: KafkaUuid::ZERO,
        leader_id: 7,
        base_sequence: None,
        request_offset_delta: 0,
        record_count: 0,
    }
}

#[test]
fn idempotent_dispatch_keeps_configured_broker_in_flight_limit() {
    let idempotent = ProducerDispatcher::with_config(
        test_wire(),
        ProducerRuntimeConfig {
            max_in_flight_requests_per_connection: 5,
            idempotence: ProducerIdempotenceConfig {
                enabled: true,
                ..ProducerIdempotenceConfig::default()
            },
            ..ProducerRuntimeConfig::default()
        },
    );
    let non_idempotent = ProducerDispatcher::with_config(
        test_wire(),
        ProducerRuntimeConfig {
            max_in_flight_requests_per_connection: 5,
            idempotence: ProducerIdempotenceConfig {
                enabled: false,
                ..ProducerIdempotenceConfig::default()
            },
            ..ProducerRuntimeConfig::default()
        },
    );

    assert_eq!(idempotent.broker_dispatch_in_flight_limit(), 5);
    assert_eq!(non_idempotent.broker_dispatch_in_flight_limit(), 5);
}

#[test]
fn dispatcher_retry_delay_doubles_to_runtime_max() {
    let dispatcher = ProducerDispatcher::with_config(
        test_wire(),
        ProducerRuntimeConfig {
            retry_backoff: Duration::from_millis(25),
            retry_backoff_max: Duration::from_millis(80),
            ..ProducerRuntimeConfig::default()
        },
    );
    let mut retry = dispatcher.retry_backoff_state();

    assert_eq!(retry.next_delay_with_sample(0.5), Duration::from_millis(25));
    assert_eq!(retry.next_delay_with_sample(0.5), Duration::from_millis(50));
    assert_eq!(retry.next_delay_with_sample(0.5), Duration::from_millis(80));
}

#[test]
fn transaction_control_unsupported_version_is_fatal_like_java() {
    assert!(is_fatal_transaction_error(ErrorCode::UnsupportedVersion));
}

#[test]
fn transaction_request_queue_orders_requests_like_java_transaction_manager() {
    let mut queue = super::TransactionRequestQueue::default();
    queue.push(super::TransactionRequestKind::EndTxn);
    queue.push(super::TransactionRequestKind::AddPartitionsOrOffsets);
    queue.push(super::TransactionRequestKind::EpochBump);
    queue.push(super::TransactionRequestKind::FindCoordinator);
    queue.push(super::TransactionRequestKind::InitProducerId);
    queue.push(super::TransactionRequestKind::AddPartitionsOrOffsets);

    let mut ordered = Vec::new();
    while let Some(kind) = queue.pop_next() {
        ordered.push(kind);
    }

    assert_eq!(
        ordered,
        vec![
            super::TransactionRequestKind::FindCoordinator,
            super::TransactionRequestKind::InitProducerId,
            super::TransactionRequestKind::AddPartitionsOrOffsets,
            super::TransactionRequestKind::AddPartitionsOrOffsets,
            super::TransactionRequestKind::EndTxn,
            super::TransactionRequestKind::EpochBump,
        ]
    );
}

#[test]
fn transaction_state_transition_matrix_matches_java_transaction_manager() {
    use super::super::transaction::TransactionState;

    let allowed = [
        (TransactionState::Ready, TransactionState::Uninitialized),
        (
            TransactionState::AbortableError,
            TransactionState::Uninitialized,
        ),
        (
            TransactionState::Uninitialized,
            TransactionState::Initializing,
        ),
        (
            TransactionState::CommittingTransaction,
            TransactionState::Initializing,
        ),
        (
            TransactionState::AbortingTransaction,
            TransactionState::Initializing,
        ),
        (TransactionState::Initializing, TransactionState::Ready),
        (
            TransactionState::CommittingTransaction,
            TransactionState::Ready,
        ),
        (
            TransactionState::AbortingTransaction,
            TransactionState::Ready,
        ),
        (TransactionState::Ready, TransactionState::InTransaction),
        (
            TransactionState::InTransaction,
            TransactionState::PreparedTransaction,
        ),
        (
            TransactionState::Initializing,
            TransactionState::PreparedTransaction,
        ),
        (
            TransactionState::InTransaction,
            TransactionState::CommittingTransaction,
        ),
        (
            TransactionState::PreparedTransaction,
            TransactionState::CommittingTransaction,
        ),
        (
            TransactionState::InTransaction,
            TransactionState::AbortingTransaction,
        ),
        (
            TransactionState::PreparedTransaction,
            TransactionState::AbortingTransaction,
        ),
        (
            TransactionState::AbortableError,
            TransactionState::AbortingTransaction,
        ),
        (
            TransactionState::InTransaction,
            TransactionState::AbortableError,
        ),
        (
            TransactionState::CommittingTransaction,
            TransactionState::AbortableError,
        ),
        (
            TransactionState::AbortableError,
            TransactionState::AbortableError,
        ),
        (
            TransactionState::Initializing,
            TransactionState::AbortableError,
        ),
    ];

    for source in TransactionState::ALL {
        for target in TransactionState::ALL {
            let expected =
                target == TransactionState::FatalError || allowed.contains(&(source, target));
            assert_eq!(
                source.is_transition_valid(target),
                expected,
                "transition {source:?} -> {target:?}"
            );
        }
    }
}

#[test]
fn transaction_request_queue_holds_end_txn_until_batches_drain_like_java() {
    let mut queue = super::TransactionRequestQueue::default();
    queue.push(super::TransactionRequestKind::EndTxn);
    queue.push(super::TransactionRequestKind::EpochBump);

    assert_eq!(queue.next_request(true), None);
    assert_eq!(
        queue.next_request(false),
        Some(super::TransactionRequestKind::EndTxn)
    );
    assert_eq!(
        queue.next_request(false),
        Some(super::TransactionRequestKind::EpochBump)
    );
}

#[test]
fn transaction_partition_sets_move_new_to_pending_to_in_transaction_like_java() {
    let mut state = ProducerIdempotenceState::default();
    let key = TopicPartitionKey {
        topic: "orders".to_owned(),
        partition: 0,
    };

    assert!(state.mark_new_transaction_partition(key.clone()));
    assert!(!state.mark_new_transaction_partition(key.clone()));
    assert!(state.is_partition_pending_add(&key));
    assert!(!state.transaction_contains_partition(&key));

    let pending = state.begin_pending_transaction_partitions();
    assert_eq!(pending, vec![key.clone()]);
    assert!(state.new_partitions_in_transaction.is_empty());
    assert!(state.pending_partitions_in_transaction.contains(&key));
    assert!(state.is_partition_pending_add(&key));

    state.complete_pending_transaction_partitions(&pending);
    assert!(!state.is_partition_pending_add(&key));
    assert!(state.transaction_contains_partition(&key));
}

#[test]
fn consumer_group_metadata_requires_member_for_known_generation() {
    assert!(matches!(
        validate_consumer_group_metadata(
            &ConsumerGroupMetadata::new("group-a").generation_id(42)
        ),
        Err(ProducerError::InvalidConsumerGroupMetadata(message))
            if message == "generation_id > 0 requires a known member_id"
    ));

    validate_consumer_group_metadata(
        &ConsumerGroupMetadata::new("group-a")
            .generation_id(42)
            .member_id("member-a"),
    )
    .expect("known generation with member id is valid");
}

#[test]
fn broker_request_scheduler_skips_in_flight_partition_without_reordering_it() {
    let options = BrokerProduceOptions {
        acks: 1,
        timeout_ms: 1_500,
        transactional_id: None,
    };
    let mut first_partition = BrokerProduceRequest::default();
    first_partition.push(
        route("orders", 0),
        Bytes::from_static(b"records-0a"),
        options,
        1,
    );
    let mut other_partition = BrokerProduceRequest::default();
    other_partition.push(
        route("orders", 1),
        Bytes::from_static(b"records-1"),
        options,
        1,
    );
    let mut next_first_partition = BrokerProduceRequest::default();
    next_first_partition.push(
        route("orders", 0),
        Bytes::from_static(b"records-0b"),
        options,
        1,
    );
    let mut pending = VecDeque::from([
        (0, first_partition),
        (1, other_partition),
        (2, next_first_partition),
    ]);
    let mut in_flight_routes = AHashSet::new();
    let _inserted = in_flight_routes.insert(TopicPartitionKey {
        topic: "orders".to_owned(),
        partition: 0,
    });

    let Some((index, request)) =
        pop_dispatchable_broker_request(&mut pending, &in_flight_routes, true)
    else {
        panic!("other partition should remain dispatchable");
    };
    assert_eq!(index, 1);
    assert_eq!(
        request
            .routes
            .first()
            .expect("route should exist")
            .partition,
        1
    );

    in_flight_routes.clear();
    let Some((index, request)) =
        pop_dispatchable_broker_request(&mut pending, &in_flight_routes, true)
    else {
        panic!("first partition should dispatch after in-flight completion");
    };
    assert_eq!(index, 0);
    assert_eq!(
        request
            .routes
            .first()
            .expect("route should exist")
            .partition,
        0
    );
}

fn add_partitions_response(
    topic: &str,
    partition: i32,
    error: ErrorCode,
) -> AddPartitionsToTxnResponseData {
    AddPartitionsToTxnResponseData {
        results_by_transaction: vec![AddPartitionsToTxnResult {
            transactional_id: KafkaString::from("txn-a".to_owned()),
            topic_results: vec![AddPartitionsToTxnTopicResult {
                name: KafkaString::from(topic.to_owned()),
                results_by_partition: vec![AddPartitionsToTxnPartitionResult {
                    partition_index: partition,
                    partition_error_code: i16::from(error),
                    _unknown_tagged_fields: Vec::new(),
                }],
                _unknown_tagged_fields: Vec::new(),
            }],
            _unknown_tagged_fields: Vec::new(),
        }],
        ..AddPartitionsToTxnResponseData::default()
    }
}

fn metadata_with_partitions(topic: &str, partitions: usize) -> ClusterMetadata {
    let mut partition_metadata = Vec::with_capacity(partitions);
    for partition in 0..partitions {
        let partition_index = i32::try_from(partition).expect("test partition fits i32");
        partition_metadata.push(PartitionMetadata {
            partition_index,
            leader_id: 7,
            leader_epoch: 1,
            replica_nodes: vec![7],
            isr_nodes: vec![7],
            offline_replicas: Vec::new(),
        });
    }
    ClusterMetadata {
        cluster_id: Some("cluster-a".to_owned()),
        controller_id: 7,
        brokers: vec![BrokerMetadata {
            node_id: 7,
            host: "localhost".to_owned(),
            port: 9092,
            rack: None,
        }],
        topics: vec![TopicMetadata {
            name: topic.to_owned(),
            topic_id: KafkaUuid::ZERO,
            partitions: partition_metadata,
        }],
    }
}

#[test]
fn default_partitioner_sticks_unkeyed_records_until_batch_budget() {
    let metadata = metadata_with_partitions("orders", 3);
    let mut state = ProducerPartitionerState::default();
    let record = ProducerRecord::unassigned("orders").value(Bytes::from_static(b"1234567890"));

    let partitions: Vec<_> = (0..4)
        .map(|_| {
            state
                .partition_for_record(&metadata, &record, false, true, 128, 1.0)
                .expect("partition")
        })
        .collect();
    assert!(
        partitions
            .iter()
            .all(|partition| *partition == partitions[0])
    );

    state.mark_sticky_batch_ready("orders", 128);
    let _next_partition = state
        .partition_for_record(&metadata, &record, false, true, 128, 1.0)
        .expect("partition");
    let sticky = state
        .sticky_by_topic
        .get("orders")
        .expect("sticky state should exist");

    assert!(!sticky.switch_on_next);
}

#[test]
fn sticky_partitioner_waits_for_batch_ready_before_switching_like_java() {
    let metadata = metadata_with_partitions("orders", 3);
    let mut state = ProducerPartitionerState::default();
    let record = ProducerRecord::unassigned("orders").value(Bytes::from_static(b"1234567890"));
    let sticky_batch_size = RECORD_BATCH_OVERHEAD_BYTES + estimate_record_batch_bytes(&record) + 1;

    let partitions: Vec<_> = (0..3)
        .map(|_| {
            state
                .partition_for_record(&metadata, &record, false, true, sticky_batch_size, 1.0)
                .expect("partition")
        })
        .collect();

    assert!(
        partitions
            .iter()
            .all(|partition| *partition == partitions[0])
    );
}

#[test]
fn sticky_partitioner_uses_compression_ratio_estimate_for_batch_budget() {
    let metadata = metadata_with_partitions("orders", 3);
    let record = ProducerRecord::unassigned("orders").value(Bytes::from(vec![b'x'; 128]));
    let sticky_batch_size = RECORD_BATCH_OVERHEAD_BYTES + estimate_record_batch_bytes(&record) / 2;

    let mut uncompressed = ProducerPartitionerState::default();
    let uncompressed_partitions: Vec<_> = (0..4)
        .map(|_| {
            uncompressed
                .partition_for_record_with_compression_ratio(
                    &metadata,
                    &record,
                    false,
                    false,
                    sticky_batch_size,
                    1.0,
                )
                .expect("partition")
        })
        .collect();

    let mut compressed = ProducerPartitionerState::default();
    let compressed_partitions: Vec<_> = (0..4)
        .map(|_| {
            compressed
                .partition_for_record_with_compression_ratio(
                    &metadata,
                    &record,
                    false,
                    false,
                    sticky_batch_size,
                    0.25,
                )
                .expect("partition")
        })
        .collect();

    assert_ne!(
        uncompressed_partitions[2], uncompressed_partitions[0],
        "raw sizing should exhaust the sticky batch before the third record"
    );
    assert!(
        compressed_partitions
            .iter()
            .all(|partition| *partition == compressed_partitions[0]),
        "compressed sizing should keep the sticky partition longer"
    );
}

#[test]
#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    reason = "Test mirrors Kafka's f32 compression estimate formula exactly."
)]
fn sticky_record_estimate_uses_java_compression_safety_factor() {
    let record = ProducerRecord::unassigned("orders").value(Bytes::from(vec![b'x'; 128]));
    let raw = estimate_record_batch_bytes(&record);
    let estimated = estimate_sticky_record_bytes(&record, 0.50);

    assert_eq!(estimated, ((raw as f32) * 0.50 * 1.05).ceil() as usize);
}

#[test]
fn cached_sticky_partition_assigns_without_metadata_until_batch_ready() {
    let metadata = metadata_with_partitions("orders", 3);
    let mut state = ProducerPartitionerState::default();
    let record = ProducerRecord::unassigned("orders").value(Bytes::from_static(b"1234567890"));
    let sticky_batch_size = 1024;
    let partition = state
        .partition_for_record(&metadata, &record, false, true, sticky_batch_size, 1.0)
        .expect("initial sticky partition");

    let mut cached_record =
        ProducerRecord::unassigned("orders").value(Bytes::from_static(b"cached"));
    assert!(state.try_assign_cached_sticky_partition(&mut cached_record, sticky_batch_size, 1.0));
    assert_eq!(cached_record.partition, partition);

    state.mark_sticky_batch_ready("orders", 1);
    let mut next_record = ProducerRecord::unassigned("orders").value(Bytes::from_static(b"next"));
    assert!(!state.try_assign_cached_sticky_partition(&mut next_record, sticky_batch_size, 1.0));
    assert_eq!(
        next_record.partition,
        crate::producer::record::UNASSIGNED_PARTITION
    );
}

#[test]
fn default_partitioner_hashes_keyed_records_with_java_murmur2() {
    let metadata = metadata_with_partitions("orders", 3);
    let mut state = ProducerPartitionerState::default();
    let record = ProducerRecord::unassigned("orders")
        .key(Bytes::from_static(b"customer-42"))
        .value(Bytes::from_static(b"1234567890"));
    let mut next_round_robin = 0;
    let expected = super::super::routing::partition_for_record(
        &metadata,
        &record,
        false,
        &mut next_round_robin,
    )
    .expect("keyed partition");

    for _ in 0..5 {
        assert_eq!(
            state
                .partition_for_record(&metadata, &record, false, true, 256, 1.0)
                .expect("partition"),
            expected
        );
    }
}

#[test]
fn ignore_keys_uses_sticky_default_partitioner() {
    let metadata = metadata_with_partitions("orders", 3);
    let mut state = ProducerPartitionerState::default();
    let record = ProducerRecord::unassigned("orders")
        .key(Bytes::from_static(b"customer-42"))
        .value(Bytes::from_static(b"1234567890"));

    let partitions: Vec<_> = (0..3)
        .map(|_| {
            state
                .partition_for_record(&metadata, &record, true, true, 128, 1.0)
                .expect("partition")
        })
        .collect();
    assert!(
        partitions
            .iter()
            .all(|partition| *partition == partitions[0])
    );

    state.mark_sticky_batch_ready("orders", 128);
    let after_switch: Vec<_> = (0..2)
        .map(|_| {
            state
                .partition_for_record(&metadata, &record, true, true, 128, 1.0)
                .expect("partition")
        })
        .collect();

    assert!(
        after_switch
            .iter()
            .all(|partition| *partition == after_switch[0])
    );
}

#[test]
fn sticky_partitioner_fallback_skips_unavailable_partitions_like_java() {
    let mut metadata = metadata_with_partitions("orders", 3);
    metadata.topics[0].partitions[0].leader_id = -1;
    metadata.topics[0].partitions[2].leader_id = -1;
    let mut state = ProducerPartitionerState::default();
    let record = ProducerRecord::unassigned("orders");

    let partitions: Vec<_> = (0..3)
        .map(|_| {
            state
                .partition_for_record(&metadata, &record, true, true, 1, 1.0)
                .expect("partition")
        })
        .collect();

    assert_eq!(partitions, vec![1, 1, 1]);
}

#[test]
fn adaptive_sticky_builds_weighted_load_stats_like_java() {
    let stats = build_partition_load_stats(&[0, 3, 1], &[0, 1, 2], 3).expect("adaptive stats");

    assert_eq!(stats.cumulative_frequency_table, vec![4, 5, 8]);
    assert_eq!(stats.partition_ids, vec![0, 1, 2]);
    assert!(build_partition_load_stats(&[1, 1, 1], &[0, 1, 2], 3).is_none());
}

#[test]
fn adaptive_sticky_selects_partition_from_weighted_frequency_table() {
    let metadata = metadata_with_partitions("orders", 3);
    let topic_metadata = metadata.topic("orders").expect("topic metadata");
    let mut state = ProducerPartitionerState::default();
    state.update_partition_load_stats("orders", &[0, 3, 1], &[0, 1, 2], 3);

    let partition = state
        .adaptive_partition_for_random("orders", topic_metadata, 4)
        .expect("adaptive partition");

    assert_eq!(partition, 1);
}

#[test]
fn adaptive_sticky_distribution_matches_java_builtin_partitioner_oracle() {
    let metadata = metadata_with_partitions("orders", 5);
    let topic_metadata = metadata.topic("orders").expect("topic metadata");
    let mut state = ProducerPartitionerState::default();
    state.update_partition_load_stats("orders", &[5, 0, 3, 0, 1], &[0, 1, 2, 3, 4], 5);
    let expected_frequencies = [1_usize, 6, 3, 6, 5];
    let range_end = expected_frequencies.iter().copied().sum::<usize>();
    let mut actual_frequencies = [0_usize; 5];

    for random in 0..range_end {
        let partition = state
            .adaptive_partition_for_random("orders", topic_metadata, random)
            .expect("adaptive partition");
        let index = usize::try_from(partition).expect("non-negative partition");
        actual_frequencies[index] += 1;
    }

    assert_eq!(actual_frequencies, expected_frequencies);
}

#[test]
fn sticky_uniform_fallback_uses_random_available_partition_like_java() {
    let mut metadata = metadata_with_partitions("orders", 4);
    metadata.topics[0].partitions[0].leader_id = -1;
    metadata.topics[0].partitions[1].leader_id = 7;
    metadata.topics[0].partitions[2].leader_id = -1;
    metadata.topics[0].partitions[3].leader_id = 9;
    let topic_metadata = metadata.topic("orders").expect("topic metadata");

    assert_eq!(
        uniform_partition_for_random("orders", topic_metadata, 0).expect("partition"),
        1
    );
    assert_eq!(
        uniform_partition_for_random("orders", topic_metadata, 1).expect("partition"),
        3
    );

    metadata.topics[0]
        .partitions
        .iter_mut()
        .for_each(|partition| partition.leader_id = -1);
    let topic_metadata = metadata.topic("orders").expect("topic metadata");
    assert_eq!(
        uniform_partition_for_random("orders", topic_metadata, 2).expect("partition"),
        2
    );
}

#[test]
fn adaptive_sticky_updates_load_stats_from_accumulator_queues() {
    let metadata = metadata_with_partitions("orders", 3);
    let topic_metadata = metadata.topic("orders").expect("topic metadata");
    let accumulator = SharedAccumulator::with_config(AccumulatorConfig::default().batch_size(1));
    let now = Instant::now();
    accumulator
        .append_at(ProducerRecord::new("orders", 0), now)
        .expect("append partition 0");
    for _ in 0..3 {
        accumulator
            .append_at(ProducerRecord::new("orders", 1), now)
            .expect("append partition 1");
    }
    accumulator
        .append_at(ProducerRecord::new("orders", 2), now)
        .expect("append partition 2");

    let mut state = ProducerPartitionerState::default();
    state.update_partition_load_stats_from_accumulator("orders", topic_metadata, &accumulator);

    let partition = state
        .adaptive_partition_for_random("orders", topic_metadata, 4)
        .expect("adaptive partition");

    assert_eq!(partition, 2);
}

#[test]
fn adaptive_sticky_keeps_drained_empty_partition_queues_like_java() {
    let metadata = metadata_with_partitions("orders", 3);
    let topic_metadata = metadata.topic("orders").expect("topic metadata");
    let accumulator = SharedAccumulator::with_config(
        AccumulatorConfig::default()
            .batch_size(128)
            .linger(Duration::from_mins(1)),
    );
    let now = Instant::now();
    accumulator
        .append_at(
            ProducerRecord::partition_value("orders", 0, vec![0_u8; 256]),
            now,
        )
        .expect("append ready partition 0");
    accumulator
        .append_at(ProducerRecord::new("orders", 1), now)
        .expect("append partition 1");
    accumulator
        .append_at(ProducerRecord::new("orders", 2), now)
        .expect("append partition 2");

    let drained = accumulator.drain_ready(now);
    assert_eq!(drained.len(), 1);
    assert_eq!(drained[0].partition, 0);

    let mut state = ProducerPartitionerState::default();
    state.update_partition_load_stats_from_accumulator("orders", topic_metadata, &accumulator);

    let partition = state
        .adaptive_partition_for_random("orders", topic_metadata, 0)
        .expect("adaptive partition should include the drained empty queue");

    assert_eq!(partition, 0);
}

#[test]
fn adaptive_sticky_excludes_partitions_whose_leader_exceeds_availability_timeout() {
    let mut metadata = metadata_with_partitions("orders", 3);
    metadata.topics[0].partitions[0].leader_id = 7;
    metadata.topics[0].partitions[1].leader_id = 8;
    metadata.topics[0].partitions[2].leader_id = 9;
    let topic_metadata = metadata.topic("orders").expect("topic metadata");
    let accumulator = SharedAccumulator::with_config(AccumulatorConfig::default().batch_size(1024));
    let now = Instant::now();
    for partition in 0..3 {
        accumulator
            .append_at(ProducerRecord::new("orders", partition), now)
            .expect("append partition");
    }

    let mut state = ProducerPartitionerState::default();
    state.update_broker_drain_stats(
        8,
        now.checked_sub(Duration::from_secs(2))
            .expect("test time before now"),
        true,
    );
    state.update_broker_drain_stats(8, now, false);
    state.update_partition_load_stats_from_accumulator_at(PartitionLoadRefresh {
        topic: "orders",
        topic_metadata,
        accumulator: &accumulator,
        now,
        availability_timeout: Duration::from_secs(1),
    });

    let partition = state
        .adaptive_partition_for_random("orders", topic_metadata, 1)
        .expect("adaptive partition");

    assert_eq!(partition, 2);
}

#[test]
fn adaptive_sticky_updates_node_latency_stats_like_java_record_accumulator() {
    let mut metadata = metadata_with_partitions("orders", 3);
    metadata.topics[0].partitions[0].leader_id = 7;
    metadata.topics[0].partitions[1].leader_id = 8;
    metadata.topics[0].partitions[2].leader_id = 9;
    let topic_metadata = metadata.topic("orders").expect("topic metadata");
    let accumulator = SharedAccumulator::with_config(AccumulatorConfig::default().batch_size(1024));
    let now = Instant::now();
    for partition in 0..3 {
        accumulator
            .append_at(ProducerRecord::new("orders", partition), now)
            .expect("append partition");
    }

    let mut state = ProducerPartitionerState::default();
    state.update_broker_latency_stats(8, now, true);
    state.update_broker_latency_stats(
        8,
        now.checked_add(Duration::from_secs(2))
            .expect("test time after now"),
        false,
    );

    let latency = state
        .broker_drain_stats_by_id
        .get(&8)
        .expect("broker stats should exist");
    assert_eq!(latency.drain_at, now);
    assert_eq!(
        latency.ready_at,
        now.checked_add(Duration::from_secs(2))
            .expect("test time after now")
    );

    state.update_partition_load_stats_from_accumulator_at(PartitionLoadRefresh {
        topic: "orders",
        topic_metadata,
        accumulator: &accumulator,
        now: now
            .checked_add(Duration::from_secs(2))
            .expect("test time after now"),
        availability_timeout: Duration::from_secs(1),
    });

    let partition = state
        .adaptive_partition_for_random("orders", topic_metadata, 1)
        .expect("adaptive partition");

    assert_eq!(partition, 2);
}

#[test]
fn unique_unassigned_record_topics_skips_assigned_records_and_duplicates() {
    let records = [
        ProducerRecord::new("orders", 0),
        ProducerRecord::unassigned("orders"),
        ProducerRecord::unassigned("payments"),
        ProducerRecord::unassigned("orders"),
        ProducerRecord::new("payments", 1),
    ];

    assert_eq!(
        unique_unassigned_record_topics(&records),
        vec!["orders".to_owned(), "payments".to_owned()]
    );
}

#[test]
fn compression_ratio_estimator_updates_and_resets_like_java() {
    let mut estimator = CompressionRatioEstimator::default();

    assert_ratio_close(estimator.estimation("orders", Compression::Lz4), 1.0);
    assert_ratio_close(
        estimator.update_estimation("orders", Compression::Lz4, 0.60),
        0.995,
    );
    assert_ratio_close(
        estimator.update_estimation("orders", Compression::Lz4, 1.30),
        1.30,
    );
    assert_ratio_close(
        estimator.update_estimation("orders", Compression::Lz4, 1.20),
        1.295,
    );

    estimator.reset_after_split("orders", Compression::Lz4, 0.42);
    assert_ratio_close(estimator.estimation("orders", Compression::Lz4), 1.0);

    estimator.reset_after_split("orders", Compression::Lz4, 1.25);
    assert_ratio_close(estimator.estimation("orders", Compression::Lz4), 1.25);
}

#[tokio::test]
async fn message_too_large_split_resets_compression_ratio_estimation() {
    let dispatcher = ProducerDispatcher::with_config(
        test_wire(),
        ProducerRuntimeConfig {
            compression: ProducerCompression {
                codec: Compression::None,
                level: None,
            },
            ..ProducerRuntimeConfig::default()
        },
    );
    dispatcher.set_compression_ratio_estimation_for_test("orders", Compression::None, 0.40);
    let batches = vec![ready_batch("orders", 0)];

    let outcome = dispatcher
        .message_too_large_split_outcome(batches, "orders".to_owned(), 0)
        .await;

    assert!(matches!(
        outcome,
        super::DispatchOutcome::Delivered(Err(ProducerError::Broker {
            topic,
            partition: 0,
            error: ErrorCode::MessageTooLarge,
        })) if topic == "orders"
    ));
    assert_ratio_close(
        dispatcher.compression_ratio_estimation_for_test("orders", Compression::None),
        1.0,
    );
}

#[test]
fn message_too_large_split_uses_compression_ratio_estimation_for_split_groups() {
    let now = Instant::now();
    let accumulator = SharedAccumulator::with_config(
        AccumulatorConfig::default()
            .batch_size(TEST_LARGE_BATCH_SIZE)
            .buffer_memory(TEST_LARGE_BATCH_SIZE * 4)
            .linger(Duration::from_secs(1)),
    );
    for value in [b"a".as_slice(), b"b".as_slice(), b"c".as_slice()] {
        accumulator
            .append_at(
                ProducerRecord::new("orders", 0).value(Bytes::copy_from_slice(value)),
                now,
            )
            .expect("append record");
    }
    let batches = accumulator.drain_all();

    let split = super::split_message_too_large_batches(batches, "orders", 0, 78, 2.0)
        .expect("multi-record batch should split");

    assert_eq!(split.len(), 3);
    assert!(split.iter().all(|batch| batch.records.len() == 1));
}

fn assert_ratio_close(actual: f32, expected: f32) {
    assert!(
        (actual - expected).abs() <= 0.000_001,
        "expected {expected}, got {actual}"
    );
}

#[test]
fn dispatcher_record_batch_encoding_uses_wire_write_buffer_pool() {
    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default().buffer_pool_capacity(1),
        "dispatcher-test",
        [BrokerEndpoint::new(
            7,
            "127.0.0.1:9092".parse().expect("valid socket address"),
        )],
    );
    let dispatcher = ProducerDispatcher::new(wire.clone());
    let batch = ready_batch("orders", 0);

    let encoded = dispatcher
        .encode_ready_batch_records(&batch, 0)
        .expect("ready batch should encode");

    assert!(!encoded.is_empty());
    let stats = wire.buffer_pool_stats();
    assert_eq!(stats.write_acquired, 1);
    assert_eq!(stats.write_released, 1);
}

#[test]
fn broker_produce_request_releases_unique_record_bytes_to_wire_pool() {
    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default().buffer_pool_capacity(2),
        "dispatcher-test",
        [BrokerEndpoint::new(
            7,
            "127.0.0.1:9092".parse().expect("valid socket address"),
        )],
    );
    let dispatcher = ProducerDispatcher::new(wire.clone());
    let records = dispatcher
        .encode_ready_batch_records_owned(&ready_batch("orders", 0), 0)
        .expect("ready batch should encode");
    let mut request = BrokerProduceRequest::with_record_buffer_owner(&wire);
    request.push(
        route("orders", 0),
        records,
        BrokerProduceOptions {
            acks: 1,
            timeout_ms: 1_500,
            transactional_id: None,
        },
        1,
    );
    let before_release = wire.buffer_pool_stats();

    assert_eq!(request.release_record_buffers(&wire).released, 1);

    let after_release = wire.buffer_pool_stats();
    assert_eq!(
        after_release.write_released,
        before_release.write_released + 1
    );
    assert!(
        request.data.topic_data[0].partition_data[0]
            .records
            .is_none()
    );
}

#[test]
fn owned_broker_produce_request_drop_releases_unique_record_bytes_to_wire_pool() {
    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default().buffer_pool_capacity(2),
        "dispatcher-test",
        [BrokerEndpoint::new(
            7,
            "127.0.0.1:9092".parse().expect("valid socket address"),
        )],
    );
    let dispatcher = ProducerDispatcher::new(wire.clone());
    let records = dispatcher
        .encode_ready_batch_records_owned(&ready_batch("orders", 0), 0)
        .expect("ready batch should encode");
    let before_drop = wire.buffer_pool_stats();

    {
        let mut request = BrokerProduceRequest::with_record_buffer_owner(&wire);
        request.push(
            route("orders", 0),
            records,
            BrokerProduceOptions {
                acks: 1,
                timeout_ms: 1_500,
                transactional_id: None,
            },
            1,
        );
    }

    let after_drop = wire.buffer_pool_stats();
    assert_eq!(after_drop.write_released, before_drop.write_released + 1);
}

#[test]
fn owned_record_buffer_releases_when_last_request_clone_drops() {
    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default().buffer_pool_capacity(2),
        "dispatcher-test",
        [BrokerEndpoint::new(
            7,
            "127.0.0.1:9092".parse().expect("valid socket address"),
        )],
    );
    let dispatcher = ProducerDispatcher::new(wire.clone());
    let records = dispatcher
        .encode_ready_batch_records_owned(&ready_batch("orders", 0), 0)
        .expect("ready batch should encode");
    let cloned_records = {
        let mut request = BrokerProduceRequest::with_record_buffer_owner(&wire);
        request.push(
            route("orders", 0),
            records,
            BrokerProduceOptions {
                acks: 1,
                timeout_ms: 1_500,
                transactional_id: None,
            },
            1,
        );
        let records = request.data.topic_data[0].partition_data[0]
            .records
            .as_ref()
            .expect("records should be present")
            .clone();
        let before_drop = wire.buffer_pool_stats();

        drop(request);

        let after_request_drop = wire.buffer_pool_stats();
        assert_eq!(
            after_request_drop.write_released,
            before_drop.write_released
        );
        records
    };

    let before_clone_drop = wire.buffer_pool_stats();
    drop(cloned_records);

    let after_clone_drop = wire.buffer_pool_stats();
    assert_eq!(
        after_clone_drop.write_released,
        before_clone_drop.write_released + 1
    );
}

#[test]
fn broker_produce_request_reports_unrecovered_record_buffer_when_clone_is_alive() {
    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default().buffer_pool_capacity(2),
        "dispatcher-test",
        [BrokerEndpoint::new(
            7,
            "127.0.0.1:9092".parse().expect("valid socket address"),
        )],
    );
    let dispatcher = ProducerDispatcher::new(wire.clone());
    let records = dispatcher
        .encode_ready_batch_records(&ready_batch("orders", 0), 0)
        .expect("ready batch should encode");
    let encoded_bytes = records.len();
    let mut request = BrokerProduceRequest::default();
    request.push(
        route("orders", 0),
        records,
        BrokerProduceOptions {
            acks: 1,
            timeout_ms: 1_500,
            transactional_id: None,
        },
        1,
    );
    let cloned_request_data = request.data.clone();
    let before_release = wire.buffer_pool_stats();

    let release = request.release_record_buffers(&wire);

    assert_eq!(
        release,
        RecordBufferRelease {
            expected: 1,
            released: 0,
            unrecovered: 1,
            expected_bytes: encoded_bytes,
            released_bytes: 0,
            unrecovered_bytes: encoded_bytes,
        }
    );
    let after_first_release = wire.buffer_pool_stats();
    assert_eq!(
        after_first_release.write_released,
        before_release.write_released
    );
    assert!(
        request.data.topic_data[0].partition_data[0]
            .records
            .is_some()
    );

    drop(cloned_request_data);
    let release = request.release_record_buffers(&wire);

    assert_eq!(
        release,
        RecordBufferRelease {
            expected: 1,
            released: 1,
            unrecovered: 0,
            expected_bytes: encoded_bytes,
            released_bytes: encoded_bytes,
            unrecovered_bytes: 0,
        }
    );
    let after_second_release = wire.buffer_pool_stats();
    assert_eq!(
        after_second_release.write_released,
        after_first_release.write_released + 1
    );
    assert!(
        request.data.topic_data[0].partition_data[0]
            .records
            .is_none()
    );
}

#[test]
fn broker_produce_request_reports_unrecovered_lease_when_records_are_missing() {
    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default().buffer_pool_capacity(2),
        "dispatcher-test",
        [BrokerEndpoint::new(
            7,
            "127.0.0.1:9092".parse().expect("valid socket address"),
        )],
    );
    let dispatcher = ProducerDispatcher::new(wire.clone());
    let records = dispatcher
        .encode_ready_batch_records(&ready_batch("orders", 0), 0)
        .expect("ready batch should encode");
    let encoded_bytes = records.len();
    let mut request = BrokerProduceRequest::default();
    request.push(
        route("orders", 0),
        records,
        BrokerProduceOptions {
            acks: 1,
            timeout_ms: 1_500,
            transactional_id: None,
        },
        1,
    );
    let _lost_records = request.data.topic_data[0].partition_data[0]
        .records
        .take()
        .expect("test should remove encoded records");
    let before_release = wire.buffer_pool_stats();

    let release = request.release_record_buffers(&wire);

    assert_eq!(
        release,
        RecordBufferRelease {
            expected: 1,
            released: 0,
            unrecovered: 1,
            expected_bytes: encoded_bytes,
            released_bytes: 0,
            unrecovered_bytes: encoded_bytes,
        }
    );
    let after_release = wire.buffer_pool_stats();
    assert_eq!(after_release.write_released, before_release.write_released);
}

#[test]
fn broker_produce_request_reports_unrecovered_lease_bytes_when_records_are_missing() {
    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default().buffer_pool_capacity(2),
        "dispatcher-test",
        [BrokerEndpoint::new(
            7,
            "127.0.0.1:9092".parse().expect("valid socket address"),
        )],
    );
    let dispatcher = ProducerDispatcher::new(wire.clone());
    let records = dispatcher
        .encode_ready_batch_records(&ready_batch("orders", 0), 0)
        .expect("ready batch should encode");
    let encoded_bytes = records.len();
    let mut request = BrokerProduceRequest::default();
    request.push(
        route("orders", 0),
        records,
        BrokerProduceOptions {
            acks: 1,
            timeout_ms: 1_500,
            transactional_id: None,
        },
        1,
    );
    let _lost_records = request.data.topic_data[0].partition_data[0]
        .records
        .take()
        .expect("test should remove encoded records");

    let release = request.release_record_buffers(&wire);

    assert_eq!(
        release,
        RecordBufferRelease {
            expected: 1,
            released: 0,
            unrecovered: 1,
            expected_bytes: encoded_bytes,
            released_bytes: 0,
            unrecovered_bytes: encoded_bytes,
        }
    );
}

#[test]
fn broker_produce_request_matches_partial_release_bytes_to_partition_lease() {
    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default().buffer_pool_capacity(2),
        "dispatcher-test",
        [BrokerEndpoint::new(
            7,
            "127.0.0.1:9092".parse().expect("valid socket address"),
        )],
    );
    let dispatcher = ProducerDispatcher::new(wire.clone());
    let first_records = dispatcher
        .encode_ready_batch_records(&ready_batch_with_value("orders", 0, b"a"), 0)
        .expect("first ready batch should encode");
    let first_bytes = first_records.len();
    let second_records = dispatcher
        .encode_ready_batch_records(
            &ready_batch_with_value("orders", 1, b"second-value-is-longer"),
            0,
        )
        .expect("second ready batch should encode");
    let second_bytes = second_records.len();
    assert_ne!(first_bytes, second_bytes);
    let mut request = BrokerProduceRequest::default();
    request.push(
        route("orders", 0),
        first_records,
        BrokerProduceOptions {
            acks: 1,
            timeout_ms: 1_500,
            transactional_id: None,
        },
        1,
    );
    request.push(
        route("orders", 1),
        second_records,
        BrokerProduceOptions {
            acks: 1,
            timeout_ms: 1_500,
            transactional_id: None,
        },
        1,
    );
    let _first_records_clone = request.data.topic_data[0].partition_data[0]
        .records
        .as_ref()
        .expect("first records")
        .clone();

    let release = request.release_record_buffers(&wire);

    assert_eq!(
        release,
        RecordBufferRelease {
            expected: 2,
            released: 1,
            unrecovered: 1,
            expected_bytes: first_bytes + second_bytes,
            released_bytes: second_bytes,
            unrecovered_bytes: first_bytes,
        }
    );
    assert!(
        request.data.topic_data[0].partition_data[0]
            .records
            .is_some()
    );
    assert!(
        request.data.topic_data[0].partition_data[1]
            .records
            .is_none()
    );
}

#[test]
fn broker_produce_request_reports_all_leases_when_same_partition_records_are_merged() {
    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default().buffer_pool_capacity(2),
        "dispatcher-test",
        [BrokerEndpoint::new(
            7,
            "127.0.0.1:9092".parse().expect("valid socket address"),
        )],
    );
    let dispatcher = ProducerDispatcher::new(wire.clone());
    let first_records = dispatcher
        .encode_ready_batch_records(&ready_batch_with_value("orders", 0, b"a"), 0)
        .expect("first ready batch should encode");
    let first_bytes = first_records.len();
    let second_records = dispatcher
        .encode_ready_batch_records(
            &ready_batch_with_value("orders", 0, b"second-value-is-longer"),
            0,
        )
        .expect("second ready batch should encode");
    let second_bytes = second_records.len();
    assert_ne!(first_bytes, second_bytes);
    let mut request = BrokerProduceRequest::default();
    request.push(
        route("orders", 0),
        first_records,
        BrokerProduceOptions {
            acks: 1,
            timeout_ms: 1_500,
            transactional_id: None,
        },
        1,
    );
    request.push(
        route("orders", 0),
        second_records,
        BrokerProduceOptions {
            acks: 1,
            timeout_ms: 1_500,
            transactional_id: None,
        },
        1,
    );
    assert_eq!(request.data.topic_data[0].partition_data.len(), 1);
    let before_release = wire.buffer_pool_stats();

    let release = request.release_record_buffers(&wire);

    assert_eq!(
        release,
        RecordBufferRelease {
            expected: 2,
            released: 2,
            unrecovered: 0,
            expected_bytes: first_bytes + second_bytes,
            released_bytes: first_bytes + second_bytes,
            unrecovered_bytes: 0,
        }
    );
    let after_release = wire.buffer_pool_stats();
    assert_eq!(
        after_release.write_released,
        before_release.write_released + 1
    );
    assert!(
        request.data.topic_data[0].partition_data[0]
            .records
            .is_none()
    );
}

#[test]
fn owned_same_partition_merge_keeps_combined_records_in_write_pool() {
    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default().buffer_pool_capacity(4),
        "dispatcher-test",
        [BrokerEndpoint::new(
            7,
            "127.0.0.1:9092".parse().expect("valid socket address"),
        )],
    );
    let dispatcher = ProducerDispatcher::new(wire.clone());
    let first_records = dispatcher
        .encode_ready_batch_records_owned(&ready_batch_with_value("orders", 0, b"a"), 0)
        .expect("first ready batch should encode");
    let second_records = dispatcher
        .encode_ready_batch_records_owned(
            &ready_batch_with_value("orders", 0, b"second-value-is-longer"),
            0,
        )
        .expect("second ready batch should encode");
    let before_merge = wire.buffer_pool_stats();

    {
        let mut request = BrokerProduceRequest::with_record_buffer_owner(&wire);
        request.push(
            route("orders", 0),
            first_records,
            BrokerProduceOptions {
                acks: 1,
                timeout_ms: 1_500,
                transactional_id: None,
            },
            1,
        );
        request.push(
            route("orders", 0),
            second_records,
            BrokerProduceOptions {
                acks: 1,
                timeout_ms: 1_500,
                transactional_id: None,
            },
            1,
        );
        assert_eq!(request.data.topic_data[0].partition_data.len(), 1);
        let after_merge = wire.buffer_pool_stats();
        assert_eq!(after_merge.write_acquired, before_merge.write_acquired + 1);
        assert_eq!(after_merge.write_released, before_merge.write_released + 2);
    }

    let after_drop = wire.buffer_pool_stats();
    assert_eq!(after_drop.write_acquired, before_merge.write_acquired + 1);
    assert_eq!(after_drop.write_released, before_merge.write_released + 3);
}

#[test]
fn broker_dispatch_result_releases_record_bytes_before_wire_error() {
    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default().buffer_pool_capacity(2),
        "dispatcher-test",
        [BrokerEndpoint::new(
            7,
            "127.0.0.1:9092".parse().expect("valid socket address"),
        )],
    );
    let dispatcher = ProducerDispatcher::new(wire.clone());
    let records = dispatcher
        .encode_ready_batch_records(&ready_batch("orders", 0), 0)
        .expect("ready batch should encode");
    let mut request = BrokerProduceRequest::default();
    request.push(
        route("orders", 0),
        records,
        BrokerProduceOptions {
            acks: 1,
            timeout_ms: 1_500,
            transactional_id: None,
        },
        1,
    );
    let before_error = wire.buffer_pool_stats();

    let result = broker_dispatch_completed_result(
        &wire,
        0,
        request,
        Err(crate::wire::WireError::ConnectionClosed),
    );

    assert!(matches!(
        result,
        Err(DispatchError::RetryableWire(ProducerError::Wire(
            crate::wire::WireError::ConnectionClosed
        )))
    ));
    let after_error = wire.buffer_pool_stats();
    assert_eq!(after_error.write_released, before_error.write_released + 1);
}

#[tokio::test]
async fn broker_dispatch_releases_record_bytes_when_metrics_sizing_fails() {
    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default().buffer_pool_capacity(2),
        "dispatcher-test",
        [BrokerEndpoint::new(
            7,
            "127.0.0.1:9092".parse().expect("valid socket address"),
        )],
    );
    let dispatcher = ProducerDispatcher::new(wire.clone());
    dispatcher.enable_metrics();
    let records = dispatcher
        .encode_ready_batch_records(&ready_batch("orders", 0), 0)
        .expect("ready batch should encode");
    let mut request = BrokerProduceRequest::default();
    request.push(
        route("orders", 0),
        records,
        BrokerProduceOptions {
            acks: 1,
            timeout_ms: 1_500,
            transactional_id: None,
        },
        1,
    );
    let before_error = wire.buffer_pool_stats();

    let result = dispatcher
        .dispatch_broker_requests(7, vec![request], i16::MAX, 0)
        .await;

    assert!(matches!(
        result,
        Err(DispatchError::Producer(ProducerError::Wire(_)))
    ));
    let after_error = wire.buffer_pool_stats();
    assert_eq!(after_error.write_released, before_error.write_released + 1);
}

#[tokio::test]
async fn broker_dispatch_backpressure_retry_is_bounded_and_releases_record_bytes() {
    let version = client_api_info(ApiKey::Produce).max_version;
    let wire = WireClient::connect_with_brokers(
        ConnectionConfig::default()
            .broker_queue_capacity(1)
            .request_timeout(Duration::from_secs(30))
            .reconnect_backoff_initial(Duration::from_secs(30))
            .reconnect_backoff_max(Duration::from_secs(30))
            .buffer_pool_capacity(2),
        "dispatcher-test",
        [BrokerEndpoint::new(
            7,
            "127.0.0.1:9".parse().expect("valid socket address"),
        )],
    );
    let filler = ProduceRequestData::default();
    let _first = wire
        .enqueue_to_broker::<_, kacrab_protocol::generated::ProduceResponseData>(
            7,
            ApiKey::Produce,
            version,
            &filler,
        )
        .expect("first command should create broker handle");
    tokio::task::yield_now().await;
    let _second = wire.enqueue_to_broker::<_, kacrab_protocol::generated::ProduceResponseData>(
        7,
        ApiKey::Produce,
        version,
        &filler,
    );
    let dispatcher =
        ProducerDispatcher::new(wire.clone()).delivery_timeout(Duration::from_millis(5));
    let records = dispatcher
        .encode_ready_batch_records(&ready_batch("orders", 0), 0)
        .expect("ready batch should encode");
    let mut request = BrokerProduceRequest::default();
    request.push(
        route("orders", 0),
        records,
        BrokerProduceOptions {
            acks: 1,
            timeout_ms: 1_500,
            transactional_id: None,
        },
        1,
    );
    let before_dispatch = wire.buffer_pool_stats();

    let result = tokio::time::timeout(
        Duration::from_millis(100),
        dispatcher.dispatch_broker_requests(7, vec![request], version, 0),
    )
    .await
    .expect("backpressure retry should be bounded by delivery timeout");

    assert!(matches!(
        result,
        Err(DispatchError::Producer(ProducerError::Wire(
            crate::wire::WireError::Backpressure
        )))
    ));
    let after_dispatch = wire.buffer_pool_stats();
    assert_eq!(
        after_dispatch.write_released,
        before_dispatch.write_released + 1
    );
}

#[tokio::test]
async fn retry_wait_returns_delivery_timeout_when_outer_deadline_expires() {
    let dispatcher =
        ProducerDispatcher::new(test_wire()).delivery_timeout(Duration::from_millis(1));
    let accumulator = SharedAccumulator::with_config(
        AccumulatorConfig::default()
            .batch_size(1)
            .buffer_memory(16 * 1024),
    );
    let now = Instant::now();
    accumulator
        .append_at(
            ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")),
            now,
        )
        .expect("append record");
    let batches = accumulator.drain_ready(now);
    let mut retry_backoff = dispatcher.retry_backoff_state();

    let error = dispatcher
        .wait_before_retry(&batches, &mut retry_backoff, false)
        .await
        .expect("retry wait should expire delivery timeout");

    assert!(matches!(
        error,
        ProducerError::DeliveryTimeout { topic, partition }
            if topic == "orders" && partition == 0
    ));
}

fn ready_batch(topic: &str, partition: i32) -> super::ReadyBatch {
    ready_batch_with_value(topic, partition, b"value")
}

fn ready_batch_with_value(topic: &str, partition: i32, value: &'static [u8]) -> super::ReadyBatch {
    let accumulator = SharedAccumulator::with_config(
        AccumulatorConfig::default()
            .batch_size(1)
            .linger(Duration::from_secs(1)),
    );
    accumulator
        .append_at(
            ProducerRecord::new(topic, partition).value(Bytes::from_static(value)),
            Instant::now(),
        )
        .expect("append test record");
    accumulator
        .drain_ready(Instant::now())
        .pop()
        .expect("ready batch")
}

#[tokio::test]
async fn retryable_broker_error_fails_definitively_when_attempts_exhausted_like_java() {
    // Kafka Sender.canRetry stops retrying once attempts are exhausted: the
    // generic retriable broker error becomes a definite Broker failure.
    let dispatcher = ProducerDispatcher::new(test_wire());
    let batches = vec![ready_batch("orders", 0)];
    let mut retry_backoff = dispatcher.retry_backoff_state();
    let mut attempts_remaining = 0;

    let error = dispatcher
        .handle_retryable_broker_retry(
            &batches,
            &mut attempts_remaining,
            &mut retry_backoff,
            "orders",
            0,
            ErrorCode::NotEnoughReplicas,
        )
        .await
        .expect("exhausted attempts fail definitively");

    assert!(matches!(
        error,
        ProducerError::Broker {
            error: ErrorCode::NotEnoughReplicas,
            ..
        }
    ));
}

#[tokio::test]
async fn retryable_broker_error_retries_while_attempts_remain_like_java() {
    // With attempts left and the delivery timeout not reached, a retriable
    // broker error is retried (no error returned) and the attempt budget
    // is decremented.
    let dispatcher = ProducerDispatcher::new(test_wire());
    let batches = vec![ready_batch("orders", 0)];
    let mut retry_backoff = dispatcher.retry_backoff_state();
    let mut attempts_remaining = 3;

    let outcome = dispatcher
        .handle_retryable_broker_retry(
            &batches,
            &mut attempts_remaining,
            &mut retry_backoff,
            "orders",
            0,
            ErrorCode::NotEnoughReplicas,
        )
        .await;

    assert!(outcome.is_none());
    assert_eq!(attempts_remaining, 2);
}

#[tokio::test]
async fn leader_change_retry_skips_backoff_but_honors_delivery_timeout() {
    // Leader changed and delivery timeout not reached -> retry immediately
    // (no backoff sleep, returns None).
    let dispatcher = ProducerDispatcher::new(test_wire());
    let batches = vec![ready_batch("orders", 0)];
    assert!(
        dispatcher
            .check_delivery_timeout_before_retry(&batches, false)
            .await
            .is_none()
    );

    // Delivery timeout already elapsed -> fail even on a leader change.
    let expired = ProducerDispatcher::new(test_wire()).delivery_timeout(Duration::ZERO);
    let batches = vec![ready_batch("orders", 0)];
    assert!(matches!(
        expired.check_delivery_timeout_before_retry(&batches, false).await,
        Some(ProducerError::DeliveryTimeout { topic, partition })
            if topic == "orders" && partition == 0
    ));
}

#[tokio::test]
async fn begin_transaction_rejects_invalid_state_transitions() {
    let non_transactional = ProducerDispatcher::new(test_wire());
    assert!(matches!(
        non_transactional.begin_transaction(),
        Err(ProducerError::TransactionalIdRequired)
    ));

    let dispatcher = transactional_dispatcher();
    assert!(matches!(
        dispatcher.begin_transaction(),
        Err(ProducerError::InvalidTransactionState(message))
            if message == "init_transactions must run before begin_transaction"
    ));

    {
        let mut state = dispatcher.producer_state.lock().await;
        state.identity = Some(ProducerIdentity {
            producer_id: 11,
            producer_epoch: 2,
        });
    }
    dispatcher
        .begin_transaction()
        .expect("identity allows opening transaction");
    assert!(matches!(
        dispatcher.begin_transaction(),
        Err(ProducerError::InvalidTransactionState(message))
            if message == "transaction is already open"
    ));
}

#[tokio::test]
async fn begin_transaction_reports_transaction_error_before_missing_identity_like_java() {
    let dispatcher = transactional_dispatcher();
    {
        let mut state = dispatcher.producer_state.lock().await;
        state.fatal_error = Some(ErrorCode::ProducerFenced);
    }

    assert!(matches!(
        dispatcher.begin_transaction(),
        Err(ProducerError::Transaction {
            operation: "transaction_state",
            error: ErrorCode::ProducerFenced
        })
    ));
}

#[tokio::test]
async fn begin_transaction_reports_pending_operation_before_transaction_error_like_java() {
    let dispatcher = transactional_dispatcher();
    {
        let mut state = dispatcher.producer_state.lock().await;
        state.fatal_error = Some(ErrorCode::ProducerFenced);
        state.pending_operation = Some(TransactionOperation::SendOffsetsToTransaction);
    }

    assert!(matches!(
        dispatcher.begin_transaction(),
        Err(ProducerError::InvalidTransactionState(
            "previous transaction operation is pending and must be retried"
        ))
    ));
}

#[tokio::test]
async fn init_transactions_rejects_repeated_initialization_like_java() {
    let dispatcher = transactional_dispatcher();
    {
        let mut state = dispatcher.producer_state.lock().await;
        state.identity = Some(ProducerIdentity {
            producer_id: 11,
            producer_epoch: 2,
        });
        state.coordinator_id = Some(7);
    }

    assert!(matches!(
        dispatcher.init_transactions().await,
        Err(ProducerError::InvalidTransactionState(
            "init_transactions must only run once"
        ))
    ));
}

#[tokio::test]
async fn transactional_produce_error_escalates_to_fatal_when_coordinator_cannot_bump_epoch() {
    use super::super::transaction::TransactionState;
    // A bump-capable coordinator keeps UnknownProducerId abortable and
    // requests a client-side epoch bump (Kafka needToTriggerEpochBumpFromClient).
    let can_bump = transactional_dispatcher();
    {
        let mut state = can_bump.producer_state.lock().await;
        state.in_transaction = true;
        // Default coordinator_lacks_epoch_bump_support == false (assume supported).
    }
    can_bump
        .record_transactional_produce_error(ErrorCode::UnknownProducerId, "orders", 0)
        .await;
    {
        let state = can_bump.producer_state.lock().await;
        assert_eq!(state.transaction_state, TransactionState::AbortableError);
        assert!(state.epoch_bump_required);
    }

    // A coordinator that cannot bump the epoch (InitProducerId < v3) turns the
    // same error fatal (Kafka canHandleAbortableError() == false).
    let cannot_bump = transactional_dispatcher();
    {
        let mut state = cannot_bump.producer_state.lock().await;
        state.in_transaction = true;
        state.coordinator_lacks_epoch_bump_support = true;
    }
    cannot_bump
        .record_transactional_produce_error(ErrorCode::UnknownProducerId, "orders", 0)
        .await;
    {
        let state = cannot_bump.producer_state.lock().await;
        assert_eq!(state.transaction_state, TransactionState::FatalError);
        assert_eq!(state.fatal_error, Some(ErrorCode::UnknownProducerId));
        assert!(!state.epoch_bump_required);
        assert!(state.abortable_error.is_none());
    }
}

#[tokio::test]
async fn transaction_control_error_escalates_to_fatal_when_coordinator_cannot_bump_epoch() {
    use super::super::transaction::TransactionState;
    // Control-plane UnknownProducerId stays abortable + epoch bump when the
    // coordinator supports bumping, and goes fatal when it does not.
    let can_bump = transactional_dispatcher();
    {
        let mut state = can_bump.producer_state.lock().await;
        state.in_transaction = true;
    }
    can_bump
        .record_transaction_error(ErrorCode::UnknownProducerId)
        .await;
    {
        let state = can_bump.producer_state.lock().await;
        assert_eq!(state.transaction_state, TransactionState::AbortableError);
        assert!(state.epoch_bump_required);
    }

    let cannot_bump = transactional_dispatcher();
    {
        let mut state = cannot_bump.producer_state.lock().await;
        state.in_transaction = true;
        state.coordinator_lacks_epoch_bump_support = true;
    }
    cannot_bump
        .record_transaction_error(ErrorCode::UnknownProducerId)
        .await;
    {
        let state = cannot_bump.producer_state.lock().await;
        assert_eq!(state.transaction_state, TransactionState::FatalError);
        assert_eq!(state.fatal_error, Some(ErrorCode::UnknownProducerId));
        assert!(!state.epoch_bump_required);
        assert!(state.abortable_error.is_none());
    }
}

#[tokio::test]
async fn send_offsets_to_transaction_reports_busy_when_transaction_state_is_locked_like_java() {
    let dispatcher = transactional_dispatcher();
    {
        let mut state = dispatcher.producer_state.lock().await;
        state.identity = Some(ProducerIdentity {
            producer_id: 11,
            producer_epoch: 2,
        });
        state.in_transaction = true;
    }
    let locked_dispatcher = dispatcher.clone();
    let _state_guard = locked_dispatcher.producer_state.lock().await;

    let result = tokio::time::timeout(
        Duration::from_millis(10),
        dispatcher.send_offsets_to_transaction(
            [(
                crate::producer::TopicPartition::new("orders", 0),
                crate::producer::OffsetAndMetadata::new(7),
            )],
            ConsumerGroupMetadata::new("group-a"),
        ),
    )
    .await;

    assert!(matches!(
        result,
        Ok(Err(ProducerError::TransactionStateBusy))
    ));
}

#[tokio::test]
async fn transaction_operations_reject_previous_pending_operation_like_java() {
    let dispatcher = transactional_dispatcher();
    {
        let mut state = dispatcher.producer_state.lock().await;
        state.identity = Some(ProducerIdentity {
            producer_id: 11,
            producer_epoch: 2,
        });
        state.in_transaction = true;
        state.transaction_started = true;
        state.pending_operation = Some(TransactionOperation::SendOffsetsToTransaction);
    }

    assert!(matches!(
        dispatcher.begin_transaction(),
        Err(ProducerError::InvalidTransactionState(
            "previous transaction operation is pending and must be retried"
        ))
    ));
    assert!(matches!(
        dispatcher
            .send_offsets_to_transaction(
                [(
                    crate::producer::TopicPartition::new("orders", 0),
                    crate::producer::OffsetAndMetadata::new(7),
                )],
                ConsumerGroupMetadata::new("group-a"),
            )
            .await,
        Err(ProducerError::InvalidTransactionState(
            "previous transaction operation is pending and must be retried"
        ))
    ));
    assert!(matches!(
        dispatcher.end_transaction(false).await,
        Err(ProducerError::InvalidTransactionState(
            "previous transaction operation is pending and must be retried"
        ))
    ));
}

#[tokio::test]
async fn dropped_pending_transaction_operation_clears_state_for_retry_like_java() {
    let dispatcher = transactional_dispatcher();
    let mut state = dispatcher.producer_state.lock().await;
    let TransactionPendingOperationStart::Started(result) = state
        .begin_pending_transaction_operation(TransactionOperation::SendOffsetsToTransaction)
        .expect("operation should start")
    else {
        panic!("operation should not be cached");
    };
    drop(state);
    {
        let state = dispatcher.producer_state.lock().await;
        assert!(state.pending_operation.is_some());
        drop(state);
    }

    drop(PendingTransactionOperationGuard::new(
        Arc::clone(&dispatcher.producer_state),
        TransactionOperation::SendOffsetsToTransaction,
        result,
    ));

    tokio::time::timeout(Duration::from_millis(100), async {
        loop {
            let pending_operation = {
                let state = dispatcher.producer_state.lock().await;
                state.pending_operation
            };
            if pending_operation.is_none() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("dropped pending operation should clear promptly");
    let mut state = dispatcher.producer_state.lock().await;
    fail_pending_transaction_operation(&mut state).expect("retry should not be blocked");
}

#[tokio::test]
async fn same_pending_transaction_operation_reuses_cached_result_like_java() {
    let mut state = ProducerIdempotenceState::default();
    let TransactionPendingOperationStart::Started(first_result) = state
        .begin_pending_transaction_operation(TransactionOperation::EndTransaction {
            committed: true,
        })
        .expect("first commit operation should start")
    else {
        panic!("first operation should start");
    };

    let TransactionPendingOperationStart::Cached(second_result) = state
        .begin_pending_transaction_operation(TransactionOperation::EndTransaction {
            committed: true,
        })
        .expect("same commit operation should reuse cached result")
    else {
        panic!("same operation should return cached result");
    };

    assert!(first_result.is_same_handle(&second_result));
    assert!(!second_result.is_completed());
    first_result.done();
    second_result
        .wait()
        .await
        .expect("cached waiter should observe successful completion");
    assert!(second_result.is_successful());
}

#[test]
fn completed_unacked_pending_transaction_result_blocks_next_operation_like_java() {
    let mut state = ProducerIdempotenceState::default();
    let TransactionPendingOperationStart::Started(first_result) = state
        .begin_pending_transaction_operation(TransactionOperation::InitTransactions)
        .expect("first init operation should start")
    else {
        panic!("first operation should start");
    };
    first_result.done();

    assert!(matches!(
        state.begin_pending_transaction_operation(TransactionOperation::SendOffsetsToTransaction),
        Err(ProducerError::InvalidTransactionState(
            "previous transaction operation is pending and must be retried"
        ))
    ));
}

#[tokio::test]
async fn acked_pending_transaction_result_allows_next_operation_like_java() {
    let mut state = ProducerIdempotenceState::default();
    let TransactionPendingOperationStart::Started(first_result) = state
        .begin_pending_transaction_operation(TransactionOperation::InitTransactions)
        .expect("first init operation should start")
    else {
        panic!("first operation should start");
    };
    first_result.done();
    first_result
        .wait()
        .await
        .expect("awaiting the cached result should ack it like Kafka");

    let TransactionPendingOperationStart::Started(second_result) = state
        .begin_pending_transaction_operation(TransactionOperation::SendOffsetsToTransaction)
        .expect("acked pending operation should be cleared for the next operation")
    else {
        panic!("next operation should start instead of using a cached result");
    };

    assert!(!first_result.is_same_handle(&second_result));
    assert_eq!(
        state.pending_operation,
        Some(TransactionOperation::SendOffsetsToTransaction)
    );
    assert_eq!(
        state.pending_requests.pop_next(),
        Some(super::TransactionRequestKind::AddPartitionsOrOffsets)
    );
}

#[test]
fn completed_unacked_pending_transaction_result_blocks_non_cached_operation_like_java() {
    let mut state = ProducerIdempotenceState::default();
    let TransactionPendingOperationStart::Started(first_result) = state
        .begin_pending_transaction_operation(TransactionOperation::InitTransactions)
        .expect("first init operation should start")
    else {
        panic!("first operation should start");
    };
    first_result.done();

    assert!(matches!(
        fail_pending_transaction_operation(&mut state),
        Err(ProducerError::InvalidTransactionState(
            "previous transaction operation is pending and must be retried"
        ))
    ));
}

#[tokio::test]
async fn acked_pending_transaction_result_allows_non_cached_operation_like_java() {
    let mut state = ProducerIdempotenceState::default();
    let TransactionPendingOperationStart::Started(first_result) = state
        .begin_pending_transaction_operation(TransactionOperation::InitTransactions)
        .expect("first init operation should start")
    else {
        panic!("first operation should start");
    };
    first_result.done();
    first_result
        .wait()
        .await
        .expect("awaiting the cached result should ack it like Kafka");

    fail_pending_transaction_operation(&mut state)
        .expect("acked pending operation should be cleared");

    assert_eq!(state.pending_operation, None);
    assert!(state.pending_result.is_none());
    assert!(state.pending_requests.pop_next().is_none());
}

#[test]
fn different_pending_transaction_operation_still_reports_busy_like_java() {
    let mut state = ProducerIdempotenceState::default();
    let _started = state
        .begin_pending_transaction_operation(TransactionOperation::EndTransaction {
            committed: true,
        })
        .expect("first commit operation should start");

    assert!(matches!(
        state.begin_pending_transaction_operation(TransactionOperation::EndTransaction {
            committed: false,
        }),
        Err(ProducerError::InvalidTransactionState(
            "previous transaction operation is pending and must be retried"
        ))
    ));
}

#[tokio::test]
async fn end_transaction_requires_transactional_id_before_network_io() {
    let dispatcher = ProducerDispatcher::new(test_wire());

    let error = dispatcher
        .end_transaction(true)
        .await
        .expect_err("non-transactional dispatcher should fail locally");

    assert!(matches!(error, ProducerError::TransactionalIdRequired));
}

#[tokio::test]
async fn end_transaction_reports_busy_when_transaction_state_is_locked_like_java() {
    let dispatcher = transactional_dispatcher();
    {
        let mut state = dispatcher.producer_state.lock().await;
        state.identity = Some(ProducerIdentity {
            producer_id: 11,
            producer_epoch: 2,
        });
        state.in_transaction = true;
    }
    let locked_dispatcher = dispatcher.clone();
    let _state_guard = locked_dispatcher.producer_state.lock().await;

    let result =
        tokio::time::timeout(Duration::from_millis(10), dispatcher.end_transaction(false)).await;

    assert!(matches!(
        result,
        Ok(Err(ProducerError::TransactionStateBusy))
    ));
}

#[tokio::test]
async fn end_transaction_reports_transaction_error_before_network_io_like_java() {
    let dispatcher = ProducerDispatcher::with_config(
        WireClient::connect_with_brokers(ConnectionConfig::default(), "producer-test", []),
        ProducerRuntimeConfig {
            idempotence: ProducerIdempotenceConfig {
                enabled: true,
                transactional_id: Some("txn-a".to_owned()),
                transaction_timeout_ms: 30_000,
                transaction_two_phase_commit: false,
            },
            ..ProducerRuntimeConfig::default()
        },
    );
    {
        let mut state = dispatcher.producer_state.lock().await;
        state.fatal_error = Some(ErrorCode::ProducerFenced);
    }

    assert!(matches!(
        dispatcher.end_transaction(false).await,
        Err(ProducerError::Transaction {
            operation: "transaction_state",
            error: ErrorCode::ProducerFenced
        })
    ));
}

#[tokio::test]
async fn end_transaction_rejects_missing_open_transaction_after_cached_identity() {
    let dispatcher = transactional_dispatcher();
    {
        let mut state = dispatcher.producer_state.lock().await;
        state.coordinator_id = Some(7);
        state.identity = Some(ProducerIdentity {
            producer_id: 11,
            producer_epoch: 2,
        });
    }

    assert!(matches!(
        dispatcher.end_transaction(false).await,
        Err(ProducerError::InvalidTransactionState(message))
            if message == "no transaction is open"
    ));
}

#[tokio::test]
async fn end_transaction_skips_end_txn_for_empty_transaction_like_java() {
    for committed in [true, false] {
        let dispatcher = ProducerDispatcher::with_config(
            WireClient::connect_with_brokers(ConnectionConfig::default(), "producer-test", []),
            ProducerRuntimeConfig {
                idempotence: ProducerIdempotenceConfig {
                    enabled: true,
                    transactional_id: Some("txn-a".to_owned()),
                    transaction_timeout_ms: 30_000,
                    transaction_two_phase_commit: false,
                },
                ..ProducerRuntimeConfig::default()
            },
        );
        {
            let mut state = dispatcher.producer_state.lock().await;
            state.identity = Some(ProducerIdentity {
                producer_id: 11,
                producer_epoch: 2,
            });
            state.in_transaction = true;
        }

        dispatcher
            .end_transaction(committed)
            .await
            .expect("empty transaction should complete without EndTxn");

        let state = dispatcher.producer_state.lock().await;
        assert!(!state.in_transaction);
        drop(state);
    }
}

#[tokio::test]
async fn dispatch_entrypoints_return_immediately_for_empty_inputs() {
    let dispatcher = ProducerDispatcher::new(test_wire());
    let accumulator = SharedAccumulator::with_config(AccumulatorConfig::default());

    assert!(
        dispatcher
            .dispatch_ready(&accumulator, Instant::now())
            .await
            .expect("empty ready")
            .is_empty()
    );
    assert!(
        dispatcher
            .dispatch_ready_batches(Vec::new(), Instant::now())
            .await
            .expect("empty batches")
            .is_empty()
    );
    assert!(
        dispatcher
            .dispatch_all(&accumulator)
            .await
            .expect("empty all")
            .is_empty()
    );
}

#[tokio::test]
async fn dispatch_ready_returns_producer_error_when_no_broker_is_available() {
    let wire = WireClient::connect_with_brokers(ConnectionConfig::default(), "client-a", []);
    let dispatcher = ProducerDispatcher::new(wire);
    let accumulator = SharedAccumulator::with_config(
        AccumulatorConfig::default()
            .batch_size(1)
            .buffer_memory(16 * 1024),
    );
    accumulator
        .append_at(
            ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")),
            Instant::now(),
        )
        .expect("append record");

    assert!(matches!(
        dispatcher
            .dispatch_ready(&accumulator, Instant::now())
            .await,
        Err(ProducerError::Wire(
            crate::wire::WireError::NoBrokerAvailable
        ))
    ));
}

#[tokio::test]
async fn dispatch_drained_reports_local_delivery_timeout() {
    let dispatcher = ProducerDispatcher::new(test_wire()).delivery_timeout(Duration::ZERO);

    let outcome = dispatcher
        .dispatch_drained(vec![ready_batch("orders", 0)], Instant::now(), 0)
        .await;

    assert!(matches!(
        outcome,
        super::DispatchOutcome::Delivered(Err(ProducerError::DeliveryTimeout {
            topic,
            partition
        })) if topic == "orders" && partition == 0
    ));
}

#[tokio::test]
async fn add_partition_to_transaction_handles_local_state_branches() {
    let route = route("orders", 0);
    ProducerDispatcher::new(test_wire())
        .add_partition_to_transaction(&route)
        .await
        .expect("non-transactional producer skips transaction partition registration");

    let dispatcher = transactional_dispatcher();
    assert!(matches!(
        dispatcher.add_partition_to_transaction(&route).await,
        Err(ProducerError::InvalidTransactionState(message))
            if message == "produce called outside an open transaction"
    ));

    {
        let mut state = dispatcher.producer_state.lock().await;
        state.identity = Some(ProducerIdentity {
            producer_id: 11,
            producer_epoch: 2,
        });
        state.in_transaction = true;
        let _inserted = state.transaction_partitions.insert(TopicPartitionKey {
            topic: "orders".to_owned(),
            partition: 0,
        });
    }
    dispatcher
        .add_partition_to_transaction(&route)
        .await
        .expect("already registered partition is a no-op");
}

#[tokio::test]
async fn add_partition_to_transaction_requires_initialized_identity_like_java() {
    for transaction_two_phase_commit in [false, true] {
        let dispatcher = ProducerDispatcher::with_config(
            test_wire(),
            ProducerRuntimeConfig {
                idempotence: ProducerIdempotenceConfig {
                    enabled: true,
                    transactional_id: Some("txn-a".to_owned()),
                    transaction_timeout_ms: 30_000,
                    transaction_two_phase_commit,
                },
                ..ProducerRuntimeConfig::default()
            },
        );
        let route = route("orders", 0);
        {
            let mut state = dispatcher.producer_state.lock().await;
            state.in_transaction = true;
            if !transaction_two_phase_commit {
                let _inserted = state.transaction_partitions.insert(TopicPartitionKey {
                    topic: "orders".to_owned(),
                    partition: 0,
                });
            }
        }

        assert!(matches!(
            dispatcher.add_partition_to_transaction(&route).await,
            Err(ProducerError::InvalidTransactionState(message))
                if message == "init_transactions must run before produce"
        ));
    }
}

#[tokio::test]
async fn transactional_send_rejects_previous_pending_operation_like_java() {
    let dispatcher = transactional_dispatcher();
    {
        let mut state = dispatcher.producer_state.lock().await;
        state.identity = Some(ProducerIdentity {
            producer_id: 11,
            producer_epoch: 2,
        });
        state.in_transaction = true;
        state.pending_operation = Some(TransactionOperation::SendOffsetsToTransaction);
    }

    let route = route("orders", 0);
    let result = tokio::time::timeout(
        Duration::from_millis(10),
        dispatcher.add_partition_to_transaction(&route),
    )
    .await;

    assert!(matches!(
        result,
        Ok(Err(ProducerError::InvalidTransactionState(
            "previous transaction operation is pending and must be retried"
        )))
    ));
}

#[tokio::test]
async fn producer_batch_state_rejects_record_counts_above_i32() {
    let dispatcher = ProducerDispatcher::with_config(
        test_wire(),
        ProducerRuntimeConfig {
            idempotence: ProducerIdempotenceConfig {
                enabled: true,
                transactional_id: None,
                transaction_timeout_ms: 30_000,
                transaction_two_phase_commit: false,
            },
            ..ProducerRuntimeConfig::default()
        },
    );
    {
        let mut state = dispatcher.producer_state.lock().await;
        state.identity = Some(ProducerIdentity {
            producer_id: 11,
            producer_epoch: 2,
        });
    }
    let too_many_records = usize::try_from(i32::MAX)
        .expect("i32 max fits usize")
        .saturating_add(1);

    assert!(matches!(
        dispatcher
            .producer_batch_state(&route("orders", 0), too_many_records)
            .await,
        Err(ProducerError::SequenceOverflow { topic, partition })
            if topic == "orders" && partition == 0
    ));
}

#[test]
fn add_partitions_response_reports_top_level_and_partition_errors() {
    let route = route("orders", 0);
    let response = AddPartitionsToTxnResponseData {
        error_code: i16::from(ErrorCode::CoordinatorNotAvailable),
        ..AddPartitionsToTxnResponseData::default()
    };
    assert!(matches!(
        ProducerDispatcher::check_add_partitions_response(response, &route),
        Err(ProducerError::Transaction {
            operation: "add_partitions_to_txn",
            error: ErrorCode::CoordinatorNotAvailable
        })
    ));

    let response = add_partitions_response("orders", 0, ErrorCode::InvalidTxnState);
    assert!(matches!(
        ProducerDispatcher::check_add_partitions_response(response, &route),
        Err(ProducerError::Transaction {
            operation: "add_partitions_to_txn",
            error: ErrorCode::InvalidTxnState
        })
    ));
}

#[test]
fn add_partitions_response_ignores_unmatched_topics_and_partitions() {
    let route = route("orders", 0);

    ProducerDispatcher::check_add_partitions_response(
        add_partitions_response("payments", 0, ErrorCode::InvalidTxnState),
        &route,
    )
    .expect("unmatched topic is not this route's failure");
    ProducerDispatcher::check_add_partitions_response(
        add_partitions_response("orders", 1, ErrorCode::InvalidTxnState),
        &route,
    )
    .expect("unmatched partition is not this route's failure");
}

#[test]
fn idempotence_state_tracks_sequences_and_wraps_at_max_like_java() {
    let mut state = ProducerIdempotenceState::default();

    assert_eq!(
        state.next_sequence("orders", 0, 3).expect("first sequence"),
        0
    );
    assert_eq!(
        state
            .next_sequence("orders", 0, 2)
            .expect("second sequence"),
        3
    );

    // Kafka DefaultRecordBatch.incrementSequence wraps at i32::MAX rather than
    // overflowing: base i32::MAX with a 1-record batch returns i32::MAX and the
    // next base wraps to 0.
    let key = TopicPartitionKey {
        topic: "orders".to_owned(),
        partition: 0,
    };
    state
        .partitions
        .entry(key.clone())
        .or_default()
        .next_sequence = i32::MAX;
    assert_eq!(
        state
            .next_sequence("orders", 0, 1)
            .expect("wrapping base sequence"),
        i32::MAX
    );
    assert_eq!(state.partitions[&key].next_sequence, 0);
    // A 3-record batch straddling the boundary: base 2147483646 -> next wraps to 2.
    state
        .partitions
        .entry(key.clone())
        .or_default()
        .next_sequence = i32::MAX - 1;
    assert_eq!(
        state
            .next_sequence("orders", 0, 3)
            .expect("straddling base sequence"),
        i32::MAX - 1
    );
    assert_eq!(state.partitions[&key].next_sequence, 1);
}

#[test]
fn idempotence_state_blocks_new_sequences_for_unresolved_partition_like_java() {
    let mut state = ProducerIdempotenceState::default();
    assert_eq!(
        state
            .next_sequence("orders", 0, 3)
            .expect("initial sequence"),
        0
    );

    state.mark_sequence_unresolved("orders", 0, 0, 3);

    assert!(matches!(
        state.next_sequence("orders", 0, 1),
        Err(ProducerError::UnresolvedSequence { topic, partition })
            if topic == "orders" && partition == 0
    ));
    assert_eq!(
        state
            .next_sequence("orders", 1, 1)
            .expect("other partition remains usable"),
        0
    );

    state.reset_sequence("orders", 0);
    assert_eq!(
        state
            .next_sequence("orders", 0, 1)
            .expect("reset clears unresolved sequence"),
        0
    );
}

#[test]
fn resolve_unresolved_after_drain_bumps_epoch_for_idempotent_like_java() {
    // Idempotent producer: unacked messages after drain request an epoch bump.
    let mut state = ProducerIdempotenceState::default();
    let _ = state.next_sequence("orders", 0, 3).expect("sequence");
    state.mark_sequence_unresolved("orders", 0, 0, 3);

    state.resolve_unresolved_sequence_after_drain("orders", 0, false, true);

    assert!(state.epoch_bump_required);
    // Marker cleared, so the partition no longer blocks (epoch bump resets it).
    assert_eq!(state.next_sequence("orders", 0, 1).expect("unblocked"), 3);
}

#[test]
fn resolve_defers_until_partition_has_no_inflight_batches_like_java() {
    // Kafka maybeResolveSequences only resolves a partition once it has NO
    // in-flight batches. Two batches are in flight (base sequence 0 and 3).
    let mut state = ProducerIdempotenceState::default();
    let seq0 = state.next_sequence("orders", 0, 3).expect("seq0");
    let seq3 = state.next_sequence("orders", 0, 3).expect("seq3");
    assert_eq!(seq0, 0);
    assert_eq!(seq3, 3);
    state.register_inflight_sequence("orders", 0, seq0);
    state.register_inflight_sequence("orders", 0, seq3);

    // Batch seq0 times out ambiguously and is marked unresolved. Resolving now
    // must NOT bump the epoch: seq3 is still in flight under the current epoch,
    // and bumping would re-send seq0 under a new epoch while seq3 could still
    // be written, risking a duplicate.
    state.mark_sequence_unresolved("orders", 0, seq0, 3);
    state.record_unresolved_loss_ambiguity("orders", 0, true);
    state.remove_inflight_sequence("orders", 0, seq0);
    let ambiguous = state.unresolved_loss_ambiguous("orders", 0);
    state.resolve_unresolved_sequence_after_drain("orders", 0, false, ambiguous);
    assert!(
        !state.epoch_bump_required,
        "must not bump the epoch while seq3 is still in flight"
    );
    assert!(state.has_inflight_batches("orders", 0));

    // seq3 drains → the partition has no in-flight batches, so the deferred
    // resolve now bumps the epoch (the loss was ambiguous).
    state.remove_inflight_sequence("orders", 0, seq3);
    assert!(!state.has_inflight_batches("orders", 0));
    let ambiguous = state.unresolved_loss_ambiguous("orders", 0);
    state.resolve_unresolved_sequence_after_drain("orders", 0, false, ambiguous);
    assert!(
        state.epoch_bump_required,
        "bump the epoch once the partition has fully drained"
    );
}

#[test]
fn detects_stale_producer_identity_after_epoch_bump_like_java() {
    use super::super::transaction::ProducerIdentity;
    // Kafka hasStaleProducerIdAndEpoch: a batch stamped under an older epoch is stale
    // once the producer identity advances, so it is re-stamped before being sent.
    let mut state = ProducerIdempotenceState::default();
    // No identity yet -> nothing is stale (cannot compare).
    assert!(!state.is_stale_identity(ProducerIdentity {
        producer_id: 42,
        producer_epoch: 3,
    }));

    state.identity = Some(ProducerIdentity {
        producer_id: 42,
        producer_epoch: 4,
    });
    // Same id, older epoch -> stale (epoch was bumped since).
    assert!(state.is_stale_identity(ProducerIdentity {
        producer_id: 42,
        producer_epoch: 3,
    }));
    // Current identity -> not stale.
    assert!(!state.is_stale_identity(ProducerIdentity {
        producer_id: 42,
        producer_epoch: 4,
    }));
    // Different producer id -> stale.
    assert!(state.is_stale_identity(ProducerIdentity {
        producer_id: 7,
        producer_epoch: 4,
    }));

    // request_epoch_bump flags the deferred bump applied in producer_batch_state.
    assert!(!state.epoch_bump_required);
    state.request_epoch_bump();
    assert!(state.epoch_bump_required);
    // reset_sequences_after_epoch_bump clears every partition's sequence state so a
    // global epoch bump restarts all partitions at 0 under the new epoch.
    let _ = state.next_sequence("orders", 0, 3).expect("sequence");
    state.register_inflight_sequence("orders", 0, 0);
    state.reset_sequences_after_epoch_bump();
    assert!(!state.epoch_bump_required);
    assert!(!state.has_inflight_batches("orders", 0));
    assert_eq!(
        state.next_sequence("orders", 0, 1).expect("reset sequence"),
        0
    );
}

#[test]
fn resolve_unresolved_after_drain_aborts_transaction_like_java() {
    use super::super::transaction::TransactionState;
    // Transactional producer with a bump-capable coordinator: abortable error.
    let mut state = ProducerIdempotenceState::default();
    state.in_transaction = true;
    let _ = state.next_sequence("orders", 0, 3).expect("sequence");
    state.mark_sequence_unresolved("orders", 0, 0, 3);

    state.resolve_unresolved_sequence_after_drain("orders", 0, true, true);

    assert_eq!(state.transaction_state, TransactionState::AbortableError);
    assert!(state.epoch_bump_required);

    // A coordinator that cannot bump escalates to fatal.
    let mut fatal = ProducerIdempotenceState::default();
    fatal.in_transaction = true;
    fatal.coordinator_lacks_epoch_bump_support = true;
    let _ = fatal.next_sequence("orders", 0, 3).expect("sequence");
    fatal.mark_sequence_unresolved("orders", 0, 0, 3);

    fatal.resolve_unresolved_sequence_after_drain("orders", 0, true, true);

    assert_eq!(fatal.transaction_state, TransactionState::FatalError);
}

#[test]
fn resolve_unresolved_after_drain_clears_marker_when_fully_acked_like_java() {
    // If subsequent batches were acked (lastAcked + 1 == next), the partition
    // is resolved without an epoch bump.
    let mut state = ProducerIdempotenceState::default();
    let _ = state.next_sequence("orders", 0, 3).expect("sequence");
    state.mark_sequence_unresolved("orders", 0, 0, 3);
    state.maybe_update_last_acked_sequence("orders", 0, 2); // next=3, lastAcked=2 => resolved

    state.resolve_unresolved_sequence_after_drain("orders", 0, false, false);

    assert!(!state.epoch_bump_required);
    assert_eq!(state.next_sequence("orders", 0, 1).expect("unblocked"), 3);
}

#[test]
fn idempotence_state_release_sequence_does_not_reuse_acked_sequence_like_java() {
    let mut state = ProducerIdempotenceState::default();
    assert_eq!(
        state
            .next_sequence("orders", 0, 3)
            .expect("initial sequence"),
        0
    );

    state.release_sequence("orders", 0, 0);

    assert_eq!(
        state
            .next_sequence("orders", 0, 1)
            .expect("next sequence after ack"),
        3
    );
}

#[test]
fn idempotence_state_out_of_order_retry_waits_when_unresolved_gap_is_not_next_like_java() {
    let mut state = ProducerIdempotenceState::default();
    state.mark_sequence_unresolved("orders", 0, 0, 1);

    assert!(
        !state.should_reset_sequence_for_idempotent_retry(IdempotentRetryDecision {
            topic: "orders",
            partition: 0,
            error: ErrorCode::OutOfOrderSequenceNumber,
            log_start_offset: -1,
            base_sequence: Some(2),
        },)
    );
    assert!(
        state.should_reset_sequence_for_idempotent_retry(IdempotentRetryDecision {
            topic: "orders",
            partition: 0,
            error: ErrorCode::OutOfOrderSequenceNumber,
            log_start_offset: -1,
            base_sequence: Some(1),
        },)
    );
}

#[test]
fn dispatch_error_classifies_route_and_wire_failures() {
    assert!(matches!(
        DispatchError::from_route(ProducerError::UnknownTopic("orders".to_owned())),
        DispatchError::Requeue
    ));
    assert!(matches!(
        DispatchError::from_route(ProducerError::Backpressure),
        DispatchError::Producer(ProducerError::Backpressure)
    ));
    assert!(matches!(
        DispatchError::from(crate::wire::WireError::UnknownBroker(42)),
        DispatchError::Producer(ProducerError::Wire(crate::wire::WireError::UnknownBroker(
            42
        )))
    ));
    assert!(matches!(
        DispatchError::from(ProducerError::Backpressure),
        DispatchError::Producer(ProducerError::Backpressure)
    ));
}

#[test]
fn broker_produce_request_groups_partitions_by_topic_id() {
    let mut request = BrokerProduceRequest::default();
    request.push(
        route("orders", 0),
        Bytes::from_static(b"records-0"),
        BrokerProduceOptions {
            acks: -1,
            timeout_ms: 1_500,
            transactional_id: Some("txn-a"),
        },
        3,
    );
    request.push(
        route("orders", 1),
        Bytes::from_static(b"records-1"),
        BrokerProduceOptions {
            acks: -1,
            timeout_ms: 1_500,
            transactional_id: Some("txn-a"),
        },
        5,
    );

    assert_eq!(request.data.acks, -1);
    assert_eq!(request.data.timeout_ms, 1_500);
    assert_eq!(
        request.data.transactional_id,
        Some(KafkaString::from("txn-a".to_owned()))
    );
    assert_eq!(request.data.topic_data.len(), 1);
    let topic = request.data.topic_data.first().expect("topic group");
    assert_eq!(topic.partition_data.len(), 2);
    assert_eq!(request.routes.len(), 2);
}

#[test]
fn broker_produce_request_keeps_zero_topic_id_topics_separate() {
    let mut request = BrokerProduceRequest::default();
    let orders = route("orders", 0);
    let payments = route("payments", 0);
    request.push(
        orders,
        Bytes::from_static(b"orders-records"),
        BrokerProduceOptions {
            acks: -1,
            timeout_ms: 1_500,
            transactional_id: None,
        },
        1,
    );

    assert!(!request.contains_route(&payments));

    request.push(
        payments,
        Bytes::from_static(b"payments-records"),
        BrokerProduceOptions {
            acks: -1,
            timeout_ms: 1_500,
            transactional_id: None,
        },
        1,
    );

    assert_eq!(request.data.topic_data.len(), 2);
    assert_eq!(request.data.topic_data[0].name.as_str(), "orders");
    assert_eq!(request.data.topic_data[1].name.as_str(), "payments");
    assert_eq!(
        request.data.topic_data[0].partition_data[0]
            .records
            .as_ref()
            .expect("orders records")
            .as_ref(),
        b"orders-records"
    );
    assert_eq!(
        request.data.topic_data[1].partition_data[0]
            .records
            .as_ref()
            .expect("payments records")
            .as_ref(),
        b"payments-records"
    );
}

#[test]
fn broker_produce_request_detects_duplicate_partition_route() {
    let mut request = BrokerProduceRequest::default();
    let options = BrokerProduceOptions {
        acks: 1,
        timeout_ms: 1_500,
        transactional_id: None,
    };
    let route = route("orders", 0);

    assert!(!request.contains_route(&route));
    request.push(route.clone(), Bytes::from_static(b"a"), options, 2);
    request.push(route.clone(), Bytes::from_static(b"b"), options, 1);

    let topic = request.data.topic_data.first().expect("topic group");
    assert_eq!(topic.partition_data.len(), 1);
    let partition = topic.partition_data.first().expect("partition data");
    assert_eq!(partition.records.as_ref().expect("records").as_ref(), b"ab");
    assert_eq!(request.routes.len(), 2);
    assert_eq!(request.routes[0].request_offset_delta, 0);
    assert_eq!(request.routes[0].record_count, 2);
    assert_eq!(request.routes[1].request_offset_delta, 2);
    assert_eq!(request.routes[1].record_count, 1);
    assert!(request.contains_route(&route));
}

#[test]
fn broker_request_index_starts_fresh_request_for_same_partition_batch() {
    let options = BrokerProduceOptions {
        acks: -1,
        timeout_ms: 1_500,
        transactional_id: None,
    };
    let version = client_api_info(ApiKey::Produce).max_version;
    let route = route("orders", 0);
    let first_records = Bytes::from(vec![b'a'; 128]);
    let second_records = Bytes::from(vec![b'b'; 128]);
    let mut request = BrokerProduceRequest::default();
    request.push(route.clone(), first_records, options, 1);
    let combined_len = request
        .encoded_len_after_push(&route, &second_records, options, version)
        .expect("combined request encoded len");

    let placement = broker_request_placement_for_batch(
        &[request],
        &route,
        &second_records,
        options,
        ProduceRequestSizing {
            version,
            max_request_size: combined_len,
        },
    )
    .expect("second same-partition batch fits in fresh request");

    assert_eq!(
        placement,
        BrokerRequestPlacement {
            index: 1,
            split: false
        }
    );
}

#[test]
fn broker_request_index_splits_when_next_partition_would_exceed_max_request_size() {
    let options = BrokerProduceOptions {
        acks: -1,
        timeout_ms: 1_500,
        transactional_id: None,
    };
    let version = client_api_info(ApiKey::Produce).max_version;
    let first_route = route("orders", 0);
    let second_route = route("orders", 1);
    let first_records = Bytes::from(vec![b'a'; 128]);
    let second_records = Bytes::from(vec![b'b'; 128]);
    let mut packed_request = BrokerProduceRequest::default();
    packed_request.push(first_route.clone(), first_records.clone(), options, 1);
    let packed_len = packed_request
        .encoded_len_after_push(&second_route, &second_records, options, version)
        .expect("packed request encoded len");
    let sizing = ProduceRequestSizing {
        version,
        max_request_size: packed_len.saturating_sub(1),
    };

    let mut requests = Vec::new();
    let first_placement = broker_request_placement_for_batch(
        &requests,
        &first_route,
        &first_records,
        options,
        sizing,
    )
    .expect("first request fits");
    assert_eq!(
        first_placement,
        BrokerRequestPlacement {
            index: 0,
            split: false
        }
    );
    requests.push(BrokerProduceRequest::default());
    requests[first_placement.index].push(first_route, first_records, options, 1);

    let second_placement = broker_request_placement_for_batch(
        &requests,
        &second_route,
        &second_records,
        options,
        sizing,
    )
    .expect("second request fits in a fresh request");

    assert_eq!(
        second_placement,
        BrokerRequestPlacement {
            index: 1,
            split: true
        }
    );
}

#[test]
fn broker_request_index_reports_record_too_large_for_single_oversize_request() {
    let options = BrokerProduceOptions {
        acks: -1,
        timeout_ms: 1_500,
        transactional_id: None,
    };
    let version = client_api_info(ApiKey::Produce).max_version;
    let route = route("orders", 0);
    let records = Bytes::from(vec![b'a'; 128]);
    let empty_request = BrokerProduceRequest::default();
    let encoded_len = empty_request
        .encoded_len_after_push(&route, &records, options, version)
        .expect("single request encoded len");

    let error = broker_request_placement_for_batch(
        &[],
        &route,
        &records,
        options,
        ProduceRequestSizing {
            version,
            max_request_size: encoded_len.saturating_sub(1),
        },
    )
    .expect_err("single oversize request should fail");

    assert!(matches!(
        error,
        DispatchError::Producer(ProducerError::RecordTooLarge { .. })
    ));
}

#[test]
fn unique_topics_keeps_first_seen_order() {
    let batches = vec![
        ready_batch("orders", 0),
        ready_batch("payments", 0),
        ready_batch("orders", 1),
    ];

    assert_eq!(unique_topics(&batches), ["orders", "payments"]);
}

#[test]
fn no_ack_receipts_use_synthetic_offsets() {
    let receipts = no_ack_receipts(&[route("orders", 2)]);

    assert_eq!(
        receipts,
        [RecordMetadata {
            topic: Arc::from("orders"),
            partition: 2,
            leader_id: 7,
            offset: -1,
            timestamp_ms: -1,
            serialized_key_size: -1,
            serialized_value_size: -1,
        }]
    );
}

#[tokio::test]
async fn complete_deliveries_skips_missing_receivers_and_missing_receipts() {
    let accumulator = SharedAccumulator::with_config(
        AccumulatorConfig::default()
            .batch_size(1)
            .linger(Duration::from_secs(1)),
    );
    let dropped_delivery = accumulator
        .append_for_delivery(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")))
        .expect("append for delivery");
    accumulator
        .append_at(
            ProducerRecord::new("payments", 0).value(Bytes::from_static(b"b")),
            Instant::now(),
        )
        .expect("append without delivery");
    let mut batches = accumulator.drain_ready(Instant::now());

    complete_deliveries(&mut batches, &[]);
    assert!(matches!(
        dropped_delivery.await,
        Err(ProducerError::DeliveryDropped)
    ));

    let accumulator = SharedAccumulator::with_config(
        AccumulatorConfig::default()
            .batch_size(1)
            .linger(Duration::from_secs(1)),
    );
    let delivery = accumulator
        .append_for_delivery(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")))
        .expect("append for delivery");
    let mut batches = accumulator.drain_ready(Instant::now());
    let receipt = RecordMetadata {
        topic: Arc::from("orders"),
        partition: 0,
        leader_id: 7,
        offset: 9,
        timestamp_ms: -1,
        serialized_key_size: -1,
        serialized_value_size: 1,
    };
    complete_deliveries(&mut batches, std::slice::from_ref(&receipt));

    assert_eq!(delivery.await.expect("delivered receipt"), receipt);
}

#[tokio::test]
async fn complete_deliveries_uses_later_split_receipts_for_original_delivery_handles() {
    let accumulator = SharedAccumulator::with_config(
        AccumulatorConfig::default()
            .batch_size(TEST_LARGE_BATCH_SIZE)
            .buffer_memory(TEST_LARGE_BATCH_SIZE * 4)
            .linger(Duration::from_secs(1)),
    );
    let first = accumulator
        .append_for_delivery(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")))
        .expect("append first delivery");
    let second = accumulator
        .append_for_delivery(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"b")))
        .expect("append second delivery");
    let third = accumulator
        .append_for_delivery(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"c")))
        .expect("append third delivery");
    let callback_offset = Arc::new(AtomicI64::new(-1));
    let callback_sink = Arc::clone(&callback_offset);
    third.register_callback(Box::new(move |result| {
        callback_sink.store(
            result.expect("third callback receipt").offset,
            Ordering::Relaxed,
        );
    }));
    let batch = accumulator
        .drain_all()
        .pop()
        .expect("drained delivery batch");
    let mut batches = batch
        .split_for_retry_with_compression_ratio(78, 1.0)
        .expect("split into retry batches");
    assert_eq!(batches.len(), 2);
    assert_eq!(batches[0].records.len(), 2);
    assert_eq!(batches[1].records.len(), 1);
    let receipts = vec![
        RecordMetadata {
            topic: Arc::from("orders"),
            partition: 0,
            leader_id: 7,
            offset: 40,
            timestamp_ms: -1,
            serialized_key_size: -1,
            serialized_value_size: 1,
        },
        RecordMetadata {
            topic: Arc::from("orders"),
            partition: 0,
            leader_id: 7,
            offset: 41,
            timestamp_ms: -1,
            serialized_key_size: -1,
            serialized_value_size: 1,
        },
        RecordMetadata {
            topic: Arc::from("orders"),
            partition: 0,
            leader_id: 7,
            offset: 99,
            timestamp_ms: -1,
            serialized_key_size: -1,
            serialized_value_size: 1,
        },
    ];

    complete_deliveries(&mut batches, &receipts);

    assert_eq!(first.await.expect("first receipt").offset, 40);
    assert_eq!(second.await.expect("second receipt").offset, 41);
    assert_eq!(callback_offset.load(Ordering::Relaxed), 99);
    assert_eq!(third.await.expect("third receipt").offset, 99);
}

#[test]
fn leadership_errors_match_retryable_metadata_failures() {
    assert!(is_leadership_error(ErrorCode::LeaderNotAvailable));
    assert!(is_leadership_error(ErrorCode::NotLeaderOrFollower));
    assert!(is_leadership_error(ErrorCode::UnknownLeaderEpoch));
    assert!(is_leadership_error(ErrorCode::FencedLeaderEpoch));
    assert!(!is_leadership_error(ErrorCode::InvalidTxnState));
}

#[test]
fn init_producer_id_version_caps_non_2pc_requests() {
    assert_eq!(init_producer_id_version(false), 5);
    assert!(
        init_producer_id_version(true) >= init_producer_id_version(false),
        "2PC path should use negotiated maximum"
    );
}

#[test]
fn transaction_v1_request_versions_match_java_caps() {
    assert_eq!(produce_version(false), 11);
    assert_eq!(txn_offset_commit_version(false), 4);
    assert_eq!(end_txn_version(false), 4);

    assert_eq!(
        produce_version(true),
        client_api_info(ApiKey::Produce).max_version
    );
    assert_eq!(
        txn_offset_commit_version(true),
        client_api_info(ApiKey::TxnOffsetCommit).max_version
    );
    assert_eq!(
        end_txn_version(true),
        client_api_info(ApiKey::EndTxn).max_version
    );
}

#[test]
fn coordinator_lookup_prefers_ipv4_when_localhost_resolves_to_both() {
    let ipv6 = SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 9092);
    let ipv4 = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9092);

    assert_eq!(choose_coordinator_addr([ipv6, ipv4]), Some(ipv4));
}

#[test]
fn coordinator_lookup_uses_first_address_when_ipv4_is_absent() {
    let ipv6 = SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 9092);

    assert_eq!(choose_coordinator_addr([ipv6]), Some(ipv6));
    assert_eq!(choose_coordinator_addr([]), None);
}
