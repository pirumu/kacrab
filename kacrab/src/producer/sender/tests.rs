#![allow(
    clippy::expect_used,
    clippy::missing_assert_message,
    reason = "Unit test fixtures fail fastest with contextual expect calls."
)]

use std::time::Duration;

use bytes::Bytes;
use kacrab_protocol::KafkaUuid;

use super::{
    AllDispatchApplication, AllDispatchProgress, AppendBackpressureAction,
    AppendCallbackDeliveryRecord, AppendCapacityWait, AppendDelivery, AppendDispatchApplication,
    AppendDispatchDecision, AppendUntracked, AppendUntrackedBatchApply, AppendUntrackedRecord,
    BatchAppendStatusApplication, BufferWaitAction, CALLBACK_READY_BATCH_POLL_THRESHOLD,
    CallbackAppendFastPath, CompletedDispatch, DENSE_READY_BATCH_RECORDS, DispatchSelection,
    DispatchSelectionStart, DispatchStart, DrainedDispatch, FlushDispatchProgress,
    PreparedAllDispatch, PreparedReadyDispatch, ProducerSender, ProducerSenderState,
    ReadyDispatchApplication, ReadyDispatchObservers, ReadyDispatchProgress, ReadyDispatchSlot,
    SenderLoopWait, SenderWaitSignal, SenderWakeAction, SenderWakeStep, TimedDispatchOutcome,
};
use crate::{
    producer::{
        ProducerError, ProducerRecord, ProducerRuntimeConfig,
        accumulator::{AccumulatorConfig, AppendStatus, ReadyBatchIdentity, SharedAccumulator},
        dispatcher::DispatchOutcome,
        metrics::{ProducerMetricValue, ProducerQueueMetrics},
    },
    wire::{
        BrokerMetadata, ClusterMetadata, ConnectionConfig, PartitionMetadata, TopicMetadata,
        WireClient,
    },
};

const TEST_LARGE_BATCH_SIZE: usize = 16 * 1024;

fn ready_batch(topic: &str, partition: i32) -> crate::producer::ReadyBatch {
    crate::producer::ReadyBatch {
        identity: ReadyBatchIdentity::test(0),
        topic: topic.to_owned(),
        partition,
        records: vec![ProducerRecord::new(topic, partition).value(Bytes::from_static(b"v"))],
        delivery: None,
        bytes: 1,
        pooled_buffer_bytes: 1,
        first_append_at: std::time::Instant::now(),
        producer_state: None,
    }
}

fn ready_accumulator() -> SharedAccumulator {
    let accumulator = SharedAccumulator::with_config(
        AccumulatorConfig::default()
            .batch_size(1)
            .linger(Duration::ZERO)
            .buffer_memory(16 * 1024),
    );
    accumulator
        .append_at(
            ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")),
            std::time::Instant::now(),
        )
        .expect("append record");
    accumulator
}

fn lingering_accumulator(now: std::time::Instant) -> SharedAccumulator {
    let accumulator = SharedAccumulator::with_config(
        AccumulatorConfig::default()
            .batch_size(16 * 1024)
            .linger(Duration::from_millis(10))
            .buffer_memory(16 * 1024),
    );
    accumulator
        .append_at(
            ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")),
            now,
        )
        .expect("append record");
    accumulator
}

fn test_dispatcher() -> crate::producer::dispatcher::ProducerDispatcher {
    crate::producer::dispatcher::ProducerDispatcher::new(WireClient::connect_with_brokers(
        ConnectionConfig::default(),
        "producer-test",
        [],
    ))
    .delivery_timeout(Duration::ZERO)
}

fn empty_cluster_metadata() -> ClusterMetadata {
    ClusterMetadata {
        cluster_id: None,
        controller_id: -1,
        brokers: Vec::new(),
        topics: Vec::new(),
    }
}

fn metadata_with_topics(topics: &[(&str, usize)]) -> ClusterMetadata {
    let topics = topics
        .iter()
        .map(|(topic, partitions)| {
            let partitions = (0..*partitions)
                .map(|partition| {
                    let partition_index =
                        i32::try_from(partition).expect("test partition fits i32");
                    PartitionMetadata {
                        partition_index,
                        leader_id: 7,
                        leader_epoch: 1,
                        replica_nodes: vec![7],
                        isr_nodes: vec![7],
                        offline_replicas: Vec::new(),
                    }
                })
                .collect();
            TopicMetadata {
                name: (*topic).to_owned(),
                topic_id: KafkaUuid::ZERO,
                is_internal: false,
                partitions,
            }
        })
        .collect();
    ClusterMetadata {
        cluster_id: Some("cluster-a".to_owned()),
        controller_id: 7,
        brokers: vec![BrokerMetadata {
            node_id: 7,
            host: "localhost".to_owned(),
            port: 9092,
            rack: None,
        }],
        topics,
    }
}

#[tokio::test]
async fn sender_state_reports_pending_work_from_buffer_or_in_flight_dispatch() {
    let mut state = ProducerSenderState::new(1);
    let empty = SharedAccumulator::with_config(AccumulatorConfig::default());
    assert!(!state.has_pending_work(&empty));

    let buffered = ready_accumulator();
    assert!(state.has_pending_work(&buffered));

    let _abort = state.spawn_in_flight(async {
        TimedDispatchOutcome {
            outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
            latency: Duration::ZERO,
            partitions: Vec::new(),
        }
    });
    assert!(state.has_pending_work(&empty));
}

#[tokio::test]
async fn sender_state_reports_in_flight_dispatches_for_flush_waits() {
    let mut state = ProducerSenderState::new(1);
    assert!(!state.has_in_flight_dispatches());

    let _abort = state.spawn_in_flight(async {
        TimedDispatchOutcome {
            outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
            latency: Duration::ZERO,
            partitions: Vec::new(),
        }
    });
    assert!(state.has_in_flight_dispatches());

    let joined = state
        .wait_for_next_dispatch()
        .await
        .expect("in-flight task should be present");
    let _completed = state
        .complete_joined_dispatch(joined)
        .expect("in-flight task should not panic");
    assert!(!state.has_in_flight_dispatches());
}

#[tokio::test]
async fn flush_completion_progress_reports_complete_or_waiting_for_in_flight_dispatch() {
    let mut state = ProducerSenderState::new(1);
    assert_eq!(
        state.flush_completion_progress(),
        FlushDispatchProgress::Complete
    );

    let _abort = state.spawn_in_flight(std::future::pending::<TimedDispatchOutcome>());

    assert_eq!(
        state.flush_completion_progress(),
        FlushDispatchProgress::WaitForCompletion
    );
}

#[tokio::test]
async fn wait_for_abort_completion_handles_in_flight_dispatches_until_empty() {
    let mut state = ProducerSenderState::new(2);
    let accumulator = SharedAccumulator::with_config(AccumulatorConfig::default());
    let first_latency = Duration::from_millis(5);
    let second_latency = Duration::from_millis(11);
    let mut observed_latencies = Vec::new();
    let mut observed_requeues = 0;

    let _first = state.spawn_in_flight(async move {
        TimedDispatchOutcome {
            outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
            latency: first_latency,
            partitions: Vec::new(),
        }
    });
    let _second = state.spawn_in_flight(async move {
        TimedDispatchOutcome {
            outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
            latency: second_latency,
            partitions: Vec::new(),
        }
    });

    state
        .wait_for_abort_completion(
            &accumulator,
            |latency| observed_latencies.push(latency),
            || observed_requeues += 1,
        )
        .await
        .expect("abort completion should drain delivered dispatches");

    observed_latencies.sort_unstable();
    assert_eq!(observed_latencies, vec![first_latency, second_latency]);
    assert_eq!(observed_requeues, 0);
    assert_eq!(
        state.flush_completion_progress(),
        FlushDispatchProgress::Complete
    );
}

#[tokio::test]
async fn wait_for_abort_completion_drops_requeued_in_flight_batches() {
    let mut state = ProducerSenderState::new(1);
    let accumulator = SharedAccumulator::with_config(AccumulatorConfig::default());
    let batch = ready_batch("orders", 0);
    let mut observed_latencies = Vec::new();
    let mut observed_requeues = 0;

    let _in_flight = state.spawn_in_flight(async move {
        TimedDispatchOutcome {
            outcome: DispatchOutcome::Requeue(vec![batch]),
            latency: Duration::from_millis(7),
            partitions: Vec::new(),
        }
    });

    state
        .wait_for_abort_completion(
            &accumulator,
            |latency| observed_latencies.push(latency),
            || observed_requeues += 1,
        )
        .await
        .expect("abort completion should drop requeued in-flight batches");

    assert!(observed_latencies.is_empty());
    assert_eq!(observed_requeues, 1);
    assert_eq!(accumulator.buffered_records(), 0);
    assert_eq!(
        state.flush_completion_progress(),
        FlushDispatchProgress::Complete
    );
}

#[tokio::test]
async fn producer_sender_waits_for_abort_completion_until_empty() {
    let mut sender = ProducerSender::new(AccumulatorConfig::default(), 2);
    let first_latency = Duration::from_millis(5);
    let second_latency = Duration::from_millis(11);
    let mut observed_latencies = Vec::new();
    let mut observed_requeues = 0;

    let _first = sender.state.spawn_in_flight(async move {
        TimedDispatchOutcome {
            outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
            latency: first_latency,
            partitions: Vec::new(),
        }
    });
    let _second = sender.state.spawn_in_flight(async move {
        TimedDispatchOutcome {
            outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
            latency: second_latency,
            partitions: Vec::new(),
        }
    });

    sender
        .wait_for_abort_completion(
            |latency| observed_latencies.push(latency),
            || observed_requeues += 1,
        )
        .await
        .expect("abort completion should drain delivered dispatches");

    observed_latencies.sort_unstable();
    assert_eq!(observed_latencies, vec![first_latency, second_latency]);
    assert_eq!(observed_requeues, 0);
    assert_eq!(
        sender.state.flush_completion_progress(),
        FlushDispatchProgress::Complete
    );
}

fn delivered_delivery_timeout_dispatch() -> TimedDispatchOutcome {
    TimedDispatchOutcome {
        // The dispatch task already failed this batch's delivery futures via
        // `fail_deliveries` before returning; the outcome only reports it.
        outcome: DispatchOutcome::Delivered(Err(ProducerError::DeliveryTimeout {
            topic: "orders".to_owned(),
            partition: 0,
        })),
        latency: Duration::from_millis(1),
        partitions: Vec::new(),
    }
}

#[tokio::test]
async fn background_pump_swallows_per_batch_delivery_error() {
    // The permanent post-outage wedge: a single expired batch's `Delivered(Err)`
    // must not abort the background drive, or every other partition's buffered
    // records starve. The pump keeps going; the error is already on the futures.
    let mut sender = ProducerSender::new(AccumulatorConfig::default(), 2);
    let _in_flight = sender
        .state
        .spawn_in_flight(async move { delivered_delivery_timeout_dispatch() });
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }

    let mut observed_latency = None;
    let result = sender
        .state
        .drive_ready_dispatch_until_blocked_with_policy(
            &sender.dispatcher,
            &sender.accumulator,
            false,
            ReadyDispatchObservers::new(
                |latency| observed_latency = Some(latency),
                || {},
                |_: &[crate::producer::ReadyBatch]| {},
            ),
        )
        .await;

    assert!(
        result.is_ok(),
        "background pump must swallow a per-batch delivery error, got {result:?}"
    );
    assert_eq!(observed_latency, Some(Duration::from_millis(1)));
}

#[tokio::test]
async fn strict_drive_surfaces_per_batch_delivery_error() {
    // The flush / append-capacity path still surfaces the failure to its caller.
    let mut sender = ProducerSender::new(AccumulatorConfig::default(), 2);
    let _in_flight = sender
        .state
        .spawn_in_flight(async move { delivered_delivery_timeout_dispatch() });
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }

    let result = sender
        .state
        .drive_ready_dispatch_until_blocked_with_policy(
            &sender.dispatcher,
            &sender.accumulator,
            true,
            ReadyDispatchObservers::new(|_| {}, || {}, |_: &[crate::producer::ReadyBatch]| {}),
        )
        .await;

    assert!(
        matches!(
            result,
            Err(ProducerError::DeliveryTimeout { partition: 0, .. })
        ),
        "strict drive must surface the delivery timeout, got {result:?}"
    );
}

#[tokio::test]
async fn drive_flush_until_complete_drains_in_flight_completion_after_empty_step() {
    let mut state = ProducerSenderState::new(1);
    let accumulator = SharedAccumulator::with_config(AccumulatorConfig::default());
    let latency = Duration::from_millis(23);
    let mut observed_latencies = Vec::new();
    let mut observed_requeues = 0;
    let mut observed_batches = Vec::new();
    let _in_flight = state.spawn_in_flight(async move {
        TimedDispatchOutcome {
            outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
            latency,
            partitions: Vec::new(),
        }
    });

    state
        .drive_flush_until_complete(
            &test_dispatcher(),
            &accumulator,
            ReadyDispatchObservers::new(
                |observed_latency| observed_latencies.push(observed_latency),
                || observed_requeues += 1,
                |batches: &[crate::producer::ReadyBatch]| {
                    observed_batches.push(batches.len());
                },
            ),
        )
        .await
        .expect("flush loop should drain in-flight completion");

    assert_eq!(observed_latencies, vec![latency]);
    assert_eq!(observed_requeues, 0);
    assert!(observed_batches.is_empty());
    assert_eq!(state.in_flight_len(), 0);
}

#[tokio::test]
async fn sender_state_reports_in_flight_dispatch_count_for_metrics() {
    let mut state = ProducerSenderState::new(2);
    assert_eq!(state.in_flight_dispatch_count(), 0);

    let _first = state.spawn_in_flight(async {
        TimedDispatchOutcome {
            outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
            latency: Duration::ZERO,
            partitions: Vec::new(),
        }
    });
    let _second = state.spawn_in_flight(async {
        TimedDispatchOutcome {
            outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
            latency: Duration::ZERO,
            partitions: Vec::new(),
        }
    });

    assert_eq!(state.in_flight_dispatch_count(), 2);
    tokio::task::yield_now().await;
    let _completed = state.collect_finished_dispatches();
    assert_eq!(state.in_flight_dispatch_count(), 0);
}

#[tokio::test]
async fn sender_state_reports_queue_snapshot_for_metrics() {
    let mut state = ProducerSenderState::new(2);
    let accumulator = ready_accumulator();
    let _dispatch = state.spawn_in_flight(async {
        TimedDispatchOutcome {
            outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
            latency: Duration::ZERO,
            partitions: Vec::new(),
        }
    });

    let snapshot = state.queue_snapshot(&accumulator);

    assert_eq!(snapshot.buffered_bytes, accumulator.buffered_bytes());
    assert_eq!(snapshot.buffered_records, accumulator.buffered_records());
    assert_eq!(snapshot.in_flight_dispatches, 1);
}

#[test]
fn producer_sender_owns_accumulator_and_state_queue_snapshot() {
    let sender = ProducerSender::new(AccumulatorConfig::default(), 2);
    sender
        .accumulator
        .append_at(
            ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")),
            std::time::Instant::now(),
        )
        .expect("append record");

    let snapshot = sender.queue_snapshot();

    assert!(sender.buffered_bytes() > 0);
    assert!(snapshot.buffered_bytes > 0);
    assert_eq!(snapshot.buffered_records, 1);
    assert_eq!(snapshot.in_flight_dispatches, 0);
    assert_eq!(sender.state.max_in_flight_requests(), 2);
}

#[test]
fn producer_sender_reports_next_ready_at_for_linger_scheduling() {
    let base = std::time::Instant::now();
    let linger = Duration::from_millis(10);
    let expected_ready_at = base
        .checked_add(linger)
        .expect("test ready deadline should fit");
    let before_ready = base
        .checked_add(Duration::from_millis(3))
        .expect("test before-ready instant should fit");
    let sender = ProducerSender::new(
        AccumulatorConfig::default()
            .batch_size(TEST_LARGE_BATCH_SIZE)
            .linger(linger),
        1,
    );
    sender
        .accumulator
        .append_at(
            ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")),
            base,
        )
        .expect("append lingering record");

    assert_eq!(sender.next_ready_at(before_ready), Some(expected_ready_at));
}

#[tokio::test]
async fn producer_sender_reports_next_wake_action_for_sender_loop() {
    let base = std::time::Instant::now();
    let linger = Duration::from_millis(10);
    let expected_ready_at = base
        .checked_add(linger)
        .expect("test ready deadline should fit");
    let before_ready = base
        .checked_add(Duration::from_millis(3))
        .expect("test before-ready instant should fit");
    let mut sender = ProducerSender::new(
        AccumulatorConfig::default()
            .batch_size(TEST_LARGE_BATCH_SIZE)
            .linger(linger),
        2,
    );

    assert_eq!(
        sender.next_wake_action(before_ready),
        SenderWakeAction::Park
    );

    sender
        .accumulator
        .append_at(
            ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")),
            base,
        )
        .expect("append lingering record");

    assert_eq!(
        sender.next_wake_action(before_ready),
        SenderWakeAction::SleepUntil(expected_ready_at)
    );
    assert_eq!(
        sender.next_wake_action(expected_ready_at),
        SenderWakeAction::DispatchReady
    );

    let _in_flight = sender.state.spawn_in_flight(async {
        TimedDispatchOutcome {
            outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
            latency: Duration::ZERO,
            partitions: Vec::new(),
        }
    });

    // One in-flight request but max.in.flight=2 -> still below the connection
    // capacity, so the loop keeps dispatching newly-ready batches (cross-partition
    // pipelining) instead of stopping at the first in-flight.
    assert_eq!(
        sender.next_wake_action(expected_ready_at),
        SenderWakeAction::DispatchReady
    );

    let _in_flight_2 = sender.state.spawn_in_flight(async {
        TimedDispatchOutcome {
            outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
            latency: Duration::ZERO,
            partitions: Vec::new(),
        }
    });

    // Now at the in-flight capacity (2) -> wait for a completion.
    assert_eq!(
        sender.next_wake_action(expected_ready_at),
        SenderWakeAction::WaitForDispatch
    );
}

#[tokio::test]
async fn producer_sender_drives_one_wake_step_for_sender_loop() {
    let now = std::time::Instant::now();
    let mut completion_sender = ProducerSender::new(AccumulatorConfig::default(), 1);
    let latency = Duration::from_millis(7);
    let mut observed_latencies = Vec::new();
    let mut observed_requeues = 0;
    let mut observed_batches = Vec::new();
    let _in_flight = completion_sender.state.spawn_in_flight(async move {
        TimedDispatchOutcome {
            outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
            latency,
            partitions: Vec::new(),
        }
    });

    let step = completion_sender
        .drive_wake_step(
            now,
            ReadyDispatchObservers::new(
                |observed_latency| observed_latencies.push(observed_latency),
                || observed_requeues += 1,
                |batches: &[crate::producer::ReadyBatch]| {
                    observed_batches.push(batches.len());
                },
            ),
        )
        .await
        .expect("sender step should handle completed dispatch");

    assert_eq!(step, SenderWakeStep::WaitedForDispatch);
    assert_eq!(observed_latencies, vec![latency]);
    assert_eq!(observed_requeues, 0);
    assert!(observed_batches.is_empty());
    assert_eq!(completion_sender.state.in_flight_len(), 0);

    let mut ready_sender = ProducerSender::new(
        AccumulatorConfig::default()
            .batch_size(1)
            .linger(Duration::ZERO),
        1,
    );
    ready_sender
        .accumulator
        .append_at(
            ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")),
            now,
        )
        .expect("append ready record");
    let mut ready_batches = Vec::new();

    let step = ready_sender
        .drive_wake_step(
            now,
            ReadyDispatchObservers::new(
                |_| {},
                || {},
                |batches: &[crate::producer::ReadyBatch]| {
                    ready_batches.push(batches.len());
                },
            ),
        )
        .await
        .expect("sender step should dispatch ready batch");

    assert_eq!(step, SenderWakeStep::DispatchedReady);
    assert_eq!(ready_batches, vec![1]);
    assert_eq!(ready_sender.accumulator.buffered_records(), 0);
    assert_eq!(ready_sender.state.in_flight_len(), 1);

    let linger = Duration::from_millis(10);
    let expected_ready_at = now
        .checked_add(linger)
        .expect("test ready deadline should fit");
    let before_ready = now
        .checked_add(Duration::from_millis(3))
        .expect("test before-ready instant should fit");
    let mut sleepy_sender = ProducerSender::new(
        AccumulatorConfig::default()
            .batch_size(TEST_LARGE_BATCH_SIZE)
            .linger(linger),
        1,
    );
    sleepy_sender
        .accumulator
        .append_at(
            ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")),
            now,
        )
        .expect("append lingering record");

    let step = sleepy_sender
        .drive_wake_step(
            before_ready,
            ReadyDispatchObservers::new(|_| {}, || {}, |_: &[crate::producer::ReadyBatch]| {}),
        )
        .await
        .expect("sender step should report linger wake");

    assert_eq!(step, SenderWakeStep::SleepUntil(expected_ready_at));
    assert_eq!(sleepy_sender.accumulator.buffered_records(), 1);

    let mut parked_sender = ProducerSender::new(AccumulatorConfig::default(), 1);
    let step = parked_sender
        .drive_wake_step(
            now,
            ReadyDispatchObservers::new(|_| {}, || {}, |_: &[crate::producer::ReadyBatch]| {}),
        )
        .await
        .expect("empty sender step should park");

    assert_eq!(step, SenderWakeStep::Parked);
}

#[tokio::test]
async fn producer_sender_drives_available_wake_work_until_waiting() {
    let now = std::time::Instant::now();
    let mut sender = ProducerSender::new(
        AccumulatorConfig::default()
            .batch_size(1)
            .linger(Duration::ZERO),
        1,
    );
    sender
        .accumulator
        .append_at(
            ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")),
            now,
        )
        .expect("append ready record");
    let latency = Duration::from_millis(13);
    let mut observed_latencies = Vec::new();
    let mut observed_requeues = 0;
    let mut observed_batches = Vec::new();
    let _completed = sender.state.spawn_in_flight(async move {
        TimedDispatchOutcome {
            outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
            latency,
            partitions: Vec::new(),
        }
    });
    tokio::task::yield_now().await;

    let wait = sender
        .drive_wake_until_waiting(
            now,
            ReadyDispatchObservers::new(
                |observed_latency| observed_latencies.push(observed_latency),
                || observed_requeues += 1,
                |batches: &[crate::producer::ReadyBatch]| {
                    observed_batches.push(batches.len());
                },
            ),
        )
        .await
        .expect("sender loop work should reach next wait state");

    assert_eq!(wait, SenderLoopWait::DispatchCompletion);
    assert_eq!(observed_latencies, vec![latency]);
    assert_eq!(observed_requeues, 0);
    assert_eq!(observed_batches, vec![1]);
    assert_eq!(sender.accumulator.buffered_records(), 0);
    assert_eq!(sender.state.in_flight_len(), 1);
}

#[test]
fn sender_state_discards_buffered_batches_for_abort_lifecycle() {
    let accumulator = ready_accumulator();

    let dropped = ProducerSenderState::discard_buffered_batches(&accumulator);

    assert_eq!(dropped, 1);
    assert_eq!(accumulator.buffered_records(), 0);
    assert_eq!(accumulator.buffered_bytes(), 0);
}

#[test]
fn producer_sender_discards_buffered_batches_for_abort_lifecycle() {
    let sender = ProducerSender::new(
        AccumulatorConfig::default()
            .batch_size(1)
            .linger(Duration::ZERO)
            .buffer_memory(16 * 1024),
        1,
    );
    sender
        .accumulator
        .append_at(
            ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")),
            std::time::Instant::now(),
        )
        .expect("append ready batch");

    let dropped = sender.discard_buffered_batches();

    assert_eq!(dropped, 1);
    assert_eq!(sender.accumulator.buffered_records(), 0);
    assert_eq!(sender.accumulator.buffered_bytes(), 0);
}

#[tokio::test]
async fn producer_sender_assigns_partition_with_accumulator_without_exposing_accumulator() {
    let sender = ProducerSender::new(AccumulatorConfig::default(), 1);
    let mut record = ProducerRecord::new("orders", 2);

    sender
        .assign_partition_with_accumulator(&mut record)
        .await
        .expect("assigned record should not need metadata");

    assert_eq!(record.partition, 2);
}

#[tokio::test]
async fn producer_sender_refreshes_empty_partition_load_stats_without_exposing_accumulator() {
    let sender = ProducerSender::new(AccumulatorConfig::default(), 1);
    let metadata = empty_cluster_metadata();

    sender
        .refresh_partition_load_stats_with_metadata(&metadata, std::iter::empty::<&str>())
        .await
        .expect("empty topic refresh should be a no-op");
}

#[tokio::test]
async fn producer_sender_refreshes_topic_load_stats_without_exposing_accumulator() {
    let sender = ProducerSender::new(AccumulatorConfig::default(), 1);
    let metadata = empty_cluster_metadata();

    let result = sender
        .refresh_topic_load_stats_with_metadata(&metadata, "orders")
        .await;

    assert!(matches!(
        result,
        Err(ProducerError::UnknownTopic(topic)) if topic == "orders"
    ));
}

#[test]
fn producer_sender_reports_default_sticky_partitioner_policy() {
    let sender = ProducerSender::new(AccumulatorConfig::default(), 1);

    assert!(sender.uses_sticky_partitioner(&ProducerRecord::unassigned("orders")));
    assert!(!sender.uses_sticky_partitioner(
        &ProducerRecord::unassigned("orders").key(Bytes::from_static(b"key"))
    ));
    assert!(!sender.uses_sticky_partitioner(&ProducerRecord::new("orders", 2)));
}

#[tokio::test]
async fn producer_sender_fetches_metadata_through_owned_dispatcher() {
    let sender = ProducerSender::new(AccumulatorConfig::default(), 1);

    let result = sender.metadata_for_topics(["orders"]).await;

    assert!(result.is_err());
}

#[tokio::test]
async fn producer_sender_checks_transaction_error_through_owned_dispatcher() {
    let sender = ProducerSender::new(AccumulatorConfig::default(), 1);

    sender.fail_if_transaction_error().await.unwrap();
}

#[tokio::test]
async fn producer_sender_assigns_partition_with_metadata_through_owned_dispatcher() {
    let sender = ProducerSender::new(AccumulatorConfig::default(), 1);
    let metadata = metadata_with_topics(&[("orders", 3)]);
    let mut record = ProducerRecord::unassigned("orders").value(Bytes::from_static(b"a"));

    sender
        .assign_partition_with_metadata(&metadata, &mut record)
        .await
        .expect("sender should assign partition through owned dispatcher");

    assert!(record.has_assigned_partition());
}

#[test]
fn producer_sender_exposes_metrics_handle_from_owned_dispatcher() {
    let sender = ProducerSender::new(AccumulatorConfig::default(), 1);

    let metrics = sender.metrics_handle();

    assert_eq!(
        metrics
            .snapshot(ProducerQueueMetrics::default())
            .produce_request_count,
        0
    );
}

#[tokio::test]
async fn producer_sender_refreshes_and_assigns_topic_partitions_with_metadata() {
    let sender = ProducerSender::new(AccumulatorConfig::default(), 1);
    let metadata = metadata_with_topics(&[("orders", 3)]);
    let mut records = vec![
        ProducerRecord::unassigned("orders").value(Bytes::from_static(b"a")),
        ProducerRecord::unassigned("orders").value(Bytes::from_static(b"b")),
    ];

    sender
        .refresh_and_assign_topic_partitions_with_metadata(&metadata, "orders", &mut records, true)
        .await
        .expect("sender should refresh load stats and assign topic partitions");

    assert!(records.iter().all(ProducerRecord::has_assigned_partition));
}

#[tokio::test]
async fn producer_sender_refreshes_and_assigns_multiple_topics_with_metadata() {
    let sender = ProducerSender::new(AccumulatorConfig::default(), 1);
    let metadata = metadata_with_topics(&[("orders", 3), ("payments", 2)]);
    let mut records = vec![
        ProducerRecord::unassigned("orders").value(Bytes::from_static(b"a")),
        ProducerRecord::unassigned("payments").value(Bytes::from_static(b"b")),
    ];

    sender
        .refresh_and_assign_partitions_with_metadata(
            &metadata,
            ["orders", "payments"],
            &mut records,
        )
        .await
        .expect("sender should refresh load stats and assign multi-topic partitions");

    assert!(records.iter().all(ProducerRecord::has_assigned_partition));
}

#[test]
fn sender_state_creates_append_poll_budget_from_in_flight_limit() {
    let state = ProducerSenderState::new(3);
    let mut budget = state.append_poll_budget();
    let ready = AppendStatus {
        batch_ready: true,
        ready_batch_records: 1,
        starts_new_batch: false,
    };

    assert!(!ProducerSenderState::observe_batch_append_status(
        &mut budget,
        ready
    ));
    assert!(!ProducerSenderState::observe_batch_append_status(
        &mut budget,
        ready
    ));
    assert!(ProducerSenderState::observe_batch_append_status(
        &mut budget,
        ready
    ));
}

#[test]
fn producer_sender_creates_append_poll_budget_from_in_flight_limit() {
    let sender = ProducerSender::new(AccumulatorConfig::default(), 3);
    let mut budget = sender.append_poll_budget();
    let ready = AppendStatus {
        batch_ready: true,
        ready_batch_records: 1,
        starts_new_batch: false,
    };

    assert!(!ProducerSenderState::observe_batch_append_status(
        &mut budget,
        ready
    ));
    assert!(!ProducerSenderState::observe_batch_append_status(
        &mut budget,
        ready
    ));
    assert!(ProducerSenderState::observe_batch_append_status(
        &mut budget,
        ready
    ));
}

#[test]
fn sender_state_observes_batch_append_poll_budget() {
    let state = ProducerSenderState::new(3);
    let mut budget = state.append_poll_budget();
    let pending = AppendStatus {
        batch_ready: false,
        ready_batch_records: 0,
        starts_new_batch: false,
    };
    let ready = AppendStatus {
        batch_ready: true,
        ready_batch_records: 1,
        starts_new_batch: false,
    };
    let dense_ready = AppendStatus {
        batch_ready: true,
        ready_batch_records: DENSE_READY_BATCH_RECORDS,
        starts_new_batch: false,
    };

    assert!(!ProducerSenderState::observe_batch_append_status(
        &mut budget,
        pending
    ));
    assert!(!ProducerSenderState::observe_batch_append_status(
        &mut budget,
        ready
    ));
    assert!(!ProducerSenderState::observe_batch_append_status(
        &mut budget,
        ready
    ));
    assert!(ProducerSenderState::observe_batch_append_status(
        &mut budget,
        ready
    ));
    assert!(ProducerSenderState::observe_batch_append_status(
        &mut budget,
        dense_ready
    ));
}

#[test]
fn sender_state_reports_append_dispatch_decisions() {
    let mut state = ProducerSenderState::new(3);
    let mut budget = state.append_poll_budget();
    let pending = AppendStatus {
        batch_ready: false,
        ready_batch_records: 0,
        starts_new_batch: false,
    };
    let ready = AppendStatus {
        batch_ready: true,
        ready_batch_records: 1,
        starts_new_batch: false,
    };
    let dense_ready = AppendStatus {
        batch_ready: true,
        ready_batch_records: DENSE_READY_BATCH_RECORDS,
        starts_new_batch: false,
    };

    assert_eq!(
        ProducerSenderState::single_append_dispatch_decision(pending),
        AppendDispatchDecision::Idle
    );
    assert_eq!(
        ProducerSenderState::single_append_dispatch_decision(ready),
        AppendDispatchDecision::DriveReady
    );
    assert_eq!(
        ProducerSenderState::batch_append_dispatch_decision(&mut budget, pending),
        AppendDispatchDecision::Idle
    );
    assert_eq!(
        ProducerSenderState::batch_append_dispatch_decision(&mut budget, ready),
        AppendDispatchDecision::MarkBatchReady
    );
    assert_eq!(
        ProducerSenderState::batch_append_dispatch_decision(&mut budget, ready),
        AppendDispatchDecision::MarkBatchReady
    );
    assert_eq!(
        ProducerSenderState::batch_append_dispatch_decision(&mut budget, ready),
        AppendDispatchDecision::DriveReady
    );
    assert_eq!(
        ProducerSenderState::batch_append_dispatch_decision(&mut budget, dense_ready),
        AppendDispatchDecision::DriveReady
    );
    assert_eq!(
        state.callback_append_dispatch_decision(ready),
        AppendDispatchDecision::MarkBatchReady
    );
}

#[test]
fn producer_sender_reports_callback_append_dispatch_decision() {
    let mut sender = ProducerSender::new(AccumulatorConfig::default(), 3);
    let pending = AppendStatus {
        batch_ready: false,
        ready_batch_records: 0,
        starts_new_batch: false,
    };
    let ready = AppendStatus {
        batch_ready: true,
        ready_batch_records: 1,
        starts_new_batch: false,
    };

    assert_eq!(
        ProducerSenderState::single_append_dispatch_decision(pending),
        AppendDispatchDecision::Idle
    );
    assert_eq!(
        sender.callback_append_dispatch_decision(ready),
        AppendDispatchDecision::MarkBatchReady
    );
}

#[tokio::test]
async fn sender_state_applies_append_dispatch_decision() {
    let mut state = ProducerSenderState::new(1);
    let dispatcher = test_dispatcher();
    let accumulator = ready_accumulator();
    let mut observed_batches = Vec::new();

    state
        .apply_append_dispatch_decision(
            &accumulator,
            AppendDispatchApplication::new(
                &dispatcher,
                AppendDispatchDecision::MarkBatchReady,
                Some("orders"),
            ),
            ReadyDispatchObservers::new(
                |_| {},
                || {},
                |batches: &[crate::producer::ReadyBatch]| {
                    observed_batches.push(batches.len());
                },
            ),
        )
        .await
        .expect("mark-only decision should not dispatch");

    assert!(observed_batches.is_empty());
    assert_eq!(accumulator.buffered_records(), 1);
    assert_eq!(state.in_flight_len(), 0);

    state
        .apply_append_dispatch_decision(
            &accumulator,
            AppendDispatchApplication::new(
                &dispatcher,
                AppendDispatchDecision::DriveReady,
                Some("orders"),
            ),
            ReadyDispatchObservers::new(
                |_| {},
                || {},
                |batches: &[crate::producer::ReadyBatch]| {
                    observed_batches.push(batches.len());
                },
            ),
        )
        .await
        .expect("drive decision should dispatch ready batches");

    assert_eq!(observed_batches, vec![1]);
    assert_eq!(accumulator.buffered_records(), 0);
    assert_eq!(state.in_flight_len(), 1);
}

#[tokio::test]
async fn sender_state_applies_append_dispatch_decision_then_collects_finished_dispatches() {
    let mut state = ProducerSenderState::new(2);
    let dispatcher = test_dispatcher();
    let accumulator = SharedAccumulator::with_config(AccumulatorConfig::default());
    let latency = Duration::from_millis(7);
    let mut observed_latencies = Vec::new();
    let mut observed_requeues = 0;
    let mut observed_batches = Vec::new();

    let _completed = state.spawn_in_flight(async move {
        TimedDispatchOutcome {
            outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
            latency,
            partitions: Vec::new(),
        }
    });
    tokio::task::yield_now().await;

    state
        .apply_append_dispatch_decision_then_collect_finished(
            &accumulator,
            AppendDispatchApplication::new(&dispatcher, AppendDispatchDecision::Idle, None),
            false,
            ReadyDispatchObservers::new(
                |duration| observed_latencies.push(duration),
                || observed_requeues += 1,
                |batches: &[crate::producer::ReadyBatch]| {
                    observed_batches.push(batches.len());
                },
            ),
        )
        .await
        .expect("idle decision should still collect completed dispatches");

    assert_eq!(observed_latencies, vec![latency]);
    assert_eq!(observed_requeues, 0);
    assert!(observed_batches.is_empty());
    assert_eq!(state.in_flight_len(), 0);
}

#[tokio::test]
async fn producer_sender_applies_append_dispatch_decision_then_collects_finished_dispatches() {
    let mut sender = ProducerSender::new(AccumulatorConfig::default(), 2);
    let latency = Duration::from_millis(7);
    let mut observed_latencies = Vec::new();
    let mut observed_requeues = 0;
    let mut observed_batches = Vec::new();

    let _completed = sender.state.spawn_in_flight(async move {
        TimedDispatchOutcome {
            outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
            latency,
            partitions: Vec::new(),
        }
    });
    tokio::task::yield_now().await;

    sender
        .apply_append_dispatch_decision_then_collect_finished(
            AppendDispatchDecision::Idle,
            None,
            false,
            ReadyDispatchObservers::new(
                |duration| observed_latencies.push(duration),
                || observed_requeues += 1,
                |batches: &[crate::producer::ReadyBatch]| {
                    observed_batches.push(batches.len());
                },
            ),
        )
        .await
        .expect("idle decision should still collect completed dispatches");

    assert_eq!(observed_latencies, vec![latency]);
    assert_eq!(observed_requeues, 0);
    assert!(observed_batches.is_empty());
    assert_eq!(sender.state.in_flight_len(), 0);
}

#[tokio::test]
async fn sender_state_finishes_batch_append_by_driving_ready_dispatch() {
    let mut state = ProducerSenderState::new(1);
    let dispatcher = test_dispatcher();
    let accumulator = ready_accumulator();
    let mut observed_batches = Vec::new();

    state
        .finish_batch_append_dispatch(
            &dispatcher,
            &accumulator,
            ReadyDispatchObservers::new(
                |_| {},
                || {},
                |batches: &[crate::producer::ReadyBatch]| {
                    observed_batches.push(batches.len());
                },
            ),
        )
        .await
        .expect("batch append finish should drive ready batches");

    assert_eq!(observed_batches, vec![1]);
    assert_eq!(accumulator.buffered_records(), 0);
    assert_eq!(state.in_flight_len(), 1);
}

#[tokio::test]
async fn producer_sender_finishes_batch_append_by_driving_ready_dispatch() {
    let mut sender = ProducerSender::new(
        AccumulatorConfig::default()
            .batch_size(1)
            .linger(Duration::ZERO),
        1,
    );
    sender
        .accumulator
        .append_at(
            ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")),
            std::time::Instant::now(),
        )
        .expect("append ready batch");
    let mut observed_batches = Vec::new();

    sender
        .finish_batch_append_dispatch(ReadyDispatchObservers::new(
            |_| {},
            || {},
            |batches: &[crate::producer::ReadyBatch]| {
                observed_batches.push(batches.len());
            },
        ))
        .await
        .expect("batch append finish should drive ready batches");

    assert_eq!(observed_batches, vec![1]);
    assert_eq!(sender.accumulator.buffered_records(), 0);
    assert_eq!(sender.state.in_flight_len(), 1);
}

#[tokio::test]
async fn sender_state_applies_batch_append_status_with_poll_budget() {
    let mut state = ProducerSenderState::new(2);
    let dispatcher = test_dispatcher();
    let accumulator = ready_accumulator();
    let mut budget = state.append_poll_budget();
    let status = AppendStatus {
        batch_ready: true,
        ready_batch_records: 1,
        starts_new_batch: false,
    };
    let mut observed_batches = Vec::new();

    state
        .apply_batch_append_status(
            &accumulator,
            &mut budget,
            BatchAppendStatusApplication::new(&dispatcher, status, Some("orders")),
            ReadyDispatchObservers::new(
                |_| {},
                || {},
                |batches: &[crate::producer::ReadyBatch]| {
                    observed_batches.push(batches.len());
                },
            ),
        )
        .await
        .expect("first ready batch should only mark sticky readiness");

    assert!(observed_batches.is_empty());
    assert_eq!(accumulator.buffered_records(), 1);
    assert_eq!(state.in_flight_len(), 0);

    state
        .apply_batch_append_status(
            &accumulator,
            &mut budget,
            BatchAppendStatusApplication::new(&dispatcher, status, Some("orders")),
            ReadyDispatchObservers::new(
                |_| {},
                || {},
                |batches: &[crate::producer::ReadyBatch]| {
                    observed_batches.push(batches.len());
                },
            ),
        )
        .await
        .expect("second ready batch should drive dispatch at budget threshold");

    assert_eq!(observed_batches, vec![1]);
    assert_eq!(accumulator.buffered_records(), 0);
    assert_eq!(state.in_flight_len(), 1);
}

#[tokio::test]
async fn producer_sender_applies_batch_append_status_with_poll_budget() {
    let mut sender = ProducerSender::new(
        AccumulatorConfig::default()
            .batch_size(1)
            .linger(Duration::ZERO),
        2,
    );
    sender
        .accumulator
        .append_at(
            ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")),
            std::time::Instant::now(),
        )
        .expect("append ready batch");
    let mut budget = sender.state.append_poll_budget();
    let status = AppendStatus {
        batch_ready: true,
        ready_batch_records: 1,
        starts_new_batch: false,
    };
    let mut observed_batches = Vec::new();

    sender
        .apply_batch_append_status(
            &mut budget,
            status,
            Some("orders"),
            ReadyDispatchObservers::new(
                |_| {},
                || {},
                |batches: &[crate::producer::ReadyBatch]| {
                    observed_batches.push(batches.len());
                },
            ),
        )
        .await
        .expect("first ready batch should only mark sticky readiness");

    assert!(observed_batches.is_empty());
    assert_eq!(sender.accumulator.buffered_records(), 1);
    assert_eq!(sender.state.in_flight_len(), 0);

    sender
        .apply_batch_append_status(
            &mut budget,
            status,
            Some("orders"),
            ReadyDispatchObservers::new(
                |_| {},
                || {},
                |batches: &[crate::producer::ReadyBatch]| {
                    observed_batches.push(batches.len());
                },
            ),
        )
        .await
        .expect("second ready batch should drive dispatch at budget threshold");

    assert_eq!(observed_batches, vec![1]);
    assert_eq!(sender.accumulator.buffered_records(), 0);
    assert_eq!(sender.state.in_flight_len(), 1);
}

#[tokio::test]
async fn producer_sender_appends_untracked_batch_record_and_applies_batch_status() {
    let now = std::time::Instant::now();
    let deadline = now + Duration::from_millis(5);
    let mut sender = ProducerSender::new(
        AccumulatorConfig::default()
            .batch_size(1)
            .linger(Duration::ZERO)
            .buffer_memory(16 * 1024),
        1,
    );
    let mut budget = sender.append_poll_budget();
    let mut observed_batches = Vec::new();

    sender
        .append_untracked_record_then_apply_batch_status(
            AppendUntrackedBatchApply::new(
                &mut budget,
                AppendUntrackedRecord::new(
                    ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")),
                    now,
                    deadline,
                ),
                None,
            ),
            ReadyDispatchObservers::new(
                |_| {},
                || {},
                |batches: &[crate::producer::ReadyBatch]| {
                    observed_batches.push(batches.len());
                },
            ),
        )
        .await
        .expect("untracked batch append should dispatch ready batch");

    assert_eq!(observed_batches, vec![1]);
    assert_eq!(sender.accumulator.buffered_records(), 0);
    assert_eq!(sender.state.in_flight_len(), 1);
}

#[test]
fn sender_state_owns_callback_append_poll_budget_across_records() {
    let mut state = ProducerSenderState::new(5);
    let ready = AppendStatus {
        batch_ready: true,
        ready_batch_records: 1,
        starts_new_batch: false,
    };

    for _ in 0..CALLBACK_READY_BATCH_POLL_THRESHOLD.saturating_sub(1) {
        assert!(!state.observe_callback_append_status(ready));
    }
    assert!(state.observe_callback_append_status(ready));
    assert!(!state.observe_callback_append_status(ready));
}

#[tokio::test]
async fn sender_state_prioritizes_dispatch_completion_for_buffer_wait() {
    let mut state = ProducerSenderState::new(1);
    let now = std::time::Instant::now();
    let accumulator = lingering_accumulator(now);
    let _abort = state.spawn_in_flight(async {
        TimedDispatchOutcome {
            outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
            latency: Duration::ZERO,
            partitions: Vec::new(),
        }
    });

    let action = state.buffer_wait_action(&accumulator, now, now + Duration::from_millis(5));

    assert_eq!(action, BufferWaitAction::WaitForDispatch);
}

#[tokio::test]
async fn wait_for_buffer_progress_handles_dispatch_completion() {
    let mut state = ProducerSenderState::new(1);
    let now = std::time::Instant::now();
    let accumulator = lingering_accumulator(now);
    let latency = Duration::from_millis(19);
    let mut observed_latencies = Vec::new();
    let mut observed_requeues = 0;
    let mut observed_batches = Vec::new();
    let _abort = state.spawn_in_flight(async move {
        TimedDispatchOutcome {
            outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
            latency,
            partitions: Vec::new(),
        }
    });

    state
        .wait_for_buffer_progress(
            &test_dispatcher(),
            &accumulator,
            now + Duration::from_millis(5),
            ReadyDispatchObservers::new(
                |observed_latency| observed_latencies.push(observed_latency),
                || observed_requeues += 1,
                |batches: &[crate::producer::ReadyBatch]| {
                    observed_batches.push(batches.len());
                },
            ),
        )
        .await
        .expect("buffer wait should handle in-flight completion");

    assert_eq!(observed_latencies, vec![latency]);
    assert_eq!(observed_requeues, 0);
    assert!(observed_batches.is_empty());
    assert_eq!(accumulator.buffered_records(), 1);
    assert_eq!(state.in_flight_len(), 0);
}

#[test]
fn sender_state_reports_buffer_wait_deadline_elapsed() {
    let state = ProducerSenderState::new(1);
    let now = std::time::Instant::now();
    let accumulator = lingering_accumulator(now);

    let action = state.buffer_wait_action(&accumulator, now, now);

    assert_eq!(action, BufferWaitAction::DeadlineElapsed);
}

#[test]
fn sender_state_polls_when_buffer_is_ready_now() {
    let state = ProducerSenderState::new(1);
    let now = std::time::Instant::now();
    let accumulator = ready_accumulator();

    let action = state.buffer_wait_action(&accumulator, now, now + Duration::from_millis(5));

    assert_eq!(action, BufferWaitAction::PollReady);
}

#[test]
fn sender_state_caps_buffer_sleep_to_one_millisecond() {
    let state = ProducerSenderState::new(1);
    let now = std::time::Instant::now();
    let accumulator = lingering_accumulator(now);

    let action = state.buffer_wait_action(&accumulator, now, now + Duration::from_millis(5));

    assert_eq!(action, BufferWaitAction::Sleep(Duration::from_millis(1)));
}

#[test]
fn sender_state_owns_append_backpressure_action() {
    let state = ProducerSenderState::new(1);
    let now = std::time::Instant::now();
    let deadline = now + Duration::from_millis(5);
    let record = ProducerRecord::new("orders", 0).value(Bytes::from_static(
        b"this record is intentionally too large for the remaining buffer",
    ));
    let available = SharedAccumulator::with_config(AccumulatorConfig::default());

    assert_eq!(
        state.append_backpressure_action(&available, &record, now, deadline),
        AppendBackpressureAction::Append
    );

    let full_without_work =
        SharedAccumulator::with_config(AccumulatorConfig::default().buffer_memory(1));
    assert_eq!(
        state.append_backpressure_action(&full_without_work, &record, now, deadline),
        AppendBackpressureAction::Backpressure
    );

    let full_with_work = SharedAccumulator::with_config(
        AccumulatorConfig::default()
            .batch_size(128)
            .buffer_memory(128),
    );
    full_with_work
        .append_at(
            ProducerRecord::new("orders", 0).value(Bytes::from_static(b"x")),
            now,
        )
        .expect("append pending work");
    assert_eq!(
        state.append_backpressure_action(&full_with_work, &record, now, deadline),
        AppendBackpressureAction::WaitForBuffer
    );
    assert_eq!(
        state.append_backpressure_action(&full_with_work, &record, deadline, deadline),
        AppendBackpressureAction::Backpressure
    );
}

#[tokio::test]
async fn wait_for_append_capacity_drains_ready_buffered_batch_before_append() {
    let mut state = ProducerSenderState::new(1);
    let now = std::time::Instant::now();
    let deadline = now + Duration::from_millis(5);
    let accumulator = SharedAccumulator::with_config(
        AccumulatorConfig::default()
            .batch_size(128)
            .linger(Duration::ZERO)
            .buffer_memory(160),
    );
    accumulator
        .append_at(
            ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")),
            now,
        )
        .expect("append pending ready batch");
    let record = ProducerRecord::new("orders", 0).value(Bytes::from_static(
        b"this record fits only after the ready batch is drained",
    ));
    let dispatcher = test_dispatcher();
    let mut observed_batches = Vec::new();

    let result = state
        .wait_for_append_capacity(
            &accumulator,
            AppendCapacityWait::new(&dispatcher, &record, deadline),
            ReadyDispatchObservers::new(
                |_| {},
                || {},
                |batches: &[crate::producer::ReadyBatch]| {
                    observed_batches.push(batches.len());
                },
            ),
        )
        .await;

    assert_eq!(observed_batches, vec![1]);
    assert_eq!(accumulator.buffered_records(), 0);
    assert_eq!(state.in_flight_len(), 0);
    assert!(matches!(
        result,
        Err(ProducerError::DeliveryTimeout {
            topic,
            partition: 0,
        }) if topic == "orders"
    ));
    assert!(state.has_available_memory_for(&accumulator, &record));
}

#[tokio::test]
async fn append_untracked_waits_for_capacity_then_appends_record() {
    let mut state = ProducerSenderState::new(1);
    let now = std::time::Instant::now();
    let deadline = now + Duration::from_millis(5);
    let accumulator = SharedAccumulator::with_config(
        AccumulatorConfig::default()
            .batch_size(128)
            .linger(Duration::ZERO)
            .buffer_memory(160),
    );
    accumulator
        .append_at(
            ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")),
            now,
        )
        .expect("append pending ready batch");
    let record = ProducerRecord::new("orders", 0).value(Bytes::from_static(
        b"this record fits only after the ready batch is drained",
    ));
    let dispatcher = test_dispatcher();
    let mut observed_batches = Vec::new();

    let result = state
        .append_untracked_with_capacity_wait(
            &accumulator,
            AppendUntracked::new(&dispatcher, record, now, deadline),
            ReadyDispatchObservers::new(
                |_| {},
                || {},
                |batches: &[crate::producer::ReadyBatch]| {
                    observed_batches.push(batches.len());
                },
            ),
        )
        .await;

    assert_eq!(observed_batches, vec![1]);
    match result {
        Ok(status) => assert!(status.starts_new_batch),
        Err(ProducerError::Backpressure | ProducerError::DeliveryTimeout { .. }) => {},
        Err(error) => panic!("unexpected append result: {error:?}"),
    }
    assert!(accumulator.buffered_records() <= 1);
    assert_eq!(state.in_flight_len(), 0);
}

#[tokio::test]
async fn producer_sender_append_untracked_owns_capacity_wait_and_append() {
    let now = std::time::Instant::now();
    let deadline = now + Duration::from_millis(5);
    let mut sender = ProducerSender::new(
        AccumulatorConfig::default()
            .batch_size(128)
            .linger(Duration::ZERO)
            .buffer_memory(160),
        1,
    );
    sender
        .accumulator
        .append_at(
            ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")),
            now,
        )
        .expect("append pending ready batch");
    let record = ProducerRecord::new("orders", 0).value(Bytes::from_static(
        b"this record fits only after the ready batch is drained",
    ));
    let mut observed_batches = Vec::new();

    let result = sender
        .append_untracked_record_with_capacity_wait(
            record,
            now,
            deadline,
            ReadyDispatchObservers::new(
                |_| {},
                || {},
                |batches: &[crate::producer::ReadyBatch]| {
                    observed_batches.push(batches.len());
                },
            ),
        )
        .await;

    assert_eq!(observed_batches, vec![1]);
    assert!(matches!(result, Err(ProducerError::Wire(_))));
    assert_eq!(sender.accumulator.buffered_records(), 0);
    assert_eq!(sender.state.in_flight_len(), 0);
}

#[tokio::test]
async fn producer_sender_notifies_sender_loop_after_untracked_append() {
    let now = std::time::Instant::now();
    let deadline = now + Duration::from_millis(5);
    let mut sender = ProducerSender::new(AccumulatorConfig::default(), 1);
    let notify = sender.sender_loop_notifier();

    let status = sender
        .append_untracked_record_with_capacity_wait(
            ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")),
            now,
            deadline,
            ReadyDispatchObservers::new(|_| {}, || {}, |_: &[crate::producer::ReadyBatch]| {}),
        )
        .await
        .expect("append should succeed");

    assert!(!status.batch_ready);
    tokio::time::timeout(Duration::from_millis(20), notify.notified())
        .await
        .expect("append should notify sender loop");
}

#[tokio::test]
async fn producer_sender_skips_redundant_pending_append_notify() {
    let now = std::time::Instant::now();
    let deadline = now + Duration::from_millis(5);
    let mut sender = ProducerSender::new(
        AccumulatorConfig::default()
            .batch_size(16 * 1024)
            .linger(Duration::from_millis(5)),
        1,
    );
    let notify = sender.sender_loop_notifier();

    let first = sender
        .append_untracked_record_with_capacity_wait(
            ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")),
            now,
            deadline,
            ReadyDispatchObservers::new(|_| {}, || {}, |_: &[crate::producer::ReadyBatch]| {}),
        )
        .await
        .expect("first append should succeed");
    tokio::time::timeout(Duration::from_millis(20), notify.notified())
        .await
        .expect("first append should notify sender loop");
    let second = sender
        .append_untracked_record_with_capacity_wait(
            ProducerRecord::new("orders", 0).value(Bytes::from_static(b"b")),
            now,
            deadline,
            ReadyDispatchObservers::new(|_| {}, || {}, |_: &[crate::producer::ReadyBatch]| {}),
        )
        .await
        .expect("second append should succeed");

    assert!(first.starts_new_batch);
    assert!(!first.batch_ready);
    assert!(!second.starts_new_batch);
    assert!(!second.batch_ready);
    assert!(
        tokio::time::timeout(Duration::from_millis(5), notify.notified())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn producer_sender_delay_wait_wakes_on_sender_loop_notification() {
    let sender = ProducerSender::new(AccumulatorConfig::default(), 1);
    let notify = sender.sender_loop_notifier();
    notify.notify_one();

    let signal = tokio::time::timeout(
        Duration::from_millis(20),
        sender.wait_for_sender_loop_delay(Duration::from_secs(5)),
    )
    .await
    .expect("notify should wake sender loop delay");

    assert_eq!(signal, SenderWaitSignal::Notified);
}

#[tokio::test]
async fn producer_sender_notifies_sender_loop_when_dispatch_completes() {
    let mut sender = ProducerSender::new(AccumulatorConfig::default(), 1);
    let notify = sender.sender_loop_notifier();
    let _in_flight = sender.state.spawn_in_flight(async {
        TimedDispatchOutcome {
            outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
            latency: Duration::ZERO,
            partitions: Vec::new(),
        }
    });

    tokio::time::timeout(Duration::from_millis(50), notify.notified())
        .await
        .expect("dispatch completion should wake sender loop");
}

#[tokio::test]
async fn producer_sender_loop_wait_handles_dispatch_completion() {
    let now = std::time::Instant::now();
    let mut sender = ProducerSender::new(AccumulatorConfig::default(), 1);
    let latency = Duration::from_millis(23);
    let mut observed_latencies = Vec::new();
    let mut observed_requeues = 0;
    let _in_flight = sender.state.spawn_in_flight(async move {
        TimedDispatchOutcome {
            outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
            latency,
            partitions: Vec::new(),
        }
    });

    let signal = sender
        .wait_for_sender_loop_wait(
            SenderLoopWait::DispatchCompletion,
            now,
            |observed_latency| observed_latencies.push(observed_latency),
            || observed_requeues += 1,
        )
        .await
        .expect("sender loop wait should handle completed dispatch");

    assert_eq!(signal, SenderWaitSignal::DispatchCompleted);
    assert_eq!(observed_latencies, vec![latency]);
    assert_eq!(observed_requeues, 0);
    assert_eq!(sender.state.in_flight_len(), 0);
}

#[tokio::test]
async fn producer_sender_loop_tick_waits_after_reaching_dispatch_completion() {
    let now = std::time::Instant::now();
    let mut sender = ProducerSender::new(AccumulatorConfig::default(), 1);
    let latency = Duration::from_millis(29);
    let mut observed_latencies = Vec::new();
    let mut observed_requeues = 0;
    let mut observed_batches = Vec::new();
    let _in_flight = sender.state.spawn_in_flight(async move {
        tokio::task::yield_now().await;
        TimedDispatchOutcome {
            outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
            latency,
            partitions: Vec::new(),
        }
    });

    let signal = tokio::time::timeout(
        Duration::from_millis(50),
        sender.drive_sender_loop_once(
            now,
            ReadyDispatchObservers::new(
                |observed_latency| observed_latencies.push(observed_latency),
                || observed_requeues += 1,
                |batches: &[crate::producer::ReadyBatch]| {
                    observed_batches.push(batches.len());
                },
            ),
        ),
    )
    .await
    .expect("sender loop tick should wait for dispatch completion")
    .expect("sender loop tick should handle dispatch completion");

    assert_eq!(signal, SenderWaitSignal::DispatchCompleted);
    assert_eq!(observed_latencies, vec![latency]);
    assert_eq!(observed_requeues, 0);
    assert!(observed_batches.is_empty());
    assert_eq!(sender.state.in_flight_len(), 0);
}

#[tokio::test]
async fn producer_sender_buffer_progress_dispatches_after_linger_sleep() {
    let now = std::time::Instant::now();
    let mut sender = ProducerSender::new(
        AccumulatorConfig::default()
            .batch_size(TEST_LARGE_BATCH_SIZE)
            .linger(Duration::from_millis(10)),
        1,
    );
    sender
        .accumulator
        .append_at(
            ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")),
            now,
        )
        .expect("append lingering record");
    let mut observed_batches = Vec::new();

    sender
        .wait_for_buffer_progress(
            now.checked_add(Duration::from_millis(50))
                .expect("deadline should fit"),
            ReadyDispatchObservers::new(
                |_| {},
                || {},
                |batches: &[crate::producer::ReadyBatch]| {
                    observed_batches.push(batches.len());
                },
            ),
        )
        .await
        .expect("buffer progress should dispatch after linger sleep");

    assert_eq!(observed_batches, vec![1]);
    assert_eq!(sender.accumulator.buffered_records(), 0);
    assert_eq!(sender.state.in_flight_len(), 1);
}

#[tokio::test]
async fn append_for_delivery_waits_for_capacity_then_returns_delivery() {
    let mut state = ProducerSenderState::new(1);
    let now = std::time::Instant::now();
    let deadline = now + Duration::from_millis(5);
    let accumulator = SharedAccumulator::with_config(
        AccumulatorConfig::default()
            .batch_size(128)
            .linger(Duration::ZERO)
            .buffer_memory(160),
    );
    accumulator
        .append_at(
            ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")),
            now,
        )
        .expect("append pending ready batch");
    let record = ProducerRecord::new("orders", 0).value(Bytes::from_static(
        b"this record fits only after the ready batch is drained",
    ));
    let dispatcher = test_dispatcher();
    let mut observed_batches = Vec::new();

    let result = state
        .append_for_delivery_with_capacity_wait(
            &accumulator,
            AppendDelivery::new(&dispatcher, record, now, deadline),
            ReadyDispatchObservers::new(
                |_| {},
                || {},
                |batches: &[crate::producer::ReadyBatch]| {
                    observed_batches.push(batches.len());
                },
            ),
        )
        .await;

    assert_eq!(observed_batches, vec![1]);
    match result {
        Ok((_delivery, status)) => assert!(status.starts_new_batch),
        Err(ProducerError::Backpressure | ProducerError::DeliveryTimeout { .. }) => {},
        Err(error) => panic!("unexpected append result: {error:?}"),
    }
    assert!(accumulator.buffered_records() <= 1);
    assert_eq!(state.in_flight_len(), 0);
}

#[tokio::test]
async fn producer_sender_append_for_delivery_owns_capacity_wait_and_append() {
    let now = std::time::Instant::now();
    let deadline = now + Duration::from_millis(5);
    let mut sender = ProducerSender::new(
        AccumulatorConfig::default()
            .batch_size(128)
            .linger(Duration::ZERO)
            .buffer_memory(160),
        1,
    );
    sender
        .accumulator
        .append_at(
            ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")),
            now,
        )
        .expect("append pending ready batch");
    let record = ProducerRecord::new("orders", 0).value(Bytes::from_static(
        b"this record fits only after the ready batch is drained",
    ));
    let mut observed_batches = Vec::new();

    let result = sender
        .append_delivery_record_with_capacity_wait(
            record,
            now,
            deadline,
            ReadyDispatchObservers::new(
                |_| {},
                || {},
                |batches: &[crate::producer::ReadyBatch]| {
                    observed_batches.push(batches.len());
                },
            ),
        )
        .await;

    assert_eq!(observed_batches, vec![1]);
    assert!(matches!(result, Err(ProducerError::Wire(_))));
    assert_eq!(sender.accumulator.buffered_records(), 0);
    assert_eq!(sender.state.in_flight_len(), 0);
}

#[tokio::test]
async fn producer_sender_appends_callback_delivery_and_returns_dispatch_decision() {
    let now = std::time::Instant::now();
    let deadline = now + Duration::from_millis(5);
    let mut sender = ProducerSender::new(
        AccumulatorConfig::default()
            .batch_size(1)
            .linger(Duration::ZERO)
            .buffer_memory(16 * 1024),
        5,
    );

    for _ in 0..CALLBACK_READY_BATCH_POLL_THRESHOLD.saturating_sub(1) {
        let (_delivery, decision) = sender
            .append_callback_delivery_record_with_capacity_wait(
                ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")),
                now,
                deadline,
                ReadyDispatchObservers::new(|_| {}, || {}, |_: &[crate::producer::ReadyBatch]| {}),
            )
            .await
            .expect("callback append should succeed");

        assert_eq!(decision, AppendDispatchDecision::MarkBatchReady);
    }

    let (_delivery, decision) = sender
        .append_callback_delivery_record_with_capacity_wait(
            ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")),
            now,
            deadline,
            ReadyDispatchObservers::new(|_| {}, || {}, |_: &[crate::producer::ReadyBatch]| {}),
        )
        .await
        .expect("callback append should reach poll threshold");

    assert_eq!(decision, AppendDispatchDecision::DriveReady);
}

#[tokio::test]
async fn producer_sender_appends_callback_delivery_and_applies_dispatch_decision() {
    let now = std::time::Instant::now();
    let deadline = now + Duration::from_millis(5);
    let mut sender = ProducerSender::new(
        AccumulatorConfig::default()
            .batch_size(1)
            .linger(Duration::ZERO)
            .buffer_memory(16 * 1024),
        5,
    );
    let mut observed_batches = Vec::new();
    let registered_before_dispatch = std::cell::Cell::new(false);

    for _ in 0..CALLBACK_READY_BATCH_POLL_THRESHOLD.saturating_sub(1) {
        let _delivery = sender
            .append_callback_delivery_record_then_apply_dispatch(
                AppendCallbackDeliveryRecord::new(
                    ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")),
                    now,
                    deadline,
                    None,
                ),
                |_| {
                    registered_before_dispatch.set(true);
                },
                ReadyDispatchObservers::new(
                    |_| {},
                    || {},
                    |batches: &[crate::producer::ReadyBatch]| {
                        assert!(registered_before_dispatch.get());
                        observed_batches.push(batches.len());
                    },
                ),
            )
            .await
            .expect("callback append should succeed before poll threshold");
    }

    assert!(observed_batches.is_empty());
    assert_eq!(
        sender.accumulator.buffered_records(),
        CALLBACK_READY_BATCH_POLL_THRESHOLD.saturating_sub(1)
    );

    let _delivery = sender
        .append_callback_delivery_record_then_apply_dispatch(
            AppendCallbackDeliveryRecord::new(
                ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")),
                now,
                deadline,
                None,
            ),
            |_| {
                registered_before_dispatch.set(true);
            },
            ReadyDispatchObservers::new(
                |_| {},
                || {},
                |batches: &[crate::producer::ReadyBatch]| {
                    assert!(registered_before_dispatch.get());
                    observed_batches.push(batches.len());
                },
            ),
        )
        .await
        .expect("callback append should dispatch at poll threshold");

    assert_eq!(observed_batches, vec![CALLBACK_READY_BATCH_POLL_THRESHOLD]);
    assert_eq!(sender.accumulator.buffered_records(), 0);
    assert_eq!(sender.state.in_flight_len(), 1);
}

#[test]
fn producer_sender_fast_appends_callback_delivery_when_capacity_available() {
    let now = std::time::Instant::now();
    let mut sender = ProducerSender::new(
        AccumulatorConfig::default()
            .batch_size(1)
            .linger(Duration::ZERO)
            .buffer_memory(16 * 1024),
        5,
    );

    let result = sender.try_append_callback_delivery_record(
        ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")),
        now,
    );

    match result {
        CallbackAppendFastPath::Appended(Ok((_delivery, decision))) => {
            assert_eq!(decision, AppendDispatchDecision::MarkBatchReady);
        },
        CallbackAppendFastPath::Appended(Err(err)) => {
            panic!("fast callback append should succeed: {err}");
        },
        CallbackAppendFastPath::WouldBlock(_) => {
            panic!("fast callback append should not block with available capacity");
        },
    }
    assert_eq!(sender.accumulator.buffered_records(), 1);
}

#[test]
fn producer_sender_fast_callback_append_returns_record_when_capacity_missing() {
    let now = std::time::Instant::now();
    let mut sender = ProducerSender::new(
        AccumulatorConfig::default()
            .batch_size(1024)
            .linger(Duration::from_millis(100))
            .buffer_memory(1),
        5,
    );

    let result = sender.try_append_callback_delivery_record(
        ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")),
        now,
    );

    match result {
        CallbackAppendFastPath::WouldBlock(record) => {
            assert_eq!(record.topic.as_ref(), "orders");
            assert_eq!(record.partition, 0);
            assert_eq!(
                record.value.as_ref().map(Bytes::as_ref),
                Some(&b"value"[..])
            );
        },
        CallbackAppendFastPath::Appended(Ok(_)) => {
            panic!("fast callback append should not append without capacity");
        },
        CallbackAppendFastPath::Appended(Err(err)) => {
            panic!("fast callback append should return record for fallback, got {err}");
        },
    }
    assert_eq!(sender.accumulator.buffered_records(), 0);
}

#[test]
fn idempotent_sender_state_defers_partitions_already_in_flight() {
    let mut state = ProducerSenderState::new(1);
    state.reserve_partitions_for_dispatch(&[ready_batch("orders", 0)]);

    let selection =
        state.select_dispatchable_batches(vec![ready_batch("orders", 0), ready_batch("orders", 1)]);

    assert_eq!(selection.dispatchable.len(), 1);
    assert_eq!(selection.dispatchable[0].partition, 1);
    assert_eq!(selection.deferred.len(), 1);
    assert_eq!(selection.deferred[0].partition, 0);
    assert_eq!(selection.partitions.len(), 1);
}

// The firstInFlightSequence retry-ordering gate now lives in the dispatcher's
// `prepare_drained_batches` (reads producer_state, the single source of truth) and is
// covered end-to-end by the fault-injection integration test
// `idempotent_kafka_producer_resends_multi_inflight_batches_in_sequence_order_after_retry`.

#[tokio::test]
async fn sender_state_owns_in_flight_task_slots() {
    let mut state = ProducerSenderState::new(1);

    let _abort = state.spawn_in_flight(async {
        TimedDispatchOutcome {
            outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
            latency: Duration::ZERO,
            partitions: Vec::new(),
        }
    });
    assert_eq!(state.in_flight_len(), 1);

    let joined = state
        .wait_for_next_dispatch()
        .await
        .expect("in-flight task should be present")
        .expect("in-flight task should not panic");

    assert!(matches!(joined.outcome, DispatchOutcome::Delivered(Ok(_))));
    assert_eq!(state.in_flight_len(), 0);
}

#[tokio::test]
async fn sender_state_collects_finished_dispatch_tasks_without_blocking() {
    let mut state = ProducerSenderState::new(2);

    let _first = state.spawn_in_flight(async {
        TimedDispatchOutcome {
            outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
            latency: Duration::ZERO,
            partitions: Vec::new(),
        }
    });
    let _second = state.spawn_in_flight(async {
        TimedDispatchOutcome {
            outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
            latency: Duration::ZERO,
            partitions: Vec::new(),
        }
    });
    tokio::task::yield_now().await;

    let completed = state.collect_finished_dispatches();

    assert_eq!(completed.len(), 2);
    assert_eq!(state.in_flight_len(), 0);
    for result in completed {
        let joined = result.expect("finished dispatch task should not panic");
        assert!(matches!(joined.outcome, DispatchOutcome::Delivered(Ok(_))));
    }
}

#[tokio::test]
async fn sender_state_waits_for_next_dispatch_task() {
    let mut state = ProducerSenderState::new(1);
    assert!(state.wait_for_next_dispatch().await.is_none());

    let _abort = state.spawn_in_flight(async {
        TimedDispatchOutcome {
            outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
            latency: Duration::ZERO,
            partitions: Vec::new(),
        }
    });

    let joined = state
        .wait_for_next_dispatch()
        .await
        .expect("in-flight task should be present")
        .expect("in-flight task should not panic");
    assert!(matches!(joined.outcome, DispatchOutcome::Delivered(Ok(_))));
    assert_eq!(state.in_flight_len(), 0);
}

#[tokio::test]
async fn sender_state_waits_for_dispatch_completion() {
    let mut state = ProducerSenderState::new(1);
    assert!(state.wait_for_dispatch_completion().await.is_none());

    let _abort = state.spawn_in_flight(async {
        TimedDispatchOutcome {
            outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
            latency: Duration::ZERO,
            partitions: Vec::new(),
        }
    });

    let joined = state
        .wait_for_dispatch_completion()
        .await
        .expect("in-flight task should be present")
        .expect("in-flight task should not panic");
    assert!(matches!(joined.outcome, DispatchOutcome::Delivered(Ok(_))));
    assert_eq!(state.in_flight_len(), 0);
}

#[tokio::test]
async fn spawn_dispatch_task_reserves_partitions_until_completion() {
    let mut state = ProducerSenderState::new(1);
    let reserved = ready_batch("orders", 0);
    let partition = super::InFlightPartitionKey::from(&reserved);
    let partition_for_task = partition.clone();

    let _abort = state
        .spawn_dispatch_task(std::slice::from_ref(&partition), async move {
            TimedDispatchOutcome {
                outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
                latency: Duration::ZERO,
                partitions: vec![partition_for_task],
            }
        })
        .expect("dispatch task should spawn");

    let blocked = state.select_dispatchable_batches(vec![ready_batch("orders", 0)]);
    assert!(blocked.dispatchable.is_empty());
    assert_eq!(blocked.deferred.len(), 1);

    let joined = state
        .wait_for_next_dispatch()
        .await
        .expect("in-flight task should be present");
    let completed = state
        .complete_joined_dispatch(joined)
        .expect("completed dispatch should not panic");
    assert!(matches!(
        completed.outcome,
        DispatchOutcome::Delivered(Ok(_))
    ));

    let unblocked = state.select_dispatchable_batches(vec![ready_batch("orders", 0)]);
    assert_eq!(unblocked.dispatchable.len(), 1);
    assert!(unblocked.deferred.is_empty());
}

#[tokio::test]
async fn spawn_drained_dispatch_owns_dispatch_task_body_and_partition_reservation() {
    let mut state = ProducerSenderState::new(1);
    let batch = ready_batch("orders", 0);
    let partition = super::InFlightPartitionKey::from(&batch);
    let dispatcher = crate::producer::dispatcher::ProducerDispatcher::new(
        WireClient::connect_with_brokers(ConnectionConfig::default(), "producer-test", []),
    )
    .delivery_timeout(Duration::ZERO);

    let dispatch = DrainedDispatch::new(
        dispatcher,
        vec![batch],
        std::time::Instant::now(),
        vec![partition.clone()],
    );
    let _abort = state
        .spawn_drained_dispatch(dispatch)
        .expect("drained dispatch should spawn");

    let blocked = state.select_dispatchable_batches(vec![ready_batch("orders", 0)]);
    assert!(blocked.dispatchable.is_empty());

    let joined = state
        .wait_for_next_dispatch()
        .await
        .expect("in-flight task should be present");
    let completed = state
        .complete_joined_dispatch(joined)
        .expect("completed dispatch should not panic");
    assert!(matches!(
        completed.outcome,
        DispatchOutcome::Delivered(Err(ProducerError::DeliveryTimeout {
            topic,
            partition: 0,
        })) if topic == "orders"
    ));

    let unblocked = state.select_dispatchable_batches(vec![ready_batch("orders", 0)]);
    assert_eq!(unblocked.dispatchable.len(), 1);
    assert!(completed.latency >= Duration::ZERO);
}

#[tokio::test]
async fn spawn_observed_drained_dispatch_records_batches_before_task_owns_them() {
    let mut state = ProducerSenderState::new(1);
    let batch = ready_batch("orders", 0);
    let partition = super::InFlightPartitionKey::from(&batch);
    let dispatcher = crate::producer::dispatcher::ProducerDispatcher::new(
        WireClient::connect_with_brokers(ConnectionConfig::default(), "producer-test", []),
    )
    .delivery_timeout(Duration::ZERO);
    let mut observed = Vec::new();

    let dispatch = DrainedDispatch::new(
        dispatcher,
        vec![batch],
        std::time::Instant::now(),
        vec![partition.clone()],
    );
    let _abort = state
        .spawn_observed_drained_dispatch(dispatch, |batches| {
            observed.push((batches.len(), batches[0].bytes, batches[0].records.len()));
        })
        .expect("observed drained dispatch should spawn");

    assert_eq!(observed, vec![(1, 1, 1)]);
    let blocked = state.select_dispatchable_batches(vec![ready_batch("orders", 0)]);
    assert!(blocked.dispatchable.is_empty());

    let completed = state
        .wait_for_completed_dispatch()
        .await
        .expect("in-flight task should be present")
        .expect("completed dispatch should not panic");
    assert!(matches!(
        completed.outcome,
        DispatchOutcome::Delivered(Err(ProducerError::DeliveryTimeout {
            topic,
            partition: 0,
        })) if topic == "orders"
    ));

    let unblocked = state.select_dispatchable_batches(vec![ready_batch("orders", 0)]);
    assert_eq!(unblocked.dispatchable.len(), 1);
}

#[tokio::test]
async fn in_flight_batch_bytes_hold_append_backpressure_until_dispatch_completes() {
    let mut state = ProducerSenderState::new(1);
    let mut batch = ready_batch("orders", 0);
    batch.bytes = 128;
    batch.pooled_buffer_bytes = 128;
    let partition = super::InFlightPartitionKey::from(&batch);
    let dispatcher = crate::producer::dispatcher::ProducerDispatcher::new(
        WireClient::connect_with_brokers(ConnectionConfig::default(), "producer-test", []),
    )
    .delivery_timeout(Duration::ZERO);
    let now = std::time::Instant::now();
    let deadline = now + Duration::from_millis(5);
    let accumulator = SharedAccumulator::with_config(
        AccumulatorConfig::default()
            .batch_size(128)
            .buffer_memory(160),
    );
    let record = ProducerRecord::new("orders", 0).value(Bytes::from_static(
        b"this record only fits after in-flight batch bytes are released",
    ));

    let dispatch = DrainedDispatch::new(dispatcher, vec![batch], now, vec![partition]);
    let _abort = state
        .spawn_observed_drained_dispatch(dispatch, |_| {})
        .expect("observed drained dispatch should spawn");

    assert_eq!(
        state.append_backpressure_action(&accumulator, &record, now, deadline),
        AppendBackpressureAction::WaitForBuffer
    );

    let result = state
        .wait_for_handled_dispatch(
            &SharedAccumulator::with_config(AccumulatorConfig::default()),
            false,
            |_| {},
            || {},
        )
        .await;
    assert!(matches!(
        result,
        Err(ProducerError::DeliveryTimeout {
            topic,
            partition: 0,
        }) if topic == "orders"
    ));

    assert_eq!(
        state.append_backpressure_action(&accumulator, &record, now, deadline),
        AppendBackpressureAction::Append
    );
}

#[tokio::test]
async fn buffer_wait_metric_tracks_append_blocked_on_buffer_memory_like_java() {
    fn ignore_latency(_: Duration) {}
    fn ignore_requeue() {}
    fn ignore_batches(_: &[super::ReadyBatch]) {}

    let now = std::time::Instant::now();
    let deadline = now + Duration::from_millis(50);
    let mut sender = ProducerSender::new(
        AccumulatorConfig::default()
            .batch_size(80)
            .linger(Duration::from_mins(1))
            .buffer_memory(80),
        1,
    );
    let _status = sender
        .accumulator
        .append_with_status_at(
            ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")),
            now,
        )
        .expect("append first buffered batch");
    let metrics = sender.metrics_handle();

    let task = tokio::spawn(async move {
        let record = ProducerRecord::new("orders", 1).value(Bytes::from_static(b"b"));
        sender
            .append_delivery_record_with_capacity_wait(
                record,
                now,
                deadline,
                ReadyDispatchObservers::new(ignore_latency, ignore_requeue, ignore_batches),
            )
            .await
    });

    let mut observed_waiter = false;
    let poll_deadline = std::time::Instant::now() + Duration::from_millis(20);
    while std::time::Instant::now() < poll_deadline {
        if metrics
            .snapshot(ProducerQueueMetrics::default())
            .metric("waiting_threads")
            == Some(ProducerMetricValue::Gauge(1))
        {
            observed_waiter = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }

    assert!(observed_waiter);
    let result = task.await.expect("buffer wait task should not panic");
    assert!(matches!(result, Err(ProducerError::Backpressure)));
    assert_eq!(
        metrics
            .snapshot(ProducerQueueMetrics::default())
            .metric("waiting_threads"),
        Some(ProducerMetricValue::Gauge(0))
    );
}

#[tokio::test]
async fn in_flight_dispatch_tracks_incomplete_batches_until_completion() {
    let mut state = ProducerSenderState::new(1);
    let mut batch = ready_batch("orders", 0);
    batch.bytes = 128;
    batch.pooled_buffer_bytes = 128;
    let partition = super::InFlightPartitionKey::from(&batch);
    let dispatcher = crate::producer::dispatcher::ProducerDispatcher::new(
        WireClient::connect_with_brokers(ConnectionConfig::default(), "producer-test", []),
    )
    .delivery_timeout(Duration::ZERO);
    let accumulator = SharedAccumulator::with_config(
        AccumulatorConfig::default()
            .batch_size(128)
            .buffer_memory(160),
    );

    let dispatch = DrainedDispatch::new(
        dispatcher,
        vec![batch],
        std::time::Instant::now(),
        vec![partition],
    );
    let _abort = state
        .spawn_observed_drained_dispatch(dispatch, |_| {})
        .expect("observed drained dispatch should spawn");

    let snapshot = state.queue_snapshot(&accumulator);
    assert_eq!(snapshot.incomplete_batches, 1);
    assert_eq!(snapshot.in_flight_dispatches, 1);
    assert_eq!(snapshot.buffered_bytes, 128);

    let result = state
        .wait_for_handled_dispatch(
            &SharedAccumulator::with_config(AccumulatorConfig::default()),
            false,
            |_| {},
            || {},
        )
        .await;
    assert!(matches!(
        result,
        Err(ProducerError::DeliveryTimeout {
            topic,
            partition: 0,
        }) if topic == "orders"
    ));

    let snapshot = state.queue_snapshot(&accumulator);
    assert_eq!(snapshot.incomplete_batches, 0);
    assert_eq!(snapshot.in_flight_dispatches, 0);
    assert_eq!(snapshot.buffered_bytes, 0);
}

#[tokio::test]
async fn in_flight_dispatch_rejects_duplicate_batch_identity_without_double_counting() {
    let mut state = ProducerSenderState::new(2);
    let mut first = ready_batch("orders", 0);
    first.bytes = 128;
    first.pooled_buffer_bytes = 128;
    let mut duplicate = ready_batch("orders", 1);
    duplicate.bytes = 128;
    duplicate.pooled_buffer_bytes = 128;
    let first_partition = super::InFlightPartitionKey::from(&first);
    let duplicate_partition = super::InFlightPartitionKey::from(&duplicate);
    let dispatcher = crate::producer::dispatcher::ProducerDispatcher::new(
        WireClient::connect_with_brokers(ConnectionConfig::default(), "producer-test", []),
    )
    .delivery_timeout(Duration::from_millis(50));
    let accumulator = SharedAccumulator::with_config(
        AccumulatorConfig::default()
            .batch_size(128)
            .buffer_memory(256),
    );

    let first_dispatch = DrainedDispatch::new(
        dispatcher.clone(),
        vec![first],
        std::time::Instant::now(),
        vec![first_partition],
    );
    let _first_abort = state
        .spawn_observed_drained_dispatch(first_dispatch, |_| {})
        .expect("first dispatch should reserve identity");
    let duplicate_dispatch = DrainedDispatch::new(
        dispatcher,
        vec![duplicate],
        std::time::Instant::now(),
        vec![duplicate_partition],
    );
    let error = state
        .spawn_observed_drained_dispatch(duplicate_dispatch, |_| {})
        .expect_err("duplicate in-flight identity should fail");

    assert!(matches!(error, ProducerError::BatchLifecycle(_)));
    let snapshot = state.queue_snapshot(&accumulator);
    assert_eq!(snapshot.incomplete_batches, 1);
    assert_eq!(snapshot.buffered_bytes, 128);
}

#[tokio::test]
async fn terminal_dispatch_rejects_stale_batch_identity_after_completion() {
    let mut state = ProducerSenderState::new(2);
    let mut first = ready_batch("orders", 0);
    first.bytes = 128;
    first.pooled_buffer_bytes = 128;
    let mut stale_batch = ready_batch("orders", 1);
    stale_batch.bytes = 128;
    stale_batch.pooled_buffer_bytes = 128;
    let first_partition = super::InFlightPartitionKey::from(&first);
    let stale_partition = super::InFlightPartitionKey::from(&stale_batch);
    let dispatcher = crate::producer::dispatcher::ProducerDispatcher::new(
        WireClient::connect_with_brokers(ConnectionConfig::default(), "producer-test", []),
    )
    .delivery_timeout(Duration::ZERO);
    let accumulator = SharedAccumulator::with_config(
        AccumulatorConfig::default()
            .batch_size(128)
            .buffer_memory(256),
    );

    let first_dispatch = DrainedDispatch::new(
        dispatcher.clone(),
        vec![first],
        std::time::Instant::now(),
        vec![first_partition],
    );
    let _first_abort = state
        .spawn_observed_drained_dispatch(first_dispatch, |_| {})
        .expect("first dispatch should reserve identity");
    let result = state
        .wait_for_handled_dispatch(
            &SharedAccumulator::with_config(AccumulatorConfig::default()),
            false,
            |_| {},
            || {},
        )
        .await;
    assert!(matches!(
        result,
        Err(ProducerError::DeliveryTimeout {
            topic,
            partition: 0,
        }) if topic == "orders"
    ));

    let stale_dispatch = DrainedDispatch::new(
        dispatcher,
        vec![stale_batch],
        std::time::Instant::now(),
        vec![stale_partition],
    );
    let error = state
        .spawn_observed_drained_dispatch(stale_dispatch, |_| {})
        .expect_err("terminal identity should not be dispatched again");

    assert!(matches!(error, ProducerError::BatchLifecycle(_)));
    let snapshot = state.queue_snapshot(&accumulator);
    assert_eq!(snapshot.incomplete_batches, 0);
    assert_eq!(snapshot.buffered_bytes, 0);
}

#[tokio::test]
async fn terminal_dispatch_completes_accumulator_batch_identity() {
    let mut state = ProducerSenderState::new(2);
    let now = std::time::Instant::now();
    let accumulator = SharedAccumulator::with_config(
        AccumulatorConfig::default()
            .batch_size(128)
            .buffer_memory(256),
    );
    let _status = accumulator
        .append_with_status_at(
            ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")),
            now,
        )
        .expect("append buffered batch");
    let mut batches = accumulator.drain_all();
    assert_eq!(batches.len(), 1);
    let mut stale_batch = crate::producer::ReadyBatch {
        identity: batches[0].identity,
        topic: batches[0].topic.clone(),
        partition: batches[0].partition,
        records: vec![ProducerRecord::new("orders", 0).value(Bytes::from_static(b"stale"))],
        delivery: None,
        bytes: batches[0].bytes,
        pooled_buffer_bytes: batches[0].pooled_buffer_bytes(),
        first_append_at: batches[0].first_append_at,
        producer_state: None,
    };
    batches[0].bytes = 128;
    batches[0].pooled_buffer_bytes = 128;
    stale_batch.bytes = 128;
    stale_batch.pooled_buffer_bytes = 128;
    let partition = super::InFlightPartitionKey::from(&batches[0]);
    let dispatcher = crate::producer::dispatcher::ProducerDispatcher::new(
        WireClient::connect_with_brokers(ConnectionConfig::default(), "producer-test", []),
    )
    .delivery_timeout(Duration::ZERO);

    let dispatch = DrainedDispatch::new(dispatcher, batches, now, vec![partition]);
    let _abort = state
        .spawn_observed_drained_dispatch(dispatch, |_| {})
        .expect("observed drained dispatch should spawn");
    let result = state
        .wait_for_handled_dispatch(&accumulator, false, |_| {}, || {})
        .await;
    assert!(matches!(
        result,
        Err(ProducerError::DeliveryTimeout {
            topic,
            partition: 0,
        }) if topic == "orders"
    ));

    let error = accumulator
        .requeue_front(vec![stale_batch])
        .expect_err("terminal dispatch should complete accumulator identity");
    assert!(matches!(error, ProducerError::BatchLifecycle(_)));
    assert_eq!(accumulator.buffered_batches(), 0);
    assert_eq!(accumulator.buffered_bytes(), 0);
}

#[test]
fn completed_batch_identity_tombstones_are_bounded_for_long_running_sender() {
    let mut state = ProducerSenderState::new(1);
    let tombstone_limit = 4096usize;
    let completed = tombstone_limit + 16;

    for index in 0..completed {
        state.mark_completed_batch_identities([ReadyBatchIdentity::test(
            u64::try_from(index).expect("test index should fit"),
        )]);
    }

    assert_eq!(state.completed_batch_identities.len(), tombstone_limit);
    assert!(
        !state
            .completed_batch_identities
            .contains(&ReadyBatchIdentity::test(0))
    );
    assert!(
        state
            .completed_batch_identities
            .contains(&ReadyBatchIdentity::test(
                u64::try_from(completed - 1).expect("test index should fit"),
            ))
    );
    assert_eq!(state.pending_accumulator_completions.len(), completed);
}

#[tokio::test]
async fn queue_snapshot_reports_buffer_available_across_in_flight_release() {
    let mut state = ProducerSenderState::new(1);
    let now = std::time::Instant::now();
    let accumulator = SharedAccumulator::with_config(
        AccumulatorConfig::default()
            .batch_size(128)
            .buffer_memory(160),
    );
    let _status = accumulator
        .append_with_status_at(
            ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")),
            now,
        )
        .expect("append buffered batch");

    let buffered = state.queue_snapshot(&accumulator);
    assert_eq!(buffered.buffered_bytes, 128);
    assert_eq!(buffered.buffer_available_bytes, 32);

    let mut batches = accumulator.drain_ready(now);
    assert_eq!(batches.len(), 1);
    batches[0].bytes = 128;
    batches[0].pooled_buffer_bytes = 128;
    let partition = super::InFlightPartitionKey::from(&batches[0]);
    let dispatcher = crate::producer::dispatcher::ProducerDispatcher::new(
        WireClient::connect_with_brokers(ConnectionConfig::default(), "producer-test", []),
    )
    .delivery_timeout(Duration::ZERO);
    let dispatch = DrainedDispatch::new(dispatcher, batches, now, vec![partition]);
    let _abort = state
        .spawn_observed_drained_dispatch(dispatch, |_| {})
        .expect("observed drained dispatch should spawn");

    let in_flight = state.queue_snapshot(&accumulator);
    assert_eq!(in_flight.buffered_bytes, 128);
    assert_eq!(in_flight.buffer_available_bytes, 32);

    let result = state
        .wait_for_handled_dispatch(
            &SharedAccumulator::with_config(AccumulatorConfig::default()),
            false,
            |_| {},
            || {},
        )
        .await;
    assert!(matches!(
        result,
        Err(ProducerError::DeliveryTimeout {
            topic,
            partition: 0,
        }) if topic == "orders"
    ));

    let released = state.queue_snapshot(&accumulator);
    assert_eq!(released.buffered_bytes, 0);
    assert_eq!(released.buffer_available_bytes, 160);
}

#[tokio::test]
async fn start_dispatch_selection_requeues_deferred_and_spawns_dispatchable_batches() {
    let mut state = ProducerSenderState::new(1);
    let dispatchable = ready_batch("orders", 0);
    let deferred = ready_batch("orders", 1);
    let partition = super::InFlightPartitionKey::from(&dispatchable);
    let dispatcher = crate::producer::dispatcher::ProducerDispatcher::new(
        WireClient::connect_with_brokers(ConnectionConfig::default(), "producer-test", []),
    )
    .delivery_timeout(Duration::ZERO);
    let accumulator = SharedAccumulator::with_config(AccumulatorConfig::default());
    let selection = DispatchSelection {
        dispatchable: vec![dispatchable],
        deferred: vec![deferred],
        partitions: vec![partition],
    };
    let mut observed_records = 0;

    let started = state
        .start_dispatch_selection(
            &accumulator,
            DispatchSelectionStart::new(dispatcher, selection, std::time::Instant::now()),
            |batches| {
                observed_records = batches.iter().map(|batch| batch.records.len()).sum();
            },
        )
        .expect("dispatch selection should start");

    assert!(matches!(started, DispatchStart::Spawned));
    assert_eq!(observed_records, 1);
    assert_eq!(accumulator.buffered_records(), 1);
    let blocked = state.select_dispatchable_batches(vec![ready_batch("orders", 0)]);
    assert!(blocked.dispatchable.is_empty());

    let completed = state
        .wait_for_completed_dispatch()
        .await
        .expect("in-flight task should be present")
        .expect("completed dispatch should not panic");
    assert!(matches!(
        completed.outcome,
        DispatchOutcome::Delivered(Err(ProducerError::DeliveryTimeout {
            topic,
            partition: 0,
        })) if topic == "orders"
    ));
}

#[tokio::test]
async fn prepare_dispatch_batches_selects_and_prepares_dispatchable_batches() {
    let mut state = ProducerSenderState::new(1);
    state.reserve_partitions_for_dispatch(&[ready_batch("orders", 0)]);
    let dispatcher = crate::producer::dispatcher::ProducerDispatcher::new(
        WireClient::connect_with_brokers(ConnectionConfig::default(), "producer-test", []),
    );

    let selection = state
        .prepare_dispatch_batches(
            &dispatcher,
            vec![ready_batch("orders", 0), ready_batch("orders", 1)],
        )
        .await
        .expect("non-idempotent dispatcher preparation should not need metadata");

    assert_eq!(selection.dispatchable.len(), 1);
    assert_eq!(selection.dispatchable[0].partition, 1);
    assert_eq!(selection.deferred.len(), 1);
    assert_eq!(selection.deferred[0].partition, 0);
    assert_eq!(selection.partitions.len(), 1);
}

#[tokio::test]
async fn drain_ready_dispatch_batches_drains_accumulator_before_preparing() {
    let state = ProducerSenderState::new(1);
    let dispatcher = crate::producer::dispatcher::ProducerDispatcher::new(
        WireClient::connect_with_brokers(ConnectionConfig::default(), "producer-test", []),
    );
    let accumulator = SharedAccumulator::with_config(
        AccumulatorConfig::default()
            .batch_size(1)
            .buffer_memory(16 * 1024),
    );
    accumulator
        .append_at(
            ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")),
            std::time::Instant::now(),
        )
        .expect("append record");

    let selection = state
        .drain_ready_dispatch_batches(&dispatcher, &accumulator, std::time::Instant::now())
        .await
        .expect("ready non-idempotent batches should prepare")
        .expect("ready batch should produce a selection");

    assert_eq!(selection.dispatchable.len(), 1);
    assert_eq!(selection.dispatchable[0].partition, 0);
    assert!(selection.deferred.is_empty());
    assert_eq!(accumulator.buffered_records(), 0);
}

#[tokio::test]
async fn sender_state_waits_for_dispatch_slot_only_when_limit_is_reached() {
    let mut state = ProducerSenderState::new(2);
    let _abort = state.spawn_in_flight(async {
        TimedDispatchOutcome {
            outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
            latency: Duration::ZERO,
            partitions: Vec::new(),
        }
    });

    assert!(state.wait_for_dispatch_slot().await.is_none());

    let _abort = state.spawn_in_flight(async {
        TimedDispatchOutcome {
            outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
            latency: Duration::ZERO,
            partitions: Vec::new(),
        }
    });

    let joined = state
        .wait_for_dispatch_slot()
        .await
        .expect("full sender state should wait for one task")
        .expect("in-flight task should not panic");
    assert!(matches!(joined.outcome, DispatchOutcome::Delivered(Ok(_))));
    assert_eq!(state.in_flight_len(), 1);
}

#[tokio::test]
async fn sender_state_waits_for_ready_dispatch_slot_after_readiness() {
    let mut state = ProducerSenderState::new(1);
    let empty = SharedAccumulator::with_config(AccumulatorConfig::default());
    let idle = state
        .wait_for_ready_dispatch_slot(&empty, std::time::Instant::now())
        .await;
    assert!(matches!(idle, ReadyDispatchSlot::Idle));

    let ready = ready_accumulator();
    let slot = state
        .wait_for_ready_dispatch_slot(&ready, std::time::Instant::now())
        .await;
    assert!(matches!(slot, ReadyDispatchSlot::Ready { completed: None }));

    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let _abort = state.spawn_in_flight(async {
        let _released = release_rx.await;
        TimedDispatchOutcome {
            outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
            latency: Duration::ZERO,
            partitions: Vec::new(),
        }
    });
    let blocked = tokio::time::timeout(
        Duration::from_millis(10),
        state.wait_for_ready_dispatch_slot(&ready, std::time::Instant::now()),
    )
    .await;
    assert!(blocked.is_err());

    release_tx.send(()).expect("release in-flight task");
    let unblocked = state
        .wait_for_ready_dispatch_slot(&ready, std::time::Instant::now())
        .await;
    assert!(matches!(
        unblocked,
        ReadyDispatchSlot::Ready {
            completed: Some(Ok(_))
        }
    ));
}

#[tokio::test]
async fn sender_state_prepares_ready_dispatch_without_draining_before_completion_is_processed() {
    let dispatcher = crate::producer::dispatcher::ProducerDispatcher::new(
        WireClient::connect_with_brokers(ConnectionConfig::default(), "producer-test", []),
    );

    let mut empty_state = ProducerSenderState::new(1);
    let empty = SharedAccumulator::with_config(AccumulatorConfig::default());
    let idle = empty_state
        .prepare_ready_dispatch_batches(&dispatcher, &empty, std::time::Instant::now())
        .await
        .expect("empty accumulator should not need prepare");
    assert!(matches!(idle, PreparedReadyDispatch::Idle));

    let mut ready_state = ProducerSenderState::new(1);
    let ready = ready_accumulator();
    let prepared = ready_state
        .prepare_ready_dispatch_batches(&dispatcher, &ready, std::time::Instant::now())
        .await
        .expect("ready accumulator should prepare");
    assert!(matches!(prepared, PreparedReadyDispatch::Prepared(_)));
    assert_eq!(ready.buffered_records(), 0);

    let mut blocked_state = ProducerSenderState::new(1);
    let _abort = blocked_state.spawn_in_flight(async {
        TimedDispatchOutcome {
            outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
            latency: Duration::ZERO,
            partitions: Vec::new(),
        }
    });
    let blocked = ready_accumulator();
    let pending = blocked_state
        .prepare_ready_dispatch_batches(&dispatcher, &blocked, std::time::Instant::now())
        .await
        .expect("completed dispatch should be returned before draining");
    assert!(matches!(
        pending,
        PreparedReadyDispatch::PendingCompletion(Ok(_))
    ));
    assert_eq!(blocked.buffered_records(), 1);
}

#[tokio::test]
async fn prepare_ready_dispatch_or_requeue_restores_batches_on_prepare_error() {
    let dispatcher = crate::producer::dispatcher::ProducerDispatcher::with_config(
        WireClient::connect_with_brokers(ConnectionConfig::default(), "producer-test", []),
        ProducerRuntimeConfig::default(),
    );
    let mut state = ProducerSenderState::new(1);
    let accumulator = ready_accumulator();

    let result = state
        .prepare_ready_dispatch_batches_or_requeue(
            &dispatcher,
            &accumulator,
            std::time::Instant::now(),
        )
        .await;

    assert!(result.is_err());
    assert_eq!(accumulator.buffered_records(), 1);
}

#[tokio::test]
async fn sender_state_prepares_all_dispatch_without_draining_before_completion_is_processed() {
    let dispatcher = crate::producer::dispatcher::ProducerDispatcher::new(
        WireClient::connect_with_brokers(ConnectionConfig::default(), "producer-test", []),
    );

    let mut empty_state = ProducerSenderState::new(1);
    let empty = SharedAccumulator::with_config(AccumulatorConfig::default());
    let idle = empty_state
        .prepare_all_dispatch_batches(&dispatcher, &empty)
        .await
        .expect("empty accumulator should not need prepare");
    assert!(matches!(idle, PreparedAllDispatch::Empty));

    let mut ready_state = ProducerSenderState::new(1);
    let ready = ready_accumulator();
    let prepared = ready_state
        .prepare_all_dispatch_batches(&dispatcher, &ready)
        .await
        .expect("buffered accumulator should prepare");
    assert!(matches!(prepared, PreparedAllDispatch::Prepared(_)));
    assert_eq!(ready.buffered_records(), 0);

    let mut blocked_state = ProducerSenderState::new(1);
    let _abort = blocked_state.spawn_in_flight(async {
        TimedDispatchOutcome {
            outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
            latency: Duration::ZERO,
            partitions: Vec::new(),
        }
    });
    let blocked = ready_accumulator();
    let pending = blocked_state
        .prepare_all_dispatch_batches(&dispatcher, &blocked)
        .await
        .expect("completed dispatch should be returned before drain-all");
    assert!(matches!(
        pending,
        PreparedAllDispatch::PendingCompletion(Ok(_))
    ));
    assert_eq!(blocked.buffered_records(), 1);
}

#[tokio::test]
async fn prepare_all_dispatch_or_requeue_restores_batches_on_prepare_error() {
    let dispatcher = crate::producer::dispatcher::ProducerDispatcher::with_config(
        WireClient::connect_with_brokers(ConnectionConfig::default(), "producer-test", []),
        ProducerRuntimeConfig::default(),
    );
    let mut state = ProducerSenderState::new(1);
    let accumulator = ready_accumulator();

    let result = state
        .prepare_all_dispatch_batches_or_requeue(&dispatcher, &accumulator)
        .await;

    assert!(result.is_err());
    assert_eq!(accumulator.buffered_records(), 1);
}

#[test]
fn completing_joined_dispatch_releases_reserved_partitions() {
    let mut state = ProducerSenderState::new(1);
    let reserved = ready_batch("orders", 0);
    let partition = super::InFlightPartitionKey::from(&reserved);
    state.reserve_dispatch_partitions(std::slice::from_ref(&partition));

    let blocked = state.select_dispatchable_batches(vec![ready_batch("orders", 0)]);
    assert!(blocked.dispatchable.is_empty());
    assert_eq!(blocked.deferred.len(), 1);

    let completed = state
        .complete_joined_dispatch(Ok(TimedDispatchOutcome {
            outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
            latency: Duration::ZERO,
            partitions: vec![partition],
        }))
        .expect("completed dispatch should not panic");
    assert!(matches!(
        completed.outcome,
        DispatchOutcome::Delivered(Ok(_))
    ));

    let unblocked = state.select_dispatchable_batches(vec![ready_batch("orders", 0)]);
    assert_eq!(unblocked.dispatchable.len(), 1);
    assert!(unblocked.deferred.is_empty());
}

#[tokio::test]
async fn complete_dispatch_result_normalizes_join_errors_and_releases_partitions() {
    let mut state = ProducerSenderState::new(1);
    let reserved = ready_batch("orders", 0);
    let partition = super::InFlightPartitionKey::from(&reserved);
    let partition_for_task = partition.clone();
    let _abort = state
        .spawn_dispatch_task(std::slice::from_ref(&partition), async move {
            TimedDispatchOutcome {
                outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
                latency: Duration::ZERO,
                partitions: vec![partition_for_task],
            }
        })
        .expect("dispatch task should spawn");

    let joined = state
        .wait_for_next_dispatch()
        .await
        .expect("in-flight task should be present");
    let completed = state
        .complete_dispatch_result(joined)
        .expect("completed dispatch should not panic");
    assert!(matches!(
        completed.outcome,
        DispatchOutcome::Delivered(Ok(_))
    ));
    let unblocked = state.select_dispatchable_batches(vec![ready_batch("orders", 0)]);
    assert_eq!(unblocked.dispatchable.len(), 1);

    let _abort = state.spawn_in_flight(async {
        panic!("dispatch task panic");
    });
    let joined = state
        .wait_for_next_dispatch()
        .await
        .expect("panicked task should be present");
    assert!(matches!(
        state.complete_dispatch_result(joined),
        Err(ProducerError::DispatchTask(_))
    ));
}

#[tokio::test]
async fn complete_dispatch_result_releases_reserved_partitions_after_dispatch_panic() {
    let mut state = ProducerSenderState::new_with_idempotent_ordering(1, true);
    let reserved = ready_batch("orders", 0);
    let partition = super::InFlightPartitionKey::from(&reserved);
    let _abort = state
        .spawn_dispatch_task(std::slice::from_ref(&partition), async {
            panic!("dispatch task panic");
        })
        .expect("dispatch task should spawn");

    let blocked = state.select_dispatchable_batches(vec![ready_batch("orders", 0)]);
    assert!(blocked.dispatchable.is_empty());
    assert_eq!(blocked.deferred.len(), 1);

    let joined = state
        .wait_for_next_dispatch()
        .await
        .expect("panicked task should be present");
    assert!(matches!(
        state.complete_dispatch_result(joined),
        Err(ProducerError::DispatchTask(_))
    ));

    let unblocked = state.select_dispatchable_batches(vec![ready_batch("orders", 0)]);
    assert_eq!(unblocked.dispatchable.len(), 1);
    assert!(unblocked.deferred.is_empty());
}

#[tokio::test]
async fn wait_for_completed_dispatch_returns_normalized_completion() {
    let mut state = ProducerSenderState::new(1);
    assert!(state.wait_for_completed_dispatch().await.is_none());

    let reserved = ready_batch("orders", 0);
    let partition = super::InFlightPartitionKey::from(&reserved);
    let partition_for_task = partition.clone();
    let _abort = state
        .spawn_dispatch_task(std::slice::from_ref(&partition), async move {
            TimedDispatchOutcome {
                outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
                latency: Duration::ZERO,
                partitions: vec![partition_for_task],
            }
        })
        .expect("dispatch task should spawn");

    let completed = state
        .wait_for_completed_dispatch()
        .await
        .expect("in-flight task should be present")
        .expect("in-flight task should not panic");
    assert!(matches!(
        completed.outcome,
        DispatchOutcome::Delivered(Ok(_))
    ));
    let unblocked = state.select_dispatchable_batches(vec![ready_batch("orders", 0)]);
    assert_eq!(unblocked.dispatchable.len(), 1);

    let _abort = state.spawn_in_flight(async {
        panic!("dispatch task panic");
    });
    let completed = state
        .wait_for_completed_dispatch()
        .await
        .expect("panicked task should be present");
    assert!(matches!(completed, Err(ProducerError::DispatchTask(_))));
}

#[tokio::test]
async fn collect_completed_dispatches_returns_normalized_completions() {
    let mut state = ProducerSenderState::new(2);
    let _delivered = state.spawn_in_flight(async {
        TimedDispatchOutcome {
            outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
            latency: Duration::ZERO,
            partitions: Vec::new(),
        }
    });
    let _panicked = state.spawn_in_flight(async {
        panic!("dispatch task panic");
    });
    tokio::task::yield_now().await;

    let completed = state.collect_completed_dispatches();

    assert_eq!(completed.len(), 2);
    assert_eq!(
        completed
            .iter()
            .filter(|result| matches!(result, Ok(TimedDispatchOutcome { .. })))
            .count(),
        1
    );
    assert_eq!(
        completed
            .iter()
            .filter(|result| matches!(result, Err(ProducerError::DispatchTask(_))))
            .count(),
        1
    );
}

#[tokio::test]
async fn handle_finished_dispatches_collects_and_handles_completed_tasks() {
    let mut state = ProducerSenderState::new(2);
    let accumulator = SharedAccumulator::with_config(AccumulatorConfig::default());
    let latency = Duration::from_millis(9);
    let batch = ready_batch("orders", 0);
    let mut observed_latencies = Vec::new();
    let mut observed_requeues = 0;

    let _delivered = state.spawn_in_flight(async move {
        TimedDispatchOutcome {
            outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
            latency,
            partitions: Vec::new(),
        }
    });
    let _requeued = state.spawn_in_flight(async move {
        TimedDispatchOutcome {
            outcome: DispatchOutcome::Requeue(vec![batch]),
            latency: Duration::ZERO,
            partitions: Vec::new(),
        }
    });
    tokio::task::yield_now().await;

    let result = state.handle_finished_dispatches(
        &accumulator,
        false,
        |duration| observed_latencies.push(duration),
        || observed_requeues += 1,
    );

    result.expect("non-flush requeue should not be an error");
    assert_eq!(observed_latencies, vec![latency]);
    assert_eq!(observed_requeues, 1);
    assert_eq!(accumulator.buffered_records(), 1);
    assert_eq!(state.in_flight_len(), 0);
}

#[tokio::test]
async fn producer_sender_handles_finished_dispatches_without_exposing_accumulator() {
    let mut sender = ProducerSender::new(AccumulatorConfig::default(), 2);
    let latency = Duration::from_millis(9);
    let batch = ready_batch("orders", 0);
    let mut observed_latencies = Vec::new();
    let mut observed_requeues = 0;

    let _delivered = sender.state.spawn_in_flight(async move {
        TimedDispatchOutcome {
            outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
            latency,
            partitions: Vec::new(),
        }
    });
    let _requeued = sender.state.spawn_in_flight(async move {
        TimedDispatchOutcome {
            outcome: DispatchOutcome::Requeue(vec![batch]),
            latency: Duration::ZERO,
            partitions: Vec::new(),
        }
    });
    tokio::task::yield_now().await;

    sender
        .handle_finished_dispatches(
            false,
            |duration| observed_latencies.push(duration),
            || observed_requeues += 1,
        )
        .expect("non-flush requeue should not be an error");

    assert_eq!(observed_latencies, vec![latency]);
    assert_eq!(observed_requeues, 1);
    assert_eq!(sender.accumulator.buffered_records(), 1);
    assert_eq!(sender.state.in_flight_len(), 0);
}

#[tokio::test]
async fn wait_for_handled_dispatch_handles_next_completed_task() {
    let mut state = ProducerSenderState::new(1);
    let accumulator = SharedAccumulator::with_config(AccumulatorConfig::default());
    let latency = Duration::from_millis(11);
    let mut observed_latencies = Vec::new();
    let mut observed_requeues = 0;

    let _delivered = state.spawn_in_flight(async move {
        TimedDispatchOutcome {
            outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
            latency,
            partitions: Vec::new(),
        }
    });

    state
        .wait_for_handled_dispatch(
            &accumulator,
            false,
            |duration| observed_latencies.push(duration),
            || observed_requeues += 1,
        )
        .await
        .expect("delivered completion should be ok");

    assert_eq!(observed_latencies, vec![latency]);
    assert_eq!(observed_requeues, 0);
    assert_eq!(accumulator.buffered_records(), 0);
    assert_eq!(state.in_flight_len(), 0);
}

#[tokio::test]
async fn producer_sender_waits_for_handled_dispatch_without_exposing_accumulator() {
    let mut sender = ProducerSender::new(AccumulatorConfig::default(), 1);
    let latency = Duration::from_millis(11);
    let mut observed_latencies = Vec::new();
    let mut observed_requeues = 0;

    let _delivered = sender.state.spawn_in_flight(async move {
        TimedDispatchOutcome {
            outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
            latency,
            partitions: Vec::new(),
        }
    });

    sender
        .wait_for_handled_dispatch(
            false,
            |duration| observed_latencies.push(duration),
            || observed_requeues += 1,
        )
        .await
        .expect("delivered completion should be ok");

    assert_eq!(observed_latencies, vec![latency]);
    assert_eq!(observed_requeues, 0);
    assert_eq!(sender.accumulator.buffered_records(), 0);
    assert_eq!(sender.state.in_flight_len(), 0);
}

#[tokio::test]
async fn apply_ready_dispatch_progress_handles_completion_and_prepared_selection() {
    let mut state = ProducerSenderState::new(1);
    let accumulator = SharedAccumulator::with_config(AccumulatorConfig::default());
    let latency = Duration::from_millis(13);
    let mut observed_latencies = Vec::new();
    let mut observed_requeues = 0;
    let mut observed_batches = Vec::new();

    let progress = state.apply_ready_dispatch_progress(
        &accumulator,
        ReadyDispatchApplication::new(
            test_dispatcher(),
            PreparedReadyDispatch::PendingCompletion(Ok(TimedDispatchOutcome {
                outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
                latency,
                partitions: Vec::new(),
            })),
            std::time::Instant::now(),
        ),
        ReadyDispatchObservers::new(
            |duration| observed_latencies.push(duration),
            || observed_requeues += 1,
            |batches: &[crate::producer::ReadyBatch]| observed_batches.push(batches.len()),
        ),
    );

    assert!(matches!(progress, Ok(ReadyDispatchProgress::Continue)));
    assert_eq!(observed_latencies, vec![latency]);
    assert_eq!(observed_requeues, 0);
    assert!(observed_batches.is_empty());

    let batch = ready_batch("orders", 0);
    let partition = super::InFlightPartitionKey::from(&batch);
    let selection = DispatchSelection {
        dispatchable: vec![batch],
        deferred: Vec::new(),
        partitions: vec![partition],
    };
    let progress = state.apply_ready_dispatch_progress(
        &accumulator,
        ReadyDispatchApplication::new(
            test_dispatcher(),
            PreparedReadyDispatch::Prepared(selection),
            std::time::Instant::now(),
        ),
        ReadyDispatchObservers::new(
            |duration| observed_latencies.push(duration),
            || observed_requeues += 1,
            |batches: &[crate::producer::ReadyBatch]| observed_batches.push(batches.len()),
        ),
    );

    assert!(matches!(
        progress,
        Ok(ReadyDispatchProgress::Started(DispatchStart::Spawned))
    ));
    assert_eq!(observed_batches, vec![1]);
    assert_eq!(state.in_flight_len(), 1);
}

#[tokio::test]
async fn drive_ready_dispatch_progress_prepares_and_applies_ready_batches() {
    let mut state = ProducerSenderState::new(1);
    let accumulator = ready_accumulator();
    let mut observed_batches = Vec::new();

    let progress = state
        .drive_ready_dispatch_progress(
            &test_dispatcher(),
            &accumulator,
            std::time::Instant::now(),
            ReadyDispatchObservers::new(
                |_| {},
                || {},
                |batches: &[crate::producer::ReadyBatch]| {
                    observed_batches.push(batches.len());
                },
            ),
        )
        .await
        .expect("ready dispatch should be applied");

    assert!(matches!(
        progress,
        ReadyDispatchProgress::Started(DispatchStart::Spawned)
    ));
    assert_eq!(observed_batches, vec![1]);
    assert_eq!(accumulator.buffered_records(), 0);
    assert_eq!(state.in_flight_len(), 1);
}

#[tokio::test]
async fn drive_ready_dispatch_until_blocked_handles_completion_then_starts_ready_batch() {
    let mut state = ProducerSenderState::new(1);
    let accumulator = ready_accumulator();
    let latency = Duration::from_millis(17);
    let mut observed_latencies = Vec::new();
    let mut observed_requeues = 0;
    let mut observed_batches = Vec::new();
    let _in_flight = state.spawn_in_flight(async move {
        TimedDispatchOutcome {
            outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
            latency,
            partitions: Vec::new(),
        }
    });

    let _dispatched = state
        .drive_ready_dispatch_until_blocked(
            &test_dispatcher(),
            &accumulator,
            ReadyDispatchObservers::new(
                |observed_latency| observed_latencies.push(observed_latency),
                || observed_requeues += 1,
                |batches: &[crate::producer::ReadyBatch]| {
                    observed_batches.push(batches.len());
                },
            ),
        )
        .await
        .expect("ready dispatch loop should consume completion and start batch");

    assert_eq!(observed_latencies, vec![latency]);
    assert_eq!(observed_requeues, 0);
    assert_eq!(observed_batches, vec![1]);
    assert_eq!(accumulator.buffered_records(), 0);
    assert_eq!(state.in_flight_len(), 1);
}

#[tokio::test]
async fn producer_sender_drives_ready_dispatch_until_blocked() {
    let mut sender = ProducerSender::new(
        AccumulatorConfig::default()
            .batch_size(1)
            .linger(Duration::ZERO),
        1,
    );
    sender
        .accumulator
        .append_at(
            ProducerRecord::new("orders", 0).value(Bytes::from_static(b"value")),
            std::time::Instant::now(),
        )
        .expect("append ready batch");
    let latency = Duration::from_millis(17);
    let mut observed_latencies = Vec::new();
    let mut observed_requeues = 0;
    let mut observed_batches = Vec::new();
    let _in_flight = sender.state.spawn_in_flight(async move {
        TimedDispatchOutcome {
            outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
            latency,
            partitions: Vec::new(),
        }
    });

    let _dispatched = sender
        .drive_ready_dispatch_until_blocked(ReadyDispatchObservers::new(
            |observed_latency| observed_latencies.push(observed_latency),
            || observed_requeues += 1,
            |batches: &[crate::producer::ReadyBatch]| {
                observed_batches.push(batches.len());
            },
        ))
        .await
        .expect("ready dispatch loop should consume completion and start batch");

    assert_eq!(observed_latencies, vec![latency]);
    assert_eq!(observed_requeues, 0);
    assert_eq!(observed_batches, vec![1]);
    assert_eq!(sender.accumulator.buffered_records(), 0);
    assert_eq!(sender.state.in_flight_len(), 1);
}

#[tokio::test]
async fn apply_all_dispatch_progress_handles_empty_completion_and_prepared_selection() {
    let mut state = ProducerSenderState::new(1);
    let accumulator = SharedAccumulator::with_config(AccumulatorConfig::default());
    let latency = Duration::from_millis(17);
    let mut observed_latencies = Vec::new();
    let mut observed_requeues = 0;
    let mut observed_batches = Vec::new();

    let progress = state.apply_all_dispatch_progress(
        &accumulator,
        AllDispatchApplication::new(
            test_dispatcher(),
            PreparedAllDispatch::Empty,
            std::time::Instant::now(),
        ),
        ReadyDispatchObservers::new(
            |duration| observed_latencies.push(duration),
            || observed_requeues += 1,
            |batches: &[crate::producer::ReadyBatch]| observed_batches.push(batches.len()),
        ),
    );

    assert!(matches!(progress, Ok(AllDispatchProgress::Empty)));
    assert!(observed_latencies.is_empty());
    assert_eq!(observed_requeues, 0);
    assert!(observed_batches.is_empty());

    let progress = state.apply_all_dispatch_progress(
        &accumulator,
        AllDispatchApplication::new(
            test_dispatcher(),
            PreparedAllDispatch::PendingCompletion(Ok(TimedDispatchOutcome {
                outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
                latency,
                partitions: Vec::new(),
            })),
            std::time::Instant::now(),
        ),
        ReadyDispatchObservers::new(
            |duration| observed_latencies.push(duration),
            || observed_requeues += 1,
            |batches: &[crate::producer::ReadyBatch]| observed_batches.push(batches.len()),
        ),
    );

    assert!(matches!(progress, Ok(AllDispatchProgress::Continue)));
    assert_eq!(observed_latencies, vec![latency]);

    let batch = ready_batch("orders", 0);
    let partition = super::InFlightPartitionKey::from(&batch);
    let selection = DispatchSelection {
        dispatchable: vec![batch],
        deferred: Vec::new(),
        partitions: vec![partition],
    };
    let progress = state.apply_all_dispatch_progress(
        &accumulator,
        AllDispatchApplication::new(
            test_dispatcher(),
            PreparedAllDispatch::Prepared(selection),
            std::time::Instant::now(),
        ),
        ReadyDispatchObservers::new(
            |duration| observed_latencies.push(duration),
            || observed_requeues += 1,
            |batches: &[crate::producer::ReadyBatch]| observed_batches.push(batches.len()),
        ),
    );

    assert!(matches!(
        progress,
        Ok(AllDispatchProgress::Started(DispatchStart::Spawned))
    ));
    assert_eq!(observed_batches, vec![1]);
    assert_eq!(state.in_flight_len(), 1);
}

#[tokio::test]
async fn drive_all_dispatch_progress_prepares_and_applies_buffered_batches() {
    let mut state = ProducerSenderState::new(1);
    let accumulator = ready_accumulator();
    let mut observed_batches = Vec::new();

    let progress = state
        .drive_all_dispatch_progress(
            &test_dispatcher(),
            &accumulator,
            std::time::Instant::now(),
            ReadyDispatchObservers::new(
                |_| {},
                || {},
                |batches: &[crate::producer::ReadyBatch]| {
                    observed_batches.push(batches.len());
                },
            ),
        )
        .await
        .expect("all dispatch should be applied");

    assert!(matches!(
        progress,
        AllDispatchProgress::Started(DispatchStart::Spawned)
    ));
    assert_eq!(observed_batches, vec![1]);
    assert_eq!(accumulator.buffered_records(), 0);
    assert_eq!(state.in_flight_len(), 1);
}

#[tokio::test]
async fn producer_sender_drives_flush_until_complete() {
    let mut sender = ProducerSender::new(AccumulatorConfig::default(), 1);
    let latency = Duration::from_millis(19);
    let mut observed_latencies = Vec::new();
    let mut observed_requeues = 0;
    let mut observed_batches = Vec::new();
    let _in_flight = sender.state.spawn_in_flight(async move {
        TimedDispatchOutcome {
            outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
            latency,
            partitions: Vec::new(),
        }
    });
    tokio::task::yield_now().await;

    sender
        .drive_flush_until_complete(ReadyDispatchObservers::new(
            |observed_latency| observed_latencies.push(observed_latency),
            || observed_requeues += 1,
            |batches: &[crate::producer::ReadyBatch]| {
                observed_batches.push(batches.len());
            },
        ))
        .await
        .expect("flush loop should dispatch and wait for completion");

    assert_eq!(observed_latencies, vec![latency]);
    assert_eq!(observed_requeues, 0);
    assert!(observed_batches.is_empty());
    assert_eq!(sender.accumulator.buffered_records(), 0);
    assert_eq!(sender.state.in_flight_len(), 0);
}

#[tokio::test]
async fn drive_flush_dispatch_progress_maps_empty_and_spawned_steps() {
    let mut state = ProducerSenderState::new(1);
    let accumulator = SharedAccumulator::with_config(AccumulatorConfig::default());
    let mut observed_batches = Vec::new();

    let progress = state
        .drive_flush_dispatch_progress(
            &test_dispatcher(),
            &accumulator,
            std::time::Instant::now(),
            ReadyDispatchObservers::new(
                |_| {},
                || {},
                |batches: &[crate::producer::ReadyBatch]| {
                    observed_batches.push(batches.len());
                },
            ),
        )
        .await
        .expect("empty flush step should complete");

    assert_eq!(progress, FlushDispatchProgress::Complete);
    assert!(observed_batches.is_empty());

    let accumulator = ready_accumulator();
    let progress = state
        .drive_flush_dispatch_progress(
            &test_dispatcher(),
            &accumulator,
            std::time::Instant::now(),
            ReadyDispatchObservers::new(
                |_| {},
                || {},
                |batches: &[crate::producer::ReadyBatch]| {
                    observed_batches.push(batches.len());
                },
            ),
        )
        .await
        .expect("spawned flush step should continue");

    assert_eq!(progress, FlushDispatchProgress::Continue);
    assert_eq!(observed_batches, vec![1]);
    assert_eq!(accumulator.buffered_records(), 0);
    assert_eq!(state.in_flight_len(), 1);
}

#[tokio::test]
async fn drive_flush_dispatch_step_waits_for_deferred_in_flight_partition() {
    // max.in.flight=1 so the single in-flight request fills the partition's pipeline
    // depth and the next same-partition batch is deferred (the flush must wait).
    let mut state = ProducerSenderState::new(1);
    state.idempotent_ordering = true;
    let accumulator = ready_accumulator();
    let blocked = ready_batch("orders", 0);
    let partition = super::InFlightPartitionKey::from(&blocked);
    let partitions = vec![partition.clone()];
    let latency = Duration::from_millis(13);
    let mut observed_latencies = Vec::new();
    let mut observed_requeues = 0;
    let mut observed_batches = Vec::new();
    state.reserve_dispatch_partitions(std::slice::from_ref(&partition));
    let (complete_tx, complete_rx) = tokio::sync::oneshot::channel();
    let _abort = state.spawn_in_flight(async move {
        let _received = complete_rx.await;
        TimedDispatchOutcome {
            outcome: DispatchOutcome::Delivered(Ok(Vec::new())),
            latency,
            partitions,
        }
    });
    let _complete = tokio::spawn(async move {
        tokio::task::yield_now().await;
        let _sent = complete_tx.send(());
    });

    let progress = state
        .drive_flush_dispatch_step(
            &test_dispatcher(),
            &accumulator,
            std::time::Instant::now(),
            ReadyDispatchObservers::new(
                |observed_latency| observed_latencies.push(observed_latency),
                || observed_requeues += 1,
                |batches: &[crate::producer::ReadyBatch]| {
                    observed_batches.push(batches.len());
                },
            ),
        )
        .await
        .expect("flush step should wait for blocked in-flight partition");

    assert_eq!(progress, FlushDispatchProgress::Continue);
    assert_eq!(observed_latencies, vec![latency]);
    assert_eq!(observed_requeues, 0);
    assert!(observed_batches.is_empty());
    assert_eq!(accumulator.buffered_records(), 1);
    assert!(!state.has_in_flight_dispatches());
    let unblocked = state.select_dispatchable_batches(vec![ready_batch("orders", 0)]);
    assert_eq!(unblocked.dispatchable.len(), 1);
}

#[test]
fn apply_flush_dispatch_progress_reports_incomplete_without_in_flight_dispatch() {
    let state = ProducerSenderState::new(1);

    let result =
        state.apply_flush_dispatch_progress(AllDispatchProgress::Started(DispatchStart::Empty));

    assert!(matches!(result, Err(ProducerError::FlushIncomplete)));
}

#[test]
fn handle_completed_dispatch_records_latency_and_propagates_delivered_error() {
    let accumulator = SharedAccumulator::with_config(AccumulatorConfig::default());
    let latency = Duration::from_millis(7);
    let mut observed_latency = None;
    let mut observed_requeues = 0;

    let result = ProducerSenderState::handle_completed_dispatch(
        &accumulator,
        CompletedDispatch::new(
            Ok(TimedDispatchOutcome {
                outcome: DispatchOutcome::Delivered(Err(ProducerError::Backpressure)),
                latency,
                partitions: Vec::new(),
            }),
            false,
        ),
        |duration| observed_latency = Some(duration),
        || observed_requeues += 1,
    );

    assert!(matches!(result, Err(ProducerError::Backpressure)));
    assert_eq!(observed_latency, Some(latency));
    assert_eq!(observed_requeues, 0);
    assert_eq!(accumulator.buffered_records(), 0);
}

#[test]
fn producer_sender_handles_completed_dispatch_without_exposing_accumulator() {
    let sender = ProducerSender::new(AccumulatorConfig::default(), 1);
    let batch = ready_batch("orders", 0);
    let mut observed_latency = None;
    let mut observed_requeues = 0;

    sender
        .handle_completed_dispatch(
            CompletedDispatch::new(
                Ok(TimedDispatchOutcome {
                    outcome: DispatchOutcome::Requeue(vec![batch]),
                    latency: Duration::from_millis(3),
                    partitions: Vec::new(),
                }),
                false,
            ),
            |duration| observed_latency = Some(duration),
            || observed_requeues += 1,
        )
        .expect("non-flush requeue should not be an error");

    assert_eq!(sender.accumulator.buffered_records(), 1);
    assert_eq!(observed_latency, None);
    assert_eq!(observed_requeues, 1);
}

#[test]
fn handle_completed_dispatch_requeues_batches_and_reports_flush_incomplete() {
    let accumulator = SharedAccumulator::with_config(AccumulatorConfig::default());
    let batch = ready_batch("orders", 0);
    let mut observed_latency = None;
    let mut observed_requeues = 0;

    ProducerSenderState::handle_completed_dispatch(
        &accumulator,
        CompletedDispatch::new(
            Ok(TimedDispatchOutcome {
                outcome: DispatchOutcome::Requeue(vec![batch]),
                latency: Duration::from_millis(3),
                partitions: Vec::new(),
            }),
            false,
        ),
        |duration| observed_latency = Some(duration),
        || observed_requeues += 1,
    )
    .expect("non-flush requeue should not be an error");
    assert_eq!(accumulator.buffered_records(), 1);
    assert_eq!(observed_latency, None);
    assert_eq!(observed_requeues, 1);

    let batch = accumulator.drain_all().pop().expect("requeued batch");
    let result = ProducerSenderState::handle_completed_dispatch(
        &accumulator,
        CompletedDispatch::new(
            Ok(TimedDispatchOutcome {
                outcome: DispatchOutcome::Requeue(vec![batch]),
                latency: Duration::ZERO,
                partitions: Vec::new(),
            }),
            true,
        ),
        |_| {},
        || observed_requeues += 1,
    );

    assert!(matches!(result, Err(ProducerError::FlushIncomplete)));
    assert_eq!(accumulator.buffered_records(), 1);
    assert_eq!(observed_requeues, 2);
}
