#![cfg(feature = "producer")]
//! Producer accumulator tests.

#![allow(
    clippy::expect_used,
    clippy::missing_assert_message,
    clippy::unwrap_used,
    reason = "Integration test fixtures fail fastest with contextual unwrap/expect calls."
)]

use std::time::{Duration, Instant};

use bytes::Bytes;
use kacrab::producer::{
    ProducerError, ProducerRecord,
    internals::{AccumulatorConfig, RecordAccumulator},
};

#[test]
fn accumulator_drains_batch_size_ready_records_by_topic_partition() {
    let mut accumulator = RecordAccumulator::new(
        AccumulatorConfig::default()
            .batch_size(1)
            .buffer_memory(16 * 1024),
    );
    accumulator
        .append(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")))
        .unwrap();
    accumulator
        .append(ProducerRecord::new("orders", 1).value(Bytes::from_static(b"b")))
        .unwrap();

    let ready = accumulator.drain_ready(Instant::now());

    assert_eq!(ready.len(), 2);
    assert!(
        ready
            .iter()
            .any(|batch| batch.topic == "orders" && batch.partition == 0)
    );
    assert!(
        ready
            .iter()
            .any(|batch| batch.topic == "orders" && batch.partition == 1)
    );
    assert_eq!(accumulator.buffered_bytes(), 0);
}

#[test]
fn accumulator_marks_linger_expired_partition_ready() {
    let now = Instant::now();
    let later = now
        .checked_add(Duration::from_millis(6))
        .expect("test instant should not overflow");
    let mut accumulator = RecordAccumulator::new(
        AccumulatorConfig::default()
            .batch_size(16 * 1024)
            .linger(Duration::from_millis(5))
            .buffer_memory(16 * 1024),
    );
    accumulator
        .append_at(
            ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")),
            now,
        )
        .unwrap();

    assert!(accumulator.drain_ready(now).is_empty());

    let ready = accumulator.drain_ready(later);

    assert_eq!(ready.len(), 1);
    assert_eq!(ready[0].records.len(), 1);
}

#[test]
fn accumulator_splits_same_partition_batches_at_batch_size() {
    let now = Instant::now();
    let mut accumulator = RecordAccumulator::new(
        AccumulatorConfig::default()
            .batch_size(150)
            .linger(Duration::from_mins(1))
            .buffer_memory(16 * 1024),
    );
    for _ in 0..24 {
        accumulator
            .append_at(
                ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")),
                now,
            )
            .unwrap();
    }

    let ready = accumulator.drain_ready(now);

    assert_eq!(ready.len(), 2);
    assert!(ready.iter().all(|batch| batch.records.len() == 11));
    assert!(accumulator.buffered_bytes() > 0);
}

#[test]
fn accumulator_batch_size_uses_encoded_batch_bytes_not_buffer_memory_overhead() {
    let now = Instant::now();
    let mut accumulator = RecordAccumulator::new(
        AccumulatorConfig::default()
            .batch_size(128)
            .linger(Duration::from_mins(1))
            .buffer_memory(16 * 1024),
    );
    for _ in 0..4 {
        accumulator
            .append_at(
                ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")),
                now,
            )
            .unwrap();
    }

    assert_eq!(accumulator.buffered_bytes(), 128);
    assert!(accumulator.drain_ready(now).is_empty());

    for _ in 0..6 {
        accumulator
            .append_at(
                ProducerRecord::new("orders", 0).value(Bytes::from_static(b"a")),
                now,
            )
            .unwrap();
    }

    let ready = accumulator.drain_ready(now);

    assert_eq!(ready.len(), 1);
    assert_eq!(ready[0].records.len(), 8);
}

#[test]
fn accumulator_rejects_append_when_buffer_memory_is_full() {
    let mut accumulator = RecordAccumulator::new(
        AccumulatorConfig::default()
            .batch_size(16 * 1024)
            .buffer_memory(1),
    );

    let error = accumulator
        .append(ProducerRecord::new("orders", 0).value(Bytes::from_static(b"too-large")))
        .unwrap_err();

    assert!(matches!(error, ProducerError::Backpressure));
    assert_eq!(accumulator.buffered_bytes(), 0);
}
