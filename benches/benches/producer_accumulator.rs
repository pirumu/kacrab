//! Producer accumulator micro-benchmarks.

#![allow(
    missing_docs,
    reason = "Criterion macros generate public benchmark entrypoints."
)]

use std::time::{Duration, Instant};

use bytes::Bytes;
use criterion::{
    BatchSize, BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main,
};
use kacrab::producer::{AccumulatorConfig, ProducerRecord, RecordAccumulator};

fn bench_accumulator_append_and_drain(c: &mut Criterion) {
    let mut group = c.benchmark_group("producer_accumulator");
    for records in [1_024_u64, 16_384] {
        let _group = group.throughput(Throughput::Elements(records));
        let _group = group.bench_with_input(
            BenchmarkId::new("append_and_drain", records),
            &records,
            |b, &records| {
                b.iter_batched(
                    || records_for_run(records),
                    |records| {
                        let mut accumulator = RecordAccumulator::new(
                            AccumulatorConfig::default()
                                .batch_size(64 * 1024)
                                .linger(Duration::from_millis(5))
                                .buffer_memory(128 * 1024 * 1024),
                        );
                        let now = Instant::now();
                        for record in records {
                            let appended = black_box(accumulator.append_at(record, now).is_ok());
                            debug_assert!(appended, "benchmark accumulator append should fit");
                        }
                        let ready = accumulator
                            .drain_ready(now.checked_add(Duration::from_millis(6)).unwrap_or(now));
                        let _ready = black_box(ready);
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

fn records_for_run(records: u64) -> Vec<ProducerRecord> {
    (0..records)
        .map(|index| {
            let partition = i32::try_from(index % 12).unwrap_or_default();
            ProducerRecord::new("orders", partition)
                .key(Bytes::from_static(b"customer-42"))
                .value(Bytes::from_static(b"created"))
        })
        .collect()
}

criterion_group!(benches, bench_accumulator_append_and_drain);
criterion_main!(benches);
