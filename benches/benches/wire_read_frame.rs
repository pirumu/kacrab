//! Wire response read-path benchmark.
//!
//! `wire_pipeline` measures a whole mock-broker round trip with connection setup
//! inside the timed region, so it cannot resolve read-path changes. This bench
//! drives `read_frame` directly over an in-memory framed stream: no sockets, no
//! setup in the timed region, and frame sizes large enough that the per-frame
//! payload handling dominates.

#![allow(
    clippy::expect_used,
    clippy::missing_assert_message,
    missing_docs,
    reason = "Benchmark fixtures fail fastest; Criterion macros generate public entrypoints."
)]

use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use kacrab::wire::bench_read_frames;
use tokio::runtime::Builder;

/// Frame payload sizes: a metadata-sized control response, a modest fetch, and a
/// large fetch where the old `resize(len, 0)` memset dominated.
const FRAME_SIZES: [usize; 3] = [512, 64 * 1024, 4 * 1024 * 1024];
const FRAMES_PER_RUN: usize = 64;
const POOL_CAPACITY: usize = 8;

fn framed_stream(frame_size: usize, frames: usize) -> Vec<u8> {
    let length = i32::try_from(frame_size).expect("frame size fits in i32");
    let mut stream = Vec::with_capacity(frame_size.saturating_add(4).saturating_mul(frames));
    for index in 0..frames {
        stream.extend_from_slice(&length.to_be_bytes());
        // Non-zero payload so a missing memset cannot be mistaken for correct data.
        stream.resize(
            stream.len().saturating_add(frame_size),
            u8::try_from(index % 251).unwrap_or(1),
        );
    }
    stream
}

fn bench_read_frame(c: &mut Criterion) {
    let runtime = Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("benchmark runtime");
    let mut group = c.benchmark_group("wire_read_frame");
    let _group = group.measurement_time(Duration::from_secs(8));

    for frame_size in FRAME_SIZES {
        let stream = framed_stream(frame_size, FRAMES_PER_RUN);
        let bytes = u64::try_from(frame_size.saturating_mul(FRAMES_PER_RUN)).unwrap_or(u64::MAX);
        let _group = group.throughput(Throughput::Bytes(bytes));

        // Frames dropped as soon as they are read: the pool can reclaim the tail it
        // parked, so this is the read path at its best.
        let _bench = group.bench_with_input(
            BenchmarkId::new("drop_frames", frame_size),
            &stream,
            |b, stream| {
                b.to_async(&runtime).iter(|| async {
                    let mut reader = &stream[..];
                    let read = bench_read_frames(
                        &mut reader,
                        FRAMES_PER_RUN,
                        Some(4096),
                        POOL_CAPACITY,
                        false,
                    )
                    .await;
                    black_box(read)
                });
            },
        );

        // Every frame held alive to the end of the run: the pool cannot reclaim any
        // tail, so this is the read path at its worst.
        let _bench = group.bench_with_input(
            BenchmarkId::new("retain_frames", frame_size),
            &stream,
            |b, stream| {
                b.to_async(&runtime).iter(|| async {
                    let mut reader = &stream[..];
                    let read = bench_read_frames(
                        &mut reader,
                        FRAMES_PER_RUN,
                        Some(4096),
                        POOL_CAPACITY,
                        true,
                    )
                    .await;
                    black_box(read)
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_read_frame);
criterion_main!(benches);
