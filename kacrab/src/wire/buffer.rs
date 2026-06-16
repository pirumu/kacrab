//! Bounded buffer pools and diagnostic allocation counters for wire tasks.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use bytes::BytesMut;
use crossbeam_queue::ArrayQueue;

/// Read/write buffer size classes span common Kafka frame sizes from tiny
/// metadata responses through multi-MiB produce batches. Powers-of-four keep
/// bucket choice cheap and avoid retaining arbitrarily large broker frames.
const BUFFER_SIZE_CLASSES: [usize; 6] = [
    4 * 1024,
    16 * 1024,
    64 * 1024,
    256 * 1024,
    1024 * 1024,
    4 * 1024 * 1024,
];
#[cfg(test)]
const SMALLEST_BUFFER_CLASS: usize = 4 * 1024;
#[cfg(test)]
const LARGEST_BUFFER_CLASS: usize = 4 * 1024 * 1024;

/// Diagnostic counters for wire read/write buffer pool activity.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BufferPoolStats {
    /// Number of read buffers acquired from the pool abstraction.
    pub read_acquired: usize,
    /// Number of read buffer acquisitions served by an existing pooled buffer.
    pub read_reused: usize,
    /// Number of read buffers returned to the pool.
    pub read_released: usize,
    /// Number of write buffers acquired from the pool abstraction.
    pub write_acquired: usize,
    /// Number of write buffer acquisitions served by an existing pooled buffer.
    pub write_reused: usize,
    /// Number of write buffers returned to the pool.
    pub write_released: usize,
}

#[derive(Debug)]
pub(crate) struct BufferPools {
    read: Arc<BufferPool>,
    write: Arc<BufferPool>,
}

impl BufferPools {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            read: Arc::new(BufferPool::new(capacity)),
            write: Arc::new(BufferPool::new(capacity)),
        }
    }

    pub(crate) fn acquire_read(&self, capacity: usize) -> BytesMut {
        self.read.acquire(capacity)
    }

    pub(crate) fn release_read(&self, buffer: BytesMut) {
        self.read.release(buffer);
    }

    pub(crate) fn acquire_write(&self, capacity: usize) -> BytesMut {
        self.write.acquire(capacity)
    }

    pub(crate) fn release_write(&self, buffer: BytesMut) {
        self.write.release(buffer);
    }

    pub(crate) fn stats(&self) -> BufferPoolStats {
        let read = self.read.stats();
        let write = self.write.stats();
        BufferPoolStats {
            read_acquired: read.acquired,
            read_reused: read.reused,
            read_released: read.released,
            write_acquired: write.acquired,
            write_reused: write.reused,
            write_released: write.released,
        }
    }
}

#[derive(Debug)]
struct BufferPool {
    per_class_capacity: usize,
    buckets: Vec<BufferBucket>,
    acquired: AtomicUsize,
    reused: AtomicUsize,
    released: AtomicUsize,
}

#[derive(Debug)]
struct BufferBucket {
    class_capacity: usize,
    queue: ArrayQueue<BytesMut>,
}

#[derive(Debug, Clone, Copy)]
struct PoolStats {
    acquired: usize,
    reused: usize,
    released: usize,
}

impl BufferPool {
    fn new(per_class_capacity: usize) -> Self {
        let mut buckets = Vec::with_capacity(BUFFER_SIZE_CLASSES.len());
        for class_capacity in BUFFER_SIZE_CLASSES {
            buckets.push(BufferBucket {
                class_capacity,
                queue: ArrayQueue::new(per_class_capacity.max(1)),
            });
        }
        Self {
            per_class_capacity,
            buckets,
            acquired: AtomicUsize::new(0),
            reused: AtomicUsize::new(0),
            released: AtomicUsize::new(0),
        }
    }

    fn acquire(&self, capacity: usize) -> BytesMut {
        let _previous = self.acquired.fetch_add(1, Ordering::Relaxed);
        let Some(class_index) = class_index_for(capacity) else {
            return BytesMut::with_capacity(capacity);
        };
        let Some(bucket) = self.buckets.get(class_index) else {
            return BytesMut::with_capacity(capacity);
        };
        let buffer = bucket.queue.pop();
        let Some(mut buffer) = buffer else {
            return BytesMut::with_capacity(bucket.class_capacity);
        };
        let _previous = self.reused.fetch_add(1, Ordering::Relaxed);
        buffer.clear();
        buffer
    }

    fn release(&self, mut buffer: BytesMut) {
        if self.per_class_capacity == 0 {
            return;
        }
        let Some(class_index) = class_index_for(buffer.capacity()) else {
            return;
        };
        let Some(bucket) = self.buckets.get(class_index) else {
            return;
        };
        buffer.clear();
        if buffer.capacity() != bucket.class_capacity {
            buffer = BytesMut::with_capacity(bucket.class_capacity);
        }
        if bucket.queue.push(buffer).is_ok() {
            let _previous = self.released.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn stats(&self) -> PoolStats {
        PoolStats {
            acquired: self.acquired.load(Ordering::Relaxed),
            reused: self.reused.load(Ordering::Relaxed),
            released: self.released.load(Ordering::Relaxed),
        }
    }
}

fn class_index_for(capacity: usize) -> Option<usize> {
    BUFFER_SIZE_CLASSES
        .iter()
        .position(|class_capacity| capacity <= *class_capacity)
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::expect_used,
        clippy::missing_assert_message,
        clippy::unwrap_used,
        reason = "Unit test fixtures fail fastest with contextual unwrap/expect calls."
    )]

    use super::{BufferPool, LARGEST_BUFFER_CLASS, SMALLEST_BUFFER_CLASS};

    #[test]
    fn pool_reuses_buffers_from_matching_size_class() {
        let pool = BufferPool::new(2);
        let buffer = pool.acquire(128);
        assert_eq!(buffer.capacity(), SMALLEST_BUFFER_CLASS);
        pool.release(buffer);

        let reused = pool.acquire(256);
        let stats = pool.stats();

        assert_eq!(reused.capacity(), SMALLEST_BUFFER_CLASS);
        assert_eq!(stats.reused, 1);
        assert_eq!(stats.released, 1);
    }

    #[test]
    fn pool_does_not_retain_buffers_above_largest_size_class() {
        let pool = BufferPool::new(2);
        let oversized = pool.acquire(LARGEST_BUFFER_CLASS.saturating_add(1));
        pool.release(oversized);

        let stats = pool.stats();

        assert_eq!(stats.reused, 0);
        assert_eq!(stats.released, 0);
    }

    #[test]
    fn pool_replaces_split_off_empty_buffers_before_reuse() {
        let pool = BufferPool::new(2);
        let mut buffer = pool.acquire(128);
        buffer.resize(128, 0);
        let _frozen = buffer.split_to(128).freeze();
        assert!(buffer.capacity() < SMALLEST_BUFFER_CLASS);

        pool.release(buffer);
        let reused = pool.acquire(128);

        assert_eq!(reused.capacity(), SMALLEST_BUFFER_CLASS);
        assert_eq!(pool.stats().reused, 1);
    }

    #[test]
    fn pool_falls_back_when_bucket_table_is_missing_class() {
        let mut pool = BufferPool::new(1);
        pool.buckets.clear();

        let buffer = pool.acquire(128);
        pool.release(buffer);

        assert_eq!(pool.stats().acquired, 1);
        assert_eq!(pool.stats().released, 0);
    }
}
