//! Rust-native producer interceptor hooks.

use std::{fmt, sync::Arc};

use kacrab_protocol::record::RecordHeader;

use super::{ProducerError, ProducerRecord, RecordMetadata, Result};

/// Hook interface around producer send and acknowledgement.
pub trait ProducerInterceptor: Send + Sync + 'static {
    /// Intercept or mutate a record before partitioning and append.
    ///
    /// Errors are ignored by the interceptor chain to match Java producer
    /// semantics.
    fn on_send(&self, record: ProducerRecord) -> Result<ProducerRecord> {
        Ok(record)
    }

    /// Observe a successful or failed acknowledgement before user callbacks.
    fn on_ack(
        &self,
        _metadata: Option<&RecordMetadata>,
        _error: Option<&ProducerError>,
        _headers: &[RecordHeader],
    ) {
    }

    /// Observe a send failure before the record is appended.
    fn on_error(&self, record: &ProducerRecord, error: &ProducerError) {
        let metadata = RecordMetadata {
            topic: record.topic.to_string(),
            partition: record.partition,
            leader_id: -1,
            offset: -1,
            timestamp_ms: -1,
            serialized_key_size: -1,
            serialized_value_size: -1,
        };
        self.on_ack(Some(&metadata), Some(error), &record.headers);
    }

    /// Release interceptor resources when the producer is closed.
    fn close(&self) {}
}

#[derive(Clone, Default)]
pub(crate) struct ProducerInterceptors {
    inner: Arc<[Arc<dyn ProducerInterceptor>]>,
}

impl ProducerInterceptors {
    pub(crate) fn push(&mut self, interceptor: impl ProducerInterceptor) {
        let mut inner = self.inner.to_vec();
        inner.push(Arc::new(interceptor));
        self.inner = Arc::from(inner.into_boxed_slice());
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub(crate) fn on_send(&self, mut record: ProducerRecord) -> ProducerRecord {
        for interceptor in self.inner.iter() {
            let previous = record.clone();
            match catch_interceptor_unwind(|| interceptor.on_send(record)) {
                Some(Ok(intercepted)) => record = intercepted,
                Some(Err(_)) | None => record = previous,
            }
        }
        record
    }

    pub(crate) fn on_ack(
        &self,
        metadata: Option<&RecordMetadata>,
        error: Option<&ProducerError>,
        headers: &[RecordHeader],
    ) {
        for interceptor in self.inner.iter() {
            let _ignored = catch_interceptor_unwind(|| {
                interceptor.on_ack(metadata, error, headers);
            });
        }
    }

    pub(crate) fn on_error(&self, record: &ProducerRecord, error: &ProducerError) {
        for interceptor in self.inner.iter() {
            let _ignored = catch_interceptor_unwind(|| {
                interceptor.on_error(record, error);
            });
        }
    }

    #[cfg(feature = "std")]
    pub(crate) fn close(&self) {
        for interceptor in self.inner.iter() {
            let _ignored = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                interceptor.close();
            }));
        }
    }

    #[cfg(not(feature = "std"))]
    pub(crate) fn close(&self) {
        for interceptor in self.inner.iter() {
            interceptor.close();
        }
    }
}

#[cfg(feature = "std")]
fn catch_interceptor_unwind<T>(f: impl FnOnce() -> T) -> Option<T> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)).ok()
}

#[cfg(not(feature = "std"))]
fn catch_interceptor_unwind<T>(f: impl FnOnce() -> T) -> Option<T> {
    Some(f())
}

impl fmt::Debug for ProducerInterceptors {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProducerInterceptors")
            .field("len", &self.inner.len())
            .finish()
    }
}
