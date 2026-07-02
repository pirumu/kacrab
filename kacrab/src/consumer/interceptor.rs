//! Rust-native consumer interceptor hooks, the analogue of Kafka's
//! `ConsumerInterceptor`. Interceptors observe and may rewrite the records a
//! [`poll`](super::Consumer::poll) is about to return (`on_consume`) and observe
//! committed offsets after a successful commit (`on_commit`). Callbacks are
//! panic-isolated so a misbehaving interceptor cannot take the consumer down,
//! matching the producer's interceptor chain.

use std::{collections::HashMap, fmt, sync::Arc};

use super::ConsumerRecords;
use crate::common::{OffsetAndMetadata, TopicPartition};

/// Consumer configuration handed to [`ConsumerInterceptor::configure`] when the
/// interceptor is registered, mirroring Kafka `Configurable.configure(configs)`.
#[derive(Debug, Clone, Default)]
pub struct InterceptorConfigs {
    /// The consumer's configured `client.id`, if any.
    pub client_id: Option<String>,
    /// The consumer's configured `group.id`, if any.
    pub group_id: Option<String>,
}

/// Hook interface around consumer poll and commit.
pub trait ConsumerInterceptor: Send + Sync + 'static {
    /// Configure the interceptor when it is registered (Kafka
    /// `Configurable.configure`). The consumer's `client.id`/`group.id` are
    /// provided.
    fn configure(&self, _configs: &InterceptorConfigs) {}

    /// Intercept or mutate the records a `poll` is about to return, before they
    /// reach the caller (Kafka `ConsumerInterceptor.onConsume`).
    fn on_consume(&self, records: ConsumerRecords) -> ConsumerRecords {
        records
    }

    /// Observe committed offsets after a successful commit — sync, async, or auto
    /// (Kafka `ConsumerInterceptor.onCommit`).
    fn on_commit(&self, _offsets: &HashMap<TopicPartition, OffsetAndMetadata>) {}

    /// Release interceptor resources when the consumer is closed.
    fn close(&self) {}
}

/// The consumer's ordered interceptor chain (cheap to clone; `Arc`-backed).
#[derive(Clone, Default)]
pub(super) struct ConsumerInterceptors {
    inner: Arc<[Arc<dyn ConsumerInterceptor>]>,
}

impl ConsumerInterceptors {
    /// Push an interceptor and immediately `configure` it once (panic-isolated).
    pub(super) fn push_and_configure(
        &mut self,
        interceptor: impl ConsumerInterceptor,
        configs: &InterceptorConfigs,
    ) {
        let interceptor: Arc<dyn ConsumerInterceptor> = Arc::new(interceptor);
        let configured = Arc::clone(&interceptor);
        let _ignored = catch_interceptor_unwind(|| configured.configure(configs));
        let mut inner = self.inner.to_vec();
        inner.push(interceptor);
        self.inner = Arc::from(inner.into_boxed_slice());
    }

    /// Run every interceptor's `on_consume` in order, threading the records
    /// through the chain. A panicking interceptor is skipped (records unchanged).
    pub(super) fn on_consume(&self, mut records: ConsumerRecords) -> ConsumerRecords {
        for interceptor in self.inner.iter() {
            // Snapshot to restore on panic: `on_consume` takes the batch by value
            // (Kafka's transform-and-return contract), so a panicking interceptor
            // would consume it. Only paid when interceptors are actually registered.
            let previous = records.clone();
            match catch_interceptor_unwind(|| interceptor.on_consume(records)) {
                Some(intercepted) => records = intercepted,
                None => records = previous,
            }
        }
        records
    }

    /// Notify every interceptor of committed offsets (panic-isolated).
    pub(super) fn on_commit(&self, offsets: &HashMap<TopicPartition, OffsetAndMetadata>) {
        for interceptor in self.inner.iter() {
            let _ignored = catch_interceptor_unwind(|| interceptor.on_commit(offsets));
        }
    }

    pub(super) fn close(&self) {
        for interceptor in self.inner.iter() {
            let _ignored = catch_interceptor_unwind(|| interceptor.close());
        }
    }
}

fn catch_interceptor_unwind<T>(f: impl FnOnce() -> T) -> Option<T> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)).ok()
}

impl fmt::Debug for ConsumerInterceptors {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConsumerInterceptors")
            .field("len", &self.inner.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    #[derive(Default)]
    struct Recorder {
        commits: Mutex<usize>,
        configured: Mutex<Vec<Option<String>>>,
    }

    impl ConsumerInterceptor for Arc<Recorder> {
        fn configure(&self, configs: &InterceptorConfigs) {
            self.configured
                .lock()
                .unwrap()
                .push(configs.group_id.clone());
        }

        fn on_consume(&self, records: ConsumerRecords) -> ConsumerRecords {
            // Drop everything to prove the chain's output is what poll returns.
            let _dropped = records;
            ConsumerRecords::empty()
        }

        fn on_commit(&self, _offsets: &HashMap<TopicPartition, OffsetAndMetadata>) {
            let mut commits = self.commits.lock().unwrap();
            *commits = commits.saturating_add(1);
        }
    }

    #[test]
    fn chain_threads_records_and_fires_commit() {
        let recorder = Arc::new(Recorder::default());
        let mut chain = ConsumerInterceptors::default();
        chain.push_and_configure(
            Arc::clone(&recorder),
            &InterceptorConfigs {
                client_id: None,
                group_id: Some("g".to_owned()),
            },
        );
        assert_eq!(
            *recorder.configured.lock().unwrap(),
            vec![Some("g".to_owned())]
        );

        let mut records = ConsumerRecords::empty();
        records.push_partition("t".to_owned(), 0, Vec::new());
        // on_consume rewrites to empty.
        assert!(chain.on_consume(records).is_empty());

        chain.on_commit(&HashMap::new());
        assert_eq!(*recorder.commits.lock().unwrap(), 1);
    }
}
