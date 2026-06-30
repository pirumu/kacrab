//! Rust-native producer interceptor hooks.

use std::{fmt, sync::Arc};

use kacrab_protocol::record::RecordHeader;

use super::{ProducerError, ProducerRecord, RecordMetadata, Result};

/// Producer configuration handed to [`ProducerInterceptor::configure`] when the
/// interceptor is registered, mirroring Kafka `Configurable.configure(configs)`
/// which always includes the `client.id`.
#[derive(Debug, Clone, Default)]
pub struct InterceptorConfigs {
    /// The producer's configured `client.id`, if any.
    pub client_id: Option<String>,
}

/// Cluster identity handed to [`ProducerInterceptor::on_update`] when the
/// producer's metadata first resolves or its cluster id changes, mirroring Kafka
/// `ClusterResourceListener.onUpdate(ClusterResource)`.
#[derive(Debug, Clone, Default)]
pub struct ClusterResource {
    /// The cluster id reported by the broker metadata, if known.
    pub cluster_id: Option<String>,
}

/// Hook interface around producer send and acknowledgement.
pub trait ProducerInterceptor: Send + Sync + 'static {
    /// Configure the interceptor when it is registered (Kafka
    /// `Configurable.configure`). The producer's `client.id` is provided.
    fn configure(&self, _configs: &InterceptorConfigs) {}

    /// Observe a cluster-metadata update (Kafka
    /// `ClusterResourceListener.onUpdate`). Called once the producer first learns
    /// the cluster id and again whenever it changes.
    fn on_update(&self, _cluster: &ClusterResource) {}

    /// Intercept or mutate a record before partitioning and append.
    ///
    /// Errors are ignored by the interceptor chain to match Kafka producer
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
            topic: Arc::clone(&record.topic),
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

    /// Push an interceptor added after construction and immediately `configure` it
    /// once (panic-isolated), since the builder-time `configure` pass has already run.
    pub(crate) fn push_and_configure(
        &mut self,
        interceptor: impl ProducerInterceptor,
        configs: &InterceptorConfigs,
    ) {
        let interceptor: Arc<dyn ProducerInterceptor> = Arc::new(interceptor);
        let configured = Arc::clone(&interceptor);
        let _ignored = catch_interceptor_unwind(|| configured.configure(configs));
        let mut inner = self.inner.to_vec();
        inner.push(interceptor);
        self.inner = Arc::from(inner.into_boxed_slice());
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Invoke `configure` on every interceptor (Kafka instantiation hook),
    /// panic-isolated like the other callbacks.
    pub(crate) fn configure(&self, configs: &InterceptorConfigs) {
        for interceptor in self.inner.iter() {
            let _ignored = catch_interceptor_unwind(|| {
                interceptor.configure(configs);
            });
        }
    }

    /// Invoke `on_update` on every interceptor with the resolved cluster resource
    /// (Kafka `ClusterResourceListener.onUpdate`), panic-isolated.
    pub(crate) fn on_cluster_update(&self, cluster: &ClusterResource) {
        for interceptor in self.inner.iter() {
            let _ignored = catch_interceptor_unwind(|| {
                interceptor.on_update(cluster);
            });
        }
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

    pub(crate) fn close(&self) {
        for interceptor in self.inner.iter() {
            let _ignored = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                interceptor.close();
            }));
        }
    }
}

fn catch_interceptor_unwind<T>(f: impl FnOnce() -> T) -> Option<T> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)).ok()
}

impl fmt::Debug for ProducerInterceptors {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProducerInterceptors")
            .field("len", &self.inner.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::{ClusterResource, InterceptorConfigs, ProducerInterceptor, ProducerInterceptors};

    #[derive(Default)]
    struct RecordingInterceptor {
        configured_client_ids: Mutex<Vec<Option<String>>>,
        cluster_ids: Mutex<Vec<Option<String>>>,
    }

    impl ProducerInterceptor for std::sync::Arc<RecordingInterceptor> {
        fn configure(&self, configs: &InterceptorConfigs) {
            self.configured_client_ids
                .lock()
                .unwrap()
                .push(configs.client_id.clone());
        }

        fn on_update(&self, cluster: &ClusterResource) {
            self.cluster_ids
                .lock()
                .unwrap()
                .push(cluster.cluster_id.clone());
        }
    }

    #[test]
    fn configure_and_on_update_reach_interceptors_like_java() {
        let recorder = std::sync::Arc::new(RecordingInterceptor::default());
        let mut interceptors = ProducerInterceptors::default();
        interceptors.push(std::sync::Arc::clone(&recorder));

        // configure(configs) delivers the client.id (Kafka Configurable.configure).
        interceptors.configure(&InterceptorConfigs {
            client_id: Some("kacrab-test".to_owned()),
        });
        assert_eq!(
            *recorder.configured_client_ids.lock().unwrap(),
            vec![Some("kacrab-test".to_owned())]
        );

        // on_update delivers the cluster id (Kafka ClusterResourceListener.onUpdate).
        interceptors.on_cluster_update(&ClusterResource {
            cluster_id: Some("cluster-a".to_owned()),
        });
        assert_eq!(
            *recorder.cluster_ids.lock().unwrap(),
            vec![Some("cluster-a".to_owned())]
        );

        // push_and_configure configures a late-added interceptor exactly once.
        let late = std::sync::Arc::new(RecordingInterceptor::default());
        interceptors.push_and_configure(
            std::sync::Arc::clone(&late),
            &InterceptorConfigs {
                client_id: Some("late".to_owned()),
            },
        );
        assert_eq!(
            *late.configured_client_ids.lock().unwrap(),
            vec![Some("late".to_owned())]
        );
    }
}
