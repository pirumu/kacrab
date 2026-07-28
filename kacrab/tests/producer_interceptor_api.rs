//! Compile-level proof that a downstream crate can implement every
//! [`ProducerInterceptor`] method, including the two whose parameter types used
//! to be unreachable from outside kacrab.
//!
//! `configure(&InterceptorConfigs)` and `on_update(&ClusterResource)` are public
//! trait methods, but neither parameter type was re-exported from
//! `kacrab::producer`, so an external crate could implement the trait only by
//! taking the defaults for those two — there was no way to name the argument.
//! The consumer side never had this gap (`consumer::InterceptorConfigs` is
//! re-exported and `tests/real_kafka_consumer.rs` overrides `configure`), so this
//! is the producer's equivalent.
//!
//! This is an integration test, i.e. a separate crate: if it compiles, a
//! downstream crate can write the same code. The assertions run the chain
//! through the public builder-free surface that is reachable without a broker.

#![allow(
    clippy::expect_used,
    clippy::missing_assert_message,
    clippy::unwrap_used,
    reason = "Test fixtures fail fastest with contextual unwrap/expect calls."
)]

use std::sync::{Arc, Mutex};

use kacrab::producer::{
    ClusterResource, InterceptorConfigs, ProducerError, ProducerInterceptor, ProducerRecord,
    RecordMetadata,
};

#[derive(Debug, Default)]
struct Recorded {
    client_ids: Mutex<Vec<Option<String>>>,
    cluster_ids: Mutex<Vec<Option<String>>>,
    acked: Mutex<usize>,
    closed: Mutex<usize>,
}

/// A downstream-shaped interceptor: an owned type that shares its state with the
/// test through an `Arc`, exactly as an application would.
#[derive(Debug, Default)]
struct RecordingInterceptor {
    recorded: Arc<Recorded>,
}

impl ProducerInterceptor for RecordingInterceptor {
    fn configure(&self, configs: &InterceptorConfigs) {
        self.recorded
            .client_ids
            .lock()
            .unwrap()
            .push(configs.client_id.clone());
    }

    fn on_update(&self, cluster: &ClusterResource) {
        self.recorded
            .cluster_ids
            .lock()
            .unwrap()
            .push(cluster.cluster_id.clone());
    }

    fn on_send(&self, record: ProducerRecord) -> kacrab::producer::Result<ProducerRecord> {
        Ok(record.header("intercepted", "yes"))
    }

    fn on_ack(
        &self,
        _metadata: Option<&RecordMetadata>,
        _error: Option<&ProducerError>,
        _headers: &[kacrab::producer::RecordHeader],
    ) {
        let mut acked = self.recorded.acked.lock().unwrap();
        *acked = acked.saturating_add(1);
    }

    fn close(&self) {
        let mut closed = self.recorded.closed.lock().unwrap();
        *closed = closed.saturating_add(1);
    }
}

#[test]
fn downstream_crate_can_implement_configure_and_on_update() {
    let interceptor = RecordingInterceptor::default();
    let recorder = Arc::clone(&interceptor.recorded);

    // Both parameter types are nameable and constructible from outside kacrab,
    // which is what makes overriding these two methods possible at all.
    interceptor.configure(&InterceptorConfigs {
        client_id: Some("downstream".to_owned()),
    });
    interceptor.on_update(&ClusterResource {
        cluster_id: Some("cluster-a".to_owned()),
    });
    interceptor.on_ack(None, None, &[]);
    interceptor.close();

    assert_eq!(
        *recorder.client_ids.lock().unwrap(),
        vec![Some("downstream".to_owned())]
    );
    assert_eq!(
        *recorder.cluster_ids.lock().unwrap(),
        vec![Some("cluster-a".to_owned())]
    );
    assert_eq!(*recorder.acked.lock().unwrap(), 1);
    assert_eq!(*recorder.closed.lock().unwrap(), 1);

    let record = interceptor
        .on_send(ProducerRecord::unassigned("orders"))
        .expect("on_send keeps the record");

    assert_eq!(
        record
            .last_header(b"intercepted")
            .expect("interceptor added a header")
            .value
            .as_deref(),
        Some(b"yes".as_slice())
    );
}

#[test]
fn interceptor_config_types_default_to_absent_ids() {
    let configs = InterceptorConfigs::default();
    let cluster = ClusterResource::default();

    assert_eq!(configs.client_id, None);
    assert_eq!(cluster.cluster_id, None);
}
