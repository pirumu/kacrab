//! Rust-native producer partitioner hooks.

use std::{fmt, sync::Arc};

use super::{ProducerRecord, Result};
use crate::wire::ClusterMetadata;

/// Rust-native hook for selecting a partition for unassigned producer records.
///
/// This mirrors Kafka Java's `Partitioner` extension point at the contract
/// level while staying native to Rust. `partitioner.class` is not JVM-loaded;
/// callers install implementations through [`super::ProducerBuilder::partitioner`]
/// or [`super::Producer::set_partitioner`].
pub trait ProducerPartitioner: Send + Sync + 'static {
    /// Select a concrete partition for `record` using the current metadata snapshot.
    ///
    /// Implementations are called only for records without an explicit
    /// partition. The returned partition must exist in `metadata` for the
    /// record topic.
    ///
    /// # Errors
    ///
    /// Returns a producer error when the partition cannot be selected.
    fn partition(&self, record: &ProducerRecord, metadata: &ClusterMetadata) -> Result<i32>;

    /// Release partitioner resources when the producer is closed.
    fn close(&self) {}
}

#[derive(Clone, Default)]
pub(crate) struct ProducerPartitionerHandle {
    inner: Option<Arc<dyn ProducerPartitioner>>,
}

impl ProducerPartitionerHandle {
    pub(crate) fn new(partitioner: impl ProducerPartitioner) -> Self {
        Self {
            inner: Some(Arc::new(partitioner)),
        }
    }

    pub(crate) const fn is_some(&self) -> bool {
        self.inner.is_some()
    }

    pub(crate) fn partition(
        &self,
        record: &ProducerRecord,
        metadata: &ClusterMetadata,
    ) -> Option<Result<i32>> {
        self.inner
            .as_ref()
            .map(|partitioner| partitioner.partition(record, metadata))
    }

    pub(crate) fn close(&self) {
        if let Some(partitioner) = &self.inner {
            partitioner.close();
        }
    }
}

impl fmt::Debug for ProducerPartitionerHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProducerPartitionerHandle")
            .field("installed", &self.inner.is_some())
            .finish()
    }
}
