//! Producer id, sequence, and transaction state helpers.

/// Producer identity assigned by Kafka's `InitProducerId` API.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducerIdentity {
    /// Kafka producer id.
    pub producer_id: i64,
    /// Kafka producer epoch.
    pub producer_epoch: i16,
}

/// Record-batch idempotence fields for one topic-partition batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducerBatchState {
    /// Producer identity used to write this batch.
    pub identity: ProducerIdentity,
    /// Base sequence for the first record in this batch.
    pub base_sequence: i32,
}
