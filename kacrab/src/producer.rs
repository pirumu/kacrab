//! Producer API built on top of the wire/session layer.

mod accumulator;
mod api;
mod batch;
mod client;
mod compression_ratio;
mod config;
mod dispatcher;
mod error;
mod interceptor;
mod metrics;
mod partitioner;
mod record;
mod response;
mod routing;
mod sender;
mod serializer;
mod transaction;

#[cfg(test)]
pub(crate) use self::{
    accumulator::{RecordAccumulator, SharedAccumulator},
    config::ProducerIdempotenceConfig,
    transaction::{ProducerBatchState, ProducerIdentity},
};
pub(crate) use self::{
    accumulator::{AccumulatorConfig, ReadyBatch},
    config::ProducerRuntimeConfig,
};
pub use self::{
    api::{
        ConsumerGroupMetadata, OffsetAndMetadata, PartitionInfo, ProducerMetricSubscription,
        ProducerPartitionInfo, TopicPartition,
    },
    client::{Producer, ProducerBuilder},
    config::ProducerCompression,
    error::{ProducerError, Result},
    interceptor::ProducerInterceptor,
    metrics::{
        KafkaMetric, MetricConfig, MetricName, MetricNameTemplate, MetricQuota, MetricReporter,
        MetricValue, Metrics, MetricsError, ProducerMetricValue, ProducerMetricsSnapshot, SensorId,
        SensorRecordingLevel,
    },
    partitioner::{ProducerPartitioner, RoundRobinPartitioner},
    record::{
        DeliveryCallback, Header, Headers, ProducerRecord, RecordHeader, RecordHeaders,
        RecordMetadata, SendFuture,
    },
    sender::SYNC_NOW_BUFFER_SPINS,
    serializer::{
        BooleanSerializer, ByteArraySerializer, BytesSerializer, ConfiguredProducerSerializer,
        DoubleSerializer, FloatSerializer, IntegerSerializer, ListInnerSerializer,
        ListSerializationStrategy, ListSerializer, LongSerializer, ProducerSerializer,
        ShortSerializer, StringSerializer, TypedProducer, UuidSerializer, VoidSerializer,
    },
};

/// Implementation details exposed for repository benchmarks and integration tests.
///
/// This module is not part of the stable user-facing producer API.
#[doc(hidden)]
pub mod internals {
    pub use super::{
        accumulator::{AccumulatorConfig, ReadyBatch, RecordAccumulator, SharedAccumulator},
        config::{ProducerIdempotenceConfig, ProducerRuntimeConfig},
        dispatcher::ProducerDispatcher,
        transaction::{ProducerBatchState, ProducerIdentity},
    };
}
