//! Producer API built on top of the wire/session layer.

mod accumulator;
mod batch;
mod config;
mod dispatcher;
mod error;
mod kafka;
mod record;
mod response;
mod routing;
mod transaction;

pub use self::{
    accumulator::{AccumulatorConfig, ReadyBatch, RecordAccumulator},
    config::{ProducerCompression, ProducerIdempotenceConfig, ProducerRuntimeConfig},
    dispatcher::ProducerDispatcher,
    error::{ProducerError, Result},
    kafka::{KafkaProducer, KafkaProducerBuilder},
    record::{Delivery, ProduceReceipt, ProducerRecord},
    transaction::{ProducerBatchState, ProducerIdentity},
};
