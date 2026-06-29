//! Typed producer with a custom serializer.
//!
//! Serializers are a plain Rust trait, [`ProducerSerializer<T>`]: implement it
//! for your own type and wire it in with `build_with_serializers`. There is no
//! `key.serializer` / `value.serializer` class-name configuration — you pass the
//! serializer value directly, so the key and value types are checked at compile
//! time.
//!
//! ```text
//! cargo run -p kacrab-examples --example typed_serializer
//! cargo run -p kacrab-examples --example typed_serializer -- 127.0.0.1:9092 kacrab-orders 0
//! ```
//!
//! Deserializers are consumer-side; the consumer is not implemented yet.

use std::{env, error::Error};

use bytes::Bytes;
use kacrab::producer::{
    Producer, ProducerRecord, ProducerSerializer, RecordHeader, Result as ProducerResult,
    StringSerializer,
};

/// An application event we publish with a compact binary encoding.
#[derive(Debug, Clone, Copy)]
struct OrderEvent {
    order_id: u64,
    amount_cents: u64,
}

/// Custom value serializer: encodes an [`OrderEvent`] as 16 big-endian bytes.
///
/// `serialize` receives the topic and a mutable header list (so a serializer can
/// attach headers before append) and returns the encoded payload, or `None` for
/// a null value.
struct OrderEventSerializer;

impl ProducerSerializer<OrderEvent> for OrderEventSerializer {
    fn serialize(
        &self,
        _topic: &str,
        _headers: &mut Vec<RecordHeader>,
        value: Option<&OrderEvent>,
    ) -> ProducerResult<Option<Bytes>> {
        Ok(value.map(|event| {
            let mut bytes = Vec::with_capacity(16);
            bytes.extend_from_slice(&event.order_id.to_be_bytes());
            bytes.extend_from_slice(&event.amount_cents.to_be_bytes());
            Bytes::from(bytes)
        }))
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let bootstrap = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:9092".to_owned());
    let topic = env::args()
        .nth(2)
        .unwrap_or_else(|| "kacrab-orders".to_owned());
    let partition: i32 = env::args()
        .nth(3)
        .map_or(Ok(0), |value| value.parse())
        .map_err(|error| format!("invalid partition: {error}"))?;

    // The key uses the built-in StringSerializer; the value uses our custom one.
    // K = String and V = OrderEvent are inferred from the serializer types.
    let mut producer = Producer::builder()
        .set("bootstrap.servers", bootstrap.as_str())
        .set("client.id", "kacrab-typed-serializer-example")
        .set("acks", "all")
        .set("enable.idempotence", "true")
        .build_with_serializers(StringSerializer::default(), OrderEventSerializer)
        .await?;

    let key = "order-42".to_owned();
    let event = OrderEvent {
        order_id: 42,
        amount_cents: 1_999,
    };

    let delivery = producer.send(
        ProducerRecord::new(topic, partition),
        Some(&key),
        Some(&event),
    )?;
    let receipt = delivery.await?;
    println!(
        "sent order {} ({} cents) -> {}-{} @ offset {}",
        event.order_id, event.amount_cents, receipt.topic, receipt.partition, receipt.offset
    );

    producer.close().await?;
    Ok(())
}
