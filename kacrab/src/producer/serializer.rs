//! Rust-native key/value serializer support for typed producer records.

use std::marker::PhantomData;

use bytes::Bytes;
use kacrab_protocol::record::RecordHeader;

use super::{Producer, ProducerError, ProducerRecord, Result, SendFuture};
use crate::config::ClientConfig;

/// Converts typed key/value data into Kafka record bytes.
pub trait ProducerSerializer<T>: Send + Sync + 'static {
    /// Configure this serializer from producer config; `is_key` selects the key
    /// or value config keys. The default is a no-op.
    ///
    /// # Errors
    ///
    /// Returns a producer error when the serializer rejects the supplied config.
    fn configure(&self, config: &ClientConfig, is_key: bool) -> Result<()> {
        let _ = (config, is_key);
        Ok(())
    }

    /// Serialize one key or value.
    ///
    /// Implementations receive mutable headers so serialization may attach
    /// headers before the record is appended.
    ///
    /// # Errors
    ///
    /// Returns a producer error when serialization fails.
    fn serialize(
        &self,
        topic: &str,
        headers: &mut Vec<RecordHeader>,
        value: Option<&T>,
    ) -> Result<Option<Bytes>>;

    /// Release serializer resources when the typed producer is closed.
    fn close(&self) {}
}

/// A serializer that can be built from producer config.
pub trait ConfiguredProducerSerializer<T>: ProducerSerializer<T> + Default {
    /// Build a serializer from producer config.
    ///
    /// The default constructs `Self::default()` and runs
    /// [`configure`](ProducerSerializer::configure); stateless serializers rely
    /// on it directly.
    ///
    /// # Errors
    ///
    /// Returns a producer error when the serializer cannot be built from config.
    fn from_client_config(config: &ClientConfig, is_key: bool) -> Result<Self> {
        let serializer = Self::default();
        serializer.configure(config, is_key)?;
        Ok(serializer)
    }
}

/// How a [`ListSerializer`] lays out inner values on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListSerializationStrategy {
    /// Fixed-width inner values; null elements are recorded in a null-index list.
    ConstantSize,
    /// Variable-width inner values; each element is length-prefixed.
    VariableSize,
}

impl ListSerializationStrategy {
    const fn wire_flag(self) -> u8 {
        match self {
            Self::ConstantSize => 0,
            Self::VariableSize => 1,
        }
    }
}

/// Inner serializer whose [`ListSerializer`] layout strategy is fixed.
pub trait ListInnerSerializer<T>: ProducerSerializer<T> {
    /// Wire layout strategy used when this serializer is a list inner element.
    const LIST_STRATEGY: ListSerializationStrategy;
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
enum TextEncoding {
    #[default]
    Utf8,
    Iso88591,
    UsAscii,
    Utf16Be,
    Utf16Le,
    Utf16,
}

impl TextEncoding {
    fn from_client_config(config: &ClientConfig, is_key: bool) -> Result<Self> {
        let specific_key = if is_key {
            "key.serializer.encoding"
        } else {
            "value.serializer.encoding"
        };
        if let Some(value) = config.get(specific_key) {
            return Self::parse(value.as_str(), specific_key);
        }
        config
            .get("serializer.encoding")
            .map_or(Ok(Self::Utf8), |value| {
                Self::parse(value.as_str(), "serializer.encoding")
            })
    }

    fn parse(value: &str, key: &'static str) -> Result<Self> {
        match value.trim().to_ascii_uppercase().as_str() {
            "UTF-8" | "UTF8" => Ok(Self::Utf8),
            "ISO-8859-1" | "ISO_8859_1" | "ISO8859-1" | "ISO88591" | "LATIN1" | "LATIN-1" => {
                Ok(Self::Iso88591)
            },
            "US-ASCII" | "ASCII" => Ok(Self::UsAscii),
            "UTF-16BE" | "UTF16BE" => Ok(Self::Utf16Be),
            "UTF-16LE" | "UTF16LE" => Ok(Self::Utf16Le),
            "UTF-16" | "UTF16" => Ok(Self::Utf16),
            _ => Err(ProducerError::InvalidConfig {
                key,
                value: value.to_owned(),
            }),
        }
    }

    fn encode(self, value: &str) -> Bytes {
        match self {
            Self::Utf8 => Bytes::copy_from_slice(value.as_bytes()),
            Self::Iso88591 => Bytes::from(
                value
                    .chars()
                    .map(|ch| u8::try_from(ch).unwrap_or(b'?'))
                    .collect::<Vec<u8>>(),
            ),
            Self::UsAscii => Bytes::from(
                value
                    .chars()
                    .map(|ch| if ch.is_ascii() { ch as u8 } else { b'?' })
                    .collect::<Vec<u8>>(),
            ),
            Self::Utf16Be => encode_utf16_bytes(value, false, false),
            Self::Utf16Le => encode_utf16_bytes(value, true, false),
            Self::Utf16 => encode_utf16_bytes(value, false, true),
        }
    }
}

fn encode_utf16_bytes(value: &str, little_endian: bool, bom: bool) -> Bytes {
    let mut bytes = Vec::with_capacity(value.len().saturating_mul(2).saturating_add(2));
    if bom {
        bytes.extend_from_slice(&[0xfe, 0xff]);
    }
    for unit in value.encode_utf16() {
        let encoded = if little_endian {
            unit.to_le_bytes()
        } else {
            unit.to_be_bytes()
        };
        bytes.extend_from_slice(&encoded);
    }
    Bytes::from(bytes)
}

/// Pass-through serializer for already-owned [`Bytes`].
#[derive(Debug, Clone, Copy, Default)]
pub struct BytesSerializer;

impl ProducerSerializer<Bytes> for BytesSerializer {
    fn serialize(
        &self,
        _topic: &str,
        _headers: &mut Vec<RecordHeader>,
        payload: Option<&Bytes>,
    ) -> Result<Option<Bytes>> {
        Ok(payload.cloned())
    }
}

impl ConfiguredProducerSerializer<Bytes> for BytesSerializer {}

impl ListInnerSerializer<Bytes> for BytesSerializer {
    const LIST_STRATEGY: ListSerializationStrategy = ListSerializationStrategy::VariableSize;
}

/// Serializer for owned byte arrays.
///
/// Uses Rust's [`Vec<u8>`] as the byte-array type; `None` serializes to null.
#[derive(Debug, Clone, Copy, Default)]
pub struct ByteArraySerializer;

impl ProducerSerializer<Vec<u8>> for ByteArraySerializer {
    fn serialize(
        &self,
        _topic: &str,
        _headers: &mut Vec<RecordHeader>,
        payload: Option<&Vec<u8>>,
    ) -> Result<Option<Bytes>> {
        Ok(payload.map(|payload| Bytes::copy_from_slice(payload)))
    }
}

impl ConfiguredProducerSerializer<Vec<u8>> for ByteArraySerializer {}

impl ListInnerSerializer<Vec<u8>> for ByteArraySerializer {
    const LIST_STRATEGY: ListSerializationStrategy = ListSerializationStrategy::VariableSize;
}

/// Serializer for [`String`] values.
///
/// Defaults to UTF-8 and honors the `*.serializer.encoding` config overrides.
#[derive(Debug, Clone, Copy, Default)]
pub struct StringSerializer {
    encoding: TextEncoding,
}

impl ProducerSerializer<String> for StringSerializer {
    fn serialize(
        &self,
        _topic: &str,
        _headers: &mut Vec<RecordHeader>,
        payload: Option<&String>,
    ) -> Result<Option<Bytes>> {
        Ok(payload.map(|payload| self.encoding.encode(payload)))
    }
}

impl ConfiguredProducerSerializer<String> for StringSerializer {
    fn from_client_config(config: &ClientConfig, is_key: bool) -> Result<Self> {
        Ok(Self {
            encoding: TextEncoding::from_client_config(config, is_key)?,
        })
    }
}

impl ListInnerSerializer<String> for StringSerializer {
    const LIST_STRATEGY: ListSerializationStrategy = ListSerializationStrategy::VariableSize;
}

/// Boolean serializer.
#[derive(Debug, Clone, Copy, Default)]
pub struct BooleanSerializer;

impl ProducerSerializer<bool> for BooleanSerializer {
    fn serialize(
        &self,
        _topic: &str,
        _headers: &mut Vec<RecordHeader>,
        payload: Option<&bool>,
    ) -> Result<Option<Bytes>> {
        Ok(payload.map(|payload| Bytes::copy_from_slice(&[u8::from(*payload)])))
    }
}

impl ConfiguredProducerSerializer<bool> for BooleanSerializer {}

impl ListInnerSerializer<bool> for BooleanSerializer {
    const LIST_STRATEGY: ListSerializationStrategy = ListSerializationStrategy::VariableSize;
}

/// 16-bit integer serializer.
#[derive(Debug, Clone, Copy, Default)]
pub struct ShortSerializer;

impl ProducerSerializer<i16> for ShortSerializer {
    fn serialize(
        &self,
        _topic: &str,
        _headers: &mut Vec<RecordHeader>,
        payload: Option<&i16>,
    ) -> Result<Option<Bytes>> {
        Ok(payload.map(|payload| Bytes::copy_from_slice(&payload.to_be_bytes())))
    }
}

impl ConfiguredProducerSerializer<i16> for ShortSerializer {}

impl ListInnerSerializer<i16> for ShortSerializer {
    const LIST_STRATEGY: ListSerializationStrategy = ListSerializationStrategy::ConstantSize;
}

/// 32-bit integer serializer.
#[derive(Debug, Clone, Copy, Default)]
pub struct IntegerSerializer;

impl ProducerSerializer<i32> for IntegerSerializer {
    fn serialize(
        &self,
        _topic: &str,
        _headers: &mut Vec<RecordHeader>,
        payload: Option<&i32>,
    ) -> Result<Option<Bytes>> {
        Ok(payload.map(|payload| Bytes::copy_from_slice(&payload.to_be_bytes())))
    }
}

impl ConfiguredProducerSerializer<i32> for IntegerSerializer {}

impl ListInnerSerializer<i32> for IntegerSerializer {
    const LIST_STRATEGY: ListSerializationStrategy = ListSerializationStrategy::ConstantSize;
}

/// 64-bit integer serializer.
#[derive(Debug, Clone, Copy, Default)]
pub struct LongSerializer;

impl ProducerSerializer<i64> for LongSerializer {
    fn serialize(
        &self,
        _topic: &str,
        _headers: &mut Vec<RecordHeader>,
        payload: Option<&i64>,
    ) -> Result<Option<Bytes>> {
        Ok(payload.map(|payload| Bytes::copy_from_slice(&payload.to_be_bytes())))
    }
}

impl ConfiguredProducerSerializer<i64> for LongSerializer {}

impl ListInnerSerializer<i64> for LongSerializer {
    const LIST_STRATEGY: ListSerializationStrategy = ListSerializationStrategy::ConstantSize;
}

/// 32-bit float serializer.
#[derive(Debug, Clone, Copy, Default)]
pub struct FloatSerializer;

impl ProducerSerializer<f32> for FloatSerializer {
    fn serialize(
        &self,
        _topic: &str,
        _headers: &mut Vec<RecordHeader>,
        payload: Option<&f32>,
    ) -> Result<Option<Bytes>> {
        Ok(payload.map(|payload| {
            // Canonicalize NaN to Java's `Float.floatToIntBits` value, matching
            // `DoubleSerializer` and the Kafka wire form.
            let bits = if payload.is_nan() {
                0x7fc0_0000_u32
            } else {
                payload.to_bits()
            };
            Bytes::copy_from_slice(&bits.to_be_bytes())
        }))
    }
}

impl ConfiguredProducerSerializer<f32> for FloatSerializer {}

impl ListInnerSerializer<f32> for FloatSerializer {
    const LIST_STRATEGY: ListSerializationStrategy = ListSerializationStrategy::ConstantSize;
}

/// 64-bit float serializer.
#[derive(Debug, Clone, Copy, Default)]
pub struct DoubleSerializer;

impl ProducerSerializer<f64> for DoubleSerializer {
    fn serialize(
        &self,
        _topic: &str,
        _headers: &mut Vec<RecordHeader>,
        payload: Option<&f64>,
    ) -> Result<Option<Bytes>> {
        Ok(payload.map(|payload| {
            let bits = if payload.is_nan() {
                0x7ff8_0000_0000_0000_u64
            } else {
                payload.to_bits()
            };
            Bytes::copy_from_slice(&bits.to_be_bytes())
        }))
    }
}

impl ConfiguredProducerSerializer<f64> for DoubleSerializer {}

impl ListInnerSerializer<f64> for DoubleSerializer {
    const LIST_STRATEGY: ListSerializationStrategy = ListSerializationStrategy::ConstantSize;
}

/// Void serializer.
#[derive(Debug, Clone, Copy, Default)]
pub struct VoidSerializer;

impl ProducerSerializer<()> for VoidSerializer {
    fn serialize(
        &self,
        _topic: &str,
        _headers: &mut Vec<RecordHeader>,
        _payload: Option<&()>,
    ) -> Result<Option<Bytes>> {
        Ok(None)
    }
}

impl ConfiguredProducerSerializer<()> for VoidSerializer {}

impl ListInnerSerializer<()> for VoidSerializer {
    const LIST_STRATEGY: ListSerializationStrategy = ListSerializationStrategy::VariableSize;
}

/// UUID serializer.
#[derive(Debug, Clone, Copy, Default)]
pub struct UuidSerializer {
    encoding: TextEncoding,
}

impl ProducerSerializer<uuid::Uuid> for UuidSerializer {
    fn serialize(
        &self,
        _topic: &str,
        _headers: &mut Vec<RecordHeader>,
        payload: Option<&uuid::Uuid>,
    ) -> Result<Option<Bytes>> {
        Ok(payload.map(|payload| self.encoding.encode(&payload.to_string())))
    }
}

impl ConfiguredProducerSerializer<uuid::Uuid> for UuidSerializer {
    fn from_client_config(config: &ClientConfig, is_key: bool) -> Result<Self> {
        Ok(Self {
            encoding: TextEncoding::from_client_config(config, is_key)?,
        })
    }
}

impl ListInnerSerializer<uuid::Uuid> for UuidSerializer {
    const LIST_STRATEGY: ListSerializationStrategy = ListSerializationStrategy::ConstantSize;
}

/// Serializer for lists with nullable entries.
///
/// The value type is [`Vec<Option<T>>`] so null elements round-trip through the
/// Kafka list wire format.
#[derive(Debug, Clone)]
pub struct ListSerializer<T, S>
where
    S: ProducerSerializer<T>,
{
    inner: S,
    strategy: ListSerializationStrategy,
    _types: PhantomData<fn(T)>,
}

impl<T, S> ListSerializer<T, S>
where
    S: ProducerSerializer<T>,
{
    /// Create a list serializer using the layout strategy declared by the
    /// native inner serializer.
    #[must_use]
    pub const fn new(inner: S) -> Self
    where
        S: ListInnerSerializer<T>,
    {
        Self {
            inner,
            strategy: S::LIST_STRATEGY,
            _types: PhantomData,
        }
    }

    /// Create a list serializer using the fixed-size element strategy.
    #[must_use]
    pub const fn constant_size(inner: S) -> Self {
        Self {
            inner,
            strategy: ListSerializationStrategy::ConstantSize,
            _types: PhantomData,
        }
    }

    /// Create a list serializer using the variable-size element strategy.
    #[must_use]
    pub const fn variable_size(inner: S) -> Self {
        Self {
            inner,
            strategy: ListSerializationStrategy::VariableSize,
            _types: PhantomData,
        }
    }
}

impl<T, S> Default for ListSerializer<T, S>
where
    S: ListInnerSerializer<T> + Default,
{
    fn default() -> Self {
        Self::new(S::default())
    }
}

impl<T, S> ProducerSerializer<Vec<Option<T>>> for ListSerializer<T, S>
where
    T: Sync + 'static,
    S: ProducerSerializer<T>,
{
    fn serialize(
        &self,
        topic: &str,
        headers: &mut Vec<RecordHeader>,
        payload: Option<&Vec<Option<T>>>,
    ) -> Result<Option<Bytes>> {
        let Some(payload) = payload else {
            return Ok(None);
        };
        let list_size = list_len_to_i32(payload.len())?;
        let mut output = Vec::with_capacity(payload.len().saturating_mul(8).saturating_add(9));
        output.push(self.strategy.wire_flag());

        if self.strategy == ListSerializationStrategy::ConstantSize {
            let null_indexes = null_indexes(payload)?;
            write_i32(
                &mut output,
                i32::try_from(null_indexes.len()).map_err(|_error| {
                    ProducerError::InvalidConfig {
                        key: "list.null.indexes",
                        value: null_indexes.len().to_string(),
                    }
                })?,
            );
            for null_index in null_indexes {
                write_i32(&mut output, null_index);
            }
        }

        write_i32(&mut output, list_size);
        for entry in payload {
            match entry {
                Some(entry) => {
                    let Some(encoded) = self.inner.serialize(topic, headers, Some(entry))? else {
                        return Err(ProducerError::InvalidRecord {
                            field: "list.entry",
                            message: "inner serializer returned null for a non-null list entry",
                        });
                    };
                    if self.strategy == ListSerializationStrategy::VariableSize {
                        write_i32(&mut output, list_len_to_i32(encoded.len())?);
                    }
                    output.extend_from_slice(&encoded);
                },
                None => {
                    if self.strategy == ListSerializationStrategy::VariableSize {
                        write_i32(&mut output, -1);
                    }
                },
            }
        }

        Ok(Some(Bytes::from(output)))
    }

    fn close(&self) {
        self.inner.close();
    }
}

impl<T, S> ConfiguredProducerSerializer<Vec<Option<T>>> for ListSerializer<T, S>
where
    T: Sync + 'static,
    S: ListInnerSerializer<T> + ConfiguredProducerSerializer<T>,
{
    fn from_client_config(config: &ClientConfig, is_key: bool) -> Result<Self> {
        Ok(Self::new(S::from_client_config(config, is_key)?))
    }
}

fn list_len_to_i32(len: usize) -> Result<i32> {
    i32::try_from(len).map_err(|_error| ProducerError::InvalidConfig {
        key: "list.size",
        value: len.to_string(),
    })
}

fn null_indexes<T>(payload: &[Option<T>]) -> Result<Vec<i32>> {
    let mut indexes = Vec::new();
    for (index, entry) in payload.iter().enumerate() {
        if entry.is_none() {
            indexes.push(
                i32::try_from(index).map_err(|_error| ProducerError::InvalidConfig {
                    key: "list.null.index",
                    value: index.to_string(),
                })?,
            );
        }
    }
    Ok(indexes)
}

fn write_i32(output: &mut Vec<u8>, value: i32) {
    output.extend_from_slice(&value.to_be_bytes());
}

/// Producer wrapper that serializes typed keys and values before append.
#[derive(Debug)]
pub struct TypedProducer<K, V, KS, VS>
where
    K: Sync,
    V: Sync,
    KS: ProducerSerializer<K>,
    VS: ProducerSerializer<V>,
{
    producer: Option<Producer>,
    key_serializer: Option<KS>,
    value_serializer: Option<VS>,
    _types: PhantomData<fn(K, V)>,
}

impl<K, V, KS, VS> TypedProducer<K, V, KS, VS>
where
    K: Sync,
    V: Sync,
    KS: ProducerSerializer<K>,
    VS: ProducerSerializer<V>,
{
    /// Create a typed producer from an existing byte-oriented producer.
    #[must_use]
    pub const fn from_parts(producer: Producer, key_serializer: KS, value_serializer: VS) -> Self {
        Self {
            producer: Some(producer),
            key_serializer: Some(key_serializer),
            value_serializer: Some(value_serializer),
            _types: PhantomData,
        }
    }

    /// Borrow the underlying byte-oriented producer.
    #[must_use]
    pub fn producer(&self) -> &Producer {
        self.producer.as_ref().unwrap_or_else(abort_missing_state)
    }

    /// Mutably borrow the underlying byte-oriented producer.
    #[must_use]
    pub fn producer_mut(&mut self) -> &mut Producer {
        self.producer.as_mut().unwrap_or_else(abort_missing_state)
    }

    /// Consume this wrapper and return the underlying byte-oriented producer.
    #[must_use]
    pub fn into_inner(mut self) -> Producer {
        self.close_serializers();
        self.producer.take().unwrap_or_else(abort_missing_state)
    }

    /// Flush and close the typed producer.
    ///
    /// # Errors
    ///
    /// Returns any error from the underlying byte-oriented producer close.
    pub async fn close(mut self) -> Result<()> {
        let result = self
            .producer
            .take()
            .unwrap_or_else(abort_missing_state)
            .close()
            .await;
        self.close_serializers();
        result
    }

    /// Close immediately without waiting for buffered records or in-flight dispatches.
    pub fn close_now(mut self) {
        self.producer
            .take()
            .unwrap_or_else(abort_missing_state)
            .close_now();
        self.close_serializers();
    }

    /// Flush and close the typed producer, bounded by `timeout`.
    ///
    /// # Errors
    ///
    /// Returns any error from the underlying byte-oriented producer close.
    pub async fn close_timeout(mut self, timeout: std::time::Duration) -> Result<()> {
        let result = self
            .producer
            .take()
            .unwrap_or_else(abort_missing_state)
            .close_timeout(timeout)
            .await;
        self.close_serializers();
        result
    }

    /// Serialize typed key/value data and send the resulting record.
    ///
    /// # Errors
    ///
    /// Returns serializer, backpressure, routing, or dispatch errors.
    pub fn send(
        &mut self,
        mut record: ProducerRecord,
        key: Option<&K>,
        value: Option<&V>,
    ) -> Result<SendFuture> {
        let topic = std::sync::Arc::clone(&record.topic);
        record.key = self
            .key_serializer
            .as_ref()
            .unwrap_or_else(abort_missing_state)
            .serialize(&topic, &mut record.headers, key)?;
        record.value = self
            .value_serializer
            .as_ref()
            .unwrap_or_else(abort_missing_state)
            .serialize(&topic, &mut record.headers, value)?;
        self.producer_mut().send(record)
    }

    fn close_serializers(&mut self) {
        if let Some(key_serializer) = self.key_serializer.take() {
            close_serializer_quietly(&key_serializer);
        }
        if let Some(value_serializer) = self.value_serializer.take() {
            close_serializer_quietly(&value_serializer);
        }
    }
}

fn close_serializer_quietly<T, S>(serializer: &S)
where
    S: ProducerSerializer<T>,
{
    let _ignored = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        serializer.close();
    }));
}

fn abort_missing_state<T>() -> T {
    std::process::abort()
}

impl<K, V, KS, VS> Drop for TypedProducer<K, V, KS, VS>
where
    K: Sync,
    V: Sync,
    KS: ProducerSerializer<K>,
    VS: ProducerSerializer<V>,
{
    fn drop(&mut self) {
        self.close_serializers();
    }
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::expect_used,
        clippy::missing_assert_message,
        clippy::unwrap_used,
        reason = "Unit test fixtures fail fastest with contextual unwrap/expect calls."
    )]

    use std::{
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use bytes::Bytes;
    use kacrab_protocol::record::RecordHeader;

    use super::{
        BooleanSerializer, ByteArraySerializer, BytesSerializer, ConfiguredProducerSerializer,
        DoubleSerializer, FloatSerializer, IntegerSerializer, ListSerializer, LongSerializer,
        ProducerSerializer, ShortSerializer, StringSerializer, TypedProducer, UuidSerializer,
        VoidSerializer,
    };
    use crate::{
        config::ClientConfig,
        producer::{
            AccumulatorConfig, Producer, ProducerIdempotenceConfig, ProducerRecord,
            ProducerRuntimeConfig,
        },
        wire::{BrokerEndpoint, ConnectionConfig, WireClient},
    };

    #[test]
    fn bytes_serializer_preserves_bytes_and_nulls() {
        let serializer = BytesSerializer;
        let mut headers = Vec::new();

        assert_eq!(
            serializer
                .serialize("orders", &mut headers, Some(&Bytes::from_static(b"k")))
                .expect("serialize bytes"),
            Some(Bytes::from_static(b"k"))
        );
        assert_eq!(
            serializer
                .serialize("orders", &mut headers, None)
                .expect("serialize null"),
            None
        );
    }

    #[test]
    fn byte_array_serializer_vec_bytes_and_null_semantics() {
        let serializer = ByteArraySerializer;
        let mut headers = Vec::new();

        assert_eq!(
            serializer
                .serialize("orders", &mut headers, Some(&b"payload".to_vec()))
                .expect("serialize byte array"),
            Some(Bytes::from_static(b"payload"))
        );
        assert_eq!(
            serializer
                .serialize("orders", &mut headers, None)
                .expect("serialize null"),
            None
        );
    }

    #[test]
    fn string_serializer_utf8_and_null_semantics() {
        let serializer = StringSerializer::default();
        let mut headers = Vec::new();

        assert_eq!(
            serializer
                .serialize(
                    "orders",
                    &mut headers,
                    Some(&"\u{0111}\u{01A1}n".to_owned())
                )
                .expect("serialize string"),
            Some(Bytes::from_static(b"\xc4\x91\xc6\xa1n"))
        );
        assert_eq!(
            serializer
                .serialize("orders", &mut headers, None)
                .expect("serialize null"),
            None
        );
    }

    #[test]
    fn primitive_serializers_big_endian_and_null_semantics() {
        let mut headers = Vec::new();

        assert_eq!(
            BooleanSerializer
                .serialize("orders", &mut headers, Some(&true))
                .expect("serialize bool"),
            Some(Bytes::from_static(&[0x01]))
        );
        assert_eq!(
            BooleanSerializer
                .serialize("orders", &mut headers, Some(&false))
                .expect("serialize bool"),
            Some(Bytes::from_static(&[0x00]))
        );
        assert_eq!(
            ShortSerializer
                .serialize("orders", &mut headers, Some(&0x1234_i16))
                .expect("serialize short"),
            Some(Bytes::from_static(&[0x12, 0x34]))
        );
        assert_eq!(
            IntegerSerializer
                .serialize("orders", &mut headers, Some(&0x1234_5678_i32))
                .expect("serialize integer"),
            Some(Bytes::from_static(&[0x12, 0x34, 0x56, 0x78]))
        );
        assert_eq!(
            LongSerializer
                .serialize("orders", &mut headers, Some(&0x0102_0304_0506_0708_i64))
                .expect("serialize long"),
            Some(Bytes::from_static(&[
                0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08
            ]))
        );
        assert_eq!(
            FloatSerializer
                .serialize("orders", &mut headers, Some(&1.5_f32))
                .expect("serialize float"),
            Some(Bytes::from_static(&[0x3f, 0xc0, 0x00, 0x00]))
        );
        assert_eq!(
            DoubleSerializer
                .serialize("orders", &mut headers, Some(&1.5_f64))
                .expect("serialize double"),
            Some(Bytes::from_static(&[
                0x3f, 0xf8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
            ]))
        );
        assert_eq!(
            VoidSerializer
                .serialize("orders", &mut headers, Some(&()))
                .expect("serialize void"),
            None
        );
        assert_eq!(
            IntegerSerializer
                .serialize("orders", &mut headers, None)
                .expect("serialize null integer"),
            None
        );
    }

    #[test]
    fn uuid_serializer_uuid_to_string_utf8_and_null_semantics() {
        let serializer = UuidSerializer::default();
        let mut headers = Vec::new();
        let uuid = uuid::Uuid::from_u128(0x0011_2233_4455_6677_8899_aabb_ccdd_eeff);

        assert_eq!(
            serializer
                .serialize("orders", &mut headers, Some(&uuid))
                .expect("serialize uuid"),
            Some(Bytes::from_static(b"00112233-4455-6677-8899-aabbccddeeff"))
        );
        assert_eq!(
            serializer
                .serialize("orders", &mut headers, None)
                .expect("serialize null uuid"),
            None
        );
    }

    #[test]
    fn string_serializer_uses_encoding_config_precedence() {
        let config: ClientConfig = [
            ("serializer.encoding", "UTF-8"),
            ("key.serializer.encoding", "ISO-8859-1"),
            ("value.serializer.encoding", "US-ASCII"),
        ]
        .into_iter()
        .collect();
        let key_serializer =
            <StringSerializer as ConfiguredProducerSerializer<String>>::from_client_config(
                &config, true,
            )
            .expect("key string serializer config");
        let value_serializer =
            <StringSerializer as ConfiguredProducerSerializer<String>>::from_client_config(
                &config, false,
            )
            .expect("value string serializer config");
        let mut headers = Vec::new();

        assert_eq!(
            key_serializer
                .serialize("orders", &mut headers, Some(&"\u{00e9}".to_owned()))
                .expect("serialize latin1 string"),
            Some(Bytes::from_static(&[0xe9]))
        );
        assert_eq!(
            value_serializer
                .serialize("orders", &mut headers, Some(&"\u{00e9}".to_owned()))
                .expect("serialize ascii replacement string"),
            Some(Bytes::from_static(b"?"))
        );
    }

    #[test]
    fn uuid_serializer_uses_encoding_config() {
        let config: ClientConfig =
            std::iter::once(("key.serializer.encoding", "UTF-16BE")).collect();
        let serializer =
            <UuidSerializer as ConfiguredProducerSerializer<uuid::Uuid>>::from_client_config(
                &config, true,
            )
            .expect("uuid serializer config");
        let uuid = uuid::Uuid::from_u128(0x0011_2233_4455_6677_8899_aabb_ccdd_eeff);
        let expected = "00112233-4455-6677-8899-aabbccddeeff"
            .encode_utf16()
            .flat_map(u16::to_be_bytes)
            .collect::<Vec<u8>>();
        let mut headers = Vec::new();

        assert_eq!(
            serializer
                .serialize("orders", &mut headers, Some(&uuid))
                .expect("serialize utf16be uuid"),
            Some(Bytes::from(expected))
        );
    }

    #[test]
    fn list_serializer_constant_size_strategy_and_null_indexes() {
        let serializer = ListSerializer::new(IntegerSerializer);
        let mut headers = Vec::new();
        let values = vec![Some(0x0102_0304_i32), None, Some(0x0506_0708_i32)];

        assert_eq!(
            serializer
                .serialize("orders", &mut headers, Some(&values))
                .expect("serialize integer list"),
            Some(Bytes::from_static(&[
                0x00, // CONSTANT_SIZE strategy ordinal
                0x00, 0x00, 0x00, 0x01, // one null index
                0x00, 0x00, 0x00, 0x01, // null index 1
                0x00, 0x00, 0x00, 0x03, // list size
                0x01, 0x02, 0x03, 0x04, // first value
                0x05, 0x06, 0x07, 0x08, // third value
            ]))
        );
        assert_eq!(
            serializer
                .serialize("orders", &mut headers, None)
                .expect("serialize null list"),
            None
        );
    }

    #[test]
    fn list_serializer_variable_size_strategy_and_null_sentinel() {
        let serializer = ListSerializer::new(StringSerializer::default());
        let mut headers = Vec::new();
        let values = vec![Some("hi".to_owned()), None, Some("\u{00e9}".to_owned())];

        assert_eq!(
            serializer
                .serialize("orders", &mut headers, Some(&values))
                .expect("serialize string list"),
            Some(Bytes::from_static(&[
                0x01, // VARIABLE_SIZE strategy ordinal
                0x00, 0x00, 0x00, 0x03, // list size
                0x00, 0x00, 0x00, 0x02, b'h', b'i', // first value
                0xff, 0xff, 0xff, 0xff, // null sentinel -1
                0x00, 0x00, 0x00, 0x02, 0xc3, 0xa9, // third value
            ]))
        );
    }

    #[test]
    fn configured_list_serializer_builds_from_config_and_serializes() {
        let config: ClientConfig = std::iter::once(("key.serializer.encoding", "UTF-8")).collect();
        let serializer =
            <ListSerializer<String, StringSerializer> as ConfiguredProducerSerializer<
                Vec<Option<String>>,
            >>::from_client_config(&config, true)
            .expect("list serializer builds from config");
        let mut headers = Vec::new();

        assert!(
            serializer
                .serialize(
                    "orders",
                    &mut headers,
                    Some(&vec![Some("a".to_owned()), None])
                )
                .expect("serialize list")
                .is_some()
        );
    }

    #[test]
    fn configured_serializer_from_client_config_runs_configure() {
        let config: ClientConfig = [
            ("key.serializer", "ConfiguringSerializer"),
            ("tag", "key-a"),
        ]
        .into_iter()
        .collect();

        let serializer =
            <ConfiguringSerializer as ConfiguredProducerSerializer<Bytes>>::from_client_config(
                &config, true,
            )
            .expect("configured serializer");

        assert_eq!(serializer.configured_as_key.load(Ordering::Relaxed), 1);
        assert_eq!(serializer.configured_tag(), Some("key-a".to_owned()));
    }

    #[tokio::test]
    async fn typed_producer_serializes_key_value_and_allows_header_mutation() {
        let key_seen = Arc::new(AtomicBool::new(false));
        let value_seen = Arc::new(AtomicBool::new(false));
        let key_serializer = RecordingSerializer {
            prefix: Bytes::from_static(b"k:"),
            seen: Arc::clone(&key_seen),
        };
        let value_serializer = RecordingSerializer {
            prefix: Bytes::from_static(b"v:"),
            seen: Arc::clone(&value_seen),
        };
        let mut producer =
            TypedProducer::from_parts(byte_producer(), key_serializer, value_serializer);

        let _delivery = producer
            .send(
                ProducerRecord::new("orders", 0).header("trace-id", Bytes::from_static(b"abc")),
                Some(&Bytes::from_static(b"customer")),
                Some(&Bytes::from_static(b"value")),
            )
            .expect("typed send");

        assert!(key_seen.load(Ordering::Relaxed));
        assert!(value_seen.load(Ordering::Relaxed));
        assert_eq!(producer.producer().metrics().records_appended, 1);
        assert!(producer.producer().buffered_bytes() > 0);
    }

    #[test]
    fn typed_producer_closes_serializers_on_drop() {
        let close_count = Arc::new(AtomicUsize::new(0));
        let producer = typed_producer_with_closing_serializers(Arc::clone(&close_count));

        drop(producer);

        assert_eq!(close_count.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn typed_producer_close_closes_serializers() {
        let close_count = Arc::new(AtomicUsize::new(0));
        let producer = typed_producer_with_closing_serializers(Arc::clone(&close_count));

        producer.close().await.expect("typed producer close");

        assert_eq!(close_count.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn typed_producer_close_now_closes_serializers() {
        let close_count = Arc::new(AtomicUsize::new(0));
        let producer = typed_producer_with_closing_serializers(Arc::clone(&close_count));

        producer.close_now();

        assert_eq!(close_count.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn typed_producer_close_now_ignores_serializer_close_panic() {
        let close_count = Arc::new(AtomicUsize::new(0));
        let producer = TypedProducer::from_parts(
            byte_producer(),
            PanickingCloseSerializer,
            ClosingSerializer {
                close_count: Arc::clone(&close_count),
            },
        );

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            producer.close_now();
        }));

        assert!(result.is_ok());
        assert_eq!(close_count.load(Ordering::Relaxed), 1);
    }

    #[derive(Debug)]
    struct RecordingSerializer {
        prefix: Bytes,
        seen: Arc<AtomicBool>,
    }

    impl ProducerSerializer<Bytes> for RecordingSerializer {
        fn serialize(
            &self,
            _topic: &str,
            headers: &mut Vec<RecordHeader>,
            data: Option<&Bytes>,
        ) -> crate::producer::Result<Option<Bytes>> {
            assert!(!headers.is_empty());
            headers.push(RecordHeader {
                key: Bytes::from_static(b"serialized"),
                value: Some(Bytes::from_static(b"1")),
            });
            self.seen.store(true, Ordering::Relaxed);
            Ok(data.map(|data| {
                let mut bytes = Vec::with_capacity(self.prefix.len().saturating_add(data.len()));
                bytes.extend_from_slice(&self.prefix);
                bytes.extend_from_slice(data);
                Bytes::from(bytes)
            }))
        }
    }

    #[derive(Debug, Default)]
    struct ConfiguringSerializer {
        configured_as_key: AtomicUsize,
        configured_tag: std::sync::Mutex<Option<String>>,
    }

    impl ConfiguringSerializer {
        fn configured_tag(&self) -> Option<String> {
            self.configured_tag
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
        }
    }

    impl ProducerSerializer<Bytes> for ConfiguringSerializer {
        fn configure(&self, config: &ClientConfig, is_key: bool) -> crate::producer::Result<()> {
            self.configured_as_key
                .store(usize::from(is_key), Ordering::Relaxed);
            {
                let mut configured_tag = self
                    .configured_tag
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                *configured_tag = config
                    .get("tag")
                    .map(crate::config::ConfigValue::as_str)
                    .map(str::to_owned);
            }
            Ok(())
        }

        fn serialize(
            &self,
            _topic: &str,
            _headers: &mut Vec<RecordHeader>,
            payload: Option<&Bytes>,
        ) -> crate::producer::Result<Option<Bytes>> {
            Ok(payload.cloned())
        }
    }

    impl ConfiguredProducerSerializer<Bytes> for ConfiguringSerializer {}

    #[derive(Debug)]
    struct ClosingSerializer {
        close_count: Arc<AtomicUsize>,
    }

    impl ProducerSerializer<Bytes> for ClosingSerializer {
        fn serialize(
            &self,
            _topic: &str,
            _headers: &mut Vec<RecordHeader>,
            payload: Option<&Bytes>,
        ) -> crate::producer::Result<Option<Bytes>> {
            Ok(payload.cloned())
        }

        fn close(&self) {
            let _previous = self.close_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[derive(Debug)]
    struct PanickingCloseSerializer;

    impl ProducerSerializer<Bytes> for PanickingCloseSerializer {
        fn serialize(
            &self,
            _topic: &str,
            _headers: &mut Vec<RecordHeader>,
            payload: Option<&Bytes>,
        ) -> crate::producer::Result<Option<Bytes>> {
            Ok(payload.cloned())
        }

        fn close(&self) {
            panic!("serializer close panic");
        }
    }

    fn typed_producer_with_closing_serializers(
        close_count: Arc<AtomicUsize>,
    ) -> TypedProducer<Bytes, Bytes, ClosingSerializer, ClosingSerializer> {
        TypedProducer::from_parts(
            byte_producer(),
            ClosingSerializer {
                close_count: Arc::clone(&close_count),
            },
            ClosingSerializer { close_count },
        )
    }

    fn byte_producer() -> Producer {
        const TEST_LARGE_BATCH_SIZE: usize = 16 * 1024;

        let wire = WireClient::connect_with_brokers(
            ConnectionConfig::default(),
            "typed-producer-test",
            [BrokerEndpoint::new(
                7,
                "127.0.0.1:9092".parse().expect("valid socket address"),
            )],
        );
        Producer::from_parts(
            wire,
            ProducerRuntimeConfig {
                accumulator: AccumulatorConfig::default()
                    .batch_size(TEST_LARGE_BATCH_SIZE)
                    .linger(Duration::from_mins(1))
                    .buffer_memory(TEST_LARGE_BATCH_SIZE * 4),
                idempotence: ProducerIdempotenceConfig {
                    enabled: false,
                    ..ProducerIdempotenceConfig::default()
                },
                ..ProducerRuntimeConfig::default()
            },
        )
    }
}
