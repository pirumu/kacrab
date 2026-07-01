//! Typed value deserializers, the consumer analogue of the producer's
//! `ProducerSerializer`. `poll` returns bytes-first records; a deserializer maps
//! a record's raw `key`/`value` bytes into a typed value on demand.

use bytes::Bytes;

use super::error::{ConsumerError, Result};

/// Converts raw Kafka record bytes into a typed key or value.
pub trait ConsumerDeserializer<T>: Send + Sync + 'static {
    /// Deserialize one key or value. `None` bytes (a null key/tombstone value)
    /// deserialize to `None`.
    ///
    /// # Errors
    /// Returns [`ConsumerError::Deserialization`] when the bytes cannot be
    /// decoded into `T`.
    fn deserialize(&self, topic: &str, bytes: Option<&Bytes>) -> Result<Option<T>>;
}

/// Identity deserializer: returns the raw [`Bytes`] unchanged.
#[derive(Debug, Clone, Copy, Default)]
pub struct BytesDeserializer;

impl ConsumerDeserializer<Bytes> for BytesDeserializer {
    fn deserialize(&self, _topic: &str, bytes: Option<&Bytes>) -> Result<Option<Bytes>> {
        Ok(bytes.cloned())
    }
}

/// Deserializes record bytes into an owned byte vector.
#[derive(Debug, Clone, Copy, Default)]
pub struct ByteArrayDeserializer;

impl ConsumerDeserializer<Vec<u8>> for ByteArrayDeserializer {
    fn deserialize(&self, _topic: &str, bytes: Option<&Bytes>) -> Result<Option<Vec<u8>>> {
        Ok(bytes.map(|bytes| bytes.to_vec()))
    }
}

/// Deserializes UTF-8 record bytes into a [`String`], mirroring Kafka's
/// `StringDeserializer`.
#[derive(Debug, Clone, Copy, Default)]
pub struct StringDeserializer;

impl ConsumerDeserializer<String> for StringDeserializer {
    fn deserialize(&self, _topic: &str, bytes: Option<&Bytes>) -> Result<Option<String>> {
        bytes
            .map(|bytes| {
                std::str::from_utf8(bytes)
                    .map(str::to_owned)
                    .map_err(|_error| {
                        ConsumerError::Deserialization("record bytes are not valid UTF-8")
                    })
            })
            .transpose()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn string_deserializer_decodes_utf8_and_null() {
        let de = StringDeserializer;
        assert_eq!(
            de.deserialize("t", Some(&Bytes::from_static(b"hi")))
                .unwrap(),
            Some("hi".to_owned())
        );
        assert_eq!(de.deserialize("t", None).unwrap(), None);
        assert!(
            de.deserialize("t", Some(&Bytes::from_static(&[0xff, 0xfe])))
                .is_err()
        );
    }

    #[test]
    fn byte_deserializers_pass_bytes_through() {
        let raw = Bytes::from_static(b"abc");
        assert_eq!(
            BytesDeserializer.deserialize("t", Some(&raw)).unwrap(),
            Some(raw.clone())
        );
        assert_eq!(
            ByteArrayDeserializer.deserialize("t", Some(&raw)).unwrap(),
            Some(b"abc".to_vec())
        );
    }
}
