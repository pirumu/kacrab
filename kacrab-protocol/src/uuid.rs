//! [`KafkaUuid`] — 128-bit UUID with Kafka-specific encoding.
//!
//! Wraps [`uuid::Uuid`] and adds:
//!
//! * Base64 URL-safe (no padding) string format — matches the Java client and topic-id wire format.
//! * Reserved constants ([`KafkaUuid::ZERO`], [`KafkaUuid::ONE`],
//!   [`KafkaUuid::METADATA_TOPIC_ID`]).
//! * [`KafkaUuid::random`] that avoids reserved values and base64 reps that start with `-`.

pub mod error;

use std::{cmp::Ordering, fmt};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use bytes::{Buf, Bytes, BytesMut};

pub use self::error::UuidError;
use crate::primitives::check_remaining;

/// Result alias for UUID parse operations.
pub type Result<T> = core::result::Result<T, UuidError>;

/// 128-bit UUID with Kafka-specific encoding semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct KafkaUuid(uuid::Uuid);

impl KafkaUuid {
    /// Size in bytes (128 bits).
    pub const SIZE: usize = 16;

    /// Maximum length of a base64 representation (16 bytes → 22 chars + slack).
    pub const MAX_BASE64_LEN: usize = 24;

    /// Maximum retries for [`Self::random`] before giving up.
    pub const MAX_RANDOM_RETRIES: u32 = 64;

    /// The nil UUID (all zeros).
    pub const ZERO: Self = Self(uuid::Uuid::nil());

    /// UUID with value 1. Reserved — never returned by [`Self::random`].
    pub const ONE: Self = Self(uuid::Uuid::from_u128(1));

    /// Metadata topic ID in `KRaft` mode (alias for [`Self::ONE`]).
    pub const METADATA_TOPIC_ID: Self = Self::ONE;

    /// Wrap a raw [`uuid::Uuid`].
    #[must_use]
    pub const fn from_uuid(inner: uuid::Uuid) -> Self {
        Self(inner)
    }

    /// Construct from two 64-bit halves (most-significant first).
    #[must_use]
    pub const fn from_parts(msb: u64, lsb: u64) -> Self {
        Self(uuid::Uuid::from_u64_pair(msb, lsb))
    }

    /// Generate a random non-reserved type-4 UUID whose base64 form does not
    /// start with `-`. Returns [`UuidError::RandomExhausted`] if no candidate
    /// is found within [`Self::MAX_RANDOM_RETRIES`] attempts.
    pub fn random() -> Result<Self> {
        for _ in 0..Self::MAX_RANDOM_RETRIES {
            let candidate = Self(uuid::Uuid::new_v4());
            if candidate.is_reserved() {
                continue;
            }
            let encoded = URL_SAFE_NO_PAD.encode(candidate.0.as_bytes());
            if !encoded.starts_with('-') {
                return Ok(candidate);
            }
        }
        Err(UuidError::RandomExhausted {
            retries: Self::MAX_RANDOM_RETRIES,
        })
    }

    /// Borrow the inner [`uuid::Uuid`].
    #[must_use]
    pub const fn inner(&self) -> &uuid::Uuid {
        &self.0
    }

    /// Most-significant 64 bits.
    #[must_use]
    pub const fn most_significant_bits(self) -> u64 {
        self.0.as_u64_pair().0
    }

    /// Least-significant 64 bits.
    #[must_use]
    pub const fn least_significant_bits(self) -> u64 {
        self.0.as_u64_pair().1
    }

    /// `true` for [`Self::ZERO`] and [`Self::ONE`].
    #[must_use]
    pub const fn is_reserved(self) -> bool {
        let (msb, lsb) = self.0.as_u64_pair();
        msb == 0 && (lsb == 0 || lsb == 1)
    }

    /// `true` if this is the nil (all-zero) UUID.
    #[must_use]
    pub const fn is_nil(self) -> bool {
        self.0.is_nil()
    }

    /// Raw 16-byte representation.
    #[must_use]
    pub const fn to_bytes(self) -> [u8; 16] {
        *self.0.as_bytes()
    }

    /// Parse from a base64 URL-safe string (no padding).
    pub fn from_base64(s: &str) -> Result<Self> {
        if s.len() > Self::MAX_BASE64_LEN {
            return Err(UuidError::StringTooLong {
                length: s.len(),
                max: Self::MAX_BASE64_LEN,
            });
        }
        let decoded = URL_SAFE_NO_PAD
            .decode(s)
            .map_err(|e| UuidError::InvalidBase64 {
                message: e.to_string(),
            })?;
        if decoded.len() != Self::SIZE {
            return Err(UuidError::InvalidLength {
                expected: Self::SIZE,
                actual: decoded.len(),
            });
        }
        let arr: [u8; 16] = decoded.try_into().map_err(|_| UuidError::InvalidLength {
            expected: Self::SIZE,
            actual: 0,
        })?;
        Ok(Self(uuid::Uuid::from_bytes(arr)))
    }
}

impl Default for KafkaUuid {
    fn default() -> Self {
        Self::ZERO
    }
}

impl fmt::Display for KafkaUuid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&URL_SAFE_NO_PAD.encode(self.0.as_bytes()))
    }
}

impl PartialOrd for KafkaUuid {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for KafkaUuid {
    fn cmp(&self, other: &Self) -> Ordering {
        let (self_high, self_low) = self.0.as_u64_pair();
        let (other_high, other_low) = other.0.as_u64_pair();
        self_high.cmp(&other_high).then(self_low.cmp(&other_low))
    }
}

impl From<uuid::Uuid> for KafkaUuid {
    fn from(u: uuid::Uuid) -> Self {
        Self(u)
    }
}

impl From<KafkaUuid> for uuid::Uuid {
    fn from(k: KafkaUuid) -> Self {
        k.0
    }
}

// ---------------------------------------------------------------------------
// Wire-level read/write — UUID is fixed 16 bytes, no length prefix.
// ---------------------------------------------------------------------------

/// Read a UUID (16 bytes, raw).
pub fn read_uuid(buf: &mut Bytes) -> crate::error::Result<KafkaUuid> {
    check_remaining(buf, KafkaUuid::SIZE)?;
    let mut arr = [0u8; 16];
    buf.copy_to_slice(&mut arr);
    Ok(KafkaUuid(uuid::Uuid::from_bytes(arr)))
}

/// Write a UUID (16 bytes, raw).
#[expect(
    clippy::trivially_copy_pass_by_ref,
    reason = "Generated protocol encoders pass borrowed struct fields and array elements; by-ref \
              keeps the wire helper signature uniform with non-Copy field writers."
)]
pub fn write_uuid(buf: &mut BytesMut, value: &KafkaUuid) {
    buf.extend_from_slice(&value.to_bytes());
}
