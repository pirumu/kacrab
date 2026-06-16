//! Record-batch compression codecs.
//!
//! The [`Compression`] enum is always available so [`crate::record::RecordBatch`]
//! can read the compression type from `attributes` regardless of which codec
//! features are enabled. Actual compress/decompress operations require the
//! corresponding Cargo feature:
//!
//! | Feature  | Codec  | Backend             | Pure Rust | HC mode |
//! |----------|--------|---------------------|-----------|---------|
//! | `gzip`   | Gzip   | `flate2`            | yes       | n/a     |
//! | `snappy` | Snappy | `snap`              | yes       | n/a     |
//! | `lz4`    | Lz4    | `lz4_flex`          | yes       | no      |
//! | `lz4-hc` | Lz4    | `lz4` (C-FFI)       | no        | yes     |
//! | `zstd`   | Zstd   | `zstd` (C-FFI)      | no        | n/a     |
//!
//! `compression` is a meta-feature that enables `gzip + snappy + lz4 + zstd`
//! (pure-Rust LZ4). Opt into `lz4-hc` explicitly if you need
//! High-Compression mode — see the LZ4 section below.
//!
//! ## LZ4 backend selection
//!
//! * **`lz4`** — `lz4_flex` block API behind a Kafka-compatible custom frame. Fast mode only; the
//!   `level` argument is ignored. Pure Rust, no C toolchain at build time, cross-compile clean.
//! * **`lz4-hc`** — `lz4` crate (FFI to liblz4) behind the same frame format. Levels 3..=12 use HC
//!   mode; levels 0..=2 fall back to fast mode. Requires a C compiler at build time.
//!
//! Both features are independent — enabling `lz4-hc` alone is sufficient
//! (the C lib handles fast mode too). When **both** are enabled, `lz4-hc`
//! wins at runtime; `lz4_flex` is linked but unused. Most users should
//! pick one.
//!
//! For high compression ratio in Kafka workloads, prefer **`zstd`** over
//! `lz4-hc` — modern Kafka clients (Java, librdkafka) default to fast LZ4
//! when LZ4 is selected, and switch to zstd when ratio matters.

#[cfg(feature = "gzip")]
pub mod gzip;
#[cfg(any(feature = "lz4", feature = "lz4-hc"))]
pub mod lz4;
#[cfg(feature = "snappy")]
pub mod snappy;
#[cfg(feature = "zstd")]
pub mod zstd;

pub mod error;

pub use self::error::{CompressionError, CompressionErrorKind};

/// Result alias for compression operations.
pub type Result<T> = core::result::Result<T, CompressionError>;

/// Kafka record-batch compression codec, encoded in bits 0–2 of the batch
/// `attributes` field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i16)]
#[non_exhaustive]
pub enum Compression {
    /// No compression.
    None = 0,
    /// Gzip (`flate2`).
    Gzip = 1,
    /// Snappy (`snap`).
    Snappy = 2,
    /// LZ4 frame format (`lz4_flex`).
    Lz4 = 3,
    /// Zstandard (`zstd` crate).
    Zstd = 4,
}

impl Compression {
    /// Decode the codec from a record-batch `attributes` field.
    /// Only bits 0–2 are examined; higher bits are masked off.
    pub const fn from_attributes(attributes: i16) -> Result<Self> {
        match attributes & 0x07 {
            0 => Ok(Self::None),
            1 => Ok(Self::Gzip),
            2 => Ok(Self::Snappy),
            3 => Ok(Self::Lz4),
            4 => Ok(Self::Zstd),
            n => Err(CompressionError::new(
                Self::None,
                CompressionErrorKind::UnknownCodec(n),
            )),
        }
    }

    /// Compress `payload` at the codec default level.
    pub fn compress(self, payload: &[u8]) -> Result<Vec<u8>> {
        self.compress_with_level(payload, None)
    }

    /// Compress `payload` at the given level (codec-specific).
    ///
    /// * `Gzip` accepts `0..=9`, default `6`.
    /// * `Zstd` accepts `1..=22`, default `3`.
    /// * `Snappy` ignores the level.
    /// * `Lz4` level handling depends on the active backend feature:
    ///   * with `lz4` only — level ignored, always fast mode.
    ///   * with `lz4-hc` — level `0..=2` → fast, `3..=12` → HC mode, values above `12` clamp to
    ///     `12`, negative levels treated as `0`.
    pub fn compress_with_level(self, payload: &[u8], level: Option<i32>) -> Result<Vec<u8>> {
        match self {
            Self::None => {
                let _ = level;
                Ok(payload.to_vec())
            },
            #[cfg(feature = "gzip")]
            Self::Gzip => gzip::compress_with_level(payload, level),
            #[cfg(feature = "snappy")]
            Self::Snappy => snappy::compress_with_level(payload, level),
            #[cfg(any(feature = "lz4", feature = "lz4-hc"))]
            Self::Lz4 => lz4::compress_with_level(payload, level),
            #[cfg(feature = "zstd")]
            Self::Zstd => zstd::compress_with_level(payload, level),
            #[cfg(not(feature = "gzip"))]
            Self::Gzip => Err(CompressionError::new(
                self,
                CompressionErrorKind::CodecDisabled,
            )),
            #[cfg(not(feature = "snappy"))]
            Self::Snappy => Err(CompressionError::new(
                self,
                CompressionErrorKind::CodecDisabled,
            )),
            #[cfg(not(any(feature = "lz4", feature = "lz4-hc")))]
            Self::Lz4 => Err(CompressionError::new(
                self,
                CompressionErrorKind::CodecDisabled,
            )),
            #[cfg(not(feature = "zstd"))]
            Self::Zstd => Err(CompressionError::new(
                self,
                CompressionErrorKind::CodecDisabled,
            )),
        }
    }

    /// Decompress `payload`. Returns the decompressed payload, or an error if
    /// the required Cargo feature is not enabled.
    pub fn decompress(self, payload: &[u8]) -> Result<Vec<u8>> {
        match self {
            Self::None => Ok(payload.to_vec()),
            #[cfg(feature = "gzip")]
            Self::Gzip => gzip::decompress(payload),
            #[cfg(feature = "snappy")]
            Self::Snappy => snappy::decompress(payload),
            #[cfg(any(feature = "lz4", feature = "lz4-hc"))]
            Self::Lz4 => lz4::decompress(payload),
            #[cfg(feature = "zstd")]
            Self::Zstd => zstd::decompress(payload),
            #[cfg(not(feature = "gzip"))]
            Self::Gzip => Err(CompressionError::new(
                self,
                CompressionErrorKind::CodecDisabled,
            )),
            #[cfg(not(feature = "snappy"))]
            Self::Snappy => Err(CompressionError::new(
                self,
                CompressionErrorKind::CodecDisabled,
            )),
            #[cfg(not(any(feature = "lz4", feature = "lz4-hc")))]
            Self::Lz4 => Err(CompressionError::new(
                self,
                CompressionErrorKind::CodecDisabled,
            )),
            #[cfg(not(feature = "zstd"))]
            Self::Zstd => Err(CompressionError::new(
                self,
                CompressionErrorKind::CodecDisabled,
            )),
        }
    }
}
