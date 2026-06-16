//! Error types for [`crate::compression`].

use super::Compression;

/// Error from compression dispatch / encode / decode.
#[derive(Debug, thiserror::Error)]
#[error("compression failed for codec {codec:?}")]
#[non_exhaustive]
pub struct CompressionError {
    /// Which codec was involved (or attempted).
    pub codec: Compression,
    /// What specifically went wrong.
    #[source]
    pub kind: CompressionErrorKind,
}

impl CompressionError {
    /// Construct a `CompressionError` for a given codec.
    #[must_use]
    pub const fn new(codec: Compression, kind: CompressionErrorKind) -> Self {
        Self { codec, kind }
    }
}

/// Specific reason a compression operation failed.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum CompressionErrorKind {
    /// `attributes` bits 0–2 produced a value not in 0..=4.
    #[error("unknown compression type: {0}")]
    UnknownCodec(i16),

    /// Codec is recognised but its Cargo feature is not enabled.
    #[error("codec not enabled; rebuild with the corresponding feature")]
    CodecDisabled,

    /// Encoding (compress) failed in the underlying codec.
    #[error("encode failed: {message}")]
    EncodeFailed {
        /// Message from the underlying codec.
        message: String,
    },

    /// Decoding (decompress) failed in the underlying codec.
    #[error("decode failed: {message}")]
    DecodeFailed {
        /// Message from the underlying codec.
        message: String,
    },
}
