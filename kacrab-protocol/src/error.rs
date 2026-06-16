//! Top-level protocol error — a thin facade over the per-module error types.
//!
//! Every module under this crate has its own error in `foo/error.rs`. This
//! enum lifts those into one type via `#[from]` so callers that cross several
//! layers can keep using `?` without bespoke conversions.
//!
//! Authors of new fallible code should prefer returning the **module's own**
//! error and let `#[from]` handle the lift; reserve direct construction of
//! [`ProtocolError`] for the crate boundary (lib facade, generated code).

use crate::{
    bytes_io::BytesError,
    compression::CompressionError,
    crc::CrcMismatch,
    frame::FrameError,
    primitives::PrimitiveError,
    record::RecordError,
    string::StringError,
    tagged::TaggedFieldError,
    uuid::UuidError,
    version::{UnsupportedFieldVersion, UnsupportedVersion},
};

/// Convenience alias for `Result<T, ProtocolError>`.
pub type Result<T> = core::result::Result<T, ProtocolError>;

/// Crate-wide error facade. Every module error converts in via `#[from]`.
///
/// # Why a facade and not a single big enum?
///
/// Module errors stay narrow — callers that only deal with primitives won't be
/// forced to `match` a `RecordError` variant they can't produce. The facade
/// only widens at the boundary where errors need to flow uniformly (e.g. the
/// generated message decoders).
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ProtocolError {
    /// Primitive int / varint / array-length read failed.
    #[error(transparent)]
    Primitive(#[from] PrimitiveError),

    /// Length-prefixed raw bytes read/write failed.
    #[error(transparent)]
    Bytes(#[from] BytesError),

    /// `KafkaString` read/write failed (length, UTF-8, null marker).
    #[error(transparent)]
    String(#[from] StringError),

    /// `KafkaUuid` parse failed.
    #[error(transparent)]
    Uuid(#[from] UuidError),

    /// Tagged-fields section is malformed (out-of-order tag, bad size).
    #[error(transparent)]
    Tagged(#[from] TaggedFieldError),

    /// CRC32C check failed on a record batch.
    #[error(transparent)]
    Crc(#[from] CrcMismatch),

    /// TCP frame length prefix is invalid or oversized.
    #[error(transparent)]
    Frame(#[from] FrameError),

    /// Record batch decode failed (CRC, magic, length, record count).
    #[error(transparent)]
    Record(#[from] RecordError),

    /// Compression codec dispatch / encode / decode failed.
    #[error(transparent)]
    Compression(#[from] CompressionError),

    /// API key / version negotiation failed.
    #[error(transparent)]
    Version(#[from] UnsupportedVersion),

    /// A generated message field was set for a version where it is absent.
    #[error(transparent)]
    FieldVersion(#[from] UnsupportedFieldVersion),
}
