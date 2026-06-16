//! Error types for [`crate::uuid`].
//!
//! Each variant already carries enough context (length, error string) on its
//! own, so this is a flat enum rather than the `struct + Kind` shape used by
//! modules with cross-variant context (e.g. [`crate::record`]).

/// Error from [`crate::uuid::KafkaUuid`] parsing or generation.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum UuidError {
    /// Input string is longer than the maximum base64 representation.
    #[error("input string too long ({length} chars, max {max})")]
    StringTooLong {
        /// Length of the input string.
        length: usize,
        /// Maximum allowed length.
        max: usize,
    },

    /// Base64 decoding failed.
    #[error("invalid base64: {message}")]
    InvalidBase64 {
        /// Underlying error message from the base64 decoder.
        message: String,
    },

    /// Decoded byte length does not match the UUID size.
    #[error("decoded {actual} bytes, expected {expected}")]
    InvalidLength {
        /// Expected byte length (16).
        expected: usize,
        /// Actual decoded byte length.
        actual: usize,
    },

    /// `random()` exhausted its retry budget.
    #[error("random UUID generation exhausted {retries} retries")]
    RandomExhausted {
        /// Number of attempts made before giving up.
        retries: u32,
    },
}
