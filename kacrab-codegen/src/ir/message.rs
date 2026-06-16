//! Top-level message-spec IR.

use super::{common_struct::CommonStructSpec, field::FieldSpec, version_range::VersionRange};

/// The category of a Kafka protocol message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MessageType {
    /// A client → broker request.
    Request,
    /// A broker → client response.
    Response,
    /// A standalone data record (e.g. record-batch headers).
    Data,
}

/// One Kafka message spec parsed from a `*.json` file.
///
/// Captures every spec-level attribute consumed by the codegen stage:
/// API key, valid/flexible version ranges, listeners, and the full field tree.
#[derive(Debug, Clone)]
pub struct MessageSpec {
    /// Schema name (e.g. `FetchRequest`).
    pub name: String,
    /// API key wire id; absent for non-RPC `data` messages.
    pub api_key: Option<i16>,
    /// Whether this is a request, response, or data message.
    pub message_type: MessageType,
    /// Versions the message itself supports.
    pub valid_versions: VersionRange,
    /// Versions that use the flexible (KIP-482) wire format.
    pub flexible_versions: VersionRange,
    /// Top-level fields.
    pub fields: Vec<FieldSpec>,
    /// Common structs referenced by `fields` via [`super::field::FieldType::Struct`].
    pub common_structs: Vec<CommonStructSpec>,
    /// Listener names this message is exposed on (e.g. `broker`, `controller`).
    pub listeners: Vec<String>,
    /// True if the highest declared version is still considered unstable upstream.
    pub latest_version_unstable: bool,
}
