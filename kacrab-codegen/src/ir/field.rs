//! Field-level IR.

use super::version_range::VersionRange;

/// One field of a Kafka message or nested struct.
#[expect(
    clippy::struct_excessive_bools,
    reason = "the spec assigns each independent boolean a distinct semantic role; bit-packing \
              them into a flags type would lose readability without runtime benefit."
)]
#[derive(Debug, Clone)]
pub struct FieldSpec {
    /// Field name as declared in the spec.
    pub name: String,
    /// Wire type (primitive, struct reference, or array).
    pub field_type: FieldType,
    /// Versions this field is present in.
    pub versions: VersionRange,
    /// Versions in which the field may be `null`.
    pub nullable_versions: VersionRange,
    /// Versions that carry this field as a tagged field (KIP-482).
    pub tagged_versions: VersionRange,
    /// Tag id when [`FieldSpec::tagged_versions`] is non-empty.
    pub tag: Option<i32>,
    /// Free-form description from the spec; surfaced as a doc comment.
    pub about: String,
    /// Optional default value (raw spec string, type-dependent).
    pub default: Option<String>,
    /// Whether a non-default value may be dropped when the negotiated version
    /// lacks this field.
    pub ignorable: bool,
    /// Whether this field acts as a map key for cross-version diffs.
    pub map_key: bool,
    /// Optional `entityType` annotation (e.g. `topicName`).
    pub entity_type: Option<String>,
    /// Whether the codec may borrow rather than copy bytes for this field.
    pub zero_copy: bool,
    /// Versions that opt this field into the flexible wire format.
    pub flexible_versions: VersionRange,
    /// Distinguishes "not specified" from explicit `"flexibleVersions"` keys.
    pub has_flexible_versions_override: bool,
    /// Inline nested struct fields.
    pub fields: Vec<Self>,
}

/// The wire type of field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FieldType {
    /// `bool`.
    Bool,
    /// 8-bit signed integer.
    Int8,
    /// 16-bit signed integer.
    Int16,
    /// 32-bit signed integer.
    Int32,
    /// 64-bit signed integer.
    Int64,
    /// 16-bit unsigned integer.
    Uint16,
    /// 64-bit IEEE float.
    Float64,
    /// UTF-8 string (length-prefixed).
    String,
    /// Length-prefixed byte buffer.
    Bytes,
    /// 128-bit UUID.
    Uuid,
    /// Embedded record-batch payload.
    Records,
    /// A reference to a named struct (inline or common).
    Struct(String),
    /// An array of another field type, e.g. `[]int32` or `[]FetchTopic`.
    Array(Box<Self>),
}

/// Reason a Kafka field-type string couldn't be parsed.
#[derive(Debug, thiserror::Error)]
#[error("invalid field type: {raw:?}")]
#[non_exhaustive]
pub struct ParseError {
    /// The offending input string.
    pub raw: String,
}

impl ParseError {
    fn new(s: &str) -> Self {
        Self { raw: s.to_owned() }
    }
}

impl FieldType {
    /// Parse a field type string from a Kafka spec.
    ///
    /// Handles primitives (`"int32"`, `"string"`, `"uuid"`, ...), arrays
    /// (`"[]int32"`, `"[]FetchTopic"`), and named struct references.
    pub fn parse(s: &str) -> Result<Self, ParseError> {
        let s = s.trim();
        if s.is_empty() {
            return Err(ParseError::new(s));
        }
        if let Some(inner) = s.strip_prefix("[]") {
            let element = Self::parse(inner)?;
            return Ok(Self::Array(Box::new(element)));
        }
        match s {
            "bool" => Ok(Self::Bool),
            "int8" => Ok(Self::Int8),
            "int16" => Ok(Self::Int16),
            "int32" => Ok(Self::Int32),
            "int64" => Ok(Self::Int64),
            "uint16" => Ok(Self::Uint16),
            "float64" => Ok(Self::Float64),
            "string" => Ok(Self::String),
            "bytes" => Ok(Self::Bytes),
            "uuid" => Ok(Self::Uuid),
            "records" => Ok(Self::Records),
            other if other.starts_with(char::is_uppercase) => Ok(Self::Struct(other.to_owned())),
            other => Err(ParseError::new(other)),
        }
    }
}
