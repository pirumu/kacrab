//! Common-struct IR (shared struct definitions referenced by multiple fields).

use super::{field::FieldSpec, version_range::VersionRange};

/// A shared struct definition referenced by name from one or more fields.
#[derive(Debug, Clone)]
pub struct CommonStructSpec {
    /// Struct name (matches the `FieldType::Struct(name)` reference).
    pub name: String,
    /// Versions in which this struct definition is valid.
    pub versions: VersionRange,
    /// Fields of this struct.
    pub fields: Vec<FieldSpec>,
}
