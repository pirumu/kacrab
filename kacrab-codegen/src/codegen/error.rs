//! Errors from the IR → Rust lowering stage.

/// Anything that can go wrong while emitting Rust for one schema.
#[derive(Debug, thiserror::Error)]
#[error("failed to generate code for schema {schema:?}")]
#[non_exhaustive]
pub struct CodegenError {
    /// Schema being lowered when this fired.
    pub schema: String,
    /// Underlying cause; preserved in the [`std::error::Error::source`] chain.
    #[source]
    pub kind: CodegenErrorKind,
}

/// Reason codegen for one schema gave up.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum CodegenErrorKind {
    /// Schema mentioned a Kafka primitive we have no Rust mapping for.
    #[error("unknown Kafka primitive type {name:?}")]
    UnknownPrimitive {
        /// Primitive name as it appeared in the schema.
        name: String,
    },
    /// Two items collapsed onto the same Rust identifier.
    ///
    /// Usually a Kafka schema quirk — JSON specs allow fields like `value`
    /// and `Value` to coexist; Rust modules don't.
    #[error("name collision: {name:?} already emitted in this module")]
    NameCollision {
        /// The Rust identifier that two items both wanted.
        name: String,
    },
    /// We emitted tokens that `syn` then refused to reparse.
    ///
    /// If you see this, it's a bug in *this* crate, not the schema.
    #[error("invalid token stream produced for {item:?}")]
    InvalidTokens {
        /// Item whose tokens didn't round-trip.
        item: String,
        /// `syn`'s complaint.
        #[source]
        source: syn::Error,
    },
    /// A schema default could not be represented as the Rust type selected for
    /// the field.
    #[error("invalid default {value:?} for field {field:?} of type {field_type:?}")]
    InvalidDefaultValue {
        /// Field whose default failed to lower.
        field: String,
        /// Raw default string from the Kafka schema.
        value: String,
        /// Debug representation of the IR type being emitted.
        field_type: String,
    },
}

impl CodegenError {
    /// Glue a schema name onto a `kind` to build the full error.
    pub fn new(schema: impl Into<String>, kind: CodegenErrorKind) -> Self {
        Self {
            schema: schema.into(),
            kind,
        }
    }
}
