//! Stage 1: load Kafka message JSON specs from disk and parse them into an
//! intermediate representation consumed by [`crate::codegen`].

mod comments;
mod error;
mod field;
mod spec;

pub use error::{ParseSchemaError, ParseSchemaErrorKind};
pub use spec::{parse_all_specs, parse_spec};
