//! Intermediate representation of a parsed Kafka message spec, consumed by
//! [`crate::codegen`].

pub mod common_struct;
pub mod field;
pub mod message;
pub mod version_range;

pub use common_struct::CommonStructSpec;
pub use field::{FieldSpec, FieldType};
pub use message::{MessageSpec, MessageType};
pub use version_range::VersionRange;
