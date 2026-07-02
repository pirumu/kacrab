//! Stage 4 (optional): parse upstream Kafka's `clients/.../Errors.java` and
//! emit a Rust mirror of every `(code, name, retriable)` triple.

mod codegen;
mod error;
mod parser;
mod retriable;

pub use codegen::lower;
pub use error::{ErrorsJavaError, ErrorsJavaErrorKind};
pub use parser::{ErrorEntry, scrape};
