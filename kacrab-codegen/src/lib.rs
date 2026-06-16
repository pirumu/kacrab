//! Kafka protocol code generator — library portion.
//!
//! The pipeline is split into four stages, each owning its own error type:
//!
//! 1. [`parser`]      — load + parse JSON message specs into an IR.
//! 2. [`codegen`]     — lower the IR into a Rust `TokenStream`.
//! 3. [`format`]      — pretty-print the `TokenStream` via `prettyplease`.
//! 4. [`errors_java`] — (optional) parse upstream `Errors.java` for the Kafka error-code table.
//! 5. [`kafka_config`] — (optional) extract upstream `ConfigDef` declarations.
//!
//! Each stage's error follows the *struct + Kind* shape (context fields on
//! the struct, recoverable variants on the enum, `#[non_exhaustive]` on
//! both). The binary in `main.rs` aggregates them through `anyhow::Result`.

pub mod codegen;
pub mod errors_java;
pub mod format;
pub mod ir;
pub mod kafka_config;
pub mod parser;
pub mod upstream;
