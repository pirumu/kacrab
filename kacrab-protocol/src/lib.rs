//! Kafka wire protocol — types, framing, record batches, compression, version negotiation.
//!
//! # Layout
//!
//! Each module owns its data, its operations, and its error (`mod foo` →
//! `foo.rs` + `foo/error.rs`). The crate root re-exports the user-facing surface
//! and the symbols `generated/` references via `use crate::*`.
//!
//! | Module        | Responsibility                                                |
//! |---------------|---------------------------------------------------------------|
//! | [`primitives`]| Fixed-width int/float/bool + (un)signed varint(long) helpers. |
//! | [`string`]    | [`KafkaString`] + length-prefixed string read/write helpers.  |
//! | [`bytes_io`]  | Length-prefixed raw byte read/write helpers.                  |
//! | [`uuid`]      | [`KafkaUuid`] + base64 URL-safe parsing (Java-compat).        |
//! | [`tagged`]    | [`RawTaggedField`] + flexible-version tagged-field section.   |
//! | [`crc`]       | CRC32C compute + validation.                                  |
//! | [`frame`]     | TCP length-prefix framing + request encoding helpers.         |
//! | [`record`]    | Record batch v2: [`RecordBatch`], [`Record`], [`RecordHeader`]. |
//! | [`compression`]| Codec dispatch (gzip/snappy/lz4/zstd, feature-gated).        |
//! | [`version`]   | API version resolution + header version selection.            |
//! | [`generated`] | Codegen output (committed). One module per Kafka message.     |
//!
//! # Errors
//!
//! Each module exposes its own error type co-located in `foo/error.rs`.
//! The top-level [`ProtocolError`] is a thin facade that `#[from]`-converts
//! every module error so callers crossing layer boundaries can propagate
//! errors with a single `?`. See [`crate::error`] for the full mapping.
pub mod bytes_io;
pub mod compression;
pub mod crc;
pub mod error;
pub mod frame;
pub mod generated;
pub mod primitives;
pub mod record;
pub mod string;
pub mod tagged;
pub mod uuid;
pub mod version;

// ---------------------------------------------------------------------------
// Internal re-exports (`#[doc(hidden)]`) — symbols re-exported at the crate root
// for `generated/*.rs` (each generated file uses `use crate::*;`). Not part of
// the public API. Prefer the module path in hand-written code:
// `crate::primitives::read_i32`, not `crate::read_i32`.
// ---------------------------------------------------------------------------
#[doc(hidden)]
pub use crate::bytes_io::{
    bytes_len, compact_bytes_len, compact_nullable_bytes_len, nullable_bytes_len, read_bytes,
    read_compact_bytes, read_compact_nullable_bytes, read_nullable_bytes, write_bytes,
    write_compact_bytes, write_compact_nullable_bytes, write_nullable_bytes,
};
#[doc(hidden)]
pub use crate::primitives::{
    array_length_len, compact_array_length_len, read_array_length, read_bool,
    read_compact_array_length, read_f64, read_i8, read_i16, read_i32, read_i64, read_u16, read_u32,
    signed_varint_len, signed_varlong_len, unsigned_varint_len, unsigned_varlong_len,
    write_array_length, write_bool, write_compact_array_length, write_f64, write_i8, write_i16,
    write_i32, write_i64, write_u16, write_u32,
};
#[doc(hidden)]
pub use crate::string::{
    compact_nullable_string_len, compact_string_len, nullable_string_len,
    read_compact_nullable_string, read_compact_string, read_nullable_string, read_string,
    string_len, write_compact_nullable_string, write_compact_string, write_nullable_string,
    write_string,
};
#[doc(hidden)]
pub use crate::tagged::{read_tagged_fields, tagged_fields_len, write_tagged_fields};
#[doc(hidden)]
pub use crate::uuid::{read_uuid, write_uuid};
// ---------------------------------------------------------------------------
// Facade re-exports — the user-visible API surface.
// ---------------------------------------------------------------------------
pub use crate::{
    error::{ProtocolError, Result},
    string::KafkaString,
    tagged::RawTaggedField,
    uuid::KafkaUuid,
    version::{UnsupportedFieldVersion, UnsupportedVersion},
};
