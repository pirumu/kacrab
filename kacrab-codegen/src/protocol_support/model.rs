//! Serializable model for the protocol support report.

use serde::{Deserialize, Serialize};

/// Where a report came from, recorded so a checked-in report stays auditable.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ReportProvenance {
    /// Schema directory as passed on the command line.
    pub schemas_dir: String,
    /// Generated Rust file the `client_api_info` table was read from.
    pub client_api_info_source: String,
    /// Pinned upstream source ref, when the schema snapshot records one.
    pub source_ref: Option<String>,
}

/// Machine-readable report of the Kafka protocol versions this client supports.
///
/// One row per request schema that declares an API key, ordered by API key.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ProtocolSupportDocument {
    /// Pinned upstream source ref of the schema snapshot, for example
    /// `apache/kafka@4.3.0`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_ref: Option<String>,
    /// Schema directory the schemas were parsed from.
    pub schemas_dir: String,
    /// Generated Rust file the `client_api_info` table was read from.
    pub client_api_info_source: String,
    /// Number of rows in `apis`.
    pub api_count: usize,
    /// Number of rows whose `client_matches_schema` is `false`.
    pub mismatch_count: usize,
    /// `ApiKey` variants the client exposes that have no request schema.
    pub client_only_apis: Vec<String>,
    /// Per-API support rows, ordered by API key.
    pub apis: Vec<ApiSupport>,
}

/// Schema-declared versus client-implemented version support for one API key.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ApiSupport {
    /// Wire API key.
    pub api_key: i16,
    /// `ApiKey` enum variant name, for example `Produce`.
    pub name: String,
    /// Request schema file name, for example `ProduceRequest`.
    pub request_schema: String,
    /// Raw `validVersions` string from the schema, for example `3-13`.
    pub schema_valid_versions: String,
    /// Lowest schema-declared version; `null` when `validVersions` is `none`.
    pub schema_min_version: Option<i16>,
    /// Highest schema-declared version; `null` when unbounded or `none`.
    pub schema_max_version: Option<i16>,
    /// Raw `flexibleVersions` string from the schema, for example `9+`.
    pub schema_flexible_versions: String,
    /// First schema-declared flexible version; `null` when never flexible.
    pub schema_flexible_versions_start: Option<i16>,
    /// True when upstream still marks the highest schema version unstable.
    pub latest_version_unstable: bool,
    /// Lowest version the client implements; `null` when the API is absent
    /// from the generated `client_api_info` table.
    pub client_min_version: Option<i16>,
    /// Highest version the client implements; `null` when the API is absent
    /// from the generated `client_api_info` table.
    pub client_max_version: Option<i16>,
    /// First flexible version the client implements; `null` when the client
    /// treats the API as never flexible.
    pub client_flexible_versions_start: Option<i16>,
    /// True when the client table agrees with the schema on every version bound.
    pub client_matches_schema: bool,
    /// Human-readable reasons `client_matches_schema` is `false`.
    pub mismatches: Vec<String>,
}
