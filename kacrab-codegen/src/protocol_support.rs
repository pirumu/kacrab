//! Stage 6 (optional): report which Kafka protocol versions this client
//! implements, per API key.
//!
//! The report is built from the same [`crate::parser`] output the protocol
//! generator consumes, then cross-checked against the `client_api_info` table
//! committed in the generated protocol crate. Anything the generated table and
//! the schema snapshot disagree on lands in the report as
//! `client_matches_schema: false` with a reason, so schema drift and APIs that
//! upstream has withdrawn are visible instead of silently assumed.

mod client_api_info;
mod error;
mod model;

use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
};

pub use client_api_info::{ClientApiInfo, ClientApiInfoTable, parse_client_api_info};
pub use error::{ProtocolSupportError, ProtocolSupportErrorKind};
pub use model::{ApiSupport, ProtocolSupportDocument, ReportProvenance};

use crate::{
    codegen::{ApiKeyEntry, collect_api_key_entries},
    ir::{
        message::{MessageSpec, MessageType},
        version_range::VersionRange,
    },
};

/// File inside a schema snapshot directory that pins the upstream source ref.
const SOURCE_REF_FILE: &str = "SOURCE_REF";

/// Build the protocol support report from parsed schemas plus the committed
/// client table.
pub fn build_report(
    specs: &[MessageSpec],
    client: &ClientApiInfoTable,
    provenance: &ReportProvenance,
) -> ProtocolSupportDocument {
    let entries = collect_api_key_entries(specs);
    let mut apis: Vec<ApiSupport> = Vec::with_capacity(entries.len());
    for entry in &entries {
        if let Some(spec) = request_spec(specs, entry.api_key) {
            apis.push(api_row(spec, entry, client.get(&entry.variant).copied()));
        }
    }

    let covered: BTreeSet<&str> = apis.iter().map(|row| row.name.as_str()).collect();
    let client_only_apis: Vec<String> = client
        .keys()
        .filter(|name| !covered.contains(name.as_str()))
        .cloned()
        .collect();
    let mismatch_count = apis.iter().filter(|row| !row.client_matches_schema).count();

    ProtocolSupportDocument {
        source_ref: provenance.source_ref.clone(),
        schemas_dir: provenance.schemas_dir.clone(),
        client_api_info_source: provenance.client_api_info_source.clone(),
        api_count: apis.len(),
        mismatch_count,
        client_only_apis,
        apis,
    }
}

/// Read the pinned upstream source ref from a schema snapshot directory.
///
/// Returns `None` when the directory is an upstream checkout rather than a
/// snapshot, since only snapshots carry a `SOURCE_REF` file.
pub fn read_source_ref(schemas_dir: &Path) -> Option<String> {
    let path: PathBuf = schemas_dir.join(SOURCE_REF_FILE);
    let raw = fs::read_to_string(path).ok()?;
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    Some(trimmed.to_owned())
}

fn request_spec(specs: &[MessageSpec], api_key: i16) -> Option<&MessageSpec> {
    specs
        .iter()
        .find(|spec| spec.message_type == MessageType::Request && spec.api_key == Some(api_key))
}

fn api_row(spec: &MessageSpec, entry: &ApiKeyEntry, client: Option<ClientApiInfo>) -> ApiSupport {
    let (schema_min_version, schema_max_version) = range_bounds(&spec.valid_versions);
    let mismatches = collect_mismatches(spec, entry, client);

    ApiSupport {
        api_key: entry.api_key,
        name: entry.variant.clone(),
        request_schema: spec.name.clone(),
        schema_valid_versions: spec.valid_versions.to_string(),
        schema_min_version,
        schema_max_version,
        schema_flexible_versions: spec.flexible_versions.to_string(),
        schema_flexible_versions_start: flexible_start(&spec.flexible_versions),
        latest_version_unstable: spec.latest_version_unstable,
        client_min_version: client.map(|info| info.min_version),
        client_max_version: client.map(|info| info.max_version),
        client_flexible_versions_start: client
            .map(|info| info.flexible_versions_start)
            .filter(|start| *start != i16::MAX),
        client_matches_schema: mismatches.is_empty(),
        mismatches,
    }
}

/// Compare the committed client table against what the schema declares.
///
/// `entry` is the generator's own lowering of `validVersions` /
/// `flexibleVersions`, so comparing against it flags a generated file that no
/// longer matches its schema snapshot. `validVersions: none` is checked
/// separately: the generator lowers it to `0-0`, which would otherwise hide an
/// API upstream has withdrawn behind an apparently valid version bound.
fn collect_mismatches(
    spec: &MessageSpec,
    entry: &ApiKeyEntry,
    client: Option<ClientApiInfo>,
) -> Vec<String> {
    let Some(client) = client else {
        return vec!["API is absent from the generated client_api_info table".to_owned()];
    };
    if spec.valid_versions.is_none() {
        return vec![format!(
            "schema declares validVersions `none` (API withdrawn upstream) but client_api_info \
             still advertises {}-{}",
            client.min_version, client.max_version
        )];
    }

    let mut mismatches = Vec::new();
    if client.min_version != entry.min_version {
        mismatches.push(format!(
            "client min_version {} does not match schema validVersions `{}`",
            client.min_version, spec.valid_versions
        ));
    }
    if client.max_version != entry.max_version {
        mismatches.push(format!(
            "client max_version {} does not match schema validVersions `{}`",
            client.max_version, spec.valid_versions
        ));
    }
    if client.flexible_versions_start != entry.flexible_versions_start {
        mismatches.push(format!(
            "client flexible_versions_start {} does not match schema flexibleVersions `{}`",
            client.flexible_versions_start, spec.flexible_versions
        ));
    }
    mismatches
}

/// Split a range into `(min, max)`, where `None` means "unbounded or absent".
const fn range_bounds(range: &VersionRange) -> (Option<i16>, Option<i16>) {
    match range {
        VersionRange::None => (None, None),
        VersionRange::From(start) => (Some(*start), None),
        VersionRange::Range(start, end) => (Some(*start), Some(*end)),
    }
}

const fn flexible_start(range: &VersionRange) -> Option<i16> {
    match range {
        VersionRange::None => None,
        VersionRange::From(start) | VersionRange::Range(start, _) => Some(*start),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ClientApiInfo, ClientApiInfoTable, ProtocolSupportDocument, ReportProvenance, build_report,
    };
    use crate::ir::{
        message::{MessageSpec, MessageType},
        version_range::VersionRange,
    };

    fn request(
        name: &str,
        api_key: i16,
        valid: VersionRange,
        flexible: VersionRange,
    ) -> MessageSpec {
        MessageSpec {
            name: name.to_owned(),
            api_key: Some(api_key),
            message_type: MessageType::Request,
            valid_versions: valid,
            flexible_versions: flexible,
            fields: Vec::new(),
            common_structs: Vec::new(),
            listeners: vec!["broker".to_owned()],
            latest_version_unstable: false,
        }
    }

    fn table(rows: &[(&str, i16, i16, i16)]) -> ClientApiInfoTable {
        let mut table = ClientApiInfoTable::new();
        for (name, min_version, max_version, flexible_versions_start) in rows {
            let _previous = table.insert(
                (*name).to_owned(),
                ClientApiInfo {
                    min_version: *min_version,
                    max_version: *max_version,
                    flexible_versions_start: *flexible_versions_start,
                },
            );
        }
        table
    }

    fn report(specs: &[MessageSpec], client: &ClientApiInfoTable) -> ProtocolSupportDocument {
        build_report(specs, client, &ReportProvenance::default())
    }

    #[test]
    fn matching_client_table_reports_schema_bounds() {
        let specs = vec![request(
            "ProduceRequest",
            0,
            VersionRange::Range(3, 13),
            VersionRange::From(9),
        )];

        let document = report(&specs, &table(&[("Produce", 3, 13, 9)]));

        let row = document.apis.first().expect("Produce row should exist");
        assert_eq!(row.name, "Produce", "variant name should drop the suffix");
        assert_eq!(
            (row.schema_min_version, row.schema_max_version),
            (Some(3), Some(13)),
            "schema bounds should be reported verbatim"
        );
        assert_eq!(
            (row.client_min_version, row.client_max_version),
            (Some(3), Some(13)),
            "client bounds should come from the generated table"
        );
        assert!(
            row.client_matches_schema && row.mismatches.is_empty(),
            "an in-sync table should not be flagged: {:?}",
            row.mismatches
        );
        assert_eq!(document.mismatch_count, 0, "no row should be flagged");
    }

    #[test]
    fn stale_client_max_version_is_flagged() {
        let specs = vec![request(
            "ProduceRequest",
            0,
            VersionRange::Range(3, 13),
            VersionRange::From(9),
        )];

        let document = report(&specs, &table(&[("Produce", 3, 12, 9)]));

        let row = document.apis.first().expect("Produce row should exist");
        assert!(
            !row.client_matches_schema,
            "a client table one version behind the schema must be flagged"
        );
        assert!(
            row.mismatches
                .iter()
                .any(|reason| reason.contains("max_version 12")),
            "the reason should name the drifted bound: {:?}",
            row.mismatches
        );
        assert_eq!(
            document.mismatch_count, 1,
            "the flagged row should be counted"
        );
    }

    #[test]
    fn stale_flexible_versions_start_is_flagged() {
        let specs = vec![request(
            "FetchRequest",
            1,
            VersionRange::Range(4, 18),
            VersionRange::From(12),
        )];

        let document = report(&specs, &table(&[("Fetch", 4, 18, 11)]));

        let row = document.apis.first().expect("Fetch row should exist");
        assert!(
            !row.client_matches_schema,
            "a drifted flexible threshold changes the wire format and must be flagged"
        );
        assert!(
            row.mismatches
                .iter()
                .any(|reason| reason.contains("flexible_versions_start 11")),
            "the reason should name the drifted threshold: {:?}",
            row.mismatches
        );
    }

    #[test]
    fn withdrawn_api_is_flagged_despite_generated_zero_range() {
        let specs = vec![request(
            "LeaderAndIsrRequest",
            4,
            VersionRange::None,
            VersionRange::None,
        )];

        let document = report(&specs, &table(&[("LeaderAndIsr", 0, 0, i16::MAX)]));

        let row = document
            .apis
            .first()
            .expect("LeaderAndIsr row should exist");
        assert_eq!(
            (row.schema_min_version, row.schema_max_version),
            (None, None),
            "`validVersions: none` has no bounds to report"
        );
        assert!(
            !row.client_matches_schema,
            "an API withdrawn upstream must not look supported"
        );
        assert!(
            row.mismatches
                .iter()
                .any(|reason| reason.contains("withdrawn upstream")),
            "the reason should explain the withdrawal: {:?}",
            row.mismatches
        );
    }

    #[test]
    fn api_missing_from_client_table_is_flagged() {
        let specs = vec![request(
            "ShareFetchRequest",
            78,
            VersionRange::Range(1, 2),
            VersionRange::From(0),
        )];

        let document = report(&specs, &ClientApiInfoTable::new());

        let row = document.apis.first().expect("ShareFetch row should exist");
        assert_eq!(
            (row.client_min_version, row.client_max_version),
            (None, None),
            "an unimplemented API has no client bounds"
        );
        assert!(
            !row.client_matches_schema,
            "an API the client cannot speak must be flagged"
        );
    }

    #[test]
    fn responses_and_client_only_apis_stay_out_of_the_rows() {
        let mut response = request(
            "ProduceResponse",
            0,
            VersionRange::Range(3, 13),
            VersionRange::From(9),
        );
        response.message_type = MessageType::Response;
        let specs = vec![
            request(
                "ProduceRequest",
                0,
                VersionRange::Range(3, 13),
                VersionRange::From(9),
            ),
            response,
        ];

        let document = report(
            &specs,
            &table(&[("Produce", 3, 13, 9), ("Fetch", 4, 18, 12)]),
        );

        assert_eq!(document.api_count, 1, "only request schemas become rows");
        assert_eq!(
            document.client_only_apis,
            vec!["Fetch".to_owned()],
            "client APIs without a schema should be reported separately"
        );
    }
}
