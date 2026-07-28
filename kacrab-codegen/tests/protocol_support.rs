//! End-to-end coverage for the `protocol-support` subcommand.
//!
//! Runs the real binary against the bundled schema snapshot and the committed
//! `kacrab-protocol` generated table, then asserts the emitted JSON against the
//! version bounds hand-extracted for the broker compatibility work.
#![allow(
    clippy::expect_used,
    clippy::panic,
    reason = "integration test setup should fail fast when the subcommand, its output file, or an \
              expected report row is missing"
)]

use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use kacrab_codegen::{
    parser,
    protocol_support::{ApiSupport, ProtocolSupportDocument},
};

fn manifest_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn schemas_dir() -> PathBuf {
    manifest_dir().join("schemas")
}

fn client_api_info_path() -> PathBuf {
    manifest_dir()
        .join("..")
        .join("kacrab-protocol")
        .join("src")
        .join("generated.rs")
}

fn scratch_path(name: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!("kacrab-codegen-{name}-{}.json", std::process::id()));
    if path.exists() {
        fs::remove_file(&path).expect("stale scratch report should be removable");
    }
    path
}

fn run_report(output: &Path) -> ProtocolSupportDocument {
    let status = Command::new(env!("CARGO_BIN_EXE_kacrab-codegen"))
        .arg("protocol-support")
        .arg("--schemas-dir")
        .arg(schemas_dir())
        .arg("--client-api-info")
        .arg(client_api_info_path())
        .arg("--output")
        .arg(output)
        .status()
        .expect("protocol-support subcommand should run");
    assert!(
        status.success(),
        "protocol-support should exit successfully"
    );

    let raw = fs::read_to_string(output).expect("report should be written to --output");
    serde_json::from_str(&raw).expect("report should be valid JSON")
}

fn row<'a>(document: &'a ProtocolSupportDocument, name: &str) -> &'a ApiSupport {
    document
        .apis
        .iter()
        .find(|api| api.name == name)
        .unwrap_or_else(|| panic!("report should carry a row for {name}"))
}

fn assert_bounds(document: &ProtocolSupportDocument, name: &str, min: i16, max: i16) {
    let api = row(document, name);
    assert_eq!(
        (api.schema_min_version, api.schema_max_version),
        (Some(min), Some(max)),
        "{name} schema validVersions should be {min}-{max}"
    );
    assert_eq!(
        (api.client_min_version, api.client_max_version),
        (Some(min), Some(max)),
        "{name} client_api_info should implement {min}-{max}"
    );
    assert!(
        api.client_matches_schema,
        "{name} should be in sync with its schema: {:?}",
        api.mismatches
    );
}

#[test]
fn report_covers_every_request_schema_with_an_api_key() {
    let output = scratch_path("support-coverage");
    let document = run_report(&output);

    let specs = parser::parse_all_specs(&schemas_dir()).expect("bundled schemas should parse");
    let mut expected: Vec<i16> = specs
        .iter()
        .filter(|spec| spec.message_type == kacrab_codegen::ir::message::MessageType::Request)
        .filter_map(|spec| spec.api_key)
        .collect();
    expected.sort_unstable();

    let reported: Vec<i16> = document.apis.iter().map(|api| api.api_key).collect();
    assert_eq!(
        reported, expected,
        "every request schema with an API key should appear exactly once, in API key order"
    );
    assert_eq!(
        document.api_count,
        document.apis.len(),
        "api_count should match the emitted rows"
    );
    assert!(
        document.client_only_apis.is_empty(),
        "the client should not advertise APIs without a schema: {:?}",
        document.client_only_apis
    );
    assert_eq!(
        document.source_ref.as_deref(),
        Some("apache/kafka@4.3.0"),
        "the report should pin the schema snapshot's source ref"
    );

    fs::remove_file(&output).expect("scratch report should be removable");
}

#[test]
fn produce_and_fetch_match_the_hand_extracted_table() {
    let output = scratch_path("support-spot-check");
    let document = run_report(&output);

    assert_bounds(&document, "Produce", 3, 13);
    assert_bounds(&document, "Fetch", 4, 18);
    assert_bounds(&document, "OffsetCommit", 2, 10);
    assert_bounds(&document, "FindCoordinator", 0, 6);
    assert_bounds(&document, "CreateTopics", 2, 7);
    assert_bounds(&document, "IncrementalAlterConfigs", 0, 1);
    assert_bounds(&document, "DescribeUserScramCredentials", 0, 0);
    assert_bounds(&document, "ConsumerGroupHeartbeat", 0, 1);
    // The hand-extracted table lumped both KIP-932 APIs as 1-2; the schemas say
    // ShareGroupHeartbeat is v1 only.
    assert_bounds(&document, "ShareGroupHeartbeat", 1, 1);
    assert_bounds(&document, "ShareFetch", 1, 2);
    assert_bounds(&document, "StreamsGroupHeartbeat", 0, 0);

    assert_eq!(
        row(&document, "Produce").schema_flexible_versions_start,
        Some(9),
        "Produce turns flexible at v9"
    );
    assert_eq!(
        row(&document, "Fetch").client_flexible_versions_start,
        Some(12),
        "Fetch turns flexible at v12"
    );

    fs::remove_file(&output).expect("scratch report should be removable");
}

#[test]
fn withdrawn_broker_apis_are_schema_only_and_nothing_is_flagged() {
    let output = scratch_path("support-mismatch");
    let document = run_report(&output);

    let flagged: Vec<(&str, &Vec<String>)> = document
        .apis
        .iter()
        .filter(|api| !api.client_matches_schema)
        .map(|api| (api.name.as_str(), &api.mismatches))
        .collect();

    assert!(
        flagged.is_empty(),
        "the generated client_api_info table should match the schema snapshot exactly; a flagged \
         row means it drifted: {flagged:?}"
    );
    assert_eq!(
        document.mismatch_count, 0,
        "mismatch_count should match the flagged rows"
    );

    // The APIs upstream withdrew in 4.0 still have a schema, so they still get a
    // report row — but the client must not advertise a version for them.
    for name in [
        "LeaderAndIsr",
        "StopReplica",
        "UpdateMetadata",
        "ControlledShutdown",
    ] {
        let api = row(&document, name);
        assert_eq!(
            api.schema_valid_versions, "none",
            "{name} should declare no valid versions upstream"
        );
        assert_eq!(
            (
                api.client_min_version,
                api.client_max_version,
                api.client_flexible_versions_start
            ),
            (None, None, None),
            "{name} is withdrawn upstream and must not appear in client_api_info at all"
        );
    }

    fs::remove_file(&output).expect("scratch report should be removable");
}
