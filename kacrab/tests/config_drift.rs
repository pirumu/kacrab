//! Drift guard between the hand-written typed config (`config/clients.rs`) and
//! the generated config catalog (`config/catalog.rs`).
//!
//! The catalog is generated from the pinned Kafka upstream by
//! `kacrab-codegen -- config`, which reads `clients.rs` to decide which keys are
//! exposed (`Native`) versus merely cataloged (`NativeReview`/`FeatureGated`/…).
//! Because the typed API is curated by hand, the two can silently diverge — e.g.
//! adding a typed field without regenerating the catalog. These tests fail when
//! they do, so a Kafka version bump becomes: regenerate the catalog, run these,
//! and reconcile exactly what they report.
//!
//! Mirrors the codegen's own status rule (`classify_status`): a
//! `#[status(native)]` key in `clients.rs` always becomes `ConfigStatus::Native`
//! in the catalog (native keys take priority over the ssl./sasl. feature gating).

use std::collections::{BTreeMap, BTreeSet};

use kacrab::config::{ClientKind, ConfigOrigin, ConfigStatus, catalog_for};

const CLIENTS_SOURCE: &str = include_str!("../src/config/clients.rs");

/// Typed keys knowingly absent from the generated catalog. Empty: the codegen
/// now extracts constant-keyed and line-broken `define(...)` calls, so every
/// exposed key is cataloged.
const KNOWN_UNCATALOGED: &[(ClientKind, &str)] = &[];

fn is_known_uncataloged(client: ClientKind, key: &str) -> bool {
    KNOWN_UNCATALOGED
        .iter()
        .any(|(allowed_client, allowed_key)| *allowed_client == client && *allowed_key == key)
}

/// One `(client, key)` declared in `clients.rs`, and whether it is exposed as a
/// native typed field (`#[status(native)]`) or only cataloged for review.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Exposure {
    Native,
    Review,
}

/// Parse `clients.rs` for the `kafka_config!` declarations: `(client, key) ->
/// exposure`, mirroring the codegen's `parse_native_config_keys` but keeping the
/// review keys too.
fn parse_client_config() -> BTreeMap<(ClientKind, String), Exposure> {
    let mut declared = BTreeMap::new();
    let mut current_client: Option<ClientKind> = None;
    let mut pending_key: Option<String> = None;

    for line in CLIENTS_SOURCE.lines().map(str::trim) {
        if let Some(client) = parse_client_attr(line) {
            current_client = Some(client);
            pending_key = None;
        } else if let Some(key) = parse_key_attr(line) {
            pending_key = Some(key);
        } else if line.starts_with("#[status(")
            && let (Some(client), Some(key)) = (current_client, pending_key.take())
        {
            let exposure = if line == "#[status(native)]" {
                Exposure::Native
            } else {
                Exposure::Review
            };
            let _previous = declared.insert((client, key), exposure);
        }
    }
    declared
}

fn parse_client_attr(line: &str) -> Option<ClientKind> {
    let inner = line.strip_prefix("#[client(")?.strip_suffix(")]")?;
    match inner {
        "Producer" => Some(ClientKind::Producer),
        "Consumer" => Some(ClientKind::Consumer),
        "Admin" => Some(ClientKind::Admin),
        _ => None,
    }
}

fn parse_key_attr(line: &str) -> Option<String> {
    let inner = line.strip_prefix("#[key(\"")?.strip_suffix("\")]")?;
    Some(inner.to_owned())
}

/// `(client, key) -> (status, origin)` from the generated catalog.
fn catalog_index() -> BTreeMap<(ClientKind, String), (ConfigStatus, ConfigOrigin)> {
    let mut index = BTreeMap::new();
    for client in [
        ClientKind::Producer,
        ClientKind::Consumer,
        ClientKind::Admin,
    ] {
        for entry in catalog_for(client) {
            let _previous =
                index.insert((client, entry.key.to_owned()), (entry.status, entry.origin));
        }
    }
    index
}

/// Every key declared in `clients.rs` must exist in the generated catalog. A
/// miss means a typo or a config Kafka removed upstream.
#[test]
fn every_typed_config_key_exists_in_catalog() {
    let catalog = catalog_index();
    let mut missing = BTreeSet::new();
    for (client, key) in parse_client_config().keys() {
        if is_known_uncataloged(*client, key) {
            continue;
        }
        if !catalog.contains_key(&(*client, key.clone())) {
            let _inserted = missing.insert(format!("{client:?}:{key}"));
        }
    }
    assert!(
        missing.is_empty(),
        "typed config keys absent from the generated catalog (typo, or removed upstream — \
         regenerate the catalog): {missing:?}"
    );
}

/// Every `#[status(native)]` key in `clients.rs` must be `Native` in the
/// catalog. Catches exposing a typed field without regenerating the catalog
/// (the catalog still says `NativeReview`).
#[test]
fn exposed_native_keys_are_native_in_catalog() {
    let catalog = catalog_index();
    let mut drifted = BTreeSet::new();
    for ((client, key), exposure) in parse_client_config() {
        if exposure != Exposure::Native || is_known_uncataloged(client, &key) {
            continue;
        }
        match catalog.get(&(client, key.clone())) {
            Some((ConfigStatus::Native, _)) => {},
            other => {
                let _inserted = drifted.insert(format!("{client:?}:{key} -> {other:?}"));
            },
        }
    }
    assert!(
        drifted.is_empty(),
        "typed (native) config keys not marked Native in the generated catalog (regenerate the \
         catalog with `kacrab-codegen -- config`): {drifted:?}"
    );
}

/// Every Kafka-origin `Native` catalog entry must be an exposed native key in
/// `clients.rs`. Catches a stale catalog that still marks a key native after the
/// typed field was removed. Runtime-overlay (`KacrabRuntime`) natives are skipped
/// — they are not `clients.rs` keys.
#[test]
fn native_catalog_entries_are_exposed_in_clients() {
    let declared = parse_client_config();
    let mut orphaned = BTreeSet::new();
    for ((client, key), (status, origin)) in catalog_index() {
        if origin == ConfigOrigin::KacrabRuntime || status != ConfigStatus::Native {
            continue;
        }
        if declared.get(&(client, key.clone())) != Some(&Exposure::Native) {
            let _inserted = orphaned.insert(format!("{client:?}:{key}"));
        }
    }
    assert!(
        orphaned.is_empty(),
        "catalog marks these keys Native but clients.rs does not expose them as #[status(native)] \
         (stale catalog — regenerate): {orphaned:?}"
    );
}
