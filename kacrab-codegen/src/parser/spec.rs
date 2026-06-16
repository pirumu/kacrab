//! Top-level entry points: parse one schema file or all schemas in a directory.

use std::{
    fs,
    path::{Path, PathBuf},
};

use serde_json::Value;

use super::{
    comments::strip_comments,
    error::{ParseSchemaError, ParseSchemaErrorKind},
    field::{parse_common_structs, parse_fields},
};
use crate::ir::{
    message::{MessageSpec, MessageType},
    version_range::VersionRange,
};

/// Parse a single Kafka spec JSON file into a [`MessageSpec`].
pub fn parse_spec(path: &Path) -> Result<MessageSpec, ParseSchemaError> {
    parse_spec_inner(path).map_err(|kind| ParseSchemaError::new(path, kind))
}

/// Parse every `*.json` file in `dir` (non-recursive), sorted by message name.
pub fn parse_all_specs(dir: &Path) -> Result<Vec<MessageSpec>, ParseSchemaError> {
    let mut entries: Vec<PathBuf> = fs::read_dir(dir)
        .map_err(|e| ParseSchemaError::new(dir, ParseSchemaErrorKind::from(e)))?
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|ext| ext == "json"))
        .collect();
    entries.sort();

    let mut specs: Vec<MessageSpec> = Vec::with_capacity(entries.len());
    for path in entries {
        specs.push(parse_spec(&path)?);
    }
    specs.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(specs)
}

fn parse_spec_inner(path: &Path) -> Result<MessageSpec, ParseSchemaErrorKind> {
    let content = fs::read_to_string(path)?;
    let cleaned = strip_comments(&content);
    let value: Value = serde_json::from_str(&cleaned)?;

    let name = value
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| ParseSchemaErrorKind::MissingField {
            name: "name".to_owned(),
        })?
        .to_owned();

    let api_key = value
        .get("apiKey")
        .and_then(Value::as_i64)
        .map(i16::try_from)
        .transpose()?;

    let type_str = value.get("type").and_then(Value::as_str).ok_or_else(|| {
        ParseSchemaErrorKind::MissingField {
            name: "type".to_owned(),
        }
    })?;
    let message_type = match type_str {
        "request" => MessageType::Request,
        "response" => MessageType::Response,
        "data" | "header" => MessageType::Data,
        other => {
            return Err(ParseSchemaErrorKind::InvalidMessageType {
                value: other.to_owned(),
            });
        },
    };

    let valid_versions = VersionRange::parse(
        value
            .get("validVersions")
            .and_then(Value::as_str)
            .ok_or_else(|| ParseSchemaErrorKind::MissingField {
                name: "validVersions".to_owned(),
            })?,
    )?;

    let flexible_versions = match value.get("flexibleVersions").and_then(Value::as_str) {
        Some(s) => VersionRange::parse(s)?,
        None => VersionRange::None,
    };

    let listeners = value
        .get("listeners")
        .and_then(Value::as_array)
        .map(|arr| {
            arr.iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default();

    let latest_version_unstable = value
        .get("latestVersionUnstable")
        .and_then(Value::as_bool)
        .unwrap_or(false);

    let fields = parse_fields(value.get("fields"))?;
    let common_structs = parse_common_structs(value.get("commonStructs"))?;

    Ok(MessageSpec {
        name,
        api_key,
        message_type,
        valid_versions,
        flexible_versions,
        fields,
        common_structs,
        listeners,
        latest_version_unstable,
    })
}
