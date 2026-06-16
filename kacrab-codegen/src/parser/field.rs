//! Parse the `fields` and `commonStructs` arrays of a Kafka message spec.

use serde_json::Value;

use super::error::ParseSchemaErrorKind;
use crate::ir::{
    common_struct::CommonStructSpec,
    field::{FieldSpec, FieldType},
    version_range::VersionRange,
};

pub(crate) fn parse_fields(value: Option<&Value>) -> Result<Vec<FieldSpec>, ParseSchemaErrorKind> {
    let Some(arr) = value.and_then(Value::as_array) else {
        return Ok(Vec::new());
    };
    arr.iter().map(parse_field).collect()
}

pub(crate) fn parse_field(value: &Value) -> Result<FieldSpec, ParseSchemaErrorKind> {
    let name = value["name"]
        .as_str()
        .ok_or_else(|| ParseSchemaErrorKind::MissingField {
            name: "field name".to_owned(),
        })?
        .to_owned();

    let type_str = value["type"]
        .as_str()
        .ok_or_else(|| ParseSchemaErrorKind::MissingField {
            name: format!("field type for {name}"),
        })?;
    let field_type = FieldType::parse(type_str)?;

    let versions = VersionRange::parse(value["versions"].as_str().ok_or_else(|| {
        ParseSchemaErrorKind::MissingField {
            name: format!("versions for {name}"),
        }
    })?)?;

    let nullable_versions = parse_optional_version_range(value, "nullableVersions")?;
    let tagged_versions = parse_optional_version_range(value, "taggedVersions")?;
    let has_flexible_versions_override = value.get("flexibleVersions").is_some();
    let flexible_versions = parse_optional_version_range(value, "flexibleVersions")?;

    let tag = value
        .get("tag")
        .and_then(Value::as_i64)
        .map(i32::try_from)
        .transpose()?;

    let about = value
        .get("about")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .unwrap_or_default();

    let default = value.get("default").map(|v| match v {
        Value::String(s) => s.clone(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        Value::Null => "null".to_owned(),
        other => other.to_string(),
    });

    let ignorable = value
        .get("ignorable")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let map_key = value
        .get("mapKey")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let entity_type = value
        .get("entityType")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let zero_copy = value
        .get("zeroCopy")
        .and_then(Value::as_bool)
        .unwrap_or(false);

    let fields = parse_fields(value.get("fields"))?;

    Ok(FieldSpec {
        name,
        field_type,
        versions,
        nullable_versions,
        tagged_versions,
        tag,
        about,
        default,
        ignorable,
        map_key,
        entity_type,
        zero_copy,
        flexible_versions,
        has_flexible_versions_override,
        fields,
    })
}

pub(crate) fn parse_optional_version_range(
    value: &Value,
    key: &str,
) -> Result<VersionRange, ParseSchemaErrorKind> {
    match value.get(key).and_then(Value::as_str) {
        Some(s) => Ok(VersionRange::parse(s)?),
        None => Ok(VersionRange::None),
    }
}

pub(crate) fn parse_common_structs(
    value: Option<&Value>,
) -> Result<Vec<CommonStructSpec>, ParseSchemaErrorKind> {
    let Some(arr) = value.and_then(Value::as_array) else {
        return Ok(Vec::new());
    };
    arr.iter()
        .map(|v| {
            let name = v["name"]
                .as_str()
                .ok_or_else(|| ParseSchemaErrorKind::MissingField {
                    name: "commonStruct name".to_owned(),
                })?
                .to_owned();
            let versions = VersionRange::parse(v["versions"].as_str().ok_or_else(|| {
                ParseSchemaErrorKind::MissingField {
                    name: "commonStruct versions".to_owned(),
                }
            })?)?;
            let fields = parse_fields(v.get("fields"))?;
            Ok(CommonStructSpec {
                name,
                versions,
                fields,
            })
        })
        .collect()
}
