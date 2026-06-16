//! Integration coverage for protocol schema parsing and Rust lowering.
#![allow(
    clippy::unwrap_used,
    reason = "integration test setup should fail fast when temporary fixture I/O or codegen calls \
              fail"
)]

use std::{
    fmt::Write as _,
    fs,
    path::{Path, PathBuf},
};

use kacrab_codegen::{
    codegen,
    errors_java::{self, ErrorsJavaErrorKind},
    ir::{
        field::{FieldSpec, FieldType},
        message::MessageType,
        version_range::VersionRange,
    },
    parser::{self, ParseSchemaErrorKind},
};

fn scratch_dir(name: &str) -> PathBuf {
    let mut dir = std::env::temp_dir();
    dir.push(format!("kacrab-codegen-{name}-{}", std::process::id()));
    if dir.exists() {
        fs::remove_dir_all(&dir).unwrap();
    }
    fs::create_dir_all(&dir).unwrap();
    dir
}

fn write_file(dir: &Path, name: &str, body: &str) -> PathBuf {
    let path = dir.join(name);
    fs::write(&path, body).unwrap();
    path
}

fn protocol_schema(name: &str, api_key: i16, kind: &str) -> String {
    format!(
        r#"{{
  // parser must strip line comments before serde_json sees the document
  "apiKey": {api_key},
  "type": "{kind}",
  "name": "{name}",
  "validVersions": "0-3",
  "flexibleVersions": "2+",
  "listeners": ["broker", "controller"],
  "latestVersionUnstable": true,
  "fields": [
    {{ "name": "Enabled", "type": "bool", "versions": "0+", "default": true }},
    {{ "name": "Tiny", "type": "int8", "versions": "0+", "default": "-128" }},
    {{ "name": "Small", "type": "int16", "versions": "0+", "default": "32767" }},
    {{ "name": "Count", "type": "int32", "versions": "0+", "default": "0x7fffffff" }},
    {{ "name": "Offset", "type": "int64", "versions": "0+", "default": "-9223372036854775808" }},
    {{ "name": "Unsigned", "type": "uint16", "versions": "0+", "default": "65535" }},
    {{ "name": "Ratio", "type": "float64", "versions": "0+", "default": "1.25" }},
    {{ "name": "Name", "type": "string", "versions": "0+", "nullableVersions": "1+", "default": "hello" }},
    {{ "name": "Blob", "type": "bytes", "versions": "0+", "nullableVersions": "2+", "zeroCopy": true }},
    {{ "name": "TopicId", "type": "uuid", "versions": "1+" }},
    {{ "name": "Records", "type": "records", "versions": "0+" }},
    {{
      "name": "Items",
      "type": "[]Item",
      "versions": "0+",
      "fields": [
        {{ "name": "ItemId", "type": "int32", "versions": "0+" }},
        {{ "name": "Tags", "type": "[]string", "versions": "0+", "nullableVersions": "2+" }}
      ]
    }},
    {{ "name": "Common", "type": "CommonThing", "versions": "0+" }},
    {{ "name": "TaggedNote", "type": "string", "versions": "0+", "taggedVersions": "2+", "tag": 0, "default": "" }}
  ],
  "commonStructs": [
    {{
      "name": "CommonThing",
      "versions": "0+",
      "fields": [
        {{ "name": "Code", "type": "int16", "versions": "0+" }}
      ]
    }}
  ]
}}"#
    )
}

#[test]
fn parse_spec_preserves_schema_metadata_and_field_annotations() {
    let dir = scratch_dir("parse-spec");
    let path = write_file(
        &dir,
        "ShapeRequest.json",
        &protocol_schema("ShapeRequest", 77, "request"),
    );

    let spec = parser::parse_spec(&path).unwrap();

    assert_eq!(spec.name, "ShapeRequest");
    assert_eq!(spec.api_key, Some(77));
    assert_eq!(spec.message_type, MessageType::Request);
    assert_eq!(spec.valid_versions, VersionRange::Range(0, 3));
    assert_eq!(spec.flexible_versions, VersionRange::From(2));
    assert_eq!(spec.listeners, ["broker", "controller"]);
    assert!(spec.latest_version_unstable);
    assert_eq!(spec.common_structs.len(), 1);

    let tagged = spec
        .fields
        .iter()
        .find(|field| field.name == "TaggedNote")
        .unwrap();
    assert_eq!(tagged.tag, Some(0));
    assert_eq!(tagged.tagged_versions, VersionRange::From(2));
    assert!(!tagged.has_flexible_versions_override);

    let items = spec
        .fields
        .iter()
        .find(|field| field.name == "Items")
        .unwrap();
    assert!(matches!(items.field_type, FieldType::Array(_)));
    assert_eq!(items.fields.len(), 2);
}

#[test]
fn parse_all_specs_sorts_by_message_name_and_reports_schema_errors() {
    let dir = scratch_dir("parse-all");
    let _zoo_path = write_file(
        &dir,
        "ZooRequest.json",
        &protocol_schema("ZooRequest", 81, "request"),
    );
    let _alpha_path = write_file(
        &dir,
        "AlphaResponse.json",
        &protocol_schema("AlphaResponse", 80, "response"),
    );
    let _ignored_path = write_file(&dir, "ignored.txt", "{}");

    let specs = parser::parse_all_specs(&dir).unwrap();
    let names: Vec<&str> = specs.iter().map(|spec| spec.name.as_str()).collect();
    assert_eq!(names, ["AlphaResponse", "ZooRequest"]);

    let missing = write_file(
        &dir,
        "Missing.json",
        r#"{ "type": "request", "validVersions": "0" }"#,
    );
    assert!(matches!(
        parser::parse_spec(&missing).unwrap_err().kind,
        ParseSchemaErrorKind::MissingField { name } if name == "name"
    ));

    let invalid_type = write_file(
        &dir,
        "InvalidType.json",
        r#"{ "name": "Bad", "type": "admin", "validVersions": "0", "fields": [] }"#,
    );
    assert!(matches!(
        parser::parse_spec(&invalid_type).unwrap_err().kind,
        ParseSchemaErrorKind::InvalidMessageType { value } if value == "admin"
    ));

    let invalid_field = write_file(
        &dir,
        "InvalidField.json",
        r#"{ "name": "Bad", "type": "request", "validVersions": "0", "fields": [
            { "name": "x", "type": "lowercaseStruct", "versions": "0" }
        ] }"#,
    );
    assert!(matches!(
        parser::parse_spec(&invalid_field).unwrap_err().kind,
        ParseSchemaErrorKind::InvalidFieldType(_)
    ));
}

#[test]
fn version_ranges_and_field_types_cover_kafka_schema_shapes() {
    assert_eq!(VersionRange::parse("none").unwrap(), VersionRange::None);
    assert_eq!(VersionRange::parse("3+").unwrap(), VersionRange::From(3));
    assert_eq!(
        VersionRange::parse("2-4").unwrap(),
        VersionRange::Range(2, 4)
    );
    assert_eq!(VersionRange::parse("5").unwrap().to_string(), "5-5");
    assert!(VersionRange::parse("-1").is_err());
    assert!(VersionRange::Range(1, 4).covers(&VersionRange::Range(2, 3)));
    assert_eq!(
        VersionRange::Range(1, 5).intersect(&VersionRange::From(3)),
        VersionRange::Range(3, 5)
    );
    assert_eq!(
        VersionRange::From(1).subtract(&VersionRange::Range(3, 4)),
        vec![VersionRange::Range(1, 2), VersionRange::From(5)]
    );
    assert_eq!(
        VersionRange::Range(1, 5).subtract(&VersionRange::Range(2, 4)),
        vec![VersionRange::Range(1, 1), VersionRange::Range(5, 5)]
    );

    for (raw, expected) in [
        ("bool", FieldType::Bool),
        ("int8", FieldType::Int8),
        ("int16", FieldType::Int16),
        ("int32", FieldType::Int32),
        ("int64", FieldType::Int64),
        ("uint16", FieldType::Uint16),
        ("float64", FieldType::Float64),
        ("string", FieldType::String),
        ("bytes", FieldType::Bytes),
        ("uuid", FieldType::Uuid),
        ("records", FieldType::Records),
        ("Topic", FieldType::Struct("Topic".to_owned())),
    ] {
        assert_eq!(FieldType::parse(raw).unwrap(), expected);
    }

    assert_eq!(
        FieldType::parse("[]int32").unwrap(),
        FieldType::Array(Box::new(FieldType::Int32))
    );
    assert!(FieldType::parse("").is_err());
    assert!(FieldType::parse("lowercaseStruct").is_err());
}

#[test]
fn generate_file_and_mod_rs_cover_flexible_tagged_nested_and_api_key_paths() {
    let dir = scratch_dir("generate");
    let request_path = write_file(
        &dir,
        "ShapeRequest.json",
        &protocol_schema("ShapeRequest", 77, "request"),
    );
    let response_path = write_file(
        &dir,
        "ShapeResponse.json",
        &protocol_schema("ShapeResponse", 77, "response"),
    );
    let request = parser::parse_spec(&request_path).unwrap();
    let response = parser::parse_spec(&response_path).unwrap();

    let generated = codegen::generate_file(&request, &[request.clone(), response.clone()])
        .unwrap()
        .to_string();
    assert!(generated.contains("pub struct ShapeRequestData"));
    assert!(generated.contains("pub struct Item"));
    assert!(generated.contains("pub struct CommonThing"));
    assert!(generated.contains("write_tagged_fields (buf , & all_tags) ?"));
    assert!(generated.contains("read_compact_nullable_bytes"));
    assert!(generated.contains("write_compact_array_length"));
    assert!(generated.contains("pub fn encoded_len (& self , version : i16) -> Result < usize >"));
    assert!(generated.contains("let mut len : usize = 0 ;"));
    assert!(generated.contains("pub fn with_enabled (mut self , value : bool) -> Self"));
    assert!(generated.contains("pub fn with_items (mut self , value : Vec < Item >) -> Self"));
    assert!(generated.contains("UnsupportedFieldVersion :: new (77 , \"topic_id\" , version)"));
    assert!(generated.contains("KafkaUuid :: ZERO"));
    assert!(generated.contains("Option < KafkaString >"));
    assert!(generated.contains("UnsupportedVersion :: new (77 , version)"));

    let all_specs = vec![request.clone(), response.clone()];
    let mod_rs =
        codegen::generate_mod_rs(&[&response, &request], &all_specs, "test-ref").to_string();
    assert!(mod_rs.contains("pub const KAFKA_PROTOCOL_SOURCE_REF : & str = \"test-ref\""));
    assert!(mod_rs.contains("pub enum RequestKind"));
    assert!(mod_rs.contains("Shape (ShapeRequestData)"));
    assert!(mod_rs.contains("pub enum ResponseKind"));
    assert!(mod_rs.contains("Shape (ShapeResponseData)"));
    assert!(mod_rs.contains("pub fn write (& self , buf : & mut BytesMut , version : i16)"));
    assert!(mod_rs.contains("pub mod shape_request"));
    assert!(mod_rs.contains("pub mod shape_response"));
    assert!(mod_rs.contains("Shape = 77"));
    assert!(mod_rs.contains("77 => Some (ApiKey :: Shape)"));

    let test_utils = codegen::generate_test_utils_file(&request)
        .unwrap()
        .to_string();
    assert!(test_utils.contains("impl TestInstance for ShapeRequestData"));
    assert!(test_utils.contains("fn test_populated"));
    assert!(test_utils.contains("fn test_null_optionals"));
    assert!(test_utils.contains("fn test_empty_collections"));
    assert!(test_utils.contains("fn test_multi_element_collections"));
    assert!(test_utils.contains("fn test_numeric_boundaries"));
    assert!(test_utils.contains("fn test_tagged_fields"));
    assert!(test_utils.contains("RawTaggedField"));
    assert!(test_utils.contains("KafkaUuid :: ONE"));
    assert!(test_utils.contains("i8 :: MIN"));
    assert!(test_utils.contains("vec ! ["));
}

#[test]
fn generated_file_handles_data_messages_without_api_key_version_guard() {
    let spec = kacrab_codegen::ir::message::MessageSpec {
        name: "MetadataValue".to_owned(),
        api_key: None,
        message_type: MessageType::Data,
        valid_versions: VersionRange::Range(0, 0),
        flexible_versions: VersionRange::None,
        fields: vec![test_field("Value", FieldType::Int32)],
        common_structs: Vec::new(),
        listeners: Vec::new(),
        latest_version_unstable: false,
    };

    let generated = codegen::generate_file(&spec, &[]).unwrap().to_string();

    assert!(generated.contains("pub struct MetadataValueData"));
    assert!(!generated.contains("UnsupportedVersion :: new"));
}

#[test]
fn errors_java_scrape_and_lower_cover_retriability_display_and_low_count_errors() {
    let dir = scratch_dir("errors-java");
    let mut body = String::from("public enum Errors {\n");
    body.push_str("    NONE(0, null, message -> null),\n");
    body.push_str(
        "    NOT_LEADER_OR_FOLLOWER(6, \"Not leader\" +\n        \" or follower\", \
         NotLeaderOrFollowerException::new),\n",
    );
    for idx in 1..=130 {
        writeln!(
            body,
            "    GENERATED_ERROR_{idx}({idx}, \"generated {idx}\", UnknownServerException::new),"
        )
        .unwrap();
    }
    body.push_str("}\n");
    let path = write_file(&dir, "Errors.java", &body);

    let entries = errors_java::scrape(&path).unwrap();
    assert!(entries.len() >= 132);
    assert_eq!(entries[0].variant_name, "NONE");
    assert_eq!(entries[0].message, None);
    assert_eq!(
        entries[1].message.as_deref(),
        Some("Not leader or follower")
    );

    let lowered = errors_java::lower(&entries).to_string();
    assert!(lowered.contains("pub enum ErrorCode"));
    assert!(lowered.contains("NotLeaderOrFollower"));
    assert!(lowered.contains("ErrorCode :: NotLeaderOrFollower => true"));
    assert!(lowered.contains("Unknown error code"));

    let small = write_file(
        &dir,
        "SmallErrors.java",
        "public enum Errors {\n    NONE(0, null, message -> null),\n}\n",
    );
    assert!(matches!(
        errors_java::scrape(&small).unwrap_err().kind,
        ErrorsJavaErrorKind::EntryCountTooLow { found: 1, min: 130 }
    ));

    let missing = write_file(&dir, "NoEnum.java", "final class Errors {}\n");
    assert!(matches!(
        errors_java::scrape(&missing).unwrap_err().kind,
        ErrorsJavaErrorKind::MissingEnumBlock
    ));
}

fn test_field(name: &str, field_type: FieldType) -> FieldSpec {
    FieldSpec {
        name: name.to_owned(),
        field_type,
        versions: VersionRange::Range(0, 0),
        nullable_versions: VersionRange::None,
        tagged_versions: VersionRange::None,
        tag: None,
        about: String::new(),
        default: None,
        ignorable: false,
        map_key: false,
        entity_type: None,
        zero_copy: false,
        flexible_versions: VersionRange::None,
        has_flexible_versions_override: false,
        fields: Vec::new(),
    }
}
