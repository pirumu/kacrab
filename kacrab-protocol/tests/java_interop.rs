//! Cross-language protocol checks against Apache Kafka's Java message classes.
//!
//! These tests are ignored by default because they shell out to Maven/Javac and
//! use the pinned `org.apache.kafka:kafka-clients` artifact as an external oracle.

use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    error::Error,
    fmt::Write as _,
    fs, io,
    path::{Path, PathBuf},
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};

#[path = "support/generated_test_utils.rs"]
mod generated_test_utils;

use bytes::{Bytes, BytesMut};
use kacrab_protocol::{
    KafkaString, KafkaUuid, RawTaggedField,
    generated::{
        ApiVersion, ApiVersionsRequestData, ApiVersionsResponseData, FinalizedFeatureKey,
        MetadataRequestData, MetadataRequestTopic, SupportedFeatureKey,
    },
};

const KAFKA_VERSION: &str = "4.3.0";
const API_VERSIONS_REQUEST_VERSION: i16 = 3;
const API_VERSIONS_RESPONSE_VERSION: i16 = 4;
const METADATA_REQUEST_VERSION: i16 = 12;
const REQUIRED_MATRIX_FIXTURES: &[&str] = &[
    "null_optionals",
    "populated",
    "empty_collections",
    "multi_element_collections",
    "numeric_boundaries",
    "tagged_fields",
];

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
pub(crate) type MatrixResult<T = String> = Result<T, Box<dyn Error>>;

pub(crate) trait TestInstance {
    fn test_populated(version: i16) -> Self;
    fn test_null_optionals(version: i16) -> Self;
    fn test_empty_collections(version: i16) -> Self;
    fn test_multi_element_collections(version: i16) -> Self;
    fn test_numeric_boundaries(version: i16) -> Self;
    fn test_tagged_fields(version: i16) -> Self;
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct MatrixCase {
    pub(crate) schema_name: &'static str,
    pub(crate) java_class: &'static str,
    pub(crate) version: i16,
    pub(crate) fixture: &'static str,
    pub(crate) rust_encode: fn(i16) -> MatrixResult<String>,
    pub(crate) rust_encoded_len: fn(i16) -> MatrixResult<usize>,
    pub(crate) rust_reencode: fn(i16, &str) -> MatrixResult<String>,
}

#[test]
fn generated_protocol_matrix_has_release_grade_fixtures_for_every_schema_version() {
    let cases = generated_test_utils::protocol_cases();
    assert!(
        !cases.is_empty(),
        "generated Java oracle matrix should contain protocol cases"
    );

    let mut by_schema_version = BTreeMap::new();
    for case in cases {
        let _inserted = by_schema_version
            .entry((case.schema_name, case.java_class, case.version))
            .or_insert_with(BTreeSet::new)
            .insert(case.fixture);
    }

    let mut missing = Vec::new();
    for ((schema_name, java_class, version), fixtures) in by_schema_version {
        for required in REQUIRED_MATRIX_FIXTURES {
            if !fixtures.contains(required) {
                missing.push(format!(
                    "{schema_name} {java_class} v{version} missing {required}"
                ));
            }
        }
    }

    assert!(missing.is_empty(), "{}", missing.join("\n"));
}

#[test]
fn generated_encoded_len_matches_rust_encoded_bytes_for_all_fixtures() {
    let cases = generated_test_utils::protocol_cases();
    assert!(
        !cases.is_empty(),
        "generated Java oracle matrix should contain protocol cases"
    );

    for case in &cases {
        match (
            (case.rust_encode)(case.version),
            (case.rust_encoded_len)(case.version),
        ) {
            (Ok(rust_hex), Ok(encoded_len)) => {
                assert_eq!(
                    rust_hex.len(),
                    encoded_len.saturating_mul(2),
                    "{} v{} {} encoded_len should match Rust-encoded bytes",
                    case.schema_name,
                    case.version,
                    case.fixture
                );
            },
            (Err(_encode_error), Err(_len_error)) => {},
            (encode_result, len_result) => {
                panic!(
                    "{} v{} {} write and encoded_len should agree on success/failure: \
                     write={encode_result:?}, encoded_len={len_result:?}",
                    case.schema_name, case.version, case.fixture
                );
            },
        }
    }
}

#[test]
#[ignore = "requires Java 17+, Maven, and org.apache.kafka:kafka-clients:4.3.0"]
fn java_client_preserves_all_rust_generated_protocol_fixtures() -> TestResult {
    let java = JavaHarness::compile()?;
    let cases = generated_test_utils::protocol_cases();
    assert!(
        !cases.is_empty(),
        "generated Java oracle matrix should contain protocol cases"
    );

    for case in &cases {
        let rust_hex = (case.rust_encode)(case.version)?;
        let java_hex = java.run_ok(&[
            "roundtrip-hex",
            case.java_class,
            &case.version.to_string(),
            &rust_hex,
        ])?;
        assert_eq!(
            rust_hex, java_hex,
            "{} v{} {} should round-trip byte-for-byte through Java",
            case.schema_name, case.version, case.fixture
        );
    }
    Ok(())
}

#[test]
#[ignore = "requires Java 17+, Maven, and org.apache.kafka:kafka-clients:4.3.0"]
fn rust_preserves_all_java_default_protocol_messages() -> TestResult {
    let java = JavaHarness::compile()?;
    let all_cases = generated_test_utils::protocol_cases();
    assert!(
        !all_cases.is_empty(),
        "generated Java oracle matrix should contain protocol cases"
    );

    let mut seen = BTreeSet::new();
    for case in all_cases {
        if !seen.insert((case.schema_name, case.java_class, case.version)) {
            continue;
        }
        let java_hex =
            java.run_ok(&["encode-default", case.java_class, &case.version.to_string()])?;
        let rust_hex = (case.rust_reencode)(case.version, &java_hex)?;
        assert_eq!(
            java_hex, rust_hex,
            "{} v{} Java default should round-trip byte-for-byte through Rust",
            case.schema_name, case.version
        );
    }
    Ok(())
}

#[test]
#[ignore = "requires Java 17+, Maven, and org.apache.kafka:kafka-clients:4.3.0"]
fn java_client_decodes_rust_and_rust_decodes_java_api_versions_request_v3() -> TestResult {
    let java = JavaHarness::compile()?;
    let original = api_versions_request_fixture();

    let rust_hex = encode_api_versions_request(&original, API_VERSIONS_REQUEST_VERSION)?;
    drop(java.run_ok(&["decode-api-versions-request-v3", &rust_hex])?);

    let java_hex = java.run_ok(&["encode-api-versions-request-v3"])?;
    assert_eq!(
        rust_hex, java_hex,
        "Rust and Java should encode identical bytes"
    );

    let decoded = decode_api_versions_request(&java_hex, API_VERSIONS_REQUEST_VERSION)?;
    assert_eq!(decoded, original);
    Ok(())
}

#[test]
#[ignore = "requires Java 17+, Maven, and org.apache.kafka:kafka-clients:4.3.0"]
fn java_client_decodes_rust_and_rust_decodes_java_api_versions_response_v4() -> TestResult {
    let java = JavaHarness::compile()?;
    let original = api_versions_response_fixture();

    let rust_hex = encode_api_versions_response(&original, API_VERSIONS_RESPONSE_VERSION)?;
    drop(java.run_ok(&["decode-api-versions-response-v4", &rust_hex])?);

    let java_hex = java.run_ok(&["encode-api-versions-response-v4"])?;
    assert_eq!(
        rust_hex, java_hex,
        "Rust and Java should encode identical bytes"
    );

    let decoded = decode_api_versions_response(&java_hex, API_VERSIONS_RESPONSE_VERSION)?;
    assert_eq!(decoded, original);
    Ok(())
}

#[test]
#[ignore = "requires Java 17+, Maven, and org.apache.kafka:kafka-clients:4.3.0"]
fn java_client_decodes_rust_and_rust_decodes_java_metadata_request_v12() -> TestResult {
    let java = JavaHarness::compile()?;
    let original = metadata_request_fixture();

    let rust_hex = encode_metadata_request(&original, METADATA_REQUEST_VERSION)?;
    drop(java.run_ok(&["decode-metadata-request-v12", &rust_hex])?);

    let java_hex = java.run_ok(&["encode-metadata-request-v12"])?;
    assert_eq!(
        rust_hex, java_hex,
        "Rust and Java should encode identical bytes"
    );

    let decoded = decode_metadata_request(&java_hex, METADATA_REQUEST_VERSION)?;
    assert_eq!(decoded, original);
    Ok(())
}

struct JavaHarness {
    classpath: String,
}

impl JavaHarness {
    fn compile() -> TestResult<Self> {
        let repo = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let jar = ensure_kafka_clients_jar()?;
        let unique_id = SystemTime::now()
            .duration_since(UNIX_EPOCH)?
            .as_nanos()
            .to_string();
        let classes = repo
            .parent()
            .ok_or_else(|| io::Error::other("crate should live inside workspace"))?
            .join("target/java-interop/classes")
            .join(format!("{}-{unique_id}", std::process::id()));
        fs::create_dir_all(&classes)?;

        let source = repo.join("tests/java/KafkaProtocolInterop.java");
        let status = Command::new("javac")
            .arg("-cp")
            .arg(&jar)
            .arg("-d")
            .arg(&classes)
            .arg(&source)
            .status()?;
        if !status.success() {
            return Err(
                io::Error::other(format!("javac should compile {}", source.display())).into(),
            );
        }

        let classpath = format!(
            "{}{}{}",
            classes.display(),
            java_path_separator(),
            jar.display()
        );
        Ok(Self { classpath })
    }

    fn run_ok(&self, args: &[&str]) -> TestResult<String> {
        let output = Command::new("java")
            .arg("-cp")
            .arg(&self.classpath)
            .arg("KafkaProtocolInterop")
            .args(args)
            .output()?;
        if !output.status.success() {
            return Err(io::Error::other(format!(
                "KafkaProtocolInterop failed\nstdout:\n{}\nstderr:\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            ))
            .into());
        }
        Ok(String::from_utf8(output.stdout)?.trim().to_owned())
    }
}

fn ensure_kafka_clients_jar() -> TestResult<PathBuf> {
    let home = env::var_os("HOME")
        .ok_or_else(|| io::Error::other("HOME should be set for local Maven repository"))?;
    let jar = Path::new(&home).join(format!(
        ".m2/repository/org/apache/kafka/kafka-clients/{KAFKA_VERSION}/\
         kafka-clients-{KAFKA_VERSION}.jar"
    ));
    if jar.exists() {
        return Ok(jar);
    }

    let artifact = format!("org.apache.kafka:kafka-clients:{KAFKA_VERSION}");
    let status = Command::new("mvn")
        .arg("-q")
        .arg("dependency:get")
        .arg(format!("-Dartifact={artifact}"))
        .arg("-Dtransitive=false")
        .status()?;
    if !status.success() {
        return Err(io::Error::other(format!("Maven should fetch {artifact}")).into());
    }
    if !jar.exists() {
        return Err(io::Error::other(format!(
            "Maven should place kafka-clients jar at {}",
            jar.display()
        ))
        .into());
    }
    Ok(jar)
}

const fn java_path_separator() -> &'static str {
    if cfg!(windows) { ";" } else { ":" }
}

fn api_versions_request_fixture() -> ApiVersionsRequestData {
    ApiVersionsRequestData {
        client_software_name: KafkaString::from("kacrab".to_owned()),
        client_software_version: KafkaString::from("0.0.1".to_owned()),
        _unknown_tagged_fields: vec![RawTaggedField {
            tag: 9,
            data: Bytes::from_static(b"client-tag"),
        }],
    }
}

fn metadata_request_fixture() -> MetadataRequestData {
    MetadataRequestData {
        topics: Some(vec![
            MetadataRequestTopic {
                topic_id: KafkaUuid::from_parts(0x0102_0304_0506_0708, 0x1112_1314_1516_1718),
                name: Some(KafkaString::from("topic-a".to_owned())),
                _unknown_tagged_fields: vec![RawTaggedField {
                    tag: 2,
                    data: Bytes::from_static(b"topic-tag"),
                }],
            },
            MetadataRequestTopic {
                topic_id: KafkaUuid::from_parts(0x2122_2324_2526_2728, 0x3132_3334_3536_3738),
                name: None,
                _unknown_tagged_fields: Vec::new(),
            },
        ]),
        allow_auto_topic_creation: true,
        include_cluster_authorized_operations: false,
        include_topic_authorized_operations: true,
        _unknown_tagged_fields: vec![RawTaggedField {
            tag: 4,
            data: Bytes::from_static(b"metadata-tag"),
        }],
    }
}

fn api_versions_response_fixture() -> ApiVersionsResponseData {
    ApiVersionsResponseData {
        error_code: 0,
        api_keys: vec![
            ApiVersion {
                api_key: 18,
                min_version: 0,
                max_version: 4,
                _unknown_tagged_fields: vec![RawTaggedField {
                    tag: 1,
                    data: Bytes::from_static(b"api-tag"),
                }],
            },
            ApiVersion {
                api_key: 3,
                min_version: 0,
                max_version: 13,
                _unknown_tagged_fields: Vec::new(),
            },
        ],
        throttle_time_ms: 12,
        supported_features: vec![SupportedFeatureKey {
            name: KafkaString::from("metadata.version".to_owned()),
            min_version: 1,
            max_version: 23,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 2,
                data: Bytes::from_static(b"supported-tag"),
            }],
        }],
        finalized_features_epoch: 42,
        finalized_features: vec![FinalizedFeatureKey {
            name: KafkaString::from("metadata.version".to_owned()),
            max_version_level: 23,
            min_version_level: 1,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 3,
                data: Bytes::from_static(b"finalized-tag"),
            }],
        }],
        zk_migration_ready: true,
        _unknown_tagged_fields: vec![RawTaggedField {
            tag: 9,
            data: Bytes::from_static(b"response-tag"),
        }],
    }
}

fn encode_api_versions_request(
    message: &ApiVersionsRequestData,
    version: i16,
) -> TestResult<String> {
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(hex(out.as_ref())?)
}

fn decode_api_versions_request(
    hex_input: &str,
    version: i16,
) -> TestResult<ApiVersionsRequestData> {
    let mut input = Bytes::from(decode_hex(hex_input)?);
    let decoded = ApiVersionsRequestData::read(&mut input, version)?;
    assert!(input.is_empty(), "Rust decoder should consume Java bytes");
    Ok(decoded)
}

fn encode_api_versions_response(
    message: &ApiVersionsResponseData,
    version: i16,
) -> TestResult<String> {
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(hex(out.as_ref())?)
}

fn decode_api_versions_response(
    hex_input: &str,
    version: i16,
) -> TestResult<ApiVersionsResponseData> {
    let mut input = Bytes::from(decode_hex(hex_input)?);
    let decoded = ApiVersionsResponseData::read(&mut input, version)?;
    assert!(input.is_empty(), "Rust decoder should consume Java bytes");
    Ok(decoded)
}

fn encode_metadata_request(message: &MetadataRequestData, version: i16) -> TestResult<String> {
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(hex(out.as_ref())?)
}

fn decode_metadata_request(hex_input: &str, version: i16) -> TestResult<MetadataRequestData> {
    let mut input = Bytes::from(decode_hex(hex_input)?);
    let decoded = MetadataRequestData::read(&mut input, version)?;
    assert!(input.is_empty(), "Rust decoder should consume Java bytes");
    Ok(decoded)
}

fn hex(bytes: &[u8]) -> Result<String, std::fmt::Error> {
    let mut out = String::new();
    for byte in bytes {
        write!(&mut out, "{byte:02x}")?;
    }
    Ok(out)
}

fn decode_hex(input: &str) -> TestResult<Vec<u8>> {
    if !input.len().is_multiple_of(2) {
        return Err(io::Error::other("hex input must have even length").into());
    }
    let mut out = Vec::with_capacity(input.len() / 2);
    for pair in input.as_bytes().chunks_exact(2) {
        let [high, low] = <[u8; 2]>::try_from(pair)
            .map_err(|_error| io::Error::other("hex chunk should contain two bytes"))?;
        let high = hex_nibble(high)?;
        let low = hex_nibble(low)?;
        out.push((high << 4) | low);
    }
    Ok(out)
}

fn ensure_input_consumed(input: &Bytes) -> TestResult {
    if input.is_empty() {
        Ok(())
    } else {
        Err(io::Error::other(format!("Rust decoder left {} byte(s)", input.len())).into())
    }
}

fn hex_nibble(byte: u8) -> TestResult<u8> {
    match byte {
        b'0' => Ok(0),
        b'1' => Ok(1),
        b'2' => Ok(2),
        b'3' => Ok(3),
        b'4' => Ok(4),
        b'5' => Ok(5),
        b'6' => Ok(6),
        b'7' => Ok(7),
        b'8' => Ok(8),
        b'9' => Ok(9),
        b'a' | b'A' => Ok(10),
        b'b' | b'B' => Ok(11),
        b'c' | b'C' => Ok(12),
        b'd' | b'D' => Ok(13),
        b'e' | b'E' => Ok(14),
        b'f' | b'F' => Ok(15),
        _ => Err(io::Error::other(format!("invalid hex byte: {byte}")).into()),
    }
}
