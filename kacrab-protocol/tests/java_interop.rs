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
    compression::Compression,
    generated::{
        ApiKey, ApiVersion, ApiVersionsRequestData, ApiVersionsResponseData, FinalizedFeatureKey,
        MetadataRequestData, MetadataRequestTopic, SupportedFeatureKey,
    },
    record::{Record, RecordBatch, RecordHeader},
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

// ---------------------------------------------------------------------------
// Seed corpora for the `cargo-fuzz` targets in `fuzz/`
// ---------------------------------------------------------------------------
//
// Fuzzing a Kafka decoder from zero bytes is close to hopeless: the wire format
// is length-prefixed and version-gated, so random input dies in the first few
// fields and libFuzzer never learns the shape of a valid message. A seed corpus
// is what turns a fuzz target from "explores framing" into "explores the
// decoder".
//
// This lives in the Java-oracle file on purpose. The matrix already builds a
// known-good encoding of *every* generated message at *every* schema version
// across six fixture shapes (populated, null optionals, empty and multi-element
// collections, numeric boundaries, tagged fields), and `protocol_cases()` hands
// them over as hex. Re-deriving that anywhere else would mean duplicating the
// fixture modules and the hex helpers; reusing it means the corpus grows
// automatically whenever a new schema version is generated.

/// Where a fuzz target's committed seed corpus lives, created if absent.
///
/// Deliberately `fuzz/seeds/`, not `fuzz/corpus/`: libFuzzer treats the first
/// corpus directory on its command line as writable and grows it by thousands
/// of files during a run, so `fuzz/corpus/` is gitignored scratch space. Seeds
/// are an input to fuzzing and are committed; a run is given both directories.
fn fuzz_seed_dir(target: &str) -> TestResult<PathBuf> {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .ok_or_else(|| io::Error::other("workspace root should exist"))?
        .join("fuzz")
        .join("seeds")
        .join(target);
    fs::create_dir_all(&dir)?;
    Ok(dir)
}

fn write_seed(target: &str, name: &str, bytes: &[u8]) -> TestResult {
    fs::write(fuzz_seed_dir(target)?.join(name), bytes)?;
    Ok(())
}

/// `ApiKey` by the schema name the matrix uses, e.g. `"FetchResponse"`.
///
/// The generated `Debug` name of an `ApiKey` is exactly the schema name minus
/// the `Request`/`Response` suffix, so the table is derived rather than typed
/// out — a new API key joins it for free.
fn response_schema_to_api_key() -> BTreeMap<String, u8> {
    let mut by_name = BTreeMap::new();
    for raw in 0_i16..=127 {
        let Some(api_key) = ApiKey::from_i16(raw) else {
            continue;
        };
        let Ok(byte) = u8::try_from(raw) else {
            continue;
        };
        let _replaced = by_name.insert(format!("{api_key:?}Response"), byte);
    }
    by_name
}

/// Seeds for `response_decode`, laid out as `[api_key, version, body..]`.
fn write_response_seeds() -> TestResult<usize> {
    let by_name = response_schema_to_api_key();
    let mut written = 0_usize;
    for case in generated_test_utils::protocol_cases() {
        let Some(api_key) = by_name.get(case.schema_name) else {
            continue;
        };
        let Ok(version_byte) = u8::try_from(case.version) else {
            continue;
        };
        // A fixture that cannot encode at this version is not a corpus entry;
        // the matrix test is what reports that as a failure, not this.
        let Ok(encoded) = (case.rust_encode)(case.version) else {
            continue;
        };
        let body = decode_hex(&encoded)?;

        let mut seed = Vec::with_capacity(body.len().saturating_add(2));
        seed.push(*api_key);
        seed.push(version_byte);
        seed.extend_from_slice(&body);
        write_seed(
            "response_decode",
            &format!(
                "{}_v{}_{}",
                case.schema_name.to_lowercase(),
                case.version,
                case.fixture
            ),
            &seed,
        )?;
        written = written.saturating_add(1);
    }
    Ok(written)
}

/// The record shapes worth seeding: empty, minimal, null key/value, headers,
/// and a batch big enough to exercise the record loop.
fn seed_record_shapes() -> Vec<(&'static str, Vec<Record>)> {
    let plain = |offset_delta: i32| Record {
        attributes: 0,
        timestamp_delta: i64::from(offset_delta),
        offset_delta,
        key: Some(Bytes::from_static(b"key")),
        value: Some(Bytes::from_static(b"value")),
        headers: Vec::new(),
    };
    vec![
        ("empty", Vec::new()),
        ("single", vec![plain(0)]),
        (
            "null_kv",
            vec![Record {
                attributes: 0,
                timestamp_delta: 0,
                offset_delta: 0,
                key: None,
                value: None,
                headers: Vec::new(),
            }],
        ),
        (
            "headers",
            vec![Record {
                attributes: 0,
                timestamp_delta: 1,
                offset_delta: 0,
                key: Some(Bytes::from_static(b"k")),
                value: Some(Bytes::from_static(b"v")),
                headers: vec![
                    RecordHeader {
                        key: Bytes::from_static(b"h1"),
                        value: Some(Bytes::from_static(b"v1")),
                    },
                    RecordHeader {
                        key: Bytes::from_static(b"h2"),
                        value: None,
                    },
                ],
            }],
        ),
        ("many", (0..32).map(plain).collect()),
    ]
}

/// Seeds for both record-batch targets: whole batches for `record_batch_decode`,
/// and the CRC-covered region alone for `record_batch_framed`, which builds its
/// own framing and CRC around whatever the fuzzer hands it.
fn write_record_batch_seeds() -> TestResult<(usize, usize)> {
    /// log overhead (12) + partitionLeaderEpoch (4) + magic (1) + crc (4).
    const HEADER_BEFORE_CRC_PAYLOAD: usize = 21;

    let codecs = [
        (Compression::None, "none"),
        (Compression::Gzip, "gzip"),
        (Compression::Snappy, "snappy"),
        (Compression::Lz4, "lz4"),
        (Compression::Zstd, "zstd"),
    ];

    let mut raw = 0_usize;
    let mut framed = 0_usize;
    for (codec, codec_name) in codecs {
        for (shape_name, records) in seed_record_shapes() {
            let last_offset_delta = i32::try_from(records.len().saturating_sub(1)).unwrap_or(0);
            let batch = RecordBatch {
                base_offset: 0,
                partition_leader_epoch: 0,
                magic: 2,
                attributes: codec as i16,
                last_offset_delta,
                first_timestamp: 1_700_000_000_000,
                max_timestamp: 1_700_000_000_999,
                producer_id: 42,
                producer_epoch: 7,
                base_sequence: 0,
                records,
            };
            let mut encoded = BytesMut::new();
            if batch.encode(&mut encoded).is_err() {
                continue;
            }
            let encoded = encoded.freeze();
            let name = format!("{codec_name}_{shape_name}");
            write_seed("record_batch_decode", &name, &encoded)?;
            raw = raw.saturating_add(1);

            if let Some(crc_payload) = encoded.get(HEADER_BEFORE_CRC_PAYLOAD..) {
                write_seed("record_batch_framed", &name, crc_payload)?;
                framed = framed.saturating_add(1);
            }
        }
    }
    Ok((raw, framed))
}

/// Seeds for `decompress`, laid out as `[codec_selector, compressed_payload..]`.
fn write_decompress_seeds() -> TestResult<usize> {
    let payloads: [(&str, Vec<u8>); 4] = [
        ("empty", Vec::new()),
        ("small", b"hello kafka".to_vec()),
        ("repetitive", vec![b'a'; 4096]),
        ("mixed", (0_u32..2048).map(|i| (i % 251) as u8).collect()),
    ];
    let codecs = [
        (Compression::Gzip, 1_u8, "gzip"),
        (Compression::Snappy, 2_u8, "snappy"),
        (Compression::Lz4, 3_u8, "lz4"),
        (Compression::Zstd, 4_u8, "zstd"),
    ];

    let mut written = 0_usize;
    for (codec, selector, codec_name) in codecs {
        for (payload_name, payload) in &payloads {
            let Ok(compressed) = codec.compress(payload) else {
                continue;
            };
            let mut seed = Vec::with_capacity(compressed.len().saturating_add(1));
            seed.push(selector);
            seed.extend_from_slice(&compressed);
            write_seed("decompress", &format!("{codec_name}_{payload_name}"), &seed)?;
            written = written.saturating_add(1);
        }
    }
    Ok(written)
}

/// Regenerate every fuzz seed corpus.
///
/// Ignored because it writes into the repo. Run it after adding a schema
/// version or a fuzz target:
///
/// ```bash
/// cargo test -p kacrab-protocol --test java_interop -- --ignored --nocapture \
///   generate_fuzz_corpus
/// ```
///
/// The seeds are committed so CI and a fresh clone start from the same
/// population; libFuzzer's own runtime growth in `fuzz/corpus/` is gitignored.
#[test]
#[ignore = "writes seed corpora into fuzz/corpus/; run explicitly"]
fn generate_fuzz_corpus() -> TestResult {
    let responses = write_response_seeds()?;
    let (raw, framed) = write_record_batch_seeds()?;
    let decompress = write_decompress_seeds()?;

    println!("response_decode:     {responses} seeds");
    println!("record_batch_decode: {raw} seeds");
    println!("record_batch_framed: {framed} seeds");
    println!("decompress:          {decompress} seeds");

    assert!(responses > 0, "response corpus should not be empty");
    assert!(raw > 0, "record-batch corpus should not be empty");
    assert!(framed > 0, "framed record-batch corpus should not be empty");
    assert!(decompress > 0, "decompress corpus should not be empty");
    Ok(())
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
