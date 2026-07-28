//! Offline version negotiation over golden `ApiVersions` responses captured
//! from real brokers.
//!
//! Every fixture in `tests/fixtures/api_versions/` is a byte-for-byte copy of
//! the `ApiVersions` v3 response frame a real Kafka broker sent to kacrab's own
//! wire codec — none of it is hand-written. The tests below replay those bytes
//! through the production decode path (`frame::decode_response_frame` →
//! `frame::decode_response_envelope` → `ApiVersionsResponseData::read`) and into
//! [`BrokerCapabilities`], so negotiation is pinned against real brokers with no
//! container in the loop and no measurable runtime.
//!
//! `capture_api_versions_fixture` is the ignored harness that produced them; see
//! `tests/fixtures/api_versions/README.md` for provenance and for how to add a
//! new broker release.

#![allow(
    clippy::expect_used,
    clippy::missing_assert_message,
    clippy::unwrap_used,
    reason = "Integration test fixtures fail fastest with contextual unwrap/expect calls."
)]

use bytes::{Bytes, BytesMut};
use kacrab::wire::{BrokerCapabilities, WireError};
use kacrab_protocol::{
    frame,
    generated::{ApiKey, ApiVersionsResponseData},
    version::{ApiVersionRange, client_api_info},
};

/// `ApiVersions` version used for both capture and replay. The handshake in
/// `wire/broker.rs` pins v3, so the fixtures are decoded at v3 too.
const HANDSHAKE_VERSION: i16 = 3;

/// A captured broker response plus the negotiation outcome it must produce.
struct Fixture {
    /// Broker release the bytes came from, matching the fixture file stem.
    label: &'static str,
    /// Raw `ApiVersions` v3 response frame, exactly as the broker sent it.
    frame: &'static [u8],
    /// Committed JSON decode summary, regenerated from `frame` by the tests.
    summary: &'static str,
    /// Expected negotiated version per API. `None` means the broker does not
    /// offer a mutually supported version — the client must report that, not
    /// panic.
    negotiated: &'static [(ApiKey, Option<i16>)],
}

/// Representative APIs asserted for every fixture, in a fixed order so the
/// per-broker expectation rows line up column by column.
///
/// `ConsumerGroupHeartbeat` (KIP-848) and `ShareFetch` (KIP-932) are the
/// forward-looking pair: they are the APIs that a pre-4.x broker cannot serve,
/// so their `None` rows are what proves an old broker degrades cleanly.
const ASSERTED_APIS: [ApiKey; 7] = [
    ApiKey::Produce,
    ApiKey::Fetch,
    ApiKey::FindCoordinator,
    ApiKey::Metadata,
    ApiKey::OffsetCommit,
    ApiKey::ConsumerGroupHeartbeat,
    ApiKey::ShareFetch,
];

/// Every committed fixture, in broker release order.
///
/// The expected versions are `min(client max, broker max)` once the ranges
/// overlap; they were read off the captured bytes, not guessed. Two of them are
/// worth calling out:
///
/// - Kafka 3.9 *does* advertise `ConsumerGroupHeartbeat`, but only at v0 (KIP-848 early access), so
///   negotiation lands on 0 rather than the client's v1.
/// - Kafka 4.0 does **not** advertise `ShareFetch` at all: KIP-932 stayed behind an unstable
///   feature flag until 4.2, so a share consumer against 4.0 must be told the API is missing.
const FIXTURES: &[Fixture] = &[
    Fixture {
        label: "3.6.2",
        frame: include_bytes!("fixtures/api_versions/kafka-3.6.2.bin"),
        summary: include_str!("fixtures/api_versions/kafka-3.6.2.json"),
        negotiated: &[
            (ApiKey::Produce, Some(9)),
            (ApiKey::Fetch, Some(15)),
            (ApiKey::FindCoordinator, Some(4)),
            (ApiKey::Metadata, Some(12)),
            (ApiKey::OffsetCommit, Some(8)),
            (ApiKey::ConsumerGroupHeartbeat, None),
            (ApiKey::ShareFetch, None),
        ],
    },
    Fixture {
        label: "3.9.0",
        frame: include_bytes!("fixtures/api_versions/kafka-3.9.0.bin"),
        summary: include_str!("fixtures/api_versions/kafka-3.9.0.json"),
        negotiated: &[
            (ApiKey::Produce, Some(11)),
            (ApiKey::Fetch, Some(17)),
            (ApiKey::FindCoordinator, Some(6)),
            (ApiKey::Metadata, Some(12)),
            (ApiKey::OffsetCommit, Some(9)),
            (ApiKey::ConsumerGroupHeartbeat, Some(0)),
            (ApiKey::ShareFetch, None),
        ],
    },
    Fixture {
        label: "4.0.0",
        frame: include_bytes!("fixtures/api_versions/kafka-4.0.0.bin"),
        summary: include_str!("fixtures/api_versions/kafka-4.0.0.json"),
        negotiated: &[
            (ApiKey::Produce, Some(12)),
            (ApiKey::Fetch, Some(17)),
            (ApiKey::FindCoordinator, Some(6)),
            (ApiKey::Metadata, Some(13)),
            (ApiKey::OffsetCommit, Some(9)),
            (ApiKey::ConsumerGroupHeartbeat, Some(1)),
            (ApiKey::ShareFetch, None),
        ],
    },
    Fixture {
        label: "4.3.0",
        frame: include_bytes!("fixtures/api_versions/kafka-4.3.0.bin"),
        summary: include_str!("fixtures/api_versions/kafka-4.3.0.json"),
        negotiated: &[
            (ApiKey::Produce, Some(13)),
            (ApiKey::Fetch, Some(18)),
            (ApiKey::FindCoordinator, Some(6)),
            (ApiKey::Metadata, Some(13)),
            (ApiKey::OffsetCommit, Some(10)),
            (ApiKey::ConsumerGroupHeartbeat, Some(1)),
            (ApiKey::ShareFetch, Some(2)),
        ],
    },
];

/// Decode a captured frame through the same path the broker task uses.
fn decode_fixture(bytes: &[u8]) -> ApiVersionsResponseData {
    let mut buf = Bytes::copy_from_slice(bytes);
    let payload = frame::decode_response_frame(&mut buf).expect("response frame");
    assert!(
        buf.is_empty(),
        "fixture must hold exactly one response frame"
    );
    let mut envelope =
        frame::decode_response_envelope(ApiKey::ApiVersions, HANDSHAKE_VERSION, payload)
            .expect("response envelope");
    let response = ApiVersionsResponseData::read(&mut envelope.body, HANDSHAKE_VERSION)
        .expect("api versions body");
    assert!(
        envelope.body.is_empty(),
        "decode left trailing bytes in the response body"
    );
    response
}

/// Decode a fixture straight into the capabilities under test.
fn capabilities_of(fixture: &Fixture) -> BrokerCapabilities {
    BrokerCapabilities::from_response(&decode_fixture(fixture.frame))
}

/// Render the JSON decode summary committed next to each fixture.
///
/// The offline tests regenerate this from the bytes and compare it to the
/// committed file, so the summary can never drift from what the broker sent.
fn summary_json(label: &str, response: &ApiVersionsResponseData) -> String {
    use std::fmt::Write as _;

    let mut out = String::new();
    out.push_str("{\n");
    writeln!(out, "  \"broker\": \"{label}\",").expect("string write");
    writeln!(
        out,
        "  \"api_versions_request_version\": {HANDSHAKE_VERSION},"
    )
    .expect("string write");
    writeln!(out, "  \"error_code\": {},", response.error_code).expect("string write");
    writeln!(
        out,
        "  \"throttle_time_ms\": {},",
        response.throttle_time_ms
    )
    .expect("string write");
    writeln!(
        out,
        "  \"finalized_features_epoch\": {},",
        response.finalized_features_epoch
    )
    .expect("string write");
    writeln!(out, "  \"api_key_count\": {},", response.api_keys.len()).expect("string write");
    out.push_str("  \"supported_features\": [\n");
    let mut first = true;
    for feature in &response.supported_features {
        if !first {
            out.push_str(",\n");
        }
        first = false;
        write!(
            out,
            "    {{ \"name\": \"{}\", \"min_version\": {}, \"max_version\": {} }}",
            feature.name.as_str(),
            feature.min_version,
            feature.max_version
        )
        .expect("string write");
    }
    out.push_str("\n  ],\n  \"finalized_features\": [\n");
    first = true;
    for feature in &response.finalized_features {
        if !first {
            out.push_str(",\n");
        }
        first = false;
        write!(
            out,
            "    {{ \"name\": \"{}\", \"min_version_level\": {}, \"max_version_level\": {} }}",
            feature.name.as_str(),
            feature.min_version_level,
            feature.max_version_level
        )
        .expect("string write");
    }
    out.push_str("\n  ],\n  \"api_keys\": [\n");
    first = true;
    for api in &response.api_keys {
        if !first {
            out.push_str(",\n");
        }
        first = false;
        let name = ApiKey::from_i16(api.api_key)
            .map_or_else(|| "Unknown".to_owned(), |key| format!("{key:?}"));
        write!(
            out,
            "    {{ \"api_key\": {}, \"name\": \"{}\", \"min_version\": {}, \"max_version\": {} }}",
            api.api_key, name, api.min_version, api.max_version
        )
        .expect("string write");
    }
    out.push_str("\n  ]\n}\n");
    out
}

#[test]
fn fixtures_decode_and_match_committed_summaries() {
    for fixture in FIXTURES {
        let response = decode_fixture(fixture.frame);

        assert_eq!(
            response.error_code, 0,
            "{}: broker returned an error code",
            fixture.label
        );
        assert!(
            !response.api_keys.is_empty(),
            "{}: broker advertised no APIs",
            fixture.label
        );
        assert_eq!(
            summary_json(fixture.label, &response),
            fixture.summary,
            "{}: committed JSON summary does not match the captured bytes",
            fixture.label
        );
    }
}

#[test]
fn fixtures_round_trip_through_the_codec() {
    for fixture in FIXTURES {
        let response = decode_fixture(fixture.frame);

        let len = response
            .encoded_len(HANDSHAKE_VERSION)
            .expect("encoded length");
        let mut buf = BytesMut::with_capacity(len);
        response
            .write(&mut buf, HANDSHAKE_VERSION)
            .expect("re-encode");
        assert_eq!(
            buf.len(),
            len,
            "{}: encoded_len disagrees with write",
            fixture.label
        );

        let mut reread = buf.freeze();
        let round_tripped = ApiVersionsResponseData::read(&mut reread, HANDSHAKE_VERSION)
            .expect("re-decode round-tripped body");
        assert!(
            reread.is_empty(),
            "{}: re-decode left trailing bytes",
            fixture.label
        );
        assert_eq!(
            round_tripped, response,
            "{}: round-trip changed the decoded response",
            fixture.label
        );
    }
}

#[test]
fn negotiated_versions_match_each_broker_release() {
    for fixture in FIXTURES {
        let capabilities = capabilities_of(fixture);

        for &(api, expected) in fixture.negotiated {
            assert_eq!(
                capabilities.version_for(api),
                expected,
                "{}: unexpected negotiated version for {api:?}",
                fixture.label
            );
        }
        assert_eq!(
            fixture.negotiated.len(),
            ASSERTED_APIS.len(),
            "{}: expectation row must cover every asserted API",
            fixture.label
        );
    }
}

#[test]
fn negotiated_versions_never_exceed_broker_or_client_maximum() {
    for fixture in FIXTURES {
        let response = decode_fixture(fixture.frame);
        let capabilities = BrokerCapabilities::from_response(&response);

        for advertised in &response.api_keys {
            // Unknown-to-kacrab API keys are skipped rather than resolved: the
            // client has no range for them at all.
            let Some(api) = ApiKey::from_i16(advertised.api_key) else {
                continue;
            };
            let Some(negotiated) = capabilities.version_for(api) else {
                continue;
            };
            let client = client_api_info(api);
            assert!(
                negotiated >= advertised.min_version && negotiated <= advertised.max_version,
                "{}: negotiated {api:?} v{negotiated} outside broker range {}..={}",
                fixture.label,
                advertised.min_version,
                advertised.max_version
            );
            assert!(
                negotiated >= client.min_version && negotiated <= client.max_version,
                "{}: negotiated {api:?} v{negotiated} outside client range {}..={}",
                fixture.label,
                client.min_version,
                client.max_version
            );
        }
    }
}

#[test]
fn version_ceiling_caps_negotiation_without_dropping_below_the_broker_floor() {
    for fixture in FIXTURES {
        let capabilities = capabilities_of(fixture);

        for &(api, expected) in fixture.negotiated {
            let Some(expected) = expected else {
                assert_eq!(
                    capabilities.version_for_limit(api, i16::MAX),
                    None,
                    "{}: {api:?} is unavailable and must stay unavailable under a limit",
                    fixture.label
                );
                continue;
            };
            assert_eq!(
                capabilities.version_for_limit(api, i16::MAX),
                Some(expected),
                "{}: an unbounded limit must not change {api:?}",
                fixture.label
            );
            let client = client_api_info(api);
            if expected > client.min_version {
                let capped = expected.saturating_sub(1);
                assert_eq!(
                    capabilities.version_for_limit(api, capped),
                    Some(capped),
                    "{}: {api:?} did not honour the caller's version ceiling",
                    fixture.label
                );
            }
            assert_eq!(
                capabilities.version_for_limit(api, client.min_version.saturating_sub(1)),
                None,
                "{}: {api:?} must report unavailable below the client floor",
                fixture.label
            );
        }
    }
}

/// The 4.x-only APIs against pre-4.x brokers: they must surface as "no
/// compatible version" through the typed path callers use, never as a panic and
/// never as a silently wrong version.
///
/// The captured bytes are stricter than "4.x only" suggests, so the assertions
/// follow the brokers rather than the slogan: 3.9 already advertises
/// `ConsumerGroupHeartbeat` at v0 (KIP-848 early access), while `ShareFetch`
/// (KIP-932) is absent from 3.6, 3.9 *and* 4.0.
#[test]
fn newer_apis_report_unavailable_on_older_brokers_without_panicking() {
    let unavailable: Vec<(&str, ApiKey)> = FIXTURES
        .iter()
        .flat_map(|fixture| {
            fixture
                .negotiated
                .iter()
                .filter(|(_, expected)| expected.is_none())
                .map(move |&(api, _)| (fixture.label, api))
        })
        .collect();

    assert_eq!(
        unavailable,
        vec![
            ("3.6.2", ApiKey::ConsumerGroupHeartbeat),
            ("3.6.2", ApiKey::ShareFetch),
            ("3.9.0", ApiKey::ShareFetch),
            ("4.0.0", ApiKey::ShareFetch),
        ],
        "the set of unavailable APIs changed; re-check the captured fixtures"
    );

    for fixture in FIXTURES {
        let capabilities = capabilities_of(fixture);

        for &(api, expected) in fixture.negotiated {
            // Mirrors the negotiation chokepoint's error shape: client range
            // from `client_api_info`, broker range from the fixture's own
            // ApiVersions data (None when the broker never advertised the key).
            let resolved = capabilities.version_for(api).ok_or_else(|| {
                let info = client_api_info(api);
                WireError::UnsupportedApiVersion {
                    api_key: api,
                    client: ApiVersionRange {
                        min_version: info.min_version,
                        max_version: info.max_version,
                    },
                    broker: capabilities.broker_range(api),
                }
            });
            if let Some(version) = expected {
                assert_eq!(
                    resolved.expect("api must be available"),
                    version,
                    "{}: unexpected negotiated version for {api:?}",
                    fixture.label
                );
                continue;
            }
            let error = resolved.expect_err("api must be unavailable");
            assert!(
                matches!(
                    &error,
                    WireError::UnsupportedApiVersion { api_key, broker: None, .. } if *api_key == api
                ),
                "{}: {api:?} produced the wrong error: {error}",
                fixture.label
            );
            let message = error.to_string();
            assert!(
                message.starts_with(&format!("no compatible {api:?} API version"))
                    && message.contains("broker does not advertise this API"),
                "{}: unavailable {api:?} must name the API",
                fixture.label
            );
        }
    }
}

#[test]
fn negotiated_versions_never_regress_across_broker_releases() {
    for (index, &api) in ASSERTED_APIS.iter().enumerate() {
        let mut previous: Option<i16> = None;
        for fixture in FIXTURES {
            let (recorded, expected) = fixture
                .negotiated
                .get(index)
                .copied()
                .expect("expectation row covers every asserted API");
            assert_eq!(
                recorded, api,
                "{}: expectation rows must stay in ASSERTED_APIS order",
                fixture.label
            );
            let Some(version) = expected else { continue };
            if let Some(previous) = previous {
                assert!(
                    version >= previous,
                    "{}: negotiated {api:?} regressed from v{previous} to v{version}",
                    fixture.label
                );
            }
            previous = Some(version);
        }
    }
}

/// Capture harness: dump the `ApiVersions` v3 response of a live broker into a
/// fixture pair (`.bin` raw frame + `.json` decode summary).
///
/// Ignored by default because it needs a broker listening on 9092. Boot one
/// broker at a time, then:
///
/// ```text
/// KACRAB_FIXTURE_BROKER=4.3.0 \
///   cargo test -p kacrab --test api_versions_fixtures -- --ignored capture
/// ```
///
/// `KACRAB_FIXTURE_BOOTSTRAP` overrides the default `127.0.0.1:9092` endpoint.
/// Full instructions live in `tests/fixtures/api_versions/README.md`.
#[test]
#[ignore = "requires a live broker on 9092"]
fn capture_api_versions_fixture() {
    use std::{
        io::{Read as _, Write as _},
        net::TcpStream,
        path::Path,
        time::Duration,
    };

    use kacrab_protocol::{
        KafkaString, frame::RequestFrameSpec, generated::ApiVersionsRequestData,
    };

    let label = std::env::var("KACRAB_FIXTURE_BROKER")
        .expect("set KACRAB_FIXTURE_BROKER to the broker release, e.g. 4.3.0");
    let bootstrap =
        std::env::var("KACRAB_FIXTURE_BOOTSTRAP").unwrap_or_else(|_| "127.0.0.1:9092".to_owned());

    let request = ApiVersionsRequestData {
        client_software_name: KafkaString::from("kacrab".to_owned()),
        client_software_version: KafkaString::from(env!("CARGO_PKG_VERSION").to_owned()),
        _unknown_tagged_fields: Vec::new(),
    };
    let encoded = frame::encode_request_frame(
        RequestFrameSpec {
            api_key: ApiKey::ApiVersions,
            api_version: HANDSHAKE_VERSION,
            correlation_id: 0,
            client_id: "kacrab-fixture-capture",
            capacity_hint: 0,
        },
        |buf| request.write(buf, HANDSHAKE_VERSION),
    )
    .expect("encode ApiVersions request");

    let mut stream = TcpStream::connect(&bootstrap).expect("connect to broker");
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .expect("set read timeout");
    stream.write_all(&encoded).expect("send request");
    stream.flush().expect("flush request");

    let mut length_prefix = [0_u8; 4];
    stream
        .read_exact(&mut length_prefix)
        .expect("read frame length");
    let length = usize::try_from(i32::from_be_bytes(length_prefix))
        .expect("broker returned a non-negative frame length");
    let mut payload = vec![0_u8; length];
    stream.read_exact(&mut payload).expect("read frame payload");

    let mut raw = Vec::with_capacity(length.saturating_add(length_prefix.len()));
    raw.extend_from_slice(&length_prefix);
    raw.extend_from_slice(&payload);

    let response = decode_fixture(&raw);
    assert_eq!(response.error_code, 0, "broker returned an error code");

    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/api_versions");
    std::fs::create_dir_all(&dir).expect("create fixture directory");
    let bin = dir.join(format!("kafka-{label}.bin"));
    let json = dir.join(format!("kafka-{label}.json"));
    std::fs::write(&bin, &raw).expect("write fixture bytes");
    std::fs::write(&json, summary_json(&label, &response)).expect("write fixture summary");

    println!(
        "captured {} bytes from {bootstrap} ({} APIs) -> {}",
        raw.len(),
        response.api_keys.len(),
        bin.display()
    );
    println!("summary -> {}", json.display());
}
