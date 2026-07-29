//! Per-test broker-capability gating for the `real_kafka_*` suites.
//!
//! The real-broker CI matrix (`.github/workflows/real-broker.yml`) runs every
//! suite unfiltered on every broker leg — 3.6.2 through 4.3.0 — so a test that
//! needs an API an old broker does not serve must skip *itself*, visibly,
//! instead of relying on a workflow-level test-name filter. The declaration
//! lives at the top of the test as a one-line guard:
//!
//! ```ignore
//! common::require_broker_api!(ApiKey::ConsumerGroupHeartbeat => 1);
//! ```
//!
//! The guard asks the connected broker's own `ApiVersions` response — never a
//! release number — whether it advertises the API at or above the named
//! version. When it does not, the guard prints a named
//! `SKIPPED: <test> needs <api> >= v<n>` line and returns from the test, so a
//! skip is visible in `--nocapture` output rather than silent. The published
//! per-surface floor table in `docs-book/src/broker-compatibility.md` is
//! derived from these guards; changing a guard means updating that table.
//!
//! Judging on the advertised range and not on the client's own range is
//! deliberate: if a broker advertises an API the client then fails to speak,
//! that is a client bug the test must surface, not a reason to skip.

use std::{
    env,
    net::{SocketAddr, ToSocketAddrs},
    sync::OnceLock,
};

use kacrab::wire::{BrokerCapabilities, BrokerEndpoint, ConnectionConfig, WireClient};
use kacrab_protocol::{
    KafkaString,
    generated::{ApiKey, ApiVersionsRequestData, ApiVersionsResponseData},
};

/// The `ApiVersions` handshake version kacrab itself pins (Kafka 2.4+); the
/// probe request below is sent at the same version.
const API_VERSIONS_REQUEST_VERSION: i16 = 3;

/// Skip the surrounding test unless the connected broker advertises every
/// listed API at or above the paired version: `require_broker_api!(api => min,
/// ...)`. On a shortfall it prints a named `SKIPPED: <test> needs <api> >=
/// v<min>` line and returns early; on a supporting broker it expands to
/// nothing observable, so the test body runs exactly as before.
macro_rules! require_broker_api {
    ($($api:expr => $min:expr),+ $(,)?) => {
        // An anchor whose `type_name` carries the enclosing test's module path,
        // so the SKIPPED line names the test without the test repeating its own
        // name (which would drift on a rename). A closure rather than a nested
        // `fn` so the expansion introduces no item into the test body.
        //
        // Known cost: expanding this macro in a test makes clippy stop applying
        // its in-test lint relaxations to that function (observed with
        // `arithmetic_side_effects` on Rust 1.95), so files using the guard
        // spell such allowances out at the top instead of relying on the
        // relaxation.
        let __kacrab_anchor = || {};
        $(
            if let Some(shortfall) =
                $crate::common::broker_capability::broker_api_shortfall($api, $min).await
            {
                println!(
                    "SKIPPED: {} needs {:?} >= v{} ({shortfall})",
                    $crate::common::broker_capability::test_name(::std::any::type_name_of_val(
                        &__kacrab_anchor,
                    )),
                    $api,
                    $min,
                );
                return;
            }
        )+
    };
}
pub(crate) use require_broker_api;

/// Why the connected broker cannot satisfy `api_key` at `min_version`, or
/// `None` when it can. The message distinguishes "not advertised at all"
/// (usually a broker predating the API) from "advertised below the required
/// version" — the same two cases kacrab's own negotiation errors keep apart.
pub(crate) async fn broker_api_shortfall(api_key: ApiKey, min_version: i16) -> Option<String> {
    match broker_capabilities().await.broker_range(api_key) {
        Some(range) if range.max_version >= min_version => None,
        Some(range) => Some(format!(
            "the broker advertises only v{}..v{}",
            range.min_version, range.max_version
        )),
        None => Some("the broker does not advertise the API".to_owned()),
    }
}

/// The enclosing test's name, recovered from the guard's anchor-closure type
/// name by trimming the closure segments — e.g.
/// `real_kafka_consumer::real_kafka_consumer_protocol_kip848::{{closure}}::{{closure}}`
/// (the async test body, then the anchor itself) becomes
/// `real_kafka_consumer::real_kafka_consumer_protocol_kip848`.
pub(crate) fn test_name(anchor: &'static str) -> &'static str {
    anchor.trim_end_matches("::{{closure}}")
}

/// The connected broker's advertised `ApiVersions` ranges, fetched once per
/// test process and cached: the suites run `--test-threads=1` against one
/// broker, so every guarded test in a binary shares the same answer.
///
/// The cache holds plain data rather than a connection because each
/// `#[tokio::test]` runs on its own runtime — a cached client would outlive
/// the runtime that created it.
async fn broker_capabilities() -> BrokerCapabilities {
    static CAPABILITIES: OnceLock<BrokerCapabilities> = OnceLock::new();
    if let Some(cached) = CAPABILITIES.get() {
        return cached.clone();
    }
    let fetched = fetch_broker_capabilities().await;
    CAPABILITIES.get_or_init(|| fetched).clone()
}

/// Ask the bootstrap broker what it serves: one `ApiVersions` round trip over
/// kacrab's own wire client, decoded into [`BrokerCapabilities`] exactly the
/// way the production handshake decodes it.
async fn fetch_broker_capabilities() -> BrokerCapabilities {
    let addr = bootstrap_socket_addr();
    let client = WireClient::connect_with_brokers(
        ConnectionConfig::default(),
        "kacrab-capability-probe",
        [BrokerEndpoint::new(0, addr)],
    );
    let request = ApiVersionsRequestData {
        client_software_name: KafkaString::from("kacrab-capability-probe".to_owned()),
        client_software_version: KafkaString::from(env!("CARGO_PKG_VERSION").to_owned()),
        _unknown_tagged_fields: Vec::new(),
    };
    let response: ApiVersionsResponseData = client
        .send_to_broker(
            0,
            ApiKey::ApiVersions,
            API_VERSIONS_REQUEST_VERSION,
            &request,
        )
        .await
        .expect("the capability probe should reach the bootstrap broker");
    BrokerCapabilities::from_response(&response)
}

/// The bootstrap address every real-broker suite reads: `KACRAB_BOOTSTRAP`,
/// defaulting to the compose fixtures' `127.0.0.1:9092`.
fn bootstrap_socket_addr() -> SocketAddr {
    let bootstrap =
        env::var("KACRAB_BOOTSTRAP").unwrap_or_else(|_error| "127.0.0.1:9092".to_owned());
    bootstrap
        .to_socket_addrs()
        .expect("KACRAB_BOOTSTRAP should be a resolvable host:port")
        .next()
        .expect("KACRAB_BOOTSTRAP should resolve to at least one address")
}
