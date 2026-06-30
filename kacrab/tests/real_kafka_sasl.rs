//! Real Kafka SASL (PLAIN / SCRAM) integration smoke tests.
//!
//! These exercise kacrab's non-Kerberos SASL client paths against a live broker
//! brought up by `docker-compose.auth.yml`. Each test authenticates over a
//! `SASL_PLAINTEXT` listener and runs `ApiVersions`, proving the full
//! handshake → authenticate → request flow works for that mechanism. A negative
//! test confirms the broker actually rejects bad credentials (i.e. auth is
//! enforced, not bypassed).
//!
//! Run them after `docker compose -f docker-compose.auth.yml up -d` with:
//!   `cargo test -p kacrab --test real_kafka_sasl -- --ignored --nocapture`

#![allow(
    clippy::arithmetic_side_effects,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::print_stdout,
    reason = "Ignored real-broker tests are explicit smoke checks; the local base64url/JWT \
              helpers index and add over small fixed inputs that cannot overflow or go out of \
              bounds."
)]

use std::{
    env,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use kacrab::wire::{BrokerEndpoint, ConnectionConfig, SaslMechanism, SecurityProtocol, WireClient};
use kacrab_protocol::{
    KafkaString,
    generated::{ApiKey, ApiVersionsRequestData, ApiVersionsResponseData},
};
use tokio::net::lookup_host;

#[tokio::test]
#[ignore = "requires the SASL broker from docker-compose.auth.yml"]
async fn real_kafka_sasl_plain_api_versions() {
    authenticate_and_assert(SaslMechanism::Plain, "plainuser", "plain-secret").await;
}

#[tokio::test]
#[ignore = "requires the SASL broker from docker-compose.auth.yml"]
async fn real_kafka_sasl_scram_sha_256_api_versions() {
    authenticate_and_assert(SaslMechanism::ScramSha256, "scram256", "scram256-secret").await;
}

#[tokio::test]
#[ignore = "requires the SASL broker from docker-compose.auth.yml"]
async fn real_kafka_sasl_scram_sha_512_api_versions() {
    authenticate_and_assert(SaslMechanism::ScramSha512, "scram512", "scram512-secret").await;
}

#[tokio::test]
#[ignore = "requires the SASL broker from docker-compose.auth.yml"]
async fn real_kafka_sasl_oauthbearer_api_versions() {
    let endpoint = sasl_endpoint().await;
    let token = unsecured_jwt("oauthuser", 3600);
    println!(
        "real Kafka SASL smoke: mechanism=OAUTHBEARER, sub=oauthuser, bootstrap={}:{} resolved={}",
        endpoint.host(),
        endpoint.port(),
        endpoint.addr,
    );

    let client = WireClient::connect_with_brokers(
        oauthbearer_config(&token),
        "kacrab-real-kafka-sasl-test",
        [endpoint],
    );
    let response: ApiVersionsResponseData = client
        .send_to_broker(0, ApiKey::ApiVersions, 3, &api_versions_request())
        .await
        .expect("OAUTHBEARER-authenticated ApiVersions should succeed");

    assert!(
        !response.api_keys.is_empty(),
        "broker should return advertised API versions after OAUTHBEARER auth"
    );
    println!(
        "OAUTHBEARER auth OK: broker advertised {} API keys",
        response.api_keys.len()
    );
}

#[tokio::test]
#[ignore = "requires the SASL broker from docker-compose.auth.yml"]
async fn real_kafka_sasl_oauthbearer_rejects_expired_token() {
    let endpoint = sasl_endpoint().await;
    // exp one hour in the past: the broker's unsecured validator must reject it.
    let token = unsecured_jwt("oauthuser", -3600);

    let client = WireClient::connect_with_brokers(
        oauthbearer_config(&token),
        "kacrab-real-kafka-sasl-negative",
        [endpoint],
    );
    let result: Result<ApiVersionsResponseData, _> = client
        .send_to_broker(0, ApiKey::ApiVersions, 3, &api_versions_request())
        .await;

    let error = result.expect_err("authentication with an expired token must fail");
    println!("OAUTHBEARER expired-token correctly rejected: {error}");
}

#[tokio::test]
#[ignore = "requires the SASL broker from docker-compose.auth.yml"]
async fn real_kafka_sasl_plain_rejects_wrong_password() {
    let endpoint = sasl_endpoint().await;
    let mut config = base_config();
    config.sasl.mechanism = Some(SaslMechanism::Plain);
    config.sasl.jaas_config = Some(jaas_config("plainuser", "the-wrong-password"));

    let client =
        WireClient::connect_with_brokers(config, "kacrab-real-kafka-sasl-negative", [endpoint]);
    let result: Result<ApiVersionsResponseData, _> = client
        .send_to_broker(0, ApiKey::ApiVersions, 3, &api_versions_request())
        .await;

    let error = result.expect_err("authentication with a wrong password must fail");
    println!("SASL/PLAIN wrong-password correctly rejected: {error}");
}

async fn authenticate_and_assert(mechanism: SaslMechanism, username: &str, password: &str) {
    let endpoint = sasl_endpoint().await;
    println!(
        "real Kafka SASL smoke: mechanism={}, user={username}, bootstrap={}:{} resolved={}",
        mechanism.as_str(),
        endpoint.host(),
        endpoint.port(),
        endpoint.addr,
    );

    let mut config = base_config();
    config.sasl.mechanism = Some(mechanism);
    config.sasl.jaas_config = Some(jaas_config(username, password));

    let client =
        WireClient::connect_with_brokers(config, "kacrab-real-kafka-sasl-test", [endpoint]);

    let response: ApiVersionsResponseData = client
        .send_to_broker(0, ApiKey::ApiVersions, 3, &api_versions_request())
        .await
        .unwrap_or_else(|error| {
            panic!(
                "{}-authenticated ApiVersions should succeed: {error}",
                mechanism.as_str()
            )
        });

    assert!(
        !response.api_keys.is_empty(),
        "broker should return advertised API versions after {} auth",
        mechanism.as_str()
    );
    println!(
        "{} auth OK: broker advertised {} API keys",
        mechanism.as_str(),
        response.api_keys.len()
    );
}

fn base_config() -> ConnectionConfig {
    let mut config = ConnectionConfig::default()
        .request_timeout(Duration::from_secs(30))
        .socket_connection_setup_timeout(Duration::from_secs(10));
    config.security.protocol = SecurityProtocol::SaslPlaintext;
    config
}

fn oauthbearer_config(token: &str) -> ConnectionConfig {
    let mut config = base_config();
    config.sasl.mechanism = Some(SaslMechanism::OAuthBearer);
    config.sasl.jaas_config = Some(format!(
        "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required \
         token=\"{token}\";"
    ));
    config
}

/// Builds an RFC 7515 "unsecured" JWS (`alg=none`, empty signature) carrying a
/// `sub`/`iat`/`exp` claim set, the exact shape Kafka's
/// `OAuthBearerUnsecuredValidatorCallbackHandler` accepts. `exp_offset_secs` is
/// added to the current time, so a negative value yields an expired token.
fn unsecured_jwt(subject: &str, exp_offset_secs: i64) -> String {
    let now = i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after unix epoch")
            .as_secs(),
    )
    .expect("unix seconds should fit i64");
    let exp = now + exp_offset_secs;
    let header = b64url(br#"{"alg":"none"}"#);
    let payload =
        b64url(format!("{{\"sub\":\"{subject}\",\"iat\":{now},\"exp\":{exp}}}").as_bytes());
    format!("{header}.{payload}.")
}

/// Minimal base64url (no padding) encoder, so the test needs no extra crate.
fn b64url(input: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b0 = u32::from(chunk[0]);
        let b1 = chunk.get(1).map_or(0, |byte| u32::from(*byte));
        let b2 = chunk.get(2).map_or(0, |byte| u32::from(*byte));
        let packed = (b0 << 16) | (b1 << 8) | b2;
        out.push(char::from(ALPHABET[(packed >> 18 & 0x3f) as usize]));
        out.push(char::from(ALPHABET[(packed >> 12 & 0x3f) as usize]));
        if chunk.len() > 1 {
            out.push(char::from(ALPHABET[(packed >> 6 & 0x3f) as usize]));
        }
        if chunk.len() > 2 {
            out.push(char::from(ALPHABET[(packed & 0x3f) as usize]));
        }
    }
    out
}

fn jaas_config(username: &str, password: &str) -> String {
    format!(
        "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{username}\" \
         password=\"{password}\";"
    )
}

fn api_versions_request() -> ApiVersionsRequestData {
    ApiVersionsRequestData {
        client_software_name: KafkaString::from("kacrab".to_owned()),
        client_software_version: KafkaString::from(env!("CARGO_PKG_VERSION").to_owned()),
        _unknown_tagged_fields: Vec::new(),
    }
}

async fn sasl_endpoint() -> BrokerEndpoint {
    let bootstrap =
        env::var("KACRAB_SASL_BOOTSTRAP").unwrap_or_else(|_error| "127.0.0.1:19092".to_owned());
    let (host, port) = parse_host_port(&bootstrap);
    let addr = lookup_host((host.as_str(), port))
        .await
        .expect("KACRAB_SASL_BOOTSTRAP should resolve")
        .next()
        .expect("KACRAB_SASL_BOOTSTRAP should resolve to at least one address");
    BrokerEndpoint::from_resolved(0, host, port, addr)
}

fn parse_host_port(value: &str) -> (String, u16) {
    if let Some(rest) = value.strip_prefix('[') {
        let (host, port) = rest
            .split_once("]:")
            .expect("bracketed IPv6 bootstrap must use [host]:port");
        return (host.to_owned(), parse_port(port));
    }
    let (host, port) = value
        .rsplit_once(':')
        .expect("KACRAB_SASL_BOOTSTRAP must be host:port");
    (host.to_owned(), parse_port(port))
}

fn parse_port(value: &str) -> u16 {
    value
        .parse()
        .expect("KACRAB_SASL_BOOTSTRAP port must fit u16")
}
