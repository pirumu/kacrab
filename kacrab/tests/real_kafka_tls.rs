//! Real Kafka TLS integration smoke tests: SSL, `SASL_SSL`, and mutual TLS.
//!
//! These drive kacrab's rustls-backed client against a broker that publishes
//! one-way SSL, SASL/SCRAM-over-TLS, and client-auth (mTLS) listeners
//! (`docker-compose.tls.yml`). Each connects and runs `ApiVersions`, proving
//! the handshake + (where applicable) SASL exchange complete over TLS. A
//! negative case confirms an untrusted server certificate is rejected.
//!
//! Run after generating the trust material and bringing the broker up:
//!   `bash scripts/gen-tls-certs.sh ./.tls-certs`
//!   `KACRAB_TLS_DIR=$PWD/.tls-certs docker compose -f docker-compose.tls.yml up -d`
//!   `KACRAB_TLS_DIR=$PWD/.tls-certs cargo test -p kacrab --test real_kafka_tls \
//!      -- --ignored --nocapture`

#![allow(
    clippy::expect_used,
    clippy::panic,
    clippy::print_stdout,
    reason = "Ignored real-broker tests are explicit smoke checks with direct failure output."
)]

use std::{env, fs, net::SocketAddr, path::PathBuf, time::Duration};

use kacrab::wire::{BrokerEndpoint, ConnectionConfig, SaslMechanism, SecurityProtocol, WireClient};
use kacrab_protocol::{
    KafkaString,
    generated::{ApiKey, ApiVersionsRequestData, ApiVersionsResponseData},
};
use tokio::net::lookup_host;

const SSL_PORT: u16 = 19096;
const SASL_SSL_PORT: u16 = 19097;
const MTLS_PORT: u16 = 19098;

#[tokio::test]
#[ignore = "requires the TLS broker from docker-compose.tls.yml"]
async fn real_kafka_ssl_api_versions() {
    let mut config = tls_config(SecurityProtocol::Ssl);
    config.tls.truststore_certificates = Some(ca_pem());
    assert_api_versions(config, SSL_PORT, "SSL").await;
}

#[tokio::test]
#[ignore = "requires the TLS broker from docker-compose.tls.yml"]
async fn real_kafka_sasl_ssl_scram_api_versions() {
    let mut config = tls_config(SecurityProtocol::SaslSsl);
    config.tls.truststore_certificates = Some(ca_pem());
    config.sasl.mechanism = Some(SaslMechanism::ScramSha256);
    config.sasl.jaas_config = Some(
        "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"scram256\" \
         password=\"scram256-secret\";"
            .to_owned(),
    );
    assert_api_versions(config, SASL_SSL_PORT, "SASL_SSL/SCRAM-SHA-256").await;
}

#[tokio::test]
#[ignore = "requires the TLS broker from docker-compose.tls.yml"]
async fn real_kafka_mutual_tls_api_versions() {
    let mut config = tls_config(SecurityProtocol::Ssl);
    config.tls.truststore_certificates = Some(ca_pem());
    config.tls.keystore_certificate_chain = Some(read_pem("client.crt"));
    config.tls.keystore_key = Some(read_pem("client.key"));
    assert_api_versions(config, MTLS_PORT, "mTLS").await;
}

#[tokio::test]
#[ignore = "requires the TLS broker from docker-compose.tls.yml"]
async fn real_kafka_ssl_rejects_untrusted_server() {
    // No truststore configured: the test CA is not among the OS roots, so the
    // server certificate must fail to verify.
    let config = tls_config(SecurityProtocol::Ssl);
    let endpoint = endpoint(SSL_PORT).await;
    let client =
        WireClient::connect_with_brokers(config, "kacrab-real-kafka-tls-negative", [endpoint]);
    let result: Result<ApiVersionsResponseData, _> = client
        .send_to_broker(0, ApiKey::ApiVersions, 3, &api_versions_request())
        .await;
    let error = result.expect_err("an untrusted server certificate must be rejected");
    println!("untrusted server cert correctly rejected: {error}");
}

async fn assert_api_versions(config: ConnectionConfig, port: u16, label: &str) {
    let endpoint = endpoint(port).await;
    println!(
        "real Kafka TLS smoke: {label}, bootstrap=localhost:{port} resolved={}",
        endpoint.addr
    );
    let client = WireClient::connect_with_brokers(config, "kacrab-real-kafka-tls-test", [endpoint]);
    let response: ApiVersionsResponseData = client
        .send_to_broker(0, ApiKey::ApiVersions, 3, &api_versions_request())
        .await
        .unwrap_or_else(|error| panic!("{label} ApiVersions should succeed over TLS: {error}"));
    assert!(
        !response.api_keys.is_empty(),
        "broker should return advertised API versions over {label}"
    );
    println!(
        "{label} OK: broker advertised {} API keys",
        response.api_keys.len()
    );
}

fn tls_config(protocol: SecurityProtocol) -> ConnectionConfig {
    let mut config = ConnectionConfig::default()
        .request_timeout(Duration::from_secs(30))
        .socket_connection_setup_timeout(Duration::from_secs(10));
    config.security.protocol = protocol;
    config
}

fn tls_dir() -> PathBuf {
    PathBuf::from(env::var("KACRAB_TLS_DIR").unwrap_or_else(|_error| "./.tls-certs".to_owned()))
}

fn read_pem(name: &str) -> String {
    let path = tls_dir().join(name);
    fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("cannot read TLS material {}: {error}", path.display()))
}

fn ca_pem() -> String {
    read_pem("ca.crt")
}

fn api_versions_request() -> ApiVersionsRequestData {
    ApiVersionsRequestData {
        client_software_name: KafkaString::from("kacrab".to_owned()),
        client_software_version: KafkaString::from(env!("CARGO_PKG_VERSION").to_owned()),
        _unknown_tagged_fields: Vec::new(),
    }
}

async fn endpoint(port: u16) -> BrokerEndpoint {
    // Keep the host string "localhost" so the TLS SNI / hostname check matches
    // the server cert SAN, but prefer the IPv4 address: Docker Desktop on macOS
    // publishes the broker on 127.0.0.1, not ::1, and resolvers often return
    // the IPv6 address first.
    let addrs: Vec<SocketAddr> = lookup_host(("localhost", port))
        .await
        .expect("localhost should resolve")
        .collect();
    let first = *addrs
        .first()
        .expect("localhost should resolve to at least one address");
    let addr = addrs
        .iter()
        .copied()
        .find(SocketAddr::is_ipv4)
        .unwrap_or(first);
    BrokerEndpoint::from_resolved(0, "localhost".to_owned(), port, addr)
}
