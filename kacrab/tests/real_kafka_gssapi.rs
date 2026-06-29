//! Real Kafka GSSAPI integration smoke tests.

#![cfg(feature = "gssapi")]
#![allow(
    clippy::expect_used,
    clippy::print_stdout,
    reason = "Ignored real-broker tests are explicit smoke checks with direct failure output."
)]

use std::{env, net::SocketAddr, time::Duration};

use kacrab::wire::{BrokerEndpoint, ConnectionConfig, SaslMechanism, SecurityProtocol, WireClient};
use kacrab_protocol::{
    KafkaString,
    generated::{ApiKey, ApiVersionsRequestData, ApiVersionsResponseData},
};
use tokio::net::lookup_host;

#[tokio::test]
#[ignore = "requires a real Kafka broker using SASL/GSSAPI and an existing Kerberos credential \
            cache"]
async fn real_kafka_gssapi_api_versions_with_existing_kerberos_credentials() {
    let endpoint = gssapi_endpoint().await;
    let service_name = env::var("KACRAB_GSSAPI_SERVICE_NAME").ok();
    let jaas_config = env::var("KACRAB_GSSAPI_JAAS_CONFIG").ok();
    let configured_service_name = service_name
        .clone()
        .or_else(|| jaas_config.is_none().then(|| "kafka".to_owned()));
    let security_protocol = env::var("KACRAB_GSSAPI_SECURITY_PROTOCOL")
        .unwrap_or_else(|_error| "SASL_PLAINTEXT".to_owned());
    println!(
        "real Kafka GSSAPI smoke: bootstrap={}:{} resolved={}, serviceName={}, protocol={}",
        endpoint.host(),
        endpoint.port(),
        endpoint.addr,
        configured_service_name
            .as_deref()
            .unwrap_or("<JAAS serviceName>"),
        security_protocol
    );

    let mut config = ConnectionConfig::default()
        .request_timeout(Duration::from_secs(30))
        .socket_connection_setup_timeout(Duration::from_secs(10));
    config.security.protocol =
        SecurityProtocol::parse(&security_protocol).expect("KACRAB_GSSAPI_SECURITY_PROTOCOL");
    config.sasl.mechanism = Some(SaslMechanism::Gssapi);
    config.sasl.kerberos_service_name = configured_service_name;
    config.sasl.jaas_config = jaas_config;
    config.tls.truststore_certificates = env::var("KACRAB_GSSAPI_TRUSTSTORE_CERTIFICATES").ok();
    if let Ok(algorithm) = env::var("KACRAB_GSSAPI_TLS_ENDPOINT_IDENTIFICATION_ALGORITHM") {
        config.tls.endpoint_identification_algorithm = Some(algorithm);
    }

    let client =
        WireClient::connect_with_brokers(config, "kacrab-real-kafka-gssapi-test", [endpoint]);
    let request = ApiVersionsRequestData {
        client_software_name: KafkaString::from("kacrab".to_owned()),
        client_software_version: KafkaString::from(env!("CARGO_PKG_VERSION").to_owned()),
        _unknown_tagged_fields: Vec::new(),
    };

    let response: ApiVersionsResponseData = client
        .send_to_broker(0, ApiKey::ApiVersions, 3, &request)
        .await
        .expect("GSSAPI-authenticated ApiVersions should succeed");

    assert!(
        !response.api_keys.is_empty(),
        "broker should return advertised API versions"
    );
}

async fn gssapi_endpoint() -> BrokerEndpoint {
    let bootstrap = env::var("KACRAB_GSSAPI_BOOTSTRAP")
        .expect("set KACRAB_GSSAPI_BOOTSTRAP to host:port for a SASL/GSSAPI Kafka broker");
    let (host, port) = parse_host_port(&bootstrap);
    let addrs: Vec<SocketAddr> = lookup_host((host.as_str(), port))
        .await
        .expect("KACRAB_GSSAPI_BOOTSTRAP should resolve")
        .collect();
    let first = *addrs
        .first()
        .expect("KACRAB_GSSAPI_BOOTSTRAP should resolve to at least one address");
    // Prefer an IPv4 address when the name resolves to both: brokers published
    // by Docker Desktop on macOS are reachable on 127.0.0.1 but not on ::1, and
    // resolvers often return the IPv6 address first. The host string is kept
    // for the Kerberos SPN, so this does not change kafka/<host>.
    let addr = addrs.iter().copied().find(SocketAddr::is_ipv4).unwrap_or(first);
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
        .expect("KACRAB_GSSAPI_BOOTSTRAP must be host:port");
    (host.to_owned(), parse_port(port))
}

fn parse_port(value: &str) -> u16 {
    value
        .parse()
        .expect("KACRAB_GSSAPI_BOOTSTRAP port must fit u16")
}
