//! Wire/session integration tests.

#![allow(
    clippy::expect_used,
    clippy::missing_assert_message,
    clippy::too_many_lines,
    clippy::unwrap_used,
    reason = "Integration test fixtures fail fastest with contextual unwrap/expect calls."
)]

use base64::{Engine, engine::general_purpose};
use bytes::{Bytes, BytesMut};
use hmac::{Hmac, Mac};
use kacrab::wire::{BrokerEndpoint, ConnectionConfig, WireClient, WireError};
use kacrab_protocol::{
    KafkaString, frame,
    generated::{
        ApiKey, ApiVersion, ApiVersionsResponseData, MetadataResponseBroker, MetadataResponseData,
        MetadataResponsePartition, MetadataResponseTopic, ProduceRequestData, ProduceResponseData,
        RequestHeaderData, ResponseHeaderData, SaslAuthenticateRequestData,
        SaslAuthenticateResponseData, SaslHandshakeRequestData, SaslHandshakeResponseData,
    },
    version::{request_header_version, response_header_version},
};
use pkcs8::{
    EncodePrivateKey, PrivateKeyInfoOwned, SecretDocument,
    der::{Decode, pem::LineEnding},
};
use rcgen::{CertifiedKey, generate_simple_self_signed};
use rustls::{
    RootCertStore, ServerConfig,
    pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer},
    server::WebPkiClientVerifier,
};
use sha2::{Digest, Sha256, Sha512};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use tokio_rustls::TlsAcceptor;

#[tokio::test]
async fn wire_client_routes_requests_by_broker_id() {
    let broker_7 = MockBroker::serve_many(vec![
        Box::new(api_versions_response),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            let response = ApiVersionsResponseData {
                error_code: 0,
                api_keys: vec![ApiVersion {
                    api_key: ApiKey::Metadata as i16,
                    min_version: 0,
                    max_version: 7,
                    _unknown_tagged_fields: Vec::new(),
                }],
                ..ApiVersionsResponseData::default()
            };
            response_frame(ApiKey::ApiVersions, 3, header.correlation_id, &response)
        }),
    ])
    .await;
    let broker_8 = MockBroker::serve_many(vec![
        Box::new(api_versions_response),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            let response = ApiVersionsResponseData {
                error_code: 0,
                api_keys: vec![ApiVersion {
                    api_key: ApiKey::Metadata as i16,
                    min_version: 0,
                    max_version: 8,
                    _unknown_tagged_fields: Vec::new(),
                }],
                ..ApiVersionsResponseData::default()
            };
            response_frame(ApiKey::ApiVersions, 3, header.correlation_id, &response)
        }),
    ])
    .await;
    let client = WireClient::connect_with_brokers(
        ConnectionConfig::default(),
        "kacrab-test",
        [
            BrokerEndpoint::new(7, broker_7.addr()),
            BrokerEndpoint::new(8, broker_8.addr()),
        ],
    );
    let request = kacrab_protocol::generated::ApiVersionsRequestData {
        client_software_name: KafkaString::from("kacrab".to_owned()),
        client_software_version: KafkaString::from("0.0.1".to_owned()),
        _unknown_tagged_fields: Vec::new(),
    };

    let response_8: ApiVersionsResponseData = client
        .send_to_broker(8, ApiKey::ApiVersions, 3, &request)
        .await
        .unwrap();
    let response_7: ApiVersionsResponseData = client
        .send_to_broker(7, ApiKey::ApiVersions, 3, &request)
        .await
        .unwrap();

    assert_eq!(response_8.api_keys[0].max_version, 8);
    assert_eq!(response_7.api_keys[0].max_version, 7);
    assert_eq!(broker_7.join().await, 2);
    assert_eq!(broker_8.join().await, 2);
}

#[tokio::test]
async fn wire_client_uses_negotiated_broker_api_version_for_request_encoding() {
    let broker = MockBroker::serve_many(vec![
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            let response = ApiVersionsResponseData {
                error_code: 0,
                api_keys: vec![
                    ApiVersion {
                        api_key: ApiKey::ApiVersions as i16,
                        min_version: 0,
                        max_version: 4,
                        _unknown_tagged_fields: Vec::new(),
                    },
                    ApiVersion {
                        api_key: ApiKey::Produce as i16,
                        min_version: 0,
                        max_version: 8,
                        _unknown_tagged_fields: Vec::new(),
                    },
                ],
                ..ApiVersionsResponseData::default()
            };
            response_frame(ApiKey::ApiVersions, 3, header.correlation_id, &response)
        }),
        Box::new(|mut request| {
            let header_version = request_header_version(ApiKey::Produce as i16, 8);
            let header = RequestHeaderData::read(&mut request, header_version)
                .expect("produce request header should use negotiated version");
            assert_eq!(header.request_api_version, 8);
            let _produce = ProduceRequestData::read(&mut request, 8)
                .expect("produce body should use negotiated version");
            response_frame(
                ApiKey::Produce,
                8,
                header.correlation_id,
                &ProduceResponseData::default(),
            )
        }),
    ])
    .await;
    let client = WireClient::connect_with_brokers(
        ConnectionConfig::default(),
        "kacrab-test",
        [BrokerEndpoint::new(7, broker.addr())],
    );
    let request = ProduceRequestData::default();

    let _response: ProduceResponseData = client
        .send_to_broker(
            7,
            ApiKey::Produce,
            kacrab_protocol::version::client_api_info(ApiKey::Produce).max_version,
            &request,
        )
        .await
        .unwrap();

    assert_eq!(broker.join().await, 2);
}

#[tokio::test]
async fn wire_client_rejects_request_when_in_flight_limit_is_full() {
    let (request_seen_tx, request_seen_rx) = tokio::sync::oneshot::channel();
    let server = MockBroker::serve_blocking_after_handshake(request_seen_tx).await;
    let client = WireClient::connect_with_brokers(
        ConnectionConfig::default()
            .max_in_flight_requests_per_connection(1)
            .request_timeout(std::time::Duration::from_millis(200)),
        "kacrab-test",
        [BrokerEndpoint::new(7, server.addr())],
    );
    let request = kacrab_protocol::generated::ApiVersionsRequestData {
        client_software_name: KafkaString::from("kacrab".to_owned()),
        client_software_version: KafkaString::from("0.0.1".to_owned()),
        _unknown_tagged_fields: Vec::new(),
    };

    let first = {
        let client = client.clone();
        let request = request.clone();
        tokio::spawn(async move {
            client
                .send_to_broker::<_, ApiVersionsResponseData>(7, ApiKey::ApiVersions, 3, &request)
                .await
        })
    };
    request_seen_rx.await.expect("first request reached broker");

    let error = client
        .send_to_broker::<_, ApiVersionsResponseData>(7, ApiKey::ApiVersions, 3, &request)
        .await
        .unwrap_err();

    assert!(matches!(error, WireError::Backpressure));
    assert!(matches!(first.await.unwrap(), Err(WireError::Timeout)));
    let _handled = server.join().await;
}

#[tokio::test]
async fn wire_client_metadata_refresh_registers_leader_broker_endpoint() {
    let leader = MockBroker::serve_many(vec![
        Box::new(api_versions_response),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            let response = ApiVersionsResponseData {
                error_code: 0,
                api_keys: vec![ApiVersion {
                    api_key: ApiKey::Produce as i16,
                    min_version: 0,
                    max_version: 9,
                    _unknown_tagged_fields: Vec::new(),
                }],
                ..ApiVersionsResponseData::default()
            };
            response_frame(ApiKey::ApiVersions, 3, header.correlation_id, &response)
        }),
    ])
    .await;
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response),
        Box::new({
            let leader_addr = leader.addr();
            move |mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::Metadata as i16);
                let response = metadata_response(
                    7,
                    "orders",
                    vec![
                        (1, "127.0.0.1".to_owned(), 19_092),
                        (
                            7,
                            leader_addr.ip().to_string(),
                            i32::from(leader_addr.port()),
                        ),
                    ],
                );
                response_frame(ApiKey::Metadata, 13, header.correlation_id, &response)
            }
        }),
    ])
    .await;
    let client = WireClient::connect_with_brokers(
        ConnectionConfig::default(),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );
    let metadata = client.metadata_for_topics(["orders"]).await.unwrap();

    assert_eq!(
        metadata
            .leader_for("orders", 0)
            .map(|broker| broker.node_id),
        Some(7)
    );

    let request = kacrab_protocol::generated::ApiVersionsRequestData {
        client_software_name: KafkaString::from("kacrab".to_owned()),
        client_software_version: KafkaString::from("0.0.1".to_owned()),
        _unknown_tagged_fields: Vec::new(),
    };
    let response: ApiVersionsResponseData = client
        .send_to_broker(7, ApiKey::ApiVersions, 3, &request)
        .await
        .unwrap();

    assert_eq!(response.api_keys[0].api_key, ApiKey::Produce as i16);
    assert_eq!(bootstrap.join().await, 2);
    assert_eq!(leader.join().await, 2);
}

#[tokio::test]
async fn wire_client_refreshes_metadata_after_partition_invalidation() {
    let bootstrap = MockBroker::serve_many(vec![
        Box::new(api_versions_response),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            let response = metadata_response(
                7,
                "orders",
                vec![
                    (1, "127.0.0.1".to_owned(), 19_092),
                    (7, "127.0.0.1".to_owned(), 19_093),
                ],
            );
            response_frame(ApiKey::Metadata, 13, header.correlation_id, &response)
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            let response = metadata_response(
                8,
                "orders",
                vec![
                    (1, "127.0.0.1".to_owned(), 19_092),
                    (8, "127.0.0.1".to_owned(), 19_094),
                ],
            );
            response_frame(ApiKey::Metadata, 13, header.correlation_id, &response)
        }),
    ])
    .await;
    let client = WireClient::connect_with_brokers(
        ConnectionConfig::default(),
        "kacrab-test",
        [BrokerEndpoint::new(1, bootstrap.addr())],
    );

    let first = client.metadata_for_topics(["orders"]).await.unwrap();
    assert_eq!(
        first.leader_for("orders", 0).map(|broker| broker.node_id),
        Some(7)
    );

    client.invalidate_topic_partition("orders", 0);

    let second = client.metadata_for_topics(["orders"]).await.unwrap();
    assert_eq!(
        second.leader_for("orders", 0).map(|broker| broker.node_id),
        Some(8)
    );
    assert_eq!(bootstrap.join().await, 3);
}

#[tokio::test]
async fn wire_client_stress_pipelines_requests_across_multiple_brokers() {
    const REQUESTS_PER_BROKER: usize = 32;

    let broker_7 = MockBroker::serve_pipelined_api_versions(REQUESTS_PER_BROKER, 7).await;
    let broker_8 = MockBroker::serve_pipelined_api_versions(REQUESTS_PER_BROKER, 8).await;
    let client = WireClient::connect_with_brokers(
        ConnectionConfig::default()
            .max_in_flight_requests_per_connection(REQUESTS_PER_BROKER + 1)
            .broker_queue_capacity(REQUESTS_PER_BROKER + 1),
        "kacrab-test",
        [
            BrokerEndpoint::new(7, broker_7.addr()),
            BrokerEndpoint::new(8, broker_8.addr()),
        ],
    );
    let request = kacrab_protocol::generated::ApiVersionsRequestData {
        client_software_name: KafkaString::from("kacrab".to_owned()),
        client_software_version: KafkaString::from("0.0.1".to_owned()),
        _unknown_tagged_fields: Vec::new(),
    };
    let mut tasks = Vec::with_capacity(REQUESTS_PER_BROKER * 2);
    for index in 0..(REQUESTS_PER_BROKER * 2) {
        let client = client.clone();
        let request = request.clone();
        let broker_id = if index % 2 == 0 { 7 } else { 8 };
        tasks.push(tokio::spawn(async move {
            client
                .send_to_broker::<_, ApiVersionsResponseData>(
                    broker_id,
                    ApiKey::ApiVersions,
                    3,
                    &request,
                )
                .await
                .map(|response| (broker_id, response))
        }));
    }

    let mut broker_7_responses = 0;
    let mut broker_8_responses = 0;
    for task in tasks {
        let (broker_id, response) = task.await.unwrap().unwrap();
        assert_eq!(response.api_keys.len(), 1);
        assert_eq!(i32::from(response.api_keys[0].max_version), broker_id);
        if broker_id == 7 {
            broker_7_responses += 1;
        } else {
            broker_8_responses += 1;
        }
    }

    assert_eq!(broker_7_responses, REQUESTS_PER_BROKER);
    assert_eq!(broker_8_responses, REQUESTS_PER_BROKER);
    assert_eq!(broker_7.join().await, REQUESTS_PER_BROKER + 1);
    assert_eq!(broker_8.join().await, REQUESTS_PER_BROKER + 1);
}

#[tokio::test]
async fn wire_client_runs_sasl_plain_before_regular_requests() {
    let broker = MockBroker::serve_many(vec![
        Box::new(api_versions_response),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(
                &mut request,
                request_header_version(ApiKey::SaslHandshake as i16, 1),
            )
            .expect("sasl handshake header");
            assert_eq!(header.request_api_key, ApiKey::SaslHandshake as i16);
            let body =
                SaslHandshakeRequestData::read(&mut request, 1).expect("sasl handshake body");
            assert_eq!(body.mechanism.to_string(), "PLAIN");
            let response = SaslHandshakeResponseData {
                error_code: 0,
                mechanisms: vec![KafkaString::from("PLAIN".to_owned())],
                _unknown_tagged_fields: Vec::new(),
            };
            response_frame(ApiKey::SaslHandshake, 1, header.correlation_id, &response)
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(
                &mut request,
                request_header_version(ApiKey::SaslAuthenticate as i16, 2),
            )
            .expect("sasl authenticate header");
            assert_eq!(header.request_api_key, ApiKey::SaslAuthenticate as i16);
            let body =
                SaslAuthenticateRequestData::read(&mut request, 2).expect("sasl authenticate body");
            assert_eq!(body.auth_bytes.as_ref(), b"\0alice\0secret");
            let response = SaslAuthenticateResponseData {
                error_code: 0,
                error_message: None,
                auth_bytes: Bytes::new(),
                session_lifetime_ms: 300_000,
                _unknown_tagged_fields: Vec::new(),
            };
            response_frame(
                ApiKey::SaslAuthenticate,
                2,
                header.correlation_id,
                &response,
            )
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::ApiVersions as i16);
            let response = ApiVersionsResponseData {
                error_code: 0,
                api_keys: vec![ApiVersion {
                    api_key: ApiKey::ApiVersions as i16,
                    min_version: 0,
                    max_version: 4,
                    _unknown_tagged_fields: Vec::new(),
                }],
                ..ApiVersionsResponseData::default()
            };
            response_frame(ApiKey::ApiVersions, 3, header.correlation_id, &response)
        }),
    ])
    .await;
    let mut config = ConnectionConfig::default().request_timeout(std::time::Duration::from_secs(1));
    config.security.protocol = kacrab::wire::SecurityProtocol::SaslPlaintext;
    config.sasl.mechanism = Some(kacrab::wire::SaslMechanism::Plain);
    config.sasl.jaas_config = Some(
        "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"alice\" \
         password=\"secret\";"
            .to_owned(),
    );
    let client = WireClient::connect_with_brokers(
        config,
        "kacrab-test",
        [BrokerEndpoint::new(7, broker.addr())],
    );
    let request = kacrab_protocol::generated::ApiVersionsRequestData {
        client_software_name: KafkaString::from("kacrab".to_owned()),
        client_software_version: KafkaString::from("0.0.1".to_owned()),
        _unknown_tagged_fields: Vec::new(),
    };

    let response: ApiVersionsResponseData = client
        .send_to_broker(7, ApiKey::ApiVersions, 3, &request)
        .await
        .unwrap();

    assert_eq!(response.api_keys[0].max_version, 4);
    assert_eq!(broker.join().await, 4);
}

#[tokio::test]
async fn wire_client_runs_oauthbearer_before_regular_requests() {
    let broker = MockBroker::serve_many(vec![
        Box::new(api_versions_response),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(
                &mut request,
                request_header_version(ApiKey::SaslHandshake as i16, 1),
            )
            .expect("sasl handshake header");
            let body =
                SaslHandshakeRequestData::read(&mut request, 1).expect("sasl handshake body");
            assert_eq!(body.mechanism.to_string(), "OAUTHBEARER");
            let response = SaslHandshakeResponseData {
                error_code: 0,
                mechanisms: vec![KafkaString::from("OAUTHBEARER".to_owned())],
                _unknown_tagged_fields: Vec::new(),
            };
            response_frame(ApiKey::SaslHandshake, 1, header.correlation_id, &response)
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(
                &mut request,
                request_header_version(ApiKey::SaslAuthenticate as i16, 2),
            )
            .expect("sasl authenticate header");
            assert_eq!(header.request_api_key, ApiKey::SaslAuthenticate as i16);
            let body =
                SaslAuthenticateRequestData::read(&mut request, 2).expect("sasl authenticate body");
            assert_eq!(
                body.auth_bytes.as_ref(),
                b"n,,\x01auth=Bearer eyJhbGciOiJub25lIn0.eyJzdWIiOiJhbGljZSJ9.\x01\x01"
            );
            let response = SaslAuthenticateResponseData {
                error_code: 0,
                error_message: None,
                auth_bytes: Bytes::new(),
                session_lifetime_ms: 300_000,
                _unknown_tagged_fields: Vec::new(),
            };
            response_frame(
                ApiKey::SaslAuthenticate,
                2,
                header.correlation_id,
                &response,
            )
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::ApiVersions as i16);
            let response = ApiVersionsResponseData {
                error_code: 0,
                api_keys: vec![ApiVersion {
                    api_key: ApiKey::ApiVersions as i16,
                    min_version: 0,
                    max_version: 4,
                    _unknown_tagged_fields: Vec::new(),
                }],
                ..ApiVersionsResponseData::default()
            };
            response_frame(ApiKey::ApiVersions, 3, header.correlation_id, &response)
        }),
    ])
    .await;
    let mut config = ConnectionConfig::default().request_timeout(std::time::Duration::from_secs(1));
    config.security.protocol = kacrab::wire::SecurityProtocol::SaslPlaintext;
    config.sasl.mechanism = Some(kacrab::wire::SaslMechanism::OAuthBearer);
    config.sasl.jaas_config = Some(
        "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required \
         token=\"eyJhbGciOiJub25lIn0.eyJzdWIiOiJhbGljZSJ9.\";"
            .to_owned(),
    );
    let client = WireClient::connect_with_brokers(
        config,
        "kacrab-test",
        [BrokerEndpoint::new(7, broker.addr())],
    );
    let request = kacrab_protocol::generated::ApiVersionsRequestData {
        client_software_name: KafkaString::from("kacrab".to_owned()),
        client_software_version: KafkaString::from("0.0.1".to_owned()),
        _unknown_tagged_fields: Vec::new(),
    };

    let response: ApiVersionsResponseData = client
        .send_to_broker(7, ApiKey::ApiVersions, 3, &request)
        .await
        .unwrap();

    assert_eq!(response.api_keys[0].max_version, 4);
    assert_eq!(broker.join().await, 4);
}

#[tokio::test]
async fn wire_client_fetches_oauthbearer_token_from_http_endpoint() {
    let oauth = MockOAuthServer::serve_token(
        "grant_type=client_credentials&client_id=client-a&client_secret=secret-a&scope=orders.\
         write",
        "token-from-endpoint",
    )
    .await;
    let broker = MockBroker::serve_many(vec![
        Box::new(api_versions_response),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(
                &mut request,
                request_header_version(ApiKey::SaslHandshake as i16, 1),
            )
            .expect("sasl handshake header");
            let body =
                SaslHandshakeRequestData::read(&mut request, 1).expect("sasl handshake body");
            assert_eq!(body.mechanism.to_string(), "OAUTHBEARER");
            let response = SaslHandshakeResponseData {
                error_code: 0,
                mechanisms: vec![KafkaString::from("OAUTHBEARER".to_owned())],
                _unknown_tagged_fields: Vec::new(),
            };
            response_frame(ApiKey::SaslHandshake, 1, header.correlation_id, &response)
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(
                &mut request,
                request_header_version(ApiKey::SaslAuthenticate as i16, 2),
            )
            .expect("sasl authenticate header");
            let body =
                SaslAuthenticateRequestData::read(&mut request, 2).expect("sasl authenticate body");
            assert_eq!(
                body.auth_bytes.as_ref(),
                b"n,,\x01auth=Bearer token-from-endpoint\x01\x01"
            );
            let response = SaslAuthenticateResponseData {
                error_code: 0,
                error_message: None,
                auth_bytes: Bytes::new(),
                session_lifetime_ms: 300_000,
                _unknown_tagged_fields: Vec::new(),
            };
            response_frame(
                ApiKey::SaslAuthenticate,
                2,
                header.correlation_id,
                &response,
            )
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::ApiVersions as i16);
            let response = ApiVersionsResponseData {
                error_code: 0,
                api_keys: vec![ApiVersion {
                    api_key: ApiKey::ApiVersions as i16,
                    min_version: 0,
                    max_version: 4,
                    _unknown_tagged_fields: Vec::new(),
                }],
                ..ApiVersionsResponseData::default()
            };
            response_frame(ApiKey::ApiVersions, 3, header.correlation_id, &response)
        }),
    ])
    .await;
    let mut config = ConnectionConfig::default();
    config.security.protocol = kacrab::wire::SecurityProtocol::SaslPlaintext;
    config.sasl.mechanism = Some(kacrab::wire::SaslMechanism::OAuthBearer);
    config.sasl.oauthbearer_token_endpoint_url = Some(oauth.url("/token"));
    config.sasl.oauthbearer_client_id = Some("client-a".to_owned());
    config.sasl.oauthbearer_client_secret = Some("secret-a".to_owned());
    config.sasl.oauthbearer_scope = Some("orders.write".to_owned());
    config.sasl.login_connect_timeout = Some(std::time::Duration::from_secs(1));
    config.sasl.login_read_timeout = Some(std::time::Duration::from_secs(1));
    let client = WireClient::connect_with_brokers(
        config,
        "kacrab-test",
        [BrokerEndpoint::new(7, broker.addr())],
    );
    let request = kacrab_protocol::generated::ApiVersionsRequestData {
        client_software_name: KafkaString::from("kacrab".to_owned()),
        client_software_version: KafkaString::from("0.0.1".to_owned()),
        _unknown_tagged_fields: Vec::new(),
    };

    let response: ApiVersionsResponseData = client
        .send_to_broker(7, ApiKey::ApiVersions, 3, &request)
        .await
        .unwrap();

    assert_eq!(response.api_keys[0].max_version, 4);
    assert_eq!(broker.join().await, 4);
    assert_eq!(oauth.join().await, 1);
}

#[tokio::test]
async fn wire_client_reuses_oauthbearer_token_across_broker_sessions() {
    let oauth = MockOAuthServer::serve_token(
        "grant_type=client_credentials&client_id=client-a&client_secret=secret-a",
        "token-from-cache",
    )
    .await;
    let broker_7 = MockBroker::serve_many(oauthbearer_handlers("token-from-cache", 7)).await;
    let broker_8 = MockBroker::serve_many(oauthbearer_handlers("token-from-cache", 8)).await;
    let mut config = ConnectionConfig::default().request_timeout(std::time::Duration::from_secs(1));
    config.security.protocol = kacrab::wire::SecurityProtocol::SaslPlaintext;
    config.sasl.mechanism = Some(kacrab::wire::SaslMechanism::OAuthBearer);
    config.sasl.oauthbearer_token_endpoint_url = Some(oauth.url("/token"));
    config.sasl.oauthbearer_client_id = Some("client-a".to_owned());
    config.sasl.oauthbearer_client_secret = Some("secret-a".to_owned());
    config.sasl.login_connect_timeout = Some(std::time::Duration::from_secs(1));
    config.sasl.login_read_timeout = Some(std::time::Duration::from_secs(1));
    let client = WireClient::connect_with_brokers(
        config,
        "kacrab-test",
        [
            BrokerEndpoint::new(7, broker_7.addr()),
            BrokerEndpoint::new(8, broker_8.addr()),
        ],
    );
    let request = kacrab_protocol::generated::ApiVersionsRequestData {
        client_software_name: KafkaString::from("kacrab".to_owned()),
        client_software_version: KafkaString::from("0.0.1".to_owned()),
        _unknown_tagged_fields: Vec::new(),
    };

    let response_7: ApiVersionsResponseData = client
        .send_to_broker(7, ApiKey::ApiVersions, 3, &request)
        .await
        .unwrap();
    let response_8: ApiVersionsResponseData = client
        .send_to_broker(8, ApiKey::ApiVersions, 3, &request)
        .await
        .unwrap();

    assert_eq!(response_7.api_keys[0].max_version, 7);
    assert_eq!(response_8.api_keys[0].max_version, 8);
    assert_eq!(broker_7.join().await, 4);
    assert_eq!(broker_8.join().await, 4);
    assert_eq!(oauth.join().await, 1);
}

#[tokio::test]
async fn wire_client_exchanges_oauthbearer_assertion_file_for_token() {
    let assertion_path = write_temp_file("assertion", b"header.payload.signature\n");
    let oauth = MockOAuthServer::serve_token(
        "grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion=header.\
         payload.signature&scope=orders.write",
        "token-from-assertion",
    )
    .await;
    let broker = MockBroker::serve_many(vec![
        Box::new(api_versions_response),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(
                &mut request,
                request_header_version(ApiKey::SaslHandshake as i16, 1),
            )
            .expect("sasl handshake header");
            let body =
                SaslHandshakeRequestData::read(&mut request, 1).expect("sasl handshake body");
            assert_eq!(body.mechanism.to_string(), "OAUTHBEARER");
            let response = SaslHandshakeResponseData {
                error_code: 0,
                mechanisms: vec![KafkaString::from("OAUTHBEARER".to_owned())],
                _unknown_tagged_fields: Vec::new(),
            };
            response_frame(ApiKey::SaslHandshake, 1, header.correlation_id, &response)
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(
                &mut request,
                request_header_version(ApiKey::SaslAuthenticate as i16, 2),
            )
            .expect("sasl authenticate header");
            let body =
                SaslAuthenticateRequestData::read(&mut request, 2).expect("sasl authenticate body");
            assert_eq!(
                body.auth_bytes.as_ref(),
                b"n,,\x01auth=Bearer token-from-assertion\x01\x01"
            );
            let response = SaslAuthenticateResponseData {
                error_code: 0,
                error_message: None,
                auth_bytes: Bytes::new(),
                session_lifetime_ms: 300_000,
                _unknown_tagged_fields: Vec::new(),
            };
            response_frame(
                ApiKey::SaslAuthenticate,
                2,
                header.correlation_id,
                &response,
            )
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::ApiVersions as i16);
            let response = ApiVersionsResponseData {
                error_code: 0,
                api_keys: vec![ApiVersion {
                    api_key: ApiKey::ApiVersions as i16,
                    min_version: 0,
                    max_version: 4,
                    _unknown_tagged_fields: Vec::new(),
                }],
                ..ApiVersionsResponseData::default()
            };
            response_frame(ApiKey::ApiVersions, 3, header.correlation_id, &response)
        }),
    ])
    .await;
    let mut config = ConnectionConfig::default();
    config.security.protocol = kacrab::wire::SecurityProtocol::SaslPlaintext;
    config.sasl.mechanism = Some(kacrab::wire::SaslMechanism::OAuthBearer);
    config.sasl.oauthbearer_token_endpoint_url = Some(oauth.url("/token"));
    config.sasl.oauthbearer_assertion_file = Some(assertion_path);
    config.sasl.oauthbearer_scope = Some("orders.write".to_owned());
    config.sasl.login_connect_timeout = Some(std::time::Duration::from_secs(1));
    config.sasl.login_read_timeout = Some(std::time::Duration::from_secs(1));
    let client = WireClient::connect_with_brokers(
        config,
        "kacrab-test",
        [BrokerEndpoint::new(7, broker.addr())],
    );
    let request = kacrab_protocol::generated::ApiVersionsRequestData {
        client_software_name: KafkaString::from("kacrab".to_owned()),
        client_software_version: KafkaString::from("0.0.1".to_owned()),
        _unknown_tagged_fields: Vec::new(),
    };

    let response: ApiVersionsResponseData = client
        .send_to_broker(7, ApiKey::ApiVersions, 3, &request)
        .await
        .unwrap();

    assert_eq!(response.api_keys[0].max_version, 4);
    assert_eq!(broker.join().await, 4);
    assert_eq!(oauth.join().await, 1);
}

#[tokio::test]
async fn wire_client_builds_oauthbearer_assertion_from_private_key_template_and_claims() {
    let key_pair = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256)
        .expect("generate assertion key");
    let encrypted_key = encrypted_pkcs8_pem(&key_pair.serialize_pem(), "assertion-secret");
    let key_path = write_temp_file("assertion-key", encrypted_key.as_bytes());
    let template_path = write_temp_file(
        "assertion-template",
        br#"{"header":{"kid":"key-a"},"claims":{"tenant":"blue"}}"#,
    );
    let oauth = MockOAuthServer::serve_token_with(
        |body| {
            assert!(body.starts_with(
                "grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion="
            ));
            assert!(body.ends_with("&scope=orders.write"));
            let assertion = body
                .split('&')
                .find_map(|part| part.strip_prefix("assertion="))
                .expect("assertion form field");
            let token = jsonwebtoken::dangerous::insecure_decode::<serde_json::Value>(assertion)
                .expect("JWT assertion should decode");

            assert_eq!(token.header.alg, jsonwebtoken::Algorithm::ES256);
            assert_eq!(token.header.kid.as_deref(), Some("key-a"));
            assert_eq!(
                token.claims.get("iss").and_then(serde_json::Value::as_str),
                Some("issuer-a")
            );
            assert_eq!(
                token.claims.get("sub").and_then(serde_json::Value::as_str),
                Some("subject-a")
            );
            assert_eq!(
                token.claims.get("aud").and_then(serde_json::Value::as_str),
                Some("audience-a")
            );
            assert_eq!(
                token
                    .claims
                    .get("tenant")
                    .and_then(serde_json::Value::as_str),
                Some("blue")
            );
            assert!(
                token
                    .claims
                    .get("exp")
                    .and_then(serde_json::Value::as_i64)
                    .is_some()
            );
            assert!(
                token
                    .claims
                    .get("nbf")
                    .and_then(serde_json::Value::as_i64)
                    .is_some()
            );
            assert!(
                token
                    .claims
                    .get("jti")
                    .and_then(serde_json::Value::as_str)
                    .is_some()
            );
        },
        "token-from-built-assertion",
    )
    .await;
    let broker =
        MockBroker::serve_many(oauthbearer_handlers("token-from-built-assertion", 4)).await;
    let mut config = ConnectionConfig::default();
    config.security.protocol = kacrab::wire::SecurityProtocol::SaslPlaintext;
    config.sasl.mechanism = Some(kacrab::wire::SaslMechanism::OAuthBearer);
    config.sasl.oauthbearer_token_endpoint_url = Some(oauth.url("/token"));
    config.sasl.oauthbearer_assertion_private_key_file = Some(key_path);
    config.sasl.oauthbearer_assertion_private_key_passphrase = Some("assertion-secret".to_owned());
    config.sasl.oauthbearer_assertion_template_file = Some(template_path);
    config.sasl.oauthbearer_assertion_algorithm = String::from("ES256");
    config.sasl.oauthbearer_assertion_claim_iss = Some("issuer-a".to_owned());
    config.sasl.oauthbearer_assertion_claim_sub = Some("subject-a".to_owned());
    config.sasl.oauthbearer_assertion_claim_aud = Some("audience-a".to_owned());
    config.sasl.oauthbearer_assertion_claim_jti_include = true;
    config.sasl.oauthbearer_assertion_claim_exp = std::time::Duration::from_mins(2);
    config.sasl.oauthbearer_assertion_claim_nbf = std::time::Duration::from_secs(5);
    config.sasl.oauthbearer_scope = Some("orders.write".to_owned());
    config.sasl.login_connect_timeout = Some(std::time::Duration::from_secs(1));
    config.sasl.login_read_timeout = Some(std::time::Duration::from_secs(1));
    let client = WireClient::connect_with_brokers(
        config,
        "kacrab-test",
        [BrokerEndpoint::new(7, broker.addr())],
    );
    let request = kacrab_protocol::generated::ApiVersionsRequestData {
        client_software_name: KafkaString::from("kacrab".to_owned()),
        client_software_version: KafkaString::from("0.0.1".to_owned()),
        _unknown_tagged_fields: Vec::new(),
    };

    let response: ApiVersionsResponseData = client
        .send_to_broker(7, ApiKey::ApiVersions, 3, &request)
        .await
        .unwrap();

    assert_eq!(response.api_keys[0].max_version, 4);
    assert_eq!(broker.join().await, 4);
    assert_eq!(oauth.join().await, 1);
}

#[tokio::test]
async fn wire_client_returns_local_gssapi_setup_error_without_retrying_until_timeout() {
    let broker = MockBroker::serve_many(vec![Box::new(api_versions_response)]).await;
    let mut config = ConnectionConfig::default().request_timeout(std::time::Duration::from_secs(5));
    config.security.protocol = kacrab::wire::SecurityProtocol::SaslPlaintext;
    config.sasl.mechanism = Some(kacrab::wire::SaslMechanism::Gssapi);
    let client = WireClient::connect_with_brokers(
        config,
        "kacrab-test",
        [BrokerEndpoint::new(7, broker.addr())],
    );
    let request = kacrab_protocol::generated::ApiVersionsRequestData {
        client_software_name: KafkaString::from("kacrab".to_owned()),
        client_software_version: KafkaString::from("0.0.1".to_owned()),
        _unknown_tagged_fields: Vec::new(),
    };

    let error = client
        .send_to_broker::<_, ApiVersionsResponseData>(7, ApiKey::ApiVersions, 3, &request)
        .await
        .unwrap_err();

    #[cfg(feature = "gssapi")]
    assert!(matches!(error, WireError::InvalidSaslConfig(_)));
    #[cfg(not(feature = "gssapi"))]
    assert!(matches!(error, WireError::GssapiBackendUnavailable));
    assert_eq!(broker.join().await, 1);
}

#[tokio::test]
async fn wire_client_rejects_java_sasl_callback_handler_classes() {
    let broker = MockBroker::serve_many(vec![Box::new(api_versions_response)]).await;
    let mut config = ConnectionConfig::default().request_timeout(std::time::Duration::from_secs(5));
    config.security.protocol = kacrab::wire::SecurityProtocol::SaslPlaintext;
    config.sasl.mechanism = Some(kacrab::wire::SaslMechanism::Plain);
    config.sasl.login_callback_handler_class = Some("com.example.LoginCallback".to_owned());
    let client = WireClient::connect_with_brokers(
        config,
        "kacrab-test",
        [BrokerEndpoint::new(7, broker.addr())],
    );
    let request = kacrab_protocol::generated::ApiVersionsRequestData {
        client_software_name: KafkaString::from("kacrab".to_owned()),
        client_software_version: KafkaString::from("0.0.1".to_owned()),
        _unknown_tagged_fields: Vec::new(),
    };

    let error = client
        .send_to_broker::<_, ApiVersionsResponseData>(7, ApiKey::ApiVersions, 3, &request)
        .await
        .unwrap_err();

    assert!(matches!(error, WireError::InvalidSaslConfig(_)));
    assert_eq!(broker.join().await, 1);
}

#[tokio::test]
async fn wire_client_runs_tls_before_api_versions() {
    let tls = MockTlsBroker::serve_many(vec![
        Box::new(api_versions_response),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::ApiVersions as i16);
            let response = ApiVersionsResponseData {
                error_code: 0,
                api_keys: vec![ApiVersion {
                    api_key: ApiKey::ApiVersions as i16,
                    min_version: 0,
                    max_version: 4,
                    _unknown_tagged_fields: Vec::new(),
                }],
                ..ApiVersionsResponseData::default()
            };
            response_frame(ApiKey::ApiVersions, 3, header.correlation_id, &response)
        }),
    ])
    .await;
    let mut config = ConnectionConfig::default();
    config.security.protocol = kacrab::wire::SecurityProtocol::Ssl;
    config.tls.truststore_location = Some(tls.truststore_path.clone());
    let client = WireClient::connect_with_brokers(
        config,
        "kacrab-test",
        [BrokerEndpoint::new(7, tls.addr())],
    );
    let request = kacrab_protocol::generated::ApiVersionsRequestData {
        client_software_name: KafkaString::from("kacrab".to_owned()),
        client_software_version: KafkaString::from("0.0.1".to_owned()),
        _unknown_tagged_fields: Vec::new(),
    };

    let response: ApiVersionsResponseData = client
        .send_to_broker(7, ApiKey::ApiVersions, 3, &request)
        .await
        .unwrap();

    assert_eq!(response.api_keys[0].max_version, 4);
    assert_eq!(tls.join().await, 2);
}

#[tokio::test]
async fn wire_client_allows_explicit_hostname_verification_disable() {
    let tls = MockTlsBroker::serve_many_for_subject(
        "broker.internal",
        vec![
            Box::new(api_versions_response),
            Box::new(|mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                let response = ApiVersionsResponseData {
                    error_code: 0,
                    api_keys: vec![ApiVersion {
                        api_key: ApiKey::ApiVersions as i16,
                        min_version: 0,
                        max_version: 4,
                        _unknown_tagged_fields: Vec::new(),
                    }],
                    ..ApiVersionsResponseData::default()
                };
                response_frame(ApiKey::ApiVersions, 3, header.correlation_id, &response)
            }),
        ],
    )
    .await;
    let mut config = ConnectionConfig::default();
    config.security.protocol = kacrab::wire::SecurityProtocol::Ssl;
    config.tls.truststore_certificates = Some(tls.truststore_pem.clone());
    config.tls.endpoint_identification_algorithm = Some(String::new());
    let client = WireClient::connect_with_brokers(
        config,
        "kacrab-test",
        [BrokerEndpoint::new(7, tls.addr())],
    );
    let request = kacrab_protocol::generated::ApiVersionsRequestData {
        client_software_name: KafkaString::from("kacrab".to_owned()),
        client_software_version: KafkaString::from("0.0.1".to_owned()),
        _unknown_tagged_fields: Vec::new(),
    };

    let response: ApiVersionsResponseData = client
        .send_to_broker(7, ApiKey::ApiVersions, 3, &request)
        .await
        .unwrap();

    assert_eq!(response.api_keys[0].max_version, 4);
    assert_eq!(tls.join().await, 2);
}

#[tokio::test]
async fn wire_client_runs_tls_with_client_certificate_before_api_versions() {
    let tls = MockTlsBroker::serve_many_with_client_auth(vec![
        Box::new(api_versions_response),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::ApiVersions as i16);
            let response = ApiVersionsResponseData {
                error_code: 0,
                api_keys: vec![ApiVersion {
                    api_key: ApiKey::ApiVersions as i16,
                    min_version: 0,
                    max_version: 4,
                    _unknown_tagged_fields: Vec::new(),
                }],
                ..ApiVersionsResponseData::default()
            };
            response_frame(ApiKey::ApiVersions, 3, header.correlation_id, &response)
        }),
    ])
    .await;
    let mut config = ConnectionConfig::default();
    config.security.protocol = kacrab::wire::SecurityProtocol::Ssl;
    config.tls.truststore_certificates = Some(tls.truststore_pem.clone());
    config.tls.keystore_certificate_chain = tls.client_cert_pem.clone();
    config.tls.keystore_key = tls.client_key_pem.clone();
    let client = WireClient::connect_with_brokers(
        config,
        "kacrab-test",
        [BrokerEndpoint::new(7, tls.addr())],
    );
    let request = kacrab_protocol::generated::ApiVersionsRequestData {
        client_software_name: KafkaString::from("kacrab".to_owned()),
        client_software_version: KafkaString::from("0.0.1".to_owned()),
        _unknown_tagged_fields: Vec::new(),
    };

    let response: ApiVersionsResponseData = client
        .send_to_broker(7, ApiKey::ApiVersions, 3, &request)
        .await
        .unwrap();

    assert_eq!(response.api_keys[0].max_version, 4);
    assert_eq!(tls.join().await, 2);
}

#[tokio::test]
async fn wire_client_runs_scram_sha256_and_verifies_server_signature() {
    let broker = MockBroker::serve_many(vec![
        Box::new(api_versions_response),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(
                &mut request,
                request_header_version(ApiKey::SaslHandshake as i16, 1),
            )
            .expect("sasl handshake header");
            let body =
                SaslHandshakeRequestData::read(&mut request, 1).expect("sasl handshake body");
            assert_eq!(body.mechanism.to_string(), "SCRAM-SHA-256");
            let response = SaslHandshakeResponseData {
                error_code: 0,
                mechanisms: vec![KafkaString::from("SCRAM-SHA-256".to_owned())],
                _unknown_tagged_fields: Vec::new(),
            };
            response_frame(ApiKey::SaslHandshake, 1, header.correlation_id, &response)
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(
                &mut request,
                request_header_version(ApiKey::SaslAuthenticate as i16, 2),
            )
            .expect("scram first header");
            let body =
                SaslAuthenticateRequestData::read(&mut request, 2).expect("scram first body");
            let client_first = std::str::from_utf8(body.auth_bytes.as_ref()).expect("utf8");
            assert!(client_first.starts_with("n,,n=alice,r="));
            let client_first_bare = client_first.strip_prefix("n,,").expect("gs2 header");
            let client_nonce =
                scram_attr(client_first_bare, "r").expect("client nonce in first message");
            let server_first = format!(
                "r={client_nonce}SERVER,s={},i=4096",
                general_purpose::STANDARD.encode(b"salt")
            );
            let response = SaslAuthenticateResponseData {
                error_code: 0,
                error_message: None,
                auth_bytes: Bytes::from(server_first),
                session_lifetime_ms: 300_000,
                _unknown_tagged_fields: Vec::new(),
            };
            response_frame(
                ApiKey::SaslAuthenticate,
                2,
                header.correlation_id,
                &response,
            )
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(
                &mut request,
                request_header_version(ApiKey::SaslAuthenticate as i16, 2),
            )
            .expect("scram final header");
            let body =
                SaslAuthenticateRequestData::read(&mut request, 2).expect("scram final body");
            let client_final = std::str::from_utf8(body.auth_bytes.as_ref()).expect("utf8");
            let client_final_without_proof = client_final
                .split(",p=")
                .next()
                .expect("client final proof separator");
            let nonce = scram_attr(client_final_without_proof, "r").expect("nonce");
            let client_first_bare = {
                let client_nonce = nonce.strip_suffix("SERVER").expect("server suffix");
                format!("n=alice,r={client_nonce}")
            };
            let server_first = format!(
                "r={nonce},s={},i=4096",
                general_purpose::STANDARD.encode(b"salt")
            );
            let auth_message =
                format!("{client_first_bare},{server_first},{client_final_without_proof}");
            let (expected_proof, server_signature) =
                scram_sha256_vectors(b"secret", b"salt", 4096, auth_message.as_bytes());
            let proof = scram_attr(client_final, "p").expect("proof");
            assert_eq!(proof, general_purpose::STANDARD.encode(expected_proof));
            let response = SaslAuthenticateResponseData {
                error_code: 0,
                error_message: None,
                auth_bytes: Bytes::from(format!(
                    "v={}",
                    general_purpose::STANDARD.encode(server_signature)
                )),
                session_lifetime_ms: 300_000,
                _unknown_tagged_fields: Vec::new(),
            };
            response_frame(
                ApiKey::SaslAuthenticate,
                2,
                header.correlation_id,
                &response,
            )
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::ApiVersions as i16);
            let response = ApiVersionsResponseData {
                error_code: 0,
                api_keys: vec![ApiVersion {
                    api_key: ApiKey::ApiVersions as i16,
                    min_version: 0,
                    max_version: 4,
                    _unknown_tagged_fields: Vec::new(),
                }],
                ..ApiVersionsResponseData::default()
            };
            response_frame(ApiKey::ApiVersions, 3, header.correlation_id, &response)
        }),
    ])
    .await;
    let mut config = ConnectionConfig::default();
    config.security.protocol = kacrab::wire::SecurityProtocol::SaslPlaintext;
    config.sasl.mechanism = Some(kacrab::wire::SaslMechanism::ScramSha256);
    config.sasl.jaas_config = Some(
        "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"alice\" \
         password=\"secret\";"
            .to_owned(),
    );
    let client = WireClient::connect_with_brokers(
        config,
        "kacrab-test",
        [BrokerEndpoint::new(7, broker.addr())],
    );
    let request = kacrab_protocol::generated::ApiVersionsRequestData {
        client_software_name: KafkaString::from("kacrab".to_owned()),
        client_software_version: KafkaString::from("0.0.1".to_owned()),
        _unknown_tagged_fields: Vec::new(),
    };

    let response: ApiVersionsResponseData = client
        .send_to_broker(7, ApiKey::ApiVersions, 3, &request)
        .await
        .unwrap();

    assert_eq!(response.api_keys[0].max_version, 4);
    assert_eq!(broker.join().await, 5);
}

#[tokio::test]
async fn wire_client_runs_scram_sha512_and_verifies_server_signature() {
    let broker = MockBroker::serve_many(vec![
        Box::new(api_versions_response),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(
                &mut request,
                request_header_version(ApiKey::SaslHandshake as i16, 1),
            )
            .expect("sasl handshake header");
            let body =
                SaslHandshakeRequestData::read(&mut request, 1).expect("sasl handshake body");
            assert_eq!(body.mechanism.to_string(), "SCRAM-SHA-512");
            let response = SaslHandshakeResponseData {
                error_code: 0,
                mechanisms: vec![KafkaString::from("SCRAM-SHA-512".to_owned())],
                _unknown_tagged_fields: Vec::new(),
            };
            response_frame(ApiKey::SaslHandshake, 1, header.correlation_id, &response)
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(
                &mut request,
                request_header_version(ApiKey::SaslAuthenticate as i16, 2),
            )
            .expect("scram first header");
            let body =
                SaslAuthenticateRequestData::read(&mut request, 2).expect("scram first body");
            let client_first = std::str::from_utf8(body.auth_bytes.as_ref()).expect("utf8");
            assert!(client_first.starts_with("n,,n=alice,r="));
            let client_first_bare = client_first.strip_prefix("n,,").expect("gs2 header");
            let client_nonce =
                scram_attr(client_first_bare, "r").expect("client nonce in first message");
            let server_first = format!(
                "r={client_nonce}SERVER,s={},i=4096",
                general_purpose::STANDARD.encode(b"salt")
            );
            let response = SaslAuthenticateResponseData {
                error_code: 0,
                error_message: None,
                auth_bytes: Bytes::from(server_first),
                session_lifetime_ms: 300_000,
                _unknown_tagged_fields: Vec::new(),
            };
            response_frame(
                ApiKey::SaslAuthenticate,
                2,
                header.correlation_id,
                &response,
            )
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(
                &mut request,
                request_header_version(ApiKey::SaslAuthenticate as i16, 2),
            )
            .expect("scram final header");
            let body =
                SaslAuthenticateRequestData::read(&mut request, 2).expect("scram final body");
            let client_final = std::str::from_utf8(body.auth_bytes.as_ref()).expect("utf8");
            let client_final_without_proof = client_final
                .split(",p=")
                .next()
                .expect("client final proof separator");
            let nonce = scram_attr(client_final_without_proof, "r").expect("nonce");
            let client_first_bare = {
                let client_nonce = nonce.strip_suffix("SERVER").expect("server suffix");
                format!("n=alice,r={client_nonce}")
            };
            let server_first = format!(
                "r={nonce},s={},i=4096",
                general_purpose::STANDARD.encode(b"salt")
            );
            let auth_message =
                format!("{client_first_bare},{server_first},{client_final_without_proof}");
            let (expected_proof, server_signature) =
                scram_sha512_vectors(b"secret", b"salt", 4096, auth_message.as_bytes());
            let proof = scram_attr(client_final, "p").expect("proof");
            assert_eq!(proof, general_purpose::STANDARD.encode(expected_proof));
            let response = SaslAuthenticateResponseData {
                error_code: 0,
                error_message: None,
                auth_bytes: Bytes::from(format!(
                    "v={}",
                    general_purpose::STANDARD.encode(server_signature)
                )),
                session_lifetime_ms: 300_000,
                _unknown_tagged_fields: Vec::new(),
            };
            response_frame(
                ApiKey::SaslAuthenticate,
                2,
                header.correlation_id,
                &response,
            )
        }),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::ApiVersions as i16);
            let response = ApiVersionsResponseData {
                error_code: 0,
                api_keys: vec![ApiVersion {
                    api_key: ApiKey::ApiVersions as i16,
                    min_version: 0,
                    max_version: 4,
                    _unknown_tagged_fields: Vec::new(),
                }],
                ..ApiVersionsResponseData::default()
            };
            response_frame(ApiKey::ApiVersions, 3, header.correlation_id, &response)
        }),
    ])
    .await;
    let mut config = ConnectionConfig::default();
    config.security.protocol = kacrab::wire::SecurityProtocol::SaslPlaintext;
    config.sasl.mechanism = Some(kacrab::wire::SaslMechanism::ScramSha512);
    config.sasl.jaas_config = Some(
        "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"alice\" \
         password=\"secret\";"
            .to_owned(),
    );
    let client = WireClient::connect_with_brokers(
        config,
        "kacrab-test",
        [BrokerEndpoint::new(7, broker.addr())],
    );
    let request = kacrab_protocol::generated::ApiVersionsRequestData {
        client_software_name: KafkaString::from("kacrab".to_owned()),
        client_software_version: KafkaString::from("0.0.1".to_owned()),
        _unknown_tagged_fields: Vec::new(),
    };

    let response: ApiVersionsResponseData = client
        .send_to_broker(7, ApiKey::ApiVersions, 3, &request)
        .await
        .unwrap();

    assert_eq!(response.api_keys[0].max_version, 4);
    assert_eq!(broker.join().await, 5);
}

#[tokio::test]
async fn wire_client_reconnects_after_initial_connect_failure() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let client = WireClient::connect_with_brokers(
        ConnectionConfig::default()
            .request_timeout(std::time::Duration::from_millis(500))
            .socket_connection_setup_timeout(std::time::Duration::from_millis(10))
            .reconnect_backoff_initial(std::time::Duration::from_millis(5))
            .reconnect_backoff_max(std::time::Duration::from_millis(20)),
        "kacrab-test",
        [BrokerEndpoint::new(7, addr)],
    );
    let request = kacrab_protocol::generated::ApiVersionsRequestData {
        client_software_name: KafkaString::from("kacrab".to_owned()),
        client_software_version: KafkaString::from("0.0.1".to_owned()),
        _unknown_tagged_fields: Vec::new(),
    };
    let response_task = {
        let client = client.clone();
        let request = request.clone();
        tokio::spawn(async move {
            client
                .send_to_broker::<_, ApiVersionsResponseData>(7, ApiKey::ApiVersions, 3, &request)
                .await
        })
    };

    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    let server = MockBroker::serve_many_on_addr(
        addr,
        vec![
            Box::new(api_versions_response),
            Box::new(|mut request| {
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                let response = ApiVersionsResponseData {
                    error_code: 0,
                    api_keys: vec![ApiVersion {
                        api_key: ApiKey::ApiVersions as i16,
                        min_version: 0,
                        max_version: 7,
                        _unknown_tagged_fields: Vec::new(),
                    }],
                    ..ApiVersionsResponseData::default()
                };
                response_frame(ApiKey::ApiVersions, 3, header.correlation_id, &response)
            }),
        ],
    )
    .await;

    let response = response_task.await.unwrap().unwrap();

    assert_eq!(response.api_keys[0].max_version, 7);
    assert_eq!(server.join().await, 2);
}

#[tokio::test]
async fn wire_client_reconnects_after_broker_closes_connection() {
    let server = MockBroker::serve_reconnecting_api_versions([7, 8]).await;
    let client = WireClient::connect_with_brokers(
        ConnectionConfig::default()
            .request_timeout(std::time::Duration::from_millis(500))
            .reconnect_backoff_initial(std::time::Duration::from_millis(5))
            .reconnect_backoff_max(std::time::Duration::from_millis(20)),
        "kacrab-test",
        [BrokerEndpoint::new(7, server.addr())],
    );
    let request = kacrab_protocol::generated::ApiVersionsRequestData {
        client_software_name: KafkaString::from("kacrab".to_owned()),
        client_software_version: KafkaString::from("0.0.1".to_owned()),
        _unknown_tagged_fields: Vec::new(),
    };

    let first: ApiVersionsResponseData = client
        .send_to_broker(7, ApiKey::ApiVersions, 3, &request)
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    let second: ApiVersionsResponseData = client
        .send_to_broker(7, ApiKey::ApiVersions, 3, &request)
        .await
        .unwrap();

    assert_eq!(first.api_keys[0].max_version, 7);
    assert_eq!(second.api_keys[0].max_version, 8);
    assert_eq!(server.join().await, 4);
}

#[tokio::test]
async fn wire_client_reuses_configured_buffer_pool() {
    let server = MockBroker::serve_many(vec![
        Box::new(api_versions_response),
        Box::new(api_versions_response),
        Box::new(api_versions_response),
    ])
    .await;
    let client = WireClient::connect_with_brokers(
        ConnectionConfig::default()
            .read_buffer_capacity(4096)
            .buffer_pool_capacity(4),
        "kacrab-test",
        [BrokerEndpoint::new(7, server.addr())],
    );
    let request = kacrab_protocol::generated::ApiVersionsRequestData {
        client_software_name: KafkaString::from("kacrab".to_owned()),
        client_software_version: KafkaString::from("0.0.1".to_owned()),
        _unknown_tagged_fields: Vec::new(),
    };

    let _first: ApiVersionsResponseData = client
        .send_to_broker(7, ApiKey::ApiVersions, 3, &request)
        .await
        .unwrap();
    let _second: ApiVersionsResponseData = client
        .send_to_broker(7, ApiKey::ApiVersions, 3, &request)
        .await
        .unwrap();
    let stats = client.buffer_pool_stats();

    assert!(stats.read_acquired >= 3);
    assert!(stats.read_released >= 3);
    assert!(stats.read_reused >= 1);
    assert!(stats.write_acquired >= 2);
    assert!(stats.write_released >= 2);
    assert!(stats.write_reused >= 1);
    assert_eq!(server.join().await, 3);
}

fn api_versions_response(mut request: Bytes) -> BytesMut {
    let header = RequestHeaderData::read(&mut request, 2).expect("request header");
    let response = ApiVersionsResponseData {
        error_code: 0,
        api_keys: vec![
            ApiVersion {
                api_key: ApiKey::ApiVersions as i16,
                min_version: 0,
                max_version: 4,
                _unknown_tagged_fields: Vec::new(),
            },
            ApiVersion {
                api_key: ApiKey::Metadata as i16,
                min_version: 0,
                max_version: 13,
                _unknown_tagged_fields: Vec::new(),
            },
        ],
        ..ApiVersionsResponseData::default()
    };
    response_frame(ApiKey::ApiVersions, 3, header.correlation_id, &response)
}

fn metadata_response(
    leader_id: i32,
    topic: &str,
    brokers: Vec<(i32, String, i32)>,
) -> MetadataResponseData {
    MetadataResponseData {
        brokers: brokers
            .into_iter()
            .map(|(node_id, host, port)| MetadataResponseBroker {
                node_id,
                host: KafkaString::from(host),
                port,
                rack: None,
                _unknown_tagged_fields: Vec::new(),
            })
            .collect(),
        cluster_id: Some(KafkaString::from("cluster-1".to_owned())),
        controller_id: leader_id,
        topics: vec![MetadataResponseTopic {
            error_code: 0,
            name: Some(KafkaString::from(topic.to_owned())),
            partitions: vec![MetadataResponsePartition {
                error_code: 0,
                partition_index: 0,
                leader_id,
                leader_epoch: 3,
                replica_nodes: vec![leader_id],
                isr_nodes: vec![leader_id],
                offline_replicas: Vec::new(),
                _unknown_tagged_fields: Vec::new(),
            }],
            ..MetadataResponseTopic::default()
        }],
        ..MetadataResponseData::default()
    }
}

fn oauthbearer_handlers(
    token: &'static str,
    max_version: i16,
) -> Vec<Box<dyn FnOnce(Bytes) -> BytesMut + Send>> {
    vec![
        Box::new(api_versions_response),
        Box::new(|mut request| {
            let header = RequestHeaderData::read(
                &mut request,
                request_header_version(ApiKey::SaslHandshake as i16, 1),
            )
            .expect("sasl handshake header");
            let body =
                SaslHandshakeRequestData::read(&mut request, 1).expect("sasl handshake body");
            assert_eq!(body.mechanism.to_string(), "OAUTHBEARER");
            let response = SaslHandshakeResponseData {
                error_code: 0,
                mechanisms: vec![KafkaString::from("OAUTHBEARER".to_owned())],
                _unknown_tagged_fields: Vec::new(),
            };
            response_frame(ApiKey::SaslHandshake, 1, header.correlation_id, &response)
        }),
        Box::new(move |mut request| {
            let header = RequestHeaderData::read(
                &mut request,
                request_header_version(ApiKey::SaslAuthenticate as i16, 2),
            )
            .expect("sasl authenticate header");
            let body =
                SaslAuthenticateRequestData::read(&mut request, 2).expect("sasl authenticate body");
            let expected = format!("n,,\x01auth=Bearer {token}\x01\x01");
            assert_eq!(body.auth_bytes.as_ref(), expected.as_bytes());
            let response = SaslAuthenticateResponseData {
                error_code: 0,
                error_message: None,
                auth_bytes: Bytes::new(),
                session_lifetime_ms: 300_000,
                _unknown_tagged_fields: Vec::new(),
            };
            response_frame(
                ApiKey::SaslAuthenticate,
                2,
                header.correlation_id,
                &response,
            )
        }),
        Box::new(move |mut request| {
            let header = RequestHeaderData::read(&mut request, 2).expect("request header");
            assert_eq!(header.request_api_key, ApiKey::ApiVersions as i16);
            let response = ApiVersionsResponseData {
                error_code: 0,
                api_keys: vec![ApiVersion {
                    api_key: ApiKey::ApiVersions as i16,
                    min_version: 0,
                    max_version,
                    _unknown_tagged_fields: Vec::new(),
                }],
                ..ApiVersionsResponseData::default()
            };
            response_frame(ApiKey::ApiVersions, 3, header.correlation_id, &response)
        }),
    ]
}

fn response_frame(
    api_key: ApiKey,
    api_version: i16,
    correlation_id: i32,
    response: &impl ApiVersions,
) -> BytesMut {
    let mut header = BytesMut::new();
    ResponseHeaderData {
        correlation_id,
        _unknown_tagged_fields: Vec::new(),
    }
    .write(
        &mut header,
        response_header_version(api_key as i16, api_version),
    )
    .expect("response header write");

    let mut body = BytesMut::new();
    response.write_api_versions(&mut body, api_version);

    frame::encode_request(&header, &body).expect("response frame")
}

struct MockBroker {
    addr: std::net::SocketAddr,
    join: tokio::task::JoinHandle<usize>,
}

struct MockOAuthServer {
    addr: std::net::SocketAddr,
    join: tokio::task::JoinHandle<usize>,
}

impl MockOAuthServer {
    async fn serve_token(expected_body: &'static str, token: &'static str) -> Self {
        Self::serve_token_with(move |body| assert_eq!(body, expected_body), token).await
    }

    async fn serve_token_with<F>(verify_body: F, token: &'static str) -> Self
    where
        F: FnOnce(&str) + Send + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let join = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let head = read_http_head(&mut socket).await;
            assert!(head.starts_with("POST /token HTTP/1.1"));
            let content_length = http_content_length(&head);
            let mut body = vec![0; content_length];
            let _bytes_read = socket.read_exact(&mut body).await.unwrap();
            let body = std::str::from_utf8(&body).unwrap();
            verify_body(body);
            let response_body =
                format!("{{\"access_token\":\"{token}\",\"token_type\":\"Bearer\"}}");
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: \
                 {}\r\nConnection: close\r\n\r\n{}",
                response_body.len(),
                response_body
            );
            socket.write_all(response.as_bytes()).await.unwrap();
            1
        });
        Self { addr, join }
    }

    fn url(&self, path: &str) -> String {
        format!("http://{}{}", self.addr, path)
    }

    async fn join(self) -> usize {
        self.join.await.unwrap()
    }
}

impl MockBroker {
    async fn serve_many(handlers: Vec<Box<dyn FnOnce(Bytes) -> BytesMut + Send>>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        Self::serve_many_with_listener(listener, addr, handlers)
    }

    async fn serve_many_on_addr(
        addr: std::net::SocketAddr,
        handlers: Vec<Box<dyn FnOnce(Bytes) -> BytesMut + Send>>,
    ) -> Self {
        let listener = TcpListener::bind(addr).await.unwrap();
        Self::serve_many_with_listener(listener, addr, handlers)
    }

    fn serve_many_with_listener(
        listener: TcpListener,
        addr: std::net::SocketAddr,
        handlers: Vec<Box<dyn FnOnce(Bytes) -> BytesMut + Send>>,
    ) -> Self {
        let join = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let handled = handlers.len();
            for handler in handlers {
                let request = read_frame(&mut socket).await;
                let response = handler(request);
                socket.write_all(&response).await.unwrap();
            }
            handled
        });
        Self { addr, join }
    }

    async fn serve_blocking_after_handshake(
        request_seen: tokio::sync::oneshot::Sender<()>,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let join = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let request = read_frame(&mut socket).await;
            let response = api_versions_response(request);
            socket.write_all(&response).await.unwrap();

            let _request = read_frame(&mut socket).await;
            let _ignored = request_seen.send(());
            tokio::time::sleep(std::time::Duration::from_millis(300)).await;
            2
        });
        Self { addr, join }
    }

    async fn serve_pipelined_api_versions(requests: usize, max_version: i16) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let join = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let handshake = read_frame(&mut socket).await;
            let response = api_versions_response(handshake);
            socket.write_all(&response).await.unwrap();

            let mut correlation_ids = Vec::with_capacity(requests);
            for _ in 0..requests {
                let mut request = read_frame(&mut socket).await;
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::ApiVersions as i16);
                correlation_ids.push(header.correlation_id);
            }
            for correlation_id in correlation_ids {
                let response = ApiVersionsResponseData {
                    error_code: 0,
                    api_keys: vec![ApiVersion {
                        api_key: ApiKey::ApiVersions as i16,
                        min_version: 0,
                        max_version,
                        _unknown_tagged_fields: Vec::new(),
                    }],
                    ..ApiVersionsResponseData::default()
                };
                let frame = response_frame(ApiKey::ApiVersions, 3, correlation_id, &response);
                socket.write_all(&frame).await.unwrap();
            }
            requests.saturating_add(1)
        });
        Self { addr, join }
    }

    async fn serve_reconnecting_api_versions(max_versions: [i16; 2]) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let join = tokio::spawn(async move {
            for max_version in max_versions {
                let (mut socket, _) = listener.accept().await.unwrap();
                let handshake = read_frame(&mut socket).await;
                let response = api_versions_response(handshake);
                socket.write_all(&response).await.unwrap();

                let mut request = read_frame(&mut socket).await;
                let header = RequestHeaderData::read(&mut request, 2).expect("request header");
                assert_eq!(header.request_api_key, ApiKey::ApiVersions as i16);
                let response = ApiVersionsResponseData {
                    error_code: 0,
                    api_keys: vec![ApiVersion {
                        api_key: ApiKey::ApiVersions as i16,
                        min_version: 0,
                        max_version,
                        _unknown_tagged_fields: Vec::new(),
                    }],
                    ..ApiVersionsResponseData::default()
                };
                let frame =
                    response_frame(ApiKey::ApiVersions, 3, header.correlation_id, &response);
                socket.write_all(&frame).await.unwrap();
            }
            4
        });
        Self { addr, join }
    }

    const fn addr(&self) -> std::net::SocketAddr {
        self.addr
    }

    async fn join(self) -> usize {
        self.join.await.unwrap()
    }
}

async fn read_http_head(socket: &mut tokio::net::TcpStream) -> String {
    let mut head = Vec::new();
    while !head.ends_with(b"\r\n\r\n") {
        let mut byte = [0_u8; 1];
        let _bytes_read = socket.read_exact(&mut byte).await.unwrap();
        head.extend_from_slice(&byte);
    }
    String::from_utf8(head).unwrap()
}

fn http_content_length(head: &str) -> usize {
    head.lines()
        .find_map(|line| line.strip_prefix("Content-Length: "))
        .unwrap()
        .parse::<usize>()
        .unwrap()
}

struct MockTlsBroker {
    addr: std::net::SocketAddr,
    truststore_path: String,
    truststore_pem: String,
    client_cert_pem: Option<String>,
    client_key_pem: Option<String>,
    join: tokio::task::JoinHandle<usize>,
}

struct MockTlsMaterial {
    truststore_path: String,
    truststore_pem: String,
    client_cert_pem: Option<String>,
    client_key_pem: Option<String>,
}

impl MockTlsBroker {
    async fn serve_many(handlers: Vec<Box<dyn FnOnce(Bytes) -> BytesMut + Send>>) -> Self {
        Self::serve_many_for_subject("127.0.0.1", handlers).await
    }

    async fn serve_many_for_subject(
        subject: &str,
        handlers: Vec<Box<dyn FnOnce(Bytes) -> BytesMut + Send>>,
    ) -> Self {
        let CertifiedKey { cert, key_pair } =
            generate_simple_self_signed([subject.to_owned()]).expect("self-signed cert");
        let truststore_pem = cert.pem();
        let truststore_path = write_temp_cert(truststore_pem.as_bytes());
        let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key_pair.serialize_der()));
        let server_config = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert.der().clone()], key)
            .expect("server cert");
        Self::serve_many_with_config(
            server_config,
            MockTlsMaterial {
                truststore_path,
                truststore_pem,
                client_cert_pem: None,
                client_key_pem: None,
            },
            handlers,
        )
        .await
    }

    async fn serve_many_with_client_auth(
        handlers: Vec<Box<dyn FnOnce(Bytes) -> BytesMut + Send>>,
    ) -> Self {
        let CertifiedKey {
            cert: server_cert,
            key_pair: server_key_pair,
        } = generate_simple_self_signed(["127.0.0.1".to_owned()]).expect("server cert");
        let CertifiedKey {
            cert: client_cert,
            key_pair: client_key_pair,
        } = generate_simple_self_signed(["kacrab-client".to_owned()]).expect("client cert");
        let truststore_pem = server_cert.pem();
        let truststore_path = write_temp_cert(truststore_pem.as_bytes());
        let mut client_roots = RootCertStore::empty();
        client_roots
            .add(client_cert.der().clone())
            .expect("client trust anchor");
        let verifier = WebPkiClientVerifier::builder(client_roots.into())
            .build()
            .expect("client verifier");
        let server_key =
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(server_key_pair.serialize_der()));
        let server_config = ServerConfig::builder()
            .with_client_cert_verifier(verifier)
            .with_single_cert(vec![server_cert.der().clone()], server_key)
            .expect("server cert");
        Self::serve_many_with_config(
            server_config,
            MockTlsMaterial {
                truststore_path,
                truststore_pem,
                client_cert_pem: Some(client_cert.pem()),
                client_key_pem: Some(client_key_pair.serialize_pem()),
            },
            handlers,
        )
        .await
    }

    async fn serve_many_with_config(
        server_config: ServerConfig,
        material: MockTlsMaterial,
        handlers: Vec<Box<dyn FnOnce(Bytes) -> BytesMut + Send>>,
    ) -> Self {
        let acceptor = TlsAcceptor::from(std::sync::Arc::new(server_config));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let join = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut socket = acceptor.accept(socket).await.unwrap();
            let handled = handlers.len();
            for handler in handlers {
                let request = read_frame(&mut socket).await;
                let response = handler(request);
                socket.write_all(&response).await.unwrap();
            }
            handled
        });
        Self {
            addr,
            truststore_path: material.truststore_path,
            truststore_pem: material.truststore_pem,
            client_cert_pem: material.client_cert_pem,
            client_key_pem: material.client_key_pem,
            join,
        }
    }

    const fn addr(&self) -> std::net::SocketAddr {
        self.addr
    }

    async fn join(self) -> usize {
        self.join.await.unwrap()
    }
}

fn write_temp_cert(contents: &[u8]) -> String {
    write_temp_file("ca", contents)
}

fn write_temp_file(kind: &str, contents: &[u8]) -> String {
    let mut path = std::env::temp_dir();
    path.push(format!(
        "kacrab-test-{}-{}-{}.tmp",
        kind,
        std::process::id(),
        unique_test_suffix()
    ));
    std::fs::write(&path, contents).expect("write temp cert");
    path.to_string_lossy().into_owned()
}

fn encrypted_pkcs8_pem(key_pem: &str, password: &str) -> String {
    let (_label, document) =
        SecretDocument::from_pem(key_pem).expect("plain PKCS#8 PEM should parse");
    let private_key =
        PrivateKeyInfoOwned::from_der(document.as_bytes()).expect("plain PKCS#8 DER should parse");
    private_key
        .to_pkcs8_encrypted_pem(password, LineEnding::LF)
        .expect("PKCS#8 encryption should succeed")
        .to_string()
}

fn unique_test_suffix() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT: AtomicU64 = AtomicU64::new(0);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

async fn read_frame<R>(socket: &mut R) -> Bytes
where
    R: tokio::io::AsyncRead + Unpin,
{
    let len = socket.read_i32().await.unwrap();
    let len = usize::try_from(len).unwrap();
    let mut bytes = vec![0; len];
    let _bytes_read = socket.read_exact(&mut bytes).await.unwrap();
    Bytes::from(bytes)
}

trait ApiVersions {
    fn write_api_versions(&self, buf: &mut BytesMut, version: i16);
}

impl ApiVersions for ApiVersionsResponseData {
    fn write_api_versions(&self, buf: &mut BytesMut, version: i16) {
        self.write(buf, version).expect("api versions response");
    }
}

impl ApiVersions for MetadataResponseData {
    fn write_api_versions(&self, buf: &mut BytesMut, version: i16) {
        self.write(buf, version).expect("metadata response");
    }
}

impl ApiVersions for ProduceResponseData {
    fn write_api_versions(&self, buf: &mut BytesMut, version: i16) {
        self.write(buf, version).expect("produce response");
    }
}

impl ApiVersions for SaslHandshakeResponseData {
    fn write_api_versions(&self, buf: &mut BytesMut, version: i16) {
        self.write(buf, version).expect("sasl handshake response");
    }
}

impl ApiVersions for SaslAuthenticateResponseData {
    fn write_api_versions(&self, buf: &mut BytesMut, version: i16) {
        self.write(buf, version)
            .expect("sasl authenticate response");
    }
}

fn scram_attr(value: &str, key: &str) -> Option<String> {
    let prefix = {
        let mut value = String::from(key);
        value.push('=');
        value
    };
    value
        .split(',')
        .find_map(|part| part.strip_prefix(prefix.as_str()).map(ToOwned::to_owned))
}

fn scram_sha256_vectors(
    password: &[u8],
    salt: &[u8],
    iterations: u32,
    auth_message: &[u8],
) -> (Vec<u8>, Vec<u8>) {
    let salted = scram_sha256_salted_password(password, salt, iterations);
    let client_key = hmac_sha256(&salted, b"Client Key");
    let stored_key = Sha256::digest(&client_key);
    let client_signature = hmac_sha256(stored_key.as_slice(), auth_message);
    let mut proof = client_key;
    for (left, right) in proof.iter_mut().zip(client_signature.iter()) {
        *left ^= *right;
    }
    let server_key = hmac_sha256(&salted, b"Server Key");
    let server_signature = hmac_sha256(&server_key, auth_message);
    (proof, server_signature)
}

fn scram_sha512_vectors(
    password: &[u8],
    salt: &[u8],
    iterations: u32,
    auth_message: &[u8],
) -> (Vec<u8>, Vec<u8>) {
    let salted = scram_sha512_salted_password(password, salt, iterations);
    let client_key = hmac_sha512(&salted, b"Client Key");
    let stored_key = Sha512::digest(&client_key);
    let client_signature = hmac_sha512(stored_key.as_slice(), auth_message);
    let mut proof = client_key;
    for (left, right) in proof.iter_mut().zip(client_signature.iter()) {
        *left ^= *right;
    }
    let server_key = hmac_sha512(&salted, b"Server Key");
    let server_signature = hmac_sha512(&server_key, auth_message);
    (proof, server_signature)
}

fn scram_sha256_salted_password(password: &[u8], salt: &[u8], iterations: u32) -> Vec<u8> {
    let mut first_input = Vec::with_capacity(salt.len().saturating_add(4));
    first_input.extend_from_slice(salt);
    first_input.extend_from_slice(&[0, 0, 0, 1]);
    let mut previous = hmac_sha256(password, &first_input);
    let mut output = previous.clone();
    for _ in 1..iterations {
        previous = hmac_sha256(password, &previous);
        for (left, right) in output.iter_mut().zip(previous.iter()) {
            *left ^= *right;
        }
    }
    output
}

fn scram_sha512_salted_password(password: &[u8], salt: &[u8], iterations: u32) -> Vec<u8> {
    let mut first_input = Vec::with_capacity(salt.len().saturating_add(4));
    first_input.extend_from_slice(salt);
    first_input.extend_from_slice(&[0, 0, 0, 1]);
    let mut previous = hmac_sha512(password, &first_input);
    let mut output = previous.clone();
    for _ in 1..iterations {
        previous = hmac_sha512(password, &previous);
        for (left, right) in output.iter_mut().zip(previous.iter()) {
            *left ^= *right;
        }
    }
    output
}

fn hmac_sha256(key: &[u8], message: &[u8]) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(message);
    mac.finalize().into_bytes().to_vec()
}

fn hmac_sha512(key: &[u8], message: &[u8]) -> Vec<u8> {
    let mut mac = Hmac::<Sha512>::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(message);
    mac.finalize().into_bytes().to_vec()
}
