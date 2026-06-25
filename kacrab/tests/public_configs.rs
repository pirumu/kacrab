//! Public typed Kafka client config behavior.

#![allow(
    clippy::missing_assert_message,
    clippy::too_many_lines,
    reason = "Public config coverage uses table-like assertions and long fixture builders."
)]

#[cfg(feature = "producer")]
use kacrab::producer::{
    ProducerCompression,
    internals::{AccumulatorConfig, ProducerIdempotenceConfig, ProducerRuntimeConfig},
};
use kacrab::{
    config::{
        AdminConfig, ByteSize, ClientConfig, ClientKind, ConfigError, ConfigStatus, ConsumerConfig,
        DurationMs, ProducerConfig, Properties, TcpCongestionControl, UnknownKeyPolicy,
        catalog_for,
    },
    wire,
};
#[cfg(feature = "producer")]
use kacrab_protocol::compression::Compression;

#[test]
fn producer_config_builder_keeps_java_keys_and_typed_fields() {
    let config = ProducerConfig::builder()
        .bootstrap_servers("localhost:9092")
        .build()
        .expect("bootstrap.servers is required and present");

    assert_eq!(
        ProducerConfig::BOOTSTRAP_SERVERS_CONFIG,
        "bootstrap.servers"
    );
    assert_eq!(ProducerConfig::CLIENT_ID_CONFIG, "client.id");
    assert_eq!(ProducerConfig::ACKS_CONFIG, "acks");
    assert_eq!(ProducerConfig::BUFFER_MEMORY_CONFIG, "buffer.memory");
    assert_eq!(
        ProducerConfig::DELIVERY_TIMEOUT_MS_CONFIG,
        "delivery.timeout.ms"
    );
    assert_eq!(config.bootstrap_servers.as_slice(), ["localhost:9092"]);
    assert_eq!(config.client_id, "");
    assert_eq!(config.acks, "all");
    assert_eq!(config.buffer_memory, ByteSize::new(33_554_432));
    assert_eq!(config.compression_type, "none");
    assert_eq!(config.retries, 2_147_483_647);
    assert_eq!(config.batch_size, ByteSize::new(16_384));
    assert_eq!(config.delivery_timeout_ms, DurationMs::from_millis(120_000));
    assert_eq!(config.linger_ms, DurationMs::from_millis(5));
    assert!(config.enable_idempotence);
    assert_eq!(ProducerConfig::CONFIG_KEYS[0].client, ClientKind::Producer);
    assert_eq!(
        ProducerConfig::CONFIG_KEYS[0].source,
        "https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_bootstrap.servers"
    );
}

#[test]
#[cfg(feature = "producer")]
fn client_config_sets_java_properties_and_builds_typed_producer_config() {
    let config = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("enable.idempotence", "false")
        .set("linger.ms", "7")
        .set("batch.size", "32768")
        .set("max.in.flight.requests.per.connection", "2");

    let producer = config
        .producer_config()
        .expect("client config should build producer config");

    assert_eq!(producer.bootstrap_servers.as_slice(), ["localhost:9092"]);
    assert!(!producer.enable_idempotence);
    assert_eq!(producer.linger_ms, DurationMs::from_millis(7));
    assert_eq!(producer.batch_size, ByteSize::new(32_768));
    assert_eq!(producer.max_in_flight_requests_per_connection, 2);
}

#[test]
#[cfg(feature = "producer")]
fn producer_config_maps_delivery_batching_and_compression_to_runtime_config() {
    let config = ProducerConfig::builder()
        .bootstrap_servers("localhost:9092")
        .acks("all")
        .compression_type("lz4")
        .compression_lz4_level(11)
        .retries(7)
        .batch_size(ByteSize::new(65_536))
        .buffer_memory(ByteSize::new(8_388_608))
        .linger_ms(DurationMs::from_millis(12))
        .delivery_timeout_ms(DurationMs::from_millis(45_000))
        .request_timeout_ms(DurationMs::from_millis(9_000))
        .partitioner_adaptive_partitioning_enable(false)
        .partitioner_availability_timeout_ms(DurationMs::from_millis(42))
        .max_in_flight_requests_per_connection(3)
        .enable_idempotence(false)
        .build()
        .expect("producer config");

    let runtime = config
        .to_producer_runtime_config()
        .expect("producer runtime config");

    assert_eq!(
        runtime,
        ProducerRuntimeConfig {
            accumulator: AccumulatorConfig {
                batch_size: 65_536,
                linger: core::time::Duration::from_millis(12),
                buffer_memory: 8_388_608,
            },
            acks: -1,
            timeout_ms: 9_000,
            retry_attempts: 7,
            retry_backoff: core::time::Duration::from_millis(100),
            retry_backoff_max: core::time::Duration::from_secs(1),
            delivery_timeout: core::time::Duration::from_secs(45),
            max_block: core::time::Duration::from_mins(1),
            max_in_flight_requests_per_connection: 3,
            max_request_size: 1_048_576,
            enable_metrics_push: true,
            compression: ProducerCompression {
                codec: Compression::Lz4,
                level: Some(11),
            },
            partitioner_ignore_keys: false,
            partitioner_adaptive_partitioning_enable: false,
            partitioner_availability_timeout: core::time::Duration::from_millis(42),
            idempotence: ProducerIdempotenceConfig {
                enabled: false,
                transactional_id: None,
                transaction_timeout_ms: 60_000,
                transaction_two_phase_commit: false,
            },
        }
    );
}

#[test]
#[cfg(feature = "producer")]
fn producer_runtime_config_accepts_idempotence_and_enforces_kafka_invariants() {
    let config = ProducerConfig::builder()
        .bootstrap_servers("localhost:9092")
        .build()
        .expect("producer config");

    let runtime = config
        .to_producer_runtime_config()
        .expect("idempotence is supported");

    assert!(runtime.idempotence.enabled);
    assert_eq!(runtime.acks, -1);
    assert_eq!(runtime.max_in_flight_requests_per_connection, 5);

    let error = ProducerConfig::builder()
        .bootstrap_servers("localhost:9092")
        .acks("1")
        .enable_idempotence(true)
        .build()
        .expect("producer config")
        .to_producer_runtime_config()
        .expect_err("idempotence requires acks=all");

    assert!(matches!(
        error,
        kacrab::producer::ProducerError::InvalidConfig {
            key: ProducerConfig::ACKS_CONFIG,
            ref value,
        } if value == "1"
    ));
}

#[test]
fn consumer_config_builder_exposes_group_id_and_client_id() {
    let config = ConsumerConfig::builder()
        .bootstrap_servers("localhost:9092,localhost:9093")
        .group_id("orders")
        .client_id("orders-worker-1")
        .build()
        .expect("bootstrap.servers is required and present");

    assert_eq!(
        ConsumerConfig::BOOTSTRAP_SERVERS_CONFIG,
        "bootstrap.servers"
    );
    assert_eq!(ConsumerConfig::GROUP_ID_CONFIG, "group.id");
    assert_eq!(ConsumerConfig::CLIENT_ID_CONFIG, "client.id");
    assert_eq!(ConsumerConfig::FETCH_MIN_BYTES_CONFIG, "fetch.min.bytes");
    assert_eq!(
        ConsumerConfig::MAX_POLL_INTERVAL_MS_CONFIG,
        "max.poll.interval.ms"
    );
    assert_eq!(
        config.bootstrap_servers.as_slice(),
        ["localhost:9092", "localhost:9093"]
    );
    assert_eq!(config.group_id, "orders");
    assert_eq!(config.client_id, "orders-worker-1");
    assert!(config.enable_auto_commit);
    assert_eq!(config.fetch_min_bytes, 1);
    assert_eq!(config.fetch_max_bytes, ByteSize::new(52_428_800));
    assert_eq!(
        config.max_poll_interval_ms,
        DurationMs::from_millis(300_000)
    );
    assert_eq!(config.max_poll_records, 500);
    assert_eq!(ConsumerConfig::CONFIG_KEYS[0].client, ClientKind::Consumer);
}

#[test]
fn admin_config_builder_exposes_core_client_keys() {
    let config = AdminConfig::builder()
        .bootstrap_servers("localhost:9092")
        .client_id("admin-1")
        .build()
        .expect("bootstrap.servers is required and present");

    assert_eq!(AdminConfig::BOOTSTRAP_SERVERS_CONFIG, "bootstrap.servers");
    assert_eq!(AdminConfig::CLIENT_ID_CONFIG, "client.id");
    assert_eq!(
        AdminConfig::DEFAULT_API_TIMEOUT_MS_CONFIG,
        "default.api.timeout.ms"
    );
    assert_eq!(config.bootstrap_servers.as_slice(), ["localhost:9092"]);
    assert_eq!(config.client_id, "admin-1");
    assert_eq!(
        config.default_api_timeout_ms,
        DurationMs::from_millis(60_000)
    );
    assert_eq!(config.request_timeout_ms, DurationMs::from_millis(30_000));
    assert_eq!(config.retries, 2_147_483_647);
    assert_eq!(AdminConfig::CONFIG_KEYS[0].client, ClientKind::Admin);
}

#[test]
fn client_configs_require_bootstrap_servers() {
    assert_eq!(
        ProducerConfig::builder()
            .build()
            .expect_err("bootstrap.servers is required"),
        ConfigError::MissingRequired {
            client: ClientKind::Producer,
            key: "bootstrap.servers",
        }
    );
    assert_eq!(
        ConsumerConfig::builder()
            .build()
            .expect_err("bootstrap.servers is required"),
        ConfigError::MissingRequired {
            client: ClientKind::Consumer,
            key: "bootstrap.servers",
        }
    );
    assert_eq!(
        AdminConfig::builder()
            .build()
            .expect_err("bootstrap.servers is required"),
        ConfigError::MissingRequired {
            client: ClientKind::Admin,
            key: "bootstrap.servers",
        }
    );
}

#[test]
fn producer_config_can_be_loaded_from_java_style_properties() {
    let properties = Properties::from_iter([
        ("acks", "1"),
        ("bootstrap.servers", "localhost:9092, localhost:9093"),
        ("client.id", "producer-1"),
    ]);

    let (config, report) = ProducerConfig::from_properties(&properties, UnknownKeyPolicy::Deny)
        .expect("properties contain supported producer keys");

    assert!(report.is_empty());
    assert_eq!(
        config.bootstrap_servers.as_slice(),
        ["localhost:9092", "localhost:9093"]
    );
    assert_eq!(config.client_id, "producer-1");
    assert_eq!(config.acks, "1");
}

#[test]
fn expanded_configs_parse_representative_java_style_values() {
    let producer_properties = Properties::from_iter([
        ("batch.size", "32768"),
        ("bootstrap.servers", "localhost:9092"),
        ("delivery.timeout.ms", "90000"),
        ("enable.idempotence", "false"),
        ("linger.ms", "25"),
        ("max.request.size", "2097152"),
    ]);
    let (producer, _report) =
        ProducerConfig::from_properties(&producer_properties, UnknownKeyPolicy::Deny)
            .expect("producer properties are supported");

    assert_eq!(producer.batch_size, ByteSize::new(32_768));
    assert_eq!(
        producer.delivery_timeout_ms,
        DurationMs::from_millis(90_000)
    );
    assert_eq!(producer.linger_ms, DurationMs::from_millis(25));
    assert_eq!(producer.max_request_size, ByteSize::new(2_097_152));
    assert!(!producer.enable_idempotence);

    let consumer_properties = Properties::from_iter([
        ("bootstrap.servers", "localhost:9092"),
        ("enable.auto.commit", "false"),
        ("fetch.max.bytes", "1048576"),
        ("fetch.min.bytes", "32"),
        ("heartbeat.interval.ms", "2500"),
        ("max.poll.records", "100"),
    ]);
    let (consumer, _report) =
        ConsumerConfig::from_properties(&consumer_properties, UnknownKeyPolicy::Deny)
            .expect("consumer properties are supported");

    assert!(!consumer.enable_auto_commit);
    assert_eq!(consumer.fetch_max_bytes, ByteSize::new(1_048_576));
    assert_eq!(consumer.fetch_min_bytes, 32);
    assert_eq!(
        consumer.heartbeat_interval_ms,
        DurationMs::from_millis(2_500)
    );
    assert_eq!(consumer.max_poll_records, 100);

    let admin_properties = Properties::from_iter([
        ("bootstrap.servers", "localhost:9092"),
        ("default.api.timeout.ms", "30000"),
        ("metadata.max.age.ms", "120000"),
        ("request.timeout.ms", "15000"),
        ("retries", "4"),
    ]);
    let (admin, _report) = AdminConfig::from_properties(&admin_properties, UnknownKeyPolicy::Deny)
        .expect("admin properties are supported");

    assert_eq!(
        admin.default_api_timeout_ms,
        DurationMs::from_millis(30_000)
    );
    assert_eq!(admin.metadata_max_age_ms, DurationMs::from_millis(120_000));
    assert_eq!(admin.request_timeout_ms, DurationMs::from_millis(15_000));
    assert_eq!(admin.retries, 4);
}

#[test]
fn runtime_socket_overlay_keys_are_typed_and_drive_socket_view() {
    let properties = Properties::from_iter([
        ("bootstrap.servers", "localhost:9092"),
        ("socket.read.buffer.capacity.bytes", "262144"),
        ("socket.reuse.address", "false"),
        ("socket.tcp.congestion", "bbr"),
        ("socket.tcp.nodelay", "false"),
        ("socket.tcp.notsent.lowat.bytes", "65536"),
        ("socket.tcp.quickack", "true"),
        ("socket.tcp.user.timeout.ms", "15000"),
        ("broker.queue.capacity", "16"),
        ("buffer.pool.capacity", "128"),
    ]);

    let (producer, report) = ProducerConfig::from_properties(&properties, UnknownKeyPolicy::Deny)
        .expect("runtime socket overlay keys should be accepted");

    assert!(report.is_empty());
    assert!(!producer.socket_tcp_nodelay);
    assert_eq!(producer.socket_tcp_quickack, Some(true));
    assert_eq!(producer.socket_tcp_notsent_lowat_bytes, Some(65_536));
    assert_eq!(
        producer.socket_tcp_user_timeout_ms,
        Some(DurationMs::from_millis(15_000))
    );
    assert_eq!(
        producer.socket_tcp_congestion,
        Some(TcpCongestionControl::Bbr)
    );
    assert!(!producer.socket_reuse_address);
    assert_eq!(producer.socket_read_buffer_capacity_bytes, Some(262_144));
    assert_eq!(producer.broker_queue_capacity, Some(16));
    assert_eq!(producer.buffer_pool_capacity, Some(128));
    assert_eq!(
        ProducerConfig::SOCKET_TCP_QUICKACK_CONFIG,
        "socket.tcp.quickack"
    );
}

#[test]
fn client_config_maps_runtime_socket_overlay_to_wire_connection_config() {
    let producer = ProducerConfig::builder()
        .bootstrap_servers("localhost:9092")
        .send_buffer_bytes(262_144)
        .receive_buffer_bytes(524_288)
        .request_timeout_ms(DurationMs::from_millis(7_000))
        .socket_connection_setup_timeout_ms(DurationMs::from_millis(3_000))
        .socket_connection_setup_timeout_max_ms(DurationMs::from_millis(11_000))
        .socket_tcp_nodelay(false)
        .socket_tcp_notsent_lowat_bytes(Some(65_536))
        .socket_tcp_quickack(Some(true))
        .socket_tcp_user_timeout_ms(Some(DurationMs::from_millis(15_000)))
        .socket_tcp_congestion(Some(TcpCongestionControl::Bbr))
        .socket_reuse_address(false)
        .socket_read_buffer_capacity_bytes(Some(262_144))
        .broker_queue_capacity(Some(16))
        .buffer_pool_capacity(Some(128))
        .max_in_flight_requests_per_connection(4)
        .build()
        .expect("producer config");

    let config = producer.to_connection_config();

    assert_eq!(config.socket.send_buffer_bytes, Some(262_144));
    assert_eq!(config.socket.receive_buffer_bytes, Some(524_288));
    assert_eq!(config.transport, wire::TransportConfig::Plaintext);
    assert_eq!(config.request_timeout, core::time::Duration::from_secs(7));
    assert_eq!(
        config.socket_connection_setup_timeout,
        core::time::Duration::from_secs(3)
    );
    assert_eq!(
        config.socket_connection_setup_timeout_max,
        core::time::Duration::from_secs(11)
    );
    assert!(!config.socket.tcp_nodelay);
    assert_eq!(config.socket.tcp_notsent_lowat_bytes, Some(65_536));
    assert_eq!(config.socket.tcp_quickack, Some(true));
    assert_eq!(
        config.socket.tcp_user_timeout_ms,
        Some(core::time::Duration::from_secs(15))
    );
    assert_eq!(
        config.socket.tcp_congestion,
        Some(wire::TcpCongestionControl::Bbr)
    );
    assert!(!config.socket.reuse_address);
    assert_eq!(config.read_buffer_capacity, Some(262_144));
    assert_eq!(config.max_in_flight_requests_per_connection, 4);
    assert_eq!(config.broker_queue_capacity, 16);
    assert_eq!(config.buffer_pool_capacity, 128);
}

#[test]
fn client_config_maps_java_security_properties_to_wire_connection_config() {
    let producer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("security.protocol", "SASL_SSL")
        .set("ssl.truststore.location", "/tmp/ca.pem")
        .set(
            "ssl.truststore.certificates",
            "-----BEGIN CERTIFICATE-----\n...\n-----END CERTIFICATE-----",
        )
        .set("ssl.truststore.type", "PEM")
        .set("ssl.keystore.location", "/tmp/client.pem")
        .set("ssl.keystore.key", "-----BEGIN PRIVATE KEY-----\n...")
        .set(
            "ssl.keystore.certificate.chain",
            "-----BEGIN CERTIFICATE-----\n...",
        )
        .set("ssl.keystore.type", "PEM")
        .set("ssl.key.password", "secret")
        .set("ssl.endpoint.identification.algorithm", "https")
        .set("sasl.mechanism", "SCRAM-SHA-512")
        .set("sasl.login.connect.timeout.ms", "1500")
        .set("sasl.login.read.timeout.ms", "2500")
        .set("sasl.login.refresh.window.factor", "0.75")
        .set("sasl.login.refresh.window.jitter", "0.10")
        .set("sasl.login.refresh.min.period.seconds", "30")
        .set("sasl.login.refresh.buffer.seconds", "120")
        .set("sasl.login.retry.backoff.ms", "250")
        .set("sasl.login.retry.backoff.max.ms", "2000")
        .set(
            "sasl.jaas.config",
            "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"u\" \
             password=\"p\";",
        )
        .set(
            "sasl.oauthbearer.token.endpoint.url",
            "file:///tmp/token.jwt",
        )
        .set("sasl.oauthbearer.assertion.file", "/tmp/assertion.jwt")
        .set("sasl.oauthbearer.client.credentials.client.id", "client-a")
        .set(
            "sasl.oauthbearer.client.credentials.client.secret",
            "secret-a",
        )
        .set("sasl.oauthbearer.scope", "orders.write")
        .set(
            "sasl.oauthbearer.assertion.private.key.file",
            "/tmp/key.pem",
        )
        .set(
            "sasl.oauthbearer.assertion.private.key.passphrase",
            "key-secret",
        )
        .set(
            "sasl.oauthbearer.assertion.template.file",
            "/tmp/template.json",
        )
        .set("sasl.oauthbearer.assertion.algorithm", "ES256")
        .set("sasl.oauthbearer.assertion.claim.aud", "orders-api")
        .set("sasl.oauthbearer.assertion.claim.iss", "issuer-a")
        .set("sasl.oauthbearer.assertion.claim.sub", "subject-a")
        .set("sasl.oauthbearer.assertion.claim.exp.seconds", "120")
        .set("sasl.oauthbearer.assertion.claim.nbf.seconds", "5")
        .set("sasl.oauthbearer.assertion.claim.jti.include", "true")
        .set(
            "sasl.login.callback.handler.class",
            "com.example.LoginCallback",
        )
        .set(
            "sasl.client.callback.handler.class",
            "com.example.ClientCallback",
        )
        .set("sasl.kerberos.service.name", "kafka")
        .set("sasl.kerberos.kinit.cmd", "/usr/bin/kinit")
        .set("sasl.kerberos.ticket.renew.window.factor", "0.70")
        .set("sasl.kerberos.ticket.renew.jitter", "0.15")
        .set("sasl.kerberos.min.time.before.relogin", "30000")
        .producer_config()
        .expect("security properties should be accepted");

    let config = producer.to_connection_config();

    assert_eq!(producer.security_protocol, "SASL_SSL");
    assert_eq!(producer.sasl_mechanism, "SCRAM-SHA-512");
    assert_eq!(config.security.protocol, wire::SecurityProtocol::SaslSsl);
    assert_eq!(
        config.sasl.mechanism,
        Some(wire::SaslMechanism::ScramSha512)
    );
    assert_java_tls_config(&config);
    assert_java_oauth_config(&config);
}

#[test]
fn consumer_and_admin_configs_map_java_security_properties_to_wire_connection_config() {
    let consumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", "orders")
        .set("security.protocol", "SASL_SSL")
        .set("ssl.truststore.location", "/tmp/ca.pem")
        .set("ssl.truststore.password", "trust-secret")
        .set(
            "ssl.truststore.certificates",
            "-----BEGIN CERTIFICATE-----\n...\n-----END CERTIFICATE-----",
        )
        .set("ssl.truststore.type", "PEM")
        .set("ssl.keystore.location", "/tmp/client.pem")
        .set("ssl.keystore.password", "store-secret")
        .set("ssl.keystore.key", "-----BEGIN PRIVATE KEY-----\n...")
        .set(
            "ssl.keystore.certificate.chain",
            "-----BEGIN CERTIFICATE-----\n...",
        )
        .set("ssl.keystore.type", "PEM")
        .set("ssl.key.password", "secret")
        .set("ssl.endpoint.identification.algorithm", "")
        .set("ssl.protocol", "TLSv1.2")
        .set("ssl.enabled.protocols", "TLSv1.2")
        .set("ssl.cipher.suites", "TLS_AES_128_GCM_SHA256")
        .set("sasl.mechanism", "OAUTHBEARER")
        .set(
            "sasl.jaas.config",
            "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;",
        )
        .set("sasl.login.connect.timeout.ms", "1500")
        .set("sasl.login.read.timeout.ms", "2500")
        .set("sasl.login.refresh.window.factor", "0.75")
        .set("sasl.login.refresh.window.jitter", "0.10")
        .set("sasl.login.refresh.min.period.seconds", "30")
        .set("sasl.login.refresh.buffer.seconds", "120")
        .set("sasl.login.retry.backoff.ms", "250")
        .set("sasl.login.retry.backoff.max.ms", "2000")
        .set(
            "sasl.oauthbearer.token.endpoint.url",
            "file:///tmp/token.jwt",
        )
        .set("sasl.oauthbearer.assertion.file", "/tmp/assertion.jwt")
        .set("sasl.oauthbearer.client.credentials.client.id", "client-a")
        .set(
            "sasl.oauthbearer.client.credentials.client.secret",
            "secret-a",
        )
        .set("sasl.oauthbearer.scope", "orders.read")
        .set(
            "sasl.oauthbearer.assertion.private.key.file",
            "/tmp/key.pem",
        )
        .set(
            "sasl.oauthbearer.assertion.private.key.passphrase",
            "key-secret",
        )
        .set(
            "sasl.oauthbearer.assertion.template.file",
            "/tmp/template.json",
        )
        .set("sasl.oauthbearer.assertion.algorithm", "ES256")
        .set("sasl.oauthbearer.assertion.claim.aud", "orders-api")
        .set("sasl.oauthbearer.assertion.claim.iss", "issuer-a")
        .set("sasl.oauthbearer.assertion.claim.sub", "subject-a")
        .set("sasl.oauthbearer.assertion.claim.exp.seconds", "120")
        .set("sasl.oauthbearer.assertion.claim.nbf.seconds", "5")
        .set("sasl.oauthbearer.assertion.claim.jti.include", "true")
        .set(
            "sasl.login.callback.handler.class",
            "com.example.LoginCallback",
        )
        .set(
            "sasl.client.callback.handler.class",
            "com.example.ClientCallback",
        )
        .set("sasl.kerberos.service.name", "kafka")
        .set("sasl.kerberos.kinit.cmd", "/usr/bin/kinit")
        .set("sasl.kerberos.ticket.renew.window.factor", "0.70")
        .set("sasl.kerberos.ticket.renew.jitter", "0.15")
        .set("sasl.kerberos.min.time.before.relogin", "30000")
        .consumer_config()
        .expect("consumer security properties should be accepted");
    let admin = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("security.protocol", "SASL_SSL")
        .set("ssl.truststore.location", "/tmp/ca.pem")
        .set("ssl.truststore.password", "trust-secret")
        .set(
            "ssl.truststore.certificates",
            "-----BEGIN CERTIFICATE-----\n...\n-----END CERTIFICATE-----",
        )
        .set("ssl.truststore.type", "PEM")
        .set("ssl.keystore.location", "/tmp/client.pem")
        .set("ssl.keystore.password", "store-secret")
        .set("ssl.keystore.key", "-----BEGIN PRIVATE KEY-----\n...")
        .set(
            "ssl.keystore.certificate.chain",
            "-----BEGIN CERTIFICATE-----\n...",
        )
        .set("ssl.keystore.type", "PEM")
        .set("ssl.key.password", "secret")
        .set("ssl.endpoint.identification.algorithm", "")
        .set("ssl.protocol", "TLSv1.2")
        .set("ssl.enabled.protocols", "TLSv1.2")
        .set("ssl.cipher.suites", "TLS_AES_128_GCM_SHA256")
        .set("sasl.mechanism", "OAUTHBEARER")
        .set(
            "sasl.jaas.config",
            "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;",
        )
        .set("sasl.login.connect.timeout.ms", "1500")
        .set("sasl.login.read.timeout.ms", "2500")
        .set("sasl.login.refresh.window.factor", "0.75")
        .set("sasl.login.refresh.window.jitter", "0.10")
        .set("sasl.login.refresh.min.period.seconds", "30")
        .set("sasl.login.refresh.buffer.seconds", "120")
        .set("sasl.login.retry.backoff.ms", "250")
        .set("sasl.login.retry.backoff.max.ms", "2000")
        .set(
            "sasl.oauthbearer.token.endpoint.url",
            "file:///tmp/token.jwt",
        )
        .set("sasl.oauthbearer.assertion.file", "/tmp/assertion.jwt")
        .set("sasl.oauthbearer.client.credentials.client.id", "client-a")
        .set(
            "sasl.oauthbearer.client.credentials.client.secret",
            "secret-a",
        )
        .set("sasl.oauthbearer.scope", "admin.read")
        .set(
            "sasl.oauthbearer.assertion.private.key.file",
            "/tmp/key.pem",
        )
        .set(
            "sasl.oauthbearer.assertion.private.key.passphrase",
            "key-secret",
        )
        .set(
            "sasl.oauthbearer.assertion.template.file",
            "/tmp/template.json",
        )
        .set("sasl.oauthbearer.assertion.algorithm", "ES256")
        .set("sasl.oauthbearer.assertion.claim.aud", "orders-api")
        .set("sasl.oauthbearer.assertion.claim.iss", "issuer-a")
        .set("sasl.oauthbearer.assertion.claim.sub", "subject-a")
        .set("sasl.oauthbearer.assertion.claim.exp.seconds", "120")
        .set("sasl.oauthbearer.assertion.claim.nbf.seconds", "5")
        .set("sasl.oauthbearer.assertion.claim.jti.include", "true")
        .set(
            "sasl.login.callback.handler.class",
            "com.example.LoginCallback",
        )
        .set(
            "sasl.client.callback.handler.class",
            "com.example.ClientCallback",
        )
        .set("sasl.kerberos.service.name", "kafka")
        .set("sasl.kerberos.kinit.cmd", "/usr/bin/kinit")
        .set("sasl.kerberos.ticket.renew.window.factor", "0.70")
        .set("sasl.kerberos.ticket.renew.jitter", "0.15")
        .set("sasl.kerberos.min.time.before.relogin", "30000")
        .admin_config()
        .expect("admin security properties should be accepted");

    let consumer_connection = consumer.to_connection_config();
    let admin_connection = admin.to_connection_config();

    assert_eq!(
        consumer_connection.security.protocol,
        wire::SecurityProtocol::SaslSsl
    );
    assert_eq!(
        admin_connection.security.protocol,
        wire::SecurityProtocol::SaslSsl
    );
    assert_eq!(
        consumer_connection.sasl.mechanism,
        Some(wire::SaslMechanism::OAuthBearer)
    );
    assert_eq!(
        admin_connection.sasl.mechanism,
        Some(wire::SaslMechanism::OAuthBearer)
    );
    assert_eq!(
        consumer_connection.tls.truststore_location.as_deref(),
        Some("/tmp/ca.pem")
    );
    assert_eq!(
        admin_connection.tls.truststore_location.as_deref(),
        Some("/tmp/ca.pem")
    );
    assert_eq!(
        consumer_connection.tls.truststore_password.as_deref(),
        Some("trust-secret")
    );
    assert_eq!(
        admin_connection.tls.keystore_password.as_deref(),
        Some("store-secret")
    );
    assert_eq!(
        consumer_connection.tls.keystore_location.as_deref(),
        Some("/tmp/client.pem")
    );
    assert_eq!(admin_connection.tls.key_password.as_deref(), Some("secret"));
    assert_eq!(
        consumer_connection
            .tls
            .endpoint_identification_algorithm
            .as_deref(),
        Some("")
    );
    assert_eq!(admin_connection.tls.protocol, "TLSv1.2");
    assert_eq!(
        consumer_connection.sasl.oauthbearer_scope.as_deref(),
        Some("orders.read")
    );
    assert_eq!(
        admin_connection.sasl.oauthbearer_scope.as_deref(),
        Some("admin.read")
    );
    assert_eq!(
        consumer_connection
            .sasl
            .oauthbearer_assertion_file
            .as_deref(),
        Some("/tmp/assertion.jwt")
    );
    assert_eq!(
        admin_connection.sasl.oauthbearer_client_id.as_deref(),
        Some("client-a")
    );
    assert_eq!(
        consumer_connection
            .sasl
            .oauthbearer_assertion_private_key_file
            .as_deref(),
        Some("/tmp/key.pem")
    );
    assert_eq!(
        admin_connection.sasl.oauthbearer_assertion_algorithm,
        "ES256"
    );
    assert!(
        consumer_connection
            .sasl
            .oauthbearer_assertion_claim_jti_include
    );
    assert_eq!(
        admin_connection
            .sasl
            .login_callback_handler_class
            .as_deref(),
        Some("com.example.LoginCallback")
    );
    assert_eq!(
        consumer_connection.sasl.kerberos_service_name.as_deref(),
        Some("kafka")
    );
    assert_eq!(
        admin_connection.sasl.login_connect_timeout,
        Some(std::time::Duration::from_millis(1500))
    );
    assert!((consumer_connection.sasl.login_refresh_window_jitter - 0.10).abs() < f64::EPSILON);
}

fn assert_java_tls_config(config: &wire::ConnectionConfig) {
    assert_eq!(
        config.tls.truststore_location.as_deref(),
        Some("/tmp/ca.pem"),
        "truststore location should map to wire TLS config"
    );
    assert_eq!(
        config.tls.truststore_certificates.as_deref(),
        Some("-----BEGIN CERTIFICATE-----\n...\n-----END CERTIFICATE-----"),
        "inline truststore certificates should map to wire TLS config"
    );
    assert_eq!(
        config.tls.truststore_type.as_deref(),
        Some("PEM"),
        "truststore type should map to wire TLS config"
    );
    assert_eq!(
        config.tls.keystore_location.as_deref(),
        Some("/tmp/client.pem"),
        "keystore location should map to wire TLS config"
    );
    assert_eq!(
        config.tls.keystore_key.as_deref(),
        Some("-----BEGIN PRIVATE KEY-----\n..."),
        "inline keystore key should map to wire TLS config"
    );
    assert_eq!(
        config.tls.keystore_certificate_chain.as_deref(),
        Some("-----BEGIN CERTIFICATE-----\n..."),
        "inline certificate chain should map to wire TLS config"
    );
    assert_eq!(
        config.tls.keystore_type.as_deref(),
        Some("PEM"),
        "keystore type should map to wire TLS config"
    );
    assert_eq!(
        config.tls.key_password.as_deref(),
        Some("secret"),
        "key password should map to wire TLS config"
    );
    assert_eq!(
        config.tls.endpoint_identification_algorithm.as_deref(),
        Some("https"),
        "endpoint identification algorithm should map to wire TLS config"
    );
}

fn assert_java_oauth_config(config: &wire::ConnectionConfig) {
    assert_eq!(
        config.sasl.oauthbearer_token_endpoint_url.as_deref(),
        Some("file:///tmp/token.jwt"),
        "OAuth token endpoint should map to wire SASL config"
    );
    assert_eq!(
        config.sasl.oauthbearer_assertion_file.as_deref(),
        Some("/tmp/assertion.jwt"),
        "OAuth assertion file should map to wire SASL config"
    );
    assert_eq!(
        config.sasl.oauthbearer_client_id.as_deref(),
        Some("client-a"),
        "OAuth client id should map to wire SASL config"
    );
    assert_eq!(
        config.sasl.oauthbearer_client_secret.as_deref(),
        Some("secret-a"),
        "OAuth client secret should map to wire SASL config"
    );
    assert_eq!(
        config.sasl.oauthbearer_scope.as_deref(),
        Some("orders.write"),
        "OAuth scope should map to wire SASL config"
    );
    assert_eq!(
        config
            .sasl
            .oauthbearer_assertion_private_key_file
            .as_deref(),
        Some("/tmp/key.pem"),
        "OAuth assertion private key file should map to wire SASL config"
    );
    assert_eq!(
        config
            .sasl
            .oauthbearer_assertion_private_key_passphrase
            .as_deref(),
        Some("key-secret"),
        "OAuth assertion private key passphrase should map to wire SASL config"
    );
    assert_eq!(
        config.sasl.oauthbearer_assertion_template_file.as_deref(),
        Some("/tmp/template.json"),
        "OAuth assertion template should map to wire SASL config"
    );
    assert_eq!(config.sasl.oauthbearer_assertion_algorithm, "ES256");
    assert_eq!(
        config.sasl.oauthbearer_assertion_claim_aud.as_deref(),
        Some("orders-api")
    );
    assert_eq!(
        config.sasl.oauthbearer_assertion_claim_iss.as_deref(),
        Some("issuer-a")
    );
    assert_eq!(
        config.sasl.oauthbearer_assertion_claim_sub.as_deref(),
        Some("subject-a")
    );
    assert_eq!(
        config.sasl.oauthbearer_assertion_claim_exp,
        std::time::Duration::from_mins(2)
    );
    assert_eq!(
        config.sasl.oauthbearer_assertion_claim_nbf,
        std::time::Duration::from_secs(5)
    );
    assert!(config.sasl.oauthbearer_assertion_claim_jti_include);
    assert_eq!(
        config.sasl.login_callback_handler_class.as_deref(),
        Some("com.example.LoginCallback")
    );
    assert_eq!(
        config.sasl.client_callback_handler_class.as_deref(),
        Some("com.example.ClientCallback")
    );
    assert_eq!(config.sasl.kerberos_service_name.as_deref(), Some("kafka"));
    assert_eq!(
        config.sasl.kerberos_kinit_cmd.as_deref(),
        Some("/usr/bin/kinit")
    );
    assert!((config.sasl.kerberos_ticket_renew_window_factor - 0.70).abs() < f64::EPSILON);
    assert!((config.sasl.kerberos_ticket_renew_jitter - 0.15).abs() < f64::EPSILON);
    assert_eq!(
        config.sasl.kerberos_min_time_before_relogin,
        std::time::Duration::from_secs(30)
    );
    assert_eq!(
        config.sasl.login_connect_timeout,
        Some(std::time::Duration::from_millis(1500)),
        "OAuth login connect timeout should map to wire SASL config"
    );
    assert_eq!(
        config.sasl.login_read_timeout,
        Some(std::time::Duration::from_millis(2500)),
        "OAuth login read timeout should map to wire SASL config"
    );
    assert!(
        (config.sasl.login_refresh_window_factor - 0.75).abs() < f64::EPSILON,
        "OAuth refresh window factor should map to wire SASL config"
    );
    assert!(
        (config.sasl.login_refresh_window_jitter - 0.10).abs() < f64::EPSILON,
        "OAuth refresh window jitter should map to wire SASL config"
    );
    assert_eq!(
        config.sasl.login_refresh_min_period,
        std::time::Duration::from_secs(30),
        "OAuth refresh min period should map to wire SASL config"
    );
    assert_eq!(
        config.sasl.login_refresh_buffer,
        std::time::Duration::from_mins(2),
        "OAuth refresh buffer should map to wire SASL config"
    );
    assert_eq!(
        config.sasl.login_retry_backoff,
        std::time::Duration::from_millis(250),
        "OAuth retry backoff should map to wire SASL config"
    );
    assert_eq!(
        config.sasl.login_retry_backoff_max,
        std::time::Duration::from_secs(2),
        "OAuth retry max backoff should map to wire SASL config"
    );
}

#[test]
fn wire_security_protocol_parses_java_values() {
    assert_eq!(
        wire::SecurityProtocol::parse("PLAINTEXT").expect("plaintext"),
        wire::SecurityProtocol::Plaintext
    );
    assert_eq!(
        wire::SecurityProtocol::parse("SSL").expect("ssl"),
        wire::SecurityProtocol::Ssl
    );
    assert_eq!(
        wire::SecurityProtocol::parse("SASL_PLAINTEXT").expect("sasl plaintext"),
        wire::SecurityProtocol::SaslPlaintext
    );
    assert_eq!(
        wire::SecurityProtocol::parse("SASL_SSL").expect("sasl ssl"),
        wire::SecurityProtocol::SaslSsl
    );
    assert!(wire::SecurityProtocol::parse("SASL").is_err());
}

#[test]
fn wire_sasl_mechanism_parses_java_values() {
    assert_eq!(
        wire::SaslMechanism::parse("PLAIN").expect("plain"),
        wire::SaslMechanism::Plain
    );
    assert_eq!(
        wire::SaslMechanism::parse("SCRAM-SHA-256").expect("scram 256"),
        wire::SaslMechanism::ScramSha256
    );
    assert_eq!(
        wire::SaslMechanism::parse("SCRAM-SHA-512").expect("scram 512"),
        wire::SaslMechanism::ScramSha512
    );
    assert_eq!(
        wire::SaslMechanism::parse("OAUTHBEARER").expect("oauth"),
        wire::SaslMechanism::OAuthBearer
    );
    assert_eq!(
        wire::SaslMechanism::parse("GSSAPI").expect("gssapi"),
        wire::SaslMechanism::Gssapi
    );
    assert!(wire::SaslMechanism::parse("ANONYMOUS").is_err());
}

#[test]
fn config_from_properties_reuses_catalog_validation() {
    let properties = Properties::from_iter([
        ("bootstrap.servers", "localhost:9092"),
        (
            "key.serializer",
            "org.apache.kafka.common.serialization.StringSerializer",
        ),
    ]);

    assert!(matches!(
        ProducerConfig::from_properties(&properties, UnknownKeyPolicy::Deny),
        Err(ConfigError::JavaOnly {
            client: ClientKind::Producer,
            key,
            ..
        }) if key == "key.serializer"
    ));
}

#[test]
fn config_from_properties_rejects_cataloged_keys_not_in_typed_schema() {
    let properties = Properties::from_iter([
        ("bootstrap.servers", "localhost:9092"),
        ("metrics.num.samples", "3"),
    ]);

    assert!(matches!(
        ProducerConfig::from_properties(&properties, UnknownKeyPolicy::Deny),
        Err(ConfigError::UnsupportedKey {
            client: ClientKind::Producer,
            key,
        }) if key == "metrics.num.samples"
    ));
}

#[test]
fn typed_configs_cover_every_native_catalog_key() {
    assert_native_keys_are_typed(ClientKind::Producer, ProducerConfig::CONFIG_KEYS);
    assert_native_keys_are_typed(ClientKind::Consumer, ConsumerConfig::CONFIG_KEYS);
    assert_native_keys_are_typed(ClientKind::Admin, AdminConfig::CONFIG_KEYS);
}

fn assert_native_keys_are_typed(client: ClientKind, typed_keys: &[kacrab::config::ConfigEntry]) {
    for entry in catalog_for(client) {
        if entry.status == ConfigStatus::Native {
            assert!(
                typed_keys.iter().any(|typed| typed.key == entry.key),
                "{client:?} native config key `{}` is not exposed in typed config",
                entry.key
            );
        }
    }
}
