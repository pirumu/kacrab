//! Public typed Kafka client configurations.
#![expect(
    clippy::large_stack_arrays,
    reason = "kafka_config! emits static config metadata arrays; they are not producer hot-path \
              data"
)]

use std::string::String;

use kacrab_macros::kafka_config;

use super::{ByteSize, ConfigList, DurationMs, TcpCongestionControl};

kafka_config! {
    #[client(Producer)]
    pub ProducerConfig {
        #[key("bootstrap.servers")]
        #[required]
        #[kafka_type("list")]
        #[kafka_default("(none)")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_bootstrap.servers")]
        #[comment("Initial Kafka broker list used to discover the full cluster.")]
        bootstrap_servers: ConfigList,

        #[key("client.id")]
        #[default(String::new())]
        #[kafka_type("string")]
        #[kafka_default("\"\"")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_client.id")]
        #[comment("Logical client name sent to brokers for quota, logging, and metrics attribution.")]
        client_id: String,

        #[key("acks")]
        #[default(String::from("all"))]
        #[kafka_type("string")]
        #[kafka_default("all")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_acks")]
        #[comment("Producer acknowledgement durability mode; kept as Kafka-compatible text until producer delivery semantics are implemented.")]
        acks: String,

        #[key("buffer.memory")]
        #[default(ByteSize::new(33_554_432))]
        #[kafka_type("long")]
        #[kafka_default("33554432")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_buffer.memory")]
        #[comment("Total producer memory available for buffering records waiting to be sent.")]
        buffer_memory: ByteSize,

        #[key("compression.type")]
        #[default(String::from("none"))]
        #[kafka_type("string")]
        #[kafka_default("none")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_compression.type")]
        #[comment("Compression codec used for producer batches.")]
        compression_type: String,

        #[key("retries")]
        #[default(2_147_483_647_i32)]
        #[kafka_type("int")]
        #[kafka_default("2147483647")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_retries")]
        #[comment("Maximum number of producer send retries before delivery timeout wins.")]
        retries: i32,

        #[key("batch.size")]
        #[default(ByteSize::new(16_384))]
        #[kafka_type("int")]
        #[kafka_default("16384")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_batch.size")]
        #[comment("Upper bound for one producer batch in bytes.")]
        batch_size: ByteSize,

        #[key("client.dns.lookup")]
        #[default(String::from("use_all_dns_ips"))]
        #[kafka_type("string")]
        #[kafka_default("use_all_dns_ips")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_client.dns.lookup")]
        #[comment("Kafka-compatible DNS lookup strategy for bootstrap and broker hostnames.")]
        client_dns_lookup: String,

        #[key("compression.gzip.level")]
        #[default(-1_i32)]
        #[kafka_type("int")]
        #[kafka_default("-1")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_compression.gzip.level")]
        #[comment("Compression level used when gzip compression is selected.")]
        compression_gzip_level: i32,

        #[key("compression.lz4.level")]
        #[default(9_i32)]
        #[kafka_type("int")]
        #[kafka_default("9")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_compression.lz4.level")]
        #[comment("Compression level used when lz4 compression is selected.")]
        compression_lz4_level: i32,

        #[key("compression.zstd.level")]
        #[default(3_i32)]
        #[kafka_type("int")]
        #[kafka_default("3")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_compression.zstd.level")]
        #[comment("Compression level used when zstd compression is selected.")]
        compression_zstd_level: i32,

        #[key("connections.max.idle.ms")]
        #[default(DurationMs::from_millis(540_000))]
        #[kafka_type("long")]
        #[kafka_default("540000 (9 minutes)")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_connections.max.idle.ms")]
        #[comment("Idle connection lifetime before the client closes a broker connection.")]
        connections_max_idle_ms: DurationMs,

        #[key("delivery.timeout.ms")]
        #[default(DurationMs::from_millis(120_000))]
        #[kafka_type("int")]
        #[kafka_default("120000 (2 minutes)")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_delivery.timeout.ms")]
        #[comment("Upper bound on total time to report producer send success or failure.")]
        delivery_timeout_ms: DurationMs,

        #[key("linger.ms")]
        #[default(DurationMs::from_millis(5))]
        #[kafka_type("long")]
        #[kafka_default("5")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_linger.ms")]
        #[comment("Maximum artificial batching delay before sending a non-full batch.")]
        linger_ms: DurationMs,

        #[key("max.block.ms")]
        #[default(DurationMs::from_millis(60_000))]
        #[kafka_type("long")]
        #[kafka_default("60000 (1 minute)")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_max.block.ms")]
        #[comment("Maximum time producer APIs may block while waiting for metadata or buffer memory.")]
        max_block_ms: DurationMs,

        #[key("max.request.size")]
        #[default(ByteSize::new(1_048_576))]
        #[kafka_type("int")]
        #[kafka_default("1048576")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_max.request.size")]
        #[comment("Maximum producer request size in bytes.")]
        max_request_size: ByteSize,

        #[key("reconnect.backoff.ms")]
        #[default(DurationMs::from_millis(50))]
        #[kafka_type("long")]
        #[kafka_default("50")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_reconnect.backoff.ms")]
        #[comment("Initial reconnect backoff for broker TCP connections.")]
        reconnect_backoff_ms: DurationMs,

        #[key("reconnect.backoff.max.ms")]
        #[default(DurationMs::from_millis(1_000))]
        #[kafka_type("long")]
        #[kafka_default("1000")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_reconnect.backoff.max.ms")]
        #[comment("Maximum reconnect backoff for broker TCP connections.")]
        reconnect_backoff_max_ms: DurationMs,

        #[key("retry.backoff.ms")]
        #[default(DurationMs::from_millis(100))]
        #[kafka_type("long")]
        #[kafka_default("100")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_retry.backoff.ms")]
        #[comment("Initial producer retry backoff for retriable produce and transaction-control errors.")]
        retry_backoff_ms: DurationMs,

        #[key("retry.backoff.max.ms")]
        #[default(DurationMs::from_millis(1_000))]
        #[kafka_type("long")]
        #[kafka_default("1000")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_retry.backoff.max.ms")]
        #[comment("Maximum producer retry backoff for retriable produce and transaction-control errors.")]
        retry_backoff_max_ms: DurationMs,

        #[key("partitioner.ignore.keys")]
        #[default(false)]
        #[kafka_type("boolean")]
        #[kafka_default("false")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_partitioner.ignore.keys")]
        #[comment("Whether the producer partitioner should ignore record keys.")]
        partitioner_ignore_keys: bool,

        #[key("partitioner.adaptive.partitioning.enable")]
        #[default(true)]
        #[kafka_type("boolean")]
        #[kafka_default("true")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_partitioner.adaptive.partitioning.enable")]
        #[comment("Whether sticky partition switching adapts to observed broker/partition queue load.")]
        partitioner_adaptive_partitioning_enable: bool,

        #[key("partitioner.availability.timeout.ms")]
        #[default(DurationMs::from_millis(0))]
        #[kafka_type("long")]
        #[kafka_default("0")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_partitioner.availability.timeout.ms")]
        #[comment("How long a leader may be unable to drain produce data before adaptive sticky temporarily excludes its partitions; zero disables exclusion.")]
        partitioner_availability_timeout_ms: DurationMs,

        #[key("receive.buffer.bytes")]
        #[default(32_768_i32)]
        #[kafka_type("int")]
        #[kafka_default("32768 (32 kibibytes)")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_receive.buffer.bytes")]
        #[comment("Socket receive buffer size for producer network connections.")]
        receive_buffer_bytes: i32,

        #[key("request.timeout.ms")]
        #[default(DurationMs::from_millis(30_000))]
        #[kafka_type("int")]
        #[kafka_default("30000 (30 seconds)")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_request.timeout.ms")]
        #[comment("Maximum time the producer waits for a broker response.")]
        request_timeout_ms: DurationMs,

        #[key("send.buffer.bytes")]
        #[default(131_072_i32)]
        #[kafka_type("int")]
        #[kafka_default("131072 (128 kibibytes)")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_send.buffer.bytes")]
        #[comment("Socket send buffer size for producer network connections.")]
        send_buffer_bytes: i32,

        #[key("socket.connection.setup.timeout.max.ms")]
        #[default(DurationMs::from_millis(30_000))]
        #[kafka_type("long")]
        #[kafka_default("30000 (30 seconds)")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_socket.connection.setup.timeout.max.ms")]
        #[comment("Maximum socket connection setup timeout after exponential backoff.")]
        socket_connection_setup_timeout_max_ms: DurationMs,

        #[key("socket.connection.setup.timeout.ms")]
        #[default(DurationMs::from_millis(10_000))]
        #[kafka_type("long")]
        #[kafka_default("10000 (10 seconds)")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_socket.connection.setup.timeout.ms")]
        #[comment("Initial socket connection setup timeout.")]
        socket_connection_setup_timeout_ms: DurationMs,

        #[key("enable.idempotence")]
        #[default(true)]
        #[kafka_type("boolean")]
        #[kafka_default("true")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_enable.idempotence")]
        #[comment("Whether the producer should avoid duplicate writes when retrying sends.")]
        enable_idempotence: bool,

        #[key("enable.metrics.push")]
        #[default(true)]
        #[kafka_type("boolean")]
        #[kafka_default("true")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_enable.metrics.push")]
        #[comment("Whether producer client metrics may be pushed to the Kafka cluster.")]
        enable_metrics_push: bool,

        #[key("max.in.flight.requests.per.connection")]
        #[default(5_i32)]
        #[kafka_type("int")]
        #[kafka_default("5")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_max.in.flight.requests.per.connection")]
        #[comment("Maximum unacknowledged producer requests per broker connection.")]
        max_in_flight_requests_per_connection: i32,

        #[key("metadata.max.age.ms")]
        #[default(DurationMs::from_millis(300_000))]
        #[kafka_type("long")]
        #[kafka_default("300000 (5 minutes)")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_metadata.max.age.ms")]
        #[comment("Maximum metadata cache age before forced refresh.")]
        metadata_max_age_ms: DurationMs,

        #[key("metadata.max.idle.ms")]
        #[default(DurationMs::from_millis(300_000))]
        #[kafka_type("long")]
        #[kafka_default("300000 (5 minutes)")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_metadata.max.idle.ms")]
        #[comment("Maximum idle time before producer topic metadata is forgotten.")]
        metadata_max_idle_ms: DurationMs,

        #[key("metadata.recovery.rebootstrap.trigger.ms")]
        #[default(DurationMs::from_millis(300_000))]
        #[kafka_type("long")]
        #[kafka_default("300000 (5 minutes)")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_metadata.recovery.rebootstrap.trigger.ms")]
        #[comment("Time without usable metadata before rebootstrap recovery may trigger.")]
        metadata_recovery_rebootstrap_trigger_ms: DurationMs,

        #[key("security.protocol")]
        #[default(String::from("PLAINTEXT"))]
        #[kafka_type("string")]
        #[kafka_default("PLAINTEXT")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_security.protocol")]
        #[comment("Kafka-compatible broker security protocol: PLAINTEXT, SSL, SASL_PLAINTEXT, or SASL_SSL.")]
        security_protocol: String,

        #[key("ssl.truststore.location")]
        #[default(None)]
        #[kafka_type("string")]
        #[kafka_default("null")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_ssl.truststore.location")]
        #[feature("tls-rustls")]
        #[comment("Trust material location used by TLS broker connections.")]
        ssl_truststore_location: Option<String>,

        #[key("ssl.truststore.password")]
        #[default(None)]
        #[kafka_type("password")]
        #[kafka_default("null")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_ssl.truststore.password")]
        #[feature("tls-rustls")]
        #[comment("Password for configured TLS trust material when the backend format requires it.")]
        ssl_truststore_password: Option<String>,

        #[key("ssl.truststore.certificates")]
        #[default(None)]
        #[kafka_type("password")]
        #[kafka_default("null")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_ssl.truststore.certificates")]
        #[feature("tls-rustls")]
        #[comment("Inline PEM trusted certificates used by TLS broker connections.")]
        ssl_truststore_certificates: Option<String>,

        #[key("ssl.truststore.type")]
        #[default(None)]
        #[kafka_type("string")]
        #[kafka_default("JKS")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_ssl.truststore.type")]
        #[feature("tls-rustls")]
        #[comment("Truststore format; rustls backend supports PEM material.")]
        ssl_truststore_type: Option<String>,

        #[key("ssl.keystore.location")]
        #[default(None)]
        #[kafka_type("string")]
        #[kafka_default("null")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_ssl.keystore.location")]
        #[feature("tls-rustls")]
        #[comment("Client identity material location used by mTLS broker connections.")]
        ssl_keystore_location: Option<String>,

        #[key("ssl.keystore.password")]
        #[default(None)]
        #[kafka_type("password")]
        #[kafka_default("null")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_ssl.keystore.password")]
        #[feature("tls-rustls")]
        #[comment("Password for configured TLS client identity material when the backend format requires it.")]
        ssl_keystore_password: Option<String>,

        #[key("ssl.keystore.key")]
        #[default(None)]
        #[kafka_type("password")]
        #[kafka_default("null")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_ssl.keystore.key")]
        #[feature("tls-rustls")]
        #[comment("Inline PEM private key for TLS client authentication.")]
        ssl_keystore_key: Option<String>,

        #[key("ssl.keystore.certificate.chain")]
        #[default(None)]
        #[kafka_type("password")]
        #[kafka_default("null")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_ssl.keystore.certificate.chain")]
        #[feature("tls-rustls")]
        #[comment("Inline PEM certificate chain for TLS client authentication.")]
        ssl_keystore_certificate_chain: Option<String>,

        #[key("ssl.keystore.type")]
        #[default(None)]
        #[kafka_type("string")]
        #[kafka_default("JKS")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_ssl.keystore.type")]
        #[feature("tls-rustls")]
        #[comment("Keystore format; rustls backend supports PEM material.")]
        ssl_keystore_type: Option<String>,

        #[key("ssl.key.password")]
        #[default(None)]
        #[kafka_type("password")]
        #[kafka_default("null")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_ssl.key.password")]
        #[feature("tls-rustls")]
        #[comment("Password for encrypted TLS private key material.")]
        ssl_key_password: Option<String>,

        #[key("ssl.endpoint.identification.algorithm")]
        #[default(Some(String::from("https")))]
        #[kafka_type("string")]
        #[kafka_default("https")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_ssl.endpoint.identification.algorithm")]
        #[feature("tls-rustls")]
        #[comment("Hostname verification algorithm; empty string disables endpoint identification explicitly.")]
        ssl_endpoint_identification_algorithm: Option<String>,

        #[key("ssl.protocol")]
        #[default(String::from("TLSv1.3"))]
        #[kafka_type("string")]
        #[kafka_default("TLSv1.3")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_ssl.protocol")]
        #[feature("tls-rustls")]
        #[comment("Preferred TLS protocol version.")]
        ssl_protocol: String,

        #[key("ssl.enabled.protocols")]
        #[default(Some(String::from("TLSv1.2,TLSv1.3")))]
        #[kafka_type("list")]
        #[kafka_default("TLSv1.2,TLSv1.3")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_ssl.enabled.protocols")]
        #[feature("tls-rustls")]
        #[comment("Comma-separated TLS protocol versions enabled for negotiation.")]
        ssl_enabled_protocols: Option<String>,

        #[key("ssl.cipher.suites")]
        #[default(None)]
        #[kafka_type("list")]
        #[kafka_default("null")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_ssl.cipher.suites")]
        #[feature("tls-rustls")]
        #[comment("Comma-separated TLS cipher suite names requested by the user.")]
        ssl_cipher_suites: Option<String>,

        #[key("sasl.mechanism")]
        #[default(String::from("GSSAPI"))]
        #[kafka_type("string")]
        #[kafka_default("GSSAPI")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_sasl.mechanism")]
        #[feature("sasl")]
        #[comment("Kafka SASL mechanism: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, OAUTHBEARER, or GSSAPI.")]
        sasl_mechanism: String,

        #[key("sasl.jaas.config")]
        #[default(None)]
        #[kafka_type("password")]
        #[kafka_default("null")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_sasl.jaas.config")]
        #[feature("sasl")]
        #[comment("Java-compatible JAAS login module options used to derive Rust SASL credentials.")]
        sasl_jaas_config: Option<String>,

        #[key("sasl.login.connect.timeout.ms")]
        #[default(None)]
        #[kafka_type("int")]
        #[kafka_default("null")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_sasl.login.connect.timeout.ms")]
        #[feature("sasl")]
        #[comment("External SASL login provider connection timeout.")]
        sasl_login_connect_timeout_ms: Option<DurationMs>,

        #[key("sasl.login.read.timeout.ms")]
        #[default(None)]
        #[kafka_type("int")]
        #[kafka_default("null")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_sasl.login.read.timeout.ms")]
        #[feature("sasl")]
        #[comment("External SASL login provider read timeout.")]
        sasl_login_read_timeout_ms: Option<DurationMs>,

        #[key("sasl.login.refresh.window.factor")]
        #[default(0.8_f64)]
        #[kafka_type("double")]
        #[kafka_default("0.8")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_sasl.login.refresh.window.factor")]
        #[feature("sasl")]
        #[comment("OAuth token refresh point as a factor of the token lifetime.")]
        sasl_login_refresh_window_factor: f64,

        #[key("sasl.login.refresh.window.jitter")]
        #[default(0.05_f64)]
        #[kafka_type("double")]
        #[kafka_default("0.05")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_sasl.login.refresh.window.jitter")]
        #[feature("sasl")]
        #[comment("OAuth token refresh jitter factor.")]
        sasl_login_refresh_window_jitter: f64,

        #[key("sasl.login.refresh.min.period.seconds")]
        #[default(60_i32)]
        #[kafka_type("short")]
        #[kafka_default("60")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_sasl.login.refresh.min.period.seconds")]
        #[feature("sasl")]
        #[comment("Minimum OAuth token lifetime before attempting refresh.")]
        sasl_login_refresh_min_period_seconds: i32,

        #[key("sasl.login.refresh.buffer.seconds")]
        #[default(300_i32)]
        #[kafka_type("short")]
        #[kafka_default("300")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_sasl.login.refresh.buffer.seconds")]
        #[feature("sasl")]
        #[comment("OAuth token expiration buffer kept before refresh.")]
        sasl_login_refresh_buffer_seconds: i32,

        #[key("sasl.login.retry.backoff.ms")]
        #[default(DurationMs::from_millis(100))]
        #[kafka_type("long")]
        #[kafka_default("100")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_sasl.login.retry.backoff.ms")]
        #[feature("sasl")]
        #[comment("Initial OAuth login retry backoff.")]
        sasl_login_retry_backoff_ms: DurationMs,

        #[key("sasl.login.retry.backoff.max.ms")]
        #[default(DurationMs::from_millis(10_000))]
        #[kafka_type("long")]
        #[kafka_default("10000")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_sasl.login.retry.backoff.max.ms")]
        #[feature("sasl")]
        #[comment("Maximum OAuth login retry backoff.")]
        sasl_login_retry_backoff_max_ms: DurationMs,

        #[key("sasl.oauthbearer.token.endpoint.url")]
        #[default(None)]
        #[kafka_type("string")]
        #[kafka_default("null")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_sasl.oauthbearer.token.endpoint.url")]
        #[feature("sasl")]
        #[comment("File token endpoint for SASL/OAUTHBEARER static JWT retrieval.")]
        sasl_oauthbearer_token_endpoint_url: Option<String>,

        #[key("sasl.oauthbearer.assertion.file")]
        #[default(None)]
        #[kafka_type("string")]
        #[kafka_default("null")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_sasl.oauthbearer.assertion.file")]
        #[feature("sasl")]
        #[comment("Pre-generated JWT assertion file for SASL/OAUTHBEARER token retrieval.")]
        sasl_oauthbearer_assertion_file: Option<String>,

        #[key("sasl.oauthbearer.client.credentials.client.id")]
        #[default(None)]
        #[kafka_type("string")]
        #[kafka_default("null")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_sasl.oauthbearer.client.credentials.client.id")]
        #[feature("sasl")]
        #[comment("OAuth client credentials client id for SASL/OAUTHBEARER token retrieval.")]
        sasl_oauthbearer_client_credentials_client_id: Option<String>,

        #[key("sasl.oauthbearer.client.credentials.client.secret")]
        #[default(None)]
        #[kafka_type("password")]
        #[kafka_default("null")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_sasl.oauthbearer.client.credentials.client.secret")]
        #[feature("sasl")]
        #[comment("OAuth client credentials client secret for SASL/OAUTHBEARER token retrieval.")]
        sasl_oauthbearer_client_credentials_client_secret: Option<String>,

        #[key("sasl.oauthbearer.scope")]
        #[default(None)]
        #[kafka_type("string")]
        #[kafka_default("null")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_sasl.oauthbearer.scope")]
        #[feature("sasl")]
        #[comment("Optional OAuth scope for SASL/OAUTHBEARER token retrieval.")]
        sasl_oauthbearer_scope: Option<String>,

        #[key("sasl.oauthbearer.assertion.private.key.file")]
        #[default(None)]
        #[kafka_type("string")]
        #[kafka_default("null")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_sasl.oauthbearer.assertion.private.key.file")]
        #[feature("sasl")]
        #[comment("PEM private key file used to sign SASL/OAUTHBEARER JWT assertions.")]
        sasl_oauthbearer_assertion_private_key_file: Option<String>,

        #[key("sasl.oauthbearer.assertion.private.key.passphrase")]
        #[default(None)]
        #[kafka_type("password")]
        #[kafka_default("null")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_sasl.oauthbearer.assertion.private.key.passphrase")]
        #[feature("sasl")]
        #[comment("Passphrase for the SASL/OAUTHBEARER assertion private key file.")]
        sasl_oauthbearer_assertion_private_key_passphrase: Option<String>,

        #[key("sasl.oauthbearer.assertion.template.file")]
        #[default(None)]
        #[kafka_type("string")]
        #[kafka_default("null")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_sasl.oauthbearer.assertion.template.file")]
        #[feature("sasl")]
        #[comment("JSON header and claim template for generated SASL/OAUTHBEARER JWT assertions.")]
        sasl_oauthbearer_assertion_template_file: Option<String>,

        #[key("sasl.oauthbearer.assertion.algorithm")]
        #[default(String::from("RS256"))]
        #[kafka_type("string")]
        #[kafka_default("DEFAULT_SASL_OAUTHBEARER_ASSERTION_ALGORITHM")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_sasl.oauthbearer.assertion.algorithm")]
        #[feature("sasl")]
        #[comment("Signing algorithm for generated SASL/OAUTHBEARER JWT assertions.")]
        sasl_oauthbearer_assertion_algorithm: String,

        #[key("sasl.oauthbearer.assertion.claim.aud")]
        #[default(None)]
        #[kafka_type("string")]
        #[kafka_default("null")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_sasl.oauthbearer.assertion.claim.aud")]
        #[feature("sasl")]
        #[comment("Audience claim for generated SASL/OAUTHBEARER JWT assertions.")]
        sasl_oauthbearer_assertion_claim_aud: Option<String>,

        #[key("sasl.oauthbearer.assertion.claim.iss")]
        #[default(None)]
        #[kafka_type("string")]
        #[kafka_default("null")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_sasl.oauthbearer.assertion.claim.iss")]
        #[feature("sasl")]
        #[comment("Issuer claim for generated SASL/OAUTHBEARER JWT assertions.")]
        sasl_oauthbearer_assertion_claim_iss: Option<String>,

        #[key("sasl.oauthbearer.assertion.claim.sub")]
        #[default(None)]
        #[kafka_type("string")]
        #[kafka_default("null")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_sasl.oauthbearer.assertion.claim.sub")]
        #[feature("sasl")]
        #[comment("Subject claim for generated SASL/OAUTHBEARER JWT assertions.")]
        sasl_oauthbearer_assertion_claim_sub: Option<String>,

        #[key("sasl.oauthbearer.assertion.claim.exp.seconds")]
        #[default(60_i32)]
        #[kafka_type("int")]
        #[kafka_default("DEFAULT_SASL_OAUTHBEARER_ASSERTION_CLAIM_EXP_SECONDS")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_sasl.oauthbearer.assertion.claim.exp.seconds")]
        #[feature("sasl")]
        #[comment("Validity period in seconds for generated SASL/OAUTHBEARER JWT assertions.")]
        sasl_oauthbearer_assertion_claim_exp_seconds: i32,

        #[key("sasl.oauthbearer.assertion.claim.nbf.seconds")]
        #[default(0_i32)]
        #[kafka_type("int")]
        #[kafka_default("DEFAULT_SASL_OAUTHBEARER_ASSERTION_CLAIM_NBF_SECONDS")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_sasl.oauthbearer.assertion.claim.nbf.seconds")]
        #[feature("sasl")]
        #[comment("Not-before offset in seconds for generated SASL/OAUTHBEARER JWT assertions.")]
        sasl_oauthbearer_assertion_claim_nbf_seconds: i32,

        #[key("sasl.oauthbearer.assertion.claim.jti.include")]
        #[default(false)]
        #[kafka_type("boolean")]
        #[kafka_default("DEFAULT_SASL_OAUTHBEARER_ASSERTION_CLAIM_JTI_INCLUDE")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_sasl.oauthbearer.assertion.claim.jti.include")]
        #[feature("sasl")]
        #[comment("Whether generated SASL/OAUTHBEARER JWT assertions include a random jti claim.")]
        sasl_oauthbearer_assertion_claim_jti_include: bool,

        #[key("sasl.login.callback.handler.class")]
        #[default(None)]
        #[kafka_type("class")]
        #[kafka_default("null")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_sasl.login.callback.handler.class")]
        #[feature("sasl")]
        #[comment("Java SASL login callback handler class name retained for explicit compatibility errors.")]
        sasl_login_callback_handler_class: Option<String>,

        #[key("sasl.client.callback.handler.class")]
        #[default(None)]
        #[kafka_type("class")]
        #[kafka_default("null")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_sasl.client.callback.handler.class")]
        #[feature("sasl")]
        #[comment("Java SASL client callback handler class name retained for explicit compatibility errors.")]
        sasl_client_callback_handler_class: Option<String>,

        #[key("sasl.kerberos.service.name")]
        #[default(None)]
        #[kafka_type("string")]
        #[kafka_default("null")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_sasl.kerberos.service.name")]
        #[feature("sasl")]
        #[comment("Kerberos service principal name for GSSAPI.")]
        sasl_kerberos_service_name: Option<String>,

        #[key("sasl.kerberos.kinit.cmd")]
        #[default(String::from("/usr/bin/kinit"))]
        #[kafka_type("string")]
        #[kafka_default("SaslConfigs.DEFAULT_KERBEROS_KINIT_CMD")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_sasl.kerberos.kinit.cmd")]
        #[feature("sasl")]
        #[comment("Kerberos kinit command configured for a future GSSAPI backend.")]
        sasl_kerberos_kinit_cmd: String,

        #[key("sasl.kerberos.ticket.renew.window.factor")]
        #[default(0.8_f64)]
        #[kafka_type("double")]
        #[kafka_default("SaslConfigs.DEFAULT_KERBEROS_TICKET_RENEW_WINDOW_FACTOR")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_sasl.kerberos.ticket.renew.window.factor")]
        #[feature("sasl")]
        #[comment("Kerberos ticket renew window factor for a future GSSAPI backend.")]
        sasl_kerberos_ticket_renew_window_factor: f64,

        #[key("sasl.kerberos.ticket.renew.jitter")]
        #[default(0.05_f64)]
        #[kafka_type("double")]
        #[kafka_default("SaslConfigs.DEFAULT_KERBEROS_TICKET_RENEW_JITTER")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_sasl.kerberos.ticket.renew.jitter")]
        #[feature("sasl")]
        #[comment("Kerberos ticket renew jitter for a future GSSAPI backend.")]
        sasl_kerberos_ticket_renew_jitter: f64,

        #[key("sasl.kerberos.min.time.before.relogin")]
        #[default(DurationMs::from_millis(60_000))]
        #[kafka_type("long")]
        #[kafka_default("SaslConfigs.DEFAULT_KERBEROS_MIN_TIME_BEFORE_RELOGIN")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_sasl.kerberos.min.time.before.relogin")]
        #[feature("sasl")]
        #[comment("Minimum time before Kerberos relogin for a future GSSAPI backend.")]
        sasl_kerberos_min_time_before_relogin: DurationMs,

        #[key("metadata.recovery.strategy")]
        #[default(String::from("rebootstrap"))]
        #[kafka_type("string")]
        #[kafka_default("rebootstrap")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_metadata.recovery.strategy")]
        #[comment("Recovery strategy used when no known broker is available.")]
        metadata_recovery_strategy: String,

        #[key("transaction.timeout.ms")]
        #[default(DurationMs::from_millis(60_000))]
        #[kafka_type("int")]
        #[kafka_default("60000 (1 minute)")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_transaction.timeout.ms")]
        #[comment("Maximum transaction timeout requested by a transactional producer.")]
        transaction_timeout_ms: DurationMs,

        #[key("transaction.two.phase.commit.enable")]
        #[default(false)]
        #[kafka_type("boolean")]
        #[kafka_default("false")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_transaction.two.phase.commit.enable")]
        #[comment("Whether producer transactions use Kafka's two-phase commit mode.")]
        transaction_two_phase_commit_enable: bool,

        #[key("transactional.id")]
        #[default(String::new())]
        #[kafka_type("string")]
        #[kafka_default("null")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_transactional.id")]
        #[comment("Transactional producer id; empty means unset in this Rust config.")]
        transactional_id: String,

        #[key("socket.tcp.nodelay")]
        #[default(true)]
        #[kafka_type("boolean")]
        #[kafka_default("true")]
        #[status(native)]
        #[origin(kacrab_runtime)]
        #[source("kacrab-runtime://config/socket.tcp.nodelay")]
        #[feature("socket2")]
        #[comment("Sets TCP_NODELAY on broker TCP connections.")]
        socket_tcp_nodelay: bool,

        #[key("socket.tcp.quickack")]
        #[default(None)]
        #[kafka_type("boolean")]
        #[kafka_default("null")]
        #[status(native)]
        #[origin(kacrab_runtime)]
        #[source("kacrab-runtime://config/socket.tcp.quickack")]
        #[platforms("linux", "android", "fuchsia", "cygwin")]
        #[feature("socket2")]
        #[comment("Sets TCP_QUICKACK after a broker TCP connection is established.")]
        socket_tcp_quickack: Option<bool>,

        #[key("socket.tcp.notsent.lowat.bytes")]
        #[default(None)]
        #[kafka_type("int")]
        #[kafka_default("null")]
        #[status(native)]
        #[origin(kacrab_runtime)]
        #[source("kacrab-runtime://config/socket.tcp.notsent.lowat.bytes")]
        #[platforms("linux", "android")]
        #[feature("socket2")]
        #[comment("Sets TCP_NOTSENT_LOWAT on supported broker TCP sockets.")]
        socket_tcp_notsent_lowat_bytes: Option<u32>,

        #[key("socket.tcp.user.timeout.ms")]
        #[default(None)]
        #[kafka_type("long")]
        #[kafka_default("null")]
        #[status(native)]
        #[origin(kacrab_runtime)]
        #[source("kacrab-runtime://config/socket.tcp.user.timeout.ms")]
        #[platforms("linux", "android", "fuchsia", "cygwin")]
        #[feature("socket2")]
        #[comment("Sets TCP_USER_TIMEOUT on supported broker TCP sockets.")]
        socket_tcp_user_timeout_ms: Option<DurationMs>,

        #[key("socket.tcp.congestion")]
        #[default(None)]
        #[kafka_type("string")]
        #[kafka_default("null")]
        #[status(native)]
        #[origin(kacrab_runtime)]
        #[source("kacrab-runtime://config/socket.tcp.congestion")]
        #[platforms("linux", "freebsd")]
        #[feature("socket2")]
        #[comment("Sets the TCP congestion-control algorithm on supported broker TCP sockets.")]
        socket_tcp_congestion: Option<TcpCongestionControl>,

        #[key("socket.reuse.address")]
        #[default(true)]
        #[kafka_type("boolean")]
        #[kafka_default("true")]
        #[status(native)]
        #[origin(kacrab_runtime)]
        #[source("kacrab-runtime://config/socket.reuse.address")]
        #[feature("socket2")]
        #[comment("Sets SO_REUSEADDR before connecting broker TCP sockets.")]
        socket_reuse_address: bool,

        #[key("socket.read.buffer.capacity.bytes")]
        #[default(None)]
        #[kafka_type("int")]
        #[kafka_default("null")]
        #[status(native)]
        #[origin(kacrab_runtime)]
        #[source("kacrab-runtime://config/socket.read.buffer.capacity.bytes")]
        #[feature("socket2")]
        #[comment("Initial reusable in-process read buffer capacity for broker frame reads.")]
        socket_read_buffer_capacity_bytes: Option<usize>,

        #[key("broker.queue.capacity")]
        #[default(None)]
        #[kafka_type("int")]
        #[kafka_default("null")]
        #[status(native)]
        #[origin(kacrab_runtime)]
        #[source("kacrab-runtime://config/broker.queue.capacity")]
        #[comment("Bounded pending command queue capacity for each broker IO task.")]
        broker_queue_capacity: Option<usize>,

        #[key("buffer.pool.capacity")]
        #[default(None)]
        #[kafka_type("int")]
        #[kafka_default("null")]
        #[status(native)]
        #[origin(kacrab_runtime)]
        #[source("kacrab-runtime://config/buffer.pool.capacity")]
        #[comment("Number of reusable wire read/write buffers retained per client.")]
        buffer_pool_capacity: Option<usize>,
    }
}

impl ProducerConfig {
    /// Builds a wire connection config from this typed producer config.
    #[must_use]
    pub fn to_connection_config(&self) -> crate::wire::ConnectionConfig {
        connection_config_from_fields(&ConnectionConfigFields {
            send_buffer_bytes: self.send_buffer_bytes,
            receive_buffer_bytes: self.receive_buffer_bytes,
            request_timeout_ms: self.request_timeout_ms,
            metadata_max_age_ms: self.metadata_max_age_ms,
            metadata_max_idle_ms: self.metadata_max_idle_ms,
            metadata_recovery_strategy: self.metadata_recovery_strategy.clone(),
            metadata_recovery_rebootstrap_trigger_ms: self.metadata_recovery_rebootstrap_trigger_ms,
            connections_max_idle_ms: self.connections_max_idle_ms,
            reconnect_backoff_ms: Some(self.reconnect_backoff_ms),
            reconnect_backoff_max_ms: Some(self.reconnect_backoff_max_ms),
            retry_backoff_ms: Some(self.retry_backoff_ms),
            retry_backoff_max_ms: Some(self.retry_backoff_max_ms),
            socket_connection_setup_timeout_ms: self.socket_connection_setup_timeout_ms,
            socket_connection_setup_timeout_max_ms: self.socket_connection_setup_timeout_max_ms,
            socket_tcp_nodelay: self.socket_tcp_nodelay,
            socket_tcp_notsent_lowat_bytes: self.socket_tcp_notsent_lowat_bytes,
            socket_tcp_quickack: self.socket_tcp_quickack,
            socket_tcp_user_timeout_ms: self.socket_tcp_user_timeout_ms,
            socket_tcp_congestion: self.socket_tcp_congestion,
            socket_reuse_address: self.socket_reuse_address,
            socket_read_buffer_capacity_bytes: self.socket_read_buffer_capacity_bytes,
            max_in_flight_requests_per_connection: Some(self.max_in_flight_requests_per_connection),
            broker_queue_capacity: self.broker_queue_capacity,
            buffer_pool_capacity: self.buffer_pool_capacity,
            allow_auto_topic_creation: false,
            security_protocol: self.security_protocol.clone(),
            ssl_truststore_location: self.ssl_truststore_location.clone(),
            ssl_truststore_password: self.ssl_truststore_password.clone(),
            ssl_truststore_certificates: self.ssl_truststore_certificates.clone(),
            ssl_truststore_type: self.ssl_truststore_type.clone(),
            ssl_keystore_location: self.ssl_keystore_location.clone(),
            ssl_keystore_password: self.ssl_keystore_password.clone(),
            ssl_keystore_key: self.ssl_keystore_key.clone(),
            ssl_keystore_certificate_chain: self.ssl_keystore_certificate_chain.clone(),
            ssl_keystore_type: self.ssl_keystore_type.clone(),
            ssl_key_password: self.ssl_key_password.clone(),
            ssl_endpoint_identification_algorithm: self
                .ssl_endpoint_identification_algorithm
                .clone(),
            ssl_protocol: self.ssl_protocol.clone(),
            ssl_enabled_protocols: self.ssl_enabled_protocols.clone(),
            ssl_cipher_suites: self.ssl_cipher_suites.clone(),
            sasl_mechanism: self.sasl_mechanism.clone(),
            sasl_jaas_config: self.sasl_jaas_config.clone(),
            sasl_login_connect_timeout_ms: self.sasl_login_connect_timeout_ms,
            sasl_login_read_timeout_ms: self.sasl_login_read_timeout_ms,
            sasl_login_refresh_window_factor: self.sasl_login_refresh_window_factor,
            sasl_login_refresh_window_jitter: self.sasl_login_refresh_window_jitter,
            sasl_login_refresh_min_period_seconds: self.sasl_login_refresh_min_period_seconds,
            sasl_login_refresh_buffer_seconds: self.sasl_login_refresh_buffer_seconds,
            sasl_login_retry_backoff_ms: self.sasl_login_retry_backoff_ms,
            sasl_login_retry_backoff_max_ms: self.sasl_login_retry_backoff_max_ms,
            sasl_oauthbearer_token_endpoint_url: self.sasl_oauthbearer_token_endpoint_url.clone(),
            sasl_oauthbearer_assertion_file: self.sasl_oauthbearer_assertion_file.clone(),
            sasl_oauthbearer_client_credentials_client_id: self
                .sasl_oauthbearer_client_credentials_client_id
                .clone(),
            sasl_oauthbearer_client_credentials_client_secret: self
                .sasl_oauthbearer_client_credentials_client_secret
                .clone(),
            sasl_oauthbearer_scope: self.sasl_oauthbearer_scope.clone(),
            sasl_oauthbearer_assertion_private_key_file: self
                .sasl_oauthbearer_assertion_private_key_file
                .clone(),
            sasl_oauthbearer_assertion_private_key_passphrase: self
                .sasl_oauthbearer_assertion_private_key_passphrase
                .clone(),
            sasl_oauthbearer_assertion_template_file: self
                .sasl_oauthbearer_assertion_template_file
                .clone(),
            sasl_oauthbearer_assertion_algorithm: self.sasl_oauthbearer_assertion_algorithm.clone(),
            sasl_oauthbearer_assertion_claim_aud: self.sasl_oauthbearer_assertion_claim_aud.clone(),
            sasl_oauthbearer_assertion_claim_iss: self.sasl_oauthbearer_assertion_claim_iss.clone(),
            sasl_oauthbearer_assertion_claim_sub: self.sasl_oauthbearer_assertion_claim_sub.clone(),
            sasl_oauthbearer_assertion_claim_exp_seconds: self
                .sasl_oauthbearer_assertion_claim_exp_seconds,
            sasl_oauthbearer_assertion_claim_nbf_seconds: self
                .sasl_oauthbearer_assertion_claim_nbf_seconds,
            sasl_oauthbearer_assertion_claim_jti_include: self
                .sasl_oauthbearer_assertion_claim_jti_include,
            sasl_login_callback_handler_class: self.sasl_login_callback_handler_class.clone(),
            sasl_client_callback_handler_class: self.sasl_client_callback_handler_class.clone(),
            sasl_kerberos_service_name: self.sasl_kerberos_service_name.clone(),
            sasl_kerberos_kinit_cmd: Some(self.sasl_kerberos_kinit_cmd.clone()),
            sasl_kerberos_ticket_renew_window_factor: self.sasl_kerberos_ticket_renew_window_factor,
            sasl_kerberos_ticket_renew_jitter: self.sasl_kerberos_ticket_renew_jitter,
            sasl_kerberos_min_time_before_relogin: self.sasl_kerberos_min_time_before_relogin,
        })
    }

    /// Builds runtime producer settings from this typed producer config.
    #[cfg(feature = "producer")]
    pub fn to_producer_runtime_config(
        &self,
    ) -> crate::producer::Result<crate::producer::ProducerRuntimeConfig> {
        crate::producer::ProducerRuntimeConfig::from_config(self)
    }
}

impl ConsumerConfig {
    /// Builds a wire connection config from this typed consumer config.
    #[must_use]
    pub fn to_connection_config(&self) -> crate::wire::ConnectionConfig {
        connection_config_from_fields(&ConnectionConfigFields {
            send_buffer_bytes: self.send_buffer_bytes,
            receive_buffer_bytes: self.receive_buffer_bytes,
            request_timeout_ms: self.request_timeout_ms,
            metadata_max_age_ms: self.metadata_max_age_ms,
            metadata_max_idle_ms: DurationMs::from_millis(300_000),
            metadata_recovery_strategy: self.metadata_recovery_strategy.clone(),
            metadata_recovery_rebootstrap_trigger_ms: self.metadata_recovery_rebootstrap_trigger_ms,
            connections_max_idle_ms: self.connections_max_idle_ms,
            reconnect_backoff_ms: None,
            reconnect_backoff_max_ms: None,
            retry_backoff_ms: None,
            retry_backoff_max_ms: None,
            socket_connection_setup_timeout_ms: self.socket_connection_setup_timeout_ms,
            socket_connection_setup_timeout_max_ms: self.socket_connection_setup_timeout_max_ms,
            socket_tcp_nodelay: self.socket_tcp_nodelay,
            socket_tcp_notsent_lowat_bytes: self.socket_tcp_notsent_lowat_bytes,
            socket_tcp_quickack: self.socket_tcp_quickack,
            socket_tcp_user_timeout_ms: self.socket_tcp_user_timeout_ms,
            socket_tcp_congestion: self.socket_tcp_congestion,
            socket_reuse_address: self.socket_reuse_address,
            socket_read_buffer_capacity_bytes: self.socket_read_buffer_capacity_bytes,
            max_in_flight_requests_per_connection: None,
            broker_queue_capacity: None,
            buffer_pool_capacity: None,
            allow_auto_topic_creation: self.allow_auto_create_topics,
            security_protocol: self.security_protocol.clone(),
            ssl_truststore_location: self.ssl_truststore_location.clone(),
            ssl_truststore_password: self.ssl_truststore_password.clone(),
            ssl_truststore_certificates: self.ssl_truststore_certificates.clone(),
            ssl_truststore_type: self.ssl_truststore_type.clone(),
            ssl_keystore_location: self.ssl_keystore_location.clone(),
            ssl_keystore_password: self.ssl_keystore_password.clone(),
            ssl_keystore_key: self.ssl_keystore_key.clone(),
            ssl_keystore_certificate_chain: self.ssl_keystore_certificate_chain.clone(),
            ssl_keystore_type: self.ssl_keystore_type.clone(),
            ssl_key_password: self.ssl_key_password.clone(),
            ssl_endpoint_identification_algorithm: self
                .ssl_endpoint_identification_algorithm
                .clone(),
            ssl_protocol: self.ssl_protocol.clone(),
            ssl_enabled_protocols: self.ssl_enabled_protocols.clone(),
            ssl_cipher_suites: self.ssl_cipher_suites.clone(),
            sasl_mechanism: self.sasl_mechanism.clone(),
            sasl_jaas_config: self.sasl_jaas_config.clone(),
            sasl_login_connect_timeout_ms: self.sasl_login_connect_timeout_ms,
            sasl_login_read_timeout_ms: self.sasl_login_read_timeout_ms,
            sasl_login_refresh_window_factor: self.sasl_login_refresh_window_factor,
            sasl_login_refresh_window_jitter: self.sasl_login_refresh_window_jitter,
            sasl_login_refresh_min_period_seconds: self.sasl_login_refresh_min_period_seconds,
            sasl_login_refresh_buffer_seconds: self.sasl_login_refresh_buffer_seconds,
            sasl_login_retry_backoff_ms: self.sasl_login_retry_backoff_ms,
            sasl_login_retry_backoff_max_ms: self.sasl_login_retry_backoff_max_ms,
            sasl_oauthbearer_token_endpoint_url: self.sasl_oauthbearer_token_endpoint_url.clone(),
            sasl_oauthbearer_assertion_file: self.sasl_oauthbearer_assertion_file.clone(),
            sasl_oauthbearer_client_credentials_client_id: self
                .sasl_oauthbearer_client_credentials_client_id
                .clone(),
            sasl_oauthbearer_client_credentials_client_secret: self
                .sasl_oauthbearer_client_credentials_client_secret
                .clone(),
            sasl_oauthbearer_scope: self.sasl_oauthbearer_scope.clone(),
            sasl_oauthbearer_assertion_private_key_file: self
                .sasl_oauthbearer_assertion_private_key_file
                .clone(),
            sasl_oauthbearer_assertion_private_key_passphrase: self
                .sasl_oauthbearer_assertion_private_key_passphrase
                .clone(),
            sasl_oauthbearer_assertion_template_file: self
                .sasl_oauthbearer_assertion_template_file
                .clone(),
            sasl_oauthbearer_assertion_algorithm: self.sasl_oauthbearer_assertion_algorithm.clone(),
            sasl_oauthbearer_assertion_claim_aud: self.sasl_oauthbearer_assertion_claim_aud.clone(),
            sasl_oauthbearer_assertion_claim_iss: self.sasl_oauthbearer_assertion_claim_iss.clone(),
            sasl_oauthbearer_assertion_claim_sub: self.sasl_oauthbearer_assertion_claim_sub.clone(),
            sasl_oauthbearer_assertion_claim_exp_seconds: self
                .sasl_oauthbearer_assertion_claim_exp_seconds,
            sasl_oauthbearer_assertion_claim_nbf_seconds: self
                .sasl_oauthbearer_assertion_claim_nbf_seconds,
            sasl_oauthbearer_assertion_claim_jti_include: self
                .sasl_oauthbearer_assertion_claim_jti_include,
            sasl_login_callback_handler_class: self.sasl_login_callback_handler_class.clone(),
            sasl_client_callback_handler_class: self.sasl_client_callback_handler_class.clone(),
            sasl_kerberos_service_name: self.sasl_kerberos_service_name.clone(),
            sasl_kerberos_kinit_cmd: Some(self.sasl_kerberos_kinit_cmd.clone()),
            sasl_kerberos_ticket_renew_window_factor: self.sasl_kerberos_ticket_renew_window_factor,
            sasl_kerberos_ticket_renew_jitter: self.sasl_kerberos_ticket_renew_jitter,
            sasl_kerberos_min_time_before_relogin: self.sasl_kerberos_min_time_before_relogin,
        })
    }
}

impl AdminConfig {
    /// Builds a wire connection config from this typed admin config.
    #[must_use]
    pub fn to_connection_config(&self) -> crate::wire::ConnectionConfig {
        connection_config_from_fields(&ConnectionConfigFields {
            send_buffer_bytes: self.send_buffer_bytes,
            receive_buffer_bytes: self.receive_buffer_bytes,
            request_timeout_ms: self.request_timeout_ms,
            metadata_max_age_ms: self.metadata_max_age_ms,
            metadata_max_idle_ms: DurationMs::from_millis(300_000),
            metadata_recovery_strategy: self.metadata_recovery_strategy.clone(),
            metadata_recovery_rebootstrap_trigger_ms: self.metadata_recovery_rebootstrap_trigger_ms,
            connections_max_idle_ms: self.connections_max_idle_ms,
            reconnect_backoff_ms: None,
            reconnect_backoff_max_ms: None,
            retry_backoff_ms: None,
            retry_backoff_max_ms: None,
            socket_connection_setup_timeout_ms: self.socket_connection_setup_timeout_ms,
            socket_connection_setup_timeout_max_ms: self.socket_connection_setup_timeout_max_ms,
            socket_tcp_nodelay: self.socket_tcp_nodelay,
            socket_tcp_notsent_lowat_bytes: self.socket_tcp_notsent_lowat_bytes,
            socket_tcp_quickack: self.socket_tcp_quickack,
            socket_tcp_user_timeout_ms: self.socket_tcp_user_timeout_ms,
            socket_tcp_congestion: self.socket_tcp_congestion,
            socket_reuse_address: self.socket_reuse_address,
            socket_read_buffer_capacity_bytes: self.socket_read_buffer_capacity_bytes,
            max_in_flight_requests_per_connection: None,
            broker_queue_capacity: None,
            buffer_pool_capacity: None,
            allow_auto_topic_creation: false,
            security_protocol: self.security_protocol.clone(),
            ssl_truststore_location: self.ssl_truststore_location.clone(),
            ssl_truststore_password: self.ssl_truststore_password.clone(),
            ssl_truststore_certificates: self.ssl_truststore_certificates.clone(),
            ssl_truststore_type: self.ssl_truststore_type.clone(),
            ssl_keystore_location: self.ssl_keystore_location.clone(),
            ssl_keystore_password: self.ssl_keystore_password.clone(),
            ssl_keystore_key: self.ssl_keystore_key.clone(),
            ssl_keystore_certificate_chain: self.ssl_keystore_certificate_chain.clone(),
            ssl_keystore_type: self.ssl_keystore_type.clone(),
            ssl_key_password: self.ssl_key_password.clone(),
            ssl_endpoint_identification_algorithm: self
                .ssl_endpoint_identification_algorithm
                .clone(),
            ssl_protocol: self.ssl_protocol.clone(),
            ssl_enabled_protocols: self.ssl_enabled_protocols.clone(),
            ssl_cipher_suites: self.ssl_cipher_suites.clone(),
            sasl_mechanism: self.sasl_mechanism.clone(),
            sasl_jaas_config: self.sasl_jaas_config.clone(),
            sasl_login_connect_timeout_ms: self.sasl_login_connect_timeout_ms,
            sasl_login_read_timeout_ms: self.sasl_login_read_timeout_ms,
            sasl_login_refresh_window_factor: self.sasl_login_refresh_window_factor,
            sasl_login_refresh_window_jitter: self.sasl_login_refresh_window_jitter,
            sasl_login_refresh_min_period_seconds: self.sasl_login_refresh_min_period_seconds,
            sasl_login_refresh_buffer_seconds: self.sasl_login_refresh_buffer_seconds,
            sasl_login_retry_backoff_ms: self.sasl_login_retry_backoff_ms,
            sasl_login_retry_backoff_max_ms: self.sasl_login_retry_backoff_max_ms,
            sasl_oauthbearer_token_endpoint_url: self.sasl_oauthbearer_token_endpoint_url.clone(),
            sasl_oauthbearer_assertion_file: self.sasl_oauthbearer_assertion_file.clone(),
            sasl_oauthbearer_client_credentials_client_id: self
                .sasl_oauthbearer_client_credentials_client_id
                .clone(),
            sasl_oauthbearer_client_credentials_client_secret: self
                .sasl_oauthbearer_client_credentials_client_secret
                .clone(),
            sasl_oauthbearer_scope: self.sasl_oauthbearer_scope.clone(),
            sasl_oauthbearer_assertion_private_key_file: self
                .sasl_oauthbearer_assertion_private_key_file
                .clone(),
            sasl_oauthbearer_assertion_private_key_passphrase: self
                .sasl_oauthbearer_assertion_private_key_passphrase
                .clone(),
            sasl_oauthbearer_assertion_template_file: self
                .sasl_oauthbearer_assertion_template_file
                .clone(),
            sasl_oauthbearer_assertion_algorithm: self.sasl_oauthbearer_assertion_algorithm.clone(),
            sasl_oauthbearer_assertion_claim_aud: self.sasl_oauthbearer_assertion_claim_aud.clone(),
            sasl_oauthbearer_assertion_claim_iss: self.sasl_oauthbearer_assertion_claim_iss.clone(),
            sasl_oauthbearer_assertion_claim_sub: self.sasl_oauthbearer_assertion_claim_sub.clone(),
            sasl_oauthbearer_assertion_claim_exp_seconds: self
                .sasl_oauthbearer_assertion_claim_exp_seconds,
            sasl_oauthbearer_assertion_claim_nbf_seconds: self
                .sasl_oauthbearer_assertion_claim_nbf_seconds,
            sasl_oauthbearer_assertion_claim_jti_include: self
                .sasl_oauthbearer_assertion_claim_jti_include,
            sasl_login_callback_handler_class: self.sasl_login_callback_handler_class.clone(),
            sasl_client_callback_handler_class: self.sasl_client_callback_handler_class.clone(),
            sasl_kerberos_service_name: self.sasl_kerberos_service_name.clone(),
            sasl_kerberos_kinit_cmd: Some(self.sasl_kerberos_kinit_cmd.clone()),
            sasl_kerberos_ticket_renew_window_factor: self.sasl_kerberos_ticket_renew_window_factor,
            sasl_kerberos_ticket_renew_jitter: self.sasl_kerberos_ticket_renew_jitter,
            sasl_kerberos_min_time_before_relogin: self.sasl_kerberos_min_time_before_relogin,
        })
    }
}

#[derive(Clone)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "internal field bag mirroring Kafka's flat connection config keys, several of which \
              are booleans"
)]
struct ConnectionConfigFields {
    send_buffer_bytes: i32,
    receive_buffer_bytes: i32,
    request_timeout_ms: DurationMs,
    metadata_max_age_ms: DurationMs,
    metadata_max_idle_ms: DurationMs,
    metadata_recovery_strategy: String,
    metadata_recovery_rebootstrap_trigger_ms: DurationMs,
    connections_max_idle_ms: DurationMs,
    reconnect_backoff_ms: Option<DurationMs>,
    reconnect_backoff_max_ms: Option<DurationMs>,
    retry_backoff_ms: Option<DurationMs>,
    retry_backoff_max_ms: Option<DurationMs>,
    socket_connection_setup_timeout_ms: DurationMs,
    socket_connection_setup_timeout_max_ms: DurationMs,
    socket_tcp_nodelay: bool,
    socket_tcp_notsent_lowat_bytes: Option<u32>,
    socket_tcp_quickack: Option<bool>,
    socket_tcp_user_timeout_ms: Option<DurationMs>,
    socket_tcp_congestion: Option<TcpCongestionControl>,
    socket_reuse_address: bool,
    socket_read_buffer_capacity_bytes: Option<usize>,
    max_in_flight_requests_per_connection: Option<i32>,
    broker_queue_capacity: Option<usize>,
    buffer_pool_capacity: Option<usize>,
    allow_auto_topic_creation: bool,
    security_protocol: String,
    ssl_truststore_location: Option<String>,
    ssl_truststore_password: Option<String>,
    ssl_truststore_certificates: Option<String>,
    ssl_truststore_type: Option<String>,
    ssl_keystore_location: Option<String>,
    ssl_keystore_password: Option<String>,
    ssl_keystore_key: Option<String>,
    ssl_keystore_certificate_chain: Option<String>,
    ssl_keystore_type: Option<String>,
    ssl_key_password: Option<String>,
    ssl_endpoint_identification_algorithm: Option<String>,
    ssl_protocol: String,
    ssl_enabled_protocols: Option<String>,
    ssl_cipher_suites: Option<String>,
    sasl_mechanism: String,
    sasl_jaas_config: Option<String>,
    sasl_login_connect_timeout_ms: Option<DurationMs>,
    sasl_login_read_timeout_ms: Option<DurationMs>,
    sasl_login_refresh_window_factor: f64,
    sasl_login_refresh_window_jitter: f64,
    sasl_login_refresh_min_period_seconds: i32,
    sasl_login_refresh_buffer_seconds: i32,
    sasl_login_retry_backoff_ms: DurationMs,
    sasl_login_retry_backoff_max_ms: DurationMs,
    sasl_oauthbearer_token_endpoint_url: Option<String>,
    sasl_oauthbearer_assertion_file: Option<String>,
    sasl_oauthbearer_client_credentials_client_id: Option<String>,
    sasl_oauthbearer_client_credentials_client_secret: Option<String>,
    sasl_oauthbearer_scope: Option<String>,
    sasl_oauthbearer_assertion_private_key_file: Option<String>,
    sasl_oauthbearer_assertion_private_key_passphrase: Option<String>,
    sasl_oauthbearer_assertion_template_file: Option<String>,
    sasl_oauthbearer_assertion_algorithm: String,
    sasl_oauthbearer_assertion_claim_aud: Option<String>,
    sasl_oauthbearer_assertion_claim_iss: Option<String>,
    sasl_oauthbearer_assertion_claim_sub: Option<String>,
    sasl_oauthbearer_assertion_claim_exp_seconds: i32,
    sasl_oauthbearer_assertion_claim_nbf_seconds: i32,
    sasl_oauthbearer_assertion_claim_jti_include: bool,
    sasl_login_callback_handler_class: Option<String>,
    sasl_client_callback_handler_class: Option<String>,
    sasl_kerberos_service_name: Option<String>,
    sasl_kerberos_kinit_cmd: Option<String>,
    sasl_kerberos_ticket_renew_window_factor: f64,
    sasl_kerberos_ticket_renew_jitter: f64,
    sasl_kerberos_min_time_before_relogin: DurationMs,
}

fn connection_config_from_fields(fields: &ConnectionConfigFields) -> crate::wire::ConnectionConfig {
    let default = crate::wire::ConnectionConfig::default();
    let security_protocol = crate::wire::SecurityProtocol::parse(&fields.security_protocol)
        .unwrap_or(default.security.protocol);
    let sasl_mechanism = if security_protocol.uses_sasl() {
        crate::wire::SaslMechanism::parse(&fields.sasl_mechanism).ok()
    } else {
        None
    };
    crate::wire::ConnectionConfig {
        socket: socket_config_from_fields(fields),
        security: crate::wire::SecurityConfig {
            protocol: security_protocol,
        },
        tls: tls_config_from_fields(fields),
        sasl: sasl_config_from_fields(fields, sasl_mechanism),
        transport: crate::wire::TransportConfig::Plaintext,
        request_timeout: fields.request_timeout_ms.duration(),
        metadata_max_age: fields.metadata_max_age_ms.duration(),
        metadata_max_idle: fields.metadata_max_idle_ms.duration(),
        metadata_recovery_strategy: metadata_recovery_strategy(&fields.metadata_recovery_strategy),
        metadata_rebootstrap_trigger: fields.metadata_recovery_rebootstrap_trigger_ms.duration(),
        metadata_refresh_backoff_initial: fields.retry_backoff_ms.map_or(
            default.metadata_refresh_backoff_initial,
            DurationMs::duration,
        ),
        metadata_refresh_backoff_max: fields
            .retry_backoff_max_ms
            .map_or(default.metadata_refresh_backoff_max, DurationMs::duration),
        connections_max_idle: fields.connections_max_idle_ms.duration(),
        socket_connection_setup_timeout: fields.socket_connection_setup_timeout_ms.duration(),
        socket_connection_setup_timeout_max: fields
            .socket_connection_setup_timeout_max_ms
            .duration(),
        read_buffer_capacity: fields.socket_read_buffer_capacity_bytes,
        max_in_flight_requests_per_connection: fields
            .max_in_flight_requests_per_connection
            .and_then(positive_i32_to_usize)
            .unwrap_or(default.max_in_flight_requests_per_connection),
        broker_queue_capacity: fields
            .broker_queue_capacity
            .filter(|capacity| *capacity > 0)
            .unwrap_or(default.broker_queue_capacity),
        reconnect_backoff_initial: fields
            .reconnect_backoff_ms
            .map_or(default.reconnect_backoff_initial, DurationMs::duration),
        reconnect_backoff_max: fields
            .reconnect_backoff_max_ms
            .map_or(default.reconnect_backoff_max, DurationMs::duration),
        buffer_pool_capacity: fields
            .buffer_pool_capacity
            .unwrap_or(default.buffer_pool_capacity),
        allow_auto_topic_creation: fields.allow_auto_topic_creation,
    }
}

fn socket_config_from_fields(fields: &ConnectionConfigFields) -> crate::wire::SocketConfig {
    crate::wire::SocketConfig {
        send_buffer_bytes: positive_i32_to_usize(fields.send_buffer_bytes),
        receive_buffer_bytes: positive_i32_to_usize(fields.receive_buffer_bytes),
        tcp_nodelay: fields.socket_tcp_nodelay,
        tcp_keepalive: None,
        tcp_notsent_lowat_bytes: fields.socket_tcp_notsent_lowat_bytes,
        tcp_quickack: fields.socket_tcp_quickack,
        tcp_user_timeout_ms: fields.socket_tcp_user_timeout_ms.map(DurationMs::duration),
        tcp_congestion: fields.socket_tcp_congestion.map(to_wire_congestion),
        reuse_address: fields.socket_reuse_address,
    }
}

fn metadata_recovery_strategy(value: &str) -> crate::wire::MetadataRecoveryStrategy {
    match value {
        "none" => crate::wire::MetadataRecoveryStrategy::None,
        _ => crate::wire::MetadataRecoveryStrategy::Rebootstrap,
    }
}

fn tls_config_from_fields(fields: &ConnectionConfigFields) -> crate::wire::TlsConfig {
    crate::wire::TlsConfig {
        truststore_location: fields.ssl_truststore_location.clone(),
        truststore_password: fields.ssl_truststore_password.clone(),
        truststore_certificates: fields.ssl_truststore_certificates.clone(),
        truststore_type: fields.ssl_truststore_type.clone(),
        keystore_location: fields.ssl_keystore_location.clone(),
        keystore_password: fields.ssl_keystore_password.clone(),
        keystore_key: fields.ssl_keystore_key.clone(),
        keystore_certificate_chain: fields.ssl_keystore_certificate_chain.clone(),
        keystore_type: fields.ssl_keystore_type.clone(),
        key_password: fields.ssl_key_password.clone(),
        endpoint_identification_algorithm: fields.ssl_endpoint_identification_algorithm.clone(),
        protocol: fields.ssl_protocol.clone(),
        enabled_protocols: fields.ssl_enabled_protocols.clone(),
        cipher_suites: fields.ssl_cipher_suites.clone(),
    }
}

fn sasl_config_from_fields(
    fields: &ConnectionConfigFields,
    mechanism: Option<crate::wire::SaslMechanism>,
) -> crate::wire::SaslConfig {
    crate::wire::SaslConfig {
        mechanism,
        jaas_config: fields.sasl_jaas_config.clone(),
        client_authenticator: None,
        client_authenticator_factory: None,
        login_callback_handler_class: fields.sasl_login_callback_handler_class.clone(),
        client_callback_handler_class: fields.sasl_client_callback_handler_class.clone(),
        kerberos_service_name: fields.sasl_kerberos_service_name.clone(),
        kerberos_kinit_cmd: fields.sasl_kerberos_kinit_cmd.clone(),
        kerberos_ticket_renew_window_factor: fields.sasl_kerberos_ticket_renew_window_factor,
        kerberos_ticket_renew_jitter: fields.sasl_kerberos_ticket_renew_jitter,
        kerberos_min_time_before_relogin: fields.sasl_kerberos_min_time_before_relogin.duration(),
        oauthbearer_token_endpoint_url: fields.sasl_oauthbearer_token_endpoint_url.clone(),
        oauthbearer_assertion_file: fields.sasl_oauthbearer_assertion_file.clone(),
        oauthbearer_assertion_private_key_file: fields
            .sasl_oauthbearer_assertion_private_key_file
            .clone(),
        oauthbearer_assertion_private_key_passphrase: fields
            .sasl_oauthbearer_assertion_private_key_passphrase
            .clone(),
        oauthbearer_assertion_template_file: fields
            .sasl_oauthbearer_assertion_template_file
            .clone(),
        oauthbearer_assertion_algorithm: fields.sasl_oauthbearer_assertion_algorithm.clone(),
        oauthbearer_assertion_claim_aud: fields.sasl_oauthbearer_assertion_claim_aud.clone(),
        oauthbearer_assertion_claim_iss: fields.sasl_oauthbearer_assertion_claim_iss.clone(),
        oauthbearer_assertion_claim_sub: fields.sasl_oauthbearer_assertion_claim_sub.clone(),
        oauthbearer_assertion_claim_exp: seconds_from_i32(
            fields.sasl_oauthbearer_assertion_claim_exp_seconds,
        ),
        oauthbearer_assertion_claim_nbf: seconds_from_i32(
            fields.sasl_oauthbearer_assertion_claim_nbf_seconds,
        ),
        oauthbearer_assertion_claim_jti_include: fields
            .sasl_oauthbearer_assertion_claim_jti_include,
        oauthbearer_client_id: fields.sasl_oauthbearer_client_credentials_client_id.clone(),
        oauthbearer_client_secret: fields
            .sasl_oauthbearer_client_credentials_client_secret
            .clone(),
        oauthbearer_scope: fields.sasl_oauthbearer_scope.clone(),
        login_connect_timeout: fields
            .sasl_login_connect_timeout_ms
            .map(DurationMs::duration),
        login_read_timeout: fields.sasl_login_read_timeout_ms.map(DurationMs::duration),
        login_refresh_window_factor: fields.sasl_login_refresh_window_factor,
        login_refresh_window_jitter: fields.sasl_login_refresh_window_jitter,
        login_refresh_min_period: seconds_from_i32(fields.sasl_login_refresh_min_period_seconds),
        login_refresh_buffer: seconds_from_i32(fields.sasl_login_refresh_buffer_seconds),
        login_retry_backoff: fields.sasl_login_retry_backoff_ms.duration(),
        login_retry_backoff_max: fields.sasl_login_retry_backoff_max_ms.duration(),
    }
}

fn seconds_from_i32(value: i32) -> std::time::Duration {
    positive_i32_to_u64(value)
        .map(std::time::Duration::from_secs)
        .unwrap_or_default()
}

const fn to_wire_congestion(value: TcpCongestionControl) -> crate::wire::TcpCongestionControl {
    match value {
        TcpCongestionControl::Bbr => crate::wire::TcpCongestionControl::Bbr,
        TcpCongestionControl::Cubic => crate::wire::TcpCongestionControl::Cubic,
        TcpCongestionControl::Reno => crate::wire::TcpCongestionControl::Reno,
    }
}

fn positive_i32_to_usize(value: i32) -> Option<usize> {
    u32::try_from(value)
        .ok()
        .and_then(|value| usize::try_from(value).ok())
        .filter(|value| *value > 0)
}

fn positive_i32_to_u64(value: i32) -> Option<u64> {
    u32::try_from(value).ok().map(u64::from)
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::expect_used,
        clippy::missing_assert_message,
        reason = "Unit test fixtures fail fastest with contextual expect calls."
    )]

    use super::{AdminConfig, ConsumerConfig, DurationMs, ProducerConfig, TcpCongestionControl};
    use crate::wire;

    #[test]
    fn consumer_connection_config_maps_socket_fields_without_producer_only_capacity() {
        let config = ConsumerConfig::builder()
            .bootstrap_servers("localhost:9092")
            .group_id("group-a")
            .send_buffer_bytes(131_072)
            .receive_buffer_bytes(262_144)
            .request_timeout_ms(DurationMs::from_millis(9_000))
            .metadata_max_age_ms(DurationMs::from_millis(10_000))
            .socket_connection_setup_timeout_ms(DurationMs::from_millis(2_000))
            .socket_connection_setup_timeout_max_ms(DurationMs::from_millis(5_000))
            .socket_tcp_nodelay(false)
            .socket_tcp_notsent_lowat_bytes(Some(32_768))
            .socket_tcp_quickack(Some(false))
            .socket_tcp_user_timeout_ms(Some(DurationMs::from_millis(12_000)))
            .socket_tcp_congestion(Some(TcpCongestionControl::Cubic))
            .socket_reuse_address(false)
            .socket_read_buffer_capacity_bytes(Some(65_536))
            .build()
            .expect("consumer config")
            .to_connection_config();

        assert_eq!(config.socket.send_buffer_bytes, Some(131_072));
        assert_eq!(config.socket.receive_buffer_bytes, Some(262_144));
        assert_eq!(config.request_timeout, std::time::Duration::from_secs(9));
        assert_eq!(config.metadata_max_age, std::time::Duration::from_secs(10));
        assert_eq!(
            config.socket_connection_setup_timeout,
            std::time::Duration::from_secs(2)
        );
        assert_eq!(
            config.socket_connection_setup_timeout_max,
            std::time::Duration::from_secs(5)
        );
        assert!(!config.socket.tcp_nodelay);
        assert_eq!(config.socket.tcp_notsent_lowat_bytes, Some(32_768));
        assert_eq!(config.socket.tcp_quickack, Some(false));
        assert_eq!(
            config.socket.tcp_user_timeout_ms,
            Some(std::time::Duration::from_secs(12))
        );
        assert_eq!(
            config.socket.tcp_congestion,
            Some(wire::TcpCongestionControl::Cubic)
        );
        assert!(!config.socket.reuse_address);
        assert_eq!(config.read_buffer_capacity, Some(65_536));
        assert_eq!(
            config.max_in_flight_requests_per_connection,
            wire::ConnectionConfig::default().max_in_flight_requests_per_connection
        );
        assert_eq!(
            config.broker_queue_capacity,
            wire::ConnectionConfig::default().broker_queue_capacity
        );
        assert_eq!(
            config.buffer_pool_capacity,
            wire::ConnectionConfig::default().buffer_pool_capacity
        );
    }

    #[test]
    fn admin_connection_config_maps_socket_fields_and_reno_congestion() {
        let config = AdminConfig::builder()
            .bootstrap_servers("localhost:9092")
            .send_buffer_bytes(65_536)
            .receive_buffer_bytes(98_304)
            .request_timeout_ms(DurationMs::from_millis(4_000))
            .metadata_max_age_ms(DurationMs::from_millis(6_000))
            .socket_connection_setup_timeout_ms(DurationMs::from_millis(1_000))
            .socket_connection_setup_timeout_max_ms(DurationMs::from_millis(8_000))
            .socket_tcp_nodelay(true)
            .socket_tcp_notsent_lowat_bytes(Some(16_384))
            .socket_tcp_quickack(Some(true))
            .socket_tcp_user_timeout_ms(Some(DurationMs::from_millis(3_000)))
            .socket_tcp_congestion(Some(TcpCongestionControl::Reno))
            .socket_reuse_address(true)
            .socket_read_buffer_capacity_bytes(Some(32_768))
            .build()
            .expect("admin config")
            .to_connection_config();

        assert_eq!(config.socket.send_buffer_bytes, Some(65_536));
        assert_eq!(config.socket.receive_buffer_bytes, Some(98_304));
        assert_eq!(config.request_timeout, std::time::Duration::from_secs(4));
        assert_eq!(config.metadata_max_age, std::time::Duration::from_secs(6));
        assert_eq!(
            config.socket_connection_setup_timeout,
            std::time::Duration::from_secs(1)
        );
        assert_eq!(
            config.socket_connection_setup_timeout_max,
            std::time::Duration::from_secs(8)
        );
        assert!(config.socket.tcp_nodelay);
        assert_eq!(config.socket.tcp_notsent_lowat_bytes, Some(16_384));
        assert_eq!(config.socket.tcp_quickack, Some(true));
        assert_eq!(
            config.socket.tcp_user_timeout_ms,
            Some(std::time::Duration::from_secs(3))
        );
        assert_eq!(
            config.socket.tcp_congestion,
            Some(wire::TcpCongestionControl::Reno)
        );
        assert!(config.socket.reuse_address);
        assert_eq!(config.read_buffer_capacity, Some(32_768));
    }

    #[test]
    fn producer_connection_config_maps_java_backoff_and_idle_fields() {
        let config = ProducerConfig::builder()
            .bootstrap_servers("localhost:9092")
            .connections_max_idle_ms(DurationMs::from_millis(11_000))
            .reconnect_backoff_ms(DurationMs::from_millis(17))
            .reconnect_backoff_max_ms(DurationMs::from_millis(71))
            .build()
            .expect("producer config")
            .to_connection_config();

        assert_eq!(
            config.connections_max_idle,
            std::time::Duration::from_secs(11)
        );
        assert_eq!(
            config.reconnect_backoff_initial,
            std::time::Duration::from_millis(17)
        );
        assert_eq!(
            config.reconnect_backoff_max,
            std::time::Duration::from_millis(71)
        );
    }

    #[test]
    fn producer_connection_config_maps_metadata_recovery_fields() {
        let config = ProducerConfig::builder()
            .bootstrap_servers("localhost:9092")
            .metadata_max_age_ms(DurationMs::from_millis(31))
            .metadata_max_idle_ms(DurationMs::from_millis(37))
            .metadata_recovery_strategy("none")
            .metadata_recovery_rebootstrap_trigger_ms(DurationMs::from_millis(41))
            .build()
            .expect("producer config")
            .to_connection_config();

        assert_eq!(
            config.metadata_max_age,
            std::time::Duration::from_millis(31)
        );
        assert_eq!(
            config.metadata_max_idle,
            std::time::Duration::from_millis(37)
        );
        assert_eq!(
            config.metadata_recovery_strategy,
            wire::MetadataRecoveryStrategy::None
        );
        assert_eq!(
            config.metadata_rebootstrap_trigger,
            std::time::Duration::from_millis(41)
        );
    }

    #[test]
    fn positive_i32_to_usize_rejects_non_positive_values() {
        assert_eq!(super::positive_i32_to_usize(-1), None);
        assert_eq!(super::positive_i32_to_usize(0), None);
        assert_eq!(super::positive_i32_to_usize(7), Some(7));
    }
}

kafka_config! {
    #[client(Consumer)]
    pub ConsumerConfig {
        #[key("bootstrap.servers")]
        #[required]
        #[kafka_type("list")]
        #[kafka_default("(none)")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/consumer-configs/#consumerconfigs_bootstrap.servers")]
        #[comment("Initial Kafka broker list used to discover the full cluster.")]
        bootstrap_servers: ConfigList,

        #[key("group.id")]
        #[default(String::new())]
        #[kafka_type("string")]
        #[kafka_default("null")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/consumer-configs/#consumerconfigs_group.id")]
        #[comment("Consumer group id used for group management and offset ownership.")]
        group_id: String,

        #[key("client.id")]
        #[default(String::new())]
        #[kafka_type("string")]
        #[kafka_default("\"\"")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/consumer-configs/#consumerconfigs_client.id")]
        #[comment("Logical client name sent to brokers for quota, logging, and metrics attribution.")]
        client_id: String,

        #[key("group.protocol")]
        #[default(String::from("classic"))]
        #[kafka_type("string")]
        #[kafka_default("classic")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/consumer-configs/#consumerconfigs_group.protocol")]
        #[comment("Consumer group protocol selection.")]
        group_protocol: String,

        #[key("fetch.min.bytes")]
        #[default(1_i32)]
        #[kafka_type("int")]
        #[kafka_default("1")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/consumer-configs/#consumerconfigs_fetch.min.bytes")]
        #[comment("Minimum data the broker should return for a fetch request.")]
        fetch_min_bytes: i32,

        #[key("heartbeat.interval.ms")]
        #[default(DurationMs::from_millis(3_000))]
        #[kafka_type("int")]
        #[kafka_default("3000 (3 seconds)")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/consumer-configs/#consumerconfigs_heartbeat.interval.ms")]
        #[comment("Heartbeat cadence for classic consumer group membership.")]
        heartbeat_interval_ms: DurationMs,

        #[key("max.partition.fetch.bytes")]
        #[default(ByteSize::new(1_048_576))]
        #[kafka_type("int")]
        #[kafka_default("1048576 (1 mebibyte)")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/consumer-configs/#consumerconfigs_max.partition.fetch.bytes")]
        #[comment("Maximum bytes fetched per partition in one request.")]
        max_partition_fetch_bytes: ByteSize,

        #[key("session.timeout.ms")]
        #[default(DurationMs::from_millis(45_000))]
        #[kafka_type("int")]
        #[kafka_default("45000 (45 seconds)")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/consumer-configs/#consumerconfigs_session.timeout.ms")]
        #[comment("Consumer group session timeout used to detect failed members.")]
        session_timeout_ms: DurationMs,

        #[key("allow.auto.create.topics")]
        #[default(true)]
        #[kafka_type("boolean")]
        #[kafka_default("true")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/consumer-configs/#consumerconfigs_allow.auto.create.topics")]
        #[comment("Whether topic subscription may trigger broker-side auto topic creation.")]
        allow_auto_create_topics: bool,

        #[key("client.dns.lookup")]
        #[default(String::from("use_all_dns_ips"))]
        #[kafka_type("string")]
        #[kafka_default("use_all_dns_ips")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/consumer-configs/#consumerconfigs_client.dns.lookup")]
        #[comment("Kafka-compatible DNS lookup strategy for bootstrap and broker hostnames.")]
        client_dns_lookup: String,

        #[key("client.rack")]
        #[default(String::new())]
        #[kafka_type("string")]
        #[kafka_default("\"\"")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/consumer-configs/#consumerconfigs_client.rack")]
        #[comment("Rack identifier used by Kafka for rack-aware fetch behavior.")]
        client_rack: String,

        #[key("connections.max.idle.ms")]
        #[default(DurationMs::from_millis(540_000))]
        #[kafka_type("long")]
        #[kafka_default("540000 (9 minutes)")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/consumer-configs/#consumerconfigs_connections.max.idle.ms")]
        #[comment("Idle connection lifetime before the consumer closes a broker connection.")]
        connections_max_idle_ms: DurationMs,

        #[key("default.api.timeout.ms")]
        #[default(DurationMs::from_millis(60_000))]
        #[kafka_type("int")]
        #[kafka_default("60000 (1 minute)")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/consumer-configs/#consumerconfigs_default.api.timeout.ms")]
        #[comment("Default timeout for consumer APIs without an explicit timeout.")]
        default_api_timeout_ms: DurationMs,

        #[key("enable.auto.commit")]
        #[default(true)]
        #[kafka_type("boolean")]
        #[kafka_default("true")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/consumer-configs/#consumerconfigs_enable.auto.commit")]
        #[comment("Whether the consumer should periodically commit offsets in the background.")]
        enable_auto_commit: bool,

        #[key("exclude.internal.topics")]
        #[default(true)]
        #[kafka_type("boolean")]
        #[kafka_default("true")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/consumer-configs/#consumerconfigs_exclude.internal.topics")]
        #[comment("Whether internal topics are excluded from wildcard subscriptions.")]
        exclude_internal_topics: bool,

        #[key("fetch.max.bytes")]
        #[default(ByteSize::new(52_428_800))]
        #[kafka_type("int")]
        #[kafka_default("52428800 (50 mebibytes)")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/consumer-configs/#consumerconfigs_fetch.max.bytes")]
        #[comment("Maximum bytes returned for one fetch request.")]
        fetch_max_bytes: ByteSize,

        #[key("group.instance.id")]
        #[default(String::new())]
        #[kafka_type("string")]
        #[kafka_default("null")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/consumer-configs/#consumerconfigs_group.instance.id")]
        #[comment("Static membership instance id; empty means unset in this Rust config.")]
        group_instance_id: String,

        #[key("group.remote.assignor")]
        #[default(String::new())]
        #[kafka_type("string")]
        #[kafka_default("null")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/consumer-configs/#consumerconfigs_group.remote.assignor")]
        #[comment("Server-side assignor name; empty means unset in this Rust config.")]
        group_remote_assignor: String,

        #[key("isolation.level")]
        #[default(String::from("read_uncommitted"))]
        #[kafka_type("string")]
        #[kafka_default("read_uncommitted")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/consumer-configs/#consumerconfigs_isolation.level")]
        #[comment("Transactional visibility mode for consumed records.")]
        isolation_level: String,

        #[key("max.poll.interval.ms")]
        #[default(DurationMs::from_millis(300_000))]
        #[kafka_type("int")]
        #[kafka_default("300000 (5 minutes)")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/consumer-configs/#consumerconfigs_max.poll.interval.ms")]
        #[comment("Maximum delay between poll calls before the consumer is considered failed.")]
        max_poll_interval_ms: DurationMs,

        #[key("max.poll.records")]
        #[default(500_i32)]
        #[kafka_type("int")]
        #[kafka_default("500")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/consumer-configs/#consumerconfigs_max.poll.records")]
        #[comment("Maximum number of records returned from one poll call.")]
        max_poll_records: i32,

        #[key("receive.buffer.bytes")]
        #[default(65_536_i32)]
        #[kafka_type("int")]
        #[kafka_default("65536 (64 kibibytes)")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/consumer-configs/#consumerconfigs_receive.buffer.bytes")]
        #[comment("Socket receive buffer size for consumer network connections.")]
        receive_buffer_bytes: i32,

        #[key("request.timeout.ms")]
        #[default(DurationMs::from_millis(30_000))]
        #[kafka_type("int")]
        #[kafka_default("30000 (30 seconds)")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/consumer-configs/#consumerconfigs_request.timeout.ms")]
        #[comment("Maximum time the consumer waits for a broker response.")]
        request_timeout_ms: DurationMs,

        #[key("send.buffer.bytes")]
        #[default(131_072_i32)]
        #[kafka_type("int")]
        #[kafka_default("131072 (128 kibibytes)")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/consumer-configs/#consumerconfigs_send.buffer.bytes")]
        #[comment("Socket send buffer size for consumer network connections.")]
        send_buffer_bytes: i32,

        #[key("socket.connection.setup.timeout.max.ms")]
        #[default(DurationMs::from_millis(30_000))]
        #[kafka_type("long")]
        #[kafka_default("30000 (30 seconds)")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/consumer-configs/#consumerconfigs_socket.connection.setup.timeout.max.ms")]
        #[comment("Maximum socket connection setup timeout after exponential backoff.")]
        socket_connection_setup_timeout_max_ms: DurationMs,

        #[key("socket.connection.setup.timeout.ms")]
        #[default(DurationMs::from_millis(10_000))]
        #[kafka_type("long")]
        #[kafka_default("10000 (10 seconds)")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/consumer-configs/#consumerconfigs_socket.connection.setup.timeout.ms")]
        #[comment("Initial socket connection setup timeout.")]
        socket_connection_setup_timeout_ms: DurationMs,

        #[key("auto.commit.interval.ms")]
        #[default(DurationMs::from_millis(5_000))]
        #[kafka_type("int")]
        #[kafka_default("5000 (5 seconds)")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/consumer-configs/#consumerconfigs_auto.commit.interval.ms")]
        #[comment("Interval between background offset commits when auto commit is enabled.")]
        auto_commit_interval_ms: DurationMs,

        #[key("check.crcs")]
        #[default(true)]
        #[kafka_type("boolean")]
        #[kafka_default("true")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/consumer-configs/#consumerconfigs_check.crcs")]
        #[comment("Whether consumed record checksums are verified.")]
        check_crcs: bool,

        #[key("enable.metrics.push")]
        #[default(true)]
        #[kafka_type("boolean")]
        #[kafka_default("true")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/consumer-configs/#consumerconfigs_enable.metrics.push")]
        #[comment("Whether consumer client metrics may be pushed to the Kafka cluster.")]
        enable_metrics_push: bool,

        #[key("fetch.max.wait.ms")]
        #[default(DurationMs::from_millis(500))]
        #[kafka_type("int")]
        #[kafka_default("500")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/consumer-configs/#consumerconfigs_fetch.max.wait.ms")]
        #[comment("Maximum broker wait before returning a fetch response.")]
        fetch_max_wait_ms: DurationMs,

        #[key("metadata.max.age.ms")]
        #[default(DurationMs::from_millis(300_000))]
        #[kafka_type("long")]
        #[kafka_default("300000 (5 minutes)")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/consumer-configs/#consumerconfigs_metadata.max.age.ms")]
        #[comment("Maximum metadata cache age before forced refresh.")]
        metadata_max_age_ms: DurationMs,

        #[key("metadata.recovery.rebootstrap.trigger.ms")]
        #[default(DurationMs::from_millis(300_000))]
        #[kafka_type("long")]
        #[kafka_default("300000 (5 minutes)")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/consumer-configs/#consumerconfigs_metadata.recovery.rebootstrap.trigger.ms")]
        #[comment("Time without usable metadata before rebootstrap recovery may trigger.")]
        metadata_recovery_rebootstrap_trigger_ms: DurationMs,

        #[key("metadata.recovery.strategy")]
        #[default(String::from("rebootstrap"))]
        #[kafka_type("string")]
        #[kafka_default("rebootstrap")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/consumer-configs/#consumerconfigs_metadata.recovery.strategy")]
        #[comment("Recovery strategy used when no known broker is available.")]
        metadata_recovery_strategy: String,

        #[key("security.protocol")]
        #[default(String::from("PLAINTEXT"))]
        #[kafka_type("string")]
        #[kafka_default("PLAINTEXT")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/consumer-configs/#consumerconfigs_security.protocol")]
        #[comment("Kafka-compatible broker security protocol: PLAINTEXT, SSL, SASL_PLAINTEXT, or SASL_SSL.")]
        security_protocol: String,

        #[key("ssl.truststore.location")]
        #[default(None)]
        #[kafka_type("string")]
        #[kafka_default("null")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/consumer-configs/#consumerconfigs_ssl.truststore.location")]
        #[feature("tls-rustls")]
        #[comment("Trust material location used by TLS broker connections.")]
        ssl_truststore_location: Option<String>,

        #[key("ssl.truststore.password")]
        #[default(None)]
        #[kafka_type("password")]
        #[kafka_default("null")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/consumer-configs/#consumerconfigs_ssl.truststore.password")]
        #[feature("tls-rustls")]
        #[comment("Password for configured TLS trust material when the backend format requires it.")]
        ssl_truststore_password: Option<String>,

        #[key("ssl.truststore.certificates")]
        #[default(None)]
        #[kafka_type("password")]
        #[kafka_default("null")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/consumer-configs/#consumerconfigs_ssl.truststore.certificates")]
        #[feature("tls-rustls")]
        #[comment("Inline PEM trusted certificates used by TLS broker connections.")]
        ssl_truststore_certificates: Option<String>,

        #[key("ssl.truststore.type")]
        #[default(None)]
        #[kafka_type("string")]
        #[kafka_default("JKS")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/consumer-configs/#consumerconfigs_ssl.truststore.type")]
        #[feature("tls-rustls")]
        #[comment("Truststore format; rustls backend supports PEM, JKS, and PKCS12 material.")]
        ssl_truststore_type: Option<String>,

        #[key("ssl.keystore.location")]
        #[default(None)]
        #[kafka_type("string")]
        #[kafka_default("null")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/consumer-configs/#consumerconfigs_ssl.keystore.location")]
        #[feature("tls-rustls")]
        #[comment("Client identity material location used by mTLS broker connections.")]
        ssl_keystore_location: Option<String>,

        #[key("ssl.keystore.password")]
        #[default(None)]
        #[kafka_type("password")]
        #[kafka_default("null")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/consumer-configs/#consumerconfigs_ssl.keystore.password")]
        #[feature("tls-rustls")]
        #[comment("Password for configured TLS client identity material when the backend format requires it.")]
        ssl_keystore_password: Option<String>,

        #[key("ssl.keystore.key")]
        #[default(None)]
        #[kafka_type("password")]
        #[kafka_default("null")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/consumer-configs/#consumerconfigs_ssl.keystore.key")]
        #[feature("tls-rustls")]
        #[comment("Inline PEM private key for TLS client authentication.")]
        ssl_keystore_key: Option<String>,

        #[key("ssl.keystore.certificate.chain")]
        #[default(None)]
        #[kafka_type("password")]
        #[kafka_default("null")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/consumer-configs/#consumerconfigs_ssl.keystore.certificate.chain")]
        #[feature("tls-rustls")]
        #[comment("Inline PEM certificate chain for TLS client authentication.")]
        ssl_keystore_certificate_chain: Option<String>,

        #[key("ssl.keystore.type")]
        #[default(None)]
        #[kafka_type("string")]
        #[kafka_default("JKS")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/consumer-configs/#consumerconfigs_ssl.keystore.type")]
        #[feature("tls-rustls")]
        #[comment("Keystore format; rustls backend supports PEM, JKS, and PKCS12 material.")]
        ssl_keystore_type: Option<String>,

        #[key("ssl.key.password")]
        #[default(None)]
        #[kafka_type("password")]
        #[kafka_default("null")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/consumer-configs/#consumerconfigs_ssl.key.password")]
        #[feature("tls-rustls")]
        #[comment("Password for encrypted TLS private key material.")]
        ssl_key_password: Option<String>,

        #[key("ssl.endpoint.identification.algorithm")]
        #[default(Some(String::from("https")))]
        #[kafka_type("string")]
        #[kafka_default("https")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/consumer-configs/#consumerconfigs_ssl.endpoint.identification.algorithm")]
        #[feature("tls-rustls")]
        #[comment("Hostname verification algorithm; empty string disables endpoint identification explicitly.")]
        ssl_endpoint_identification_algorithm: Option<String>,

        #[key("ssl.protocol")]
        #[default(String::from("TLSv1.3"))]
        #[kafka_type("string")]
        #[kafka_default("TLSv1.3")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/consumer-configs/#consumerconfigs_ssl.protocol")]
        #[feature("tls-rustls")]
        #[comment("Preferred TLS protocol version.")]
        ssl_protocol: String,

        #[key("ssl.enabled.protocols")]
        #[default(Some(String::from("TLSv1.2,TLSv1.3")))]
        #[kafka_type("list")]
        #[kafka_default("TLSv1.2,TLSv1.3")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/consumer-configs/#consumerconfigs_ssl.enabled.protocols")]
        #[feature("tls-rustls")]
        #[comment("Comma-separated TLS protocol versions enabled for negotiation.")]
        ssl_enabled_protocols: Option<String>,

        #[key("ssl.cipher.suites")]
        #[default(None)]
        #[kafka_type("list")]
        #[kafka_default("null")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/consumer-configs/#consumerconfigs_ssl.cipher.suites")]
        #[feature("tls-rustls")]
        #[comment("Comma-separated TLS cipher suite names requested by the user.")]
        ssl_cipher_suites: Option<String>,

        #[key("sasl.mechanism")]
        #[default(String::from("GSSAPI"))]
        #[kafka_type("string")]
        #[kafka_default("GSSAPI")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/consumer-configs/#consumerconfigs_sasl.mechanism")]
        #[feature("sasl")]
        #[comment("Kafka SASL mechanism: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, OAUTHBEARER, or GSSAPI.")]
        sasl_mechanism: String,

        #[key("sasl.jaas.config")]
        #[default(None)]
        #[kafka_type("password")]
        #[kafka_default("null")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/consumer-configs/#consumerconfigs_sasl.jaas.config")]
        #[feature("sasl")]
        #[comment("Java-compatible JAAS login module options used to derive Rust SASL credentials.")]
        sasl_jaas_config: Option<String>,

        #[key("sasl.login.connect.timeout.ms")]
        #[default(None)]
        #[kafka_type("int")]
        #[kafka_default("null")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/consumer-configs/#consumerconfigs_sasl.login.connect.timeout.ms")]
        #[feature("sasl")]
        #[comment("External SASL login provider connection timeout.")]
        sasl_login_connect_timeout_ms: Option<DurationMs>,

        #[key("sasl.login.read.timeout.ms")]
        #[default(None)]
        #[kafka_type("int")]
        #[kafka_default("null")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/consumer-configs/#consumerconfigs_sasl.login.read.timeout.ms")]
        #[feature("sasl")]
        #[comment("External SASL login provider read timeout.")]
        sasl_login_read_timeout_ms: Option<DurationMs>,

        #[key("sasl.login.refresh.window.factor")]
        #[default(0.8_f64)]
        #[kafka_type("double")]
        #[kafka_default("0.8")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/consumer-configs/#consumerconfigs_sasl.login.refresh.window.factor")]
        #[feature("sasl")]
        #[comment("OAuth token refresh point as a factor of the token lifetime.")]
        sasl_login_refresh_window_factor: f64,

        #[key("sasl.login.refresh.window.jitter")]
        #[default(0.05_f64)]
        #[kafka_type("double")]
        #[kafka_default("0.05")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/consumer-configs/#consumerconfigs_sasl.login.refresh.window.jitter")]
        #[feature("sasl")]
        #[comment("OAuth token refresh jitter factor.")]
        sasl_login_refresh_window_jitter: f64,

        #[key("sasl.login.refresh.min.period.seconds")]
        #[default(60_i32)]
        #[kafka_type("short")]
        #[kafka_default("60")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/consumer-configs/#consumerconfigs_sasl.login.refresh.min.period.seconds")]
        #[feature("sasl")]
        #[comment("Minimum OAuth token lifetime before attempting refresh.")]
        sasl_login_refresh_min_period_seconds: i32,

        #[key("sasl.login.refresh.buffer.seconds")]
        #[default(300_i32)]
        #[kafka_type("short")]
        #[kafka_default("300")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/consumer-configs/#consumerconfigs_sasl.login.refresh.buffer.seconds")]
        #[feature("sasl")]
        #[comment("OAuth token expiration buffer kept before refresh.")]
        sasl_login_refresh_buffer_seconds: i32,

        #[key("sasl.login.retry.backoff.ms")]
        #[default(DurationMs::from_millis(100))]
        #[kafka_type("long")]
        #[kafka_default("100")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/consumer-configs/#consumerconfigs_sasl.login.retry.backoff.ms")]
        #[feature("sasl")]
        #[comment("Initial OAuth login retry backoff.")]
        sasl_login_retry_backoff_ms: DurationMs,

        #[key("sasl.login.retry.backoff.max.ms")]
        #[default(DurationMs::from_millis(10_000))]
        #[kafka_type("long")]
        #[kafka_default("10000")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/consumer-configs/#consumerconfigs_sasl.login.retry.backoff.max.ms")]
        #[feature("sasl")]
        #[comment("Maximum OAuth login retry backoff.")]
        sasl_login_retry_backoff_max_ms: DurationMs,

        #[key("sasl.oauthbearer.token.endpoint.url")]
        #[default(None)]
        #[kafka_type("string")]
        #[kafka_default("null")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/consumer-configs/#consumerconfigs_sasl.oauthbearer.token.endpoint.url")]
        #[feature("sasl")]
        #[comment("File token endpoint for SASL/OAUTHBEARER static JWT retrieval.")]
        sasl_oauthbearer_token_endpoint_url: Option<String>,

        #[key("sasl.oauthbearer.assertion.file")]
        #[default(None)]
        #[kafka_type("string")]
        #[kafka_default("null")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/consumer-configs/#consumerconfigs_sasl.oauthbearer.assertion.file")]
        #[feature("sasl")]
        #[comment("Pre-generated JWT assertion file for SASL/OAUTHBEARER token retrieval.")]
        sasl_oauthbearer_assertion_file: Option<String>,

        #[key("sasl.oauthbearer.client.credentials.client.id")]
        #[default(None)]
        #[kafka_type("string")]
        #[kafka_default("null")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/consumer-configs/#consumerconfigs_sasl.oauthbearer.client.credentials.client.id")]
        #[feature("sasl")]
        #[comment("OAuth client credentials client id for SASL/OAUTHBEARER token retrieval.")]
        sasl_oauthbearer_client_credentials_client_id: Option<String>,

        #[key("sasl.oauthbearer.client.credentials.client.secret")]
        #[default(None)]
        #[kafka_type("password")]
        #[kafka_default("null")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/consumer-configs/#consumerconfigs_sasl.oauthbearer.client.credentials.client.secret")]
        #[feature("sasl")]
        #[comment("OAuth client credentials client secret for SASL/OAUTHBEARER token retrieval.")]
        sasl_oauthbearer_client_credentials_client_secret: Option<String>,

        #[key("sasl.oauthbearer.scope")]
        #[default(None)]
        #[kafka_type("string")]
        #[kafka_default("null")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/consumer-configs/#consumerconfigs_sasl.oauthbearer.scope")]
        #[feature("sasl")]
        #[comment("Optional OAuth scope for SASL/OAUTHBEARER token retrieval.")]
        sasl_oauthbearer_scope: Option<String>,

        #[key("sasl.oauthbearer.assertion.private.key.file")]
        #[default(None)]
        #[kafka_type("string")]
        #[kafka_default("null")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/consumer-configs/#consumerconfigs_sasl.oauthbearer.assertion.private.key.file")]
        #[feature("sasl")]
        #[comment("PEM private key file used to sign SASL/OAUTHBEARER JWT assertions.")]
        sasl_oauthbearer_assertion_private_key_file: Option<String>,

        #[key("sasl.oauthbearer.assertion.private.key.passphrase")]
        #[default(None)]
        #[kafka_type("password")]
        #[kafka_default("null")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/consumer-configs/#consumerconfigs_sasl.oauthbearer.assertion.private.key.passphrase")]
        #[feature("sasl")]
        #[comment("Passphrase for the SASL/OAUTHBEARER assertion private key file.")]
        sasl_oauthbearer_assertion_private_key_passphrase: Option<String>,

        #[key("sasl.oauthbearer.assertion.template.file")]
        #[default(None)]
        #[kafka_type("string")]
        #[kafka_default("null")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/consumer-configs/#consumerconfigs_sasl.oauthbearer.assertion.template.file")]
        #[feature("sasl")]
        #[comment("JSON header and claim template for generated SASL/OAUTHBEARER JWT assertions.")]
        sasl_oauthbearer_assertion_template_file: Option<String>,

        #[key("sasl.oauthbearer.assertion.algorithm")]
        #[default(String::from("RS256"))]
        #[kafka_type("string")]
        #[kafka_default("DEFAULT_SASL_OAUTHBEARER_ASSERTION_ALGORITHM")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/consumer-configs/#consumerconfigs_sasl.oauthbearer.assertion.algorithm")]
        #[feature("sasl")]
        #[comment("Signing algorithm for generated SASL/OAUTHBEARER JWT assertions.")]
        sasl_oauthbearer_assertion_algorithm: String,

        #[key("sasl.oauthbearer.assertion.claim.aud")]
        #[default(None)]
        #[kafka_type("string")]
        #[kafka_default("null")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/consumer-configs/#consumerconfigs_sasl.oauthbearer.assertion.claim.aud")]
        #[feature("sasl")]
        #[comment("Audience claim for generated SASL/OAUTHBEARER JWT assertions.")]
        sasl_oauthbearer_assertion_claim_aud: Option<String>,

        #[key("sasl.oauthbearer.assertion.claim.iss")]
        #[default(None)]
        #[kafka_type("string")]
        #[kafka_default("null")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/consumer-configs/#consumerconfigs_sasl.oauthbearer.assertion.claim.iss")]
        #[feature("sasl")]
        #[comment("Issuer claim for generated SASL/OAUTHBEARER JWT assertions.")]
        sasl_oauthbearer_assertion_claim_iss: Option<String>,

        #[key("sasl.oauthbearer.assertion.claim.sub")]
        #[default(None)]
        #[kafka_type("string")]
        #[kafka_default("null")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/consumer-configs/#consumerconfigs_sasl.oauthbearer.assertion.claim.sub")]
        #[feature("sasl")]
        #[comment("Subject claim for generated SASL/OAUTHBEARER JWT assertions.")]
        sasl_oauthbearer_assertion_claim_sub: Option<String>,

        #[key("sasl.oauthbearer.assertion.claim.exp.seconds")]
        #[default(60_i32)]
        #[kafka_type("int")]
        #[kafka_default("DEFAULT_SASL_OAUTHBEARER_ASSERTION_CLAIM_EXP_SECONDS")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/consumer-configs/#consumerconfigs_sasl.oauthbearer.assertion.claim.exp.seconds")]
        #[feature("sasl")]
        #[comment("Validity period in seconds for generated SASL/OAUTHBEARER JWT assertions.")]
        sasl_oauthbearer_assertion_claim_exp_seconds: i32,

        #[key("sasl.oauthbearer.assertion.claim.nbf.seconds")]
        #[default(0_i32)]
        #[kafka_type("int")]
        #[kafka_default("DEFAULT_SASL_OAUTHBEARER_ASSERTION_CLAIM_NBF_SECONDS")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/consumer-configs/#consumerconfigs_sasl.oauthbearer.assertion.claim.nbf.seconds")]
        #[feature("sasl")]
        #[comment("Not-before offset in seconds for generated SASL/OAUTHBEARER JWT assertions.")]
        sasl_oauthbearer_assertion_claim_nbf_seconds: i32,

        #[key("sasl.oauthbearer.assertion.claim.jti.include")]
        #[default(false)]
        #[kafka_type("boolean")]
        #[kafka_default("DEFAULT_SASL_OAUTHBEARER_ASSERTION_CLAIM_JTI_INCLUDE")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/consumer-configs/#consumerconfigs_sasl.oauthbearer.assertion.claim.jti.include")]
        #[feature("sasl")]
        #[comment("Whether generated SASL/OAUTHBEARER JWT assertions include a random jti claim.")]
        sasl_oauthbearer_assertion_claim_jti_include: bool,

        #[key("sasl.login.callback.handler.class")]
        #[default(None)]
        #[kafka_type("class")]
        #[kafka_default("null")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/consumer-configs/#consumerconfigs_sasl.login.callback.handler.class")]
        #[feature("sasl")]
        #[comment("Java SASL login callback handler class name retained for explicit compatibility errors.")]
        sasl_login_callback_handler_class: Option<String>,

        #[key("sasl.client.callback.handler.class")]
        #[default(None)]
        #[kafka_type("class")]
        #[kafka_default("null")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/consumer-configs/#consumerconfigs_sasl.client.callback.handler.class")]
        #[feature("sasl")]
        #[comment("Java SASL client callback handler class name retained for explicit compatibility errors.")]
        sasl_client_callback_handler_class: Option<String>,

        #[key("sasl.kerberos.service.name")]
        #[default(None)]
        #[kafka_type("string")]
        #[kafka_default("null")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/consumer-configs/#consumerconfigs_sasl.kerberos.service.name")]
        #[feature("sasl")]
        #[comment("Kerberos service principal name for GSSAPI.")]
        sasl_kerberos_service_name: Option<String>,

        #[key("sasl.kerberos.kinit.cmd")]
        #[default(String::from("/usr/bin/kinit"))]
        #[kafka_type("string")]
        #[kafka_default("SaslConfigs.DEFAULT_KERBEROS_KINIT_CMD")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/consumer-configs/#consumerconfigs_sasl.kerberos.kinit.cmd")]
        #[feature("sasl")]
        #[comment("Kerberos kinit command configured for a future GSSAPI backend.")]
        sasl_kerberos_kinit_cmd: String,

        #[key("sasl.kerberos.ticket.renew.window.factor")]
        #[default(0.8_f64)]
        #[kafka_type("double")]
        #[kafka_default("SaslConfigs.DEFAULT_KERBEROS_TICKET_RENEW_WINDOW_FACTOR")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/consumer-configs/#consumerconfigs_sasl.kerberos.ticket.renew.window.factor")]
        #[feature("sasl")]
        #[comment("Kerberos ticket renew window factor for a future GSSAPI backend.")]
        sasl_kerberos_ticket_renew_window_factor: f64,

        #[key("sasl.kerberos.ticket.renew.jitter")]
        #[default(0.05_f64)]
        #[kafka_type("double")]
        #[kafka_default("SaslConfigs.DEFAULT_KERBEROS_TICKET_RENEW_JITTER")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/consumer-configs/#consumerconfigs_sasl.kerberos.ticket.renew.jitter")]
        #[feature("sasl")]
        #[comment("Kerberos ticket renew jitter for a future GSSAPI backend.")]
        sasl_kerberos_ticket_renew_jitter: f64,

        #[key("sasl.kerberos.min.time.before.relogin")]
        #[default(DurationMs::from_millis(60_000))]
        #[kafka_type("long")]
        #[kafka_default("SaslConfigs.DEFAULT_KERBEROS_MIN_TIME_BEFORE_RELOGIN")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/consumer-configs/#consumerconfigs_sasl.kerberos.min.time.before.relogin")]
        #[feature("sasl")]
        #[comment("Minimum time before Kerberos relogin for a future GSSAPI backend.")]
        sasl_kerberos_min_time_before_relogin: DurationMs,

        #[key("socket.tcp.nodelay")]
        #[default(true)]
        #[kafka_type("boolean")]
        #[kafka_default("true")]
        #[status(native)]
        #[origin(kacrab_runtime)]
        #[source("kacrab-runtime://config/socket.tcp.nodelay")]
        #[feature("socket2")]
        #[comment("Sets TCP_NODELAY on broker TCP connections.")]
        socket_tcp_nodelay: bool,

        #[key("socket.tcp.quickack")]
        #[default(None)]
        #[kafka_type("boolean")]
        #[kafka_default("null")]
        #[status(native)]
        #[origin(kacrab_runtime)]
        #[source("kacrab-runtime://config/socket.tcp.quickack")]
        #[platforms("linux", "android", "fuchsia", "cygwin")]
        #[feature("socket2")]
        #[comment("Sets TCP_QUICKACK after a broker TCP connection is established.")]
        socket_tcp_quickack: Option<bool>,

        #[key("socket.tcp.notsent.lowat.bytes")]
        #[default(None)]
        #[kafka_type("int")]
        #[kafka_default("null")]
        #[status(native)]
        #[origin(kacrab_runtime)]
        #[source("kacrab-runtime://config/socket.tcp.notsent.lowat.bytes")]
        #[platforms("linux", "android")]
        #[feature("socket2")]
        #[comment("Sets TCP_NOTSENT_LOWAT on supported broker TCP sockets.")]
        socket_tcp_notsent_lowat_bytes: Option<u32>,

        #[key("socket.tcp.user.timeout.ms")]
        #[default(None)]
        #[kafka_type("long")]
        #[kafka_default("null")]
        #[status(native)]
        #[origin(kacrab_runtime)]
        #[source("kacrab-runtime://config/socket.tcp.user.timeout.ms")]
        #[platforms("linux", "android", "fuchsia", "cygwin")]
        #[feature("socket2")]
        #[comment("Sets TCP_USER_TIMEOUT on supported broker TCP sockets.")]
        socket_tcp_user_timeout_ms: Option<DurationMs>,

        #[key("socket.tcp.congestion")]
        #[default(None)]
        #[kafka_type("string")]
        #[kafka_default("null")]
        #[status(native)]
        #[origin(kacrab_runtime)]
        #[source("kacrab-runtime://config/socket.tcp.congestion")]
        #[platforms("linux", "freebsd")]
        #[feature("socket2")]
        #[comment("Sets the TCP congestion-control algorithm on supported broker TCP sockets.")]
        socket_tcp_congestion: Option<TcpCongestionControl>,

        #[key("socket.reuse.address")]
        #[default(true)]
        #[kafka_type("boolean")]
        #[kafka_default("true")]
        #[status(native)]
        #[origin(kacrab_runtime)]
        #[source("kacrab-runtime://config/socket.reuse.address")]
        #[feature("socket2")]
        #[comment("Sets SO_REUSEADDR before connecting broker TCP sockets.")]
        socket_reuse_address: bool,

        #[key("socket.read.buffer.capacity.bytes")]
        #[default(None)]
        #[kafka_type("int")]
        #[kafka_default("null")]
        #[status(native)]
        #[origin(kacrab_runtime)]
        #[source("kacrab-runtime://config/socket.read.buffer.capacity.bytes")]
        #[feature("socket2")]
        #[comment("Initial reusable in-process read buffer capacity for broker frame reads.")]
        socket_read_buffer_capacity_bytes: Option<usize>,
    }
}

kafka_config! {
    #[client(Admin)]
    pub AdminConfig {
        #[key("bootstrap.controllers")]
        #[default(ConfigList::from_csv(""))]
        #[kafka_type("list")]
        #[kafka_default("\"\"")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/admin-configs/#adminclientconfigs_bootstrap.controllers")]
        #[comment("Initial Kafka controller list used by admin clients when targeting controllers directly.")]
        bootstrap_controllers: ConfigList,

        #[key("bootstrap.servers")]
        #[required]
        #[kafka_type("list")]
        #[kafka_default("\"\"")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/admin-configs/#adminclientconfigs_bootstrap.servers")]
        #[comment("Initial Kafka broker list used to discover the full cluster.")]
        bootstrap_servers: ConfigList,

        #[key("client.id")]
        #[default(String::new())]
        #[kafka_type("string")]
        #[kafka_default("\"\"")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/admin-configs/#adminclientconfigs_client.id")]
        #[comment("Logical client name sent to brokers for quota, logging, and metrics attribution.")]
        client_id: String,

        #[key("client.dns.lookup")]
        #[default(String::from("use_all_dns_ips"))]
        #[kafka_type("string")]
        #[kafka_default("use_all_dns_ips")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/admin-configs/#adminclientconfigs_client.dns.lookup")]
        #[comment("Kafka-compatible DNS lookup strategy for bootstrap and broker hostnames.")]
        client_dns_lookup: String,

        #[key("connections.max.idle.ms")]
        #[default(DurationMs::from_millis(300_000))]
        #[kafka_type("long")]
        #[kafka_default("300000 (5 minutes)")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/admin-configs/#adminclientconfigs_connections.max.idle.ms")]
        #[comment("Idle connection lifetime before the admin client closes a broker connection.")]
        connections_max_idle_ms: DurationMs,

        #[key("default.api.timeout.ms")]
        #[default(DurationMs::from_millis(60_000))]
        #[kafka_type("int")]
        #[kafka_default("60000 (1 minute)")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/admin-configs/#adminclientconfigs_default.api.timeout.ms")]
        #[comment("Default timeout for admin APIs without an explicit timeout.")]
        default_api_timeout_ms: DurationMs,

        #[key("receive.buffer.bytes")]
        #[default(65_536_i32)]
        #[kafka_type("int")]
        #[kafka_default("65536 (64 kibibytes)")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/admin-configs/#adminclientconfigs_receive.buffer.bytes")]
        #[comment("Socket receive buffer size for admin network connections.")]
        receive_buffer_bytes: i32,

        #[key("request.timeout.ms")]
        #[default(DurationMs::from_millis(30_000))]
        #[kafka_type("int")]
        #[kafka_default("30000 (30 seconds)")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/admin-configs/#adminclientconfigs_request.timeout.ms")]
        #[comment("Maximum time the admin client waits for a broker response.")]
        request_timeout_ms: DurationMs,

        #[key("send.buffer.bytes")]
        #[default(131_072_i32)]
        #[kafka_type("int")]
        #[kafka_default("131072 (128 kibibytes)")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/admin-configs/#adminclientconfigs_send.buffer.bytes")]
        #[comment("Socket send buffer size for admin network connections.")]
        send_buffer_bytes: i32,

        #[key("socket.connection.setup.timeout.max.ms")]
        #[default(DurationMs::from_millis(30_000))]
        #[kafka_type("long")]
        #[kafka_default("30000 (30 seconds)")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/admin-configs/#adminclientconfigs_socket.connection.setup.timeout.max.ms")]
        #[comment("Maximum socket connection setup timeout after exponential backoff.")]
        socket_connection_setup_timeout_max_ms: DurationMs,

        #[key("socket.connection.setup.timeout.ms")]
        #[default(DurationMs::from_millis(10_000))]
        #[kafka_type("long")]
        #[kafka_default("10000 (10 seconds)")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/admin-configs/#adminclientconfigs_socket.connection.setup.timeout.ms")]
        #[comment("Initial socket connection setup timeout.")]
        socket_connection_setup_timeout_ms: DurationMs,

        #[key("enable.metrics.push")]
        #[default(false)]
        #[kafka_type("boolean")]
        #[kafka_default("false")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/admin-configs/#adminclientconfigs_enable.metrics.push")]
        #[comment("Whether admin client metrics may be pushed to the Kafka cluster.")]
        enable_metrics_push: bool,

        #[key("metadata.max.age.ms")]
        #[default(DurationMs::from_millis(300_000))]
        #[kafka_type("long")]
        #[kafka_default("300000 (5 minutes)")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/admin-configs/#adminclientconfigs_metadata.max.age.ms")]
        #[comment("Maximum metadata cache age before forced refresh.")]
        metadata_max_age_ms: DurationMs,

        #[key("metadata.recovery.rebootstrap.trigger.ms")]
        #[default(DurationMs::from_millis(300_000))]
        #[kafka_type("long")]
        #[kafka_default("300000 (5 minutes)")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/admin-configs/#adminclientconfigs_metadata.recovery.rebootstrap.trigger.ms")]
        #[comment("Time without usable metadata before rebootstrap recovery may trigger.")]
        metadata_recovery_rebootstrap_trigger_ms: DurationMs,

        #[key("metadata.recovery.strategy")]
        #[default(String::from("rebootstrap"))]
        #[kafka_type("string")]
        #[kafka_default("rebootstrap")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/admin-configs/#adminclientconfigs_metadata.recovery.strategy")]
        #[comment("Recovery strategy used when no known broker is available.")]
        metadata_recovery_strategy: String,

        #[key("retries")]
        #[default(2_147_483_647_i32)]
        #[kafka_type("int")]
        #[kafka_default("2147483647")]
        #[status(native)]
        #[source("https://kafka.apache.org/43/configuration/admin-configs/#adminclientconfigs_retries")]
        #[comment("Maximum number of retry attempts for retriable admin requests.")]
        retries: i32,

        #[key("security.protocol")]
        #[default(String::from("PLAINTEXT"))]
        #[kafka_type("string")]
        #[kafka_default("PLAINTEXT")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/admin-configs/#adminclientconfigs_security.protocol")]
        #[comment("Kafka-compatible broker security protocol: PLAINTEXT, SSL, SASL_PLAINTEXT, or SASL_SSL.")]
        security_protocol: String,

        #[key("ssl.truststore.location")]
        #[default(None)]
        #[kafka_type("string")]
        #[kafka_default("null")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/admin-configs/#adminclientconfigs_ssl.truststore.location")]
        #[feature("tls-rustls")]
        #[comment("Trust material location used by TLS broker connections.")]
        ssl_truststore_location: Option<String>,

        #[key("ssl.truststore.password")]
        #[default(None)]
        #[kafka_type("password")]
        #[kafka_default("null")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/admin-configs/#adminclientconfigs_ssl.truststore.password")]
        #[feature("tls-rustls")]
        #[comment("Password for configured TLS trust material when the backend format requires it.")]
        ssl_truststore_password: Option<String>,

        #[key("ssl.truststore.certificates")]
        #[default(None)]
        #[kafka_type("password")]
        #[kafka_default("null")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/admin-configs/#adminclientconfigs_ssl.truststore.certificates")]
        #[feature("tls-rustls")]
        #[comment("Inline PEM trusted certificates used by TLS broker connections.")]
        ssl_truststore_certificates: Option<String>,

        #[key("ssl.truststore.type")]
        #[default(None)]
        #[kafka_type("string")]
        #[kafka_default("JKS")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/admin-configs/#adminclientconfigs_ssl.truststore.type")]
        #[feature("tls-rustls")]
        #[comment("Truststore format; rustls backend supports PEM, JKS, and PKCS12 material.")]
        ssl_truststore_type: Option<String>,

        #[key("ssl.keystore.location")]
        #[default(None)]
        #[kafka_type("string")]
        #[kafka_default("null")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/admin-configs/#adminclientconfigs_ssl.keystore.location")]
        #[feature("tls-rustls")]
        #[comment("Client identity material location used by mTLS broker connections.")]
        ssl_keystore_location: Option<String>,

        #[key("ssl.keystore.password")]
        #[default(None)]
        #[kafka_type("password")]
        #[kafka_default("null")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/admin-configs/#adminclientconfigs_ssl.keystore.password")]
        #[feature("tls-rustls")]
        #[comment("Password for configured TLS client identity material when the backend format requires it.")]
        ssl_keystore_password: Option<String>,

        #[key("ssl.keystore.key")]
        #[default(None)]
        #[kafka_type("password")]
        #[kafka_default("null")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/admin-configs/#adminclientconfigs_ssl.keystore.key")]
        #[feature("tls-rustls")]
        #[comment("Inline PEM private key for TLS client authentication.")]
        ssl_keystore_key: Option<String>,

        #[key("ssl.keystore.certificate.chain")]
        #[default(None)]
        #[kafka_type("password")]
        #[kafka_default("null")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/admin-configs/#adminclientconfigs_ssl.keystore.certificate.chain")]
        #[feature("tls-rustls")]
        #[comment("Inline PEM certificate chain for TLS client authentication.")]
        ssl_keystore_certificate_chain: Option<String>,

        #[key("ssl.keystore.type")]
        #[default(None)]
        #[kafka_type("string")]
        #[kafka_default("JKS")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/admin-configs/#adminclientconfigs_ssl.keystore.type")]
        #[feature("tls-rustls")]
        #[comment("Keystore format; rustls backend supports PEM, JKS, and PKCS12 material.")]
        ssl_keystore_type: Option<String>,

        #[key("ssl.key.password")]
        #[default(None)]
        #[kafka_type("password")]
        #[kafka_default("null")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/admin-configs/#adminclientconfigs_ssl.key.password")]
        #[feature("tls-rustls")]
        #[comment("Password for encrypted TLS private key material.")]
        ssl_key_password: Option<String>,

        #[key("ssl.endpoint.identification.algorithm")]
        #[default(Some(String::from("https")))]
        #[kafka_type("string")]
        #[kafka_default("https")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/admin-configs/#adminclientconfigs_ssl.endpoint.identification.algorithm")]
        #[feature("tls-rustls")]
        #[comment("Hostname verification algorithm; empty string disables endpoint identification explicitly.")]
        ssl_endpoint_identification_algorithm: Option<String>,

        #[key("ssl.protocol")]
        #[default(String::from("TLSv1.3"))]
        #[kafka_type("string")]
        #[kafka_default("TLSv1.3")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/admin-configs/#adminclientconfigs_ssl.protocol")]
        #[feature("tls-rustls")]
        #[comment("Preferred TLS protocol version.")]
        ssl_protocol: String,

        #[key("ssl.enabled.protocols")]
        #[default(Some(String::from("TLSv1.2,TLSv1.3")))]
        #[kafka_type("list")]
        #[kafka_default("TLSv1.2,TLSv1.3")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/admin-configs/#adminclientconfigs_ssl.enabled.protocols")]
        #[feature("tls-rustls")]
        #[comment("Comma-separated TLS protocol versions enabled for negotiation.")]
        ssl_enabled_protocols: Option<String>,

        #[key("ssl.cipher.suites")]
        #[default(None)]
        #[kafka_type("list")]
        #[kafka_default("null")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/admin-configs/#adminclientconfigs_ssl.cipher.suites")]
        #[feature("tls-rustls")]
        #[comment("Comma-separated TLS cipher suite names requested by the user.")]
        ssl_cipher_suites: Option<String>,

        #[key("sasl.mechanism")]
        #[default(String::from("GSSAPI"))]
        #[kafka_type("string")]
        #[kafka_default("GSSAPI")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/admin-configs/#adminclientconfigs_sasl.mechanism")]
        #[feature("sasl")]
        #[comment("Kafka SASL mechanism: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, OAUTHBEARER, or GSSAPI.")]
        sasl_mechanism: String,

        #[key("sasl.jaas.config")]
        #[default(None)]
        #[kafka_type("password")]
        #[kafka_default("null")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/admin-configs/#adminclientconfigs_sasl.jaas.config")]
        #[feature("sasl")]
        #[comment("Java-compatible JAAS login module options used to derive Rust SASL credentials.")]
        sasl_jaas_config: Option<String>,

        #[key("sasl.login.connect.timeout.ms")]
        #[default(None)]
        #[kafka_type("int")]
        #[kafka_default("null")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/admin-configs/#adminclientconfigs_sasl.login.connect.timeout.ms")]
        #[feature("sasl")]
        #[comment("External SASL login provider connection timeout.")]
        sasl_login_connect_timeout_ms: Option<DurationMs>,

        #[key("sasl.login.read.timeout.ms")]
        #[default(None)]
        #[kafka_type("int")]
        #[kafka_default("null")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/admin-configs/#adminclientconfigs_sasl.login.read.timeout.ms")]
        #[feature("sasl")]
        #[comment("External SASL login provider read timeout.")]
        sasl_login_read_timeout_ms: Option<DurationMs>,

        #[key("sasl.login.refresh.window.factor")]
        #[default(0.8_f64)]
        #[kafka_type("double")]
        #[kafka_default("0.8")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/admin-configs/#adminclientconfigs_sasl.login.refresh.window.factor")]
        #[feature("sasl")]
        #[comment("OAuth token refresh point as a factor of the token lifetime.")]
        sasl_login_refresh_window_factor: f64,

        #[key("sasl.login.refresh.window.jitter")]
        #[default(0.05_f64)]
        #[kafka_type("double")]
        #[kafka_default("0.05")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/admin-configs/#adminclientconfigs_sasl.login.refresh.window.jitter")]
        #[feature("sasl")]
        #[comment("OAuth token refresh jitter factor.")]
        sasl_login_refresh_window_jitter: f64,

        #[key("sasl.login.refresh.min.period.seconds")]
        #[default(60_i32)]
        #[kafka_type("short")]
        #[kafka_default("60")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/admin-configs/#adminclientconfigs_sasl.login.refresh.min.period.seconds")]
        #[feature("sasl")]
        #[comment("Minimum OAuth token lifetime before attempting refresh.")]
        sasl_login_refresh_min_period_seconds: i32,

        #[key("sasl.login.refresh.buffer.seconds")]
        #[default(300_i32)]
        #[kafka_type("short")]
        #[kafka_default("300")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/admin-configs/#adminclientconfigs_sasl.login.refresh.buffer.seconds")]
        #[feature("sasl")]
        #[comment("OAuth token expiration buffer kept before refresh.")]
        sasl_login_refresh_buffer_seconds: i32,

        #[key("sasl.login.retry.backoff.ms")]
        #[default(DurationMs::from_millis(100))]
        #[kafka_type("long")]
        #[kafka_default("100")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/admin-configs/#adminclientconfigs_sasl.login.retry.backoff.ms")]
        #[feature("sasl")]
        #[comment("Initial OAuth login retry backoff.")]
        sasl_login_retry_backoff_ms: DurationMs,

        #[key("sasl.login.retry.backoff.max.ms")]
        #[default(DurationMs::from_millis(10_000))]
        #[kafka_type("long")]
        #[kafka_default("10000")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/admin-configs/#adminclientconfigs_sasl.login.retry.backoff.max.ms")]
        #[feature("sasl")]
        #[comment("Maximum OAuth login retry backoff.")]
        sasl_login_retry_backoff_max_ms: DurationMs,

        #[key("sasl.oauthbearer.token.endpoint.url")]
        #[default(None)]
        #[kafka_type("string")]
        #[kafka_default("null")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/admin-configs/#adminclientconfigs_sasl.oauthbearer.token.endpoint.url")]
        #[feature("sasl")]
        #[comment("File token endpoint for SASL/OAUTHBEARER static JWT retrieval.")]
        sasl_oauthbearer_token_endpoint_url: Option<String>,

        #[key("sasl.oauthbearer.assertion.file")]
        #[default(None)]
        #[kafka_type("string")]
        #[kafka_default("null")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/admin-configs/#adminclientconfigs_sasl.oauthbearer.assertion.file")]
        #[feature("sasl")]
        #[comment("Pre-generated JWT assertion file for SASL/OAUTHBEARER token retrieval.")]
        sasl_oauthbearer_assertion_file: Option<String>,

        #[key("sasl.oauthbearer.client.credentials.client.id")]
        #[default(None)]
        #[kafka_type("string")]
        #[kafka_default("null")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/admin-configs/#adminclientconfigs_sasl.oauthbearer.client.credentials.client.id")]
        #[feature("sasl")]
        #[comment("OAuth client credentials client id for SASL/OAUTHBEARER token retrieval.")]
        sasl_oauthbearer_client_credentials_client_id: Option<String>,

        #[key("sasl.oauthbearer.client.credentials.client.secret")]
        #[default(None)]
        #[kafka_type("password")]
        #[kafka_default("null")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/admin-configs/#adminclientconfigs_sasl.oauthbearer.client.credentials.client.secret")]
        #[feature("sasl")]
        #[comment("OAuth client credentials client secret for SASL/OAUTHBEARER token retrieval.")]
        sasl_oauthbearer_client_credentials_client_secret: Option<String>,

        #[key("sasl.oauthbearer.scope")]
        #[default(None)]
        #[kafka_type("string")]
        #[kafka_default("null")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/admin-configs/#adminclientconfigs_sasl.oauthbearer.scope")]
        #[feature("sasl")]
        #[comment("Optional OAuth scope for SASL/OAUTHBEARER token retrieval.")]
        sasl_oauthbearer_scope: Option<String>,

        #[key("sasl.oauthbearer.assertion.private.key.file")]
        #[default(None)]
        #[kafka_type("string")]
        #[kafka_default("null")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/admin-configs/#adminclientconfigs_sasl.oauthbearer.assertion.private.key.file")]
        #[feature("sasl")]
        #[comment("PEM private key file used to sign SASL/OAUTHBEARER JWT assertions.")]
        sasl_oauthbearer_assertion_private_key_file: Option<String>,

        #[key("sasl.oauthbearer.assertion.private.key.passphrase")]
        #[default(None)]
        #[kafka_type("password")]
        #[kafka_default("null")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/admin-configs/#adminclientconfigs_sasl.oauthbearer.assertion.private.key.passphrase")]
        #[feature("sasl")]
        #[comment("Passphrase for the SASL/OAUTHBEARER assertion private key file.")]
        sasl_oauthbearer_assertion_private_key_passphrase: Option<String>,

        #[key("sasl.oauthbearer.assertion.template.file")]
        #[default(None)]
        #[kafka_type("string")]
        #[kafka_default("null")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/admin-configs/#adminclientconfigs_sasl.oauthbearer.assertion.template.file")]
        #[feature("sasl")]
        #[comment("JSON header and claim template for generated SASL/OAUTHBEARER JWT assertions.")]
        sasl_oauthbearer_assertion_template_file: Option<String>,

        #[key("sasl.oauthbearer.assertion.algorithm")]
        #[default(String::from("RS256"))]
        #[kafka_type("string")]
        #[kafka_default("DEFAULT_SASL_OAUTHBEARER_ASSERTION_ALGORITHM")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/admin-configs/#adminclientconfigs_sasl.oauthbearer.assertion.algorithm")]
        #[feature("sasl")]
        #[comment("Signing algorithm for generated SASL/OAUTHBEARER JWT assertions.")]
        sasl_oauthbearer_assertion_algorithm: String,

        #[key("sasl.oauthbearer.assertion.claim.aud")]
        #[default(None)]
        #[kafka_type("string")]
        #[kafka_default("null")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/admin-configs/#adminclientconfigs_sasl.oauthbearer.assertion.claim.aud")]
        #[feature("sasl")]
        #[comment("Audience claim for generated SASL/OAUTHBEARER JWT assertions.")]
        sasl_oauthbearer_assertion_claim_aud: Option<String>,

        #[key("sasl.oauthbearer.assertion.claim.iss")]
        #[default(None)]
        #[kafka_type("string")]
        #[kafka_default("null")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/admin-configs/#adminclientconfigs_sasl.oauthbearer.assertion.claim.iss")]
        #[feature("sasl")]
        #[comment("Issuer claim for generated SASL/OAUTHBEARER JWT assertions.")]
        sasl_oauthbearer_assertion_claim_iss: Option<String>,

        #[key("sasl.oauthbearer.assertion.claim.sub")]
        #[default(None)]
        #[kafka_type("string")]
        #[kafka_default("null")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/admin-configs/#adminclientconfigs_sasl.oauthbearer.assertion.claim.sub")]
        #[feature("sasl")]
        #[comment("Subject claim for generated SASL/OAUTHBEARER JWT assertions.")]
        sasl_oauthbearer_assertion_claim_sub: Option<String>,

        #[key("sasl.oauthbearer.assertion.claim.exp.seconds")]
        #[default(60_i32)]
        #[kafka_type("int")]
        #[kafka_default("DEFAULT_SASL_OAUTHBEARER_ASSERTION_CLAIM_EXP_SECONDS")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/admin-configs/#adminclientconfigs_sasl.oauthbearer.assertion.claim.exp.seconds")]
        #[feature("sasl")]
        #[comment("Validity period in seconds for generated SASL/OAUTHBEARER JWT assertions.")]
        sasl_oauthbearer_assertion_claim_exp_seconds: i32,

        #[key("sasl.oauthbearer.assertion.claim.nbf.seconds")]
        #[default(0_i32)]
        #[kafka_type("int")]
        #[kafka_default("DEFAULT_SASL_OAUTHBEARER_ASSERTION_CLAIM_NBF_SECONDS")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/admin-configs/#adminclientconfigs_sasl.oauthbearer.assertion.claim.nbf.seconds")]
        #[feature("sasl")]
        #[comment("Not-before offset in seconds for generated SASL/OAUTHBEARER JWT assertions.")]
        sasl_oauthbearer_assertion_claim_nbf_seconds: i32,

        #[key("sasl.oauthbearer.assertion.claim.jti.include")]
        #[default(false)]
        #[kafka_type("boolean")]
        #[kafka_default("DEFAULT_SASL_OAUTHBEARER_ASSERTION_CLAIM_JTI_INCLUDE")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/admin-configs/#adminclientconfigs_sasl.oauthbearer.assertion.claim.jti.include")]
        #[feature("sasl")]
        #[comment("Whether generated SASL/OAUTHBEARER JWT assertions include a random jti claim.")]
        sasl_oauthbearer_assertion_claim_jti_include: bool,

        #[key("sasl.login.callback.handler.class")]
        #[default(None)]
        #[kafka_type("class")]
        #[kafka_default("null")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/admin-configs/#adminclientconfigs_sasl.login.callback.handler.class")]
        #[feature("sasl")]
        #[comment("Java SASL login callback handler class name retained for explicit compatibility errors.")]
        sasl_login_callback_handler_class: Option<String>,

        #[key("sasl.client.callback.handler.class")]
        #[default(None)]
        #[kafka_type("class")]
        #[kafka_default("null")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/admin-configs/#adminclientconfigs_sasl.client.callback.handler.class")]
        #[feature("sasl")]
        #[comment("Java SASL client callback handler class name retained for explicit compatibility errors.")]
        sasl_client_callback_handler_class: Option<String>,

        #[key("sasl.kerberos.service.name")]
        #[default(None)]
        #[kafka_type("string")]
        #[kafka_default("null")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/admin-configs/#adminclientconfigs_sasl.kerberos.service.name")]
        #[feature("sasl")]
        #[comment("Kerberos service principal name for GSSAPI.")]
        sasl_kerberos_service_name: Option<String>,

        #[key("sasl.kerberos.kinit.cmd")]
        #[default(String::from("/usr/bin/kinit"))]
        #[kafka_type("string")]
        #[kafka_default("SaslConfigs.DEFAULT_KERBEROS_KINIT_CMD")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/admin-configs/#adminclientconfigs_sasl.kerberos.kinit.cmd")]
        #[feature("sasl")]
        #[comment("Kerberos kinit command configured for a future GSSAPI backend.")]
        sasl_kerberos_kinit_cmd: String,

        #[key("sasl.kerberos.ticket.renew.window.factor")]
        #[default(0.8_f64)]
        #[kafka_type("double")]
        #[kafka_default("SaslConfigs.DEFAULT_KERBEROS_TICKET_RENEW_WINDOW_FACTOR")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/admin-configs/#adminclientconfigs_sasl.kerberos.ticket.renew.window.factor")]
        #[feature("sasl")]
        #[comment("Kerberos ticket renew window factor for a future GSSAPI backend.")]
        sasl_kerberos_ticket_renew_window_factor: f64,

        #[key("sasl.kerberos.ticket.renew.jitter")]
        #[default(0.05_f64)]
        #[kafka_type("double")]
        #[kafka_default("SaslConfigs.DEFAULT_KERBEROS_TICKET_RENEW_JITTER")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/admin-configs/#adminclientconfigs_sasl.kerberos.ticket.renew.jitter")]
        #[feature("sasl")]
        #[comment("Kerberos ticket renew jitter for a future GSSAPI backend.")]
        sasl_kerberos_ticket_renew_jitter: f64,

        #[key("sasl.kerberos.min.time.before.relogin")]
        #[default(DurationMs::from_millis(60_000))]
        #[kafka_type("long")]
        #[kafka_default("SaslConfigs.DEFAULT_KERBEROS_MIN_TIME_BEFORE_RELOGIN")]
        #[status(native_review)]
        #[source("https://kafka.apache.org/43/configuration/admin-configs/#adminclientconfigs_sasl.kerberos.min.time.before.relogin")]
        #[feature("sasl")]
        #[comment("Minimum time before Kerberos relogin for a future GSSAPI backend.")]
        sasl_kerberos_min_time_before_relogin: DurationMs,

        #[key("socket.tcp.nodelay")]
        #[default(true)]
        #[kafka_type("boolean")]
        #[kafka_default("true")]
        #[status(native)]
        #[origin(kacrab_runtime)]
        #[source("kacrab-runtime://config/socket.tcp.nodelay")]
        #[feature("socket2")]
        #[comment("Sets TCP_NODELAY on broker TCP connections.")]
        socket_tcp_nodelay: bool,

        #[key("socket.tcp.quickack")]
        #[default(None)]
        #[kafka_type("boolean")]
        #[kafka_default("null")]
        #[status(native)]
        #[origin(kacrab_runtime)]
        #[source("kacrab-runtime://config/socket.tcp.quickack")]
        #[platforms("linux", "android", "fuchsia", "cygwin")]
        #[feature("socket2")]
        #[comment("Sets TCP_QUICKACK after a broker TCP connection is established.")]
        socket_tcp_quickack: Option<bool>,

        #[key("socket.tcp.notsent.lowat.bytes")]
        #[default(None)]
        #[kafka_type("int")]
        #[kafka_default("null")]
        #[status(native)]
        #[origin(kacrab_runtime)]
        #[source("kacrab-runtime://config/socket.tcp.notsent.lowat.bytes")]
        #[platforms("linux", "android")]
        #[feature("socket2")]
        #[comment("Sets TCP_NOTSENT_LOWAT on supported broker TCP sockets.")]
        socket_tcp_notsent_lowat_bytes: Option<u32>,

        #[key("socket.tcp.user.timeout.ms")]
        #[default(None)]
        #[kafka_type("long")]
        #[kafka_default("null")]
        #[status(native)]
        #[origin(kacrab_runtime)]
        #[source("kacrab-runtime://config/socket.tcp.user.timeout.ms")]
        #[platforms("linux", "android", "fuchsia", "cygwin")]
        #[feature("socket2")]
        #[comment("Sets TCP_USER_TIMEOUT on supported broker TCP sockets.")]
        socket_tcp_user_timeout_ms: Option<DurationMs>,

        #[key("socket.tcp.congestion")]
        #[default(None)]
        #[kafka_type("string")]
        #[kafka_default("null")]
        #[status(native)]
        #[origin(kacrab_runtime)]
        #[source("kacrab-runtime://config/socket.tcp.congestion")]
        #[platforms("linux", "freebsd")]
        #[feature("socket2")]
        #[comment("Sets the TCP congestion-control algorithm on supported broker TCP sockets.")]
        socket_tcp_congestion: Option<TcpCongestionControl>,

        #[key("socket.reuse.address")]
        #[default(true)]
        #[kafka_type("boolean")]
        #[kafka_default("true")]
        #[status(native)]
        #[origin(kacrab_runtime)]
        #[source("kacrab-runtime://config/socket.reuse.address")]
        #[feature("socket2")]
        #[comment("Sets SO_REUSEADDR before connecting broker TCP sockets.")]
        socket_reuse_address: bool,

        #[key("socket.read.buffer.capacity.bytes")]
        #[default(None)]
        #[kafka_type("int")]
        #[kafka_default("null")]
        #[status(native)]
        #[origin(kacrab_runtime)]
        #[source("kacrab-runtime://config/socket.read.buffer.capacity.bytes")]
        #[feature("socket2")]
        #[comment("Initial reusable in-process read buffer capacity for broker frame reads.")]
        socket_read_buffer_capacity_bytes: Option<usize>,
    }
}
