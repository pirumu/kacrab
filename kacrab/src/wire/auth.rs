//! SASL and security-protocol configuration for broker sessions.

use std::{
    fmt::Write as _,
    fs,
    future::Future,
    str,
    string::String,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
    vec::Vec,
};

use base64::{Engine, engine::general_purpose};
use bytes::Bytes;
use hmac::{Hmac, Mac};
use jsonwebtoken::{Algorithm, AlgorithmFamily, EncodingKey, Header};
use pkcs8::der::{Decode, pem::PemLabel};
use serde_json::{Map, Value};
use sha2::{Sha256, Sha512};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpStream,
    time,
};

use super::{
    SaslClientAuthenticator, SaslClientAuthenticatorFactory, SaslClientAuthenticatorFactoryHandle,
    SaslClientAuthenticatorHandle, TlsConfig, tls,
};

/// Kafka `security.protocol` value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecurityProtocol {
    /// Plain TCP without TLS or SASL.
    Plaintext,
    /// TLS without SASL.
    Ssl,
    /// SASL over plain TCP.
    SaslPlaintext,
    /// SASL over TLS.
    SaslSsl,
}

impl SecurityProtocol {
    /// Parses a Java-compatible `security.protocol` value.
    ///
    /// # Errors
    ///
    /// Returns [`crate::wire::WireError`] for unknown values.
    pub fn parse(value: &str) -> Result<Self, crate::wire::WireError> {
        match value {
            value if value.eq_ignore_ascii_case("PLAINTEXT") => Ok(Self::Plaintext),
            value if value.eq_ignore_ascii_case("SSL") => Ok(Self::Ssl),
            value if value.eq_ignore_ascii_case("SASL_PLAINTEXT") => Ok(Self::SaslPlaintext),
            value if value.eq_ignore_ascii_case("SASL_SSL") => Ok(Self::SaslSsl),
            _ => Err(crate::wire::WireError::InvalidSecurityProtocol(
                value.to_owned(),
            )),
        }
    }

    /// Returns whether this protocol requires TLS before Kafka handshakes.
    #[must_use]
    pub const fn uses_tls(self) -> bool {
        matches!(self, Self::Ssl | Self::SaslSsl)
    }

    /// Returns whether this protocol requires SASL before normal requests.
    #[must_use]
    pub const fn uses_sasl(self) -> bool {
        matches!(self, Self::SaslPlaintext | Self::SaslSsl)
    }
}

/// Kafka SASL mechanism.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SaslMechanism {
    /// SASL/PLAIN.
    Plain,
    /// SCRAM with SHA-256.
    ScramSha256,
    /// SCRAM with SHA-512.
    ScramSha512,
    /// SASL/OAUTHBEARER.
    OAuthBearer,
    /// SASL/GSSAPI Kerberos.
    Gssapi,
}

impl SaslMechanism {
    /// Parses a Java-compatible `sasl.mechanism` value.
    ///
    /// # Errors
    ///
    /// Returns [`crate::wire::WireError`] for unknown values.
    pub fn parse(value: &str) -> Result<Self, crate::wire::WireError> {
        match value {
            value if value.eq_ignore_ascii_case("PLAIN") => Ok(Self::Plain),
            value if value.eq_ignore_ascii_case("SCRAM-SHA-256") => Ok(Self::ScramSha256),
            value if value.eq_ignore_ascii_case("SCRAM-SHA-512") => Ok(Self::ScramSha512),
            value if value.eq_ignore_ascii_case("OAUTHBEARER") => Ok(Self::OAuthBearer),
            value if value.eq_ignore_ascii_case("GSSAPI") => Ok(Self::Gssapi),
            _ => Err(crate::wire::WireError::UnsupportedSaslMechanism(
                value.to_owned(),
            )),
        }
    }

    /// Returns the canonical Kafka mechanism name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Plain => "PLAIN",
            Self::ScramSha256 => "SCRAM-SHA-256",
            Self::ScramSha512 => "SCRAM-SHA-512",
            Self::OAuthBearer => "OAUTHBEARER",
            Self::Gssapi => "GSSAPI",
        }
    }
}

/// Builds the SASL/PLAIN client response from a Java JAAS config string.
///
/// # Errors
///
/// Returns [`crate::wire::WireError`] when `username` or `password` is not
/// present in the JAAS options.
pub(crate) fn plain_auth_bytes(jaas_config: Option<&str>) -> Result<Bytes, crate::wire::WireError> {
    let Some(jaas_config) = jaas_config else {
        return Err(crate::wire::WireError::InvalidSaslConfig(
            "sasl.jaas.config is required for PLAIN".to_owned(),
        ));
    };
    let username = jaas_option(jaas_config, "username").ok_or_else(|| {
        crate::wire::WireError::InvalidSaslConfig(
            "sasl.jaas.config must contain username for PLAIN".to_owned(),
        )
    })?;
    let password = jaas_option(jaas_config, "password").ok_or_else(|| {
        crate::wire::WireError::InvalidSaslConfig(
            "sasl.jaas.config must contain password for PLAIN".to_owned(),
        )
    })?;
    let mut bytes = Vec::with_capacity(
        username
            .len()
            .saturating_add(password.len())
            .saturating_add(2),
    );
    bytes.push(0);
    bytes.extend_from_slice(username.as_bytes());
    bytes.push(0);
    bytes.extend_from_slice(password.as_bytes());
    Ok(Bytes::from(bytes))
}

/// Builds the SASL/OAUTHBEARER client response from a static token source.
///
/// Kafka's wire payload follows RFC 7628: GS2 header, optional key/value
/// attributes, the Bearer token, then a final empty field.
pub(crate) async fn oauthbearer_auth_bytes(
    config: &SaslConfig,
    tls_config: &TlsConfig,
    token_cache: &tokio::sync::Mutex<OAuthTokenCache>,
) -> Result<Bytes, crate::wire::WireError> {
    let token = oauthbearer_token(config, tls_config, token_cache).await?;
    let mut bytes = Vec::with_capacity(token.len().saturating_add(18));
    bytes.extend_from_slice(b"n,,\x01auth=Bearer ");
    bytes.extend_from_slice(token.trim().as_bytes());
    bytes.extend_from_slice(b"\x01\x01");
    Ok(Bytes::from(bytes))
}

pub(crate) fn validate_sasl_extension_hooks(
    config: &SaslConfig,
) -> Result<(), crate::wire::WireError> {
    if config.login_callback_handler_class.is_some()
        || config.client_callback_handler_class.is_some()
    {
        return Err(crate::wire::WireError::InvalidSaslConfig(
            "Java sasl.*.callback.handler.class extensions cannot run inside the Rust wire backend"
                .to_owned(),
        ));
    }
    Ok(())
}

/// SCRAM client-side state for one SASL authentication exchange.
#[derive(Debug)]
pub(crate) struct ScramExchange {
    mechanism: SaslMechanism,
    password: String,
    client_nonce: String,
    client_first_bare: String,
}

impl ScramExchange {
    /// Creates a SCRAM exchange and returns the first client message.
    ///
    /// # Errors
    ///
    /// Returns [`crate::wire::WireError`] when credentials or nonce generation fail.
    pub(crate) fn start(
        mechanism: SaslMechanism,
        jaas_config: Option<&str>,
    ) -> Result<(Self, Bytes), crate::wire::WireError> {
        let Some(jaas_config) = jaas_config else {
            return Err(crate::wire::WireError::InvalidSaslConfig(
                "sasl.jaas.config is required for SCRAM".to_owned(),
            ));
        };
        let username = jaas_option(jaas_config, "username").ok_or_else(|| {
            crate::wire::WireError::InvalidSaslConfig(
                "sasl.jaas.config must contain username for SCRAM".to_owned(),
            )
        })?;
        let password = jaas_option(jaas_config, "password").ok_or_else(|| {
            crate::wire::WireError::InvalidSaslConfig(
                "sasl.jaas.config must contain password for SCRAM".to_owned(),
            )
        })?;
        let client_nonce = generate_nonce()?;
        let escaped_username = escape_scram_name(&username);
        let client_first_bare = format!("n={escaped_username},r={client_nonce}");
        let client_first = {
            let mut value = String::from("n,,");
            value.push_str(&client_first_bare);
            Bytes::from(value)
        };
        Ok((
            Self {
                mechanism,
                password,
                client_nonce,
                client_first_bare,
            },
            client_first,
        ))
    }

    /// Handles the server-first message and returns the final client proof
    /// plus the expected server signature bytes.
    ///
    /// # Errors
    ///
    /// Returns [`crate::wire::WireError`] when the server challenge is invalid.
    pub(crate) fn client_final(
        &self,
        server_first: &[u8],
    ) -> Result<(Bytes, Vec<u8>), crate::wire::WireError> {
        let server_first = str::from_utf8(server_first).map_err(|_error| {
            crate::wire::WireError::SaslAuthentication(
                "SCRAM server-first message is not UTF-8".to_owned(),
            )
        })?;
        let attributes = ScramServerFirst::parse(server_first)?;
        if !attributes.nonce.starts_with(&self.client_nonce) {
            return Err(crate::wire::WireError::SaslAuthentication(
                "SCRAM server nonce does not extend client nonce".to_owned(),
            ));
        }
        let salt = general_purpose::STANDARD
            .decode(attributes.salt.as_bytes())
            .map_err(|_error| {
                crate::wire::WireError::SaslAuthentication(
                    "SCRAM server salt is not valid base64".to_owned(),
                )
            })?;
        let client_final_without_proof = {
            let mut value = String::from("c=biws,r=");
            value.push_str(&attributes.nonce);
            value
        };
        let auth_message = {
            let mut value = self.client_first_bare.clone();
            value.push(',');
            value.push_str(server_first);
            value.push(',');
            value.push_str(&client_final_without_proof);
            value
        };
        let salted_password = salted_password(
            self.mechanism,
            self.password.as_bytes(),
            &salt,
            attributes.iterations,
        )?;
        let client_key = hmac_bytes(self.mechanism, &salted_password, b"Client Key")?;
        let stored_key = digest_bytes(self.mechanism, &client_key);
        let client_signature = hmac_bytes(self.mechanism, &stored_key, auth_message.as_bytes())?;
        let proof = xor_bytes(&client_key, &client_signature)?;
        let server_key = hmac_bytes(self.mechanism, &salted_password, b"Server Key")?;
        let server_signature = hmac_bytes(self.mechanism, &server_key, auth_message.as_bytes())?;
        let client_final = {
            let mut value = client_final_without_proof;
            value.push_str(",p=");
            value.push_str(&general_purpose::STANDARD.encode(proof));
            Bytes::from(value)
        };
        Ok((client_final, server_signature))
    }

    /// Verifies the server-final message signature.
    ///
    /// # Errors
    ///
    /// Returns [`crate::wire::WireError`] when the server reports an error or
    /// the signature does not match the SCRAM transcript.
    pub(crate) fn verify_server_final(
        server_final: &[u8],
        expected_signature: &[u8],
    ) -> Result<(), crate::wire::WireError> {
        let server_final = str::from_utf8(server_final).map_err(|_error| {
            crate::wire::WireError::SaslAuthentication(
                "SCRAM server-final message is not UTF-8".to_owned(),
            )
        })?;
        if let Some(error) = parse_scram_attribute(server_final, "e") {
            return Err(crate::wire::WireError::SaslAuthentication(error));
        }
        let signature = parse_scram_attribute(server_final, "v").ok_or_else(|| {
            crate::wire::WireError::SaslAuthentication(
                "SCRAM server-final message is missing verifier".to_owned(),
            )
        })?;
        let signature = general_purpose::STANDARD
            .decode(signature.as_bytes())
            .map_err(|_error| {
                crate::wire::WireError::SaslAuthentication(
                    "SCRAM server verifier is not valid base64".to_owned(),
                )
            })?;
        if signature != expected_signature {
            return Err(crate::wire::WireError::SaslServerSignatureMismatch);
        }
        Ok(())
    }
}

#[derive(Debug)]
struct ScramServerFirst {
    nonce: String,
    salt: String,
    iterations: u32,
}

impl ScramServerFirst {
    fn parse(value: &str) -> Result<Self, crate::wire::WireError> {
        let nonce = parse_scram_attribute(value, "r").ok_or_else(|| {
            crate::wire::WireError::SaslAuthentication(
                "SCRAM server-first message is missing nonce".to_owned(),
            )
        })?;
        let salt = parse_scram_attribute(value, "s").ok_or_else(|| {
            crate::wire::WireError::SaslAuthentication(
                "SCRAM server-first message is missing salt".to_owned(),
            )
        })?;
        let iterations = parse_scram_attribute(value, "i").ok_or_else(|| {
            crate::wire::WireError::SaslAuthentication(
                "SCRAM server-first message is missing iteration count".to_owned(),
            )
        })?;
        let iterations = iterations.parse::<u32>().map_err(|_error| {
            crate::wire::WireError::SaslAuthentication(
                "SCRAM iteration count is invalid".to_owned(),
            )
        })?;
        if iterations == 0 {
            return Err(crate::wire::WireError::SaslAuthentication(
                "SCRAM iteration count must be positive".to_owned(),
            ));
        }
        Ok(Self {
            nonce,
            salt,
            iterations,
        })
    }
}

pub(crate) fn jaas_option(config: &str, key: &str) -> Option<String> {
    let needle = {
        let mut value = String::from(key);
        value.push('=');
        value
    };
    let start = config.find(&needle)?;
    let value_start = start.checked_add(needle.len())?;
    let rest = config.get(value_start..)?;
    if let Some(stripped) = rest.strip_prefix('"') {
        let end = stripped.find('"')?;
        return stripped.get(..end).map(ToOwned::to_owned);
    }
    let end = rest
        .find(|ch: char| ch == ';' || ch.is_ascii_whitespace())
        .unwrap_or(rest.len());
    rest.get(..end).map(ToOwned::to_owned)
}

fn parse_scram_attribute(value: &str, key: &str) -> Option<String> {
    let prefix = {
        let mut value = String::from(key);
        value.push('=');
        value
    };
    value
        .split(',')
        .find_map(|part| part.strip_prefix(prefix.as_str()).map(ToOwned::to_owned))
}

fn generate_nonce() -> Result<String, crate::wire::WireError> {
    let mut bytes = [0_u8; 24];
    getrandom::fill(&mut bytes).map_err(|error| {
        crate::wire::WireError::InvalidSaslConfig(format!("SCRAM nonce generation failed: {error}"))
    })?;
    Ok(general_purpose::URL_SAFE_NO_PAD.encode(bytes))
}

fn escape_scram_name(value: &str) -> String {
    value.replace('=', "=3D").replace(',', "=2C")
}

fn salted_password(
    mechanism: SaslMechanism,
    password: &[u8],
    salt: &[u8],
    iterations: u32,
) -> Result<Vec<u8>, crate::wire::WireError> {
    let mut first_input = Vec::with_capacity(salt.len().saturating_add(4));
    first_input.extend_from_slice(salt);
    first_input.extend_from_slice(&[0, 0, 0, 1]);
    let mut previous = hmac_bytes(mechanism, password, &first_input)?;
    let mut output = previous.clone();
    for _ in 1..iterations {
        previous = hmac_bytes(mechanism, password, &previous)?;
        xor_into(&mut output, &previous)?;
    }
    Ok(output)
}

fn hmac_bytes(
    mechanism: SaslMechanism,
    key: &[u8],
    message: &[u8],
) -> Result<Vec<u8>, crate::wire::WireError> {
    match mechanism {
        SaslMechanism::ScramSha256 => {
            let mut mac = Hmac::<Sha256>::new_from_slice(key).map_err(|_error| {
                crate::wire::WireError::InvalidSaslConfig("invalid SCRAM SHA-256 key".to_owned())
            })?;
            mac.update(message);
            Ok(mac.finalize().into_bytes().to_vec())
        },
        SaslMechanism::ScramSha512 => {
            let mut mac = Hmac::<Sha512>::new_from_slice(key).map_err(|_error| {
                crate::wire::WireError::InvalidSaslConfig("invalid SCRAM SHA-512 key".to_owned())
            })?;
            mac.update(message);
            Ok(mac.finalize().into_bytes().to_vec())
        },
        _ => Err(crate::wire::WireError::UnsupportedSaslMechanism(
            mechanism.as_str().to_owned(),
        )),
    }
}

fn digest_bytes(mechanism: SaslMechanism, payload: &[u8]) -> Vec<u8> {
    match mechanism {
        SaslMechanism::ScramSha256 => {
            use sha2::Digest;
            Sha256::digest(payload).to_vec()
        },
        SaslMechanism::ScramSha512 => {
            use sha2::Digest;
            Sha512::digest(payload).to_vec()
        },
        _ => Vec::new(),
    }
}

fn xor_bytes(left: &[u8], right: &[u8]) -> Result<Vec<u8>, crate::wire::WireError> {
    let mut output = left.to_vec();
    xor_into(&mut output, right)?;
    Ok(output)
}

fn xor_into(left: &mut [u8], right: &[u8]) -> Result<(), crate::wire::WireError> {
    if left.len() != right.len() {
        return Err(crate::wire::WireError::SaslAuthentication(
            "SCRAM proof length mismatch".to_owned(),
        ));
    }
    for (left, right) in left.iter_mut().zip(right.iter()) {
        *left ^= *right;
    }
    Ok(())
}

/// Security protocol wrapper on `ConnectionConfig`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecurityConfig {
    /// Selected Kafka security protocol.
    pub protocol: SecurityProtocol,
}

impl Default for SecurityConfig {
    fn default() -> Self {
        Self {
            protocol: SecurityProtocol::Plaintext,
        }
    }
}

/// SASL configuration carried by one broker connection.
#[derive(Debug, Clone, PartialEq)]
pub struct SaslConfig {
    /// Selected SASL mechanism when `security.protocol` uses SASL.
    pub mechanism: Option<SaslMechanism>,
    /// Raw Java-compatible JAAS config string.
    pub jaas_config: Option<String>,
    /// Native Rust authenticator for custom SASL flows.
    pub client_authenticator: Option<SaslClientAuthenticatorHandle>,
    /// Native Rust factory for per-session custom SASL flows.
    pub client_authenticator_factory: Option<SaslClientAuthenticatorFactoryHandle>,
    /// Java key: `sasl.login.callback.handler.class`.
    pub login_callback_handler_class: Option<String>,
    /// Java key: `sasl.client.callback.handler.class`.
    pub client_callback_handler_class: Option<String>,
    /// Java key: `sasl.kerberos.service.name`.
    pub kerberos_service_name: Option<String>,
    /// Java key: `sasl.kerberos.kinit.cmd`.
    pub kerberos_kinit_cmd: Option<String>,
    /// Java key: `sasl.kerberos.ticket.renew.window.factor`.
    pub kerberos_ticket_renew_window_factor: f64,
    /// Java key: `sasl.kerberos.ticket.renew.jitter`.
    pub kerberos_ticket_renew_jitter: f64,
    /// Java key: `sasl.kerberos.min.time.before.relogin`.
    pub kerberos_min_time_before_relogin: Duration,
    /// Java key: `sasl.oauthbearer.token.endpoint.url`.
    pub oauthbearer_token_endpoint_url: Option<String>,
    /// Java key: `sasl.oauthbearer.assertion.file`.
    pub oauthbearer_assertion_file: Option<String>,
    /// Java key: `sasl.oauthbearer.assertion.private.key.file`.
    pub oauthbearer_assertion_private_key_file: Option<String>,
    /// Java key: `sasl.oauthbearer.assertion.private.key.passphrase`.
    pub oauthbearer_assertion_private_key_passphrase: Option<String>,
    /// Java key: `sasl.oauthbearer.assertion.template.file`.
    pub oauthbearer_assertion_template_file: Option<String>,
    /// Java key: `sasl.oauthbearer.assertion.algorithm`.
    pub oauthbearer_assertion_algorithm: String,
    /// Java key: `sasl.oauthbearer.assertion.claim.aud`.
    pub oauthbearer_assertion_claim_aud: Option<String>,
    /// Java key: `sasl.oauthbearer.assertion.claim.iss`.
    pub oauthbearer_assertion_claim_iss: Option<String>,
    /// Java key: `sasl.oauthbearer.assertion.claim.sub`.
    pub oauthbearer_assertion_claim_sub: Option<String>,
    /// Java key: `sasl.oauthbearer.assertion.claim.exp.seconds`.
    pub oauthbearer_assertion_claim_exp: Duration,
    /// Java key: `sasl.oauthbearer.assertion.claim.nbf.seconds`.
    pub oauthbearer_assertion_claim_nbf: Duration,
    /// Java key: `sasl.oauthbearer.assertion.claim.jti.include`.
    pub oauthbearer_assertion_claim_jti_include: bool,
    /// Java key: `sasl.oauthbearer.client.credentials.client.id`.
    pub oauthbearer_client_id: Option<String>,
    /// Java key: `sasl.oauthbearer.client.credentials.client.secret`.
    pub oauthbearer_client_secret: Option<String>,
    /// Java key: `sasl.oauthbearer.scope`.
    pub oauthbearer_scope: Option<String>,
    /// Java key: `sasl.login.connect.timeout.ms`.
    pub login_connect_timeout: Option<Duration>,
    /// Java key: `sasl.login.read.timeout.ms`.
    pub login_read_timeout: Option<Duration>,
    /// Java key: `sasl.login.refresh.window.factor`.
    pub login_refresh_window_factor: f64,
    /// Java key: `sasl.login.refresh.window.jitter`.
    pub login_refresh_window_jitter: f64,
    /// Java key: `sasl.login.refresh.min.period.seconds`.
    pub login_refresh_min_period: Duration,
    /// Java key: `sasl.login.refresh.buffer.seconds`.
    pub login_refresh_buffer: Duration,
    /// Java key: `sasl.login.retry.backoff.ms`.
    pub login_retry_backoff: Duration,
    /// Java key: `sasl.login.retry.backoff.max.ms`.
    pub login_retry_backoff_max: Duration,
}

impl Default for SaslConfig {
    fn default() -> Self {
        Self {
            mechanism: None,
            jaas_config: None,
            client_authenticator: None,
            client_authenticator_factory: None,
            login_callback_handler_class: None,
            client_callback_handler_class: None,
            kerberos_service_name: None,
            kerberos_kinit_cmd: Some(String::from("/usr/bin/kinit")),
            kerberos_ticket_renew_window_factor: 0.8,
            kerberos_ticket_renew_jitter: 0.05,
            kerberos_min_time_before_relogin: Duration::from_mins(1),
            oauthbearer_token_endpoint_url: None,
            oauthbearer_assertion_file: None,
            oauthbearer_assertion_private_key_file: None,
            oauthbearer_assertion_private_key_passphrase: None,
            oauthbearer_assertion_template_file: None,
            oauthbearer_assertion_algorithm: String::from("RS256"),
            oauthbearer_assertion_claim_aud: None,
            oauthbearer_assertion_claim_iss: None,
            oauthbearer_assertion_claim_sub: None,
            oauthbearer_assertion_claim_exp: Duration::from_mins(1),
            oauthbearer_assertion_claim_nbf: Duration::ZERO,
            oauthbearer_assertion_claim_jti_include: false,
            oauthbearer_client_id: None,
            oauthbearer_client_secret: None,
            oauthbearer_scope: None,
            login_connect_timeout: None,
            login_read_timeout: None,
            login_refresh_window_factor: 0.8,
            login_refresh_window_jitter: 0.05,
            login_refresh_min_period: Duration::from_mins(1),
            login_refresh_buffer: Duration::from_mins(5),
            login_retry_backoff: Duration::from_millis(100),
            login_retry_backoff_max: Duration::from_secs(10),
        }
    }
}

impl SaslConfig {
    /// Sets a native Rust SASL client authenticator.
    #[must_use]
    pub fn client_authenticator(mut self, authenticator: impl SaslClientAuthenticator) -> Self {
        self.client_authenticator = Some(SaslClientAuthenticatorHandle::new(authenticator));
        self
    }

    /// Sets a native Rust SASL client authenticator factory.
    #[must_use]
    pub fn client_authenticator_factory(
        mut self,
        factory: impl SaslClientAuthenticatorFactory,
    ) -> Self {
        self.client_authenticator_factory =
            Some(SaslClientAuthenticatorFactoryHandle::new(factory));
        self
    }
}

#[derive(Debug, Default)]
pub(crate) struct OAuthTokenCache {
    token: Option<OAuthToken>,
}

#[derive(Debug, Clone)]
struct OAuthToken {
    value: String,
    issued_at: Instant,
    expires_at: Option<Instant>,
    refresh_window_jitter: f64,
}

impl OAuthTokenCache {
    fn get(&self, config: &SaslConfig) -> Option<String> {
        let token = self.token.as_ref()?;
        if token.needs_refresh(config) {
            return None;
        }
        Some(token.value.clone())
    }

    fn store(&mut self, token: OAuthToken) {
        self.token = Some(token);
    }
}

impl OAuthToken {
    fn with_refresh_jitter(mut self, config: &SaslConfig) -> Result<Self, crate::wire::WireError> {
        self.refresh_window_jitter = random_refresh_window_jitter(config)?;
        Ok(self)
    }

    fn refresh_deadline(&self, config: &SaslConfig) -> Option<Instant> {
        let expires_at = self.expires_at?;
        let lifetime = expires_at.saturating_duration_since(self.issued_at);
        let factor = config.login_refresh_window_factor.clamp(0.5, 1.0);
        let jittered_factor = (factor + self.refresh_window_jitter).clamp(factor, 1.0);
        let factor_refresh = self
            .issued_at
            .checked_add(lifetime.mul_f64(jittered_factor))
            .unwrap_or(expires_at);
        let min_refresh = self
            .issued_at
            .checked_add(config.login_refresh_min_period)
            .unwrap_or(expires_at);
        let mut refresh_at = if factor_refresh < min_refresh {
            min_refresh
        } else {
            factor_refresh
        };
        if config.login_refresh_buffer < lifetime
            && let Some(buffered) = expires_at.checked_sub(config.login_refresh_buffer)
            && buffered < refresh_at
        {
            refresh_at = buffered;
        }
        Some(refresh_at)
    }

    fn needs_refresh(&self, config: &SaslConfig) -> bool {
        let Some(refresh_at) = self.refresh_deadline(config) else {
            return false;
        };
        Instant::now() >= refresh_at
    }
}

fn random_refresh_window_jitter(config: &SaslConfig) -> Result<f64, crate::wire::WireError> {
    let max_jitter = config.login_refresh_window_jitter.clamp(0.0, 1.0);
    if max_jitter <= f64::EPSILON {
        return Ok(0.0);
    }
    let mut bytes = [0_u8; 4];
    getrandom::fill(&mut bytes).map_err(|error| {
        crate::wire::WireError::TokenRefresh(format!(
            "OAUTHBEARER refresh jitter generation failed: {error}"
        ))
    })?;
    let sample = f64::from(u32::from_be_bytes(bytes)) / f64::from(u32::MAX);
    Ok(sample * max_jitter)
}

async fn oauthbearer_token(
    config: &SaslConfig,
    tls_config: &TlsConfig,
    token_cache: &tokio::sync::Mutex<OAuthTokenCache>,
) -> Result<String, crate::wire::WireError> {
    if let Some(jaas_config) = &config.jaas_config
        && let Some(token) = jaas_option(jaas_config, "token")
            .or_else(|| jaas_option(jaas_config, "oauthBearerToken"))
    {
        return Ok(token);
    }
    let Some(endpoint) = &config.oauthbearer_token_endpoint_url else {
        return Err(crate::wire::WireError::InvalidSaslConfig(
            "OAUTHBEARER requires sasl.oauthbearer.token.endpoint.url or token in sasl.jaas.config"
                .to_owned(),
        ));
    };
    let cached_token = token_cache.lock().await.get(config);
    if let Some(token) = cached_token {
        return Ok(token);
    }
    let path = endpoint
        .strip_prefix("file://")
        .unwrap_or(endpoint.as_str());
    if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        let token = fetch_oauthbearer_http_token(config, tls_config, endpoint).await?;
        token_cache.lock().await.store(token.clone());
        return Ok(token.value);
    }
    let token = fs::read_to_string(path).map_err(|error| {
        crate::wire::WireError::TokenRefresh(format!(
            "cannot read sasl.oauthbearer.token.endpoint.url: {error}"
        ))
    })?;
    if token.trim().is_empty() {
        return Err(crate::wire::WireError::TokenRefresh(
            "OAUTHBEARER token file is empty".to_owned(),
        ));
    }
    let token = OAuthToken {
        value: token,
        issued_at: Instant::now(),
        expires_at: None,
        refresh_window_jitter: 0.0,
    }
    .with_refresh_jitter(config)?;
    token_cache.lock().await.store(token.clone());
    Ok(token.value)
}

trait TokenEndpointIo: AsyncRead + AsyncWrite + Unpin + Send {}

impl<T> TokenEndpointIo for T where T: AsyncRead + AsyncWrite + Unpin + Send {}

type TokenEndpointStream = Box<dyn TokenEndpointIo>;

#[derive(Debug)]
struct OAuthEndpoint {
    tls: bool,
    host: String,
    port: u16,
    path: String,
}

async fn fetch_oauthbearer_http_token(
    config: &SaslConfig,
    tls_config: &TlsConfig,
    endpoint: &str,
) -> Result<OAuthToken, crate::wire::WireError> {
    let mut backoff = config
        .login_retry_backoff
        .min(config.login_retry_backoff_max);
    let mut retries_remaining = 2_u8;
    loop {
        match fetch_oauthbearer_http_token_once(config, tls_config, endpoint).await {
            Ok(token) => return token.with_refresh_jitter(config),
            Err(_error) if !backoff.is_zero() && retries_remaining > 0 => {
                retries_remaining = retries_remaining.saturating_sub(1);
                time::sleep(backoff).await;
                backoff = next_login_retry_backoff(backoff, config.login_retry_backoff_max);
            },
            Err(error) => return Err(error),
        }
    }
}

async fn fetch_oauthbearer_http_token_once(
    config: &SaslConfig,
    tls_config: &TlsConfig,
    endpoint: &str,
) -> Result<OAuthToken, crate::wire::WireError> {
    let endpoint = OAuthEndpoint::parse(endpoint)?;
    let connect = async {
        let stream = TcpStream::connect(endpoint.addr()).await.map_err(|error| {
            crate::wire::WireError::TokenRefresh(format!(
                "cannot connect to OAUTHBEARER token endpoint: {error}"
            ))
        })?;
        if endpoint.tls {
            let tls = tls::connect_client(stream, tls_config, &endpoint.host).await?;
            Ok::<TokenEndpointStream, crate::wire::WireError>(Box::new(tls))
        } else {
            Ok::<TokenEndpointStream, crate::wire::WireError>(Box::new(stream))
        }
    };
    let mut stream = with_optional_timeout(
        connect,
        config.login_connect_timeout,
        "OAUTHBEARER token endpoint connect timed out",
    )
    .await?;
    let request = oauthbearer_http_request(config, &endpoint)?;
    stream
        .write_all(request.as_bytes())
        .await
        .map_err(|error| {
            crate::wire::WireError::TokenRefresh(format!(
                "cannot write OAUTHBEARER token request: {error}"
            ))
        })?;
    stream.flush().await.map_err(|error| {
        crate::wire::WireError::TokenRefresh(format!(
            "cannot flush OAUTHBEARER token request: {error}"
        ))
    })?;
    let read = async {
        let mut response = Vec::new();
        let _bytes_read = stream.read_to_end(&mut response).await.map_err(|error| {
            crate::wire::WireError::TokenRefresh(format!(
                "cannot read OAUTHBEARER token response: {error}"
            ))
        })?;
        Ok::<Vec<u8>, crate::wire::WireError>(response)
    };
    let response = with_optional_timeout(
        read,
        config.login_read_timeout,
        "OAUTHBEARER token endpoint read timed out",
    )
    .await?;
    parse_oauthbearer_http_response(&response)
}

fn next_login_retry_backoff(current: Duration, max: Duration) -> Duration {
    if current >= max {
        return Duration::ZERO;
    }
    current.saturating_mul(2).min(max)
}

async fn with_optional_timeout<F, T>(
    future: F,
    timeout: Option<Duration>,
    message: &'static str,
) -> Result<T, crate::wire::WireError>
where
    F: Future<Output = Result<T, crate::wire::WireError>>,
{
    if let Some(timeout) = timeout {
        time::timeout(timeout, future)
            .await
            .map_err(|_error| crate::wire::WireError::TokenRefresh(message.to_owned()))?
    } else {
        future.await
    }
}

impl OAuthEndpoint {
    fn parse(endpoint: &str) -> Result<Self, crate::wire::WireError> {
        let (tls, rest, default_port) = if let Some(rest) = endpoint.strip_prefix("https://") {
            (true, rest, 443)
        } else if let Some(rest) = endpoint.strip_prefix("http://") {
            (false, rest, 80)
        } else {
            return Err(crate::wire::WireError::TokenRefresh(
                "OAUTHBEARER token endpoint must be http:// or https://".to_owned(),
            ));
        };
        let (authority, path) = rest
            .split_once('/')
            .map_or((rest, "/"), |(authority, path)| {
                (authority, path.strip_prefix('/').unwrap_or(path))
            });
        let path = if path == "/" {
            String::from("/")
        } else {
            let mut output = String::from("/");
            output.push_str(path);
            output
        };
        let (host, port) = authority
            .split_once(':')
            .map_or_else(|| Ok((authority.to_owned(), default_port)), parse_host_port)?;
        if host.is_empty() {
            return Err(crate::wire::WireError::TokenRefresh(
                "OAUTHBEARER token endpoint host is empty".to_owned(),
            ));
        }
        Ok(Self {
            tls,
            host,
            port,
            path,
        })
    }

    fn addr(&self) -> String {
        let mut addr = self.host.clone();
        addr.push(':');
        addr.push_str(&self.port.to_string());
        addr
    }
}

fn parse_host_port(value: (&str, &str)) -> Result<(String, u16), crate::wire::WireError> {
    let (host, port) = value;
    let port = port.parse::<u16>().map_err(|_error| {
        crate::wire::WireError::TokenRefresh(
            "OAUTHBEARER token endpoint port is invalid".to_owned(),
        )
    })?;
    Ok((host.to_owned(), port))
}

fn oauthbearer_http_request(
    config: &SaslConfig,
    endpoint: &OAuthEndpoint,
) -> Result<String, crate::wire::WireError> {
    let body = oauthbearer_form_body(config)?;
    let mut request = String::from("POST ");
    request.push_str(&endpoint.path);
    request.push_str(" HTTP/1.1\r\nHost: ");
    request.push_str(&endpoint.host);
    request.push_str("\r\nContent-Type: application/x-www-form-urlencoded\r\n");
    request.push_str("Accept: application/json\r\nConnection: close\r\nContent-Length: ");
    request.push_str(&body.len().to_string());
    request.push_str("\r\n\r\n");
    request.push_str(&body);
    Ok(request)
}

fn oauthbearer_form_body(config: &SaslConfig) -> Result<String, crate::wire::WireError> {
    if let Some(path) = &config.oauthbearer_assertion_file {
        let assertion = fs::read_to_string(path).map_err(|error| {
            crate::wire::WireError::TokenRefresh(format!(
                "cannot read sasl.oauthbearer.assertion.file: {error}"
            ))
        })?;
        let assertion = assertion.trim();
        if assertion.is_empty() {
            return Err(crate::wire::WireError::TokenRefresh(
                "sasl.oauthbearer.assertion.file is empty".to_owned(),
            ));
        }
        let mut body =
            String::from("grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer");
        body.push_str("&assertion=");
        body.push_str(&form_encode(assertion)?);
        if let Some(scope) = oauth_scope(config) {
            body.push_str("&scope=");
            body.push_str(&form_encode(&scope)?);
        }
        return Ok(body);
    }
    if config.oauthbearer_assertion_private_key_file.is_some() {
        let assertion = build_oauthbearer_assertion(config)?;
        let mut body =
            String::from("grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer");
        body.push_str("&assertion=");
        body.push_str(&form_encode(&assertion)?);
        if let Some(scope) = oauth_scope(config) {
            body.push_str("&scope=");
            body.push_str(&form_encode(&scope)?);
        }
        return Ok(body);
    }
    let client_id = oauth_config_value(
        config,
        config.oauthbearer_client_id.as_ref(),
        "clientId",
        "sasl.oauthbearer.client.credentials.client.id",
    )?;
    let client_secret = oauth_config_value(
        config,
        config.oauthbearer_client_secret.as_ref(),
        "clientSecret",
        "sasl.oauthbearer.client.credentials.client.secret",
    )?;
    let mut body = String::from("grant_type=client_credentials&client_id=");
    body.push_str(&form_encode(&client_id)?);
    body.push_str("&client_secret=");
    body.push_str(&form_encode(&client_secret)?);
    if let Some(scope) = oauth_scope(config) {
        body.push_str("&scope=");
        body.push_str(&form_encode(&scope)?);
    }
    Ok(body)
}

fn build_oauthbearer_assertion(config: &SaslConfig) -> Result<String, crate::wire::WireError> {
    let key_path = config
        .oauthbearer_assertion_private_key_file
        .as_ref()
        .ok_or_else(|| {
            crate::wire::WireError::InvalidSaslConfig(
                "OAUTHBEARER assertion builder requires \
                 sasl.oauthbearer.assertion.private.key.file"
                    .to_owned(),
            )
        })?;
    let key = fs::read(key_path).map_err(|error| {
        crate::wire::WireError::TokenRefresh(format!(
            "cannot read sasl.oauthbearer.assertion.private.key.file: {error}"
        ))
    })?;
    let algorithm = config
        .oauthbearer_assertion_algorithm
        .parse::<Algorithm>()
        .map_err(|_error| {
            crate::wire::WireError::InvalidSaslConfig(format!(
                "unsupported sasl.oauthbearer.assertion.algorithm `{}`",
                config.oauthbearer_assertion_algorithm
            ))
        })?;
    let encoding_key = oauthbearer_assertion_encoding_key(
        algorithm,
        &key,
        config
            .oauthbearer_assertion_private_key_passphrase
            .as_deref(),
    )?;
    let (mut header, mut claims) = oauthbearer_assertion_template(config)?;
    header.alg = algorithm;
    merge_oauthbearer_config_claims(config, &mut claims)?;
    jsonwebtoken::encode(&header, &Value::Object(claims), &encoding_key).map_err(|error| {
        crate::wire::WireError::TokenRefresh(format!("cannot sign OAUTHBEARER assertion: {error}"))
    })
}

fn oauthbearer_assertion_encoding_key(
    algorithm: Algorithm,
    key: &[u8],
    passphrase: Option<&str>,
) -> Result<EncodingKey, crate::wire::WireError> {
    match algorithm.family() {
        AlgorithmFamily::Rsa => oauthbearer_pem_or_encrypted_pkcs8_key(
            key,
            passphrase,
            EncodingKey::from_rsa_der,
            EncodingKey::from_rsa_pem,
        ),
        AlgorithmFamily::Ec => oauthbearer_pem_or_encrypted_pkcs8_key(
            key,
            passphrase,
            EncodingKey::from_ec_der,
            EncodingKey::from_ec_pem,
        ),
        AlgorithmFamily::Ed => oauthbearer_pem_or_encrypted_pkcs8_key(
            key,
            passphrase,
            EncodingKey::from_ed_der,
            EncodingKey::from_ed_pem,
        ),
        AlgorithmFamily::Hmac => Err(crate::wire::WireError::InvalidSaslConfig(
            "OAUTHBEARER assertion private-key flow does not support HMAC algorithms".to_owned(),
        )),
    }
}

fn oauthbearer_pem_or_encrypted_pkcs8_key(
    key: &[u8],
    passphrase: Option<&str>,
    der_key: fn(&[u8]) -> EncodingKey,
    pem_key: fn(&[u8]) -> jsonwebtoken::errors::Result<EncodingKey>,
) -> Result<EncodingKey, crate::wire::WireError> {
    match pem_key(key) {
        Ok(key) => Ok(key),
        Err(error) => {
            let Some(passphrase) = passphrase else {
                return Err(crate::wire::WireError::TokenRefresh(format!(
                    "cannot load sasl.oauthbearer.assertion.private.key.file: {error}"
                )));
            };
            let decrypted = encrypted_pkcs8_der(key, passphrase).map_err(|error| {
                crate::wire::WireError::TokenRefresh(format!(
                    "cannot load sasl.oauthbearer.assertion.private.key.file: {error}"
                ))
            })?;
            Ok(der_key(decrypted.as_bytes()))
        },
    }
}

fn encrypted_pkcs8_der(pem: &[u8], passphrase: &str) -> Result<pkcs8::SecretDocument, String> {
    let pem = str::from_utf8(pem).map_err(|error| format!("private key is not UTF-8: {error}"))?;
    let (label, document) = pkcs8::SecretDocument::from_pem(pem)
        .map_err(|error| format!("private key is not encrypted PKCS#8 PEM: {error}"))?;
    pkcs8::EncryptedPrivateKeyInfoRef::validate_pem_label(label)
        .map_err(|error| format!("private key is not encrypted PKCS#8 PEM: {error}"))?;
    let encrypted = pkcs8::EncryptedPrivateKeyInfoRef::from_der(document.as_bytes())
        .map_err(|error| format!("private key encrypted PKCS#8 DER is invalid: {error}"))?;
    encrypted
        .decrypt(passphrase.as_bytes())
        .map_err(|error| format!("cannot decrypt private key: {error}"))
}

fn oauthbearer_assertion_template(
    config: &SaslConfig,
) -> Result<(Header, Map<String, Value>), crate::wire::WireError> {
    let Some(path) = &config.oauthbearer_assertion_template_file else {
        return Ok((Header::default(), Map::new()));
    };
    let template = fs::read_to_string(path).map_err(|error| {
        crate::wire::WireError::TokenRefresh(format!(
            "cannot read sasl.oauthbearer.assertion.template.file: {error}"
        ))
    })?;
    let value = serde_json::from_str::<Value>(&template).map_err(|error| {
        crate::wire::WireError::TokenRefresh(format!(
            "sasl.oauthbearer.assertion.template.file is not valid JSON: {error}"
        ))
    })?;
    let object = value.as_object().ok_or_else(|| {
        crate::wire::WireError::TokenRefresh(
            "sasl.oauthbearer.assertion.template.file must contain a JSON object".to_owned(),
        )
    })?;
    let mut header = Header::default();
    if let Some(kid) = object
        .get("header")
        .and_then(Value::as_object)
        .and_then(|header| header.get("kid"))
        .and_then(Value::as_str)
    {
        header.kid = Some(kid.to_owned());
    }
    let claims = object
        .get("claims")
        .map_or_else(|| Ok(Map::new()), template_claims_object)?;
    Ok((header, claims))
}

fn template_claims_object(value: &Value) -> Result<Map<String, Value>, crate::wire::WireError> {
    value.as_object().cloned().ok_or_else(|| {
        crate::wire::WireError::TokenRefresh(
            "sasl.oauthbearer.assertion.template.file claims must be a JSON object".to_owned(),
        )
    })
}

fn merge_oauthbearer_config_claims(
    config: &SaslConfig,
    claims: &mut Map<String, Value>,
) -> Result<(), crate::wire::WireError> {
    if let Some(aud) = &config.oauthbearer_assertion_claim_aud {
        set_claim(claims, "aud", Value::String(aud.clone()));
    }
    if let Some(iss) = &config.oauthbearer_assertion_claim_iss {
        set_claim(claims, "iss", Value::String(iss.clone()));
    }
    if let Some(sub) = &config.oauthbearer_assertion_claim_sub {
        set_claim(claims, "sub", Value::String(sub.clone()));
    }
    let now = now_unix_seconds()?;
    let exp = now.saturating_add(config.oauthbearer_assertion_claim_exp.as_secs());
    let nbf = now.saturating_sub(config.oauthbearer_assertion_claim_nbf.as_secs());
    set_claim(claims, "exp", Value::from(exp));
    set_claim(claims, "nbf", Value::from(nbf));
    if config.oauthbearer_assertion_claim_jti_include {
        set_claim(claims, "jti", Value::String(generate_jti()?));
    }
    Ok(())
}

fn set_claim(claims: &mut Map<String, Value>, key: &str, value: Value) {
    let _previous = claims.insert(key.to_owned(), value);
}

fn now_unix_seconds() -> Result<u64, crate::wire::WireError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|error| {
            crate::wire::WireError::TokenRefresh(format!(
                "system clock is before Unix epoch: {error}"
            ))
        })
}

fn generate_jti() -> Result<String, crate::wire::WireError> {
    let mut bytes = [0_u8; 16];
    getrandom::fill(&mut bytes).map_err(|error| {
        crate::wire::WireError::TokenRefresh(format!(
            "OAUTHBEARER assertion jti generation failed: {error}"
        ))
    })?;
    Ok(general_purpose::URL_SAFE_NO_PAD.encode(bytes))
}

fn oauth_scope(config: &SaslConfig) -> Option<String> {
    config.oauthbearer_scope.clone().or_else(|| {
        config
            .jaas_config
            .as_deref()
            .and_then(|jaas| jaas_option(jaas, "scope"))
    })
}

fn oauth_config_value(
    config: &SaslConfig,
    direct: Option<&String>,
    jaas_key: &str,
    config_key: &str,
) -> Result<String, crate::wire::WireError> {
    direct
        .cloned()
        .or_else(|| {
            config
                .jaas_config
                .as_deref()
                .and_then(|jaas| jaas_option(jaas, jaas_key))
        })
        .ok_or_else(|| {
            crate::wire::WireError::InvalidSaslConfig(format!(
                "OAUTHBEARER HTTP token endpoint requires {config_key}"
            ))
        })
}

fn form_encode(value: &str) -> Result<String, crate::wire::WireError> {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                encoded.push(char::from(byte));
            },
            b' ' => encoded.push('+'),
            _ => write!(&mut encoded, "%{byte:02X}").map_err(|_error| {
                crate::wire::WireError::TokenRefresh(
                    "cannot encode OAUTHBEARER token request".to_owned(),
                )
            })?,
        }
    }
    Ok(encoded)
}

fn parse_oauthbearer_http_response(response: &[u8]) -> Result<OAuthToken, crate::wire::WireError> {
    let response = str::from_utf8(response).map_err(|_error| {
        crate::wire::WireError::TokenRefresh("OAUTHBEARER token response is not UTF-8".to_owned())
    })?;
    let (head, body) = response.split_once("\r\n\r\n").ok_or_else(|| {
        crate::wire::WireError::TokenRefresh(
            "OAUTHBEARER token response is missing HTTP headers".to_owned(),
        )
    })?;
    let status_code = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .ok_or_else(|| {
            crate::wire::WireError::TokenRefresh(
                "OAUTHBEARER token response is missing status".to_owned(),
            )
        })?
        .parse::<u16>()
        .map_err(|_error| {
            crate::wire::WireError::TokenRefresh(
                "OAUTHBEARER token response status is invalid".to_owned(),
            )
        })?;
    if !(200..300).contains(&status_code) {
        return Err(crate::wire::WireError::TokenRefresh(format!(
            "OAUTHBEARER token endpoint returned HTTP {status_code}"
        )));
    }
    let body = serde_json::from_str::<Value>(body).map_err(|error| {
        crate::wire::WireError::TokenRefresh(format!(
            "OAUTHBEARER token response is not valid JSON: {error}"
        ))
    })?;
    let token = body
        .get("access_token")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            crate::wire::WireError::TokenRefresh(
                "OAUTHBEARER token response is missing access_token".to_owned(),
            )
        })?;
    if token.is_empty() {
        return Err(crate::wire::WireError::TokenRefresh(
            "OAUTHBEARER access_token is empty".to_owned(),
        ));
    }
    let issued_at = Instant::now();
    let expires_at = body
        .get("expires_in")
        .and_then(Value::as_u64)
        .map(|seconds| {
            issued_at
                .checked_add(Duration::from_secs(seconds))
                .unwrap_or(issued_at)
        });
    Ok(OAuthToken {
        value: token.to_owned(),
        issued_at,
        expires_at,
        refresh_window_jitter: 0.0,
    })
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::expect_used,
        clippy::missing_assert_message,
        reason = "Unit tests use direct fixture construction and fail-fast assertions."
    )]

    use std::time::{Duration, Instant};

    use super::{OAuthToken, SaslConfig};

    #[test]
    fn oauth_token_refresh_deadline_applies_configured_jitter_once() {
        let issued_at = Instant::now();
        let expires_at = issued_at
            .checked_add(Duration::from_secs(100))
            .expect("test instant should stay in range");
        let config = SaslConfig {
            login_refresh_window_factor: 0.5,
            login_refresh_window_jitter: 0.1,
            login_refresh_min_period: Duration::ZERO,
            login_refresh_buffer: Duration::from_secs(1),
            ..SaslConfig::default()
        };
        let token = OAuthToken {
            value: String::from("token"),
            issued_at,
            expires_at: Some(expires_at),
            refresh_window_jitter: 0.1,
        };

        let refresh_at = token
            .refresh_deadline(&config)
            .expect("expiring token should have refresh deadline");

        assert_eq!(refresh_at.duration_since(issued_at), Duration::from_mins(1));
    }
}
