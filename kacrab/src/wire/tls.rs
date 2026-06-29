//! TLS configuration and rustls client connection support.

use std::{fs, io::Cursor, str, string::String, sync::Arc, vec::Vec};

use pkcs8::der::{Decode, pem::PemLabel};
use rustls::{
    ClientConfig, DigitallySignedStruct, Error as RustlsError, RootCertStore, SignatureScheme,
    SupportedProtocolVersion,
    client::{
        danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
        verify_server_cert_signed_by_trust_anchor,
    },
    crypto::{CryptoProvider, WebPkiSupportedAlgorithms},
    pki_types::{
        CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName, UnixTime, pem::PemObject,
    },
    server::ParsedCertificate,
};
use tokio::net::TcpStream;
use tokio_rustls::{TlsConnector, client::TlsStream};

use super::{Result, WireError};

/// TLS settings for `SSL` and `SASL_SSL` broker connections.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlsConfig {
    /// Kafka key: `ssl.truststore.location`.
    pub truststore_location: Option<String>,
    /// Kafka key: `ssl.truststore.password`.
    pub truststore_password: Option<String>,
    /// Kafka key: `ssl.truststore.certificates`.
    pub truststore_certificates: Option<String>,
    /// Kafka key: `ssl.truststore.type`.
    pub truststore_type: Option<String>,
    /// Kafka key: `ssl.keystore.location`.
    pub keystore_location: Option<String>,
    /// Kafka key: `ssl.keystore.password`.
    pub keystore_password: Option<String>,
    /// Kafka key: `ssl.keystore.key`.
    pub keystore_key: Option<String>,
    /// Kafka key: `ssl.keystore.certificate.chain`.
    pub keystore_certificate_chain: Option<String>,
    /// Kafka key: `ssl.keystore.type`.
    pub keystore_type: Option<String>,
    /// Kafka key: `ssl.key.password`.
    pub key_password: Option<String>,
    /// Kafka key: `ssl.endpoint.identification.algorithm`.
    pub endpoint_identification_algorithm: Option<String>,
    /// Kafka key: `ssl.protocol`.
    pub protocol: String,
    /// Kafka key: `ssl.enabled.protocols`.
    pub enabled_protocols: Option<String>,
    /// Kafka key: `ssl.cipher.suites`.
    pub cipher_suites: Option<String>,
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            truststore_location: None,
            truststore_password: None,
            truststore_certificates: None,
            truststore_type: None,
            keystore_location: None,
            keystore_password: None,
            keystore_key: None,
            keystore_certificate_chain: None,
            keystore_type: None,
            key_password: None,
            endpoint_identification_algorithm: Some(String::from("https")),
            protocol: String::from("TLSv1.3"),
            enabled_protocols: Some(String::from("TLSv1.2,TLSv1.3")),
            cipher_suites: None,
        }
    }
}

pub(crate) async fn connect_client(
    stream: TcpStream,
    config: &TlsConfig,
    server_name: &str,
) -> Result<TlsStream<TcpStream>> {
    validate_supported_options(config)?;
    let roots = root_store(config)?;
    let provider = CryptoProvider::get_default()
        .cloned()
        .unwrap_or_else(|| Arc::new(rustls::crypto::aws_lc_rs::default_provider()));
    let versions = enabled_protocol_versions(config)?;
    let builder = ClientConfig::builder_with_provider(Arc::clone(&provider))
        .with_protocol_versions(&versions)
        .map_err(|error| WireError::InvalidTlsConfig(format!("invalid TLS versions: {error}")))?;
    let builder = if config
        .endpoint_identification_algorithm
        .as_deref()
        .is_some_and(str::is_empty)
    {
        let verifier = SkipServerNameVerifier {
            roots: roots.clone(),
            supported: provider.signature_verification_algorithms,
        };
        builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(verifier))
    } else {
        builder.with_root_certificates(roots)
    };
    let client_config = if let Some((certs, key)) = client_identity(config)? {
        builder
            .with_client_auth_cert(certs, key)
            .map_err(|error| WireError::InvalidTlsConfig(format!("invalid client cert: {error}")))?
    } else {
        builder.with_no_client_auth()
    };
    let server_name = ServerName::try_from(server_name.to_owned())
        .map_err(|_error| WireError::InvalidTlsConfig("invalid TLS server name".to_owned()))?;
    TlsConnector::from(Arc::new(client_config))
        .connect(server_name, stream)
        .await
        .map_err(|error| WireError::TlsHandshake(error.to_string()))
}

fn validate_supported_options(config: &TlsConfig) -> Result<()> {
    if config.cipher_suites.is_some() {
        return Err(WireError::UnsupportedTlsOption(
            "ssl.cipher.suites".to_owned(),
        ));
    }
    Ok(())
}

#[derive(Debug)]
struct SkipServerNameVerifier {
    roots: RootCertStore,
    supported: WebPkiSupportedAlgorithms,
}

impl ServerCertVerifier for SkipServerNameVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        now: UnixTime,
    ) -> std::result::Result<ServerCertVerified, RustlsError> {
        let cert = ParsedCertificate::try_from(end_entity)?;
        verify_server_cert_signed_by_trust_anchor(
            &cert,
            &self.roots,
            intermediates,
            now,
            self.supported.all,
        )?;
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, RustlsError> {
        rustls::crypto::verify_tls12_signature(message, cert, dss, &self.supported)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, RustlsError> {
        rustls::crypto::verify_tls13_signature(message, cert, dss, &self.supported)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.supported.supported_schemes()
    }
}

fn enabled_protocol_versions(config: &TlsConfig) -> Result<Vec<&'static SupportedProtocolVersion>> {
    let _preferred = protocol_version(config.protocol.as_str())?;
    let protocols = config
        .enabled_protocols
        .as_deref()
        .unwrap_or("TLSv1.2,TLSv1.3");
    let mut versions: Vec<&'static SupportedProtocolVersion> = Vec::new();
    for protocol in protocols.split(',') {
        let protocol = protocol.trim();
        if protocol.is_empty() {
            continue;
        }
        let version = protocol_version(protocol)?;
        if !versions
            .iter()
            .any(|existing| existing.version == version.version)
        {
            versions.push(version);
        }
    }
    if versions.is_empty() {
        return Err(WireError::InvalidTlsConfig(
            "ssl.enabled.protocols does not contain a supported TLS protocol".to_owned(),
        ));
    }
    Ok(versions)
}

fn protocol_version(value: &str) -> Result<&'static SupportedProtocolVersion> {
    match value {
        value if value.eq_ignore_ascii_case("TLSv1.3") => Ok(&rustls::version::TLS13),
        value if value.eq_ignore_ascii_case("TLSv1.2") => Ok(&rustls::version::TLS12),
        value if value.eq_ignore_ascii_case("TLS") => Ok(&rustls::version::TLS13),
        _ => Err(WireError::UnsupportedTlsOption(format!(
            "TLS protocol {value}"
        ))),
    }
}

fn root_store(config: &TlsConfig) -> Result<RootCertStore> {
    let mut roots = RootCertStore::empty();
    let native = rustls_native_certs::load_native_certs();
    for cert in native.certs {
        roots.add(cert).map_err(|error| {
            WireError::InvalidTlsConfig(format!("native root certificate rejected: {error}"))
        })?;
    }
    if let Some(pem) = &config.truststore_certificates {
        add_pem_certs(&mut roots, pem.as_bytes(), "ssl.truststore.certificates")?;
    }
    if let Some(path) = &config.truststore_location {
        let store_type = config
            .truststore_type
            .as_deref()
            .unwrap_or("PEM")
            .to_ascii_uppercase();
        let contents = fs::read(path).map_err(|error| {
            WireError::InvalidTlsConfig(format!("cannot open ssl.truststore.location: {error}"))
        })?;
        match store_type.as_str() {
            "PEM" => add_pem_certs(&mut roots, &contents, "ssl.truststore.location")?,
            "JKS" => add_jks_truststore(&mut roots, &contents, config)?,
            "PKCS12" | "PKCS#12" => add_pkcs12_truststore(&mut roots, &contents, config)?,
            _ => {
                return Err(WireError::UnsupportedTlsOption(format!(
                    "ssl.truststore.type={store_type}"
                )));
            },
        }
    }
    if roots.is_empty() {
        return Err(WireError::InvalidTlsConfig(
            "TLS root store is empty".to_owned(),
        ));
    }
    Ok(roots)
}

fn add_jks_truststore(
    roots: &mut RootCertStore,
    contents: &[u8],
    config: &TlsConfig,
) -> Result<()> {
    let password = required_store_password(
        config.truststore_password.as_deref(),
        "ssl.truststore.password",
        "ssl.truststore.type=JKS",
    )?;
    let mut keystore = jks::KeyStore::new();
    keystore
        .load(Cursor::new(contents), password.as_bytes())
        .map_err(|error| {
            WireError::InvalidTlsConfig(format!(
                "cannot load ssl.truststore.location as JKS: {error}"
            ))
        })?;
    let mut count = 0_usize;
    for alias in keystore.aliases() {
        if keystore.is_trusted_certificate_entry(&alias) {
            let entry = keystore
                .get_trusted_certificate_entry(&alias)
                .map_err(|error| {
                    WireError::InvalidTlsConfig(format!(
                        "cannot read JKS trusted cert {alias}: {error}"
                    ))
                })?;
            add_der_root(roots, entry.certificate.content, "ssl.truststore.location")?;
            count = count.saturating_add(1);
        } else if keystore.is_private_key_entry(&alias) {
            let chain = keystore
                .get_private_key_entry_certificate_chain(&alias)
                .map_err(|error| {
                    WireError::InvalidTlsConfig(format!(
                        "cannot read JKS private key cert chain {alias}: {error}"
                    ))
                })?;
            for cert in chain {
                add_der_root(roots, cert.content, "ssl.truststore.location")?;
                count = count.saturating_add(1);
            }
        }
    }
    if count == 0 {
        return Err(WireError::InvalidTlsConfig(
            "ssl.truststore.location contains no JKS certificates".to_owned(),
        ));
    }
    Ok(())
}

fn add_pkcs12_truststore(
    roots: &mut RootCertStore,
    contents: &[u8],
    config: &TlsConfig,
) -> Result<()> {
    let password = required_store_password(
        config.truststore_password.as_deref(),
        "ssl.truststore.password",
        "ssl.truststore.type=PKCS12",
    )?;
    let keystore = p12_keystore::KeyStore::from_pkcs12(
        contents,
        password,
        p12_keystore::Pkcs12ImportPolicy::Relaxed,
    )
    .map_err(|error| {
        WireError::InvalidTlsConfig(format!(
            "cannot load ssl.truststore.location as PKCS12: {error}"
        ))
    })?;
    let mut count = 0_usize;
    for (_alias, entry) in keystore.entries() {
        match entry {
            p12_keystore::KeyStoreEntry::Certificate(cert) => {
                add_der_root(roots, cert.as_der().to_vec(), "ssl.truststore.location")?;
                count = count.saturating_add(1);
            },
            p12_keystore::KeyStoreEntry::PrivateKeyChain(chain) => {
                for cert in chain.certs() {
                    add_der_root(roots, cert.as_der().to_vec(), "ssl.truststore.location")?;
                    count = count.saturating_add(1);
                }
            },
            p12_keystore::KeyStoreEntry::Secret(_) => {},
        }
    }
    if count == 0 {
        return Err(WireError::InvalidTlsConfig(
            "ssl.truststore.location contains no PKCS12 certificates".to_owned(),
        ));
    }
    Ok(())
}

fn add_der_root(roots: &mut RootCertStore, cert: Vec<u8>, source: &str) -> Result<()> {
    roots.add(CertificateDer::from(cert)).map_err(|error| {
        WireError::InvalidTlsConfig(format!("{source} certificate rejected: {error}"))
    })
}

fn required_store_password<'a>(
    password: Option<&'a str>,
    password_key: &str,
    store_type: &str,
) -> Result<&'a str> {
    password
        .ok_or_else(|| WireError::InvalidTlsConfig(format!("{store_type} requires {password_key}")))
}

fn add_pem_certs(roots: &mut RootCertStore, pem: &[u8], source: &str) -> Result<()> {
    let mut count = 0_usize;
    for cert in CertificateDer::pem_slice_iter(pem) {
        let cert = cert.map_err(|error| {
            WireError::InvalidTlsConfig(format!("invalid PEM certificate in {source}: {error}"))
        })?;
        roots.add(cert).map_err(|error| {
            WireError::InvalidTlsConfig(format!("{source} certificate rejected: {error}"))
        })?;
        count = count.saturating_add(1);
    }
    if count == 0 {
        return Err(WireError::InvalidTlsConfig(format!(
            "{source} contains no PEM certificates"
        )));
    }
    Ok(())
}

fn client_identity(
    config: &TlsConfig,
) -> Result<Option<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)>> {
    match (
        &config.keystore_certificate_chain,
        &config.keystore_key,
        &config.keystore_location,
    ) {
        (Some(chain), Some(key), _) => {
            let certs = pem_certs(chain.as_bytes(), "ssl.keystore.certificate.chain")?;
            let key = pem_private_key(
                key.as_bytes(),
                config.key_password.as_deref(),
                "ssl.keystore.key",
            )?;
            Ok(Some((certs, key)))
        },
        (None, None, Some(path)) => {
            let store_type = config
                .keystore_type
                .as_deref()
                .unwrap_or("PEM")
                .to_ascii_uppercase();
            let contents = fs::read(path).map_err(|error| {
                WireError::InvalidTlsConfig(format!("cannot open ssl.keystore.location: {error}"))
            })?;
            match store_type.as_str() {
                "PEM" => {
                    let certs = pem_certs(&contents, "ssl.keystore.location")?;
                    let key = pem_private_key(
                        &contents,
                        config.key_password.as_deref(),
                        "ssl.keystore.location",
                    )?;
                    Ok(Some((certs, key)))
                },
                "JKS" => jks_client_identity(&contents, config),
                "PKCS12" | "PKCS#12" => pkcs12_client_identity(&contents, config),
                _ => Err(WireError::UnsupportedTlsOption(format!(
                    "ssl.keystore.type={store_type}"
                ))),
            }
        },
        (None, None, None) => Ok(None),
        _ => Err(WireError::InvalidTlsConfig(
            "TLS client auth requires both ssl.keystore.certificate.chain and ssl.keystore.key"
                .to_owned(),
        )),
    }
}

fn pem_certs(pem: &[u8], source: &str) -> Result<Vec<CertificateDer<'static>>> {
    let certs = CertificateDer::pem_slice_iter(pem)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| {
            WireError::InvalidTlsConfig(format!("invalid PEM certificate in {source}: {error}"))
        })?;
    if certs.is_empty() {
        return Err(WireError::InvalidTlsConfig(format!(
            "{source} contains no PEM certificates"
        )));
    }
    Ok(certs)
}

fn pem_private_key(
    pem: &[u8],
    password: Option<&str>,
    source: &str,
) -> Result<PrivateKeyDer<'static>> {
    if let Ok(key) = PrivateKeyDer::from_pem_slice(pem) {
        return Ok(key);
    }
    let password = password.ok_or_else(|| {
        WireError::InvalidTlsConfig(format!("{source} contains no PEM private key"))
    })?;
    let pem = str::from_utf8(pem).map_err(|error| {
        WireError::InvalidTlsConfig(format!("invalid UTF-8 encrypted PEM in {source}: {error}"))
    })?;
    let (label, document) = pkcs8::SecretDocument::from_pem(pem).map_err(|error| {
        WireError::InvalidTlsConfig(format!("invalid encrypted PKCS#8 PEM in {source}: {error}"))
    })?;
    pkcs8::EncryptedPrivateKeyInfoRef::validate_pem_label(label).map_err(|error| {
        WireError::InvalidTlsConfig(format!("invalid encrypted PKCS#8 PEM in {source}: {error}"))
    })?;
    let encrypted =
        pkcs8::EncryptedPrivateKeyInfoRef::from_der(document.as_bytes()).map_err(|error| {
            WireError::InvalidTlsConfig(format!(
                "invalid encrypted PKCS#8 key in {source}: {error}"
            ))
        })?;
    let decrypted = encrypted.decrypt(password.as_bytes()).map_err(|error| {
        WireError::InvalidTlsConfig(format!(
            "cannot decrypt encrypted PKCS#8 key in {source}: {error}"
        ))
    })?;
    Ok(PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
        decrypted.as_bytes().to_vec(),
    )))
}

fn jks_client_identity(
    contents: &[u8],
    config: &TlsConfig,
) -> Result<Option<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)>> {
    let store_password = required_store_password(
        config.keystore_password.as_deref(),
        "ssl.keystore.password",
        "ssl.keystore.type=JKS",
    )?;
    let key_password = config.key_password.as_deref().unwrap_or(store_password);
    let mut keystore = jks::KeyStore::new();
    keystore
        .load(Cursor::new(contents), store_password.as_bytes())
        .map_err(|error| {
            WireError::InvalidTlsConfig(format!(
                "cannot load ssl.keystore.location as JKS: {error}"
            ))
        })?;
    for alias in keystore.aliases() {
        if !keystore.is_private_key_entry(&alias) {
            continue;
        }
        let entry = keystore
            .get_private_key_entry(&alias, key_password.as_bytes())
            .map_err(|error| {
                WireError::InvalidTlsConfig(format!(
                    "cannot read JKS private key entry {alias}: {error}"
                ))
            })?;
        let certs = certificate_chain(entry.certificate_chain, "ssl.keystore.location")?;
        if certs.is_empty() {
            return Err(WireError::InvalidTlsConfig(format!(
                "JKS private key entry {alias} has no certificate chain"
            )));
        }
        let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(entry.private_key));
        return Ok(Some((certs, key)));
    }
    Err(WireError::InvalidTlsConfig(
        "ssl.keystore.location contains no JKS private key entries".to_owned(),
    ))
}

fn pkcs12_client_identity(
    contents: &[u8],
    config: &TlsConfig,
) -> Result<Option<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)>> {
    let password = required_store_password(
        config
            .keystore_password
            .as_deref()
            .or(config.key_password.as_deref()),
        "ssl.keystore.password",
        "ssl.keystore.type=PKCS12",
    )?;
    let keystore = p12_keystore::KeyStore::from_pkcs12(
        contents,
        password,
        p12_keystore::Pkcs12ImportPolicy::Relaxed,
    )
    .map_err(|error| {
        WireError::InvalidTlsConfig(format!(
            "cannot load ssl.keystore.location as PKCS12: {error}"
        ))
    })?;
    if let Some((alias, chain)) = keystore.private_key_chain() {
        let certs = chain
            .certs()
            .iter()
            .map(|cert| CertificateDer::from(cert.as_der().to_vec()))
            .collect::<Vec<_>>();
        if certs.is_empty() {
            return Err(WireError::InvalidTlsConfig(format!(
                "PKCS12 private key entry {alias} has no certificate chain"
            )));
        }
        let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(chain.key().as_der().to_vec()));
        return Ok(Some((certs, key)));
    }
    Err(WireError::InvalidTlsConfig(
        "ssl.keystore.location contains no PKCS12 private key entries".to_owned(),
    ))
}

fn certificate_chain(
    chain: Vec<jks::Certificate>,
    source: &str,
) -> Result<Vec<CertificateDer<'static>>> {
    chain
        .into_iter()
        .map(|cert| {
            if cert.content.is_empty() {
                Err(WireError::InvalidTlsConfig(format!(
                    "{source} contains an empty certificate"
                )))
            } else {
                Ok(CertificateDer::from(cert.content))
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use pkcs8::{
        EncodePrivateKey, PrivateKeyInfoOwned, SecretDocument,
        der::{Decode, pem::LineEnding},
    };
    use rcgen::{CertifiedKey, generate_simple_self_signed};
    use rustls::RootCertStore;

    use super::{
        TlsConfig, add_jks_truststore, add_pkcs12_truststore, client_identity,
        enabled_protocol_versions, pem_private_key,
    };
    use crate::wire::WireError;

    #[test]
    fn tls_enabled_protocols_select_user_configured_versions() {
        let config = TlsConfig {
            protocol: String::from("TLSv1.2"),
            enabled_protocols: Some(String::from("TLSv1.2")),
            ..TlsConfig::default()
        };

        let versions = enabled_protocol_versions(&config).expect("TLSv1.2 should be supported");

        assert_eq!(versions.len(), 1);
        assert_eq!(versions[0].version, rustls::ProtocolVersion::TLSv1_2);
    }

    #[test]
    fn tls_protocol_validation_rejects_unsupported_java_protocol_names() {
        let config = TlsConfig {
            protocol: String::from("TLSv1.1"),
            enabled_protocols: Some(String::from("TLSv1.1")),
            ..TlsConfig::default()
        };

        let error = enabled_protocol_versions(&config).unwrap_err();

        assert!(
            matches!(error, WireError::UnsupportedTlsOption(option) if option == "TLS protocol TLSv1.1")
        );
    }

    #[test]
    fn tls_root_store_loads_jks_truststore() {
        let (cert_der, _key_der, _cert_pem, _key_pem) = test_identity_material();
        let mut keystore = jks::KeyStore::new();
        keystore
            .set_trusted_certificate_entry(
                "ca",
                jks::TrustedCertificateEntry {
                    creation_time: SystemTime::UNIX_EPOCH,
                    certificate: jks::Certificate {
                        cert_type: String::from("X509"),
                        content: cert_der,
                    },
                },
            )
            .expect("JKS trusted certificate entry should be accepted");
        let mut store = Vec::new();
        keystore
            .store(&mut store, b"changeit")
            .expect("JKS truststore should serialize");
        let config = TlsConfig {
            truststore_password: Some(String::from("changeit")),
            ..TlsConfig::default()
        };
        let mut roots = RootCertStore::empty();

        add_jks_truststore(&mut roots, &store, &config).expect("JKS cert should load");

        assert!(!roots.is_empty());
    }

    #[test]
    fn tls_root_store_loads_pkcs12_truststore() {
        let (cert_der, _key_der, _cert_pem, _key_pem) = test_identity_material();
        let cert =
            p12_keystore::Certificate::from_der(&cert_der).expect("cert should parse as X.509");
        let mut keystore = p12_keystore::KeyStore::new();
        keystore.add_entry("ca", p12_keystore::KeyStoreEntry::Certificate(cert));
        let store = keystore
            .writer("changeit")
            .write()
            .expect("PKCS12 truststore should serialize");
        let config = TlsConfig {
            truststore_password: Some(String::from("changeit")),
            ..TlsConfig::default()
        };
        let mut roots = RootCertStore::empty();

        add_pkcs12_truststore(&mut roots, &store, &config).expect("PKCS12 cert should load");

        assert!(!roots.is_empty());
    }

    #[test]
    fn tls_client_identity_loads_jks_private_key_entry() {
        let (cert_der, key_der, _cert_pem, _key_pem) = test_identity_material();
        let mut keystore = jks::KeyStore::new();
        keystore
            .set_private_key_entry(
                "client",
                jks::PrivateKeyEntry {
                    creation_time: SystemTime::UNIX_EPOCH,
                    private_key: key_der.clone(),
                    certificate_chain: vec![jks::Certificate {
                        cert_type: String::from("X509"),
                        content: cert_der.clone(),
                    }],
                },
                b"keypass",
            )
            .expect("JKS client key entry should be accepted");
        let mut store = Vec::new();
        keystore
            .store(&mut store, b"storepass")
            .expect("JKS keystore should serialize");
        let config = TlsConfig {
            keystore_location: Some(write_test_file("client.jks", &store)),
            keystore_type: Some(String::from("JKS")),
            keystore_password: Some(String::from("storepass")),
            key_password: Some(String::from("keypass")),
            ..TlsConfig::default()
        };

        let (certs, key) = client_identity(&config)
            .expect("JKS identity should load")
            .expect("JKS identity should be configured");

        assert_eq!(certs.len(), 1);
        assert_eq!(certs[0].as_ref(), cert_der.as_slice());
        assert_eq!(key.secret_der(), key_der.as_slice());
    }

    #[test]
    fn tls_client_identity_decrypts_encrypted_pkcs8_pem_key() {
        let (cert_der, key_der, cert_pem, key_pem) = test_identity_material();
        let encrypted_key = encrypted_pkcs8_pem(&key_pem, "secret");
        let config = TlsConfig {
            keystore_certificate_chain: Some(cert_pem),
            keystore_key: Some(encrypted_key),
            key_password: Some(String::from("secret")),
            ..TlsConfig::default()
        };

        let (certs, key) = client_identity(&config)
            .expect("encrypted PEM identity should load")
            .expect("encrypted PEM identity should be configured");

        assert_eq!(certs[0].as_ref(), cert_der.as_slice());
        assert_eq!(key.secret_der(), key_der.as_slice());
    }

    #[test]
    fn tls_pem_private_key_rejects_encrypted_key_without_password() {
        let (_cert_der, _key_der, _cert_pem, key_pem) = test_identity_material();
        let encrypted_key = encrypted_pkcs8_pem(&key_pem, "secret");

        let error = pem_private_key(encrypted_key.as_bytes(), None, "ssl.keystore.key")
            .expect_err("encrypted key without password should fail");

        assert!(
            matches!(error, WireError::InvalidTlsConfig(message) if message.contains("no PEM private key"))
        );
    }

    fn test_identity_material() -> (Vec<u8>, Vec<u8>, String, String) {
        let CertifiedKey { cert, key_pair } =
            generate_simple_self_signed(["localhost".to_owned()]).expect("self-signed cert");
        (
            cert.der().as_ref().to_vec(),
            key_pair.serialize_der(),
            cert.pem(),
            key_pair.serialize_pem(),
        )
    }

    fn encrypted_pkcs8_pem(key_pem: &str, password: &str) -> String {
        let (_label, document) =
            SecretDocument::from_pem(key_pem).expect("plain PKCS#8 PEM should parse");
        let private_key = PrivateKeyInfoOwned::from_der(document.as_bytes())
            .expect("plain PKCS#8 DER should parse");
        private_key
            .to_pkcs8_encrypted_pem(password, LineEnding::LF)
            .expect("PKCS#8 encryption should succeed")
            .to_string()
    }

    fn write_test_file(name: &str, bytes: &[u8]) -> String {
        let mut path = std::env::temp_dir();
        let suffix = SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock should be after epoch")
            .as_nanos();
        path.push(format!("kacrab-{suffix}-{name}"));
        std::fs::write(&path, bytes).expect("test file should be writable");
        path.to_string_lossy().into_owned()
    }
}
