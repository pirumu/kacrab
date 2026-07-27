//! Process-wide crypto provider selection for `rustls` and `jsonwebtoken`.
//!
//! Both crates pick a backend from their own crate features and both panic on
//! first use when the features do not name exactly one. kacrab's `aws-lc-rs-tls`
//! and `pure-rust-tls` features are additive like every Cargo feature, so a build
//! that enables both — `--all-features` does — lands in exactly that state.
//!
//! Rather than leave that to a panic deep inside a handshake, kacrab resolves it
//! at the first use of either crate: `aws-lc-rs` wins when both are on. Selection
//! is first-wins, so an application that installs its own provider before touching
//! kacrab keeps it.

use std::sync::Arc;

use rustls::crypto::CryptoProvider;

use super::{Result, WireError};

const NO_PROVIDER_MESSAGE: &str =
    "no TLS crypto provider is compiled in - enable the kacrab `aws-lc-rs-tls` feature (the \
     default provider) or `pure-rust-tls` (drops `aws-lc-sys`), or install a \
     `rustls::crypto::CryptoProvider` as the process default before connecting";

/// Whether kacrab compiled a crypto backend in at all.
pub(crate) const HAS_COMPILED_IN_PROVIDER: bool =
    cfg!(any(feature = "aws-lc-rs-tls", feature = "pure-rust-tls"));

/// Install kacrab's compiled-in providers as the process defaults, if nothing else
/// claimed that slot first. Cheap and idempotent, so call sites need no `Once`.
#[cfg_attr(
    not(any(feature = "aws-lc-rs-tls", feature = "pure-rust-tls")),
    expect(
        clippy::missing_const_for_fn,
        reason = "the body is empty only in the no-provider configuration; the callers are the \
                  same in every configuration"
    )
)]
pub(crate) fn install_default_providers() {
    #[cfg(feature = "aws-lc-rs-tls")]
    {
        let _first_wins = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let _first_wins = jsonwebtoken::crypto::aws_lc::DEFAULT_PROVIDER.install_default();
    }
    #[cfg(all(feature = "pure-rust-tls", not(feature = "aws-lc-rs-tls")))]
    {
        let _first_wins = rustls::crypto::ring::default_provider().install_default();
        let _first_wins = jsonwebtoken::crypto::rust_crypto::DEFAULT_PROVIDER.install_default();
    }
}

/// The `rustls` provider to build client configs from.
///
/// # Errors
///
/// Returns [`WireError::InvalidTlsConfig`] when neither provider feature is enabled
/// and the process has not installed a provider of its own — a `PLAINTEXT`-only
/// build should not have to compile a crypto backend it never uses, so this is a
/// configuration error rather than a compile error.
pub(crate) fn rustls_provider() -> Result<Arc<CryptoProvider>> {
    install_default_providers();
    CryptoProvider::get_default().map_or_else(
        || Err(WireError::InvalidTlsConfig(NO_PROVIDER_MESSAGE.to_owned())),
        |installed| Ok(Arc::clone(installed)),
    )
}

/// Guard the `jsonwebtoken` signing path, which panics instead of erroring when it
/// cannot resolve a provider.
///
/// # Errors
///
/// Returns [`WireError::InvalidTlsConfig`] when no provider is available.
pub(crate) fn ensure_jwt_provider() -> Result<()> {
    install_default_providers();
    if HAS_COMPILED_IN_PROVIDER {
        Ok(())
    } else {
        Err(WireError::InvalidTlsConfig(format!(
            "OAUTHBEARER assertion signing needs a crypto provider: {NO_PROVIDER_MESSAGE}"
        )))
    }
}
