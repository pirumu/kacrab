//! Optional Kerberos/GSSAPI SASL authenticator.

use std::{fmt, string::String, sync::Mutex};

use bytes::Bytes;
use libgssapi::{
    context::{ClientCtx, CtxFlags, SecurityContext},
    credential::{Cred, CredUsage},
    name::Name,
    oid::{GSS_MECH_KRB5, GSS_NT_HOSTBASED_SERVICE, OidSet},
    util::Buf,
};

use super::{
    SaslClientAction, SaslClientAuthenticator, SaslMechanism, WireError,
    kerberos::KerberosLoginManager,
};

/// SASL/GSSAPI security-layer bitmask for "no security layer" (auth only).
/// Kafka runs its own transport, so the client always selects this; matches the
/// JDK `GssKrb5Client`, which Kafka uses.
const SECURITY_LAYER_NONE: u8 = 0x01;

/// Where we are in the SASL/GSSAPI exchange. After the GSSAPI context is
/// established there is still the RFC 4752 security-layer round (unwrap the
/// server's offer, send back the chosen layer + max buffer), which must finish
/// before the broker will accept application requests.
#[derive(Clone, Copy, PartialEq, Eq)]
enum GssapiPhase {
    Establishing,
    SecurityLayer,
    Done,
}

struct GssapiSession {
    context: ClientCtx,
    phase: GssapiPhase,
}

/// Native Kerberos/GSSAPI authenticator backed by the platform GSSAPI library.
pub struct GssapiAuthenticator {
    service_name: String,
    host: String,
    target_name: String,
    session: Mutex<Option<GssapiSession>>,
    kerberos_login: Option<KerberosLoginManager>,
}

impl GssapiAuthenticator {
    /// Creates a Kerberos/GSSAPI authenticator for `service@host`.
    #[must_use]
    pub fn new(service_name: String, host: String) -> Self {
        let mut target_name = String::with_capacity(
            service_name
                .len()
                .saturating_add(host.len())
                .saturating_add(1),
        );
        target_name.push_str(&service_name);
        target_name.push('@');
        target_name.push_str(&host);
        Self {
            service_name,
            host,
            target_name,
            session: Mutex::new(None),
            kerberos_login: None,
        }
    }

    /// Attach a Java-compatible Kerberos login manager for kinit and TGT renewal.
    #[must_use]
    pub(crate) fn with_kerberos_login(mut self, kerberos_login: KerberosLoginManager) -> Self {
        self.kerberos_login = Some(kerberos_login);
        self
    }

    /// Returns the hostbased GSSAPI target name.
    #[must_use]
    pub fn target_name(&self) -> &str {
        &self.target_name
    }

    fn new_context(&self) -> Result<ClientCtx, WireError> {
        if let Some(kerberos_login) = &self.kerberos_login {
            kerberos_login.login_blocking()?;
        }
        let desired_mechs = OidSet::singleton(GSS_MECH_KRB5).map_err(gssapi_error)?;
        let name = Name::new(self.target_name.as_bytes(), Some(GSS_NT_HOSTBASED_SERVICE))
            .map_err(gssapi_error)?;
        let target = name
            .canonicalize(Some(GSS_MECH_KRB5))
            .map_err(gssapi_error)?;
        let cred = Cred::acquire(None, None, CredUsage::Initiate, Some(&desired_mechs))
            .map_err(gssapi_error)?;
        if let Some(kerberos_login) = &self.kerberos_login {
            let lifetime = cred.lifetime().map_err(gssapi_error)?;
            kerberos_login.start_renewal(lifetime)?;
        }
        Ok(ClientCtx::new(
            Some(cred),
            target,
            CtxFlags::GSS_C_MUTUAL_FLAG,
            Some(GSS_MECH_KRB5),
        ))
    }
}

impl fmt::Debug for GssapiAuthenticator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GssapiAuthenticator")
            .field("service_name", &self.service_name)
            .field("host", &self.host)
            .field("target_name", &self.target_name)
            .finish_non_exhaustive()
    }
}

impl SaslClientAuthenticator for GssapiAuthenticator {
    fn mechanism(&self) -> SaslMechanism {
        SaslMechanism::Gssapi
    }

    fn start(&self) -> Result<SaslClientAction, WireError> {
        let mut context = self.new_context()?;
        let token = context.step(None, None).map_err(gssapi_error)?;
        // The initial GSS_Init_sec_context always yields the AP-REQ token.
        let action = token.as_ref().map(send_token).ok_or_else(|| {
            WireError::SaslAuthentication("GSSAPI produced no initial token".to_owned())
        })?;
        let mut stored = self.session.lock().map_err(gssapi_lock_error)?;
        *stored = Some(GssapiSession {
            context,
            phase: GssapiPhase::Establishing,
        });
        drop(stored);
        Ok(action)
    }

    fn next(&self, challenge: &[u8]) -> Result<SaslClientAction, WireError> {
        let mut stored = self.session.lock().map_err(gssapi_lock_error)?;
        let session = stored.as_mut().ok_or_else(|| {
            WireError::SaslAuthentication(
                "GSSAPI challenge received before context start".to_owned(),
            )
        })?;
        let action = match session.phase {
            GssapiPhase::Establishing => {
                let token = session
                    .context
                    .step(Some(challenge), None)
                    .map_err(gssapi_error)?;
                if session.context.is_complete() {
                    // Context established. The next server message is the
                    // RFC 4752 security-layer offer, so send any final context
                    // token (or an empty token to yield the turn) and move on.
                    session.phase = GssapiPhase::SecurityLayer;
                    send_token_or_empty(token.as_ref())
                } else {
                    // Still negotiating: a missing token here means a stalled
                    // context rather than a normal continuation.
                    token.as_ref().map(send_token).ok_or_else(|| {
                        WireError::SaslAuthentication(
                            "GSSAPI context did not complete and produced no token".to_owned(),
                        )
                    })?
                }
            },
            GssapiPhase::SecurityLayer => {
                // The challenge is the GSS-wrapped server offer: byte 0 is the
                // supported security-layer bitmask, bytes 1..4 the max server
                // buffer. We select "no security layer" (max buffer 0) and
                // return the GSS-wrapped reply, matching the JDK GssKrb5Client.
                let offer = session.context.unwrap(challenge).map_err(gssapi_error)?;
                if offer
                    .as_ref()
                    .first()
                    .is_some_and(|supported| supported & SECURITY_LAYER_NONE == 0)
                {
                    return Err(WireError::SaslAuthentication(
                        "broker does not offer the SASL/GSSAPI no-security-layer option".to_owned(),
                    ));
                }
                let reply = [SECURITY_LAYER_NONE, 0x00, 0x00, 0x00];
                let wrapped = session.context.wrap(false, &reply).map_err(gssapi_error)?;
                session.phase = GssapiPhase::Done;
                SaslClientAction::Send(Bytes::copy_from_slice(wrapped.as_ref()))
            },
            GssapiPhase::Done => SaslClientAction::Complete,
        };
        drop(stored);
        Ok(action)
    }
}

fn send_token(token: &Buf) -> SaslClientAction {
    SaslClientAction::Send(Bytes::copy_from_slice(token.as_ref()))
}

fn send_token_or_empty(token: Option<&Buf>) -> SaslClientAction {
    token.map_or_else(|| SaslClientAction::Send(Bytes::new()), send_token)
}

fn gssapi_error(error: libgssapi::error::Error) -> WireError {
    WireError::SaslAuthentication(format!("GSSAPI authentication failed: {error}"))
}

fn gssapi_lock_error<T>(_error: std::sync::PoisonError<T>) -> WireError {
    WireError::SaslAuthentication("GSSAPI context lock is poisoned".to_owned())
}
