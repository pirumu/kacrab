//! Kerberos login helpers for the optional GSSAPI backend.

use std::{
    process::Command,
    sync::{Arc, Mutex},
    time::Duration,
};

use super::{SaslConfig, WireError, auth::jaas_option};

/// Java-compatible Kerberos `kinit` command shape.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KerberosKinitCommand {
    pub(crate) program: String,
    pub(crate) args: Vec<String>,
}

impl KerberosKinitCommand {
    fn run(&self) -> Result<(), WireError> {
        let status = Command::new(&self.program)
            .args(&self.args)
            .status()
            .map_err(|error| {
                WireError::InvalidSaslConfig(format!(
                    "failed to run Kerberos kinit command: {error}"
                ))
            })?;
        if status.success() {
            return Ok(());
        }
        Err(WireError::InvalidSaslConfig(format!(
            "Kerberos kinit command exited with status {status}"
        )))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct KerberosLoginManager {
    options: KerberosLoginOptions,
    renewal: Arc<Mutex<Option<KerberosRenewalTask>>>,
}

impl KerberosLoginManager {
    pub(crate) fn new(config: &SaslConfig) -> Self {
        Self {
            options: KerberosLoginOptions::from_config(config),
            renewal: Arc::new(Mutex::new(None)),
        }
    }

    pub(crate) fn login_blocking(&self) -> Result<(), WireError> {
        if let Some(command) = self.options.initial_command() {
            command.run()?;
        }
        Ok(())
    }

    pub(crate) fn start_renewal(&self, lifetime: Duration) -> Result<(), WireError> {
        let Some(command) = self.options.renew_command() else {
            return Ok(());
        };
        let mut renewal = self.renewal.lock().map_err(|_error| {
            WireError::InvalidSaslConfig("Kerberos renewal lock is poisoned".to_owned())
        })?;
        if renewal.is_some() {
            return Ok(());
        }
        *renewal = Some(KerberosRenewalTask::spawn(command, &self.options, lifetime));
        drop(renewal);
        Ok(())
    }
}

#[derive(Debug)]
struct KerberosRenewalTask {
    handle: tokio::task::JoinHandle<()>,
}

impl KerberosRenewalTask {
    fn spawn(
        command: KerberosKinitCommand,
        options: &KerberosLoginOptions,
        lifetime: Duration,
    ) -> Self {
        let ticket_renew_window_factor = options.ticket_renew_window_factor;
        let ticket_renew_jitter = options.ticket_renew_jitter;
        let min_time_before_relogin = options.min_time_before_relogin;
        let handle = tokio::spawn(async move {
            let mut lifetime = lifetime;
            loop {
                let sample = random_unit_interval().unwrap_or(0.0);
                let delay = kerberos_refresh_delay(
                    lifetime,
                    ticket_renew_window_factor,
                    ticket_renew_jitter,
                    sample,
                    min_time_before_relogin,
                );
                if !delay.is_zero() {
                    tokio::time::sleep(delay).await;
                }
                let command = command.clone();
                let result = tokio::task::spawn_blocking(move || command.run()).await;
                if !matches!(result, Ok(Ok(()))) {
                    return;
                }
                lifetime = lifetime.max(min_time_before_relogin);
            }
        });
        Self { handle }
    }
}

impl Drop for KerberosRenewalTask {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct KerberosLoginOptions {
    kinit_cmd: String,
    use_keytab: bool,
    use_ticket_cache: bool,
    keytab: Option<String>,
    principal: Option<String>,
    ticket_renew_window_factor: f64,
    ticket_renew_jitter: f64,
    min_time_before_relogin: Duration,
}

impl KerberosLoginOptions {
    pub(crate) fn from_config(config: &SaslConfig) -> Self {
        let jaas_config = config.jaas_config.as_deref();
        Self {
            kinit_cmd: config
                .kerberos_kinit_cmd
                .clone()
                .unwrap_or_else(|| "/usr/bin/kinit".to_owned()),
            use_keytab: jaas_config
                .and_then(|config| jaas_option(config, "useKeyTab"))
                .is_some_and(|value| value.eq_ignore_ascii_case("true")),
            use_ticket_cache: jaas_config
                .and_then(|config| jaas_option(config, "useTicketCache"))
                .is_some_and(|value| value.eq_ignore_ascii_case("true")),
            keytab: jaas_config.and_then(|config| jaas_option(config, "keyTab")),
            principal: jaas_config.and_then(|config| jaas_option(config, "principal")),
            ticket_renew_window_factor: config.kerberos_ticket_renew_window_factor,
            ticket_renew_jitter: config.kerberos_ticket_renew_jitter,
            min_time_before_relogin: config.kerberos_min_time_before_relogin,
        }
    }

    pub(crate) fn initial_command(&self) -> Option<KerberosKinitCommand> {
        self.keytab_command()
    }

    pub(crate) fn renew_command(&self) -> Option<KerberosKinitCommand> {
        self.keytab_command().or_else(|| {
            self.use_ticket_cache.then(|| KerberosKinitCommand {
                program: self.kinit_cmd.clone(),
                args: vec!["-R".to_owned()],
            })
        })
    }

    fn keytab_command(&self) -> Option<KerberosKinitCommand> {
        if !self.use_keytab {
            return None;
        }
        let keytab = self.keytab.as_ref()?;
        let principal = self.principal.as_ref()?;
        Some(KerberosKinitCommand {
            program: self.kinit_cmd.clone(),
            args: vec!["-kt".to_owned(), keytab.clone(), principal.clone()],
        })
    }
}

pub(crate) fn kerberos_service_name(config: &SaslConfig) -> Result<String, WireError> {
    let jaas_service_name = config
        .jaas_config
        .as_deref()
        .and_then(|jaas| jaas_option(jaas, "serviceName"));
    match (config.kerberos_service_name.as_ref(), jaas_service_name) {
        (Some(configured), Some(jaas)) if configured != &jaas => Err(WireError::InvalidSaslConfig(
            "conflicting Kerberos serviceName values in JAAS and sasl.kerberos.service.name"
                .to_owned(),
        )),
        (Some(configured), _) => Ok(configured.clone()),
        (None, Some(jaas)) => Ok(jaas),
        (None, None) => Err(WireError::InvalidSaslConfig(
            "No Kerberos serviceName defined in either JAAS or sasl.kerberos.service.name"
                .to_owned(),
        )),
    }
}

pub(crate) fn kerberos_renew_jitter(max_jitter: f64, sample: f64) -> f64 {
    max_jitter.clamp(0.0, 1.0) * sample.clamp(0.0, 1.0)
}

pub(crate) fn kerberos_refresh_delay(
    lifetime: Duration,
    ticket_renew_window_factor: f64,
    ticket_renew_jitter: f64,
    jitter_sample: f64,
    min_time_before_relogin: Duration,
) -> Duration {
    let factor =
        ticket_renew_window_factor + kerberos_renew_jitter(ticket_renew_jitter, jitter_sample);
    let proposed = lifetime.mul_f64(factor);
    if proposed > lifetime || min_time_before_relogin > lifetime {
        return Duration::ZERO;
    }
    proposed.max(min_time_before_relogin)
}

fn random_unit_interval() -> Result<f64, WireError> {
    let mut bytes = [0_u8; 4];
    getrandom::fill(&mut bytes).map_err(|error| {
        WireError::InvalidSaslConfig(format!(
            "Kerberos renewal jitter generation failed: {error}"
        ))
    })?;
    Ok(f64::from(u32::from_be_bytes(bytes)) / f64::from(u32::MAX))
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::missing_assert_message,
        reason = "Unit tests keep assertions compact around Java compatibility formulas."
    )]

    use std::time::Duration;

    use super::{
        KerberosKinitCommand, KerberosLoginOptions, kerberos_refresh_delay, kerberos_renew_jitter,
        kerberos_service_name,
    };
    use crate::wire::{SaslConfig, WireError};

    #[test]
    fn kerberos_options_parse_java_jaas_keytab_login() {
        let config = SaslConfig {
            jaas_config: Some(
                "com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true \
                 keyTab=\"/tmp/client.keytab\" principal=\"alice/[email protected]\";"
                    .to_owned(),
            ),
            ..SaslConfig::default()
        };

        let options = KerberosLoginOptions::from_config(&config);

        assert_eq!(
            options.initial_command(),
            Some(KerberosKinitCommand {
                program: "/usr/bin/kinit".to_owned(),
                args: vec![
                    "-kt".to_owned(),
                    "/tmp/client.keytab".to_owned(),
                    "alice/[email protected]".to_owned(),
                ],
            })
        );
        assert_eq!(options.renew_command(), options.initial_command());
    }

    #[test]
    fn kerberos_options_parse_java_jaas_ticket_cache_renewal() {
        let config = SaslConfig {
            jaas_config: Some(
                "com.sun.security.auth.module.Krb5LoginModule required useTicketCache=true;"
                    .to_owned(),
            ),
            ..SaslConfig::default()
        };

        let options = KerberosLoginOptions::from_config(&config);

        assert_eq!(options.initial_command(), None);
        assert_eq!(
            options.renew_command(),
            Some(KerberosKinitCommand {
                program: "/usr/bin/kinit".to_owned(),
                args: vec!["-R".to_owned()],
            })
        );
    }

    #[test]
    fn kerberos_options_require_java_use_keytab_flag_for_keytab_kinit() {
        let config = SaslConfig {
            jaas_config: Some(
                "com.sun.security.auth.module.Krb5LoginModule required useKeyTab=false \
                 keyTab=\"/tmp/client.keytab\" principal=\"alice/[email protected]\";"
                    .to_owned(),
            ),
            ..SaslConfig::default()
        };

        let options = KerberosLoginOptions::from_config(&config);

        assert_eq!(options.initial_command(), None);
        assert_eq!(options.renew_command(), None);
    }

    #[test]
    fn kerberos_service_name_uses_jaas_when_kafka_config_is_missing() {
        let config = SaslConfig {
            jaas_config: Some(
                "com.sun.security.auth.module.Krb5LoginModule required serviceName=\"kafka\";"
                    .to_owned(),
            ),
            ..SaslConfig::default()
        };

        assert_eq!(
            kerberos_service_name(&config).expect("service name"),
            "kafka"
        );
    }

    #[test]
    fn kerberos_service_name_rejects_conflicting_jaas_and_kafka_config_values() {
        let config = SaslConfig {
            jaas_config: Some(
                "com.sun.security.auth.module.Krb5LoginModule required serviceName=\"not-kafka\";"
                    .to_owned(),
            ),
            kerberos_service_name: Some("kafka".to_owned()),
            ..SaslConfig::default()
        };

        assert!(matches!(
            kerberos_service_name(&config),
            Err(WireError::InvalidSaslConfig(message)) if message.contains("conflicting")
        ));
    }

    #[test]
    fn kerberos_refresh_delay_matches_kafka_43_formula() {
        let lifetime = Duration::from_secs(100);

        assert_eq!(
            kerberos_refresh_delay(lifetime, 0.80, 0.05, 0.50, Duration::from_mins(1)),
            Duration::from_millis(82_500)
        );
        assert_eq!(
            kerberos_refresh_delay(lifetime, 0.80, 0.05, 0.50, Duration::from_secs(90)),
            Duration::from_secs(90)
        );
        assert_eq!(
            kerberos_refresh_delay(lifetime, 0.99, 0.05, 1.00, Duration::from_mins(1)),
            Duration::ZERO
        );
        assert_eq!(
            kerberos_refresh_delay(lifetime, 0.80, 0.05, 0.50, Duration::from_secs(101)),
            Duration::ZERO
        );
    }

    #[test]
    fn kerberos_renew_jitter_uses_configured_upper_bound() {
        assert_float_eq(kerberos_renew_jitter(0.0, 0.50), 0.0);
        assert_float_eq(kerberos_renew_jitter(0.05, 0.50), 0.025);
        assert_float_eq(kerberos_renew_jitter(1.50, 0.50), 0.50);
    }

    fn assert_float_eq(actual: f64, expected: f64) {
        assert!((actual - expected).abs() < f64::EPSILON);
    }
}
