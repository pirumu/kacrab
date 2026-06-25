//! Ergonomic Java-style client configuration facade.

use super::{
    AdminConfig, ConfigError, ConfigKey, ConfigValue, ConsumerConfig, ProducerConfig, Properties,
    UnknownKeyPolicy, WarningReport,
};

/// Java/librdkafka-style client configuration map.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ClientConfig {
    properties: Properties,
}

impl ClientConfig {
    /// Creates an empty client config.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            properties: Properties::new(),
        }
    }

    /// Sets a Kafka property key/value pair and returns the updated config.
    #[must_use]
    pub fn set(mut self, key: impl Into<ConfigKey>, value: impl Into<ConfigValue>) -> Self {
        let _previous = self.properties.insert(key, value);
        self
    }

    /// Inserts or replaces a Kafka property on this config.
    pub fn insert(
        &mut self,
        key: impl Into<ConfigKey>,
        value: impl Into<ConfigValue>,
    ) -> Option<ConfigValue> {
        self.properties.insert(key, value)
    }

    /// Returns a raw property value by Kafka key.
    #[must_use]
    pub fn get(&self, key: &str) -> Option<&ConfigValue> {
        self.properties.get(key)
    }

    /// Returns the raw Java-style properties.
    #[must_use]
    pub const fn properties(&self) -> &Properties {
        &self.properties
    }

    /// Consumes this facade and returns the raw Java-style properties.
    #[must_use]
    pub fn into_properties(self) -> Properties {
        self.properties
    }

    /// Builds a typed producer config using strict Kafka key validation.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] for unknown, unsupported, missing, or invalid
    /// producer config keys.
    pub fn producer_config(&self) -> Result<ProducerConfig, ConfigError> {
        let (config, _report) =
            ProducerConfig::from_properties(&self.properties, UnknownKeyPolicy::Deny)?;
        Ok(config)
    }

    /// Builds a typed producer config and returns validation warnings.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] for unsupported, missing, or invalid producer
    /// config keys.
    pub fn producer_config_with_warnings(
        &self,
        unknown_key_policy: UnknownKeyPolicy,
    ) -> Result<(ProducerConfig, WarningReport), ConfigError> {
        ProducerConfig::from_properties(&self.properties, unknown_key_policy)
    }

    /// Builds a typed consumer config using strict Kafka key validation.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] for unknown, unsupported, missing, or invalid
    /// consumer config keys.
    pub fn consumer_config(&self) -> Result<ConsumerConfig, ConfigError> {
        let (config, _report) =
            ConsumerConfig::from_properties(&self.properties, UnknownKeyPolicy::Deny)?;
        Ok(config)
    }

    /// Builds a typed admin config using strict Kafka key validation.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] for unknown, unsupported, missing, or invalid
    /// admin config keys.
    pub fn admin_config(&self) -> Result<AdminConfig, ConfigError> {
        let (config, _report) =
            AdminConfig::from_properties(&self.properties, UnknownKeyPolicy::Deny)?;
        Ok(config)
    }

    /// Builds a producer directly from this Java-style config.
    ///
    /// # Errors
    ///
    /// Returns a producer error when config validation, bootstrap resolution, or
    /// producer setup fails.
    #[cfg(feature = "producer")]
    pub async fn create_producer(&self) -> crate::producer::Result<crate::producer::Producer> {
        crate::producer::Producer::from_client_config(self).await
    }
}

impl From<Properties> for ClientConfig {
    fn from(properties: Properties) -> Self {
        Self { properties }
    }
}

impl<K, V> FromIterator<(K, V)> for ClientConfig
where
    K: Into<ConfigKey>,
    V: Into<ConfigValue>,
{
    fn from_iter<T: IntoIterator<Item = (K, V)>>(iter: T) -> Self {
        Self {
            properties: Properties::from_iter(iter),
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::expect_used,
        clippy::missing_assert_message,
        clippy::unwrap_used,
        reason = "Unit test fixtures fail fastest with contextual unwrap/expect calls."
    )]

    use super::ClientConfig;
    use crate::config::{ConfigError, UnknownKeyPolicy};

    #[test]
    fn facade_exposes_property_get_insert_and_into_properties() {
        let mut config = ClientConfig::new().set("bootstrap.servers", "localhost:9092");

        let previous = config.insert("client.id", "client-a");

        assert!(previous.is_none());
        assert_eq!(
            config
                .get("bootstrap.servers")
                .map(crate::config::ConfigValue::as_str),
            Some("localhost:9092")
        );
        assert_eq!(config.properties().len(), 2);
        assert_eq!(config.into_properties().len(), 2);
    }

    #[test]
    fn facade_builds_consumer_and_admin_configs() {
        let consumer = ClientConfig::new()
            .set("bootstrap.servers", "localhost:9092")
            .set("group.id", "group-a")
            .consumer_config()
            .expect("consumer config");
        let admin = ClientConfig::new()
            .set("bootstrap.servers", "localhost:9092")
            .admin_config()
            .expect("admin config");

        assert_eq!(consumer.group_id, "group-a");
        assert_eq!(admin.bootstrap_servers.as_slice(), ["localhost:9092"]);
    }

    #[test]
    fn producer_config_with_warnings_returns_report_for_valid_config() {
        let (config, report) = ClientConfig::new()
            .set("bootstrap.servers", "localhost:9092")
            .producer_config_with_warnings(UnknownKeyPolicy::Report)
            .expect("producer config");

        assert_eq!(config.bootstrap_servers.as_slice(), ["localhost:9092"]);
        assert!(report.warnings().is_empty());
    }

    #[test]
    fn strict_facade_reports_unknown_admin_key() {
        let error = ClientConfig::new()
            .set("bootstrap.servers", "localhost:9092")
            .set("unknown.kafka.key", "value")
            .admin_config()
            .expect_err("unknown key should fail");

        assert!(matches!(error, ConfigError::UnknownKey { .. }));
    }

    #[cfg(feature = "producer")]
    #[tokio::test]
    async fn create_producer_maps_config_errors_through_producer_facade() {
        assert!(matches!(
            ClientConfig::new().create_producer().await,
            Err(crate::producer::ProducerError::Config { .. })
        ));
    }
}
