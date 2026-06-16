//! Broker API version capabilities discovered from `ApiVersions`.

use std::collections::HashMap;

use kacrab_protocol::{
    generated::{ApiKey, ApiVersionsResponseData},
    version::{ApiVersionRange, client_api_info, resolve_api_version},
};

/// Broker API version capabilities discovered by `ApiVersions`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct BrokerCapabilities {
    versions: HashMap<i16, ApiVersionRange>,
}

impl BrokerCapabilities {
    /// Build broker capabilities from an `ApiVersionsResponse`.
    #[must_use]
    pub fn from_response(response: &ApiVersionsResponseData) -> Self {
        let versions = response
            .api_keys
            .iter()
            .map(|api| {
                (
                    api.api_key,
                    ApiVersionRange {
                        min_version: api.min_version,
                        max_version: api.max_version,
                    },
                )
            })
            .collect();
        Self { versions }
    }

    /// Resolve the highest mutually supported version for an API.
    #[must_use]
    pub fn version_for(&self, api_key: ApiKey) -> Option<i16> {
        let broker = *self.versions.get(&(api_key as i16))?;
        resolve_api_version(api_key as i16, broker)
    }

    /// Resolve the highest mutually supported version for an API, capped by
    /// the caller's maximum acceptable version.
    #[must_use]
    pub fn version_for_limit(&self, api_key: ApiKey, max_version: i16) -> Option<i16> {
        let mut broker = *self.versions.get(&(api_key as i16))?;
        let client = client_api_info(api_key);
        broker.max_version = broker.max_version.min(max_version).min(client.max_version);
        resolve_api_version(api_key as i16, broker)
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

    use kacrab_protocol::generated::{ApiKey, ApiVersion, ApiVersionsResponseData};

    use super::BrokerCapabilities;

    #[test]
    fn capabilities_resolve_supported_version_and_missing_api() {
        let capabilities = BrokerCapabilities::from_response(&ApiVersionsResponseData {
            api_keys: vec![ApiVersion {
                api_key: ApiKey::Metadata as i16,
                min_version: 0,
                max_version: 12,
                _unknown_tagged_fields: Vec::new(),
            }],
            ..ApiVersionsResponseData::default()
        });

        assert_eq!(capabilities.version_for(ApiKey::Metadata), Some(12));
        assert_eq!(capabilities.version_for_limit(ApiKey::Metadata, 8), Some(8));
        assert_eq!(capabilities.version_for(ApiKey::Produce), None);
    }
}
