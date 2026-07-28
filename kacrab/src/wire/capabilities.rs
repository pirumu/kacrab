//! Broker API version capabilities discovered from `ApiVersions`.

use std::collections::HashMap;

use kacrab_protocol::{
    generated::{ApiKey, ApiVersionsResponseData},
    version::{ApiVersionRange, client_api_info, resolve_api_version},
};

use super::error::{Result, WireError};

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

    /// Range the broker advertised for an API, or `None` when its
    /// `ApiVersions` response did not list the API key at all.
    #[must_use]
    pub fn broker_range(&self, api_key: ApiKey) -> Option<ApiVersionRange> {
        self.versions.get(&(api_key as i16)).copied()
    }

    /// Resolve the version to send for an API, capped by the caller's maximum,
    /// or a [`WireError::UnsupportedApiVersion`] naming both sides' ranges.
    ///
    /// This is the single place a version negotiation failure becomes an error,
    /// so every caller gets the same actionable message instead of a bare
    /// "unsupported" with no versions in it.
    pub(crate) fn resolve_version_for_limit(
        &self,
        api_key: ApiKey,
        max_version: i16,
    ) -> Result<i16> {
        self.version_for_limit(api_key, max_version).ok_or_else(|| {
            let info = client_api_info(api_key);
            WireError::UnsupportedApiVersion {
                api_key,
                client: ApiVersionRange {
                    min_version: info.min_version,
                    max_version: info.max_version.min(max_version),
                },
                broker: self.broker_range(api_key),
            }
        })
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

    use kacrab_protocol::{
        generated::{ApiKey, ApiVersion, ApiVersionsResponseData, client_api_info},
        version::ApiVersionRange,
    };

    use super::{BrokerCapabilities, WireError};

    fn capabilities_for(api_key: ApiKey, min_version: i16, max_version: i16) -> BrokerCapabilities {
        BrokerCapabilities::from_response(&ApiVersionsResponseData {
            api_keys: vec![ApiVersion {
                api_key: api_key as i16,
                min_version,
                max_version,
                _unknown_tagged_fields: Vec::new(),
            }],
            ..ApiVersionsResponseData::default()
        })
    }

    #[test]
    fn capabilities_resolve_supported_version_and_missing_api() {
        let capabilities = capabilities_for(ApiKey::Metadata, 0, 12);

        assert_eq!(capabilities.version_for(ApiKey::Metadata), Some(12));
        assert_eq!(capabilities.version_for_limit(ApiKey::Metadata, 8), Some(8));
        assert_eq!(capabilities.version_for(ApiKey::Produce), None);
        assert_eq!(
            capabilities.broker_range(ApiKey::Metadata),
            Some(ApiVersionRange {
                min_version: 0,
                max_version: 12,
            })
        );
        assert_eq!(capabilities.broker_range(ApiKey::Produce), None);
    }

    /// A negotiation failure must carry both sides' ranges, and must
    /// distinguish "the broker never advertised this API" from "the ranges do
    /// not overlap" — the two need different fixes.
    #[test]
    fn resolve_version_reports_absent_api_and_disjoint_ranges() {
        let capabilities = capabilities_for(ApiKey::Metadata, 0, 12);
        let metadata_info = client_api_info(ApiKey::Metadata);

        let error = capabilities
            .resolve_version_for_limit(ApiKey::Produce, i16::MAX)
            .expect_err("broker advertised no Produce versions");
        assert!(
            matches!(
                error,
                WireError::UnsupportedApiVersion {
                    api_key: ApiKey::Produce,
                    broker: None,
                    ..
                }
            ),
            "absent API must report no broker range: {error:?}"
        );

        // Metadata capped below the broker's floor: the ranges are disjoint,
        // but the broker did advertise the API.
        let error = capabilities
            .resolve_version_for_limit(
                ApiKey::Metadata,
                metadata_info.min_version.saturating_sub(1),
            )
            .expect_err("caller cap is below the client floor");
        assert!(
            matches!(
                error,
                WireError::UnsupportedApiVersion {
                    api_key: ApiKey::Metadata,
                    broker: Some(ApiVersionRange {
                        min_version: 0,
                        max_version: 12,
                    }),
                    ..
                }
            ),
            "disjoint ranges must report the broker range: {error:?}"
        );

        assert_eq!(
            capabilities
                .resolve_version_for_limit(ApiKey::Metadata, 8)
                .expect("metadata v8 is mutually supported"),
            8
        );
    }
}
