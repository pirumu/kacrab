//! API version resolution + header version selection.
//!
//! Kafka clients negotiate the highest mutually-supported API version per
//! request type with the broker; the resolved version then determines whether
//! the request/response uses the flexible (varint-prefixed, tagged-fields)
//! header layout or the older fixed-width one.

pub mod error;

pub use self::error::UnsupportedVersion;
pub use crate::generated::{ApiInfo, ApiKey, client_api_info};

/// Result alias for version operations.
pub type Result<T> = core::result::Result<T, UnsupportedVersion>;

/// Kafka `ApiVersions` API key (`18`). Special-cased by KIP-511 for response
/// header version selection.
pub const API_VERSIONS_KEY: i16 = 18;

/// A contiguous range of supported API versions, inclusive on both ends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ApiVersionRange {
    /// Lowest supported version.
    pub min_version: i16,
    /// Highest supported version.
    pub max_version: i16,
}

/// Resolve the highest API version supported by both client and broker.
///
/// Returns `None` if the supported ranges are disjoint.
#[must_use]
pub fn resolve_api_version(api_key: i16, broker_range: ApiVersionRange) -> Option<i16> {
    if broker_range.min_version > broker_range.max_version {
        return None;
    }

    let api_key = ApiKey::from_i16(api_key)?;
    let info = client_api_info(api_key);
    let min_version = broker_range.min_version.max(info.min_version);
    let max_version = broker_range.max_version.min(info.max_version);
    if min_version > max_version {
        return None;
    }

    Some(max_version)
}

/// Request header version for `(api_key, api_version)`.
///
/// Flexible versions use header v2; most non-flexible versions use header v1.
/// `ControlledShutdown` v0 is the legacy request-header v0 exception.
#[must_use]
pub fn request_header_version(api_key: i16, api_version: i16) -> i16 {
    if api_key == ApiKey::ControlledShutdown as i16 && api_version == 0 {
        return 0;
    }
    if is_flexible_version(api_key, api_version) {
        2
    } else {
        1
    }
}

/// Response header version for `(api_key, api_version)`.
///
/// Flexible versions use header v1; non-flexible use header v0. Special case
/// (KIP-511): `ApiVersions` always uses header v0 because the client doesn't
/// yet know the broker's supported versions when it parses this response.
#[must_use]
pub fn response_header_version(api_key: i16, api_version: i16) -> i16 {
    if api_key == API_VERSIONS_KEY {
        return 0;
    }
    i16::from(is_flexible_version(api_key, api_version))
}

fn is_flexible_version(api_key: i16, api_version: i16) -> bool {
    let Some(api_key) = ApiKey::from_i16(api_key) else {
        return false;
    };
    let info = client_api_info(api_key);
    api_version >= info.flexible_versions_start
        && api_version >= info.min_version
        && api_version <= info.max_version
}

#[cfg(test)]
mod tests {
    use super::{
        API_VERSIONS_KEY, ApiVersionRange, request_header_version, resolve_api_version,
        response_header_version,
    };
    use crate::generated::ApiKey;

    #[test]
    fn resolve_api_version_intersects_client_and_broker_ranges() {
        let metadata = ApiKey::Metadata as i16;

        assert_eq!(
            resolve_api_version(
                metadata,
                ApiVersionRange {
                    min_version: 0,
                    max_version: 99,
                },
            ),
            Some(13),
        );
        assert_eq!(
            resolve_api_version(
                metadata,
                ApiVersionRange {
                    min_version: 0,
                    max_version: 2,
                },
            ),
            Some(2),
        );
        assert_eq!(
            resolve_api_version(
                metadata,
                ApiVersionRange {
                    min_version: 99,
                    max_version: 100,
                },
            ),
            None,
        );
        assert_eq!(
            resolve_api_version(
                -32,
                ApiVersionRange {
                    min_version: 0,
                    max_version: 1,
                },
            ),
            None,
        );
    }

    #[test]
    fn header_versions_follow_flexible_metadata_and_special_cases() {
        let metadata = ApiKey::Metadata as i16;
        let controlled_shutdown = ApiKey::ControlledShutdown as i16;

        assert_eq!(request_header_version(controlled_shutdown, 0), 0);
        assert_eq!(request_header_version(metadata, 8), 1);
        assert_eq!(request_header_version(metadata, 9), 2);

        assert_eq!(response_header_version(API_VERSIONS_KEY, 3), 0);
        assert_eq!(response_header_version(metadata, 8), 0);
        assert_eq!(response_header_version(metadata, 9), 1);
    }
}
