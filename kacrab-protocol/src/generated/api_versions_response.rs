//! Generated from ApiVersionsResponse.json - DO NOT EDIT
#![allow(
    missing_docs,
    clippy::all,
    clippy::pedantic,
    clippy::nursery,
    clippy::arithmetic_side_effects,
    reason = "Generated protocol modules mirror Kafka's schema shape and intentionally trade \
              hand-written lint style for reproducible wire-code output."
)]
use bytes::{Bytes, BytesMut};

use crate::*;

#[derive(Debug, Clone, PartialEq)]
pub struct ApiVersionsResponseData {
    /// The top-level error code.
    pub error_code: i16,
    /// The APIs supported by the broker.
    pub api_keys: Vec<ApiVersion>,
    /// The duration in milliseconds for which the request was throttled due to a quota violation,
    /// or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// Features supported by the broker. Note: in v0-v3, features with MinSupportedVersion = 0 are
    /// omitted.
    pub supported_features: Vec<SupportedFeatureKey>,
    /// The monotonically increasing epoch for the finalized features information. Valid values are
    /// >= 0. A value of -1 is special and represents unknown epoch.
    pub finalized_features_epoch: i64,
    /// List of cluster-wide finalized features. The information is valid only if
    /// FinalizedFeaturesEpoch >= 0.
    pub finalized_features: Vec<FinalizedFeatureKey>,
    /// Set by a KRaft controller if the required configurations for ZK migration are present.
    pub zk_migration_ready: bool,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for ApiVersionsResponseData {
    fn default() -> Self {
        Self {
            error_code: 0_i16,
            api_keys: Vec::new(),
            throttle_time_ms: 0_i32,
            supported_features: Vec::new(),
            finalized_features_epoch: -1i64,
            finalized_features: Vec::new(),
            zk_migration_ready: false,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl ApiVersionsResponseData {
    pub fn with_error_code(mut self, value: i16) -> Self {
        self.error_code = value;
        self
    }
    pub fn with_api_keys(mut self, value: Vec<ApiVersion>) -> Self {
        self.api_keys = value;
        self
    }
    pub fn with_throttle_time_ms(mut self, value: i32) -> Self {
        self.throttle_time_ms = value;
        self
    }
    pub fn with_supported_features(mut self, value: Vec<SupportedFeatureKey>) -> Self {
        self.supported_features = value;
        self
    }
    pub fn with_finalized_features_epoch(mut self, value: i64) -> Self {
        self.finalized_features_epoch = value;
        self
    }
    pub fn with_finalized_features(mut self, value: Vec<FinalizedFeatureKey>) -> Self {
        self.finalized_features = value;
        self
    }
    pub fn with_zk_migration_ready(mut self, value: bool) -> Self {
        self.zk_migration_ready = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 0 || version > 4 {
            return Err(UnsupportedVersion::new(18, version).into());
        }
        let error_code;
        let api_keys;
        let mut throttle_time_ms = 0_i32;
        let mut supported_features = Vec::new();
        let mut finalized_features_epoch = -1i64;
        let mut finalized_features = Vec::new();
        let mut zk_migration_ready = false;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        error_code = read_i16(buf)?;
        if version >= 3 {
            api_keys = {
                let len = read_compact_array_length(buf)?;
                let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
                for _ in 0..len {
                    arr.push(ApiVersion::read(buf, version)?);
                }
                arr
            };
        } else {
            api_keys = {
                let len = read_array_length(buf)?;
                let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
                for _ in 0..len {
                    arr.push(ApiVersion::read(buf, version)?);
                }
                arr
            };
        }
        if version >= 1 {
            throttle_time_ms = read_i32(buf)?;
        }
        if version >= 3 {
            let tagged_fields = read_tagged_fields(buf)?;
            for field in &tagged_fields {
                match field.tag {
                    0 => {
                        let mut tag_buf = field.data.clone();
                        supported_features = {
                            let len = read_compact_array_length(&mut tag_buf)?;
                            let mut arr =
                                Vec::with_capacity(array_read_capacity(len, (&mut tag_buf).len()));
                            for _ in 0..len {
                                arr.push(SupportedFeatureKey::read(&mut tag_buf, version)?);
                            }
                            arr
                        };
                    },
                    1 => {
                        let mut tag_buf = field.data.clone();
                        finalized_features_epoch = read_i64(&mut tag_buf)?;
                    },
                    2 => {
                        let mut tag_buf = field.data.clone();
                        finalized_features = {
                            let len = read_compact_array_length(&mut tag_buf)?;
                            let mut arr =
                                Vec::with_capacity(array_read_capacity(len, (&mut tag_buf).len()));
                            for _ in 0..len {
                                arr.push(FinalizedFeatureKey::read(&mut tag_buf, version)?);
                            }
                            arr
                        };
                    },
                    3 => {
                        let mut tag_buf = field.data.clone();
                        zk_migration_ready = read_bool(&mut tag_buf)?;
                    },
                    _ => {
                        _unknown_tagged_fields.push(field.clone());
                    },
                }
            }
        }
        Ok(Self {
            error_code,
            api_keys,
            throttle_time_ms,
            supported_features,
            finalized_features_epoch,
            finalized_features,
            zk_migration_ready,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 0 || version > 4 {
            return Err(UnsupportedVersion::new(18, version).into());
        }
        write_i16(buf, self.error_code);
        if version >= 3 {
            write_compact_array_length(buf, self.api_keys.len() as i32);
            for el in &self.api_keys {
                el.write(buf, version)?;
            }
        } else {
            write_array_length(buf, self.api_keys.len() as i32);
            for el in &self.api_keys {
                el.write(buf, version)?;
            }
        }
        if version >= 1 {
            write_i32(buf, self.throttle_time_ms);
        } else if self.throttle_time_ms != 0_i32 {
            return Err(UnsupportedFieldVersion::new(18, "throttle_time_ms", version).into());
        }
        if version >= 3 {
            let mut known_tagged_fields: Vec<RawTaggedField> = Vec::new();
            if !self.supported_features.is_empty() {
                let mut tag_buf = BytesMut::new();
                write_compact_array_length(&mut tag_buf, self.supported_features.len() as i32);
                for el in &self.supported_features {
                    el.write(&mut tag_buf, version)?;
                }
                known_tagged_fields.push(RawTaggedField {
                    tag: 0,
                    data: tag_buf.freeze(),
                });
            }
            if self.finalized_features_epoch != -1_i64 {
                let mut tag_buf = BytesMut::new();
                write_i64(&mut tag_buf, self.finalized_features_epoch);
                known_tagged_fields.push(RawTaggedField {
                    tag: 1,
                    data: tag_buf.freeze(),
                });
            }
            if !self.finalized_features.is_empty() {
                let mut tag_buf = BytesMut::new();
                write_compact_array_length(&mut tag_buf, self.finalized_features.len() as i32);
                for el in &self.finalized_features {
                    el.write(&mut tag_buf, version)?;
                }
                known_tagged_fields.push(RawTaggedField {
                    tag: 2,
                    data: tag_buf.freeze(),
                });
            }
            if self.zk_migration_ready {
                let mut tag_buf = BytesMut::new();
                write_bool(&mut tag_buf, self.zk_migration_ready);
                known_tagged_fields.push(RawTaggedField {
                    tag: 3,
                    data: tag_buf.freeze(),
                });
            }
            let mut all_tags = known_tagged_fields;
            all_tags.extend(self._unknown_tagged_fields.iter().cloned());
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        if version < 0 || version > 4 {
            return Err(UnsupportedVersion::new(18, version).into());
        }
        let mut len: usize = 0;
        len += 2;
        if version >= 3 {
            len += compact_array_length_len(self.api_keys.len() as i32);
            for el in &self.api_keys {
                len += el.encoded_len(version)?;
            }
        } else {
            len += array_length_len();
            for el in &self.api_keys {
                len += el.encoded_len(version)?;
            }
        }
        if version >= 1 {
            len += 4;
        } else if self.throttle_time_ms != 0_i32 {
            return Err(UnsupportedFieldVersion::new(18, "throttle_time_ms", version).into());
        }
        if version >= 3 {
            let mut known_tagged_fields: Vec<RawTaggedField> = Vec::new();
            if !self.supported_features.is_empty() {
                let mut tag_buf = BytesMut::new();
                write_compact_array_length(&mut tag_buf, self.supported_features.len() as i32);
                for el in &self.supported_features {
                    el.write(&mut tag_buf, version)?;
                }
                known_tagged_fields.push(RawTaggedField {
                    tag: 0,
                    data: tag_buf.freeze(),
                });
            }
            if self.finalized_features_epoch != -1_i64 {
                let mut tag_buf = BytesMut::new();
                write_i64(&mut tag_buf, self.finalized_features_epoch);
                known_tagged_fields.push(RawTaggedField {
                    tag: 1,
                    data: tag_buf.freeze(),
                });
            }
            if !self.finalized_features.is_empty() {
                let mut tag_buf = BytesMut::new();
                write_compact_array_length(&mut tag_buf, self.finalized_features.len() as i32);
                for el in &self.finalized_features {
                    el.write(&mut tag_buf, version)?;
                }
                known_tagged_fields.push(RawTaggedField {
                    tag: 2,
                    data: tag_buf.freeze(),
                });
            }
            if self.zk_migration_ready {
                let mut tag_buf = BytesMut::new();
                write_bool(&mut tag_buf, self.zk_migration_ready);
                known_tagged_fields.push(RawTaggedField {
                    tag: 3,
                    data: tag_buf.freeze(),
                });
            }
            let mut all_tags = known_tagged_fields;
            all_tags.extend(self._unknown_tagged_fields.iter().cloned());
            all_tags.sort_by_key(|f| f.tag);
            len += tagged_fields_len(&all_tags)?;
        }
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct ApiVersion {
    /// The API index.
    pub api_key: i16,
    /// The minimum supported version, inclusive.
    pub min_version: i16,
    /// The maximum supported version, inclusive.
    pub max_version: i16,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for ApiVersion {
    fn default() -> Self {
        Self {
            api_key: 0_i16,
            min_version: 0_i16,
            max_version: 0_i16,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl ApiVersion {
    pub fn with_api_key(mut self, value: i16) -> Self {
        self.api_key = value;
        self
    }
    pub fn with_min_version(mut self, value: i16) -> Self {
        self.min_version = value;
        self
    }
    pub fn with_max_version(mut self, value: i16) -> Self {
        self.max_version = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let api_key;
        let min_version;
        let max_version;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        api_key = read_i16(buf)?;
        min_version = read_i16(buf)?;
        max_version = read_i16(buf)?;
        if version >= 3 {
            let tagged_fields = read_tagged_fields(buf)?;
            for field in &tagged_fields {
                match field.tag {
                    _ => {
                        _unknown_tagged_fields.push(field.clone());
                    },
                }
            }
        }
        Ok(Self {
            api_key,
            min_version,
            max_version,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        write_i16(buf, self.api_key);
        write_i16(buf, self.min_version);
        write_i16(buf, self.max_version);
        if version >= 3 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        let mut len: usize = 0;
        len += 2;
        len += 2;
        len += 2;
        if version >= 3 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            len += tagged_fields_len(&all_tags)?;
        }
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct SupportedFeatureKey {
    /// The name of the feature.
    pub name: KafkaString,
    /// The minimum supported version for the feature.
    pub min_version: i16,
    /// The maximum supported version for the feature.
    pub max_version: i16,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for SupportedFeatureKey {
    fn default() -> Self {
        Self {
            name: KafkaString::default(),
            min_version: 0_i16,
            max_version: 0_i16,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl SupportedFeatureKey {
    pub fn with_name(mut self, value: KafkaString) -> Self {
        self.name = value;
        self
    }
    pub fn with_min_version(mut self, value: i16) -> Self {
        self.min_version = value;
        self
    }
    pub fn with_max_version(mut self, value: i16) -> Self {
        self.max_version = value;
        self
    }
    pub fn read(buf: &mut Bytes, _version: i16) -> Result<Self> {
        let name;
        let min_version;
        let max_version;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        name = read_compact_string(buf)?;
        min_version = read_i16(buf)?;
        max_version = read_i16(buf)?;
        let tagged_fields = read_tagged_fields(buf)?;
        for field in &tagged_fields {
            match field.tag {
                _ => {
                    _unknown_tagged_fields.push(field.clone());
                },
            }
        }
        Ok(Self {
            name,
            min_version,
            max_version,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, _version: i16) -> Result<()> {
        write_compact_string(buf, &self.name)?;
        write_i16(buf, self.min_version);
        write_i16(buf, self.max_version);
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
    pub fn encoded_len(&self, _version: i16) -> Result<usize> {
        let mut len: usize = 0;
        len += compact_string_len(&self.name)?;
        len += 2;
        len += 2;
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct FinalizedFeatureKey {
    /// The name of the feature.
    pub name: KafkaString,
    /// The cluster-wide finalized max version level for the feature.
    pub max_version_level: i16,
    /// The cluster-wide finalized min version level for the feature.
    pub min_version_level: i16,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for FinalizedFeatureKey {
    fn default() -> Self {
        Self {
            name: KafkaString::default(),
            max_version_level: 0_i16,
            min_version_level: 0_i16,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl FinalizedFeatureKey {
    pub fn with_name(mut self, value: KafkaString) -> Self {
        self.name = value;
        self
    }
    pub fn with_max_version_level(mut self, value: i16) -> Self {
        self.max_version_level = value;
        self
    }
    pub fn with_min_version_level(mut self, value: i16) -> Self {
        self.min_version_level = value;
        self
    }
    pub fn read(buf: &mut Bytes, _version: i16) -> Result<Self> {
        let name;
        let max_version_level;
        let min_version_level;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        name = read_compact_string(buf)?;
        max_version_level = read_i16(buf)?;
        min_version_level = read_i16(buf)?;
        let tagged_fields = read_tagged_fields(buf)?;
        for field in &tagged_fields {
            match field.tag {
                _ => {
                    _unknown_tagged_fields.push(field.clone());
                },
            }
        }
        Ok(Self {
            name,
            max_version_level,
            min_version_level,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, _version: i16) -> Result<()> {
        write_compact_string(buf, &self.name)?;
        write_i16(buf, self.max_version_level);
        write_i16(buf, self.min_version_level);
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
    pub fn encoded_len(&self, _version: i16) -> Result<usize> {
        let mut len: usize = 0;
        len += compact_string_len(&self.name)?;
        len += 2;
        len += 2;
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        len += tagged_fields_len(&all_tags)?;
        Ok(len)
    }
}
