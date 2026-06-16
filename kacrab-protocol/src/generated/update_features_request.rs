//! Generated from UpdateFeaturesRequest.json - DO NOT EDIT
#![allow(
    missing_docs,
    clippy::all,
    clippy::pedantic,
    clippy::nursery,
    reason = "Generated protocol modules mirror Kafka's schema shape and intentionally trade \
              hand-written lint style for reproducible wire-code output."
)]
use bytes::{Bytes, BytesMut};

use crate::*;

#[derive(Debug, Clone, PartialEq)]
pub struct UpdateFeaturesRequestData {
    /// How long to wait in milliseconds before timing out the request.
    pub timeout_ms: i32,
    /// The list of updates to finalized features.
    pub feature_updates: Vec<FeatureUpdateKey>,
    /// True if we should validate the request, but not perform the upgrade or downgrade.
    pub validate_only: bool,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for UpdateFeaturesRequestData {
    fn default() -> Self {
        Self {
            timeout_ms: 60000i32,
            feature_updates: Vec::new(),
            validate_only: false,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl UpdateFeaturesRequestData {
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 0 || version > 2 {
            return Err(UnsupportedVersion::new(57, version).into());
        }
        let timeout_ms;
        let feature_updates;
        let mut validate_only = false;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        timeout_ms = read_i32(buf)?;
        feature_updates = {
            let len = read_compact_array_length(buf)?;
            let mut arr = Vec::with_capacity(len.max(0) as usize);
            for _ in 0..len {
                arr.push(FeatureUpdateKey::read(buf, version)?);
            }
            arr
        };
        if version >= 1 {
            validate_only = read_bool(buf)?;
        }
        let tagged_fields = read_tagged_fields(buf)?;
        for field in &tagged_fields {
            match field.tag {
                _ => {
                    _unknown_tagged_fields.push(field.clone());
                },
            }
        }
        Ok(Self {
            timeout_ms,
            feature_updates,
            validate_only,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 0 || version > 2 {
            return Err(UnsupportedVersion::new(57, version).into());
        }
        write_i32(buf, self.timeout_ms);
        write_compact_array_length(buf, self.feature_updates.len() as i32);
        for el in &self.feature_updates {
            el.write(buf, version)?;
        }
        if version >= 1 {
            write_bool(buf, self.validate_only);
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct FeatureUpdateKey {
    /// The name of the finalized feature to be updated.
    pub feature: KafkaString,
    /// The new maximum version level for the finalized feature. A value >= 1 is valid. A value <
    /// 1, is special, and can be used to request the deletion of the finalized feature.
    pub max_version_level: i16,
    /// DEPRECATED in version 1 (see DowngradeType). When set to true, the finalized feature
    /// version level is allowed to be downgraded/deleted. The downgrade request will fail if the
    /// new maximum version level is a value that's not lower than the existing maximum finalized
    /// version level.
    pub allow_downgrade: bool,
    /// Determine which type of upgrade will be performed: 1 will perform an upgrade only
    /// (default), 2 is safe downgrades only (lossless), 3 is unsafe downgrades (lossy).
    pub upgrade_type: i8,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for FeatureUpdateKey {
    fn default() -> Self {
        Self {
            feature: KafkaString::default(),
            max_version_level: 0_i16,
            allow_downgrade: false,
            upgrade_type: 1i8,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl FeatureUpdateKey {
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let feature;
        let max_version_level;
        let mut allow_downgrade = false;
        let mut upgrade_type = 1i8;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        feature = read_compact_string(buf)?;
        max_version_level = read_i16(buf)?;
        if version == 0 {
            allow_downgrade = read_bool(buf)?;
        }
        if version >= 1 {
            upgrade_type = read_i8(buf)?;
        }
        let tagged_fields = read_tagged_fields(buf)?;
        for field in &tagged_fields {
            match field.tag {
                _ => {
                    _unknown_tagged_fields.push(field.clone());
                },
            }
        }
        Ok(Self {
            feature,
            max_version_level,
            allow_downgrade,
            upgrade_type,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        write_compact_string(buf, &self.feature)?;
        write_i16(buf, self.max_version_level);
        if version == 0 {
            write_bool(buf, self.allow_downgrade);
        }
        if version >= 1 {
            write_i8(buf, self.upgrade_type);
        }
        let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
        all_tags.sort_by_key(|f| f.tag);
        write_tagged_fields(buf, &all_tags)?;
        Ok(())
    }
}
