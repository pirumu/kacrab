//! Typed parsing for Java-style configuration values.

use std::{str::FromStr, string::String, time::Duration, vec::Vec};

use super::{ConfigValue, ParseConfigValueError};

/// Parses a raw Java-style config value into a typed Rust value.
pub trait ParseConfigValue: Sized {
    /// Parses a config value.
    ///
    /// # Errors
    ///
    /// Returns [`ParseConfigValueError`] when the raw value cannot be parsed
    /// as the target type.
    fn parse_config_value(value: &ConfigValue) -> Result<Self, ParseConfigValueError>;
}

impl ParseConfigValue for bool {
    fn parse_config_value(value: &ConfigValue) -> Result<Self, ParseConfigValueError> {
        let raw = value.as_str();
        if raw.eq_ignore_ascii_case("true") {
            Ok(true)
        } else if raw.eq_ignore_ascii_case("false") {
            Ok(false)
        } else {
            Err(ParseConfigValueError::new("bool", raw))
        }
    }
}

macro_rules! impl_integer_parser {
    ($ty:ty, $target:literal) => {
        impl ParseConfigValue for $ty {
            fn parse_config_value(value: &ConfigValue) -> Result<Self, ParseConfigValueError> {
                <$ty>::from_str(value.as_str())
                    .map_err(|_error| ParseConfigValueError::new($target, value.as_str()))
            }
        }
    };
}

impl_integer_parser!(i16, "i16");
impl_integer_parser!(i32, "i32");
impl_integer_parser!(i64, "i64");
impl_integer_parser!(u16, "u16");
impl_integer_parser!(u32, "u32");
impl_integer_parser!(u64, "u64");
impl_integer_parser!(usize, "usize");

impl ParseConfigValue for f64 {
    fn parse_config_value(value: &ConfigValue) -> Result<Self, ParseConfigValueError> {
        Self::from_str(value.as_str())
            .map_err(|_error| ParseConfigValueError::new("f64", value.as_str()))
    }
}

impl<T> ParseConfigValue for Option<T>
where
    T: ParseConfigValue,
{
    fn parse_config_value(value: &ConfigValue) -> Result<Self, ParseConfigValueError> {
        Ok(Some(T::parse_config_value(value)?))
    }
}

impl ParseConfigValue for String {
    fn parse_config_value(value: &ConfigValue) -> Result<Self, ParseConfigValueError> {
        Ok(value.as_str().into())
    }
}

/// Duration parsed from a Kafka `*.ms` value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DurationMs(Duration);

impl DurationMs {
    /// Creates a duration from milliseconds.
    #[must_use]
    pub const fn from_millis(milliseconds: u64) -> Self {
        Self(Duration::from_millis(milliseconds))
    }

    /// Returns the inner duration.
    #[must_use]
    pub const fn duration(self) -> Duration {
        self.0
    }

    /// Returns the duration as milliseconds.
    #[must_use]
    pub const fn as_millis(self) -> u128 {
        self.0.as_millis()
    }
}

impl ParseConfigValue for DurationMs {
    fn parse_config_value(value: &ConfigValue) -> Result<Self, ParseConfigValueError> {
        let milliseconds = u64::parse_config_value(value).map_err(|_error| {
            ParseConfigValueError::new("duration milliseconds", value.as_str())
        })?;
        Ok(Self::from_millis(milliseconds))
    }
}

/// Byte count parsed from Kafka byte-size config values.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ByteSize(u64);

impl ByteSize {
    /// Creates a byte size.
    #[must_use]
    pub const fn new(bytes: u64) -> Self {
        Self(bytes)
    }

    /// Returns the byte count.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl ParseConfigValue for ByteSize {
    fn parse_config_value(value: &ConfigValue) -> Result<Self, ParseConfigValueError> {
        let bytes = u64::parse_config_value(value)
            .map_err(|_error| ParseConfigValueError::new("byte size", value.as_str()))?;
        Ok(Self::new(bytes))
    }
}

/// TCP congestion control algorithm for platforms that expose per-socket control.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TcpCongestionControl {
    /// Linux BBR congestion control.
    Bbr,
    /// CUBIC congestion control.
    Cubic,
    /// Reno congestion control.
    Reno,
}

impl ParseConfigValue for TcpCongestionControl {
    fn parse_config_value(value: &ConfigValue) -> Result<Self, ParseConfigValueError> {
        match value.as_str().to_ascii_lowercase().as_str() {
            "bbr" => Ok(Self::Bbr),
            "cubic" => Ok(Self::Cubic),
            "reno" => Ok(Self::Reno),
            _ => Err(ParseConfigValueError::new(
                "TCP congestion control algorithm",
                value.as_str(),
            )),
        }
    }
}

/// Comma-separated Kafka list value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfigList(Vec<String>);

impl ConfigList {
    /// Parses a Kafka comma-separated list from a string.
    #[must_use]
    pub fn from_csv(value: &str) -> Self {
        Self(
            value
                .split(',')
                .filter_map(|part| {
                    let trimmed = part.trim();
                    if trimmed.is_empty() {
                        None
                    } else {
                        Some(trimmed.into())
                    }
                })
                .collect(),
        )
    }

    /// Returns the parsed items.
    #[must_use]
    pub const fn as_slice(&self) -> &[String] {
        self.0.as_slice()
    }
}

impl From<&str> for ConfigList {
    fn from(value: &str) -> Self {
        Self::from_csv(value)
    }
}

impl From<String> for ConfigList {
    fn from(value: String) -> Self {
        Self::from_csv(value.as_str())
    }
}

impl ParseConfigValue for ConfigList {
    fn parse_config_value(value: &ConfigValue) -> Result<Self, ParseConfigValueError> {
        Ok(Self::from_csv(value.as_str()))
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

    use super::{ByteSize, ConfigList, DurationMs, ParseConfigValue, TcpCongestionControl};
    use crate::config::ConfigValue;

    #[test]
    fn parser_rejects_invalid_duration_bytes_and_congestion() {
        let duration_error = DurationMs::parse_config_value(&ConfigValue::from("abc"))
            .expect_err("duration should reject text");
        let byte_error = ByteSize::parse_config_value(&ConfigValue::from("abc"))
            .expect_err("byte size should reject text");
        let congestion_error =
            TcpCongestionControl::parse_config_value(&ConfigValue::from("vegas"))
                .expect_err("unknown congestion algorithm should fail");

        assert_eq!(duration_error.target, "duration milliseconds");
        assert_eq!(byte_error.target, "byte size");
        assert_eq!(congestion_error.target, "TCP congestion control algorithm");
    }

    #[test]
    fn config_list_from_string_and_empty_csv_trims_values() {
        let list = ConfigList::from(String::from(" a, ,b ,, c "));

        assert_eq!(list.as_slice(), ["a", "b", "c"]);
    }
}
