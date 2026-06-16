//! Deterministic Java-style property containers.

extern crate alloc;

use alloc::{collections::BTreeMap, string::String};
use core::fmt;

/// Normalized Java-style configuration key.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ConfigKey(String);

impl ConfigKey {
    /// Creates a configuration key.
    #[must_use]
    pub fn new(key: impl Into<String>) -> Self {
        Self(key.into())
    }

    /// Returns the key as a string slice.
    #[must_use]
    pub const fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl From<&str> for ConfigKey {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for ConfigKey {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl fmt::Display for ConfigKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Raw Java-style configuration value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfigValue(String);

impl ConfigValue {
    /// Creates a configuration value.
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Returns the value as a string slice.
    #[must_use]
    pub const fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl From<&str> for ConfigValue {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for ConfigValue {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl fmt::Display for ConfigValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Deterministic Java-style property container.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Properties {
    entries: BTreeMap<ConfigKey, ConfigValue>,
}

impl Properties {
    /// Creates an empty property set.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
        }
    }

    /// Inserts or replaces a key/value pair.
    pub fn insert(
        &mut self,
        key: impl Into<ConfigKey>,
        value: impl Into<ConfigValue>,
    ) -> Option<ConfigValue> {
        self.entries.insert(key.into(), value.into())
    }

    /// Returns a value by key.
    #[must_use]
    pub fn get(&self, key: &str) -> Option<&ConfigValue> {
        self.entries.get(&ConfigKey::from(key))
    }

    /// Returns entries in deterministic key order.
    pub fn iter(&self) -> impl Iterator<Item = (&ConfigKey, &ConfigValue)> {
        self.entries.iter()
    }

    /// Returns the number of entries.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns whether the property set is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl<K, V> FromIterator<(K, V)> for Properties
where
    K: Into<ConfigKey>,
    V: Into<ConfigValue>,
{
    fn from_iter<T: IntoIterator<Item = (K, V)>>(iter: T) -> Self {
        let mut properties = Self::new();
        for (key, value) in iter {
            let _previous = properties.insert(key, value);
        }
        properties
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

    use super::{ConfigKey, ConfigValue, Properties};

    #[test]
    fn keys_values_and_properties_display_deterministically() {
        let key = ConfigKey::from(String::from("bootstrap.servers"));
        let value = ConfigValue::from(String::from("localhost:9092"));

        assert_eq!(key.to_string(), "bootstrap.servers");
        assert_eq!(value.to_string(), "localhost:9092");

        let properties =
            Properties::from_iter([("z.key", "last"), ("bootstrap.servers", "localhost:9092")]);
        let keys: Vec<_> = properties
            .iter()
            .map(|(key, _value)| key.as_str())
            .collect();

        assert_eq!(keys, ["bootstrap.servers", "z.key"]);
        assert!(!properties.is_empty());
    }
}
