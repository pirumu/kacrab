//! Kafka protocol version range — parsing and set algebra.

use std::fmt;

/// A range of Kafka protocol versions.
///
/// Kafka specs express version ranges in three formats:
/// - `"none"` — no versions (field/feature absent)
/// - `"N+"` — all versions from N onwards (open-ended)
/// - `"N-M"` — versions N through M inclusive (bounded)
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VersionRange {
    /// No versions — the feature or field does not exist.
    None,
    /// All versions starting from `start` (inclusive, open-ended).
    From(i16),
    /// Versions from `start` to `end` inclusive.
    Range(i16, i16),
}

/// Reason a version range string couldn't be parsed.
#[derive(Debug, thiserror::Error)]
#[error("invalid version range: {raw:?}")]
#[non_exhaustive]
pub struct ParseError {
    /// The offending input string.
    pub raw: String,
}

impl ParseError {
    fn new(s: &str) -> Self {
        Self { raw: s.to_owned() }
    }
}

impl VersionRange {
    /// Parse a version range string from a Kafka spec.
    ///
    /// Accepted formats: `"none"`, `"N+"`, `"N-M"`, `"N"`.
    pub fn parse(s: &str) -> Result<Self, ParseError> {
        let s = s.trim();
        if s.is_empty() {
            return Err(ParseError::new(s));
        }
        if s == "none" {
            return Ok(Self::None);
        }
        if let Some(prefix) = s.strip_suffix('+') {
            let start: i16 = prefix.parse().map_err(|_| ParseError::new(s))?;
            if start < 0 {
                return Err(ParseError::new(s));
            }
            return Ok(Self::From(start));
        }
        if let Some((start_str, end_str)) = s.split_once('-') {
            if start_str.is_empty() || end_str.is_empty() {
                return Err(ParseError::new(s));
            }
            let start: i16 = start_str.parse().map_err(|_| ParseError::new(s))?;
            let end: i16 = end_str.parse().map_err(|_| ParseError::new(s))?;
            if start < 0 || end < start {
                return Err(ParseError::new(s));
            }
            return Ok(Self::Range(start, end));
        }
        if let Ok(v) = s.parse::<i16>()
            && v >= 0
        {
            return Ok(Self::Range(v, v));
        }
        Err(ParseError::new(s))
    }

    /// Returns true if this range contains the given version.
    pub const fn contains(&self, version: i16) -> bool {
        match self {
            Self::None => false,
            Self::From(start) => version >= *start,
            Self::Range(start, end) => version >= *start && version <= *end,
        }
    }

    /// Returns true if this is the [`VersionRange::None`] variant.
    pub const fn is_none(&self) -> bool {
        matches!(self, Self::None)
    }

    /// Returns true if this range fully covers (is a superset of) `other`.
    pub fn covers(&self, other: &Self) -> bool {
        match (self, other) {
            (_, Self::None) => true,
            (Self::None, _) | (Self::Range(_, _), Self::From(_)) => false,
            (Self::From(a), Self::From(b)) => a <= b,
            (Self::From(a), Self::Range(b_start, _)) => a <= b_start,
            (Self::Range(a_start, a_end), Self::Range(b_start, b_end)) => {
                a_start <= b_start && a_end >= b_end
            },
        }
    }

    /// Compute the intersection of two version ranges.
    #[must_use]
    pub fn intersect(&self, other: &Self) -> Self {
        if !self.intersects(other) {
            return Self::None;
        }
        let (self_min, self_max) = match self {
            Self::None => return Self::None,
            Self::From(s) => (*s, i16::MAX),
            Self::Range(s, e) => (*s, *e),
        };
        let (other_min, other_max) = match other {
            Self::None => return Self::None,
            Self::From(s) => (*s, i16::MAX),
            Self::Range(s, e) => (*s, *e),
        };
        let start = self_min.max(other_min);
        let end = self_max.min(other_max);
        if end == i16::MAX {
            Self::From(start)
        } else {
            Self::Range(start, end)
        }
    }

    /// Compute the set difference `self \ other`.
    ///
    /// Returns the versions in `self` not in `other`. May be 0, 1, or 2 ranges
    /// (when `other` sits in the middle of `self`).
    pub fn subtract(&self, other: &Self) -> Vec<Self> {
        if other.is_none() || !self.intersects(other) {
            if self.is_none() {
                return vec![];
            }
            return vec![self.clone()];
        }
        if other.covers(self) {
            return vec![];
        }
        let (self_min, self_max) = match self {
            Self::None => return vec![],
            Self::From(s) => (*s, i16::MAX),
            Self::Range(s, e) => (*s, *e),
        };
        let (other_min, other_max) = match other {
            Self::None => return vec![self.clone()],
            Self::From(s) => (*s, i16::MAX),
            Self::Range(s, e) => (*s, *e),
        };
        let mut result = vec![];
        if self_min < other_min {
            let end = other_min.saturating_sub(1).min(self_max);
            if self_min <= end {
                result.push(Self::Range(self_min, end));
            }
        }
        if self_max > other_max && other_max < i16::MAX {
            let start = other_max.saturating_add(1).max(self_min);
            if start <= self_max {
                if self_max == i16::MAX {
                    result.push(Self::From(start));
                } else {
                    result.push(Self::Range(start, self_max));
                }
            }
        }
        result
    }

    /// Returns true if this range overlaps with another range.
    pub const fn intersects(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::None, _) | (_, Self::None) => false,
            (Self::From(_), Self::From(_)) => true,
            (Self::From(a_start), Self::Range(_, b_end))
            | (Self::Range(_, b_end), Self::From(a_start)) => *a_start <= *b_end,
            (Self::Range(a_start, a_end), Self::Range(b_start, b_end)) => {
                *a_start <= *b_end && *b_start <= *a_end
            },
        }
    }
}

impl fmt::Display for VersionRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::None => write!(f, "none"),
            Self::From(start) => write!(f, "{start}+"),
            Self::Range(start, end) => write!(f, "{start}-{end}"),
        }
    }
}
