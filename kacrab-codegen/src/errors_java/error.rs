//! Errors from the optional `Errors.java` scraper.

use std::path::PathBuf;

/// Anything that can go wrong while scraping upstream's `Errors.java`.
#[derive(Debug, thiserror::Error)]
#[error("failed to parse Errors.java at {path}")]
#[non_exhaustive]
pub struct ErrorsJavaError {
    /// Path we tried to scrape — typically the value of `--errors-java`.
    pub path: PathBuf,
    /// Underlying cause; preserved in the [`std::error::Error::source`] chain.
    #[source]
    pub kind: ErrorsJavaErrorKind,
}

/// Reason the `Errors.java` scraper bailed.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ErrorsJavaErrorKind {
    /// Disk I/O — usually `--errors-java` pointing at the wrong file.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// Opened the file fine but never found the `public enum Errors { ... }` block.
    #[error("could not locate the `public enum Errors` block")]
    MissingEnumBlock,
    /// A variant declared a numeric code that didn't fit into [`i16`].
    #[error("invalid error code for variant {variant:?}: {raw:?}")]
    InvalidCode {
        /// `SCREAMING_SNAKE_CASE` variant name.
        variant: String,
        /// The raw code string we couldn't parse.
        raw: String,
    },
    /// Far fewer entries than expected — almost always a parser-regex regression.
    #[error("expected at least {min} entries but found only {found}")]
    EntryCountTooLow {
        /// Number of entries actually parsed.
        found: usize,
        /// Lower bound below which we treat the scrape as broken.
        min: usize,
    },
}

impl ErrorsJavaError {
    /// Glue a `path` onto a `kind` to build the full error.
    pub fn new(path: impl Into<PathBuf>, kind: impl Into<ErrorsJavaErrorKind>) -> Self {
        Self {
            path: path.into(),
            kind: kind.into(),
        }
    }
}
