//! Scrape `(code, name, exception)` triples from upstream Kafka's `Errors.java`.

use std::{fs, path::Path, sync::OnceLock};

use regex::Regex;

use super::error::{ErrorsJavaError, ErrorsJavaErrorKind};

/// Lower bound below which we treat a scrape as broken.
///
/// Upstream had ~134 entries at the time this was written. A real regression in
/// the regex would drop the count to ~0; tightening this guards against silent
/// half-broken parses.
const MIN_EXPECTED_ENTRIES: usize = 130;

/// One entry parsed from `Errors.java`.
#[derive(Debug, Clone)]
pub struct ErrorEntry {
    /// `SCREAMING_SNAKE_CASE` variant name, e.g. `UNKNOWN_SERVER_ERROR`.
    pub variant_name: String,
    /// The `i16` wire code, e.g. `-1`.
    pub code: i16,
    /// Human-readable message; `None` for the `NONE` sentinel which uses `null`.
    pub message: Option<String>,
    /// Exception class simple name, e.g. `UnknownServerException`.
    pub exception: Option<String>,
}

/// Scrape `Errors.java` at `path` and return one [`ErrorEntry`] per declared variant.
pub fn scrape(path: &Path) -> Result<Vec<ErrorEntry>, ErrorsJavaError> {
    scrape_inner(path).map_err(|kind| ErrorsJavaError::new(path, kind))
}

fn scrape_inner(path: &Path) -> Result<Vec<ErrorEntry>, ErrorsJavaErrorKind> {
    let content = fs::read_to_string(path)?;
    let enum_start = content
        .find("public enum Errors {")
        .ok_or(ErrorsJavaErrorKind::MissingEnumBlock)?;
    let enum_body = content
        .get(enum_start..)
        .ok_or(ErrorsJavaErrorKind::MissingEnumBlock)?;

    let normalized = concat_re().replace_all(enum_body, "").into_owned();

    let mut entries = Vec::new();
    for cap in entry_re().captures_iter(&normalized) {
        let variant = cap[1].to_owned();
        let raw_code = &cap[2];
        let code: i16 = raw_code
            .parse()
            .map_err(|_| ErrorsJavaErrorKind::InvalidCode {
                variant: variant.clone(),
                raw: raw_code.to_owned(),
            })?;
        let message = cap.get(3).map(|m| m.as_str().to_owned());
        let exception = cap.get(4).map(|m| m.as_str().to_owned());
        entries.push(ErrorEntry {
            variant_name: variant,
            code,
            message,
            exception,
        });
    }

    if entries.len() < MIN_EXPECTED_ENTRIES {
        return Err(ErrorsJavaErrorKind::EntryCountTooLow {
            found: entries.len(),
            min: MIN_EXPECTED_ENTRIES,
        });
    }
    Ok(entries)
}

#[expect(
    clippy::expect_used,
    reason = "regex pattern is a static string literal; a parse failure is a developer error \
              caught at the first call to scrape()."
)]
fn concat_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r#""\s*\+\s*\n\s*""#).expect("hand-written regex compiles"))
}

#[expect(
    clippy::expect_used,
    reason = "regex pattern is a static string literal; a parse failure is a developer error \
              caught at the first call to scrape()."
)]
fn entry_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(
            r#"(?m)^\s*([A-Z][A-Z0-9_]*)\((-?\d+),\s*(?:"([^"]+)"|null),\s*(?:(\w+)::new|[^)]*->.*?null)\)"#,
        )
        .expect("hand-written regex compiles")
    })
}
