//! Strip C-style line comments from Kafka spec JSON before deserializing.

/// Remove `// ...` line comments from `input`, ignoring `//` inside JSON strings.
///
/// Kafka spec files are JSON with C-style line comments — invalid JSON until
/// these are stripped. The pass tracks whether the cursor is inside a quoted
/// string (with backslash escape handling) so a `//` literal inside a value is
/// preserved.
pub(crate) fn strip_comments(input: &str) -> String {
    let mut result = String::with_capacity(input.len());
    for line in input.lines() {
        let mut in_string = false;
        let mut escape = false;
        let mut comment_start = None;

        let mut iter = line.as_bytes().iter().enumerate().peekable();
        while let Some((i, &ch)) = iter.next() {
            if escape {
                escape = false;
                continue;
            }
            if ch == b'\\' && in_string {
                escape = true;
                continue;
            }
            if ch == b'"' {
                in_string = !in_string;
                continue;
            }
            if !in_string && ch == b'/' && iter.peek().is_some_and(|&(_, &next)| next == b'/') {
                comment_start = Some(i);
                break;
            }
        }

        let trimmed = comment_start.map_or(line, |pos| line.get(..pos).unwrap_or(line));
        result.push_str(trimmed);
        result.push('\n');
    }
    result
}
