//! Stage 3: pretty-print a [`proc_macro2::TokenStream`] with [`prettyplease`].
//!
//! One struct, one source — there's basically only one way this stage can
//! fail. If we ever grow more, split into `format/error.rs` then.

use proc_macro2::TokenStream;

/// `prettyplease` couldn't make sense of the tokens we handed it.
///
/// In practice this means stage 2 emitted something that isn't valid Rust,
/// which is a bug in *this* crate rather than something a caller can recover
/// from. The error type exists mostly so the binary's `anyhow::Result` chain
/// carries it cleanly.
#[derive(Debug, thiserror::Error)]
#[error("failed to pretty-print generated tokens for {item:?}")]
#[non_exhaustive]
pub struct FormatError {
    /// What we were trying to format — usually a Kafka message name.
    pub item: String,
    /// Whatever `syn` threw at us.
    #[source]
    pub source: syn::Error,
}

/// Pretty-print `tokens` with [`prettyplease`], tagging the result with `item`
/// for diagnostics.
///
/// Inserts a blank line after the leading `use` block so the output matches the
/// project's house style.
pub fn pretty(tokens: TokenStream, item: impl Into<String>) -> Result<String, FormatError> {
    let item = item.into();
    let file = syn::parse2::<syn::File>(tokens).map_err(|source| FormatError {
        item: item.clone(),
        source,
    })?;
    let unparsed = prettyplease::unparse(&file);
    Ok(insert_blank_line_after_uses(&unparsed))
}

/// Insert a single blank line after the contiguous block of leading `use`
/// statements, if any.
fn insert_blank_line_after_uses(source: &str) -> String {
    let mut out = String::with_capacity(source.len().saturating_add(1));
    let mut lines = source.lines().peekable();
    let mut in_use_block = false;
    let mut break_inserted = false;

    while let Some(line) = lines.next() {
        let trimmed = line.trim_start();
        let is_use = trimmed.starts_with("use ");

        if is_use {
            in_use_block = true;
            out.push_str(line);
            out.push('\n');
            continue;
        }

        if in_use_block && !break_inserted {
            if !trimmed.is_empty() {
                out.push('\n');
            }
            break_inserted = true;
            in_use_block = false;
        }

        out.push_str(line);
        out.push('\n');

        // `lines()` strips the trailing newline, so we add one per line above; undo it
        // on the last line when the input had none, preserving the original ending.
        if lines.peek().is_none() && !source.ends_with('\n') {
            let _ = out.pop();
        }
    }
    out
}
