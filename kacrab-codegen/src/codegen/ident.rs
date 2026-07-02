//! Rust keyword escaping and shadow-avoiding identifier helpers.

use heck::ToSnakeCase;
use proc_macro2::{Ident, Span};

const RUST_KEYWORDS: &[&str] = &[
    "type", "match", "self", "super", "crate", "mod", "fn", "let", "mut", "ref", "pub", "use",
    "as", "in", "for", "if", "else", "while", "loop", "break", "continue", "return", "where",
    "move", "impl", "trait", "struct", "enum", "const", "static", "extern", "async", "await",
    "dyn", "abstract", "become", "box", "do", "final", "macro", "override", "priv", "typeof",
    "unsized", "virtual", "yield", "try",
];

/// Names that would shadow read/write method parameters.
const METHOD_PARAM_NAMES: &[&str] = &["version", "buf"];

/// Keywords that are *not* valid raw identifiers: `Ident::new_raw` panics on them,
/// so they get an underscore suffix instead of `r#` escaping. (`to_snake_case`
/// lower-cases `Self` to `self`, so only the lower-case forms need listing.)
const NON_RAW_KEYWORDS: &[&str] = &["self", "super", "crate"];

/// Convert a field name to a [`proc_macro2::Ident`], using a raw identifier when
/// the snake-cased form clashes with a Rust keyword. Reserved words that cannot be
/// raw identifiers (`self`, `super`, `crate`) get an underscore suffix instead.
pub(crate) fn safe_rust_ident(name: &str) -> Ident {
    let snake = name.to_snake_case();
    if NON_RAW_KEYWORDS.contains(&snake.as_str()) {
        Ident::new(&format!("{snake}_"), Span::call_site())
    } else if RUST_KEYWORDS.contains(&snake.as_str()) {
        Ident::new_raw(&snake, Span::call_site())
    } else {
        Ident::new(&snake, Span::call_site())
    }
}

/// Returns `true` if the snake-cased `name` would shadow a read/write method parameter.
pub(crate) fn shadows_param(name: &str) -> bool {
    METHOD_PARAM_NAMES.contains(&name.to_snake_case().as_str())
}

/// Identifier for a local variable that avoids shadowing read/write parameters and
/// keyword collisions.
pub(crate) fn local_var_ident(name: &str) -> Ident {
    let snake = name.to_snake_case();
    if METHOD_PARAM_NAMES.contains(&snake.as_str()) || NON_RAW_KEYWORDS.contains(&snake.as_str()) {
        Ident::new(&format!("{snake}_"), Span::call_site())
    } else if RUST_KEYWORDS.contains(&snake.as_str()) {
        Ident::new_raw(&snake, Span::call_site())
    } else {
        Ident::new(&snake, Span::call_site())
    }
}

/// Builder setter name for a field. Field identifiers may be raw (`r#type`),
/// but method names should stay ordinary Rust identifiers (`with_type`).
pub(crate) fn builder_method_ident(name: &str) -> Ident {
    let snake = name.to_snake_case();
    Ident::new(&format!("with_{snake}"), Span::call_site())
}
