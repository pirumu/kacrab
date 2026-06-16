//! Lower scraped [`ErrorEntry`] triples into a Rust `ErrorCode` enum.

use heck::ToUpperCamelCase;
use proc_macro2::{Ident, Literal, Span, TokenStream};
use quote::quote;

use super::{parser::ErrorEntry, retriable::is_retriable};

/// Lower a slice of [`ErrorEntry`] into the [`proc_macro2::TokenStream`] for the
/// generated `error_code.rs` module.
pub fn lower(entries: &[ErrorEntry]) -> TokenStream {
    let mut variant_defs = Vec::with_capacity(entries.len());
    let mut from_i16_arms = Vec::with_capacity(entries.len());
    let mut code_arms = Vec::with_capacity(entries.len());
    let mut retriable_arms = Vec::new();
    let mut display_arms = Vec::with_capacity(entries.len());

    for entry in entries {
        let variant = entry.variant_name.to_upper_camel_case();
        let ident = Ident::new(&variant, Span::call_site());
        let code_lit = Literal::i16_unsuffixed(entry.code);

        variant_defs.push(quote! { #ident, });
        from_i16_arms.push(quote! { #code_lit => ErrorCode::#ident, });
        code_arms.push(quote! { ErrorCode::#ident => #code_lit, });

        if is_retriable(entry.exception.as_deref()) {
            retriable_arms.push(quote! { ErrorCode::#ident => true, });
        }

        let display_str = entry
            .message
            .clone()
            .unwrap_or_else(|| entry.variant_name.clone());
        let display_lit = Literal::string(&display_str);
        display_arms.push(quote! { ErrorCode::#ident => write!(f, #display_lit), });
    }

    quote! {
        //! Kafka protocol error codes.
        //!
        //! Generated from Kafka's `Errors.java` -- DO NOT EDIT.
        #![allow(
            missing_docs,
            clippy::all,
            clippy::pedantic,
            clippy::nursery,
            reason = "Generated protocol error-code variants mirror Kafka's Java enum and intentionally avoid duplicating Java docs for every variant."
        )]

        /// Kafka protocol error code.
        ///
        /// Every response in the Kafka protocol carries an `i16` error code.
        /// This enum provides a typed representation with human-readable messages,
        /// retriability classification, and forward-compatible handling of unknown codes.
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        #[non_exhaustive]
        pub enum ErrorCode {
            #(#variant_defs)*
            /// An error code not recognized by this client version.
            Unknown(i16),
        }

        impl ErrorCode {
            /// Returns the `i16` wire value for this error code.
            pub fn code(&self) -> i16 {
                match self {
                    #(#code_arms)*
                    ErrorCode::Unknown(c) => *c,
                }
            }

            /// Returns `true` if the operation that produced this error can be retried.
            ///
            /// The retriable set matches the Kafka Java client's `RetriableException` hierarchy.
            /// [`ErrorCode::Unknown`] is **not** retriable (matches Java client behaviour).
            pub fn is_retriable(&self) -> bool {
                match self {
                    #(#retriable_arms)*
                    _ => false,
                }
            }

            /// Returns `true` if this represents an error condition.
            ///
            /// Only [`ErrorCode::None`] (code 0) returns `false`.
            pub fn is_error(&self) -> bool {
                !matches!(self, ErrorCode::None)
            }
        }

        impl From<i16> for ErrorCode {
            fn from(code: i16) -> Self {
                match code {
                    #(#from_i16_arms)*
                    other => ErrorCode::Unknown(other),
                }
            }
        }

        impl From<ErrorCode> for i16 {
            fn from(error: ErrorCode) -> Self {
                error.code()
            }
        }

        impl std::fmt::Display for ErrorCode {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                match self {
                    #(#display_arms)*
                    ErrorCode::Unknown(code) => write!(f, "Unknown error code: {}", code),
                }
            }
        }
    }
}
