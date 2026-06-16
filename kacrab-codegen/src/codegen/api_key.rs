//! Generation of the `ApiKey` enum, `ApiInfo` struct, and `client_api_info` lookup.

use heck::ToUpperCamelCase;
use proc_macro2::{Ident, Literal, Span, TokenStream};
use quote::quote;

use crate::ir::{
    message::{MessageSpec, MessageType},
    version_range::VersionRange,
};

/// Generate the `ApiKey` + `ApiInfo` + `client_api_info` token stream from the
/// request specs in `specs`.
pub(crate) fn generate_api_key(specs: &[MessageSpec]) -> TokenStream {
    let entries = collect_api_key_entries(specs);

    let variants: Vec<TokenStream> = entries
        .iter()
        .map(|e| {
            let ident = Ident::new(&e.variant, Span::call_site());
            let lit = Literal::i16_unsuffixed(e.api_key);
            quote! { #ident = #lit }
        })
        .collect();

    let from_arms: Vec<TokenStream> = entries
        .iter()
        .map(|e| {
            let ident = Ident::new(&e.variant, Span::call_site());
            let lit = Literal::i16_unsuffixed(e.api_key);
            quote! { #lit => Some(ApiKey::#ident) }
        })
        .collect();

    let api_info_arms: Vec<TokenStream> = entries
        .iter()
        .map(|e| {
            let ident = Ident::new(&e.variant, Span::call_site());
            let min_lit = Literal::i16_unsuffixed(e.min_version);
            let max_lit = Literal::i16_unsuffixed(e.max_version);
            let flex_lit = if e.flexible_versions_start == i16::MAX {
                quote! { i16::MAX }
            } else {
                let lit = Literal::i16_unsuffixed(e.flexible_versions_start);
                quote! { #lit }
            };
            quote! {
                ApiKey::#ident => ApiInfo {
                    min_version: #min_lit,
                    max_version: #max_lit,
                    flexible_versions_start: #flex_lit,
                }
            }
        })
        .collect();

    quote! {
        /// Kafka API keys for request dispatch.
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        #[repr(i16)]
        pub enum ApiKey {
            #(#variants,)*
        }

        impl ApiKey {
            pub fn from_i16(value: i16) -> Option<Self> {
                match value {
                    #(#from_arms,)*
                    _ => None,
                }
            }
        }

        /// Static metadata about a Kafka API: supported version range and flexible-encoding threshold.
        ///
        /// `flexible_versions_start` is the first message version that uses flexible encoding.
        /// Use `i16::MAX` to indicate the API is never flexible.
        #[derive(Debug, Clone, Copy)]
        pub struct ApiInfo {
            pub min_version: i16,
            pub max_version: i16,
            pub flexible_versions_start: i16,
        }

        /// Returns the client-side [`ApiInfo`] for the given [`ApiKey`].
        ///
        /// The data is derived from the Kafka protocol JSON spec files at code-generation time.
        pub fn client_api_info(api_key: ApiKey) -> ApiInfo {
            match api_key {
                #(#api_info_arms,)*
            }
        }
    }
}

struct ApiKeyEntry {
    api_key: i16,
    variant: String,
    min_version: i16,
    max_version: i16,
    flexible_versions_start: i16,
}

fn collect_api_key_entries(specs: &[MessageSpec]) -> Vec<ApiKeyEntry> {
    let mut entries: Vec<ApiKeyEntry> = specs
        .iter()
        .filter(|s| s.message_type == MessageType::Request)
        .filter_map(|s| {
            let api_key = s.api_key?;
            let variant = s
                .name
                .strip_suffix("Request")
                .unwrap_or(&s.name)
                .to_upper_camel_case();
            let (min_version, max_version) = match &s.valid_versions {
                VersionRange::Range(lo, hi) => (*lo, *hi),
                VersionRange::From(lo) => (*lo, i16::MAX),
                VersionRange::None => (0, 0),
            };
            let flexible_versions_start = match &s.flexible_versions {
                VersionRange::From(start) | VersionRange::Range(start, _) => *start,
                VersionRange::None => i16::MAX,
            };
            Some(ApiKeyEntry {
                api_key,
                variant,
                min_version,
                max_version,
                flexible_versions_start,
            })
        })
        .collect();
    entries.sort_by_key(|e| e.api_key);
    entries
}
