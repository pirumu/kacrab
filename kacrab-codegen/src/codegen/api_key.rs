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
    let message_enums = generate_message_enums(specs);

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

        #message_enums
    }
}

fn generate_message_enums(specs: &[MessageSpec]) -> TokenStream {
    let pairs = collect_message_pairs(specs);
    let request_variants = message_kind_variants(&pairs, MessageKind::Request);
    let response_variants = message_kind_variants(&pairs, MessageKind::Response);
    let request_write_arms = message_kind_write_arms(&pairs, MessageKind::Request);
    let response_write_arms = message_kind_write_arms(&pairs, MessageKind::Response);
    let request_len_arms = message_kind_len_arms(&pairs, MessageKind::Request);
    let response_len_arms = message_kind_len_arms(&pairs, MessageKind::Response);
    let request_key_arms = message_kind_key_arms(&pairs, MessageKind::Request);
    let response_key_arms = message_kind_key_arms(&pairs, MessageKind::Response);

    quote! {
        #[cfg(feature = "message-enums")]
        use bytes::BytesMut;

        /// Type-erased generated Kafka requests.
        #[cfg(feature = "message-enums")]
        #[derive(Debug, Clone, PartialEq)]
        pub enum RequestKind {
            #(#request_variants,)*
        }

        #[cfg(feature = "message-enums")]
        impl RequestKind {
            pub fn api_key(&self) -> ApiKey {
                match self {
                    #(#request_key_arms,)*
                }
            }

            pub fn write(&self, buf: &mut BytesMut, version: i16) -> crate::Result<()> {
                match self {
                    #(#request_write_arms,)*
                }
            }

            pub fn encoded_len(&self, version: i16) -> crate::Result<usize> {
                match self {
                    #(#request_len_arms,)*
                }
            }
        }

        /// Type-erased generated Kafka responses.
        #[cfg(feature = "message-enums")]
        #[derive(Debug, Clone, PartialEq)]
        pub enum ResponseKind {
            #(#response_variants,)*
        }

        #[cfg(feature = "message-enums")]
        impl ResponseKind {
            pub fn api_key(&self) -> ApiKey {
                match self {
                    #(#response_key_arms,)*
                }
            }

            pub fn write(&self, buf: &mut BytesMut, version: i16) -> crate::Result<()> {
                match self {
                    #(#response_write_arms,)*
                }
            }

            pub fn encoded_len(&self, version: i16) -> crate::Result<usize> {
                match self {
                    #(#response_len_arms,)*
                }
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

struct MessagePair {
    variant: String,
    request_type: String,
    response_type: String,
}

#[derive(Clone, Copy)]
enum MessageKind {
    Request,
    Response,
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

fn collect_message_pairs(specs: &[MessageSpec]) -> Vec<MessagePair> {
    let mut requests = std::collections::BTreeMap::new();
    let mut responses = std::collections::BTreeMap::new();
    for spec in specs {
        let Some(api_key) = spec.api_key else {
            continue;
        };
        match spec.message_type {
            MessageType::Request => {
                let _previous = requests.insert(api_key, spec.name.clone());
            },
            MessageType::Response => {
                let _previous = responses.insert(api_key, spec.name.clone());
            },
            MessageType::Data => {},
        }
    }

    requests
        .into_iter()
        .filter_map(|(api_key, request)| {
            let response = responses.remove(&api_key)?;
            let variant = request
                .strip_suffix("Request")
                .unwrap_or(&request)
                .to_upper_camel_case();
            Some(MessagePair {
                variant,
                request_type: format!("{request}Data"),
                response_type: format!("{response}Data"),
            })
        })
        .collect()
}

fn message_kind_variants(pairs: &[MessagePair], kind: MessageKind) -> Vec<TokenStream> {
    pairs
        .iter()
        .map(|pair| {
            let variant = Ident::new(&pair.variant, Span::call_site());
            let ty = match kind {
                MessageKind::Request => Ident::new(&pair.request_type, Span::call_site()),
                MessageKind::Response => Ident::new(&pair.response_type, Span::call_site()),
            };
            quote! { #variant(#ty) }
        })
        .collect()
}

fn message_kind_write_arms(pairs: &[MessagePair], kind: MessageKind) -> Vec<TokenStream> {
    pairs
        .iter()
        .map(|pair| {
            let variant = Ident::new(&pair.variant, Span::call_site());
            let enum_name = match kind {
                MessageKind::Request => quote! { RequestKind },
                MessageKind::Response => quote! { ResponseKind },
            };
            quote! { #enum_name::#variant(message) => message.write(buf, version) }
        })
        .collect()
}

fn message_kind_len_arms(pairs: &[MessagePair], kind: MessageKind) -> Vec<TokenStream> {
    pairs
        .iter()
        .map(|pair| {
            let variant = Ident::new(&pair.variant, Span::call_site());
            let enum_name = match kind {
                MessageKind::Request => quote! { RequestKind },
                MessageKind::Response => quote! { ResponseKind },
            };
            quote! { #enum_name::#variant(message) => message.encoded_len(version) }
        })
        .collect()
}

fn message_kind_key_arms(pairs: &[MessagePair], kind: MessageKind) -> Vec<TokenStream> {
    pairs
        .iter()
        .map(|pair| {
            let variant = Ident::new(&pair.variant, Span::call_site());
            let api_variant = Ident::new(&pair.variant, Span::call_site());
            let enum_name = match kind {
                MessageKind::Request => quote! { RequestKind },
                MessageKind::Response => quote! { ResponseKind },
            };
            quote! { #enum_name::#variant(_) => ApiKey::#api_variant }
        })
        .collect()
}
