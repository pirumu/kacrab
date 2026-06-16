//! Generation of encoded-size expressions mirroring write-side encoding.

use proc_macro2::{Ident, TokenStream};
use quote::{format_ident, quote};

use super::{
    ty::{has_version_conditional_nullability, is_field_nullable, needs_flexible_branching},
    version_check::{
        effective_flex_versions, flexible_version_check_with_context, version_check_always_true,
        version_check_never_true, version_contains_check_with_context,
    },
};
use crate::ir::{
    field::{FieldSpec, FieldType},
    version_range::VersionRange,
};

/// Generate size accumulation for one non-tagged field.
pub(crate) fn generate_len_field_expr(
    field: &FieldSpec,
    var_ident: &Ident,
    flex_versions: &VersionRange,
    effective_versions: &VersionRange,
) -> TokenStream {
    let is_nullable = is_field_nullable(field);

    if has_version_conditional_nullability(field) {
        let nullable_context = field.versions.intersect(effective_versions);
        let nullable_check =
            version_contains_check_with_context(&field.nullable_versions, &nullable_context);
        let nullable_effective = nullable_context.intersect(&field.nullable_versions);
        let mut non_nullable_parts = nullable_context.subtract(&field.nullable_versions);
        let non_nullable_effective = match non_nullable_parts.len() {
            0 => VersionRange::None,
            1 => non_nullable_parts.swap_remove(0),
            _ => nullable_context,
        };
        let nullable_len = generate_len_field_expr_inner(
            field,
            var_ident,
            flex_versions,
            true,
            &nullable_effective,
        );
        let non_nullable_len = generate_len_option_as_non_nullable(
            field,
            var_ident,
            flex_versions,
            &non_nullable_effective,
        );
        return quote! {
            if #nullable_check {
                #nullable_len
            } else {
                #non_nullable_len
            }
        };
    }

    generate_len_field_expr_inner(
        field,
        var_ident,
        flex_versions,
        is_nullable,
        effective_versions,
    )
}

fn generate_len_field_expr_inner(
    field: &FieldSpec,
    var_ident: &Ident,
    flex_versions: &VersionRange,
    nullable: bool,
    effective_versions: &VersionRange,
) -> TokenStream {
    let narrowed = field.versions.intersect(effective_versions);
    let effective_versions = &narrowed;
    let needs_flex_branch = needs_flexible_branching(&field.field_type);
    let eff_flex = effective_flex_versions(field, flex_versions);

    if needs_flex_branch {
        if eff_flex.is_none() {
            len_expr_for_type(&field.field_type, var_ident, nullable, false)
        } else if version_check_always_true(&eff_flex, effective_versions) {
            len_expr_for_type(&field.field_type, var_ident, nullable, true)
        } else if version_check_never_true(&eff_flex, effective_versions) {
            len_expr_for_type(&field.field_type, var_ident, nullable, false)
        } else {
            let flex_check = flexible_version_check_with_context(&eff_flex, effective_versions);
            let compact_expr = len_expr_for_type(&field.field_type, var_ident, nullable, true);
            let standard_expr = len_expr_for_type(&field.field_type, var_ident, nullable, false);
            quote! {
                if #flex_check {
                    #compact_expr
                } else {
                    #standard_expr
                }
            }
        }
    } else {
        len_expr_for_type(&field.field_type, var_ident, nullable, false)
    }
}

fn generate_len_option_as_non_nullable(
    field: &FieldSpec,
    var_ident: &Ident,
    flex_versions: &VersionRange,
    effective_versions: &VersionRange,
) -> TokenStream {
    let eff_flex = effective_flex_versions(field, flex_versions);
    match &field.field_type {
        FieldType::String => {
            len_string_option_as_non_nullable(var_ident, &eff_flex, effective_versions)
        },
        FieldType::Bytes => {
            len_bytes_option_as_non_nullable(var_ident, &eff_flex, effective_versions)
        },
        FieldType::Array(inner) => {
            if eff_flex.is_none() {
                generate_len_array_from_option(inner, var_ident, false)
            } else if version_check_always_true(&eff_flex, effective_versions) {
                generate_len_array_from_option(inner, var_ident, true)
            } else if version_check_never_true(&eff_flex, effective_versions) {
                generate_len_array_from_option(inner, var_ident, false)
            } else {
                let flex_check = flexible_version_check_with_context(&eff_flex, effective_versions);
                let compact_len = generate_len_array_from_option(inner, var_ident, true);
                let standard_len = generate_len_array_from_option(inner, var_ident, false);
                quote! {
                    if #flex_check {
                        #compact_len
                    } else {
                        #standard_len
                    }
                }
            }
        },
        FieldType::Struct(_) => {
            let non_nullable_len = len_expr_for_type(&field.field_type, var_ident, false, false);
            quote! {
                match &self.#var_ident {
                    Some(v) => {
                        len += v.encoded_len(version)?;
                    }
                    None => {
                        #non_nullable_len
                    }
                }
            }
        },
        _ => len_expr_for_type(&field.field_type, var_ident, false, false),
    }
}

fn len_string_option_as_non_nullable(
    var_ident: &Ident,
    eff_flex: &VersionRange,
    effective_versions: &VersionRange,
) -> TokenStream {
    if eff_flex.is_none() {
        return quote! {
            let _nn_default = KafkaString::default();
            let _nn_val = self.#var_ident.as_ref().unwrap_or(&_nn_default);
            len += string_len(_nn_val)?;
        };
    }
    if version_check_always_true(eff_flex, effective_versions) {
        quote! {
            let _nn_default = KafkaString::default();
            let _nn_val = self.#var_ident.as_ref().unwrap_or(&_nn_default);
            len += compact_string_len(_nn_val)?;
        }
    } else if version_check_never_true(eff_flex, effective_versions) {
        quote! {
            let _nn_default = KafkaString::default();
            let _nn_val = self.#var_ident.as_ref().unwrap_or(&_nn_default);
            len += string_len(_nn_val)?;
        }
    } else {
        let flex_check = flexible_version_check_with_context(eff_flex, effective_versions);
        quote! {
            let _nn_default = KafkaString::default();
            let _nn_val = self.#var_ident.as_ref().unwrap_or(&_nn_default);
            if #flex_check {
                len += compact_string_len(_nn_val)?;
            } else {
                len += string_len(_nn_val)?;
            }
        }
    }
}

fn len_bytes_option_as_non_nullable(
    var_ident: &Ident,
    eff_flex: &VersionRange,
    effective_versions: &VersionRange,
) -> TokenStream {
    if eff_flex.is_none() {
        return quote! {
            let _nn_default = Bytes::new();
            let _nn_val = self.#var_ident.as_ref().unwrap_or(&_nn_default);
            len += bytes_len(_nn_val)?;
        };
    }
    if version_check_always_true(eff_flex, effective_versions) {
        quote! {
            let _nn_default = Bytes::new();
            let _nn_val = self.#var_ident.as_ref().unwrap_or(&_nn_default);
            len += compact_bytes_len(_nn_val)?;
        }
    } else if version_check_never_true(eff_flex, effective_versions) {
        quote! {
            let _nn_default = Bytes::new();
            let _nn_val = self.#var_ident.as_ref().unwrap_or(&_nn_default);
            len += bytes_len(_nn_val)?;
        }
    } else {
        let flex_check = flexible_version_check_with_context(eff_flex, effective_versions);
        quote! {
            let _nn_default = Bytes::new();
            let _nn_val = self.#var_ident.as_ref().unwrap_or(&_nn_default);
            if #flex_check {
                len += compact_bytes_len(_nn_val)?;
            } else {
                len += bytes_len(_nn_val)?;
            }
        }
    }
}

fn len_expr_for_type(
    ft: &FieldType,
    var_ident: &Ident,
    nullable: bool,
    flexible: bool,
) -> TokenStream {
    match ft {
        FieldType::Bool | FieldType::Int8 => quote! { len += 1; },
        FieldType::Int16 | FieldType::Uint16 => quote! { len += 2; },
        FieldType::Int32 => quote! { len += 4; },
        FieldType::Int64 | FieldType::Float64 => quote! { len += 8; },
        FieldType::Uuid => quote! { len += 16; },
        FieldType::String => match (nullable, flexible) {
            (true, true) => {
                quote! { len += compact_nullable_string_len(self.#var_ident.as_ref())?; }
            },
            (true, false) => quote! { len += nullable_string_len(self.#var_ident.as_ref())?; },
            (false, true) => quote! { len += compact_string_len(&self.#var_ident)?; },
            (false, false) => quote! { len += string_len(&self.#var_ident)?; },
        },
        FieldType::Bytes => match (nullable, flexible) {
            (true, true) => quote! {
                len += compact_nullable_bytes_len(self.#var_ident.as_ref().map(|b| b.as_ref()))?;
            },
            (true, false) => quote! {
                len += nullable_bytes_len(self.#var_ident.as_ref().map(|b| b.as_ref()))?;
            },
            (false, true) => quote! { len += compact_bytes_len(&self.#var_ident)?; },
            (false, false) => quote! { len += bytes_len(&self.#var_ident)?; },
        },
        FieldType::Records => {
            if flexible {
                quote! {
                    len += compact_nullable_bytes_len(self.#var_ident.as_ref().map(|b| b.as_ref()))?;
                }
            } else {
                quote! {
                    len += nullable_bytes_len(self.#var_ident.as_ref().map(|b| b.as_ref()))?;
                }
            }
        },
        FieldType::Struct(_) => {
            if nullable {
                quote! {
                    match &self.#var_ident {
                        None => {
                            len += 1;
                        }
                        Some(v) => {
                            len += 1;
                            len += v.encoded_len(version)?;
                        }
                    }
                }
            } else {
                quote! { len += self.#var_ident.encoded_len(version)?; }
            }
        },
        FieldType::Array(inner) => generate_len_array_expr(inner, var_ident, nullable, flexible),
    }
}

fn generate_len_array_from_option(
    inner: &FieldType,
    var_ident: &Ident,
    flexible: bool,
) -> TokenStream {
    let none_len = if flexible {
        quote! { compact_array_length_len(0) }
    } else {
        quote! { array_length_len() }
    };
    let element_ident = format_ident!("el");
    let element_expr = quote! { #element_ident };
    let array_len = array_len_expr(flexible, &quote! { arr.len() });
    let inner_len = array_element_len(inner, &element_expr, flexible, 1);
    let array_payload_len = fixed_element_size(inner).map_or_else(
        || {
            quote! {
                for #element_ident in arr {
                    #inner_len
                }
            }
        },
        |size| quote! { len += arr.len() * #size; },
    );

    quote! {
        match &self.#var_ident {
            Some(arr) => {
                len += #array_len;
                #array_payload_len
            }
            None => {
                len += #none_len;
            }
        }
    }
}

fn generate_len_array_expr(
    inner: &FieldType,
    var_ident: &Ident,
    nullable: bool,
    flexible: bool,
) -> TokenStream {
    let self_array_len = array_len_expr(flexible, &quote! { self.#var_ident.len() });
    let borrowed_array_len = array_len_expr(flexible, &quote! { arr.len() });
    let element_ident = format_ident!("el");
    let element_expr = quote! { #element_ident };
    let inner_len = array_element_len(inner, &element_expr, flexible, 1);
    let array_payload_len = fixed_element_size(inner).map_or_else(
        || {
            quote! {
                for #element_ident in arr {
                    #inner_len
                }
            }
        },
        |size| quote! { len += arr.len() * #size; },
    );
    let self_array_payload_len = fixed_element_size(inner).map_or_else(
        || {
            quote! {
                for #element_ident in &self.#var_ident {
                    #inner_len
                }
            }
        },
        |size| quote! { len += self.#var_ident.len() * #size; },
    );

    if nullable {
        let none_len = if flexible {
            quote! { compact_array_length_len(-1) }
        } else {
            quote! { array_length_len() }
        };
        quote! {
            match &self.#var_ident {
                None => {
                    len += #none_len;
                }
                Some(arr) => {
                    len += #borrowed_array_len;
                    #array_payload_len
                }
            }
        }
    } else {
        quote! {
            len += #self_array_len;
            #self_array_payload_len
        }
    }
}

fn array_len_expr(flexible: bool, len_expr: &TokenStream) -> TokenStream {
    if flexible {
        quote! { compact_array_length_len(#len_expr as i32) }
    } else {
        quote! { array_length_len() }
    }
}

const fn fixed_element_size(inner: &FieldType) -> Option<usize> {
    match inner {
        FieldType::Bool | FieldType::Int8 => Some(1),
        FieldType::Int16 | FieldType::Uint16 => Some(2),
        FieldType::Int32 => Some(4),
        FieldType::Int64 | FieldType::Float64 => Some(8),
        FieldType::Uuid => Some(16),
        FieldType::String
        | FieldType::Bytes
        | FieldType::Records
        | FieldType::Struct(_)
        | FieldType::Array(_) => None,
    }
}

fn array_element_len(
    inner: &FieldType,
    element_expr: &TokenStream,
    flexible: bool,
    depth: usize,
) -> TokenStream {
    match inner {
        FieldType::Struct(_) => quote! { len += #element_expr.encoded_len(version)?; },
        FieldType::Bool | FieldType::Int8 => quote! { len += 1; },
        FieldType::Int16 | FieldType::Uint16 => quote! { len += 2; },
        FieldType::Int32 => quote! { len += 4; },
        FieldType::Int64 | FieldType::Float64 => quote! { len += 8; },
        FieldType::Uuid => quote! { len += 16; },
        FieldType::String => {
            if flexible {
                quote! { len += compact_string_len(#element_expr)?; }
            } else {
                quote! { len += string_len(#element_expr)?; }
            }
        },
        FieldType::Bytes => {
            if flexible {
                quote! { len += compact_bytes_len(#element_expr)?; }
            } else {
                quote! { len += bytes_len(#element_expr)?; }
            }
        },
        FieldType::Records => {
            if flexible {
                quote! { len += compact_nullable_bytes_len(#element_expr.as_ref().map(|b| b.as_ref()))?; }
            } else {
                quote! { len += nullable_bytes_len(#element_expr.as_ref().map(|b| b.as_ref()))?; }
            }
        },
        FieldType::Array(nested) => {
            let nested_ident = format_ident!("el_{depth}");
            let nested_expr = quote! { #nested_ident };
            let nested_len =
                array_element_len(nested, &nested_expr, flexible, depth.saturating_add(1));
            let nested_array_len = array_len_expr(flexible, &quote! { #element_expr.len() });
            quote! {
                len += #nested_array_len;
                for #nested_ident in #element_expr {
                    #nested_len
                }
            }
        },
    }
}
