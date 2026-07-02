//! Generation of write-side field expressions: per-field encoding, tagged
//! writes, and primitive/array encoders.

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

/// Generate the write expression for a non-tagged field.
pub(crate) fn generate_write_field_expr(
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
        let nullable_write = generate_write_field_expr_inner(
            field,
            var_ident,
            flex_versions,
            true,
            &nullable_effective,
        );
        let non_nullable_write = generate_write_option_as_non_nullable(
            field,
            var_ident,
            flex_versions,
            &non_nullable_effective,
        );
        return quote! {
            if #nullable_check {
                #nullable_write
            } else {
                #non_nullable_write
            }
        };
    }

    generate_write_field_expr_inner(
        field,
        var_ident,
        flex_versions,
        is_nullable,
        effective_versions,
    )
}

fn generate_write_field_expr_inner(
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
            write_expr_for_type(&field.field_type, var_ident, nullable, false)
        } else if version_check_always_true(&eff_flex, effective_versions) {
            write_expr_for_type(&field.field_type, var_ident, nullable, true)
        } else if version_check_never_true(&eff_flex, effective_versions) {
            write_expr_for_type(&field.field_type, var_ident, nullable, false)
        } else {
            let flex_check = flexible_version_check_with_context(&eff_flex, effective_versions);
            let compact_expr = write_expr_for_type(&field.field_type, var_ident, nullable, true);
            let standard_expr = write_expr_for_type(&field.field_type, var_ident, nullable, false);
            quote! {
                if #flex_check {
                    #compact_expr
                } else {
                    #standard_expr
                }
            }
        }
    } else {
        write_expr_for_type(&field.field_type, var_ident, nullable, false)
    }
}

fn generate_write_option_as_non_nullable(
    field: &FieldSpec,
    var_ident: &Ident,
    flex_versions: &VersionRange,
    effective_versions: &VersionRange,
) -> TokenStream {
    let eff_flex = effective_flex_versions(field, flex_versions);
    let buf_expr = quote! { buf };
    match &field.field_type {
        FieldType::String => {
            write_string_option_as_non_nullable(var_ident, &eff_flex, effective_versions, &buf_expr)
        },
        FieldType::Bytes => {
            write_bytes_option_as_non_nullable(var_ident, &eff_flex, effective_versions, &buf_expr)
        },
        FieldType::Array(inner) => {
            let flexible = !eff_flex.is_none();
            let needs_flex = needs_flexible_branching(&field.field_type);
            if needs_flex && flexible {
                if version_check_always_true(&eff_flex, effective_versions) {
                    generate_write_array_from_option(inner, var_ident, true, &buf_expr)
                } else if version_check_never_true(&eff_flex, effective_versions) {
                    generate_write_array_from_option(inner, var_ident, false, &buf_expr)
                } else {
                    let flex_check =
                        flexible_version_check_with_context(&eff_flex, effective_versions);
                    let compact_write =
                        generate_write_array_from_option(inner, var_ident, true, &buf_expr);
                    let standard_write =
                        generate_write_array_from_option(inner, var_ident, false, &buf_expr);
                    quote! {
                        if #flex_check {
                            #compact_write
                        } else {
                            #standard_write
                        }
                    }
                }
            } else {
                generate_write_array_from_option(inner, var_ident, false, &buf_expr)
            }
        },
        FieldType::Struct(_) => {
            let non_nullable_write =
                write_expr_for_type(&field.field_type, var_ident, false, false);
            quote! {
                match &self.#var_ident {
                    Some(v) => { v.write(#buf_expr, version)?; }
                    None => {
                        #non_nullable_write
                    }
                }
            }
        },
        _ => write_expr_for_type(&field.field_type, var_ident, false, false),
    }
}

fn write_string_option_as_non_nullable(
    var_ident: &Ident,
    eff_flex: &VersionRange,
    effective_versions: &VersionRange,
    buf_expr: &TokenStream,
) -> TokenStream {
    // `FieldType::String` always needs flex branching, so we gate only on `eff_flex`.
    if eff_flex.is_none() {
        quote! {
            {
                let _nn_default = KafkaString::default();
                let _nn_val = self.#var_ident.as_ref().unwrap_or(&_nn_default);
                write_string(#buf_expr, _nn_val)?;
            }
        }
    } else if version_check_always_true(eff_flex, effective_versions) {
        quote! {
            {
                let _nn_default = KafkaString::default();
                let _nn_val = self.#var_ident.as_ref().unwrap_or(&_nn_default);
                write_compact_string(#buf_expr, _nn_val)?;
            }
        }
    } else if version_check_never_true(eff_flex, effective_versions) {
        quote! {
            {
                let _nn_default = KafkaString::default();
                let _nn_val = self.#var_ident.as_ref().unwrap_or(&_nn_default);
                write_string(#buf_expr, _nn_val)?;
            }
        }
    } else {
        let flex_check = flexible_version_check_with_context(eff_flex, effective_versions);
        quote! {
            {
                let _nn_default = KafkaString::default();
                let _nn_val = self.#var_ident.as_ref().unwrap_or(&_nn_default);
                if #flex_check {
                    write_compact_string(#buf_expr, _nn_val)?;
                } else {
                    write_string(#buf_expr, _nn_val)?;
                }
            }
        }
    }
}

fn write_bytes_option_as_non_nullable(
    var_ident: &Ident,
    eff_flex: &VersionRange,
    effective_versions: &VersionRange,
    buf_expr: &TokenStream,
) -> TokenStream {
    // `FieldType::Bytes` always needs flex branching, so we gate only on `eff_flex`.
    if eff_flex.is_none() {
        quote! {
            {
                let _nn_default = Bytes::new();
                let _nn_val = self.#var_ident.as_ref().unwrap_or(&_nn_default);
                write_bytes(#buf_expr, _nn_val)?;
            }
        }
    } else if version_check_always_true(eff_flex, effective_versions) {
        quote! {
            {
                let _nn_default = Bytes::new();
                let _nn_val = self.#var_ident.as_ref().unwrap_or(&_nn_default);
                write_compact_bytes(#buf_expr, _nn_val)?;
            }
        }
    } else if version_check_never_true(eff_flex, effective_versions) {
        quote! {
            {
                let _nn_default = Bytes::new();
                let _nn_val = self.#var_ident.as_ref().unwrap_or(&_nn_default);
                write_bytes(#buf_expr, _nn_val)?;
            }
        }
    } else {
        let flex_check = flexible_version_check_with_context(eff_flex, effective_versions);
        quote! {
            {
                let _nn_default = Bytes::new();
                let _nn_val = self.#var_ident.as_ref().unwrap_or(&_nn_default);
                if #flex_check {
                    write_compact_bytes(#buf_expr, _nn_val)?;
                } else {
                    write_bytes(#buf_expr, _nn_val)?;
                }
            }
        }
    }
}

fn generate_write_array_from_option(
    inner: &FieldType,
    var_ident: &Ident,
    flexible: bool,
    buf_expr: &TokenStream,
) -> TokenStream {
    let len_write = if flexible {
        quote! { write_compact_array_length }
    } else {
        quote! { write_array_length }
    };

    let element_ident = format_ident!("el");
    let element_expr = quote! { #element_ident };
    let inner_write = array_element_write(inner, &element_expr, flexible, buf_expr, 1);

    quote! {
        match &self.#var_ident {
            Some(arr) => {
                #len_write(#buf_expr, arr.len() as i32);
                for #element_ident in arr {
                    #inner_write
                }
            }
            None => {
                #len_write(#buf_expr, 0);
            }
        }
    }
}

/// Generate the write expression for a tagged field (always compact encoding,
/// writes to `tag_buf`).
pub(crate) fn generate_write_tagged_field_expr(
    field: &FieldSpec,
    var_ident: &Ident,
) -> TokenStream {
    let is_nullable = is_field_nullable(field);
    let buf_expr = quote! { &mut tag_buf };
    write_expr_for_type_with_buf(&field.field_type, var_ident, is_nullable, true, &buf_expr)
}

fn write_expr_for_type(
    ft: &FieldType,
    var_ident: &Ident,
    nullable: bool,
    flexible: bool,
) -> TokenStream {
    let buf = quote! { buf };
    write_expr_for_type_with_buf(ft, var_ident, nullable, flexible, &buf)
}

fn write_expr_for_type_with_buf(
    ft: &FieldType,
    var_ident: &Ident,
    nullable: bool,
    flexible: bool,
    buf_expr: &TokenStream,
) -> TokenStream {
    match ft {
        FieldType::Bool => quote! { write_bool(#buf_expr, self.#var_ident); },
        FieldType::Int8 => quote! { write_i8(#buf_expr, self.#var_ident); },
        FieldType::Int16 => quote! { write_i16(#buf_expr, self.#var_ident); },
        FieldType::Int32 => quote! { write_i32(#buf_expr, self.#var_ident); },
        FieldType::Int64 => quote! { write_i64(#buf_expr, self.#var_ident); },
        FieldType::Uint16 => quote! { write_u16(#buf_expr, self.#var_ident); },
        FieldType::Float64 => quote! { write_f64(#buf_expr, self.#var_ident); },
        FieldType::Uuid => quote! { write_uuid(#buf_expr, &self.#var_ident); },
        FieldType::String => match (nullable, flexible) {
            (true, true) => {
                quote! { write_compact_nullable_string(#buf_expr, self.#var_ident.as_ref())?; }
            },
            (true, false) => {
                quote! { write_nullable_string(#buf_expr, self.#var_ident.as_ref())?; }
            },
            (false, true) => {
                quote! { write_compact_string(#buf_expr, &self.#var_ident)?; }
            },
            (false, false) => quote! { write_string(#buf_expr, &self.#var_ident)?; },
        },
        FieldType::Bytes => match (nullable, flexible) {
            (true, true) => quote! {
                write_compact_nullable_bytes(#buf_expr, self.#var_ident.as_ref().map(|b| b.as_ref()))?;
            },
            (true, false) => quote! {
                write_nullable_bytes(#buf_expr, self.#var_ident.as_ref().map(|b| b.as_ref()))?;
            },
            (false, true) => {
                quote! { write_compact_bytes(#buf_expr, &self.#var_ident)?; }
            },
            (false, false) => quote! { write_bytes(#buf_expr, &self.#var_ident)?; },
        },
        FieldType::Records => {
            if flexible {
                quote! {
                    write_compact_nullable_bytes(#buf_expr, self.#var_ident.as_ref().map(|b| b.as_ref()))?;
                }
            } else {
                quote! {
                    write_nullable_bytes(#buf_expr, self.#var_ident.as_ref().map(|b| b.as_ref()))?;
                }
            }
        },
        FieldType::Struct(_) => {
            if nullable {
                quote! {
                    match &self.#var_ident {
                        None => {
                            write_i8(#buf_expr, -1);
                        }
                        Some(v) => {
                            write_i8(#buf_expr, 1);
                            v.write(#buf_expr, version)?;
                        }
                    }
                }
            } else {
                quote! { self.#var_ident.write(#buf_expr, version)?; }
            }
        },
        FieldType::Array(inner) => {
            generate_write_array_expr(inner, var_ident, nullable, flexible, buf_expr)
        },
    }
}

fn generate_write_array_expr(
    inner: &FieldType,
    var_ident: &Ident,
    nullable: bool,
    flexible: bool,
    buf_expr: &TokenStream,
) -> TokenStream {
    let len_write = if flexible {
        quote! { write_compact_array_length }
    } else {
        quote! { write_array_length }
    };

    let element_ident = format_ident!("el");
    let element_expr = quote! { #element_ident };
    let inner_write = array_element_write(inner, &element_expr, flexible, buf_expr, 1);

    if nullable {
        quote! {
            match &self.#var_ident {
                None => { #len_write(#buf_expr, -1); }
                Some(arr) => {
                    #len_write(#buf_expr, arr.len() as i32);
                    for #element_ident in arr {
                        #inner_write
                    }
                }
            }
        }
    } else {
        quote! {
            #len_write(#buf_expr, self.#var_ident.len() as i32);
            for #element_ident in &self.#var_ident {
                #inner_write
            }
        }
    }
}

fn array_element_write(
    inner: &FieldType,
    element_expr: &TokenStream,
    flexible: bool,
    buf_expr: &TokenStream,
    depth: usize,
) -> TokenStream {
    match inner {
        FieldType::Struct(_) => quote! { #element_expr.write(#buf_expr, version)?; },
        FieldType::Bool => quote! { write_bool(#buf_expr, *#element_expr); },
        FieldType::Int8 => quote! { write_i8(#buf_expr, *#element_expr); },
        FieldType::Int16 => quote! { write_i16(#buf_expr, *#element_expr); },
        FieldType::Int32 => quote! { write_i32(#buf_expr, *#element_expr); },
        FieldType::Int64 => quote! { write_i64(#buf_expr, *#element_expr); },
        FieldType::Uint16 => quote! { write_u16(#buf_expr, *#element_expr); },
        FieldType::Float64 => quote! { write_f64(#buf_expr, *#element_expr); },
        FieldType::Uuid => quote! { write_uuid(#buf_expr, #element_expr); },
        FieldType::String => {
            if flexible {
                quote! { write_compact_string(#buf_expr, #element_expr)?; }
            } else {
                quote! { write_string(#buf_expr, #element_expr)?; }
            }
        },
        FieldType::Bytes => {
            if flexible {
                quote! { write_compact_bytes(#buf_expr, #element_expr)?; }
            } else {
                quote! { write_bytes(#buf_expr, #element_expr)?; }
            }
        },
        FieldType::Records => {
            if flexible {
                quote! { write_compact_nullable_bytes(#buf_expr, #element_expr.as_ref().map(|b| b.as_ref()))?; }
            } else {
                quote! { write_nullable_bytes(#buf_expr, #element_expr.as_ref().map(|b| b.as_ref()))?; }
            }
        },
        FieldType::Array(nested) => {
            let len_write = if flexible {
                quote! { write_compact_array_length }
            } else {
                quote! { write_array_length }
            };
            let nested_ident = format_ident!("el_{depth}");
            let nested_expr = quote! { #nested_ident };
            let nested_write = array_element_write(
                nested,
                &nested_expr,
                flexible,
                buf_expr,
                depth.saturating_add(1),
            );
            quote! {
                #len_write(#buf_expr, #element_expr.len() as i32);
                for #nested_ident in #element_expr {
                    #nested_write
                }
            }
        },
    }
}
