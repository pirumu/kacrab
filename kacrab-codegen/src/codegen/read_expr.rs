//! Generation of read-side field expressions: per-field assignment, tagged
//! reads, and primitive/array decoders.

use proc_macro2::{Ident, Span, TokenStream};
use quote::quote;

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

/// Generate the read expression for a non-tagged field, assigning into `var_ident`.
pub(crate) fn generate_read_field_expr(
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
        let nullable_read = generate_read_field_expr_inner(
            field,
            var_ident,
            flex_versions,
            true,
            &nullable_effective,
        );
        let non_nullable_read = generate_read_non_nullable_to_option(
            field,
            var_ident,
            flex_versions,
            &non_nullable_effective,
        );
        return quote! {
            if #nullable_check {
                #nullable_read
            } else {
                #non_nullable_read
            }
        };
    }

    generate_read_field_expr_inner(
        field,
        var_ident,
        flex_versions,
        is_nullable,
        effective_versions,
    )
}

fn generate_read_field_expr_inner(
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
            let expr = read_expr_for_type(&field.field_type, nullable, false);
            quote! { #var_ident = #expr; }
        } else if version_check_always_true(&eff_flex, effective_versions) {
            let compact_expr = read_expr_for_type(&field.field_type, nullable, true);
            quote! { #var_ident = #compact_expr; }
        } else if version_check_never_true(&eff_flex, effective_versions) {
            let standard_expr = read_expr_for_type(&field.field_type, nullable, false);
            quote! { #var_ident = #standard_expr; }
        } else {
            let flex_check = flexible_version_check_with_context(&eff_flex, effective_versions);
            let compact_expr = read_expr_for_type(&field.field_type, nullable, true);
            let standard_expr = read_expr_for_type(&field.field_type, nullable, false);
            quote! {
                if #flex_check {
                    #var_ident = #compact_expr;
                } else {
                    #var_ident = #standard_expr;
                }
            }
        }
    } else {
        let expr = read_expr_for_type(&field.field_type, nullable, false);
        quote! { #var_ident = #expr; }
    }
}

/// Generate the read expression for a tagged field (always compact encoding,
/// reads from `tag_buf`).
pub(crate) fn generate_read_tagged_field_expr(field: &FieldSpec, var_ident: &Ident) -> TokenStream {
    let is_nullable = is_field_nullable(field);
    let buf_expr = quote! { &mut tag_buf };
    let expr = read_expr_for_type_with_buf(&field.field_type, is_nullable, true, &buf_expr);
    quote! { #var_ident = #expr; }
}

fn generate_read_non_nullable_to_option(
    field: &FieldSpec,
    var_ident: &Ident,
    flex_versions: &VersionRange,
    effective_versions: &VersionRange,
) -> TokenStream {
    let eff_flex = effective_flex_versions(field, flex_versions);
    let needs_flex = needs_flexible_branching(&field.field_type);

    match &field.field_type {
        FieldType::String | FieldType::Bytes if needs_flex && !eff_flex.is_none() => {
            if version_check_always_true(&eff_flex, effective_versions) {
                let compact_expr = read_expr_for_type(&field.field_type, false, true);
                quote! { #var_ident = Some(#compact_expr); }
            } else if version_check_never_true(&eff_flex, effective_versions) {
                let standard_expr = read_expr_for_type(&field.field_type, false, false);
                quote! { #var_ident = Some(#standard_expr); }
            } else {
                let flex_check = flexible_version_check_with_context(&eff_flex, effective_versions);
                let compact_expr = read_expr_for_type(&field.field_type, false, true);
                let standard_expr = read_expr_for_type(&field.field_type, false, false);
                quote! {
                    if #flex_check {
                        #var_ident = Some(#compact_expr);
                    } else {
                        #var_ident = Some(#standard_expr);
                    }
                }
            }
        },
        FieldType::Array(inner) if needs_flex && !eff_flex.is_none() => {
            let buf_expr = quote! { buf };
            if version_check_always_true(&eff_flex, effective_versions) {
                let compact_read = generate_read_array_expr(inner, false, true, &buf_expr);
                quote! { #var_ident = Some(#compact_read); }
            } else if version_check_never_true(&eff_flex, effective_versions) {
                let standard_read = generate_read_array_expr(inner, false, false, &buf_expr);
                quote! { #var_ident = Some(#standard_read); }
            } else {
                let flex_check = flexible_version_check_with_context(&eff_flex, effective_versions);
                let compact_read = generate_read_array_expr(inner, false, true, &buf_expr);
                let standard_read = generate_read_array_expr(inner, false, false, &buf_expr);
                quote! {
                    if #flex_check {
                        #var_ident = Some(#compact_read);
                    } else {
                        #var_ident = Some(#standard_read);
                    }
                }
            }
        },
        FieldType::Struct(name) => {
            let id = Ident::new(name, Span::call_site());
            quote! { #var_ident = Some(Box::new(#id::read(buf, version)?)); }
        },
        _ => {
            let expr = read_expr_for_type(&field.field_type, false, false);
            quote! { #var_ident = Some(#expr); }
        },
    }
}

fn read_expr_for_type(ft: &FieldType, nullable: bool, flexible: bool) -> TokenStream {
    let buf = quote! { buf };
    read_expr_for_type_with_buf(ft, nullable, flexible, &buf)
}

fn read_expr_for_type_with_buf(
    ft: &FieldType,
    nullable: bool,
    flexible: bool,
    buf_expr: &TokenStream,
) -> TokenStream {
    match ft {
        FieldType::Bool => quote! { read_bool(#buf_expr)? },
        FieldType::Int8 => quote! { read_i8(#buf_expr)? },
        FieldType::Int16 => quote! { read_i16(#buf_expr)? },
        FieldType::Int32 => quote! { read_i32(#buf_expr)? },
        FieldType::Int64 => quote! { read_i64(#buf_expr)? },
        FieldType::Uint16 => quote! { read_u16(#buf_expr)? },
        FieldType::Float64 => quote! { read_f64(#buf_expr)? },
        FieldType::Uuid => quote! { read_uuid(#buf_expr)? },
        FieldType::String => match (nullable, flexible) {
            (true, true) => quote! { read_compact_nullable_string(#buf_expr)? },
            (true, false) => quote! { read_nullable_string(#buf_expr)? },
            (false, true) => quote! { read_compact_string(#buf_expr)? },
            (false, false) => quote! { read_string(#buf_expr)? },
        },
        FieldType::Bytes => match (nullable, flexible) {
            (true, true) => quote! { read_compact_nullable_bytes(#buf_expr)? },
            (true, false) => quote! { read_nullable_bytes(#buf_expr)? },
            (false, true) => quote! { read_compact_bytes(#buf_expr)? },
            (false, false) => quote! { read_bytes(#buf_expr)? },
        },
        FieldType::Records => {
            if flexible {
                quote! { read_compact_nullable_bytes(#buf_expr)? }
            } else {
                quote! { read_nullable_bytes(#buf_expr)? }
            }
        },
        FieldType::Struct(name) => {
            let id = Ident::new(name, Span::call_site());
            if nullable {
                quote! {
                    {
                        let marker = read_i8(#buf_expr)?;
                        if marker < 0 {
                            None
                        } else {
                            Some(Box::new(#id::read(#buf_expr, version)?))
                        }
                    }
                }
            } else {
                quote! { #id::read(#buf_expr, version)? }
            }
        },
        FieldType::Array(inner) => generate_read_array_expr(inner, nullable, flexible, buf_expr),
    }
}

fn generate_read_array_expr(
    inner: &FieldType,
    nullable: bool,
    flexible: bool,
    buf_expr: &TokenStream,
) -> TokenStream {
    let len_read = if flexible {
        quote! { read_compact_array_length(#buf_expr)? }
    } else {
        quote! { read_array_length(#buf_expr)? }
    };

    let inner_read = match inner {
        FieldType::Struct(name) => {
            let id = Ident::new(name, Span::call_site());
            quote! { #id::read(#buf_expr, version)? }
        },
        _ => read_expr_for_type_with_buf(inner, false, flexible, buf_expr),
    };

    if nullable {
        quote! {
            {
                let len = #len_read;
                if len < 0 {
                    None
                } else {
                    let mut arr = Vec::with_capacity(len as usize);
                    for _ in 0..len {
                        arr.push(#inner_read);
                    }
                    Some(arr)
                }
            }
        }
    } else {
        quote! {
            {
                let len = #len_read;
                let mut arr = Vec::with_capacity(len.max(0) as usize);
                for _ in 0..len {
                    arr.push(#inner_read);
                }
                arr
            }
        }
    }
}
