//! Field-type queries and Rust-type lowering.

use proc_macro2::{Ident, Span, TokenStream};
use quote::quote;

use crate::ir::field::{FieldSpec, FieldType};

/// Map a [`FieldSpec`] to its Rust type [`TokenStream`], applying the
/// `Option<T>` wrapper when the field is nullable.
pub(crate) fn map_field_type(field: &FieldSpec) -> TokenStream {
    let base = map_field_type_inner(&field.field_type);
    if is_field_nullable(field) {
        wrap_nullable(&field.field_type, base)
    } else {
        base
    }
}

/// True when the field's nullable-version range overlaps its presence range.
pub(crate) const fn is_field_nullable(field: &FieldSpec) -> bool {
    if field.nullable_versions.is_none() {
        return false;
    }
    field.nullable_versions.intersects(&field.versions)
}

/// True when the field is nullable in some — but not all — of its versions.
///
/// Forces version-conditional read/write branching: nullable encoding inside the
/// nullable range, non-nullable encoding outside.
pub(crate) fn has_version_conditional_nullability(field: &FieldSpec) -> bool {
    if !is_field_nullable(field) {
        return false;
    }
    if matches!(field.field_type, FieldType::Records) {
        return false;
    }
    !field.nullable_versions.covers(&field.versions)
}

/// Map a [`FieldType`] to its base Rust type [`TokenStream`] (without nullable wrapping).
pub(crate) fn map_field_type_inner(ft: &FieldType) -> TokenStream {
    match ft {
        FieldType::Bool => quote! { bool },
        FieldType::Int8 => quote! { i8 },
        FieldType::Int16 => quote! { i16 },
        FieldType::Int32 => quote! { i32 },
        FieldType::Int64 => quote! { i64 },
        FieldType::Uint16 => quote! { u16 },
        FieldType::Float64 => quote! { f64 },
        FieldType::String => quote! { KafkaString },
        FieldType::Bytes => quote! { Bytes },
        FieldType::Uuid => quote! { KafkaUuid },
        FieldType::Records => quote! { Option<Bytes> },
        FieldType::Struct(name) => {
            let id = Ident::new(name, Span::call_site());
            quote! { #id }
        },
        FieldType::Array(inner) => {
            let inner_type = map_field_type_inner(inner);
            quote! { Vec<#inner_type> }
        },
    }
}

/// Wrap `base` in `Option<T>` (or `Option<Box<T>>` for structs); records keep
/// their built-in nullability.
pub(crate) fn wrap_nullable(ft: &FieldType, base: TokenStream) -> TokenStream {
    match ft {
        FieldType::Records => base,
        FieldType::Struct(_) => quote! { Option<Box<#base>> },
        _ => quote! { Option<#base> },
    }
}

/// True when the type uses different wire encodings in flexible vs non-flexible
/// versions (length prefixes change for strings, bytes, arrays, records).
pub(crate) const fn needs_flexible_branching(ft: &FieldType) -> bool {
    matches!(
        ft,
        FieldType::String | FieldType::Bytes | FieldType::Records | FieldType::Array(_)
    )
}

/// True when generated read/write code for `ft` references the `version` parameter
/// (struct types do; primitives don't).
pub(crate) fn field_type_uses_version(ft: &FieldType) -> bool {
    match ft {
        FieldType::Struct(_) => true,
        FieldType::Array(inner) => field_type_uses_version(inner),
        _ => false,
    }
}

/// True when the field is wire-tagged at runtime (has a tag id and a non-empty
/// tagged-version range).
pub(crate) const fn is_tagged_at_runtime(field: &FieldSpec) -> bool {
    field.tag.is_some() && !field.tagged_versions.is_none()
}
