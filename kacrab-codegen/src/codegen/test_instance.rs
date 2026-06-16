//! Generation of `impl TestInstance for {Type}` blocks for round-trip testing.

use proc_macro2::{Ident, Span, TokenStream};
use quote::quote;

use super::{
    default_impl::resolve_default, error::CodegenErrorKind, ident::safe_rust_ident,
    struct_def::StructDef, ty::is_field_nullable,
};
use crate::ir::{
    field::{FieldSpec, FieldType},
    version_range::VersionRange,
};

/// Generate `impl TestInstance for {Name}` with release-grade oracle fixtures.
pub(crate) fn generate_test_instance_impl(
    def: &StructDef<'_>,
) -> Result<TokenStream, CodegenErrorKind> {
    let name = Ident::new(&def.name, Span::call_site());

    let populated_fields = fields_for_fixture(def, FixtureKind::Populated)?;
    let null_optionals_fields = fields_for_fixture(def, FixtureKind::NullOptionals)?;
    let empty_collections_fields = fields_for_fixture(def, FixtureKind::EmptyCollections)?;
    let multi_element_collections_fields =
        fields_for_fixture(def, FixtureKind::MultiElementCollections)?;
    let numeric_boundaries_fields = fields_for_fixture(def, FixtureKind::NumericBoundaries)?;
    let tagged_fields = fields_for_fixture(def, FixtureKind::TaggedFields)?;

    let has_flex = !def.flexible_versions.is_none();
    let populated_tagged_field_value = tagged_field_value(has_flex);
    let tagged_fields_tagged_field_value = tagged_field_value(has_flex);
    let empty_tagged_field_value = quote! { _unknown_tagged_fields: Vec::new(), };

    let default_exercise = default_exercise(def);
    let nullable_exercises = nullable_exercises(def);

    Ok(quote! {
        impl TestInstance for #name {
            fn test_populated() -> Self {
                Self {
                    #(#populated_fields)*
                    #populated_tagged_field_value
                }
            }
            fn test_null_optionals() -> Self {
                #default_exercise
                #(#nullable_exercises)*
                Self {
                    #(#null_optionals_fields)*
                    _unknown_tagged_fields: Vec::new(),
                }
            }
            fn test_empty_collections() -> Self {
                Self {
                    #(#empty_collections_fields)*
                    #empty_tagged_field_value
                }
            }
            fn test_multi_element_collections() -> Self {
                Self {
                    #(#multi_element_collections_fields)*
                    #empty_tagged_field_value
                }
            }
            fn test_numeric_boundaries() -> Self {
                Self {
                    #(#numeric_boundaries_fields)*
                    #empty_tagged_field_value
                }
            }
            fn test_tagged_fields() -> Self {
                Self {
                    #(#tagged_fields)*
                    #tagged_fields_tagged_field_value
                }
            }
        }
    })
}

#[derive(Clone, Copy)]
enum FixtureKind {
    Populated,
    NullOptionals,
    EmptyCollections,
    MultiElementCollections,
    NumericBoundaries,
    TaggedFields,
}

fn fields_for_fixture(
    def: &StructDef<'_>,
    kind: FixtureKind,
) -> Result<Vec<TokenStream>, CodegenErrorKind> {
    def.fields
        .iter()
        .map(|field| {
            let rust_name = safe_rust_ident(&field.name);
            let rust_name_val = fixture_value(field, &def.effective_versions, kind)?;
            Ok(quote! { #rust_name: #rust_name_val, })
        })
        .collect()
}

fn fixture_value(
    field: &FieldSpec,
    effective_versions: &VersionRange,
    kind: FixtureKind,
) -> Result<TokenStream, CodegenErrorKind> {
    match kind {
        FixtureKind::Populated => Ok(populated_value(field)),
        FixtureKind::NullOptionals => null_optionals_value(field, effective_versions),
        FixtureKind::EmptyCollections => Ok(empty_collections_value(field)),
        FixtureKind::MultiElementCollections => Ok(multi_element_collections_value(field)),
        FixtureKind::NumericBoundaries => Ok(numeric_boundaries_value(field)),
        FixtureKind::TaggedFields => Ok(tagged_fields_value(field)),
    }
}

fn tagged_field_value(has_flex: bool) -> TokenStream {
    if has_flex {
        quote! {
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    } else {
        quote! { _unknown_tagged_fields: Vec::new(), }
    }
}

fn default_exercise(def: &StructDef<'_>) -> TokenStream {
    if def.is_data_struct {
        quote! {}
    } else {
        quote! { drop(Self::default()); }
    }
}

fn nullable_exercises(def: &StructDef<'_>) -> Vec<TokenStream> {
    def.fields
        .iter()
        .filter_map(|field| {
            let needs_exercise = is_field_nullable(field) || field.tag.is_some();
            if !needs_exercise {
                return None;
            }
            match &field.field_type {
                FieldType::Struct(name) => {
                    let id = Ident::new(name, Span::call_site());
                    Some(quote! { drop(<#id as TestInstance>::test_null_optionals()); })
                },
                FieldType::Array(inner) => {
                    if let FieldType::Struct(name) = inner.as_ref() {
                        let id = Ident::new(name, Span::call_site());
                        Some(quote! { drop(<#id as TestInstance>::test_null_optionals()); })
                    } else {
                        None
                    }
                },
                _ => None,
            }
        })
        .collect()
}

fn null_optionals_value(
    field: &FieldSpec,
    effective_versions: &VersionRange,
) -> Result<TokenStream, CodegenErrorKind> {
    if field.nullable_versions.intersects(effective_versions) {
        return Ok(quote! { None });
    }
    if field.tag.is_some() {
        return resolve_default(field);
    }
    let value = null_optionals_value_for_type(&field.field_type);
    if is_field_nullable(field) {
        return Ok(wrap_non_null_nullable_value(&field.field_type, value));
    }
    Ok(value)
}

fn wrap_non_null_nullable_value(ft: &FieldType, value: TokenStream) -> TokenStream {
    match ft {
        FieldType::Records => value,
        FieldType::Struct(_) => quote! { Some(Box::new(#value)) },
        _ => quote! { Some(#value) },
    }
}

fn null_optionals_value_for_type(ft: &FieldType) -> TokenStream {
    match ft {
        FieldType::Bool => quote! { false },
        FieldType::Int8 => quote! { 0_i8 },
        FieldType::Int16 => quote! { 0_i16 },
        FieldType::Int32 => quote! { 0_i32 },
        FieldType::Int64 => quote! { 0_i64 },
        FieldType::Uint16 => quote! { 0_u16 },
        FieldType::Float64 => quote! { 0.0_f64 },
        FieldType::String => quote! { KafkaString::default() },
        FieldType::Bytes => quote! { Bytes::new() },
        FieldType::Uuid => quote! { KafkaUuid::ZERO },
        FieldType::Records => quote! { Some(Bytes::new()) },
        FieldType::Struct(name) => {
            let id = Ident::new(name, Span::call_site());
            quote! { <#id as TestInstance>::test_null_optionals() }
        },
        FieldType::Array(inner) => {
            let inner_val = null_optionals_array_element(inner);
            quote! { vec![#inner_val] }
        },
    }
}

fn null_optionals_array_element(ft: &FieldType) -> TokenStream {
    match ft {
        FieldType::Struct(name) => {
            let id = Ident::new(name, Span::call_site());
            quote! { <#id as TestInstance>::test_null_optionals() }
        },
        FieldType::Bool => quote! { false },
        FieldType::Int8 => quote! { 0_i8 },
        FieldType::Int16 => quote! { 0_i16 },
        FieldType::Int32 => quote! { 0_i32 },
        FieldType::Int64 => quote! { 0_i64 },
        FieldType::Uint16 => quote! { 0_u16 },
        FieldType::Float64 => quote! { 0.0_f64 },
        FieldType::String => quote! { KafkaString::default() },
        FieldType::Bytes => quote! { Bytes::new() },
        FieldType::Uuid => quote! { KafkaUuid::ZERO },
        FieldType::Records => quote! { Some(Bytes::new()) },
        FieldType::Array(inner) => {
            let inner_val = null_optionals_array_element(inner);
            quote! { vec![#inner_val] }
        },
    }
}

fn populated_value(field: &FieldSpec) -> TokenStream {
    let is_nullable = is_field_nullable(field);
    let base = populated_value_for_type(&field.field_type);

    if is_nullable {
        match &field.field_type {
            FieldType::Records => quote! { Some(Bytes::from_static(b"\x00")) },
            FieldType::Struct(name) => {
                let id = Ident::new(name, Span::call_site());
                quote! { Some(Box::new(<#id as TestInstance>::test_populated())) }
            },
            _ => quote! { Some(#base) },
        }
    } else {
        base
    }
}

fn empty_collections_value(field: &FieldSpec) -> TokenStream {
    let is_nullable = is_field_nullable(field);
    let base = empty_collections_value_for_type(&field.field_type);
    if is_nullable {
        wrap_non_null_nullable_value(&field.field_type, base)
    } else {
        base
    }
}

fn multi_element_collections_value(field: &FieldSpec) -> TokenStream {
    let is_nullable = is_field_nullable(field);
    let base = multi_element_collections_value_for_type(&field.field_type);
    if is_nullable {
        wrap_non_null_nullable_value(&field.field_type, base)
    } else {
        base
    }
}

fn numeric_boundaries_value(field: &FieldSpec) -> TokenStream {
    let is_nullable = is_field_nullable(field);
    let base = numeric_boundaries_value_for_type(&field.field_type);
    if is_nullable {
        match &field.field_type {
            FieldType::Records => base,
            FieldType::Struct(name) => {
                let id = Ident::new(name, Span::call_site());
                quote! { Some(Box::new(<#id as TestInstance>::test_numeric_boundaries())) }
            },
            _ => quote! { Some(#base) },
        }
    } else {
        base
    }
}

fn tagged_fields_value(field: &FieldSpec) -> TokenStream {
    let is_nullable = is_field_nullable(field);
    let base = tagged_fields_value_for_type(&field.field_type);
    if is_nullable {
        match &field.field_type {
            FieldType::Records => base,
            FieldType::Struct(name) => {
                let id = Ident::new(name, Span::call_site());
                quote! { Some(Box::new(<#id as TestInstance>::test_tagged_fields())) }
            },
            _ => quote! { Some(#base) },
        }
    } else {
        base
    }
}

fn populated_value_for_type(ft: &FieldType) -> TokenStream {
    match ft {
        FieldType::Bool => quote! { true },
        FieldType::Int8 => quote! { 7_i8 },
        FieldType::Int16 => quote! { 42_i16 },
        FieldType::Int32 => quote! { 12345_i32 },
        FieldType::Int64 => quote! { 9_876_543_210_i64 },
        FieldType::Uint16 => quote! { 42_u16 },
        FieldType::Float64 => quote! { 6.25_f64 },
        FieldType::String => quote! { KafkaString::from("test".to_owned()) },
        FieldType::Bytes => quote! { Bytes::from_static(b"\xca\xfe") },
        FieldType::Uuid => quote! { KafkaUuid::ONE },
        FieldType::Records => quote! { Some(Bytes::new()) },
        FieldType::Struct(name) => {
            let id = Ident::new(name, Span::call_site());
            quote! { <#id as TestInstance>::test_populated() }
        },
        FieldType::Array(inner) => {
            let inner_val = populated_value_for_type(inner);
            quote! { vec![#inner_val] }
        },
    }
}

fn empty_collections_value_for_type(ft: &FieldType) -> TokenStream {
    match ft {
        FieldType::Bytes => quote! { Bytes::new() },
        FieldType::Records => quote! { Some(Bytes::new()) },
        FieldType::Array(_) => quote! { Vec::new() },
        _ => null_optionals_value_for_type(ft),
    }
}

fn multi_element_collections_value_for_type(ft: &FieldType) -> TokenStream {
    match ft {
        FieldType::Bytes => quote! { Bytes::from_static(b"\x00\xff") },
        FieldType::Records => quote! { Some(Bytes::new()) },
        FieldType::Array(inner) => {
            let first = populated_value_for_type(inner);
            let second = multi_element_array_element(inner);
            quote! { vec![#first, #second] }
        },
        _ => multi_element_scalar_value_for_type(ft),
    }
}

fn multi_element_array_element(ft: &FieldType) -> TokenStream {
    match ft {
        FieldType::Struct(name) => {
            let id = Ident::new(name, Span::call_site());
            quote! { <#id as TestInstance>::test_multi_element_collections() }
        },
        FieldType::Array(inner) => {
            let first = populated_value_for_type(inner);
            let second = multi_element_array_element(inner);
            quote! { vec![#first, #second] }
        },
        FieldType::Bytes => quote! { Bytes::from_static(b"\x00\xff") },
        FieldType::Records => quote! { Some(Bytes::new()) },
        _ => multi_element_scalar_value_for_type(ft),
    }
}

fn multi_element_scalar_value_for_type(ft: &FieldType) -> TokenStream {
    match ft {
        FieldType::Bool => quote! { false },
        FieldType::Int8 => quote! { 8_i8 },
        FieldType::Int16 => quote! { 43_i16 },
        FieldType::Int32 => quote! { 23456_i32 },
        FieldType::Int64 => quote! { 9_876_543_211_i64 },
        FieldType::Uint16 => quote! { 43_u16 },
        FieldType::Float64 => quote! { 7.5_f64 },
        FieldType::String => quote! { KafkaString::from("test-2".to_owned()) },
        FieldType::Bytes => quote! { Bytes::from_static(b"\x00\xff") },
        FieldType::Uuid => quote! { KafkaUuid::from_parts(2, 3) },
        FieldType::Records => quote! { Some(Bytes::new()) },
        FieldType::Struct(name) => {
            let id = Ident::new(name, Span::call_site());
            quote! { <#id as TestInstance>::test_multi_element_collections() }
        },
        FieldType::Array(inner) => {
            let first = populated_value_for_type(inner);
            let second = multi_element_array_element(inner);
            quote! { vec![#first, #second] }
        },
    }
}

fn numeric_boundaries_value_for_type(ft: &FieldType) -> TokenStream {
    match ft {
        FieldType::Bool => quote! { true },
        FieldType::Int8 => quote! { i8::MIN },
        FieldType::Int16 => quote! { i16::MIN },
        FieldType::Int32 => quote! { i32::MIN },
        FieldType::Int64 => quote! { i64::MIN },
        FieldType::Uint16 => quote! { u16::MAX },
        FieldType::Float64 => quote! { f64::MIN },
        FieldType::String => quote! { KafkaString::from("boundary".to_owned()) },
        FieldType::Bytes => quote! { Bytes::from_static(b"\x00\xff") },
        FieldType::Uuid => quote! { KafkaUuid::ONE },
        FieldType::Records => quote! { Some(Bytes::new()) },
        FieldType::Struct(name) => {
            let id = Ident::new(name, Span::call_site());
            quote! { <#id as TestInstance>::test_numeric_boundaries() }
        },
        FieldType::Array(inner) => {
            let inner_val = numeric_boundaries_value_for_type(inner);
            quote! { vec![#inner_val] }
        },
    }
}

fn tagged_fields_value_for_type(ft: &FieldType) -> TokenStream {
    match ft {
        FieldType::Struct(name) => {
            let id = Ident::new(name, Span::call_site());
            quote! { <#id as TestInstance>::test_tagged_fields() }
        },
        FieldType::Array(inner) => {
            let inner_val = tagged_fields_array_element(inner);
            quote! { vec![#inner_val] }
        },
        _ => populated_value_for_type(ft),
    }
}

fn tagged_fields_array_element(ft: &FieldType) -> TokenStream {
    match ft {
        FieldType::Struct(name) => {
            let id = Ident::new(name, Span::call_site());
            quote! { <#id as TestInstance>::test_tagged_fields() }
        },
        FieldType::Array(inner) => {
            let inner_val = tagged_fields_array_element(inner);
            quote! { vec![#inner_val] }
        },
        _ => populated_value_for_type(ft),
    }
}
