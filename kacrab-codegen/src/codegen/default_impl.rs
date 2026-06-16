//! `Default` impl generation, including resolution of explicit-default-value
//! strings from the spec.

use proc_macro2::{Ident, Span, TokenStream};
use quote::quote;

use super::{
    error::CodegenErrorKind, ident::safe_rust_ident, struct_def::StructDef, ty::is_field_nullable,
};
use crate::ir::field::{FieldSpec, FieldType};

/// Generate the `impl Default for {Name}` block for a [`StructDef`].
pub(crate) fn generate_default_impl(def: &StructDef<'_>) -> Result<TokenStream, CodegenErrorKind> {
    let name = Ident::new(&def.name, Span::call_site());

    let field_defaults: Vec<TokenStream> = def
        .fields
        .iter()
        .map(|field| {
            let rust_name = safe_rust_ident(&field.name);
            let default_val = resolve_default(field)?;
            Ok(quote! { #rust_name: #default_val, })
        })
        .collect::<Result<_, CodegenErrorKind>>()?;

    Ok(quote! {
        impl Default for #name {
            fn default() -> Self {
                Self {
                    #(#field_defaults)*
                    _unknown_tagged_fields: Vec::new(),
                }
            }
        }
    })
}

/// Resolve the default-value expression for a field.
pub(crate) fn resolve_default(field: &FieldSpec) -> Result<TokenStream, CodegenErrorKind> {
    let is_nullable = is_field_nullable(field);
    if let Some(ref default_str) = field.default {
        return resolve_explicit_default(default_str, field, is_nullable);
    }
    Ok(natural_default(&field.field_type, is_nullable))
}

fn invalid_default(default_str: &str, field: &FieldSpec) -> CodegenErrorKind {
    CodegenErrorKind::InvalidDefaultValue {
        field: field.name.clone(),
        value: default_str.to_owned(),
        field_type: format!("{:?}", field.field_type),
    }
}

fn resolve_explicit_default(
    default_str: &str,
    field: &FieldSpec,
    is_nullable: bool,
) -> Result<TokenStream, CodegenErrorKind> {
    if default_str == "null" {
        return Ok(quote! { None });
    }
    if default_str == "true" {
        return Ok(if is_nullable {
            quote! { Some(true) }
        } else {
            quote! { true }
        });
    }
    if default_str == "false" {
        return Ok(if is_nullable {
            quote! { Some(false) }
        } else {
            quote! { false }
        });
    }
    if default_str.is_empty() {
        return Ok(if is_nullable {
            quote! { Some(KafkaString::default()) }
        } else {
            quote! { KafkaString::default() }
        });
    }
    if default_str.starts_with("0x") || default_str.starts_with("0X") {
        let inner = format_hex_default(default_str, field)?;
        return Ok(if is_nullable {
            quote! { Some(#inner) }
        } else {
            inner
        });
    }
    if default_str.parse::<i64>().is_ok() {
        let inner = format_numeric_default(default_str, field)?;
        return Ok(if is_nullable {
            quote! { Some(#inner) }
        } else {
            inner
        });
    }
    Ok(if is_nullable {
        quote! { Some(KafkaString::from(#default_str.to_string())) }
    } else {
        quote! { KafkaString::from(#default_str.to_string()) }
    })
}

fn format_hex_default(hex_str: &str, field: &FieldSpec) -> Result<TokenStream, CodegenErrorKind> {
    let ft = &field.field_type;
    let Some(value) = parse_hex_u64(hex_str) else {
        return Err(invalid_default(hex_str, field));
    };
    match ft {
        FieldType::Int8 => {
            let value = i8::try_from(value).map_err(|_error| invalid_default(hex_str, field))?;
            let literal = proc_macro2::Literal::i8_suffixed(value);
            Ok(quote! { #literal })
        },
        FieldType::Int16 => {
            let value = i16::try_from(value).map_err(|_error| invalid_default(hex_str, field))?;
            let literal = proc_macro2::Literal::i16_suffixed(value);
            Ok(quote! { #literal })
        },
        FieldType::Int64 => {
            let value = i64::try_from(value).map_err(|_error| invalid_default(hex_str, field))?;
            let literal = proc_macro2::Literal::i64_suffixed(value);
            Ok(quote! { #literal })
        },
        FieldType::Uint16 => {
            let value = u16::try_from(value).map_err(|_error| invalid_default(hex_str, field))?;
            let literal = proc_macro2::Literal::u16_suffixed(value);
            Ok(quote! { #literal })
        },
        FieldType::Int32 => match i32::try_from(value) {
            Ok(value) if value == i32::MAX => Ok(quote! { i32::MAX }),
            Ok(value) => {
                let literal = proc_macro2::Literal::i32_suffixed(value);
                Ok(quote! { #literal })
            },
            Err(_) => Err(invalid_default(hex_str, field)),
        },
        _ => Err(invalid_default(hex_str, field)),
    }
}

fn parse_hex_u64(hex_str: &str) -> Option<u64> {
    let digits = hex_str
        .strip_prefix("0x")
        .or_else(|| hex_str.strip_prefix("0X"))?;
    u64::from_str_radix(digits, 16).ok()
}

fn format_numeric_default(
    num_str: &str,
    field: &FieldSpec,
) -> Result<TokenStream, CodegenErrorKind> {
    let ft = &field.field_type;
    match ft {
        FieldType::Int8 => match num_str.parse::<i8>() {
            Ok(value) if value == i8::MIN => Ok(quote! { i8::MIN }),
            Ok(value) if value == i8::MAX => Ok(quote! { i8::MAX }),
            Ok(value) => {
                let literal = proc_macro2::Literal::i8_suffixed(value);
                Ok(quote! { #literal })
            },
            Err(_) => Err(invalid_default(num_str, field)),
        },
        FieldType::Int16 => match num_str.parse::<i16>() {
            Ok(value) if value == i16::MIN => Ok(quote! { i16::MIN }),
            Ok(value) if value == i16::MAX => Ok(quote! { i16::MAX }),
            Ok(value) => {
                let literal = proc_macro2::Literal::i16_suffixed(value);
                Ok(quote! { #literal })
            },
            Err(_) => Err(invalid_default(num_str, field)),
        },
        FieldType::Int32 => match num_str.parse::<i32>() {
            Ok(value) if value == i32::MIN => Ok(quote! { i32::MIN }),
            Ok(value) if value == i32::MAX => Ok(quote! { i32::MAX }),
            Ok(value) => {
                let literal = proc_macro2::Literal::i32_suffixed(value);
                Ok(quote! { #literal })
            },
            Err(_) => Err(invalid_default(num_str, field)),
        },
        FieldType::Int64 => match num_str.parse::<i64>() {
            Ok(value) if value == i64::MIN => Ok(quote! { i64::MIN }),
            Ok(value) if value == i64::MAX => Ok(quote! { i64::MAX }),
            Ok(value) => {
                let literal = proc_macro2::Literal::i64_suffixed(value);
                Ok(quote! { #literal })
            },
            Err(_) => Err(invalid_default(num_str, field)),
        },
        FieldType::Uint16 => match num_str.parse::<u16>() {
            Ok(value) if value == u16::MAX => Ok(quote! { u16::MAX }),
            Ok(value) => {
                let literal = proc_macro2::Literal::u16_suffixed(value);
                Ok(quote! { #literal })
            },
            Err(_) => Err(invalid_default(num_str, field)),
        },
        FieldType::Float64 => {
            let value = num_str
                .parse::<f64>()
                .map_err(|_error| invalid_default(num_str, field))?;
            let literal = proc_macro2::Literal::f64_suffixed(value);
            Ok(quote! { #literal })
        },
        _ => Err(invalid_default(num_str, field)),
    }
}

fn natural_default(ft: &FieldType, is_nullable: bool) -> TokenStream {
    if is_nullable {
        return quote! { None };
    }
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
        FieldType::Records => quote! { None },
        FieldType::Struct(name) => {
            let id = Ident::new(name, Span::call_site());
            quote! { #id::default() }
        },
        FieldType::Array(_) => quote! { Vec::new() },
    }
}

#[cfg(test)]
mod tests {
    use super::{format_hex_default, format_numeric_default, resolve_default};
    use crate::ir::{
        field::{FieldSpec, FieldType},
        version_range::VersionRange,
    };

    fn field(field_type: FieldType) -> FieldSpec {
        FieldSpec {
            name: "Example".to_owned(),
            field_type,
            versions: VersionRange::Range(0, 0),
            nullable_versions: VersionRange::None,
            tagged_versions: VersionRange::None,
            tag: None,
            about: String::new(),
            default: None,
            ignorable: false,
            map_key: false,
            entity_type: None,
            zero_copy: false,
            flexible_versions: VersionRange::None,
            has_flexible_versions_override: false,
            fields: Vec::new(),
        }
    }

    fn nullable_field(field_type: FieldType) -> FieldSpec {
        FieldSpec {
            nullable_versions: VersionRange::Range(0, 0),
            ..field(field_type)
        }
    }

    fn explicit_field(field_type: FieldType, default: &str) -> FieldSpec {
        FieldSpec {
            default: Some(default.to_owned()),
            ..field(field_type)
        }
    }

    #[test]
    fn numeric_defaults_use_readable_bound_constants() {
        assert_eq!(
            format_numeric_default("-2147483648", &field(FieldType::Int32))
                .unwrap()
                .to_string(),
            "i32 :: MIN"
        );
        assert_eq!(
            format_numeric_default("9223372036854775807", &field(FieldType::Int64))
                .unwrap()
                .to_string(),
            "i64 :: MAX"
        );
        assert_eq!(
            format_hex_default("0x7fffffff", &field(FieldType::Int32))
                .unwrap()
                .to_string(),
            "i32 :: MAX"
        );
    }

    #[test]
    fn numeric_defaults_fail_instead_of_emitting_empty_tokens() {
        assert!(format_numeric_default("128", &field(FieldType::Int8)).is_err());
        assert!(format_hex_default("0xff", &field(FieldType::Int8)).is_err());
    }

    #[test]
    fn explicit_defaults_cover_bool_null_string_hex_and_float_shapes() {
        assert_eq!(
            resolve_default(&explicit_field(FieldType::Bool, "true"))
                .unwrap()
                .to_string(),
            "true"
        );
        assert_eq!(
            resolve_default(&FieldSpec {
                default: Some("false".to_owned()),
                ..nullable_field(FieldType::Bool)
            })
            .unwrap()
            .to_string(),
            "Some (false)"
        );
        assert_eq!(
            resolve_default(&explicit_field(FieldType::String, ""))
                .unwrap()
                .to_string(),
            "KafkaString :: default ()"
        );
        assert_eq!(
            resolve_default(&FieldSpec {
                default: Some("hello".to_owned()),
                ..nullable_field(FieldType::String)
            })
            .unwrap()
            .to_string(),
            "Some (KafkaString :: from (\"hello\" . to_string ()))"
        );
        assert_eq!(
            resolve_default(&explicit_field(FieldType::Uint16, "0Xffff"))
                .unwrap()
                .to_string(),
            "65535u16"
        );
        assert_eq!(
            resolve_default(&explicit_field(FieldType::Float64, "1"))
                .unwrap()
                .to_string(),
            "1f64"
        );
        assert_eq!(
            resolve_default(&explicit_field(FieldType::Bytes, "null"))
                .unwrap()
                .to_string(),
            "None"
        );
    }

    #[test]
    fn natural_defaults_cover_non_numeric_types_and_nullable_wrapper() {
        assert_eq!(
            resolve_default(&field(FieldType::Bytes))
                .unwrap()
                .to_string(),
            "Bytes :: new ()"
        );
        assert_eq!(
            resolve_default(&field(FieldType::Uuid))
                .unwrap()
                .to_string(),
            "KafkaUuid :: ZERO"
        );
        assert_eq!(
            resolve_default(&field(FieldType::Records))
                .unwrap()
                .to_string(),
            "None"
        );
        assert_eq!(
            resolve_default(&field(FieldType::Struct("Thing".to_owned())))
                .unwrap()
                .to_string(),
            "Thing :: default ()"
        );
        assert_eq!(
            resolve_default(&field(FieldType::Array(Box::new(FieldType::Int32))))
                .unwrap()
                .to_string(),
            "Vec :: new ()"
        );
        assert_eq!(
            resolve_default(&nullable_field(FieldType::Int32))
                .unwrap()
                .to_string(),
            "None"
        );
    }
}
