//! Stage 2: lower the parsed schema IR into a Rust [`proc_macro2::TokenStream`]
//! ready to be formatted by [`crate::format`].

mod api_key;
mod default_impl;
mod error;
mod ident;
mod io;
mod read_expr;
mod struct_def;
mod test_instance;
mod test_utils;
mod ty;
mod version_check;
mod write_expr;

use api_key::generate_api_key;
use default_impl::generate_default_impl;
pub use error::{CodegenError, CodegenErrorKind};
use heck::ToSnakeCase;
use io::generate_read_write_impl;
use proc_macro2::{Ident, TokenStream};
use quote::{format_ident, quote};
use struct_def::{StructDef, collect_nested_structs, generate_struct};
pub use test_utils::{generate_test_utils_file, generate_test_utils_mod_rs};

use crate::ir::message::MessageSpec;

/// Build the per-spec [`TokenStream`] for one Kafka message file.
///
/// Emits the top-level `{Name}Data` struct, every nested inline struct, every
/// `commonStruct` definition, plus their `Default`/`read`/`write` impls.
pub fn generate_file(
    spec: &MessageSpec,
    _all_specs: &[MessageSpec],
) -> Result<TokenStream, CodegenError> {
    let mut structs: Vec<StructDef<'_>> = Vec::new();

    let top_name = format!("{}Data", spec.name);
    let top_level_info = spec.api_key.map(|k| (k, spec.valid_versions.clone()));
    structs.push(StructDef {
        name: top_name,
        about: String::new(),
        fields: &spec.fields,
        top_level: top_level_info,
        is_data_struct: true,
        flexible_versions: spec.flexible_versions.clone(),
        effective_versions: spec.valid_versions.clone(),
    });

    collect_nested_structs(
        &spec.fields,
        &spec.flexible_versions,
        &spec.valid_versions,
        &mut structs,
    );

    for cs in &spec.common_structs {
        structs.push(StructDef {
            name: cs.name.clone(),
            about: String::new(),
            fields: &cs.fields,
            top_level: None,
            is_data_struct: false,
            flexible_versions: spec.flexible_versions.clone(),
            effective_versions: spec.valid_versions.clone(),
        });
        collect_nested_structs(
            &cs.fields,
            &spec.flexible_versions,
            &spec.valid_versions,
            &mut structs,
        );
    }

    let mut struct_tokens: Vec<TokenStream> = Vec::with_capacity(structs.len().saturating_mul(3));
    for s in &structs {
        let st = generate_struct(s);
        let def =
            generate_default_impl(s).map_err(|kind| CodegenError::new(spec.name.clone(), kind))?;
        let rw = generate_read_write_impl(s)
            .map_err(|kind| CodegenError::new(spec.name.clone(), kind))?;
        struct_tokens.extend([st, def, rw]);
    }

    let header = format!(" Generated from {}.json - DO NOT EDIT", spec.name);

    Ok(quote! {
        #![doc = #header]
        #![allow(
            missing_docs,
            clippy::all,
            clippy::pedantic,
            clippy::nursery,
            reason = "Generated protocol modules mirror Kafka's schema shape and intentionally trade hand-written lint style for reproducible wire-code output."
        )]

        use bytes::{Bytes, BytesMut};
        use crate::*;

        #(#struct_tokens)*
    })
}

/// Build the protocol `mod.rs` [`TokenStream`].
///
/// `modules` controls which per-spec modules are declared (typically the full
/// spec set, or a filtered subset for partial regeneration). `all_specs` is
/// always passed in full because the `ApiKey` enum must include every request,
/// even when only a subset of files are emitted.
pub fn generate_mod_rs(
    modules: &[&MessageSpec],
    all_specs: &[MessageSpec],
    source_ref: &str,
) -> TokenStream {
    let mut module_names: Vec<String> = modules.iter().map(|s| s.name.to_snake_case()).collect();
    module_names.sort();
    let mod_idents: Vec<Ident> = module_names
        .iter()
        .map(|m| format_ident!("{}", m))
        .collect();

    let api_key_tokens = generate_api_key(all_specs);

    quote! {
        #![doc = " Generated Kafka protocol message types — DO NOT EDIT"]
        #![allow(
            missing_docs,
            unused_imports,
            ambiguous_glob_reexports,
            clippy::all,
            clippy::pedantic,
            clippy::nursery,
            reason = "Generated protocol modules mirror Kafka's schema shape and intentionally trade hand-written lint style for reproducible wire-code output."
        )]

        pub const KAFKA_PROTOCOL_SOURCE_REF: &str = #source_ref;

        pub mod error_code;
        pub use error_code::ErrorCode;

        #(pub mod #mod_idents;)*
        #(pub use #mod_idents::*;)*

        #api_key_tokens
    }
}

#[cfg(test)]
mod tests {
    use super::{CodegenErrorKind, generate_file};
    use crate::ir::{
        field::{FieldSpec, FieldType},
        message::{MessageSpec, MessageType},
        version_range::VersionRange,
    };

    #[test]
    fn generated_writes_propagate_fallible_helpers_and_use_version_error_facade() {
        let spec = MessageSpec {
            name: "ExampleRequest".to_owned(),
            api_key: Some(99),
            message_type: MessageType::Request,
            valid_versions: VersionRange::Range(0, 1),
            flexible_versions: VersionRange::From(1),
            fields: vec![FieldSpec {
                name: "ClientName".to_owned(),
                field_type: FieldType::String,
                versions: VersionRange::Range(0, 1),
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
            }],
            common_structs: Vec::new(),
            listeners: Vec::new(),
            latest_version_unstable: false,
        };

        let generated = generate_file(&spec, &[])
            .expect("example schema should generate")
            .to_string();

        assert!(
            generated.contains("return Err (UnsupportedVersion :: new (99 , version) . into ()) ;"),
            "version guard should construct the runtime version error facade: {generated}"
        );
        assert!(
            generated.contains("write_compact_string (buf , & self . client_name) ? ;"),
            "fallible compact string writes must propagate errors: {generated}"
        );
        assert!(
            generated.contains("write_tagged_fields (buf , & all_tags) ? ;"),
            "fallible tagged-field writes must propagate errors: {generated}"
        );
    }

    #[test]
    fn generated_writes_support_nested_array_elements() {
        let spec = MessageSpec {
            name: "ExampleRequest".to_owned(),
            api_key: Some(99),
            message_type: MessageType::Request,
            valid_versions: VersionRange::Range(0, 0),
            flexible_versions: VersionRange::None,
            fields: vec![FieldSpec {
                name: "Matrix".to_owned(),
                field_type: FieldType::Array(Box::new(FieldType::Array(Box::new(
                    FieldType::Int32,
                )))),
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
            }],
            common_structs: Vec::new(),
            listeners: Vec::new(),
            latest_version_unstable: false,
        };

        let generated = generate_file(&spec, &[])
            .expect("example schema should generate")
            .to_string();

        assert!(
            !generated.contains("unsupported array element write"),
            "nested array writes must emit real element writers: {generated}"
        );
        assert!(
            generated.contains("for el in & self . matrix")
                && generated.contains("for el_1 in el")
                && generated.contains("write_i32 (buf , * el_1) ;"),
            "nested array writes should recurse through inner elements: {generated}"
        );
    }

    #[test]
    fn generate_file_reports_invalid_numeric_defaults() {
        let spec = MessageSpec {
            name: "ExampleRequest".to_owned(),
            api_key: Some(99),
            message_type: MessageType::Request,
            valid_versions: VersionRange::Range(0, 0),
            flexible_versions: VersionRange::None,
            fields: vec![FieldSpec {
                name: "Tiny".to_owned(),
                field_type: FieldType::Int8,
                versions: VersionRange::Range(0, 0),
                nullable_versions: VersionRange::None,
                tagged_versions: VersionRange::None,
                tag: None,
                about: String::new(),
                default: Some("128".to_owned()),
                ignorable: false,
                map_key: false,
                entity_type: None,
                zero_copy: false,
                flexible_versions: VersionRange::None,
                has_flexible_versions_override: false,
                fields: Vec::new(),
            }],
            common_structs: Vec::new(),
            listeners: Vec::new(),
            latest_version_unstable: false,
        };

        let error = generate_file(&spec, &[]).expect_err("invalid int8 default must fail codegen");

        assert_eq!(error.schema, "ExampleRequest");
        match error.kind {
            CodegenErrorKind::InvalidDefaultValue {
                field,
                value,
                field_type,
            } => {
                assert_eq!(field, "Tiny");
                assert_eq!(value, "128");
                assert_eq!(field_type, "Int8");
            },
            other => panic!("expected invalid default error, got {other:?}"),
        }
    }
}
