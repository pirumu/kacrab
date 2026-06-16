//! Generate test-utils files: `impl TestInstance for ...` per spec, plus the
//! `mod.rs` that wires them together.

use heck::ToSnakeCase;
use proc_macro2::{Ident, TokenStream};
use quote::{format_ident, quote};

use super::{
    error::CodegenError,
    struct_def::{StructDef, collect_nested_structs},
    test_instance::generate_test_instance_impl,
};
use crate::ir::{
    message::{MessageSpec, MessageType},
    version_range::VersionRange,
};

/// Build the test-utils file [`TokenStream`] for one [`MessageSpec`].
pub fn generate_test_utils_file(spec: &MessageSpec) -> Result<TokenStream, CodegenError> {
    let structs = collect_test_structs(spec);
    let impl_tokens = generate_test_instance_impls(spec, &structs)?;
    let mod_name = format_ident!("{}", spec.name.to_snake_case());
    let top_name = format_ident!("{}Data", spec.name);
    let schema_name = spec.name.as_str();
    let java_class = java_message_class(spec);
    let versions = version_literals(&spec.valid_versions);
    let protocol_import = if needs_protocol_prelude(&structs) {
        quote! { use kacrab_protocol::*; }
    } else {
        TokenStream::new()
    };
    let bytes_import = bytes_import(&structs, !versions.is_empty());
    let populated_cases = populated_case_entries(&versions, schema_name, &java_class);
    let case_helpers = case_helpers(&versions, &top_name);
    let append_protocol_cases =
        append_protocol_cases(&versions, schema_name, &java_class, &populated_cases);

    Ok(quote! {
        #bytes_import
        #protocol_import
        use kacrab_protocol::generated::#mod_name::*;
        use crate::TestInstance;

        #(#impl_tokens)*

        #case_helpers

        #append_protocol_cases
    })
}

fn collect_test_structs(spec: &MessageSpec) -> Vec<StructDef<'_>> {
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
    structs
}

fn generate_test_instance_impls(
    spec: &MessageSpec,
    structs: &[StructDef<'_>],
) -> Result<Vec<TokenStream>, CodegenError> {
    structs
        .iter()
        .map(|s| {
            generate_test_instance_impl(s)
                .map_err(|kind| CodegenError::new(spec.name.clone(), kind))
        })
        .collect::<Result<_, CodegenError>>()
}

fn populated_case_entries(versions: &[i16], schema_name: &str, java_class: &str) -> TokenStream {
    quote! {
        #(
            crate::MatrixCase {
                schema_name: #schema_name,
                java_class: #java_class,
                version: #versions,
                fixture: "populated",
                rust_encode: encode_populated,
                rust_reencode: reencode,
            },
        )*
    }
}

fn append_protocol_cases(
    versions: &[i16],
    schema_name: &str,
    java_class: &str,
    populated_cases: &TokenStream,
) -> TokenStream {
    if versions.is_empty() {
        return TokenStream::new();
    }
    quote! {
        const MATRIX_CASES: &[crate::MatrixCase] = &[
            #(
                crate::MatrixCase {
                    schema_name: #schema_name,
                    java_class: #java_class,
                    version: #versions,
                    fixture: "null_optionals",
                    rust_encode: encode_null_optionals,
                    rust_reencode: reencode,
                },
            )*
            #populated_cases
            #(
                crate::MatrixCase {
                    schema_name: #schema_name,
                    java_class: #java_class,
                    version: #versions,
                    fixture: "empty_collections",
                    rust_encode: encode_empty_collections,
                    rust_reencode: reencode,
                },
                crate::MatrixCase {
                    schema_name: #schema_name,
                    java_class: #java_class,
                    version: #versions,
                    fixture: "multi_element_collections",
                    rust_encode: encode_multi_element_collections,
                    rust_reencode: reencode,
                },
                crate::MatrixCase {
                    schema_name: #schema_name,
                    java_class: #java_class,
                    version: #versions,
                    fixture: "numeric_boundaries",
                    rust_encode: encode_numeric_boundaries,
                    rust_reencode: reencode,
                },
                crate::MatrixCase {
                    schema_name: #schema_name,
                    java_class: #java_class,
                    version: #versions,
                    fixture: "tagged_fields",
                    rust_encode: encode_tagged_fields,
                    rust_reencode: reencode,
                },
            )*
        ];

        pub(crate) fn append_protocol_cases(cases: &mut Vec<crate::MatrixCase>) {
            cases.extend_from_slice(MATRIX_CASES);
        }
    }
}

fn case_helpers(versions: &[i16], top_name: &Ident) -> TokenStream {
    if versions.is_empty() {
        TokenStream::new()
    } else {
        quote! {
            fn encode_populated(version: i16) -> crate::MatrixResult<String> {
                let message = <#top_name as TestInstance>::test_populated();
                let mut out = BytesMut::new();
                message.write(&mut out, version)?;
                Ok(crate::hex(out.as_ref())?)
            }

            fn encode_null_optionals(version: i16) -> crate::MatrixResult<String> {
                let message = <#top_name as TestInstance>::test_null_optionals();
                let mut out = BytesMut::new();
                message.write(&mut out, version)?;
                Ok(crate::hex(out.as_ref())?)
            }

            fn encode_empty_collections(version: i16) -> crate::MatrixResult<String> {
                let message = <#top_name as TestInstance>::test_empty_collections();
                let mut out = BytesMut::new();
                message.write(&mut out, version)?;
                Ok(crate::hex(out.as_ref())?)
            }

            fn encode_multi_element_collections(version: i16) -> crate::MatrixResult<String> {
                let message = <#top_name as TestInstance>::test_multi_element_collections();
                let mut out = BytesMut::new();
                message.write(&mut out, version)?;
                Ok(crate::hex(out.as_ref())?)
            }

            fn encode_numeric_boundaries(version: i16) -> crate::MatrixResult<String> {
                let message = <#top_name as TestInstance>::test_numeric_boundaries();
                let mut out = BytesMut::new();
                message.write(&mut out, version)?;
                Ok(crate::hex(out.as_ref())?)
            }

            fn encode_tagged_fields(version: i16) -> crate::MatrixResult<String> {
                let message = <#top_name as TestInstance>::test_tagged_fields();
                let mut out = BytesMut::new();
                message.write(&mut out, version)?;
                Ok(crate::hex(out.as_ref())?)
            }

            fn reencode(version: i16, hex_input: &str) -> crate::MatrixResult<String> {
                let mut input = Bytes::from(crate::decode_hex(hex_input)?);
                let message = #top_name::read(&mut input, version)?;
                crate::ensure_input_consumed(&input)?;
                let mut out = BytesMut::new();
                message.write(&mut out, version)?;
                Ok(crate::hex(out.as_ref())?)
            }
        }
    }
}

fn bytes_import(structs: &[StructDef<'_>], has_cases: bool) -> TokenStream {
    let needs_bytes = has_cases
        || structs.iter().any(|def| {
            !def.flexible_versions.is_none()
                || def
                    .fields
                    .iter()
                    .any(|field| field_type_uses_bytes(&field.field_type))
        });
    let needs_bytes_mut = has_cases;
    match (needs_bytes, needs_bytes_mut) {
        (true, true) => quote! { use bytes::{Bytes, BytesMut}; },
        (true, false) => quote! { use bytes::Bytes; },
        (false, true) => quote! { use bytes::BytesMut; },
        (false, false) => TokenStream::new(),
    }
}

fn field_type_uses_bytes(field_type: &crate::ir::field::FieldType) -> bool {
    match field_type {
        crate::ir::field::FieldType::Bytes | crate::ir::field::FieldType::Records => true,
        crate::ir::field::FieldType::Array(inner) => field_type_uses_bytes(inner),
        crate::ir::field::FieldType::Bool
        | crate::ir::field::FieldType::Int8
        | crate::ir::field::FieldType::Int16
        | crate::ir::field::FieldType::Int32
        | crate::ir::field::FieldType::Int64
        | crate::ir::field::FieldType::Uint16
        | crate::ir::field::FieldType::Float64
        | crate::ir::field::FieldType::String
        | crate::ir::field::FieldType::Uuid
        | crate::ir::field::FieldType::Struct(_) => false,
    }
}

fn needs_protocol_prelude(structs: &[StructDef<'_>]) -> bool {
    structs.iter().any(|def| {
        !def.flexible_versions.is_none()
            || def
                .fields
                .iter()
                .any(|field| field_type_uses_protocol_prelude(&field.field_type))
    })
}

fn field_type_uses_protocol_prelude(field_type: &crate::ir::field::FieldType) -> bool {
    match field_type {
        crate::ir::field::FieldType::String | crate::ir::field::FieldType::Uuid => true,
        crate::ir::field::FieldType::Array(inner) => field_type_uses_protocol_prelude(inner),
        crate::ir::field::FieldType::Bool
        | crate::ir::field::FieldType::Int8
        | crate::ir::field::FieldType::Int16
        | crate::ir::field::FieldType::Int32
        | crate::ir::field::FieldType::Int64
        | crate::ir::field::FieldType::Uint16
        | crate::ir::field::FieldType::Float64
        | crate::ir::field::FieldType::Bytes
        | crate::ir::field::FieldType::Records
        | crate::ir::field::FieldType::Struct(_) => false,
    }
}

/// Build the test-utils `mod.rs` [`TokenStream`] declaring one private module
/// per spec name.
pub fn generate_test_utils_mod_rs(specs: &[&MessageSpec]) -> TokenStream {
    let mut modules: Vec<String> = specs.iter().map(|s| s.name.to_snake_case()).collect();
    modules.sort();
    let mod_idents: Vec<Ident> = modules.iter().map(|m| format_ident!("{}", m)).collect();
    let mod_paths: Vec<String> = modules
        .iter()
        .map(|module| format!("generated_test_utils/{module}.rs"))
        .collect();
    let mut case_modules: Vec<String> = specs
        .iter()
        .filter(|s| !version_literals(&s.valid_versions).is_empty())
        .map(|s| s.name.to_snake_case())
        .collect();
    case_modules.sort();
    let case_mod_idents: Vec<Ident> = case_modules
        .iter()
        .map(|m| format_ident!("{}", m))
        .collect();
    quote! {
        #(#[path = #mod_paths] mod #mod_idents;)*

        const APPENDERS: &[fn(&mut Vec<crate::MatrixCase>)] = &[
            #(#case_mod_idents::append_protocol_cases,)*
        ];

        pub(crate) fn protocol_cases() -> Vec<crate::MatrixCase> {
            let mut cases = Vec::new();
            for append in APPENDERS {
                append(&mut cases);
            }
            cases
        }
    }
}

fn java_message_class(spec: &MessageSpec) -> String {
    let class_name = match spec.message_type {
        MessageType::Request | MessageType::Response => format!("{}Data", spec.name),
        MessageType::Data if matches!(spec.name.as_str(), "RequestHeader" | "ResponseHeader") => {
            format!("{}Data", spec.name)
        },
        MessageType::Data => spec.name.clone(),
    };
    format!("org.apache.kafka.common.message.{class_name}")
}

fn version_literals(versions: &VersionRange) -> Vec<i16> {
    match versions {
        VersionRange::None => Vec::new(),
        VersionRange::From(start) => vec![*start],
        VersionRange::Range(start, end) => {
            let mut values = Vec::new();
            let mut version = *start;
            while version <= *end {
                values.push(version);
                version = version.saturating_add(1);
            }
            values
        },
    }
}

#[cfg(test)]
mod tests {
    use super::{generate_test_utils_file, generate_test_utils_mod_rs};
    use crate::ir::{
        message::{MessageSpec, MessageType},
        version_range::VersionRange,
    };

    fn spec(name: &str, message_type: MessageType) -> MessageSpec {
        MessageSpec {
            name: name.to_owned(),
            api_key: Some(18),
            message_type,
            valid_versions: VersionRange::Range(0, 1),
            flexible_versions: VersionRange::None,
            fields: Vec::new(),
            common_structs: Vec::new(),
            listeners: Vec::new(),
            latest_version_unstable: false,
        }
    }

    #[test]
    fn generated_test_utils_mod_registers_java_oracle_cases() {
        let request = spec("ApiVersionsRequest", MessageType::Request);
        let data = spec("KRaftVersionRecord", MessageType::Data);
        let generated = generate_test_utils_mod_rs(&[&request, &data]).to_string();

        assert!(generated.contains("pub (crate) fn protocol_cases"));
        assert!(generated.contains("api_versions_request :: append_protocol_cases"));
        assert!(generated.contains("k_raft_version_record :: append_protocol_cases"));
    }

    #[test]
    fn generated_test_utils_file_registers_java_message_class_names() {
        let request = spec("ApiVersionsRequest", MessageType::Request);
        let data = spec("KRaftVersionRecord", MessageType::Data);
        let request_generated = generate_test_utils_file(&request)
            .expect("request test-utils should generate")
            .to_string();
        let data_generated = generate_test_utils_file(&data)
            .expect("data test-utils should generate")
            .to_string();

        assert!(
            request_generated
                .contains("\"org.apache.kafka.common.message.ApiVersionsRequestData\"")
        );
        assert!(data_generated.contains("\"org.apache.kafka.common.message.KRaftVersionRecord\""));
    }

    #[test]
    fn generated_test_utils_file_registers_populated_case_for_every_valid_version() {
        let request = spec("ApiVersionsRequest", MessageType::Request);
        let generated = generate_test_utils_file(&request)
            .expect("request test-utils should generate")
            .to_string();

        assert!(
            generated.contains("version : 0i16 , fixture : \"populated\""),
            "v0 populated case should be registered: {generated}"
        );
        assert!(
            generated.contains("version : 1i16 , fixture : \"populated\""),
            "v1 populated case should be registered: {generated}"
        );
    }

    #[test]
    fn generated_test_utils_file_registers_release_grade_fixture_families() {
        let request = spec("ApiVersionsRequest", MessageType::Request);
        let generated = generate_test_utils_file(&request)
            .expect("request test-utils should generate")
            .to_string();

        for fixture in [
            "null_optionals",
            "populated",
            "empty_collections",
            "multi_element_collections",
            "numeric_boundaries",
            "tagged_fields",
        ] {
            assert!(
                generated.contains(&format!("version : 0i16 , fixture : \"{fixture}\"")),
                "v0 {fixture} case should be registered: {generated}"
            );
            assert!(
                generated.contains(&format!("version : 1i16 , fixture : \"{fixture}\"")),
                "v1 {fixture} case should be registered: {generated}"
            );
        }
    }
}
