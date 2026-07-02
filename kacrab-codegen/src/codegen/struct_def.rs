//! Internal struct-emission IR plus the helpers that flatten a [`MessageSpec`]
//! into a list of structs to emit.

use proc_macro2::{Ident, Span, TokenStream};
use quote::quote;

use super::{ident::safe_rust_ident, ty::map_field_type};
use crate::ir::{
    field::{FieldSpec, FieldType},
    version_range::VersionRange,
};

/// Codegen-internal description of one struct to emit.
pub(crate) struct StructDef<'a> {
    /// Generated Rust struct name.
    pub(crate) name: String,
    /// Doc comment text, if any.
    pub(crate) about: String,
    /// Fields belonging to this struct.
    pub(crate) fields: &'a [FieldSpec],
    /// `(api_key, valid_versions)` for top-level message structs.
    pub(crate) top_level: Option<(i16, VersionRange)>,
    /// Kafka API key for field-level diagnostics inside nested structs.
    pub(crate) api_key: Option<i16>,
    /// True for the primary `{Name}Data` struct of a message file.
    pub(crate) is_data_struct: bool,
    /// Flexible-version range inherited from the enclosing message.
    pub(crate) flexible_versions: VersionRange,
    /// Effective valid-version range for this struct.
    ///
    /// Used by codegen to eliminate dead version-conditional branches.
    pub(crate) effective_versions: VersionRange,
}

/// Recursively collect nested struct definitions from `fields` into `out`.
///
/// The effective-version range for each nested struct is narrowed by the
/// enclosing field's own version range, since the struct only exists when its
/// owning field is present.
pub(crate) fn collect_nested_structs<'a>(
    fields: &'a [FieldSpec],
    flexible_versions: &VersionRange,
    effective_versions: &VersionRange,
    api_key: Option<i16>,
    out: &mut Vec<StructDef<'a>>,
) {
    for field in fields {
        if !field.fields.is_empty()
            && let Some(struct_name) = extract_struct_name(&field.field_type)
        {
            let narrowed = field.versions.intersect(effective_versions);
            out.push(StructDef {
                name: struct_name,
                about: String::new(),
                fields: &field.fields,
                top_level: None,
                api_key,
                is_data_struct: false,
                flexible_versions: flexible_versions.clone(),
                effective_versions: narrowed.clone(),
            });
            collect_nested_structs(&field.fields, flexible_versions, &narrowed, api_key, out);
        }
    }
}

/// Extract the struct name from a [`FieldType`], peeling array layers.
pub(crate) fn extract_struct_name(ft: &FieldType) -> Option<String> {
    match ft {
        FieldType::Struct(name) => Some(name.clone()),
        FieldType::Array(inner) => extract_struct_name(inner),
        _ => None,
    }
}

/// Escape `[`/`]` so Kafka schema `about` text is not parsed as a rustdoc
/// intra-doc link (e.g. `Array[0]` would otherwise deny-lint as a broken link).
fn doc_text(about: &str) -> String {
    about.replace('[', "\\[").replace(']', "\\]")
}

/// Generate the `pub struct ...` definition for a [`StructDef`].
pub(crate) fn generate_struct(def: &StructDef<'_>) -> TokenStream {
    let name = Ident::new(&def.name, Span::call_site());

    let field_tokens: Vec<TokenStream> = def
        .fields
        .iter()
        .map(|field| {
            let rust_name = safe_rust_ident(&field.name);
            let rust_type = map_field_type(field);
            if field.about.is_empty() {
                quote! {
                    pub #rust_name: #rust_type,
                }
            } else {
                let doc = doc_text(&field.about);
                quote! {
                    #[doc = #doc]
                    pub #rust_name: #rust_type,
                }
            }
        })
        .collect();

    let doc = if def.about.is_empty() {
        quote! {}
    } else {
        let about = doc_text(&def.about);
        quote! { #[doc = #about] }
    };

    quote! {
        #doc
        #[derive(Debug, Clone, PartialEq)]
        pub struct #name {
            #(#field_tokens)*
            pub _unknown_tagged_fields: Vec<RawTaggedField>,
        }
    }
}
