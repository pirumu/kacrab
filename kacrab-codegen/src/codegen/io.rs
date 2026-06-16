//! Generation of `read`/`write` methods for emitted structs.

use proc_macro2::{Ident, Literal, Span, TokenStream};
use quote::quote;

use super::{
    default_impl::resolve_default,
    error::CodegenErrorKind,
    ident::{local_var_ident, safe_rust_ident, shadows_param},
    read_expr::{generate_read_field_expr, generate_read_tagged_field_expr},
    struct_def::StructDef,
    ty::is_tagged_at_runtime,
    version_check::{
        flexible_version_check_with_context, generate_non_default_check, version_check_always_true,
        version_check_never_true, version_contains_check_with_context, version_range_bounds,
        version_used_in_read, version_used_in_write,
    },
    write_expr::{generate_write_field_expr, generate_write_tagged_field_expr},
};

/// Generate the `impl {Name} { fn read(...); fn write(...); }` block.
pub(crate) fn generate_read_write_impl(
    def: &StructDef<'_>,
) -> Result<TokenStream, CodegenErrorKind> {
    let name = Ident::new(&def.name, Span::call_site());
    let read_method = generate_read_method(def)?;
    let write_method = generate_write_method(def);
    Ok(quote! {
        impl #name {
            #read_method
            #write_method
        }
    })
}

fn generate_read_method(def: &StructDef<'_>) -> Result<TokenStream, CodegenErrorKind> {
    let mut body: Vec<TokenStream> = Vec::new();

    if let Some(guard) = generate_top_level_version_guard(def) {
        body.push(guard);
    }
    body.extend(generate_read_field_var_decls(def)?);
    body.push(quote! {
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
    });
    body.extend(generate_read_field_assignments(def));
    if let Some(tagged) = generate_read_tagged_section(def) {
        body.push(tagged);
    }
    body.push(generate_read_constructor(def));

    let (buf_param, ver_param) = read_method_signature(def);
    Ok(quote! {
        pub fn read(#buf_param, #ver_param) -> Result<Self> {
            #(#body)*
        }
    })
}

/// Top-level entry-point version-range guard (only for request/response structs).
fn generate_top_level_version_guard(def: &StructDef<'_>) -> Option<TokenStream> {
    let (api_key, valid_versions) = def.top_level.as_ref()?;
    let (min, max) = version_range_bounds(valid_versions);
    let min_lit = Literal::i16_unsuffixed(min);
    let max_lit = Literal::i16_unsuffixed(max);
    let api_key_lit = Literal::i16_unsuffixed(*api_key);
    Some(quote! {
        if version < #min_lit || version > #max_lit {
            return Err(UnsupportedVersion::new(#api_key_lit, version).into());
        }
    })
}

/// Per-field local-variable declarations for the read method.
///
/// Variables that are unconditionally written get a bare `let`; variables that
/// are dead at the effective version range get a `let = default`; everything
/// else gets `let mut = default` so the field-read pass can assign into them.
fn generate_read_field_var_decls(
    def: &StructDef<'_>,
) -> Result<Vec<TokenStream>, CodegenErrorKind> {
    def.fields
        .iter()
        .map(|field| {
            let local_name = local_var_ident(&field.name);
            let is_tagged = is_tagged_at_runtime(field);
            let is_dead =
                !is_tagged && version_check_never_true(&field.versions, &def.effective_versions);
            let is_always = !is_tagged
                && !is_dead
                && version_check_always_true(&field.versions, &def.effective_versions);

            if is_always {
                Ok(quote! { let #local_name; })
            } else if is_dead {
                let default_val = resolve_default(field)?;
                Ok(quote! { let #local_name = #default_val; })
            } else {
                let default_val = resolve_default(field)?;
                Ok(quote! { let mut #local_name = #default_val; })
            }
        })
        .collect()
}

/// Per-field read assignments, version-gated when the field's range is narrower
/// than the enclosing struct's effective range.
fn generate_read_field_assignments(def: &StructDef<'_>) -> Vec<TokenStream> {
    def.fields
        .iter()
        .filter(|f| !is_tagged_at_runtime(f))
        .filter(|f| !version_check_never_true(&f.versions, &def.effective_versions))
        .map(|field| {
            let local_name = local_var_ident(&field.name);
            let read_expr = generate_read_field_expr(
                field,
                &local_name,
                &def.flexible_versions,
                &def.effective_versions,
            );
            if version_check_always_true(&field.versions, &def.effective_versions) {
                read_expr
            } else {
                let version_check =
                    version_contains_check_with_context(&field.versions, &def.effective_versions);
                quote! {
                    if #version_check {
                        #read_expr
                    }
                }
            }
        })
        .collect()
}

/// Tagged-field reader (KIP-482), gated on the struct's flexible-version range.
fn generate_read_tagged_section(def: &StructDef<'_>) -> Option<TokenStream> {
    let has_tagged_fields = def.fields.iter().any(is_tagged_at_runtime);
    if !has_tagged_fields && def.flexible_versions.is_none() {
        return None;
    }

    let tagged_context = def.flexible_versions.intersect(&def.effective_versions);
    let tag_arms: Vec<TokenStream> = def
        .fields
        .iter()
        .filter_map(|field| {
            let tag = field.tag?;
            let local_name = local_var_ident(&field.name);
            let tag_lit = Literal::i32_unsuffixed(tag);
            let read_expr = generate_read_tagged_field_expr(field, &local_name);
            let arm = if version_check_always_true(&field.tagged_versions, &tagged_context) {
                quote! {
                    #tag_lit => {
                        let mut tag_buf = field.data.clone();
                        #read_expr
                    }
                }
            } else {
                let tagged_check =
                    version_contains_check_with_context(&field.tagged_versions, &tagged_context);
                quote! {
                    #tag_lit => {
                        if #tagged_check {
                            let mut tag_buf = field.data.clone();
                            #read_expr
                        }
                    }
                }
            };
            Some(arm)
        })
        .collect();

    let tagged_body = quote! {
        let tagged_fields = read_tagged_fields(buf)?;
        for field in &tagged_fields {
            match field.tag {
                #(#tag_arms)*
                _ => {
                    _unknown_tagged_fields.push(field.clone());
                }
            }
        }
    };

    Some(
        if version_check_always_true(&def.flexible_versions, &def.effective_versions) {
            tagged_body
        } else {
            let flex_check = flexible_version_check_with_context(
                &def.flexible_versions,
                &def.effective_versions,
            );
            quote! {
                if #flex_check {
                    #tagged_body
                }
            }
        },
    )
}

/// `Ok(Self { ... })` constructor, mapping fields whose names collide with
/// `version`/`buf` to their local-var alias.
fn generate_read_constructor(def: &StructDef<'_>) -> TokenStream {
    let field_inits: Vec<TokenStream> = def
        .fields
        .iter()
        .map(|f| {
            let field_name = safe_rust_ident(&f.name);
            let local_name = local_var_ident(&f.name);
            if shadows_param(&f.name) {
                quote! { #field_name: #local_name }
            } else {
                quote! { #field_name }
            }
        })
        .collect();
    quote! {
        Ok(Self {
            #(#field_inits,)*
            _unknown_tagged_fields,
        })
    }
}

/// Compute `(buf_param, version_param)` — leading-underscore prefixes the
/// parameter name when the body never references it, which dodges
/// `unused_variables` without an `#[allow]` on the generated code.
fn read_method_signature(def: &StructDef<'_>) -> (TokenStream, TokenStream) {
    let has_non_tagged_fields = def.fields.iter().any(|f| !is_tagged_at_runtime(f));
    let has_tagged_section =
        def.fields.iter().any(is_tagged_at_runtime) || !def.flexible_versions.is_none();
    let buf_used = has_non_tagged_fields || has_tagged_section;
    let ver_used = version_used_in_read(def);
    let buf_param = if buf_used {
        quote! { buf: &mut Bytes }
    } else {
        quote! { _buf: &mut Bytes }
    };
    let ver_param = if ver_used {
        quote! { version: i16 }
    } else {
        quote! { _version: i16 }
    };
    (buf_param, ver_param)
}

fn generate_write_method(def: &StructDef<'_>) -> TokenStream {
    let mut body: Vec<TokenStream> = Vec::new();

    if let Some(guard) = generate_top_level_version_guard(def) {
        body.push(guard);
    }
    body.extend(generate_write_field_exprs(def));
    if let Some(tagged) = generate_write_tagged_section(def) {
        body.push(tagged);
    }
    body.push(quote! { Ok(()) });

    let (buf_param, ver_param) = write_method_signature(def);
    quote! {
        pub fn write(&self, #buf_param, #ver_param) -> Result<()> {
            #(#body)*
        }
    }
}

/// Per-field write expressions, version-gated when the field's range is
/// narrower than the enclosing struct's effective range.
fn generate_write_field_exprs(def: &StructDef<'_>) -> Vec<TokenStream> {
    def.fields
        .iter()
        .filter(|f| !is_tagged_at_runtime(f))
        .filter(|f| !version_check_never_true(&f.versions, &def.effective_versions))
        .map(|field| {
            let write_expr = generate_write_field_expr(
                field,
                &safe_rust_ident(&field.name),
                &def.flexible_versions,
                &def.effective_versions,
            );
            if version_check_always_true(&field.versions, &def.effective_versions) {
                write_expr
            } else {
                let version_check =
                    version_contains_check_with_context(&field.versions, &def.effective_versions);
                quote! {
                    if #version_check {
                        #write_expr
                    }
                }
            }
        })
        .collect()
}

/// Tagged-field writer (KIP-482), gated on the struct's flexible-version range.
fn generate_write_tagged_section(def: &StructDef<'_>) -> Option<TokenStream> {
    let has_tagged_fields = def.fields.iter().any(is_tagged_at_runtime);
    if !has_tagged_fields && def.flexible_versions.is_none() {
        return None;
    }

    let tagged_context = def.flexible_versions.intersect(&def.effective_versions);
    let tag_writes: Vec<TokenStream> = def
        .fields
        .iter()
        .filter_map(|field| generate_tagged_write_arm(field, &tagged_context))
        .collect();

    let tagged_body = if tag_writes.is_empty() {
        quote! {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
    } else {
        quote! {
            let mut known_tagged_fields: Vec<RawTaggedField> = Vec::new();
            #(#tag_writes)*
            let mut all_tags = known_tagged_fields;
            all_tags.extend(self._unknown_tagged_fields.iter().cloned());
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
    };

    Some(
        if version_check_always_true(&def.flexible_versions, &def.effective_versions) {
            tagged_body
        } else {
            let flex_check = flexible_version_check_with_context(
                &def.flexible_versions,
                &def.effective_versions,
            );
            quote! {
                if #flex_check {
                    #tagged_body
                }
            }
        },
    )
}

/// Emit one tagged-field branch (`if non_default { write_to_tag_buf; push }`).
fn generate_tagged_write_arm(
    field: &crate::ir::field::FieldSpec,
    tagged_context: &crate::ir::version_range::VersionRange,
) -> Option<TokenStream> {
    let tag = field.tag?;
    let rust_name = safe_rust_ident(&field.name);
    let non_default_check = generate_non_default_check(field, &rust_name);
    let tag_lit = Literal::i32_unsuffixed(tag);
    let write_expr = generate_write_tagged_field_expr(field, &rust_name);
    let arm = if version_check_always_true(&field.tagged_versions, tagged_context) {
        quote! {
            if #non_default_check {
                let mut tag_buf = BytesMut::new();
                #write_expr
                known_tagged_fields.push(RawTaggedField { tag: #tag_lit, data: tag_buf.freeze() });
            }
        }
    } else {
        let tagged_check =
            version_contains_check_with_context(&field.tagged_versions, tagged_context);
        quote! {
            if #tagged_check && #non_default_check {
                let mut tag_buf = BytesMut::new();
                #write_expr
                known_tagged_fields.push(RawTaggedField { tag: #tag_lit, data: tag_buf.freeze() });
            }
        }
    };
    Some(arm)
}

/// Compute `(buf_param, version_param)` for `write` — same param-naming rule
/// as [`read_method_signature`].
fn write_method_signature(def: &StructDef<'_>) -> (TokenStream, TokenStream) {
    let has_non_tagged_fields = def.fields.iter().any(|f| !is_tagged_at_runtime(f));
    let has_tagged_section =
        def.fields.iter().any(is_tagged_at_runtime) || !def.flexible_versions.is_none();
    let buf_used = has_non_tagged_fields || has_tagged_section;
    let ver_used = version_used_in_write(def);
    let buf_param = if buf_used {
        quote! { buf: &mut BytesMut }
    } else {
        quote! { _buf: &mut BytesMut }
    };
    let ver_param = if ver_used {
        quote! { version: i16 }
    } else {
        quote! { _version: i16 }
    };
    (buf_param, ver_param)
}
