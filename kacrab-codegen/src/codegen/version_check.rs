//! Version-aware check expressions and analysis used by read/write generation.

use proc_macro2::{Ident, Literal, Span, TokenStream};
use quote::quote;

use super::{
    struct_def::StructDef,
    ty::{
        field_type_uses_version, has_version_conditional_nullability, is_field_nullable,
        is_tagged_at_runtime, needs_flexible_branching,
    },
};
use crate::ir::{
    field::{FieldSpec, FieldType},
    version_range::VersionRange,
};

/// Resolve the effective `flexibleVersions` for a field, falling back to the
/// enclosing message's range when the field has no explicit override.
pub(crate) fn effective_flex_versions(
    field: &FieldSpec,
    message_flex: &VersionRange,
) -> VersionRange {
    if field.has_flexible_versions_override {
        field.flexible_versions.clone()
    } else {
        message_flex.clone()
    }
}

/// Generate a "version is in range" check expression, optimised against
/// `effective` so redundant lower/upper bound comparisons are dropped.
pub(crate) fn version_contains_check_with_context(
    vr: &VersionRange,
    effective: &VersionRange,
) -> TokenStream {
    let (eff_min, eff_max) = match effective {
        VersionRange::None => (i16::MIN, i16::MAX),
        VersionRange::From(s) => (*s, i16::MAX),
        VersionRange::Range(s, e) => (*s, *e),
    };

    match vr {
        VersionRange::None => quote! { false },
        VersionRange::From(start) => {
            if eff_min >= *start {
                quote! { true }
            } else {
                let lit = Literal::i16_unsuffixed(*start);
                quote! { version >= #lit }
            }
        },
        VersionRange::Range(start, end) => {
            if start == end {
                let lit = Literal::i16_unsuffixed(*start);
                return quote! { version == #lit };
            }
            let lower_redundant = eff_min >= *start;
            let upper_redundant = eff_max <= *end;
            if lower_redundant && upper_redundant {
                quote! { true }
            } else if lower_redundant {
                let e = Literal::i16_unsuffixed(*end);
                quote! { version <= #e }
            } else if upper_redundant {
                let s = Literal::i16_unsuffixed(*start);
                quote! { version >= #s }
            } else {
                let s = Literal::i16_unsuffixed(*start);
                let e = Literal::i16_unsuffixed(*end);
                quote! { version >= #s && version <= #e }
            }
        },
    }
}

/// Generate the flexible-version check expression.
pub(crate) fn flexible_version_check_with_context(
    vr: &VersionRange,
    effective: &VersionRange,
) -> TokenStream {
    version_contains_check_with_context(vr, effective)
}

/// Extract `(min, max)` bounds from a [`VersionRange`].
///
/// `From(n)` reports `(n, i16::MAX)`. `None` reports `(0, -1)` so the canonical
/// `min..=max` check evaluates to false.
pub(crate) const fn version_range_bounds(vr: &VersionRange) -> (i16, i16) {
    match vr {
        VersionRange::None => (0, -1),
        VersionRange::From(start) => (*start, i16::MAX),
        VersionRange::Range(start, end) => (*start, *end),
    }
}

/// True when every version in `effective` is also in `check`.
pub(crate) fn version_check_always_true(check: &VersionRange, effective: &VersionRange) -> bool {
    check.covers(effective)
}

/// True when no version in `effective` is also in `check`.
pub(crate) const fn version_check_never_true(
    check: &VersionRange,
    effective: &VersionRange,
) -> bool {
    !check.intersects(effective)
}

/// Determine whether the `version` parameter is referenced in the generated
/// `read` body for `def`.
pub(crate) fn version_used_in_read(def: &StructDef<'_>) -> bool {
    if def.top_level.is_some() {
        return true;
    }
    for field in def.fields {
        if is_tagged_at_runtime(field) {
            continue;
        }
        if version_check_never_true(&field.versions, &def.effective_versions) {
            continue;
        }
        if !version_check_always_true(&field.versions, &def.effective_versions) {
            return true;
        }
        if needs_flexible_branching(&field.field_type) {
            let eff_flex = effective_flex_versions(field, &def.flexible_versions);
            let narrowed = field.versions.intersect(&def.effective_versions);
            if !eff_flex.is_none()
                && !version_check_always_true(&eff_flex, &narrowed)
                && !version_check_never_true(&eff_flex, &narrowed)
            {
                return true;
            }
        }
        if has_version_conditional_nullability(field) {
            return true;
        }
        if field_type_uses_version(&field.field_type) {
            return true;
        }
    }
    let has_tagged_fields = def.fields.iter().any(is_tagged_at_runtime);
    if has_tagged_fields || !def.flexible_versions.is_none() {
        if !version_check_always_true(&def.flexible_versions, &def.effective_versions) {
            return true;
        }
        for field in def.fields {
            if field.tag.is_some() {
                if !version_check_always_true(&field.tagged_versions, &def.effective_versions) {
                    return true;
                }
                if field_type_uses_version(&field.field_type) {
                    return true;
                }
            }
        }
    }
    false
}

/// Determine whether the `version` parameter is referenced in the generated
/// `write` body for `def`.
pub(crate) fn version_used_in_write(def: &StructDef<'_>) -> bool {
    if def.top_level.is_some() {
        return true;
    }
    for field in def.fields {
        if is_tagged_at_runtime(field) {
            continue;
        }
        if version_check_never_true(&field.versions, &def.effective_versions) {
            continue;
        }
        if !version_check_always_true(&field.versions, &def.effective_versions) {
            return true;
        }
        if needs_flexible_branching(&field.field_type) {
            let eff_flex = effective_flex_versions(field, &def.flexible_versions);
            let narrowed = field.versions.intersect(&def.effective_versions);
            if !eff_flex.is_none()
                && !version_check_always_true(&eff_flex, &narrowed)
                && !version_check_never_true(&eff_flex, &narrowed)
            {
                return true;
            }
        }
        if has_version_conditional_nullability(field) {
            return true;
        }
        if field_type_uses_version(&field.field_type) {
            return true;
        }
    }
    let has_tagged_fields = def.fields.iter().any(is_tagged_at_runtime);
    if has_tagged_fields || !def.flexible_versions.is_none() {
        if !version_check_always_true(&def.flexible_versions, &def.effective_versions) {
            return true;
        }
        for field in def.fields {
            if field.tag.is_some() {
                if !version_check_always_true(&field.tagged_versions, &def.effective_versions) {
                    return true;
                }
                if field_type_uses_version(&field.field_type) {
                    return true;
                }
            }
        }
    }
    false
}

/// Generate the "field has a non-default value" check used to gate tagged-field writes.
pub(crate) fn generate_non_default_check(field: &FieldSpec, var_ident: &Ident) -> TokenStream {
    if is_field_nullable(field) {
        return quote! { self.#var_ident.is_some() };
    }
    match &field.field_type {
        FieldType::Bool => quote! { self.#var_ident },
        FieldType::Int8
        | FieldType::Int16
        | FieldType::Int32
        | FieldType::Int64
        | FieldType::Uint16 => {
            let default_val = field.default.as_ref().map_or_else(
                || "0".to_owned(),
                |d| {
                    if d.starts_with("0x") || d.starts_with("0X") {
                        d.clone()
                    } else if let Ok(v) = d.parse::<i64>() {
                        v.to_string()
                    } else {
                        "0".to_owned()
                    }
                },
            );

            let suffix = match &field.field_type {
                FieldType::Int8 => "_i8",
                FieldType::Int16 => "_i16",
                FieldType::Int32 => "_i32",
                FieldType::Int64 => "_i64",
                FieldType::Uint16 => "_u16",
                _ => "",
            };
            let default_expr: TokenStream =
                format!("{default_val}{suffix}").parse().unwrap_or_default();
            quote! { self.#var_ident != #default_expr }
        },
        FieldType::Float64 => quote! { self.#var_ident != 0.0 },
        FieldType::String | FieldType::Bytes | FieldType::Array(_) => {
            quote! { !self.#var_ident.is_empty() }
        },
        FieldType::Uuid => quote! { !self.#var_ident.is_nil() },
        FieldType::Records => quote! { self.#var_ident.is_some() },
        FieldType::Struct(name) => {
            let id = Ident::new(name, Span::call_site());
            quote! { self.#var_ident != #id::default() }
        },
    }
}

#[cfg(test)]
mod tests {
    use proc_macro2::Ident;

    use super::{
        effective_flex_versions, flexible_version_check_with_context, generate_non_default_check,
        version_check_always_true, version_check_never_true, version_contains_check_with_context,
        version_range_bounds, version_used_in_read, version_used_in_write,
    };
    use crate::{
        codegen::struct_def::StructDef,
        ir::{
            field::{FieldSpec, FieldType},
            version_range::VersionRange,
        },
    };

    fn field(name: &str, field_type: FieldType) -> FieldSpec {
        FieldSpec {
            name: name.to_owned(),
            field_type,
            versions: VersionRange::Range(0, 3),
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

    fn def(
        fields: &[FieldSpec],
        top_level: bool,
        flex: VersionRange,
        effective: VersionRange,
    ) -> StructDef<'_> {
        StructDef {
            name: "ExampleData".to_owned(),
            about: String::new(),
            fields,
            top_level: top_level.then_some((99, effective.clone())),
            api_key: Some(99),
            is_data_struct: true,
            flexible_versions: flex,
            effective_versions: effective,
        }
    }

    #[test]
    fn version_checks_elide_redundant_bounds_and_detect_truth_tables() {
        assert_eq!(
            version_contains_check_with_context(
                &VersionRange::Range(1, 3),
                &VersionRange::Range(1, 3)
            )
            .to_string(),
            "true"
        );
        assert_eq!(
            version_contains_check_with_context(
                &VersionRange::Range(1, 3),
                &VersionRange::Range(2, 5)
            )
            .to_string(),
            "version <= 3"
        );
        assert_eq!(
            version_contains_check_with_context(
                &VersionRange::Range(1, 3),
                &VersionRange::Range(0, 2)
            )
            .to_string(),
            "version >= 1"
        );
        assert_eq!(
            flexible_version_check_with_context(&VersionRange::From(2), &VersionRange::Range(0, 3))
                .to_string(),
            "version >= 2"
        );
        assert_eq!(version_range_bounds(&VersionRange::None), (0, -1));
        assert!(version_check_always_true(
            &VersionRange::From(0),
            &VersionRange::Range(1, 3)
        ));
        assert!(version_check_never_true(
            &VersionRange::Range(0, 1),
            &VersionRange::Range(2, 3)
        ));
    }

    #[test]
    fn effective_flex_versions_prefers_field_override() {
        let inherited = field("Name", FieldType::String);
        assert_eq!(
            effective_flex_versions(&inherited, &VersionRange::From(2)),
            VersionRange::From(2)
        );

        let overridden = FieldSpec {
            has_flexible_versions_override: true,
            flexible_versions: VersionRange::Range(3, 3),
            ..field("Name", FieldType::String)
        };
        assert_eq!(
            effective_flex_versions(&overridden, &VersionRange::From(2)),
            VersionRange::Range(3, 3)
        );
    }

    #[test]
    fn version_used_tracks_top_level_presence_flex_nullability_structs_and_tags() {
        let plain = [field("Count", FieldType::Int32)];
        assert!(version_used_in_read(&def(
            &plain,
            true,
            VersionRange::None,
            VersionRange::Range(0, 3)
        )));
        assert!(!version_used_in_read(&def(
            &plain,
            false,
            VersionRange::None,
            VersionRange::Range(0, 3)
        )));
        assert!(!version_used_in_write(&def(
            &plain,
            false,
            VersionRange::None,
            VersionRange::Range(0, 3)
        )));

        let versioned = [FieldSpec {
            versions: VersionRange::Range(1, 3),
            ..field("Count", FieldType::Int32)
        }];
        assert!(version_used_in_read(&def(
            &versioned,
            false,
            VersionRange::None,
            VersionRange::Range(0, 3)
        )));

        let flex_branch = [field("Name", FieldType::String)];
        assert!(version_used_in_write(&def(
            &flex_branch,
            false,
            VersionRange::From(2),
            VersionRange::Range(0, 3)
        )));

        let nullable_branch = [FieldSpec {
            nullable_versions: VersionRange::Range(2, 3),
            ..field("Name", FieldType::String)
        }];
        assert!(version_used_in_read(&def(
            &nullable_branch,
            false,
            VersionRange::None,
            VersionRange::Range(0, 3)
        )));

        let struct_field = [field("Child", FieldType::Struct("Child".to_owned()))];
        assert!(version_used_in_write(&def(
            &struct_field,
            false,
            VersionRange::None,
            VersionRange::Range(0, 3)
        )));

        let tagged = [FieldSpec {
            tag: Some(1),
            tagged_versions: VersionRange::Range(2, 3),
            ..field("Tagged", FieldType::String)
        }];
        assert!(version_used_in_read(&def(
            &tagged,
            false,
            VersionRange::From(2),
            VersionRange::Range(0, 3)
        )));
    }

    #[test]
    fn non_default_checks_match_field_kind_and_defaults() {
        let ident = Ident::new("value", proc_macro2::Span::call_site());
        assert_eq!(
            generate_non_default_check(&field("Enabled", FieldType::Bool), &ident).to_string(),
            "self . value"
        );
        assert_eq!(
            generate_non_default_check(
                &FieldSpec {
                    default: Some("0x7f".to_owned()),
                    ..field("Tiny", FieldType::Int8)
                },
                &ident,
            )
            .to_string(),
            "self . value != 0x7f_i8"
        );
        assert_eq!(
            generate_non_default_check(&field("Ratio", FieldType::Float64), &ident).to_string(),
            "self . value != 0.0"
        );
        assert_eq!(
            generate_non_default_check(&field("Name", FieldType::String), &ident).to_string(),
            "! self . value . is_empty ()"
        );
        assert_eq!(
            generate_non_default_check(&field("TopicId", FieldType::Uuid), &ident).to_string(),
            "! self . value . is_nil ()"
        );
        assert_eq!(
            generate_non_default_check(&field("Records", FieldType::Records), &ident).to_string(),
            "self . value . is_some ()"
        );
        assert_eq!(
            generate_non_default_check(
                &field("Child", FieldType::Struct("Child".to_owned())),
                &ident
            )
            .to_string(),
            "self . value != Child :: default ()"
        );
        assert_eq!(
            generate_non_default_check(
                &FieldSpec {
                    nullable_versions: VersionRange::Range(0, 3),
                    ..field("Maybe", FieldType::String)
                },
                &ident,
            )
            .to_string(),
            "self . value . is_some ()"
        );
    }
}
