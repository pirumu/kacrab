//! Read the committed `client_api_info` table back out of generated Rust.
//!
//! The generator bakes one `ApiInfo` per API key into
//! `kacrab-protocol/src/generated.rs`. Reading that table back — instead of
//! re-deriving it — is what makes the report able to detect a generated file
//! that has drifted from the schema snapshot it was generated from.

use std::collections::BTreeMap;

use quote::ToTokens as _;
use syn::{Expr, ExprLit, ExprMatch, ExprStruct, Item, ItemFn, Lit, Member, Pat, Path, Stmt};

use super::error::{ProtocolSupportError, ProtocolSupportErrorKind};

/// One `ApiInfo` row as committed in the generated protocol crate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClientApiInfo {
    /// Lowest request version the client can encode.
    pub min_version: i16,
    /// Highest request version the client can encode.
    pub max_version: i16,
    /// First flexible version, or `i16::MAX` when the API is never flexible.
    pub flexible_versions_start: i16,
}

/// `ApiKey` variant name to the committed [`ClientApiInfo`] for that key.
pub type ClientApiInfoTable = BTreeMap<String, ClientApiInfo>;

/// Parse the generated `client_api_info` dispatch table out of Rust source.
pub fn parse_client_api_info(source: &str) -> Result<ClientApiInfoTable, ProtocolSupportError> {
    let file = syn::parse_file(source).map_err(ProtocolSupportError::new)?;
    let function = find_client_api_info(&file.items)
        .ok_or_else(|| ProtocolSupportError::new(ProtocolSupportErrorKind::MissingClientApiInfo))?;
    let dispatch = match_expression(function).ok_or_else(|| {
        ProtocolSupportError::new(ProtocolSupportErrorKind::MissingMatchExpression)
    })?;

    let mut table = ClientApiInfoTable::new();
    for arm in &dispatch.arms {
        let variant = arm_variant(&arm.pat)?;
        let info = arm_api_info(&variant, &arm.body)?;
        let _previous = table.insert(variant, info);
    }
    Ok(table)
}

fn find_client_api_info(items: &[Item]) -> Option<&ItemFn> {
    items.iter().find_map(|item| match item {
        Item::Fn(function) if function.sig.ident == "client_api_info" => Some(function),
        Item::Mod(module) => module
            .content
            .as_ref()
            .and_then(|(_brace, nested)| find_client_api_info(nested)),
        _ => None,
    })
}

fn match_expression(function: &ItemFn) -> Option<&ExprMatch> {
    function.block.stmts.iter().find_map(|stmt| match stmt {
        Stmt::Expr(Expr::Match(dispatch), _semicolon) => Some(dispatch),
        _ => None,
    })
}

fn arm_variant(pat: &Pat) -> Result<String, ProtocolSupportError> {
    let variant = match pat {
        Pat::Path(path) => path.path.segments.last().map(|last| last.ident.to_string()),
        _ => None,
    };
    variant.ok_or_else(|| {
        ProtocolSupportError::new(ProtocolSupportErrorKind::UnsupportedArmPattern {
            pattern: pat.to_token_stream().to_string(),
        })
    })
}

fn arm_api_info(api: &str, body: &Expr) -> Result<ClientApiInfo, ProtocolSupportError> {
    let Expr::Struct(literal) = body else {
        return Err(ProtocolSupportError::new(
            ProtocolSupportErrorKind::UnsupportedArmBody {
                api: api.to_owned(),
            },
        ));
    };
    collect_api_info_fields(api, literal)
}

fn collect_api_info_fields(
    api: &str,
    literal: &ExprStruct,
) -> Result<ClientApiInfo, ProtocolSupportError> {
    let mut min_version = None;
    let mut max_version = None;
    let mut flexible_versions_start = None;

    for field in &literal.fields {
        let name = match &field.member {
            Member::Named(ident) => ident.to_string(),
            Member::Unnamed(index) => index.index.to_string(),
        };
        let value = field_value(api, &name, &field.expr)?;
        match name.as_str() {
            "min_version" => min_version = Some(value),
            "max_version" => max_version = Some(value),
            "flexible_versions_start" => flexible_versions_start = Some(value),
            _ => {
                return Err(ProtocolSupportError::new(
                    ProtocolSupportErrorKind::UnknownApiInfoField {
                        api: api.to_owned(),
                        field: name,
                    },
                ));
            },
        }
    }

    Ok(ClientApiInfo {
        min_version: required(api, "min_version", min_version)?,
        max_version: required(api, "max_version", max_version)?,
        flexible_versions_start: required(api, "flexible_versions_start", flexible_versions_start)?,
    })
}

fn required(
    api: &str,
    field: &'static str,
    value: Option<i16>,
) -> Result<i16, ProtocolSupportError> {
    value.ok_or_else(|| {
        ProtocolSupportError::new(ProtocolSupportErrorKind::MissingApiInfoField {
            api: api.to_owned(),
            field,
        })
    })
}

fn field_value(api: &str, field: &str, expr: &Expr) -> Result<i16, ProtocolSupportError> {
    let unsupported = || {
        ProtocolSupportError::new(ProtocolSupportErrorKind::UnsupportedApiInfoValue {
            api: api.to_owned(),
            field: field.to_owned(),
        })
    };
    match expr {
        Expr::Lit(ExprLit {
            lit: Lit::Int(int), ..
        }) => int.base10_parse::<i16>().map_err(|_parse| unsupported()),
        Expr::Path(path) if is_i16_max(&path.path) => Ok(i16::MAX),
        _ => Err(unsupported()),
    }
}

fn is_i16_max(path: &Path) -> bool {
    let idents: Vec<String> = path
        .segments
        .iter()
        .map(|segment| segment.ident.to_string())
        .collect();
    idents.iter().map(String::as_str).eq(["i16", "MAX"])
}

#[cfg(test)]
mod tests {
    use super::{ClientApiInfo, parse_client_api_info};
    use crate::protocol_support::error::ProtocolSupportErrorKind;

    const TABLE: &str = r"
        pub fn client_api_info(api_key: ApiKey) -> ApiInfo {
            match api_key {
                ApiKey::Produce => ApiInfo {
                    min_version: 3,
                    max_version: 13,
                    flexible_versions_start: 9,
                },
                ApiKey::LeaderAndIsr => ApiInfo {
                    min_version: 0,
                    max_version: 0,
                    flexible_versions_start: i16::MAX,
                },
            }
        }
    ";

    #[test]
    fn reads_every_generated_arm() {
        let table = parse_client_api_info(TABLE).expect("fixture table should parse");

        assert_eq!(table.len(), 2, "every match arm should become a row");
        assert_eq!(
            table.get("Produce").copied(),
            Some(ClientApiInfo {
                min_version: 3,
                max_version: 13,
                flexible_versions_start: 9,
            }),
            "arm literals should be read verbatim"
        );
        assert_eq!(
            table
                .get("LeaderAndIsr")
                .map(|info| info.flexible_versions_start),
            Some(i16::MAX),
            "`i16::MAX` should decode as the never-flexible sentinel"
        );
    }

    #[test]
    fn missing_table_is_an_error() {
        let error = parse_client_api_info("pub fn other() {}")
            .expect_err("source without the table should fail");

        assert!(
            matches!(error.kind, ProtocolSupportErrorKind::MissingClientApiInfo),
            "reporter should refuse to guess when the table is absent, got {error:?}"
        );
    }

    #[test]
    fn unmodelled_api_info_field_is_an_error() {
        let source = r"
            pub fn client_api_info(api_key: ApiKey) -> ApiInfo {
                match api_key {
                    ApiKey::Produce => ApiInfo {
                        min_version: 3,
                        max_version: 13,
                        flexible_versions_start: 9,
                        deprecated_versions_start: 3,
                    },
                }
            }
        ";

        let error =
            parse_client_api_info(source).expect_err("new ApiInfo fields should fail loudly");

        assert!(
            matches!(
                error.kind,
                ProtocolSupportErrorKind::UnknownApiInfoField { .. }
            ),
            "a widened ApiInfo must force the reporter to be updated, got {error:?}"
        );
    }
}
