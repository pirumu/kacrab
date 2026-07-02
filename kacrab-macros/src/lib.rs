//! Procedural macros for kacrab.
//!
//! This crate produces proc-macros consumed by the main `kacrab` crate via
//! the `macros` feature. It is not intended to be used directly.

use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::{format_ident, quote};
use syn::{
    Attribute, Error, Expr, ExprLit, Ident, Lit, LitStr, Meta, Result, Token, Type, Visibility,
    braced,
    parse::{Parse, ParseStream},
    parse_macro_input,
    punctuated::Punctuated,
};

/// Declares a Kafka client config schema and generates a typed struct,
/// builder, key constants, and static config metadata.
#[proc_macro]
pub fn kafka_config(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as ConfigInput);
    expand_config(input)
        .unwrap_or_else(Error::into_compile_error)
        .into()
}

struct ConfigInput {
    attrs: Vec<Attribute>,
    visibility: Visibility,
    name: Ident,
    fields: Punctuated<ConfigField, Token![,]>,
}

impl Parse for ConfigInput {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let attrs = input.call(Attribute::parse_outer)?;
        let visibility = input.parse()?;
        let name = input.parse()?;
        let content;
        braced!(content in input);
        let fields = content.parse_terminated(ConfigField::parse, Token![,])?;
        Ok(Self {
            attrs,
            visibility,
            name,
            fields,
        })
    }
}

struct ConfigField {
    attrs: Vec<Attribute>,
    name: Ident,
    ty: Type,
}

impl Parse for ConfigField {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let attrs = input.call(Attribute::parse_outer)?;
        let name = input.parse()?;
        let _: Token![:] = input.parse()?;
        let ty = input.parse()?;
        Ok(Self { attrs, name, ty })
    }
}

#[derive(Clone, Copy)]
enum ClientKindAttr {
    Producer,
    Consumer,
    Admin,
}

struct FieldAttrs {
    key: LitStr,
    required: bool,
    default: Option<Expr>,
    kafka_type: LitStr,
    kafka_default: LitStr,
    status: StatusAttr,
    origin: OriginAttr,
    source: LitStr,
    platforms: Vec<LitStr>,
    feature: Option<LitStr>,
    comment: LitStr,
}

#[derive(Clone)]
enum StatusAttr {
    Native,
    NativeReview,
    SkipJavaOnly,
    FeatureGated(LitStr),
    Future(LitStr),
}

#[derive(Clone, Copy)]
enum OriginAttr {
    Kafka,
    KacrabRuntime,
}

type Tokens = proc_macro2::TokenStream;

struct FieldExpansion {
    struct_field: Tokens,
    builder_field: Tokens,
    builder_initializer: Tokens,
    setter: Tokens,
    build_field: Tokens,
    constant: Tokens,
    metadata: Tokens,
    key_match: Tokens,
    property_parse: Tokens,
}

fn expand_config(input: ConfigInput) -> Result<proc_macro2::TokenStream> {
    let client = parse_client(&input.attrs)?;
    let client_tokens = client_tokens(client);
    let client_label = client_label(client);
    let visibility = input.visibility;
    let name = input.name;
    let builder_name = format_ident!("{name}Builder");
    let struct_doc = LitStr::new(
        &format!("Typed Kafka {client_label} configuration."),
        Span::call_site(),
    );
    let builder_doc = LitStr::new(&format!("Builder for [`{name}`]."), Span::call_site());

    let expansions = input
        .fields
        .into_iter()
        .map(|field| expand_field(field, &client_tokens))
        .collect::<Result<Vec<_>>>()?;
    let struct_fields: Vec<_> = expansions.iter().map(|field| &field.struct_field).collect();
    let builder_fields: Vec<_> = expansions
        .iter()
        .map(|field| &field.builder_field)
        .collect();
    let builder_initializers: Vec<_> = expansions
        .iter()
        .map(|field| &field.builder_initializer)
        .collect();
    let setters: Vec<_> = expansions.iter().map(|field| &field.setter).collect();
    let build_fields: Vec<_> = expansions.iter().map(|field| &field.build_field).collect();
    let constants: Vec<_> = expansions.iter().map(|field| &field.constant).collect();
    let metadata: Vec<_> = expansions.iter().map(|field| &field.metadata).collect();
    let key_matches: Vec<_> = expansions.iter().map(|field| &field.key_match).collect();
    let property_parsers: Vec<_> = expansions
        .iter()
        .map(|field| &field.property_parse)
        .collect();

    Ok(quote! {
        #[doc = #struct_doc]
        #[derive(Clone, Debug)]
        #visibility struct #name {
            #(#struct_fields,)*
        }

        #[doc = #builder_doc]
        #[derive(Clone, Debug, Default)]
        #visibility struct #builder_name {
            #(#builder_fields,)*
        }

        impl #name {
            #(#constants)*

            /// Official Kafka configuration metadata for this typed config.
            pub const CONFIG_KEYS: &'static [::kacrab::config::ConfigEntry] = &[
                #(#metadata,)*
            ];

            /// Creates a builder for this config.
            #[must_use]
            pub fn builder() -> #builder_name {
                #builder_name {
                    #(#builder_initializers,)*
                }
            }

            /// Parses Java-style properties into this typed config.
            pub fn from_properties(
                properties: &::kacrab::config::Properties,
                unknown_key_policy: ::kacrab::config::UnknownKeyPolicy,
            ) -> ::core::result::Result<(#name, ::kacrab::config::WarningReport), ::kacrab::config::ConfigError> {
                let report = ::kacrab::config::validate_properties(
                    #client_tokens,
                    properties,
                    unknown_key_policy,
                )?;
                for (key, _value) in properties.iter() {
                    let key = key.as_str();
                    if !::core::matches!(key, #(#key_matches)|*) {
                        return ::core::result::Result::Err(::kacrab::config::ConfigError::UnsupportedKey {
                            client: #client_tokens,
                            key: key.into(),
                        });
                    }
                }
                let mut builder = Self::builder();
                #(#property_parsers)*
                let config = builder.build()?;
                ::core::result::Result::Ok((config, report))
            }
        }

        impl #builder_name {
            #(#setters)*

            /// Builds the config, returning an error when required keys are missing.
            pub fn build(self) -> ::core::result::Result<#name, ::kacrab::config::ConfigError> {
                ::core::result::Result::Ok(#name {
                    #(#build_fields,)*
                })
            }
        }
    })
}

fn expand_field(field: ConfigField, client_tokens: &Tokens) -> Result<FieldExpansion> {
    let attrs = parse_field_attrs(&field.attrs)?;
    if attrs.required && attrs.default.is_some() {
        return Err(Error::new_spanned(
            field.name,
            "config field cannot be both required and defaulted",
        ));
    }

    let field_name = field.name;
    let field_ty = field.ty;
    let option_field_ty = &field_ty;
    let key = attrs.key;
    let kafka_type = attrs.kafka_type;
    let kafka_default = attrs.kafka_default;
    let source = attrs.source;
    let comment = attrs.comment;
    let status = status_tokens(attrs.status);
    let origin = origin_tokens(attrs.origin);
    let platforms = attrs.platforms;
    let feature = option_lit_str_tokens(attrs.feature.as_ref());
    let const_name = format_ident!("{}_CONFIG", screaming_snake(&field_name.to_string()));
    let const_doc = LitStr::new(
        &format!("Kafka property key `{}`.", key.value()),
        key.span(),
    );
    let setter_doc = LitStr::new(
        &format!("Sets the `{}` Kafka property.", key.value()),
        key.span(),
    );

    let struct_field = quote! {
        #[doc = #comment]
        pub #field_name: #field_ty
    };
    let builder_field = quote! {
        #field_name: ::core::option::Option<#option_field_ty>
    };
    let builder_initializer = quote! {
        #field_name: ::core::option::Option::None
    };
    let setter = quote! {
        #[doc = #setter_doc]
        #[must_use]
        pub fn #field_name(mut self, value: impl ::core::convert::Into<#field_ty>) -> Self {
            self.#field_name = ::core::option::Option::Some(value.into());
            self
        }
    };
    let constant = quote! {
        #[doc = #const_doc]
        pub const #const_name: &'static str = #key;
    };
    let metadata = quote! {
        ::kacrab::config::ConfigEntry {
            client: #client_tokens,
            origin: #origin,
            key: #key,
            rust_field: ::core::stringify!(#field_name),
            kafka_type: #kafka_type,
            default: #kafka_default,
            status: #status,
            comment: #comment,
            documentation: "",
            source: #source,
            platforms: &[#(#platforms),*],
            feature: #feature,
        }
    };
    let key_match = quote!(#key);
    let build_field = build_field_tokens(
        attrs.required,
        attrs.default,
        &field_name,
        &key,
        client_tokens,
    )?;
    let property_parse = property_parse_tokens(&field_name, &field_ty, &key, client_tokens);

    Ok(FieldExpansion {
        struct_field,
        builder_field,
        builder_initializer,
        setter,
        build_field,
        constant,
        metadata,
        key_match,
        property_parse,
    })
}

fn property_parse_tokens(
    field_name: &Ident,
    field_ty: &Type,
    key: &LitStr,
    client_tokens: &Tokens,
) -> Tokens {
    quote! {
        if let ::core::option::Option::Some(value) = properties.get(#key) {
            let parsed = <#field_ty as ::kacrab::config::ParseConfigValue>::parse_config_value(value)
                .map_err(|error| ::kacrab::config::ConfigError::InvalidValue {
                    client: #client_tokens,
                    key: #key,
                    target: error.target,
                    value: error.value,
                })?;
            builder = builder.#field_name(parsed);
        }
    }
}

fn build_field_tokens(
    required: bool,
    default: Option<Expr>,
    field_name: &Ident,
    key: &LitStr,
    client_tokens: &Tokens,
) -> Result<Tokens> {
    if required {
        return Ok(quote! {
            #field_name: match self.#field_name {
                ::core::option::Option::Some(value) => value,
                ::core::option::Option::None => {
                    return ::core::result::Result::Err(::kacrab::config::ConfigError::MissingRequired {
                        client: #client_tokens,
                        key: #key,
                    });
                }
            }
        });
    }

    let Some(default) = default else {
        return Err(Error::new_spanned(
            field_name,
            "optional config field must define #[default(...)]",
        ));
    };
    Ok(quote! {
        #field_name: self.#field_name.unwrap_or_else(|| #default)
    })
}

fn parse_client(attrs: &[Attribute]) -> Result<ClientKindAttr> {
    for attr in attrs {
        if !attr.path().is_ident("client") {
            continue;
        }
        let expr = parse_expr_attr(attr, "client")?;
        return parse_client_expr(&expr);
    }
    Err(Error::new(
        Span::call_site(),
        "kafka_config! requires #[client(Producer|Consumer|Admin)]",
    ))
}

fn parse_client_expr(expr: &Expr) -> Result<ClientKindAttr> {
    let Expr::Path(path) = expr else {
        return Err(Error::new_spanned(
            expr,
            "expected Producer, Consumer, or Admin",
        ));
    };
    let Some(ident) = path.path.get_ident() else {
        return Err(Error::new_spanned(
            expr,
            "expected Producer, Consumer, or Admin",
        ));
    };

    match ident.to_string().as_str() {
        "Producer" => Ok(ClientKindAttr::Producer),
        "Consumer" => Ok(ClientKindAttr::Consumer),
        "Admin" => Ok(ClientKindAttr::Admin),
        _ => Err(Error::new_spanned(
            ident,
            "expected Producer, Consumer, or Admin",
        )),
    }
}

fn parse_field_attrs(attrs: &[Attribute]) -> Result<FieldAttrs> {
    let mut key = None;
    let mut required = false;
    let mut default = None;
    let mut kafka_type = None;
    let mut kafka_default = None;
    let mut status = None;
    let mut origin = None;
    let mut source = None;
    let mut platforms = Vec::new();
    let mut feature = None;
    let mut comment = None;

    for attr in attrs {
        if attr.path().is_ident("key") {
            key = Some(parse_lit_str_attr(attr, "key")?);
        } else if attr.path().is_ident("required") {
            parse_marker_attr(attr, "required")?;
            required = true;
        } else if attr.path().is_ident("default") {
            default = Some(parse_expr_attr(attr, "default")?);
        } else if attr.path().is_ident("kafka_type") {
            kafka_type = Some(parse_lit_str_attr(attr, "kafka_type")?);
        } else if attr.path().is_ident("kafka_default") {
            kafka_default = Some(parse_lit_str_attr(attr, "kafka_default")?);
        } else if attr.path().is_ident("status") {
            status = Some(parse_status(attr)?);
        } else if attr.path().is_ident("origin") {
            origin = Some(parse_origin(attr)?);
        } else if attr.path().is_ident("source") {
            source = Some(parse_lit_str_attr(attr, "source")?);
        } else if attr.path().is_ident("platforms") {
            platforms = parse_lit_str_list_attr(attr, "platforms")?;
        } else if attr.path().is_ident("feature") {
            feature = Some(parse_lit_str_attr(attr, "feature")?);
        } else if attr.path().is_ident("comment") {
            comment = Some(parse_lit_str_attr(attr, "comment")?);
        }
    }

    Ok(FieldAttrs {
        key: required_attr(key, "key")?,
        required,
        default,
        kafka_type: required_attr(kafka_type, "kafka_type")?,
        kafka_default: required_attr(kafka_default, "kafka_default")?,
        status: required_attr(status, "status")?,
        origin: origin.unwrap_or(OriginAttr::Kafka),
        source: required_attr(source, "source")?,
        platforms,
        feature,
        comment: required_attr(comment, "comment")?,
    })
}

fn parse_status(attr: &Attribute) -> Result<StatusAttr> {
    let expr = parse_expr_attr(attr, "status")?;
    parse_status_expr(&expr)
}

fn parse_origin(attr: &Attribute) -> Result<OriginAttr> {
    let expr = parse_expr_attr(attr, "origin")?;
    let Expr::Path(path) = &expr else {
        return Err(Error::new_spanned(expr, "expected kafka or kacrab_runtime"));
    };
    let Some(ident) = path.path.get_ident() else {
        return Err(Error::new_spanned(expr, "expected kafka or kacrab_runtime"));
    };
    match ident.to_string().as_str() {
        "kafka" => Ok(OriginAttr::Kafka),
        "kacrab_runtime" => Ok(OriginAttr::KacrabRuntime),
        _ => Err(Error::new_spanned(
            ident,
            "expected kafka or kacrab_runtime",
        )),
    }
}

fn parse_status_expr(expr: &Expr) -> Result<StatusAttr> {
    if let Expr::Path(path) = expr {
        let Some(ident) = path.path.get_ident() else {
            return Err(Error::new_spanned(expr, "unknown config status"));
        };
        return match ident.to_string().as_str() {
            "native" => Ok(StatusAttr::Native),
            "native_review" => Ok(StatusAttr::NativeReview),
            "skip_java_only" => Ok(StatusAttr::SkipJavaOnly),
            _ => Err(Error::new_spanned(ident, "unknown config status")),
        };
    }

    if let Expr::Call(call) = expr
        && let Expr::Path(path) = call.func.as_ref()
        && call.args.len() == 1
        && let Some(Expr::Lit(expr_lit)) = call.args.first()
        && let Lit::Str(feature) = &expr_lit.lit
    {
        if path.path.is_ident("feature_gated") {
            return Ok(StatusAttr::FeatureGated(feature.clone()));
        }
        if path.path.is_ident("future") {
            return Ok(StatusAttr::Future(feature.clone()));
        }
    }

    Err(Error::new_spanned(
        expr,
        "expected status native, native_review, skip_java_only, feature_gated(\"feature\"), or \
         future(\"feature\")",
    ))
}

fn parse_lit_str_attr(attr: &Attribute, name: &str) -> Result<LitStr> {
    let Expr::Lit(ExprLit {
        lit: Lit::Str(value),
        ..
    }) = parse_expr_attr(attr, name)?
    else {
        return Err(Error::new_spanned(
            attr,
            format!("expected #[{name}(\"...\")]"),
        ));
    };
    Ok(value)
}

fn parse_lit_str_list_attr(attr: &Attribute, name: &str) -> Result<Vec<LitStr>> {
    let parser = Punctuated::<LitStr, Token![,]>::parse_terminated;
    match &attr.meta {
        Meta::List(_) => Ok(attr.parse_args_with(parser)?.into_iter().collect()),
        _ => Err(Error::new_spanned(
            attr,
            format!("expected #[{name}(\"...\", ...)]"),
        )),
    }
}

fn parse_expr_attr(attr: &Attribute, name: &str) -> Result<Expr> {
    match &attr.meta {
        Meta::NameValue(meta) => Ok(meta.value.clone()),
        Meta::List(_) => attr.parse_args(),
        Meta::Path(_) => Err(Error::new_spanned(attr, format!("expected #[{name}(...)]"))),
    }
}

fn parse_marker_attr(attr: &Attribute, name: &str) -> Result<()> {
    if matches!(attr.meta, Meta::Path(_)) {
        Ok(())
    } else {
        Err(Error::new_spanned(attr, format!("expected #[{name}]")))
    }
}

fn required_attr<T>(value: Option<T>, name: &str) -> Result<T> {
    value.ok_or_else(|| {
        Error::new(
            Span::call_site(),
            format!("config field requires #[{name}(...)]"),
        )
    })
}

fn client_tokens(client: ClientKindAttr) -> proc_macro2::TokenStream {
    match client {
        ClientKindAttr::Producer => quote!(::kacrab::config::ClientKind::Producer),
        ClientKindAttr::Consumer => quote!(::kacrab::config::ClientKind::Consumer),
        ClientKindAttr::Admin => quote!(::kacrab::config::ClientKind::Admin),
    }
}

const fn client_label(client: ClientKindAttr) -> &'static str {
    match client {
        ClientKindAttr::Producer => "producer",
        ClientKindAttr::Consumer => "consumer",
        ClientKindAttr::Admin => "admin",
    }
}

fn status_tokens(status: StatusAttr) -> proc_macro2::TokenStream {
    match status {
        StatusAttr::Native => quote!(::kacrab::config::ConfigStatus::Native),
        StatusAttr::NativeReview => quote!(::kacrab::config::ConfigStatus::NativeReview),
        StatusAttr::SkipJavaOnly => quote!(::kacrab::config::ConfigStatus::SkipJavaOnly),
        StatusAttr::FeatureGated(feature) => {
            quote!(::kacrab::config::ConfigStatus::FeatureGated { feature: #feature })
        },
        StatusAttr::Future(feature) => {
            quote!(::kacrab::config::ConfigStatus::Future { feature: #feature })
        },
    }
}

fn origin_tokens(origin: OriginAttr) -> proc_macro2::TokenStream {
    match origin {
        OriginAttr::Kafka => quote!(::kacrab::config::ConfigOrigin::Kafka),
        OriginAttr::KacrabRuntime => quote!(::kacrab::config::ConfigOrigin::KacrabRuntime),
    }
}

fn option_lit_str_tokens(value: Option<&LitStr>) -> proc_macro2::TokenStream {
    value.map_or_else(
        || quote!(::core::option::Option::None),
        |value| quote!(::core::option::Option::Some(#value)),
    )
}

fn screaming_snake(value: &str) -> String {
    let mut result = String::with_capacity(value.len());
    for (index, ch) in value.chars().enumerate() {
        if ch.is_ascii_uppercase() && index > 0 {
            result.push('_');
        }
        result.push(ch.to_ascii_uppercase());
    }
    result
}
