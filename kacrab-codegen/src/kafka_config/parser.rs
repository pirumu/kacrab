//! Java source parser for Kafka `ConfigDef.define(...)` declarations.

use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    sync::OnceLock,
};

use regex::Regex;

use super::{
    error::{KafkaConfigError, KafkaConfigErrorKind},
    model::{
        ConfigCatalogDocument, ConfigClientDocument, ConfigKeyDocument, ConfigValueDefault,
        JavaConfigType, KafkaConfigClient,
    },
};

const CLIENT_CLASSES: &[(KafkaConfigClient, &str)] = &[
    (KafkaConfigClient::Producer, "ProducerConfig"),
    (KafkaConfigClient::Consumer, "ConsumerConfig"),
    (KafkaConfigClient::Admin, "AdminClientConfig"),
];

const HELPER_CLASSES: &[HelperClass] = &[
    HelperClass {
        method: "withClientSslSupport",
        class_name: "SslConfigs",
    },
    HelperClass {
        method: "withClientSaslSupport",
        class_name: "SaslConfigs",
    },
];

#[derive(Clone, Copy, Debug)]
struct HelperClass {
    method: &'static str,
    class_name: &'static str,
}

/// Extract config metadata from every Java source under `java_root`.
pub fn extract_from_java_root(
    java_root: &Path,
    source_ref: &str,
) -> Result<ConfigCatalogDocument, KafkaConfigError> {
    extract_from_java_root_inner(java_root, source_ref).map_err(KafkaConfigError::new)
}

fn extract_from_java_root_inner(
    java_root: &Path,
    source_ref: &str,
) -> Result<ConfigCatalogDocument, KafkaConfigErrorKind> {
    if source_ref.trim().is_empty() {
        return Err(KafkaConfigErrorKind::EmptySourceRef);
    }

    let sources = read_java_sources(java_root)?;
    if sources.is_empty() {
        return Err(KafkaConfigErrorKind::EmptyJavaRoot {
            root: java_root.to_path_buf(),
        });
    }
    extract_from_sources_inner(&sources, source_ref)
}

/// Extract config metadata from an in-memory set of Java sources.
pub fn extract_from_sources(
    sources: &BTreeMap<String, String>,
    source_ref: &str,
) -> Result<ConfigCatalogDocument, KafkaConfigError> {
    extract_from_sources_inner(sources, source_ref).map_err(KafkaConfigError::new)
}

fn extract_from_sources_inner(
    sources: &BTreeMap<String, String>,
    source_ref: &str,
) -> Result<ConfigCatalogDocument, KafkaConfigErrorKind> {
    if source_ref.trim().is_empty() {
        return Err(KafkaConfigErrorKind::EmptySourceRef);
    }

    let constants = collect_string_constants(sources);
    let mut clients = Vec::with_capacity(CLIENT_CLASSES.len());
    for (client, class_name) in CLIENT_CLASSES {
        let source = find_source_by_class(sources, class_name).ok_or(
            KafkaConfigErrorKind::MissingClientSource {
                client: *client,
                class_name,
            },
        )?;
        clients.push(parse_client_config_inner(
            *client,
            class_name,
            source,
            Some(sources),
            &constants,
        )?);
    }

    Ok(ConfigCatalogDocument {
        source_ref: source_ref.to_owned(),
        clients,
    })
}

/// Parse one Java client config source file.
pub fn parse_client_config(
    client: KafkaConfigClient,
    java_class: &str,
    source: &str,
) -> Result<ConfigClientDocument, KafkaConfigError> {
    let mut sources = BTreeMap::new();
    let previous = sources.insert(java_class.to_owned(), source.to_owned());
    debug_assert!(
        previous.is_none(),
        "single-source parser should not replace an existing fixture"
    );
    let constants = collect_string_constants(&sources);
    parse_client_config_inner(client, java_class, source, None, &constants)
        .map_err(KafkaConfigError::new)
}

fn parse_client_config_inner(
    client: KafkaConfigClient,
    java_class: &str,
    source: &str,
    sources: Option<&BTreeMap<String, String>>,
    constants: &BTreeMap<String, String>,
) -> Result<ConfigClientDocument, KafkaConfigErrorKind> {
    let mut configs = parse_define_entries(java_class, source, constants)?;
    if let Some(sources) = sources {
        append_helper_configs(&mut configs, source, sources, constants)?;
    }

    Ok(ConfigClientDocument {
        client,
        java_class: java_class.to_owned(),
        configs,
    })
}

fn append_helper_configs(
    configs: &mut Vec<ConfigKeyDocument>,
    client_source: &str,
    sources: &BTreeMap<String, String>,
    constants: &BTreeMap<String, String>,
) -> Result<(), KafkaConfigErrorKind> {
    for helper in HELPER_CLASSES {
        let call = format!(".{}()", helper.method);
        if !client_source.contains(&call) {
            continue;
        }
        let helper_source = find_source_by_class(sources, helper.class_name).ok_or(
            KafkaConfigErrorKind::MissingHelperSource {
                helper_method: helper.method,
                class_name: helper.class_name,
            },
        )?;
        configs.extend(parse_define_entries(
            helper.class_name,
            helper_source,
            constants,
        )?);
    }
    Ok(())
}

fn parse_define_entries(
    java_class: &str,
    source: &str,
    constants: &BTreeMap<String, String>,
) -> Result<Vec<ConfigKeyDocument>, KafkaConfigErrorKind> {
    let mut configs = Vec::new();
    for body in define_bodies(source) {
        let args = split_java_args(body);
        if args.len() < 2 {
            return Err(KafkaConfigErrorKind::MalformedDefine {
                raw: body.to_owned(),
            });
        }

        let Some(name_arg) = args.first() else {
            return Err(KafkaConfigErrorKind::MalformedDefine {
                raw: body.to_owned(),
            });
        };
        let Some(type_arg) = args.get(1) else {
            return Err(KafkaConfigErrorKind::MalformedDefine {
                raw: body.to_owned(),
            });
        };
        let java_constant = name_arg.trim().to_owned();
        let key = resolve_string_token(&java_constant, java_class, constants)
            .unwrap_or_else(|| java_constant.clone());
        let java_type = parse_java_type(type_arg.trim())?;
        let default = default_arg(&args).map(parse_default_token);
        let importance = args.iter().find_map(|arg| parse_importance(arg.trim()));
        let documentation = args
            .iter()
            .skip_while(|arg| parse_importance(arg.trim()).is_none())
            .skip(1)
            .find_map(|arg| resolve_string_token(arg.trim(), java_class, constants));

        configs.push(ConfigKeyDocument {
            origin: super::model::ConfigOrigin::Kafka,
            key,
            java_constant,
            rust_field: None,
            java_type,
            default,
            importance,
            documentation,
            platforms: Vec::new(),
            feature: None,
        });
    }
    Ok(configs)
}

fn read_java_sources(root: &Path) -> Result<BTreeMap<String, String>, KafkaConfigErrorKind> {
    let mut paths = Vec::new();
    collect_java_paths(root, &mut paths)?;
    paths.sort();

    let mut sources = BTreeMap::new();
    for path in paths {
        let content = fs::read_to_string(&path)?;
        let key = path.to_string_lossy().into_owned();
        let previous = sources.insert(key, content);
        debug_assert!(
            previous.is_none(),
            "recursive Java source walk should not visit the same path twice"
        );
    }
    Ok(sources)
}

fn collect_java_paths(root: &Path, paths: &mut Vec<PathBuf>) -> Result<(), KafkaConfigErrorKind> {
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_java_paths(&path, paths)?;
        } else if path.extension().is_some_and(|ext| ext == "java") {
            paths.push(path);
        }
    }
    Ok(())
}

fn find_source_by_class<'a>(
    sources: &'a BTreeMap<String, String>,
    class_name: &str,
) -> Option<&'a str> {
    let file_suffix = format!("{class_name}.java");
    sources
        .iter()
        .find(|(path, _)| path.ends_with(&file_suffix) || path.as_str() == class_name)
        .map(|(_, source)| source.as_str())
}

fn collect_string_constants(sources: &BTreeMap<String, String>) -> BTreeMap<String, String> {
    let mut raw_constants = Vec::new();
    for (path, source) in sources {
        let class_name = path
            .rsplit(['/', '\\'])
            .next()
            .and_then(|name| name.strip_suffix(".java"))
            .unwrap_or(path);

        for cap in string_constant_re().captures_iter(source) {
            let Some(name_match) = cap.get(1) else {
                continue;
            };
            let Some(decl_match) = cap.get(0) else {
                continue;
            };
            let Some(expr_end) = java_statement_end(source, decl_match.end()) else {
                continue;
            };
            let Some(expr) = source.get(decl_match.end()..expr_end) else {
                continue;
            };
            raw_constants.push((
                class_name.to_owned(),
                name_match.as_str().to_owned(),
                expr.trim().to_owned(),
            ));
        }
    }

    let mut constants = BTreeMap::new();
    for _ in 0..raw_constants.len() {
        let before = constants.len();
        for (class_name, name, expr) in &raw_constants {
            let qualified_name = format!("{class_name}.{name}");
            if constants.contains_key(name) && constants.contains_key(&qualified_name) {
                continue;
            }
            let Some(value) = eval_string_expr(expr, class_name, &constants) else {
                continue;
            };
            if !constants.contains_key(name) {
                let previous = constants.insert(name.clone(), value.clone());
                debug_assert!(
                    previous.is_none(),
                    "constant was checked as absent before insertion"
                );
            }
            let previous = constants.insert(qualified_name, value);
            debug_assert!(
                previous.is_none(),
                "qualified Java constants should be unique per class"
            );
        }
        if constants.len() == before {
            break;
        }
    }
    constants
}

#[expect(
    clippy::arithmetic_side_effects,
    clippy::string_slice,
    reason = "Java source is scanned by byte offsets that are produced from ASCII delimiters."
)]
fn define_bodies(source: &str) -> Vec<&str> {
    let mut bodies = Vec::new();
    let mut search_from = 0;
    while let Some(relative_start) = source[search_from..].find(".define(") {
        let open = search_from + relative_start + ".define".len();
        if let Some(close) = matching_paren(source, open) {
            bodies.push(&source[open + 1..close]);
            search_from = close + 1;
        } else {
            break;
        }
    }
    bodies
}

fn matching_paren(source: &str, open: usize) -> Option<usize> {
    let bytes = source.as_bytes();
    if bytes.get(open).copied() != Some(b'(') {
        return None;
    }

    let mut depth = 0usize;
    let mut in_string = false;
    let mut escaped = false;
    for (index, byte) in bytes.iter().enumerate().skip(open) {
        if in_string {
            if escaped {
                escaped = false;
            } else if *byte == b'\\' {
                escaped = true;
            } else if *byte == b'"' {
                in_string = false;
            }
            continue;
        }

        match *byte {
            b'"' => in_string = true,
            b'(' => depth = depth.saturating_add(1),
            b')' => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return Some(index);
                }
            },
            _ => {},
        }
    }
    None
}

#[expect(
    clippy::arithmetic_side_effects,
    clippy::string_slice,
    reason = "argument boundaries are byte offsets produced from ASCII Java delimiters."
)]
fn split_java_args(body: &str) -> Vec<String> {
    let mut args = Vec::new();
    let mut start = 0usize;
    let mut paren_depth = 0usize;
    let mut angle_depth = 0usize;
    let mut in_string = false;
    let mut escaped = false;

    for (index, byte) in body.bytes().enumerate() {
        if in_string {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                in_string = false;
            }
            continue;
        }

        match byte {
            b'"' => in_string = true,
            b'(' => paren_depth = paren_depth.saturating_add(1),
            b')' => paren_depth = paren_depth.saturating_sub(1),
            b'<' => angle_depth = angle_depth.saturating_add(1),
            b'>' => angle_depth = angle_depth.saturating_sub(1),
            b',' if paren_depth == 0 && angle_depth == 0 => {
                args.push(body[start..index].trim().to_owned());
                start = index + 1;
            },
            _ => {},
        }
    }
    args.push(body[start..].trim().to_owned());
    args
}

fn parse_java_type(raw: &str) -> Result<JavaConfigType, KafkaConfigErrorKind> {
    let type_name = raw
        .strip_prefix("Type.")
        .or_else(|| raw.strip_prefix("ConfigDef.Type."))
        .unwrap_or(raw);
    match type_name {
        "BOOLEAN" => Ok(JavaConfigType::Boolean),
        "SHORT" => Ok(JavaConfigType::Short),
        "INT" => Ok(JavaConfigType::Int),
        "LONG" => Ok(JavaConfigType::Long),
        "DOUBLE" => Ok(JavaConfigType::Double),
        "STRING" => Ok(JavaConfigType::String),
        "LIST" => Ok(JavaConfigType::List),
        "CLASS" => Ok(JavaConfigType::Class),
        "PASSWORD" => Ok(JavaConfigType::Password),
        other => Err(KafkaConfigErrorKind::UnsupportedType {
            raw: other.to_owned(),
        }),
    }
}

fn parse_importance(raw: &str) -> Option<String> {
    raw.strip_prefix("Importance.")
        .or_else(|| raw.strip_prefix("ConfigDef.Importance."))
        .map(str::to_owned)
}

fn default_arg(args: &[String]) -> Option<&str> {
    let candidate = args.get(2)?.trim();
    if candidate.starts_with("Importance.") {
        return None;
    }
    Some(candidate)
}

fn parse_default_token(raw: &str) -> ConfigValueDefault {
    match raw {
        "null" => ConfigValueDefault::Null,
        "true" => ConfigValueDefault::Boolean(true),
        "false" => ConfigValueDefault::Boolean(false),
        _ => raw.parse::<i64>().map_or_else(
            |_| parse_non_integer_default(raw),
            ConfigValueDefault::Integer,
        ),
    }
}

fn parse_non_integer_default(raw: &str) -> ConfigValueDefault {
    if raw.starts_with('"') && raw.ends_with('"') {
        return ConfigValueDefault::String(unescape_java_string(raw));
    }
    ConfigValueDefault::Symbol(raw.to_owned())
}

fn resolve_string_token(
    token: &str,
    current_class: &str,
    constants: &BTreeMap<String, String>,
) -> Option<String> {
    if token.starts_with('"') && token.ends_with('"') {
        return Some(unescape_java_string(token));
    }
    if !token.contains('.') {
        let qualified_token = format!("{current_class}.{token}");
        if let Some(value) = constants.get(&qualified_token) {
            return Some(value.clone());
        }
    }
    constants.get(token).cloned()
}

fn eval_string_expr(
    expr: &str,
    current_class: &str,
    constants: &BTreeMap<String, String>,
) -> Option<String> {
    let mut out = String::new();
    for part in split_java_concat(expr) {
        let trimmed = part.trim();
        let value = resolve_string_token(trimmed, current_class, constants)
            .or_else(|| parse_scalar_literal(trimmed))?;
        out.push_str(&value);
    }
    Some(out)
}

fn parse_scalar_literal(raw: &str) -> Option<String> {
    match raw {
        "true" | "false" => return Some(raw.to_owned()),
        _ => {},
    }

    let numeric = raw
        .strip_suffix('L')
        .or_else(|| raw.strip_suffix('l'))
        .unwrap_or(raw);
    if numeric.parse::<i64>().is_ok() || numeric.parse::<f64>().is_ok() {
        return Some(numeric.to_owned());
    }
    None
}

#[expect(
    clippy::arithmetic_side_effects,
    clippy::string_slice,
    reason = "concat boundaries are byte offsets produced from ASCII Java delimiters."
)]
fn split_java_concat(expr: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut start = 0usize;
    let mut in_string = false;
    let mut escaped = false;

    for (index, byte) in expr.bytes().enumerate() {
        if in_string {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                in_string = false;
            }
            continue;
        }

        match byte {
            b'"' => in_string = true,
            b'+' => {
                parts.push(&expr[start..index]);
                start = index + 1;
            },
            _ => {},
        }
    }
    parts.push(&expr[start..]);
    parts
}

fn java_statement_end(source: &str, start: usize) -> Option<usize> {
    let mut in_string = false;
    let mut escaped = false;

    for (index, byte) in source.bytes().enumerate().skip(start) {
        if in_string {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                in_string = false;
            }
            continue;
        }

        match byte {
            b'"' => in_string = true,
            b';' => return Some(index),
            _ => {},
        }
    }
    None
}

fn unescape_java_string(raw: &str) -> String {
    let inner = raw
        .strip_prefix('"')
        .and_then(|s| s.strip_suffix('"'))
        .unwrap_or(raw);
    let mut out = String::with_capacity(inner.len());
    let mut chars = inner.chars();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            out.push(ch);
            continue;
        }
        match chars.next() {
            Some('n') => out.push('\n'),
            Some('r') => out.push('\r'),
            Some('t') => out.push('\t'),
            Some('"') => out.push('"'),
            Some('\\') | None => out.push('\\'),
            Some(other) => {
                out.push('\\');
                out.push(other);
            },
        }
    }
    out
}

#[expect(
    clippy::expect_used,
    reason = "regex pattern is a static string literal; a parse failure is a developer error \
              caught by parser tests."
)]
fn string_constant_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(
            r"(?:public|private|protected)?\s*static\s+final\s+(?:String|int|long|short|boolean|double)\s+([A-Z][A-Z0-9_]*)\s*=",
        )
        .expect("hand-written regex compiles")
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{
        ConfigValueDefault, JavaConfigType, KafkaConfigClient, extract_from_sources,
        parse_client_config,
    };

    #[test]
    fn parses_config_def_define_calls() {
        let source = r#"
            public class ProducerConfig {
                public static final String BOOTSTRAP_SERVERS_CONFIG = "bootstrap.servers";
                private static final String BOOTSTRAP_SERVERS_DOC = "Host list.";
                public static final String ACKS_CONFIG = "acks";
                private static final String ACKS_DOC = "Required acknowledgments.";

                static {
                    CONFIG = new ConfigDef()
                        .define(BOOTSTRAP_SERVERS_CONFIG, Type.LIST, Importance.HIGH, BOOTSTRAP_SERVERS_DOC)
                        .define(ACKS_CONFIG, Type.STRING, "all", Importance.LOW, ACKS_DOC);
                }
            }
        "#;

        let parsed = parse_client_config(KafkaConfigClient::Producer, "ProducerConfig", source)
            .expect("fixture should parse");

        assert_eq!(parsed.configs.len(), 2, "expected both define calls");
        assert_eq!(parsed.configs[0].key, "bootstrap.servers");
        assert_eq!(parsed.configs[0].java_type, JavaConfigType::List);
        assert_eq!(parsed.configs[0].default, None);
        assert_eq!(parsed.configs[0].importance.as_deref(), Some("HIGH"));
        assert_eq!(
            parsed.configs[0].documentation.as_deref(),
            Some("Host list.")
        );

        assert_eq!(parsed.configs[1].key, "acks");
        assert_eq!(parsed.configs[1].java_type, JavaConfigType::String);
        assert_eq!(
            parsed.configs[1].default,
            Some(ConfigValueDefault::String("all".to_owned())),
            "string literal defaults should be normalized"
        );
        assert_eq!(parsed.configs[1].importance.as_deref(), Some("LOW"));
        assert_eq!(
            parsed.configs[1].documentation.as_deref(),
            Some("Required acknowledgments.")
        );
    }

    #[test]
    fn resolves_cross_file_string_constants() {
        let mut sources = BTreeMap::new();
        assert!(
            sources
                .insert(
                    "CommonClientConfigs.java".to_owned(),
                    r#"
                        public class CommonClientConfigs {
                            public static final String CLIENT_ID_CONFIG = "client.id";
                            public static final String CLIENT_ID_DOC = "Client " + "id.";
                        }
                    "#
                    .to_owned(),
                )
                .is_none(),
            "fixture paths should be unique"
        );
        assert!(
            sources
                .insert(
                    "ProducerConfig.java".to_owned(),
                    r#"
                        public class ProducerConfig {
                            static {
                                CONFIG = new ConfigDef()
                                    .define(CommonClientConfigs.CLIENT_ID_CONFIG, Type.STRING, "", Importance.LOW, CommonClientConfigs.CLIENT_ID_DOC);
                            }
                        }
                    "#
                    .to_owned(),
                )
                .is_none(),
            "fixture paths should be unique"
        );
        assert!(
            sources
                .insert(
                    "ConsumerConfig.java".to_owned(),
                    "public class ConsumerConfig {}".to_owned(),
                )
                .is_none(),
            "fixture paths should be unique"
        );
        assert!(
            sources
                .insert(
                    "AdminClientConfig.java".to_owned(),
                    "public class AdminClientConfig {}".to_owned(),
                )
                .is_none(),
            "fixture paths should be unique"
        );

        let parsed =
            extract_from_sources(&sources, "apache/kafka@4.3.0").expect("fixture should parse");
        let producer = parsed
            .clients
            .iter()
            .find(|client| client.client == KafkaConfigClient::Producer)
            .expect("producer client should exist");
        let config = producer
            .configs
            .first()
            .expect("producer config should be parsed");

        assert_eq!(config.key, "client.id");
        assert_eq!(config.documentation.as_deref(), Some("Client id."));
    }

    #[test]
    fn expands_known_config_def_helpers() {
        let mut sources = BTreeMap::new();
        assert!(
            sources
                .insert(
                    "ProducerConfig.java".to_owned(),
                    r#"
                        public class ProducerConfig {
                            public static final String BOOTSTRAP_SERVERS_CONFIG = "bootstrap.servers";
                            public static final String BOOTSTRAP_SERVERS_DOC = "Bootstrap servers.";

                            static {
                                CONFIG = new ConfigDef()
                                    .define(BOOTSTRAP_SERVERS_CONFIG, Type.LIST, Importance.HIGH, BOOTSTRAP_SERVERS_DOC)
                                    .withClientSslSupport();
                            }
                        }
                    "#
                    .to_owned(),
                )
                .is_none(),
            "fixture paths should be unique"
        );
        assert!(
            sources
                .insert(
                    "SslConfigs.java".to_owned(),
                    r#"
                        public class SslConfigs {
                            public static final String SSL_PROTOCOL_CONFIG = "ssl.protocol";
                            public static final String DEFAULT_SSL_PROTOCOL = "TLSv1.3";
                            public static final String SSL_PROTOCOL_DOC = "SSL protocol.";

                            public static void addClientSslSupport(ConfigDef config) {
                                config.define(SslConfigs.SSL_PROTOCOL_CONFIG, ConfigDef.Type.STRING, SslConfigs.DEFAULT_SSL_PROTOCOL, ConfigDef.Importance.MEDIUM, SslConfigs.SSL_PROTOCOL_DOC);
                            }
                        }
                    "#
                    .to_owned(),
                )
                .is_none(),
            "fixture paths should be unique"
        );
        assert!(
            sources
                .insert(
                    "ConsumerConfig.java".to_owned(),
                    "public class ConsumerConfig {}".to_owned(),
                )
                .is_none(),
            "fixture paths should be unique"
        );
        assert!(
            sources
                .insert(
                    "AdminClientConfig.java".to_owned(),
                    "public class AdminClientConfig {}".to_owned(),
                )
                .is_none(),
            "fixture paths should be unique"
        );

        let parsed =
            extract_from_sources(&sources, "apache/kafka@4.3.0").expect("fixture should parse");
        let producer = parsed
            .clients
            .iter()
            .find(|client| client.client == KafkaConfigClient::Producer)
            .expect("producer client should exist");

        assert!(
            producer
                .configs
                .iter()
                .any(|config| config.key == "ssl.protocol"),
            "producer config should include helper-provided SSL keys"
        );
    }
}
