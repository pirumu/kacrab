//! Kafka maintainer code generator.
//!
//! Provides one maintainer CLI for generated Kafka protocol messages and
//! generated Kafka client config catalogs.
//!
//! This binary is **maintainer build-tooling**, not a runtime dependency.
//! Downstream users of `kacrab` consume the generated `.rs` files committed
//! under the runtime crates; they never invoke this binary.
//!
//! # Examples
//!
//! Regenerate the protocol crate from a pinned upstream Kafka release:
//!
//! ```text
//! cargo run -p kacrab-codegen -- protocol \
//!     --kafka-ref 4.3.0 \
//!     --output-dir  kacrab-protocol/src/generated \
//!     --schema-snapshot-dir kacrab-codegen/schemas
//! ```
//!
//! Preview a single run without touching the filesystem:
//!
//! ```text
//! cargo run -p kacrab-codegen -- protocol \
//!     --schemas-dir kacrab-codegen/schemas \
//!     --source-ref  apache/kafka@4.3.0 \
//!     --output-dir  /tmp/kacrab-out \
//!     --dry-run
//! ```

use std::{
    collections::HashSet,
    fs,
    path::{Path, PathBuf},
    process::ExitCode,
};

use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand};
use heck::ToSnakeCase;
use kacrab_codegen::{
    codegen, errors_java, format, ir::message::MessageSpec, kafka_config, parser,
    upstream::KafkaSource,
};

/// Maintainer codegen for Kafka protocol messages and config metadata.
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Generate Rust protocol message code from upstream Kafka schemas.
    Protocol(ProtocolCli),
    /// Extract Kafka client config metadata from upstream Java sources.
    Config(ConfigCli),
}

/// Command-line interface for the Kafka schema -> Rust source generator.
///
/// Field-level doc comments are surfaced verbatim in `--help`, so they double
/// as the binary's user-facing documentation. Keep them concise and
/// imperative; put longer prose in a second paragraph (clap renders it under
/// `--help` only, not the short `-h` summary).
#[derive(Args, Debug)]
struct ProtocolCli {
    /// Offline snapshot directory of Kafka message JSON specs.
    ///
    /// Prefer `--kafka-ref` or `--kafka-root` for normal regeneration so
    /// protocol and config come from the same Kafka source tree. This fallback
    /// parses every `*.json` file in this directory without recursing.
    #[arg(long)]
    schemas_dir: Option<PathBuf>,

    /// Upstream Kafka tag or commit SHA to download message specs from.
    ///
    /// When provided, the generator downloads the Kafka source archive and
    /// reads `clients/src/main/resources/common/message` from that archive.
    #[arg(long)]
    kafka_ref: Option<String>,

    /// Local upstream Kafka checkout root.
    ///
    /// The generator derives message schemas and `Errors.java` from this root.
    /// Use with `--source-ref` for reproducible local checkout generation.
    #[arg(long)]
    kafka_root: Option<PathBuf>,

    /// Pinned upstream source ref for local `--kafka-root` or `--schemas-dir` mode.
    ///
    /// Remote `--kafka-ref` mode derives this as `apache/kafka@<ref>`.
    #[arg(long)]
    source_ref: Option<String>,

    /// Comma-separated message names to skip (e.g. `FetchRequest,ProduceRequest`).
    ///
    /// Useful when a message has not yet been wired into the runtime crate
    /// and would otherwise fail to compile after generation.
    #[arg(long)]
    exclude_schemas: Option<String>,

    /// Directory to write generated `.rs` files into.
    ///
    /// Created if it does not exist. Existing files with the same name are
    /// overwritten without prompt — this directory is treated as fully owned
    /// by the generator.
    #[arg(long)]
    output_dir: PathBuf,

    /// Optional directory for generated round-trip test helpers.
    ///
    /// When omitted, no test utilities are emitted.
    #[arg(long)]
    test_utils_dir: Option<PathBuf>,

    /// Optional directory to refresh the bundled upstream JSON schema snapshot.
    ///
    /// The generator copies every upstream `*.json` message spec into this
    /// directory, writes `SOURCE_REF`, writes `VERSION`, and copies
    /// `Errors.java` when the selected source provides one.
    #[arg(long)]
    schema_snapshot_dir: Option<PathBuf>,

    /// Print generated code to stdout instead of writing files.
    #[arg(long)]
    dry_run: bool,

    /// Path to upstream Kafka's `clients/.../Errors.java`.
    ///
    /// When provided, an additional `error_code.rs` module is emitted mirroring
    /// every `(code, name, retriable)` triple from the Java source.
    #[arg(long)]
    errors_java: Option<PathBuf>,
}

/// Kafka config metadata extractor.
#[derive(Args, Debug)]
struct ConfigCli {
    /// Upstream Kafka tag or commit SHA to download from GitHub.
    #[arg(long)]
    kafka_ref: Option<String>,

    /// Local upstream Kafka checkout root.
    ///
    /// The generator derives `clients/src/main/java` from this root. Use with
    /// `--source-ref` for reproducible local checkout generation.
    #[arg(long)]
    kafka_root: Option<PathBuf>,

    /// Pinned upstream source ref, for example `apache/kafka@4.3.0`.
    #[arg(long)]
    source_ref: Option<String>,

    /// JSON file to write.
    #[arg(long)]
    output: PathBuf,

    /// Optional `kacrab/src/config/clients.rs` path used to classify typed native keys.
    #[arg(long)]
    native_schema: Option<PathBuf>,

    /// Optional Kacrab runtime config overlay JSON to merge into the Kafka catalog.
    #[arg(long)]
    runtime_overlay: Option<PathBuf>,

    /// Optional `kacrab/src/config/catalog.rs` path to write generated Rust metadata.
    #[arg(long)]
    rust_catalog_output: Option<PathBuf>,

    /// Print generated JSON to stdout instead of writing `--output`.
    #[arg(long)]
    dry_run: bool,
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    if let Err(err) = run(&cli) {
        eprintln!("error: {err:#}");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}

fn run(cli: &Cli) -> Result<()> {
    match &cli.command {
        Command::Protocol(protocol) => run_protocol(protocol),
        Command::Config(config) => run_config(config),
    }
}

fn run_protocol(cli: &ProtocolCli) -> Result<()> {
    let source = resolve_protocol_source(cli)?;
    let exclude = parse_exclude(cli.exclude_schemas.as_deref());
    let all_specs = parser::parse_all_specs(&source.schemas_dir)
        .with_context(|| format!("parse schemas in {}", source.schemas_dir.display()))?;
    let selected: Vec<&MessageSpec> = all_specs
        .iter()
        .filter(|s| !exclude.contains(s.name.as_str()))
        .collect();

    if let Some(snapshot_dir) = &cli.schema_snapshot_dir {
        emit_schema_snapshot(
            &source.schemas_dir,
            source.errors_java.as_deref(),
            &source.source_ref,
            snapshot_dir,
            cli.dry_run,
        )?;
    }

    if !cli.dry_run {
        fs::create_dir_all(&cli.output_dir)
            .with_context(|| format!("create output dir {}", cli.output_dir.display()))?;
    }

    emit_protocol(
        &selected,
        &all_specs,
        &source.source_ref,
        &cli.output_dir,
        cli.dry_run,
    )?;

    if let Some(test_utils_dir) = &cli.test_utils_dir {
        if !cli.dry_run {
            fs::create_dir_all(test_utils_dir)
                .with_context(|| format!("create test-utils dir {}", test_utils_dir.display()))?;
        }
        emit_test_utils(&selected, test_utils_dir, cli.dry_run)?;
    }

    if let Some(errors_java_path) = source.errors_java.as_deref() {
        emit_errors_java(errors_java_path, &cli.output_dir, cli.dry_run)?;
    }

    Ok(())
}

fn run_config(cli: &ConfigCli) -> Result<()> {
    let source = resolve_config_source(cli)?;
    let document = kafka_config::extract_from_java_root(&source.java_root, &source.source_ref)
        .with_context(|| {
            format!(
                "extract Kafka config metadata from {}",
                source.java_root.display()
            )
        })?;
    let document = merge_runtime_overlay(document, cli.runtime_overlay.as_deref())?;
    let json = serde_json::to_string_pretty(&document).context("serialize config metadata JSON")?;
    let rust_catalog = if cli.rust_catalog_output.is_some() {
        let native_keys = read_native_keys(cli.native_schema.as_deref())?;
        Some(kafka_config::generate_rust_catalog(
            &document,
            &native_keys,
        )?)
    } else {
        None
    };

    if cli.dry_run {
        println!("{json}");
        if let Some(rust_catalog) = rust_catalog {
            println!("// ========== rust catalog ==========");
            print!("{rust_catalog}");
        }
        return Ok(());
    }

    create_parent_dir(&cli.output)?;
    fs::write(&cli.output, json).with_context(|| format!("write {}", cli.output.display()))?;
    if let (Some(path), Some(rust_catalog)) = (&cli.rust_catalog_output, rust_catalog) {
        create_parent_dir(path)?;
        fs::write(path, rust_catalog).with_context(|| format!("write {}", path.display()))?;
    }
    Ok(())
}

fn merge_runtime_overlay(
    document: kafka_config::ConfigCatalogDocument,
    path: Option<&Path>,
) -> Result<kafka_config::ConfigCatalogDocument> {
    let Some(path) = path else {
        return Ok(document);
    };
    let raw = fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    let overlay: kafka_config::RuntimeConfigOverlayDocument = serde_json::from_str(&raw)
        .with_context(|| format!("parse runtime overlay {}", path.display()))?;
    kafka_config::merge_runtime_overlay(document, &overlay)
        .with_context(|| format!("merge runtime overlay {}", path.display()))
}

#[derive(Debug)]
struct ProtocolSource {
    schemas_dir: PathBuf,
    errors_java: Option<PathBuf>,
    source_ref: String,
    _source: Option<KafkaSource>,
}

fn resolve_protocol_source(cli: &ProtocolCli) -> Result<ProtocolSource> {
    match (&cli.schemas_dir, &cli.kafka_ref, &cli.kafka_root) {
        (Some(_), Some(_), _) | (Some(_), _, Some(_)) => {
            anyhow::bail!(
                "pass exactly one source mode: --schemas-dir, --kafka-ref, or --kafka-root"
            )
        },
        (Some(schemas_dir), None, None) => {
            let Some(source_ref) = &cli.source_ref else {
                anyhow::bail!("--source-ref is required when using --schemas-dir");
            };
            Ok(ProtocolSource {
                schemas_dir: schemas_dir.clone(),
                errors_java: cli.errors_java.clone(),
                source_ref: source_ref.clone(),
                _source: None,
            })
        },
        (None, kafka_ref, kafka_root) => {
            let source = KafkaSource::resolve(
                kafka_ref.as_deref(),
                kafka_root.as_deref(),
                cli.source_ref.as_deref(),
            )?;
            let schemas_dir = source.message_schema_root();
            let errors_java = Some(source.errors_java());
            let source_ref = source.source_ref().to_owned();
            Ok(ProtocolSource {
                schemas_dir,
                errors_java,
                source_ref,
                _source: Some(source),
            })
        },
    }
}

#[derive(Debug)]
struct ConfigSource {
    java_root: PathBuf,
    source_ref: String,
    _source: Option<KafkaSource>,
}

fn resolve_config_source(cli: &ConfigCli) -> Result<ConfigSource> {
    let source = KafkaSource::resolve(
        cli.kafka_ref.as_deref(),
        cli.kafka_root.as_deref(),
        cli.source_ref.as_deref(),
    )?;
    let java_root = source.java_root();
    let source_ref = source.source_ref().to_owned();
    Ok(ConfigSource {
        java_root,
        source_ref,
        _source: Some(source),
    })
}

fn read_native_keys(path: Option<&Path>) -> Result<kafka_config::NativeConfigKeys> {
    let Some(path) = path else {
        return Ok(kafka_config::NativeConfigKeys::new());
    };
    let source = fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    Ok(kafka_config::parse_native_config_keys(&source))
}

fn create_parent_dir(path: &Path) -> Result<()> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    }
    Ok(())
}

fn parse_exclude(raw: Option<&str>) -> HashSet<String> {
    raw.map(|s| {
        s.split(',')
            .map(str::trim)
            .filter(|item| !item.is_empty())
            .map(str::to_owned)
            .collect()
    })
    .unwrap_or_default()
}

fn emit_protocol(
    selected: &[&MessageSpec],
    all_specs: &[MessageSpec],
    source_ref: &str,
    output_dir: &Path,
    dry_run: bool,
) -> Result<()> {
    for spec in selected {
        let tokens = codegen::generate_file(spec, all_specs)
            .with_context(|| format!("generate Rust tokens for {}", spec.name))?;
        let formatted = format::pretty(tokens, &spec.name)
            .with_context(|| format!("format generated tokens for {}", spec.name))?;
        write_or_print(output_dir, &spec.name, &formatted, dry_run)?;
    }

    let mod_tokens = codegen::generate_mod_rs(selected, all_specs, source_ref);
    let mod_formatted =
        format::pretty(mod_tokens, "mod").context("format generated module tokens")?;
    if dry_run {
        print_section("mod", &mod_formatted);
    } else {
        let mod_path = sibling_module_file(output_dir);
        fs::write(&mod_path, &mod_formatted)
            .with_context(|| format!("write {}", mod_path.display()))?;
    }
    Ok(())
}

/// Compute the `foo.rs` file path that sits *next to* the `foo/` directory.
///
/// The project follows the modern Rust 2018+ convention where each module
/// directory `foo/` is accompanied by a sibling file `foo.rs` (instead of
/// `foo/mod.rs`). Joins `dir.parent()` with `dir.file_name() + ".rs"` so a
/// trailing separator on `dir` is tolerated.
fn sibling_module_file(dir: &Path) -> PathBuf {
    let parent = dir.parent().unwrap_or_else(|| Path::new(""));
    let mut file_name = dir.file_name().unwrap_or_default().to_owned();
    file_name.push(".rs");
    parent.join(file_name)
}

fn emit_test_utils(selected: &[&MessageSpec], test_utils_dir: &Path, dry_run: bool) -> Result<()> {
    for spec in selected {
        let tokens = codegen::generate_test_utils_file(spec)
            .with_context(|| format!("generate test-utils tokens for {}", spec.name))?;
        let formatted = format::pretty(tokens, &spec.name)
            .with_context(|| format!("format generated test-utils tokens for {}", spec.name))?;
        write_or_print(test_utils_dir, &spec.name, &formatted, dry_run)?;
    }

    let mod_tokens = codegen::generate_test_utils_mod_rs(selected);
    let mod_formatted = format::pretty(mod_tokens, "test-utils mod")
        .context("format generated test-utils module tokens")?;
    if dry_run {
        print_section("test-utils mod", &mod_formatted);
    } else {
        let mod_path = sibling_module_file(test_utils_dir);
        fs::write(&mod_path, &mod_formatted)
            .with_context(|| format!("write {}", mod_path.display()))?;
    }
    Ok(())
}

fn emit_errors_java(java_path: &Path, output_dir: &Path, dry_run: bool) -> Result<()> {
    let entries = errors_java::scrape(java_path)
        .with_context(|| format!("scrape Errors.java at {}", java_path.display()))?;
    let tokens = errors_java::lower(&entries);
    let formatted =
        format::pretty(tokens, "error_code").context("format generated error_code tokens")?;
    if dry_run {
        print_section("error_code", &formatted);
    } else {
        let path = output_dir.join("error_code.rs");
        fs::write(&path, &formatted).with_context(|| format!("write {}", path.display()))?;
    }
    Ok(())
}

fn emit_schema_snapshot(
    source_schemas_dir: &Path,
    errors_java_path: Option<&Path>,
    source_ref: &str,
    snapshot_dir: &Path,
    dry_run: bool,
) -> Result<()> {
    if dry_run {
        eprintln!(
            "dry-run: would refresh schema snapshot {} from {}",
            snapshot_dir.display(),
            source_schemas_dir.display()
        );
        return Ok(());
    }

    fs::create_dir_all(snapshot_dir)
        .with_context(|| format!("create schema snapshot dir {}", snapshot_dir.display()))?;
    remove_snapshot_outputs(snapshot_dir)?;

    let mut schema_paths = Vec::new();
    for entry in fs::read_dir(source_schemas_dir)
        .with_context(|| format!("read schema dir {}", source_schemas_dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) == Some("json") {
            schema_paths.push(path);
        }
    }
    schema_paths.sort();

    for path in schema_paths {
        let file_name = path
            .file_name()
            .ok_or_else(|| anyhow::anyhow!("schema path has no file name: {}", path.display()))?;
        let _bytes_copied = fs::copy(&path, snapshot_dir.join(file_name))
            .with_context(|| format!("copy schema {}", path.display()))?;
    }

    fs::write(snapshot_dir.join("SOURCE_REF"), format!("{source_ref}\n"))
        .with_context(|| format!("write {}", snapshot_dir.join("SOURCE_REF").display()))?;
    fs::write(
        snapshot_dir.join("VERSION"),
        format!("{}\n", source_version_label(source_ref)),
    )
    .with_context(|| format!("write {}", snapshot_dir.join("VERSION").display()))?;

    if let Some(errors_java_path) = errors_java_path {
        let _bytes_copied = fs::copy(errors_java_path, snapshot_dir.join("Errors.java"))
            .with_context(|| format!("copy {}", errors_java_path.display()))?;
    }

    Ok(())
}

fn remove_snapshot_outputs(snapshot_dir: &Path) -> Result<()> {
    for entry in
        fs::read_dir(snapshot_dir).with_context(|| format!("read {}", snapshot_dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        let file_name = path.file_name().and_then(|name| name.to_str());
        let is_owned = path.extension().and_then(|ext| ext.to_str()) == Some("json")
            || matches!(file_name, Some("Errors.java" | "SOURCE_REF" | "VERSION"));
        if is_owned {
            fs::remove_file(&path).with_context(|| format!("remove {}", path.display()))?;
        }
    }
    Ok(())
}

fn source_version_label(source_ref: &str) -> &str {
    source_ref
        .strip_prefix("apache/kafka@")
        .unwrap_or(source_ref)
}

fn write_or_print(dir: &Path, name: &str, formatted: &str, dry_run: bool) -> Result<()> {
    let file_name = format!("{}.rs", name.to_snake_case());
    if dry_run {
        print_section(name, formatted);
    } else {
        let path = dir.join(&file_name);
        fs::write(&path, formatted).with_context(|| format!("write {}", path.display()))?;
    }
    Ok(())
}

fn print_section(label: &str, content: &str) {
    println!("// ========== {label} ==========");
    print!("{content}");
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::{ConfigCli, ProtocolCli, resolve_config_source, resolve_protocol_source};

    fn protocol_cli() -> ProtocolCli {
        ProtocolCli {
            schemas_dir: None,
            kafka_ref: None,
            kafka_root: None,
            source_ref: None,
            exclude_schemas: None,
            output_dir: PathBuf::from("generated"),
            test_utils_dir: None,
            schema_snapshot_dir: None,
            dry_run: false,
            errors_java: None,
        }
    }

    fn config_cli() -> ConfigCli {
        ConfigCli {
            kafka_ref: None,
            kafka_root: None,
            source_ref: None,
            output: PathBuf::from("out.json"),
            native_schema: None,
            runtime_overlay: None,
            rust_catalog_output: None,
            dry_run: false,
        }
    }

    #[test]
    fn protocol_source_requires_one_input_mode() {
        let cli = protocol_cli();

        let error = resolve_protocol_source(&cli).expect_err("missing source should fail");

        assert!(
            error.to_string().contains("--kafka-ref"),
            "error should point maintainers at source selection flags"
        );
    }

    #[test]
    fn protocol_source_modes_are_mutually_exclusive() {
        let mut cli = protocol_cli();
        cli.schemas_dir = Some(PathBuf::from("kacrab-codegen/schemas"));
        cli.kafka_ref = Some("4.3.0".to_owned());

        let error = resolve_protocol_source(&cli).expect_err("ambiguous source should fail");

        assert!(
            error.to_string().contains("exactly one"),
            "error should reject ambiguous source selection"
        );
    }

    #[test]
    fn protocol_kafka_root_derives_standard_paths() {
        let mut cli = protocol_cli();
        cli.kafka_root = Some(PathBuf::from("/tmp/kafka"));
        cli.source_ref = Some("apache/kafka@local-sha".to_owned());

        let source = resolve_protocol_source(&cli).expect("local Kafka root should resolve");

        assert_eq!(
            source.schemas_dir,
            PathBuf::from("/tmp/kafka/clients/src/main/resources/common/message")
        );
        assert_eq!(
            source.errors_java,
            Some(PathBuf::from(
                "/tmp/kafka/clients/src/main/java/org/apache/kafka/common/protocol/Errors.java"
            ))
        );
        assert_eq!(source.source_ref, "apache/kafka@local-sha");
    }

    #[test]
    fn protocol_local_source_requires_source_ref() {
        let mut cli = protocol_cli();
        cli.schemas_dir = Some(PathBuf::from("kacrab-codegen/schemas"));

        let error = resolve_protocol_source(&cli).expect_err("local source should fail");

        assert!(
            error.to_string().contains("--source-ref"),
            "error should keep local schema generation reproducible"
        );
    }

    #[test]
    fn protocol_local_source_keeps_optional_errors_java() {
        let mut cli = protocol_cli();
        cli.schemas_dir = Some(PathBuf::from("kacrab-codegen/schemas"));
        cli.source_ref = Some("apache/kafka@4.3.0".to_owned());
        cli.errors_java = Some(PathBuf::from("kacrab-codegen/schemas/Errors.java"));

        let source = resolve_protocol_source(&cli).expect("local source should resolve");

        assert_eq!(source.schemas_dir, PathBuf::from("kacrab-codegen/schemas"));
        assert_eq!(source.source_ref, "apache/kafka@4.3.0");
        assert_eq!(
            source.errors_java,
            Some(PathBuf::from("kacrab-codegen/schemas/Errors.java"))
        );
    }

    #[test]
    fn config_local_source_requires_source_ref() {
        let mut cli = config_cli();
        cli.kafka_root = Some(PathBuf::from("/tmp/kafka"));

        let error = resolve_config_source(&cli).expect_err("local mode should require source ref");

        assert!(
            error.to_string().contains("--source-ref"),
            "error should tell maintainers how to make local mode reproducible"
        );
    }

    #[test]
    fn config_source_modes_are_mutually_exclusive() {
        let mut cli = config_cli();
        cli.kafka_root = Some(PathBuf::from("/tmp/kafka"));
        cli.kafka_ref = Some("4.3.0".to_owned());
        cli.source_ref = Some("apache/kafka@4.3.0".to_owned());

        let error = resolve_config_source(&cli).expect_err("source modes should be exclusive");

        assert!(
            error.to_string().contains("exactly one"),
            "error should reject ambiguous source selection"
        );
    }

    #[test]
    fn config_local_source_keeps_explicit_source_ref() {
        let mut cli = config_cli();
        cli.kafka_root = Some(PathBuf::from("/tmp/kafka"));
        cli.source_ref = Some("apache/kafka@4.3.0".to_owned());

        let source = resolve_config_source(&cli).expect("local mode should resolve");

        assert_eq!(
            source.java_root,
            PathBuf::from("/tmp/kafka/clients/src/main/java")
        );
        assert_eq!(source.source_ref, "apache/kafka@4.3.0");
    }
}
