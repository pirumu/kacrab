//! Helpers for resolving pinned upstream Kafka source trees.

use std::{
    env, fs,
    path::{Path, PathBuf},
    process::Command,
    sync::atomic::{AtomicU64, Ordering},
};

use anyhow::{Context, Result};

/// Temporary upstream Kafka checkout/archive extraction.
#[derive(Debug)]
pub struct KafkaSource {
    root: PathBuf,
    source_ref: String,
    _temp_dir: Option<TempDir>,
}

impl KafkaSource {
    /// Use an existing local Kafka checkout.
    pub fn local(root: impl Into<PathBuf>, source_ref: impl Into<String>) -> Self {
        Self {
            root: root.into(),
            source_ref: source_ref.into(),
            _temp_dir: None,
        }
    }

    /// Download and extract `apache/kafka` source for one tag or commit SHA.
    pub fn remote(kafka_ref: &str) -> Result<Self> {
        let temp_dir = download_kafka_archive(kafka_ref)?;
        let root = find_archive_root(temp_dir.path()).with_context(|| {
            format!("find extracted Kafka source root for upstream ref {kafka_ref}")
        })?;
        Ok(Self {
            root,
            source_ref: format!("apache/kafka@{kafka_ref}"),
            _temp_dir: Some(temp_dir),
        })
    }

    /// Resolve exactly one Kafka source tree mode.
    ///
    /// Remote mode downloads `apache/kafka` from GitHub by tag/SHA. Local mode
    /// uses an existing Kafka checkout root and requires an explicit source ref
    /// so generated artifacts stay reproducible.
    pub fn resolve(
        kafka_ref: Option<&str>,
        kafka_root: Option<&Path>,
        source_ref: Option<&str>,
    ) -> Result<Self> {
        match (kafka_ref, kafka_root) {
            (Some(_), Some(_)) => anyhow::bail!("pass exactly one of --kafka-ref or --kafka-root"),
            (None, None) => anyhow::bail!("pass one of --kafka-ref or --kafka-root"),
            (Some(kafka_ref), None) => Self::remote(kafka_ref),
            (None, Some(kafka_root)) => {
                let Some(source_ref) = source_ref else {
                    anyhow::bail!("--source-ref is required when using --kafka-root");
                };
                Ok(Self::local(kafka_root, source_ref))
            },
        }
    }

    /// Pinned source identifier stored in generated artifacts.
    pub fn source_ref(&self) -> &str {
        &self.source_ref
    }

    /// `clients/src/main/java` root.
    pub fn java_root(&self) -> PathBuf {
        self.root.join("clients/src/main/java")
    }

    /// `clients/src/main/resources/common/message` root.
    pub fn message_schema_root(&self) -> PathBuf {
        self.root.join("clients/src/main/resources/common/message")
    }

    /// Upstream `Errors.java` path.
    pub fn errors_java(&self) -> PathBuf {
        self.root
            .join("clients/src/main/java/org/apache/kafka/common/protocol/Errors.java")
    }
}

fn download_kafka_archive(kafka_ref: &str) -> Result<TempDir> {
    let temp_dir = TempDir::new("kacrab-kafka-source")?;
    let archive_path = temp_dir.path().join("kafka.tar.gz");
    let url = format!("https://codeload.github.com/apache/kafka/tar.gz/{kafka_ref}");

    run_command(
        Command::new("curl")
            .arg("--fail")
            .arg("--location")
            .arg("--silent")
            .arg("--show-error")
            .arg("--output")
            .arg(&archive_path)
            .arg(&url),
        "download Kafka source archive",
    )?;
    run_command(
        Command::new("tar")
            .arg("-xzf")
            .arg(&archive_path)
            .arg("-C")
            .arg(temp_dir.path()),
        "extract Kafka source archive",
    )?;

    Ok(temp_dir)
}

fn run_command(command: &mut Command, action: &str) -> Result<()> {
    let status = command
        .status()
        .with_context(|| format!("{action} command"))?;
    if !status.success() {
        anyhow::bail!("{action} failed with status {status}");
    }
    Ok(())
}

fn find_archive_root(root: &Path) -> Result<PathBuf> {
    if root.join("clients/src/main").is_dir() {
        return Ok(root.to_path_buf());
    }

    for entry in fs::read_dir(root).with_context(|| format!("read {}", root.display()))? {
        let entry = entry?;
        let path = entry.path();
        if path.join("clients/src/main").is_dir() {
            return Ok(path);
        }
    }

    anyhow::bail!("Kafka source root not found under {}", root.display())
}

#[derive(Debug)]
struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new(prefix: &str) -> Result<Self> {
        static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(0);
        let id = NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed);
        let path = env::temp_dir().join(format!("{prefix}-{}-{id}", std::process::id()));
        if path.exists() {
            fs::remove_dir_all(&path)
                .with_context(|| format!("remove stale {}", path.display()))?;
        }
        fs::create_dir_all(&path).with_context(|| format!("create {}", path.display()))?;
        Ok(Self { path })
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ignored = fs::remove_dir_all(&self.path);
    }
}

#[cfg(test)]
mod tests {
    use super::{KafkaSource, TempDir, find_archive_root};

    #[test]
    fn finds_extracted_archive_root() {
        let temp = TempDir::new("kacrab-kafka-source-test").expect("temp dir should be created");
        let source_root = temp.path().join("kafka-4.3.0");
        std::fs::create_dir_all(source_root.join("clients/src/main/resources/common/message"))
            .expect("message schema root should be created");

        let found = find_archive_root(temp.path()).expect("archive root should be found");

        assert_eq!(found, source_root);
    }

    #[test]
    fn local_source_derives_standard_subpaths() {
        let source = KafkaSource::local("/tmp/kafka", "apache/kafka@4.3.0");

        assert_eq!(source.source_ref(), "apache/kafka@4.3.0");
        assert_eq!(
            source.message_schema_root(),
            std::path::PathBuf::from("/tmp/kafka/clients/src/main/resources/common/message")
        );
        assert_eq!(
            source.errors_java(),
            std::path::PathBuf::from(
                "/tmp/kafka/clients/src/main/java/org/apache/kafka/common/protocol/Errors.java"
            )
        );
    }

    #[test]
    fn resolve_requires_one_source_mode() {
        let error = KafkaSource::resolve(None, None, None).expect_err("missing source should fail");

        assert!(
            error.to_string().contains("--kafka-ref"),
            "error should mention supported Kafka source flags"
        );
    }

    #[test]
    fn resolve_rejects_ambiguous_source_mode() {
        let error = KafkaSource::resolve(
            Some("4.3.0"),
            Some(std::path::Path::new("/tmp/kafka")),
            Some("apache/kafka@4.3.0"),
        )
        .expect_err("ambiguous source should fail");

        assert!(
            error.to_string().contains("exactly one"),
            "error should reject ambiguous Kafka source selection"
        );
    }

    #[test]
    fn resolve_local_source_requires_source_ref() {
        let error = KafkaSource::resolve(None, Some(std::path::Path::new("/tmp/kafka")), None)
            .expect_err("local source should fail without source ref");

        assert!(
            error.to_string().contains("--source-ref"),
            "error should keep local source generation reproducible"
        );
    }
}
