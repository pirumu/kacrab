//! Stage 4 (optional): extract upstream Kafka `ConfigDef` declarations into a
//! reproducible intermediate config catalog.

mod error;
mod model;
mod parser;
mod rust_catalog;

pub use error::{KafkaConfigError, KafkaConfigErrorKind};
pub use model::{
    ConfigCatalogDocument, ConfigClientDocument, ConfigKeyDocument, ConfigOrigin,
    ConfigValueDefault, JavaConfigType, KafkaConfigClient, RuntimeConfigKeyDocument,
    RuntimeConfigOverlayDocument,
};
pub use parser::{extract_from_java_root, extract_from_sources, parse_client_config};
pub use rust_catalog::{NativeConfigKeys, generate_rust_catalog, parse_native_config_keys};

/// Merge Kacrab runtime config overlay declarations into the Kafka catalog.
pub fn merge_runtime_overlay(
    mut document: ConfigCatalogDocument,
    overlay: &RuntimeConfigOverlayDocument,
) -> Result<ConfigCatalogDocument, RuntimeOverlayError> {
    for overlay_config in &overlay.configs {
        for client in &overlay_config.clients {
            let client_doc = ensure_client_document(&mut document, *client)?;
            if client_doc
                .configs
                .iter()
                .any(|config| config.key == overlay_config.key)
            {
                return Err(RuntimeOverlayError::DuplicateKey {
                    client: *client,
                    key: overlay_config.key.clone(),
                });
            }
            client_doc
                .configs
                .push(runtime_config_entry(overlay_config));
        }
    }
    Ok(document)
}

fn ensure_client_document(
    document: &mut ConfigCatalogDocument,
    client: KafkaConfigClient,
) -> Result<&mut ConfigClientDocument, RuntimeOverlayError> {
    if document.clients.iter().all(|entry| entry.client != client) {
        document.clients.push(ConfigClientDocument {
            client,
            java_class: client.java_class().to_owned(),
            configs: Vec::new(),
        });
    }

    document
        .clients
        .iter_mut()
        .find(|entry| entry.client == client)
        .ok_or(RuntimeOverlayError::MissingClientAfterInsert { client })
}

fn runtime_config_entry(config: &RuntimeConfigKeyDocument) -> ConfigKeyDocument {
    ConfigKeyDocument {
        origin: ConfigOrigin::KacrabRuntime,
        key: config.key.clone(),
        java_constant: config.key.to_ascii_uppercase().replace('.', "_"),
        rust_field: Some(config.rust_field.clone()),
        java_type: config.java_type.clone(),
        default: config.default.clone(),
        importance: Some("LOW".to_owned()),
        documentation: Some(config.documentation.clone()),
        platforms: config.platforms.clone(),
        feature: config.feature.clone(),
    }
}

/// Runtime overlay validation error.
#[derive(Debug, thiserror::Error)]
pub enum RuntimeOverlayError {
    /// Overlay tried to define the same key twice for a client.
    #[error("duplicate runtime config key `{key}` for {client:?}")]
    DuplicateKey {
        /// Client that received a duplicate key.
        client: KafkaConfigClient,
        /// Duplicate key.
        key: String,
    },
    /// Internal invariant failed while inserting an overlay client.
    #[error("missing runtime overlay client after insert: {client:?}")]
    MissingClientAfterInsert {
        /// Client that should exist after insertion.
        client: KafkaConfigClient,
    },
}
