//! Multi-broker wire client facade.

use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, RwLock},
    time::Instant,
};

use kacrab_protocol::{
    generated::{ApiKey, MetadataResponseData},
    version::client_api_info,
};
use tokio::sync::Mutex;

use super::{
    auth::OAuthTokenCache,
    broker::{BrokerEndpoint, BrokerHandle},
    buffer::{BufferPoolStats, BufferPools},
    config::ConnectionConfig,
    error::{Result, WireError},
    message::{RequestMessage, ResponseMessage},
    metadata::{ClusterMetadata, map_metadata, metadata_request},
};

/// Cloneable wire facade that routes requests to broker-owned tasks.
#[derive(Debug, Clone)]
pub struct WireClient {
    inner: Arc<WireClientInner>,
}

#[derive(Debug)]
struct WireClientInner {
    config: ConnectionConfig,
    client_id: String,
    endpoints: RwLock<HashMap<i32, BrokerEndpoint>>,
    handles: RwLock<HashMap<i32, BrokerHandle>>,
    metadata: RwLock<Option<MetadataSnapshot>>,
    metadata_refresh: Mutex<()>,
    oauth_token_cache: Arc<Mutex<OAuthTokenCache>>,
    buffers: Arc<BufferPools>,
}

#[derive(Debug, Clone)]
struct MetadataSnapshot {
    metadata: Arc<ClusterMetadata>,
    updated_at: Instant,
}

impl WireClient {
    /// Create a wire client from known broker endpoints.
    #[must_use]
    pub fn connect_with_brokers(
        config: ConnectionConfig,
        client_id: impl Into<String>,
        brokers: impl IntoIterator<Item = BrokerEndpoint>,
    ) -> Self {
        let endpoints = brokers
            .into_iter()
            .map(|endpoint| (endpoint.node_id, endpoint))
            .collect();
        Self {
            inner: Arc::new(WireClientInner {
                buffers: Arc::new(BufferPools::new(config.buffer_pool_capacity)),
                config,
                client_id: client_id.into(),
                endpoints: RwLock::new(endpoints),
                handles: RwLock::new(HashMap::new()),
                metadata: RwLock::new(None),
                metadata_refresh: Mutex::new(()),
                oauth_token_cache: Arc::new(Mutex::new(OAuthTokenCache::default())),
            }),
        }
    }

    /// Add or replace a broker endpoint discovered from metadata.
    pub fn upsert_broker(&self, endpoint: BrokerEndpoint) {
        let mut endpoints = self
            .inner
            .endpoints
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let _previous = endpoints.insert(endpoint.node_id, endpoint);
    }

    /// Return one known broker id for metadata/control-plane requests.
    #[cfg(feature = "producer")]
    pub(crate) fn any_broker_id(&self) -> Result<i32> {
        self.refresh_broker_id()
    }

    /// Return wire buffer pool diagnostic counters.
    #[must_use]
    pub fn buffer_pool_stats(&self) -> BufferPoolStats {
        self.inner.buffers.stats()
    }

    /// Send a generated request to a specific broker id.
    pub async fn send_to_broker<Req, Resp>(
        &self,
        broker_id: i32,
        api_key: ApiKey,
        api_version: i16,
        request: &Req,
    ) -> Result<Resp>
    where
        Req: RequestMessage + Clone + Send + Sync + 'static,
        Resp: ResponseMessage,
    {
        let handle = self.handle_for(broker_id)?;
        handle.send(api_key, api_version, request).await
    }

    /// Return cached metadata for topics, refreshing through a known broker when needed.
    pub async fn metadata_for_topics<I, S>(&self, topics: I) -> Result<Arc<ClusterMetadata>>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let topics: Vec<String> = topics
            .into_iter()
            .map(|topic| topic.as_ref().to_owned())
            .collect();
        if let Some(metadata) = self.cached_metadata_for(&topics) {
            return Ok(metadata);
        }

        let _refresh_guard = self.inner.metadata_refresh.lock().await;
        if let Some(metadata) = self.cached_metadata_for(&topics) {
            return Ok(metadata);
        }

        let broker_id = self.refresh_broker_id()?;
        let request = metadata_request(&topics);
        let version = client_api_info(ApiKey::Metadata).max_version;
        let response: MetadataResponseData = self
            .send_to_broker(broker_id, ApiKey::Metadata, version, &request)
            .await?;
        let metadata = Arc::new(map_metadata(response)?);
        self.update_broker_registry(&metadata).await?;
        self.store_metadata(Arc::clone(&metadata));
        Ok(metadata)
    }

    /// Invalidate cached metadata when a topic-partition leadership error is observed.
    pub fn invalidate_topic_partition(&self, topic: &str, partition: i32) {
        let mut guard = self
            .inner
            .metadata
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let should_invalidate = guard
            .as_ref()
            .and_then(|snapshot| snapshot.metadata.topic(topic))
            .is_some_and(|topic_metadata| {
                topic_metadata
                    .partitions
                    .iter()
                    .any(|metadata| metadata.partition_index == partition)
            });
        if should_invalidate {
            *guard = None;
        }
    }

    fn handle_for(&self, broker_id: i32) -> Result<BrokerHandle> {
        let existing_handle = {
            let handles = self
                .inner
                .handles
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            handles.get(&broker_id).cloned()
        };
        if let Some(handle) = existing_handle {
            return Ok(handle);
        }

        let endpoint = {
            let endpoints = self
                .inner
                .endpoints
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            endpoints
                .get(&broker_id)
                .cloned()
                .ok_or(WireError::UnknownBroker(broker_id))?
        };

        let result = {
            let mut handles = self
                .inner
                .handles
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            handles.get(&broker_id).cloned().unwrap_or_else(|| {
                let handle = BrokerHandle::spawn(
                    endpoint,
                    self.inner.client_id.clone(),
                    self.inner.config.clone(),
                    Arc::clone(&self.inner.buffers),
                    Arc::clone(&self.inner.oauth_token_cache),
                );
                let _previous = handles.insert(broker_id, handle.clone());
                handle
            })
        };
        Ok(result)
    }

    fn cached_metadata_for(&self, topics: &[String]) -> Option<Arc<ClusterMetadata>> {
        let snapshot = {
            let metadata = self
                .inner
                .metadata
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            metadata.clone()
        }?;
        if snapshot.updated_at.elapsed() > self.inner.config.metadata_max_age {
            return None;
        }
        topics
            .iter()
            .all(|topic| snapshot.metadata.topic(topic).is_some())
            .then_some(snapshot.metadata)
    }

    fn refresh_broker_id(&self) -> Result<i32> {
        let existing_handle_id = {
            let handles = self
                .inner
                .handles
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            handles.keys().copied().min()
        };
        if let Some(broker_id) = existing_handle_id {
            return Ok(broker_id);
        }

        let endpoints = self
            .inner
            .endpoints
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        endpoints
            .keys()
            .min()
            .copied()
            .ok_or(WireError::NoBrokerAvailable)
    }

    async fn update_broker_registry(&self, metadata: &ClusterMetadata) -> Result<()> {
        let mut endpoints = Vec::with_capacity(metadata.brokers.len());
        for broker in &metadata.brokers {
            let port =
                u16::try_from(broker.port).map_err(|_| WireError::InvalidBrokerEndpoint {
                    node_id: broker.node_id,
                    host: broker.host.clone(),
                    port: broker.port,
                })?;
            let addresses = tokio::net::lookup_host((broker.host.as_str(), port)).await?;
            let Some(addr) = choose_broker_addr(addresses) else {
                return Err(WireError::InvalidBrokerEndpoint {
                    node_id: broker.node_id,
                    host: broker.host.clone(),
                    port: broker.port,
                });
            };
            endpoints.push(BrokerEndpoint::from_resolved(
                broker.node_id,
                broker.host.clone(),
                port,
                addr,
            ));
        }

        {
            let mut guard = self
                .inner
                .endpoints
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            for endpoint in endpoints {
                let _previous = guard.insert(endpoint.node_id, endpoint);
            }
        }
        Ok(())
    }

    fn store_metadata(&self, metadata: Arc<ClusterMetadata>) {
        let mut guard = self
            .inner
            .metadata
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *guard = Some(MetadataSnapshot {
            metadata,
            updated_at: Instant::now(),
        });
    }
}

fn choose_broker_addr(addresses: impl IntoIterator<Item = SocketAddr>) -> Option<SocketAddr> {
    let mut first = None;
    for address in addresses {
        if first.is_none() {
            first = Some(address);
        }
        if address.is_ipv4() {
            return Some(address);
        }
    }
    first
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::expect_used,
        clippy::missing_assert_message,
        clippy::unwrap_used,
        reason = "Unit test fixtures fail fastest with contextual unwrap/expect calls."
    )]

    use std::{
        net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
        sync::Arc,
        time::Duration,
    };

    use kacrab_protocol::KafkaUuid;

    use super::{BrokerEndpoint, ClusterMetadata, WireClient, choose_broker_addr};
    use crate::wire::{
        BrokerMetadata, ConnectionConfig, PartitionMetadata, TopicMetadata, WireError,
    };

    #[test]
    fn choose_broker_addr_prefers_ipv4_when_available() {
        let ipv6 = SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 9092);
        let ipv4 = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9092);

        assert_eq!(choose_broker_addr([ipv6, ipv4]), Some(ipv4));
    }

    #[test]
    fn choose_broker_addr_keeps_ipv6_when_it_is_the_only_address() {
        let ipv6 = SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 9092);

        assert_eq!(choose_broker_addr([ipv6]), Some(ipv6));
    }

    #[test]
    fn cached_metadata_rejects_expired_or_missing_topic_snapshots() {
        let client = WireClient::connect_with_brokers(
            ConnectionConfig::default().metadata_max_age(Duration::ZERO),
            "client-a",
            [BrokerEndpoint::new(
                7,
                "127.0.0.1:9092".parse().expect("socket address"),
            )],
        );
        let metadata = Arc::new(ClusterMetadata {
            cluster_id: Some("cluster-a".to_owned()),
            controller_id: 7,
            brokers: vec![BrokerMetadata {
                node_id: 7,
                host: "localhost".to_owned(),
                port: 9092,
                rack: None,
            }],
            topics: vec![TopicMetadata {
                name: "orders".to_owned(),
                topic_id: KafkaUuid::ZERO,
                partitions: vec![PartitionMetadata {
                    partition_index: 0,
                    leader_id: 7,
                    leader_epoch: 1,
                    replica_nodes: vec![7],
                    isr_nodes: vec![7],
                    offline_replicas: Vec::new(),
                }],
            }],
        });

        client.store_metadata(Arc::clone(&metadata));

        assert!(client.cached_metadata_for(&["orders".to_owned()]).is_none());

        let client = WireClient::connect_with_brokers(ConnectionConfig::default(), "client-a", []);
        client.store_metadata(metadata);
        assert!(
            client
                .cached_metadata_for(&["payments".to_owned()])
                .is_none()
        );
    }

    #[test]
    fn refresh_broker_id_reports_empty_registry() {
        let client = WireClient::connect_with_brokers(ConnectionConfig::default(), "client-a", []);

        assert!(matches!(
            client.refresh_broker_id(),
            Err(WireError::NoBrokerAvailable)
        ));
    }

    #[tokio::test]
    async fn update_broker_registry_rejects_invalid_ports_before_dns_lookup() {
        let client = WireClient::connect_with_brokers(ConnectionConfig::default(), "client-a", []);
        let metadata = ClusterMetadata {
            cluster_id: None,
            controller_id: -1,
            brokers: vec![BrokerMetadata {
                node_id: 7,
                host: "localhost".to_owned(),
                port: -1,
                rack: None,
            }],
            topics: Vec::new(),
        };

        assert!(matches!(
            client.update_broker_registry(&metadata).await,
            Err(WireError::InvalidBrokerEndpoint {
                node_id: 7,
                port: -1,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn update_broker_registry_preserves_advertised_hostname() {
        let client = WireClient::connect_with_brokers(ConnectionConfig::default(), "client-a", []);
        let metadata = ClusterMetadata {
            cluster_id: None,
            controller_id: -1,
            brokers: vec![BrokerMetadata {
                node_id: 7,
                host: "localhost".to_owned(),
                port: 9092,
                rack: None,
            }],
            topics: Vec::new(),
        };

        client.update_broker_registry(&metadata).await.unwrap();

        let endpoint = client
            .inner
            .endpoints
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&7)
            .expect("broker endpoint")
            .clone();
        assert_eq!(endpoint.host(), "localhost");
        assert_eq!(endpoint.port(), 9092);
        assert_eq!(endpoint.addr.port(), 9092);
    }
}
