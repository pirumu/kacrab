//! Multi-broker wire client facade.

use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, RwLock},
    time::Instant,
};

#[cfg(feature = "producer")]
use bytes::BytesMut;
use kacrab_protocol::{
    generated::{ApiKey, MetadataResponseData},
    version::client_api_info,
};
use tokio::sync::Mutex;

#[cfg(feature = "producer")]
use super::broker::PendingBrokerResponse;
#[cfg(feature = "producer")]
use super::metadata::PartitionLeaderChange;
use super::{
    auth::OAuthTokenCache,
    broker::{BrokerEndpoint, BrokerHandle},
    buffer::{BufferPoolStats, BufferPools},
    config::ConnectionConfig,
    error::{Result, WireError},
    message::{RequestMessage, ResponseMessage},
    metadata::{
        BrokerMetadata, ClusterMetadata, MetadataManager, MetadataRecoveryAction, map_metadata,
        metadata_request, metadata_topic_states,
    },
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
    metadata: RwLock<MetadataManager>,
    metadata_refresh: Mutex<()>,
    oauth_token_cache: Arc<Mutex<OAuthTokenCache>>,
    buffers: Arc<BufferPools>,
}

impl WireClient {
    /// Create a wire client from known broker endpoints.
    #[must_use]
    pub fn connect_with_brokers(
        config: ConnectionConfig,
        client_id: impl Into<String>,
        brokers: impl IntoIterator<Item = BrokerEndpoint>,
    ) -> Self {
        let endpoints = brokers.into_iter().collect::<Vec<_>>();
        let endpoint_registry = endpoints
            .iter()
            .cloned()
            .map(|endpoint| (endpoint.node_id, endpoint))
            .collect();
        Self {
            inner: Arc::new(WireClientInner {
                buffers: Arc::new(BufferPools::new(config.buffer_pool_capacity)),
                metadata: RwLock::new(MetadataManager::new(config.clone(), endpoints)),
                config,
                client_id: client_id.into(),
                endpoints: RwLock::new(endpoint_registry),
                handles: RwLock::new(HashMap::new()),
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

    /// Highest mutually-supported version the given broker advertised for
    /// `api_key`, or `None` if the broker is unknown or its connection has not
    /// yet completed `ApiVersions` negotiation. Used by the producer to gate
    /// coordinator-capability-dependent behavior (e.g. epoch bumping).
    #[cfg(feature = "producer")]
    pub(crate) fn negotiated_version(&self, broker_id: i32, api_key: ApiKey) -> Option<i16> {
        self.handle_for(broker_id).ok()?.negotiated_version(api_key)
    }

    /// Age of the currently cached cluster metadata (Kafka `metadata-age`), or
    /// `None` when no metadata has been fetched yet.
    #[cfg(feature = "producer")]
    pub(crate) fn metadata_age(&self) -> Option<std::time::Duration> {
        self.inner
            .metadata
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .current_age(Instant::now())
    }

    /// Return wire buffer pool diagnostic counters.
    #[must_use]
    pub fn buffer_pool_stats(&self) -> BufferPoolStats {
        self.inner.buffers.stats()
    }

    #[cfg(feature = "producer")]
    pub(crate) fn acquire_write_buffer(&self, capacity: usize) -> BytesMut {
        self.inner.buffers.acquire_write(capacity)
    }

    #[cfg(feature = "producer")]
    pub(crate) fn release_write_buffer(&self, buffer: BytesMut) {
        self.inner.buffers.release_write(buffer);
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

    #[cfg(feature = "producer")]
    pub(crate) fn enqueue_to_broker<Req, Resp>(
        &self,
        broker_id: i32,
        api_key: ApiKey,
        api_version: i16,
        request: &Req,
    ) -> Result<PendingBrokerResponse<Resp>>
    where
        Req: RequestMessage + Clone + Send + Sync + 'static,
        Resp: ResponseMessage,
    {
        let handle = self.handle_for(broker_id)?;
        handle.enqueue(api_key, api_version, request)
    }

    /// Send a generated request to a broker when the Kafka API will not return
    /// a response for this request, such as Produce with `acks=0`.
    pub async fn send_to_broker_without_response<Req>(
        &self,
        broker_id: i32,
        api_key: ApiKey,
        api_version: i16,
        request: &Req,
    ) -> Result<()>
    where
        Req: RequestMessage + Clone + Send + Sync + 'static,
    {
        let handle = self.handle_for(broker_id)?;
        handle
            .send_without_response(api_key, api_version, request)
            .await
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
        self.request_metadata_update_for_missing_topics(&topics);
        if let Some((topic, error)) = self.persistent_topic_error_for(&topics) {
            return Err(WireError::MetadataTopic { topic, error });
        }

        let _refresh_guard = self.inner.metadata_refresh.lock().await;
        if let Some(metadata) = self.cached_metadata_for(&topics) {
            return Ok(metadata);
        }
        self.request_metadata_update_for_missing_topics(&topics);
        if let Some((topic, error)) = self.persistent_topic_error_for(&topics) {
            return Err(WireError::MetadataTopic { topic, error });
        }

        self.wait_for_metadata_refresh_slot().await;
        self.record_metadata_refresh_attempt();

        let response = match self.fetch_metadata_response(&topics).await {
            Ok(response) => response,
            Err(error) => {
                self.record_metadata_refresh_failure()?;
                if self.record_no_usable_metadata() == MetadataRecoveryAction::Rebootstrap {
                    self.restore_bootstrap_endpoints();
                    self.wait_for_metadata_refresh_slot().await;
                    self.record_metadata_refresh_attempt();
                    self.fetch_metadata_response(&topics).await?
                } else {
                    return Err(error);
                }
            },
        };
        self.record_metadata_topic_states(&response);
        let metadata = Arc::new(match map_metadata(response) {
            Ok(metadata) => metadata,
            Err(error) => {
                self.record_metadata_refresh_failure()?;
                return Err(error);
            },
        });
        self.update_broker_registry(&metadata).await?;
        self.store_metadata(Arc::clone(&metadata))?;
        self.mark_topics_used(&topics);
        Ok(metadata)
    }

    /// Invalidate cached metadata when a topic-partition leadership error is observed.
    pub fn invalidate_topic_partition(&self, _topic: &str, _partition: i32) {
        self.inner
            .metadata
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .invalidate_all();
    }

    #[cfg(feature = "producer")]
    pub(crate) async fn upsert_broker_metadata(&self, brokers: &[BrokerMetadata]) -> Result<()> {
        self.update_broker_registry_from_brokers(brokers).await
    }

    #[cfg(feature = "producer")]
    pub(crate) fn apply_partition_leader_update(&self, change: PartitionLeaderChange<'_>) -> bool {
        self.inner
            .metadata
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .apply_partition_leader_update(change)
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
        let mut manager = self
            .inner
            .metadata
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let metadata = manager.cached_for(topics, Instant::now())?;
        manager.mark_topics_used(topics, Instant::now());
        drop(manager);
        Some(metadata)
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
        self.update_broker_registry_from_brokers(&metadata.brokers)
            .await
    }

    async fn update_broker_registry_from_brokers(&self, brokers: &[BrokerMetadata]) -> Result<()> {
        let endpoints = resolve_broker_endpoints(brokers).await?;
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

    fn store_metadata(&self, metadata: Arc<ClusterMetadata>) -> Result<()> {
        self.inner
            .metadata
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .store(metadata, Instant::now())?;
        Ok(())
    }

    async fn fetch_metadata_response(&self, topics: &[String]) -> Result<MetadataResponseData> {
        let broker_id = self.refresh_broker_id()?;
        let request = metadata_request(topics);
        let version = client_api_info(ApiKey::Metadata).max_version;
        self.send_to_broker(broker_id, ApiKey::Metadata, version, &request)
            .await
    }

    fn mark_topics_used(&self, topics: &[String]) {
        self.inner
            .metadata
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .mark_topics_used(topics, Instant::now());
    }

    fn record_no_usable_metadata(&self) -> MetadataRecoveryAction {
        self.inner
            .metadata
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .record_no_usable_metadata(Instant::now())
    }

    async fn wait_for_metadata_refresh_slot(&self) {
        loop {
            let delay = self
                .inner
                .metadata
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .refresh_delay(Instant::now());
            if delay.is_zero() {
                return;
            }
            tokio::time::sleep(delay).await;
        }
    }

    fn record_metadata_refresh_attempt(&self) {
        self.inner
            .metadata
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .record_refresh_attempt(Instant::now());
    }

    fn record_metadata_refresh_failure(&self) -> Result<()> {
        let _delay = self
            .inner
            .metadata
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .record_refresh_failure(Instant::now())?;
        Ok(())
    }

    fn record_metadata_topic_states(&self, response: &MetadataResponseData) {
        self.inner
            .metadata
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .record_topic_states(metadata_topic_states(response));
    }

    fn request_metadata_update_for_missing_topics(&self, topics: &[String]) {
        self.inner
            .metadata
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .request_update_for_missing_topics(topics);
    }

    fn persistent_topic_error_for(
        &self,
        topics: &[String],
    ) -> Option<(String, kacrab_protocol::generated::ErrorCode)> {
        let manager = self
            .inner
            .metadata
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        topics.iter().find_map(|topic| {
            if manager.is_invalid_topic(topic) || manager.is_unauthorized_topic(topic) {
                manager
                    .topic_error(topic)
                    .map(|error| (topic.clone(), error))
            } else {
                None
            }
        })
    }

    fn restore_bootstrap_endpoints(&self) {
        let endpoints = self
            .inner
            .metadata
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .bootstrap_endpoints()
            .map(|endpoint| (endpoint.node_id, endpoint))
            .collect::<HashMap<_, _>>();
        if endpoints.is_empty() {
            return;
        }
        let mut guard = self
            .inner
            .endpoints
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *guard = endpoints;
    }
}

async fn resolve_broker_endpoints(brokers: &[BrokerMetadata]) -> Result<Vec<BrokerEndpoint>> {
    let mut endpoints = Vec::with_capacity(brokers.len());
    for broker in brokers {
        let port = u16::try_from(broker.port).map_err(|_| WireError::InvalidBrokerEndpoint {
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
    Ok(endpoints)
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

    use kacrab_protocol::{
        KafkaString, KafkaUuid,
        generated::{ErrorCode, MetadataResponseData, MetadataResponseTopic},
    };

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

        client.store_metadata(Arc::clone(&metadata)).unwrap();

        assert!(client.cached_metadata_for(&["orders".to_owned()]).is_none());

        let client = WireClient::connect_with_brokers(ConnectionConfig::default(), "client-a", []);
        client.store_metadata(metadata).unwrap();
        assert!(
            client
                .cached_metadata_for(&["payments".to_owned()])
                .is_none()
        );
    }

    #[test]
    fn cached_metadata_rejects_idle_topic_snapshots() {
        let client = WireClient::connect_with_brokers(
            ConnectionConfig::default()
                .metadata_max_age(Duration::from_mins(1))
                .metadata_max_idle(Duration::ZERO),
            "client-a",
            [BrokerEndpoint::new(
                7,
                "127.0.0.1:9092".parse().expect("socket address"),
            )],
        );
        client
            .store_metadata(Arc::new(ClusterMetadata {
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
            }))
            .unwrap();

        assert!(client.cached_metadata_for(&["orders".to_owned()]).is_none());
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

    #[tokio::test]
    async fn update_broker_registry_accepts_produce_response_endpoint_metadata() {
        let client = WireClient::connect_with_brokers(ConnectionConfig::default(), "client-a", []);
        client
            .update_broker_registry_from_brokers(&[BrokerMetadata {
                node_id: 9,
                host: "localhost".to_owned(),
                port: 19_092,
                rack: Some("rack-a".to_owned()),
            }])
            .await
            .unwrap();

        let endpoint = client
            .inner
            .endpoints
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&9)
            .expect("broker endpoint")
            .clone();
        assert_eq!(endpoint.host(), "localhost");
        assert_eq!(endpoint.port(), 19_092);
    }

    #[test]
    fn persistent_topic_error_returns_invalid_and_unauthorized_bookkeeping() {
        let client = WireClient::connect_with_brokers(ConnectionConfig::default(), "client-a", []);
        let response = MetadataResponseData {
            topics: vec![
                MetadataResponseTopic {
                    error_code: i16::from(ErrorCode::InvalidTopicException),
                    name: Some(KafkaString::from("bad topic".to_owned())),
                    ..MetadataResponseTopic::default()
                },
                MetadataResponseTopic {
                    error_code: i16::from(ErrorCode::TopicAuthorizationFailed),
                    name: Some(KafkaString::from("secret".to_owned())),
                    ..MetadataResponseTopic::default()
                },
            ],
            ..MetadataResponseData::default()
        };
        client.record_metadata_topic_states(&response);

        assert_eq!(
            client.persistent_topic_error_for(&["bad topic".to_owned()]),
            Some(("bad topic".to_owned(), ErrorCode::InvalidTopicException))
        );
        assert_eq!(
            client.persistent_topic_error_for(&["secret".to_owned()]),
            Some(("secret".to_owned(), ErrorCode::TopicAuthorizationFailed))
        );
    }
}
