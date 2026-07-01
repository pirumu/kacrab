//! The admin client: topic, partition, and config management.
//!
//! Controller-only operations (create/delete topics, create partitions) are
//! routed to the cluster controller discovered from metadata, retrying with a
//! fresh controller when a broker reports `NOT_CONTROLLER`. Describe operations
//! and broker-targeted config requests go directly to a relevant broker.

use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use kacrab_protocol::{
    KafkaString, KafkaUuid,
    generated::{
        AddRaftVoterRequestData, AddRaftVoterResponseData, AlterClientQuotasRequestData,
        AlterClientQuotasResponseData, AlterConfigsRequestData, AlterConfigsResponseData,
        AlterPartitionReassignmentsRequestData, AlterPartitionReassignmentsResponseData,
        AlterReplicaLogDirsRequestData, AlterReplicaLogDirsResponseData,
        AlterShareGroupOffsetsRequestData, AlterShareGroupOffsetsResponseData,
        AlterUserScramCredentialsRequestData, AlterUserScramCredentialsResponseData, ApiKey,
        ApiVersionsRequestData, ApiVersionsResponseData, ConsumerGroupDescribeRequestData,
        ConsumerGroupDescribeResponseData, ConsumerProtocolAssignmentData, CreateAclsRequestData,
        CreateAclsResponseData, CreateDelegationTokenRequestData,
        CreateDelegationTokenResponseData, CreatePartitionsRequestData,
        CreatePartitionsResponseData, CreateTopicsRequestData, CreateTopicsResponseData,
        DeleteAclsRequestData, DeleteAclsResponseData, DeleteGroupsRequestData,
        DeleteGroupsResponseData, DeleteRecordsPartition, DeleteRecordsRequestData,
        DeleteRecordsResponseData, DeleteRecordsTopic, DeleteShareGroupOffsetsRequestData,
        DeleteShareGroupOffsetsResponseData, DeleteTopicState, DeleteTopicsRequestData,
        DeleteTopicsResponseData, DescribableLogDirTopic, DescribeAclsRequestData,
        DescribeAclsResponseData, DescribeClientQuotasRequestData,
        DescribeClientQuotasResponseData, DescribeClusterRequestData, DescribeClusterResponseData,
        DescribeConfigsRequestData, DescribeConfigsResponseData,
        DescribeDelegationTokenRequestData, DescribeDelegationTokenResponseData,
        DescribeGroupsRequestData, DescribeGroupsResponseData, DescribeLogDirsRequestData,
        DescribeLogDirsResponseData, DescribeProducersRequestData, DescribeProducersResponseData,
        DescribeQuorumRequestData, DescribeQuorumResponseData,
        DescribeShareGroupOffsetsRequestData, DescribeShareGroupOffsetsResponseData,
        DescribeTransactionsRequestData, DescribeTransactionsResponseData,
        DescribeUserScramCredentialsRequestData, DescribeUserScramCredentialsResponseData,
        ElectLeadersRequestData, ElectLeadersResponseData, ErrorCode,
        ExpireDelegationTokenRequestData, ExpireDelegationTokenResponseData, FeatureUpdateKey,
        FindCoordinatorRequestData, FindCoordinatorResponseData,
        GetTelemetrySubscriptionsRequestData, GetTelemetrySubscriptionsResponseData,
        IncrementalAlterConfigsRequestData, IncrementalAlterConfigsResponseData,
        InitProducerIdRequestData, InitProducerIdResponseData, LeaveGroupRequestData,
        LeaveGroupResponseData, ListConfigResourcesRequestData, ListConfigResourcesResponseData,
        ListGroupsRequestData, ListGroupsResponseData, ListOffsetsPartition,
        ListOffsetsRequestData, ListOffsetsResponseData, ListOffsetsTopic,
        ListPartitionReassignmentsRequestData, ListPartitionReassignmentsResponseData,
        ListPartitionReassignmentsTopics, ListTransactionsRequestData,
        ListTransactionsResponseData, OffsetCommitRequestData, OffsetCommitRequestPartition,
        OffsetCommitRequestTopic, OffsetCommitResponseData, OffsetDeleteRequestData,
        OffsetDeleteRequestPartition, OffsetDeleteRequestTopic, OffsetDeleteResponseData,
        OffsetFetchRequestData, OffsetFetchRequestGroup, OffsetFetchRequestTopics,
        OffsetFetchResponseData, ReassignablePartition, ReassignableTopic,
        RemoveRaftVoterRequestData, RemoveRaftVoterResponseData, RenewDelegationTokenRequestData,
        RenewDelegationTokenResponseData, ShareGroupDescribeRequestData,
        ShareGroupDescribeResponseData, StreamsGroupDescribeRequestData,
        StreamsGroupDescribeResponseData, UnregisterBrokerRequestData,
        UnregisterBrokerResponseData, UpdateFeaturesRequestData, UpdateFeaturesResponseData,
        WriteTxnMarkersRequestData, WriteTxnMarkersResponseData,
        add_raft_voter_request::Listener,
        alter_client_quotas_request::{
            EntityData as AlterClientQuotasEntity, EntryData as AlterClientQuotasEntry, OpData,
        },
        alter_replica_log_dirs_request::{AlterReplicaLogDir, AlterReplicaLogDirTopic},
        alter_share_group_offsets_request::{
            AlterShareGroupOffsetsRequestPartition, AlterShareGroupOffsetsRequestTopic,
        },
        alter_user_scram_credentials_request::{
            ScramCredentialDeletion as WireScramCredentialDeletion,
            ScramCredentialUpsertion as WireScramCredentialUpsertion,
        },
        consumer_group_describe_response::Assignment as ConsumerGroupDescribeAssignment,
        create_acls_request::AclCreation,
        create_delegation_token_request::CreatableRenewers,
        delete_acls_request::DeleteAclsFilter,
        delete_share_group_offsets_request::DeleteShareGroupOffsetsRequestTopic,
        describe_client_quotas_request::ComponentData,
        describe_delegation_token_request::DescribeDelegationTokenOwner,
        describe_producers_request::TopicRequest as DescribeProducersTopicRequest,
        describe_share_group_offsets_request::{
            DescribeShareGroupOffsetsRequestGroup, DescribeShareGroupOffsetsRequestTopic,
        },
        describe_user_scram_credentials_request::UserName,
        elect_leaders_request::TopicPartitions as ElectLeadersTopicPartitions,
        leave_group_request::MemberIdentity,
        write_txn_markers_request::{WritableTxnMarker, WritableTxnMarkerTopic},
    },
    version::client_api_info,
};

use super::{
    error::{AdminError, Result},
    metrics::{AdminMetrics, AdminMetricsSnapshot},
    types::{
        AbortTransactionSpec, AclBinding, AclBindingFilter, AclOperation, AclPatternType,
        AclPermissionType, AclResourceType, AlterConfigOp, AlterConfigsOptions, BrokerLogDirs,
        ClientQuotaAlteration, ClientQuotaEntity, ClientQuotaEntry, ClientQuotaFilterComponent,
        ClientQuotaMatch, ClusterDescription, ConfigEntry, ConfigResource, ConfigSource,
        ConsumerGroupDescription, ConsumerGroupListing, CreateDelegationTokenOptions,
        CreatePartitionsOptions, CreateTopicsOptions, DelegationToken, DeletedRecords,
        DescribeConsumerGroupsOptions, DescribeTopicsOptions, ElectionType, FeatureMetadata,
        FeatureUpdate, FencedProducer, FinalizedVersionRange, GroupOffset, GroupState, GroupType,
        ListConsumerGroupOffsetsOptions, ListConsumerGroupsOptions, ListOffsetsResult,
        ListTopicsOptions, ListTransactionsOptions, LogDirDescription, LogDirReplicaInfo,
        MemberDescription, MemberToRemove, NewPartitionReassignment, NewPartitions, NewTopic, Node,
        OffsetSpec, PartitionProducerState, PartitionReassignment, ProducerState, QuorumInfo,
        QuorumReplicaState, RaftVoterEndpoint, ReplicaLogDirAssignment, ReplicaLogDirInfo,
        ResourceConfig, ResourceType, ScramCredentialDeletion, ScramCredentialInfo,
        ScramCredentialUpsertion, ScramMechanism, ShareGroupDescription, StreamsGroupDescription,
        SupportedVersionRange, TopicDescription, TopicListing, TopicPartitionInfo,
        TransactionDescription, TransactionListing, UserScramCredentials,
    },
};
use crate::{
    common::{OffsetAndMetadata, TopicPartition},
    config::{AdminConfig, ClientConfig, ConfigKey, ConfigValue, Properties},
    wire::{
        BrokerEndpoint, ClusterMetadata, RequestMessage, ResponseMessage, WireClient, WireError,
    },
};

/// Backoff between controller-routing attempts after a `NOT_CONTROLLER` reply.
const CONTROLLER_RETRY_BACKOFF: Duration = Duration::from_millis(100);
/// Upper bound on controller-routing attempts, independent of configured retries.
const MAX_CONTROLLER_ATTEMPTS: u32 = 8;
/// Transaction timeout sent in the `InitProducerId` used to fence producers; the
/// value is immaterial since fencing only bumps the epoch.
const FENCE_PRODUCER_TXN_TIMEOUT_MS: i32 = 60_000;
/// The internal topic backing the `KRaft` metadata quorum, queried by
/// `describe_metadata_quorum`.
const METADATA_QUORUM_TOPIC: &str = "__cluster_metadata";

/// A Kafka admin client for managing topics, partitions, and configs.
///
/// Build one with [`AdminClient::from_config`]. The client is cheap to clone —
/// clones share the underlying wire connections.
#[derive(Debug, Clone)]
pub struct AdminClient {
    wire: WireClient,
    request_timeout_ms: i32,
    controller_attempts: u32,
    metrics: AdminMetrics,
}

impl AdminClient {
    /// Build an admin client from a typed [`AdminConfig`], resolving and
    /// connecting to the configured bootstrap servers.
    ///
    /// # Errors
    /// Returns an error when `bootstrap.servers` is empty or malformed, or when
    /// no bootstrap entry resolves to a socket address.
    pub async fn from_config(config: AdminConfig) -> Result<Self> {
        let endpoints = resolve_bootstrap_brokers(&config).await?;
        let request_timeout_ms = i32::try_from(config.request_timeout_ms.as_millis())
            .unwrap_or(i32::MAX)
            .max(0);
        let controller_attempts = controller_attempts_from_retries(config.retries);
        let wire = WireClient::connect_with_brokers(
            config.to_connection_config(),
            config.client_id,
            endpoints,
        );
        Ok(Self {
            wire,
            request_timeout_ms,
            controller_attempts,
            metrics: AdminMetrics::default(),
        })
    }

    /// Build an admin client around an existing wire client (for tests and
    /// embedding alongside a producer that already owns connections).
    #[must_use]
    pub fn from_parts(wire: WireClient, request_timeout_ms: i32) -> Self {
        Self {
            wire,
            request_timeout_ms: request_timeout_ms.max(0),
            controller_attempts: MAX_CONTROLLER_ATTEMPTS,
            metrics: AdminMetrics::default(),
        }
    }

    /// Build an admin client from an ergonomic Kafka [`ClientConfig`], mirroring
    /// Java's `Admin.create(Properties)`.
    ///
    /// # Errors
    /// Returns an error when config validation, DNS resolution, or connection
    /// setup fails.
    pub async fn new(config: ClientConfig) -> Result<Self> {
        Self::from_client_config(&config).await
    }

    /// Build an admin client from a borrowed Kafka [`ClientConfig`].
    ///
    /// # Errors
    /// Returns an error when config validation, DNS resolution, or connection
    /// setup fails.
    pub async fn from_client_config(config: &ClientConfig) -> Result<Self> {
        let config = config.admin_config()?;
        Self::from_config(config).await
    }

    /// Build an admin client from `Properties`-style entries.
    ///
    /// # Errors
    /// Returns an error when config validation, DNS resolution, or connection
    /// setup fails.
    pub async fn from_properties(properties: Properties) -> Result<Self> {
        Self::from_client_config(&ClientConfig::from(properties)).await
    }

    /// Build an admin client from a map/iterator of Kafka config entries.
    ///
    /// # Errors
    /// Returns an error when config validation, DNS resolution, or connection
    /// setup fails.
    pub async fn from_map<I, K, V>(entries: I) -> Result<Self>
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<ConfigKey>,
        V: Into<ConfigValue>,
    {
        let config: ClientConfig = entries.into_iter().collect();
        Self::from_client_config(&config).await
    }

    /// Create one or more topics.
    ///
    /// # Errors
    /// Returns the first per-topic broker error code, or a routing error if the
    /// cluster never settled on a controller.
    pub async fn create_topics(
        &self,
        topics: Vec<NewTopic>,
        options: CreateTopicsOptions,
    ) -> Result<()> {
        let request = CreateTopicsRequestData {
            topics: topics.into_iter().map(NewTopic::into_creatable).collect(),
            timeout_ms: self.request_timeout_ms,
            validate_only: options.validate_only,
            _unknown_tagged_fields: Vec::new(),
        };
        let response: CreateTopicsResponseData = self
            .route_to_controller(
                ApiKey::CreateTopics,
                &request,
                |response: &CreateTopicsResponseData| {
                    response
                        .topics
                        .iter()
                        .any(|topic| is_not_controller(topic.error_code))
                },
            )
            .await?;
        for topic in &response.topics {
            check_code(
                topic.name.as_str(),
                topic.error_code,
                topic.error_message.as_ref(),
            )?;
        }
        Ok(())
    }

    /// Delete topics by name.
    ///
    /// # Errors
    /// Returns the first per-topic broker error code, or a routing error.
    pub async fn delete_topics(&self, names: Vec<String>) -> Result<()> {
        let request = DeleteTopicsRequestData {
            topics: names
                .iter()
                .map(|name| DeleteTopicState {
                    name: Some(name.clone().into()),
                    topic_id: KafkaUuid::ZERO,
                    _unknown_tagged_fields: Vec::new(),
                })
                .collect(),
            topic_names: Vec::new(),
            timeout_ms: self.request_timeout_ms,
            _unknown_tagged_fields: Vec::new(),
        };
        let response: DeleteTopicsResponseData = self
            .route_to_controller(
                ApiKey::DeleteTopics,
                &request,
                |response: &DeleteTopicsResponseData| {
                    response
                        .responses
                        .iter()
                        .any(|result| is_not_controller(result.error_code))
                },
            )
            .await?;
        for result in &response.responses {
            let target = result.name.as_ref().map_or("", KafkaString::as_str);
            check_code(target, result.error_code, result.error_message.as_ref())?;
        }
        Ok(())
    }

    /// Increase the partition count of one or more topics.
    ///
    /// # Errors
    /// Returns the first per-topic broker error code, or a routing error.
    pub async fn create_partitions(
        &self,
        partitions: Vec<NewPartitions>,
        options: CreatePartitionsOptions,
    ) -> Result<()> {
        let request = CreatePartitionsRequestData {
            topics: partitions
                .into_iter()
                .map(NewPartitions::into_topic)
                .collect(),
            timeout_ms: self.request_timeout_ms,
            validate_only: options.validate_only,
            _unknown_tagged_fields: Vec::new(),
        };
        let response: CreatePartitionsResponseData = self
            .route_to_controller(
                ApiKey::CreatePartitions,
                &request,
                |response: &CreatePartitionsResponseData| {
                    response
                        .results
                        .iter()
                        .any(|result| is_not_controller(result.error_code))
                },
            )
            .await?;
        for result in &response.results {
            check_code(
                result.name.as_str(),
                result.error_code,
                result.error_message.as_ref(),
            )?;
        }
        Ok(())
    }

    /// Describe the cluster: its id, current controller, and live brokers.
    ///
    /// # Errors
    /// Returns the broker's top-level error code, or a wire error.
    pub async fn describe_cluster(&self) -> Result<ClusterDescription> {
        let request = DescribeClusterRequestData {
            include_cluster_authorized_operations: false,
            endpoint_type: 1,
            include_fenced_brokers: false,
            _unknown_tagged_fields: Vec::new(),
        };
        let broker_id = self.wire.admin_any_broker_id()?;
        let version = client_api_info(ApiKey::DescribeCluster).max_version;
        let response: DescribeClusterResponseData = self
            .send_metered(broker_id, ApiKey::DescribeCluster, version, &request)
            .await?;
        check_code("", response.error_code, response.error_message.as_ref())?;

        let nodes: Vec<Node> = response
            .brokers
            .iter()
            .map(|broker| Node {
                id: broker.broker_id,
                host: broker.host.as_str().to_owned(),
                port: broker.port,
                rack: broker.rack.as_ref().map(|rack| rack.as_str().to_owned()),
            })
            .collect();
        let controller = nodes
            .iter()
            .find(|node| node.id == response.controller_id)
            .cloned();
        let cluster_id = {
            let id = response.cluster_id.as_str();
            (!id.is_empty()).then(|| id.to_owned())
        };
        Ok(ClusterDescription {
            cluster_id,
            controller,
            nodes,
        })
    }

    /// List the topics in the cluster.
    ///
    /// Internal topics (e.g. `__consumer_offsets`) are excluded unless
    /// [`ListTopicsOptions::list_internal`] is set, matching Java's
    /// `listTopics`/`listInternal`.
    ///
    /// # Errors
    /// Returns a wire error if metadata cannot be fetched.
    pub async fn list_topics(&self, options: ListTopicsOptions) -> Result<Vec<TopicListing>> {
        let metadata = self.wire.admin_metadata(None).await?;
        Ok(metadata
            .topics
            .iter()
            .filter(|topic| options.list_internal || !topic.is_internal)
            .map(|topic| TopicListing {
                name: topic.name.clone(),
                topic_id: topic.topic_id,
                is_internal: topic.is_internal,
            })
            .collect())
    }

    /// Describe the named topics: partition placement, leaders, replicas, ISR.
    ///
    /// # Errors
    /// Returns a wire error (e.g. unknown topic) or [`AdminError::MissingResult`]
    /// when the broker omits a requested topic.
    pub async fn describe_topics(
        &self,
        names: Vec<String>,
        _options: DescribeTopicsOptions,
    ) -> Result<Vec<TopicDescription>> {
        let metadata = self.wire.admin_metadata(Some(&names)).await?;
        let mut described = Vec::with_capacity(names.len());
        for name in &names {
            let topic = metadata
                .topics
                .iter()
                .find(|topic| &topic.name == name)
                .ok_or_else(|| AdminError::MissingResult {
                    target: name.clone(),
                })?;
            let partitions = topic
                .partitions
                .iter()
                .map(|partition| TopicPartitionInfo {
                    partition: partition.partition_index,
                    leader: node_for(&metadata, partition.leader_id),
                    replicas: partition
                        .replica_nodes
                        .iter()
                        .map(|id| node_or_placeholder(&metadata, *id))
                        .collect(),
                    isr: partition
                        .isr_nodes
                        .iter()
                        .map(|id| node_or_placeholder(&metadata, *id))
                        .collect(),
                })
                .collect();
            described.push(TopicDescription {
                name: topic.name.clone(),
                topic_id: topic.topic_id,
                is_internal: topic.is_internal,
                partitions,
            });
        }
        Ok(described)
    }

    /// Describe the configs of the given resources.
    ///
    /// # Errors
    /// Returns the first per-resource broker error code, or a wire error.
    pub async fn describe_configs(
        &self,
        resources: Vec<ConfigResource>,
    ) -> Result<Vec<ResourceConfig>> {
        let request = DescribeConfigsRequestData {
            resources: resources.iter().map(ConfigResource::to_describe).collect(),
            include_synonyms: false,
            include_documentation: false,
            _unknown_tagged_fields: Vec::new(),
        };
        let broker_id = self.broker_for_configs(&resources)?;
        let version = client_api_info(ApiKey::DescribeConfigs).max_version;
        let response: DescribeConfigsResponseData = self
            .send_metered(broker_id, ApiKey::DescribeConfigs, version, &request)
            .await?;

        let mut described = Vec::with_capacity(response.results.len());
        for result in response.results {
            let resource = ConfigResource {
                resource_type: ResourceType::from_wire(result.resource_type),
                name: result.resource_name.as_str().to_owned(),
            };
            check_code(
                &resource.name,
                result.error_code,
                result.error_message.as_ref(),
            )?;
            let entries = result
                .configs
                .into_iter()
                .map(|config| ConfigEntry {
                    name: config.name.as_str().to_owned(),
                    value: config.value.map(|value| value.as_str().to_owned()),
                    read_only: config.read_only,
                    is_sensitive: config.is_sensitive,
                    source: ConfigSource::from_wire(config.config_source),
                })
                .collect();
            described.push(ResourceConfig { resource, entries });
        }
        Ok(described)
    }

    /// Replace the configs of the given resources (full-set alter semantics).
    ///
    /// Each call replaces the dynamic config set of every named resource with
    /// exactly the supplied entries, matching Kafka's (legacy) `alterConfigs`.
    ///
    /// # Errors
    /// Returns the first per-resource broker error code, or a routing error.
    pub async fn alter_configs(
        &self,
        configs: Vec<(ConfigResource, Vec<ConfigEntry>)>,
        options: AlterConfigsOptions,
    ) -> Result<()> {
        let resources: Vec<ConfigResource> = configs
            .iter()
            .map(|(resource, _)| resource.clone())
            .collect();
        let request = AlterConfigsRequestData {
            resources: configs
                .into_iter()
                .map(|(resource, entries)| resource.to_alter(entries))
                .collect(),
            validate_only: options.validate_only,
            _unknown_tagged_fields: Vec::new(),
        };

        // Per-broker configs are served by that broker; everything else
        // (topic configs, cluster-wide broker defaults) is controller-routed.
        let response: AlterConfigsResponseData = match broker_target(&resources) {
            Some(broker_id) => {
                let version = client_api_info(ApiKey::AlterConfigs).max_version;
                self.send_metered(broker_id, ApiKey::AlterConfigs, version, &request)
                    .await?
            },
            None => {
                self.route_to_controller(
                    ApiKey::AlterConfigs,
                    &request,
                    |response: &AlterConfigsResponseData| {
                        response
                            .responses
                            .iter()
                            .any(|result| is_not_controller(result.error_code))
                    },
                )
                .await?
            },
        };
        for result in &response.responses {
            check_code(
                result.resource_name.as_str(),
                result.error_code,
                result.error_message.as_ref(),
            )?;
        }
        Ok(())
    }

    /// Return the client's unique instance id, fetched from a broker via
    /// `GetTelemetrySubscriptions` (Kafka's `clientInstanceId`).
    ///
    /// # Errors
    /// Returns the broker error code (e.g. when client telemetry is disabled), or
    /// a wire error such as `UnsupportedApiVersion` when the broker does not
    /// advertise the client-telemetry API at all.
    pub async fn client_instance_id(&self) -> Result<KafkaUuid> {
        let request = GetTelemetrySubscriptionsRequestData {
            client_instance_id: KafkaUuid::ZERO,
            _unknown_tagged_fields: Vec::new(),
        };
        let broker_id = self.wire.admin_any_broker_id()?;
        // Pass the client's max as the ceiling; the broker session negotiates the
        // actual version down to what the broker advertised (client telemetry is
        // optional, so this may resolve to `UnsupportedApiVersion`).
        let version = client_api_info(ApiKey::GetTelemetrySubscriptions).max_version;
        let response: GetTelemetrySubscriptionsResponseData = self
            .send_metered(
                broker_id,
                ApiKey::GetTelemetrySubscriptions,
                version,
                &request,
            )
            .await?;
        check_code("", response.error_code, None)?;
        Ok(response.client_instance_id)
    }

    /// A snapshot of the admin client's metrics — request counts, request
    /// latency, and the wire buffer-pool counters. kacrab's native analogue of
    /// Java's `Admin.metrics()`.
    #[must_use]
    pub fn metrics(&self) -> AdminMetricsSnapshot {
        self.metrics.snapshot(self.wire.buffer_pool_stats())
    }

    /// Send a request to a broker, recording it in the admin metrics.
    async fn send_metered<Req, Resp>(
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
        let started = Instant::now();
        let wire = &self.wire;
        let result = wire
            .send_to_broker(broker_id, api_key, api_version, request)
            .await;
        self.metrics.record(started.elapsed(), result.is_err());
        Ok(result?)
    }

    /// Send a controller-only request, retrying against a freshly discovered
    /// controller while the broker reports `NOT_CONTROLLER`.
    async fn route_to_controller<Req, Resp>(
        &self,
        api_key: ApiKey,
        request: &Req,
        is_not_controller: impl Fn(&Resp) -> bool,
    ) -> Result<Resp>
    where
        Req: RequestMessage + Clone + Send + Sync + 'static,
        Resp: ResponseMessage,
    {
        let version = client_api_info(api_key).max_version;
        let mut attempt: u32 = 0;
        loop {
            attempt = attempt.saturating_add(1);
            let controller_id = self.controller_id().await?;
            let response: Resp = self
                .send_metered(controller_id, api_key, version, request)
                .await?;
            if is_not_controller(&response) {
                if attempt >= self.controller_attempts {
                    return Err(AdminError::ControllerUnavailable { attempts: attempt });
                }
                tokio::time::sleep(CONTROLLER_RETRY_BACKOFF).await;
                continue;
            }
            return Ok(response);
        }
    }

    /// Send a request to the coordinator for `key` (a group id with
    /// `key_type` 0, or a transactional id with `key_type` 1), retrying against a
    /// freshly resolved coordinator while it reports a transient coordinator
    /// error (`NOT_COORDINATOR` / `COORDINATOR_NOT_AVAILABLE` /
    /// `COORDINATOR_LOAD_IN_PROGRESS`) — whether from `FindCoordinator` or in the
    /// response. Returns the (possibly still-erroring) response and the resolved
    /// coordinator so the caller can run its own per-result error checks.
    #[expect(
        clippy::too_many_arguments,
        reason = "Coordinator routing needs the key, its type, the API, request, and an error \
                  predicate; each is distinct."
    )]
    async fn route_to_coordinator<Req, Resp>(
        &self,
        key: &str,
        key_type: i8,
        api_key: ApiKey,
        request: &Req,
        has_coordinator_error: impl Fn(&Resp) -> bool,
    ) -> Result<(Resp, Node)>
    where
        Req: RequestMessage + Clone + Send + Sync + 'static,
        Resp: ResponseMessage,
    {
        let version = client_api_info(api_key).max_version;
        let mut attempt: u32 = 0;
        loop {
            attempt = attempt.saturating_add(1);
            let coordinator = match self.coordinator_node(key, key_type).await {
                Ok(coordinator) => coordinator,
                Err(AdminError::Broker { error, .. })
                    if is_coordinator_error(i16::from(error))
                        && attempt < self.controller_attempts =>
                {
                    tokio::time::sleep(CONTROLLER_RETRY_BACKOFF).await;
                    continue;
                },
                Err(error) => return Err(error),
            };
            let response: Resp = self
                .send_metered(coordinator.id, api_key, version, request)
                .await?;
            if has_coordinator_error(&response) && attempt < self.controller_attempts {
                tokio::time::sleep(CONTROLLER_RETRY_BACKOFF).await;
                continue;
            }
            return Ok((response, coordinator));
        }
    }

    /// Fetch fresh metadata and return the current controller broker id.
    async fn controller_id(&self) -> Result<i32> {
        let metadata: std::sync::Arc<ClusterMetadata> = self.wire.admin_metadata(None).await?;
        if metadata.controller_id < 0 {
            return Err(AdminError::NoController);
        }
        Ok(metadata.controller_id)
    }

    /// Pick the broker to serve a describe-configs request: the targeted broker
    /// when all resources name the same one, otherwise any live broker.
    fn broker_for_configs(&self, resources: &[ConfigResource]) -> Result<i32> {
        match broker_target(resources) {
            Some(broker_id) => Ok(broker_id),
            None => Ok(self.wire.admin_any_broker_id()?),
        }
    }

    /// List the consumer groups known to the cluster.
    ///
    /// Mirrors Java's `listConsumerGroups`: every live broker is asked for the
    /// groups it coordinates and the results are aggregated.
    ///
    /// # Errors
    /// Returns a wire error, or the first broker top-level error code.
    pub async fn list_consumer_groups(
        &self,
        options: ListConsumerGroupsOptions,
    ) -> Result<Vec<ConsumerGroupListing>> {
        let request = ListGroupsRequestData {
            states_filter: options.states_filter.into_iter().map(Into::into).collect(),
            types_filter: options.types_filter.into_iter().map(Into::into).collect(),
            _unknown_tagged_fields: Vec::new(),
        };
        let metadata = self.wire.admin_metadata(None).await?;
        let broker_ids: Vec<i32> = metadata
            .brokers
            .iter()
            .map(|broker| broker.node_id)
            .collect();
        let version = client_api_info(ApiKey::ListGroups).max_version;
        let mut listings = Vec::new();
        for broker_id in broker_ids {
            let response: ListGroupsResponseData = self
                .send_metered(broker_id, ApiKey::ListGroups, version, &request)
                .await?;
            check_code("", response.error_code, None)?;
            for group in response.groups {
                listings.push(ConsumerGroupListing {
                    group_id: group.group_id.as_str().to_owned(),
                    is_simple_consumer_group: group.protocol_type.as_str().is_empty(),
                    state: non_empty(group.group_state.as_str())
                        .map(|state| GroupState::from_broker(&state)),
                    group_type: non_empty(group.group_type.as_str())
                        .map(|group_type| GroupType::from_broker(&group_type)),
                });
            }
        }
        Ok(listings)
    }

    /// Describe the given consumer groups, routing each to its coordinator.
    ///
    /// Consumer-protocol groups (KIP-848) are described via `ConsumerGroupDescribe`
    /// and classic groups via `DescribeGroups`; each id is tried on the new API
    /// first and falls back automatically.
    ///
    /// # Errors
    /// Returns the coordinator-lookup error, the first per-group broker error
    /// code, or [`AdminError::MissingResult`] when the broker omits a group.
    pub async fn describe_consumer_groups(
        &self,
        group_ids: Vec<String>,
        options: DescribeConsumerGroupsOptions,
    ) -> Result<Vec<ConsumerGroupDescription>> {
        let mut described = Vec::with_capacity(group_ids.len());
        for group_id in &group_ids {
            let description = match self
                .describe_consumer_group_consumer_protocol(group_id, &options)
                .await?
            {
                Some(description) => description,
                None => {
                    self.describe_consumer_group_classic(group_id, &options)
                        .await?
                },
            };
            described.push(description);
        }
        Ok(described)
    }

    /// Describe a consumer-protocol group via `ConsumerGroupDescribe`. Returns
    /// `Ok(None)` when the group is not a consumer-protocol group (or the broker
    /// does not support the API), so the caller falls back to `DescribeGroups`.
    async fn describe_consumer_group_consumer_protocol(
        &self,
        group_id: &str,
        options: &DescribeConsumerGroupsOptions,
    ) -> Result<Option<ConsumerGroupDescription>> {
        let request = ConsumerGroupDescribeRequestData {
            group_ids: vec![group_id.to_owned().into()],
            include_authorized_operations: options.include_authorized_operations,
            _unknown_tagged_fields: Vec::new(),
        };
        let (response, coordinator) = match self
            .route_to_coordinator(
                group_id,
                0,
                ApiKey::ConsumerGroupDescribe,
                &request,
                |response: &ConsumerGroupDescribeResponseData| {
                    response
                        .groups
                        .iter()
                        .any(|group| is_coordinator_error(group.error_code))
                },
            )
            .await
        {
            Ok(pair) => pair,
            // Broker predates ConsumerGroupDescribe — fall back to DescribeGroups.
            Err(AdminError::Wire(WireError::UnsupportedApiVersion(_))) => return Ok(None),
            Err(other) => return Err(other),
        };
        let Some(group) = response
            .groups
            .into_iter()
            .find(|group| group.group_id.as_str() == group_id)
        else {
            return Ok(None);
        };
        // GROUP_ID_NOT_FOUND here means the group is a classic group.
        if ErrorCode::from(group.error_code) == ErrorCode::GroupIdNotFound {
            return Ok(None);
        }
        check_code(group_id, group.error_code, group.error_message.as_ref())?;
        let members = group
            .members
            .into_iter()
            .map(|member| MemberDescription {
                member_id: member.member_id.as_str().to_owned(),
                group_instance_id: member.instance_id.map(|id| id.as_str().to_owned()),
                rack_id: member.rack_id.map(|id| id.as_str().to_owned()),
                client_id: member.client_id.as_str().to_owned(),
                host: member.client_host.as_str().to_owned(),
                assignment: consumer_assignment_partitions(member.assignment),
                target_assignment: consumer_assignment_partitions(member.target_assignment),
                member_epoch: Some(member.member_epoch),
                upgraded: match member.member_type {
                    0 => Some(false),
                    1 => Some(true),
                    _ => None,
                },
            })
            .collect();
        Ok(Some(ConsumerGroupDescription {
            group_id: group.group_id.as_str().to_owned(),
            is_simple_consumer_group: false,
            members,
            partition_assignor: group.assignor_name.as_str().to_owned(),
            state: GroupState::from_broker(group.group_state.as_str()),
            group_type: GroupType::Consumer,
            coordinator,
            authorized_operations: decode_authorized_operations(group.authorized_operations),
            group_epoch: Some(group.group_epoch),
            target_assignment_epoch: Some(group.assignment_epoch),
        }))
    }

    /// Describe a classic group via `DescribeGroups`.
    async fn describe_consumer_group_classic(
        &self,
        group_id: &str,
        options: &DescribeConsumerGroupsOptions,
    ) -> Result<ConsumerGroupDescription> {
        let request = DescribeGroupsRequestData {
            groups: vec![group_id.to_owned().into()],
            include_authorized_operations: options.include_authorized_operations,
            _unknown_tagged_fields: Vec::new(),
        };
        let (response, coordinator) = self
            .route_to_coordinator(
                group_id,
                0,
                ApiKey::DescribeGroups,
                &request,
                |response: &DescribeGroupsResponseData| {
                    response
                        .groups
                        .iter()
                        .any(|group| is_coordinator_error(group.error_code))
                },
            )
            .await?;
        let group = response
            .groups
            .into_iter()
            .find(|group| group.group_id.as_str() == group_id)
            .ok_or_else(|| AdminError::MissingResult {
                target: group_id.to_owned(),
            })?;
        check_code(group_id, group.error_code, group.error_message.as_ref())?;
        let members = group
            .members
            .into_iter()
            .map(|member| MemberDescription {
                member_id: member.member_id.as_str().to_owned(),
                group_instance_id: member.group_instance_id.map(|id| id.as_str().to_owned()),
                rack_id: None,
                client_id: member.client_id.as_str().to_owned(),
                host: member.client_host.as_str().to_owned(),
                assignment: decode_member_assignment(&member.member_assignment),
                target_assignment: Vec::new(),
                member_epoch: None,
                upgraded: None,
            })
            .collect();
        Ok(ConsumerGroupDescription {
            group_id: group.group_id.as_str().to_owned(),
            is_simple_consumer_group: group.protocol_type.as_str().is_empty(),
            members,
            partition_assignor: group.protocol_data.as_str().to_owned(),
            state: GroupState::from_broker(group.group_state.as_str()),
            group_type: GroupType::Classic,
            coordinator,
            authorized_operations: decode_authorized_operations(group.authorized_operations),
            group_epoch: None,
            target_assignment_epoch: None,
        })
    }

    /// Delete consumer groups by id, routing each to its coordinator.
    ///
    /// # Errors
    /// Returns the coordinator-lookup error or the first per-group broker error.
    pub async fn delete_consumer_groups(&self, group_ids: Vec<String>) -> Result<()> {
        for group_id in &group_ids {
            let request = DeleteGroupsRequestData {
                groups_names: vec![group_id.clone().into()],
                _unknown_tagged_fields: Vec::new(),
            };
            let (response, _coordinator) = self
                .route_to_coordinator(
                    group_id,
                    0,
                    ApiKey::DeleteGroups,
                    &request,
                    |response: &DeleteGroupsResponseData| {
                        response
                            .results
                            .iter()
                            .any(|result| is_coordinator_error(result.error_code))
                    },
                )
                .await?;
            for result in &response.results {
                check_code(result.group_id.as_str(), result.error_code, None)?;
            }
        }
        Ok(())
    }

    /// List the committed offsets of a consumer group.
    ///
    /// # Errors
    /// Returns the coordinator-lookup error or the first broker error code.
    pub async fn list_consumer_group_offsets(
        &self,
        group_id: &str,
        options: ListConsumerGroupOffsetsOptions,
    ) -> Result<Vec<GroupOffset>> {
        // Newer OffsetFetch versions key topics by id, so resolve ids for the
        // partition filter (the "all topics" case sends no per-topic entry).
        let topic_names = unique_topics(
            &options
                .partitions
                .iter()
                .map(|tp| tp.topic.as_str())
                .collect::<Vec<_>>(),
        );
        let topic_ids = self.resolve_topic_ids(&topic_names).await?;
        let request = OffsetFetchRequestData {
            groups: vec![OffsetFetchRequestGroup {
                group_id: group_id.to_owned().into(),
                member_id: None,
                member_epoch: -1,
                topics: group_offset_fetch_topics(&options.partitions, &topic_ids),
                _unknown_tagged_fields: Vec::new(),
            }],
            require_stable: options.require_stable,
            ..OffsetFetchRequestData::default()
        };
        let (response, _coordinator) = self
            .route_to_coordinator(
                group_id,
                0,
                ApiKey::OffsetFetch,
                &request,
                |response: &OffsetFetchResponseData| {
                    response
                        .groups
                        .iter()
                        .any(|group| is_coordinator_error(group.error_code))
                },
            )
            .await?;
        let group = response
            .groups
            .into_iter()
            .find(|group| group.group_id.as_str() == group_id)
            .ok_or_else(|| AdminError::MissingResult {
                target: group_id.to_owned(),
            })?;
        check_code(group_id, group.error_code, None)?;
        let mut offsets = Vec::new();
        for topic in group.topics {
            for partition in topic.partitions {
                check_code(topic.name.as_str(), partition.error_code, None)?;
                // A committed offset of -1 means the group has no commit for this
                // partition; Java omits these from the returned map.
                if partition.committed_offset < 0 {
                    continue;
                }
                offsets.push(GroupOffset {
                    partition: TopicPartition::new(
                        topic.name.as_str().to_owned(),
                        partition.partition_index,
                    ),
                    offset: build_offset(
                        partition.committed_offset,
                        partition.committed_leader_epoch,
                        partition.metadata,
                    ),
                });
            }
        }
        Ok(offsets)
    }

    /// Alter (commit) the offsets of a consumer group.
    ///
    /// # Errors
    /// Returns the coordinator-lookup error or the first per-partition error.
    pub async fn alter_consumer_group_offsets(
        &self,
        group_id: &str,
        offsets: Vec<(TopicPartition, OffsetAndMetadata)>,
    ) -> Result<()> {
        // OffsetCommit v10 drops the topic name in favour of the topic id, so
        // resolve ids from metadata (older versions still serialize the name).
        let topic_names = unique_topics(
            &offsets
                .iter()
                .map(|(tp, _)| tp.topic.as_str())
                .collect::<Vec<_>>(),
        );
        let topic_ids = self.resolve_topic_ids(&topic_names).await?;
        let request = OffsetCommitRequestData {
            group_id: group_id.to_owned().into(),
            generation_id_or_member_epoch: -1,
            member_id: String::new().into(),
            group_instance_id: None,
            retention_time_ms: -1,
            topics: offset_commit_topics(offsets, &topic_ids),
            _unknown_tagged_fields: Vec::new(),
        };
        let (response, _coordinator) = self
            .route_to_coordinator(
                group_id,
                0,
                ApiKey::OffsetCommit,
                &request,
                |response: &OffsetCommitResponseData| {
                    response.topics.iter().any(|topic| {
                        topic
                            .partitions
                            .iter()
                            .any(|partition| is_coordinator_error(partition.error_code))
                    })
                },
            )
            .await?;
        for topic in &response.topics {
            for partition in &topic.partitions {
                check_code(topic.name.as_str(), partition.error_code, None)?;
            }
        }
        Ok(())
    }

    /// Delete committed offsets of a consumer group for the given partitions.
    ///
    /// # Errors
    /// Returns the coordinator-lookup error, the top-level error, or the first
    /// per-partition error.
    pub async fn delete_consumer_group_offsets(
        &self,
        group_id: &str,
        partitions: Vec<TopicPartition>,
    ) -> Result<()> {
        let request = OffsetDeleteRequestData {
            group_id: group_id.to_owned().into(),
            topics: offset_delete_topics(partitions),
            _unknown_tagged_fields: Vec::new(),
        };
        let (response, _coordinator) = self
            .route_to_coordinator(
                group_id,
                0,
                ApiKey::OffsetDelete,
                &request,
                |response: &OffsetDeleteResponseData| {
                    is_coordinator_error(response.error_code)
                        || response.topics.iter().any(|topic| {
                            topic
                                .partitions
                                .iter()
                                .any(|partition| is_coordinator_error(partition.error_code))
                        })
                },
            )
            .await?;
        check_code(group_id, response.error_code, None)?;
        for topic in &response.topics {
            for partition in &topic.partitions {
                check_code(topic.name.as_str(), partition.error_code, None)?;
            }
        }
        Ok(())
    }

    /// Incrementally alter the configs of the given resources: set, delete,
    /// append, or subtract individual keys without replacing the whole set
    /// (Kafka's `incrementalAlterConfigs`).
    ///
    /// Per-broker configs are served by that broker; everything else (topic
    /// configs, cluster-wide broker defaults) is controller-routed.
    ///
    /// # Errors
    /// Returns the first per-resource broker error code, or a routing error.
    pub async fn incremental_alter_configs(
        &self,
        configs: Vec<(ConfigResource, Vec<AlterConfigOp>)>,
        options: AlterConfigsOptions,
    ) -> Result<()> {
        let resources: Vec<ConfigResource> = configs
            .iter()
            .map(|(resource, _)| resource.clone())
            .collect();
        let request = IncrementalAlterConfigsRequestData {
            resources: configs
                .into_iter()
                .map(|(resource, ops)| resource.to_incremental(ops))
                .collect(),
            validate_only: options.validate_only,
            _unknown_tagged_fields: Vec::new(),
        };
        let response: IncrementalAlterConfigsResponseData = match broker_target(&resources) {
            Some(broker_id) => {
                let version = client_api_info(ApiKey::IncrementalAlterConfigs).max_version;
                self.send_metered(
                    broker_id,
                    ApiKey::IncrementalAlterConfigs,
                    version,
                    &request,
                )
                .await?
            },
            None => {
                self.route_to_controller(
                    ApiKey::IncrementalAlterConfigs,
                    &request,
                    |response: &IncrementalAlterConfigsResponseData| {
                        response
                            .responses
                            .iter()
                            .any(|result| is_not_controller(result.error_code))
                    },
                )
                .await?
            },
        };
        for result in &response.responses {
            check_code(
                result.resource_name.as_str(),
                result.error_code,
                result.error_message.as_ref(),
            )?;
        }
        Ok(())
    }

    /// Trigger leader election for the given partitions, or all partitions when
    /// `partitions` is empty. Routed to the controller.
    ///
    /// # Errors
    /// Returns the top-level error, the first per-partition error (other than the
    /// benign `ELECTION_NOT_NEEDED`), or a routing error.
    pub async fn elect_leaders(
        &self,
        election_type: ElectionType,
        partitions: Vec<TopicPartition>,
    ) -> Result<()> {
        let request = ElectLeadersRequestData {
            election_type: election_type.to_wire(),
            topic_partitions: elect_leaders_partitions(partitions),
            timeout_ms: self.request_timeout_ms,
            _unknown_tagged_fields: Vec::new(),
        };
        let response: ElectLeadersResponseData = self
            .route_to_controller(
                ApiKey::ElectLeaders,
                &request,
                |response: &ElectLeadersResponseData| is_not_controller(response.error_code),
            )
            .await?;
        check_code("", response.error_code, None)?;
        for topic in &response.replica_election_results {
            for partition in &topic.partition_result {
                // ELECTION_NOT_NEEDED means the desired leader is already current;
                // Java treats it as success, not a failure.
                if partition.error_code == i16::from(ErrorCode::ElectionNotNeeded) {
                    continue;
                }
                check_code(
                    topic.topic.as_str(),
                    partition.error_code,
                    partition.error_message.as_ref(),
                )?;
            }
        }
        Ok(())
    }

    /// Look up offsets (earliest/latest/by-timestamp) for the given partitions,
    /// routing each partition's request to its current leader.
    ///
    /// # Errors
    /// Returns a metadata/wire error, `LEADER_NOT_AVAILABLE` when a partition has
    /// no known leader, or the first per-partition broker error code.
    pub async fn list_offsets(
        &self,
        partitions: Vec<(TopicPartition, OffsetSpec)>,
    ) -> Result<Vec<ListOffsetsResult>> {
        if partitions.is_empty() {
            return Ok(Vec::new());
        }
        let topic_names = unique_topics(
            &partitions
                .iter()
                .map(|(tp, _)| tp.topic.as_str())
                .collect::<Vec<_>>(),
        );
        let metadata = self.wire.admin_metadata(Some(&topic_names)).await?;
        let mut by_leader: HashMap<i32, Vec<(String, i32, i64)>> = HashMap::new();
        for (topic_partition, spec) in &partitions {
            let leader =
                partition_leader(&metadata, &topic_partition.topic, topic_partition.partition)
                    .ok_or_else(|| leader_unavailable(topic_partition))?;
            by_leader.entry(leader).or_default().push((
                topic_partition.topic.clone(),
                topic_partition.partition,
                spec.to_wire(),
            ));
        }
        let version = client_api_info(ApiKey::ListOffsets).max_version;
        let mut results = Vec::with_capacity(partitions.len());
        for (leader, entries) in by_leader {
            let request = ListOffsetsRequestData {
                replica_id: -1,
                isolation_level: 0,
                topics: list_offsets_topics(entries),
                timeout_ms: self.request_timeout_ms,
                _unknown_tagged_fields: Vec::new(),
            };
            let response: ListOffsetsResponseData = self
                .send_metered(leader, ApiKey::ListOffsets, version, &request)
                .await?;
            for topic in response.topics {
                for partition in topic.partitions {
                    check_code(topic.name.as_str(), partition.error_code, None)?;
                    results.push(ListOffsetsResult {
                        partition: TopicPartition::new(
                            topic.name.as_str().to_owned(),
                            partition.partition_index,
                        ),
                        offset: partition.offset,
                        timestamp: partition.timestamp,
                        leader_epoch: (partition.leader_epoch >= 0)
                            .then_some(partition.leader_epoch),
                    });
                }
            }
        }
        Ok(results)
    }

    /// Delete records before the given offset on each partition (offset `-1`
    /// deletes up to the high watermark), routing each partition to its leader.
    ///
    /// # Errors
    /// Returns a metadata/wire error, `LEADER_NOT_AVAILABLE` when a partition has
    /// no known leader, or the first per-partition broker error code.
    pub async fn delete_records(
        &self,
        records: Vec<(TopicPartition, i64)>,
    ) -> Result<Vec<DeletedRecords>> {
        if records.is_empty() {
            return Ok(Vec::new());
        }
        let topic_names = unique_topics(
            &records
                .iter()
                .map(|(tp, _)| tp.topic.as_str())
                .collect::<Vec<_>>(),
        );
        let metadata = self.wire.admin_metadata(Some(&topic_names)).await?;
        let mut by_leader: HashMap<i32, Vec<(String, i32, i64)>> = HashMap::new();
        for (topic_partition, offset) in &records {
            let leader =
                partition_leader(&metadata, &topic_partition.topic, topic_partition.partition)
                    .ok_or_else(|| leader_unavailable(topic_partition))?;
            by_leader.entry(leader).or_default().push((
                topic_partition.topic.clone(),
                topic_partition.partition,
                *offset,
            ));
        }
        let version = client_api_info(ApiKey::DeleteRecords).max_version;
        let mut results = Vec::with_capacity(records.len());
        for (leader, entries) in by_leader {
            let request = DeleteRecordsRequestData {
                topics: delete_records_topics(entries),
                timeout_ms: self.request_timeout_ms,
                _unknown_tagged_fields: Vec::new(),
            };
            let response: DeleteRecordsResponseData = self
                .send_metered(leader, ApiKey::DeleteRecords, version, &request)
                .await?;
            for topic in response.topics {
                for partition in topic.partitions {
                    check_code(topic.name.as_str(), partition.error_code, None)?;
                    results.push(DeletedRecords {
                        partition: TopicPartition::new(
                            topic.name.as_str().to_owned(),
                            partition.partition_index,
                        ),
                        low_watermark: partition.low_watermark,
                    });
                }
            }
        }
        Ok(results)
    }

    /// Create a delegation token (routed to a live broker).
    ///
    /// # Errors
    /// Returns the broker error code or a wire error.
    pub async fn create_delegation_token(
        &self,
        options: CreateDelegationTokenOptions,
    ) -> Result<DelegationToken> {
        let (owner_principal_type, owner_principal_name) = match options.owner {
            Some((principal_type, principal_name)) => {
                (Some(principal_type.into()), Some(principal_name.into()))
            },
            None => (None, None),
        };
        let request = CreateDelegationTokenRequestData {
            owner_principal_type,
            owner_principal_name,
            renewers: options
                .renewers
                .into_iter()
                .map(|(principal_type, principal_name)| CreatableRenewers {
                    principal_type: principal_type.into(),
                    principal_name: principal_name.into(),
                    _unknown_tagged_fields: Vec::new(),
                })
                .collect(),
            max_lifetime_ms: options.max_lifetime_ms,
            _unknown_tagged_fields: Vec::new(),
        };
        let broker_id = self.wire.admin_any_broker_id()?;
        let version = client_api_info(ApiKey::CreateDelegationToken).max_version;
        let response: CreateDelegationTokenResponseData = self
            .send_metered(broker_id, ApiKey::CreateDelegationToken, version, &request)
            .await?;
        check_code("", response.error_code, None)?;
        Ok(DelegationToken {
            token_id: response.token_id.as_str().to_owned(),
            owner_principal_type: response.principal_type.as_str().to_owned(),
            owner_principal_name: response.principal_name.as_str().to_owned(),
            issue_timestamp_ms: response.issue_timestamp_ms,
            expiry_timestamp_ms: response.expiry_timestamp_ms,
            max_timestamp_ms: response.max_timestamp_ms,
            hmac: response.hmac.to_vec(),
            renewers: Vec::new(),
        })
    }

    /// Renew a delegation token by its HMAC, returning the new expiry timestamp.
    ///
    /// # Errors
    /// Returns the broker error code or a wire error.
    pub async fn renew_delegation_token(&self, hmac: Vec<u8>, renew_period_ms: i64) -> Result<i64> {
        let request = RenewDelegationTokenRequestData {
            hmac: hmac.into(),
            renew_period_ms,
            _unknown_tagged_fields: Vec::new(),
        };
        let broker_id = self.wire.admin_any_broker_id()?;
        let version = client_api_info(ApiKey::RenewDelegationToken).max_version;
        let response: RenewDelegationTokenResponseData = self
            .send_metered(broker_id, ApiKey::RenewDelegationToken, version, &request)
            .await?;
        check_code("", response.error_code, None)?;
        Ok(response.expiry_timestamp_ms)
    }

    /// Expire (or shorten the lifetime of) a delegation token by its HMAC,
    /// returning the new expiry timestamp.
    ///
    /// # Errors
    /// Returns the broker error code or a wire error.
    pub async fn expire_delegation_token(
        &self,
        hmac: Vec<u8>,
        expiry_time_period_ms: i64,
    ) -> Result<i64> {
        let request = ExpireDelegationTokenRequestData {
            hmac: hmac.into(),
            expiry_time_period_ms,
            _unknown_tagged_fields: Vec::new(),
        };
        let broker_id = self.wire.admin_any_broker_id()?;
        let version = client_api_info(ApiKey::ExpireDelegationToken).max_version;
        let response: ExpireDelegationTokenResponseData = self
            .send_metered(broker_id, ApiKey::ExpireDelegationToken, version, &request)
            .await?;
        check_code("", response.error_code, None)?;
        Ok(response.expiry_timestamp_ms)
    }

    /// Describe delegation tokens, optionally restricted to the given owners
    /// (`(principal_type, principal_name)` pairs; empty = all visible tokens).
    ///
    /// # Errors
    /// Returns the broker error code or a wire error.
    pub async fn describe_delegation_token(
        &self,
        owners: Vec<(String, String)>,
    ) -> Result<Vec<DelegationToken>> {
        let owners_filter = if owners.is_empty() {
            None
        } else {
            Some(
                owners
                    .into_iter()
                    .map(
                        |(principal_type, principal_name)| DescribeDelegationTokenOwner {
                            principal_type: principal_type.into(),
                            principal_name: principal_name.into(),
                            _unknown_tagged_fields: Vec::new(),
                        },
                    )
                    .collect(),
            )
        };
        let request = DescribeDelegationTokenRequestData {
            owners: owners_filter,
            _unknown_tagged_fields: Vec::new(),
        };
        let broker_id = self.wire.admin_any_broker_id()?;
        let version = client_api_info(ApiKey::DescribeDelegationToken).max_version;
        let response: DescribeDelegationTokenResponseData = self
            .send_metered(
                broker_id,
                ApiKey::DescribeDelegationToken,
                version,
                &request,
            )
            .await?;
        check_code("", response.error_code, None)?;
        let tokens = response
            .tokens
            .into_iter()
            .map(|token| DelegationToken {
                token_id: token.token_id.as_str().to_owned(),
                owner_principal_type: token.principal_type.as_str().to_owned(),
                owner_principal_name: token.principal_name.as_str().to_owned(),
                issue_timestamp_ms: token.issue_timestamp,
                expiry_timestamp_ms: token.expiry_timestamp,
                max_timestamp_ms: token.max_timestamp,
                hmac: token.hmac.to_vec(),
                renewers: token
                    .renewers
                    .into_iter()
                    .map(|renewer| {
                        (
                            renewer.principal_type.as_str().to_owned(),
                            renewer.principal_name.as_str().to_owned(),
                        )
                    })
                    .collect(),
            })
            .collect();
        Ok(tokens)
    }

    /// Move replicas to specific log directories on their hosting brokers.
    ///
    /// # Errors
    /// Returns a wire error or the first per-partition broker error code.
    pub async fn alter_replica_log_dirs(
        &self,
        assignments: Vec<ReplicaLogDirAssignment>,
    ) -> Result<()> {
        let mut by_broker: HashMap<i32, Vec<(String, String, i32)>> = HashMap::new();
        for assignment in assignments {
            by_broker.entry(assignment.broker_id).or_default().push((
                assignment.log_dir,
                assignment.topic_partition.topic,
                assignment.topic_partition.partition,
            ));
        }
        let version = client_api_info(ApiKey::AlterReplicaLogDirs).max_version;
        for (broker_id, entries) in by_broker {
            let request = AlterReplicaLogDirsRequestData {
                dirs: build_alter_replica_log_dirs(entries),
                _unknown_tagged_fields: Vec::new(),
            };
            let response: AlterReplicaLogDirsResponseData = self
                .send_metered(broker_id, ApiKey::AlterReplicaLogDirs, version, &request)
                .await?;
            for result in &response.results {
                for partition in &result.partitions {
                    check_code(result.topic_name.as_str(), partition.error_code, None)?;
                }
            }
        }
        Ok(())
    }

    /// Fence the active producers of the given transactional ids by bumping each
    /// producer epoch via `InitProducerId` against the transaction coordinator.
    ///
    /// # Errors
    /// Returns the coordinator-lookup error or the per-id broker error code.
    pub async fn fence_producers(
        &self,
        transactional_ids: Vec<String>,
    ) -> Result<Vec<FencedProducer>> {
        let mut fenced = Vec::with_capacity(transactional_ids.len());
        for transactional_id in &transactional_ids {
            let request = InitProducerIdRequestData {
                transactional_id: Some(transactional_id.clone().into()),
                transaction_timeout_ms: FENCE_PRODUCER_TXN_TIMEOUT_MS,
                producer_id: -1,
                producer_epoch: -1,
                enable2_pc: false,
                keep_prepared_txn: false,
                _unknown_tagged_fields: Vec::new(),
            };
            let (response, _coordinator) = self
                .route_to_coordinator(
                    transactional_id,
                    1,
                    ApiKey::InitProducerId,
                    &request,
                    |response: &InitProducerIdResponseData| {
                        is_coordinator_error(response.error_code)
                    },
                )
                .await?;
            check_code(transactional_id, response.error_code, None)?;
            fenced.push(FencedProducer {
                transactional_id: transactional_id.clone(),
                producer_id: response.producer_id,
                producer_epoch: response.producer_epoch,
            });
        }
        Ok(fenced)
    }

    /// Forcibly abort a hanging transaction on a partition by writing an abort
    /// marker to the partition leader (`WriteTxnMarkers`).
    ///
    /// # Errors
    /// Returns a metadata/wire error, `LEADER_NOT_AVAILABLE`, or the partition
    /// broker error code.
    pub async fn abort_transaction(&self, spec: AbortTransactionSpec) -> Result<()> {
        let topic = spec.topic_partition.topic.clone();
        let metadata = self
            .wire
            .admin_metadata(Some(std::slice::from_ref(&topic)))
            .await?;
        let leader = partition_leader(&metadata, &topic, spec.topic_partition.partition)
            .ok_or_else(|| leader_unavailable(&spec.topic_partition))?;
        let request = WriteTxnMarkersRequestData {
            markers: vec![WritableTxnMarker {
                producer_id: spec.producer_id,
                producer_epoch: spec.producer_epoch,
                transaction_result: false, // abort
                topics: vec![WritableTxnMarkerTopic {
                    name: topic.clone().into(),
                    partition_indexes: vec![spec.topic_partition.partition],
                    _unknown_tagged_fields: Vec::new(),
                }],
                coordinator_epoch: spec.coordinator_epoch,
                transaction_version: 0,
                _unknown_tagged_fields: Vec::new(),
            }],
            _unknown_tagged_fields: Vec::new(),
        };
        let version = client_api_info(ApiKey::WriteTxnMarkers).max_version;
        let response: WriteTxnMarkersResponseData = self
            .send_metered(leader, ApiKey::WriteTxnMarkers, version, &request)
            .await?;
        for marker in &response.markers {
            for topic_result in &marker.topics {
                for partition in &topic_result.partitions {
                    check_code(topic_result.name.as_str(), partition.error_code, None)?;
                }
            }
        }
        Ok(())
    }

    /// Describe the cluster's supported and finalized features (via
    /// `ApiVersions`).
    ///
    /// # Errors
    /// Returns the broker error code or a wire error.
    pub async fn describe_features(&self) -> Result<FeatureMetadata> {
        // ApiVersions v3+ rejects an empty `client_software_name` with
        // INVALID_REQUEST, so the request must identify the client.
        let request = ApiVersionsRequestData {
            client_software_name: "kacrab".to_owned().into(),
            client_software_version: env!("CARGO_PKG_VERSION").to_owned().into(),
            _unknown_tagged_fields: Vec::new(),
        };
        let broker_id = self.wire.admin_any_broker_id()?;
        let version = client_api_info(ApiKey::ApiVersions).max_version;
        let response: ApiVersionsResponseData = self
            .send_metered(broker_id, ApiKey::ApiVersions, version, &request)
            .await?;
        check_code("", response.error_code, None)?;
        let supported_features = response
            .supported_features
            .into_iter()
            .map(|feature| {
                (
                    feature.name.as_str().to_owned(),
                    SupportedVersionRange {
                        min_version: feature.min_version,
                        max_version: feature.max_version,
                    },
                )
            })
            .collect();
        let finalized_features = response
            .finalized_features
            .into_iter()
            .map(|feature| {
                (
                    feature.name.as_str().to_owned(),
                    FinalizedVersionRange {
                        min_version_level: feature.min_version_level,
                        max_version_level: feature.max_version_level,
                    },
                )
            })
            .collect();
        Ok(FeatureMetadata {
            finalized_features_epoch: (response.finalized_features_epoch >= 0)
                .then_some(response.finalized_features_epoch),
            supported_features,
            finalized_features,
        })
    }

    /// List all groups in the cluster (any type), aggregated across brokers.
    /// Mirrors Java's `listGroups`.
    ///
    /// # Errors
    /// Returns a wire error or the first broker top-level error code.
    pub async fn list_groups(
        &self,
        options: ListConsumerGroupsOptions,
    ) -> Result<Vec<ConsumerGroupListing>> {
        let request = ListGroupsRequestData {
            states_filter: options.states_filter.into_iter().map(Into::into).collect(),
            types_filter: options.types_filter.into_iter().map(Into::into).collect(),
            _unknown_tagged_fields: Vec::new(),
        };
        let metadata = self.wire.admin_metadata(None).await?;
        let broker_ids: Vec<i32> = metadata
            .brokers
            .iter()
            .map(|broker| broker.node_id)
            .collect();
        let version = client_api_info(ApiKey::ListGroups).max_version;
        let mut listings = Vec::new();
        for broker_id in broker_ids {
            let response: ListGroupsResponseData = self
                .send_metered(broker_id, ApiKey::ListGroups, version, &request)
                .await?;
            check_code("", response.error_code, None)?;
            for group in response.groups {
                listings.push(ConsumerGroupListing {
                    group_id: group.group_id.as_str().to_owned(),
                    is_simple_consumer_group: group.protocol_type.as_str().is_empty(),
                    state: non_empty(group.group_state.as_str())
                        .map(|state| GroupState::from_broker(&state)),
                    group_type: non_empty(group.group_type.as_str())
                        .map(|group_type| GroupType::from_broker(&group_type)),
                });
            }
        }
        Ok(listings)
    }

    /// Describe the given classic-protocol groups. Like
    /// [`describe_consumer_groups`](Self::describe_consumer_groups), it uses the
    /// `DescribeGroups` API against each group's coordinator.
    ///
    /// # Errors
    /// As [`describe_consumer_groups`](Self::describe_consumer_groups).
    pub async fn describe_classic_groups(
        &self,
        group_ids: Vec<String>,
        options: DescribeConsumerGroupsOptions,
    ) -> Result<Vec<ConsumerGroupDescription>> {
        self.describe_consumer_groups(group_ids, options).await
    }

    /// Remove members from a consumer group via `LeaveGroup` (e.g. to evict
    /// static members), routed to the group coordinator.
    ///
    /// # Errors
    /// Returns the coordinator-lookup error, the top-level, or first per-member
    /// broker error code.
    pub async fn remove_members_from_consumer_group(
        &self,
        group_id: &str,
        members: Vec<MemberToRemove>,
    ) -> Result<()> {
        let request = LeaveGroupRequestData {
            group_id: group_id.to_owned().into(),
            member_id: String::new().into(),
            members: members
                .into_iter()
                .map(|member| MemberIdentity {
                    member_id: member.member_id.unwrap_or_default().into(),
                    group_instance_id: member.group_instance_id.map(Into::into),
                    reason: None,
                    _unknown_tagged_fields: Vec::new(),
                })
                .collect(),
            _unknown_tagged_fields: Vec::new(),
        };
        let (response, _coordinator) = self
            .route_to_coordinator(
                group_id,
                0,
                ApiKey::LeaveGroup,
                &request,
                |response: &LeaveGroupResponseData| is_coordinator_error(response.error_code),
            )
            .await?;
        check_code(group_id, response.error_code, None)?;
        for member in &response.members {
            check_code(group_id, member.error_code, None)?;
        }
        Ok(())
    }

    /// List the config resources of the given types (e.g. topics, brokers,
    /// client-metrics subscriptions). Mirrors Java's `listConfigResources`.
    ///
    /// # Errors
    /// Returns the broker error code or a wire error.
    pub async fn list_config_resources(
        &self,
        resource_types: Vec<ResourceType>,
    ) -> Result<Vec<ConfigResource>> {
        let request = ListConfigResourcesRequestData {
            resource_types: resource_types.iter().map(|ty| ty.to_wire()).collect(),
            _unknown_tagged_fields: Vec::new(),
        };
        let broker_id = self.wire.admin_any_broker_id()?;
        let version = client_api_info(ApiKey::ListConfigResources).max_version;
        let response: ListConfigResourcesResponseData = self
            .send_metered(broker_id, ApiKey::ListConfigResources, version, &request)
            .await?;
        check_code("", response.error_code, None)?;
        Ok(response
            .config_resources
            .into_iter()
            .map(|resource| ConfigResource {
                resource_type: ResourceType::from_wire(resource.resource_type),
                name: resource.resource_name.as_str().to_owned(),
            })
            .collect())
    }

    /// List the names of client-metrics subscription resources. Mirrors Java's
    /// `listClientMetricsResources` (a `listConfigResources` over `ClientMetrics`).
    ///
    /// # Errors
    /// Returns the broker error code or a wire error.
    pub async fn list_client_metrics_resources(&self) -> Result<Vec<String>> {
        let resources = self
            .list_config_resources(vec![ResourceType::ClientMetrics])
            .await?;
        Ok(resources
            .into_iter()
            .map(|resource| resource.name)
            .collect())
    }

    /// Describe the log directory hosting each given replica
    /// `(partition, broker)`, including any pending future-replica directory.
    ///
    /// # Errors
    /// Returns a wire error or the first per-broker top-level error code.
    pub async fn describe_replica_log_dirs(
        &self,
        replicas: Vec<(TopicPartition, i32)>,
    ) -> Result<Vec<ReplicaLogDirInfo>> {
        let mut by_broker: HashMap<i32, Vec<TopicPartition>> = HashMap::new();
        for (topic_partition, broker_id) in &replicas {
            by_broker
                .entry(*broker_id)
                .or_default()
                .push(topic_partition.clone());
        }
        let version = client_api_info(ApiKey::DescribeLogDirs).max_version;
        let mut infos = Vec::with_capacity(replicas.len());
        for (broker_id, partitions) in by_broker {
            let request = DescribeLogDirsRequestData {
                topics: describe_log_dirs_topics(&partitions),
                _unknown_tagged_fields: Vec::new(),
            };
            let response: DescribeLogDirsResponseData = self
                .send_metered(broker_id, ApiKey::DescribeLogDirs, version, &request)
                .await?;
            check_code("", response.error_code, None)?;
            for partition in partitions {
                let mut current_log_dir = None;
                let mut future_log_dir = None;
                for result in &response.results {
                    for topic in &result.topics {
                        if topic.name.as_str() != partition.topic {
                            continue;
                        }
                        for entry in &topic.partitions {
                            if entry.partition_index != partition.partition {
                                continue;
                            }
                            let dir = result.log_dir.as_str().to_owned();
                            if entry.is_future_key {
                                future_log_dir = Some(dir);
                            } else {
                                current_log_dir = Some(dir);
                            }
                        }
                    }
                }
                infos.push(ReplicaLogDirInfo {
                    topic_partition: partition,
                    broker_id,
                    current_log_dir,
                    future_log_dir,
                });
            }
        }
        Ok(infos)
    }

    /// Describe the `KRaft` metadata quorum state.
    ///
    /// # Errors
    /// Returns the broker error code, a wire error, or
    /// [`AdminError::MissingResult`] if the broker omits the quorum partition.
    pub async fn describe_metadata_quorum(&self) -> Result<QuorumInfo> {
        let request = DescribeQuorumRequestData {
            topics: vec![
                kacrab_protocol::generated::describe_quorum_request::TopicData {
                    topic_name: METADATA_QUORUM_TOPIC.to_owned().into(),
                    partitions: vec![
                        kacrab_protocol::generated::describe_quorum_request::PartitionData {
                            partition_index: 0,
                            _unknown_tagged_fields: Vec::new(),
                        },
                    ],
                    _unknown_tagged_fields: Vec::new(),
                },
            ],
            _unknown_tagged_fields: Vec::new(),
        };
        let broker_id = self.wire.admin_any_broker_id()?;
        let version = client_api_info(ApiKey::DescribeQuorum).max_version;
        let response: DescribeQuorumResponseData = self
            .send_metered(broker_id, ApiKey::DescribeQuorum, version, &request)
            .await?;
        check_code("", response.error_code, response.error_message.as_ref())?;
        let partition = response
            .topics
            .into_iter()
            .find(|topic| topic.topic_name.as_str() == METADATA_QUORUM_TOPIC)
            .and_then(|topic| topic.partitions.into_iter().next())
            .ok_or_else(|| AdminError::MissingResult {
                target: METADATA_QUORUM_TOPIC.to_owned(),
            })?;
        check_code(
            METADATA_QUORUM_TOPIC,
            partition.error_code,
            partition.error_message.as_ref(),
        )?;
        let map_state =
            |replica: &kacrab_protocol::generated::describe_quorum_response::ReplicaState| {
                QuorumReplicaState {
                    replica_id: replica.replica_id,
                    log_end_offset: replica.log_end_offset,
                }
            };
        Ok(QuorumInfo {
            leader_id: partition.leader_id,
            leader_epoch: partition.leader_epoch,
            high_watermark: partition.high_watermark,
            voters: partition.current_voters.iter().map(map_state).collect(),
            observers: partition.observers.iter().map(map_state).collect(),
        })
    }

    /// Add a `KRaft` voter to the metadata quorum (controller-routed).
    ///
    /// # Errors
    /// Returns the broker error code or a routing error.
    pub async fn add_raft_voter(
        &self,
        voter_id: i32,
        voter_directory_id: KafkaUuid,
        endpoints: Vec<RaftVoterEndpoint>,
    ) -> Result<()> {
        let request = AddRaftVoterRequestData {
            cluster_id: None,
            timeout_ms: self.request_timeout_ms,
            voter_id,
            voter_directory_id,
            listeners: endpoints
                .into_iter()
                .map(|endpoint| Listener {
                    name: endpoint.name.into(),
                    host: endpoint.host.into(),
                    port: endpoint.port,
                    _unknown_tagged_fields: Vec::new(),
                })
                .collect(),
            ack_when_committed: true,
            _unknown_tagged_fields: Vec::new(),
        };
        let response: AddRaftVoterResponseData = self
            .route_to_controller(
                ApiKey::AddRaftVoter,
                &request,
                |response: &AddRaftVoterResponseData| is_not_controller(response.error_code),
            )
            .await?;
        check_code("", response.error_code, response.error_message.as_ref())?;
        Ok(())
    }

    /// Remove a `KRaft` voter from the metadata quorum (controller-routed).
    ///
    /// # Errors
    /// Returns the broker error code or a routing error.
    pub async fn remove_raft_voter(
        &self,
        voter_id: i32,
        voter_directory_id: KafkaUuid,
    ) -> Result<()> {
        let request = RemoveRaftVoterRequestData {
            cluster_id: None,
            voter_id,
            voter_directory_id,
            _unknown_tagged_fields: Vec::new(),
        };
        let response: RemoveRaftVoterResponseData = self
            .route_to_controller(
                ApiKey::RemoveRaftVoter,
                &request,
                |response: &RemoveRaftVoterResponseData| is_not_controller(response.error_code),
            )
            .await?;
        check_code("", response.error_code, response.error_message.as_ref())?;
        Ok(())
    }

    /// Force-terminate a hanging transaction by fencing its producer (a
    /// single-id [`fence_producers`](Self::fence_producers)). Mirrors Java's
    /// `forceTerminateTransaction`.
    ///
    /// # Errors
    /// As [`fence_producers`](Self::fence_producers).
    pub async fn force_terminate_transaction(
        &self,
        transactional_id: &str,
    ) -> Result<FencedProducer> {
        let mut fenced = self
            .fence_producers(vec![transactional_id.to_owned()])
            .await?;
        fenced.pop().ok_or_else(|| AdminError::MissingResult {
            target: transactional_id.to_owned(),
        })
    }

    /// Describe the given share groups (Kafka 4.x, KIP-932), routed per group to
    /// its coordinator.
    ///
    /// # Errors
    /// Returns the coordinator-lookup error, the per-group broker error code, or
    /// [`AdminError::MissingResult`] when the broker omits a group.
    pub async fn describe_share_groups(
        &self,
        group_ids: Vec<String>,
    ) -> Result<Vec<ShareGroupDescription>> {
        let mut described = Vec::with_capacity(group_ids.len());
        for group_id in &group_ids {
            let request = ShareGroupDescribeRequestData {
                group_ids: vec![group_id.clone().into()],
                include_authorized_operations: false,
                _unknown_tagged_fields: Vec::new(),
            };
            let (response, coordinator) = self
                .route_to_coordinator(
                    group_id,
                    0,
                    ApiKey::ShareGroupDescribe,
                    &request,
                    |response: &ShareGroupDescribeResponseData| {
                        response
                            .groups
                            .iter()
                            .any(|group| is_coordinator_error(group.error_code))
                    },
                )
                .await?;
            let group = response
                .groups
                .into_iter()
                .find(|group| group.group_id.as_str() == group_id)
                .ok_or_else(|| AdminError::MissingResult {
                    target: group_id.clone(),
                })?;
            check_code(group_id, group.error_code, group.error_message.as_ref())?;
            let members = group
                .members
                .into_iter()
                .map(|member| MemberDescription {
                    member_id: member.member_id.as_str().to_owned(),
                    group_instance_id: None,
                    rack_id: member.rack_id.map(|id| id.as_str().to_owned()),
                    client_id: member.client_id.as_str().to_owned(),
                    host: member.client_host.as_str().to_owned(),
                    assignment: member
                        .assignment
                        .topic_partitions
                        .into_iter()
                        .flat_map(|topic| {
                            let name = topic.topic_name.as_str().to_owned();
                            topic
                                .partitions
                                .into_iter()
                                .map(move |partition| TopicPartition::new(name.clone(), partition))
                        })
                        .collect(),
                    target_assignment: Vec::new(),
                    member_epoch: Some(member.member_epoch),
                    upgraded: None,
                })
                .collect();
            described.push(ShareGroupDescription {
                group_id: group.group_id.as_str().to_owned(),
                state: GroupState::from_broker(group.group_state.as_str()),
                group_epoch: group.group_epoch,
                assignor_name: group.assignor_name.as_str().to_owned(),
                members,
                coordinator,
                authorized_operations: decode_authorized_operations(group.authorized_operations),
            });
        }
        Ok(described)
    }

    /// Describe the given streams groups (Kafka 4.x, KIP-1071), routed per group
    /// to its coordinator.
    ///
    /// # Errors
    /// As [`describe_share_groups`](Self::describe_share_groups).
    pub async fn describe_streams_groups(
        &self,
        group_ids: Vec<String>,
    ) -> Result<Vec<StreamsGroupDescription>> {
        let mut described = Vec::with_capacity(group_ids.len());
        for group_id in &group_ids {
            let request = StreamsGroupDescribeRequestData {
                group_ids: vec![group_id.clone().into()],
                include_authorized_operations: false,
                _unknown_tagged_fields: Vec::new(),
            };
            let (response, coordinator) = self
                .route_to_coordinator(
                    group_id,
                    0,
                    ApiKey::StreamsGroupDescribe,
                    &request,
                    |response: &StreamsGroupDescribeResponseData| {
                        response
                            .groups
                            .iter()
                            .any(|group| is_coordinator_error(group.error_code))
                    },
                )
                .await?;
            let group = response
                .groups
                .into_iter()
                .find(|group| group.group_id.as_str() == group_id)
                .ok_or_else(|| AdminError::MissingResult {
                    target: group_id.clone(),
                })?;
            check_code(group_id, group.error_code, group.error_message.as_ref())?;
            let members = group
                .members
                .into_iter()
                .map(|member| MemberDescription {
                    member_id: member.member_id.as_str().to_owned(),
                    group_instance_id: member.instance_id.map(|id| id.as_str().to_owned()),
                    rack_id: member.rack_id.map(|id| id.as_str().to_owned()),
                    client_id: member.client_id.as_str().to_owned(),
                    host: member.client_host.as_str().to_owned(),
                    // Streams members are assigned tasks, not partitions.
                    assignment: Vec::new(),
                    target_assignment: Vec::new(),
                    member_epoch: Some(member.member_epoch),
                    upgraded: None,
                })
                .collect();
            described.push(StreamsGroupDescription {
                group_id: group.group_id.as_str().to_owned(),
                state: GroupState::from_broker(group.group_state.as_str()),
                group_epoch: group.group_epoch,
                members,
                coordinator,
                authorized_operations: decode_authorized_operations(group.authorized_operations),
            });
        }
        Ok(described)
    }

    /// List the committed share-group offsets, routed to the group coordinator.
    ///
    /// # Errors
    /// Returns the coordinator-lookup error or the first broker error code.
    pub async fn list_share_group_offsets(
        &self,
        group_id: &str,
        partitions: Vec<TopicPartition>,
    ) -> Result<Vec<GroupOffset>> {
        let topics = if partitions.is_empty() {
            None
        } else {
            let mut grouped: Vec<DescribeShareGroupOffsetsRequestTopic> = Vec::new();
            for partition in &partitions {
                if let Some(topic) = grouped
                    .iter_mut()
                    .find(|topic| topic.topic_name.as_str() == partition.topic)
                {
                    topic.partitions.push(partition.partition);
                } else {
                    grouped.push(DescribeShareGroupOffsetsRequestTopic {
                        topic_name: partition.topic.clone().into(),
                        partitions: vec![partition.partition],
                        _unknown_tagged_fields: Vec::new(),
                    });
                }
            }
            Some(grouped)
        };
        let request = DescribeShareGroupOffsetsRequestData {
            groups: vec![DescribeShareGroupOffsetsRequestGroup {
                group_id: group_id.to_owned().into(),
                topics,
                _unknown_tagged_fields: Vec::new(),
            }],
            _unknown_tagged_fields: Vec::new(),
        };
        let (response, _coordinator) = self
            .route_to_coordinator(
                group_id,
                0,
                ApiKey::DescribeShareGroupOffsets,
                &request,
                |response: &DescribeShareGroupOffsetsResponseData| {
                    response
                        .groups
                        .iter()
                        .any(|group| is_coordinator_error(group.error_code))
                },
            )
            .await?;
        let group = response
            .groups
            .into_iter()
            .find(|group| group.group_id.as_str() == group_id)
            .ok_or_else(|| AdminError::MissingResult {
                target: group_id.to_owned(),
            })?;
        check_code(group_id, group.error_code, group.error_message.as_ref())?;
        let mut offsets = Vec::new();
        for topic in group.topics {
            for partition in topic.partitions {
                check_code(
                    topic.topic_name.as_str(),
                    partition.error_code,
                    partition.error_message.as_ref(),
                )?;
                if partition.start_offset < 0 {
                    continue;
                }
                offsets.push(GroupOffset {
                    partition: TopicPartition::new(
                        topic.topic_name.as_str().to_owned(),
                        partition.partition_index,
                    ),
                    offset: build_offset(partition.start_offset, partition.leader_epoch, None),
                });
            }
        }
        Ok(offsets)
    }

    /// Reset share-group offsets to the given start offsets, routed to the group
    /// coordinator.
    ///
    /// # Errors
    /// Returns the coordinator-lookup error, the top-level, or first per-partition
    /// broker error code.
    pub async fn alter_share_group_offsets(
        &self,
        group_id: &str,
        offsets: Vec<(TopicPartition, i64)>,
    ) -> Result<()> {
        let mut topics: Vec<AlterShareGroupOffsetsRequestTopic> = Vec::new();
        for (topic_partition, start_offset) in offsets {
            let partition = AlterShareGroupOffsetsRequestPartition {
                partition_index: topic_partition.partition,
                start_offset,
                _unknown_tagged_fields: Vec::new(),
            };
            if let Some(topic) = topics
                .iter_mut()
                .find(|topic| topic.topic_name.as_str() == topic_partition.topic)
            {
                topic.partitions.push(partition);
            } else {
                topics.push(AlterShareGroupOffsetsRequestTopic {
                    topic_name: topic_partition.topic.clone().into(),
                    partitions: vec![partition],
                    _unknown_tagged_fields: Vec::new(),
                });
            }
        }
        let request = AlterShareGroupOffsetsRequestData {
            group_id: group_id.to_owned().into(),
            topics,
            _unknown_tagged_fields: Vec::new(),
        };
        let (response, _coordinator) = self
            .route_to_coordinator(
                group_id,
                0,
                ApiKey::AlterShareGroupOffsets,
                &request,
                |response: &AlterShareGroupOffsetsResponseData| {
                    is_coordinator_error(response.error_code)
                },
            )
            .await?;
        check_code(
            group_id,
            response.error_code,
            response.error_message.as_ref(),
        )?;
        for topic in &response.responses {
            for partition in &topic.partitions {
                check_code(
                    topic.topic_name.as_str(),
                    partition.error_code,
                    partition.error_message.as_ref(),
                )?;
            }
        }
        Ok(())
    }

    /// Delete share-group offsets for the given topics, routed to the group
    /// coordinator.
    ///
    /// # Errors
    /// Returns the coordinator-lookup error, the top-level, or first per-topic
    /// broker error code.
    pub async fn delete_share_group_offsets(
        &self,
        group_id: &str,
        topics: Vec<String>,
    ) -> Result<()> {
        let request = DeleteShareGroupOffsetsRequestData {
            group_id: group_id.to_owned().into(),
            topics: topics
                .into_iter()
                .map(|topic_name| DeleteShareGroupOffsetsRequestTopic {
                    topic_name: topic_name.into(),
                    _unknown_tagged_fields: Vec::new(),
                })
                .collect(),
            _unknown_tagged_fields: Vec::new(),
        };
        let (response, _coordinator) = self
            .route_to_coordinator(
                group_id,
                0,
                ApiKey::DeleteShareGroupOffsets,
                &request,
                |response: &DeleteShareGroupOffsetsResponseData| {
                    is_coordinator_error(response.error_code)
                },
            )
            .await?;
        check_code(
            group_id,
            response.error_code,
            response.error_message.as_ref(),
        )?;
        for topic in &response.responses {
            check_code(
                topic.topic_name.as_str(),
                topic.error_code,
                topic.error_message.as_ref(),
            )?;
        }
        Ok(())
    }

    /// Delete share groups by id (Kafka 4.x). Uses the `DeleteGroups` API, like
    /// [`delete_consumer_groups`](Self::delete_consumer_groups).
    ///
    /// # Errors
    /// As [`delete_consumer_groups`](Self::delete_consumer_groups).
    pub async fn delete_share_groups(&self, group_ids: Vec<String>) -> Result<()> {
        self.delete_consumer_groups(group_ids).await
    }

    /// List the committed offsets of a streams group (Kafka 4.x). Streams-group
    /// offsets are stored like consumer-group offsets, so this uses the same
    /// `OffsetFetch` path as
    /// [`list_consumer_group_offsets`](Self::list_consumer_group_offsets).
    ///
    /// # Errors
    /// As [`list_consumer_group_offsets`](Self::list_consumer_group_offsets).
    pub async fn list_streams_group_offsets(
        &self,
        group_id: &str,
        options: ListConsumerGroupOffsetsOptions,
    ) -> Result<Vec<GroupOffset>> {
        self.list_consumer_group_offsets(group_id, options).await
    }

    /// Alter (commit) the offsets of a streams group (Kafka 4.x), using the same
    /// `OffsetCommit` path as
    /// [`alter_consumer_group_offsets`](Self::alter_consumer_group_offsets).
    ///
    /// # Errors
    /// As [`alter_consumer_group_offsets`](Self::alter_consumer_group_offsets).
    pub async fn alter_streams_group_offsets(
        &self,
        group_id: &str,
        offsets: Vec<(TopicPartition, OffsetAndMetadata)>,
    ) -> Result<()> {
        self.alter_consumer_group_offsets(group_id, offsets).await
    }

    /// Delete committed offsets of a streams group (Kafka 4.x), using the same
    /// `OffsetDelete` path as
    /// [`delete_consumer_group_offsets`](Self::delete_consumer_group_offsets).
    ///
    /// # Errors
    /// As [`delete_consumer_group_offsets`](Self::delete_consumer_group_offsets).
    pub async fn delete_streams_group_offsets(
        &self,
        group_id: &str,
        partitions: Vec<TopicPartition>,
    ) -> Result<()> {
        self.delete_consumer_group_offsets(group_id, partitions)
            .await
    }

    /// Delete streams groups by id (Kafka 4.x). Uses the `DeleteGroups` API, like
    /// [`delete_consumer_groups`](Self::delete_consumer_groups).
    ///
    /// # Errors
    /// As [`delete_consumer_groups`](Self::delete_consumer_groups).
    pub async fn delete_streams_groups(&self, group_ids: Vec<String>) -> Result<()> {
        self.delete_consumer_groups(group_ids).await
    }

    /// Describe client quotas matching the given filter components. With
    /// `strict`, only entities whose types exactly equal the filter set match.
    ///
    /// # Errors
    /// Returns the broker error code or a wire error.
    pub async fn describe_client_quotas(
        &self,
        filter: Vec<ClientQuotaFilterComponent>,
        strict: bool,
    ) -> Result<Vec<ClientQuotaEntry>> {
        let request = DescribeClientQuotasRequestData {
            components: filter
                .into_iter()
                .map(|component| {
                    let (match_type, match_value) = match component.match_type {
                        ClientQuotaMatch::Exact(name) => (0, Some(name.into())),
                        ClientQuotaMatch::Default => (1, None),
                        ClientQuotaMatch::Any => (2, None),
                    };
                    ComponentData {
                        entity_type: component.entity_type.into(),
                        match_type,
                        r#match: match_value,
                        _unknown_tagged_fields: Vec::new(),
                    }
                })
                .collect(),
            strict,
            _unknown_tagged_fields: Vec::new(),
        };
        let broker_id = self.wire.admin_any_broker_id()?;
        let version = client_api_info(ApiKey::DescribeClientQuotas).max_version;
        let response: DescribeClientQuotasResponseData = self
            .send_metered(broker_id, ApiKey::DescribeClientQuotas, version, &request)
            .await?;
        check_code("", response.error_code, response.error_message.as_ref())?;
        let mut entries = Vec::new();
        for entry in response.entries.unwrap_or_default() {
            let entity = ClientQuotaEntity {
                entries: entry
                    .entity
                    .into_iter()
                    .map(|component| {
                        (
                            component.entity_type.as_str().to_owned(),
                            component.entity_name.map(|name| name.as_str().to_owned()),
                        )
                    })
                    .collect(),
            };
            let quotas = entry
                .values
                .into_iter()
                .map(|value| (value.key.as_str().to_owned(), value.value))
                .collect();
            entries.push(ClientQuotaEntry { entity, quotas });
        }
        Ok(entries)
    }

    /// Alter client quotas (controller-routed). A [`ClientQuotaOp`] with a `None`
    /// value removes that quota.
    ///
    /// # Errors
    /// Returns the first per-entity broker error code or a routing error.
    pub async fn alter_client_quotas(
        &self,
        alterations: Vec<ClientQuotaAlteration>,
        validate_only: bool,
    ) -> Result<()> {
        let request = AlterClientQuotasRequestData {
            entries: alterations
                .into_iter()
                .map(|alteration| AlterClientQuotasEntry {
                    entity: alteration
                        .entity
                        .entries
                        .into_iter()
                        .map(|(entity_type, entity_name)| AlterClientQuotasEntity {
                            entity_type: entity_type.into(),
                            entity_name: entity_name.map(Into::into),
                            _unknown_tagged_fields: Vec::new(),
                        })
                        .collect(),
                    ops: alteration
                        .ops
                        .into_iter()
                        .map(|op| OpData {
                            key: op.key.into(),
                            value: op.value.unwrap_or(0.0),
                            remove: op.value.is_none(),
                            _unknown_tagged_fields: Vec::new(),
                        })
                        .collect(),
                    _unknown_tagged_fields: Vec::new(),
                })
                .collect(),
            validate_only,
            _unknown_tagged_fields: Vec::new(),
        };
        let response: AlterClientQuotasResponseData = self
            .route_to_controller(
                ApiKey::AlterClientQuotas,
                &request,
                |response: &AlterClientQuotasResponseData| {
                    response
                        .entries
                        .iter()
                        .any(|entry| is_not_controller(entry.error_code))
                },
            )
            .await?;
        for entry in &response.entries {
            check_code("", entry.error_code, entry.error_message.as_ref())?;
        }
        Ok(())
    }

    /// Describe the SCRAM credentials of the given users (or all users when
    /// `users` is empty).
    ///
    /// # Errors
    /// Returns the top-level or first per-user broker error code, or a wire error.
    pub async fn describe_user_scram_credentials(
        &self,
        users: Vec<String>,
    ) -> Result<Vec<UserScramCredentials>> {
        let users_filter = if users.is_empty() {
            None
        } else {
            Some(
                users
                    .iter()
                    .map(|user| UserName {
                        name: user.clone().into(),
                        _unknown_tagged_fields: Vec::new(),
                    })
                    .collect(),
            )
        };
        let request = DescribeUserScramCredentialsRequestData {
            users: users_filter,
            _unknown_tagged_fields: Vec::new(),
        };
        let broker_id = self.wire.admin_any_broker_id()?;
        let version = client_api_info(ApiKey::DescribeUserScramCredentials).max_version;
        let response: DescribeUserScramCredentialsResponseData = self
            .send_metered(
                broker_id,
                ApiKey::DescribeUserScramCredentials,
                version,
                &request,
            )
            .await?;
        check_code("", response.error_code, response.error_message.as_ref())?;
        let mut all = Vec::with_capacity(response.results.len());
        for result in response.results {
            check_code(
                result.user.as_str(),
                result.error_code,
                result.error_message.as_ref(),
            )?;
            let credentials = result
                .credential_infos
                .into_iter()
                .map(|info| ScramCredentialInfo {
                    mechanism: ScramMechanism::from_wire(info.mechanism),
                    iterations: info.iterations,
                })
                .collect();
            all.push(UserScramCredentials {
                user: result.user.as_str().to_owned(),
                credentials,
            });
        }
        Ok(all)
    }

    /// Create/update and delete SCRAM credentials (controller-routed). The caller
    /// supplies the already-salted password for each upsertion.
    ///
    /// # Errors
    /// Returns the first per-user broker error code or a routing error.
    pub async fn alter_user_scram_credentials(
        &self,
        deletions: Vec<ScramCredentialDeletion>,
        upsertions: Vec<ScramCredentialUpsertion>,
    ) -> Result<()> {
        let request = AlterUserScramCredentialsRequestData {
            deletions: deletions
                .into_iter()
                .map(|deletion| WireScramCredentialDeletion {
                    name: deletion.user.into(),
                    mechanism: deletion.mechanism.to_wire(),
                    _unknown_tagged_fields: Vec::new(),
                })
                .collect(),
            upsertions: upsertions
                .into_iter()
                .map(|upsertion| WireScramCredentialUpsertion {
                    name: upsertion.user.into(),
                    mechanism: upsertion.mechanism.to_wire(),
                    iterations: upsertion.iterations,
                    salt: upsertion.salt.into(),
                    salted_password: upsertion.salted_password.into(),
                    _unknown_tagged_fields: Vec::new(),
                })
                .collect(),
            _unknown_tagged_fields: Vec::new(),
        };
        let response: AlterUserScramCredentialsResponseData = self
            .route_to_controller(
                ApiKey::AlterUserScramCredentials,
                &request,
                |response: &AlterUserScramCredentialsResponseData| {
                    response
                        .results
                        .iter()
                        .any(|result| is_not_controller(result.error_code))
                },
            )
            .await?;
        for result in &response.results {
            check_code(
                result.user.as_str(),
                result.error_code,
                result.error_message.as_ref(),
            )?;
        }
        Ok(())
    }

    /// Describe the ACL bindings matching `filter`.
    ///
    /// # Errors
    /// Returns the broker error code or a wire error.
    pub async fn describe_acls(&self, filter: AclBindingFilter) -> Result<Vec<AclBinding>> {
        let request = DescribeAclsRequestData {
            resource_type_filter: filter.resource_type.to_wire(),
            resource_name_filter: filter.resource_name.map(Into::into),
            pattern_type_filter: filter.pattern_type.to_wire(),
            principal_filter: filter.principal.map(Into::into),
            host_filter: filter.host.map(Into::into),
            operation: filter.operation.to_wire(),
            permission_type: filter.permission_type.to_wire(),
            _unknown_tagged_fields: Vec::new(),
        };
        let broker_id = self.wire.admin_any_broker_id()?;
        let version = client_api_info(ApiKey::DescribeAcls).max_version;
        let response: DescribeAclsResponseData = self
            .send_metered(broker_id, ApiKey::DescribeAcls, version, &request)
            .await?;
        check_code("", response.error_code, response.error_message.as_ref())?;
        let mut bindings = Vec::new();
        for resource in response.resources {
            let resource_type = AclResourceType::from_wire(resource.resource_type);
            let resource_name = resource.resource_name.as_str().to_owned();
            let pattern_type = AclPatternType::from_wire(resource.pattern_type);
            for acl in resource.acls {
                bindings.push(AclBinding {
                    resource_type,
                    resource_name: resource_name.clone(),
                    pattern_type,
                    principal: acl.principal.as_str().to_owned(),
                    host: acl.host.as_str().to_owned(),
                    operation: AclOperation::from_wire(acl.operation),
                    permission_type: AclPermissionType::from_wire(acl.permission_type),
                });
            }
        }
        Ok(bindings)
    }

    /// Create ACL bindings (controller-routed).
    ///
    /// # Errors
    /// Returns the first per-binding broker error code or a routing error.
    pub async fn create_acls(&self, bindings: Vec<AclBinding>) -> Result<()> {
        let request = CreateAclsRequestData {
            creations: bindings
                .into_iter()
                .map(|binding| AclCreation {
                    resource_type: binding.resource_type.to_wire(),
                    resource_name: binding.resource_name.into(),
                    resource_pattern_type: binding.pattern_type.to_wire(),
                    principal: binding.principal.into(),
                    host: binding.host.into(),
                    operation: binding.operation.to_wire(),
                    permission_type: binding.permission_type.to_wire(),
                    _unknown_tagged_fields: Vec::new(),
                })
                .collect(),
            _unknown_tagged_fields: Vec::new(),
        };
        let response: CreateAclsResponseData = self
            .route_to_controller(
                ApiKey::CreateAcls,
                &request,
                |response: &CreateAclsResponseData| {
                    response
                        .results
                        .iter()
                        .any(|result| is_not_controller(result.error_code))
                },
            )
            .await?;
        for result in &response.results {
            check_code("", result.error_code, result.error_message.as_ref())?;
        }
        Ok(())
    }

    /// Delete ACL bindings matching the given filters, returning the bindings
    /// that were deleted (controller-routed).
    ///
    /// # Errors
    /// Returns the first per-filter/per-binding broker error code or a routing
    /// error.
    pub async fn delete_acls(&self, filters: Vec<AclBindingFilter>) -> Result<Vec<AclBinding>> {
        let request = DeleteAclsRequestData {
            filters: filters
                .into_iter()
                .map(|filter| DeleteAclsFilter {
                    resource_type_filter: filter.resource_type.to_wire(),
                    resource_name_filter: filter.resource_name.map(Into::into),
                    pattern_type_filter: filter.pattern_type.to_wire(),
                    principal_filter: filter.principal.map(Into::into),
                    host_filter: filter.host.map(Into::into),
                    operation: filter.operation.to_wire(),
                    permission_type: filter.permission_type.to_wire(),
                    _unknown_tagged_fields: Vec::new(),
                })
                .collect(),
            _unknown_tagged_fields: Vec::new(),
        };
        let response: DeleteAclsResponseData = self
            .route_to_controller(
                ApiKey::DeleteAcls,
                &request,
                |response: &DeleteAclsResponseData| {
                    response
                        .filter_results
                        .iter()
                        .any(|result| is_not_controller(result.error_code))
                },
            )
            .await?;
        let mut deleted = Vec::new();
        for filter_result in response.filter_results {
            check_code(
                "",
                filter_result.error_code,
                filter_result.error_message.as_ref(),
            )?;
            for acl in filter_result.matching_acls {
                check_code("", acl.error_code, acl.error_message.as_ref())?;
                deleted.push(AclBinding {
                    resource_type: AclResourceType::from_wire(acl.resource_type),
                    resource_name: acl.resource_name.as_str().to_owned(),
                    pattern_type: AclPatternType::from_wire(acl.pattern_type),
                    principal: acl.principal.as_str().to_owned(),
                    host: acl.host.as_str().to_owned(),
                    operation: AclOperation::from_wire(acl.operation),
                    permission_type: AclPermissionType::from_wire(acl.permission_type),
                });
            }
        }
        Ok(deleted)
    }

    /// Describe the log directories of the given brokers (or all brokers when
    /// `broker_ids` is empty), optionally restricted to `partitions` (empty =
    /// all). Each broker reports its own dirs.
    ///
    /// # Errors
    /// Returns a wire error or the first per-broker top-level error code.
    pub async fn describe_log_dirs(
        &self,
        broker_ids: Vec<i32>,
        partitions: Vec<TopicPartition>,
    ) -> Result<Vec<BrokerLogDirs>> {
        let broker_ids = if broker_ids.is_empty() {
            let metadata = self.wire.admin_metadata(None).await?;
            metadata
                .brokers
                .iter()
                .map(|broker| broker.node_id)
                .collect()
        } else {
            broker_ids
        };
        let request = DescribeLogDirsRequestData {
            topics: describe_log_dirs_topics(&partitions),
            _unknown_tagged_fields: Vec::new(),
        };
        let version = client_api_info(ApiKey::DescribeLogDirs).max_version;
        let mut all = Vec::with_capacity(broker_ids.len());
        for broker_id in broker_ids {
            let response: DescribeLogDirsResponseData = self
                .send_metered(broker_id, ApiKey::DescribeLogDirs, version, &request)
                .await?;
            check_code("", response.error_code, None)?;
            let log_dirs = response
                .results
                .into_iter()
                .map(|result| {
                    let error = ErrorCode::from(result.error_code);
                    let replicas = result
                        .topics
                        .into_iter()
                        .flat_map(|topic| {
                            let name = topic.name.as_str().to_owned();
                            topic.partitions.into_iter().map(move |partition| {
                                (
                                    TopicPartition::new(name.clone(), partition.partition_index),
                                    LogDirReplicaInfo {
                                        size: partition.partition_size,
                                        offset_lag: partition.offset_lag,
                                        is_future: partition.is_future_key,
                                    },
                                )
                            })
                        })
                        .collect();
                    LogDirDescription {
                        log_dir: result.log_dir.as_str().to_owned(),
                        error: error.is_error().then_some(error),
                        total_bytes: result.total_bytes,
                        usable_bytes: result.usable_bytes,
                        replicas,
                    }
                })
                .collect();
            all.push(BrokerLogDirs {
                broker_id,
                log_dirs,
            });
        }
        Ok(all)
    }

    /// Start or cancel partition reassignments (controller-routed). A
    /// [`NewPartitionReassignment::cancel`] entry cancels an ongoing move.
    ///
    /// # Errors
    /// Returns the top-level error, the first per-partition error, or a routing
    /// error.
    pub async fn alter_partition_reassignments(
        &self,
        reassignments: Vec<NewPartitionReassignment>,
    ) -> Result<()> {
        let request = AlterPartitionReassignmentsRequestData {
            timeout_ms: self.request_timeout_ms,
            allow_replication_factor_change: true,
            topics: reassignable_topics(reassignments),
            _unknown_tagged_fields: Vec::new(),
        };
        let response: AlterPartitionReassignmentsResponseData = self
            .route_to_controller(
                ApiKey::AlterPartitionReassignments,
                &request,
                |response: &AlterPartitionReassignmentsResponseData| {
                    is_not_controller(response.error_code)
                },
            )
            .await?;
        check_code("", response.error_code, response.error_message.as_ref())?;
        for topic in &response.responses {
            for partition in &topic.partitions {
                check_code(
                    topic.name.as_str(),
                    partition.error_code,
                    partition.error_message.as_ref(),
                )?;
            }
        }
        Ok(())
    }

    /// List ongoing partition reassignments, optionally restricted to
    /// `partitions` (empty = all). Controller-routed.
    ///
    /// # Errors
    /// Returns the top-level error or a routing error.
    pub async fn list_partition_reassignments(
        &self,
        partitions: Vec<TopicPartition>,
    ) -> Result<Vec<PartitionReassignment>> {
        let request = ListPartitionReassignmentsRequestData {
            timeout_ms: self.request_timeout_ms,
            topics: list_reassignments_topics(&partitions),
            _unknown_tagged_fields: Vec::new(),
        };
        let response: ListPartitionReassignmentsResponseData = self
            .route_to_controller(
                ApiKey::ListPartitionReassignments,
                &request,
                |response: &ListPartitionReassignmentsResponseData| {
                    is_not_controller(response.error_code)
                },
            )
            .await?;
        check_code("", response.error_code, response.error_message.as_ref())?;
        let mut all = Vec::new();
        for topic in response.topics {
            let name = topic.name.as_str().to_owned();
            for partition in topic.partitions {
                all.push(PartitionReassignment {
                    topic_partition: TopicPartition::new(name.clone(), partition.partition_index),
                    replicas: partition.replicas,
                    adding_replicas: partition.adding_replicas,
                    removing_replicas: partition.removing_replicas,
                });
            }
        }
        Ok(all)
    }

    /// Finalize feature version-level changes (controller-routed). Set
    /// `validate_only` to check without applying.
    ///
    /// # Errors
    /// Returns the top-level error, the first per-feature error, or a routing
    /// error.
    pub async fn update_features(
        &self,
        updates: Vec<FeatureUpdate>,
        validate_only: bool,
    ) -> Result<()> {
        let request = UpdateFeaturesRequestData {
            timeout_ms: self.request_timeout_ms,
            feature_updates: updates
                .into_iter()
                .map(|update| FeatureUpdateKey {
                    feature: update.feature.into(),
                    max_version_level: update.max_version_level,
                    allow_downgrade: update.upgrade_type.allows_downgrade(),
                    upgrade_type: update.upgrade_type.to_wire(),
                    _unknown_tagged_fields: Vec::new(),
                })
                .collect(),
            validate_only,
            _unknown_tagged_fields: Vec::new(),
        };
        let response: UpdateFeaturesResponseData = self
            .route_to_controller(
                ApiKey::UpdateFeatures,
                &request,
                |response: &UpdateFeaturesResponseData| is_not_controller(response.error_code),
            )
            .await?;
        check_code("", response.error_code, response.error_message.as_ref())?;
        for result in &response.results {
            check_code(
                result.feature.as_str(),
                result.error_code,
                result.error_message.as_ref(),
            )?;
        }
        Ok(())
    }

    /// Unregister a broker from the (`KRaft`) cluster, controller-routed.
    ///
    /// # Errors
    /// Returns the broker error code or a routing error.
    pub async fn unregister_broker(&self, broker_id: i32) -> Result<()> {
        let request = UnregisterBrokerRequestData {
            broker_id,
            _unknown_tagged_fields: Vec::new(),
        };
        let response: UnregisterBrokerResponseData = self
            .route_to_controller(
                ApiKey::UnregisterBroker,
                &request,
                |response: &UnregisterBrokerResponseData| is_not_controller(response.error_code),
            )
            .await?;
        check_code("", response.error_code, response.error_message.as_ref())?;
        Ok(())
    }

    /// Describe the active producers writing to the given partitions, routing
    /// each partition's request to its current leader.
    ///
    /// # Errors
    /// Returns a metadata/wire error, `LEADER_NOT_AVAILABLE` when a partition has
    /// no known leader, or the first per-partition broker error code.
    pub async fn describe_producers(
        &self,
        partitions: Vec<TopicPartition>,
    ) -> Result<Vec<PartitionProducerState>> {
        if partitions.is_empty() {
            return Ok(Vec::new());
        }
        let topic_names = unique_topics(
            &partitions
                .iter()
                .map(|tp| tp.topic.as_str())
                .collect::<Vec<_>>(),
        );
        let metadata = self.wire.admin_metadata(Some(&topic_names)).await?;
        let mut by_leader: HashMap<i32, Vec<(String, i32)>> = HashMap::new();
        for topic_partition in &partitions {
            let leader =
                partition_leader(&metadata, &topic_partition.topic, topic_partition.partition)
                    .ok_or_else(|| leader_unavailable(topic_partition))?;
            by_leader
                .entry(leader)
                .or_default()
                .push((topic_partition.topic.clone(), topic_partition.partition));
        }
        let version = client_api_info(ApiKey::DescribeProducers).max_version;
        let mut results = Vec::with_capacity(partitions.len());
        for (leader, entries) in by_leader {
            let request = DescribeProducersRequestData {
                topics: describe_producers_topics(entries),
                _unknown_tagged_fields: Vec::new(),
            };
            let response: DescribeProducersResponseData = self
                .send_metered(leader, ApiKey::DescribeProducers, version, &request)
                .await?;
            for topic in response.topics {
                for partition in topic.partitions {
                    check_code(
                        topic.name.as_str(),
                        partition.error_code,
                        partition.error_message.as_ref(),
                    )?;
                    let active_producers = partition
                        .active_producers
                        .into_iter()
                        .map(|producer| ProducerState {
                            producer_id: producer.producer_id,
                            producer_epoch: producer.producer_epoch,
                            last_sequence: producer.last_sequence,
                            last_timestamp: producer.last_timestamp,
                            coordinator_epoch: producer.coordinator_epoch,
                            current_transaction_start_offset: producer.current_txn_start_offset,
                        })
                        .collect();
                    results.push(PartitionProducerState {
                        partition: TopicPartition::new(
                            topic.name.as_str().to_owned(),
                            partition.partition_index,
                        ),
                        active_producers,
                    });
                }
            }
        }
        Ok(results)
    }

    /// Describe the given transactions, routing each to its transaction
    /// coordinator.
    ///
    /// # Errors
    /// Returns the coordinator-lookup error, the per-transaction broker error
    /// code, or [`AdminError::MissingResult`] when the broker omits one.
    pub async fn describe_transactions(
        &self,
        transactional_ids: Vec<String>,
    ) -> Result<Vec<TransactionDescription>> {
        let mut described = Vec::with_capacity(transactional_ids.len());
        for transactional_id in &transactional_ids {
            let request = DescribeTransactionsRequestData {
                transactional_ids: vec![transactional_id.clone().into()],
                _unknown_tagged_fields: Vec::new(),
            };
            let (response, coordinator) = self
                .route_to_coordinator(
                    transactional_id,
                    1,
                    ApiKey::DescribeTransactions,
                    &request,
                    |response: &DescribeTransactionsResponseData| {
                        response
                            .transaction_states
                            .iter()
                            .any(|state| is_coordinator_error(state.error_code))
                    },
                )
                .await?;
            let state = response
                .transaction_states
                .into_iter()
                .find(|state| state.transactional_id.as_str() == transactional_id)
                .ok_or_else(|| AdminError::MissingResult {
                    target: transactional_id.clone(),
                })?;
            check_code(transactional_id, state.error_code, None)?;
            let topic_partitions = state
                .topics
                .into_iter()
                .flat_map(|topic| {
                    let name = topic.topic.as_str().to_owned();
                    topic
                        .partitions
                        .into_iter()
                        .map(move |partition| TopicPartition::new(name.clone(), partition))
                })
                .collect();
            described.push(TransactionDescription {
                transactional_id: state.transactional_id.as_str().to_owned(),
                state: state.transaction_state.as_str().to_owned(),
                producer_id: state.producer_id,
                producer_epoch: state.producer_epoch,
                transaction_timeout_ms: state.transaction_timeout_ms,
                transaction_start_time_ms: state.transaction_start_time_ms,
                topic_partitions,
                coordinator,
            });
        }
        Ok(described)
    }

    /// List the transactions known to the cluster, aggregated across all brokers.
    ///
    /// # Errors
    /// Returns a wire error or the first broker top-level error code.
    pub async fn list_transactions(
        &self,
        options: ListTransactionsOptions,
    ) -> Result<Vec<TransactionListing>> {
        let request = ListTransactionsRequestData {
            state_filters: options.state_filters.into_iter().map(Into::into).collect(),
            producer_id_filters: options.producer_id_filters,
            duration_filter: -1,
            transactional_id_pattern: None,
            _unknown_tagged_fields: Vec::new(),
        };
        let metadata = self.wire.admin_metadata(None).await?;
        let broker_ids: Vec<i32> = metadata
            .brokers
            .iter()
            .map(|broker| broker.node_id)
            .collect();
        let version = client_api_info(ApiKey::ListTransactions).max_version;
        let mut listings = Vec::new();
        for broker_id in broker_ids {
            let response: ListTransactionsResponseData = self
                .send_metered(broker_id, ApiKey::ListTransactions, version, &request)
                .await?;
            check_code("", response.error_code, None)?;
            for state in response.transaction_states {
                listings.push(TransactionListing {
                    transactional_id: state.transactional_id.as_str().to_owned(),
                    producer_id: state.producer_id,
                    state: state.transaction_state.as_str().to_owned(),
                });
            }
        }
        Ok(listings)
    }

    /// Resolve topic ids from metadata for the given topic names (newer
    /// offset-commit/fetch versions key topics by id rather than name).
    async fn resolve_topic_ids(
        &self,
        topic_names: &[String],
    ) -> Result<HashMap<String, KafkaUuid>> {
        if topic_names.is_empty() {
            return Ok(HashMap::new());
        }
        let metadata = self.wire.admin_metadata(Some(topic_names)).await?;
        Ok(metadata
            .topics
            .iter()
            .map(|topic| (topic.name.clone(), topic.topic_id))
            .collect())
    }

    /// Resolve a coordinator broker for `key` of the given `FindCoordinator`
    /// `key_type`, registering its endpoint. Mirrors the producer's
    /// `FindCoordinator` flow (resolve advertised host:port, then upsert).
    async fn coordinator_node(&self, key: &str, key_type: i8) -> Result<Node> {
        let request = FindCoordinatorRequestData {
            key_type,
            coordinator_keys: vec![key.to_owned().into()],
            ..FindCoordinatorRequestData::default()
        };
        let broker_id = self.wire.admin_any_broker_id()?;
        let version = client_api_info(ApiKey::FindCoordinator).max_version;
        let response: FindCoordinatorResponseData = self
            .send_metered(broker_id, ApiKey::FindCoordinator, version, &request)
            .await?;
        let coordinator = response
            .coordinators
            .into_iter()
            .find(|coordinator| coordinator.key.as_str() == key)
            .ok_or_else(|| AdminError::CoordinatorUnavailable {
                key: key.to_owned(),
            })?;
        check_code(
            key,
            coordinator.error_code,
            coordinator.error_message.as_ref(),
        )?;
        let port = u16::try_from(coordinator.port).map_err(|_error| {
            AdminError::CoordinatorUnavailable {
                key: key.to_owned(),
            }
        })?;
        let host = coordinator.host.as_str().to_owned();
        let addr = tokio::net::lookup_host((host.as_str(), port))
            .await
            .map_err(WireError::from)?
            .next()
            .ok_or_else(|| AdminError::CoordinatorUnavailable {
                key: key.to_owned(),
            })?;
        self.wire.upsert_broker(BrokerEndpoint::from_resolved(
            coordinator.node_id,
            host.clone(),
            port,
            addr,
        ));
        Ok(Node {
            id: coordinator.node_id,
            host,
            port: coordinator.port,
            rack: None,
        })
    }
}

/// Return `Some(owned)` when the string is non-empty, else `None`.
fn non_empty(value: &str) -> Option<String> {
    (!value.is_empty()).then(|| value.to_owned())
}

/// Build [`OffsetAndMetadata`] from an `OffsetFetch` partition result, dropping
/// the sentinel `-1` leader epoch and empty metadata.
fn build_offset(
    committed_offset: i64,
    leader_epoch: i32,
    metadata: Option<KafkaString>,
) -> OffsetAndMetadata {
    let mut offset = OffsetAndMetadata::new(committed_offset);
    if leader_epoch >= 0 {
        offset = offset.leader_epoch(leader_epoch);
    }
    if let Some(metadata) = metadata {
        let metadata = metadata.as_str();
        if !metadata.is_empty() {
            offset = offset.metadata(metadata.to_owned());
        }
    }
    offset
}

/// Group partitions by topic into `OffsetFetch` topic entries, or `None` (fetch
/// all committed partitions) when the filter is empty.
fn group_offset_fetch_topics(
    partitions: &[TopicPartition],
    topic_ids: &HashMap<String, KafkaUuid>,
) -> Option<Vec<OffsetFetchRequestTopics>> {
    if partitions.is_empty() {
        return None;
    }
    let mut topics: Vec<OffsetFetchRequestTopics> = Vec::new();
    for partition in partitions {
        if let Some(topic) = topics
            .iter_mut()
            .find(|topic| topic.name.as_str() == partition.topic)
        {
            topic.partition_indexes.push(partition.partition);
        } else {
            // As with OffsetCommit, newer OffsetFetch versions key by topic id
            // and reject a stale name; only send the name when the id is unknown.
            let topic_id = topic_ids
                .get(&partition.topic)
                .copied()
                .unwrap_or(KafkaUuid::ZERO);
            let name = if topic_id == KafkaUuid::ZERO {
                partition.topic.clone().into()
            } else {
                KafkaString::default()
            };
            topics.push(OffsetFetchRequestTopics {
                name,
                topic_id,
                partition_indexes: vec![partition.partition],
                _unknown_tagged_fields: Vec::new(),
            });
        }
    }
    Some(topics)
}

/// Group offset commits by topic into `OffsetCommit` topic entries.
fn offset_commit_topics(
    offsets: Vec<(TopicPartition, OffsetAndMetadata)>,
    topic_ids: &HashMap<String, KafkaUuid>,
) -> Vec<OffsetCommitRequestTopic> {
    let mut topics: Vec<OffsetCommitRequestTopic> = Vec::new();
    for (topic_partition, offset) in offsets {
        let partition = OffsetCommitRequestPartition {
            partition_index: topic_partition.partition,
            committed_offset: offset.offset,
            committed_leader_epoch: offset.leader_epoch.unwrap_or(-1),
            committed_metadata: offset.metadata.map(Into::into),
            _unknown_tagged_fields: Vec::new(),
        };
        if let Some(topic) = topics
            .iter_mut()
            .find(|topic| topic.name.as_str() == topic_partition.topic)
        {
            topic.partitions.push(partition);
        } else {
            // OffsetCommit v10 removed the topic `name` (keyed by id) and the
            // strict codec rejects a non-default name at v10; send the name only
            // when the id is unknown (older brokers / unresolved topic).
            let topic_id = topic_ids
                .get(&topic_partition.topic)
                .copied()
                .unwrap_or(KafkaUuid::ZERO);
            let name = if topic_id == KafkaUuid::ZERO {
                topic_partition.topic.clone().into()
            } else {
                KafkaString::default()
            };
            topics.push(OffsetCommitRequestTopic {
                name,
                topic_id,
                partitions: vec![partition],
                _unknown_tagged_fields: Vec::new(),
            });
        }
    }
    topics
}

/// Group partitions by topic into `OffsetDelete` topic entries.
fn offset_delete_topics(partitions: Vec<TopicPartition>) -> Vec<OffsetDeleteRequestTopic> {
    let mut topics: Vec<OffsetDeleteRequestTopic> = Vec::new();
    for topic_partition in partitions {
        let partition = OffsetDeleteRequestPartition {
            partition_index: topic_partition.partition,
            _unknown_tagged_fields: Vec::new(),
        };
        if let Some(topic) = topics
            .iter_mut()
            .find(|topic| topic.name.as_str() == topic_partition.topic)
        {
            topic.partitions.push(partition);
        } else {
            topics.push(OffsetDeleteRequestTopic {
                name: topic_partition.topic.clone().into(),
                partitions: vec![partition],
                _unknown_tagged_fields: Vec::new(),
            });
        }
    }
    topics
}

/// Collect distinct topic names, preserving first-seen order.
fn unique_topics(topics: &[&str]) -> Vec<String> {
    let mut names: Vec<String> = Vec::new();
    for topic in topics {
        if !names.iter().any(|name| name == topic) {
            names.push((*topic).to_owned());
        }
    }
    names
}

/// Resolve the current leader broker id of a partition from fetched metadata, or
/// `None` when the topic/partition is unknown or has no leader (`-1`).
fn partition_leader(metadata: &ClusterMetadata, topic: &str, partition: i32) -> Option<i32> {
    metadata
        .topics
        .iter()
        .find(|candidate| candidate.name == topic)
        .and_then(|candidate| {
            candidate
                .partitions
                .iter()
                .find(|entry| entry.partition_index == partition)
        })
        .map(|entry| entry.leader_id)
        .filter(|leader| *leader >= 0)
}

/// Build the `LEADER_NOT_AVAILABLE` error for a partition with no known leader.
fn leader_unavailable(topic_partition: &TopicPartition) -> AdminError {
    AdminError::Broker {
        target: format!("{}-{}", topic_partition.topic, topic_partition.partition),
        error: ErrorCode::LeaderNotAvailable,
        message: None,
    }
}

/// Group partitions by topic into `ElectLeaders` topic entries, or `None` (elect
/// for every partition) when the list is empty.
fn elect_leaders_partitions(
    partitions: Vec<TopicPartition>,
) -> Option<Vec<ElectLeadersTopicPartitions>> {
    if partitions.is_empty() {
        return None;
    }
    let mut topics: Vec<ElectLeadersTopicPartitions> = Vec::new();
    for topic_partition in partitions {
        if let Some(topic) = topics
            .iter_mut()
            .find(|topic| topic.topic.as_str() == topic_partition.topic)
        {
            topic.partitions.push(topic_partition.partition);
        } else {
            topics.push(ElectLeadersTopicPartitions {
                topic: topic_partition.topic.clone().into(),
                partitions: vec![topic_partition.partition],
                _unknown_tagged_fields: Vec::new(),
            });
        }
    }
    Some(topics)
}

/// Group `(topic, partition, timestamp)` leader-batched entries into
/// `ListOffsets` topic entries.
fn list_offsets_topics(entries: Vec<(String, i32, i64)>) -> Vec<ListOffsetsTopic> {
    let mut topics: Vec<ListOffsetsTopic> = Vec::new();
    for (name, partition_index, timestamp) in entries {
        let partition = ListOffsetsPartition {
            partition_index,
            current_leader_epoch: -1,
            timestamp,
            _unknown_tagged_fields: Vec::new(),
        };
        if let Some(topic) = topics.iter_mut().find(|topic| topic.name.as_str() == name) {
            topic.partitions.push(partition);
        } else {
            topics.push(ListOffsetsTopic {
                name: name.into(),
                partitions: vec![partition],
                _unknown_tagged_fields: Vec::new(),
            });
        }
    }
    topics
}

/// Group partitions by topic into `DescribeLogDirs` topic entries, or `None`
/// (all partitions) when the filter is empty.
fn describe_log_dirs_topics(partitions: &[TopicPartition]) -> Option<Vec<DescribableLogDirTopic>> {
    if partitions.is_empty() {
        return None;
    }
    let mut topics: Vec<DescribableLogDirTopic> = Vec::new();
    for partition in partitions {
        if let Some(topic) = topics
            .iter_mut()
            .find(|topic| topic.topic.as_str() == partition.topic)
        {
            topic.partitions.push(partition.partition);
        } else {
            topics.push(DescribableLogDirTopic {
                topic: partition.topic.clone().into(),
                partitions: vec![partition.partition],
                _unknown_tagged_fields: Vec::new(),
            });
        }
    }
    Some(topics)
}

/// Group reassignments by topic into `AlterPartitionReassignments` topic
/// entries (a `None` replica list cancels an ongoing reassignment).
fn reassignable_topics(reassignments: Vec<NewPartitionReassignment>) -> Vec<ReassignableTopic> {
    let mut topics: Vec<ReassignableTopic> = Vec::new();
    for reassignment in reassignments {
        let partition = ReassignablePartition {
            partition_index: reassignment.topic_partition.partition,
            replicas: reassignment.replicas,
            _unknown_tagged_fields: Vec::new(),
        };
        if let Some(topic) = topics
            .iter_mut()
            .find(|topic| topic.name.as_str() == reassignment.topic_partition.topic)
        {
            topic.partitions.push(partition);
        } else {
            topics.push(ReassignableTopic {
                name: reassignment.topic_partition.topic.clone().into(),
                partitions: vec![partition],
                _unknown_tagged_fields: Vec::new(),
            });
        }
    }
    topics
}

/// Group partitions by topic into `ListPartitionReassignments` topic entries,
/// or `None` (all partitions) when the filter is empty.
fn list_reassignments_topics(
    partitions: &[TopicPartition],
) -> Option<Vec<ListPartitionReassignmentsTopics>> {
    if partitions.is_empty() {
        return None;
    }
    let mut topics: Vec<ListPartitionReassignmentsTopics> = Vec::new();
    for partition in partitions {
        if let Some(topic) = topics
            .iter_mut()
            .find(|topic| topic.name.as_str() == partition.topic)
        {
            topic.partition_indexes.push(partition.partition);
        } else {
            topics.push(ListPartitionReassignmentsTopics {
                name: partition.topic.clone().into(),
                partition_indexes: vec![partition.partition],
                _unknown_tagged_fields: Vec::new(),
            });
        }
    }
    Some(topics)
}

/// Group `(log_dir, topic, partition)` broker-batched entries into
/// `AlterReplicaLogDirs` dir entries (by log dir, then topic).
fn build_alter_replica_log_dirs(entries: Vec<(String, String, i32)>) -> Vec<AlterReplicaLogDir> {
    let mut dirs: Vec<AlterReplicaLogDir> = Vec::new();
    let mut dir_index: HashMap<String, usize> = HashMap::new();
    for (log_dir, topic_name, partition) in entries {
        let next_index = dirs.len();
        let index = *dir_index.entry(log_dir.clone()).or_insert(next_index);
        if index == next_index {
            dirs.push(AlterReplicaLogDir {
                path: log_dir.into(),
                topics: Vec::new(),
                _unknown_tagged_fields: Vec::new(),
            });
        }
        let Some(dir) = dirs.get_mut(index) else {
            continue;
        };
        if let Some(topic) = dir
            .topics
            .iter_mut()
            .find(|topic| topic.name.as_str() == topic_name)
        {
            topic.partitions.push(partition);
        } else {
            dir.topics.push(AlterReplicaLogDirTopic {
                name: topic_name.into(),
                partitions: vec![partition],
                _unknown_tagged_fields: Vec::new(),
            });
        }
    }
    dirs
}

/// Group `(topic, partition)` leader-batched entries into `DescribeProducers`
/// topic entries.
fn describe_producers_topics(entries: Vec<(String, i32)>) -> Vec<DescribeProducersTopicRequest> {
    let mut topics: Vec<DescribeProducersTopicRequest> = Vec::new();
    for (name, partition_index) in entries {
        if let Some(topic) = topics.iter_mut().find(|topic| topic.name.as_str() == name) {
            topic.partition_indexes.push(partition_index);
        } else {
            topics.push(DescribeProducersTopicRequest {
                name: name.into(),
                partition_indexes: vec![partition_index],
                _unknown_tagged_fields: Vec::new(),
            });
        }
    }
    topics
}

/// Group `(topic, partition, offset)` leader-batched entries into
/// `DeleteRecords` topic entries.
fn delete_records_topics(entries: Vec<(String, i32, i64)>) -> Vec<DeleteRecordsTopic> {
    let mut topics: Vec<DeleteRecordsTopic> = Vec::new();
    for (name, partition_index, offset) in entries {
        let partition = DeleteRecordsPartition {
            partition_index,
            offset,
            _unknown_tagged_fields: Vec::new(),
        };
        if let Some(topic) = topics.iter_mut().find(|topic| topic.name.as_str() == name) {
            topic.partitions.push(partition);
        } else {
            topics.push(DeleteRecordsTopic {
                name: name.into(),
                partitions: vec![partition],
                _unknown_tagged_fields: Vec::new(),
            });
        }
    }
    topics
}

/// Decode a classic consumer-group member assignment blob (a version-prefixed
/// `ConsumerProtocolAssignment`) into the partitions the member owns. Returns an
/// empty list for an absent or non-consumer-protocol assignment.
fn decode_member_assignment(assignment: &bytes::Bytes) -> Vec<TopicPartition> {
    use bytes::Buf as _;

    if assignment.len() < 2 {
        return Vec::new();
    }
    let mut buf = assignment.clone();
    let version = buf.get_i16();
    let Ok(decoded) = ConsumerProtocolAssignmentData::read(&mut buf, version) else {
        return Vec::new();
    };
    decoded
        .assigned_partitions
        .into_iter()
        .flat_map(|topic| {
            let name = topic.topic.as_str().to_owned();
            topic
                .partitions
                .into_iter()
                .map(move |partition| TopicPartition::new(name.clone(), partition))
        })
        .collect()
}

/// Flatten a `ConsumerGroupDescribe` structured assignment into the partitions
/// it covers.
fn consumer_assignment_partitions(
    assignment: ConsumerGroupDescribeAssignment,
) -> Vec<TopicPartition> {
    assignment
        .topic_partitions
        .into_iter()
        .flat_map(|topic| {
            let name = topic.topic_name.as_str().to_owned();
            topic
                .partitions
                .into_iter()
                .map(move |partition| TopicPartition::new(name.clone(), partition))
        })
        .collect()
}

/// Decode a Kafka `authorized_operations` bit field into the authorized ACL
/// operations. `Integer.MIN_VALUE` marks "not requested" and yields an empty
/// list.
fn decode_authorized_operations(bits: i32) -> Vec<AclOperation> {
    if bits == i32::MIN {
        return Vec::new();
    }
    let bits = bits.cast_unsigned();
    (2_i8..15)
        .filter(|code| bits & (1_u32 << u32::from(code.unsigned_abs())) != 0)
        .map(AclOperation::from_wire)
        .filter(|operation| !matches!(operation, AclOperation::Unknown))
        .collect()
}

/// If every resource targets the same specific broker id, return it; otherwise
/// `None` (mixed/non-broker resources, or the cluster-wide broker default whose
/// empty name does not parse).
fn broker_target(resources: &[ConfigResource]) -> Option<i32> {
    let mut target = None;
    for resource in resources {
        if resource.resource_type != ResourceType::Broker {
            return None;
        }
        let broker_id = resource.name.parse::<i32>().ok()?;
        match target {
            None => target = Some(broker_id),
            Some(existing) if existing != broker_id => return None,
            Some(_) => {},
        }
    }
    target
}

/// Resolve a broker id to a [`Node`] using fetched metadata, or `None` if the
/// id is not a known broker (e.g. a leaderless partition's `-1`).
fn node_for(metadata: &ClusterMetadata, broker_id: i32) -> Option<Node> {
    metadata
        .brokers
        .iter()
        .find(|broker| broker.node_id == broker_id)
        .map(|broker| Node {
            id: broker.node_id,
            host: broker.host.clone(),
            port: broker.port,
            rack: broker.rack.clone(),
        })
}

/// Resolve a broker id to a [`Node`], falling back to a placeholder that
/// preserves the id when the broker is absent from metadata (e.g. an offline
/// replica), matching Java's handling of unknown replica nodes.
fn node_or_placeholder(metadata: &ClusterMetadata, broker_id: i32) -> Node {
    node_for(metadata, broker_id).unwrap_or(Node {
        id: broker_id,
        host: String::new(),
        port: -1,
        rack: None,
    })
}

fn is_not_controller(error_code: i16) -> bool {
    error_code == i16::from(ErrorCode::NotController)
}

/// Whether `error_code` is a transient coordinator error worth retrying against a
/// freshly resolved coordinator.
fn is_coordinator_error(error_code: i16) -> bool {
    matches!(
        ErrorCode::from(error_code),
        ErrorCode::NotCoordinator
            | ErrorCode::CoordinatorNotAvailable
            | ErrorCode::CoordinatorLoadInProgress
    )
}

/// Map a broker error code on a single result to an [`AdminError::Broker`],
/// treating `NONE` as success.
fn check_code(target: &str, error_code: i16, message: Option<&KafkaString>) -> Result<()> {
    let error = ErrorCode::from(error_code);
    if error.is_error() {
        return Err(AdminError::Broker {
            target: target.to_owned(),
            error,
            message: message.map(|message| message.as_str().to_owned()),
        });
    }
    Ok(())
}

/// Clamp configured `retries` into a controller-routing attempt count.
fn controller_attempts_from_retries(retries: i32) -> u32 {
    if retries <= 0 {
        return 1;
    }
    u32::try_from(retries)
        .unwrap_or(MAX_CONTROLLER_ATTEMPTS)
        .clamp(1, MAX_CONTROLLER_ATTEMPTS)
}

/// Resolve the bootstrap server list into connectable broker endpoints.
async fn resolve_bootstrap_brokers(config: &AdminConfig) -> Result<Vec<BrokerEndpoint>> {
    let mut endpoints = Vec::new();
    for (index, server) in config.bootstrap_servers.as_slice().iter().enumerate() {
        let node_id = i32::try_from(index).map_err(|_error| AdminError::InvalidArgument {
            field: "bootstrap.servers",
            message: format!("too many bootstrap servers (entry {index})"),
        })?;
        let (host, port) = parse_bootstrap_server(server)?;
        let mut addresses = tokio::net::lookup_host((host.as_str(), port))
            .await
            .map_err(WireError::from)?;
        let addr = addresses.next();
        drop(addresses);
        if let Some(addr) = addr {
            endpoints.push(BrokerEndpoint::from_resolved(node_id, host, port, addr));
        }
    }
    if endpoints.is_empty() {
        return Err(AdminError::InvalidArgument {
            field: "bootstrap.servers",
            message: "no bootstrap server resolved to a socket address".to_owned(),
        });
    }
    Ok(endpoints)
}

fn parse_bootstrap_server(server: &str) -> Result<(String, u16)> {
    let (host, port) = server
        .rsplit_once(':')
        .ok_or_else(|| AdminError::InvalidArgument {
            field: "bootstrap.servers",
            message: format!("missing port in bootstrap server {server:?}"),
        })?;
    let port = port
        .parse::<u16>()
        .map_err(|_error| AdminError::InvalidArgument {
            field: "bootstrap.servers",
            message: format!("invalid port in bootstrap server {server:?}"),
        })?;
    let host = host
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(host);
    if host.is_empty() {
        return Err(AdminError::InvalidArgument {
            field: "bootstrap.servers",
            message: format!("missing host in bootstrap server {server:?}"),
        });
    }
    Ok((host.to_owned(), port))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn broker_target_returns_id_when_all_same_broker() {
        let resources = vec![ConfigResource::broker(1), ConfigResource::broker(1)];
        assert_eq!(broker_target(&resources), Some(1));
    }

    #[test]
    fn broker_target_is_none_for_mixed_brokers() {
        let resources = vec![ConfigResource::broker(1), ConfigResource::broker(2)];
        assert_eq!(broker_target(&resources), None);
    }

    #[test]
    fn broker_target_is_none_for_topic_or_cluster_default() {
        assert_eq!(broker_target(&[ConfigResource::topic("orders")]), None);
        // Cluster-wide broker default uses an empty name, which does not parse.
        let cluster_default = ConfigResource {
            resource_type: ResourceType::Broker,
            name: String::new(),
        };
        assert_eq!(broker_target(&[cluster_default]), None);
    }

    #[test]
    fn controller_attempts_clamp_to_bounds() {
        assert_eq!(controller_attempts_from_retries(0), 1);
        assert_eq!(controller_attempts_from_retries(-5), 1);
        assert_eq!(controller_attempts_from_retries(3), 3);
        assert_eq!(
            controller_attempts_from_retries(i32::MAX),
            MAX_CONTROLLER_ATTEMPTS
        );
    }

    #[test]
    fn check_code_maps_error_with_target_and_message() {
        let message = KafkaString::from_static("topic exists");
        let err = check_code(
            "orders",
            i16::from(ErrorCode::TopicAlreadyExists),
            Some(&message),
        )
        .expect_err("error code should map");
        match err {
            AdminError::Broker {
                target,
                error,
                message,
            } => {
                assert_eq!(target, "orders");
                assert_eq!(error, ErrorCode::TopicAlreadyExists);
                assert_eq!(message.as_deref(), Some("topic exists"));
            },
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn check_code_treats_none_as_success() {
        assert!(check_code("orders", i16::from(ErrorCode::None), None).is_ok());
    }

    #[test]
    fn is_not_controller_matches_only_not_controller() {
        assert!(is_not_controller(i16::from(ErrorCode::NotController)));
        assert!(!is_not_controller(i16::from(ErrorCode::None)));
        assert!(!is_not_controller(i16::from(ErrorCode::LeaderNotAvailable)));
    }

    #[test]
    fn parse_bootstrap_server_handles_host_port_and_ipv6() {
        assert_eq!(
            parse_bootstrap_server("broker:9092").expect("parse"),
            ("broker".to_owned(), 9092)
        );
        assert_eq!(
            parse_bootstrap_server("[::1]:9092").expect("parse ipv6"),
            ("::1".to_owned(), 9092)
        );
    }

    fn metadata_with_brokers() -> ClusterMetadata {
        ClusterMetadata {
            cluster_id: Some("cluster".to_owned()),
            controller_id: 1,
            brokers: vec![
                crate::wire::BrokerMetadata {
                    node_id: 1,
                    host: "broker-1".to_owned(),
                    port: 9092,
                    rack: Some("rack-a".to_owned()),
                },
                crate::wire::BrokerMetadata {
                    node_id: 2,
                    host: "broker-2".to_owned(),
                    port: 9093,
                    rack: None,
                },
            ],
            topics: Vec::new(),
        }
    }

    #[test]
    fn node_for_resolves_known_broker_and_misses_unknown() {
        let metadata = metadata_with_brokers();
        let node = node_for(&metadata, 1).expect("known broker");
        assert_eq!(node.id, 1);
        assert_eq!(node.host, "broker-1");
        assert_eq!(node.port, 9092);
        assert_eq!(node.rack.as_deref(), Some("rack-a"));
        assert!(node_for(&metadata, -1).is_none());
        assert!(node_for(&metadata, 99).is_none());
    }

    #[test]
    fn node_or_placeholder_preserves_unknown_id() {
        let metadata = metadata_with_brokers();
        let known = node_or_placeholder(&metadata, 2);
        assert_eq!(known.host, "broker-2");
        let placeholder = node_or_placeholder(&metadata, 7);
        assert_eq!(placeholder.id, 7);
        assert_eq!(placeholder.host, "");
        assert_eq!(placeholder.port, -1);
        assert_eq!(placeholder.rack, None);
    }

    #[test]
    fn parse_bootstrap_server_rejects_malformed_input() {
        assert!(parse_bootstrap_server("broker").is_err());
        assert!(parse_bootstrap_server("broker:notaport").is_err());
        assert!(parse_bootstrap_server(":9092").is_err());
    }

    #[test]
    fn non_empty_filters_blank_strings() {
        assert_eq!(non_empty("Stable"), Some("Stable".to_owned()));
        assert_eq!(non_empty(""), None);
    }

    #[test]
    fn build_offset_drops_sentinel_epoch_and_empty_metadata() {
        let bare = build_offset(42, -1, None);
        assert_eq!(bare.offset, 42);
        assert_eq!(bare.leader_epoch, None);
        assert_eq!(bare.metadata, None);

        let empty_meta = build_offset(42, -1, Some(KafkaString::from_static("")));
        assert_eq!(empty_meta.metadata, None);

        let full = build_offset(7, 3, Some(KafkaString::from_static("note")));
        assert_eq!(full.leader_epoch, Some(3));
        assert_eq!(full.metadata.as_deref(), Some("note"));
    }

    #[test]
    fn group_offset_fetch_topics_is_none_when_empty_else_groups_by_topic() {
        let topic_ids = HashMap::new();
        assert!(group_offset_fetch_topics(&[], &topic_ids).is_none());

        let topics = group_offset_fetch_topics(
            &[
                TopicPartition::new("orders", 0),
                TopicPartition::new("orders", 1),
                TopicPartition::new("payments", 0),
            ],
            &topic_ids,
        )
        .expect("partitions present");
        assert_eq!(topics.len(), 2);
        let orders = topics
            .iter()
            .find(|topic| topic.name.as_str() == "orders")
            .expect("orders topic");
        assert_eq!(orders.partition_indexes, vec![0, 1]);
    }

    #[test]
    fn offset_commit_topics_groups_and_maps_offset_fields() {
        let topics = offset_commit_topics(
            vec![
                (
                    TopicPartition::new("orders", 0),
                    OffsetAndMetadata::new(10).leader_epoch(2).metadata("m"),
                ),
                (TopicPartition::new("orders", 1), OffsetAndMetadata::new(20)),
            ],
            &HashMap::new(),
        );
        assert_eq!(topics.len(), 1);
        let orders = &topics[0];
        assert_eq!(orders.partitions.len(), 2);
        assert_eq!(orders.partitions[0].committed_offset, 10);
        assert_eq!(orders.partitions[0].committed_leader_epoch, 2);
        assert_eq!(
            orders.partitions[0]
                .committed_metadata
                .as_ref()
                .map(KafkaString::as_str),
            Some("m")
        );
        // Absent leader epoch maps to the -1 sentinel.
        assert_eq!(orders.partitions[1].committed_leader_epoch, -1);
        assert_eq!(orders.partitions[1].committed_metadata, None);
    }

    #[test]
    fn offset_delete_topics_groups_by_topic() {
        let topics = offset_delete_topics(vec![
            TopicPartition::new("orders", 0),
            TopicPartition::new("orders", 2),
            TopicPartition::new("payments", 1),
        ]);
        assert_eq!(topics.len(), 2);
        let orders = topics
            .iter()
            .find(|topic| topic.name.as_str() == "orders")
            .expect("orders topic");
        let indexes: Vec<i32> = orders
            .partitions
            .iter()
            .map(|partition| partition.partition_index)
            .collect();
        assert_eq!(indexes, vec![0, 2]);
    }

    fn metadata_with_topic() -> ClusterMetadata {
        ClusterMetadata {
            cluster_id: Some("cluster".to_owned()),
            controller_id: 1,
            brokers: Vec::new(),
            topics: vec![crate::wire::TopicMetadata {
                name: "orders".to_owned(),
                topic_id: KafkaUuid::ZERO,
                is_internal: false,
                partitions: vec![
                    crate::wire::PartitionMetadata {
                        partition_index: 0,
                        leader_id: 3,
                        leader_epoch: 5,
                        replica_nodes: vec![3, 4],
                        isr_nodes: vec![3, 4],
                        offline_replicas: Vec::new(),
                    },
                    crate::wire::PartitionMetadata {
                        partition_index: 1,
                        leader_id: -1,
                        leader_epoch: -1,
                        replica_nodes: vec![4],
                        isr_nodes: Vec::new(),
                        offline_replicas: vec![4],
                    },
                ],
            }],
        }
    }

    #[test]
    fn partition_leader_resolves_present_and_skips_leaderless_or_unknown() {
        let metadata = metadata_with_topic();
        assert_eq!(partition_leader(&metadata, "orders", 0), Some(3));
        // Leaderless partition (-1) is treated as no leader.
        assert_eq!(partition_leader(&metadata, "orders", 1), None);
        // Unknown topic/partition.
        assert_eq!(partition_leader(&metadata, "orders", 9), None);
        assert_eq!(partition_leader(&metadata, "missing", 0), None);
    }

    #[test]
    fn unique_topics_dedups_preserving_order() {
        assert_eq!(
            unique_topics(&["orders", "payments", "orders", "events"]),
            vec![
                "orders".to_owned(),
                "payments".to_owned(),
                "events".to_owned(),
            ]
        );
    }

    #[test]
    fn elect_leaders_partitions_is_none_when_empty_else_grouped() {
        assert!(elect_leaders_partitions(Vec::new()).is_none());
        let topics = elect_leaders_partitions(vec![
            TopicPartition::new("orders", 0),
            TopicPartition::new("orders", 1),
            TopicPartition::new("payments", 0),
        ])
        .expect("partitions present");
        assert_eq!(topics.len(), 2);
        let orders = topics
            .iter()
            .find(|topic| topic.topic.as_str() == "orders")
            .expect("orders topic");
        assert_eq!(orders.partitions, vec![0, 1]);
    }

    #[test]
    fn build_alter_replica_log_dirs_groups_by_dir_then_topic() {
        let dirs = build_alter_replica_log_dirs(vec![
            ("/d1".to_owned(), "orders".to_owned(), 0),
            ("/d1".to_owned(), "orders".to_owned(), 1),
            ("/d1".to_owned(), "payments".to_owned(), 0),
            ("/d2".to_owned(), "orders".to_owned(), 2),
        ]);
        assert_eq!(dirs.len(), 2);
        let d1 = dirs
            .iter()
            .find(|dir| dir.path.as_str() == "/d1")
            .expect("/d1");
        assert_eq!(d1.topics.len(), 2);
        let orders = d1
            .topics
            .iter()
            .find(|topic| topic.name.as_str() == "orders")
            .expect("orders");
        assert_eq!(orders.partitions, vec![0, 1]);
        let d2 = dirs
            .iter()
            .find(|dir| dir.path.as_str() == "/d2")
            .expect("/d2");
        assert_eq!(d2.topics[0].partitions, vec![2]);
    }

    #[test]
    fn list_and_delete_records_topic_builders_group_and_map_fields() {
        let list = list_offsets_topics(vec![
            ("orders".to_owned(), 0, OffsetSpec::Earliest.to_wire()),
            ("orders".to_owned(), 1, OffsetSpec::Latest.to_wire()),
        ]);
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].partitions[0].timestamp, -2);
        assert_eq!(list[0].partitions[0].current_leader_epoch, -1);
        assert_eq!(list[0].partitions[1].timestamp, -1);

        let delete = delete_records_topics(vec![
            ("orders".to_owned(), 0, 100),
            ("payments".to_owned(), 0, -1),
        ]);
        assert_eq!(delete.len(), 2);
        assert_eq!(delete[0].partitions[0].offset, 100);
    }

    #[test]
    fn decode_authorized_operations_maps_bits_and_omitted() {
        // Integer.MIN_VALUE = "not requested", 0 = none.
        assert!(decode_authorized_operations(i32::MIN).is_empty());
        assert!(decode_authorized_operations(0).is_empty());

        // Read (code 3), Write (4), Describe (8).
        let bits = (1 << 3) | (1 << 4) | (1 << 8);
        let ops = decode_authorized_operations(bits);
        assert_eq!(ops.len(), 3);
        assert!(ops.contains(&AclOperation::Read));
        assert!(ops.contains(&AclOperation::Write));
        assert!(ops.contains(&AclOperation::Describe));
    }

    #[test]
    fn decode_member_assignment_round_trips_consumer_protocol() {
        use bytes::{BufMut, BytesMut};
        use kacrab_protocol::generated::consumer_protocol_assignment::TopicPartition as WireTopic;

        let mut buf = BytesMut::new();
        buf.put_i16(3); // ConsumerProtocol assignment version prefix
        ConsumerProtocolAssignmentData {
            assigned_partitions: vec![WireTopic {
                topic: "orders".to_owned().into(),
                partitions: vec![0, 2],
                _unknown_tagged_fields: Vec::new(),
            }],
            user_data: None,
            _unknown_tagged_fields: Vec::new(),
        }
        .write(&mut buf, 3)
        .expect("write assignment");

        assert_eq!(
            decode_member_assignment(&buf.freeze()),
            vec![
                TopicPartition::new("orders", 0),
                TopicPartition::new("orders", 2)
            ]
        );
        // Absent / too-short assignment decodes to empty.
        assert!(decode_member_assignment(&bytes::Bytes::new()).is_empty());
    }
}
