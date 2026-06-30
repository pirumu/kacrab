//! Public input and output types for the admin client.
//!
//! These mirror the shapes of Java's `org.apache.kafka.clients.admin` API
//! (`NewTopic`, `NewPartitions`, `ConfigResource`, `Config`, `Node`, ...) but
//! use idiomatic Rust ownership and avoid leaking the generated wire structs.

use kacrab_protocol::{
    KafkaUuid,
    generated::{
        CreatableReplicaAssignment, CreatableTopic, CreatableTopicConfig,
        CreatePartitionsAssignment, CreatePartitionsTopic, DescribeConfigsResource, ErrorCode,
        alter_configs_request::{AlterConfigsResource, AlterableConfig},
        incremental_alter_configs_request::{
            AlterConfigsResource as IncrementalAlterConfigsResource,
            AlterableConfig as IncrementalAlterableConfig,
        },
    },
};

use crate::common::{OffsetAndMetadata, TopicPartition};

/// A topic to create with [`AdminClient::create_topics`](super::AdminClient::create_topics).
///
/// Set partition count and replication factor for broker-assigned placement, or
/// supply explicit [`replica_assignments`](NewTopic::replica_assignments) for
/// manual placement (the two are mutually exclusive, matching Kafka).
#[derive(Debug, Clone)]
pub struct NewTopic {
    name: String,
    num_partitions: i32,
    replication_factor: i16,
    replica_assignments: Vec<(i32, Vec<i32>)>,
    configs: Vec<(String, Option<String>)>,
}

impl NewTopic {
    /// Create a topic with a fixed partition count and replication factor.
    #[must_use]
    pub fn new(name: impl Into<String>, num_partitions: i32, replication_factor: i16) -> Self {
        Self {
            name: name.into(),
            num_partitions,
            replication_factor,
            replica_assignments: Vec::new(),
            configs: Vec::new(),
        }
    }

    /// Create a topic with explicit per-partition replica assignments.
    ///
    /// Each entry maps a partition index to its ordered replica broker ids. The
    /// partition count and replication factor are derived by the broker from the
    /// assignment, so both are sent as `-1`.
    #[must_use]
    pub fn with_replica_assignments(
        name: impl Into<String>,
        assignments: Vec<(i32, Vec<i32>)>,
    ) -> Self {
        Self {
            name: name.into(),
            num_partitions: -1,
            replication_factor: -1,
            replica_assignments: assignments,
            configs: Vec::new(),
        }
    }

    /// Attach a topic-level config override (e.g. `retention.ms`). A `None` value
    /// asks the broker to use its default for that key.
    #[must_use]
    pub fn config(mut self, name: impl Into<String>, value: Option<String>) -> Self {
        self.configs.push((name.into(), value));
        self
    }

    /// The topic name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    pub(super) fn into_creatable(self) -> CreatableTopic {
        CreatableTopic {
            name: self.name.into(),
            num_partitions: self.num_partitions,
            replication_factor: self.replication_factor,
            assignments: self
                .replica_assignments
                .into_iter()
                .map(|(partition_index, broker_ids)| CreatableReplicaAssignment {
                    partition_index,
                    broker_ids,
                    _unknown_tagged_fields: Vec::new(),
                })
                .collect(),
            configs: self
                .configs
                .into_iter()
                .map(|(name, value)| CreatableTopicConfig {
                    name: name.into(),
                    value: value.map(Into::into),
                    _unknown_tagged_fields: Vec::new(),
                })
                .collect(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}

/// A request to increase a topic's partition count via
/// [`AdminClient::create_partitions`](super::AdminClient::create_partitions).
#[derive(Debug, Clone)]
pub struct NewPartitions {
    topic: String,
    total_count: i32,
    new_assignments: Vec<Vec<i32>>,
}

impl NewPartitions {
    /// Increase `topic` to `total_count` partitions, letting the broker place the
    /// new replicas.
    #[must_use]
    pub fn increase_to(topic: impl Into<String>, total_count: i32) -> Self {
        Self {
            topic: topic.into(),
            total_count,
            new_assignments: Vec::new(),
        }
    }

    /// Provide explicit replica broker ids for each newly added partition.
    #[must_use]
    pub fn assigning(mut self, new_assignments: Vec<Vec<i32>>) -> Self {
        self.new_assignments = new_assignments;
        self
    }

    /// The topic being expanded.
    #[must_use]
    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub(super) fn into_topic(self) -> CreatePartitionsTopic {
        let assignments = if self.new_assignments.is_empty() {
            None
        } else {
            Some(
                self.new_assignments
                    .into_iter()
                    .map(|broker_ids| CreatePartitionsAssignment {
                        broker_ids,
                        _unknown_tagged_fields: Vec::new(),
                    })
                    .collect(),
            )
        };
        CreatePartitionsTopic {
            name: self.topic.into(),
            count: self.total_count,
            assignments,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}

/// The kind of resource a [`ConfigResource`] addresses, mirroring Kafka's
/// `ConfigResource.Type` byte values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ResourceType {
    /// An unrecognized resource type.
    Unknown,
    /// A topic's dynamic config.
    Topic,
    /// A broker's dynamic config.
    Broker,
    /// A broker's logger levels.
    BrokerLogger,
    /// A consumer group's config.
    Group,
    /// Client-metrics subscription config.
    ClientMetrics,
}

impl ResourceType {
    pub(super) const fn to_wire(self) -> i8 {
        match self {
            Self::Unknown => 0,
            Self::Topic => 2,
            Self::Broker => 4,
            Self::BrokerLogger => 8,
            Self::ClientMetrics => 16,
            Self::Group => 32,
        }
    }

    pub(super) const fn from_wire(value: i8) -> Self {
        match value {
            2 => Self::Topic,
            4 => Self::Broker,
            8 => Self::BrokerLogger,
            16 => Self::ClientMetrics,
            32 => Self::Group,
            _ => Self::Unknown,
        }
    }
}

/// Identifies a configurable resource (a topic, broker, group, ...) for the
/// describe/alter config operations.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ConfigResource {
    /// The resource kind.
    pub resource_type: ResourceType,
    /// The resource name — a topic name, broker id as a string, etc. The empty
    /// string addresses the cluster-wide default for broker configs.
    pub name: String,
}

impl ConfigResource {
    /// Address a topic's configs.
    #[must_use]
    pub fn topic(name: impl Into<String>) -> Self {
        Self {
            resource_type: ResourceType::Topic,
            name: name.into(),
        }
    }

    /// Address a broker's configs by broker id.
    #[must_use]
    pub fn broker(broker_id: i32) -> Self {
        Self {
            resource_type: ResourceType::Broker,
            name: broker_id.to_string(),
        }
    }

    pub(super) fn to_describe(&self) -> DescribeConfigsResource {
        DescribeConfigsResource {
            resource_type: self.resource_type.to_wire(),
            resource_name: self.name.clone().into(),
            configuration_keys: None,
            _unknown_tagged_fields: Vec::new(),
        }
    }

    pub(super) fn to_alter(&self, entries: Vec<ConfigEntry>) -> AlterConfigsResource {
        AlterConfigsResource {
            resource_type: self.resource_type.to_wire(),
            resource_name: self.name.clone().into(),
            configs: entries
                .into_iter()
                .map(|entry| AlterableConfig {
                    name: entry.name.into(),
                    value: entry.value.map(Into::into),
                    _unknown_tagged_fields: Vec::new(),
                })
                .collect(),
            _unknown_tagged_fields: Vec::new(),
        }
    }

    pub(super) fn to_incremental(
        &self,
        ops: Vec<AlterConfigOp>,
    ) -> IncrementalAlterConfigsResource {
        IncrementalAlterConfigsResource {
            resource_type: self.resource_type.to_wire(),
            resource_name: self.name.clone().into(),
            configs: ops
                .into_iter()
                .map(|op| IncrementalAlterableConfig {
                    name: op.name.into(),
                    config_operation: op.op_type.to_wire(),
                    value: op.value.map(Into::into),
                    _unknown_tagged_fields: Vec::new(),
                })
                .collect(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
}

/// The kind of incremental config edit, mirroring Kafka's
/// `AlterConfigOp.OpType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlterConfigOpType {
    /// Set the config key to a value.
    Set,
    /// Reset the config key to its default / remove the override.
    Delete,
    /// Append values to a list-typed config key.
    Append,
    /// Remove values from a list-typed config key.
    Subtract,
}

impl AlterConfigOpType {
    pub(super) const fn to_wire(self) -> i8 {
        match self {
            Self::Set => 0,
            Self::Delete => 1,
            Self::Append => 2,
            Self::Subtract => 3,
        }
    }
}

/// One incremental config edit for
/// [`AdminClient::incremental_alter_configs`](super::AdminClient::incremental_alter_configs),
/// mirroring Java's `AlterConfigOp`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterConfigOp {
    /// The config key to edit.
    pub name: String,
    /// The value for `Set`/`Append`/`Subtract`; ignored (and sent as null) for
    /// `Delete`.
    pub value: Option<String>,
    /// The kind of edit.
    pub op_type: AlterConfigOpType,
}

impl AlterConfigOp {
    /// Set a config key to `value`.
    #[must_use]
    pub fn set(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            value: Some(value.into()),
            op_type: AlterConfigOpType::Set,
        }
    }

    /// Reset a config key to its default.
    #[must_use]
    pub fn delete(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            value: None,
            op_type: AlterConfigOpType::Delete,
        }
    }

    /// Append `value` to a list-typed config key.
    #[must_use]
    pub fn append(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            value: Some(value.into()),
            op_type: AlterConfigOpType::Append,
        }
    }

    /// Remove `value` from a list-typed config key.
    #[must_use]
    pub fn subtract(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            value: Some(value.into()),
            op_type: AlterConfigOpType::Subtract,
        }
    }
}

/// A single config key/value pair, used both when altering configs (input) and
/// when describing them (output).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigEntry {
    /// The config key.
    pub name: String,
    /// The config value, or `None` to clear it / use the broker default.
    pub value: Option<String>,
    /// Whether the broker reports this key as read-only (describe only).
    pub read_only: bool,
    /// Whether the broker redacted the value as sensitive (describe only).
    pub is_sensitive: bool,
    /// Where the broker says the value came from (describe only).
    pub source: ConfigSource,
}

impl ConfigEntry {
    /// Build a config entry to set or clear when altering configs.
    #[must_use]
    pub fn set(name: impl Into<String>, value: Option<String>) -> Self {
        Self {
            name: name.into(),
            value,
            read_only: false,
            is_sensitive: false,
            source: ConfigSource::Unknown,
        }
    }
}

/// Where a described config value originated, mirroring Kafka's
/// `DescribeConfigsResponse.ConfigSource`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigSource {
    /// Source not reported / not recognized.
    Unknown,
    /// Set as a per-topic config.
    TopicConfig,
    /// Set dynamically on a single broker.
    DynamicBrokerConfig,
    /// Set dynamically as the cluster-wide broker default.
    DynamicDefaultBrokerConfig,
    /// Read from the broker's static `server.properties`.
    StaticBrokerConfig,
    /// The built-in default.
    DefaultConfig,
    /// A dynamically configured broker logger level.
    DynamicBrokerLoggerConfig,
}

impl ConfigSource {
    pub(super) const fn from_wire(value: i8) -> Self {
        match value {
            1 => Self::TopicConfig,
            2 => Self::DynamicBrokerConfig,
            3 => Self::DynamicDefaultBrokerConfig,
            4 => Self::StaticBrokerConfig,
            5 => Self::DefaultConfig,
            6 => Self::DynamicBrokerLoggerConfig,
            _ => Self::Unknown,
        }
    }
}

/// The described configs of one resource: its addressed resource plus the
/// config entries the broker returned.
#[derive(Debug, Clone)]
pub struct ResourceConfig {
    /// The resource these configs belong to.
    pub resource: ConfigResource,
    /// The config entries, in broker order.
    pub entries: Vec<ConfigEntry>,
}

// `Node` lives in `kacrab::common` (always compiled) and is re-exported here so
// `kacrab::admin::Node` keeps working and describe operations share one type.
pub use crate::common::Node;

/// A description of the cluster: its id, the current controller, and the live
/// broker nodes.
#[derive(Debug, Clone)]
pub struct ClusterDescription {
    /// The cluster id, if the broker reports one.
    pub cluster_id: Option<String>,
    /// The controller node, or `None` when the cluster has no controller yet.
    pub controller: Option<Node>,
    /// The live broker nodes.
    pub nodes: Vec<Node>,
}

/// A topic entry returned by
/// [`AdminClient::list_topics`](super::AdminClient::list_topics), mirroring
/// Java's `TopicListing`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicListing {
    /// Topic name.
    pub name: String,
    /// Stable topic id (`KafkaUuid::ZERO` if the broker did not report one).
    pub topic_id: KafkaUuid,
    /// Whether this is an internal topic (e.g. `__consumer_offsets`).
    pub is_internal: bool,
}

/// Per-partition leadership and replica placement, mirroring Java's
/// `TopicPartitionInfo`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicPartitionInfo {
    /// Partition index.
    pub partition: i32,
    /// Current leader node, or `None` when the partition has no leader.
    pub leader: Option<Node>,
    /// Replica nodes, in assignment order.
    pub replicas: Vec<Node>,
    /// In-sync replica nodes.
    pub isr: Vec<Node>,
}

/// A full topic description returned by
/// [`AdminClient::describe_topics`](super::AdminClient::describe_topics),
/// mirroring Java's `TopicDescription`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicDescription {
    /// Topic name.
    pub name: String,
    /// Stable topic id.
    pub topic_id: KafkaUuid,
    /// Whether this is an internal topic.
    pub is_internal: bool,
    /// Partition placement, ordered by partition index.
    pub partitions: Vec<TopicPartitionInfo>,
}

/// Options for [`AdminClient::list_topics`](super::AdminClient::list_topics).
#[derive(Debug, Clone, Default)]
pub struct ListTopicsOptions {
    /// Include internal topics in the listing (Java `listInternal`, default off).
    pub list_internal: bool,
}

/// Options for
/// [`AdminClient::describe_topics`](super::AdminClient::describe_topics).
#[derive(Debug, Clone, Default)]
pub struct DescribeTopicsOptions;

/// Options for [`AdminClient::create_topics`](super::AdminClient::create_topics).
#[derive(Debug, Clone, Default)]
pub struct CreateTopicsOptions {
    /// Validate the request on the broker without actually creating the topics.
    pub validate_only: bool,
}

/// Options for
/// [`AdminClient::create_partitions`](super::AdminClient::create_partitions).
#[derive(Debug, Clone, Default)]
pub struct CreatePartitionsOptions {
    /// Validate the request on the broker without actually adding partitions.
    pub validate_only: bool,
}

/// Options for [`AdminClient::alter_configs`](super::AdminClient::alter_configs).
#[derive(Debug, Clone, Default)]
pub struct AlterConfigsOptions {
    /// Validate the request on the broker without applying the changes.
    pub validate_only: bool,
}

/// A consumer group entry returned by
/// [`AdminClient::list_consumer_groups`](super::AdminClient::list_consumer_groups),
/// mirroring Java's `ConsumerGroupListing`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerGroupListing {
    /// Consumer group id.
    pub group_id: String,
    /// Whether this is a "simple" consumer group (one with no protocol type, i.e.
    /// managed directly via the offset APIs rather than the group protocol).
    pub is_simple_consumer_group: bool,
    /// The broker-reported group state name (e.g. `Stable`, `Empty`), if any.
    pub state: Option<String>,
    /// The broker-reported group type name (e.g. `classic`, `consumer`), if any.
    pub group_type: Option<String>,
}

/// One member of a consumer group, mirroring Java's `MemberDescription`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberDescription {
    /// The member id assigned by the coordinator.
    pub member_id: String,
    /// The static group instance id, if the member joined with one.
    pub group_instance_id: Option<String>,
    /// The client id the member connected with.
    pub client_id: String,
    /// The host the member connected from.
    pub host: String,
}

/// A consumer group description returned by
/// [`AdminClient::describe_consumer_groups`](super::AdminClient::describe_consumer_groups),
/// mirroring Java's `ConsumerGroupDescription`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerGroupDescription {
    /// Consumer group id.
    pub group_id: String,
    /// Whether this is a "simple" consumer group (empty protocol type).
    pub is_simple_consumer_group: bool,
    /// The group members.
    pub members: Vec<MemberDescription>,
    /// The partition assignor / protocol the group settled on.
    pub partition_assignor: String,
    /// The broker-reported group state name (e.g. `Stable`, `Empty`).
    pub state: String,
    /// The group's coordinator broker.
    pub coordinator: Node,
}

/// A committed offset for one partition, returned by
/// [`AdminClient::list_consumer_group_offsets`](super::AdminClient::list_consumer_group_offsets).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupOffset {
    /// The partition the offset belongs to.
    pub partition: TopicPartition,
    /// The committed offset and its metadata.
    pub offset: OffsetAndMetadata,
}

/// Options for
/// [`AdminClient::list_consumer_groups`](super::AdminClient::list_consumer_groups).
#[derive(Debug, Clone, Default)]
pub struct ListConsumerGroupsOptions {
    /// Only return groups in these states (broker state names); empty = all.
    pub states_filter: Vec<String>,
    /// Only return groups of these types (broker type names); empty = all.
    pub types_filter: Vec<String>,
}

/// Options for
/// [`AdminClient::describe_consumer_groups`](super::AdminClient::describe_consumer_groups).
#[derive(Debug, Clone, Default)]
pub struct DescribeConsumerGroupsOptions {
    /// Ask the broker to include the caller's authorized operations.
    pub include_authorized_operations: bool,
}

/// Options for
/// [`AdminClient::list_consumer_group_offsets`](super::AdminClient::list_consumer_group_offsets).
#[derive(Debug, Clone, Default)]
pub struct ListConsumerGroupOffsetsOptions {
    /// Restrict the fetch to these partitions; empty fetches all committed
    /// partitions for the group.
    pub partitions: Vec<TopicPartition>,
    /// Only return offsets for partitions with stable (non-transactional-pending)
    /// commits.
    pub require_stable: bool,
}

/// Which leader election to trigger via
/// [`AdminClient::elect_leaders`](super::AdminClient::elect_leaders), mirroring
/// Kafka's `ElectionType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ElectionType {
    /// Elect the preferred (first) replica as leader where possible.
    Preferred,
    /// Elect any in-sync replica as leader, allowing an unclean election from a
    /// non-preferred replica.
    Unclean,
}

impl ElectionType {
    pub(super) const fn to_wire(self) -> i8 {
        match self {
            Self::Preferred => 0,
            Self::Unclean => 1,
        }
    }
}

/// Which offset to look up for a partition in
/// [`AdminClient::list_offsets`](super::AdminClient::list_offsets), mirroring
/// Kafka's `OffsetSpec`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OffsetSpec {
    /// The earliest available offset (log start).
    Earliest,
    /// The next offset to be written (log end / high watermark).
    Latest,
    /// The offset of the record with the largest timestamp.
    MaxTimestamp,
    /// The earliest offset whose record timestamp is `>=` this value (ms).
    Timestamp(i64),
}

impl OffsetSpec {
    /// The wire timestamp sentinel/value Kafka uses for this spec.
    pub(super) const fn to_wire(self) -> i64 {
        match self {
            Self::Earliest => -2,
            Self::Latest => -1,
            Self::MaxTimestamp => -3,
            Self::Timestamp(timestamp) => timestamp,
        }
    }
}

/// One partition's resolved offset from
/// [`AdminClient::list_offsets`](super::AdminClient::list_offsets), mirroring
/// Java's `ListOffsetsResultInfo`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListOffsetsResult {
    /// The partition the offset belongs to.
    pub partition: TopicPartition,
    /// The resolved offset.
    pub offset: i64,
    /// The timestamp of the record at `offset`, or `-1` when not applicable.
    pub timestamp: i64,
    /// The leader epoch at `offset`, if the broker reported one.
    pub leader_epoch: Option<i32>,
}

/// The new low watermark of a partition after
/// [`AdminClient::delete_records`](super::AdminClient::delete_records).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeletedRecords {
    /// The partition whose records were deleted.
    pub partition: TopicPartition,
    /// The partition's low watermark after the delete (the first offset that
    /// still exists).
    pub low_watermark: i64,
}

/// One active producer's state on a partition, mirroring Java's `ProducerState`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProducerState {
    /// Producer id.
    pub producer_id: i64,
    /// Producer epoch.
    pub producer_epoch: i32,
    /// The last sequence number the producer wrote.
    pub last_sequence: i32,
    /// The timestamp of the producer's last write (ms).
    pub last_timestamp: i64,
    /// The coordinator epoch of the producer's current transaction, or `-1`.
    pub coordinator_epoch: i32,
    /// The start offset of the producer's current transaction, or `-1` when no
    /// transaction is open.
    pub current_transaction_start_offset: i64,
}

/// The active producers on one partition, from
/// [`AdminClient::describe_producers`](super::AdminClient::describe_producers).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionProducerState {
    /// The partition these producers are writing to.
    pub partition: TopicPartition,
    /// The active producers on the partition.
    pub active_producers: Vec<ProducerState>,
}

/// A transaction's description from
/// [`AdminClient::describe_transactions`](super::AdminClient::describe_transactions),
/// mirroring Java's `TransactionDescription`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionDescription {
    /// The transactional id.
    pub transactional_id: String,
    /// The broker-reported transaction state name (e.g. `Ongoing`, `Empty`).
    pub state: String,
    /// The producer id bound to the transactional id.
    pub producer_id: i64,
    /// The producer epoch.
    pub producer_epoch: i16,
    /// The configured transaction timeout (ms).
    pub transaction_timeout_ms: i32,
    /// When the current transaction started (ms since epoch), or `-1`.
    pub transaction_start_time_ms: i64,
    /// The partitions enrolled in the current transaction.
    pub topic_partitions: Vec<TopicPartition>,
    /// The transaction coordinator broker.
    pub coordinator: Node,
}

/// A transaction listing from
/// [`AdminClient::list_transactions`](super::AdminClient::list_transactions),
/// mirroring Java's `TransactionListing`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionListing {
    /// The transactional id.
    pub transactional_id: String,
    /// The producer id bound to the transactional id.
    pub producer_id: i64,
    /// The broker-reported transaction state name.
    pub state: String,
}

/// Options for
/// [`AdminClient::list_transactions`](super::AdminClient::list_transactions).
#[derive(Debug, Clone, Default)]
pub struct ListTransactionsOptions {
    /// Only list transactions in these states (broker state names); empty = all.
    pub state_filters: Vec<String>,
    /// Only list transactions of these producer ids; empty = all.
    pub producer_id_filters: Vec<i64>,
}

/// One replica's on-disk footprint within a log dir, mirroring Java's
/// `ReplicaInfo`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogDirReplicaInfo {
    /// The replica's size on disk, in bytes.
    pub size: i64,
    /// The lag of the replica's log end offset behind the partition high
    /// watermark (`0` for the leader).
    pub offset_lag: i64,
    /// Whether this is a "future" replica being created by a reassignment to a
    /// different log dir.
    pub is_future: bool,
}

/// The replicas hosted in one broker log directory, mirroring Java's
/// `LogDirDescription`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogDirDescription {
    /// The absolute log directory path.
    pub log_dir: String,
    /// The error the broker reported for this log dir, if any.
    pub error: Option<ErrorCode>,
    /// Total size of the volume backing the log dir, in bytes (`-1` if unknown).
    pub total_bytes: i64,
    /// Usable (free) size of the volume, in bytes (`-1` if unknown).
    pub usable_bytes: i64,
    /// The replicas hosted in this log dir.
    pub replicas: Vec<(TopicPartition, LogDirReplicaInfo)>,
}

/// One broker's log directories from
/// [`AdminClient::describe_log_dirs`](super::AdminClient::describe_log_dirs).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerLogDirs {
    /// The broker the log dirs belong to.
    pub broker_id: i32,
    /// The broker's log directories.
    pub log_dirs: Vec<LogDirDescription>,
}

/// A requested partition reassignment for
/// [`AdminClient::alter_partition_reassignments`](super::AdminClient::alter_partition_reassignments),
/// mirroring Java's `Optional<NewPartitionReassignment>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewPartitionReassignment {
    /// The partition to reassign.
    pub topic_partition: TopicPartition,
    /// The target replica broker ids, or `None` to cancel an ongoing
    /// reassignment of this partition.
    pub replicas: Option<Vec<i32>>,
}

impl NewPartitionReassignment {
    /// Reassign the partition to `replicas`.
    #[must_use]
    pub const fn assigning(topic_partition: TopicPartition, replicas: Vec<i32>) -> Self {
        Self {
            topic_partition,
            replicas: Some(replicas),
        }
    }

    /// Cancel an ongoing reassignment of the partition.
    #[must_use]
    pub const fn cancel(topic_partition: TopicPartition) -> Self {
        Self {
            topic_partition,
            replicas: None,
        }
    }
}

/// An ongoing partition reassignment from
/// [`AdminClient::list_partition_reassignments`](super::AdminClient::list_partition_reassignments),
/// mirroring Java's `PartitionReassignment`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionReassignment {
    /// The partition being reassigned.
    pub topic_partition: TopicPartition,
    /// The current full replica set (including replicas being added/removed).
    pub replicas: Vec<i32>,
    /// Replicas being added by the reassignment.
    pub adding_replicas: Vec<i32>,
    /// Replicas being removed by the reassignment.
    pub removing_replicas: Vec<i32>,
}

/// How a feature's version level is being changed by
/// [`AdminClient::update_features`](super::AdminClient::update_features),
/// mirroring Kafka's `FeatureUpdate.UpgradeType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeatureUpdateUpgradeType {
    /// Raise the feature's max version level.
    Upgrade,
    /// Lower the version level, refusing if it would lose metadata.
    SafeDowngrade,
    /// Lower the version level even if it may lose metadata.
    UnsafeDowngrade,
}

impl FeatureUpdateUpgradeType {
    pub(super) const fn to_wire(self) -> i8 {
        match self {
            Self::Upgrade => 1,
            Self::SafeDowngrade => 2,
            Self::UnsafeDowngrade => 3,
        }
    }

    pub(super) const fn allows_downgrade(self) -> bool {
        !matches!(self, Self::Upgrade)
    }
}

/// A single feature version-level change for
/// [`AdminClient::update_features`](super::AdminClient::update_features),
/// mirroring Java's `FeatureUpdate`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeatureUpdate {
    /// The feature name.
    pub feature: String,
    /// The target max version level (`0` finalizes/removes the feature).
    pub max_version_level: i16,
    /// How the level is being changed.
    pub upgrade_type: FeatureUpdateUpgradeType,
}

impl FeatureUpdate {
    /// Create a feature update.
    #[must_use]
    pub fn new(
        feature: impl Into<String>,
        max_version_level: i16,
        upgrade_type: FeatureUpdateUpgradeType,
    ) -> Self {
        Self {
            feature: feature.into(),
            max_version_level,
            upgrade_type,
        }
    }
}

/// The kind of resource an ACL applies to, mirroring Kafka's `ResourceType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AclResourceType {
    /// Unknown / unrecognized.
    Unknown,
    /// Matches any resource type (filters only).
    Any,
    /// A topic.
    Topic,
    /// A consumer group.
    Group,
    /// The cluster.
    Cluster,
    /// A transactional id.
    TransactionalId,
    /// A delegation token.
    DelegationToken,
    /// A user (SCRAM/quota principal).
    User,
}

impl AclResourceType {
    pub(super) const fn to_wire(self) -> i8 {
        match self {
            Self::Unknown => 0,
            Self::Any => 1,
            Self::Topic => 2,
            Self::Group => 3,
            Self::Cluster => 4,
            Self::TransactionalId => 5,
            Self::DelegationToken => 6,
            Self::User => 7,
        }
    }

    pub(super) const fn from_wire(value: i8) -> Self {
        match value {
            1 => Self::Any,
            2 => Self::Topic,
            3 => Self::Group,
            4 => Self::Cluster,
            5 => Self::TransactionalId,
            6 => Self::DelegationToken,
            7 => Self::User,
            _ => Self::Unknown,
        }
    }
}

/// How an ACL resource name is matched, mirroring Kafka's `PatternType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AclPatternType {
    /// Unknown / unrecognized.
    Unknown,
    /// Matches any pattern type (filters only).
    Any,
    /// Matches literal and prefixed patterns that would match a name (filters).
    Match,
    /// An exact resource name.
    Literal,
    /// A resource-name prefix.
    Prefixed,
}

impl AclPatternType {
    pub(super) const fn to_wire(self) -> i8 {
        match self {
            Self::Unknown => 0,
            Self::Any => 1,
            Self::Match => 2,
            Self::Literal => 3,
            Self::Prefixed => 4,
        }
    }

    pub(super) const fn from_wire(value: i8) -> Self {
        match value {
            1 => Self::Any,
            2 => Self::Match,
            3 => Self::Literal,
            4 => Self::Prefixed,
            _ => Self::Unknown,
        }
    }
}

/// An ACL operation, mirroring Kafka's `AclOperation`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[expect(
    missing_docs,
    reason = "Variants mirror Kafka's AclOperation constants 1:1."
)]
pub enum AclOperation {
    Unknown,
    Any,
    All,
    Read,
    Write,
    Create,
    Delete,
    Alter,
    Describe,
    ClusterAction,
    DescribeConfigs,
    AlterConfigs,
    IdempotentWrite,
    CreateTokens,
    DescribeTokens,
}

impl AclOperation {
    pub(super) const fn to_wire(self) -> i8 {
        match self {
            Self::Unknown => 0,
            Self::Any => 1,
            Self::All => 2,
            Self::Read => 3,
            Self::Write => 4,
            Self::Create => 5,
            Self::Delete => 6,
            Self::Alter => 7,
            Self::Describe => 8,
            Self::ClusterAction => 9,
            Self::DescribeConfigs => 10,
            Self::AlterConfigs => 11,
            Self::IdempotentWrite => 12,
            Self::CreateTokens => 13,
            Self::DescribeTokens => 14,
        }
    }

    pub(super) const fn from_wire(value: i8) -> Self {
        match value {
            1 => Self::Any,
            2 => Self::All,
            3 => Self::Read,
            4 => Self::Write,
            5 => Self::Create,
            6 => Self::Delete,
            7 => Self::Alter,
            8 => Self::Describe,
            9 => Self::ClusterAction,
            10 => Self::DescribeConfigs,
            11 => Self::AlterConfigs,
            12 => Self::IdempotentWrite,
            13 => Self::CreateTokens,
            14 => Self::DescribeTokens,
            _ => Self::Unknown,
        }
    }
}

/// Whether an ACL allows or denies, mirroring Kafka's `AclPermissionType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AclPermissionType {
    /// Unknown / unrecognized.
    Unknown,
    /// Matches any permission type (filters only).
    Any,
    /// Denies the operation.
    Deny,
    /// Allows the operation.
    Allow,
}

impl AclPermissionType {
    pub(super) const fn to_wire(self) -> i8 {
        match self {
            Self::Unknown => 0,
            Self::Any => 1,
            Self::Deny => 2,
            Self::Allow => 3,
        }
    }

    pub(super) const fn from_wire(value: i8) -> Self {
        match value {
            1 => Self::Any,
            2 => Self::Deny,
            3 => Self::Allow,
            _ => Self::Unknown,
        }
    }
}

/// An ACL binding (rule), mirroring Java's `AclBinding`.
///
/// Used as input to [`create_acls`](super::AdminClient::create_acls) and
/// returned by [`describe_acls`](super::AdminClient::describe_acls).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AclBinding {
    /// The resource kind.
    pub resource_type: AclResourceType,
    /// The resource name.
    pub resource_name: String,
    /// How the name is matched.
    pub pattern_type: AclPatternType,
    /// The principal (e.g. `User:alice`).
    pub principal: String,
    /// The host the rule applies to (`*` for any).
    pub host: String,
    /// The operation governed.
    pub operation: AclOperation,
    /// Allow or deny.
    pub permission_type: AclPermissionType,
}

/// A filter selecting ACL bindings, mirroring Java's `AclBindingFilter`.
///
/// Used by [`describe_acls`](super::AdminClient::describe_acls) and
/// [`delete_acls`](super::AdminClient::delete_acls). `None` string fields and
/// `Any` enums match anything.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AclBindingFilter {
    /// The resource kind to match (`Any` matches all).
    pub resource_type: AclResourceType,
    /// The resource name to match, or `None` for any.
    pub resource_name: Option<String>,
    /// The pattern type to match (`Any` matches all).
    pub pattern_type: AclPatternType,
    /// The principal to match, or `None` for any.
    pub principal: Option<String>,
    /// The host to match, or `None` for any.
    pub host: Option<String>,
    /// The operation to match (`Any` matches all).
    pub operation: AclOperation,
    /// The permission type to match (`Any` matches all).
    pub permission_type: AclPermissionType,
}

impl AclBindingFilter {
    /// A filter matching every ACL (`describe_acls`/`delete_acls` "match all").
    #[must_use]
    pub const fn any() -> Self {
        Self {
            resource_type: AclResourceType::Any,
            resource_name: None,
            pattern_type: AclPatternType::Any,
            principal: None,
            host: None,
            operation: AclOperation::Any,
            permission_type: AclPermissionType::Any,
        }
    }
}

/// A client-quota entity, mirroring Java's `ClientQuotaEntity`.
///
/// An ordered set of `(entity_type, entity_name)` pairs (a `None` name addresses
/// the default entity of that type). Common entity types are
/// [`USER`](Self::USER), [`CLIENT_ID`](Self::CLIENT_ID), and [`IP`](Self::IP).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ClientQuotaEntity {
    /// The `(entity_type, entity_name)` pairs; `None` name = the type's default.
    pub entries: Vec<(String, Option<String>)>,
}

impl ClientQuotaEntity {
    /// The `user` entity type.
    pub const USER: &'static str = "user";
    /// The `client-id` entity type.
    pub const CLIENT_ID: &'static str = "client-id";
    /// The `ip` entity type.
    pub const IP: &'static str = "ip";

    /// Build an entity from `(entity_type, entity_name)` pairs.
    #[must_use]
    pub const fn new(entries: Vec<(String, Option<String>)>) -> Self {
        Self { entries }
    }
}

/// How a [`ClientQuotaFilterComponent`] matches an entity name, mirroring
/// Kafka's quota match types.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientQuotaMatch {
    /// Match a specific entity name.
    Exact(String),
    /// Match only the default entity of the type.
    Default,
    /// Match any entity of the type.
    Any,
}

/// One component of a client-quota filter for
/// [`describe_client_quotas`](super::AdminClient::describe_client_quotas).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientQuotaFilterComponent {
    /// The entity type to match (e.g. `user`, `client-id`).
    pub entity_type: String,
    /// How to match the entity name.
    pub match_type: ClientQuotaMatch,
}

/// A described quota entity and its quota values, returned by
/// [`describe_client_quotas`](super::AdminClient::describe_client_quotas).
#[derive(Debug, Clone, PartialEq)]
pub struct ClientQuotaEntry {
    /// The entity the quotas apply to.
    pub entity: ClientQuotaEntity,
    /// The quota `(key, value)` pairs (e.g. `producer_byte_rate`).
    pub quotas: Vec<(String, f64)>,
}

/// A single quota change (set when `value` is `Some`, remove when `None`),
/// mirroring Java's `ClientQuotaAlteration.Op`.
#[derive(Debug, Clone, PartialEq)]
pub struct ClientQuotaOp {
    /// The quota key (e.g. `consumer_byte_rate`).
    pub key: String,
    /// The new value, or `None` to remove the quota.
    pub value: Option<f64>,
}

/// A set of quota changes to apply to one entity via
/// [`alter_client_quotas`](super::AdminClient::alter_client_quotas), mirroring
/// Java's `ClientQuotaAlteration`.
#[derive(Debug, Clone, PartialEq)]
pub struct ClientQuotaAlteration {
    /// The entity to change.
    pub entity: ClientQuotaEntity,
    /// The quota operations to apply.
    pub ops: Vec<ClientQuotaOp>,
}

/// A SCRAM hash mechanism, mirroring Kafka's `ScramMechanism`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScramMechanism {
    /// Unknown / unrecognized.
    Unknown,
    /// SCRAM-SHA-256.
    ScramSha256,
    /// SCRAM-SHA-512.
    ScramSha512,
}

impl ScramMechanism {
    pub(super) const fn to_wire(self) -> i8 {
        match self {
            Self::Unknown => 0,
            Self::ScramSha256 => 1,
            Self::ScramSha512 => 2,
        }
    }

    pub(super) const fn from_wire(value: i8) -> Self {
        match value {
            1 => Self::ScramSha256,
            2 => Self::ScramSha512,
            _ => Self::Unknown,
        }
    }
}

/// One stored SCRAM credential's parameters, mirroring Java's
/// `ScramCredentialInfo`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScramCredentialInfo {
    /// The hash mechanism.
    pub mechanism: ScramMechanism,
    /// The iteration count.
    pub iterations: i32,
}

/// A user's SCRAM credentials, returned by
/// [`describe_user_scram_credentials`](super::AdminClient::describe_user_scram_credentials).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserScramCredentials {
    /// The user name.
    pub user: String,
    /// The user's stored credentials, one per mechanism.
    pub credentials: Vec<ScramCredentialInfo>,
}

/// A SCRAM credential to create or update via
/// [`alter_user_scram_credentials`](super::AdminClient::alter_user_scram_credentials).
///
/// The caller supplies the already-salted password (kacrab does not perform the
/// SCRAM salting itself), matching the wire shape.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScramCredentialUpsertion {
    /// The user name.
    pub user: String,
    /// The hash mechanism.
    pub mechanism: ScramMechanism,
    /// The iteration count.
    pub iterations: i32,
    /// The random salt.
    pub salt: Vec<u8>,
    /// The salted password (the SCRAM `SaltedPassword`).
    pub salted_password: Vec<u8>,
}

/// A SCRAM credential to delete via
/// [`alter_user_scram_credentials`](super::AdminClient::alter_user_scram_credentials).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScramCredentialDeletion {
    /// The user name.
    pub user: String,
    /// The mechanism to delete.
    pub mechanism: ScramMechanism,
}

/// A SASL/OAUTHBEARER-style delegation token, mirroring Java's `DelegationToken`
/// / `TokenInformation`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DelegationToken {
    /// The token id.
    pub token_id: String,
    /// The token owner's principal type (e.g. `User`).
    pub owner_principal_type: String,
    /// The token owner's principal name.
    pub owner_principal_name: String,
    /// When the token was issued (ms since epoch).
    pub issue_timestamp_ms: i64,
    /// When the token currently expires (ms since epoch).
    pub expiry_timestamp_ms: i64,
    /// The latest time the token can be renewed to (ms since epoch).
    pub max_timestamp_ms: i64,
    /// The token HMAC (the secret used to authenticate with the token).
    pub hmac: Vec<u8>,
    /// The principals allowed to renew the token, as `(type, name)` pairs.
    pub renewers: Vec<(String, String)>,
}

/// Options for
/// [`create_delegation_token`](super::AdminClient::create_delegation_token).
#[derive(Debug, Clone, Default)]
pub struct CreateDelegationTokenOptions {
    /// The token owner `(principal_type, principal_name)`, or `None` for the
    /// authenticated principal.
    pub owner: Option<(String, String)>,
    /// Principals allowed to renew the token, as `(type, name)` pairs.
    pub renewers: Vec<(String, String)>,
    /// The token's max lifetime in ms, or `-1` for the broker default.
    pub max_lifetime_ms: i64,
}

/// A replica-to-log-directory assignment for
/// [`alter_replica_log_dirs`](super::AdminClient::alter_replica_log_dirs),
/// mirroring Java's `TopicPartitionReplica` → log-dir map entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaLogDirAssignment {
    /// The partition whose replica is being moved.
    pub topic_partition: TopicPartition,
    /// The broker hosting the replica.
    pub broker_id: i32,
    /// The absolute target log directory path on that broker.
    pub log_dir: String,
}

/// The result of fencing one producer via
/// [`fence_producers`](super::AdminClient::fence_producers).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FencedProducer {
    /// The transactional id that was fenced.
    pub transactional_id: String,
    /// The producer id now bound to the transactional id.
    pub producer_id: i64,
    /// The bumped producer epoch (old producers are now fenced).
    pub producer_epoch: i16,
}

/// Identifies a transaction to forcibly abort via
/// [`abort_transaction`](super::AdminClient::abort_transaction), mirroring Java's
/// `AbortTransactionSpec`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AbortTransactionSpec {
    /// The partition with the hanging transaction.
    pub topic_partition: TopicPartition,
    /// The producer id of the hanging transaction.
    pub producer_id: i64,
    /// The producer epoch of the hanging transaction.
    pub producer_epoch: i16,
    /// The transaction coordinator epoch (from `describe_producers`).
    pub coordinator_epoch: i32,
}

/// A feature's broker-supported version range, mirroring Java's
/// `SupportedVersionRange`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SupportedVersionRange {
    /// The minimum supported version.
    pub min_version: i16,
    /// The maximum supported version.
    pub max_version: i16,
}

/// A feature's cluster-finalized version range, mirroring Java's
/// `FinalizedVersionRange`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FinalizedVersionRange {
    /// The minimum finalized version level.
    pub min_version_level: i16,
    /// The maximum finalized version level.
    pub max_version_level: i16,
}

/// The cluster's feature metadata from
/// [`describe_features`](super::AdminClient::describe_features), mirroring Java's
/// `FeatureMetadata`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeatureMetadata {
    /// The epoch of the finalized feature set, if the broker reports one.
    pub finalized_features_epoch: Option<i64>,
    /// Each feature's broker-supported range, by feature name.
    pub supported_features: Vec<(String, SupportedVersionRange)>,
    /// Each feature's cluster-finalized range, by feature name.
    pub finalized_features: Vec<(String, FinalizedVersionRange)>,
}

/// A consumer-group member to remove, mirroring Java's `MemberToRemove`.
///
/// Used by
/// [`remove_members_from_consumer_group`](super::AdminClient::remove_members_from_consumer_group):
/// identify a static member by its `group_instance_id` or a dynamic member by
/// its `member_id`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MemberToRemove {
    /// The dynamic member id, if removing a dynamic member.
    pub member_id: Option<String>,
    /// The static group instance id, if removing a static member.
    pub group_instance_id: Option<String>,
}

impl MemberToRemove {
    /// Remove a static member by its group instance id.
    #[must_use]
    pub fn static_member(group_instance_id: impl Into<String>) -> Self {
        Self {
            member_id: None,
            group_instance_id: Some(group_instance_id.into()),
        }
    }

    /// Remove a dynamic member by its member id.
    #[must_use]
    pub fn dynamic_member(member_id: impl Into<String>) -> Self {
        Self {
            member_id: Some(member_id.into()),
            group_instance_id: None,
        }
    }
}

/// The current/future log directory of one replica, returned by
/// [`describe_replica_log_dirs`](super::AdminClient::describe_replica_log_dirs),
/// mirroring Java's `ReplicaLogDirInfo`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaLogDirInfo {
    /// The replica's partition.
    pub topic_partition: TopicPartition,
    /// The broker hosting the replica.
    pub broker_id: i32,
    /// The log dir currently hosting the replica, if known.
    pub current_log_dir: Option<String>,
    /// The log dir a pending move is creating the replica in, if any.
    pub future_log_dir: Option<String>,
}

/// One replica's state in the metadata quorum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QuorumReplicaState {
    /// The replica (voter/observer) node id.
    pub replica_id: i32,
    /// The replica's log end offset.
    pub log_end_offset: i64,
}

/// The metadata (`KRaft`) quorum state from
/// [`describe_metadata_quorum`](super::AdminClient::describe_metadata_quorum),
/// mirroring Java's `QuorumInfo`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuorumInfo {
    /// The current quorum leader node id.
    pub leader_id: i32,
    /// The current leader epoch.
    pub leader_epoch: i32,
    /// The quorum high watermark.
    pub high_watermark: i64,
    /// The voter replicas and their state.
    pub voters: Vec<QuorumReplicaState>,
    /// The observer replicas and their state.
    pub observers: Vec<QuorumReplicaState>,
}

/// One advertised endpoint of a `KRaft` voter for
/// [`add_raft_voter`](super::AdminClient::add_raft_voter).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaftVoterEndpoint {
    /// The listener name (e.g. `CONTROLLER`).
    pub name: String,
    /// The advertised host.
    pub host: String,
    /// The advertised port.
    pub port: u16,
}

/// A share-group description from
/// [`describe_share_groups`](super::AdminClient::describe_share_groups)
/// (Kafka 4.x share groups, KIP-932).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareGroupDescription {
    /// The share group id.
    pub group_id: String,
    /// The broker-reported group state name.
    pub state: String,
    /// The group epoch.
    pub group_epoch: i32,
    /// The assignor the group settled on.
    pub assignor_name: String,
    /// The group members.
    pub members: Vec<MemberDescription>,
    /// The group's coordinator broker.
    pub coordinator: Node,
}

/// A streams-group description from
/// [`describe_streams_groups`](super::AdminClient::describe_streams_groups)
/// (Kafka 4.x streams groups, KIP-1071).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamsGroupDescription {
    /// The streams group id.
    pub group_id: String,
    /// The broker-reported group state name.
    pub state: String,
    /// The group epoch.
    pub group_epoch: i32,
    /// The group members.
    pub members: Vec<MemberDescription>,
    /// The group's coordinator broker.
    pub coordinator: Node,
}

#[cfg(test)]
mod tests {
    use kacrab_protocol::KafkaString;

    use super::*;

    #[test]
    fn resource_type_wire_round_trips() {
        for ty in [
            ResourceType::Topic,
            ResourceType::Broker,
            ResourceType::BrokerLogger,
            ResourceType::ClientMetrics,
            ResourceType::Group,
            ResourceType::Unknown,
        ] {
            assert_eq!(ResourceType::from_wire(ty.to_wire()), ty);
        }
        // Unrecognized wire values fold into Unknown.
        assert_eq!(ResourceType::from_wire(99), ResourceType::Unknown);
    }

    #[test]
    fn config_source_maps_known_and_unknown() {
        assert_eq!(ConfigSource::from_wire(1), ConfigSource::TopicConfig);
        assert_eq!(ConfigSource::from_wire(4), ConfigSource::StaticBrokerConfig);
        assert_eq!(ConfigSource::from_wire(5), ConfigSource::DefaultConfig);
        assert_eq!(ConfigSource::from_wire(-1), ConfigSource::Unknown);
        assert_eq!(ConfigSource::from_wire(42), ConfigSource::Unknown);
    }

    #[test]
    fn new_topic_partition_count_and_configs() {
        let topic = NewTopic::new("orders", 6, 3)
            .config("retention.ms", Some("60000".to_owned()))
            .config("cleanup.policy", None);
        assert_eq!(topic.name(), "orders");
        let wire = topic.into_creatable();
        assert_eq!(wire.name.as_str(), "orders");
        assert_eq!(wire.num_partitions, 6);
        assert_eq!(wire.replication_factor, 3);
        assert!(wire.assignments.is_empty());
        assert_eq!(wire.configs.len(), 2);
        assert_eq!(wire.configs[0].name.as_str(), "retention.ms");
        assert_eq!(
            wire.configs[0].value.as_ref().map(KafkaString::as_str),
            Some("60000")
        );
        assert_eq!(wire.configs[1].value, None);
    }

    #[test]
    fn new_topic_with_replica_assignments_sends_negative_counts() {
        let topic =
            NewTopic::with_replica_assignments("orders", vec![(0, vec![1, 2]), (1, vec![2, 3])]);
        let wire = topic.into_creatable();
        assert_eq!(wire.num_partitions, -1);
        assert_eq!(wire.replication_factor, -1);
        assert_eq!(wire.assignments.len(), 2);
        assert_eq!(wire.assignments[0].partition_index, 0);
        assert_eq!(wire.assignments[0].broker_ids, vec![1, 2]);
    }

    #[test]
    fn new_partitions_increase_and_assign() {
        let plain = NewPartitions::increase_to("orders", 8).into_topic();
        assert_eq!(plain.name.as_str(), "orders");
        assert_eq!(plain.count, 8);
        assert!(plain.assignments.is_none());

        let assigned = NewPartitions::increase_to("orders", 8)
            .assigning(vec![vec![1, 2], vec![3, 4]])
            .into_topic();
        let assignments = assigned.assignments.expect("assignments present");
        assert_eq!(assignments.len(), 2);
        assert_eq!(assignments[1].broker_ids, vec![3, 4]);
    }

    #[test]
    fn config_resource_builders_set_type_and_name() {
        let topic = ConfigResource::topic("orders");
        assert_eq!(topic.resource_type, ResourceType::Topic);
        assert_eq!(topic.name, "orders");

        let broker = ConfigResource::broker(7);
        assert_eq!(broker.resource_type, ResourceType::Broker);
        assert_eq!(broker.name, "7");

        let describe = broker.to_describe();
        assert_eq!(describe.resource_type, ResourceType::Broker.to_wire());
        assert_eq!(describe.resource_name.as_str(), "7");
        assert!(describe.configuration_keys.is_none());
    }

    #[test]
    fn alter_config_op_type_wire_values_match_kafka() {
        assert_eq!(AlterConfigOpType::Set.to_wire(), 0);
        assert_eq!(AlterConfigOpType::Delete.to_wire(), 1);
        assert_eq!(AlterConfigOpType::Append.to_wire(), 2);
        assert_eq!(AlterConfigOpType::Subtract.to_wire(), 3);
    }

    #[test]
    fn election_and_offset_spec_wire_values_match_kafka() {
        assert_eq!(ElectionType::Preferred.to_wire(), 0);
        assert_eq!(ElectionType::Unclean.to_wire(), 1);
        assert_eq!(OffsetSpec::Earliest.to_wire(), -2);
        assert_eq!(OffsetSpec::Latest.to_wire(), -1);
        assert_eq!(OffsetSpec::MaxTimestamp.to_wire(), -3);
        assert_eq!(OffsetSpec::Timestamp(1234).to_wire(), 1234);
    }

    #[test]
    fn config_resource_to_incremental_maps_ops() {
        let resource = ConfigResource::topic("orders");
        let incremental = resource.to_incremental(vec![
            AlterConfigOp::set("retention.ms", "60000"),
            AlterConfigOp::delete("cleanup.policy"),
            AlterConfigOp::append("follower.replication.throttled.replicas", "1:2"),
        ]);
        assert_eq!(incremental.resource_name.as_str(), "orders");
        assert_eq!(incremental.configs.len(), 3);
        assert_eq!(incremental.configs[0].config_operation, 0);
        assert_eq!(
            incremental.configs[0]
                .value
                .as_ref()
                .map(KafkaString::as_str),
            Some("60000")
        );
        assert_eq!(incremental.configs[1].config_operation, 1);
        assert_eq!(incremental.configs[1].value, None);
        assert_eq!(incremental.configs[2].config_operation, 2);
    }

    #[test]
    fn config_resource_to_alter_carries_entries() {
        let resource = ConfigResource::topic("orders");
        let alter = resource.to_alter(vec![
            ConfigEntry::set("retention.ms", Some("60000".to_owned())),
            ConfigEntry::set("cleanup.policy", None),
        ]);
        assert_eq!(alter.resource_name.as_str(), "orders");
        assert_eq!(alter.configs.len(), 2);
        assert_eq!(alter.configs[0].name.as_str(), "retention.ms");
        assert_eq!(
            alter.configs[0].value.as_ref().map(KafkaString::as_str),
            Some("60000")
        );
        assert_eq!(alter.configs[1].value, None);
    }

    #[test]
    fn acl_enums_round_trip_through_wire_values() {
        for ty in [
            AclResourceType::Any,
            AclResourceType::Topic,
            AclResourceType::Group,
            AclResourceType::Cluster,
            AclResourceType::TransactionalId,
            AclResourceType::DelegationToken,
            AclResourceType::User,
            AclResourceType::Unknown,
        ] {
            assert_eq!(AclResourceType::from_wire(ty.to_wire()), ty);
        }
        assert_eq!(AclResourceType::from_wire(99), AclResourceType::Unknown);

        for pattern in [
            AclPatternType::Any,
            AclPatternType::Match,
            AclPatternType::Literal,
            AclPatternType::Prefixed,
            AclPatternType::Unknown,
        ] {
            assert_eq!(AclPatternType::from_wire(pattern.to_wire()), pattern);
        }

        for permission in [
            AclPermissionType::Any,
            AclPermissionType::Deny,
            AclPermissionType::Allow,
            AclPermissionType::Unknown,
        ] {
            assert_eq!(
                AclPermissionType::from_wire(permission.to_wire()),
                permission
            );
        }

        // Spot-check a few operation wire constants and the full round-trip.
        assert_eq!(AclOperation::Read.to_wire(), 3);
        assert_eq!(AclOperation::DescribeTokens.to_wire(), 14);
        for op in [
            AclOperation::All,
            AclOperation::Read,
            AclOperation::Write,
            AclOperation::Create,
            AclOperation::Delete,
            AclOperation::Alter,
            AclOperation::Describe,
            AclOperation::IdempotentWrite,
            AclOperation::Unknown,
        ] {
            assert_eq!(AclOperation::from_wire(op.to_wire()), op);
        }
    }

    #[test]
    fn acl_binding_filter_any_matches_everything() {
        let filter = AclBindingFilter::any();
        assert_eq!(filter.resource_type, AclResourceType::Any);
        assert_eq!(filter.pattern_type, AclPatternType::Any);
        assert_eq!(filter.operation, AclOperation::Any);
        assert_eq!(filter.permission_type, AclPermissionType::Any);
        assert_eq!(filter.resource_name, None);
        assert_eq!(filter.principal, None);
        assert_eq!(filter.host, None);
    }

    #[test]
    fn member_to_remove_constructors_set_one_identifier() {
        let static_member = MemberToRemove::static_member("instance-1");
        assert_eq!(
            static_member.group_instance_id.as_deref(),
            Some("instance-1")
        );
        assert_eq!(static_member.member_id, None);

        let dynamic_member = MemberToRemove::dynamic_member("member-1");
        assert_eq!(dynamic_member.member_id.as_deref(), Some("member-1"));
        assert_eq!(dynamic_member.group_instance_id, None);
    }

    #[test]
    fn scram_mechanism_round_trips_and_feature_upgrade_maps() {
        for mechanism in [
            ScramMechanism::ScramSha256,
            ScramMechanism::ScramSha512,
            ScramMechanism::Unknown,
        ] {
            assert_eq!(ScramMechanism::from_wire(mechanism.to_wire()), mechanism);
        }
        assert_eq!(ScramMechanism::from_wire(9), ScramMechanism::Unknown);

        assert_eq!(FeatureUpdateUpgradeType::Upgrade.to_wire(), 1);
        assert!(!FeatureUpdateUpgradeType::Upgrade.allows_downgrade());
        assert_eq!(FeatureUpdateUpgradeType::SafeDowngrade.to_wire(), 2);
        assert!(FeatureUpdateUpgradeType::SafeDowngrade.allows_downgrade());
        assert_eq!(FeatureUpdateUpgradeType::UnsafeDowngrade.to_wire(), 3);
        assert!(FeatureUpdateUpgradeType::UnsafeDowngrade.allows_downgrade());
    }
}
