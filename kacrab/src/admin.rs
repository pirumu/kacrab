//! Admin API for managing topics, partitions, and configs.
//!
//! The admin client issues management requests over the wire/session layer:
//! controller-only operations (create/delete topics, create partitions) are
//! routed to the cluster controller with not-controller refresh-and-retry,
//! while describe operations target any live broker.

mod client;
mod error;
mod types;

pub use self::{
    client::AdminClient,
    error::{AdminError, Result},
    types::{
        AbortTransactionSpec, AclBinding, AclBindingFilter, AclOperation, AclPatternType,
        AclPermissionType, AclResourceType, AlterConfigOp, AlterConfigOpType, AlterConfigsOptions,
        BrokerLogDirs, ClientQuotaAlteration, ClientQuotaEntity, ClientQuotaEntry,
        ClientQuotaFilterComponent, ClientQuotaMatch, ClientQuotaOp, ClusterDescription,
        ConfigEntry, ConfigResource, ConfigSource, ConsumerGroupDescription, ConsumerGroupListing,
        CreateDelegationTokenOptions, CreatePartitionsOptions, CreateTopicsOptions,
        DelegationToken, DeletedRecords, DescribeConsumerGroupsOptions, DescribeTopicsOptions,
        ElectionType, FeatureMetadata, FeatureUpdate, FeatureUpdateUpgradeType, FencedProducer,
        FinalizedVersionRange, GroupOffset, GroupState, GroupType, ListConsumerGroupOffsetsOptions,
        ListConsumerGroupsOptions, ListOffsetsResult, ListTopicsOptions, ListTransactionsOptions,
        LogDirDescription, LogDirReplicaInfo, MemberDescription, MemberToRemove,
        NewPartitionReassignment, NewPartitions, NewTopic, Node, OffsetSpec,
        PartitionProducerState, PartitionReassignment, ProducerState, QuorumInfo,
        QuorumReplicaState, RaftVoterEndpoint, ReplicaLogDirAssignment, ReplicaLogDirInfo,
        ResourceConfig, ResourceType, ScramCredentialDeletion, ScramCredentialInfo,
        ScramCredentialUpsertion, ScramMechanism, ShareGroupDescription, StreamsGroupDescription,
        SupportedVersionRange, TopicDescription, TopicListing, TopicPartitionInfo,
        TransactionDescription, TransactionListing, UserScramCredentials,
    },
};
// Shared `org.apache.kafka.common` domain types the admin API accepts/returns.
pub use crate::common::{ConsumerGroupMetadata, OffsetAndMetadata, TopicPartition};
