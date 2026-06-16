//! Generated Kafka protocol message types — DO NOT EDIT
#![allow(
    missing_docs,
    unused_imports,
    ambiguous_glob_reexports,
    clippy::all,
    clippy::pedantic,
    clippy::nursery,
    clippy::arithmetic_side_effects,
    reason = "Generated protocol modules mirror Kafka's schema shape and intentionally trade \
              hand-written lint style for reproducible wire-code output."
)]
pub const KAFKA_PROTOCOL_SOURCE_REF: &str = "apache/kafka@4.3.0";
pub mod error_code;
pub use error_code::ErrorCode;
pub mod add_offsets_to_txn_request;
pub mod add_offsets_to_txn_response;
pub mod add_partitions_to_txn_request;
pub mod add_partitions_to_txn_response;
pub mod add_raft_voter_request;
pub mod add_raft_voter_response;
pub mod allocate_producer_ids_request;
pub mod allocate_producer_ids_response;
pub mod alter_client_quotas_request;
pub mod alter_client_quotas_response;
pub mod alter_configs_request;
pub mod alter_configs_response;
pub mod alter_partition_reassignments_request;
pub mod alter_partition_reassignments_response;
pub mod alter_partition_request;
pub mod alter_partition_response;
pub mod alter_replica_log_dirs_request;
pub mod alter_replica_log_dirs_response;
pub mod alter_share_group_offsets_request;
pub mod alter_share_group_offsets_response;
pub mod alter_user_scram_credentials_request;
pub mod alter_user_scram_credentials_response;
pub mod api_versions_request;
pub mod api_versions_response;
pub mod assign_replicas_to_dirs_request;
pub mod assign_replicas_to_dirs_response;
pub mod begin_quorum_epoch_request;
pub mod begin_quorum_epoch_response;
pub mod broker_heartbeat_request;
pub mod broker_heartbeat_response;
pub mod broker_registration_request;
pub mod broker_registration_response;
pub mod consumer_group_describe_request;
pub mod consumer_group_describe_response;
pub mod consumer_group_heartbeat_request;
pub mod consumer_group_heartbeat_response;
pub mod consumer_protocol_assignment;
pub mod consumer_protocol_subscription;
pub mod control_record_type_schema;
pub mod controlled_shutdown_request;
pub mod controlled_shutdown_response;
pub mod controller_registration_request;
pub mod controller_registration_response;
pub mod create_acls_request;
pub mod create_acls_response;
pub mod create_delegation_token_request;
pub mod create_delegation_token_response;
pub mod create_partitions_request;
pub mod create_partitions_response;
pub mod create_topics_request;
pub mod create_topics_response;
pub mod default_principal_data;
pub mod delete_acls_request;
pub mod delete_acls_response;
pub mod delete_groups_request;
pub mod delete_groups_response;
pub mod delete_records_request;
pub mod delete_records_response;
pub mod delete_share_group_offsets_request;
pub mod delete_share_group_offsets_response;
pub mod delete_share_group_state_request;
pub mod delete_share_group_state_response;
pub mod delete_topics_request;
pub mod delete_topics_response;
pub mod describe_acls_request;
pub mod describe_acls_response;
pub mod describe_client_quotas_request;
pub mod describe_client_quotas_response;
pub mod describe_cluster_request;
pub mod describe_cluster_response;
pub mod describe_configs_request;
pub mod describe_configs_response;
pub mod describe_delegation_token_request;
pub mod describe_delegation_token_response;
pub mod describe_groups_request;
pub mod describe_groups_response;
pub mod describe_log_dirs_request;
pub mod describe_log_dirs_response;
pub mod describe_producers_request;
pub mod describe_producers_response;
pub mod describe_quorum_request;
pub mod describe_quorum_response;
pub mod describe_share_group_offsets_request;
pub mod describe_share_group_offsets_response;
pub mod describe_topic_partitions_request;
pub mod describe_topic_partitions_response;
pub mod describe_transactions_request;
pub mod describe_transactions_response;
pub mod describe_user_scram_credentials_request;
pub mod describe_user_scram_credentials_response;
pub mod elect_leaders_request;
pub mod elect_leaders_response;
pub mod end_quorum_epoch_request;
pub mod end_quorum_epoch_response;
pub mod end_txn_marker;
pub mod end_txn_request;
pub mod end_txn_response;
pub mod envelope_request;
pub mod envelope_response;
pub mod expire_delegation_token_request;
pub mod expire_delegation_token_response;
pub mod fetch_request;
pub mod fetch_response;
pub mod fetch_snapshot_request;
pub mod fetch_snapshot_response;
pub mod find_coordinator_request;
pub mod find_coordinator_response;
pub mod get_telemetry_subscriptions_request;
pub mod get_telemetry_subscriptions_response;
pub mod heartbeat_request;
pub mod heartbeat_response;
pub mod incremental_alter_configs_request;
pub mod incremental_alter_configs_response;
pub mod init_producer_id_request;
pub mod init_producer_id_response;
pub mod initialize_share_group_state_request;
pub mod initialize_share_group_state_response;
pub mod join_group_request;
pub mod join_group_response;
pub mod k_raft_version_record;
pub mod leader_and_isr_request;
pub mod leader_and_isr_response;
pub mod leader_change_message;
pub mod leave_group_request;
pub mod leave_group_response;
pub mod list_config_resources_request;
pub mod list_config_resources_response;
pub mod list_groups_request;
pub mod list_groups_response;
pub mod list_offsets_request;
pub mod list_offsets_response;
pub mod list_partition_reassignments_request;
pub mod list_partition_reassignments_response;
pub mod list_transactions_request;
pub mod list_transactions_response;
pub mod metadata_request;
pub mod metadata_response;
pub mod offset_commit_request;
pub mod offset_commit_response;
pub mod offset_delete_request;
pub mod offset_delete_response;
pub mod offset_fetch_request;
pub mod offset_fetch_response;
pub mod offset_for_leader_epoch_request;
pub mod offset_for_leader_epoch_response;
pub mod produce_request;
pub mod produce_response;
pub mod push_telemetry_request;
pub mod push_telemetry_response;
pub mod read_share_group_state_request;
pub mod read_share_group_state_response;
pub mod read_share_group_state_summary_request;
pub mod read_share_group_state_summary_response;
pub mod remove_raft_voter_request;
pub mod remove_raft_voter_response;
pub mod renew_delegation_token_request;
pub mod renew_delegation_token_response;
pub mod request_header;
pub mod response_header;
pub mod sasl_authenticate_request;
pub mod sasl_authenticate_response;
pub mod sasl_handshake_request;
pub mod sasl_handshake_response;
pub mod share_acknowledge_request;
pub mod share_acknowledge_response;
pub mod share_fetch_request;
pub mod share_fetch_response;
pub mod share_group_describe_request;
pub mod share_group_describe_response;
pub mod share_group_heartbeat_request;
pub mod share_group_heartbeat_response;
pub mod snapshot_footer_record;
pub mod snapshot_header_record;
pub mod stop_replica_request;
pub mod stop_replica_response;
pub mod streams_group_describe_request;
pub mod streams_group_describe_response;
pub mod streams_group_heartbeat_request;
pub mod streams_group_heartbeat_response;
pub mod sync_group_request;
pub mod sync_group_response;
pub mod txn_offset_commit_request;
pub mod txn_offset_commit_response;
pub mod unregister_broker_request;
pub mod unregister_broker_response;
pub mod update_features_request;
pub mod update_features_response;
pub mod update_metadata_request;
pub mod update_metadata_response;
pub mod update_raft_voter_request;
pub mod update_raft_voter_response;
pub mod vote_request;
pub mod vote_response;
pub mod voters_record;
pub mod write_share_group_state_request;
pub mod write_share_group_state_response;
pub mod write_txn_markers_request;
pub mod write_txn_markers_response;
pub use add_offsets_to_txn_request::*;
pub use add_offsets_to_txn_response::*;
pub use add_partitions_to_txn_request::*;
pub use add_partitions_to_txn_response::*;
pub use add_raft_voter_request::*;
pub use add_raft_voter_response::*;
pub use allocate_producer_ids_request::*;
pub use allocate_producer_ids_response::*;
pub use alter_client_quotas_request::*;
pub use alter_client_quotas_response::*;
pub use alter_configs_request::*;
pub use alter_configs_response::*;
pub use alter_partition_reassignments_request::*;
pub use alter_partition_reassignments_response::*;
pub use alter_partition_request::*;
pub use alter_partition_response::*;
pub use alter_replica_log_dirs_request::*;
pub use alter_replica_log_dirs_response::*;
pub use alter_share_group_offsets_request::*;
pub use alter_share_group_offsets_response::*;
pub use alter_user_scram_credentials_request::*;
pub use alter_user_scram_credentials_response::*;
pub use api_versions_request::*;
pub use api_versions_response::*;
pub use assign_replicas_to_dirs_request::*;
pub use assign_replicas_to_dirs_response::*;
pub use begin_quorum_epoch_request::*;
pub use begin_quorum_epoch_response::*;
pub use broker_heartbeat_request::*;
pub use broker_heartbeat_response::*;
pub use broker_registration_request::*;
pub use broker_registration_response::*;
pub use consumer_group_describe_request::*;
pub use consumer_group_describe_response::*;
pub use consumer_group_heartbeat_request::*;
pub use consumer_group_heartbeat_response::*;
pub use consumer_protocol_assignment::*;
pub use consumer_protocol_subscription::*;
pub use control_record_type_schema::*;
pub use controlled_shutdown_request::*;
pub use controlled_shutdown_response::*;
pub use controller_registration_request::*;
pub use controller_registration_response::*;
pub use create_acls_request::*;
pub use create_acls_response::*;
pub use create_delegation_token_request::*;
pub use create_delegation_token_response::*;
pub use create_partitions_request::*;
pub use create_partitions_response::*;
pub use create_topics_request::*;
pub use create_topics_response::*;
pub use default_principal_data::*;
pub use delete_acls_request::*;
pub use delete_acls_response::*;
pub use delete_groups_request::*;
pub use delete_groups_response::*;
pub use delete_records_request::*;
pub use delete_records_response::*;
pub use delete_share_group_offsets_request::*;
pub use delete_share_group_offsets_response::*;
pub use delete_share_group_state_request::*;
pub use delete_share_group_state_response::*;
pub use delete_topics_request::*;
pub use delete_topics_response::*;
pub use describe_acls_request::*;
pub use describe_acls_response::*;
pub use describe_client_quotas_request::*;
pub use describe_client_quotas_response::*;
pub use describe_cluster_request::*;
pub use describe_cluster_response::*;
pub use describe_configs_request::*;
pub use describe_configs_response::*;
pub use describe_delegation_token_request::*;
pub use describe_delegation_token_response::*;
pub use describe_groups_request::*;
pub use describe_groups_response::*;
pub use describe_log_dirs_request::*;
pub use describe_log_dirs_response::*;
pub use describe_producers_request::*;
pub use describe_producers_response::*;
pub use describe_quorum_request::*;
pub use describe_quorum_response::*;
pub use describe_share_group_offsets_request::*;
pub use describe_share_group_offsets_response::*;
pub use describe_topic_partitions_request::*;
pub use describe_topic_partitions_response::*;
pub use describe_transactions_request::*;
pub use describe_transactions_response::*;
pub use describe_user_scram_credentials_request::*;
pub use describe_user_scram_credentials_response::*;
pub use elect_leaders_request::*;
pub use elect_leaders_response::*;
pub use end_quorum_epoch_request::*;
pub use end_quorum_epoch_response::*;
pub use end_txn_marker::*;
pub use end_txn_request::*;
pub use end_txn_response::*;
pub use envelope_request::*;
pub use envelope_response::*;
pub use expire_delegation_token_request::*;
pub use expire_delegation_token_response::*;
pub use fetch_request::*;
pub use fetch_response::*;
pub use fetch_snapshot_request::*;
pub use fetch_snapshot_response::*;
pub use find_coordinator_request::*;
pub use find_coordinator_response::*;
pub use get_telemetry_subscriptions_request::*;
pub use get_telemetry_subscriptions_response::*;
pub use heartbeat_request::*;
pub use heartbeat_response::*;
pub use incremental_alter_configs_request::*;
pub use incremental_alter_configs_response::*;
pub use init_producer_id_request::*;
pub use init_producer_id_response::*;
pub use initialize_share_group_state_request::*;
pub use initialize_share_group_state_response::*;
pub use join_group_request::*;
pub use join_group_response::*;
pub use k_raft_version_record::*;
pub use leader_and_isr_request::*;
pub use leader_and_isr_response::*;
pub use leader_change_message::*;
pub use leave_group_request::*;
pub use leave_group_response::*;
pub use list_config_resources_request::*;
pub use list_config_resources_response::*;
pub use list_groups_request::*;
pub use list_groups_response::*;
pub use list_offsets_request::*;
pub use list_offsets_response::*;
pub use list_partition_reassignments_request::*;
pub use list_partition_reassignments_response::*;
pub use list_transactions_request::*;
pub use list_transactions_response::*;
pub use metadata_request::*;
pub use metadata_response::*;
pub use offset_commit_request::*;
pub use offset_commit_response::*;
pub use offset_delete_request::*;
pub use offset_delete_response::*;
pub use offset_fetch_request::*;
pub use offset_fetch_response::*;
pub use offset_for_leader_epoch_request::*;
pub use offset_for_leader_epoch_response::*;
pub use produce_request::*;
pub use produce_response::*;
pub use push_telemetry_request::*;
pub use push_telemetry_response::*;
pub use read_share_group_state_request::*;
pub use read_share_group_state_response::*;
pub use read_share_group_state_summary_request::*;
pub use read_share_group_state_summary_response::*;
pub use remove_raft_voter_request::*;
pub use remove_raft_voter_response::*;
pub use renew_delegation_token_request::*;
pub use renew_delegation_token_response::*;
pub use request_header::*;
pub use response_header::*;
pub use sasl_authenticate_request::*;
pub use sasl_authenticate_response::*;
pub use sasl_handshake_request::*;
pub use sasl_handshake_response::*;
pub use share_acknowledge_request::*;
pub use share_acknowledge_response::*;
pub use share_fetch_request::*;
pub use share_fetch_response::*;
pub use share_group_describe_request::*;
pub use share_group_describe_response::*;
pub use share_group_heartbeat_request::*;
pub use share_group_heartbeat_response::*;
pub use snapshot_footer_record::*;
pub use snapshot_header_record::*;
pub use stop_replica_request::*;
pub use stop_replica_response::*;
pub use streams_group_describe_request::*;
pub use streams_group_describe_response::*;
pub use streams_group_heartbeat_request::*;
pub use streams_group_heartbeat_response::*;
pub use sync_group_request::*;
pub use sync_group_response::*;
pub use txn_offset_commit_request::*;
pub use txn_offset_commit_response::*;
pub use unregister_broker_request::*;
pub use unregister_broker_response::*;
pub use update_features_request::*;
pub use update_features_response::*;
pub use update_metadata_request::*;
pub use update_metadata_response::*;
pub use update_raft_voter_request::*;
pub use update_raft_voter_response::*;
pub use vote_request::*;
pub use vote_response::*;
pub use voters_record::*;
pub use write_share_group_state_request::*;
pub use write_share_group_state_response::*;
pub use write_txn_markers_request::*;
pub use write_txn_markers_response::*;
/// Kafka API keys for request dispatch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i16)]
pub enum ApiKey {
    Produce = 0,
    Fetch = 1,
    ListOffsets = 2,
    Metadata = 3,
    LeaderAndIsr = 4,
    StopReplica = 5,
    UpdateMetadata = 6,
    ControlledShutdown = 7,
    OffsetCommit = 8,
    OffsetFetch = 9,
    FindCoordinator = 10,
    JoinGroup = 11,
    Heartbeat = 12,
    LeaveGroup = 13,
    SyncGroup = 14,
    DescribeGroups = 15,
    ListGroups = 16,
    SaslHandshake = 17,
    ApiVersions = 18,
    CreateTopics = 19,
    DeleteTopics = 20,
    DeleteRecords = 21,
    InitProducerId = 22,
    OffsetForLeaderEpoch = 23,
    AddPartitionsToTxn = 24,
    AddOffsetsToTxn = 25,
    EndTxn = 26,
    WriteTxnMarkers = 27,
    TxnOffsetCommit = 28,
    DescribeAcls = 29,
    CreateAcls = 30,
    DeleteAcls = 31,
    DescribeConfigs = 32,
    AlterConfigs = 33,
    AlterReplicaLogDirs = 34,
    DescribeLogDirs = 35,
    SaslAuthenticate = 36,
    CreatePartitions = 37,
    CreateDelegationToken = 38,
    RenewDelegationToken = 39,
    ExpireDelegationToken = 40,
    DescribeDelegationToken = 41,
    DeleteGroups = 42,
    ElectLeaders = 43,
    IncrementalAlterConfigs = 44,
    AlterPartitionReassignments = 45,
    ListPartitionReassignments = 46,
    OffsetDelete = 47,
    DescribeClientQuotas = 48,
    AlterClientQuotas = 49,
    DescribeUserScramCredentials = 50,
    AlterUserScramCredentials = 51,
    Vote = 52,
    BeginQuorumEpoch = 53,
    EndQuorumEpoch = 54,
    DescribeQuorum = 55,
    AlterPartition = 56,
    UpdateFeatures = 57,
    Envelope = 58,
    FetchSnapshot = 59,
    DescribeCluster = 60,
    DescribeProducers = 61,
    BrokerRegistration = 62,
    BrokerHeartbeat = 63,
    UnregisterBroker = 64,
    DescribeTransactions = 65,
    ListTransactions = 66,
    AllocateProducerIds = 67,
    ConsumerGroupHeartbeat = 68,
    ConsumerGroupDescribe = 69,
    ControllerRegistration = 70,
    GetTelemetrySubscriptions = 71,
    PushTelemetry = 72,
    AssignReplicasToDirs = 73,
    ListConfigResources = 74,
    DescribeTopicPartitions = 75,
    ShareGroupHeartbeat = 76,
    ShareGroupDescribe = 77,
    ShareFetch = 78,
    ShareAcknowledge = 79,
    AddRaftVoter = 80,
    RemoveRaftVoter = 81,
    UpdateRaftVoter = 82,
    InitializeShareGroupState = 83,
    ReadShareGroupState = 84,
    WriteShareGroupState = 85,
    DeleteShareGroupState = 86,
    ReadShareGroupStateSummary = 87,
    StreamsGroupHeartbeat = 88,
    StreamsGroupDescribe = 89,
    DescribeShareGroupOffsets = 90,
    AlterShareGroupOffsets = 91,
    DeleteShareGroupOffsets = 92,
}
impl ApiKey {
    pub fn from_i16(value: i16) -> Option<Self> {
        match value {
            0 => Some(ApiKey::Produce),
            1 => Some(ApiKey::Fetch),
            2 => Some(ApiKey::ListOffsets),
            3 => Some(ApiKey::Metadata),
            4 => Some(ApiKey::LeaderAndIsr),
            5 => Some(ApiKey::StopReplica),
            6 => Some(ApiKey::UpdateMetadata),
            7 => Some(ApiKey::ControlledShutdown),
            8 => Some(ApiKey::OffsetCommit),
            9 => Some(ApiKey::OffsetFetch),
            10 => Some(ApiKey::FindCoordinator),
            11 => Some(ApiKey::JoinGroup),
            12 => Some(ApiKey::Heartbeat),
            13 => Some(ApiKey::LeaveGroup),
            14 => Some(ApiKey::SyncGroup),
            15 => Some(ApiKey::DescribeGroups),
            16 => Some(ApiKey::ListGroups),
            17 => Some(ApiKey::SaslHandshake),
            18 => Some(ApiKey::ApiVersions),
            19 => Some(ApiKey::CreateTopics),
            20 => Some(ApiKey::DeleteTopics),
            21 => Some(ApiKey::DeleteRecords),
            22 => Some(ApiKey::InitProducerId),
            23 => Some(ApiKey::OffsetForLeaderEpoch),
            24 => Some(ApiKey::AddPartitionsToTxn),
            25 => Some(ApiKey::AddOffsetsToTxn),
            26 => Some(ApiKey::EndTxn),
            27 => Some(ApiKey::WriteTxnMarkers),
            28 => Some(ApiKey::TxnOffsetCommit),
            29 => Some(ApiKey::DescribeAcls),
            30 => Some(ApiKey::CreateAcls),
            31 => Some(ApiKey::DeleteAcls),
            32 => Some(ApiKey::DescribeConfigs),
            33 => Some(ApiKey::AlterConfigs),
            34 => Some(ApiKey::AlterReplicaLogDirs),
            35 => Some(ApiKey::DescribeLogDirs),
            36 => Some(ApiKey::SaslAuthenticate),
            37 => Some(ApiKey::CreatePartitions),
            38 => Some(ApiKey::CreateDelegationToken),
            39 => Some(ApiKey::RenewDelegationToken),
            40 => Some(ApiKey::ExpireDelegationToken),
            41 => Some(ApiKey::DescribeDelegationToken),
            42 => Some(ApiKey::DeleteGroups),
            43 => Some(ApiKey::ElectLeaders),
            44 => Some(ApiKey::IncrementalAlterConfigs),
            45 => Some(ApiKey::AlterPartitionReassignments),
            46 => Some(ApiKey::ListPartitionReassignments),
            47 => Some(ApiKey::OffsetDelete),
            48 => Some(ApiKey::DescribeClientQuotas),
            49 => Some(ApiKey::AlterClientQuotas),
            50 => Some(ApiKey::DescribeUserScramCredentials),
            51 => Some(ApiKey::AlterUserScramCredentials),
            52 => Some(ApiKey::Vote),
            53 => Some(ApiKey::BeginQuorumEpoch),
            54 => Some(ApiKey::EndQuorumEpoch),
            55 => Some(ApiKey::DescribeQuorum),
            56 => Some(ApiKey::AlterPartition),
            57 => Some(ApiKey::UpdateFeatures),
            58 => Some(ApiKey::Envelope),
            59 => Some(ApiKey::FetchSnapshot),
            60 => Some(ApiKey::DescribeCluster),
            61 => Some(ApiKey::DescribeProducers),
            62 => Some(ApiKey::BrokerRegistration),
            63 => Some(ApiKey::BrokerHeartbeat),
            64 => Some(ApiKey::UnregisterBroker),
            65 => Some(ApiKey::DescribeTransactions),
            66 => Some(ApiKey::ListTransactions),
            67 => Some(ApiKey::AllocateProducerIds),
            68 => Some(ApiKey::ConsumerGroupHeartbeat),
            69 => Some(ApiKey::ConsumerGroupDescribe),
            70 => Some(ApiKey::ControllerRegistration),
            71 => Some(ApiKey::GetTelemetrySubscriptions),
            72 => Some(ApiKey::PushTelemetry),
            73 => Some(ApiKey::AssignReplicasToDirs),
            74 => Some(ApiKey::ListConfigResources),
            75 => Some(ApiKey::DescribeTopicPartitions),
            76 => Some(ApiKey::ShareGroupHeartbeat),
            77 => Some(ApiKey::ShareGroupDescribe),
            78 => Some(ApiKey::ShareFetch),
            79 => Some(ApiKey::ShareAcknowledge),
            80 => Some(ApiKey::AddRaftVoter),
            81 => Some(ApiKey::RemoveRaftVoter),
            82 => Some(ApiKey::UpdateRaftVoter),
            83 => Some(ApiKey::InitializeShareGroupState),
            84 => Some(ApiKey::ReadShareGroupState),
            85 => Some(ApiKey::WriteShareGroupState),
            86 => Some(ApiKey::DeleteShareGroupState),
            87 => Some(ApiKey::ReadShareGroupStateSummary),
            88 => Some(ApiKey::StreamsGroupHeartbeat),
            89 => Some(ApiKey::StreamsGroupDescribe),
            90 => Some(ApiKey::DescribeShareGroupOffsets),
            91 => Some(ApiKey::AlterShareGroupOffsets),
            92 => Some(ApiKey::DeleteShareGroupOffsets),
            _ => None,
        }
    }
}
/// Static metadata about a Kafka API: supported version range and flexible-encoding threshold.
///
/// `flexible_versions_start` is the first message version that uses flexible encoding.
/// Use `i16::MAX` to indicate the API is never flexible.
#[derive(Debug, Clone, Copy)]
pub struct ApiInfo {
    pub min_version: i16,
    pub max_version: i16,
    pub flexible_versions_start: i16,
}
/// Returns the client-side [`ApiInfo`] for the given [`ApiKey`].
///
/// The data is derived from the Kafka protocol JSON spec files at code-generation time.
pub fn client_api_info(api_key: ApiKey) -> ApiInfo {
    match api_key {
        ApiKey::Produce => ApiInfo {
            min_version: 3,
            max_version: 13,
            flexible_versions_start: 9,
        },
        ApiKey::Fetch => ApiInfo {
            min_version: 4,
            max_version: 18,
            flexible_versions_start: 12,
        },
        ApiKey::ListOffsets => ApiInfo {
            min_version: 1,
            max_version: 11,
            flexible_versions_start: 6,
        },
        ApiKey::Metadata => ApiInfo {
            min_version: 0,
            max_version: 13,
            flexible_versions_start: 9,
        },
        ApiKey::LeaderAndIsr => ApiInfo {
            min_version: 0,
            max_version: 0,
            flexible_versions_start: i16::MAX,
        },
        ApiKey::StopReplica => ApiInfo {
            min_version: 0,
            max_version: 0,
            flexible_versions_start: i16::MAX,
        },
        ApiKey::UpdateMetadata => ApiInfo {
            min_version: 0,
            max_version: 0,
            flexible_versions_start: i16::MAX,
        },
        ApiKey::ControlledShutdown => ApiInfo {
            min_version: 0,
            max_version: 0,
            flexible_versions_start: i16::MAX,
        },
        ApiKey::OffsetCommit => ApiInfo {
            min_version: 2,
            max_version: 10,
            flexible_versions_start: 8,
        },
        ApiKey::OffsetFetch => ApiInfo {
            min_version: 1,
            max_version: 10,
            flexible_versions_start: 6,
        },
        ApiKey::FindCoordinator => ApiInfo {
            min_version: 0,
            max_version: 6,
            flexible_versions_start: 3,
        },
        ApiKey::JoinGroup => ApiInfo {
            min_version: 0,
            max_version: 9,
            flexible_versions_start: 6,
        },
        ApiKey::Heartbeat => ApiInfo {
            min_version: 0,
            max_version: 4,
            flexible_versions_start: 4,
        },
        ApiKey::LeaveGroup => ApiInfo {
            min_version: 0,
            max_version: 5,
            flexible_versions_start: 4,
        },
        ApiKey::SyncGroup => ApiInfo {
            min_version: 0,
            max_version: 5,
            flexible_versions_start: 4,
        },
        ApiKey::DescribeGroups => ApiInfo {
            min_version: 0,
            max_version: 6,
            flexible_versions_start: 5,
        },
        ApiKey::ListGroups => ApiInfo {
            min_version: 0,
            max_version: 5,
            flexible_versions_start: 3,
        },
        ApiKey::SaslHandshake => ApiInfo {
            min_version: 0,
            max_version: 1,
            flexible_versions_start: i16::MAX,
        },
        ApiKey::ApiVersions => ApiInfo {
            min_version: 0,
            max_version: 4,
            flexible_versions_start: 3,
        },
        ApiKey::CreateTopics => ApiInfo {
            min_version: 2,
            max_version: 7,
            flexible_versions_start: 5,
        },
        ApiKey::DeleteTopics => ApiInfo {
            min_version: 1,
            max_version: 6,
            flexible_versions_start: 4,
        },
        ApiKey::DeleteRecords => ApiInfo {
            min_version: 0,
            max_version: 2,
            flexible_versions_start: 2,
        },
        ApiKey::InitProducerId => ApiInfo {
            min_version: 0,
            max_version: 6,
            flexible_versions_start: 2,
        },
        ApiKey::OffsetForLeaderEpoch => ApiInfo {
            min_version: 2,
            max_version: 4,
            flexible_versions_start: 4,
        },
        ApiKey::AddPartitionsToTxn => ApiInfo {
            min_version: 0,
            max_version: 5,
            flexible_versions_start: 3,
        },
        ApiKey::AddOffsetsToTxn => ApiInfo {
            min_version: 0,
            max_version: 4,
            flexible_versions_start: 3,
        },
        ApiKey::EndTxn => ApiInfo {
            min_version: 0,
            max_version: 5,
            flexible_versions_start: 3,
        },
        ApiKey::WriteTxnMarkers => ApiInfo {
            min_version: 1,
            max_version: 2,
            flexible_versions_start: 1,
        },
        ApiKey::TxnOffsetCommit => ApiInfo {
            min_version: 0,
            max_version: 5,
            flexible_versions_start: 3,
        },
        ApiKey::DescribeAcls => ApiInfo {
            min_version: 1,
            max_version: 3,
            flexible_versions_start: 2,
        },
        ApiKey::CreateAcls => ApiInfo {
            min_version: 1,
            max_version: 3,
            flexible_versions_start: 2,
        },
        ApiKey::DeleteAcls => ApiInfo {
            min_version: 1,
            max_version: 3,
            flexible_versions_start: 2,
        },
        ApiKey::DescribeConfigs => ApiInfo {
            min_version: 1,
            max_version: 4,
            flexible_versions_start: 4,
        },
        ApiKey::AlterConfigs => ApiInfo {
            min_version: 0,
            max_version: 2,
            flexible_versions_start: 2,
        },
        ApiKey::AlterReplicaLogDirs => ApiInfo {
            min_version: 1,
            max_version: 2,
            flexible_versions_start: 2,
        },
        ApiKey::DescribeLogDirs => ApiInfo {
            min_version: 1,
            max_version: 5,
            flexible_versions_start: 2,
        },
        ApiKey::SaslAuthenticate => ApiInfo {
            min_version: 0,
            max_version: 2,
            flexible_versions_start: 2,
        },
        ApiKey::CreatePartitions => ApiInfo {
            min_version: 0,
            max_version: 3,
            flexible_versions_start: 2,
        },
        ApiKey::CreateDelegationToken => ApiInfo {
            min_version: 1,
            max_version: 3,
            flexible_versions_start: 2,
        },
        ApiKey::RenewDelegationToken => ApiInfo {
            min_version: 1,
            max_version: 2,
            flexible_versions_start: 2,
        },
        ApiKey::ExpireDelegationToken => ApiInfo {
            min_version: 1,
            max_version: 2,
            flexible_versions_start: 2,
        },
        ApiKey::DescribeDelegationToken => ApiInfo {
            min_version: 1,
            max_version: 3,
            flexible_versions_start: 2,
        },
        ApiKey::DeleteGroups => ApiInfo {
            min_version: 0,
            max_version: 2,
            flexible_versions_start: 2,
        },
        ApiKey::ElectLeaders => ApiInfo {
            min_version: 0,
            max_version: 2,
            flexible_versions_start: 2,
        },
        ApiKey::IncrementalAlterConfigs => ApiInfo {
            min_version: 0,
            max_version: 1,
            flexible_versions_start: 1,
        },
        ApiKey::AlterPartitionReassignments => ApiInfo {
            min_version: 0,
            max_version: 1,
            flexible_versions_start: 0,
        },
        ApiKey::ListPartitionReassignments => ApiInfo {
            min_version: 0,
            max_version: 0,
            flexible_versions_start: 0,
        },
        ApiKey::OffsetDelete => ApiInfo {
            min_version: 0,
            max_version: 0,
            flexible_versions_start: i16::MAX,
        },
        ApiKey::DescribeClientQuotas => ApiInfo {
            min_version: 0,
            max_version: 1,
            flexible_versions_start: 1,
        },
        ApiKey::AlterClientQuotas => ApiInfo {
            min_version: 0,
            max_version: 1,
            flexible_versions_start: 1,
        },
        ApiKey::DescribeUserScramCredentials => ApiInfo {
            min_version: 0,
            max_version: 0,
            flexible_versions_start: 0,
        },
        ApiKey::AlterUserScramCredentials => ApiInfo {
            min_version: 0,
            max_version: 0,
            flexible_versions_start: 0,
        },
        ApiKey::Vote => ApiInfo {
            min_version: 0,
            max_version: 2,
            flexible_versions_start: 0,
        },
        ApiKey::BeginQuorumEpoch => ApiInfo {
            min_version: 0,
            max_version: 1,
            flexible_versions_start: 1,
        },
        ApiKey::EndQuorumEpoch => ApiInfo {
            min_version: 0,
            max_version: 1,
            flexible_versions_start: 1,
        },
        ApiKey::DescribeQuorum => ApiInfo {
            min_version: 0,
            max_version: 2,
            flexible_versions_start: 0,
        },
        ApiKey::AlterPartition => ApiInfo {
            min_version: 2,
            max_version: 3,
            flexible_versions_start: 0,
        },
        ApiKey::UpdateFeatures => ApiInfo {
            min_version: 0,
            max_version: 2,
            flexible_versions_start: 0,
        },
        ApiKey::Envelope => ApiInfo {
            min_version: 0,
            max_version: 0,
            flexible_versions_start: 0,
        },
        ApiKey::FetchSnapshot => ApiInfo {
            min_version: 0,
            max_version: 1,
            flexible_versions_start: 0,
        },
        ApiKey::DescribeCluster => ApiInfo {
            min_version: 0,
            max_version: 2,
            flexible_versions_start: 0,
        },
        ApiKey::DescribeProducers => ApiInfo {
            min_version: 0,
            max_version: 0,
            flexible_versions_start: 0,
        },
        ApiKey::BrokerRegistration => ApiInfo {
            min_version: 0,
            max_version: 4,
            flexible_versions_start: 0,
        },
        ApiKey::BrokerHeartbeat => ApiInfo {
            min_version: 0,
            max_version: 2,
            flexible_versions_start: 0,
        },
        ApiKey::UnregisterBroker => ApiInfo {
            min_version: 0,
            max_version: 0,
            flexible_versions_start: 0,
        },
        ApiKey::DescribeTransactions => ApiInfo {
            min_version: 0,
            max_version: 0,
            flexible_versions_start: 0,
        },
        ApiKey::ListTransactions => ApiInfo {
            min_version: 0,
            max_version: 2,
            flexible_versions_start: 0,
        },
        ApiKey::AllocateProducerIds => ApiInfo {
            min_version: 0,
            max_version: 0,
            flexible_versions_start: 0,
        },
        ApiKey::ConsumerGroupHeartbeat => ApiInfo {
            min_version: 0,
            max_version: 1,
            flexible_versions_start: 0,
        },
        ApiKey::ConsumerGroupDescribe => ApiInfo {
            min_version: 0,
            max_version: 1,
            flexible_versions_start: 0,
        },
        ApiKey::ControllerRegistration => ApiInfo {
            min_version: 0,
            max_version: 0,
            flexible_versions_start: 0,
        },
        ApiKey::GetTelemetrySubscriptions => ApiInfo {
            min_version: 0,
            max_version: 0,
            flexible_versions_start: 0,
        },
        ApiKey::PushTelemetry => ApiInfo {
            min_version: 0,
            max_version: 0,
            flexible_versions_start: 0,
        },
        ApiKey::AssignReplicasToDirs => ApiInfo {
            min_version: 0,
            max_version: 0,
            flexible_versions_start: 0,
        },
        ApiKey::ListConfigResources => ApiInfo {
            min_version: 0,
            max_version: 1,
            flexible_versions_start: 0,
        },
        ApiKey::DescribeTopicPartitions => ApiInfo {
            min_version: 0,
            max_version: 0,
            flexible_versions_start: 0,
        },
        ApiKey::ShareGroupHeartbeat => ApiInfo {
            min_version: 1,
            max_version: 1,
            flexible_versions_start: 0,
        },
        ApiKey::ShareGroupDescribe => ApiInfo {
            min_version: 1,
            max_version: 1,
            flexible_versions_start: 0,
        },
        ApiKey::ShareFetch => ApiInfo {
            min_version: 1,
            max_version: 2,
            flexible_versions_start: 0,
        },
        ApiKey::ShareAcknowledge => ApiInfo {
            min_version: 1,
            max_version: 2,
            flexible_versions_start: 0,
        },
        ApiKey::AddRaftVoter => ApiInfo {
            min_version: 0,
            max_version: 1,
            flexible_versions_start: 0,
        },
        ApiKey::RemoveRaftVoter => ApiInfo {
            min_version: 0,
            max_version: 0,
            flexible_versions_start: 0,
        },
        ApiKey::UpdateRaftVoter => ApiInfo {
            min_version: 0,
            max_version: 0,
            flexible_versions_start: 0,
        },
        ApiKey::InitializeShareGroupState => ApiInfo {
            min_version: 0,
            max_version: 0,
            flexible_versions_start: 0,
        },
        ApiKey::ReadShareGroupState => ApiInfo {
            min_version: 0,
            max_version: 0,
            flexible_versions_start: 0,
        },
        ApiKey::WriteShareGroupState => ApiInfo {
            min_version: 0,
            max_version: 1,
            flexible_versions_start: 0,
        },
        ApiKey::DeleteShareGroupState => ApiInfo {
            min_version: 0,
            max_version: 0,
            flexible_versions_start: 0,
        },
        ApiKey::ReadShareGroupStateSummary => ApiInfo {
            min_version: 0,
            max_version: 1,
            flexible_versions_start: 0,
        },
        ApiKey::StreamsGroupHeartbeat => ApiInfo {
            min_version: 0,
            max_version: 0,
            flexible_versions_start: 0,
        },
        ApiKey::StreamsGroupDescribe => ApiInfo {
            min_version: 0,
            max_version: 0,
            flexible_versions_start: 0,
        },
        ApiKey::DescribeShareGroupOffsets => ApiInfo {
            min_version: 0,
            max_version: 1,
            flexible_versions_start: 0,
        },
        ApiKey::AlterShareGroupOffsets => ApiInfo {
            min_version: 0,
            max_version: 0,
            flexible_versions_start: 0,
        },
        ApiKey::DeleteShareGroupOffsets => ApiInfo {
            min_version: 0,
            max_version: 0,
            flexible_versions_start: 0,
        },
    }
}
#[cfg(feature = "message-enums")]
use bytes::BytesMut;

/// Type-erased generated Kafka requests.
#[cfg(feature = "message-enums")]
#[derive(Debug, Clone, PartialEq)]
pub enum RequestKind {
    Produce(ProduceRequestData),
    Fetch(FetchRequestData),
    ListOffsets(ListOffsetsRequestData),
    Metadata(MetadataRequestData),
    LeaderAndIsr(LeaderAndIsrRequestData),
    StopReplica(StopReplicaRequestData),
    UpdateMetadata(UpdateMetadataRequestData),
    ControlledShutdown(ControlledShutdownRequestData),
    OffsetCommit(OffsetCommitRequestData),
    OffsetFetch(OffsetFetchRequestData),
    FindCoordinator(FindCoordinatorRequestData),
    JoinGroup(JoinGroupRequestData),
    Heartbeat(HeartbeatRequestData),
    LeaveGroup(LeaveGroupRequestData),
    SyncGroup(SyncGroupRequestData),
    DescribeGroups(DescribeGroupsRequestData),
    ListGroups(ListGroupsRequestData),
    SaslHandshake(SaslHandshakeRequestData),
    ApiVersions(ApiVersionsRequestData),
    CreateTopics(CreateTopicsRequestData),
    DeleteTopics(DeleteTopicsRequestData),
    DeleteRecords(DeleteRecordsRequestData),
    InitProducerId(InitProducerIdRequestData),
    OffsetForLeaderEpoch(OffsetForLeaderEpochRequestData),
    AddPartitionsToTxn(AddPartitionsToTxnRequestData),
    AddOffsetsToTxn(AddOffsetsToTxnRequestData),
    EndTxn(EndTxnRequestData),
    WriteTxnMarkers(WriteTxnMarkersRequestData),
    TxnOffsetCommit(TxnOffsetCommitRequestData),
    DescribeAcls(DescribeAclsRequestData),
    CreateAcls(CreateAclsRequestData),
    DeleteAcls(DeleteAclsRequestData),
    DescribeConfigs(DescribeConfigsRequestData),
    AlterConfigs(AlterConfigsRequestData),
    AlterReplicaLogDirs(AlterReplicaLogDirsRequestData),
    DescribeLogDirs(DescribeLogDirsRequestData),
    SaslAuthenticate(SaslAuthenticateRequestData),
    CreatePartitions(CreatePartitionsRequestData),
    CreateDelegationToken(CreateDelegationTokenRequestData),
    RenewDelegationToken(RenewDelegationTokenRequestData),
    ExpireDelegationToken(ExpireDelegationTokenRequestData),
    DescribeDelegationToken(DescribeDelegationTokenRequestData),
    DeleteGroups(DeleteGroupsRequestData),
    ElectLeaders(ElectLeadersRequestData),
    IncrementalAlterConfigs(IncrementalAlterConfigsRequestData),
    AlterPartitionReassignments(AlterPartitionReassignmentsRequestData),
    ListPartitionReassignments(ListPartitionReassignmentsRequestData),
    OffsetDelete(OffsetDeleteRequestData),
    DescribeClientQuotas(DescribeClientQuotasRequestData),
    AlterClientQuotas(AlterClientQuotasRequestData),
    DescribeUserScramCredentials(DescribeUserScramCredentialsRequestData),
    AlterUserScramCredentials(AlterUserScramCredentialsRequestData),
    Vote(VoteRequestData),
    BeginQuorumEpoch(BeginQuorumEpochRequestData),
    EndQuorumEpoch(EndQuorumEpochRequestData),
    DescribeQuorum(DescribeQuorumRequestData),
    AlterPartition(AlterPartitionRequestData),
    UpdateFeatures(UpdateFeaturesRequestData),
    Envelope(EnvelopeRequestData),
    FetchSnapshot(FetchSnapshotRequestData),
    DescribeCluster(DescribeClusterRequestData),
    DescribeProducers(DescribeProducersRequestData),
    BrokerRegistration(BrokerRegistrationRequestData),
    BrokerHeartbeat(BrokerHeartbeatRequestData),
    UnregisterBroker(UnregisterBrokerRequestData),
    DescribeTransactions(DescribeTransactionsRequestData),
    ListTransactions(ListTransactionsRequestData),
    AllocateProducerIds(AllocateProducerIdsRequestData),
    ConsumerGroupHeartbeat(ConsumerGroupHeartbeatRequestData),
    ConsumerGroupDescribe(ConsumerGroupDescribeRequestData),
    ControllerRegistration(ControllerRegistrationRequestData),
    GetTelemetrySubscriptions(GetTelemetrySubscriptionsRequestData),
    PushTelemetry(PushTelemetryRequestData),
    AssignReplicasToDirs(AssignReplicasToDirsRequestData),
    ListConfigResources(ListConfigResourcesRequestData),
    DescribeTopicPartitions(DescribeTopicPartitionsRequestData),
    ShareGroupHeartbeat(ShareGroupHeartbeatRequestData),
    ShareGroupDescribe(ShareGroupDescribeRequestData),
    ShareFetch(ShareFetchRequestData),
    ShareAcknowledge(ShareAcknowledgeRequestData),
    AddRaftVoter(AddRaftVoterRequestData),
    RemoveRaftVoter(RemoveRaftVoterRequestData),
    UpdateRaftVoter(UpdateRaftVoterRequestData),
    InitializeShareGroupState(InitializeShareGroupStateRequestData),
    ReadShareGroupState(ReadShareGroupStateRequestData),
    WriteShareGroupState(WriteShareGroupStateRequestData),
    DeleteShareGroupState(DeleteShareGroupStateRequestData),
    ReadShareGroupStateSummary(ReadShareGroupStateSummaryRequestData),
    StreamsGroupHeartbeat(StreamsGroupHeartbeatRequestData),
    StreamsGroupDescribe(StreamsGroupDescribeRequestData),
    DescribeShareGroupOffsets(DescribeShareGroupOffsetsRequestData),
    AlterShareGroupOffsets(AlterShareGroupOffsetsRequestData),
    DeleteShareGroupOffsets(DeleteShareGroupOffsetsRequestData),
}
#[cfg(feature = "message-enums")]
impl RequestKind {
    pub fn api_key(&self) -> ApiKey {
        match self {
            RequestKind::Produce(_) => ApiKey::Produce,
            RequestKind::Fetch(_) => ApiKey::Fetch,
            RequestKind::ListOffsets(_) => ApiKey::ListOffsets,
            RequestKind::Metadata(_) => ApiKey::Metadata,
            RequestKind::LeaderAndIsr(_) => ApiKey::LeaderAndIsr,
            RequestKind::StopReplica(_) => ApiKey::StopReplica,
            RequestKind::UpdateMetadata(_) => ApiKey::UpdateMetadata,
            RequestKind::ControlledShutdown(_) => ApiKey::ControlledShutdown,
            RequestKind::OffsetCommit(_) => ApiKey::OffsetCommit,
            RequestKind::OffsetFetch(_) => ApiKey::OffsetFetch,
            RequestKind::FindCoordinator(_) => ApiKey::FindCoordinator,
            RequestKind::JoinGroup(_) => ApiKey::JoinGroup,
            RequestKind::Heartbeat(_) => ApiKey::Heartbeat,
            RequestKind::LeaveGroup(_) => ApiKey::LeaveGroup,
            RequestKind::SyncGroup(_) => ApiKey::SyncGroup,
            RequestKind::DescribeGroups(_) => ApiKey::DescribeGroups,
            RequestKind::ListGroups(_) => ApiKey::ListGroups,
            RequestKind::SaslHandshake(_) => ApiKey::SaslHandshake,
            RequestKind::ApiVersions(_) => ApiKey::ApiVersions,
            RequestKind::CreateTopics(_) => ApiKey::CreateTopics,
            RequestKind::DeleteTopics(_) => ApiKey::DeleteTopics,
            RequestKind::DeleteRecords(_) => ApiKey::DeleteRecords,
            RequestKind::InitProducerId(_) => ApiKey::InitProducerId,
            RequestKind::OffsetForLeaderEpoch(_) => ApiKey::OffsetForLeaderEpoch,
            RequestKind::AddPartitionsToTxn(_) => ApiKey::AddPartitionsToTxn,
            RequestKind::AddOffsetsToTxn(_) => ApiKey::AddOffsetsToTxn,
            RequestKind::EndTxn(_) => ApiKey::EndTxn,
            RequestKind::WriteTxnMarkers(_) => ApiKey::WriteTxnMarkers,
            RequestKind::TxnOffsetCommit(_) => ApiKey::TxnOffsetCommit,
            RequestKind::DescribeAcls(_) => ApiKey::DescribeAcls,
            RequestKind::CreateAcls(_) => ApiKey::CreateAcls,
            RequestKind::DeleteAcls(_) => ApiKey::DeleteAcls,
            RequestKind::DescribeConfigs(_) => ApiKey::DescribeConfigs,
            RequestKind::AlterConfigs(_) => ApiKey::AlterConfigs,
            RequestKind::AlterReplicaLogDirs(_) => ApiKey::AlterReplicaLogDirs,
            RequestKind::DescribeLogDirs(_) => ApiKey::DescribeLogDirs,
            RequestKind::SaslAuthenticate(_) => ApiKey::SaslAuthenticate,
            RequestKind::CreatePartitions(_) => ApiKey::CreatePartitions,
            RequestKind::CreateDelegationToken(_) => ApiKey::CreateDelegationToken,
            RequestKind::RenewDelegationToken(_) => ApiKey::RenewDelegationToken,
            RequestKind::ExpireDelegationToken(_) => ApiKey::ExpireDelegationToken,
            RequestKind::DescribeDelegationToken(_) => ApiKey::DescribeDelegationToken,
            RequestKind::DeleteGroups(_) => ApiKey::DeleteGroups,
            RequestKind::ElectLeaders(_) => ApiKey::ElectLeaders,
            RequestKind::IncrementalAlterConfigs(_) => ApiKey::IncrementalAlterConfigs,
            RequestKind::AlterPartitionReassignments(_) => ApiKey::AlterPartitionReassignments,
            RequestKind::ListPartitionReassignments(_) => ApiKey::ListPartitionReassignments,
            RequestKind::OffsetDelete(_) => ApiKey::OffsetDelete,
            RequestKind::DescribeClientQuotas(_) => ApiKey::DescribeClientQuotas,
            RequestKind::AlterClientQuotas(_) => ApiKey::AlterClientQuotas,
            RequestKind::DescribeUserScramCredentials(_) => ApiKey::DescribeUserScramCredentials,
            RequestKind::AlterUserScramCredentials(_) => ApiKey::AlterUserScramCredentials,
            RequestKind::Vote(_) => ApiKey::Vote,
            RequestKind::BeginQuorumEpoch(_) => ApiKey::BeginQuorumEpoch,
            RequestKind::EndQuorumEpoch(_) => ApiKey::EndQuorumEpoch,
            RequestKind::DescribeQuorum(_) => ApiKey::DescribeQuorum,
            RequestKind::AlterPartition(_) => ApiKey::AlterPartition,
            RequestKind::UpdateFeatures(_) => ApiKey::UpdateFeatures,
            RequestKind::Envelope(_) => ApiKey::Envelope,
            RequestKind::FetchSnapshot(_) => ApiKey::FetchSnapshot,
            RequestKind::DescribeCluster(_) => ApiKey::DescribeCluster,
            RequestKind::DescribeProducers(_) => ApiKey::DescribeProducers,
            RequestKind::BrokerRegistration(_) => ApiKey::BrokerRegistration,
            RequestKind::BrokerHeartbeat(_) => ApiKey::BrokerHeartbeat,
            RequestKind::UnregisterBroker(_) => ApiKey::UnregisterBroker,
            RequestKind::DescribeTransactions(_) => ApiKey::DescribeTransactions,
            RequestKind::ListTransactions(_) => ApiKey::ListTransactions,
            RequestKind::AllocateProducerIds(_) => ApiKey::AllocateProducerIds,
            RequestKind::ConsumerGroupHeartbeat(_) => ApiKey::ConsumerGroupHeartbeat,
            RequestKind::ConsumerGroupDescribe(_) => ApiKey::ConsumerGroupDescribe,
            RequestKind::ControllerRegistration(_) => ApiKey::ControllerRegistration,
            RequestKind::GetTelemetrySubscriptions(_) => ApiKey::GetTelemetrySubscriptions,
            RequestKind::PushTelemetry(_) => ApiKey::PushTelemetry,
            RequestKind::AssignReplicasToDirs(_) => ApiKey::AssignReplicasToDirs,
            RequestKind::ListConfigResources(_) => ApiKey::ListConfigResources,
            RequestKind::DescribeTopicPartitions(_) => ApiKey::DescribeTopicPartitions,
            RequestKind::ShareGroupHeartbeat(_) => ApiKey::ShareGroupHeartbeat,
            RequestKind::ShareGroupDescribe(_) => ApiKey::ShareGroupDescribe,
            RequestKind::ShareFetch(_) => ApiKey::ShareFetch,
            RequestKind::ShareAcknowledge(_) => ApiKey::ShareAcknowledge,
            RequestKind::AddRaftVoter(_) => ApiKey::AddRaftVoter,
            RequestKind::RemoveRaftVoter(_) => ApiKey::RemoveRaftVoter,
            RequestKind::UpdateRaftVoter(_) => ApiKey::UpdateRaftVoter,
            RequestKind::InitializeShareGroupState(_) => ApiKey::InitializeShareGroupState,
            RequestKind::ReadShareGroupState(_) => ApiKey::ReadShareGroupState,
            RequestKind::WriteShareGroupState(_) => ApiKey::WriteShareGroupState,
            RequestKind::DeleteShareGroupState(_) => ApiKey::DeleteShareGroupState,
            RequestKind::ReadShareGroupStateSummary(_) => ApiKey::ReadShareGroupStateSummary,
            RequestKind::StreamsGroupHeartbeat(_) => ApiKey::StreamsGroupHeartbeat,
            RequestKind::StreamsGroupDescribe(_) => ApiKey::StreamsGroupDescribe,
            RequestKind::DescribeShareGroupOffsets(_) => ApiKey::DescribeShareGroupOffsets,
            RequestKind::AlterShareGroupOffsets(_) => ApiKey::AlterShareGroupOffsets,
            RequestKind::DeleteShareGroupOffsets(_) => ApiKey::DeleteShareGroupOffsets,
        }
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> crate::Result<()> {
        match self {
            RequestKind::Produce(message) => message.write(buf, version),
            RequestKind::Fetch(message) => message.write(buf, version),
            RequestKind::ListOffsets(message) => message.write(buf, version),
            RequestKind::Metadata(message) => message.write(buf, version),
            RequestKind::LeaderAndIsr(message) => message.write(buf, version),
            RequestKind::StopReplica(message) => message.write(buf, version),
            RequestKind::UpdateMetadata(message) => message.write(buf, version),
            RequestKind::ControlledShutdown(message) => message.write(buf, version),
            RequestKind::OffsetCommit(message) => message.write(buf, version),
            RequestKind::OffsetFetch(message) => message.write(buf, version),
            RequestKind::FindCoordinator(message) => message.write(buf, version),
            RequestKind::JoinGroup(message) => message.write(buf, version),
            RequestKind::Heartbeat(message) => message.write(buf, version),
            RequestKind::LeaveGroup(message) => message.write(buf, version),
            RequestKind::SyncGroup(message) => message.write(buf, version),
            RequestKind::DescribeGroups(message) => message.write(buf, version),
            RequestKind::ListGroups(message) => message.write(buf, version),
            RequestKind::SaslHandshake(message) => message.write(buf, version),
            RequestKind::ApiVersions(message) => message.write(buf, version),
            RequestKind::CreateTopics(message) => message.write(buf, version),
            RequestKind::DeleteTopics(message) => message.write(buf, version),
            RequestKind::DeleteRecords(message) => message.write(buf, version),
            RequestKind::InitProducerId(message) => message.write(buf, version),
            RequestKind::OffsetForLeaderEpoch(message) => message.write(buf, version),
            RequestKind::AddPartitionsToTxn(message) => message.write(buf, version),
            RequestKind::AddOffsetsToTxn(message) => message.write(buf, version),
            RequestKind::EndTxn(message) => message.write(buf, version),
            RequestKind::WriteTxnMarkers(message) => message.write(buf, version),
            RequestKind::TxnOffsetCommit(message) => message.write(buf, version),
            RequestKind::DescribeAcls(message) => message.write(buf, version),
            RequestKind::CreateAcls(message) => message.write(buf, version),
            RequestKind::DeleteAcls(message) => message.write(buf, version),
            RequestKind::DescribeConfigs(message) => message.write(buf, version),
            RequestKind::AlterConfigs(message) => message.write(buf, version),
            RequestKind::AlterReplicaLogDirs(message) => message.write(buf, version),
            RequestKind::DescribeLogDirs(message) => message.write(buf, version),
            RequestKind::SaslAuthenticate(message) => message.write(buf, version),
            RequestKind::CreatePartitions(message) => message.write(buf, version),
            RequestKind::CreateDelegationToken(message) => message.write(buf, version),
            RequestKind::RenewDelegationToken(message) => message.write(buf, version),
            RequestKind::ExpireDelegationToken(message) => message.write(buf, version),
            RequestKind::DescribeDelegationToken(message) => message.write(buf, version),
            RequestKind::DeleteGroups(message) => message.write(buf, version),
            RequestKind::ElectLeaders(message) => message.write(buf, version),
            RequestKind::IncrementalAlterConfigs(message) => message.write(buf, version),
            RequestKind::AlterPartitionReassignments(message) => message.write(buf, version),
            RequestKind::ListPartitionReassignments(message) => message.write(buf, version),
            RequestKind::OffsetDelete(message) => message.write(buf, version),
            RequestKind::DescribeClientQuotas(message) => message.write(buf, version),
            RequestKind::AlterClientQuotas(message) => message.write(buf, version),
            RequestKind::DescribeUserScramCredentials(message) => message.write(buf, version),
            RequestKind::AlterUserScramCredentials(message) => message.write(buf, version),
            RequestKind::Vote(message) => message.write(buf, version),
            RequestKind::BeginQuorumEpoch(message) => message.write(buf, version),
            RequestKind::EndQuorumEpoch(message) => message.write(buf, version),
            RequestKind::DescribeQuorum(message) => message.write(buf, version),
            RequestKind::AlterPartition(message) => message.write(buf, version),
            RequestKind::UpdateFeatures(message) => message.write(buf, version),
            RequestKind::Envelope(message) => message.write(buf, version),
            RequestKind::FetchSnapshot(message) => message.write(buf, version),
            RequestKind::DescribeCluster(message) => message.write(buf, version),
            RequestKind::DescribeProducers(message) => message.write(buf, version),
            RequestKind::BrokerRegistration(message) => message.write(buf, version),
            RequestKind::BrokerHeartbeat(message) => message.write(buf, version),
            RequestKind::UnregisterBroker(message) => message.write(buf, version),
            RequestKind::DescribeTransactions(message) => message.write(buf, version),
            RequestKind::ListTransactions(message) => message.write(buf, version),
            RequestKind::AllocateProducerIds(message) => message.write(buf, version),
            RequestKind::ConsumerGroupHeartbeat(message) => message.write(buf, version),
            RequestKind::ConsumerGroupDescribe(message) => message.write(buf, version),
            RequestKind::ControllerRegistration(message) => message.write(buf, version),
            RequestKind::GetTelemetrySubscriptions(message) => message.write(buf, version),
            RequestKind::PushTelemetry(message) => message.write(buf, version),
            RequestKind::AssignReplicasToDirs(message) => message.write(buf, version),
            RequestKind::ListConfigResources(message) => message.write(buf, version),
            RequestKind::DescribeTopicPartitions(message) => message.write(buf, version),
            RequestKind::ShareGroupHeartbeat(message) => message.write(buf, version),
            RequestKind::ShareGroupDescribe(message) => message.write(buf, version),
            RequestKind::ShareFetch(message) => message.write(buf, version),
            RequestKind::ShareAcknowledge(message) => message.write(buf, version),
            RequestKind::AddRaftVoter(message) => message.write(buf, version),
            RequestKind::RemoveRaftVoter(message) => message.write(buf, version),
            RequestKind::UpdateRaftVoter(message) => message.write(buf, version),
            RequestKind::InitializeShareGroupState(message) => message.write(buf, version),
            RequestKind::ReadShareGroupState(message) => message.write(buf, version),
            RequestKind::WriteShareGroupState(message) => message.write(buf, version),
            RequestKind::DeleteShareGroupState(message) => message.write(buf, version),
            RequestKind::ReadShareGroupStateSummary(message) => message.write(buf, version),
            RequestKind::StreamsGroupHeartbeat(message) => message.write(buf, version),
            RequestKind::StreamsGroupDescribe(message) => message.write(buf, version),
            RequestKind::DescribeShareGroupOffsets(message) => message.write(buf, version),
            RequestKind::AlterShareGroupOffsets(message) => message.write(buf, version),
            RequestKind::DeleteShareGroupOffsets(message) => message.write(buf, version),
        }
    }
    pub fn encoded_len(&self, version: i16) -> crate::Result<usize> {
        match self {
            RequestKind::Produce(message) => message.encoded_len(version),
            RequestKind::Fetch(message) => message.encoded_len(version),
            RequestKind::ListOffsets(message) => message.encoded_len(version),
            RequestKind::Metadata(message) => message.encoded_len(version),
            RequestKind::LeaderAndIsr(message) => message.encoded_len(version),
            RequestKind::StopReplica(message) => message.encoded_len(version),
            RequestKind::UpdateMetadata(message) => message.encoded_len(version),
            RequestKind::ControlledShutdown(message) => message.encoded_len(version),
            RequestKind::OffsetCommit(message) => message.encoded_len(version),
            RequestKind::OffsetFetch(message) => message.encoded_len(version),
            RequestKind::FindCoordinator(message) => message.encoded_len(version),
            RequestKind::JoinGroup(message) => message.encoded_len(version),
            RequestKind::Heartbeat(message) => message.encoded_len(version),
            RequestKind::LeaveGroup(message) => message.encoded_len(version),
            RequestKind::SyncGroup(message) => message.encoded_len(version),
            RequestKind::DescribeGroups(message) => message.encoded_len(version),
            RequestKind::ListGroups(message) => message.encoded_len(version),
            RequestKind::SaslHandshake(message) => message.encoded_len(version),
            RequestKind::ApiVersions(message) => message.encoded_len(version),
            RequestKind::CreateTopics(message) => message.encoded_len(version),
            RequestKind::DeleteTopics(message) => message.encoded_len(version),
            RequestKind::DeleteRecords(message) => message.encoded_len(version),
            RequestKind::InitProducerId(message) => message.encoded_len(version),
            RequestKind::OffsetForLeaderEpoch(message) => message.encoded_len(version),
            RequestKind::AddPartitionsToTxn(message) => message.encoded_len(version),
            RequestKind::AddOffsetsToTxn(message) => message.encoded_len(version),
            RequestKind::EndTxn(message) => message.encoded_len(version),
            RequestKind::WriteTxnMarkers(message) => message.encoded_len(version),
            RequestKind::TxnOffsetCommit(message) => message.encoded_len(version),
            RequestKind::DescribeAcls(message) => message.encoded_len(version),
            RequestKind::CreateAcls(message) => message.encoded_len(version),
            RequestKind::DeleteAcls(message) => message.encoded_len(version),
            RequestKind::DescribeConfigs(message) => message.encoded_len(version),
            RequestKind::AlterConfigs(message) => message.encoded_len(version),
            RequestKind::AlterReplicaLogDirs(message) => message.encoded_len(version),
            RequestKind::DescribeLogDirs(message) => message.encoded_len(version),
            RequestKind::SaslAuthenticate(message) => message.encoded_len(version),
            RequestKind::CreatePartitions(message) => message.encoded_len(version),
            RequestKind::CreateDelegationToken(message) => message.encoded_len(version),
            RequestKind::RenewDelegationToken(message) => message.encoded_len(version),
            RequestKind::ExpireDelegationToken(message) => message.encoded_len(version),
            RequestKind::DescribeDelegationToken(message) => message.encoded_len(version),
            RequestKind::DeleteGroups(message) => message.encoded_len(version),
            RequestKind::ElectLeaders(message) => message.encoded_len(version),
            RequestKind::IncrementalAlterConfigs(message) => message.encoded_len(version),
            RequestKind::AlterPartitionReassignments(message) => message.encoded_len(version),
            RequestKind::ListPartitionReassignments(message) => message.encoded_len(version),
            RequestKind::OffsetDelete(message) => message.encoded_len(version),
            RequestKind::DescribeClientQuotas(message) => message.encoded_len(version),
            RequestKind::AlterClientQuotas(message) => message.encoded_len(version),
            RequestKind::DescribeUserScramCredentials(message) => message.encoded_len(version),
            RequestKind::AlterUserScramCredentials(message) => message.encoded_len(version),
            RequestKind::Vote(message) => message.encoded_len(version),
            RequestKind::BeginQuorumEpoch(message) => message.encoded_len(version),
            RequestKind::EndQuorumEpoch(message) => message.encoded_len(version),
            RequestKind::DescribeQuorum(message) => message.encoded_len(version),
            RequestKind::AlterPartition(message) => message.encoded_len(version),
            RequestKind::UpdateFeatures(message) => message.encoded_len(version),
            RequestKind::Envelope(message) => message.encoded_len(version),
            RequestKind::FetchSnapshot(message) => message.encoded_len(version),
            RequestKind::DescribeCluster(message) => message.encoded_len(version),
            RequestKind::DescribeProducers(message) => message.encoded_len(version),
            RequestKind::BrokerRegistration(message) => message.encoded_len(version),
            RequestKind::BrokerHeartbeat(message) => message.encoded_len(version),
            RequestKind::UnregisterBroker(message) => message.encoded_len(version),
            RequestKind::DescribeTransactions(message) => message.encoded_len(version),
            RequestKind::ListTransactions(message) => message.encoded_len(version),
            RequestKind::AllocateProducerIds(message) => message.encoded_len(version),
            RequestKind::ConsumerGroupHeartbeat(message) => message.encoded_len(version),
            RequestKind::ConsumerGroupDescribe(message) => message.encoded_len(version),
            RequestKind::ControllerRegistration(message) => message.encoded_len(version),
            RequestKind::GetTelemetrySubscriptions(message) => message.encoded_len(version),
            RequestKind::PushTelemetry(message) => message.encoded_len(version),
            RequestKind::AssignReplicasToDirs(message) => message.encoded_len(version),
            RequestKind::ListConfigResources(message) => message.encoded_len(version),
            RequestKind::DescribeTopicPartitions(message) => message.encoded_len(version),
            RequestKind::ShareGroupHeartbeat(message) => message.encoded_len(version),
            RequestKind::ShareGroupDescribe(message) => message.encoded_len(version),
            RequestKind::ShareFetch(message) => message.encoded_len(version),
            RequestKind::ShareAcknowledge(message) => message.encoded_len(version),
            RequestKind::AddRaftVoter(message) => message.encoded_len(version),
            RequestKind::RemoveRaftVoter(message) => message.encoded_len(version),
            RequestKind::UpdateRaftVoter(message) => message.encoded_len(version),
            RequestKind::InitializeShareGroupState(message) => message.encoded_len(version),
            RequestKind::ReadShareGroupState(message) => message.encoded_len(version),
            RequestKind::WriteShareGroupState(message) => message.encoded_len(version),
            RequestKind::DeleteShareGroupState(message) => message.encoded_len(version),
            RequestKind::ReadShareGroupStateSummary(message) => message.encoded_len(version),
            RequestKind::StreamsGroupHeartbeat(message) => message.encoded_len(version),
            RequestKind::StreamsGroupDescribe(message) => message.encoded_len(version),
            RequestKind::DescribeShareGroupOffsets(message) => message.encoded_len(version),
            RequestKind::AlterShareGroupOffsets(message) => message.encoded_len(version),
            RequestKind::DeleteShareGroupOffsets(message) => message.encoded_len(version),
        }
    }
}
/// Type-erased generated Kafka responses.
#[cfg(feature = "message-enums")]
#[derive(Debug, Clone, PartialEq)]
pub enum ResponseKind {
    Produce(ProduceResponseData),
    Fetch(FetchResponseData),
    ListOffsets(ListOffsetsResponseData),
    Metadata(MetadataResponseData),
    LeaderAndIsr(LeaderAndIsrResponseData),
    StopReplica(StopReplicaResponseData),
    UpdateMetadata(UpdateMetadataResponseData),
    ControlledShutdown(ControlledShutdownResponseData),
    OffsetCommit(OffsetCommitResponseData),
    OffsetFetch(OffsetFetchResponseData),
    FindCoordinator(FindCoordinatorResponseData),
    JoinGroup(JoinGroupResponseData),
    Heartbeat(HeartbeatResponseData),
    LeaveGroup(LeaveGroupResponseData),
    SyncGroup(SyncGroupResponseData),
    DescribeGroups(DescribeGroupsResponseData),
    ListGroups(ListGroupsResponseData),
    SaslHandshake(SaslHandshakeResponseData),
    ApiVersions(ApiVersionsResponseData),
    CreateTopics(CreateTopicsResponseData),
    DeleteTopics(DeleteTopicsResponseData),
    DeleteRecords(DeleteRecordsResponseData),
    InitProducerId(InitProducerIdResponseData),
    OffsetForLeaderEpoch(OffsetForLeaderEpochResponseData),
    AddPartitionsToTxn(AddPartitionsToTxnResponseData),
    AddOffsetsToTxn(AddOffsetsToTxnResponseData),
    EndTxn(EndTxnResponseData),
    WriteTxnMarkers(WriteTxnMarkersResponseData),
    TxnOffsetCommit(TxnOffsetCommitResponseData),
    DescribeAcls(DescribeAclsResponseData),
    CreateAcls(CreateAclsResponseData),
    DeleteAcls(DeleteAclsResponseData),
    DescribeConfigs(DescribeConfigsResponseData),
    AlterConfigs(AlterConfigsResponseData),
    AlterReplicaLogDirs(AlterReplicaLogDirsResponseData),
    DescribeLogDirs(DescribeLogDirsResponseData),
    SaslAuthenticate(SaslAuthenticateResponseData),
    CreatePartitions(CreatePartitionsResponseData),
    CreateDelegationToken(CreateDelegationTokenResponseData),
    RenewDelegationToken(RenewDelegationTokenResponseData),
    ExpireDelegationToken(ExpireDelegationTokenResponseData),
    DescribeDelegationToken(DescribeDelegationTokenResponseData),
    DeleteGroups(DeleteGroupsResponseData),
    ElectLeaders(ElectLeadersResponseData),
    IncrementalAlterConfigs(IncrementalAlterConfigsResponseData),
    AlterPartitionReassignments(AlterPartitionReassignmentsResponseData),
    ListPartitionReassignments(ListPartitionReassignmentsResponseData),
    OffsetDelete(OffsetDeleteResponseData),
    DescribeClientQuotas(DescribeClientQuotasResponseData),
    AlterClientQuotas(AlterClientQuotasResponseData),
    DescribeUserScramCredentials(DescribeUserScramCredentialsResponseData),
    AlterUserScramCredentials(AlterUserScramCredentialsResponseData),
    Vote(VoteResponseData),
    BeginQuorumEpoch(BeginQuorumEpochResponseData),
    EndQuorumEpoch(EndQuorumEpochResponseData),
    DescribeQuorum(DescribeQuorumResponseData),
    AlterPartition(AlterPartitionResponseData),
    UpdateFeatures(UpdateFeaturesResponseData),
    Envelope(EnvelopeResponseData),
    FetchSnapshot(FetchSnapshotResponseData),
    DescribeCluster(DescribeClusterResponseData),
    DescribeProducers(DescribeProducersResponseData),
    BrokerRegistration(BrokerRegistrationResponseData),
    BrokerHeartbeat(BrokerHeartbeatResponseData),
    UnregisterBroker(UnregisterBrokerResponseData),
    DescribeTransactions(DescribeTransactionsResponseData),
    ListTransactions(ListTransactionsResponseData),
    AllocateProducerIds(AllocateProducerIdsResponseData),
    ConsumerGroupHeartbeat(ConsumerGroupHeartbeatResponseData),
    ConsumerGroupDescribe(ConsumerGroupDescribeResponseData),
    ControllerRegistration(ControllerRegistrationResponseData),
    GetTelemetrySubscriptions(GetTelemetrySubscriptionsResponseData),
    PushTelemetry(PushTelemetryResponseData),
    AssignReplicasToDirs(AssignReplicasToDirsResponseData),
    ListConfigResources(ListConfigResourcesResponseData),
    DescribeTopicPartitions(DescribeTopicPartitionsResponseData),
    ShareGroupHeartbeat(ShareGroupHeartbeatResponseData),
    ShareGroupDescribe(ShareGroupDescribeResponseData),
    ShareFetch(ShareFetchResponseData),
    ShareAcknowledge(ShareAcknowledgeResponseData),
    AddRaftVoter(AddRaftVoterResponseData),
    RemoveRaftVoter(RemoveRaftVoterResponseData),
    UpdateRaftVoter(UpdateRaftVoterResponseData),
    InitializeShareGroupState(InitializeShareGroupStateResponseData),
    ReadShareGroupState(ReadShareGroupStateResponseData),
    WriteShareGroupState(WriteShareGroupStateResponseData),
    DeleteShareGroupState(DeleteShareGroupStateResponseData),
    ReadShareGroupStateSummary(ReadShareGroupStateSummaryResponseData),
    StreamsGroupHeartbeat(StreamsGroupHeartbeatResponseData),
    StreamsGroupDescribe(StreamsGroupDescribeResponseData),
    DescribeShareGroupOffsets(DescribeShareGroupOffsetsResponseData),
    AlterShareGroupOffsets(AlterShareGroupOffsetsResponseData),
    DeleteShareGroupOffsets(DeleteShareGroupOffsetsResponseData),
}
#[cfg(feature = "message-enums")]
impl ResponseKind {
    pub fn api_key(&self) -> ApiKey {
        match self {
            ResponseKind::Produce(_) => ApiKey::Produce,
            ResponseKind::Fetch(_) => ApiKey::Fetch,
            ResponseKind::ListOffsets(_) => ApiKey::ListOffsets,
            ResponseKind::Metadata(_) => ApiKey::Metadata,
            ResponseKind::LeaderAndIsr(_) => ApiKey::LeaderAndIsr,
            ResponseKind::StopReplica(_) => ApiKey::StopReplica,
            ResponseKind::UpdateMetadata(_) => ApiKey::UpdateMetadata,
            ResponseKind::ControlledShutdown(_) => ApiKey::ControlledShutdown,
            ResponseKind::OffsetCommit(_) => ApiKey::OffsetCommit,
            ResponseKind::OffsetFetch(_) => ApiKey::OffsetFetch,
            ResponseKind::FindCoordinator(_) => ApiKey::FindCoordinator,
            ResponseKind::JoinGroup(_) => ApiKey::JoinGroup,
            ResponseKind::Heartbeat(_) => ApiKey::Heartbeat,
            ResponseKind::LeaveGroup(_) => ApiKey::LeaveGroup,
            ResponseKind::SyncGroup(_) => ApiKey::SyncGroup,
            ResponseKind::DescribeGroups(_) => ApiKey::DescribeGroups,
            ResponseKind::ListGroups(_) => ApiKey::ListGroups,
            ResponseKind::SaslHandshake(_) => ApiKey::SaslHandshake,
            ResponseKind::ApiVersions(_) => ApiKey::ApiVersions,
            ResponseKind::CreateTopics(_) => ApiKey::CreateTopics,
            ResponseKind::DeleteTopics(_) => ApiKey::DeleteTopics,
            ResponseKind::DeleteRecords(_) => ApiKey::DeleteRecords,
            ResponseKind::InitProducerId(_) => ApiKey::InitProducerId,
            ResponseKind::OffsetForLeaderEpoch(_) => ApiKey::OffsetForLeaderEpoch,
            ResponseKind::AddPartitionsToTxn(_) => ApiKey::AddPartitionsToTxn,
            ResponseKind::AddOffsetsToTxn(_) => ApiKey::AddOffsetsToTxn,
            ResponseKind::EndTxn(_) => ApiKey::EndTxn,
            ResponseKind::WriteTxnMarkers(_) => ApiKey::WriteTxnMarkers,
            ResponseKind::TxnOffsetCommit(_) => ApiKey::TxnOffsetCommit,
            ResponseKind::DescribeAcls(_) => ApiKey::DescribeAcls,
            ResponseKind::CreateAcls(_) => ApiKey::CreateAcls,
            ResponseKind::DeleteAcls(_) => ApiKey::DeleteAcls,
            ResponseKind::DescribeConfigs(_) => ApiKey::DescribeConfigs,
            ResponseKind::AlterConfigs(_) => ApiKey::AlterConfigs,
            ResponseKind::AlterReplicaLogDirs(_) => ApiKey::AlterReplicaLogDirs,
            ResponseKind::DescribeLogDirs(_) => ApiKey::DescribeLogDirs,
            ResponseKind::SaslAuthenticate(_) => ApiKey::SaslAuthenticate,
            ResponseKind::CreatePartitions(_) => ApiKey::CreatePartitions,
            ResponseKind::CreateDelegationToken(_) => ApiKey::CreateDelegationToken,
            ResponseKind::RenewDelegationToken(_) => ApiKey::RenewDelegationToken,
            ResponseKind::ExpireDelegationToken(_) => ApiKey::ExpireDelegationToken,
            ResponseKind::DescribeDelegationToken(_) => ApiKey::DescribeDelegationToken,
            ResponseKind::DeleteGroups(_) => ApiKey::DeleteGroups,
            ResponseKind::ElectLeaders(_) => ApiKey::ElectLeaders,
            ResponseKind::IncrementalAlterConfigs(_) => ApiKey::IncrementalAlterConfigs,
            ResponseKind::AlterPartitionReassignments(_) => ApiKey::AlterPartitionReassignments,
            ResponseKind::ListPartitionReassignments(_) => ApiKey::ListPartitionReassignments,
            ResponseKind::OffsetDelete(_) => ApiKey::OffsetDelete,
            ResponseKind::DescribeClientQuotas(_) => ApiKey::DescribeClientQuotas,
            ResponseKind::AlterClientQuotas(_) => ApiKey::AlterClientQuotas,
            ResponseKind::DescribeUserScramCredentials(_) => ApiKey::DescribeUserScramCredentials,
            ResponseKind::AlterUserScramCredentials(_) => ApiKey::AlterUserScramCredentials,
            ResponseKind::Vote(_) => ApiKey::Vote,
            ResponseKind::BeginQuorumEpoch(_) => ApiKey::BeginQuorumEpoch,
            ResponseKind::EndQuorumEpoch(_) => ApiKey::EndQuorumEpoch,
            ResponseKind::DescribeQuorum(_) => ApiKey::DescribeQuorum,
            ResponseKind::AlterPartition(_) => ApiKey::AlterPartition,
            ResponseKind::UpdateFeatures(_) => ApiKey::UpdateFeatures,
            ResponseKind::Envelope(_) => ApiKey::Envelope,
            ResponseKind::FetchSnapshot(_) => ApiKey::FetchSnapshot,
            ResponseKind::DescribeCluster(_) => ApiKey::DescribeCluster,
            ResponseKind::DescribeProducers(_) => ApiKey::DescribeProducers,
            ResponseKind::BrokerRegistration(_) => ApiKey::BrokerRegistration,
            ResponseKind::BrokerHeartbeat(_) => ApiKey::BrokerHeartbeat,
            ResponseKind::UnregisterBroker(_) => ApiKey::UnregisterBroker,
            ResponseKind::DescribeTransactions(_) => ApiKey::DescribeTransactions,
            ResponseKind::ListTransactions(_) => ApiKey::ListTransactions,
            ResponseKind::AllocateProducerIds(_) => ApiKey::AllocateProducerIds,
            ResponseKind::ConsumerGroupHeartbeat(_) => ApiKey::ConsumerGroupHeartbeat,
            ResponseKind::ConsumerGroupDescribe(_) => ApiKey::ConsumerGroupDescribe,
            ResponseKind::ControllerRegistration(_) => ApiKey::ControllerRegistration,
            ResponseKind::GetTelemetrySubscriptions(_) => ApiKey::GetTelemetrySubscriptions,
            ResponseKind::PushTelemetry(_) => ApiKey::PushTelemetry,
            ResponseKind::AssignReplicasToDirs(_) => ApiKey::AssignReplicasToDirs,
            ResponseKind::ListConfigResources(_) => ApiKey::ListConfigResources,
            ResponseKind::DescribeTopicPartitions(_) => ApiKey::DescribeTopicPartitions,
            ResponseKind::ShareGroupHeartbeat(_) => ApiKey::ShareGroupHeartbeat,
            ResponseKind::ShareGroupDescribe(_) => ApiKey::ShareGroupDescribe,
            ResponseKind::ShareFetch(_) => ApiKey::ShareFetch,
            ResponseKind::ShareAcknowledge(_) => ApiKey::ShareAcknowledge,
            ResponseKind::AddRaftVoter(_) => ApiKey::AddRaftVoter,
            ResponseKind::RemoveRaftVoter(_) => ApiKey::RemoveRaftVoter,
            ResponseKind::UpdateRaftVoter(_) => ApiKey::UpdateRaftVoter,
            ResponseKind::InitializeShareGroupState(_) => ApiKey::InitializeShareGroupState,
            ResponseKind::ReadShareGroupState(_) => ApiKey::ReadShareGroupState,
            ResponseKind::WriteShareGroupState(_) => ApiKey::WriteShareGroupState,
            ResponseKind::DeleteShareGroupState(_) => ApiKey::DeleteShareGroupState,
            ResponseKind::ReadShareGroupStateSummary(_) => ApiKey::ReadShareGroupStateSummary,
            ResponseKind::StreamsGroupHeartbeat(_) => ApiKey::StreamsGroupHeartbeat,
            ResponseKind::StreamsGroupDescribe(_) => ApiKey::StreamsGroupDescribe,
            ResponseKind::DescribeShareGroupOffsets(_) => ApiKey::DescribeShareGroupOffsets,
            ResponseKind::AlterShareGroupOffsets(_) => ApiKey::AlterShareGroupOffsets,
            ResponseKind::DeleteShareGroupOffsets(_) => ApiKey::DeleteShareGroupOffsets,
        }
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> crate::Result<()> {
        match self {
            ResponseKind::Produce(message) => message.write(buf, version),
            ResponseKind::Fetch(message) => message.write(buf, version),
            ResponseKind::ListOffsets(message) => message.write(buf, version),
            ResponseKind::Metadata(message) => message.write(buf, version),
            ResponseKind::LeaderAndIsr(message) => message.write(buf, version),
            ResponseKind::StopReplica(message) => message.write(buf, version),
            ResponseKind::UpdateMetadata(message) => message.write(buf, version),
            ResponseKind::ControlledShutdown(message) => message.write(buf, version),
            ResponseKind::OffsetCommit(message) => message.write(buf, version),
            ResponseKind::OffsetFetch(message) => message.write(buf, version),
            ResponseKind::FindCoordinator(message) => message.write(buf, version),
            ResponseKind::JoinGroup(message) => message.write(buf, version),
            ResponseKind::Heartbeat(message) => message.write(buf, version),
            ResponseKind::LeaveGroup(message) => message.write(buf, version),
            ResponseKind::SyncGroup(message) => message.write(buf, version),
            ResponseKind::DescribeGroups(message) => message.write(buf, version),
            ResponseKind::ListGroups(message) => message.write(buf, version),
            ResponseKind::SaslHandshake(message) => message.write(buf, version),
            ResponseKind::ApiVersions(message) => message.write(buf, version),
            ResponseKind::CreateTopics(message) => message.write(buf, version),
            ResponseKind::DeleteTopics(message) => message.write(buf, version),
            ResponseKind::DeleteRecords(message) => message.write(buf, version),
            ResponseKind::InitProducerId(message) => message.write(buf, version),
            ResponseKind::OffsetForLeaderEpoch(message) => message.write(buf, version),
            ResponseKind::AddPartitionsToTxn(message) => message.write(buf, version),
            ResponseKind::AddOffsetsToTxn(message) => message.write(buf, version),
            ResponseKind::EndTxn(message) => message.write(buf, version),
            ResponseKind::WriteTxnMarkers(message) => message.write(buf, version),
            ResponseKind::TxnOffsetCommit(message) => message.write(buf, version),
            ResponseKind::DescribeAcls(message) => message.write(buf, version),
            ResponseKind::CreateAcls(message) => message.write(buf, version),
            ResponseKind::DeleteAcls(message) => message.write(buf, version),
            ResponseKind::DescribeConfigs(message) => message.write(buf, version),
            ResponseKind::AlterConfigs(message) => message.write(buf, version),
            ResponseKind::AlterReplicaLogDirs(message) => message.write(buf, version),
            ResponseKind::DescribeLogDirs(message) => message.write(buf, version),
            ResponseKind::SaslAuthenticate(message) => message.write(buf, version),
            ResponseKind::CreatePartitions(message) => message.write(buf, version),
            ResponseKind::CreateDelegationToken(message) => message.write(buf, version),
            ResponseKind::RenewDelegationToken(message) => message.write(buf, version),
            ResponseKind::ExpireDelegationToken(message) => message.write(buf, version),
            ResponseKind::DescribeDelegationToken(message) => message.write(buf, version),
            ResponseKind::DeleteGroups(message) => message.write(buf, version),
            ResponseKind::ElectLeaders(message) => message.write(buf, version),
            ResponseKind::IncrementalAlterConfigs(message) => message.write(buf, version),
            ResponseKind::AlterPartitionReassignments(message) => message.write(buf, version),
            ResponseKind::ListPartitionReassignments(message) => message.write(buf, version),
            ResponseKind::OffsetDelete(message) => message.write(buf, version),
            ResponseKind::DescribeClientQuotas(message) => message.write(buf, version),
            ResponseKind::AlterClientQuotas(message) => message.write(buf, version),
            ResponseKind::DescribeUserScramCredentials(message) => message.write(buf, version),
            ResponseKind::AlterUserScramCredentials(message) => message.write(buf, version),
            ResponseKind::Vote(message) => message.write(buf, version),
            ResponseKind::BeginQuorumEpoch(message) => message.write(buf, version),
            ResponseKind::EndQuorumEpoch(message) => message.write(buf, version),
            ResponseKind::DescribeQuorum(message) => message.write(buf, version),
            ResponseKind::AlterPartition(message) => message.write(buf, version),
            ResponseKind::UpdateFeatures(message) => message.write(buf, version),
            ResponseKind::Envelope(message) => message.write(buf, version),
            ResponseKind::FetchSnapshot(message) => message.write(buf, version),
            ResponseKind::DescribeCluster(message) => message.write(buf, version),
            ResponseKind::DescribeProducers(message) => message.write(buf, version),
            ResponseKind::BrokerRegistration(message) => message.write(buf, version),
            ResponseKind::BrokerHeartbeat(message) => message.write(buf, version),
            ResponseKind::UnregisterBroker(message) => message.write(buf, version),
            ResponseKind::DescribeTransactions(message) => message.write(buf, version),
            ResponseKind::ListTransactions(message) => message.write(buf, version),
            ResponseKind::AllocateProducerIds(message) => message.write(buf, version),
            ResponseKind::ConsumerGroupHeartbeat(message) => message.write(buf, version),
            ResponseKind::ConsumerGroupDescribe(message) => message.write(buf, version),
            ResponseKind::ControllerRegistration(message) => message.write(buf, version),
            ResponseKind::GetTelemetrySubscriptions(message) => message.write(buf, version),
            ResponseKind::PushTelemetry(message) => message.write(buf, version),
            ResponseKind::AssignReplicasToDirs(message) => message.write(buf, version),
            ResponseKind::ListConfigResources(message) => message.write(buf, version),
            ResponseKind::DescribeTopicPartitions(message) => message.write(buf, version),
            ResponseKind::ShareGroupHeartbeat(message) => message.write(buf, version),
            ResponseKind::ShareGroupDescribe(message) => message.write(buf, version),
            ResponseKind::ShareFetch(message) => message.write(buf, version),
            ResponseKind::ShareAcknowledge(message) => message.write(buf, version),
            ResponseKind::AddRaftVoter(message) => message.write(buf, version),
            ResponseKind::RemoveRaftVoter(message) => message.write(buf, version),
            ResponseKind::UpdateRaftVoter(message) => message.write(buf, version),
            ResponseKind::InitializeShareGroupState(message) => message.write(buf, version),
            ResponseKind::ReadShareGroupState(message) => message.write(buf, version),
            ResponseKind::WriteShareGroupState(message) => message.write(buf, version),
            ResponseKind::DeleteShareGroupState(message) => message.write(buf, version),
            ResponseKind::ReadShareGroupStateSummary(message) => message.write(buf, version),
            ResponseKind::StreamsGroupHeartbeat(message) => message.write(buf, version),
            ResponseKind::StreamsGroupDescribe(message) => message.write(buf, version),
            ResponseKind::DescribeShareGroupOffsets(message) => message.write(buf, version),
            ResponseKind::AlterShareGroupOffsets(message) => message.write(buf, version),
            ResponseKind::DeleteShareGroupOffsets(message) => message.write(buf, version),
        }
    }
    pub fn encoded_len(&self, version: i16) -> crate::Result<usize> {
        match self {
            ResponseKind::Produce(message) => message.encoded_len(version),
            ResponseKind::Fetch(message) => message.encoded_len(version),
            ResponseKind::ListOffsets(message) => message.encoded_len(version),
            ResponseKind::Metadata(message) => message.encoded_len(version),
            ResponseKind::LeaderAndIsr(message) => message.encoded_len(version),
            ResponseKind::StopReplica(message) => message.encoded_len(version),
            ResponseKind::UpdateMetadata(message) => message.encoded_len(version),
            ResponseKind::ControlledShutdown(message) => message.encoded_len(version),
            ResponseKind::OffsetCommit(message) => message.encoded_len(version),
            ResponseKind::OffsetFetch(message) => message.encoded_len(version),
            ResponseKind::FindCoordinator(message) => message.encoded_len(version),
            ResponseKind::JoinGroup(message) => message.encoded_len(version),
            ResponseKind::Heartbeat(message) => message.encoded_len(version),
            ResponseKind::LeaveGroup(message) => message.encoded_len(version),
            ResponseKind::SyncGroup(message) => message.encoded_len(version),
            ResponseKind::DescribeGroups(message) => message.encoded_len(version),
            ResponseKind::ListGroups(message) => message.encoded_len(version),
            ResponseKind::SaslHandshake(message) => message.encoded_len(version),
            ResponseKind::ApiVersions(message) => message.encoded_len(version),
            ResponseKind::CreateTopics(message) => message.encoded_len(version),
            ResponseKind::DeleteTopics(message) => message.encoded_len(version),
            ResponseKind::DeleteRecords(message) => message.encoded_len(version),
            ResponseKind::InitProducerId(message) => message.encoded_len(version),
            ResponseKind::OffsetForLeaderEpoch(message) => message.encoded_len(version),
            ResponseKind::AddPartitionsToTxn(message) => message.encoded_len(version),
            ResponseKind::AddOffsetsToTxn(message) => message.encoded_len(version),
            ResponseKind::EndTxn(message) => message.encoded_len(version),
            ResponseKind::WriteTxnMarkers(message) => message.encoded_len(version),
            ResponseKind::TxnOffsetCommit(message) => message.encoded_len(version),
            ResponseKind::DescribeAcls(message) => message.encoded_len(version),
            ResponseKind::CreateAcls(message) => message.encoded_len(version),
            ResponseKind::DeleteAcls(message) => message.encoded_len(version),
            ResponseKind::DescribeConfigs(message) => message.encoded_len(version),
            ResponseKind::AlterConfigs(message) => message.encoded_len(version),
            ResponseKind::AlterReplicaLogDirs(message) => message.encoded_len(version),
            ResponseKind::DescribeLogDirs(message) => message.encoded_len(version),
            ResponseKind::SaslAuthenticate(message) => message.encoded_len(version),
            ResponseKind::CreatePartitions(message) => message.encoded_len(version),
            ResponseKind::CreateDelegationToken(message) => message.encoded_len(version),
            ResponseKind::RenewDelegationToken(message) => message.encoded_len(version),
            ResponseKind::ExpireDelegationToken(message) => message.encoded_len(version),
            ResponseKind::DescribeDelegationToken(message) => message.encoded_len(version),
            ResponseKind::DeleteGroups(message) => message.encoded_len(version),
            ResponseKind::ElectLeaders(message) => message.encoded_len(version),
            ResponseKind::IncrementalAlterConfigs(message) => message.encoded_len(version),
            ResponseKind::AlterPartitionReassignments(message) => message.encoded_len(version),
            ResponseKind::ListPartitionReassignments(message) => message.encoded_len(version),
            ResponseKind::OffsetDelete(message) => message.encoded_len(version),
            ResponseKind::DescribeClientQuotas(message) => message.encoded_len(version),
            ResponseKind::AlterClientQuotas(message) => message.encoded_len(version),
            ResponseKind::DescribeUserScramCredentials(message) => message.encoded_len(version),
            ResponseKind::AlterUserScramCredentials(message) => message.encoded_len(version),
            ResponseKind::Vote(message) => message.encoded_len(version),
            ResponseKind::BeginQuorumEpoch(message) => message.encoded_len(version),
            ResponseKind::EndQuorumEpoch(message) => message.encoded_len(version),
            ResponseKind::DescribeQuorum(message) => message.encoded_len(version),
            ResponseKind::AlterPartition(message) => message.encoded_len(version),
            ResponseKind::UpdateFeatures(message) => message.encoded_len(version),
            ResponseKind::Envelope(message) => message.encoded_len(version),
            ResponseKind::FetchSnapshot(message) => message.encoded_len(version),
            ResponseKind::DescribeCluster(message) => message.encoded_len(version),
            ResponseKind::DescribeProducers(message) => message.encoded_len(version),
            ResponseKind::BrokerRegistration(message) => message.encoded_len(version),
            ResponseKind::BrokerHeartbeat(message) => message.encoded_len(version),
            ResponseKind::UnregisterBroker(message) => message.encoded_len(version),
            ResponseKind::DescribeTransactions(message) => message.encoded_len(version),
            ResponseKind::ListTransactions(message) => message.encoded_len(version),
            ResponseKind::AllocateProducerIds(message) => message.encoded_len(version),
            ResponseKind::ConsumerGroupHeartbeat(message) => message.encoded_len(version),
            ResponseKind::ConsumerGroupDescribe(message) => message.encoded_len(version),
            ResponseKind::ControllerRegistration(message) => message.encoded_len(version),
            ResponseKind::GetTelemetrySubscriptions(message) => message.encoded_len(version),
            ResponseKind::PushTelemetry(message) => message.encoded_len(version),
            ResponseKind::AssignReplicasToDirs(message) => message.encoded_len(version),
            ResponseKind::ListConfigResources(message) => message.encoded_len(version),
            ResponseKind::DescribeTopicPartitions(message) => message.encoded_len(version),
            ResponseKind::ShareGroupHeartbeat(message) => message.encoded_len(version),
            ResponseKind::ShareGroupDescribe(message) => message.encoded_len(version),
            ResponseKind::ShareFetch(message) => message.encoded_len(version),
            ResponseKind::ShareAcknowledge(message) => message.encoded_len(version),
            ResponseKind::AddRaftVoter(message) => message.encoded_len(version),
            ResponseKind::RemoveRaftVoter(message) => message.encoded_len(version),
            ResponseKind::UpdateRaftVoter(message) => message.encoded_len(version),
            ResponseKind::InitializeShareGroupState(message) => message.encoded_len(version),
            ResponseKind::ReadShareGroupState(message) => message.encoded_len(version),
            ResponseKind::WriteShareGroupState(message) => message.encoded_len(version),
            ResponseKind::DeleteShareGroupState(message) => message.encoded_len(version),
            ResponseKind::ReadShareGroupStateSummary(message) => message.encoded_len(version),
            ResponseKind::StreamsGroupHeartbeat(message) => message.encoded_len(version),
            ResponseKind::StreamsGroupDescribe(message) => message.encoded_len(version),
            ResponseKind::DescribeShareGroupOffsets(message) => message.encoded_len(version),
            ResponseKind::AlterShareGroupOffsets(message) => message.encoded_len(version),
            ResponseKind::DeleteShareGroupOffsets(message) => message.encoded_len(version),
        }
    }
}
