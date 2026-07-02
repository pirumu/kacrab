//! Kafka protocol error codes.
//!
//! Generated from Kafka's `Errors.java` -- DO NOT EDIT.
#![allow(
    missing_docs,
    clippy::all,
    clippy::pedantic,
    clippy::nursery,
    reason = "Generated protocol error-code variants mirror Kafka's Java enum and intentionally \
              avoid duplicating Java docs for every variant."
)]
/// Kafka protocol error code.
///
/// Every response in the Kafka protocol carries an `i16` error code.
/// This enum provides a typed representation with human-readable messages,
/// retriability classification, and forward-compatible handling of unknown codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ErrorCode {
    UnknownServerError,
    None,
    OffsetOutOfRange,
    CorruptMessage,
    UnknownTopicOrPartition,
    InvalidFetchSize,
    LeaderNotAvailable,
    NotLeaderOrFollower,
    RequestTimedOut,
    BrokerNotAvailable,
    ReplicaNotAvailable,
    MessageTooLarge,
    StaleControllerEpoch,
    OffsetMetadataTooLarge,
    NetworkException,
    CoordinatorLoadInProgress,
    CoordinatorNotAvailable,
    NotCoordinator,
    InvalidTopicException,
    RecordListTooLarge,
    NotEnoughReplicas,
    NotEnoughReplicasAfterAppend,
    InvalidRequiredAcks,
    IllegalGeneration,
    InconsistentGroupProtocol,
    InvalidGroupId,
    UnknownMemberId,
    InvalidSessionTimeout,
    RebalanceInProgress,
    InvalidCommitOffsetSize,
    TopicAuthorizationFailed,
    GroupAuthorizationFailed,
    ClusterAuthorizationFailed,
    InvalidTimestamp,
    UnsupportedSaslMechanism,
    IllegalSaslState,
    UnsupportedVersion,
    TopicAlreadyExists,
    InvalidPartitions,
    InvalidReplicationFactor,
    InvalidReplicaAssignment,
    InvalidConfig,
    NotController,
    InvalidRequest,
    UnsupportedForMessageFormat,
    PolicyViolation,
    OutOfOrderSequenceNumber,
    DuplicateSequenceNumber,
    InvalidProducerEpoch,
    InvalidTxnState,
    InvalidProducerIdMapping,
    InvalidTransactionTimeout,
    ConcurrentTransactions,
    TransactionCoordinatorFenced,
    TransactionalIdAuthorizationFailed,
    SecurityDisabled,
    OperationNotAttempted,
    KafkaStorageError,
    LogDirNotFound,
    SaslAuthenticationFailed,
    UnknownProducerId,
    ReassignmentInProgress,
    DelegationTokenAuthDisabled,
    DelegationTokenNotFound,
    DelegationTokenOwnerMismatch,
    DelegationTokenRequestNotAllowed,
    DelegationTokenAuthorizationFailed,
    DelegationTokenExpired,
    InvalidPrincipalType,
    NonEmptyGroup,
    GroupIdNotFound,
    FetchSessionIdNotFound,
    InvalidFetchSessionEpoch,
    ListenerNotFound,
    TopicDeletionDisabled,
    FencedLeaderEpoch,
    UnknownLeaderEpoch,
    UnsupportedCompressionType,
    StaleBrokerEpoch,
    OffsetNotAvailable,
    MemberIdRequired,
    PreferredLeaderNotAvailable,
    GroupMaxSizeReached,
    FencedInstanceId,
    EligibleLeadersNotAvailable,
    ElectionNotNeeded,
    NoReassignmentInProgress,
    GroupSubscribedToTopic,
    InvalidRecord,
    UnstableOffsetCommit,
    ThrottlingQuotaExceeded,
    ProducerFenced,
    ResourceNotFound,
    DuplicateResource,
    UnacceptableCredential,
    InconsistentVoterSet,
    InvalidUpdateVersion,
    FeatureUpdateFailed,
    PrincipalDeserializationFailure,
    SnapshotNotFound,
    PositionOutOfRange,
    UnknownTopicId,
    DuplicateBrokerRegistration,
    BrokerIdNotRegistered,
    InconsistentTopicId,
    InconsistentClusterId,
    TransactionalIdNotFound,
    FetchSessionTopicIdError,
    IneligibleReplica,
    NewLeaderElected,
    OffsetMovedToTieredStorage,
    FencedMemberEpoch,
    UnreleasedInstanceId,
    UnsupportedAssignor,
    StaleMemberEpoch,
    MismatchedEndpointType,
    UnsupportedEndpointType,
    UnknownControllerId,
    UnknownSubscriptionId,
    TelemetryTooLarge,
    InvalidRegistration,
    TransactionAbortable,
    InvalidRecordState,
    ShareSessionNotFound,
    InvalidShareSessionEpoch,
    FencedStateEpoch,
    InvalidVoterKey,
    DuplicateVoter,
    VoterNotFound,
    InvalidRegularExpression,
    RebootstrapRequired,
    StreamsInvalidTopology,
    StreamsInvalidTopologyEpoch,
    StreamsTopologyFenced,
    ShareSessionLimitReached,
    /// An error code not recognized by this client version.
    Unknown(i16),
}
impl ErrorCode {
    /// Returns the `i16` wire value for this error code.
    pub fn code(&self) -> i16 {
        match self {
            ErrorCode::UnknownServerError => -1,
            ErrorCode::None => 0,
            ErrorCode::OffsetOutOfRange => 1,
            ErrorCode::CorruptMessage => 2,
            ErrorCode::UnknownTopicOrPartition => 3,
            ErrorCode::InvalidFetchSize => 4,
            ErrorCode::LeaderNotAvailable => 5,
            ErrorCode::NotLeaderOrFollower => 6,
            ErrorCode::RequestTimedOut => 7,
            ErrorCode::BrokerNotAvailable => 8,
            ErrorCode::ReplicaNotAvailable => 9,
            ErrorCode::MessageTooLarge => 10,
            ErrorCode::StaleControllerEpoch => 11,
            ErrorCode::OffsetMetadataTooLarge => 12,
            ErrorCode::NetworkException => 13,
            ErrorCode::CoordinatorLoadInProgress => 14,
            ErrorCode::CoordinatorNotAvailable => 15,
            ErrorCode::NotCoordinator => 16,
            ErrorCode::InvalidTopicException => 17,
            ErrorCode::RecordListTooLarge => 18,
            ErrorCode::NotEnoughReplicas => 19,
            ErrorCode::NotEnoughReplicasAfterAppend => 20,
            ErrorCode::InvalidRequiredAcks => 21,
            ErrorCode::IllegalGeneration => 22,
            ErrorCode::InconsistentGroupProtocol => 23,
            ErrorCode::InvalidGroupId => 24,
            ErrorCode::UnknownMemberId => 25,
            ErrorCode::InvalidSessionTimeout => 26,
            ErrorCode::RebalanceInProgress => 27,
            ErrorCode::InvalidCommitOffsetSize => 28,
            ErrorCode::TopicAuthorizationFailed => 29,
            ErrorCode::GroupAuthorizationFailed => 30,
            ErrorCode::ClusterAuthorizationFailed => 31,
            ErrorCode::InvalidTimestamp => 32,
            ErrorCode::UnsupportedSaslMechanism => 33,
            ErrorCode::IllegalSaslState => 34,
            ErrorCode::UnsupportedVersion => 35,
            ErrorCode::TopicAlreadyExists => 36,
            ErrorCode::InvalidPartitions => 37,
            ErrorCode::InvalidReplicationFactor => 38,
            ErrorCode::InvalidReplicaAssignment => 39,
            ErrorCode::InvalidConfig => 40,
            ErrorCode::NotController => 41,
            ErrorCode::InvalidRequest => 42,
            ErrorCode::UnsupportedForMessageFormat => 43,
            ErrorCode::PolicyViolation => 44,
            ErrorCode::OutOfOrderSequenceNumber => 45,
            ErrorCode::DuplicateSequenceNumber => 46,
            ErrorCode::InvalidProducerEpoch => 47,
            ErrorCode::InvalidTxnState => 48,
            ErrorCode::InvalidProducerIdMapping => 49,
            ErrorCode::InvalidTransactionTimeout => 50,
            ErrorCode::ConcurrentTransactions => 51,
            ErrorCode::TransactionCoordinatorFenced => 52,
            ErrorCode::TransactionalIdAuthorizationFailed => 53,
            ErrorCode::SecurityDisabled => 54,
            ErrorCode::OperationNotAttempted => 55,
            ErrorCode::KafkaStorageError => 56,
            ErrorCode::LogDirNotFound => 57,
            ErrorCode::SaslAuthenticationFailed => 58,
            ErrorCode::UnknownProducerId => 59,
            ErrorCode::ReassignmentInProgress => 60,
            ErrorCode::DelegationTokenAuthDisabled => 61,
            ErrorCode::DelegationTokenNotFound => 62,
            ErrorCode::DelegationTokenOwnerMismatch => 63,
            ErrorCode::DelegationTokenRequestNotAllowed => 64,
            ErrorCode::DelegationTokenAuthorizationFailed => 65,
            ErrorCode::DelegationTokenExpired => 66,
            ErrorCode::InvalidPrincipalType => 67,
            ErrorCode::NonEmptyGroup => 68,
            ErrorCode::GroupIdNotFound => 69,
            ErrorCode::FetchSessionIdNotFound => 70,
            ErrorCode::InvalidFetchSessionEpoch => 71,
            ErrorCode::ListenerNotFound => 72,
            ErrorCode::TopicDeletionDisabled => 73,
            ErrorCode::FencedLeaderEpoch => 74,
            ErrorCode::UnknownLeaderEpoch => 75,
            ErrorCode::UnsupportedCompressionType => 76,
            ErrorCode::StaleBrokerEpoch => 77,
            ErrorCode::OffsetNotAvailable => 78,
            ErrorCode::MemberIdRequired => 79,
            ErrorCode::PreferredLeaderNotAvailable => 80,
            ErrorCode::GroupMaxSizeReached => 81,
            ErrorCode::FencedInstanceId => 82,
            ErrorCode::EligibleLeadersNotAvailable => 83,
            ErrorCode::ElectionNotNeeded => 84,
            ErrorCode::NoReassignmentInProgress => 85,
            ErrorCode::GroupSubscribedToTopic => 86,
            ErrorCode::InvalidRecord => 87,
            ErrorCode::UnstableOffsetCommit => 88,
            ErrorCode::ThrottlingQuotaExceeded => 89,
            ErrorCode::ProducerFenced => 90,
            ErrorCode::ResourceNotFound => 91,
            ErrorCode::DuplicateResource => 92,
            ErrorCode::UnacceptableCredential => 93,
            ErrorCode::InconsistentVoterSet => 94,
            ErrorCode::InvalidUpdateVersion => 95,
            ErrorCode::FeatureUpdateFailed => 96,
            ErrorCode::PrincipalDeserializationFailure => 97,
            ErrorCode::SnapshotNotFound => 98,
            ErrorCode::PositionOutOfRange => 99,
            ErrorCode::UnknownTopicId => 100,
            ErrorCode::DuplicateBrokerRegistration => 101,
            ErrorCode::BrokerIdNotRegistered => 102,
            ErrorCode::InconsistentTopicId => 103,
            ErrorCode::InconsistentClusterId => 104,
            ErrorCode::TransactionalIdNotFound => 105,
            ErrorCode::FetchSessionTopicIdError => 106,
            ErrorCode::IneligibleReplica => 107,
            ErrorCode::NewLeaderElected => 108,
            ErrorCode::OffsetMovedToTieredStorage => 109,
            ErrorCode::FencedMemberEpoch => 110,
            ErrorCode::UnreleasedInstanceId => 111,
            ErrorCode::UnsupportedAssignor => 112,
            ErrorCode::StaleMemberEpoch => 113,
            ErrorCode::MismatchedEndpointType => 114,
            ErrorCode::UnsupportedEndpointType => 115,
            ErrorCode::UnknownControllerId => 116,
            ErrorCode::UnknownSubscriptionId => 117,
            ErrorCode::TelemetryTooLarge => 118,
            ErrorCode::InvalidRegistration => 119,
            ErrorCode::TransactionAbortable => 120,
            ErrorCode::InvalidRecordState => 121,
            ErrorCode::ShareSessionNotFound => 122,
            ErrorCode::InvalidShareSessionEpoch => 123,
            ErrorCode::FencedStateEpoch => 124,
            ErrorCode::InvalidVoterKey => 125,
            ErrorCode::DuplicateVoter => 126,
            ErrorCode::VoterNotFound => 127,
            ErrorCode::InvalidRegularExpression => 128,
            ErrorCode::RebootstrapRequired => 129,
            ErrorCode::StreamsInvalidTopology => 130,
            ErrorCode::StreamsInvalidTopologyEpoch => 131,
            ErrorCode::StreamsTopologyFenced => 132,
            ErrorCode::ShareSessionLimitReached => 133,
            ErrorCode::Unknown(c) => *c,
        }
    }
    /// Returns `true` if the operation that produced this error can be retried.
    ///
    /// The retriable set matches the Kafka Java client's `RetriableException` hierarchy.
    /// [`ErrorCode::Unknown`] is **not** retriable (matches Java client behaviour).
    pub fn is_retriable(&self) -> bool {
        match self {
            ErrorCode::CorruptMessage => true,
            ErrorCode::UnknownTopicOrPartition => true,
            ErrorCode::LeaderNotAvailable => true,
            ErrorCode::NotLeaderOrFollower => true,
            ErrorCode::RequestTimedOut => true,
            ErrorCode::ReplicaNotAvailable => true,
            ErrorCode::StaleControllerEpoch => true,
            ErrorCode::NetworkException => true,
            ErrorCode::CoordinatorLoadInProgress => true,
            ErrorCode::CoordinatorNotAvailable => true,
            ErrorCode::NotCoordinator => true,
            ErrorCode::NotEnoughReplicas => true,
            ErrorCode::NotEnoughReplicasAfterAppend => true,
            ErrorCode::RebalanceInProgress => true,
            ErrorCode::NotController => true,
            ErrorCode::FetchSessionIdNotFound => true,
            ErrorCode::InvalidFetchSessionEpoch => true,
            ErrorCode::OffsetNotAvailable => true,
            ErrorCode::PreferredLeaderNotAvailable => true,
            ErrorCode::EligibleLeadersNotAvailable => true,
            ErrorCode::ElectionNotNeeded => true,
            ErrorCode::NoReassignmentInProgress => true,
            ErrorCode::UnstableOffsetCommit => true,
            ErrorCode::ThrottlingQuotaExceeded => true,
            ErrorCode::UnknownTopicId => true,
            ErrorCode::FetchSessionTopicIdError => true,
            ErrorCode::NewLeaderElected => true,
            ErrorCode::OffsetMovedToTieredStorage => true,
            ErrorCode::FencedMemberEpoch => true,
            ErrorCode::UnreleasedInstanceId => true,
            ErrorCode::StaleMemberEpoch => true,
            ErrorCode::ShareSessionNotFound => true,
            ErrorCode::InvalidShareSessionEpoch => true,
            ErrorCode::RebootstrapRequired => true,
            _ => false,
        }
    }
    /// Returns `true` if this represents an error condition.
    ///
    /// Only [`ErrorCode::None`] (code 0) returns `false`.
    pub fn is_error(&self) -> bool {
        !matches!(self, ErrorCode::None)
    }
}
impl From<i16> for ErrorCode {
    fn from(code: i16) -> Self {
        match code {
            -1 => ErrorCode::UnknownServerError,
            0 => ErrorCode::None,
            1 => ErrorCode::OffsetOutOfRange,
            2 => ErrorCode::CorruptMessage,
            3 => ErrorCode::UnknownTopicOrPartition,
            4 => ErrorCode::InvalidFetchSize,
            5 => ErrorCode::LeaderNotAvailable,
            6 => ErrorCode::NotLeaderOrFollower,
            7 => ErrorCode::RequestTimedOut,
            8 => ErrorCode::BrokerNotAvailable,
            9 => ErrorCode::ReplicaNotAvailable,
            10 => ErrorCode::MessageTooLarge,
            11 => ErrorCode::StaleControllerEpoch,
            12 => ErrorCode::OffsetMetadataTooLarge,
            13 => ErrorCode::NetworkException,
            14 => ErrorCode::CoordinatorLoadInProgress,
            15 => ErrorCode::CoordinatorNotAvailable,
            16 => ErrorCode::NotCoordinator,
            17 => ErrorCode::InvalidTopicException,
            18 => ErrorCode::RecordListTooLarge,
            19 => ErrorCode::NotEnoughReplicas,
            20 => ErrorCode::NotEnoughReplicasAfterAppend,
            21 => ErrorCode::InvalidRequiredAcks,
            22 => ErrorCode::IllegalGeneration,
            23 => ErrorCode::InconsistentGroupProtocol,
            24 => ErrorCode::InvalidGroupId,
            25 => ErrorCode::UnknownMemberId,
            26 => ErrorCode::InvalidSessionTimeout,
            27 => ErrorCode::RebalanceInProgress,
            28 => ErrorCode::InvalidCommitOffsetSize,
            29 => ErrorCode::TopicAuthorizationFailed,
            30 => ErrorCode::GroupAuthorizationFailed,
            31 => ErrorCode::ClusterAuthorizationFailed,
            32 => ErrorCode::InvalidTimestamp,
            33 => ErrorCode::UnsupportedSaslMechanism,
            34 => ErrorCode::IllegalSaslState,
            35 => ErrorCode::UnsupportedVersion,
            36 => ErrorCode::TopicAlreadyExists,
            37 => ErrorCode::InvalidPartitions,
            38 => ErrorCode::InvalidReplicationFactor,
            39 => ErrorCode::InvalidReplicaAssignment,
            40 => ErrorCode::InvalidConfig,
            41 => ErrorCode::NotController,
            42 => ErrorCode::InvalidRequest,
            43 => ErrorCode::UnsupportedForMessageFormat,
            44 => ErrorCode::PolicyViolation,
            45 => ErrorCode::OutOfOrderSequenceNumber,
            46 => ErrorCode::DuplicateSequenceNumber,
            47 => ErrorCode::InvalidProducerEpoch,
            48 => ErrorCode::InvalidTxnState,
            49 => ErrorCode::InvalidProducerIdMapping,
            50 => ErrorCode::InvalidTransactionTimeout,
            51 => ErrorCode::ConcurrentTransactions,
            52 => ErrorCode::TransactionCoordinatorFenced,
            53 => ErrorCode::TransactionalIdAuthorizationFailed,
            54 => ErrorCode::SecurityDisabled,
            55 => ErrorCode::OperationNotAttempted,
            56 => ErrorCode::KafkaStorageError,
            57 => ErrorCode::LogDirNotFound,
            58 => ErrorCode::SaslAuthenticationFailed,
            59 => ErrorCode::UnknownProducerId,
            60 => ErrorCode::ReassignmentInProgress,
            61 => ErrorCode::DelegationTokenAuthDisabled,
            62 => ErrorCode::DelegationTokenNotFound,
            63 => ErrorCode::DelegationTokenOwnerMismatch,
            64 => ErrorCode::DelegationTokenRequestNotAllowed,
            65 => ErrorCode::DelegationTokenAuthorizationFailed,
            66 => ErrorCode::DelegationTokenExpired,
            67 => ErrorCode::InvalidPrincipalType,
            68 => ErrorCode::NonEmptyGroup,
            69 => ErrorCode::GroupIdNotFound,
            70 => ErrorCode::FetchSessionIdNotFound,
            71 => ErrorCode::InvalidFetchSessionEpoch,
            72 => ErrorCode::ListenerNotFound,
            73 => ErrorCode::TopicDeletionDisabled,
            74 => ErrorCode::FencedLeaderEpoch,
            75 => ErrorCode::UnknownLeaderEpoch,
            76 => ErrorCode::UnsupportedCompressionType,
            77 => ErrorCode::StaleBrokerEpoch,
            78 => ErrorCode::OffsetNotAvailable,
            79 => ErrorCode::MemberIdRequired,
            80 => ErrorCode::PreferredLeaderNotAvailable,
            81 => ErrorCode::GroupMaxSizeReached,
            82 => ErrorCode::FencedInstanceId,
            83 => ErrorCode::EligibleLeadersNotAvailable,
            84 => ErrorCode::ElectionNotNeeded,
            85 => ErrorCode::NoReassignmentInProgress,
            86 => ErrorCode::GroupSubscribedToTopic,
            87 => ErrorCode::InvalidRecord,
            88 => ErrorCode::UnstableOffsetCommit,
            89 => ErrorCode::ThrottlingQuotaExceeded,
            90 => ErrorCode::ProducerFenced,
            91 => ErrorCode::ResourceNotFound,
            92 => ErrorCode::DuplicateResource,
            93 => ErrorCode::UnacceptableCredential,
            94 => ErrorCode::InconsistentVoterSet,
            95 => ErrorCode::InvalidUpdateVersion,
            96 => ErrorCode::FeatureUpdateFailed,
            97 => ErrorCode::PrincipalDeserializationFailure,
            98 => ErrorCode::SnapshotNotFound,
            99 => ErrorCode::PositionOutOfRange,
            100 => ErrorCode::UnknownTopicId,
            101 => ErrorCode::DuplicateBrokerRegistration,
            102 => ErrorCode::BrokerIdNotRegistered,
            103 => ErrorCode::InconsistentTopicId,
            104 => ErrorCode::InconsistentClusterId,
            105 => ErrorCode::TransactionalIdNotFound,
            106 => ErrorCode::FetchSessionTopicIdError,
            107 => ErrorCode::IneligibleReplica,
            108 => ErrorCode::NewLeaderElected,
            109 => ErrorCode::OffsetMovedToTieredStorage,
            110 => ErrorCode::FencedMemberEpoch,
            111 => ErrorCode::UnreleasedInstanceId,
            112 => ErrorCode::UnsupportedAssignor,
            113 => ErrorCode::StaleMemberEpoch,
            114 => ErrorCode::MismatchedEndpointType,
            115 => ErrorCode::UnsupportedEndpointType,
            116 => ErrorCode::UnknownControllerId,
            117 => ErrorCode::UnknownSubscriptionId,
            118 => ErrorCode::TelemetryTooLarge,
            119 => ErrorCode::InvalidRegistration,
            120 => ErrorCode::TransactionAbortable,
            121 => ErrorCode::InvalidRecordState,
            122 => ErrorCode::ShareSessionNotFound,
            123 => ErrorCode::InvalidShareSessionEpoch,
            124 => ErrorCode::FencedStateEpoch,
            125 => ErrorCode::InvalidVoterKey,
            126 => ErrorCode::DuplicateVoter,
            127 => ErrorCode::VoterNotFound,
            128 => ErrorCode::InvalidRegularExpression,
            129 => ErrorCode::RebootstrapRequired,
            130 => ErrorCode::StreamsInvalidTopology,
            131 => ErrorCode::StreamsInvalidTopologyEpoch,
            132 => ErrorCode::StreamsTopologyFenced,
            133 => ErrorCode::ShareSessionLimitReached,
            other => ErrorCode::Unknown(other),
        }
    }
}
impl From<ErrorCode> for i16 {
    fn from(error: ErrorCode) -> Self {
        error.code()
    }
}
impl std::fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ErrorCode::UnknownServerError => f.write_str(
                "The server experienced an unexpected error when processing the request.",
            ),
            ErrorCode::None => f.write_str("NONE"),
            ErrorCode::OffsetOutOfRange => f.write_str(
                "The requested offset is not within the range of offsets maintained by the server.",
            ),
            ErrorCode::CorruptMessage => f.write_str(
                "This message has failed its CRC checksum, exceeds the valid size, has a null key \
                 for a compacted topic, or is otherwise corrupt.",
            ),
            ErrorCode::UnknownTopicOrPartition => {
                f.write_str("This server does not host this topic-partition.")
            },
            ErrorCode::InvalidFetchSize => f.write_str("The requested fetch size is invalid."),
            ErrorCode::LeaderNotAvailable => f.write_str(
                "There is no leader for this topic-partition as we are in the middle of a \
                 leadership election.",
            ),
            ErrorCode::NotLeaderOrFollower => f.write_str(
                "For requests intended only for the leader, this error indicates that the broker \
                 is not the current leader. For requests intended for any replica, this error \
                 indicates that the broker is not a replica of the topic partition.",
            ),
            ErrorCode::RequestTimedOut => f.write_str("The request timed out."),
            ErrorCode::BrokerNotAvailable => f.write_str("The broker is not available."),
            ErrorCode::ReplicaNotAvailable => f.write_str(
                "The replica is not available for the requested topic-partition. Produce/Fetch \
                 requests and other requests intended only for the leader or follower return \
                 NOT_LEADER_OR_FOLLOWER if the broker is not a replica of the topic-partition.",
            ),
            ErrorCode::MessageTooLarge => f.write_str(
                "The request included a message larger than the max message size the server will \
                 accept.",
            ),
            ErrorCode::StaleControllerEpoch => {
                f.write_str("The controller moved to another broker.")
            },
            ErrorCode::OffsetMetadataTooLarge => {
                f.write_str("The metadata field of the offset request was too large.")
            },
            ErrorCode::NetworkException => {
                f.write_str("The server disconnected before a response was received.")
            },
            ErrorCode::CoordinatorLoadInProgress => {
                f.write_str("The coordinator is loading and hence can't process requests.")
            },
            ErrorCode::CoordinatorNotAvailable => f.write_str("The coordinator is not available."),
            ErrorCode::NotCoordinator => f.write_str("This is not the correct coordinator."),
            ErrorCode::InvalidTopicException => {
                f.write_str("The request attempted to perform an operation on an invalid topic.")
            },
            ErrorCode::RecordListTooLarge => f.write_str(
                "The request included message batch larger than the configured segment size on \
                 the server.",
            ),
            ErrorCode::NotEnoughReplicas => f.write_str(
                "Messages are rejected since there are fewer in-sync replicas than required.",
            ),
            ErrorCode::NotEnoughReplicasAfterAppend => f.write_str(
                "Messages are written to the log, but to fewer in-sync replicas than required.",
            ),
            ErrorCode::InvalidRequiredAcks => {
                f.write_str("Produce request specified an invalid value for required acks.")
            },
            ErrorCode::IllegalGeneration => {
                f.write_str("Specified group generation id is not valid.")
            },
            ErrorCode::InconsistentGroupProtocol => f.write_str(
                "The group member's supported protocols are incompatible with those of existing \
                 members or first group member tried to join with empty protocol type or empty \
                 protocol list.",
            ),
            ErrorCode::InvalidGroupId => f.write_str("The group id is invalid."),
            ErrorCode::UnknownMemberId => {
                f.write_str("The coordinator is not aware of this member.")
            },
            ErrorCode::InvalidSessionTimeout => f.write_str(
                "The session timeout is not within the range allowed by the broker (as configured \
                 by group.min.session.timeout.ms and group.max.session.timeout.ms).",
            ),
            ErrorCode::RebalanceInProgress => {
                f.write_str("The group is rebalancing, so a rejoin is needed.")
            },
            ErrorCode::InvalidCommitOffsetSize => {
                f.write_str("The committing offset data size is not valid.")
            },
            ErrorCode::TopicAuthorizationFailed => f.write_str("Topic authorization failed."),
            ErrorCode::GroupAuthorizationFailed => f.write_str("Group authorization failed."),
            ErrorCode::ClusterAuthorizationFailed => f.write_str("Cluster authorization failed."),
            ErrorCode::InvalidTimestamp => {
                f.write_str("The timestamp of the message is out of acceptable range.")
            },
            ErrorCode::UnsupportedSaslMechanism => {
                f.write_str("The broker does not support the requested SASL mechanism.")
            },
            ErrorCode::IllegalSaslState => {
                f.write_str("Request is not valid given the current SASL state.")
            },
            ErrorCode::UnsupportedVersion => f.write_str("The version of API is not supported."),
            ErrorCode::TopicAlreadyExists => f.write_str("Topic with this name already exists."),
            ErrorCode::InvalidPartitions => f.write_str("Number of partitions is below 1."),
            ErrorCode::InvalidReplicationFactor => f.write_str(
                "Replication factor is below 1 or larger than the number of available brokers.",
            ),
            ErrorCode::InvalidReplicaAssignment => f.write_str("Replica assignment is invalid."),
            ErrorCode::InvalidConfig => f.write_str("Configuration is invalid."),
            ErrorCode::NotController => {
                f.write_str("This is not the correct controller for this cluster.")
            },
            ErrorCode::InvalidRequest => f.write_str(
                "This most likely occurs because of a request being malformed by the client \
                 library or the message was sent to an incompatible broker. See the broker logs \
                 for more details.",
            ),
            ErrorCode::UnsupportedForMessageFormat => f.write_str(
                "The message format version on the broker does not support the request.",
            ),
            ErrorCode::PolicyViolation => {
                f.write_str("Request parameters do not satisfy the configured policy.")
            },
            ErrorCode::OutOfOrderSequenceNumber => {
                f.write_str("The broker received an out of order sequence number.")
            },
            ErrorCode::DuplicateSequenceNumber => {
                f.write_str("The broker received a duplicate sequence number.")
            },
            ErrorCode::InvalidProducerEpoch => {
                f.write_str("Producer attempted to produce with an old epoch.")
            },
            ErrorCode::InvalidTxnState => {
                f.write_str("The producer attempted a transactional operation in an invalid state.")
            },
            ErrorCode::InvalidProducerIdMapping => f.write_str(
                "The producer attempted to use a producer id which is not currently assigned to \
                 its transactional id.",
            ),
            ErrorCode::InvalidTransactionTimeout => f.write_str(
                "The transaction timeout is larger than the maximum value allowed by the broker \
                 (as configured by transaction.max.timeout.ms).",
            ),
            ErrorCode::ConcurrentTransactions => f.write_str(
                "The producer attempted to update a transaction while another concurrent \
                 operation on the same transaction was ongoing.",
            ),
            ErrorCode::TransactionCoordinatorFenced => f.write_str(
                "Indicates that the transaction coordinator sending a WriteTxnMarker is no longer \
                 the current coordinator for a given producer.",
            ),
            ErrorCode::TransactionalIdAuthorizationFailed => {
                f.write_str("Transactional Id authorization failed.")
            },
            ErrorCode::SecurityDisabled => f.write_str("Security features are disabled."),
            ErrorCode::OperationNotAttempted => f.write_str(
                "The broker did not attempt to execute this operation. This may happen for \
                 batched RPCs where some operations in the batch failed, causing the broker to \
                 respond without trying the rest.",
            ),
            ErrorCode::KafkaStorageError => {
                f.write_str("Disk error when trying to access log file on the disk.")
            },
            ErrorCode::LogDirNotFound => {
                f.write_str("The user-specified log directory is not found in the broker config.")
            },
            ErrorCode::SaslAuthenticationFailed => f.write_str("SASL Authentication failed."),
            ErrorCode::UnknownProducerId => f.write_str(
                "This exception is raised by the broker if it could not locate the producer \
                 metadata associated with the producerId in question. This could happen if, for \
                 instance, the producer's records were deleted because their retention time had \
                 elapsed. Once the last records of the producerId are removed, the producer's \
                 metadata is removed from the broker, and future appends by the producer will \
                 return this exception.",
            ),
            ErrorCode::ReassignmentInProgress => {
                f.write_str("A partition reassignment is in progress.")
            },
            ErrorCode::DelegationTokenAuthDisabled => {
                f.write_str("Delegation Token feature is not enabled.")
            },
            ErrorCode::DelegationTokenNotFound => {
                f.write_str("Delegation Token is not found on server.")
            },
            ErrorCode::DelegationTokenOwnerMismatch => {
                f.write_str("Specified Principal is not valid Owner/Renewer.")
            },
            ErrorCode::DelegationTokenRequestNotAllowed => f.write_str(
                "Delegation Token requests are not allowed on PLAINTEXT/1-way SSL channels and on \
                 delegation token authenticated channels.",
            ),
            ErrorCode::DelegationTokenAuthorizationFailed => {
                f.write_str("Delegation Token authorization failed.")
            },
            ErrorCode::DelegationTokenExpired => f.write_str("Delegation Token is expired."),
            ErrorCode::InvalidPrincipalType => {
                f.write_str("Supplied principalType is not supported.")
            },
            ErrorCode::NonEmptyGroup => f.write_str("The group is not empty."),
            ErrorCode::GroupIdNotFound => f.write_str("The group id does not exist."),
            ErrorCode::FetchSessionIdNotFound => f.write_str("The fetch session ID was not found."),
            ErrorCode::InvalidFetchSessionEpoch => {
                f.write_str("The fetch session epoch is invalid.")
            },
            ErrorCode::ListenerNotFound => f.write_str(
                "There is no listener on the leader broker that matches the listener on which \
                 metadata request was processed.",
            ),
            ErrorCode::TopicDeletionDisabled => f.write_str("Topic deletion is disabled."),
            ErrorCode::FencedLeaderEpoch => f.write_str(
                "The leader epoch in the request is older than the epoch on the broker.",
            ),
            ErrorCode::UnknownLeaderEpoch => f.write_str(
                "The leader epoch in the request is newer than the epoch on the broker.",
            ),
            ErrorCode::UnsupportedCompressionType => f.write_str(
                "The requesting client does not support the compression type of given partition.",
            ),
            ErrorCode::StaleBrokerEpoch => f.write_str("Broker epoch has changed."),
            ErrorCode::OffsetNotAvailable => f.write_str(
                "The leader high watermark has not caught up from a recent leader election so the \
                 offsets cannot be guaranteed to be monotonically increasing.",
            ),
            ErrorCode::MemberIdRequired => f.write_str(
                "The group member needs to have a valid member id before actually entering a \
                 consumer group.",
            ),
            ErrorCode::PreferredLeaderNotAvailable => {
                f.write_str("The preferred leader was not available.")
            },
            ErrorCode::GroupMaxSizeReached => {
                f.write_str("The group has reached its maximum size.")
            },
            ErrorCode::FencedInstanceId => f.write_str(
                "The broker rejected this static consumer since another consumer with the same \
                 group.instance.id has registered with a different member.id.",
            ),
            ErrorCode::EligibleLeadersNotAvailable => {
                f.write_str("Eligible topic partition leaders are not available.")
            },
            ErrorCode::ElectionNotNeeded => {
                f.write_str("Leader election not needed for topic partition.")
            },
            ErrorCode::NoReassignmentInProgress => {
                f.write_str("No partition reassignment is in progress.")
            },
            ErrorCode::GroupSubscribedToTopic => f.write_str(
                "Deleting offsets of a topic is forbidden while the consumer group is actively \
                 subscribed to it.",
            ),
            ErrorCode::InvalidRecord => f.write_str(
                "This record has failed the validation on broker and hence will be rejected.",
            ),
            ErrorCode::UnstableOffsetCommit => {
                f.write_str("There are unstable offsets that need to be cleared.")
            },
            ErrorCode::ThrottlingQuotaExceeded => {
                f.write_str("The throttling quota has been exceeded.")
            },
            ErrorCode::ProducerFenced => f.write_str(
                "There is a newer producer with the same transactionalId which fences the current \
                 one.",
            ),
            ErrorCode::ResourceNotFound => {
                f.write_str("A request illegally referred to a resource that does not exist.")
            },
            ErrorCode::DuplicateResource => {
                f.write_str("A request illegally referred to the same resource twice.")
            },
            ErrorCode::UnacceptableCredential => {
                f.write_str("Requested credential would not meet criteria for acceptability.")
            },
            ErrorCode::InconsistentVoterSet => f.write_str(
                "Indicates that the either the sender or recipient of a voter-only request is not \
                 one of the expected voters.",
            ),
            ErrorCode::InvalidUpdateVersion => f.write_str("The given update version was invalid."),
            ErrorCode::FeatureUpdateFailed => f.write_str(
                "Unable to update finalized features due to an unexpected server error.",
            ),
            ErrorCode::PrincipalDeserializationFailure => f.write_str(
                "Request principal deserialization failed during forwarding. This indicates an \
                 internal error on the broker cluster security setup.",
            ),
            ErrorCode::SnapshotNotFound => f.write_str("Requested snapshot was not found."),
            ErrorCode::PositionOutOfRange => f.write_str(
                "Requested position is not greater than or equal to zero, and less than the size \
                 of the snapshot.",
            ),
            ErrorCode::UnknownTopicId => f.write_str("This server does not host this topic ID."),
            ErrorCode::DuplicateBrokerRegistration => {
                f.write_str("This broker ID is already in use.")
            },
            ErrorCode::BrokerIdNotRegistered => {
                f.write_str("The given broker ID was not registered.")
            },
            ErrorCode::InconsistentTopicId => {
                f.write_str("The log's topic ID did not match the topic ID in the request.")
            },
            ErrorCode::InconsistentClusterId => {
                f.write_str("The clusterId in the request does not match that found on the server.")
            },
            ErrorCode::TransactionalIdNotFound => {
                f.write_str("The transactionalId could not be found.")
            },
            ErrorCode::FetchSessionTopicIdError => {
                f.write_str("The fetch session encountered inconsistent topic ID usage.")
            },
            ErrorCode::IneligibleReplica => {
                f.write_str("The new ISR contains at least one ineligible replica.")
            },
            ErrorCode::NewLeaderElected => f.write_str(
                "The AlterPartition request successfully updated the partition state but the \
                 leader has changed.",
            ),
            ErrorCode::OffsetMovedToTieredStorage => {
                f.write_str("The requested offset is moved to tiered storage.")
            },
            ErrorCode::FencedMemberEpoch => f.write_str(
                "The member epoch is fenced by the group coordinator. The member must abandon all \
                 its partitions and rejoin.",
            ),
            ErrorCode::UnreleasedInstanceId => f.write_str(
                "The instance ID is still used by another member in the consumer group. That \
                 member must leave first.",
            ),
            ErrorCode::UnsupportedAssignor => f.write_str(
                "The assignor or its version range is not supported by the consumer group.",
            ),
            ErrorCode::StaleMemberEpoch => f.write_str(
                "The member epoch is stale. The member must retry after receiving its updated \
                 member epoch via the ConsumerGroupHeartbeat API.",
            ),
            ErrorCode::MismatchedEndpointType => {
                f.write_str("The request was sent to an endpoint of the wrong type.")
            },
            ErrorCode::UnsupportedEndpointType => {
                f.write_str("This endpoint type is not supported yet.")
            },
            ErrorCode::UnknownControllerId => f.write_str("This controller ID is not known."),
            ErrorCode::UnknownSubscriptionId => f.write_str(
                "Client sent a push telemetry request with an invalid or outdated subscription ID.",
            ),
            ErrorCode::TelemetryTooLarge => f.write_str(
                "Client sent a push telemetry request larger than the maximum size the broker \
                 will accept.",
            ),
            ErrorCode::InvalidRegistration => {
                f.write_str("The controller has considered the broker registration to be invalid.")
            },
            ErrorCode::TransactionAbortable => f.write_str(
                "The server encountered an error with the transaction. The client can abort the \
                 transaction to continue using this transactional ID.",
            ),
            ErrorCode::InvalidRecordState => f.write_str(
                "The record state is invalid. The acknowledgement of delivery could not be \
                 completed.",
            ),
            ErrorCode::ShareSessionNotFound => f.write_str("The share session was not found."),
            ErrorCode::InvalidShareSessionEpoch => {
                f.write_str("The share session epoch is invalid.")
            },
            ErrorCode::FencedStateEpoch => f.write_str(
                "The share coordinator rejected the request because the share-group state epoch \
                 did not match.",
            ),
            ErrorCode::InvalidVoterKey => {
                f.write_str("The voter key doesn't match the receiving replica's key.")
            },
            ErrorCode::DuplicateVoter => {
                f.write_str("The voter is already part of the set of voters.")
            },
            ErrorCode::VoterNotFound => f.write_str("The voter is not part of the set of voters."),
            ErrorCode::InvalidRegularExpression => {
                f.write_str("The regular expression is not valid.")
            },
            ErrorCode::RebootstrapRequired => f.write_str(
                "Client metadata is stale. The client should rebootstrap to obtain new metadata.",
            ),
            ErrorCode::StreamsInvalidTopology => f.write_str("The supplied topology is invalid."),
            ErrorCode::StreamsInvalidTopologyEpoch => {
                f.write_str("The supplied topology epoch is invalid.")
            },
            ErrorCode::StreamsTopologyFenced => {
                f.write_str("The supplied topology epoch is outdated.")
            },
            ErrorCode::ShareSessionLimitReached => {
                f.write_str("The limit of share sessions has been reached.")
            },
            ErrorCode::Unknown(code) => write!(f, "Unknown error code: {}", code),
        }
    }
}
