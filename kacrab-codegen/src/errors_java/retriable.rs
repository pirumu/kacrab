//! Static table of Kafka exception classes that extend `RetriableException`.

/// Exception class names whose Java classes extend `RetriableException`.
///
/// Derived from the Kafka 4.3.0 exception hierarchy. Used to flag emitted
/// [`crate::errors_java::ErrorEntry`] variants as retriable.
pub(crate) const RETRIABLE_EXCEPTIONS: &[&str] = &[
    "CoordinatorLoadInProgressException",
    "CoordinatorNotAvailableException",
    "CorruptRecordException",
    "EligibleLeadersNotAvailableException",
    "ElectionNotNeededException",
    "FencedMemberEpochException",
    "FetchSessionIdNotFoundException",
    "FetchSessionTopicIdException",
    "InvalidFetchSessionEpochException",
    "InvalidMetadataException",
    "LeaderNotAvailableException",
    "NetworkException",
    "NewLeaderElectedException",
    "NoReassignmentInProgressException",
    "NotControllerException",
    "NotCoordinatorException",
    "NotEnoughReplicasAfterAppendException",
    "NotEnoughReplicasException",
    "NotLeaderOrFollowerException",
    "OffsetMovedToTieredStorageException",
    "OffsetNotAvailableException",
    "PreferredLeaderNotAvailableException",
    "RebalanceInProgressException",
    "RebootstrapRequiredException",
    "ReplicaNotAvailableException",
    "StaleControllerEpochException",
    "ControllerMovedException",
    "ThrottlingQuotaExceededException",
    "UnknownTopicIdException",
    "UnknownTopicOrPartitionException",
    "UnstableOffsetCommitException",
    "UnreleasedInstanceIdException",
    "ShareSessionNotFoundException",
    "InvalidShareSessionEpochException",
    "TimeoutException",
    "StaleMemberEpochException",
];

/// Returns `true` if `exception` (when present) extends `RetriableException`.
pub(crate) fn is_retriable(exception: Option<&str>) -> bool {
    exception.is_some_and(|name| RETRIABLE_EXCEPTIONS.contains(&name))
}
