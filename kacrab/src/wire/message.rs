//! Generated request/response adapter traits for broker sessions.

use std::borrow::Cow;

use bytes::{Bytes, BytesMut};
use kacrab_protocol::{
    KafkaString, KafkaUuid, Result,
    generated::{
        AddOffsetsToTxnRequestData, AddOffsetsToTxnResponseData, AddPartitionsToTxnRequestData,
        AddPartitionsToTxnResponseData, AddRaftVoterRequestData, AddRaftVoterResponseData,
        AlterClientQuotasRequestData, AlterClientQuotasResponseData, AlterConfigsRequestData,
        AlterConfigsResponseData, AlterPartitionReassignmentsRequestData,
        AlterPartitionReassignmentsResponseData, AlterReplicaLogDirsRequestData,
        AlterReplicaLogDirsResponseData, AlterShareGroupOffsetsRequestData,
        AlterShareGroupOffsetsResponseData, AlterUserScramCredentialsRequestData,
        AlterUserScramCredentialsResponseData, ApiVersionsRequestData, ApiVersionsResponseData,
        ConsumerGroupDescribeRequestData, ConsumerGroupDescribeResponseData,
        ConsumerGroupHeartbeatRequestData, ConsumerGroupHeartbeatResponseData,
        CreateAclsRequestData, CreateAclsResponseData, CreateDelegationTokenRequestData,
        CreateDelegationTokenResponseData, CreatePartitionsRequestData,
        CreatePartitionsResponseData, CreateTopicsRequestData, CreateTopicsResponseData,
        DeleteAclsRequestData, DeleteAclsResponseData, DeleteGroupsRequestData,
        DeleteGroupsResponseData, DeleteRecordsRequestData, DeleteRecordsResponseData,
        DeleteShareGroupOffsetsRequestData, DeleteShareGroupOffsetsResponseData,
        DeleteTopicsRequestData, DeleteTopicsResponseData, DescribeAclsRequestData,
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
        ElectLeadersRequestData, ElectLeadersResponseData, EndTxnRequestData, EndTxnResponseData,
        ExpireDelegationTokenRequestData, ExpireDelegationTokenResponseData, FetchRequestData,
        FetchResponseData, FindCoordinatorRequestData, FindCoordinatorResponseData,
        GetTelemetrySubscriptionsRequestData, GetTelemetrySubscriptionsResponseData,
        HeartbeatRequestData, HeartbeatResponseData, IncrementalAlterConfigsRequestData,
        IncrementalAlterConfigsResponseData, InitProducerIdRequestData, InitProducerIdResponseData,
        JoinGroupRequestData, JoinGroupResponseData, LeaveGroupRequestData, LeaveGroupResponseData,
        ListConfigResourcesRequestData, ListConfigResourcesResponseData, ListGroupsRequestData,
        ListGroupsResponseData, ListOffsetsRequestData, ListOffsetsResponseData,
        ListPartitionReassignmentsRequestData, ListPartitionReassignmentsResponseData,
        ListTransactionsRequestData, ListTransactionsResponseData, MetadataRequestData,
        MetadataResponseData, OffsetCommitRequestData, OffsetCommitResponseData,
        OffsetDeleteRequestData, OffsetDeleteResponseData, OffsetFetchRequestData,
        OffsetFetchResponseData, OffsetForLeaderEpochRequestData, OffsetForLeaderEpochResponseData,
        ProduceRequestData, ProduceResponseData, PushTelemetryRequestData,
        PushTelemetryResponseData, RemoveRaftVoterRequestData, RemoveRaftVoterResponseData,
        RenewDelegationTokenRequestData, RenewDelegationTokenResponseData,
        ShareGroupDescribeRequestData, ShareGroupDescribeResponseData,
        StreamsGroupDescribeRequestData, StreamsGroupDescribeResponseData, SyncGroupRequestData,
        SyncGroupResponseData, TxnOffsetCommitRequestData, TxnOffsetCommitResponseData,
        UnregisterBrokerRequestData, UnregisterBrokerResponseData, UpdateFeaturesRequestData,
        UpdateFeaturesResponseData, WriteTxnMarkersRequestData, WriteTxnMarkersResponseData,
    },
};

/// A generated Kafka request body that can be encoded by the wire client.
pub trait RequestMessage {
    /// Encode this request body for `version`.
    fn write_request(&self, buf: &mut BytesMut, version: i16) -> Result<()>;

    /// Return the exact encoded body length for `version`.
    fn encoded_len(&self, version: i16) -> Result<usize>;
}

/// A generated Kafka response body that can be decoded by the wire client.
pub trait ResponseMessage: Sized {
    /// Decode this response body for `version`.
    fn read_response(buf: &mut Bytes, version: i16) -> Result<Self>;
}

/// Implement [`RequestMessage`]/[`ResponseMessage`] for a generated request and
/// response pair whose encoding is a straight pass-through to the generated
/// `write`/`encoded_len`/`read` methods (no version-specific normalization).
macro_rules! impl_passthrough_message {
    ($($request:ty => $response:ty),+ $(,)?) => {
        $(
            impl RequestMessage for $request {
                fn write_request(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
                    self.write(buf, version)?;
                    Ok(())
                }

                fn encoded_len(&self, version: i16) -> Result<usize> {
                    self.encoded_len(version)
                }
            }

            impl ResponseMessage for $response {
                fn read_response(buf: &mut Bytes, version: i16) -> Result<Self> {
                    Self::read(buf, version)
                }
            }
        )+
    };
}

// Core client request/response pairs (api-versions, metadata, producer id,
// coordinator discovery, transactions, and telemetry). Pure pass-through codecs.
impl_passthrough_message! {
    ApiVersionsRequestData => ApiVersionsResponseData,
    MetadataRequestData => MetadataResponseData,
    InitProducerIdRequestData => InitProducerIdResponseData,
    FindCoordinatorRequestData => FindCoordinatorResponseData,
    AddPartitionsToTxnRequestData => AddPartitionsToTxnResponseData,
    AddOffsetsToTxnRequestData => AddOffsetsToTxnResponseData,
    TxnOffsetCommitRequestData => TxnOffsetCommitResponseData,
    EndTxnRequestData => EndTxnResponseData,
    GetTelemetrySubscriptionsRequestData => GetTelemetrySubscriptionsResponseData,
    PushTelemetryRequestData => PushTelemetryResponseData,
}

// Produce is the only request that is not a straight pass-through: depending on
// the negotiated version the wire form carries either the topic name (v < 13) or
// the topic id (v >= 13), so the unused field is cleared before the generated
// encoder runs (see `normalize_produce_request`). The response is pass-through.
impl RequestMessage for ProduceRequestData {
    fn write_request(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        normalize_produce_request(self, version).write(buf, version)?;
        Ok(())
    }

    fn encoded_len(&self, version: i16) -> Result<usize> {
        normalize_produce_request(self, version).encoded_len(version)
    }
}

impl ResponseMessage for ProduceResponseData {
    fn read_response(buf: &mut Bytes, version: i16) -> Result<Self> {
        Self::read(buf, version)
    }
}

// Admin client request/response pairs. These are pure pass-through codecs, so
// the macro above generates their wire adapters.
impl_passthrough_message! {
    CreateTopicsRequestData => CreateTopicsResponseData,
    DeleteTopicsRequestData => DeleteTopicsResponseData,
    CreatePartitionsRequestData => CreatePartitionsResponseData,
    DescribeClusterRequestData => DescribeClusterResponseData,
    DescribeConfigsRequestData => DescribeConfigsResponseData,
    AlterConfigsRequestData => AlterConfigsResponseData,
    ListGroupsRequestData => ListGroupsResponseData,
    DescribeGroupsRequestData => DescribeGroupsResponseData,
    DeleteGroupsRequestData => DeleteGroupsResponseData,
    OffsetFetchRequestData => OffsetFetchResponseData,
    OffsetCommitRequestData => OffsetCommitResponseData,
    OffsetDeleteRequestData => OffsetDeleteResponseData,
    IncrementalAlterConfigsRequestData => IncrementalAlterConfigsResponseData,
    ElectLeadersRequestData => ElectLeadersResponseData,
    ListOffsetsRequestData => ListOffsetsResponseData,
    DeleteRecordsRequestData => DeleteRecordsResponseData,
    DescribeProducersRequestData => DescribeProducersResponseData,
    DescribeTransactionsRequestData => DescribeTransactionsResponseData,
    ListTransactionsRequestData => ListTransactionsResponseData,
    DescribeLogDirsRequestData => DescribeLogDirsResponseData,
    AlterPartitionReassignmentsRequestData => AlterPartitionReassignmentsResponseData,
    ListPartitionReassignmentsRequestData => ListPartitionReassignmentsResponseData,
    UpdateFeaturesRequestData => UpdateFeaturesResponseData,
    UnregisterBrokerRequestData => UnregisterBrokerResponseData,
    DescribeAclsRequestData => DescribeAclsResponseData,
    CreateAclsRequestData => CreateAclsResponseData,
    DeleteAclsRequestData => DeleteAclsResponseData,
    DescribeClientQuotasRequestData => DescribeClientQuotasResponseData,
    AlterClientQuotasRequestData => AlterClientQuotasResponseData,
    DescribeUserScramCredentialsRequestData => DescribeUserScramCredentialsResponseData,
    AlterUserScramCredentialsRequestData => AlterUserScramCredentialsResponseData,
    CreateDelegationTokenRequestData => CreateDelegationTokenResponseData,
    RenewDelegationTokenRequestData => RenewDelegationTokenResponseData,
    ExpireDelegationTokenRequestData => ExpireDelegationTokenResponseData,
    DescribeDelegationTokenRequestData => DescribeDelegationTokenResponseData,
    AlterReplicaLogDirsRequestData => AlterReplicaLogDirsResponseData,
    WriteTxnMarkersRequestData => WriteTxnMarkersResponseData,
    LeaveGroupRequestData => LeaveGroupResponseData,
    ConsumerGroupDescribeRequestData => ConsumerGroupDescribeResponseData,
    ListConfigResourcesRequestData => ListConfigResourcesResponseData,
    DescribeQuorumRequestData => DescribeQuorumResponseData,
    AddRaftVoterRequestData => AddRaftVoterResponseData,
    RemoveRaftVoterRequestData => RemoveRaftVoterResponseData,
    ShareGroupDescribeRequestData => ShareGroupDescribeResponseData,
    StreamsGroupDescribeRequestData => StreamsGroupDescribeResponseData,
    DescribeShareGroupOffsetsRequestData => DescribeShareGroupOffsetsResponseData,
    AlterShareGroupOffsetsRequestData => AlterShareGroupOffsetsResponseData,
    DeleteShareGroupOffsetsRequestData => DeleteShareGroupOffsetsResponseData,
}

// Consumer client request/response pairs: fetch, the classic consumer-group
// coordination RPCs (join/sync/heartbeat), offset-for-leader-epoch, and the new
// KIP-848 consumer group protocol (ConsumerGroupHeartbeat). All are pure
// pass-through codecs, like the admin block above.
impl_passthrough_message! {
    FetchRequestData => FetchResponseData,
    JoinGroupRequestData => JoinGroupResponseData,
    SyncGroupRequestData => SyncGroupResponseData,
    HeartbeatRequestData => HeartbeatResponseData,
    OffsetForLeaderEpochRequestData => OffsetForLeaderEpochResponseData,
    ConsumerGroupHeartbeatRequestData => ConsumerGroupHeartbeatResponseData,
}

/// Clear the topic key that the negotiated `version` does not put on the wire so
/// the generated encoder does not reject a request that still carries both the
/// topic name and topic id.
///
/// The request is borrowed unchanged when it is already in the wire form for
/// `version`; a clone is only taken when a field actually has to be cleared. The
/// cleared field carries no bytes for its version, so the borrowed and cleared
/// forms encode to the identical length and body.
fn normalize_produce_request(
    request: &ProduceRequestData,
    version: i16,
) -> Cow<'_, ProduceRequestData> {
    let needs_clear = request.topic_data.iter().any(|topic| {
        if version >= 13 {
            topic.name != KafkaString::default()
        } else {
            topic.topic_id != KafkaUuid::ZERO
        }
    });
    if !needs_clear {
        return Cow::Borrowed(request);
    }
    let mut normalized = request.clone();
    for topic in &mut normalized.topic_data {
        if version >= 13 {
            topic.name = KafkaString::default();
        } else {
            topic.topic_id = KafkaUuid::ZERO;
        }
    }
    Cow::Owned(normalized)
}
