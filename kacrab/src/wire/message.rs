//! Generated request/response adapter traits for broker sessions.

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

impl RequestMessage for ApiVersionsRequestData {
    fn write_request(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        self.write(buf, version)?;
        Ok(())
    }

    fn encoded_len(&self, version: i16) -> Result<usize> {
        self.encoded_len(version)
    }
}

impl ResponseMessage for ApiVersionsResponseData {
    fn read_response(buf: &mut Bytes, version: i16) -> Result<Self> {
        Self::read(buf, version)
    }
}

impl RequestMessage for MetadataRequestData {
    fn write_request(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        self.write(buf, version)?;
        Ok(())
    }

    fn encoded_len(&self, version: i16) -> Result<usize> {
        self.encoded_len(version)
    }
}

impl ResponseMessage for MetadataResponseData {
    fn read_response(buf: &mut Bytes, version: i16) -> Result<Self> {
        Self::read(buf, version)
    }
}

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

impl RequestMessage for InitProducerIdRequestData {
    fn write_request(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        self.write(buf, version)?;
        Ok(())
    }

    fn encoded_len(&self, version: i16) -> Result<usize> {
        self.encoded_len(version)
    }
}

impl ResponseMessage for InitProducerIdResponseData {
    fn read_response(buf: &mut Bytes, version: i16) -> Result<Self> {
        Self::read(buf, version)
    }
}

impl RequestMessage for FindCoordinatorRequestData {
    fn write_request(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        self.write(buf, version)?;
        Ok(())
    }

    fn encoded_len(&self, version: i16) -> Result<usize> {
        self.encoded_len(version)
    }
}

impl ResponseMessage for FindCoordinatorResponseData {
    fn read_response(buf: &mut Bytes, version: i16) -> Result<Self> {
        Self::read(buf, version)
    }
}

impl RequestMessage for AddPartitionsToTxnRequestData {
    fn write_request(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        self.write(buf, version)?;
        Ok(())
    }

    fn encoded_len(&self, version: i16) -> Result<usize> {
        self.encoded_len(version)
    }
}

impl ResponseMessage for AddPartitionsToTxnResponseData {
    fn read_response(buf: &mut Bytes, version: i16) -> Result<Self> {
        Self::read(buf, version)
    }
}

impl RequestMessage for AddOffsetsToTxnRequestData {
    fn write_request(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        self.write(buf, version)?;
        Ok(())
    }

    fn encoded_len(&self, version: i16) -> Result<usize> {
        self.encoded_len(version)
    }
}

impl ResponseMessage for AddOffsetsToTxnResponseData {
    fn read_response(buf: &mut Bytes, version: i16) -> Result<Self> {
        Self::read(buf, version)
    }
}

impl RequestMessage for TxnOffsetCommitRequestData {
    fn write_request(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        self.write(buf, version)?;
        Ok(())
    }

    fn encoded_len(&self, version: i16) -> Result<usize> {
        self.encoded_len(version)
    }
}

impl ResponseMessage for TxnOffsetCommitResponseData {
    fn read_response(buf: &mut Bytes, version: i16) -> Result<Self> {
        Self::read(buf, version)
    }
}

impl RequestMessage for EndTxnRequestData {
    fn write_request(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        self.write(buf, version)?;
        Ok(())
    }

    fn encoded_len(&self, version: i16) -> Result<usize> {
        self.encoded_len(version)
    }
}

impl ResponseMessage for EndTxnResponseData {
    fn read_response(buf: &mut Bytes, version: i16) -> Result<Self> {
        Self::read(buf, version)
    }
}

impl RequestMessage for GetTelemetrySubscriptionsRequestData {
    fn write_request(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        self.write(buf, version)?;
        Ok(())
    }

    fn encoded_len(&self, version: i16) -> Result<usize> {
        self.encoded_len(version)
    }
}

impl ResponseMessage for GetTelemetrySubscriptionsResponseData {
    fn read_response(buf: &mut Bytes, version: i16) -> Result<Self> {
        Self::read(buf, version)
    }
}

impl RequestMessage for PushTelemetryRequestData {
    fn write_request(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        self.write(buf, version)?;
        Ok(())
    }

    fn encoded_len(&self, version: i16) -> Result<usize> {
        self.encoded_len(version)
    }
}

impl ResponseMessage for PushTelemetryResponseData {
    fn read_response(buf: &mut Bytes, version: i16) -> Result<Self> {
        Self::read(buf, version)
    }
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

// Consumer client request/response pairs (fetch + classic group coordination).
// Pure pass-through codecs, like the admin block above.
impl_passthrough_message! {
    FetchRequestData => FetchResponseData,
    JoinGroupRequestData => JoinGroupResponseData,
    SyncGroupRequestData => SyncGroupResponseData,
    HeartbeatRequestData => HeartbeatResponseData,
    OffsetForLeaderEpochRequestData => OffsetForLeaderEpochResponseData,
    ConsumerGroupHeartbeatRequestData => ConsumerGroupHeartbeatResponseData,
}

fn normalize_produce_request(request: &ProduceRequestData, version: i16) -> ProduceRequestData {
    let mut normalized = request.clone();
    for topic in &mut normalized.topic_data {
        if version >= 13 {
            topic.name = KafkaString::default();
        } else {
            topic.topic_id = KafkaUuid::ZERO;
        }
    }
    normalized
}
