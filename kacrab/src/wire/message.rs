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
        ShareAcknowledgeRequestData, ShareAcknowledgeResponseData, ShareFetchRequestData,
        ShareFetchResponseData, ShareGroupDescribeRequestData, ShareGroupDescribeResponseData,
        ShareGroupHeartbeatRequestData, ShareGroupHeartbeatResponseData,
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
    AddPartitionsToTxnRequestData => AddPartitionsToTxnResponseData,
    AddOffsetsToTxnRequestData => AddOffsetsToTxnResponseData,
    TxnOffsetCommitRequestData => TxnOffsetCommitResponseData,
    EndTxnRequestData => EndTxnResponseData,
    GetTelemetrySubscriptionsRequestData => GetTelemetrySubscriptionsResponseData,
    PushTelemetryRequestData => PushTelemetryResponseData,
}

// `FindCoordinator` is not a straight pass-through either: v0-3 carries a
// singular `key`, v4+ (KIP-699) the batched `coordinator_keys` array, and the
// generated encoder rejects whichever one its version does not carry. Call sites
// only learn the negotiated version after they have built the request, so the
// request is rewritten into the negotiated version's form here, mirroring Java's
// `FindCoordinatorRequest.Builder.build`. The response is pass-through — callers
// read both shapes through `common::coordinator::coordinator_for_key`.
impl RequestMessage for FindCoordinatorRequestData {
    fn write_request(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        normalize_find_coordinator_request(self, version).write(buf, version)?;
        Ok(())
    }

    fn encoded_len(&self, version: i16) -> Result<usize> {
        normalize_find_coordinator_request(self, version).encoded_len(version)
    }
}

impl ResponseMessage for FindCoordinatorResponseData {
    fn read_response(buf: &mut Bytes, version: i16) -> Result<Self> {
        Self::read(buf, version)
    }
}

// `ListOffsets` carries the KIP-1075 remote-storage `timeout_ms` only from v10.
// Kafka marks the field ignorable, so a broker that negotiates an older version
// is sent the request without it rather than being sent a request its encoder
// rejects (see `normalize_list_offsets_request`). The response is pass-through.
impl RequestMessage for ListOffsetsRequestData {
    fn write_request(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        normalize_list_offsets_request(self, version).write(buf, version)?;
        Ok(())
    }

    fn encoded_len(&self, version: i16) -> Result<usize> {
        normalize_list_offsets_request(self, version).encoded_len(version)
    }
}

impl ResponseMessage for ListOffsetsResponseData {
    fn read_response(buf: &mut Bytes, version: i16) -> Result<Self> {
        Self::read(buf, version)
    }
}

// `SyncGroup` carries the assignor's `protocol_type`/`protocol_name` only from
// v5, where the coordinator uses them to fence a member speaking a different
// protocol. Kafka marks both ignorable, so an older negotiated version is sent
// the request without them (see `normalize_sync_group_request`) instead of one
// its encoder rejects. The response is pass-through.
impl RequestMessage for SyncGroupRequestData {
    fn write_request(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        normalize_sync_group_request(self, version).write(buf, version)?;
        Ok(())
    }

    fn encoded_len(&self, version: i16) -> Result<usize> {
        normalize_sync_group_request(self, version).encoded_len(version)
    }
}

impl ResponseMessage for SyncGroupResponseData {
    fn read_response(buf: &mut Bytes, version: i16) -> Result<Self> {
        Self::read(buf, version)
    }
}

// Produce is not a straight pass-through: depending on the negotiated version
// the wire form carries either the topic name (v < 13) or the topic id (v >= 13),
// so the unused field is cleared before the generated encoder runs (see
// `normalize_produce_request`). The response is pass-through.
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
    HeartbeatRequestData => HeartbeatResponseData,
    OffsetForLeaderEpochRequestData => OffsetForLeaderEpochResponseData,
    ConsumerGroupHeartbeatRequestData => ConsumerGroupHeartbeatResponseData,
}

// Share consumer request/response pairs (KIP-932): membership, acquire-and-fetch,
// and acknowledgement. Also pure pass-through codecs.
impl_passthrough_message! {
    ShareGroupHeartbeatRequestData => ShareGroupHeartbeatResponseData,
    ShareFetchRequestData => ShareFetchResponseData,
    ShareAcknowledgeRequestData => ShareAcknowledgeResponseData,
}

/// First `FindCoordinator` version carrying the batched `coordinator_keys` array
/// instead of the singular `key` (KIP-699, broker 3.0).
const FIND_COORDINATOR_BATCHED_MIN_VERSION: i16 = 4;

/// Rewrite a `FindCoordinator` request into the coordinator-key form the
/// negotiated `version` speaks: the singular `key` below v4, the batched
/// `coordinator_keys` array from v4 on.
///
/// A caller asking for several coordinators at once cannot be expressed below
/// v4, so that request is left untouched for the generated encoder to reject
/// rather than silently dropping the keys it cannot carry (Java raises
/// `NoBatchedFindCoordinatorsException` for the same case).
fn normalize_find_coordinator_request(
    request: &FindCoordinatorRequestData,
    version: i16,
) -> Cow<'_, FindCoordinatorRequestData> {
    if version >= FIND_COORDINATOR_BATCHED_MIN_VERSION {
        if request.key == KafkaString::default() {
            return Cow::Borrowed(request);
        }
        let mut normalized = request.clone();
        let key = core::mem::take(&mut normalized.key);
        if normalized.coordinator_keys.is_empty() {
            normalized.coordinator_keys.push(key);
        }
        return Cow::Owned(normalized);
    }
    let [key] = request.coordinator_keys.as_slice() else {
        return Cow::Borrowed(request);
    };
    let mut normalized = request.clone();
    normalized.key = key.clone();
    normalized.coordinator_keys.clear();
    Cow::Owned(normalized)
}

/// First `ListOffsets` version carrying the remote-storage `timeout_ms`
/// (KIP-1075).
const LIST_OFFSETS_TIMEOUT_MIN_VERSION: i16 = 10;

/// Drop the `ListOffsets` request timeout for a negotiated `version` that does
/// not carry it.
///
/// Kafka marks the field ignorable — the broker applies its own timeout when the
/// request cannot express one — so dropping it is what its own encoder does, and
/// leaves the offset lookup itself unchanged.
fn normalize_list_offsets_request(
    request: &ListOffsetsRequestData,
    version: i16,
) -> Cow<'_, ListOffsetsRequestData> {
    if version >= LIST_OFFSETS_TIMEOUT_MIN_VERSION || request.timeout_ms == 0 {
        return Cow::Borrowed(request);
    }
    let mut normalized = request.clone();
    normalized.timeout_ms = 0;
    Cow::Owned(normalized)
}

/// First `SyncGroup` version carrying the assignor's `protocol_type` and
/// `protocol_name`.
const SYNC_GROUP_PROTOCOL_MIN_VERSION: i16 = 5;

/// Drop the `SyncGroup` protocol identity for a negotiated `version` that does
/// not carry it.
///
/// Kafka marks both fields ignorable: a coordinator too old to fence on them
/// simply does not receive them, which is what its own encoder sends, and the
/// assignment the member is asking for is unchanged.
fn normalize_sync_group_request(
    request: &SyncGroupRequestData,
    version: i16,
) -> Cow<'_, SyncGroupRequestData> {
    if version >= SYNC_GROUP_PROTOCOL_MIN_VERSION
        || (request.protocol_type.is_none() && request.protocol_name.is_none())
    {
        return Cow::Borrowed(request);
    }
    let mut normalized = request.clone();
    normalized.protocol_type = None;
    normalized.protocol_name = None;
    Cow::Owned(normalized)
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

#[cfg(test)]
mod tests {
    #![allow(
        clippy::expect_used,
        clippy::missing_assert_message,
        clippy::unwrap_used,
        reason = "Unit test fixtures fail fastest with contextual unwrap/expect calls."
    )]

    use bytes::{Bytes, BytesMut};
    use kacrab_protocol::{
        KafkaString,
        generated::{FindCoordinatorRequestData, ListOffsetsRequestData, SyncGroupRequestData},
    };

    use super::RequestMessage;

    fn find_coordinator_request() -> FindCoordinatorRequestData {
        FindCoordinatorRequestData {
            key_type: 0,
            coordinator_keys: vec![KafkaString::from("group-a".to_owned())],
            ..FindCoordinatorRequestData::default()
        }
    }

    fn encode(request: &FindCoordinatorRequestData, version: i16) -> Bytes {
        let mut buf = BytesMut::new();
        request
            .write_request(&mut buf, version)
            .expect("find coordinator request should encode for the negotiated version");
        assert_eq!(
            RequestMessage::encoded_len(request, version).expect("encoded length"),
            buf.len()
        );
        buf.freeze()
    }

    #[test]
    fn find_coordinator_request_uses_singular_key_below_batched_versions() {
        let request = find_coordinator_request();

        for version in 0..=3 {
            let mut encoded = encode(&request, version);
            let decoded = FindCoordinatorRequestData::read(&mut encoded, version)
                .expect("find coordinator request should decode");
            assert_eq!(decoded.key, KafkaString::from("group-a".to_owned()));
            assert!(decoded.coordinator_keys.is_empty());
        }
    }

    #[test]
    fn find_coordinator_request_uses_coordinator_keys_from_batched_version() {
        let request = find_coordinator_request();

        for version in 4..=6 {
            let mut encoded = encode(&request, version);
            let decoded = FindCoordinatorRequestData::read(&mut encoded, version)
                .expect("find coordinator request should decode");
            assert_eq!(
                decoded.coordinator_keys,
                vec![KafkaString::from("group-a".to_owned())]
            );
            assert_eq!(decoded.key, KafkaString::default());
        }
    }

    #[test]
    fn find_coordinator_request_promotes_singular_key_to_batched_versions() {
        let request = FindCoordinatorRequestData {
            key: KafkaString::from("group-a".to_owned()),
            key_type: 0,
            ..FindCoordinatorRequestData::default()
        };

        let mut encoded = encode(&request, 6);

        let decoded =
            FindCoordinatorRequestData::read(&mut encoded, 6).expect("v6 request should decode");
        assert_eq!(
            decoded.coordinator_keys,
            vec![KafkaString::from("group-a".to_owned())]
        );
        assert_eq!(decoded.key, KafkaString::default());
    }

    #[test]
    fn list_offsets_request_drops_the_timeout_below_its_version() {
        let request = ListOffsetsRequestData {
            replica_id: -1,
            timeout_ms: 30_000,
            ..ListOffsetsRequestData::default()
        };

        for version in 1..=9 {
            let mut buf = BytesMut::new();
            request
                .write_request(&mut buf, version)
                .expect("list offsets request should encode for the negotiated version");
            assert_eq!(
                RequestMessage::encoded_len(&request, version).expect("encoded length"),
                buf.len()
            );
            let mut encoded = buf.freeze();
            let decoded = ListOffsetsRequestData::read(&mut encoded, version)
                .expect("list offsets request should decode");
            assert_eq!(decoded.timeout_ms, 0);
        }
    }

    #[test]
    fn list_offsets_request_keeps_the_timeout_from_its_version() {
        let request = ListOffsetsRequestData {
            replica_id: -1,
            timeout_ms: 30_000,
            ..ListOffsetsRequestData::default()
        };

        for version in 10..=11 {
            let mut buf = BytesMut::new();
            request
                .write_request(&mut buf, version)
                .expect("list offsets request should encode");
            let mut encoded = buf.freeze();
            let decoded = ListOffsetsRequestData::read(&mut encoded, version)
                .expect("list offsets request should decode");
            assert_eq!(decoded.timeout_ms, 30_000);
        }
    }

    #[test]
    fn sync_group_request_drops_the_protocol_fields_below_their_version() {
        let request = SyncGroupRequestData {
            group_id: KafkaString::from("group-a".to_owned()),
            generation_id: 3,
            member_id: KafkaString::from("member-1".to_owned()),
            protocol_type: Some(KafkaString::from("consumer".to_owned())),
            protocol_name: Some(KafkaString::from("range".to_owned())),
            ..SyncGroupRequestData::default()
        };

        for version in 0..=4 {
            let mut buf = BytesMut::new();
            request
                .write_request(&mut buf, version)
                .expect("sync group request should encode for the negotiated version");
            assert_eq!(
                RequestMessage::encoded_len(&request, version).expect("encoded length"),
                buf.len()
            );
            let mut encoded = buf.freeze();
            let decoded = SyncGroupRequestData::read(&mut encoded, version)
                .expect("sync group request should decode");
            assert_eq!(decoded.member_id, KafkaString::from("member-1".to_owned()));
            assert_eq!(decoded.protocol_type, None);
            assert_eq!(decoded.protocol_name, None);
        }
    }

    #[test]
    fn sync_group_request_keeps_the_protocol_fields_from_their_version() {
        let request = SyncGroupRequestData {
            group_id: KafkaString::from("group-a".to_owned()),
            generation_id: 3,
            member_id: KafkaString::from("member-1".to_owned()),
            protocol_type: Some(KafkaString::from("consumer".to_owned())),
            protocol_name: Some(KafkaString::from("range".to_owned())),
            ..SyncGroupRequestData::default()
        };
        let mut buf = BytesMut::new();

        request
            .write_request(&mut buf, 5)
            .expect("sync group request should encode");

        let mut encoded = buf.freeze();
        let decoded =
            SyncGroupRequestData::read(&mut encoded, 5).expect("sync group request should decode");
        assert_eq!(
            decoded.protocol_type,
            Some(KafkaString::from("consumer".to_owned()))
        );
        assert_eq!(
            decoded.protocol_name,
            Some(KafkaString::from("range".to_owned()))
        );
    }

    #[test]
    fn find_coordinator_request_rejects_batched_keys_below_batched_versions() {
        let request = FindCoordinatorRequestData {
            key_type: 0,
            coordinator_keys: vec![
                KafkaString::from("group-a".to_owned()),
                KafkaString::from("group-b".to_owned()),
            ],
            ..FindCoordinatorRequestData::default()
        };
        let mut buf = BytesMut::new();

        let error = request.write_request(&mut buf, 3);

        assert!(error.is_err());
    }
}
