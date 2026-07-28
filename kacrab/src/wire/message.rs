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
        AlterUserScramCredentialsResponseData, ApiKey, ApiVersionsRequestData,
        ApiVersionsResponseData, ConsumerGroupDescribeRequestData,
        ConsumerGroupDescribeResponseData, ConsumerGroupHeartbeatRequestData,
        ConsumerGroupHeartbeatResponseData, CreateAclsRequestData, CreateAclsResponseData,
        CreateDelegationTokenRequestData, CreateDelegationTokenResponseData,
        CreatePartitionsRequestData, CreatePartitionsResponseData, CreateTopicsRequestData,
        CreateTopicsResponseData, DeleteAclsRequestData, DeleteAclsResponseData,
        DeleteGroupsRequestData, DeleteGroupsResponseData, DeleteRecordsRequestData,
        DeleteRecordsResponseData, DeleteShareGroupOffsetsRequestData,
        DeleteShareGroupOffsetsResponseData, DeleteTopicsRequestData, DeleteTopicsResponseData,
        DescribeAclsRequestData, DescribeAclsResponseData, DescribeClientQuotasRequestData,
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
        OffsetFetchRequestGroup, OffsetFetchRequestTopic, OffsetFetchRequestTopics,
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
    version::UnsupportedFieldVersion,
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

// `OffsetFetch` is the same two-shape story as `FindCoordinator`: v1-7 carry the
// flat `group_id`/`topics` pair, v8+ the batched `groups` array, and the
// generated encoder rejects whichever one its version does not carry. The
// request is rewritten into the negotiated version's form here, mirroring Java's
// `OffsetFetchRequest.Builder.maybeDowngrade`. The response is pass-through —
// callers read both shapes (see `Admin::list_consumer_group_offsets`).
impl RequestMessage for OffsetFetchRequestData {
    fn write_request(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        normalize_offset_fetch_request(self, version)?.write(buf, version)?;
        Ok(())
    }

    fn encoded_len(&self, version: i16) -> Result<usize> {
        normalize_offset_fetch_request(self, version)?.encoded_len(version)
    }
}

impl ResponseMessage for OffsetFetchResponseData {
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

// Consumer client request/response pairs: fetch, offset lookup, the classic
// consumer-group coordination RPCs (join/sync/heartbeat), offset-for-leader-epoch,
// and the new KIP-848 consumer group protocol (ConsumerGroupHeartbeat). All are
// pure pass-through codecs, like the admin block above.
//
// `Fetch`, `ListOffsets`, and `SyncGroup` each carry a field that only appears
// from some version on — the consumer's `rack_id` (v11, KIP-392), the
// remote-storage `timeout_ms` (v10, KIP-1075), and the assignor's
// `protocol_type`/`protocol_name` (v5). Kafka marks all of them ignorable, so the
// generated encoder drops them below their version instead of rejecting the
// request, and no normalization is needed here.
impl_passthrough_message! {
    FetchRequestData => FetchResponseData,
    ListOffsetsRequestData => ListOffsetsResponseData,
    JoinGroupRequestData => JoinGroupResponseData,
    SyncGroupRequestData => SyncGroupResponseData,
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

/// First `OffsetFetch` version carrying the batched `groups` array instead of
/// the flat `group_id`/`topics` pair (broker 3.0).
const OFFSET_FETCH_BATCHED_MIN_VERSION: i16 = 8;

/// First `OffsetFetch` version carrying `require_stable` (KIP-447, broker 2.5).
const OFFSET_FETCH_REQUIRE_STABLE_MIN_VERSION: i16 = 7;

/// First `OffsetFetch` version whose `topics` is nullable, i.e. that can ask for
/// every topic the group has committed rather than a named list.
const OFFSET_FETCH_ALL_TOPICS_MIN_VERSION: i16 = 2;

/// First `OffsetFetch` version that keys topics by `topic_id` instead of `name`
/// (KIP-1140).
const OFFSET_FETCH_TOPIC_ID_MIN_VERSION: i16 = 10;

/// Rewrite an `OffsetFetch` request into the group form the negotiated `version`
/// speaks: the flat `group_id`/`topics` pair up to v7, the batched `groups`
/// array from v8 on.
///
/// Mirrors Java's `OffsetFetchRequest.Builder`:
///
/// * `maybeDowngrade` folds a single `groups` entry back into the flat pair; a request naming
///   several groups cannot be expressed below v8, so it is left untouched for the generated encoder
///   to reject rather than silently dropping the groups it cannot carry (Java raises
///   `NoBatchedOffsetFetchRequestException`).
/// * `throwIfStableOffsetsUnsupported` drops `require_stable` below v7. Kafka's admin client passes
///   `throwOnFetchStableOffsetsUnsupported = false`, which logs and falls the flag back to `false`
///   — a coordinator that predates KIP-447 has no unstable offsets to hold back.
/// * `throwIfRequestingAllTopicsIsUnsupported` refuses an "all topics" fetch below v2, where
///   `topics` is not nullable. The generated encoder would write an empty array there, turning
///   "every topic" into "no topics", so this is an error rather than a silent wrong answer.
///
/// The per-group `member_id`/`member_epoch` (KIP-848) have no place in the flat
/// form and are dropped with it, exactly as `maybeDowngrade` does; the classic
/// group path this serves never sets them.
///
/// Topics carry both keys — the name and the id — because which one goes on the
/// wire is also a per-version choice: v10 (KIP-1140) keys topics by `topic_id`
/// and refuses a name, every version below it keys by `name` and refuses an id.
/// The key the negotiated version does not carry is cleared here, the way
/// [`normalize_produce_request`] does for `Produce` v13, so a call site can fill
/// both without knowing what it will negotiate.
fn normalize_offset_fetch_request(
    request: &OffsetFetchRequestData,
    version: i16,
) -> Result<Cow<'_, OffsetFetchRequestData>> {
    if version >= OFFSET_FETCH_BATCHED_MIN_VERSION {
        let needs_key_clear = request.groups.iter().any(|group| {
            group
                .topics
                .iter()
                .flatten()
                .any(|topic| offset_fetch_topic_needs_key_clear(topic, version))
        });
        if request.group_id == KafkaString::default() && !needs_key_clear {
            return Ok(Cow::Borrowed(request));
        }
        let mut normalized = request.clone();
        if normalized.group_id != KafkaString::default() {
            let group_id = core::mem::take(&mut normalized.group_id);
            let topics = normalized.topics.take();
            if normalized.groups.is_empty() {
                normalized.groups.push(OffsetFetchRequestGroup {
                    group_id,
                    member_id: None,
                    member_epoch: -1,
                    topics: topics.map(|topics| {
                        topics
                            .into_iter()
                            .map(|topic| OffsetFetchRequestTopics {
                                name: topic.name,
                                topic_id: KafkaUuid::ZERO,
                                partition_indexes: topic.partition_indexes,
                                _unknown_tagged_fields: Vec::new(),
                            })
                            .collect()
                    }),
                    _unknown_tagged_fields: Vec::new(),
                });
            }
        }
        for group in &mut normalized.groups {
            for topic in group.topics.iter_mut().flatten() {
                if version >= OFFSET_FETCH_TOPIC_ID_MIN_VERSION {
                    topic.name = KafkaString::default();
                } else {
                    topic.topic_id = KafkaUuid::ZERO;
                }
            }
        }
        return Ok(Cow::Owned(normalized));
    }
    let mut normalized = Cow::Borrowed(request);
    if let [group] = request.groups.as_slice() {
        normalized = Cow::Owned(OffsetFetchRequestData {
            group_id: group.group_id.clone(),
            topics: group.topics.as_ref().map(|topics| {
                topics
                    .iter()
                    .map(|topic| OffsetFetchRequestTopic {
                        name: topic.name.clone(),
                        partition_indexes: topic.partition_indexes.clone(),
                        _unknown_tagged_fields: Vec::new(),
                    })
                    .collect()
            }),
            groups: Vec::new(),
            require_stable: request.require_stable,
            _unknown_tagged_fields: request._unknown_tagged_fields.clone(),
        });
    }
    if version < OFFSET_FETCH_ALL_TOPICS_MIN_VERSION && normalized.topics.is_none() {
        return Err(
            UnsupportedFieldVersion::new(ApiKey::OffsetFetch as i16, "topics", version).into(),
        );
    }
    if version < OFFSET_FETCH_REQUIRE_STABLE_MIN_VERSION && normalized.require_stable {
        normalized.to_mut().require_stable = false;
    }
    Ok(normalized)
}

/// Whether an `OffsetFetch` topic still carries the key the negotiated `version`
/// does not put on the wire (the name from v10, the id below it).
fn offset_fetch_topic_needs_key_clear(topic: &OffsetFetchRequestTopics, version: i16) -> bool {
    if version >= OFFSET_FETCH_TOPIC_ID_MIN_VERSION {
        topic.name != KafkaString::default()
    } else {
        topic.topic_id != KafkaUuid::ZERO
    }
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
        KafkaString, KafkaUuid,
        generated::{
            FetchRequestData, FindCoordinatorRequestData, ListOffsetsRequestData,
            OffsetFetchRequestData, OffsetFetchRequestGroup, OffsetFetchRequestTopic,
            OffsetFetchRequestTopics, SyncGroupRequestData,
        },
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
    fn fetch_request_drops_the_rack_id_below_its_version() {
        let request = FetchRequestData {
            replica_id: -1,
            max_wait_ms: 500,
            rack_id: KafkaString::from("rack-1".to_owned()),
            ..FetchRequestData::default()
        };

        for version in 4..=10 {
            let mut buf = BytesMut::new();
            request
                .write_request(&mut buf, version)
                .expect("fetch request should encode for the negotiated version");
            assert_eq!(
                RequestMessage::encoded_len(&request, version).expect("encoded length"),
                buf.len()
            );
            let mut encoded = buf.freeze();
            let decoded =
                FetchRequestData::read(&mut encoded, version).expect("fetch request should decode");
            assert_eq!(decoded.max_wait_ms, 500);
            assert_eq!(decoded.rack_id, KafkaString::default());
        }
    }

    #[test]
    fn fetch_request_keeps_the_rack_id_from_its_version() {
        let request = FetchRequestData {
            replica_id: -1,
            max_wait_ms: 500,
            rack_id: KafkaString::from("rack-1".to_owned()),
            ..FetchRequestData::default()
        };
        let mut buf = BytesMut::new();

        request
            .write_request(&mut buf, 11)
            .expect("fetch request should encode");

        let mut encoded = buf.freeze();
        let decoded =
            FetchRequestData::read(&mut encoded, 11).expect("fetch request should decode");
        assert_eq!(decoded.rack_id, KafkaString::from("rack-1".to_owned()));
    }

    fn offset_fetch_request() -> OffsetFetchRequestData {
        OffsetFetchRequestData {
            groups: vec![OffsetFetchRequestGroup {
                group_id: KafkaString::from("group-a".to_owned()),
                member_id: None,
                member_epoch: -1,
                topics: Some(vec![OffsetFetchRequestTopics {
                    name: KafkaString::from("orders".to_owned()),
                    topic_id: KafkaUuid::from_parts(7, 7),
                    partition_indexes: vec![1, 2],
                    _unknown_tagged_fields: Vec::new(),
                }]),
                _unknown_tagged_fields: Vec::new(),
            }],
            require_stable: true,
            ..OffsetFetchRequestData::default()
        }
    }

    fn encode_offset_fetch(request: &OffsetFetchRequestData, version: i16) -> Bytes {
        let mut buf = BytesMut::new();
        request
            .write_request(&mut buf, version)
            .expect("offset fetch request should encode for the negotiated version");
        assert_eq!(
            RequestMessage::encoded_len(request, version).expect("encoded length"),
            buf.len()
        );
        buf.freeze()
    }

    #[test]
    fn offset_fetch_request_uses_the_flat_group_below_batched_versions() {
        let request = offset_fetch_request();

        let mut encoded = encode_offset_fetch(&request, 7);

        let decoded =
            OffsetFetchRequestData::read(&mut encoded, 7).expect("v7 request should decode");
        assert_eq!(decoded.group_id, KafkaString::from("group-a".to_owned()));
        assert!(decoded.groups.is_empty());
        assert!(decoded.require_stable);
        let topics = decoded.topics.expect("flat topics");
        assert_eq!(topics[0].name, KafkaString::from("orders".to_owned()));
        assert_eq!(topics[0].partition_indexes, vec![1, 2]);
    }

    #[test]
    fn offset_fetch_request_drops_require_stable_below_its_version() {
        let request = offset_fetch_request();

        for version in 2..=6 {
            let mut encoded = encode_offset_fetch(&request, version);
            let decoded = OffsetFetchRequestData::read(&mut encoded, version)
                .expect("offset fetch request should decode");
            assert_eq!(decoded.group_id, KafkaString::from("group-a".to_owned()));
            assert!(!decoded.require_stable);
        }
    }

    #[test]
    fn offset_fetch_request_uses_groups_from_batched_versions() {
        let request = offset_fetch_request();

        for version in 8..=9 {
            let mut encoded = encode_offset_fetch(&request, version);
            let decoded = OffsetFetchRequestData::read(&mut encoded, version)
                .expect("offset fetch request should decode");
            assert_eq!(decoded.group_id, KafkaString::default());
            assert_eq!(decoded.groups.len(), 1);
            let topics = decoded.groups[0].topics.as_ref().expect("batched topics");
            assert_eq!(topics[0].name, KafkaString::from("orders".to_owned()));
            assert_eq!(topics[0].topic_id, KafkaUuid::ZERO);
        }
    }

    #[test]
    fn offset_fetch_request_keys_topics_by_id_from_the_topic_id_version() {
        let request = offset_fetch_request();

        let mut encoded = encode_offset_fetch(&request, 10);

        let decoded = OffsetFetchRequestData::read(&mut encoded, 10)
            .expect("offset fetch request should decode");
        let topics = decoded.groups[0].topics.as_ref().expect("batched topics");
        assert_eq!(topics[0].name, KafkaString::default());
        assert_eq!(topics[0].topic_id, KafkaUuid::from_parts(7, 7));
    }

    #[test]
    fn offset_fetch_request_promotes_the_flat_group_to_batched_versions() {
        let request = OffsetFetchRequestData {
            group_id: KafkaString::from("group-a".to_owned()),
            topics: Some(vec![OffsetFetchRequestTopic {
                name: KafkaString::from("orders".to_owned()),
                partition_indexes: vec![1],
                _unknown_tagged_fields: Vec::new(),
            }]),
            ..OffsetFetchRequestData::default()
        };

        let mut encoded = encode_offset_fetch(&request, 8);

        let decoded =
            OffsetFetchRequestData::read(&mut encoded, 8).expect("v8 request should decode");
        assert_eq!(decoded.group_id, KafkaString::default());
        assert_eq!(
            decoded.groups[0].group_id,
            KafkaString::from("group-a".to_owned())
        );
        let topics = decoded.groups[0].topics.as_ref().expect("batched topics");
        assert_eq!(topics[0].name, KafkaString::from("orders".to_owned()));
        assert_eq!(topics[0].partition_indexes, vec![1]);
    }

    #[test]
    fn offset_fetch_request_rejects_batched_groups_below_batched_versions() {
        let request = OffsetFetchRequestData {
            groups: vec![
                OffsetFetchRequestGroup {
                    group_id: KafkaString::from("group-a".to_owned()),
                    ..OffsetFetchRequestGroup::default()
                },
                OffsetFetchRequestGroup {
                    group_id: KafkaString::from("group-b".to_owned()),
                    ..OffsetFetchRequestGroup::default()
                },
            ],
            ..OffsetFetchRequestData::default()
        };
        let mut buf = BytesMut::new();

        let error = request.write_request(&mut buf, 7);

        assert!(error.is_err());
    }

    #[test]
    fn offset_fetch_request_rejects_an_all_topics_fetch_below_its_version() {
        let request = OffsetFetchRequestData {
            groups: vec![OffsetFetchRequestGroup {
                group_id: KafkaString::from("group-a".to_owned()),
                topics: None,
                ..OffsetFetchRequestGroup::default()
            }],
            ..OffsetFetchRequestData::default()
        };
        let mut buf = BytesMut::new();

        let error = request.write_request(&mut buf, 1);

        assert!(error.is_err());
        assert!(request.write_request(&mut BytesMut::new(), 2).is_ok());
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
