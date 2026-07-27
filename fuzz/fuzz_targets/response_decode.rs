//! Generated response decoding, one API key and version per input.
//!
//! The generated `*ResponseData::read` functions are what turn a broker's bytes
//! into typed Rust. The Java oracle matrix checks them byte-for-byte on
//! *well-formed* input; this target covers the other half — garbage,
//! truncation, and hostile length prefixes.
//!
//! Input layout is `[api_key, version, body..]`. The first byte is the real
//! Kafka API key (`ApiKey::Produce == 0`, `Fetch == 1`, …) rather than an index
//! into a list, so a seed file is self-describing and stays valid when this
//! match grows. `generate_fuzz_corpus` in `kacrab-protocol/tests/java_interop.rs` writes seeds in this shape from
//! the same fixtures the Java oracle uses.
//!
//! Versions are deliberately unclamped: an out-of-range version must produce
//! `UnsupportedVersion`, never a panic.
//!
//! Coverage is every API key this client actually issues — all 67 of them,
//! enumerated against the crate rather than picked by hand, since a
//! hand-picked "hot path" set had quietly missed 15.
//!
//! The other 26 generated API keys are deliberately absent and are not a gap.
//! Responses are dispatched by the correlation id of a request this client
//! sent, so a decoder for an API the client never issues cannot be reached: a
//! broker cannot make us parse `LeaderAndIsrResponse` when we never sent
//! `LeaderAndIsr`. Unreachable code is not attack surface.

#![no_main]

use bytes::Bytes;
use kacrab_protocol::generated::*;
use libfuzzer_sys::fuzz_target;

/// Decode `body` as the response for `api_key` at `version`.
///
/// Returns `false` for an API key this target does not cover, so the fuzzer
/// learns those bytes are dead ends.
fn decode(api_key: u8, version: i16, body: &mut Bytes) -> bool {
    match api_key {
        0 => drop(ProduceResponseData::read(body, version)),
        1 => drop(FetchResponseData::read(body, version)),
        2 => drop(ListOffsetsResponseData::read(body, version)),
        3 => drop(MetadataResponseData::read(body, version)),
        8 => drop(OffsetCommitResponseData::read(body, version)),
        9 => drop(OffsetFetchResponseData::read(body, version)),
        10 => drop(FindCoordinatorResponseData::read(body, version)),
        11 => drop(JoinGroupResponseData::read(body, version)),
        12 => drop(HeartbeatResponseData::read(body, version)),
        13 => drop(LeaveGroupResponseData::read(body, version)),
        14 => drop(SyncGroupResponseData::read(body, version)),
        15 => drop(DescribeGroupsResponseData::read(body, version)),
        16 => drop(ListGroupsResponseData::read(body, version)),
        17 => drop(SaslHandshakeResponseData::read(body, version)),
        18 => drop(ApiVersionsResponseData::read(body, version)),
        19 => drop(CreateTopicsResponseData::read(body, version)),
        20 => drop(DeleteTopicsResponseData::read(body, version)),
        21 => drop(DeleteRecordsResponseData::read(body, version)),
        22 => drop(InitProducerIdResponseData::read(body, version)),
        23 => drop(OffsetForLeaderEpochResponseData::read(body, version)),
        24 => drop(AddPartitionsToTxnResponseData::read(body, version)),
        25 => drop(AddOffsetsToTxnResponseData::read(body, version)),
        26 => drop(EndTxnResponseData::read(body, version)),
        27 => drop(WriteTxnMarkersResponseData::read(body, version)),
        28 => drop(TxnOffsetCommitResponseData::read(body, version)),
        29 => drop(DescribeAclsResponseData::read(body, version)),
        30 => drop(CreateAclsResponseData::read(body, version)),
        31 => drop(DeleteAclsResponseData::read(body, version)),
        32 => drop(DescribeConfigsResponseData::read(body, version)),
        33 => drop(AlterConfigsResponseData::read(body, version)),
        34 => drop(AlterReplicaLogDirsResponseData::read(body, version)),
        35 => drop(DescribeLogDirsResponseData::read(body, version)),
        36 => drop(SaslAuthenticateResponseData::read(body, version)),
        37 => drop(CreatePartitionsResponseData::read(body, version)),
        38 => drop(CreateDelegationTokenResponseData::read(body, version)),
        39 => drop(RenewDelegationTokenResponseData::read(body, version)),
        40 => drop(ExpireDelegationTokenResponseData::read(body, version)),
        41 => drop(DescribeDelegationTokenResponseData::read(body, version)),
        42 => drop(DeleteGroupsResponseData::read(body, version)),
        43 => drop(ElectLeadersResponseData::read(body, version)),
        44 => drop(IncrementalAlterConfigsResponseData::read(body, version)),
        45 => drop(AlterPartitionReassignmentsResponseData::read(body, version)),
        46 => drop(ListPartitionReassignmentsResponseData::read(body, version)),
        47 => drop(OffsetDeleteResponseData::read(body, version)),
        48 => drop(DescribeClientQuotasResponseData::read(body, version)),
        49 => drop(AlterClientQuotasResponseData::read(body, version)),
        50 => drop(DescribeUserScramCredentialsResponseData::read(body, version)),
        51 => drop(AlterUserScramCredentialsResponseData::read(body, version)),
        55 => drop(DescribeQuorumResponseData::read(body, version)),
        57 => drop(UpdateFeaturesResponseData::read(body, version)),
        60 => drop(DescribeClusterResponseData::read(body, version)),
        61 => drop(DescribeProducersResponseData::read(body, version)),
        64 => drop(UnregisterBrokerResponseData::read(body, version)),
        65 => drop(DescribeTransactionsResponseData::read(body, version)),
        66 => drop(ListTransactionsResponseData::read(body, version)),
        68 => drop(ConsumerGroupHeartbeatResponseData::read(body, version)),
        69 => drop(ConsumerGroupDescribeResponseData::read(body, version)),
        71 => drop(GetTelemetrySubscriptionsResponseData::read(body, version)),
        72 => drop(PushTelemetryResponseData::read(body, version)),
        74 => drop(ListConfigResourcesResponseData::read(body, version)),
        77 => drop(ShareGroupDescribeResponseData::read(body, version)),
        80 => drop(AddRaftVoterResponseData::read(body, version)),
        81 => drop(RemoveRaftVoterResponseData::read(body, version)),
        89 => drop(StreamsGroupDescribeResponseData::read(body, version)),
        90 => drop(DescribeShareGroupOffsetsResponseData::read(body, version)),
        91 => drop(AlterShareGroupOffsetsResponseData::read(body, version)),
        92 => drop(DeleteShareGroupOffsetsResponseData::read(body, version)),
        _ => return false,
    }
    true
}

fuzz_target!(|data: &[u8]| {
    let [api_key, version, body @ ..] = data else {
        return;
    };
    let mut body = Bytes::copy_from_slice(body);
    let _covered = decode(*api_key, i16::from(*version), &mut body);
});
