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
//! Coverage here is the client-facing responses, not all 93 API keys — the
//! broker-internal and KRaft controller RPCs are never parsed by this client.

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
        28 => drop(TxnOffsetCommitResponseData::read(body, version)),
        29 => drop(DescribeAclsResponseData::read(body, version)),
        30 => drop(CreateAclsResponseData::read(body, version)),
        31 => drop(DeleteAclsResponseData::read(body, version)),
        32 => drop(DescribeConfigsResponseData::read(body, version)),
        35 => drop(DescribeLogDirsResponseData::read(body, version)),
        36 => drop(SaslAuthenticateResponseData::read(body, version)),
        37 => drop(CreatePartitionsResponseData::read(body, version)),
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
        65 => drop(DescribeTransactionsResponseData::read(body, version)),
        66 => drop(ListTransactionsResponseData::read(body, version)),
        68 => drop(ConsumerGroupHeartbeatResponseData::read(body, version)),
        69 => drop(ConsumerGroupDescribeResponseData::read(body, version)),
        71 => drop(GetTelemetrySubscriptionsResponseData::read(body, version)),
        74 => drop(ListConfigResourcesResponseData::read(body, version)),
        77 => drop(ShareGroupDescribeResponseData::read(body, version)),
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
