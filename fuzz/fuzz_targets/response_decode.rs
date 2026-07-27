//! Generated response decoding, one API key and version per input.
//!
//! The generated `*ResponseData::read` functions are what turn a broker's bytes
//! into typed Rust. They are checked byte-for-byte against the Java client
//! oracle on *well-formed* input; this target covers the other half — garbage,
//! truncation, and hostile length prefixes — for the responses a client
//! actually parses on its hot paths.
//!
//! The first two bytes of the input steer the target (which API key, which
//! version); the rest is the response body. Steering this way rather than
//! fuzzing one key at a time lets a single corpus reach every decoder, and
//! lets libFuzzer discover which version bytes unlock deeper structure.

#![no_main]

use bytes::Bytes;
use kacrab_protocol::generated::{
    ApiVersionsResponseData, DescribeGroupsResponseData, FetchResponseData, FindCoordinatorResponseData,
    InitProducerIdResponseData, ListOffsetsResponseData, MetadataResponseData, OffsetCommitResponseData,
    OffsetFetchResponseData, ProduceResponseData, SaslAuthenticateResponseData, SaslHandshakeResponseData,
};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if data.len() < 2 {
        return;
    }
    let selector = data[0];
    // Versions are deliberately allowed to run past each API's supported range:
    // an out-of-range version must produce `UnsupportedVersion`, not a panic.
    let version = i16::from(data[1]);
    let mut buf = Bytes::copy_from_slice(&data[2..]);

    match selector % 12 {
        0 => drop(ProduceResponseData::read(&mut buf, version)),
        1 => drop(FetchResponseData::read(&mut buf, version)),
        2 => drop(MetadataResponseData::read(&mut buf, version)),
        3 => drop(ListOffsetsResponseData::read(&mut buf, version)),
        4 => drop(ApiVersionsResponseData::read(&mut buf, version)),
        5 => drop(FindCoordinatorResponseData::read(&mut buf, version)),
        6 => drop(OffsetFetchResponseData::read(&mut buf, version)),
        7 => drop(OffsetCommitResponseData::read(&mut buf, version)),
        8 => drop(InitProducerIdResponseData::read(&mut buf, version)),
        9 => drop(DescribeGroupsResponseData::read(&mut buf, version)),
        10 => drop(SaslHandshakeResponseData::read(&mut buf, version)),
        _ => drop(SaslAuthenticateResponseData::read(&mut buf, version)),
    }
});
