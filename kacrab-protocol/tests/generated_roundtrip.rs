//! Round-trip checks for generated protocol header and request codecs.

//! Round-trip coverage for generated protocol messages wired through runtime helpers.

use bytes::{Bytes, BytesMut};
use kacrab_protocol::{
    KafkaString, RawTaggedField,
    generated::{ApiVersionsRequestData, FetchResponseData, RequestHeaderData, ResponseHeaderData},
};

#[test]
fn request_header_v2_round_trips_with_unknown_tags() {
    let original = RequestHeaderData {
        request_api_key: 18,
        request_api_version: 3,
        correlation_id: 42,
        client_id: Some(KafkaString::from("kacrab".to_owned())),
        _unknown_tagged_fields: vec![RawTaggedField {
            tag: 7,
            data: Bytes::from_static(b"tag"),
        }],
    };

    let mut out = BytesMut::new();
    original.write(&mut out, 2).expect("header should encode");

    let mut input = out.freeze();
    let decoded = RequestHeaderData::read(&mut input, 2).expect("header should decode");

    assert_eq!(decoded, original);
    assert!(input.is_empty(), "decoder should consume the whole buffer");
}

#[test]
fn response_header_v1_round_trips_with_unknown_tags() {
    let original = ResponseHeaderData {
        correlation_id: 42,
        _unknown_tagged_fields: vec![RawTaggedField {
            tag: 3,
            data: Bytes::from_static(b"rsp"),
        }],
    };

    let mut out = BytesMut::new();
    original.write(&mut out, 1).expect("header should encode");

    let mut input = out.freeze();
    let decoded = ResponseHeaderData::read(&mut input, 1).expect("header should decode");

    assert_eq!(decoded, original);
    assert!(input.is_empty(), "decoder should consume the whole buffer");
}

#[test]
fn api_versions_request_v3_round_trips_compact_strings_and_tags() {
    let original = ApiVersionsRequestData {
        client_software_name: KafkaString::from("kacrab".to_owned()),
        client_software_version: KafkaString::from("0.0.1".to_owned()),
        _unknown_tagged_fields: vec![RawTaggedField {
            tag: 9,
            data: Bytes::from_static(b"client-tag"),
        }],
    };

    let mut out = BytesMut::new();
    original.write(&mut out, 3).expect("request should encode");

    let mut input = out.freeze();
    let decoded = ApiVersionsRequestData::read(&mut input, 3).expect("request should decode");

    assert_eq!(decoded, original);
    assert!(input.is_empty(), "decoder should consume the whole buffer");
}

#[test]
fn hostile_array_length_fails_cleanly_instead_of_allocating() {
    // A FetchResponse v12 frame whose `responses` compact-array length claims
    // ~i32::MAX topics but carries no element bytes. The decoder must clamp its
    // preallocation to the bytes actually present and fail on the first missing
    // element — not reserve gigabytes for the claimed count (which would abort
    // the process under `panic = "abort"`).
    let mut hostile = BytesMut::new();
    hostile.extend_from_slice(&0i32.to_be_bytes()); // throttle_time_ms
    hostile.extend_from_slice(&0i16.to_be_bytes()); // error_code
    hostile.extend_from_slice(&0i32.to_be_bytes()); // session_id
    // Compact array length: unsigned varint of (len + 1) = i32::MAX
    // → claimed len = i32::MAX - 1.
    hostile.extend_from_slice(&[0xFF, 0xFF, 0xFF, 0xFF, 0x07]);

    let mut input = hostile.freeze();
    let result = FetchResponseData::read(&mut input, 12);
    assert!(
        result.is_err(),
        "a truncated hostile-length array must fail to decode"
    );
}
