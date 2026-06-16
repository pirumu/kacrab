//! Runtime helper coverage for hand-written protocol modules.
#![allow(
    clippy::unwrap_used,
    reason = "integration tests assert success paths directly and should fail fast on unexpected \
              errors"
)]

use bytes::{BufMut, Bytes, BytesMut};
use kacrab_protocol::{
    KafkaString, KafkaUuid, RawTaggedField, bytes_io,
    compression::{Compression, CompressionErrorKind},
    frame::{self, FrameErrorKind, MAX_FRAME_LENGTH},
    primitives, record,
    record::{Record, RecordBatch, RecordErrorKind, RecordHeader, TimestampType},
    string, tagged,
};

fn sample_record() -> Record {
    Record {
        attributes: 0,
        timestamp_delta: 3,
        offset_delta: 1,
        key: Some(Bytes::from_static(b"key")),
        value: Some(Bytes::from_static(b"value")),
        headers: vec![RecordHeader {
            key: Bytes::from_static(b"h"),
            value: Some(Bytes::from_static(b"v")),
        }],
    }
}

fn sample_batch(attributes: i16) -> RecordBatch {
    RecordBatch {
        base_offset: 7,
        partition_leader_epoch: 2,
        magic: 2,
        attributes,
        last_offset_delta: 1,
        first_timestamp: 10,
        max_timestamp: 13,
        producer_id: 99,
        producer_epoch: 4,
        base_sequence: 5,
        records: vec![sample_record()],
    }
}

#[test]
fn bytes_helpers_round_trip_fixed_compact_and_nullable_shapes() {
    let mut out = BytesMut::new();
    bytes_io::write_bytes(&mut out, b"abc").unwrap();
    bytes_io::write_nullable_bytes(&mut out, None).unwrap();
    bytes_io::write_nullable_bytes(&mut out, Some(b"def")).unwrap();
    bytes_io::write_compact_bytes(&mut out, b"ghi").unwrap();
    bytes_io::write_compact_nullable_bytes(&mut out, None).unwrap();
    bytes_io::write_compact_nullable_bytes(&mut out, Some(b"jkl")).unwrap();

    let mut input = out.freeze();
    assert_eq!(
        bytes_io::read_bytes(&mut input).unwrap(),
        Bytes::from_static(b"abc")
    );
    assert_eq!(bytes_io::read_nullable_bytes(&mut input).unwrap(), None);
    assert_eq!(
        bytes_io::read_nullable_bytes(&mut input).unwrap(),
        Some(Bytes::from_static(b"def"))
    );
    assert_eq!(
        bytes_io::read_compact_bytes(&mut input).unwrap(),
        Bytes::from_static(b"ghi")
    );
    assert_eq!(
        bytes_io::read_compact_nullable_bytes(&mut input).unwrap(),
        None
    );
    assert_eq!(
        bytes_io::read_compact_nullable_bytes(&mut input).unwrap(),
        Some(Bytes::from_static(b"jkl"))
    );
    assert!(input.is_empty());
}

#[test]
fn bytes_helpers_reject_negative_null_and_truncated_lengths() {
    let mut negative = Bytes::from_static(&[0xff, 0xff, 0xff, 0xfe]);
    assert!(matches!(
        bytes_io::read_bytes(&mut negative).unwrap_err().kind,
        bytes_io::BytesErrorKind::NegativeLength { length: -2 }
    ));

    let mut compact_null = Bytes::from_static(&[0]);
    assert!(matches!(
        bytes_io::read_compact_bytes(&mut compact_null)
            .unwrap_err()
            .kind,
        bytes_io::BytesErrorKind::UnexpectedNull
    ));

    let mut truncated = Bytes::from_static(&[0, 0, 0, 3, b'a']);
    assert!(bytes_io::read_bytes(&mut truncated).is_err());
}

#[test]
fn string_helpers_round_trip_and_validate_utf8() {
    let plain = KafkaString::from("kacrab".to_owned());
    let compact = KafkaString::from_static("wire");
    let mut out = BytesMut::new();
    string::write_string(&mut out, &plain).unwrap();
    string::write_nullable_string(&mut out, None).unwrap();
    string::write_nullable_string(&mut out, Some(&plain)).unwrap();
    string::write_compact_string(&mut out, &compact).unwrap();
    string::write_compact_nullable_string(&mut out, None).unwrap();
    string::write_compact_nullable_string(&mut out, Some(&compact)).unwrap();

    let mut input = out.freeze();
    assert_eq!(string::read_string(&mut input).unwrap().as_str(), "kacrab");
    assert_eq!(string::read_nullable_string(&mut input).unwrap(), None);
    assert_eq!(
        string::read_nullable_string(&mut input)
            .unwrap()
            .unwrap()
            .as_str(),
        "kacrab"
    );
    assert_eq!(
        string::read_compact_string(&mut input).unwrap().as_str(),
        "wire"
    );
    assert_eq!(
        string::read_compact_nullable_string(&mut input).unwrap(),
        None
    );
    assert_eq!(
        string::read_compact_nullable_string(&mut input)
            .unwrap()
            .unwrap()
            .as_str(),
        "wire"
    );
    assert!(input.is_empty());

    assert!(KafkaString::new(Bytes::from_static(&[0xff])).is_err());
}

#[test]
fn string_helpers_reject_bad_lengths_and_null_non_nullable_compact() {
    let mut negative = Bytes::from_static(&[0xff, 0xfe]);
    assert!(matches!(
        string::read_string(&mut negative).unwrap_err().kind,
        string::StringErrorKind::NegativeLength { length: -2 }
    ));

    let mut compact_null = Bytes::from_static(&[0]);
    assert!(matches!(
        string::read_compact_string(&mut compact_null)
            .unwrap_err()
            .kind,
        string::StringErrorKind::UnexpectedNull
    ));

    let mut invalid_utf8 = Bytes::from_static(&[0, 1, 0xff]);
    assert!(matches!(
        string::read_string(&mut invalid_utf8).unwrap_err().kind,
        string::StringErrorKind::InvalidUtf8 { .. }
    ));
}

#[test]
fn primitive_helpers_cover_fixed_width_varints_and_array_lengths() {
    let mut out = BytesMut::new();
    primitives::write_bool(&mut out, true);
    primitives::write_i8(&mut out, -8);
    primitives::write_i16(&mut out, -16);
    primitives::write_i32(&mut out, -32);
    primitives::write_i64(&mut out, -64);
    primitives::write_u16(&mut out, 16);
    primitives::write_u32(&mut out, 32);
    primitives::write_f64(&mut out, 1.5);
    primitives::write_unsigned_varint(&mut out, 16_384);
    primitives::write_unsigned_varlong(&mut out, 1 << 35);
    primitives::write_signed_varint(&mut out, -123);
    primitives::write_signed_varlong(&mut out, -456);
    primitives::write_array_length(&mut out, -1);
    primitives::write_compact_array_length(&mut out, -1);
    primitives::write_compact_array_length(&mut out, 3);

    let mut input = out.freeze();
    assert!(primitives::read_bool(&mut input).unwrap());
    assert_eq!(primitives::read_i8(&mut input).unwrap(), -8);
    assert_eq!(primitives::read_i16(&mut input).unwrap(), -16);
    assert_eq!(primitives::read_i32(&mut input).unwrap(), -32);
    assert_eq!(primitives::read_i64(&mut input).unwrap(), -64);
    assert_eq!(primitives::read_u16(&mut input).unwrap(), 16);
    assert_eq!(primitives::read_u32(&mut input).unwrap(), 32);
    assert_eq!(
        primitives::read_f64(&mut input).unwrap().to_bits(),
        1.5_f64.to_bits()
    );
    assert_eq!(
        primitives::read_unsigned_varint(&mut input).unwrap(),
        16_384
    );
    assert_eq!(
        primitives::read_unsigned_varlong(&mut input).unwrap(),
        1 << 35
    );
    assert_eq!(primitives::read_signed_varint(&mut input).unwrap(), -123);
    assert_eq!(primitives::read_signed_varlong(&mut input).unwrap(), -456);
    assert_eq!(primitives::read_array_length(&mut input).unwrap(), -1);
    assert_eq!(
        primitives::read_compact_array_length(&mut input).unwrap(),
        -1
    );
    assert_eq!(
        primitives::read_compact_array_length(&mut input).unwrap(),
        3
    );
}

#[test]
fn primitive_helpers_reject_truncated_and_overlong_varints() {
    let mut empty = Bytes::new();
    assert!(matches!(
        primitives::read_i32(&mut empty).unwrap_err().kind,
        primitives::PrimitiveErrorKind::InsufficientData {
            needed: 4,
            available: 0
        }
    ));

    let mut bad_varint = Bytes::from_static(&[0x80, 0x80, 0x80, 0x80, 0x80]);
    assert!(matches!(
        primitives::read_unsigned_varint(&mut bad_varint)
            .unwrap_err()
            .kind,
        primitives::PrimitiveErrorKind::InvalidVarint { max_bytes: 5 }
    ));

    let mut bad_varlong = Bytes::from_static(&[0x80; 10]);
    assert!(matches!(
        primitives::read_unsigned_varlong(&mut bad_varlong)
            .unwrap_err()
            .kind,
        primitives::PrimitiveErrorKind::InvalidVarint { max_bytes: 10 }
    ));
}

#[test]
fn tagged_fields_round_trip_and_enforce_sorted_bounded_payloads() {
    let fields = vec![
        RawTaggedField {
            tag: 1,
            data: Bytes::from_static(b"a"),
        },
        RawTaggedField {
            tag: 3,
            data: Bytes::from_static(b"bbb"),
        },
    ];
    let mut out = BytesMut::new();
    tagged::write_tagged_fields(&mut out, &fields).unwrap();
    let mut input = out.freeze();
    assert_eq!(tagged::read_tagged_fields(&mut input).unwrap(), fields);

    let unsorted = vec![
        RawTaggedField {
            tag: 2,
            data: Bytes::new(),
        },
        RawTaggedField {
            tag: 2,
            data: Bytes::new(),
        },
    ];
    assert!(matches!(
        tagged::write_tagged_fields(&mut BytesMut::new(), &unsorted).unwrap_err(),
        tagged::TaggedFieldError::OutOfOrder {
            tag: 2,
            prev_tag: 2
        }
    ));

    let mut too_short = Bytes::from_static(&[1, 7, 3, b'a']);
    assert!(matches!(
        tagged::read_tagged_fields(&mut too_short).unwrap_err(),
        tagged::TaggedFieldError::SizeOverflow {
            tag: 7,
            size: 3,
            remaining: 1
        }
    ));
}

#[test]
fn uuid_helpers_cover_wire_base64_ordering_and_parse_errors() {
    let id = KafkaUuid::from_parts(0x0011_2233_4455_6677, 0x8899_aabb_ccdd_eeff);
    let encoded = id.to_string();
    assert_eq!(KafkaUuid::from_base64(&encoded).unwrap(), id);
    assert_eq!(id.most_significant_bits(), 0x0011_2233_4455_6677);
    assert_eq!(id.least_significant_bits(), 0x8899_aabb_ccdd_eeff);
    assert!(KafkaUuid::ZERO.is_reserved());
    assert!(KafkaUuid::ONE.is_reserved());
    assert!(!id.is_nil());
    assert!(KafkaUuid::random().is_ok());
    assert!(KafkaUuid::from_base64("this-string-is-definitely-too-long").is_err());
    assert!(KafkaUuid::from_base64("bad?").is_err());
    assert!(KafkaUuid::ZERO < id);

    let mut out = BytesMut::new();
    kacrab_protocol::write_uuid(&mut out, &id);
    let mut input = out.freeze();
    assert_eq!(kacrab_protocol::read_uuid(&mut input).unwrap(), id);
}

#[test]
fn compression_dispatch_round_trips_all_enabled_codecs_and_reports_errors() {
    let payload = b"kacrab compression payload";
    assert_eq!(
        Compression::from_attributes(0x18).unwrap(),
        Compression::None
    );
    assert!(Compression::from_attributes(7).is_err());

    for codec in [
        Compression::None,
        Compression::Gzip,
        Compression::Snappy,
        Compression::Lz4,
        Compression::Zstd,
    ] {
        let compressed = codec.compress_with_level(payload, Some(1)).unwrap();
        assert_eq!(codec.decompress(&compressed).unwrap(), payload);
    }

    assert!(matches!(
        Compression::Gzip.decompress(b"not gzip").unwrap_err().kind,
        CompressionErrorKind::DecodeFailed { .. }
    ));
    assert!(matches!(
        Compression::Snappy
            .decompress(&[0x82, b'S'])
            .unwrap_err()
            .kind,
        CompressionErrorKind::DecodeFailed { .. }
    ));
    assert!(matches!(
        Compression::Zstd.decompress(b"not zstd").unwrap_err().kind,
        CompressionErrorKind::DecodeFailed { .. }
    ));
}

#[test]
fn frame_helpers_encode_decode_and_reject_bad_lengths() {
    let encoded = frame::encode_request(b"head", b"body").unwrap();
    assert_eq!(&encoded[..4], 8_i32.to_be_bytes().as_slice());
    assert_eq!(&encoded[4..], b"headbody");

    let mut response = BytesMut::new();
    response.put_i32(3);
    response.extend_from_slice(b"abc");
    let mut response = response.freeze();
    assert_eq!(
        frame::decode_response_frame(&mut response).unwrap(),
        Bytes::from_static(b"abc")
    );

    let mut negative = BytesMut::new();
    negative.put_i32(-1);
    assert!(matches!(
        frame::read_frame_length(&mut negative.freeze())
            .unwrap_err()
            .kind,
        FrameErrorKind::NegativeLength { length: -1 }
    ));

    let mut too_large = BytesMut::new();
    too_large.put_i32(MAX_FRAME_LENGTH.saturating_add(1));
    assert!(matches!(
        frame::read_frame_length(&mut too_large.freeze())
            .unwrap_err()
            .kind,
        FrameErrorKind::TooLarge { .. }
    ));

    let mut truncated = BytesMut::new();
    truncated.put_i32(4);
    truncated.extend_from_slice(b"ab");
    assert!(matches!(
        frame::decode_response_frame(&mut truncated.freeze())
            .unwrap_err()
            .kind,
        FrameErrorKind::Truncated {
            needed: 4,
            available: 2
        }
    ));
}

#[test]
fn record_and_batch_round_trip_flags_compression_and_trailing_decode() {
    let mut batch = sample_batch(0x08 | 0x10 | 0x20 | Compression::Gzip as i16);
    batch.last_offset_delta = 0;
    let mut out = BytesMut::new();
    batch
        .encode_with_compression_level(&mut out, Some(1))
        .unwrap();

    let mut input = out.clone().freeze();
    let decoded = RecordBatch::decode(&mut input).unwrap();
    assert_eq!(decoded.compression().unwrap(), Compression::Gzip);
    assert_eq!(decoded.timestamp_type(), TimestampType::LogAppendTime);
    assert!(decoded.is_transactional());
    assert!(decoded.is_control_batch());
    assert_eq!(decoded.records, batch.records);

    out.extend_from_slice(&[0, 1, 2]);
    let mut joined = out.freeze();
    let decoded_batches = record::decode_batches(&mut joined).unwrap();
    assert_eq!(decoded_batches.len(), 1);
}

#[test]
fn record_decode_rejects_negative_lengths_and_batch_corruption() {
    let mut bad_record = Bytes::from_static(&[1]);
    assert!(matches!(
        Record::decode(&mut bad_record).unwrap_err().kind,
        RecordErrorKind::NegativeLength {
            field: "record body",
            length: -1
        }
    ));

    let mut bad_header = BytesMut::new();
    primitives::write_signed_varint(&mut bad_header, -1);
    let mut bad_header = bad_header.freeze();
    assert!(matches!(
        RecordHeader::decode(&mut bad_header).unwrap_err().kind,
        RecordErrorKind::NegativeLength {
            field: "header key",
            length: -1
        }
    ));

    let mut encoded = BytesMut::new();
    sample_batch(Compression::None as i16)
        .encode(&mut encoded)
        .unwrap();

    let mut too_small = BytesMut::new();
    too_small.put_i64(4);
    too_small.put_i32(1);
    assert!(matches!(
        RecordBatch::decode(&mut too_small.freeze())
            .unwrap_err()
            .kind,
        RecordErrorKind::BatchTooSmall { got: 1, .. }
    ));

    let mut bad_magic = encoded.clone();
    bad_magic[16] = 1;
    assert!(matches!(
        RecordBatch::decode(&mut bad_magic.freeze())
            .unwrap_err()
            .kind,
        RecordErrorKind::UnsupportedMagic(1)
    ));

    let mut bad_crc = encoded;
    bad_crc[20] ^= 1;
    assert!(matches!(
        RecordBatch::decode(&mut bad_crc.freeze()).unwrap_err().kind,
        RecordErrorKind::Crc(_)
    ));
}
