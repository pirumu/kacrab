//! Record-batch decoding *past* the CRC gate.
//!
//! `record_batch_decode` feeds raw bytes straight in, which is the honest test
//! of the framing and rejection paths — but it is almost worthless for anything
//! deeper. `decode_next_batch` validates CRC32C over the batch body
//! (`record/batch.rs:300`) *before* it looks at the magic byte, the record
//! count, the varint record headers, or the compressed blob. Random bytes pass
//! a CRC32C check with probability 2^-32, so a raw-byte fuzzer bounces off that
//! gate forever and never reaches the logic worth fuzzing. That shows up
//! directly as a flat coverage curve.
//!
//! This target hands the fuzzer the CRC-covered region and builds correct
//! framing around it: log overhead, batch length, magic, and a recomputed CRC.
//! Every mutation therefore lands *inside* the decoder — attributes, record
//! count vs actual payload, varint lengths, compression codec bits — which is
//! where a panic, an unbounded allocation, or a non-terminating loop would
//! actually live.
//!
//! Keep both targets. This one cannot catch a bug in CRC handling or in the
//! length-prefix framing, because it always constructs those correctly.

#![no_main]

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;

/// Wire magic for the v2 record batch, mirroring `MAGIC_V2` in
/// `kacrab-protocol/src/record/batch.rs:24`.
const MAGIC_V2: u8 = 2;

fuzz_target!(|data: &[u8]| {
    // `data` is the CRC-covered region: attributes(2) | lastOffsetDelta(4) |
    // firstTimestamp(8) | maxTimestamp(8) | producerId(8) | producerEpoch(2) |
    // baseSequence(4) | recordCount(4) | records[..]. Shorter inputs are still
    // useful — they exercise the truncation paths inside the batch body.
    let mut body = Vec::with_capacity(data.len() + 9);
    body.extend_from_slice(&0_i32.to_be_bytes()); // partitionLeaderEpoch
    body.push(MAGIC_V2);
    body.extend_from_slice(&kacrab_protocol::crc::crc32c(data).to_be_bytes());
    body.extend_from_slice(data);

    let Ok(batch_len) = i32::try_from(body.len()) else {
        return;
    };

    let mut framed = Vec::with_capacity(body.len() + 12);
    framed.extend_from_slice(&0_i64.to_be_bytes()); // baseOffset
    framed.extend_from_slice(&batch_len.to_be_bytes());
    framed.extend_from_slice(&body);

    let mut buf = Bytes::from(framed);
    let _decoded = kacrab_protocol::record::decode_next_batch(&mut buf);
});
