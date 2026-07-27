//! Record-batch decoding over arbitrary bytes.
//!
//! This is the decoder closest to untrusted input: every byte a broker returns
//! in a `Fetch` response ends up here, including the record count, the varint
//! headers, and the compressed blob. `forbid(unsafe_code)` rules out memory
//! corruption, but it does not rule out a panic (slice index, `unwrap`,
//! arithmetic overflow), an unbounded allocation from a bogus length prefix, or
//! a non-terminating loop — and in a client a panic on the decode path is a
//! denial of service.
//!
//! The contract under test: `decode_batches` either returns `Ok` or a
//! `kacrab_protocol` error. It never panics and never diverges.

#![no_main]

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let mut buf = Bytes::copy_from_slice(data);
    let _decoded = kacrab_protocol::record::decode_batches(&mut buf);

    // Also drive the incremental entry point, which the consumer's fetch path
    // uses directly: it must make progress or stop, never spin on a batch that
    // consumes zero bytes.
    let mut buf = Bytes::copy_from_slice(data);
    let mut guard = 0_u32;
    while let Ok(Some(_batch)) = kacrab_protocol::record::decode_next_batch(&mut buf) {
        guard += 1;
        assert!(
            guard < 100_000,
            "decode_next_batch yielded 100k batches from {} bytes — it is not \
             consuming the buffer",
            data.len(),
        );
    }
});
